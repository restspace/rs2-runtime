//! The agent surface (PRD §12), schema-first and generated per tenant:
//!
//! - `/.well-known/rs2/services` — service/mount catalogue with metadata
//! - `/.well-known/rs2/agent-surface` — entities, queries, and actions,
//!   filtered by `x-expose` surface and the caller's read permission
//! - `/.well-known/rs2/openapi` — OpenAPI 3.1; every schema referenced here
//!   is the same schema enforced at runtime (no drift by construction)
//!
//! Idempotency guidance is advertised: each action's entry states its
//! effect class and that `Idempotency-Key` is honored, so agent frameworks
//! can implement safe retries generically.

use serde_json::{json, Map, Value};

use crate::error::RsError;
use crate::message::Message;
use crate::router::Mount;
use crate::tenant::Tenant;

pub const WELL_KNOWN_PREFIX: &str = "/.well-known/rs2/";

/// Whether a path belongs to the discovery surface.
pub fn is_discovery_path(path: &str) -> bool {
    path.starts_with(WELL_KNOWN_PREFIX)
}

/// Handle a discovery request. The caller's principal must already be
/// attached (read-permission filtering depends on it). Takes the message by
/// value: the agent surface awaits store reads, and a `&Message` may not be
/// held across an await (request bodies are not Sync).
pub async fn handle(tenant: &Tenant, msg: Message) -> Result<Message, RsError> {
    if msg.method != http::Method::GET {
        return Err(RsError::new(
            405,
            crate::error::codes::BAD_REQUEST,
            "Method Not Allowed",
            "the discovery surface is read-only",
        ));
    }
    let doc = match &msg.url.path[WELL_KNOWN_PREFIX.len()..] {
        "services" => services_doc(tenant, &msg),
        "agent-surface" => {
            let mounts = readable_mounts(tenant, &msg);
            let surface = msg.url.query_param("surface");
            agent_surface_doc(tenant, mounts, surface, msg.tenant.clone()).await
        }
        "openapi" => {
            let mounts = readable_mounts(tenant, &msg);
            openapi_doc(tenant, mounts, msg.tenant.clone()).await
        }
        other => {
            return Err(RsError::not_found(format!(
                "no discovery document '{other}' (have: services, agent-surface, openapi)"
            )))
        }
    };
    Ok(msg.ok_json(&doc))
}

/// Stored specs for a spec-store mount (top-level, capped): each entry's
/// envelope contributes its schemas/metadata to the surface. Respects the
/// mount's `"store": {"root"}` override via [`spec_store::store_root`].
async fn stored_specs(tenant: &Tenant, mount: &Mount, kind_prefix: &str) -> Vec<(String, Value)> {
    let Some((_, ctx)) = tenant.instance(&mount.base_path) else {
        return vec![];
    };
    let Some(files) = &ctx.files else {
        return vec![];
    };
    let root =
        crate::services::spec_store::store_root(kind_prefix, &mount.base_path, &mount.config);
    let Ok((entries, _)) = files.list(&format!("{root}/"), 100, 0).await else {
        return vec![];
    };
    let mut out = Vec::new();
    for entry in entries {
        if entry.dir {
            continue;
        }
        let name = entry.name.clone();
        let Ok(mut body) = files.read(&format!("{root}/{name}"), None).await else {
            continue;
        };
        let Ok(bytes) = body.materialize(1024 * 1024).await else {
            continue;
        };
        if let Ok(doc) = serde_json::from_slice::<Value>(bytes) {
            out.push((name, doc));
        }
    }
    out
}

/// Parse a `code:<name>@<version>` service reference into its parts.
fn code_ref_parts(service: &str) -> Option<(&str, &str)> {
    service.strip_prefix("code:")?.split_once('@')
}

/// The optional deploy manifest for a `code:` mount (input/output schema,
/// effect class, media types, store pattern), read from the sidecar stored
/// alongside the content-addressed bundle (`.rs2-code/<name>/<version>.
/// manifest.json`). `None` when the mount declares no manifest — discovery
/// then surfaces the mount as a bare action with no schemas.
async fn code_manifest(tenant: &Tenant, mount: &Mount) -> Option<Value> {
    let (name, version) = code_ref_parts(&mount.service)?;
    let (_, ctx) = tenant.instance(&mount.base_path)?;
    let files = ctx.files.as_ref()?;
    let path = format!(
        "{}/{name}/{version}.manifest.json",
        crate::services::code::CODE_PREFIX
    );
    let mut body = files.read(&path, None).await.ok()?;
    let bytes = body.materialize(1024 * 1024).await.ok()?;
    serde_json::from_slice(bytes).ok()
}

/// Mounts the caller may read, with their agent metadata.
fn readable_mounts<'t>(tenant: &'t Tenant, msg: &Message) -> Vec<&'t Mount> {
    tenant
        .mounts
        .mounts()
        .iter()
        .filter(|m| {
            let mut probe = Message::request(http::Method::GET, &m.base_path, &msg.tenant);
            probe.principal = msg.principal.clone();
            probe.source = msg.source;
            // Filter on mount-level read access. For pipeline mounts the host
            // defers execution authz to the service (per-spec), but that is too
            // granular to advertise — discovery uses the mount's own `access`.
            match m.config.get("access") {
                None => true,
                Some(access) => crate::wrapper::check_role_spec(access, "read", &probe).is_ok(),
            }
        })
        .collect()
}

/// `?surface=mcp` filtering against the mount's `x-expose` (PRD §12):
/// a mount with `x-expose` lists the surfaces it appears on; mounts
/// without `x-expose` appear everywhere.
fn exposed_on(mount: &Mount, surface: Option<&str>) -> bool {
    let Some(surface) = surface else { return true };
    match mount.config.get("x-expose") {
        None => true,
        Some(Value::String(s)) => s == surface,
        Some(Value::Array(items)) => items.iter().any(|v| v.as_str() == Some(surface)),
        Some(_) => false,
    }
}

fn meta(mount: &Mount) -> Map<String, Value> {
    let mut out = Map::new();
    // The agent-surface metadata keys, plus a wrapper's declared request/
    // response schemas (`inputSchema`/`outputSchema` — only present on wrapper
    // configs). Copied into the services catalogue, the OPTIONS descriptor, and
    // the agent surface alike.
    for key in [
        "x-agent",
        "x-policy",
        "x-expose",
        "x-render",
        "x-context",
        "description",
        "inputSchema",
        "outputSchema",
    ] {
        if let Some(v) = mount.config.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    // Surface the selected storage backend so a client can tell a `builtin:`
    // or `code:` adapter from the node default without reading the raw config.
    if let Some(adapter) = mount
        .config
        .get("store")
        .and_then(|s| s.get("adapter"))
        .filter(|a| a.is_string())
    {
        out.insert("adapter".to_string(), adapter.clone());
    }
    // Spec stores author their specs under a reserved subtree; advertise it so a
    // generic client learns the authoring root without special-casing names.
    if let Some(subtree) = spec_subtree_of(mount) {
        out.insert("specSubtree".to_string(), json!(subtree));
    }
    // How a generic client should author this store's specs (beyond plain JSON).
    // Feature-detected; omitted for stores with no special authoring.
    if let Some(authoring) = authoring_of(mount) {
        out.insert("authoring".to_string(), authoring);
    }
    out
}

/// The reserved authoring subtree of a spec-store mount (`.queries`/
/// `.pipelines`/`.templates`), or `None` for non-spec stores. Reuses the
/// service constants; `TEMPLATE_SUBTREE` is `js`-gated, so the template arm
/// falls back to the literal on a non-`js` build (where a template mount can't
/// be built anyway).
fn spec_subtree_of(mount: &Mount) -> Option<&'static str> {
    match mount.service.as_str() {
        "query" => Some(crate::services::QUERY_SUBTREE),
        "pipeline" => Some(crate::services::PIPELINE_SUBTREE),
        #[cfg(feature = "js")]
        "template" => Some(crate::services::TEMPLATE_SUBTREE),
        #[cfg(not(feature = "js"))]
        "template" => Some(".templates"),
        _ => None,
    }
}

/// How a generic client should author this store's specs, beyond plain JSON
/// editing — `None` when there's nothing special (query/pipeline edit JSON
/// directly). Feature-detected, no service-name special-casing by the client.
/// The `template` descriptor is `js`-gated, exactly like the template mount
/// (which can't be built without the JS engine), so it's omitted on a non-`js`
/// build rather than advertised for a store that can't exist.
fn authoring_of(mount: &Mount) -> Option<Value> {
    match mount.service.as_str() {
        // The envelope validator passes `x-…` fields through untouched, so a UI
        // round-trips the author's concise notation in `x-source` and the
        // canonicalized spec lives in `pipeline`. No JS engine needed.
        "pipeline" => Some(json!({
            "kind": "pipeline-dsl",
            "compiledField": "pipeline",
            "sourceField": "x-source",
        })),
        #[cfg(feature = "js")]
        "template" => Some(json!({
            "kind": "jsx",
            "framework": "preact",
            "compiledField": "source",
            "sourceField": "jsxSource",
            "render": "html",
        })),
        _ => None,
    }
}

/// API pattern + facets (the polymorphism contract, carried over from
/// Restspace v1): `pattern` names the conversation shape so one client
/// codepath can drive every mount sharing it; `facets` declare optional
/// capabilities within the shape (feature-detect, don't special-case).
fn pattern_of(mount: &Mount) -> (String, Vec<String>) {
    // A `wrapper` mount fronts another mount, so it *declares* the shape it
    // presents (e.g. `store`) — the one service whose advertised contract comes
    // from config rather than its service type. Validated against the known set
    // at `Tenant::build`; defaults to `store-transform` (it is a pipeline).
    if mount.service == "wrapper" {
        let pattern = mount
            .config
            .get("pattern")
            .and_then(|v| v.as_str())
            .unwrap_or("store-transform")
            .to_string();
        let facets = mount
            .config
            .get("facets")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        return (pattern, facets);
    }
    let (pattern, facets): (&str, Vec<&str>) = match mount.service.as_str() {
        "file" => {
            // `meta-sort`: directory listings take `$sort` over listing
            // metadata (@name/@size/@lastModified/@contentType/@dir).
            let mut facets = vec!["range", "confirm-delete", "move", "meta-sort"];
            if mount.config.get("defaultResource").is_some()
                || mount.config.get("spaFallback").is_some()
            {
                facets.push("static-site");
            }
            ("store", facets)
        }
        "data" => (
            "store",
            // `list-projection`: dataset listings accept `$select`/`$sort`
            // (always available — host fallback when the adapter has no
            // native pushdown; see `listProjection` in the services doc).
            vec!["schema", "patch", "echo", "confirm-delete", "list-projection"],
        ),
        // Spec stores' authoring subtrees delegate to an owned FileService,
        // so their listings inherit `meta-sort`.
        "pipeline" => ("store-transform", vec!["any-verb", "meta-sort"]),
        "query" => (
            "store-view",
            vec!["positional-params", "url-params", "any-verb", "meta-sort"],
        ),
        "template" => (
            "store-view",
            vec![
                "positional-params",
                "url-params",
                "json-props",
                "any-verb",
                "meta-sort",
            ],
        ),
        "log" => ("view", vec!["url-params", "time-range", "trace-scoped"]),
        "auth" | "services" => ("api", vec![]),
        s if s.starts_with("code:") => ("api", vec![]),
        _ => ("api", vec![]),
    };
    let mut facets: Vec<String> = facets.into_iter().map(str::to_string).collect();
    // Every store-shaped mount honours `If-Match`/`If-None-Match` on writes
    // (412 on mismatch) and returns an `ETag` on PUT — the host does an atomic
    // or best-effort check depending on the adapter. A future atomic adapter
    // can add a `conditional-write-atomic` marker; today the guarantee is
    // best-effort and the client behaviour (send If-Match, handle 412) is the
    // same either way.
    if pattern.starts_with("store") {
        facets.push("conditional-write".to_string());
    }
    (pattern.to_string(), facets)
}

fn with_pattern(mut entry: Value, mount: &Mount) -> Value {
    let (pattern, facets) = pattern_of(mount);
    entry["pattern"] = json!(pattern);
    if !facets.is_empty() {
        entry["facets"] = json!(facets);
    }
    entry
}

/// Capability descriptor for one mount — the body of the `OPTIONS` probe
/// (G3). Carries the same `pattern`/`facets`/metadata a generic client reads
/// from `/.well-known/rs2/services`, plus the data schema-URL hint, so it can
/// render any path from a single round trip instead of correlating the path
/// back to the services list itself.
pub fn describe_mount(mount: &Mount) -> Value {
    let base = if mount.base_path.is_empty() {
        "/"
    } else {
        mount.base_path.as_str()
    };
    let mut out = json!({ "path": base, "service": mount.service });
    for (k, v) in meta(mount) {
        out[k] = v;
    }
    if mount.service == "data" {
        out["schemaUrlPattern"] = json!(format!("{base}/{{dataset}}/.schema.json"));
    }
    with_pattern(out, mount)
}

/// The `Allow` header set for a mount's `OPTIONS` probe, derived from its
/// pattern and facets. Store-shaped mounts share the contract's verbs (PATCH
/// only with the `patch` facet); `api` mounts get a best-effort superset,
/// since their verbs are service-defined.
pub fn allowed_methods(mount: &Mount) -> Vec<&'static str> {
    let (pattern, facets) = pattern_of(mount);
    let has = |f: &str| facets.iter().any(|x| x == f);
    let mut methods = match pattern.as_str() {
        "store" if has("patch") => {
            vec!["GET", "HEAD", "PUT", "POST", "PATCH", "DELETE", "OPTIONS"]
        }
        "store" => vec!["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS"],
        "store-transform" | "store-view" => {
            vec!["GET", "HEAD", "PUT", "POST", "PATCH", "DELETE", "OPTIONS"]
        }
        "view" => vec!["GET", "HEAD", "OPTIONS"],
        _ => vec!["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"],
    };
    if has("move") {
        methods.push("MOVE");
    }
    methods
}

fn services_doc(tenant: &Tenant, msg: &Message) -> Value {
    let readable = readable_mounts(tenant, msg);
    let services: Vec<Value> = readable
        .iter()
        .copied()
        .map(|m| {
            let mut entry = json!({
                "path": if m.base_path.is_empty() { "/" } else { &m.base_path },
                "service": m.service,
            });
            for (k, v) in meta(m) {
                entry[k] = v;
            }
            let mut entry = with_pattern(entry, m);
            // Listing cost signal for the `list-projection` facet: whether
            // `$select`/`$sort` push down to the backing engine or run on
            // the host key-walk fallback.
            if m.service == "data" {
                if let Some((_, ctx)) = tenant.instance(&m.base_path) {
                    if let Some(data) = ctx.data.as_ref() {
                        entry["listProjection"] = json!(if data.listing_pushdown() {
                            "native"
                        } else {
                            "fallback"
                        });
                    }
                }
            }
            entry
        })
        .collect();
    // The control surface (config / catalogue / code) lives on the
    // `services` mount, which a tenant may mount at any path (or not at
    // all). Surface its location explicitly so a generic admin client has
    // one stable entry point instead of scanning for `service ==
    // "services"`. `null` when the caller can't read such a mount.
    let control = readable
        .iter()
        .copied()
        .find(|m| m.service == "services")
        .map(|m| {
            let base = m.base_path.as_str();
            json!({
                "path": if base.is_empty() { "/" } else { base },
                "config": format!("{base}/raw"),
                "catalogue": format!("{base}/catalogue"),
                "catalogues": format!("{base}/catalogues"),
                "available": format!("{base}/catalogue/available"),
                "install": format!("{base}/catalogue/install"),
                "mounts": format!("{base}/services"),
                "code": format!("{base}/code/"),
            })
        });
    json!({ "tenant": msg.tenant, "services": services, "control": control })
}

async fn agent_surface_doc(
    tenant: &Tenant,
    mounts: Vec<&Mount>,
    surface: Option<String>,
    tenant_name: String,
) -> Value {
    let mut entities = Vec::new();
    let mut actions = Vec::new();
    let mut queries = Vec::new();

    for mount in mounts {
        if !exposed_on(mount, surface.as_deref()) {
            continue;
        }
        let base = if mount.base_path.is_empty() {
            "/"
        } else {
            &mount.base_path
        };
        match mount.service.as_str() {
            "data" => {
                let mut entry = json!({
                    "path": base,
                    "kind": "entity",
                    "schemaUrlPattern": format!("{base}/{{dataset}}/.schema.json"),
                    "idempotency": { "header": "Idempotency-Key", "honored": true },
                });
                for (k, v) in meta(mount) {
                    entry[k] = v;
                }
                entities.push(with_pattern(entry, mount));
            }
            "pipeline" => {
                for (name, doc) in
                    stored_specs(tenant, mount, crate::services::PIPELINE_PREFIX).await
                {
                    // `.root` governs the mount root itself.
                    let exec_path = if name == crate::services::spec_store::ROOT_SPEC {
                        base.to_string()
                    } else {
                        format!("{base}/{name}")
                    };
                    let mut entry = json!({
                        "path": exec_path,
                        "kind": "action",
                        "effect": doc.get("effect").cloned().unwrap_or(json!("unsafe")),
                        "plan": format!("{base}/{}/{name}?$plan", crate::services::PIPELINE_SUBTREE),
                        "idempotency": { "header": "Idempotency-Key", "honored": true },
                    });
                    // Envelope metadata wins over mount metadata.
                    for (k, v) in meta(mount) {
                        entry[k] = v.clone();
                    }
                    for key in ["x-agent", "x-policy", "description"] {
                        if let Some(v) = doc.get(key) {
                            entry[key] = v.clone();
                        }
                    }
                    // Optional I/O schemas from the envelope (advisory — the
                    // pipeline doesn't validate its own body), surfaced as the
                    // uniform `inputSchema`/`outputSchema` an action carries.
                    if let Some(v) = doc.get("input") {
                        entry["inputSchema"] = v.clone();
                    }
                    if let Some(v) = doc.get("output") {
                        entry["outputSchema"] = v.clone();
                    }
                    actions.push(with_pattern(entry, mount));
                }
            }
            "query" => {
                for (name, doc) in stored_specs(tenant, mount, crate::services::QUERY_PREFIX).await
                {
                    let mut entry = json!({
                        "path": format!("{base}/{name}"),
                        "kind": "query",
                        "effect": "pure",
                        "params": doc.get("params").cloned().unwrap_or(json!({})),
                    });
                    if let Some(output) = doc.get("output") {
                        entry["output"] = output.clone();
                    }
                    for (k, v) in meta(mount) {
                        entry[k] = v.clone();
                    }
                    queries.push(with_pattern(entry, mount));
                }
            }
            // A wrapper appears by the shape it declares: a `store*` pattern is an
            // entity (it fronts a dataset), anything else an action. `meta()`
            // carries its `inputSchema`/`outputSchema` so a planner sees the
            // request/response contract.
            "wrapper" => {
                let (pattern, _) = pattern_of(mount);
                let mut entry = json!({
                    "path": base,
                    "idempotency": { "header": "Idempotency-Key", "honored": true },
                });
                for (k, v) in meta(mount) {
                    entry[k] = v;
                }
                if pattern.starts_with("store") {
                    entry["kind"] = json!("entity");
                    entities.push(with_pattern(entry, mount));
                } else {
                    entry["kind"] = json!("action");
                    entry["effect"] = mount
                        .config
                        .get("effect")
                        .cloned()
                        .unwrap_or(json!("unsafe"));
                    actions.push(with_pattern(entry, mount));
                }
            }
            // A deployed `code:` service appears by its optional manifest: a
            // `storePattern` makes it an entity (it fronts a store), otherwise
            // an action. The manifest's input/output schemas (when present) are
            // surfaced as the uniform `inputSchema`/`outputSchema`; with no
            // manifest the mount still lists, as a bare action.
            s if s.starts_with("code:") => {
                let manifest = code_manifest(tenant, mount).await;
                let mut entry = json!({
                    "path": base,
                    "idempotency": { "header": "Idempotency-Key", "honored": true },
                });
                for (k, v) in meta(mount) {
                    entry[k] = v;
                }
                if let Some(m) = &manifest {
                    for key in ["inputSchema", "outputSchema", "description"] {
                        if let Some(v) = m.get(key) {
                            entry[key] = v.clone();
                        }
                    }
                }
                let store_pattern = manifest
                    .as_ref()
                    .and_then(|m| m.get("storePattern"))
                    .and_then(|v| v.as_str());
                let mut entry = with_pattern(entry, mount);
                if let Some(p) = store_pattern {
                    entry["pattern"] = json!(p);
                }
                if store_pattern
                    .map(|p| p.starts_with("store"))
                    .unwrap_or(false)
                {
                    entry["kind"] = json!("entity");
                    entities.push(entry);
                } else {
                    entry["kind"] = json!("action");
                    entry["effect"] = manifest
                        .as_ref()
                        .and_then(|m| m.get("effect").cloned())
                        .unwrap_or(json!("unsafe"));
                    actions.push(entry);
                }
            }
            _ => {}
        }
    }

    json!({
        "tenant": tenant_name,
        "entities": entities,
        "actions": actions,
        "queries": queries,
    })
}

const PROBLEM_RESPONSE: &str = "#/components/responses/Problem";

async fn openapi_doc(tenant: &Tenant, mounts: Vec<&Mount>, tenant_name: String) -> Value {
    let mut paths = Map::new();
    // Component schemas accumulate here: the always-present `Problem`, plus any
    // live dataset schemas inlined from `data` mounts (so the document is
    // self-contained — no second fetch to resolve a `$ref`).
    let mut schemas = Map::new();
    schemas.insert("Problem".to_string(), problem_schema());

    for mount in mounts {
        let base = mount.base_path.clone();
        match mount.service.as_str() {
            // Store-patterned mounts share one pair of path-item shapes —
            // structurally identical contracts, by construction.
            "file" => {
                paths.insert(
                    format!("{base}/{{dirPath}}/"),
                    json!({ "$ref": "#/components/pathItems/StoreContainer" }),
                );
                paths.insert(
                    format!("{base}/{{filePath}}"),
                    json!({ "$ref": "#/components/pathItems/StoreChild" }),
                );
            }
            "data" => {
                paths.insert(
                    format!("{base}/"),
                    json!({ "$ref": "#/components/pathItems/StoreContainer" }),
                );
                paths.insert(
                    format!("{base}/{{dataset}}/"),
                    json!({ "$ref": "#/components/pathItems/StoreContainer" }),
                );
                paths.insert(
                    format!("{base}/{{dataset}}/{{key}}"),
                    json!({ "$ref": "#/components/pathItems/StoreChild" }),
                );
                paths.insert(
                    format!("{base}/{{dataset}}/.schema.json"),
                    json!({
                        "get": op("Read the dataset schema", "pure"),
                        "put": op("Install the dataset schema", "idempotent"),
                    }),
                );
                // Inline each installed dataset schema: register it under
                // components/schemas and emit a concrete `{dataset}/{key}` path
                // bound to it (the same schema the data service enforces on
                // write — no drift). Datasets with no schema keep the generic
                // StoreChild shape via the `{dataset}` template above.
                if let Some((_, ctx)) = tenant.instance(&mount.base_path) {
                    if let Some(data) = &ctx.data {
                        if let Ok((names, _)) = data.list_datasets(10_000, 0).await {
                            for name in names {
                                let Ok(Some(schema)) = data.get_schema(&name).await else {
                                    continue;
                                };
                                let key = schema_component_name(&base, &name);
                                schemas.insert(key.clone(), schema);
                                let schema_ref =
                                    json!({ "$ref": format!("#/components/schemas/{key}") });
                                paths.insert(
                                    format!("{base}/{name}/{{key}}"),
                                    store_child_for_schema(&schema_ref),
                                );
                            }
                        }
                    }
                }
            }
            // Spec-store mounts: authoring under the reserved dot-subtree
            // shares the store shape; everything else executes.
            "pipeline" => {
                let subtree = crate::services::PIPELINE_SUBTREE;
                paths.insert(
                    format!("{base}/{subtree}/"),
                    json!({ "$ref": "#/components/pathItems/StoreContainer" }),
                );
                paths.insert(
                    format!("{base}/{subtree}/{{specPath}}"),
                    json!({ "$ref": "#/components/pathItems/SpecChild" }),
                );
                // The mount-root spec's optional I/O schemas bind to the
                // execute path (advisory — pipelines don't validate their body).
                let root = stored_specs(tenant, mount, crate::services::PIPELINE_PREFIX)
                    .await
                    .into_iter()
                    .find(|(name, _)| name == crate::services::spec_store::ROOT_SPEC)
                    .map(|(_, doc)| doc);
                let input = root.as_ref().and_then(|d| d.get("input")).cloned();
                let output = root.as_ref().and_then(|d| d.get("output")).cloned();
                paths.insert(
                    format!("{base}/{{path}}"),
                    json!({
                        "description": "Execute the longest-prefix-matched stored pipeline \
                                        (.root governs the mount root). All HTTP verbs pass \
                                        through to the pipeline.",
                        "get": op_with_schemas("Run the matched stored pipeline", "unsafe",
                                               None, output.as_ref(), JSON, JSON),
                        "post": op_with_schemas("Run the matched stored pipeline with a body",
                                                "unsafe", input.as_ref(), output.as_ref(), JSON, JSON),
                    }),
                );
            }
            "query" => {
                let subtree = crate::services::QUERY_SUBTREE;
                paths.insert(
                    format!("{base}/{subtree}/"),
                    json!({ "$ref": "#/components/pathItems/StoreContainer" }),
                );
                paths.insert(
                    format!("{base}/{subtree}/{{specPath}}"),
                    json!({ "$ref": "#/components/pathItems/SpecChild" }),
                );
                paths.insert(
                    format!("{base}/{{queryPath}}"),
                    json!({
                        "description": "Execute the longest-prefix-matched stored query; \
                                        extra path segments append positional params; params \
                                        come from the query string (schema-coerced) and/or a \
                                        JSON body; results page with X-Total-Count.",
                        "get": op("Execute the stored query (params from the query string)", "pure"),
                        "post": op("Execute the stored query (params from the body)", "pure"),
                    }),
                );
            }
            "auth" => {
                paths.insert(
                    format!("{base}/login"),
                    json!({
                        "post": op("Log in (sets the rs-auth cookie, returns a JWT)", "unsafe")
                    }),
                );
                paths.insert(
                    format!("{base}/refresh"),
                    json!({
                        "post": op("Sliding session refresh", "idempotent")
                    }),
                );
                paths.insert(
                    format!("{base}/logout"),
                    json!({
                        "post": op("Log out (clears the cookie)", "idempotent")
                    }),
                );
                paths.insert(
                    format!("{base}/user"),
                    json!({
                        "get": op("The authenticated principal", "pure")
                    }),
                );
            }
            "services" => {
                paths.insert(
                    format!("{base}/catalogue"),
                    json!({
                        "get": op("Available services and config schemas", "pure")
                    }),
                );
                paths.insert(format!("{base}/raw"), json!({
                    "get": op("The tenant's raw config (ETag-versioned; secrets masked)", "pure"),
                    "put": op("Replace the tenant config (validated, atomic; If-Match)", "idempotent"),
                }));
                // The deployed-code store (content-addressed facet: deploy
                // is keyless POST; PUT only at the true hash name).
                paths.insert(
                    format!("{base}/code/"),
                    json!({ "$ref": "#/components/pathItems/StoreContainer" }),
                );
                paths.insert(
                    format!("{base}/code/{{name}}/"),
                    json!({ "$ref": "#/components/pathItems/StoreContainer" }),
                );
                paths.insert(
                    format!("{base}/code/{{name}}/{{version}}"),
                    json!({ "$ref": "#/components/pathItems/StoreChild" }),
                );
            }
            "log" => {
                let root = if base.is_empty() {
                    "/".to_string()
                } else {
                    base.clone()
                };
                paths.insert(
                    root,
                    json!({
                        "description": "Query this tenant's structured logs, newest first. \
                                        Params: $take, severity (debug|info|warn|error floor), \
                                        traceId, service (mount or path prefix), since/until \
                                        (unix-ms or RFC 3339), q (body substring). JSON array of \
                                        OTLP LogRecords, or text/plain NDJSON via Accept; \
                                        X-Total-Count set.",
                        "get": op_media("Query structured logs", "pure",
                                        &["application/json", "text/plain"]),
                    }),
                );
                paths.insert(
                    format!("{base}/{{traceId}}"),
                    json!({
                        "description": "All log records for one trace (request-debugging view).",
                        "get": op("Read one trace's log records", "pure"),
                    }),
                );
            }
            // A wrapper fronts another mount with one inline pipeline; advertise
            // a single path item carrying its declared request/response schemas
            // (inputSchema enforced, outputSchema advisory) on the verbs its
            // declared pattern allows.
            "wrapper" => {
                let input = mount.config.get("inputSchema");
                let output = mount.config.get("outputSchema");
                let mut item = Map::new();
                item.insert(
                    "description".to_string(),
                    json!(
                        "Single inline pipeline fronting another mount; forwards the exact \
                           path beyond the mount with ${url.rest}."
                    ),
                );
                for method in allowed_methods(mount) {
                    let (key, is_write, effect) = match method {
                        "GET" => ("get", false, "pure"),
                        "PUT" => ("put", true, "idempotent"),
                        "POST" => ("post", true, "unsafe"),
                        "PATCH" => ("patch", true, "unsafe"),
                        "DELETE" => ("delete", false, "idempotent"),
                        _ => continue, // HEAD / OPTIONS / MOVE: not OpenAPI operations
                    };
                    let req = if is_write { input } else { None };
                    item.insert(
                        key.to_string(),
                        op_with_schemas(
                            "Run the wrapper pipeline",
                            effect,
                            req,
                            output,
                            JSON,
                            JSON,
                        ),
                    );
                }
                paths.insert(format!("{base}/{{path}}"), Value::Object(item));
            }
            // A deployed `code:` service: emit one path item carrying the
            // manifest's declared request/response schemas and media types (the
            // input schema the engine enforces if it validates; the output is
            // advisory), mirroring the wrapper. With no manifest the path item
            // is still emitted, schema-free, so the service is discoverable.
            s if s.starts_with("code:") => {
                let manifest = code_manifest(tenant, mount).await;
                let input = manifest.as_ref().and_then(|m| m.get("inputSchema"));
                let output = manifest.as_ref().and_then(|m| m.get("outputSchema"));
                let req_media = manifest
                    .as_ref()
                    .and_then(|m| m.get("requestMediaType"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(JSON);
                let resp_media = manifest
                    .as_ref()
                    .and_then(|m| m.get("responseMediaType"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(JSON);
                let mut item = Map::new();
                item.insert(
                    "description".to_string(),
                    json!(
                        "Deployed custom-code service; the path beyond the mount is passed \
                           to the guest. Schemas/media types come from the deploy manifest."
                    ),
                );
                for method in allowed_methods(mount) {
                    let (key, is_write, effect) = match method {
                        "GET" => ("get", false, "pure"),
                        "PUT" => ("put", true, "idempotent"),
                        "POST" => ("post", true, "unsafe"),
                        "DELETE" => ("delete", false, "idempotent"),
                        _ => continue, // HEAD / OPTIONS: not OpenAPI operations
                    };
                    let req = if is_write { input } else { None };
                    item.insert(
                        key.to_string(),
                        op_with_schemas(
                            "Invoke the custom-code service",
                            effect,
                            req,
                            output,
                            req_media,
                            resp_media,
                        ),
                    );
                }
                paths.insert(format!("{base}/{{path}}"), Value::Object(item));
            }
            _ => {}
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": format!("RS2 tenant '{tenant_name}'"),
            "version": "0.1.0",
        },
        "paths": Value::Object(paths),
        "components": {
            "pathItems": {
                // The store pattern: one conversation shape for every
                // store-patterned mount. Optional capabilities (Range,
                // PATCH, schemas) are facets declared per mount on the
                // discovery surface — feature-detect, don't special-case.
                "StoreContainer": {
                    "get": op("List children (application/vnd.rs2.dir+json: {path, entries: [{name, dir, ...}], total}; $take/$skip paginate; X-Total-Count). Stores with the 'list-projection' facet also take $select=<field,dot.path,...> (entries gain a 'fields' object) and $sort=<-field,...> (contractual field-sorted paging); stores with 'meta-sort' take $sort over listing metadata (@name, @size, @lastModified, @contentType, @dir)", "pure"),
                    "post": op("Keyless create: store the body under a generated child name; 201 + Location (stores with the 'echo' facet return the stored representation)", "unsafe"),
                    "delete": op("Delete the container; non-empty containers require ?confirm=<container name> (409 without it)", "idempotent"),
                },
                "StoreChild": {
                    "get": op("Read the stored resource; ETag carries the version", "pure"),
                    "put": op("Upsert; 201 created / 200 overwritten, empty body", "idempotent"),
                    "post": op("Upsert and return the stored representation (stores with the 'echo' facet)", "unsafe"),
                    "patch": op("JSON merge-patch (stores with the 'patch' facet)", "unsafe"),
                    "delete": op("Delete the resource", "idempotent"),
                },
                // Spec stores: children under the authoring dot-subtree
                // (.pipelines/, .queries/) are validated documents.
                "SpecChild": {
                    "get": op("Read the stored spec envelope (pipelines: ?$plan returns the segment plan)", "pure"),
                    "put": op("Author/replace the spec (validated and canonicalized at write time)", "idempotent"),
                    "delete": op("Delete the stored spec", "idempotent"),
                }
            },
            "responses": {
                "Problem": {
                    "description": "Structured error (RFC 9457)",
                    "content": {
                        "application/problem+json": {
                            "schema": { "$ref": "#/components/schemas/Problem" }
                        }
                    }
                }
            },
            "schemas": Value::Object(schemas)
        }
    })
}

/// The default operation media type when a manifest/spec doesn't declare one.
const JSON: &str = "application/json";

/// The RFC 9457 problem document schema (always present in `components`).
fn problem_schema() -> Value {
    json!({
        "type": "object",
        "required": ["type", "title", "status", "code"],
        "properties": {
            "type": { "type": "string" },
            "title": { "type": "string" },
            "status": { "type": "integer" },
            "code": { "type": "string" },
            "detail": { "type": "string" },
            "tenant": { "type": "string" },
            "traceId": { "type": "string" },
            "retryable": { "type": "boolean" },
            "retryAfterMs": { "type": "integer" }
        }
    })
}

/// A stable `components/schemas` key for a dataset's inlined schema, derived
/// from the mount base and dataset name (OpenAPI keys must match
/// `^[a-zA-Z0-9._-]+$`; other characters become `_`).
fn schema_component_name(base: &str, dataset: &str) -> String {
    let sanitize = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '.' | '-') {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>()
    };
    format!("Dataset{}_{}", sanitize(base), sanitize(dataset))
}

/// A `StoreChild` path item bound to a concrete dataset schema (the same
/// schema the data service enforces on write — no drift). Mirrors the generic
/// `StoreChild` shape; writes/reads carry the schema, PATCH/DELETE don't.
fn store_child_for_schema(schema_ref: &Value) -> Value {
    json!({
        "get": op_with_schemas("Read the stored resource; ETag carries the version",
                               "pure", None, Some(schema_ref), JSON, JSON),
        "put": op_with_schemas("Upsert; 201 created / 200 overwritten, empty body",
                               "idempotent", Some(schema_ref), None, JSON, JSON),
        "post": op_with_schemas("Upsert and return the stored representation (echo facet)",
                                "unsafe", Some(schema_ref), Some(schema_ref), JSON, JSON),
        "patch": op("JSON merge-patch (stores with the 'patch' facet)", "unsafe"),
        "delete": op("Delete the resource", "idempotent"),
    })
}

/// A minimal operation object with effect-class and idempotency metadata.
fn op(summary: &str, effect: &str) -> Value {
    json!({
        "summary": summary,
        "x-effect": effect,
        "x-idempotency-key": "honored",
        "responses": {
            "default": { "$ref": PROBLEM_RESPONSE },
            "200": { "description": "Success" }
        }
    })
}

/// [`op`] whose `200` advertises content under each given media type (no
/// schema) — for operations that vary representation by `Accept` (e.g. the
/// log view's JSON vs NDJSON).
fn op_media(summary: &str, effect: &str, response_medias: &[&str]) -> Value {
    let mut op = op(summary, effect);
    let content: Map<String, Value> = response_medias
        .iter()
        .map(|m| (m.to_string(), json!({})))
        .collect();
    op["responses"]["200"] = json!({ "description": "Success", "content": content });
    op
}

/// [`op`] plus an inline `requestBody` (`input`) and/or `200` response
/// (`output`) schema under the given request/response media types — used to
/// advertise the declared contract of a wrapper, deployed code, or a
/// schema-bound dataset child.
fn op_with_schemas(
    summary: &str,
    effect: &str,
    input: Option<&Value>,
    output: Option<&Value>,
    req_media: &str,
    resp_media: &str,
) -> Value {
    let mut op = op(summary, effect);
    if let Some(schema) = input {
        op["requestBody"] = json!({
            "content": { req_media: { "schema": schema.clone() } }
        });
    }
    if let Some(schema) = output {
        op["responses"]["200"] = json!({
            "description": "Success",
            "content": { resp_media: { "schema": schema.clone() } }
        });
    }
    op
}
