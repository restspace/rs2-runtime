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
use crate::wrapper::check_access;

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
        "openapi" => openapi_doc(tenant, &msg),
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
    let Some((_, ctx)) = tenant.instance(&mount.base_path) else { return vec![] };
    let Some(files) = &ctx.files else { return vec![] };
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
        let Ok(mut body) = files.read(&format!("{root}/{name}"), None).await else { continue };
        let Ok(bytes) = body.materialize(1024 * 1024).await else { continue };
        if let Ok(doc) = serde_json::from_slice::<Value>(bytes) {
            out.push((name, doc));
        }
    }
    out
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
            check_access(&probe, &m.config).is_ok()
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
    for key in ["x-agent", "x-policy", "x-expose", "x-render", "x-context", "description"] {
        if let Some(v) = mount.config.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    out
}

/// API pattern + facets (the polymorphism contract, carried over from
/// Restspace v1): `pattern` names the conversation shape so one client
/// codepath can drive every mount sharing it; `facets` declare optional
/// capabilities within the shape (feature-detect, don't special-case).
fn pattern_of(mount: &Mount) -> (&'static str, Vec<&'static str>) {
    match mount.service.as_str() {
        "file" => {
            let mut facets = vec!["range", "confirm-delete"];
            if mount.config.get("defaultResource").is_some()
                || mount.config.get("spaFallback").is_some()
            {
                facets.push("static-site");
            }
            ("store", facets)
        }
        "data" => ("store", vec!["schema", "patch", "echo", "confirm-delete"]),
        "pipeline" => ("store-transform", vec!["any-verb"]),
        "query" => ("store-view", vec!["positional-params", "url-params", "any-verb"]),
        "log" => ("view", vec!["url-params", "time-range", "trace-scoped"]),
        "auth" | "services" => ("api", vec![]),
        s if s.starts_with("code:") => ("api", vec![]),
        _ => ("api", vec![]),
    }
}

fn with_pattern(mut entry: Value, mount: &Mount) -> Value {
    let (pattern, facets) = pattern_of(mount);
    entry["pattern"] = json!(pattern);
    if !facets.is_empty() {
        entry["facets"] = json!(facets);
    }
    entry
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
            with_pattern(entry, m)
        })
        .collect();
    // The control surface (config / catalogue / code) lives on the
    // `services` mount, which a tenant may mount at any path (or not at
    // all). Surface its location explicitly so a generic admin client has
    // one stable entry point instead of scanning for `service ==
    // "services"`. `null` when the caller can't read such a mount.
    let control = readable.iter().copied().find(|m| m.service == "services").map(|m| {
        let base = m.base_path.as_str();
        json!({
            "path": if base.is_empty() { "/" } else { base },
            "config": format!("{base}/raw"),
            "catalogue": format!("{base}/catalogue"),
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
        let base = if mount.base_path.is_empty() { "/" } else { &mount.base_path };
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

fn openapi_doc(tenant: &Tenant, msg: &Message) -> Value {
    let mut paths = Map::new();

    for mount in readable_mounts(tenant, msg) {
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
                paths.insert(
                    format!("{base}/{{path}}"),
                    json!({
                        "description": "Execute the longest-prefix-matched stored pipeline \
                                        (.root governs the mount root). All HTTP verbs pass \
                                        through to the pipeline.",
                        "get": op("Run the matched stored pipeline", "unsafe"),
                        "post": op("Run the matched stored pipeline with a body", "unsafe"),
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
                paths.insert(format!("{base}/login"), json!({
                    "post": op("Log in (sets the rs-auth cookie, returns a JWT)", "unsafe")
                }));
                paths.insert(format!("{base}/refresh"), json!({
                    "post": op("Sliding session refresh", "idempotent")
                }));
                paths.insert(format!("{base}/logout"), json!({
                    "post": op("Log out (clears the cookie)", "idempotent")
                }));
                paths.insert(format!("{base}/user"), json!({
                    "get": op("The authenticated principal", "pure")
                }));
            }
            "services" => {
                paths.insert(format!("{base}/catalogue"), json!({
                    "get": op("Available services and config schemas", "pure")
                }));
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
                let root = if base.is_empty() { "/".to_string() } else { base.clone() };
                paths.insert(
                    root,
                    json!({
                        "description": "Query this tenant's structured logs, newest first. \
                                        Params: $take, severity (debug|info|warn|error floor), \
                                        traceId, service (mount or path prefix), since/until \
                                        (unix-ms or RFC 3339), q (body substring). JSON array of \
                                        OTLP LogRecords, or text/plain NDJSON via Accept; \
                                        X-Total-Count set.",
                        "get": op("Query structured logs", "pure"),
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
            _ => {}
        }
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": format!("RS2 tenant '{}'", msg.tenant),
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
                    "get": op("List children (application/vnd.rs2.dir+json: {path, entries: [{name, dir, ...}], total}; $take/$skip paginate; X-Total-Count)", "pure"),
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
            "schemas": {
                "Problem": {
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
                }
            }
        }
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
