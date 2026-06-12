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
use crate::pipeline::PipelineSpec;
use crate::retry::EffectClass;
use crate::router::Mount;
use crate::tenant::Tenant;
use crate::wrapper::check_access;

pub const WELL_KNOWN_PREFIX: &str = "/.well-known/rs2/";

/// Whether a path belongs to the discovery surface.
pub fn is_discovery_path(path: &str) -> bool {
    path.starts_with(WELL_KNOWN_PREFIX)
}

/// Handle a discovery request. The caller's principal must already be
/// attached (read-permission filtering depends on it).
pub fn handle(tenant: &Tenant, msg: &Message) -> Result<Message, RsError> {
    if msg.method != http::Method::GET {
        return Err(RsError::new(
            405,
            crate::error::codes::BAD_REQUEST,
            "Method Not Allowed",
            "the discovery surface is read-only",
        ));
    }
    match &msg.url.path[WELL_KNOWN_PREFIX.len()..] {
        "services" => Ok(msg.ok_json(&services_doc(tenant, msg))),
        "agent-surface" => Ok(msg.ok_json(&agent_surface_doc(tenant, msg))),
        "openapi" => Ok(msg.ok_json(&openapi_doc(tenant, msg))),
        other => Err(RsError::not_found(format!(
            "no discovery document '{other}' (have: services, agent-surface, openapi)"
        ))),
    }
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
        "file" => ("store", vec!["range", "confirm-delete"]),
        "data" => ("store", vec!["schema", "patch", "echo", "confirm-delete"]),
        "pipeline" => ("transform", vec![]),
        "query" => ("store-view", vec![]),
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
    let services: Vec<Value> = readable_mounts(tenant, msg)
        .into_iter()
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
    json!({ "tenant": msg.tenant, "services": services })
}

fn agent_surface_doc(tenant: &Tenant, msg: &Message) -> Value {
    let surface = msg.url.query_param("surface");
    let mut entities = Vec::new();
    let mut actions = Vec::new();
    let mut queries = Vec::new();

    for mount in readable_mounts(tenant, msg) {
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
                let effect = mount
                    .config
                    .get("effect")
                    .cloned()
                    .unwrap_or(json!("unsafe"));
                let mut entry = json!({
                    "path": base,
                    "kind": "action",
                    "effect": effect,
                    "plan": format!("{base}?$plan"),
                    "idempotency": { "header": "Idempotency-Key", "honored": true },
                });
                for (k, v) in meta(mount) {
                    entry[k] = v;
                }
                actions.push(with_pattern(entry, mount));
            }
            "query" => {
                if let Some(defs) = mount.config.get("queries").and_then(|q| q.as_object()) {
                    for (name, def) in defs {
                        let mut entry = json!({
                            "path": format!("{base}/{name}"),
                            "kind": "query",
                            "method": "POST",
                            "effect": "pure",
                            "params": def.get("params").cloned().unwrap_or(json!({})),
                        });
                        if let Some(output) = def.get("output") {
                            entry["output"] = output.clone();
                        }
                        for (k, v) in meta(mount) {
                            entry[k] = v.clone();
                        }
                        queries.push(with_pattern(entry, mount));
                    }
                }
            }
            _ => {}
        }
    }

    json!({
        "tenant": msg.tenant,
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
            "pipeline" => {
                // The mounted pipeline spec drives the doc; the segment plan
                // is linked, not inlined.
                let effect = mount
                    .config
                    .get("pipeline")
                    .and_then(|p| PipelineSpec::from_value(p).ok())
                    .map(|spec| {
                        let has_unsafe = spec
                            .steps
                            .iter()
                            .any(|s| s.effect_class() == Some(EffectClass::Unsafe));
                        if has_unsafe { "unsafe" } else { "idempotent" }
                    })
                    .unwrap_or("unsafe");
                paths.insert(
                    base.clone(),
                    json!({
                        "get": op("Run the pipeline", effect),
                        "post": op("Run the pipeline with a body", effect),
                    }),
                );
            }
            "query" => {
                if let Some(defs) = mount.config.get("queries").and_then(|q| q.as_object()) {
                    for (name, def) in defs {
                        let mut post = op(&format!("Execute stored query '{name}'"), "pure");
                        if let Some(params) = def.get("params") {
                            // The same schema enforced before execution.
                            post["requestBody"] = json!({
                                "required": true,
                                "content": { "application/json": { "schema": params } }
                            });
                        }
                        paths.insert(format!("{base}/{name}"), json!({ "post": post }));
                    }
                }
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
                    "get": op("The tenant's raw config (ETag-versioned)", "pure"),
                    "put": op("Replace the tenant config (validated, atomic; If-Match)", "idempotent"),
                }));
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
