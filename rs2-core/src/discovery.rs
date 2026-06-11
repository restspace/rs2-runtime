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
            entry
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
                entities.push(entry);
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
                actions.push(entry);
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
                        queries.push(entry);
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
            "file" => {
                paths.insert(
                    format!("{base}/{{filePath}}"),
                    json!({
                        "get": op("Read a file (Range supported) or list a directory", "pure"),
                        "put": op("Write a file (streamed)", "idempotent"),
                        "delete": op("Delete a file or empty directory", "idempotent"),
                    }),
                );
            }
            "data" => {
                paths.insert(
                    format!("{base}/{{dataset}}"),
                    json!({
                        "get": op("List record keys (paginated)", "pure"),
                        "post": op("Create a record with a generated key", "unsafe"),
                    }),
                );
                paths.insert(
                    format!("{base}/{{dataset}}/{{key}}"),
                    json!({
                        "get": op("Read a record (schema-typed)", "pure"),
                        "put": op("Write a record (schema-validated)", "idempotent"),
                        "patch": op("JSON merge-patch a record", "unsafe"),
                        "delete": op("Delete a record", "idempotent"),
                    }),
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
