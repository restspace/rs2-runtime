//! `mcp` service: the tenant's agent surface as a Model Context Protocol
//! server, over the Streamable HTTP transport (JSON responses, no sessions).
//!
//! ```json
//! { "path": "/mcp", "service": "mcp",
//!   "config": { "access": { "invoke": "authenticated" } } }
//! ```
//!
//! An MCP client POSTs JSON-RPC to the mount root. The tool list is not
//! configured: it is derived, per request, from the caller's own
//! `/.well-known/rs2/agent-surface?surface=<surface>` (default `mcp`), so it
//! is already filtered by the caller's read permission and by each mount's
//! `x-expose`. A tool call becomes one ordinary internal request carrying the
//! caller's principal — access checks, limits, idempotency and logging all
//! apply exactly as they would over HTTP. The service holds no grants of its
//! own beyond the internal requester: it can do nothing the caller couldn't.
//!
//! Tools:
//! - each **action** (pipeline spec, wrapper, `code:` mount) → `POST <path>`;
//!   an object `inputSchema` becomes the tool's input schema and the
//!   arguments are the body;
//! - each **query** → `GET <path>?<params>`, input schema from `params`;
//! - each **entity** (store-shaped mount) → `<name>_list`/`_get`/`_put`/
//!   `_delete` over a `path` relative to the mount.
//!
//! An effect class maps onto the MCP tool annotations (`readOnlyHint`,
//! `idempotentHint`). A caller can pass an `Idempotency-Key` for a call as
//! `params._meta["rs2/idempotencyKey"]`.

use std::collections::HashSet;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use http::{Method, StatusCode};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::{json, Map, Value};

use super::{Service, ServiceContext};
use crate::error::RsError;
use crate::message::{Body, Message, Principal, Source, TraceContext};

/// Protocol revisions this server speaks, newest first. `initialize` echoes
/// the client's version when listed here, otherwise offers the newest.
pub const MCP_PROTOCOL_VERSIONS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26"];

/// `_meta` key a client uses to send an `Idempotency-Key` with a tool call.
pub const IDEMPOTENCY_META_KEY: &str = "rs2/idempotencyKey";

// JSON-RPC 2.0 error codes.
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;

/// Query-string value encoding: everything but RFC 3986 unreserved.
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Tool names: MCP allows `[A-Za-z0-9_.-]`, 1–128 characters.
const MAX_TOOL_NAME: usize = 64;

#[derive(Default)]
pub struct McpService;

impl McpService {
    pub fn new() -> Self {
        McpService
    }
}

/// The caller, extracted owned up front so nothing borrows the request
/// message across an await (`Body` is not `Sync`).
struct Caller {
    tenant: String,
    principal: Option<Principal>,
    trace: TraceContext,
    depth: u16,
}

#[async_trait]
impl Service for McpService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let at_root = matches!(msg.url.service_path.as_str(), "" | "/");
        if !at_root {
            return Err(RsError::not_found(
                "an MCP endpoint answers only at its mount root",
            ));
        }
        if msg.method != Method::POST {
            // Streamable HTTP: a server that offers no server-initiated SSE
            // stream answers GET with 405, and with no sessions there is
            // nothing for DELETE to end.
            let mut resp = msg.error_response(&RsError::new(
                405,
                crate::error::codes::BAD_REQUEST,
                "Method Not Allowed",
                "this MCP endpoint takes JSON-RPC by POST (no SSE stream, no sessions)",
            ));
            resp.set_header("allow", "POST");
            return Ok(resp);
        }
        if let Some(v) = msg.header("mcp-protocol-version") {
            if !MCP_PROTOCOL_VERSIONS.contains(&v) {
                return Ok(rpc_http_error(
                    &msg,
                    StatusCode::BAD_REQUEST,
                    INVALID_REQUEST,
                    &format!(
                        "unsupported MCP-Protocol-Version '{v}' (supported: {})",
                        MCP_PROTOCOL_VERSIONS.join(", ")
                    ),
                ));
            }
        }

        let raw = match msg.body.as_mut() {
            Some(body) => body
                .materialize(ctx.limits.materialized_body_bytes)
                .await?
                .clone(),
            None => Default::default(),
        };
        let request: Value = match serde_json::from_slice(&raw) {
            Ok(v) => v,
            Err(e) => {
                return Ok(rpc_http_error(
                    &msg,
                    StatusCode::BAD_REQUEST,
                    PARSE_ERROR,
                    &format!("body is not JSON: {e}"),
                ))
            }
        };
        let Value::Object(request) = request else {
            let detail = if request.is_array() {
                "JSON-RPC batches are not supported"
            } else {
                "a JSON-RPC message is an object"
            };
            return Ok(rpc_http_error(
                &msg,
                StatusCode::BAD_REQUEST,
                INVALID_REQUEST,
                detail,
            ));
        };

        // Notifications and client responses carry no reply.
        let Some(method) = request.get("method").and_then(|m| m.as_str()) else {
            return Ok(msg.response(StatusCode::ACCEPTED, None));
        };
        let Some(id) = request.get("id").cloned() else {
            return Ok(msg.response(StatusCode::ACCEPTED, None));
        };
        let params = request.get("params").cloned().unwrap_or(json!({}));

        let caller = Caller {
            tenant: msg.tenant.clone(),
            principal: msg.principal.clone(),
            trace: msg.trace.clone(),
            depth: msg.depth,
        };
        let outcome = match method {
            "initialize" => Ok(initialize(&params, ctx)),
            "ping" => Ok(json!({})),
            "tools/list" => list_tools(ctx, &caller).await.map(
                |tools| json!({ "tools": tools.iter().map(Tool::describe).collect::<Vec<_>>() }),
            ),
            "tools/call" => call_tool(&params, ctx, &caller).await,
            other => Err(RpcError::new(
                METHOD_NOT_FOUND,
                format!("method '{other}' is not supported"),
            )),
        };
        let reply = match outcome {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(e) => json!({ "jsonrpc": "2.0", "id": id, "error": e.to_json() }),
        };
        Ok(msg.ok_json(&reply))
    }
}

struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        RpcError {
            code,
            message: message.into(),
        }
    }

    fn to_json(&self) -> Value {
        json!({ "code": self.code, "message": self.message })
    }
}

/// A transport-level failure: an HTTP error status carrying a JSON-RPC error
/// with no id (the request could not be read far enough to have one).
fn rpc_http_error(msg: &Message, status: StatusCode, code: i64, detail: &str) -> Message {
    let body = json!({
        "jsonrpc": "2.0",
        "id": null,
        "error": { "code": code, "message": detail },
    });
    msg.response(status, Some(Body::from_json(&body)))
}

fn initialize(params: &Value, ctx: &ServiceContext) -> Value {
    let requested = params.get("protocolVersion").and_then(|v| v.as_str());
    let version = requested
        .filter(|v| MCP_PROTOCOL_VERSIONS.contains(v))
        .unwrap_or(MCP_PROTOCOL_VERSIONS[0]);
    let name = ctx
        .config
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("rs2");
    let mut result = json!({
        "protocolVersion": version,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": name, "version": env!("CARGO_PKG_VERSION") },
    });
    if let Some(instructions) = ctx.config.get("instructions").and_then(|v| v.as_str()) {
        result["instructions"] = json!(instructions);
    }
    result
}

/// What a tool does when called.
enum Op {
    /// `POST <path>`; `wrapped` puts the body under an argument named `body`.
    Action { path: String, wrapped: bool },
    /// `GET <path>?<arguments>`.
    Query { path: String },
    /// A store verb over `<base>/<arguments.path>`.
    Entity { base: String, verb: EntityVerb },
}

#[derive(Clone, Copy)]
enum EntityVerb {
    List,
    Get,
    Put,
    Delete,
}

struct Tool {
    name: String,
    description: String,
    input_schema: Value,
    annotations: Value,
    op: Op,
}

impl Tool {
    fn describe(&self) -> Value {
        json!({
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
            "annotations": self.annotations,
        })
    }
}

/// The caller's agent surface, fetched as the caller.
async fn agent_surface(ctx: &ServiceContext, caller: &Caller) -> Result<Value, RpcError> {
    let requester = ctx.requester.as_ref().ok_or_else(|| {
        RpcError::new(
            INTERNAL_ERROR,
            "this host gives the mcp service no internal requester",
        )
    })?;
    let surface = ctx
        .config
        .get("surface")
        .and_then(|v| v.as_str())
        .unwrap_or("mcp");
    let path = format!(
        "{}agent-surface?surface={}",
        crate::discovery::WELL_KNOWN_PREFIX,
        utf8_percent_encode(surface, QUERY_VALUE)
    );
    let mut resp = requester
        .request(internal_request(caller, Method::GET, &path))
        .await;
    if !resp.is_ok() {
        return Err(RpcError::new(
            INTERNAL_ERROR,
            format!("reading the agent surface failed with {:?}", resp.status),
        ));
    }
    match resp.body.as_mut() {
        Some(body) => body
            .as_json(ctx.limits.materialized_body_bytes)
            .await
            .map_err(|e| RpcError::new(INTERNAL_ERROR, e.detail)),
        None => Ok(json!({})),
    }
}

fn internal_request(caller: &Caller, method: Method, path: &str) -> Message {
    let mut req = Message::request(method, path, &caller.tenant);
    req.source = Source::Internal;
    req.principal = caller.principal.clone();
    req.trace = caller.trace.child();
    req.depth = caller.depth.saturating_add(1);
    req
}

async fn list_tools(ctx: &ServiceContext, caller: &Caller) -> Result<Vec<Tool>, RpcError> {
    Ok(tools_from_surface(&agent_surface(ctx, caller).await?))
}

/// Derive the tool set from an agent-surface document. Deterministic: the
/// surface's own order, entities then queries then actions, with numeric
/// suffixes resolving any name collision.
fn tools_from_surface(surface: &Value) -> Vec<Tool> {
    let mut tools = Vec::new();
    let mut names = HashSet::new();
    let entries = |key: &str| {
        surface
            .get(key)
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
    };

    for entry in entries("entities") {
        let Some(base) = entry.get("path").and_then(|v| v.as_str()) else {
            continue;
        };
        let slug = slug_of(base);
        let hint = describe_entry(&entry);
        let path_doc = if entry.get("schemaUrlPattern").is_some() {
            "Path under the mount: '' lists datasets, '<dataset>/' lists a dataset, \
             '<dataset>/<key>' is one record, '<dataset>/.schema.json' is its schema."
        } else {
            "Path under the mount ('' is the mount root; a trailing '/' lists a directory)."
        };
        let path_prop = json!({ "type": "string", "description": path_doc });
        for (suffix, verb, summary) in [
            ("list", EntityVerb::List, "List entries"),
            ("get", EntityVerb::Get, "Read one entry"),
            ("put", EntityVerb::Put, "Create or replace one entry"),
            ("delete", EntityVerb::Delete, "Delete one entry"),
        ] {
            let mut props = Map::new();
            props.insert("path".into(), path_prop.clone());
            let mut required = vec![];
            if !matches!(verb, EntityVerb::List) {
                required.push("path");
            }
            if matches!(verb, EntityVerb::Put) {
                props.insert(
                    "record".into(),
                    json!({ "description": "The entry to store (JSON)." }),
                );
                required.push("record");
            }
            let annotations = match verb {
                EntityVerb::List | EntityVerb::Get => json!({ "readOnlyHint": true }),
                EntityVerb::Put => json!({ "readOnlyHint": false, "idempotentHint": true }),
                EntityVerb::Delete => json!({
                    "readOnlyHint": false, "destructiveHint": true, "idempotentHint": true
                }),
            };
            tools.push(Tool {
                name: unique_name(&mut names, &format!("{slug}_{suffix}")),
                description: format!("{summary} in the store at {base}.{hint}"),
                input_schema: json!({
                    "type": "object",
                    "properties": props,
                    "required": required,
                }),
                annotations,
                op: Op::Entity {
                    base: base.to_string(),
                    verb,
                },
            });
        }
    }

    for entry in entries("queries") {
        let Some(path) = entry.get("path").and_then(|v| v.as_str()) else {
            continue;
        };
        let input_schema = match entry.get("params") {
            Some(p) if is_object_schema(p) => p.clone(),
            _ => json!({ "type": "object" }),
        };
        tools.push(Tool {
            name: unique_name(&mut names, &slug_of(path)),
            description: format!("Run the query at GET {path}.{}", describe_entry(&entry)),
            input_schema,
            annotations: effect_annotations("pure"),
            op: Op::Query {
                path: path.to_string(),
            },
        });
    }

    for entry in entries("actions") {
        let Some(path) = entry.get("path").and_then(|v| v.as_str()) else {
            continue;
        };
        let effect = entry
            .get("effect")
            .and_then(|v| v.as_str())
            .unwrap_or("unsafe");
        let (input_schema, wrapped) = match entry.get("inputSchema") {
            Some(s) if is_object_schema(s) => (s.clone(), false),
            Some(s) => (
                json!({
                    "type": "object",
                    "properties": { "body": s },
                    "required": ["body"],
                }),
                true,
            ),
            None => (json!({ "type": "object" }), false),
        };
        tools.push(Tool {
            name: unique_name(&mut names, &slug_of(path)),
            description: format!(
                "Run the action at POST {path} (effect: {effect}).{}",
                describe_entry(&entry)
            ),
            input_schema,
            annotations: effect_annotations(effect),
            op: Op::Action {
                path: path.to_string(),
                wrapped,
            },
        });
    }
    tools
}

/// ` <description>` from an entry's metadata, or empty.
fn describe_entry(entry: &Value) -> String {
    let text = entry
        .get("description")
        .and_then(|v| v.as_str())
        .or_else(|| {
            entry
                .get("x-agent")
                .and_then(|a| a.get("description"))
                .and_then(|v| v.as_str())
        });
    match text {
        Some(t) if !t.trim().is_empty() => format!(" {}", t.trim()),
        _ => String::new(),
    }
}

fn effect_annotations(effect: &str) -> Value {
    match effect {
        "pure" => json!({ "readOnlyHint": true }),
        "idempotent" => json!({ "readOnlyHint": false, "idempotentHint": true }),
        _ => json!({ "readOnlyHint": false, "idempotentHint": false }),
    }
}

/// An input schema MCP accepts as-is: an object schema.
fn is_object_schema(schema: &Value) -> bool {
    schema.get("type").and_then(|t| t.as_str()) == Some("object")
        || (schema.get("type").is_none() && schema.get("properties").is_some())
}

/// A path as a tool name: `/orders/create` → `orders_create`, root → `root`.
fn slug_of(path: &str) -> String {
    let slug: String = path
        .trim_matches('/')
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let slug = if slug.is_empty() {
        "root".to_string()
    } else {
        slug
    };
    slug.chars().take(MAX_TOOL_NAME).collect()
}

fn unique_name(taken: &mut HashSet<String>, base: &str) -> String {
    let mut name = base.to_string();
    let mut n = 2;
    while taken.contains(&name) {
        name = format!("{base}_{n}");
        n += 1;
    }
    taken.insert(name.clone());
    name
}

async fn call_tool(
    params: &Value,
    ctx: &ServiceContext,
    caller: &Caller,
) -> Result<Value, RpcError> {
    let name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| RpcError::new(INVALID_PARAMS, "tools/call needs a string 'name'"))?;
    let args = match params.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(m)) => m.clone(),
        Some(_) => {
            return Err(RpcError::new(
                INVALID_PARAMS,
                "'arguments' must be an object",
            ))
        }
    };
    let tools = list_tools(ctx, caller).await?;
    let tool = tools
        .iter()
        .find(|t| t.name == name)
        .ok_or_else(|| RpcError::new(INVALID_PARAMS, format!("unknown tool '{name}'")))?;

    let mut req = match build_call(&tool.op, args, caller) {
        Ok(req) => req,
        // An argument the tool can't use is reported to the model, which can
        // correct it — a tool execution error, not a protocol error.
        Err(detail) => return Ok(tool_error(&detail)),
    };
    if let Some(key) = params
        .get("_meta")
        .and_then(|m| m.get(IDEMPOTENCY_META_KEY))
        .and_then(|v| v.as_str())
    {
        req.set_header("idempotency-key", key);
    }
    let requester = ctx.requester.as_ref().ok_or_else(|| {
        RpcError::new(
            INTERNAL_ERROR,
            "this host gives the mcp service no internal requester",
        )
    })?;
    let resp = requester.request(req).await;
    Ok(tool_result(resp, ctx.limits.materialized_body_bytes).await)
}

fn build_call(op: &Op, mut args: Map<String, Value>, caller: &Caller) -> Result<Message, String> {
    match op {
        Op::Action { path, wrapped } => {
            let body = if *wrapped {
                args.remove("body").unwrap_or(Value::Null)
            } else {
                Value::Object(args)
            };
            Ok(internal_request(caller, Method::POST, path).with_json(&body))
        }
        Op::Query { path } => {
            let mut query = String::new();
            for (k, v) in &args {
                let value = match v {
                    Value::Null => continue,
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                query.push(if query.is_empty() { '?' } else { '&' });
                query.push_str(&utf8_percent_encode(k, QUERY_VALUE).to_string());
                query.push('=');
                query.push_str(&utf8_percent_encode(&value, QUERY_VALUE).to_string());
            }
            Ok(internal_request(
                caller,
                Method::GET,
                &format!("{path}{query}"),
            ))
        }
        Op::Entity { base, verb } => {
            let rel = match args.get("path") {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => s.trim_start_matches('/').to_string(),
                Some(_) => return Err("'path' must be a string".into()),
            };
            if rel.contains('?') || rel.contains('#') {
                return Err("'path' may not carry a query or fragment".into());
            }
            let base = base.trim_end_matches('/');
            let mut path = if rel.is_empty() {
                format!("{base}/")
            } else {
                format!("{base}/{rel}")
            };
            let method = match verb {
                EntityVerb::List => {
                    if !path.ends_with('/') {
                        path.push('/');
                    }
                    Method::GET
                }
                EntityVerb::Get => Method::GET,
                EntityVerb::Put => Method::PUT,
                EntityVerb::Delete => Method::DELETE,
            };
            if !matches!(verb, EntityVerb::List) && rel.is_empty() {
                return Err("'path' names the entry and may not be empty".into());
            }
            let req = internal_request(caller, method, &path);
            Ok(match verb {
                EntityVerb::Put => {
                    let record = args.remove("record").unwrap_or(Value::Null);
                    req.with_json(&record)
                }
                _ => req,
            })
        }
    }
}

fn tool_error(detail: &str) -> Value {
    json!({ "content": [{ "type": "text", "text": detail }], "isError": true })
}

/// An internal response as a `CallToolResult`. A 4xx/5xx is a tool execution
/// error (`isError`) carrying the problem document, so the model sees why.
async fn tool_result(mut resp: Message, cap: u64) -> Value {
    let status = resp.status.unwrap_or(StatusCode::OK);
    let location = resp.header("location").map(str::to_string);
    let mut content = Vec::new();
    let mut structured = None;
    if let Some(body) = resp.body.as_mut() {
        let media = body.media_type.clone();
        match body.materialize(cap).await {
            Err(e) => content.push(json!({ "type": "text", "text": format!("response body unreadable: {}", e.detail) })),
            Ok(bytes) => {
                let bytes = bytes.clone();
                if media.is_json() {
                    match serde_json::from_slice::<Value>(&bytes) {
                        Ok(v) => {
                            content.push(json!({ "type": "text", "text": v.to_string() }));
                            if v.is_object() {
                                structured = Some(v);
                            }
                        }
                        Err(_) => content.push(json!({
                            "type": "text", "text": String::from_utf8_lossy(&bytes)
                        })),
                    }
                } else if media.is_text() {
                    content.push(json!({ "type": "text", "text": String::from_utf8_lossy(&bytes) }));
                } else if media.essence().starts_with("image/") {
                    content.push(json!({
                        "type": "image", "data": B64.encode(&bytes), "mimeType": media.essence()
                    }));
                } else if !bytes.is_empty() {
                    content.push(json!({
                        "type": "text",
                        "text": format!("{} bytes of {} (base64): {}", bytes.len(), media.essence(), B64.encode(&bytes)),
                    }));
                }
            }
        }
    }
    if content.is_empty() {
        content.push(json!({ "type": "text", "text": format!("HTTP {}", status.as_u16()) }));
    }
    if let Some(loc) = location {
        content.push(json!({ "type": "text", "text": format!("Location: {loc}") }));
    }
    let mut result = json!({ "content": content, "isError": status.as_u16() >= 400 });
    if let Some(v) = structured {
        result["structuredContent"] = v;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_valid_tool_names() {
        assert_eq!(slug_of("/orders/create"), "orders_create");
        assert_eq!(slug_of("/"), "root");
        assert_eq!(slug_of("/a b/c.d"), "a_b_c_d");
    }

    #[test]
    fn collisions_get_suffixes() {
        let mut taken = HashSet::new();
        assert_eq!(unique_name(&mut taken, "x"), "x");
        assert_eq!(unique_name(&mut taken, "x"), "x_2");
        assert_eq!(unique_name(&mut taken, "x"), "x_3");
    }

    #[test]
    fn surface_maps_to_tools() {
        let surface = json!({
            "entities": [{ "path": "/data", "schemaUrlPattern": "/data/{dataset}/.schema.json" }],
            "queries": [{ "path": "/q/byName", "params": { "type": "object",
                "properties": { "name": { "type": "string" } } } }],
            "actions": [
                { "path": "/pipes/order", "effect": "keyed", "description": "Place an order",
                  "inputSchema": { "type": "object", "properties": { "sku": { "type": "string" } } } },
                { "path": "/pipes/raw", "effect": "pure", "inputSchema": { "type": "string" } }
            ]
        });
        let tools = tools_from_surface(&surface);
        let names: Vec<_> = tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "data_list",
                "data_get",
                "data_put",
                "data_delete",
                "q_byName",
                "pipes_order",
                "pipes_raw"
            ]
        );
        let order = tools.iter().find(|t| t.name == "pipes_order").unwrap();
        assert_eq!(order.input_schema["properties"]["sku"]["type"], "string");
        assert!(order.description.contains("Place an order"));
        assert_eq!(order.annotations["readOnlyHint"], false);
        let raw = tools.iter().find(|t| t.name == "pipes_raw").unwrap();
        assert_eq!(raw.input_schema["required"], json!(["body"]));
        assert_eq!(raw.annotations["readOnlyHint"], true);
    }
}
