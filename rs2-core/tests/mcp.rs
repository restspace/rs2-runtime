//! The `mcp` service: the agent surface as an MCP server. Tools come from the
//! caller's own agent surface and every call re-enters dispatch as the
//! caller, so permissions and `x-expose` filtering carry over unchanged.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Message, Principal};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct StaticLoader(Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn as_role(mut msg: Message, role: &str) -> Message {
    msg.principal = Some(Principal {
        id: format!("{role}@example.com"),
        roles: vec![role.into()],
        kind: "user".into(),
        extra: Default::default(),
    });
    msg
}

async fn body_json(msg: &mut Message) -> Value {
    msg.body
        .as_mut()
        .expect("body")
        .as_json(1024 * 1024)
        .await
        .expect("json body")
}

fn runtime(dir: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/mcp", "service": "mcp",
          "config": { "access": { "read": "all", "invoke": "all" },
                      "instructions": "Order desk tools." } },
        { "path": "/data", "service": "data",
          "config": { "access": { "read": "U A", "write": "A" } } },
        { "path": "/hidden", "service": "data",
          "config": { "access": { "read": "all", "write": "A" }, "x-expose": ["ui"] } },
        { "path": "/pipes", "service": "pipeline",
          "config": { "access": { "read": "all", "invoke": "all", "write": "A" } } }
    ] })));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

/// POST one JSON-RPC request to the MCP mount.
async fn rpc(rt: &Runtime, role: Option<&str>, method: &str, params: Value) -> Value {
    let mut msg = Message::request(Method::POST, "/mcp", "t").with_json(&json!({
        "jsonrpc": "2.0", "id": 7, "method": method, "params": params
    }));
    if let Some(r) = role {
        msg = as_role(msg, r);
    }
    let mut resp = rt.handle(msg).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::OK),
        "{method}: {:?}",
        resp.body
    );
    let body = body_json(&mut resp).await;
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], 7);
    body
}

fn tool_names(list: &Value) -> Vec<String> {
    list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect()
}

async fn author_pipeline(rt: &Runtime) {
    let put = as_role(
        Message::request(Method::PUT, "/pipes/.pipelines/greet", "t"),
        "A",
    )
    .with_json(&json!({
        "description": "Greet someone by name",
        "effect": "pure",
        "input": { "type": "object", "properties": { "name": { "type": "string" } },
                   "required": ["name"] },
        "pipeline": [ { "greeting": "\"Hello\"", "who": "name" } ]
    }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
}

#[tokio::test]
async fn initialize_and_transport_rules() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());

    let init = rpc(
        &rt,
        None,
        "initialize",
        json!({
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": { "name": "test", "version": "0" }
        }),
    )
    .await;
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(
        init["result"]["capabilities"]["tools"]["listChanged"],
        false
    );
    assert_eq!(init["result"]["serverInfo"]["name"], "rs2");
    assert_eq!(init["result"]["instructions"], "Order desk tools.");

    // An unknown version is answered with the newest one supported.
    let init = rpc(
        &rt,
        None,
        "initialize",
        json!({ "protocolVersion": "1999-01-01" }),
    )
    .await;
    assert_eq!(
        init["result"]["protocolVersion"],
        rs2_core::services::MCP_PROTOCOL_VERSIONS[0]
    );

    assert_eq!(rpc(&rt, None, "ping", json!({})).await["result"], json!({}));
    assert_eq!(
        rpc(&rt, None, "resources/list", json!({})).await["error"]["code"],
        -32601
    );

    // A notification is accepted with no body.
    let note = Message::request(Method::POST, "/mcp", "t")
        .with_json(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
    let resp = rt.handle(note).await;
    assert_eq!(resp.status, Some(StatusCode::ACCEPTED));

    // No SSE stream: GET is 405 with Allow: POST.
    let resp = rt.handle(Message::request(Method::GET, "/mcp", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::METHOD_NOT_ALLOWED));
    assert_eq!(resp.header("allow"), Some("POST"));

    // Batches and unknown protocol versions are refused at the HTTP level.
    let batch = Message::request(Method::POST, "/mcp", "t").with_json(&json!([]));
    let mut resp = rt.handle(batch).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    assert_eq!(body_json(&mut resp).await["error"]["code"], -32600);
    let mut bad = Message::request(Method::POST, "/mcp", "t")
        .with_json(&json!({ "jsonrpc": "2.0", "id": 1, "method": "ping" }));
    bad.set_header("mcp-protocol-version", "1999-01-01");
    assert_eq!(rt.handle(bad).await.status, Some(StatusCode::BAD_REQUEST));
}

#[tokio::test]
async fn tools_follow_the_callers_surface() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    author_pipeline(&rt).await;

    // Anonymous: /data is not readable, /hidden is not on the mcp surface.
    let anon = tool_names(&rpc(&rt, None, "tools/list", json!({})).await);
    assert_eq!(anon, ["pipes_greet"]);

    let user = rpc(&rt, Some("U"), "tools/list", json!({})).await;
    assert_eq!(
        tool_names(&user),
        [
            "data_list",
            "data_get",
            "data_put",
            "data_delete",
            "pipes_greet"
        ]
    );
    let greet = user["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "pipes_greet")
        .unwrap()
        .clone();
    assert_eq!(greet["inputSchema"]["required"], json!(["name"]));
    assert_eq!(greet["annotations"]["readOnlyHint"], true);
    assert!(greet["description"]
        .as_str()
        .unwrap()
        .contains("Greet someone by name"));
}

#[tokio::test]
async fn calls_run_as_the_caller() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    author_pipeline(&rt).await;

    // An action: arguments are the body, a JSON object reply is structured.
    let call = rpc(
        &rt,
        None,
        "tools/call",
        json!({ "name": "pipes_greet", "arguments": { "name": "Ada" } }),
    )
    .await;
    assert_eq!(call["result"]["isError"], false, "{call}");
    assert_eq!(call["result"]["structuredContent"]["who"], "Ada");

    // A user can read /data but not write it: the store's own 403 comes back
    // as a tool error, not a protocol error.
    let put = rpc(
        &rt,
        Some("U"),
        "tools/call",
        json!({ "name": "data_put",
        "arguments": { "path": "orders/1", "record": { "sku": "x" } } }),
    )
    .await;
    assert_eq!(put["result"]["isError"], true, "{put}");
    assert!(put["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("403"));

    // An admin can, and the user then reads it back.
    let put = rpc(
        &rt,
        Some("A"),
        "tools/call",
        json!({ "name": "data_put",
        "arguments": { "path": "orders/1", "record": { "sku": "x" } } }),
    )
    .await;
    assert_eq!(put["result"]["isError"], false, "{put}");
    let get = rpc(
        &rt,
        Some("U"),
        "tools/call",
        json!({ "name": "data_get",
        "arguments": { "path": "orders/1" } }),
    )
    .await;
    assert_eq!(get["result"]["structuredContent"]["sku"], "x", "{get}");
    let list = rpc(
        &rt,
        Some("U"),
        "tools/call",
        json!({ "name": "data_list",
        "arguments": { "path": "orders" } }),
    )
    .await;
    assert_eq!(list["result"]["isError"], false, "{list}");

    // Anonymous callers never see data tools, so calling one is unknown.
    let anon = rpc(
        &rt,
        None,
        "tools/call",
        json!({ "name": "data_get", "arguments": { "path": "orders/1" } }),
    )
    .await;
    assert_eq!(anon["error"]["code"], -32602);

    // A bad argument is reported to the model as a tool error.
    let bad = rpc(
        &rt,
        Some("U"),
        "tools/call",
        json!({ "name": "data_get", "arguments": { "path": "a?b=1" } }),
    )
    .await;
    assert_eq!(bad["result"]["isError"], true);
}

#[tokio::test]
async fn mcp_mount_advertises_the_facet() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    let mut resp = rt
        .handle(Message::request(Method::OPTIONS, "/mcp", "t"))
        .await;
    let desc = body_json(&mut resp).await;
    assert_eq!(desc["pattern"], "api");
    assert_eq!(desc["facets"], json!(["mcp"]));
}
