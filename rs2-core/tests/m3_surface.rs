//! M3 integration tests (PRD §16, "Surface & migration"): the `query`
//! service, the generated agent surface + OpenAPI, pipeline failure
//! context, and custom code deployment.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Body, MediaType, Message};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct MutableLoader(Mutex<serde_json::Value>);

#[async_trait]
impl ConfigLoader for MutableLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.lock().unwrap().clone())
            .map_err(|e| RsError::internal(e.to_string()))
    }

    async fn load_raw(&self, _tenant: &str) -> Result<(serde_json::Value, String), RsError> {
        Ok((self.0.lock().unwrap().clone(), "v".to_string()))
    }

    async fn save_raw(
        &self,
        _tenant: &str,
        config: &serde_json::Value,
        _expected_version: Option<&str>,
    ) -> Result<String, RsError> {
        *self.0.lock().unwrap() = config.clone();
        Ok("v2".to_string())
    }
}

fn surface_config() -> serde_json::Value {
    json!({
        "mounts": [
            { "path": "/data", "service": "data" },
            { "path": "/q", "service": "query", "config": {
                "x-expose": ["mcp"],
                "queries": {
                    "open-orders": {
                        "query": { "dataset": "orders",
                                   "where": { "status": "${status}", "total": { "op": ">=", "value": "${min}" } },
                                   "orderBy": "total" },
                        "params": { "type": "object", "required": ["status", "min"],
                                    "properties": { "status": { "type": "string" },
                                                    "min": { "type": "number" } } }
                    }
                }
            } },
            { "path": "/summary", "service": "pipeline", "config": {
                "x-agent": { "kind": "action", "safe": true },
                "pipeline": [ "GET /data/orders/${id}", { "status": "$.status" } ]
            } },
            { "path": "/broken", "service": "pipeline", "config": {
                "pipeline": { "onFail": "stop", "steps": [
                    { "call": { "method": "GET", "url": "/data/orders/missing-one" } },
                    { "transform": { "x": "$" } }
                ] }
            } },
            { "path": "/services", "service": "services" },
            { "path": "/secret", "service": "file",
              "config": { "access": { "readRoles": "A", "writeRoles": "A" } } }
        ]
    })
}

fn rt_with(file_root: &std::path::Path, config: serde_json::Value) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(MutableLoader(Mutex::new(config)));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body.as_mut().expect("body").as_json(10 * 1024 * 1024).await.expect("json body")
}

async fn seed_orders(rt: &Runtime) {
    for (key, status, total) in
        [("o1", "open", 50.0), ("o2", "open", 10.0), ("o3", "closed", 99.0)]
    {
        let put = req(Method::PUT, &format!("/data/orders/{key}"))
            .with_json(&json!({ "status": status, "total": total }));
        assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    }
}

// ---------------------------------------------------------------------------
// query service (PRD §10.4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_service_executes_validated_stored_queries() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());
    seed_orders(&rt).await;

    // Valid parameters → filtered, ordered, counted results.
    let mut resp = rt
        .handle(req(Method::POST, "/q/open-orders").with_json(&json!({
            "status": "open", "min": 5
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    assert_eq!(resp.header("x-total-count"), Some("2"));
    let rows = body_json(&mut resp).await;
    let totals: Vec<f64> =
        rows.as_array().unwrap().iter().map(|r| r["total"].as_f64().unwrap()).collect();
    assert_eq!(totals, vec![10.0, 50.0], "orderBy applied");

    // Pagination caps results but keeps the true total.
    let mut page = rt
        .handle(req(Method::POST, "/q/open-orders?$take=1").with_json(&json!({
            "status": "open", "min": 0
        })))
        .await;
    assert_eq!(page.header("x-total-count"), Some("2"));
    assert_eq!(body_json(&mut page).await.as_array().unwrap().len(), 1);

    // Parameters are schema-validated before execution → 422 with details.
    let mut invalid = rt
        .handle(req(Method::POST, "/q/open-orders").with_json(&json!({ "status": 42 })))
        .await;
    assert_eq!(invalid.status, Some(StatusCode::UNPROCESSABLE_ENTITY));
    let problem = body_json(&mut invalid).await;
    assert_eq!(problem["code"], "validation_failed");
    assert!(problem["errors"].as_array().is_some_and(|e| !e.is_empty()));

    // Unknown stored query → 404.
    let missing =
        rt.handle(req(Method::POST, "/q/nope").with_json(&json!({}))).await;
    assert_eq!(missing.status, Some(StatusCode::NOT_FOUND));
}

// ---------------------------------------------------------------------------
// agent surface + OpenAPI (PRD §12)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discovery_surface_filters_and_advertises() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    // /services catalogue: anonymous caller sees readable mounts only —
    // /secret (readRoles: "A") is filtered out.
    let mut services = rt.handle(req(Method::GET, "/.well-known/rs2/services")).await;
    assert_eq!(services.status, Some(StatusCode::OK));
    let doc = body_json(&mut services).await;
    let paths: Vec<&str> =
        doc["services"].as_array().unwrap().iter().map(|s| s["path"].as_str().unwrap()).collect();
    assert!(paths.contains(&"/data") && paths.contains(&"/summary"));
    assert!(!paths.contains(&"/secret"), "unreadable mounts are hidden: {paths:?}");

    // agent-surface: entities, actions (with idempotency guidance), queries
    // (with the same param schema enforced at runtime).
    let mut surface = rt.handle(req(Method::GET, "/.well-known/rs2/agent-surface")).await;
    let doc = body_json(&mut surface).await;
    assert_eq!(doc["entities"][0]["path"], "/data");
    let action = &doc["actions"][0];
    assert_eq!(action["path"], "/summary");
    assert_eq!(action["idempotency"]["header"], "Idempotency-Key");
    assert_eq!(action["x-agent"]["kind"], "action");
    let query = &doc["queries"][0];
    assert_eq!(query["path"], "/q/open-orders");
    assert_eq!(query["params"]["required"][0], "status");

    // x-expose filtering: /q is exposed on "mcp" only.
    let mut ui = rt.handle(req(Method::GET, "/.well-known/rs2/agent-surface?surface=ui")).await;
    let doc = body_json(&mut ui).await;
    assert!(doc["queries"].as_array().unwrap().is_empty(), "{doc}");
    let mut mcp = rt.handle(req(Method::GET, "/.well-known/rs2/agent-surface?surface=mcp")).await;
    let doc = body_json(&mut mcp).await;
    assert_eq!(doc["queries"].as_array().unwrap().len(), 1);

    // OpenAPI 3.1: generated paths + the problem schema; the stored query's
    // param schema is the request body schema (no drift by construction).
    let mut openapi = rt.handle(req(Method::GET, "/.well-known/rs2/openapi")).await;
    let doc = body_json(&mut openapi).await;
    assert_eq!(doc["openapi"], "3.1.0");
    assert!(doc["paths"]["/data/{dataset}/{key}"]["put"].is_object());
    let query_body =
        &doc["paths"]["/q/open-orders"]["post"]["requestBody"]["content"]["application/json"]["schema"];
    assert_eq!(query_body["required"][0], "status");
    assert!(doc["components"]["schemas"]["Problem"].is_object());

    // The surface is read-only.
    let post = rt.handle(req(Method::POST, "/.well-known/rs2/services")).await;
    assert_eq!(post.status.unwrap().as_u16(), 405);
}

// ---------------------------------------------------------------------------
// structured pipeline failures (PRD §12)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_failures_name_the_failing_step() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    let mut resp = rt.handle(req(Method::GET, "/broken")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND), "step failure propagates");
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "not_found");
    assert_eq!(problem["pipeline"]["failedStep"], "/0", "{problem}");
    let steps = problem["pipeline"]["steps"].as_array().unwrap();
    assert_eq!(steps[0]["kind"], "call");
    assert_eq!(steps[0]["status"], 404);
}

// ---------------------------------------------------------------------------
// custom code deployment (PRD §10.6, §11)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn code_deploys_content_addressed_and_mounts() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    // A header-valid but bogus component: with the engine in the build the
    // deployment-time smoke test rejects it outright (PRD §10.6).
    let fake_component = b"\0asm-fake-component".to_vec();
    let deploy = req(Method::PUT, "/services/code/echo").with_body(Body::from_bytes(
        fake_component.clone(),
        MediaType::new("application/wasm"),
    ));
    let mut resp = rt.handle(deploy).await;
    if cfg!(feature = "wasm") {
        assert_eq!(resp.status.unwrap().as_u16(), 502, "smoke test rejects non-components");
        return;
    }
    // Featureless build: accepted unvalidated, content-addressed.
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    let body = body_json(&mut resp).await;
    let code_ref = body["ref"].as_str().unwrap().to_string();
    assert!(code_ref.starts_with("code:echo@"), "{code_ref}");

    // Re-deploying identical bytes yields the same version (immutable,
    // content-addressed — PRD §14).
    let again = req(Method::PUT, "/services/code/echo")
        .with_body(Body::from_bytes(fake_component, MediaType::new("application/wasm")));
    let mut resp2 = rt.handle(again).await;
    assert_eq!(body_json(&mut resp2).await["ref"].as_str().unwrap(), code_ref);

    // Versions list.
    let mut list = rt.handle(req(Method::GET, "/services/code/echo")).await;
    assert_eq!(body_json(&mut list).await["versions"].as_array().unwrap().len(), 1);

    // Mount it via self-config; without the wasm feature the mount builds
    // but serves a structured 501 at request time.
    let (mut config, _) = (surface_config(), ());
    config["mounts"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "path": "/custom", "service": code_ref, "config": { "grants": {} } }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    let mut hit = rt.handle(req(Method::GET, "/custom/hello")).await;
    assert_eq!(hit.status, Some(StatusCode::NOT_IMPLEMENTED));
    assert_eq!(body_json(&mut hit).await["code"], "engine_unavailable");
}

/// With the JS engine in the build: deploy a JS bundle through the
/// self-config API, mount it with a capability grant, and serve a request
/// that round-trips through the grant back into the data service.
#[cfg(feature = "js")]
#[tokio::test]
async fn deployed_js_bundle_serves_requests_with_grants() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());
    seed_orders(&rt).await;

    let bundle = r#"
        export default async (msg, ctx) => {
            const order = ctx.request("orders", { url: "/o1" });
            ctx.log("info", `loaded order o1: ${order.status}`);
            return {
                status: 200,
                headers: { "x-engine": "js" },
                body: { engine: "js", orderStatus: order.body.status },
            };
        };
    "#;
    let deploy = req(Method::PUT, "/services/code/order-view").with_body(Body::from_bytes(
        bundle.as_bytes().to_vec(),
        MediaType::new("application/javascript"),
    ));
    let mut resp = rt.handle(deploy).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let body = body_json(&mut resp).await;
    assert_eq!(body["validated"], true, "compile smoke test ran");
    let code_ref = body["ref"].as_str().unwrap().to_string();

    // Mount it with a grant scoping the capability to /data/orders.
    let mut config = surface_config();
    config["mounts"].as_array_mut().unwrap().push(json!({
        "path": "/order-view", "service": code_ref,
        "config": { "grants": { "orders": { "prefix": "/data/orders" } } }
    }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    let mut hit = rt.handle(req(Method::GET, "/order-view/anything")).await;
    assert_eq!(hit.status, Some(StatusCode::OK), "{:?}", hit.body);
    assert_eq!(hit.header("x-engine"), Some("js"));
    let out = body_json(&mut hit).await;
    assert_eq!(out["orderStatus"], "open", "grant round-tripped into the data service");

    // A broken bundle is rejected at deploy time by the compile smoke test.
    let bad = req(Method::PUT, "/services/code/broken").with_body(Body::from_bytes(
        b"export default ((((".to_vec(),
        MediaType::new("application/javascript"),
    ));
    assert_eq!(rt.handle(bad).await.status.unwrap().as_u16(), 502);
}

/// With the wasm engine in the build: deploy the real conformance echo
/// component end-to-end and invoke it through a mount. Self-skips unless
/// `RS2_CONFORMANCE_COMPONENT` points at the built guest.
#[cfg(feature = "wasm")]
#[tokio::test]
async fn deployed_wasm_component_serves_requests() {
    let Ok(component_path) = std::env::var("RS2_CONFORMANCE_COMPONENT") else {
        eprintln!("skipping: RS2_CONFORMANCE_COMPONENT not set");
        return;
    };
    let bytes = std::fs::read(&component_path).expect("read conformance component");

    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    let deploy = req(Method::PUT, "/services/code/echo")
        .with_body(Body::from_bytes(bytes, MediaType::new("application/wasm")));
    let mut resp = rt.handle(deploy).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    let body = body_json(&mut resp).await;
    assert_eq!(body["validated"], true);
    let code_ref = body["ref"].as_str().unwrap().to_string();

    let mut config = surface_config();
    config["mounts"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "path": "/custom", "service": code_ref, "config": { "grants": {} } }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    let mut hit = rt.handle(req(Method::GET, "/custom/hello?x=1")).await;
    assert_eq!(hit.status, Some(StatusCode::OK));
    assert_eq!(hit.header("x-engine"), Some("wasm"));
    let echoed = body_json(&mut hit).await;
    assert_eq!(echoed["method"], "GET");
}
