//! G13 reference proof that the resident substrate generalizes beyond storage
//! to a typed *provider* capability: a loadable `code:` SMS adapter backs an
//! `sms` mount's [`SmsGateway`]. The stock `sms` service maps `POST /send` /
//! `GET /status/{id}` to the trait; the guest bundle maps those to a (mock)
//! provider. Swapping providers is a `store.adapter` change — no recompile.

#![cfg(feature = "js")]

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::FileStore;
use rs2_core::message::{Body, MediaType, Message};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct StaticLoader(serde_json::Value);
#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _t: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

/// A self-contained mock SMS provider adapter: it keeps sent messages in a
/// module-level map and reports them "delivered". It speaks the `sms`-pattern
/// envelopes the host's `GuestSmsGateway` sends.
const SMS_ADAPTER: &str = r#"
let SENT = {};
let N = 0;
export default async (msg) => {
  const path = String(msg.url).split("?")[0];
  if (msg.method === "POST" && path === "/send") {
    const id = "sm_" + (++N);
    SENT[id] = { to: msg.body.to, body: msg.body.body };
    return { status: 201, body: { id } };
  }
  if (msg.method === "GET" && path.startsWith("/status/")) {
    const id = path.slice("/status/".length);
    const rec = SENT[id];
    if (!rec) return { status: 404, body: { detail: "unknown message " + id } };
    return { status: 200, body: { id, to: rec.to, status: "delivered" } };
  }
  return { status: 400, body: { detail: "unsupported" } };
};
"#;

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn body_json(resp: &mut Message) -> serde_json::Value {
    resp.body.as_mut().unwrap().as_json(65536).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_sms_gateway_sends_and_reports_status() {
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    files
        .write(
            "t",
            ".rs2-code/twilio/v1.js",
            Body::from_string(SMS_ADAPTER, MediaType::new("application/javascript")),
        )
        .await
        .unwrap();

    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/sms", "service": "sms", "config": {
            "access": "open", "store": { "adapter": "code:twilio@v1" } } }
    ]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );

    // POST /send → 201 with a provider message id.
    let mut resp = rt
        .handle(req(Method::POST, "/sms/send").with_json(&json!({ "to": "+15551234567", "body": "hi" })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let sent = body_json(&mut resp).await;
    let id = sent["id"].as_str().expect("id").to_string();
    assert!(id.starts_with("sm_"), "provider id: {id}");

    // GET /status/{id} → delivery status mapped back through the trait.
    let mut resp = rt.handle(req(Method::GET, &format!("/sms/status/{id}"))).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let st = body_json(&mut resp).await;
    assert_eq!(st["status"], "delivered");
    assert_eq!(st["to"], "+15551234567");

    // Unknown id → the adapter's 404 surfaces with its status class preserved.
    let resp = rt.handle(req(Method::GET, "/sms/status/sm_999")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));

    // A missing field is the service's own 400 (before the adapter is called).
    let resp = rt.handle(req(Method::POST, "/sms/send").with_json(&json!({ "body": "no to" }))).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sms_mount_without_adapter_is_a_config_error() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/sms", "service": "sms", "config": { "access": "open" } }
    ]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );
    let mut resp = rt
        .handle(req(Method::POST, "/sms/send").with_json(&json!({ "to": "+1", "body": "x" })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let detail = body_json(&mut resp).await;
    assert!(
        detail.to_string().contains("store.adapter"),
        "explains the missing adapter: {detail}"
    );
}
