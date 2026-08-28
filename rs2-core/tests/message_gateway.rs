//! G13 reference proof that the resident substrate generalizes beyond storage
//! to a typed *provider* capability: loadable `code:` adapters back a `message`
//! mount's [`MessageGateway`]. The stock `message` service maps `POST /send` /
//! `GET /status/{id}` / `GET /channels` to the trait; the guest bundles map
//! those to (mock) providers. Swapping providers is a `store.adapter` change —
//! no recompile.
//!
//! The interesting case is the second test: one mount, two providers, two
//! channels, routed by the message's own `channel` — and one of those providers
//! cannot report delivery status, which the surface must admit rather than fake.

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

/// A mock email provider: keeps what it accepted and reports it "delivered".
/// It speaks the channel-tagged envelope the host's `GuestMessageGateway` sends.
const EMAIL_ADAPTER: &str = r#"
let SENT = {};
let N = 0;
export default async (msg) => {
  const path = String(msg.url).split("?")[0];
  if (msg.method === "POST" && path === "/send") {
    if (msg.body.channel !== "email") return { status: 400, body: { detail: "email only" } };
    const id = "em_" + (++N);
    SENT[id] = { to: msg.body.to, subject: msg.body.subject, html: msg.body.html };
    return { status: 201, body: { id } };
  }
  if (msg.method === "GET" && path.startsWith("/status/")) {
    const id = path.slice("/status/".length);
    const rec = SENT[id];
    if (!rec) return { status: 404, body: { detail: "unknown message " + id } };
    return { status: 200, body: { id, subject: rec.subject, status: "delivered" } };
  }
  return { status: 400, body: { detail: "unsupported" } };
};
"#;

/// A mock SMS provider that accepts sends but, like AWS SNS, has no per-message
/// delivery status to give. It is mounted with `deliveryStatus: false`.
const SMS_ADAPTER: &str = r#"
let N = 0;
export default async (msg) => {
  const path = String(msg.url).split("?")[0];
  if (msg.method === "POST" && path === "/send") {
    if (msg.body.channel !== "sms") return { status: 400, body: { detail: "sms only" } };
    return { status: 201, body: { id: "sm_" + (++N), to: msg.body.to } };
  }
  return { status: 501, body: { detail: "no delivery status" } };
};
"#;

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn body_json(resp: &mut Message) -> serde_json::Value {
    resp.body.as_mut().unwrap().as_json(65536).await.unwrap()
}

async fn runtime_with(mounts: serde_json::Value, bundles: &[(&str, &str)]) -> Arc<Runtime> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    for (path, src) in bundles {
        files
            .write(
                "t",
                path,
                Body::from_string(*src, MediaType::new("application/javascript")),
            )
            .await
            .unwrap();
    }
    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(StaticLoader(mounts)),
        LimitTable::default(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_email_sends_and_reports_status() {
    let rt = runtime_with(
        json!({ "mounts": [
            { "path": "/msg", "service": "message", "config": {
                "access": "open",
                "store": { "adapter": "code:mailer@v1", "channels": ["email"] } } }
        ]}),
        &[(".rs2-code/mailer/v1.js", EMAIL_ADAPTER)],
    )
    .await;

    // POST /send → 201 with the provider's receipt.
    let mut resp = rt
        .handle(req(Method::POST, "/msg/send").with_json(&json!({
            "channel": "email",
            "to": [{"email": "a@b.com", "name": "A"}],
            "subject": "Welcome",
            "html": "<p>hi</p>"
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let sent = body_json(&mut resp).await;
    let id = sent["id"].as_str().expect("id").to_string();
    assert!(id.starts_with("em_"), "provider id: {id}");
    assert_eq!(sent["channel"], "email");
    assert_eq!(sent["provider"], "code:mailer@v1");

    // GET /status/{id} → mapped back through the trait.
    let mut resp = rt
        .handle(req(Method::GET, &format!("/msg/status/{id}")))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let st = body_json(&mut resp).await;
    assert_eq!(st["status"], "delivered");
    assert_eq!(st["subject"], "Welcome");

    // Unknown id → the adapter's 404 surfaces with its status class preserved.
    let resp = rt.handle(req(Method::GET, "/msg/status/em_999")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));

    // The mount declares what it can do, and it is the truth the router enforces.
    let mut resp = rt.handle(req(Method::GET, "/msg/channels")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let ch = body_json(&mut resp).await;
    assert_eq!(ch["channels"], json!(["email"]));
    assert_eq!(ch["deliveryStatus"], true);

    // An SMS to an email-only mount is refused before the provider is called.
    let mut resp = rt
        .handle(
            req(Method::POST, "/msg/send")
                .with_json(&json!({"channel": "sms", "to": "+15551234567", "text": "hi"})),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let detail = body_json(&mut resp).await;
    assert!(
        detail
            .to_string()
            .contains("no adapter for the 'sms' channel"),
        "names the gap: {detail}"
    );

    // A missing field is the service's own 400 (before the adapter is called).
    let resp = rt
        .handle(
            req(Method::POST, "/msg/send").with_json(&json!({"channel": "email", "to": "a@b.com"})),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_mount_routes_two_channels_to_two_providers() {
    let rt = runtime_with(
        json!({ "mounts": [
            { "path": "/msg", "service": "message", "config": {
                "access": "open",
                "store": { "adapters": {
                    "email": { "adapter": "code:mailer@v1", "channels": ["email"] },
                    "sms": { "adapter": "code:texter@v1", "channels": ["sms"],
                             "deliveryStatus": false } } } } }
        ]}),
        &[
            (".rs2-code/mailer/v1.js", EMAIL_ADAPTER),
            (".rs2-code/texter/v1.js", SMS_ADAPTER),
        ],
    )
    .await;

    // The same endpoint reaches both providers; the channel picks the route.
    let mut resp = rt
        .handle(req(Method::POST, "/msg/send").with_json(&json!({
            "channel": "email", "to": "a@b.com", "subject": "Hi", "text": "hi"
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    assert_eq!(body_json(&mut resp).await["provider"], "code:mailer@v1");

    let mut resp = rt
        .handle(req(Method::POST, "/msg/send").with_json(&json!({
            "channel": "sms", "to": "+15551234567", "text": "hi"
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let receipt = body_json(&mut resp).await;
    assert_eq!(receipt["provider"], "code:texter@v1");
    assert!(receipt["id"].as_str().unwrap().starts_with("sm_"));

    let mut resp = rt.handle(req(Method::GET, "/msg/channels")).await;
    let ch = body_json(&mut resp).await;
    assert_eq!(ch["channels"], json!(["email", "sms"]));
    // One route reports status, so the mount does — the email ids still resolve.
    assert_eq!(ch["deliveryStatus"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_provider_without_delivery_status_says_so_instead_of_faking_it() {
    let rt = runtime_with(
        json!({ "mounts": [
            { "path": "/msg", "service": "message", "config": {
                "access": "open",
                "store": { "adapter": "code:texter@v1", "channels": ["sms"],
                           "deliveryStatus": false } } }
        ]}),
        &[(".rs2-code/texter/v1.js", SMS_ADAPTER)],
    )
    .await;

    let resp = rt
        .handle(req(Method::POST, "/msg/send").with_json(&json!({
            "channel": "sms", "to": "+15551234567", "text": "hi"
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));

    let mut resp = rt.handle(req(Method::GET, "/msg/status/sm_1")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_IMPLEMENTED));
    let detail = body_json(&mut resp).await;
    assert!(
        detail
            .to_string()
            .contains("does not report per-message delivery status"),
        "names the limitation: {detail}"
    );

    let mut resp = rt.handle(req(Method::GET, "/msg/channels")).await;
    assert_eq!(body_json(&mut resp).await["deliveryStatus"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_mount_without_an_adapter_is_a_config_error() {
    let rt = runtime_with(
        json!({ "mounts": [
            { "path": "/msg", "service": "message", "config": { "access": "open" } }
        ]}),
        &[],
    )
    .await;
    let mut resp = rt
        .handle(req(Method::POST, "/msg/send").with_json(&json!({
            "channel": "sms", "to": "+1555", "text": "x"
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let detail = body_json(&mut resp).await;
    assert!(
        detail.to_string().contains("store.adapter"),
        "explains the missing adapter: {detail}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_adapter_routed_for_a_channel_it_does_not_serve_is_rejected_at_build() {
    let rt = runtime_with(
        json!({ "mounts": [
            { "path": "/msg", "service": "message", "config": {
                "access": "open",
                "store": { "adapters": {
                    "sms": { "adapter": "code:mailer@v1", "channels": ["email"] } } } } }
        ]}),
        &[(".rs2-code/mailer/v1.js", EMAIL_ADAPTER)],
    )
    .await;
    let mut resp = rt
        .handle(req(Method::POST, "/msg/send").with_json(&json!({
            "channel": "sms", "to": "+1555", "text": "x"
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let detail = body_json(&mut resp).await;
    assert!(
        detail
            .to_string()
            .contains("is routed for the 'sms' channel"),
        "names the mismatch: {detail}"
    );
}
