//! External call steps: an absolute `http(s)://` URL in a pipeline `call`
//! leaves the node through the mount's `httpOut` grants — allowlist checked
//! before any I/O, credentials injected host-side, and the standard retry
//! policy (statuses, network errors, effect classes) applying unchanged.
//! Runs under default features — outbound HTTP is a mock adapter.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::HttpOut;
use rs2_core::infra::InfraSet;
use rs2_core::message::Message;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// One scripted reply from the mock upstream.
enum Reply {
    Status(u16),
    NetworkError,
}

/// Records every outbound request (URL + headers) and pops scripted replies;
/// an empty script answers 200 with a JSON body.
struct MockHttp {
    urls: Mutex<Vec<String>>,
    headers: Mutex<Vec<Vec<(String, String)>>>,
    script: Mutex<VecDeque<Reply>>,
}

impl MockHttp {
    fn new() -> Arc<Self> {
        Arc::new(MockHttp {
            urls: Mutex::new(Vec::new()),
            headers: Mutex::new(Vec::new()),
            script: Mutex::new(VecDeque::new()),
        })
    }

    fn scripted(replies: Vec<Reply>) -> Arc<Self> {
        let mock = MockHttp::new();
        *mock.script.lock().unwrap() = replies.into();
        mock
    }

    fn calls(&self) -> usize {
        self.urls.lock().unwrap().len()
    }

    fn header(&self, call: usize, name: &str) -> Option<String> {
        self.headers.lock().unwrap()[call]
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    }
}

#[async_trait]
impl HttpOut for MockHttp {
    async fn request(&self, msg: Message) -> Result<Message, RsError> {
        let url = if msg.url.query.is_empty() {
            msg.url.path.clone()
        } else {
            format!("{}?{}", msg.url.path, msg.url.query)
        };
        self.urls.lock().unwrap().push(url);
        self.headers.lock().unwrap().push(
            msg.headers
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
                .collect(),
        );
        match self.script.lock().unwrap().pop_front() {
            None => Ok(msg.ok_json(&json!({ "upstream": true }))),
            Some(Reply::Status(code)) => {
                Ok(msg.response(StatusCode::from_u16(code).unwrap(), None))
            }
            Some(Reply::NetworkError) => {
                let mut err = RsError::new(
                    502,
                    "internal_error",
                    "Upstream Error",
                    "connection reset by peer",
                );
                err.retryable = true;
                Err(err)
            }
        }
    }
}

struct Loader(serde_json::Value);
#[async_trait]
impl ConfigLoader for Loader {
    async fn load_tenant(&self, _t: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

/// A node with a pipeline mount at `/p` whose config is `mount_config`, plus
/// the mock adapter (when given) and an infra set.
fn node(
    dir: &std::path::Path,
    http: Option<Arc<MockHttp>>,
    infras: serde_json::Value,
    mount_config: serde_json::Value,
) -> Arc<Runtime> {
    let mut adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir)),
        Arc::new(MemDataStore::new()),
    );
    if let Some(http) = http {
        adapters = adapters.with_http(http);
    }
    if !infras.as_object().map(|o| o.is_empty()).unwrap_or(true) {
        adapters = adapters.with_infras(InfraSet::from_json(infras).unwrap());
    }
    let config = json!({ "mounts": [
        { "path": "/p", "service": "pipeline", "config": mount_config },
        { "path": "/data", "service": "data", "config": { "access": "open" } },
    ]});
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    )
}

/// A fast, deterministic retry policy JSON for spec envelopes.
fn fast_retry(max_attempts: u32) -> serde_json::Value {
    json!({
        "enabled": true, "maxAttempts": max_attempts, "baseDelayMs": 1,
        "maxDelayMs": 5, "jitter": "none",
        "retryStatuses": [503], "retryOnNetworkError": true
    })
}

async fn put_spec(rt: &Runtime, name: &str, envelope: serde_json::Value) {
    let resp = rt
        .handle(Message::request(Method::PUT, &format!("/p/.pipelines/{name}"), "t").with_json(&envelope))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "PUT spec '{name}' failed");
}

fn stripe_grant() -> serde_json::Value {
    json!({ "access": "open",
            "grants": { "api": { "type": "httpOut", "hosts": ["api.stripe.com"] } } })
}

#[tokio::test]
async fn external_call_dispatches_through_http_out() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "charge", json!({ "pipeline": { "steps": [
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges?limit=3" } }
    ] } }))
    .await;

    let mut resp = rt.handle(Message::request(Method::POST, "/p/charge", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    let body = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(body["upstream"], true);
    assert_eq!(http.urls.lock().unwrap().as_slice(), ["https://api.stripe.com/v1/charges?limit=3"]);
}

#[tokio::test]
async fn external_call_denied_without_matching_grant() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "evil", json!({ "pipeline": { "steps": [
        { "call": { "method": "GET", "url": "https://evil.example.io/x" } }
    ] } }))
    .await;

    let mut resp = rt.handle(Message::request(Method::POST, "/p/evil", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN), "{:?}", resp.body);
    // Denied before any I/O, and the problem lists the allowed patterns.
    assert_eq!(http.calls(), 0);
    let text = String::from_utf8_lossy(
        resp.body.as_mut().unwrap().materialize(65536).await.unwrap(),
    )
    .to_string();
    assert!(text.contains("api.stripe.com"), "{text}");
}

#[tokio::test]
async fn external_call_denied_with_no_grants_at_all() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), json!({ "access": "open" }));

    put_spec(&rt, "out", json!({ "pipeline": { "steps": [
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges" } }
    ] } }))
    .await;

    let resp = rt.handle(Message::request(Method::POST, "/p/out", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN));
    assert_eq!(http.calls(), 0);
}

#[tokio::test]
async fn external_call_unavailable_without_adapter() {
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), None, json!({}), stripe_grant());

    put_spec(&rt, "out", json!({ "pipeline": { "steps": [
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges" } }
    ] } }))
    .await;

    let resp = rt.handle(Message::request(Method::POST, "/p/out", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_IMPLEMENTED));
}

#[tokio::test]
async fn external_call_injects_matching_grant_credentials() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    // Two grants with different hosts and different credentials: the call's
    // host selects the injector (wildcard grant covers subdomains).
    let rt = node(
        dir.path(),
        Some(http.clone()),
        json!({
            "stripe-key": { "adapter": "credential",
                            "config": { "auth": "bearer", "token": "sk_live_42" } },
            "acme-key": { "adapter": "credential",
                          "config": { "auth": "header", "name": "X-Api-Key", "value": "acme_9" } }
        }),
        json!({ "access": "open", "grants": {
            "stripe": { "type": "httpOut", "hosts": ["api.stripe.com"], "inject": "infra:stripe-key" },
            "acme":   { "type": "httpOut", "hosts": ["*.acme.io"], "inject": "infra:acme-key" }
        } }),
    );

    put_spec(&rt, "both", json!({ "pipeline": { "mode": "serial", "steps": [
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges" }, "as": "$a" },
        { "call": { "method": "GET", "url": "https://sub.acme.io/v2/things" }, "as": "$b" },
        { "transform": { "ok": true } }
    ] } }))
    .await;

    let resp = rt.handle(Message::request(Method::POST, "/p/both", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(http.calls(), 2);
    assert_eq!(http.header(0, "authorization").as_deref(), Some("Bearer sk_live_42"));
    assert_eq!(http.header(0, "x-api-key"), None);
    assert_eq!(http.header(1, "x-api-key").as_deref(), Some("acme_9"));
    assert_eq!(http.header(1, "authorization"), None);
}

#[tokio::test]
async fn external_call_retries_retryable_status() {
    let http = MockHttp::scripted(vec![Reply::Status(503)]);
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "flaky", json!({
        "retry": fast_retry(3),
        "pipeline": { "steps": [
            { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges" } }
        ] }
    }))
    .await;

    let mut resp = rt.handle(Message::request(Method::POST, "/p/flaky", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    let body = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(body["upstream"], true);
    // 503 then success — two attempts, one retry.
    assert_eq!(http.calls(), 2);
}

#[tokio::test]
async fn external_call_retries_network_error() {
    let http = MockHttp::scripted(vec![Reply::NetworkError]);
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "reset", json!({
        "retry": fast_retry(3),
        "pipeline": { "steps": [
            { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges" } }
        ] }
    }))
    .await;

    let resp = rt.handle(Message::request(Method::POST, "/p/reset", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(http.calls(), 2);
}

#[tokio::test]
async fn exhausted_transport_failure_shapes_the_step_report() {
    let http = MockHttp::scripted(vec![Reply::NetworkError, Reply::NetworkError]);
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "down", json!({
        "retry": fast_retry(2),
        "pipeline": { "steps": [
            { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges" } }
        ] }
    }))
    .await;

    let mut resp = rt.handle(Message::request(Method::POST, "/p/down", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_GATEWAY), "{:?}", resp.body);
    assert_eq!(http.calls(), 2, "both attempts made before giving up");
    // The failure is a structured problem carrying the per-step report.
    let body = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(body["pipeline"]["failedStep"], "/0", "{body}");
    assert!(body["pipeline"]["steps"].is_array(), "{body}");
}

#[tokio::test]
async fn keyed_external_post_carries_a_stable_idempotency_key() {
    let http = MockHttp::scripted(vec![Reply::Status(503)]);
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "pay", json!({
        "retry": fast_retry(3),
        "pipeline": { "steps": [
            { "call": { "method": "POST", "url": "https://api.stripe.com/v1/charges",
                        "effect": "keyed" } }
        ] }
    }))
    .await;

    let resp = rt
        .handle(Message::request(Method::POST, "/p/pay", "t").with_json(&json!({ "amount": 5 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(http.calls(), 2);
    let k0 = http.header(0, "idempotency-key").expect("keyed call carries a key");
    let k1 = http.header(1, "idempotency-key").expect("retry carries the key too");
    assert_eq!(k0, k1, "key must be stable across attempts");
}

#[tokio::test]
async fn external_request_carries_only_spec_headers() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "clean", json!({ "pipeline": { "steps": [
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges",
                    "headers": { "x-spec": "yes" } },
          "elevate": true }
    ] } }))
    .await;

    // The inbound request carries credentials to *this* node; none of them may
    // reach the upstream — the external request is built fresh from the spec.
    let mut inbound = Message::request(Method::POST, "/p/clean", "t");
    inbound.set_header("cookie", "rs-auth=tok");
    inbound.set_header("x-custom", "inbound");
    let resp = rt.handle(inbound).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(http.header(0, "x-spec").as_deref(), Some("yes"));
    assert_eq!(http.header(0, "authorization"), None);
    assert_eq!(http.header(0, "cookie"), None);
    assert_eq!(http.header(0, "x-custom"), None);
}

#[tokio::test]
async fn spec_headers_interpolate_captured_variables() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    // A stored connection record supplies the token the header interpolates.
    let resp = rt
        .handle(
            Message::request(Method::PUT, "/data/conns/stripe", "t")
                .with_json(&json!({ "accessToken": "sek_12345" })),
        )
        .await;
    assert!(resp.is_ok(), "{:?}", resp.status);

    put_spec(&rt, "charge", json!({ "pipeline": { "steps": [
        { "call": { "method": "GET", "url": "/data/conns/stripe" }, "as": "$conn" },
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges",
                    "headers": { "authorization": "Bearer ${conn.accessToken}",
                                 "x-static": "plain" } } }
    ] } }))
    .await;

    let resp = rt.handle(Message::request(Method::POST, "/p/charge", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    assert_eq!(http.header(0, "authorization").as_deref(), Some("Bearer sek_12345"));
    assert_eq!(http.header(0, "x-static").as_deref(), Some("plain"));
}

#[tokio::test]
async fn header_interpolation_failure_aborts_before_any_io() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    // `${conn.accessToken}` never captured: the step must fail at resolution
    // time — the request must not go out with a missing/mangled auth header.
    put_spec(&rt, "noauth", json!({ "pipeline": { "steps": [
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges",
                    "headers": { "authorization": "Bearer ${conn.accessToken}" } } }
    ] } }))
    .await;

    let resp = rt.handle(Message::request(Method::POST, "/p/noauth", "t")).await;
    assert!(!resp.is_ok(), "{:?}", resp.status);
    assert_eq!(http.calls(), 0);
}

#[tokio::test]
async fn wrapper_inline_spec_calls_externally_through_its_grants() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_http(http.clone());
    let config = json!({ "mounts": [
        { "path": "/w", "service": "wrapper", "config": {
            "access": "open",
            "grants": { "api": { "type": "httpOut", "hosts": ["api.stripe.com"] } },
            "pipeline": { "steps": [
                { "call": { "method": "GET", "url": "https://api.stripe.com/v1${url.rest}" } }
            ] }
        } }
    ]});
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    );

    let mut resp = rt.handle(Message::request(Method::GET, "/w/charges", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    let body = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(body["upstream"], true);
    assert_eq!(http.urls.lock().unwrap().as_slice(), ["https://api.stripe.com/v1/charges"]);
}

#[tokio::test]
async fn plan_warns_on_uncovered_external_hosts() {
    let http = MockHttp::new();
    let dir = tempfile::tempdir().unwrap();
    let rt = node(dir.path(), Some(http.clone()), json!({}), stripe_grant());

    put_spec(&rt, "mixed", json!({ "pipeline": { "mode": "serial", "steps": [
        { "call": { "method": "GET", "url": "https://api.stripe.com/v1/charges" }, "as": "$a" },
        { "call": { "method": "GET", "url": "https://uncovered.example.io/x" }, "as": "$b" },
        { "call": { "method": "GET", "url": "/data/orders/1" }, "as": "$c" },
        { "transform": { "ok": true } }
    ] } }))
    .await;

    let mut resp =
        rt.handle(Message::request(Method::GET, "/p/.pipelines/mixed?$plan", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let body = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    let warnings: Vec<String> = body["plan"]["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w.as_str().unwrap().to_string())
        .collect();
    // Only the uncovered external host warns; covered and internal are silent.
    assert!(
        warnings.iter().any(|w| w.contains("uncovered.example.io")),
        "{warnings:?}"
    );
    assert!(!warnings.iter().any(|w| w.contains("api.stripe.com")), "{warnings:?}");
}
