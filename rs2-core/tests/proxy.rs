//! `proxy` service (v1 "proxy adapter"): forward to a fixed external target,
//! attaching operator-supplied auth host-side. Runs under default features —
//! the proxy service is pure Rust; outbound HTTP is a mock adapter.

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

struct MockHttp {
    url: Mutex<Vec<String>>,
    auth: Mutex<Vec<Option<String>>>,
    cookie: Mutex<Vec<Option<String>>>,
}

impl MockHttp {
    fn new() -> Self {
        MockHttp { url: Mutex::new(Vec::new()), auth: Mutex::new(Vec::new()), cookie: Mutex::new(Vec::new()) }
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
        self.url.lock().unwrap().push(url);
        self.auth.lock().unwrap().push(msg.header("authorization").map(String::from));
        self.cookie.lock().unwrap().push(msg.header("cookie").map(String::from));
        Ok(msg.ok_json(&json!({ "object": "charge", "paid": true })))
    }
}

struct Loader(serde_json::Value);
#[async_trait]
impl ConfigLoader for Loader {
    async fn load_tenant(&self, _t: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
    async fn load_raw(&self, _t: &str) -> Result<(serde_json::Value, String), RsError> {
        Ok((self.0.clone(), "v".into()))
    }
    async fn save_raw(
        &self,
        _t: &str,
        _c: &serde_json::Value,
        _v: Option<&str>,
    ) -> Result<String, RsError> {
        Ok("v".into())
    }
}

#[tokio::test]
async fn proxy_forwards_to_target_with_injected_auth_and_no_secret_leak() {
    let http = Arc::new(MockHttp::new());
    let dir = tempfile::tempdir().unwrap();

    let infras = InfraSet::from_json(json!({
        "stripe-key": { "adapter": "credential", "config": { "auth": "bearer", "token": "sk_live_42" } }
    }))
    .unwrap();

    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_http(http.clone())
    .with_infras(infras);

    let config = json!({ "mounts": [
        { "path": "/services", "service": "services", "config": { "access": "open" } },
        { "path": "/stripe", "service": "proxy", "config": {
            "access": "open",
            "target": "https://api.stripe.com",
            "inject": "infra:stripe-key"
        } }
    ]});

    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    );

    let mut resp = rt.handle(Message::request(Method::GET, "/stripe/v1/charges?limit=3", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    assert_eq!(resp.body.as_mut().unwrap().as_json(65536).await.unwrap()["paid"], true);

    // The request reached the target host with the remaining path + query…
    let url = http.url.lock().unwrap();
    assert_eq!(url.len(), 1);
    assert_eq!(url[0], "https://api.stripe.com/v1/charges?limit=3");
    // …and carried the operator credential, injected host-side.
    assert_eq!(http.auth.lock().unwrap()[0].as_deref(), Some("Bearer sk_live_42"));

    // The secret never appears in the tenant config round-trip.
    let mut raw = rt.handle(Message::request(Method::GET, "/services/raw", "t")).await;
    let raw_text =
        String::from_utf8_lossy(raw.body.as_mut().unwrap().materialize(1 << 20).await.unwrap())
            .to_string();
    assert!(!raw_text.contains("sk_live_42"), "secret leaked into /services/raw");
}

#[tokio::test]
async fn proxy_without_target_is_a_config_error() {
    let http = Arc::new(MockHttp::new());
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_http(http);
    let config = json!({ "mounts": [
        { "path": "/p", "service": "proxy", "config": { "access": "open" } }
    ]});
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    );
    let resp = rt.handle(Message::request(Method::GET, "/p/x", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
}

/// The caller's own credentials to this gateway (its `Authorization` and the
/// `rs-auth` cookie) must never be forwarded to the upstream target. With a
/// header-strategy inject (which doesn't touch `Authorization`), the inbound
/// bearer/cookie would otherwise leak straight to the third party.
#[tokio::test]
async fn proxy_strips_caller_credentials_before_forwarding() {
    let http = Arc::new(MockHttp::new());
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_http(http.clone());

    let config = json!({ "mounts": [
        { "path": "/up", "service": "proxy", "config": {
            "access": "open",
            "target": "https://api.upstream.com",
            "secrets": ["apiKey"],
            "inject": { "auth": "header", "name": "X-Api-Key", "value": "secret:apiKey" }
        } }
    ], "secrets": { "apiKey": "up_key_9" } });

    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    );

    let mut req = Message::request(Method::GET, "/up/thing", "t");
    req.set_header("authorization", "Bearer rs2_session_token");
    req.set_header("cookie", "rs-auth=rs2_session_token; other=1");
    let resp = rt.handle(req).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);

    // The caller's bearer and cookie were stripped, not relayed upstream.
    assert_eq!(http.auth.lock().unwrap()[0], None, "caller Authorization leaked upstream");
    assert_eq!(http.cookie.lock().unwrap()[0], None, "caller Cookie leaked upstream");
}
