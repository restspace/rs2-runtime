//! Outbound credential injection (the "proxy adapter" use case): an `httpOut`
//! grant with an `inject` ref attaches operator-supplied auth to the outbound
//! request **host-side**, and the secret never appears in the tenant config.
//! Runs under `--features js` — the guest makes the `fetch` that the (mock)
//! adapter captures.

#![cfg(feature = "js")]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::HttpOut;
use rs2_core::infra::InfraSet;
use rs2_core::message::{Body, MediaType, Message};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// Captures the `authorization` header (and query) of each outbound request.
struct MockHttp {
    auth: Mutex<Vec<Option<String>>>,
    query: Mutex<Vec<String>>,
}

#[async_trait]
impl HttpOut for MockHttp {
    async fn request(&self, msg: Message) -> Result<Message, RsError> {
        self.auth
            .lock()
            .unwrap()
            .push(msg.header("authorization").map(String::from));
        self.query.lock().unwrap().push(msg.url.query.clone());
        Ok(msg.ok_json(&json!({ "ok": true })))
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

const BUNDLE: &str = r#"
    export default async (msg, ctx) => {
        const r = await fetch("https://api.provider.com/v1/send?to=42", { method: "POST" });
        // Surface what the guest can see of its own config — it must not carry
        // the operator secret.
        return { status: 200, body: { ok: (await r.json()).ok, ctx: JSON.stringify(ctx ?? {}) } };
    };
"#;

#[tokio::test]
async fn infra_bearer_is_injected_host_side_and_secret_never_leaks() {
    let http = Arc::new(MockHttp {
        auth: Mutex::new(Vec::new()),
        query: Mutex::new(Vec::new()),
    });
    let dir = tempfile::tempdir().unwrap();

    // The operator infra carries the strategy + secret; `adapter` is required by
    // the infra schema but unused for a credential infra (a self-documenting
    // marker — only `config` is read by the injector).
    let infras = InfraSet::from_json(json!({
        "provider-key": {
            "adapter": "credential",
            "config": { "auth": "bearer", "token": "sk_secret_123" }
        }
    }))
    .unwrap();

    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_http(http.clone())
    .with_infras(infras);

    let version = rs2_core::services::code::version_of(BUNDLE.as_bytes());
    let config = json!({ "mounts": [
        { "path": "/services", "service": "services", "config": { "access": "open" } },
        { "path": "/send", "service": format!("code:send@{version}"), "config": {
            "access": "open",
            "grants": { "fetch": {
                "type": "httpOut",
                "hosts": ["api.provider.com"],
                "inject": "infra:provider-key"
            } }
        } }
    ]});

    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    );

    let deploy =
        Message::request(Method::POST, "/services/code/send/", "t").with_body(Body::from_bytes(
            BUNDLE.as_bytes().to_vec(),
            MediaType::new("application/javascript"),
        ));
    assert_eq!(rt.handle(deploy).await.status, Some(StatusCode::CREATED));

    let mut resp = rt
        .handle(Message::request(Method::GET, "/send/go", "t"))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    let out = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(out["ok"], true);

    // The outbound request carried the operator credential, injected host-side.
    {
        let auth = http.auth.lock().unwrap();
        assert_eq!(auth.len(), 1);
        assert_eq!(auth[0].as_deref(), Some("Bearer sk_secret_123"));
    }

    // The secret never reached the guest's view of its config…
    assert!(
        !out["ctx"].as_str().unwrap().contains("sk_secret_123"),
        "secret leaked into guest ctx: {}",
        out["ctx"]
    );

    // …nor the tenant config round-trip (`GET /services/raw`).
    let mut raw = rt
        .handle(Message::request(Method::GET, "/services/raw", "t"))
        .await;
    assert_eq!(raw.status, Some(StatusCode::OK));
    let raw_text = String::from_utf8_lossy(
        raw.body
            .as_mut()
            .unwrap()
            .materialize(1 << 20)
            .await
            .unwrap(),
    )
    .to_string();
    assert!(
        !raw_text.contains("sk_secret_123"),
        "secret leaked into /services/raw"
    );
}

#[tokio::test]
async fn inline_strategy_draws_token_from_granted_secret() {
    let http = Arc::new(MockHttp {
        auth: Mutex::new(Vec::new()),
        query: Mutex::new(Vec::new()),
    });
    let dir = tempfile::tempdir().unwrap();

    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_http(http.clone());

    let version = rs2_core::services::code::version_of(BUNDLE.as_bytes());
    // Inline query-param strategy with the value drawn from a granted tenant
    // secret (`secret:apiKey`).
    let config = json!({ "mounts": [
        { "path": "/services", "service": "services", "config": { "access": "open" } },
        { "path": "/send", "service": format!("code:send@{version}"), "config": {
            "access": "open",
            "secrets": ["apiKey"],
            "grants": { "fetch": {
                "type": "httpOut",
                "hosts": ["api.provider.com"],
                "inject": { "auth": "query", "name": "api_key", "value": "secret:apiKey" }
            } }
        } }
    ], "secrets": { "apiKey": "tok_live_xyz" } });

    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    );

    let deploy =
        Message::request(Method::POST, "/services/code/send/", "t").with_body(Body::from_bytes(
            BUNDLE.as_bytes().to_vec(),
            MediaType::new("application/javascript"),
        ));
    assert_eq!(rt.handle(deploy).await.status, Some(StatusCode::CREATED));

    let resp = rt
        .handle(Message::request(Method::GET, "/send/go", "t"))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));

    // The secret was spliced into the query string host-side, after the guest's
    // own `?to=42`.
    let query = http.query.lock().unwrap();
    assert_eq!(query.len(), 1);
    assert_eq!(query[0], "to=42&api_key=tok_live_xyz");
}
