//! CORS + trusted-origin cookies (v1's runtime-enforced `trustedDomains`
//! posture, host-owned in RS2): preflights answered before routing,
//! credentialed CORS for trusted origins, the cookie-CSRF guard, and the
//! auth service's cookie-attribute matrix.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::Message;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::services::auth::hash_password;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct StaticLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn rt(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({
        "auth": { "jwtSecret": "cors-secret",
                  "allowedLoginOrigins": ["https://app.acme.test", "*.acme.dev"] },
        "cors": {
            "trustedOrigins": ["https://app.acme.test", "*.acme.dev"],
            "allowedOrigins": ["https://reader.example"]
        },
        "mounts": [
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/auth", "service": "auth", "config": { "access": "open" } }
        ]
    })));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

const HOST: &str = "api.acme.test";

fn req(method: Method, path: &str, origin: Option<&str>) -> Message {
    let mut msg = Message::request(method, path, "t");
    msg.set_header("host", HOST);
    if let Some(origin) = origin {
        msg.set_header("origin", origin);
    }
    msg
}

async fn seed_user(rt: &Runtime) {
    let put = Message::request(Method::PUT, "/data/users/ada@t.test", "t").with_json(&json!({
        "passwordHash": hash_password("pw").unwrap(), "roles": "A"
    }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
}

fn login_body() -> serde_json::Value {
    json!({ "email": "ada@t.test", "password": "pw" })
}

#[tokio::test]
async fn preflights_answer_before_routing() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    // Trusted origin: 204 with credentialed CORS, never reaching a service.
    let mut preflight = req(
        Method::OPTIONS,
        "/data/things",
        Some("https://app.acme.test"),
    );
    preflight.set_header("access-control-request-method", "PUT");
    preflight.set_header("access-control-request-headers", "content-type, if-match");
    let resp = rt.handle(preflight).await;
    assert_eq!(resp.status, Some(StatusCode::NO_CONTENT));
    assert_eq!(
        resp.header("access-control-allow-origin"),
        Some("https://app.acme.test")
    );
    assert_eq!(
        resp.header("access-control-allow-credentials"),
        Some("true")
    );
    assert_eq!(resp.header("access-control-allow-methods"), Some("PUT"));
    assert_eq!(
        resp.header("access-control-allow-headers"),
        Some("content-type, if-match")
    );

    // Allowed (non-trusted) origin: CORS yes, credentials no.
    let mut preflight = req(
        Method::OPTIONS,
        "/data/things",
        Some("https://reader.example"),
    );
    preflight.set_header("access-control-request-method", "GET");
    let resp = rt.handle(preflight).await;
    assert_eq!(resp.status, Some(StatusCode::NO_CONTENT));
    assert_eq!(
        resp.header("access-control-allow-origin"),
        Some("https://reader.example")
    );
    assert_eq!(resp.header("access-control-allow-credentials"), None);

    // Unlisted origin: not answered as a preflight, no CORS headers.
    let mut preflight = req(
        Method::OPTIONS,
        "/data/things",
        Some("https://evil.example"),
    );
    preflight.set_header("access-control-request-method", "GET");
    let resp = rt.handle(preflight).await;
    assert_eq!(resp.header("access-control-allow-origin"), None);
    assert_ne!(resp.status, Some(StatusCode::NO_CONTENT));
}

#[tokio::test]
async fn responses_and_errors_carry_cors_for_permitted_origins() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    // Success path, allowed origin: origin echo + exposed headers, no credentials.
    let put = req(Method::PUT, "/data/things/x", None).with_json(&json!({ "n": 1 }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    let resp = rt
        .handle(req(
            Method::GET,
            "/data/things/x",
            Some("https://reader.example"),
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.header("access-control-allow-origin"),
        Some("https://reader.example")
    );
    assert!(resp
        .header("access-control-expose-headers")
        .unwrap()
        .contains("x-total-count"));
    assert_eq!(resp.header("access-control-allow-credentials"), None);

    // Error responses are decorated too (browsers can't read undecorated errors).
    let resp = rt
        .handle(req(
            Method::GET,
            "/data/things/nope",
            Some("https://x.acme.dev"),
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
    assert_eq!(
        resp.header("access-control-allow-origin"),
        Some("https://x.acme.dev")
    );
    assert_eq!(
        resp.header("access-control-allow-credentials"),
        Some("true")
    );

    // Unlisted origin: no CORS headers at all.
    let resp = rt
        .handle(req(
            Method::GET,
            "/data/things/x",
            Some("https://evil.example"),
        ))
        .await;
    assert_eq!(resp.header("access-control-allow-origin"), None);

    // Same-origin browser request: CORS not involved.
    let resp = rt
        .handle(req(
            Method::GET,
            "/data/things/x",
            Some(&format!("https://{HOST}")),
        ))
        .await;
    assert_eq!(resp.header("access-control-allow-origin"), None);
}

#[tokio::test]
async fn csrf_guard_blocks_untrusted_cookie_writes() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    // Cookie-authenticated unsafe request from an untrusted origin → 403
    // before routing.
    let mut evil = req(Method::PUT, "/data/things/x", Some("https://evil.example"))
        .with_json(&json!({ "n": 1 }));
    evil.set_header("cookie", "rs-auth=whatever");
    let resp = rt.handle(evil).await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN));

    // The same write with a bearer header instead is not CSRF-able → routed.
    let mut bearer = req(Method::PUT, "/data/things/x", Some("https://evil.example"))
        .with_json(&json!({ "n": 1 }));
    bearer.set_header("authorization", "Bearer junk");
    let resp = rt.handle(bearer).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::UNAUTHORIZED),
        "fails on the bad token, not CSRF"
    );

    // Trusted origin with a cookie is allowed through the guard.
    let mut trusted = req(Method::PUT, "/data/things/x", Some("https://app.acme.test"))
        .with_json(&json!({ "n": 1 }));
    trusted.set_header("cookie", "other=1"); // no rs-auth: also fine
    assert_eq!(rt.handle(trusted).await.status, Some(StatusCode::CREATED));
}

#[tokio::test]
async fn auth_cookie_attributes_follow_origin_trust() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    seed_user(&rt).await;

    // Non-browser (no Origin): SameSite=Strict cookie.
    let resp = rt
        .handle(req(Method::POST, "/auth/login", None).with_json(&login_body()))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let cookie = resp.header("set-cookie").unwrap();
    assert!(cookie.contains("SameSite=Strict"), "{cookie}");

    // Same-origin browser: SameSite=Strict.
    let resp = rt
        .handle(
            req(
                Method::POST,
                "/auth/login",
                Some(&format!("https://{HOST}")),
            )
            .with_json(&login_body()),
        )
        .await;
    assert!(resp
        .header("set-cookie")
        .unwrap()
        .contains("SameSite=Strict"));

    // Trusted cross-origin: SameSite=None; Secure (required for cross-site).
    let resp = rt
        .handle(
            req(Method::POST, "/auth/login", Some("https://app.acme.test"))
                .with_json(&login_body()),
        )
        .await;
    let cookie = resp.header("set-cookie").unwrap();
    assert!(
        cookie.contains("SameSite=None") && cookie.contains("Secure"),
        "{cookie}"
    );

    // An origin outside allowedLoginOrigins may not log in at all (v1's
    // `allowedLoginDomains`); the untrusted-but-allowed no-cookie path is
    // covered by `untrusted_origin_login_is_bearer_only` below.
    let mut resp = rt
        .handle(
            req(Method::POST, "/auth/login", Some("https://reader.example"))
                .with_json(&login_body()),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN));
    let problem = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(problem["code"], "forbidden");
}

/// With no login allowlist configured, an untrusted origin may log in but
/// receives no cookie — the body token is the credential (v1's `_jwt`).
#[tokio::test]
async fn untrusted_origin_login_is_bearer_only() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({
        "auth": { "jwtSecret": "cors-secret" },   // no allowedLoginOrigins
        "cors": { "allowedOrigins": ["*"] },      // CORS-readable from anywhere
        "mounts": [
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/auth", "service": "auth", "config": { "access": "open" } }
        ]
    })));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );
    seed_user(&rt).await;

    let mut resp = rt
        .handle(
            req(
                Method::POST,
                "/auth/login",
                Some("https://anywhere.example"),
            )
            .with_json(&login_body()),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.header("set-cookie"),
        None,
        "no cookie for untrusted origins"
    );
    let body = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert!(
        body["token"].as_str().is_some(),
        "body token is the credential"
    );
    assert_eq!(
        resp.header("access-control-allow-origin"),
        Some("https://anywhere.example"),
        "response is CORS-readable"
    );
}
