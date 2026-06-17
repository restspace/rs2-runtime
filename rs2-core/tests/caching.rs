//! Universal caching config (v1's standard `caching` field, host-applied):
//! default `no-store` everywhere, per-mount opt-in with the `public` clamp
//! on non-open mounts, the Set-Cookie carve-out, and conditional-GET 304s
//! from the store services.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Body, MediaType, Message};
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
        "auth": { "jwtSecret": "cache-secret" },
        "mounts": [
            // No caching config: the default posture.
            { "path": "/data", "service": "data" },
            // Openly readable + public cache: the static-site/CDN case.
            { "path": "/assets", "service": "file", "config": {
                "caching": { "mode": "cache", "maxAgeSeconds": 3600,
                             "public": true, "immutable": true } } },
            // Revalidate mode: always fresh, 304s save bandwidth.
            { "path": "/fresh", "service": "data", "config": {
                "caching": { "mode": "revalidate" } } },
            // public requested on an authenticated mount: must clamp.
            { "path": "/private", "service": "file", "config": {
                "access": { "read": "U", "write": "A" },
                "caching": { "mode": "cache", "maxAgeSeconds": 600, "public": true } } },
            // Caching config on the auth mount must not leak onto cookies.
            { "path": "/auth", "service": "auth", "config": {
                "caching": { "mode": "cache", "maxAgeSeconds": 600, "public": true } } }
        ]
    })));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

#[tokio::test]
async fn default_posture_is_no_store_everywhere() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    let put = req(Method::PUT, "/data/things/x").with_json(&json!({ "n": 1 }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));

    // Unconfigured mount.
    let resp = rt.handle(req(Method::GET, "/data/things/x")).await;
    assert_eq!(resp.header("cache-control"), Some("no-store"));

    // Errors.
    let resp = rt.handle(req(Method::GET, "/data/things/missing")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
    assert_eq!(resp.header("cache-control"), Some("no-store"));

    // The generated discovery surface (permission-filtered per caller).
    let resp = rt.handle(req(Method::GET, "/.well-known/rs2/services")).await;
    assert_eq!(resp.header("cache-control"), Some("no-store"));
}

#[tokio::test]
async fn opt_in_modes_render_and_clamp() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    let put = req(Method::PUT, "/assets/site/app.css")
        .with_body(Body::from_string("body{}", MediaType::new("text/css")));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));

    // Open mount + public cache: the full header.
    let resp = rt.handle(req(Method::GET, "/assets/site/app.css")).await;
    assert_eq!(
        resp.header("cache-control"),
        Some("public, max-age=3600, immutable")
    );

    // Revalidate mode: private no-cache + Vary on credentials.
    let put = req(Method::PUT, "/fresh/items/k").with_json(&json!({ "v": 1 }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    let resp = rt.handle(req(Method::GET, "/fresh/items/k")).await;
    assert_eq!(resp.header("cache-control"), Some("private, no-cache"));
    assert!(resp.header("vary").unwrap().contains("authorization"));

    // `public` on an authenticated mount clamps to private + Vary: a
    // shared cache must never serve one principal's response to another.
    let seed = req(Method::PUT, "/data/users/u@t.t").with_json(&json!({
        "passwordHash": hash_password("pw").unwrap(), "roles": "U A"
    }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED));
    let mut login = rt
        .handle(req(Method::POST, "/auth/login").with_json(&json!({
            "email": "u@t.t", "password": "pw"
        })))
        .await;
    let token = login.body.as_mut().unwrap().as_json(65536).await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let put = req(Method::PUT, "/private/doc.txt")
        .with_body(Body::from_string("secret", MediaType::new("text/plain")));
    let mut put = put;
    put.set_header("authorization", &format!("Bearer {token}"));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    let mut get = req(Method::GET, "/private/doc.txt");
    get.set_header("authorization", &format!("Bearer {token}"));
    let resp = rt.handle(get).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.header("cache-control"), Some("private, max-age=600"));
    assert!(resp.header("vary").unwrap().contains("authorization"));
}

#[tokio::test]
async fn cookie_responses_are_never_cacheable() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    let seed = req(Method::PUT, "/data/users/u@t.t").with_json(&json!({
        "passwordHash": hash_password("pw").unwrap(), "roles": "U"
    }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED));

    // The auth mount has an (ill-advised) public caching config; the
    // Set-Cookie carve-out wins.
    let resp = rt
        .handle(req(Method::POST, "/auth/login").with_json(&json!({
            "email": "u@t.t", "password": "pw"
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert!(resp.header("set-cookie").is_some());
    assert_eq!(resp.header("cache-control"), Some("no-store"));
}

#[tokio::test]
async fn conditional_gets_return_304() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    // file: ETag revalidation.
    let put = req(Method::PUT, "/assets/app.js")
        .with_body(Body::from_string("x()", MediaType::new("application/javascript")));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    let first = rt.handle(req(Method::GET, "/assets/app.js")).await;
    let etag = first.header("etag").unwrap().to_string();
    let mut revalidate = req(Method::GET, "/assets/app.js");
    revalidate.set_header("if-none-match", &etag);
    let resp = rt.handle(revalidate).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_MODIFIED));
    assert!(resp.body.is_none(), "304 carries no body");
    assert_eq!(resp.header("etag").unwrap(), etag);

    // A stale validator gets the full response again.
    let mut stale = req(Method::GET, "/assets/app.js");
    stale.set_header("if-none-match", "\"deadbeef\"");
    assert_eq!(rt.handle(stale).await.status, Some(StatusCode::OK));

    // data: content-hash ETag revalidation, weak/list forms accepted.
    let put = req(Method::PUT, "/fresh/items/r").with_json(&json!({ "v": 1 }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    let first = rt.handle(req(Method::GET, "/fresh/items/r")).await;
    let etag = first.header("etag").unwrap().to_string();
    let mut revalidate = req(Method::GET, "/fresh/items/r");
    revalidate.set_header("if-none-match", &format!("\"other\", W/{etag}"));
    let resp = rt.handle(revalidate).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_MODIFIED));

    // The record changing invalidates the validator.
    let put = req(Method::PUT, "/fresh/items/r").with_json(&json!({ "v": 2 }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::OK));
    let mut revalidate = req(Method::GET, "/fresh/items/r");
    revalidate.set_header("if-none-match", &etag);
    assert_eq!(rt.handle(revalidate).await.status, Some(StatusCode::OK));
}
