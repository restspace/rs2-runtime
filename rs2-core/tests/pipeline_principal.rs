//! Principal exposure to pipeline specs (the migration "Route C"): the
//! original caller is bound into pipeline vars as `_user`/`principal`, so
//! transforms see `$_user.accountId` (extra JWT claims included) and step
//! URLs interpolate `${_user.accountId}`. Anonymous callers get no binding
//! at all — specs must guard with defaults.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Message, Principal};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
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

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

/// Attach a principal carrying an `accountId` extra claim.
fn as_user(mut msg: Message, email: &str, account: &str) -> Message {
    let mut extra = serde_json::Map::new();
    extra.insert("accountId".into(), json!(account));
    msg.principal = Some(Principal {
        id: email.into(),
        roles: vec!["U".into()],
        kind: "user".into(),
        extra,
    });
    msg
}

fn as_admin(mut msg: Message) -> Message {
    msg.principal = Some(Principal {
        id: "admin-1".into(),
        roles: vec!["A".into()],
        kind: "user".into(),
        extra: Default::default(),
    });
    msg
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body.as_mut().expect("body").as_json(1024 * 1024).await.expect("json body")
}

fn runtime(dir: &std::path::Path, mounts: serde_json::Value) -> Arc<Runtime> {
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(dir)), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": mounts })));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
}

#[tokio::test]
async fn transforms_see_the_caller_as_dollar_user() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(
        dir.path(),
        json!([{ "path": "/pipe", "service": "pipeline",
                 "config": { "access": { "invoke": "all", "write": "A" } } }]),
    );

    let author = as_admin(req(Method::PUT, "/pipe/.pipelines/.root")).with_json(&json!({
        "pipeline": [
            // In the DSL an object step IS the transform template.
            {
                "account": "$_user.accountId",
                "email": "$_user.email",
                "viaPrincipal": "$principal.accountId"
            }
        ]
    }));
    assert_eq!(rt.handle(author).await.status, Some(StatusCode::CREATED), "author");

    let call = as_user(req(Method::POST, "/pipe"), "ada@example.com", "acc-42")
        .with_json(&json!({ "anything": true }));
    let mut resp = rt.handle(call).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "run: {:?}", resp.body);
    let body = body_json(&mut resp).await;
    assert_eq!(body["account"], "acc-42", "extra claim flows into $_user");
    assert_eq!(body["email"], "ada@example.com", "principal id binds as email");
    assert_eq!(body["viaPrincipal"], "acc-42", "also bound under $principal");
}

#[tokio::test]
async fn step_urls_interpolate_user_fields() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(
        dir.path(),
        json!([
            { "path": "/store", "service": "data",
              "config": { "access": { "read": "U", "write": "A" } } },
            { "path": "/pipe", "service": "pipeline",
              "config": { "access": { "invoke": "all", "write": "A" } } }
        ]),
    );

    let seed = as_admin(req(Method::PUT, "/store/things/acc-42_p1")).with_json(&json!({ "v": 7 }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED), "seed");

    // The compound-key pattern the Atelyr migration relies on.
    let author = as_admin(req(Method::PUT, "/pipe/.pipelines/.root"))
        .with_json(&json!({ "pipeline": ["GET /store/things/${_user.accountId}_p1"] }));
    assert_eq!(rt.handle(author).await.status, Some(StatusCode::CREATED), "author");

    let mut resp = rt.handle(as_user(req(Method::GET, "/pipe"), "ada@example.com", "acc-42")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "run: {:?}", resp.body);
    assert_eq!(body_json(&mut resp).await["v"], 7, "URL interpolated the extra claim");
}

#[tokio::test]
async fn the_triggering_url_binds_as_dollar_url() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(
        dir.path(),
        json!([{ "path": "/pipe", "service": "pipeline",
                 "config": { "access": { "invoke": "all", "write": "A" } } }]),
    );

    let author = as_admin(req(Method::PUT, "/pipe/.pipelines/.root")).with_json(&json!({
        "pipeline": [ { "first": "$_url.path[0]", "second": "$_url.path[1]",
                        "q": "$_url.query.q" } ]
    }));
    assert_eq!(rt.handle(author).await.status, Some(StatusCode::CREATED), "author");

    let call = req(Method::POST, "/pipe/report/blocks?q=hi").with_json(&json!({}));
    let mut resp = rt.handle(call).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "run: {:?}", resp.body);
    let body = body_json(&mut resp).await;
    assert_eq!(body["first"], "report", "path segment 0: {body}");
    assert_eq!(body["second"], "blocks", "path segment 1");
    assert_eq!(body["q"], "hi", "query param");
}

#[tokio::test]
async fn anonymous_callers_have_no_user_binding() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(
        dir.path(),
        json!([{ "path": "/pipe", "service": "pipeline",
                 "config": { "access": { "invoke": "all", "write": "A" } } }]),
    );

    let author = as_admin(req(Method::PUT, "/pipe/.pipelines/.root")).with_json(&json!({
        "pipeline": [ { "account": "$_user.accountId", "ok": "true" } ]
    }));
    assert_eq!(rt.handle(author).await.status, Some(StatusCode::CREATED), "author");

    let mut resp = rt.handle(req(Method::POST, "/pipe").with_json(&json!({}))).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "no 500 for anonymous: {:?}", resp.body);
    let body = body_json(&mut resp).await;
    assert_eq!(body["ok"], true);
    assert!(
        body.get("account").map(|v| v.is_null()).unwrap_or(true),
        "unbound $_user yields no value, not a phantom one: {body}"
    );
}
