//! `$response` shaping end-to-end: a transform whose output is
//! `{"$response": {...}}` sets response status/headers/mediaType/body;
//! captured transforms and plain outputs keep today's behavior.

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

fn runtime(dir: &std::path::Path) -> Arc<Runtime> {
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(dir)), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/pipe", "service": "pipeline",
          "config": { "access": { "invoke": "all", "write": "A" } } }
    ]})));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
}

async fn author(rt: &Runtime, pipeline: serde_json::Value) {
    let put = as_admin(req(Method::PUT, "/pipe/.pipelines/.root"))
        .with_json(&json!({ "pipeline": pipeline }));
    let resp = rt.handle(put).await;
    assert!(
        matches!(resp.status, Some(StatusCode::CREATED) | Some(StatusCode::OK)),
        "author: {:?}",
        resp.status
    );
}

#[tokio::test]
async fn envelope_sets_status_headers_and_media_type() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    author(
        &rt,
        json!([{
            "$response": {
                "status": "201",
                "headers": { "Location": "'/things/1'" },
                "mediaType": "'application/hal+json'",
                "body": { "made": "true" }
            }
        }]),
    )
    .await;

    let mut resp = rt.handle(req(Method::POST, "/pipe").with_json(&json!({}))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "status from envelope: {:?}", resp.body);
    assert_eq!(resp.header("location"), Some("/things/1"));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "application/hal+json",
        "mediaType applied"
    );
    assert_eq!(body_json(&mut resp).await["made"], true);
}

#[tokio::test]
async fn string_body_is_raw_text_and_plain_objects_stay_200() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());

    // v1 `to-text`: JSON in, text/plain out.
    author(&rt, json!([{ "$response": { "body": "'rendered text'" } }])).await;
    let mut resp = rt.handle(req(Method::POST, "/pipe").with_json(&json!({}))).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.body.as_ref().unwrap().media_type.essence(), "text/plain");
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"rendered text");

    // A plain transform output is unaffected: body + forced 200.
    author(&rt, json!([{ "shaped": "false" }])).await;
    let mut resp = rt.handle(req(Method::POST, "/pipe").with_json(&json!({}))).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(body_json(&mut resp).await["shaped"], false);
}

#[tokio::test]
async fn captured_envelopes_are_data_not_directives() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    // Typed steps: capture the envelope, then emit plain data. If capture
    // applied the directive, the response would be the envelope's 500.
    author(
        &rt,
        json!({ "steps": [
            { "transform": { "$response": { "status": "500" } }, "as": "$env" },
            { "transform": { "captured": "$exists($env)" } }
        ]}),
    )
    .await;

    let mut resp = rt.handle(req(Method::POST, "/pipe").with_json(&json!({}))).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "capture path shapes nothing: {:?}", resp.body);
    assert_eq!(body_json(&mut resp).await["captured"], true);
}

#[tokio::test]
async fn invalid_status_is_a_structured_400() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());
    author(&rt, json!([{ "$response": { "status": "1000" } }])).await;

    let resp = rt.handle(req(Method::POST, "/pipe").with_json(&json!({}))).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST), "bad status surfaces: {:?}", resp.body);
}
