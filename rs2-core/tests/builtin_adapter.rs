//! The built-in adapter registry: `store.adapter = "builtin:<name>"` selects a
//! built-in Rust backend, the sibling of the JS `code:<name>@<version>` path.
//! Runs under default features — the builtin path never touches the resident
//! engine, so this works without `--features js`.

use std::sync::Arc;

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

struct StaticLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn runtime_with(mounts: serde_json::Value, file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": mounts })));
    Runtime::new(
        Tenancy::Single {
            tenant: "t1".into(),
        },
        adapters,
        loader,
        LimitTable::default(),
    )
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t1")
}

/// Read back an error response body as text (problem+json), for substring
/// assertions that don't depend on the exact field layout.
async fn error_text(resp: &mut Message) -> String {
    let bytes = resp
        .body
        .as_mut()
        .unwrap()
        .materialize(65536)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).to_string()
}

#[tokio::test]
async fn builtin_mem_is_a_shared_store_independent_of_the_default() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(
        json!([
            { "path": "/d", "service": "data", "config": { "access": "open" } },
            { "path": "/a", "service": "data", "config": { "access": "open", "store": { "adapter": "builtin:mem" } } },
            { "path": "/b", "service": "data", "config": { "access": "open", "store": { "adapter": "builtin:mem" } } }
        ]),
        dir.path(),
    );

    // Two `builtin:mem` mounts resolve to one shared in-memory store.
    let resp = rt
        .handle(req(Method::PUT, "/a/things/x").with_json(&json!({ "v": 1 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    let mut resp = rt.handle(req(Method::GET, "/b/things/x")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_mut().unwrap().as_json(65536).await.unwrap()["v"],
        1
    );

    // ...but `builtin:mem` is its own store, distinct from the node default —
    // the default no longer aliases an in-memory store.
    let resp = rt.handle(req(Method::GET, "/d/things/x")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn builtin_local_file_store_matches_default() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(
        json!([
            { "path": "/fd", "service": "file", "config": { "access": "open" } },
            { "path": "/fb", "service": "file", "config": { "access": "open", "store": { "adapter": "builtin:local" } } }
        ]),
        dir.path(),
    );

    let resp = rt
        .handle(
            req(Method::PUT, "/fb/docs/a.txt")
                .with_body(Body::from_string("alpha", MediaType::new("text/plain"))),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));

    let mut resp = rt.handle(req(Method::GET, "/fd/docs/a.txt")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(
        &bytes[..],
        b"alpha",
        "builtin:local must share the default file store"
    );
}

#[tokio::test]
async fn unknown_builtin_name_is_a_config_400() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(
        json!([
            { "path": "/d", "service": "data", "config": { "store": { "adapter": "builtin:nope" } } }
        ]),
        dir.path(),
    );

    let mut resp = rt.handle(req(Method::GET, "/d/things/x")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let text = error_text(&mut resp).await;
    assert!(
        text.contains("builtin:nope"),
        "names the bad adapter: {text}"
    );
    assert!(
        text.contains("mem"),
        "lists available data adapters: {text}"
    );
}

#[tokio::test]
async fn wrong_kind_builtin_name_is_a_config_400() {
    let dir = tempfile::tempdir().unwrap();
    // `local` is a file adapter; on a data mount it isn't in the data registry.
    let rt = runtime_with(
        json!([
            { "path": "/d", "service": "data", "config": { "store": { "adapter": "builtin:local" } } }
        ]),
        dir.path(),
    );

    let mut resp = rt.handle(req(Method::GET, "/d/things/x")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let text = error_text(&mut resp).await;
    assert!(
        text.contains("builtin:local"),
        "names the wrong-kind adapter: {text}"
    );
    assert!(
        text.contains("mem"),
        "lists only data adapters, not 'local': {text}"
    );
}

#[tokio::test]
async fn malformed_adapter_ref_is_a_config_400() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime_with(
        json!([
            { "path": "/d", "service": "data", "config": { "store": { "adapter": "redis" } } }
        ]),
        dir.path(),
    );

    let mut resp = rt.handle(req(Method::GET, "/d/things/x")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let text = error_text(&mut resp).await;
    assert!(
        text.contains("builtin:") && text.contains("code:"),
        "explains the two schemes: {text}"
    );
}
