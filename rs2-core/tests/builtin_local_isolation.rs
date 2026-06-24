//! Per-mount isolation for the built-in `local` **file** adapter (the core
//! production factory wired in `Adapters::new`). Two `file` mounts selecting
//! `builtin:local` with different `store.root`s must not collide on identical
//! keys (e.g. `index.html`); a missing/unsafe `root` is a config error.
//!
//! The data-side equivalent (`builtin:file`) lives in `file_data_store.rs`.

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

/// A runtime over `mounts`, with the node default file/data stores rooted at
/// `file_root`. `builtin:local` uses the core production factory (no test
/// override), so this exercises the real wiring.
fn runtime(file_root: &std::path::Path, mounts: serde_json::Value) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": mounts })));
    Runtime::new(
        Tenancy::Single { tenant: "t1".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

fn put_html(path: &str, body: &str) -> Message {
    Message::request(Method::PUT, path, "t1")
        .with_body(Body::from_string(body.to_string(), MediaType::parse("text/html")))
}

async fn get_text(rt: &Runtime, path: &str) -> (Option<StatusCode>, String) {
    let mut resp = rt.handle(Message::request(Method::GET, path, "t1")).await;
    let text = match resp.body.as_mut() {
        Some(b) => String::from_utf8(b.materialize(65536).await.unwrap().to_vec()).unwrap(),
        None => String::new(),
    };
    (resp.status, text)
}

/// Two `file` mounts with different `builtin:local` roots are isolated on the
/// same key — neither overwrites the other's `index.html`.
#[tokio::test]
async fn builtin_local_file_mounts_with_different_roots_are_isolated() {
    let files = tempfile::tempdir().unwrap();
    let rt = runtime(
        files.path(),
        json!([
            { "path": "/html", "service": "file",
              "config": { "access": "open", "store": { "adapter": "builtin:local", "root": ".rs2-html" } } },
            { "path": "/docs", "service": "file",
              "config": { "access": "open", "store": { "adapter": "builtin:local", "root": ".rs2-docs" } } }
        ]),
    );

    assert_eq!(
        rt.handle(put_html("/html/index.html", "<h1>site</h1>")).await.status,
        Some(StatusCode::CREATED)
    );
    assert_eq!(
        rt.handle(put_html("/docs/index.html", "<h1>docs</h1>")).await.status,
        Some(StatusCode::CREATED)
    );

    // Each mount reads back only its own content.
    assert_eq!(get_text(&rt, "/html/index.html").await.1, "<h1>site</h1>");
    assert_eq!(get_text(&rt, "/docs/index.html").await.1, "<h1>docs</h1>");
}

/// An explicit `builtin:local` store with no/unsafe `root` fails to build, so
/// the mount never serves (a 400 at tenant build time, not a silent share).
#[tokio::test]
async fn unsafe_or_missing_root_is_rejected_for_builtin_local() {
    let files = tempfile::tempdir().unwrap();
    for bad in [
        json!({ "adapter": "builtin:local" }),
        json!({ "adapter": "builtin:local", "root": "" }),
        json!({ "adapter": "builtin:local", "root": "../escape" }),
        json!({ "adapter": "builtin:local", "root": "C:\\abs" }),
    ] {
        let rt = runtime(
            files.path(),
            json!([{ "path": "/f", "service": "file", "config": { "access": "open", "store": bad } }]),
        );
        let (status, _) = get_text(&rt, "/f/index.html").await;
        assert_ne!(status, Some(StatusCode::OK), "unsafe root unexpectedly built a working mount");
    }
}
