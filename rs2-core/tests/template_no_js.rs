//! Without the JS engine, a `template` mount can't render JSX, so the tenant
//! refuses to build it — surfaced as `501 Engine Unavailable` rather than a
//! confusing runtime failure later. (The rendering path itself is covered by
//! `tests/template.rs`, which only compiles `--features js`.)

#![cfg(not(feature = "js"))]

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::Message;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct StaticLoader(Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

#[tokio::test]
async fn template_mount_without_js_is_engine_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(dir.path())), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({
        "mounts": [{ "path": "/render", "service": "template" }]
    })));
    let rt =
        Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());

    let resp = rt.handle(Message::request(Method::GET, "/render/welcome", "t")).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::NOT_IMPLEMENTED),
        "a template mount needs --features js: {:?}",
        resp.body
    );
}
