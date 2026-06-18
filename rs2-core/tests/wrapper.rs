//! `wrapper` service: one inline pipeline fronting another mount, exact-path
//! passthrough via `${url.rest}`, host-enforced access, and a config-declared
//! discovery pattern.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::Message;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// A fixed in-memory tenant config (only `load_tenant` is needed; the rest of
/// `ConfigLoader` has defaults).
struct FixedLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for FixedLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn runtime(config: serde_json::Value, file_root: &Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(FixedLoader(config)),
        LimitTable::default(),
    )
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body.as_mut().expect("body").as_json(10 * 1024 * 1024).await.expect("json body")
}

/// A wrapper whose inline pipeline forwards to `/data${url.rest}` reproduces the
/// exact request path beyond the mount — sub-paths and a directory's trailing
/// slash alike.
#[tokio::test]
async fn wrapper_forwards_exact_path() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/wrapper", "service": "wrapper", "config": {
                "access": "open",
                "pipeline": ["GET /data${url.rest}"]
            }}
        ]
    });
    let rt = runtime(config, dir.path());

    // Seed a record straight on the wrapped mount.
    let seed = req(Method::PUT, "/data/things/abc").with_json(&json!({ "v": 1 }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED));

    // GET /wrapper/things/abc → /data/things/abc (exact sub-path).
    let mut got = rt.handle(req(Method::GET, "/wrapper/things/abc")).await;
    assert_eq!(got.status, Some(StatusCode::OK), "{:?}", got.body);
    assert_eq!(body_json(&mut got).await["v"], 1);

    // GET /wrapper/things/ → /data/things/ (directory listing — trailing slash kept).
    let list = rt.handle(req(Method::GET, "/wrapper/things/")).await;
    assert_eq!(list.status, Some(StatusCode::OK), "{:?}", list.body);
}

/// The wrapper enforces its own mount `access` at the host: no policy ⇒ denied.
#[tokio::test]
async fn wrapper_without_access_is_denied() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/wrapper", "service": "wrapper", "config": {
                "pipeline": ["GET /data${url.rest}"]
            }}
        ]
    });
    let rt = runtime(config, dir.path());
    let resp = rt.handle(req(Method::GET, "/wrapper/x")).await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED), "{:?}", resp.body);
}

/// A config-declared `pattern`/`facets` surface in the discovery catalogue —
/// so clients treat the wrapper like the mount it fronts.
#[tokio::test]
async fn wrapper_declares_discovery_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/wrapper", "service": "wrapper", "config": {
                "access": "open",
                "pattern": "store",
                "facets": ["schema", "patch"],
                "pipeline": ["GET /data${url.rest}"]
            }}
        ]
    });
    let rt = runtime(config, dir.path());
    let mut resp = rt.handle(req(Method::GET, "/.well-known/rs2/services")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    let doc = body_json(&mut resp).await;
    let wrapper = doc["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["path"] == "/wrapper")
        .expect("wrapper in catalogue");
    assert_eq!(wrapper["pattern"], "store");
    let facets: Vec<&str> =
        wrapper["facets"].as_array().unwrap().iter().filter_map(|v| v.as_str()).collect();
    assert!(facets.contains(&"schema"), "facets: {facets:?}");
}

/// An unknown declared `pattern` is a config error at build time.
#[tokio::test]
async fn wrapper_rejects_unknown_pattern() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/wrapper", "service": "wrapper", "config": {
                "access": "open",
                "pattern": "bogus",
                "pipeline": ["GET /data${url.rest}"]
            }}
        ]
    });
    let rt = runtime(config, dir.path());
    // Tenant build fails ⇒ the request surfaces the config error, not a 200.
    let resp = rt.handle(req(Method::GET, "/wrapper/x")).await;
    assert_ne!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
}
