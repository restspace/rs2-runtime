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

/// The seed-auth shape: a `store`-pattern wrapper fronting `/data/users` with a
/// conditional GET/PUT pipeline that hashes `password` on write and uses
/// `${url.rest}` for both the addressed key and the directory listing.
#[tokio::test]
async fn wrapper_hashing_store_facade() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/users", "service": "wrapper", "config": {
                "access": "open",
                "pattern": "store",
                "pipeline": { "mode": "conditional", "steps": [
                    { "if": "method == 'GET'",
                      "call": { "method": "GET", "url": "/data/users${url.rest}" } },
                    { "if": "method == 'PUT'", "pipeline": { "mode": "serial", "steps": [
                        { "transform": {
                            "passwordHash": "$hashPassword(password)",
                            "roles": "roles ? roles : 'U'",
                            "kind": "kind ? kind : 'user'"
                        } },
                        { "call": { "method": "PUT", "url": "/data/users${url.rest}",
                                    "effect": "idempotent" } }
                    ]}}
                ]}
            }}
        ]
    });
    let rt = runtime(config, dir.path());

    // PUT a user through the facade: plaintext password is hashed, never stored.
    let put = req(Method::PUT, "/users/ada@example.com")
        .with_json(&json!({ "password": "ada-secret-pw", "roles": "U" }));
    let put_status = rt.handle(put).await.status;
    assert!(
        matches!(put_status, Some(StatusCode::OK) | Some(StatusCode::CREATED)),
        "{put_status:?}"
    );

    // GET the key back: a hashed record, no plaintext `password`.
    let mut got = rt.handle(req(Method::GET, "/users/ada@example.com")).await;
    assert_eq!(got.status, Some(StatusCode::OK), "{:?}", got.body);
    let rec = body_json(&mut got).await;
    assert!(rec.get("password").is_none(), "plaintext must not be stored: {rec}");
    assert!(
        rec["passwordHash"].as_str().unwrap_or("").starts_with("$argon2"),
        "argon2id hash expected: {rec}"
    );
    assert_eq!(rec["roles"], "U");
    assert_eq!(rec["kind"], "user");

    // The listing works via ${url.rest} = "/": GET /users/ → GET /data/users/.
    let list = rt.handle(req(Method::GET, "/users/")).await;
    assert_eq!(list.status, Some(StatusCode::OK), "{:?}", list.body);
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
