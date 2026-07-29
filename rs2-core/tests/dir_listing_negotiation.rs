//! Accept-negotiated directory listings on the `file` service. On a static-site
//! mount (`defaultResource` set) a directory GET serves the default doc — which
//! shadows the `dir+json` listing the discovery surface needs. A deliberate
//! `Accept: application/vnd.rs2.dir+json` opts into the listing at the same URL;
//! browsers (`*/*`) and humans still get the default doc. `listings: false`
//! remains a hard suppression the negotiation cannot override for any ordinary
//! caller — tenant operators excepted, since they can toggle the flag anyway
//! (their listing is `no-store`, so it can't leak via a shared cache) — and every
//! directory-GET response carries `Vary: Accept` for cache correctness.

use std::sync::Arc;

use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Body, MediaType, Message, Principal};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

const DIR_JSON: &str = "application/vnd.rs2.dir+json";

struct StaticLoader(serde_json::Value);

#[async_trait::async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

/// `/site` — static-site mount (default doc, listings on).
/// `/locked` — static-site mount with listings suppressed, and the public
/// caching a real anonymously-readable site would carry (the case where an
/// operator-only listing must not reach a shared cache).
fn test_runtime(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({
        "operatorRoles": "op",
        "mounts": [
            { "path": "/site", "service": "file", "config": {
                "access": "open", "defaultResource": "index.html" } },
            { "path": "/locked", "service": "file", "config": {
                "access": "open", "defaultResource": "index.html", "listings": false,
                "caching": { "mode": "cache", "maxAgeSeconds": 300, "public": true } } }
        ]
    })));
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

fn req_accept(method: Method, path: &str, accept: &str) -> Message {
    let mut m = Message::request(method, path, "t1");
    m.set_header("accept", accept);
    m
}

fn as_role(mut msg: Message, role: &str) -> Message {
    msg.principal = Some(Principal {
        id: format!("{role}-1"),
        roles: vec![role.to_string()],
        kind: "user".into(),
        extra: Default::default(),
    });
    msg
}

async fn put_ok(rt: &Runtime, path: &str, content: &str, mt: &str) {
    let m = req(Method::PUT, path).with_body(Body::from_string(content, MediaType::new(mt)));
    assert_eq!(
        rt.handle(m).await.status,
        Some(StatusCode::CREATED),
        "{path}"
    );
}

async fn body_of(resp: &mut Message) -> Vec<u8> {
    resp.body
        .as_mut()
        .unwrap()
        .materialize(65536)
        .await
        .unwrap()
        .to_vec()
}

#[tokio::test]
async fn explicit_dir_json_accept_forces_listing() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/index.html", "<html>home</html>", "text/html").await;
    put_ok(&rt, "/site/data.json", "{}", "application/json").await;

    // Explicit dir+json → the listing, not the default doc.
    let mut resp = rt.handle(req_accept(Method::GET, "/site/", DIR_JSON)).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.body.as_ref().unwrap().media_type.essence(), DIR_JSON);
    assert_eq!(resp.header("vary"), Some("accept"));
    let listing: serde_json::Value = serde_json::from_slice(&body_of(&mut resp).await).unwrap();
    let names: Vec<&str> = listing["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"index.html") && names.contains(&"data.json"),
        "{names:?}"
    );
}

#[tokio::test]
async fn no_accept_serves_default_doc_with_vary() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/index.html", "<html>home</html>", "text/html").await;

    let mut resp = rt.handle(req(Method::GET, "/site/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "text/html"
    );
    // Negotiation is in play here, so the default-doc response advertises Vary too.
    assert_eq!(resp.header("vary"), Some("accept"));
    assert_eq!(&body_of(&mut resp).await[..], b"<html>home</html>");
}

#[tokio::test]
async fn browser_wildcard_accept_serves_default_doc() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/index.html", "<html>home</html>", "text/html").await;

    // A browser's Accept includes `*/*` — which must NOT be read as a listing request.
    let mut resp = rt
        .handle(req_accept(
            Method::GET,
            "/site/",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "text/html"
    );
    assert_eq!(&body_of(&mut resp).await[..], b"<html>home</html>");
}

#[tokio::test]
async fn listings_false_suppresses_even_explicit_dir_json() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/locked/index.html", "<html>home</html>", "text/html").await;
    put_ok(&rt, "/locked/secret.json", "{}", "application/json").await;

    // Anonymous: negotiation cannot bypass the suppression.
    let resp = rt
        .handle(req_accept(Method::GET, "/locked/", DIR_JSON))
        .await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));

    // Nor can a non-operator role, however privileged in the mount's `access`.
    let resp = rt
        .handle(as_role(
            req_accept(Method::GET, "/locked/", DIR_JSON),
            "editor",
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));

    // The default doc is still served to a normal request.
    let normal = rt.handle(req(Method::GET, "/locked/")).await;
    assert_eq!(normal.status, Some(StatusCode::OK));
}

#[tokio::test]
async fn operator_sees_listing_through_listings_false() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/locked/index.html", "<html>home</html>", "text/html").await;
    put_ok(&rt, "/locked/secret.json", "{}", "application/json").await;

    // `listings: false` is concealment, not authorization: an operator can flip
    // the flag in tenant config anyway, so they get the listing on request.
    let mut resp = rt
        .handle(as_role(req_accept(Method::GET, "/locked/", DIR_JSON), "op"))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.body.as_ref().unwrap().media_type.essence(), DIR_JSON);

    // The representation is principal-dependent, and this mount is publicly
    // cacheable — the operator's listing must never enter a shared cache.
    assert_eq!(resp.header("cache-control"), Some("no-store"));
    let vary = resp.header("vary").unwrap().to_string();
    assert!(vary.contains("accept"), "{vary}");
    assert!(vary.contains("authorization") && vary.contains("cookie"), "{vary}");

    let listing: serde_json::Value = serde_json::from_slice(&body_of(&mut resp).await).unwrap();
    let names: Vec<&str> = listing["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"secret.json"), "{names:?}");
}

#[tokio::test]
async fn operator_browsing_a_suppressed_site_still_gets_the_default_doc() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/locked/index.html", "<html>home</html>", "text/html").await;

    // The bypass sits at the `listings` gate, below the default-resource
    // branch — an operator with a browser still sees the site, not a listing.
    let mut resp = rt
        .handle(as_role(
            req_accept(
                Method::GET,
                "/locked/",
                "text/html,application/xhtml+xml,*/*;q=0.8",
            ),
            "op",
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "text/html"
    );
    assert_eq!(&body_of(&mut resp).await[..], b"<html>home</html>");
}

#[tokio::test]
async fn listing_available_at_subdirs_too() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/docs/a.md", "# a", "text/markdown").await;
    put_ok(&rt, "/site/docs/b.md", "# b", "text/markdown").await;

    let mut resp = rt
        .handle(req_accept(Method::GET, "/site/docs/", DIR_JSON))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.header("x-total-count"), Some("2"));
    let listing: serde_json::Value = serde_json::from_slice(&body_of(&mut resp).await).unwrap();
    assert_eq!(listing["total"], 2);
}
