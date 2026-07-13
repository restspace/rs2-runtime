//! Integration tests: the full dispatch path (router → wrapper → service)
//! against the `file` and `data` services with built-in adapters (PRD G4).

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

fn test_runtime(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({
        "mounts": [
            { "path": "/files", "service": "file", "config": { "access": "open" } },
            { "path": "/data", "service": "data", "config": { "enforceSchema": true, "access": "open" } },
            { "path": "/private", "service": "file", "config": { "access": "authenticated" } }
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

#[tokio::test]
async fn file_write_read_list_delete() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());

    // PUT creates → 201
    let resp = rt
        .handle(
            req(Method::PUT, "/files/docs/a.txt")
                .with_body(Body::from_string("alpha", MediaType::new("text/plain"))),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));

    // PUT overwrites → 200
    let resp = rt
        .handle(
            req(Method::PUT, "/files/docs/a.txt")
                .with_body(Body::from_string("alpha2", MediaType::new("text/plain"))),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));

    // GET streams back with ETag
    let mut resp = rt.handle(req(Method::GET, "/files/docs/a.txt")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert!(resp.header("etag").is_some());
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"alpha2");

    // Range read → 206 partial
    let mut range_req = req(Method::GET, "/files/docs/a.txt");
    range_req.set_header("range", "bytes=0-2");
    let mut resp = rt.handle(range_req).await;
    assert_eq!(resp.status, Some(StatusCode::PARTIAL_CONTENT));
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"alp");

    // Directory listing is paginated dir+json
    rt.handle(
        req(Method::PUT, "/files/docs/b.txt")
            .with_body(Body::from_string("beta", MediaType::new("text/plain"))),
    )
    .await;
    let mut resp = rt
        .handle(req(Method::GET, "/files/docs/?$take=1&$skip=1"))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "application/vnd.rs2.dir+json"
    );
    let listing = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(listing["total"], 2);
    assert_eq!(listing["entries"].as_array().unwrap().len(), 1);
    assert_eq!(listing["entries"][0]["name"], "b.txt");

    // DELETE file then empty dir
    assert_eq!(
        rt.handle(req(Method::DELETE, "/files/docs/a.txt"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::DELETE, "/files/docs/b.txt"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::DELETE, "/files/docs/")).await.status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::GET, "/files/docs/a.txt"))
            .await
            .status,
        Some(StatusCode::NOT_FOUND)
    );
}

#[tokio::test]
async fn file_move_renames_and_lists_content_type() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    let mv = || Method::from_bytes(b"MOVE").unwrap();

    rt.handle(
        req(Method::PUT, "/files/a.txt")
            .with_body(Body::from_string("x", MediaType::new("text/plain"))),
    )
    .await;

    // MOVE to a fresh name: created → 201, Location points at the new path.
    let mut m = req(mv(), "/files/a.txt");
    m.set_header("destination", "/files/b.txt");
    let resp = rt.handle(m).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    assert_eq!(resp.header("location"), Some("/files/b.txt"));

    // Source gone, destination readable.
    assert_eq!(
        rt.handle(req(Method::GET, "/files/a.txt")).await.status,
        Some(StatusCode::NOT_FOUND)
    );
    let mut got = rt.handle(req(Method::GET, "/files/b.txt")).await;
    assert_eq!(got.status, Some(StatusCode::OK));
    assert_eq!(
        &got.body.as_mut().unwrap().materialize(16).await.unwrap()[..],
        b"x"
    );

    // Listing carries per-entry contentType (G5).
    let mut listing = rt.handle(req(Method::GET, "/files/")).await;
    let doc = listing.body.as_mut().unwrap().as_json(65536).await.unwrap();
    let entry = doc["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "b.txt")
        .unwrap();
    assert!(
        entry["contentType"]
            .as_str()
            .unwrap()
            .starts_with("text/plain"),
        "{entry}"
    );

    // MOVE over an existing file overwrites → 200.
    rt.handle(
        req(Method::PUT, "/files/c.txt")
            .with_body(Body::from_string("y", MediaType::new("text/plain"))),
    )
    .await;
    let mut m2 = req(mv(), "/files/c.txt");
    m2.set_header("destination", "/files/b.txt");
    assert_eq!(rt.handle(m2).await.status, Some(StatusCode::OK));

    // Missing source → 404.
    let mut m3 = req(mv(), "/files/nope.txt");
    m3.set_header("destination", "/files/x.txt");
    assert_eq!(rt.handle(m3).await.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn data_schema_index_and_listing_content_type() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());

    let schema = json!({ "type": "object", "properties": { "n": { "type": "integer" } } });
    assert_eq!(
        rt.handle(req(Method::PUT, "/data/widgets/.schema.json").with_json(&schema))
            .await
            .status,
        Some(StatusCode::OK)
    );
    assert_eq!(
        rt.handle(req(Method::PUT, "/data/widgets/w1").with_json(&json!({ "n": 1 })))
            .await
            .status,
        Some(StatusCode::CREATED)
    );

    // .schemas indexes every dataset's installed schema in one call (G6).
    let mut idx = rt.handle(req(Method::GET, "/data/.schemas")).await;
    assert_eq!(idx.status, Some(StatusCode::OK));
    let doc = idx.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(
        doc["schemas"]["widgets"]["schemaUrl"],
        "/data/widgets/.schema.json"
    );
    assert_eq!(
        doc["schemas"]["widgets"]["schema"]["properties"]["n"]["type"],
        "integer"
    );

    // Record listings carry contentType (G5).
    let mut listing = rt.handle(req(Method::GET, "/data/widgets/")).await;
    let doc = listing.body.as_mut().unwrap().as_json(65536).await.unwrap();
    let entry = doc["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "w1")
        .unwrap();
    assert_eq!(entry["contentType"], "application/json");
}

#[tokio::test]
async fn path_traversal_is_rejected_at_the_router() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    for path in [
        "/files/../secret",
        "/files/%2e%2e/secret",
        "/files/a%00.txt",
    ] {
        let resp = rt.handle(req(Method::GET, path)).await;
        assert_eq!(
            resp.status,
            Some(StatusCode::BAD_REQUEST),
            "path {path} must be rejected"
        );
    }
}

#[tokio::test]
async fn data_crud_schema_validation_and_confirm_delete() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());

    // Install a dataset schema.
    let schema = json!({ "type": "object", "required": ["name"], "properties": { "name": { "type": "string" } } });
    let resp = rt
        .handle(req(Method::PUT, "/data/people/.schema.json").with_json(&schema))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));

    // Valid write → 201; invalid write → 422 with structured errors.
    let resp = rt
        .handle(req(Method::PUT, "/data/people/ada").with_json(&json!({ "name": "Ada" })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    let mut resp = rt
        .handle(req(Method::PUT, "/data/people/bad").with_json(&json!({ "age": 7 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::UNPROCESSABLE_ENTITY));
    let problem = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(problem["code"], "validation_failed");
    assert!(problem["errors"].is_array());

    // GET carries the schema reference in the media type (PRD §6.2).
    let mut resp = rt.handle(req(Method::GET, "/data/people/ada")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.schema(),
        Some("/data/people/.schema.json")
    );
    let v = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(v["name"], "Ada");

    // PATCH = JSON merge, result re-validated.
    let mut resp = rt
        .handle(req(Method::PATCH, "/data/people/ada").with_json(&json!({ "nick": "A" })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let v = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(v["nick"], "A");
    let resp = rt
        .handle(req(Method::PATCH, "/data/people/ada").with_json(&json!({ "name": null })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::UNPROCESSABLE_ENTITY));

    // Keyless POST generates a key and Location.
    let resp = rt
        .handle(req(Method::POST, "/data/people").with_json(&json!({ "name": "Grace" })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    let location = resp.header("location").unwrap().to_string();
    assert!(location.starts_with("/data/people/"));

    // Listing paginates.
    let mut resp = rt.handle(req(Method::GET, "/data/people/")).await;
    let listing = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(listing["total"], 2);

    // Dataset delete needs the explicit confirm token (409 per the store
    // contract's non-empty-container guard).
    let resp = rt.handle(req(Method::DELETE, "/data/people")).await;
    assert_eq!(resp.status, Some(StatusCode::CONFLICT));
    let resp = rt
        .handle(req(Method::DELETE, "/data/people?confirm=people"))
        .await;
    assert_eq!(resp.status, Some(StatusCode::NO_CONTENT));
    let resp = rt.handle(req(Method::GET, "/data/people/ada")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn errors_are_problem_json_with_trace() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    let mut resp = rt.handle(req(Method::GET, "/nowhere/x")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "application/problem+json"
    );
    let problem = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(problem["code"], "not_found");
    assert_eq!(problem["tenant"], "t1");
    assert!(problem["traceId"].is_string());
}

#[tokio::test]
async fn auth_stub_gates_protected_mounts() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    let resp = rt.handle(req(Method::GET, "/private/x.txt")).await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));

    let mut authed = req(Method::GET, "/private/missing.txt");
    authed.principal = Some(rs2_core::message::Principal {
        id: "u1".into(),
        roles: vec!["U".into()],
        kind: "user".into(),
        extra: Default::default(),
    });
    // Passes auth, then 404s on the missing file — proving the gate opened.
    let resp = rt.handle(authed).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
}

/// Static-site mode (v1's static-site service, as file config): default
/// resource, SPA fallback, suppressed listings.
#[tokio::test]
async fn file_static_site_mode() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/site", "service": "file", "config": {
            "access": "open",
            "spaFallback": true, "listings": false,
            "caching": { "mode": "cache", "maxAgeSeconds": 300, "public": true } } },
        { "path": "/files", "service": "file", "config": { "access": "open" } }
    ]})));
    let rt = Runtime::new(
        Tenancy::Single {
            tenant: "t1".into(),
        },
        adapters,
        loader,
        LimitTable::default(),
    );

    // Author the site (the mount is still a store for writes).
    for (path, content, mt) in [
        ("/site/index.html", "<html>app</html>", "text/html"),
        ("/site/app.js", "boot()", "application/javascript"),
        ("/site/docs/index.html", "<html>docs</html>", "text/html"),
    ] {
        let put = req(Method::PUT, path).with_body(Body::from_string(content, MediaType::new(mt)));
        assert_eq!(
            rt.handle(put).await.status,
            Some(StatusCode::CREATED),
            "{path}"
        );
    }

    // Directory GETs serve the default resource, not a listing.
    let mut root = rt.handle(req(Method::GET, "/site/")).await;
    assert_eq!(root.status, Some(StatusCode::OK));
    assert_eq!(
        root.body.as_ref().unwrap().media_type.essence(),
        "text/html"
    );
    let bytes = root
        .body
        .as_mut()
        .unwrap()
        .materialize(65536)
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"<html>app</html>");
    assert_eq!(root.header("cache-control"), Some("public, max-age=300"));
    let mut docs = rt.handle(req(Method::GET, "/site/docs/")).await;
    let bytes = docs
        .body
        .as_mut()
        .unwrap()
        .materialize(65536)
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"<html>docs</html>", "nearest default doc wins");

    // SPA fallback: extension-less routes serve the root index with 200…
    let mut route = rt.handle(req(Method::GET, "/site/users/42/profile")).await;
    assert_eq!(route.status, Some(StatusCode::OK));
    let bytes = route
        .body
        .as_mut()
        .unwrap()
        .materialize(65536)
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"<html>app</html>");

    // …while missing assets stay honest 404s.
    let asset = rt.handle(req(Method::GET, "/site/missing.js")).await;
    assert_eq!(asset.status, Some(StatusCode::NOT_FOUND));
    // And real assets serve normally.
    let js = rt.handle(req(Method::GET, "/site/app.js")).await;
    assert_eq!(js.status, Some(StatusCode::OK));

    // Listings are suppressed on the site mount, intact on the plain one.
    // (A directory with no default doc 404s rather than listing.)
    let put = req(Method::PUT, "/site/assets/x.png")
        .with_body(Body::from_string("png", MediaType::new("image/png")));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    // /site/assets/ has no index.html → SPA fallback serves the app shell
    // (it is an extension-less navigation), proving routes under asset
    // dirs still work; the listing never leaks.
    let mut assets = rt.handle(req(Method::GET, "/site/assets/")).await;
    assert_eq!(assets.status, Some(StatusCode::OK));
    let bytes = assets
        .body
        .as_mut()
        .unwrap()
        .materialize(65536)
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"<html>app</html>");
    let listing = rt.handle(req(Method::GET, "/files/")).await;
    assert_eq!(
        listing.body.as_ref().unwrap().media_type.essence(),
        "application/vnd.rs2.dir+json"
    );

    // The discovery surface declares the facet.
    let mut services = rt
        .handle(req(Method::GET, "/.well-known/rs2/services"))
        .await;
    let doc = services
        .body
        .as_mut()
        .unwrap()
        .as_json(65536)
        .await
        .unwrap();
    let site = doc["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["path"] == "/site")
        .unwrap();
    assert!(
        site["facets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f == "static-site"),
        "{site}"
    );
}

#[tokio::test]
async fn tenant_storage_is_host_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(
        json!({ "mounts": [ { "path": "/files", "service": "file", "config": { "access": "open" } } ] }),
    ));
    let rt = Runtime::new(
        Tenancy::Multi {
            domain_map: Default::default(),
            main_domain: Some("rs2.test".into()),
        },
        adapters,
        loader,
        LimitTable::default(),
    );

    let write = Message::request(Method::PUT, "/files/x.txt", "alpha").with_body(
        Body::from_string("alpha-data", MediaType::new("text/plain")),
    );
    assert_eq!(rt.handle(write).await.status, Some(StatusCode::CREATED));

    // The same path under another tenant does not exist.
    let read = Message::request(Method::GET, "/files/x.txt", "beta");
    assert_eq!(rt.handle(read).await.status, Some(StatusCode::NOT_FOUND));

    // And on disk the file sits under the tenant prefix.
    assert!(dir.path().join("alpha").join("x.txt").exists());
}
