//! Friendly (extension-less) URLs on the `file` service: `GET /docs/readme`
//! resolves a stored `docs/readme.<ext>`, choosing among collisions by `Accept`
//! negotiation, serving in place with a `Content-Location` to the real path.
//! Opt-in via `friendlyUrls`; off by default. An extension-less PUT pins to the
//! first `extensionPriority` entry so a no-`Accept` GET of the slug always
//! returns it (and no later sibling can dislodge it).

use std::sync::Arc;

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

#[async_trait::async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

/// `/friendly` — friendly URLs with an `html, md, json` priority.
/// `/nopri` — friendly URLs, no priority (extension-less PUT must 400).
/// `/plain` — defaults (friendly off). `/spa` — friendly over SPA fallback.
fn test_runtime(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/friendly", "service": "file", "config": {
            "access": "open", "friendlyUrls": true,
            "extensionPriority": ["html", "md", "json"] } },
        { "path": "/nopri", "service": "file", "config": {
            "access": "open", "friendlyUrls": true } },
        { "path": "/plain", "service": "file", "config": { "access": "open" } },
        { "path": "/spa", "service": "file", "config": {
            "access": "open", "friendlyUrls": true, "spaFallback": true, "listings": false,
            "extensionPriority": ["html", "md"] } }
    ]})));
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

async fn put(rt: &Runtime, path: &str, content: &str, mt: &str) -> Message {
    let m = req(Method::PUT, path).with_body(Body::from_string(content, MediaType::new(mt)));
    rt.handle(m).await
}

async fn put_ok(rt: &Runtime, path: &str, content: &str, mt: &str) {
    assert_eq!(
        put(rt, path, content, mt).await.status,
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
async fn friendly_url_resolves_and_advertises_real_path() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/friendly/docs/readme.md", "# hello", "text/markdown").await;

    let mut resp = rt.handle(req(Method::GET, "/friendly/docs/readme")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "text/markdown"
    );
    assert_eq!(
        resp.header("content-location"),
        Some("/friendly/docs/readme.md")
    );
    assert_eq!(&body_of(&mut resp).await[..], b"# hello");
}

#[tokio::test]
async fn collision_resolved_by_accept_then_priority() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    // Write the variants directly (an extension-less PUT would pin, not branch).
    put_ok(&rt, "/friendly/page.md", "# md", "text/markdown").await;
    put_ok(&rt, "/friendly/page.html", "<h1>html</h1>", "text/html").await;

    // Accept prefers HTML.
    let mut html = rt
        .handle(req_accept(Method::GET, "/friendly/page", "text/html"))
        .await;
    assert_eq!(html.header("content-location"), Some("/friendly/page.html"));
    assert_eq!(&body_of(&mut html).await[..], b"<h1>html</h1>");

    // Accept prefers markdown.
    let mut md = rt
        .handle(req_accept(Method::GET, "/friendly/page", "text/markdown"))
        .await;
    assert_eq!(md.header("content-location"), Some("/friendly/page.md"));
    assert_eq!(&body_of(&mut md).await[..], b"# md");

    // No Accept → priority order (html is E1).
    let plain = rt.handle(req(Method::GET, "/friendly/page")).await;
    assert_eq!(
        plain.header("content-location"),
        Some("/friendly/page.html")
    );
}

#[tokio::test]
async fn extensionless_put_pins_to_e1_ignoring_content_type() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());

    // PUT an extension-less slug with a markdown body — pins to E1 (html).
    let put = put(&rt, "/friendly/page", "# hi", "text/markdown").await;
    assert_eq!(put.status, Some(StatusCode::CREATED));
    assert_eq!(put.header("location"), Some("/friendly/page"));
    assert_eq!(put.header("content-location"), Some("/friendly/page.html"));

    // No-Accept GET of the slug returns it, served as E1's type (html).
    let mut got = rt.handle(req(Method::GET, "/friendly/page")).await;
    assert_eq!(got.status, Some(StatusCode::OK));
    assert_eq!(got.body.as_ref().unwrap().media_type.essence(), "text/html");
    assert_eq!(got.header("content-location"), Some("/friendly/page.html"));
    assert_eq!(&body_of(&mut got).await[..], b"# hi");

    // The real path is reachable directly too.
    let exact = rt.handle(req(Method::GET, "/friendly/page.html")).await;
    assert_eq!(exact.status, Some(StatusCode::OK));
}

#[tokio::test]
async fn pin_not_dislodged_by_later_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/friendly/page", "# hi", "text/markdown").await; // → page.html (E1)
    put_ok(&rt, "/friendly/page.md", "# md", "text/markdown").await; // a lower-priority sibling

    // No-Accept GET still returns the pinned html — not dislodged.
    let plain = rt.handle(req(Method::GET, "/friendly/page")).await;
    assert_eq!(
        plain.header("content-location"),
        Some("/friendly/page.html")
    );

    // An explicit Accept is the caller opting out — they may get the sibling.
    let md = rt
        .handle(req_accept(Method::GET, "/friendly/page", "text/markdown"))
        .await;
    assert_eq!(md.header("content-location"), Some("/friendly/page.md"));
}

#[tokio::test]
async fn extensionless_put_without_priority_is_400() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    // `/nopri` has friendlyUrls on but no extensionPriority → nowhere to pin.
    let resp = put(&rt, "/nopri/page", "# hi", "text/markdown").await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    // An extensioned PUT is unaffected.
    put_ok(&rt, "/nopri/page.md", "# md", "text/markdown").await;
}

#[tokio::test]
async fn friendly_off_stores_extensionless_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    // friendlyUrls off: the slug is taken literally, no pin, no 400.
    put_ok(&rt, "/plain/page", "raw", "application/octet-stream").await;
    let mut got = rt.handle(req(Method::GET, "/plain/page")).await;
    assert_eq!(got.status, Some(StatusCode::OK));
    assert_eq!(got.header("content-location"), None);
    assert_eq!(&body_of(&mut got).await[..], b"raw");
}

#[tokio::test]
async fn exact_match_wins_over_friendly() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    // A literal extension-less file (e.g. authored before pinning, or out-of-band)
    // plus a same-stem variant. Layout is `<root>/<tenant>/<path>`.
    let tdir = dir.path().join("t1");
    std::fs::create_dir_all(&tdir).unwrap();
    std::fs::write(tdir.join("readme"), b"raw").unwrap();
    put_ok(&rt, "/friendly/readme.md", "# md", "text/markdown").await;

    let mut resp = rt.handle(req(Method::GET, "/friendly/readme")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    // Served directly — no friendly probe, no Content-Location.
    assert_eq!(resp.header("content-location"), None);
    assert_eq!(&body_of(&mut resp).await[..], b"raw");
}

#[tokio::test]
async fn friendly_off_by_default_is_404() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/plain/docs/readme.md", "# hello", "text/markdown").await;

    let resp = rt.handle(req(Method::GET, "/plain/docs/readme")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn friendly_miss_falls_through_to_spa() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/spa/index.html", "<html>app</html>", "text/html").await;
    put_ok(&rt, "/spa/about.md", "# about", "text/markdown").await;

    // A real friendly file beats the SPA shell.
    let mut about = rt.handle(req(Method::GET, "/spa/about")).await;
    assert_eq!(about.header("content-location"), Some("/spa/about.md"));
    assert_eq!(&body_of(&mut about).await[..], b"# about");

    // No friendly match → SPA fallback serves the index with 200.
    let mut route = rt.handle(req(Method::GET, "/spa/users/42/profile")).await;
    assert_eq!(route.status, Some(StatusCode::OK));
    assert_eq!(&body_of(&mut route).await[..], b"<html>app</html>");
}

#[tokio::test]
async fn delete_removes_pinned_then_falls_through() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/friendly/page", "# hi", "text/markdown").await; // → page.html (pinned)
    put_ok(&rt, "/friendly/page.md", "# md", "text/markdown").await; // sibling

    // DELETE of the slug removes the pinned E1 (page.html).
    let del = rt.handle(req(Method::DELETE, "/friendly/page")).await;
    assert_eq!(del.status, Some(StatusCode::NO_CONTENT));

    // GET now falls through to the surviving lower-priority sibling.
    let after = rt.handle(req(Method::GET, "/friendly/page")).await;
    assert_eq!(after.header("content-location"), Some("/friendly/page.md"));

    // DELETE again removes the sibling; the slug is now gone.
    let del2 = rt.handle(req(Method::DELETE, "/friendly/page")).await;
    assert_eq!(del2.status, Some(StatusCode::NO_CONTENT));
    let gone = rt.handle(req(Method::GET, "/friendly/page")).await;
    assert_eq!(gone.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn conditional_delete_checks_the_resolved_file() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/friendly/docs/readme.md", "# hello", "text/markdown").await;

    let resp = rt.handle(req(Method::GET, "/friendly/docs/readme")).await;
    let etag = resp.header("etag").unwrap().to_string();

    // A stale If-Match on the slug is checked against the resolved file:
    // 412, and the file survives.
    let mut stale = req(Method::DELETE, "/friendly/docs/readme");
    stale.set_header("if-match", "\"not-the-current-etag\"");
    let refused = rt.handle(stale).await;
    assert_eq!(refused.status, Some(StatusCode::PRECONDITION_FAILED));
    let still = rt.handle(req(Method::GET, "/friendly/docs/readme")).await;
    assert_eq!(still.status, Some(StatusCode::OK));

    // The matching ETag deletes through the same resolution.
    let mut matched = req(Method::DELETE, "/friendly/docs/readme");
    matched.set_header("if-match", &etag);
    let deleted = rt.handle(matched).await;
    assert_eq!(deleted.status, Some(StatusCode::NO_CONTENT));
    let gone = rt.handle(req(Method::GET, "/friendly/docs/readme")).await;
    assert_eq!(gone.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn head_resolves_friendly() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/friendly/docs/readme.md", "# hello", "text/markdown").await;

    let resp = rt.handle(req(Method::HEAD, "/friendly/docs/readme")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.header("content-type"), Some("text/markdown"));
    assert_eq!(
        resp.header("content-location"),
        Some("/friendly/docs/readme.md")
    );
    assert_eq!(resp.header("content-length"), Some("7"));
}
