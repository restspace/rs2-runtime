//! DirectorySlash on the `file` service: a GET/HEAD whose path names a
//! directory *without* the trailing slash 301-redirects to the slash form
//! (preserving the query string), so relative URLs in the served default
//! document resolve correctly. What the slash form then yields — default doc,
//! listing, or 404 — is decided by the directory arm as before. A directory
//! beats a same-named friendly-URL (`.html`) probe.

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

/// `/site` — static-site mount (default doc).
/// `/locked` — no default doc, listings suppressed.
/// `/plain` — bare file mount (listings on, no default doc).
/// `/f` — friendly URLs enabled.
fn test_runtime(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/site", "service": "file", "config": {
            "access": "open", "defaultResource": "index.html" } },
        { "path": "/locked", "service": "file", "config": {
            "access": "open", "listings": false } },
        { "path": "/plain", "service": "file", "config": { "access": "open" } },
        { "path": "/f", "service": "file", "config": {
            "access": "open", "friendlyUrls": true,
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
async fn no_slash_dir_redirects_then_serves_default_doc() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/abc/index.html", "<html>abc</html>", "text/html").await;

    let resp = rt.handle(req(Method::GET, "/site/abc")).await;
    assert_eq!(resp.status, Some(StatusCode::MOVED_PERMANENTLY));
    assert_eq!(resp.header("location"), Some("/site/abc/"));

    // Following the redirect serves the default doc.
    let mut followed = rt.handle(req(Method::GET, "/site/abc/")).await;
    assert_eq!(followed.status, Some(StatusCode::OK));
    assert_eq!(&body_of(&mut followed).await[..], b"<html>abc</html>");
}

#[tokio::test]
async fn redirect_preserves_query_string() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/abc/index.html", "<html>abc</html>", "text/html").await;

    let resp = rt.handle(req(Method::GET, "/site/abc?x=1&y=2")).await;
    assert_eq!(resp.status, Some(StatusCode::MOVED_PERMANENTLY));
    assert_eq!(resp.header("location"), Some("/site/abc/?x=1&y=2"));
}

#[tokio::test]
async fn head_gets_the_same_redirect() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/abc/index.html", "<html>abc</html>", "text/html").await;

    let resp = rt.handle(req(Method::HEAD, "/site/abc")).await;
    assert_eq!(resp.status, Some(StatusCode::MOVED_PERMANENTLY));
    assert_eq!(resp.header("location"), Some("/site/abc/"));
}

#[tokio::test]
async fn genuine_miss_is_still_404() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/site/index.html", "<html>home</html>", "text/html").await;

    let resp = rt.handle(req(Method::GET, "/site/missing")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn redirect_fires_even_when_slash_form_404s() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    // `/locked` has no default doc and listings off: the slash form is a 404,
    // but the no-slash form still redirects — the directory arm stays the
    // single authority on what a directory URL yields.
    put_ok(&rt, "/locked/sub/file.txt", "x", "text/plain").await;

    let resp = rt.handle(req(Method::GET, "/locked/sub")).await;
    assert_eq!(resp.status, Some(StatusCode::MOVED_PERMANENTLY));
    assert_eq!(resp.header("location"), Some("/locked/sub/"));

    let followed = rt.handle(req(Method::GET, "/locked/sub/")).await;
    assert_eq!(followed.status, Some(StatusCode::NOT_FOUND));
}

#[tokio::test]
async fn redirect_then_listing_on_plain_mount() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    put_ok(&rt, "/plain/sub/file.txt", "x", "text/plain").await;

    let resp = rt.handle(req(Method::GET, "/plain/sub")).await;
    assert_eq!(resp.status, Some(StatusCode::MOVED_PERMANENTLY));
    assert_eq!(resp.header("location"), Some("/plain/sub/"));

    let mut followed = rt.handle(req(Method::GET, "/plain/sub/")).await;
    assert_eq!(followed.status, Some(StatusCode::OK));
    let listing: serde_json::Value = serde_json::from_slice(&body_of(&mut followed).await).unwrap();
    assert_eq!(listing["entries"][0]["name"], "file.txt");
}

#[tokio::test]
async fn directory_beats_friendly_url_probe() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    // Both a directory `abc/` and a file `abc.html` exist. The directory wins:
    // friendly resolution must not shadow a real container.
    put_ok(&rt, "/f/abc/inner.txt", "inner", "text/plain").await;
    put_ok(&rt, "/f/abc.html", "<html>page</html>", "text/html").await;

    let resp = rt.handle(req(Method::GET, "/f/abc")).await;
    assert_eq!(resp.status, Some(StatusCode::MOVED_PERMANENTLY));
    assert_eq!(resp.header("location"), Some("/f/abc/"));
}

#[tokio::test]
async fn friendly_resolution_skips_directory_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let rt = test_runtime(dir.path());
    // A *directory* named `readme.html` must not satisfy the friendly probe;
    // resolution moves on to the next candidate extension.
    put_ok(&rt, "/f/readme.html/inner.txt", "inner", "text/plain").await;
    put_ok(&rt, "/f/readme.md", "# readme", "text/markdown").await;

    let mut resp = rt.handle(req(Method::GET, "/f/readme")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.header("content-location"), Some("/f/readme.md"));
    assert_eq!(&body_of(&mut resp).await[..], b"# readme");
}
