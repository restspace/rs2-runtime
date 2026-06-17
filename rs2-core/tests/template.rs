//! The `template` service end to end: author a compiled bundle into the
//! `.templates` subtree, then render it with request props. The fixtures are
//! hand-written self-contained ESM (no esbuild/Preact in CI) — the JSX→bundle
//! step is the CLI's job; this proves the runtime path: store → resolve →
//! props → resident render → `text/html`, plus per-version isolate caching.

#![cfg(feature = "js")]

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

/// A compiled template bundle (what `rs2 template build` would emit): default
/// export renders props to an HTML string. Greets `props.name`.
const GREETER_V1: &str = r#"
export default async (msg) => {
  const p = (msg && msg.body) || {};
  const name = p.name || "world";
  return { status: 200, headers: { "content-type": "text/html; charset=utf-8" },
           body: "<!doctype html><h1>Hello, " + name + "!</h1>" };
};
"#;

/// A different compiled bundle for the same template name (new content version).
const GREETER_V2: &str = r#"
export default async (msg) => {
  const p = (msg && msg.body) || {};
  return { status: 200, body: "<p>v2:" + (p.name || "?") + "</p>" };
};
"#;

struct StaticLoader(Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn text(resp: &mut Message) -> String {
    let bytes = resp.body.as_mut().expect("body").materialize(1 << 20).await.expect("bytes");
    String::from_utf8(bytes.to_vec()).expect("utf-8")
}

/// A single-`/render` template runtime. Returns the runtime and the tempdir
/// (kept in scope so its backing files outlive the runtime).
fn template_rt() -> (Arc<Runtime>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(dir.path())), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({
        "mounts": [{ "path": "/render", "service": "template" }]
    })));
    let rt =
        Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());
    (rt, dir)
}

/// PUT a compiled template envelope into the authoring subtree.
async fn put_template(rt: &Runtime, name: &str, source: &str) -> Option<StatusCode> {
    rt.handle(
        req(Method::PUT, &format!("/render/.templates/{name}")).with_json(&json!({ "source": source })),
    )
    .await
    .status
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renders_from_json_body_and_query_params() {
    let (rt, _dir) = template_rt();
    assert_eq!(put_template(&rt, "welcome", GREETER_V1).await, Some(StatusCode::CREATED));

    // POST with a JSON body → props.
    let mut resp = rt.handle(req(Method::POST, "/render/welcome").with_json(&json!({ "name": "Alice" }))).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "render: {:?}", resp.body);
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "text/html",
        "rendered body is html"
    );
    assert!(text(&mut resp).await.contains("Hello, Alice!"), "body greets Alice");

    // GET with query params → props (same template, no body).
    let mut resp = rt.handle(req(Method::GET, "/render/welcome?name=Bob")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert!(text(&mut resp).await.contains("Hello, Bob!"), "query param becomes a prop");

    // No props → the template's own default.
    let mut resp = rt.handle(req(Method::GET, "/render/welcome")).await;
    assert!(text(&mut resp).await.contains("Hello, world!"), "default when no props");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authoring_subtree_round_trips_and_lists() {
    let (rt, _dir) = template_rt();
    assert_eq!(put_template(&rt, "welcome", GREETER_V1).await, Some(StatusCode::CREATED));

    // GET the stored envelope back.
    let mut resp = rt.handle(req(Method::GET, "/render/.templates/welcome")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let env: Value = serde_json::from_str(&text(&mut resp).await).expect("stored envelope is JSON");
    assert!(env["source"].as_str().unwrap().contains("Hello, "), "source round-trips");

    // The template appears in the authoring listing.
    let mut resp = rt.handle(req(Method::GET, "/render/.templates/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let listing: Value = serde_json::from_str(&text(&mut resp).await).unwrap();
    assert!(
        listing["entries"].as_array().unwrap().iter().any(|e| e["name"] == "welcome"),
        "welcome is listed: {listing}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_version_replaces_the_cached_isolate() {
    let (rt, _dir) = template_rt();
    assert_eq!(put_template(&rt, "welcome", GREETER_V1).await, Some(StatusCode::CREATED));

    let mut resp = rt.handle(req(Method::GET, "/render/welcome?name=Ada")).await;
    assert!(text(&mut resp).await.contains("Hello, Ada!"), "v1 renders");

    // Overwrite with a different bundle → new content version.
    assert_eq!(put_template(&rt, "welcome", GREETER_V2).await, Some(StatusCode::OK));
    let mut resp = rt.handle(req(Method::GET, "/render/welcome?name=Ada")).await;
    let body = text(&mut resp).await;
    assert!(body.contains("v2:Ada"), "the cached isolate was rebuilt for v2: {body}");
    assert!(!body.contains("Hello, "), "no stale v1 output");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validator_rejects_an_envelope_without_source() {
    let (rt, _dir) = template_rt();
    let resp = rt
        .handle(req(Method::PUT, "/render/.templates/bad").with_json(&json!({ "notSource": 1 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST), "missing source is rejected at write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmatched_path_is_not_found() {
    let (rt, _dir) = template_rt();
    let resp = rt.handle(req(Method::POST, "/render/nope").with_json(&json!({}))).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND), "no template → 404");
}
