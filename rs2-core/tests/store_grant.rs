//! The `store` grant (service-private storage): a code mount's
//! `{"type": "store", "root": "..."}` grant hands the guest a full store
//! surface over a private `.rs2-store/<root>` tree — never routed through a
//! mount, so no caller principal is involved (the operator-configured grant
//! is the authority). Contrast the `prefix` grant, which re-enters dispatch
//! under the caller's identity.

#![cfg(feature = "js")]

use std::sync::Arc;
use std::sync::Mutex;

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

struct MutableLoader(Mutex<serde_json::Value>);

#[async_trait]
impl ConfigLoader for MutableLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.lock().unwrap().clone())
            .map_err(|e| RsError::internal(e.to_string()))
    }

    async fn load_raw(&self, _tenant: &str) -> Result<(serde_json::Value, String), RsError> {
        Ok((self.0.lock().unwrap().clone(), "v".to_string()))
    }

    async fn save_raw(
        &self,
        _tenant: &str,
        config: &serde_json::Value,
        _expected_version: Option<&str>,
    ) -> Result<String, RsError> {
        *self.0.lock().unwrap() = config.clone();
        Ok("v2".to_string())
    }
}

fn base_config() -> serde_json::Value {
    json!({
        "mounts": [
            { "path": "/services", "service": "services", "config": { "access": "open" } }
        ]
    })
}

fn rt_with(file_root: &std::path::Path, config: serde_json::Value) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(MutableLoader(Mutex::new(config)));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body
        .as_mut()
        .expect("body")
        .as_json(10 * 1024 * 1024)
        .await
        .expect("json body")
}

/// Deploy `bundle` and mount it at `/svc` with the given mount config.
async fn deploy_and_mount(rt: &Runtime, bundle: &str, mount_config: serde_json::Value) {
    let deploy = req(Method::POST, "/services/code/store-user/").with_body(Body::from_bytes(
        bundle.as_bytes().to_vec(),
        MediaType::new("application/javascript"),
    ));
    let mut resp = rt.handle(deploy).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let code_ref = body_json(&mut resp).await["ref"].as_str().unwrap().to_string();

    let mut config = base_config();
    config["mounts"].as_array_mut().unwrap().push(json!({
        "path": "/svc", "service": code_ref, "config": mount_config
    }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));
}

/// The full private-store round trip: conditional-capable writes, reads,
/// listings, denial of ungranted capabilities — and the tree lands under
/// the reserved `.rs2-store/<root>` prefix on disk.
#[tokio::test]
async fn store_grant_round_trips_and_stays_private() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), base_config());

    let bundle = r#"
        export default async (msg, ctx) => {
            const put = ctx.request("cache", { method: "PUT", url: "/a/note.txt",
                body: "hello cache", mediaType: "text/plain" });
            const got = ctx.request("cache", { url: "/a/note.txt" });
            const listing = ctx.request("cache", { url: "/a/" });
            let denied = null;
            try { ctx.request("elsewhere", { url: "/x" }); }
            catch (e) { denied = e.code; }
            return { status: 200, body: {
                putStatus: put.status,
                etag: (put.headers && (put.headers.etag || put.headers.ETag)) || null,
                read: got.body,
                listTotal: listing.body.total,
                denied,
            } };
        };
    "#;
    deploy_and_mount(
        &rt,
        bundle,
        json!({ "access": "open",
                "grants": { "cache": { "type": "store", "root": "img-cache" } } }),
    )
    .await;

    let mut hit = rt.handle(req(Method::GET, "/svc/run")).await;
    assert_eq!(hit.status, Some(StatusCode::OK), "{:?}", hit.body);
    let out = body_json(&mut hit).await;
    assert_eq!(out["putStatus"], 201, "first write creates: {out}");
    assert_eq!(out["read"], "hello cache");
    assert_eq!(out["listTotal"], 1);
    assert_eq!(out["denied"], "capability_denied");

    // The bytes are on disk under the reserved private tree.
    let stored = dir
        .path()
        .join("t")
        .join(".rs2-store")
        .join("img-cache")
        .join("a")
        .join("note.txt");
    assert_eq!(
        std::fs::read_to_string(&stored).expect("stored file exists"),
        "hello cache"
    );
}

/// Traversal in a guest-supplied path cannot escape the private root: the
/// grant target runs the router's path safety and the guest sees a
/// structured `path_unsafe` response, not an escape.
#[tokio::test]
async fn store_grant_rejects_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), base_config());

    let bundle = r#"
        export default async (msg, ctx) => {
            const r = ctx.request("cache", { url: "/../../../escape.txt" });
            return { status: 200, body: { status: r.status, code: r.body && r.body.code } };
        };
    "#;
    deploy_and_mount(
        &rt,
        bundle,
        json!({ "access": "open",
                "grants": { "cache": { "type": "store", "root": "img-cache" } } }),
    )
    .await;

    let mut hit = rt.handle(req(Method::GET, "/svc/run")).await;
    assert_eq!(hit.status, Some(StatusCode::OK), "{:?}", hit.body);
    let out = body_json(&mut hit).await;
    assert_eq!(out["code"], "path_unsafe", "{out}");
}

/// `x-rs2-body-ref`: the guest returns a reference instead of a body; the
/// host reads it through the named grant after the guest returns and
/// attaches the bytes — the hot path for caches/passthrough, with no body
/// crossing the sandbox.
#[tokio::test]
async fn body_ref_attaches_a_granted_read_host_side() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), base_config());

    let bundle = r#"
        export default async (msg, ctx) => {
            if (msg.url.includes("missing")) {
                return { status: 200,
                         headers: { "x-rs2-body-ref": "cache:/nope.txt" } };
            }
            ctx.request("cache", { method: "PUT", url: "/hit.txt",
                body: "derived bytes", mediaType: "text/plain" });
            return { status: 200,
                     headers: { "x-rs2-body-ref": "cache:/hit.txt", "x-img-cache": "hit" } };
        };
    "#;
    deploy_and_mount(
        &rt,
        bundle,
        json!({ "access": "open",
                "grants": { "cache": { "type": "store", "root": "img-cache" } } }),
    )
    .await;

    let mut hit = rt.handle(req(Method::GET, "/svc/run")).await;
    assert_eq!(hit.status, Some(StatusCode::OK), "{:?}", hit.body);
    assert_eq!(hit.header("x-img-cache"), Some("hit"), "guest headers kept");
    assert_eq!(hit.header("x-rs2-body-ref"), None, "ref header is stripped");
    let bytes = hit
        .body
        .as_mut()
        .expect("spliced body")
        .materialize(1024)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(String::from_utf8(bytes).unwrap(), "derived bytes");

    // A dangling reference is the service's bug: a 502 contract violation,
    // not a silent empty 200.
    let mut miss = rt.handle(req(Method::GET, "/svc/missing")).await;
    assert_eq!(miss.status.map(|s| s.as_u16()), Some(502), "{:?}", miss.body);
    let out = body_json(&mut miss).await;
    assert_eq!(out["code"], "contract_violation");
}

/// A store grant without a usable relative root is a structured 400 at
/// invocation, mirroring the httpOut hosts-allowlist check.
#[tokio::test]
async fn store_grant_requires_a_relative_root() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), base_config());

    let bundle = r#"export default async () => ({ status: 200, body: "unreachable" });"#;
    deploy_and_mount(
        &rt,
        bundle,
        json!({ "access": "open",
                "grants": { "cache": { "type": "store", "root": "/absolute" } } }),
    )
    .await;

    let mut hit = rt.handle(req(Method::GET, "/svc/run")).await;
    assert_eq!(hit.status, Some(StatusCode::BAD_REQUEST), "{:?}", hit.body);
    let out = body_json(&mut hit).await;
    assert_eq!(out["code"], "bad_request");
}
