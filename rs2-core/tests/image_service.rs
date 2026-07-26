//! End-to-end for the image transform service (`guest-services/image`):
//! deploy the built component, mount it decorating a file mount, and drive
//! the resize/cache/conditional flow through full dispatch. Self-skips
//! unless `RS2_IMAGE_COMPONENT` points at the built component:
//!
//! ```powershell
//! cd guest-services/image; cargo build --target wasm32-wasip2 --release; cd ../..
//! $env:RS2_IMAGE_COMPONENT = "$PWD\guest-services\image\target\wasm32-wasip2\release\rs2_image.wasm"
//! cargo test --features wasm -p rs2-core --test image_service
//! ```

#![cfg(feature = "wasm")]

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

const GRADIENT_PNG: &[u8] = include_bytes!("fixtures/gradient.png");

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
            { "path": "/files", "service": "file", "config": { "access": "open" } },
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

async fn body_bytes(msg: &mut Message) -> Vec<u8> {
    msg.body
        .as_mut()
        .expect("body")
        .materialize(10 * 1024 * 1024)
        .await
        .expect("materialize")
        .to_vec()
}

/// Width/height from a PNG IHDR (big-endian u32s at offsets 16 and 20).
fn png_dims(bytes: &[u8]) -> (u32, u32) {
    assert!(bytes.len() > 24, "not a PNG: {} bytes", bytes.len());
    assert_eq!(&bytes[1..4], b"PNG", "not a PNG");
    let w = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    let h = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
    (w, h)
}

/// Deploy the built component and mount it at `/img` decorating `/files`.
async fn setup(rt: &Runtime, component: Vec<u8>) {
    let deploy = req(Method::POST, "/services/code/image/")
        .with_body(Body::from_bytes(component, MediaType::new("application/wasm")));
    let mut resp = rt.handle(deploy).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let body = resp
        .body
        .as_mut()
        .unwrap()
        .as_json(10 * 1024 * 1024)
        .await
        .unwrap();
    let code_ref = body["ref"].as_str().unwrap().to_string();

    let mut config = base_config();
    config["mounts"].as_array_mut().unwrap().push(json!({
        "path": "/img", "service": code_ref,
        "config": {
            "access": "open",
            "grants": {
                "source": { "prefix": "/files" },
                "cache":  { "type": "store", "root": "img-cache" }
            },
            "widths": [8, 16, 32],
            "caching": { "mode": "cache", "maxAgeSeconds": 60, "public": true }
        }
    }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    let put = req(Method::PUT, "/files/pic.png").with_body(Body::from_bytes(
        GRADIENT_PNG.to_vec(),
        MediaType::new("image/png"),
    ));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
}

#[tokio::test]
async fn resize_cache_and_conditional_flow() {
    let Ok(component_path) = std::env::var("RS2_IMAGE_COMPONENT") else {
        eprintln!("skipping: RS2_IMAGE_COMPONENT not set");
        return;
    };
    let component = std::fs::read(&component_path).expect("read image component");
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), base_config());
    setup(&rt, component).await;

    // Miss: transforms, stores, and serves the derivative via body splice.
    // (w=10 snaps up the [8,16,32] ladder to 16; source is 64x32 PNG so
    // f=auto stays PNG and scale-down keeps aspect: 16x8.)
    let mut first = rt.handle(req(Method::GET, "/img/pic.png?w=10")).await;
    assert_eq!(first.status, Some(StatusCode::OK), "{:?}", first.body);
    assert_eq!(first.header("x-img-cache"), Some("miss"));
    assert_eq!(first.header("x-rs2-body-ref"), None, "splice header stripped");
    let etag = first.header("etag").expect("derived etag").to_string();
    let bytes = body_bytes(&mut first).await;
    assert_eq!(png_dims(&bytes), (16, 8));

    // Hit: same canonical params (dpr folds, order differs) → same entry,
    // no transform, same bytes.
    let mut second = rt.handle(req(Method::GET, "/img/pic.png?dpr=2&w=8")).await;
    assert_eq!(second.status, Some(StatusCode::OK));
    assert_eq!(second.header("x-img-cache"), Some("hit"));
    assert_eq!(second.header("etag"), Some(etag.as_str()));
    assert_eq!(body_bytes(&mut second).await, bytes);

    // Conditional: the derived ETag revalidates without any image work.
    let mut third = req(Method::GET, "/img/pic.png?w=10");
    third.set_header("if-none-match", &etag);
    let third = rt.handle(third).await;
    assert_eq!(third.status, Some(StatusCode::NOT_MODIFIED));

    // The mount's caching config is host-applied to the 200s.
    assert_eq!(first.header("cache-control"), Some("public, max-age=60"));

    // Derivatives live under the private store-grant tree.
    let cache_root = dir.path().join("t").join(".rs2-store").join("img-cache");
    assert!(cache_root.is_dir(), "cache tree exists");
}

#[tokio::test]
async fn passthrough_transforms_and_errors() {
    let Ok(component_path) = std::env::var("RS2_IMAGE_COMPONENT") else {
        eprintln!("skipping: RS2_IMAGE_COMPONENT not set");
        return;
    };
    let component = std::fs::read(&component_path).expect("read image component");
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), base_config());
    setup(&rt, component).await;

    // No params: the original streams through untouched.
    let mut plain = rt.handle(req(Method::GET, "/img/pic.png")).await;
    assert_eq!(plain.status, Some(StatusCode::OK), "{:?}", plain.body);
    assert_eq!(body_bytes(&mut plain).await, GRADIENT_PNG);

    // Cover with gravity: exact box.
    let mut cover = rt
        .handle(req(Method::GET, "/img/pic.png?w=8&h=8&fit=cover&g=e"))
        .await;
    assert_eq!(cover.status, Some(StatusCode::OK), "{:?}", cover.body);
    assert_eq!(png_dims(&body_bytes(&mut cover).await), (8, 8));

    // JPEG conversion.
    let mut jpg = rt.handle(req(Method::GET, "/img/pic.png?w=8&f=jpeg")).await;
    assert_eq!(jpg.status, Some(StatusCode::OK));
    let jb = body_bytes(&mut jpg).await;
    assert_eq!(&jb[..2], &[0xFF, 0xD8], "JPEG magic");

    // Unknown parameter: a structured 400 before any I/O.
    let mut bad = rt.handle(req(Method::GET, "/img/pic.png?wat=1")).await;
    assert_eq!(bad.status, Some(StatusCode::BAD_REQUEST));
    let err = bad
        .body
        .as_mut()
        .unwrap()
        .as_json(1024 * 1024)
        .await
        .unwrap();
    assert_eq!(err["code"], "bad_request");

    // Missing source: 404, not a transform error.
    let missing = rt.handle(req(Method::GET, "/img/nope.png?w=8")).await;
    assert_eq!(missing.status, Some(StatusCode::NOT_FOUND));

    // $info: source metadata for asset pickers.
    let mut info = rt.handle(req(Method::GET, "/img/pic.png?$info")).await;
    assert_eq!(info.status, Some(StatusCode::OK), "{:?}", info.body);
    let meta = info
        .body
        .as_mut()
        .unwrap()
        .as_json(1024 * 1024)
        .await
        .unwrap();
    assert_eq!(meta["width"], 64);
    assert_eq!(meta["height"], 32);
    assert_eq!(meta["mediaType"], "image/png");

    // Purge: seed a derivative, wipe the cache, and the next request is a
    // fresh miss. (Purge honors the mount's delete access, checked by
    // dispatch before the guest runs.)
    let seeded = rt.handle(req(Method::GET, "/img/pic.png?w=8")).await;
    assert_eq!(seeded.header("x-img-cache"), Some("miss"));
    let unconfirmed = rt.handle(req(Method::DELETE, "/img/.cache")).await;
    assert_eq!(unconfirmed.status, Some(StatusCode::CONFLICT));
    let purge = rt.handle(req(Method::DELETE, "/img/.cache?confirm=1")).await;
    assert_eq!(purge.status, Some(StatusCode::NO_CONTENT), "{:?}", purge.body);
    let again = rt.handle(req(Method::GET, "/img/pic.png?w=8")).await;
    assert_eq!(again.header("x-img-cache"), Some("miss"), "cache was emptied");
}
