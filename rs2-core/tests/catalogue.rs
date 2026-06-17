//! External catalogues: register a catalogue (tenant config), browse the
//! aggregated `available` list (built-in services + built-in adapters + remote
//! items), and install — fetch (operator-allowlisted) → verify content hash →
//! compile-check → store into `/code/`. Uses a mock `HttpOut` so the real
//! `HttpCatalogueClient` (allowlist + hash pinning) is exercised end-to-end,
//! no network. Runs under default features — install stores without an engine
//! (validated=false), exactly like the upload path.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::HttpOut;
use rs2_core::message::{Body, MediaType, Message};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

const BUNDLE: &[u8] = b"export default function(){}";
const CAT_HOST: &str = "catalogues.test";

/// Canned-response HttpOut keyed by full URL; everything else 404s.
struct MockHttpOut {
    responses: HashMap<String, (u16, Vec<u8>, String)>,
}

#[async_trait]
impl HttpOut for MockHttpOut {
    async fn request(&self, msg: Message) -> Result<Message, RsError> {
        let url = if msg.url.query.is_empty() {
            msg.url.path.clone()
        } else {
            format!("{}?{}", msg.url.path, msg.url.query)
        };
        let template = msg.response(StatusCode::OK, None);
        match self.responses.get(&url) {
            Some((status, body, ct)) => Ok(template.response(
                StatusCode::from_u16(*status).unwrap(),
                Some(Body::from_bytes(body.clone(), MediaType::parse(ct))),
            )),
            None => Ok(template.response(StatusCode::NOT_FOUND, None)),
        }
    }
}

struct Loader(Value);

#[async_trait]
impl ConfigLoader for Loader {
    async fn load_tenant(&self, _t: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
    async fn load_raw(&self, _t: &str) -> Result<(Value, String), RsError> {
        Ok((self.0.clone(), "v1".into()))
    }
}

fn catalogue_doc(version: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "items": [{
            "name": "greeter", "kind": "service", "engine": "js",
            "version": version,
            "bundleUrl": format!("https://{CAT_HOST}/greeter.js"),
            "description": "a greeter",
        }]
    }))
    .unwrap()
}

fn build(
    file_root: &std::path::Path,
    catalogues: Value,
    allowlist: Vec<String>,
    responses: HashMap<String, (u16, Vec<u8>, String)>,
) -> Arc<Runtime> {
    let http: Arc<dyn HttpOut> = Arc::new(MockHttpOut { responses });
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(file_root)), Arc::new(MemDataStore::new()))
            .with_http(http)
            .with_catalogue(allowlist);
    let config = json!({
        "mounts": [{ "path": "/admin", "service": "services" }],
        "catalogues": catalogues,
    });
    Runtime::new(
        Tenancy::Single { tenant: "t1".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    )
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t1")
}

async fn body_json(resp: &mut Message) -> Value {
    resp.body.as_mut().unwrap().as_json(1 << 20).await.unwrap()
}

#[tokio::test]
async fn list_available_and_install_from_catalogue() {
    let dir = tempfile::tempdir().unwrap();
    let version = rs2_core::services::code::version_of(BUNDLE);
    let mut responses = HashMap::new();
    responses.insert(
        format!("https://{CAT_HOST}/cat.json"),
        (200, catalogue_doc(&version), "application/json".into()),
    );
    responses.insert(
        format!("https://{CAT_HOST}/greeter.js"),
        (200, BUNDLE.to_vec(), "application/javascript".into()),
    );
    let catalogues = json!([{ "name": "main", "url": format!("https://{CAT_HOST}/cat.json") }]);
    let rt = build(dir.path(), catalogues, vec![CAT_HOST.to_string()], responses);

    // Registered catalogues, allowlist-annotated.
    let mut resp = rt.handle(req(Method::GET, "/admin/catalogues")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let cats = body_json(&mut resp).await;
    assert_eq!(cats["enabled"], true);
    assert_eq!(cats["catalogues"][0]["allowlisted"], true);

    // Aggregated available: built-in services + built-in adapters + remote item.
    let mut resp = rt.handle(req(Method::GET, "/admin/catalogue/available")).await;
    let items = body_json(&mut resp).await["items"].as_array().unwrap().clone();
    assert!(items.iter().any(|i| i["source"] == "builtin" && i["kind"] == "service"));
    assert!(items.iter().any(|i| i["kind"] == "adapter" && i["ref"] == "builtin:mem"));
    let greeter = items.iter().find(|i| i["name"] == "greeter").unwrap();
    assert_eq!(greeter["source"], "catalogue");
    assert_eq!(greeter["installed"], false);
    assert_eq!(greeter["ref"], format!("code:greeter@{version}"));

    // Install: fetch → hash-pin → compile-check → store.
    let mut resp = rt
        .handle(req(Method::POST, "/admin/catalogue/install").with_json(&json!({
            "catalogue": "main", "name": "greeter", "version": version,
        })))
        .await;
    let status = resp.status;
    let installed = body_json(&mut resp).await;
    assert_eq!(status, Some(StatusCode::CREATED), "{installed:?}");
    assert_eq!(installed["ref"], format!("code:greeter@{version}"));

    // available now reports it installed.
    let mut resp = rt.handle(req(Method::GET, "/admin/catalogue/available")).await;
    let items = body_json(&mut resp).await["items"].as_array().unwrap().clone();
    let greeter = items.iter().find(|i| i["name"] == "greeter").unwrap();
    assert_eq!(greeter["installed"], true);

    // The bundle is readable from the code store at its content ref.
    let mut resp = rt.handle(req(Method::GET, &format!("/admin/code/greeter/{version}"))).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let bytes = resp.body.as_mut().unwrap().materialize(1 << 20).await.unwrap();
    assert_eq!(&bytes[..], BUNDLE);
}

#[tokio::test]
async fn install_rejects_non_allowlisted_host() {
    let dir = tempfile::tempdir().unwrap();
    // Catalogue registered on a host the operator did NOT allowlist.
    let catalogues = json!([{ "name": "evil", "url": "https://evil.test/cat.json" }]);
    let rt = build(dir.path(), catalogues, vec![CAT_HOST.to_string()], HashMap::new());

    let resp = rt
        .handle(req(Method::POST, "/admin/catalogue/install").with_json(&json!({
            "catalogue": "evil", "name": "x", "version": "y",
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN)); // capability_denied
}

#[tokio::test]
async fn install_rejects_content_hash_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let wrong = "0123456789abcdef"; // not the real version_of(BUNDLE)
    let mut responses = HashMap::new();
    responses.insert(
        format!("https://{CAT_HOST}/cat.json"),
        (200, catalogue_doc(wrong), "application/json".into()),
    );
    responses.insert(
        format!("https://{CAT_HOST}/greeter.js"),
        (200, BUNDLE.to_vec(), "application/javascript".into()),
    );
    let catalogues = json!([{ "name": "main", "url": format!("https://{CAT_HOST}/cat.json") }]);
    let rt = build(dir.path(), catalogues, vec![CAT_HOST.to_string()], responses);

    let resp = rt
        .handle(req(Method::POST, "/admin/catalogue/install").with_json(&json!({
            "catalogue": "main", "name": "greeter", "version": wrong,
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST)); // hash pin refuses
}

#[tokio::test]
async fn catalogue_feature_dormant_without_allowlist() {
    let dir = tempfile::tempdir().unwrap();
    let catalogues = json!([{ "name": "main", "url": format!("https://{CAT_HOST}/cat.json") }]);
    // Empty operator allowlist ⇒ catalogue client is None (feature off).
    let rt = build(dir.path(), catalogues, vec![], HashMap::new());

    // available still lists built-ins only.
    let mut resp = rt.handle(req(Method::GET, "/admin/catalogue/available")).await;
    let items = body_json(&mut resp).await["items"].as_array().unwrap().clone();
    assert!(!items.is_empty());
    assert!(items.iter().all(|i| i["source"] == "builtin"));

    // catalogues listing reports the feature off.
    let mut resp = rt.handle(req(Method::GET, "/admin/catalogues")).await;
    assert_eq!(body_json(&mut resp).await["enabled"], false);

    // install is unavailable.
    let resp = rt
        .handle(req(Method::POST, "/admin/catalogue/install").with_json(&json!({
            "catalogue": "main", "name": "greeter", "version": "x",
        })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::NOT_IMPLEMENTED)); // engine_unavailable
}
