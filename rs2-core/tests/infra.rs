//! Infras (PRD §9.1): operator-defined named partial adapter configs that a
//! tenant references as `infra:<name>` and completes. Covers capability-adapter
//! resolution, spec-store backing, `allowedTenants`/`requires`/`infraOnly`
//! enforcement, the tenant-facing listing (secrets redacted), and live reload.
//! Runs under default features — the builtin path never touches the JS engine.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::infra::{InfraLoader, InfraSet};
use rs2_core::message::Message;
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

/// A reloadable infra source backed by a shared JSON cell, so a test can swap
/// the operator's `infras.json` and call `reload_infras`.
struct CellLoader(Arc<Mutex<serde_json::Value>>);

impl InfraLoader for CellLoader {
    fn load(&self) -> Result<InfraSet, RsError> {
        InfraSet::from_json(self.0.lock().unwrap().clone())
    }
}

/// Build a single-tenant runtime over `tenant` with the given mounts and infra
/// document. Returns the runtime and the shared infra cell (for reload tests).
fn rt_with(
    tenant: &str,
    mounts: serde_json::Value,
    infras: serde_json::Value,
    file_root: &std::path::Path,
) -> (Arc<Runtime>, Arc<Mutex<serde_json::Value>>) {
    let cell = Arc::new(Mutex::new(infras));
    let loader = Arc::new(CellLoader(cell.clone()));
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    )
    .with_infras(loader.load().unwrap())
    .with_infra_loader(loader);
    let config = Arc::new(StaticLoader(json!({ "mounts": mounts })));
    let rt = Runtime::new(
        Tenancy::Single { tenant: tenant.into() },
        adapters,
        config,
        LimitTable::default(),
    );
    (rt, cell)
}

fn req(tenant: &str, method: Method, path: &str) -> Message {
    Message::request(method, path, tenant)
}

async fn body_text(resp: &mut Message) -> String {
    match resp.body.as_mut() {
        Some(b) => String::from_utf8_lossy(&b.materialize(65536).await.unwrap()).to_string(),
        None => String::new(),
    }
}

async fn body_json(resp: &mut Message) -> serde_json::Value {
    resp.body.as_mut().unwrap().as_json(65536).await.unwrap()
}

// ---------------------------------------------------------------------------
// capability-adapter resolution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn infra_backed_data_mount_resolves_and_serves() {
    let dir = tempfile::tempdir().unwrap();
    let (rt, _) = rt_with(
        "t1",
        json!([
            { "path": "/d", "service": "data", "config": {
                "access": "open", "store": { "adapter": "infra:shared", "prefix": "p" } } }
        ]),
        json!({
            "shared": {
                "adapter": "builtin:mem",
                "config": { "secretKey": "shh" },
                "requires": ["prefix"]
            }
        }),
        dir.path(),
    );

    // The infra resolved to builtin:mem; the mount serves a normal CRUD cycle.
    let resp = rt.handle(req("t1", Method::PUT, "/d/things/x").with_json(&json!({ "v": 1 }))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.status);
    let mut got = rt.handle(req("t1", Method::GET, "/d/things/x")).await;
    assert_eq!(got.status, Some(StatusCode::OK));
    assert_eq!(body_json(&mut got).await["v"], 1);
}

#[tokio::test]
async fn infra_enforces_allowed_tenants_requires_and_infra_only() {
    let dir = tempfile::tempdir().unwrap();
    let infras = json!({
        "locked": {
            "adapter": "builtin:mem",
            "allowedTenants": ["other"],
            "requires": ["prefix"],
            "infraOnly": ["tenantDirectories"]
        }
    });

    // allowedTenants: t1 is not on the list → 400/403 at build (surfaced per request).
    let (rt, _) = rt_with(
        "t1",
        json!([{ "path": "/d", "service": "data", "config": {
            "store": { "adapter": "infra:locked", "prefix": "p" } } }]),
        infras.clone(),
        dir.path(),
    );
    let resp = rt.handle(req("t1", Method::GET, "/d/x")).await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN));

    // missing required `prefix` → 400.
    let (rt, _) = rt_with(
        "other",
        json!([{ "path": "/d", "service": "data", "config": {
            "store": { "adapter": "infra:locked" } } }]),
        infras.clone(),
        dir.path(),
    );
    let mut resp = rt.handle(req("other", Method::GET, "/d/x")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    assert!(body_text(&mut resp).await.contains("requires"));

    // tenant setting an infra-only field → 400.
    let (rt, _) = rt_with(
        "other",
        json!([{ "path": "/d", "service": "data", "config": {
            "store": { "adapter": "infra:locked", "prefix": "p", "tenantDirectories": false } } }]),
        infras,
        dir.path(),
    );
    let mut resp = rt.handle(req("other", Method::GET, "/d/x")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    assert!(body_text(&mut resp).await.contains("forbids"));
}

#[tokio::test]
async fn unknown_infra_is_a_config_400() {
    let dir = tempfile::tempdir().unwrap();
    let (rt, _) = rt_with(
        "t1",
        json!([{ "path": "/d", "service": "data", "config": {
            "store": { "adapter": "infra:ghost" } } }]),
        json!({ "real": { "adapter": "builtin:mem" } }),
        dir.path(),
    );
    let mut resp = rt.handle(req("t1", Method::GET, "/d/x")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    let text = body_text(&mut resp).await;
    assert!(text.contains("unknown infra") && text.contains("real"), "{text}");
}

// ---------------------------------------------------------------------------
// spec stores backed by an infra (specStore, independent of store.adapter)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_specs_stored_via_infra_backend() {
    let dir = tempfile::tempdir().unwrap();
    let (rt, _) = rt_with(
        "t1",
        json!([
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/p", "service": "pipeline", "config": {
                "access": "open", "specStore": { "adapter": "infra:specs" } } }
        ]),
        // The infra fronts the node file store (where specs live by default),
        // proving the specStore backend resolves through the infra machinery.
        json!({ "specs": { "adapter": "builtin:local", "description": "managed spec store" } }),
        dir.path(),
    );

    // Seed data, author the .root spec into the infra-backed store, execute it.
    let put = req("t1", Method::PUT, "/data/orders/o1").with_json(&json!({ "status": "open" }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    let spec = json!({ "pipeline": [ "GET /data/orders/o1", { "status": "$.status" } ] });
    let put = req("t1", Method::PUT, "/p/.pipelines/.root").with_json(&spec);
    let mut authored = rt.handle(put).await;
    let status = authored.status;
    let detail = body_text(&mut authored).await;
    assert_eq!(status, Some(StatusCode::CREATED), "spec authored to infra store: {detail}");
    // It reads back as a store child (proving it landed in the infra-backed
    // store), and executes.
    let read = rt.handle(req("t1", Method::GET, "/p/.pipelines/.root")).await;
    assert_eq!(read.status, Some(StatusCode::OK));
    let mut run = rt.handle(req("t1", Method::GET, "/p/run")).await;
    let run_status = run.status;
    let run_body = body_text(&mut run).await;
    assert_eq!(run_status, Some(StatusCode::OK), "pipeline executed: {run_body}");
    assert_eq!(serde_json::from_str::<serde_json::Value>(&run_body).unwrap()["status"], "open");
}

// ---------------------------------------------------------------------------
// tenant-facing listing (GET /services/infras)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn infra_listing_filters_by_tenant_and_redacts_values() {
    let dir = tempfile::tempdir().unwrap();
    let (rt, _) = rt_with(
        "t1",
        json!([{ "path": "/services", "service": "services", "config": { "access": "open" } }]),
        json!({
            "shared": {
                "adapter": "builtin:mem",
                "description": "shared store",
                "config": { "region": "eu", "secretKey": "shh" },
                "requires": ["prefix"],
                "infraOnly": ["tenantDirectories"]
            },
            "private": {
                "adapter": "builtin:mem",
                "allowedTenants": ["someone-else"],
                "config": { "secretKey": "nope" }
            }
        }),
        dir.path(),
    );

    let mut resp = rt.handle(req("t1", Method::GET, "/services/infras")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let doc = body_json(&mut resp).await;
    let items = doc["infras"].as_array().unwrap();

    // `private` is filtered out (t1 not on its allowlist); only `shared` shows.
    assert_eq!(items.len(), 1, "{doc}");
    let s = &items[0];
    assert_eq!(s["name"], "shared");
    assert_eq!(s["description"], "shared store");
    assert_eq!(s["adapterKind"], "builtin");
    assert_eq!(s["requires"][0], "prefix");
    assert_eq!(s["infraOnly"][0], "tenantDirectories");
    // Provided keys are surfaced, but never their values (no secrets leak).
    let provided = s["providedKeys"].as_array().unwrap();
    assert!(provided.iter().any(|k| k == "region") && provided.iter().any(|k| k == "secretKey"));
    let whole = doc.to_string();
    assert!(!whole.contains("shh") && !whole.contains("\"eu\""), "values redacted: {whole}");
}

// ---------------------------------------------------------------------------
// live reload (no restart)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reload_infras_swaps_set_and_purges_tenants() {
    let dir = tempfile::tempdir().unwrap();
    let (rt, cell) = rt_with(
        "t1",
        json!([{ "path": "/d", "service": "data", "config": {
            "access": "open", "store": { "adapter": "infra:a" } } }]),
        json!({ "a": { "adapter": "builtin:mem" } }),
        dir.path(),
    );

    // Works with infra "a".
    let resp = rt.handle(req("t1", Method::PUT, "/d/things/x").with_json(&json!({ "v": 1 }))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));

    // Operator edits infras.json: "a" gone, "b" added — then reload.
    *cell.lock().unwrap() = json!({ "b": { "adapter": "builtin:mem" } });
    let names = rt.reload_infras().await.unwrap();
    assert_eq!(names, vec!["b".to_string()]);

    // The tenant was purged and rebuilds against the new set: infra:a is now
    // unknown, so the mount fails to build → 400.
    let mut resp = rt.handle(req("t1", Method::GET, "/d/things/x")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
    assert!(body_text(&mut resp).await.contains("unknown infra"));
}

#[tokio::test]
async fn reload_without_source_reports_unavailable() {
    // An Adapters with no infra loader: reload has nothing to read.
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t1".into() },
        adapters,
        Arc::new(StaticLoader(json!({ "mounts": [] }))),
        LimitTable::default(),
    );
    let err = rt.reload_infras().await.unwrap_err();
    assert_eq!(err.status, 501, "no infra source ⇒ engine_unavailable");
}
