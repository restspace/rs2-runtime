//! Store-pattern conformance (the polymorphism contract, carried over from
//! Restspace v1): any mount declaring `pattern: "store"` must satisfy one
//! conversation shape, so a single client codepath can drive every store.
//! This suite *is* the contract — it runs identically against `file` and
//! `data`, and any future store (S3, SQL, custom) joins by passing it.
//!
//! The shape:
//! - `GET <container>/` → `application/vnd.rs2.dir+json`
//!   `{path, entries: [{name, dir, ...}], total}` + `X-Total-Count`,
//!   paginated with `$take`/`$skip`, at every container level incl. the
//!   mount root. Directory entries have `dir: true`.
//! - `PUT <child>` → upsert: 201 created / 200 overwritten, empty body,
//!   `ETag` on the response.
//! - `POST <container>/` → keyless create: 201 + `Location` of the new
//!   child ("echo" facet stores also return the stored representation).
//! - `GET <child>` → the resource, with `ETag`.
//! - `DELETE <child>` → 204; subsequent GET → 404.
//! - `DELETE <container>` non-empty → 409; with `?confirm=<name>` → 204.
//! Facets (`range`, `patch`, `schema`, `echo`) are additive capabilities
//! and never change the core shape.

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

fn rt(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/files", "service": "file", "config": { "access": "open" } },
        { "path": "/data", "service": "data", "config": { "access": "open" } },
        { "path": "/q", "service": "query", "config": { "access": "open" } },
        { "path": "/pipes", "service": "pipeline", "config": { "access": "open" } }
    ]})));
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
        .as_json(1024 * 1024)
        .await
        .expect("json body")
}

/// The parameterized contract. `make_body` produces a valid child body for
/// this store; `container` has no trailing slash.
async fn assert_store_contract(
    rt: &Runtime,
    mount: &str,
    container: &str,
    make_body: impl Fn(u32) -> Body,
) {
    let child = |name: &str| format!("{mount}{container}/{name}");
    let container_path = format!("{mount}{container}/");

    // PUT child: 201 create, 200 overwrite, empty body, ETag.
    let resp = rt
        .handle(req(Method::PUT, &child("alpha")).with_body(make_body(1)))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::CREATED),
        "[{mount}] PUT create"
    );
    assert!(resp.body.is_none(), "[{mount}] PUT returns no body");
    assert!(
        resp.header("etag").is_some(),
        "[{mount}] PUT create carries ETag"
    );
    let resp = rt
        .handle(req(Method::PUT, &child("alpha")).with_body(make_body(2)))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "[{mount}] PUT overwrite");
    assert!(
        resp.header("etag").is_some(),
        "[{mount}] PUT overwrite carries ETag"
    );

    // GET child: the resource, with a version ETag.
    let resp = rt.handle(req(Method::GET, &child("alpha"))).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "[{mount}] GET child");
    let etag = resp
        .header("etag")
        .unwrap_or_else(|| panic!("[{mount}] child GET carries ETag"))
        .to_string();

    // Conditional write (the `conditional-write` facet): a matching If-Match
    // succeeds; a stale one is 412; If-None-Match: * refuses to clobber.
    let mut stale = req(Method::PUT, &child("alpha")).with_body(make_body(4));
    stale.set_header("if-match", "\"definitely-not-the-current-etag\"");
    let resp = rt.handle(stale).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::PRECONDITION_FAILED),
        "[{mount}] stale If-Match is 412"
    );
    let mut matched = req(Method::PUT, &child("alpha")).with_body(make_body(5));
    matched.set_header("if-match", &etag);
    let resp = rt.handle(matched).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::OK),
        "[{mount}] matching If-Match writes"
    );
    let mut create_only = req(Method::PUT, &child("alpha")).with_body(make_body(6));
    create_only.set_header("if-none-match", "*");
    let resp = rt.handle(create_only).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::PRECONDITION_FAILED),
        "[{mount}] If-None-Match: * refuses an existing child"
    );

    // POST container: keyless create with Location; the child is fetchable.
    let resp = rt
        .handle(req(Method::POST, &container_path).with_body(make_body(3)))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::CREATED),
        "[{mount}] keyless POST: {:?}",
        resp.body
    );
    let location = resp
        .header("location")
        .unwrap_or_else(|| panic!("[{mount}] keyless POST returns Location"))
        .to_string();
    assert!(
        location.starts_with(&container_path),
        "[{mount}] Location under container"
    );
    let resp = rt.handle(req(Method::GET, &location)).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::OK),
        "[{mount}] created child fetchable"
    );

    // Container listing: one shape, one media type, paginated.
    let mut resp = rt.handle(req(Method::GET, &container_path)).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "[{mount}] container GET");
    let ct = resp.body.as_ref().unwrap().media_type.essence().to_string();
    assert_eq!(
        ct, "application/vnd.rs2.dir+json",
        "[{mount}] listing media type"
    );
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert!(total >= 2, "[{mount}] X-Total-Count counts both children");
    let listing = body_json(&mut resp).await;
    assert!(listing["path"].is_string(), "[{mount}] listing.path");
    assert_eq!(
        listing["total"].as_u64(),
        Some(total),
        "[{mount}] listing.total"
    );
    let entries = listing["entries"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e["name"] == "alpha" && e["dir"] == false),
        "[{mount}] child appears as an entry: {listing}"
    );

    // Pagination narrows entries, not the reported total.
    let mut resp = rt
        .handle(req(Method::GET, &format!("{container_path}?$take=1")))
        .await;
    let page = body_json(&mut resp).await;
    assert_eq!(
        page["entries"].as_array().unwrap().len(),
        1,
        "[{mount}] $take pages"
    );
    assert_eq!(
        page["total"].as_u64(),
        Some(total),
        "[{mount}] paged total is the full count"
    );

    // Mount root also lists, and shows the container as a directory entry.
    let mut resp = rt.handle(req(Method::GET, &format!("{mount}/"))).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::OK),
        "[{mount}] mount root lists"
    );
    let root = body_json(&mut resp).await;
    let leaf = container.trim_start_matches('/');
    assert!(
        root["entries"].as_array().unwrap().iter().any(|e| e["name"]
            .as_str()
            .unwrap_or("")
            .trim_end_matches('/')
            == leaf
            && e["dir"] == true),
        "[{mount}] container is a dir entry at the root: {root}"
    );

    // DELETE child: 204, then gone.
    let resp = rt.handle(req(Method::DELETE, &child("alpha"))).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::NO_CONTENT),
        "[{mount}] DELETE child"
    );
    let resp = rt.handle(req(Method::GET, &child("alpha"))).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::NOT_FOUND),
        "[{mount}] deleted child is gone"
    );

    // Container guard: non-empty delete refuses with 409; confirm succeeds.
    let resp = rt.handle(req(Method::DELETE, &container_path)).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::CONFLICT),
        "[{mount}] non-empty container delete is 409 without confirm"
    );
    let resp = rt
        .handle(req(
            Method::DELETE,
            &format!("{container_path}?confirm={leaf}"),
        ))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::NO_CONTENT),
        "[{mount}] confirmed delete"
    );
    let mut resp = rt.handle(req(Method::GET, &format!("{mount}/"))).await;
    let root = body_json(&mut resp).await;
    assert!(
        !root["entries"].as_array().unwrap().iter().any(|e| e["name"]
            .as_str()
            .unwrap_or("")
            .trim_end_matches('/')
            == leaf),
        "[{mount}] deleted container left the root listing: {root}"
    );
}

#[tokio::test]
async fn file_service_satisfies_the_store_contract() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    assert_store_contract(&rt, "/files", "/docs", |i| {
        Body::from_string(format!("content-{i}"), MediaType::new("text/plain"))
    })
    .await;
}

#[tokio::test]
async fn data_service_satisfies_the_store_contract() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    assert_store_contract(&rt, "/data", "/things", |i| {
        Body::from_json(&json!({ "n": i }))
    })
    .await;
}

/// Spec stores' authoring subtrees satisfy the same contract as file and
/// data — by construction: they delegate to an owned FileService (the
/// SpecStore façade), so this suite tests one implementation through
/// several doors.
#[tokio::test]
async fn query_authoring_subtree_satisfies_the_store_contract() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    assert_store_contract(&rt, "/q/.queries", "/reports", |i| {
        Body::from_json(&json!({ "query": { "dataset": "orders", "v": i } }))
    })
    .await;
}

#[tokio::test]
async fn pipeline_authoring_subtree_satisfies_the_store_contract() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    assert_store_contract(&rt, "/pipes/.pipelines", "/flows", |i| {
        Body::from_json(&json!({ "pipeline": [ format!("GET /step{i}") ] }))
    })
    .await;
}

/// Facets are additive: they must not alter the shared shape.
#[tokio::test]
async fn facets_extend_without_forking_the_shape() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    // data "echo" facet: POST to a child upserts AND returns the stored
    // representation (PUT stays empty-bodied on every store).
    let mut resp = rt
        .handle(req(Method::POST, "/data/things/alpha").with_json(&json!({ "n": 9 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    assert_eq!(body_json(&mut resp).await["n"], 9);

    // data "schema" facet: the schema is a fixed child visible in the listing.
    let schema = json!({ "type": "object" });
    let put = req(Method::PUT, "/data/things/.schema.json").with_json(&schema);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::OK));
    let mut listing = rt.handle(req(Method::GET, "/data/things/")).await;
    let listing = body_json(&mut listing).await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == ".schema.json"),
        "{listing}"
    );

    // The discovery surface declares pattern + facets per mount.
    let mut services = rt
        .handle(req(Method::GET, "/.well-known/rs2/services"))
        .await;
    let doc = body_json(&mut services).await;
    let by_path = |p: &str| {
        doc["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["path"] == p)
            .cloned()
            .unwrap_or_else(|| panic!("mount {p} missing from {doc}"))
    };
    assert_eq!(by_path("/files")["pattern"], "store");
    assert_eq!(by_path("/data")["pattern"], "store");
    assert!(by_path("/data")["facets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f == "schema"));
    assert!(by_path("/files")["facets"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f == "range"));
}
