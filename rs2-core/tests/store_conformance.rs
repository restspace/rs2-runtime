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
//!
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

    // Conditional delete: the DELETE side of the `conditional-write`
    // contract — a stale If-Match refuses (412) and leaves the child alone.
    let mut stale = req(Method::DELETE, &child("alpha"));
    stale.set_header("if-match", "\"definitely-not-the-current-etag\"");
    let resp = rt.handle(stale).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::PRECONDITION_FAILED),
        "[{mount}] stale If-Match DELETE is 412"
    );
    let resp = rt.handle(req(Method::GET, &child("alpha"))).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::OK),
        "[{mount}] refused delete left the child in place"
    );
    let etag = resp
        .header("etag")
        .unwrap_or_else(|| panic!("[{mount}] child GET carries ETag"))
        .to_string();

    // DELETE child (matching If-Match): 204, then gone.
    let mut matched = req(Method::DELETE, &child("alpha"));
    matched.set_header("if-match", &etag);
    let resp = rt.handle(matched).await;
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

/// The projected-listing contract (the `list-projection` facet): `$select`
/// projects fields into entries, `$sort` orders by the pinned semantics
/// (binary UTF-8 code points; missing < null < false < true < numbers <
/// strings; key as final tiebreak), pagination pages the sorted whole. Every
/// `DataStore` — host fallback or native pushdown — must produce exactly
/// this output over the same records.
async fn assert_listing_contract(rt: &Runtime, mount: &str) {
    let put = |key: &str, val: serde_json::Value| {
        let path = format!("{mount}/posts/{key}");
        async move {
            let resp = rt
                .handle(req(Method::PUT, &path).with_body(Body::from_json(&val)))
                .await;
            assert_eq!(resp.status, Some(StatusCode::CREATED), "seed {path}");
        }
    };
    put(
        "ka",
        json!({ "title": "apple",  "n": 5,  "meta": { "date": "2026-01-02" } }),
    )
    .await;
    put(
        "kb",
        json!({ "title": "Zebra",  "n": 2,  "meta": { "date": "2026-01-03" } }),
    )
    .await;
    put("kc", json!({ "title": "banana", "n": 2 })).await;
    put(
        "kd",
        json!({ "title": "cherry", "n": 10, "meta": { "date": "2026-01-01" } }),
    )
    .await;

    let names = |listing: &serde_json::Value| -> Vec<String> {
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap().to_string())
            .collect()
    };

    // $select: dir+json entries gain `fields` (projected, nested shape kept,
    // absent paths omitted); no `.schema.json` fixed entry in table data.
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/posts/?$select=title,meta.date"),
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "[{mount}] $select lists");
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "application/vnd.rs2.dir+json",
        "[{mount}] projected listing keeps the listing media type"
    );
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert_eq!(total, 4, "[{mount}] projected listing counts records");
    let listing = body_json(&mut resp).await;
    assert_eq!(listing["total"].as_u64(), Some(4));
    let entries = listing["entries"].as_array().unwrap();
    assert!(
        entries.iter().all(|e| e["name"] != ".schema.json"),
        "[{mount}] no fixed entries in a projected listing: {listing}"
    );
    let ka = entries.iter().find(|e| e["name"] == "ka").unwrap();
    assert_eq!(
        ka["fields"],
        json!({ "title": "apple", "meta": { "date": "2026-01-02" } }),
        "[{mount}] projection keeps nested shape"
    );
    let kc = entries.iter().find(|e| e["name"] == "kc").unwrap();
    assert_eq!(
        kc["fields"],
        json!({ "title": "banana" }),
        "[{mount}] absent path omitted, not an error"
    );

    // $sort asc: binary code-point order — "Zebra" before "apple".
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/posts/?$select=title&$sort=title"),
        ))
        .await;
    let listing = body_json(&mut resp).await;
    assert_eq!(
        names(&listing),
        ["kb", "ka", "kc", "kd"],
        "[{mount}] code-point ascending sort"
    );

    // Multi-key with direction: -n then title; the n=2 tie breaks by title.
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/posts/?$select=title&$sort=-n,title"),
        ))
        .await;
    let listing = body_json(&mut resp).await;
    assert_eq!(
        names(&listing),
        ["kd", "ka", "kb", "kc"],
        "[{mount}] multi-key sort with descending first key"
    );

    // A missing sort field is smallest: first ascending.
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/posts/?$select=title&$sort=meta.date"),
        ))
        .await;
    let listing = body_json(&mut resp).await;
    assert_eq!(
        names(&listing),
        ["kc", "kd", "ka", "kb"],
        "[{mount}] missing sort field sorts first ascending"
    );

    // Pagination pages the *sorted* sequence; total stays the full count.
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/posts/?$select=title&$sort=title&$take=2&$skip=1"),
        ))
        .await;
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert_eq!(
        total, 4,
        "[{mount}] paged projected total is the full count"
    );
    let listing = body_json(&mut resp).await;
    assert_eq!(
        names(&listing),
        ["ka", "kc"],
        "[{mount}] pagination applies after the sort"
    );

    // Malformed specs are client errors, never silently ignored.
    let resp = rt
        .handle(req(Method::GET, &format!("{mount}/posts/?$select=a..b")))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::BAD_REQUEST),
        "[{mount}] malformed $select path is 400"
    );
    let resp = rt
        .handle(req(Method::GET, &format!("{mount}/posts/?$sort=title")))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::BAD_REQUEST),
        "[{mount}] $sort without $select is 400, not ignored"
    );

    // A plain listing is unchanged by the feature existing: no `fields`.
    let mut resp = rt
        .handle(req(Method::GET, &format!("{mount}/posts/")))
        .await;
    let listing = body_json(&mut resp).await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e.get("fields").is_none()),
        "[{mount}] plain listing carries no fields objects: {listing}"
    );

    // Cleanup so the caller's store is reusable.
    let resp = rt
        .handle(req(
            Method::DELETE,
            &format!("{mount}/posts/?confirm=posts"),
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::NO_CONTENT));
}

/// The metadata-sort contract (the `meta-sort` facet): `$sort` over
/// `@`-prefixed listing metadata orders any file-pattern listing without
/// content reads — same comparison semantics as the projected-listing
/// contract (binary strings, missing-first ascending, name tiebreak),
/// pagination after the sort, unknown/unprefixed keys a 400.
async fn assert_meta_sort_contract(rt: &Runtime, mount: &str, make_body: impl Fn(usize) -> Body) {
    // Distinct sizes (body length scales with i) + a subdirectory.
    for (name, i) in [("bb", 3), ("aa", 1), ("cc", 2)] {
        let resp = rt
            .handle(req(Method::PUT, &format!("{mount}/msort/{name}")).with_body(make_body(i)))
            .await;
        assert_eq!(resp.status, Some(StatusCode::CREATED), "seed {name}");
    }
    let resp = rt
        .handle(req(Method::PUT, &format!("{mount}/msort/sub/inner")).with_body(make_body(1)))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "seed sub/inner");

    let names = |listing: &serde_json::Value| -> Vec<String> {
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap().to_string())
            .collect()
    };

    // @name descending (code-point order, dirs by their slashed name).
    let mut resp = rt
        .handle(req(Method::GET, &format!("{mount}/msort/?$sort=-@name")))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "[{mount}] -@name lists");
    let listing = body_json(&mut resp).await;
    assert_eq!(
        names(&listing),
        ["sub/", "cc", "bb", "aa"],
        "[{mount}] -@name order"
    );

    // -@size with @name tiebreak: sizes scale with the seed index; the dir
    // (size 0) and the smallest file tie region stays name-deterministic.
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/msort/?$sort=-@size,@name"),
        ))
        .await;
    let listing = body_json(&mut resp).await;
    let got = names(&listing);
    assert_eq!(got[0], "bb", "[{mount}] largest first: {got:?}");
    assert_eq!(got[1], "cc", "[{mount}] then next: {got:?}");

    // Pagination applies after the sort; total is the full count.
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/msort/?$sort=-@name&$take=2&$skip=1"),
        ))
        .await;
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert_eq!(total, 4, "[{mount}] meta-sorted total is the full count");
    let listing = body_json(&mut resp).await;
    assert_eq!(
        names(&listing),
        ["cc", "bb"],
        "[{mount}] pagination after the meta sort"
    );

    // @lastModified sorts without error and keeps every entry (mtime
    // granularity makes strict order assertions flaky; the name tiebreak
    // keeps the result deterministic per run).
    let mut resp = rt
        .handle(req(
            Method::GET,
            &format!("{mount}/msort/?$sort=-@lastModified"),
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let listing = body_json(&mut resp).await;
    assert_eq!(listing["entries"].as_array().unwrap().len(), 4);

    // Unknown and unprefixed keys are client errors, never ignored.
    for bad in ["@nope", "name", "-size"] {
        let resp = rt
            .handle(req(Method::GET, &format!("{mount}/msort/?$sort={bad}")))
            .await;
        assert_eq!(
            resp.status,
            Some(StatusCode::BAD_REQUEST),
            "[{mount}] $sort={bad} is 400"
        );
    }

    // The plain listing is untouched by the feature existing.
    let mut resp = rt
        .handle(req(Method::GET, &format!("{mount}/msort/")))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let listing = body_json(&mut resp).await;
    assert_eq!(listing["entries"].as_array().unwrap().len(), 4);

    // Cleanup.
    let resp = rt
        .handle(req(
            Method::DELETE,
            &format!("{mount}/msort/?confirm=msort"),
        ))
        .await;
    assert_eq!(resp.status, Some(StatusCode::NO_CONTENT));
}

#[tokio::test]
async fn file_service_satisfies_the_meta_sort_contract() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    assert_meta_sort_contract(&rt, "/files", |i| {
        Body::from_string("x".repeat(i * 100), MediaType::new("text/plain"))
    })
    .await;
}

/// Spec stores delegate their authoring subtrees to an owned FileService, so
/// they inherit the meta-sort facet — held to it here through the query
/// store's door.
#[tokio::test]
async fn query_authoring_subtree_satisfies_the_meta_sort_contract() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    assert_meta_sort_contract(&rt, "/q/.queries", |i| {
        Body::from_json(&json!({ "query": { "dataset": "orders", "pad": "x".repeat(i * 100) } }))
    })
    .await;
}

#[tokio::test]
async fn data_service_satisfies_the_listing_contract_mem() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    assert_listing_contract(&rt, "/data").await;
}

/// The same contract over the file-backed `DataStore` (the production node
/// default) — the default trait fallback through a second adapter.
#[tokio::test]
async fn data_service_satisfies_the_listing_contract_file_backed() {
    use rs2_core::adapters::FileDataStore;
    let file_dir = tempfile::tempdir().unwrap();
    let data_dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_dir.path())),
        Arc::new(FileDataStore::new(Arc::new(LocalFsFileStore::new(
            data_dir.path(),
        )))),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/data", "service": "data", "config": { "access": "open" } }
    ]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );
    assert_listing_contract(&rt, "/data").await;
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
