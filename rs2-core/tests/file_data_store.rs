//! `FileDataStore`: the file-backed `DataStore`. Two angles — direct trait
//! parity against `MemDataStore` (the reference semantics), and the store
//! contract end-to-end through the `data` service via `store.adapter =
//! "builtin:file"`, including durability across a simulated restart.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{FileDataStore, LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::{DataStore, FileStore};
use rs2_core::message::Message;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

// `code` is a stable contract string (error.rs `codes`).
const NOT_FOUND: &str = "not_found";

// ---- direct trait parity vs MemDataStore --------------------------------

/// The same sequence of `DataStore` calls, asserting behavior both adapters
/// must share: created-vs-updated, missing → not_found, schema-only datasets
/// list empty, listings, and delete semantics.
async fn assert_datastore_contract(store: &dyn DataStore) {
    // Missing record / missing dataset are not_found.
    assert_eq!(store.get("t", "ds", "nope").await.unwrap_err().code, NOT_FOUND);
    assert_eq!(store.list_keys("t", "ds", 100, 0).await.unwrap_err().code, NOT_FOUND);

    // put returns created (true) then updated (false); get reads back.
    assert!(store.put("t", "ds", "k1", json!({ "v": 1 })).await.unwrap());
    assert!(!store.put("t", "ds", "k1", json!({ "v": 2 })).await.unwrap());
    assert_eq!(store.get("t", "ds", "k1").await.unwrap(), json!({ "v": 2 }));

    // Listings: keys within a dataset, datasets at the root.
    store.put("t", "ds", "k2", json!({ "v": 3 })).await.unwrap();
    let (mut keys, total) = store.list_keys("t", "ds", 100, 0).await.unwrap();
    keys.sort();
    assert_eq!((keys, total), (vec!["k1".to_string(), "k2".to_string()], 2));
    let (datasets, dtotal) = store.list_datasets("t", 100, 0).await.unwrap();
    assert_eq!((datasets, dtotal), (vec!["ds".to_string()], 1));

    // Schema round-trips; a schema-only dataset lists empty (not not_found).
    assert_eq!(store.get_schema("t", "ds").await.unwrap(), None);
    store.put_schema("t", "empties", json!({ "type": "object" })).await.unwrap();
    assert_eq!(store.get_schema("t", "empties").await.unwrap(), Some(json!({ "type": "object" })));
    assert_eq!(store.list_keys("t", "empties", 100, 0).await.unwrap(), (vec![], 0));

    // Delete record (idempotency: second delete is not_found), then dataset.
    store.delete("t", "ds", "k1").await.unwrap();
    assert_eq!(store.get("t", "ds", "k1").await.unwrap_err().code, NOT_FOUND);
    assert_eq!(store.delete("t", "ds", "k1").await.unwrap_err().code, NOT_FOUND);
    store.delete_dataset("t", "ds").await.unwrap();
    assert_eq!(store.list_keys("t", "ds", 100, 0).await.unwrap_err().code, NOT_FOUND);
    assert_eq!(store.delete_dataset("t", "nope").await.unwrap_err().code, NOT_FOUND);
}

#[tokio::test]
async fn file_data_store_matches_mem_data_store() {
    let dir = tempfile::tempdir().unwrap();
    let file: Arc<dyn FileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    assert_datastore_contract(&FileDataStore::new(file)).await;
    assert_datastore_contract(&MemDataStore::new()).await;
}

#[tokio::test]
async fn records_survive_a_dropped_file_store_handle() {
    let dir = tempfile::tempdir().unwrap();
    let key = "user@example.com"; // exercises key encoding on disk
    {
        let file: Arc<dyn FileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
        FileDataStore::new(file).put("t", "users", key, json!({ "role": "A" })).await.unwrap();
    }
    // A fresh adapter over the same root still sees the record — unlike mem.
    let file: Arc<dyn FileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let got = FileDataStore::new(file).get("t", "users", key).await.unwrap();
    assert_eq!(got, json!({ "role": "A" }));
}

// ---- end-to-end through the data service via builtin:file ----------------

struct StaticLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

/// A runtime with a `data` mount selecting `builtin:file` over `data_root`.
/// Re-call with the same `data_root` to simulate a node restart.
fn runtime(file_root: &std::path::Path, data_root: &std::path::Path) -> Arc<Runtime> {
    let mut adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(file_root)), Arc::new(MemDataStore::new()));
    let data_fs: Arc<dyn FileStore> = Arc::new(LocalFsFileStore::new(data_root));
    adapters.builtins.register_data(
        "file",
        Arc::new(move |_cfg| Ok(Arc::new(FileDataStore::new(data_fs.clone())) as Arc<dyn DataStore>)),
    );
    let loader = Arc::new(StaticLoader(json!({
        "mounts": [
            { "path": "/data", "service": "data", "config": { "store": { "adapter": "builtin:file" } } }
        ]
    })));
    Runtime::new(Tenancy::Single { tenant: "t1".into() }, adapters, loader, LimitTable::default())
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t1")
}

#[tokio::test]
async fn data_service_store_contract_over_builtin_file() {
    let files = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let rt = runtime(files.path(), data.path());

    // PUT creates → 201; re-PUT overwrites → 200.
    assert_eq!(
        rt.handle(req(Method::PUT, "/data/orders/o1").with_json(&json!({ "total": 5 }))).await.status,
        Some(StatusCode::CREATED)
    );
    assert_eq!(
        rt.handle(req(Method::PUT, "/data/orders/o1").with_json(&json!({ "total": 7 }))).await.status,
        Some(StatusCode::OK)
    );

    // GET returns the record with an ETag.
    let mut resp = rt.handle(req(Method::GET, "/data/orders/o1")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert!(resp.header("etag").is_some());
    assert_eq!(resp.body.as_mut().unwrap().as_json(65536).await.unwrap()["total"], 7);

    // Dataset listing (dir+json) and the root dataset enumeration.
    rt.handle(req(Method::PUT, "/data/orders/o2").with_json(&json!({ "total": 9 }))).await;
    let mut resp = rt.handle(req(Method::GET, "/data/orders/")).await;
    let listing = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(listing["total"], 2);
    let mut resp = rt.handle(req(Method::GET, "/data/")).await;
    let roots = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(roots["entries"][0]["name"], "orders/");

    // DELETE a record → 204; the dataset delete needs the confirm token.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/orders/o2")).await.status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/orders/")).await.status,
        Some(StatusCode::CONFLICT)
    );
}

#[tokio::test]
async fn records_persist_across_restart() {
    let files = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();

    // First "boot": write a record.
    let rt = runtime(files.path(), data.path());
    assert_eq!(
        rt.handle(req(Method::PUT, "/data/users/ada").with_json(&json!({ "role": "A" }))).await.status,
        Some(StatusCode::CREATED)
    );
    drop(rt);

    // Second "boot" over the same data root: the record is still there.
    let rt = runtime(files.path(), data.path());
    let mut resp = rt.handle(req(Method::GET, "/data/users/ada")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.body.as_mut().unwrap().as_json(65536).await.unwrap()["role"], "A");
}
