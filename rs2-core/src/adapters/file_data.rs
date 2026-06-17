//! File-backed [`DataStore`]: a thin translation of the data-store contract
//! onto any [`FileStore`]. Records become JSON files, so a node gains durable
//! data persistence without a database — and any future file store (S3, …)
//! becomes a data backend for free.
//!
//! Layout (tenant scoping is applied by the caller, as for every adapter):
//! ```text
//! /<dataset>/records/<enc-key>.json   one file per record
//! /<dataset>/schema.json              the dataset schema (optional)
//! ```
//! The `records/` subdir keeps record files from ever colliding with the
//! schema (or future per-dataset metadata), so record keys need no reserved
//! blocklist. Keys are percent-encoded to safe filenames, so `/` can't escape
//! the dataset and exotic keys (emails, etc.) round-trip exactly.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::capabilities::{DataStore, FileStore};
use crate::error::{codes, RsError};
use crate::message::{Body, MediaType};

/// Safety cap for materializing a single record/schema file (records are
/// small; this only guards against a pathological file).
const RECORD_CAP: u64 = 64 * 1024 * 1024;

/// A [`DataStore`] backed by a [`FileStore`]. Wraps the trait object, so it
/// composes over the local filesystem today and any future file store later.
pub struct FileDataStore {
    inner: Arc<dyn FileStore>,
}

impl FileDataStore {
    pub fn new(inner: Arc<dyn FileStore>) -> Self {
        FileDataStore { inner }
    }
}

fn dataset_dir(dataset: &str) -> String {
    format!("/{dataset}/")
}

fn records_dir(dataset: &str) -> String {
    format!("/{dataset}/records/")
}

fn record_path(dataset: &str, key: &str) -> String {
    format!("/{dataset}/records/{}.json", encode_key(key))
}

fn schema_path(dataset: &str) -> String {
    format!("/{dataset}/schema.json")
}

/// Percent-encode a record key to a safe single path segment. Unreserved
/// bytes (`A-Za-z0-9._-`) pass through; everything else (notably `/`) becomes
/// `%XX`. `.json` is always appended, so an encoded key is never exactly `.`
/// or `..`.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Inverse of [`encode_key`].
fn decode_key(name: &str) -> String {
    let bytes = name.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&name[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[async_trait]
impl DataStore for FileDataStore {
    async fn get(&self, tenant: &str, dataset: &str, key: &str) -> Result<Value, RsError> {
        let mut body = self
            .inner
            .read(tenant, &record_path(dataset, key), None)
            .await
            .map_err(|e| missing_record(e, dataset, key))?;
        let bytes = body.materialize(RECORD_CAP).await?.clone();
        serde_json::from_slice(&bytes).map_err(|e| {
            RsError::internal(format!("record '{key}' in dataset '{dataset}' is not valid JSON: {e}"))
        })
    }

    async fn put(&self, tenant: &str, dataset: &str, key: &str, value: Value) -> Result<bool, RsError> {
        let bytes = serde_json::to_vec(&value).map_err(|e| RsError::internal(e.to_string()))?;
        self.inner
            .write(tenant, &record_path(dataset, key), Body::from_bytes(bytes, MediaType::json()))
            .await
    }

    async fn delete(&self, tenant: &str, dataset: &str, key: &str) -> Result<(), RsError> {
        self.inner
            .delete(tenant, &record_path(dataset, key))
            .await
            .map_err(|e| missing_record(e, dataset, key))
    }

    async fn list_keys(
        &self,
        tenant: &str,
        dataset: &str,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<String>, u64), RsError> {
        match self.inner.list(tenant, &records_dir(dataset), take, skip).await {
            Ok((entries, total)) => {
                let keys = entries
                    .into_iter()
                    .filter(|e| !e.dir)
                    .filter_map(|e| e.name.strip_suffix(".json").map(decode_key))
                    .collect();
                Ok((keys, total))
            }
            // No `records/` dir: an empty listing if the dataset otherwise
            // exists (e.g. schema-only), else a genuine missing dataset — the
            // same distinction `MemDataStore` draws.
            Err(e) if e.code == codes::NOT_FOUND => {
                if self.inner.list(tenant, &dataset_dir(dataset), 1, 0).await.is_ok() {
                    Ok((vec![], 0))
                } else {
                    Err(RsError::not_found(format!("no dataset '{dataset}'")))
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn list_datasets(
        &self,
        tenant: &str,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<String>, u64), RsError> {
        let (entries, total) = self.inner.list(tenant, "/", take, skip).await?;
        let names = entries
            .into_iter()
            .filter(|e| e.dir)
            .map(|e| e.name.trim_end_matches('/').to_string())
            .collect();
        Ok((names, total))
    }

    async fn get_schema(&self, tenant: &str, dataset: &str) -> Result<Option<Value>, RsError> {
        match self.inner.read(tenant, &schema_path(dataset), None).await {
            Ok(mut body) => {
                let bytes = body.materialize(RECORD_CAP).await?.clone();
                let schema = serde_json::from_slice(&bytes).map_err(|e| {
                    RsError::internal(format!("schema for dataset '{dataset}' is not valid JSON: {e}"))
                })?;
                Ok(Some(schema))
            }
            Err(e) if e.code == codes::NOT_FOUND => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn put_schema(&self, tenant: &str, dataset: &str, schema: Value) -> Result<(), RsError> {
        let bytes = serde_json::to_vec(&schema).map_err(|e| RsError::internal(e.to_string()))?;
        self.inner
            .write(tenant, &schema_path(dataset), Body::from_bytes(bytes, MediaType::json()))
            .await?;
        Ok(())
    }

    async fn delete_dataset(&self, tenant: &str, dataset: &str) -> Result<(), RsError> {
        self.inner.delete_dir_all(tenant, &dataset_dir(dataset)).await.map_err(|e| {
            if e.code == codes::NOT_FOUND {
                RsError::not_found(format!("no dataset '{dataset}'"))
            } else {
                e
            }
        })
    }
}

/// Map a record file's `not_found` to a record-shaped message; pass others
/// through.
fn missing_record(e: RsError, dataset: &str, key: &str) -> RsError {
    if e.code == codes::NOT_FOUND {
        RsError::not_found(format!("no record '{key}' in dataset '{dataset}'"))
    } else {
        e
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_through_encoding() {
        for key in ["plain", "user@example.com", "a/b/c", "weird key!", "100%", "ünïcode", ".."] {
            assert_eq!(decode_key(&encode_key(key)), key, "round-trip {key:?}");
        }
    }

    #[test]
    fn encoding_keeps_keys_to_one_safe_segment() {
        // `/` must be escaped so a key can't escape its dataset dir.
        assert!(!encode_key("a/b").contains('/'));
        // `..` + the always-appended `.json` is never a traversal segment.
        assert_eq!(format!("{}.json", encode_key("..")), "...json");
    }
}
