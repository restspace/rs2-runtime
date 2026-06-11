//! Capability traits (PRD §9.2): services reach infrastructure only through
//! capability handles. Storage namespacing is **host-enforced**: handles are
//! constructed pre-scoped to a tenant, so an adapter never receives a path
//! outside the tenant prefix and a service can never choose its tenant.

use async_trait::async_trait;
use serde::Serialize;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::error::RsError;
use crate::message::{Body, Message};

/// Inclusive byte range for partial reads (`Range: bytes=start-end`).
#[derive(Debug, Clone, Copy)]
pub struct ByteRange {
    pub start: u64,
    /// Inclusive end; `None` means "to the end of the resource".
    pub end: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileMeta {
    pub size: u64,
    #[serde(skip)]
    pub last_modified: Option<OffsetDateTime>,
    pub is_dir: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirEntry {
    pub name: String,
    pub size: u64,
    #[serde(rename = "lastModified", skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    pub dir: bool,
}

/// Streamed file storage. `tenant` is supplied by the host-side scoping
/// wrapper, never by service code. Paths are tenant-relative, already
/// safety-validated by the router.
#[async_trait]
pub trait FileStore: Send + Sync {
    async fn head(&self, tenant: &str, path: &str) -> Result<FileMeta, RsError>;
    async fn read(&self, tenant: &str, path: &str, range: Option<ByteRange>) -> Result<Body, RsError>;
    /// Returns `true` if the resource was created (vs. overwritten).
    async fn write(&self, tenant: &str, path: &str, body: Body) -> Result<bool, RsError>;
    async fn delete(&self, tenant: &str, path: &str) -> Result<(), RsError>;
    /// Delete a directory; fails unless empty.
    async fn delete_dir(&self, tenant: &str, path: &str) -> Result<(), RsError>;
    /// Paginated listing; returns (entries, total count).
    async fn list(&self, tenant: &str, path: &str, take: usize, skip: usize) -> Result<(Vec<DirEntry>, u64), RsError>;
}

/// Schema-validated JSON storage keyed by dataset + key.
#[async_trait]
pub trait DataStore: Send + Sync {
    async fn get(&self, tenant: &str, dataset: &str, key: &str) -> Result<serde_json::Value, RsError>;
    /// Returns `true` if the record was created (vs. updated).
    async fn put(&self, tenant: &str, dataset: &str, key: &str, value: serde_json::Value) -> Result<bool, RsError>;
    async fn delete(&self, tenant: &str, dataset: &str, key: &str) -> Result<(), RsError>;
    /// Paginated key listing; returns (keys, total count).
    async fn list_keys(&self, tenant: &str, dataset: &str, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError>;
    async fn get_schema(&self, tenant: &str, dataset: &str) -> Result<Option<serde_json::Value>, RsError>;
    async fn put_schema(&self, tenant: &str, dataset: &str, schema: serde_json::Value) -> Result<(), RsError>;
    async fn delete_dataset(&self, tenant: &str, dataset: &str) -> Result<(), RsError>;
}

/// Outbound HTTP, granted with allowed-host patterns; default deny.
#[async_trait]
pub trait HttpOut: Send + Sync {
    async fn request(&self, msg: Message) -> Result<Message, RsError>;
}

/// A [`FileStore`] handle pre-scoped to one tenant — the only form services see.
#[derive(Clone)]
pub struct ScopedFileStore {
    inner: Arc<dyn FileStore>,
    tenant: String,
}

impl ScopedFileStore {
    pub fn new(inner: Arc<dyn FileStore>, tenant: &str) -> Self {
        ScopedFileStore { inner, tenant: tenant.to_string() }
    }

    pub async fn head(&self, path: &str) -> Result<FileMeta, RsError> {
        self.inner.head(&self.tenant, path).await
    }

    pub async fn read(&self, path: &str, range: Option<ByteRange>) -> Result<Body, RsError> {
        self.inner.read(&self.tenant, path, range).await
    }

    pub async fn write(&self, path: &str, body: Body) -> Result<bool, RsError> {
        self.inner.write(&self.tenant, path, body).await
    }

    pub async fn delete(&self, path: &str) -> Result<(), RsError> {
        self.inner.delete(&self.tenant, path).await
    }

    pub async fn delete_dir(&self, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir(&self.tenant, path).await
    }

    pub async fn list(&self, path: &str, take: usize, skip: usize) -> Result<(Vec<DirEntry>, u64), RsError> {
        self.inner.list(&self.tenant, path, take, skip).await
    }
}

/// A [`DataStore`] handle pre-scoped to one tenant.
#[derive(Clone)]
pub struct ScopedDataStore {
    inner: Arc<dyn DataStore>,
    tenant: String,
}

impl ScopedDataStore {
    pub fn new(inner: Arc<dyn DataStore>, tenant: &str) -> Self {
        ScopedDataStore { inner, tenant: tenant.to_string() }
    }

    pub async fn get(&self, dataset: &str, key: &str) -> Result<serde_json::Value, RsError> {
        self.inner.get(&self.tenant, dataset, key).await
    }

    pub async fn put(&self, dataset: &str, key: &str, value: serde_json::Value) -> Result<bool, RsError> {
        self.inner.put(&self.tenant, dataset, key, value).await
    }

    pub async fn delete(&self, dataset: &str, key: &str) -> Result<(), RsError> {
        self.inner.delete(&self.tenant, dataset, key).await
    }

    pub async fn list_keys(&self, dataset: &str, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError> {
        self.inner.list_keys(&self.tenant, dataset, take, skip).await
    }

    pub async fn get_schema(&self, dataset: &str) -> Result<Option<serde_json::Value>, RsError> {
        self.inner.get_schema(&self.tenant, dataset).await
    }

    pub async fn put_schema(&self, dataset: &str, schema: serde_json::Value) -> Result<(), RsError> {
        self.inner.put_schema(&self.tenant, dataset, schema).await
    }

    pub async fn delete_dataset(&self, dataset: &str) -> Result<(), RsError> {
        self.inner.delete_dataset(&self.tenant, dataset).await
    }
}
