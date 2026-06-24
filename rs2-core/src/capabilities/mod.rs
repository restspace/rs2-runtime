//! Capability traits (PRD §9.2): services reach infrastructure only through
//! capability handles. Storage namespacing is **host-enforced**: handles are
//! constructed pre-scoped to a tenant, so an adapter never receives a path
//! outside the tenant prefix and a service can never choose its tenant.

use async_trait::async_trait;
use serde::Serialize;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::error::RsError;
use crate::message::{Body, Message, Provenance};

/// A parsed write precondition (the store equivalent of HTTP conditional
/// headers), supplied by a service after parsing `If-Match`/`If-None-Match`.
#[derive(Debug, Clone)]
pub enum WritePrecondition {
    /// No precondition — an unconditional upsert.
    None,
    /// `If-Match`: write only if the current ETag matches (comma list; `*`
    /// requires the resource to exist). Mismatch ⇒ `412`.
    IfMatch(String),
    /// `If-None-Match: *`: write only if the resource does **not** exist
    /// (atomic create-only where the adapter supports it). Exists ⇒ `412`.
    IfNoneMatchStar,
}

/// The result of a (conditional) write: whether it created the resource, and
/// the new content-version ETag (quoted) so the caller can return it without a
/// follow-up read.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub created: bool,
    pub etag: Option<String>,
}

/// Whether an `If-Match` header value (comma list, weak-aware; `*` matches any
/// existing resource) matches the resource's current quoted ETag.
pub(crate) fn if_match_hits(if_match: &str, etag: &str) -> bool {
    if_match
        .split(',')
        .map(str::trim)
        .any(|c| c == "*" || c.trim_start_matches("W/") == etag)
}

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
    /// Media type of a file entry, so a client can pick an icon/editor
    /// without a `HEAD` per item. `None` for directories.
    #[serde(rename = "contentType", skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
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

    /// The current content-version ETag (quoted) of a stored file, or `None`
    /// if it does not exist / the adapter has no validator. The default reads
    /// the resource's `Replayable` provenance — cheap, since `read` exposes
    /// provenance without consuming the body stream.
    async fn current_etag(&self, tenant: &str, path: &str) -> Result<Option<String>, RsError> {
        match self.read(tenant, path, None).await {
            Ok(body) => Ok(match body.provenance {
                Provenance::Replayable { version, .. } => Some(format!("\"{version}\"")),
                _ => None,
            }),
            Err(e) if e.status == 404 => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Conditional upsert honouring a [`WritePrecondition`]. The default is a
    /// **best-effort** check-then-write (`current_etag` → compare → `write`):
    /// a narrower race window than any client GET-then-PUT, but not atomic.
    /// Adapters whose backing store has compare-and-swap should override this
    /// and report [`FileStore::conditional_write_atomic`] `== true`.
    async fn write_cond(
        &self,
        tenant: &str,
        path: &str,
        body: Body,
        precondition: WritePrecondition,
    ) -> Result<WriteOutcome, RsError> {
        match &precondition {
            WritePrecondition::None => {}
            WritePrecondition::IfMatch(want) => match self.current_etag(tenant, path).await? {
                Some(cur) if if_match_hits(want, &cur) => {}
                Some(_) => {
                    return Err(RsError::precondition_failed(
                        "If-Match does not match the current ETag — re-read and retry",
                    ))
                }
                None => {
                    return Err(RsError::precondition_failed(
                        "If-Match given but the resource does not exist",
                    ))
                }
            },
            WritePrecondition::IfNoneMatchStar => {
                if self.current_etag(tenant, path).await?.is_some() {
                    return Err(RsError::precondition_failed(
                        "If-None-Match: * given but the resource already exists",
                    ));
                }
            }
        }
        let created = self.write(tenant, path, body).await?;
        let etag = self.current_etag(tenant, path).await?;
        Ok(WriteOutcome { created, etag })
    }

    /// Whether [`FileStore::write_cond`] is atomic (backed by a real
    /// compare-and-swap) rather than the best-effort default. Drives the
    /// `conditional-write` discovery facet's strength.
    fn conditional_write_atomic(&self) -> bool {
        false
    }

    async fn delete(&self, tenant: &str, path: &str) -> Result<(), RsError>;
    /// Move/rename a file. Returns `true` if the destination was created
    /// (vs. overwritten). Fails if the source is missing or a directory.
    async fn rename(&self, tenant: &str, from: &str, to: &str) -> Result<bool, RsError>;
    /// Delete a directory; fails unless empty.
    async fn delete_dir(&self, tenant: &str, path: &str) -> Result<(), RsError>;
    /// Delete a directory and all its contents (the `?confirm=` path).
    async fn delete_dir_all(&self, tenant: &str, path: &str) -> Result<(), RsError>;
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
    /// Paginated dataset enumeration; returns (names, total count).
    async fn list_datasets(&self, tenant: &str, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError>;
    async fn get_schema(&self, tenant: &str, dataset: &str) -> Result<Option<serde_json::Value>, RsError>;
    async fn put_schema(&self, tenant: &str, dataset: &str, schema: serde_json::Value) -> Result<(), RsError>;
    async fn delete_dataset(&self, tenant: &str, dataset: &str) -> Result<(), RsError>;
}

/// Outbound HTTP, granted with allowed-host patterns; default deny.
#[async_trait]
pub trait HttpOut: Send + Sync {
    async fn request(&self, msg: Message) -> Result<Message, RsError>;
}

/// Outbound SMS behind a swappable provider adapter (Twilio, AWS SNS, …) — the
/// reference *typed provider capability* (PRD §9.2): the canonical interface a
/// tenant's app speaks, while the adapter maps it to one provider's wire format
/// and auth. Like [`QueryStore`], it is one trait with many provider impls
/// (a Rust built-in or a loadable `code:` guest), selected by config — a new
/// provider needs no recompile. `tenant` is supplied by the host scoping
/// wrapper, never by service code.
#[async_trait]
pub trait SmsGateway: Send + Sync {
    /// Send a text message to `to`; returns the provider's message id.
    async fn send(&self, tenant: &str, to: &str, body: &str) -> Result<String, RsError>;
    /// Delivery status of a previously sent message (provider-shaped JSON).
    async fn status(&self, tenant: &str, id: &str) -> Result<serde_json::Value, RsError>;
}

/// A [`SmsGateway`] handle pre-scoped to one tenant — the only form services see.
#[derive(Clone)]
pub struct ScopedSmsGateway {
    inner: Arc<dyn SmsGateway>,
    tenant: String,
}

impl ScopedSmsGateway {
    pub fn new(inner: Arc<dyn SmsGateway>, tenant: &str) -> Self {
        ScopedSmsGateway { inner, tenant: tenant.to_string() }
    }

    pub async fn send(&self, to: &str, body: &str) -> Result<String, RsError> {
        self.inner.send(&self.tenant, to, body).await
    }

    pub async fn status(&self, id: &str) -> Result<serde_json::Value, RsError> {
        self.inner.status(&self.tenant, id).await
    }
}

/// Parameterized queries against a backing store (PRD §10.4) — the RS2
/// equivalent of v1's `IQueryAdapter`, language-agnostic by design.
///
/// JSON-language templates (Mongo aggregates, Elastic DSL, the reference
/// adapter) arrive **already substituted** structurally by the service;
/// `params` is supplementary. String-language templates (SQL) arrive
/// **unsubstituted** with their `${name}` placeholders intact: the adapter
/// rewrites them to its bind syntax and uses prepared statements — the
/// service never splices values into string templates.
#[async_trait]
pub trait QueryStore: Send + Sync {
    /// Execute a query; returns (rows, total count).
    async fn run_query(
        &self,
        tenant: &str,
        query: &serde_json::Value,
        params: &serde_json::Map<String, serde_json::Value>,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<serde_json::Value>, u64), RsError>;

    /// Quote a JSON value for safe splicing into an embedded string position
    /// of this adapter's query language (within JSON templates only).
    /// Unquotable values are structured 400s.
    fn quote(&self, value: &serde_json::Value) -> Result<String, RsError>;
}

/// A [`QueryStore`] handle pre-scoped to one tenant.
#[derive(Clone)]
pub struct ScopedQueryStore {
    inner: Arc<dyn QueryStore>,
    tenant: String,
}

impl ScopedQueryStore {
    pub fn new(inner: Arc<dyn QueryStore>, tenant: &str) -> Self {
        ScopedQueryStore { inner, tenant: tenant.to_string() }
    }

    pub async fn run_query(
        &self,
        query: &serde_json::Value,
        params: &serde_json::Map<String, serde_json::Value>,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<serde_json::Value>, u64), RsError> {
        self.inner.run_query(&self.tenant, query, params, take, skip).await
    }

    pub fn quote(&self, value: &serde_json::Value) -> Result<String, RsError> {
        self.inner.quote(value)
    }
}

/// A [`FileStore`] decorator that roots every call under a path prefix.
/// The seam for spec stores (and, later, per-store named infra): the
/// wrapped consumer stays oblivious to where its files actually live.
pub struct PrefixedFileStore {
    inner: Arc<dyn FileStore>,
    prefix: String,
}

impl PrefixedFileStore {
    pub fn new(inner: Arc<dyn FileStore>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        PrefixedFileStore { inner, prefix: prefix.trim_matches('/').to_string() }
    }

    fn join(&self, path: &str) -> String {
        let rel = path.trim_start_matches('/');
        if rel.is_empty() {
            format!("/{}", self.prefix)
        } else {
            format!("/{}/{rel}", self.prefix)
        }
    }
}

#[async_trait]
impl FileStore for PrefixedFileStore {
    async fn head(&self, tenant: &str, path: &str) -> Result<FileMeta, RsError> {
        self.inner.head(tenant, &self.join(path)).await
    }

    async fn read(&self, tenant: &str, path: &str, range: Option<ByteRange>) -> Result<Body, RsError> {
        self.inner.read(tenant, &self.join(path), range).await
    }

    async fn write(&self, tenant: &str, path: &str, body: Body) -> Result<bool, RsError> {
        self.inner.write(tenant, &self.join(path), body).await
    }

    async fn current_etag(&self, tenant: &str, path: &str) -> Result<Option<String>, RsError> {
        self.inner.current_etag(tenant, &self.join(path)).await
    }

    async fn write_cond(
        &self,
        tenant: &str,
        path: &str,
        body: Body,
        precondition: WritePrecondition,
    ) -> Result<WriteOutcome, RsError> {
        self.inner.write_cond(tenant, &self.join(path), body, precondition).await
    }

    fn conditional_write_atomic(&self) -> bool {
        self.inner.conditional_write_atomic()
    }

    async fn delete(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete(tenant, &self.join(path)).await
    }

    async fn rename(&self, tenant: &str, from: &str, to: &str) -> Result<bool, RsError> {
        self.inner.rename(tenant, &self.join(from), &self.join(to)).await
    }

    async fn delete_dir(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir(tenant, &self.join(path)).await
    }

    async fn delete_dir_all(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir_all(tenant, &self.join(path)).await
    }

    async fn list(&self, tenant: &str, path: &str, take: usize, skip: usize) -> Result<(Vec<DirEntry>, u64), RsError> {
        self.inner.list(tenant, &self.join(path), take, skip).await
    }
}

/// Read an optional, **safe** storage root from a mount's expanded `store`
/// config (the value a built-in registry factory receives). `Ok(None)` when no
/// (or an empty) `root` is set; `Ok(Some(trimmed))` for a present root; a config
/// error (a 400 at `PUT /raw` build time) for an unsafe one.
///
/// The root is sanitized against path-traversal and absolute escapes (`..`, a
/// leading `/`/`\`, or a drive letter) rather than silently normalized, so an
/// unsafe value is rejected outright. A built-in file/local factory uses this
/// to root its backing store under the mount's prefix when a root is given;
/// [`require_store_root`] is the variant that *demands* one.
pub fn sanitized_store_root(store: &serde_json::Value) -> Result<Option<String>, RsError> {
    let Some(root) = store
        .get("root")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    // Reject absolute paths and Windows drive letters (`C:\…`), which would
    // escape the tenant/node storage area.
    let bytes = root.as_bytes();
    if root.starts_with('/') || root.starts_with('\\') || (bytes.len() >= 2 && bytes[1] == b':') {
        return Err(RsError::bad_request(format!(
            "store root '{root}' must be a relative path (no leading '/'/'\\' or drive letter)"
        )));
    }
    // Reject `..` traversal in any path component (either separator).
    if root.split(['/', '\\']).any(|c| c == "..") {
        return Err(RsError::bad_request(format!(
            "store root '{root}' must not contain a '..' path segment"
        )));
    }
    Ok(Some(root.trim_matches('/').to_string()))
}

/// Resolve a **required, safe** storage root from an explicit `builtin:file` /
/// `builtin:local` *primary mount* store. Errors (a 400 at build time) when no
/// root is set.
///
/// An explicit node-aliasing built-in backend (`builtin:file` for `data`,
/// `builtin:local` for `file`) means the mount wants its own physical store, so
/// a missing `root` is a hard error: sharing the node default would silently
/// collide datasets/paths across mounts (an isolation/auth bypass). Mounts that
/// genuinely want the shared default simply omit the `store` block entirely.
/// (Spec stores reuse the same factory but apply their own root, so they go
/// through [`sanitized_store_root`], not this.)
pub fn require_store_root(store: &serde_json::Value) -> Result<String, RsError> {
    sanitized_store_root(store)?.ok_or_else(|| {
        RsError::bad_request(
            "an explicit 'builtin:file'/'builtin:local' store requires a non-empty 'root' (each \
             such mount gets its own physical store); omit the 'store' block to share the node \
             default instead",
        )
    })
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

    /// A view of this handle rooted under a path prefix (composing the
    /// [`PrefixedFileStore`] decorator inside the tenant scope).
    pub fn prefixed(&self, prefix: &str) -> ScopedFileStore {
        ScopedFileStore {
            inner: Arc::new(PrefixedFileStore::new(self.inner.clone(), prefix)),
            tenant: self.tenant.clone(),
        }
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

    pub async fn current_etag(&self, path: &str) -> Result<Option<String>, RsError> {
        self.inner.current_etag(&self.tenant, path).await
    }

    pub async fn write_cond(
        &self,
        path: &str,
        body: Body,
        precondition: WritePrecondition,
    ) -> Result<WriteOutcome, RsError> {
        self.inner.write_cond(&self.tenant, path, body, precondition).await
    }

    pub fn conditional_write_atomic(&self) -> bool {
        self.inner.conditional_write_atomic()
    }

    pub async fn delete(&self, path: &str) -> Result<(), RsError> {
        self.inner.delete(&self.tenant, path).await
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<bool, RsError> {
        self.inner.rename(&self.tenant, from, to).await
    }

    pub async fn delete_dir(&self, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir(&self.tenant, path).await
    }

    pub async fn delete_dir_all(&self, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir_all(&self.tenant, path).await
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

    pub async fn list_datasets(&self, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError> {
        self.inner.list_datasets(&self.tenant, take, skip).await
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
