//! Resident adapter runtimes (G13 Phase 2): a deployed JS module kept resident
//! so its connections pool across requests. Phase 1's socket capability opened
//! a fresh connection per invocation (one runtime per request); a *resident*
//! runtime evaluates the bundle once on its own OS thread and then services
//! many jobs against the same isolate — so a socket the adapter opens in job N
//! survives to job N+1, and the adapter pools it in a module-level JS var.
//!
//! The first consumer is [`GuestDataStore`]: a loadable `DataStore` adapter. A
//! data mount whose config says `"store": { "adapter": "code:my-redis@v1" }`
//! backs its persistence with the resident bundle instead of the built-in
//! store. The bundle speaks the **store pattern** over HTTP-shaped messages
//! (`GET /{ds}/{key}`, `PUT …`, …); `GuestDataStore` translates the `DataStore`
//! trait into those messages and the responses back into the trait's returns,
//! so the stock `DataService` runs unchanged on top — inheriting schema
//! validation, ETags, `.schemas`, the store contract.
//!
//! V8 isolates are `!Send`, so the runtime lives on a dedicated thread and
//! jobs cross to it as `Send` values over a channel. Host capability work
//! (socket I/O) still re-enters the *main* runtime via `block_on_main`, exactly
//! as for per-invocation isolates.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex, OnceCell};

use base64::Engine as _;

use crate::capabilities::{
    list_records_fallback, ByteRange, DataStore, DirEntry, FileMeta, FileStore, QueryStore,
    ScopedFileStore, SmsGateway,
};
use crate::contract::{GrantedHost, HostApi, InvocationLimits};
use crate::error::{codes, RsError};
use crate::message::{Body, MediaType, Provenance};

use super::js::{build_runtime, dispatch_once, socket_allowlist_from_config, InvocationState};

/// A unit of work for a resident runtime: dispatch `input` (a store-pattern
/// request envelope) against the resident bundle and reply with the response
/// envelope (or a structured error).
struct Job {
    input: Value,
    config: Value,
    reply: oneshot::Sender<Result<Value, RsError>>,
}

/// A cloneable handle to a resident runtime running on its own OS thread.
/// Dropping every clone closes the channel, which ends the runtime's loop and
/// drops the isolate — closing all pooled sockets.
#[derive(Clone)]
pub struct ResidentHandle {
    tx: mpsc::UnboundedSender<Job>,
}

impl ResidentHandle {
    /// Whether the runtime thread is still receiving jobs.
    fn alive(&self) -> bool {
        !self.tx.is_closed()
    }

    /// Dispatch one job and await its response envelope. Calls serialize on the
    /// runtime's single thread (a small per-mount pool is a later throughput
    /// step).
    async fn call(&self, input: Value, config: Value) -> Result<Value, RsError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Job {
                input,
                config,
                reply,
            })
            .map_err(|_| RsError::internal("resident adapter runtime is gone"))?;
        rx.await
            .map_err(|_| RsError::internal("resident adapter runtime dropped the job"))?
    }
}

/// Spawn a resident runtime for `source` on a dedicated thread, returning a
/// handle — plus the feature flags the bundle exported (`export const
/// features = […]`, absent → empty) — once the bundle has loaded and
/// evaluated (so a build error surfaces to the caller rather than to the
/// first job). The thread runs a current-thread tokio runtime driving one
/// `deno_core` isolate; `main` is the handle host ops re-enter for socket I/O.
async fn spawn_resident(
    source: String,
    host: Arc<dyn HostApi>,
    limits: InvocationLimits,
    tenant: String,
    socket_allowlist: Vec<String>,
) -> Result<(ResidentHandle, Vec<String>), RsError> {
    let main = tokio::runtime::Handle::current();
    let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<Vec<String>, RsError>>();

    std::thread::Builder::new()
        .name("rs2-resident".into())
        .spawn(move || {
            let local = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx.send(Err(RsError::internal(format!(
                        "resident runtime build failed: {e}"
                    ))));
                    return;
                }
            };
            local.block_on(async move {
                let inv = InvocationState::new(
                    host,
                    main,
                    tenant,
                    0,
                    // Resident loadable adapters are infrastructure (no request
                    // principal); their guest calls use granted capabilities,
                    // not ambient authority.
                    None,
                    limits.materialized_body_bytes,
                    socket_allowlist,
                );
                let oom = Arc::new(AtomicBool::new(false));
                let (mut runtime, default_export) =
                    match build_runtime(&source, inv.clone(), &limits, oom.clone()).await {
                        Ok((runtime, default_export, features)) => {
                            let _ = ready_tx.send(Ok(features));
                            (runtime, default_export)
                        }
                        Err(e) => {
                            let _ = ready_tx.send(Err(e));
                            return;
                        }
                    };
                // Service jobs until every handle is dropped; then the isolate
                // (and its pooled sockets) drops with `runtime` here.
                while let Some(job) = rx.recv().await {
                    let out = dispatch_once(
                        &mut runtime,
                        &default_export,
                        &job.input,
                        &job.config,
                        &inv,
                        &limits,
                        &oom,
                    )
                    .await;
                    let _ = job.reply.send(out);
                }
            });
        })
        .map_err(|e| RsError::internal(format!("failed to spawn resident thread: {e}")))?;

    ready_rx
        .await
        .map_err(|_| RsError::internal("resident runtime thread exited during startup"))?
        .map(|features| (ResidentHandle { tx }, features))
}

/// Loads a deployed adapter bundle's source from the tenant file store.
struct BundleLoader {
    files: ScopedFileStore,
    name: String,
    version: String,
    cap: u64,
}

impl BundleLoader {
    async fn load(&self) -> Result<String, RsError> {
        let path = crate::services::code::code_path_js(&self.name, &self.version);
        let mut body = self.files.read(&path, None).await.map_err(|_| {
            RsError::not_found(format!(
                "data adapter bundle 'code:{}@{}' not found — deploy it via PUT /code/{}",
                self.name, self.version, self.name
            ))
        })?;
        let bytes = body.materialize(self.cap).await?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| RsError::contract_violation("data adapter bundle is not valid UTF-8"))
    }
}

/// One resident runtime in a mount's pool, tracked for least-busy dispatch
/// (`inflight`) and idle eviction (`last_used`).
struct PoolEntry {
    handle: ResidentHandle,
    inflight: Arc<AtomicUsize>,
    last_used: Instant,
}

/// Decrements a runtime's in-flight count when a call finishes (or unwinds).
struct InflightGuard(Arc<AtomicUsize>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Shared machinery for guest-backed capability adapters: a small per-mount
/// **pool** of resident runtimes plus the request/response call helper. A
/// concrete adapter ([`GuestDataStore`], [`GuestQueryStore`]) wraps one of
/// these and maps its capability trait onto messages. The runtime is built from
/// a `"store"` config block (`{ "adapter": "code:<name>@<version>", "grants":
/// { … socket … }, … }`) and the whole block is handed to the bundle as
/// `ctx.config` (it reads connection params from there).
///
/// The pool grows lazily under concurrent load up to `maxRuntimes` (default 4)
/// — each runtime serializes its own jobs, so calls dispatch to the least-busy
/// one and a serial workload stays at a single runtime — and a background
/// sweeper evicts runtimes idle longer than `idleMs`/`idleSeconds` (default
/// 60 s; `0` disables), dropping the isolate and its pooled sockets. Lifecycle
/// is bounded by the owning adapter, which the tenant owns: a config change
/// rebuilds the tenant, dropping the pool (and every runtime) with it.
pub(crate) struct ResidentAdapter {
    pool: Arc<Mutex<Vec<PoolEntry>>>,
    max_runtimes: usize,
    idle: Duration,
    sweeper_started: AtomicBool,
    /// The adapter bundle source, read once and reused for every spawn.
    source: OnceCell<String>,
    /// Loads the bundle from the file store (deployed `code:` adapters).
    /// `None` when the source is pinned up front (inline templates).
    loader: Option<BundleLoader>,
    host: Arc<dyn HostApi>,
    limits: InvocationLimits,
    tenant: String,
    socket_allowlist: Vec<String>,
    store_config: Value,
    /// Feature flags the bundle exported (`export const features = […]`),
    /// captured at spawn. The spawn is lazy, so before the first runtime comes
    /// up this is empty and [`ResidentAdapter::has_feature`] reads `false` —
    /// callers treat that as "no pushdown yet", which is safe (the fallback
    /// path is always correct).
    features: std::sync::Mutex<Vec<String>>,
}

impl ResidentAdapter {
    /// Build a resident adapter from a `"store"` config block. `kind` names the
    /// capability for error messages (`"data"`, `"query"`); `files` is the
    /// tenant-scoped file store the bundle is read from.
    pub(crate) fn from_config(
        kind: &str,
        adapter_ref: &str,
        store_config: &Value,
        files: ScopedFileStore,
        tenant: &str,
        limits: InvocationLimits,
    ) -> Result<Self, RsError> {
        let invalid = || {
            RsError::bad_request(format!(
                "{kind} store adapter '{adapter_ref}' must be 'code:<name>@<version>'"
            ))
        };
        let (name, version) = adapter_ref
            .strip_prefix("code:")
            .ok_or_else(invalid)?
            .split_once('@')
            .ok_or_else(invalid)?;
        if name.is_empty() || version.is_empty() || name.contains(['/', '\\', '.']) {
            return Err(RsError::bad_request(format!(
                "invalid {kind} store adapter reference '{adapter_ref}'"
            )));
        }
        // The adapter only needs its socket grant (enforced host-side via the
        // allowlist); request/fetch default-deny unless a later kind is added.
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all(adapter_ref));
        let max_runtimes = store_config
            .get("maxRuntimes")
            .and_then(|v| v.as_u64())
            .unwrap_or(4)
            .clamp(1, 64) as usize;
        let idle_ms = store_config
            .get("idleMs")
            .and_then(|v| v.as_u64())
            .or_else(|| {
                store_config
                    .get("idleSeconds")
                    .and_then(|v| v.as_u64())
                    .map(|s| s.saturating_mul(1000))
            })
            .unwrap_or(60_000);
        Ok(ResidentAdapter {
            pool: Arc::new(Mutex::new(Vec::new())),
            max_runtimes,
            idle: Duration::from_millis(idle_ms),
            sweeper_started: AtomicBool::new(false),
            source: OnceCell::new(),
            loader: Some(BundleLoader {
                cap: limits.materialized_body_bytes,
                files,
                name: name.to_string(),
                version: version.to_string(),
            }),
            host,
            limits,
            tenant: tenant.to_string(),
            socket_allowlist: socket_allowlist_from_config(store_config),
            store_config: store_config.clone(),
            features: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Build a resident adapter from in-memory bundle source (a compiled
    /// template) rather than a deployed `code:` reference. The source is pinned
    /// up front, so no file load ever happens, and the bundle gets no grants —
    /// rendering is pure compute, so host capabilities default-deny.
    pub(crate) fn from_inline(source: String, tenant: &str, limits: InvocationLimits) -> Self {
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("template"));
        let cell = OnceCell::new();
        // Infallible: the cell is freshly created and never raced here.
        let _ = cell.set(source);
        ResidentAdapter {
            pool: Arc::new(Mutex::new(Vec::new())),
            max_runtimes: 4,
            idle: Duration::from_millis(60_000),
            sweeper_started: AtomicBool::new(false),
            source: cell,
            loader: None,
            host,
            limits,
            tenant: tenant.to_string(),
            socket_allowlist: Vec::new(),
            store_config: json!({}),
            features: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Whether the bundle advertised `name` in its `features` export. `false`
    /// until the first runtime has spawned (lazy spawn — see `features`).
    pub(crate) fn has_feature(&self, name: &str) -> bool {
        self.features.lock().unwrap().iter().any(|f| f == name)
    }

    /// The adapter bundle source, loaded once and cached for every spawn.
    async fn cached_source(&self) -> Result<String, RsError> {
        self.source
            .get_or_try_init(|| async {
                match &self.loader {
                    Some(l) => l.load().await,
                    None => Err(RsError::internal("resident adapter has no bundle source")),
                }
            })
            .await
            .map(|s| s.clone())
    }

    /// Reserve a runtime for one call: reuse an idle one, else grow the pool up
    /// to `max_runtimes`, else dispatch to the least-busy. The returned guard
    /// releases the reservation when the call finishes. Dead runtimes are
    /// pruned first (re-spawn on demand). The spawn awaits under the pool lock
    /// so concurrent first-calls don't over-spawn.
    async fn acquire(&self) -> Result<(ResidentHandle, InflightGuard), RsError> {
        let mut entries = self.pool.lock().await;
        entries.retain(|e| e.handle.alive());
        let least = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (i, e.inflight.load(Ordering::SeqCst)))
            .min_by_key(|(_, n)| *n);
        if let Some((i, n)) = least {
            if n == 0 || entries.len() >= self.max_runtimes {
                let e = &mut entries[i];
                e.last_used = Instant::now();
                e.inflight.fetch_add(1, Ordering::SeqCst);
                return Ok((e.handle.clone(), InflightGuard(e.inflight.clone())));
            }
        }
        let source = self.cached_source().await?;
        let (handle, features) = spawn_resident(
            source,
            self.host.clone(),
            self.limits.clone(),
            self.tenant.clone(),
            self.socket_allowlist.clone(),
        )
        .await?;
        *self.features.lock().unwrap() = features;
        let inflight = Arc::new(AtomicUsize::new(1));
        entries.push(PoolEntry {
            handle: handle.clone(),
            inflight: inflight.clone(),
            last_used: Instant::now(),
        });
        Ok((handle, InflightGuard(inflight)))
    }

    /// Start the idle-eviction sweeper once, on first use. It holds a `Weak` to
    /// the pool, so it stops itself when the adapter (and pool) drop.
    fn ensure_sweeper(&self) {
        if self.idle.is_zero() || self.sweeper_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let pool = Arc::downgrade(&self.pool);
        let idle = self.idle;
        tokio::spawn(async move {
            let tick = idle.max(Duration::from_millis(50));
            loop {
                tokio::time::sleep(tick).await;
                let Some(pool) = pool.upgrade() else {
                    return; // adapter dropped → nothing left to sweep
                };
                let now = Instant::now();
                let mut entries = pool.lock().await;
                // Keep a runtime only while it is live and either busy or
                // recently used; an evicted handle's thread exits and its
                // pooled sockets close.
                entries.retain(|e| {
                    e.handle.alive()
                        && (e.inflight.load(Ordering::SeqCst) > 0
                            || now.duration_since(e.last_used) < idle)
                });
            }
        });
    }

    /// Send a request envelope to the adapter; return `(status, body)`.
    pub(crate) async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), RsError> {
        self.ensure_sweeper();
        let (handle, _guard) = self.acquire().await?;
        let mut req = json!({ "method": method, "url": path });
        if let Some(b) = body {
            req["body"] = b;
            req["mediaType"] = json!("application/json");
        }
        let envelope = handle.call(req, self.store_config.clone()).await?;
        let status = envelope
            .get("status")
            .and_then(|s| s.as_u64())
            .unwrap_or(200) as u16;
        let body = envelope.get("body").cloned().unwrap_or(Value::Null);
        Ok((status, body))
    }
}

/// A [`DataStore`] backed by a resident JS adapter. The adapter implements the
/// store-pattern HTTP surface over a wire protocol (Redis, Mongo, …); this type
/// maps each trait method to a store-pattern request and the response back to
/// the trait's return. The `tenant` argument is ignored — the resident runtime
/// is already the tenant's, connected to the tenant's backend.
pub struct GuestDataStore {
    inner: ResidentAdapter,
}

impl GuestDataStore {
    /// Build a guest-backed data store from a `"store"` config block.
    pub fn from_config(
        adapter_ref: &str,
        store_config: &Value,
        files: ScopedFileStore,
        tenant: &str,
        limits: InvocationLimits,
    ) -> Result<Self, RsError> {
        Ok(GuestDataStore {
            inner: ResidentAdapter::from_config(
                "data",
                adapter_ref,
                store_config,
                files,
                tenant,
                limits,
            )?,
        })
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), RsError> {
        self.inner.call(method, path, body).await
    }
}

/// Map a non-2xx store-pattern response to an `RsError`, preserving the status
/// class and detail so the `DataService` and clients see the same identity a
/// built-in store would produce.
fn store_error(status: u16, body: &Value) -> RsError {
    let detail = body
        .get("detail")
        .and_then(|d| d.as_str())
        .or_else(|| body.get("message").and_then(|m| m.as_str()))
        .map(String::from)
        .unwrap_or_else(|| format!("data adapter returned {status}"));
    match status {
        400 => RsError::bad_request(detail),
        401 => RsError::unauthorized(detail),
        403 => RsError::forbidden(detail),
        404 => RsError::not_found(detail),
        409 => RsError::conflict(detail),
        413 => RsError::payload_too_large(detail),
        422 => {
            RsError::validation_failed(detail, body.get("errors").cloned().unwrap_or(Value::Null))
        }
        _ => RsError::contract_violation(detail),
    }
}

/// Child entries of a store listing whose `dir` flag matches `want_dir`,
/// returning each entry's `name` (the schema sentinel is dropped for keys).
fn listing_names(body: &Value, want_dir: bool) -> Vec<String> {
    body.get("entries")
        .and_then(|e| e.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter(|e| e.get("dir").and_then(|d| d.as_bool()).unwrap_or(false) == want_dir)
                .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
                .filter(|n| *n != ".schema.json")
                .map(|n| n.trim_end_matches('/').to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn listing_total(body: &Value) -> u64 {
    body.get("total").and_then(|t| t.as_u64()).unwrap_or(0)
}

/// Percent-encode a `$select`/`$sort` value for the store-shaped wire form, so
/// a field path can't smuggle separators (`,`, `&`, `=`) into the query. The
/// guest decodes with `URLSearchParams`, which reverses this exactly.
fn query_encode(s: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    utf8_percent_encode(s, QUERY_VALUE).to_string()
}

#[async_trait]
impl DataStore for GuestDataStore {
    async fn get(&self, _tenant: &str, dataset: &str, key: &str) -> Result<Value, RsError> {
        let (status, body) = self.call("GET", &format!("/{dataset}/{key}"), None).await?;
        if (200..300).contains(&status) {
            Ok(body)
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn put(
        &self,
        _tenant: &str,
        dataset: &str,
        key: &str,
        value: Value,
    ) -> Result<bool, RsError> {
        let (status, body) = self
            .call("PUT", &format!("/{dataset}/{key}"), Some(value))
            .await?;
        match status {
            201 => Ok(true),
            200 => Ok(false),
            _ => Err(store_error(status, &body)),
        }
    }

    async fn delete(&self, _tenant: &str, dataset: &str, key: &str) -> Result<(), RsError> {
        let (status, body) = self
            .call("DELETE", &format!("/{dataset}/{key}"), None)
            .await?;
        if matches!(status, 200 | 204) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn list_keys(
        &self,
        _tenant: &str,
        dataset: &str,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<String>, u64), RsError> {
        let (status, body) = self
            .call(
                "GET",
                &format!("/{dataset}/?$take={take}&$skip={skip}"),
                None,
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        Ok((listing_names(&body, false), listing_total(&body)))
    }

    async fn list_datasets(
        &self,
        _tenant: &str,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<String>, u64), RsError> {
        let (status, body) = self
            .call("GET", &format!("/?$take={take}&$skip={skip}"), None)
            .await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        Ok((listing_names(&body, true), listing_total(&body)))
    }

    async fn get_schema(&self, _tenant: &str, dataset: &str) -> Result<Option<Value>, RsError> {
        let (status, body) = self
            .call("GET", &format!("/{dataset}/.schema.json"), None)
            .await?;
        match status {
            s if (200..300).contains(&s) => Ok(Some(body)),
            404 => Ok(None),
            _ => Err(store_error(status, &body)),
        }
    }

    async fn put_schema(&self, _tenant: &str, dataset: &str, schema: Value) -> Result<(), RsError> {
        let (status, body) = self
            .call("PUT", &format!("/{dataset}/.schema.json"), Some(schema))
            .await?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn delete_dataset(&self, _tenant: &str, dataset: &str) -> Result<(), RsError> {
        let (status, body) = self
            .call("DELETE", &format!("/{dataset}/?confirm={dataset}"), None)
            .await?;
        if matches!(status, 200 | 204) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn list_records(
        &self,
        tenant: &str,
        dataset: &str,
        spec: &crate::listing::ListSpec,
    ) -> Result<(Vec<(String, Value)>, u64), RsError> {
        // Never forward `$select`/`$sort` to a bundle that hasn't advertised
        // `"list-records"` — it would misread them as a plain listing. The
        // key-walk fallback over the guest's own get/list_keys is always
        // correct (and is what the flag reads before the lazy first spawn).
        if !self.inner.has_feature("list-records") {
            return list_records_fallback(self, tenant, dataset, spec).await;
        }
        let select: Vec<String> = spec.fields.iter().map(|f| query_encode(&f.dotted())).collect();
        let mut path = format!("/{dataset}/?$select={}", select.join(","));
        if !spec.sort.is_empty() {
            let sort: Vec<String> = spec
                .sort
                .iter()
                .map(|(p, dir)| {
                    let prefix = match dir {
                        crate::listing::Dir::Desc => "-",
                        crate::listing::Dir::Asc => "",
                    };
                    format!("{prefix}{}", query_encode(&p.dotted()))
                })
                .collect();
            path.push_str(&format!("&$sort={}", sort.join(",")));
        }
        path.push_str(&format!("&$take={}&$skip={}", spec.take, spec.skip));
        let (status, body) = self.call("GET", &path, None).await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        let page = body
            .get("entries")
            .and_then(|e| e.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|e| {
                        (
                            e.get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("")
                                .to_string(),
                            e.get("fields").cloned().unwrap_or_else(|| json!({})),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok((page, listing_total(&body)))
    }

    fn listing_pushdown(&self) -> bool {
        // Reflects the bundle's `features` export; reads `false` until the
        // lazy first spawn has evaluated the module.
        self.inner.has_feature("list-records")
    }
}

/// An [`SmsGateway`] backed by a resident JS adapter — the reference proof that
/// the resident substrate generalizes beyond storage to a typed *provider*
/// capability. Maps `send`/`status` to store-pattern requests (`POST /send`,
/// `GET /status/{id}`); the adapter bundle maps those to the provider's wire
/// format + auth. The stock `sms` service runs unchanged over any provider.
pub struct GuestSmsGateway {
    inner: ResidentAdapter,
}

impl GuestSmsGateway {
    /// Build a guest-backed SMS gateway from a `"store"` config block.
    pub fn from_config(
        adapter_ref: &str,
        store_config: &Value,
        files: ScopedFileStore,
        tenant: &str,
        limits: InvocationLimits,
    ) -> Result<Self, RsError> {
        Ok(GuestSmsGateway {
            inner: ResidentAdapter::from_config(
                "sms",
                adapter_ref,
                store_config,
                files,
                tenant,
                limits,
            )?,
        })
    }
}

#[async_trait]
impl SmsGateway for GuestSmsGateway {
    async fn send(&self, _tenant: &str, to: &str, body: &str) -> Result<String, RsError> {
        let (status, resp) = self
            .inner
            .call("POST", "/send", Some(json!({ "to": to, "body": body })))
            .await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &resp));
        }
        resp.get("id")
            .and_then(|i| i.as_str())
            .map(String::from)
            .ok_or_else(|| RsError::contract_violation("sms adapter response missing string 'id'"))
    }

    async fn status(&self, _tenant: &str, id: &str) -> Result<Value, RsError> {
        let (status, resp) = self
            .inner
            .call("GET", &format!("/status/{id}"), None)
            .await?;
        if (200..300).contains(&status) {
            Ok(resp)
        } else {
            Err(store_error(status, &resp))
        }
    }
}

/// A [`QueryStore`] backed by a resident JS adapter. The query service
/// substitutes JSON templates / validates params first, then calls `run_query`;
/// this type ships `{query, params, take, skip}` to the adapter as
/// `POST /query` and reads `{rows, total}` back. The adapter executes the query
/// against its backend (e.g. push it down over a pooled socket).
pub struct GuestQueryStore {
    inner: ResidentAdapter,
}

impl GuestQueryStore {
    /// Build a guest-backed query store from a `"store"` config block.
    pub fn from_config(
        adapter_ref: &str,
        store_config: &Value,
        files: ScopedFileStore,
        tenant: &str,
        limits: InvocationLimits,
    ) -> Result<Self, RsError> {
        Ok(GuestQueryStore {
            inner: ResidentAdapter::from_config(
                "query",
                adapter_ref,
                store_config,
                files,
                tenant,
                limits,
            )?,
        })
    }
}

#[async_trait]
impl QueryStore for GuestQueryStore {
    async fn run_query(
        &self,
        _tenant: &str,
        query: &Value,
        params: &serde_json::Map<String, Value>,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<Value>, u64), RsError> {
        let body = json!({ "query": query, "params": params, "take": take, "skip": skip });
        let (status, resp) = self.inner.call("POST", "/query", Some(body)).await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &resp));
        }
        let rows = resp
            .get("rows")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        let total = resp
            .get("total")
            .and_then(|t| t.as_u64())
            .unwrap_or(rows.len() as u64);
        Ok((rows, total))
    }

    fn quote(&self, value: &Value) -> Result<String, RsError> {
        // `quote` is synchronous, so it can't round-trip to the isolate;
        // adapter-specific quoting (for embedded string positions in JSON
        // templates) uses the same scalar default as the reference adapter —
        // SQL adapters bind via params and never reach this path.
        scalar_quote(value)
    }
}

/// Default scalar quoting for embedded string positions of a JSON template
/// (matches the reference `MemQueryStore`): scalars stringify, composites are
/// rejected (a 400) rather than silently splicing structure into a string.
fn scalar_quote(value: &Value) -> Result<String, RsError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => Err(RsError::bad_request(format!(
            "cannot splice a {} into a string query position",
            match other {
                Value::Null => "null",
                Value::Array(_) => "array",
                _ => "object",
            }
        ))),
    }
}

/// A [`FileStore`] backed by a resident JS adapter. The adapter implements the
/// store pattern over its backend (`GET/PUT/DELETE /{path}`, `HEAD`, `MOVE`,
/// container listings `GET /{path}/`); file contents cross the message boundary
/// **base64-encoded** (the envelope is JSON), so the reference path materializes
/// — a streaming or presigned-redirect mode is a later optimization. The
/// `tenant` argument is ignored — the resident runtime is already the tenant's.
pub struct GuestFileStore {
    inner: ResidentAdapter,
}

impl GuestFileStore {
    /// Build a guest-backed file store from a `"store"` config block.
    pub fn from_config(
        adapter_ref: &str,
        store_config: &Value,
        files: ScopedFileStore,
        tenant: &str,
        limits: InvocationLimits,
    ) -> Result<Self, RsError> {
        Ok(GuestFileStore {
            inner: ResidentAdapter::from_config(
                "file",
                adapter_ref,
                store_config,
                files,
                tenant,
                limits,
            )?,
        })
    }
}

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// A stable content version (for the file service's ETags), hashed from bytes.
fn content_version(bytes: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    format!("{:016x}", h.finish())
}

#[async_trait]
impl FileStore for GuestFileStore {
    async fn head(&self, _tenant: &str, path: &str) -> Result<FileMeta, RsError> {
        let (status, body) = self.inner.call("HEAD", path, None).await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        Ok(FileMeta {
            size: body.get("size").and_then(|s| s.as_u64()).unwrap_or(0),
            last_modified: None,
            is_dir: body.get("isDir").and_then(|d| d.as_bool()).unwrap_or(false),
        })
    }

    async fn read(
        &self,
        _tenant: &str,
        path: &str,
        range: Option<ByteRange>,
    ) -> Result<Body, RsError> {
        let (status, body) = self.inner.call("GET", path, None).await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        let b64 = body
            .get("contentBase64")
            .and_then(|c| c.as_str())
            .unwrap_or("");
        let bytes = B64.decode(b64).map_err(|e| {
            RsError::contract_violation(format!("adapter returned invalid base64: {e}"))
        })?;
        let media_type = body
            .get("mediaType")
            .and_then(|m| m.as_str())
            .map(MediaType::parse)
            .unwrap_or_else(|| MediaType::for_path(path));
        let version = content_version(&bytes);
        // The whole-file version is the ETag; a Range serves the slice. (The
        // reference adapter returns the full body and we slice host-side; a
        // backend with native range support would push the range down.)
        let total = bytes.len() as u64;
        let slice = match range {
            None => bytes,
            Some(r) => {
                let start = r.start.min(total);
                let end = r.end.map(|e| (e + 1).min(total)).unwrap_or(total);
                if start >= end {
                    return Err(RsError::new(
                        416,
                        codes::BAD_REQUEST,
                        "Range Not Satisfiable",
                        format!("range start {start} beyond resource size {total}"),
                    ));
                }
                bytes[start as usize..end as usize].to_vec()
            }
        };
        let mut out = Body::from_bytes(slice, media_type);
        out.provenance = Provenance::Replayable {
            url: path.to_string(),
            version,
        };
        Ok(out)
    }

    async fn write(&self, _tenant: &str, path: &str, mut body: Body) -> Result<bool, RsError> {
        let media_type = body.media_type.to_string();
        let bytes = body
            .materialize(self.inner.limits.materialized_body_bytes)
            .await?;
        let payload = json!({ "contentBase64": B64.encode(bytes), "mediaType": media_type });
        let (status, resp) = self.inner.call("PUT", path, Some(payload)).await?;
        match status {
            201 => Ok(true),
            200 => Ok(false),
            _ => Err(store_error(status, &resp)),
        }
    }

    async fn delete(&self, _tenant: &str, path: &str) -> Result<(), RsError> {
        let (status, body) = self.inner.call("DELETE", path, None).await?;
        if matches!(status, 200 | 204) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn rename(&self, _tenant: &str, from: &str, to: &str) -> Result<bool, RsError> {
        let (status, body) = self
            .inner
            .call("MOVE", from, Some(json!({ "to": to })))
            .await?;
        match status {
            201 => Ok(true),
            200 => Ok(false),
            _ => Err(store_error(status, &body)),
        }
    }

    async fn delete_dir(&self, _tenant: &str, path: &str) -> Result<(), RsError> {
        let (status, body) = self.inner.call("DELETE", path, None).await?;
        if matches!(status, 200 | 204) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn delete_dir_all(&self, _tenant: &str, path: &str) -> Result<(), RsError> {
        let (status, body) = self
            .inner
            .call("DELETE", path, Some(json!({ "recursive": true })))
            .await?;
        if matches!(status, 200 | 204) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn list(
        &self,
        _tenant: &str,
        path: &str,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<DirEntry>, u64), RsError> {
        let sep = if path.contains('?') { '&' } else { '?' };
        let (status, body) = self
            .inner
            .call(
                "GET",
                &format!("{path}{sep}$take={take}&$skip={skip}"),
                None,
            )
            .await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        let total = body.get("total").and_then(|t| t.as_u64()).unwrap_or(0);
        let entries = body
            .get("entries")
            .and_then(|e| e.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|e| DirEntry {
                        name: e
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("")
                            .to_string(),
                        size: e.get("size").and_then(|s| s.as_u64()).unwrap_or(0),
                        last_modified: e
                            .get("lastModified")
                            .and_then(|l| l.as_str())
                            .map(String::from),
                        dir: e.get("dir").and_then(|d| d.as_bool()).unwrap_or(false),
                        content_type: e
                            .get("contentType")
                            .and_then(|c| c.as_str())
                            .map(String::from),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok((entries, total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The runtime is resident: module-level state survives between jobs (the
    /// property that lets an adapter pool a connection across requests).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resident_runtime_persists_module_state_across_jobs() {
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("resident-test"));
        let src = r#"
            let N = 0;
            export default async (msg, ctx) => { N += 1; return { status: 200, body: { n: N } }; };
        "#
        .to_string();
        let (h, features) = spawn_resident(src, host, InvocationLimits::default(), "t".into(), vec![])
            .await
            .unwrap();
        assert!(features.is_empty(), "no features export → empty");
        let a = h
            .call(json!({ "method": "GET", "url": "/" }), json!({}))
            .await
            .unwrap();
        let b = h
            .call(json!({ "method": "GET", "url": "/" }), json!({}))
            .await
            .unwrap();
        assert_eq!(a["body"]["n"], 1, "first job sees N=1");
        assert_eq!(b["body"]["n"], 2, "second job reuses the same isolate: N=2");
    }

    /// The optional `features` export is read at spawn — the handshake by
    /// which an adapter advertises native capabilities like `"list-records"`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resident_spawn_reads_the_features_export() {
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("resident-test"));
        let src = r#"
            export const features = ["list-records"];
            export default async (msg, ctx) => ({ status: 200 });
        "#
        .to_string();
        let (_h, features) =
            spawn_resident(src, host, InvocationLimits::default(), "t".into(), vec![])
                .await
                .unwrap();
        assert_eq!(features, ["list-records"]);
    }

    /// A bundle that fails to evaluate surfaces the error at spawn, not at the
    /// first job.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resident_build_error_surfaces_at_spawn() {
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("resident-test"));
        let result = spawn_resident(
            "this is not valid javascript ===".to_string(),
            host,
            InvocationLimits::default(),
            "t".into(),
            vec![],
        )
        .await;
        let err = result.err().expect("invalid bundle fails to spawn");
        assert_eq!(err.code, crate::error::codes::CONTRACT_VIOLATION);
    }
}
