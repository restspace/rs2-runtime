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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::capabilities::{DataStore, ScopedFileStore};
use crate::contract::{GrantedHost, HostApi, InvocationLimits};
use crate::error::RsError;

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
            .send(Job { input, config, reply })
            .map_err(|_| RsError::internal("resident adapter runtime is gone"))?;
        rx.await.map_err(|_| RsError::internal("resident adapter runtime dropped the job"))?
    }
}

/// Spawn a resident runtime for `source` on a dedicated thread, returning a
/// handle once the bundle has loaded and evaluated (so a build error surfaces
/// to the caller rather than to the first job). The thread runs a
/// current-thread tokio runtime driving one `deno_core` isolate; `main` is the
/// handle host ops re-enter for socket I/O.
async fn spawn_resident(
    source: String,
    host: Arc<dyn HostApi>,
    limits: InvocationLimits,
    tenant: String,
    socket_allowlist: Vec<String>,
) -> Result<ResidentHandle, RsError> {
    let main = tokio::runtime::Handle::current();
    let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), RsError>>();

    std::thread::Builder::new()
        .name("rs2-resident".into())
        .spawn(move || {
            let local = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready_tx
                        .send(Err(RsError::internal(format!("resident runtime build failed: {e}"))));
                    return;
                }
            };
            local.block_on(async move {
                let inv = InvocationState::new(
                    host,
                    main,
                    tenant,
                    0,
                    limits.materialized_body_bytes,
                    socket_allowlist,
                );
                let oom = Arc::new(AtomicBool::new(false));
                let (mut runtime, default_export) =
                    match build_runtime(&source, inv.clone(), &limits, oom.clone()).await {
                        Ok(built) => {
                            let _ = ready_tx.send(Ok(()));
                            built
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
        .map(|()| ResidentHandle { tx })
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

/// A [`DataStore`] backed by a resident JS adapter. The adapter implements the
/// store-pattern HTTP surface over a wire protocol (Redis, Mongo, …); this type
/// maps each trait method to a store-pattern request and the response back to
/// the trait's return. The `tenant` argument is ignored — the resident runtime
/// is already the tenant's, connected to the tenant's backend.
pub struct GuestDataStore {
    /// The resident runtime, spawned lazily on first use and re-spawned if it
    /// dies. A one-slot pool: lifecycle is tied to this `GuestDataStore`, which
    /// the tenant owns — a config change rebuilds the tenant, dropping the old
    /// store (and its runtime) and a new `code_ref` with it.
    handle: Mutex<Option<ResidentHandle>>,
    loader: BundleLoader,
    host: Arc<dyn HostApi>,
    limits: InvocationLimits,
    tenant: String,
    socket_allowlist: Vec<String>,
    /// The mount's `store` config, handed to the adapter as `ctx.config` (it
    /// reads connection params from here).
    store_config: Value,
}

impl GuestDataStore {
    /// Build a guest-backed data store from a `"store"` config block:
    /// `{ "adapter": "code:<name>@<version>", "grants": { … socket … }, … }`.
    /// `files` is the tenant-scoped file store the bundle is read from.
    pub fn from_config(
        adapter_ref: &str,
        store_config: &Value,
        files: ScopedFileStore,
        tenant: &str,
        limits: InvocationLimits,
    ) -> Result<Self, RsError> {
        let rest = adapter_ref.strip_prefix("code:").ok_or_else(|| {
            RsError::bad_request(format!(
                "data store adapter '{adapter_ref}' must be 'code:<name>@<version>'"
            ))
        })?;
        let (name, version) = rest.split_once('@').ok_or_else(|| {
            RsError::bad_request(format!(
                "data store adapter '{adapter_ref}' must be 'code:<name>@<version>'"
            ))
        })?;
        if name.is_empty() || version.is_empty() || name.contains(['/', '\\', '.']) {
            return Err(RsError::bad_request(format!(
                "invalid data store adapter reference '{adapter_ref}'"
            )));
        }
        // The adapter only needs its socket grant (enforced host-side via the
        // allowlist); request/fetch default-deny unless a later kind is added.
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all(adapter_ref));
        Ok(GuestDataStore {
            handle: Mutex::new(None),
            loader: BundleLoader {
                cap: limits.materialized_body_bytes,
                files,
                name: name.to_string(),
                version: version.to_string(),
            },
            host,
            limits,
            tenant: tenant.to_string(),
            socket_allowlist: socket_allowlist_from_config(store_config),
            store_config: store_config.clone(),
        })
    }

    /// Get a live resident handle, spawning (or re-spawning a dead one) on
    /// demand. Holds the slot lock across the spawn so first calls spawn once.
    async fn resident(&self) -> Result<ResidentHandle, RsError> {
        let mut slot = self.handle.lock().await;
        if let Some(h) = slot.as_ref() {
            if h.alive() {
                return Ok(h.clone());
            }
        }
        let source = self.loader.load().await?;
        let handle = spawn_resident(
            source,
            self.host.clone(),
            self.limits.clone(),
            self.tenant.clone(),
            self.socket_allowlist.clone(),
        )
        .await?;
        *slot = Some(handle.clone());
        Ok(handle)
    }

    /// Send a store-pattern request to the adapter; return `(status, body)`.
    async fn call(&self, method: &str, path: &str, body: Option<Value>) -> Result<(u16, Value), RsError> {
        let mut req = json!({ "method": method, "url": path });
        if let Some(b) = body {
            req["body"] = b;
            req["mediaType"] = json!("application/json");
        }
        let handle = self.resident().await?;
        let envelope = handle.call(req, self.store_config.clone()).await?;
        let status = envelope.get("status").and_then(|s| s.as_u64()).unwrap_or(200) as u16;
        let body = envelope.get("body").cloned().unwrap_or(Value::Null);
        Ok((status, body))
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
        422 => RsError::validation_failed(detail, body.get("errors").cloned().unwrap_or(Value::Null)),
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

    async fn put(&self, _tenant: &str, dataset: &str, key: &str, value: Value) -> Result<bool, RsError> {
        let (status, body) = self.call("PUT", &format!("/{dataset}/{key}"), Some(value)).await?;
        match status {
            201 => Ok(true),
            200 => Ok(false),
            _ => Err(store_error(status, &body)),
        }
    }

    async fn delete(&self, _tenant: &str, dataset: &str, key: &str) -> Result<(), RsError> {
        let (status, body) = self.call("DELETE", &format!("/{dataset}/{key}"), None).await?;
        if matches!(status, 200 | 204) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn list_keys(&self, _tenant: &str, dataset: &str, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError> {
        let (status, body) =
            self.call("GET", &format!("/{dataset}/?$take={take}&$skip={skip}"), None).await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        Ok((listing_names(&body, false), listing_total(&body)))
    }

    async fn list_datasets(&self, _tenant: &str, take: usize, skip: usize) -> Result<(Vec<String>, u64), RsError> {
        let (status, body) = self.call("GET", &format!("/?$take={take}&$skip={skip}"), None).await?;
        if !(200..300).contains(&status) {
            return Err(store_error(status, &body));
        }
        Ok((listing_names(&body, true), listing_total(&body)))
    }

    async fn get_schema(&self, _tenant: &str, dataset: &str) -> Result<Option<Value>, RsError> {
        let (status, body) = self.call("GET", &format!("/{dataset}/.schema.json"), None).await?;
        match status {
            s if (200..300).contains(&s) => Ok(Some(body)),
            404 => Ok(None),
            _ => Err(store_error(status, &body)),
        }
    }

    async fn put_schema(&self, _tenant: &str, dataset: &str, schema: Value) -> Result<(), RsError> {
        let (status, body) =
            self.call("PUT", &format!("/{dataset}/.schema.json"), Some(schema)).await?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
    }

    async fn delete_dataset(&self, _tenant: &str, dataset: &str) -> Result<(), RsError> {
        let (status, body) =
            self.call("DELETE", &format!("/{dataset}/?confirm={dataset}"), None).await?;
        if matches!(status, 200 | 204) {
            Ok(())
        } else {
            Err(store_error(status, &body))
        }
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
        let h = spawn_resident(src, host, InvocationLimits::default(), "t".into(), vec![])
            .await
            .unwrap();
        let a = h.call(json!({ "method": "GET", "url": "/" }), json!({})).await.unwrap();
        let b = h.call(json!({ "method": "GET", "url": "/" }), json!({})).await.unwrap();
        assert_eq!(a["body"]["n"], 1, "first job sees N=1");
        assert_eq!(b["body"]["n"], 2, "second job reuses the same isolate: N=2");
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
