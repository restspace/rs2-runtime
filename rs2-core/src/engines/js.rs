//! V8 isolate engine (PRD §5.3): runs JS service bundles in sandboxed
//! isolates under the engine-neutral contract, on `deno_core`.
//!
//! Shape: **one runtime per invocation**, driven on a dedicated blocking
//! thread by a current-thread tokio runtime. Host capability work re-enters
//! the *main* runtime via `Handle::spawn`, so re-entrant dispatch (prefix
//! grants) and outbound HTTP run where they normally do, while the isolate's
//! event loop stays on its own thread (V8 isolates are `!Send`).
//!
//! Bundle contract (unchanged): a single-file ES module whose default export
//! is either `async (msg, ctx) => response` or an object with such a `handle`
//! method. `msg` = `{ method, url, headers, body, mediaType }` (JSON bodies
//! arrive parsed); the response is `{ status?, headers?, body?, mediaType? }`,
//! or any other value to mean a 200 JSON body. `ctx` =
//! `{ config, request(capability, req), log(level, text),
//!    state: { get(key), put(key, value) } }`.
//!
//! Limits: wall clock via a watchdog thread calling `terminate_execution`;
//! memory via the near-heap-limit callback; the outbound budget and
//! materialization caps are host-enforced as for every engine.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use deno_core::{
    extension, op2, resolve_url, serde_v8, v8, JsRuntime, OpState, PollEventLoopOptions,
    RuntimeOptions,
};
use deno_error::JsErrorBox;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::contract::{Engine, HostApi, InvocationLimits, LogLevel, ServiceCode};
use crate::error::{codes, RsError};
use crate::message::{Body, ByteStream, MediaType, Message, Principal, Provenance, Source};
use bytes::Bytes;
use futures::StreamExt;

/// Bootstrap: capture the host ops into closures, install the native hooks
/// the compat prelude expects (`__rs2_native_*`, `__rs2_sleep`), and define
/// the dispatch entry point. Runs before the compat prelude and the bundle.
const BOOTSTRAP: &str = r#"
((core) => {
  const ops = core.ops;
  // A structured host error is returned as a marker; rethrow it as a JS
  // Error carrying `.code`/`.status` so the guest can branch on `e.code`.
  const rethrow = (r) => {
    if (r && typeof r === "object" && r.__rs2_error === true) {
      const e = new Error(r.message);
      e.code = r.code;
      e.status = r.status;
      throw e;
    }
    return r;
  };
  globalThis.__rs2_native_log = (l, t) => ops.op_rs2_log(String(l), String(t));
  globalThis.__rs2_native_random = (n) => ops.op_rs2_random(n);
  globalThis.__rs2_native_fetch = (r) => rethrow(ops.op_rs2_fetch(r ?? {}));
  // Gated raw socket for non-HTTP wire protocols (the `socket` grant).
  // Synchronous from the guest's view (host calls block); pooling across
  // requests is Phase 2's resident-runtime model.
  class RS2Socket {
    constructor(id) { this._id = id; }
    static connect(host, port, opts) {
      const r = rethrow(ops.op_rs2_sock_connect(String(host), port | 0, !!(opts && opts.tls)));
      return new RS2Socket(r.id);
    }
    write(data) {
      const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
      rethrow(ops.op_rs2_sock_write(this._id, bytes));
    }
    read(max) {
      const r = rethrow(ops.op_rs2_sock_read(this._id, (max | 0) || 65536));
      if (!r.data || r.data.length === 0) return null; // EOF
      return Uint8Array.from(r.data);
    }
    close() { ops.op_rs2_sock_close(this._id); }
  }
  globalThis.RS2Socket = RS2Socket;
  globalThis.__rs2_dispatch = async (userDefault, msg, config) => {
    const ctx = {
      config,
      request: (cap, req) => rethrow(ops.op_rs2_request(String(cap), req ?? {})),
      log: (level, text) => ops.op_rs2_log(String(level), String(text)),
      state: {
        get: (k) => ops.op_rs2_state_get(String(k)),
        put: (k, v) => ops.op_rs2_state_put(String(k), String(v)),
      },
      // Request streaming (`"requestStreaming": true`): pull the body
      // chunk-by-chunk instead of receiving it materialized in `msg.body`.
      // `readBody()` returns the next chunk as a Uint8Array, or null at EOF.
      readBody: () => {
        const r = rethrow(ops.op_rs2_body_read());
        if (r === null) return null; // EOF (the op is the sole EOF authority)
        return Uint8Array.from(r.data || []);
      },
      // Async-iterable sugar over `readBody`: `for await (const c of ctx.body())`.
      body: () => ({
        async *[Symbol.asyncIterator]() {
          for (;;) {
            const chunk = ctx.readBody();
            if (chunk === null) return;
            yield chunk;
          }
        },
      }),
      // Response streaming (`"responseStreaming": true`): declare the status +
      // headers, then push the body incrementally. The host returns the response
      // to the client as soon as `beginStream` is called; `write` applies
      // backpressure (it blocks when the consumer is behind). The stream ends
      // when the handler returns.
      beginStream: (envelope) => {
        rethrow(ops.op_rs2_stream_begin(envelope ?? {}));
        return {
          write: (data) => {
            const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
            rethrow(ops.op_rs2_body_write(bytes));
          },
          end: () => {}, // the stream closes when the handler returns
        };
      },
    };
    const handle = typeof userDefault === "function"
      ? userDefault
      : (userDefault && userDefault.handle);
    if (typeof handle !== "function") {
      throw new Error("default export must be a function or { handle }");
    }
    return await handle(msg, ctx);
  };
})(globalThis.Deno.core);
"#;

/// The compat prelude (web/Node API surface, PRD §11): captures the native
/// hooks the bootstrap installed and builds `console`/`fetch`/`TextEncoder`/
/// timers/etc. Proven against `tests/npm_compat.rs`.
const COMPAT_PRELUDE: &str = include_str!("js_prelude.js");

/// A V8 startup snapshot with [`BOOTSTRAP`] + [`COMPAT_PRELUDE`] already
/// evaluated, so a per-request isolate skips parsing/compiling ~500 lines of
/// prelude on every invocation. Generated out of band by the `gen-js-snapshot`
/// example (V8 can only init in snapshot *or* normal mode per process, so it
/// can't be built in the serving process) and committed alongside the source.
///
/// **Pure optimization with a fallback:** when the blob is empty (not yet
/// generated, or intentionally cleared), [`build_runtime`] runs the scripts the
/// old way, so behaviour is identical either way. `tests/prelude_snapshot.rs`
/// fails if the committed blob is stale relative to the prelude source.
static PRELUDE_SNAPSHOT: &[u8] = include_bytes!("js_prelude.snapshot.bin");

/// Build the prelude startup snapshot. Called only by the `gen-js-snapshot`
/// example — **never** from the serving process, which inits V8 in normal mode
/// (deno_core asserts a single V8 init mode per process). Returns the raw blob
/// to commit as `js_prelude.snapshot.bin`.
pub fn generate_prelude_snapshot() -> Vec<u8> {
    let mut runtime = deno_core::JsRuntimeForSnapshot::new(RuntimeOptions {
        extensions: vec![rs2_host::init()],
        module_loader: Some(Rc::new(deno_core::NoopModuleLoader)),
        ..Default::default()
    });
    runtime.execute_script("rs2:bootstrap", BOOTSTRAP).expect("bootstrap snapshot");
    runtime.execute_script("rs2:prelude", COMPAT_PRELUDE).expect("prelude snapshot");
    runtime.snapshot().to_vec()
}

/// Stable FNV-1a hash of the bootstrap + prelude source. The generator commits
/// this next to the blob; `tests/prelude_snapshot.rs` recomputes it and fails if
/// the source drifted without regenerating the snapshot.
#[doc(hidden)]
pub fn prelude_snapshot_source_hash() -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in BOOTSTRAP.bytes().chain(COMPAT_PRELUDE.bytes()) {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Per-invocation state, stored in the runtime's `OpState` and read by the
/// host-bridge ops. `host_error` preserves a structured host error's identity
/// across the JS boundary (an op records it before throwing).
///
/// A resident adapter runtime ([`crate::engines::resident`]) holds one of these
/// long-lived: `sockets` then pool across jobs, and `host_error` is cleared by
/// [`dispatch_once`] before each call.
pub(crate) struct InvocationState {
    host: Arc<dyn HostApi>,
    main: tokio::runtime::Handle,
    tenant: String,
    depth: u16,
    /// The invoking principal — a `code:` service runs as its caller, so guest
    /// `fetch`/`request` is bounded by the caller's access (`None` for resident
    /// loadable adapters, which are infrastructure with no request principal).
    principal: Option<Principal>,
    materialize_cap: u64,
    host_error: Mutex<Option<RsError>>,
    /// Open sockets for the `socket` capability. Per-invocation runtimes open
    /// and drop these within one request; a resident runtime keeps them across
    /// jobs, which is how an adapter pools connections (it caches the socket id
    /// in a module-level JS var).
    sockets: Mutex<HashMap<u32, Arc<tokio::sync::Mutex<SockStream>>>>,
    socket_seq: AtomicU32,
    /// Allowed `host:port` patterns, from the mount's `socket` grants.
    socket_allowlist: Vec<String>,
    /// The request body as a pull stream, present only under request streaming
    /// (`"requestStreaming": true`): the guest drains it chunk-by-chunk via
    /// `op_rs2_body_read` instead of receiving it materialized. `tokio::Mutex`
    /// so the read op can poll it from the main runtime (like `sockets`).
    request_body: Mutex<Option<Arc<tokio::sync::Mutex<ByteStream>>>>,
    /// Cumulative bytes pulled from `request_body`, bounded by `materialize_cap`
    /// (streaming bypasses the one-shot materialization cap, so it needs its own
    /// running bound).
    streamed_in: AtomicU64,
    /// The response sink, present only under response streaming
    /// (`"responseStreaming": true`): the guest emits the response incrementally
    /// via `ctx.beginStream()` + `write()`, which feed this sink. The host
    /// returns the response (headers + a `ByteStream`) the moment the guest
    /// begins, then keeps the isolate running to drain chunks under backpressure.
    response_sink: Mutex<Option<Arc<StreamSink>>>,
}

/// The destination for a streamed response body. `headers` carries the status +
/// headers envelope to the host exactly once (on `beginStream`); `chunks` is the
/// bounded byte channel feeding the response `ByteStream` (its capacity is the
/// backpressure). `sent`/`cap` bound the cumulative response size.
pub(crate) struct StreamSink {
    headers: Mutex<Option<tokio::sync::oneshot::Sender<Value>>>,
    chunks: tokio::sync::mpsc::Sender<Bytes>,
    sent: AtomicU64,
    cap: u64,
    began: AtomicBool,
}

impl InvocationState {
    /// Construct shared invocation state. `socket_allowlist` is the mount's
    /// `socket` grant patterns; `depth` seeds re-entrant dispatch.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        host: Arc<dyn HostApi>,
        main: tokio::runtime::Handle,
        tenant: String,
        depth: u16,
        principal: Option<Principal>,
        materialize_cap: u64,
        socket_allowlist: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(InvocationState {
            host,
            main,
            tenant,
            depth,
            principal,
            materialize_cap,
            host_error: Mutex::new(None),
            sockets: Mutex::new(HashMap::new()),
            socket_seq: AtomicU32::new(1),
            socket_allowlist,
            request_body: Mutex::new(None),
            streamed_in: AtomicU64::new(0),
            response_sink: Mutex::new(None),
        })
    }

    /// Install the request body as a pull stream for `op_rs2_body_read`. Called
    /// before dispatch when the mount opts into request streaming.
    pub(crate) fn set_request_body(&self, stream: ByteStream) {
        *self.request_body.lock().unwrap() = Some(Arc::new(tokio::sync::Mutex::new(stream)));
    }

    /// Install the response sink for `op_rs2_stream_begin`/`op_rs2_body_write`.
    /// Called before dispatch when the mount opts into response streaming.
    pub(crate) fn set_response_sink(&self, sink: Arc<StreamSink>) {
        *self.response_sink.lock().unwrap() = Some(sink);
    }

    fn socket_allowed(&self, host: &str, port: u16) -> bool {
        let target = format!("{host}:{port}");
        self.socket_allowlist.iter().any(|pat| socket_pattern_matches(pat, host, port, &target))
    }
}

/// A raw socket the guest speaks a wire protocol over: plain TCP or TLS.
enum SockStream {
    Plain(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>),
}

impl SockStream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            SockStream::Plain(s) => s.write_all(buf).await,
            SockStream::Tls(s) => s.write_all(buf).await,
        }
    }
    async fn read_up_to(&mut self, max: usize) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; max];
        let n = match self {
            SockStream::Plain(s) => s.read(&mut buf).await?,
            SockStream::Tls(s) => s.read(&mut buf).await?,
        };
        buf.truncate(n);
        Ok(buf)
    }
}

/// The TLS client connector (webpki roots, ring provider). Tests may override
/// it via [`set_tls_connector_for_test`] to trust a self-signed cert.
fn tls_connector() -> tokio_rustls::TlsConnector {
    static CONNECTOR: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
            tokio_rustls::TlsConnector::from(Arc::new(config))
        })
        .clone()
}

#[cfg(test)]
static TEST_CONNECTOR: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();

/// Install a TLS connector for tests (e.g. trusting a self-signed cert).
#[cfg(test)]
pub fn set_tls_connector_for_test(connector: tokio_rustls::TlsConnector) {
    let _ = TEST_CONNECTOR.set(connector);
}

fn active_connector() -> tokio_rustls::TlsConnector {
    #[cfg(test)]
    if let Some(c) = TEST_CONNECTOR.get() {
        return c.clone();
    }
    tls_connector()
}

/// Open a socket to `host:port`, optionally upgrading to TLS.
async fn connect_stream(host: &str, port: u16, tls: bool) -> Result<SockStream, RsError> {
    let tcp = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| RsError::new(502, codes::CONTRACT_VIOLATION, "Bad Gateway", format!("connect {host}:{port} failed: {e}")))?;
    if !tls {
        return Ok(SockStream::Plain(tcp));
    }
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|_| RsError::bad_request(format!("invalid TLS server name '{host}'")))?;
    let stream = active_connector()
        .connect(server_name, tcp)
        .await
        .map_err(|e| RsError::new(502, codes::CONTRACT_VIOLATION, "Bad Gateway", format!("TLS handshake to {host}:{port} failed: {e}")))?;
    Ok(SockStream::Tls(Box::new(stream)))
}

/// Match a `host:port` grant pattern: exact, host-only (any port), or a
/// `*.suffix[:port]` host wildcard (matching the apex too).
fn socket_pattern_matches(pat: &str, host: &str, port: u16, target: &str) -> bool {
    if pat == target {
        return true;
    }
    let (pat_host, pat_port) = match pat.rsplit_once(':') {
        Some((h, p)) => match p.parse::<u16>() {
            Ok(pp) => (h, Some(pp)),
            Err(_) => (pat, None),
        },
        None => (pat, None),
    };
    if let Some(pp) = pat_port {
        if pp != port {
            return false;
        }
    }
    if pat_host == "*" || pat_host == host {
        return true;
    }
    if let Some(suffix) = pat_host.strip_prefix("*.") {
        return host == suffix || host.ends_with(&format!(".{suffix}"));
    }
    false
}

/// Collect the `socket` grant allowlist (`host:port` patterns) from a mount
/// config's `grants` (or a `store` block's `grants`, for adapters).
pub(crate) fn socket_allowlist_from_config(config: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(grants) = config.get("grants").and_then(|g| g.as_object()) {
        for grant in grants.values() {
            if grant.get("type").and_then(|t| t.as_str()) == Some("socket") {
                if let Some(hosts) = grant.get("hosts").and_then(|h| h.as_array()) {
                    out.extend(hosts.iter().filter_map(|h| h.as_str().map(String::from)));
                }
            }
        }
    }
    out
}

/// A structured host error rendered as the marker the bootstrap rethrows.
fn error_marker(e: &RsError) -> Value {
    json!({ "__rs2_error": true, "code": e.code, "status": e.status, "message": e.detail })
}

/// Record a structured socket error (so an uncaught one keeps its identity out
/// of the engine, invariant 2) and return the marker the bootstrap rethrows.
fn fail_socket(inv: &InvocationState, e: RsError) -> Value {
    let marker = error_marker(&e);
    *inv.host_error.lock().unwrap() = Some(e);
    marker
}

/// Build an internal `Message` from a guest request envelope
/// `{ method?, url, headers?, body?, mediaType? }`. The guest call carries the
/// invoking principal (a `code:` service runs **as its caller**), so its
/// `fetch`/`request` is authorized exactly as the caller would be — never with
/// ambient authority.
fn message_from_request(
    req: &Value,
    tenant: &str,
    depth: u16,
    principal: Option<Principal>,
) -> Message {
    let method = req
        .get("method")
        .and_then(|m| m.as_str())
        .and_then(|m| http::Method::from_bytes(m.as_bytes()).ok())
        .unwrap_or(http::Method::GET);
    let url = req.get("url").and_then(|u| u.as_str()).unwrap_or("/");
    let mut call = Message::request(method, url, tenant);
    call.source = Source::Internal;
    call.principal = principal;
    call.depth = depth.saturating_add(1);
    if let Some(headers) = req.get("headers").and_then(|h| h.as_object()) {
        for (k, v) in headers {
            if let (Some(v), Ok(name)) =
                (v.as_str(), http::header::HeaderName::try_from(k.as_str()))
            {
                if let Ok(value) = http::HeaderValue::from_str(v) {
                    call.headers.insert(name, value);
                }
            }
        }
    }
    match req.get("body") {
        None | Some(Value::Null) => {}
        Some(Value::String(text)) => {
            let mt = req
                .get("mediaType")
                .and_then(|m| m.as_str())
                .map(MediaType::parse)
                .unwrap_or_else(|| MediaType::new("text/plain"));
            call.body = Some(Body::from_string(text.clone(), mt));
        }
        Some(other) => call.body = Some(Body::from_json(other)),
    }
    call
}

/// Run a host request to completion on the main runtime, materializing the
/// response body, and return the guest-facing envelope.
async fn run_host_request(
    host: Arc<dyn HostApi>,
    capability: String,
    call: Message,
    materialize_cap: u64,
) -> Result<Value, RsError> {
    let mut resp = host.request(&capability, call).await?;
    let status = resp.status.map(|s| s.as_u16()).unwrap_or(200);
    let (media_type, payload) = match &mut resp.body {
        None => (Value::Null, Value::Null),
        Some(body) => {
            let mt = body.media_type.to_string();
            let is_json = body.media_type.is_json();
            let bytes = body.materialize(materialize_cap).await?;
            let text = String::from_utf8_lossy(bytes).into_owned();
            let payload = if is_json {
                serde_json::from_str(&text).unwrap_or(Value::String(text))
            } else {
                Value::String(text)
            };
            (json!(mt), payload)
        }
    };
    let headers: serde_json::Map<String, Value> = resp
        .headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), json!(s))))
        .collect();
    Ok(json!({ "status": status, "headers": headers, "body": payload, "mediaType": media_type }))
}

/// `fetch` (compat layer): an absolute-URL request through the `fetch`
/// capability grant, returning `{status, headers, body}` with the body as
/// text (the compat `Response` parses it). Default-deny via the grant.
async fn run_host_fetch(
    host: Arc<dyn HostApi>,
    call: Message,
    materialize_cap: u64,
) -> Result<Value, RsError> {
    let mut resp = host.request("fetch", call).await?;
    let status = resp.status.map(|s| s.as_u16()).unwrap_or(200);
    let text = match &mut resp.body {
        None => String::new(),
        Some(body) => {
            let bytes = body.materialize(materialize_cap).await?;
            String::from_utf8_lossy(bytes).into_owned()
        }
    };
    let mut headers: serde_json::Map<String, Value> = resp
        .headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), json!(s))))
        .collect();
    // SDKs gate JSON parsing on content-type: surface the body's media type
    // when the response didn't carry one.
    if !headers.contains_key("content-type") {
        if let Some(body) = &resp.body {
            headers.insert("content-type".into(), json!(body.media_type.to_string()));
        }
    }
    Ok(json!({ "status": status, "headers": headers, "body": text }))
}

fn level_from_str(s: &str) -> LogLevel {
    match s {
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => LogLevel::Info,
    }
}

// ---- host-bridge ops ----------------------------------------------------

/// Run a future to completion on the main runtime, blocking the calling
/// (isolate) thread until it finishes. Host calls are **synchronous from the
/// guest's point of view** (the contract): the isolate parks here while the
/// host work runs on the main runtime. Uses a channel rather than
/// `Handle::block_on` because the isolate thread is already inside a tokio
/// runtime (driving the deno event loop), where `block_on` would panic.
fn block_on_main<T: Send + 'static>(
    main: &tokio::runtime::Handle,
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> Result<T, JsErrorBox> {
    let (tx, rx) = std::sync::mpsc::channel();
    main.spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.recv().map_err(|_| JsErrorBox::generic("host task dropped"))
}

#[op2]
#[serde]
fn op_rs2_request(
    state: &mut OpState,
    #[string] capability: String,
    #[serde] req: serde_json::Value,
) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let call = message_from_request(&req, &inv.tenant, inv.depth, inv.principal.clone());
    let host = inv.host.clone();
    let cap = inv.materialize_cap;
    // A structured host error is returned as a marker the prelude rethrows
    // as a JS `Error` carrying `.code`/`.status` (so the guest can branch on
    // `e.code`), and recorded so an *uncaught* one keeps its identity out of
    // the engine (invariant 2).
    let err = match block_on_main(&inv.main, run_host_request(host, capability, call, cap)) {
        Ok(Ok(envelope)) => return envelope,
        Ok(Err(e)) => e,
        Err(_) => RsError::internal("host task dropped"),
    };
    let marker = json!({
        "__rs2_error": true, "code": err.code, "status": err.status, "message": err.detail,
    });
    *inv.host_error.lock().unwrap() = Some(err);
    marker
}

#[op2(fast)]
fn op_rs2_log(state: &mut OpState, #[string] level: &str, #[string] text: &str) {
    let inv = state.borrow::<Arc<InvocationState>>();
    inv.host.log(level_from_str(level), text);
}

#[op2]
#[string]
fn op_rs2_state_get(state: &mut OpState, #[string] key: String) -> Option<String> {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let host = inv.host.clone();
    let bytes = block_on_main(&inv.main, async move { host.state_get(&key).await }).ok().flatten();
    bytes.map(|b| String::from_utf8_lossy(&b).into_owned())
}

#[op2(fast)]
fn op_rs2_state_put(state: &mut OpState, #[string] key: String, #[string] value: String) {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let host = inv.host.clone();
    let _ = block_on_main(&inv.main, async move { host.state_put(&key, value.into_bytes()).await });
}

/// `fetch` compat hook — like [`op_rs2_request`] but always the `fetch`
/// capability and a text body (returns the envelope or an error marker).
#[op2]
#[serde]
fn op_rs2_fetch(state: &mut OpState, #[serde] req: serde_json::Value) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let call = message_from_request(&req, &inv.tenant, inv.depth, inv.principal.clone());
    let host = inv.host.clone();
    let cap = inv.materialize_cap;
    let err = match block_on_main(&inv.main, run_host_fetch(host, call, cap)) {
        Ok(Ok(envelope)) => return envelope,
        Ok(Err(e)) => e,
        Err(_) => RsError::internal("host task dropped"),
    };
    let marker = json!({
        "__rs2_error": true, "code": err.code, "status": err.status, "message": err.detail,
    });
    *inv.host_error.lock().unwrap() = Some(err);
    marker
}

/// `crypto.getRandomValues`/`randomUUID` backing: `n` random bytes.
#[op2]
#[serde]
fn op_rs2_random(#[smi] n: u32) -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = vec![0u8; (n as usize).min(65536)];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Open a gated socket to `host:port` (TLS optional). Returns `{id}` or an
/// error marker; the connect is allowlisted by the mount's `socket` grants.
#[op2]
#[serde]
fn op_rs2_sock_connect(
    state: &mut OpState,
    #[string] host: String,
    #[smi] port: u32,
    tls: bool,
) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let port = port as u16;
    if !inv.socket_allowed(&host, port) {
        return fail_socket(&inv, RsError::capability_denied(&format!("socket {host}:{port}")));
    }
    let host2 = host.clone();
    let stream = match block_on_main(&inv.main, async move { connect_stream(&host2, port, tls).await }) {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return fail_socket(&inv, e),
        Err(_) => return fail_socket(&inv, RsError::internal("socket task dropped")),
    };
    let id = inv.socket_seq.fetch_add(1, Ordering::SeqCst);
    inv.sockets.lock().unwrap().insert(id, Arc::new(tokio::sync::Mutex::new(stream)));
    json!({ "id": id })
}

#[op2]
#[serde]
fn op_rs2_sock_write(state: &mut OpState, #[smi] id: u32, #[buffer] data: &[u8]) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let Some(sock) = inv.sockets.lock().unwrap().get(&id).cloned() else {
        return fail_socket(&inv, RsError::bad_request("write to unknown socket"));
    };
    let data = data.to_vec();
    match block_on_main(&inv.main, async move { sock.lock().await.write_all(&data).await }) {
        Ok(Ok(())) => Value::Null,
        Ok(Err(e)) => fail_socket(&inv, RsError::new(502, codes::CONTRACT_VIOLATION, "Bad Gateway", format!("socket write failed: {e}"))),
        Err(_) => fail_socket(&inv, RsError::internal("socket task dropped")),
    }
}

#[op2]
#[serde]
fn op_rs2_sock_read(state: &mut OpState, #[smi] id: u32, #[smi] max: u32) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let Some(sock) = inv.sockets.lock().unwrap().get(&id).cloned() else {
        return fail_socket(&inv, RsError::bad_request("read from unknown socket"));
    };
    let max = (max as usize).clamp(1, 1 << 20);
    match block_on_main(&inv.main, async move { sock.lock().await.read_up_to(max).await }) {
        Ok(Ok(bytes)) => json!({ "data": bytes }),
        Ok(Err(e)) => fail_socket(&inv, RsError::new(502, codes::CONTRACT_VIOLATION, "Bad Gateway", format!("socket read failed: {e}"))),
        Err(_) => fail_socket(&inv, RsError::internal("socket task dropped")),
    }
}

#[op2(fast)]
fn op_rs2_sock_close(state: &mut OpState, #[smi] id: u32) {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    inv.sockets.lock().unwrap().remove(&id);
}

/// Pull the next chunk of the request body, or `null` at end of stream. Present
/// only under request streaming (`"requestStreaming": true`); without it (or
/// once drained) it returns `null`. Chunks arrive as the source yields them;
/// the cumulative size is bounded by the materialization cap. Synchronous from
/// the guest's view, like the socket read op.
#[op2]
#[serde]
fn op_rs2_body_read(state: &mut OpState) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let Some(stream) = inv.request_body.lock().unwrap().clone() else {
        return Value::Null; // no streaming body (or never opted in)
    };
    let chunk = block_on_main(&inv.main, async move {
        let mut guard = stream.lock().await;
        guard.next().await
    });
    match chunk {
        Ok(Some(Ok(bytes))) => {
            let total = inv.streamed_in.fetch_add(bytes.len() as u64, Ordering::SeqCst)
                + bytes.len() as u64;
            if total > inv.materialize_cap {
                return fail_body(
                    &inv,
                    RsError::limit_exceeded("materialized_body_bytes", total, inv.materialize_cap),
                );
            }
            json!({ "data": bytes.to_vec() })
        }
        Ok(None) => Value::Null, // EOF
        Ok(Some(Err(e))) => fail_body(
            &inv,
            RsError::new(502, codes::CONTRACT_VIOLATION, "Bad Gateway", format!("request body stream error: {e}")),
        ),
        Err(_) => fail_body(&inv, RsError::internal("body read task dropped")),
    }
}

/// Record a structured request-body error (so an uncaught one keeps its identity
/// out of the engine, invariant 2) and return the marker the bootstrap rethrows.
fn fail_body(inv: &InvocationState, e: RsError) -> Value {
    let marker = error_marker(&e);
    *inv.host_error.lock().unwrap() = Some(e);
    marker
}

/// Begin a streamed response: hand the status/headers envelope to the host
/// (which builds the response and returns it while this isolate keeps running)
/// and arm the body writer. Present only under response streaming. Calling it
/// twice, or without the mount opting in, is a contract violation.
#[op2]
#[serde]
fn op_rs2_stream_begin(state: &mut OpState, #[serde] envelope: serde_json::Value) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let Some(sink) = inv.response_sink.lock().unwrap().clone() else {
        return fail_body(&inv, RsError::internal("response streaming is not enabled for this mount"));
    };
    let Some(tx) = sink.headers.lock().unwrap().take() else {
        return fail_body(&inv, RsError::contract_violation("beginStream called more than once"));
    };
    sink.began.store(true, Ordering::SeqCst);
    let _ = tx.send(envelope);
    Value::Null
}

/// Write one chunk of the streamed response body. Blocks (from the guest's view)
/// when the bounded channel is full — that backpressure is the consumer pacing
/// the producer. A gone consumer (client disconnect) surfaces as an error the
/// guest can catch. Bounded cumulatively by the materialization cap.
#[op2]
#[serde]
fn op_rs2_body_write(state: &mut OpState, #[buffer] data: &[u8]) -> serde_json::Value {
    let inv = state.borrow::<Arc<InvocationState>>().clone();
    let Some(sink) = inv.response_sink.lock().unwrap().clone() else {
        return fail_body(&inv, RsError::internal("response streaming is not enabled for this mount"));
    };
    if !sink.began.load(Ordering::SeqCst) {
        return fail_body(&inv, RsError::contract_violation("body write before beginStream"));
    }
    let total = sink.sent.fetch_add(data.len() as u64, Ordering::SeqCst) + data.len() as u64;
    if total > sink.cap {
        return fail_body(&inv, RsError::limit_exceeded("materialized_body_bytes", total, sink.cap));
    }
    let bytes = Bytes::copy_from_slice(data);
    let chunks = sink.chunks.clone();
    match block_on_main(&inv.main, async move { chunks.send(bytes).await }) {
        Ok(Ok(())) => Value::Null,
        Ok(Err(_)) => fail_body(
            &inv,
            RsError::new(502, codes::CONTRACT_VIOLATION, "Bad Gateway", "response stream consumer is gone"),
        ),
        Err(_) => fail_body(&inv, RsError::internal("stream write task dropped")),
    }
}

extension!(
    rs2_host,
    ops = [
        op_rs2_request,
        op_rs2_log,
        op_rs2_state_get,
        op_rs2_state_put,
        op_rs2_fetch,
        op_rs2_random,
        op_rs2_sock_connect,
        op_rs2_sock_write,
        op_rs2_sock_read,
        op_rs2_sock_close,
        op_rs2_body_read,
        op_rs2_stream_begin,
        op_rs2_body_write
    ]
);

// ---- the engine ---------------------------------------------------------

#[derive(Default)]
pub struct JsEngine;

impl JsEngine {
    pub fn new() -> Self {
        JsEngine
    }

    /// Compile-only validation: the deployment-time smoke test for
    /// `PUT /code/<name>` with a JS bundle (PRD §10.6). Loads and evaluates
    /// the bundle as a module with no invocation behind it.
    pub fn compile_check(&self, source: &str) -> Result<(), RsError> {
        let source = source.to_string();
        std::thread::spawn(move || -> Result<(), RsError> {
            let local = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| RsError::internal(format!("runtime build failed: {e}")))?;
            local.block_on(async move {
                let mut runtime = JsRuntime::new(RuntimeOptions {
                    extensions: vec![rs2_host::init()],
                    module_loader: Some(Rc::new(deno_core::NoopModuleLoader)),
                    ..Default::default()
                });
                let spec = resolve_url("rs2:service")
                    .map_err(|e| RsError::internal(format!("bad specifier: {e}")))?;
                let mod_id = runtime
                    .load_main_es_module_from_code(&spec, source)
                    .await
                    .map_err(|e| {
                        RsError::contract_violation(format!("JS bundle failed to compile: {e}"))
                    })?;
                let eval = runtime.mod_evaluate(mod_id);
                runtime
                    .run_event_loop(Default::default())
                    .await
                    .map_err(|e| RsError::contract_violation(format!("JS bundle errored: {e}")))?;
                eval.await
                    .map_err(|e| RsError::contract_violation(format!("JS bundle errored: {e}")))?;
                Ok(())
            })
        })
        .join()
        .map_err(|_| RsError::internal("compile-check thread panicked"))?
    }
}

#[async_trait]
impl Engine for JsEngine {
    async fn invoke(
        &self,
        code: &ServiceCode,
        mut msg: Message,
        config: &serde_json::Value,
        host: Arc<dyn HostApi>,
        limits: &InvocationLimits,
    ) -> Result<Message, RsError> {
        let source = match code {
            ServiceCode::JsBundle(s) => s.clone(),
            _ => return Err(RsError::engine_unavailable("js engine only runs js bundles")),
        };
        let template = msg.response(http::StatusCode::OK, None);

        // Body passthrough (`"bodyPassthrough": true` on the mount): the guest
        // sees the request body's *metadata* but not its bytes — the body is
        // carried past the isolate untouched and, unless the guest replaces it,
        // forwarded as the response body. A large upload/download proxied this
        // way never materializes (its `Payload::Stream` crosses by reference).
        let passthrough = config
            .get("bodyPassthrough")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // Request streaming (`"requestStreaming": true`): the guest pulls the
        // body chunk-by-chunk via `ctx.readBody()` rather than receiving it
        // materialized. Takes precedence over passthrough (the guest consumes
        // the body, so there is nothing left to forward).
        let request_streaming = config
            .get("requestStreaming")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // Response streaming (`"responseStreaming": true`): the guest emits the
        // body incrementally via `ctx.beginStream()` + `write()`; the host
        // returns the response the moment the guest begins and keeps the isolate
        // running (backpressured) until the handler finishes.
        let response_streaming = config
            .get("responseStreaming")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let mut carried_body: Option<Body> = None;
        let mut request_stream: Option<ByteStream> = None;

        // Bodies materialize at the engine boundary, capped (invariant 1) —
        // except under streaming/passthrough, where they cross by reference.
        let body_json = if request_streaming {
            match msg.body.take() {
                None => json!({ "payload": Value::Null, "mediaType": Value::Null }),
                Some(body) => {
                    let media_type = body.media_type.to_string();
                    let size = body.size;
                    // `into_stream` adapts a materialized body too, so the guest
                    // sees a uniform pull API regardless of the source.
                    request_stream = Some(body.into_stream());
                    json!({ "payload": Value::Null, "mediaType": media_type, "size": size })
                }
            }
        } else if passthrough {
            match msg.body.take() {
                None => json!({ "payload": Value::Null, "mediaType": Value::Null }),
                Some(body) => {
                    let media_type = body.media_type.to_string();
                    let size = body.size;
                    carried_body = Some(body);
                    json!({ "payload": Value::Null, "mediaType": media_type, "size": size })
                }
            }
        } else {
            match &mut msg.body {
                None => Value::Null,
                Some(body) => {
                    let media_type = body.media_type.to_string();
                    let is_json = body.media_type.is_json();
                    let bytes = body.materialize(limits.materialized_body_bytes).await?;
                    let text = String::from_utf8_lossy(bytes).into_owned();
                    let payload = if is_json {
                        serde_json::from_str(&text).unwrap_or(Value::String(text))
                    } else {
                        Value::String(text)
                    };
                    json!({ "payload": payload, "mediaType": media_type })
                }
            }
        };
        let headers: serde_json::Map<String, Value> = msg
            .headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), json!(s))))
            .collect();
        let url = if msg.url.query.is_empty() {
            msg.url.path.clone()
        } else {
            format!("{}?{}", msg.url.path, msg.url.query)
        };
        let input = json!({
            "method": msg.method.as_str(),
            "url": url,
            "headers": headers,
            "body": body_json.get("payload").cloned().unwrap_or(Value::Null),
            "mediaType": body_json.get("mediaType").cloned().unwrap_or(Value::Null),
            // Streaming surface: the guest can branch on these without ever
            // touching the bytes (`body` is null under either mode — pull it via
            // `ctx.readBody()` when `requestStreaming` holds).
            "bodyPassthrough": passthrough,
            "requestStreaming": request_streaming,
            "responseStreaming": response_streaming,
            "bodySize": body_json.get("size").cloned().unwrap_or(Value::Null),
        });

        let config = config.clone();
        let main = tokio::runtime::Handle::current();
        let tenant = msg.tenant.clone();
        let depth = msg.depth;
        // A `code:` service runs as its caller: the principal propagates into
        // the guest's own `fetch`/`request` calls.
        let principal = msg.principal.clone();
        let limits = limits.clone();

        // Response streaming: spawn the isolate, but return as soon as the guest
        // calls `beginStream` — the handler keeps running on its thread, feeding
        // the response `ByteStream` under backpressure. If the handler finishes
        // without ever streaming, fall back to its envelope.
        if response_streaming {
            return invoke_streaming(
                source, input, config, host, main, tenant, depth, principal, limits, template,
                request_stream, carried_body,
            )
            .await;
        }

        let outcome = tokio::task::spawn_blocking(move || {
            let inv = InvocationState::new(
                host,
                main,
                tenant,
                depth,
                principal,
                limits.materialized_body_bytes,
                socket_allowlist_from_config(&config),
            );
            // Hand the request body to the read op (request streaming only).
            if let Some(stream) = request_stream {
                inv.set_request_body(stream);
            }
            let local = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| RsError::internal(format!("runtime build failed: {e}")))?;
            local.block_on(run_invocation(&source, input, config, inv, &limits))
        })
        .await
        .map_err(|e| RsError::internal(format!("isolate thread failed: {e}")))??;

        Ok(envelope_to_message(&template, &outcome, carried_body))
    }
}

/// Map a guest response envelope (`{ status?, headers?, body?, mediaType? }`) to
/// a `Message`, using `template` for identity. `fallback_body` is used when the
/// envelope carries no `body` — the passthrough request body, or a streamed
/// response body.
fn envelope_to_message(template: &Message, outcome: &Value, fallback_body: Option<Body>) -> Message {
    let status = outcome
        .get("status")
        .and_then(|s| s.as_u64())
        .and_then(|s| http::StatusCode::from_u16(s as u16).ok())
        .unwrap_or(http::StatusCode::OK);
    let media_type = outcome.get("mediaType").and_then(|m| m.as_str()).map(MediaType::parse);
    let body = match outcome.get("body") {
        // No body in the envelope → use the fallback (a passthrough or streamed
        // body); otherwise an absent body simply means no body.
        None | Some(Value::Null) => fallback_body,
        Some(Value::String(text)) => Some(Body::from_string(
            text.clone(),
            media_type.clone().unwrap_or_else(|| MediaType::new("text/plain")),
        )),
        Some(other) => {
            let mut b = Body::from_json(other);
            if let Some(mt) = media_type.clone() {
                b.media_type = mt;
            }
            Some(b)
        }
    };
    let mut resp = template.response(status, body);
    if let Some(headers) = outcome.get("headers").and_then(|h| h.as_object()) {
        for (k, v) in headers {
            if let Some(v) = v.as_str() {
                if let (Ok(name), Ok(value)) = (
                    http::header::HeaderName::try_from(k.as_str()),
                    http::HeaderValue::from_str(v),
                ) {
                    resp.headers.insert(name, value);
                }
            }
        }
    }
    resp
}

/// Response-streaming dispatch. Spawns the isolate on a blocking thread and
/// returns the moment the guest calls `beginStream` — the handler keeps running
/// on that thread, feeding the bounded chunk channel under backpressure, and the
/// returned `ByteStream` drains it. If the handler finishes without ever
/// streaming, its envelope is mapped as usual. The wall-clock watchdog inside
/// `dispatch_once` bounds the whole handler run, so it doubles as the stream's
/// total time budget (backpressure waits count against it).
#[allow(clippy::too_many_arguments)]
async fn invoke_streaming(
    source: Arc<String>,
    input: Value,
    config: Value,
    host: Arc<dyn HostApi>,
    main: tokio::runtime::Handle,
    tenant: String,
    depth: u16,
    principal: Option<Principal>,
    limits: InvocationLimits,
    template: Message,
    request_stream: Option<ByteStream>,
    carried_body: Option<Body>,
) -> Result<Message, RsError> {
    // How many chunks the producer may run ahead of the consumer (backpressure).
    const CHANNEL_CAP: usize = 4;

    let (hdr_tx, hdr_rx) = tokio::sync::oneshot::channel::<Value>();
    let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel::<Bytes>(CHANNEL_CAP);
    let sink = Arc::new(StreamSink {
        headers: Mutex::new(Some(hdr_tx)),
        chunks: chunk_tx,
        sent: AtomicU64::new(0),
        cap: limits.materialized_body_bytes,
        began: AtomicBool::new(false),
    });

    let mut join = tokio::task::spawn_blocking(move || {
        let inv = InvocationState::new(
            host,
            main,
            tenant,
            depth,
            principal,
            limits.materialized_body_bytes,
            socket_allowlist_from_config(&config),
        );
        if let Some(stream) = request_stream {
            inv.set_request_body(stream);
        }
        // The isolate thread is now the sole owner of the chunk sender (via the
        // sink): when the handler ends and `inv` drops, the sender drops and the
        // response stream sees EOF.
        inv.set_response_sink(sink);
        let local = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| RsError::internal(format!("runtime build failed: {e}")))?;
        local.block_on(run_invocation(&source, input, config, inv, &limits))
    });

    let mut hdr_rx = hdr_rx;
    tokio::select! {
        biased;
        hdr = &mut hdr_rx => match hdr {
            Ok(envelope) => {
                // The guest began streaming; build the response over the channel
                // and leave the handler running (the join handle detaches on
                // return — a blocking task keeps running, feeding the stream).
                let media_type = envelope
                    .get("mediaType")
                    .and_then(|m| m.as_str())
                    .map(MediaType::parse)
                    .unwrap_or_else(MediaType::octet_stream);
                let stream = futures::stream::unfold(chunk_rx, |mut rx| async move {
                    rx.recv().await.map(|bytes| (Ok(bytes), rx))
                })
                .boxed();
                let body = Body::from_stream(stream, media_type, None, Provenance::Ephemeral);
                Ok(envelope_to_message(&template, &envelope, Some(body)))
            }
            Err(_) => {
                // The sender dropped without beginning a stream → the handler
                // ended; take its envelope (or error) from the join handle.
                let outcome = join
                    .await
                    .map_err(|e| RsError::internal(format!("isolate thread failed: {e}")))??;
                Ok(envelope_to_message(&template, &outcome, carried_body))
            }
        },
        res = &mut join => {
            // The handler finished before ever calling `beginStream`.
            let outcome =
                res.map_err(|e| RsError::internal(format!("isolate thread failed: {e}")))??;
            Ok(envelope_to_message(&template, &outcome, carried_body))
        }
    }
}

/// Build the runtime, run the bundle once, and return the response envelope.
/// Runs on a current-thread runtime on its own thread (per invocation).
async fn run_invocation(
    source: &str,
    input: Value,
    config: Value,
    inv: Arc<InvocationState>,
    limits: &InvocationLimits,
) -> Result<Value, RsError> {
    let oom = Arc::new(AtomicBool::new(false));
    let (mut runtime, default_export) = build_runtime(source, inv.clone(), limits, oom.clone()).await?;
    dispatch_once(&mut runtime, &default_export, &input, &config, &inv, limits, &oom).await
}

/// Wall-clock watchdog: terminate the isolate at `deadline`. Returns
/// `(timed_out, done)` — store `done` when the guarded work finishes so the
/// watcher exits without firing. Shared by the build and dispatch phases.
fn spawn_watchdog(
    handle: v8::IsolateHandle,
    wall: Duration,
) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
    let timed_out = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let (t2, d2) = (timed_out.clone(), done.clone());
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + wall;
        while std::time::Instant::now() < deadline {
            if d2.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if !d2.load(Ordering::SeqCst) {
            t2.store(true, Ordering::SeqCst);
            handle.terminate_execution();
        }
    });
    (timed_out, done)
}

/// Build a runtime from a bundle: install the host extension + heap cap, run
/// the bootstrap and compat prelude, load and evaluate the module, and capture
/// its default export. After this returns `__rs2_dispatch` is ready and the
/// module's top-level state is live. Shared by per-invocation [`run_invocation`]
/// and the resident adapter runtime ([`crate::engines::resident`]) — the latter
/// builds once and then [`dispatch_once`]es many jobs against the same isolate.
pub(crate) async fn build_runtime(
    source: &str,
    inv: Arc<InvocationState>,
    limits: &InvocationLimits,
    oom: Arc<AtomicBool>,
) -> Result<(JsRuntime, v8::Global<v8::Value>), RsError> {
    // When the committed snapshot is present, the bootstrap + prelude globals
    // are already baked into the isolate's heap; otherwise fall back to running
    // them from source (identical result, just slower per request).
    let snapshot = (!PRELUDE_SNAPSHOT.is_empty()).then_some(PRELUDE_SNAPSHOT);
    let create = v8::CreateParams::default().heap_limits(0, limits.memory_bytes as usize);
    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![rs2_host::init()],
        module_loader: Some(Rc::new(deno_core::NoopModuleLoader)),
        create_params: Some(create),
        startup_snapshot: snapshot,
        ..Default::default()
    });

    // Near-heap-limit: flag and terminate (don't let V8 abort the process).
    // Installed once; `dispatch_once` clears the flag before each call.
    {
        let oom = oom.clone();
        let handle = runtime.v8_isolate().thread_safe_handle();
        runtime.add_near_heap_limit_callback(move |current, _initial| {
            oom.store(true, Ordering::SeqCst);
            handle.terminate_execution();
            current * 2
        });
    }
    runtime.op_state().borrow_mut().put(inv.clone());

    // The bootstrap + compat prelude install the guest contract globals — only
    // needed when they aren't already resident from the snapshot.
    if snapshot.is_none() {
        runtime
            .execute_script("rs2:bootstrap", BOOTSTRAP)
            .map_err(|e| RsError::contract_violation(format!("bootstrap: {e}")))?;
        runtime
            .execute_script("rs2:prelude", COMPAT_PRELUDE)
            .map_err(|e| RsError::contract_violation(format!("prelude: {e}")))?;
    }

    // Load + evaluate the module under the wall clock (a bundle that loops at
    // top level can't hang the build), then map a kill to the right limit.
    let handle = runtime.v8_isolate().thread_safe_handle();
    let (timed_out, done) = spawn_watchdog(handle, limits.wall_clock);
    let evaluated = load_and_evaluate(&mut runtime, source).await;
    done.store(true, Ordering::SeqCst);
    match evaluated {
        Ok(default_export) => Ok((runtime, default_export)),
        Err(fail) => {
            if timed_out.load(Ordering::SeqCst) {
                runtime.v8_isolate().cancel_terminate_execution();
                let ms = limits.wall_clock.as_millis() as u64;
                return Err(RsError::limit_exceeded("wall_clock_ms", ms, ms));
            }
            if oom.load(Ordering::SeqCst) {
                runtime.v8_isolate().cancel_terminate_execution();
                return Err(RsError::limit_exceeded(
                    "memory_bytes",
                    limits.memory_bytes,
                    limits.memory_bytes,
                ));
            }
            if let Some(err) = inv.host_error.lock().unwrap().take() {
                return Err(err);
            }
            Err(RsError::contract_violation(format!("JS bundle failed: {fail}")))
        }
    }
}

/// Load + evaluate the ESM bundle and return its default export.
async fn load_and_evaluate(
    runtime: &mut JsRuntime,
    source: &str,
) -> Result<v8::Global<v8::Value>, String> {
    let spec = resolve_url("rs2:service").map_err(|e| format!("specifier: {e}"))?;
    let mod_id = runtime
        .load_main_es_module_from_code(&spec, source.to_string())
        .await
        .map_err(|e| format!("module load: {e}"))?;
    let eval = runtime.mod_evaluate(mod_id);
    runtime.run_event_loop(Default::default()).await.map_err(|e| format!("evaluate: {e}"))?;
    eval.await.map_err(|e| format!("evaluate: {e}"))?;

    let namespace = runtime.get_module_namespace(mod_id).map_err(|e| format!("namespace: {e}"))?;
    deno_core::scope!(scope, runtime);
    let ns = v8::Local::new(scope, namespace);
    let default_key = v8::String::new(scope, "default").ok_or("oom")?;
    let default_export = ns.get(scope, default_key.into()).ok_or("no default export")?;
    Ok(v8::Global::new(scope, default_export))
}

/// Call `__rs2_dispatch(default, msg, config)` once and return the response
/// envelope, guarded by the wall clock. Maps a timeout / OOM / host error to
/// the right `RsError`. Reusable across calls on a resident runtime: it clears
/// the per-call host-error slot and OOM flag, and cancels any pending
/// termination so the isolate survives a killed call.
pub(crate) async fn dispatch_once(
    runtime: &mut JsRuntime,
    default_export: &v8::Global<v8::Value>,
    input: &Value,
    config: &Value,
    inv: &InvocationState,
    limits: &InvocationLimits,
    oom: &Arc<AtomicBool>,
) -> Result<Value, RsError> {
    *inv.host_error.lock().unwrap() = None;
    oom.store(false, Ordering::SeqCst);

    let handle = runtime.v8_isolate().thread_safe_handle();
    let (timed_out, done) = spawn_watchdog(handle, limits.wall_clock);
    let result = drive_dispatch(runtime, default_export, input, config).await;
    done.store(true, Ordering::SeqCst);

    match result {
        Ok(value) => Ok(normalize_envelope(value)),
        Err(fail) => {
            if timed_out.load(Ordering::SeqCst) {
                runtime.v8_isolate().cancel_terminate_execution();
                let ms = limits.wall_clock.as_millis() as u64;
                return Err(RsError::limit_exceeded("wall_clock_ms", ms, ms));
            }
            if oom.load(Ordering::SeqCst) {
                runtime.v8_isolate().cancel_terminate_execution();
                return Err(RsError::limit_exceeded(
                    "memory_bytes",
                    limits.memory_bytes,
                    limits.memory_bytes,
                ));
            }
            if let Some(err) = inv.host_error.lock().unwrap().take() {
                return Err(err);
            }
            Err(RsError::contract_violation(format!("JS service failed: {fail}")))
        }
    }
}

/// Call `__rs2_dispatch(default, msg, config)` and drive: drain microtasks via
/// the event loop and, when the result promise is still pending with no real
/// work left, advance the virtual-time timers (host ops are synchronous, so
/// timers are the only async surface — backoffs fast-forward instead of
/// waiting). Returns the settled result as JSON.
async fn drive_dispatch(
    runtime: &mut JsRuntime,
    default_export: &v8::Global<v8::Value>,
    input: &Value,
    config: &Value,
) -> Result<Value, String> {
    // Gather the dispatch fn + args (no awaits while a scope is live).
    let (dispatch_fn, args): (v8::Global<v8::Function>, Vec<v8::Global<v8::Value>>) = {
        deno_core::scope!(scope, runtime);
        let global = scope.get_current_context().global(scope);
        let dispatch_key = v8::String::new(scope, "__rs2_dispatch").ok_or("oom")?;
        let dispatch_val =
            global.get(scope, dispatch_key.into()).ok_or("dispatch prelude missing")?;
        let dispatch_local: v8::Local<v8::Function> =
            dispatch_val.try_into().map_err(|_| "dispatch is not a function".to_string())?;

        let msg_local =
            serde_v8::to_v8(scope, input).map_err(|e| format!("msg marshaling: {e}"))?;
        let cfg_local =
            serde_v8::to_v8(scope, config).map_err(|e| format!("config marshaling: {e}"))?;

        let args = vec![
            default_export.clone(),
            v8::Global::new(scope, msg_local),
            v8::Global::new(scope, cfg_local),
        ];
        (v8::Global::new(scope, dispatch_local), args)
    };

    let promise = {
        deno_core::scope!(scope, runtime);
        let f = v8::Local::new(scope, &dispatch_fn);
        let recv = v8::undefined(scope).into();
        let argv: Vec<v8::Local<v8::Value>> =
            args.iter().map(|a| v8::Local::new(scope, a)).collect();
        let value = f.call(scope, recv, &argv).ok_or("dispatch threw")?;
        v8::Global::new(scope, value)
    };
    let run_timers: Option<v8::Global<v8::Function>> = {
        deno_core::scope!(scope, runtime);
        let global = scope.get_current_context().global(scope);
        let key = v8::String::new(scope, "__rs2_runTimers").ok_or("oom")?;
        global
            .get(scope, key.into())
            .and_then(|v| v8::Local::<v8::Function>::try_from(v).ok())
            .map(|f| v8::Global::new(scope, f))
    };

    let mut spins = 0u32;
    let settled = loop {
        let loop_result =
            runtime.run_event_loop(PollEventLoopOptions::default()).await;

        // Inspect the (original) promise directly — checked before propagating
        // a loop error so an uncaught rejection still surfaces its identity.
        let state: Option<Result<v8::Global<v8::Value>, String>> = {
            deno_core::scope!(scope, runtime);
            let p = v8::Local::new(scope, &promise);
            if !p.is_promise() {
                Some(Ok(v8::Global::new(scope, p)))
            } else {
                let promise = v8::Local::<v8::Promise>::try_from(p).unwrap();
                match promise.state() {
                    v8::PromiseState::Fulfilled => {
                        Some(Ok(v8::Global::new(scope, promise.result(scope))))
                    }
                    v8::PromiseState::Rejected => {
                        let r = promise.result(scope);
                        Some(Err(format!("handle rejected: {}", r.to_rust_string_lossy(scope))))
                    }
                    v8::PromiseState::Pending => None,
                }
            }
        };
        if let Some(done) = state {
            break done;
        }
        loop_result.map_err(|e| format!("event loop: {e}"))?;

        // Pending: fast-forward the earliest due virtual timers.
        let ran = match &run_timers {
            Some(f) => {
                deno_core::scope!(scope, runtime);
                let func = v8::Local::new(scope, f);
                let recv = v8::undefined(scope).into();
                func.call(scope, recv, &[]).map(|v| v.is_true()).unwrap_or(false)
            }
            None => false,
        };
        spins += 1;
        if !ran {
            return Err(
                "handle's promise never settles (no pending timers or host work)".to_string(),
            );
        }
        if spins > 1_000_000 {
            return Err("handle's timers never converge".to_string());
        }
    };

    let result = settled?;
    deno_core::scope!(scope, runtime);
    let local = v8::Local::new(scope, &result);
    if local.is_null_or_undefined() {
        return Ok(json!({ "status": 204 }));
    }
    serde_v8::from_v8::<Value>(scope, local).map_err(|e| format!("response marshaling: {e}"))
}

/// A bare value (not a `{ status?, body? }` envelope) is the 200 JSON body.
fn normalize_envelope(value: Value) -> Value {
    if value.is_object() && (value.get("status").is_some() || value.get("body").is_some()) {
        value
    } else {
        json!({ "status": 200, "body": value })
    }
}

#[cfg(test)]
mod socket_tests {
    use super::JsEngine;
    use crate::contract::{Engine, GrantedHost, HostApi, InvocationLimits, ServiceCode};
    use crate::message::Message;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn run(code: &str, config: serde_json::Value) -> Result<Message, crate::error::RsError> {
        let engine = JsEngine::new();
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("sock-test"));
        engine
            .invoke(
                &ServiceCode::JsBundle(Arc::new(code.to_string())),
                Message::request(http::Method::GET, "/x", "t1"),
                &config,
                host,
                &InvocationLimits::default(),
            )
            .await
    }

    /// A TCP echo server on a random port; returns the port.
    async fn spawn_echo() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if sock.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn socket_plain_tcp_roundtrip() {
        let port = spawn_echo().await;
        let config = json!({
            "grants": { "db": { "type": "socket", "hosts": [format!("127.0.0.1:{port}")] } }
        });
        let code = format!(
            r#"
            export default async (msg, ctx) => {{
                const s = RS2Socket.connect("127.0.0.1", {port});
                s.write("ping-42");
                const reply = s.read();
                s.close();
                return {{ status: 200, body: new TextDecoder().decode(reply) }};
            }};
            "#
        );
        let mut resp = run(&code, config).await.unwrap();
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"ping-42");
    }

    #[tokio::test]
    async fn socket_tls_roundtrip() {
        // Self-signed cert for "localhost".
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let key_der =
            rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());

        let provider = || Arc::new(rustls::crypto::ring::default_provider());
        let server_config = rustls::ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((tcp, _)) = listener.accept().await {
                if let Ok(mut tls) = acceptor.accept(tcp).await {
                    let mut buf = [0u8; 1024];
                    loop {
                        match tls.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if tls.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        // Client connector trusting the self-signed cert.
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client_config = rustls::ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        super::set_tls_connector_for_test(tokio_rustls::TlsConnector::from(Arc::new(client_config)));

        let config = json!({
            "grants": { "db": { "type": "socket", "hosts": [format!("localhost:{port}")] } }
        });
        let code = format!(
            r#"
            export default async (msg, ctx) => {{
                const s = RS2Socket.connect("localhost", {port}, {{ tls: true }});
                s.write("secure-hi");
                const reply = s.read();
                s.close();
                return {{ status: 200, body: new TextDecoder().decode(reply) }};
            }};
            "#
        );
        let mut resp = run(&code, config).await.unwrap();
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"secure-hi");
    }

    #[tokio::test]
    async fn socket_denied_without_grant() {
        let port = spawn_echo().await;
        let code = format!(
            r#"
            export default async (msg, ctx) => {{
                RS2Socket.connect("127.0.0.1", {port});
                return {{ status: 200 }};
            }};
            "#
        );
        // No socket grant → the connect is capability-denied.
        let err = run(&code, json!({})).await.unwrap_err();
        assert_eq!(err.code, crate::error::codes::CAPABILITY_DENIED);
    }
}

#[cfg(test)]
mod passthrough_tests {
    use super::JsEngine;
    use crate::contract::{Engine, GrantedHost, HostApi, InvocationLimits, ServiceCode};
    use crate::message::{Body, MediaType, Message, Provenance};
    use bytes::Bytes;
    use futures::StreamExt;
    use serde_json::json;
    use std::sync::Arc;

    /// A request carrying a two-chunk streaming body of the given media type.
    fn streaming_request(media_type: MediaType) -> Message {
        let chunks: Vec<Result<Bytes, std::io::Error>> =
            vec![Ok(Bytes::from_static(b"chunk1")), Ok(Bytes::from_static(b"chunk2"))];
        let stream = futures::stream::iter(chunks).boxed();
        let mut msg = Message::request(http::Method::POST, "/x", "t1");
        msg.body = Some(Body::from_stream(stream, media_type, Some(12), Provenance::Ephemeral));
        msg
    }

    async fn run(msg: Message, config: serde_json::Value, code: &str) -> Message {
        let engine = JsEngine::new();
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("passthrough-test"));
        engine
            .invoke(
                &ServiceCode::JsBundle(Arc::new(code.to_string())),
                msg,
                &config,
                host,
                &InvocationLimits::default(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn forwards_request_stream_without_materializing() {
        // The guest returns no body, so the carried request stream is forwarded
        // as the response body — still a stream (never materialized).
        let code = r#"
            export default async (msg, ctx) => {
                return { status: 200, headers: {
                    "x-passthrough": String(msg.bodyPassthrough),
                    "x-size": String(msg.bodySize),
                    "x-body-null": String(msg.body === null),
                } };
            };
        "#;
        let config = json!({ "bodyPassthrough": true });
        let mut resp = run(streaming_request(MediaType::octet_stream()), config, code).await;

        // The guest saw metadata, not bytes.
        assert_eq!(resp.headers.get("x-passthrough").unwrap(), "true");
        assert_eq!(resp.headers.get("x-size").unwrap(), "12");
        assert_eq!(resp.headers.get("x-body-null").unwrap(), "true");

        // The forwarded body is still a stream (proof it never materialized).
        let body = resp.body.as_mut().unwrap();
        assert!(body.is_stream(), "passthrough must forward the stream by reference");
        let bytes = body.materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"chunk1chunk2");
    }

    #[tokio::test]
    async fn guest_body_replaces_the_carried_stream() {
        // When the guest returns its own body under passthrough, that wins and
        // the carried request stream is dropped.
        let code = r#"
            export default async (msg, ctx) => {
                return { status: 200, body: "replaced" };
            };
        "#;
        let config = json!({ "bodyPassthrough": true });
        let mut resp = run(streaming_request(MediaType::octet_stream()), config, code).await;
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"replaced");
    }

    #[tokio::test]
    async fn without_passthrough_body_materializes_for_the_guest() {
        // Control: no passthrough flag → the guest sees the body bytes as usual.
        let code = r#"
            export default async (msg, ctx) => {
                return { status: 200, body: msg.body, mediaType: "text/plain" };
            };
        "#;
        let mut resp =
            run(streaming_request(MediaType::new("text/plain")), json!({}), code).await;
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"chunk1chunk2");
    }

    /// Like [`run`] but with explicit limits, surfacing the error path.
    async fn run_limited(
        msg: Message,
        config: serde_json::Value,
        code: &str,
        limits: InvocationLimits,
    ) -> Result<Message, crate::error::RsError> {
        let engine = JsEngine::new();
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("streaming-test"));
        engine
            .invoke(
                &ServiceCode::JsBundle(Arc::new(code.to_string())),
                msg,
                &config,
                host,
                &limits,
            )
            .await
    }

    #[tokio::test]
    async fn request_streaming_reads_chunks_incrementally() {
        // The guest pulls the body chunk-by-chunk; `msg.body` stays null.
        let code = r#"
            export default async (msg, ctx) => {
                if (!msg.requestStreaming || msg.body !== null) {
                    return { status: 500, body: "not streaming" };
                }
                let chunks = 0; const parts = [];
                for (;;) {
                    const c = ctx.readBody();
                    if (c === null) break;
                    chunks++; parts.push(new TextDecoder().decode(c));
                }
                return { status: 200, headers: { "x-chunks": String(chunks) }, body: parts.join("") };
            };
        "#;
        let config = json!({ "requestStreaming": true });
        let mut resp = run(streaming_request(MediaType::octet_stream()), config, code).await;
        assert_eq!(resp.headers.get("x-chunks").unwrap(), "2");
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"chunk1chunk2");
    }

    #[tokio::test]
    async fn request_streaming_async_iterator() {
        // The `for await (… of ctx.body())` sugar drains the same stream.
        let code = r#"
            export default async (msg, ctx) => {
                const parts = [];
                for await (const c of ctx.body()) { parts.push(new TextDecoder().decode(c)); }
                return { status: 200, body: parts.join("") };
            };
        "#;
        let config = json!({ "requestStreaming": true });
        let mut resp = run(streaming_request(MediaType::octet_stream()), config, code).await;
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"chunk1chunk2");
    }

    #[tokio::test]
    async fn request_streaming_enforces_cumulative_cap() {
        // A cap below the body size aborts mid-stream with LIMIT_EXCEEDED, even
        // though no single chunk exceeds it (the bound is cumulative).
        let code = r#"
            export default async (msg, ctx) => {
                for (;;) { if (ctx.readBody() === null) break; }
                return { status: 200 };
            };
        "#;
        let mut limits = InvocationLimits::default();
        limits.materialized_body_bytes = 8; // body is 12 bytes across two chunks
        let config = json!({ "requestStreaming": true });
        let err = run_limited(streaming_request(MediaType::octet_stream()), config, code, limits)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::codes::LIMIT_EXCEEDED);
    }

    /// Drain a streamed response body into its discrete chunks (asserting it is
    /// actually a stream, not a buffer).
    async fn collect_chunks(mut msg: Message) -> Vec<Vec<u8>> {
        let body = msg.body.take().expect("response has a body");
        assert!(body.is_stream(), "expected a streamed response body");
        body.into_stream().map(|r| r.unwrap().to_vec()).collect().await
    }

    #[tokio::test]
    async fn response_streaming_emits_chunks_progressively() {
        // The guest declares headers, then pushes the body chunk-by-chunk; the
        // chunks arrive as discrete stream items (not one buffered body).
        let code = r#"
            export default async (msg, ctx) => {
                if (!msg.responseStreaming) throw new Error("expected responseStreaming");
                const w = ctx.beginStream({ status: 200, mediaType: "text/event-stream" });
                w.write("data: 1\n\n");
                w.write("data: 2\n\n");
                w.write("data: 3\n\n");
            };
        "#;
        let config = json!({ "responseStreaming": true });
        let resp = run(Message::request(http::Method::GET, "/sse", "t1"), config, code).await;
        assert_eq!(resp.status.unwrap(), http::StatusCode::OK);
        let chunks = collect_chunks(resp).await;
        assert_eq!(chunks.len(), 3, "each write should surface as its own chunk");
        let joined: Vec<u8> = chunks.concat();
        assert_eq!(&joined[..], b"data: 1\n\ndata: 2\n\ndata: 3\n\n");
    }

    #[tokio::test]
    async fn response_streaming_carries_media_type_and_headers() {
        let code = r#"
            export default async (msg, ctx) => {
                const w = ctx.beginStream({
                    status: 201,
                    mediaType: "application/x-ndjson",
                    headers: { "x-stream": "yes" },
                });
                w.write("{}\n");
            };
        "#;
        let config = json!({ "responseStreaming": true });
        let mut resp = run(Message::request(http::Method::GET, "/x", "t1"), config, code).await;
        assert_eq!(resp.status.unwrap(), http::StatusCode::CREATED);
        assert_eq!(resp.headers.get("x-stream").unwrap(), "yes");
        let body = resp.body.as_mut().unwrap();
        assert_eq!(body.media_type.essence(), "application/x-ndjson");
    }

    #[tokio::test]
    async fn response_streaming_handler_without_stream_falls_back() {
        // responseStreaming is enabled but the guest returns a normal envelope
        // (never calls beginStream) → the envelope maps as usual.
        let code = r#"
            export default async (msg, ctx) => {
                return { status: 200, body: "plain" };
            };
        "#;
        let config = json!({ "responseStreaming": true });
        let mut resp = run(Message::request(http::Method::GET, "/x", "t1"), config, code).await;
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"plain");
    }

    #[tokio::test]
    async fn response_streaming_enforces_cumulative_cap() {
        // With a cap of 8 bytes, the first two 4-byte writes pass and the third
        // is rejected mid-stream — so the consumer sees exactly the first 8 bytes
        // then EOF (the over-cap chunk never lands).
        let code = r#"
            export default async (msg, ctx) => {
                const w = ctx.beginStream({ status: 200 });
                w.write("aaaa");
                w.write("bbbb");
                try { w.write("cccc"); } catch (e) { /* cap exceeded → stop */ }
            };
        "#;
        let mut limits = InvocationLimits::default();
        limits.materialized_body_bytes = 8;
        let config = json!({ "responseStreaming": true });
        let resp = run_limited(Message::request(http::Method::GET, "/x", "t1"), config, code, limits)
            .await
            .unwrap();
        let chunks = collect_chunks(resp).await;
        let joined: Vec<u8> = chunks.concat();
        assert_eq!(&joined[..], b"aaaabbbb", "the over-cap chunk must not be delivered");
    }
}
