//! V8 isolate engine (PRD §5.3): runs JS service bundles in sandboxed
//! isolates under the engine-neutral contract.
//!
//! Shape (PRD open question 2 resolved conservatively): **one isolate per
//! invocation**, created on a blocking thread — full isolation between
//! concurrent invocations at the cost of cold-start latency; warm pools and
//! snapshot-amortized starts are a later optimization behind this same API.
//!
//! Bundle contract: a single-file ES module whose default export is either
//! `async (msg, ctx) => response` or an object with such a `handle` method.
//! `msg` = `{ method, url, headers, body, mediaType }` (JSON bodies arrive
//! parsed); the response is `{ status?, headers?, body?, mediaType? }`, or
//! any other value to mean a 200 JSON body. `ctx` =
//! `{ config, request(capability, req), log(level, text),
//!    state: { get(key), put(key, value) } }`.
//!
//! Host calls are synchronous from the guest's point of view (the isolate
//! thread parks on the async host future); `async`/`await` in guest code
//! works because the microtask queue is pumped to quiescence. There is no
//! event loop: timers are not available in v1.
//!
//! Limits: wall clock via a watchdog thread calling `terminate_execution`;
//! memory via isolate heap limits with a near-limit callback; the outbound
//! budget and materialization caps are host-enforced as for every engine.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::contract::{Engine, HostApi, InvocationLimits, LogLevel, ServiceCode};
use crate::error::{codes, RsError};
use crate::message::{Body, MediaType, Message, Source};

/// The compat prelude (G5): the supported web/Node API surface, injected
/// before every bundle. The explicit supported-API list lives at the top of
/// the prelude file and is pinned by `tests/npm_compat.rs`.
const PRELUDE: &str = include_str!("js_prelude.js");

static V8_INIT: Once = Once::new();

fn ensure_v8() {
    V8_INIT.call_once(|| {
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    });
}

/// Map a wire error code back to its static identity (RsError carries
/// `&'static str` codes by design).
fn static_code(code: &str) -> &'static str {
    for known in [
        codes::BAD_REQUEST,
        codes::UNAUTHORIZED,
        codes::FORBIDDEN,
        codes::NOT_FOUND,
        codes::CONFLICT,
        codes::PAYLOAD_TOO_LARGE,
        codes::VALIDATION_FAILED,
        codes::IDEMPOTENCY_KEY_REUSE,
        codes::LIMIT_EXCEEDED,
        codes::CAPABILITY_DENIED,
        codes::CONTRACT_VIOLATION,
        codes::ENGINE_UNAVAILABLE,
        codes::PATH_UNSAFE,
        codes::INTERNAL,
    ] {
        if code == known {
            return known;
        }
    }
    codes::CONTRACT_VIOLATION
}

/// Per-invocation state reachable from V8 callbacks (via `External`).
/// Lives on the isolate thread's stack for the duration of the invocation.
struct InvocationState {
    host: Arc<dyn HostApi>,
    rt: tokio::runtime::Handle,
    tenant: String,
    depth: u16,
    materialize_cap: u64,
    /// The last structured host error thrown into JS — if it propagates
    /// uncaught, the invocation fails with this error, not a generic one.
    host_error: Mutex<Option<RsError>>,
}

struct Watchdog {
    done: Arc<AtomicBool>,
    timed_out: Arc<AtomicBool>,
}

impl Watchdog {
    fn arm(isolate: &v8::Isolate, wall_clock: Duration) -> Watchdog {
        let handle = isolate.thread_safe_handle();
        let done = Arc::new(AtomicBool::new(false));
        let timed_out = Arc::new(AtomicBool::new(false));
        let (done2, timed_out2) = (done.clone(), timed_out.clone());
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + wall_clock;
            while std::time::Instant::now() < deadline {
                if done2.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            if !done2.load(Ordering::SeqCst) {
                timed_out2.store(true, Ordering::SeqCst);
                handle.terminate_execution();
            }
        });
        Watchdog { done, timed_out }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
    }
}

/// Near-heap-limit handling: flag the breach, give V8 headroom to unwind,
/// and terminate. Without this an over-allocating guest aborts the process.
struct OomState {
    flagged: AtomicBool,
    isolate_handle: v8::IsolateHandle,
}

extern "C" fn near_heap_limit(data: *mut c_void, current_heap_limit: usize, _initial: usize) -> usize {
    let state = unsafe { &*(data as *const OomState) };
    state.flagged.store(true, Ordering::SeqCst);
    state.isolate_handle.terminate_execution();
    // Headroom so termination can unwind instead of hard-OOM-ing.
    current_heap_limit * 2
}

#[derive(Default)]
pub struct JsEngine;

impl JsEngine {
    pub fn new() -> Self {
        ensure_v8();
        JsEngine
    }

    /// Compile-only validation: the deployment-time smoke test for
    /// `PUT /code/<name>` with a JS bundle (PRD §10.6).
    pub fn compile_check(&self, source: &str) -> Result<(), RsError> {
        let source = source.to_string();
        std::thread::spawn(move || -> Result<(), RsError> {
            ensure_v8();
            let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
            v8::scope!(let scope, isolate);
            let context = v8::Context::new(scope, Default::default());
            let scope = &mut v8::ContextScope::new(scope, context);
            v8::tc_scope!(let tc, scope);
            // The compat runtime with no invocation behind it: host-call
            // natives throw if a bundle does I/O at module top level.
            install_runtime(tc, std::ptr::null_mut())
                .ok_or_else(|| RsError::internal("compat runtime installation failed"))?;
            match compile_bundle(tc, &source) {
                Some(_) => Ok(()),
                None => {
                    let detail = caught_text(tc);
                    Err(RsError::contract_violation(format!(
                        "JS bundle failed to compile: {detail}"
                    )))
                }
            }
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

        // Bodies materialize at the engine boundary, capped (invariant 1).
        let body_json = match &mut msg.body {
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
            "body": body_json["payload"],
            "mediaType": body_json["mediaType"],
        });

        let config_json = config.to_string();
        let rt = tokio::runtime::Handle::current();
        let tenant = msg.tenant.clone();
        let depth = msg.depth;
        let limits = limits.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            run_isolate(&source, input, &config_json, host, rt, tenant, depth, &limits)
        })
        .await
        .map_err(|e| RsError::internal(format!("isolate thread failed: {e}")))??;

        // Map the guest's response envelope to a Message.
        let status = outcome
            .get("status")
            .and_then(|s| s.as_u64())
            .and_then(|s| http::StatusCode::from_u16(s as u16).ok())
            .unwrap_or(http::StatusCode::OK);
        let media_type = outcome
            .get("mediaType")
            .and_then(|m| m.as_str())
            .map(MediaType::parse);
        let body = match outcome.get("body") {
            None | Some(Value::Null) => None,
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
        Ok(resp)
    }
}

/// Everything inside the isolate, on the blocking thread. Returns the
/// guest's response envelope as JSON.
#[allow(clippy::too_many_arguments)]
fn run_isolate(
    source: &str,
    input: Value,
    config_json: &str,
    host: Arc<dyn HostApi>,
    rt: tokio::runtime::Handle,
    tenant: String,
    depth: u16,
    limits: &InvocationLimits,
) -> Result<Value, RsError> {
    ensure_v8();
    let params = v8::CreateParams::default().heap_limits(0, limits.memory_bytes as usize);
    let isolate = &mut v8::Isolate::new(params);

    let oom = Box::new(OomState {
        flagged: AtomicBool::new(false),
        isolate_handle: isolate.thread_safe_handle(),
    });
    isolate.add_near_heap_limit_callback(near_heap_limit, &*oom as *const OomState as *mut c_void);

    let watchdog = Watchdog::arm(isolate, limits.wall_clock);

    let state = InvocationState {
        host,
        rt,
        tenant,
        depth,
        materialize_cap: limits.materialized_body_bytes,
        host_error: Mutex::new(None),
    };

    let result = {
        v8::scope!(let scope, isolate);
        let context = v8::Context::new(scope, Default::default());
        let scope = &mut v8::ContextScope::new(scope, context);
        v8::tc_scope!(let tc, scope);
        let result = execute(tc, source, &input, config_json, &state);
        // Attach the caught JS exception text while the TryCatch is live.
        result.map_err(|m| {
            if tc.has_caught() {
                format!("{m}: {}", caught_text(tc))
            } else {
                m
            }
        })
    };

    let timed_out = watchdog.timed_out.load(Ordering::SeqCst);
    drop(watchdog);

    match result {
        Ok(value) => Ok(value),
        Err(fail) => {
            if timed_out {
                return Err(RsError::limit_exceeded(
                    "wall_clock_ms",
                    limits.wall_clock.as_millis() as u64,
                    limits.wall_clock.as_millis() as u64,
                ));
            }
            if oom.flagged.load(Ordering::SeqCst) {
                return Err(RsError::limit_exceeded(
                    "memory_bytes",
                    limits.memory_bytes,
                    limits.memory_bytes,
                ));
            }
            // An uncaught structured host error keeps its identity
            // (invariant 2: capability denial surfaces as such).
            if let Some(err) = state.host_error.lock().unwrap().take() {
                return Err(err);
            }
            Err(RsError::contract_violation(format!("JS service failed: {fail}")))
        }
    }
}

/// Compile the bundle as a single-file ES module and return its namespace.
fn compile_bundle<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    source: &str,
) -> Option<v8::Local<'s, v8::Object>> {
    let source_str = v8::String::new(scope, source)?;
    let name = v8::String::new(scope, "service.js")?;
    let origin = v8::ScriptOrigin::new(
        scope,
        name.into(),
        0,
        0,
        false,
        0,
        None,
        false,
        false,
        true, // is_module
        None,
    );
    let mut compiler_source = v8::script_compiler::Source::new(source_str, Some(&origin));
    let module = v8::script_compiler::compile_module(scope, &mut compiler_source)?;
    module.instantiate_module(scope, no_imports_resolve)?;
    module.evaluate(scope)?;
    if module.get_status() != v8::ModuleStatus::Evaluated {
        return None;
    }
    module.get_module_namespace().to_object(scope)
}

/// Single-file bundles only: imports fail at instantiation.
fn no_imports_resolve<'s>(
    _context: v8::Local<'s, v8::Context>,
    _specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
    _referrer: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    // Bundle dependencies at build time (esbuild); module imports are not
    // resolvable inside the sandbox.
    None
}

fn execute(
    scope: &mut v8::PinScope<'_, '_>,
    source: &str,
    input: &Value,
    config_json: &str,
    state: &InvocationState,
) -> Result<Value, String> {
    install_runtime(scope, state as *const InvocationState as *mut c_void)
        .ok_or("compat runtime installation failed")?;
    let namespace = compile_bundle(scope, source).ok_or("bundle error")?;

    let default_key = v8::String::new(scope, "default").ok_or("oom")?;
    let default_export =
        namespace.get(scope, default_key.into()).ok_or("no default export")?;

    // The default export is `handle` itself or `{ handle }`.
    let handle_fn: v8::Local<v8::Function> = if default_export.is_function() {
        default_export.try_into().map_err(|_| "default export is not callable".to_string())?
    } else if default_export.is_object() {
        let obj = default_export.to_object(scope).ok_or("default export is not an object")?;
        let handle_key = v8::String::new(scope, "handle").ok_or("oom")?;
        let handle = obj.get(scope, handle_key.into()).ok_or("default export has no 'handle'")?;
        handle
            .try_into()
            .map_err(|_| "default export's 'handle' is not a function".to_string())?
    } else {
        return Err("default export must be a function or { handle }".to_string());
    };

    let msg_value = parse_json(scope, &input.to_string()).ok_or("input marshaling failed")?;
    let ctx_value = build_ctx(scope, config_json, state).ok_or("ctx construction failed")?;

    // Timer hook installed by the prelude, used to fast-forward virtual
    // time while the handler's promise is pending.
    let global = scope.get_current_context().global(scope);
    let run_timers_key = v8::String::new(scope, "__rs2_runTimers").ok_or("oom")?;
    let run_timers: Option<v8::Local<v8::Function>> = global
        .get(scope, run_timers_key.into())
        .and_then(|v| v.try_into().ok());

    let recv = v8::undefined(scope).into();
    let result = handle_fn.call(scope, recv, &[msg_value, ctx_value]).ok_or("handle threw")?;

    // Pump microtasks until the (possibly async) result settles. Host calls
    // are synchronous, so quiescence is reached without an event loop.
    let result = if result.is_promise() {
        let promise: v8::Local<v8::Promise> =
            result.try_into().map_err(|_| "promise cast failed".to_string())?;
        let mut spins = 0;
        loop {
            match promise.state() {
                v8::PromiseState::Fulfilled => break promise.result(scope),
                v8::PromiseState::Rejected => {
                    let rejection = promise.result(scope);
                    return Err(format!(
                        "handle rejected: {}",
                        rejection.to_rust_string_lossy(scope)
                    ));
                }
                v8::PromiseState::Pending => {
                    scope.perform_microtask_checkpoint();
                    if promise.state() != v8::PromiseState::Pending {
                        continue;
                    }
                    // Microtasks exhausted and still pending: fast-forward
                    // virtual time and run due timers (retry backoffs etc.).
                    let ran_timer = match &run_timers {
                        Some(f) => {
                            let recv = v8::undefined(scope).into();
                            f.call(scope, recv, &[])
                                .map(|v| v.is_true())
                                .ok_or("timer callback threw")?
                        }
                        None => false,
                    };
                    spins += 1;
                    if !ran_timer && spins > 10_000 {
                        return Err(
                            "handle's promise never settles (no pending timers or host work)"
                                .to_string(),
                        );
                    }
                    if spins > 1_000_000 {
                        return Err("handle's timers never converge".to_string());
                    }
                }
            }
        }
    } else {
        result
    };

    // Marshal the response envelope out as JSON.
    if result.is_null_or_undefined() {
        return Ok(json!({ "status": 204 }));
    }
    let text = stringify(scope, result).ok_or("response is not JSON-serializable")?;
    let value: Value =
        serde_json::from_str(&text).map_err(|e| format!("response marshaling failed: {e}"))?;
    // Envelope if it looks like one; otherwise the value is the body.
    if value.is_object() && (value.get("status").is_some() || value.get("body").is_some()) {
        Ok(value)
    } else {
        Ok(json!({ "status": 200, "body": value }))
    }
}

/// Build the `ctx` capability object handed to the guest.
fn build_ctx<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    config_json: &str,
    state: &InvocationState,
) -> Option<v8::Local<'s, v8::Value>> {
    let ctx = v8::Object::new(scope);
    let external = v8::External::new(scope, state as *const InvocationState as *mut c_void);

    let config = parse_json(scope, config_json).unwrap_or_else(|| v8::null(scope).into());
    let config_key = v8::String::new(scope, "config")?;
    ctx.set(scope, config_key.into(), config)?;

    let request_fn =
        v8::Function::builder(host_request_callback).data(external.into()).build(scope)?;
    let request_key = v8::String::new(scope, "request")?;
    ctx.set(scope, request_key.into(), request_fn.into())?;

    let log_fn = v8::Function::builder(host_log_callback).data(external.into()).build(scope)?;
    let log_key = v8::String::new(scope, "log")?;
    ctx.set(scope, log_key.into(), log_fn.into())?;

    let state_obj = v8::Object::new(scope);
    let get_fn = v8::Function::builder(state_get_callback).data(external.into()).build(scope)?;
    let get_key = v8::String::new(scope, "get")?;
    state_obj.set(scope, get_key.into(), get_fn.into())?;
    let put_fn = v8::Function::builder(state_put_callback).data(external.into()).build(scope)?;
    let put_key = v8::String::new(scope, "put")?;
    state_obj.set(scope, put_key.into(), put_fn.into())?;
    let state_key = v8::String::new(scope, "state")?;
    ctx.set(scope, state_key.into(), state_obj.into())?;

    Some(ctx.into())
}

fn invocation_state<'a>(args: &v8::FunctionCallbackArguments<'a>) -> &'a InvocationState {
    try_invocation_state(args).expect("callback requires a live invocation")
}

fn try_invocation_state<'a>(
    args: &v8::FunctionCallbackArguments<'a>,
) -> Option<&'a InvocationState> {
    let external: v8::Local<v8::External> =
        args.data().try_into().expect("callback data is the invocation external");
    let ptr = external.value() as *const InvocationState;
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*ptr })
    }
}

/// Install the compat runtime (G5): raw native hooks, then the prelude that
/// wraps them into the supported web/Node API surface and removes them.
fn install_runtime(scope: &mut v8::PinScope<'_, '_>, state_ptr: *mut c_void) -> Option<()> {
    let global = scope.get_current_context().global(scope);
    let external = v8::External::new(scope, state_ptr);

    let fetch_fn = v8::Function::builder(native_fetch_callback).data(external.into()).build(scope)?;
    let fetch_key = v8::String::new(scope, "__rs2_native_fetch")?;
    global.set(scope, fetch_key.into(), fetch_fn.into())?;

    let log_fn = v8::Function::builder(native_log_callback).data(external.into()).build(scope)?;
    let log_key = v8::String::new(scope, "__rs2_native_log")?;
    global.set(scope, log_key.into(), log_fn.into())?;

    let random_fn =
        v8::Function::builder(native_random_callback).data(external.into()).build(scope)?;
    let random_key = v8::String::new(scope, "__rs2_native_random")?;
    global.set(scope, random_key.into(), random_fn.into())?;

    let prelude = v8::String::new(scope, PRELUDE)?;
    let script = v8::Script::compile(scope, prelude, None)?;
    script.run(scope)?;
    Some(())
}

/// `fetch` (compat layer): an absolute-URL request through the `fetch`
/// capability grant — default deny, host-allowlisted (PRD §9.2).
fn native_fetch_callback(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(state) = try_invocation_state(&args) else {
        throw_text(scope, "fetch is not available during deploy-time validation");
        return;
    };
    let req_json = match stringify(scope, args.get(0)) {
        Some(s) => s,
        None => {
            throw_text(scope, "fetch request is not serializable");
            return;
        }
    };
    let req: Value = serde_json::from_str(&req_json).unwrap_or(Value::Null);
    let url = req.get("url").and_then(|u| u.as_str()).unwrap_or("");
    let method = req
        .get("method")
        .and_then(|m| m.as_str())
        .and_then(|m| http::Method::from_bytes(m.as_bytes()).ok())
        .unwrap_or(http::Method::GET);

    // Absolute URL carried in the message path; the HttpOut adapter parses.
    let mut call = Message::request(method, url, &state.tenant);
    call.source = Source::Internal;
    call.depth = state.depth.saturating_add(1);
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
    if let Some(Value::String(text)) = req.get("body") {
        let mt = call
            .header("content-type")
            .map(MediaType::parse)
            .unwrap_or_else(|| MediaType::new("text/plain"));
        call.body = Some(Body::from_string(text.clone(), mt));
    }

    match state.rt.block_on(state.host.request("fetch", call)) {
        Ok(mut resp) => {
            let status = resp.status.map(|s| s.as_u16()).unwrap_or(200);
            let text = match &mut resp.body {
                None => String::new(),
                Some(body) => match state.rt.block_on(body.materialize(state.materialize_cap)) {
                    Ok(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    Err(e) => {
                        *state.host_error.lock().unwrap() = Some(e.clone());
                        throw_rs_error(scope, &e);
                        return;
                    }
                },
            };
            let headers: serde_json::Map<String, Value> = resp
                .headers
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), json!(s))))
                .collect();
            let envelope = json!({ "status": status, "headers": headers, "body": text });
            if let Some(value) = parse_json(scope, &envelope.to_string()) {
                rv.set(value);
            }
        }
        Err(e) => {
            *state.host_error.lock().unwrap() = Some(e.clone());
            throw_rs_error(scope, &e);
        }
    }
}

fn native_log_callback(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Some(state) = try_invocation_state(&args) else { return };
    let level = match args.get(0).to_rust_string_lossy(scope).as_str() {
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => LogLevel::Info,
    };
    let text = args.get(1).to_rust_string_lossy(scope);
    state.host.log(level, &text);
}

fn native_random_callback(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use rand::RngCore;
    let n = args
        .get(0)
        .to_uint32(scope)
        .map(|v| v.value() as usize)
        .unwrap_or(0)
        .min(65536);
    let mut bytes = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut bytes);
    let array: Vec<Value> = bytes.into_iter().map(|b| json!(b)).collect();
    if let Some(value) = parse_json(scope, &Value::Array(array).to_string()) {
        rv.set(value);
    }
}

/// `ctx.request(capability, { method?, url, headers?, body?, mediaType? })`
/// — the only way out of the sandbox; default-deny via the GrantedHost.
fn host_request_callback(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let state = invocation_state(&args);
    let capability = args.get(0).to_rust_string_lossy(scope);
    let req_json = match stringify(scope, args.get(1)) {
        Some(s) => s,
        None => {
            throw_text(scope, "request payload is not serializable");
            return;
        }
    };
    let req: Value = serde_json::from_str(&req_json).unwrap_or(Value::Null);

    let method = req
        .get("method")
        .and_then(|m| m.as_str())
        .and_then(|m| http::Method::from_bytes(m.as_bytes()).ok())
        .unwrap_or(http::Method::GET);
    let url = req.get("url").and_then(|u| u.as_str()).unwrap_or("/");
    let mut call = Message::request(method, url, &state.tenant);
    call.source = Source::Internal;
    call.depth = state.depth.saturating_add(1);
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

    let result = state.rt.block_on(state.host.request(&capability, call));
    match result {
        Ok(mut resp) => {
            let status = resp.status.map(|s| s.as_u16()).unwrap_or(200);
            let (media_type, payload) = match &mut resp.body {
                None => (Value::Null, Value::Null),
                Some(body) => {
                    let mt = body.media_type.to_string();
                    let is_json = body.media_type.is_json();
                    match state.rt.block_on(body.materialize(state.materialize_cap)) {
                        Ok(bytes) => {
                            let text = String::from_utf8_lossy(bytes).into_owned();
                            let payload = if is_json {
                                serde_json::from_str(&text).unwrap_or(Value::String(text))
                            } else {
                                Value::String(text)
                            };
                            (json!(mt), payload)
                        }
                        Err(e) => {
                            *state.host_error.lock().unwrap() = Some(e.clone());
                            throw_rs_error(scope, &e);
                            return;
                        }
                    }
                }
            };
            let headers: serde_json::Map<String, Value> = resp
                .headers
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), json!(s))))
                .collect();
            let envelope =
                json!({ "status": status, "headers": headers, "body": payload, "mediaType": media_type });
            if let Some(value) = parse_json(scope, &envelope.to_string()) {
                rv.set(value);
            }
        }
        Err(e) => {
            *state.host_error.lock().unwrap() = Some(e.clone());
            throw_rs_error(scope, &e);
        }
    }
}

fn host_log_callback(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let state = invocation_state(&args);
    let level = match args.get(0).to_rust_string_lossy(scope).as_str() {
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => LogLevel::Info,
    };
    let text = args.get(1).to_rust_string_lossy(scope);
    state.host.log(level, &text);
}

fn state_get_callback(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let state = invocation_state(&args);
    let key = args.get(0).to_rust_string_lossy(scope);
    match state.rt.block_on(state.host.state_get(&key)) {
        Some(bytes) => {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            if let Some(s) = v8::String::new(scope, &text) {
                rv.set(s.into());
            }
        }
        None => rv.set(v8::null(scope).into()),
    }
}

fn state_put_callback(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let state = invocation_state(&args);
    let key = args.get(0).to_rust_string_lossy(scope);
    let value = args.get(1).to_rust_string_lossy(scope);
    state.rt.block_on(state.host.state_put(&key, value.into_bytes()));
}

/// Throw a structured host error into JS: an `Error` whose `code` and
/// `status` properties carry the RsError identity.
fn throw_rs_error(scope: &mut v8::PinScope, err: &RsError) {
    let message = v8::String::new(scope, &err.detail).unwrap_or_else(|| {
        v8::String::new(scope, "host error").unwrap()
    });
    let exception = v8::Exception::error(scope, message);
    if let Some(obj) = exception.to_object(scope) {
        if let (Some(code_key), Some(code_val)) =
            (v8::String::new(scope, "code"), v8::String::new(scope, err.code))
        {
            obj.set(scope, code_key.into(), code_val.into());
        }
        if let Some(status_key) = v8::String::new(scope, "status") {
            let status_val = v8::Number::new(scope, err.status as f64);
            obj.set(scope, status_key.into(), status_val.into());
        }
    }
    scope.throw_exception(exception);
}

fn throw_text(scope: &mut v8::PinScope, text: &str) {
    if let Some(message) = v8::String::new(scope, text) {
        let exception = v8::Exception::error(scope, message);
        scope.throw_exception(exception);
    }
}

fn parse_json<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    json_text: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let s = v8::String::new(scope, json_text)?;
    v8::json::parse(scope, s)
}

fn stringify(scope: &mut v8::PinScope, value: v8::Local<v8::Value>) -> Option<String> {
    let s = v8::json::stringify(scope, value)?;
    Some(s.to_rust_string_lossy(scope))
}

/// The caught exception's message text, while the TryCatch is live.
fn caught_text(tc: &mut v8::PinnedRef<'_, v8::TryCatch<'_, '_, v8::HandleScope<'_>>>) -> String {
    match tc.message() {
        Some(m) => m.get(tc).to_rust_string_lossy(tc),
        None => "(terminated)".to_string(),
    }
}

/// `static_code` keeps wire codes static; used when surfacing guest-thrown
/// structured errors in future revisions.
#[allow(dead_code)]
fn rs_error_from_code(code: &str, status: u16, detail: String) -> RsError {
    let mut e = RsError::new(status, static_code(code), "Service Error", detail);
    if e.code == codes::LIMIT_EXCEEDED {
        e.retryable = true;
    }
    e
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_code_round_trip() {
        assert_eq!(static_code("capability_denied"), codes::CAPABILITY_DENIED);
        assert_eq!(static_code("nonsense"), codes::CONTRACT_VIOLATION);
    }
}
