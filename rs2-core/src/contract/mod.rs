//! Engine-neutral service contract (PRD §5.4): one contract, multiple
//! engine bindings (Wasm components, V8 isolates, and an in-process native
//! binding used as the conformance reference).
//!
//! Contract invariants every engine must uphold (conformance-tested):
//! 1. Body materialization is subject to host size limits.
//! 2. All I/O goes through named capabilities; an ungranted capability name
//!    fails with `capability_denied`.
//! 3. `host.request` is the only way to call other services/HTTP; the host
//!    stamps trace context onto it.
//! 4. Invocation is the unit of limit enforcement; no state survives an
//!    invocation except via the `state` capability.
//!
//! The normative WIT world lives at `rs2-core/wit/service.wit`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::error::RsError;
use crate::message::{Body, Message, MsgUrl, Source};
use crate::pipeline::Requester;

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// What the host exposes to sandboxed code, behind capability grants.
#[async_trait]
pub trait HostApi: Send + Sync {
    /// Capability-scoped call: `capability` resolves against config grants.
    async fn request(&self, capability: &str, msg: Message) -> Result<Message, RsError>;
    fn log(&self, level: LogLevel, text: &str);
    async fn state_get(&self, key: &str) -> Option<Vec<u8>>;
    async fn state_put(&self, key: &str, value: Vec<u8>);

    /// One guest socket op (`socket.send`/`close`, `ctx.sockets.*` —
    /// `docs/agents/websocket.md`, "Guests"): a request to the invocation's
    /// **own** mount's `/.sockets/` subtree. `target` is what the guest passed
    /// (a socket id, or a `{path?, subtree?, id?, user?}` selection) and is
    /// untrusted; `call` carries only the invocation's identity (tenant,
    /// principal, depth) — the host decides the method, the URL and the
    /// source. Default deny: a host with no socket surface (an unflagged
    /// mount, a resident adapter, a test double) answers `capability_denied`.
    async fn socket_control(
        &self,
        op: SocketOp,
        target: &serde_json::Value,
        call: Message,
    ) -> Result<Message, RsError> {
        let _ = (op, target, call);
        Err(RsError::capability_denied("socket"))
    }
}

/// What a guest may do to a selection of its mount's sockets.
pub enum SocketOp {
    /// Send the body as one frame (its media type picks text or binary, as
    /// for any `POST /<m>/.sockets/…`).
    Send(Body),
    /// Close the selection; the host defaults the code to 1000 and holds it to
    /// the range a caller may send.
    Close {
        code: Option<i64>,
        reason: Option<String>,
    },
    /// List the selection's connections (a `path` lists that subtree).
    List,
}

/// Where a guest socket op may act: the invocation's OWN mount base and the
/// way back into dispatch (the same [`Requester`] a `prefix` grant uses).
/// Host-side state — the guest never names a mount.
#[derive(Clone)]
pub struct SocketMount {
    /// The mount's base path (`/chat`, or `/` for a root mount).
    pub base: String,
    pub requester: Arc<dyn Requester>,
}

/// A path segment with nothing that could re-route a URL: no separators, no
/// query/fragment marks, no control characters.
fn plain_segment(seg: &str) -> bool {
    !seg.chars()
        .any(|c| c.is_control() || matches!(c, '/' | '\\' | '?' | '#'))
}

/// Strict percent-decoding (JS `decodeURIComponent`): a `%` not followed by
/// two hex digits, or bytes that are not UTF-8, is an error rather than
/// passed through — a lenient decode here and a strict one in the router
/// would be two readings of one segment.
fn decode_segment(seg: &str) -> Option<String> {
    let bytes = seg.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    percent_encoding::percent_decode_str(seg)
        .decode_utf8()
        .ok()
        .map(|s| s.into_owned())
}

/// A value that can only ever be a query *value*: everything but the
/// unreserved set is escaped, so an id cannot add a parameter or a path.
fn query_value(s: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
    const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    utf8_percent_encode(s, QUERY_VALUE).to_string()
}

/// The URL a guest socket op addresses: always inside the invocation's own
/// mount's reserved subtree. The mount base comes from host-side state;
/// `id`/`user` are only ever percent-encoded query values; and a `path` is
/// admitted segment by segment, so nothing the guest passes can steer these
/// `System` messages onto another mount or out of `/.sockets/`
/// (`router::validate_path` in dispatch is the second line of defence).
/// `extra` is a host-built query tail (`&code=…`), never guest text.
pub fn socket_control_url(
    base: &str,
    target: &serde_json::Value,
    extra: &str,
) -> Result<String, RsError> {
    use serde_json::Value;
    let scalar = |v: &Value| match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let root = format!(
        "{}/{}/",
        base.trim_end_matches('/'),
        crate::sockets::SOCKETS_SEGMENT
    );
    // A bare id addresses one socket, wherever on the mount it connected.
    if let Value::String(id) = target {
        return Ok(format!("{root}?$id={}{extra}", query_value(id)));
    }
    let field = |name: &str| target.get(name).filter(|v| !v.is_null());
    let mut rest = String::new();
    if let Some(path) = field("path") {
        let path = path
            .as_str()
            .ok_or_else(|| RsError::bad_request("sockets selector: 'path' must be a string"))?;
        let segments: Vec<&str> = path.split('/').filter(|seg| !seg.is_empty()).collect();
        for seg in &segments {
            let decoded = decode_segment(seg).ok_or_else(|| {
                RsError::bad_request("sockets selector: 'path' is not a valid URL path")
            })?;
            if decoded == "." || decoded == ".." || !plain_segment(&decoded) || !plain_segment(seg)
            {
                return Err(RsError::bad_request(
                    "sockets selector: 'path' must be a plain path relative to the mount",
                ));
            }
        }
        let subtree = field("subtree") == Some(&Value::Bool(true)) || path.ends_with('/');
        if !segments.is_empty() {
            rest = segments.join("/");
            if subtree {
                rest.push('/');
            }
        }
    }
    let mut query = String::new();
    if let Some(id) = field("id") {
        query.push_str(&format!("&$id={}", query_value(&scalar(id))));
    }
    if let Some(user) = field("user") {
        query.push_str(&format!("&$user={}", query_value(&scalar(user))));
    }
    query.push_str(extra);
    Ok(match query.strip_prefix('&') {
        Some(q) => format!("{root}{rest}?{q}"),
        None => format!("{root}{rest}"),
    })
}

/// Per-invocation limits (PRD §9.3), enforced by the engine + host.
#[derive(Debug, Clone)]
pub struct InvocationLimits {
    pub wall_clock: Duration,
    pub memory_bytes: u64,
    pub outbound_calls: u32,
    pub materialized_body_bytes: u64,
}

impl Default for InvocationLimits {
    fn default() -> Self {
        InvocationLimits {
            wall_clock: Duration::from_secs(30),
            memory_bytes: 128 * 1024 * 1024,
            outbound_calls: 64,
            materialized_body_bytes: 100 * 1024 * 1024,
        }
    }
}

pub type NativeHandler = Arc<
    dyn Fn(
            Message,
            serde_json::Value,
            Arc<dyn HostApi>,
        ) -> Pin<Box<dyn Future<Output = Result<Message, RsError>> + Send>>
        + Send
        + Sync,
>;

/// Deployed service code, dispatched to the engine that can run it.
#[derive(Clone)]
pub enum ServiceCode {
    /// A compiled Wasm component against the published WIT world.
    WasmComponent(Arc<Vec<u8>>),
    /// A bundled ESM module for the V8 isolate engine.
    JsBundle(Arc<String>),
    /// An in-process handler: the conformance-reference binding, also usable
    /// by embedders for trusted extensions.
    Native(NativeHandler),
}

/// An engine executes service code under the contract.
#[async_trait]
pub trait Engine: Send + Sync {
    async fn invoke(
        &self,
        code: &ServiceCode,
        msg: Message,
        config: &serde_json::Value,
        host: Arc<dyn HostApi>,
        limits: &InvocationLimits,
    ) -> Result<Message, RsError>;
}

/// Resolves a named capability grant to an executable target.
pub type CapabilityTarget = Arc<
    dyn Fn(Message) -> Pin<Box<dyn Future<Output = Result<Message, RsError>> + Send>> + Send + Sync,
>;

/// Identity stamped onto a sandbox service's own logs (`HostApi::log`). Built
/// per invocation — `GrantedHost` is constructed per call, where the trace and
/// mount are known — so each line carries the right span and the host-fixed
/// service identity (the author supplies only level + text, can't forge it).
pub struct LogContext {
    pub sink: Arc<dyn crate::logging::LogStore>,
    pub tenant: String,
    pub mount: String,
    pub service: String,
    pub trace_id: String,
    pub span_id: String,
}

/// The host side handed to engines: enforces capability grants (default
/// deny), the outbound-call budget, and trace propagation. State is
/// invocation-external (keyed per service instance), satisfying invariant 4.
pub struct GrantedHost {
    grants: HashMap<String, CapabilityTarget>,
    outbound_budget: u32,
    outbound_used: AtomicU32,
    state: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    service_name: String,
    /// Where this invocation's `log()` calls go; `None` drops them (tests and
    /// the default-deny baseline).
    log_ctx: Option<LogContext>,
    /// The mount whose `/.sockets/` subtree this invocation may address;
    /// `None` (no socket surface) unless the mount opted into `webSocket`.
    socket_mount: Option<SocketMount>,
}

impl GrantedHost {
    pub fn new(
        grants: HashMap<String, CapabilityTarget>,
        outbound_budget: u32,
        state: Arc<RwLock<HashMap<String, Vec<u8>>>>,
        service_name: &str,
    ) -> Self {
        GrantedHost {
            grants,
            outbound_budget,
            outbound_used: AtomicU32::new(0),
            state,
            service_name: service_name.to_string(),
            log_ctx: None,
            socket_mount: None,
        }
    }

    /// Give this invocation its own mount's socket surface. Only a `webSocket`
    /// mount has the subtree: on any other, `/.sockets/` is an ordinary path
    /// of the service itself, and the op would call straight back in.
    pub fn with_socket_mount(mut self, mount: SocketMount) -> Self {
        self.socket_mount = Some(mount);
        self
    }

    /// Account one outbound call — the budget, a child trace, depth + 1 —
    /// however its target was chosen: a named grant, or a target the host
    /// itself owns (the socket ops), which the guest cannot reach through
    /// `request` because it has no name.
    fn outbound(&self, mut msg: Message) -> Result<Message, RsError> {
        let used = self.outbound_used.fetch_add(1, Ordering::SeqCst) + 1;
        if used > self.outbound_budget {
            return Err(RsError::limit_exceeded(
                "outbound_calls",
                used,
                self.outbound_budget,
            ));
        }
        msg.trace = msg.trace.child();
        msg.depth = msg.depth.saturating_add(1);
        Ok(msg)
    }

    /// Route this invocation's sandbox logs to the node sink (PRD §14).
    pub fn with_log_context(mut self, ctx: LogContext) -> Self {
        self.log_ctx = Some(ctx);
        self
    }

    /// A host with no grants at all — the default-deny baseline.
    pub fn deny_all(service_name: &str) -> Self {
        Self::new(
            HashMap::new(),
            0,
            Arc::new(RwLock::new(HashMap::new())),
            service_name,
        )
    }
}

#[async_trait]
impl HostApi for GrantedHost {
    async fn request(&self, capability: &str, msg: Message) -> Result<Message, RsError> {
        let target = self
            .grants
            .get(capability)
            .ok_or_else(|| RsError::capability_denied(capability))?;
        target(self.outbound(msg)?).await
    }

    /// `System`-sourced — the guest is answering on its own mount, so the
    /// connected user need not hold that mount's write role — which is exactly
    /// why the URL is built entirely from host-side state
    /// ([`socket_control_url`]) and why this is **not** a general escalation:
    /// `request` above still runs as the caller. It is an ordinary host call
    /// otherwise: counted against the outbound budget, child trace, depth + 1.
    async fn socket_control(
        &self,
        op: SocketOp,
        target: &serde_json::Value,
        mut call: Message,
    ) -> Result<Message, RsError> {
        let mount = self
            .socket_mount
            .as_ref()
            .ok_or_else(|| RsError::capability_denied("socket"))?;
        let (method, extra, body, listed) = match op {
            SocketOp::Send(body) => (http::Method::POST, String::new(), Some(body), None),
            SocketOp::Close { code, reason } => {
                let mut extra = String::new();
                if let Some(code) = code {
                    extra.push_str(&format!("&code={code}"));
                }
                if let Some(reason) = reason {
                    extra.push_str(&format!("&reason={}", query_value(&reason)));
                }
                (http::Method::DELETE, extra, None, None)
            }
            // A listing is of a container: a selection with a `path` lists
            // that subtree (a bare id stays one socket on the whole mount).
            SocketOp::List => {
                let mut sel = target.clone();
                if let Some(obj) = sel.as_object_mut() {
                    obj.insert("subtree".into(), true.into());
                }
                (http::Method::GET, String::new(), None, Some(sel))
            }
        };
        let url = socket_control_url(&mount.base, listed.as_ref().unwrap_or(target), &extra)?;
        call.method = method;
        call.url = MsgUrl::parse(&url);
        call.source = Source::System;
        call.body = body;
        let call = self.outbound(call)?;
        Ok(mount.requester.request(call).await)
    }

    fn log(&self, level: LogLevel, text: &str) {
        // Sandbox `console.log` / Wasm `log()` land here, stamped with this
        // invocation's identity (PRD §14). `None` context drops them.
        let Some(c) = &self.log_ctx else { return };
        let trace = crate::message::TraceContext {
            trace_id: c.trace_id.clone(),
            span_id: c.span_id.clone(),
        };
        let rec = crate::logging::LogRecord::now(
            crate::logging::Severity::from_log_level(level),
            &c.tenant,
            &trace,
            text.to_string(),
        )
        .attr("rs2.mount", c.mount.as_str())
        .attr("rs2.service", c.service.as_str())
        .attr("rs2.source", "custom");
        c.sink.emit(rec);
    }

    async fn state_get(&self, key: &str) -> Option<Vec<u8>> {
        self.state
            .read()
            .await
            .get(&format!("{}:{key}", self.service_name))
            .cloned()
    }

    async fn state_put(&self, key: &str, value: Vec<u8>) {
        self.state
            .write()
            .await
            .insert(format!("{}:{key}", self.service_name), value);
    }
}

#[cfg(test)]
mod tests {
    use super::socket_control_url;
    use serde_json::json;

    #[test]
    fn socket_urls_stay_inside_the_mounts_subtree() {
        let url = |base: &str, target| socket_control_url(base, &target, "").unwrap();
        assert_eq!(url("/chat", json!({})), "/chat/.sockets/");
        assert_eq!(url("/", json!({})), "/.sockets/");
        assert_eq!(url("/chat/", json!(null)), "/chat/.sockets/");
        assert_eq!(url("/chat", json!("s 1")), "/chat/.sockets/?$id=s%201");
        assert_eq!(
            url("/chat", json!({ "path": "/room//42" })),
            "/chat/.sockets/room/42"
        );
        assert_eq!(
            url("/chat", json!({ "path": "room", "subtree": true })),
            "/chat/.sockets/room/"
        );
        assert_eq!(
            url(
                "/chat",
                json!({ "path": "room/", "id": "a&$id=b", "user": "u/../x" })
            ),
            "/chat/.sockets/room/?$id=a%26%24id%3Db&$user=u%2F..%2Fx"
        );
        // An encoded-but-plain segment is kept as the guest wrote it.
        assert_eq!(
            url("/chat", json!({ "path": "caf%C3%A9" })),
            "/chat/.sockets/caf%C3%A9"
        );
        assert_eq!(
            socket_control_url("/chat", &json!("s1"), "&code=4001").unwrap(),
            "/chat/.sockets/?$id=s1&code=4001"
        );
        assert_eq!(
            socket_control_url("/chat", &json!({}), "&code=4001").unwrap(),
            "/chat/.sockets/?code=4001"
        );
    }

    #[test]
    fn socket_paths_are_admitted_segment_by_segment() {
        for path in [
            "..",
            "../other",
            "a/../../x",
            "./a",
            "a/.",
            "x?$id=1",
            "x#y",
            "a\\b",
            "%2e%2e",
            "%2E%2e/other",
            "a%2fb",
            "a%5cb",
            "a%3fb",
            "a%23b",
            "a%00b",
            "a%0ab",
            "a\u{7f}b",
            "%",
            "%4",
            "%zz",
            "%ff",
        ] {
            let err = socket_control_url("/chat", &json!({ "path": path }), "").unwrap_err();
            assert_eq!(err.code, crate::error::codes::BAD_REQUEST, "path {path:?}");
        }
        let err = socket_control_url("/chat", &json!({ "path": 7 }), "").unwrap_err();
        assert_eq!(err.code, crate::error::codes::BAD_REQUEST);
    }
}
