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
use crate::message::Message;

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
        }
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
        let used = self.outbound_used.fetch_add(1, Ordering::SeqCst) + 1;
        if used > self.outbound_budget {
            return Err(RsError::limit_exceeded(
                "outbound_calls",
                used,
                self.outbound_budget,
            ));
        }
        let mut msg = msg;
        msg.trace = msg.trace.child();
        msg.depth = msg.depth.saturating_add(1);
        target(msg).await
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
