//! Wasmtime component engine (PRD §5.3): runs services compiled to Wasm
//! components against the published WIT world (`wit/service.wit`).
//!
//! Limits: guests yield to the executor at every engine-wide epoch tick
//! (so CPU-bound code can't pin a worker thread), which lets the tokio
//! timeout enforce the wall-clock budget; linear memory capped per store via
//! `StoreLimits`. Compiled components are cached by content hash
//! (content-addressed deployment, PRD §14).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::contract::{Engine as RsEngine, HostApi, InvocationLimits, LogLevel, ServiceCode};
use crate::error::RsError;
use crate::message::{Body, MediaType, Message as RsMessage};

wasmtime::component::bindgen!({
    path: "wit",
    world: "service",
    imports: { default: async },
    exports: { default: async },
});

use rs2::service::types as wit_types;

struct HostState {
    api: Arc<dyn HostApi>,
    limits: InvocationLimits,
    template: RsMessage,
    store_limits: StoreLimits,
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

fn level_from_wit(level: wit_types::LogLevel) -> LogLevel {
    match level {
        wit_types::LogLevel::Debug => LogLevel::Debug,
        wit_types::LogLevel::Info => LogLevel::Info,
        wit_types::LogLevel::Warn => LogLevel::Warn,
        wit_types::LogLevel::Error => LogLevel::Error,
    }
}

async fn message_to_wit(msg: &mut RsMessage, materialize_cap: u64) -> Result<wit_types::Message, RsError> {
    let body = match &mut msg.body {
        None => None,
        Some(b) => {
            let bytes = b.materialize(materialize_cap).await?.to_vec();
            Some(wit_types::BodyData {
                media_type: b.media_type.essence().to_string(),
                schema_ref: b.media_type.schema().map(|s| s.to_string()),
                bytes,
            })
        }
    };
    Ok(wit_types::Message {
        method: msg.method.as_str().to_string(),
        url: format!(
            "{}{}",
            msg.url.path,
            if msg.url.query.is_empty() { String::new() } else { format!("?{}", msg.url.query) }
        ),
        headers: msg
            .headers
            .iter()
            .map(|(k, v)| wit_types::Header {
                name: k.as_str().to_string(),
                value: String::from_utf8_lossy(v.as_bytes()).to_string(),
            })
            .collect(),
        status: msg.status.map(|s| s.as_u16()).unwrap_or(0),
        body,
    })
}

fn message_from_wit(template: &RsMessage, wit: wit_types::Message) -> RsMessage {
    let status = http::StatusCode::from_u16(if wit.status == 0 { 200 } else { wit.status })
        .unwrap_or(http::StatusCode::OK);
    let body = wit.body.map(|b| {
        let mut mt = MediaType::new(&b.media_type);
        if let Some(s) = b.schema_ref {
            mt = mt.with_schema(s);
        }
        Body::from_bytes(b.bytes, mt)
    });
    let mut resp = template.response(status, body);
    for h in wit.headers {
        if let (Ok(name), Ok(value)) = (
            http::header::HeaderName::from_bytes(h.name.as_bytes()),
            http::HeaderValue::from_str(&h.value),
        ) {
            resp.headers.insert(name, value);
        }
    }
    resp
}

impl rs2::service::host::Host for HostState {
    async fn request(
        &mut self,
        capability: String,
        msg: wit_types::Message,
    ) -> Result<wit_types::Message, wit_types::HostError> {
        let mut out = self.template.response(http::StatusCode::OK, None);
        out.status = None;
        out.method = msg.method.parse().unwrap_or(http::Method::GET);
        out.url = crate::message::MsgUrl::parse(&msg.url);
        for h in &msg.headers {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::from_bytes(h.name.as_bytes()),
                http::HeaderValue::from_str(&h.value),
            ) {
                out.headers.insert(name, value);
            }
        }
        if let Some(b) = msg.body {
            let mut mt = MediaType::new(&b.media_type);
            if let Some(s) = b.schema_ref {
                mt = mt.with_schema(s);
            }
            out.body = Some(Body::from_bytes(b.bytes, mt));
        }
        let mut result = self.api.request(&capability, out).await.map_err(|e| match e.code {
            crate::error::codes::CAPABILITY_DENIED => wit_types::HostError::CapabilityDenied(e.detail),
            crate::error::codes::LIMIT_EXCEEDED => wit_types::HostError::LimitExceeded(e.detail),
            _ => wit_types::HostError::Failed(e.detail),
        })?;
        message_to_wit(&mut result, self.limits.materialized_body_bytes)
            .await
            .map_err(|e| wit_types::HostError::Failed(e.detail))
    }

    async fn log(&mut self, level: wit_types::LogLevel, text: String) {
        self.api.log(level_from_wit(level), &text);
    }

    async fn state_get(&mut self, key: String) -> Option<Vec<u8>> {
        self.api.state_get(&key).await
    }

    async fn state_put(&mut self, key: String, value: Vec<u8>) {
        self.api.state_put(&key, value).await;
    }
}

/// Period of the engine-wide epoch ticker. The epoch counter is shared by
/// every in-flight store on the engine, so per-invocation deadlines must be
/// expressed in ticks of a fixed clock — a per-invocation "increment once at
/// my deadline" would trip every *other* store's deadline too.
const EPOCH_TICK: std::time::Duration = std::time::Duration::from_millis(10);

pub struct WasmEngine {
    engine: wasmtime::Engine,
    compiled: tokio::sync::RwLock<HashMap<u64, Component>>,
    ticker_stop: Arc<std::sync::atomic::AtomicBool>,
}

impl WasmEngine {
    pub fn new() -> Result<Self, RsError> {
        let mut config = Config::new();
        config.async_support(true);
        config.epoch_interruption(true);
        config.wasm_component_model(true);
        let engine = wasmtime::Engine::new(&config)
            .map_err(|e| RsError::internal(format!("wasmtime engine init failed: {e}")))?;
        // One dedicated ticker thread per engine: a tokio timer can be starved
        // by the very guests the ticks are meant to preempt. Stopped via flag
        // on drop (tenant rebuilds drop the engine with its CodeService).
        let ticker_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let engine = engine.clone();
            let stop = ticker_stop.clone();
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(EPOCH_TICK);
                    engine.increment_epoch();
                }
            });
        }
        Ok(WasmEngine { engine, compiled: tokio::sync::RwLock::new(HashMap::new()), ticker_stop })
    }

    /// Compile-only validation: the deployment-time smoke test for
    /// `PUT /code/<name>` (PRD §10.6).
    pub fn compile_check(&self, bytes: &[u8]) -> Result<(), RsError> {
        Component::new(&self.engine, bytes)
            .map(|_| ())
            .map_err(|e| RsError::contract_violation(format!("component failed to compile: {e}")))
    }

    async fn component_for(&self, bytes: &[u8]) -> Result<Component, RsError> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut hasher);
        let key = hasher.finish();
        if let Some(c) = self.compiled.read().await.get(&key) {
            return Ok(c.clone());
        }
        let component = Component::new(&self.engine, bytes)
            .map_err(|e| RsError::contract_violation(format!("component failed to compile: {e}")))?;
        self.compiled.write().await.insert(key, component.clone());
        Ok(component)
    }
}

impl Drop for WasmEngine {
    fn drop(&mut self) {
        self.ticker_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[async_trait]
impl RsEngine for WasmEngine {
    async fn invoke(
        &self,
        code: &ServiceCode,
        mut msg: RsMessage,
        config: &serde_json::Value,
        host: Arc<dyn HostApi>,
        limits: &InvocationLimits,
    ) -> Result<RsMessage, RsError> {
        let bytes = match code {
            ServiceCode::WasmComponent(b) => b.clone(),
            _ => return Err(RsError::engine_unavailable("wasm engine only runs wasm components")),
        };
        let component = self.component_for(&bytes).await?;

        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|e| RsError::internal(format!("wasi linker: {e}")))?;
        rs2::service::host::add_to_linker::<_, HasSelf<HostState>>(&mut linker, |s: &mut HostState| s)
            .map_err(|e| RsError::internal(format!("host linker: {e}")))?;

        let template = msg.response(http::StatusCode::OK, None);
        let wit_msg = message_to_wit(&mut msg, limits.materialized_body_bytes).await?;

        let state = HostState {
            api: host,
            limits: limits.clone(),
            template: template.response(http::StatusCode::OK, None),
            store_limits: StoreLimitsBuilder::new()
                .memory_size(limits.memory_bytes as usize)
                .build(),
            wasi: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
        };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.store_limits);
        // Yield at every epoch tick rather than trap: the guest can't pin an
        // executor thread between ticks, which guarantees the tokio timeout
        // below a chance to fire — dropping the store cancels the guest.
        store.epoch_deadline_async_yield_and_update(1);

        let result = tokio::time::timeout(limits.wall_clock, async {
            let instance = Service::instantiate_async(&mut store, &component, &linker)
                .await
                .map_err(|e| RsError::contract_violation(format!("instantiation failed: {e}")))?;
            instance
                .call_handle(&mut store, &wit_msg, &config.to_string())
                .await
                .map_err(|e| RsError::contract_violation(format!("trap: {e}")))?
                .map_err(RsError::contract_violation)
        })
        .await;

        match result {
            Ok(Ok(wit_out)) => Ok(message_from_wit(&template, wit_out)),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(RsError::limit_exceeded(
                "wall_clock_ms",
                limits.wall_clock.as_millis() as u64,
                limits.wall_clock.as_millis() as u64,
            )),
        }
    }
}
