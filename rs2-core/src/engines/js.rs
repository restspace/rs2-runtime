//! V8 isolate engine (PRD §5.3) — M1 skeleton.
//!
//! The full engine (one isolate per service version, per-invocation
//! contexts, warm pools, snapshot-amortized cold starts, Node-compat layer
//! scoped to the npm API-wrapper corpus) embeds `rusty_v8` and is tracked as
//! the remaining M1 engine work. The contract surface it must satisfy is
//! already pinned by the conformance suite (`tests/conformance.rs`), which
//! runs engine-neutrally — implementing [`Engine`] here and passing that
//! suite is the definition of done.

use std::sync::Arc;

use async_trait::async_trait;

use crate::contract::{Engine, HostApi, InvocationLimits, ServiceCode};
use crate::error::RsError;
use crate::message::Message;

#[derive(Default)]
pub struct JsEngine;

impl JsEngine {
    pub fn new() -> Self {
        JsEngine
    }
}

#[async_trait]
impl Engine for JsEngine {
    async fn invoke(
        &self,
        code: &ServiceCode,
        _msg: Message,
        _config: &serde_json::Value,
        _host: Arc<dyn HostApi>,
        _limits: &InvocationLimits,
    ) -> Result<Message, RsError> {
        match code {
            ServiceCode::JsBundle(_) => Err(RsError::engine_unavailable(
                "the V8 isolate engine is not yet implemented; deploy a Wasm component or enable a native handler",
            )),
            _ => Err(RsError::engine_unavailable("js engine only runs js bundles")),
        }
    }
}
