//! Native reference engine: runs `ServiceCode::Native` handlers in-process.
//!
//! This is **not** a sandbox — it exists as the reference implementation of
//! the contract semantics (limits, capability denial via [`GrantedHost`],
//! error mapping) that the conformance suite pins down, and as the binding
//! embedders use for trusted in-process extensions.

use std::sync::Arc;

use async_trait::async_trait;

use crate::contract::{Engine, HostApi, InvocationLimits, ServiceCode};
use crate::error::RsError;
use crate::message::Message;

#[derive(Default)]
pub struct NativeEngine;

impl NativeEngine {
    pub fn new() -> Self {
        NativeEngine
    }
}

#[async_trait]
impl Engine for NativeEngine {
    async fn invoke(
        &self,
        code: &ServiceCode,
        msg: Message,
        config: &serde_json::Value,
        host: Arc<dyn HostApi>,
        limits: &InvocationLimits,
    ) -> Result<Message, RsError> {
        let handler = match code {
            ServiceCode::Native(h) => h.clone(),
            _ => return Err(RsError::engine_unavailable("native engine only runs native handlers")),
        };
        let fut = handler(msg, config.clone(), host);
        match tokio::time::timeout(limits.wall_clock, fut).await {
            Ok(result) => result,
            Err(_) => Err(RsError::limit_exceeded(
                "wall_clock_ms",
                limits.wall_clock.as_millis() as u64,
                limits.wall_clock.as_millis() as u64,
            )),
        }
    }
}
