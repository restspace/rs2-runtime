//! Prebuilt services (PRD §5.5): host-native Rust, trusted runtime
//! components running in-process. They consume the same capability traits
//! the sandbox host exposes, so swapping adapters affects prebuilt and
//! custom services identically.

pub mod auth;
pub mod code;
mod data;
mod file;
mod pipeline_service;
mod query;
pub mod query_template;
mod services_config;
pub mod spec_store;

pub use auth::AuthService;
pub use code::CodeService;
pub use data::DataService;
pub use file::FileService;
pub use pipeline_service::{PipelineService, PIPELINE_PREFIX, PIPELINE_SUBTREE};
pub use query::{QueryService, QUERY_PREFIX, QUERY_SUBTREE};
pub use services_config::ServicesService;

use std::sync::Arc;

use async_trait::async_trait;

use crate::capabilities::{ScopedDataStore, ScopedFileStore, ScopedQueryStore};
use crate::contract::InvocationLimits;
use crate::error::RsError;
use crate::message::Message;
use crate::pipeline::Requester;
use crate::retry::RetryPolicy;

/// What a service instance was granted at mount time.
pub struct ServiceContext {
    pub config: serde_json::Value,
    pub files: Option<ScopedFileStore>,
    pub data: Option<ScopedDataStore>,
    pub query: Option<ScopedQueryStore>,
    /// Outbound HTTP adapter; grants scope it per mount (PRD §9.2).
    pub http: Option<Arc<dyn crate::capabilities::HttpOut>>,
    pub limits: InvocationLimits,
    /// Internal dispatch capability (pipelines and composition).
    pub requester: Option<Arc<dyn Requester>>,
    /// Control-plane capability (`services` self-config only).
    pub control: Option<Arc<dyn crate::runtime::TenantControl>>,
    /// Tenant CORS policy (the auth service consults it for cookie
    /// attributes; enforcement is the host's).
    pub cors: Arc<crate::wrapper::CorsPolicy>,
    /// Tenant-level retry default (PRD §7.3 resolution chain).
    pub tenant_retry: Option<RetryPolicy>,
    /// Pipeline wall-clock budget (PRD §9.3: 120 s default).
    pub pipeline_wall_clock: std::time::Duration,
}

/// A unit of behavior handling messages at a mount: Message → Message.
#[async_trait]
pub trait Service: Send + Sync {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError>;
}

/// Parse `$take`/`$skip` pagination query params with bounded defaults.
pub(crate) fn pagination(msg: &Message) -> (usize, usize) {
    let take = msg
        .url
        .query_param("$take")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(10_000);
    let skip = msg.url.query_param("$skip").and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
    (take, skip)
}
