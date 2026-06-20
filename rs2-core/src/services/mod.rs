//! Prebuilt services (PRD §5.5): host-native Rust, trusted runtime
//! components running in-process. They consume the same capability traits
//! the sandbox host exposes, so swapping adapters affects prebuilt and
//! custom services identically.

pub mod auth;
pub mod catalogue;
pub mod code;
mod data;
mod file;
mod log_reader;
mod pipeline_service;
mod query;
pub mod query_template;
mod services_config;
pub mod spec_store;
#[cfg(feature = "js")]
mod template;
mod wrapper_service;

pub use auth::AuthService;
pub use code::CodeService;
pub use data::DataService;
pub use file::FileService;
pub use log_reader::LogReaderService;
pub use pipeline_service::{PipelineService, PIPELINE_PREFIX, PIPELINE_SUBTREE};
pub use query::{QueryService, QUERY_PREFIX, QUERY_SUBTREE};
pub use services_config::ServicesService;
#[cfg(feature = "js")]
pub use template::{TemplateService, TEMPLATE_PREFIX, TEMPLATE_SUBTREE};
pub use wrapper_service::WrapperService;

use std::sync::Arc;

use async_trait::async_trait;

use crate::capabilities::{ScopedDataStore, ScopedFileStore, ScopedQueryStore, WritePrecondition};
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
    /// Host-applied response caching policy, parsed once at tenant build.
    pub cache_policy: crate::wrapper::CachePolicy,
    pub cache_openly_readable: bool,
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
    /// Tenant operator roles (`operatorRoles`) — the principals permitted to
    /// change authorization config. Consulted where a service guards
    /// authority-defining writes (e.g. the `data` service gates `.schema.json`
    /// writes under `fieldLevelAuthz`). Empty/None ⇒ no API operator.
    pub operator_roles: Option<String>,
    /// Pipeline wall-clock budget (PRD §9.3: 120 s default).
    pub pipeline_wall_clock: std::time::Duration,
    /// Application-log emit handle (PRD §14), bound to this mount's
    /// tenant/mount/service. Always present (a no-op when the node sink is
    /// Null); not a grant — the host stamps identity, so a service can only
    /// write its own stamped lines.
    pub logger: crate::logging::ServiceLogger,
    /// Log read-back capability — granted only to `log` reader mounts
    /// (`None` elsewhere). Reading logs back is sensitive; writing is not.
    pub log_store: Option<Arc<dyn crate::logging::LogStore>>,
    /// External-catalogue fetch capability — granted only to the `services`
    /// self-config mount (`None` elsewhere). Bounded by the operator
    /// host-allowlist; `None` also when no allowlist is configured.
    pub catalogue: Option<Arc<dyn catalogue::CatalogueClient>>,
    /// Snapshot of the node's built-in adapter registry — granted only to the
    /// `services` mount, so it can list selectable built-in adapters.
    pub builtin_adapters: Option<crate::adapters::BuiltinRegistry>,
    /// Tenant secrets this mount is granted (resolved from the mount's `secrets`
    /// grant against the tenant `secrets` block; default-deny). Host-side only —
    /// never exposed to sandboxed guests. The `pipeline` service binds these as
    /// `$<name>` variables for inline `$hmacVerify`.
    pub secrets: Option<serde_json::Map<String, serde_json::Value>>,
}

/// A unit of behavior handling messages at a mount: Message → Message.
#[async_trait]
pub trait Service: Send + Sync {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError>;
}

/// Whether an `If-None-Match` header value matches the resource's ETag
/// (list-aware; weak validators compare by value; `*` matches anything).
pub(crate) fn if_none_match_hits(if_none_match: Option<&str>, etag: &str) -> bool {
    let Some(inm) = if_none_match else { return false };
    inm.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || candidate.trim_start_matches("W/") == etag
    })
}

/// Parse a store write's conditional headers into a [`WritePrecondition`].
/// `If-None-Match: *` (create-only) takes precedence over `If-Match`.
pub(crate) fn write_precondition(msg: &Message) -> WritePrecondition {
    if let Some(inm) = msg.header("if-none-match") {
        if inm.split(',').map(str::trim).any(|c| c == "*") {
            return WritePrecondition::IfNoneMatchStar;
        }
    }
    match msg.header("if-match") {
        Some(im) => WritePrecondition::IfMatch(im.to_string()),
        None => WritePrecondition::None,
    }
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
