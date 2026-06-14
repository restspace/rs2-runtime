//! Tenant (PRD §4, §9.1): an isolation domain — its own mounts, config,
//! storage namespace, and resource quotas. Built atomically from a
//! [`TenantConfig`]; a future config change builds a new `Tenant` and swaps.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::capabilities::{DataStore, FileStore, ScopedDataStore, ScopedFileStore, ScopedQueryStore};
use crate::error::RsError;
use crate::router::{Mount, MountTable};
use crate::services::{DataService, FileService, Service, ServiceContext};
use crate::wrapper::LimitTable;

/// Successor to Restspace's `services.json` (PRD §13), v1 subset.
#[derive(Debug, Clone, Deserialize)]
pub struct TenantConfig {
    pub mounts: Vec<MountSpec>,
    /// Tenant-level retry default (PRD §7.3 resolution chain).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryPolicy>,
    /// Auth settings consumed by the `auth` service and the RBAC wrapper.
    #[serde(default)]
    pub auth: Option<serde_json::Value>,
    /// CORS policy (PRD §5.2: an external-only wrapper concern).
    #[serde(default)]
    pub cors: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MountSpec {
    /// URL path prefix, e.g. `/files`.
    pub path: String,
    /// Service reference: prebuilt name (`file`, `data`) or `code:<name>@<version>`.
    pub service: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Shared adapter wiring handed to tenants. Embedders replace these with
/// their own trait implementations (PRD §3, P2).
#[derive(Clone)]
pub struct Adapters {
    pub files: Arc<dyn FileStore>,
    pub data: Arc<dyn DataStore>,
    /// Idempotency store (PRD §7.2). The default in-memory adapter is
    /// single-node; supply a shared adapter for scale-out.
    pub idempotency: Arc<dyn crate::idempotency::IdempotencyStore>,
    /// Query store (PRD §10.4). Defaults to the reference adapter scanning
    /// the data store; SQL adapters push queries down.
    pub query: Arc<dyn crate::capabilities::QueryStore>,
    /// Outbound HTTP (PRD §9.2): granted per mount with allowed-host
    /// patterns; `None` disables external calls entirely.
    pub http: Option<Arc<dyn crate::capabilities::HttpOut>>,
    /// Log sink (PRD §14). Defaults to a no-op; the server swaps in a
    /// `FileLogStore`. Both the sink and its level floor are node infra
    /// (operator config), so they ride on `Adapters`.
    pub log: Arc<dyn crate::logging::LogStore>,
    /// Boundary-log severity floor; failures (Err arm / 5xx) bypass it.
    pub log_level: crate::logging::Severity,
}

impl Adapters {
    pub fn new(files: Arc<dyn FileStore>, data: Arc<dyn DataStore>) -> Self {
        Adapters {
            files,
            query: Arc::new(crate::adapters::MemQueryStore::new(data.clone())),
            data,
            idempotency: Arc::new(crate::idempotency::MemIdempotencyStore::default()),
            http: None,
            log: Arc::new(crate::logging::NullLogStore),
            log_level: crate::logging::Severity::Info,
        }
    }

    pub fn with_http(mut self, http: Arc<dyn crate::capabilities::HttpOut>) -> Self {
        self.http = Some(http);
        self
    }

    /// Install a log sink and the boundary-log severity floor.
    pub fn with_logging(
        mut self,
        log: Arc<dyn crate::logging::LogStore>,
        level: crate::logging::Severity,
    ) -> Self {
        self.log = log;
        self.log_level = level;
        self
    }
}

pub struct Tenant {
    pub name: String,
    pub mounts: MountTable,
    /// Tenant auth settings (signing key etc.) for the RBAC wrapper.
    pub auth: Option<serde_json::Value>,
    /// Host-enforced CORS policy.
    pub cors: Arc<crate::wrapper::CorsPolicy>,
    /// Service instance + granted context per mount base path.
    instances: HashMap<String, (Arc<dyn Service>, Arc<ServiceContext>)>,
}

impl Tenant {
    /// Validate config and build all service instances. Fails as a whole on
    /// any invalid mount — an invalid config never half-applies (PRD §10.6).
    pub fn build(
        name: &str,
        config: TenantConfig,
        adapters: &Adapters,
        limits: &LimitTable,
        requester: Option<Arc<dyn crate::pipeline::Requester>>,
        control: Option<Arc<dyn crate::runtime::TenantControl>>,
    ) -> Result<Self, RsError> {
        let mounts = MountTable::new(
            config
                .mounts
                .iter()
                .map(|m| Mount {
                    base_path: m.path.clone(),
                    service: m.service.clone(),
                    config: m.config.clone(),
                })
                .collect(),
        )?;
        let mut instances = HashMap::new();
        for mount in mounts.mounts() {
            let service: Arc<dyn Service> = match mount.service.as_str() {
                "file" => Arc::new(FileService::new()),
                "data" => Arc::new(DataService::new()),
                "pipeline" => {
                    let root = crate::services::spec_store::store_root(
                        crate::services::PIPELINE_PREFIX,
                        &mount.base_path,
                        &mount.config,
                    );
                    let store = crate::services::spec_store::SpecStore::new(
                        adapters.files.clone(),
                        name,
                        &root,
                        crate::services::PIPELINE_SUBTREE,
                        limits.invocation_limits(),
                        crate::services::PipelineService::validator(),
                    );
                    Arc::new(crate::services::PipelineService::from_config(&mount.config, store)?)
                }
                "query" => {
                    let root = crate::services::spec_store::store_root(
                        crate::services::QUERY_PREFIX,
                        &mount.base_path,
                        &mount.config,
                    );
                    let store = crate::services::spec_store::SpecStore::new(
                        adapters.files.clone(),
                        name,
                        &root,
                        crate::services::QUERY_SUBTREE,
                        limits.invocation_limits(),
                        crate::services::QueryService::validator(),
                    );
                    Arc::new(crate::services::QueryService::from_config(&mount.config, store)?)
                }
                "auth" => Arc::new(crate::services::AuthService::from_config(
                    &mount.config,
                    config.auth.as_ref(),
                )?),
                "services" => Arc::new(crate::services::ServicesService::new()),
                "log" => Arc::new(crate::services::LogReaderService::new()),
                code_ref if code_ref.starts_with("code:") => {
                    Arc::new(crate::services::CodeService::from_ref(code_ref)?)
                }
                other => {
                    return Err(RsError::bad_request(format!(
                        "unknown service '{other}' at mount '{}' (custom `code:` services arrive with the self-config API)",
                        if mount.base_path.is_empty() { "/" } else { &mount.base_path }
                    )))
                }
            };
            // The `data`/`query` capabilities are normally the node's built-in
            // adapters; a `data`/`query` mount may instead name a loadable
            // adapter (`"store": {"adapter":"code:…"}`, G13 Phase 2/3) backed by
            // a resident JS bundle. The stock service runs unchanged on either.
            let files = file_capability(mount, adapters, name, limits.invocation_limits())?;
            let data = data_capability(mount, adapters, name, limits.invocation_limits())?;
            let query = query_capability(mount, adapters, name, limits.invocation_limits())?;

            // Capability grants: each instance gets handles pre-scoped to
            // this tenant — host-enforced isolation (PRD §9.2).
            let ctx = ServiceContext {
                config: mount.config.clone(),
                files,
                data,
                query,
                http: adapters.http.clone(),
                cors: Arc::new(crate::wrapper::CorsPolicy::from_config(config.cors.as_ref())),
                limits: limits.invocation_limits(),
                requester: requester.clone(),
                control: control.clone(),
                tenant_retry: config.retry.clone(),
                pipeline_wall_clock: limits.wall_clock_pipeline,
                logger: crate::logging::ServiceLogger::new(
                    adapters.log.clone(),
                    name,
                    &mount.base_path,
                    &mount.service,
                ),
                // The log reader alone gets read-back; every other mount sees
                // `None` (default-deny).
                log_store: if mount.service == "log" {
                    Some(adapters.log.clone())
                } else {
                    None
                },
            };
            instances.insert(mount.base_path.clone(), (service, Arc::new(ctx)));
        }
        Ok(Tenant {
            name: name.to_string(),
            mounts,
            instances,
            auth: config.auth.clone(),
            cors: Arc::new(crate::wrapper::CorsPolicy::from_config(config.cors.as_ref())),
        })
    }

    pub fn instance(&self, base_path: &str) -> Option<&(Arc<dyn Service>, Arc<ServiceContext>)> {
        self.instances.get(base_path)
    }
}

/// Resolve a mount's `data` capability. Every mount sees the node's built-in
/// store by default; a `data` mount with `"store": {"adapter": "code:…"}` is
/// instead backed by a resident loadable adapter (G13 Phase 2), scoped to this
/// tenant. The bundle is loaded lazily on first use from the tenant file store.
fn data_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Option<ScopedDataStore>, RsError> {
    let adapter_ref = (mount.service == "data")
        .then(|| mount.config.get("store").and_then(|s| s.get("adapter")).and_then(|a| a.as_str()))
        .flatten();
    let Some(adapter_ref) = adapter_ref else {
        return Ok(Some(ScopedDataStore::new(adapters.data.clone(), name)));
    };

    #[cfg(feature = "js")]
    {
        let store = mount.config.get("store").cloned().unwrap_or_else(|| serde_json::json!({}));
        let files = ScopedFileStore::new(adapters.files.clone(), name);
        let guest = crate::engines::resident::GuestDataStore::from_config(
            adapter_ref, &store, files, name, limits,
        )?;
        Ok(Some(ScopedDataStore::new(Arc::new(guest), name)))
    }
    #[cfg(not(feature = "js"))]
    {
        let _ = limits;
        Err(RsError::engine_unavailable(format!(
            "data mount '{}' uses a loadable adapter ('{adapter_ref}') but this build has no JS \
             engine (rebuild with --features js)",
            if mount.base_path.is_empty() { "/" } else { &mount.base_path }
        )))
    }
}

/// Resolve a mount's `files` capability. Every mount sees the node's built-in
/// file store by default; a `file` mount with `"store": {"adapter": "code:…"}`
/// is instead backed by a resident loadable adapter (G13 Phase 3). The bundle
/// itself is still loaded from the built-in store, so there is no circularity.
fn file_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Option<ScopedFileStore>, RsError> {
    let adapter_ref = (mount.service == "file")
        .then(|| mount.config.get("store").and_then(|s| s.get("adapter")).and_then(|a| a.as_str()))
        .flatten();
    let Some(adapter_ref) = adapter_ref else {
        return Ok(Some(ScopedFileStore::new(adapters.files.clone(), name)));
    };

    #[cfg(feature = "js")]
    {
        let store = mount.config.get("store").cloned().unwrap_or_else(|| serde_json::json!({}));
        let loader = ScopedFileStore::new(adapters.files.clone(), name);
        let guest = crate::engines::resident::GuestFileStore::from_config(
            adapter_ref, &store, loader, name, limits,
        )?;
        Ok(Some(ScopedFileStore::new(Arc::new(guest), name)))
    }
    #[cfg(not(feature = "js"))]
    {
        let _ = limits;
        Err(RsError::engine_unavailable(format!(
            "file mount '{}' uses a loadable adapter ('{adapter_ref}') but this build has no JS \
             engine (rebuild with --features js)",
            if mount.base_path.is_empty() { "/" } else { &mount.base_path }
        )))
    }
}

/// Resolve a mount's `query` capability. Every mount sees the node's built-in
/// `QueryStore` by default; a `query` mount with `"store": {"adapter": "code:…"}`
/// is instead backed by a resident loadable adapter (G13 Phase 3) that executes
/// stored queries against its own backend. (`store.root` still relocates the
/// authoring subtree — the two `store` keys are independent.)
fn query_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Option<ScopedQueryStore>, RsError> {
    let adapter_ref = (mount.service == "query")
        .then(|| mount.config.get("store").and_then(|s| s.get("adapter")).and_then(|a| a.as_str()))
        .flatten();
    let Some(adapter_ref) = adapter_ref else {
        return Ok(Some(ScopedQueryStore::new(adapters.query.clone(), name)));
    };

    #[cfg(feature = "js")]
    {
        let store = mount.config.get("store").cloned().unwrap_or_else(|| serde_json::json!({}));
        let files = ScopedFileStore::new(adapters.files.clone(), name);
        let guest = crate::engines::resident::GuestQueryStore::from_config(
            adapter_ref, &store, files, name, limits,
        )?;
        Ok(Some(ScopedQueryStore::new(Arc::new(guest), name)))
    }
    #[cfg(not(feature = "js"))]
    {
        let _ = limits;
        Err(RsError::engine_unavailable(format!(
            "query mount '{}' uses a loadable adapter ('{adapter_ref}') but this build has no JS \
             engine (rebuild with --features js)",
            if mount.base_path.is_empty() { "/" } else { &mount.base_path }
        )))
    }
}
