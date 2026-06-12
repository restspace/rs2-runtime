//! Tenant (PRD §4, §9.1): an isolation domain — its own mounts, config,
//! storage namespace, and resource quotas. Built atomically from a
//! [`TenantConfig`]; a future config change builds a new `Tenant` and swaps.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::capabilities::{DataStore, FileStore, ScopedDataStore, ScopedFileStore};
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
}

impl Adapters {
    pub fn new(files: Arc<dyn FileStore>, data: Arc<dyn DataStore>) -> Self {
        Adapters {
            files,
            query: Arc::new(crate::adapters::MemQueryStore::new(data.clone())),
            data,
            idempotency: Arc::new(crate::idempotency::MemIdempotencyStore::default()),
            http: None,
        }
    }

    pub fn with_http(mut self, http: Arc<dyn crate::capabilities::HttpOut>) -> Self {
        self.http = Some(http);
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
            // Capability grants: each instance gets handles pre-scoped to
            // this tenant — host-enforced isolation (PRD §9.2).
            let ctx = ServiceContext {
                config: mount.config.clone(),
                files: Some(ScopedFileStore::new(adapters.files.clone(), name)),
                data: Some(ScopedDataStore::new(adapters.data.clone(), name)),
                query: Some(crate::capabilities::ScopedQueryStore::new(
                    adapters.query.clone(),
                    name,
                )),
                http: adapters.http.clone(),
                cors: Arc::new(crate::wrapper::CorsPolicy::from_config(config.cors.as_ref())),
                limits: limits.invocation_limits(),
                requester: requester.clone(),
                control: control.clone(),
                tenant_retry: config.retry.clone(),
                pipeline_wall_clock: limits.wall_clock_pipeline,
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
