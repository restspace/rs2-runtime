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
    /// Tenant **operator** roles (PRD §5.2): space-separated role names that
    /// confer operator status. Operators are the only principals permitted to
    /// change authorization — a mount's or spec's `access`, and role
    /// assignment. Distinct from the *installer* (server/host config, quotas,
    /// tenancy), which is file/deploy-rooted and outside any tenant config.
    /// Absent ⇒ no API operator (authorization config is file-only). The first
    /// operator is bootstrapped here by the installer (it can't come from the
    /// API).
    #[serde(default, rename = "operatorRoles")]
    pub operator_roles: Option<String>,
    /// Tenant-level retry default (PRD §7.3 resolution chain).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryPolicy>,
    /// Auth settings consumed by the `auth` service and the RBAC wrapper.
    #[serde(default)]
    pub auth: Option<serde_json::Value>,
    /// CORS policy (PRD §5.2: an external-only wrapper concern).
    #[serde(default)]
    pub cors: Option<serde_json::Value>,
    /// External catalogues the tenant has registered (`name` → document `url`).
    /// The `services` API lists their items and installs from them; the host
    /// only fetches from operator-allowlisted hosts.
    #[serde(default)]
    pub catalogues: Vec<crate::config_schema::CatalogueRef>,
    /// Named tenant secrets (e.g. webhook signing keys). Write-only through the
    /// self-config API (redacted on `GET /raw`); a mount opts into specific
    /// names via its `secrets` grant, the host binds them host-side.
    #[serde(default)]
    pub secrets: Option<serde_json::Value>,
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
    /// Built-in adapters selectable per mount via `store.adapter =
    /// "builtin:<name>"` (the Rust-side sibling of `code:` loadable adapters).
    /// Seeded with the node's own built-ins; absent `adapter` still uses the
    /// `data`/`files`/`query` defaults above directly.
    pub builtins: crate::adapters::BuiltinRegistry,
    /// Host-side external-catalogue fetch client, bounded by the operator
    /// host-allowlist. `None` when no allowlist is configured (feature off).
    pub catalogue: Option<Arc<dyn crate::services::catalogue::CatalogueClient>>,
    /// Scheduler coordination store (fire-once across a cluster). The default
    /// in-memory adapter is single-node; a shared adapter (Redis) makes
    /// scheduled invocation HA-correct.
    pub schedule: Arc<dyn crate::scheduler::ScheduleStore>,
}

impl Adapters {
    pub fn new(files: Arc<dyn FileStore>, data: Arc<dyn DataStore>) -> Self {
        let query: Arc<dyn crate::capabilities::QueryStore> =
            Arc::new(crate::adapters::MemQueryStore::new(data.clone()));

        // Seed the node built-ins under their canonical names.
        //
        // `local` hands back the node file store itself (a `file` mount's
        // default *is* the local filesystem), so `builtin:local` shares it.
        // `mem`, by contrast, is its own dedicated in-memory store: the node
        // default data adapter may be something else entirely (the server
        // defaults it to file-backed), so `builtin:mem` must always mean an
        // in-memory store, not "whatever the default is". `reference` rebuilds
        // the scanning query adapter over the default data store. Factories
        // ignore their config for now.
        let mut builtins = crate::adapters::BuiltinRegistry::default();
        builtins.register_data("mem", {
            let mem: Arc<dyn DataStore> = Arc::new(crate::adapters::MemDataStore::new());
            Arc::new(move |_cfg: &serde_json::Value| Ok(mem.clone()))
        });
        builtins.register_files("local", {
            let files = files.clone();
            Arc::new(move |_cfg: &serde_json::Value| Ok(files.clone()))
        });
        builtins.register_query("reference", {
            let data = data.clone();
            Arc::new(move |_cfg: &serde_json::Value| {
                Ok(Arc::new(crate::adapters::MemQueryStore::new(data.clone()))
                    as Arc<dyn crate::capabilities::QueryStore>)
            })
        });

        Adapters {
            files,
            query,
            data,
            idempotency: Arc::new(crate::idempotency::MemIdempotencyStore::default()),
            http: None,
            log: Arc::new(crate::logging::NullLogStore),
            log_level: crate::logging::Severity::Info,
            builtins,
            catalogue: None,
            schedule: Arc::new(crate::scheduler::MemScheduleStore::new()),
        }
    }

    /// Swap the scheduler coordination store (e.g. a shared Redis adapter for
    /// HA fire-once). Defaults to the in-memory single-node store.
    pub fn with_schedule_store(
        mut self,
        store: Arc<dyn crate::scheduler::ScheduleStore>,
    ) -> Self {
        self.schedule = store;
        self
    }

    pub fn with_http(mut self, http: Arc<dyn crate::capabilities::HttpOut>) -> Self {
        self.http = Some(http);
        self
    }

    /// Enable external-catalogue fetching, bounded by the operator host
    /// allowlist `hosts` (wildcard patterns). No-op (catalogue stays `None`)
    /// when `hosts` is empty or no outbound HTTP adapter is configured —
    /// call after [`Self::with_http`].
    pub fn with_catalogue(mut self, hosts: Vec<String>) -> Self {
        self.catalogue = match (&self.http, hosts.is_empty()) {
            (Some(http), false) => Some(Arc::new(
                crate::services::catalogue::HttpCatalogueClient::new(http.clone(), hosts),
            )),
            _ => None,
        };
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
        // Registered catalogue URLs are validated at config time (dry-build),
        // so a malformed URL is a 400 at `PUT /raw`, not a runtime surprise.
        // The operator host-allowlist is enforced separately, at fetch time.
        for cat in &config.catalogues {
            if crate::services::code::url_host(&cat.url).is_none()
                || !(cat.url.starts_with("http://") || cat.url.starts_with("https://"))
            {
                return Err(RsError::bad_request(format!(
                    "catalogue '{}' url '{}' must be an absolute http(s) URL",
                    cat.name, cat.url
                )));
            }
        }
        // Validate any mount `schedule` (interval/cron) at config time, so a bad
        // value is a 400 at `PUT /raw` rather than a silent runtime skip.
        for mount in &config.mounts {
            if let Some(sched) = mount.config.get("schedule") {
                crate::scheduler::Schedule::from_config(sched)?;
            }
        }
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
                    // An `elevate` role must not confer operator authority —
                    // otherwise an author on this mount could elevate into
                    // permission-changing power.
                    if let (Some(elevate), Some(ops)) = (
                        mount.config.get("elevate").and_then(|v| v.as_str()),
                        config.operator_roles.as_deref(),
                    ) {
                        if ops.split_whitespace().any(|r| r == elevate) {
                            return Err(RsError::bad_request(format!(
                                "mount '{}' sets elevate role '{elevate}', which is an \
                                 operator role; elevation must not confer operator authority",
                                if mount.base_path.is_empty() { "/" } else { &mount.base_path }
                            )));
                        }
                    }
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
                        config.operator_roles.clone(),
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
                        config.operator_roles.clone(),
                    );
                    Arc::new(crate::services::QueryService::from_config(&mount.config, store)?)
                }
                "template" => {
                    // Rendering JSX needs the JS engine; without it the mount
                    // can't be built (mirrors the loadable-adapter branches).
                    #[cfg(feature = "js")]
                    {
                        let root = crate::services::spec_store::store_root(
                            crate::services::TEMPLATE_PREFIX,
                            &mount.base_path,
                            &mount.config,
                        );
                        let store = crate::services::spec_store::SpecStore::new(
                            adapters.files.clone(),
                            name,
                            &root,
                            crate::services::TEMPLATE_SUBTREE,
                            limits.invocation_limits(),
                            crate::services::TemplateService::validator(),
                            config.operator_roles.clone(),
                        );
                        Arc::new(crate::services::TemplateService::from_config(&mount.config, store)?)
                    }
                    #[cfg(not(feature = "js"))]
                    {
                        return Err(RsError::engine_unavailable(format!(
                            "template mount '{}' renders JSX with the JS engine but this build has \
                             no JS engine (rebuild with --features js)",
                            if mount.base_path.is_empty() { "/" } else { &mount.base_path }
                        )));
                    }
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
                operator_roles: config.operator_roles.clone(),
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
                // The self-config service alone reaches the catalogue client and
                // the built-in adapter registry (to list/install); every other
                // mount sees `None` (default-deny).
                catalogue: if mount.service == "services" {
                    adapters.catalogue.clone()
                } else {
                    None
                },
                builtin_adapters: if mount.service == "services" {
                    Some(adapters.builtins.clone())
                } else {
                    None
                },
                // Default-deny: only the secrets this mount's grant names, looked
                // up in the tenant `secrets` block, resolved host-side.
                secrets: resolve_secrets(&mount.config, config.secrets.as_ref()),
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

/// How a mount's `store.adapter` selects its backend. Read only when the mount
/// is of the relevant kind; absent ⇒ the node default. `builtin:<name>` picks a
/// built-in Rust adapter from the registry; `code:<name>@<version>` a JS
/// loadable adapter; any other string is a config error.
enum AdapterRef<'a> {
    Default,
    Builtin(&'a str),
    Code(&'a str),
}

/// Classify a mount's `store.adapter` for the given service `kind`. Runs at
/// `Tenant::build` time, so a malformed scheme is a 400 at config PUT, not at
/// first use.
fn classify_adapter<'a>(mount: &'a Mount, kind: &str) -> Result<AdapterRef<'a>, RsError> {
    if mount.service != kind {
        return Ok(AdapterRef::Default);
    }
    let Some(adapter) = mount
        .config
        .get("store")
        .and_then(|s| s.get("adapter"))
        .and_then(|a| a.as_str())
    else {
        return Ok(AdapterRef::Default);
    };
    if adapter.starts_with("code:") {
        Ok(AdapterRef::Code(adapter))
    } else if let Some(builtin) = adapter.strip_prefix("builtin:") {
        Ok(AdapterRef::Builtin(builtin))
    } else {
        Err(RsError::bad_request(format!(
            "{kind} store adapter '{adapter}' must be 'builtin:<name>' or 'code:<name>@<version>'"
        )))
    }
}

/// The `store` sub-config passed to an adapter factory (`{}` when absent).
fn store_config(mount: &Mount) -> serde_json::Value {
    mount.config.get("store").cloned().unwrap_or_else(|| serde_json::json!({}))
}

/// Resolve a mount's granted secrets: the names in its `secrets` config, looked
/// up in the tenant `secrets` block. Default-deny — unlisted names (or a missing
/// tenant block) yield nothing.
fn resolve_secrets(
    mount_config: &serde_json::Value,
    tenant_secrets: Option<&serde_json::Value>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let names = mount_config.get("secrets")?.as_array()?;
    let tenant = tenant_secrets.and_then(|s| s.as_object());
    let mut out = serde_json::Map::new();
    for name in names.iter().filter_map(|n| n.as_str()) {
        if let Some(value) = tenant.and_then(|t| t.get(name)) {
            out.insert(name.to_string(), value.clone());
        }
    }
    (!out.is_empty()).then_some(out)
}

/// Resolve a mount's `data` capability. Absent `store.adapter` ⇒ the node
/// built-in; `builtin:<name>` ⇒ a registered Rust adapter; `code:…` ⇒ a
/// resident JS loadable adapter (G13 Phase 2), loaded lazily from the tenant
/// file store. Every backend is tenant-scoped here.
fn data_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Option<ScopedDataStore>, RsError> {
    match classify_adapter(mount, "data")? {
        AdapterRef::Default => Ok(Some(ScopedDataStore::new(adapters.data.clone(), name))),
        AdapterRef::Builtin(builtin) => {
            let inner = adapters
                .builtins
                .build_data(builtin, &store_config(mount))?
                .ok_or_else(|| unknown_builtin("data", builtin, adapters.builtins.data_names()))?;
            Ok(Some(ScopedDataStore::new(inner, name)))
        }
        AdapterRef::Code(adapter_ref) => {
            #[cfg(feature = "js")]
            {
                let files = ScopedFileStore::new(adapters.files.clone(), name);
                let guest = crate::engines::resident::GuestDataStore::from_config(
                    adapter_ref, &store_config(mount), files, name, limits,
                )?;
                Ok(Some(ScopedDataStore::new(Arc::new(guest), name)))
            }
            #[cfg(not(feature = "js"))]
            {
                let _ = limits;
                Err(loadable_without_js("data", mount, adapter_ref))
            }
        }
    }
}

/// Resolve a mount's `files` capability. Absent `store.adapter` ⇒ the node
/// built-in; `builtin:<name>` ⇒ a registered Rust adapter; `code:…` ⇒ a
/// resident JS loadable adapter (G13 Phase 3), whose bundle is still loaded
/// from the built-in store (no circularity).
fn file_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Option<ScopedFileStore>, RsError> {
    match classify_adapter(mount, "file")? {
        AdapterRef::Default => Ok(Some(ScopedFileStore::new(adapters.files.clone(), name))),
        AdapterRef::Builtin(builtin) => {
            let inner = adapters
                .builtins
                .build_files(builtin, &store_config(mount))?
                .ok_or_else(|| unknown_builtin("file", builtin, adapters.builtins.files_names()))?;
            Ok(Some(ScopedFileStore::new(inner, name)))
        }
        AdapterRef::Code(adapter_ref) => {
            #[cfg(feature = "js")]
            {
                let loader = ScopedFileStore::new(adapters.files.clone(), name);
                let guest = crate::engines::resident::GuestFileStore::from_config(
                    adapter_ref, &store_config(mount), loader, name, limits,
                )?;
                Ok(Some(ScopedFileStore::new(Arc::new(guest), name)))
            }
            #[cfg(not(feature = "js"))]
            {
                let _ = limits;
                Err(loadable_without_js("file", mount, adapter_ref))
            }
        }
    }
}

/// Resolve a mount's `query` capability. Absent `store.adapter` ⇒ the node
/// built-in `QueryStore`; `builtin:<name>` ⇒ a registered Rust adapter; `code:…`
/// ⇒ a resident JS loadable adapter (G13 Phase 3) executing stored queries
/// against its own backend. (`store.root` still relocates the authoring subtree
/// — the two `store` keys are independent.)
fn query_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Option<ScopedQueryStore>, RsError> {
    match classify_adapter(mount, "query")? {
        AdapterRef::Default => Ok(Some(ScopedQueryStore::new(adapters.query.clone(), name))),
        AdapterRef::Builtin(builtin) => {
            let inner = adapters
                .builtins
                .build_query(builtin, &store_config(mount))?
                .ok_or_else(|| unknown_builtin("query", builtin, adapters.builtins.query_names()))?;
            Ok(Some(ScopedQueryStore::new(inner, name)))
        }
        AdapterRef::Code(adapter_ref) => {
            #[cfg(feature = "js")]
            {
                let files = ScopedFileStore::new(adapters.files.clone(), name);
                let guest = crate::engines::resident::GuestQueryStore::from_config(
                    adapter_ref, &store_config(mount), files, name, limits,
                )?;
                Ok(Some(ScopedQueryStore::new(Arc::new(guest), name)))
            }
            #[cfg(not(feature = "js"))]
            {
                let _ = limits;
                Err(loadable_without_js("query", mount, adapter_ref))
            }
        }
    }
}

/// Unknown `builtin:<name>` for a kind: a 400 listing the available names.
fn unknown_builtin(kind: &str, name: &str, available: Vec<&str>) -> RsError {
    RsError::bad_request(format!(
        "{kind} store adapter 'builtin:{name}' is unknown (available: {})",
        available.join(", ")
    ))
}

/// A `code:` adapter on a build without the JS engine.
#[cfg(not(feature = "js"))]
fn loadable_without_js(kind: &str, mount: &Mount, adapter_ref: &str) -> RsError {
    RsError::engine_unavailable(format!(
        "{kind} mount '{}' uses a loadable adapter ('{adapter_ref}') but this build has no JS \
         engine (rebuild with --features js)",
        if mount.base_path.is_empty() { "/" } else { &mount.base_path }
    ))
}
