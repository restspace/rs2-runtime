//! Tenant (PRD §4, §9.1): an isolation domain — its own mounts, config,
//! storage namespace, and resource quotas. Built atomically from a
//! [`TenantConfig`]; a future config change builds a new `Tenant` and swaps.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::capabilities::{
    DataStore, FileStore, ScopedDataStore, ScopedFileStore, ScopedMessageGateway, ScopedQueryStore,
};
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
// Strict: service settings (notably `access`) belong under `config`; a typo
// that puts them at the mount level must fail the dry-build loudly rather
// than silently leave the mount on default access.
#[serde(deny_unknown_fields)]
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
    /// Optional embedder-supplied default message gateway (PRD §9.2). There is
    /// no node default, so a `message` mount normally names a `builtin:`/
    /// `code:`/`infra:` adapter; this is the fallback when the mount sets no
    /// `store.adapter`.
    pub messaging: Option<Arc<dyn crate::capabilities::MessageGateway>>,
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
    /// Operator-defined infras (PRD §9.1): named partial adapter configs a
    /// tenant references via `store.adapter = "infra:<name>"`. Held behind a
    /// swappable cell so the operator can reload without restarting
    /// (`Runtime::reload_infras`); shared across `Adapters` clones. Defaults to
    /// empty.
    pub infras: Arc<std::sync::RwLock<Arc<crate::infra::InfraSet>>>,
    /// Source the infra set is (re)loaded from. The server supplies a
    /// file-backed loader over `infras.json`; `None` ⇒ infras are fixed at
    /// startup and `reload_infras` reports no source.
    pub infra_loader: Option<Arc<dyn crate::infra::InfraLoader>>,
}

impl Adapters {
    pub fn new(files: Arc<dyn FileStore>, data: Arc<dyn DataStore>) -> Self {
        let query: Arc<dyn crate::capabilities::QueryStore> =
            Arc::new(crate::adapters::MemQueryStore::new(data.clone()));

        // Seed the node built-ins under their canonical names.
        //
        // `local` hands back the node file store, rooted under `store.root`
        // when one is given (so a primary `builtin:local` mount is isolated —
        // `file_capability` *requires* the root). Without a root it returns the
        // bare node store: that's the spec-store backend path, where the
        // `SpecStore` applies its own root. `mem`, by contrast, is a single
        // dedicated in-memory store shared by the node default and every
        // `builtin:mem` mount: the node default data adapter may be something
        // else entirely (the server defaults it to file-backed), so `builtin:mem`
        // must always mean an in-memory store, not "whatever the default is" —
        // but it offers no per-mount isolation (it ignores `root`). `reference`
        // rebuilds the scanning query adapter over the default data store.
        let mut builtins = crate::adapters::BuiltinRegistry::default();
        builtins.register_data("mem", {
            let mem: Arc<dyn DataStore> = Arc::new(crate::adapters::MemDataStore::new());
            Arc::new(move |_cfg: &serde_json::Value| Ok(mem.clone()))
        });
        builtins.register_files("local", {
            let files = files.clone();
            Arc::new(move |cfg: &serde_json::Value| {
                Ok(match crate::capabilities::sanitized_store_root(cfg)? {
                    Some(root) => Arc::new(crate::capabilities::PrefixedFileStore::new(
                        files.clone(),
                        root,
                    )) as Arc<dyn FileStore>,
                    None => files.clone(),
                })
            })
        });
        builtins.register_query("reference", {
            let data = data.clone();
            Arc::new(move |_cfg: &serde_json::Value| {
                Ok(Arc::new(crate::adapters::MemQueryStore::new(data.clone()))
                    as Arc<dyn crate::capabilities::QueryStore>)
            })
        });
        // First-party provider adapters. Unlike the store kinds these take the
        // node's outbound HTTP capability, so every provider call crosses the
        // one audited egress choke point instead of opening its own client.
        builtins.register_message(
            "cf-email",
            Arc::new(|cfg: &serde_json::Value, http| {
                Ok(
                    Arc::new(crate::adapters::CfEmailGateway::from_config(cfg, http)?)
                        as Arc<dyn crate::capabilities::MessageGateway>,
                )
            }),
        );
        builtins.register_message(
            "aws-sns",
            Arc::new(|cfg: &serde_json::Value, http| {
                Ok(
                    Arc::new(crate::adapters::AwsSnsGateway::from_config(cfg, http)?)
                        as Arc<dyn crate::capabilities::MessageGateway>,
                )
            }),
        );

        Adapters {
            files,
            query,
            data,
            messaging: None,
            idempotency: Arc::new(crate::idempotency::MemIdempotencyStore::default()),
            http: None,
            log: Arc::new(crate::logging::NullLogStore),
            log_level: crate::logging::Severity::Info,
            builtins,
            catalogue: None,
            schedule: Arc::new(crate::scheduler::MemScheduleStore::new()),
            infras: Arc::new(std::sync::RwLock::new(Arc::new(
                crate::infra::InfraSet::default(),
            ))),
            infra_loader: None,
        }
    }

    /// Seed the node's infras (the operator's `infras.json`). Replaces the
    /// empty default; later swapped live by [`Self::swap_infras`].
    pub fn with_infras(self, infras: crate::infra::InfraSet) -> Self {
        *self.infras.write().unwrap() = Arc::new(infras);
        self
    }

    /// Install the infra reload source. `Runtime::reload_infras` re-reads it,
    /// swaps the live set, and purges tenants.
    pub fn with_infra_loader(mut self, loader: Arc<dyn crate::infra::InfraLoader>) -> Self {
        self.infra_loader = Some(loader);
        self
    }

    /// Atomically replace the live infra set (the reload path). Shared across
    /// `Adapters` clones, so the running `Runtime` sees it; rebuilt tenants
    /// pick it up on their next build.
    pub fn swap_infras(&self, infras: Arc<crate::infra::InfraSet>) {
        *self.infras.write().unwrap() = infras;
    }

    /// A snapshot of the live infra set — taken once per `Tenant::build`, never
    /// re-read mid-build (so a concurrent reload can't tear a build).
    pub fn infras_snapshot(&self) -> Arc<crate::infra::InfraSet> {
        self.infras.read().unwrap().clone()
    }

    /// Swap the scheduler coordination store (e.g. a shared Redis adapter for
    /// HA fire-once). Defaults to the in-memory single-node store.
    pub fn with_schedule_store(mut self, store: Arc<dyn crate::scheduler::ScheduleStore>) -> Self {
        self.schedule = store;
        self
    }

    pub fn with_http(mut self, http: Arc<dyn crate::capabilities::HttpOut>) -> Self {
        self.http = Some(http);
        self
    }

    /// Supply an embedder default message gateway, used by a `message` mount
    /// that names no `store.adapter`.
    pub fn with_messaging(mut self, gateway: Arc<dyn crate::capabilities::MessageGateway>) -> Self {
        self.messaging = Some(gateway);
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
            if crate::outbound::url_host(&cat.url).is_none()
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
        // Snapshot the live infra set once for the whole build — a concurrent
        // reload swaps the cell but never tears this build.
        let infras = adapters.infras_snapshot();
        let mut instances = HashMap::new();
        for mount in mounts.mounts() {
            let service: Arc<dyn Service> = match mount.service.as_str() {
                "file" => Arc::new(FileService::from_config(&mount.config)),
                "data" => Arc::new(DataService::from_config(&mount.config)),
                "pipeline" => {
                    check_elevate_not_operator(mount, config.operator_roles.as_deref())?;
                    let root = crate::services::spec_store::store_root(
                        crate::services::PIPELINE_PREFIX,
                        &mount.base_path,
                        &mount.config,
                    );
                    let backend = resolve_spec_backend(
                        mount, adapters, name, limits.invocation_limits(), &infras,
                    )?;
                    let store = crate::services::spec_store::SpecStore::new(
                        backend,
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
                    let backend = resolve_spec_backend(
                        mount, adapters, name, limits.invocation_limits(), &infras,
                    )?;
                    let store = crate::services::spec_store::SpecStore::new(
                        backend,
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
                        let backend = resolve_spec_backend(
                            mount, adapters, name, limits.invocation_limits(), &infras,
                        )?;
                        let store = crate::services::spec_store::SpecStore::new(
                            backend,
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
                "wrapper" => {
                    // One inline pipeline fronting another mount: same elevate
                    // guard as `pipeline`, plus a config-declared discovery
                    // `pattern` validated here so a bad value is a 400 at PUT.
                    check_elevate_not_operator(mount, config.operator_roles.as_deref())?;
                    if let Some(p) = mount.config.get("pattern").and_then(|v| v.as_str()) {
                        if !KNOWN_PATTERNS.contains(&p) {
                            return Err(RsError::bad_request(format!(
                                "wrapper mount '{}' declares unknown pattern '{p}' (one of: {})",
                                if mount.base_path.is_empty() { "/" } else { &mount.base_path },
                                KNOWN_PATTERNS.join(", ")
                            )));
                        }
                    }
                    Arc::new(crate::services::WrapperService::from_config(&mount.config)?)
                }
                "auth" => Arc::new(crate::services::AuthService::from_config(
                    &mount.config,
                    config.auth.as_ref(),
                )?),
                "services" => Arc::new(crate::services::ServicesService::new()),
                "log" => Arc::new(crate::services::LogReaderService::new()),
                "message" => Arc::new(crate::services::MessageService::new()),
                "proxy" => Arc::new(crate::services::ProxyService::new()),
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
            let files =
                file_capability(mount, adapters, name, limits.invocation_limits(), &infras)?;
            let data = data_capability(mount, adapters, name, limits.invocation_limits(), &infras)?;
            let query =
                query_capability(mount, adapters, name, limits.invocation_limits(), &infras)?;
            let messaging =
                message_capability(mount, adapters, name, limits.invocation_limits(), &infras)?;

            // Resolve secrets and outbound credential injectors once, host-side,
            // so the secret material lands only in the injector / `secrets` map —
            // never echoed back into the mount `config`.
            let mount_secrets = resolve_secrets(&mount.config, config.secrets.as_ref());
            let outbound_injectors =
                resolve_outbound_injectors(mount, &infras, name, mount_secrets.as_ref())?;

            // Capability grants: each instance gets handles pre-scoped to
            // this tenant — host-enforced isolation (PRD §9.2).
            let ctx = ServiceContext {
                config: mount.config.clone(),
                files,
                data,
                query,
                messaging,
                http: adapters.http.clone(),
                cache_policy: crate::wrapper::CachePolicy::from_config(mount.config.get("caching")),
                cache_openly_readable: crate::wrapper::CachePolicy::mount_is_openly_readable(
                    &mount.config,
                ),
                cors: Arc::new(crate::wrapper::CorsPolicy::from_config(
                    config.cors.as_ref(),
                )),
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
                // The self-config service alone sees the node infra set, to list
                // what a tenant may consume (secrets redacted); every other mount
                // sees `None` (default-deny).
                infras: if mount.service == "services" {
                    Some(infras.clone())
                } else {
                    None
                },
                // Default-deny: only the secrets this mount's grant names, looked
                // up in the tenant `secrets` block, resolved host-side.
                secrets: mount_secrets,
                outbound_injectors,
            };
            instances.insert(mount.base_path.clone(), (service, Arc::new(ctx)));
        }
        Ok(Tenant {
            name: name.to_string(),
            mounts,
            instances,
            auth: config.auth.clone(),
            cors: Arc::new(crate::wrapper::CorsPolicy::from_config(
                config.cors.as_ref(),
            )),
        })
    }

    pub fn instance(&self, base_path: &str) -> Option<&(Arc<dyn Service>, Arc<ServiceContext>)> {
        self.instances.get(base_path)
    }
}

/// Discovery API patterns a `wrapper` mount may declare (it fronts a mount of
/// one of these shapes). Kept in sync with `discovery::pattern_of`.
const KNOWN_PATTERNS: &[&str] = &["store", "store-transform", "store-view", "view", "api"];

/// An `elevate` role must not be an operator role — otherwise an author on a
/// `pipeline`/`wrapper` mount could elevate a call into permission-changing
/// power. Shared by both service arms.
fn check_elevate_not_operator(mount: &Mount, operator_roles: Option<&str>) -> Result<(), RsError> {
    if let (Some(elevate), Some(ops)) = (
        mount.config.get("elevate").and_then(|v| v.as_str()),
        operator_roles,
    ) {
        if ops.split_whitespace().any(|r| r == elevate) {
            return Err(RsError::bad_request(format!(
                "mount '{}' sets elevate role '{elevate}', which is an operator role; \
                 elevation must not confer operator authority",
                if mount.base_path.is_empty() {
                    "/"
                } else {
                    &mount.base_path
                }
            )));
        }
    }
    Ok(())
}

/// The built-in names that alias the node's own default stores: selecting one
/// as a *primary* mount backend therefore requires an explicit `store.root` so
/// the mount is physically isolated (`require_store_root`). Other built-ins (and
/// the spec-store backend path) manage their own namespacing.
const NODE_DATA_BUILTIN: &str = "file";
const NODE_FILE_BUILTIN: &str = "local";

/// A resolved storage backend selection, after any `infra:<name>` expansion.
/// `builtin:<name>` picks a built-in Rust adapter from the registry;
/// `code:<name>@<version>` a JS loadable adapter; absent ⇒ the node default.
enum StoreKind {
    Default,
    Builtin(String),
    Code(String),
}

/// Classify the `adapter` of an (already infra-expanded) `store`-shaped value.
/// An `infra:` prefix here means expansion didn't rewrite it (an operator bug);
/// callers always expand first, so it can't normally occur.
fn classify_store(store: &serde_json::Value) -> Result<StoreKind, RsError> {
    let Some(adapter) = store.get("adapter").and_then(|a| a.as_str()) else {
        return Ok(StoreKind::Default);
    };
    if adapter.starts_with("code:") {
        Ok(StoreKind::Code(adapter.to_string()))
    } else if let Some(builtin) = adapter.strip_prefix("builtin:") {
        Ok(StoreKind::Builtin(builtin.to_string()))
    } else {
        Err(RsError::bad_request(format!(
            "store adapter '{adapter}' must be 'builtin:<name>', 'code:<name>@<version>', \
             or 'infra:<name>'"
        )))
    }
}

/// Expand a mount's primary `store` for the given service `kind`: returns the
/// node default unless the mount *is* that kind, in which case it resolves any
/// `infra:<name>` ref (operator config + secrets merged in, default-deny) and
/// classifies the result. Runs at `Tenant::build` time, so a bad scheme or a
/// rejected infra is a 400 at config PUT, not at first use.
fn expand_store(
    mount: &Mount,
    kind: &str,
    tenant: &str,
    infras: &crate::infra::InfraSet,
) -> Result<(StoreKind, serde_json::Value), RsError> {
    if mount.service != kind {
        return Ok((StoreKind::Default, serde_json::json!({})));
    }
    let raw = mount
        .config
        .get("store")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let expanded = crate::infra::expand_infra(&raw, infras, tenant)?.into_owned();
    Ok((classify_store(&expanded)?, expanded))
}

/// Resolve a `file` backend from an (already infra-expanded) store value —
/// shared by `file_capability` and the spec-store backend. Returns the
/// **unscoped** `Arc<dyn FileStore>`; callers scope it to the tenant. Some
/// params are only used on one side of the `js` feature split.
#[allow(unused_variables)]
fn build_file_backend(
    kind: StoreKind,
    store: &serde_json::Value,
    adapters: &Adapters,
    label: &str,
    mount: &Mount,
    tenant: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Arc<dyn FileStore>, RsError> {
    match kind {
        StoreKind::Default => Ok(adapters.files.clone()),
        StoreKind::Builtin(builtin) => adapters
            .builtins
            .build_files(&builtin, store)?
            .ok_or_else(|| unknown_builtin(label, &builtin, adapters.builtins.files_names())),
        StoreKind::Code(adapter_ref) => {
            #[cfg(feature = "js")]
            {
                let loader = ScopedFileStore::new(adapters.files.clone(), tenant);
                let guest = crate::engines::resident::GuestFileStore::from_config(
                    &adapter_ref,
                    store,
                    loader,
                    tenant,
                    limits,
                )?;
                Ok(Arc::new(guest))
            }
            #[cfg(not(feature = "js"))]
            {
                Err(loadable_without_js(label, mount, &adapter_ref))
            }
        }
    }
}

/// Resolve the **file backend** for a spec store (pipeline/query/template
/// authoring subtree) from the mount's `specStore` block: absent ⇒ the node
/// file store (today's behavior); otherwise an `infra:`/`builtin:`/`code:`
/// file adapter. Independent of the mount's primary `store.adapter` (which, on
/// a `query` mount, selects the query *execution* backend). Returns the
/// unscoped backend; `SpecStore::new` roots + scopes it.
fn resolve_spec_backend(
    mount: &Mount,
    adapters: &Adapters,
    tenant: &str,
    limits: crate::contract::InvocationLimits,
    infras: &crate::infra::InfraSet,
) -> Result<Arc<dyn FileStore>, RsError> {
    let raw = mount
        .config
        .get("specStore")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let expanded = crate::infra::expand_infra(&raw, infras, tenant)?.into_owned();
    let kind = classify_store(&expanded)?;
    build_file_backend(kind, &expanded, adapters, "spec", mount, tenant, limits)
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

/// Resolve a mount's outbound credential injectors from its `httpOut` grants'
/// `inject` refs. Two forms (PRD §9.2 credential-injection-without-disclosure):
///
/// - `"inject": "infra:<name>"` — the operator infra supplies the whole strategy
///   (`{"auth":…}` + secret) in `infras.json`; the tenant only names it.
/// - `"inject": { "auth": …, "token": "secret:<name>", … }` — an inline strategy
///   where any `secret:<name>` leaf is replaced by the mount's granted secret.
///
/// Either way the secret is read here, host-side, and returned inside the
/// injector — never written back to the mount `config`. Runs at build time, so a
/// bad ref or strategy is a 400 at config PUT.
fn resolve_outbound_injectors(
    mount: &Mount,
    infras: &crate::infra::InfraSet,
    tenant: &str,
    secrets: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<HashMap<String, Arc<crate::adapters::CredentialInjector>>, RsError> {
    let mut out = HashMap::new();
    // Each `httpOut` grant on a code service may carry its own `inject`.
    if let Some(grants) = mount.config.get("grants").and_then(|g| g.as_object()) {
        for (capability, grant) in grants {
            if let Some(inject) = grant.get("inject") {
                if let Some(inj) =
                    resolve_one_injector(capability, inject, infras, tenant, secrets)?
                {
                    out.insert(capability.clone(), Arc::new(inj));
                }
            }
        }
    }
    // A `proxy` mount carries a single top-level `inject`, stored under a
    // reserved key the service reads (a proxy mount has no `grants`).
    if mount.service == "proxy" {
        if let Some(inject) = mount.config.get("inject") {
            if let Some(inj) = resolve_one_injector("proxy", inject, infras, tenant, secrets)? {
                out.insert(
                    crate::services::PROXY_INJECTOR_KEY.to_string(),
                    Arc::new(inj),
                );
            }
        }
    }
    Ok(out)
}

/// Resolve a single `inject` value (a grant's, or a proxy mount's) to a
/// [`CredentialInjector`]. `label` names the source for error messages.
fn resolve_one_injector(
    label: &str,
    inject: &serde_json::Value,
    infras: &crate::infra::InfraSet,
    tenant: &str,
    secrets: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<crate::adapters::CredentialInjector>, RsError> {
    match inject {
        serde_json::Value::String(s) if s.starts_with("infra:") => {
            let placeholder = serde_json::json!({ "adapter": s });
            let merged = crate::infra::expand_infra(&placeholder, infras, tenant)?;
            crate::adapters::CredentialInjector::from_config(&merged)
        }
        serde_json::Value::String(s) => Err(RsError::bad_request(format!(
            "'{label}' inject string must be 'infra:<name>' (got '{s}'); use an object to inject \
             from tenant secrets"
        ))),
        serde_json::Value::Object(_) => {
            let resolved = substitute_secret_refs(inject, secrets)?;
            crate::adapters::CredentialInjector::from_config(&resolved)
        }
        _ => Err(RsError::bad_request(format!(
            "'{label}' inject must be 'infra:<name>' or an inline object"
        ))),
    }
}

/// Replace every `secret:<name>` string leaf in an inline inject config with the
/// mount's granted secret value (host-side). Default-deny: an unknown/ungranted
/// name is a 400. Non-`secret:` strings pass through unchanged.
fn substitute_secret_refs(
    value: &serde_json::Value,
    secrets: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<serde_json::Value, RsError> {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k.clone(), substitute_secret_refs(v, secrets)?);
            }
            Ok(Value::Object(out))
        }
        Value::String(s) => match s.strip_prefix("secret:") {
            Some(name) => match secrets.and_then(|m| m.get(name)) {
                Some(v @ Value::String(_)) => Ok(v.clone()),
                Some(_) => Err(RsError::bad_request(format!(
                    "secret '{name}' must be a string to use in an inject strategy"
                ))),
                None => Err(RsError::bad_request(format!(
                    "inject references secret '{name}' not granted to this mount"
                ))),
            },
            None => Ok(value.clone()),
        },
        _ => Ok(value.clone()),
    }
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
    infras: &crate::infra::InfraSet,
) -> Result<Option<ScopedDataStore>, RsError> {
    let (kind, store) = expand_store(mount, "data", name, infras)?;
    match kind {
        StoreKind::Default => Ok(Some(ScopedDataStore::new(adapters.data.clone(), name))),
        StoreKind::Builtin(builtin) => {
            // `builtin:file` aliases the node default data store; an explicit
            // selection must root itself for per-mount isolation (a missing
            // `root` is a config 400, preventing a public-write mount from
            // sharing the store `auth` reads users from). `builtin:mem` is a
            // dedicated shared in-memory store and intentionally ignores `root`.
            if builtin == NODE_DATA_BUILTIN {
                crate::capabilities::require_store_root(&store)?;
            }
            let inner = adapters
                .builtins
                .build_data(&builtin, &store)?
                .ok_or_else(|| unknown_builtin("data", &builtin, adapters.builtins.data_names()))?;
            Ok(Some(ScopedDataStore::new(inner, name)))
        }
        StoreKind::Code(adapter_ref) => {
            #[cfg(feature = "js")]
            {
                let files = ScopedFileStore::new(adapters.files.clone(), name);
                let guest = crate::engines::resident::GuestDataStore::from_config(
                    &adapter_ref,
                    &store,
                    files,
                    name,
                    limits,
                )?;
                Ok(Some(ScopedDataStore::new(Arc::new(guest), name)))
            }
            #[cfg(not(feature = "js"))]
            {
                let _ = limits;
                Err(loadable_without_js("data", mount, &adapter_ref))
            }
        }
    }
}

/// Resolve a mount's `files` capability. Absent `store.adapter` ⇒ the node
/// built-in; `builtin:<name>` ⇒ a registered Rust adapter; `code:…` ⇒ a
/// resident JS loadable adapter (G13 Phase 3); `infra:<name>` ⇒ an operator
/// infra resolving to one of those, with its config (and secrets) merged in.
fn file_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
    infras: &crate::infra::InfraSet,
) -> Result<Option<ScopedFileStore>, RsError> {
    let (kind, store) = expand_store(mount, "file", name, infras)?;
    // `builtin:local` aliases the node file store; selecting it as a primary
    // file backend must root itself for per-mount isolation (a missing `root`
    // is a config 400). Spec stores reuse this adapter via `build_file_backend`
    // directly and root themselves, so the requirement lives here, not in the
    // factory.
    if let StoreKind::Builtin(builtin) = &kind {
        if builtin == NODE_FILE_BUILTIN {
            crate::capabilities::require_store_root(&store)?;
        }
    }
    let inner = build_file_backend(kind, &store, adapters, "file", mount, name, limits)?;
    Ok(Some(ScopedFileStore::new(inner, name)))
}

/// Resolve a mount's `query` capability. Absent `store.adapter` ⇒ the node
/// built-in `QueryStore`; `builtin:<name>` ⇒ a registered Rust adapter; `code:…`
/// ⇒ a resident JS loadable adapter (G13 Phase 3) executing stored queries
/// against its own backend; `infra:<name>` ⇒ an operator infra. (`store.root` /
/// the `specStore` block relocate the authoring subtree — independent of this.)
fn query_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
    infras: &crate::infra::InfraSet,
) -> Result<Option<ScopedQueryStore>, RsError> {
    let (kind, store) = expand_store(mount, "query", name, infras)?;
    match kind {
        StoreKind::Default => Ok(Some(ScopedQueryStore::new(adapters.query.clone(), name))),
        StoreKind::Builtin(builtin) => {
            let inner = adapters
                .builtins
                .build_query(&builtin, &store)?
                .ok_or_else(|| {
                    unknown_builtin("query", &builtin, adapters.builtins.query_names())
                })?;
            Ok(Some(ScopedQueryStore::new(inner, name)))
        }
        StoreKind::Code(adapter_ref) => {
            #[cfg(feature = "js")]
            {
                let files = ScopedFileStore::new(adapters.files.clone(), name);
                let guest = crate::engines::resident::GuestQueryStore::from_config(
                    &adapter_ref,
                    &store,
                    files,
                    name,
                    limits,
                )?;
                Ok(Some(ScopedQueryStore::new(Arc::new(guest), name)))
            }
            #[cfg(not(feature = "js"))]
            {
                let _ = limits;
                Err(loadable_without_js("query", mount, &adapter_ref))
            }
        }
    }
}

/// Resolve a `message` mount's gateway. Only the `message` service gets it
/// (every other mount sees `None`).
///
/// Two config shapes, because one mount often needs two providers:
///
/// - `store.adapter` — a single adapter serving whatever channels it declares.
/// - `store.adapters` — a `channel → store block` map; each entry resolves
///   exactly like a single `store` (so `infra:` and `code:` work per channel),
///   and the results are composed behind a [`RoutingGateway`]. A bare string is
///   shorthand for `{"adapter": "<string>"}`.
///
/// `builtin:<name>` selects a first-party provider adapter; `code:…` a resident
/// JS one; `infra:<name>` an operator infra resolving to either. Absent ⇒ the
/// embedder default if any, else a config 400 — there is no node default
/// provider, because sending mail requires credentials nobody can guess.
fn message_capability(
    mount: &Mount,
    adapters: &Adapters,
    name: &str,
    limits: crate::contract::InvocationLimits,
    infras: &crate::infra::InfraSet,
) -> Result<Option<ScopedMessageGateway>, RsError> {
    if mount.service != "message" {
        return Ok(None);
    }
    let raw = mount
        .config
        .get("store")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    // The per-channel map wins when present; a mount that sets both is a
    // config error rather than a silent precedence rule.
    if let Some(map) = raw.get("adapters") {
        if raw.get("adapter").is_some() {
            return Err(RsError::bad_request(
                "a message mount sets either store.adapter or store.adapters, not both",
            ));
        }
        let entries = map.as_object().ok_or_else(|| {
            RsError::bad_request("store.adapters must be an object of channel → adapter")
        })?;
        if entries.is_empty() {
            return Err(RsError::bad_request(
                "store.adapters is empty — name at least one channel",
            ));
        }
        let mut routes: Vec<(
            crate::capabilities::Channel,
            Arc<dyn crate::capabilities::MessageGateway>,
        )> = Vec::new();
        for (channel_name, entry) in entries {
            let channel = crate::capabilities::Channel::parse(channel_name).ok_or_else(|| {
                RsError::bad_request(format!(
                    "unknown channel '{channel_name}' in store.adapters (one of: {})",
                    crate::capabilities::Channel::all()
                        .iter()
                        .map(|c| c.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
            // A bare string is shorthand for a one-key store block.
            let block = match entry {
                serde_json::Value::String(s) => serde_json::json!({ "adapter": s }),
                other => other.clone(),
            };
            let expanded = crate::infra::expand_infra(&block, infras, name)?.into_owned();
            let gateway = build_message_backend(
                classify_store(&expanded)?,
                &expanded,
                adapters,
                mount,
                name,
                limits.clone(),
            )?;
            // An adapter routed for a channel it does not serve is a config
            // error, not a runtime 400 on the first send.
            if !gateway.channels().contains(&channel) {
                return Err(RsError::bad_request(format!(
                    "adapter '{}' is routed for the '{channel}' channel but serves: {}",
                    gateway.provider(),
                    gateway
                        .channels()
                        .iter()
                        .map(|c| c.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            routes.push((channel, gateway));
        }
        let routing = crate::capabilities::RoutingGateway::new(routes);
        return Ok(Some(ScopedMessageGateway::new(Arc::new(routing), name)));
    }

    let expanded = crate::infra::expand_infra(&raw, infras, name)?.into_owned();
    let kind = classify_store(&expanded)?;
    if matches!(kind, StoreKind::Default) {
        return match &adapters.messaging {
            Some(g) => Ok(Some(ScopedMessageGateway::new(g.clone(), name))),
            None => Err(RsError::bad_request(
                "message mount requires a store.adapter ('builtin:<name>', \
                 'code:<name>@<version>' or 'infra:<name>') or a per-channel store.adapters map",
            )),
        };
    }
    let gateway = build_message_backend(kind, &expanded, adapters, mount, name, limits)?;
    Ok(Some(ScopedMessageGateway::new(gateway, name)))
}

/// Build one un-scoped provider adapter from an (already infra-expanded) store
/// block. Shared by the single-adapter and per-channel paths so both accept
/// exactly the same forms.
#[allow(unused_variables)]
fn build_message_backend(
    kind: StoreKind,
    store: &serde_json::Value,
    adapters: &Adapters,
    mount: &Mount,
    tenant: &str,
    limits: crate::contract::InvocationLimits,
) -> Result<Arc<dyn crate::capabilities::MessageGateway>, RsError> {
    match kind {
        StoreKind::Default => Err(RsError::bad_request(
            "each store.adapters entry needs an 'adapter'",
        )),
        StoreKind::Builtin(builtin) => {
            let http = adapters.http.clone().ok_or_else(|| {
                RsError::bad_request(format!(
                    "message adapter 'builtin:{builtin}' calls a provider over HTTP, but this \
                     node has no outbound HTTP capability"
                ))
            })?;
            adapters
                .builtins
                .build_message(&builtin, store, http)?
                .ok_or_else(|| {
                    unknown_builtin("message", &builtin, adapters.builtins.message_names())
                })
        }
        StoreKind::Code(adapter_ref) => {
            #[cfg(feature = "js")]
            {
                let files = ScopedFileStore::new(adapters.files.clone(), tenant);
                let guest = crate::engines::resident::GuestMessageGateway::from_config(
                    &adapter_ref,
                    store,
                    files,
                    tenant,
                    limits,
                )?;
                Ok(Arc::new(guest))
            }
            #[cfg(not(feature = "js"))]
            {
                Err(loadable_without_js("message", mount, &adapter_ref))
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
        if mount.base_path.is_empty() {
            "/"
        } else {
            &mount.base_path
        }
    ))
}
