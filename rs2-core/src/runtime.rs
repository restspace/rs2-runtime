//! The runtime: lazy tenant loading and the dispatch path (PRD §5.2).
//!
//! `handle` never returns an error — every failure becomes a structured
//! problem+json response attributed to the tenant and trace.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use crate::error::RsError;
use crate::idempotency;
use crate::message::Message;
use crate::router::{validate_path, Tenancy};
use crate::tenant::{Adapters, Tenant, TenantConfig};
use crate::wrapper::{check_access, check_declared_body_size, LimitTable, TenantBreaker, TenantLimiter};

/// Source of tenant configs ("config store" — PRD §13). The server binary
/// supplies a file-backed loader; embedders supply their own.
#[async_trait]
pub trait ConfigLoader: Send + Sync {
    async fn load_tenant(&self, tenant: &str) -> Result<TenantConfig, RsError>;

    /// Raw config document + opaque version (for `If-Match` optimistic
    /// concurrency on the self-config API, PRD §10.6).
    async fn load_raw(&self, tenant: &str) -> Result<(serde_json::Value, String), RsError> {
        let _ = tenant;
        Err(RsError::engine_unavailable("this config loader does not support raw access"))
    }

    /// Enumerate known tenants for host-driven background work (the scheduler).
    /// Default: none — loaders that can't enumerate opt out, and the scheduler
    /// still covers the tenancy-derived set (single tenant / multi domain map).
    async fn list_tenants(&self) -> Result<Vec<String>, RsError> {
        Ok(vec![])
    }

    /// Persist a raw config document; `expected_version` mismatches fail
    /// with 409. Returns the new version.
    async fn save_raw(
        &self,
        tenant: &str,
        config: &serde_json::Value,
        expected_version: Option<&str>,
    ) -> Result<String, RsError> {
        let _ = (tenant, config, expected_version);
        Err(RsError::engine_unavailable("this config loader does not support writes"))
    }
}

/// Control-plane capability granted to the `services` self-config service
/// (PRD §10.6): read and atomically replace the tenant's config.
#[async_trait]
pub trait TenantControl: Send + Sync {
    async fn raw_config(&self, tenant: &str) -> Result<(serde_json::Value, String), RsError>;
    /// Validate the whole config, persist it, and swap the running tenant
    /// atomically. Invalid config → 400; the running tenant is untouched.
    async fn put_config(
        &self,
        tenant: &str,
        config: serde_json::Value,
        if_match: Option<&str>,
    ) -> Result<String, RsError>;
}

pub struct Runtime {
    tenancy: Tenancy,
    adapters: Adapters,
    loader: Arc<dyn ConfigLoader>,
    limits: LimitTable,
    limiter: TenantLimiter,
    breaker: TenantBreaker,
    tenants: tokio::sync::RwLock<HashMap<String, Arc<Tenant>>>,
    /// Serializes tenant builds so concurrent first requests load once
    /// (re-entrance guard, PRD §9.1).
    load_guard: tokio::sync::Mutex<()>,
    /// Self-reference handed to services as the internal-dispatch capability.
    self_ref: std::sync::Weak<Runtime>,
    /// Node log sink (PRD §14): boundary + error logs emit here.
    log: Arc<dyn crate::logging::LogStore>,
    /// Boundary-log severity floor; failures bypass it.
    log_level: crate::logging::Severity,
}

/// Internal dispatch capability handed to services (pipelines): requests
/// route back through the full dispatch path — authz, limits, idempotency
/// all apply to internal calls (PRD §5.2).
struct RuntimeRequester(std::sync::Weak<Runtime>);

#[async_trait]
impl crate::pipeline::Requester for RuntimeRequester {
    async fn request(&self, msg: Message) -> Message {
        match self.0.upgrade() {
            Some(rt) => rt.handle(msg).await,
            None => {
                let template = msg.response(StatusCode::INTERNAL_SERVER_ERROR, None);
                template.error_response(&RsError::internal("runtime has shut down"))
            }
        }
    }
}

struct RuntimeControl(std::sync::Weak<Runtime>);

#[async_trait]
impl TenantControl for RuntimeControl {
    async fn raw_config(&self, tenant: &str) -> Result<(serde_json::Value, String), RsError> {
        let rt = self.0.upgrade().ok_or_else(|| RsError::internal("runtime has shut down"))?;
        rt.loader.load_raw(tenant).await
    }

    async fn put_config(
        &self,
        tenant: &str,
        config: serde_json::Value,
        if_match: Option<&str>,
    ) -> Result<String, RsError> {
        let rt = self.0.upgrade().ok_or_else(|| RsError::internal("runtime has shut down"))?;
        // Validate the entire config by dry-building the tenant (PRD §10.6):
        // mounts, service configs, pipeline specs all checked before any
        // persistence — an invalid config never touches the running tenant.
        let parsed: TenantConfig = serde_json::from_value(config.clone())
            .map_err(|e| RsError::bad_request(format!("invalid tenant config: {e}")))?;
        Tenant::build(tenant, parsed, &rt.adapters, &rt.limits, None, None)?;
        let version = rt.loader.save_raw(tenant, &config, if_match).await?;
        // Atomic swap: purge the built tenant; the next request rebuilds
        // from the persisted config. In-flight requests hold the old Arc.
        rt.purge_tenant(tenant).await;
        Ok(version)
    }
}

impl Runtime {
    pub fn new(
        tenancy: Tenancy,
        adapters: Adapters,
        loader: Arc<dyn ConfigLoader>,
        limits: LimitTable,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Runtime {
            log: adapters.log.clone(),
            log_level: adapters.log_level,
            tenancy,
            adapters,
            loader,
            limits,
            limiter: TenantLimiter::new(),
            breaker: TenantBreaker::new(),
            tenants: tokio::sync::RwLock::new(HashMap::new()),
            load_guard: tokio::sync::Mutex::new(()),
            self_ref: me.clone(),
        })
    }

    pub fn resolve_tenant(&self, host: &str) -> Option<String> {
        self.tenancy.resolve(host)
    }

    async fn tenant(&self, name: &str) -> Result<Arc<Tenant>, RsError> {
        if let Some(t) = self.tenants.read().await.get(name) {
            return Ok(t.clone());
        }
        let _guard = self.load_guard.lock().await;
        // Double-check: another request may have loaded it while we waited.
        if let Some(t) = self.tenants.read().await.get(name) {
            return Ok(t.clone());
        }
        let config = self.loader.load_tenant(name).await?;
        let requester: Arc<dyn crate::pipeline::Requester> =
            Arc::new(RuntimeRequester(self.self_ref.clone()));
        let control: Arc<dyn TenantControl> = Arc::new(RuntimeControl(self.self_ref.clone()));
        let tenant = Arc::new(Tenant::build(
            name,
            config,
            &self.adapters,
            &self.limits,
            Some(requester),
            Some(control),
        )?);
        self.tenants.write().await.insert(name.to_string(), tenant.clone());
        Ok(tenant)
    }

    /// Drop a tenant's built instances so the next request rebuilds from
    /// config — the M2 self-config hot-reload path swaps atomically here.
    pub async fn purge_tenant(&self, name: &str) {
        self.tenants.write().await.remove(name);
    }

    /// Spawn the host scheduler (G1): a detached task that periodically fires a
    /// synthetic internal request at every mount carrying a `config.schedule`.
    /// No-op when nothing is scheduled; the task holds a `Weak` and exits when
    /// the runtime drops. Single-node by default — configure a shared
    /// `ScheduleStore` (Adapters::with_schedule_store) for HA fire-once.
    pub fn spawn_scheduler(self: &std::sync::Arc<Self>) {
        self.spawn_scheduler_with(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(15),
        );
    }

    /// [`Self::spawn_scheduler`] with explicit due-check and reconcile cadences
    /// (tests pass short ones).
    pub fn spawn_scheduler_with(
        self: &std::sync::Arc<Self>,
        tick: std::time::Duration,
        reconcile: std::time::Duration,
    ) {
        tokio::spawn(scheduler_loop(self.self_ref.clone(), tick, reconcile));
    }

    /// Handle a message; failures become problem+json responses. CORS
    /// response headers are added here — the single choke point — so error
    /// responses carry them too (browsers can't read un-decorated errors).
    pub async fn handle(&self, msg: Message) -> Message {
        let start = std::time::Instant::now();
        // Capture enough context to build an error response and a boundary log
        // after `msg` moves into dispatch.
        let template = msg.response(StatusCode::OK, None);
        let origin = msg.header("origin").map(str::to_string);
        let request_host = msg.header("host").map(str::to_string);
        let tenant_name = msg.tenant.clone();
        let external = msg.source == crate::message::Source::External;
        let method = msg.method.as_str().to_string();
        let path = msg.url.path.clone();
        let trace = msg.trace.clone();
        let principal = msg.principal.clone();

        let (mut resp, err) = match self.dispatch(msg).await {
            Ok(resp) => (resp, None),
            Err(err) => {
                let resp = template.error_response(&err);
                (resp, Some(err))
            }
        };
        if external {
            if let Some(origin) = origin {
                if let Some(tenant) = self.tenants.read().await.get(&tenant_name) {
                    tenant.cors.decorate(&mut resp, &origin, request_host.as_deref());
                }
            }
        }
        // Default caching posture: anything that didn't opt in — errors,
        // discovery docs, preflights, auth responses — is never stored.
        // (304s are exempt: the cached entry's own policy governs them.)
        if resp.header("cache-control").is_none()
            && resp.status != Some(StatusCode::NOT_MODIFIED)
        {
            resp.set_header("cache-control", "no-store");
        }

        // Boundary log (PRD §14): one record per dispatch — external requests
        // and internal hops alike (each its own span, shared trace), severity
        // from the outcome. Errors carry their code/detail.
        self.emit_boundary_log(
            &resp,
            err,
            external,
            &method,
            &path,
            &trace,
            &tenant_name,
            principal.as_ref(),
            start,
        );
        resp
    }

    /// Emit the per-dispatch boundary log. Severity is status-driven (5xx →
    /// Error, 4xx → Warn, external success → Info, internal success → Debug);
    /// server failures (5xx) always emit, everything else obeys the level
    /// floor. Off the hot path only by virtue of `emit` being a channel send.
    #[allow(clippy::too_many_arguments)]
    fn emit_boundary_log(
        &self,
        resp: &Message,
        err: Option<RsError>,
        external: bool,
        method: &str,
        path: &str,
        trace: &crate::message::TraceContext,
        tenant: &str,
        principal: Option<&crate::message::Principal>,
        start: std::time::Instant,
    ) {
        use crate::logging::{LogRecord, Severity};
        // Fast path: no sink configured ⇒ build nothing (keeps the dispatch
        // hot path allocation-free when logging is off).
        if !self.log.enabled() {
            return;
        }
        let status = resp.status.map(|s| s.as_u16()).unwrap_or(0);
        let severity = if status >= 500 {
            Severity::Error
        } else if status >= 400 {
            Severity::Warn
        } else if external {
            Severity::Info
        } else {
            Severity::Debug
        };
        let always = status >= 500;
        if !always && severity < self.log_level {
            return;
        }
        let source = if external { "external" } else { "internal" };
        let mut rec = LogRecord::now(severity, tenant, trace, format!("{method} {path} -> {status}"))
            .attr("http.request.method", method)
            .attr("url.path", path)
            .attr("http.response.status_code", status as i64)
            .attr("rs2.source", source)
            .attr("duration_ms", start.elapsed().as_millis() as i64);
        if let Some(p) = principal {
            rec = rec.attr("enduser.id", p.id.as_str()).attr("rs2.principal.kind", p.kind.as_str());
        }
        if let Some(e) = &err {
            rec = rec
                .attr("error.type", e.code)
                .attr("error.message", e.detail.as_str())
                .attr("rs2.retryable", e.retryable);
        }
        self.log.emit(rec);
    }

    async fn dispatch(&self, mut msg: Message) -> Result<Message, RsError> {
        validate_path(&msg.url.path)?;
        if msg.depth > self.limits.max_depth {
            return Err(RsError::limit_exceeded("call_depth", msg.depth as u64, self.limits.max_depth as u64));
        }
        // Breach circuit breaker (PRD §9.3): a tenant tripping limits
        // repeatedly fails fast here, before holding any node resources.
        self.breaker.check(&msg.tenant)?;
        let tenant = self.tenant(&msg.tenant).await?;

        // CORS (PRD §5.2: external-only): answer permitted preflights
        // without routing, and enforce the cookie-CSRF guard.
        if msg.source == crate::message::Source::External {
            if let Some(origin) = msg.header("origin").map(str::to_string) {
                let request_host = msg.header("host").map(str::to_string);
                if msg.method == http::Method::OPTIONS {
                    if let Some(preflight) =
                        tenant.cors.preflight(&msg, &origin, request_host.as_deref())
                    {
                        return Ok(preflight);
                    }
                }
                tenant.cors.check_cookie_csrf(&msg, &origin, request_host.as_deref())?;
            }
        }

        // Verify any presented token into a principal (PRD §10.5); a bad
        // token is rejected outright rather than treated as anonymous.
        if msg.principal.is_none() {
            if let Some(secret) =
                tenant.auth.as_ref().and_then(|a| a.get("jwtSecret")).and_then(|v| v.as_str())
            {
                msg.principal = crate::services::auth::principal_from_token(&msg, secret)?;
            }
        }

        // The discovery surface (PRD §12) is generated, not mounted; its
        // documents are already filtered by the caller's read permission.
        if crate::discovery::is_discovery_path(&msg.url.path) {
            return crate::discovery::handle(&tenant, msg).await;
        }

        let mount = tenant
            .mounts
            .route(&msg.url.path)
            .ok_or_else(|| RsError::not_found(format!("no service mounted at '{}'", msg.url.path)))?;
        msg.url.apply_mount(&mount.base_path);

        check_access(&msg, mount)?;

        // OPTIONS is a read-only capability probe (G3): describe the resolved
        // mount — pattern, facets, schema hint — with an `Allow` header, so a
        // generic client can render any path from one round trip. A permitted
        // CORS preflight was already answered above, before routing.
        if msg.method == http::Method::OPTIONS {
            let desc = crate::discovery::describe_mount(mount);
            let mut resp = msg.ok_json(&desc);
            resp.set_header("allow", &crate::discovery::allowed_methods(mount).join(", "));
            return Ok(resp);
        }

        check_declared_body_size(&msg, &self.limits)?;
        let _permit = self.limiter.admit(&msg.tenant, self.limits.tenant_concurrency).await?;

        let (service, ctx) = tenant
            .instance(&mount.base_path)
            .ok_or_else(|| RsError::internal("mount has no built instance"))?;
        let (service, ctx) = (service.clone(), ctx.clone());

        // Caching policy (v1's universal `caching` config, host-applied):
        // resolved per mount, applied to successful responses below.
        let cache_policy = ctx.cache_policy.clone();
        let openly_readable = ctx.cache_openly_readable;

        // Idempotency-Key handling (PRD §7.2): dedupe + replay around the
        // service invocation, scoped tenant + mount + method + path.
        let idem_key = msg.header("idempotency-key").map(str::to_string);
        let result = if let Some(key) = idem_key {
            if key.len() > idempotency::MAX_KEY_LEN {
                return Err(RsError::bad_request(format!(
                    "Idempotency-Key exceeds {} characters",
                    idempotency::MAX_KEY_LEN
                )));
            }
            let scope = idempotency::scope_for(&msg, &mount.base_path);
            let hash = idempotency::payload_hash(&msg);
            let store = self.adapters.idempotency.clone();
            match store.begin(&scope, &key, hash.as_deref()).await? {
                idempotency::Begin::Replay(stored) => Ok(stored.into_message(&msg)),
                idempotency::Begin::InFlight => {
                    let mut err = RsError::conflict(
                        "a request with this Idempotency-Key is still executing",
                    );
                    err.retryable = true;
                    err.retry_after_ms = Some(1000);
                    Err(err)
                }
                idempotency::Begin::PayloadMismatch => Err(RsError::idempotency_key_reuse(
                    "Idempotency-Key was already used with a different request payload",
                )),
                idempotency::Begin::Fresh => {
                    match self.invoke(service, ctx, msg).await {
                        Ok(resp) => {
                            let (resp, stored) =
                                idempotency::capture_response(resp, idempotency::DEFAULT_BODY_CAP)
                                    .await;
                            match stored {
                                Some(s) => store.complete(&scope, &key, s).await?,
                                None => store.abandon(&scope, &key).await?,
                            }
                            Ok(resp)
                        }
                        Err(err) => {
                            store.abandon(&scope, &key).await?;
                            Err(err)
                        }
                    }
                }
            }
        } else {
            self.invoke(service, ctx, msg).await
        };

        // Apply the mount's caching policy to successful responses that
        // didn't set their own Cache-Control (Set-Cookie responses are
        // exempt inside `apply`; errors get the catch-all `no-store`).
        result.map(|mut resp| {
            if resp.is_ok() {
                cache_policy.apply(&mut resp, openly_readable);
            }
            resp
        })
    }

    async fn invoke(
        &self,
        service: Arc<dyn crate::services::Service>,
        ctx: Arc<crate::services::ServiceContext>,
        msg: Message,
    ) -> Result<Message, RsError> {
        let tenant_name = msg.tenant.clone();
        let wall = self.limits.wall_clock_service;
        let result = match tokio::time::timeout(wall, service.handle(msg, &ctx)).await {
            Ok(result) => result,
            Err(_) => Err(RsError::limit_exceeded(
                "wall_clock_ms",
                wall.as_millis() as u64,
                wall.as_millis() as u64,
            )),
        };
        // Resource-limit breaches feed the tenant's circuit breaker;
        // admission rejections and breaker trips themselves do not.
        if let Err(err) = &result {
            if err.code == crate::error::codes::LIMIT_EXCEEDED {
                let limit = err
                    .extra
                    .as_ref()
                    .and_then(|e| e.get("limit"))
                    .and_then(|l| l.as_str());
                if !matches!(limit, Some("tenant_concurrency") | Some("tenant_breaker")) {
                    self.breaker.record_breach(&tenant_name, &self.limits);
                }
            }
        }
        result
    }
}

// ---- scheduler loop (G1) -------------------------------------------------

/// One armed schedule's mutable timing state.
struct SchedEntry {
    schedule: crate::scheduler::Schedule,
    state: SchedState,
}

enum SchedState {
    Interval { last_fired: tokio::time::Instant },
    Cron { next_due: time::OffsetDateTime },
}

impl SchedEntry {
    /// If a new occurrence is due, advance the cursor and return its
    /// deterministic occurrence-id (ms); else `None`.
    fn due_occurrence(
        &mut self,
        now: tokio::time::Instant,
        now_wall: time::OffsetDateTime,
    ) -> Option<i64> {
        match (&self.schedule, &mut self.state) {
            (crate::scheduler::Schedule::Interval(every), SchedState::Interval { last_fired }) => {
                if now.saturating_duration_since(*last_fired) >= *every {
                    *last_fired = now;
                    Some(crate::scheduler::interval_bucket_ms(now_wall, *every))
                } else {
                    None
                }
            }
            (crate::scheduler::Schedule::Cron(cron), SchedState::Cron { next_due }) => {
                if now_wall >= *next_due {
                    let occ = (next_due.unix_timestamp_nanos() / 1_000_000) as i64;
                    *next_due = cron
                        .next_occurrence_after(now_wall)
                        .unwrap_or(now_wall + time::Duration::days(366));
                    Some(occ)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// How long a claim is remembered — a couple of periods, floored at 60s.
    fn claim_ttl(&self) -> std::time::Duration {
        match &self.schedule {
            crate::scheduler::Schedule::Interval(every) => {
                (*every * 2).max(std::time::Duration::from_secs(60))
            }
            crate::scheduler::Schedule::Cron(_) => std::time::Duration::from_secs(120),
        }
    }
}

/// Clears a mount's in-flight marker on drop (panic-safe overlap guard).
struct InFlightGuard {
    set: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<(String, String)>>>,
    key: (String, String),
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.key);
        }
    }
}

/// The detached scheduler task: two cadences (reconcile + due-check); exits when
/// the runtime drops (the `Weak` no longer upgrades).
async fn scheduler_loop(
    weak: std::sync::Weak<Runtime>,
    tick: std::time::Duration,
    reconcile: std::time::Duration,
) {
    use std::collections::{HashMap, HashSet};
    let mut desired: HashMap<(String, String), SchedEntry> = HashMap::new();
    let mut config_versions: HashMap<String, String> = HashMap::new();
    let in_flight = std::sync::Arc::new(std::sync::Mutex::new(HashSet::<(String, String)>::new()));
    let mut last_reconcile: Option<tokio::time::Instant> = None;

    loop {
        tokio::time::sleep(tick).await;
        let Some(rt) = weak.upgrade() else { return };

        if last_reconcile.map_or(true, |t| t.elapsed() >= reconcile) {
            last_reconcile = Some(tokio::time::Instant::now());
            reconcile_schedules(&rt, &mut desired, &mut config_versions).await;
        }

        let now = tokio::time::Instant::now();
        let now_wall = time::OffsetDateTime::now_utc();
        for (key, entry) in desired.iter_mut() {
            let Some(occurrence_ms) = entry.due_occurrence(now, now_wall) else { continue };
            // Overlap guard: skip while this mount's previous fire is running.
            {
                let mut set = in_flight.lock().unwrap();
                if set.contains(key) {
                    continue;
                }
                set.insert(key.clone());
            }
            // Cluster dedupe: only the claim winner fires this occurrence.
            let claim_key = format!("{}|{}", key.0, key.1);
            let won = rt
                .adapters
                .schedule
                .claim(&claim_key, occurrence_ms, entry.claim_ttl())
                .await
                .unwrap_or(false);
            if !won {
                in_flight.lock().unwrap().remove(key);
                continue;
            }
            let rt2 = rt.clone();
            let guard = InFlightGuard { set: in_flight.clone(), key: key.clone() };
            let (tenant, base) = key.clone();
            tokio::spawn(async move {
                let _guard = guard; // removes the in-flight marker on completion/panic
                fire_tick(&rt2, &tenant, &base).await;
            });
        }
        drop(rt); // release the strong ref so the runtime can drop between ticks
    }
}

/// Re-derive the desired schedule set from each watched tenant's config (cheap
/// `load_raw`, skipped on unchanged version). Per-tenant errors skip, never kill.
async fn reconcile_schedules(
    rt: &std::sync::Arc<Runtime>,
    desired: &mut std::collections::HashMap<(String, String), SchedEntry>,
    config_versions: &mut std::collections::HashMap<String, String>,
) {
    let watched = watched_tenants(rt).await;
    for tenant in &watched {
        let (cfg, version) = match rt.loader.load_raw(tenant).await {
            Ok(cv) => cv,
            Err(_) => continue,
        };
        if config_versions.get(tenant).map(|v| v == &version).unwrap_or(false) {
            continue;
        }
        desired.retain(|(t, _), _| t != tenant);
        let now = tokio::time::Instant::now();
        let now_wall = time::OffsetDateTime::now_utc();
        if let Some(mounts) = cfg.get("mounts").and_then(|m| m.as_array()) {
            for mount in mounts {
                let base = mount.get("path").and_then(|p| p.as_str()).unwrap_or("");
                let Some(sched_cfg) = mount.get("config").and_then(|c| c.get("schedule")) else {
                    continue;
                };
                let Ok(schedule) = crate::scheduler::Schedule::from_config(sched_cfg) else {
                    continue; // already rejected at PUT /raw; defensive
                };
                let state = match &schedule {
                    crate::scheduler::Schedule::Interval(_) => {
                        SchedState::Interval { last_fired: now }
                    }
                    crate::scheduler::Schedule::Cron(cron) => {
                        match cron.next_occurrence_after(now_wall) {
                            Some(next_due) => SchedState::Cron { next_due },
                            None => continue, // never matches in a year; skip
                        }
                    }
                };
                desired.insert((tenant.clone(), base.to_string()), SchedEntry { schedule, state });
            }
        }
        config_versions.insert(tenant.clone(), version);
    }
    desired.retain(|(t, _), _| watched.contains(t));
    config_versions.retain(|t, _| watched.contains(t));
}

/// The tenants the scheduler watches: the tenancy-derived set plus whatever the
/// loader can enumerate.
async fn watched_tenants(rt: &std::sync::Arc<Runtime>) -> Vec<String> {
    let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
    match &rt.tenancy {
        Tenancy::Single { tenant } => {
            set.insert(tenant.clone());
        }
        Tenancy::Multi { domain_map, .. } => {
            for t in domain_map.values() {
                set.insert(t.clone());
            }
        }
    }
    if let Ok(list) = rt.loader.list_tenants().await {
        set.extend(list);
    }
    set.into_iter().collect()
}

/// Build and dispatch the synthetic internal tick (`POST` + `x-rs2-trigger`).
async fn fire_tick(rt: &std::sync::Arc<Runtime>, tenant: &str, base_path: &str) {
    let _ = rt.handle(crate::scheduler::tick_message(tenant, base_path)).await;
}
