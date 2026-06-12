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

    /// Handle a message; failures become problem+json responses.
    pub async fn handle(&self, msg: Message) -> Message {
        // Capture enough context to build an error response after `msg` moves.
        let template = msg.response(StatusCode::OK, None);
        match self.dispatch(msg).await {
            Ok(resp) => resp,
            Err(err) => template.error_response(&err),
        }
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

        check_access(&msg, &mount.config)?;
        check_declared_body_size(&msg, &self.limits)?;
        let _permit = self.limiter.admit(&msg.tenant, self.limits.tenant_concurrency).await?;

        let (service, ctx) = tenant
            .instance(&mount.base_path)
            .ok_or_else(|| RsError::internal("mount has no built instance"))?;
        let (service, ctx) = (service.clone(), ctx.clone());

        // Idempotency-Key handling (PRD §7.2): dedupe + replay around the
        // service invocation, scoped tenant + mount + method + path.
        let idem_key = msg.header("idempotency-key").map(str::to_string);
        if let Some(key) = idem_key {
            if key.len() > idempotency::MAX_KEY_LEN {
                return Err(RsError::bad_request(format!(
                    "Idempotency-Key exceeds {} characters",
                    idempotency::MAX_KEY_LEN
                )));
            }
            let scope = idempotency::scope_for(&msg, &mount.base_path);
            let hash = idempotency::payload_hash(&msg);
            let store = self.adapters.idempotency.clone();
            return match store.begin(&scope, &key, hash.as_deref()).await? {
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
            };
        }

        self.invoke(service, ctx, msg).await
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
