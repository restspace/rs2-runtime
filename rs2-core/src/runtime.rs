//! The runtime: lazy tenant loading and the dispatch path (PRD §5.2).
//!
//! `handle` never returns an error — every failure becomes a structured
//! problem+json response attributed to the tenant and trace.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use crate::error::RsError;
use crate::message::Message;
use crate::router::{validate_path, Tenancy};
use crate::tenant::{Adapters, Tenant, TenantConfig};
use crate::wrapper::{check_access, check_declared_body_size, LimitTable, TenantLimiter};

/// Source of tenant configs ("config store" — PRD §13). The server binary
/// supplies a file-backed loader; embedders supply their own.
#[async_trait]
pub trait ConfigLoader: Send + Sync {
    async fn load_tenant(&self, tenant: &str) -> Result<TenantConfig, RsError>;
}

pub struct Runtime {
    tenancy: Tenancy,
    adapters: Adapters,
    loader: Arc<dyn ConfigLoader>,
    limits: LimitTable,
    limiter: TenantLimiter,
    tenants: tokio::sync::RwLock<HashMap<String, Arc<Tenant>>>,
    /// Serializes tenant builds so concurrent first requests load once
    /// (re-entrance guard, PRD §9.1).
    load_guard: tokio::sync::Mutex<()>,
}

impl Runtime {
    pub fn new(tenancy: Tenancy, adapters: Adapters, loader: Arc<dyn ConfigLoader>, limits: LimitTable) -> Self {
        Runtime {
            tenancy,
            adapters,
            loader,
            limits,
            limiter: TenantLimiter::new(),
            tenants: tokio::sync::RwLock::new(HashMap::new()),
            load_guard: tokio::sync::Mutex::new(()),
        }
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
        let tenant = Arc::new(Tenant::build(name, config, &self.adapters, &self.limits)?);
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
        let tenant = self.tenant(&msg.tenant).await?;
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

        let wall = self.limits.wall_clock_service;
        match tokio::time::timeout(wall, service.handle(msg, ctx)).await {
            Ok(result) => result,
            Err(_) => Err(RsError::limit_exceeded(
                "wall_clock_ms",
                wall.as_millis() as u64,
                wall.as_millis() as u64,
            )),
        }
    }
}
