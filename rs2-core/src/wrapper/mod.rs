//! Per-mount wrapper concerns (PRD §5.2): authn/authz (M1 stub), limits
//! admission, and structured error mapping. Idempotency, CORS, and caching
//! land in M2 — the dispatch order is already shaped for them.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::contract::InvocationLimits;
use crate::error::RsError;
use crate::message::{Message, Source};

/// Limit table (PRD §9.3 defaults). Operator ceilings; tenant-configurable
/// downward (the downward-merge lands with tenant config in M2 self-config).
#[derive(Debug, Clone)]
pub struct LimitTable {
    pub wall_clock_service: Duration,
    pub wall_clock_pipeline: Duration,
    pub memory_bytes: u64,
    pub materialized_body_bytes: u64,
    pub tenant_concurrency: usize,
    pub outbound_calls: u32,
    pub max_depth: u16,
}

impl Default for LimitTable {
    fn default() -> Self {
        LimitTable {
            wall_clock_service: Duration::from_secs(30),
            wall_clock_pipeline: Duration::from_secs(120),
            memory_bytes: 128 * 1024 * 1024,
            materialized_body_bytes: 100 * 1024 * 1024,
            tenant_concurrency: 64,
            outbound_calls: 64,
            max_depth: 16,
        }
    }
}

impl LimitTable {
    pub fn invocation_limits(&self) -> InvocationLimits {
        InvocationLimits {
            wall_clock: self.wall_clock_service,
            memory_bytes: self.memory_bytes,
            outbound_calls: self.outbound_calls,
            materialized_body_bytes: self.materialized_body_bytes,
        }
    }
}

/// Per-tenant concurrency admission. Node-local (PRD §9.3).
#[derive(Default)]
pub struct TenantLimiter {
    semaphores: RwLock<HashMap<String, Arc<Semaphore>>>,
}

impl TenantLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to admit an invocation for a tenant; `Err(limit_exceeded)` when
    /// the tenant is at its concurrency cap (no queueing — fail fast so a
    /// flooded tenant cannot hold node resources hostage).
    pub async fn admit(&self, tenant: &str, cap: usize) -> Result<OwnedSemaphorePermit, RsError> {
        let sem = {
            let read = self.semaphores.read().await;
            read.get(tenant).cloned()
        };
        let sem = match sem {
            Some(s) => s,
            None => {
                let mut write = self.semaphores.write().await;
                write
                    .entry(tenant.to_string())
                    .or_insert_with(|| Arc::new(Semaphore::new(cap)))
                    .clone()
            }
        };
        sem.try_acquire_owned()
            .map_err(|_| RsError::limit_exceeded("tenant_concurrency", cap as u64, cap as u64))
    }
}

/// Authn/authz stub for M1. The full `auth` service (JWT verification, role
/// specs, RBAC) is M2; this enforces the one policy M1 config can express:
/// `"access": "authenticated"` on a mount requires a principal.
pub fn check_access(msg: &Message, mount_config: &serde_json::Value) -> Result<(), RsError> {
    let access = mount_config.get("access").and_then(|v| v.as_str()).unwrap_or("open");
    match access {
        "open" => Ok(()),
        "authenticated" => {
            if msg.principal.is_some() || msg.source == Source::Internal {
                Ok(())
            } else {
                Err(RsError::unauthorized("this mount requires authentication"))
            }
        }
        other => Err(RsError::internal(format!("unknown access policy '{other}'"))),
    }
}

/// Body-size admission from the declared Content-Length, before any read.
pub fn check_declared_body_size(msg: &Message, limits: &LimitTable) -> Result<(), RsError> {
    if let Some(body) = &msg.body {
        if let Some(size) = body.size {
            // Streamed bodies may exceed the materialization cap when they
            // flow to stores without materializing; the cap is enforced at
            // materialization time (Body::materialize). Here we only reject
            // sizes beyond any plausible handling.
            const ABSOLUTE_CAP: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB
            if size > ABSOLUTE_CAP {
                return Err(RsError::limit_exceeded("request_body_bytes", size, ABSOLUTE_CAP));
            }
        }
    }
    let _ = limits;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    #[tokio::test]
    async fn concurrency_admission_fails_fast_at_cap() {
        let limiter = TenantLimiter::new();
        let _p1 = limiter.admit("t1", 2).await.unwrap();
        let _p2 = limiter.admit("t1", 2).await.unwrap();
        let err = limiter.admit("t1", 2).await.unwrap_err();
        assert_eq!(err.code, crate::error::codes::LIMIT_EXCEEDED);
        // Other tenants are unaffected.
        assert!(limiter.admit("t2", 2).await.is_ok());
        // Releasing a permit re-admits.
        drop(_p1);
        assert!(limiter.admit("t1", 2).await.is_ok());
    }

    #[test]
    fn access_stub_requires_principal() {
        let msg = Message::request(Method::GET, "/x", "t1");
        assert!(check_access(&msg, &serde_json::json!({})).is_ok());
        assert!(check_access(&msg, &serde_json::json!({"access": "authenticated"})).is_err());
        let mut authed = Message::request(Method::GET, "/x", "t1");
        authed.principal = Some(crate::message::Principal {
            id: "u1".into(),
            roles: vec!["U".into()],
            kind: "user".into(),
        });
        assert!(check_access(&authed, &serde_json::json!({"access": "authenticated"})).is_ok());
    }
}
