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

/// Per-mount authorization (PRD §10.5 role model). The mount's `access`
/// config is either a string policy (`"open"` / `"authenticated"`) or a
/// role-spec object:
///
/// ```json
/// { "readRoles": "all", "writeRoles": "A E", "createRoles": "A E U",
///   "manageRoles": "A" }
/// ```
///
/// Role-spec strings are space-separated tokens: `all`, `authenticated`,
/// role letters/names, and path-scoped grants — a role token followed by a
/// path pattern (`"U /user/{email}"`) grants that role only on matching
/// paths, with `{email}` substituted from the principal id.
///
/// Internal calls with no principal are trusted composition (the original
/// principal propagates when present); CORS-style external-only concerns
/// never apply, but role specs do whenever a principal is attached.
pub fn check_access(msg: &Message, mount_config: &serde_json::Value) -> Result<(), RsError> {
    let access = match mount_config.get("access") {
        None => return Ok(()),
        Some(a) => a,
    };
    match access {
        serde_json::Value::String(s) => match s.as_str() {
            "open" => Ok(()),
            "authenticated" => {
                if msg.principal.is_some() || msg.source == Source::Internal {
                    Ok(())
                } else {
                    Err(RsError::unauthorized("this mount requires authentication"))
                }
            }
            other => Err(RsError::internal(format!("unknown access policy '{other}'"))),
        },
        serde_json::Value::Object(spec) => {
            if msg.principal.is_none() && msg.source == Source::Internal {
                return Ok(());
            }
            let write = spec.get("writeRoles").and_then(|v| v.as_str()).unwrap_or("A");
            let role_spec = match msg.method {
                http::Method::GET | http::Method::HEAD | http::Method::OPTIONS => {
                    spec.get("readRoles").and_then(|v| v.as_str()).unwrap_or("all")
                }
                http::Method::POST => {
                    spec.get("createRoles").and_then(|v| v.as_str()).unwrap_or(write)
                }
                _ => write,
            };
            if satisfies_role_spec(role_spec, msg) {
                Ok(())
            } else if msg.principal.is_none() {
                Err(RsError::unauthorized("this mount requires authentication"))
            } else {
                Err(RsError::forbidden(format!(
                    "principal lacks a role satisfying '{role_spec}' for {}",
                    msg.method
                )))
            }
        }
        _ => Err(RsError::internal("invalid 'access' config")),
    }
}

/// Evaluate a role-spec string against the message's principal and path.
fn satisfies_role_spec(spec: &str, msg: &Message) -> bool {
    let principal = msg.principal.as_ref();
    let tokens: Vec<&str> = spec.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let role = tokens[i];
        // A role token may be followed by a path pattern scoping it.
        let path_pattern = tokens.get(i + 1).filter(|t| t.starts_with('/'));
        let step = if path_pattern.is_some() { 2 } else { 1 };
        let role_ok = match role {
            "all" => true,
            "authenticated" => principal.is_some(),
            r => principal.is_some_and(|p| p.roles.iter().any(|have| have == r)),
        };
        if role_ok {
            match path_pattern {
                None => return true,
                Some(pattern) => {
                    let resolved = match principal {
                        Some(p) => pattern.replace("{email}", &p.id),
                        None => pattern.to_string(),
                    };
                    let path = &msg.url.service_path;
                    if path == &resolved || path.starts_with(&format!("{resolved}/")) {
                        return true;
                    }
                }
            }
        }
        i += step;
    }
    false
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
