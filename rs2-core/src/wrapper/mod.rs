//! Per-mount wrapper concerns (PRD §5.2): authn/authz (M1 stub), limits
//! admission, and structured error mapping. Idempotency, CORS, and caching
//! land in M2 — the dispatch order is already shaped for them.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::contract::InvocationLimits;
use crate::error::RsError;
use crate::message::{Message, Principal, Source};

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
    /// Circuit breaker (PRD §9.3 breach behavior): this many resource-limit
    /// breaches within the window trip the tenant open for the cooldown.
    pub breaker_threshold: u32,
    pub breaker_window: Duration,
    pub breaker_cooldown: Duration,
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
            breaker_threshold: 8,
            breaker_window: Duration::from_secs(10),
            breaker_cooldown: Duration::from_secs(5),
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

/// Caching policy (per mount, host-applied — v1's universal `caching`
/// config). The default everywhere is **`no-store`**: RS2 responses are
/// tenant-scoped and principal-filtered, so nothing caches unless a mount
/// deliberately opts in. Applied only when the response carries no
/// `Cache-Control` of its own (so composed services can pass upstream
/// headers through), never to error responses, and never to responses
/// that set cookies.
#[derive(Debug, Clone, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct CachePolicy {
    pub mode: CacheMode,
    pub max_age_seconds: u64,
    /// `public` is honored only on mounts anonymously readable (`access`
    /// absent, `"open"`, or `read: "all"`); otherwise the policy is
    /// clamped to `private` + `Vary: Authorization, Cookie` — a shared
    /// cache must never serve one principal's response to another.
    pub public: bool,
    pub immutable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum CacheMode {
    /// Never stored anywhere (the default).
    #[default]
    NoStore,
    /// May be stored but must revalidate (`no-cache`) — pairs with the
    /// store services' conditional-GET 304s: always fresh, bandwidth saved.
    Revalidate,
    /// Cached for `maxAgeSeconds`.
    Cache,
}

impl CachePolicy {
    pub fn from_config(value: Option<&serde_json::Value>) -> CachePolicy {
        value
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }

    /// Whether a mount's read surface is anonymously readable (the
    /// precondition for honoring `public`).
    pub fn mount_is_openly_readable(mount_config: &serde_json::Value) -> bool {
        match mount_config.get("access") {
            None => true,
            Some(serde_json::Value::String(s)) => s == "open",
            Some(serde_json::Value::Object(spec)) => {
                spec.get("read").and_then(|v| v.as_str()).unwrap_or("all") == "all"
            }
            _ => false,
        }
    }

    /// Apply to a successful response lacking its own `Cache-Control`.
    pub fn apply(&self, resp: &mut Message, openly_readable: bool) {
        if resp.header("cache-control").is_some()
            || resp.headers.contains_key(http::header::SET_COOKIE)
        {
            return;
        }
        let clamped = self.public && !openly_readable;
        let scope = if self.public && !clamped { "public" } else { "private" };
        let value = match self.mode {
            CacheMode::NoStore => "no-store".to_string(),
            CacheMode::Revalidate => format!("{scope}, no-cache"),
            CacheMode::Cache => {
                let immutable = if self.immutable { ", immutable" } else { "" };
                format!("{scope}, max-age={}{immutable}", self.max_age_seconds)
            }
        };
        resp.set_header("cache-control", &value);
        if (clamped || !self.public) && self.mode != CacheMode::NoStore {
            append_header_value(resp, "vary", "authorization, cookie");
        }
    }
}

/// Comma-append to a header without clobbering existing values (Vary is
/// written by both CORS and caching).
pub fn append_header_value(resp: &mut Message, name: &'static str, value: &str) {
    let merged = match resp.header(name) {
        Some(existing) if !existing.is_empty() => {
            let has = existing
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case(value.trim()));
            if has {
                return;
            }
            format!("{existing}, {value}")
        }
        _ => value.to_string(),
    };
    resp.set_header(name, &merged);
}

/// CORS policy (per tenant, host-enforced — carried over from v1 where the
/// runtime enforced `trustedDomains`). Trusted origins receive credentialed
/// CORS and may send cookie-authenticated unsafe requests; allowed origins
/// receive plain CORS (bearer-token browser apps); everything else gets no
/// CORS headers. Same-origin requests never involve this policy.
#[derive(Debug, Clone, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct CorsPolicy {
    /// Credentialed CORS + cross-site cookies (`SameSite=None; Secure`).
    pub trusted_origins: Vec<String>,
    /// Non-credentialed CORS.
    pub allowed_origins: Vec<String>,
}

/// Response headers other than the CORS-safelisted set that browsers may
/// read from RS2 responses.
const EXPOSED_HEADERS: &str =
    "etag, location, link, x-total-count, x-trace-id, idempotency-replayed, retry-after";

/// Match an origin (`scheme://host[:port]`) against a pattern: `*`, a full
/// origin, a bare hostname, or a `*.suffix` host wildcard (matching the
/// apex too).
pub fn origin_matches(pattern: &str, origin: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    let origin = origin.to_ascii_lowercase();
    if pattern == "*" {
        return true;
    }
    if pattern.contains("://") {
        return pattern.trim_end_matches('/') == origin.trim_end_matches('/');
    }
    // Host-level patterns compare against the origin's host (port ignored
    // unless the pattern carries one).
    let host_port = origin.split_once("://").map(|(_, hp)| hp).unwrap_or(&origin);
    let host = host_port.split(':').next().unwrap_or(host_port);
    if let Some(suffix) = pattern.strip_prefix("*.") {
        return host == suffix || host.ends_with(&format!(".{suffix}"));
    }
    if pattern.contains(':') {
        return host_port == pattern;
    }
    host == pattern
}

/// Whether the Origin header names the same host the request was sent to —
/// in which case CORS does not apply at all.
pub fn is_same_origin(origin: &str, request_host: Option<&str>) -> bool {
    let Some(request_host) = request_host else { return false };
    let origin_host = origin.split_once("://").map(|(_, hp)| hp).unwrap_or(origin);
    origin_host.eq_ignore_ascii_case(request_host.trim())
}

impl CorsPolicy {
    pub fn from_config(value: Option<&serde_json::Value>) -> CorsPolicy {
        value
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }

    pub fn is_trusted(&self, origin: &str) -> bool {
        self.trusted_origins.iter().any(|p| origin_matches(p, origin))
    }

    pub fn is_allowed(&self, origin: &str) -> bool {
        self.is_trusted(origin) || self.allowed_origins.iter().any(|p| origin_matches(p, origin))
    }

    /// Add CORS response headers for a permitted cross-origin caller.
    /// No-op for same-origin or unpermitted origins.
    pub fn decorate(&self, resp: &mut Message, origin: &str, request_host: Option<&str>) {
        if is_same_origin(origin, request_host) || !self.is_allowed(origin) {
            return;
        }
        resp.set_header("access-control-allow-origin", origin);
        append_header_value(resp, "vary", "origin");
        resp.set_header("access-control-expose-headers", EXPOSED_HEADERS);
        if self.is_trusted(origin) {
            resp.set_header("access-control-allow-credentials", "true");
        }
    }

    /// Answer a preflight (`OPTIONS` + `Origin` + request-method header)
    /// from a permitted origin; `None` lets the request route normally.
    pub fn preflight(&self, msg: &Message, origin: &str, request_host: Option<&str>) -> Option<Message> {
        if is_same_origin(origin, request_host) || !self.is_allowed(origin) {
            return None;
        }
        let requested_method = msg.header("access-control-request-method")?;
        let mut resp = msg.response(http::StatusCode::NO_CONTENT, None);
        resp.set_header("access-control-allow-origin", origin);
        append_header_value(&mut resp, "vary", "origin");
        resp.set_header("access-control-allow-methods", requested_method);
        let allow_headers = msg
            .header("access-control-request-headers")
            .unwrap_or("authorization, content-type, idempotency-key, if-match");
        resp.set_header("access-control-allow-headers", allow_headers);
        resp.set_header("access-control-max-age", "86400");
        if self.is_trusted(origin) {
            resp.set_header("access-control-allow-credentials", "true");
        }
        Some(resp)
    }

    /// The CSRF guard (v1 rule, host-enforced): a cookie-authenticated
    /// **unsafe** request from a cross-site, untrusted origin is rejected
    /// before routing. Bearer-token and non-browser requests are unaffected.
    pub fn check_cookie_csrf(&self, msg: &Message, origin: &str, request_host: Option<&str>) -> Result<(), RsError> {
        if is_same_origin(origin, request_host) || self.is_trusted(origin) {
            return Ok(());
        }
        let unsafe_method =
            !matches!(msg.method, http::Method::GET | http::Method::HEAD | http::Method::OPTIONS);
        let has_auth_cookie = msg
            .header("cookie")
            .map(|c| c.split(';').any(|p| p.trim_start().starts_with("rs-auth=")))
            .unwrap_or(false);
        if unsafe_method && has_auth_cookie {
            return Err(RsError::forbidden(format!(
                "cookie-authenticated cross-origin request from untrusted origin '{origin}' \
                 (add it to cors.trustedOrigins, or use a bearer token)"
            )));
        }
        Ok(())
    }
}

/// Per-tenant circuit breaker (PRD §9.3): repeated resource-limit breaches
/// trip the tenant open — its requests fail fast with 503 + Retry-After
/// instead of re-occupying engine threads and degrading neighbors.
#[derive(Default)]
pub struct TenantBreaker {
    states: std::sync::Mutex<HashMap<String, BreakerState>>,
}

#[derive(Default)]
struct BreakerState {
    recent_breaches: std::collections::VecDeque<std::time::Instant>,
    open_until: Option<std::time::Instant>,
}

impl TenantBreaker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail fast while the tenant's breaker is open.
    pub fn check(&self, tenant: &str) -> Result<(), RsError> {
        let mut states = self.states.lock().unwrap();
        if let Some(state) = states.get_mut(tenant) {
            if let Some(until) = state.open_until {
                let now = std::time::Instant::now();
                if now < until {
                    let mut err = RsError::limit_exceeded("tenant_breaker", 1u64, 0u64);
                    err.detail = format!(
                        "tenant '{tenant}' tripped the limit-breach circuit breaker; retry later"
                    );
                    err.retry_after_ms = Some((until - now).as_millis() as u64);
                    return Err(err);
                }
                // Cooldown elapsed: half-open — clear and allow traffic.
                state.open_until = None;
                state.recent_breaches.clear();
            }
        }
        Ok(())
    }

    /// Record a resource-limit breach; trips the breaker at the threshold.
    pub fn record_breach(&self, tenant: &str, limits: &LimitTable) {
        let mut states = self.states.lock().unwrap();
        let state = states.entry(tenant.to_string()).or_default();
        let now = std::time::Instant::now();
        state.recent_breaches.push_back(now);
        while let Some(front) = state.recent_breaches.front() {
            if now.duration_since(*front) > limits.breaker_window {
                state.recent_breaches.pop_front();
            } else {
                break;
            }
        }
        if state.recent_breaches.len() as u32 >= limits.breaker_threshold {
            state.open_until = Some(now + limits.breaker_cooldown);
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

/// The action a request's HTTP method maps to (PRD §5.2). POST — and any other
/// non-idempotent verb — is the *action* verb (`invoke`): a store's keyless
/// create, a pipeline run, an auth login. PUT/PATCH are the idempotent upsert
/// (`write`); DELETE is `delete`; GET/HEAD/OPTIONS are `read`.
pub(crate) fn action_for(method: &http::Method) -> &'static str {
    match *method {
        http::Method::GET | http::Method::HEAD | http::Method::OPTIONS => "read",
        http::Method::PUT | http::Method::PATCH => "write",
        http::Method::DELETE => "delete",
        _ => "invoke",
    }
}

/// Resolve the role-spec string an action key gates within a role object,
/// applying the default chain: `read`→`"all"`, `write`→`"A"`, and both
/// `delete` and `invoke` default to whatever `write` resolves to.
fn resolve_role<'a>(
    spec: &'a serde_json::Map<String, serde_json::Value>,
    action_key: &str,
) -> &'a str {
    let write = spec.get("write").and_then(|v| v.as_str()).unwrap_or("A");
    match action_key {
        "read" => spec.get("read").and_then(|v| v.as_str()).unwrap_or("all"),
        "delete" => spec.get("delete").and_then(|v| v.as_str()).unwrap_or(write),
        "invoke" => spec.get("invoke").and_then(|v| v.as_str()).unwrap_or(write),
        _ => write,
    }
}

/// Evaluate an `access` value for one action key against the message. The value
/// is either a string policy (`"open"` / `"authenticated"`) or a role-spec
/// object keyed by action (`read`/`write`/`delete`/`invoke`).
///
/// Role-spec strings are space-separated tokens: `all`, `authenticated`, role
/// names, and path-scoped grants — a role token followed by a path pattern
/// (`"U /user/{email}"`) grants that role only on matching paths, with
/// `{email}` substituted from the principal id.
///
/// Shared by the mount-level host check ([`check_access`]) and the pipeline
/// per-spec execution check (`PipelineService`).
pub(crate) fn check_role_spec(
    access: &serde_json::Value,
    action_key: &str,
    msg: &Message,
) -> Result<(), RsError> {
    // Runtime-originated system calls (e.g. scheduler ticks) are trusted: they
    // cannot be forged from the wire and exist only by operator config. Plain
    // internal *composition* (pipeline steps, guest fetch) is NOT trusted — it
    // is authorized by the principal it carries, like any other call.
    if msg.source == Source::System {
        return Ok(());
    }
    match access {
        serde_json::Value::String(s) => match s.as_str() {
            "open" => Ok(()),
            "authenticated" => {
                if msg.principal.is_some() {
                    Ok(())
                } else {
                    Err(RsError::unauthorized("this mount requires authentication"))
                }
            }
            other => Err(RsError::internal(format!("unknown access policy '{other}'"))),
        },
        serde_json::Value::Object(spec) => {
            let role_spec = resolve_role(spec, action_key);
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

/// Per-mount authorization (PRD §5.2 role model). The mount's `access` config
/// gates each request by the action its method maps to ([`action_for`]).
///
/// A **pipeline** mount enforces its *execution* surface per-spec inside the
/// service (the mount `access` is the floor; a matched spec may override it
/// looser or tighter), so the host defers it here. Authoring paths (a
/// dot-prefixed first service segment — the instruction-plane subtree) stay
/// host-enforced against the mount `access` like any other path.
pub fn check_access(msg: &Message, mount: &crate::router::Mount) -> Result<(), RsError> {
    let is_authoring =
        msg.url.service_segments().first().is_some_and(|s| s.starts_with('.'));
    if mount.service == "pipeline" && !is_authoring {
        return Ok(());
    }
    let access = match mount.config.get("access") {
        None => return Ok(()),
        Some(a) => a,
    };
    check_role_spec(access, action_for(&msg.method), msg)
}

/// Whether a principal holds any role designated as a tenant **operator** role
/// (`operatorRoles`, space-separated). Operators are the only principals
/// permitted to change authorization config (a mount's or spec's `access`,
/// role assignment). A path-scoped grant grammar does not apply — operator
/// status is a plain role-membership test.
pub(crate) fn is_operator(principal: Option<&Principal>, operator_roles: &str) -> bool {
    let Some(p) = principal else { return false };
    operator_roles
        .split_whitespace()
        .any(|role| p.roles.iter().any(|have| have == role))
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
    fn origin_patterns_match_origins() {
        assert!(origin_matches("*", "https://x.y"));
        assert!(origin_matches("https://app.acme.com", "https://app.acme.com"));
        assert!(!origin_matches("https://app.acme.com", "http://app.acme.com"), "scheme matters for full-origin patterns");
        assert!(origin_matches("app.acme.com", "https://app.acme.com:8443"), "bare hostname ignores scheme+port");
        assert!(origin_matches("*.acme.dev", "https://x.acme.dev"));
        assert!(origin_matches("*.acme.dev", "https://acme.dev"), "wildcard matches apex");
        assert!(!origin_matches("*.acme.dev", "https://evil-acme.dev"));
        assert!(is_same_origin("https://api.acme.com", Some("api.acme.com")));
        assert!(is_same_origin("https://api.acme.com:8443", Some("api.acme.com:8443")));
        assert!(!is_same_origin("https://other.com", Some("api.acme.com")));
        assert!(!is_same_origin("https://api.acme.com", None));
    }

    #[test]
    fn csrf_guard_blocks_untrusted_cookie_writes_only() {
        let policy = CorsPolicy {
            trusted_origins: vec!["https://app.acme.com".into()],
            allowed_origins: vec![],
        };
        let cookie_post = |origin_ok: bool| {
            let mut msg = Message::request(Method::POST, "/data/x", "t");
            msg.set_header("cookie", "rs-auth=tok");
            let origin = if origin_ok { "https://app.acme.com" } else { "https://evil.com" };
            policy.check_cookie_csrf(&msg, origin, Some("api.acme.com"))
        };
        assert!(cookie_post(true).is_ok(), "trusted origin may write with a cookie");
        assert!(cookie_post(false).is_err(), "untrusted cross-origin cookie write rejected");

        // Reads, bearer auth, and same-origin writes all pass.
        let mut read = Message::request(Method::GET, "/data/x", "t");
        read.set_header("cookie", "rs-auth=tok");
        assert!(policy.check_cookie_csrf(&read, "https://evil.com", Some("api.acme.com")).is_ok());
        let mut bearer = Message::request(Method::POST, "/data/x", "t");
        bearer.set_header("authorization", "Bearer tok");
        assert!(policy.check_cookie_csrf(&bearer, "https://evil.com", Some("api.acme.com")).is_ok());
        let mut same = Message::request(Method::POST, "/data/x", "t");
        same.set_header("cookie", "rs-auth=tok");
        assert!(policy.check_cookie_csrf(&same, "https://api.acme.com", Some("api.acme.com")).is_ok());
    }

    #[test]
    fn breaker_trips_at_threshold_and_recovers() {
        let limits = LimitTable {
            breaker_threshold: 3,
            breaker_window: Duration::from_secs(10),
            breaker_cooldown: Duration::from_millis(20),
            ..LimitTable::default()
        };
        let breaker = TenantBreaker::new();
        assert!(breaker.check("t1").is_ok());
        breaker.record_breach("t1", &limits);
        breaker.record_breach("t1", &limits);
        assert!(breaker.check("t1").is_ok(), "below threshold");
        breaker.record_breach("t1", &limits);
        let err = breaker.check("t1").unwrap_err();
        assert_eq!(err.code, crate::error::codes::LIMIT_EXCEEDED);
        assert!(err.retry_after_ms.is_some());
        // Other tenants unaffected.
        assert!(breaker.check("t2").is_ok());
        // Cooldown elapses → half-open → traffic flows again.
        std::thread::sleep(Duration::from_millis(30));
        assert!(breaker.check("t1").is_ok());
    }

    #[test]
    fn access_stub_requires_principal() {
        let mount = |config| crate::router::Mount {
            base_path: String::new(),
            service: "file".into(),
            config,
        };
        let msg = Message::request(Method::GET, "/x", "t1");
        assert!(check_access(&msg, &mount(serde_json::json!({}))).is_ok());
        assert!(check_access(&msg, &mount(serde_json::json!({"access": "authenticated"}))).is_err());
        let mut authed = Message::request(Method::GET, "/x", "t1");
        authed.principal = Some(crate::message::Principal {
            id: "u1".into(),
            roles: vec!["U".into()],
            kind: "user".into(),
        });
        assert!(
            check_access(&authed, &mount(serde_json::json!({"access": "authenticated"}))).is_ok()
        );
    }
}
