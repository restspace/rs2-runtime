//! Engine-backed custom services (PRD §10.6, §11): mounts referencing
//! `code:<name>@<version>` run a deployed Wasm component in the sandbox.
//!
//! Deployed bundles are content-addressed and immutable per version
//! (PRD §14), stored in the tenant's file store under `.rs2-code/`.
//! Capability grants come from the mount config (default deny):
//!
//! ```json
//! { "grants": { "orders": { "prefix": "/data/orders" } } }
//! ```
//!
//! A grant maps a capability name the guest may call (`host.request("orders",
//! …)`) to an internal URL prefix; the host rewrites and dispatches through
//! the full wrapper path, so authz/limits/idempotency apply.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::contract::CapabilityTarget;
use crate::error::RsError;
use crate::message::{Message, Source};
use crate::outbound::{host_matches, url_host};

use super::{FileService, Service, ServiceContext};

/// Storage prefix for deployed code in the tenant's file store.
pub const CODE_PREFIX: &str = ".rs2-code";

/// Storage prefix for `store` grants (service-private storage), sibling to
/// the other reserved `.rs2-*` trees.
pub const STORE_GRANT_PREFIX: &str = ".rs2-store";

/// Response header naming a granted capability read to attach as the body
/// (`<capability>:<path>`), resolved host-side after the guest returns.
pub const BODY_REF_HEADER: &str = "x-rs2-body-ref";

/// Request header stamped by the host before invoking a guest: the matched
/// mount prefix, so the guest can derive its mount-relative sub-path from
/// `msg.url` without hard-coding where it is mounted.
pub const BASE_PATH_HEADER: &str = "x-rs2-base-path";

/// Content-addressed version of a bundle (PRD §14).
pub fn version_of(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub fn code_path(name: &str, version: &str) -> String {
    format!("{CODE_PREFIX}/{name}/{version}.wasm")
}

pub fn code_path_js(name: &str, version: &str) -> String {
    format!("{CODE_PREFIX}/{name}/{version}.js")
}

/// A deployed bundle, dispatched to the engine that can run it (PRD §5.3).
pub enum LoadedCode {
    Wasm(Arc<Vec<u8>>),
    Js(Arc<String>),
}

pub struct CodeService {
    name: String,
    version: String,
    #[cfg(feature = "wasm")]
    wasm_engine: crate::engines::wasm::WasmEngine,
    #[cfg(feature = "js")]
    js_engine: crate::engines::js::JsEngine,
    #[cfg_attr(not(any(feature = "wasm", feature = "js")), allow(dead_code))]
    state: Arc<tokio::sync::RwLock<HashMap<String, Vec<u8>>>>,
    code: tokio::sync::OnceCell<LoadedCode>,
}

impl CodeService {
    /// Parse a `code:<name>@<version>` service reference.
    pub fn from_ref(service_ref: &str) -> Result<Self, RsError> {
        let rest = service_ref
            .strip_prefix("code:")
            .ok_or_else(|| RsError::bad_request("not a code: service reference"))?;
        let (name, version) = rest.split_once('@').ok_or_else(|| {
            RsError::bad_request(format!(
                "code reference '{service_ref}' must be 'code:<name>@<version>'"
            ))
        })?;
        if name.is_empty() || version.is_empty() || name.contains(['/', '\\', '.']) {
            return Err(RsError::bad_request(format!(
                "invalid code reference '{service_ref}'"
            )));
        }
        Ok(CodeService {
            name: name.to_string(),
            version: version.to_string(),
            #[cfg(feature = "wasm")]
            wasm_engine: crate::engines::wasm::WasmEngine::new()?,
            #[cfg(feature = "js")]
            js_engine: crate::engines::js::JsEngine::new(),
            state: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            code: tokio::sync::OnceCell::new(),
        })
    }

    async fn load_code(&self, ctx: &ServiceContext) -> Result<&LoadedCode, RsError> {
        self.code
            .get_or_try_init(|| async {
                let files = ctx
                    .files
                    .as_ref()
                    .ok_or_else(|| RsError::internal("code service has no file capability"))?;
                let cap = ctx.limits.materialized_body_bytes;
                if let Ok(mut body) = files
                    .read(&code_path(&self.name, &self.version), None)
                    .await
                {
                    let bytes = body.materialize(cap).await?;
                    return Ok(LoadedCode::Wasm(Arc::new(bytes.to_vec())));
                }
                if let Ok(mut body) = files
                    .read(&code_path_js(&self.name, &self.version), None)
                    .await
                {
                    let bytes = body.materialize(cap).await?;
                    let text = String::from_utf8(bytes.to_vec()).map_err(|_| {
                        RsError::contract_violation("deployed JS bundle is not valid UTF-8")
                    })?;
                    return Ok(LoadedCode::Js(Arc::new(text)));
                }
                Err(RsError::not_found(format!(
                    "deployed code '{}@{}' not found — deploy it via PUT /code/{}",
                    self.name, self.version, self.name
                )))
            })
            .await
    }

    /// Build the default-deny capability table from the mount's `grants`.
    /// Grant kinds: `{ "prefix": "/data/orders" }` scopes internal dispatch
    /// under a URL prefix; `{ "type": "httpOut", "hosts": ["api.x.com",
    /// "*.x.com"] }` allows outbound HTTP to matching hosts (PRD §9.2);
    /// `{ "type": "store", "root": "cache" }` is service-private storage —
    /// a full store surface over a private root (below), no mount involved.
    #[cfg_attr(not(any(feature = "wasm", feature = "js")), allow(dead_code))]
    fn grants(&self, ctx: &ServiceContext) -> Result<HashMap<String, CapabilityTarget>, RsError> {
        let mut grants: HashMap<String, CapabilityTarget> = HashMap::new();
        let Some(config_grants) = ctx.config.get("grants").and_then(|g| g.as_object()) else {
            return Ok(grants);
        };
        let requester = ctx
            .requester
            .clone()
            .ok_or_else(|| RsError::internal("code service has no requester capability"))?;
        for (capability, grant) in config_grants {
            if grant.get("type").and_then(|t| t.as_str()) == Some("store") {
                grants.insert(
                    capability.clone(),
                    store_grant_target(capability, grant, ctx)?,
                );
                continue;
            }
            if grant.get("type").and_then(|t| t.as_str()) == Some("httpOut") {
                let hosts: Vec<String> = grant
                    .get("hosts")
                    .and_then(|h| h.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                if hosts.is_empty() {
                    return Err(RsError::bad_request(format!(
                        "httpOut grant '{capability}' requires a non-empty 'hosts' allowlist"
                    )));
                }
                let http = ctx.http.clone().ok_or_else(|| {
                    RsError::engine_unavailable(
                        "this deployment has no outbound HTTP adapter configured",
                    )
                })?;
                // Host-side credential injection (resolved at tenant build, never
                // in `config`): applied after the allowlist check, before the
                // request leaves the host. Absent ⇒ headers pass through verbatim.
                let injector = ctx.outbound_injectors.get(capability).cloned();
                let body_cap = ctx.limits.materialized_body_bytes;
                let target: CapabilityTarget = Arc::new(move |mut msg: Message| {
                    let http = http.clone();
                    let hosts = hosts.clone();
                    let injector = injector.clone();
                    Box::pin(async move {
                        let host = url_host(&msg.url.path).ok_or_else(|| {
                            RsError::bad_request(format!(
                                "outbound call needs an absolute URL, got '{}'",
                                msg.url.path
                            ))
                        })?;
                        if !hosts.iter().any(|pattern| host_matches(pattern, &host)) {
                            return Err(RsError::capability_denied(&format!(
                                "httpOut to '{host}'"
                            )));
                        }
                        if let Some(inj) = &injector {
                            inj.apply(&mut msg, body_cap).await?;
                        }
                        http.request(msg).await
                    })
                });
                grants.insert(capability.clone(), target);
                continue;
            }

            let prefix = grant
                .get("prefix")
                .and_then(|p| p.as_str())
                .ok_or_else(|| {
                    RsError::bad_request(format!("grant '{capability}' requires a 'prefix'"))
                })?
                .trim_end_matches('/')
                .to_string();
            let requester = requester.clone();
            let target: CapabilityTarget = Arc::new(move |mut msg: Message| {
                let requester = requester.clone();
                let prefix = prefix.clone();
                Box::pin(async move {
                    // Scope the call under the granted prefix; the rewritten
                    // request re-enters full dispatch (authz, limits,
                    // idempotency all apply — PRD §9.2).
                    let sub_path = msg.url.path.trim_start_matches('/');
                    let path = if sub_path.is_empty() {
                        prefix.clone()
                    } else {
                        format!("{prefix}/{sub_path}")
                    };
                    let query = msg.url.query.clone();
                    msg.url = crate::message::MsgUrl::parse(&if query.is_empty() {
                        path
                    } else {
                        format!("{path}?{query}")
                    });
                    msg.source = Source::Internal;
                    Ok(requester.request(msg).await)
                })
            });
            grants.insert(capability.clone(), target);
        }
        Ok(grants)
    }
}

/// Build the target for a `{"type": "store", "root": "..."}` grant:
/// **service-private storage**. A capability call is handled by a private
/// [`FileService`] over the tenant file store rooted at
/// `.rs2-store/<root>` — never routed through a mount, so no principal is
/// involved: the operator-configured grant *is* the authority (unlike a
/// `prefix` grant, which re-enters dispatch under the caller's identity).
/// The guest gets the full store contract — listings, ETags,
/// `If-Match`/`If-None-Match`, keyless POST, the `?confirm=` guard — and
/// two mounts granting the same root deliberately share storage.
#[cfg_attr(not(any(feature = "wasm", feature = "js")), allow(dead_code))]
fn store_grant_target(
    capability: &str,
    grant: &serde_json::Value,
    ctx: &ServiceContext,
) -> Result<CapabilityTarget, RsError> {
    let root = crate::capabilities::sanitized_store_root(grant)?.ok_or_else(|| {
        RsError::bad_request(format!(
            "store grant '{capability}' requires a non-empty relative 'root'"
        ))
    })?;
    let files = ctx
        .files
        .clone()
        .ok_or_else(|| RsError::internal("code service has no file capability"))?
        .prefixed(&format!("{STORE_GRANT_PREFIX}/{root}"));
    let inner_ctx = Arc::new(ServiceContext {
        config: serde_json::json!({}),
        files: Some(files),
        data: None,
        query: None,
        messaging: None,
        http: None,
        cache_policy: crate::wrapper::CachePolicy::default(),
        cache_openly_readable: true,
        cors: Arc::new(crate::wrapper::CorsPolicy::default()),
        limits: ctx.limits.clone(),
        requester: None,
        control: None,
        tenant_retry: None,
        operator_roles: None,
        pipeline_wall_clock: std::time::Duration::from_secs(120),
        logger: ctx.logger.clone(),
        log_store: None,
        catalogue: None,
        builtin_adapters: None,
        infras: None,
        secrets: None,
        outbound_injectors: std::collections::HashMap::new(),
    });
    let inner = Arc::new(FileService::new());
    let target: CapabilityTarget = Arc::new(move |msg: Message| {
        let inner = inner.clone();
        let inner_ctx = inner_ctx.clone();
        Box::pin(async move {
            // Failures become status responses, as for a `prefix` grant —
            // the guest sees one shape either way.
            let template = msg.response(http::StatusCode::OK, None);
            // Direct service call, no router — apply its path safety here
            // (traversal in a guest path must not escape the private root).
            if let Err(e) = crate::router::validate_path(&msg.url.path) {
                return Ok(template.error_response(&e));
            }
            Ok(inner
                .handle(msg, &inner_ctx)
                .await
                .unwrap_or_else(|e| template.error_response(&e)))
        })
    });
    Ok(target)
}

/// Resolve a [`BODY_REF_HEADER`] on a guest response: read the named path
/// through the mount's own grant **after** the guest returned, and attach the
/// result as the response body. Cache hits and passthrough thereby stream
/// host-side — no image/file bytes ever cross the sandbox boundary. The
/// original caller's principal is carried, so a `prefix`-grant read keeps
/// exactly the authz it would have had from inside the guest; `store` grants
/// ignore it.
#[cfg_attr(not(any(feature = "wasm", feature = "js")), allow(dead_code))]
async fn resolve_body_ref(
    mut resp: Message,
    grants: &HashMap<String, CapabilityTarget>,
    tenant: &str,
    principal: Option<crate::message::Principal>,
    trace: crate::message::TraceContext,
    depth: u16,
) -> Result<Message, RsError> {
    let Some(refval) = resp.header(BODY_REF_HEADER).map(str::to_string) else {
        return Ok(resp);
    };
    resp.headers.remove(BODY_REF_HEADER);
    let (capability, path) = refval.split_once(':').ok_or_else(|| {
        RsError::contract_violation(format!(
            "{BODY_REF_HEADER} must be '<capability>:<path>', got '{refval}'"
        ))
    })?;
    if resp.body.is_some() {
        return Err(RsError::contract_violation(format!(
            "response carries both a body and {BODY_REF_HEADER}"
        )));
    }
    let target = grants
        .get(capability)
        .ok_or_else(|| RsError::capability_denied(capability))?;
    let mut get = Message::request(http::Method::GET, path, tenant);
    get.principal = principal;
    get.trace = trace.child();
    get.depth = depth.saturating_add(1);
    get.source = Source::Internal;
    let body_resp = target(get).await?;
    if !body_resp.is_ok() || body_resp.body.is_none() {
        return Err(RsError::contract_violation(format!(
            "{BODY_REF_HEADER} read '{capability}:{path}' returned status {}",
            body_resp.status.map(|s| s.as_u16()).unwrap_or(0)
        )));
    }
    resp.body = body_resp.body;
    Ok(resp)
}

#[async_trait]
impl Service for CodeService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let code = self.load_code(ctx).await?;
        let base = if msg.url.base_path.is_empty() {
            "/".to_string()
        } else {
            msg.url.base_path.clone()
        };
        msg.set_header(BASE_PATH_HEADER, &base);
        // Captured before `msg` moves into an engine: what a post-return
        // `x-rs2-body-ref` resolution needs to issue the read.
        #[cfg_attr(not(any(feature = "wasm", feature = "js")), allow(unused_variables))]
        let (tenant, principal, trace, depth) = (
            msg.tenant.clone(),
            msg.principal.clone(),
            msg.trace.clone(),
            msg.depth,
        );
        match code {
            LoadedCode::Wasm(_bytes) => {
                #[cfg(feature = "wasm")]
                {
                    use crate::contract::{Engine, ServiceCode};
                    let service_ref = format!("{}@{}", self.name, self.version);
                    let log_ctx = crate::contract::LogContext {
                        sink: ctx.logger.sink(),
                        tenant: msg.tenant.clone(),
                        mount: msg.url.base_path.clone(),
                        service: service_ref.clone(),
                        trace_id: msg.trace.trace_id.clone(),
                        span_id: msg.trace.span_id.clone(),
                    };
                    let grants = self.grants(ctx)?;
                    let host = Arc::new(
                        crate::contract::GrantedHost::new(
                            grants.clone(),
                            ctx.limits.outbound_calls,
                            self.state.clone(),
                            &service_ref,
                        )
                        .with_log_context(log_ctx),
                    );
                    let resp = self
                        .wasm_engine
                        .invoke(
                            &ServiceCode::WasmComponent(_bytes.clone()),
                            msg,
                            &ctx.config,
                            host,
                            &ctx.limits,
                        )
                        .await?;
                    return resolve_body_ref(resp, &grants, &tenant, principal, trace, depth).await;
                }
                #[cfg(not(feature = "wasm"))]
                {
                    let _ = msg;
                    Err(RsError::engine_unavailable(format!(
                        "code:{}@{} is a wasm component but this build has no wasm engine \
                         (rebuild with --features wasm)",
                        self.name, self.version
                    )))
                }
            }
            LoadedCode::Js(_source) => {
                #[cfg(feature = "js")]
                {
                    use crate::contract::{Engine, ServiceCode};
                    let service_ref = format!("{}@{}", self.name, self.version);
                    let log_ctx = crate::contract::LogContext {
                        sink: ctx.logger.sink(),
                        tenant: msg.tenant.clone(),
                        mount: msg.url.base_path.clone(),
                        service: service_ref.clone(),
                        trace_id: msg.trace.trace_id.clone(),
                        span_id: msg.trace.span_id.clone(),
                    };
                    let grants = self.grants(ctx)?;
                    let host = Arc::new(
                        crate::contract::GrantedHost::new(
                            grants.clone(),
                            ctx.limits.outbound_calls,
                            self.state.clone(),
                            &service_ref,
                        )
                        .with_log_context(log_ctx),
                    );
                    let resp = self
                        .js_engine
                        .invoke(
                            &ServiceCode::JsBundle(_source.clone()),
                            msg,
                            &ctx.config,
                            host,
                            &ctx.limits,
                        )
                        .await?;
                    return resolve_body_ref(resp, &grants, &tenant, principal, trace, depth).await;
                }
                #[cfg(not(feature = "js"))]
                {
                    let _ = msg;
                    Err(RsError::engine_unavailable(format!(
                        "code:{}@{} is a JS bundle but this build has no JS engine \
                         (rebuild with --features js)",
                        self.name, self.version
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_code_refs() {
        let svc = CodeService::from_ref("code:stripe-wrapper@abc123").unwrap();
        assert_eq!(svc.name, "stripe-wrapper");
        assert_eq!(svc.version, "abc123");
        assert!(CodeService::from_ref("code:noversion").is_err());
        assert!(CodeService::from_ref("code:bad/name@v").is_err());
        assert!(CodeService::from_ref("file").is_err());
    }

    #[test]
    fn versions_are_content_addressed() {
        assert_eq!(version_of(b"abc"), version_of(b"abc"));
        assert_ne!(version_of(b"abc"), version_of(b"abd"));
        assert_eq!(version_of(b"abc").len(), 16);
    }
}
