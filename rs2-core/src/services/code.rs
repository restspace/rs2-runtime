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

use super::{Service, ServiceContext};

/// Storage prefix for deployed code in the tenant's file store.
pub const CODE_PREFIX: &str = ".rs2-code";

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
    /// "*.x.com"] }` allows outbound HTTP to matching hosts (PRD §9.2).
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

#[async_trait]
impl Service for CodeService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let code = self.load_code(ctx).await?;
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
                    let host = Arc::new(
                        crate::contract::GrantedHost::new(
                            self.grants(ctx)?,
                            ctx.limits.outbound_calls,
                            self.state.clone(),
                            &service_ref,
                        )
                        .with_log_context(log_ctx),
                    );
                    return self
                        .wasm_engine
                        .invoke(
                            &ServiceCode::WasmComponent(_bytes.clone()),
                            msg,
                            &ctx.config,
                            host,
                            &ctx.limits,
                        )
                        .await;
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
                    let host = Arc::new(
                        crate::contract::GrantedHost::new(
                            self.grants(ctx)?,
                            ctx.limits.outbound_calls,
                            self.state.clone(),
                            &service_ref,
                        )
                        .with_log_context(log_ctx),
                    );
                    return self
                        .js_engine
                        .invoke(
                            &ServiceCode::JsBundle(_source.clone()),
                            msg,
                            &ctx.config,
                            host,
                            &ctx.limits,
                        )
                        .await;
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
