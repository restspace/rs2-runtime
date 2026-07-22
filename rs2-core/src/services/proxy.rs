//! `proxy` service (PRD §9.2, the v1 "proxy adapter" use case): forward a
//! request to a fixed external `target`, attaching operator-supplied auth
//! **host-side** on the way out. The credential is resolved at tenant build from
//! the mount's `inject` ref (operator `infra:` or granted `secret:`) and never
//! appears in the mount config or reaches any guest.
//!
//! ```json
//! { "path": "/stripe", "service": "proxy", "config": {
//!     "target": "https://api.stripe.com",
//!     "inject": "infra:stripe-key" } }
//! ```
//!
//! `GET /stripe/v1/charges?x=1` → `GET https://api.stripe.com/v1/charges?x=1`
//! with the credential applied. The destination is fixed by `target`, so there
//! is no host allowlist to choose — the mount *is* the allowlist.

use async_trait::async_trait;

use super::{Service, ServiceContext};
use crate::error::RsError;
use crate::message::Message;

/// Reserved key under which `Tenant::build` stores a proxy mount's resolved
/// credential injector (a proxy mount has no `grants`, so this can't collide).
pub(crate) const PROXY_INJECTOR_KEY: &str = "proxy";

#[derive(Default)]
pub struct ProxyService;

impl ProxyService {
    pub fn new() -> Self {
        ProxyService
    }
}

#[async_trait]
impl Service for ProxyService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let http = ctx.http.as_ref().ok_or_else(|| {
            RsError::engine_unavailable("this deployment has no outbound HTTP adapter configured")
        })?;
        let target = ctx
            .config
            .get("target")
            .and_then(|t| t.as_str())
            .ok_or_else(|| RsError::bad_request("proxy mount requires a 'target' base URL"))?
            .trim_end_matches('/');

        // target + the remaining path (+ query) — the host stays the target's.
        let mut url = format!("{target}{}", msg.url.service_path);
        if !msg.url.query.is_empty() {
            url.push('?');
            url.push_str(&msg.url.query);
        }

        let mut out = Message::request(msg.method.clone(), &url, &msg.tenant);
        // Forward the client's headers, but strip anything that must not reach
        // the upstream target: the inbound Host (the transport sets it from the
        // target), the caller's *own* credentials to this gateway (RS2 reads
        // `Authorization: Bearer` and the `rs-auth` cookie — see `auth.rs`), and
        // hop-by-hop headers that only describe the client↔gateway connection.
        // The injector adds the target's own auth next.
        out.headers = msg.headers.clone();
        for h in [
            http::header::HOST,
            http::header::AUTHORIZATION,
            http::header::COOKIE,
            http::header::CONNECTION,
            http::header::TRANSFER_ENCODING,
            http::header::PROXY_AUTHORIZATION,
        ] {
            out.headers.remove(h);
        }
        out.body = msg.body.take();

        if let Some(inj) = ctx.outbound_injectors.get(PROXY_INJECTOR_KEY) {
            inj.apply(&mut out, ctx.limits.materialized_body_bytes)
                .await?;
        }

        http.request(out).await
    }
}
