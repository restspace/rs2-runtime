//! Outbound-call gating shared by every surface that egresses HTTP (PRD §9.2):
//! host-allowlist matching (`code:` httpOut grants, the catalogue client) and
//! [`ExternalDispatch`] — the pipeline/wrapper executor's external-call path,
//! built from a mount's `httpOut` grants.

use std::collections::HashMap;
use std::sync::Arc;

use crate::adapters::CredentialInjector;
use crate::capabilities::HttpOut;
use crate::error::RsError;
use crate::message::Message;

/// Host of an absolute URL ("https://api.x.com:8443/v1" → "api.x.com").
pub(crate) fn url_host(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host = authority.rsplit('@').next()?.split(':').next()?;
    if host.is_empty() {
        None
    } else {
        Some(host.to_ascii_lowercase())
    }
}

/// Whether a resolved call URL leaves the node: an absolute `http(s)://` URL
/// (case-insensitive scheme). Other schemes fall through to internal dispatch,
/// where path validation rejects them.
pub(crate) fn is_external_url(url: &str) -> bool {
    let lower_starts = |prefix: &str| {
        url.len() >= prefix.len() && url[..prefix.len()].eq_ignore_ascii_case(prefix)
    };
    lower_starts("http://") || lower_starts("https://")
}

/// Allowlist matching: exact host or a `*.suffix` wildcard.
pub(crate) fn host_matches(pattern: &str, host: &str) -> bool {
    let pattern = pattern.to_ascii_lowercase();
    if let Some(suffix) = pattern.strip_prefix("*.") {
        host == suffix || host.ends_with(&format!(".{suffix}"))
    } else {
        host == pattern
    }
}

/// One `httpOut` grant from a mount's config: an allowed-host list plus the
/// optional credential injector resolved for it at tenant build.
pub(crate) struct HostGrant {
    pub name: String,
    pub hosts: Vec<String>,
    pub injector: Option<Arc<CredentialInjector>>,
}

/// The pipeline executor's external-call capability: the union of a mount's
/// `httpOut` grants over the node's outbound adapter. The allowlist is checked
/// before any I/O; the matched grant's injector applies its credential
/// host-side on the way out (never visible in spec content or to guests).
pub(crate) struct ExternalDispatch {
    /// `None` ⇒ grants exist but the deployment has no adapter — surfaced as
    /// `engine_unavailable` at call time, matching the `code:` grant precedent.
    http: Option<Arc<dyn HttpOut>>,
    /// Sorted by grant name so first-match injector selection is deterministic
    /// (JSON object order is not).
    grants: Vec<HostGrant>,
    /// Bound on body materialization for signing injectors (hmac/awsSigV4).
    max_body_bytes: u64,
}

impl ExternalDispatch {
    /// Build from a mount's config `grants` block. `None` when the mount has
    /// no `httpOut` grants — external calls are then denied outright.
    pub fn from_mount(
        config: &serde_json::Value,
        http: Option<Arc<dyn HttpOut>>,
        injectors: &HashMap<String, Arc<CredentialInjector>>,
        max_body_bytes: u64,
    ) -> Option<Self> {
        let config_grants = config.get("grants")?.as_object()?;
        let mut grants: Vec<HostGrant> = config_grants
            .iter()
            .filter(|(_, g)| g.get("type").and_then(|t| t.as_str()) == Some("httpOut"))
            .map(|(name, g)| HostGrant {
                name: name.clone(),
                hosts: g
                    .get("hosts")
                    .and_then(|h| h.as_array())
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                    .unwrap_or_default(),
                injector: injectors.get(name).cloned(),
            })
            .collect();
        if grants.is_empty() {
            return None;
        }
        grants.sort_by(|a, b| a.name.cmp(&b.name));
        Some(ExternalDispatch { http, grants, max_body_bytes })
    }

    /// Whether `host` is covered by any grant's allowlist (no I/O, no errors —
    /// the `?$plan` static-coverage check).
    pub fn covers(&self, host: &str) -> bool {
        self.grants.iter().any(|g| g.hosts.iter().any(|p| host_matches(p, host)))
    }

    /// Allowlist check for an absolute URL — no I/O. Returns the index of the
    /// first matching grant (grant-name order); `capability_denied` when no
    /// grant covers the host, `engine_unavailable` when no adapter is wired.
    pub fn authorize(&self, url: &str) -> Result<usize, RsError> {
        let host = url_host(url).ok_or_else(|| {
            RsError::bad_request(format!("outbound call needs an absolute URL, got '{url}'"))
        })?;
        let grant = self
            .grants
            .iter()
            .position(|g| g.hosts.iter().any(|p| host_matches(p, &host)))
            .ok_or_else(|| {
                let allowed: Vec<&str> =
                    self.grants.iter().flat_map(|g| g.hosts.iter().map(String::as_str)).collect();
                RsError::capability_denied(&format!(
                    "httpOut to '{host}' (this mount's httpOut grants allow: {})",
                    allowed.join(", ")
                ))
            })?;
        if self.http.is_none() {
            return Err(RsError::engine_unavailable(
                "this deployment has no outbound HTTP adapter configured",
            ));
        }
        Ok(grant)
    }

    /// Inject the matched grant's credential (if any) and dispatch through the
    /// outbound adapter. Transport errors propagate as `Err` with the
    /// adapter's `retryable` flag intact, so the retry loop can act on them.
    pub async fn send(&self, grant: usize, mut msg: Message) -> Result<Message, RsError> {
        let http = self.http.as_ref().ok_or_else(|| {
            RsError::engine_unavailable("this deployment has no outbound HTTP adapter configured")
        })?;
        // Inside the node a body's media type travels on the `Body` itself and
        // headers are never consulted, but on the wire only headers exist —
        // default `Content-Type` from the body so a forwarded/shaped body
        // isn't sent untyped, plus the standard headers picky upstreams
        // require (GitHub 403s UA-less requests). Explicit spec headers win;
        // `Host`/`Content-Length` stay with the transport.
        if let Some(body) = &msg.body {
            if !msg.headers.contains_key(http::header::CONTENT_TYPE) {
                if let Ok(v) = http::HeaderValue::from_str(&body.media_type.to_string()) {
                    msg.headers.insert(http::header::CONTENT_TYPE, v);
                }
            }
        }
        if !msg.headers.contains_key(http::header::ACCEPT) {
            msg.headers.insert(http::header::ACCEPT, http::HeaderValue::from_static("*/*"));
        }
        if !msg.headers.contains_key(http::header::USER_AGENT) {
            msg.headers.insert(
                http::header::USER_AGENT,
                http::HeaderValue::from_static(concat!("rs2/", env!("CARGO_PKG_VERSION"))),
            );
        }
        if let Some(inj) = &self.grants[grant].injector {
            inj.apply(&mut msg, self.max_body_bytes).await?;
        }
        http.request(msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_allowlist_matching() {
        assert_eq!(url_host("https://api.stripe.com/v1/charges").as_deref(), Some("api.stripe.com"));
        assert_eq!(url_host("https://api.x.com:8443/v1?q=1").as_deref(), Some("api.x.com"));
        assert_eq!(url_host("/relative/path"), None);
        assert!(host_matches("api.stripe.com", "api.stripe.com"));
        assert!(host_matches("*.stripe.com", "api.stripe.com"));
        assert!(host_matches("*.stripe.com", "stripe.com"));
        assert!(!host_matches("*.stripe.com", "evil-stripe.com"));
        assert!(!host_matches("api.stripe.com", "api.stripe.com.evil.io"));
    }

    #[test]
    fn authorize_matches_deny_and_missing_adapter() {
        let grants = serde_json::json!({ "grants": {
            "b-wild": { "type": "httpOut", "hosts": ["*.example.com"] },
            "a-exact": { "type": "httpOut", "hosts": ["api.stripe.com"] },
            "internal": { "prefix": "/data" }
        }});
        let ext =
            ExternalDispatch::from_mount(&grants, None, &HashMap::new(), 1024).unwrap();
        // Sorted by grant name: a-exact first.
        assert_eq!(ext.grants[0].name, "a-exact");
        assert!(ext.covers("api.stripe.com"));
        assert!(ext.covers("sub.example.com"));
        assert!(!ext.covers("evil.io"));
        // Match without an adapter ⇒ 501; no match ⇒ 403; relative ⇒ 400.
        assert_eq!(ext.authorize("https://api.stripe.com/v1").unwrap_err().status, 501);
        let denied = ext.authorize("https://evil.io/x").unwrap_err();
        assert_eq!(denied.status, 403);
        assert!(denied.detail.contains("api.stripe.com"), "{}", denied.detail);
        assert_eq!(ext.authorize("/relative").unwrap_err().status, 400);
    }

    #[test]
    fn no_http_out_grants_means_none() {
        assert!(ExternalDispatch::from_mount(
            &serde_json::json!({ "grants": { "d": { "prefix": "/data" } } }),
            None,
            &HashMap::new(),
            1024
        )
        .is_none());
        assert!(ExternalDispatch::from_mount(
            &serde_json::json!({}),
            None,
            &HashMap::new(),
            1024
        )
        .is_none());
    }
}
