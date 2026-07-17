//! External service/adapter catalogues (control plane).
//!
//! A [`CatalogueClient`] is a HOST capability — not a sandbox grant — handed
//! only to the `services` self-config mount. It fetches catalogue documents and
//! code bundles from operator-allowlisted hosts (an SSRF bound), reusing the
//! outbound [`HttpOut`] adapter. The registered catalogue list itself lives in
//! tenant config; this client only performs the bounded fetches.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::capabilities::HttpOut;
use crate::error::RsError;
use crate::message::Message;

/// A catalogue document served at a registered URL: a list of installable items.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogueDoc {
    #[serde(default)]
    pub items: Vec<CatalogueItem>,
}

/// One installable service or adapter advertised by a catalogue. Mirrors the
/// local `manifest.json` shape plus the catalogue fields (`version` content
/// hash + `bundleUrl`).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogueItem {
    pub name: String,
    /// `"service"` or `"adapter"`.
    pub kind: String,
    /// For `kind == "adapter"`: `"data" | "file" | "query"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_kind: Option<String>,
    /// `"js"` or `"wasm"`.
    pub engine: String,
    /// Content hash (`version_of`) the fetched bundle must match.
    pub version: String,
    /// Absolute URL serving the bundle bytes.
    pub bundle_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub endpoints: serde_json::Value,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub capabilities: serde_json::Value,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub config_schema: serde_json::Value,
}

/// Host-side catalogue fetch capability (operator-bounded). Injected only into
/// the `services` mount's context.
#[async_trait]
pub trait CatalogueClient: Send + Sync {
    /// Fetch and parse a catalogue document.
    async fn fetch_catalogue(&self, url: &str) -> Result<CatalogueDoc, RsError>;
    /// Fetch raw bundle bytes.
    async fn fetch_bundle(&self, url: &str) -> Result<bytes::Bytes, RsError>;
    /// Whether `url`'s host is on the operator allowlist (no I/O).
    fn host_allowed(&self, url: &str) -> bool;
}

/// The real client: a bounded outbound `GET` over [`HttpOut`], gated by the
/// operator host-allowlist (wildcard patterns, reusing `outbound::host_matches`)
/// before any I/O.
pub struct HttpCatalogueClient {
    http: Arc<dyn HttpOut>,
    hosts: Vec<String>,
}

impl HttpCatalogueClient {
    pub fn new(http: Arc<dyn HttpOut>, hosts: Vec<String>) -> Self {
        HttpCatalogueClient { http, hosts }
    }

    fn check_host(&self, url: &str) -> Result<(), RsError> {
        let host = crate::outbound::url_host(url)
            .ok_or_else(|| RsError::bad_request(format!("catalogue URL '{url}' has no host")))?;
        if self.hosts.iter().any(|p| crate::outbound::host_matches(p, &host)) {
            Ok(())
        } else {
            Err(RsError::capability_denied(&format!("catalogue fetch to '{host}'")))
        }
    }

    async fn get_bytes(&self, url: &str) -> Result<bytes::Bytes, RsError> {
        self.check_host(url)?;
        let req = Message::request(http::Method::GET, url, "-");
        let mut resp = self.http.request(req).await?;
        let status = resp.status.unwrap_or(http::StatusCode::BAD_GATEWAY);
        if !status.is_success() {
            return Err(RsError::new(
                502,
                crate::error::codes::CONTRACT_VIOLATION,
                "Catalogue Fetch Failed",
                format!("fetching '{url}' returned {}", status.as_u16()),
            ));
        }
        match &mut resp.body {
            Some(b) => Ok(b.materialize(64 * 1024 * 1024).await?.clone()),
            None => Ok(bytes::Bytes::new()),
        }
    }
}

#[async_trait]
impl CatalogueClient for HttpCatalogueClient {
    async fn fetch_catalogue(&self, url: &str) -> Result<CatalogueDoc, RsError> {
        let bytes = self.get_bytes(url).await?;
        serde_json::from_slice(&bytes).map_err(|e| {
            RsError::bad_request(format!("catalogue '{url}' is not a valid catalogue document: {e}"))
        })
    }

    async fn fetch_bundle(&self, url: &str) -> Result<bytes::Bytes, RsError> {
        self.get_bytes(url).await
    }

    fn host_allowed(&self, url: &str) -> bool {
        self.check_host(url).is_ok()
    }
}
