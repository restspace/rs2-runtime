//! `services` — the self-configuration API (PRD §10.6): a tenant's control
//! surface. Reads the catalogue and current config; `PUT /raw` validates the
//! entire new config, persists it, and hot-swaps the tenant atomically.

use async_trait::async_trait;

use crate::error::RsError;
use crate::message::Message;

use super::{Service, ServiceContext};

pub struct ServicesService;

impl ServicesService {
    pub fn new() -> Self {
        ServicesService
    }
}

impl Default for ServicesService {
    fn default() -> Self {
        Self::new()
    }
}

/// Catalogue of available services (PRD §10.6). Static for the built-ins;
/// custom `code:` services join via deployment (M3).
fn catalogue() -> serde_json::Value {
    serde_json::json!({
        "services": [
            { "name": "file", "description": "Streamed file storage (PRD §10.1)",
              "configSchema": { "type": "object", "properties": {} } },
            { "name": "data", "description": "Schema-validated JSON store (PRD §10.2)",
              "configSchema": { "type": "object", "properties": {
                  "enforceSchema": { "type": "boolean" } } } },
            { "name": "pipeline", "description": "Pipeline-as-a-service (PRD §10.3)",
              "configSchema": { "type": "object", "required": ["pipeline"], "properties": {
                  "pipeline": {}, "retry": { "type": "object" } } } },
            { "name": "auth", "description": "Authentication & RBAC (PRD §10.5)",
              "configSchema": { "type": "object", "properties": {
                  "userDataset": { "type": "string" } } } },
            { "name": "services", "description": "Self-configuration API (PRD §10.6)",
              "configSchema": { "type": "object", "properties": {} } }
        ]
    })
}

#[async_trait]
impl Service for ServicesService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let segments: Vec<String> =
            msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        let segments: Vec<&str> = segments.iter().map(|s| s.as_str()).collect();
        let control = ctx
            .control
            .clone()
            .ok_or_else(|| RsError::internal("services service has no control capability"))?;

        match (&msg.method, segments.as_slice()) {
            (&http::Method::GET, ["catalogue"]) => Ok(msg.ok_json(&catalogue())),

            // Exposed view of current mounts.
            (&http::Method::GET, ["services"]) => {
                let (config, _) = control.raw_config(&msg.tenant).await?;
                let mounts: Vec<serde_json::Value> = config
                    .get("mounts")
                    .and_then(|m| m.as_array())
                    .map(|ms| {
                        ms.iter()
                            .map(|m| {
                                serde_json::json!({
                                    "path": m.get("path"),
                                    "service": m.get("service"),
                                    "access": m.get("config").and_then(|c| c.get("access")),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(msg.ok_json(&serde_json::json!({ "mounts": mounts })))
            }

            (&http::Method::GET, ["raw"]) => {
                let (config, version) = control.raw_config(&msg.tenant).await?;
                let mut resp = msg.ok_json(&config);
                resp.set_header("etag", &format!("\"{version}\""));
                Ok(resp)
            }

            // Full config replace: validate → persist → atomic swap
            // (PRD §10.6). `If-Match` gives optimistic concurrency.
            (&http::Method::PUT, ["raw"]) => {
                let body = match &mut msg.body {
                    Some(b) => b.as_json(ctx.limits.materialized_body_bytes).await?,
                    None => return Err(RsError::bad_request("PUT /raw requires a JSON body")),
                };
                let if_match = msg
                    .header("if-match")
                    .map(|v| v.trim().trim_matches('"').to_string());
                let version = control.put_config(&msg.tenant, body, if_match.as_deref()).await?;
                let mut resp = msg.no_content();
                resp.set_header("etag", &format!("\"{version}\""));
                Ok(resp)
            }

            _ => Err(RsError::not_found(format!(
                "services endpoint '{}' (have: GET catalogue/services/raw, PUT raw)",
                msg.url.service_path
            ))),
        }
    }
}
