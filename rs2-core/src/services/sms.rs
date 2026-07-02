//! `sms` service: the canonical HTTP surface over the [`SmsGateway`] capability
//! (PRD §9.2). The provider is swappable per mount via `store.adapter`
//! (`code:<name>@<version>` or `infra:<name>`); the service is provider-agnostic
//! and runs unchanged over Twilio, AWS SNS, … — only the adapter differs.
//!
//! - `POST /<mount>/send` with `{ "to": "+1…", "body": "…" }` → `201 { "id": … }`
//! - `GET  /<mount>/status/<id>` → `200` provider-shaped delivery status.

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use super::{Service, ServiceContext};
use crate::error::RsError;
use crate::message::Message;

#[derive(Default)]
pub struct SmsService;

impl SmsService {
    pub fn new() -> Self {
        SmsService
    }
}

#[async_trait]
impl Service for SmsService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let sms = ctx.sms.as_ref().ok_or_else(|| RsError::capability_denied("sms"))?;

        let method = msg.method.clone();
        let segs: Vec<String> = msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        let parts: Vec<&str> = segs.iter().map(String::as_str).collect();

        match (&method, parts.as_slice()) {
            (&Method::POST, ["send"]) => {
                let body = msg
                    .body
                    .as_mut()
                    .ok_or_else(|| RsError::bad_request("POST /send requires a JSON body {to, body}"))?;
                let payload = body.as_json(ctx.limits.materialized_body_bytes).await?;
                let to = payload
                    .get("to")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RsError::bad_request("'to' (string) is required"))?;
                let text = payload
                    .get("body")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RsError::bad_request("'body' (string) is required"))?;
                let id = sms.send(to, text).await?;
                Ok(msg.response(StatusCode::CREATED, Some(crate::message::Body::from_json(&json!({ "id": id })))))
            }
            (&Method::GET, ["status", id]) => {
                let status = sms.status(id).await?;
                Ok(msg.ok_json(&status))
            }
            _ => Err(RsError::bad_request(
                "sms endpoint: POST /send {to, body}, GET /status/{id}",
            )),
        }
    }
}
