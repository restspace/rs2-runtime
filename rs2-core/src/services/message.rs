//! `message` service: the canonical HTTP surface over the [`MessageGateway`]
//! capability (PRD §9.2). One endpoint for every delivery channel; the provider
//! is swappable per mount via `store.adapter` (one channel) or `store.adapters`
//! (a channel → adapter map), so a tenant mails through one provider and texts
//! through another at the same mount. The service is provider-agnostic and runs
//! unchanged over Cloudflare Email Service, AWS SNS, a `code:` guest bundle, …
//!
//! - `POST /<mount>/send` `{channel, to, …}` → `201 {id, channel, provider}`
//! - `GET  /<mount>/status/<id>` → `200` provider-shaped status, or `501` when
//!   the configured provider does not report delivery status at all
//! - `GET  /<mount>/channels` → what this mount can actually do
//!
//! Sending is a non-idempotent, externally visible effect: a retried `POST`
//! must not send twice. The mount therefore declares the `unsafe` effect class
//! and honours `Idempotency-Key` through the host's idempotency layer — the
//! service itself stays a pure function on the message.

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use super::{Service, ServiceContext};
use crate::capabilities::Outbound;
use crate::error::RsError;
use crate::message::Message;

#[derive(Default)]
pub struct MessageService;

impl MessageService {
    pub fn new() -> Self {
        MessageService
    }
}

#[async_trait]
impl Service for MessageService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let gateway = ctx
            .messaging
            .as_ref()
            .ok_or_else(|| RsError::capability_denied("message"))?;

        let method = msg.method.clone();
        let segs: Vec<String> = msg
            .url
            .service_segments()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let parts: Vec<&str> = segs.iter().map(String::as_str).collect();

        match (&method, parts.as_slice()) {
            (&Method::POST, ["send"]) => {
                let body = msg.body.as_mut().ok_or_else(|| {
                    RsError::bad_request("POST /send requires a JSON body {channel, to, …}")
                })?;
                let payload = body.as_json(ctx.limits.materialized_body_bytes).await?;
                let out = Outbound::from_json(&payload)?;

                // Refuse an unserved channel here rather than at the provider:
                // the mount's own configuration is the answer, and it is a 400
                // that names what this mount can do.
                let channel = out.channel();
                let served = gateway.channels();
                if !served.contains(&channel) {
                    return Err(RsError::bad_request(format!(
                        "this mount has no adapter for the '{channel}' channel (configured: {})",
                        channel_list(&served)
                    )));
                }

                let receipt = gateway.send(&out).await?;
                Ok(msg.response(
                    StatusCode::CREATED,
                    Some(crate::message::Body::from_json(&receipt.to_json())),
                ))
            }
            (&Method::GET, ["status", id]) => {
                if !gateway.delivery_status() {
                    return Err(RsError::provider_unavailable(format!(
                        "provider '{}' does not report per-message delivery status",
                        gateway.provider()
                    )));
                }
                let status = gateway.status(id).await?;
                Ok(msg.ok_json(&status))
            }
            (&Method::GET, ["channels"]) => Ok(msg.ok_json(&json!({
                "channels": gateway.channels().iter().map(|c| c.as_str()).collect::<Vec<_>>(),
                "deliveryStatus": gateway.delivery_status(),
                "provider": gateway.provider(),
            }))),
            _ => Err(RsError::bad_request(
                "message endpoint: POST /send {channel, to, …}, GET /status/{id}, GET /channels",
            )),
        }
    }
}

fn channel_list(channels: &[crate::capabilities::Channel]) -> String {
    if channels.is_empty() {
        return "none".to_string();
    }
    channels
        .iter()
        .map(|c| c.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}
