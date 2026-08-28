//! Cloudflare Email Service as a [`MessageGateway`] — the `builtin:cf-email`
//! provider adapter for the `email` channel.
//!
//! Uses the **REST API** rather than the Workers `send_email` binding. The
//! binding needs no token, but its permitted senders are fixed in
//! `wrangler.jsonc` at deploy time (wrong for per-tenant sending domains) and
//! it does not exist off Workers — one REST implementation serves both hosts
//! and takes its bearer token from operator infra, so the same mount config
//! works on the Rust node and on the Worker.
//!
//! Two provider facts shape what this can promise:
//!
//! - The REST send answers **synchronously** with per-recipient delivery
//!   (`delivered` / `queued` / `permanent_bounces`) and mints **no message
//!   id**. So the receipt carries `detail` and no `id`, and
//!   [`MessageGateway::delivery_status`] is `false` — there is nothing to look
//!   up afterwards, not because the provider is deficient but because it
//!   already told us.
//! - Sending is Workers-Paid-only and quota'd; a provider rejection is
//!   surfaced with its own message rather than flattened to a generic 502.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use crate::capabilities::{Addr, Channel, HttpOut, MessageGateway, Outbound, Receipt};
use crate::error::RsError;
use crate::message::Message;

use super::credential::CredentialInjector;

const SEND_PATH: &str = "/email/sending/send";
/// Provider responses are small JSON documents; this bounds a hostile one.
const MAX_RESPONSE_BYTES: u64 = 256 * 1024;

pub struct CfEmailGateway {
    http: Arc<dyn HttpOut>,
    injector: CredentialInjector,
    account_id: String,
    /// Default sender when a message names none.
    from: Option<Addr>,
    api_base: String,
}

impl CfEmailGateway {
    /// Build from an (already infra-expanded) `store` block:
    ///
    /// ```json
    /// { "adapter": "builtin:cf-email", "accountId": "…",
    ///   "from": "noreply@example.com", "fromName": "Example",
    ///   "auth": "bearer", "token": "<from infra/secret>" }
    /// ```
    pub fn from_config(config: &Value, http: Arc<dyn HttpOut>) -> Result<Self, RsError> {
        let account_id = config
            .get("accountId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                RsError::bad_request(
                    "message adapter 'builtin:cf-email' requires 'accountId' (the Cloudflare \
                     account the sending domain belongs to)",
                )
            })?
            .to_string();
        let injector = CredentialInjector::from_config(config)?.ok_or_else(|| {
            RsError::bad_request(
                "message adapter 'builtin:cf-email' requires an 'auth' credential — supply it \
                 through 'infra:<name>' so the token never lands in tenant config",
            )
        })?;
        let from = match config.get("from").and_then(|v| v.as_str()) {
            Some(email) => Some(Addr {
                email: email.to_string(),
                name: config
                    .get("fromName")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
            }),
            None => None,
        };
        // Overridable only so tests can point at a local stub; operators never
        // set it, and the default is the real API.
        let api_base = config
            .get("apiBase")
            .and_then(|v| v.as_str())
            .unwrap_or("https://api.cloudflare.com/client/v4")
            .trim_end_matches('/')
            .to_string();
        Ok(CfEmailGateway {
            http,
            injector,
            account_id,
            from,
            api_base,
        })
    }

    /// The provider's JSON body for one email.
    fn payload(&self, out: &Outbound) -> Result<Value, RsError> {
        let Outbound::Email {
            to,
            cc,
            bcc,
            from,
            reply_to,
            subject,
            text,
            html,
            attachments,
            headers,
        } = out
        else {
            // The service and the routing gateway both check first; this is the
            // belt-and-braces case for a directly constructed gateway.
            return Err(RsError::bad_request(
                "the cf-email adapter serves the 'email' channel only",
            ));
        };
        let sender = from.as_ref().or(self.from.as_ref()).ok_or_else(|| {
            RsError::bad_request(
                "no sender: give the message a 'from', or configure the adapter with a default \
                 'from' on a verified sending domain",
            )
        })?;
        let mut body = Map::new();
        body.insert("from".into(), Value::String(sender.rfc5322()));
        body.insert("to".into(), addr_array(to));
        if !cc.is_empty() {
            body.insert("cc".into(), addr_array(cc));
        }
        if !bcc.is_empty() {
            body.insert("bcc".into(), addr_array(bcc));
        }
        if let Some(r) = reply_to {
            body.insert("replyTo".into(), Value::String(r.rfc5322()));
        }
        body.insert("subject".into(), Value::String(subject.clone()));
        if let Some(t) = text {
            body.insert("text".into(), Value::String(t.clone()));
        }
        if let Some(h) = html {
            body.insert("html".into(), Value::String(h.clone()));
        }
        if !attachments.is_empty() {
            body.insert(
                "attachments".into(),
                Value::Array(
                    attachments
                        .iter()
                        .map(|a| {
                            let mut m = Map::new();
                            m.insert("filename".into(), Value::String(a.filename.clone()));
                            m.insert("contentType".into(), Value::String(a.content_type.clone()));
                            m.insert("content".into(), Value::String(a.content.clone()));
                            if let Some(cid) = &a.content_id {
                                m.insert("contentId".into(), Value::String(cid.clone()));
                            }
                            Value::Object(m)
                        })
                        .collect(),
                ),
            );
        }
        if !headers.is_empty() {
            body.insert(
                "headers".into(),
                Value::Object(
                    headers
                        .iter()
                        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                        .collect(),
                ),
            );
        }
        Ok(Value::Object(body))
    }
}

fn addr_array(list: &[Addr]) -> Value {
    Value::Array(list.iter().map(|a| Value::String(a.rfc5322())).collect())
}

#[async_trait]
impl MessageGateway for CfEmailGateway {
    async fn send(&self, tenant: &str, out: &Outbound) -> Result<Receipt, RsError> {
        let payload = self.payload(out)?;
        let url = format!("{}/accounts/{}{SEND_PATH}", self.api_base, self.account_id);
        let mut req = Message::request(http::Method::POST, &url, tenant).with_json(&payload);
        self.injector.apply(&mut req, MAX_RESPONSE_BYTES).await?;

        let mut resp = self.http.request(req).await?;
        let status = resp.status.map(|s| s.as_u16()).unwrap_or(0);
        let doc = match resp.body.as_mut() {
            Some(b) => b.as_json(MAX_RESPONSE_BYTES).await.unwrap_or(Value::Null),
            None => Value::Null,
        };
        if !(200..300).contains(&status) || doc.get("success") == Some(&Value::Bool(false)) {
            return Err(provider_error(status, &doc));
        }
        let result = doc.get("result").cloned();
        Ok(Receipt {
            // The REST send mints no id (see the module note); if a future
            // response carries one, pass it through rather than discard it.
            id: result
                .as_ref()
                .and_then(|r| r.get("message_id").or_else(|| r.get("messageId")))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            channel: Channel::Email,
            provider: "cf-email".to_string(),
            detail: result,
        })
    }

    async fn status(&self, _tenant: &str, _id: &str) -> Result<Value, RsError> {
        Err(RsError::provider_unavailable(
            "cf-email reports delivery in the send response, not by later lookup",
        ))
    }

    fn channels(&self) -> Vec<Channel> {
        vec![Channel::Email]
    }

    fn delivery_status(&self) -> bool {
        false
    }

    fn provider(&self) -> &str {
        "cf-email"
    }
}

/// Keep the provider's own words: a quota rejection and a bad sending domain
/// are different operator problems, and flattening both to "502" hides which.
fn provider_error(status: u16, doc: &Value) -> RsError {
    let detail = doc
        .get("errors")
        .and_then(|e| e.as_array())
        .map(|errs| {
            errs.iter()
                .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                .collect::<Vec<_>>()
                .join("; ")
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("provider returned {status}"));
    let mut err = match status {
        400..=499 => RsError::bad_request(format!("cf-email rejected the message: {detail}")),
        _ => RsError::provider_unavailable(format!("cf-email send failed: {detail}")),
    };
    err.extra = Some(json!({ "provider": "cf-email", "status": status }));
    err
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Captures the outbound request and replies with a canned response, so the
    /// tests assert the exact wire shape without reaching Cloudflare.
    struct StubHttp {
        seen: Mutex<Option<(String, Value)>>,
        status: u16,
        reply: Value,
    }

    #[async_trait]
    impl HttpOut for StubHttp {
        async fn request(&self, mut msg: Message) -> Result<Message, RsError> {
            let url = msg.url.path.clone();
            let body = match msg.body.as_mut() {
                Some(b) => b.as_json(1 << 20).await?,
                None => Value::Null,
            };
            *self.seen.lock().unwrap() = Some((url, body));
            let mut resp = msg.response(
                http::StatusCode::from_u16(self.status).unwrap(),
                Some(crate::message::Body::from_json(&self.reply)),
            );
            resp.status = Some(http::StatusCode::from_u16(self.status).unwrap());
            Ok(resp)
        }
    }

    fn stub(status: u16, reply: Value) -> Arc<StubHttp> {
        Arc::new(StubHttp {
            seen: Mutex::new(None),
            status,
            reply,
        })
    }

    fn config() -> Value {
        json!({
            "accountId": "acct123",
            "from": "noreply@example.com",
            "fromName": "Example",
            "auth": "bearer",
            "token": "tok"
        })
    }

    fn welcome() -> Outbound {
        Outbound::Email {
            to: vec![Addr {
                email: "a@b.com".into(),
                name: Some("A".into()),
            }],
            cc: vec![],
            bcc: vec![],
            from: None,
            reply_to: None,
            subject: "Welcome".into(),
            text: Some("hi".into()),
            html: Some("<p>hi</p>".into()),
            attachments: vec![],
            headers: Default::default(),
        }
    }

    #[tokio::test]
    async fn a_send_hits_the_account_endpoint_with_the_providers_field_names() {
        let http = stub(
            200,
            json!({"success": true, "errors": [],
                   "result": {"delivered": ["a@b.com"], "queued": [], "permanent_bounces": []}}),
        );
        let g = CfEmailGateway::from_config(&config(), http.clone()).unwrap();
        let receipt = g.send("t", &welcome()).await.expect("sends");

        let (url, body) = http.seen.lock().unwrap().clone().expect("called");
        assert!(
            url.ends_with("/accounts/acct123/email/sending/send"),
            "endpoint: {url}"
        );
        assert_eq!(body["from"], "Example <noreply@example.com>");
        assert_eq!(body["to"], json!(["A <a@b.com>"]));
        assert_eq!(body["subject"], "Welcome");
        assert_eq!(body["html"], "<p>hi</p>");

        // No id to give, and the provider's own answer preserved verbatim.
        assert!(receipt.id.is_none(), "REST send mints no message id");
        assert_eq!(receipt.provider, "cf-email");
        assert_eq!(receipt.detail.unwrap()["delivered"], json!(["a@b.com"]));
        assert!(!g.delivery_status(), "nothing to look up afterwards");
    }

    #[tokio::test]
    async fn a_message_sender_overrides_the_configured_default() {
        let http = stub(200, json!({"success": true, "result": {}}));
        let g = CfEmailGateway::from_config(&config(), http.clone()).unwrap();
        let mut out = welcome();
        if let Outbound::Email { from, .. } = &mut out {
            *from = Some(Addr::new("billing@example.com"));
        }
        g.send("t", &out).await.unwrap();
        let (_, body) = http.seen.lock().unwrap().clone().unwrap();
        assert_eq!(body["from"], "billing@example.com");
    }

    #[tokio::test]
    async fn a_provider_rejection_keeps_the_providers_own_words() {
        let http = stub(
            403,
            json!({"success": false,
                   "errors": [{"code": 1001, "message": "sending domain not verified"}]}),
        );
        let g = CfEmailGateway::from_config(&config(), http).unwrap();
        let err = g.send("t", &welcome()).await.expect_err("rejected");
        assert_eq!(
            err.status, 400,
            "a 4xx from the provider is the caller's fix"
        );
        assert!(
            err.detail.contains("sending domain not verified"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_success_false_body_is_a_failure_even_with_a_200() {
        let http = stub(
            200,
            json!({"success": false, "errors": [{"message": "quota exceeded"}]}),
        );
        let g = CfEmailGateway::from_config(&config(), http).unwrap();
        let err = g.send("t", &welcome()).await.expect_err("rejected");
        assert!(err.detail.contains("quota exceeded"), "{err:?}");
    }

    #[test]
    fn an_adapter_without_a_credential_is_a_config_error() {
        let err =
            match CfEmailGateway::from_config(&json!({"accountId": "a"}), stub(200, Value::Null)) {
                Err(e) => e,
                Ok(_) => panic!("an adapter with no credential must not build"),
            };
        assert!(
            err.detail.contains("requires an 'auth' credential"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_message_with_no_sender_anywhere_says_which_two_places_to_fix() {
        let cfg = json!({"accountId": "a", "auth": "bearer", "token": "t"});
        let g = CfEmailGateway::from_config(&cfg, stub(200, Value::Null)).unwrap();
        let err = g.send("t", &welcome()).await.expect_err("no sender");
        assert!(err.detail.contains("no sender"), "{err:?}");
    }
}
