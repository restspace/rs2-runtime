//! AWS SNS as a [`MessageGateway`] — the `builtin:aws-sns` provider adapter
//! for the `sms` channel.
//!
//! SNS is the mirror image of `cf-email`, which is why the two were built
//! together: it **mints a message id but reports no delivery status** (that
//! needs separate CloudWatch delivery logging, which is not a per-message
//! lookup). So [`MessageGateway::delivery_status`] is `false` here for the
//! opposite reason — nothing to ask, rather than nothing left to ask.
//!
//! Auth is `AuthStrategy::AwsSigV4`, already implemented and vector-tested in
//! [`super::credential`]; this adapter adds no cryptography. The Query API is
//! form-encoded in and XML out, so the response is scanned for `<MessageId>`
//! rather than parsed — one field of one shape, and pulling in an XML parser
//! for it would be the larger risk.

use std::sync::Arc;

use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::{json, Value};

use crate::capabilities::{Channel, HttpOut, MessageGateway, Outbound, Receipt};
use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

use super::credential::CredentialInjector;

/// SNS Query API version, fixed by the service.
const API_VERSION: &str = "2010-03-31";
const MAX_RESPONSE_BYTES: u64 = 256 * 1024;

/// `application/x-www-form-urlencoded` with AWS's unreserved set left alone.
/// SigV4 signs the hash of this body, so the encoding must be stable and must
/// match what the service canonicalizes.
const FORM: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

pub struct AwsSnsGateway {
    http: Arc<dyn HttpOut>,
    injector: CredentialInjector,
    endpoint: String,
    /// Alphanumeric sender id, where the destination country allows one.
    sender_id: Option<String>,
    /// `Transactional` (default) or `Promotional` — transactional buys
    /// deliverability for one-time codes and password resets.
    sms_type: String,
}

impl AwsSnsGateway {
    /// Build from an (already infra-expanded) `store` block:
    ///
    /// ```json
    /// { "adapter": "builtin:aws-sns", "region": "eu-west-1",
    ///   "senderId": "Example", "smsType": "Transactional",
    ///   "auth": "awsSigV4", "accessKeyId": "…", "secretAccessKey": "…" }
    /// ```
    ///
    /// `service` defaults to `sns` and `region` is shared with the signer, so
    /// an operator writes the region once.
    pub fn from_config(config: &Value, http: Arc<dyn HttpOut>) -> Result<Self, RsError> {
        let region = config
            .get("region")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                RsError::bad_request(
                    "message adapter 'builtin:aws-sns' requires 'region' (e.g. 'eu-west-1')",
                )
            })?
            .to_string();

        // Fill in the signer fields this adapter already knows, so the operator
        // does not repeat them and cannot get them inconsistent.
        let mut signing = config.clone();
        if let Some(obj) = signing.as_object_mut() {
            obj.entry("auth")
                .or_insert_with(|| Value::String("awsSigV4".into()));
            obj.insert("service".into(), Value::String("sns".into()));
            obj.insert("region".into(), Value::String(region.clone()));
        }
        let injector = CredentialInjector::from_config(&signing)?.ok_or_else(|| {
            RsError::bad_request(
                "message adapter 'builtin:aws-sns' requires AWS credentials ('accessKeyId' and \
                 'secretAccessKey') — supply them through 'infra:<name>' so they never land in \
                 tenant config",
            )
        })?;

        let endpoint = config
            .get("endpoint")
            .and_then(|v| v.as_str())
            .map(|e| e.trim_end_matches('/').to_string())
            .unwrap_or_else(|| format!("https://sns.{region}.amazonaws.com"));
        let sms_type = config
            .get("smsType")
            .and_then(|v| v.as_str())
            .unwrap_or("Transactional")
            .to_string();
        Ok(AwsSnsGateway {
            http,
            injector,
            endpoint,
            sender_id: config
                .get("senderId")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            sms_type,
        })
    }

    fn form_body(&self, out: &Outbound) -> Result<String, RsError> {
        let Outbound::Sms { to, from, text } = out else {
            return Err(RsError::bad_request(
                "the aws-sns adapter serves the 'sms' channel only",
            ));
        };
        let mut pairs: Vec<(String, String)> = vec![
            ("Action".into(), "Publish".into()),
            ("Version".into(), API_VERSION.into()),
            ("PhoneNumber".into(), to.clone()),
            ("Message".into(), text.clone()),
        ];
        // Message attributes are positional in the Query API; the per-message
        // `from` wins over the adapter default.
        let sender = from.clone().or_else(|| self.sender_id.clone());
        let mut n = 0;
        if let Some(s) = sender {
            n += 1;
            attribute(&mut pairs, n, "AWS.SNS.SMS.SenderID", &s);
        }
        n += 1;
        attribute(&mut pairs, n, "AWS.SNS.SMS.SMSType", &self.sms_type);

        Ok(pairs
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    utf8_percent_encode(k, FORM),
                    utf8_percent_encode(v, FORM)
                )
            })
            .collect::<Vec<_>>()
            .join("&"))
    }
}

fn attribute(pairs: &mut Vec<(String, String)>, n: usize, name: &str, value: &str) {
    pairs.push((format!("MessageAttributes.entry.{n}.Name"), name.into()));
    pairs.push((
        format!("MessageAttributes.entry.{n}.Value.DataType"),
        "String".into(),
    ));
    pairs.push((
        format!("MessageAttributes.entry.{n}.Value.StringValue"),
        value.into(),
    ));
}

/// Pull one element's text out of the Query API's XML response. Deliberately
/// not a parser: the documents we read have exactly one interesting field.
fn xml_field(body: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body.find(&open)? + open.len();
    let end = body[start..].find(&close)? + start;
    Some(body[start..end].trim().to_string())
}

#[async_trait]
impl MessageGateway for AwsSnsGateway {
    async fn send(&self, tenant: &str, out: &Outbound) -> Result<Receipt, RsError> {
        let form = self.form_body(out)?;
        let mut req = Message::request(http::Method::POST, &self.endpoint, tenant).with_body(
            Body::from_string(form, MediaType::new("application/x-www-form-urlencoded")),
        );
        self.injector.apply(&mut req, MAX_RESPONSE_BYTES).await?;

        let mut resp = self.http.request(req).await?;
        let status = resp.status.map(|s| s.as_u16()).unwrap_or(0);
        let text = match resp.body.as_mut() {
            Some(b) => {
                String::from_utf8_lossy(b.materialize(MAX_RESPONSE_BYTES).await?).to_string()
            }
            None => String::new(),
        };
        if !(200..300).contains(&status) {
            return Err(provider_error(status, &text));
        }
        let id = xml_field(&text, "MessageId").ok_or_else(|| {
            RsError::contract_violation("aws-sns accepted the message but returned no MessageId")
        })?;
        Ok(Receipt::with_id(id, Channel::Sms, "aws-sns"))
    }

    async fn status(&self, _tenant: &str, _id: &str) -> Result<Value, RsError> {
        Err(RsError::provider_unavailable(
            "aws-sns has no per-message status API — enable SMS delivery-status logging and read \
             it from CloudWatch",
        ))
    }

    fn channels(&self) -> Vec<Channel> {
        vec![Channel::Sms]
    }

    fn delivery_status(&self) -> bool {
        false
    }

    fn provider(&self) -> &str {
        "aws-sns"
    }
}

/// SNS puts its reason in `<Code>` / `<Message>`; keep both, because
/// `InvalidParameter` on a phone number and `Throttling` are different
/// operator problems.
fn provider_error(status: u16, body: &str) -> RsError {
    let code = xml_field(body, "Code").unwrap_or_default();
    let message = xml_field(body, "Message").unwrap_or_default();
    let detail = if message.is_empty() {
        format!("provider returned {status}")
    } else if code.is_empty() {
        message
    } else {
        format!("{code}: {message}")
    };
    let mut err = match status {
        400..=499 => RsError::bad_request(format!("aws-sns rejected the message: {detail}")),
        _ => RsError::provider_unavailable(format!("aws-sns send failed: {detail}")),
    };
    err.extra = Some(json!({ "provider": "aws-sns", "status": status }));
    err
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gateway(config: Value) -> AwsSnsGateway {
        struct NoHttp;
        #[async_trait]
        impl HttpOut for NoHttp {
            async fn request(&self, _msg: Message) -> Result<Message, RsError> {
                unreachable!("these tests never send")
            }
        }
        AwsSnsGateway::from_config(&config, Arc::new(NoHttp)).expect("builds")
    }

    fn base() -> Value {
        json!({
            "region": "eu-west-1",
            "accessKeyId": "AKIAEXAMPLE",
            "secretAccessKey": "secret"
        })
    }

    #[test]
    fn the_publish_form_carries_the_number_the_text_and_the_sms_type() {
        let g = gateway(base());
        let form = g
            .form_body(&Outbound::Sms {
                to: "+447700900000".into(),
                from: None,
                text: "your code is 123".into(),
            })
            .unwrap();
        assert!(form.contains("Action=Publish"), "{form}");
        assert!(form.contains("PhoneNumber=%2B447700900000"), "{form}");
        // Spaces are %20, never '+': a '+' would decode back as a space and
        // corrupt an E.164 number if the same encoder were used on one.
        assert!(form.contains("Message=your%20code%20is%20123"), "{form}");
        assert!(form.contains("StringValue=Transactional"), "{form}");
    }

    #[test]
    fn a_per_message_sender_beats_the_adapter_default() {
        let mut cfg = base();
        cfg["senderId"] = json!("Default");
        let g = gateway(cfg);
        let form = g
            .form_body(&Outbound::Sms {
                to: "+447700900000".into(),
                from: Some("Override".into()),
                text: "hi".into(),
            })
            .unwrap();
        assert!(form.contains("StringValue=Override"), "{form}");
        assert!(!form.contains("StringValue=Default"), "{form}");
    }

    #[test]
    fn an_email_handed_to_the_sms_adapter_is_refused() {
        let g = gateway(base());
        let err = g
            .form_body(&Outbound::Email {
                to: vec![crate::capabilities::Addr::new("a@b.com")],
                cc: vec![],
                bcc: vec![],
                from: None,
                reply_to: None,
                subject: "s".into(),
                text: Some("t".into()),
                html: None,
                attachments: vec![],
                headers: Default::default(),
            })
            .expect_err("wrong channel");
        assert!(err.detail.contains("'sms' channel only"), "{err:?}");
    }

    #[test]
    fn the_message_id_is_read_out_of_the_query_api_response() {
        let xml = "<PublishResponse><PublishResult><MessageId>abc-123</MessageId>\
                   </PublishResult></PublishResponse>";
        assert_eq!(xml_field(xml, "MessageId").as_deref(), Some("abc-123"));
        assert_eq!(xml_field(xml, "Nope"), None);
    }

    #[test]
    fn a_provider_rejection_keeps_its_code_and_message() {
        let xml = "<ErrorResponse><Error><Code>InvalidParameter</Code>\
                   <Message>Invalid phone number</Message></Error></ErrorResponse>";
        let err = provider_error(400, xml);
        assert_eq!(err.status, 400);
        assert!(
            err.detail
                .contains("InvalidParameter: Invalid phone number"),
            "{err:?}"
        );
    }
}
