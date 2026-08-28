//! Outbound messaging: the typed *provider* capability (PRD §9.2) covering
//! every delivery channel behind one interface, with the provider swapped by
//! config (`store.adapter` / `store.adapters`) rather than by a recompile.
//!
//! The interface is deliberately shaped by two channels that do **not** agree,
//! because an abstraction proven against one provider is not an abstraction:
//!
//! - **Payload.** Email carries a subject, HTML and attachments; SMS carries a
//!   string. Rather than one struct of optionals — where `subject` on an SMS is
//!   representable but meaningless — [`Outbound`] is a channel-tagged enum, so
//!   illegal states cannot be constructed. The wire form is still one flat
//!   tagged object (`{"channel": "email", …}`).
//! - **Delivery status.** Cloudflare can report it; AWS SNS cannot without
//!   separate CloudWatch delivery logging. So `status` is *not* universal:
//!   [`MessageGateway::delivery_status`] declares whether an adapter answers it
//!   at all, and the service turns "no" into a 501 with the provider named —
//!   the same feature-detected-facet pattern as [`super::DataStore`]'s
//!   `listing_pushdown`. Advertised capability is the enforced capability.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::error::RsError;

/// A delivery channel. Adding one is a variant plus its adapters — every
/// consumer that matches exhaustively is then a compile error until updated,
/// which is the point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Channel {
    Email,
    Sms,
}

impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Channel::Email => "email",
            Channel::Sms => "sms",
        }
    }

    pub fn parse(s: &str) -> Option<Channel> {
        match s {
            "email" => Some(Channel::Email),
            "sms" => Some(Channel::Sms),
            _ => None,
        }
    }

    /// Every channel this build knows, in the order they are advertised.
    pub fn all() -> [Channel; 2] {
        [Channel::Email, Channel::Sms]
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An email participant. Accepts `"a@b.com"` or `{"email": …, "name": …}` on
/// the wire; the display name is optional everywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    pub email: String,
    pub name: Option<String>,
}

impl Addr {
    pub fn new(email: impl Into<String>) -> Self {
        Addr {
            email: email.into(),
            name: None,
        }
    }

    /// RFC 5322 display form: `Name <a@b.com>`, or the bare address.
    pub fn rfc5322(&self) -> String {
        match &self.name {
            Some(n) if !n.is_empty() => format!("{n} <{}>", self.email),
            _ => self.email.clone(),
        }
    }

    fn from_json(v: &Value, field: &str) -> Result<Addr, RsError> {
        match v {
            Value::String(s) => Addr::checked(s.clone(), None, field),
            Value::Object(o) => {
                let email = o
                    .get("email")
                    .and_then(|e| e.as_str())
                    .ok_or_else(|| {
                        RsError::bad_request(format!("'{field}' object requires 'email' (string)"))
                    })?
                    .to_string();
                let name = o
                    .get("name")
                    .and_then(|n| n.as_str())
                    .map(str::to_string)
                    .filter(|n| !n.is_empty());
                Addr::checked(email, name, field)
            }
            _ => Err(RsError::bad_request(format!(
                "'{field}' must be an address string or {{email, name}} object"
            ))),
        }
    }

    /// The shallow syntax check every provider would reject on anyway — done
    /// here so it is one 400 at the edge, not a provider-shaped 502 later.
    fn checked(email: String, name: Option<String>, field: &str) -> Result<Addr, RsError> {
        let at = email.find('@');
        let ok = match at {
            Some(i) => {
                i > 0 && email[i + 1..].contains('.') && !email.contains(char::is_whitespace)
            }
            None => false,
        };
        if !ok {
            return Err(RsError::bad_request(format!(
                "'{field}' is not an email address: '{email}'"
            )));
        }
        Ok(Addr { email, name })
    }
}

/// A file carried with an email. `content` is base64 — the host never sees the
/// bytes as anything else, so the same value crosses to a guest adapter or a
/// provider API unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    pub filename: String,
    pub content_type: String,
    /// Base64-encoded contents.
    pub content: String,
    /// When set, the attachment is inline and referenced as `cid:<id>`.
    pub content_id: Option<String>,
}

/// The combined `to` + `cc` + `bcc` cap. Not an arbitrary number: it is the
/// lowest common ceiling across the providers we target, so exceeding it is a
/// 400 here rather than a different error from each provider.
pub const MAX_RECIPIENTS: usize = 50;

/// One outbound message, tagged by channel.
///
/// Serialized form (what `POST /<mount>/send` accepts and what a `code:`
/// adapter receives):
///
/// ```json
/// {"channel": "email", "to": ["a@b.com"], "subject": "Hi", "text": "…"}
/// {"channel": "sms",   "to": "+447700900000", "text": "…"}
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    Email {
        to: Vec<Addr>,
        cc: Vec<Addr>,
        bcc: Vec<Addr>,
        /// Absent ⇒ the adapter's configured default sender.
        from: Option<Addr>,
        reply_to: Option<Addr>,
        subject: String,
        text: Option<String>,
        html: Option<String>,
        attachments: Vec<Attachment>,
        headers: BTreeMap<String, String>,
    },
    Sms {
        to: String,
        /// Absent ⇒ the adapter's configured sender id / originating number.
        from: Option<String>,
        text: String,
    },
}

impl Outbound {
    pub fn channel(&self) -> Channel {
        match self {
            Outbound::Email { .. } => Channel::Email,
            Outbound::Sms { .. } => Channel::Sms,
        }
    }

    /// Parse and validate a send body. Every failure is a 400 whose wording
    /// names the offending field — these are read by humans writing config and
    /// by agents reading the error, so they must say what to fix.
    pub fn from_json(v: &Value) -> Result<Outbound, RsError> {
        let obj = v.as_object().ok_or_else(|| {
            RsError::bad_request("send body must be a JSON object {channel, to, …}")
        })?;
        let channel = obj.get("channel").and_then(|c| c.as_str()).ok_or_else(|| {
            RsError::bad_request("'channel' (string) is required: 'email' or 'sms'")
        })?;
        match Channel::parse(channel) {
            Some(Channel::Email) => Outbound::email_from_json(obj),
            Some(Channel::Sms) => Outbound::sms_from_json(obj),
            None => Err(RsError::bad_request(format!(
                "unknown channel '{channel}' (one of: {})",
                Channel::all()
                    .iter()
                    .map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    fn email_from_json(obj: &Map<String, Value>) -> Result<Outbound, RsError> {
        let to = addr_list(obj, "to")?;
        if to.is_empty() {
            return Err(RsError::bad_request(
                "'to' must name at least one recipient",
            ));
        }
        let cc = addr_list(obj, "cc")?;
        let bcc = addr_list(obj, "bcc")?;
        let total = to.len() + cc.len() + bcc.len();
        if total > MAX_RECIPIENTS {
            return Err(RsError::bad_request(format!(
                "{total} recipients across to/cc/bcc exceeds the {MAX_RECIPIENTS} limit"
            )));
        }
        let from = match obj.get("from") {
            Some(v) if !v.is_null() => Some(Addr::from_json(v, "from")?),
            _ => None,
        };
        let reply_to = match obj.get("replyTo") {
            Some(v) if !v.is_null() => Some(Addr::from_json(v, "replyTo")?),
            _ => None,
        };
        let subject = obj
            .get("subject")
            .and_then(|s| s.as_str())
            .ok_or_else(|| RsError::bad_request("'subject' (string) is required for email"))?
            .to_string();
        let text = opt_str(obj, "text")?;
        let html = opt_str(obj, "html")?;
        if text.is_none() && html.is_none() {
            return Err(RsError::bad_request(
                "an email needs 'text', 'html', or both",
            ));
        }
        let attachments = match obj.get("attachments") {
            Some(Value::Array(items)) => items
                .iter()
                .map(attachment_from_json)
                .collect::<Result<Vec<_>, _>>()?,
            Some(Value::Null) | None => Vec::new(),
            Some(_) => return Err(RsError::bad_request("'attachments' must be an array")),
        };
        let mut headers = BTreeMap::new();
        match obj.get("headers") {
            Some(Value::Object(h)) => {
                for (k, val) in h {
                    let s = val.as_str().ok_or_else(|| {
                        RsError::bad_request(format!("header '{k}' must be a string"))
                    })?;
                    headers.insert(k.clone(), s.to_string());
                }
            }
            Some(Value::Null) | None => {}
            Some(_) => return Err(RsError::bad_request("'headers' must be an object")),
        }
        Ok(Outbound::Email {
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
        })
    }

    fn sms_from_json(obj: &Map<String, Value>) -> Result<Outbound, RsError> {
        let to = obj
            .get("to")
            .and_then(|t| t.as_str())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| RsError::bad_request("'to' (non-empty string) is required for sms"))?
            .to_string();
        let text = obj
            .get("text")
            .and_then(|t| t.as_str())
            .filter(|t| !t.is_empty())
            .ok_or_else(|| RsError::bad_request("'text' (non-empty string) is required for sms"))?
            .to_string();
        let from = opt_str(obj, "from")?;
        Ok(Outbound::Sms { to, from, text })
    }

    /// The wire form — the inverse of [`Outbound::from_json`]. Guest (`code:`)
    /// adapters receive exactly this, so a bundle and an HTTP caller speak one
    /// vocabulary.
    pub fn to_json(&self) -> Value {
        match self {
            Outbound::Email {
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
            } => {
                let mut o = Map::new();
                o.insert("channel".into(), Value::String("email".into()));
                o.insert("to".into(), addrs_json(to));
                if !cc.is_empty() {
                    o.insert("cc".into(), addrs_json(cc));
                }
                if !bcc.is_empty() {
                    o.insert("bcc".into(), addrs_json(bcc));
                }
                if let Some(f) = from {
                    o.insert("from".into(), addr_json(f));
                }
                if let Some(r) = reply_to {
                    o.insert("replyTo".into(), addr_json(r));
                }
                o.insert("subject".into(), Value::String(subject.clone()));
                if let Some(t) = text {
                    o.insert("text".into(), Value::String(t.clone()));
                }
                if let Some(h) = html {
                    o.insert("html".into(), Value::String(h.clone()));
                }
                if !attachments.is_empty() {
                    o.insert(
                        "attachments".into(),
                        Value::Array(
                            attachments
                                .iter()
                                .map(|a| {
                                    let mut m = Map::new();
                                    m.insert("filename".into(), Value::String(a.filename.clone()));
                                    m.insert(
                                        "contentType".into(),
                                        Value::String(a.content_type.clone()),
                                    );
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
                    o.insert(
                        "headers".into(),
                        Value::Object(
                            headers
                                .iter()
                                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                                .collect(),
                        ),
                    );
                }
                Value::Object(o)
            }
            Outbound::Sms { to, from, text } => {
                let mut o = Map::new();
                o.insert("channel".into(), Value::String("sms".into()));
                o.insert("to".into(), Value::String(to.clone()));
                if let Some(f) = from {
                    o.insert("from".into(), Value::String(f.clone()));
                }
                o.insert("text".into(), Value::String(text.clone()));
                Value::Object(o)
            }
        }
    }
}

fn addr_json(a: &Addr) -> Value {
    match &a.name {
        Some(n) => {
            let mut m = Map::new();
            m.insert("email".into(), Value::String(a.email.clone()));
            m.insert("name".into(), Value::String(n.clone()));
            Value::Object(m)
        }
        None => Value::String(a.email.clone()),
    }
}

fn addrs_json(list: &[Addr]) -> Value {
    Value::Array(list.iter().map(addr_json).collect())
}

/// `to`/`cc`/`bcc`: a single address or an array of them.
fn addr_list(obj: &Map<String, Value>, field: &str) -> Result<Vec<Addr>, RsError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|v| Addr::from_json(v, field))
            .collect::<Result<Vec<_>, _>>(),
        Some(one) => Ok(vec![Addr::from_json(one, field)?]),
    }
}

fn opt_str(obj: &Map<String, Value>, field: &str) -> Result<Option<String>, RsError> {
    match obj.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(RsError::bad_request(format!("'{field}' must be a string"))),
    }
}

fn attachment_from_json(v: &Value) -> Result<Attachment, RsError> {
    let o = v
        .as_object()
        .ok_or_else(|| RsError::bad_request("each attachment must be an object"))?;
    let req = |key: &str| -> Result<String, RsError> {
        o.get(key)
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .ok_or_else(|| RsError::bad_request(format!("attachment requires '{key}' (string)")))
    };
    Ok(Attachment {
        filename: req("filename")?,
        content_type: o
            .get("contentType")
            .and_then(|x| x.as_str())
            .unwrap_or("application/octet-stream")
            .to_string(),
        content: req("content")?,
        content_id: o
            .get("contentId")
            .and_then(|x| x.as_str())
            .map(str::to_string),
    })
}

/// What the provider gives back for an accepted message.
///
/// `id` is optional because a message id is **not** universal either: AWS SNS
/// returns one and no status endpoint; Cloudflare's REST send returns no id at
/// all, because it reports per-recipient delivery synchronously in the send
/// response — there is nothing left to look up later. Rather than mint a fake
/// id no caller could use, the adapter leaves it absent and puts the provider's
/// own answer in `detail`. Between them, `id` and [`MessageGateway::
/// delivery_status`] tell a caller exactly which of the three provider shapes
/// it is talking to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    /// The provider's message id, when it mints one — the key `status` is
    /// later asked about.
    pub id: Option<String>,
    pub channel: Channel,
    /// The adapter that accepted it, for logs and for the send response.
    pub provider: String,
    /// The provider's own send-time answer, passed through unaltered (e.g.
    /// Cloudflare's `{delivered, queued, permanent_bounces}` grouping).
    pub detail: Option<Value>,
}

impl Receipt {
    /// A receipt for a provider that mints ids.
    pub fn with_id(id: impl Into<String>, channel: Channel, provider: impl Into<String>) -> Self {
        Receipt {
            id: Some(id.into()),
            channel,
            provider: provider.into(),
            detail: None,
        }
    }

    pub fn to_json(&self) -> Value {
        let mut o = Map::new();
        if let Some(id) = &self.id {
            o.insert("id".into(), Value::String(id.clone()));
        }
        o.insert(
            "channel".into(),
            Value::String(self.channel.as_str().to_string()),
        );
        o.insert("provider".into(), Value::String(self.provider.clone()));
        if let Some(d) = &self.detail {
            o.insert("detail".into(), d.clone());
        }
        Value::Object(o)
    }
}

/// Outbound messaging behind a swappable provider adapter (Cloudflare Email
/// Service, AWS SNS, a `code:` guest bundle, …) — one trait, many providers,
/// selected by config. `tenant` is supplied by the host scoping wrapper, never
/// by service code.
#[async_trait]
pub trait MessageGateway: Send + Sync {
    /// Accept a message for delivery; returns the provider's receipt.
    async fn send(&self, tenant: &str, out: &Outbound) -> Result<Receipt, RsError>;

    /// Delivery status of a previously sent message (provider-shaped JSON).
    /// Only called when [`MessageGateway::delivery_status`] is `true`.
    async fn status(&self, tenant: &str, id: &str) -> Result<Value, RsError>;

    /// Which channels this adapter actually serves. A send on any other
    /// channel is a 400 before the provider is ever called.
    fn channels(&self) -> Vec<Channel>;

    /// Whether the provider can answer `status` at all. AWS SNS cannot without
    /// separate delivery logging, so this is a declared facet rather than an
    /// error every caller has to discover by trying.
    fn delivery_status(&self) -> bool;

    /// Adapter name, as advertised in receipts and errors.
    fn provider(&self) -> &str;
}

/// A [`MessageGateway`] handle pre-scoped to one tenant — the only form
/// services see.
#[derive(Clone)]
pub struct ScopedMessageGateway {
    inner: Arc<dyn MessageGateway>,
    tenant: String,
}

impl ScopedMessageGateway {
    pub fn new(inner: Arc<dyn MessageGateway>, tenant: &str) -> Self {
        ScopedMessageGateway {
            inner,
            tenant: tenant.to_string(),
        }
    }

    pub async fn send(&self, out: &Outbound) -> Result<Receipt, RsError> {
        self.inner.send(&self.tenant, out).await
    }

    pub async fn status(&self, id: &str) -> Result<Value, RsError> {
        self.inner.status(&self.tenant, id).await
    }

    pub fn channels(&self) -> Vec<Channel> {
        self.inner.channels()
    }

    pub fn delivery_status(&self) -> bool {
        self.inner.delivery_status()
    }

    pub fn provider(&self) -> &str {
        self.inner.provider()
    }
}

/// Several single-channel adapters behind one mount: dispatch on the message's
/// channel. This is why the capability is worth unifying — a tenant sends mail
/// through Cloudflare and texts through AWS at one endpoint, and that is a
/// config map, not a second service.
pub struct RoutingGateway {
    routes: Vec<(Channel, Arc<dyn MessageGateway>)>,
}

impl RoutingGateway {
    pub fn new(routes: Vec<(Channel, Arc<dyn MessageGateway>)>) -> Self {
        RoutingGateway { routes }
    }

    fn route(&self, channel: Channel) -> Option<&Arc<dyn MessageGateway>> {
        self.routes
            .iter()
            .find(|(c, _)| *c == channel)
            .map(|(_, g)| g)
    }

    /// The adapter that owns `id`'s channel cannot be known from the id alone,
    /// so `status` asks each route that supports it, in order, and returns the
    /// first answer. Providers mint distinct id shapes, so a wrong-provider hit
    /// is a 404 there and we move on.
    fn status_routes(&self) -> impl Iterator<Item = &Arc<dyn MessageGateway>> {
        self.routes
            .iter()
            .filter(|(_, g)| g.delivery_status())
            .map(|(_, g)| g)
    }
}

#[async_trait]
impl MessageGateway for RoutingGateway {
    async fn send(&self, tenant: &str, out: &Outbound) -> Result<Receipt, RsError> {
        let channel = out.channel();
        match self.route(channel) {
            Some(g) => g.send(tenant, out).await,
            None => Err(RsError::bad_request(format!(
                "no adapter is configured for the '{channel}' channel (configured: {})",
                self.channels()
                    .iter()
                    .map(|c| c.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    async fn status(&self, tenant: &str, id: &str) -> Result<Value, RsError> {
        let mut last: Option<RsError> = None;
        for g in self.status_routes() {
            match g.status(tenant, id).await {
                Ok(v) => return Ok(v),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| {
            RsError::provider_unavailable(
                "no configured adapter reports delivery status".to_string(),
            )
        }))
    }

    fn channels(&self) -> Vec<Channel> {
        let mut out: Vec<Channel> = self.routes.iter().map(|(c, _)| *c).collect();
        out.sort();
        out.dedup();
        out
    }

    fn delivery_status(&self) -> bool {
        self.routes.iter().any(|(_, g)| g.delivery_status())
    }

    fn provider(&self) -> &str {
        "routing"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn email_round_trips_through_the_wire_form() {
        let v = json!({
            "channel": "email",
            "to": [{"email": "a@b.com", "name": "A"}, "c@d.com"],
            "subject": "Hi",
            "html": "<p>hi</p>",
            "headers": {"X-Tag": "welcome"}
        });
        let out = Outbound::from_json(&v).expect("parses");
        assert_eq!(out.channel(), Channel::Email);
        let back = Outbound::from_json(&out.to_json()).expect("re-parses");
        assert_eq!(out, back);
    }

    #[test]
    fn an_email_without_a_body_is_rejected() {
        let v = json!({"channel": "email", "to": "a@b.com", "subject": "Hi"});
        let err = Outbound::from_json(&v).expect_err("no text or html");
        assert!(err.detail.contains("'text', 'html', or both"), "{err:?}");
    }

    #[test]
    fn recipients_are_capped_across_all_three_fields() {
        let many: Vec<String> = (0..40).map(|i| format!("u{i}@b.com")).collect();
        let cc: Vec<String> = (0..11).map(|i| format!("c{i}@b.com")).collect();
        let v = json!({
            "channel": "email", "to": many, "cc": cc,
            "subject": "Hi", "text": "hi"
        });
        let err = Outbound::from_json(&v).expect_err("51 recipients");
        assert!(err.detail.contains("exceeds the 50 limit"), "{err:?}");
    }

    #[test]
    fn sms_takes_a_bare_string_and_rejects_email_fields_silently() {
        let out =
            Outbound::from_json(&json!({"channel": "sms", "to": "+447700900000", "text": "hi"}))
                .expect("parses");
        match &out {
            Outbound::Sms { to, text, from } => {
                assert_eq!(to, "+447700900000");
                assert_eq!(text, "hi");
                assert!(from.is_none());
            }
            _ => panic!("wrong variant"),
        }
        // A subject cannot survive the round trip: the type has nowhere to put it.
        assert!(out.to_json().get("subject").is_none());
    }

    #[test]
    fn an_unknown_channel_names_the_known_ones() {
        let err = Outbound::from_json(&json!({"channel": "carrier-pigeon", "to": "x"}))
            .expect_err("unknown channel");
        assert!(err.detail.contains("one of: email, sms"), "{err:?}");
    }

    #[test]
    fn a_malformed_address_is_a_400_at_the_edge() {
        let err = Outbound::from_json(&json!({
            "channel": "email", "to": "not-an-address", "subject": "s", "text": "t"
        }))
        .expect_err("bad address");
        assert!(err.detail.contains("is not an email address"), "{err:?}");
    }
}
