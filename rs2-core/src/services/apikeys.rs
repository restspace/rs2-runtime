//! `apikeys`: long-lived, revocable API keys, for clients that cannot run a
//! login flow (an MCP client, a script, CI). Mounting it is what turns keys
//! on: without the mount a key is just an unrecognised bearer token.
//!
//! - `POST /<mount>` `{name, roles?, expiresInDays?}` mints a key. Its roles
//!   are a subset of the caller's; it is returned once and never stored.
//! - `GET /<mount>` lists the caller's keys (all keys for `adminRole`), and
//!   `DELETE /<mount>/<id>` revokes one.
//! - As an [`Authenticator`] it accepts `Authorization: Bearer rs2k_…`; the
//!   principal is the key's owner with the key's roles and `kind: "key"`.
//!
//! A key is `rs2k_<id>_<secret>`. Its record is stored under `<id>` with an
//! HMAC "seal" over the secret and every field, keyed by the tenant's
//! `auth.jwtSecret`. So the record holds nothing that lets anyone rebuild
//! the key, and a record written or edited through some other path (a
//! `data` mount on the same store) fails verification instead of granting
//! its roles. Rotating `auth.jwtSecret` invalidates every key, as it does
//! every token.

use async_trait::async_trait;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde_json::{json, Map, Value};
use sha2::Sha256;

use crate::authn::{AuthOutcome, Authenticator};
use crate::error::RsError;
use crate::message::{Message, Principal};

use super::{Service, ServiceContext};

type HmacSha256 = Hmac<Sha256>;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Every key starts with this, so authenticators can tell keys from tokens.
pub const API_KEY_PREFIX: &str = "rs2k_";
/// The principal `kind` of a request authenticated by an API key.
pub const API_KEY_KIND: &str = "key";

const DAY_SECS: u64 = 24 * 60 * 60;
const MAX_NAME_CHARS: usize = 100;

/// `apikeys` service config (beyond the base envelope).
#[derive(Debug, Clone, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct ApiKeysSettings {
    /// The longest lifetime a key may have, in days (default 90). Keys never
    /// outlive it, so a key whose owner lost a role lapses within this time.
    pub max_days: u64,
    /// A role that may list and revoke every key, not just its own.
    pub admin_role: Option<String>,
    /// Dataset holding key records (default `apikeys`).
    pub dataset: String,
}

impl Default for ApiKeysSettings {
    fn default() -> Self {
        ApiKeysSettings {
            max_days: 90,
            admin_role: None,
            dataset: "apikeys".into(),
        }
    }
}

pub struct ApiKeysService {
    settings: ApiKeysSettings,
    /// The tenant's `auth.jwtSecret`: the seal key.
    seal_key: String,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// JSON with object keys sorted at every level, so a seal computed on one
/// host verifies on the other (`rs2 sync` copies records between them).
fn canonical(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let fields: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{}:{}", Value::String(k.clone()), canonical(&m[k])))
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(canonical).collect::<Vec<_>>().join(",")
        ),
        other => other.to_string(),
    }
}

/// The fields a seal covers, in a fixed order.
fn seal_input(id: &str, secret: &str, record: &Value) -> String {
    canonical(&json!([
        "rs2-apikey-v1",
        id,
        secret,
        record.get("owner"),
        record.get("roles"),
        record.get("extra"),
        record.get("created"),
        record.get("expires"),
        record.get("name"),
    ]))
}

fn split_key(key: &str) -> Option<(&str, &str)> {
    let (id, secret) = key.strip_prefix(API_KEY_PREFIX)?.split_once('_')?;
    let id_ok = !id.is_empty() && id.bytes().all(|b| b.is_ascii_hexdigit());
    (id_ok && !secret.is_empty()).then_some((id, secret))
}

fn roles_of(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => s.split_whitespace().map(String::from).collect(),
        Value::Array(a) => a
            .iter()
            .filter_map(|r| r.as_str())
            .flat_map(|s| s.split_whitespace())
            .map(String::from)
            .collect(),
        _ => vec![],
    }
}

/// A record as listed: never the seal.
fn public_view(id: &str, record: &Value) -> Value {
    json!({
        "id": id,
        "name": record.get("name"),
        "owner": record.get("owner"),
        "roles": record.get("roles"),
        "created": record.get("created"),
        "expires": record.get("expires"),
    })
}

impl ApiKeysService {
    pub fn from_config(mount_config: &Value, tenant_auth: Option<&Value>) -> Result<Self, RsError> {
        let mut settings = mount_config.clone();
        if let Some(m) = settings.as_object_mut() {
            m.remove("access");
        }
        let settings: ApiKeysSettings = serde_json::from_value(settings)
            .map_err(|e| RsError::bad_request(format!("invalid apikeys config: {e}")))?;
        if settings.max_days == 0 {
            return Err(RsError::bad_request("apikeys 'maxDays' must be at least 1"));
        }
        let seal_key = tenant_auth
            .and_then(|a| a.get("jwtSecret"))
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                RsError::bad_request(
                    "apikeys requires a 'jwtSecret' in the tenant config's auth settings \
                     (it seals key records)",
                )
            })?
            .to_string();
        Ok(ApiKeysService { settings, seal_key })
    }

    fn seal(&self, id: &str, secret: &str, record: &Value) -> Result<String, RsError> {
        let mut mac = HmacSha256::new_from_slice(self.seal_key.as_bytes())
            .map_err(|e| RsError::internal(format!("bad seal key: {e}")))?;
        mac.update(seal_input(id, secret, record).as_bytes());
        Ok(B64.encode(mac.finalize().into_bytes()))
    }

    fn seal_matches(&self, id: &str, secret: &str, record: &Value) -> bool {
        let Some(sealed) = record
            .get("seal")
            .and_then(|s| s.as_str())
            .and_then(|s| B64.decode(s).ok())
        else {
            return false;
        };
        let Ok(mut mac) = HmacSha256::new_from_slice(self.seal_key.as_bytes()) else {
            return false;
        };
        mac.update(seal_input(id, secret, record).as_bytes());
        mac.verify_slice(&sealed).is_ok()
    }

    fn is_admin(&self, p: &Principal) -> bool {
        self.settings
            .admin_role
            .as_deref()
            .is_some_and(|r| p.roles.iter().any(|have| have == r))
    }

    async fn create(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let caller = msg
            .principal
            .clone()
            .ok_or_else(|| RsError::unauthorized("creating an API key requires a login"))?;
        if caller.kind == API_KEY_KIND {
            return Err(RsError::forbidden(
                "a request authenticated by an API key cannot create keys",
            ));
        }
        let body = match &mut msg.body {
            Some(b) => b.as_json(ctx.limits.materialized_body_bytes).await?,
            None => json!({}),
        };
        let name = body
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .ok_or_else(|| RsError::bad_request("an API key needs a 'name'"))?;
        if name.chars().count() > MAX_NAME_CHARS {
            return Err(RsError::bad_request(format!(
                "an API key 'name' is at most {MAX_NAME_CHARS} characters"
            )));
        }
        let roles = match body.get("roles") {
            None | Some(Value::Null) => caller.roles.clone(),
            Some(v @ (Value::String(_) | Value::Array(_))) => roles_of(v),
            Some(_) => {
                return Err(RsError::bad_request(
                    "'roles' is a space-separated string or an array of role names",
                ))
            }
        };
        if let Some(extra) = roles.iter().find(|r| !caller.roles.contains(r)) {
            return Err(RsError::forbidden(format!(
                "an API key cannot have role '{extra}', which the caller lacks"
            )));
        }
        let days = match body.get("expiresInDays") {
            None | Some(Value::Null) => self.settings.max_days,
            Some(v) => v
                .as_u64()
                .filter(|d| (1..=self.settings.max_days).contains(d))
                .ok_or_else(|| {
                    RsError::bad_request(format!(
                        "'expiresInDays' is a whole number from 1 to {}",
                        self.settings.max_days
                    ))
                })?,
        };

        let data = ctx
            .data
            .as_ref()
            .ok_or_else(|| RsError::internal("apikeys has no data capability"))?;
        let mut rng = rand::rngs::OsRng;
        let mut id_bytes = [0u8; 8];
        let mut secret_bytes = [0u8; 32];
        rng.fill_bytes(&mut id_bytes);
        rng.fill_bytes(&mut secret_bytes);
        let id: String = id_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let secret = B64.encode(secret_bytes);
        let created = now_secs();
        let mut record = json!({
            "name": name,
            "owner": caller.id,
            "roles": roles.join(" "),
            "extra": caller.extra,
            "created": created,
            "expires": created + days * DAY_SECS,
        });
        let seal = self.seal(&id, &secret, &record)?;
        record["seal"] = json!(seal);
        data.put(&self.settings.dataset, &id, record.clone())
            .await?;

        let mut view = public_view(&id, &record);
        view["key"] = json!(format!("{API_KEY_PREFIX}{id}_{secret}"));
        let mut resp = msg.ok_json(&view);
        resp.status = Some(http::StatusCode::CREATED);
        let location = format!("{}/{id}", msg.url.base_path.trim_end_matches('/'));
        resp.set_header("location", &location);
        Ok(resp)
    }

    async fn list(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let caller = msg
            .principal
            .clone()
            .ok_or_else(|| RsError::unauthorized("listing API keys requires a login"))?;
        let data = ctx
            .data
            .as_ref()
            .ok_or_else(|| RsError::internal("apikeys has no data capability"))?;
        let admin = self.is_admin(&caller);
        let mut keys = vec![];
        let mut skip = 0;
        loop {
            let (ids, total) = data.list_keys(&self.settings.dataset, 500, skip).await?;
            if ids.is_empty() {
                break;
            }
            skip += ids.len();
            for id in ids {
                let Ok(record) = data.get(&self.settings.dataset, &id).await else {
                    continue;
                };
                if admin || record.get("owner").and_then(|o| o.as_str()) == Some(&caller.id) {
                    keys.push(public_view(&id, &record));
                }
            }
            if skip as u64 >= total {
                break;
            }
        }
        Ok(msg.ok_json(&Value::Array(keys)))
    }

    async fn revoke(
        &self,
        msg: Message,
        id: &str,
        ctx: &ServiceContext,
    ) -> Result<Message, RsError> {
        let caller = msg
            .principal
            .clone()
            .ok_or_else(|| RsError::unauthorized("revoking an API key requires a login"))?;
        let data = ctx
            .data
            .as_ref()
            .ok_or_else(|| RsError::internal("apikeys has no data capability"))?;
        let not_found = || RsError::not_found(format!("no API key '{id}'"));
        let record = data
            .get(&self.settings.dataset, id)
            .await
            .map_err(|_| not_found())?;
        let owns = record.get("owner").and_then(|o| o.as_str()) == Some(&caller.id);
        if !owns && !self.is_admin(&caller) {
            // Someone else's key: indistinguishable from no key.
            return Err(not_found());
        }
        data.delete(&self.settings.dataset, id).await?;
        Ok(msg.no_content())
    }
}

#[async_trait]
impl Service for ApiKeysService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let segments: Vec<String> = msg
            .url
            .service_segments()
            .iter()
            .map(|s| s.to_string())
            .collect();
        match (&msg.method, segments.as_slice()) {
            (&http::Method::POST, []) => self.create(msg, ctx).await,
            (&http::Method::GET, []) => self.list(msg, ctx).await,
            (&http::Method::DELETE, [id]) => self.revoke(msg, id, ctx).await,
            _ => Err(RsError::not_found(format!(
                "apikeys endpoint '{}' (have: POST / to create, GET / to list, DELETE /<id> to revoke)",
                msg.url.service_path
            ))),
        }
    }

    fn authenticator(&self) -> Option<&dyn Authenticator> {
        Some(self)
    }
}

/// Keys come only in `Authorization: Bearer`, never a cookie or the
/// WebSocket subprotocol, so a key is never an ambient credential.
#[async_trait]
impl Authenticator for ApiKeysService {
    async fn try_authenticate(
        &self,
        msg: &mut Message,
        ctx: &ServiceContext,
    ) -> Result<AuthOutcome, RsError> {
        let Some(key) = msg
            .header("authorization")
            .and_then(|a| a.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|k| k.starts_with(API_KEY_PREFIX))
            .map(str::to_string)
        else {
            return Ok(AuthOutcome::Pass);
        };
        let (id, secret) =
            split_key(&key).ok_or_else(|| RsError::unauthorized("malformed API key"))?;
        let data = ctx
            .data
            .as_ref()
            .ok_or_else(|| RsError::internal("apikeys has no data capability"))?;
        let refused = || RsError::unauthorized("unknown or revoked API key");
        let record = match data.get(&self.settings.dataset, id).await {
            Ok(r) => r,
            Err(e) if e.status == 404 => return Err(refused()),
            Err(e) => return Err(e),
        };
        if !self.seal_matches(id, secret, &record) {
            return Err(refused());
        }
        let expires = record.get("expires").and_then(|e| e.as_u64()).unwrap_or(0);
        if expires < now_secs() {
            return Err(RsError::unauthorized("API key expired"));
        }
        let mut extra = match record.get("extra") {
            Some(Value::Object(m)) => m.clone(),
            _ => Map::new(),
        };
        extra.insert("keyId".into(), json!(id));
        msg.principal = Some(Principal {
            id: record
                .get("owner")
                .and_then(|o| o.as_str())
                .unwrap_or_default()
                .to_string(),
            roles: roles_of(record.get("roles").unwrap_or(&Value::Null)),
            kind: API_KEY_KIND.into(),
            extra,
        });
        Ok(AuthOutcome::Authenticated {
            expires: Some(expires),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_sorts_keys_at_every_level() {
        let a = json!({ "b": 1, "a": { "y": [1, { "q": 2, "p": 1 }], "x": "s" } });
        assert_eq!(
            canonical(&a),
            r#"{"a":{"x":"s","y":[1,{"p":1,"q":2}]},"b":1}"#
        );
    }

    #[test]
    fn key_splitting() {
        assert_eq!(split_key("rs2k_0a1b_sec_ret"), Some(("0a1b", "sec_ret")));
        assert_eq!(split_key("rs2k_xyz_secret"), None, "id is hex");
        assert_eq!(split_key("rs2k__secret"), None);
        assert_eq!(split_key("rs2k_0a1b_"), None);
        assert_eq!(split_key("rs2k_0a1b"), None);
        assert_eq!(split_key("token"), None);
    }

    #[test]
    fn a_seal_covers_every_field_and_the_secret() {
        let svc =
            ApiKeysService::from_config(&json!({}), Some(&json!({ "jwtSecret": "s" }))).unwrap();
        let record = json!({ "name": "ci", "owner": "ann", "roles": "U", "extra": {},
                             "created": 1, "expires": 2 });
        let mut sealed = record.clone();
        sealed["seal"] = json!(svc.seal("ab", "secret", &record).unwrap());
        assert!(svc.seal_matches("ab", "secret", &sealed));
        assert!(!svc.seal_matches("ab", "other", &sealed));
        assert!(!svc.seal_matches("ac", "secret", &sealed));
        let mut escalated = sealed.clone();
        escalated["roles"] = json!("U A");
        assert!(!svc.seal_matches("ab", "secret", &escalated));
        let mut extended = sealed.clone();
        extended["expires"] = json!(99);
        assert!(!svc.seal_matches("ab", "secret", &extended));
    }

    /// The same vector is pinned in `rs2-worker/test/apikeys.test.ts`: a
    /// record sealed on one host must verify on the other (`rs2 sync`).
    #[test]
    fn seal_matches_the_worker_host() {
        let svc = ApiKeysService::from_config(
            &json!({}),
            Some(&json!({ "jwtSecret": "vector-secret" })),
        )
        .unwrap();
        let record = json!({
            "name": "ci key", "owner": "ann@example.com", "roles": "U W",
            "extra": { "tier": { "a": "é\n", "n": 2 }, "accountId": "acme" },
            "created": 1_700_000_000u64, "expires": 1_707_776_000u64
        });
        assert_eq!(
            svc.seal("00ff", "s3cret", &record).unwrap(),
            "tIOq7m8FMkSZWRuhiE2bb7Je4g3M3XRfBDVPyS8OE1U"
        );
    }
}
