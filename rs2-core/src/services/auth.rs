//! `auth` — authentication & RBAC (PRD §10.5).
//!
//! JWT (HMAC-SHA512, constant-time verify) in the `rs-auth` HttpOnly cookie
//! or bearer header; sliding refresh past 50% of the session; login lockout;
//! user records resolved from the data store; argon2id for new hashes with
//! bcrypt verified for migration. Role specs are enforced per mount by the
//! wrapper ([`crate::wrapper::authorize`]).
//!
//! M2 deviations (tracked): lockout state is node-local (the PRD puts it in
//! the shared state store); TOTP MFA and impersonation are deferred.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::Sha512;

use crate::error::RsError;
use crate::message::{Message, Principal};

use super::{Service, ServiceContext};

type HmacSha512 = Hmac<Sha512>;

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Tenant auth settings (the tenant config's `auth` object).
#[derive(Debug, Clone, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct AuthSettings {
    /// Per-tenant token signing key (PRD §10.5).
    #[schemars(schema_with = "crate::config_schema::secret_string_schema")]
    pub jwt_secret: String,
    pub session_minutes: u64,
    pub max_attempts: u32,
    pub lock_minutes: u64,
    /// Dataset holding user records keyed by email.
    pub user_dataset: String,
    /// Browser origins allowed to call login/refresh (v1's
    /// `allowedLoginDomains`); empty = no origin restriction. Same-origin
    /// requests are always allowed.
    pub allowed_login_origins: Vec<String>,
    /// User-record fields copied into the token as extra claims (and exposed
    /// on the [`Principal`]). These become bearer authority for the session,
    /// so every named field must be operator-writable-only in the user
    /// dataset (enforce via the data mount's field-level authz); never name a
    /// self-writable field here.
    pub jwt_user_props: Vec<String>,
}

impl Default for AuthSettings {
    fn default() -> Self {
        AuthSettings {
            jwt_secret: String::new(),
            session_minutes: 60,
            max_attempts: 5,
            lock_minutes: 10,
            user_dataset: "users".to_string(),
            allowed_login_origins: vec![],
            jwt_user_props: vec![],
        }
    }
}

/// JWT claims carried by RS2 tokens.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    pub sub: String,
    pub roles: String,
    /// `user` or `agent` — agent principals are first-class (PRD §10.5).
    pub kind: String,
    pub iat: u64,
    pub exp: u64,
    /// Extra claims copied from the user record per `jwtUserProps`. Absent
    /// from the payload when empty, so pre-existing tokens verify unchanged.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Sign claims as a HS512 JWT.
pub fn sign(claims: &Claims, secret: &str) -> Result<String, RsError> {
    let header = B64.encode(br#"{"alg":"HS512","typ":"JWT"}"#);
    let payload =
        B64.encode(serde_json::to_vec(claims).map_err(|e| RsError::internal(e.to_string()))?);
    let signing_input = format!("{header}.{payload}");
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes())
        .map_err(|e| RsError::internal(format!("bad signing key: {e}")))?;
    mac.update(signing_input.as_bytes());
    let sig = B64.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{sig}"))
}

/// Verify a HS512 JWT (constant-time MAC comparison) and return its claims.
pub fn verify(token: &str, secret: &str) -> Result<Claims, RsError> {
    let mut parts = token.split('.');
    let (header, payload, sig) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(h), Some(p), Some(s), None) => (h, p, s),
        _ => return Err(RsError::unauthorized("malformed token")),
    };
    let header_json: serde_json::Value = serde_json::from_slice(
        &B64.decode(header)
            .map_err(|_| RsError::unauthorized("malformed token header"))?,
    )
    .map_err(|_| RsError::unauthorized("malformed token header"))?;
    if header_json.get("alg").and_then(|a| a.as_str()) != Some("HS512") {
        return Err(RsError::unauthorized("unsupported token algorithm"));
    }
    let mut mac = HmacSha512::new_from_slice(secret.as_bytes())
        .map_err(|e| RsError::internal(format!("bad signing key: {e}")))?;
    mac.update(format!("{header}.{payload}").as_bytes());
    let sig_bytes = B64
        .decode(sig)
        .map_err(|_| RsError::unauthorized("malformed signature"))?;
    mac.verify_slice(&sig_bytes)
        .map_err(|_| RsError::unauthorized("invalid token signature"))?;
    let claims: Claims = serde_json::from_slice(
        &B64.decode(payload)
            .map_err(|_| RsError::unauthorized("malformed token payload"))?,
    )
    .map_err(|_| RsError::unauthorized("malformed token payload"))?;
    if claims.exp < now_secs() {
        return Err(RsError::unauthorized("token expired"));
    }
    Ok(claims)
}

/// Extract the token from the `rs-auth` cookie or `Authorization: Bearer`.
pub fn extract_token(msg: &Message) -> Option<String> {
    if let Some(auth) = msg.header("authorization") {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            return Some(token.trim().to_string());
        }
    }
    let cookies = msg.header("cookie")?;
    cookies.split(';').find_map(|c| {
        let (k, v) = c.trim().split_once('=')?;
        if k == "rs-auth" {
            Some(v.to_string())
        } else {
            None
        }
    })
}

/// Verify the request's token (if any) into a [`Principal`].
pub fn principal_from_token(msg: &Message, secret: &str) -> Result<Option<Principal>, RsError> {
    let Some(token) = extract_token(msg) else {
        return Ok(None);
    };
    let claims = verify(&token, secret)?;
    Ok(Some(Principal {
        id: claims.sub,
        roles: claims.roles.split_whitespace().map(String::from).collect(),
        kind: claims.kind,
        extra: claims.extra,
    }))
}

/// Hash a password with argon2id (the at-rest format for new hashes).
pub fn hash_password(password: &str) -> Result<String, RsError> {
    use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut OsRng);
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| RsError::internal(format!("hashing failed: {e}")))
}

/// Verify a password against an argon2id PHC string or a legacy bcrypt hash
/// (PRD §10.5: bcrypt verified for migration; new hashes are argon2id).
pub fn verify_password(password: &str, hash: &str) -> bool {
    if hash.starts_with("$argon2") {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        PasswordHash::new(hash)
            .map(|parsed| {
                argon2::Argon2::default()
                    .verify_password(password.as_bytes(), &parsed)
                    .is_ok()
            })
            .unwrap_or(false)
    } else if hash.starts_with("$2") {
        bcrypt::verify(password, hash).unwrap_or(false)
    } else {
        false
    }
}

#[derive(Default)]
struct LockoutState {
    failures: u32,
    locked_until: Option<Instant>,
}

pub struct AuthService {
    settings: AuthSettings,
    /// Login lockout per user. Node-local in M2 (PRD wants the shared
    /// state store; the trait seam is the data capability when it lands).
    lockouts: Mutex<HashMap<String, LockoutState>>,
}

impl AuthService {
    /// Mount config may override tenant-level `auth` settings.
    pub fn from_config(
        mount_config: &serde_json::Value,
        tenant_auth: Option<&serde_json::Value>,
    ) -> Result<Self, RsError> {
        let mut merged = tenant_auth.cloned().unwrap_or(serde_json::json!({}));
        if let (Some(m), Some(o)) = (merged.as_object_mut(), mount_config.as_object()) {
            for (k, v) in o {
                if k != "access" {
                    m.insert(k.clone(), v.clone());
                }
            }
        }
        let settings: AuthSettings = serde_json::from_value(merged)
            .map_err(|e| RsError::bad_request(format!("invalid auth settings: {e}")))?;
        if settings.jwt_secret.is_empty() {
            return Err(RsError::bad_request(
                "auth requires a 'jwtSecret' in the tenant config's auth settings",
            ));
        }
        Ok(AuthService {
            settings,
            lockouts: Mutex::new(HashMap::new()),
        })
    }

    fn check_lockout(&self, user: &str) -> Result<(), RsError> {
        let lockouts = self.lockouts.lock().unwrap();
        if let Some(state) = lockouts.get(user) {
            if let Some(until) = state.locked_until {
                if Instant::now() < until {
                    let mut err = RsError::unauthorized("account temporarily locked");
                    err.retry_after_ms = Some((until - Instant::now()).as_millis() as u64);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn record_failure(&self, user: &str) {
        let mut lockouts = self.lockouts.lock().unwrap();
        // Entries are otherwise only removed on that user's successful login,
        // so a spray of invented usernames would grow the map for the mount's
        // lifetime. Past a threshold, keep only currently-locked accounts —
        // losing a sub-threshold failure count is harmless.
        if lockouts.len() >= 10_000 {
            let now = Instant::now();
            lockouts.retain(|_, s| s.locked_until.is_some_and(|until| until > now));
        }
        let state = lockouts.entry(user.to_string()).or_default();
        state.failures += 1;
        if state.failures >= self.settings.max_attempts {
            state.locked_until =
                Some(Instant::now() + Duration::from_secs(self.settings.lock_minutes * 60));
            state.failures = 0;
        }
    }

    fn clear_failures(&self, user: &str) {
        self.lockouts.lock().unwrap().remove(user);
    }

    fn issue(
        &self,
        sub: &str,
        roles: &str,
        kind: &str,
        extra: serde_json::Map<String, serde_json::Value>,
    ) -> Result<(String, u64), RsError> {
        let iat = now_secs();
        let exp = iat + self.settings.session_minutes * 60;
        let claims = Claims {
            sub: sub.to_string(),
            roles: roles.to_string(),
            kind: kind.to_string(),
            iat,
            exp,
            extra,
        };
        Ok((sign(&claims, &self.settings.jwt_secret)?, exp))
    }

    /// v1's login-origin allowlist: when configured, cross-origin browser
    /// calls to login/refresh must come from a listed origin.
    fn check_login_origin(&self, msg: &Message) -> Result<(), RsError> {
        let Some(origin) = msg.header("origin") else {
            return Ok(());
        };
        if crate::wrapper::is_same_origin(origin, msg.header("host")) {
            return Ok(());
        }
        if self.settings.allowed_login_origins.is_empty()
            || self
                .settings
                .allowed_login_origins
                .iter()
                .any(|p| crate::wrapper::origin_matches(p, origin))
        {
            Ok(())
        } else {
            Err(RsError::forbidden(format!(
                "login origin '{origin}' is not allowed"
            )))
        }
    }

    /// Cookie attributes by origin trust (v1's matrix): same-origin (or
    /// non-browser) → `SameSite=Strict`; trusted cross-origin →
    /// `SameSite=None; Secure`; untrusted origin → no cookie, the body
    /// token is the credential.
    fn token_response(
        &self,
        msg: &Message,
        ctx: &ServiceContext,
        token: &str,
        exp: u64,
    ) -> Message {
        let mut resp = msg.ok_json(&serde_json::json!({ "token": token, "exp": exp }));
        let max_age = self.settings.session_minutes * 60;
        let cookie = match msg.header("origin") {
            None => Some(format!(
                "rs-auth={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}"
            )),
            Some(origin) if crate::wrapper::is_same_origin(origin, msg.header("host")) => Some(
                format!("rs-auth={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}"),
            ),
            Some(origin) if ctx.cors.is_trusted(origin) => Some(format!(
                "rs-auth={token}; HttpOnly; SameSite=None; Secure; Path=/; Max-Age={max_age}"
            )),
            Some(_) => None, // untrusted browser origin: bearer-only
        };
        if let Some(cookie) = cookie {
            if let Ok(v) = http::HeaderValue::from_str(&cookie) {
                resp.headers.insert(http::header::SET_COOKIE, v);
            }
        }
        resp
    }

    async fn login(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        self.check_login_origin(&msg)?;
        let body = match &mut msg.body {
            Some(b) => b.as_json(ctx.limits.materialized_body_bytes).await?,
            None => return Err(RsError::bad_request("login requires a JSON body")),
        };
        let email = body
            .get("email")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RsError::bad_request("login requires 'email'"))?
            .to_string();
        let password = body
            .get("password")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RsError::bad_request("login requires 'password'"))?
            .to_string();

        self.check_lockout(&email)?;

        let data = ctx
            .data
            .as_ref()
            .ok_or_else(|| RsError::internal("auth service has no data capability"))?;
        let user = match data.get(&self.settings.user_dataset, &email).await {
            Ok(u) => u,
            Err(_) => {
                // Indistinguishable from a bad password (no user enumeration).
                self.record_failure(&email);
                ctx.logger
                    .at(&msg.trace)
                    .warn(format!("login failed for '{email}' (unknown user)"));
                return Err(RsError::unauthorized("invalid credentials"));
            }
        };
        let hash = user
            .get("passwordHash")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !verify_password(&password, hash) {
            self.record_failure(&email);
            ctx.logger
                .at(&msg.trace)
                .warn(format!("login failed for '{email}' (bad password)"));
            return Err(RsError::unauthorized("invalid credentials"));
        }
        self.clear_failures(&email);

        let roles = match user.get("roles") {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Array(a)) => a
                .iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(" "),
            _ => "U".to_string(),
        };
        let kind = user.get("kind").and_then(|v| v.as_str()).unwrap_or("user");
        let mut extra = serde_json::Map::new();
        for prop in &self.settings.jwt_user_props {
            if let Some(v) = user.get(prop) {
                if !v.is_null() {
                    extra.insert(prop.clone(), v.clone());
                }
            }
        }
        let (token, exp) = self.issue(&email, &roles, kind, extra)?;
        Ok(self.token_response(&msg, ctx, &token, exp))
    }

    /// Sliding refresh: a valid token past 50% of its session re-issues
    /// (PRD §10.5).
    fn refresh(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        self.check_login_origin(&msg)?;
        let token =
            extract_token(&msg).ok_or_else(|| RsError::unauthorized("refresh requires a token"))?;
        let claims = verify(&token, &self.settings.jwt_secret)?;
        let now = now_secs();
        let halfway = claims.iat + (claims.exp - claims.iat) / 2;
        if now < halfway {
            // Not yet refreshable: return the existing token unchanged.
            return Ok(msg.ok_json(&serde_json::json!({ "token": token, "exp": claims.exp })));
        }
        let (token, exp) = self.issue(
            &claims.sub,
            &claims.roles,
            &claims.kind,
            claims.extra.clone(),
        )?;
        Ok(self.token_response(&msg, ctx, &token, exp))
    }

    fn logout(&self, msg: Message) -> Message {
        let mut resp = msg.no_content();
        if let Ok(v) =
            http::HeaderValue::from_str("rs-auth=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0")
        {
            resp.headers.insert(http::header::SET_COOKIE, v);
        }
        resp
    }
}

#[async_trait]
impl Service for AuthService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let segments: Vec<String> = msg
            .url
            .service_segments()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let segments: Vec<&str> = segments.iter().map(|s| s.as_str()).collect();
        match (&msg.method, segments.as_slice()) {
            (&http::Method::POST, ["login"]) => self.login(msg, ctx).await,
            (&http::Method::POST, ["refresh"]) => self.refresh(msg, ctx),
            (&http::Method::POST, ["logout"]) => Ok(self.logout(msg)),
            (&http::Method::GET, ["user"]) => match &msg.principal {
                Some(p) => {
                    let mut body = serde_json::Map::new();
                    body.insert("id".into(), serde_json::json!(p.id));
                    body.insert("roles".into(), serde_json::json!(p.roles));
                    body.insert("kind".into(), serde_json::json!(p.kind));
                    // Extra claims ride along; id/roles/kind win name clashes.
                    for (k, v) in &p.extra {
                        body.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                    Ok(msg.ok_json(&serde_json::Value::Object(body)))
                }
                None => Err(RsError::unauthorized("no authenticated principal")),
            },
            _ => Err(RsError::not_found(format!(
                "auth endpoint '{}' (have: POST login/refresh/logout, GET user)",
                msg.url.service_path
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_round_trip_and_tamper_rejection() {
        let claims = Claims {
            sub: "ada@example.com".into(),
            roles: "A U".into(),
            kind: "user".into(),
            iat: now_secs(),
            exp: now_secs() + 3600,
            extra: serde_json::Map::new(),
        };
        let token = sign(&claims, "secret-key").unwrap();
        let back = verify(&token, "secret-key").unwrap();
        assert_eq!(back.sub, "ada@example.com");
        assert_eq!(back.roles, "A U");
        assert!(verify(&token, "wrong-key").is_err());
        let tampered = format!("{}x", token);
        assert!(verify(&tampered, "secret-key").is_err());
        // Expired token.
        let expired = Claims {
            exp: now_secs() - 10,
            ..claims
        };
        let token = sign(&expired, "secret-key").unwrap();
        assert!(verify(&token, "secret-key").is_err());
    }

    #[test]
    fn extra_claims_round_trip_and_extra_free_tokens_stay_compatible() {
        let mut extra = serde_json::Map::new();
        extra.insert("accountId".into(), serde_json::json!("acc-42"));
        let claims = Claims {
            sub: "ada@example.com".into(),
            roles: "U".into(),
            kind: "user".into(),
            iat: now_secs(),
            exp: now_secs() + 3600,
            extra,
        };
        let token = sign(&claims, "secret-key").unwrap();
        let back = verify(&token, "secret-key").unwrap();
        assert_eq!(back.extra["accountId"], "acc-42");

        // With no extra claims the payload omits the field entirely, so the
        // token bytes match the pre-`extra` format and old tokens verify.
        let old = Claims {
            extra: serde_json::Map::new(),
            ..claims
        };
        let token = sign(&old, "secret-key").unwrap();
        let payload =
            String::from_utf8(B64.decode(token.split('.').nth(1).unwrap()).unwrap()).unwrap();
        assert!(
            !payload.contains("extra"),
            "empty extra must not appear in the payload"
        );
        let back = verify(&token, "secret-key").unwrap();
        assert!(back.extra.is_empty());
    }

    #[test]
    fn password_hash_and_verify_argon2_and_bcrypt() {
        let hash = hash_password("hunter2").unwrap();
        assert!(hash.starts_with("$argon2"));
        assert!(verify_password("hunter2", &hash));
        assert!(!verify_password("wrong", &hash));
        // bcrypt migration path.
        let bhash = bcrypt::hash("legacy-pass", 4).unwrap();
        assert!(verify_password("legacy-pass", &bhash));
        assert!(!verify_password("wrong", &bhash));
        // Unknown formats never verify.
        assert!(!verify_password("x", "plaintext"));
    }

    #[test]
    fn token_extraction_from_cookie_and_bearer() {
        let mut msg = Message::request(http::Method::GET, "/x", "t");
        assert_eq!(extract_token(&msg), None);
        msg.set_header("cookie", "a=1; rs-auth=tok123; b=2");
        assert_eq!(extract_token(&msg).as_deref(), Some("tok123"));
        let mut msg2 = Message::request(http::Method::GET, "/x", "t");
        msg2.set_header("authorization", "Bearer tok456");
        assert_eq!(extract_token(&msg2).as_deref(), Some("tok456"));
    }
}
