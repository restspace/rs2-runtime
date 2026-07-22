//! Host-side outbound credential injection (the "proxy adapter" use case).
//!
//! A [`CredentialInjector`] applies an [`AuthStrategy`] to a [`Message`] just
//! before it leaves the host (in the `httpOut` grant target, or the `proxy`
//! service). The secret material is resolved **host-side** — from an operator
//! `infra:` definition (its `infra_only` fields, never readable by the tenant)
//! or a granted tenant `secret:` — and lives only inside this injector, never in
//! any guest- or tenant-visible `config`. `UreqHttpOut` stays a dumb transport;
//! all credential logic is here.
//!
//! Strategies that only add headers/query (`bearer`/`header`/`basic`/`query`)
//! leave the body untouched and are **streaming-safe**. Signing strategies
//! (`hmac`/`awsSigV4`) must hash the request body, so they **materialize** it
//! first (bounded by the caller's cap).

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use http::header::{HeaderName, HeaderValue};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::Value;
use time::OffsetDateTime;

use crate::crypto::{hmac_bytes, hmac_sha256, sha256_hex, to_hex};
use crate::error::RsError;
use crate::message::Message;

/// RFC 3986 unreserved set (`A-Z a-z 0-9 - _ . ~`) — everything else is
/// percent-encoded. Used for query values and SigV4 canonicalization.
const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');
/// As [`UNRESERVED`] but keeps `/` literal — for a canonical URI path.
const UNRESERVED_PATH: &AsciiSet = &UNRESERVED.remove(b'/');

fn enc(s: &str) -> String {
    utf8_percent_encode(s, UNRESERVED).to_string()
}

/// Re-canonicalize an already wire-encoded component: percent-decode it, then
/// re-encode with `set`. SigV4 canonicalization must run over the *decoded*
/// value — the query/path we sign arrive percent-encoded on the wire (see
/// `MsgUrl`), and the verifying peer decodes before it canonicalizes. Encoding
/// the raw wire form directly would double-encode (`%20` → `%2520`) and break
/// the signature for any request with reserved characters.
fn recanon(s: &str, set: &'static AsciiSet) -> String {
    let decoded = percent_decode_str(s).decode_utf8_lossy();
    utf8_percent_encode(&decoded, set).to_string()
}

/// How to authenticate an outbound request. The secret-bearing variants are
/// constructed from operator infra / granted secrets, never echoed back out.
#[derive(Debug, Clone)]
pub enum AuthStrategy {
    /// `Authorization: Bearer <token>`.
    Bearer { token: String },
    /// An arbitrary header (e.g. `X-Api-Key: <value>`).
    Header { name: String, value: String },
    /// `Authorization: Basic base64(user:pass)`.
    Basic { username: String, password: String },
    /// Append `?<name>=<value>` to the query (e.g. `?api_key=…`).
    QueryParam { name: String, value: String },
    /// HMAC the request body and place the hex MAC in `header`.
    Hmac {
        algorithm: String,
        secret: String,
        header: String,
    },
    /// AWS Signature Version 4 (`Authorization` + `X-Amz-Date`).
    AwsSigV4 {
        access_key: String,
        secret_key: String,
        region: String,
        service: String,
    },
}

/// A resolved credential to apply to outbound messages.
#[derive(Debug, Clone)]
pub struct CredentialInjector {
    strategy: AuthStrategy,
}

impl CredentialInjector {
    pub fn new(strategy: AuthStrategy) -> Self {
        CredentialInjector { strategy }
    }

    /// Build an injector from an **already infra-merged** config value. Returns
    /// `Ok(None)` when there is no `auth` key (a grant/mount without injection),
    /// `Err` on a malformed strategy. The secret fields are read here and kept
    /// inside the returned injector — callers must not place them in any
    /// guest-visible config.
    pub fn from_config(merged: &Value) -> Result<Option<Self>, RsError> {
        let Some(auth) = merged.get("auth").and_then(|a| a.as_str()) else {
            return Ok(None);
        };
        let s = |key: &str| -> Result<String, RsError> {
            merged
                .get(key)
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| RsError::bad_request(format!("auth '{auth}' requires '{key}'")))
        };
        let strategy = match auth {
            "bearer" => AuthStrategy::Bearer { token: s("token")? },
            "header" => AuthStrategy::Header {
                name: s("name")?,
                value: s("value")?,
            },
            "basic" => AuthStrategy::Basic {
                username: s("username")?,
                password: s("password")?,
            },
            "query" => AuthStrategy::QueryParam {
                name: s("name")?,
                value: s("value")?,
            },
            "hmac" => AuthStrategy::Hmac {
                algorithm: merged
                    .get("algorithm")
                    .and_then(|v| v.as_str())
                    .unwrap_or("sha256")
                    .to_string(),
                secret: s("secret")?,
                header: merged
                    .get("header")
                    .and_then(|v| v.as_str())
                    .unwrap_or("X-Signature")
                    .to_string(),
            },
            "awsSigV4" => AuthStrategy::AwsSigV4 {
                access_key: s("accessKeyId")?,
                secret_key: s("secretAccessKey")?,
                region: s("region")?,
                service: s("service")?,
            },
            other => {
                return Err(RsError::bad_request(format!(
                    "unknown auth strategy '{other}' (one of: bearer, header, basic, query, \
                     hmac, awsSigV4)"
                )))
            }
        };
        Ok(Some(CredentialInjector { strategy }))
    }

    /// Apply the credential to `msg` in place. `max_body_bytes` bounds body
    /// materialization for signing strategies; the header/query strategies
    /// ignore it and leave a streaming body untouched.
    pub async fn apply(&self, msg: &mut Message, max_body_bytes: u64) -> Result<(), RsError> {
        match &self.strategy {
            AuthStrategy::Bearer { token } => {
                set_header(msg, "authorization", &format!("Bearer {token}"))
            }
            AuthStrategy::Header { name, value } => set_header(msg, name, value),
            AuthStrategy::Basic { username, password } => {
                let creds = B64.encode(format!("{username}:{password}"));
                set_header(msg, "authorization", &format!("Basic {creds}"))
            }
            AuthStrategy::QueryParam { name, value } => {
                let pair = format!("{}={}", enc(name), enc(value));
                if msg.url.query.is_empty() {
                    msg.url.query = pair;
                } else {
                    msg.url.query.push('&');
                    msg.url.query.push_str(&pair);
                }
                Ok(())
            }
            AuthStrategy::Hmac {
                algorithm,
                secret,
                header,
            } => {
                let body = materialize(msg, max_body_bytes).await?;
                let mac = hmac_bytes(algorithm, secret.as_bytes(), &body).ok_or_else(|| {
                    RsError::bad_request(format!("hmac: unsupported algorithm '{algorithm}'"))
                })?;
                set_header(msg, header, &to_hex(&mac))
            }
            AuthStrategy::AwsSigV4 {
                access_key,
                secret_key,
                region,
                service,
            } => {
                let body = materialize(msg, max_body_bytes).await?;
                let (host, path) = split_url(&msg.url.path)?;
                let (authorization, amz_date) = sign_aws_sigv4(
                    msg.method.as_ref(),
                    &host,
                    &path,
                    &msg.url.query,
                    &body,
                    access_key,
                    secret_key,
                    region,
                    service,
                    OffsetDateTime::now_utc(),
                );
                set_header(msg, "x-amz-date", &amz_date)?;
                set_header(msg, "authorization", &authorization)
            }
        }
    }
}

fn set_header(msg: &mut Message, name: &str, value: &str) -> Result<(), RsError> {
    let name = HeaderName::try_from(name)
        .map_err(|_| RsError::bad_request(format!("invalid header name '{name}'")))?;
    let value = HeaderValue::from_str(value)
        .map_err(|_| RsError::bad_request("credential value is not a valid header value"))?;
    msg.headers.insert(name, value);
    Ok(())
}

/// Materialize the request body to bytes (empty when there is none).
async fn materialize(msg: &mut Message, max_body_bytes: u64) -> Result<Vec<u8>, RsError> {
    match &mut msg.body {
        None => Ok(Vec::new()),
        Some(b) => Ok(b.materialize(max_body_bytes).await?.to_vec()),
    }
}

/// Split an absolute URL into `(host, path)` — `host` is the authority (incl.
/// any port), `path` the absolute path (`/` when empty).
fn split_url(url: &str) -> Result<(String, String), RsError> {
    let rest = url
        .split_once("://")
        .map(|(_, r)| r)
        .ok_or_else(|| RsError::bad_request(format!("not an absolute URL: '{url}'")))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let path = path.split(['?', '#']).next().unwrap_or("/");
    Ok((
        authority.to_string(),
        if path.is_empty() {
            "/".into()
        } else {
            path.to_string()
        },
    ))
}

/// Compute an AWS SigV4 `Authorization` header (signing `host` + `x-amz-date`).
/// Returns `(authorization, x-amz-date)`. `datetime` is injectable for tests.
#[allow(clippy::too_many_arguments)]
fn sign_aws_sigv4(
    method: &str,
    host: &str,
    path: &str,
    query: &str,
    body: &[u8],
    access_key: &str,
    secret_key: &str,
    region: &str,
    service: &str,
    datetime: OffsetDateTime,
) -> (String, String) {
    let amz_date = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        datetime.year(),
        u8::from(datetime.month()),
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        datetime.second(),
    );
    let date_stamp = &amz_date[..8];

    let canonical_uri = canonical_path(path, service);
    let canonical_query = canonical_query_string(query);
    let payload_hash = sha256_hex(body);
    let canonical_headers = format!("host:{host}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-date";
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{secret_key}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = to_hex(&hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, \
         Signature={signature}"
    );
    (authorization, amz_date)
}

/// Canonical URI path for SigV4. The wire path arrives percent-encoded, so we
/// canonicalize from the decoded form (`recanon`, `/` preserved). Non-S3
/// services additionally **double-encode** each segment — the SigV4
/// canonical-URI rule the verifying peer applies (`%20` → `%2520`). S3 is the
/// documented exception: it signs the single-encoded path so object keys that
/// contain `%` or reserved characters are not altered.
fn canonical_path(path: &str, service: &str) -> String {
    let single = recanon(path, UNRESERVED_PATH);
    if service == "s3" {
        single
    } else {
        utf8_percent_encode(&single, UNRESERVED_PATH).to_string()
    }
}

/// Canonical query string: params sorted by encoded key (then value), each
/// key and value re-canonicalized (decode the wire form, then RFC-3986 encode),
/// joined by `&`.
fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (recanon(k, UNRESERVED), recanon(v, UNRESERVED))
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Body, MediaType};
    use http::Method;
    use serde_json::json;
    use time::{Date, Month, Time};

    fn req() -> Message {
        Message::request(Method::POST, "https://api.example.com/v1/send?b=2&a=1", "t")
    }

    #[tokio::test]
    async fn bearer_sets_authorization() {
        let inj = CredentialInjector::from_config(&json!({ "auth": "bearer", "token": "sk-123" }))
            .unwrap()
            .unwrap();
        let mut msg = req();
        inj.apply(&mut msg, 1 << 20).await.unwrap();
        assert_eq!(msg.header("authorization"), Some("Bearer sk-123"));
    }

    #[tokio::test]
    async fn header_and_basic_and_query() {
        let mut msg = req();
        CredentialInjector::from_config(
            &json!({ "auth": "header", "name": "X-Api-Key", "value": "k" }),
        )
        .unwrap()
        .unwrap()
        .apply(&mut msg, 1 << 20)
        .await
        .unwrap();
        assert_eq!(msg.header("x-api-key"), Some("k"));

        let mut msg = req();
        CredentialInjector::from_config(
            &json!({ "auth": "basic", "username": "u", "password": "p" }),
        )
        .unwrap()
        .unwrap()
        .apply(&mut msg, 1 << 20)
        .await
        .unwrap();
        assert_eq!(msg.header("authorization"), Some("Basic dTpw")); // base64("u:p")
    }

    #[tokio::test]
    async fn query_param_appends() {
        let mut msg = req();
        CredentialInjector::from_config(
            &json!({ "auth": "query", "name": "api_key", "value": "a b" }),
        )
        .unwrap()
        .unwrap()
        .apply(&mut msg, 1 << 20)
        .await
        .unwrap();
        assert_eq!(msg.url.query, "b=2&a=1&api_key=a%20b");
    }

    #[tokio::test]
    async fn hmac_signs_body() {
        let mut msg = req().with_body(Body::from_string("payload", MediaType::json()));
        CredentialInjector::from_config(
            &json!({ "auth": "hmac", "algorithm": "sha256", "secret": "Jefe", "header": "X-Sig" }),
        )
        .unwrap()
        .unwrap()
        .apply(&mut msg, 1 << 20)
        .await
        .unwrap();
        let expected = to_hex(&hmac_bytes("sha256", b"Jefe", b"payload").unwrap());
        assert_eq!(msg.header("x-sig"), Some(expected.as_str()));
    }

    #[test]
    fn no_auth_key_yields_none() {
        assert!(CredentialInjector::from_config(&json!({ "adapter": "x" }))
            .unwrap()
            .is_none());
    }

    #[test]
    fn canonical_query_recanonicalizes_without_double_encoding() {
        // Wire query is already percent-encoded (`a%20b`); the canonical form
        // must decode-then-encode to `a%20b`, not double-encode to `a%2520b`,
        // and sort params by key.
        assert_eq!(canonical_query_string("v=a%20b&k=1"), "k=1&v=a%20b");
        // A value with a literal reserved char decodes and re-encodes stably.
        assert_eq!(canonical_query_string("q=a%2Fb"), "q=a%2Fb");
        assert_eq!(canonical_query_string(""), "");
    }

    #[test]
    fn canonical_path_double_encodes_for_non_s3_only() {
        // Wire path is single-encoded (`%20`); the canonical URI for a non-S3
        // service double-encodes it (`%2520`, matching AWS's `get-space`
        // vector), while S3 signs the single-encoded path unchanged.
        assert_eq!(
            canonical_path("/example%20space/", "execute-api"),
            "/example%2520space/"
        );
        assert_eq!(
            canonical_path("/example%20space/", "s3"),
            "/example%20space/"
        );
        // A literal space in the decoded path canonicalizes identically.
        assert_eq!(canonical_path("/a b", "execute-api"), "/a%2520b");
        assert_eq!(canonical_path("/a b", "s3"), "/a%20b");
        // `/` between segments is always preserved (never encoded).
        assert_eq!(canonical_path("/v1/send", "execute-api"), "/v1/send");
    }

    // AWS SigV4 `get-vanilla` golden vector from the published test suite.
    #[test]
    fn sigv4_matches_get_vanilla_vector() {
        let (authorization, amz_date) = sign_aws_sigv4(
            "GET",
            "example.amazonaws.com",
            "/",
            "",
            b"",
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "service",
            OffsetDateTime::new_utc(
                Date::from_calendar_date(2015, Month::August, 30).unwrap(),
                Time::from_hms(12, 36, 0).unwrap(),
            ),
        );
        assert_eq!(amz_date, "20150830T123600Z");
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }
}
