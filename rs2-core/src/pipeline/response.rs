//! `$response` — success-path response shaping from a transform (PRD §8.2
//! adjacent; added for the v1 migration to absorb v1's `set-status` /
//! `to-text` `/lib` helpers). A transform result is a response directive
//! iff it is an object whose single key is `$response`:
//!
//! ```json
//! { "$response": { "status": 201, "headers": { "Location": "/x/1" },
//!                  "mediaType": "text/plain", "body": "created" } }
//! ```
//!
//! Every field is optional. `status` replaces the transform default of 200;
//! `body` (any JSON; strings become the raw text body) replaces the message
//! body; `mediaType` sets the body's content type (default `text/plain` for
//! a string body, JSON otherwise); `headers` set response headers. Omitted
//! `body` keeps the pre-transform body. Error-path shaping stays with
//! `$error(...)` (a 400), which this deliberately does not replace.

use serde_json::{Map, Value};

use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

/// A parsed `$response` directive.
pub struct ResponseEnvelope<'a> {
    inner: &'a Map<String, Value>,
}

impl<'a> ResponseEnvelope<'a> {
    /// Match a transform output as a response directive: an object whose
    /// only key is `$response`, holding an object. Anything else — including
    /// `$response` alongside other keys — is a plain body.
    pub fn detect(out: &'a Value) -> Option<Self> {
        let obj = out.as_object()?;
        if obj.len() != 1 {
            return None;
        }
        obj.get("$response")?
            .as_object()
            .map(|inner| ResponseEnvelope { inner })
    }

    /// Apply the directive to the response message. Invalid directives are
    /// structured 400s: the spec author asked for shaping and got it wrong,
    /// which must surface, not silently degrade.
    pub fn apply(&self, msg: &mut Message) -> Result<(), RsError> {
        for key in self.inner.keys() {
            if !matches!(key.as_str(), "status" | "headers" | "mediaType" | "body") {
                return Err(RsError::bad_request(format!(
                    "$response: unknown field '{key}' (have: status, headers, mediaType, body)"
                )));
            }
        }

        let status = match self.inner.get("status") {
            None => http::StatusCode::OK,
            Some(v) => {
                let code = v
                    .as_u64()
                    .and_then(|n| u16::try_from(n).ok())
                    .ok_or_else(|| {
                        RsError::bad_request(format!(
                            "$response.status must be an integer, got {v}"
                        ))
                    })?;
                http::StatusCode::from_u16(code).map_err(|_| {
                    RsError::bad_request(format!(
                        "$response.status {code} is not a valid HTTP status"
                    ))
                })?
            }
        };

        let media_type = match self.inner.get("mediaType") {
            None => None,
            Some(Value::String(s)) => Some(MediaType::new(s)),
            Some(v) => {
                return Err(RsError::bad_request(format!(
                    "$response.mediaType must be a string, got {v}"
                )))
            }
        };

        match self.inner.get("body") {
            // A string body is raw text (v1 `to-text`), not a JSON-encoded
            // string; other JSON stays JSON.
            Some(Value::String(s)) => {
                let mt = media_type
                    .clone()
                    .unwrap_or_else(|| MediaType::new("text/plain"));
                msg.body = Some(Body::from_string(s.clone(), mt));
            }
            Some(v) => {
                let mut body = Body::from_json(v);
                if let Some(mt) = media_type.clone() {
                    body.media_type = mt;
                }
                msg.body = Some(body);
            }
            // No body: shape in place (v1 `set-status`), retyping if asked.
            None => {
                if let (Some(mt), Some(body)) = (media_type, msg.body.as_mut()) {
                    body.media_type = mt;
                }
            }
        }

        if let Some(headers) = self.inner.get("headers") {
            let headers = headers.as_object().ok_or_else(|| {
                RsError::bad_request(format!(
                    "$response.headers must be an object, got {headers}"
                ))
            })?;
            for (name, value) in headers {
                let value = match value {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    other => {
                        return Err(RsError::bad_request(format!(
                            "$response.headers.{name} must be a scalar, got {other}"
                        )))
                    }
                };
                let name = http::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                    RsError::bad_request(format!("$response: invalid header name '{name}'"))
                })?;
                let value = http::HeaderValue::from_str(&value).map_err(|_| {
                    RsError::bad_request(format!("$response: invalid value for header '{name}'"))
                })?;
                msg.headers.insert(name, value);
            }
        }

        msg.status = Some(status);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg() -> Message {
        Message::request(http::Method::POST, "/x", "t")
    }

    #[test]
    fn detects_only_the_exact_envelope_shape() {
        assert!(ResponseEnvelope::detect(&json!({ "$response": {} })).is_some());
        assert!(ResponseEnvelope::detect(&json!({ "$response": { "status": 201 } })).is_some());
        // Not directives: extra keys, non-object payloads, plain bodies.
        assert!(ResponseEnvelope::detect(&json!({ "$response": {}, "x": 1 })).is_none());
        assert!(ResponseEnvelope::detect(&json!({ "$response": 201 })).is_none());
        assert!(ResponseEnvelope::detect(&json!({ "status": 201 })).is_none());
        assert!(ResponseEnvelope::detect(&json!([1, 2])).is_none());
        assert!(ResponseEnvelope::detect(&json!("text")).is_none());
    }

    #[test]
    fn applies_status_headers_media_type_and_body() {
        let out = json!({ "$response": {
            "status": 201,
            "headers": { "Location": "/things/1", "X-Count": 3 },
            "body": { "ok": true }
        }});
        let mut m = msg();
        ResponseEnvelope::detect(&out)
            .unwrap()
            .apply(&mut m)
            .unwrap();
        assert_eq!(m.status, Some(http::StatusCode::CREATED));
        assert_eq!(m.header("location"), Some("/things/1"));
        assert_eq!(m.header("x-count"), Some("3"));
        let body = m.body.as_ref().unwrap();
        assert!(body.media_type.is_json());
    }

    #[test]
    fn string_body_becomes_raw_text() {
        let out = json!({ "$response": { "body": "plain words" } });
        let mut m = msg();
        ResponseEnvelope::detect(&out)
            .unwrap()
            .apply(&mut m)
            .unwrap();
        assert_eq!(m.status, Some(http::StatusCode::OK));
        let body = m.body.as_ref().unwrap();
        assert_eq!(body.media_type.essence(), "text/plain");
    }

    #[test]
    fn media_type_overrides_and_retypes_in_place() {
        let out = json!({ "$response": { "body": "<b>hi</b>", "mediaType": "text/html" } });
        let mut m = msg();
        ResponseEnvelope::detect(&out)
            .unwrap()
            .apply(&mut m)
            .unwrap();
        assert_eq!(m.body.as_ref().unwrap().media_type.essence(), "text/html");

        // No body: retype the existing one (set-status/to-text combo).
        let out = json!({ "$response": { "status": 202, "mediaType": "text/plain" } });
        let mut m = msg().with_json(&json!({ "kept": true }));
        ResponseEnvelope::detect(&out)
            .unwrap()
            .apply(&mut m)
            .unwrap();
        assert_eq!(m.status, Some(http::StatusCode::ACCEPTED));
        assert_eq!(m.body.as_ref().unwrap().media_type.essence(), "text/plain");
    }

    #[test]
    fn invalid_directives_are_structured_400s() {
        let cases = [
            json!({ "$response": { "status": "created" } }),
            json!({ "$response": { "status": 99 } }),
            json!({ "$response": { "status": 1000 } }),
            json!({ "$response": { "headers": ["not", "an", "object"] } }),
            json!({ "$response": { "headers": { "X-Bad": { "nested": true } } } }),
            json!({ "$response": { "mediaType": 42 } }),
            json!({ "$response": { "statuss": 200 } }),
        ];
        for out in cases {
            let err = ResponseEnvelope::detect(&out)
                .unwrap()
                .apply(&mut msg())
                .unwrap_err();
            assert_eq!(err.status, 400, "case {out}");
        }
    }
}
