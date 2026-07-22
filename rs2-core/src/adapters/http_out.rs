//! Outbound HTTP adapter (`http` feature): a blocking ureq client run on
//! the blocking pool. Granted to services via `httpOut` capability grants
//! with allowed-host patterns — the allowlist is enforced by the grant
//! target, before this adapter ever sees a request (PRD §9.2).

use async_trait::async_trait;

use crate::capabilities::HttpOut;
use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

pub struct UreqHttpOut {
    /// One agent for the adapter's lifetime: ureq pools keep-alive
    /// connections per agent, so the top-level `ureq::request` helper (a
    /// fresh agent per call) would pay a new TCP + TLS handshake on every
    /// outbound request.
    agent: ureq::Agent,
}

impl UreqHttpOut {
    pub fn new() -> Self {
        UreqHttpOut {
            agent: ureq::AgentBuilder::new()
                .timeout(std::time::Duration::from_secs(30))
                .build(),
        }
    }
}

impl Default for UreqHttpOut {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpOut for UreqHttpOut {
    async fn request(&self, mut msg: Message) -> Result<Message, RsError> {
        let url = if msg.url.query.is_empty() {
            msg.url.path.clone()
        } else {
            format!("{}?{}", msg.url.path, msg.url.query)
        };
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(RsError::bad_request(format!(
                "not an absolute http(s) URL: '{url}'"
            )));
        }
        let method = msg.method.to_string();
        let headers: Vec<(String, String)> = msg
            .headers
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
            .collect();
        let body_bytes = match &mut msg.body {
            None => None,
            Some(b) => Some(b.materialize(100 * 1024 * 1024).await?.to_vec()),
        };
        let template = msg.response(http::StatusCode::OK, None);
        let agent = self.agent.clone();

        let outcome = tokio::task::spawn_blocking(move || {
            let mut req = agent.request(&method, &url);
            for (k, v) in &headers {
                req = req.set(k, v);
            }
            let result = match body_bytes {
                Some(bytes) => req.send_bytes(&bytes),
                None => req.call(),
            };
            let resp = match result {
                Ok(resp) => resp,
                // Status errors still carry a response to surface.
                Err(ureq::Error::Status(_, resp)) => resp,
                Err(e) => {
                    let mut err = RsError::new(
                        502,
                        crate::error::codes::INTERNAL,
                        "Upstream Error",
                        e.to_string(),
                    );
                    err.retryable = true;
                    return Err(err);
                }
            };
            let status = resp.status();
            let header_pairs: Vec<(String, String)> = resp
                .headers_names()
                .into_iter()
                .filter_map(|name| resp.header(&name).map(|v| (name.clone(), v.to_string())))
                .collect();
            let media_type = resp.content_type().to_string();
            let mut bytes = Vec::new();
            use std::io::Read;
            resp.into_reader()
                .take(100 * 1024 * 1024)
                .read_to_end(&mut bytes)
                .map_err(|e| RsError::internal(format!("upstream body read failed: {e}")))?;
            Ok((status, header_pairs, media_type, bytes))
        })
        .await
        .map_err(|e| RsError::internal(format!("http task failed: {e}")))??;

        let (status, header_pairs, media_type, bytes) = outcome;
        let body = if bytes.is_empty() {
            None
        } else {
            Some(Body::from_bytes(bytes, MediaType::parse(&media_type)))
        };
        let mut resp = template.response(
            http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::BAD_GATEWAY),
            body,
        );
        for (k, v) in header_pairs {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::try_from(k.as_str()),
                http::HeaderValue::from_str(&v),
            ) {
                resp.headers.insert(name, value);
            }
        }
        Ok(resp)
    }
}
