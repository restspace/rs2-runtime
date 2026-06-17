//! Message (PRD §6.1): method, URL, headers, status, Body, plus runtime
//! context that is never serialized to the wire.

use http::{HeaderMap, HeaderValue, Method, StatusCode};
use uuid::Uuid;

use super::body::Body;
use super::media_type::PROBLEM_JSON;
use crate::error::RsError;

/// Where a message entered the runtime. Non-external calls skip external-only
/// concerns (CORS) but not idempotency or limits (PRD §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Inbound from the wire — the only source that can carry forged headers,
    /// so external-only guards (CORS) apply.
    External,
    /// Internal **composition** — a pipeline step, a granted capability call, a
    /// guest `fetch`. Authorized by the principal it carries (which propagates
    /// from the original caller); it is **not** inherently trusted.
    Internal,
    /// Runtime-originated **system** call (e.g. a scheduler tick fired per the
    /// mount's operator-configured `schedule`). Trusted: it cannot be forged
    /// from the wire and exists only because an operator configured it.
    System,
}

/// W3C-style trace context threaded through every internal/external call.
#[derive(Debug, Clone)]
pub struct TraceContext {
    pub trace_id: String,
    pub span_id: String,
}

impl TraceContext {
    pub fn new() -> Self {
        TraceContext {
            trace_id: Uuid::new_v4().simple().to_string(),
            span_id: Uuid::new_v4().simple().to_string()[..16].to_string(),
        }
    }

    pub fn child(&self) -> Self {
        TraceContext {
            trace_id: self.trace_id.clone(),
            span_id: Uuid::new_v4().simple().to_string()[..16].to_string(),
        }
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Authenticated principal. M1 carries this through the wrapper as a stub;
/// the `auth` service (M2) populates and verifies it.
#[derive(Debug, Clone)]
pub struct Principal {
    pub id: String,
    pub roles: Vec<String>,
    /// `user` or `agent` (PRD §10.5: agent principals are first-class).
    pub kind: String,
}

/// Path + query of a message URL, with the mount split applied by the router:
/// `base_path` is the matched mount prefix, `service_path` the remainder.
#[derive(Debug, Clone, Default)]
pub struct MsgUrl {
    pub path: String,
    pub query: String,
    pub base_path: String,
    pub service_path: String,
}

impl MsgUrl {
    /// Parse a path-and-query string like `/data/orders/1?x=1`.
    pub fn parse(path_and_query: &str) -> Self {
        let (path, query) = match path_and_query.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path_and_query.to_string(), String::new()),
        };
        MsgUrl { service_path: path.clone(), path, query, base_path: String::new() }
    }

    /// First value of a query parameter, percent-decoded.
    pub fn query_param(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            if k == name {
                Some(
                    percent_encoding::percent_decode_str(v)
                        .decode_utf8_lossy()
                        .replace('+', " "),
                )
            } else {
                None
            }
        })
    }

    /// Split the path at a matched mount prefix.
    pub fn apply_mount(&mut self, base_path: &str) {
        self.base_path = base_path.trim_end_matches('/').to_string();
        self.service_path = self.path[self.base_path.len()..].to_string();
        if self.service_path.is_empty() {
            self.service_path = "/".to_string();
        }
    }

    /// Non-empty segments of the service path.
    pub fn service_segments(&self) -> Vec<&str> {
        self.service_path.split('/').filter(|s| !s.is_empty()).collect()
    }

    /// Non-empty segments of the mount prefix (`base_path`).
    pub fn base_segments(&self) -> Vec<&str> {
        self.base_path.split('/').filter(|s| !s.is_empty()).collect()
    }

    /// The resource name: the last service-path segment, if any.
    pub fn name(&self) -> Option<&str> {
        self.service_segments().last().copied()
    }

    /// Whether the request addresses a directory (trailing slash or mount root).
    pub fn is_directory(&self) -> bool {
        self.service_path.ends_with('/')
    }
}

#[derive(Debug)]
pub struct Message {
    pub method: Method,
    pub url: MsgUrl,
    pub headers: HeaderMap,
    pub status: Option<StatusCode>,
    pub body: Option<Body>,
    // Runtime context — not serialized to the wire.
    pub tenant: String,
    pub principal: Option<Principal>,
    pub trace: TraceContext,
    pub source: Source,
    /// Pipeline message naming (`as: "$name"`).
    pub name: Option<String>,
    /// Internal-call depth, capped by limits to bound recursion.
    pub depth: u16,
}

impl Message {
    pub fn request(method: Method, path_and_query: &str, tenant: &str) -> Self {
        Message {
            method,
            url: MsgUrl::parse(path_and_query),
            headers: HeaderMap::new(),
            status: None,
            body: None,
            tenant: tenant.to_string(),
            principal: None,
            trace: TraceContext::new(),
            source: Source::External,
            name: None,
            depth: 0,
        }
    }

    pub fn with_body(mut self, body: Body) -> Self {
        self.body = Some(body);
        self
    }

    pub fn with_json(self, value: &serde_json::Value) -> Self {
        self.with_body(Body::from_json(value))
    }

    /// A response derived from this message's runtime context.
    pub fn response(&self, status: StatusCode, body: Option<Body>) -> Message {
        Message {
            method: self.method.clone(),
            url: self.url.clone(),
            headers: HeaderMap::new(),
            status: Some(status),
            body,
            tenant: self.tenant.clone(),
            principal: self.principal.clone(),
            trace: self.trace.clone(),
            source: self.source,
            name: self.name.clone(),
            depth: self.depth,
        }
    }

    pub fn ok(&self, body: Option<Body>) -> Message {
        self.response(StatusCode::OK, body)
    }

    pub fn ok_json(&self, value: &serde_json::Value) -> Message {
        self.ok(Some(Body::from_json(value)))
    }

    pub fn no_content(&self) -> Message {
        self.response(StatusCode::NO_CONTENT, None)
    }

    /// Map an [`RsError`] to a problem+json response (PRD §12).
    pub fn error_response(&self, err: &RsError) -> Message {
        let problem = err.to_problem_json(&self.tenant, &self.trace.trace_id);
        let mut resp = self.response(
            StatusCode::from_u16(err.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Some(Body::from_string(problem.to_string(), super::MediaType::new(PROBLEM_JSON))),
        );
        if let Some(ms) = err.retry_after_ms {
            let secs = ms.div_ceil(1000).to_string();
            if let Ok(v) = HeaderValue::from_str(&secs) {
                resp.headers.insert(http::header::RETRY_AFTER, v);
            }
        }
        resp
    }

    pub fn is_ok(&self) -> bool {
        match self.status {
            None => true,
            Some(s) => s.is_success(),
        }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    pub fn set_header(&mut self, name: &'static str, value: &str) {
        if let Ok(v) = HeaderValue::from_str(value) {
            self.headers.insert(name, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_url_and_query() {
        let url = MsgUrl::parse("/data/orders/1?x=1&$take=5&q=a+b%21");
        assert_eq!(url.path, "/data/orders/1");
        assert_eq!(url.query_param("$take").as_deref(), Some("5"));
        assert_eq!(url.query_param("q").as_deref(), Some("a b!"));
        assert_eq!(url.query_param("missing"), None);
    }

    #[test]
    fn applies_mount_split() {
        let mut url = MsgUrl::parse("/files/docs/readme.md");
        url.apply_mount("/files");
        assert_eq!(url.base_path, "/files");
        assert_eq!(url.service_path, "/docs/readme.md");
        assert_eq!(url.service_segments(), vec!["docs", "readme.md"]);
    }

    #[test]
    fn mount_root_becomes_directory_path() {
        let mut url = MsgUrl::parse("/files");
        url.apply_mount("/files");
        assert_eq!(url.service_path, "/");
        assert!(url.is_directory());
    }
}
