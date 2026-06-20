//! A thin `ureq` wrapper that threads the server base URL and a bearer token
//! through each request, and shapes errors into readable strings (surfacing
//! RFC-9457 `detail` when the server returns problem-details JSON). Built on
//! the same blocking `ureq` client `rs2 deploy` already uses.

/// Outcome of a request: the HTTP status, an optional `ETag`, and the response
/// body as a string (empty for `204`).
pub struct Response {
    pub status: u16,
    pub etag: Option<String>,
    pub body: String,
}

pub struct Client {
    host: String,
    token: Option<String>,
}

impl Client {
    /// `host` is the base URL (no trailing slash); `token`, when present, is
    /// sent as `Authorization: Bearer`.
    pub fn new(host: impl Into<String>, token: Option<String>) -> Self {
        Self { host: host.into(), token }
    }

    fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("{}{}", self.host, path)
        } else {
            format!("{}/{}", self.host, path)
        }
    }

    fn auth(&self, req: ureq::Request) -> ureq::Request {
        match &self.token {
            Some(t) => req.set("authorization", &format!("Bearer {t}")),
            None => req,
        }
    }

    pub fn get(&self, path: &str) -> Result<Response, String> {
        let req = self.auth(ureq::get(&self.url(path)));
        finish(req.call())
    }

    /// PUT raw bytes with a content type; `if_match` sets `If-Match` for
    /// optimistic concurrency (the self-config API).
    pub fn put(
        &self,
        path: &str,
        content_type: &str,
        bytes: &[u8],
        if_match: Option<&str>,
    ) -> Result<Response, String> {
        let mut req = self.auth(ureq::put(&self.url(path))).set("content-type", content_type);
        if let Some(tag) = if_match {
            req = req.set("if-match", tag);
        }
        finish(req.send_bytes(bytes))
    }

    /// PUT raw bytes with `If-None-Match: *` — atomic create-only where the
    /// store supports it (the `conditional-write` facet); an existing resource
    /// is rejected with `412`.
    pub fn put_create(
        &self,
        path: &str,
        content_type: &str,
        bytes: &[u8],
    ) -> Result<Response, String> {
        let req = self
            .auth(ureq::put(&self.url(path)))
            .set("content-type", content_type)
            .set("if-none-match", "*");
        finish(req.send_bytes(bytes))
    }

    pub fn post_json(&self, path: &str, value: &serde_json::Value) -> Result<Response, String> {
        let body = serde_json::to_vec(value).map_err(|e| format!("cannot serialize body: {e}"))?;
        let req = self.auth(ureq::post(&self.url(path))).set("content-type", "application/json");
        finish(req.send_bytes(&body))
    }

    pub fn delete(&self, path: &str) -> Result<Response, String> {
        let req = self.auth(ureq::delete(&self.url(path)));
        finish(req.call())
    }
}

/// Convert a `ureq` result into a [`Response`]. A non-2xx status is **not** an
/// error here — callers decide what is expected (e.g. `409` for `service add`);
/// only transport failures error. `ureq` reports 4xx/5xx as `Error::Status`,
/// so unwrap that back into a `Response`.
fn finish(result: Result<ureq::Response, ureq::Error>) -> Result<Response, String> {
    match result {
        Ok(resp) => Ok(to_response(resp)),
        Err(ureq::Error::Status(_, resp)) => Ok(to_response(resp)),
        Err(e) => Err(format!("request failed: {e}")),
    }
}

fn to_response(resp: ureq::Response) -> Response {
    let status = resp.status();
    let etag = resp.header("etag").map(str::to_string);
    let body = resp.into_string().unwrap_or_default();
    Response { status, etag, body }
}

impl Response {
    /// A human-readable explanation for an unexpected status: prefers the
    /// problem-details `detail`/`title`, falls back to the raw body.
    pub fn error_detail(&self) -> String {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&self.body) {
            let detail = json.get("detail").and_then(|v| v.as_str());
            let title = json.get("title").and_then(|v| v.as_str());
            if let Some(msg) = detail.or(title) {
                let mut out = msg.to_string();
                if let Some(ms) = json.get("retryAfterMs").and_then(|v| v.as_i64()) {
                    out.push_str(&format!(" (retry after {ms} ms)"));
                }
                return out;
            }
        }
        if self.body.is_empty() {
            format!("HTTP {}", self.status)
        } else {
            self.body.clone()
        }
    }
}
