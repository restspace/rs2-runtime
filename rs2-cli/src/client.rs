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
    agent: ureq::Agent,
}

impl Client {
    /// `host` is the base URL (no trailing slash); `token`, when present, is
    /// sent as `Authorization: Bearer`.
    pub fn new(host: impl Into<String>, token: Option<String>) -> Self {
        let host = host.into();
        let agent = build_agent(&host);
        Self { host, token, agent }
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
        let req = self.auth(self.agent.get(&self.url(path)));
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
        let mut req = self
            .auth(self.agent.put(&self.url(path)))
            .set("content-type", content_type);
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
            .auth(self.agent.put(&self.url(path)))
            .set("content-type", content_type)
            .set("if-none-match", "*");
        finish(req.send_bytes(bytes))
    }

    /// POST raw bytes with a content type — the code store's keyless deploy,
    /// where the server derives the (content-addressed) version name.
    ///
    /// Uploads are big enough (a Wasm component is ~1 MB) that a server which
    /// rejects the request answers and closes while the body is still going
    /// out; the client then sees a connection reset rather than the status. So
    /// a mid-transfer I/O failure carries the auth hint the lost status would
    /// have given. A failure to connect at all is reported as-is.
    pub fn post_bytes(
        &self,
        path: &str,
        content_type: &str,
        bytes: &[u8],
    ) -> Result<Response, String> {
        let req = self
            .auth(self.agent.post(&self.url(path)))
            .set("content-type", content_type);
        match req.send_bytes(bytes) {
            Err(ureq::Error::Transport(t)) if t.kind() == ureq::ErrorKind::Io => Err(format!(
                "request failed: {t} — the server may have closed the upload before it finished, \
                 which is how a rejected (e.g. unauthenticated) deploy usually surfaces; \
                 try `rs2 login`"
            )),
            other => finish(other),
        }
    }

    pub fn post_json(&self, path: &str, value: &serde_json::Value) -> Result<Response, String> {
        let body = serde_json::to_vec(value).map_err(|e| format!("cannot serialize body: {e}"))?;
        let req = self
            .auth(self.agent.post(&self.url(path)))
            .set("content-type", "application/json");
        finish(req.send_bytes(&body))
    }

    pub fn delete(&self, path: &str) -> Result<Response, String> {
        let req = self.auth(self.agent.delete(&self.url(path)));
        finish(req.call())
    }
}

/// One agent per client, so the connection (and its TLS handshake) is reused
/// across the many requests `rs2 send --dir` and `rs2 run` issue — the bare
/// `ureq::get`-style helpers build a fresh agent, and so a fresh connection,
/// every call.
///
/// It carries the workspace trust roots from [`rs2_core::tls`] instead of
/// `ureq`'s compiled-in Mozilla bundle, which is what lets the CLI reach a
/// server whose TLS is terminated by a corporate proxy, an inspection
/// appliance, or any CA the host trusts.
fn build_agent(host: &str) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .tls_config(rs2_core::tls::client_config())
        .try_proxy_from_env(use_env_proxy(host))
        .build()
}

/// Whether requests to `host` should go through the proxy named in the
/// environment (`ALL_PROXY`, `HTTPS_PROXY`, `HTTP_PROXY`, either case).
///
/// `ureq` reads those variables but knows nothing about `NO_PROXY`, and its
/// proxy is fixed per agent rather than chosen per request — so the bypass has
/// to be decided here, where the one host this client talks to is known.
/// Without it, an operator with a proxy exported globally could no longer
/// reach `rs2 dev` on localhost.
fn use_env_proxy(host: &str) -> bool {
    let Some(name) = host_name(host) else {
        return false;
    };
    // Loopback is never proxied, matching Go's `httpproxy` (and so the wider
    // ecosystem): the local development server is the common case, and no one
    // lists it in `NO_PROXY`.
    if name == "localhost"
        || name == "::1"
        || name.starts_with("127.")
        || name.ends_with(".localhost")
    {
        return false;
    }
    let no_proxy = ["NO_PROXY", "no_proxy"]
        .iter()
        .find_map(|v| std::env::var(v).ok())
        .unwrap_or_default();
    for entry in no_proxy.split(',') {
        let entry = entry.trim().trim_start_matches('.').to_ascii_lowercase();
        if entry.is_empty() {
            continue;
        }
        if entry == "*" || name == entry || name.ends_with(&format!(".{entry}")) {
            return false;
        }
    }
    true
}

/// The bare host name from a base URL — no scheme, userinfo, port, or path.
fn host_name(url: &str) -> Option<String> {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    // An IPv6 literal keeps its brackets around the colons.
    let name = match authority.strip_prefix('[') {
        Some(v6) => v6.split_once(']').map_or(v6, |(h, _)| h),
        None => authority.split_once(':').map_or(authority, |(h, _)| h),
    };
    (!name.is_empty()).then(|| name.to_ascii_lowercase())
}

/// Convert a `ureq` result into a [`Response`]. A non-2xx status is **not** an
/// error here — callers decide what is expected (e.g. `409` for `service add`);
/// only transport failures error. `ureq` reports 4xx/5xx as `Error::Status`,
/// so unwrap that back into a `Response`.
fn finish(result: Result<ureq::Response, ureq::Error>) -> Result<Response, String> {
    match result {
        Ok(resp) => Ok(to_response(resp)),
        Err(ureq::Error::Status(_, resp)) => Ok(to_response(resp)),
        Err(e) => Err(transport_error(&e.to_string())),
    }
}

/// Shape a transport failure into something actionable. A rejected server
/// certificate arrives as a bare `invalid peer certificate: UnknownIssuer`,
/// which names neither the roots that did the rejecting nor any way to add
/// one — the single most confusing failure this CLI can produce, since no
/// environment variable used to be able to fix it.
fn transport_error(message: &str) -> String {
    if rs2_core::tls::is_certificate_error(message) {
        format!(
            "request failed: {message}\n  {}",
            rs2_core::tls::unknown_issuer_help()
        )
    } else {
        format!("request failed: {message}")
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
