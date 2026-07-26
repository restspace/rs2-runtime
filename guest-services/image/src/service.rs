//! Sandbox-facing glue: the RS2 service world implementation. The flow is
//! built to make the steady state free of pixel work:
//!
//! 1. parse + canonicalize (reject bad input before any I/O)
//! 2. `HEAD` the source (authz-preserving; its ETag versions the cache key)
//! 3. derived ETag = hash(source path, source ETag, canonical params) —
//!    an `If-None-Match` match is a 304 with no further work
//! 4. cache `HEAD` — a hit answers with `x-rs2-body-ref`, so the host
//!    streams the derivative without the bytes entering the sandbox
//! 5. only a miss decodes: transform, best-effort cache `PUT`, respond

use sha2::{Digest, Sha256};

use crate::params::{self, Config};
use crate::transform::{self, TransformError};

wit_bindgen::generate!({
    path: "../../rs2-core/wit",
    world: "service",
});

use rs2::service::host;
use rs2::service::types::{BodyData, Header};

type WitMessage = Message;

fn hdr(name: &str, value: &str) -> Header {
    Header {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn header<'a>(headers: &'a [Header], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case(name))
        .map(|h| h.value.as_str())
}

fn resp(status: u16, headers: Vec<Header>, body: Option<BodyData>) -> WitMessage {
    WitMessage {
        method: "GET".to_string(),
        url: String::new(),
        headers,
        status,
        body,
    }
}

fn json_error(status: u16, code: &str, detail: &str) -> WitMessage {
    let body = serde_json::json!({ "code": code, "detail": detail }).to_string();
    resp(
        status,
        vec![],
        Some(BodyData {
            media_type: "application/json".to_string(),
            schema_ref: None,
            bytes: body.into_bytes(),
        }),
    )
}

fn req(method: &str, url: &str) -> WitMessage {
    WitMessage {
        method: method.to_string(),
        url: url.to_string(),
        headers: vec![],
        status: 0,
        body: None,
    }
}

/// Capability call; host errors become structured responses.
fn call(capability: &str, msg: &WitMessage) -> Result<WitMessage, WitMessage> {
    host::request(capability, msg).map_err(|e| match e {
        host::HostError::CapabilityDenied(d) => json_error(403, "capability_denied", &d),
        host::HostError::LimitExceeded(d) => json_error(503, "limit_exceeded", &d),
        host::HostError::Failed(d) => json_error(502, "internal", &d),
    })
}

fn is_2xx(m: &WitMessage) -> bool {
    (200..300).contains(&m.status)
}

/// Whether an `If-None-Match` header value hits `etag` (comma list, `*`,
/// weak-prefix tolerant).
fn if_none_match_hits(inm: &str, etag: &str) -> bool {
    inm.split(',')
        .map(str::trim)
        .any(|c| c == "*" || c.trim_start_matches("W/") == etag)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

struct ImageService;

impl Guest for ImageService {
    fn init(_config: String) -> Result<(), String> {
        Ok(())
    }

    fn handle(msg: WitMessage, config: String) -> Result<WitMessage, String> {
        let cfg_json: serde_json::Value =
            serde_json::from_str(&config).unwrap_or(serde_json::Value::Null);
        let cfg = match Config::from_json(&cfg_json) {
            Ok(c) => c,
            Err(e) => return Ok(json_error(500, "internal", &e)),
        };

        let (path, query) = msg.url.split_once('?').unwrap_or((msg.url.as_str(), ""));
        let base = header(&msg.headers, "x-rs2-base-path").unwrap_or("/");
        let sub = if base == "/" {
            path
        } else {
            path.strip_prefix(base).unwrap_or(path)
        };
        let sub = if sub.is_empty() { "/" } else { sub };

        // Operator purge of the derivative cache: the mount's `delete`
        // access role has already been enforced by dispatch.
        if msg.method == "DELETE" {
            if sub != "/.cache" {
                return Ok(json_error(405, "bad_request", "only DELETE /.cache?confirm="));
            }
            if !query.starts_with("confirm=") {
                return Ok(json_error(
                    409,
                    "conflict",
                    "purging the derivative cache requires ?confirm=",
                ));
            }
            return Ok(match call("cache", &req("DELETE", "/d/?confirm=d")) {
                Ok(d) if is_2xx(&d) || d.status == 404 => resp(204, vec![], None),
                Ok(d) => d,
                Err(e) => e,
            });
        }
        if msg.method != "GET" && msg.method != "HEAD" {
            return Ok(json_error(405, "bad_request", "image mounts serve GET/HEAD"));
        }
        let is_head = msg.method == "HEAD";

        // `?$info`: source metadata for pickers/asset libraries.
        if query == "$info" {
            let got = match call("source", &req("GET", sub)) {
                Ok(g) => g,
                Err(e) => return Ok(e),
            };
            if !is_2xx(&got) || got.body.is_none() {
                return Ok(json_error(404, "not_found", &format!("no image at '{sub}'")));
            }
            let body = got.body.unwrap();
            let (w, h) = match transform::probe(&body.bytes) {
                Ok(d) => d,
                Err(_) => {
                    return Ok(json_error(415, "bad_request", "not a decodable image"))
                }
            };
            let info = serde_json::json!({
                "width": w, "height": h,
                "mediaType": body.media_type, "bytes": body.bytes.len(),
            });
            return Ok(resp(
                200,
                vec![],
                Some(BodyData {
                    media_type: "application/json".to_string(),
                    schema_ref: None,
                    bytes: info.to_string().into_bytes(),
                }),
            ));
        }

        let p = match params::parse(query, &cfg) {
            Ok(p) => p,
            Err(e) => return Ok(json_error(400, "bad_request", &e)),
        };

        // Either path starts by HEADing the source: existence, ETag, type.
        let head = match call("source", &req("HEAD", sub)) {
            Ok(h) => h,
            Err(e) => return Ok(e),
        };
        if !is_2xx(&head) {
            return Ok(json_error(404, "not_found", &format!("no image at '{sub}'")));
        }
        let source_etag = header(&head.headers, "etag").unwrap_or("").to_string();
        let source_type = header(&head.headers, "content-type")
            .unwrap_or("application/octet-stream")
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();

        let Some(p) = p else {
            // Passthrough: the original, streamed host-side.
            let mut headers = Vec::new();
            if !source_etag.is_empty() {
                if let Some(inm) = header(&msg.headers, "if-none-match") {
                    if if_none_match_hits(inm, &source_etag) {
                        return Ok(resp(304, vec![hdr("etag", &source_etag)], None));
                    }
                }
                headers.push(hdr("etag", &source_etag));
            }
            if is_head {
                headers.push(hdr("content-type", &source_type));
                return Ok(resp(200, headers, None));
            }
            headers.push(hdr("x-rs2-body-ref", &format!("source:{sub}")));
            return Ok(resp(200, headers, None));
        };

        // Derived identity: the cache key (and strong ETag) covers the
        // source path + version and the canonical parameters, so a changed
        // source implicitly invalidates every derivative.
        let (resolved, media_type, ext) = params::resolve_format(p.format, &source_type);
        let canon = params::canonical(&p, resolved);
        let mut hasher = Sha256::new();
        hasher.update(sub.as_bytes());
        hasher.update(b"\n");
        hasher.update(source_etag.as_bytes());
        hasher.update(b"\n");
        hasher.update(canon.as_bytes());
        let key = hex(&hasher.finalize());
        let derived_etag = format!("\"{}\"", &key[..32]);

        if let Some(inm) = header(&msg.headers, "if-none-match") {
            if if_none_match_hits(inm, &derived_etag) {
                return Ok(resp(304, vec![hdr("etag", &derived_etag)], None));
            }
        }

        // Derivatives live under one `/d/` container (sharded by key
        // prefix) so a purge is a single confirm-delete of `/d/`.
        let cache_rel = format!("/d/{}/{}.{}", &key[..2], key, ext);
        let mut headers = vec![hdr("etag", &derived_etag)];

        let cache_hit = matches!(call("cache", &req("HEAD", &cache_rel)), Ok(h) if is_2xx(&h));
        if cache_hit {
            headers.push(hdr("x-img-cache", "hit"));
            if is_head {
                headers.push(hdr("content-type", media_type));
                return Ok(resp(200, headers, None));
            }
            headers.push(hdr("x-rs2-body-ref", &format!("cache:{cache_rel}")));
            return Ok(resp(200, headers, None));
        }

        // Miss: the one path that decodes pixels.
        let got = match call("source", &req("GET", sub)) {
            Ok(g) => g,
            Err(e) => return Ok(e),
        };
        if !is_2xx(&got) || got.body.is_none() {
            return Ok(json_error(404, "not_found", &format!("no image at '{sub}'")));
        }
        let source_bytes = got.body.unwrap().bytes;
        let out = match transform::transform(&source_bytes, &p, resolved, cfg.max_source_pixels) {
            Ok(o) => o,
            Err(TransformError::TooLarge { pixels, cap }) => {
                return Ok(json_error(
                    413,
                    "payload_too_large",
                    &format!("source is {pixels}px, the mount allows {cap}px"),
                ))
            }
            Err(TransformError::Unsupported(d)) => {
                return Ok(json_error(415, "bad_request", &format!("not a decodable image: {d}")))
            }
            Err(TransformError::BadRequest(d)) => return Ok(json_error(400, "bad_request", &d)),
        };

        // Best-effort store; a failed write serves the bytes inline rather
        // than failing the request.
        let mut put = req("PUT", &cache_rel);
        put.body = Some(BodyData {
            media_type: out.media_type.to_string(),
            schema_ref: None,
            bytes: out.bytes.clone(),
        });
        let stored = matches!(call("cache", &put), Ok(r) if is_2xx(&r));

        headers.push(hdr("x-img-cache", if stored { "miss" } else { "miss,nostore" }));
        if is_head {
            headers.push(hdr("content-type", out.media_type));
            return Ok(resp(200, headers, None));
        }
        if stored {
            headers.push(hdr("x-rs2-body-ref", &format!("cache:{cache_rel}")));
            return Ok(resp(200, headers, None));
        }
        Ok(resp(
            200,
            headers,
            Some(BodyData {
                media_type: out.media_type.to_string(),
                schema_ref: None,
                bytes: out.bytes,
            }),
        ))
    }
}

export!(ImageService);
