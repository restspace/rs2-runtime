//! `file` service (PRD §10.1): streamed file storage over a `FileStore`
//! capability. Writes stream end-to-end; directory listings paginate;
//! ETags are content-version based, enabling `Replayable` provenance.

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use super::{pagination, Service, ServiceContext};
use crate::capabilities::ByteRange;
use crate::error::RsError;
use crate::message::{Body, MediaType, Message, Provenance};

#[derive(Default)]
pub struct FileService;

impl FileService {
    pub fn new() -> Self {
        FileService
    }
}

/// Extension for a server-named file from its declared media type
/// (keyless POST to a directory). Unknown types get no extension.
fn extension_for(media_type: &MediaType) -> &'static str {
    match media_type.essence() {
        "application/json" => ".json",
        "text/plain" => ".txt",
        "text/html" => ".html",
        "text/css" => ".css",
        "text/csv" => ".csv",
        "application/javascript" | "text/javascript" => ".js",
        "application/wasm" => ".wasm",
        "image/png" => ".png",
        "image/jpeg" => ".jpg",
        "image/gif" => ".gif",
        "image/svg+xml" => ".svg",
        "application/pdf" => ".pdf",
        "application/zip" => ".zip",
        _ => "",
    }
}

fn parse_range(header: &str) -> Option<ByteRange> {
    // Single range only: `bytes=start-end` | `bytes=start-`.
    let spec = header.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None; // multi-range unsupported; serve the full resource
    }
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end: Option<u64> = if end.trim().is_empty() { None } else { end.trim().parse().ok() };
    Some(ByteRange { start, end })
}

#[async_trait]
impl Service for FileService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let files = ctx
            .files
            .as_ref()
            .ok_or_else(|| RsError::capability_denied("files"))?;
        let path = msg.url.service_path.clone();

        match msg.method {
            Method::GET if msg.url.is_directory() => {
                let (take, skip) = pagination(&msg);
                let (entries, total) = files.list(&path, take, skip).await?;
                let listing = json!({
                    "path": path,
                    "entries": entries,
                    "total": total,
                });
                let mut resp = msg.ok(Some(Body::from_bytes(listing.to_string(), MediaType::dir_json())));
                resp.set_header("x-total-count", &total.to_string());
                Ok(resp)
            }
            Method::GET => {
                let range = msg.header("range").and_then(parse_range);
                let body = files.read(&path, range).await?;
                let mut resp = if range.is_some() {
                    msg.response(StatusCode::PARTIAL_CONTENT, Some(body))
                } else {
                    msg.ok(Some(body))
                };
                resp.set_header("accept-ranges", "bytes");
                let (etag, last_modified) = match &resp.body {
                    Some(b) => (
                        match &b.provenance {
                            Provenance::Replayable { version, .. } => Some(format!("\"{version}\"")),
                            _ => None,
                        },
                        b.last_modified
                            .and_then(|lm| lm.format(&time::format_description::well_known::Rfc2822).ok()),
                    ),
                    None => (None, None),
                };
                if let Some(etag) = etag {
                    resp.set_header("etag", &etag);
                }
                if let Some(lm) = last_modified {
                    resp.set_header("last-modified", &lm);
                }
                Ok(resp)
            }
            Method::HEAD => {
                let meta = files.head(&path).await?;
                let mut resp = msg.response(StatusCode::OK, None);
                resp.set_header("content-length", &meta.size.to_string());
                resp.set_header("accept-ranges", "bytes");
                resp.set_header("content-type", &MediaType::for_path(&path).to_string());
                Ok(resp)
            }
            // Store contract: keyless POST to a container creates a
            // server-named child and returns its Location.
            Method::POST if msg.url.is_directory() => {
                let body = match msg.body {
                    Some(b) => b,
                    None => return Err(RsError::bad_request("write requires a body")),
                };
                let name = format!(
                    "{}{}",
                    uuid::Uuid::new_v4().simple(),
                    extension_for(&body.media_type)
                );
                let child_path = format!("{path}{name}");
                files.write(&child_path, body).await?;
                let location = format!("{}{}", msg.url.base_path, child_path);
                let template = Message::request(msg.method.clone(), &msg.url.path, &msg.tenant);
                let mut resp = template.response(StatusCode::CREATED, None);
                resp.trace = msg.trace.clone();
                resp.set_header("location", &location);
                Ok(resp)
            }
            Method::PUT | Method::POST => {
                if msg.url.is_directory() {
                    return Err(RsError::bad_request("cannot PUT to a directory path"));
                }
                let body = match msg.body {
                    Some(b) => b,
                    None => return Err(RsError::bad_request("write requires a body")),
                };
                // Fully streamed: the body flows to the store without
                // materializing (G7 holds for the single-step case).
                let created = files.write(&path, body).await?;
                let template = Message::request(msg.method.clone(), &msg.url.path, &msg.tenant);
                let mut resp = template.response(
                    if created { StatusCode::CREATED } else { StatusCode::OK },
                    None,
                );
                resp.trace = msg.trace.clone();
                Ok(resp)
            }
            Method::DELETE => {
                if msg.url.is_directory() {
                    // Store contract guard: non-empty containers delete only
                    // with `?confirm=<container name>` (matching `data`).
                    let dir_name = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
                    if msg.url.query_param("confirm").as_deref() == Some(dir_name) && !dir_name.is_empty()
                    {
                        files.delete_dir_all(&path).await?;
                    } else {
                        files.delete_dir(&path).await?;
                    }
                } else {
                    files.delete(&path).await?;
                }
                Ok(msg.no_content())
            }
            _ => Err(RsError::new(
                405,
                crate::error::codes::BAD_REQUEST,
                "Method Not Allowed",
                format!("file service does not support {}", msg.method),
            )),
        }
    }
}
