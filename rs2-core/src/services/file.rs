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
pub struct FileService {
    site: SiteOptions,
}

impl FileService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_config(config: &serde_json::Value) -> Self {
        FileService { site: SiteOptions::from_config(config) }
    }

    /// Serve one stored file with Range/ETag/304 semantics. Takes owned
    /// request facts rather than `&Message` (request bodies are not Sync,
    /// so a `&Message` may not be held across awaits).
    async fn serve_file(
        &self,
        template: Message,
        range: Option<ByteRange>,
        if_none_match: Option<String>,
        files: &crate::capabilities::ScopedFileStore,
        path: &str,
    ) -> Result<Message, RsError> {
        let body = files.read(path, range).await?;
        let mut resp = if range.is_some() {
            template.response(StatusCode::PARTIAL_CONTENT, Some(body))
        } else {
            template.ok(Some(body))
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
        if let Some(etag) = &etag {
            resp.set_header("etag", etag);
        }
        if let Some(lm) = &last_modified {
            resp.set_header("last-modified", lm);
        }
        // Conditional GET: a matching If-None-Match revalidates the
        // caller's copy without resending the body.
        if let Some(etag) = &etag {
            if super::if_none_match_hits(if_none_match.as_deref(), etag) {
                let mut not_modified = resp.response(StatusCode::NOT_MODIFIED, None);
                not_modified.set_header("etag", etag);
                if let Some(lm) = &last_modified {
                    not_modified.set_header("last-modified", lm);
                }
                not_modified.set_header("accept-ranges", "bytes");
                return Ok(not_modified);
            }
        }
        Ok(resp)
    }
}

/// Static-site options on a file mount (v1's static-site manifest variant,
/// expressed as config — the same module, the same store underneath).
#[derive(Clone)]
struct SiteOptions {
    /// Directory GETs serve this file instead of a listing.
    default_resource: Option<String>,
    /// Extension-less misses serve the mount-root default resource with
    /// 200 (client-side routing); asset misses (paths with extensions)
    /// still 404.
    spa_fallback: bool,
    /// Suppress dir+json listings (a public site shouldn't be browsable).
    listings: bool,
}

impl Default for SiteOptions {
    fn default() -> Self {
        SiteOptions { default_resource: None, spa_fallback: false, listings: true }
    }
}

impl SiteOptions {
    fn from_config(config: &serde_json::Value) -> SiteOptions {
        let cfg = serde_json::from_value::<crate::config_schema::FileConfig>(config.clone())
            .unwrap_or_default();
        let default_resource = cfg
            .default_resource
            .or_else(|| cfg.spa_fallback.then(|| "index.html".to_string()));
        SiteOptions {
            default_resource,
            spa_fallback: cfg.spa_fallback,
            listings: cfg.listings,
        }
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

        let site = &self.site;
        let range = msg.header("range").and_then(parse_range);
        let if_none_match = msg.header("if-none-match").map(String::from);

        // MOVE renames a file within this store (the `move` facet). The
        // `Destination` header is addressed like any path; map it into the
        // mount's store space (cross-mount moves aren't supported).
        if msg.method.as_str() == "MOVE" {
            if msg.url.is_directory() {
                return Err(RsError::bad_request("MOVE source must be a file, not a directory"));
            }
            let dest_raw = msg
                .header("destination")
                .ok_or_else(|| RsError::bad_request("MOVE requires a 'Destination' header"))?
                .to_string();
            let base = msg.url.base_path.as_str();
            let rel = dest_raw.strip_prefix(base).unwrap_or(&dest_raw);
            let dest = if rel.starts_with('/') { rel.to_string() } else { format!("/{rel}") };
            if dest.ends_with('/') {
                return Err(RsError::bad_request("MOVE destination must be a file path"));
            }
            let created = files.rename(&path, &dest).await?;
            let mut resp = msg.response(
                if created { StatusCode::CREATED } else { StatusCode::OK },
                None,
            );
            resp.set_header("location", &format!("{base}{dest}"));
            return Ok(resp);
        }

        match msg.method {
            Method::GET if msg.url.is_directory() => {
                // Static-site mode: directories serve the default resource.
                if let Some(default) = &site.default_resource {
                    match self
                        .serve_file(
                            msg.response(StatusCode::OK, None),
                            range,
                            if_none_match.clone(),
                            files,
                            &format!("{path}{default}"),
                        )
                        .await
                    {
                        Ok(resp) => return Ok(resp),
                        Err(e) if e.code != crate::error::codes::NOT_FOUND => return Err(e),
                        // No default doc here: SPA routes fall to the root.
                        Err(_) if site.spa_fallback && path != "/" => {
                            return self
                                .serve_file(
                                    msg.response(StatusCode::OK, None),
                                    range,
                                    if_none_match,
                                    files,
                                    &format!("/{default}"),
                                )
                                .await
                        }
                        Err(_) => {}
                    }
                }
                if !site.listings {
                    return Err(RsError::not_found(format!("'{path}' does not exist")));
                }
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
                match self
                    .serve_file(msg.response(StatusCode::OK, None), range, if_none_match.clone(), files, &path)
                    .await
                {
                    Ok(resp) => Ok(resp),
                    // SPA fallback: an extension-less miss is a client-side
                    // route — serve the root default with 200. Asset misses
                    // (paths with extensions) stay honest 404s.
                    Err(e)
                        if e.code == crate::error::codes::NOT_FOUND
                            && site.spa_fallback
                            && !path.rsplit('/').next().unwrap_or("").contains('.') =>
                    {
                        let default = site.default_resource.as_deref().unwrap_or("index.html");
                        self.serve_file(msg.response(StatusCode::OK, None), range, if_none_match, files, &format!("/{default}"))
                            .await
                    }
                    Err(e) => Err(e),
                }
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
