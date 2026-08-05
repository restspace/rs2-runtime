//! `file` service (PRD §10.1): streamed file storage over a `FileStore`
//! capability. Writes stream end-to-end; directory listings paginate;
//! ETags are content-version based, enabling `Replayable` provenance.

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use super::{pagination, Service, ServiceContext};
use crate::capabilities::{ByteRange, WritePrecondition};
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
        FileService {
            site: SiteOptions::from_config(config),
        }
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
                b.last_modified.and_then(|lm| {
                    lm.format(&time::format_description::well_known::Rfc2822)
                        .ok()
                }),
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

    /// Resolve an extension-less request to a stored file by probing the known
    /// servable extensions in `Accept`-negotiated order (falling back to the
    /// fixed preference order). Returns the first real path that exists, or
    /// `None` if no candidate exists. Only call for extension-less paths.
    async fn resolve_friendly(
        &self,
        path: &str,
        accept: Option<&str>,
        files: &crate::capabilities::ScopedFileStore,
        priority: &[String],
    ) -> Result<Option<String>, RsError> {
        for ext in negotiate_extensions(accept, priority) {
            let candidate = format!("{path}.{ext}");
            match files.head(&candidate).await {
                // A directory that happens to carry the extension is not a
                // servable file — keep probing.
                Ok(meta) if meta.is_dir => continue,
                Ok(_) => return Ok(Some(candidate)),
                Err(e) if e.code == crate::error::codes::NOT_FOUND => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    /// 301 to the trailing-slash form of a directory addressed without one
    /// (the DirectorySlash convention: relative URLs in the served default
    /// document only resolve correctly against the slash form). `url.path`
    /// is the full external path, so the Location works across mounts.
    fn slash_redirect(msg: &Message) -> Message {
        let mut resp = msg.response(StatusCode::MOVED_PERMANENTLY, None);
        let location = if msg.url.query.is_empty() {
            format!("{}/", msg.url.path)
        } else {
            format!("{}/?{}", msg.url.path, msg.url.query)
        };
        resp.set_header("location", &location);
        resp
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
    /// Every miss — extension or not — serves the mount-root default
    /// resource with 200. Implies `spa_fallback`.
    spa_fallback_all: bool,
    /// Suppress dir+json listings (a public site shouldn't be browsable).
    /// Concealment, not authorization: tenant operators still see the listing,
    /// since they can toggle this flag anyway.
    listings: bool,
    /// Resolve extension-less URLs to a stored file (e.g. `/docs/readme` →
    /// `docs/readme.md`), choosing among collisions by `Accept` negotiation.
    friendly_urls: bool,
    /// Extension preference (no dots) for friendly URLs. `[0]` is the canonical
    /// slot an extension-less PUT pins to; the list also fronts the GET probe
    /// order. Empty → extension-less PUTs are rejected (see the PUT arm).
    extension_priority: Vec<String>,
}

impl Default for SiteOptions {
    fn default() -> Self {
        SiteOptions {
            default_resource: None,
            spa_fallback: false,
            spa_fallback_all: false,
            listings: true,
            friendly_urls: false,
            extension_priority: Vec::new(),
        }
    }
}

impl SiteOptions {
    fn from_config(config: &serde_json::Value) -> SiteOptions {
        let cfg = serde_json::from_value::<crate::config_schema::FileConfig>(config.clone())
            .unwrap_or_default();
        let default_resource = cfg.default_resource.or_else(|| {
            (cfg.spa_fallback || cfg.spa_fallback_all).then(|| "index.html".to_string())
        });
        SiteOptions {
            default_resource,
            spa_fallback: cfg.spa_fallback,
            spa_fallback_all: cfg.spa_fallback_all,
            listings: cfg.listings,
            friendly_urls: cfg.friendly_urls,
            extension_priority: cfg.extension_priority,
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

/// Whether `path`'s final segment carries no extension — the precondition for
/// treating it as a friendly URL (`/docs/readme`, not `/docs/readme.md`).
fn is_extensionless(path: &str) -> bool {
    !path.rsplit('/').next().unwrap_or("").contains('.')
}

/// Whether `Accept` *explicitly* asks for the directory-listing type
/// (`application/vnd.rs2.dir+json`) — i.e. names it exactly with `q > 0`.
/// A wildcard (`*/*`, `application/*`) does **not** count: browsers send
/// `Accept: …,*/*`, and we must keep serving them the `default_resource`, not a
/// listing. Only a deliberate `Accept: application/vnd.rs2.dir+json` (agents,
/// the CLI, the discovery surface) forces the listing on a static-site mount.
fn wants_dir_listing(accept: Option<&str>) -> bool {
    let (dtype, dsubtype) = crate::message::media_type::DIR_JSON
        .split_once('/')
        .unwrap_or(("application", "vnd.rs2.dir+json"));
    accept
        .into_iter()
        .flat_map(|h| h.split(','))
        .filter_map(|part| {
            let mut params = part.split(';');
            let media = params.next()?.trim();
            let (t, s) = media.split_once('/')?;
            let q = params
                .find_map(|p| {
                    p.trim()
                        .strip_prefix("q=")
                        .and_then(|v| v.trim().parse::<f32>().ok())
                })
                .unwrap_or(1.0);
            Some((
                t.trim().to_ascii_lowercase(),
                s.trim().to_ascii_lowercase(),
                q,
            ))
        })
        // Exact type+subtype only — no wildcard match.
        .any(|(t, s, q)| q > 0.0 && t == dtype && s == dsubtype)
}

/// Quality of `essence` against one parsed `Accept` range `(type, subtype, q)`.
/// `*/*` and `type/*` match as wildcards; an exact essence match also scores its
/// `q`. Returns `0.0` when the range doesn't apply.
fn accept_match_q(essence: &str, range: &(String, String, f32)) -> f32 {
    let (rtype, rsubtype, q) = range;
    let (etype, esubtype) = essence.split_once('/').unwrap_or((essence, ""));
    let matches = (rtype == "*" || rtype == etype) && (rsubtype == "*" || rsubtype == esubtype);
    if matches {
        *q
    } else {
        0.0
    }
}

/// Order the servable extensions for an extension-less request by `Accept`
/// quality, with a stable preference order as the tiebreak. The base order is
/// the configured `priority` list first (deduped), then the remaining built-in
/// known extensions — so a configured list fronts the order (and `priority[0]`
/// stays first for a no-`Accept`/`*/*` request) without losing resolvability of
/// other types. With an empty `priority`, the base order is the built-in table.
fn negotiate_extensions(accept: Option<&str>, priority: &[String]) -> Vec<String> {
    // Parse `Accept` into `(type, subtype, q)` ranges; ignore malformed parts.
    let ranges: Vec<(String, String, f32)> = accept
        .into_iter()
        .flat_map(|h| h.split(','))
        .filter_map(|part| {
            let mut params = part.split(';');
            let media = params.next()?.trim();
            let (t, s) = media.split_once('/')?;
            let q = params
                .find_map(|p| {
                    p.trim()
                        .strip_prefix("q=")
                        .and_then(|v| v.trim().parse::<f32>().ok())
                })
                .unwrap_or(1.0);
            Some((
                t.trim().to_ascii_lowercase(),
                s.trim().to_ascii_lowercase(),
                q,
            ))
        })
        .collect();

    // Base candidate order: configured priority first, then the rest of the
    // built-in table not already listed.
    let mut base: Vec<String> = Vec::new();
    for ext in priority {
        let ext = ext.trim_start_matches('.').to_ascii_lowercase();
        if !ext.is_empty() && !base.contains(&ext) {
            base.push(ext);
        }
    }
    for (ext, _) in MediaType::known_extensions() {
        if !base.iter().any(|e| e == ext) {
            base.push(ext.to_string());
        }
    }

    let mut candidates: Vec<(String, f32)> = base
        .into_iter()
        .map(|ext| {
            let essence = MediaType::from_extension(&ext).unwrap_or_else(MediaType::octet_stream);
            let q = ranges
                .iter()
                .map(|r| accept_match_q(essence.essence(), r))
                .fold(0.0_f32, f32::max);
            (ext, q)
        })
        .collect();
    // Stable sort by descending quality; ties keep the base preference order.
    candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    candidates.into_iter().map(|(ext, _)| ext).collect()
}

fn parse_range(header: &str) -> Option<ByteRange> {
    // Single range only: `bytes=start-end` | `bytes=start-`.
    let spec = header.strip_prefix("bytes=")?;
    if spec.contains(',') {
        return None; // multi-range unsupported; serve the full resource
    }
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.trim().parse().ok()?;
    let end: Option<u64> = if end.trim().is_empty() {
        None
    } else {
        end.trim().parse().ok()
    };
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
        let accept = msg.header("accept").map(String::from);

        // MOVE renames a file within this store (the `move` facet). The
        // `Destination` header is addressed like any path; map it into the
        // mount's store space (cross-mount moves aren't supported).
        if msg.method.as_str() == "MOVE" {
            if msg.url.is_directory() {
                return Err(RsError::bad_request(
                    "MOVE source must be a file, not a directory",
                ));
            }
            let dest_raw = msg
                .header("destination")
                .ok_or_else(|| RsError::bad_request("MOVE requires a 'Destination' header"))?
                .to_string();
            let base = msg.url.base_path.as_str();
            let rel = dest_raw.strip_prefix(base).unwrap_or(&dest_raw);
            let dest = if rel.starts_with('/') {
                rel.to_string()
            } else {
                format!("/{rel}")
            };
            if dest.ends_with('/') {
                return Err(RsError::bad_request("MOVE destination must be a file path"));
            }
            let created = files.rename(&path, &dest).await?;
            let mut resp = msg.response(
                if created {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                None,
            );
            resp.set_header("location", &format!("{base}{dest}"));
            return Ok(resp);
        }

        match msg.method {
            Method::GET if msg.url.is_directory() => {
                // A deliberate `Accept: application/vnd.rs2.dir+json` asks for
                // the machine listing even on a static-site mount whose
                // `default_resource` would otherwise shadow it — this is how the
                // discovery surface enumerates files. Browsers (which send
                // `*/*`) and humans keep getting the default doc. The `listings`
                // gate below still applies: an explicit request can surface a
                // listing the default resource hides, but it cannot override
                // `listings: false` for anyone but an operator. The
                // representation served at a directory URL now depends on
                // `Accept`, so every branch sets `Vary`.
                let force_listing = wants_dir_listing(accept.as_deref());

                // Static-site mode: directories serve the default resource.
                if !force_listing {
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
                            Ok(mut resp) => {
                                resp.set_header("vary", "accept");
                                return Ok(resp);
                            }
                            Err(e) if e.code != crate::error::codes::NOT_FOUND => return Err(e),
                            // No default doc here: SPA routes fall to the root.
                            Err(_)
                                if (site.spa_fallback || site.spa_fallback_all) && path != "/" =>
                            {
                                let mut resp = self
                                    .serve_file(
                                        msg.response(StatusCode::OK, None),
                                        range,
                                        if_none_match,
                                        files,
                                        &format!("/{default}"),
                                    )
                                    .await?;
                                resp.set_header("vary", "accept");
                                return Ok(resp);
                            }
                            Err(_) => {}
                        }
                    }
                }
                // `listings: false` conceals the inventory from the public; it
                // is not an authorization boundary (every file stays readable
                // by name under `access.read`). An **operator** can flip the
                // flag in tenant config at will, so withholding the listing
                // from them protects nothing and costs them the CLI/agent view
                // of their own mount. The gate runs *after* the
                // `default_resource` branch, so an operator browsing the site
                // still gets the default doc — only a deliberate
                // `Accept: dir+json` surfaces the listing.
                if !site.listings
                    && !crate::wrapper::is_operator(
                        msg.principal.as_ref(),
                        ctx.operator_roles.as_deref().unwrap_or(""),
                    )
                {
                    return Err(RsError::not_found(format!("'{path}' does not exist")));
                }
                // Past the gate with `listings: false` ⇒ this listing exists
                // only because the caller is an operator.
                let operator_only = !site.listings;
                let (take, skip) = pagination(&msg);
                // Metadata sort (the `meta-sort` facet): order by what the
                // listing already knows (@name/@size/@lastModified/
                // @contentType/@dir) — no content reads. A sort needs the
                // whole directory before paging (a page of the natural order
                // can't be re-sorted), so fetch unpaged, sort, then slice;
                // the plain path stays adapter-paginated as before.
                let (entries, total) = match msg.url.query_param("$sort") {
                    Some(sort) => {
                        let sort = crate::listing::MetaSort::parse(&sort)?;
                        let (mut entries, total) = files.list(&path, usize::MAX, 0).await?;
                        sort.sort(&mut entries);
                        let entries: Vec<_> = entries.into_iter().skip(skip).take(take).collect();
                        (entries, total)
                    }
                    None => files.list(&path, take, skip).await?,
                };
                let listing = json!({
                    "path": path,
                    "entries": entries,
                    "total": total,
                });
                let mut resp = msg.ok(Some(Body::from_bytes(
                    listing.to_string(),
                    MediaType::dir_json(),
                )));
                resp.set_header("x-total-count", &total.to_string());
                resp.set_header("vary", "accept");
                if operator_only {
                    // This representation is principal-dependent: the same URL
                    // and `Accept` 404s for everyone else. A `listings: false`
                    // static site is exactly the mount most likely to run
                    // `caching: {public: true}` (it's anonymously readable), so
                    // without this a shared cache would store the operator's
                    // listing and serve the public the inventory the flag
                    // hides. Set explicitly — the host cache policy leaves a
                    // response's own `Cache-Control` alone.
                    resp.set_header("cache-control", "no-store");
                    crate::wrapper::append_header_value(&mut resp, "vary", "authorization, cookie");
                }
                Ok(resp)
            }
            Method::GET => {
                let e = match self
                    .serve_file(
                        msg.response(StatusCode::OK, None),
                        range,
                        if_none_match.clone(),
                        files,
                        &path,
                    )
                    .await
                {
                    Ok(resp) => return Ok(resp),
                    Err(e) => e,
                };
                // The path may name a directory without the trailing slash.
                // Redirect to the slash form, whose arm decides what a
                // directory URL yields (default doc, listing, or 404).
                // Checked before friendly URLs / SPA so a directory always
                // beats a same-named `.html` probe.
                if matches!(files.head(&path).await, Ok(m) if m.is_dir) {
                    return Ok(Self::slash_redirect(&msg));
                }
                if e.code == crate::error::codes::NOT_FOUND {
                    let extensionless = is_extensionless(&path);
                    // Friendly URL: resolve `/docs/readme` to a stored
                    // `docs/readme.<ext>` (Accept-negotiated). A real file
                    // beats the SPA shell, so try this first. Only meaningful
                    // for extension-less requests — an extensioned miss
                    // can't be a friendly-URL stem.
                    if extensionless && site.friendly_urls {
                        if let Some(real) = self
                            .resolve_friendly(
                                &path,
                                accept.as_deref(),
                                files,
                                &site.extension_priority,
                            )
                            .await?
                        {
                            let mut resp = self
                                .serve_file(
                                    msg.response(StatusCode::OK, None),
                                    range,
                                    if_none_match,
                                    files,
                                    &real,
                                )
                                .await?;
                            resp.set_header(
                                "content-location",
                                &format!("{}{}", msg.url.base_path, real),
                            );
                            return Ok(resp);
                        }
                    }
                    // SPA fallback: `spaFallback` rescues an extension-less
                    // miss only (a client-side route); asset misses (paths
                    // with extensions) stay honest 404s. `spaFallbackAll`
                    // rescues *every* miss, extension included — for a mount
                    // that hosts nothing but one SPA build, where the app's
                    // own router may reflect extensioned paths into the URL.
                    if (extensionless && site.spa_fallback) || site.spa_fallback_all {
                        let default = site.default_resource.as_deref().unwrap_or("index.html");
                        return self
                            .serve_file(
                                msg.response(StatusCode::OK, None),
                                range,
                                if_none_match,
                                files,
                                &format!("/{default}"),
                            )
                            .await;
                    }
                }
                Err(e)
            }
            Method::HEAD => {
                // Resolve the same way GET does: exact match, then (if the path
                // is extension-less and friendly URLs are on) a negotiated probe.
                let resolved = match files.head(&path).await {
                    // Same DirectorySlash redirect as GET (the old behavior —
                    // a 200 with a path-guessed content type — was bogus).
                    Ok(meta) if meta.is_dir && !msg.url.is_directory() => {
                        return Ok(Self::slash_redirect(&msg));
                    }
                    Ok(meta) => Some((path.clone(), meta)),
                    Err(e)
                        if e.code == crate::error::codes::NOT_FOUND
                            && site.friendly_urls
                            && is_extensionless(&path) =>
                    {
                        match self
                            .resolve_friendly(
                                &path,
                                accept.as_deref(),
                                files,
                                &site.extension_priority,
                            )
                            .await?
                        {
                            Some(real) => {
                                let meta = files.head(&real).await?;
                                Some((real, meta))
                            }
                            None => None,
                        }
                    }
                    Err(e) => return Err(e),
                };
                let (real, meta) = match resolved {
                    Some(r) => r,
                    None => return Err(RsError::not_found(format!("'{path}' does not exist"))),
                };
                let mut resp = msg.response(StatusCode::OK, None);
                resp.set_header("content-length", &meta.size.to_string());
                resp.set_header("accept-ranges", "bytes");
                resp.set_header("content-type", &MediaType::for_path(&real).to_string());
                if real != path {
                    resp.set_header(
                        "content-location",
                        &format!("{}{}", msg.url.base_path, real),
                    );
                }
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
                let etag = files.current_etag(&child_path).await?;
                let location = format!("{}{}", msg.url.base_path, child_path);
                let template = Message::request(msg.method.clone(), &msg.url.path, &msg.tenant);
                let mut resp = template.response(StatusCode::CREATED, None);
                resp.trace = msg.trace.clone();
                resp.set_header("location", &location);
                if let Some(etag) = &etag {
                    resp.set_header("etag", etag);
                }
                Ok(resp)
            }
            Method::PUT | Method::POST => {
                if msg.url.is_directory() {
                    return Err(RsError::bad_request("cannot PUT to a directory path"));
                }
                let precondition = super::write_precondition(&msg);
                let body = match msg.body {
                    Some(b) => b,
                    None => return Err(RsError::bad_request("write requires a body")),
                };
                // Friendly URLs: an extension-less write pins to the canonical
                // top-priority slot (`E1`), so a no-`Accept` GET of the slug
                // always returns this write and no later sibling can outrank it.
                // The declared Content-Type is intentionally discarded — pin to
                // `E1` regardless. Without a configured priority there's no slot
                // to pin to, so the write is a 400.
                let store_path = if site.friendly_urls && is_extensionless(&path) {
                    let e1 = site.extension_priority.first().ok_or_else(|| {
                        RsError::bad_request(
                            "extension-less write requires a configured 'extensionPriority'",
                        )
                    })?;
                    format!(
                        "{}.{}",
                        path,
                        e1.trim_start_matches('.').to_ascii_lowercase()
                    )
                } else {
                    path.clone()
                };
                // Fully streamed: the body flows to the store without
                // materializing (G7 holds for the single-step case). With a
                // conditional header the store does a best-effort (or atomic)
                // check-then-write; an empty precondition is a plain upsert.
                let outcome = files.write_cond(&store_path, body, precondition).await?;
                let template = Message::request(msg.method.clone(), &msg.url.path, &msg.tenant);
                let mut resp = template.response(
                    if outcome.created {
                        StatusCode::CREATED
                    } else {
                        StatusCode::OK
                    },
                    None,
                );
                resp.trace = msg.trace.clone();
                if let Some(etag) = &outcome.etag {
                    resp.set_header("etag", etag);
                }
                // Pinned write: advertise the request URL as canonical and the
                // real stored path so clients can discover the chosen extension.
                if store_path != path {
                    resp.set_header("location", &format!("{}{}", msg.url.base_path, path));
                    resp.set_header(
                        "content-location",
                        &format!("{}{}", msg.url.base_path, store_path),
                    );
                }
                Ok(resp)
            }
            Method::DELETE => {
                let precondition = super::write_precondition(&msg);
                if msg.url.is_directory() {
                    // Directories have no ETag, so a conditional header can't
                    // be evaluated — refuse rather than silently ignore it.
                    if !matches!(precondition, WritePrecondition::None) {
                        return Err(RsError::bad_request(
                            "conditional headers are not supported on directory deletes",
                        ));
                    }
                    // Store contract guard: non-empty containers delete only
                    // with `?confirm=<container name>` (matching `data`).
                    let dir_name = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
                    if msg.url.query_param("confirm").as_deref() == Some(dir_name)
                        && !dir_name.is_empty()
                    {
                        files.delete_dir_all(&path).await?;
                    } else {
                        files.delete_dir(&path).await?;
                    }
                } else {
                    // Mirror GET precedence: exact match first, then (friendly
                    // URLs on) the pinned `E1` a no-`Accept` GET would resolve.
                    // A missing exact path falls out of `delete_cond` as the
                    // 404 that triggers the fallback, so the precondition is
                    // checked against whichever file is actually deleted.
                    match files.delete_cond(&path, precondition.clone()).await {
                        Ok(()) => {}
                        Err(e)
                            if e.code == crate::error::codes::NOT_FOUND
                                && site.friendly_urls
                                && is_extensionless(&path) =>
                        {
                            match self
                                .resolve_friendly(&path, None, files, &site.extension_priority)
                                .await?
                            {
                                Some(real) => files.delete_cond(&real, precondition).await?,
                                None => return Err(e),
                            }
                        }
                        Err(e) => return Err(e),
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extensionless_detects_final_segment() {
        assert!(is_extensionless("/docs/readme"));
        assert!(is_extensionless("/readme"));
        assert!(!is_extensionless("/docs/readme.md"));
        // A dot earlier in the path doesn't count — only the final segment.
        assert!(is_extensionless("/v1.2/readme"));
    }

    #[test]
    fn negotiate_defaults_to_table_order() {
        // No configured priority, no Accept → built-in table order (HTML first).
        let order = negotiate_extensions(None, &[]);
        assert_eq!(order.first().map(String::as_str), Some("html"));
        let star = negotiate_extensions(Some("*/*"), &[]);
        assert_eq!(star.first().map(String::as_str), Some("html"));
    }

    #[test]
    fn negotiate_honors_accept() {
        let md = negotiate_extensions(Some("text/markdown"), &[]);
        assert_eq!(md.first().map(String::as_str), Some("md"));
        // A type wildcard floats every image type above non-images.
        let img = negotiate_extensions(Some("image/*"), &[]);
        assert!(matches!(
            img.first().map(String::as_str),
            Some("svg" | "png" | "jpg" | "jpeg" | "gif" | "webp")
        ));
        // q-weighting: lower q for html lets markdown win.
        let weighted = negotiate_extensions(Some("text/html;q=0.1, text/markdown;q=0.9"), &[]);
        assert_eq!(weighted.first().map(String::as_str), Some("md"));
    }

    #[test]
    fn negotiate_fronts_configured_priority() {
        let priority = vec!["md".to_string(), "json".to_string()];
        // No Accept → priority[0] is first, regardless of built-in table order.
        let order = negotiate_extensions(None, &priority);
        assert_eq!(order.first().map(String::as_str), Some("md"));
        // Other known types still resolvable (appended after the priority front).
        assert!(order.iter().any(|e| e == "html"));
        // Accept still reorders among existing types (explicit opt-out).
        let html = negotiate_extensions(Some("text/html"), &priority);
        assert_eq!(html.first().map(String::as_str), Some("html"));
    }
}
