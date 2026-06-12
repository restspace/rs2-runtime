//! `query` — stored parameterized queries (PRD §10.4), store-patterned.
//!
//! Queries are authored like files (v1's store-view pattern): `PUT` a
//! [`query_template::QueryEnvelope`] document to a path, `GET` it back,
//! `POST` to execute it, `DELETE` to remove it — no tenant-config round
//! trip. Specs live in the tenant file store under a per-mount prefix.
//!
//! Execution: `POST /<spec-path>[/<extra>/<segments>]` — extra segments
//! beyond the stored spec become positional params `"0"`, `"1"`, …; the
//! JSON body supplies named params (an array body supplies positionals).
//! Parameters are defaulted + validated against the envelope's `params`
//! schema before execution; results page with `X-Total-Count`.

use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};

use super::query_template::{Language, QueryEnvelope};
use super::{pagination, Service, ServiceContext};
use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

/// Per-mount storage prefix in the tenant file store (the mount base path
/// keeps multiple query mounts apart), like `.rs2-code/` for deployed code.
pub const QUERY_PREFIX: &str = ".rs2-queries";

pub struct QueryService;

impl QueryService {
    pub fn new() -> Self {
        QueryService
    }

    /// Mount-time config check: stored queries replaced config-defined ones,
    /// so a leftover `"queries"` config key is a misconfiguration to flag.
    pub fn from_config(config: &Value) -> Result<Self, RsError> {
        if config.get("queries").is_some() {
            return Err(RsError::bad_request(
                "config-defined queries are no longer supported: PUT query envelopes to the \
                 mount instead (stored-query store)",
            ));
        }
        Ok(QueryService)
    }
}

impl Default for QueryService {
    fn default() -> Self {
        Self::new()
    }
}

fn etag_of(bytes: &[u8]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    format!("\"{:016x}\"", hasher.finish())
}

#[async_trait]
impl Service for QueryService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let files = ctx
            .files
            .as_ref()
            .ok_or_else(|| RsError::capability_denied("files"))?;
        let base = msg.url.base_path.clone();
        let prefix = format!("{QUERY_PREFIX}{base}");
        let service_path = msg.url.service_path.clone();
        let store_path = format!("{prefix}{service_path}");

        match msg.method {
            // ---- store contract: trailing-slash listing at every level ----
            Method::GET if msg.url.is_directory() => {
                let (take, skip) = pagination(&msg);
                let (entries, total) = match files.list(&store_path, take, skip).await {
                    Ok(listing) => listing,
                    // No queries stored yet: an empty store, not a 404.
                    Err(e) if e.code == crate::error::codes::NOT_FOUND => (vec![], 0),
                    Err(e) => return Err(e),
                };
                let listing = json!({ "path": service_path, "entries": entries, "total": total });
                let mut resp =
                    msg.ok(Some(Body::from_bytes(listing.to_string(), MediaType::dir_json())));
                resp.set_header("x-total-count", &total.to_string());
                Ok(resp)
            }

            // ---- read a stored spec ----
            Method::GET => {
                let mut body = files.read(&store_path, None).await.map_err(|_| {
                    RsError::not_found(format!("no stored query at '{service_path}'"))
                })?;
                let bytes = body.materialize(ctx.limits.materialized_body_bytes).await?.clone();
                let etag = etag_of(&bytes);
                let mut resp = msg.ok(Some(Body::from_bytes(bytes, MediaType::json())));
                resp.set_header("etag", &etag);
                Ok(resp)
            }

            // ---- author a spec (validated at write time) ----
            Method::PUT => {
                if msg.url.is_directory() {
                    return Err(RsError::bad_request("cannot PUT to a container path"));
                }
                let doc = match &mut msg.body {
                    Some(b) => b.as_json(ctx.limits.materialized_body_bytes).await?,
                    None => return Err(RsError::bad_request("PUT requires an envelope body")),
                };
                QueryEnvelope::parse(&doc)?;
                let bytes = doc.to_string().into_bytes();
                let etag = etag_of(&bytes);
                let created = files
                    .write(&store_path, Body::from_bytes(bytes, MediaType::json()))
                    .await?;
                let mut resp = msg
                    .response(if created { StatusCode::CREATED } else { StatusCode::OK }, None);
                resp.set_header("etag", &etag);
                Ok(resp)
            }

            // ---- store contract: keyless create on a container ----
            Method::POST if msg.url.is_directory() => {
                let doc = match &mut msg.body {
                    Some(b) => b.as_json(ctx.limits.materialized_body_bytes).await?,
                    None => return Err(RsError::bad_request("POST requires an envelope body")),
                };
                QueryEnvelope::parse(&doc)?;
                let name = uuid::Uuid::new_v4().simple().to_string();
                let bytes = doc.to_string().into_bytes();
                files
                    .write(&format!("{store_path}{name}"), Body::from_bytes(bytes, MediaType::json()))
                    .await?;
                let mut resp = msg.response(StatusCode::CREATED, None);
                resp.set_header("location", &format!("{base}{service_path}{name}"));
                Ok(resp)
            }

            // ---- execute (longest stored prefix; extra segments = $0…) ----
            Method::POST => self.execute(msg, ctx).await,

            Method::DELETE => {
                if msg.url.is_directory() {
                    // Store contract guard: ?confirm=<container name> for
                    // non-empty containers.
                    let dir_name =
                        service_path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
                    if msg.url.query_param("confirm").as_deref() == Some(dir_name)
                        && !dir_name.is_empty()
                    {
                        files.delete_dir_all(&store_path).await?;
                    } else {
                        files.delete_dir(&store_path).await?;
                    }
                } else {
                    files.delete(&store_path).await.map_err(|_| {
                        RsError::not_found(format!("no stored query at '{service_path}'"))
                    })?;
                }
                Ok(msg.no_content())
            }

            _ => Err(RsError::new(
                405,
                crate::error::codes::BAD_REQUEST,
                "Method Not Allowed",
                "query store supports GET, PUT, POST, DELETE",
            )),
        }
    }
}

impl QueryService {
    async fn execute(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let files = ctx.files.as_ref().unwrap();
        let store = ctx
            .query
            .as_ref()
            .ok_or_else(|| RsError::internal("query service has no QueryStore capability"))?;
        let base = msg.url.base_path.clone();
        let prefix = format!("{QUERY_PREFIX}{base}");

        // Longest-prefix spec resolution: peel trailing segments into
        // positional params (v1's subpath params, made explicit).
        let segments: Vec<String> =
            msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        if segments.is_empty() {
            return Err(RsError::not_found("POST a stored query path to execute it"));
        }
        let mut spec_bytes = None;
        let mut split = segments.len();
        while split >= 1 {
            let candidate = format!("{prefix}/{}", segments[..split].join("/"));
            match files.read(&candidate, None).await {
                Ok(mut body) => {
                    spec_bytes =
                        Some(body.materialize(ctx.limits.materialized_body_bytes).await?.clone());
                    break;
                }
                Err(_) => split -= 1,
            }
        }
        let spec_bytes = spec_bytes.ok_or_else(|| {
            RsError::not_found(format!(
                "no stored query matches '{}' (PUT an envelope first)",
                msg.url.service_path
            ))
        })?;
        let doc: Value = serde_json::from_slice(&spec_bytes)
            .map_err(|e| RsError::internal(format!("stored query is corrupt: {e}")))?;
        let envelope = QueryEnvelope::parse(&doc)?;

        // Parameters: URL positionals, then the JSON body (body wins).
        let mut params = Map::new();
        for (i, seg) in segments[split..].iter().enumerate() {
            params.insert(i.to_string(), Value::String(seg.clone()));
        }
        match &mut msg.body {
            None => {}
            Some(b) => match b.as_json(ctx.limits.materialized_body_bytes).await? {
                Value::Object(named) => params.extend(named),
                Value::Array(items) => {
                    for (i, v) in items.into_iter().enumerate() {
                        params.insert(i.to_string(), v);
                    }
                }
                Value::Null => {}
                _ => {
                    return Err(RsError::bad_request(
                        "query parameters must be a JSON object or array",
                    ))
                }
            },
        }
        let params = envelope.prepare_params(params)?;

        // JSON templates substitute structurally here; string templates
        // (SQL) pass through for the adapter to bind.
        let query = match envelope.language {
            Language::Json => {
                let quote = |v: &Value| store.quote(v);
                super::query_template::substitute_json(&envelope.query, &params, &quote)?
            }
            Language::Text => envelope.query.clone(),
        };

        let (take, skip) = pagination(&msg);
        let (rows, total) = store.run_query(&query, &params, take, skip).await?;

        let mut resp = msg.ok_json(&Value::Array(rows));
        resp.set_header("x-total-count", &total.to_string());
        Ok(resp)
    }
}
