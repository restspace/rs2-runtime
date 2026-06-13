//! `services` — the self-configuration API (PRD §10.6): a tenant's control
//! surface. Reads the catalogue and current config; `PUT /raw` validates the
//! entire new config, persists it, and hot-swaps the tenant atomically.

use async_trait::async_trait;

use crate::error::RsError;
use crate::message::Message;

use super::{Service, ServiceContext};

pub struct ServicesService;

impl ServicesService {
    pub fn new() -> Self {
        ServicesService
    }
}

impl Default for ServicesService {
    fn default() -> Self {
        Self::new()
    }
}

/// Catalogue of available services with config schemas (PRD §10.6). Built
/// from the typed config model in [`crate::config_schema`] so the advertised
/// schemas can't drift from what the runtime deserializes; it also carries
/// the shared `baseSchema` (mount envelope) and `tenantSchema`. Static for
/// the built-ins; custom `code:` services join via deployment (M3).
fn catalogue() -> serde_json::Value {
    crate::config_schema::catalogue()
}

/// The placeholder secrets read back as. A PUT carrying it means "keep the
/// stored value".
pub const SECRET_MASK: &str = "<secret>";

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn secrets_redact_and_round_trip() {
        let stored = json!({
            "auth": { "jwtSecret": "real-secret", "sessionMinutes": 60 },
            "secrets": { "stripe": "sk_live_x", "nested": { "k": "v" } },
            "mounts": []
        });

        // Read: secrets masked, structure intact.
        let mut read = stored.clone();
        redact_secrets(&mut read);
        assert_eq!(read["auth"]["jwtSecret"], SECRET_MASK);
        assert_eq!(read["auth"]["sessionMinutes"], 60);
        assert_eq!(read["secrets"]["stripe"], SECRET_MASK);
        assert_eq!(read["secrets"]["nested"]["k"], SECRET_MASK);

        // Write-back of the masked doc restores the stored values.
        let mut incoming = read.clone();
        incoming["mounts"] = json!([{ "path": "/x", "service": "file" }]);
        restore_secrets(&mut incoming, &stored).unwrap();
        assert_eq!(incoming["auth"]["jwtSecret"], "real-secret");
        assert_eq!(incoming["secrets"]["nested"]["k"], "v");
        assert_eq!(incoming["mounts"][0]["path"], "/x", "edits preserved");

        // Supplying a new real value is untouched.
        let mut rotated = read.clone();
        rotated["auth"]["jwtSecret"] = json!("new-secret");
        restore_secrets(&mut rotated, &stored).unwrap();
        assert_eq!(rotated["auth"]["jwtSecret"], "new-secret");

        // A mask with no stored counterpart is refused, not stored.
        let mut orphan = json!({ "auth": { "jwtSecret": SECRET_MASK }, "mounts": [] });
        assert!(restore_secrets(&mut orphan, &json!({ "mounts": [] })).is_err());
    }
}

/// Walk the config and mask secret values: `auth.jwtSecret`, plus every
/// string leaf under a top-level `secrets` object (forward-compatible with
/// the PRD's encrypted tenant-secrets block).
fn redact_secrets(config: &mut serde_json::Value) {
    if let Some(secret) = config.pointer_mut("/auth/jwtSecret") {
        if secret.is_string() {
            *secret = serde_json::json!(SECRET_MASK);
        }
    }
    if let Some(secrets) = config.get_mut("secrets") {
        mask_leaves(secrets);
    }
}

fn mask_leaves(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(_) => *value = serde_json::json!(SECRET_MASK),
        serde_json::Value::Object(o) => o.values_mut().for_each(mask_leaves),
        serde_json::Value::Array(a) => a.iter_mut().for_each(mask_leaves),
        _ => {}
    }
}

/// Replace [`SECRET_MASK`] markers in an incoming config with the stored
/// values at the same locations (structural walk), so read-modify-write
/// round trips preserve secrets. A marker with no stored counterpart is an
/// error — accepting it would store the literal mask as a (weak) secret.
fn restore_secrets(
    incoming: &mut serde_json::Value,
    current: &serde_json::Value,
) -> Result<(), RsError> {
    restore_at(incoming, current);
    if has_mask(incoming) {
        return Err(RsError::bad_request(format!(
            "config contains the secret placeholder '{SECRET_MASK}' at a location with no \
             stored value to restore — supply the real value there"
        )));
    }
    Ok(())
}

fn has_mask(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => s == SECRET_MASK,
        serde_json::Value::Object(o) => o.values().any(has_mask),
        serde_json::Value::Array(a) => a.iter().any(has_mask),
        _ => false,
    }
}

fn restore_at(incoming: &mut serde_json::Value, current: &serde_json::Value) {
    match incoming {
        serde_json::Value::String(s) if s == SECRET_MASK => {
            if current.is_string() {
                *incoming = current.clone();
            }
        }
        serde_json::Value::Object(o) => {
            for (k, v) in o.iter_mut() {
                restore_at(v, current.get(k).unwrap_or(&serde_json::Value::Null));
            }
        }
        serde_json::Value::Array(a) => {
            for (i, v) in a.iter_mut().enumerate() {
                restore_at(v, current.get(i).unwrap_or(&serde_json::Value::Null));
            }
        }
        _ => {}
    }
}

impl ServicesService {
    /// The store-patterned deployed-code subtree (`/<mount>/code/…`).
    ///
    /// Store contract mapping: trailing-slash dir+json listings at every
    /// level (bundle names as directories, versions as children, annotated
    /// `mountedAt` where the live config references them); GET child reads
    /// the bundle (ETag = version, immutable caching); **deploy = keyless
    /// POST to `/code/<name>/`** (the child name derives from content);
    /// PUT child succeeds only when the name matches the content hash (the
    /// `content-addressed` facet — idempotent for sync tools, impossible to
    /// mislabel); DELETE child/container with the usual `?confirm=` guard,
    /// plus: a version referenced by a live mount cannot be deleted.
    async fn handle_code_store(
        &self,
        mut msg: Message,
        ctx: &ServiceContext,
        control: &std::sync::Arc<dyn crate::runtime::TenantControl>,
        rest: &[&str],
    ) -> Result<Message, RsError> {
        let files = ctx
            .files
            .as_ref()
            .ok_or_else(|| RsError::internal("services service has no file capability"))?
            .prefixed(super::code::CODE_PREFIX);

        // Versions the live config references, per bundle name.
        let mounted = |config: &serde_json::Value, name: &str| -> Vec<(String, String)> {
            let wanted = format!("code:{name}@");
            config
                .get("mounts")
                .and_then(|m| m.as_array())
                .map(|ms| {
                    ms.iter()
                        .filter_map(|m| {
                            let service = m.get("service")?.as_str()?;
                            let version = service.strip_prefix(wanted.as_str())?;
                            let path = m.get("path")?.as_str()?;
                            Some((path.to_string(), version.to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        match (&msg.method, rest) {
            // ---- listings (store contract: dir+json at every level;
            // one-segment GETs list with or without the trailing slash) ----
            (&http::Method::GET, []) | (&http::Method::GET, [_]) => {
                let dir_path = if rest.is_empty() {
                    "/".to_string()
                } else {
                    format!("/{}/", rest[0])
                };
                let (take, skip) = super::pagination(&msg);
                let (mut entries, total) = match files.list(&dir_path, take, skip).await {
                    Ok(listing) => listing,
                    // An empty store lists as empty, not absent.
                    Err(e) if e.code == crate::error::codes::NOT_FOUND && rest.is_empty() => {
                        (vec![], 0)
                    }
                    Err(e) if e.code == crate::error::codes::NOT_FOUND => {
                        return Err(RsError::not_found(format!("no deployed code '{}'", rest[0])))
                    }
                    Err(e) => return Err(e),
                };
                // Annotate version entries with where the live config
                // mounts them (extra entry fields are contract-legal).
                let mut listing_entries: Vec<serde_json::Value> =
                    entries.drain(..).map(|e| serde_json::to_value(e).unwrap()).collect();
                if let [name] = rest {
                    if let Ok((config, _)) = control.raw_config(&msg.tenant).await {
                        let live = mounted(&config, name);
                        for entry in &mut listing_entries {
                            let stem = entry["name"]
                                .as_str()
                                .unwrap_or("")
                                .trim_end_matches(".wasm")
                                .trim_end_matches(".js")
                                .to_string();
                            let at: Vec<&String> = live
                                .iter()
                                .filter(|(_, v)| *v == stem)
                                .map(|(p, _)| p)
                                .collect();
                            if !at.is_empty() {
                                entry["mountedAt"] = serde_json::json!(at);
                            }
                        }
                    }
                }
                let listing = serde_json::json!({
                    "path": dir_path, "entries": listing_entries, "total": total
                });
                let mut resp = msg.ok(Some(crate::message::Body::from_bytes(
                    listing.to_string(),
                    crate::message::MediaType::dir_json(),
                )));
                resp.set_header("x-total-count", &total.to_string());
                Ok(resp)
            }

            // ---- read a bundle: immutable per version ----
            (&http::Method::GET, [name, version]) => {
                let stem = version.trim_end_matches(".wasm").trim_end_matches(".js");
                let candidates = [
                    (format!("/{name}/{version}"), None),
                    (format!("/{name}/{stem}.wasm"), Some("application/wasm")),
                    (format!("/{name}/{stem}.js"), Some("application/javascript")),
                ];
                for (path, forced_type) in candidates {
                    if let Ok(mut body) = files.read(&path, None).await {
                        if let Some(t) = forced_type {
                            body.media_type = crate::message::MediaType::new(t);
                        } else {
                            body.media_type = crate::message::MediaType::for_path(&path);
                        }
                        let mut resp = msg.ok(Some(body));
                        resp.set_header("etag", &format!("\"{stem}\""));
                        resp.set_header("cache-control", "private, max-age=31536000, immutable");
                        return Ok(resp);
                    }
                }
                Err(RsError::not_found(format!("no deployed code '{name}@{version}'")))
            }

            // ---- deploy: keyless POST to a bundle's container ----
            (&http::Method::POST, [name]) => {
                let name = name.to_string();
                if name.is_empty() || name.contains(['/', '\\', '.']) {
                    return Err(RsError::bad_request("invalid code bundle name"));
                }
                let (bytes, is_js, validated) = self.validate_bundle(&mut msg, ctx).await?;
                let version = super::code::version_of(&bytes);
                let (child, media_type) = if is_js {
                    (format!("{version}.js"), "application/javascript")
                } else {
                    (format!("{version}.wasm"), "application/wasm")
                };
                files
                    .write(
                        &format!("/{name}/{child}"),
                        crate::message::Body::from_bytes(
                            bytes,
                            crate::message::MediaType::new(media_type),
                        ),
                    )
                    .await?;
                let mut resp = msg.response(
                    http::StatusCode::CREATED,
                    Some(crate::message::Body::from_json(&serde_json::json!({
                        "name": name,
                        "version": version,
                        "ref": format!("code:{name}@{version}"),
                        "validated": validated,
                    }))),
                );
                resp.set_header(
                    "location",
                    &format!("{}/code/{name}/{child}", msg.url.base_path),
                );
                Ok(resp)
            }

            // ---- PUT child: only at its true content-derived name ----
            (&http::Method::PUT, [name, version]) => {
                let (name, version) = (name.to_string(), version.to_string());
                let stem =
                    version.trim_end_matches(".wasm").trim_end_matches(".js").to_string();
                let (bytes, is_js, _) = self.validate_bundle(&mut msg, ctx).await?;
                let computed = super::code::version_of(&bytes);
                if computed != stem {
                    return Err(RsError::conflict(format!(
                        "versions are content-addressed: these bytes are '{computed}', not \
                         '{stem}' — POST {}/code/{name}/ to deploy them",
                        msg.url.base_path
                    )));
                }
                let child = if is_js { format!("{stem}.js") } else { format!("{stem}.wasm") };
                let created = files
                    .write(
                        &format!("/{name}/{child}"),
                        crate::message::Body::from_bytes(
                            bytes,
                            crate::message::MediaType::for_path(&child),
                        ),
                    )
                    .await?;
                let mut resp = msg.response(
                    if created { http::StatusCode::CREATED } else { http::StatusCode::OK },
                    None,
                );
                resp.set_header("etag", &format!("\"{stem}\""));
                Ok(resp)
            }

            // ---- deletes, guarded by liveness ----
            (&http::Method::DELETE, [name, version]) => {
                let stem = version.trim_end_matches(".wasm").trim_end_matches(".js");
                let (config, _) = control.raw_config(&msg.tenant).await?;
                let live = mounted(&config, name);
                if let Some((path, _)) = live.iter().find(|(_, v)| v == stem) {
                    return Err(RsError::conflict(format!(
                        "version '{stem}' is mounted at '{path}' — repoint the mount first"
                    )));
                }
                for candidate in [
                    format!("/{name}/{version}"),
                    format!("/{name}/{stem}.wasm"),
                    format!("/{name}/{stem}.js"),
                ] {
                    if files.delete(&candidate).await.is_ok() {
                        return Ok(msg.no_content());
                    }
                }
                Err(RsError::not_found(format!("no deployed code '{name}@{stem}'")))
            }
            (&http::Method::DELETE, [name]) => {
                let (config, _) = control.raw_config(&msg.tenant).await?;
                let live = mounted(&config, name);
                if let Some((path, version)) = live.first() {
                    return Err(RsError::conflict(format!(
                        "version '{version}' is mounted at '{path}' — repoint the mount first"
                    )));
                }
                if msg.url.query_param("confirm").as_deref() == Some(*name) {
                    files.delete_dir_all(&format!("/{name}")).await?;
                } else {
                    files.delete_dir(&format!("/{name}")).await?;
                }
                Ok(msg.no_content())
            }

            _ => Err(RsError::not_found(
                "code store: GET listings/bundles, POST <name>/ deploys, PUT <name>/<version> \
                 (content-addressed), DELETE <name>/<version> | <name>/?confirm=",
            )),
        }
    }

    /// Materialize and smoke-test an uploaded bundle; returns
    /// (bytes, is_js, validated).
    async fn validate_bundle(
        &self,
        msg: &mut Message,
        ctx: &ServiceContext,
    ) -> Result<(bytes::Bytes, bool, bool), RsError> {
        let is_js = msg
            .body
            .as_ref()
            .map(|b| b.media_type.essence().contains("javascript"))
            .unwrap_or(false);
        let bytes = match &mut msg.body {
            Some(b) => b.materialize(ctx.limits.materialized_body_bytes).await?.clone(),
            None => return Err(RsError::bad_request("deploying requires a bundle body")),
        };
        #[allow(unused_mut, unused_assignments)]
        let mut validated = false;
        if is_js {
            #[cfg(feature = "js")]
            {
                let source = std::str::from_utf8(&bytes)
                    .map_err(|_| RsError::bad_request("JS bundle is not valid UTF-8"))?;
                crate::engines::js::JsEngine::new().compile_check(source)?;
                validated = true;
            }
        } else {
            #[cfg(feature = "wasm")]
            {
                crate::engines::wasm::WasmEngine::new()?.compile_check(&bytes)?;
                validated = true;
            }
        }
        Ok((bytes, is_js, validated))
    }
}

#[async_trait]
impl Service for ServicesService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let segments: Vec<String> =
            msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        let segments: Vec<&str> = segments.iter().map(|s| s.as_str()).collect();
        let control = ctx
            .control
            .clone()
            .ok_or_else(|| RsError::internal("services service has no control capability"))?;

        // The deployed-code subtree is store-patterned (an editor UI can
        // browse/read/create/delete it with the generic store client),
        // with the `content-addressed` facet: child names derive from
        // content, so deploy = keyless POST and PUT must name the true hash.
        if segments.first() == Some(&"code") {
            return self.handle_code_store(msg, ctx, &control, &segments[1..]).await;
        }

        match (&msg.method, segments.as_slice()) {
            (&http::Method::GET, ["catalogue"]) => Ok(msg.ok_json(&catalogue())),

            // Exposed view of current mounts.
            (&http::Method::GET, ["services"]) => {
                let (config, _) = control.raw_config(&msg.tenant).await?;
                let mounts: Vec<serde_json::Value> = config
                    .get("mounts")
                    .and_then(|m| m.as_array())
                    .map(|ms| {
                        ms.iter()
                            .map(|m| {
                                serde_json::json!({
                                    "path": m.get("path"),
                                    "service": m.get("service"),
                                    "access": m.get("config").and_then(|c| c.get("access")),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(msg.ok_json(&serde_json::json!({ "mounts": mounts })))
            }

            (&http::Method::GET, ["raw"]) => {
                let (mut config, version) = control.raw_config(&msg.tenant).await?;
                // Secrets are write-only through the self-config API
                // (PRD §9.2): never readable back.
                redact_secrets(&mut config);
                let mut resp = msg.ok_json(&config);
                resp.set_header("etag", &format!("\"{version}\""));
                Ok(resp)
            }

            // Full config replace: validate → persist → atomic swap
            // (PRD §10.6). `If-Match` gives optimistic concurrency.
            (&http::Method::PUT, ["raw"]) => {
                let mut body = match &mut msg.body {
                    Some(b) => b.as_json(ctx.limits.materialized_body_bytes).await?,
                    None => return Err(RsError::bad_request("PUT /raw requires a JSON body")),
                };
                // Read-modify-write safety: redaction markers in the
                // incoming config are replaced with the stored values, so
                // a GET → edit → PUT round trip never destroys a secret.
                let (current, _) = control.raw_config(&msg.tenant).await.unwrap_or_default();
                restore_secrets(&mut body, &current)?;
                let if_match = msg
                    .header("if-match")
                    .map(|v| v.trim().trim_matches('"').to_string());
                let version = control.put_config(&msg.tenant, body, if_match.as_deref()).await?;
                let mut resp = msg.no_content();
                resp.set_header("etag", &format!("\"{version}\""));
                Ok(resp)
            }

            _ => Err(RsError::not_found(format!(
                "services endpoint '{}' (have: GET catalogue/services/raw, PUT raw, \
                 store-patterned code/ subtree)",
                msg.url.service_path
            ))),
        }
    }
}
