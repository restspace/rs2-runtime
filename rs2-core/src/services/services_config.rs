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

/// Catalogue of available services (PRD §10.6). Static for the built-ins;
/// custom `code:` services join via deployment (M3).
fn catalogue() -> serde_json::Value {
    serde_json::json!({
        "services": [
            { "name": "file", "description": "Streamed file storage (PRD §10.1)",
              "configSchema": { "type": "object", "properties": {} } },
            { "name": "data", "description": "Schema-validated JSON store (PRD §10.2)",
              "configSchema": { "type": "object", "properties": {
                  "enforceSchema": { "type": "boolean" } } } },
            { "name": "pipeline", "description": "Pipeline store (PRD §10.3): PUT spec envelopes to /<mount>/.pipelines/<name> (.root governs the mount root); every other path on any verb executes the longest-prefix match",
              "configSchema": { "type": "object", "properties": {
                  "retry": { "type": "object" },
                  "store": { "type": "object", "properties": { "root": { "type": "string" } } } } } },
            { "name": "query", "description": "Stored parameterized queries (PRD §10.4): PUT envelopes {language?, query, params?, output?} to /<mount>/.queries/<name>; any verb elsewhere executes the longest-prefix match",
              "configSchema": { "type": "object", "properties": {
                  "store": { "type": "object", "properties": { "root": { "type": "string" } } } } } },
            { "name": "auth", "description": "Authentication & RBAC (PRD §10.5)",
              "configSchema": { "type": "object", "properties": {
                  "userDataset": { "type": "string" } } } },
            { "name": "services", "description": "Self-configuration API (PRD §10.6)",
              "configSchema": { "type": "object", "properties": {} } }
        ]
    })
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

            // Custom service deployment (PRD §10.6): content-addressed,
            // immutable per version. Mounts reference `code:<name>@<version>`.
            (&http::Method::PUT, ["code", name]) => {
                let name = name.to_string();
                if name.is_empty() || name.contains(['/', '\\', '.']) {
                    return Err(RsError::bad_request("invalid code bundle name"));
                }
                let is_js = msg
                    .body
                    .as_ref()
                    .map(|b| b.media_type.essence().contains("javascript"))
                    .unwrap_or(false);
                let bytes = match &mut msg.body {
                    Some(b) => b.materialize(ctx.limits.materialized_body_bytes).await?.clone(),
                    None => return Err(RsError::bad_request("PUT /code/<name> requires a component body")),
                };
                // Validation: a compile smoke test in a quarantine sandbox
                // when the matching engine is in this build.
                #[allow(unused_mut, unused_assignments)]
                let mut validated = false;
                if is_js {
                    #[cfg(feature = "js")]
                    {
                        let source = std::str::from_utf8(&bytes).map_err(|_| {
                            RsError::bad_request("JS bundle is not valid UTF-8")
                        })?;
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

                let version = super::code::version_of(&bytes);
                let files = ctx
                    .files
                    .as_ref()
                    .ok_or_else(|| RsError::internal("services service has no file capability"))?;
                let (path, media_type) = if is_js {
                    (super::code::code_path_js(&name, &version), "application/javascript")
                } else {
                    (super::code::code_path(&name, &version), "application/wasm")
                };
                let body = crate::message::Body::from_bytes(
                    bytes,
                    crate::message::MediaType::new(media_type),
                );
                files.write(&path, body).await?;
                Ok(msg.response(
                    http::StatusCode::CREATED,
                    Some(crate::message::Body::from_json(&serde_json::json!({
                        "name": name,
                        "version": version,
                        "ref": format!("code:{name}@{version}"),
                        "validated": validated,
                    }))),
                ))
            }

            (&http::Method::GET, ["code", name]) => {
                let files = ctx
                    .files
                    .as_ref()
                    .ok_or_else(|| RsError::internal("services service has no file capability"))?;
                let (entries, _) = files
                    .list(&format!("{}/{name}/", super::code::CODE_PREFIX), 1000, 0)
                    .await
                    .map_err(|_| RsError::not_found(format!("no deployed code '{name}'")))?;
                let versions: Vec<String> = entries
                    .iter()
                    .map(|e| e.name.trim_end_matches(".wasm").trim_end_matches(".js").to_string())
                    .collect();
                // "current": the version(s) the live config actually mounts
                // (the deployment store itself has no notion of liveness).
                let (config, _) = control.raw_config(&msg.tenant).await?;
                let wanted = format!("code:{name}@");
                let current: Vec<serde_json::Value> = config
                    .get("mounts")
                    .and_then(|m| m.as_array())
                    .map(|ms| {
                        ms.iter()
                            .filter_map(|m| {
                                let service = m.get("service")?.as_str()?;
                                let version = service.strip_prefix(wanted.as_str())?;
                                Some(serde_json::json!({
                                    "path": m.get("path"),
                                    "version": version,
                                }))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(msg.ok_json(&serde_json::json!({
                    "name": name,
                    "versions": versions,
                    "current": current,
                })))
            }

            // Read back a deployed bundle: immutable per version, so the
            // version is the ETag and the response may cache forever.
            (&http::Method::GET, ["code", name, version]) => {
                let files = ctx
                    .files
                    .as_ref()
                    .ok_or_else(|| RsError::internal("services service has no file capability"))?;
                let (name, version) = (name.to_string(), version.to_string());
                let candidates = [
                    (super::code::code_path(&name, &version), "application/wasm"),
                    (super::code::code_path_js(&name, &version), "application/javascript"),
                ];
                for (path, media_type) in candidates {
                    if let Ok(mut body) = files.read(&path, None).await {
                        body.media_type = crate::message::MediaType::new(media_type);
                        let mut resp = msg.ok(Some(body));
                        resp.set_header("etag", &format!("\"{version}\""));
                        resp.set_header("cache-control", "private, max-age=31536000, immutable");
                        return Ok(resp);
                    }
                }
                Err(RsError::not_found(format!("no deployed code '{name}@{version}'")))
            }

            _ => Err(RsError::not_found(format!(
                "services endpoint '{}' (have: GET catalogue/services/raw/code, \
                 GET code/<name>/<version>, PUT raw, PUT code/<name>)",
                msg.url.service_path
            ))),
        }
    }
}
