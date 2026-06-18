//! `pipeline` — a pipeline store (PRD §10.3), v1's store-transform pattern.
//!
//! Authoring lives under the reserved subtree `/<mount>/.pipelines/…` — a
//! normal store-contract surface delegated to the owned `FileService` via
//! [`SpecStore`], with envelopes validated (and DSL canonicalized to the
//! typed spec) at write time. `GET <spec>?$plan` returns the segment plan.
//!
//! Every other path, on **any HTTP verb**, executes: the longest stored
//! prefix wins, and a spec named `.root` governs the mount root — so a
//! pipeline can transparently wrap another service (custom security
//! context, unchanged API), the reason pipelines pass verbs through.

use async_trait::async_trait;
use serde_json::Value;

use super::spec_store::SpecStore;
use super::{Service, ServiceContext};
use crate::error::RsError;
use crate::message::{Message, Source};
use crate::pipeline::{plan, Executor, PipelineLimits, PipelineSpec};
use crate::retry::RetryPolicy;

/// Default storage prefix (under the tenant file store) for pipeline mounts.
pub const PIPELINE_PREFIX: &str = ".rs2-pipelines";

/// The reserved authoring subtree segment.
pub const PIPELINE_SUBTREE: &str = ".pipelines";

pub struct PipelineService {
    store: SpecStore,
}

impl PipelineService {
    pub fn from_config(config: &Value, store: SpecStore) -> Result<Self, RsError> {
        if config.get("pipeline").is_some() {
            return Err(RsError::bad_request(
                "config-defined pipelines are no longer supported: PUT the spec envelope to \
                 /<mount>/.pipelines/<name> (or .pipelines/.root to govern the mount root)",
            ));
        }
        Ok(PipelineService { store })
    }

    /// Write-time validator: envelope `{pipeline, retry?, description?,
    /// x-…}`; the pipeline (typed or string DSL) is converted and validated,
    /// and the **typed form** is what gets stored (the PRD's stored format).
    pub fn validator() -> super::spec_store::SpecValidator {
        std::sync::Arc::new(|doc: &Value| {
            let obj = doc.as_object().ok_or_else(|| {
                RsError::bad_request("a stored pipeline is a JSON object envelope")
            })?;
            let spec_value = obj
                .get("pipeline")
                .ok_or_else(|| RsError::bad_request("pipeline envelope requires a 'pipeline'"))?;
            let spec = PipelineSpec::from_value(spec_value)?;
            if let Some(retry) = obj.get("retry") {
                serde_json::from_value::<RetryPolicy>(retry.clone())
                    .map_err(|e| RsError::bad_request(format!("invalid 'retry' policy: {e}")))?;
            }
            if let Some(access) = obj.get("access") {
                validate_access_shape(access)?;
            }
            let mut canonical = obj.clone();
            canonical.insert(
                "pipeline".to_string(),
                serde_json::to_value(&spec).map_err(|e| RsError::internal(e.to_string()))?,
            );
            Ok(Value::Object(canonical))
        })
    }

    fn spec_from_doc(doc: &Value) -> Result<PipelineSpec, RsError> {
        serde_json::from_value(doc.get("pipeline").cloned().unwrap_or(Value::Null))
            .map_err(|e| RsError::internal(format!("stored pipeline is corrupt: {e}")))
    }
}

/// The effective per-spec access: the spec's `access` overlaid on the mount's
/// `access` floor. When both are role objects the spec wins **per key** (so a
/// spec can loosen `invoke` while inheriting the mount's `read`); otherwise the
/// most-specific present value (the spec) wins wholesale. `None` ⇒ open.
fn effective_access(mount: Option<&Value>, spec: Option<&Value>) -> Option<Value> {
    match (mount, spec) {
        (None, None) => None,
        (Some(m), None) => Some(m.clone()),
        (None, Some(s)) => Some(s.clone()),
        (Some(Value::Object(m)), Some(Value::Object(s))) => {
            let mut merged = m.clone();
            for (k, v) in s {
                merged.insert(k.clone(), v.clone());
            }
            Some(Value::Object(merged))
        }
        (Some(_), Some(s)) => Some(s.clone()),
    }
}

/// Validate the shape of a spec envelope's `access` field: a string shorthand
/// (`"open"` / `"authenticated"`) or an object of string role specs keyed only
/// by the action vocabulary (`read`/`write`/`delete`/`invoke`). `manage` and
/// unknown keys are rejected (a spec is not a management scope, and the guard
/// catches typos like `invokeRoles`).
fn validate_access_shape(access: &Value) -> Result<(), RsError> {
    match access {
        Value::String(s) if s == "open" || s == "authenticated" => Ok(()),
        Value::String(s) => Err(RsError::bad_request(format!(
            "unknown access policy '{s}' (expected \"open\" or \"authenticated\")"
        ))),
        Value::Object(map) => {
            for (key, val) in map {
                if !matches!(key.as_str(), "read" | "write" | "delete" | "invoke") {
                    return Err(RsError::bad_request(format!(
                        "unknown access key '{key}' (allowed: read, write, delete, invoke)"
                    )));
                }
                if !val.is_string() {
                    return Err(RsError::bad_request(format!(
                        "access '{key}' must be a role-spec string"
                    )));
                }
            }
            Ok(())
        }
        _ => Err(RsError::bad_request(
            "'access' must be a string or role object",
        )),
    }
}

#[async_trait]
impl Service for PipelineService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        if self.store.is_authoring(&msg) {
            // `GET <spec>?$plan` — segment-plan introspection (PRD §8.3).
            if msg.method == http::Method::GET
                && !msg.url.is_directory()
                && msg.url.query_param("$plan").is_some()
            {
                let rel = msg
                    .url
                    .service_path
                    .strip_prefix(&format!("/{PIPELINE_SUBTREE}"))
                    .unwrap_or("/")
                    .to_string();
                let doc = self.store.read(&rel).await?;
                let spec = Self::spec_from_doc(&doc)?;
                return Ok(msg.ok_json(&serde_json::json!({
                    "pipeline": spec,
                    "plan": plan(&spec),
                })));
            }
            return self.store.handle_authoring(msg).await;
        }

        // ---- execution: any verb, longest stored prefix, .root fallback ----
        let segments: Vec<String> = msg
            .url
            .service_segments()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let Some((doc, matched_len)) = self.store.resolve(&segments).await? else {
            return Err(RsError::not_found(format!(
                "no stored pipeline matches '{}' (author one at {}{}/…)",
                msg.url.service_path, msg.url.base_path, PIPELINE_SUBTREE,
            )));
        };
        let spec = Self::spec_from_doc(&doc)?;

        // The peeled sub-path (segments beyond the matched spec prefix) is the
        // URL plane for `${url.path[…]}` in call URLs — so a `.root` spec can
        // transparently forward the addressed key (e.g. `/users/<email>` →
        // `/data/users/${url.path[0]}`).
        let peeled: Vec<String> = segments[matched_len..].to_vec();
        let base_segs: Vec<String> = msg
            .url
            .base_segments()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let url_name = peeled.last().cloned();
        let url_query = msg.url.query.clone();

        // Per-spec authorization. The host defers a pipeline mount's execution
        // surface to here: the matched spec's `access` overrides the mount's
        // `access` floor per key (`.root` is the mount-wide floor), evaluated
        // with the same verb→action map — POST→invoke = "run this pipeline".
        //
        // Fail closed: with no `access` on either the mount or the matched spec,
        // execution is denied (401 anonymous / 403 authenticated) — never run
        // open by default. A public pipeline must opt in with `"access": "open"`
        // on the mount or the spec.
        match effective_access(ctx.config.get("access"), doc.get("access")) {
            Some(access) => crate::wrapper::check_role_spec(
                &access,
                crate::wrapper::action_for(&msg.method),
                &msg,
            )?,
            // A runtime-originated system call (a scheduler tick) is trusted —
            // it exists only by operator config and can't be forged from the
            // wire — matching `check_role_spec`'s trust of `Source::System`.
            None if msg.source == Source::System => {}
            None if msg.principal.is_some() => {
                return Err(RsError::forbidden(
                    "this pipeline has no access policy configured",
                ))
            }
            None => {
                return Err(RsError::unauthorized(
                    "this pipeline has no access policy configured",
                ))
            }
        }

        let requester = ctx
            .requester
            .clone()
            .ok_or_else(|| RsError::internal("pipeline service has no requester capability"))?;
        let to_step = msg
            .url
            .query_param("$to-step")
            .and_then(|v| v.parse::<usize>().ok());

        let limits = PipelineLimits {
            wall_clock: ctx.pipeline_wall_clock,
            materialize_cap: ctx.limits.materialized_body_bytes,
            ..PipelineLimits::default()
        };
        // Retry resolution: envelope → mount config → tenant default.
        let envelope_retry = doc.get("retry").and_then(RetryPolicy::from_config);
        let mount_retry = RetryPolicy::from_config(ctx.config.get("retry").unwrap_or(&Value::Null));
        let retry = RetryPolicy::resolve(&[
            envelope_retry.as_ref(),
            mount_retry.as_ref(),
            ctx.tenant_retry.as_ref(),
        ]);
        let mut executor = Executor::new(requester, limits.clone(), retry)
            // The role an `elevate` step adds, from operator-controlled mount
            // config (`"elevate": "<role>"`). Authority lives here, not in the
            // authored spec.
            .with_elevate_role(
                ctx.config
                    .get("elevate")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
            )
            .with_url(peeled, base_segs, url_name, url_query);
        // Bind the mount's granted secrets as `$<name>` variables (host-side),
        // so a transform can `$hmacVerify('sha256', $<name>, $_rawBody, $sig)`.
        if let Some(secrets) = &ctx.secrets {
            executor = executor.with_vars(secrets.clone());
        }

        let result = match tokio::time::timeout(
            limits.wall_clock,
            executor.run(&spec, msg, to_step),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(RsError::limit_exceeded(
                "pipeline_wall_clock_ms",
                limits.wall_clock.as_millis() as u64,
                limits.wall_clock.as_millis() as u64,
            )),
        };

        // Pipeline failures include the failing step and per-step statuses
        // (PRD §12) so agents can recover instead of guessing.
        match result {
            Ok(mut resp) if !resp.is_ok() => {
                let steps = executor.report();
                let failed = steps
                    .iter()
                    .rev()
                    .find(|s| s["status"].as_u64().unwrap_or(0) >= 400)
                    .cloned();
                if let Some(body) = &mut resp.body {
                    if body.media_type.is_json() {
                        if let Ok(mut problem) =
                            body.as_json(ctx.limits.materialized_body_bytes).await
                        {
                            if let Some(obj) = problem.as_object_mut() {
                                obj.insert(
                                    "pipeline".to_string(),
                                    serde_json::json!({
                                        "failedStep": failed.as_ref().and_then(|f| f.get("step")),
                                        "steps": steps,
                                    }),
                                );
                                let media_type = body.media_type.clone();
                                resp.body = Some(crate::message::Body::from_string(
                                    problem.to_string(),
                                    media_type,
                                ));
                            }
                        }
                    }
                }
                Ok(resp)
            }
            other => other,
        }
    }
}
