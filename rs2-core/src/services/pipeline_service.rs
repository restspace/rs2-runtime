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
use crate::message::Message;
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
            let obj = doc
                .as_object()
                .ok_or_else(|| RsError::bad_request("a stored pipeline is a JSON object envelope"))?;
            let spec_value = obj
                .get("pipeline")
                .ok_or_else(|| RsError::bad_request("pipeline envelope requires a 'pipeline'"))?;
            let spec = PipelineSpec::from_value(spec_value)?;
            if let Some(retry) = obj.get("retry") {
                serde_json::from_value::<RetryPolicy>(retry.clone())
                    .map_err(|e| RsError::bad_request(format!("invalid 'retry' policy: {e}")))?;
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
        let segments: Vec<String> =
            msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        let Some((doc, _split)) = self.store.resolve(&segments).await? else {
            return Err(RsError::not_found(format!(
                "no stored pipeline matches '{}' (author one at {}{}/…)",
                msg.url.service_path,
                msg.url.base_path,
                PIPELINE_SUBTREE,
            )));
        };
        let spec = Self::spec_from_doc(&doc)?;

        let requester = ctx
            .requester
            .clone()
            .ok_or_else(|| RsError::internal("pipeline service has no requester capability"))?;
        let to_step = msg.url.query_param("$to-step").and_then(|v| v.parse::<usize>().ok());

        let limits = PipelineLimits {
            wall_clock: ctx.pipeline_wall_clock,
            materialize_cap: ctx.limits.materialized_body_bytes,
            ..PipelineLimits::default()
        };
        // Retry resolution: envelope → mount config → tenant default.
        let envelope_retry = doc.get("retry").and_then(RetryPolicy::from_config);
        let mount_retry =
            RetryPolicy::from_config(ctx.config.get("retry").unwrap_or(&Value::Null));
        let retry = RetryPolicy::resolve(&[
            envelope_retry.as_ref(),
            mount_retry.as_ref(),
            ctx.tenant_retry.as_ref(),
        ]);
        let executor = Executor::new(requester, limits.clone(), retry);

        let result =
            match tokio::time::timeout(limits.wall_clock, executor.run(&spec, msg, to_step)).await
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
