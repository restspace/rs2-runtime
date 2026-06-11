//! `pipeline` — pipeline-as-a-service (PRD §10.3): mount a pipeline spec at
//! a path; requests to the mount flow through the pipeline. `?$plan` returns
//! the computed segment plan (retry/checkpoint boundaries) for authors and
//! agents.

use async_trait::async_trait;

use crate::error::RsError;
use crate::message::Message;
use crate::pipeline::{plan, Executor, PipelineLimits, PipelineSpec};
use crate::retry::RetryPolicy;

use super::{Service, ServiceContext};

pub struct PipelineService {
    spec: PipelineSpec,
}

impl PipelineService {
    /// Build from mount config: `{ "pipeline": <typed spec | string DSL>,
    /// "retry": <policy>? }`. Validates at config time; segment-plan
    /// warnings are part of the plan, surfaced via `?$plan`.
    pub fn from_config(config: &serde_json::Value) -> Result<Self, RsError> {
        let spec_value = config
            .get("pipeline")
            .ok_or_else(|| RsError::bad_request("pipeline mount requires a 'pipeline' config key"))?;
        let spec = PipelineSpec::from_value(spec_value)?;
        Ok(PipelineService { spec })
    }

    pub fn spec(&self) -> &PipelineSpec {
        &self.spec
    }
}

#[async_trait]
impl Service for PipelineService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        if msg.url.query_param("$plan").is_some() {
            let plan = plan(&self.spec);
            return Ok(msg.ok_json(&serde_json::json!({
                "pipeline": self.spec,
                "plan": plan,
            })));
        }

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
        let retry = RetryPolicy::resolve(&[
            RetryPolicy::from_config(ctx.config.get("retry").unwrap_or(&serde_json::Value::Null))
                .as_ref(),
            ctx.tenant_retry.as_ref(),
        ]);
        let executor = Executor::new(requester, limits.clone(), retry);

        match tokio::time::timeout(limits.wall_clock, executor.run(&self.spec, msg, to_step)).await
        {
            Ok(result) => result,
            Err(_) => Err(RsError::limit_exceeded(
                "pipeline_wall_clock_ms",
                limits.wall_clock.as_millis() as u64,
                limits.wall_clock.as_millis() as u64,
            )),
        }
    }
}
