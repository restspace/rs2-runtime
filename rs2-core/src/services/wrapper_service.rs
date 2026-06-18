//! `wrapper` — a single inline pipeline fronting another mount (PRD §10.3).
//!
//! Where the `pipeline` service is a *store* of authored specs resolved per
//! request, a `wrapper` mount carries **one** pipeline spec in its config and
//! runs it for every verb and every sub-path — a fixed transform/proxy in
//! front of a wrapped service. Two things make it behave like the mount it
//! fronts:
//!
//! - it can declare its discovery `pattern`/`facets` in config (read by
//!   `discovery`), so a client treats `/wrapper` like the `store` (etc.) it
//!   wraps; and
//! - a step forwards the **exact** request path beyond the mount with
//!   `${url.rest}` — `GET /wrapper/a/b` → a call to `/wrapped${url.rest}`
//!   reaches `/wrapped/a/b`, and `/wrapper/` reaches `/wrapped/`.
//!
//! Access is host-enforced against the mount's own `access` (the standard
//! `check_access` path, fail-closed) — unlike `pipeline`, there is no per-spec
//! access layer to defer to. Sub-calls are internal requests carrying the
//! principal, so the wrapped mount still enforces its own `access`.

use async_trait::async_trait;
use serde_json::Value;

use super::pipeline_service::{run_inline, ExecInputs};
use super::{Service, ServiceContext};
use crate::error::RsError;
use crate::message::Message;
use crate::pipeline::PipelineSpec;

pub struct WrapperService {
    spec: PipelineSpec,
}

impl WrapperService {
    /// Parse and validate the inline pipeline from `config.pipeline` (typed or
    /// string-DSL — [`PipelineSpec::from_value`] validates either, 400 on a bad
    /// spec). `pattern`/`facets` live in the mount config and are read directly
    /// by `discovery`, so they need no storage here.
    pub fn from_config(config: &Value) -> Result<Self, RsError> {
        let spec_value = config
            .get("pipeline")
            .ok_or_else(|| RsError::bad_request("a wrapper mount requires an inline 'pipeline' spec"))?;
        let spec = PipelineSpec::from_value(spec_value)?;
        Ok(WrapperService { spec })
    }
}

#[async_trait]
impl Service for WrapperService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        // No spec-prefix matching (the inline spec governs the whole mount), so
        // the entire sub-path beyond the mount is the URL plane. `rest` is the
        // byte-exact suffix (`MsgUrl::service_path`) for transparent forwarding.
        let peeled: Vec<String> =
            msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        let base_segs: Vec<String> =
            msg.url.base_segments().iter().map(|s| s.to_string()).collect();
        let url_name = peeled.last().cloned();
        let url_query = msg.url.query.clone();
        let rest = msg.url.service_path.clone();
        run_inline(
            &self.spec,
            msg,
            ctx,
            ExecInputs { peeled, base_segs, url_name, url_query, rest, envelope_retry: None },
        )
        .await
    }
}
