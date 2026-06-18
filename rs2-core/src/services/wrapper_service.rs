//! `wrapper` — a single inline pipeline fronting another mount (PRD §10.3).
//!
//! Where the `pipeline` service is a *store* of authored specs resolved per
//! request, a `wrapper` mount carries **one** pipeline spec in its config and
//! runs it for every verb and every sub-path — a fixed transform/proxy in
//! front of a wrapped service. Three things make it behave like the mount it
//! fronts:
//!
//! - it declares its discovery `pattern`/`facets` in config (read by
//!   `discovery`), so a client treats `/wrapper` like the `store` (etc.) it
//!   wraps;
//! - a step forwards the **exact** request path beyond the mount with
//!   `${url.rest}` — `GET /wrapper/a/b` → a call to `/wrapped${url.rest}`
//!   reaches `/wrapped/a/b`, and `/wrapper/` reaches `/wrapped/`; and
//! - it can declare an `inputSchema` (enforced on the request body) and an
//!   `outputSchema` (advisory), surfaced in discovery/OpenAPI/agent-surface.
//!
//! Access is host-enforced against the mount's own `access` (the standard
//! `check_access` path, fail-closed) — unlike `pipeline`, there is no per-spec
//! access layer to defer to. Sub-calls are internal requests carrying the
//! principal, so the wrapped mount still enforces its own `access`.

use std::sync::Arc;

use async_trait::async_trait;
use http::Method;
use serde_json::{json, Value};

use super::pipeline_service::{run_inline, ExecInputs};
use super::{Service, ServiceContext};
use crate::error::RsError;
use crate::message::Message;
use crate::pipeline::PipelineSpec;

pub struct WrapperService {
    spec: PipelineSpec,
    /// Compiled validator for the declared `inputSchema` (enforced on bodied
    /// verbs). `outputSchema` is advisory — compile-checked at build time but
    /// not stored or enforced (`discovery` reads it from the raw config).
    input_validator: Option<Arc<jsonschema::Validator>>,
}

impl WrapperService {
    /// Parse and validate the inline pipeline from `config.pipeline`, and
    /// compile the declared schemas. A malformed pipeline spec or a malformed
    /// `inputSchema`/`outputSchema` is a 400 at build time. `pattern`/`facets`
    /// and the schemas live in the mount config and are read directly by
    /// `discovery`, so only the input validator is retained here.
    pub fn from_config(config: &Value) -> Result<Self, RsError> {
        let spec_value = config
            .get("pipeline")
            .ok_or_else(|| RsError::bad_request("a wrapper mount requires an inline 'pipeline' spec"))?;
        let spec = PipelineSpec::from_value(spec_value)?;
        let input_validator = compile_schema(config.get("inputSchema"), "inputSchema")?;
        // Compile-check the advisory output schema so a malformed one fails fast;
        // it is not enforced, so the compiled validator is discarded.
        compile_schema(config.get("outputSchema"), "outputSchema")?;
        Ok(WrapperService { spec, input_validator })
    }
}

/// Compile an optional JSON Schema: `Ok(None)` when absent, 400 when malformed.
fn compile_schema(
    schema: Option<&Value>,
    label: &str,
) -> Result<Option<Arc<jsonschema::Validator>>, RsError> {
    match schema {
        None => Ok(None),
        Some(s) => jsonschema::validator_for(s).map(|v| Some(Arc::new(v))).map_err(|e| {
            RsError::bad_request(format!("wrapper '{label}' is not a valid JSON Schema: {e}"))
        }),
    }
}

#[async_trait]
impl Service for WrapperService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        // Enforce the declared input schema on verbs that carry a body, before
        // the pipeline runs. `as_json` materializes the body in place, so it is
        // intact for the pipeline to read afterwards.
        if let Some(validator) = &self.input_validator {
            if matches!(msg.method, Method::PUT | Method::POST | Method::PATCH) {
                if let Some(body) = msg.body.as_mut() {
                    let value = body.as_json(ctx.limits.materialized_body_bytes).await?;
                    validate_input(validator, &value)?;
                }
            }
        }

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

/// Validate `value` against the wrapper's input schema, shaping failures into a
/// 422 with a problem+json `errors` array (mirrors the data service).
fn validate_input(validator: &jsonschema::Validator, value: &Value) -> Result<(), RsError> {
    let errors: Vec<Value> = validator
        .iter_errors(value)
        .map(|e| json!({ "path": e.instance_path.to_string(), "message": e.to_string() }))
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(RsError::validation_failed(
            "request body does not conform to the wrapper's inputSchema",
            Value::Array(errors),
        ))
    }
}
