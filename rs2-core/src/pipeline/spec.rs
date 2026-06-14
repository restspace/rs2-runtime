//! Typed pipeline spec (PRD §8.1). The stored format is structured and
//! schema-validatable; the terse string DSL is accepted as sugar and
//! converted on the way in ([`super::dsl`]).

use serde::{Deserialize, Serialize};

use crate::error::RsError;
use crate::retry::{EffectClass, RetryPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Mode {
    #[default]
    Serial,
    Parallel,
    /// First step whose condition passes executes; its result ends the pipeline.
    Conditional,
    /// Fire-and-forget branch; the original message continues immediately.
    Tee,
    /// Branch executes to completion, result discarded; original continues.
    TeeWait,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Action {
    /// Abort the pipeline; the failing message is the output.
    Stop,
    /// Continue to the next step.
    Next,
    /// The message exits the pipeline here (skips remaining steps).
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Joiner {
    /// Named messages → one JSON object keyed by message name.
    JsonObject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Splitter {
    /// JSON array/object body → one message per element.
    JsonSplit,
}

/// An internal or external call made by a pipeline step.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallSpec {
    pub method: String,
    /// Target URL; `${...}` interpolates variables and input-URL parts.
    pub url: String,
    /// Declared effect class; defaults from the method (PRD §7.1).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect: Option<EffectClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<std::collections::HashMap<String, String>>,
}

impl CallSpec {
    pub fn effect_class(&self) -> EffectClass {
        self.effect.unwrap_or_else(|| {
            http::Method::from_bytes(self.method.as_bytes())
                .map(|m| EffectClass::default_for_method(&m))
                .unwrap_or(EffectClass::Unsafe)
        })
    }
}

/// One pipeline step. Exactly one of `call` / `transform` / `pipeline` /
/// `split` must be present ([`PipelineSpec::validate`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Step {
    /// Condition (PRD §8.2 expression language); step skipped when false.
    #[serde(rename = "if", skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call: Option<CallSpec>,
    /// JSONata transform template applied to the JSON body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<Box<PipelineSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split: Option<Splitter>,
    /// Convert a failure into `{ _errorStatus, _errorMessage }` and continue.
    #[serde(rename = "try", skip_serializing_if = "std::ops::Not::not")]
    pub try_mode: bool,
    /// Run the step's call as **trusted internal composition**: the caller
    /// principal is dropped, so the call is authorized as an unattributed
    /// internal hop and can reach a mount the caller's roles could not (the
    /// gateway pattern — lock a service to e.g. `writeRoles: "A"` and front it
    /// with a caller-accessible pipeline that elevates). The target must admit
    /// internal access (a role-spec mount admits no-principal internal calls).
    /// Applies to `call` steps; default off. The authoring surface is
    /// `manageRoles`-gated, so only privileged authors can create elevations.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub elevate: bool,
    /// `as: "$name"` captures the step result into a variable; the in-flight
    /// message keeps its prior body.
    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub capture: Option<String>,
    /// Message name, keying `jsonObject` joins.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Per-call retry policy override (PRD §7.3 resolution).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
}

impl Step {
    pub fn call(method: &str, url: &str) -> Step {
        Step {
            call: Some(CallSpec {
                method: method.to_string(),
                url: url.to_string(),
                effect: None,
                headers: None,
            }),
            ..Default::default()
        }
    }

    /// The effect class this step contributes to segment analysis.
    pub fn effect_class(&self) -> Option<EffectClass> {
        self.call.as_ref().map(|c| c.effect_class())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PipelineSpec {
    pub mode: Mode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_fail: Option<Action>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_succeed: Option<Action>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join: Option<Joiner>,
    pub steps: Vec<Step>,
}

impl PipelineSpec {
    /// Effective on-fail action: configured, else `stop` for parallel,
    /// `next` otherwise (Restspace semantics retained).
    pub fn fail_action(&self) -> Action {
        self.on_fail.unwrap_or(match self.mode {
            Mode::Parallel => Action::Stop,
            _ => Action::Next,
        })
    }

    pub fn succeed_action(&self) -> Action {
        self.on_succeed.unwrap_or(match self.mode {
            Mode::Conditional => Action::End,
            _ => Action::Next,
        })
    }

    /// Config-time validation: step shape, condition grammar, mode rules,
    /// nested pipelines. Returns all problems, not just the first.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        self.validate_into("", &mut errors);
        errors
    }

    fn validate_into(&self, at: &str, errors: &mut Vec<String>) {
        if self.steps.is_empty() {
            errors.push(format!("{at}: pipeline has no steps"));
        }
        if self.mode == Mode::Parallel && self.on_fail.is_some_and(|a| a != Action::Stop) {
            errors.push(format!("{at}: parallel mode must stop on fail"));
        }
        if self.on_succeed == Some(Action::Stop) {
            errors.push(format!("{at}: cannot stop on success or the pipeline always fails"));
        }
        for (i, step) in self.steps.iter().enumerate() {
            let here = format!("{at}/steps[{i}]");
            let actions = [
                step.call.is_some(),
                step.transform.is_some(),
                step.pipeline.is_some(),
                step.split.is_some(),
            ]
            .iter()
            .filter(|p| **p)
            .count();
            if actions != 1 {
                errors.push(format!(
                    "{here}: a step must have exactly one of call/transform/pipeline/split"
                ));
            }
            if let Some(cond) = &step.condition {
                if let Err(e) = super::condition::Condition::parse(cond) {
                    errors.push(format!("{here}: invalid condition '{cond}': {e}"));
                }
            }
            if let Some(capture) = &step.capture {
                if !capture.starts_with('$') {
                    errors.push(format!("{here}: 'as' must name a variable starting with '$'"));
                }
            }
            if let Some(call) = &step.call {
                if http::Method::from_bytes(call.method.as_bytes()).is_err() {
                    errors.push(format!("{here}: invalid method '{}'", call.method));
                }
            }
            if step.elevate && step.call.is_none() {
                errors.push(format!("{here}: 'elevate' applies only to call steps"));
            }
            if let Some(sub) = &step.pipeline {
                sub.validate_into(&here, errors);
            }
        }
    }

    /// Parse from JSON: the typed form directly, or an array (the Restspace
    /// string DSL, PRD §8.1) converted via [`super::dsl::convert`].
    pub fn from_value(value: &serde_json::Value) -> Result<PipelineSpec, RsError> {
        let spec = if value.is_array() {
            super::dsl::convert(value)?
        } else {
            serde_json::from_value(value.clone())
                .map_err(|e| RsError::bad_request(format!("invalid pipeline spec: {e}")))?
        };
        let errors = spec.validate();
        if !errors.is_empty() {
            return Err(RsError::validation_failed(
                "pipeline spec failed validation",
                serde_json::json!(errors),
            ));
        }
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_prd_example() {
        let spec: PipelineSpec = serde_json::from_value(serde_json::json!({
            "mode": "serial",
            "onFail": "stop",
            "concurrency": 12,
            "steps": [
                { "call": { "method": "GET", "url": "/data/orders/${id}" }, "as": "$order" },
                { "if": "$order.status == 'open'",
                  "call": { "method": "POST", "url": "/payments/charge", "effect": "keyed" },
                  "try": true },
                { "transform": { "total": "$sum($order.lines.price)", "charged": "$_ok" } },
                { "pipeline": {
                    "mode": "parallel",
                    "steps": [
                        { "call": { "method": "GET", "url": "/data/customers/${order.customerId}" }, "as": "$customer" },
                        { "call": { "method": "GET", "url": "/stock/check" }, "as": "$stock" }
                    ],
                    "join": "jsonObject" } }
            ]
        }))
        .unwrap();
        assert_eq!(spec.steps.len(), 4);
        assert_eq!(spec.on_fail, Some(Action::Stop));
        assert_eq!(spec.steps[1].call.as_ref().unwrap().effect_class(), EffectClass::Keyed);
        assert!(spec.steps[1].try_mode);
        assert!(spec.validate().is_empty());
        // Method-inferred effects.
        assert_eq!(spec.steps[0].call.as_ref().unwrap().effect_class(), EffectClass::Pure);
    }

    #[test]
    fn validation_catches_shape_errors() {
        let spec: PipelineSpec = serde_json::from_value(serde_json::json!({
            "steps": [
                { "as": "$x" },
                { "if": "status ==", "call": { "method": "GET", "url": "/x" } },
                { "call": { "method": "NOT A METHOD", "url": "/x" } }
            ]
        }))
        .unwrap();
        let errors = spec.validate();
        assert_eq!(errors.len(), 3, "{errors:?}");
    }
}
