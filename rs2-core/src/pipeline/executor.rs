//! The pipeline executor (PRD §8.2–8.3, §7.3).
//!
//! Executes a typed [`PipelineSpec`] over a message: serial segments with
//! retry, parallel fan-out with joins, conditional selection, tee branches,
//! splitters, JSONata transforms, variables, and `${...}` interpolation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::StreamExt;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::error::RsError;
use crate::message::{Body, Message, Source};
use crate::retry::{retry_request, EffectClass, RetryPolicy};

use super::condition::Condition;
use super::segments;
use super::spec::{Action, Joiner, Mode, PipelineSpec, Step};
use super::transform;

/// Dispatches a message produced by a pipeline step. The runtime supplies an
/// implementation that routes internally (and, in time, externally through
/// `HttpOut` grants); failures come back as error-status messages.
#[async_trait]
pub trait Requester: Send + Sync {
    async fn request(&self, msg: Message) -> Message;
}

/// Executor limits (PRD §8.3, §9.3), tenant-configurable within ceilings.
#[derive(Debug, Clone)]
pub struct PipelineLimits {
    pub wall_clock: Duration,
    pub max_steps: usize,
    pub max_fanout: usize,
    pub max_depth: usize,
    pub default_concurrency: usize,
    pub materialize_cap: u64,
    /// Bodies at or under this size are snapshotted at segment boundaries,
    /// making the segment retryable (PRD §7.3, default 1 MB).
    pub snapshot_threshold: u64,
}

impl Default for PipelineLimits {
    fn default() -> Self {
        PipelineLimits {
            wall_clock: Duration::from_secs(120),
            max_steps: 1000,
            max_fanout: 1000,
            max_depth: 16,
            default_concurrency: 12,
            materialize_cap: 100 * 1024 * 1024,
            snapshot_threshold: 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct Executor {
    requester: Arc<dyn Requester>,
    limits: PipelineLimits,
    /// Resolved retry policy for segment retries and calls without a
    /// per-call override (per-call → mount → tenant → runtime default).
    retry: RetryPolicy,
    /// Pipeline invocation id: the root of idempotency key derivation
    /// (PRD §7.3) — fresh per invocation, stable across segment retries.
    invocation_id: String,
    /// Per-step execution report: failures surface the failing step and
    /// per-step statuses in the structured error (PRD §12).
    report: Arc<std::sync::Mutex<Vec<Value>>>,
}

/// What a step did with the in-flight message.
enum Flow {
    /// Continue to the next step with this message.
    Continue(Message),
    /// The message exits the pipeline now (`end` action, conditional hit,
    /// a split consumed the remaining steps).
    Exit(Message),
    /// Abort: the failing message is the pipeline's output (`stop`).
    Abort(Message),
}

impl Executor {
    pub fn new(requester: Arc<dyn Requester>, limits: PipelineLimits, retry: RetryPolicy) -> Self {
        Executor {
            requester,
            limits,
            retry,
            invocation_id: uuid::Uuid::new_v4().simple().to_string(),
            report: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Per-step statuses recorded during the last run (step key paths are
    /// `/<index>` chains; segment retries append repeated entries).
    pub fn report(&self) -> Vec<Value> {
        self.report.lock().unwrap().clone()
    }

    fn record(&self, step_path: &str, kind: &str, status: u16) {
        self.report.lock().unwrap().push(serde_json::json!({
            "step": step_path,
            "kind": kind,
            "status": status,
        }));
    }

    /// Run a pipeline over a message. `to_step` truncates execution after
    /// the given top-level step index (`?$to-step=N` debugging, PRD §8.2).
    pub async fn run(
        &self,
        spec: &PipelineSpec,
        msg: Message,
        to_step: Option<usize>,
    ) -> Result<Message, RsError> {
        let total = count_steps(spec);
        if total > self.limits.max_steps {
            return Err(RsError::limit_exceeded(
                "pipeline_steps",
                total as u64,
                self.limits.max_steps as u64,
            ));
        }
        let mut vars = Map::new();
        match self.run_pipeline(spec, msg, &mut vars, 0, "", to_step).await? {
            Flow::Continue(m) | Flow::Exit(m) | Flow::Abort(m) => Ok(m),
        }
    }

    /// `key_path` uniquely locates a step in the (possibly nested) spec, so
    /// auto-derived idempotency keys are stable across retries and distinct
    /// across steps.
    fn run_pipeline<'a>(
        &'a self,
        spec: &'a PipelineSpec,
        msg: Message,
        vars: &'a mut Map<String, Value>,
        depth: usize,
        key_path: &'a str,
        to_step: Option<usize>,
    ) -> BoxFuture<'a, Result<Flow, RsError>> {
        Box::pin(async move {
            if depth > self.limits.max_depth {
                return Err(RsError::limit_exceeded(
                    "pipeline_depth",
                    depth as u64,
                    self.limits.max_depth as u64,
                ));
            }
            match spec.mode {
                Mode::Serial => self.run_serial(spec, msg, vars, depth, key_path, to_step).await,
                Mode::Parallel => self.run_parallel(spec, msg, vars, depth, key_path).await,
                Mode::Conditional => {
                    self.run_conditional(spec, msg, vars, depth, key_path).await
                }
                Mode::Tee | Mode::TeeWait => self.run_tee(spec, msg, vars, depth, key_path).await,
            }
        })
    }

    /// Serial execution in segments (PRD §7.3): the segment is the atomic
    /// unit of retry; its materialized input is the restart point.
    async fn run_serial(
        &self,
        spec: &PipelineSpec,
        mut msg: Message,
        vars: &mut Map<String, Value>,
        depth: usize,
        key_path: &str,
        to_step: Option<usize>,
    ) -> Result<Flow, RsError> {
        let plan = segments::plan(spec);
        for segment in plan.segments.iter() {
            // Snapshot the segment input when it is (or can cheaply become)
            // materialized bytes — that makes the segment retryable.
            let snapshot: Option<Option<Body>> = self.snapshot_body(&mut msg).await?;
            let retryable_segment = snapshot.is_some()
                && spec.steps[segment.start..segment.end].iter().all(|s| {
                    s.effect_class()
                        .map(|e| e.permits_retry(true)) // auto-keys count as keys (PRD §7.3)
                        .unwrap_or(true)
                });
            let max_attempts = if retryable_segment && self.retry.enabled {
                self.retry.max_attempts.max(1)
            } else {
                1
            };

            let mut attempt = 1u32;
            let flow = loop {
                let attempt_msg = match &snapshot {
                    Some(body) => clone_with_body(&msg, body.as_ref().and_then(clone_body)),
                    // Unsnapshottable (large/unknown stream): single attempt,
                    // the message itself moves in.
                    None => {
                        let bodyless = clone_with_body(&msg, None);
                        std::mem::replace(&mut msg, bodyless)
                    }
                };
                let mut attempt_vars = vars.clone();
                let flow = self
                    .run_segment(
                        spec,
                        segment.start,
                        segment.end,
                        attempt_msg,
                        &mut attempt_vars,
                        depth,
                        key_path,
                        to_step,
                    )
                    .await?;
                match flow {
                    Flow::Abort(failed) => {
                        let status = failed.status.map(|s| s.as_u16()).unwrap_or(500);
                        if attempt < max_attempts && self.retry.retryable_status(status) {
                            tokio::time::sleep(self.retry.delay(attempt, None)).await;
                            attempt += 1;
                            continue;
                        }
                        break Flow::Abort(failed);
                    }
                    other => {
                        *vars = attempt_vars;
                        break other;
                    }
                }
            };
            match flow {
                Flow::Continue(out) => msg = out,
                exit_or_abort => return Ok(exit_or_abort),
            }
            if let Some(n) = to_step {
                if n < segment.end {
                    return Ok(Flow::Exit(msg));
                }
            }
        }
        Ok(Flow::Continue(msg))
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_segment(
        &self,
        spec: &PipelineSpec,
        start: usize,
        end: usize,
        mut msg: Message,
        vars: &mut Map<String, Value>,
        depth: usize,
        key_path: &str,
        to_step: Option<usize>,
    ) -> Result<Flow, RsError> {
        for i in start..end {
            if let Some(n) = to_step {
                if i > n {
                    return Ok(Flow::Exit(msg));
                }
            }
            let step = &spec.steps[i];
            let step_path = format!("{key_path}/{i}");

            // A split consumes the remaining steps of the whole pipeline:
            // each element runs them in parallel, then joins (PRD §8.2).
            if step.split.is_some() {
                if !self.step_condition_passes(step, &msg, vars)? {
                    continue;
                }
                return self.run_split(spec, i, msg, vars, depth, key_path).await;
            }

            let kind = if step.call.is_some() {
                "call"
            } else if step.transform.is_some() {
                "transform"
            } else {
                "pipeline"
            };
            match self.run_step(step, msg, vars, depth, &step_path).await? {
                Flow::Exit(m) => {
                    self.record(&step_path, kind, m.status.map(|s| s.as_u16()).unwrap_or(200));
                    return Ok(Flow::Exit(m));
                }
                Flow::Abort(m) => {
                    self.record(&step_path, kind, m.status.map(|s| s.as_u16()).unwrap_or(500));
                    return Ok(Flow::Abort(m));
                }
                Flow::Continue(out) => {
                    self.record(&step_path, kind, out.status.map(|s| s.as_u16()).unwrap_or(200));
                    if !out.is_ok() && step.capture.is_none() && !step.try_mode {
                        match spec.fail_action() {
                            Action::Stop => return Ok(Flow::Abort(out)),
                            Action::End => return Ok(Flow::Exit(out)),
                            Action::Next => msg = out,
                        }
                    } else {
                        match spec.succeed_action() {
                            Action::End => return Ok(Flow::Exit(out)),
                            _ => msg = out,
                        }
                    }
                }
            }
        }
        Ok(Flow::Continue(msg))
    }

    /// Parallel mode (PRD §8.2): every step runs concurrently on a copy of
    /// the input; named results join into one JSON object.
    async fn run_parallel(
        &self,
        spec: &PipelineSpec,
        mut msg: Message,
        vars: &mut Map<String, Value>,
        depth: usize,
        key_path: &str,
    ) -> Result<Flow, RsError> {
        self.materialize_if_needed(&mut msg).await?;
        let concurrency = spec.concurrency.unwrap_or(self.limits.default_concurrency).max(1);

        // Clone branch inputs eagerly: the branch futures must not borrow
        // `msg` (`&Message` is not Sync because of stream bodies).
        type BranchOut<'s> =
            (usize, &'s Step, Result<Flow, RsError>, Map<String, Value>);
        let mut branches: Vec<BoxFuture<'_, BranchOut<'_>>> = Vec::new();
        for (i, step) in spec.steps.iter().enumerate() {
            let branch_msg = try_clone(&msg);
            let branch_vars = vars.clone();
            let step_path = format!("{key_path}/{i}");
            branches.push(Box::pin(async move {
                let mut branch_vars = branch_vars;
                let branch_msg = match branch_msg {
                    Some(m) => m,
                    None => {
                        return (
                            i,
                            step,
                            Err(RsError::internal("parallel branch over unmaterialized stream")),
                            branch_vars,
                        )
                    }
                };
                let flow =
                    self.run_step(step, branch_msg, &mut branch_vars, depth, &step_path).await;
                (i, step, flow, branch_vars)
            }));
        }
        let outcomes: Vec<_> = futures::stream::iter(branches)
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut results: Vec<Option<(String, Message)>> =
            (0..spec.steps.len()).map(|_| None).collect();
        for (i, step, flow, branch_vars) in outcomes {
            let out = match flow? {
                Flow::Continue(m) | Flow::Exit(m) => m,
                Flow::Abort(m) => return Ok(Flow::Abort(m)),
            };
            if !out.is_ok() && spec.fail_action() == Action::Stop && step.capture.is_none() {
                return Ok(Flow::Abort(out));
            }
            for (k, v) in branch_vars {
                vars.insert(k, v);
            }
            let name = step
                .name
                .clone()
                .or_else(|| out.name.clone())
                .unwrap_or_else(|| i.to_string());
            results[i] = Some((name, out));
        }

        let named: Vec<(String, Message)> = results.into_iter().flatten().collect();
        let template = clone_with_body(&msg, None);
        let joined = self.join(spec.join.unwrap_or(Joiner::JsonObject), named, template).await?;
        Ok(Flow::Continue(joined))
    }

    /// Conditional mode: the first step whose condition passes executes and
    /// its result ends the pipeline; no match passes the message through.
    async fn run_conditional(
        &self,
        spec: &PipelineSpec,
        msg: Message,
        vars: &mut Map<String, Value>,
        depth: usize,
        key_path: &str,
    ) -> Result<Flow, RsError> {
        for (i, step) in spec.steps.iter().enumerate() {
            if self.step_condition_passes(step, &msg, vars)? {
                let mut chosen = step.clone();
                chosen.condition = None; // already tested
                let step_path = format!("{key_path}/{i}");
                return match self.run_step(&chosen, msg, vars, depth, &step_path).await? {
                    Flow::Continue(m) | Flow::Exit(m) => Ok(Flow::Exit(m)),
                    abort => Ok(abort),
                };
            }
        }
        Ok(Flow::Continue(msg))
    }

    /// Tee modes (PRD §8.2): the steps run as a branch over a copy; the
    /// original message continues. `tee` is fire-and-forget; `teeWait`
    /// completes the branch first.
    async fn run_tee(
        &self,
        spec: &PipelineSpec,
        mut msg: Message,
        vars: &mut Map<String, Value>,
        depth: usize,
        key_path: &str,
    ) -> Result<Flow, RsError> {
        self.materialize_if_needed(&mut msg).await?;
        let branch_msg = try_clone(&msg)
            .ok_or_else(|| RsError::internal("tee branch over unmaterialized stream"))?;
        let branch_spec = PipelineSpec { mode: Mode::Serial, ..spec.clone() };
        if spec.mode == Mode::TeeWait {
            let mut branch_vars = vars.clone();
            let _ = self
                .run_pipeline(&branch_spec, branch_msg, &mut branch_vars, depth + 1, key_path, None)
                .await?;
        } else {
            let executor = self.clone();
            let key_path = key_path.to_string();
            tokio::spawn(async move {
                let mut branch_vars = Map::new();
                let _ = executor
                    .run_pipeline(&branch_spec, branch_msg, &mut branch_vars, depth + 1, &key_path, None)
                    .await;
            });
        }
        Ok(Flow::Continue(msg))
    }

    fn step_condition_passes(
        &self,
        step: &Step,
        msg: &Message,
        vars: &Map<String, Value>,
    ) -> Result<bool, RsError> {
        match &step.condition {
            None => Ok(true),
            Some(cond) => Condition::parse(cond)
                .map_err(RsError::bad_request)?
                .evaluate(msg, vars),
        }
    }

    async fn run_step(
        &self,
        step: &Step,
        mut msg: Message,
        vars: &mut Map<String, Value>,
        depth: usize,
        key_path: &str,
    ) -> Result<Flow, RsError> {
        if !self.step_condition_passes(step, &msg, vars)? {
            return Ok(Flow::Continue(msg));
        }

        if let Some(call) = &step.call {
            return self.run_call(step, call, msg, vars, key_path).await;
        }

        if let Some(template) = &step.transform {
            let input = match &mut msg.body {
                Some(body) => body.as_json(self.limits.materialize_cap).await?,
                None => Value::Null,
            };
            let mut eval_vars = vars.clone();
            let status = msg.status.map(|s| s.as_u16()).unwrap_or(200);
            eval_vars.insert("_status".into(), Value::from(status));
            eval_vars.insert("_ok".into(), Value::Bool(msg.is_ok()));
            let headers: Map<String, Value> = msg
                .headers
                .iter()
                .filter_map(|(k, v)| {
                    v.to_str().ok().map(|s| (k.to_string(), Value::String(s.to_string())))
                })
                .collect();
            eval_vars.insert("_headers".into(), Value::Object(headers));
            let out = transform::apply(template, &input, &eval_vars)?;
            if let Some(capture) = &step.capture {
                vars.insert(capture.trim_start_matches('$').to_string(), out);
            } else {
                msg.body = Some(Body::from_json(&out));
                msg.status = Some(http::StatusCode::OK);
            }
            return Ok(Flow::Continue(msg));
        }

        if let Some(sub) = &step.pipeline {
            let flow = self.run_pipeline(sub, msg, vars, depth + 1, key_path, None).await?;
            return Ok(match flow {
                // A subpipeline's `end` is local to it.
                Flow::Exit(m) => Flow::Continue(m),
                other => other,
            });
        }

        Err(RsError::bad_request("step has no action (call/transform/pipeline/split)"))
    }

    async fn run_call(
        &self,
        step: &Step,
        call: &super::spec::CallSpec,
        mut msg: Message,
        vars: &mut Map<String, Value>,
        key_path: &str,
    ) -> Result<Flow, RsError> {
        let method = http::Method::from_bytes(call.method.as_bytes())
            .map_err(|_| RsError::bad_request(format!("invalid method '{}'", call.method)))?;
        let effect = call.effect_class();
        let preserve = step.capture.is_some();

        // Interpolation context: variables ∪ input-body JSON fields ∪ query.
        let interp_ctx = self.interpolation_context(&mut msg, vars).await?;
        let url = interpolate(&call.url, &interp_ctx)?;

        // Request body: requests with bodies forward the in-flight body.
        // Cloneable (materialized) bodies feed every retry attempt; a
        // one-shot stream feeds the first attempt only and disables retry.
        let (cloneable_body, one_shot_body): (Option<Body>, Option<Body>) =
            if matches!(method, http::Method::GET | http::Method::HEAD) {
                (None, None)
            } else if preserve {
                self.materialize_if_needed(&mut msg).await?;
                (msg.body.as_ref().and_then(clone_body), None)
            } else {
                match msg.body.take() {
                    Some(b) if b.is_stream() => (None, Some(b)),
                    other => (other.as_ref().and_then(clone_body), None),
                }
            };

        let needs_key = matches!(effect, EffectClass::Keyed | EffectClass::Unsafe);
        let auto_key = derive_key(&self.invocation_id, key_path);
        let policy = if one_shot_body.is_some() {
            RetryPolicy::no_retry()
        } else {
            step.retry.clone().unwrap_or_else(|| self.retry.clone())
        };

        // Owned copies for the retry closure (`&Message` is not Sync —
        // stream bodies — so the closure must not borrow `msg`).
        let tenant = msg.tenant.clone();
        let depth_for_call = msg.depth + 1;
        let trace = msg.trace.clone();
        let principal = msg.principal.clone();

        // Bodies move into the closure: borrowing them across the retry
        // awaits would require `Body: Sync`, which stream payloads are not.
        let mut bodies = (cloneable_body, one_shot_body);
        let requester = self.requester.clone();
        let headers = call.headers.clone();
        let response = retry_request(&policy, effect, needs_key, move |_attempt| {
            let mut req = Message::request(method.clone(), &url, &tenant);
            req.source = Source::Internal;
            req.depth = depth_for_call;
            req.trace = trace.child();
            req.principal = principal.clone();
            req.body = bodies.0.as_ref().and_then(clone_body).or_else(|| bodies.1.take());
            if let Some(headers) = &headers {
                for (k, v) in headers {
                    if let (Ok(name), Ok(value)) = (
                        http::header::HeaderName::try_from(k.as_str()),
                        http::HeaderValue::from_str(v),
                    ) {
                        req.headers.insert(name, value);
                    }
                }
            }
            // Auto-generated idempotency key (PRD §7.2): keyed targets get
            // exactly-once-effective semantics without key plumbing.
            if needs_key && req.header("idempotency-key").is_none() {
                req.set_header("idempotency-key", &auto_key);
            }
            let requester = requester.clone();
            async move { Ok(requester.request(req).await) }
        })
        .await?;

        let mut out = response;
        if let Some(name) = &step.name {
            out.name = Some(name.clone());
        } else if out.name.is_none() {
            out.name = msg.name.clone();
        }

        if let Some(capture) = &step.capture {
            let value = if !out.is_ok() {
                serde_json::json!({
                    "_errorStatus": out.status.map(|s| s.as_u16()).unwrap_or(500),
                    "_errorMessage": self.body_text(&mut out).await,
                })
            } else {
                match &mut out.body {
                    Some(b) if b.media_type.is_json() => {
                        b.as_json(self.limits.materialize_cap).await?
                    }
                    _ => Value::Null,
                }
            };
            vars.insert(capture.trim_start_matches('$').to_string(), value);
            // The in-flight message keeps its prior body and continues.
            return Ok(Flow::Continue(msg));
        }

        if !out.is_ok() && step.try_mode {
            let error_body = serde_json::json!({
                "_errorStatus": out.status.map(|s| s.as_u16()).unwrap_or(500),
                "_errorMessage": self.body_text(&mut out).await,
            });
            out.body = Some(Body::from_json(&error_body));
            out.status = Some(http::StatusCode::OK);
        }
        Ok(Flow::Continue(out))
    }

    /// jsonSplit (PRD §8.2): array/object body → one message per element;
    /// each element runs the remaining pipeline steps in parallel under the
    /// concurrency limit; the named results join back into one body.
    async fn run_split(
        &self,
        spec: &PipelineSpec,
        split_index: usize,
        mut msg: Message,
        vars: &mut Map<String, Value>,
        depth: usize,
        key_path: &str,
    ) -> Result<Flow, RsError> {
        let json = match &mut msg.body {
            Some(body) => body.as_json(self.limits.materialize_cap).await?,
            None => return Err(RsError::bad_request("jsonSplit requires a JSON body")),
        };
        let elements: Vec<(String, Value)> = match json {
            Value::Array(items) => {
                items.into_iter().enumerate().map(|(i, v)| (i.to_string(), v)).collect()
            }
            Value::Object(map) => map.into_iter().collect(),
            other => vec![("0".to_string(), other)],
        };
        if elements.len() > self.limits.max_fanout {
            return Err(RsError::limit_exceeded(
                "pipeline_fanout",
                elements.len() as u64,
                self.limits.max_fanout as u64,
            ));
        }

        let rest = PipelineSpec {
            mode: Mode::Serial,
            on_fail: spec.on_fail,
            on_succeed: spec.on_succeed,
            concurrency: spec.concurrency,
            join: None,
            steps: spec.steps[split_index + 1..].to_vec(),
        };
        let concurrency = spec.concurrency.unwrap_or(self.limits.default_concurrency).max(1);

        // Build element messages and futures eagerly: the element futures
        // must not borrow `msg` (`&Message` is not Sync).
        let rest = &rest;
        let mut runs: Vec<BoxFuture<'_, (String, Result<Flow, RsError>)>> = Vec::new();
        for (name, value) in elements {
            let mut element_msg = clone_with_body(&msg, Some(Body::from_json(&value)));
            element_msg.name = Some(name.clone());
            let element_vars = vars.clone();
            let key_path = format!("{key_path}/split[{name}]");
            runs.push(Box::pin(async move {
                let mut element_vars = element_vars;
                if rest.steps.is_empty() {
                    return (name, Ok(Flow::Continue(element_msg)));
                }
                let flow = self
                    .run_pipeline(rest, element_msg, &mut element_vars, depth + 1, &key_path, None)
                    .await;
                (name, flow)
            }));
        }
        let outcomes: Vec<(String, Result<Flow, RsError>)> = futures::stream::iter(runs)
            .buffer_unordered(concurrency)
            .collect()
            .await;

        let mut named = Vec::with_capacity(outcomes.len());
        for (name, flow) in outcomes {
            let out = match flow? {
                Flow::Continue(m) | Flow::Exit(m) | Flow::Abort(m) => m,
            };
            named.push((name, out));
        }
        // Restore input order (buffer_unordered scrambles completion order).
        named.sort_by(|(a, _), (b, _)| match (a.parse::<u64>(), b.parse::<u64>()) {
            (Ok(x), Ok(y)) => x.cmp(&y),
            _ => a.cmp(b),
        });
        let template = clone_with_body(&msg, None);
        let joined = self.join(spec.join.unwrap_or(Joiner::JsonObject), named, template).await?;
        Ok(Flow::Exit(joined))
    }

    async fn join(
        &self,
        joiner: Joiner,
        named: Vec<(String, Message)>,
        template: Message,
    ) -> Result<Message, RsError> {
        match joiner {
            Joiner::JsonObject => {
                let mut out = Map::new();
                for (name, mut m) in named {
                    let value = if !m.is_ok() {
                        serde_json::json!({
                            "_errorStatus": m.status.map(|s| s.as_u16()).unwrap_or(500),
                            "_errorMessage": self.body_text(&mut m).await,
                        })
                    } else {
                        match &mut m.body {
                            Some(b) if b.media_type.is_json() => {
                                b.as_json(self.limits.materialize_cap).await?
                            }
                            Some(b) => {
                                let bytes = b.materialize(self.limits.materialize_cap).await?;
                                Value::String(String::from_utf8_lossy(bytes).into_owned())
                            }
                            None => Value::Null,
                        }
                    };
                    out.insert(name, value);
                }
                Ok(template.ok_json(&Value::Object(out)))
            }
        }
    }

    async fn body_text(&self, msg: &mut Message) -> String {
        match &mut msg.body {
            Some(b) => match b.materialize(self.limits.materialize_cap).await {
                Ok(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                Err(_) => String::new(),
            },
            None => String::new(),
        }
    }

    /// Interpolation context for `${...}`: query params, then input-body
    /// JSON fields, then variables (variables win).
    async fn interpolation_context(
        &self,
        msg: &mut Message,
        vars: &Map<String, Value>,
    ) -> Result<Map<String, Value>, RsError> {
        let mut ctx = Map::new();
        for pair in msg.url.query.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            ctx.insert(k.to_string(), Value::String(v.to_string()));
        }
        if let Some(body) = &mut msg.body {
            if body.media_type.is_json() && !body.is_stream() {
                if let Ok(Value::Object(fields)) = body.as_json(self.limits.materialize_cap).await {
                    for (k, v) in fields {
                        ctx.insert(k, v);
                    }
                }
            }
        }
        for (k, v) in vars {
            ctx.insert(k.clone(), v.clone());
        }
        Ok(ctx)
    }

    /// Snapshot the in-flight body at a segment boundary. `Some(snapshot)`
    /// means the segment input is restorable (retryable); `None` means the
    /// body is a large/unknown stream and the segment runs at most once.
    async fn snapshot_body(&self, msg: &mut Message) -> Result<Option<Option<Body>>, RsError> {
        match &mut msg.body {
            None => Ok(Some(None)),
            Some(body) => {
                if body.is_stream() {
                    match body.size {
                        // Under the snapshot threshold: force materialization
                        // (PRD §7.3 forcible materialization point).
                        Some(size) if size <= self.limits.snapshot_threshold => {
                            body.materialize(self.limits.materialize_cap).await?;
                        }
                        _ => return Ok(None),
                    }
                }
                Ok(clone_body(body).map(|b| Some(Some(b))).unwrap_or(None))
            }
        }
    }

    async fn materialize_if_needed(&self, msg: &mut Message) -> Result<(), RsError> {
        if let Some(body) = &mut msg.body {
            if body.is_stream() {
                body.materialize(self.limits.materialize_cap).await?;
            }
        }
        Ok(())
    }
}

fn count_steps(spec: &PipelineSpec) -> usize {
    spec.steps
        .iter()
        .map(|s| 1 + s.pipeline.as_ref().map(|p| count_steps(p)).unwrap_or(0))
        .sum()
}

/// Deterministic auto idempotency key (PRD §7.3): hash of the invocation id
/// and the step's location — stable across retries, distinct per step.
fn derive_key(invocation_id: &str, key_path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(invocation_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(key_path.as_bytes());
    hasher.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Clone a materialized body; `None` for streams.
fn clone_body(body: &Body) -> Option<Body> {
    match &body.payload {
        crate::message::Payload::Bytes(b) => {
            let mut cloned = Body::from_bytes(b.clone(), body.media_type.clone());
            cloned.last_modified = body.last_modified;
            cloned.provenance = body.provenance.clone();
            Some(cloned)
        }
        crate::message::Payload::Stream(_) => None,
    }
}

fn try_clone(msg: &Message) -> Option<Message> {
    let body = match &msg.body {
        None => None,
        Some(b) => Some(clone_body(b)?),
    };
    Some(clone_with_body(msg, body))
}

fn clone_with_body(msg: &Message, body: Option<Body>) -> Message {
    Message {
        method: msg.method.clone(),
        url: msg.url.clone(),
        headers: msg.headers.clone(),
        status: msg.status,
        body,
        tenant: msg.tenant.clone(),
        principal: msg.principal.clone(),
        trace: msg.trace.clone(),
        source: msg.source,
        name: msg.name.clone(),
        depth: msg.depth,
    }
}

/// Replace `${name}` / `${name.path}` with values from the context.
/// Strings interpolate raw; other values as JSON.
fn interpolate(template: &str, ctx: &Map<String, Value>) -> Result<String, RsError> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| RsError::bad_request(format!("unterminated ${{...}} in '{template}'")))?;
        let path = &after[..end];
        let mut parts = path.split('.');
        let head = parts.next().unwrap_or("");
        let mut value = ctx.get(head).cloned().unwrap_or(Value::Null);
        for key in parts {
            value = match value {
                Value::Object(mut o) => o.remove(key).unwrap_or(Value::Null),
                _ => Value::Null,
            };
        }
        match value {
            Value::String(s) => out.push_str(&s),
            Value::Null => {
                return Err(RsError::bad_request(format!(
                    "interpolation '${{{path}}}' resolved to nothing"
                )))
            }
            other => out.push_str(&other.to_string()),
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolation_resolves_paths_and_rejects_missing() {
        let ctx = serde_json::json!({ "id": "o1", "order": { "customerId": 7 } });
        let ctx = ctx.as_object().unwrap();
        assert_eq!(interpolate("/data/orders/${id}", ctx).unwrap(), "/data/orders/o1");
        assert_eq!(interpolate("/c/${order.customerId}", ctx).unwrap(), "/c/7");
        assert!(interpolate("/x/${missing}", ctx).is_err());
        assert!(interpolate("/x/${unclosed", ctx).is_err());
    }

    #[test]
    fn derived_keys_are_stable_and_distinct() {
        assert_eq!(derive_key("inv", "/0"), derive_key("inv", "/0"));
        assert_ne!(derive_key("inv", "/0"), derive_key("inv", "/1"));
        assert_ne!(derive_key("inv", "/0"), derive_key("inv2", "/0"));
    }
}
