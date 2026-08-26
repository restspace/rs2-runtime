// The pipeline executor (PRD §8.2–8.3, §7.3). Executes a typed
// `PipelineSpec` over a message: serial segments with retry, parallel
// fan-out with joins, conditional selection, tee branches, splitters,
// JSONata transforms, variables, and `${...}` interpolation. Port of
// `rs2-core/src/pipeline/executor.rs`; the flow algebra, key derivation,
// interpolation precedence, and error shapes are the contract
// (cloudflare.md §D).

import { Body } from "../runtime/body";
import { sha256Hex } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { Message, MsgUrl, simpleUuid } from "../runtime/message";
import type { Principal } from "../runtime/message";
import { isExternalUrl, urlHost } from "../runtime/outbound";
import type { ExternalDispatch } from "../runtime/outbound";
import { resolve as resolvePattern } from "../runtime/path-pattern";
import type { UrlView } from "../runtime/path-pattern";
import { noRetry, retryDelayMs, retryRequest, retryableStatus, permitsRetry } from "../runtime/retry";
import type { RetryPolicy } from "../runtime/retry";
import type { Requester } from "../services/context";
import { evaluateCondition, parseCondition } from "./condition";
import { applyResponse, detectResponse } from "./response";
import { plan as planSegments } from "./segments";
import { callEffectClass, countSteps, failAction, isValidMethod, stepEffectClass, succeedAction } from "./spec";
import type { CallSpec, Joiner, PipelineSpec, Step } from "./spec";
import * as transform from "./transform";

/// Executor limits (PRD §8.3, §9.3), tenant-configurable within ceilings.
export interface PipelineLimits {
  wallClockMs: number;
  maxSteps: number;
  maxFanout: number;
  /// Aggregate cap on a fan-out's materialized footprint (branches × bytes).
  maxFanoutBytes: number;
  maxDepth: number;
  defaultConcurrency: number;
  materializeCap: number;
  /// Bodies at or under this size are snapshotted at segment boundaries.
  snapshotThreshold: number;
}

export function defaultPipelineLimits(): PipelineLimits {
  return {
    wallClockMs: 120_000,
    maxSteps: 1000,
    maxFanout: 1000,
    maxFanoutBytes: 256 * 1024 * 1024,
    maxDepth: 16,
    defaultConcurrency: 12,
    materializeCap: 100 * 1024 * 1024,
    snapshotThreshold: 1024 * 1024,
  };
}

/// What a step did with the in-flight message.
type Flow = { kind: "continue"; msg: Message } | { kind: "exit"; msg: Message } | { kind: "abort"; msg: Message };

const cont = (msg: Message): Flow => ({ kind: "continue", msg });
const exit = (msg: Message): Flow => ({ kind: "exit", msg });
const abort = (msg: Message): Flow => ({ kind: "abort", msg });

/// Where a resolved call goes: back through internal dispatch, or out
/// through the mount's `httpOut` grants (the grant index selects the injector).
type CallTarget = { kind: "internal"; requester: Requester } | { kind: "external"; ext: ExternalDispatch; grant: number };

/// Run `tasks` with at most `limit` in flight; results in input order.
async function boundedAll<T>(tasks: Array<() => Promise<T>>, limit: number): Promise<T[]> {
  const results: T[] = new Array<T>(tasks.length);
  let next = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      const i = next++;
      if (i >= tasks.length) return;
      results[i] = await tasks[i]!();
    }
  };
  await Promise.all(Array.from({ length: Math.min(Math.max(limit, 1), tasks.length) }, worker));
  return results;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export class Executor {
  private readonly requester: Requester;
  private readonly limits: PipelineLimits;
  /// Resolved retry policy for segment retries and calls without a per-call
  /// override (per-call → mount → tenant → runtime default).
  private readonly retry: RetryPolicy;
  /// Pipeline invocation id: the root of idempotency key derivation —
  /// fresh per invocation, stable across segment retries.
  private readonly invocationId: string;
  /// Per-step execution report (failures surface it in the problem body).
  private readonly reportRows: JsonObject[] = [];
  private initialVars: JsonObject = {};
  private elevateRole: string | undefined;
  private urlPath: string[] = [];
  private urlBase: string[] = [];
  private urlName: string | undefined;
  private urlQuery = "";
  private urlRest = "";
  private external: ExternalDispatch | undefined;

  constructor(requester: Requester, limits: PipelineLimits, retry: RetryPolicy) {
    this.requester = requester;
    this.limits = limits;
    this.retry = retry;
    this.invocationId = simpleUuid();
  }

  /// Supply the incoming request's URL plane for `${url.…}` patterns.
  /// `path` is the segment list patterns index against (the peeled sub-path).
  withUrl(path: string[], base: string[], name: string | undefined, query: string, rest: string): this {
    this.urlPath = path;
    this.urlBase = base;
    this.urlName = name;
    this.urlQuery = query;
    this.urlRest = rest;
    return this;
  }

  /// Seed host-provided variables (e.g. granted secrets) into every run's scope.
  withVars(vars: JsonObject): this {
    this.initialVars = vars;
    return this;
  }

  /// The role an `elevate` step adds to the call's principal (from the
  /// pipeline mount's operator-controlled config).
  withElevateRole(role: string | undefined): this {
    this.elevateRole = role;
    return this;
  }

  /// The mount's external-call capability (its `httpOut` grants).
  withExternal(external: ExternalDispatch | undefined): this {
    this.external = external;
    return this;
  }

  /// Per-step statuses recorded during the last run.
  report(): JsonObject[] {
    return this.reportRows.map((r) => ({ ...r }));
  }

  private record(stepPath: string, kind: string, status: number): void {
    this.reportRows.push({ step: stepPath, kind, status });
  }

  /// Run a pipeline over a message. `toStep` truncates execution after the
  /// given top-level step index (`?$to-step=N`).
  async run(spec: PipelineSpec, msg: Message, toStep: number | undefined): Promise<Message> {
    const total = countSteps(spec);
    if (total > this.limits.maxSteps) throw RsError.limitExceeded("pipeline_steps", total, this.limits.maxSteps);
    const vars: JsonObject = { ...this.initialVars };
    // Bind the original caller into the spec planes: `$_user.x` in
    // transforms, `${_user.x}` in URLs. Anonymous callers get no binding.
    if (msg.principal) {
      const user: JsonObject = { email: msg.principal.id, roles: [...msg.principal.roles] };
      for (const [k, v] of Object.entries(msg.principal.extra)) user[k] = v;
      vars._user = { ...user };
      vars.principal = user;
    }
    // Bind the triggering URL: `$_url.path[n]`, `${_url.query.x}`.
    const query: JsonObject = {};
    for (const pair of msg.url.query.split("&").filter((p) => p !== "")) {
      const eq = pair.indexOf("=");
      const k = eq < 0 ? pair : pair.slice(0, eq);
      const v = msg.url.queryParam(k);
      if (v !== undefined && !Object.prototype.hasOwnProperty.call(query, k)) query[k] = v;
    }
    vars._url = { path: msg.url.serviceSegments(), query };
    const flow = await this.runPipeline(spec, msg, vars, 0, "", toStep);
    return flow.msg;
  }

  /// `keyPath` uniquely locates a step in the (possibly nested) spec, so
  /// auto-derived idempotency keys are stable across retries.
  private async runPipeline(
    spec: PipelineSpec,
    msg: Message,
    vars: JsonObject,
    depth: number,
    keyPath: string,
    toStep: number | undefined,
  ): Promise<Flow> {
    if (depth > this.limits.maxDepth) throw RsError.limitExceeded("pipeline_depth", depth, this.limits.maxDepth);
    switch (spec.mode) {
      case "serial":
        return this.runSerial(spec, msg, vars, depth, keyPath, toStep);
      case "parallel":
        return this.runParallel(spec, msg, vars, depth, keyPath);
      case "conditional":
        return this.runConditional(spec, msg, vars, depth, keyPath);
      case "tee":
      case "teeWait":
        return this.runTee(spec, msg, vars, depth, keyPath);
    }
  }

  /// Serial execution in segments (PRD §7.3): the segment is the atomic
  /// unit of retry; its materialized input is the restart point.
  private async runSerial(
    spec: PipelineSpec,
    msgIn: Message,
    vars: JsonObject,
    depth: number,
    keyPath: string,
    toStep: number | undefined,
  ): Promise<Flow> {
    let msg = msgIn;
    const segPlan = planSegments(spec);
    for (const segment of segPlan.segments) {
      // Snapshot the segment input when it is (or can cheaply become)
      // materialized bytes — that makes the segment retryable.
      const snapshot = await this.snapshotBody(msg);
      const retryableSegment =
        snapshot !== undefined &&
        spec.steps.slice(segment.start, segment.end).every((s) => {
          const e = stepEffectClass(s);
          return e === undefined ? true : permitsRetry(e, true); // auto-keys count as keys
        });
      const maxAttempts = retryableSegment && this.retry.enabled ? Math.max(this.retry.maxAttempts, 1) : 1;

      let attempt = 1;
      let flow: Flow;
      for (;;) {
        let attemptMsg: Message;
        if (snapshot !== undefined) {
          attemptMsg = cloneWithBody(msg, snapshot.body ? cloneBody(snapshot.body) : undefined);
        } else {
          // Unsnapshottable (large/unknown stream): single attempt, the
          // message itself moves in.
          attemptMsg = msg;
          msg = cloneWithBody(msg, undefined);
        }
        // A retryable segment runs against scratch vars so a failed attempt
        // can't leak partial captures into the retry.
        if (maxAttempts === 1) {
          flow = await this.runSegment(spec, segment.start, segment.end, attemptMsg, vars, depth, keyPath, toStep);
        } else {
          const attemptVars = cloneVars(vars);
          flow = await this.runSegment(spec, segment.start, segment.end, attemptMsg, attemptVars, depth, keyPath, toStep);
          if (flow.kind !== "abort") {
            for (const k of Object.keys(vars)) delete vars[k];
            Object.assign(vars, attemptVars);
          }
        }
        if (flow.kind === "abort") {
          const status = flow.msg.status ?? 500;
          if (attempt < maxAttempts && retryableStatus(this.retry, status)) {
            await sleep(retryDelayMs(this.retry, attempt, undefined));
            attempt += 1;
            continue;
          }
        }
        break;
      }
      if (flow.kind !== "continue") return flow;
      msg = flow.msg;
      if (toStep !== undefined && toStep < segment.end) return exit(msg);
    }
    return cont(msg);
  }

  private async runSegment(
    spec: PipelineSpec,
    start: number,
    end: number,
    msgIn: Message,
    vars: JsonObject,
    depth: number,
    keyPath: string,
    toStep: number | undefined,
  ): Promise<Flow> {
    let msg = msgIn;
    for (let i = start; i < end; i++) {
      if (toStep !== undefined && i > toStep) return exit(msg);
      const step = spec.steps[i]!;
      const stepPath = `${keyPath}/${i}`;

      // A split consumes the remaining steps of the whole pipeline.
      if (step.split !== undefined) {
        if (!this.stepConditionPasses(step, msg, vars)) continue;
        return this.runSplit(spec, i, msg, vars, depth, keyPath);
      }

      const kind = step.call ? "call" : step.transform !== undefined ? "transform" : "pipeline";
      const flow = await this.runStep(step, msg, vars, depth, stepPath);
      if (flow.kind === "exit") {
        this.record(stepPath, kind, flow.msg.status ?? 200);
        return flow;
      }
      if (flow.kind === "abort") {
        this.record(stepPath, kind, flow.msg.status ?? 500);
        return flow;
      }
      const out = flow.msg;
      this.record(stepPath, kind, out.status ?? 200);
      if (!out.isOk() && step.capture === undefined && !step.tryMode) {
        switch (failAction(spec)) {
          case "stop":
            return abort(out);
          case "end":
            return exit(out);
          case "next":
            msg = out;
        }
      } else if (succeedAction(spec) === "end") {
        return exit(out);
      } else {
        msg = out;
      }
    }
    return cont(msg);
  }

  /// Parallel mode: every step runs concurrently on a copy of the input;
  /// named results join into one JSON object.
  private async runParallel(spec: PipelineSpec, msg: Message, vars: JsonObject, depth: number, keyPath: string): Promise<Flow> {
    await this.materializeIfNeeded(msg);
    const bodySize = msg.body?.size ?? 0;
    this.checkFanoutBudget(spec.steps.length, bodySize);
    const concurrency = Math.max(spec.concurrency ?? this.limits.defaultConcurrency, 1);

    type BranchOut = { i: number; step: Step; flow: Flow; vars: JsonObject };
    const tasks = spec.steps.map((step, i) => async (): Promise<BranchOut> => {
      const branchMsg = tryClone(msg);
      if (!branchMsg) throw RsError.internal("parallel branch over unmaterialized stream");
      const branchVars = cloneVars(vars);
      const flow = await this.runStep(step, branchMsg, branchVars, depth, `${keyPath}/${i}`);
      return { i, step, flow, vars: branchVars };
    });
    const outcomes = await boundedAll(tasks, concurrency);

    const results: Array<[string, Message] | undefined> = spec.steps.map(() => undefined);
    for (const { i, step, flow, vars: branchVars } of outcomes) {
      if (flow.kind === "abort") return flow;
      const out = flow.msg;
      if (!out.isOk() && failAction(spec) === "stop" && step.capture === undefined) return abort(out);
      Object.assign(vars, branchVars);
      const name = step.name ?? out.name ?? String(i);
      results[i] = [name, out];
    }
    const named = results.filter((r): r is [string, Message] => r !== undefined);
    const template = cloneWithBody(msg, undefined);
    return cont(await this.join(spec.join ?? "jsonObject", named, template));
  }

  /// Conditional mode: the first step whose condition passes executes and
  /// its result ends the pipeline; no match passes the message through.
  private async runConditional(spec: PipelineSpec, msg: Message, vars: JsonObject, depth: number, keyPath: string): Promise<Flow> {
    for (let i = 0; i < spec.steps.length; i++) {
      const step = spec.steps[i]!;
      if (this.stepConditionPasses(step, msg, vars)) {
        const chosen: Step = { ...step, condition: undefined }; // already tested
        const flow = await this.runStep(chosen, msg, vars, depth, `${keyPath}/${i}`);
        return flow.kind === "abort" ? flow : exit(flow.msg);
      }
    }
    return cont(msg);
  }

  /// Tee modes: the steps run as a branch over a copy; the original message
  /// continues. `tee` is fire-and-forget; `teeWait` completes the branch first.
  private async runTee(spec: PipelineSpec, msg: Message, vars: JsonObject, depth: number, keyPath: string): Promise<Flow> {
    await this.materializeIfNeeded(msg);
    const branchMsg = tryClone(msg);
    if (!branchMsg) throw RsError.internal("tee branch over unmaterialized stream");
    const branchSpec: PipelineSpec = { ...spec, mode: "serial" };
    if (spec.mode === "teeWait") {
      await this.runPipeline(branchSpec, branchMsg, cloneVars(vars), depth + 1, keyPath, undefined);
    } else {
      // Detached: errors are swallowed, as with the Rust `tokio::spawn`
      // branch. (A DO keeps running while the request is open; a
      // `waitUntil` hand-off is a P3 refinement.)
      void this.runPipeline(branchSpec, branchMsg, {}, depth + 1, keyPath, undefined).catch(() => undefined);
    }
    return cont(msg);
  }

  private stepConditionPasses(step: Step, msg: Message, vars: JsonObject): boolean {
    if (step.condition === undefined) return true;
    let cond;
    try {
      cond = parseCondition(step.condition);
    } catch (e) {
      throw RsError.badRequest((e as Error).message);
    }
    return evaluateCondition(cond, msg, vars);
  }

  private async runStep(step: Step, msg: Message, vars: JsonObject, depth: number, keyPath: string): Promise<Flow> {
    if (!this.stepConditionPasses(step, msg, vars)) return cont(msg);

    if (step.call) return this.runCall(step, step.call, msg, vars, keyPath);

    if (step.transform !== undefined) {
      const template = step.transform;
      const input: Json = msg.body ? await msg.body.asJson(this.limits.materializeCap) : null;
      // The exact request bytes, bound only when the template mentions it.
      let rawBody: string | undefined;
      if (msg.body && transform.mentions(template, "_rawBody")) {
        try {
          rawBody = new TextDecoder().decode(await msg.body.materialize(this.limits.materializeCap));
        } catch {
          rawBody = undefined;
        }
      }
      const evalVars: JsonObject = cloneVars(vars);
      evalVars._status = msg.status ?? 200;
      evalVars._ok = msg.isOk();
      if (rawBody !== undefined) evalVars._rawBody = rawBody;
      const headers: JsonObject = {};
      msg.headers.forEach((v, k) => {
        headers[k] = v;
      });
      evalVars._headers = headers;
      const out = await transform.apply(template, input, evalVars);
      if (step.capture !== undefined) {
        // Captured raw — a `$response` envelope is data here.
        vars[step.capture.replace(/^\$+/, "")] = out;
      } else {
        const envelope = detectResponse(out);
        if (envelope) applyResponse(envelope, msg);
        else {
          msg.body = Body.fromJson(out);
          msg.status = 200;
        }
      }
      return cont(msg);
    }

    if (step.pipeline) {
      const flow = await this.runPipeline(step.pipeline, msg, vars, depth + 1, keyPath, undefined);
      // A subpipeline's `end` is local to it.
      return flow.kind === "exit" ? cont(flow.msg) : flow;
    }

    throw RsError.badRequest("step has no action (call/transform/pipeline/split)");
  }

  private async runCall(step: Step, call: CallSpec, msg: Message, vars: JsonObject, keyPath: string): Promise<Flow> {
    if (!isValidMethod(call.method)) throw RsError.badRequest(`invalid method '${call.method}'`);
    // Verbatim, not uppercased: Rust treats `get` as an extension method
    // distinct from GET, and the target service's method match agrees.
    const method = call.method;
    const effect = callEffectClass(call);
    const preserve = step.capture !== undefined;

    // Interpolation context: variables ∪ input-body JSON fields ∪ query.
    const interpCtx = await this.interpolationContext(msg, vars);
    const urlView: UrlView = {
      path: this.urlPath,
      base: this.urlBase,
      name: this.urlName,
      query: this.urlQuery,
      rest: this.urlRest,
    };
    const url = resolvePattern(call.url, urlView, interpCtx);

    // Request body: requests with bodies forward the in-flight body.
    // Cloneable (materialized) bodies feed every retry attempt; a one-shot
    // stream feeds the first attempt only and disables retry.
    let cloneableBody: Body | undefined;
    let oneShotBody: Body | undefined;
    if (method === "GET" || method === "HEAD") {
      /* no body */
    } else if (preserve) {
      await this.materializeIfNeeded(msg);
      cloneableBody = msg.body ? cloneBody(msg.body) : undefined;
    } else {
      const taken = msg.body;
      msg.body = undefined;
      if (taken && taken.isStream()) oneShotBody = taken;
      else if (taken) cloneableBody = cloneBody(taken);
    }

    const needsKey = effect === "keyed" || effect === "unsafe";
    const autoKey = await deriveKey(this.invocationId, keyPath);
    const policy = oneShotBody ? noRetry() : (step.retry ?? this.retry);

    const tenant = msg.tenant;
    const depthForCall = msg.depth + 1;
    const trace = msg.trace;
    // `elevate`: add the mount's operator-configured role to the call's
    // principal, keeping the caller's identity intact; an anonymous caller
    // gets a synthetic principal carrying just the role.
    let principal: Principal | undefined;
    if (step.elevate && this.elevateRole !== undefined) {
      const p: Principal = msg.principal
        ? { id: msg.principal.id, roles: [...msg.principal.roles], kind: msg.principal.kind, extra: { ...msg.principal.extra } }
        : { id: "", roles: [], kind: "agent", extra: {} };
      if (!p.roles.includes(this.elevateRole)) p.roles.push(this.elevateRole);
      principal = p;
    } else {
      principal = msg.principal;
    }

    // Where the call goes: an absolute `http(s)://` URL leaves the node
    // through the mount's `httpOut` grants — allowlist checked once here,
    // before the retry loop. Everything else re-enters internal dispatch.
    const external = isExternalUrl(url);
    let target: CallTarget;
    let targetError: RsError | undefined;
    if (external) {
      if (this.external) {
        try {
          target = { kind: "external", ext: this.external, grant: this.external.authorize(url) };
        } catch (e) {
          targetError = e as RsError;
          target = { kind: "internal", requester: this.requester };
        }
      } else {
        targetError = RsError.capabilityDenied(
          `httpOut to '${urlHost(url) ?? ""}' (this pipeline's mount has no httpOut grants)`,
        );
        target = { kind: "internal", requester: this.requester };
      }
    } else {
      target = { kind: "internal", requester: this.requester };
    }

    // Header values interpolate over the same context as the URL, resolved
    // once here so every attempt sends identical headers.
    let headers: Array<[string, string]> | undefined;
    if (call.headers) {
      headers = [];
      for (const [k, v] of Object.entries(call.headers)) {
        if (!/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(k)) throw RsError.badRequest(`invalid header name '${k}'`);
        const value = resolvePattern(v, urlView, interpCtx);
        if (/[\r\n\0]/.test(value)) throw RsError.badRequest(`header '${k}' resolved to an invalid header value`);
        headers.push([k, value]);
      }
    }

    let response: Message;
    if (targetError) {
      // External gate denials are shaped like a failed call.
      response = msg.errorResponse(targetError);
    } else {
      const finalTarget = target;
      let result: Message;
      try {
        result = await retryRequest(policy, effect, needsKey, async () => {
          // Not `Message.request` — that helper uppercases the method.
          const req = new Message(method, MsgUrl.parse(url), tenant);
          req.source = "internal";
          req.depth = depthForCall;
          req.trace = trace.child();
          // The principal stays inside the node.
          if (finalTarget.kind === "internal") req.principal = principal;
          if (cloneableBody) req.body = cloneBody(cloneableBody);
          else if (oneShotBody) {
            req.body = oneShotBody;
            oneShotBody = undefined;
          }
          if (headers) for (const [name, value] of headers) req.headers.set(name, value);
          // Auto-generated idempotency key (PRD §7.2).
          if (needsKey && req.header("idempotency-key") === undefined) req.setHeader("idempotency-key", autoKey);
          if (finalTarget.kind === "internal") return finalTarget.requester.request(req);
          return finalTarget.ext.send(finalTarget.grant, req);
        });
      } catch (e) {
        // Exhausted transport errors on an external call are shaped like a
        // failed call; anything else propagates.
        if (external) result = msg.errorResponse(e instanceof RsError ? e : RsError.internal(String(e)));
        else throw e;
      }
      response = result;
    }

    const out = response;
    if (step.name !== undefined) out.name = step.name;
    else if (out.name === undefined) out.name = msg.name;

    if (step.capture !== undefined) {
      let value: Json;
      if (!out.isOk()) {
        value = { _errorStatus: out.status ?? 500, _errorMessage: await this.bodyText(out) };
      } else if (out.body && out.body.mediaType.isJson()) {
        value = await out.body.asJson(this.limits.materializeCap);
      } else {
        value = null;
      }
      vars[step.capture.replace(/^\$+/, "")] = value;
      // The in-flight message keeps its prior body and continues.
      return cont(msg);
    }

    if (!out.isOk() && step.tryMode) {
      const errorBody: Json = { _errorStatus: out.status ?? 500, _errorMessage: await this.bodyText(out) };
      out.body = Body.fromJson(errorBody);
      out.status = 200;
    }
    return cont(out);
  }

  /// jsonSplit: array/object body → one message per element; each element
  /// runs the remaining pipeline steps in parallel; results join back.
  private async runSplit(
    spec: PipelineSpec,
    splitIndex: number,
    msg: Message,
    vars: JsonObject,
    depth: number,
    keyPath: string,
  ): Promise<Flow> {
    if (!msg.body) throw RsError.badRequest("jsonSplit requires a JSON body");
    const json = await msg.body.asJson(this.limits.materializeCap);
    let elements: Array<[string, Json]>;
    if (Array.isArray(json)) elements = json.map((v, i) => [String(i), v]);
    else if (json && typeof json === "object") elements = Object.entries(json);
    else elements = [["0", json]];
    if (elements.length > this.limits.maxFanout) {
      throw RsError.limitExceeded("pipeline_fanout", elements.length, this.limits.maxFanout);
    }

    const rest: PipelineSpec = {
      mode: "serial",
      onFail: spec.onFail,
      onSucceed: spec.onSucceed,
      concurrency: spec.concurrency,
      join: undefined,
      steps: spec.steps.slice(splitIndex + 1),
    };
    const concurrency = Math.max(spec.concurrency ?? this.limits.defaultConcurrency, 1);

    const tasks = elements.map(([name, value]) => async (): Promise<[string, Flow]> => {
      const elementMsg = cloneWithBody(msg, Body.fromJson(value));
      elementMsg.name = name;
      if (rest.steps.length === 0) return [name, cont(elementMsg)];
      const flow = await this.runPipeline(rest, elementMsg, cloneVars(vars), depth + 1, `${keyPath}/split[${name}]`, undefined);
      return [name, flow];
    });
    const outcomes = await boundedAll(tasks, concurrency);
    const named: Array<[string, Message]> = outcomes.map(([name, flow]) => [name, flow.msg]);
    named.sort(([a], [b]) => {
      const x = /^\d+$/.test(a) ? Number(a) : undefined;
      const y = /^\d+$/.test(b) ? Number(b) : undefined;
      if (x !== undefined && y !== undefined) return x - y;
      return a < b ? -1 : a > b ? 1 : 0;
    });
    const template = cloneWithBody(msg, undefined);
    return exit(await this.join(spec.join ?? "jsonObject", named, template));
  }

  private async join(joiner: Joiner, named: Array<[string, Message]>, template: Message): Promise<Message> {
    switch (joiner) {
      case "jsonObject": {
        const out: JsonObject = {};
        for (const [name, m] of named) {
          let value: Json;
          if (!m.isOk()) {
            value = { _errorStatus: m.status ?? 500, _errorMessage: await this.bodyText(m) };
          } else if (m.body && m.body.mediaType.isJson()) {
            value = await m.body.asJson(this.limits.materializeCap);
          } else if (m.body) {
            value = new TextDecoder().decode(await m.body.materialize(this.limits.materializeCap));
          } else {
            value = null;
          }
          out[name] = value;
        }
        return template.okJson(out);
      }
    }
  }

  private async bodyText(msg: Message): Promise<string> {
    if (!msg.body) return "";
    try {
      return new TextDecoder().decode(await msg.body.materialize(this.limits.materializeCap));
    } catch {
      return "";
    }
  }

  /// Interpolation context for `${...}`: query params, then input-body JSON
  /// fields, then variables (variables win).
  private async interpolationContext(msg: Message, vars: JsonObject): Promise<JsonObject> {
    const ctx: JsonObject = {};
    for (const pair of msg.url.query.split("&").filter((p) => p !== "")) {
      const eq = pair.indexOf("=");
      ctx[eq < 0 ? pair : pair.slice(0, eq)] = eq < 0 ? "" : pair.slice(eq + 1);
    }
    if (msg.body && msg.body.mediaType.isJson() && !msg.body.isStream()) {
      try {
        const fields = await msg.body.asJson(this.limits.materializeCap);
        if (fields && typeof fields === "object" && !Array.isArray(fields)) {
          for (const [k, v] of Object.entries(fields)) ctx[k] = v;
        }
      } catch {
        /* a non-object or unparseable body contributes nothing */
      }
    }
    for (const [k, v] of Object.entries(vars)) ctx[k] = v;
    return ctx;
  }

  /// Snapshot the in-flight body at a segment boundary. A value means the
  /// segment input is restorable (retryable); `undefined` means the body is
  /// a large/unknown stream and the segment runs at most once.
  private async snapshotBody(msg: Message): Promise<{ body: Body | undefined } | undefined> {
    const body = msg.body;
    if (!body) return { body: undefined };
    if (body.isStream()) {
      // Under the snapshot threshold: force materialization.
      if (body.size !== undefined && body.size <= this.limits.snapshotThreshold) {
        await body.materialize(this.limits.materializeCap);
      } else {
        return undefined;
      }
    }
    const cloned = cloneBody(body);
    return cloned ? { body: cloned } : undefined;
  }

  private checkFanoutBudget(count: number, bodySize: number): void {
    const total = count * bodySize;
    if (total > this.limits.maxFanoutBytes) throw RsError.limitExceeded("pipeline_fanout_bytes", total, this.limits.maxFanoutBytes);
  }

  private async materializeIfNeeded(msg: Message): Promise<void> {
    if (msg.body && msg.body.isStream()) await msg.body.materialize(this.limits.materializeCap);
  }
}

/// Deterministic auto idempotency key (PRD §7.3): hash of the invocation id
/// and the step's location — stable across retries, distinct per step.
export async function deriveKey(invocationId: string, keyPath: string): Promise<string> {
  const enc = new TextEncoder();
  const a = enc.encode(invocationId);
  const b = enc.encode(keyPath);
  const buf = new Uint8Array(a.length + 1 + b.length);
  buf.set(a, 0);
  buf[a.length] = 0;
  buf.set(b, a.length + 1);
  return sha256Hex(buf);
}

function cloneVars(vars: JsonObject): JsonObject {
  return JSON.parse(JSON.stringify(vars)) as JsonObject;
}

/// Clone a materialized body; `undefined` for streams.
export function cloneBody(body: Body): Body | undefined {
  if (body.payload.kind !== "bytes") return undefined;
  const cloned = Body.fromBytes(body.payload.bytes, body.mediaType);
  cloned.lastModified = body.lastModified;
  cloned.provenance = body.provenance;
  return cloned;
}

function tryClone(msg: Message): Message | undefined {
  if (!msg.body) return cloneWithBody(msg, undefined);
  const body = cloneBody(msg.body);
  return body ? cloneWithBody(msg, body) : undefined;
}

export function cloneWithBody(msg: Message, body: Body | undefined): Message {
  const out = new Message(msg.method, msg.url.clone(), msg.tenant);
  out.headers = new Headers(msg.headers);
  out.status = msg.status;
  out.body = body;
  out.principal = msg.principal
    ? { id: msg.principal.id, roles: [...msg.principal.roles], kind: msg.principal.kind, extra: { ...msg.principal.extra } }
    : undefined;
  out.trace = msg.trace.clone();
  out.source = msg.source;
  out.name = msg.name;
  out.depth = msg.depth;
  return out;
}
