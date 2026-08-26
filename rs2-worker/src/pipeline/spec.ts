// Typed pipeline spec (PRD §8.1). The stored format is structured and
// schema-validatable; the terse string DSL is accepted as sugar and
// converted on the way in (`./dsl`). Port of `rs2-core/src/pipeline/spec.rs`
// — `parse` reproduces serde's leniency (unknown keys ignored, wrong types
// rejected) and `toJson` its field order and `skip_serializing_if` rules,
// so a canonical spec stored by either host reads back identically.

import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { validate as validatePattern } from "../runtime/path-pattern";
import { effectForMethod, retryFromConfig } from "../runtime/retry";
import type { EffectClass, RetryPolicy } from "../runtime/retry";
import { parseCondition } from "./condition";
import { validateExpr } from "./transform";

export type Mode = "serial" | "parallel" | "conditional" | "tee" | "teeWait";
export type Action = "stop" | "next" | "end";
export type Joiner = "jsonObject";
export type Splitter = "jsonSplit";

const MODES: Mode[] = ["serial", "parallel", "conditional", "tee", "teeWait"];
const ACTIONS: Action[] = ["stop", "next", "end"];
const EFFECTS: EffectClass[] = ["pure", "idempotent", "keyed", "unsafe"];

/// An internal or external call made by a pipeline step.
export interface CallSpec {
  method: string;
  /// Target URL; `${...}` interpolates variables and input-URL parts.
  url: string;
  effect: EffectClass | undefined;
  /// Extra request headers; values `${...}`-interpolate like the URL.
  headers: Record<string, string> | undefined;
}

/// Declared effect class; defaults from the method (PRD §7.1).
export function callEffectClass(call: CallSpec): EffectClass {
  if (call.effect) return call.effect;
  return isValidMethod(call.method) ? effectForMethod(call.method) : "unsafe";
}

/// One pipeline step. Exactly one of `call` / `transform` / `pipeline` /
/// `split` must be present (`validateSpec`).
export interface Step {
  condition: string | undefined;
  call: CallSpec | undefined;
  transform: Json | undefined;
  pipeline: PipelineSpec | undefined;
  split: Splitter | undefined;
  tryMode: boolean;
  /// Add the pipeline mount's `elevate` role to this call's principal.
  elevate: boolean;
  /// `as: "$name"` captures the step result into a variable.
  capture: string | undefined;
  /// Message name, keying `jsonObject` joins.
  name: string | undefined;
  retry: RetryPolicy | undefined;
}

export interface PipelineSpec {
  mode: Mode;
  onFail: Action | undefined;
  onSucceed: Action | undefined;
  concurrency: number | undefined;
  join: Joiner | undefined;
  steps: Step[];
}

export function emptyStep(): Step {
  return {
    condition: undefined,
    call: undefined,
    transform: undefined,
    pipeline: undefined,
    split: undefined,
    tryMode: false,
    elevate: false,
    capture: undefined,
    name: undefined,
    retry: undefined,
  };
}

export function emptySpec(): PipelineSpec {
  return { mode: "serial", onFail: undefined, onSucceed: undefined, concurrency: undefined, join: undefined, steps: [] };
}

export function callStep(method: string, url: string): Step {
  return { ...emptyStep(), call: { method, url, effect: undefined, headers: undefined } };
}

/// The effect class this step contributes to segment analysis.
export function stepEffectClass(step: Step): EffectClass | undefined {
  return step.call ? callEffectClass(step.call) : undefined;
}

/// Effective on-fail action: configured, else `stop` for parallel, `next`
/// otherwise (Restspace semantics retained).
export function failAction(spec: PipelineSpec): Action {
  return spec.onFail ?? (spec.mode === "parallel" ? "stop" : "next");
}

export function succeedAction(spec: PipelineSpec): Action {
  return spec.onSucceed ?? (spec.mode === "conditional" ? "end" : "next");
}

/// `http::Method::from_bytes`: any non-empty HTTP token.
export function isValidMethod(method: string): boolean {
  return /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(method);
}

/// `HeaderName::try_from`: a non-empty token (case-insensitive).
export function isValidHeaderName(name: string): boolean {
  return /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(name);
}

// ---- parsing (serde semantics) -----------------------------------------

class SpecParseError extends Error {}

function isObject(v: Json | undefined): v is JsonObject {
  return v !== undefined && v !== null && typeof v === "object" && !Array.isArray(v);
}

function enumField<T extends string>(obj: JsonObject, key: string, allowed: T[]): T | undefined {
  const v = obj[key];
  if (v === undefined || v === null) return undefined;
  if (typeof v !== "string" || !allowed.includes(v as T)) {
    throw new SpecParseError(`unknown variant ${JSON.stringify(v)} for '${key}', expected one of ${allowed.join(", ")}`);
  }
  return v as T;
}

function boolField(obj: JsonObject, key: string): boolean {
  const v = obj[key];
  if (v === undefined || v === null) return false;
  if (typeof v !== "boolean") throw new SpecParseError(`invalid type for '${key}': expected a boolean`);
  return v;
}

function stringField(obj: JsonObject, key: string): string | undefined {
  const v = obj[key];
  if (v === undefined || v === null) return undefined;
  if (typeof v !== "string") throw new SpecParseError(`invalid type for '${key}': expected a string`);
  return v;
}

function parseCall(v: Json): CallSpec {
  if (!isObject(v)) throw new SpecParseError("invalid type for 'call': expected an object");
  const method = v.method;
  const url = v.url;
  if (typeof method !== "string") throw new SpecParseError("missing field 'method' in call");
  if (typeof url !== "string") throw new SpecParseError("missing field 'url' in call");
  let headers: Record<string, string> | undefined;
  if (v.headers !== undefined && v.headers !== null) {
    if (!isObject(v.headers)) throw new SpecParseError("invalid type for 'headers': expected an object");
    headers = {};
    for (const [k, hv] of Object.entries(v.headers)) {
      if (typeof hv !== "string") throw new SpecParseError(`invalid type for header '${k}': expected a string`);
      headers[k] = hv;
    }
  }
  return { method, url, effect: enumField(v, "effect", EFFECTS), headers };
}

function parseStep(v: Json): Step {
  if (!isObject(v)) throw new SpecParseError("invalid type for step: expected an object");
  const step = emptyStep();
  step.condition = stringField(v, "if");
  if (v.call !== undefined && v.call !== null) step.call = parseCall(v.call);
  if (v.transform !== undefined) step.transform = v.transform;
  if (v.pipeline !== undefined && v.pipeline !== null) step.pipeline = parseSpecValue(v.pipeline);
  step.split = enumField(v, "split", ["jsonSplit"]);
  step.tryMode = boolField(v, "try");
  step.elevate = boolField(v, "elevate");
  step.capture = stringField(v, "as");
  step.name = stringField(v, "name");
  if (v.retry !== undefined && v.retry !== null) {
    const retry = retryFromConfig(v.retry);
    if (!retry) throw new SpecParseError("invalid 'retry' policy");
    step.retry = retry;
  }
  return step;
}

function parseSpecValue(v: Json): PipelineSpec {
  if (!isObject(v)) throw new SpecParseError("invalid type: expected a pipeline object");
  const spec = emptySpec();
  spec.mode = enumField(v, "mode", MODES) ?? "serial";
  spec.onFail = enumField(v, "onFail", ACTIONS);
  spec.onSucceed = enumField(v, "onSucceed", ACTIONS);
  if (v.concurrency !== undefined && v.concurrency !== null) {
    const c = v.concurrency;
    if (typeof c !== "number" || !Number.isInteger(c) || c < 0) {
      throw new SpecParseError("invalid type for 'concurrency': expected a non-negative integer");
    }
    spec.concurrency = c;
  }
  spec.join = enumField(v, "join", ["jsonObject"]);
  if (v.steps !== undefined && v.steps !== null) {
    if (!Array.isArray(v.steps)) throw new SpecParseError("invalid type for 'steps': expected an array");
    spec.steps = v.steps.map(parseStep);
  }
  return spec;
}

/// Deserialize the typed form (serde `from_value`), without validation.
export function specFromJson(value: Json): PipelineSpec {
  try {
    return parseSpecValue(value);
  } catch (e) {
    if (e instanceof SpecParseError) throw RsError.badRequest(`invalid pipeline spec: ${e.message}`);
    throw e;
  }
}

/// Parse from JSON: the typed form directly, or an array (the Restspace
/// string DSL, PRD §8.1) converted via `dsl.convert`. Validates.
export function specFromValue(value: Json, convertDsl: (v: Json) => PipelineSpec): PipelineSpec {
  const spec = Array.isArray(value) ? convertDsl(value) : specFromJson(value);
  const errors = validateSpec(spec);
  if (errors.length > 0) {
    throw RsError.validationFailed("pipeline spec failed validation", errors);
  }
  return spec;
}

// ---- serialization (serde `Serialize`, field order + skips) -------------

export function retryToJson(p: RetryPolicy): JsonObject {
  return {
    enabled: p.enabled,
    maxAttempts: p.maxAttempts,
    baseDelayMs: p.baseDelayMs,
    maxDelayMs: p.maxDelayMs,
    backoffMultiplier: p.backoffMultiplier,
    jitter: p.jitter,
    retryStatuses: [...p.retryStatuses],
    retryOnNetworkError: p.retryOnNetworkError,
    respectRetryAfter: p.respectRetryAfter,
  };
}

function callToJson(c: CallSpec): JsonObject {
  const out: JsonObject = { method: c.method, url: c.url };
  if (c.effect !== undefined) out.effect = c.effect;
  if (c.headers !== undefined) out.headers = { ...c.headers };
  return out;
}

function stepToJson(s: Step): JsonObject {
  const out: JsonObject = {};
  if (s.condition !== undefined) out.if = s.condition;
  if (s.call !== undefined) out.call = callToJson(s.call);
  if (s.transform !== undefined) out.transform = s.transform;
  if (s.pipeline !== undefined) out.pipeline = specToJson(s.pipeline);
  if (s.split !== undefined) out.split = s.split;
  if (s.tryMode) out.try = true;
  if (s.elevate) out.elevate = true;
  if (s.capture !== undefined) out.as = s.capture;
  if (s.name !== undefined) out.name = s.name;
  if (s.retry !== undefined) out.retry = retryToJson(s.retry);
  return out;
}

export function specToJson(spec: PipelineSpec): JsonObject {
  const out: JsonObject = { mode: spec.mode };
  if (spec.onFail !== undefined) out.onFail = spec.onFail;
  if (spec.onSucceed !== undefined) out.onSucceed = spec.onSucceed;
  if (spec.concurrency !== undefined) out.concurrency = spec.concurrency;
  if (spec.join !== undefined) out.join = spec.join;
  out.steps = spec.steps.map(stepToJson);
  return out;
}

// ---- validation ---------------------------------------------------------

/// Config-time validation: step shape, condition grammar, mode rules,
/// nested pipelines. Returns all problems, not just the first.
export function validateSpec(spec: PipelineSpec): string[] {
  const errors: string[] = [];
  validateInto(spec, "", errors);
  return errors;
}

function validateInto(spec: PipelineSpec, at: string, errors: string[]): void {
  if (spec.steps.length === 0) errors.push(`${at}: pipeline has no steps`);
  if (spec.mode === "parallel" && spec.onFail !== undefined && spec.onFail !== "stop") {
    errors.push(`${at}: parallel mode must stop on fail`);
  }
  if (spec.onSucceed === "stop") {
    errors.push(`${at}: cannot stop on success or the pipeline always fails`);
  }
  spec.steps.forEach((step, i) => {
    const here = `${at}/steps[${i}]`;
    const actions = [step.call, step.transform, step.pipeline, step.split].filter((p) => p !== undefined).length;
    if (actions !== 1) errors.push(`${here}: a step must have exactly one of call/transform/pipeline/split`);
    if (step.condition !== undefined) {
      try {
        parseCondition(step.condition);
      } catch (e) {
        errors.push(`${here}: invalid condition '${step.condition}': ${(e as Error).message}`);
      }
    }
    if (step.capture !== undefined && !step.capture.startsWith("$")) {
      errors.push(`${here}: 'as' must name a variable starting with '$'`);
    }
    if (step.call) {
      if (!isValidMethod(step.call.method)) errors.push(`${here}: invalid method '${step.call.method}'`);
      try {
        validatePattern(step.call.url);
      } catch (e) {
        errors.push(`${here}: ${(e as RsError).detail}`);
      }
      if (step.call.headers) {
        for (const [name, value] of Object.entries(step.call.headers)) {
          if (!isValidHeaderName(name)) errors.push(`${here}: invalid header name '${name}'`);
          try {
            validatePattern(value);
          } catch (e) {
            errors.push(`${here}: in header '${name}': ${(e as RsError).detail}`);
          }
        }
      }
    }
    if (step.transform !== undefined) validateTemplate(step.transform, here, errors);
    if (step.elevate && !step.call) errors.push(`${here}: 'elevate' applies only to call steps`);
    if (step.pipeline) validateInto(step.pipeline, here, errors);
  });
}

/// Walk a transform template with the same shape semantics as the runtime
/// apply (string leaves are JSONata expressions; objects and arrays recurse)
/// and record every leaf that fails to parse.
function validateTemplate(template: Json, at: string, errors: string[]): void {
  if (typeof template === "string") {
    try {
      validateExpr(template);
    } catch (e) {
      errors.push(`${at}: ${(e as RsError).detail}`);
    }
  } else if (Array.isArray(template)) {
    for (const v of template) validateTemplate(v, at, errors);
  } else if (template && typeof template === "object") {
    for (const v of Object.values(template)) validateTemplate(v, at, errors);
  }
}

export function countSteps(spec: PipelineSpec): number {
  return spec.steps.reduce((n, s) => n + 1 + (s.pipeline ? countSteps(s.pipeline) : 0), 0);
}
