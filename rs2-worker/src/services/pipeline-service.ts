// `pipeline` — a pipeline store (PRD §10.3), v1's store-transform pattern.
// Authoring lives under `/<mount>/.pipelines/…` (a `SpecStore` over the
// owned file service, envelopes validated and DSL canonicalized at write
// time; `GET <spec>?$plan` returns the segment plan). Every other path, on
// any verb, executes: the longest stored prefix wins, `.root` governs the
// mount root. Port of `rs2-core/src/services/pipeline_service.rs`.

import { Executor, defaultPipelineLimits } from "../pipeline/executor";
import { convert as convertDsl } from "../pipeline/dsl";
import { plan, planToJson } from "../pipeline/segments";
import { specFromJson, specFromValue, specToJson } from "../pipeline/spec";
import type { PipelineSpec } from "../pipeline/spec";
import { Body } from "../runtime/body";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import type { Message } from "../runtime/message";
import { ExternalDispatch, isExternalUrl, urlHost } from "../runtime/outbound";
import { resolveRetry, retryFromConfig } from "../runtime/retry";
import type { RetryPolicy } from "../runtime/retry";
import { actionFor, checkRoleSpec } from "../runtime/wrapper";
import { PIPELINE_SUBTREE } from "./context";
import type { Service, ServiceContext } from "./context";
import type { SpecStore, SpecValidator } from "./spec-store";

export class PipelineService implements Service {
  private constructor(private readonly store: SpecStore) {}

  static fromConfig(config: JsonObject, store: SpecStore): PipelineService {
    if (config.pipeline !== undefined) {
      throw RsError.badRequest(
        "config-defined pipelines are no longer supported: PUT the spec envelope to /<mount>/.pipelines/<name> (or .pipelines/.root to govern the mount root)",
      );
    }
    return new PipelineService(store);
  }

  /// Write-time validator: envelope `{pipeline, retry?, description?, x-…}`;
  /// the pipeline (typed or string DSL) is converted and validated, and the
  /// typed form is what gets stored.
  static validator(): SpecValidator {
    return (doc: Json): Json => {
      if (!doc || typeof doc !== "object" || Array.isArray(doc)) {
        throw RsError.badRequest("a stored pipeline is a JSON object envelope");
      }
      const specValue = doc.pipeline;
      if (specValue === undefined) throw RsError.badRequest("pipeline envelope requires a 'pipeline'");
      const spec = specFromValue(specValue, convertDsl);
      if (doc.retry !== undefined) {
        if (doc.retry !== null && !retryFromConfig(doc.retry)) {
          throw RsError.badRequest("invalid 'retry' policy: expected a retry policy object");
        }
      }
      if (doc.access !== undefined) validateAccessShape(doc.access);
      const canonical: JsonObject = { ...doc, pipeline: specToJson(spec) };
      return canonical;
    };
  }

  private static specFromDoc(doc: Json): PipelineSpec {
    const value = doc && typeof doc === "object" && !Array.isArray(doc) ? (doc.pipeline ?? null) : null;
    try {
      return specFromJson(value);
    } catch (e) {
      throw RsError.internal(`stored pipeline is corrupt: ${(e as RsError).detail ?? String(e)}`);
    }
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    if (this.store.isAuthoring(msg)) {
      // `GET <spec>?$plan` — segment-plan introspection (PRD §8.3).
      if (msg.method === "GET" && !msg.url.isDirectory() && msg.url.queryParam("$plan") !== undefined) {
        const prefix = `/${PIPELINE_SUBTREE}`;
        const rel = msg.url.servicePath.startsWith(prefix) ? msg.url.servicePath.slice(prefix.length) : "/";
        const doc = await this.store.read(rel === "" ? "/" : rel);
        const spec = PipelineService.specFromDoc(doc);
        const segPlan = plan(spec);
        // Static external-coverage check: literal absolute call URLs whose
        // host no `httpOut` grant covers would be denied at execution.
        const external = ExternalDispatch.fromMount(ctx.config, ctx.http, ctx.outboundInjectors, ctx.limits.materializedBodyBytes);
        appendExternalWarnings(spec, "", external, segPlan.warnings);
        return msg.okJson({ pipeline: specToJson(spec), plan: planToJson(segPlan) });
      }
      return this.store.handleAuthoring(msg);
    }

    // ---- execution: any verb, longest stored prefix, .root fallback ----
    const segments = msg.url.serviceSegments();
    const resolved = await this.store.resolve(segments);
    if (!resolved) {
      throw RsError.notFound(
        `no stored pipeline matches '${msg.url.servicePath}' (author one at ${msg.url.basePath}${PIPELINE_SUBTREE}/…)`,
      );
    }
    const [doc, matchedLen] = resolved;
    const spec = PipelineService.specFromDoc(doc);

    // The peeled sub-path is the URL plane for `${url.path[…]}`.
    const peeled = segments.slice(matchedLen);
    const baseSegs = msg.url.baseSegments();
    const urlName = peeled.length ? peeled[peeled.length - 1] : undefined;
    const urlQuery = msg.url.query;

    // Per-spec authorization: the matched spec's `access` overrides the
    // mount's floor per key. Fail closed when neither declares one.
    const docObj = doc && typeof doc === "object" && !Array.isArray(doc) ? doc : {};
    const access = effectiveAccess(ctx.config.access, docObj.access);
    if (access !== undefined) {
      checkRoleSpec(access, actionFor(msg.method), msg);
    } else if (msg.source === "system") {
      /* a runtime-originated system call (a scheduler tick) is trusted */
    } else if (msg.principal) {
      throw RsError.forbidden("this pipeline has no access policy configured");
    } else {
      throw RsError.unauthorized("this pipeline has no access policy configured");
    }

    const envelopeRetry = docObj.retry !== undefined ? retryFromConfig(docObj.retry) : undefined;
    const rest = rebuildRest(peeled, msg.url.isDirectory());
    return runInline(spec, msg, ctx, { peeled, baseSegs, urlName, urlQuery, rest, envelopeRetry });
  }
}

/// The effective per-spec access: the spec's `access` overlaid on the
/// mount's floor. Both role objects ⇒ the spec wins per key; otherwise the
/// most-specific present value wins wholesale. `undefined` ⇒ open.
function effectiveAccess(mount: Json | undefined, spec: Json | undefined): Json | undefined {
  if (mount === undefined && spec === undefined) return undefined;
  if (spec === undefined) return mount;
  if (mount === undefined) return spec;
  if (mount && typeof mount === "object" && !Array.isArray(mount) && spec && typeof spec === "object" && !Array.isArray(spec)) {
    return { ...mount, ...spec };
  }
  return spec;
}

/// Validate the shape of a spec envelope's `access` field.
function validateAccessShape(access: Json): void {
  if (typeof access === "string") {
    if (access === "open" || access === "authenticated") return;
    throw RsError.badRequest(`unknown access policy '${access}' (expected "open" or "authenticated")`);
  }
  if (access && typeof access === "object" && !Array.isArray(access)) {
    for (const [key, val] of Object.entries(access)) {
      if (!["read", "write", "delete", "invoke"].includes(key)) {
        throw RsError.badRequest(`unknown access key '${key}' (allowed: read, write, delete, invoke)`);
      }
      if (typeof val !== "string") throw RsError.badRequest(`access '${key}' must be a role-spec string`);
    }
    return;
  }
  throw RsError.badRequest("'access' must be a string or role object");
}

/// URL-plane inputs for `runInline` — the incoming request's path, peeled
/// to the plane a pipeline addresses, plus the verbatim `rest`.
export interface ExecInputs {
  peeled: string[];
  baseSegs: string[];
  urlName: string | undefined;
  urlQuery: string;
  rest: string;
  envelopeRetry: RetryPolicy | undefined;
}

/// Build the executor with the request's URL plane, run `spec` under the
/// wall-clock budget, and shape a failure into a structured problem with
/// per-step statuses (PRD §12). Shared by `pipeline` and `wrapper`.
export async function runInline(spec: PipelineSpec, msg: Message, ctx: ServiceContext, inputs: ExecInputs): Promise<Message> {
  const requester = ctx.requester;
  if (!requester) throw RsError.internal("pipeline service has no requester capability");
  const toStepRaw = msg.url.queryParam("$to-step");
  const toStep = toStepRaw !== undefined && /^\d+$/.test(toStepRaw) ? Number(toStepRaw) : undefined;

  const limits = { ...defaultPipelineLimits(), wallClockMs: ctx.pipelineWallClockMs, materializeCap: ctx.limits.materializedBodyBytes };
  // Retry resolution: envelope → mount config → tenant default.
  const mountRetry = retryFromConfig(ctx.config.retry ?? null);
  const retry = resolveRetry([inputs.envelopeRetry, mountRetry, ctx.tenantRetry]);
  const elevate = ctx.config.elevate;
  const executor = new Executor(requester, limits, retry)
    .withElevateRole(typeof elevate === "string" ? elevate : undefined)
    .withExternal(ExternalDispatch.fromMount(ctx.config, ctx.http, ctx.outboundInjectors, ctx.limits.materializedBodyBytes))
    .withUrl(inputs.peeled, inputs.baseSegs, inputs.urlName, inputs.urlQuery, inputs.rest);
  if (ctx.secrets) executor.withVars({ ...ctx.secrets });

  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(RsError.limitExceeded("pipeline_wall_clock_ms", limits.wallClockMs, limits.wallClockMs)),
      limits.wallClockMs,
    );
  });
  let resp: Message;
  try {
    resp = await Promise.race([executor.run(spec, msg, toStep), timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }

  // Pipeline failures include the failing step and per-step statuses.
  if (!resp.isOk() && resp.body && resp.body.mediaType.isJson()) {
    const steps = executor.report();
    const failed = [...steps].reverse().find((s) => typeof s.status === "number" && s.status >= 400);
    let problem: Json | undefined;
    try {
      problem = await resp.body.asJson(ctx.limits.materializedBodyBytes);
    } catch {
      problem = undefined;
    }
    if (problem && typeof problem === "object" && !Array.isArray(problem)) {
      problem.pipeline = { failedStep: failed?.step ?? null, steps };
      const mediaType = resp.body.mediaType;
      resp.body = Body.fromString(JSON.stringify(problem), new MediaType(mediaType.essence(), mediaType.schema()));
    }
  }
  return resp;
}

/// `?$plan` static coverage: warn on literal external call URLs whose host
/// no `httpOut` grant on the mount covers.
function appendExternalWarnings(spec: PipelineSpec, at: string, external: ExternalDispatch | undefined, warnings: string[]): void {
  spec.steps.forEach((step, i) => {
    if (step.call && isExternalUrl(step.call.url)) {
      const host = urlHost(step.call.url);
      if (host !== undefined && !host.includes("${") && !(external?.covers(host) ?? false)) {
        warnings.push(
          `${at}/steps[${i}]: external host '${host}' is not covered by any httpOut grant on this mount — the call would be denied`,
        );
      }
    }
    if (step.pipeline) appendExternalWarnings(step.pipeline, `${at}/steps[${i}]/pipeline`, external, warnings);
  });
}

/// Rebuild the `${url.rest}` value from peeled segments: a leading slash,
/// the segments `/`-joined, and a trailing slash for a directory request.
export function rebuildRest(peeled: string[], isDirectory: boolean): string {
  if (peeled.length === 0) return "/";
  return `/${peeled.join("/")}${isDirectory ? "/" : ""}`;
}
