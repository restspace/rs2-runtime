// The Dynamic Worker engine (cloudflare.md §E): runs deployed JS bundles as
// sandboxed Dynamic Workers under the engine-neutral contract — the port of
// `rs2-core/src/engines/js.rs` over `env.LOADER`. One worker per
// mount + bundle (`<tenant>:<mount base>:<name>@<version>`); one invocation =
// one `Rs2Guest.invoke(msg, config, invocationId)` RPC. Guest capability
// calls come back into the DO through the `HostApi`/`Egress` entrypoints
// (`egress.ts`) keyed by the invocation id; the DO-side handlers live here
// so `tenant-object.ts` only delegates.

import { Body, EPHEMERAL, utf8Decode } from "../runtime/body";
import { RsError, codes, toRsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import { Message } from "../runtime/message";
import type { Principal } from "../runtime/message";
import { GrantedHost } from "./host-api";
import type { CapabilityTarget, GuestStateKv, LogContext } from "./host-api";
import { GUEST_GLOBALS, GUEST_SHIM } from "./guest-shim.bundled";

/// The guests' platform snapshot; bump deliberately, never per deploy.
const GUEST_COMPAT_DATE = "2026-08-22";

/// `nodejs_als` enables only `node:async_hooks` (the shim's per-invocation
/// attribution, `guest-globals.js`) — not the full `nodejs_compat` surface,
/// which would change which globals the platform hands the bundle.
const GUEST_COMPAT_FLAGS = ["nodejs_als"];

/// The loader `modules` for one deployed bundle: the shim (main), its
/// globals module, and the bundle as `bundle.js`.
export function guestModules(source: string): Record<string, string | WorkerLoaderModule> {
  return { "shim.js": GUEST_SHIM, "globals.js": GUEST_GLOBALS, "bundle.js": { js: source } };
}

/// The loader code descriptor every guest worker shares, minus the
/// per-worker bindings/limits.
export function guestCodeBase(source: string): Pick<
  WorkerLoaderWorkerCode,
  "compatibilityDate" | "compatibilityFlags" | "mainModule" | "modules" | "tails"
> {
  return {
    compatibilityDate: GUEST_COMPAT_DATE,
    compatibilityFlags: GUEST_COMPAT_FLAGS,
    mainModule: "shim.js",
    modules: guestModules(source),
    tails: [],
  };
}

/// The platform's fixed per-isolate heap (reported, not configurable).
export const GUEST_MEMORY_BYTES = 128 * 1024 * 1024;

/// Worker-only mount knob `limits.cpuMs` (default 5 000, ceiling 30 000).
export function cpuMsFromConfig(config: JsonObject): number {
  const limits = config.limits;
  const raw =
    limits && typeof limits === "object" && !Array.isArray(limits) && typeof limits.cpuMs === "number"
      ? limits.cpuMs
      : undefined;
  if (raw === undefined || !Number.isFinite(raw) || raw <= 0) return 5_000;
  return Math.min(Math.floor(raw), 30_000);
}

/// Match a `host:port` grant pattern: exact, host-only (any port), or a
/// `*.suffix[:port]` host wildcard (matching the apex too). Port of
/// `js.rs::socket_pattern_matches`.
export function socketPatternMatches(pat: string, host: string, port: number, target: string): boolean {
  if (pat === target) return true;
  let patHost = pat;
  let patPort: number | undefined;
  const colon = pat.lastIndexOf(":");
  if (colon >= 0) {
    const p = pat.slice(colon + 1);
    if (/^\d+$/.test(p) && Number(p) <= 65535) {
      patHost = pat.slice(0, colon);
      patPort = Number(p);
    }
  }
  if (patPort !== undefined && patPort !== port) return false;
  if (patHost === "*" || patHost === host) return true;
  if (patHost.startsWith("*.")) {
    const suffix = patHost.slice(2);
    return host === suffix || host.endsWith(`.${suffix}`);
  }
  return false;
}

/// Collect the `socket` grant allowlist (`host:port` patterns) from a mount
/// config's `grants`. Port of `js.rs::socket_allowlist_from_config`.
export function socketAllowlistFromConfig(config: JsonObject): string[] {
  const out: string[] = [];
  const grants = config.grants;
  if (grants && typeof grants === "object" && !Array.isArray(grants)) {
    for (const grant of Object.values(grants)) {
      if (!grant || typeof grant !== "object" || Array.isArray(grant)) continue;
      if (grant.type !== "socket") continue;
      const hosts = grant.hosts;
      if (Array.isArray(hosts)) for (const h of hosts) if (typeof h === "string") out.push(h);
    }
  }
  return out;
}

/// A structured host error rendered as the marker the shim rethrows.
export function errorMarker(e: RsError): JsonObject {
  return { __rs2_error: true, code: e.code, status: e.status, message: e.detail };
}

/// The destination for a streamed response body (`js.rs::StreamSink`).
interface ResponseSink {
  /// Resolves the engine's race the first time the guest begins.
  begin: ((envelope: Json) => void) | undefined;
  writer: WritableStreamDefaultWriter<Uint8Array>;
  began: boolean;
  sent: number;
  cap: number;
}

/// Per-invocation state, held in the DO's `invocations` map for the call's
/// lifetime (`js.rs::InvocationState` + the spec §E.3 table).
export interface InvocationRecord {
  host: GrantedHost;
  tenant: string;
  depth: number;
  principal: Principal | undefined;
  materializeCap: number;
  /// A structured host error recorded before its marker crosses the RPC
  /// boundary, so an *uncaught* one keeps its identity out of the engine
  /// (contract invariant 2).
  hostError: RsError | undefined;
  socketAllowlist: string[];
  /// The request body as a pull stream (request streaming only).
  bodyReader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  streamedIn: number;
  sink: ResponseSink | undefined;
}

export type Invocations = Map<string, InvocationRecord>;

/// Build an internal `Message` from a guest request envelope
/// `{ method?, url, headers?, body?, mediaType? }`. The call carries the
/// invoking principal (a `code:` service runs **as its caller**). Port of
/// `js.rs::message_from_request`.
export function messageFromRequest(
  req: JsonObject,
  tenant: string,
  depth: number,
  principal: Principal | undefined,
): Message {
  const method = typeof req.method === "string" && /^[!#$%&'*+.^_`|~0-9a-zA-Z-]+$/.test(req.method) ? req.method : "GET";
  const url = typeof req.url === "string" ? req.url : "/";
  const call = Message.request(method, url, tenant);
  call.source = "internal";
  call.principal = principal;
  call.depth = Math.min(depth + 1, 0xffff);
  const headers = req.headers;
  if (headers && typeof headers === "object" && !Array.isArray(headers)) {
    for (const [k, v] of Object.entries(headers)) {
      if (typeof v === "string") call.setHeader(k, v);
    }
  }
  const body = req.body;
  if (body === undefined || body === null) {
    // no body
  } else if (typeof body === "string") {
    const mt = typeof req.mediaType === "string" ? MediaType.parse(req.mediaType) : new MediaType("text/plain");
    call.body = Body.fromString(body, mt);
  } else {
    call.body = Body.fromJson(body);
  }
  return call;
}

/// Run a host request to completion, materializing the response body, and
/// return the guest-facing envelope. Port of `js.rs::run_host_request`.
async function runHostRequest(
  host: GrantedHost,
  capability: string,
  call: Message,
  materializeCap: number,
): Promise<JsonObject> {
  const resp = await host.request(capability, call);
  const status = resp.status ?? 200;
  let mediaType: Json = null;
  let payload: Json = null;
  if (resp.body) {
    mediaType = resp.body.mediaType.toString();
    const isJson = resp.body.mediaType.isJson();
    const bytes = await resp.body.materialize(materializeCap);
    const text = utf8Decode(bytes);
    if (isJson) {
      try {
        payload = JSON.parse(text) as Json;
      } catch {
        payload = text;
      }
    } else {
      payload = text;
    }
  }
  const headers: JsonObject = {};
  resp.headers.forEach((v, k) => {
    headers[k] = v;
  });
  return { status, headers, body: payload, mediaType };
}

// ---- DO-side guest RPC handlers (the `env.RS2` op table, §E.3) ------------

function failGuest(record: InvocationRecord, e: RsError): JsonObject {
  record.hostError = e;
  return errorMarker(e);
}

/// `op_rs2_request`: grant lookup (default deny), budget debit, child
/// trace/depth, response materialized into the guest envelope.
export async function guestRequestOp(
  invocations: Invocations,
  id: string,
  capability: string,
  req: Json,
): Promise<Json> {
  const record = invocations.get(id);
  if (!record) return errorMarker(RsError.internal(`unknown invocation '${id}'`));
  const reqObj = req && typeof req === "object" && !Array.isArray(req) ? req : {};
  try {
    const call = messageFromRequest(reqObj, record.tenant, record.depth, record.principal ? { ...record.principal } : undefined);
    return await runHostRequest(record.host, String(capability), call, record.materializeCap);
  } catch (e) {
    return failGuest(record, toRsError(e));
  }
}

export function guestLogOp(invocations: Invocations, id: string, level: string, text: string): void {
  const record = invocations.get(id);
  if (!record) return;
  record.host.log(String(level), String(text));
}

export async function guestStateGetOp(invocations: Invocations, id: string, key: string): Promise<Json> {
  const record = invocations.get(id);
  if (!record) return null;
  const value = await record.host.stateGet(String(key));
  return value ?? null;
}

export async function guestStatePutOp(invocations: Invocations, id: string, key: string, value: string): Promise<Json> {
  const record = invocations.get(id);
  if (!record) return null;
  await record.host.statePut(String(key), String(value));
  return null;
}

/// `op_rs2_body_read`: next chunk of the streamed request body (or null at
/// EOF / when streaming was never enabled); cumulative bytes bounded by the
/// materialization cap.
export async function guestBodyReadOp(
  invocations: Invocations,
  id: string,
): Promise<{ data: Uint8Array } | JsonObject | null> {
  const record = invocations.get(id);
  if (!record) return null;
  const reader = record.bodyReader;
  if (!reader) return null; // no streaming body (or never opted in)
  try {
    const { done, value } = await reader.read();
    if (done || !value) return null; // EOF
    record.streamedIn += value.byteLength;
    if (record.streamedIn > record.materializeCap) {
      return failGuest(
        record,
        RsError.limitExceeded("materialized_body_bytes", record.streamedIn, record.materializeCap),
      );
    }
    return { data: value };
  } catch (e) {
    return failGuest(
      record,
      new RsError(502, codes.CONTRACT_VIOLATION, "Bad Gateway", `request body stream error: ${toRsError(e).detail}`),
    );
  }
}

/// `op_rs2_stream_begin`: hand the status/headers envelope to the host
/// (which resolves the client response) and arm the body writer.
export function guestStreamBeginOp(invocations: Invocations, id: string, envelope: Json): Json {
  const record = invocations.get(id);
  if (!record) return errorMarker(RsError.internal(`unknown invocation '${id}'`));
  const sink = record.sink;
  if (!sink) {
    return failGuest(record, RsError.internal("response streaming is not enabled for this mount"));
  }
  const begin = sink.begin;
  if (!begin) {
    return failGuest(record, RsError.contractViolation("beginStream called more than once"));
  }
  sink.begin = undefined;
  sink.began = true;
  begin(envelope ?? {});
  return null;
}

/// `op_rs2_body_write`: one chunk of the streamed response body. The await
/// on the writer is the backpressure. Bounded cumulatively by the cap.
export async function guestBodyWriteOp(invocations: Invocations, id: string, data: Uint8Array): Promise<Json> {
  const record = invocations.get(id);
  if (!record) return errorMarker(RsError.internal(`unknown invocation '${id}'`));
  const sink = record.sink;
  if (!sink) {
    return failGuest(record, RsError.internal("response streaming is not enabled for this mount"));
  }
  if (!sink.began) {
    return failGuest(record, RsError.contractViolation("body write before beginStream"));
  }
  sink.sent += data.byteLength;
  if (sink.sent > sink.cap) {
    return failGuest(record, RsError.limitExceeded("materialized_body_bytes", sink.sent, sink.cap));
  }
  try {
    await sink.writer.write(data);
    return null;
  } catch {
    return failGuest(
      record,
      new RsError(502, codes.CONTRACT_VIOLATION, "Bad Gateway", "response stream consumer is gone"),
    );
  }
}

/// The `socket` grant allowlist check (spec §E.3: sockets stay in the
/// guest; the host only gates the connect).
export function guestSocketCheckOp(invocations: Invocations, id: string, host: string, port: number): Json {
  const record = invocations.get(id);
  if (!record) return errorMarker(RsError.internal(`unknown invocation '${id}'`));
  const target = `${host}:${port}`;
  const allowed = record.socketAllowlist.some((pat) => socketPatternMatches(pat, host, port, target));
  if (!allowed) {
    return failGuest(record, RsError.capabilityDenied(`socket ${host}:${port}`));
  }
  return null;
}

/// The gateway fetch (`run_host_fetch` + §E.4): grant name fixed to
/// `"fetch"`; the serialized request/response keep bodies as bytes.
export interface SerializedRequest {
  method: string;
  url: string;
  headers: [string, string][];
  body: Uint8Array | null;
}

export interface SerializedResponse {
  status: number;
  headers: [string, string][];
  body: Uint8Array | null;
  /// Marks a structured host error travelling as a response (the shim's
  /// fetch wrapper rethrows it with `.code`/`.status`).
  guestError: boolean;
}

export async function guestFetchOp(
  invocations: Invocations,
  id: string | null,
  req: SerializedRequest,
): Promise<SerializedResponse> {
  const markerResponse = (e: RsError): SerializedResponse => ({
    status: e.status,
    headers: [["content-type", "application/json"]],
    body: new TextEncoder().encode(JSON.stringify(errorMarker(e))),
    guestError: true,
  });
  const record = id !== null ? invocations.get(id) : undefined;
  if (!record) {
    // A fetch the shim did not stamp (a bundle that captured the platform
    // fetch at module scope) cannot be attributed to an invocation; deny
    // rather than allow ungated egress.
    return markerResponse(RsError.capabilityDenied("fetch"));
  }
  try {
    const call = Message.request(req.method, req.url, record.tenant);
    call.source = "internal";
    call.principal = record.principal ? { ...record.principal } : undefined;
    call.depth = Math.min(record.depth + 1, 0xffff);
    for (const [k, v] of req.headers) call.setHeader(k, v);
    if (req.body && req.body.byteLength > 0) {
      const ct = call.header("content-type");
      call.body = Body.fromBytes(req.body, ct !== undefined ? MediaType.parse(ct) : MediaType.octetStream());
    }
    const resp = await record.host.request("fetch", call);
    const headers: [string, string][] = [];
    resp.headers.forEach((v, k) => headers.push([k, v]));
    // SDKs gate JSON parsing on content-type: surface the body's media type
    // when the response didn't carry one.
    if (resp.body && !headers.some(([k]) => k.toLowerCase() === "content-type")) {
      headers.push(["content-type", resp.body.mediaType.toString()]);
    }
    const bytes = resp.body ? await resp.body.materialize(record.materializeCap) : null;
    return { status: resp.status ?? 200, headers, body: bytes, guestError: false };
  } catch (e) {
    const err = toRsError(e);
    record.hostError = err;
    return markerResponse(err);
  }
}

// ---- envelope mapping -----------------------------------------------------

/// Map a guest response envelope (`{ status?, headers?, body?, mediaType? }`)
/// to a `Message`. `fallbackBody` is the passthrough request body or a
/// streamed response body. Port of `js.rs::envelope_to_message`.
export function envelopeToMessage(template: Message, outcome: Json, fallbackBody: Body | undefined): Message {
  const env = outcome && typeof outcome === "object" && !Array.isArray(outcome) ? outcome : {};
  const rawStatus = typeof env.status === "number" ? Math.floor(env.status) : undefined;
  const status = rawStatus !== undefined && rawStatus >= 100 && rawStatus <= 999 ? rawStatus : 200;
  const mediaType = typeof env.mediaType === "string" ? MediaType.parse(env.mediaType) : undefined;
  let body: Body | undefined;
  const envBody = env.body;
  if (envBody === undefined || envBody === null) {
    body = fallbackBody;
  } else if (typeof envBody === "string") {
    body = Body.fromString(envBody, mediaType ?? new MediaType("text/plain"));
  } else {
    body = Body.fromJson(envBody);
    if (mediaType) body.mediaType = mediaType;
  }
  const resp = template.response(status, body);
  const headers = env.headers;
  if (headers && typeof headers === "object" && !Array.isArray(headers)) {
    for (const [k, v] of Object.entries(headers)) {
      if (typeof v === "string") resp.setHeader(k, v);
    }
  }
  return resp;
}

// ---- the engine -----------------------------------------------------------

/// What the `TenantObject` wires into the engine: the loader binding, the
/// per-tenant `HostApi`/`Egress` entrypoint stubs (via `ctx.exports`), the
/// shared invocations map, and the KV accessor backing guest state.
export interface EngineHost {
  loader: WorkerLoader;
  invocations: Invocations;
  hostApiStub(tenant: string): Fetcher;
  egressStub(tenant: string): Fetcher;
  stateKv: GuestStateKv;
}

/// Everything one invocation needs beyond the message: the identity, the
/// grants, and the loader limits.
export interface InvokeArgs {
  /// Worker id: `<tenant>:<mount base>:<name>@<version>` — content-addressed
  /// AND mount-addressed, so two mounts of one bundle with different grants
  /// never share an isolate (module-scope state, captured closures).
  codeId: string;
  source: string;
  msg: Message;
  config: JsonObject;
  grants: Map<string, CapabilityTarget>;
  /// `<name>@<version>` — the guest state/log identity.
  serviceRef: string;
  logCtx: LogContext;
  outboundBudget: number;
  materializeCap: number;
  /// The engine wall-clock backstop (ms) and the loader CPU budget.
  wallClockMs: number;
  cpuMs: number;
}

interface RaceOutcome {
  kind: "done" | "failed" | "began" | "timeout";
  value?: Json;
  error?: unknown;
  envelope?: Json;
}

export class DynamicWorkerEngine {
  constructor(private readonly hostEnv: EngineHost) {}

  stateKv(): GuestStateKv {
    return this.hostEnv.stateKv;
  }

  /// Compile-only validation: the deployment-time smoke test for
  /// `POST/PUT /code/<name>` with a JS bundle. Loads the bundle into a
  /// throwaway Dynamic Worker (`check:<hash>`) with no bindings and no
  /// egress; module evaluation errors → 502 `contract_violation`.
  async compileCheck(source: string, hash: string): Promise<void> {
    try {
      const worker = this.hostEnv.loader.get(`check:${hash}`, () => ({
        ...guestCodeBase(source),
        env: {},
        globalOutbound: null,
        limits: { cpuMs: 10_000 },
      }));
      const ep = worker.getEntrypoint("Rs2Guest") as unknown as { check(): Promise<boolean> };
      await ep.check();
    } catch (e) {
      const detail = e instanceof Error ? e.message : String(e);
      throw RsError.contractViolation(`JS bundle failed to compile: ${detail}`);
    }
  }

  /// One invocation of a deployed bundle (`JsEngine::invoke`). Body modes
  /// (`bodyPassthrough` / `requestStreaming` / `responseStreaming`) follow
  /// `js.rs` exactly; limit breaches map to the same `limit_exceeded` names.
  async invoke(args: InvokeArgs): Promise<Message> {
    const { msg, config } = args;
    const template = msg.response(200, undefined);
    const passthrough = config.bodyPassthrough === true;
    const requestStreaming = config.requestStreaming === true;
    const responseStreaming = config.responseStreaming === true;

    let carriedBody: Body | undefined;
    let bodyReader: ReadableStreamDefaultReader<Uint8Array> | undefined;

    // Bodies materialize at the engine boundary, capped (invariant 1) —
    // except under streaming/passthrough, where they cross by reference.
    let payload: Json = null;
    let mediaType: Json = null;
    let bodySize: Json = null;
    if (requestStreaming) {
      if (msg.body) {
        const body = msg.body;
        msg.body = undefined;
        mediaType = body.mediaType.toString();
        bodySize = body.size ?? null;
        bodyReader = body.intoStream().getReader();
      }
    } else if (passthrough) {
      if (msg.body) {
        carriedBody = msg.body;
        msg.body = undefined;
        mediaType = carriedBody.mediaType.toString();
        bodySize = carriedBody.size ?? null;
      }
    } else if (msg.body) {
      mediaType = msg.body.mediaType.toString();
      const isJson = msg.body.mediaType.isJson();
      const bytes = await msg.body.materialize(args.materializeCap);
      const text = utf8Decode(bytes);
      if (isJson) {
        try {
          payload = JSON.parse(text) as Json;
        } catch {
          payload = text;
        }
      } else {
        payload = text;
      }
    }

    const headers: JsonObject = {};
    msg.headers.forEach((v, k) => {
      headers[k] = v;
    });
    const url = msg.url.query === "" ? msg.url.path : `${msg.url.path}?${msg.url.query}`;
    const input: JsonObject = {
      method: msg.method,
      url,
      headers,
      body: payload,
      mediaType,
      bodyPassthrough: passthrough,
      requestStreaming,
      responseStreaming,
      bodySize,
    };

    const host = new GrantedHost(args.grants, args.outboundBudget, this.hostEnv.stateKv, args.serviceRef).withLogContext(
      args.logCtx,
    );
    const invocationId = crypto.randomUUID().replace(/-/g, "");
    const record: InvocationRecord = {
      host,
      tenant: msg.tenant,
      depth: msg.depth,
      principal: msg.principal ? { ...msg.principal } : undefined,
      materializeCap: args.materializeCap,
      hostError: undefined,
      socketAllowlist: socketAllowlistFromConfig(config),
      bodyReader,
      streamedIn: 0,
      sink: undefined,
    };

    let streamedReadable: ReadableStream<Uint8Array> | undefined;
    let beganResolve: ((envelope: Json) => void) | undefined;
    let beganPromise: Promise<RaceOutcome> | undefined;
    if (responseStreaming) {
      const ts = new TransformStream<Uint8Array, Uint8Array>();
      streamedReadable = ts.readable;
      beganPromise = new Promise<RaceOutcome>((resolve) => {
        beganResolve = (envelope) => resolve({ kind: "began", envelope });
      });
      record.sink = {
        begin: beganResolve,
        writer: ts.writable.getWriter(),
        began: false,
        sent: 0,
        cap: args.materializeCap,
      };
    }

    this.hostEnv.invocations.set(invocationId, record);
    const invocations = this.hostEnv.invocations;
    const tenant = msg.tenant;

    const worker = this.hostEnv.loader.get(args.codeId, () => ({
      ...guestCodeBase(args.source),
      env: { RS2: this.hostEnv.hostApiStub(tenant) },
      globalOutbound: this.hostEnv.egressStub(tenant),
      limits: { cpuMs: args.cpuMs, subRequests: args.outboundBudget },
    }));
    const ep = worker.getEntrypoint("Rs2Guest") as unknown as {
      invoke(msg: Json, config: Json, invocationId: string): Promise<Json>;
    };

    const invokePromise = ep.invoke(input, config, invocationId);
    const settled: Promise<RaceOutcome> = invokePromise.then(
      (value) => ({ kind: "done", value }),
      (error) => ({ kind: "failed", error }),
    );
    // The invocation record lives as long as the guest may still call back
    // (a streamed response outlives the race below).
    void settled.finally(() => {
      invocations.delete(invocationId);
      if (record.sink) {
        record.sink.writer.close().catch(() => undefined);
      }
      if (record.bodyReader) record.bodyReader.cancel().catch(() => undefined);
    });

    // The engine wall clock is a backstop (the dispatch path races the same
    // budget); a CPU-limit kill from the loader arrives as a rejection.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<RaceOutcome>((resolve) => {
      timer = setTimeout(() => resolve({ kind: "timeout" }), args.wallClockMs);
    });

    try {
      const races: Promise<RaceOutcome>[] = [settled, timeout];
      if (beganPromise) races.push(beganPromise);
      const outcome = await Promise.race(races);
      if (outcome.kind === "done" && isGuestThrow(outcome.value)) {
        throw this.mapFailure(new Error(guestThrowMessage(outcome.value)), record, args);
      }
      switch (outcome.kind) {
        case "began": {
          // The guest began streaming; return the response over the stream
          // and leave the handler running, feeding it under backpressure.
          const env =
            outcome.envelope && typeof outcome.envelope === "object" && !Array.isArray(outcome.envelope)
              ? outcome.envelope
              : {};
          const mt = typeof env.mediaType === "string" ? MediaType.parse(env.mediaType) : MediaType.octetStream();
          const body = Body.fromStream(streamedReadable!, mt, undefined, EPHEMERAL);
          return envelopeToMessage(template, outcome.envelope ?? {}, body);
        }
        case "done":
          return envelopeToMessage(template, outcome.value ?? {}, carriedBody);
        case "failed":
          throw this.mapFailure(outcome.error, record, args);
        case "timeout":
          throw RsError.limitExceeded("wall_clock_ms", args.wallClockMs, args.wallClockMs);
      }
      throw RsError.internal("unreachable invoke outcome");
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  /// Resident-envelope invocation (`template`, cloudflare.md §D): a locked
  /// sandbox — no bindings, no egress, `limits: {cpuMs}` — dispatched with
  /// `{method, url, body, mediaType}` like `ResidentAdapter::call`.
  async invokeResident(
    codeId: string,
    source: string,
    req: JsonObject,
    config: JsonObject,
    cpuMs: number,
    wallClockMs: number,
  ): Promise<{ status: number; body: Json }> {
    const worker = this.hostEnv.loader.get(codeId, () => ({
      ...guestCodeBase(source),
      env: {},
      globalOutbound: null,
      limits: { cpuMs },
    }));
    const ep = worker.getEntrypoint("Rs2Guest") as unknown as {
      invoke(msg: Json, config: Json, invocationId: string): Promise<Json>;
    };
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(RsError.limitExceeded("wall_clock_ms", wallClockMs, wallClockMs)), wallClockMs);
    });
    let envelope: Json;
    try {
      envelope = await Promise.race([ep.invoke(req, config, "resident"), timeout]);
    } catch (e) {
      if (e instanceof RsError) throw e;
      throw this.mapPlatformKill(e, cpuMs) ?? RsError.contractViolation(`JS service failed: ${describe(e)}`);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
    if (isGuestThrow(envelope)) {
      throw this.mapPlatformKill(new Error(guestThrowMessage(envelope)), cpuMs) ??
        RsError.contractViolation(`JS service failed: ${guestThrowMessage(envelope)}`);
    }
    const env = envelope && typeof envelope === "object" && !Array.isArray(envelope) ? envelope : {};
    const status = typeof env.status === "number" ? Math.floor(env.status) : 200;
    return { status, body: env.body ?? null };
  }

  /// Map an invocation failure to its `RsError` identity: a recorded host
  /// error first (invariant 2), then platform kills (CPU → `wall_clock_ms`,
  /// OOM → `memory_bytes`), else 502 `contract_violation`.
  private mapFailure(error: unknown, record: InvocationRecord, args: InvokeArgs): RsError {
    if (record.hostError) return record.hostError;
    const kill = this.mapPlatformKill(error, args.cpuMs);
    if (kill) return kill;
    return RsError.contractViolation(`JS service failed: ${describe(error)}`);
  }

  private mapPlatformKill(error: unknown, cpuMs: number): RsError | undefined {
    const message = describe(error);
    if (/memory|allocation failed|oom/i.test(message)) {
      return RsError.limitExceeded("memory_bytes", GUEST_MEMORY_BYTES, GUEST_MEMORY_BYTES);
    }
    if (/cpu time|cpu limit|exceeded the cpu|worker.*hung|script exceeded/i.test(message)) {
      // The loader's CPU budget is the closest contractual name we have
      // (spec §E.3): report it as a wall-clock breach at the CPU budget.
      return RsError.limitExceeded("wall_clock_ms", cpuMs, cpuMs);
    }
    return undefined;
  }
}

function describe(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/// A handler throw crossing back as a value (see the shim's `invoke`).
function isGuestThrow(value: Json | undefined): boolean {
  return !!value && typeof value === "object" && !Array.isArray(value) && value.__rs2_guest_throw === true;
}

function guestThrowMessage(value: Json | undefined): string {
  const m = value && typeof value === "object" && !Array.isArray(value) ? value.message : undefined;
  return typeof m === "string" ? m : "guest handler threw";
}

/// Resolve a `x-rs2-body-ref` response header (`services/code.rs`): read
/// the named path through the mount's own grant **after** the guest
/// returned, and attach the result as the response body.
export async function resolveBodyRef(
  resp: Message,
  grants: Map<string, CapabilityTarget>,
  tenant: string,
  principal: Principal | undefined,
  trace: Message["trace"],
  depth: number,
  bodyRefHeader: string,
): Promise<Message> {
  const refval = resp.header(bodyRefHeader);
  if (refval === undefined) return resp;
  resp.headers.delete(bodyRefHeader);
  const colon = refval.indexOf(":");
  if (colon < 0) {
    throw RsError.contractViolation(`${bodyRefHeader} must be '<capability>:<path>', got '${refval}'`);
  }
  const capability = refval.slice(0, colon);
  const path = refval.slice(colon + 1);
  if (resp.body) {
    throw RsError.contractViolation(`response carries both a body and ${bodyRefHeader}`);
  }
  const target = grants.get(capability);
  if (!target) throw RsError.capabilityDenied(capability);
  const get = Message.request("GET", path, tenant);
  get.principal = principal ? { ...principal } : undefined;
  get.trace = trace.child();
  get.depth = Math.min(depth + 1, 0xffff);
  get.source = "internal";
  const bodyResp = await target(get);
  if (!bodyResp.isOk() || !bodyResp.body) {
    throw RsError.contractViolation(
      `${bodyRefHeader} read '${capability}:${path}' returned status ${bodyResp.status ?? 0}`,
    );
  }
  resp.body = bodyResp.body;
  return resp;
}
