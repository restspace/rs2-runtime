// `TenantObject`: one Durable Object per tenant = `Runtime::dispatch` +
// `handle` (cloudflare.md §B). KV: the tenant config + version. SQLite:
// data, idempotency, logs, schedule claims. Memory: breaker, concurrency,
// auth lockout, the built `Tenant`. Alarms: scheduled mounts (§B.6).

import { DurableObject } from "cloudflare:workers";
import { FetchHttpOut } from "./capabilities/fetch-http-out";
import {
  DynamicWorkerEngine,
  guestBodyReadOp,
  guestBodyWriteOp,
  guestFetchOp,
  guestLogOp,
  guestRequestOp,
  guestSocketCheckOp,
  guestStateGetOp,
  guestStatePutOp,
  guestStreamBeginOp,
} from "./engines/dynamic-worker";
import type { Invocations, SerializedRequest, SerializedResponse } from "./engines/dynamic-worker";
import { R2FileStore } from "./capabilities/r2-file-store";
import { DATA_SCHEMA_SQL, SqliteDataStore } from "./capabilities/sqlite-data-store";
import {
  IDEMPOTENCY_SCHEMA_SQL,
  SqliteIdempotencyStore,
  migrateIdempotencySchema,
} from "./capabilities/sqlite-idempotency";
import { LOG_SCHEMA_SQL, SqliteLogStore } from "./capabilities/sqlite-log-store";
import type { Env } from "./env";
import { INFRAS_VERSION_HEADER, TENANT_HEADER, TRACE_HEADER } from "./env";
import { Body, EPHEMERAL } from "./runtime/body";
import { parseTenantConfig } from "./runtime/config-schema";
import { sha256Hex } from "./runtime/crypto";
import { Runtime } from "./runtime/dispatch";
import { RsError, toRsError } from "./runtime/error";
import type { Json, JsonObject } from "./runtime/error";
import { InfraSet } from "./runtime/infra";
import { NullLogStore, parseSeverity } from "./runtime/logging";
import type { LogStore } from "./runtime/logging";
import { MediaType } from "./runtime/media-type";

/// A forward that names a tenant this object does not embody (see
/// `TenantObject.isSelf`). Not a client-visible condition in a correct
/// deployment, so a plain 500 problem rather than a tenant-scoped one.
function misrouted(tenant: string): Response {
  return new Response(
    JSON.stringify({
      type: "https://rs2.dev/errors#internal",
      title: "Internal Error",
      status: 500,
      code: "internal",
      detail: `request for tenant '${tenant}' reached a different tenant object`,
    }),
    { status: 500, headers: { "content-type": "application/problem+json" } },
  );
}
import { Message, TraceContext } from "./runtime/message";
import { claimOccurrence, claimTtlMs, dueOccurrenceMs, earliestNextDueMs, scheduledMounts, tickMessage } from "./runtime/scheduler";
import { buildTenant, seedBuiltins } from "./runtime/tenant-build";
import type { Adapters } from "./runtime/tenant-build";
import { defaultLimits } from "./runtime/wrapper";
import { HttpCatalogueClient } from "./services/catalogue";
import { hashPassword } from "./services/auth";

const SCHEDULE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS schedule_claims (
  key TEXT NOT NULL, occurrence_ms INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
  PRIMARY KEY (key, occurrence_ms));
`;

/// The Worker's analogue of `FileConfigLoader::version_of`: 16 lowercase hex
/// = the first 8 bytes of SHA-256 over the stored JSON text.
export async function configVersionOf(text: string): Promise<string> {
  return (await sha256Hex(text)).slice(0, 16);
}

/// Whether a tenant config disables the log sink (`"logging": {"sink": "none"}`).
function loggingEnabled(config: JsonObject | undefined): boolean {
  const logging = config?.logging;
  if (logging && typeof logging === "object" && !Array.isArray(logging)) return logging.sink !== "none";
  return true;
}

export class TenantObject extends DurableObject<Env> {
  private runtime: Runtime | undefined;
  private tenantName: string | undefined;
  /// The registry `infrasVersion` the current build used; a different value
  /// on an incoming request purges the build (§B.5).
  private builtInfrasVersion: string | undefined;
  private infras: InfraSet = new InfraSet();
  private logStore: LogStore = new NullLogStore();
  /// Overlap guard (§B.6): mounts whose previous scheduled fire is running.
  private readonly schedInFlight = new Set<string>();
  /// Live guest invocations (§E.3): id → grants/budget/trace/streams, held
  /// for the call's lifetime so the `HostApi`/`Egress` entrypoints can act
  /// with exactly that invocation's authority.
  private readonly invocations: Invocations = new Map();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      for (const stmt of [DATA_SCHEMA_SQL, IDEMPOTENCY_SCHEMA_SQL, LOG_SCHEMA_SQL, SCHEDULE_SCHEMA_SQL]) {
        ctx.storage.sql.exec(stmt);
      }
      migrateIdempotencySchema(ctx.storage.sql);
    });
  }

  // ---- config storage (§B.4) ---------------------------------------------

  private async loadRaw(): Promise<[JsonObject, string] | undefined> {
    const text = await this.ctx.storage.get<string>("config");
    if (text === undefined) return undefined;
    const version = (await this.ctx.storage.get<string>("config.version")) ?? (await configVersionOf(text));
    return [JSON.parse(text) as JsonObject, version];
  }

  private async saveRaw(config: JsonObject, expectedVersion: string | undefined): Promise<string> {
    if (expectedVersion !== undefined) {
      const current = (await this.ctx.storage.get<string>("config.version")) ?? "";
      if (current !== expectedVersion) {
        throw RsError.conflict("config version mismatch (If-Match): reload and reapply");
      }
    }
    const text = JSON.stringify(config, null, 2);
    const version = await configVersionOf(text);
    await this.ctx.storage.put({ config: text, "config.version": version });
    // Rust parity: a config write rebuilds every service instance, which
    // resets guest `ctx.state` (`GrantedHost.state` lives on the instance).
    // The KV-backed state still survives DO eviction/restarts between
    // config writes — the durable half of cloudflare.md decision 12.
    const stale = await this.ctx.storage.list({ prefix: "state:" });
    const keys = [...stale.keys()];
    for (let i = 0; i < keys.length; i += 128) {
      await this.ctx.storage.delete(keys.slice(i, i + 128));
    }
    return version;
  }

  // ---- runtime wiring ------------------------------------------------------

  private async refreshInfras(): Promise<void> {
    const registry = this.env.REGISTRY.get(this.env.REGISTRY.idFromName("registry"));
    const { infrasText, version } = await registry.getInfras();
    this.infras = InfraSet.fromJson(JSON.parse(infrasText) as Json);
    this.builtInfrasVersion = version;
    await this.ctx.storage.put("infras.version", version);
  }

  private buildAdapters(tenant: string, config: JsonObject | undefined): Adapters {
    const files = new R2FileStore(this.env.RS2_FILES);
    const sql = this.ctx.storage.sql;
    const dataFactory = (ns: string) => new SqliteDataStore(sql, ns);
    const http = new FetchHttpOut();
    const catalogueHosts = (this.env.RS2_CATALOGUE_HOSTS ?? "")
      .split(",")
      .map((h) => h.trim())
      .filter((h) => h !== "");
    this.logStore = new SqliteLogStore(sql, tenant, loggingEnabled(config));
    return {
      files,
      data: dataFactory(""),
      query: undefined,
      http,
      log: this.logStore,
      logLevel: parseSeverity(this.env.RS2_LOG_LEVEL ?? "info") ?? 1,
      builtins: seedBuiltins(files, dataFactory, undefined),
      catalogue: catalogueHosts.length ? new HttpCatalogueClient(http, catalogueHosts) : undefined,
      infras: this.infras,
      engine: this.buildEngine(tenant),
    };
  }

  /// The Dynamic Worker engine (§E): absent without a `worker_loaders`
  /// binding, in which case `code:`/`template` mounts answer 501.
  private buildEngine(tenant: string): DynamicWorkerEngine | undefined {
    const loader = this.env.LOADER;
    if (!loader) return undefined;
    void tenant;
    const exports = (this.ctx as unknown as { exports: Record<string, (opts: { props: Json }) => Fetcher> }).exports;
    return new DynamicWorkerEngine({
      loader,
      invocations: this.invocations,
      hostApiStub: (t) => exports.HostApi!({ props: { tenant: t } }),
      egressStub: (t) => exports.Egress!({ props: { tenant: t } }),
      stateKv: {
        get: (key) => this.ctx.storage.get<string>(key),
        put: (key, value) => this.ctx.storage.put(key, value),
      },
    });
  }

  private async runtimeFor(tenant: string): Promise<Runtime> {
    this.assertSelf(tenant);
    if (this.runtime && this.tenantName === tenant) return this.runtime;
    this.tenantName = tenant;
    if (this.builtInfrasVersion === undefined) await this.refreshInfras();
    const raw = await this.loadRaw();
    const adapters = this.buildAdapters(tenant, raw?.[0]);
    this.runtime = new Runtime({
      adapters,
      limits: defaultLimits(),
      idempotency: new SqliteIdempotencyStore(this.ctx.storage),
      loadRaw: () => this.loadRaw(),
      saveRaw: async (_t, config, expected) => {
        const version = await this.saveRaw(config, expected);
        // The log sink knob lives in the config, so re-evaluate it.
        this.logStore = new SqliteLogStore(this.ctx.storage.sql, tenant, loggingEnabled(config));
        adapters.log = this.logStore;
        // Self-arm (§B.6): a config carrying `schedule` mounts arms this
        // DO's alarm right here — the tenant name is persisted so alarm()
        // can rebuild after eviction, and the registry learns whether this
        // tenant needs the cron safety net (best-effort; the alarm is the
        // real trigger).
        await this.ctx.storage.put("tenant.name", tenant);
        const count = await this.armSchedules(config, true);
        try {
          const registry = this.env.REGISTRY.get(this.env.REGISTRY.idFromName("registry"));
          await registry.noteScheduled(tenant, count > 0);
        } catch {
          /* the safety net misses this tenant until the next config write */
        }
        return version;
      },
    });
    return this.runtime;
  }

  /// Drop the in-memory build (config PUT / infras reload).
  private purge(): void {
    this.runtime = undefined;
  }

  /// Defence in depth: this object serves exactly the tenant whose name
  /// derives its id (`TENANTS.idFromName(tenant)`). A caller naming another
  /// tenant — a leaked stub, a forged forward header — is refused rather
  /// than trusted, so the R2 prefix can never diverge from the DO identity.
  private isSelf(tenant: string): boolean {
    return this.env.TENANTS.idFromName(tenant).equals(this.ctx.id);
  }

  private assertSelf(tenant: string): void {
    if (!this.isSelf(tenant)) {
      throw new Error(`tenant object identity mismatch: this object is not tenant '${tenant}'`);
    }
  }

  // ---- HTTP entry (§B.3 steps 4–8) -----------------------------------------

  override async fetch(request: Request): Promise<Response> {
    const tenant = request.headers.get(TENANT_HEADER) ?? this.env.RS2_DEFAULT_TENANT ?? "main";
    if (!this.isSelf(tenant)) return misrouted(tenant);
    const traceId = request.headers.get(TRACE_HEADER) ?? undefined;
    const infrasVersion = request.headers.get(INFRAS_VERSION_HEADER);
    if (infrasVersion !== null && this.builtInfrasVersion !== undefined && infrasVersion !== this.builtInfrasVersion) {
      await this.refreshInfras();
      this.purge();
    }
    const runtime = await this.runtimeFor(tenant);
    const msg = requestToMessage(request, tenant, traceId);
    const resp = await runtime.handle(msg);
    // A body the service never consumed (a 412, a replay, a 4xx before the
    // read) must be drained here: workerd treats an unread forwarded body
    // as "Can't read from request stream after response has been sent" and
    // tears the event context down.
    await drainUnread(request.body);
    return messageToResponse(resp);
  }

  // ---- RPC surface (admin API, cron) ---------------------------------------

  /// `PUT /admin/tenants/<name>`: dry-build, persist, register. The config
  /// travels as JSON text so the RPC types stay shallow.
  async putConfig(tenant: string, configText: string, ifMatch: string | undefined): Promise<{ version: string; created: boolean }> {
    this.assertSelf(tenant);
    const runtime = await this.runtimeFor(tenant);
    const existed = (await this.ctx.storage.get<string>("config")) !== undefined;
    const config = JSON.parse(configText) as JsonObject;
    const version = await runtime.tenantControl().putConfig(tenant, config, ifMatch);
    return { version, created: !existed };
  }

  async rawConfig(): Promise<{ configText: string; version: string } | undefined> {
    const raw = await this.loadRaw();
    return raw ? { configText: JSON.stringify(raw[0]), version: raw[1] } : undefined;
  }

  /// Seed the bootstrap admin **if absent** exactly as `seed_bootstrap_admin`.
  async seedAdmin(tenant: string, email: string, password: string): Promise<"seeded" | "present"> {
    this.assertSelf(tenant);
    const raw = await this.loadRaw();
    if (!raw) throw RsError.notFound(`unknown tenant '${tenant}'`);
    const auth = raw[0].auth;
    const jwt = auth && typeof auth === "object" && !Array.isArray(auth) ? auth.jwtSecret : undefined;
    if (typeof jwt !== "string" || jwt === "") {
      throw RsError.badRequest(
        `bootstrap admin set but tenant '${tenant}' has no auth.jwtSecret — login can't mint tokens; add one before seeding`,
      );
    }
    const datasetRaw = auth && typeof auth === "object" && !Array.isArray(auth) ? auth.userDataset : undefined;
    const dataset = typeof datasetRaw === "string" ? datasetRaw : "users";
    const data = new SqliteDataStore(this.ctx.storage.sql, "");
    try {
      await data.get(tenant, dataset, email);
      return "present";
    } catch (e) {
      if (!(e instanceof RsError) || e.status !== 404) throw e;
    }
    await data.put(tenant, dataset, email, { passwordHash: await hashPassword(password), roles: "A", kind: "user" });
    return "seeded";
  }

  /// `DELETE /admin/tenants/<name>?confirm=`: wipe every table and key.
  async deleteAll(): Promise<void> {
    this.purge();
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
  }

  /// Validate a config without persisting (used by the admin API before any
  /// registry write). Errors are the same 400s `PUT /raw` produces.
  async dryBuild(tenant: string, configText: string): Promise<void> {
    this.assertSelf(tenant);
    if (this.builtInfrasVersion === undefined) await this.refreshInfras();
    const config = JSON.parse(configText) as JsonObject;
    const parsed = parseTenantConfig(config);
    buildTenant(tenant, parsed, this.buildAdapters(tenant, config), defaultLimits(), undefined, undefined);
  }

  /// Cron reconcile (§B.6): re-arm an alarm that was lost. Alarms survive
  /// eviction and deploys, so this is only the safety net for the rare loss
  /// (alarm retries exhausted); the DO self-arms on every config write.
  async reconcileSchedules(): Promise<number> {
    const raw = await this.loadRaw();
    return this.armSchedules(raw?.[0], false);
  }

  /// Derive the scheduled mounts and arm the alarm at the earliest due time.
  /// `force` (config writes) always moves the alarm to the new schedule; the
  /// safety net only arms a missing alarm or pulls one earlier, so it can
  /// never postpone an imminent fire.
  private async armSchedules(config: JsonObject | undefined, force: boolean): Promise<number> {
    const mounts = config ? scheduledMounts(config) : [];
    if (mounts.length === 0) {
      await this.ctx.storage.deleteAlarm();
      return 0;
    }
    const next = earliestNextDueMs(mounts, Date.now());
    if (next === undefined) return mounts.length; // no cron occurrence within 366 days
    const current = await this.ctx.storage.getAlarm();
    if (force || current === null || next < current) await this.ctx.storage.setAlarm(next);
    return mounts.length;
  }

  /// §B.6: fire every due mount as `tick_message` through `handle`, with the
  /// overlap guard and the `schedule_claims` fire-once claim, then re-arm.
  override async alarm(): Promise<void> {
    const raw = await this.loadRaw();
    if (!raw) return; // tenant deleted; the alarm dies with it
    // No guessing: a tenant configured before names were persisted has no
    // recorded name, and defaulting would tick under the wrong R2 prefix.
    // Its next config write persists the name and re-arms the alarm.
    const tenant = this.tenantName ?? (await this.ctx.storage.get<string>("tenant.name"));
    if (tenant === undefined || !this.isSelf(tenant)) return;
    const mounts = scheduledMounts(raw[0]);
    const nowMs = Date.now();
    const fires: Array<Promise<void>> = [];
    for (const m of mounts) {
      const occ = dueOccurrenceMs(m.schedule, nowMs);
      if (occ === undefined) continue;
      // Overlap guard: skip while this mount's previous fire is running.
      if (this.schedInFlight.has(m.base)) continue;
      // Fire-once: a retried alarm loses the claim for an occurrence that
      // already fired and skips it.
      if (!claimOccurrence(this.ctx.storage.sql, `${tenant}|${m.base}`, occ, claimTtlMs(m.schedule), nowMs)) continue;
      this.schedInFlight.add(m.base);
      fires.push(this.fireTick(tenant, m.base).finally(() => this.schedInFlight.delete(m.base)));
    }
    // Re-arm before awaiting the fires so a crash mid-fire leaves the chain
    // armed (the claims make the retried occurrence a no-op).
    const next = earliestNextDueMs(mounts, nowMs);
    if (next !== undefined) await this.ctx.storage.setAlarm(next);
    await Promise.allSettled(fires);
  }

  /// Dispatch the synthetic internal tick (`fire_tick` in `runtime.rs`);
  /// errors surface as the tick's problem response and are logged there.
  private async fireTick(tenant: string, base: string): Promise<void> {
    const runtime = await this.runtimeFor(tenant);
    const resp = await runtime.handle(tickMessage(tenant, base));
    if (resp.body) await resp.body.intoStream().cancel().catch(() => undefined);
  }

  // ---- guest RPC surface (§E.3): called by the HostApi/Egress entrypoints --

  guestRequest(invocationId: string, capability: string, req: Json): Promise<Json> {
    return guestRequestOp(this.invocations, invocationId, capability, req);
  }

  async guestLog(invocationId: string, level: string, text: string): Promise<void> {
    guestLogOp(this.invocations, invocationId, level, text);
  }

  guestStateGet(invocationId: string, key: string): Promise<Json> {
    return guestStateGetOp(this.invocations, invocationId, key);
  }

  guestStatePut(invocationId: string, key: string, value: string): Promise<Json> {
    return guestStatePutOp(this.invocations, invocationId, key, value);
  }

  guestBodyRead(invocationId: string): Promise<Json | { data: Uint8Array }> {
    return guestBodyReadOp(this.invocations, invocationId);
  }

  async guestStreamBegin(invocationId: string, envelope: Json): Promise<Json> {
    return guestStreamBeginOp(this.invocations, invocationId, envelope);
  }

  guestBodyWrite(invocationId: string, data: Uint8Array): Promise<Json> {
    return guestBodyWriteOp(this.invocations, invocationId, data);
  }

  async guestSocketCheck(invocationId: string, host: string, port: number): Promise<Json> {
    return guestSocketCheckOp(this.invocations, invocationId, host, port);
  }

  guestFetch(invocationId: string | null, req: SerializedRequest): Promise<SerializedResponse> {
    return guestFetchOp(this.invocations, invocationId, req);
  }
}

/// Cancel a request body stream nobody read (no-op when consumed or locked).
export async function drainUnread(body: ReadableStream<Uint8Array> | null): Promise<void> {
  if (!body || body.locked) return;
  try {
    const reader = body.getReader();
    for (;;) {
      const { done } = await reader.read();
      if (done) break;
    }
  } catch {
    /* already closed */
  }
}

/// Build the `Message` (§B.3 step 4): method, `MsgUrl.parse` of path+query,
/// headers, body as a stream with `size` from `Content-Length`,
/// `Provenance::Ephemeral`; no body for GET/HEAD or `Content-Length: 0`.
export function requestToMessage(request: Request, tenant: string, traceId: string | undefined): Message {
  const url = new URL(request.url);
  const msg = Message.request(request.method, `${url.pathname}${url.search}`, tenant);
  const headers = new Headers(request.headers);
  headers.delete(TENANT_HEADER);
  headers.delete(TRACE_HEADER);
  headers.delete(INFRAS_VERSION_HEADER);
  msg.headers = headers;
  if (traceId !== undefined) msg.trace = new TraceContext(traceId);
  const ct = request.headers.get("content-type");
  const mediaType = ct !== null ? MediaType.parse(ct) : MediaType.octetStream();
  const cl = request.headers.get("content-length");
  const size = cl !== null && /^\d+$/.test(cl) ? Number(cl) : undefined;
  const hasBody = size === undefined ? true : size > 0;
  if (hasBody && msg.method !== "GET" && msg.method !== "HEAD" && request.body) {
    msg.body = Body.fromStream(request.body, mediaType, size, EPHEMERAL);
  }
  return msg;
}

/// The response conversion (§B.3 step 8): header sets copied verbatim,
/// `Content-Type` from the media type, `Content-Length` when known.
export function messageToResponse(msg: Message): Response {
  const status = msg.status ?? 200;
  const headers = new Headers(msg.headers);
  const noBody = status === 204 || status === 304 || msg.method === "HEAD";
  if (!msg.body || noBody) {
    if (msg.body && noBody) msg.body.intoStream().cancel().catch(() => undefined);
    return new Response(null, { status, headers });
  }
  headers.set("content-type", msg.body.mediaType.toString());
  if (msg.body.size !== undefined) headers.set("content-length", String(msg.body.size));
  if (msg.body.payload.kind === "bytes") return new Response(msg.body.payload.bytes, { status, headers });
  return new Response(msg.body.payload.stream, { status, headers });
}

export function problemResponse(err: RsError, tenant: string, traceId: string): Response {
  return new Response(JSON.stringify(err.toProblemJson(tenant, traceId)), {
    status: err.status,
    headers: { "content-type": "application/problem+json" },
  });
}

export function asRsError(e: unknown): RsError {
  return toRsError(e);
}

export type { Json };
