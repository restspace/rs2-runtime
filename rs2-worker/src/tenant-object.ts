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
  guestSocketCloseOp,
  guestSocketListOp,
  guestSocketSendOp,
  consumeSocketApproval,
  guestStateGetOp,
  guestStatePutOp,
  guestStreamBeginOp,
} from "./engines/dynamic-worker";
import type { GuestSocketTarget, Invocations, SerializedRequest, SerializedResponse, SocketApprovals } from "./engines/dynamic-worker";
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
import { NullLogStore, Severity, attr, parseSeverity, recordNow } from "./runtime/logging";
import type { LogStore } from "./runtime/logging";
import { MediaType } from "./runtime/media-type";
import { closeFor, selectorMatches, socketEventMessage } from "./runtime/sockets";
import type { SocketAccept, SocketHub, SocketInfo, SocketSelector } from "./runtime/sockets";

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
import { limitsFromJson } from "./runtime/wrapper";
import type { LimitTable } from "./runtime/wrapper";
import { HttpCatalogueClient } from "./services/catalogue";
import { hashPassword } from "./services/auth";

const SCHEDULE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS schedule_claims (
  key TEXT NOT NULL, occurrence_ms INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
  PRIMARY KEY (key, occurrence_ms));
`;

// ---- inbound WebSockets (§E.6) ---------------------------------------------

/// How much of a `SocketAccept` we are willing to hold in the hibernation
/// attachment. The platform cap is larger, but `principal.extra` carries
/// arbitrary JWT claims, so anything over this spills to DO storage
/// (`ws:<id>`) and the attachment keeps only what the hub matches on.
const ATTACHMENT_CAP = 2048;
/// Tags are matched by the platform on hibernated sockets, so they stay
/// short and few (≤ 10 tags, ≤ 256 chars each).
const TAG_CAP = 256;

/// The attachment: a `SocketAccept`, with `spilled` marking the trimmed copy
/// whose full record lives in `ws:<id>`.
type Attachment = SocketAccept & { spilled?: boolean };

/// FNV-1a/32, hex — a synchronous digest for over-long tag values. Tags are
/// only a pre-filter (`selectorMatches` re-checks the real values), so a
/// collision costs a wasted comparison and nothing else.
function shortHash(value: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < value.length; i++) {
    h ^= value.charCodeAt(i) & 0xff;
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}

function socketTag(prefix: string, value: string): string {
  const tag = `${prefix}${value}`;
  return tag.length <= TAG_CAP ? tag : `${prefix}#${shortHash(value)}`;
}

/// The tags one socket is accepted with: mount, id, and the principal. Path
/// matching is *not* a tag — `/.sockets/` selects by filtering attachments,
/// which reads hibernated sockets without waking them.
function socketTags(a: SocketAccept): string[] {
  const tags = [socketTag("m:", a.mount), socketTag("id:", a.id)];
  if (a.principal !== undefined) tags.push(socketTag("u:", a.principal.id));
  return tags;
}

function attachmentOf(ws: WebSocket): Attachment | undefined {
  try {
    const a = ws.deserializeAttachment() as Attachment | null;
    return a && typeof a === "object" && typeof a.id === "string" && typeof a.tenant === "string" ? a : undefined;
  } catch {
    return undefined;
  }
}

/// A frame send that must never throw out of the hub: a socket the client
/// has already closed is simply not a recipient.
function sendFrame(ws: WebSocket, frame: string | Uint8Array): boolean {
  try {
    ws.send(frame);
    return true;
  } catch {
    return false;
  }
}

/// Close codes the platform will put on the wire: the defined 1xxx set minus
/// the ones only an endpoint may synthesize (1004/1005/1006/1015), plus the
/// application range. Anything else becomes 1000.
function sendableCloseCode(code: number): number {
  if (code === 1000 || (code >= 1001 && code <= 1003) || (code >= 1007 && code <= 1011)) return code;
  if (code >= 3000 && code <= 4999) return code;
  return 1000;
}

function closeSocket(ws: WebSocket, code: number, reason: string): void {
  try {
    ws.close(sendableCloseCode(code), reason.slice(0, 123));
  } catch {
    /* already closing or closed */
  }
}

/// Per-socket frame accounting (in memory, keyed by socket id). Eviction
/// resets it, which is acceptable: the caps bound a burst, not a quota.
interface SocketMeter {
  inFlight: number;
  /// Start of the current one-second rate window, and frames counted in it.
  windowStart: number;
  windowCount: number;
  /// The `open` dispatch, awaited by the first frame so a service sees
  /// `open` before `message` whenever the client is that quick.
  opened: Promise<void> | undefined;
}

const utf8 = new TextEncoder();

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
  /// One-shot socket approvals bridging `socketCheck` to the egress
  /// gateway's `connect` hook (see `SocketApprovals`).
  private readonly socketApprovals: SocketApprovals = new Map();
  /// The host limit table: defaults with the operator's `RS2_LIMITS`
  /// overrides applied. Parsed once per object, not per request.
  private limitTable: LimitTable | undefined;
  /// Per-socket frame meters (§E.6), keyed by socket id.
  private readonly socketMeters = new Map<string, SocketMeter>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Keep-alives answered by the platform: a `ping` never wakes a
    // hibernating object, so an idle socket costs nothing.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    ctx.blockConcurrencyWhile(async () => {
      for (const stmt of [DATA_SCHEMA_SQL, IDEMPOTENCY_SCHEMA_SQL, LOG_SCHEMA_SQL, SCHEDULE_SCHEMA_SQL]) {
        ctx.storage.sql.exec(stmt);
      }
      migrateIdempotencySchema(ctx.storage.sql);
    });
  }

  /// Host limits (PRD §9.3) as this deployment configures them; what
  /// `/.well-known/rs2/services` advertises and what the wrapper enforces.
  private limits(): LimitTable {
    if (this.limitTable === undefined) this.limitTable = limitsFromJson(this.env.RS2_LIMITS);
    return this.limitTable;
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
      images: this.env.IMAGES,
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
      egressStub: (t) => exports.EgressSockets!({ props: { tenant: t } }),
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
      limits: this.limits(),
      sockets: this.socketHub(),
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
    // An authorized upgrade (§E.6): `dispatch` never sends a 101 before
    // `checkAccess`, so reaching here means the socket is allowed.
    if (resp.status === 101 && resp.socketAccept !== undefined) {
      const upgrade = await this.acceptSocket(resp.socketAccept, resp);
      await drainUnread(request.body);
      return upgrade;
    }
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
    // The tenant is going away: every open socket goes with it (1001), or a
    // client would keep a connection to a mount that no longer exists.
    for (const ws of this.ctx.getWebSockets()) closeSocket(ws, 1001, "tenant deleted");
    this.socketMeters.clear();
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
    buildTenant(tenant, parsed, this.buildAdapters(tenant, config), this.limits(), undefined, undefined);
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
    // Piggy-backed on the schedule alarm (§E.6): expired sockets go early
    // when the object happens to be awake. Nothing here arms an alarm.
    this.sweepExpiredSockets();
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

  // ---- inbound WebSockets (§E.6) -------------------------------------------

  /// Complete an accepted upgrade with the **Hibernation API**: the socket is
  /// handed to the platform (`acceptWebSocket`, never `addEventListener`), so
  /// this object may be evicted while the connection stays open. Everything a
  /// later frame needs travels in the attachment, not in memory.
  private async acceptSocket(accept: SocketAccept, resp: Message): Promise<Response> {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server, socketTags(accept));
    await this.attach(server, accept);
    const headers = new Headers(resp.headers);
    headers.delete("content-length");
    headers.delete("content-type");
    // A handshake may only select a protocol the client offered; `dispatch`
    // already did the choosing.
    if (accept.protocol !== undefined) headers.set("sec-websocket-protocol", accept.protocol);
    // `open` is dispatched off the 101 (the client is connected the moment
    // this response is returned), but the promise is started here and the
    // first frame awaits it, so a service sees `open` before `message`.
    if (accept.events.includes("open")) {
      const opened = this.dispatchSocketEvent(accept, "open").catch(() => undefined);
      this.meter(accept.id).opened = opened;
      this.ctx.waitUntil(opened);
    }
    return new Response(null, { status: 101, webSocket: client, headers });
  }

  /// Store the identity on the socket. `principal.extra` is arbitrary JWT
  /// claims, so an over-large record spills to `ws:<id>` and the attachment
  /// keeps the matching fields only — the hub stays synchronous and the
  /// handlers reload the rest.
  private async attach(ws: WebSocket, accept: SocketAccept): Promise<void> {
    const full = JSON.stringify(accept);
    if (utf8.encode(full).byteLength <= ATTACHMENT_CAP) {
      ws.serializeAttachment(accept satisfies Attachment);
      return;
    }
    await this.ctx.storage.put(`ws:${accept.id}`, full);
    const trimmed: Attachment = {
      ...accept,
      principal: accept.principal ? { ...accept.principal, extra: {} } : undefined,
      spilled: true,
    };
    ws.serializeAttachment(trimmed);
  }

  /// The full record behind an attachment (the spill, when there is one).
  private async fullAccept(a: Attachment): Promise<SocketAccept> {
    if (!a.spilled) return a;
    const text = await this.ctx.storage.get<string>(`ws:${a.id}`);
    return text !== undefined ? (JSON.parse(text) as SocketAccept) : a;
  }

  private meter(id: string): SocketMeter {
    let m = this.socketMeters.get(id);
    if (m === undefined) {
      m = { inFlight: 0, windowStart: Date.now(), windowCount: 0, opened: undefined };
      this.socketMeters.set(id, m);
    }
    return m;
  }

  /// Everything one socket leaves behind: the meter and any spilled record.
  private async forget(id: string): Promise<void> {
    this.socketMeters.delete(id);
    await this.ctx.storage.delete(`ws:${id}`).catch(() => undefined);
  }

  /// The host's `SocketHub` (`runtime/sockets.ts`): the hibernating sockets
  /// of this tenant's object, selected by attachment. `getWebSockets(tag)`
  /// pre-filters by mount; `selectorMatches` is the authority.
  private socketHub(): SocketHub {
    // A socket whose close handshake is still in flight is no longer a
    // recipient: it can linger in `getWebSockets()` with its attachment
    // intact, and counting it would report a frame nobody received. A
    // hibernating socket is `OPEN`, so this does not exclude one.
    const live = (tag?: string): WebSocket[] =>
      this.ctx.getWebSockets(tag).filter((ws) => ws.readyState === WebSocket.READY_STATE_OPEN);
    const matching = (sel: SocketSelector): Array<[WebSocket, Attachment]> => {
      const out: Array<[WebSocket, Attachment]> = [];
      for (const ws of live(socketTag("m:", sel.mount))) {
        const a = attachmentOf(ws);
        if (a !== undefined && selectorMatches(sel, a)) out.push([ws, a]);
      }
      return out;
    };
    return {
      count: () => live().length,
      send: (sel, frame) => {
        let sent = 0;
        for (const [ws] of matching(sel)) if (sendFrame(ws, frame)) sent++;
        return sent;
      },
      close: (sel, code, reason) => {
        let closed = 0;
        for (const [ws, a] of matching(sel)) {
          closeSocket(ws, code, reason);
          // Release the bookkeeping now: `webSocketClose` still fires when the
          // client answers, but only if it does. KNOWN ISSUE (local workerd,
          // unverified in production): a close issued from another event's
          // context — this one, under a `DELETE /.sockets/` request — completes
          // the frame handshake but leaves the client's transport open, so its
          // `close` event can lag; a close from the socket's own event (a
          // limit breach, guest `socket.close`) tears down promptly.
          void this.forget(a.id);
          closed++;
        }
        return closed;
      },
      list: (sel) =>
        matching(sel).map(
          ([, a]): SocketInfo => ({ id: a.id, path: a.path, user: a.principal?.id, connectedAt: a.connectedAt }),
        ),
    };
  }

  /// One socket event as an ordinary dispatch (`socketEventMessage`): its own
  /// wall clock, breaker, admission and boundary log. The reply, if any, goes
  /// back down the socket.
  private async dispatchSocketEvent(
    accept: SocketAccept,
    event: "open" | "message" | "close",
    payload?: string | Uint8Array | { code: number; reason: string; wasClean: boolean },
    ws?: WebSocket,
  ): Promise<void> {
    const runtime = await this.runtimeFor(accept.tenant);
    const resp = await runtime.handle(socketEventMessage(accept, event, payload));
    if (ws === undefined) {
      // Nothing to reply to (`open`/`close`); drain as `fireTick` does.
      if (resp.body) await resp.body.intoStream().cancel().catch(() => undefined);
      return;
    }
    await this.deliverReply(ws, accept, resp);
  }

  /// The reply rule (§E.6): 2xx with a body → one frame on this socket (text
  /// for JSON/text media types, binary otherwise); 204/no body → nothing; an
  /// error that is a `limit_exceeded` → close; any other error → the problem
  /// JSON as a text frame, socket kept.
  private async deliverReply(ws: WebSocket, accept: SocketAccept, resp: Message): Promise<void> {
    const status = resp.status ?? 200;
    const body = resp.body;
    if (body === undefined || status === 204) {
      if (body) await body.intoStream().cancel().catch(() => undefined);
      return;
    }
    let bytes: Uint8Array;
    try {
      bytes = await body.materialize(this.limits().wsMessageBytes);
    } catch (e) {
      // A reply too large for a frame is the service's problem, not the
      // client's: log it and send nothing rather than tearing the socket down.
      this.logSocket(accept, Severity.Warn, `socket reply not deliverable: ${toRsError(e).detail}`);
      return;
    }
    const text = new TextDecoder().decode(bytes);
    if (status >= 200 && status < 300) {
      sendFrame(ws, body.mediaType.isJson() || body.mediaType.isText() ? text : bytes);
      return;
    }
    // Problem+json: `limit_exceeded` (breaker open, wall clock, admission)
    // closes with the same 4000+status mapping `closeFor` uses.
    let code: unknown;
    let limit: unknown;
    try {
      const problem = JSON.parse(text) as JsonObject;
      code = problem.code;
      limit = problem.limit;
    } catch {
      /* not JSON; treat as an ordinary error frame */
    }
    if (code === "limit_exceeded") {
      const reason = typeof limit === "string" ? `limit_exceeded:${limit}` : "limit_exceeded";
      closeSocket(ws, 4000 + status, reason);
      await this.forget(accept.id);
      return;
    }
    sendFrame(ws, text);
  }

  /// A limit breached on the wire, outside `invoke`: it feeds the tenant
  /// breaker exactly as an in-band breach does, then closes. `closeFor` maps
  /// the RS2 error, except that a per-frame breach is a payload/rate problem
  /// (4413/4429), not the 503 the `limit_exceeded` status would give.
  private async breachSocket(ws: WebSocket, a: Attachment, limit: string, observed: number, cap: number): Promise<void> {
    try {
      (await this.runtimeFor(a.tenant)).recordBreach(a.tenant);
    } catch {
      /* the config is gone; the close still stands */
    }
    const { reason } = closeFor(RsError.limitExceeded(limit, observed, cap));
    closeSocket(ws, limit === "ws_message_bytes" ? 4413 : 4429, reason);
    await this.forget(a.id);
  }

  /// Hibernation handler: one client frame. Identity comes from the
  /// attachment, so this works with every in-memory field gone.
  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const a = attachmentOf(ws);
    if (a === undefined) {
      closeSocket(ws, 1011, "socket identity lost");
      return;
    }
    const limits = this.limits();
    const size = typeof message === "string" ? utf8.encode(message).byteLength : message.byteLength;
    if (size > limits.wsMessageBytes) {
      await this.breachSocket(ws, a, "ws_message_bytes", size, limits.wsMessageBytes);
      return;
    }
    const meter = this.meter(a.id);
    if (meter.inFlight >= limits.wsMessagesInFlight) {
      await this.breachSocket(ws, a, "ws_messages_in_flight", meter.inFlight + 1, limits.wsMessagesInFlight);
      return;
    }
    const now = Date.now();
    if (now - meter.windowStart >= 1000) {
      meter.windowStart = now;
      meter.windowCount = 0;
    }
    meter.windowCount++;
    if (meter.windowCount > limits.wsMessagesPerSecond) {
      await this.breachSocket(ws, a, "ws_messages_per_second", meter.windowCount, limits.wsMessagesPerSecond);
      return;
    }
    // Expiry is lazy: an idle socket whose token has passed is closed on its
    // next frame (the alarm sweep is only a best-effort shortcut).
    if (a.exp !== undefined && now / 1000 >= a.exp) {
      closeSocket(ws, 4401, "unauthorized");
      await this.forget(a.id);
      return;
    }
    // Limits bind every frame; dispatch only happens for a subscribed event.
    if (!a.events.includes("message")) return;
    if (meter.opened) await meter.opened;
    meter.inFlight++;
    try {
      const accept = await this.fullAccept(a);
      const payload = typeof message === "string" ? message : new Uint8Array(message);
      await this.dispatchSocketEvent(accept, "message", payload, ws);
    } catch (e) {
      // `handle` does not throw; this is the rebuild path failing (a deleted
      // tenant, a config that no longer parses).
      this.logSocket(a, Severity.Error, `socket message dispatch failed: ${toRsError(e).detail}`);
      closeSocket(ws, 4000 + toRsError(e).status, toRsError(e).code);
      await this.forget(a.id);
    } finally {
      meter.inFlight--;
    }
  }

  /// Hibernation handler: the client closed. Dispatch `close` if subscribed,
  /// complete the handshake, and release the socket's state.
  override async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void> {
    const a = attachmentOf(ws);
    // Complete the handshake **first** (1005/1006 are synthesized by an
    // endpoint and may not go on the wire). A socket left half-closed while
    // its `close` event dispatches lingers in `getWebSockets()`, where it is
    // a stale match for a concurrent `/.sockets/` send.
    closeSocket(ws, code, reason);
    if (a === undefined) return;
    if (a.events.includes("close")) {
      try {
        await this.dispatchSocketEvent(await this.fullAccept(a), "close", { code, reason, wasClean });
      } catch (e) {
        this.logSocket(a, Severity.Warn, `socket close dispatch failed: ${toRsError(e).detail}`);
      }
    }
    await this.forget(a.id);
  }

  /// Hibernation handler: the connection failed. Nothing to reply to — log
  /// it against the tenant and let the socket go.
  override async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    const a = attachmentOf(ws);
    if (a === undefined) return;
    this.logSocket(a, Severity.Warn, `socket error: ${error instanceof Error ? error.message : String(error)}`);
    await this.forget(a.id);
  }

  /// A socket-scoped log line through the tenant's own sink. Best-effort: a
  /// failure here must never take a handler down.
  private logSocket(a: Attachment, severity: Severity, text: string): void {
    void this.runtimeFor(a.tenant)
      .then(() => {
        const rec = attr(attr(recordNow(severity, a.tenant, new TraceContext(), text), "rs2.mount", a.mount), "rs2.socket", a.id);
        this.logStore.emit(rec);
      })
      .catch(() => undefined);
  }

  /// Best-effort sweep of sockets whose token expired while idle. Runs on the
  /// alarm the schedules already arm — it never arms one of its own, so the
  /// lazy close on the next frame remains the guarantee.
  private sweepExpiredSockets(): void {
    const nowSec = Date.now() / 1000;
    for (const ws of this.ctx.getWebSockets()) {
      const a = attachmentOf(ws);
      if (a?.exp === undefined || nowSec < a.exp) continue;
      closeSocket(ws, 4401, "unauthorized");
      void this.forget(a.id);
    }
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

  async guestSocketCheck(invocationId: string, host: string, port: number, tls: boolean): Promise<Json> {
    return guestSocketCheckOp(this.invocations, this.socketApprovals, invocationId, host, port, tls);
  }

  /// Consume a single-use socket approval minted by an allowed
  /// `guestSocketCheck` — called by the `EgressSockets.connect` hook, which
  /// has only the nonce it was dialed with (raw TCP carries no invocation
  /// id). Returns the real target, or null when the nonce is unknown,
  /// spent, or expired.
  async guestSocketConsume(nonce: string): Promise<{ host: string; port: number; tls: boolean } | null> {
    const approval = consumeSocketApproval(this.socketApprovals, nonce);
    return approval ? { host: approval.host, port: approval.port, tls: approval.tls } : null;
  }

  /// Guest `socket.send`/`socket.close` (§E.6): sugar over the mount's own
  /// `/.sockets/` subtree, issued as `system` for the invocation's mount only.
  guestSocketSend(invocationId: string, target: GuestSocketTarget, data: string | Uint8Array): Promise<Json> {
    return guestSocketSendOp(this.invocations, invocationId, target, data);
  }

  guestSocketList(invocationId: string, target: GuestSocketTarget): Promise<Json> {
    return guestSocketListOp(this.invocations, invocationId, target);
  }

  guestSocketClose(invocationId: string, target: GuestSocketTarget, code: number | undefined, reason: string | undefined): Promise<Json> {
    return guestSocketCloseOp(this.invocations, invocationId, target, code, reason);
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
