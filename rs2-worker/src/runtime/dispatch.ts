// The runtime: lazy tenant loading and the dispatch path (PRD §5.2). Port
// of `rs2-core/src/runtime.rs` — the step order in `dispatch` is
// contractual (cloudflare.md §B.3). `handle` never throws: every failure
// becomes a structured problem+json response attributed to the tenant and
// trace.

import { selectSubprotocol, verifiedToken } from "../services/auth";
import type { Requester, Service, ServiceContext, TenantControl } from "../services/context";
import { Body } from "./body";
import { parseTenantConfig } from "./config-schema";
import type { TenantConfig } from "./config-schema";
import { allowedMethods, describeMount, handleDiscovery, isDiscoveryPath } from "./discovery";
import { RsError, codes, toRsError } from "./error";
import type { JsonObject } from "./error";
import * as idem from "./idempotency";
import type { IdempotencyStore } from "./idempotency";
import { Severity, attr, recordNow } from "./logging";
import { MediaType } from "./media-type";
import { simpleUuid } from "./message";
import type { Message } from "./message";
import { validatePath } from "./router";
import type { Mount } from "./router";
import { SOCKETS_SEGMENT, isWebSocketUpgrade, webSocketConfigOf } from "./sockets";
import type { SocketAccept, SocketSelector, WebSocketConfig } from "./sockets";
import { buildTenant } from "./tenant-build";
import type { Adapters, Tenant } from "./tenant-build";
import { TenantBreaker, TenantLimiter, checkAccess, checkDeclaredBodySize, checkMountAccess } from "./wrapper";
import type { LimitTable } from "./wrapper";

/// What the host (the `TenantObject`) supplies: adapters, limits, and the
/// config store (`ConfigLoader` + `save_raw` in Rust).
export interface RuntimeHost {
  adapters: Adapters;
  limits: LimitTable;
  idempotency: IdempotencyStore;
  /// The host's socket registry; absent ⇒ `webSocket` mounts never upgrade
  /// and `/.sockets/` answers 501.
  sockets?: import("./sockets").SocketHub;
  /// The raw config document + opaque version; `undefined` ⇒ unknown tenant.
  loadRaw(tenant: string): Promise<[JsonObject, string] | undefined>;
  /// Persist a raw config document; `expectedVersion` mismatches fail with
  /// 409. Returns the new version.
  saveRaw(tenant: string, config: JsonObject, expectedVersion: string | undefined): Promise<string>;
}

export class Runtime {
  private readonly limiter = new TenantLimiter();
  private readonly breaker = new TenantBreaker();
  private readonly tenants = new Map<string, Tenant>();
  private readonly loading = new Map<string, Promise<Tenant>>();
  private readonly requester: Requester;
  private readonly control: TenantControl;

  constructor(private readonly host: RuntimeHost) {
    this.requester = { request: (msg) => this.handle(msg) };
    this.control = {
      rawConfig: async (tenant) => {
        const raw = await host.loadRaw(tenant);
        if (!raw) throw RsError.notFound(`unknown tenant '${tenant}'`);
        return raw;
      },
      putConfig: async (tenant, config, ifMatch) => {
        // Validate the entire config by dry-building the tenant (PRD §10.6).
        const parsed = parseTenantConfig(config);
        buildTenant(tenant, parsed, host.adapters, host.limits, undefined, undefined);
        const version = await host.saveRaw(tenant, config, ifMatch);
        this.purgeTenant(tenant);
        return version;
      },
    };
  }

  /// The control-plane capability (also used by the admin API).
  tenantControl(): TenantControl {
    return this.control;
  }

  private async tenant(name: string): Promise<Tenant> {
    const built = this.tenants.get(name);
    if (built) return built;
    const inFlight = this.loading.get(name);
    if (inFlight) return inFlight;
    const p = (async () => {
      const raw = await this.host.loadRaw(name);
      if (!raw) throw RsError.notFound(`unknown tenant '${name}'`);
      let config: TenantConfig;
      try {
        config = parseTenantConfig(raw[0]);
      } catch (e) {
        throw RsError.internal(`tenant config for '${name}' is invalid: ${toRsError(e).detail}`);
      }
      const t = buildTenant(name, config, this.host.adapters, this.host.limits, this.requester, this.control);
      this.tenants.set(name, t);
      return t;
    })();
    this.loading.set(name, p);
    try {
      return await p;
    } finally {
      this.loading.delete(name);
    }
  }

  /// A limit breach the host observed outside `invoke` (an oversized or
  /// over-rate socket frame) feeds the same breaker.
  recordBreach(tenant: string): void {
    this.breaker.recordBreach(tenant, this.host.limits);
  }

  /// Drop a tenant's built instances so the next request rebuilds from config.
  purgeTenant(name: string): void {
    this.tenants.delete(name);
  }

  purgeAll(): void {
    this.tenants.clear();
  }

  /// Handle a message; failures become problem+json responses. CORS
  /// response headers are added here — the single choke point.
  async handle(msg: Message): Promise<Message> {
    const start = Date.now();
    const template = msg.response(200, undefined);
    const origin = msg.header("origin");
    const requestHost = msg.header("host");
    const tenantName = msg.tenant;
    const external = msg.source === "external";
    const method = msg.method;
    const path = msg.url.path;
    const trace = msg.trace.clone();
    const principal = msg.principal;

    let resp: Message;
    let err: RsError | undefined;
    try {
      resp = await this.dispatch(msg);
    } catch (e) {
      err = toRsError(e);
      resp = template.errorResponse(err);
    }
    if (external && origin !== undefined) {
      const tenant = this.tenants.get(tenantName);
      if (tenant) tenant.cors.decorate(resp, origin, requestHost);
    }
    // Default caching posture: anything that didn't opt in is never stored
    // (304s are exempt, and a 101 is a handshake, not a representation).
    if (resp.header("cache-control") === undefined && resp.status !== 304 && resp.status !== 101) {
      resp.setHeader("cache-control", "no-store");
    }
    this.emitBoundaryLog(resp, err, external, method, path, trace, tenantName, principal, start);
    return resp;
  }

  private emitBoundaryLog(
    resp: Message,
    err: RsError | undefined,
    external: boolean,
    method: string,
    path: string,
    trace: Message["trace"],
    tenant: string,
    principal: Message["principal"],
    start: number,
  ): void {
    const log = this.host.adapters.log;
    if (!log.enabled()) return;
    const status = resp.status ?? 0;
    const severity =
      status >= 500 ? Severity.Error : status >= 400 ? Severity.Warn : external ? Severity.Info : Severity.Debug;
    const always = status >= 500;
    if (!always && severity < this.host.adapters.logLevel) return;
    const rec = recordNow(severity, tenant, trace, `${method} ${path} -> ${status}`);
    attr(rec, "http.request.method", method);
    attr(rec, "url.path", path);
    attr(rec, "http.response.status_code", status);
    attr(rec, "rs2.source", external ? "external" : "internal");
    attr(rec, "duration_ms", Date.now() - start);
    if (principal) {
      attr(rec, "enduser.id", principal.id);
      attr(rec, "rs2.principal.kind", principal.kind);
    }
    if (err) {
      attr(rec, "error.type", err.code);
      attr(rec, "error.message", err.detail);
      attr(rec, "rs2.retryable", err.retryable);
    }
    log.emit(rec);
  }

  private async dispatch(msg: Message): Promise<Message> {
    validatePath(msg.url.path);
    const limits = this.host.limits;
    if (msg.depth > limits.maxDepth) throw RsError.limitExceeded("call_depth", msg.depth, limits.maxDepth);
    this.breaker.check(msg.tenant);
    const tenant = await this.tenant(msg.tenant);

    // CORS (external-only): answer permitted preflights without routing,
    // and enforce the cookie-CSRF guard.
    if (msg.source === "external") {
      const origin = msg.header("origin");
      if (origin !== undefined) {
        const requestHost = msg.header("host");
        if (msg.method === "OPTIONS") {
          const preflight = tenant.cors.preflight(msg, origin, requestHost);
          if (preflight) return preflight;
        }
        tenant.cors.checkCookieCsrf(msg, origin, requestHost);
      }
    }

    // Verify any presented token into a principal; a bad token is rejected
    // outright rather than treated as anonymous. The expiry is kept because
    // a socket outlives the request that opened it (`SocketAccept.exp`).
    let tokenExp: number | undefined;
    if (!msg.principal) {
      const auth = tenant.auth;
      const secret = auth && typeof auth === "object" && !Array.isArray(auth) ? auth.jwtSecret : undefined;
      if (typeof secret === "string") {
        const verified = await verifiedToken(msg, secret);
        if (verified) {
          msg.principal = verified.principal;
          tokenExp = verified.exp;
        }
      }
    }

    if (isDiscoveryPath(msg.url.path)) return handleDiscovery(tenant, msg, limits);

    const mount = tenant.mounts.route(msg.url.path);
    if (!mount) throw RsError.notFound(`no service mounted at '${msg.url.path}'`);
    msg.url.applyMount(mount.basePath);

    checkAccess(msg, mount);

    // OPTIONS is a read-only capability probe.
    if (msg.method === "OPTIONS") {
      const resp = msg.okJson(describeMount(mount));
      resp.setHeader("allow", allowedMethods(mount).join(", "));
      return resp;
    }

    // WebSockets (cloudflare.md §E.6). Both branches sit after `checkAccess`
    // — a 101 is never sent before authorization — and only on a mount that
    // opted in, so an unflagged mount serves the plain GET (RFC 9110) and
    // `.sockets/` reaches the service as an ordinary path.
    const ws = webSocketConfigOf(mount.config, mount.service);
    if (ws !== undefined) {
      if (msg.source === "external" && msg.method === "GET" && isWebSocketUpgrade(msg) && this.host.sockets) {
        // `checkAccess` defers a pipeline mount's execution paths to the
        // service, which an upgrade never reaches: gate it on the mount's
        // read role here, failing closed like any other mount.
        if (mount.service === "pipeline") checkMountAccess(msg, mount);
        return this.acceptUpgrade(msg, mount, ws, tokenExp);
      }
      if (msg.url.serviceSegments()[0] === SOCKETS_SEGMENT) {
        // Pushing or closing frames is a **write**, never `invoke`/`delete`:
        // a mount that lets the public invoke its pipelines (`invoke: "all"`)
        // must not thereby let the public write to its sockets.
        if (msg.method !== "GET" && msg.method !== "HEAD") checkMountAccess(msg, mount, "write");
        return this.handleSockets(msg, mount);
      }
    }

    checkDeclaredBodySize(msg);
    const release = this.limiter.admit(msg.tenant, limits.tenantConcurrency);
    try {
      const inst = tenant.instance(mount.basePath);
      if (!inst) throw RsError.internal("mount has no built instance");
      const [service, ctx] = inst;
      const cachePolicy = ctx.cachePolicy;
      const openlyReadable = ctx.cacheOpenlyReadable;

      // Idempotency-Key handling (PRD §7.2).
      const key = msg.header("idempotency-key");
      let resp: Message;
      if (key !== undefined) {
        if (key.length > idem.MAX_KEY_LEN) {
          throw RsError.badRequest(`Idempotency-Key exceeds ${idem.MAX_KEY_LEN} characters`);
        }
        const scope = idem.scopeFor(msg, mount.basePath);
        const hash = await idem.payloadHash(msg);
        const store = this.host.idempotency;
        const begin = await store.begin(scope, key, hash);
        switch (begin.kind) {
          case "replay":
            resp = idem.storedIntoMessage(begin.stored, msg);
            break;
          case "inFlight": {
            const err = RsError.conflict("a request with this Idempotency-Key is still executing");
            err.retryable = true;
            err.retryAfterMs = 1000;
            throw err;
          }
          case "payloadMismatch":
            throw RsError.idempotencyKeyReuse("Idempotency-Key was already used with a different request payload");
          case "fresh": {
            let out: Message;
            try {
              out = await this.invoke(service, ctx, msg);
            } catch (e) {
              await store.abandon(scope, key);
              throw e;
            }
            const [captured, stored] = await idem.captureResponse(out, idem.DEFAULT_BODY_CAP);
            if (stored) await store.complete(scope, key, stored);
            else await store.abandon(scope, key);
            resp = captured;
          }
        }
      } else {
        resp = await this.invoke(service, ctx, msg);
      }
      if (resp.isOk()) cachePolicy.apply(resp, openlyReadable);
      return resp;
    } finally {
      release();
    }
  }

  /// Accept an upgrade: a `101` carrying the identity the host needs to
  /// complete the handshake. Nothing else about the request is special —
  /// concurrency admission is not held (the connection is never inside a
  /// wall clock) and idempotency/caching do not apply to a handshake.
  private acceptUpgrade(msg: Message, mount: Mount, ws: WebSocketConfig, exp: number | undefined): Message {
    const limits = this.host.limits;
    const hub = this.host.sockets!;
    const observed = hub.count();
    if (observed >= limits.wsSocketsPerTenant) {
      // A tenant hammering the socket ceiling feeds the breaker exactly as an
      // in-invocation breach would.
      this.recordBreach(msg.tenant);
      throw RsError.limitExceeded("ws_sockets_per_tenant", observed, limits.wsSocketsPerTenant);
    }
    const accept: SocketAccept = {
      id: simpleUuid(),
      tenant: msg.tenant,
      mount: mount.basePath,
      path: msg.url.path,
      query: msg.url.query === "" ? "" : `?${msg.url.query}`,
      principal: msg.principal,
      exp,
      text: ws.text,
      events: ws.events,
      protocol: selectSubprotocol(msg),
      connectedAt: Date.now(),
    };
    const resp = msg.response(101, undefined);
    resp.socketAccept = accept;
    return resp;
  }

  /// The reserved `/<mount>/.sockets/…` subtree (the `.pipelines/`
  /// precedent), answered from the host's `SocketHub` rather than routed to
  /// the service. `checkAccess` has already run with the request's own
  /// method, so a send or a close needed the mount's write role and a
  /// listing its read role.
  private async handleSockets(msg: Message, mount: Mount): Promise<Message> {
    const hub = this.host.sockets;
    if (!hub) throw RsError.providerUnavailable("this host has no socket registry, so '/.sockets/' cannot be served");
    const rest = msg.url.servicePath.slice(`/${SOCKETS_SEGMENT}`.length);
    const relative = rest.replace(/^\/+/, "");
    const sel: SocketSelector = {
      mount: mount.basePath,
      // No path at all (`/<m>/.sockets[/]`) addresses the whole mount.
      path: relative === "" ? undefined : `${mount.basePath}/${relative}`,
      subtree: relative === "" || rest.endsWith("/"),
      id: msg.url.queryParam("$id"),
      user: msg.url.queryParam("$user"),
    };

    switch (msg.method) {
      case "POST": {
        const cap = this.host.limits.wsMessageBytes;
        const body = msg.body;
        if (!body) throw RsError.badRequest("sending a frame requires a request body");
        let bytes: Uint8Array;
        try {
          bytes = await body.materialize(cap);
        } catch (e) {
          const err = toRsError(e);
          if (err.code !== codes.LIMIT_EXCEEDED) throw err;
          throw RsError.payloadTooLarge(`a frame may not exceed the wsMessageBytes limit of ${cap} bytes`);
        }
        // The reply rule, mirrored: JSON and text media types go out as text
        // frames, everything else as binary.
        const media = body.mediaType;
        const frame = media.isJson() || media.isText() ? new TextDecoder().decode(bytes) : bytes;
        return msg.okJson({ sent: hub.send(sel, frame) });
      }
      case "GET": {
        const sockets = hub.list(sel);
        const entries = sockets.map((s) => {
          const entry: JsonObject = { name: s.id, dir: false, path: s.path };
          if (s.user !== undefined) entry.user = s.user;
          entry.connectedAt = s.connectedAt;
          return entry;
        });
        const listing = { path: msg.url.path, entries, total: entries.length };
        const resp = msg.ok(Body.fromString(JSON.stringify(listing), MediaType.dirJson()));
        resp.setHeader("x-total-count", String(entries.length));
        return resp;
      }
      case "DELETE": {
        const raw = msg.url.queryParam("code");
        let code = 1000;
        if (raw !== undefined) {
          const n = Number(raw);
          // Only the normal closure and the application range are a caller's
          // to send; the protocol reserves the rest.
          if (!Number.isInteger(n) || !(n === 1000 || (n >= 3000 && n <= 4999))) {
            throw RsError.badRequest(`close code '${raw}' must be 1000 or in the application range 3000–4999`);
          }
          code = n;
        }
        return msg.okJson({ closed: hub.close(sel, code, msg.url.queryParam("reason") ?? "") });
      }
      default:
        throw new RsError(
          405,
          codes.BAD_REQUEST,
          "Method Not Allowed",
          `'/${SOCKETS_SEGMENT}/' takes POST (send), GET (list) and DELETE (close), not ${msg.method}`,
        );
    }
  }

  private async invoke(service: Service, ctx: ServiceContext, msg: Message): Promise<Message> {
    const tenantName = msg.tenant;
    const wall = this.host.limits.wallClockServiceMs;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(RsError.limitExceeded("wall_clock_ms", wall, wall)), wall);
    });
    try {
      return await Promise.race([service.handle(msg, ctx), timeout]);
    } catch (e) {
      const err = toRsError(e);
      // Resource-limit breaches feed the tenant's circuit breaker; admission
      // rejections and breaker trips themselves do not.
      if (err.code === codes.LIMIT_EXCEEDED) {
        const limit = err.extra?.limit;
        if (limit !== "tenant_concurrency" && limit !== "tenant_breaker") {
          this.breaker.recordBreach(tenantName, this.host.limits);
        }
      }
      throw err;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }
}
