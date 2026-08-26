// Per-mount wrapper concerns (PRD §5.2): limits, caching, CORS, authz, the
// breaker and concurrency admission. Port of `rs2-core/src/wrapper/mod.rs`.

import { RsError } from "./error";
import type { Json, JsonObject } from "./error";
import type { Message, Principal } from "./message";
import type { Mount } from "./router";
import { configGet } from "./router";

/// Limit table (PRD §9.3 defaults). Durations in milliseconds. The
/// materialized-body cap is 32 MiB on this host (cloudflare.md §A/§I.6).
export interface LimitTable {
  wallClockServiceMs: number;
  wallClockPipelineMs: number;
  memoryBytes: number;
  materializedBodyBytes: number;
  tenantConcurrency: number;
  outboundCalls: number;
  maxDepth: number;
  breakerThreshold: number;
  breakerWindowMs: number;
  breakerCooldownMs: number;
}

export function defaultLimits(): LimitTable {
  return {
    wallClockServiceMs: 30_000,
    wallClockPipelineMs: 120_000,
    memoryBytes: 128 * 1024 * 1024,
    materializedBodyBytes: 32 * 1024 * 1024,
    tenantConcurrency: 64,
    outboundCalls: 64,
    maxDepth: 16,
    breakerThreshold: 8,
    breakerWindowMs: 10_000,
    breakerCooldownMs: 5_000,
  };
}

/// Per-invocation limits (PRD §9.3), the slice handed to services/engines.
export interface InvocationLimits {
  wallClockMs: number;
  memoryBytes: number;
  outboundCalls: number;
  materializedBodyBytes: number;
}

export function invocationLimits(l: LimitTable): InvocationLimits {
  return {
    wallClockMs: l.wallClockServiceMs,
    memoryBytes: l.memoryBytes,
    outboundCalls: l.outboundCalls,
    materializedBodyBytes: l.materializedBodyBytes,
  };
}

// ---- caching -------------------------------------------------------------

export type CacheMode = "noStore" | "revalidate" | "cache";

/// Caching policy (per mount, host-applied). Default everywhere: `no-store`.
export class CachePolicy {
  mode: CacheMode = "noStore";
  maxAgeSeconds = 0;
  public_ = false;
  immutable = false;

  static fromConfig(value: Json | undefined): CachePolicy {
    const p = new CachePolicy();
    if (!value || typeof value !== "object" || Array.isArray(value)) return p;
    // A value serde would reject (wrong types / unknown enum) yields the
    // default policy, exactly as `from_value(..).ok().unwrap_or_default()`.
    const mode = value.mode;
    if (mode !== undefined && mode !== "noStore" && mode !== "revalidate" && mode !== "cache") return new CachePolicy();
    if (value.maxAgeSeconds !== undefined && (typeof value.maxAgeSeconds !== "number" || value.maxAgeSeconds < 0))
      return new CachePolicy();
    if (value.public !== undefined && typeof value.public !== "boolean") return new CachePolicy();
    if (value.immutable !== undefined && typeof value.immutable !== "boolean") return new CachePolicy();
    if (mode !== undefined) p.mode = mode;
    if (typeof value.maxAgeSeconds === "number") p.maxAgeSeconds = Math.floor(value.maxAgeSeconds);
    if (typeof value.public === "boolean") p.public_ = value.public;
    if (typeof value.immutable === "boolean") p.immutable = value.immutable;
    return p;
  }

  /// Whether a mount's read surface is anonymously readable (the
  /// precondition for honoring `public`).
  static mountIsOpenlyReadable(mountConfig: JsonObject): boolean {
    const access = configGet(mountConfig, "access");
    if (access === undefined) return true;
    if (typeof access === "string") return access === "open";
    if (access && typeof access === "object" && !Array.isArray(access)) {
      const read = access.read;
      return (typeof read === "string" ? read : "all") === "all";
    }
    return false;
  }

  /// Apply to a successful response lacking its own `Cache-Control`.
  apply(resp: Message, openlyReadable: boolean): void {
    if (resp.header("cache-control") !== undefined || resp.headers.has("set-cookie")) return;
    const clamped = this.public_ && !openlyReadable;
    const scope = this.public_ && !clamped ? "public" : "private";
    let value: string;
    switch (this.mode) {
      case "noStore":
        value = "no-store";
        break;
      case "revalidate":
        value = `${scope}, no-cache`;
        break;
      case "cache":
        value = `${scope}, max-age=${this.maxAgeSeconds}${this.immutable ? ", immutable" : ""}`;
        break;
    }
    resp.setHeader("cache-control", value);
    if ((clamped || !this.public_) && this.mode !== "noStore") {
      appendHeaderValue(resp, "vary", "authorization, cookie");
    }
  }
}

/// Comma-append to a header without clobbering existing values (Vary is
/// written by both CORS and caching). Dedupes case-insensitively.
export function appendHeaderValue(resp: Message, name: string, value: string): void {
  const existing = resp.header(name);
  let merged: string;
  if (existing !== undefined && existing !== "") {
    const has = existing.split(",").some((part) => part.trim().toLowerCase() === value.trim().toLowerCase());
    if (has) return;
    merged = `${existing}, ${value}`;
  } else {
    merged = value;
  }
  resp.setHeader(name, merged);
}

// ---- CORS ----------------------------------------------------------------

/// Response headers other than the CORS-safelisted set that browsers may read.
const EXPOSED_HEADERS = "etag, location, link, x-total-count, x-trace-id, idempotency-replayed, retry-after";

/// Match an origin (`scheme://host[:port]`) against a pattern: `*`, a full
/// origin, a bare hostname, or a `*.suffix` host wildcard (matching the apex).
export function originMatches(patternIn: string, originIn: string): boolean {
  const pattern = patternIn.toLowerCase();
  const origin = originIn.toLowerCase();
  if (pattern === "*") return true;
  if (pattern.includes("://")) {
    return pattern.replace(/\/+$/, "") === origin.replace(/\/+$/, "");
  }
  const i = origin.indexOf("://");
  const hostPort = i >= 0 ? origin.slice(i + 3) : origin;
  const host = hostPort.split(":")[0] ?? hostPort;
  if (pattern.startsWith("*.")) {
    const suffix = pattern.slice(2);
    return host === suffix || host.endsWith(`.${suffix}`);
  }
  if (pattern.includes(":")) return hostPort === pattern;
  return host === pattern;
}

/// Whether the Origin header names the same host the request was sent to.
export function isSameOrigin(origin: string, requestHost: string | undefined): boolean {
  if (requestHost === undefined) return false;
  const i = origin.indexOf("://");
  const originHost = i >= 0 ? origin.slice(i + 3) : origin;
  return originHost.toLowerCase() === requestHost.trim().toLowerCase();
}

/// CORS policy (per tenant, host-enforced).
export class CorsPolicy {
  trustedOrigins: string[] = [];
  allowedOrigins: string[] = [];

  static fromConfig(value: Json | undefined): CorsPolicy {
    const p = new CorsPolicy();
    if (!value || typeof value !== "object" || Array.isArray(value)) return p;
    const strList = (v: Json | undefined): string[] | undefined => {
      if (v === undefined) return [];
      if (!Array.isArray(v) || !v.every((x) => typeof x === "string")) return undefined;
      return v as string[];
    };
    const trusted = strList(value.trustedOrigins);
    const allowed = strList(value.allowedOrigins);
    if (!trusted || !allowed) return new CorsPolicy();
    p.trustedOrigins = trusted;
    p.allowedOrigins = allowed;
    return p;
  }

  isTrusted(origin: string): boolean {
    return this.trustedOrigins.some((p) => originMatches(p, origin));
  }

  isAllowed(origin: string): boolean {
    return this.isTrusted(origin) || this.allowedOrigins.some((p) => originMatches(p, origin));
  }

  /// Add CORS response headers for a permitted cross-origin caller.
  decorate(resp: Message, origin: string, requestHost: string | undefined): void {
    if (isSameOrigin(origin, requestHost) || !this.isAllowed(origin)) return;
    resp.setHeader("access-control-allow-origin", origin);
    appendHeaderValue(resp, "vary", "origin");
    resp.setHeader("access-control-expose-headers", EXPOSED_HEADERS);
    if (this.isTrusted(origin)) resp.setHeader("access-control-allow-credentials", "true");
  }

  /// Answer a preflight (`OPTIONS` + `Origin` + request-method header) from a
  /// permitted origin; `undefined` lets the request route normally.
  preflight(msg: Message, origin: string, requestHost: string | undefined): Message | undefined {
    if (isSameOrigin(origin, requestHost) || !this.isAllowed(origin)) return undefined;
    const requestedMethod = msg.header("access-control-request-method");
    if (requestedMethod === undefined) return undefined;
    const resp = msg.response(204, undefined);
    resp.setHeader("access-control-allow-origin", origin);
    appendHeaderValue(resp, "vary", "origin");
    resp.setHeader("access-control-allow-methods", requestedMethod);
    const allowHeaders =
      msg.header("access-control-request-headers") ?? "authorization, content-type, idempotency-key, if-match";
    resp.setHeader("access-control-allow-headers", allowHeaders);
    resp.setHeader("access-control-max-age", "86400");
    if (this.isTrusted(origin)) resp.setHeader("access-control-allow-credentials", "true");
    return resp;
  }

  /// The CSRF guard: a cookie-authenticated **unsafe** request from a
  /// cross-site, untrusted origin is rejected before routing.
  checkCookieCsrf(msg: Message, origin: string, requestHost: string | undefined): void {
    if (isSameOrigin(origin, requestHost) || this.isTrusted(origin)) return;
    const unsafeMethod = !(msg.method === "GET" || msg.method === "HEAD" || msg.method === "OPTIONS");
    const cookie = msg.header("cookie");
    const hasAuthCookie = cookie !== undefined && cookie.split(";").some((p) => p.trimStart().startsWith("rs-auth="));
    if (unsafeMethod && hasAuthCookie) {
      throw RsError.forbidden(
        `cookie-authenticated cross-origin request from untrusted origin '${origin}' (add it to cors.trustedOrigins, or use a bearer token)`,
      );
    }
  }
}

// ---- breaker + limiter ---------------------------------------------------

/// Per-tenant circuit breaker (PRD §9.3): repeated resource-limit breaches
/// trip the tenant open — requests fail fast with 503 + Retry-After.
export class TenantBreaker {
  private states = new Map<string, { recentBreaches: number[]; openUntil: number | undefined }>();

  check(tenant: string): void {
    const state = this.states.get(tenant);
    if (!state || state.openUntil === undefined) return;
    const now = Date.now();
    if (now < state.openUntil) {
      const err = RsError.limitExceeded("tenant_breaker", 1, 0);
      err.detail = `tenant '${tenant}' tripped the limit-breach circuit breaker; retry later`;
      err.retryAfterMs = state.openUntil - now;
      throw err;
    }
    // Cooldown elapsed: half-open — clear and allow traffic.
    state.openUntil = undefined;
    state.recentBreaches = [];
  }

  recordBreach(tenant: string, limits: LimitTable): void {
    let state = this.states.get(tenant);
    if (!state) {
      state = { recentBreaches: [], openUntil: undefined };
      this.states.set(tenant, state);
    }
    const now = Date.now();
    state.recentBreaches.push(now);
    while (state.recentBreaches.length && now - state.recentBreaches[0]! > limits.breakerWindowMs) {
      state.recentBreaches.shift();
    }
    if (state.recentBreaches.length >= limits.breakerThreshold) {
      state.openUntil = now + limits.breakerCooldownMs;
    }
  }
}

/// Per-tenant concurrency admission: fail fast at the cap, no queueing.
export class TenantLimiter {
  private inFlight = new Map<string, number>();

  admit(tenant: string, cap: number): () => void {
    const n = this.inFlight.get(tenant) ?? 0;
    if (n >= cap) throw RsError.limitExceeded("tenant_concurrency", cap, cap);
    this.inFlight.set(tenant, n + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.inFlight.set(tenant, (this.inFlight.get(tenant) ?? 1) - 1);
    };
  }
}

// ---- authz ---------------------------------------------------------------

/// The action a request's HTTP method maps to (PRD §5.2).
export function actionFor(method: string): "read" | "write" | "delete" | "invoke" {
  switch (method) {
    case "GET":
    case "HEAD":
    case "OPTIONS":
      return "read";
    case "PUT":
    case "PATCH":
      return "write";
    case "DELETE":
      return "delete";
    default:
      return "invoke";
  }
}

function strField(spec: JsonObject, key: string): string | undefined {
  const v = spec[key];
  return typeof v === "string" ? v : undefined;
}

/// Resolve the role-spec string an action key gates within a role object.
function resolveRole(spec: JsonObject, actionKey: string): string {
  const write = strField(spec, "write") ?? "A";
  switch (actionKey) {
    case "read":
      return strField(spec, "read") ?? "all";
    case "delete":
      return strField(spec, "delete") ?? write;
    case "invoke":
      return strField(spec, "invoke") ?? write;
    default:
      return write;
  }
}

/// Evaluate an `access` value for one action key against the message.
export function checkRoleSpec(access: Json, actionKey: string, msg: Message): void {
  if (msg.source === "system") return;
  if (typeof access === "string") {
    if (access === "open") return;
    if (access === "authenticated") {
      if (msg.principal) return;
      throw RsError.unauthorized("this mount requires authentication");
    }
    throw RsError.internal(`unknown access policy '${access}'`);
  }
  if (access && typeof access === "object" && !Array.isArray(access)) {
    const roleSpec = resolveRole(access, actionKey);
    if (satisfiesRoleSpec(roleSpec, msg)) return;
    if (!msg.principal) throw RsError.unauthorized("this mount requires authentication");
    throw RsError.forbidden(`principal lacks a role satisfying '${roleSpec}' for ${msg.method}`);
  }
  throw RsError.internal("invalid 'access' config");
}

/// Per-mount authorization (PRD §5.2). Fail closed: no `access` ⇒ denied.
export function checkAccess(msg: Message, mount: Mount): void {
  const first = msg.url.serviceSegments()[0];
  const isAuthoring = first !== undefined && first.startsWith(".");
  if (mount.service === "pipeline" && !isAuthoring) return;
  const access = configGet(mount.config, "access");
  if (access === undefined) {
    if (msg.principal) throw RsError.forbidden("this mount has no access policy configured");
    throw RsError.unauthorized("this mount has no access policy configured");
  }
  checkRoleSpec(access, actionFor(msg.method), msg);
}

/// Whether a principal holds any tenant **operator** role (`operatorRoles`).
export function isOperator(principal: Principal | undefined, operatorRoles: string): boolean {
  if (!principal) return false;
  return operatorRoles.split(/\s+/).filter((r) => r !== "").some((role) => principal.roles.includes(role));
}

/// Evaluate a role-spec string against the message's principal and path.
export function satisfiesRoleSpec(spec: string, msg: Message): boolean {
  const principal = msg.principal;
  const tokens = spec.split(/\s+/).filter((t) => t !== "");
  let i = 0;
  while (i < tokens.length) {
    const role = tokens[i]!;
    const next = tokens[i + 1];
    const pathPattern = next !== undefined && next.startsWith("/") ? next : undefined;
    const step = pathPattern !== undefined ? 2 : 1;
    let roleOk: boolean;
    if (role === "all") roleOk = true;
    else if (role === "authenticated") roleOk = principal !== undefined;
    else roleOk = principal !== undefined && principal.roles.includes(role);
    if (roleOk) {
      if (pathPattern === undefined) return true;
      // `{email}` resolves to the principal id; any other `{name}` to a
      // string extra claim. An unresolved placeholder stays verbatim.
      let resolved = pathPattern;
      if (principal) {
        resolved = resolved.split("{email}").join(principal.id);
        for (const [k, v] of Object.entries(principal.extra)) {
          if (typeof v === "string") resolved = resolved.split(`{${k}}`).join(v);
        }
      }
      const path = msg.url.servicePath;
      if (path === resolved || path.startsWith(`${resolved}/`)) return true;
    }
    i += step;
  }
  return false;
}

/// Body-size admission from the declared Content-Length, before any read.
export function checkDeclaredBodySize(msg: Message): void {
  const size = msg.body?.size;
  if (size !== undefined) {
    const ABSOLUTE_CAP = 10 * 1024 * 1024 * 1024;
    if (size > ABSOLUTE_CAP) throw RsError.limitExceeded("request_body_bytes", size, ABSOLUTE_CAP);
  }
}
