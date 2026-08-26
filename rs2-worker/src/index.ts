// The stateless Worker (cloudflare.md §B.2/§B.3 [W] steps): ops endpoints,
// the admin API (§B.5), hostname → tenant, forward to the tenant's
// `TenantObject`, and the cron fan-out. Every tenant request routes through
// the DO — there is no Worker-side fast path.

import { cfApiFromEnv, domainResponse } from "./domains";
import type { Env } from "./env";
import { INFRAS_VERSION_HEADER, TENANT_HEADER, TRACE_HEADER } from "./env";
import { RegistryObject } from "./registry-object";
import type { RegistrySnapshot } from "./registry-object";
import { constantTimeEqual } from "./runtime/crypto";
import { RsError, toRsError } from "./runtime/error";
import type { Json, JsonObject } from "./runtime/error";
import { simpleUuid } from "./runtime/message";
import { resolveTenant } from "./runtime/router";
import { redactSecrets } from "./services/services-config";
import { TenantObject, drainUnread } from "./tenant-object";

export { RegistryObject, TenantObject };
// Guest-boundary entrypoints (§E.3/§E.4): top-level exports so the tenant
// DO can instantiate them via `ctx.exports` with per-tenant props.
export { Egress, HostApi } from "./egress";

const SNAPSHOT_TTL_MS = 30_000;
/// The Cache API key for the registry snapshot (a synthetic, never-served URL).
const SNAPSHOT_CACHE_URL = "https://rs2-registry.internal/snapshot";
let snapshotCache: { at: number; value: RegistrySnapshot } | undefined;

/// Hostname→tenant on the hot path (§B.3 step 2): a 30 s isolate cache, then
/// the colo-local Cache API (so a cold isolate skips the DO round trip), then
/// the RegistryObject — which is therefore not consulted per request.
async function registrySnapshot(env: Env): Promise<RegistrySnapshot> {
  const now = Date.now();
  if (snapshotCache && now - snapshotCache.at < SNAPSHOT_TTL_MS) return snapshotCache.value;
  try {
    const hit = await caches.default.match(SNAPSHOT_CACHE_URL);
    if (hit) {
      const value = (await hit.json()) as RegistrySnapshot;
      snapshotCache = { at: now, value };
      return value;
    }
  } catch {
    /* Cache API unavailable in some harnesses; fall through to the DO */
  }
  const value = await registry(env).snapshot();
  snapshotCache = { at: now, value };
  try {
    await caches.default.put(
      SNAPSHOT_CACHE_URL,
      new Response(JSON.stringify(value), {
        headers: { "content-type": "application/json", "cache-control": `max-age=${SNAPSHOT_TTL_MS / 1000}` },
      }),
    );
  } catch {
    /* best-effort */
  }
  return value;
}

/// Registry writes (domains, tenants, infras) drop both cache layers in the
/// writing isolate; other isolates/colos converge within the 30 s TTL.
async function invalidateSnapshot(): Promise<void> {
  snapshotCache = undefined;
  try {
    await caches.default.delete(SNAPSHOT_CACHE_URL);
  } catch {
    /* best-effort */
  }
}

function registry(env: Env): DurableObjectStub<RegistryObject> {
  return env.REGISTRY.get(env.REGISTRY.idFromName("registry"));
}

function tenantStub(env: Env, name: string): DurableObjectStub<TenantObject> {
  return env.TENANTS.get(env.TENANTS.idFromName(name));
}

function text(status: number, body: string): Response {
  return new Response(body, { status, headers: { "content-type": "text/plain" } });
}

function json(status: number, body: Json, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
}

function problem(err: RsError): Response {
  return new Response(JSON.stringify(err.toProblemJson("-", "-")), {
    status: err.status,
    headers: { "content-type": "application/problem+json" },
  });
}

/// The bearer/token presented for a node admin request.
function presentedAdminToken(request: Request): string | undefined {
  const auth = request.headers.get("authorization");
  if (auth !== null && auth.startsWith("Bearer ")) return auth.slice("Bearer ".length).trim();
  return request.headers.get("x-admin-token") ?? undefined;
}

/// The admin gate, verbatim from `rs2-server`: 503 without a configured
/// token, 401 on a missing/invalid one (constant-time compare).
function adminGate(request: Request, env: Env): Response | undefined {
  const expected = env.RS2_ADMIN_TOKEN;
  if (expected === undefined || expected === "") {
    return text(503, "admin endpoint disabled: set RS2_ADMIN_TOKEN or serverConfig.adminToken\n");
  }
  const presented = presentedAdminToken(request);
  const enc = new TextEncoder();
  if (presented === undefined || !constantTimeEqual(enc.encode(presented), enc.encode(expected))) {
    return text(401, "missing or invalid admin token\n");
  }
  return undefined;
}

function validTenantName(name: string): boolean {
  return name !== "" && !/[/\\.]/.test(name);
}

async function readJsonObject(request: Request): Promise<JsonObject> {
  let v: Json;
  try {
    v = (await request.json()) as Json;
  } catch (e) {
    throw RsError.badRequest(`invalid JSON body: ${e instanceof Error ? e.message : String(e)}`);
  }
  if (!v || typeof v !== "object" || Array.isArray(v)) throw RsError.badRequest("request body must be a JSON object");
  return v;
}

/// `/admin/*` (§B.5). Errors are problem+json with `tenant: "-"`.
async function handleAdmin(request: Request, env: Env, url: URL): Promise<Response> {
  const path = url.pathname;
  if (path === "/admin/reload-infras") {
    if (request.method !== "POST") return text(405, "POST only\n");
    const gate = adminGate(request, env);
    if (gate) return gate;
    try {
      const names = await registry(env).reloadInfras();
      await invalidateSnapshot();
      return json(200, { loaded: names.length, names });
    } catch (e) {
      return problem(toRsError(e));
    }
  }
  const gate = adminGate(request, env);
  if (gate) return gate;
  try {
    const parts = path.split("/").filter((s) => s !== "");
    // parts[0] === "admin"
    if (parts[1] === "tenants" && parts.length === 2 && request.method === "GET") {
      return json(200, { tenants: await registry(env).listTenants() });
    }
    if (parts[1] === "tenants" && parts.length === 3) {
      const name = decodeURIComponent(parts[2]!);
      if (!validTenantName(name)) throw RsError.badRequest("invalid tenant name");
      const stub = tenantStub(env, name);
      if (request.method === "PUT") {
        const body = await readJsonObject(request);
        const config = body.config;
        if (!config || typeof config !== "object" || Array.isArray(config)) {
          throw RsError.badRequest("PUT /admin/tenants/<name> requires a 'config' object");
        }
        const domainsRaw = body.domains ?? [];
        if (!Array.isArray(domainsRaw) || !domainsRaw.every((d) => typeof d === "string")) {
          throw RsError.badRequest("'domains' must be an array of host names");
        }
        const bootstrap = body.bootstrapAdmin;
        let admin: [string, string] | undefined;
        if (bootstrap !== undefined && bootstrap !== null) {
          if (
            !bootstrap ||
            typeof bootstrap !== "object" ||
            Array.isArray(bootstrap) ||
            typeof bootstrap.email !== "string" ||
            typeof bootstrap.password !== "string" ||
            bootstrap.email === "" ||
            bootstrap.password === ""
          ) {
            throw RsError.badRequest("bootstrapAdmin requires both 'email' and 'password'");
          }
          const auth = config.auth;
          const jwt = auth && typeof auth === "object" && !Array.isArray(auth) ? auth.jwtSecret : undefined;
          if (typeof jwt !== "string" || jwt === "") {
            throw RsError.badRequest(
              `bootstrap admin set but tenant '${name}' has no auth.jwtSecret — login can't mint tokens; add one before seeding`,
            );
          }
          admin = [bootstrap.email, bootstrap.password];
        }
        const ifMatchRaw = request.headers.get("if-match");
        const ifMatch = ifMatchRaw !== null ? ifMatchRaw.trim().replace(/^"+|"+$/g, "") : undefined;
        const { version, created } = await stub.putConfig(name, JSON.stringify(config), ifMatch);
        await registry(env).upsertTenant(name, domainsRaw as string[], version);
        await invalidateSnapshot();
        if (admin) await stub.seedAdmin(name, admin[0], admin[1]);
        return json(created ? 201 : 200, { name, version, created }, { etag: `"${version}"` });
      }
      if (request.method === "GET") {
        const raw = await stub.rawConfig();
        if (!raw) throw RsError.notFound(`unknown tenant '${name}'`);
        return json(200, redactSecrets(JSON.parse(raw.configText) as JsonObject), { etag: `"${raw.version}"` });
      }
      if (request.method === "DELETE") {
        if (url.searchParams.get("confirm") !== name) {
          throw RsError.conflict(`tenant delete requires '?confirm=${name}'`);
        }
        await registry(env).deleteTenant(name);
        await invalidateSnapshot();
        await stub.deleteAll();
        return new Response(null, { status: 204 });
      }
      return text(405, "GET, PUT, DELETE only\n");
    }
    if (parts[1] === "domains" && parts.length === 3) {
      const host = decodeURIComponent(parts[2]!).toLowerCase();
      // With CF_API_TOKEN + CF_ZONE_ID configured, the endpoints also manage
      // a Cloudflare for SaaS custom hostname (src/domains.ts); without them
      // they are registry-only and the response body says so.
      const cf = cfApiFromEnv(env);
      if (request.method === "PUT") {
        const body = await readJsonObject(request);
        if (typeof body.tenant !== "string" || !validTenantName(body.tenant)) {
          throw RsError.badRequest("PUT /admin/domains/<host> requires a 'tenant' name");
        }
        await registry(env).putDomain(host, body.tenant);
        await invalidateSnapshot();
        const provisioning = cf ? await cf.ensure(host) : undefined;
        return json(200, domainResponse(host, body.tenant, env, cf !== undefined, provisioning));
      }
      if (request.method === "GET") {
        const tenant = await registry(env).getDomain(host);
        if (tenant === undefined) throw RsError.notFound(`no domain '${host}'`);
        const provisioning = cf ? await cf.find(host) : undefined;
        return json(200, domainResponse(host, tenant, env, cf !== undefined, provisioning));
      }
      if (request.method === "DELETE") {
        await registry(env).deleteDomain(host);
        await invalidateSnapshot();
        if (cf) await cf.remove(host);
        return new Response(null, { status: 204 });
      }
      return text(405, "GET, PUT, DELETE only\n");
    }
    if (parts[1] === "infras" && parts.length === 2) {
      if (request.method !== "PUT") return text(405, "PUT only\n");
      const doc = await readJsonObject(request);
      const version = await registry(env).putInfras(JSON.stringify(doc));
      await invalidateSnapshot();
      return json(200, { version });
    }
    throw RsError.notFound(`no admin endpoint '${path}'`);
  } catch (e) {
    return problem(toRsError(e));
  }
}

/// Only the exact operator routes are claimed by the Worker — the Rust server
/// claims just its own ops endpoints and lets every other path (a tenant mount
/// at `/admin`, say) fall through to tenant routing, and the Worker must too.
function isOperatorPath(path: string): boolean {
  if (path === "/admin/reload-infras") return true;
  for (const root of ["/admin/tenants", "/admin/domains", "/admin/infras"]) {
    if (path === root || path.startsWith(root + "/")) return true;
  }
  return false;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;
    // Ops endpoints (PRD §14), outside tenant routing.
    if (path === "/healthz" || path === "/readyz") return text(200, "ok");
    if (isOperatorPath(path)) return handleAdmin(request, env, url);

    // Hostname → tenant (§B.3 step 2).
    const host = request.headers.get("host") ?? "";
    const snapshot = await registrySnapshot(env);
    const tenant = resolveTenant(
      {
        domainMap: new Map(Object.entries(snapshot.domainMap)),
        mainDomain: env.RS2_MAIN_DOMAIN || undefined,
        defaultTenant: env.RS2_DEFAULT_TENANT || undefined,
      },
      host,
    );
    if (tenant === undefined) return problem(RsError.notFound(`no tenant for host '${host}'`));

    // Forward (§B.3 step 3): original URL, method, headers, streaming body.
    const traceId = simpleUuid();
    const headers = new Headers(request.headers);
    headers.set(TENANT_HEADER, tenant);
    headers.set(TRACE_HEADER, traceId);
    headers.set(INFRAS_VERSION_HEADER, snapshot.infrasVersion);
    const forwarded = new Request(request, { headers });
    const resp = await tenantStub(env, tenant).fetch(forwarded);
    await drainUnread(forwarded.body);
    const out = new Response(resp.body, resp);
    out.headers.set("x-trace-id", traceId);
    return out;
  },

  /// The reconcile safety net (§B.6, decision in the WF2 log): tenant DOs
  /// self-arm their alarm on every config write and DO alarms survive
  /// eviction and deploys, so the cron only re-arms the rare lost alarm
  /// (retries exhausted). It pings just the tenants the registry knows carry
  /// scheduled mounts — never the whole tenant list.
  async scheduled(_controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(
      (async () => {
        const names = await registry(env).scheduledTenants();
        for (const name of names) {
          await tenantStub(env, name)
            .reconcileSchedules()
            .catch(() => undefined);
        }
      })(),
    );
  },
} satisfies ExportedHandler<Env>;
