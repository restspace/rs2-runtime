// Tenant seeding and reshaping (spec F.2). Both hosts start with one
// pre-provisioned tenant `conf` whose config is `fixtures/conf.base.json`;
// every suite then reshapes it THROUGH THE API IT IS TESTING (login,
// `GET/PUT /services/raw` with `If-Match`) and restores it in `afterAll`.
//
// Additional principals are created through a `wrapper` mount identical to
// `tests/wrapper.rs`'s hashing facade, so password hashes are minted by the
// host under test — the runner never depends on a local argon2.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { env, PACKAGE_ROOT, Rs2Client, seg, type Rs2Response } from "./client.ts";

export interface Mount {
  path: string;
  service: string;
  config?: Record<string, unknown>;
  [extra: string]: unknown;
}

export interface TenantConfig {
  auth?: Record<string, unknown>;
  operatorRoles?: string;
  cors?: Record<string, unknown>;
  mounts: Mount[];
  [extra: string]: unknown;
}

export const BASE_CONFIG_PATH = resolve(PACKAGE_ROOT, "fixtures", "conf.base.json");

/** Mounts every reshaped config keeps (`/services`, `/auth`, `/data`). */
export const BASE_MOUNT_PATHS = ["/services", "/auth", "/data"] as const;

/** A fresh deep copy of `conf.base.json`. */
export function baseConfig(): TenantConfig {
  return JSON.parse(readFileSync(BASE_CONFIG_PATH, "utf8")) as TenantConfig;
}

/** Where the hashing facade is mounted while principals are being created. */
export const HASHING_FACADE_PATH = "/seed-users";

/**
 * The hashing facade from `tests/wrapper.rs` (`wrapper_hashing_store_facade`):
 * a `store`-pattern wrapper fronting `/data/users` whose PUT hashes
 * `password` with `$hashPassword` on the host. `roles`/`kind` default to
 * `U`/`user` exactly as the Rust test's transform does.
 */
export function hashingFacadeMount(path: string = HASHING_FACADE_PATH): Mount {
  return {
    path,
    service: "wrapper",
    config: {
      access: "open",
      pattern: "store",
      pipeline: {
        mode: "conditional",
        steps: [
          { if: "method == 'GET'", call: { method: "GET", url: "/data/users${url.rest}" } },
          {
            if: "method == 'PUT'",
            pipeline: {
              mode: "serial",
              steps: [
                {
                  transform: {
                    passwordHash: "$hashPassword(password)",
                    roles: "roles ? roles : 'U'",
                    kind: "kind ? kind : 'user'",
                  },
                },
                { call: { method: "PUT", url: "/data/users${url.rest}", effect: "idempotent" } },
              ],
            },
          },
        ],
      },
    },
  };
}

export interface Principal {
  email: string;
  password: string;
  /** Role spec string (`"U"`, `"U E"`) or array — stored as given. */
  roles?: string | string[];
  kind?: string;
  /** Extra user-record fields (merge-PATCHed after creation, e.g. for `jwtUserProps`). */
  extra?: Record<string, unknown>;
}

/** `POST /auth/login` → bearer token. Throws with the problem body on failure. */
export async function login(client: Rs2Client, email: string, password: string, authMount = "/auth"): Promise<string> {
  const res = await client.post(`${authMount}/login`, { json: { email, password }, token: null });
  if (res.status !== 200) throw new Error(`login as ${email} failed: ${res.describe()}`);
  const body = res.json<{ token?: string }>();
  if (!body.token) throw new Error(`login as ${email}: no token in ${res.text()}`);
  return body.token;
}

function expectStatus(res: Rs2Response, wanted: number | number[], what: string): void {
  const ok = Array.isArray(wanted) ? wanted.includes(res.status) : res.status === wanted;
  if (!ok) throw new Error(`${what}: expected ${JSON.stringify(wanted)}, got ${res.describe()}`);
}

/**
 * Wait for `/readyz`, then make sure the fixture tenant exists (the Worker
 * needs `PUT /admin/tenants/conf`; the Rust host has it on disk) and the
 * admin can log in. Called from vitest `globalSetup`; safe to call again.
 */
export async function provisionTenant(): Promise<void> {
  const e = env();
  const anon = new Rs2Client();
  await waitForReady(anon, 120_000);

  if (e.hostKind === "cloudflare") {
    if (!e.adminToken) {
      throw new Error("RS2_HOST_KIND=cloudflare needs RS2_ADMIN_TOKEN to seed the tenant through /admin/tenants");
    }
    // Seed-if-absent: the Worker's admin API replaces the config but keeps
    // an existing admin record, so re-running is idempotent (spec B.5).
    const res = await anon.put(`/admin/tenants/${e.tenant}`, {
      json: {
        config: baseConfig(),
        domains: [],
        bootstrapAdmin: { email: e.adminEmail, password: e.adminPassword },
      },
      headers: { authorization: `Bearer ${e.adminToken}` },
      host: e.host,
    });
    expectStatus(res, [200, 201], `PUT /admin/tenants/${e.tenant}`);
  }

  try {
    await login(anon, e.adminEmail, e.adminPassword);
  } catch (err) {
    throw new Error(
      `the fixture tenant '${e.tenant}' at ${e.baseUrl} does not accept the admin login (${e.adminEmail}). ` +
        `Start the host with the fixtures (npm run host:rust / host:cf) or set RS2_ADMIN_EMAIL/RS2_ADMIN_PASSWORD. Cause: ${(err as Error).message}`,
    );
  }
}

/** Poll `GET /readyz` until it answers 200. */
export async function waitForReady(client: Rs2Client, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    try {
      const res = await client.get("/readyz", { token: null });
      if (res.status === 200) return;
      last = res.describe();
    } catch (err) {
      last = (err as Error).message;
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`no RS2 host answering /readyz at ${client.baseUrl} after ${timeoutMs} ms (last: ${last}). Start one with \`npm run host:rust\` (RS2_PORT=${process.env.RS2_PORT || 3100}).`);
}

/**
 * One suite's handle on the shared tenant: the admin/anon clients, the
 * config version, and a record of what to tear down in `restore()`.
 *
 * Usage:
 *   const seed = await Seed.create();            // beforeAll: base config
 *   await seed.applyMounts([{ path: "/files", service: "file", config: { access: "open" } }]);
 *   const dev = await seed.createPrincipals([{ email: "dev@conf.test", password: "pw", roles: "U" }]);
 *   ...
 *   await seed.restore();                         // afterAll
 */
export class Seed {
  readonly anon: Rs2Client;
  readonly admin: Rs2Client;
  readonly adminToken: string;
  /** The current `ETag` of `/services/raw`, tracked through every PUT. */
  etag: string;
  readonly servicesMount: string;
  private readonly principals = new Set<string>();
  private readonly datasets: { mount: string; dataset: string }[] = [];
  private readonly dirs: { mount: string; dir: string }[] = [];

  private constructor(anon: Rs2Client, adminToken: string, etag: string, servicesMount: string) {
    this.anon = anon;
    this.admin = anon.withToken(adminToken);
    this.adminToken = adminToken;
    this.etag = etag;
    this.servicesMount = servicesMount;
  }

  /**
   * Log in as the bootstrap admin and reset the tenant to `conf.base.json`
   * (so a suite that crashed earlier cannot leak its shape into this one).
   */
  static async create(opts: { servicesMount?: string; reset?: boolean } = {}): Promise<Seed> {
    const e = env();
    const anon = new Rs2Client();
    const token = await login(anon, e.adminEmail, e.adminPassword);
    const servicesMount = opts.servicesMount ?? "/services";
    const raw = await anon.withToken(token).get(`${servicesMount}/raw`);
    expectStatus(raw, 200, `GET ${servicesMount}/raw`);
    const etag = raw.etag();
    if (!etag) throw new Error(`GET ${servicesMount}/raw carries no ETag`);
    const seed = new Seed(anon, token, etag, servicesMount);
    if (opts.reset !== false) await seed.putConfig(baseConfig());
    return seed;
  }

  /** `GET /services/raw`: the redacted config and its `ETag`. */
  async currentConfig(): Promise<{ config: TenantConfig; etag: string }> {
    const res = await this.admin.get(`${this.servicesMount}/raw`);
    expectStatus(res, 200, `GET ${this.servicesMount}/raw`);
    const etag = res.etag();
    if (!etag) throw new Error(`GET ${this.servicesMount}/raw carries no ETag`);
    this.etag = etag;
    return { config: res.json<TenantConfig>(), etag };
  }

  /**
   * `PUT /services/raw` with `If-Match` (defaults to the tracked ETag;
   * `null` sends none). Expects 204 and returns the new ETag.
   */
  async putConfig(config: TenantConfig, opts: { ifMatch?: string | null } = {}): Promise<string> {
    const ifMatch = opts.ifMatch === undefined ? this.etag : opts.ifMatch;
    const headers: Record<string, string> = {};
    if (ifMatch) headers["if-match"] = ifMatch;
    const res = await this.admin.put(`${this.servicesMount}/raw`, { json: config, headers });
    expectStatus(res, 204, `PUT ${this.servicesMount}/raw`);
    const etag = res.etag();
    if (!etag) throw new Error(`PUT ${this.servicesMount}/raw (204) carries no ETag`);
    this.etag = etag;
    return etag;
  }

  /** Raw PUT of a config document — no status check — for suites testing `/raw` itself. */
  async tryPutConfig(config: unknown, ifMatch?: string | null): Promise<Rs2Response> {
    const headers: Record<string, string> = {};
    const im = ifMatch === undefined ? this.etag : ifMatch;
    if (im) headers["if-match"] = im;
    const res = await this.admin.put(`${this.servicesMount}/raw`, { json: config, headers });
    const etag = res.etag();
    if (res.status === 204 && etag) this.etag = etag;
    return res;
  }

  /**
   * Reshape the tenant: `conf.base.json` + `extra` mounts (an extra mount
   * at a base path replaces the base one), plus optional top-level
   * overrides (`auth`, `cors`, `retry`, ...). Returns the new ETag.
   */
  async applyMounts(extra: Mount[], overrides: Partial<Omit<TenantConfig, "mounts">> = {}): Promise<string> {
    const config = { ...baseConfig(), ...overrides } as TenantConfig;
    const replaced = new Set(extra.map((m) => m.path));
    config.mounts = [...config.mounts.filter((m) => !replaced.has(m.path)), ...extra];
    return this.putConfig(config);
  }

  /**
   * Create principals with host-minted password hashes. Mounts the hashing
   * facade if the current config lacks it, PUTs each user through it,
   * merge-PATCHes any `extra` fields, then removes the facade again unless
   * `keepFacade`. Records the emails for `restore()`.
   */
  async createPrincipals(list: Principal[], opts: { keepFacade?: boolean; dataMount?: string } = {}): Promise<void> {
    const dataMount = opts.dataMount ?? "/data";
    const { config } = await this.currentConfig();
    const hadFacade = config.mounts.some((m) => m.path === HASHING_FACADE_PATH);
    if (!hadFacade) {
      config.mounts.push(hashingFacadeMount());
      await this.putConfig(config);
    }
    for (const p of list) {
      const body: Record<string, unknown> = { password: p.password };
      if (p.roles !== undefined) body.roles = p.roles;
      if (p.kind !== undefined) body.kind = p.kind;
      const res = await this.admin.put(`${HASHING_FACADE_PATH}/${seg(p.email)}`, { json: body });
      expectStatus(res, [200, 201], `PUT ${HASHING_FACADE_PATH}/${p.email} (hashing facade)`);
      if (p.extra && Object.keys(p.extra).length > 0) {
        const patch = await this.admin.patch(`${dataMount}/users/${seg(p.email)}`, {
          json: p.extra,
          contentType: "application/merge-patch+json",
        });
        expectStatus(patch, 200, `PATCH ${dataMount}/users/${p.email} (extra fields)`);
      }
      this.principals.add(p.email);
    }
    if (!hadFacade && !opts.keepFacade) {
      config.mounts = config.mounts.filter((m) => m.path !== HASHING_FACADE_PATH);
      await this.putConfig(config);
    }
  }

  /** Log in as any principal (the admin by default). */
  async login(email?: string, password?: string): Promise<string> {
    const e = env();
    return login(this.anon, email ?? e.adminEmail, password ?? e.adminPassword);
  }

  /** A client bearing a fresh token for `principal`. */
  async clientAs(principal: Pick<Principal, "email" | "password">): Promise<Rs2Client> {
    return this.anon.withToken(await this.login(principal.email, principal.password));
  }

  /** Remember a dataset to confirm-delete in `restore()`. */
  trackDataset(mount: string, dataset: string): void {
    this.datasets.push({ mount, dataset });
  }

  /** Remember a directory to confirm-delete in `restore()`. */
  trackDir(mount: string, dir: string): void {
    this.dirs.push({ mount, dir: dir.replace(/^\/|\/$/g, "") });
  }

  /**
   * Tear down: delete tracked dirs/datasets and created principals while
   * their mounts still exist, then put `conf.base.json` back. Never throws
   * on a 404 (a suite may have removed things itself).
   */
  async restore(): Promise<void> {
    const tolerate = (res: Rs2Response, what: string) => {
      if (![204, 404].includes(res.status)) {
        throw new Error(`restore: ${what}: ${res.describe()}`);
      }
    };
    for (const { mount, dir } of this.dirs.reverse()) {
      const leaf = dir.split("/").pop();
      tolerate(await this.admin.delete(`${mount}/${dir}/?confirm=${leaf}`), `DELETE ${mount}/${dir}/`);
    }
    for (const { mount, dataset } of this.datasets.reverse()) {
      tolerate(await this.admin.delete(`${mount}/${dataset}/?confirm=${dataset}`), `DELETE ${mount}/${dataset}/`);
    }
    for (const email of this.principals) {
      tolerate(await this.admin.delete(`/data/users/${seg(email)}`), `DELETE /data/users/${email}`);
    }
    this.dirs.length = 0;
    this.datasets.length = 0;
    this.principals.clear();
    // Re-read the version: a suite may have PUT the config without this
    // object (e.g. while testing `/raw` itself).
    await this.currentConfig();
    await this.putConfig(baseConfig());
  }
}
