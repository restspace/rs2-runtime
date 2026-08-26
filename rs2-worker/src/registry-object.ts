// `RegistryObject`: the operator table (cloudflare.md §B.1, §B.5) — the
// domain map, the tenant list, and the `infras.json` equivalent. One
// instance (`idFromName("registry")`), SQLite-backed KV.

import { DurableObject } from "cloudflare:workers";
import type { Env } from "./env";
import { sha256Hex } from "./runtime/crypto";
import { RsError } from "./runtime/error";
import type { Json, JsonObject } from "./runtime/error";
import { InfraSet } from "./runtime/infra";

export interface TenantEntry {
  domains: string[];
  configVersion: string;
}

/// What the Worker caches per isolate for hostname resolution.
export interface RegistrySnapshot {
  domainMap: Record<string, string>;
  infrasVersion: string;
}

type TenantTable = Record<string, TenantEntry>;

export class RegistryObject extends DurableObject<Env> {
  private async domains(): Promise<Record<string, string>> {
    return (await this.ctx.storage.get<Record<string, string>>("domains")) ?? {};
  }

  private async tenants(): Promise<TenantTable> {
    return (await this.ctx.storage.get<TenantTable>("tenants")) ?? {};
  }

  async snapshot(): Promise<RegistrySnapshot> {
    return {
      domainMap: await this.domains(),
      infrasVersion: (await this.ctx.storage.get<string>("infrasVersion")) ?? "0",
    };
  }

  /// The infras document (as JSON text — RPC types stay shallow) and its
  /// version (the DO snapshots it at build time).
  async getInfras(): Promise<{ infrasText: string; version: string }> {
    return {
      infrasText: JSON.stringify((await this.ctx.storage.get<Json>("infras")) ?? {}),
      version: (await this.ctx.storage.get<string>("infrasVersion")) ?? "0",
    };
  }

  /// Store the `infras.json` document (validated); bumps the version.
  async putInfras(docText: string): Promise<string> {
    const doc = JSON.parse(docText) as Json;
    InfraSet.fromJson(doc);
    const version = (await sha256Hex(docText)).slice(0, 16);
    await this.ctx.storage.put({ infras: doc, infrasVersion: version });
    return version;
  }

  /// `POST /admin/reload-infras`: re-snapshot (a new version so every built
  /// tenant rebuilds on its next request) and report the names.
  async reloadInfras(): Promise<string[]> {
    const doc = (await this.ctx.storage.get<Json>("infras")) ?? {};
    const set = InfraSet.fromJson(doc);
    const version = `${(await sha256Hex(JSON.stringify(doc))).slice(0, 12)}${Date.now().toString(16).slice(-4)}`;
    await this.ctx.storage.put("infrasVersion", version);
    return set.names();
  }

  async listTenants(): Promise<Array<{ name: string; domains: string[]; configVersion: string }>> {
    const t = await this.tenants();
    return Object.keys(t)
      .sort()
      .map((name) => ({ name, domains: t[name]!.domains, configVersion: t[name]!.configVersion }));
  }

  /// Register (or re-register) a tenant with its domains.
  async upsertTenant(name: string, domains: string[], configVersion: string): Promise<void> {
    const tenants = await this.tenants();
    const map = await this.domains();
    const previous = tenants[name]?.domains ?? [];
    for (const d of previous) if (map[d] === name) delete map[d];
    const normalized = domains.map((d) => d.toLowerCase());
    for (const d of normalized) map[d] = name;
    tenants[name] = { domains: normalized, configVersion };
    await this.ctx.storage.put({ tenants, domains: map });
  }

  async deleteTenant(name: string): Promise<void> {
    const tenants = await this.tenants();
    const map = await this.domains();
    for (const [host, t] of Object.entries(map)) if (t === name) delete map[host];
    delete tenants[name];
    await this.ctx.storage.put({ tenants, domains: map });
    await this.noteScheduled(name, false);
  }

  /// The tenant a host maps to, if registered.
  async getDomain(host: string): Promise<string | undefined> {
    return (await this.domains())[host.toLowerCase()];
  }

  async putDomain(host: string, tenant: string): Promise<void> {
    const map = await this.domains();
    const tenants = await this.tenants();
    const h = host.toLowerCase();
    map[h] = tenant;
    if (tenants[tenant] && !tenants[tenant].domains.includes(h)) tenants[tenant].domains.push(h);
    await this.ctx.storage.put({ domains: map, tenants });
  }

  async deleteDomain(host: string): Promise<void> {
    const map = await this.domains();
    const tenants = await this.tenants();
    const h = host.toLowerCase();
    const owner = map[h];
    delete map[h];
    if (owner !== undefined && tenants[owner]) {
      tenants[owner].domains = tenants[owner].domains.filter((d) => d !== h);
    }
    await this.ctx.storage.put({ domains: map, tenants });
  }

  /// The tenants whose configs carry scheduled mounts (§B.6): the cron
  /// safety net reconciles only these instead of fanning out over every
  /// tenant. Each TenantObject reports its own flag on config writes.
  async noteScheduled(name: string, scheduled: boolean): Promise<void> {
    const set = (await this.ctx.storage.get<Record<string, true>>("scheduled")) ?? {};
    if (scheduled === (set[name] === true)) return;
    if (scheduled) set[name] = true;
    else delete set[name];
    await this.ctx.storage.put("scheduled", set);
  }

  async scheduledTenants(): Promise<string[]> {
    const set = (await this.ctx.storage.get<Record<string, true>>("scheduled")) ?? {};
    return Object.keys(set).sort();
  }

  /// Record a tenant's current config version (after `PUT /services/raw`).
  async noteConfigVersion(name: string, configVersion: string): Promise<void> {
    const tenants = await this.tenants();
    if (tenants[name]) {
      tenants[name].configVersion = configVersion;
      await this.ctx.storage.put("tenants", tenants);
    }
  }
}

export function ensureObject(v: Json, what: string): JsonObject {
  if (!v || typeof v !== "object" || Array.isArray(v)) throw RsError.badRequest(`${what} must be a JSON object`);
  return v;
}
