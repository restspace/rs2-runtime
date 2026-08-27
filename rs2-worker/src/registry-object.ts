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

/// A domain claimed by a tenant but not yet proven, so not yet routing
/// (§B.5): it holds the claim, the challenge token the `manual` provider
/// serves, and which provider is verifying it.
export interface PendingAttachment {
  tenant: string;
  token: string;
  provider: string;
  requestedAt: number;
}

type PendingTable = Record<string, PendingAttachment>;

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
    const waiting = await this.pending();
    for (const [host, t] of Object.entries(map)) if (t === name) delete map[host];
    // A claim outlives its tenant otherwise, and would block a re-attach.
    for (const [host, entry] of Object.entries(waiting)) if (entry.tenant === name) delete waiting[host];
    delete tenants[name];
    await this.ctx.storage.put({ tenants, domains: map, pendingDomains: waiting });
    await this.noteScheduled(name, false);
  }

  /// The tenant a host maps to, if registered.
  async getDomain(host: string): Promise<string | undefined> {
    return (await this.domains())[host.toLowerCase()];
  }

  private async pending(): Promise<PendingTable> {
    return (await this.ctx.storage.get<PendingTable>("pendingDomains")) ?? {};
  }

  /// Record an attachment that is not routing yet. Claimed by one tenant, so
  /// a second tenant asking for the same host is refused rather than
  /// silently overwriting a claim someone is mid-way through proving.
  async putPending(host: string, tenant: string, token: string, provider: string): Promise<void> {
    const table = await this.pending();
    table[host.toLowerCase()] = { tenant, token, provider, requestedAt: Date.now() };
    await this.ctx.storage.put("pendingDomains", table);
  }

  async getPending(host: string): Promise<PendingAttachment | undefined> {
    return (await this.pending())[host.toLowerCase()];
  }

  /// Every attachment still waiting on DNS — what the cron reconciles.
  async listPending(): Promise<Array<PendingAttachment & { host: string }>> {
    const table = await this.pending();
    return Object.keys(table)
      .sort()
      .map((host) => ({ host, ...table[host]! }));
  }

  async deletePending(host: string): Promise<void> {
    const table = await this.pending();
    if (table[host.toLowerCase()] === undefined) return;
    delete table[host.toLowerCase()];
    await this.ctx.storage.put("pendingDomains", table);
  }

  /// The gate closing: a proven attachment becomes routing. Returns the
  /// tenant it went live for, or `undefined` if nothing was pending (a
  /// concurrent activation won, or the operator deleted it mid-flight).
  async activatePending(host: string): Promise<string | undefined> {
    const h = host.toLowerCase();
    const entry = (await this.pending())[h];
    if (entry === undefined) return undefined;
    await this.putDomain(h, entry.tenant);
    await this.deletePending(h);
    return entry.tenant;
  }

  /// Every host this registry knows, live or not — the domains list endpoint.
  async listDomains(): Promise<Array<{ host: string; tenant: string; status: "active" | "pending" }>> {
    const live = Object.entries(await this.domains()).map(([host, tenant]) => ({
      host,
      tenant,
      status: "active" as const,
    }));
    const waiting = Object.entries(await this.pending()).map(([host, entry]) => ({
      host,
      tenant: entry.tenant,
      status: "pending" as const,
    }));
    return [...live, ...waiting].sort((a, b) => (a.host < b.host ? -1 : a.host > b.host ? 1 : 0));
  }

  async putDomain(host: string, tenant: string): Promise<void> {
    const map = await this.domains();
    const tenants = await this.tenants();
    const h = host.toLowerCase();
    map[h] = tenant;
    if (tenants[tenant] && !tenants[tenant].domains.includes(h)) tenants[tenant].domains.push(h);
    await this.ctx.storage.put({ domains: map, tenants });
  }

  /// Detach a host: both the live mapping and any unproven claim on it.
  async deleteDomain(host: string): Promise<void> {
    const map = await this.domains();
    const tenants = await this.tenants();
    const h = host.toLowerCase();
    await this.deletePending(h);
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
