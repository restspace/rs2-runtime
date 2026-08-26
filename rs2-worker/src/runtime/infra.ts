// Infras (PRD §9.1): operator-defined, named partial storage-adapter
// configurations. Port of `rs2-core/src/infra.rs`; the source is the
// `RegistryObject` snapshot the DO fetched at build time.

import { RsError } from "./error";
import type { Json, JsonObject } from "./error";

export interface InfraDef {
  adapter: string;
  description: string | undefined;
  allowedTenants: string[];
  config: JsonObject;
  infraOnly: string[];
  requires: string[];
}

export function adapterKind(def: InfraDef): "code" | "builtin" {
  return def.adapter.startsWith("code:") ? "code" : "builtin";
}

function strList(v: Json | undefined, what: string): string[] {
  if (v === undefined) return [];
  if (!Array.isArray(v) || !v.every((x) => typeof x === "string")) {
    throw RsError.badRequest(`invalid infras document: '${what}' must be an array of strings`);
  }
  return v as string[];
}

/// The node's set of infras (name → definition).
export class InfraSet {
  private readonly defs: Map<string, InfraDef>;

  constructor(defs?: Map<string, InfraDef>) {
    this.defs = defs ?? new Map();
  }

  /// Parse an `infras.json` document: a top-level object of name → infra.
  static fromJson(value: Json): InfraSet {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw RsError.badRequest("invalid infras document: expected an object of name → infra");
    }
    const defs = new Map<string, InfraDef>();
    for (const [name, raw] of Object.entries(value)) {
      if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
        throw RsError.badRequest(`invalid infras document: infra '${name}' must be an object`);
      }
      if (typeof raw.adapter !== "string") {
        throw RsError.badRequest(`invalid infras document: missing field \`adapter\` in infra '${name}'`);
      }
      const config = raw.config;
      if (config !== undefined && (!config || typeof config !== "object" || Array.isArray(config))) {
        throw RsError.badRequest(`invalid infras document: 'config' of infra '${name}' must be an object`);
      }
      defs.set(name, {
        adapter: raw.adapter,
        description: typeof raw.description === "string" ? raw.description : undefined,
        allowedTenants: strList(raw.allowedTenants, "allowedTenants"),
        config: (config as JsonObject | undefined) ?? {},
        infraOnly: strList(raw.infraOnly, "infraOnly"),
        requires: strList(raw.requires, "requires"),
      });
    }
    return new InfraSet(defs);
  }

  get(name: string): InfraDef | undefined {
    return this.defs.get(name);
  }
  size(): number {
    return this.defs.size;
  }
  isEmpty(): boolean {
    return this.defs.size === 0;
  }
  names(): string[] {
    return [...this.defs.keys()].sort();
  }

  /// (name, def) pairs visible to `tenant`, sorted by name.
  visibleTo(tenant: string): Array<[string, InfraDef]> {
    return [...this.defs.entries()].filter(([, d]) => infraAllows(d, tenant)).sort((a, b) => (a[0] < b[0] ? -1 : 1));
  }
}

function infraAllows(def: InfraDef, tenant: string): boolean {
  return def.allowedTenants.length === 0 || def.allowedTenants.includes(tenant);
}

/// Expand a mount's `store`-shaped sub-config: an `infra:<name>` adapter is
/// resolved into the merged config (infra wins). Anything else passes through.
export function expandInfra(store: Json, infras: InfraSet, tenant: string): Json {
  if (!store || typeof store !== "object" || Array.isArray(store)) return store;
  const adapter = store.adapter;
  if (typeof adapter !== "string" || !adapter.startsWith("infra:")) return store;
  const infraName = adapter.slice("infra:".length);
  const def = infras.get(infraName);
  if (!def) throw RsError.badRequest(`unknown infra '${infraName}' (available: ${infras.names().join(", ")})`);
  if (!infraAllows(def, tenant)) throw RsError.forbidden(`infra '${infraName}' is not available to tenant '${tenant}'`);

  const merged: JsonObject = {};
  for (const [k, v] of Object.entries(store)) if (k !== "adapter") merged[k] = v;

  const trespass = def.infraOnly.filter((f) => Object.prototype.hasOwnProperty.call(merged, f));
  if (trespass.length) {
    throw RsError.badRequest(
      `infra '${infraName}' forbids setting these adapter fields on the mount: ${trespass.join(", ")}`,
    );
  }
  for (const [k, v] of Object.entries(def.config)) merged[k] = v;
  merged.adapter = def.adapter;
  const missing = def.requires.filter((f) => !Object.prototype.hasOwnProperty.call(merged, f));
  if (missing.length) {
    throw RsError.badRequest(
      `infra '${infraName}' requires these adapter fields to be set on the mount: ${missing.join(", ")}`,
    );
  }
  return merged;
}
