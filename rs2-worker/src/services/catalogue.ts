// External service/adapter catalogues (control plane). Port of
// `rs2-core/src/services/catalogue.rs`: the client is a HOST capability
// handed only to the `services` self-config mount, fetching catalogue
// documents and code bundles from operator-allowlisted hosts (an SSRF
// bound). The allowlist is checked before any I/O; a non-2xx upstream is
// 502 `Catalogue Fetch Failed`; reads are capped at 64 MiB.

import type { HttpOut } from "../capabilities/types";
import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { Message } from "../runtime/message";
import { hostMatches, urlHost } from "../runtime/outbound";

const CATALOGUE_CAP = 64 * 1024 * 1024;

/// One installable service or adapter advertised by a catalogue. Mirrors
/// `catalogue.rs`'s `CatalogueItem` (serde camelCase); `raw` is the item's
/// serde-shaped serialization — required fields plus the present optionals,
/// in struct order — for listings that embed the whole item.
export interface CatalogueItem {
  name: string;
  kind: string;
  adapterKind?: string;
  engine: string;
  version: string;
  bundleUrl: string;
  description?: string;
  endpoints: Json;
  capabilities: Json;
  configSchema: Json;
  raw: JsonObject;
}

export interface CatalogueDoc {
  items: CatalogueItem[];
}

export interface CatalogueClient {
  /// Whether `url`'s host is on the operator allowlist (no I/O).
  hostAllowed(url: string): boolean;
  /// Fetch and parse a catalogue document.
  fetchCatalogue(url: string): Promise<CatalogueDoc>;
  /// Fetch raw bundle bytes.
  fetchBundle(url: string): Promise<Uint8Array>;
}

/// Serde-faithful item parse: required string fields error like serde's
/// `missing field`/`invalid type`; optional ones accept absent or null.
function parseItem(value: Json): CatalogueItem {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid type: catalogue item must be an object");
  }
  const required = (k: string): string => {
    const v = value[k];
    if (v === undefined) throw new Error(`missing field \`${k}\``);
    if (typeof v !== "string") throw new Error(`invalid type for field \`${k}\`: expected a string`);
    return v;
  };
  const optionalString = (k: string): string | undefined => {
    const v = value[k];
    if (v === undefined || v === null) return undefined;
    if (typeof v !== "string") throw new Error(`invalid type for field \`${k}\`: expected a string`);
    return v;
  };
  const item: CatalogueItem = {
    name: required("name"),
    kind: required("kind"),
    adapterKind: optionalString("adapterKind"),
    engine: required("engine"),
    version: required("version"),
    bundleUrl: required("bundleUrl"),
    description: optionalString("description"),
    endpoints: value.endpoints ?? null,
    capabilities: value.capabilities ?? null,
    configSchema: value.configSchema ?? null,
    raw: {},
  };
  // `serde_json::to_value(&item)`: struct order, `Option::is_none` and
  // `Value::is_null` fields skipped, unknown source keys dropped.
  const raw: JsonObject = { name: item.name, kind: item.kind };
  if (item.adapterKind !== undefined) raw.adapterKind = item.adapterKind;
  raw.engine = item.engine;
  raw.version = item.version;
  raw.bundleUrl = item.bundleUrl;
  if (item.description !== undefined) raw.description = item.description;
  if (item.endpoints !== null) raw.endpoints = item.endpoints;
  if (item.capabilities !== null) raw.capabilities = item.capabilities;
  if (item.configSchema !== null) raw.configSchema = item.configSchema;
  item.raw = raw;
  return item;
}

/// The real client: a bounded outbound `GET` over `HttpOut`, gated by the
/// operator host-allowlist (wildcard patterns) before any I/O.
export class HttpCatalogueClient implements CatalogueClient {
  constructor(
    private readonly http: HttpOut,
    private readonly hosts: string[],
  ) {}

  private checkHost(url: string): void {
    const host = urlHost(url);
    if (host === undefined) throw RsError.badRequest(`catalogue URL '${url}' has no host`);
    if (!this.hosts.some((p) => hostMatches(p, host))) {
      throw RsError.capabilityDenied(`catalogue fetch to '${host}'`);
    }
  }

  hostAllowed(url: string): boolean {
    try {
      this.checkHost(url);
      return true;
    } catch {
      return false;
    }
  }

  private async getBytes(url: string): Promise<Uint8Array> {
    this.checkHost(url);
    const resp = await this.http.request(Message.request("GET", url, "-"));
    const status = resp.status ?? 502;
    if (!(status >= 200 && status < 300)) {
      throw new RsError(502, codes.CONTRACT_VIOLATION, "Catalogue Fetch Failed", `fetching '${url}' returned ${status}`);
    }
    if (!resp.body) return new Uint8Array(0);
    return resp.body.materialize(CATALOGUE_CAP);
  }

  async fetchCatalogue(url: string): Promise<CatalogueDoc> {
    const bytes = await this.getBytes(url);
    try {
      const doc = JSON.parse(new TextDecoder().decode(bytes)) as Json;
      if (!doc || typeof doc !== "object" || Array.isArray(doc)) {
        throw new Error("invalid type: expected a catalogue document object");
      }
      const items = doc.items === undefined ? [] : doc.items;
      if (!Array.isArray(items)) throw new Error("invalid type for field `items`: expected an array");
      return { items: items.map(parseItem) };
    } catch (e) {
      throw RsError.badRequest(
        `catalogue '${url}' is not a valid catalogue document: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  fetchBundle(url: string): Promise<Uint8Array> {
    return this.getBytes(url);
  }
}
