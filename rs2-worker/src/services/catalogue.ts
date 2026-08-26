// External catalogue client (PRD §10.6). Port of
// `rs2-core/src/services/catalogue.rs`: host allowlist checked before I/O,
// 502 `Catalogue Fetch Failed`, 64 MiB cap.

import type { HttpOut } from "../capabilities/types";
import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { Message } from "../runtime/message";
import { hostMatches, urlHost } from "../runtime/outbound";

const CATALOGUE_CAP = 64 * 1024 * 1024;

export interface CatalogueItem {
  name: string;
  version: string;
  engine: string;
  bundleUrl: string;
  raw: JsonObject;
}

export interface CatalogueDoc {
  items: CatalogueItem[];
}

export interface CatalogueClient {
  hostAllowed(url: string): boolean;
  fetchCatalogue(url: string): Promise<CatalogueDoc>;
  fetchBundle(url: string): Promise<Uint8Array>;
}

export class HttpCatalogueClient implements CatalogueClient {
  constructor(
    private readonly http: HttpOut,
    private readonly hosts: string[],
  ) {}

  hostAllowed(url: string): boolean {
    const host = urlHost(url);
    return host !== undefined && this.hosts.some((p) => hostMatches(p, host));
  }

  private async fetchBytes(url: string): Promise<Uint8Array> {
    const host = urlHost(url);
    if (host === undefined) throw RsError.badRequest(`catalogue URL '${url}' must be absolute`);
    if (!this.hosts.some((p) => hostMatches(p, host))) {
      throw RsError.capabilityDenied(`catalogue fetch to '${host}'`);
    }
    const resp = await this.http.request(Message.request("GET", url, "-"));
    if (!resp.isOk()) {
      throw new RsError(502, codes.INTERNAL, "Catalogue Fetch Failed", `'${url}' returned status ${resp.status ?? 0}`);
    }
    if (!resp.body) return new Uint8Array(0);
    return resp.body.materialize(CATALOGUE_CAP);
  }

  async fetchCatalogue(url: string): Promise<CatalogueDoc> {
    const bytes = await this.fetchBytes(url);
    let doc: Json;
    try {
      doc = JSON.parse(new TextDecoder().decode(bytes)) as Json;
    } catch (e) {
      throw new RsError(502, codes.INTERNAL, "Catalogue Fetch Failed", `'${url}' is not valid JSON: ${String(e)}`);
    }
    const items = doc && typeof doc === "object" && !Array.isArray(doc) && Array.isArray(doc.items) ? doc.items : [];
    const out: CatalogueItem[] = [];
    for (const it of items) {
      if (!it || typeof it !== "object" || Array.isArray(it)) continue;
      const name = typeof it.name === "string" ? it.name : undefined;
      const version = typeof it.version === "string" ? it.version : undefined;
      const bundleUrl = typeof it.bundleUrl === "string" ? it.bundleUrl : undefined;
      if (name === undefined || version === undefined || bundleUrl === undefined) continue;
      out.push({ name, version, engine: typeof it.engine === "string" ? it.engine : "wasm", bundleUrl, raw: it });
    }
    return { items: out };
  }

  fetchBundle(url: string): Promise<Uint8Array> {
    return this.fetchBytes(url);
  }
}
