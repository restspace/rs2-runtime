// `services` — the self-configuration API (PRD §10.6). Port of
// `rs2-core/src/services/services_config.rs`: `catalogue`, `catalogues`,
// `catalogue/available`, `catalogue/install`, `services`, `infras`, `raw`
// GET/PUT (409 on `If-Match`), and the store-patterned `code/` subtree.

import type { ScopedFileStore } from "../capabilities/scoped";
import { dirEntryJson } from "../capabilities/types";
import { Body } from "../runtime/body";
import { catalogue } from "../runtime/config-schema";
import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { adapterKind } from "../runtime/infra";
import { MediaType } from "../runtime/media-type";
import type { Message } from "../runtime/message";
import { urlHost } from "../runtime/outbound";
import { CODE_PREFIX, versionOf } from "./code";
import type { DynamicWorkerEngine } from "../engines/dynamic-worker";
import { etagVersion, pagination } from "./context";
import type { Service, ServiceContext, TenantControl } from "./context";

/// The placeholder secrets read back as. A PUT carrying it means "keep the stored value".
export const SECRET_MASK = "<secret>";

function maskLeaves(value: Json): Json {
  if (typeof value === "string") return SECRET_MASK;
  if (Array.isArray(value)) return value.map(maskLeaves);
  if (value && typeof value === "object") {
    const out: JsonObject = {};
    for (const [k, v] of Object.entries(value)) out[k] = maskLeaves(v);
    return out;
  }
  return value;
}

/// Mask secret values: `auth.jwtSecret`, plus every string leaf under `secrets`.
export function redactSecrets(config: JsonObject): JsonObject {
  const out: JsonObject = { ...config };
  const auth = out.auth;
  if (auth && typeof auth === "object" && !Array.isArray(auth) && typeof auth.jwtSecret === "string") {
    out.auth = { ...auth, jwtSecret: SECRET_MASK };
  }
  if (out.secrets !== undefined) out.secrets = maskLeaves(out.secrets);
  return out;
}

function hasMask(value: Json): boolean {
  if (typeof value === "string") return value === SECRET_MASK;
  if (Array.isArray(value)) return value.some(hasMask);
  if (value && typeof value === "object") return Object.values(value).some(hasMask);
  return false;
}

function restoreAt(incoming: Json, current: Json): Json {
  if (typeof incoming === "string" && incoming === SECRET_MASK) {
    return typeof current === "string" ? current : incoming;
  }
  if (Array.isArray(incoming)) {
    return incoming.map((v, i) => restoreAt(v, Array.isArray(current) ? (current[i] ?? null) : null));
  }
  if (incoming && typeof incoming === "object") {
    const out: JsonObject = {};
    const cur = current && typeof current === "object" && !Array.isArray(current) ? current : {};
    for (const [k, v] of Object.entries(incoming)) out[k] = restoreAt(v, cur[k] ?? null);
    return out;
  }
  return incoming;
}

/// Replace `SECRET_MASK` markers with the stored values at the same locations.
export function restoreSecrets(incoming: JsonObject, current: Json): JsonObject {
  const restored = restoreAt(incoming, current) as JsonObject;
  if (hasMask(restored)) {
    throw RsError.badRequest(
      `config contains the secret placeholder '${SECRET_MASK}' at a location with no stored value to restore — supply the real value there`,
    );
  }
  return restored;
}

function stem(version: string): string {
  return version.replace(/\.wasm$/, "").replace(/\.js$/, "");
}

/// Versions the live config references for a bundle name: `[path, version]`.
function mounted(config: JsonObject, name: string): Array<[string, string]> {
  const wanted = `code:${name}@`;
  const mounts = Array.isArray(config.mounts) ? config.mounts : [];
  const out: Array<[string, string]> = [];
  for (const m of mounts) {
    if (!m || typeof m !== "object" || Array.isArray(m)) continue;
    if (typeof m.service !== "string" || !m.service.startsWith(wanted)) continue;
    if (typeof m.path !== "string") continue;
    out.push([m.path, m.service.slice(wanted.length)]);
  }
  return out;
}

export class ServicesService implements Service {
  constructor(private readonly engine: DynamicWorkerEngine | undefined = undefined) {}

  private async handleCodeStore(
    msg: Message,
    ctx: ServiceContext,
    control: TenantControl,
    rest: string[],
  ): Promise<Message> {
    if (!ctx.files) throw RsError.internal("services service has no file capability");
    const files = ctx.files.prefixed(CODE_PREFIX);
    const m = msg.method;

    if (m === "GET" && rest.length <= 1) {
      const dirPath = rest.length === 0 ? "/" : `/${rest[0]}/`;
      const [take, skip] = pagination(msg);
      let entries: JsonObject[];
      let total: number;
      try {
        const [es, t] = await files.list(dirPath, take, skip);
        entries = es.map(dirEntryJson);
        total = t;
      } catch (e) {
        if (e instanceof RsError && e.code === codes.NOT_FOUND && rest.length === 0) {
          entries = [];
          total = 0;
        } else if (e instanceof RsError && e.code === codes.NOT_FOUND) {
          throw RsError.notFound(`no deployed code '${rest[0]}'`);
        } else {
          throw e;
        }
      }
      if (rest.length === 1) {
        try {
          const [config] = await control.rawConfig(msg.tenant);
          const live = mounted(config, rest[0]!);
          for (const entry of entries) {
            const s = stem(typeof entry.name === "string" ? entry.name : "");
            const at = live.filter(([, v]) => v === s).map(([p]) => p);
            if (at.length) entry.mountedAt = at;
          }
        } catch {
          /* annotation is best-effort */
        }
      }
      const resp = msg.ok(Body.fromString(JSON.stringify({ path: dirPath, entries, total }), MediaType.dirJson()));
      resp.setHeader("x-total-count", String(total));
      return resp;
    }

    if (m === "GET" && rest.length === 2) {
      const [name, version] = rest as [string, string];
      const s = stem(version);
      const candidates: Array<[string, string | undefined]> = [
        [`/${name}/${version}`, undefined],
        [`/${name}/${s}.wasm`, "application/wasm"],
        [`/${name}/${s}.js`, "application/javascript"],
      ];
      for (const [path, forced] of candidates) {
        let body: Body;
        try {
          body = await files.read(path, undefined);
        } catch {
          continue;
        }
        body.mediaType = forced ? new MediaType(forced) : MediaType.forPath(path);
        const resp = msg.ok(body);
        resp.setHeader("etag", `"${s}"`);
        resp.setHeader("cache-control", "private, max-age=31536000, immutable");
        return resp;
      }
      throw RsError.notFound(`no deployed code '${name}@${version}'`);
    }

    if (m === "POST" && rest.length === 1) {
      const name = rest[0]!;
      if (name === "" || /[/\\.]/.test(name)) throw RsError.badRequest("invalid code bundle name");
      const manifest = msg.header("x-rs2-manifest");
      const [bytes, isJs, validated] = await this.validateBundle(msg, ctx);
      const [version, child] = await ServicesService.storeBundle(files, name, bytes, isJs);
      if (manifest !== undefined) await ServicesService.storeManifest(files, name, version, manifest);
      const resp = msg.response(
        201,
        Body.fromJson({ name, version, ref: `code:${name}@${version}`, validated }),
      );
      resp.setHeader("location", `${msg.url.basePath}/code/${name}/${child}`);
      return resp;
    }

    if (m === "PUT" && rest.length === 2) {
      const [name, version] = rest as [string, string];
      const s = stem(version);
      const manifest = msg.header("x-rs2-manifest");
      const [bytes, isJs] = await this.validateBundle(msg, ctx);
      const computed = await versionOf(bytes);
      if (computed !== s) {
        throw RsError.conflict(
          `versions are content-addressed: these bytes are '${computed}', not '${s}' — POST ${msg.url.basePath}/code/${name}/ to deploy them`,
        );
      }
      if (manifest !== undefined) await ServicesService.storeManifest(files, name, s, manifest);
      const child = isJs ? `${s}.js` : `${s}.wasm`;
      const created = await files.write(`/${name}/${child}`, Body.fromBytes(bytes, MediaType.forPath(child)));
      const resp = msg.response(created ? 201 : 200, undefined);
      resp.setHeader("etag", `"${s}"`);
      return resp;
    }

    if (m === "DELETE" && rest.length === 2) {
      const [name, version] = rest as [string, string];
      const s = stem(version);
      const [config] = await control.rawConfig(msg.tenant);
      const live = mounted(config, name).find(([, v]) => v === s);
      if (live) throw RsError.conflict(`version '${s}' is mounted at '${live[0]}' — repoint the mount first`);
      for (const candidate of [`/${name}/${version}`, `/${name}/${s}.wasm`, `/${name}/${s}.js`]) {
        try {
          await files.delete(candidate);
        } catch {
          continue;
        }
        await files.delete(`/${name}/${s}.manifest.json`).catch(() => undefined);
        return msg.noContent();
      }
      throw RsError.notFound(`no deployed code '${name}@${s}'`);
    }

    if (m === "DELETE" && rest.length === 1) {
      const name = rest[0]!;
      const [config] = await control.rawConfig(msg.tenant);
      const live = mounted(config, name)[0];
      if (live) throw RsError.conflict(`version '${live[1]}' is mounted at '${live[0]}' — repoint the mount first`);
      if (msg.url.queryParam("confirm") === name) await files.deleteDirAll(`/${name}`);
      else await files.deleteDir(`/${name}`);
      return msg.noContent();
    }

    throw RsError.notFound(
      "code store: GET listings/bundles, POST <name>/ deploys, PUT <name>/<version> (content-addressed), DELETE <name>/<version> | <name>/?confirm=",
    );
  }

  /// Materialize and smoke-test an uploaded bundle: `[bytes, isJs, validated]`.
  private async validateBundle(msg: Message, ctx: ServiceContext): Promise<[Uint8Array, boolean, boolean]> {
    const isJs = msg.body?.mediaType.essence().includes("javascript") ?? false;
    if (!msg.body) throw RsError.badRequest("deploying requires a bundle body");
    const bytes = await msg.body.materialize(ctx.limits.materializedBodyBytes);
    const validated = await this.compileCheckBytes(bytes, isJs);
    return [bytes, isJs, validated];
  }

  /// Engine compile smoke test for bundle bytes; returns whether validation
  /// actually ran (`compile_check_bytes` in Rust — false when the matching
  /// engine is absent: always for `.wasm` here, and for JS without a
  /// `worker_loaders` binding). Module evaluation errors → 502.
  private async compileCheckBytes(bytes: Uint8Array, isJs: boolean): Promise<boolean> {
    if (!isJs) return false; // no wasm engine on this host (§A)
    let source: string;
    try {
      source = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes);
    } catch {
      throw RsError.badRequest("JS bundle is not valid UTF-8");
    }
    if (!this.engine) return false;
    await this.engine.compileCheck(source, await versionOf(bytes));
    return true;
  }

  private static async storeBundle(
    files: ScopedFileStore,
    name: string,
    bytes: Uint8Array,
    isJs: boolean,
  ): Promise<[string, string]> {
    const version = await versionOf(bytes);
    const [child, mediaType] = isJs ? [`${version}.js`, "application/javascript"] : [`${version}.wasm`, "application/wasm"];
    await files.write(`/${name}/${child}`, Body.fromBytes(bytes, new MediaType(mediaType)));
    return [version, child];
  }

  private static async storeManifest(files: ScopedFileStore, name: string, version: string, manifest: string): Promise<void> {
    let value: Json;
    try {
      value = JSON.parse(manifest) as Json;
    } catch (e) {
      throw RsError.badRequest(`X-RS2-Manifest is not valid JSON: ${e instanceof Error ? e.message : String(e)}`);
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw RsError.badRequest("X-RS2-Manifest must be a JSON object");
    }
    await files.write(`/${name}/${version}.manifest.json`, Body.fromJson(value));
  }

  private async listCatalogues(msg: Message, ctx: ServiceContext, control: TenantControl): Promise<Message> {
    const [config] = await control.rawConfig(msg.tenant);
    const entries: Json[] = [];
    for (const c of Array.isArray(config.catalogues) ? config.catalogues : []) {
      if (!c || typeof c !== "object" || Array.isArray(c)) continue;
      if (typeof c.name !== "string" || typeof c.url !== "string") continue;
      entries.push({
        name: c.name,
        url: c.url,
        host: urlHost(c.url) ?? null,
        allowlisted: ctx.catalogue ? ctx.catalogue.hostAllowed(c.url) : false,
      });
    }
    return msg.okJson({ catalogues: entries, enabled: ctx.catalogue !== undefined });
  }

  private async listAvailable(msg: Message, ctx: ServiceContext, control: TenantControl): Promise<Message> {
    const items: Json[] = [];
    const builtin = catalogue();
    for (const s of Array.isArray(builtin.services) ? builtin.services : []) {
      if (!s || typeof s !== "object" || Array.isArray(s)) continue;
      items.push({
        name: s.name ?? null,
        kind: "service",
        source: "builtin",
        description: s.description ?? null,
        configSchema: s.configSchema ?? null,
      });
    }
    const reg = ctx.builtinAdapters;
    if (reg) {
      for (const [adapterKindName, names] of [
        ["data", reg.dataNames()],
        ["file", reg.filesNames()],
        ["query", reg.queryNames()],
        ["message", reg.messageNames()],
      ] as Array<[string, string[]]>) {
        for (const name of names) {
          items.push({ name, kind: "adapter", adapterKind: adapterKindName, source: "builtin", ref: `builtin:${name}` });
        }
      }
    }
    const client = ctx.catalogue;
    if (client) {
      const [config] = await control.rawConfig(msg.tenant);
      const codeStore = ctx.files?.prefixed(CODE_PREFIX);
      for (const c of Array.isArray(config.catalogues) ? config.catalogues : []) {
        if (!c || typeof c !== "object" || Array.isArray(c)) continue;
        if (typeof c.name !== "string" || typeof c.url !== "string") continue;
        try {
          const doc = await client.fetchCatalogue(c.url);
          for (const item of doc.items) {
            let installed = false;
            if (codeStore) {
              const ext = item.engine === "js" ? "js" : "wasm";
              installed = await codeStore
                .head(`/${item.name}/${item.version}.${ext}`)
                .then(() => true)
                .catch(() => false);
            }
            items.push({
              ...item.raw,
              catalogue: c.name,
              source: "catalogue",
              ref: `code:${item.name}@${item.version}`,
              installed,
            });
          }
        } catch (e) {
          items.push({ catalogue: c.name, source: "catalogue", error: e instanceof RsError ? e.detail : String(e) });
        }
      }
    }
    return msg.okJson({ items });
  }

  private async installFromCatalogue(msg: Message, ctx: ServiceContext, control: TenantControl): Promise<Message> {
    const client = ctx.catalogue;
    if (!client) {
      throw RsError.engineUnavailable("external catalogues are not enabled on this node (no operator host allowlist)");
    }
    if (!msg.body) throw RsError.badRequest("install requires a JSON body { catalogue, name, version }");
    const body = await msg.body.asJson(ctx.limits.materializedBodyBytes);
    const b = body && typeof body === "object" && !Array.isArray(body) ? body : {};
    const field = (k: string): string => {
      const v = b[k];
      if (typeof v !== "string") throw RsError.badRequest(`install requires '${k}'`);
      return v;
    };
    const catName = field("catalogue");
    const itemName = field("name");
    const itemVersion = field("version");
    const [config] = await control.rawConfig(msg.tenant);
    const cats = Array.isArray(config.catalogues) ? config.catalogues : [];
    const cat = cats.find((c) => c && typeof c === "object" && !Array.isArray(c) && c.name === catName);
    const url = cat && typeof cat === "object" && !Array.isArray(cat) && typeof cat.url === "string" ? cat.url : undefined;
    if (url === undefined) throw RsError.notFound(`no registered catalogue '${catName}'`);
    const doc = await client.fetchCatalogue(url);
    const item = doc.items.find((i) => i.name === itemName && i.version === itemVersion);
    if (!item) throw RsError.notFound(`catalogue '${catName}' has no item '${itemName}@${itemVersion}'`);
    if (item.name === "" || /[/\\.]/.test(item.name)) throw RsError.badRequest(`invalid code bundle name '${item.name}'`);
    const bytes = await client.fetchBundle(item.bundleUrl);
    const computed = await versionOf(bytes);
    if (computed !== item.version) {
      throw RsError.badRequest(
        `bundle content hash '${computed}' does not match catalogue version '${item.version}' for '${itemName}' — refusing`,
      );
    }
    const isJs = item.engine === "js";
    if (!ctx.files) throw RsError.internal("services service has no file capability");
    const files = ctx.files.prefixed(CODE_PREFIX);
    const validated = await this.compileCheckBytes(bytes, isJs);
    const [version] = await ServicesService.storeBundle(files, item.name, bytes, isJs);
    return msg.response(
      201,
      Body.fromJson({ name: item.name, version, ref: `code:${item.name}@${version}`, validated }),
    );
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const segments = msg.url.serviceSegments();
    const control = ctx.control;
    if (!control) throw RsError.internal("services service has no control capability");

    if (segments[0] === "code") return this.handleCodeStore(msg, ctx, control, segments.slice(1));

    const m = msg.method;
    const key = segments.join("/");
    if (m === "GET" && key === "catalogue") return msg.okJson(catalogue());
    if (m === "GET" && key === "catalogues") return this.listCatalogues(msg, ctx, control);
    if (m === "GET" && key === "catalogue/available") return this.listAvailable(msg, ctx, control);
    if (m === "POST" && key === "catalogue/install") return this.installFromCatalogue(msg, ctx, control);

    if (m === "GET" && key === "services") {
      const [config] = await control.rawConfig(msg.tenant);
      const mounts: Json[] = (Array.isArray(config.mounts) ? config.mounts : []).map((mt) => {
        const o = mt && typeof mt === "object" && !Array.isArray(mt) ? mt : {};
        const cfg = o.config && typeof o.config === "object" && !Array.isArray(o.config) ? o.config : {};
        return { path: o.path ?? null, service: o.service ?? null, access: cfg.access ?? null };
      });
      return msg.okJson({ mounts });
    }

    if (m === "GET" && key === "infras") {
      const infras = ctx.infras;
      if (!infras) throw RsError.internal("services service has no infra grant");
      const items: Json[] = infras.visibleTo(msg.tenant).map(([name, def]) => ({
        name,
        description: def.description ?? null,
        adapterKind: adapterKind(def),
        providedKeys: Object.keys(def.config).sort(),
        requires: def.requires,
        infraOnly: def.infraOnly,
      }));
      return msg.okJson({ infras: items });
    }

    if (m === "GET" && key === "raw") {
      const [config, version] = await control.rawConfig(msg.tenant);
      const resp = msg.okJson(redactSecrets(config));
      resp.setHeader("etag", `"${version}"`);
      return resp;
    }

    if (m === "PUT" && key === "raw") {
      if (!msg.body) throw RsError.badRequest("PUT /raw requires a JSON body");
      const body = await msg.body.asJson(ctx.limits.materializedBodyBytes);
      let current: Json = null;
      try {
        [current] = await control.rawConfig(msg.tenant);
      } catch {
        current = null;
      }
      const incoming = body && typeof body === "object" && !Array.isArray(body) ? body : undefined;
      // A non-object config fails the tenant-config parse the same way it
      // does in Rust (`invalid tenant config: …`).
      const restored = incoming ? restoreSecrets(incoming, current) : undefined;
      const ifMatchRaw = msg.header("if-match");
      const ifMatch = ifMatchRaw !== undefined ? etagVersion(ifMatchRaw) : undefined;
      if (!restored) throw RsError.badRequest("invalid tenant config: expected an object");
      const version = await control.putConfig(msg.tenant, restored, ifMatch);
      const resp = msg.noContent();
      resp.setHeader("etag", `"${version}"`);
      return resp;
    }

    throw RsError.notFound(
      `services endpoint '${msg.url.servicePath}' (have: GET catalogue/services/infras/raw, PUT raw, store-patterned code/ subtree)`,
    );
  }
}
