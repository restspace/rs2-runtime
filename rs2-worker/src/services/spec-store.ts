// The spec-store façade: file-like authoring for instruction-plane stores
// (pipelines, queries, templates), DRY over the one real store
// implementation. Port of `rs2-core/src/services/spec_store.rs`. P2 ships
// the validators as identity (cloudflare.md §H).

import { PrefixedFileStore } from "../capabilities/prefixed";
import { ScopedFileStore } from "../capabilities/scoped";
import type { FileStore } from "../capabilities/types";
import { Body } from "../runtime/body";
import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { NullLogStore, ServiceLogger } from "../runtime/logging";
import { MediaType } from "../runtime/media-type";
import type { Message } from "../runtime/message";
import { CachePolicy, CorsPolicy, isOperator } from "../runtime/wrapper";
import type { InvocationLimits } from "../runtime/wrapper";
import type { ServiceContext } from "./context";
import { FileService } from "./file";

/// Validates a spec document at write time; returns the canonical form.
export type SpecValidator = (doc: Json) => Json;

/// The spec name governing the mount root.
export const ROOT_SPEC = ".root";

const SPEC_CACHE_CAP = 1024;

/// Resolve the storage root for a spec store: `specStore.root`, else
/// `store.root`, else `.rs2-<kind><mount base>`.
export function storeRoot(defaultKindPrefix: string, basePath: string, config: JsonObject): string {
  const rootOf = (key: string): string | undefined => {
    const s = config[key];
    if (!s || typeof s !== "object" || Array.isArray(s)) return undefined;
    const r = s.root;
    return typeof r === "string" ? r : undefined;
  };
  const r = rootOf("specStore") ?? rootOf("store");
  if (r !== undefined) return r.replace(/^\/+|\/+$/g, "");
  return `${defaultKindPrefix}${basePath}`.replace(/^\/+|\/+$/g, "");
}

export class SpecStore {
  private readonly inner = new FileService();
  private readonly innerCtx: ServiceContext;
  private readonly operatorRoles: string;
  private readonly cache = new Map<string, Json | undefined>();

  constructor(
    files: FileStore,
    tenant: string,
    root: string,
    private readonly subtree_: string,
    limits: InvocationLimits,
    private readonly validate: SpecValidator,
    operatorRoles: string | undefined,
  ) {
    const prefixed = new PrefixedFileStore(files, root);
    this.innerCtx = {
      config: {},
      files: new ScopedFileStore(prefixed, tenant),
      data: undefined,
      query: undefined,
      messaging: undefined,
      http: undefined,
      cachePolicy: new CachePolicy(),
      cacheOpenlyReadable: true,
      cors: new CorsPolicy(),
      limits,
      requester: undefined,
      control: undefined,
      tenantRetry: undefined,
      operatorRoles: undefined,
      pipelineWallClockMs: 120_000,
      logger: new ServiceLogger(new NullLogStore(), tenant, "", "spec-store"),
      logStore: undefined,
      catalogue: undefined,
      builtinAdapters: undefined,
      infras: undefined,
      secrets: undefined,
      outboundInjectors: new Map(),
    };
    this.operatorRoles = operatorRoles ?? "";
  }

  private cacheInsert(path: string, doc: Json | undefined): void {
    if (this.cache.size >= SPEC_CACHE_CAP) this.cache.clear();
    this.cache.set(path, doc);
  }

  /// Whether a request path enters the authoring subtree.
  isAuthoring(msg: Message): boolean {
    return msg.url.serviceSegments()[0] === this.subtree_;
  }

  /// Handle an authoring request: validate spec bodies on PUT and keyless
  /// POST, remap the mount split to `{base}/{subtree}`, delegate.
  async handleAuthoring(msg: Message): Promise<Message> {
    msg.url.applyMount(`${msg.url.basePath}/${this.subtree_}`);
    const isWrite = msg.method === "PUT" || (msg.method === "POST" && msg.url.isDirectory());
    if (isWrite) {
      if (!msg.body) throw RsError.badRequest("spec write requires a JSON body");
      const doc = await msg.body.asJson(this.innerCtx.limits.materializedBodyBytes);
      const incomingAccess = doc && typeof doc === "object" && !Array.isArray(doc) ? doc.access : undefined;
      let existingAccess: Json | undefined;
      try {
        const existing = await this.read(msg.url.servicePath);
        existingAccess = existing && typeof existing === "object" && !Array.isArray(existing) ? existing.access : undefined;
      } catch {
        existingAccess = undefined;
      }
      const same =
        incomingAccess === undefined || existingAccess === undefined
          ? incomingAccess === existingAccess
          : JSON.stringify(incomingAccess) === JSON.stringify(existingAccess);
      if (!same && !isOperator(msg.principal, this.operatorRoles)) {
        throw RsError.forbidden("setting or changing a spec's 'access' requires an operator (operatorRoles)");
      }
      const canonical = this.validate(doc);
      msg.body = Body.fromString(JSON.stringify(canonical), MediaType.json());
    }
    const isListing = msg.method === "GET" && msg.url.isDirectory();
    const isSpecRead = msg.method === "GET" && !msg.url.isDirectory();
    const mutates = msg.method !== "GET" && msg.method !== "HEAD";
    const listingPath = msg.url.servicePath;
    const template = msg.response(200, undefined);
    let result: Message;
    try {
      result = await this.inner.handle(msg, this.innerCtx);
    } catch (e) {
      if (mutates) this.cache.clear();
      if (isListing && e instanceof RsError && e.code === codes.NOT_FOUND) {
        const listing = { path: listingPath, entries: [], total: 0 };
        const resp = template.ok(Body.fromString(JSON.stringify(listing), MediaType.dirJson()));
        resp.setHeader("x-total-count", "0");
        return resp;
      }
      throw e;
    }
    if (mutates) this.cache.clear();
    if (isSpecRead && result.body) result.body.mediaType = MediaType.json();
    return result;
  }

  /// Read one stored spec by its subtree-relative path (cached, incl. not-found).
  async read(specPath: string): Promise<Json> {
    if (this.cache.has(specPath)) {
      const cached = this.cache.get(specPath);
      if (cached === undefined) throw RsError.notFound(`no stored spec at '${specPath}'`);
      return cached;
    }
    const files = this.innerCtx.files!;
    let body: Body;
    try {
      body = await files.read(specPath, undefined);
    } catch {
      this.cacheInsert(specPath, undefined);
      throw RsError.notFound(`no stored spec at '${specPath}'`);
    }
    const bytes = await body.materialize(this.innerCtx.limits.materializedBodyBytes);
    let doc: Json;
    try {
      doc = JSON.parse(new TextDecoder().decode(bytes)) as Json;
    } catch (e) {
      throw RsError.internal(`stored spec is corrupt: ${e instanceof Error ? e.message : String(e)}`);
    }
    this.cacheInsert(specPath, doc);
    return doc;
  }

  /// Execution resolution: the longest stored prefix wins; else `.root`.
  async resolve(segments: readonly string[]): Promise<[Json, number] | undefined> {
    for (let split = segments.length; split >= 1; split--) {
      const candidate = `/${segments.slice(0, split).join("/")}`;
      try {
        return [await this.read(candidate), split];
      } catch {
        /* keep peeling */
      }
    }
    try {
      return [await this.read(`/${ROOT_SPEC}`), 0];
    } catch {
      return undefined;
    }
  }

  subtree(): string {
    return this.subtree_;
  }
}
