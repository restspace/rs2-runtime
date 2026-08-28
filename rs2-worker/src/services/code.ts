// Engine-backed custom services (`code:<name>@<version>`). Port of
// `rs2-core/src/services/code.rs` over the Dynamic Worker engine
// (cloudflare.md §E): deployed bundles are content-addressed under
// `.rs2-code/`, capability grants come from the mount config (default
// deny), and the host resolves `x-rs2-body-ref` after the guest returns.

import { cpuMsFromConfig, resolveBodyRef } from "../engines/dynamic-worker";
import type { DynamicWorkerEngine } from "../engines/dynamic-worker";
import type { CapabilityTarget, LogContext } from "../engines/host-api";
import { Body, utf8Decode } from "../runtime/body";
import { sha256Hex } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Json } from "../runtime/error";
import { Message, MsgUrl } from "../runtime/message";
import { validatePath } from "../runtime/router";
import { CachePolicy, CorsPolicy } from "../runtime/wrapper";
import { sanitizedStoreRoot } from "../capabilities/types";
import { hostMatches, urlHost } from "../runtime/outbound";
import { FileService } from "./file";
import type { Service, ServiceContext } from "./context";

/// Storage prefix for deployed code in the tenant's file store.
export const CODE_PREFIX = ".rs2-code";
/// Storage prefix for `store` grants (service-private storage).
export const STORE_GRANT_PREFIX = ".rs2-store";
export const BODY_REF_HEADER = "x-rs2-body-ref";
export const BASE_PATH_HEADER = "x-rs2-base-path";

/// Content-addressed version of a bundle: sha256[0..8] hex.
export async function versionOf(bytes: Uint8Array): Promise<string> {
  return (await sha256Hex(bytes)).slice(0, 16);
}

export function codePath(name: string, version: string): string {
  return `${CODE_PREFIX}/${name}/${version}.wasm`;
}

export function codePathJs(name: string, version: string): string {
  return `${CODE_PREFIX}/${name}/${version}.js`;
}

export class CodeService implements Service {
  /// The deployed bundle source, loaded once per built instance (the
  /// analogue of Rust's `OnceCell<LoadedCode>`).
  private loaded: Promise<string> | undefined;

  private constructor(
    readonly name: string,
    readonly version: string,
    private readonly engine: DynamicWorkerEngine | undefined,
  ) {}

  /// Parse a `code:<name>@<version>` service reference.
  static fromRef(serviceRef: string, engine: DynamicWorkerEngine | undefined): CodeService {
    if (!serviceRef.startsWith("code:")) throw RsError.badRequest("not a code: service reference");
    const rest = serviceRef.slice("code:".length);
    const at = rest.indexOf("@");
    if (at < 0) throw RsError.badRequest(`code reference '${serviceRef}' must be 'code:<name>@<version>'`);
    const name = rest.slice(0, at);
    const version = rest.slice(at + 1);
    if (name === "" || version === "" || /[/\\.]/.test(name)) {
      throw RsError.badRequest(`invalid code reference '${serviceRef}'`);
    }
    return new CodeService(name, version, engine);
  }

  /// Same load order as Rust: `.wasm` first (→ 501 here), then `.js`.
  private async loadSource(ctx: ServiceContext): Promise<string> {
    if (this.loaded) return this.loaded;
    const files = ctx.files;
    if (!files) throw RsError.internal("code service has no file capability");
    const load = (async () => {
      try {
        await files.head(codePath(this.name, this.version));
        throw RsError.engineUnavailable(
          `code:${this.name}@${this.version} is a wasm component but this build has no wasm engine (rebuild with --features wasm)`,
        );
      } catch (e) {
        if (!(e instanceof RsError) || e.status !== 404) throw e;
      }
      let body: Body;
      try {
        body = await files.read(codePathJs(this.name, this.version), undefined);
      } catch (e) {
        if (e instanceof RsError && e.status === 404) {
          throw RsError.notFound(
            `deployed code '${this.name}@${this.version}' not found — deploy it via PUT /code/${this.name}`,
          );
        }
        throw e;
      }
      return utf8Decode(await body.materialize(ctx.limits.materializedBodyBytes));
    })();
    // Cache only successful loads: a 404 before deployment must not pin.
    this.loaded = load;
    load.catch(() => {
      this.loaded = undefined;
    });
    return load;
  }

  /// Build the default-deny capability table from the mount's `grants`.
  /// Grant kinds mirror `services/code.rs`: `prefix` (re-enters dispatch
  /// with the caller's principal), `httpOut` (allowlist + injector),
  /// `store` (service-private storage under `.rs2-store/<root>`).
  grants(ctx: ServiceContext): Map<string, CapabilityTarget> {
    const grants = new Map<string, CapabilityTarget>();
    const configGrants = ctx.config.grants;
    if (!configGrants || typeof configGrants !== "object" || Array.isArray(configGrants)) return grants;
    const requester = ctx.requester;
    if (!requester) throw RsError.internal("code service has no requester capability");
    for (const [capability, grant] of Object.entries(configGrants)) {
      if (!grant || typeof grant !== "object" || Array.isArray(grant)) continue;
      if (grant.type === "store") {
        grants.set(capability, storeGrantTarget(capability, grant, ctx));
        continue;
      }
      if (grant.type === "httpOut") {
        const hosts = Array.isArray(grant.hosts) ? grant.hosts.filter((h): h is string => typeof h === "string") : [];
        if (hosts.length === 0) {
          throw RsError.badRequest(`httpOut grant '${capability}' requires a non-empty 'hosts' allowlist`);
        }
        const http = ctx.http;
        if (!http) {
          throw RsError.engineUnavailable("this deployment has no outbound HTTP adapter configured");
        }
        // Host-side credential injection (resolved at tenant build, never in
        // `config`): applied after the allowlist check, before the request
        // leaves the host. Absent ⇒ headers pass through verbatim.
        const injector = ctx.outboundInjectors.get(capability);
        const bodyCap = ctx.limits.materializedBodyBytes;
        grants.set(capability, async (msg: Message) => {
          const host = urlHost(msg.url.path);
          if (host === undefined) {
            throw RsError.badRequest(`outbound call needs an absolute URL, got '${msg.url.path}'`);
          }
          if (!hosts.some((pattern) => hostMatches(pattern, host))) {
            throw RsError.capabilityDenied(`httpOut to '${host}'`);
          }
          if (injector) await injector.apply(msg, bodyCap);
          return http.request(msg);
        });
        continue;
      }
      // NOTE `type: "socket"` grants fall through to the prefix requirement
      // and 400 — exactly as Rust `CodeService::grants` does today. The
      // engine's socket allowlist machinery (shim `RS2Socket`, the
      // `socketCheck` RPC) stays for the P4b resident adapters, whose
      // grants come from the `store` block, not this table.
      const prefixRaw = grant.prefix;
      if (typeof prefixRaw !== "string") {
        throw RsError.badRequest(`grant '${capability}' requires a 'prefix'`);
      }
      const prefix = prefixRaw.replace(/\/+$/, "");
      grants.set(capability, async (msg: Message) => {
        // Scope the call under the granted prefix; the rewritten request
        // re-enters full dispatch (authz, limits, idempotency — PRD §9.2).
        const subPath = msg.url.path.replace(/^\/+/, "");
        const path = subPath === "" ? prefix : `${prefix}/${subPath}`;
        const query = msg.url.query;
        msg.url = MsgUrl.parse(query === "" ? path : `${path}?${query}`);
        msg.source = "internal";
        return requester.request(msg);
      });
    }
    return grants;
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const source = await this.loadSource(ctx);
    const engine = this.engine;
    if (!engine) {
      throw RsError.engineUnavailable(
        `code:${this.name}@${this.version} is a JS bundle but this deployment has no worker loader binding (wrangler.jsonc worker_loaders)`,
      );
    }
    const base = msg.url.basePath === "" ? "/" : msg.url.basePath;
    msg.setHeader(BASE_PATH_HEADER, base);
    // Captured before the engine runs: what a post-return `x-rs2-body-ref`
    // resolution needs to issue the read.
    const tenant = msg.tenant;
    const principal = msg.principal;
    const trace = msg.trace.clone();
    const depth = msg.depth;

    const serviceRef = `${this.name}@${this.version}`;
    const logCtx: LogContext = {
      sink: ctx.logger.sink(),
      tenant: msg.tenant,
      mount: msg.url.basePath,
      service: serviceRef,
      traceId: msg.trace.traceId,
      spanId: msg.trace.spanId,
    };
    const grants = this.grants(ctx);
    const resp = await engine.invoke({
      // Mount-addressed as well as content-addressed: one isolate per mount
      // of a bundle, so grants never cross mounts through shared module
      // state (issue #2 item 1).
      codeId: `${msg.tenant}:${base}:${serviceRef}`,
      source,
      msg,
      config: ctx.config,
      grants,
      serviceRef,
      logCtx,
      outboundBudget: ctx.limits.outboundCalls,
      materializeCap: ctx.limits.materializedBodyBytes,
      wallClockMs: ctx.limits.wallClockMs,
      cpuMs: cpuMsFromConfig(ctx.config),
    });
    return resolveBodyRef(resp, grants, tenant, principal, trace, depth, BODY_REF_HEADER);
  }
}

/// Build the target for a `{"type": "store", "root": "…"}` grant:
/// **service-private storage** — a private `FileService` over the tenant
/// file store rooted at `.rs2-store/<root>`, never routed through a mount,
/// so no principal is involved: the operator-configured grant *is* the
/// authority. Failures become status responses, as for a `prefix` grant.
function storeGrantTarget(capability: string, grant: Json, ctx: ServiceContext): CapabilityTarget {
  const root = sanitizedStoreRoot(grant);
  if (root === undefined) {
    throw RsError.badRequest(`store grant '${capability}' requires a non-empty relative 'root'`);
  }
  const files = ctx.files;
  if (!files) throw RsError.internal("code service has no file capability");
  const scoped = files.prefixed(`${STORE_GRANT_PREFIX}/${root}`);
  const innerCtx: ServiceContext = {
    config: {},
    files: scoped,
    data: undefined,
    query: undefined,
    messaging: undefined,
    http: undefined,
    cachePolicy: new CachePolicy(),
    cacheOpenlyReadable: true,
    cors: new CorsPolicy(),
    limits: ctx.limits,
    requester: undefined,
    control: undefined,
    tenantRetry: undefined,
    operatorRoles: undefined,
    pipelineWallClockMs: 120_000,
    logger: ctx.logger,
    logStore: undefined,
    catalogue: undefined,
    builtinAdapters: undefined,
    infras: undefined,
    secrets: undefined,
    outboundInjectors: new Map(),
  };
  const inner = new FileService();
  return async (msg: Message) => {
    const template = msg.response(200, undefined);
    // Direct service call, no router — apply its path safety here
    // (traversal in a guest path must not escape the private root).
    try {
      validatePath(msg.url.path);
    } catch (e) {
      if (e instanceof RsError) return template.errorResponse(e);
      throw e;
    }
    try {
      return await inner.handle(msg, innerCtx);
    } catch (e) {
      if (e instanceof RsError) return template.errorResponse(e);
      throw e;
    }
  };
}
