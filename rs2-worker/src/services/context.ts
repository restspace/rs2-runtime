// Prebuilt services: the `ServiceContext` a mount instance was granted at
// build time, the `Service` shape, and the shared helpers. Port of
// `rs2-core/src/services/mod.rs`.

import type { CredentialInjector } from "../capabilities/credential";
import type { ScopedDataStore, ScopedFileStore, ScopedQueryStore, ScopedSmsGateway } from "../capabilities/scoped";
import type { HttpOut, WritePrecondition } from "../capabilities/types";
import type { JsonObject } from "../runtime/error";
import type { InfraSet } from "../runtime/infra";
import type { LogStore, ServiceLogger } from "../runtime/logging";
import type { Message } from "../runtime/message";
import type { RetryPolicy } from "../runtime/retry";
import type { CachePolicy, CorsPolicy, InvocationLimits } from "../runtime/wrapper";
import type { BuiltinRegistry } from "../capabilities/builtin-registry";
import type { CatalogueClient } from "./catalogue";

/// Internal dispatch capability (pipelines and composition): requests route
/// back through the full dispatch path.
export interface Requester {
  request(msg: Message): Promise<Message>;
}

/// Control-plane capability granted to the `services` self-config service.
export interface TenantControl {
  rawConfig(tenant: string): Promise<[JsonObject, string]>;
  /// Validate the whole config, persist it, and swap the running tenant
  /// atomically. Returns the new version.
  putConfig(tenant: string, config: JsonObject, ifMatch: string | undefined): Promise<string>;
}

/// What a service instance was granted at mount time.
export interface ServiceContext {
  config: JsonObject;
  files: ScopedFileStore | undefined;
  data: ScopedDataStore | undefined;
  query: ScopedQueryStore | undefined;
  sms: ScopedSmsGateway | undefined;
  http: HttpOut | undefined;
  cachePolicy: CachePolicy;
  cacheOpenlyReadable: boolean;
  limits: InvocationLimits;
  requester: Requester | undefined;
  control: TenantControl | undefined;
  cors: CorsPolicy;
  tenantRetry: RetryPolicy | undefined;
  operatorRoles: string | undefined;
  pipelineWallClockMs: number;
  logger: ServiceLogger;
  /// Log read-back — granted only to `log` reader mounts.
  logStore: LogStore | undefined;
  /// Granted only to the `services` mount.
  catalogue: CatalogueClient | undefined;
  builtinAdapters: BuiltinRegistry | undefined;
  infras: InfraSet | undefined;
  secrets: JsonObject | undefined;
  outboundInjectors: Map<string, CredentialInjector>;
}

/// A unit of behavior handling messages at a mount: Message → Message.
export interface Service {
  handle(msg: Message, ctx: ServiceContext): Promise<Message>;
}

/// Whether an `If-None-Match` header value matches the resource's ETag.
export function ifNoneMatchHits(ifNoneMatch: string | undefined, etag: string): boolean {
  if (ifNoneMatch === undefined) return false;
  return ifNoneMatch
    .split(",")
    .map((c) => c.trim())
    .some((c) => c === "*" || c.replace(/^W\//, "") === etag);
}

/// Parse a store write's conditional headers into a `WritePrecondition`.
/// `If-None-Match: *` (create-only) takes precedence over `If-Match`.
export function writePrecondition(msg: Message): WritePrecondition {
  const inm = msg.header("if-none-match");
  if (inm !== undefined && inm.split(",").some((c) => c.trim() === "*")) return { kind: "ifNoneMatchStar" };
  const im = msg.header("if-match");
  return im !== undefined ? { kind: "ifMatch", value: im } : { kind: "none" };
}

/// Rust `str::parse::<usize>`: decimal digits only (a leading `+` is accepted).
function parseUsize(s: string | undefined): number | undefined {
  if (s === undefined || !/^\+?\d+$/.test(s)) return undefined;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : undefined;
}

/// Parse `$take`/`$skip` pagination query params with bounded defaults.
export function pagination(msg: Message): [number, number] {
  const take = Math.min(parseUsize(msg.url.queryParam("$take")) ?? 1000, 10_000);
  const skip = parseUsize(msg.url.queryParam("$skip")) ?? 0;
  return [take, skip];
}

export const PIPELINE_PREFIX = ".rs2-pipelines";
export const PIPELINE_SUBTREE = ".pipelines";
export const QUERY_PREFIX = ".rs2-queries";
export const QUERY_SUBTREE = ".queries";
export const TEMPLATE_PREFIX = ".rs2-templates";
export const TEMPLATE_SUBTREE = ".templates";
export const PROXY_INJECTOR_KEY = "proxy";
