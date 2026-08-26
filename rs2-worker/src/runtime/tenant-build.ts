// Tenant (PRD §4, §9.1): an isolation domain — its own mounts, config,
// storage namespace, and resource quotas. Built atomically from a
// `TenantConfig`. Port of `rs2-core/src/tenant.rs`; the service-name
// `match` is the registry of services.

import { BuiltinRegistry } from "../capabilities/builtin-registry";
import { CredentialInjector } from "../capabilities/credential";
import { PrefixedFileStore } from "../capabilities/prefixed";
import { ScopedDataStore, ScopedFileStore, ScopedQueryStore } from "../capabilities/scoped";
import type { ScopedSmsGateway } from "../capabilities/scoped";
import type { DataStore, FileStore, HttpOut, QueryStore } from "../capabilities/types";
import { requireStoreRoot, sanitizedStoreRoot } from "../capabilities/types";
import { AuthService } from "../services/auth";
import type { CatalogueClient } from "../services/catalogue";
import { CodeService } from "../services/code";
import {
  PIPELINE_PREFIX,
  PIPELINE_SUBTREE,
  PROXY_INJECTOR_KEY,
  QUERY_PREFIX,
  QUERY_SUBTREE,
  TEMPLATE_PREFIX,
  TEMPLATE_SUBTREE,
} from "../services/context";
import type { Requester, Service, ServiceContext, TenantControl } from "../services/context";
import { DataService } from "../services/data";
import { FileService } from "../services/file";
import { LogReaderService } from "../services/log-reader";
import { ServicesService } from "../services/services-config";
import { SpecStore, storeRoot } from "../services/spec-store";
import type { SpecValidator } from "../services/spec-store";
import { PipelineService } from "../services/pipeline-service";
import { NotYetService, SpecBackedStub } from "../services/stubs";
import { WrapperService } from "../services/wrapper-service";
import type { TenantConfig } from "./config-schema";
import { RsError } from "./error";
import type { Json, JsonObject } from "./error";
import { InfraSet, expandInfra } from "./infra";
import { ServiceLogger } from "./logging";
import type { LogStore, Severity } from "./logging";
import { urlHost } from "./outbound";
import { MountTable } from "./router";
import type { Mount } from "./router";
import { scheduleFromConfig } from "./scheduler";
import { CachePolicy, CorsPolicy, invocationLimits } from "./wrapper";
import type { LimitTable } from "./wrapper";

/// Shared adapter wiring handed to tenants (the DO builds one per instance).
export interface Adapters {
  files: FileStore;
  data: DataStore;
  query: QueryStore | undefined;
  http: HttpOut | undefined;
  log: LogStore;
  logLevel: Severity;
  builtins: BuiltinRegistry;
  catalogue: CatalogueClient | undefined;
  infras: InfraSet;
}

/// Seed the built-in registry with the node's own built-ins: `mem`, `local`
/// (files), `file` (data), `reference` (query).
export function seedBuiltins(
  files: FileStore,
  dataFactory: (ns: string) => DataStore,
  queryFactory: ((data: DataStore) => QueryStore) | undefined,
): BuiltinRegistry {
  const builtins = new BuiltinRegistry();
  builtins.registerData("mem", () => dataFactory("mem"));
  builtins.registerData("file", (cfg) => dataFactory(sanitizedStoreRoot(cfg) ?? ""));
  builtins.registerFiles("local", (cfg) => {
    const root = sanitizedStoreRoot(cfg);
    return root !== undefined ? new PrefixedFileStore(files, root) : files;
  });
  if (queryFactory) builtins.registerQuery("reference", () => queryFactory(dataFactory("")));
  return builtins;
}

export class Tenant {
  constructor(
    readonly name: string,
    readonly mounts: MountTable,
    readonly auth: Json | undefined,
    readonly cors: CorsPolicy,
    private readonly instances: Map<string, [Service, ServiceContext]>,
  ) {}

  instance(basePath: string): [Service, ServiceContext] | undefined {
    return this.instances.get(basePath);
  }
}

/// Discovery API patterns a `wrapper` mount may declare.
export const KNOWN_PATTERNS = ["store", "store-transform", "store-view", "view", "api"];

function mountLabel(mount: Mount): string {
  return mount.basePath === "" ? "/" : mount.basePath;
}

/// An `elevate` role must not be an operator role.
function checkElevateNotOperator(mount: Mount, operatorRoles: string | undefined): void {
  const elevate = mount.config.elevate;
  if (typeof elevate !== "string" || operatorRoles === undefined) return;
  if (operatorRoles.split(/\s+/).some((r) => r === elevate)) {
    throw RsError.badRequest(
      `mount '${mountLabel(mount)}' sets elevate role '${elevate}', which is an operator role; elevation must not confer operator authority`,
    );
  }
}

const NODE_DATA_BUILTIN = "file";
const NODE_FILE_BUILTIN = "local";

type StoreKind = { kind: "default" } | { kind: "builtin"; name: string } | { kind: "code"; ref: string };

function classifyStore(store: Json): StoreKind {
  const adapter = store && typeof store === "object" && !Array.isArray(store) ? store.adapter : undefined;
  if (typeof adapter !== "string") return { kind: "default" };
  if (adapter.startsWith("code:")) return { kind: "code", ref: adapter };
  if (adapter.startsWith("builtin:")) return { kind: "builtin", name: adapter.slice("builtin:".length) };
  throw RsError.badRequest(
    `store adapter '${adapter}' must be 'builtin:<name>', 'code:<name>@<version>', or 'infra:<name>'`,
  );
}

function expandStore(mount: Mount, kind: string, tenant: string, infras: InfraSet): [StoreKind, Json] {
  if (mount.service !== kind) return [{ kind: "default" }, {}];
  const raw = mount.config.store ?? {};
  const expanded = expandInfra(raw, infras, tenant);
  return [classifyStore(expanded), expanded];
}

function unknownBuiltin(kind: string, name: string, available: string[]): RsError {
  return RsError.badRequest(`${kind} store adapter 'builtin:${name}' is unknown (available: ${available.join(", ")})`);
}

/// Resident (`code:`) adapters are 501 on this host until P4b (cloudflare.md §C.1).
function loadableUnavailable(kind: string, mount: Mount, adapterRef: string): RsError {
  return RsError.engineUnavailable(
    `${kind} mount '${mountLabel(mount)}' uses a loadable adapter ('${adapterRef}') but this build has no JS engine (rebuild with --features js)`,
  );
}

function buildFileBackend(kind: StoreKind, store: Json, adapters: Adapters, label: string, mount: Mount): FileStore {
  switch (kind.kind) {
    case "default":
      return adapters.files;
    case "builtin": {
      const built = adapters.builtins.buildFiles(kind.name, store);
      if (!built) throw unknownBuiltin(label, kind.name, adapters.builtins.filesNames());
      return built;
    }
    case "code":
      throw loadableUnavailable(label, mount, kind.ref);
  }
}

function resolveSpecBackend(mount: Mount, adapters: Adapters, tenant: string, infras: InfraSet): FileStore {
  const raw = mount.config.specStore ?? {};
  const expanded = expandInfra(raw, infras, tenant);
  return buildFileBackend(classifyStore(expanded), expanded, adapters, "spec", mount);
}

function resolveSecrets(mountConfig: JsonObject, tenantSecrets: Json | undefined): JsonObject | undefined {
  const names = mountConfig.secrets;
  if (!Array.isArray(names)) return undefined;
  const tenant = tenantSecrets && typeof tenantSecrets === "object" && !Array.isArray(tenantSecrets) ? tenantSecrets : undefined;
  const out: JsonObject = {};
  for (const n of names) {
    if (typeof n !== "string") continue;
    if (tenant && Object.prototype.hasOwnProperty.call(tenant, n)) out[n] = tenant[n]!;
  }
  return Object.keys(out).length ? out : undefined;
}

function substituteSecretRefs(value: Json, secrets: JsonObject | undefined): Json {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const out: JsonObject = {};
    for (const [k, v] of Object.entries(value)) out[k] = substituteSecretRefs(v, secrets);
    return out;
  }
  if (typeof value === "string" && value.startsWith("secret:")) {
    const name = value.slice("secret:".length);
    const v = secrets && Object.prototype.hasOwnProperty.call(secrets, name) ? secrets[name] : undefined;
    if (v === undefined) throw RsError.badRequest(`inject references secret '${name}' not granted to this mount`);
    if (typeof v !== "string") throw RsError.badRequest(`secret '${name}' must be a string to use in an inject strategy`);
    return v;
  }
  return value;
}

function resolveOneInjector(
  label: string,
  inject: Json,
  infras: InfraSet,
  tenant: string,
  secrets: JsonObject | undefined,
): CredentialInjector | undefined {
  if (typeof inject === "string") {
    if (inject.startsWith("infra:")) {
      const merged = expandInfra({ adapter: inject }, infras, tenant);
      return CredentialInjector.fromConfig(merged);
    }
    throw RsError.badRequest(
      `'${label}' inject string must be 'infra:<name>' (got '${inject}'); use an object to inject from tenant secrets`,
    );
  }
  if (inject && typeof inject === "object" && !Array.isArray(inject)) {
    return CredentialInjector.fromConfig(substituteSecretRefs(inject, secrets));
  }
  throw RsError.badRequest(`'${label}' inject must be 'infra:<name>' or an inline object`);
}

function resolveOutboundInjectors(
  mount: Mount,
  infras: InfraSet,
  tenant: string,
  secrets: JsonObject | undefined,
): Map<string, CredentialInjector> {
  const out = new Map<string, CredentialInjector>();
  const grants = mount.config.grants;
  if (grants && typeof grants === "object" && !Array.isArray(grants)) {
    for (const [capability, grant] of Object.entries(grants)) {
      if (!grant || typeof grant !== "object" || Array.isArray(grant)) continue;
      if (grant.inject === undefined) continue;
      const inj = resolveOneInjector(capability, grant.inject, infras, tenant, secrets);
      if (inj) out.set(capability, inj);
    }
  }
  if (mount.service === "proxy" && mount.config.inject !== undefined) {
    const inj = resolveOneInjector("proxy", mount.config.inject, infras, tenant, secrets);
    if (inj) out.set(PROXY_INJECTOR_KEY, inj);
  }
  return out;
}

function dataCapability(mount: Mount, adapters: Adapters, name: string, infras: InfraSet): ScopedDataStore {
  const [kind, store] = expandStore(mount, "data", name, infras);
  switch (kind.kind) {
    case "default":
      return new ScopedDataStore(adapters.data, name);
    case "builtin": {
      if (kind.name === NODE_DATA_BUILTIN) requireStoreRoot(store);
      const inner = adapters.builtins.buildData(kind.name, store);
      if (!inner) throw unknownBuiltin("data", kind.name, adapters.builtins.dataNames());
      return new ScopedDataStore(inner, name);
    }
    case "code":
      throw loadableUnavailable("data", mount, kind.ref);
  }
}

function fileCapability(mount: Mount, adapters: Adapters, name: string, infras: InfraSet): ScopedFileStore {
  const [kind, store] = expandStore(mount, "file", name, infras);
  if (kind.kind === "builtin" && kind.name === NODE_FILE_BUILTIN) requireStoreRoot(store);
  const inner = buildFileBackend(kind, store, adapters, "file", mount);
  return new ScopedFileStore(inner, name);
}

function queryCapability(mount: Mount, adapters: Adapters, name: string, infras: InfraSet): ScopedQueryStore | undefined {
  const [kind, store] = expandStore(mount, "query", name, infras);
  switch (kind.kind) {
    case "default":
      return adapters.query ? new ScopedQueryStore(adapters.query, name) : undefined;
    case "builtin": {
      const inner = adapters.builtins.buildQuery(kind.name, store);
      if (!inner) throw unknownBuiltin("query", kind.name, adapters.builtins.queryNames());
      return new ScopedQueryStore(inner, name);
    }
    case "code":
      throw loadableUnavailable("query", mount, kind.ref);
  }
}

function smsCapability(mount: Mount, name: string, infras: InfraSet): ScopedSmsGateway | undefined {
  if (mount.service !== "sms") return undefined;
  const [kind] = expandStore(mount, "sms", name, infras);
  switch (kind.kind) {
    case "default":
      throw RsError.badRequest("sms mount requires a store.adapter ('code:<name>@<version>' or 'infra:<name>')");
    case "builtin":
      throw RsError.badRequest(
        `sms store adapter 'builtin:${kind.name}' is unknown (no first-party SMS providers ship yet; use a 'code:<name>@<version>' adapter)`,
      );
    case "code":
      throw loadableUnavailable("sms", mount, kind.ref);
  }
}

const identityValidator: SpecValidator = (doc) => doc;

function specStoreFor(
  mount: Mount,
  adapters: Adapters,
  name: string,
  limits: LimitTable,
  infras: InfraSet,
  prefix: string,
  subtree: string,
  operatorRoles: string | undefined,
  validator: SpecValidator = identityValidator,
): SpecStore {
  const root = storeRoot(prefix, mount.basePath, mount.config);
  const backend = resolveSpecBackend(mount, adapters, name, infras);
  return new SpecStore(backend, name, root, subtree, invocationLimits(limits), validator, operatorRoles);
}

/// Validate config and build all service instances. Fails as a whole on
/// any invalid mount — an invalid config never half-applies (PRD §10.6).
export function buildTenant(
  name: string,
  config: TenantConfig,
  adapters: Adapters,
  limits: LimitTable,
  requester: Requester | undefined,
  control: TenantControl | undefined,
): Tenant {
  for (const cat of config.catalogues) {
    if (urlHost(cat.url) === undefined || !(cat.url.startsWith("http://") || cat.url.startsWith("https://"))) {
      throw RsError.badRequest(`catalogue '${cat.name}' url '${cat.url}' must be an absolute http(s) URL`);
    }
  }
  for (const m of config.mounts) {
    if (m.config.schedule !== undefined) scheduleFromConfig(m.config.schedule);
  }
  const mounts = new MountTable(config.mounts.map((m) => ({ basePath: m.path, service: m.service, config: m.config })));
  const infras = adapters.infras;
  const instances = new Map<string, [Service, ServiceContext]>();
  const cors = CorsPolicy.fromConfig(config.cors);

  for (const mount of mounts.mounts()) {
    let service: Service;
    switch (mount.service) {
      case "file":
        service = FileService.fromConfig(mount.config);
        break;
      case "data":
        service = DataService.fromConfig(mount.config);
        break;
      case "pipeline": {
        checkElevateNotOperator(mount, config.operatorRoles);
        const store = specStoreFor(
          mount,
          adapters,
          name,
          limits,
          infras,
          PIPELINE_PREFIX,
          PIPELINE_SUBTREE,
          config.operatorRoles,
          PipelineService.validator(),
        );
        service = PipelineService.fromConfig(mount.config, store);
        break;
      }
      case "query": {
        const store = specStoreFor(mount, adapters, name, limits, infras, QUERY_PREFIX, QUERY_SUBTREE, config.operatorRoles);
        service = new SpecBackedStub("query", store);
        break;
      }
      case "template": {
        const store = specStoreFor(mount, adapters, name, limits, infras, TEMPLATE_PREFIX, TEMPLATE_SUBTREE, config.operatorRoles);
        service = new SpecBackedStub("template", store);
        break;
      }
      case "wrapper": {
        checkElevateNotOperator(mount, config.operatorRoles);
        const p = mount.config.pattern;
        if (typeof p === "string" && !KNOWN_PATTERNS.includes(p)) {
          throw RsError.badRequest(
            `wrapper mount '${mountLabel(mount)}' declares unknown pattern '${p}' (one of: ${KNOWN_PATTERNS.join(", ")})`,
          );
        }
        service = WrapperService.fromConfig(mount.config);
        break;
      }
      case "auth":
        service = AuthService.fromConfig(mount.config, config.auth);
        break;
      case "services":
        service = new ServicesService();
        break;
      case "log":
        service = new LogReaderService();
        break;
      case "sms":
        service = new NotYetService("sms");
        break;
      case "proxy":
        service = new NotYetService("proxy");
        break;
      default:
        if (mount.service.startsWith("code:")) {
          service = CodeService.fromRef(mount.service);
          break;
        }
        throw RsError.badRequest(
          `unknown service '${mount.service}' at mount '${mountLabel(mount)}' (custom \`code:\` services arrive with the self-config API)`,
        );
    }

    const files = fileCapability(mount, adapters, name, infras);
    const data = dataCapability(mount, adapters, name, infras);
    const query = queryCapability(mount, adapters, name, infras);
    const sms = smsCapability(mount, name, infras);
    const mountSecrets = resolveSecrets(mount.config, config.secrets);
    const outboundInjectors = resolveOutboundInjectors(mount, infras, name, mountSecrets);

    const ctx: ServiceContext = {
      config: mount.config,
      files,
      data,
      query,
      sms,
      http: adapters.http,
      cachePolicy: CachePolicy.fromConfig(mount.config.caching),
      cacheOpenlyReadable: CachePolicy.mountIsOpenlyReadable(mount.config),
      cors,
      limits: invocationLimits(limits),
      requester,
      control,
      tenantRetry: config.retry,
      operatorRoles: config.operatorRoles,
      pipelineWallClockMs: limits.wallClockPipelineMs,
      logger: new ServiceLogger(adapters.log, name, mount.basePath, mount.service),
      logStore: mount.service === "log" ? adapters.log : undefined,
      catalogue: mount.service === "services" ? adapters.catalogue : undefined,
      builtinAdapters: mount.service === "services" ? adapters.builtins : undefined,
      infras: mount.service === "services" ? infras : undefined,
      secrets: mountSecrets,
      outboundInjectors,
    };
    instances.set(mount.basePath, [service, ctx]);
  }
  return new Tenant(name, mounts, config.auth, cors, instances);
}
