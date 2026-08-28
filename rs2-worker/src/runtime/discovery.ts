// The agent surface (PRD §12), schema-first and generated per tenant. Port
// of `rs2-core/src/discovery.rs`, plus the `limits` object on the services
// document (cloudflare.md §A) with `host: "cloudflare"`.

import { CODE_PREFIX } from "../services/code";
import { PIPELINE_PREFIX, PIPELINE_SUBTREE, QUERY_PREFIX, QUERY_SUBTREE, TEMPLATE_SUBTREE } from "../services/context";
import { ROOT_SPEC, storeRoot } from "../services/spec-store";
import { RsError, codes } from "./error";
import type { Json, JsonObject } from "./error";
import { Message } from "./message";
import type { Mount } from "./router";
import { configGet } from "./router";
import type { Tenant } from "./tenant-build";
import { RS2_VERSION } from "./version";
import { checkRoleSpec } from "./wrapper";
import type { LimitTable } from "./wrapper";

export const WELL_KNOWN_PREFIX = "/.well-known/rs2/";

export function isDiscoveryPath(path: string): boolean {
  return path.startsWith(WELL_KNOWN_PREFIX);
}

/// The declared per-invocation limits (cloudflare.md §A).
export function limitsDoc(limits: LimitTable): JsonObject {
  return {
    wallClockMs: limits.wallClockServiceMs,
    memoryBytes: limits.memoryBytes,
    materializedBodyBytes: limits.materializedBodyBytes,
    outboundCalls: limits.outboundCalls,
    maxDepth: limits.maxDepth,
    host: "cloudflare",
  };
}

/// Handle a discovery request. The caller's principal must already be attached.
export async function handleDiscovery(tenant: Tenant, msg: Message, limits: LimitTable): Promise<Message> {
  if (msg.method !== "GET") {
    throw new RsError(405, codes.BAD_REQUEST, "Method Not Allowed", "the discovery surface is read-only");
  }
  const doc = msg.url.path.slice(WELL_KNOWN_PREFIX.length);
  let out: Json;
  switch (doc) {
    case "services":
      out = servicesDoc(tenant, msg, limits);
      break;
    case "agent-surface":
      out = await agentSurfaceDoc(tenant, readableMounts(tenant, msg), msg.url.queryParam("surface"), msg.tenant);
      break;
    case "openapi":
      out = await openapiDoc(tenant, readableMounts(tenant, msg), msg.tenant);
      break;
    default:
      throw RsError.notFound(`no discovery document '${doc}' (have: services, agent-surface, openapi)`);
  }
  return msg.okJson(out);
}

/// Stored specs for a spec-store mount (top-level, capped at 100 entries /
/// 1 MiB reads).
async function storedSpecs(tenant: Tenant, mount: Mount, kindPrefix: string): Promise<Array<[string, Json]>> {
  const inst = tenant.instance(mount.basePath);
  const files = inst?.[1].files;
  if (!files) return [];
  const root = storeRoot(kindPrefix, mount.basePath, mount.config);
  let entries;
  try {
    [entries] = await files.list(`${root}/`, 100, 0);
  } catch {
    return [];
  }
  const out: Array<[string, Json]> = [];
  for (const entry of entries) {
    if (entry.dir) continue;
    try {
      const body = await files.read(`${root}/${entry.name}`, undefined);
      const bytes = await body.materialize(1024 * 1024);
      out.push([entry.name, JSON.parse(new TextDecoder().decode(bytes)) as Json]);
    } catch {
      continue;
    }
  }
  return out;
}

function codeRefParts(service: string): [string, string] | undefined {
  if (!service.startsWith("code:")) return undefined;
  const rest = service.slice(5);
  const at = rest.indexOf("@");
  return at < 0 ? undefined : [rest.slice(0, at), rest.slice(at + 1)];
}

async function codeManifest(tenant: Tenant, mount: Mount): Promise<JsonObject | undefined> {
  const parts = codeRefParts(mount.service);
  if (!parts) return undefined;
  const files = tenant.instance(mount.basePath)?.[1].files;
  if (!files) return undefined;
  try {
    const body = await files.read(`${CODE_PREFIX}/${parts[0]}/${parts[1]}.manifest.json`, undefined);
    const v = JSON.parse(new TextDecoder().decode(await body.materialize(1024 * 1024))) as Json;
    return v && typeof v === "object" && !Array.isArray(v) ? v : undefined;
  } catch {
    return undefined;
  }
}

/// Mounts the caller may read (filtered on mount-level read access; **no
/// `access` is visible**).
function readableMounts(tenant: Tenant, msg: Message): Mount[] {
  return tenant.mounts.mounts().filter((m) => {
    const probe = Message.request("GET", m.basePath, msg.tenant);
    probe.principal = msg.principal;
    probe.source = msg.source;
    const access = configGet(m.config, "access");
    if (access === undefined) return true;
    try {
      checkRoleSpec(access, "read", probe);
      return true;
    } catch {
      return false;
    }
  });
}

function exposedOn(mount: Mount, surface: string | undefined): boolean {
  if (surface === undefined) return true;
  const x = configGet(mount.config, "x-expose");
  if (x === undefined) return true;
  if (typeof x === "string") return x === surface;
  if (Array.isArray(x)) return x.some((v) => v === surface);
  return false;
}

function meta(mount: Mount): JsonObject {
  const out: JsonObject = {};
  for (const key of ["x-agent", "x-policy", "x-expose", "x-render", "x-context", "description", "inputSchema", "outputSchema"]) {
    const v = configGet(mount.config, key);
    if (v !== undefined) out[key] = v;
  }
  const store = mount.config.store;
  if (store && typeof store === "object" && !Array.isArray(store) && typeof store.adapter === "string") {
    out.adapter = store.adapter;
  }
  const subtree = specSubtreeOf(mount);
  if (subtree !== undefined) out.specSubtree = subtree;
  const authoring = authoringOf(mount);
  if (authoring !== undefined) out.authoring = authoring;
  return out;
}

function specSubtreeOf(mount: Mount): string | undefined {
  switch (mount.service) {
    case "query":
      return QUERY_SUBTREE;
    case "pipeline":
      return PIPELINE_SUBTREE;
    case "template":
      return TEMPLATE_SUBTREE;
    default:
      return undefined;
  }
}

function authoringOf(mount: Mount): Json | undefined {
  switch (mount.service) {
    case "pipeline":
      return { kind: "pipeline-dsl", compiledField: "pipeline", sourceField: "x-source" };
    case "template":
      return { kind: "jsx", framework: "preact", compiledField: "source", sourceField: "jsxSource", render: "html" };
    default:
      return undefined;
  }
}

/// API pattern + facets (the polymorphism contract).
export function patternOf(mount: Mount): [string, string[]] {
  if (mount.service === "wrapper") {
    const p = mount.config.pattern;
    const pattern = typeof p === "string" ? p : "store-transform";
    const f = mount.config.facets;
    const facets = Array.isArray(f) ? f.filter((v): v is string => typeof v === "string") : [];
    return [pattern, facets];
  }
  let pattern: string;
  let facets: string[];
  switch (mount.service) {
    case "file": {
      facets = ["range", "confirm-delete", "move", "meta-sort"];
      if (
        configGet(mount.config, "defaultResource") !== undefined ||
        configGet(mount.config, "spaFallback") !== undefined ||
        configGet(mount.config, "spaFallbackAll") !== undefined
      ) {
        facets.push("static-site");
      }
      pattern = "store";
      break;
    }
    case "data":
      pattern = "store";
      facets = ["schema", "patch", "echo", "confirm-delete", "list-projection"];
      break;
    case "pipeline":
      pattern = "store-transform";
      facets = ["any-verb", "meta-sort"];
      break;
    case "query":
      pattern = "store-view";
      facets = ["positional-params", "url-params", "any-verb", "meta-sort"];
      break;
    case "template":
      pattern = "store-view";
      facets = ["positional-params", "url-params", "json-props", "any-verb", "meta-sort"];
      break;
    case "log":
      pattern = "view";
      facets = ["url-params", "time-range", "trace-scoped"];
      break;
    // `channels`: `GET /<mount>/channels` answers which channels this mount
    // actually sends on and whether delivery status can be asked for. That set
    // comes from the built adapters, not from config, so it is published there
    // rather than guessed here.
    case "message":
      pattern = "api";
      facets = ["channels"];
      break;
    default:
      pattern = "api";
      // Every `code:` mount declares the Worker-only guest contract
      // difference: guest capabilities are Promises and timers are real
      // (cloudflare.md §A, §E.2). Rust emits no facet here.
      facets = mount.service.startsWith("code:") ? ["guest-async"] : [];
  }
  if (pattern.startsWith("store")) facets.push("conditional-write");
  return [pattern, facets];
}

function withPattern(entry: JsonObject, mount: Mount): JsonObject {
  const [pattern, facets] = patternOf(mount);
  entry.pattern = pattern;
  if (facets.length) entry.facets = facets;
  return entry;
}

/// Capability descriptor for one mount — the body of the `OPTIONS` probe.
export function describeMount(mount: Mount): JsonObject {
  const base = mount.basePath === "" ? "/" : mount.basePath;
  const out: JsonObject = { path: base, service: mount.service };
  for (const [k, v] of Object.entries(meta(mount))) out[k] = v;
  if (mount.service === "data") out.schemaUrlPattern = `${base}/{dataset}/.schema.json`;
  return withPattern(out, mount);
}

/// The `Allow` header set for a mount's `OPTIONS` probe.
export function allowedMethods(mount: Mount): string[] {
  const [pattern, facets] = patternOf(mount);
  const has = (f: string) => facets.includes(f);
  let methods: string[];
  switch (pattern) {
    case "store":
      methods = has("patch")
        ? ["GET", "HEAD", "PUT", "POST", "PATCH", "DELETE", "OPTIONS"]
        : ["GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS"];
      break;
    case "store-transform":
    case "store-view":
      methods = ["GET", "HEAD", "PUT", "POST", "PATCH", "DELETE", "OPTIONS"];
      break;
    case "view":
      methods = ["GET", "HEAD", "OPTIONS"];
      break;
    default:
      methods = ["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"];
  }
  if (has("move")) methods.push("MOVE");
  return methods;
}

function servicesDoc(tenant: Tenant, msg: Message, limits: LimitTable): JsonObject {
  const surface = msg.url.queryParam("surface");
  const readable = readableMounts(tenant, msg).filter((m) => exposedOn(m, surface));
  const services: Json[] = readable.map((m) => {
    const entry: JsonObject = { path: m.basePath === "" ? "/" : m.basePath, service: m.service };
    for (const [k, v] of Object.entries(meta(m))) entry[k] = v;
    withPattern(entry, m);
    if (m.service === "data") {
      const data = tenant.instance(m.basePath)?.[1].data;
      if (data) entry.listProjection = data.listingPushdown() ? "native" : "fallback";
    }
    return entry;
  });
  const control = readable.find((m) => m.service === "services");
  const controlDoc: Json = control
    ? (() => {
        const base = control.basePath;
        return {
          path: base === "" ? "/" : base,
          config: `${base}/raw`,
          catalogue: `${base}/catalogue`,
          catalogues: `${base}/catalogues`,
          available: `${base}/catalogue/available`,
          install: `${base}/catalogue/install`,
          mounts: `${base}/services`,
          code: `${base}/code/`,
        };
      })()
    : null;
  return { tenant: msg.tenant, services, control: controlDoc, limits: limitsDoc(limits) };
}

function objGet(doc: Json, key: string): Json | undefined {
  return doc && typeof doc === "object" && !Array.isArray(doc) && Object.prototype.hasOwnProperty.call(doc, key)
    ? doc[key]
    : undefined;
}

async function agentSurfaceDoc(
  tenant: Tenant,
  mounts: Mount[],
  surface: string | undefined,
  tenantName: string,
): Promise<JsonObject> {
  const entities: Json[] = [];
  const actions: Json[] = [];
  const queries: Json[] = [];
  const idem = { header: "Idempotency-Key", honored: true };

  for (const mount of mounts) {
    if (!exposedOn(mount, surface)) continue;
    const base = mount.basePath === "" ? "/" : mount.basePath;
    switch (mount.service) {
      case "data": {
        const entry: JsonObject = {
          path: base,
          kind: "entity",
          schemaUrlPattern: `${base}/{dataset}/.schema.json`,
          idempotency: idem,
        };
        for (const [k, v] of Object.entries(meta(mount))) entry[k] = v;
        entities.push(withPattern(entry, mount));
        break;
      }
      case "pipeline": {
        for (const [name, doc] of await storedSpecs(tenant, mount, PIPELINE_PREFIX)) {
          const execPath = name === ROOT_SPEC ? base : `${base}/${name}`;
          const entry: JsonObject = {
            path: execPath,
            kind: "action",
            effect: objGet(doc, "effect") ?? "unsafe",
            plan: `${base}/${PIPELINE_SUBTREE}/${name}?$plan`,
            idempotency: idem,
          };
          for (const [k, v] of Object.entries(meta(mount))) entry[k] = v;
          for (const key of ["x-agent", "x-policy", "description"]) {
            const v = objGet(doc, key);
            if (v !== undefined) entry[key] = v;
          }
          const input = objGet(doc, "input");
          if (input !== undefined) entry.inputSchema = input;
          const output = objGet(doc, "output");
          if (output !== undefined) entry.outputSchema = output;
          actions.push(withPattern(entry, mount));
        }
        break;
      }
      case "query": {
        for (const [name, doc] of await storedSpecs(tenant, mount, QUERY_PREFIX)) {
          const entry: JsonObject = { path: `${base}/${name}`, kind: "query", effect: "pure", params: objGet(doc, "params") ?? {} };
          const output = objGet(doc, "output");
          if (output !== undefined) entry.output = output;
          for (const [k, v] of Object.entries(meta(mount))) entry[k] = v;
          queries.push(withPattern(entry, mount));
        }
        break;
      }
      case "wrapper": {
        const [pattern] = patternOf(mount);
        const entry: JsonObject = { path: base, idempotency: idem };
        for (const [k, v] of Object.entries(meta(mount))) entry[k] = v;
        if (pattern.startsWith("store")) {
          entry.kind = "entity";
          entities.push(withPattern(entry, mount));
        } else {
          entry.kind = "action";
          entry.effect = configGet(mount.config, "effect") ?? "unsafe";
          actions.push(withPattern(entry, mount));
        }
        break;
      }
      default: {
        if (!mount.service.startsWith("code:")) break;
        const manifest = await codeManifest(tenant, mount);
        const entry0: JsonObject = { path: base, idempotency: idem };
        for (const [k, v] of Object.entries(meta(mount))) entry0[k] = v;
        if (manifest) {
          for (const key of ["inputSchema", "outputSchema", "description"]) {
            if (manifest[key] !== undefined) entry0[key] = manifest[key]!;
          }
        }
        const storePattern = manifest && typeof manifest.storePattern === "string" ? manifest.storePattern : undefined;
        const entry = withPattern(entry0, mount);
        if (storePattern !== undefined) entry.pattern = storePattern;
        if (storePattern !== undefined && storePattern.startsWith("store")) {
          entry.kind = "entity";
          entities.push(entry);
        } else {
          entry.kind = "action";
          entry.effect = manifest && manifest.effect !== undefined ? manifest.effect : "unsafe";
          actions.push(entry);
        }
      }
    }
  }
  return { tenant: tenantName, entities, actions, queries };
}

const PROBLEM_RESPONSE = "#/components/responses/Problem";
const JSON_MEDIA = "application/json";

function problemSchema(): Json {
  return {
    type: "object",
    required: ["type", "title", "status", "code"],
    properties: {
      type: { type: "string" },
      title: { type: "string" },
      status: { type: "integer" },
      code: { type: "string" },
      detail: { type: "string" },
      tenant: { type: "string" },
      traceId: { type: "string" },
      retryable: { type: "boolean" },
      retryAfterMs: { type: "integer" },
    },
  };
}

function schemaComponentName(base: string, dataset: string): string {
  const sanitize = (s: string) => s.replace(/[^A-Za-z0-9.-]/g, "_");
  return `Dataset${sanitize(base)}_${sanitize(dataset)}`;
}

function op(summary: string, effect: string): JsonObject {
  return {
    summary,
    "x-effect": effect,
    "x-idempotency-key": "honored",
    responses: { default: { $ref: PROBLEM_RESPONSE }, "200": { description: "Success" } },
  };
}

function opMedia(summary: string, effect: string, responseMedias: string[]): JsonObject {
  const o = op(summary, effect);
  const content: JsonObject = {};
  for (const m of responseMedias) content[m] = {};
  (o.responses as JsonObject)["200"] = { description: "Success", content };
  return o;
}

function opWithSchemas(
  summary: string,
  effect: string,
  input: Json | undefined,
  output: Json | undefined,
  reqMedia: string,
  respMedia: string,
): JsonObject {
  const o = op(summary, effect);
  if (input !== undefined) o.requestBody = { content: { [reqMedia]: { schema: input } } };
  if (output !== undefined) {
    (o.responses as JsonObject)["200"] = { description: "Success", content: { [respMedia]: { schema: output } } };
  }
  return o;
}

function storeChildForSchema(schemaRef: Json): JsonObject {
  return {
    get: opWithSchemas("Read the stored resource; ETag carries the version", "pure", undefined, schemaRef, JSON_MEDIA, JSON_MEDIA),
    put: opWithSchemas("Upsert; 201 created / 200 overwritten, empty body", "idempotent", schemaRef, undefined, JSON_MEDIA, JSON_MEDIA),
    post: opWithSchemas("Upsert and return the stored representation (echo facet)", "unsafe", schemaRef, schemaRef, JSON_MEDIA, JSON_MEDIA),
    patch: op("JSON merge-patch (stores with the 'patch' facet)", "unsafe"),
    delete: op("Delete the resource", "idempotent"),
  };
}

async function openapiDoc(tenant: Tenant, mounts: Mount[], tenantName: string): Promise<JsonObject> {
  const paths: JsonObject = {};
  const schemas: JsonObject = { Problem: problemSchema() };
  const container = { $ref: "#/components/pathItems/StoreContainer" };
  const child = { $ref: "#/components/pathItems/StoreChild" };
  const specChild = { $ref: "#/components/pathItems/SpecChild" };

  for (const mount of mounts) {
    const base = mount.basePath;
    switch (mount.service) {
      case "file":
        paths[`${base}/{dirPath}/`] = container;
        paths[`${base}/{filePath}`] = child;
        break;
      case "data": {
        paths[`${base}/`] = container;
        paths[`${base}/{dataset}/`] = container;
        paths[`${base}/{dataset}/{key}`] = child;
        paths[`${base}/{dataset}/.schema.json`] = {
          get: op("Read the dataset schema", "pure"),
          put: op("Install the dataset schema", "idempotent"),
        };
        const data = tenant.instance(mount.basePath)?.[1].data;
        if (data) {
          let names: string[] = [];
          try {
            [names] = await data.listDatasets(10_000, 0);
          } catch {
            names = [];
          }
          for (const name of names) {
            let schema: Json | undefined;
            try {
              schema = await data.getSchema(name);
            } catch {
              continue;
            }
            if (schema === undefined) continue;
            const key = schemaComponentName(base, name);
            schemas[key] = schema;
            paths[`${base}/${name}/{key}`] = storeChildForSchema({ $ref: `#/components/schemas/${key}` });
          }
        }
        break;
      }
      case "pipeline": {
        paths[`${base}/${PIPELINE_SUBTREE}/`] = container;
        paths[`${base}/${PIPELINE_SUBTREE}/{specPath}`] = specChild;
        const root = (await storedSpecs(tenant, mount, PIPELINE_PREFIX)).find(([n]) => n === ROOT_SPEC)?.[1];
        const input = objGet(root ?? null, "input");
        const output = objGet(root ?? null, "output");
        paths[`${base}/{path}`] = {
          description:
            "Execute the longest-prefix-matched stored pipeline (.root governs the mount root). All HTTP verbs pass through to the pipeline.",
          get: opWithSchemas("Run the matched stored pipeline", "unsafe", undefined, output, JSON_MEDIA, JSON_MEDIA),
          post: opWithSchemas("Run the matched stored pipeline with a body", "unsafe", input, output, JSON_MEDIA, JSON_MEDIA),
        };
        break;
      }
      case "query":
        paths[`${base}/${QUERY_SUBTREE}/`] = container;
        paths[`${base}/${QUERY_SUBTREE}/{specPath}`] = specChild;
        paths[`${base}/{queryPath}`] = {
          description:
            "Execute the longest-prefix-matched stored query; extra path segments append positional params; params come from the query string (schema-coerced) and/or a JSON body; results page with X-Total-Count.",
          get: op("Execute the stored query (params from the query string)", "pure"),
          post: op("Execute the stored query (params from the body)", "pure"),
        };
        break;
      case "auth":
        paths[`${base}/login`] = { post: op("Log in (sets the rs-auth cookie, returns a JWT)", "unsafe") };
        paths[`${base}/refresh`] = { post: op("Sliding session refresh", "idempotent") };
        paths[`${base}/logout`] = { post: op("Log out (clears the cookie)", "idempotent") };
        paths[`${base}/user`] = { get: op("The authenticated principal", "pure") };
        break;
      case "services":
        paths[`${base}/catalogue`] = { get: op("Available services and config schemas", "pure") };
        paths[`${base}/raw`] = {
          get: op("The tenant's raw config (ETag-versioned; secrets masked)", "pure"),
          put: op("Replace the tenant config (validated, atomic; If-Match)", "idempotent"),
        };
        paths[`${base}/code/`] = container;
        paths[`${base}/code/{name}/`] = container;
        paths[`${base}/code/{name}/{version}`] = child;
        break;
      case "log":
        paths[base === "" ? "/" : base] = {
          description:
            "Query this tenant's structured logs, newest first. Params: $take, severity (debug|info|warn|error floor), traceId, service (mount or path prefix), since/until (unix-ms or RFC 3339), q (body substring). JSON array of OTLP LogRecords, or text/plain NDJSON via Accept; X-Total-Count set.",
          get: opMedia("Query structured logs", "pure", ["application/json", "text/plain"]),
        };
        paths[`${base}/{traceId}`] = {
          description: "All log records for one trace (request-debugging view).",
          get: op("Read one trace's log records", "pure"),
        };
        break;
      case "wrapper": {
        const input = configGet(mount.config, "inputSchema");
        const output = configGet(mount.config, "outputSchema");
        const item: JsonObject = {
          description: "Single inline pipeline fronting another mount; forwards the exact path beyond the mount with ${url.rest}.",
        };
        for (const method of allowedMethods(mount)) {
          const spec = OPENAPI_VERBS[method];
          if (!spec || method === "MOVE") continue;
          const [key, isWrite, effect] = spec;
          item[key] = opWithSchemas("Run the wrapper pipeline", effect, isWrite ? input : undefined, output, JSON_MEDIA, JSON_MEDIA);
        }
        paths[`${base}/{path}`] = item;
        break;
      }
      default: {
        if (!mount.service.startsWith("code:")) break;
        const manifest = await codeManifest(tenant, mount);
        const input = manifest?.inputSchema;
        const output = manifest?.outputSchema;
        const reqMedia = manifest && typeof manifest.requestMediaType === "string" ? manifest.requestMediaType : JSON_MEDIA;
        const respMedia = manifest && typeof manifest.responseMediaType === "string" ? manifest.responseMediaType : JSON_MEDIA;
        const item: JsonObject = {
          description:
            "Deployed custom-code service; the path beyond the mount is passed to the guest. Schemas/media types come from the deploy manifest.",
        };
        for (const method of allowedMethods(mount)) {
          const spec = OPENAPI_VERBS[method];
          if (!spec || method === "PATCH" || method === "MOVE") continue;
          const [key, isWrite, effect] = spec;
          item[key] = opWithSchemas("Invoke the custom-code service", effect, isWrite ? input : undefined, output, reqMedia, respMedia);
        }
        paths[`${base}/{path}`] = item;
      }
    }
  }

  return {
    openapi: "3.1.0",
    info: { title: `RS2 tenant '${tenantName}'`, version: RS2_VERSION },
    paths,
    components: {
      pathItems: {
        StoreContainer: {
          get: op(
            "List children (application/vnd.rs2.dir+json: {path, entries: [{name, dir, ...}], total}; $take/$skip paginate; X-Total-Count). Stores with the 'list-projection' facet also take $select=<field,dot.path,...> (entries gain a 'fields' object) and $sort=<-field,...> (contractual field-sorted paging); stores with 'meta-sort' take $sort over listing metadata (@name, @size, @lastModified, @contentType, @dir)",
            "pure",
          ),
          post: op(
            "Keyless create: store the body under a generated child name; 201 + Location (stores with the 'echo' facet return the stored representation)",
            "unsafe",
          ),
          delete: op("Delete the container; non-empty containers require ?confirm=<container name> (409 without it)", "idempotent"),
        },
        StoreChild: {
          get: op("Read the stored resource; ETag carries the version", "pure"),
          put: op("Upsert; 201 created / 200 overwritten, empty body", "idempotent"),
          post: op("Upsert and return the stored representation (stores with the 'echo' facet)", "unsafe"),
          patch: op("JSON merge-patch (stores with the 'patch' facet)", "unsafe"),
          delete: op("Delete the resource", "idempotent"),
        },
        SpecChild: {
          get: op("Read the stored spec envelope (pipelines: ?$plan returns the segment plan)", "pure"),
          put: op("Author/replace the spec (validated and canonicalized at write time)", "idempotent"),
          delete: op("Delete the stored spec", "idempotent"),
        },
      },
      responses: {
        Problem: {
          description: "Structured error (RFC 9457)",
          content: { "application/problem+json": { schema: { $ref: "#/components/schemas/Problem" } } },
        },
      },
      schemas,
    },
  };
}

/// method → [openapi key, carries a body, effect]; HEAD/OPTIONS/MOVE are not operations.
const OPENAPI_VERBS: Record<string, [string, boolean, string] | undefined> = {
  GET: ["get", false, "pure"],
  PUT: ["put", true, "idempotent"],
  POST: ["post", true, "unsafe"],
  PATCH: ["patch", true, "unsafe"],
  DELETE: ["delete", false, "idempotent"],
};
