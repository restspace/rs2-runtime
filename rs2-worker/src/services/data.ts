// `data` service (PRD §10.2): schema-validated JSON store over a
// `DataStore` capability. Port of `rs2-core/src/services/data.rs`. The
// record ETag is `"<sha256(JSON)[0..16]>"` over the unredacted value
// (opaque by contract; the Rust host uses a different hash).

import type { ScopedDataStore } from "../capabilities/scoped";
import { ifMatchHits } from "../capabilities/types";
import { Body } from "../runtime/body";
import { sha256Hex } from "../runtime/crypto";
import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { parseListSpec, stringifyJson } from "../runtime/listing";
import { MediaType, SCHEMA_JSON } from "../runtime/media-type";
import type { Message } from "../runtime/message";
import { simpleUuid } from "../runtime/message";
import { isOperator, satisfiesRoleSpec } from "../runtime/wrapper";
import { ifNoneMatchHits, pagination, writePrecondition } from "./context";
import type { Service, ServiceContext } from "./context";
import { compileSchema, validateInstance } from "./schema";

const SCHEMA_RESOURCE = ".schema.json";
/// Mount-level schema index: `GET /<mount>/.schemas`.
const SCHEMAS_RESOURCE = ".schemas";

/// Record version for ETags: a stable hash of the serialized value.
export async function recordEtag(value: Json): Promise<string> {
  return `"${(await sha256Hex(stringifyJson(value))).slice(0, 16)}"`;
}

/// RFC 7386 JSON merge patch (returns the merged value).
export function mergePatch(target: Json, patch: Json): Json {
  if (patch && typeof patch === "object" && !Array.isArray(patch)) {
    const out: JsonObject =
      target && typeof target === "object" && !Array.isArray(target) ? { ...target } : ({} as JsonObject);
    for (const [k, v] of Object.entries(patch)) {
      if (v === null) delete out[k];
      else out[k] = mergePatch(Object.prototype.hasOwnProperty.call(out, k) ? out[k]! : null, v);
    }
    return out;
  }
  return patch;
}

/// Top-level per-field access rules from a dataset schema.
function fieldRules(schema: Json): Array<[string, string | undefined, string | undefined]> {
  if (!schema || typeof schema !== "object" || Array.isArray(schema)) return [];
  const props = schema.properties;
  if (!props || typeof props !== "object" || Array.isArray(props)) return [];
  const out: Array<[string, string | undefined, string | undefined]> = [];
  for (const [name, def] of Object.entries(props)) {
    if (!def || typeof def !== "object" || Array.isArray(def)) continue;
    const read = typeof def["x-rs-read"] === "string" ? (def["x-rs-read"] as string) : undefined;
    const write = typeof def["x-rs-write"] === "string" ? (def["x-rs-write"] as string) : undefined;
    if (read !== undefined || write !== undefined) out.push([name, read, write]);
  }
  return out;
}

async function getOrNull(data: ScopedDataStore, dataset: string, key: string): Promise<Json> {
  try {
    return await data.get(dataset, key);
  } catch (e) {
    if (e instanceof RsError && e.code === codes.NOT_FOUND) return null;
    throw e;
  }
}

/// Drop the top-level fields whose `x-rs-read` the caller doesn't satisfy.
function redactFields(value: Json, schema: Json, msg: Message): void {
  if (!value || typeof value !== "object" || Array.isArray(value)) return;
  for (const [name, read] of fieldRules(schema)) {
    if (read !== undefined && !satisfiesRoleSpec(read, msg)) delete value[name];
  }
}

function jsonEq(a: Json | undefined, b: Json | undefined): boolean {
  if (a === undefined || b === undefined) return a === b;
  return stringifyJson(a) === stringifyJson(b);
}

/// Enforce per-field write rules on the record a write would produce.
function enforceWriteRules(finalValue: Json, stored: Json, schema: Json, msg: Message): void {
  if (!finalValue || typeof finalValue !== "object" || Array.isArray(finalValue)) return;
  const storedObj = stored && typeof stored === "object" && !Array.isArray(stored) ? stored : undefined;
  for (const [name, read, write] of fieldRules(schema)) {
    const readable = read === undefined || satisfiesRoleSpec(read, msg);
    const writable = write === undefined || satisfiesRoleSpec(write, msg);
    const storedField = storedObj && Object.prototype.hasOwnProperty.call(storedObj, name) ? storedObj[name] : undefined;
    if (!readable) {
      if (storedField !== undefined) finalValue[name] = storedField;
      else delete finalValue[name];
    } else if (!writable) {
      const incoming = Object.prototype.hasOwnProperty.call(finalValue, name) ? finalValue[name] : undefined;
      if (!jsonEq(incoming, storedField)) throw RsError.forbidden(`field '${name}' is not writable by this principal`);
    }
  }
}

export class DataService implements Service {
  private readonly enforceSchema: boolean;
  private readonly fieldAuthz: boolean;

  constructor(enforceSchema = false, fieldAuthz = false) {
    this.enforceSchema = enforceSchema;
    this.fieldAuthz = fieldAuthz;
  }

  static fromConfig(config: JsonObject): DataService {
    const es = config.enforceSchema;
    const fa = config.fieldLevelAuthz;
    // A wrong-typed field fails the whole parse → defaults (`unwrap_or_default`).
    if ((es !== undefined && typeof es !== "boolean") || (fa !== undefined && typeof fa !== "boolean")) {
      return new DataService();
    }
    return new DataService(es === true, fa === true);
  }

  private validate(dataset: string, schema: Json, instance: Json): void {
    const validator = compileSchema(schema, (e) =>
      RsError.badRequest(`dataset schema is not a valid JSON Schema: ${e}`),
    );
    const errors = validateInstance(validator, instance);
    if (errors.length) {
      throw RsError.validationFailed(`body does not conform to the '${dataset}' dataset schema`, errors);
    }
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const data = ctx.data;
    if (!data) throw RsError.capabilityDenied("data");
    const enforceSchema = this.enforceSchema;
    const fieldAuthz = this.fieldAuthz;
    const segments = msg.url.serviceSegments();
    const schemaBase = msg.url.basePath;

    // ---- mount root: enumerate datasets ----
    if (segments.length === 0) {
      if (msg.method !== "GET") throw RsError.badRequest("the mount root supports GET (dataset listing)");
      const [take, skip] = pagination(msg);
      const [names, total] = await data.listDatasets(take, skip);
      const entries = names.map((n) => ({ name: `${n}/`, dir: true }));
      const resp = msg.ok(Body.fromString(JSON.stringify({ path: "/", entries, total }), MediaType.dirJson()));
      resp.setHeader("x-total-count", String(total));
      return resp;
    }

    // ---- mount-level schema index ----
    if (segments.length === 1 && segments[0] === SCHEMAS_RESOURCE) {
      if (msg.method !== "GET") throw RsError.badRequest(".schemas supports GET");
      const [names] = await data.listDatasets(10_000, 0);
      const schemas: JsonObject = {};
      for (const name of names) {
        const schema = await data.getSchema(name);
        if (schema !== undefined) {
          schemas[name] = { schemaUrl: `${schemaBase}/${name}/${SCHEMA_RESOURCE}`, schema };
        }
      }
      return msg.ok(Body.fromString(JSON.stringify({ schemas }), MediaType.json()));
    }

    // ---- dataset level (a store container) ----
    if (segments.length === 1) {
      const dataset = segments[0]!;
      const select = msg.url.queryParam("$select");
      if (msg.method === "GET" && select !== undefined) {
        const [take, skip] = pagination(msg);
        const spec = parseListSpec(select, msg.url.queryParam("$sort"), take, skip);
        const [page, total] = await data.listRecords(dataset, spec);
        const schema = fieldAuthz ? await data.getSchema(dataset) : undefined;
        const entries = page.map(([key, fields]) => {
          if (schema !== undefined) redactFields(fields, schema, msg);
          return { name: key, dir: false, contentType: "application/json", fields };
        });
        const listing = { path: `/${dataset}/`, entries, total };
        const resp = msg.ok(Body.fromString(stringifyJson(listing), MediaType.dirJson()));
        resp.setHeader("x-total-count", String(total));
        return resp;
      }
      switch (msg.method) {
        case "GET": {
          if (msg.url.queryParam("$sort") !== undefined) {
            throw RsError.badRequest("$sort requires $select (projected listing)");
          }
          const [take, skip] = pagination(msg);
          const [keys, total] = await data.listKeys(dataset, take, skip);
          const entries: Json[] = keys.map((k) => ({ name: k, dir: false, contentType: "application/json" }));
          if ((await data.getSchema(dataset)) !== undefined) {
            entries.push({ name: SCHEMA_RESOURCE, dir: false, fixed: true });
          }
          const listing = { path: `/${dataset}/`, entries, total };
          const resp = msg.ok(Body.fromString(JSON.stringify(listing), MediaType.dirJson()));
          resp.setHeader("x-total-count", String(total));
          return resp;
        }
        case "POST": {
          if (!msg.body) throw RsError.badRequest("write requires a JSON body");
          const value = await msg.body.asJson(ctx.limits.materializedBodyBytes);
          const schema = fieldAuthz || enforceSchema ? await data.getSchema(dataset) : undefined;
          if (fieldAuthz && schema !== undefined) enforceWriteRules(value, null, schema, msg);
          if (enforceSchema && schema !== undefined) this.validate(dataset, schema, value);
          const key = simpleUuid();
          const etag = await recordEtag(value);
          await data.put(dataset, key, value);
          const schemaUrl = `${schemaBase}/${dataset}/${SCHEMA_RESOURCE}`;
          if (fieldAuthz && schema !== undefined) redactFields(value, schema, msg);
          const resp = msg.response(201, Body.fromString(stringifyJson(value), MediaType.json()).withSchema(schemaUrl));
          resp.setHeader("location", `${schemaBase}/${dataset}/${key}`);
          resp.setHeader("etag", etag);
          return resp;
        }
        case "DELETE": {
          if (writePrecondition(msg).kind !== "none") {
            throw RsError.badRequest("conditional headers are not supported on dataset deletes");
          }
          if (msg.url.queryParam("confirm") !== dataset) {
            throw RsError.conflict(`dataset delete requires '?confirm=${dataset}'`);
          }
          await data.deleteDataset(dataset);
          return msg.noContent();
        }
        default:
          throw RsError.badRequest("dataset level supports GET, POST, DELETE");
      }
    }

    // ---- dataset schema ----
    if (segments.length === 2 && segments[1] === SCHEMA_RESOURCE) {
      const dataset = segments[0]!;
      switch (msg.method) {
        case "GET": {
          const schema = await data.getSchema(dataset);
          if (schema === undefined) throw RsError.notFound(`dataset '${dataset}' has no schema`);
          return msg.ok(Body.fromString(JSON.stringify(schema), new MediaType(SCHEMA_JSON)));
        }
        case "PUT": {
          if (fieldAuthz && !isOperator(msg.principal, ctx.operatorRoles ?? "")) {
            throw RsError.forbidden("editing the schema on a fieldLevelAuthz mount requires an operator");
          }
          if (!msg.body) throw RsError.badRequest("schema write requires a body");
          const schema = await msg.body.asJson(ctx.limits.materializedBodyBytes);
          compileSchema(schema, (e) => RsError.badRequest(`not a valid JSON Schema: ${e}`));
          await data.putSchema(dataset, schema);
          return msg.ok(undefined);
        }
        default:
          throw RsError.badRequest("schema resource supports GET and PUT");
      }
    }

    // ---- record level ----
    if (segments.length === 2) {
      const [dataset, key] = segments as [string, string];
      const schemaUrl = `${schemaBase}/${dataset}/${SCHEMA_RESOURCE}`;
      switch (msg.method) {
        case "GET": {
          const value = await data.get(dataset, key);
          const etag = await recordEtag(value);
          if (ifNoneMatchHits(msg.header("if-none-match"), etag)) {
            const notModified = msg.response(304, undefined);
            notModified.setHeader("etag", etag);
            return notModified;
          }
          if (fieldAuthz) {
            const schema = await data.getSchema(dataset);
            if (schema !== undefined) redactFields(value, schema, msg);
          }
          const resp = msg.ok(Body.fromString(stringifyJson(value), MediaType.json()).withSchema(schemaUrl));
          resp.setHeader("link", `<${schemaUrl}>; rel="describedby"`);
          resp.setHeader("etag", etag);
          return resp;
        }
        case "PUT":
        case "POST": {
          const pre = writePrecondition(msg);
          if (pre.kind === "ifMatch") {
            const cur = await getOrNull(data, dataset, key);
            if (cur === null) throw RsError.preconditionFailed("If-Match given but the record does not exist");
            if (!ifMatchHits(pre.value, await recordEtag(cur))) {
              throw RsError.preconditionFailed("If-Match does not match the current ETag — re-read and retry");
            }
          } else if (pre.kind === "ifNoneMatchStar") {
            if ((await getOrNull(data, dataset, key)) !== null) {
              throw RsError.preconditionFailed("If-None-Match: * given but the record already exists");
            }
          }
          const echo = msg.method === "POST";
          if (!msg.body) throw RsError.badRequest("write requires a JSON body");
          const value = await msg.body.asJson(ctx.limits.materializedBodyBytes);
          const schema = fieldAuthz || enforceSchema ? await data.getSchema(dataset) : undefined;
          if (fieldAuthz && schema !== undefined) {
            const stored = await getOrNull(data, dataset, key);
            enforceWriteRules(value, stored, schema, msg);
          }
          if (enforceSchema && schema !== undefined) this.validate(dataset, schema, value);
          const etag = await recordEtag(value);
          const created = await data.put(dataset, key, value);
          let stored: Body | undefined;
          if (echo) {
            if (fieldAuthz && schema !== undefined) redactFields(value, schema, msg);
            stored = Body.fromString(stringifyJson(value), MediaType.json()).withSchema(schemaUrl);
          }
          const resp = msg.response(created ? 201 : 200, stored);
          resp.setHeader("etag", etag);
          return resp;
        }
        case "PATCH": {
          if (!msg.body) throw RsError.badRequest("patch requires a JSON body");
          const patch = await msg.body.asJson(ctx.limits.materializedBodyBytes);
          const stored = await data.get(dataset, key);
          const current = mergePatch(stored, patch);
          const schema = fieldAuthz || enforceSchema ? await data.getSchema(dataset) : undefined;
          if (fieldAuthz && schema !== undefined) enforceWriteRules(current, stored, schema, msg);
          if (enforceSchema && schema !== undefined) this.validate(dataset, schema, current);
          await data.put(dataset, key, current);
          if (fieldAuthz && schema !== undefined) redactFields(current, schema, msg);
          return msg.ok(Body.fromString(stringifyJson(current), MediaType.json()).withSchema(schemaUrl));
        }
        case "DELETE": {
          const pre = writePrecondition(msg);
          if (pre.kind === "ifMatch") {
            const cur = await getOrNull(data, dataset, key);
            if (cur !== null && !ifMatchHits(pre.value, await recordEtag(cur))) {
              throw RsError.preconditionFailed("If-Match does not match the current ETag — re-read and retry");
            }
          } else if (pre.kind === "ifNoneMatchStar") {
            if ((await getOrNull(data, dataset, key)) !== null) {
              throw RsError.preconditionFailed("If-None-Match: * given but the record exists");
            }
          }
          await data.delete(dataset, key);
          return msg.noContent();
        }
        default:
          throw RsError.badRequest("record level supports GET, PUT, POST, PATCH, DELETE");
      }
    }

    throw RsError.badRequest("data paths are /<dataset>/<key>");
  }
}
