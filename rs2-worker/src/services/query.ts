// `query` — stored parameterized queries (PRD §10.4), store-patterned.
// Authoring lives under the reserved subtree `/<mount>/.queries/…` — a
// normal store-contract surface delegated to the owned FileService via the
// SpecStore (validation at write time). Every other path, on **any verb**,
// executes: the longest stored prefix wins (peeled segments become
// positional params `"0"`, `"1"`, …; a `.root` spec governs the mount
// root), so a stored query can serve plain `GET`. Port of
// `rs2-core/src/services/query.rs`.
//
// Execution parameters, later sources winning: positional URL segments →
// query-string pairs (coerced to the params schema's declared types) →
// JSON body (object named / array positional). Defaults apply, then the
// whole set validates against the envelope's `params` schema.

import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import type { Message } from "../runtime/message";
import { QUERY_SUBTREE, pagination } from "./context";
import type { Service, ServiceContext } from "./context";
import { QueryEnvelope, substituteJson, urlParams } from "./query-template";
import type { SpecStore, SpecValidator } from "./spec-store";

export class QueryService implements Service {
  private constructor(private readonly store: SpecStore) {}

  static fromConfig(config: JsonObject, store: SpecStore): QueryService {
    if (config.queries !== undefined) {
      throw RsError.badRequest(
        "config-defined queries are no longer supported: PUT query envelopes to /<mount>/.queries/<name> instead",
      );
    }
    return new QueryService(store);
  }

  /// The write-time validator: the envelope must parse (schemas compile,
  /// placeholders scan); stored as submitted.
  static validator(): SpecValidator {
    return (doc: Json): Json => {
      QueryEnvelope.parse(doc);
      return doc;
    };
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    if (this.store.isAuthoring(msg)) return this.store.handleAuthoring(msg);

    // ---- execution: any verb, longest stored prefix ----
    const store = ctx.query;
    if (!store) throw RsError.internal("query service has no QueryStore capability");
    const segments = msg.url.serviceSegments();
    const resolved = await this.store.resolve(segments);
    if (!resolved) {
      throw RsError.notFound(
        `no stored query matches '${msg.url.servicePath}' (author one at ${msg.url.basePath}${QUERY_SUBTREE}/…)`,
      );
    }
    const [doc, split] = resolved;
    const envelope = QueryEnvelope.parse(doc);

    // Parameters: positional URL segments, then query-string pairs
    // (schema-coerced), then the JSON body — later wins.
    const params: JsonObject = {};
    segments.slice(split).forEach((seg, i) => {
      params[String(i)] = seg;
    });
    Object.assign(params, urlParams(msg.url.query, envelope.paramsSchema));
    if (msg.body) {
      const body = await msg.body.asJson(ctx.limits.materializedBodyBytes);
      if (Array.isArray(body)) {
        body.forEach((v, i) => {
          params[String(i)] = v;
        });
      } else if (body && typeof body === "object") {
        Object.assign(params, body);
      } else if (body !== null) {
        throw RsError.badRequest("query parameters must be a JSON object or array");
      }
    }
    const prepared = envelope.prepareParams(params);

    // JSON templates substitute structurally here; string templates (SQL)
    // pass through for the adapter to bind.
    const query =
      envelope.language === "json" ? substituteJson(envelope.query, prepared, (v) => store.quote(v)) : envelope.query;

    const [take, skip] = pagination(msg);
    const [rows, total] = await store.runQuery(query, prepared, take, skip);

    const resp = msg.okJson(rows);
    resp.setHeader("x-total-count", String(total));
    return resp;
  }
}
