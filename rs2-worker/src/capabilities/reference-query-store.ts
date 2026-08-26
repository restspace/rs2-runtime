// Query adapter over any `DataStore` (PRD §10.4): the default adapter for
// deployments without a SQL backend, and the reference for the `QueryStore`
// contract. Port of `rs2-core/src/adapters/mem_query.rs`; registered as
// `builtin:reference`.
//
// Query template shape:
//
//   { "dataset": "orders",
//     "where": { "status": "open", "total": { "op": ">", "value": 100 } },
//     "orderBy": "createdAt" }
//
// `where` clauses AND together; field names may be dot-paths. Supported
// ops: `==` (default), `!=`, `<`, `>`, `<=`, `>=`, `contains`.

import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { compareUtf8 } from "../runtime/listing";
import type { DataStore, QueryStore } from "./types";

function field(record: Json, path: string): Json | undefined {
  let current: Json = record;
  for (const part of path.split(".")) {
    if (!current || typeof current !== "object" || Array.isArray(current)) return undefined;
    if (!Object.prototype.hasOwnProperty.call(current, part)) return undefined;
    current = current[part]!;
  }
  return current;
}

/// serde_json `Value` equality (order-insensitive on object keys).
function jsonEq(a: Json, b: Json): boolean {
  if (a === b) return true;
  if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((v, i) => jsonEq(v, b[i]!));
  }
  const keys = Object.keys(a);
  if (keys.length !== Object.keys(b).length) return false;
  return keys.every((k) => Object.prototype.hasOwnProperty.call(b, k) && jsonEq(a[k]!, b[k]!));
}

function matches(record: Json, path: string, clause: Json): boolean {
  let op = "==";
  let expected: Json = clause;
  if (clause && typeof clause === "object" && !Array.isArray(clause) && clause.op !== undefined) {
    if (typeof clause.op === "string") op = clause.op;
    expected = clause.value ?? null;
  }
  const actual = field(record, path);
  // Ordering exists for number/number (as floats) and string/string
  // (byte-wise UTF-8, matching Rust `str::cmp`) only.
  let ord: number | undefined;
  if (typeof actual === "number" && typeof expected === "number") {
    ord = actual < expected ? -1 : actual > expected ? 1 : 0;
  } else if (typeof actual === "string" && typeof expected === "string") {
    ord = compareUtf8(actual, expected);
  }
  switch (op) {
    case "==":
      return actual !== undefined && jsonEq(actual, expected);
    case "!=":
      return actual === undefined || !jsonEq(actual, expected);
    case "<":
      return ord === -1;
    case ">":
      return ord === 1;
    case "<=":
      return ord === -1 || ord === 0;
    case ">=":
      return ord === 1 || ord === 0;
    case "contains":
      if (typeof actual === "string" && typeof expected === "string") return actual.includes(expected);
      if (Array.isArray(actual)) return actual.some((v) => jsonEq(v, expected));
      return false;
    default:
      throw RsError.badRequest(`unknown query op '${op}'`);
  }
}

export class ReferenceQueryStore implements QueryStore {
  constructor(private readonly data: DataStore) {}

  async runQuery(tenant: string, query: Json, _params: JsonObject, take: number, skip: number): Promise<[Json[], number]> {
    // JSON templates arrive structurally substituted; string-language
    // (SQL) templates are for binding adapters, not this one.
    if (typeof query === "string") {
      throw RsError.engineUnavailable("this query adapter executes JSON templates; sql/text queries need a SQL adapter");
    }
    const obj = query && typeof query === "object" && !Array.isArray(query) ? query : {};
    const dataset = typeof obj.dataset === "string" ? obj.dataset : undefined;
    if (dataset === undefined) throw RsError.badRequest("query template requires a 'dataset'");
    let clauses: JsonObject = {};
    if (obj.where !== undefined) {
      if (!obj.where || typeof obj.where !== "object" || Array.isArray(obj.where)) {
        throw RsError.badRequest("'where' must be an object");
      }
      clauses = obj.where;
    }

    // Scan the dataset (the reference adapter trades efficiency for having
    // no backend dependency; SQL adapters push this down). The store
    // filters in one pass, so non-matching records never leave it.
    const keep = (record: Json): boolean => Object.entries(clauses).every(([path, clause]) => matches(record, path, clause));
    const matched = await this.data.scanMatching(tenant, dataset, keep);
    const rows = matched.map(([key, record]) => {
      if (record && typeof record === "object" && !Array.isArray(record) && !Object.prototype.hasOwnProperty.call(record, "_key")) {
        record._key = key;
      }
      return record;
    });

    const orderBy = typeof obj.orderBy === "string" ? obj.orderBy : undefined;
    if (orderBy !== undefined) {
      // Stable sort (guaranteed by JS); mixed types compare Equal.
      rows.sort((a, b) => {
        const x = field(a, orderBy);
        const y = field(b, orderBy);
        if (typeof x === "number" && typeof y === "number") return x < y ? -1 : x > y ? 1 : 0;
        if (typeof x === "string" && typeof y === "string") return compareUtf8(x, y);
        return 0;
      });
    }

    const total = rows.length;
    return [rows.slice(skip, skip + take), total];
  }

  quote(value: Json): string {
    if (typeof value === "string") return value;
    if (typeof value === "number") return JSON.stringify(value);
    if (typeof value === "boolean") return String(value);
    const kind = value === null ? "null" : Array.isArray(value) ? "array" : "object";
    throw RsError.badRequest(`cannot splice a ${kind} into a string query position`);
  }
}
