// Stored-query envelopes and parameter substitution (PRD §10.4). Port of
// `rs2-core/src/services/query_template.rs`.
//
// Substitution is **structural** for JSON-language templates: a string node
// that is exactly `"${name}"` is replaced by the parameter's JSON value
// (numbers stay numbers, never spliced into text — injection-safe by
// construction). `"${name?}"` marks an optional placeholder: when the
// parameter is absent the nearest enclosing object member or array element
// is elided. Placeholders embedded in longer strings splice through the
// adapter's `quote`. An object key that is exactly a placeholder
// substitutes to the parameter's string value, and an object carrying
// `"$if": "${flag?}"` is elided wholesale when the flag is absent or empty.
// String-language templates (SQL) are **not** substituted by the service:
// they pass to the adapter with the validated params intact, so adapters
// bind (prepared statements) instead of splicing.

import type { Validator } from "@cfworker/json-schema";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { compileSchema, validateInstance } from "./schema";

/// `json`: JSON-shaped template, structurally substituted by the service.
/// `text`: string template (SQL family), passed through for the adapter to bind.
export type Language = "json" | "text";

/// Splices a value into an embedded string position (the adapter's `quote`).
export type Quote = (value: Json) => string;

/// A parsed stored-query envelope.
export class QueryEnvelope {
  private constructor(
    readonly language: Language,
    readonly query: Json,
    readonly paramsSchema: Json | undefined,
    readonly outputSchema: Json | undefined,
    /// Compiled once at parse — `prepareParams` runs per request, and
    /// schema compilation dwarfs validation.
    private readonly paramsValidator: Validator | undefined,
  ) {}

  /// Parse and validate an envelope (the `PUT`-time check): the shape,
  /// schema compilation, and — for JSON templates — a placeholder scan.
  static parse(doc: Json): QueryEnvelope {
    if (!doc || typeof doc !== "object" || Array.isArray(doc)) {
      throw RsError.badRequest("a stored query is a JSON object envelope");
    }
    const query = doc.query;
    if (query === undefined) throw RsError.badRequest("stored query envelope requires a 'query'");
    const declared = typeof doc.language === "string" ? doc.language : undefined;
    let language: Language;
    if (declared === "json") language = "json";
    else if (declared === "sql" || declared === "text") language = "text";
    else if (declared !== undefined) throw RsError.badRequest(`unknown query language '${declared}' (json | sql)`);
    else language = typeof query === "string" ? "text" : "json";
    if (language === "text" && typeof query !== "string") {
      throw RsError.badRequest("a sql/text query must be a string template");
    }
    const paramsValidator =
      doc.params !== undefined
        ? compileSchema(doc.params, (e) => RsError.badRequest(`'params' is not a valid JSON Schema: ${e}`))
        : undefined;
    if (doc.output !== undefined) {
      compileSchema(doc.output, (e) => RsError.badRequest(`'output' is not a valid JSON Schema: ${e}`));
    }
    // Placeholder scan: malformed placeholders fail at write time.
    if (language === "json") scanPlaceholders(query);
    return new QueryEnvelope(language, query, doc.params, doc.output, paramsValidator);
  }

  /// Apply schema `default`s for missing top-level params, then validate.
  prepareParams(params: JsonObject): JsonObject {
    if (this.paramsSchema !== undefined) {
      const props =
        this.paramsSchema && typeof this.paramsSchema === "object" && !Array.isArray(this.paramsSchema)
          ? this.paramsSchema.properties
          : undefined;
      if (props && typeof props === "object" && !Array.isArray(props)) {
        for (const [name, prop] of Object.entries(props)) {
          if (params[name] !== undefined || !prop || typeof prop !== "object" || Array.isArray(prop)) continue;
          if (prop.default !== undefined) params[name] = prop.default;
        }
      }
      const validator = this.paramsValidator;
      if (!validator) throw RsError.internal("params schema was not compiled at parse");
      const errors = validateInstance(validator, params).map((e) => {
        const o = e as JsonObject;
        return { path: o.path ?? "", error: o.message ?? "" };
      });
      if (errors.length) {
        throw RsError.validationFailed("query parameters failed validation", errors);
      }
    }
    return params;
  }
}

/// Rust `percent_decode_str(..).decode_utf8_lossy()`.
function percentDecodeLossy(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s.replace(/%[0-9a-fA-F]{2}/g, (m) => {
      try {
        return decodeURIComponent(m);
      } catch {
        return "�";
      }
    });
  }
}

/// Parse query-string pairs into params, coerced to the params schema's
/// declared property types (`number`/`integer`/`boolean` parse; everything
/// else stays a string; unparseable values stay strings so schema
/// validation reports them). `$`-prefixed keys (`$take`, `$skip`, …) are
/// runtime controls, not params.
export function urlParams(query: string, schema: Json | undefined): JsonObject {
  const props =
    schema && typeof schema === "object" && !Array.isArray(schema) && schema.properties && typeof schema.properties === "object" && !Array.isArray(schema.properties)
      ? schema.properties
      : undefined;
  const out: JsonObject = {};
  for (const pair of query.split("&")) {
    if (pair === "") continue;
    const eq = pair.indexOf("=");
    const k = eq < 0 ? pair : pair.slice(0, eq);
    const v = eq < 0 ? "" : pair.slice(eq + 1);
    if (k.startsWith("$")) continue;
    const key = percentDecodeLossy(k).replace(/\+/g, " ");
    const raw = percentDecodeLossy(v).replace(/\+/g, " ");
    const prop = props?.[key];
    const declared =
      prop && typeof prop === "object" && !Array.isArray(prop) && typeof prop.type === "string" ? prop.type : undefined;
    let value: Json = raw;
    if (declared === "number") {
      const n = parseFloatStrict(raw);
      if (n !== undefined) value = n;
    } else if (declared === "integer") {
      // Rust `i64` parse: decimal digits with an optional sign, no dot.
      if (/^[+-]?\d+$/.test(raw) && Number.isSafeInteger(Number(raw))) value = Number(raw);
    } else if (declared === "boolean") {
      if (raw === "true") value = true;
      else if (raw === "false") value = false;
    }
    out[key] = value;
  }
  return out;
}

/// Rust `f64` parse, minus the `inf`/`NaN` spellings JSON cannot carry.
function parseFloatStrict(raw: string): number | undefined {
  if (!/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(raw)) return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

/// One placeholder: `${name}` / `${name?}`.
interface Placeholder {
  name: string;
  optional: boolean;
}

function parseExact(s: string): Placeholder | undefined {
  if (!s.startsWith("${") || !s.endsWith("}")) return undefined;
  const inner = s.slice(2, -1);
  if (inner === "" || /[{}$]/.test(inner)) return undefined;
  return inner.endsWith("?") ? { name: inner.slice(0, -1), optional: true } : { name: inner, optional: false };
}

/// Scan a JSON template for malformed placeholders (write-time check).
function scanPlaceholders(template: Json): void {
  if (typeof template === "string") {
    const s = template;
    if (s.includes("${") && parseExact(s) === undefined) {
      // Embedded placeholders: each must close.
      let rest = s;
      for (let start = rest.indexOf("${"); start >= 0; start = rest.indexOf("${")) {
        const after = rest.slice(start + 2);
        const end = after.indexOf("}");
        if (end < 0) throw RsError.badRequest(`unterminated placeholder in '${s}'`);
        rest = after.slice(end + 1);
      }
    }
    return;
  }
  if (Array.isArray(template)) {
    for (const v of template) scanPlaceholders(v);
  } else if (template && typeof template === "object") {
    for (const v of Object.values(template)) scanPlaceholders(v);
  }
}

/// Optional placeholder with no param: elide the enclosing member.
const ELIDE: unique symbol = Symbol("elide");

/// Structurally substitute a JSON template (see module docs). `quote`
/// splices values into embedded string positions.
export function substituteJson(template: Json, params: JsonObject, quote: Quote): Json {
  const out = substNode(template, params, quote);
  if (out === ELIDE) {
    throw RsError.badRequest("the whole query is an optional placeholder with no parameter");
  }
  return out;
}

function substNode(node: Json, params: JsonObject, quote: Quote): Json | typeof ELIDE {
  if (typeof node === "string") {
    const ph = parseExact(node);
    if (ph) {
      const v = params[ph.name];
      if (v !== undefined) return v;
      if (ph.optional) return ELIDE;
      throw RsError.badRequest(`missing query parameter '${ph.name}'`);
    }
    if (node.includes("$")) return splice(node, params, quote);
    return node;
  }
  if (Array.isArray(node)) {
    const out: Json[] = [];
    for (const v of node) {
      const s = substNode(v, params, quote);
      if (s !== ELIDE) out.push(s); // drop elided elements
    }
    return out;
  }
  if (node && typeof node === "object") {
    // A `"$if": "${flag?}"` member gates its whole object: when the
    // parameter is absent or empty (null, false, "", []) the object is
    // elided from its parent — the structural equivalent of v1 Mongo's
    // `_include`-gated conditional clauses. Otherwise the marker itself is
    // dropped and the object substitutes normally.
    const cond = node.$if;
    if (cond !== undefined) {
      const ph = typeof cond === "string" ? parseExact(cond) : undefined;
      if (!ph) {
        throw RsError.badRequest(`$if must be a '\${name}' or '\${name?}' placeholder, got ${JSON.stringify(cond)}`);
      }
      const v = params[ph.name];
      let keep: boolean;
      if (v === undefined) {
        if (!ph.optional) {
          throw RsError.badRequest(`missing query parameter '${ph.name}' (required $if condition)`);
        }
        keep = false;
      } else if (v === null || v === false) keep = false;
      else if (typeof v === "string") keep = v !== "";
      else if (Array.isArray(v)) keep = v.length !== 0;
      else keep = true;
      if (!keep) return ELIDE;
    }
    const out: JsonObject = {};
    for (const [k, v] of Object.entries(node)) {
      if (k === "$if") continue;
      // Keys may themselves be exact placeholders (v1's
      // `{${sortField}: ${sortDir}}`); an absent optional key elides the
      // member. Key parameters must be strings — a spliced/quoted key
      // would be an injection surface.
      let key = k;
      const ph = parseExact(k);
      if (ph) {
        const param = params[ph.name];
        if (typeof param === "string") key = param;
        else if (param !== undefined) {
          throw RsError.badRequest(
            `query parameter '${ph.name}' is used as an object key and must be a string, got ${JSON.stringify(param)}`,
          );
        } else if (ph.optional) continue;
        else throw RsError.badRequest(`missing query parameter '${ph.name}'`);
      }
      const s = substNode(v, params, quote);
      if (s !== ELIDE) out[key] = s; // drop elided members
    }
    return out;
  }
  return node;
}

/// Splice `${name}` / `$0` placeholders embedded in a longer string,
/// adapter-quoted. Missing params are errors — never silent ''.
function splice(s: string, params: JsonObject, quote: Quote): string {
  let out = "";
  let rest = s;
  for (let start = rest.indexOf("$"); start >= 0; start = rest.indexOf("$")) {
    out += rest.slice(0, start);
    const after = rest.slice(start);
    if (after.startsWith("${")) {
      const body = after.slice(2);
      const end = body.indexOf("}");
      if (end < 0) throw RsError.badRequest(`unterminated placeholder in '${s}'`);
      const raw = body.slice(0, end);
      const optional = raw.endsWith("?");
      const name = optional ? raw.slice(0, -1) : raw;
      const v = params[name];
      if (v !== undefined) out += quote(v);
      else if (!optional) throw RsError.badRequest(`missing query parameter '${name}'`);
      rest = body.slice(end + 1);
    } else {
      const digits = (/^\d*/.exec(after.slice(1)) ?? [""])[0];
      if (digits === "") {
        out += "$";
        rest = after.slice(1);
      } else {
        const v = params[digits];
        if (v === undefined) throw RsError.badRequest(`missing positional query parameter '$${digits}'`);
        out += quote(v);
        rest = after.slice(1 + digits.length);
      }
    }
  }
  return out + rest;
}
