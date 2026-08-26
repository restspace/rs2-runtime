// Path-pattern resolution: build a string from `${…}` placeholders over the
// URL plane (`${url.…}`) and the data plane (`${field.path}`). Port of
// `rs2-core/src/path_pattern.rs`; the grammar is verbatim.

import { RsError } from "./error";
import type { Json, JsonObject } from "./error";

/// View of the URL plane a pattern reads. `path` is the segment list the
/// pattern indexes against (the peeled sub-path for a pipeline).
export interface UrlView {
  path: readonly string[];
  base: readonly string[];
  name: string | undefined;
  query: string;
  /// The verbatim service-path remainder (`MsgUrl.servicePath`).
  rest: string;
}

export const EMPTY_URL: UrlView = { path: [], base: [], name: undefined, query: "", rest: "" };

type Token = { kind: "value"; value: string } | { kind: "elide" };

type Expr = { kind: "literal"; text: string } | { kind: "url"; sel: UrlSel } | { kind: "data"; path: string };

type Section = "path" | "base" | "full";
type Index = { kind: "at"; i: number } | { kind: "slice"; a: number | undefined; b: number | undefined };

type UrlSel =
  | { kind: "segments"; section: Section; index: Index | undefined }
  | { kind: "name" }
  | { kind: "rest" }
  | { kind: "queryAll" }
  | { kind: "queryKey"; key: string };

interface ParsedToken {
  primary: Expr;
  optional: boolean;
  default_: Expr | undefined;
}

class PatternError extends Error {}

/// Resolve all `${…}` placeholders in `pattern`.
export function resolve(pattern: string, url: UrlView, data: JsonObject): string {
  let out = "";
  let rest = pattern;
  for (;;) {
    const start = rest.indexOf("${");
    if (start < 0) break;
    out += rest.slice(0, start);
    const after = rest.slice(start + 2);
    const end = after.indexOf("}");
    if (end < 0) throw RsError.badRequest(`unterminated \${…} in '${pattern}'`);
    const interior = after.slice(0, end);
    let token: Token;
    try {
      token = evalToken(interior, url, data);
    } catch (e) {
      if (e instanceof PatternError) throw RsError.badRequest(`in pattern '${pattern}': ${e.message}`);
      throw e;
    }
    if (token.kind === "value") {
      out += token.value;
    } else if (out.endsWith("/")) {
      // Collapse a separator the elision would have left dangling.
      out = out.slice(0, -1);
    }
    rest = after.slice(end + 1);
  }
  out += rest;
  return out;
}

/// Validate every `${…}` placeholder parses (config-time check).
export function validate(pattern: string): void {
  let rest = pattern;
  for (;;) {
    const start = rest.indexOf("${");
    if (start < 0) break;
    const after = rest.slice(start + 2);
    const end = after.indexOf("}");
    if (end < 0) throw RsError.badRequest(`unterminated \${…} in '${pattern}'`);
    try {
      parseToken(after.slice(0, end));
    } catch (e) {
      if (e instanceof PatternError) throw RsError.badRequest(`in pattern '${pattern}': ${e.message}`);
      throw e;
    }
    rest = after.slice(end + 1);
  }
}

function parseToken(interior: string): ParsedToken {
  // Split off `|| default` at the first top-level `||`.
  const i = interior.indexOf("||");
  let primarySrc = i >= 0 ? interior.slice(0, i).trim() : interior.trim();
  const defaultSrc = i >= 0 ? interior.slice(i + 2).trim() : undefined;
  // A trailing `?` on the primary marks it optional.
  let optional = false;
  if (primarySrc.endsWith("?")) {
    primarySrc = primarySrc.slice(0, -1).trimEnd();
    optional = true;
  }
  return {
    primary: parseExpr(primarySrc),
    optional,
    default_: defaultSrc !== undefined ? parseExpr(defaultSrc) : undefined,
  };
}

function parseExpr(s: string): Expr {
  if (s === "") throw new PatternError("empty placeholder");
  if (s.length >= 2 && s.startsWith("'") && s.endsWith("'")) {
    return { kind: "literal", text: s.slice(1, -1) };
  }
  if (s === "url" || s.startsWith("url.")) {
    return { kind: "url", sel: parseUrlSel(s) };
  }
  return { kind: "data", path: s };
}

function parseUrlSel(s: string): UrlSel {
  if (!s.startsWith("url.")) {
    throw new PatternError(`'${s}': use url.path / url.base / url.full / url.name / url.query`);
  }
  const rest = s.slice(4);
  const m = rest.search(/[.[]/);
  const headEnd = m < 0 ? rest.length : m;
  const section = rest.slice(0, headEnd);
  const tail = rest.slice(headEnd);
  switch (section) {
    case "name":
      requireEmpty(tail, "url.name");
      return { kind: "name" };
    case "rest":
      requireEmpty(tail, "url.rest");
      return { kind: "rest" };
    case "query":
      if (tail === "") return { kind: "queryAll" };
      if (tail.startsWith(".")) {
        const key = tail.slice(1);
        if (key === "" || /[.[]/.test(key)) throw new PatternError(`'${s}': expected url.query.<key>`);
        return { kind: "queryKey", key };
      }
      throw new PatternError(`'${s}': query takes a .<key>, not an index`);
    case "path":
    case "base":
    case "full":
      return { kind: "segments", section, index: tail === "" ? undefined : parseIndex(tail) };
    default:
      throw new PatternError(`'${s}': unknown url section '${section}'`);
  }
}

function parseIndex(tail: string): Index {
  if (!(tail.startsWith("[") && tail.endsWith("]"))) {
    throw new PatternError(`malformed index '${tail}' (expected [n], [-1], or [a:b])`);
  }
  const inner = tail.slice(1, -1);
  const c = inner.indexOf(":");
  if (c >= 0) {
    return { kind: "slice", a: parseOptInt(inner.slice(0, c)), b: parseOptInt(inner.slice(c + 1)) };
  }
  return { kind: "at", i: parseInt_(inner) };
}

function parseOptInt(s: string): number | undefined {
  const t = s.trim();
  return t === "" ? undefined : parseInt_(t);
}

function parseInt_(s: string): number {
  const t = s.trim();
  if (!/^[+-]?\d+$/.test(t)) throw new PatternError(`'${s}' is not an integer index`);
  return parseInt(t, 10);
}

function requireEmpty(tail: string, what: string): void {
  if (tail !== "") throw new PatternError(`${what} takes no index or key`);
}

function evalToken(interior: string, url: UrlView, data: JsonObject): Token {
  const parsed = parseToken(interior);
  const v = evalExpr(parsed.primary, url, data);
  if (v !== undefined && v !== "") return { kind: "value", value: v };
  if (parsed.default_) return { kind: "value", value: evalExpr(parsed.default_, url, data) ?? "" };
  if (parsed.optional) return { kind: "elide" };
  throw new PatternError(`'${interior}' resolved to nothing (mark it optional with \`?\` or give a \`|| default\`)`);
}

function evalExpr(expr: Expr, url: UrlView, data: JsonObject): string | undefined {
  switch (expr.kind) {
    case "literal":
      return expr.text;
    case "data":
      return evalData(expr.path, data);
    case "url":
      return evalUrl(expr.sel, url);
  }
}

function evalUrl(sel: UrlSel, url: UrlView): string | undefined {
  switch (sel.kind) {
    case "name":
      return url.name !== undefined && url.name !== "" ? url.name : undefined;
    case "rest":
      return url.rest !== "" ? url.rest : undefined;
    case "queryAll":
      return url.query !== "" ? url.query : undefined;
    case "queryKey":
      return queryGet(url.query, sel.key);
    case "segments": {
      const segs =
        sel.section === "path" ? [...url.path] : sel.section === "base" ? [...url.base] : [...url.base, ...url.path];
      if (sel.index === undefined) return joinNonempty(segs);
      if (sel.index.kind === "at") {
        const n = normIndex(sel.index.i, segs.length);
        return n === undefined ? undefined : segs[n];
      }
      const len = segs.length;
      const start = normBound(sel.index.a ?? 0, len);
      const end = normBound(sel.index.b ?? len, len);
      if (start >= end) return undefined;
      return joinNonempty(segs.slice(start, end));
    }
  }
}

function joinNonempty(segs: string[]): string | undefined {
  return segs.length === 0 ? undefined : segs.join("/");
}

function normIndex(i: number, len: number): number | undefined {
  const idx = i < 0 ? len + i : i;
  return idx >= 0 && idx < len ? idx : undefined;
}

function normBound(x: number, len: number): number {
  const v = x < 0 ? len + x : x;
  return Math.min(Math.max(v, 0), len);
}

/// Data-plane dot-path walk. `undefined` if any segment is missing or the
/// value is null; a string is returned raw, other JSON via `JSON.stringify`.
function evalData(path: string, data: JsonObject): string | undefined {
  const parts = path.split(".");
  const head = parts[0] ?? "";
  if (!Object.prototype.hasOwnProperty.call(data, head)) return undefined;
  let value: Json = data[head]!;
  for (const key of parts.slice(1)) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return undefined;
    if (!Object.prototype.hasOwnProperty.call(value, key)) return undefined;
    value = value[key]!;
  }
  if (value === null) return undefined;
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

/// First value of `key` in a raw query string, percent-decoded (`+` → space).
function queryGet(query: string, key: string): string | undefined {
  for (const pair of query.split("&")) {
    if (pair === "") continue;
    const eq = pair.indexOf("=");
    const k = eq < 0 ? pair : pair.slice(0, eq);
    const v = eq < 0 ? "" : pair.slice(eq + 1);
    if (k === key) {
      let decoded: string;
      try {
        decoded = decodeURIComponent(v);
      } catch {
        decoded = v;
      }
      return decoded.replace(/\+/g, " ");
    }
  }
  return undefined;
}
