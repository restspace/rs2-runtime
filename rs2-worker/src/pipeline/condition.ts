// Pipeline condition language (PRD §8.2): a small expression grammar over
// status / mime / method / headers / variables, parsed and checked at
// config time. Port of `rs2-core/src/pipeline/condition.rs`; grammar and
// truthiness rules verbatim.

import type { Json, JsonObject } from "../runtime/error";
import type { Message } from "../runtime/message";

export type CmpOp = "eq" | "ne" | "lt" | "gt" | "le" | "ge";

export type Expr =
  | { kind: "literal"; value: Json }
  | { kind: "builtin"; name: string }
  | { kind: "var"; name: string; path: string[] }
  | { kind: "header"; name: string }
  | { kind: "not"; inner: Expr }
  | { kind: "and"; left: Expr; right: Expr }
  | { kind: "or"; left: Expr; right: Expr }
  | { kind: "cmp"; left: Expr; op: CmpOp; right: Expr };

const BUILTINS = ["status", "ok", "method", "mime", "isJson", "isText", "isBinary", "name", "isDirectory"];

export interface Condition {
  expr: Expr;
  source: string;
}

/// Parse a condition; throws an `Error` whose message is the grammar
/// diagnostic (callers wrap it).
export function parseCondition(source: string): Condition {
  const p = new Parser(source);
  const expr = p.expr();
  p.skipWs();
  if (p.pos !== p.input.length) throw new Error(`unexpected input at position ${p.pos}`);
  return { expr, source };
}

/// Evaluate against a message and the pipeline's variables.
export function evaluateCondition(cond: Condition, msg: Message, vars: JsonObject): boolean {
  return truthy(evalExpr(cond.expr, msg, vars));
}

function truthy(v: Json): boolean {
  if (v === null) return false;
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "string") return v !== "";
  if (Array.isArray(v)) return v.length > 0;
  return true;
}

function evalExpr(expr: Expr, msg: Message, vars: JsonObject): Json {
  switch (expr.kind) {
    case "literal":
      return expr.value;
    case "builtin": {
      const status = msg.status ?? 200;
      const body = msg.body;
      switch (expr.name) {
        case "status":
          return status;
        case "ok":
          return msg.isOk();
        case "method":
          return msg.method;
        case "mime":
          return body ? body.mediaType.essence() : null;
        case "isJson":
          return body !== undefined && body.mediaType.isJson();
        case "isText":
          return body !== undefined && body.mediaType.isText();
        case "isBinary":
          return body !== undefined && !body.mediaType.isJson() && !body.mediaType.isText();
        case "name":
          return msg.name ?? null;
        case "isDirectory":
          return msg.url.isDirectory();
        default:
          throw new Error(`unknown builtin '${expr.name}'`);
      }
    }
    case "var": {
      let v: Json = vars[expr.name] ?? null;
      for (const key of expr.path) {
        if (v && typeof v === "object" && !Array.isArray(v)) v = v[key] ?? null;
        else v = null;
      }
      return v;
    }
    case "header":
      return msg.header(expr.name) ?? null;
    case "not":
      return !truthy(evalExpr(expr.inner, msg, vars));
    case "and":
      return truthy(evalExpr(expr.left, msg, vars)) && truthy(evalExpr(expr.right, msg, vars));
    case "or":
      return truthy(evalExpr(expr.left, msg, vars)) || truthy(evalExpr(expr.right, msg, vars));
    case "cmp":
      return compare(evalExpr(expr.left, msg, vars), expr.op, evalExpr(expr.right, msg, vars));
  }
}

/// serde `Value == Value`: deep equality, object key order insignificant.
function jsonEqual(a: Json, b: Json): boolean {
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((v, i) => jsonEqual(v, b[i]!));
  }
  if (a && b && typeof a === "object" && typeof b === "object" && !Array.isArray(a) && !Array.isArray(b)) {
    const keys = Object.keys(a);
    return keys.length === Object.keys(b).length && keys.every((k) => k in b && jsonEqual(a[k]!, b[k]!));
  }
  return false;
}

function compare(l: Json, op: CmpOp, r: Json): boolean {
  // Ordering exists for number/number and string/string only; `undefined`
  // otherwise (mixed types compare false, except for (in)equality).
  let ord: number | undefined;
  if (typeof l === "number" && typeof r === "number") ord = Number.isNaN(l) || Number.isNaN(r) ? undefined : l < r ? -1 : l > r ? 1 : 0;
  else if (typeof l === "string" && typeof r === "string") ord = compareUtf8(l, r);
  switch (op) {
    case "eq":
      return ord !== undefined ? ord === 0 : jsonEqual(l, r);
    case "ne":
      return ord !== undefined ? ord !== 0 : !jsonEqual(l, r);
    case "lt":
      return ord === -1;
    case "gt":
      return ord === 1;
    case "le":
      return ord === -1 || ord === 0;
    case "ge":
      return ord === 1 || ord === 0;
  }
}

const encoder = new TextEncoder();

/// Rust `str::cmp` is byte-wise over UTF-8.
function compareUtf8(a: string, b: string): number {
  const x = encoder.encode(a);
  const y = encoder.encode(b);
  const n = Math.min(x.length, y.length);
  for (let i = 0; i < n; i++) {
    if (x[i]! !== y[i]!) return x[i]! < y[i]! ? -1 : 1;
  }
  return x.length === y.length ? 0 : x.length < y.length ? -1 : 1;
}

class Parser {
  readonly input: string;
  pos = 0;

  constructor(input: string) {
    this.input = input;
  }

  skipWs(): void {
    while (this.pos < this.input.length && /[ \t\n\r\f\v]/.test(this.input[this.pos]!)) this.pos += 1;
  }

  private peek(): string | undefined {
    return this.input[this.pos];
  }

  private eat(token: string): boolean {
    this.skipWs();
    if (this.input.startsWith(token, this.pos)) {
      this.pos += token.length;
      return true;
    }
    return false;
  }

  expr(): Expr {
    let left = this.and();
    for (;;) {
      if (this.eat("||")) {
        const right = this.and();
        left = { kind: "or", left, right };
      } else return left;
    }
  }

  private and(): Expr {
    let left = this.unary();
    for (;;) {
      if (this.eat("&&")) {
        const right = this.unary();
        left = { kind: "and", left, right };
      } else return left;
    }
  }

  private unary(): Expr {
    this.skipWs();
    // `!` but not `!=`
    if (this.peek() === "!" && this.input[this.pos + 1] !== "=") {
      this.pos += 1;
      return { kind: "not", inner: this.unary() };
    }
    return this.cmp();
  }

  private cmp(): Expr {
    const left = this.primary();
    this.skipWs();
    let op: CmpOp;
    if (this.eat("==")) op = "eq";
    else if (this.eat("!=")) op = "ne";
    else if (this.eat("<=")) op = "le";
    else if (this.eat(">=")) op = "ge";
    else if (this.eat("<")) op = "lt";
    else if (this.eat(">")) op = "gt";
    else return left;
    const right = this.primary();
    return { kind: "cmp", left, op, right };
  }

  private primary(): Expr {
    this.skipWs();
    const c = this.peek();
    if (c === undefined) throw new Error("unexpected end of expression");
    if (c === "(") {
      this.pos += 1;
      const inner = this.expr();
      if (!this.eat(")")) throw new Error("expected ')'");
      return inner;
    }
    if (c === "'" || c === '"') {
      this.pos += 1;
      const start = this.pos;
      while (this.pos < this.input.length && this.input[this.pos] !== c) this.pos += 1;
      if (this.pos >= this.input.length) throw new Error("unterminated string");
      const s = this.input.slice(start, this.pos);
      this.pos += 1;
      return { kind: "literal", value: s };
    }
    if (c === "$") {
      this.pos += 1;
      const name = this.ident();
      const path: string[] = [];
      while (this.peek() === ".") {
        this.pos += 1;
        path.push(this.ident());
      }
      return { kind: "var", name, path };
    }
    if (/[0-9-]/.test(c)) {
      const start = this.pos;
      if (c === "-") this.pos += 1;
      while (this.pos < this.input.length && /[0-9.]/.test(this.input[this.pos]!)) this.pos += 1;
      const text = this.input.slice(start, this.pos);
      const n = Number(text);
      if (text === "" || text === "-" || !Number.isFinite(n)) throw new Error(`bad number '${text}'`);
      return { kind: "literal", value: n };
    }
    if (/[A-Za-z_]/.test(c)) {
      const name = this.ident();
      switch (name) {
        case "true":
          return { kind: "literal", value: true };
        case "false":
          return { kind: "literal", value: false };
        case "null":
          return { kind: "literal", value: null };
        case "header": {
          if (!this.eat("(")) throw new Error("header requires '(\"name\")'");
          const arg = this.primary();
          if (!this.eat(")")) throw new Error("expected ')' after header argument");
          if (arg.kind === "literal" && typeof arg.value === "string") return { kind: "header", name: arg.value };
          throw new Error("header argument must be a string literal");
        }
        default:
          if (BUILTINS.includes(name)) return { kind: "builtin", name };
          throw new Error(`unknown identifier '${name}' (builtins: ${BUILTINS.join(", ")})`);
      }
    }
    throw new Error(`unexpected character '${c}'`);
  }

  private ident(): string {
    const start = this.pos;
    while (this.pos < this.input.length && /[A-Za-z0-9_]/.test(this.input[this.pos]!)) this.pos += 1;
    if (this.pos === start) throw new Error("expected identifier");
    return this.input.slice(start, this.pos);
  }
}
