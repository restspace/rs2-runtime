// Pipeline transforms (PRD §8.2): JSONata expressions over the JSON body
// with message context variables (`$_status`, `$_ok`, `$_headers`,
// `$_rawBody`, named variables) and host crypto functions (`$hmac`,
// `$hmacVerify`, `$hashPassword`, `$verifyPassword`). Port of
// `rs2-core/src/pipeline/transform.rs` over the jsonata reference
// implementation (cloudflare.md §D): the host functions are async-capable
// (argon2 and WebCrypto return Promises), which jsonata awaits.

import jsonata from "jsonata";

import { hmacBytes, hmacVerify, toHex } from "../runtime/crypto";
import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { hashPassword, verifyPassword } from "../services/auth";

/// Cap on evaluation depth and wall time per expression. Generous enough
/// for `$hashPassword` (argon2id, deliberately slow); the pipeline wall
/// clock remains the outer bound.
const MAX_DEPTH = 100;
const TIME_LIMIT_MS = 5000;

function errorText(e: unknown): string {
  if (e instanceof Error) return e.message;
  if (e && typeof e === "object") {
    const o = e as { message?: unknown; code?: unknown };
    if (typeof o.message === "string") return o.message;
    if (typeof o.code === "string") return o.code;
  }
  return String(e);
}

function compile(expr: string): jsonata.Expression {
  try {
    return jsonata(expr, { timeout: TIME_LIMIT_MS, stack: MAX_DEPTH });
  } catch (e) {
    throw RsError.badRequest(`invalid JSONata expression '${expr}': ${errorText(e)}`);
  }
}

function argStr(v: unknown): string {
  return typeof v === "string" ? v : "";
}

/// Evaluate one JSONata expression against `input` with variable bindings.
export async function evaluate(expr: string, input: Json, vars: JsonObject): Promise<Json> {
  const compiled = compile(expr);
  // `$hmac(algorithm, key, message)` → lowercase hex MAC (`""` on bad input).
  compiled.registerFunction("hmac", async (algorithm: unknown, key: unknown, message: unknown) => {
    const mac = await hmacBytes(argStr(algorithm), argStr(key), argStr(message));
    return mac ? toHex(mac) : "";
  });
  // `$hmacVerify(algorithm, key, message, signatureHex)` → bool, constant-time.
  compiled.registerFunction("hmacVerify", async (algorithm: unknown, key: unknown, message: unknown, sig: unknown) =>
    hmacVerify(argStr(algorithm), argStr(key), argStr(message), argStr(sig)),
  );
  // `$hashPassword(password)` → argon2id PHC string (`""` on failure).
  compiled.registerFunction("hashPassword", async (password: unknown) => {
    try {
      return await hashPassword(argStr(password));
    } catch {
      return "";
    }
  });
  // `$verifyPassword(password, hash)` → bool.
  compiled.registerFunction("verifyPassword", async (password: unknown, hash: unknown) =>
    verifyPassword(argStr(password), argStr(hash)),
  );
  // Bindings are referenced as `$name`; the frame stores them unprefixed.
  const bindings: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(vars)) bindings[name.replace(/^\$+/, "")] = value;
  let result: unknown;
  try {
    result = await compiled.evaluate(input, bindings);
  } catch (e) {
    throw RsError.badRequest(`JSONata evaluation failed for '${expr}': ${errorText(e)}`);
  }
  if (result === undefined) return null;
  // jsonata results carry sequence markers and may contain `undefined`
  // holes; a JSON round trip yields plain data.
  try {
    const text = JSON.stringify(result);
    return text === undefined ? null : (JSON.parse(text) as Json);
  } catch (e) {
    throw RsError.internal(`JSONata produced unserializable output: ${errorText(e)}`);
  }
}

/// Parse-only check of a single expression (spec validation at write time).
export function validateExpr(expr: string): void {
  compile(expr);
}

/// Apply a transform template: object → evaluate each value recursively
/// (string leaves are JSONata expressions); bare string → whole-body
/// expression; other scalars pass through unchanged.
export async function apply(template: Json, input: Json, vars: JsonObject): Promise<Json> {
  if (typeof template === "string") return evaluate(template, input, vars);
  if (Array.isArray(template)) {
    const out: Json[] = [];
    for (const v of template) out.push(await apply(v, input, vars));
    return out;
  }
  if (template && typeof template === "object") {
    const out: JsonObject = {};
    for (const [k, v] of Object.entries(template)) out[k] = await apply(v, input, vars);
    return out;
  }
  return template;
}

/// Whether any string leaf of a template mentions `name` (substring check).
export function mentions(template: Json, name: string): boolean {
  if (typeof template === "string") return template.includes(name);
  if (Array.isArray(template)) return template.some((v) => mentions(v, name));
  if (template && typeof template === "object") return Object.values(template).some((v) => mentions(v, name));
  return false;
}
