// JSON Schema validation over `@cfworker/json-schema` (cloudflare.md §D
// names ajv, but ajv generates validator code with `new Function`, which
// Workers forbid — "Code generation from strings disallowed"; see §I of the
// P2 result). Draft auto-detection follows `jsonschema` 0.33: `$schema`
// selects draft-04/07/2019-09, default 2020-12. Compiled validators are
// cached per schema text.

import { Validator } from "@cfworker/json-schema";
import type { SchemaDraft } from "@cfworker/json-schema";
import type { RsError } from "../runtime/error";
import type { Json } from "../runtime/error";

const cache = new Map<string, Validator>();
const CACHE_CAP = 512;

function draftOf(schema: Json): SchemaDraft {
  if (!schema || typeof schema !== "object" || Array.isArray(schema)) return "2020-12";
  const s = schema.$schema;
  if (typeof s !== "string") return "2020-12";
  if (s.includes("draft-04")) return "4";
  if (s.includes("draft-07") || s.includes("draft-06")) return "7";
  if (s.includes("2019-09")) return "2019-09";
  return "2020-12";
}

const KNOWN_TYPES = new Set(["null", "boolean", "object", "array", "number", "string", "integer"]);

/// The structural checks a schema compiler would refuse (the validator
/// library itself resolves lazily, so a malformed document must be caught
/// here to keep "not a valid JSON Schema" a 400 at write time).
function checkSchemaShape(schema: Json, path: string): string | undefined {
  if (typeof schema === "boolean") return undefined;
  if (!schema || typeof schema !== "object" || Array.isArray(schema)) return `${path || "/"}: a schema must be an object or boolean`;
  const t = schema.type;
  if (t !== undefined) {
    const types = Array.isArray(t) ? t : [t];
    for (const x of types) {
      if (typeof x !== "string" || !KNOWN_TYPES.has(x)) return `${path}/type: unknown type ${JSON.stringify(x)}`;
    }
  }
  if (schema.required !== undefined && (!Array.isArray(schema.required) || !schema.required.every((r) => typeof r === "string"))) {
    return `${path}/required: must be an array of strings`;
  }
  for (const key of ["properties", "patternProperties", "$defs", "definitions"]) {
    const sub = schema[key];
    if (sub === undefined) continue;
    if (!sub || typeof sub !== "object" || Array.isArray(sub)) return `${path}/${key}: must be an object`;
    for (const [k, v] of Object.entries(sub)) {
      const err = checkSchemaShape(v, `${path}/${key}/${k}`);
      if (err) return err;
    }
  }
  for (const key of ["items", "additionalProperties", "not", "if", "then", "else", "contains", "propertyNames"]) {
    const sub = schema[key];
    if (sub === undefined || Array.isArray(sub)) continue;
    const err = checkSchemaShape(sub, `${path}/${key}`);
    if (err) return err;
  }
  for (const key of ["allOf", "anyOf", "oneOf", "prefixItems"]) {
    const sub = schema[key];
    if (sub === undefined) continue;
    if (!Array.isArray(sub)) return `${path}/${key}: must be an array`;
    for (let i = 0; i < sub.length; i++) {
      const err = checkSchemaShape(sub[i]!, `${path}/${key}/${i}`);
      if (err) return err;
    }
  }
  return undefined;
}

/// Compile (or fetch from cache) a validator; `onError` shapes the failure.
export function compileSchema(schema: Json, onError: (detail: string) => RsError): Validator {
  const text = JSON.stringify(schema);
  const hit = cache.get(text);
  if (hit) return hit;
  const shapeError = checkSchemaShape(schema, "");
  if (shapeError) throw onError(shapeError);
  let v: Validator;
  try {
    v = new Validator(schema as ConstructorParameters<typeof Validator>[0], draftOf(schema), false);
    // Exercise the schema once so unresolvable `$ref`s surface at compile time.
    v.validate({});
  } catch (e) {
    throw onError(e instanceof Error ? e.message : String(e));
  }
  if (cache.size >= CACHE_CAP) cache.clear();
  cache.set(text, v);
  return v;
}

/// Run a validator; returns `[{path, message}]` (JSON-pointer `path`).
export function validateInstance(v: Validator, instance: Json): Json[] {
  let result;
  try {
    result = v.validate(instance);
  } catch (e) {
    return [{ path: "", message: e instanceof Error ? e.message : String(e) }];
  }
  if (result.valid) return [];
  return result.errors.map((e) => ({ path: e.instanceLocation.replace(/^#/, ""), message: e.error }));
}
