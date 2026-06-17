//! Pipeline transforms (PRD §8.2): JSONata expressions over the JSON body
//! with message context variables (`$_status`, `$_ok`, `$_headers`, `$_rawBody`,
//! named variables) and host crypto functions (`$hmac`, `$hmacVerify`).
//!
//! Engine: the `jsonata-rs` crate (the PRD's "evaluate Rust JSONata early
//! in M2" risk item). It is evaluated timeboxed; expressions are strings in
//! a template — an object template transforms per-key, a bare string is a
//! whole-body expression (Restspace semantics retained).

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Sha256, Sha512};

use crate::error::RsError;

/// Cap on evaluation depth and wall time per expression.
const MAX_DEPTH: usize = 100;
const TIME_LIMIT_MS: usize = 1000;

/// Evaluate one JSONata expression against `input` with variable bindings.
pub fn evaluate(
    expr: &str,
    input: &Value,
    vars: &serde_json::Map<String, Value>,
) -> Result<Value, RsError> {
    let arena = bumpalo::Bump::new();
    let jsonata = jsonata_rs::JsonAta::new(expr, &arena)
        .map_err(|e| RsError::bad_request(format!("invalid JSONata expression '{expr}': {e}")))?;
    for (name, value) in vars {
        let bound = json_to_value(&arena, value);
        // Bindings are referenced as `$name`; the frame stores them unprefixed.
        jsonata.assign_var(name.trim_start_matches('$'), bound);
    }
    // Host crypto primitives (G3): verify webhook HMAC signatures inline —
    // `$hmacVerify('sha256', $secret, $_rawBody, $sig)` gates a pipeline, and
    // `$hmac(...)` returns the hex MAC. The secret arrives as a host-bound
    // variable; the raw request bytes as `$_rawBody`.
    jsonata.register_function("hmac", 3, jsonata_hmac);
    jsonata.register_function("hmacVerify", 4, jsonata_hmac_verify);
    let input_text = input.to_string();
    let result = jsonata
        .evaluate_timeboxed(Some(&input_text), Some(MAX_DEPTH), Some(TIME_LIMIT_MS))
        .map_err(|e| {
            RsError::bad_request(format!("JSONata evaluation failed for '{expr}': {e}"))
        })?;
    if result.is_undefined() {
        return Ok(Value::Null);
    }
    serde_json::from_str(&result.serialize(false))
        .map_err(|e| RsError::internal(format!("JSONata produced unserializable output: {e}")))
}

fn json_to_value<'a>(
    arena: &'a bumpalo::Bump,
    json: &Value,
) -> &'a jsonata_rs::Value<'a> {
    match json {
        Value::Null => jsonata_rs::Value::null(arena),
        Value::Bool(b) => arena.alloc(jsonata_rs::Value::Bool(*b)),
        Value::Number(n) => jsonata_rs::Value::number(arena, n.as_f64().unwrap_or(0.0)),
        Value::String(s) => jsonata_rs::Value::string(arena, s),
        Value::Array(items) => {
            let array = jsonata_rs::Value::array_with_capacity(
                arena,
                items.len(),
                jsonata_rs::ArrayFlags::empty(),
            );
            for item in items {
                array.push(json_to_value(arena, item));
            }
            array
        }
        Value::Object(map) => {
            let object = jsonata_rs::Value::object_with_capacity(arena, map.len());
            for (k, v) in map {
                object.insert(k, json_to_value(arena, v));
            }
            object
        }
    }
}

/// Apply a transform template: object → evaluate each value recursively
/// (string leaves are JSONata expressions); bare string → whole-body
/// expression; other scalars pass through unchanged.
pub fn apply(
    template: &Value,
    input: &Value,
    vars: &serde_json::Map<String, Value>,
) -> Result<Value, RsError> {
    match template {
        Value::String(expr) => evaluate(expr, input, vars),
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k.clone(), apply(v, input, vars)?);
            }
            Ok(Value::Object(out))
        }
        Value::Array(items) => {
            Ok(Value::Array(items.iter().map(|v| apply(v, input, vars)).collect::<Result<_, _>>()?))
        }
        other => Ok(other.clone()),
    }
}

// ---- host crypto primitives (G3) ----------------------------------------

type J<'a> = jsonata_rs::Value<'a>;

/// String value of arg `i`, or empty if missing/non-string.
fn arg_str<'a>(args: &[&'a J<'a>], i: usize) -> String {
    match args.get(i) {
        Some(v) if v.is_string() => v.as_str().to_string(),
        _ => String::new(),
    }
}

/// `$hmac(algorithm, key, message)` → lowercase hex MAC (`""` on bad input).
fn jsonata_hmac<'a>(
    ctx: jsonata_rs::FunctionContext<'a, '_>,
    args: &[&'a J<'a>],
) -> jsonata_rs::Result<&'a J<'a>> {
    let hex = hmac_bytes(&arg_str(args, 0), arg_str(args, 1).as_bytes(), arg_str(args, 2).as_bytes())
        .map(|b| to_hex(&b))
        .unwrap_or_default();
    Ok(jsonata_rs::Value::string(ctx.arena, &hex))
}

/// `$hmacVerify(algorithm, key, message, signatureHex)` → bool, constant-time.
/// Any malformed input (unknown algorithm, non-hex signature) → `false`.
fn jsonata_hmac_verify<'a>(
    _ctx: jsonata_rs::FunctionContext<'a, '_>,
    args: &[&'a J<'a>],
) -> jsonata_rs::Result<&'a J<'a>> {
    let ok = hmac_verify(
        &arg_str(args, 0),
        arg_str(args, 1).as_bytes(),
        arg_str(args, 2).as_bytes(),
        &arg_str(args, 3),
    );
    Ok(jsonata_rs::Value::bool(ok))
}

/// HMAC of `message` under `key` for `sha256`/`sha512`; `None` on unknown algo.
fn hmac_bytes(algorithm: &str, key: &[u8], message: &[u8]) -> Option<Vec<u8>> {
    match algorithm {
        "sha256" => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
            mac.update(message);
            Some(mac.finalize().into_bytes().to_vec())
        }
        "sha512" => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key).ok()?;
            mac.update(message);
            Some(mac.finalize().into_bytes().to_vec())
        }
        _ => None,
    }
}

/// Constant-time HMAC verification against a hex signature.
fn hmac_verify(algorithm: &str, key: &[u8], message: &[u8], signature_hex: &str) -> bool {
    let Some(provided) = from_hex(signature_hex) else { return false };
    match algorithm {
        "sha256" => match Hmac::<Sha256>::new_from_slice(key) {
            Ok(mut mac) => {
                mac.update(message);
                mac.verify_slice(&provided).is_ok()
            }
            Err(_) => false,
        },
        "sha512" => match Hmac::<Sha512>::new_from_slice(key) {
            Ok(mut mac) => {
                mac.update(message);
                mac.verify_slice(&provided).is_ok()
            }
            Err(_) => false,
        },
        _ => false,
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.is_empty() || s.len() % 2 != 0 {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn evaluates_jsonata_over_body_and_variables() {
        let input = json!({ "lines": [ { "price": 2 }, { "price": 3 } ] });
        let vars = json!({
            "order": { "customerId": "c1" },
            "_status": 200,
            "_ok": true
        });
        let vars = vars.as_object().unwrap();
        let out = apply(
            &json!({
                "total": "$sum(lines.price)",
                "customer": "$order.customerId",
                "charged": "$_ok",
                "fixed": 42
            }),
            &input,
            vars,
        )
        .unwrap();
        assert_eq!(out, json!({ "total": 5, "customer": "c1", "charged": true, "fixed": 42 }));
    }

    #[test]
    fn bare_string_template_replaces_whole_body() {
        let input = json!([1, 2, 3]);
        let out = apply(&json!("$sum($)"), &input, &serde_json::Map::new()).unwrap();
        assert_eq!(out, json!(6));
    }

    #[test]
    fn invalid_expression_is_a_structured_error() {
        let err = apply(&json!("$$$nonsense((("), &json!({}), &serde_json::Map::new()).unwrap_err();
        assert_eq!(err.status, 400);
    }

    // RFC 4231 / well-known HMAC-SHA256 test vector.
    const KEY: &str = "key";
    const MSG: &str = "The quick brown fox jumps over the lazy dog";
    const MAC: &str = "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8";

    #[test]
    fn hmac_function_matches_known_vector() {
        let out = apply(
            &json!(format!("$hmac('sha256', '{KEY}', '{MSG}')")),
            &json!(null),
            &serde_json::Map::new(),
        )
        .unwrap();
        assert_eq!(out, json!(MAC));
    }

    #[test]
    fn hmac_verify_accepts_valid_and_rejects_invalid() {
        let v = |sig: &str| {
            apply(
                &json!(format!("$hmacVerify('sha256', '{KEY}', '{MSG}', '{sig}')")),
                &json!(null),
                &serde_json::Map::new(),
            )
            .unwrap()
        };
        assert_eq!(v(MAC), json!(true), "valid signature");
        assert_eq!(v(&MAC.replace('f', "0")), json!(false), "tampered signature");
        assert_eq!(v("not-hex!!"), json!(false), "malformed signature");
        assert_eq!(v(""), json!(false), "empty signature");
        // Unknown algorithm never verifies.
        let bad_algo =
            apply(&json!(format!("$hmacVerify('md5', '{KEY}', '{MSG}', '{MAC}')")), &json!(null), &serde_json::Map::new()).unwrap();
        assert_eq!(bad_algo, json!(false));
    }

    #[test]
    fn gate_pattern_passes_or_errors() {
        // The G3 gate: valid → pass the body through; invalid → 400.
        let expr = format!("$hmacVerify('sha256', '{KEY}', '{MSG}', '{MAC}') ? $ : $error('bad sig')");
        let ok = apply(&json!(expr), &json!({ "ok": 1 }), &serde_json::Map::new()).unwrap();
        assert_eq!(ok, json!({ "ok": 1 }));

        let bad_expr = format!("$hmacVerify('sha256', 'wrong', '{MSG}', '{MAC}') ? $ : $error('bad sig')");
        let err = apply(&json!(bad_expr), &json!({ "ok": 1 }), &serde_json::Map::new()).unwrap_err();
        assert_eq!(err.status, 400, "a failed gate is a 400");
    }
}
