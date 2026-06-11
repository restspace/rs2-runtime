//! Pipeline transforms (PRD §8.2): JSONata expressions over the JSON body
//! with message context variables (`$_status`, `$_ok`, `$_headers`, named
//! variables).
//!
//! Engine: the `jsonata-rs` crate (the PRD's "evaluate Rust JSONata early
//! in M2" risk item). It is evaluated timeboxed; expressions are strings in
//! a template — an object template transforms per-key, a bare string is a
//! whole-body expression (Restspace semantics retained).

use serde_json::Value;

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
}
