//! Stored-query envelopes and parameter substitution (PRD §10.4).
//!
//! A stored query is a JSON envelope, agnostic to the backing query
//! language:
//!
//! ```json
//! { "language": "json",
//!   "query": { "dataset": "orders", "where": { "status": "${status}" } },
//!   "params": { "type": "object", "required": ["status"],
//!               "properties": { "status": { "type": "string" },
//!                               "min": { "type": "number", "default": 0 } } },
//!   "output": { "type": "array" } }
//! ```
//!
//! Substitution is **structural** for JSON-language templates: a string node
//! that is exactly `"${name}"` is replaced by the parameter's JSON value
//! (numbers stay numbers, never spliced into text — injection-safe by
//! construction), so templates are valid JSON at rest. `"${name?}"` marks an
//! optional placeholder: when the parameter is absent the nearest enclosing
//! object member or array element is elided (the language-agnostic
//! generalization of v1 Mongo's `ignoreEmptyVariables`). Placeholders
//! embedded in longer strings splice through the adapter's `quote`.
//!
//! String-language templates (SQL) are **not** substituted by the service:
//! they pass to the adapter with the validated params intact, so adapters
//! bind (prepared statements) instead of splicing.

use serde_json::{Map, Value};

use crate::error::RsError;

/// A parsed stored-query envelope.
pub struct QueryEnvelope {
    pub language: Language,
    pub query: Value,
    pub params_schema: Option<Value>,
    pub output_schema: Option<Value>,
    /// Compiled once at parse — `prepare_params` runs per request, and
    /// schema compilation dwarfs validation.
    params_validator: Option<jsonschema::Validator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    /// JSON-shaped template (Mongo aggregates, Elastic DSL, the reference
    /// adapter): structurally substituted by the service.
    Json,
    /// String template (SQL family): passed through with params for the
    /// adapter to bind.
    Text,
}

impl QueryEnvelope {
    /// Parse and validate an envelope (the `PUT`-time check): the shape,
    /// schema compilation, and — for JSON templates — a placeholder scan.
    pub fn parse(doc: &Value) -> Result<QueryEnvelope, RsError> {
        let obj = doc
            .as_object()
            .ok_or_else(|| RsError::bad_request("a stored query is a JSON object envelope"))?;
        let query = obj
            .get("query")
            .cloned()
            .ok_or_else(|| RsError::bad_request("stored query envelope requires a 'query'"))?;
        let language = match obj.get("language").and_then(|l| l.as_str()) {
            Some("json") => Language::Json,
            Some("sql") | Some("text") => Language::Text,
            Some(other) => {
                return Err(RsError::bad_request(format!(
                    "unknown query language '{other}' (json | sql)"
                )))
            }
            None => {
                if query.is_string() {
                    Language::Text
                } else {
                    Language::Json
                }
            }
        };
        if language == Language::Text && !query.is_string() {
            return Err(RsError::bad_request("a sql/text query must be a string template"));
        }
        let params_validator = match obj.get("params") {
            Some(schema) => Some(jsonschema::validator_for(schema).map_err(|e| {
                RsError::bad_request(format!("'params' is not a valid JSON Schema: {e}"))
            })?),
            None => None,
        };
        if let Some(schema) = obj.get("output") {
            jsonschema::validator_for(schema).map_err(|e| {
                RsError::bad_request(format!("'output' is not a valid JSON Schema: {e}"))
            })?;
        }
        // Placeholder scan: malformed placeholders fail at write time.
        if language == Language::Json {
            scan_placeholders(&query)?;
        }
        Ok(QueryEnvelope {
            language,
            query,
            params_schema: obj.get("params").cloned(),
            output_schema: obj.get("output").cloned(),
            params_validator,
        })
    }

    /// Apply schema `default`s for missing top-level params, then validate.
    pub fn prepare_params(&self, mut params: Map<String, Value>) -> Result<Map<String, Value>, RsError> {
        if let Some(schema) = &self.params_schema {
            if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
                for (name, prop) in props {
                    if !params.contains_key(name) {
                        if let Some(default) = prop.get("default") {
                            params.insert(name.clone(), default.clone());
                        }
                    }
                }
            }
            let validator = self
                .params_validator
                .as_ref()
                .ok_or_else(|| RsError::internal("params schema was not compiled at parse"))?;
            let value = Value::Object(params.clone());
            let errors: Vec<Value> = validator
                .iter_errors(&value)
                .map(|e| {
                    serde_json::json!({ "path": e.instance_path.to_string(), "error": e.to_string() })
                })
                .collect();
            if !errors.is_empty() {
                return Err(RsError::validation_failed(
                    "query parameters failed validation",
                    Value::Array(errors),
                ));
            }
        }
        Ok(params)
    }
}

/// Parse query-string pairs into params, coerced to the params schema's
/// declared property types (`number`/`integer`/`boolean` parse; everything
/// else stays a string; unparseable values stay strings so schema
/// validation reports them). `$`-prefixed keys (`$take`, `$skip`, …) are
/// runtime controls, not params.
pub fn url_params(query: &str, schema: Option<&Value>) -> Map<String, Value> {
    let props = schema.and_then(|s| s.get("properties")).and_then(|p| p.as_object());
    let mut out = Map::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k.starts_with('$') {
            continue;
        }
        let key = percent_encoding::percent_decode_str(k)
            .decode_utf8_lossy()
            .replace('+', " ");
        let raw = percent_encoding::percent_decode_str(v)
            .decode_utf8_lossy()
            .replace('+', " ");
        let declared = props
            .and_then(|p| p.get(&key))
            .and_then(|prop| prop.get("type"))
            .and_then(|t| t.as_str());
        let value = match declared {
            Some("number") => raw.parse::<f64>().map(Value::from).unwrap_or(Value::String(raw)),
            Some("integer") => raw.parse::<i64>().map(Value::from).unwrap_or(Value::String(raw)),
            Some("boolean") => match raw.as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => Value::String(raw),
            },
            _ => Value::String(raw),
        };
        out.insert(key, value);
    }
    out
}

/// One placeholder: `${name}` / `${name?}` / `$0`.
struct Placeholder {
    name: String,
    optional: bool,
}

fn parse_exact(s: &str) -> Option<Placeholder> {
    let inner = s.strip_prefix("${")?.strip_suffix('}')?;
    if inner.is_empty() || inner.contains(['{', '}', '$']) {
        return None;
    }
    let (name, optional) = match inner.strip_suffix('?') {
        Some(name) => (name, true),
        None => (inner, false),
    };
    Some(Placeholder { name: name.to_string(), optional })
}

/// Scan a JSON template for malformed placeholders (write-time check).
fn scan_placeholders(template: &Value) -> Result<(), RsError> {
    match template {
        Value::String(s) => {
            if s.contains("${") && parse_exact(s).is_none() {
                // Embedded placeholders: each must close.
                let mut rest = s.as_str();
                while let Some(start) = rest.find("${") {
                    let after = &rest[start + 2..];
                    match after.find('}') {
                        Some(end) => rest = &after[end + 1..],
                        None => {
                            return Err(RsError::bad_request(format!(
                                "unterminated placeholder in '{s}'"
                            )))
                        }
                    }
                }
            }
            Ok(())
        }
        Value::Object(o) => o.values().try_for_each(scan_placeholders),
        Value::Array(a) => a.iter().try_for_each(scan_placeholders),
        _ => Ok(()),
    }
}

/// Outcome of substituting one node.
enum Subst {
    /// Replace the node with this value.
    Value(Value),
    /// Optional placeholder with no param: elide the enclosing member.
    Elide,
}

/// Structurally substitute a JSON template (see module docs). `quote`
/// splices values into embedded string positions.
pub fn substitute_json(
    template: &Value,
    params: &Map<String, Value>,
    quote: &dyn Fn(&Value) -> Result<String, RsError>,
) -> Result<Value, RsError> {
    match subst_node(template, params, quote)? {
        Subst::Value(v) => Ok(v),
        Subst::Elide => Err(RsError::bad_request(
            "the whole query is an optional placeholder with no parameter",
        )),
    }
}

fn subst_node(
    node: &Value,
    params: &Map<String, Value>,
    quote: &dyn Fn(&Value) -> Result<String, RsError>,
) -> Result<Subst, RsError> {
    match node {
        Value::String(s) => {
            if let Some(ph) = parse_exact(s) {
                return match params.get(&ph.name) {
                    Some(v) => Ok(Subst::Value(v.clone())),
                    None if ph.optional => Ok(Subst::Elide),
                    None => Err(RsError::bad_request(format!(
                        "missing query parameter '{}'",
                        ph.name
                    ))),
                };
            }
            if s.contains("${") || s.contains('$') {
                return Ok(Subst::Value(Value::String(splice(s, params, quote)?)));
            }
            Ok(Subst::Value(node.clone()))
        }
        Value::Object(o) => {
            let mut out = Map::with_capacity(o.len());
            for (k, v) in o {
                match subst_node(v, params, quote)? {
                    Subst::Value(v) => {
                        out.insert(k.clone(), v);
                    }
                    Subst::Elide => {} // drop the member
                }
            }
            Ok(Subst::Value(Value::Object(out)))
        }
        Value::Array(a) => {
            let mut out = Vec::with_capacity(a.len());
            for v in a {
                match subst_node(v, params, quote)? {
                    Subst::Value(v) => out.push(v),
                    Subst::Elide => {} // drop the element
                }
            }
            Ok(Subst::Value(Value::Array(out)))
        }
        other => Ok(Subst::Value(other.clone())),
    }
}

/// Splice `${name}` / `$0` placeholders embedded in a longer string,
/// adapter-quoted. Missing params are errors — never silent ''.
fn splice(
    s: &str,
    params: &Map<String, Value>,
    quote: &dyn Fn(&Value) -> Result<String, RsError>,
) -> Result<String, RsError> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find('$') {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        if let Some(body) = after.strip_prefix("${") {
            let end = body.find('}').ok_or_else(|| {
                RsError::bad_request(format!("unterminated placeholder in '{s}'"))
            })?;
            let raw = &body[..end];
            let (name, optional) =
                match raw.strip_suffix('?') {
                    Some(n) => (n, true),
                    None => (raw, false),
                };
            match params.get(name) {
                Some(v) => out.push_str(&quote(v)?),
                None if optional => {}
                None => {
                    return Err(RsError::bad_request(format!("missing query parameter '{name}'")))
                }
            }
            rest = &body[end + 1..];
        } else {
            let digits: String =
                after[1..].chars().take_while(|c| c.is_ascii_digit()).collect();
            if digits.is_empty() {
                out.push('$');
                rest = &after[1..];
            } else {
                match params.get(&digits) {
                    Some(v) => out.push_str(&quote(v)?),
                    None => {
                        return Err(RsError::bad_request(format!(
                            "missing positional query parameter '${digits}'"
                        )))
                    }
                }
                rest = &after[1 + digits.len()..];
            }
        }
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn q(v: &Value) -> Result<String, RsError> {
        match v {
            Value::String(s) => Ok(s.clone()),
            other => Ok(other.to_string()),
        }
    }

    fn params(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn envelope_parses_and_infers_language() {
        let env = QueryEnvelope::parse(&json!({ "query": { "dataset": "x" } })).unwrap();
        assert_eq!(env.language, Language::Json);
        let env = QueryEnvelope::parse(&json!({ "query": "SELECT 1" })).unwrap();
        assert_eq!(env.language, Language::Text);
        assert!(QueryEnvelope::parse(&json!({ "noquery": 1 })).is_err());
        assert!(QueryEnvelope::parse(&json!({ "query": {}, "params": { "type": 42 } })).is_err());
        assert!(QueryEnvelope::parse(&json!({ "language": "sql", "query": {} })).is_err());
        assert!(
            QueryEnvelope::parse(&json!({ "query": { "w": "${unterminated" } })).is_err(),
            "write-time placeholder scan"
        );
    }

    #[test]
    fn structural_substitution_keeps_types() {
        let template = json!({ "where": { "status": "${status}", "min": "${min}" } });
        let out = substitute_json(&template, &params(json!({ "status": "open", "min": 5 })), &q)
            .unwrap();
        assert_eq!(out, json!({ "where": { "status": "open", "min": 5 } }));
    }

    #[test]
    fn optional_placeholders_elide_members_and_elements() {
        // A placeholder that is a member's value elides the member; the
        // containing object remains (an empty $match matches everything).
        let template = json!({
            "where": { "status": "${status?}", "always": 1 },
            "pipeline": [ { "$match": "${extra?}" }, { "$limit": 10 } ]
        });
        let out = substitute_json(&template, &params(json!({})), &q).unwrap();
        assert_eq!(
            out,
            json!({ "where": { "always": 1 }, "pipeline": [ {}, { "$limit": 10 } ] })
        );
        // A placeholder that is itself an array element elides the element.
        let template = json!({ "clauses": [ "${a?}", "${b}" ] });
        let out = substitute_json(&template, &params(json!({ "b": 2 })), &q).unwrap();
        assert_eq!(out, json!({ "clauses": [2] }));
        // Present optional params substitute normally.
        let template = json!({ "where": { "status": "${status?}" } });
        let out = substitute_json(&template, &params(json!({ "status": "open" })), &q).unwrap();
        assert_eq!(out, json!({ "where": { "status": "open" } }));
    }

    #[test]
    fn missing_required_is_an_error_never_silent() {
        let template = json!({ "where": { "status": "${status}" } });
        let err = substitute_json(&template, &params(json!({})), &q).unwrap_err();
        assert_eq!(err.status, 400);
        let embedded = json!("prefix-${name}-suffix");
        assert!(substitute_json(&embedded, &params(json!({})), &q).is_err());
        let positional = json!("x-$0");
        assert!(substitute_json(&positional, &params(json!({})), &q).is_err());
    }

    #[test]
    fn embedded_and_positional_splice_through_quote() {
        let template = json!({ "q": "name:${who} AND idx-$0", "plain": "$ 5 cost" });
        let out =
            substitute_json(&template, &params(json!({ "who": "ada", "0": "seven" })), &q).unwrap();
        assert_eq!(out["q"], "name:ada AND idx-seven");
        assert_eq!(out["plain"], "$ 5 cost", "a bare $ is not a placeholder");
    }

    #[test]
    fn url_params_coerce_to_schema_types() {
        let schema = json!({ "properties": {
            "min": { "type": "number" }, "n": { "type": "integer" },
            "active": { "type": "boolean" }, "name": { "type": "string" } } });
        let out = url_params("min=2.5&n=7&active=true&name=a+b%21&$take=5&loose=x", Some(&schema));
        assert_eq!(out["min"], json!(2.5));
        assert_eq!(out["n"], json!(7));
        assert_eq!(out["active"], json!(true));
        assert_eq!(out["name"], "a b!");
        assert_eq!(out["loose"], "x", "undeclared params stay strings");
        assert!(!out.contains_key("$take"), "$-controls are not params");
        // Unparseable values stay strings for the validator to report.
        let out = url_params("min=notanumber", Some(&schema));
        assert_eq!(out["min"], "notanumber");
    }

    #[test]
    fn defaults_apply_then_validate() {
        let env = QueryEnvelope::parse(&json!({
            "query": {},
            "params": { "type": "object", "required": ["status", "min"],
                        "properties": { "status": { "type": "string" },
                                        "min": { "type": "number", "default": 0 } } }
        }))
        .unwrap();
        let prepared = env.prepare_params(params(json!({ "status": "open" }))).unwrap();
        assert_eq!(prepared.get("min"), Some(&json!(0)), "default applied");
        let err = env.prepare_params(params(json!({ "min": 3 }))).unwrap_err();
        assert_eq!(err.status, 422);
    }
}
