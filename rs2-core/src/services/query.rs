//! `query` — parameterized queries (PRD §10.4).
//!
//! Stored query templates are first-class config objects:
//!
//! ```json
//! { "queries": {
//!     "open-orders": {
//!       "query": { "dataset": "orders", "where": { "status": "${status}" } },
//!       "params": { "type": "object", "required": ["status"],
//!                   "properties": { "status": { "type": "string" } } },
//!       "output": { "type": "array" }
//!     }
//! } }
//! ```
//!
//! `POST /<name>` executes with the JSON body as named parameters
//! (positional `$0…` accepted as `{"0": ...}` or an array body). Parameters
//! are validated against the declared schema *before* execution; quoting
//! failures are structured 400s; results page with `X-Total-Count`.

use async_trait::async_trait;

use crate::error::RsError;
use crate::message::Message;

use super::{pagination, Service, ServiceContext};

pub struct QueryService;

impl QueryService {
    pub fn new() -> Self {
        QueryService
    }

    /// Validate all stored templates at config time.
    pub fn from_config(config: &serde_json::Value) -> Result<Self, RsError> {
        let queries = config
            .get("queries")
            .and_then(|q| q.as_object())
            .ok_or_else(|| RsError::bad_request("query mount requires a 'queries' object"))?;
        for (name, def) in queries {
            if def.get("query").is_none() {
                return Err(RsError::bad_request(format!(
                    "stored query '{name}' has no 'query' template"
                )));
            }
            if let Some(schema) = def.get("params") {
                jsonschema::validator_for(schema).map_err(|e| {
                    RsError::bad_request(format!("stored query '{name}' has an invalid params schema: {e}"))
                })?;
            }
        }
        Ok(QueryService)
    }
}

impl Default for QueryService {
    fn default() -> Self {
        Self::new()
    }
}

/// Substitute `${name}` / `$0…` placeholders through the template. A string
/// that *is* exactly one placeholder takes the parameter's JSON value;
/// placeholders embedded in longer strings splice via the adapter's `quote`.
fn substitute(
    template: &serde_json::Value,
    params: &serde_json::Map<String, serde_json::Value>,
    quote: &dyn Fn(&serde_json::Value) -> Result<String, RsError>,
) -> Result<serde_json::Value, RsError> {
    match template {
        serde_json::Value::String(s) => {
            if let Some(name) = exact_placeholder(s) {
                let value = lookup(params, name)?;
                return Ok(value.clone());
            }
            let mut out = String::with_capacity(s.len());
            let mut rest = s.as_str();
            while let Some(start) = rest.find('$') {
                out.push_str(&rest[..start]);
                let after = &rest[start..];
                if let Some((name, len)) = parse_placeholder(after) {
                    out.push_str(&quote(lookup(params, &name)?)?);
                    rest = &after[len..];
                } else {
                    out.push('$');
                    rest = &after[1..];
                }
            }
            out.push_str(rest);
            Ok(serde_json::Value::String(out))
        }
        serde_json::Value::Object(o) => {
            let mut out = serde_json::Map::with_capacity(o.len());
            for (k, v) in o {
                out.insert(k.clone(), substitute(v, params, quote)?);
            }
            Ok(serde_json::Value::Object(out))
        }
        serde_json::Value::Array(items) => Ok(serde_json::Value::Array(
            items.iter().map(|v| substitute(v, params, quote)).collect::<Result<_, _>>()?,
        )),
        other => Ok(other.clone()),
    }
}

fn exact_placeholder(s: &str) -> Option<&str> {
    let inner = s.strip_prefix("${")?.strip_suffix('}')?;
    if !inner.is_empty() && !inner.contains(['{', '}', '$']) {
        Some(inner)
    } else {
        None
    }
}

/// Parse `${name}` or `$N` at the start of `s` → (name, consumed length).
fn parse_placeholder(s: &str) -> Option<(String, usize)> {
    if let Some(after) = s.strip_prefix("${") {
        let end = after.find('}')?;
        return Some((after[..end].to_string(), end + 3));
    }
    let digits: String = s[1..].chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        let len = digits.len() + 1;
        Some((digits, len))
    }
}

fn lookup<'a>(
    params: &'a serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> Result<&'a serde_json::Value, RsError> {
    params
        .get(name)
        .ok_or_else(|| RsError::bad_request(format!("missing query parameter '{name}'")))
}

#[async_trait]
impl Service for QueryService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let segments = msg.url.service_segments();
        let name = match segments.as_slice() {
            [name] => name.to_string(),
            _ => {
                // GET / lists the stored queries (discovery support).
                if msg.method == http::Method::GET && segments.is_empty() {
                    let names: Vec<&String> = ctx
                        .config
                        .get("queries")
                        .and_then(|q| q.as_object())
                        .map(|o| o.keys().collect())
                        .unwrap_or_default();
                    return Ok(msg.ok_json(&serde_json::json!({ "queries": names })));
                }
                return Err(RsError::not_found("expected POST /<query-name>"));
            }
        };
        if msg.method != http::Method::POST {
            return Err(RsError::new(405, crate::error::codes::BAD_REQUEST, "Method Not Allowed",
                "stored queries execute with POST"));
        }

        let def = ctx
            .config
            .get("queries")
            .and_then(|q| q.get(&name))
            .ok_or_else(|| RsError::not_found(format!("no stored query '{name}'")))?
            .clone();

        // Parameters: JSON object body (or array → positional "0","1",…).
        let params = match &mut msg.body {
            None => serde_json::Map::new(),
            Some(b) => match b.as_json(ctx.limits.materialized_body_bytes).await? {
                serde_json::Value::Object(o) => o,
                serde_json::Value::Array(items) => items
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| (i.to_string(), v))
                    .collect(),
                serde_json::Value::Null => serde_json::Map::new(),
                _ => return Err(RsError::bad_request("query parameters must be an object or array")),
            },
        };

        // Validate parameters against the declared schema before execution.
        if let Some(schema) = def.get("params") {
            let validator = jsonschema::validator_for(schema)
                .map_err(|e| RsError::internal(format!("params schema failed to compile: {e}")))?;
            let value = serde_json::Value::Object(params.clone());
            let errors: Vec<serde_json::Value> = validator
                .iter_errors(&value)
                .map(|e| serde_json::json!({ "path": e.instance_path.to_string(), "error": e.to_string() }))
                .collect();
            if !errors.is_empty() {
                return Err(RsError::validation_failed(
                    format!("parameters for query '{name}' failed validation"),
                    serde_json::json!(errors),
                ));
            }
        }

        let store = ctx
            .query
            .as_ref()
            .ok_or_else(|| RsError::internal("query service has no QueryStore capability"))?;
        let template = def.get("query").unwrap();
        let quote = |v: &serde_json::Value| store.quote(v);
        let query = substitute(template, &params, &quote)?;

        let (take, skip) = pagination(&msg);
        let (rows, total) = store.run_query(&query, take, skip).await?;

        let mut resp = msg.ok_json(&serde_json::Value::Array(rows));
        resp.set_header("x-total-count", &total.to_string());
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_substitution_modes() {
        let params: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({ "status": "open", "min": 5, "0": "x" }))
                .unwrap();
        let quote = |v: &serde_json::Value| match v {
            serde_json::Value::String(s) => Ok(s.clone()),
            other => Ok(other.to_string()),
        };
        // Exact placeholder takes the JSON value (number stays a number).
        let out = substitute(
            &serde_json::json!({ "where": { "status": "${status}", "total": { "op": ">", "value": "${min}" } } }),
            &params,
            &quote,
        )
        .unwrap();
        assert_eq!(out["where"]["total"]["value"], 5);
        assert_eq!(out["where"]["status"], "open");
        // Embedded placeholders splice as quoted strings; $0 is positional.
        let out = substitute(&serde_json::json!("prefix-${status}-$0"), &params, &quote).unwrap();
        assert_eq!(out, "prefix-open-x");
        // Missing parameter is a structured 400.
        assert!(substitute(&serde_json::json!("${nope}"), &params, &quote).is_err());
    }
}
