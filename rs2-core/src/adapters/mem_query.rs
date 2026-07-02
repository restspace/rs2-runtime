//! Query adapter over any [`DataStore`] (PRD §10.4): the default adapter
//! for deployments without a SQL backend, and the reference for the
//! `QueryStore` contract.
//!
//! Query template shape:
//!
//! ```json
//! { "dataset": "orders",
//!   "where": { "status": "open", "total": { "op": ">", "value": 100 } },
//!   "orderBy": "createdAt" }
//! ```
//!
//! `where` clauses AND together; field names may be dot-paths. Supported
//! ops: `==` (default), `!=`, `<`, `>`, `<=`, `>=`, `contains`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::capabilities::{DataStore, QueryStore};
use crate::error::RsError;

pub struct MemQueryStore {
    data: Arc<dyn DataStore>,
}

impl MemQueryStore {
    pub fn new(data: Arc<dyn DataStore>) -> Self {
        MemQueryStore { data }
    }
}

fn field<'a>(record: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = record;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

fn matches(record: &serde_json::Value, path: &str, clause: &serde_json::Value) -> Result<bool, RsError> {
    let (op, expected) = match clause {
        serde_json::Value::Object(o) if o.contains_key("op") => (
            o.get("op").and_then(|v| v.as_str()).unwrap_or("=="),
            o.get("value").cloned().unwrap_or(serde_json::Value::Null),
        ),
        other => ("==", other.clone()),
    };
    let actual = field(record, path);
    let ord = match (actual, &expected) {
        (Some(serde_json::Value::Number(a)), serde_json::Value::Number(b)) => {
            a.as_f64().zip(b.as_f64()).and_then(|(a, b)| a.partial_cmp(&b))
        }
        (Some(serde_json::Value::String(a)), serde_json::Value::String(b)) => Some(a.cmp(b).into()),
        _ => None,
    };
    Ok(match op {
        "==" => actual == Some(&expected),
        "!=" => actual != Some(&expected),
        "<" => ord == Some(std::cmp::Ordering::Less),
        ">" => ord == Some(std::cmp::Ordering::Greater),
        "<=" => matches!(ord, Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
        ">=" => matches!(ord, Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
        "contains" => match (actual, &expected) {
            (Some(serde_json::Value::String(s)), serde_json::Value::String(needle)) => {
                s.contains(needle.as_str())
            }
            (Some(serde_json::Value::Array(items)), v) => items.contains(v),
            _ => false,
        },
        other => return Err(RsError::bad_request(format!("unknown query op '{other}'"))),
    })
}

#[async_trait]
impl QueryStore for MemQueryStore {
    async fn run_query(
        &self,
        tenant: &str,
        query: &serde_json::Value,
        _params: &serde_json::Map<String, serde_json::Value>,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<serde_json::Value>, u64), RsError> {
        // JSON templates arrive structurally substituted; string-language
        // (SQL) templates are for binding adapters, not this one.
        if query.is_string() {
            return Err(RsError::engine_unavailable(
                "this query adapter executes JSON templates; sql/text queries need a SQL adapter",
            ));
        }
        let dataset = query
            .get("dataset")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RsError::bad_request("query template requires a 'dataset'"))?;
        let empty = serde_json::Map::new();
        let clauses = match query.get("where") {
            None => &empty,
            Some(serde_json::Value::Object(o)) => o,
            Some(_) => return Err(RsError::bad_request("'where' must be an object")),
        };

        // Scan the dataset (the reference adapter trades efficiency for
        // having no backend dependency; SQL adapters push this down). The
        // store filters in one pass, so non-matching records are never
        // cloned out of it.
        let keep = |record: &serde_json::Value| -> Result<bool, RsError> {
            for (path, clause) in clauses {
                if !matches(record, path, clause)? {
                    return Ok(false);
                }
            }
            Ok(true)
        };
        let matched = self.data.scan_matching(tenant, dataset, &keep).await?;
        let mut rows: Vec<serde_json::Value> = matched
            .into_iter()
            .map(|(key, mut record)| {
                if let Some(obj) = record.as_object_mut() {
                    obj.entry("_key".to_string()).or_insert(serde_json::json!(key));
                }
                record
            })
            .collect();

        if let Some(order_by) = query.get("orderBy").and_then(|v| v.as_str()) {
            rows.sort_by(|a, b| {
                let (a, b) = (field(a, order_by), field(b, order_by));
                match (a, b) {
                    (Some(serde_json::Value::Number(x)), Some(serde_json::Value::Number(y))) => x
                        .as_f64()
                        .partial_cmp(&y.as_f64())
                        .unwrap_or(std::cmp::Ordering::Equal),
                    (Some(serde_json::Value::String(x)), Some(serde_json::Value::String(y))) => {
                        x.cmp(y)
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            });
        }

        let total = rows.len() as u64;
        let page = rows.into_iter().skip(skip).take(take).collect();
        Ok((page, total))
    }

    fn quote(&self, value: &serde_json::Value) -> Result<String, RsError> {
        match value {
            serde_json::Value::String(s) => Ok(s.clone()),
            serde_json::Value::Number(n) => Ok(n.to_string()),
            serde_json::Value::Bool(b) => Ok(b.to_string()),
            other => Err(RsError::bad_request(format!(
                "cannot splice a {} into a string query position",
                match other {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                    _ => "value",
                }
            ))),
        }
    }
}
