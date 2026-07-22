//! `query` — stored parameterized queries (PRD §10.4), store-patterned.
//!
//! Authoring lives under the reserved subtree `/<mount>/.queries/…` — a
//! normal store-contract surface delegated to the owned `FileService` via
//! [`SpecStore`] (validation at write time). Every other path, on **any
//! verb**, executes: the longest stored prefix wins (peeled segments become
//! positional params `"0"`, `"1"`, …; a `.root` spec governs the mount
//! root), so a stored query can serve plain `GET`.
//!
//! Execution parameters, later sources winning: positional URL segments →
//! query-string pairs (coerced to the params schema's declared types) →
//! JSON body (object named / array positional). Defaults apply, then the
//! whole set validates against the envelope's `params` schema.

use async_trait::async_trait;
use serde_json::{Map, Value};

use super::query_template::{self, Language, QueryEnvelope};
use super::spec_store::SpecStore;
use super::{pagination, Service, ServiceContext};
use crate::error::RsError;
use crate::message::Message;

/// Default storage prefix (under the tenant file store) for query mounts.
pub const QUERY_PREFIX: &str = ".rs2-queries";

/// The reserved authoring subtree segment.
pub const QUERY_SUBTREE: &str = ".queries";

pub struct QueryService {
    store: SpecStore,
}

impl QueryService {
    pub fn from_config(config: &Value, store: SpecStore) -> Result<Self, RsError> {
        if config.get("queries").is_some() {
            return Err(RsError::bad_request(
                "config-defined queries are no longer supported: PUT query envelopes to \
                 /<mount>/.queries/<name> instead",
            ));
        }
        Ok(QueryService { store })
    }

    /// The write-time validator: the envelope must parse (schemas compile,
    /// placeholders scan); stored as submitted.
    pub fn validator() -> super::spec_store::SpecValidator {
        std::sync::Arc::new(|doc: &Value| {
            QueryEnvelope::parse(doc)?;
            Ok(doc.clone())
        })
    }
}

#[async_trait]
impl Service for QueryService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        if self.store.is_authoring(&msg) {
            return self.store.handle_authoring(msg).await;
        }

        // ---- execution: any verb, longest stored prefix ----
        let store = ctx
            .query
            .as_ref()
            .ok_or_else(|| RsError::internal("query service has no QueryStore capability"))?;
        let segments: Vec<String> = msg
            .url
            .service_segments()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let Some((doc, split)) = self.store.resolve(&segments).await? else {
            return Err(RsError::not_found(format!(
                "no stored query matches '{}' (author one at {}{}/…)",
                msg.url.service_path, msg.url.base_path, QUERY_SUBTREE,
            )));
        };
        let envelope = QueryEnvelope::parse(&doc)?;

        // Parameters: positional URL segments, then query-string pairs
        // (schema-coerced), then the JSON body — later wins.
        let mut params = Map::new();
        for (i, seg) in segments[split..].iter().enumerate() {
            params.insert(i.to_string(), Value::String(seg.clone()));
        }
        params.extend(query_template::url_params(
            &msg.url.query,
            envelope.params_schema.as_ref(),
        ));
        match &mut msg.body {
            None => {}
            Some(b) => match b.as_json(ctx.limits.materialized_body_bytes).await? {
                Value::Object(named) => params.extend(named),
                Value::Array(items) => {
                    for (i, v) in items.into_iter().enumerate() {
                        params.insert(i.to_string(), v);
                    }
                }
                Value::Null => {}
                _ => {
                    return Err(RsError::bad_request(
                        "query parameters must be a JSON object or array",
                    ))
                }
            },
        }
        let params = envelope.prepare_params(params)?;

        // JSON templates substitute structurally here; string templates
        // (SQL) pass through for the adapter to bind.
        let query = match envelope.language {
            Language::Json => {
                let quote = |v: &Value| store.quote(v);
                query_template::substitute_json(&envelope.query, &params, &quote)?
            }
            Language::Text => envelope.query.clone(),
        };

        let (take, skip) = pagination(&msg);
        let (rows, total) = store.run_query(&query, &params, take, skip).await?;

        let mut resp = msg.ok_json(&Value::Array(rows));
        resp.set_header("x-total-count", &total.to_string());
        Ok(resp)
    }
}
