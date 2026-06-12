//! `data` service (PRD §10.2): schema-validated JSON store over a
//! `DataStore` capability. `/<dataset>/<key>` CRUD; PATCH = JSON merge;
//! keyless POST generates a key; `.schema.json` endpoints manage the dataset
//! schema; instance responses carry `application/json; schema="<url>"`;
//! dataset delete requires an explicit `?confirm=<dataset>` token.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use tokio::sync::RwLock;

use super::{pagination, Service, ServiceContext};
use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

const SCHEMA_RESOURCE: &str = ".schema.json";

#[derive(Default)]
pub struct DataService {
    /// Compiled validators cached per (dataset, schema version) — PRD §10.2:
    /// validator compiled and cached per dataset schema version.
    validators: RwLock<HashMap<(String, u64), Arc<jsonschema::Validator>>>,
}

impl DataService {
    pub fn new() -> Self {
        Self::default()
    }

    async fn validator_for(&self, dataset: &str, schema: &Value) -> Result<Arc<jsonschema::Validator>, RsError> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        schema.to_string().hash(&mut hasher);
        let version = hasher.finish();
        let key = (dataset.to_string(), version);
        if let Some(v) = self.validators.read().await.get(&key) {
            return Ok(v.clone());
        }
        let validator = jsonschema::validator_for(schema)
            .map_err(|e| RsError::bad_request(format!("dataset schema is not a valid JSON Schema: {e}")))?;
        let validator = Arc::new(validator);
        self.validators.write().await.insert(key, validator.clone());
        Ok(validator)
    }

    async fn validate(&self, dataset: &str, schema: &Value, instance: &Value) -> Result<(), RsError> {
        let validator = self.validator_for(dataset, schema).await?;
        let errors: Vec<Value> = validator
            .iter_errors(instance)
            .map(|e| json!({ "path": e.instance_path.to_string(), "message": e.to_string() }))
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(RsError::validation_failed(
                format!("body does not conform to the '{dataset}' dataset schema"),
                Value::Array(errors),
            ))
        }
    }
}

/// Record version for ETags: a stable hash of the serialized value, so
/// optimistic concurrency reads identically to the file store's ETags.
fn record_etag(value: &Value) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.to_string().hash(&mut hasher);
    format!("\"{:016x}\"", hasher.finish())
}

/// RFC 7386 JSON merge patch.
fn merge_patch(target: &mut Value, patch: &Value) {
    if let Value::Object(patch_obj) = patch {
        if !target.is_object() {
            *target = json!({});
        }
        let target_obj = target.as_object_mut().unwrap();
        for (k, v) in patch_obj {
            if v.is_null() {
                target_obj.remove(k);
            } else {
                merge_patch(target_obj.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
    } else {
        *target = patch.clone();
    }
}

#[async_trait]
impl Service for DataService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let data = ctx.data.as_ref().ok_or_else(|| RsError::capability_denied("data"))?;
        let enforce_schema = ctx.config.get("enforceSchema").and_then(|v| v.as_bool()).unwrap_or(false);
        let segments: Vec<String> = msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        let schema_base = format!("{}", msg.url.base_path);

        match segments.as_slice() {
            // ---- mount root: enumerate datasets (store contract: every
            // container level lists in dir+json) ----
            [] => match msg.method {
                Method::GET => {
                    let (take, skip) = pagination(&msg);
                    let (names, total) = data.list_datasets(take, skip).await?;
                    let entries: Vec<Value> = names
                        .iter()
                        .map(|n| json!({ "name": format!("{n}/"), "dir": true }))
                        .collect();
                    let listing = json!({ "path": "/", "entries": entries, "total": total });
                    let mut resp =
                        msg.ok(Some(Body::from_bytes(listing.to_string(), MediaType::dir_json())));
                    resp.set_header("x-total-count", &total.to_string());
                    Ok(resp)
                }
                _ => Err(RsError::bad_request("the mount root supports GET (dataset listing)")),
            },

            // ---- dataset level (a store container) ----
            [dataset] => {
                let dataset = dataset.clone();
                match msg.method {
                    Method::GET => {
                        let (take, skip) = pagination(&msg);
                        let (keys, total) = data.list_keys(&dataset, take, skip).await?;
                        // One listing shape across all stores (PRD patterns):
                        // keys are child entries; the schema is a fixed child.
                        let mut entries: Vec<Value> = keys
                            .iter()
                            .map(|k| json!({ "name": k, "dir": false }))
                            .collect();
                        if data.get_schema(&dataset).await?.is_some() {
                            entries.push(json!({ "name": SCHEMA_RESOURCE, "dir": false, "fixed": true }));
                        }
                        let listing = json!({
                            "path": format!("/{dataset}/"),
                            "entries": entries,
                            "total": total,
                        });
                        let mut resp = msg.ok(Some(Body::from_bytes(listing.to_string(), MediaType::dir_json())));
                        resp.set_header("x-total-count", &total.to_string());
                        Ok(resp)
                    }
                    // Keyless POST to a container creates under a generated
                    // key and returns the stored representation + Location.
                    Method::POST => {
                        let body = msg
                            .body
                            .as_mut()
                            .ok_or_else(|| RsError::bad_request("write requires a JSON body"))?;
                        let value = body.as_json(ctx.limits.materialized_body_bytes).await?;
                        if enforce_schema {
                            if let Some(schema) = data.get_schema(&dataset).await? {
                                self.validate(&dataset, &schema, &value).await?;
                            }
                        }
                        let key = uuid::Uuid::new_v4().simple().to_string();
                        data.put(&dataset, &key, value.clone()).await?;
                        let schema_url = format!("{schema_base}/{dataset}/{SCHEMA_RESOURCE}");
                        let mut resp = msg.response(
                            StatusCode::CREATED,
                            Some(
                                Body::from_bytes(value.to_string(), MediaType::json())
                                    .with_schema(schema_url),
                            ),
                        );
                        resp.set_header("location", &format!("{schema_base}/{dataset}/{key}"));
                        resp.set_header("etag", &record_etag(&value));
                        Ok(resp)
                    }
                    Method::DELETE => {
                        // Explicit confirm token replaces emptiness
                        // heuristics. 409 matches the store contract's
                        // non-empty-container guard across all stores.
                        let confirm = msg.url.query_param("confirm");
                        if confirm.as_deref() != Some(dataset.as_str()) {
                            return Err(RsError::conflict(format!(
                                "dataset delete requires '?confirm={dataset}'"
                            )));
                        }
                        data.delete_dataset(&dataset).await?;
                        Ok(msg.no_content())
                    }
                    _ => Err(RsError::bad_request("dataset level supports GET, POST, DELETE")),
                }
            }

            // ---- dataset schema ----
            [dataset, resource] if resource == SCHEMA_RESOURCE => {
                let dataset = dataset.clone();
                match msg.method {
                    Method::GET => {
                        let schema = data
                            .get_schema(&dataset)
                            .await?
                            .ok_or_else(|| RsError::not_found(format!("dataset '{dataset}' has no schema")))?;
                        Ok(msg.ok(Some(Body::from_bytes(
                            schema.to_string(),
                            MediaType::new(crate::message::media_type::SCHEMA_JSON),
                        ))))
                    }
                    Method::PUT => {
                        let body = msg
                            .body
                            .as_mut()
                            .ok_or_else(|| RsError::bad_request("schema write requires a body"))?;
                        let schema = body.as_json(ctx.limits.materialized_body_bytes).await?;
                        // Refuse schemas that won't compile, before storing.
                        jsonschema::validator_for(&schema)
                            .map_err(|e| RsError::bad_request(format!("not a valid JSON Schema: {e}")))?;
                        data.put_schema(&dataset, schema).await?;
                        Ok(msg.ok(None))
                    }
                    _ => Err(RsError::bad_request("schema resource supports GET and PUT")),
                }
            }

            // ---- record level ----
            [dataset, key] => {
                let (dataset, key) = (dataset.clone(), key.clone());
                let schema_url = format!("{schema_base}/{dataset}/{SCHEMA_RESOURCE}");
                match msg.method {
                    Method::GET => {
                        let value = data.get(&dataset, &key).await?;
                        let mut resp = msg.ok(Some(
                            Body::from_bytes(value.to_string(), MediaType::json()).with_schema(schema_url.clone()),
                        ));
                        resp.set_header("link", &format!("<{schema_url}>; rel=\"describedby\""));
                        resp.set_header("etag", &record_etag(&value));
                        Ok(resp)
                    }
                    // Store contract: PUT upserts (empty body); POST upserts
                    // and returns the stored representation.
                    Method::PUT | Method::POST => {
                        let echo = msg.method == Method::POST;
                        let body = msg
                            .body
                            .as_mut()
                            .ok_or_else(|| RsError::bad_request("write requires a JSON body"))?;
                        let value = body.as_json(ctx.limits.materialized_body_bytes).await?;
                        if enforce_schema {
                            if let Some(schema) = data.get_schema(&dataset).await? {
                                self.validate(&dataset, &schema, &value).await?;
                            }
                        }
                        let created = data.put(&dataset, &key, value.clone()).await?;
                        let status = if created { StatusCode::CREATED } else { StatusCode::OK };
                        let stored = if echo {
                            Some(
                                Body::from_bytes(value.to_string(), MediaType::json())
                                    .with_schema(schema_url),
                            )
                        } else {
                            None
                        };
                        let mut resp = msg.response(status, stored);
                        resp.set_header("etag", &record_etag(&value));
                        Ok(resp)
                    }
                    Method::PATCH => {
                        let body = msg
                            .body
                            .as_mut()
                            .ok_or_else(|| RsError::bad_request("patch requires a JSON body"))?;
                        let patch = body.as_json(ctx.limits.materialized_body_bytes).await?;
                        let mut current = data.get(&dataset, &key).await?;
                        merge_patch(&mut current, &patch);
                        // PATCH results are validated too (PRD §10.2).
                        if enforce_schema {
                            if let Some(schema) = data.get_schema(&dataset).await? {
                                self.validate(&dataset, &schema, &current).await?;
                            }
                        }
                        data.put(&dataset, &key, current.clone()).await?;
                        Ok(msg.ok(Some(
                            Body::from_bytes(current.to_string(), MediaType::json()).with_schema(schema_url),
                        )))
                    }
                    Method::DELETE => {
                        data.delete(&dataset, &key).await?;
                        Ok(msg.no_content())
                    }
                    _ => Err(RsError::bad_request("record level supports GET, PUT, POST, PATCH, DELETE")),
                }
            }

            _ => Err(RsError::bad_request("data paths are /<dataset>/<key>")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_patch_follows_rfc7386() {
        let mut target = json!({"a": 1, "b": {"c": 2, "d": 3}, "e": 4});
        merge_patch(&mut target, &json!({"a": 9, "b": {"c": null}, "e": null, "f": 5}));
        assert_eq!(target, json!({"a": 9, "b": {"d": 3}, "f": 5}));
    }
}
