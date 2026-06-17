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
/// Mount-level schema index: `GET /<mount>/.schemas` returns every dataset's
/// installed schema in one call (G6).
const SCHEMAS_RESOURCE: &str = ".schemas";

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

/// Top-level per-field access rules from a dataset schema (PRD §5.2): each
/// `properties.<field>` carrying `x-rs-read` and/or `x-rs-write` (role-spec
/// strings). Fields without either are unrestricted (the mount `access` is the
/// coarse gate; field rules only narrow). Top-level properties only.
fn field_rules(schema: &Value) -> Vec<(String, Option<String>, Option<String>)> {
    let Some(props) = schema.get("properties").and_then(|p| p.as_object()) else {
        return Vec::new();
    };
    props
        .iter()
        .filter_map(|(name, def)| {
            let read = def.get("x-rs-read").and_then(|v| v.as_str()).map(str::to_string);
            let write = def.get("x-rs-write").and_then(|v| v.as_str()).map(str::to_string);
            (read.is_some() || write.is_some()).then(|| (name.clone(), read, write))
        })
        .collect()
}

/// Fetch a record, mapping "not found" to JSON null — the write-target of a
/// create that doesn't exist yet.
async fn get_or_null(
    data: &crate::capabilities::ScopedDataStore,
    dataset: &str,
    key: &str,
) -> Result<Value, RsError> {
    match data.get(dataset, key).await {
        Ok(v) => Ok(v),
        Err(e) if e.code == crate::error::codes::NOT_FOUND => Ok(Value::Null),
        Err(e) => Err(e),
    }
}

/// Drop the top-level fields whose `x-rs-read` the caller doesn't satisfy.
fn redact_fields(value: &mut Value, schema: &Value, msg: &Message) {
    let Value::Object(obj) = value else { return };
    for (name, read, _) in field_rules(schema) {
        if let Some(rule) = read {
            if !crate::wrapper::satisfies_role_spec(&rule, msg) {
                obj.remove(&name);
            }
        }
    }
}

/// Enforce per-field write rules on the record a write would produce, mutating
/// `final_value` in place. A field the caller can't *read* is restored from
/// `stored` (a write can neither change nor drop what the caller can't see — so
/// a read→edit→write-back never loses redacted data, and a blind write to a
/// hidden field is silently ignored rather than leaking its existence). A change
/// to a field the caller *can* read but can't *write* is rejected with 403.
fn enforce_write_rules(
    final_value: &mut Value,
    stored: &Value,
    schema: &Value,
    msg: &Message,
) -> Result<(), RsError> {
    let Value::Object(obj) = final_value else { return Ok(()) };
    for (name, read, write) in field_rules(schema) {
        let readable = read.as_deref().map_or(true, |r| crate::wrapper::satisfies_role_spec(r, msg));
        let writable = write.as_deref().map_or(true, |w| crate::wrapper::satisfies_role_spec(w, msg));
        if !readable {
            match stored.get(&name) {
                Some(v) => {
                    obj.insert(name.clone(), v.clone());
                }
                None => {
                    obj.remove(&name);
                }
            }
        } else if !writable && obj.get(&name) != stored.get(&name) {
            return Err(RsError::forbidden(format!(
                "field '{name}' is not writable by this principal"
            )));
        }
    }
    Ok(())
}

#[async_trait]
impl Service for DataService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        let data = ctx.data.as_ref().ok_or_else(|| RsError::capability_denied("data"))?;
        let cfg = serde_json::from_value::<crate::config_schema::DataConfig>(ctx.config.clone())
            .unwrap_or_default();
        let enforce_schema = cfg.enforce_schema;
        let field_authz = cfg.field_level_authz;
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

            // ---- mount-level schema index: every dataset's installed schema
            // in one call, so a generic client can discover all shapes up
            // front instead of fetching each `.schema.json` by name ----
            [reserved] if reserved == SCHEMAS_RESOURCE => match msg.method {
                Method::GET => {
                    let (names, _) = data.list_datasets(10_000, 0).await?;
                    let mut schemas = serde_json::Map::new();
                    for name in names {
                        if let Some(schema) = data.get_schema(&name).await? {
                            schemas.insert(
                                name.clone(),
                                json!({
                                    "schemaUrl": format!("{schema_base}/{name}/{SCHEMA_RESOURCE}"),
                                    "schema": schema,
                                }),
                            );
                        }
                    }
                    Ok(msg.ok(Some(Body::from_bytes(
                        json!({ "schemas": schemas }).to_string(),
                        MediaType::json(),
                    ))))
                }
                _ => Err(RsError::bad_request(".schemas supports GET")),
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
                            .map(|k| json!({ "name": k, "dir": false, "contentType": "application/json" }))
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
                        let mut value = body.as_json(ctx.limits.materialized_body_bytes).await?;
                        let schema = if field_authz || enforce_schema {
                            data.get_schema(&dataset).await?
                        } else {
                            None
                        };
                        if field_authz {
                            if let Some(schema) = &schema {
                                // New key ⇒ no stored record: a no-write field
                                // present here is a create attempt → 403.
                                enforce_write_rules(&mut value, &Value::Null, schema, &msg)?;
                            }
                        }
                        if enforce_schema {
                            if let Some(schema) = &schema {
                                self.validate(&dataset, schema, &value).await?;
                            }
                        }
                        let key = uuid::Uuid::new_v4().simple().to_string();
                        let etag = record_etag(&value);
                        data.put(&dataset, &key, value.clone()).await?;
                        let schema_url = format!("{schema_base}/{dataset}/{SCHEMA_RESOURCE}");
                        if field_authz {
                            if let Some(schema) = &schema {
                                redact_fields(&mut value, schema, &msg);
                            }
                        }
                        let mut resp = msg.response(
                            StatusCode::CREATED,
                            Some(
                                Body::from_bytes(value.to_string(), MediaType::json())
                                    .with_schema(schema_url),
                            ),
                        );
                        resp.set_header("location", &format!("{schema_base}/{dataset}/{key}"));
                        resp.set_header("etag", &etag);
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
                        // The schema carries the field-authz policy, so on a
                        // fieldLevelAuthz mount editing it is authority-defining
                        // and requires an operator (else the policy is
                        // self-bypassable).
                        if field_authz
                            && !crate::wrapper::is_operator(
                                msg.principal.as_ref(),
                                ctx.operator_roles.as_deref().unwrap_or(""),
                            )
                        {
                            return Err(RsError::forbidden(
                                "editing the schema on a fieldLevelAuthz mount requires an operator",
                            ));
                        }
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
                        let mut value = data.get(&dataset, &key).await?;
                        // ETag is over the stored (unredacted) record so
                        // optimistic concurrency is stable across callers with
                        // different field visibility.
                        let etag = record_etag(&value);
                        // Conditional GET: revalidate without resending.
                        if super::if_none_match_hits(msg.header("if-none-match"), &etag) {
                            let mut not_modified = msg.response(StatusCode::NOT_MODIFIED, None);
                            not_modified.set_header("etag", &etag);
                            return Ok(not_modified);
                        }
                        if field_authz {
                            if let Some(schema) = data.get_schema(&dataset).await? {
                                redact_fields(&mut value, &schema, &msg);
                            }
                        }
                        let mut resp = msg.ok(Some(
                            Body::from_bytes(value.to_string(), MediaType::json()).with_schema(schema_url.clone()),
                        ));
                        resp.set_header("link", &format!("<{schema_url}>; rel=\"describedby\""));
                        resp.set_header("etag", &etag);
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
                        let mut value = body.as_json(ctx.limits.materialized_body_bytes).await?;
                        let schema = if field_authz || enforce_schema {
                            data.get_schema(&dataset).await?
                        } else {
                            None
                        };
                        if field_authz {
                            if let Some(schema) = &schema {
                                // PUT is full-replace; under field authz it
                                // becomes a field-scoped merge against the
                                // existing record, so a caller can't drop a
                                // field they can't see and a change to a
                                // no-write field is rejected.
                                let stored = get_or_null(data, &dataset, &key).await?;
                                enforce_write_rules(&mut value, &stored, schema, &msg)?;
                            }
                        }
                        if enforce_schema {
                            if let Some(schema) = &schema {
                                self.validate(&dataset, schema, &value).await?;
                            }
                        }
                        // ETag over the stored (unredacted) record.
                        let etag = record_etag(&value);
                        let created = data.put(&dataset, &key, value.clone()).await?;
                        let status = if created { StatusCode::CREATED } else { StatusCode::OK };
                        let stored = if echo {
                            if field_authz {
                                if let Some(schema) = &schema {
                                    redact_fields(&mut value, schema, &msg);
                                }
                            }
                            Some(
                                Body::from_bytes(value.to_string(), MediaType::json())
                                    .with_schema(schema_url),
                            )
                        } else {
                            None
                        };
                        let mut resp = msg.response(status, stored);
                        resp.set_header("etag", &etag);
                        Ok(resp)
                    }
                    Method::PATCH => {
                        let body = msg
                            .body
                            .as_mut()
                            .ok_or_else(|| RsError::bad_request("patch requires a JSON body"))?;
                        let patch = body.as_json(ctx.limits.materialized_body_bytes).await?;
                        let stored = data.get(&dataset, &key).await?;
                        let mut current = stored.clone();
                        merge_patch(&mut current, &patch);
                        let schema = if field_authz || enforce_schema {
                            data.get_schema(&dataset).await?
                        } else {
                            None
                        };
                        // The merge ran against the full stored record, so a
                        // redacted (unseen) field is simply absent from the
                        // patch and preserved; the gate only rejects changes to
                        // fields the caller can read but not write.
                        if field_authz {
                            if let Some(schema) = &schema {
                                enforce_write_rules(&mut current, &stored, schema, &msg)?;
                            }
                        }
                        // PATCH results are validated too (PRD §10.2).
                        if enforce_schema {
                            if let Some(schema) = &schema {
                                self.validate(&dataset, schema, &current).await?;
                            }
                        }
                        data.put(&dataset, &key, current.clone()).await?;
                        if field_authz {
                            if let Some(schema) = &schema {
                                redact_fields(&mut current, schema, &msg);
                            }
                        }
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
