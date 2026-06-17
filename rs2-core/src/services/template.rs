//! `template` — JSX templates rendered to HTML, store-patterned.
//!
//! Authoring lives under the reserved subtree `/<mount>/.templates/…` — a
//! normal store-contract surface delegated to the owned `FileService` via
//! [`SpecStore`] (validated at write time). Every other path, on **any verb**,
//! renders: the longest stored prefix wins (peeled segments become positional
//! props `"0"`, `"1"`, …; a `.root` template governs the mount root), so a
//! template can serve a plain `GET`.
//!
//! A stored template is **not** raw JSX: the JS engine has no transpiler and
//! runs single-file ESM only, so JSX is transpiled+bundled CLI-side
//! (`rs2 template build`, esbuild + Preact) into a self-contained module whose
//! default export is `async (msg) => { status, headers, body }`. The compiled
//! bundle is stored inside a small JSON envelope `{ "source": "…" }` so it
//! round-trips through the JSON-shaped [`SpecStore`]. Rendering is then pure
//! execution: the bundle is built once into a resident isolate (cached per
//! template version) and dispatched with the request props as its body.
//!
//! Render props, later sources winning: positional URL segments → query-string
//! pairs → JSON request body (an object spreads as named props). The merged
//! object reaches the template component as its `props`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{Map, Value};

use super::spec_store::{SpecStore, ROOT_SPEC};
use super::{query_template, Service, ServiceContext};
use crate::contract::InvocationLimits;
use crate::engines::resident::ResidentAdapter;
use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

/// Default storage prefix (under the tenant file store) for template mounts.
pub const TEMPLATE_PREFIX: &str = ".rs2-templates";

/// The reserved authoring subtree segment.
pub const TEMPLATE_SUBTREE: &str = ".templates";

/// Media type used when a template envelope declares no `contentType`.
const DEFAULT_CONTENT_TYPE: &str = "text/html; charset=utf-8";

pub struct TemplateService {
    store: SpecStore,
    /// Compiled-bundle isolates, keyed by matched template name → its content
    /// version. A `PUT` that changes the bundle bumps the version, so the next
    /// render replaces the stale isolate. Built lazily on first render.
    cache: Mutex<HashMap<String, (String, Arc<ResidentAdapter>)>>,
}

impl TemplateService {
    pub fn from_config(_config: &Value, store: SpecStore) -> Result<Self, RsError> {
        Ok(TemplateService { store, cache: Mutex::new(HashMap::new()) })
    }

    /// The write-time validator: the envelope must carry a non-empty compiled
    /// `source` string. (Bundles that fail to evaluate surface their error at
    /// first render — V8 compilation needs an isolate, which the sync validator
    /// has no access to.)
    pub fn validator() -> super::spec_store::SpecValidator {
        Arc::new(|doc: &Value| match doc.get("source").and_then(|s| s.as_str()) {
            Some(s) if !s.trim().is_empty() => Ok(doc.clone()),
            _ => Err(RsError::bad_request(
                "template envelope must be a JSON object with a non-empty \"source\" string \
                 (the compiled bundle from `rs2 template build`)",
            )),
        })
    }

    /// The resident isolate for a template, reused while its version matches.
    fn adapter_for(
        &self,
        key: &str,
        version: &str,
        source: String,
        tenant: &str,
        limits: InvocationLimits,
    ) -> Arc<ResidentAdapter> {
        let mut cache = self.cache.lock().expect("template cache mutex poisoned");
        if let Some((have, adapter)) = cache.get(key) {
            if have == version {
                return adapter.clone();
            }
        }
        let adapter = Arc::new(ResidentAdapter::from_inline(source, tenant, limits));
        cache.insert(key.to_string(), (version.to_string(), adapter.clone()));
        adapter
    }
}

/// A stable content version for a compiled bundle (cache key for its isolate).
fn version_of(source: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    source.hash(&mut h);
    format!("{:016x}", h.finish())
}

#[async_trait]
impl Service for TemplateService {
    async fn handle(&self, mut msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        if self.store.is_authoring(&msg) {
            return self.store.handle_authoring(msg).await;
        }

        // ---- render: any verb, longest stored prefix ----
        let segments: Vec<String> =
            msg.url.service_segments().iter().map(|s| s.to_string()).collect();
        let Some((doc, split)) = self.store.resolve(&segments).await? else {
            return Err(RsError::not_found(format!(
                "no template matches '{}' (author one at {}{}/…)",
                msg.url.service_path, msg.url.base_path, TEMPLATE_SUBTREE,
            )));
        };
        let source = doc
            .get("source")
            .and_then(|s| s.as_str())
            .ok_or_else(|| RsError::internal("stored template is missing its compiled 'source'"))?
            .to_string();
        let content_type = doc
            .get("contentType")
            .and_then(|c| c.as_str())
            .unwrap_or(DEFAULT_CONTENT_TYPE)
            .to_string();

        // Props: positional URL segments, then query-string pairs, then the
        // JSON body (object = named props) — later wins.
        let mut props = Map::new();
        for (i, seg) in segments[split..].iter().enumerate() {
            props.insert(i.to_string(), Value::String(seg.clone()));
        }
        props.extend(query_template::url_params(&msg.url.query, None));
        match &mut msg.body {
            None => {}
            Some(b) => match b.as_json(ctx.limits.materialized_body_bytes).await? {
                Value::Object(named) => props.extend(named),
                Value::Null => {}
                _ => {
                    return Err(RsError::bad_request(
                        "template props must be a JSON object",
                    ))
                }
            },
        }

        // Build (or reuse) the isolate for this template version and render.
        let key = if split == 0 { ROOT_SPEC.to_string() } else { segments[..split].join("/") };
        let version = version_of(&source);
        let adapter =
            self.adapter_for(&key, &version, source, &msg.tenant, ctx.limits.clone());
        let (status, body) = adapter.call("POST", "/", Some(Value::Object(props))).await?;
        let html = body.as_str().ok_or_else(|| {
            RsError::contract_violation("template render did not return a string body")
        })?;

        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
        Ok(msg.response(status, Some(Body::from_string(html, MediaType::parse(&content_type)))))
    }
}
