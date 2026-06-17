//! The spec-store façade: file-like authoring for instruction-plane stores
//! (pipelines, queries), DRY over the one real store implementation.
//!
//! The store contract is implemented **once**, in [`FileService`]; spec
//! stores reach it by ownership, not duplication — v1 composed private
//! services through the config plane (manifest `privateServiceConfigs` +
//! pipelines); RS2 composes in the type system. The façade adds exactly
//! two things to `FileService`: write-time validation (with optional
//! canonicalization, e.g. DSL → typed pipeline), and the path mapping that
//! puts authoring under the mount's reserved dot-subtree
//! (`/<mount>/.pipelines/…`). Everything else — listings, ETags, Range,
//! keyless-POST naming, the 409 + `?confirm=` guard, pagination — is
//! inherited, so the store-conformance suite tests one implementation
//! through several doors.
//!
//! `PrefixedFileStore` roots the private capability under the store's
//! storage prefix; mount config `"store": {"root": "..."}` overrides the
//! default `.rs2-<kind><mount base>`. That decorator is also the seam where
//! per-store named infra (PRD §9.1) plugs in later.

use std::sync::Arc;

use serde_json::Value;

use crate::capabilities::{PrefixedFileStore, ScopedFileStore};
use crate::contract::InvocationLimits;
use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

use super::{FileService, Service, ServiceContext};

/// Validates a spec document at write time; returns the canonical form to
/// store (e.g. pipeline DSL converted to the typed spec).
pub type SpecValidator = Arc<dyn Fn(&Value) -> Result<Value, RsError> + Send + Sync>;

/// The spec name governing the mount root (an empty child name is not
/// representable in a file store).
pub const ROOT_SPEC: &str = ".root";

pub struct SpecStore {
    inner: FileService,
    inner_ctx: ServiceContext,
    /// The reserved authoring subtree segment, e.g. `.pipelines`.
    subtree: &'static str,
    validate: SpecValidator,
    /// Tenant operator roles — only an operator may set or change a spec's
    /// `access` field (authority is operator-controlled config, not author
    /// content). Empty ⇒ no API operator.
    operator_roles: String,
}

/// Resolve the storage root for a spec store: mount config
/// `"store": {"root": ...}` or the default `.rs2-<kind><mount base>`.
/// (Also used by discovery, which lists specs through the tenant handle.)
pub fn store_root(default_kind_prefix: &str, base_path: &str, config: &Value) -> String {
    let store = config
        .get("store")
        .and_then(|s| serde_json::from_value::<crate::config_schema::StoreConfig>(s.clone()).ok())
        .unwrap_or_default();
    store
        .root
        .map(|r| r.trim_matches('/').to_string())
        .unwrap_or_else(|| format!("{default_kind_prefix}{base_path}").trim_matches('/').to_string())
}

impl SpecStore {
    /// `files` is the raw (unscoped) tenant file adapter; the façade roots
    /// it under `root` and scopes it to the tenant — the private capability
    /// only the inner `FileService` sees.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        files: Arc<dyn crate::capabilities::FileStore>,
        tenant: &str,
        root: &str,
        subtree: &'static str,
        limits: InvocationLimits,
        validate: SpecValidator,
        operator_roles: Option<String>,
    ) -> Self {
        let prefixed: Arc<dyn crate::capabilities::FileStore> =
            Arc::new(PrefixedFileStore::new(files, root));
        let inner_ctx = ServiceContext {
            config: serde_json::json!({}),
            files: Some(ScopedFileStore::new(prefixed, tenant)),
            data: None,
            query: None,
            http: None,
            cors: Arc::new(crate::wrapper::CorsPolicy::default()),
            limits,
            requester: None,
            control: None,
            tenant_retry: None,
            pipeline_wall_clock: std::time::Duration::from_secs(120),
            // The spec store's private FileService doesn't emit app logs.
            logger: crate::logging::ServiceLogger::new(
                Arc::new(crate::logging::NullLogStore),
                tenant,
                "",
                "spec-store",
            ),
            log_store: None,
            catalogue: None,
            builtin_adapters: None,
            secrets: None,
        };
        SpecStore {
            inner: FileService::new(),
            inner_ctx,
            subtree,
            validate,
            operator_roles: operator_roles.unwrap_or_default(),
        }
    }

    /// Whether a request path enters the authoring subtree (its first
    /// service segment is the reserved dot-name).
    pub fn is_authoring(&self, msg: &Message) -> bool {
        msg.url.service_segments().first().map(|s| *s == self.subtree).unwrap_or(false)
    }

    /// Handle an authoring request: validate spec bodies on PUT and keyless
    /// POST, remap the mount split to `{base}/{subtree}`, and delegate to
    /// the owned `FileService` (which generates publicly-correct Locations
    /// and listing paths because the mount split says so).
    pub async fn handle_authoring(&self, mut msg: Message) -> Result<Message, RsError> {
        // Re-split the URL so the inner FileService sees paths relative to
        // the authoring subtree.
        let inner_base = format!("{}/{}", msg.url.base_path, self.subtree);
        msg.url.apply_mount(&inner_base);

        // Spec writes validate (and canonicalize) before they store.
        let is_write = msg.method == http::Method::PUT
            || (msg.method == http::Method::POST && msg.url.is_directory());
        if is_write {
            let doc = match &mut msg.body {
                Some(b) => b.as_json(self.inner_ctx.limits.materialized_body_bytes).await?,
                None => return Err(RsError::bad_request("spec write requires a JSON body")),
            };
            // Authority gate: a spec's `access` field is operator-controlled
            // configuration (per-spec authorization), not author content. A
            // non-operator may author or edit a spec but must leave `access`
            // exactly as stored; setting, changing, or removing it requires an
            // operator principal (a GET-modify-PUT that resends the existing
            // `access` is fine).
            let incoming_access = doc.get("access");
            let existing_doc = self.read(&msg.url.service_path).await.ok();
            let existing_access = existing_doc.as_ref().and_then(|d| d.get("access"));
            if incoming_access != existing_access
                && !crate::wrapper::is_operator(msg.principal.as_ref(), &self.operator_roles)
            {
                return Err(RsError::forbidden(
                    "setting or changing a spec's 'access' requires an operator (operatorRoles)",
                ));
            }
            let canonical = (self.validate)(&doc)?;
            msg.body = Some(Body::from_bytes(canonical.to_string(), MediaType::json()));
        }

        let is_listing = msg.method == http::Method::GET && msg.url.is_directory();
        let is_spec_read = msg.method == http::Method::GET && !msg.url.is_directory();
        let listing_path = msg.url.service_path.clone();
        let template = msg.response(http::StatusCode::OK, None);
        match self.inner.handle(msg, &self.inner_ctx).await {
            // Specs are JSON by construction; extension-less files would
            // otherwise serve as octet-stream.
            Ok(mut resp) if is_spec_read => {
                if let Some(body) = &mut resp.body {
                    body.media_type = MediaType::json();
                }
                Ok(resp)
            }
            // A store with nothing in it yet is empty, not absent.
            Err(e) if is_listing && e.code == crate::error::codes::NOT_FOUND => {
                let listing =
                    serde_json::json!({ "path": listing_path, "entries": [], "total": 0 });
                let mut resp = template.ok(Some(Body::from_bytes(
                    listing.to_string(),
                    crate::message::MediaType::dir_json(),
                )));
                resp.set_header("x-total-count", "0");
                Ok(resp)
            }
            other => other,
        }
    }

    /// Read one stored spec by its subtree-relative path.
    pub async fn read(&self, spec_path: &str) -> Result<Value, RsError> {
        let files = self.inner_ctx.files.as_ref().unwrap();
        let mut body = files
            .read(spec_path, None)
            .await
            .map_err(|_| RsError::not_found(format!("no stored spec at '{spec_path}'")))?;
        let bytes = body.materialize(self.inner_ctx.limits.materialized_body_bytes).await?;
        serde_json::from_slice(bytes)
            .map_err(|e| RsError::internal(format!("stored spec is corrupt: {e}")))
    }

    /// Execution resolution: the longest stored prefix of `segments` wins
    /// (peeled segments become the caller's positional params); with no
    /// match, the `.root` spec governs. Returns `(doc, matched_len)`.
    pub async fn resolve(&self, segments: &[String]) -> Result<Option<(Value, usize)>, RsError> {
        let mut split = segments.len();
        while split >= 1 {
            let candidate = format!("/{}", segments[..split].join("/"));
            if let Ok(doc) = self.read(&candidate).await {
                return Ok(Some((doc, split)));
            }
            split -= 1;
        }
        match self.read(&format!("/{ROOT_SPEC}")).await {
            Ok(doc) => Ok(Some((doc, 0))),
            Err(_) => Ok(None),
        }
    }

    pub fn subtree(&self) -> &'static str {
        self.subtree
    }
}
