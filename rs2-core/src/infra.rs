//! Infras (PRD §9.1): operator-defined, named *partial* storage-adapter
//! configurations. A tenant references an infra by name in a mount's `store`
//! (or `specStore`) sub-config and supplies only the fields the operator left
//! open; the operator's pre-baked fields — region, bucket, **secrets/keys** —
//! are merged in at `Tenant::build` time and never travel back to the tenant.
//!
//! This is the managed-infrastructure seam: a tenant owner can consume "the
//! prod S3" or "the shared Postgres" without ever seeing credentials, and the
//! operator can bake in limitations (`infraOnly` fields a tenant may not set)
//! and an `allowedTenants` allowlist.
//!
//! Infras live in a node-level source (the server's `infras.json`), loaded
//! through [`InfraLoader`] and held behind a swappable cell on `Adapters` so
//! the operator can reload without restarting (see `Runtime::reload_infras`).
//! They are never part of any tenant config, so they cannot leak through
//! `GET /services/raw`.

use std::borrow::Cow;
use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::RsError;

/// One operator-defined infra: a real adapter ref plus pre-baked config.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfraDef {
    /// The real backend this infra resolves to: `builtin:<name>` or
    /// `code:<name>@<version>`. Replaces the tenant's `infra:<name>` placeholder.
    pub adapter: String,
    /// Human-readable summary, surfaced on the tenant-facing infra listing.
    #[serde(default)]
    pub description: Option<String>,
    /// Tenants allowed to reference this infra. Empty ⇒ every tenant may.
    #[serde(default)]
    pub allowed_tenants: Vec<String>,
    /// Pre-baked adapter config (region, bucket, **secrets**). Overlaid onto
    /// the tenant's fields at merge time — **infra wins** on any conflict.
    #[serde(default)]
    pub config: Map<String, Value>,
    /// Fields a tenant is forbidden from setting (the "baked-in limitations").
    /// A tenant `store` carrying any of these is a 400 — even though the infra
    /// would win the merge, the explicit rejection is clearer than a silent
    /// override and lets an operator lock a field the infra doesn't itself set.
    #[serde(default)]
    pub infra_only: Vec<String>,
    /// Fields the tenant **must** supply (the gaps the operator left open). A
    /// merged config missing any of these is a 400 at config time.
    #[serde(default)]
    pub requires: Vec<String>,
}

impl InfraDef {
    /// The kind label of the real adapter (`builtin` / `code`), for the
    /// tenant-facing listing — never the full ref (which may carry hints).
    pub fn adapter_kind(&self) -> &'static str {
        if self.adapter.starts_with("code:") {
            "code"
        } else {
            "builtin"
        }
    }
}

/// The node's set of infras (name → definition). Built from `infras.json`.
#[derive(Debug, Clone, Default)]
pub struct InfraSet {
    defs: HashMap<String, InfraDef>,
}

impl InfraSet {
    /// Parse an `infras.json` document: a top-level object of name → infra.
    pub fn from_json(value: Value) -> Result<Self, RsError> {
        let defs: HashMap<String, InfraDef> = serde_json::from_value(value)
            .map_err(|e| RsError::bad_request(format!("invalid infras document: {e}")))?;
        Ok(InfraSet { defs })
    }

    pub fn get(&self, name: &str) -> Option<&InfraDef> {
        self.defs.get(name)
    }

    pub fn len(&self) -> usize {
        self.defs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.defs.is_empty()
    }

    /// Infra names, sorted — for error messages and the reload summary.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.defs.keys().cloned().collect();
        names.sort();
        names
    }

    /// (name, def) pairs visible to `tenant` (its `allowedTenants` gate),
    /// sorted by name — for the tenant-facing listing.
    pub fn visible_to<'a>(&'a self, tenant: &str) -> Vec<(&'a str, &'a InfraDef)> {
        let mut out: Vec<(&str, &InfraDef)> = self
            .defs
            .iter()
            .filter(|(_, def)| infra_allows(def, tenant))
            .map(|(n, d)| (n.as_str(), d))
            .collect();
        out.sort_by(|a, b| a.0.cmp(b.0));
        out
    }
}

/// Source of the node's infras. The server supplies a file-backed loader over
/// `infras.json`; embedders supply their own. Sync — `Tenant::build` is sync,
/// and reading the source is a fast local operation.
pub trait InfraLoader: Send + Sync {
    fn load(&self) -> Result<InfraSet, RsError>;
}

/// Whether `def` admits `tenant` (empty allowlist ⇒ all tenants).
fn infra_allows(def: &InfraDef, tenant: &str) -> bool {
    def.allowed_tenants.is_empty() || def.allowed_tenants.iter().any(|t| t == tenant)
}

/// Expand a mount's `store`-shaped sub-config: if its `adapter` is
/// `infra:<name>`, resolve the infra and return the **merged** config (the real
/// `builtin:`/`code:` adapter, operator fields overlaid onto the tenant's,
/// infra wins). Anything else passes through untouched (borrowed), so the
/// non-infra path is byte-for-byte identical to before.
///
/// Not gated on the mount's service kind, so the same fn serves both the
/// capability `store` and the spec-store `specStore`. Mismatches (an infra
/// pointing at an adapter not registered for the resolving kind) surface as the
/// usual `unknown_builtin` 400 downstream.
pub fn expand_infra<'a>(
    store: &'a Value,
    infras: &InfraSet,
    tenant: &str,
) -> Result<Cow<'a, Value>, RsError> {
    let Some(adapter) = store.get("adapter").and_then(|a| a.as_str()) else {
        return Ok(Cow::Borrowed(store));
    };
    let Some(infra_name) = adapter.strip_prefix("infra:") else {
        return Ok(Cow::Borrowed(store));
    };

    let def = infras.get(infra_name).ok_or_else(|| {
        RsError::bad_request(format!(
            "unknown infra '{infra_name}' (available: {})",
            infras.names().join(", ")
        ))
    })?;
    if !infra_allows(def, tenant) {
        return Err(RsError::forbidden(format!(
            "infra '{infra_name}' is not available to tenant '{tenant}'"
        )));
    }

    // Start from the tenant's fields (minus the placeholder `adapter`), then
    // overlay the infra's config so the operator's values win.
    let mut merged: Map<String, Value> = store
        .as_object()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|(k, _)| k != "adapter")
        .collect();

    // Reject any tenant-set infra-only field before merging it away.
    let trespass: Vec<&String> =
        def.infra_only.iter().filter(|f| merged.contains_key(f.as_str())).collect();
    if !trespass.is_empty() {
        let names: Vec<&str> = trespass.iter().map(|s| s.as_str()).collect();
        return Err(RsError::bad_request(format!(
            "infra '{infra_name}' forbids setting these adapter fields on the mount: {}",
            names.join(", ")
        )));
    }

    for (k, v) in &def.config {
        merged.insert(k.clone(), v.clone());
    }
    merged.insert("adapter".to_string(), Value::String(def.adapter.clone()));

    // The tenant must have supplied every required gap.
    let missing: Vec<&str> = def
        .requires
        .iter()
        .filter(|f| !merged.contains_key(f.as_str()))
        .map(|s| s.as_str())
        .collect();
    if !missing.is_empty() {
        return Err(RsError::bad_request(format!(
            "infra '{infra_name}' requires these adapter fields to be set on the mount: {}",
            missing.join(", ")
        )));
    }

    Ok(Cow::Owned(Value::Object(merged)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set() -> InfraSet {
        InfraSet::from_json(json!({
            "s3": {
                "adapter": "builtin:mem",
                "description": "shared store",
                "allowedTenants": ["t1"],
                "config": { "bucket": "rs-prod", "secretKey": "shh" },
                "infraOnly": ["tenantDirectories"],
                "requires": ["prefix"]
            },
            "open": {
                "adapter": "builtin:mem",
                "config": { "k": "v" }
            }
        }))
        .unwrap()
    }

    #[test]
    fn passes_non_infra_through_untouched() {
        let store = json!({ "adapter": "builtin:mem", "root": "x" });
        let out = expand_infra(&store, &set(), "t1").unwrap();
        assert!(matches!(out, Cow::Borrowed(_)));
        assert_eq!(*out, store);
    }

    #[test]
    fn no_adapter_passes_through() {
        let store = json!({ "root": "x" });
        let out = expand_infra(&store, &set(), "t1").unwrap();
        assert!(matches!(out, Cow::Borrowed(_)));
    }

    #[test]
    fn merges_infra_winning_and_rewrites_adapter() {
        let store = json!({ "adapter": "infra:s3", "prefix": "p", "bucket": "tenant-tried" });
        let out = expand_infra(&store, &set(), "t1").unwrap();
        assert_eq!(out["adapter"], "builtin:mem");
        assert_eq!(out["bucket"], "rs-prod", "infra wins over tenant");
        assert_eq!(out["secretKey"], "shh");
        assert_eq!(out["prefix"], "p", "tenant gap preserved");
    }

    #[test]
    fn preserves_root() {
        let store = json!({ "adapter": "infra:s3", "prefix": "p", "root": "sub" });
        let out = expand_infra(&store, &set(), "t1").unwrap();
        assert_eq!(out["root"], "sub");
    }

    #[test]
    fn empty_allowlist_admits_all() {
        let store = json!({ "adapter": "infra:open" });
        assert!(expand_infra(&store, &set(), "anyone").is_ok());
    }

    #[test]
    fn allowlist_blocks_other_tenants() {
        let store = json!({ "adapter": "infra:s3", "prefix": "p" });
        let err = expand_infra(&store, &set(), "t2").unwrap_err();
        assert_eq!(err.status, 403);
    }

    #[test]
    fn unknown_infra_errors() {
        let store = json!({ "adapter": "infra:nope" });
        let err = expand_infra(&store, &set(), "t1").unwrap_err();
        assert_eq!(err.status, 400);
        assert!(err.detail.contains("unknown infra"));
    }

    #[test]
    fn missing_required_field_errors() {
        let store = json!({ "adapter": "infra:s3" });
        let err = expand_infra(&store, &set(), "t1").unwrap_err();
        assert_eq!(err.status, 400);
        assert!(err.detail.contains("requires"));
    }

    #[test]
    fn tenant_set_infra_only_field_errors() {
        let store = json!({ "adapter": "infra:s3", "prefix": "p", "tenantDirectories": false });
        let err = expand_infra(&store, &set(), "t1").unwrap_err();
        assert_eq!(err.status, 400);
        assert!(err.detail.contains("forbids"));
    }
}
