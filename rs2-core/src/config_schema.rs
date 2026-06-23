//! Typed tenant/mount configuration and its JSON Schemas (G1).
//!
//! Config used to be free-form `serde_json::Value` picked apart field by
//! field; the only typed slices were `caching`/`cors`/`retry`/`auth`. This
//! module gives every config slice a Rust type and derives the JSON Schema
//! from that same type via `schemars`, so the schema a config UI consumes
//! (`GET /<services-mount>/catalogue`) cannot drift from what the runtime
//! deserializes.
//!
//! Layout mirrors how config is actually shaped: a mount's `config` object
//! is the **union** of a common envelope ([`MountBaseConfig`] — `access`,
//! `caching`, `retry`) and the service's own fields (the per-service structs
//! below). The catalogue therefore advertises a `baseSchema` (envelope), a
//! `tenantSchema` (top-level `auth`/`cors`/`retry`), and one `configSchema`
//! per service type.
//!
//! `access` is typed here for documentation/forms only; the wrapper still
//! enforces it from the raw value ([`crate::wrapper::check_access`]) because
//! the role-spec strings carry a path-scoping mini-DSL the struct doesn't
//! model.

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

use crate::retry::RetryPolicy;
use crate::services::auth::AuthSettings;
use crate::wrapper::{CachePolicy, CorsPolicy};

/// Render a type's JSON Schema as a JSON value (draft-07, per `schemars`).
pub fn schema_of<T: JsonSchema>() -> Value {
    let root = schemars::gen::SchemaGenerator::default().into_root_schema_for::<T>();
    serde_json::to_value(root).expect("a derived JSON Schema serializes to JSON")
}

/// Schema for a write-only secret string. Referenced via `#[schemars(
/// schema_with = "...")]` on secret fields so a config UI renders them masked
/// and never round-trips the value back: `GET /raw` masks secrets as
/// `"<secret>"`, and a PUT keeps the stored value when it sees that marker.
pub fn secret_string_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let mut obj = <String as JsonSchema>::json_schema(gen).into_object();
    obj.format = Some("password".to_string());
    obj.metadata().write_only = true;
    schemars::schema::Schema::Object(obj)
}

/// Schema for a field that holds a free-form JSON Schema value (e.g. a
/// wrapper's `inputSchema`/`outputSchema`). A bare `serde_json::Value` derives
/// an untyped schema; this emits `{ "type": "object", "editor": "json" }` so a
/// config UI knows to render a JSON editor. Referenced via `#[schemars(
/// schema_with = "json_object_schema")]`.
pub fn json_object_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let mut obj = schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::Object.into()),
        ..Default::default()
    };
    obj.extensions.insert("editor".to_string(), serde_json::json!("json"));
    schemars::schema::Schema::Object(obj)
}

/// Schema for a wrapper's inline `pipeline`: a JSON value edited as raw JSON,
/// but either an object (the typed spec) or an array (the string DSL) —
/// `{ "type": ["object", "array"], "editor": "json" }`.
pub fn pipeline_value_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    use schemars::schema::{InstanceType, SingleOrVec};
    let mut obj = schemars::schema::SchemaObject {
        instance_type: Some(SingleOrVec::Vec(vec![InstanceType::Object, InstanceType::Array])),
        ..Default::default()
    };
    obj.extensions.insert("editor".to_string(), serde_json::json!("json"));
    schemars::schema::Schema::Object(obj)
}

/// Per-mount access control (the `access` field). Either a string shorthand
/// (`"open"` / `"authenticated"`) or an object of space-separated role specs.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum AccessControl {
    /// `"open"` (anonymous) or `"authenticated"` (any valid principal).
    Shorthand(AccessShorthand),
    /// Per-verb role specs.
    Roles(RoleAccess),
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AccessShorthand {
    Open,
    Authenticated,
}

/// Space-separated role-spec strings per action. A token may be followed
/// by a path pattern (`"U /user/{email}"`) scoping that role. Actions map to
/// HTTP methods (PRD §5.2): `read` = GET/HEAD/OPTIONS, `write` = PUT/PATCH,
/// `delete` = DELETE, `invoke` = POST (and other non-idempotent verbs).
/// `delete` and `invoke` default to `write`; `read` defaults to `"all"`,
/// `write` to `"A"`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct RoleAccess {
    pub read: Option<String>,
    pub write: Option<String>,
    pub delete: Option<String>,
    pub invoke: Option<String>,
}

/// The common mount-config envelope every service shares (the `baseSchema`).
/// A mount's full `config` is this merged with the service's own fields.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct MountBaseConfig {
    /// Access control for this mount (default-deny writes, open reads).
    pub access: Option<AccessControl>,
    /// Host-applied response caching (default `no-store`).
    pub caching: Option<CachePolicy>,
    /// Mount-level retry override in the resolution chain (PRD §7.3).
    pub retry: Option<RetryPolicy>,
    /// Periodic host-driven invocation: the runtime fires a synthetic internal
    /// `POST` (header `x-rs2-trigger: schedule`) at this mount on cadence.
    pub schedule: Option<ScheduleConfig>,
    /// Names of tenant `secrets` this mount may use (default-deny). A `pipeline`
    /// mount binds each as a `$<name>` variable for inline HMAC verification
    /// (`$hmacVerify`); the secret value is never exposed on `GET /raw`.
    pub secrets: Option<Vec<String>>,
}

/// A mount schedule: exactly one of `every` (interval) or `cron` (5-field, UTC).
/// The "exactly one" rule is enforced when parsed (`scheduler::Schedule`), so a
/// bad value is a 400 at `PUT /raw`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct ScheduleConfig {
    /// Fixed interval, e.g. `"500ms"`, `"30s"`, `"5m"`, `"2h"`.
    pub every: Option<String>,
    /// 5-field cron (minute hour day-of-month month day-of-week), UTC,
    /// e.g. `"0 9 * * *"`.
    pub cron: Option<String>,
}

/// A registered external catalogue: a name and the URL serving its document.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogueRef {
    pub name: String,
    /// Absolute http(s) URL serving the catalogue document.
    pub url: String,
}

/// Top-level tenant settings outside the mount list (the `tenantSchema`).
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct TenantSettings {
    pub auth: Option<AuthSettings>,
    pub cors: Option<CorsPolicy>,
    pub retry: Option<RetryPolicy>,
    /// External catalogues to browse/install services and adapters from.
    pub catalogues: Option<Vec<CatalogueRef>>,
}

// ---- per-service config -------------------------------------------------

/// `file` service config (beyond the base envelope).
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct FileConfig {
    /// Resource served for directory GETs (static-site mode), e.g.
    /// `index.html`. Defaults to `index.html` when `spaFallback` is set.
    pub default_resource: Option<String>,
    /// Extension-less misses serve the root default resource (SPA routing).
    pub spa_fallback: bool,
    /// Whether directory GETs return `dir+json` listings (default `true`).
    pub listings: bool,
    /// Serve files at extension-less "friendly" URLs (e.g. `/docs/readme`
    /// resolves `docs/readme.md`). Exact matches still win; on a stem
    /// collision the variant is chosen by `Accept` negotiation. Default `false`.
    pub friendly_urls: bool,
    /// Extension preference (no dots, e.g. `["html", "md"]`) for friendly URLs.
    /// Its first entry is the canonical slot an extension-less PUT pins to (so a
    /// no-`Accept` GET of the slug always returns that write); the list also
    /// fronts the GET probe order. Required for extension-less PUTs when
    /// `friendlyUrls` is on (empty → such a PUT is a 400).
    pub extension_priority: Vec<String>,
    /// Backend selection (`store.adapter`); see [`StoreConfig`].
    pub store: Option<StoreConfig>,
}

impl Default for FileConfig {
    fn default() -> Self {
        FileConfig {
            default_resource: None,
            spa_fallback: false,
            listings: true,
            friendly_urls: false,
            extension_priority: Vec::new(),
            store: None,
        }
    }
}

/// `data` service config (beyond the base envelope).
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct DataConfig {
    /// Reject writes that fail the dataset's `.schema.json` (default `false`).
    pub enforce_schema: bool,
    /// Honor per-field access rules in the dataset schema (`x-rs-read` /
    /// `x-rs-write` on a property): reads redact fields the caller can't read,
    /// writes that change a field the caller can't write return a 4xx, and
    /// editing the schema itself requires an operator (PRD §5.2). Default
    /// `false`.
    pub field_level_authz: bool,
    /// Backend selection (`store.adapter`); see [`StoreConfig`].
    pub store: Option<StoreConfig>,
}

/// Storage-root override shared by spec stores, plus per-mount backend
/// selection.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct StoreConfig {
    /// Storage root; defaults to `.rs2-<kind><mount>`.
    pub root: Option<String>,
    /// Backend selection: `builtin:<name>` for a built-in Rust adapter (e.g.
    /// `builtin:mem`) or `code:<name>@<version>` for a deployed JS loadable
    /// adapter. Absent ⇒ the node default for this service kind.
    pub adapter: Option<String>,
}

/// `pipeline` service config (beyond the base envelope).
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct PipelineConfig {
    pub store: Option<StoreConfig>,
    /// Where the authoring specs themselves are physically stored
    /// (`specStore.adapter` selects an `infra:`/`builtin:`/`code:` file
    /// backend, `specStore.root` the root). Absent ⇒ the node file store.
    pub spec_store: Option<StoreConfig>,
    pub retry: Option<RetryPolicy>,
}

/// `query` service config (beyond the base envelope).
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct QueryConfig {
    /// Query *execution* backend (`store.adapter`); `store.root` relocates the
    /// authoring subtree. See [`StoreConfig`].
    pub store: Option<StoreConfig>,
    /// Where the authoring specs are physically stored — independent of the
    /// execution backend in `store`. See [`PipelineConfig::spec_store`].
    pub spec_store: Option<StoreConfig>,
}

/// `wrapper` service config: one inline pipeline fronting another mount.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct WrapperConfig {
    /// The inline pipeline spec (typed object or string-DSL array) run for
    /// every verb and sub-path. A step forwards the exact remaining path with
    /// `${url.rest}` (e.g. `/wrapped${url.rest}`). Advertised as a JSON editor
    /// over an object (typed spec) or array (string DSL).
    #[schemars(schema_with = "pipeline_value_schema")]
    pub pipeline: Option<Value>,
    /// Discovery pattern this mount advertises — it fronts a mount of that
    /// shape (e.g. `"store"`). One of store / store-transform / store-view /
    /// view / api. Defaults to `store-transform`.
    pub pattern: Option<String>,
    /// Optional discovery facets advertised alongside the pattern.
    pub facets: Option<Vec<String>>,
    /// JSON Schema for the request body the wrapper accepts. **Enforced**: a
    /// body that fails it is rejected with 422 before the pipeline runs (only
    /// on verbs that carry a body — PUT/POST/PATCH). Surfaced in discovery,
    /// OpenAPI (`requestBody`), and the agent surface.
    #[schemars(schema_with = "json_object_schema")]
    pub input_schema: Option<Value>,
    /// JSON Schema describing the wrapper's success response. **Advisory** —
    /// surfaced in discovery, OpenAPI (`200`), and the agent surface, but not
    /// enforced at runtime (a facade's responses vary: a keyed record vs a
    /// store listing, and field-authz may redact fields).
    #[schemars(schema_with = "json_object_schema")]
    pub output_schema: Option<Value>,
    /// Role an `elevate` step adds to sub-calls (operator-controlled).
    pub elevate: Option<String>,
    pub retry: Option<RetryPolicy>,
}

/// `template` service config (beyond the base envelope).
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct TemplateConfig {
    pub store: Option<StoreConfig>,
    /// Where the authoring specs are physically stored. See
    /// [`PipelineConfig::spec_store`].
    pub spec_store: Option<StoreConfig>,
}

/// `services` self-config API: no fields beyond the base envelope.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServicesConfig {}

/// The control-surface catalogue (`GET /<services-mount>/catalogue`):
/// available service types with full config schemas, plus the shared
/// `baseSchema` (mount envelope) and `tenantSchema` (top-level settings).
pub fn catalogue() -> Value {
    serde_json::json!({
        "baseSchema": schema_of::<MountBaseConfig>(),
        "tenantSchema": schema_of::<TenantSettings>(),
        "services": [
            { "name": "file",
              "description": "Streamed file storage (PRD §10.1)",
              "configSchema": schema_of::<FileConfig>() },
            { "name": "data",
              "description": "Schema-validated JSON store (PRD §10.2)",
              "configSchema": schema_of::<DataConfig>() },
            { "name": "pipeline",
              "description": "Pipeline store (PRD §10.3): PUT spec envelopes to /<mount>/.pipelines/<name> (.root governs the mount root); every other path on any verb executes the longest-prefix match",
              "configSchema": schema_of::<PipelineConfig>() },
            { "name": "query",
              "description": "Stored parameterized queries (PRD §10.4): PUT envelopes {language?, query, params?, output?} to /<mount>/.queries/<name>; any verb elsewhere executes the longest-prefix match",
              "configSchema": schema_of::<QueryConfig>() },
            { "name": "wrapper",
              "description": "One inline pipeline fronting another mount: runs config.pipeline for every verb/sub-path. Declares its discovery pattern/facets and forwards the exact remaining path via ${url.rest}",
              "configSchema": schema_of::<WrapperConfig>() },
            { "name": "template",
              "description": "JSX templates rendered to HTML: PUT compiled bundles (from `rs2 template build`) to /<mount>/.templates/<name> (.root governs the mount root); any verb elsewhere renders the longest-prefix match with the request body as props",
              "configSchema": schema_of::<TemplateConfig>() },
            { "name": "auth",
              "description": "Authentication & RBAC (PRD §10.5)",
              "configSchema": schema_of::<AuthSettings>() },
            { "name": "services",
              "description": "Self-configuration API (PRD §10.6)",
              "configSchema": schema_of::<ServicesConfig>() }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every advertised schema must be a valid JSON Schema the runtime's own
    /// validator can compile — the cheap guard against malformed output.
    #[test]
    fn catalogue_schemas_compile() {
        let cat = catalogue();
        jsonschema::validator_for(&cat["baseSchema"]).expect("baseSchema compiles");
        jsonschema::validator_for(&cat["tenantSchema"]).expect("tenantSchema compiles");
        for svc in cat["services"].as_array().unwrap() {
            let name = svc["name"].as_str().unwrap();
            jsonschema::validator_for(&svc["configSchema"])
                .unwrap_or_else(|e| panic!("{name} configSchema compiles: {e}"));
        }
    }

    /// The wrapper's free-form schema fields advertise `{type:object,
    /// editor:json}` (a JSON-editor hint) rather than an untyped schema.
    #[test]
    fn wrapper_schema_fields_carry_json_editor_hint() {
        let schema = schema_of::<WrapperConfig>();
        let props = &schema["properties"];
        // inputSchema/outputSchema are always objects.
        for field in ["inputSchema", "outputSchema"] {
            assert_eq!(props[field]["type"], "object", "{field} type: {}", props[field]);
            assert_eq!(props[field]["editor"], "json", "{field} editor: {}", props[field]);
        }
        // pipeline is an object (typed spec) or an array (string DSL).
        assert_eq!(props["pipeline"]["editor"], "json", "{}", props["pipeline"]);
        assert_eq!(
            props["pipeline"]["type"],
            serde_json::json!(["object", "array"]),
            "{}",
            props["pipeline"]
        );
    }

    /// A schema and its deserializer come from the same type, so a config
    /// that deserializes must also validate against the advertised schema
    /// (and vice-versa) — the anti-drift round trip.
    #[test]
    fn typed_config_round_trips_against_its_schema() {
        let file_schema = schema_of::<FileConfig>();
        let validator = jsonschema::validator_for(&file_schema).unwrap();

        let sample = serde_json::json!({ "defaultResource": "index.html", "spaFallback": true });
        assert!(validator.is_valid(&sample));
        let typed: FileConfig = serde_json::from_value(sample).unwrap();
        assert_eq!(typed.default_resource.as_deref(), Some("index.html"));
        assert!(typed.spa_fallback);

        // The base envelope accepts both access shorthands and role objects.
        let base_schema = schema_of::<MountBaseConfig>();
        let base = jsonschema::validator_for(&base_schema).unwrap();
        assert!(base.is_valid(&serde_json::json!({ "access": "open" })));
        assert!(base.is_valid(&serde_json::json!({
            "access": { "read": "all", "write": "A", "delete": "A" }
        })));
        // Stale v1 keys (and any typo) fail loudly rather than silently
        // deserializing to an all-`None` (open) role object.
        assert!(!base.is_valid(&serde_json::json!({ "access": { "readRoles": "all" } })));
        assert!(!base.is_valid(&serde_json::json!({ "access": { "manageRoles": "A" } })));
        serde_json::from_value::<MountBaseConfig>(
            serde_json::json!({ "access": { "write": "A E", "invoke": "A" }, "caching": { "mode": "cache", "maxAgeSeconds": 60 } }),
        )
        .expect("envelope deserializes");
    }

    /// The signing key is advertised as a write-only secret so a config UI
    /// renders it masked and never expects it back from `GET /raw`.
    #[test]
    fn jwt_secret_is_marked_write_only() {
        let auth = schema_of::<AuthSettings>();
        let jwt = &auth["properties"]["jwtSecret"];
        assert_eq!(jwt["writeOnly"], serde_json::json!(true), "schema: {auth:#}");
        assert_eq!(jwt["format"], serde_json::json!("password"));
    }
}
