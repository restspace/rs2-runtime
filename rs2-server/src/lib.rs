//! rs2-server: the supported v1 packaging of `rs2-core` (PRD 5.1) —
//! a hyper HTTP listener + config loading + adapter wiring + ops endpoints.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use http::{Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpListener;

use rs2_core::message::{Body, MediaType, Message, Provenance};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// Successor to Restspace's `serverConfig.json` (PRD 13), v1 subset.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerConfig {
    #[serde(default = "default_listen")]
    listen: String,
    tenancy: TenancyConfig,
    /// Root directory for the local-fs file store adapter.
    #[serde(default = "default_file_root")]
    file_root: String,
    /// Root directory for the file-backed `data` adapter (`builtin:file`),
    /// kept separate from `fileRoot` so records (which may hold secrets like
    /// password hashes) are never browsable through a `file` mount.
    #[serde(default = "default_data_root")]
    data_root: String,
    /// Directory holding `<tenant>.json` tenant configs.
    #[serde(default = "default_tenants_dir")]
    tenants_dir: String,
    /// Logging (PRD §14): where logs physically go is operator config.
    #[serde(default)]
    logging: LoggingConfig,
    /// Operator allowlist of catalogue/bundle hosts the host may fetch from
    /// (wildcard `*.suffix` patterns). Empty ⇒ external catalogues are off,
    /// regardless of what tenants register. Bounds SSRF.
    #[serde(default)]
    catalogue_hosts: Vec<String>,
    /// Optional admin seeded at startup so a locked-down `services` mount has
    /// an `A`-role principal to manage it (single-tenant). The env vars
    /// `RS2_ADMIN_EMAIL`/`RS2_ADMIN_PASSWORD` override these on-disk values.
    #[serde(default)]
    bootstrap_admin: Option<BootstrapAdmin>,
    /// Operator infras (PRD §9.1): named partial adapter configs (with baked-in
    /// secrets) tenants reference as `infra:<name>`. A missing file ⇒ no infras.
    #[serde(default = "default_infras_path")]
    infras_path: String,
    /// Token gating `POST /admin/reload-infras`. `RS2_ADMIN_TOKEN` overrides
    /// this; with neither set the admin endpoint is disabled (503).
    #[serde(default)]
    admin_token: Option<String>,
}

/// Bootstrap admin credentials (PRD §10.5). Env vars
/// `RS2_ADMIN_EMAIL`/`RS2_ADMIN_PASSWORD` take precedence over these; prefer
/// env for the password so it never lands in the config file at rest.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapAdmin {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

/// Node logging configuration. Absent ⇒ file sink at `./logs`, Info floor.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoggingConfig {
    /// `"file"` (default) writes per-tenant NDJSON; `"none"` disables.
    #[serde(default = "default_log_sink")]
    sink: String,
    #[serde(default)]
    file: FileLogConfig,
    /// Boundary-log severity floor (`debug|info|warn|error`); 5xx always emit.
    #[serde(default = "default_log_level")]
    level: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileLogConfig {
    #[serde(default = "default_log_path")]
    path: String,
    #[serde(default = "default_log_max_bytes")]
    max_bytes: u64,
    #[serde(default = "default_log_backups")]
    backups: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig { sink: default_log_sink(), file: FileLogConfig::default(), level: default_log_level() }
    }
}

impl Default for FileLogConfig {
    fn default() -> Self {
        FileLogConfig {
            path: default_log_path(),
            max_bytes: default_log_max_bytes(),
            backups: default_log_backups(),
        }
    }
}

fn default_listen() -> String {
    "127.0.0.1:3100".to_string()
}
fn default_file_root() -> String {
    "./data".to_string()
}
fn default_data_root() -> String {
    "./data-store".to_string()
}
fn default_tenants_dir() -> String {
    "./tenants".to_string()
}
fn default_infras_path() -> String {
    "./infras.json".to_string()
}
fn default_log_sink() -> String {
    "file".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_path() -> String {
    "./logs".to_string()
}
fn default_log_max_bytes() -> u64 {
    8 * 1024 * 1024
}
fn default_log_backups() -> usize {
    5
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "mode")]
enum TenancyConfig {
    #[serde(rename = "single")]
    Single { tenant: String },
    #[serde(rename = "multi")]
    Multi {
        #[serde(default)]
        domain_map: HashMap<String, String>,
        main_domain: Option<String>,
    },
}

/// File-backed infra source: reads the operator's `infras.json` on demand
/// (so `POST /admin/reload-infras` always picks up edits). A missing file is
/// an empty set — infras are optional; only malformed JSON is an error.
struct FileInfraLoader {
    path: PathBuf,
}

impl rs2_core::infra::InfraLoader for FileInfraLoader {
    fn load(&self) -> Result<rs2_core::infra::InfraSet, RsError> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(rs2_core::infra::InfraSet::default())
            }
            Err(e) => {
                return Err(RsError::internal(format!(
                    "cannot read infras file '{}': {e}",
                    self.path.display()
                )))
            }
        };
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| RsError::bad_request(format!("invalid infras.json: {e}")))?;
        rs2_core::infra::InfraSet::from_json(value)
    }
}

/// File-backed tenant config store: `<tenants_dir>/<tenant>.json`.
struct FileConfigLoader {
    dir: PathBuf,
}

impl FileConfigLoader {
    fn tenant_path(&self, tenant: &str) -> Result<PathBuf, RsError> {
        if tenant.contains(['/', '\\', '.']) {
            return Err(RsError::bad_request("invalid tenant name"));
        }
        Ok(self.dir.join(format!("{tenant}.json")))
    }

    /// Config version for optimistic concurrency: content hash.
    fn version_of(text: &str) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

#[async_trait]
impl ConfigLoader for FileConfigLoader {
    async fn load_tenant(&self, tenant: &str) -> Result<TenantConfig, RsError> {
        let path = self.tenant_path(tenant)?;
        let text = tokio::fs::read_to_string(&path)
            .await
            .map_err(|_| RsError::not_found(format!("unknown tenant '{tenant}'")))?;
        serde_json::from_str(&text)
            .map_err(|e| RsError::internal(format!("tenant config for '{tenant}' is invalid: {e}")))
    }

    /// Enumerate `<dir>/<tenant>.json` files (skip write-then-rename temps) so
    /// the scheduler can discover all tenants of this node.
    async fn list_tenants(&self) -> Result<Vec<String>, RsError> {
        let mut out = Vec::new();
        let mut rd = match tokio::fs::read_dir(&self.dir).await {
            Ok(rd) => rd,
            Err(_) => return Ok(out),
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".json.tmp") {
                continue;
            }
            if let Some(stem) = name.strip_suffix(".json") {
                if !stem.is_empty() {
                    out.push(stem.to_string());
                }
            }
        }
        Ok(out)
    }

    async fn load_raw(&self, tenant: &str) -> Result<(serde_json::Value, String), RsError> {
        let path = self.tenant_path(tenant)?;
        let text = tokio::fs::read_to_string(&path)
            .await
            .map_err(|_| RsError::not_found(format!("unknown tenant '{tenant}'")))?;
        let value = serde_json::from_str(&text)
            .map_err(|e| RsError::internal(format!("tenant config for '{tenant}' is invalid: {e}")))?;
        Ok((value, Self::version_of(&text)))
    }

    async fn save_raw(
        &self,
        tenant: &str,
        config: &serde_json::Value,
        expected_version: Option<&str>,
    ) -> Result<String, RsError> {
        let path = self.tenant_path(tenant)?;
        if let Some(expected) = expected_version {
            let current = tokio::fs::read_to_string(&path).await.unwrap_or_default();
            if Self::version_of(&current) != expected {
                return Err(RsError::conflict(
                    "config version mismatch (If-Match): reload and reapply",
                ));
            }
        }
        let text = serde_json::to_string_pretty(config)
            .map_err(|e| RsError::internal(e.to_string()))?;
        // Write-then-rename so a crash never leaves a torn config.
        let tmp = self.dir.join(format!("{tenant}.json.tmp"));
        tokio::fs::write(&tmp, &text)
            .await
            .map_err(|e| RsError::internal(format!("config write failed: {e}")))?;
        if tokio::fs::rename(&tmp, &path).await.is_err() {
            // Windows rename-over-existing: remove target first.
            let _ = tokio::fs::remove_file(&path).await;
            tokio::fs::rename(&tmp, &path)
                .await
                .map_err(|e| RsError::internal(format!("config rename failed: {e}")))?;
        }
        Ok(Self::version_of(&text))
    }
}

fn hyper_request_to_message(req: hyper::Request<Incoming>, tenant: &str) -> Message {
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let mut msg = Message::request(parts.method, &path_and_query, tenant);
    let media_type = parts
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(MediaType::parse)
        .unwrap_or_else(MediaType::octet_stream);
    let size = parts
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    msg.headers = parts.headers;
    let has_body = size.map(|s| s > 0).unwrap_or(true);
    if has_body && msg.method != http::Method::GET && msg.method != http::Method::HEAD {
        let stream = http_body_util::BodyStream::new(body)
            .filter_map(|frame| async move {
                match frame {
                    Ok(f) => f.into_data().ok().map(Ok),
                    Err(e) => Some(Err(std::io::Error::other(e))),
                }
            })
            .boxed();
        // Ingress bodies are Ephemeral: no replayable source (PRD 6.3).
        msg.body = Some(Body::from_stream(stream, media_type, size, Provenance::Ephemeral));
    }
    msg
}

type OutBody = http_body_util::combinators::UnsyncBoxBody<Bytes, std::io::Error>;

fn message_to_hyper_response(msg: Message) -> Response<OutBody> {
    let status = msg.status.unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(status);
    if let Some(headers) = builder.headers_mut() {
        *headers = msg.headers.clone();
    }
    match msg.body {
        None => builder
            .body(http_body_util::Empty::new().map_err(std::io::Error::other).boxed_unsync())
            .unwrap(),
        Some(body) => {
            let media_type = body.media_type.to_string();
            let size = body.size;
            let stream = body
                .into_stream()
                .map(|chunk| chunk.map(hyper::body::Frame::data));
            let mut resp = builder
                .body(BodyExt::boxed_unsync(StreamBody::new(stream)))
                .unwrap();
            resp.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_str(&media_type)
                    .unwrap_or(http::HeaderValue::from_static("application/octet-stream")),
            );
            if let Some(s) = size {
                resp.headers_mut()
                    .insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(s));
            }
            resp
        }
    }
}

/// A small fixed-body ops response (plain text / JSON), outside tenant routing.
fn ops_response(status: StatusCode, content_type: &'static str, body: String) -> Response<OutBody> {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(http_body_util::Full::new(Bytes::from(body)).map_err(std::io::Error::other).boxed_unsync())
        .unwrap()
}

/// Constant-time equality (no `subtle` dep): compares every byte so timing
/// doesn't leak how much of the token matched. Unequal lengths fail fast — the
/// length isn't the secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// The bearer/token presented for a node admin request: `Authorization: Bearer
/// <t>` or `X-Admin-Token: <t>`.
fn presented_admin_token(req: &hyper::Request<Incoming>) -> Option<String> {
    if let Some(v) = req.headers().get(http::header::AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        if let Some(tok) = v.strip_prefix("Bearer ") {
            return Some(tok.trim().to_string());
        }
    }
    req.headers().get("x-admin-token").and_then(|v| v.to_str().ok()).map(|s| s.to_string())
}

async fn serve_request(
    runtime: Arc<Runtime>,
    admin_token: Option<Arc<str>>,
    req: hyper::Request<Incoming>,
) -> Response<OutBody> {
    let path = req.uri().path();
    // Ops endpoints (PRD 14), outside tenant routing.
    if path == "/healthz" || path == "/readyz" {
        return Response::builder()
            .status(StatusCode::OK)
            .body(http_body_util::Full::new(Bytes::from_static(b"ok")).map_err(std::io::Error::other).boxed_unsync())
            .unwrap();
    }
    // Node admin: reload infras without restarting (PRD §9.1). Operator-gated by
    // the admin token; disabled (503) when no token is configured.
    if path == "/admin/reload-infras" {
        if req.method() != http::Method::POST {
            return ops_response(
                StatusCode::METHOD_NOT_ALLOWED,
                "text/plain",
                "POST only\n".to_string(),
            );
        }
        let Some(expected) = admin_token.as_deref() else {
            return ops_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "text/plain",
                "admin endpoint disabled: set RS2_ADMIN_TOKEN or serverConfig.adminToken\n"
                    .to_string(),
            );
        };
        match presented_admin_token(&req) {
            Some(t) if constant_time_eq(t.as_bytes(), expected.as_bytes()) => {}
            _ => {
                return ops_response(
                    StatusCode::UNAUTHORIZED,
                    "text/plain",
                    "missing or invalid admin token\n".to_string(),
                )
            }
        }
        return match runtime.reload_infras().await {
            Ok(names) => ops_response(
                StatusCode::OK,
                "application/json",
                serde_json::json!({ "loaded": names.len(), "names": names }).to_string(),
            ),
            Err(e) => ops_response(
                StatusCode::from_u16(e.status).unwrap_or(StatusCode::BAD_REQUEST),
                "application/problem+json",
                e.to_problem_json("-", "-").to_string(),
            ),
        };
    }
    let host = req
        .headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Some(tenant) = runtime.resolve_tenant(host) else {
        let problem = RsError::not_found(format!("no tenant for host '{host}'"))
            .to_problem_json("-", "-")
            .to_string();
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(http::header::CONTENT_TYPE, "application/problem+json")
            .body(http_body_util::Full::new(Bytes::from(problem)).map_err(std::io::Error::other).boxed_unsync())
            .unwrap();
    };
    let msg = hyper_request_to_message(req, &tenant);
    let trace_id = msg.trace.trace_id.clone();
    let mut resp = message_to_hyper_response(runtime.handle(msg).await);
    if let Ok(v) = http::HeaderValue::from_str(&trace_id) {
        resp.headers_mut().insert("x-trace-id", v);
    }
    resp
}

/// Seed a single bootstrap admin into the tenant's user dataset at startup
/// (PRD §10.5/§10.6), breaking the config-plane chicken-and-egg: a locked
/// `services` mount needs an `A`-role principal, which needs a user record,
/// which a locked `data` mount won't accept over the wire. We write it
/// straight through the data adapter instead.
///
/// Credentials resolve env-first (`RS2_ADMIN_EMAIL`/`RS2_ADMIN_PASSWORD`),
/// then the `bootstrapAdmin` config block; both absent ⇒ no-op (opt-in).
/// Written through the node's default data store — the same store `auth` reads
/// `userDataset` from — so with the file-backed default the admin (and any
/// runtime change to it) persists across restarts. Seeds only when the record
/// is absent, so a rotated password is never clobbered. Single-tenant only —
/// `run` doesn't enumerate tenants in multi-tenant mode, where each tenant's
/// admin is seeded out of band.
async fn seed_bootstrap_admin(
    tenancy: &Tenancy,
    bootstrap: Option<&BootstrapAdmin>,
    loader: &FileConfigLoader,
    data: &Arc<dyn rs2_core::capabilities::DataStore>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Env wins over config; an empty value counts as unset.
    fn resolve(env: &str, configured: Option<&String>) -> Option<String> {
        std::env::var(env)
            .ok()
            .or_else(|| configured.cloned())
            .filter(|s| !s.is_empty())
    }
    let email = resolve("RS2_ADMIN_EMAIL", bootstrap.and_then(|b| b.email.as_ref()));
    let password = resolve("RS2_ADMIN_PASSWORD", bootstrap.and_then(|b| b.password.as_ref()));

    let (email, password) = match (email, password) {
        (None, None) => return Ok(()), // opted out
        (Some(email), Some(password)) => (email, password),
        _ => {
            return Err("bootstrap admin: set both an email and a password \
                        (RS2_ADMIN_EMAIL/RS2_ADMIN_PASSWORD or the bootstrapAdmin config block), \
                        or neither"
                .into())
        }
    };

    let tenant = match tenancy {
        Tenancy::Single { tenant } => tenant.clone(),
        Tenancy::Multi { .. } => {
            eprintln!(
                "bootstrapAdmin is ignored in multi-tenant mode; seed each tenant's admin out of band"
            );
            return Ok(());
        }
    };

    // The auth service mints tokens from the user record's roles; with no
    // jwtSecret it can't, so seeding would be pointless. Fail loudly.
    let (raw, _) = loader
        .load_raw(&tenant)
        .await
        .map_err(|e| format!("bootstrap admin: cannot load tenant '{tenant}' config: {e}"))?;
    let auth = raw.get("auth");
    let jwt_present = auth
        .and_then(|a| a.get("jwtSecret"))
        .and_then(|v| v.as_str())
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    if !jwt_present {
        return Err(format!(
            "bootstrap admin set but tenant '{tenant}' has no auth.jwtSecret — \
             login can't mint tokens; add one before seeding"
        )
        .into());
    }
    let dataset = auth
        .and_then(|a| a.get("userDataset"))
        .and_then(|v| v.as_str())
        .unwrap_or("users")
        .to_string();

    // Seed-if-absent: never overwrite an existing record.
    if data.get(&tenant, &dataset, &email).await.is_ok() {
        println!("bootstrap admin '{email}' already present in '{dataset}'; leaving as-is");
        return Ok(());
    }
    let record = serde_json::json!({
        "passwordHash": rs2_core::services::auth::hash_password(&password)?,
        "roles": "A",
        "kind": "user",
    });
    data.put(&tenant, &dataset, &email, record).await?;
    println!("seeded bootstrap admin '{email}' (role A) into dataset '{dataset}' of tenant '{tenant}'");
    Ok(())
}

/// Resolve a config-relative path against `base` (the config file's directory),
/// so paths in `serverConfig.json` are interpreted relative to the config file
/// — never the process CWD, which a long-running `rs2 dev` can't control and
/// which silently pointed `tenantsDir` at the wrong place. Absolute paths pass
/// through unchanged.
fn resolve_against(base: &Path, path: &str) -> String {
    let p = Path::new(path);
    if p.is_absolute() {
        path.to_string()
    } else {
        base.join(p).to_string_lossy().into_owned()
    }
}

/// Run the server from a config file path. Also the `rs2 dev` entry point.
pub async fn run(config_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config_text = std::fs::read_to_string(config_path)
        .map_err(|e| format!("cannot read server config '{config_path}': {e}"))?;
    let mut config: ServerConfig = serde_json::from_str(&config_text)
        .map_err(|e| format!("invalid server config '{config_path}': {e}"))?;

    // Anchor relative paths to the config file's directory, not the CWD. We
    // canonicalize the config path (it exists — we just read it) to get an
    // absolute base, then rewrite the path-bearing fields in place.
    let config_abs = std::fs::canonicalize(config_path)
        .map_err(|e| format!("cannot resolve server config path '{config_path}': {e}"))?;
    let base_dir = config_abs.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
    config.tenants_dir = resolve_against(&base_dir, &config.tenants_dir);
    config.file_root = resolve_against(&base_dir, &config.file_root);
    config.data_root = resolve_against(&base_dir, &config.data_root);
    config.logging.file.path = resolve_against(&base_dir, &config.logging.file.path);
    config.infras_path = resolve_against(&base_dir, &config.infras_path);

    let tenancy = match config.tenancy {
        TenancyConfig::Single { tenant } => Tenancy::Single { tenant },
        TenancyConfig::Multi { domain_map, main_domain } => Tenancy::Multi { domain_map, main_domain },
    };
    // Log sink (PRD §14): file store (default) or a no-op. The severity floor
    // governs boundary-log volume; 5xx always emit.
    let log_level = rs2_core::logging::Severity::parse(&config.logging.level)
        .unwrap_or(rs2_core::logging::Severity::Info);
    let log_store: Arc<dyn rs2_core::logging::LogStore> = match config.logging.sink.as_str() {
        "none" => Arc::new(rs2_core::logging::NullLogStore),
        _ => Arc::new(rs2_core::adapters::FileLogStore::new(
            &config.logging.file.path,
            config.logging.file.max_bytes,
            config.logging.file.backups,
        )),
    };

    // The node's default data store is file-backed (durable) over `dataRoot`,
    // kept separate from the file store's `fileRoot` so records — which can
    // hold secrets like password hashes — are never browsable through a `file`
    // mount. Seeded users and runtime writes therefore survive restarts.
    // `builtin:file` selects the same backend explicitly; `builtin:mem` remains
    // available for ephemeral data.
    let data_fs: Arc<dyn rs2_core::capabilities::FileStore> =
        Arc::new(rs2_core::adapters::LocalFsFileStore::new(&config.data_root));
    let mut adapters = Adapters::new(
        Arc::new(rs2_core::adapters::LocalFsFileStore::new(&config.file_root)),
        Arc::new(rs2_core::adapters::FileDataStore::new(data_fs.clone())),
    )
    // Outbound HTTP: granted per mount via `httpOut` allowlists (PRD §9.2).
    .with_http(Arc::new(rs2_core::adapters::UreqHttpOut::new()))
    .with_logging(log_store, log_level)
    // External-catalogue fetching, bounded by the operator host allowlist.
    .with_catalogue(config.catalogue_hosts.clone());
    {
        let data_fs = data_fs.clone();
        adapters.builtins.register_data(
            "file",
            Arc::new(move |_cfg| {
                Ok(Arc::new(rs2_core::adapters::FileDataStore::new(data_fs.clone()))
                    as Arc<dyn rs2_core::capabilities::DataStore>)
            }),
        );
    }
    // Infras (PRD §9.1): seed from `infras.json` and keep the loader so the
    // admin endpoint can hot-reload it. A malformed file fails startup.
    let infra_loader = Arc::new(FileInfraLoader { path: PathBuf::from(&config.infras_path) });
    let infras = rs2_core::infra::InfraLoader::load(infra_loader.as_ref())
        .map_err(|e| format!("loading infras '{}': {}", config.infras_path, e.detail))?;
    if !infras.is_empty() {
        println!("loaded {} infra(s): {}", infras.len(), infras.names().join(", "));
    }
    let adapters = adapters.with_infras(infras).with_infra_loader(infra_loader);

    let loader = Arc::new(FileConfigLoader { dir: PathBuf::from(&config.tenants_dir) });

    // Seed the bootstrap admin (single-tenant) before serving, so a locked
    // `services` mount always has an `A`-role principal to manage it.
    seed_bootstrap_admin(&tenancy, config.bootstrap_admin.as_ref(), &loader, &adapters.data).await?;

    let runtime = Runtime::new(tenancy, adapters, loader, LimitTable::default());

    // Admin token for node ops endpoints (`POST /admin/reload-infras`): env
    // first (preferred — never at rest in the config file), then config. `None`
    // ⇒ the admin endpoint is disabled.
    let admin_token: Option<Arc<str>> = std::env::var("RS2_ADMIN_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
        .or_else(|| config.admin_token.clone())
        .map(|t| Arc::from(t.as_str()));

    // Host scheduler (G1): fires mounts that declare a `schedule`. No-op when
    // none do; single-node by default (swap a shared `ScheduleStore` for HA).
    runtime.spawn_scheduler();

    let addr: SocketAddr = config.listen.parse()?;
    let listener = TcpListener::bind(addr).await?;
    println!("rs2-server listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let runtime = runtime.clone();
        let admin_token = admin_token.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req| {
                let runtime = runtime.clone();
                let admin_token = admin_token.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(
                        serve_request(runtime, admin_token, req).await,
                    )
                }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                eprintln!("connection error: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_eq_matches_only_equal_bytes() {
        assert!(constant_time_eq(b"s3cr3t", b"s3cr3t"));
        assert!(!constant_time_eq(b"s3cr3t", b"s3cr3T"));
        assert!(!constant_time_eq(b"s3cr3t", b"s3cr3")); // length differs
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }
}

