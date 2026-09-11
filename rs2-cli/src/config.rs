//! `rsconfig.json` — persistent CLI state: which server to talk to, login
//! credentials, and the JWT obtained by `rs2 login`. Discovered by walking up
//! from the current directory (like the `rs` CLI's project config), so a
//! `run` script or a repo can carry its own server identity.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// The on-disk `rsconfig.json` shape. All fields optional so a hand-written
/// config can hold just `host`, or `host` + `login`, and `login` fills in the
/// rest.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RsConfig {
    /// Server base URL, e.g. `http://127.0.0.1:3100` (trailing slash trimmed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Stored login credentials (used by `login` when flags are omitted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login: Option<Login>,
    /// The session minted by `login`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,
    /// PEM file of extra certificate authorities to trust when talking to
    /// `host` — so a repo pointed at a server behind a private CA carries that
    /// with its server identity, as it already carries the host and the token.
    #[serde(default, rename = "caFile", skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
    /// Named servers beyond the default one — so a repo can hold a token for
    /// `staging` and `prod` at once and `rs2 sync --from staging --to prod`
    /// can authenticate to both. `rs2 login --server <name>` fills an entry.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub servers: BTreeMap<String, ServerEntry>,
}

/// One named server: the same shape as the top-level default (`host`,
/// optional `login`, the `auth` minted by `login`, optional `caFile`).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ServerEntry {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login: Option<Login>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,
    #[serde(default, rename = "caFile", skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
}

/// A resolved server to talk to: its base URL, a usable token if one is
/// stored, and a label (the server name or the origin) for messages.
#[derive(Debug, Clone)]
pub struct Target {
    pub host: String,
    pub token: Option<String>,
    pub label: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Login {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Auth {
    pub token: String,
    /// Token expiry, unix seconds (the `exp` claim returned by the server).
    pub exp: i64,
    /// The host this token was issued for; a token is only used when it matches
    /// the resolved host.
    pub host: String,
}

/// A loaded config plus the path it should be saved back to (the file it came
/// from, or `./rsconfig.json` if none was found).
pub struct Loaded {
    pub config: RsConfig,
    pub path: PathBuf,
}

const FILE_NAME: &str = "rsconfig.json";

/// Walk up from the current directory looking for a file named `file_name`,
/// returning the first match. The shared discovery rule for repo/project-scoped
/// state (`rsconfig.json`, the mirror marker), so a nested working directory
/// inherits its enclosing repo's identity.
pub fn find_up(file_name: &str) -> Result<Option<PathBuf>, String> {
    let cwd = std::env::current_dir().map_err(|e| format!("cannot read current dir: {e}"))?;
    let mut dir: Option<&Path> = Some(cwd.as_path());
    while let Some(d) = dir {
        let candidate = d.join(file_name);
        if candidate.is_file() {
            return Ok(Some(candidate));
        }
        dir = d.parent();
    }
    Ok(None)
}

/// Walk up from the current directory looking for `rsconfig.json`. Returns the
/// parsed config and where to save it; if none is found, an empty config whose
/// save target is `./rsconfig.json`.
pub fn load() -> Result<Loaded, String> {
    match find_up(FILE_NAME)? {
        Some(candidate) => {
            let text = std::fs::read_to_string(&candidate)
                .map_err(|e| format!("cannot read {}: {e}", candidate.display()))?;
            let config: RsConfig = serde_json::from_str(text.trim_start_matches('\u{feff}'))
                .map_err(|e| format!("{} is not valid JSON: {e}", candidate.display()))?;
            Ok(Loaded {
                config,
                path: candidate,
            })
        }
        None => {
            let cwd =
                std::env::current_dir().map_err(|e| format!("cannot read current dir: {e}"))?;
            Ok(Loaded {
                config: RsConfig::default(),
                path: cwd.join(FILE_NAME),
            })
        }
    }
}

/// Persist a config as pretty JSON to the given path.
pub fn save(path: &Path, config: &RsConfig) -> Result<(), String> {
    let text = serde_json::to_string_pretty(config)
        .map_err(|e| format!("cannot serialize config: {e}"))?;
    std::fs::write(path, text).map_err(|e| format!("cannot write {}: {e}", path.display()))
}

/// The `caFile` from the discovered `rsconfig.json`, resolved against the
/// directory holding it so a repo can carry a relative path. Read at startup,
/// before any subcommand runs, so a missing or malformed config is simply no
/// answer here rather than an error on commands that never open a connection.
pub fn ca_file() -> Option<String> {
    let loaded = load().ok()?;
    let ca_file = loaded.config.ca_file?;
    let path = Path::new(&ca_file);
    if path.is_absolute() {
        return Some(ca_file);
    }
    let base = loaded.path.parent()?;
    Some(base.join(path).to_string_lossy().into_owned())
}

/// Resolve the server host: an explicit flag wins, else the stored `host`.
/// Trailing slashes are trimmed so callers can join `host + "/path"` safely.
pub fn resolve_host(flag: Option<&str>, config: &RsConfig) -> Result<String, String> {
    let host = flag
        .map(str::to_string)
        .or_else(|| config.host.clone())
        .ok_or_else(|| {
            "no server host — pass --host or set \"host\" in rsconfig.json".to_string()
        })?;
    Ok(host.trim_end_matches('/').to_string())
}

/// The scheme + authority of a URL: `http://h:3100/services` → `http://h:3100`.
/// A stored token records the *host* it was issued for, so a command holding a
/// deeper URL (the `services` mount) has to strip back to the origin before
/// asking [`token_if_valid`].
pub fn origin(url: &str) -> String {
    let after_scheme = match url.find("://") {
        Some(i) => i + 3,
        None => return url.trim_end_matches('/').to_string(),
    };
    match url[after_scheme..].find('/') {
        Some(j) => url[..after_scheme + j].to_string(),
        None => url.trim_end_matches('/').to_string(),
    }
}

/// Resolve a usable bearer token for `host`: requires a stored `auth` issued
/// for the same host and not past its `exp`.
/// A usable bearer token for `host`, if one is stored, matches the host, and
/// hasn't expired — otherwise `None`. Commands send it when present and let the
/// server decide; this avoids forcing `rs2 login` against an open mount (e.g.
/// bootstrapping the first `/auth` mount before any admin exists).
pub fn token_if_valid(config: &RsConfig, host: &str) -> Option<String> {
    token_for_origin(config, host)
}

/// A usable token for `host` from anywhere in the config: the top-level
/// `auth`, then every named server's `auth`. The first unexpired match wins,
/// so a token minted with `rs2 login --server prod` also serves `rs2 send`
/// pointed at that host.
pub fn token_for_origin(config: &RsConfig, host: &str) -> Option<String> {
    let host = host.trim_end_matches('/');
    let now = now_secs();
    std::iter::once(config.auth.as_ref())
        .chain(config.servers.values().map(|s| s.auth.as_ref()))
        .flatten()
        .find(|a| a.host.trim_end_matches('/') == host && a.exp > now)
        .map(|a| a.token.clone())
}

/// Resolve a server named on the command line: a key of `servers`, or a bare
/// URL (anything with `://`). A URL is reduced to its origin and its token is
/// looked up by origin across the whole config; an unknown name is an error
/// naming the file to fix.
pub fn resolve_server(spec: &str, loaded: &Loaded) -> Result<Target, String> {
    let config = &loaded.config;
    if let Some(entry) = config.servers.get(spec) {
        let host = entry.host.trim_end_matches('/').to_string();
        let token = entry
            .auth
            .as_ref()
            .filter(|a| a.host.trim_end_matches('/') == host && a.exp > now_secs())
            .map(|a| a.token.clone())
            .or_else(|| token_for_origin(config, &host));
        return Ok(Target {
            host,
            token,
            label: spec.to_string(),
        });
    }
    if spec.contains("://") {
        let host = origin(spec);
        let token = token_for_origin(config, &host);
        return Ok(Target {
            label: host.clone(),
            host,
            token,
        });
    }
    Err(format!(
        "'{spec}' is neither a URL nor a server named under \"servers\" in {}",
        loaded.path.display()
    ))
}

pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_strips_the_path() {
        assert_eq!(
            origin("http://127.0.0.1:3100/services"),
            "http://127.0.0.1:3100"
        );
        assert_eq!(origin("https://a.example/x/y/z"), "https://a.example");
        // Already an origin, with or without a trailing slash.
        assert_eq!(origin("http://127.0.0.1:3100"), "http://127.0.0.1:3100");
        assert_eq!(origin("http://127.0.0.1:3100/"), "http://127.0.0.1:3100");
    }

    /// The deploy path resolves a token by origin, so a `services` URL must
    /// match a token issued for its host — and must not match another node.
    #[test]
    fn token_matches_by_origin() {
        let config = RsConfig {
            host: Some("http://127.0.0.1:3100".to_string()),
            login: None,
            auth: Some(Auth {
                token: "t".to_string(),
                exp: now_secs() + 600,
                host: "http://127.0.0.1:3100".to_string(),
            }),
            ca_file: None,
            servers: BTreeMap::new(),
        };
        let services = "http://127.0.0.1:3100/services";
        assert_eq!(
            token_if_valid(&config, &origin(services)),
            Some("t".to_string())
        );
        // A different node gets nothing, even on the same machine.
        assert_eq!(
            token_if_valid(&config, &origin("http://127.0.0.1:3200/services")),
            None
        );
        // The un-stripped URL would never match — the bug this guards.
        assert_eq!(token_if_valid(&config, services), None);
    }

    #[test]
    fn expired_tokens_are_not_used() {
        let config = RsConfig {
            host: None,
            login: None,
            auth: Some(Auth {
                token: "t".to_string(),
                exp: now_secs() - 1,
                host: "http://h".to_string(),
            }),
            ca_file: None,
            servers: BTreeMap::new(),
        };
        assert_eq!(token_if_valid(&config, "http://h"), None);
    }

    fn loaded(config: RsConfig) -> Loaded {
        Loaded {
            config,
            path: PathBuf::from("rsconfig.json"),
        }
    }

    fn entry(host: &str, token: Option<&str>, exp_delta: i64) -> ServerEntry {
        ServerEntry {
            host: host.to_string(),
            login: None,
            auth: token.map(|t| Auth {
                token: t.to_string(),
                exp: now_secs() + exp_delta,
                host: host.to_string(),
            }),
            ca_file: None,
        }
    }

    /// A pre-`servers` file parses unchanged, and a file with only `servers`
    /// (no default host) parses too.
    #[test]
    fn legacy_and_servers_only_files_parse() {
        let legacy: RsConfig = serde_json::from_str(
            r#"{"host":"http://a","auth":{"token":"t","exp":1,"host":"http://a"}}"#,
        )
        .unwrap();
        assert_eq!(legacy.host.as_deref(), Some("http://a"));
        assert!(legacy.servers.is_empty());
        let named: RsConfig =
            serde_json::from_str(r#"{"servers":{"prod":{"host":"https://p.example/"}}}"#).unwrap();
        assert_eq!(named.host, None);
        assert_eq!(named.servers["prod"].host, "https://p.example/");
        // Round-trip omits an empty map, so legacy files stay legacy.
        let text = serde_json::to_string(&legacy).unwrap();
        assert!(!text.contains("servers"));
    }

    #[test]
    fn resolve_server_by_name_and_by_url() {
        let mut servers = BTreeMap::new();
        servers.insert(
            "prod".to_string(),
            entry("https://p.example/", Some("tp"), 600),
        );
        servers.insert(
            "stale".to_string(),
            entry("https://s.example", Some("ts"), -1),
        );
        let cfg = loaded(RsConfig {
            host: Some("http://127.0.0.1:3100".to_string()),
            login: None,
            auth: Some(Auth {
                token: "tl".to_string(),
                exp: now_secs() + 600,
                host: "http://127.0.0.1:3100".to_string(),
            }),
            ca_file: None,
            servers,
        });
        let prod = resolve_server("prod", &cfg).unwrap();
        assert_eq!(prod.host, "https://p.example");
        assert_eq!(prod.token.as_deref(), Some("tp"));
        assert_eq!(prod.label, "prod");
        // An expired named token is not used.
        assert_eq!(resolve_server("stale", &cfg).unwrap().token, None);
        // A URL finds the token by origin — from the map or the default.
        let by_url = resolve_server("https://p.example/services", &cfg).unwrap();
        assert_eq!(by_url.host, "https://p.example");
        assert_eq!(by_url.token.as_deref(), Some("tp"));
        let local = resolve_server("http://127.0.0.1:3100/", &cfg).unwrap();
        assert_eq!(local.token.as_deref(), Some("tl"));
        // Unknown name, not a URL.
        assert!(resolve_server("nope", &cfg).is_err());
        // The legacy lookup also sees named-server tokens now.
        assert_eq!(
            token_if_valid(&cfg.config, "https://p.example"),
            Some("tp".to_string())
        );
    }
}
