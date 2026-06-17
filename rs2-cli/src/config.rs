//! `rsconfig.json` — persistent CLI state: which server to talk to, login
//! credentials, and the JWT obtained by `rs2 login`. Discovered by walking up
//! from the current directory (like the `rs` CLI's project config), so a
//! `run` script or a repo can carry its own server identity.

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

/// Walk up from the current directory looking for `rsconfig.json`. Returns the
/// parsed config and where to save it; if none is found, an empty config whose
/// save target is `./rsconfig.json`.
pub fn load() -> Result<Loaded, String> {
    let cwd = std::env::current_dir().map_err(|e| format!("cannot read current dir: {e}"))?;
    let mut dir: Option<&Path> = Some(cwd.as_path());
    while let Some(d) = dir {
        let candidate = d.join(FILE_NAME);
        if candidate.is_file() {
            let text = std::fs::read_to_string(&candidate)
                .map_err(|e| format!("cannot read {}: {e}", candidate.display()))?;
            let config: RsConfig = serde_json::from_str(text.trim_start_matches('\u{feff}'))
                .map_err(|e| format!("{} is not valid JSON: {e}", candidate.display()))?;
            return Ok(Loaded { config, path: candidate });
        }
        dir = d.parent();
    }
    Ok(Loaded { config: RsConfig::default(), path: cwd.join(FILE_NAME) })
}

/// Persist a config as pretty JSON to the given path.
pub fn save(path: &Path, config: &RsConfig) -> Result<(), String> {
    let text = serde_json::to_string_pretty(config)
        .map_err(|e| format!("cannot serialize config: {e}"))?;
    std::fs::write(path, text).map_err(|e| format!("cannot write {}: {e}", path.display()))
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

/// Resolve a usable bearer token for `host`: requires a stored `auth` issued
/// for the same host and not past its `exp`.
/// A usable bearer token for `host`, if one is stored, matches the host, and
/// hasn't expired — otherwise `None`. Commands send it when present and let the
/// server decide; this avoids forcing `rs2 login` against an open mount (e.g.
/// bootstrapping the first `/auth` mount before any admin exists).
pub fn token_if_valid(config: &RsConfig, host: &str) -> Option<String> {
    let auth = config.auth.as_ref()?;
    if auth.host.trim_end_matches('/') != host {
        return None;
    }
    if auth.exp <= now_secs() {
        return None;
    }
    Some(auth.token.clone())
}

pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
