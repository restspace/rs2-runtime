//! Server-facing admin commands: `login`, `send`, `service add`. Each loads
//! `rsconfig.json`, resolves the host (and, where needed, a bearer token), and
//! drives the corresponding HTTP API.

use crate::client::Client;
use crate::config;

/// `rs2 login` — authenticate against `{host}/auth/login` and persist the
/// returned token into `rsconfig.json`.
pub fn login(
    host: Option<&str>,
    email: Option<&str>,
    password: Option<&str>,
) -> Result<(), String> {
    let mut loaded = config::load()?;
    let host = config::resolve_host(host, &loaded.config)?;

    let stored_login = loaded.config.login.clone().unwrap_or_default();
    let email = email
        .map(str::to_string)
        .or(stored_login.email)
        .ok_or_else(|| "no email — pass --email or set login.email in rsconfig.json".to_string())?;
    let password = password
        .map(str::to_string)
        .or_else(|| std::env::var("RS2_PASSWORD").ok())
        .or(stored_login.password)
        .ok_or_else(|| {
            "no password — pass --password, set RS2_PASSWORD, or set login.password in rsconfig.json"
                .to_string()
        })?;

    let client = Client::new(host.clone(), None);
    let body = serde_json::json!({ "email": email, "password": password });
    let resp = client.post_json("/auth/login", &body)?;
    if resp.status != 200 {
        return Err(format!("login failed: {}", resp.error_detail()));
    }

    let json: serde_json::Value =
        serde_json::from_str(&resp.body).map_err(|e| format!("login response was not JSON: {e}"))?;
    let token = json
        .get("token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "login response had no 'token'".to_string())?
        .to_string();
    let exp = json.get("exp").and_then(|v| v.as_i64()).unwrap_or(0);

    loaded.config.host = Some(host.clone());
    loaded.config.auth = Some(config::Auth { token, exp, host });
    config::save(&loaded.path, &loaded.config)?;

    let mins = ((exp - config::now_secs()).max(0)) / 60;
    println!("logged in — token saved to {} (expires in ~{mins} min)", loaded.path.display());
    Ok(())
}

/// `rs2 send` — PUT a local file to a server path.
pub fn send(server_path: &str, file: &str, content_type: Option<&str>) -> Result<(), String> {
    let loaded = config::load()?;
    let host = config::resolve_host(None, &loaded.config)?;
    let token = config::token_if_valid(&loaded.config, &host);

    let bytes = std::fs::read(file).map_err(|e| format!("cannot read {file}: {e}"))?;
    let content_type = content_type.map(str::to_string).unwrap_or_else(|| {
        mime_guess::from_path(file).first_raw().unwrap_or("application/octet-stream").to_string()
    });

    let had_token = token.is_some();
    let client = Client::new(host, token);
    let resp = client.put(server_path, &content_type, &bytes, None)?;
    match resp.status {
        201 => println!("created {server_path} ({} bytes, {content_type})", bytes.len()),
        200 => println!("overwritten {server_path} ({} bytes, {content_type})", bytes.len()),
        _ => return Err(format!("send failed: {}{}", resp.error_detail(), login_hint(resp.status, had_token))),
    }
    Ok(())
}

/// `rs2 service add` — read a mount spec from a JSON file and append it to the
/// tenant config via the self-config API, failing if a mount already occupies
/// the target path.
pub fn service_add(file: &str, path_override: Option<&str>) -> Result<(), String> {
    let loaded = config::load()?;
    let host = config::resolve_host(None, &loaded.config)?;
    let token = config::token_if_valid(&loaded.config, &host);
    let had_token = token.is_some();
    let client = Client::new(host, token);

    // The mount spec from the local file.
    let mount_text = std::fs::read_to_string(file).map_err(|e| format!("cannot read {file}: {e}"))?;
    let mut mount: serde_json::Value = serde_json::from_str(mount_text.trim_start_matches('\u{feff}'))
        .map_err(|e| format!("{file} is not valid JSON: {e}"))?;
    if !mount.is_object() {
        return Err(format!("{file} must be a JSON object (a mount spec)"));
    }
    if mount.get("service").and_then(|v| v.as_str()).is_none() {
        return Err(format!("{file} is missing the required string \"service\""));
    }

    // Resolve the mount path: --path wins, else the file's own "path".
    let target_path = match path_override {
        Some(p) => p.to_string(),
        None => mount
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                format!("no mount path — pass --path or set \"path\" in {file}")
            })?
            .to_string(),
    };
    mount["path"] = serde_json::Value::String(target_path.clone());

    // Read the current config (with its ETag for optimistic concurrency).
    let current = client.get("/services/raw")?;
    if current.status != 200 {
        return Err(format!(
            "could not read current config: {}{}",
            current.error_detail(),
            login_hint(current.status, had_token)
        ));
    }
    let etag = current
        .etag
        .ok_or_else(|| "self-config response had no ETag".to_string())?;
    let mut cfg: serde_json::Value = serde_json::from_str(&current.body)
        .map_err(|e| format!("self-config response was not JSON: {e}"))?;

    let mounts = cfg
        .get_mut("mounts")
        .and_then(|m| m.as_array_mut())
        .ok_or_else(|| "current config has no \"mounts\" array".to_string())?;

    // Collision guard: refuse if anything already occupies that exact path.
    if mounts
        .iter()
        .any(|m| m.get("path").and_then(|v| v.as_str()) == Some(target_path.as_str()))
    {
        return Err(format!("a mount already exists at {target_path} — nothing changed"));
    }
    mounts.push(mount);

    let resp = client.put("/services/raw", "application/json", cfg.to_string().as_bytes(), Some(&etag))?;
    match resp.status {
        204 => println!("added mount at {target_path}"),
        409 => return Err("config changed under us (ETag mismatch) — re-run".to_string()),
        _ => return Err(format!("service add failed: {}{}", resp.error_detail(), login_hint(resp.status, had_token))),
    }
    Ok(())
}

/// Appended to an auth-ish failure when no token was sent, nudging the user to
/// authenticate (the server enforces access; the CLI just hints).
fn login_hint(status: u16, had_token: bool) -> String {
    if !had_token && (status == 401 || status == 403) {
        " — you may need to `rs2 login` first".to_string()
    } else {
        String::new()
    }
}
