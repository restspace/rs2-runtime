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

    merge_config(&client, had_token, |cfg| {
        let mounts = mounts_mut(cfg)?;
        // Collision guard: refuse if anything already occupies that exact path.
        if mounts
            .iter()
            .any(|m| m.get("path").and_then(|v| v.as_str()) == Some(target_path.as_str()))
        {
            return Err(format!("a mount already exists at {target_path} — nothing changed"));
        }
        mounts.push(mount.clone());
        Ok(true)
    })?;
    println!("added mount at {target_path}");
    Ok(())
}

/// `rs2 service set-access` — set the `access` policy (and optional extra
/// `config` keys via `--set k=v`) on an existing mount. Unlike `service add`
/// this edits a mount in place; used to tighten an open bootstrap mount.
pub fn service_set_access(path: &str, access: &str, sets: &[String]) -> Result<(), String> {
    let loaded = config::load()?;
    let host = config::resolve_host(None, &loaded.config)?;
    let token = config::token_if_valid(&loaded.config, &host);
    let had_token = token.is_some();
    let client = Client::new(host, token);

    let access_val: serde_json::Value =
        serde_json::from_str(access).map_err(|e| format!("--access is not valid JSON: {e}"))?;
    // `--set k=v`: the value is parsed as JSON (so `true`/numbers/objects work),
    // falling back to a bare string when it isn't valid JSON.
    let mut extra: Vec<(String, serde_json::Value)> = Vec::new();
    for kv in sets {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("--set must be key=value, got '{kv}'"))?;
        let val = serde_json::from_str(v).unwrap_or_else(|_| serde_json::Value::String(v.to_string()));
        extra.push((k.to_string(), val));
    }

    let path = path.to_string();
    merge_config(&client, had_token, |cfg| {
        let mounts = mounts_mut(cfg)?;
        set_mount_access(mounts, &path, access_val.clone(), &extra)?;
        Ok(true)
    })?;
    println!("updated access for mount {path}");
    Ok(())
}

/// `rs2 auth enable` — turn on authentication on an open node: ensure a
/// `jwtSecret` (generated once if absent), `operatorRoles`, an `/auth` mount,
/// and a temporarily write-open user-store `data` mount. Idempotent: a present
/// secret is kept (never rotated), present mounts are left untouched.
#[allow(clippy::too_many_arguments)]
pub fn auth_enable(
    host: Option<&str>,
    operator_roles: &str,
    user_dataset: Option<&str>,
    session_minutes: Option<u32>,
    data_mount: Option<&str>,
    show_secret: bool,
) -> Result<(), String> {
    let loaded = config::load()?;
    let host = config::resolve_host(host, &loaded.config)?;
    let token = config::token_if_valid(&loaded.config, &host);
    let had_token = token.is_some();
    let client = Client::new(host, token);

    let user_dataset = user_dataset.unwrap_or("users").to_string();
    let data_mount = data_mount.unwrap_or("/data").trim_end_matches('/').to_string();
    let operator_roles = operator_roles.to_string();
    // Set once, shared across merge_config retries so a 409 re-run reuses the
    // same secret rather than generating a fresh one each attempt.
    let mut generated: Option<String> = None;

    merge_config(&client, had_token, |cfg| {
        let obj = cfg
            .as_object_mut()
            .ok_or_else(|| "config is not a JSON object".to_string())?;

        // auth block.
        let auth = obj
            .entry("auth")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .ok_or_else(|| "config \"auth\" is not an object".to_string())?;
        let has_secret = auth
            .get("jwtSecret")
            .and_then(|v| v.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_secret {
            let secret = generated.get_or_insert_with(generate_jwt_secret).clone();
            auth.insert("jwtSecret".to_string(), serde_json::Value::String(secret));
        }
        auth.entry("userDataset")
            .or_insert_with(|| serde_json::Value::String(user_dataset.clone()));
        if let Some(mins) = session_minutes {
            auth.insert("sessionMinutes".to_string(), serde_json::json!(mins));
        }

        // operatorRoles (top-level).
        obj.insert(
            "operatorRoles".to_string(),
            serde_json::Value::String(operator_roles.clone()),
        );

        // /auth + a temporarily write-open user store. The data mount is opened
        // only for the bootstrap window; `auth init` / the lockdown step tighten
        // its `write` to the operator role afterwards.
        let mounts = obj
            .get_mut("mounts")
            .and_then(|m| m.as_array_mut())
            .ok_or_else(|| "current config has no \"mounts\" array".to_string())?;
        ensure_mount(
            mounts,
            "/auth",
            serde_json::json!({
                "path": "/auth", "service": "auth", "config": { "access": { "invoke": "all" } }
            }),
        );
        ensure_mount(
            mounts,
            &data_mount,
            serde_json::json!({
                "path": data_mount, "service": "data",
                "config": { "access": { "read": "authenticated", "write": "all" } }
            }),
        );
        Ok(true)
    })?;

    match generated {
        Some(secret) if show_secret => println!("auth enabled — generated jwtSecret: {secret}"),
        Some(_) => println!("auth enabled — generated a jwtSecret (re-run with --show-secret to print it)"),
        None => println!("auth enabled — kept the existing jwtSecret"),
    }
    Ok(())
}

/// `rs2 auth create-admin` — seed the first operator user by writing a hashed
/// record straight to the user dataset. Seed-if-absent: skips when the record
/// is already visible. Must run while the data mount is write-open and before
/// any field-authz schema is installed (otherwise setting `roles` is rejected).
pub fn auth_create_admin(
    host: Option<&str>,
    email: &str,
    password: Option<&str>,
    roles: &str,
    data_mount: Option<&str>,
    user_dataset: Option<&str>,
) -> Result<(), String> {
    let loaded = config::load()?;
    let host = config::resolve_host(host, &loaded.config)?;
    let token = config::token_if_valid(&loaded.config, &host);
    let stored_pw = loaded
        .config
        .login
        .as_ref()
        .and_then(|l| l.password.clone());
    let password = resolve_admin_password(password, stored_pw.as_deref())?;
    let data_mount = data_mount.unwrap_or("/data").trim_end_matches('/').to_string();
    let user_dataset = user_dataset.unwrap_or("users");
    let client = Client::new(host, token);

    // The data service uses the literal path segment as the record key (it does
    // not percent-decode), and the auth service looks the user up by the raw
    // email — so the email goes into the path verbatim, matching `send
    // /users/<email>` and the startup seeder's key.
    let record_path = format!("{data_mount}/{user_dataset}/{email}");

    // Seed-if-absent: a visible 200 means it already exists. A 401/403/404 just
    // means we can't see it (locked read, or absent) — fall through and create.
    if client.get(&record_path)?.status == 200 {
        println!("admin '{email}' already exists — skipped");
        return Ok(());
    }

    // Guard against the schema-before-admin inversion: a field-authz schema on
    // the dataset would reject `roles` from a non-operator. Fail with a clear
    // message rather than an opaque 403.
    let schema = client.get(&format!("{data_mount}/{user_dataset}/.schema.json"))?;
    if schema.status == 200 && schema.body.contains("x-rs-write") {
        return Err(format!(
            "the '{user_dataset}' dataset already has a field-authz schema — create the admin \
             before installing the schema"
        ));
    }

    let hash = rs2_core::services::auth::hash_password(&password)
        .map_err(|e| format!("password hashing failed: {e}"))?;
    let record = serde_json::json!({ "passwordHash": hash, "roles": roles, "kind": "user" });
    let resp = client.put(
        &record_path,
        "application/json",
        record.to_string().as_bytes(),
        None,
    )?;
    match resp.status {
        200 | 201 => println!("created admin '{email}' (roles: {roles})"),
        401 | 403 => {
            return Err(format!(
                "could not write {record_path}: {} — the user store is not writable; on a fresh \
                 node run `rs2 auth enable` first (before locking it down)",
                resp.error_detail()
            ))
        }
        _ => return Err(format!("could not create admin: {}", resp.error_detail())),
    }
    Ok(())
}

/// `rs2 auth init` — one-shot bootstrap on a fresh open node: enable auth,
/// create the first admin, log in as them, then lock down the user store and
/// the `/services` mount to the operator role. For a self-hashing user pipeline
/// + schema, drive the granular verbs instead (see examples/seed-auth).
#[allow(clippy::too_many_arguments)]
pub fn auth_init(
    host: Option<&str>,
    admin_email: &str,
    admin_password: Option<&str>,
    operator_roles: &str,
    user_dataset: Option<&str>,
    data_mount: Option<&str>,
    show_secret: bool,
) -> Result<(), String> {
    // Resolve the password once so create-admin and login use the same value.
    let loaded = config::load()?;
    let stored_pw = loaded.config.login.as_ref().and_then(|l| l.password.clone());
    let password = resolve_admin_password(admin_password, stored_pw.as_deref())?;
    let data_mount_path = data_mount.unwrap_or("/data").trim_end_matches('/').to_string();

    // 1. Enable auth (jwtSecret, operatorRoles, /auth + temp-open /data).
    auth_enable(host, operator_roles, user_dataset, None, data_mount, show_secret)?;
    // 2. Create the first admin while /data is write-open and schema-free.
    auth_create_admin(host, admin_email, Some(&password), operator_roles, data_mount, user_dataset)?;
    // 3. Log in as that admin so the lockdown PUT carries operator authority.
    login(host, Some(admin_email), Some(&password))?;

    // 4. Lock down: user store write → operator, /services read+write → operator.
    let loaded = config::load()?;
    let resolved_host = config::resolve_host(host, &loaded.config)?;
    let token = config::token_if_valid(&loaded.config, &resolved_host);
    let had_token = token.is_some();
    let client = Client::new(resolved_host, token);
    let ops = operator_roles.to_string();
    merge_config(&client, had_token, |cfg| {
        let mounts = mounts_mut(cfg)?;
        set_mount_access(
            mounts,
            &data_mount_path,
            serde_json::json!({ "read": "authenticated", "write": ops }),
            &[],
        )?;
        set_mount_access(
            mounts,
            "/services",
            serde_json::json!({ "read": ops, "write": ops }),
            &[],
        )?;
        Ok(true)
    })?;
    println!(
        "auth bootstrap complete — '{admin_email}' can log in; {data_mount_path} and /services are operator-only"
    );
    Ok(())
}

/// Read the live tenant config from `/services/raw` (with its ETag), apply
/// `patch`, and PUT it back under `If-Match`, retrying on a 409 version clash.
/// `patch` returns `Ok(true)` to proceed with the PUT, `Ok(false)` for an
/// idempotent no-op, or `Err` to abort. It re-runs against a freshly read
/// config on each attempt, so write it as "ensure X is present", not "append X".
fn merge_config<F>(client: &Client, had_token: bool, mut patch: F) -> Result<(), String>
where
    F: FnMut(&mut serde_json::Value) -> Result<bool, String>,
{
    const MAX_TRIES: u32 = 3;
    for attempt in 0..MAX_TRIES {
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
        if !patch(&mut cfg)? {
            return Ok(());
        }
        let resp = client.put(
            "/services/raw",
            "application/json",
            cfg.to_string().as_bytes(),
            Some(&etag),
        )?;
        match resp.status {
            204 => return Ok(()),
            409 if attempt + 1 < MAX_TRIES => continue,
            409 => return Err("config changed under us (ETag mismatch) — re-run".to_string()),
            s => {
                return Err(format!(
                    "config update failed: {}{}",
                    resp.error_detail(),
                    login_hint(s, had_token)
                ))
            }
        }
    }
    unreachable!("the loop returns on the final attempt")
}

/// The mutable `mounts` array of a tenant config, or an error if absent.
fn mounts_mut(cfg: &mut serde_json::Value) -> Result<&mut Vec<serde_json::Value>, String> {
    cfg.get_mut("mounts")
        .and_then(|m| m.as_array_mut())
        .ok_or_else(|| "current config has no \"mounts\" array".to_string())
}

/// Append `mount` unless a mount already occupies `path` (keeps `auth enable`
/// idempotent — a present mount, possibly already locked down, is left alone).
fn ensure_mount(mounts: &mut Vec<serde_json::Value>, path: &str, mount: serde_json::Value) {
    let exists = mounts
        .iter()
        .any(|m| m.get("path").and_then(|v| v.as_str()) == Some(path));
    if !exists {
        mounts.push(mount);
    }
}

/// Set `config.access` (and any extra `config.<k> = <v>`) on the mount at `path`.
fn set_mount_access(
    mounts: &mut [serde_json::Value],
    path: &str,
    access: serde_json::Value,
    extra: &[(String, serde_json::Value)],
) -> Result<(), String> {
    let mount = mounts
        .iter_mut()
        .find(|m| m.get("path").and_then(|v| v.as_str()) == Some(path))
        .ok_or_else(|| format!("no mount at {path}"))?;
    let obj = mount
        .as_object_mut()
        .ok_or_else(|| format!("mount {path} is not an object"))?;
    let cfg = obj
        .entry("config")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or_else(|| format!("mount {path} has a non-object config"))?;
    cfg.insert("access".to_string(), access);
    for (k, v) in extra {
        cfg.insert(k.clone(), v.clone());
    }
    Ok(())
}

/// A fresh 256-bit signing secret as a 64-char hex string, from the OS RNG.
fn generate_jwt_secret() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let mut out = String::with_capacity(64);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Resolve an admin password: `--password` flag, else `RS2_ADMIN_PASSWORD`,
/// else `RS2_PASSWORD`, else the stored `login.password` from rsconfig.json.
fn resolve_admin_password(flag: Option<&str>, stored: Option<&str>) -> Result<String, String> {
    flag.map(str::to_string)
        .or_else(|| std::env::var("RS2_ADMIN_PASSWORD").ok())
        .or_else(|| std::env::var("RS2_PASSWORD").ok())
        .or_else(|| stored.map(str::to_string))
        .ok_or_else(|| {
            "no password — pass --password, set RS2_ADMIN_PASSWORD/RS2_PASSWORD, or set \
             login.password in rsconfig.json"
                .to_string()
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn generate_jwt_secret_is_64_hex_chars() {
        let s = generate_jwt_secret();
        assert_eq!(s.len(), 64);
        assert!(s.bytes().all(|b| b.is_ascii_hexdigit()));
        // Practically certain to differ across calls.
        assert_ne!(generate_jwt_secret(), s);
    }

    #[test]
    fn ensure_mount_is_idempotent() {
        let mut mounts = vec![json!({ "path": "/services", "service": "services" })];
        ensure_mount(&mut mounts, "/auth", json!({ "path": "/auth", "service": "auth" }));
        assert_eq!(mounts.len(), 2);
        // A second call for the same path is a no-op (leaves the existing one).
        ensure_mount(&mut mounts, "/auth", json!({ "path": "/auth", "service": "OTHER" }));
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[1]["service"], "auth");
    }

    #[test]
    fn set_mount_access_sets_access_and_extras() {
        let mut mounts = vec![json!({
            "path": "/data", "service": "data",
            "config": { "access": { "write": "all" } }
        })];
        set_mount_access(
            &mut mounts,
            "/data",
            json!({ "read": "authenticated", "write": "A" }),
            &[("enforceSchema".to_string(), json!(true))],
        )
        .unwrap();
        let cfg = &mounts[0]["config"];
        assert_eq!(cfg["access"]["write"], "A");
        assert_eq!(cfg["access"]["read"], "authenticated");
        assert_eq!(cfg["enforceSchema"], true);
    }

    #[test]
    fn set_mount_access_creates_config_when_absent() {
        let mut mounts = vec![json!({ "path": "/services", "service": "services" })];
        set_mount_access(&mut mounts, "/services", json!({ "write": "A" }), &[]).unwrap();
        assert_eq!(mounts[0]["config"]["access"]["write"], "A");
    }

    #[test]
    fn set_mount_access_unknown_path_errors() {
        let mut mounts = vec![json!({ "path": "/services", "service": "services" })];
        assert!(set_mount_access(&mut mounts, "/nope", json!({}), &[]).is_err());
    }

    #[test]
    fn resolve_admin_password_prefers_flag_then_stored() {
        assert_eq!(resolve_admin_password(Some("flagpw"), Some("storedpw")).unwrap(), "flagpw");
        assert_eq!(resolve_admin_password(None, Some("storedpw")).unwrap(), "storedpw");
        // Only error when neither env-var fallback is set in this environment.
        if std::env::var("RS2_ADMIN_PASSWORD").is_err() && std::env::var("RS2_PASSWORD").is_err() {
            assert!(resolve_admin_password(None, None).is_err());
        }
    }
}
