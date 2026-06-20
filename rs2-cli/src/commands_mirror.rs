//! `rs2 pull` / `rs2 push` — mirror a tenant's instruction plane into a local
//! `rs2/` directory and push edits back through the validated APIs. See
//! [`crate::mirror`] for the format and the discovery-driven machinery.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::client::Client;
use crate::commands::login_hint;
use crate::config;
use crate::mirror::{
    self, CodeSection, ConfigBaseline, ControlPaths, MirrorState, SpecBaseline, STATE_FILE,
};

/// `rs2 pull` — materialize the instruction plane (config + every spec store +
/// code pins) into the mirror directory, recording baselines for later push.
pub fn pull(host: Option<&str>, dir: Option<&str>) -> Result<(), String> {
    let loaded = config::load()?;
    let host = config::resolve_host(host, &loaded.config)?;
    let token = config::token_if_valid(&loaded.config, &host);
    let client = Client::new(host.clone(), token);

    let disc = mirror::discover(&client)?;
    let control = disc.control.clone().expect("discover() guarantees control");

    // Resolve the mirror directory.
    let dir = match dir {
        Some(d) => PathBuf::from(d),
        None => match mirror::find_mirror(None)? {
            Some(m) => m.dir,
            None => std::env::current_dir()
                .map_err(|e| format!("cannot read current dir: {e}"))?
                .join(mirror::MIRROR_DIR),
        },
    };
    let marker = dir.join(STATE_FILE);

    // Refuse to repoint an existing mirror at a different server/tenant.
    if marker.is_file() {
        let existing = mirror::load_state(&marker)?;
        if existing.host.trim_end_matches('/') != host {
            return Err(format!(
                "this mirror tracks {} — refusing to pull from {host}",
                existing.host
            ));
        }
        if !existing.tenant.is_empty() && existing.tenant != disc.tenant {
            return Err(format!(
                "this mirror tracks tenant '{}' — server is '{}'",
                existing.tenant, disc.tenant
            ));
        }
    }

    std::fs::create_dir_all(&dir).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;

    let mut state = MirrorState {
        version: 1,
        host: host.clone(),
        tenant: disc.tenant.clone(),
        control: ControlPaths { config: control.config.clone(), code: control.code.clone() },
        config: ConfigBaseline::default(),
        specs: BTreeMap::new(),
        code: CodeSection::default(),
    };

    // Config (secrets already redacted by the server as "<secret>").
    let cfg_resp = client.get(&control.config)?;
    if cfg_resp.status != 200 {
        return Err(format!("cannot read tenant config: {}", cfg_resp.error_detail()));
    }
    let cfg_value: Value = serde_json::from_str(&cfg_resp.body)
        .map_err(|e| format!("tenant config was not JSON: {e}"))?;
    write_file(&dir.join("tenant.json"), &mirror::canonical_json(&cfg_resp.body))?;
    state.config.etag = cfg_resp.etag;

    // Specs: remote is the source of truth, so rebuild the tree from scratch
    // (a remotely-deleted spec disappears locally; warn before clobbering).
    let specs_root = dir.join("specs");
    if specs_root.exists() {
        eprintln!("note: overwriting local specs under {} (remote is source of truth — commit first)", specs_root.display());
        std::fs::remove_dir_all(&specs_root)
            .map_err(|e| format!("cannot refresh {}: {e}", specs_root.display()))?;
    }
    let mut spec_count = 0usize;
    let mut store_count = 0usize;
    for svc in &disc.services {
        let Some(subtree) = &svc.spec_subtree else { continue };
        store_count += 1;
        for spec in mirror::walk_store(&client, &svc.path, subtree)? {
            let local_rel = mirror::local_spec_path(&svc.path, subtree, &spec.rel);
            let pretty = mirror::canonical_json(&spec.body);
            write_file(&dir.join(&local_rel), &pretty)?;
            state.specs.insert(
                local_rel,
                SpecBaseline { etag: spec.etag, hash: mirror::content_hash(pretty.as_bytes()) },
            );
            spec_count += 1;
        }
    }

    // Code pins (from the config's `code:` mount references).
    state.code.lock = mirror::code_pins(&cfg_value);
    let lock = serde_json::to_string_pretty(&state.code.lock)
        .map_err(|e| format!("cannot serialize code.lock: {e}"))?;
    write_file(&dir.join("code.lock"), &lock)?;

    write_readme(&dir)?;
    mirror::save_state(&marker, &state)?;

    println!(
        "pulled tenant '{}' → {} ({spec_count} spec(s) across {store_count} store(s), {} code pin(s))",
        disc.tenant,
        dir.display(),
        state.code.lock.len(),
    );
    Ok(())
}

/// `rs2 push` — send local instruction-plane edits back: config via the
/// self-config API (server-enforced `If-Match`), specs via store writes
/// (`If-Match`/`If-None-Match: *`, the `conditional-write` facet). Aborts on a
/// remote change rather than clobbering.
pub fn push(dir: Option<&str>, dry_run: bool, allow_secret_rotation: bool) -> Result<(), String> {
    let located = mirror::find_mirror(dir)?
        .ok_or_else(|| "not in an rs2 mirror — run `rs2 pull` first".to_string())?;
    let dir = located.dir;
    let mut state = located.state;
    let marker = dir.join(STATE_FILE);

    let host = state.host.clone();
    if host.is_empty() {
        return Err("mirror state has no host — re-run `rs2 pull`".to_string());
    }
    let loaded = config::load()?;
    let token = config::token_if_valid(&loaded.config, &host);
    let had_token = token.is_some();
    let client = Client::new(host.clone(), token);

    let disc = mirror::discover(&client)?;
    if !state.tenant.is_empty() && disc.tenant != state.tenant {
        return Err(format!(
            "server tenant '{}' does not match mirror tenant '{}' — wrong server?",
            disc.tenant, state.tenant
        ));
    }
    let control = disc.control.clone().expect("discover() guarantees control");
    if control.config != state.control.config {
        eprintln!(
            "note: control config path moved ({} → {}); using the server's",
            state.control.config, control.config
        );
    }
    let spec_mounts: Vec<(String, String)> = disc
        .services
        .iter()
        .filter_map(|s| s.spec_subtree.clone().map(|st| (s.path.clone(), st)))
        .collect();

    // --- Config diff + secret guard ---
    let tenant_path = dir.join("tenant.json");
    let tenant_text = std::fs::read_to_string(&tenant_path)
        .map_err(|e| format!("cannot read {}: {e}", tenant_path.display()))?;
    let tenant_value: Value = serde_json::from_str(tenant_text.trim_start_matches('\u{feff}'))
        .map_err(|e| format!("tenant.json is not valid JSON: {e}"))?;
    let secrets = mirror::real_secret_locations(&tenant_value);
    if !secrets.is_empty() && !allow_secret_rotation {
        return Err(format!(
            "tenant.json holds real secret value(s) at {} — restore the \"<secret>\" marker, or \
             re-run with --allow-secret-rotation to push them intentionally",
            secrets.join(", ")
        ));
    }
    let cfg_resp = client.get(&control.config)?;
    if cfg_resp.status != 200 {
        return Err(format!(
            "cannot read current config: {}{}",
            cfg_resp.error_detail(),
            login_hint(cfg_resp.status, had_token)
        ));
    }
    let config_changed =
        mirror::canonical_json(&tenant_text) != mirror::canonical_json(&cfg_resp.body);

    // --- Spec diff ---
    let local_specs = collect_local_specs(&dir)?;
    let mut creates: Vec<(String, String, Vec<u8>)> = Vec::new();
    let mut updates: Vec<(String, String, Vec<u8>, Option<String>)> = Vec::new();
    let mut deletes: Vec<(String, String, Option<String>)> = Vec::new();
    let mut seen = HashSet::new();
    for (local_rel, full) in &local_specs {
        seen.insert(local_rel.clone());
        let bytes = std::fs::read(full).map_err(|e| format!("cannot read {}: {e}", full.display()))?;
        let remote = mirror::remote_spec_path(local_rel, &spec_mounts)
            .ok_or_else(|| format!("{local_rel}: no spec-store mount matches this path"))?;
        match state.specs.get(local_rel) {
            Some(base) if base.hash == mirror::content_hash(&bytes) => {}
            Some(base) => updates.push((local_rel.clone(), remote, bytes, base.etag.clone())),
            None => creates.push((local_rel.clone(), remote, bytes)),
        }
    }
    for (local_rel, base) in &state.specs {
        if !seen.contains(local_rel) {
            let remote = mirror::remote_spec_path(local_rel, &spec_mounts)
                .ok_or_else(|| format!("{local_rel}: no spec-store mount matches this path"))?;
            deletes.push((local_rel.clone(), remote, base.etag.clone()));
        }
    }

    // --- Dry run: report and stop ---
    if dry_run {
        println!("config: {}", if config_changed { "CHANGED → PUT" } else { "unchanged" });
        for (_, remote, _) in &creates {
            println!("create  {remote}");
        }
        for (_, remote, _, _) in &updates {
            println!("update  {remote}");
        }
        for (_, remote, _) in &deletes {
            println!("delete  {remote}");
        }
        if !config_changed && creates.is_empty() && updates.is_empty() && deletes.is_empty() {
            println!("(nothing to push)");
        }
        return Ok(());
    }

    if !config_changed && creates.is_empty() && updates.is_empty() && deletes.is_empty() {
        println!("nothing to push — mirror matches the server");
        return Ok(());
    }

    // --- Apply config first (server-enforced If-Match) ---
    if config_changed {
        let etag = state.config.etag.clone().or_else(|| cfg_resp.etag.clone());
        let resp = client.put(&control.config, "application/json", tenant_text.as_bytes(), etag.as_deref())?;
        match resp.status {
            204 => {
                state.config.etag = resp.etag.clone();
                state.code.lock = mirror::code_pins(&tenant_value);
                println!("config updated");
            }
            409 => {
                return Err(
                    "config changed remotely since pull — run `rs2 pull` to reconcile".to_string()
                )
            }
            400 => return Err(format!("config rejected: {}", resp.error_detail())),
            s => {
                return Err(format!(
                    "config update failed: {}{}",
                    resp.error_detail(),
                    login_hint(s, had_token)
                ))
            }
        }
        // Refresh code.lock on disk and persist state before touching specs.
        if let Ok(lock) = serde_json::to_string_pretty(&state.code.lock) {
            let _ = std::fs::write(dir.join("code.lock"), lock);
        }
        mirror::save_state(&marker, &state)?;
    }

    // --- Apply spec creates ---
    for (local_rel, remote, bytes) in &creates {
        let resp = client.put_create(remote, "application/json", bytes)?;
        match resp.status {
            200 | 201 => {
                state.specs.insert(
                    local_rel.clone(),
                    SpecBaseline { etag: resp.etag, hash: mirror::content_hash(bytes) },
                );
                println!("created {remote}");
            }
            412 => {
                return Err(format!(
                    "{remote} already exists on the server — run `rs2 pull` to reconcile"
                ))
            }
            400 => return Err(format!("{remote} rejected: {}", resp.error_detail())),
            s => {
                return Err(format!(
                    "create {remote} failed: {}{}",
                    resp.error_detail(),
                    login_hint(s, had_token)
                ))
            }
        }
        mirror::save_state(&marker, &state)?;
    }

    // --- Apply spec updates (If-Match baseline) ---
    for (local_rel, remote, bytes, base_etag) in &updates {
        let resp = client.put(remote, "application/json", bytes, base_etag.as_deref())?;
        match resp.status {
            200 | 201 => {
                state.specs.insert(
                    local_rel.clone(),
                    SpecBaseline { etag: resp.etag, hash: mirror::content_hash(bytes) },
                );
                println!("updated {remote}");
            }
            412 => {
                return Err(format!(
                    "{remote} changed remotely since pull — run `rs2 pull` to reconcile"
                ))
            }
            400 => return Err(format!("{remote} rejected: {}", resp.error_detail())),
            s => {
                return Err(format!(
                    "update {remote} failed: {}{}",
                    resp.error_detail(),
                    login_hint(s, had_token)
                ))
            }
        }
        mirror::save_state(&marker, &state)?;
    }

    // --- Apply spec deletes ---
    for (local_rel, remote, _base) in &deletes {
        let resp = client.delete(remote)?;
        match resp.status {
            200 | 202 | 204 | 404 => {
                state.specs.remove(local_rel);
                println!("deleted {remote}");
            }
            s => {
                return Err(format!(
                    "delete {remote} failed: {}{}",
                    resp.error_detail(),
                    login_hint(s, had_token)
                ))
            }
        }
        mirror::save_state(&marker, &state)?;
    }

    println!("push complete");
    Ok(())
}

/// Every spec file under `<dir>/specs`, as `(posix-relative-to-dir, full path)`.
fn collect_local_specs(dir: &Path) -> Result<Vec<(String, PathBuf)>, String> {
    let mut out = Vec::new();
    let specs_root = dir.join("specs");
    if specs_root.is_dir() {
        collect_files(&specs_root, dir, &mut out)?;
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn collect_files(d: &Path, base: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<(), String> {
    let entries = std::fs::read_dir(d).map_err(|e| format!("cannot read {}: {e}", d.display()))?;
    for entry in entries {
        let path = entry.map_err(|e| format!("cannot read dir entry: {e}"))?.path();
        if path.is_dir() {
            collect_files(&path, base, out)?;
        } else {
            let rel = path
                .strip_prefix(base)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push((rel, path));
        }
    }
    Ok(())
}

fn write_file(path: &Path, contents: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    }
    std::fs::write(path, contents).map_err(|e| format!("cannot write {}: {e}", path.display()))
}

/// A one-time orientation note dropped into the mirror on first pull.
fn write_readme(dir: &Path) -> Result<(), String> {
    let readme = dir.join("README.md");
    if readme.exists() {
        return Ok(());
    }
    let body = "# RS2 instruction-plane mirror\n\n\
        This directory is a git-able copy of how the tenant behaves:\n\n\
        - `tenant.json` — the tenant config (secrets shown as `\"<secret>\"`).\n\
        - `specs/<mount>/<subtree>/…` — stored pipeline/query/template specs.\n\
        - `code.lock` — pinned custom-code versions (informational).\n\
        - `mirror.json` — sync state (host, tenant, baseline ETags). Don't edit.\n\n\
        Edit these files, then `rs2 push`. For specs with a source/compiled split \
        (a pipeline's `x-source`, a template's `jsxSource`), edit the **source** \
        field — the server regenerates the compiled field on push.\n\n\
        Data and front-end assets are **not** part of the mirror; deploy those \
        separately.\n";
    std::fs::write(&readme, body).map_err(|e| format!("cannot write {}: {e}", readme.display()))
}
