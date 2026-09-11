//! `rs2 sync` — copy a tenant, or named mounts of it, from one node to
//! another: code bundles first (so `code:` pins resolve), then the config
//! through the validated self-config API, then spec stores, then the data
//! plane. Source wins; target-only things survive unless `--prune`. The
//! decisions live in [`crate::sync`]; this module drives the HTTP.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::client::{BytesResponse, Client};
use crate::commands::{login_hint, merge_config_at};
use crate::config;
use crate::mirror::{self, ControlBlock, Discovery, ServiceEntry};
use crate::sync::{self, Action, Scope};

/// Command-line options, as parsed by `main`.
pub struct Options<'a> {
    pub from: &'a str,
    pub to: &'a str,
    pub mounts: &'a [String],
    pub no_data: bool,
    pub data_only: bool,
    pub prune: bool,
    pub dry_run: bool,
}

/// One node in the transfer.
struct Side {
    label: String,
    client: Client,
    had_token: bool,
    disc: Discovery,
    control: ControlBlock,
    config: Value,
}

impl Side {
    fn connect(spec: &str, loaded: &config::Loaded, role: &str) -> Result<Self, String> {
        let target = config::resolve_server(spec, loaded)?;
        let had_token = target.token.is_some();
        let client = Client::new(target.host.clone(), target.token);
        let disc = mirror::discover(&client).map_err(|e| {
            format!(
                "{role} {}: {e}{}",
                target.label,
                if had_token {
                    String::new()
                } else {
                    format!(" (try `rs2 login --server {}`)", target.label)
                }
            )
        })?;
        let control = disc.control.clone().expect("discover() guarantees control");
        let resp = client.get(&control.config)?;
        if resp.status != 200 {
            return Err(format!(
                "{role} {}: cannot read tenant config: {}{}",
                target.label,
                resp.error_detail(),
                login_hint(resp.status, had_token)
            ));
        }
        let config: Value = serde_json::from_str(&resp.body)
            .map_err(|e| format!("{role} {}: tenant config was not JSON: {e}", target.label))?;
        Ok(Self {
            label: target.label,
            client,
            had_token,
            disc,
            control,
            config,
        })
    }

    fn mount_paths(&self) -> Vec<String> {
        self.disc.services.iter().map(|s| s.path.clone()).collect()
    }
}

/// Running totals for the summary line.
#[derive(Default)]
struct Tally {
    created: usize,
    updated: usize,
    deleted: usize,
    skipped: usize,
}

impl Tally {
    fn note(&mut self, action: Action) {
        match action {
            Action::Create => self.created += 1,
            Action::Update => self.updated += 1,
            Action::Skip => self.skipped += 1,
        }
    }
    fn summary(&self) -> String {
        format!(
            "{} created, {} updated, {} deleted, {} unchanged",
            self.created, self.updated, self.deleted, self.skipped
        )
    }
}

pub fn sync(opts: Options) -> Result<(), String> {
    if opts.no_data && opts.data_only {
        return Err("--no-data and --data-only exclude each other".to_string());
    }
    let loaded = config::load()?;
    let from = Side::connect(opts.from, &loaded, "source")?;
    let to = Side::connect(opts.to, &loaded, "target")?;
    if config::origin(&from.client.host()) == config::origin(&to.client.host()) {
        return Err(format!(
            "--from and --to are the same server ({})",
            from.client.host()
        ));
    }
    if from.disc.tenant != to.disc.tenant {
        eprintln!(
            "note: tenant '{}' on {} → tenant '{}' on {}",
            from.disc.tenant, from.label, to.disc.tenant, to.label
        );
    }
    let scope = Scope {
        mounts: (!opts.mounts.is_empty()).then_some(opts.mounts),
        prune: opts.prune,
    };
    let dry = opts.dry_run;
    let prefix = if dry { "would " } else { "" };

    // --- Plan the config (also validates --mount paths) ---
    let mut plan = sync::plan_config(&from.config, &to.config, &scope)?;
    if opts.data_only {
        for path in opts.mounts {
            let src = mount_entry(&from.disc, path);
            let tgt = mount_entry(&to.disc, path);
            match (src, tgt) {
                (Some(s), Some(t)) if s.service == t.service => {}
                (Some(_), Some(t)) => {
                    return Err(format!(
                        "--data-only: {path} is a '{}' mount on {} — sync the config first",
                        t.service, to.label
                    ))
                }
                _ => {
                    return Err(format!(
                        "--data-only: {path} is not mounted on {} — sync the config first",
                        to.label
                    ))
                }
            }
        }
    } else if !plan.missing_secrets.is_empty() {
        for p in &plan.missing_secrets {
            println!("secret  MISSING {p}");
        }
        let msg = format!(
            "{} has no stored value for {} — set it there first (`rs2 auth enable` for \
             /auth/jwtSecret; a real value via PUT {} for /secrets/…)",
            to.label,
            plan.missing_secrets.join(", "),
            to.control.config
        );
        if !dry {
            return Err(msg);
        }
        eprintln!("note: {msg}");
    }
    if !opts.data_only {
        let source_roles = from.config.get("operatorRoles");
        if scope.mounts.is_none() && source_roles != to.config.get("operatorRoles") {
            eprintln!(
                "warning: operatorRoles will change on {} — your token may lose operator access",
                to.label
            );
        }
    }

    let mut code_tally = Tally::default();
    let mut config_changed = false;
    let mut spec_tally = Tally::default();
    let mut data_tally = Tally::default();

    // --- Code bundles, before the config that pins them ---
    let mut rewrites: Vec<(String, String, String)> = Vec::new();
    if !opts.data_only {
        let pins = sync::pins_for(&plan.config, &scope);
        if !pins.is_empty() && (from.control.code.is_empty() || to.control.code.is_empty()) {
            return Err("a side's discovery surface names no code store".to_string());
        }
        for (name, pin) in &pins {
            if pin.version.is_empty() {
                return Err(format!(
                    "mount(s) {:?} pin code:{name} with no version",
                    pin.mounted_at
                ));
            }
            let probe = to
                .client
                .get_bytes(&format!("{}{name}/{}", to.control.code, pin.version))?;
            if probe.status == 200 {
                code_tally.note(Action::Skip);
                continue;
            }
            let src = from
                .client
                .get_bytes(&format!("{}{name}/{}", from.control.code, pin.version))?;
            if src.status != 200 {
                return Err(format!(
                    "code:{name}@{} is pinned but not deployed on {}: {}",
                    pin.version,
                    from.label,
                    src.error_detail()
                ));
            }
            let manifest = from
                .client
                .get(&format!(
                    "{}{name}/{}.manifest.json",
                    from.control.code, pin.version
                ))
                .ok()
                .filter(|r| r.status == 200)
                .map(|r| r.body);
            println!("code    {prefix}deploy code:{name}@{}", pin.version);
            code_tally.note(Action::Create);
            if dry {
                continue;
            }
            let ct = src
                .content_type
                .as_deref()
                .unwrap_or("application/octet-stream")
                .to_string();
            let headers: Vec<(&str, &str)> = manifest
                .as_deref()
                .map(|m| vec![("x-rs2-manifest", m)])
                .unwrap_or_default();
            let resp = to.client.post_bytes_with(
                &format!("{}{name}/", to.control.code),
                &ct,
                &src.body,
                &headers,
            )?;
            if resp.status != 201 && resp.status != 200 {
                return Err(format!(
                    "deploy of code:{name} to {} failed: {}{}",
                    to.label,
                    resp.error_detail(),
                    login_hint(resp.status, to.had_token)
                ));
            }
            let got: Value = serde_json::from_str(&resp.body).unwrap_or(Value::Null);
            if let Some(v) = got.get("version").and_then(Value::as_str) {
                if v != pin.version {
                    eprintln!(
                        "note: {} names this bundle code:{name}@{v} (pinned @{}) — repointing",
                        to.label, pin.version
                    );
                    rewrites.push((name.clone(), pin.version.clone(), v.to_string()));
                }
            }
        }
        for (name, old, new) in &rewrites {
            sync::rewrite_pin(&mut plan.config, name, old, new);
        }
    }

    // --- Config through the validated self-config API ---
    if !opts.data_only {
        let planned_differs = plan.config != to.config;
        for path in &plan.changed_mounts {
            println!("config  {prefix}set mount {path}");
        }
        for path in &plan.pruned_mounts {
            println!("config  {prefix}remove mount {path}");
        }
        if planned_differs && plan.changed_mounts.is_empty() && plan.pruned_mounts.is_empty() {
            println!("config  {prefix}update (top-level keys)");
        }
        for path in &plan.kept_mounts {
            println!("config  keep target-only mount {path} (no --prune)");
        }
        if planned_differs && !dry {
            merge_config_at(&to.client, to.had_token, &to.control.config, |current| {
                // Re-plan against what the target holds now (a 409 retry).
                let mut fresh = sync::plan_config(&from.config, current, &scope)?;
                for (name, old, new) in &rewrites {
                    sync::rewrite_pin(&mut fresh.config, name, old, new);
                }
                if fresh.config == *current {
                    return Ok(false);
                }
                *current = fresh.config;
                Ok(true)
            })
            .map_err(|e| format!("{}: {e}", to.label))?;
            config_changed = true;
            println!("config  updated on {}", to.label);
        } else if !planned_differs {
            println!("config  unchanged");
        }
    }

    // --- Specs ---
    if !opts.data_only {
        for svc in sync::spec_mounts(&from.disc, &scope) {
            let subtree = svc.spec_subtree.as_deref().unwrap_or_default();
            let root = format!("{}/", mirror::join_path(&svc.path, subtree));
            sync_tree(
                &from,
                &to,
                &root,
                &root,
                &[],
                Kind::Spec,
                opts.prune,
                dry,
                &mut spec_tally,
            )?;
        }
    }

    // --- Data ---
    if !opts.no_data {
        let all_mounts = from.mount_paths();
        for svc in sync::data_mounts(&from.disc, &scope) {
            let root = format!("{}/", svc.path.trim_end_matches('/'));
            let root = if root == "//" { "/".to_string() } else { root };
            let kind = if svc.service == "data" {
                Kind::Data
            } else {
                Kind::File
            };
            sync_tree(
                &from,
                &to,
                &root,
                &svc.path,
                &all_mounts,
                kind,
                opts.prune,
                dry,
                &mut data_tally,
            )?;
        }
    }

    println!(
        "{}sync {} → {}: code {} deployed/{} present; config {}; specs {}; data {}",
        if dry { "[dry-run] " } else { "" },
        from.label,
        to.label,
        code_tally.created,
        code_tally.skipped,
        if dry {
            if plan.config != to.config {
                "would change"
            } else {
                "unchanged"
            }
        } else if config_changed {
            "updated"
        } else {
            "unchanged"
        },
        spec_tally.summary(),
        data_tally.summary(),
    );
    Ok(())
}

fn mount_entry<'a>(disc: &'a Discovery, path: &str) -> Option<&'a ServiceEntry> {
    disc.services
        .iter()
        .find(|s| sync::same_mount(&s.path, path))
}

/// What a tree holds, which decides how bodies are compared and written.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Spec store subtree: JSON, canonicalised by the server on write.
    Spec,
    /// `file` mount: opaque bytes with a content type.
    File,
    /// `data` mount: datasets of JSON records, each with an optional schema.
    Data,
}

/// A leaf found by walking a container tree.
struct Leaf {
    path: String,
    size: Option<u64>,
}

/// Walk `container` (ending in `/`) recursively. Returns the leaves and the
/// containers seen, or `None` when the root itself could not be listed
/// (`status` says why). Containers owned by a mount nested under `mount` are
/// left to that mount's own walk; reserved `.rs2-*` names are never entered.
fn walk(
    client: &Client,
    container: &str,
    mount: &str,
    all_mounts: &[String],
) -> Result<Result<(Vec<Leaf>, BTreeSet<String>), u16>, String> {
    let mut leaves = Vec::new();
    let mut dirs = BTreeSet::new();
    let (status, entries) = mirror::list_dir_status(client, container)?;
    if status != 200 {
        return Ok(Err(status));
    }
    walk_entries(
        client,
        container,
        mount,
        all_mounts,
        entries,
        &mut leaves,
        &mut dirs,
    )?;
    Ok(Ok((leaves, dirs)))
}

fn walk_entries(
    client: &Client,
    container: &str,
    mount: &str,
    all_mounts: &[String],
    mut entries: Vec<mirror::DirEntry>,
    leaves: &mut Vec<Leaf>,
    dirs: &mut BTreeSet<String>,
) -> Result<(), String> {
    // A dataset's schema must land before its records (`enforceSchema`).
    entries.sort_by_key(|e| (e.name != ".schema.json", e.dir));
    for entry in entries {
        let name = entry.name.trim_end_matches('/');
        if name.is_empty() || sync::is_reserved_name(name) {
            continue;
        }
        let child = format!("{container}{name}");
        if entry.dir {
            let sub = format!("{child}/");
            if sync::owned_by_child(&sub, mount, all_mounts) {
                continue;
            }
            dirs.insert(sub.clone());
            let (status, sub_entries) = mirror::list_dir_status(client, &sub)?;
            if status != 200 {
                eprintln!("warning: cannot list {sub} (HTTP {status}) — skipped");
                continue;
            }
            walk_entries(client, &sub, mount, all_mounts, sub_entries, leaves, dirs)?;
        } else {
            leaves.push(Leaf {
                path: child,
                size: entry.size,
            });
        }
    }
    Ok(())
}

/// Bring one tree on the target in line with the source: create/update
/// differing leaves, and with `prune` delete target-only leaves (and, for a
/// `data` mount, whole target-only datasets).
#[allow(clippy::too_many_arguments)]
fn sync_tree(
    from: &Side,
    to: &Side,
    root: &str,
    mount: &str,
    all_mounts: &[String],
    kind: Kind,
    prune: bool,
    dry: bool,
    tally: &mut Tally,
) -> Result<(), String> {
    let tag = match kind {
        Kind::Spec => "spec  ",
        Kind::File | Kind::Data => "data  ",
    };
    let prefix = if dry { "would " } else { "" };
    let (src_leaves, src_dirs) = match walk(&from.client, root, mount, all_mounts)? {
        Ok(v) => v,
        Err(404) if kind == Kind::Spec => (Vec::new(), BTreeSet::new()),
        Err(status) => {
            eprintln!(
                "warning: cannot list {root} on {} (HTTP {status}) — skipped{}",
                from.label,
                login_hint(status, from.had_token)
            );
            return Ok(());
        }
    };
    let (tgt_leaves, tgt_dirs) = match walk(&to.client, root, mount, all_mounts)? {
        Ok(v) => v,
        Err(404) => (Vec::new(), BTreeSet::new()),
        Err(status) => {
            return Err(format!(
                "cannot list {root} on {}: HTTP {status}{}",
                to.label,
                login_hint(status, to.had_token)
            ))
        }
    };
    let tgt_index: BTreeMap<&str, Option<u64>> = tgt_leaves
        .iter()
        .map(|l| (l.path.as_str(), l.size))
        .collect();

    for leaf in &src_leaves {
        let src = from.client.get_bytes(&leaf.path)?;
        if src.status != 200 {
            return Err(format!(
                "cannot read {} on {}: {}",
                leaf.path,
                from.label,
                src.error_detail()
            ));
        }
        let src_cmp = comparable(kind, &src);
        let tgt_entry = tgt_index.get(leaf.path.as_str()).copied();
        // Sizes are only trusted for files; JSON is re-serialised by the server.
        let tgt_entry = if kind == Kind::File {
            tgt_entry
        } else {
            tgt_entry.map(|_| None)
        };
        let action = sync::decide(&src_cmp, tgt_entry, || {
            let r = to.client.get_bytes(&leaf.path)?;
            Ok((r.status == 200).then(|| comparable(kind, &r)))
        })?;
        tally.note(action);
        match action {
            Action::Skip => continue,
            Action::Create => println!("{tag}  {prefix}create {}", leaf.path),
            Action::Update => println!("{tag}  {prefix}update {}", leaf.path),
        }
        if dry {
            continue;
        }
        let ct = match kind {
            Kind::Spec | Kind::Data => "application/json".to_string(),
            Kind::File => src
                .content_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
        };
        let resp = to.client.put(&leaf.path, &ct, &src.body, None)?;
        if !matches!(resp.status, 200 | 201 | 204) {
            return Err(format!(
                "write of {} to {} failed: {}{}",
                leaf.path,
                to.label,
                resp.error_detail(),
                login_hint(resp.status, to.had_token)
            ));
        }
    }

    if !prune {
        return Ok(());
    }
    let src_paths: BTreeSet<&str> = src_leaves.iter().map(|l| l.path.as_str()).collect();
    // A target-only dataset goes in one confirmed delete, records and all.
    let mut dropped_dirs: Vec<String> = Vec::new();
    if kind == Kind::Data {
        for dir in tgt_dirs.difference(&src_dirs) {
            if dir
                .trim_start_matches(root)
                .trim_end_matches('/')
                .contains('/')
            {
                continue; // not a dataset (nested), records are handled below
            }
            let ds = dir.trim_end_matches('/');
            let name = ds.rsplit('/').next().unwrap_or_default();
            println!("{tag}  {prefix}delete dataset {ds}");
            tally.deleted += 1;
            dropped_dirs.push(dir.clone());
            if dry {
                continue;
            }
            let resp = to.client.delete(&format!("{ds}?confirm={name}"))?;
            if !matches!(resp.status, 200 | 202 | 204 | 404) {
                return Err(format!(
                    "delete of {ds} on {} failed: {}",
                    to.label,
                    resp.error_detail()
                ));
            }
        }
    }
    for leaf in &tgt_leaves {
        if src_paths.contains(leaf.path.as_str()) {
            continue;
        }
        if dropped_dirs
            .iter()
            .any(|d| leaf.path.starts_with(d.as_str()))
        {
            continue;
        }
        if leaf.path.ends_with("/.schema.json") {
            continue; // a schema the source lacks is left; records were synced
        }
        println!("{tag}  {prefix}delete {}", leaf.path);
        tally.deleted += 1;
        if dry {
            continue;
        }
        let resp = to.client.delete(&leaf.path)?;
        if !matches!(resp.status, 200 | 202 | 204 | 404) {
            return Err(format!(
                "delete of {} on {} failed: {}",
                leaf.path,
                to.label,
                resp.error_detail()
            ));
        }
    }
    Ok(())
}

/// The bytes two sides are compared on: raw for a file, canonical JSON for
/// anything JSON (the server re-serialises, so raw bytes would never match).
fn comparable(kind: Kind, resp: &BytesResponse) -> Vec<u8> {
    match kind {
        Kind::File => resp.body.clone(),
        Kind::Spec | Kind::Data => {
            let text = String::from_utf8_lossy(&resp.body);
            mirror::canonical_json(&text).into_bytes()
        }
    }
}
