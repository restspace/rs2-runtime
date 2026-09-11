//! The pure half of `rs2 sync`: deciding what a transfer between two nodes
//! should do, with no network. Config planning (source wins, with the
//! target's control mount and secrets protected), mount classification over
//! the discovery surface, nested-mount ownership, code-pin rewriting, and
//! the per-leaf create/update/skip decision. `commands_sync` drives the HTTP.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::mirror::{self, CodePin, Discovery, ServiceEntry};

/// The `"<secret>"` marker the server round-trips in place of a secret.
pub const SECRET_MASK: &str = "<secret>";

/// What to transfer: every mount, or only the named ones; and whether
/// target-only things are deleted.
pub struct Scope<'a> {
    pub mounts: Option<&'a [String]>,
    pub prune: bool,
}

impl Scope<'_> {
    /// Whether a mount at `path` is in scope.
    pub fn includes(&self, path: &str) -> bool {
        match self.mounts {
            None => true,
            Some(list) => list.iter().any(|m| same_mount(m, path)),
        }
    }
}

/// The planned target config plus what changed to get there.
#[derive(Debug, Default)]
pub struct Plan {
    pub config: Value,
    /// Mount paths whose entry differs from the target's (created or replaced).
    pub changed_mounts: Vec<String>,
    /// Target-only mount paths dropped by `--prune`.
    pub pruned_mounts: Vec<String>,
    /// Target-only mount paths kept because `--prune` was not given.
    pub kept_mounts: Vec<String>,
    /// JSON pointers where the planned config carries a `"<secret>"` marker
    /// the target cannot restore (no stored string there) — the server would
    /// reject the PUT, so the caller aborts before writing anything.
    pub missing_secrets: Vec<String>,
}

/// Mount paths compare with a normalised leading/trailing slash so `/x`,
/// `/x/` and `x` name the same mount.
pub fn same_mount(a: &str, b: &str) -> bool {
    a.trim_matches('/') == b.trim_matches('/')
}

fn mount_path(m: &Value) -> &str {
    m.get("path").and_then(Value::as_str).unwrap_or("")
}

fn mount_service(m: &Value) -> &str {
    m.get("service").and_then(Value::as_str).unwrap_or("")
}

fn mounts_of(config: &Value) -> Vec<Value> {
    config
        .get("mounts")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// Compute the config the target should hold.
///
/// Whole-config mode (`scope.mounts == None`): the source document wins every
/// top-level key, except that the target's own `services` mount entry is kept
/// verbatim (never move or re-lock the control mount the CLI is talking
/// through), and target-only mounts are appended after the source's unless
/// `prune`.
///
/// Scoped mode: the target document is untouched except that each in-scope
/// source mount replaces the target entry at the same path in place, or is
/// appended.
///
/// Either way secret slots stay `"<secret>"`; see [`Plan::missing_secrets`].
pub fn plan_config(source: &Value, target: &Value, scope: &Scope) -> Result<Plan, String> {
    let source_mounts = mounts_of(source);
    let target_mounts = mounts_of(target);
    let mut plan = Plan::default();

    let mounts: Vec<Value> = match scope.mounts {
        None => {
            let mut out: Vec<Value> = source_mounts
                .iter()
                .filter(|m| mount_service(m) != "services")
                .cloned()
                .collect();
            // The target's control mount stays exactly where — and how — it is.
            if let Some(i) = target_mounts
                .iter()
                .position(|m| mount_service(m) == "services")
            {
                let at = i.min(out.len());
                out.insert(at, target_mounts[i].clone());
            }
            for m in &target_mounts {
                if mount_service(m) == "services" {
                    continue;
                }
                let path = mount_path(m);
                if source_mounts
                    .iter()
                    .any(|s| same_mount(mount_path(s), path))
                {
                    continue;
                }
                if scope.prune {
                    plan.pruned_mounts.push(path.to_string());
                } else {
                    plan.kept_mounts.push(path.to_string());
                    out.push(m.clone());
                }
            }
            out
        }
        Some(wanted) => {
            let mut out = target_mounts.clone();
            for w in wanted {
                let src = source_mounts
                    .iter()
                    .find(|m| same_mount(mount_path(m), w))
                    .ok_or_else(|| format!("source has no mount at {w}"))?;
                if mount_service(src) == "services" {
                    return Err(format!(
                        "{w} is the source's `services` control mount — it cannot be transferred"
                    ));
                }
                match out.iter().position(|m| same_mount(mount_path(m), w)) {
                    Some(i) => {
                        if mount_service(&out[i]) == "services" {
                            return Err(format!(
                                "{w} is the target's `services` control mount — refusing to replace it"
                            ));
                        }
                        out[i] = src.clone();
                    }
                    None => out.push(src.clone()),
                }
            }
            out
        }
    };

    // Which entries actually differ from what the target has now.
    for m in &mounts {
        let path = mount_path(m);
        let current = target_mounts
            .iter()
            .find(|t| same_mount(mount_path(t), path));
        if current != Some(m) {
            plan.changed_mounts.push(path.to_string());
        }
    }

    let mut config = match scope.mounts {
        None => source.clone(),
        Some(_) => target.clone(),
    };
    if !config.is_object() {
        return Err("tenant config is not a JSON object".to_string());
    }
    config["mounts"] = Value::Array(mounts);

    plan.missing_secrets = missing_secrets(&config, target);
    plan.config = config;
    Ok(plan)
}

/// JSON pointers where `planned` carries the secret marker but `target` has
/// no string to restore it from.
pub fn missing_secrets(planned: &Value, target: &Value) -> Vec<String> {
    let mut out = Vec::new();
    walk_masks(planned, target, "", &mut out);
    out
}

fn walk_masks(planned: &Value, current: &Value, pointer: &str, out: &mut Vec<String>) {
    match planned {
        Value::String(s) if s == SECRET_MASK => {
            let restorable = current.as_str().is_some_and(|c| !c.is_empty());
            if !restorable {
                out.push(pointer.to_string());
            }
        }
        Value::Object(map) => {
            for (k, v) in map {
                let child = current.get(k).unwrap_or(&Value::Null);
                walk_masks(v, child, &format!("{pointer}/{k}"), out);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                let child = current.get(i).unwrap_or(&Value::Null);
                walk_masks(v, child, &format!("{pointer}/{i}"), out);
            }
        }
        _ => {}
    }
}

/// The code pins (`code:<name>@<version>`) referenced by in-scope mounts.
pub fn pins_for(config: &Value, scope: &Scope) -> BTreeMap<String, CodePin> {
    let in_scope: Vec<Value> = mounts_of(config)
        .into_iter()
        .filter(|m| scope.includes(mount_path(m)))
        .collect();
    mirror::code_pins(&serde_json::json!({ "mounts": in_scope }))
}

/// Repoint every mount pinned to `code:<name>@<from>` at `code:<name>@<to>`.
/// Returns how many mounts changed.
pub fn rewrite_pin(config: &mut Value, name: &str, from: &str, to: &str) -> usize {
    let old = format!("code:{name}@{from}");
    let new = format!("code:{name}@{to}");
    let mut n = 0;
    if let Some(mounts) = config.get_mut("mounts").and_then(Value::as_array_mut) {
        for m in mounts {
            if m.get("service").and_then(Value::as_str) == Some(old.as_str()) {
                m["service"] = Value::String(new.clone());
                n += 1;
            }
        }
    }
    n
}

/// Mounts whose contents are the data plane: store-shaped (`file`, `data`, a
/// custom service declaring `store`) but not a `wrapper` (it fronts another
/// mount — walking it would copy that mount twice) or the control mount.
pub fn data_mounts<'a>(disc: &'a Discovery, scope: &Scope) -> Vec<&'a ServiceEntry> {
    disc.services
        .iter()
        .filter(|s| s.pattern == "store")
        .filter(|s| s.service != "wrapper" && s.service != "services")
        .filter(|s| scope.includes(&s.path))
        .collect()
}

/// Mounts with an authoring subtree — the spec stores `rs2 pull` mirrors.
pub fn spec_mounts<'a>(disc: &'a Discovery, scope: &Scope) -> Vec<&'a ServiceEntry> {
    disc.services
        .iter()
        .filter(|s| s.spec_subtree.is_some())
        .filter(|s| scope.includes(&s.path))
        .collect()
}

/// Whether `container` (a path under `mount`) falls inside another mount
/// nested below `mount` — `/data/x/` while walking `/data` when `/data/x` is
/// its own mount. Such containers belong to the child mount's walk.
pub fn owned_by_child(container: &str, mount: &str, all_mounts: &[String]) -> bool {
    let container = container.trim_end_matches('/');
    let mount = mount.trim_end_matches('/');
    all_mounts.iter().any(|m| {
        let m = m.trim_end_matches('/');
        if m == mount || !m.starts_with(&format!("{mount}/")) {
            return false;
        }
        container == m || container.starts_with(&format!("{m}/"))
    })
}

/// Names the walker must never descend: the host's reserved subtrees.
pub fn is_reserved_name(name: &str) -> bool {
    name.trim_end_matches('/').starts_with(".rs2-")
}

/// What to do with one source leaf on the target.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Action {
    Create,
    Update,
    Skip,
}

/// Decide from the target's listing entry (`None` = absent; `Some(size)` =
/// present, with the size if the listing gave one) and, only when the size
/// alone can't tell, the target's bytes. ETags are not compared: they are
/// adapter version strings and mean nothing across two nodes.
pub fn decide(
    src_body: &[u8],
    tgt_entry: Option<Option<u64>>,
    tgt_body: impl FnOnce() -> Result<Option<Vec<u8>>, String>,
) -> Result<Action, String> {
    let Some(size) = tgt_entry else {
        return Ok(Action::Create);
    };
    if let Some(size) = size {
        if size != src_body.len() as u64 {
            return Ok(Action::Update);
        }
    }
    match tgt_body()? {
        None => Ok(Action::Create),
        Some(bytes) if bytes == src_body => Ok(Action::Skip),
        Some(_) => Ok(Action::Update),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg(mounts: Value, extra: Value) -> Value {
        let mut v = extra;
        v["mounts"] = mounts;
        v
    }

    fn paths(config: &Value) -> Vec<String> {
        mounts_of(config)
            .iter()
            .map(|m| mount_path(m).to_string())
            .collect()
    }

    const ALL: Scope<'static> = Scope {
        mounts: None,
        prune: false,
    };

    #[test]
    fn whole_config_source_wins_but_control_mount_and_extras_survive() {
        let source = cfg(
            json!([
                {"path": "/services", "service": "services", "config": {"access": {"read": "A"}}},
                {"path": "/files", "service": "file", "config": {"x": 1}},
                {"path": "/data", "service": "data", "config": {}},
            ]),
            json!({"operatorRoles": ["A"], "cors": {"allow": "*"}}),
        );
        let target = cfg(
            json!([
                {"path": "/files", "service": "file", "config": {"x": 0}},
                {"path": "/svc", "service": "services", "config": {"access": {"read": "B"}}},
                {"path": "/only-here", "service": "log", "config": {}},
            ]),
            json!({"operatorRoles": ["B"]}),
        );
        let plan = plan_config(&source, &target, &ALL).unwrap();
        assert_eq!(plan.config["operatorRoles"], json!(["A"]));
        assert_eq!(plan.config["cors"], json!({"allow": "*"}));
        assert_eq!(
            paths(&plan.config),
            ["/files", "/svc", "/data", "/only-here"]
        );
        // The target's control mount, verbatim, at its own position.
        assert_eq!(plan.config["mounts"][1]["config"]["access"]["read"], "B");
        assert_eq!(plan.changed_mounts, ["/files", "/data"]);
        assert_eq!(plan.kept_mounts, ["/only-here"]);
        assert!(plan.pruned_mounts.is_empty());

        let prune = Scope {
            mounts: None,
            prune: true,
        };
        let pruned = plan_config(&source, &target, &prune).unwrap();
        assert_eq!(paths(&pruned.config), ["/files", "/svc", "/data"]);
        assert_eq!(pruned.pruned_mounts, ["/only-here"]);
    }

    #[test]
    fn scoped_transfer_touches_only_named_mounts() {
        let source = cfg(
            json!([
                {"path": "/files", "service": "file", "config": {"x": 1}},
                {"path": "/new", "service": "data", "config": {}},
                {"path": "/services", "service": "services", "config": {}},
            ]),
            json!({"operatorRoles": ["A"]}),
        );
        let target = cfg(
            json!([
                {"path": "/services", "service": "services", "config": {}},
                {"path": "/files", "service": "file", "config": {"x": 0}},
                {"path": "/other", "service": "log", "config": {}},
            ]),
            json!({"operatorRoles": ["B"]}),
        );
        let wanted = ["/files/".to_string(), "/new".to_string()];
        let scope = Scope {
            mounts: Some(&wanted),
            prune: true,
        };
        let plan = plan_config(&source, &target, &scope).unwrap();
        assert_eq!(plan.config["operatorRoles"], json!(["B"]));
        assert_eq!(
            paths(&plan.config),
            ["/services", "/files", "/other", "/new"]
        );
        assert_eq!(plan.config["mounts"][1]["config"]["x"], 1);
        assert_eq!(plan.changed_mounts, ["/files", "/new"]);
        assert!(plan.pruned_mounts.is_empty());

        let missing = ["/nope".to_string()];
        let scope = Scope {
            mounts: Some(&missing),
            prune: false,
        };
        assert!(plan_config(&source, &target, &scope).is_err());
        let control = ["/services".to_string()];
        let scope = Scope {
            mounts: Some(&control),
            prune: false,
        };
        assert!(plan_config(&source, &target, &scope).is_err());
    }

    #[test]
    fn missing_secrets_are_pointers_the_target_cannot_restore() {
        let planned = json!({
            "auth": {"jwtSecret": SECRET_MASK},
            "secrets": {"stripe": SECRET_MASK, "ok": SECRET_MASK, "n": 3},
            "mounts": []
        });
        let target = json!({"auth": {}, "secrets": {"ok": "real"}});
        assert_eq!(
            missing_secrets(&planned, &target),
            ["/auth/jwtSecret", "/secrets/stripe"]
        );
        let full = json!({"auth": {"jwtSecret": "s"}, "secrets": {"stripe": "k", "ok": "real"}});
        assert!(missing_secrets(&planned, &full).is_empty());
        // The whole-config plan checks its markers against the target.
        let plan = plan_config(&planned, &target, &ALL).unwrap();
        assert_eq!(plan.missing_secrets, ["/auth/jwtSecret", "/secrets/stripe"]);
    }

    #[test]
    fn pins_follow_scope_and_rewrite_hits_every_mount() {
        let mut config = json!({"mounts": [
            {"path": "/a", "service": "code:echo@v1"},
            {"path": "/b", "service": "code:echo@v1"},
            {"path": "/c", "service": "code:other@v9"},
            {"path": "/d", "service": "file"},
        ]});
        let all = pins_for(&config, &ALL);
        assert_eq!(all.len(), 2);
        assert_eq!(all["echo"].mounted_at, ["/a", "/b"]);
        let only_c = ["/c".to_string()];
        let scope = Scope {
            mounts: Some(&only_c),
            prune: false,
        };
        let scoped = pins_for(&config, &scope);
        assert_eq!(scoped.keys().collect::<Vec<_>>(), ["other"]);

        assert_eq!(rewrite_pin(&mut config, "echo", "v1", "v2"), 2);
        assert_eq!(config["mounts"][0]["service"], "code:echo@v2");
        assert_eq!(config["mounts"][2]["service"], "code:other@v9");
        assert_eq!(rewrite_pin(&mut config, "echo", "v1", "v2"), 0);
    }

    #[test]
    fn mount_classification_comes_from_the_discovery_surface() {
        let disc: Discovery = serde_json::from_value(json!({
            "tenant": "t",
            "services": [
                {"path": "/files", "service": "file", "pattern": "store"},
                {"path": "/data", "service": "data", "pattern": "store"},
                {"path": "/w", "service": "wrapper", "pattern": "store"},
                {"path": "/p", "service": "pipeline", "pattern": "store-transform", "specSubtree": ".pipelines"},
                {"path": "/services", "service": "services", "pattern": "api"},
                {"path": "/log", "service": "log", "pattern": "view"},
                {"path": "/pay", "service": "code:stripe@a1", "pattern": "api"},
                {"path": "/custom", "service": "code:kv@b2", "pattern": "store"},
            ],
            "control": {"config": "/services/raw"}
        }))
        .unwrap();
        let data: Vec<&str> = data_mounts(&disc, &ALL)
            .iter()
            .map(|s| s.path.as_str())
            .collect();
        assert_eq!(data, ["/files", "/data", "/custom"]);
        let specs: Vec<&str> = spec_mounts(&disc, &ALL)
            .iter()
            .map(|s| s.path.as_str())
            .collect();
        assert_eq!(specs, ["/p"]);
        let just_files = ["/files".to_string()];
        let scoped = Scope {
            mounts: Some(&just_files),
            prune: false,
        };
        assert_eq!(data_mounts(&disc, &scoped).len(), 1);
        assert!(spec_mounts(&disc, &scoped).is_empty());
    }

    #[test]
    fn nested_mounts_own_their_subtrees() {
        let all = ["/data".to_string(), "/data/x".to_string(), "/".to_string()];
        assert!(owned_by_child("/data/x/", "/data", &all));
        assert!(owned_by_child("/data/x/deep/", "/data", &all));
        assert!(!owned_by_child("/data/xy/", "/data", &all));
        assert!(!owned_by_child("/data/", "/data", &all));
        // The root mount yields to every other mount.
        assert!(owned_by_child("/data/", "/", &all));
        assert!(!owned_by_child("/other/", "/", &all));
        assert!(is_reserved_name(".rs2-code/"));
        assert!(!is_reserved_name("rs2.txt"));
    }

    #[test]
    fn decide_matrix() {
        let body = b"hello".to_vec();
        let never = || -> Result<Option<Vec<u8>>, String> { panic!("no fetch expected") };
        assert_eq!(decide(&body, None, never).unwrap(), Action::Create);
        assert_eq!(decide(&body, Some(Some(3)), never).unwrap(), Action::Update);
        assert_eq!(
            decide(&body, Some(Some(5)), || Ok(Some(b"hello".to_vec()))).unwrap(),
            Action::Skip
        );
        assert_eq!(
            decide(&body, Some(Some(5)), || Ok(Some(b"hellp".to_vec()))).unwrap(),
            Action::Update
        );
        // No size in the listing (a data record): always compare bytes.
        assert_eq!(
            decide(&body, Some(None), || Ok(Some(b"hello".to_vec()))).unwrap(),
            Action::Skip
        );
        assert_eq!(
            decide(&body, Some(None), || Ok(None)).unwrap(),
            Action::Create
        );
    }
}
