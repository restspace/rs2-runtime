//! The instruction-plane *mirror*: a local, git-able copy of everything that
//! describes how a tenant behaves — its config plus every spec store — kept in
//! a single `rs2/` directory so a repo can version-control the back end beside
//! its front end. `rs2 pull` materializes it; `rs2 push` sends edits back
//! through the validated self-config / store APIs (never by writing files under
//! a live node).
//!
//! The set of things to mirror is **discovered, not hardcoded**: the server's
//! discovery surface (`/.well-known/rs2/services`) names the control endpoints
//! and flags each spec store with a `specSubtree`, so a new spec store joins
//! the mirror with no CLI change.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::Client;
use crate::config;

/// The mirror directory name (under the repo root) and its state marker.
pub const MIRROR_DIR: &str = "rs2";
pub const STATE_FILE: &str = "mirror.json";

// ---------------------------------------------------------------------------
// On-disk mirror state (`rs2/mirror.json`)
// ---------------------------------------------------------------------------

/// Sync state recorded at pull and consulted at push: the server identity, the
/// discovered control endpoints, and per-artifact baselines (ETags) used for
/// optimistic concurrency and change detection.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MirrorState {
    pub version: u32,
    pub host: String,
    pub tenant: String,
    pub control: ControlPaths,
    pub config: ConfigBaseline,
    /// Local mirror-relative spec path → baseline. A key whose file is gone is
    /// a deletion; a file absent from the map is a creation.
    #[serde(default)]
    pub specs: BTreeMap<String, SpecBaseline>,
    #[serde(default)]
    pub code: CodeSection,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ControlPaths {
    pub config: String,
    #[serde(default)]
    pub code: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ConfigBaseline {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SpecBaseline {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// Stable content hash of the on-disk file as written at pull, so push can
    /// tell a locally-edited spec from an untouched one without a network read.
    pub hash: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CodeSection {
    #[serde(default)]
    pub lock: BTreeMap<String, CodePin>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CodePin {
    pub version: String,
    #[serde(rename = "mountedAt", default)]
    pub mounted_at: Vec<String>,
}

/// A located mirror: its root directory and the parsed state.
pub struct Mirror {
    pub dir: PathBuf,
    pub state: MirrorState,
}

// ---------------------------------------------------------------------------
// Typed views of the discovery surface (only the fields we consume)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct Discovery {
    pub tenant: String,
    #[serde(default)]
    pub services: Vec<ServiceEntry>,
    #[serde(default)]
    pub control: Option<ControlBlock>,
}

#[derive(Debug, Deserialize)]
pub struct ServiceEntry {
    pub path: String,
    /// The reserved authoring subtree of a spec store (`.pipelines`, …); its
    /// presence is what marks a mount as part of the instruction plane.
    #[serde(default, rename = "specSubtree")]
    pub spec_subtree: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ControlBlock {
    pub config: String,
    #[serde(default)]
    pub code: String,
}

#[derive(Debug, Deserialize)]
struct DirListing {
    #[serde(default)]
    entries: Vec<DirEntry>,
    #[serde(default)]
    total: u64,
}

#[derive(Debug, Deserialize)]
struct DirEntry {
    name: String,
    #[serde(default)]
    dir: bool,
}

/// One spec walked out of a store: its path relative to the subtree root, the
/// raw bytes, and the current ETag.
pub struct RemoteSpec {
    pub rel: String,
    pub body: String,
    pub etag: Option<String>,
}

// ---------------------------------------------------------------------------
// Locating / loading / saving the mirror
// ---------------------------------------------------------------------------

/// Resolve the mirror directory: an explicit `--dir` wins; otherwise walk up to
/// the nearest existing `rs2/mirror.json`. Returns `None` when neither is found
/// (a first pull will create one).
pub fn find_mirror(dir: Option<&str>) -> Result<Option<Mirror>, String> {
    if let Some(d) = dir {
        let dir = PathBuf::from(d);
        let marker = dir.join(STATE_FILE);
        if marker.is_file() {
            return Ok(Some(Mirror { state: load_state(&marker)?, dir }));
        }
        return Ok(None);
    }
    match config::find_up(&format!("{MIRROR_DIR}/{STATE_FILE}"))? {
        Some(marker) => {
            let dir = marker.parent().map(Path::to_path_buf).unwrap_or_default();
            Ok(Some(Mirror { state: load_state(&marker)?, dir }))
        }
        None => Ok(None),
    }
}

pub fn load_state(marker: &Path) -> Result<MirrorState, String> {
    let text = std::fs::read_to_string(marker)
        .map_err(|e| format!("cannot read {}: {e}", marker.display()))?;
    serde_json::from_str(text.trim_start_matches('\u{feff}'))
        .map_err(|e| format!("{} is not valid mirror state: {e}", marker.display()))
}

pub fn save_state(marker: &Path, state: &MirrorState) -> Result<(), String> {
    let text = serde_json::to_string_pretty(state)
        .map_err(|e| format!("cannot serialize mirror state: {e}"))?;
    std::fs::write(marker, text).map_err(|e| format!("cannot write {}: {e}", marker.display()))
}

// ---------------------------------------------------------------------------
// Server interaction
// ---------------------------------------------------------------------------

/// Fetch and parse the discovery surface; errors if the caller can't read a
/// `services` control mount (the whole instruction plane lives behind it).
pub fn discover(client: &Client) -> Result<Discovery, String> {
    let resp = client.get("/.well-known/rs2/services")?;
    if resp.status != 200 {
        return Err(format!("cannot read discovery surface: {}", resp.error_detail()));
    }
    let disc: Discovery = serde_json::from_str(&resp.body)
        .map_err(|e| format!("discovery surface was not the expected JSON: {e}"))?;
    if disc.control.is_none() {
        return Err(
            "discovery `control` is null — no readable `services` mount; you likely need to \
             `rs2 login` as an operator"
                .to_string(),
        );
    }
    Ok(disc)
}

/// List a container fully, following `$take`/`$skip` pagination to `total`.
/// `container` must end in `/`.
fn list_dir_all(client: &Client, container: &str) -> Result<Vec<DirEntry>, String> {
    const PAGE: u64 = 1000;
    let mut all = Vec::new();
    let mut skip = 0u64;
    loop {
        let path = format!("{container}?$take={PAGE}&$skip={skip}");
        let resp = client.get(&path)?;
        if resp.status != 200 {
            return Err(format!("cannot list {container}: {}", resp.error_detail()));
        }
        let listing: DirListing = serde_json::from_str(&resp.body)
            .map_err(|e| format!("listing of {container} was not dir+json: {e}"))?;
        let got = listing.entries.len() as u64;
        all.extend(listing.entries);
        skip += got;
        if got == 0 || skip >= listing.total {
            break;
        }
    }
    Ok(all)
}

/// Recursively walk a spec store's authoring subtree, returning every stored
/// spec with its path relative to the subtree root.
pub fn walk_store(client: &Client, mount_path: &str, subtree: &str) -> Result<Vec<RemoteSpec>, String> {
    let root = join_path(mount_path, subtree); // e.g. "/q/.queries"
    let mut out = Vec::new();
    walk_into(client, &format!("{root}/"), &root, &mut out)?;
    Ok(out)
}

fn walk_into(
    client: &Client,
    container: &str,
    root: &str,
    out: &mut Vec<RemoteSpec>,
) -> Result<(), String> {
    for entry in list_dir_all(client, container)? {
        let name = entry.name.trim_end_matches('/');
        if name.is_empty() {
            continue;
        }
        let child = format!("{container}{name}");
        if entry.dir {
            walk_into(client, &format!("{child}/"), root, out)?;
        } else {
            let resp = client.get(&child)?;
            if resp.status != 200 {
                return Err(format!("cannot read spec {child}: {}", resp.error_detail()));
            }
            let rel = child
                .strip_prefix(root)
                .unwrap_or(&child)
                .trim_start_matches('/')
                .to_string();
            out.push(RemoteSpec { rel, body: resp.body, etag: resp.etag });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Path mapping (local mirror ↔ remote store)
// ---------------------------------------------------------------------------

/// A mount path as a single local directory segment: `/` → `root`, leading `/`
/// dropped, inner `/` → `__`. Unique mount paths ⇒ collision-free.
pub fn mount_slug(mount_path: &str) -> String {
    let trimmed = mount_path.trim_matches('/');
    if trimmed.is_empty() {
        "root".to_string()
    } else {
        trimmed.replace('/', "__")
    }
}

/// Local mirror-relative path for a spec, e.g.
/// `specs/q/.queries/reports/top.json`.
pub fn local_spec_path(mount_path: &str, subtree: &str, rel: &str) -> String {
    format!("specs/{}/{}/{}", mount_slug(mount_path), subtree.trim_matches('/'), rel)
}

/// Reverse a local spec path to its remote store path using the discovered
/// mounts. `None` if the file's mount slug matches no spec store.
pub fn remote_spec_path(local_rel: &str, mounts: &[(String, String)]) -> Option<String> {
    // local_rel == "specs/<slug>/<subtree>/<rest...>"
    let rest = local_rel.strip_prefix("specs/")?;
    let (slug, tail) = rest.split_once('/')?;
    let (mount_path, _subtree) = mounts.iter().find(|(p, _)| mount_slug(p) == slug)?;
    Some(join_path(mount_path, tail))
}

/// Join a mount path and a sub-path into a single slash-normalized path.
fn join_path(base: &str, rest: &str) -> String {
    let base = base.trim_end_matches('/');
    let rest = rest.trim_start_matches('/');
    if base.is_empty() {
        format!("/{rest}")
    } else {
        format!("{base}/{rest}")
    }
}

// ---------------------------------------------------------------------------
// Hashing, secrets, code pins
// ---------------------------------------------------------------------------

/// A stable, dependency-free content hash (FNV-1a, 64-bit) for change
/// detection — deterministic across machines, unlike `DefaultHasher`.
pub fn content_hash(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a-{h:016x}")
}

/// Pretty-print JSON with stable formatting so specs diff cleanly; passes
/// non-JSON bytes through unchanged.
pub fn canonical_json(body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_else(|_| body.to_string()),
        Err(_) => body.to_string(),
    }
}

/// JSON pointers in a tenant config that hold real secret values (anything
/// other than the `"<secret>"` marker the server round-trips): `auth.jwtSecret`
/// and every string leaf under a top-level `secrets` block.
pub fn real_secret_locations(config: &Value) -> Vec<String> {
    const MARKER: &str = "<secret>";
    let mut hits = Vec::new();
    if let Some(s) = config.pointer("/auth/jwtSecret").and_then(Value::as_str) {
        if !s.is_empty() && s != MARKER {
            hits.push("/auth/jwtSecret".to_string());
        }
    }
    if let Some(secrets) = config.get("secrets") {
        collect_real_secrets(secrets, "/secrets", MARKER, &mut hits);
    }
    hits
}

fn collect_real_secrets(value: &Value, pointer: &str, marker: &str, hits: &mut Vec<String>) {
    match value {
        Value::String(s) if !s.is_empty() && s != marker => hits.push(pointer.to_string()),
        Value::Object(map) => {
            for (k, v) in map {
                collect_real_secrets(v, &format!("{pointer}/{k}"), marker, hits);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                collect_real_secrets(v, &format!("{pointer}/{i}"), marker, hits);
            }
        }
        _ => {}
    }
}

/// Build the code pin map from a tenant config's `code:<name>@<version>` mount
/// references — the reproducible record of which bundle versions the tenant
/// uses, and where each is mounted.
pub fn code_pins(config: &Value) -> BTreeMap<String, CodePin> {
    let mut pins: BTreeMap<String, CodePin> = BTreeMap::new();
    let Some(mounts) = config.get("mounts").and_then(Value::as_array) else {
        return pins;
    };
    for m in mounts {
        let Some(service) = m.get("service").and_then(Value::as_str) else { continue };
        let Some(rest) = service.strip_prefix("code:") else { continue };
        let (name, version) = match rest.split_once('@') {
            Some((n, v)) => (n.to_string(), v.to_string()),
            None => (rest.to_string(), String::new()),
        };
        let path = m.get("path").and_then(Value::as_str).unwrap_or("").to_string();
        let pin = pins.entry(name).or_default();
        pin.version = version;
        if !path.is_empty() {
            pin.mounted_at.push(path);
        }
    }
    pins
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mount_slug_maps_paths() {
        assert_eq!(mount_slug("/q"), "q");
        assert_eq!(mount_slug("/"), "root");
        assert_eq!(mount_slug(""), "root");
        assert_eq!(mount_slug("/a/b"), "a__b");
    }

    #[test]
    fn spec_path_round_trips() {
        let mounts = vec![
            ("/q".to_string(), ".queries".to_string()),
            ("/a/b".to_string(), ".pipelines".to_string()),
            ("/".to_string(), ".pipelines".to_string()),
        ];
        for (mount, subtree, rel) in [
            ("/q", ".queries", "reports/top.json"),
            ("/a/b", ".pipelines", "flow.json"),
            ("/", ".pipelines", ".root"),
        ] {
            let local = local_spec_path(mount, subtree, rel);
            let remote = remote_spec_path(&local, &mounts).expect("maps back");
            assert_eq!(remote, join_path(mount, &format!("{subtree}/{rel}")));
        }
    }

    #[test]
    fn unknown_slug_has_no_remote() {
        let mounts = vec![("/q".to_string(), ".queries".to_string())];
        assert!(remote_spec_path("specs/nope/.queries/x.json", &mounts).is_none());
    }

    #[test]
    fn content_hash_is_stable_and_distinct() {
        assert_eq!(content_hash(b"hello"), content_hash(b"hello"));
        assert_ne!(content_hash(b"hello"), content_hash(b"world"));
    }

    #[test]
    fn secret_guard_flags_only_real_values() {
        let clean = json!({ "auth": { "jwtSecret": "<secret>" }, "secrets": { "hook": "<secret>" } });
        assert!(real_secret_locations(&clean).is_empty());
        let dirty = json!({
            "auth": { "jwtSecret": "deadbeef" },
            "secrets": { "hook": "<secret>", "stripe": "sk_live_x" }
        });
        let hits = real_secret_locations(&dirty);
        assert!(hits.contains(&"/auth/jwtSecret".to_string()));
        assert!(hits.contains(&"/secrets/stripe".to_string()));
        assert!(!hits.contains(&"/secrets/hook".to_string()));
    }

    #[test]
    fn code_pins_from_mounts() {
        let cfg = json!({ "mounts": [
            { "path": "/pay", "service": "code:stripe@abc123" },
            { "path": "/pay2", "service": "code:stripe@abc123" },
            { "path": "/files", "service": "file" }
        ]});
        let pins = code_pins(&cfg);
        assert_eq!(pins.len(), 1);
        let stripe = &pins["stripe"];
        assert_eq!(stripe.version, "abc123");
        assert_eq!(stripe.mounted_at, vec!["/pay", "/pay2"]);
    }
}
