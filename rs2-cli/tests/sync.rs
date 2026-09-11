//! `rs2 sync` between two real nodes: two `rs2 dev` processes on loopback,
//! a seeded source, and the CLI driven through `rsconfig.json` named servers.
//! Covers the phase order (bundle → config → specs → data), source-wins,
//! `--dry-run`, `--mount`, `--no-data`, `--prune`, idempotence, and the
//! missing-secret guard.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// A running `rs2 dev` node, killed on drop.
struct Node {
    child: Child,
    host: String,
    _dir: tempfile::TempDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A free loopback port: bind `:0`, read it back, release it. The server
/// prints its configured address, so it must be told a concrete port.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn node() -> Node {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    std::fs::write(
        dir.path().join("serverConfig.json"),
        json!({
            "listen": format!("127.0.0.1:{port}"),
            "tenancy": { "mode": "single", "tenant": "main" },
            "fileRoot": "./data",
            "dataRoot": "./data-store",
            "tenantsDir": "./tenants",
        })
        .to_string(),
    )
    .unwrap();
    std::fs::create_dir_all(dir.path().join("tenants")).unwrap();
    std::fs::write(
        dir.path().join("tenants/main.json"),
        json!({ "mounts": [
            { "path": "/services", "service": "services", "config": { "access": "open" } },
            { "path": "/files", "service": "file", "config": { "access": "open" } },
            { "path": "/data", "service": "data", "config": { "access": "open", "enforceSchema": true } },
            { "path": "/pipes", "service": "pipeline", "config": { "access": "open" } },
        ]})
        .to_string(),
    )
    .unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rs2"));
    cmd.current_dir(dir.path())
        .args(["dev", "serverConfig.json"])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    strip_proxy(&mut cmd);
    let child = cmd.spawn().expect("spawn rs2 dev");
    let host = format!("http://127.0.0.1:{port}");
    let node = Node {
        child,
        host,
        _dir: dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(resp) = ureq::get(&format!("{}/.well-known/rs2/services", node.host)).call() {
            if resp.status() == 200 {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "node on {} never came up",
            node.host
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    node
}

/// Inherited proxy settings would send loopback requests elsewhere.
fn strip_proxy(cmd: &mut Command) {
    for v in [
        "ALL_PROXY",
        "all_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ] {
        cmd.env_remove(v);
    }
}

fn put(host: &str, path: &str, ct: &str, body: &[u8]) -> u16 {
    match ureq::put(&format!("{host}{path}"))
        .set("content-type", ct)
        .send_bytes(body)
    {
        Ok(r) => r.status(),
        Err(ureq::Error::Status(s, _)) => s,
        Err(e) => panic!("PUT {path}: {e}"),
    }
}

fn get(host: &str, path: &str) -> (u16, String, Option<String>) {
    match ureq::get(&format!("{host}{path}")).call() {
        Ok(r) => {
            let ct = r.header("content-type").map(str::to_string);
            (r.status(), r.into_string().unwrap_or_default(), ct)
        }
        Err(ureq::Error::Status(s, r)) => (s, r.into_string().unwrap_or_default(), None),
        Err(e) => panic!("GET {path}: {e}"),
    }
}

fn get_json(host: &str, path: &str) -> Value {
    let (status, body, _) = get(host, path);
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).unwrap()
}

fn config_of(host: &str) -> Value {
    get_json(host, "/services/raw")
}

fn put_config(host: &str, config: &Value) {
    let (status, _, _) = get(host, "/services/raw");
    assert_eq!(status, 200);
    let etag = ureq::get(&format!("{host}/services/raw"))
        .call()
        .unwrap()
        .header("etag")
        .unwrap()
        .to_string();
    let resp = ureq::put(&format!("{host}/services/raw"))
        .set("content-type", "application/json")
        .set("if-match", &etag)
        .send_bytes(config.to_string().as_bytes());
    match resp {
        Ok(r) => assert_eq!(r.status(), 204),
        Err(ureq::Error::Status(s, r)) => panic!("PUT config: {s} {}", r.into_string().unwrap()),
        Err(e) => panic!("PUT config: {e}"),
    }
}

/// A workspace whose `rsconfig.json` names both nodes.
fn workspace(a: &Node, b: &Node) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("rsconfig.json"),
        json!({ "servers": { "a": { "host": a.host }, "b": { "host": b.host } } }).to_string(),
    )
    .unwrap();
    dir
}

struct Outcome {
    ok: bool,
    output: String,
}

fn rs2(dir: &tempfile::TempDir, args: &[&str]) -> Outcome {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rs2"));
    cmd.current_dir(dir.path()).args(args);
    strip_proxy(&mut cmd);
    let out = cmd.output().expect("run rs2");
    Outcome {
        ok: out.status.success(),
        output: format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    }
}

fn sync(dir: &tempfile::TempDir, extra: &[&str]) -> Outcome {
    let mut args = vec!["sync", "--from", "a", "--to", "b"];
    args.extend_from_slice(extra);
    rs2(dir, &args)
}

const BUNDLE: &str = "export default { async handle(msg) { return msg; } }";

/// Seed the source with one of everything sync moves.
fn seed_source(a: &Node) {
    assert_eq!(put(&a.host, "/files/a.txt", "text/plain", b"alpha"), 201);
    assert_eq!(
        put(
            &a.host,
            "/files/sub/b.bin",
            "application/octet-stream",
            &[0, 1, 2, 255]
        ),
        201
    );
    let schema = json!({ "type": "object", "properties": { "n": { "type": "integer" } }, "required": ["n"] });
    assert_eq!(
        put(
            &a.host,
            "/data/things/.schema.json",
            "application/json",
            schema.to_string().as_bytes()
        ),
        200
    );
    assert_eq!(
        put(
            &a.host,
            "/data/things/k1",
            "application/json",
            br#"{"n":1}"#
        ),
        201
    );
    assert_eq!(
        put(
            &a.host,
            "/data/things/k2",
            "application/json",
            br#"{"n":2}"#
        ),
        201
    );
    let spec = json!({ "pipeline": [ "GET /files/a.txt" ] });
    assert_eq!(
        put(
            &a.host,
            "/pipes/.pipelines/p1",
            "application/json",
            spec.to_string().as_bytes()
        ),
        201
    );
    // A JS bundle (stored unvalidated: this test build has no JS engine),
    // deployed keyless and mounted by its content-addressed version.
    let deployed: Value = serde_json::from_str(
        &ureq::post(&format!("{}/services/code/echo/", a.host))
            .set("content-type", "application/javascript")
            .send_bytes(BUNDLE.as_bytes())
            .unwrap()
            .into_string()
            .unwrap(),
    )
    .unwrap();
    let service_ref = deployed["ref"].as_str().unwrap().to_string();
    let mut cfg = config_of(&a.host);
    cfg["mounts"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "path": "/echo", "service": service_ref, "config": {} }));
    cfg["cors"] = json!({ "allowOrigins": ["https://app.example"] });
    put_config(&a.host, &cfg);
}

#[test]
fn sync_moves_a_tenant_between_two_nodes() {
    let a = node();
    let b = node();
    seed_source(&a);
    // Something only the target has, in the data plane and in the config.
    assert_eq!(
        put(&b.host, "/files/only-here.txt", "text/plain", b"keep me"),
        201
    );
    let mut bcfg = config_of(&b.host);
    bcfg["mounts"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "path": "/log", "service": "log", "config": { "access": "open" } }));
    put_config(&b.host, &bcfg);
    let ws = workspace(&a, &b);

    // --- dry run: reports, changes nothing ---
    let dry = sync(&ws, &["--dry-run"]);
    assert!(dry.ok, "{}", dry.output);
    assert!(
        dry.output.contains("would deploy code:echo@"),
        "{}",
        dry.output
    );
    assert!(
        dry.output.contains("would set mount /echo"),
        "{}",
        dry.output
    );
    assert!(
        dry.output.contains("would create /files/a.txt"),
        "{}",
        dry.output
    );
    assert!(
        dry.output
            .contains("would create /data/things/.schema.json"),
        "{}",
        dry.output
    );
    assert!(
        dry.output.contains("would create /pipes/.pipelines/p1"),
        "{}",
        dry.output
    );
    assert!(
        dry.output.contains("keep target-only mount /log"),
        "{}",
        dry.output
    );
    assert_eq!(get(&b.host, "/files/a.txt").0, 404);
    assert!(config_of(&b.host)["mounts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|m| m["path"] != "/echo"));

    // --- scoped dry run touches one mount only ---
    let scoped = sync(&ws, &["--mount", "/files", "--dry-run"]);
    assert!(scoped.ok, "{}", scoped.output);
    assert!(
        scoped.output.contains("would create /files/a.txt"),
        "{}",
        scoped.output
    );
    assert!(!scoped.output.contains("/data/things"), "{}", scoped.output);
    assert!(!scoped.output.contains("/echo"), "{}", scoped.output);
    assert!(!scoped.output.contains("code:echo"), "{}", scoped.output);

    // --- the real thing ---
    let run = sync(&ws, &[]);
    assert!(run.ok, "{}", run.output);
    let cfg = config_of(&b.host);
    let mounts = cfg["mounts"].as_array().unwrap();
    let echo = mounts
        .iter()
        .find(|m| m["path"] == "/echo")
        .expect("echo mount");
    let version = echo["service"]
        .as_str()
        .unwrap()
        .strip_prefix("code:echo@")
        .unwrap()
        .to_string();
    assert_eq!(
        cfg["cors"],
        json!({ "allowOrigins": ["https://app.example"] })
    );
    // Target-only mount kept; the target's own control mount untouched.
    assert!(mounts.iter().any(|m| m["path"] == "/log"));
    assert!(mounts.iter().any(|m| m["path"] == "/services"));
    // Bundle present under the same content-addressed version.
    let (status, body, _) = get(&b.host, &format!("/services/code/echo/{version}"));
    assert_eq!(status, 200);
    assert_eq!(body, BUNDLE);
    // Spec, files (bytes + type), schema, records.
    // The server stores the compiled spec; both sides hold the same one.
    assert_eq!(
        get_json(&b.host, "/pipes/.pipelines/p1"),
        get_json(&a.host, "/pipes/.pipelines/p1")
    );
    let (status, body, ct) = get(&b.host, "/files/a.txt");
    assert_eq!((status, body.as_str()), (200, "alpha"));
    assert!(ct.unwrap().starts_with("text/plain"));
    let bin = ureq::get(&format!("{}/files/sub/b.bin", b.host))
        .call()
        .unwrap();
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut bin.into_reader(), &mut bytes).unwrap();
    assert_eq!(bytes, [0, 1, 2, 255]);
    assert_eq!(
        get_json(&b.host, "/data/things/.schema.json")["required"],
        json!(["n"])
    );
    assert_eq!(get_json(&b.host, "/data/things/k1"), json!({ "n": 1 }));
    assert_eq!(get_json(&b.host, "/data/things/k2"), json!({ "n": 2 }));
    assert_eq!(get(&b.host, "/files/only-here.txt").0, 200);

    // --- idempotent: a second run changes nothing ---
    let again = sync(&ws, &[]);
    assert!(again.ok, "{}", again.output);
    assert!(
        again.output.contains("config unchanged"),
        "{}",
        again.output
    );
    assert!(
        again.output.contains("data 0 created, 0 updated"),
        "{}",
        again.output
    );
    assert!(
        again.output.contains("specs 0 created, 0 updated"),
        "{}",
        again.output
    );
    assert!(
        again.output.contains("code 0 deployed/1 present"),
        "{}",
        again.output
    );

    // --- source wins on a changed record; --no-data leaves data alone ---
    assert_eq!(
        put(
            &a.host,
            "/data/things/k1",
            "application/json",
            br#"{"n":11}"#
        ),
        200
    );
    let no_data = sync(&ws, &["--no-data"]);
    assert!(no_data.ok, "{}", no_data.output);
    assert_eq!(get_json(&b.host, "/data/things/k1"), json!({ "n": 1 }));
    let data_only = sync(&ws, &["--data-only"]);
    assert!(data_only.ok, "{}", data_only.output);
    assert!(
        data_only.output.contains("update /data/things/k1"),
        "{}",
        data_only.output
    );
    assert_eq!(get_json(&b.host, "/data/things/k1"), json!({ "n": 11 }));

    // --- prune: target-only file and mount go; a target-only dataset too ---
    assert_eq!(
        put(
            &b.host,
            "/data/stale/.schema.json",
            "application/json",
            b"{}"
        ),
        200
    );
    assert_eq!(
        put(&b.host, "/data/stale/x", "application/json", b"{}"),
        201
    );
    let prune = sync(&ws, &["--prune"]);
    assert!(prune.ok, "{}", prune.output);
    assert!(
        prune.output.contains("delete /files/only-here.txt"),
        "{}",
        prune.output
    );
    assert!(
        prune.output.contains("delete dataset /data/stale"),
        "{}",
        prune.output
    );
    assert!(
        prune.output.contains("remove mount /log"),
        "{}",
        prune.output
    );
    assert_eq!(get(&b.host, "/files/only-here.txt").0, 404);
    assert_eq!(get(&b.host, "/data/stale/x").0, 404);
    assert!(config_of(&b.host)["mounts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|m| m["path"] != "/log"));
    assert_eq!(get(&b.host, "/files/a.txt").0, 200);

    // --- a secret the target cannot restore aborts before any write ---
    let mut acfg = config_of(&a.host);
    acfg["auth"] = json!({ "jwtSecret": "0123456789abcdef0123456789abcdef" });
    put_config(&a.host, &acfg);
    assert_eq!(put(&a.host, "/files/late.txt", "text/plain", b"late"), 201);
    let blocked = sync(&ws, &[]);
    assert!(!blocked.ok, "{}", blocked.output);
    assert!(
        blocked.output.contains("/auth/jwtSecret"),
        "{}",
        blocked.output
    );
    assert_eq!(get(&b.host, "/files/late.txt").0, 404);
    // The scoped form doesn't carry the top-level auth block, so it still works.
    let scoped = sync(&ws, &["--mount", "/files"]);
    assert!(scoped.ok, "{}", scoped.output);
    assert_eq!(get(&b.host, "/files/late.txt").0, 200);

    // --- same server both sides is refused ---
    let same = rs2(&ws, &["sync", "--from", "a", "--to", &a.host]);
    assert!(!same.ok, "{}", same.output);
    assert!(same.output.contains("same server"), "{}", same.output);
}
