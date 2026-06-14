//! G13 Phase 2 proof: a loadable `DataStore` adapter (a deployed JS bundle on a
//! resident runtime) backing a `data` mount's persistence, validated against
//! the store contract — the same conversation shape `tests/store_conformance.rs`
//! runs over the built-in stores, now over a guest-backed store.
//!
//! The adapter speaks Redis's RESP protocol over the gated socket capability to
//! an in-process mock Redis. Because the runtime is resident, the adapter pools
//! one connection across every request — asserted at the end (the mock accepts
//! exactly one connection for the whole suite).

#![cfg(feature = "js")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::FileStore;
use rs2_core::message::{Body, MediaType, Message};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

// ---- the adapter bundle: a store-pattern surface over RESP -----------------

/// A minimal RESP (Redis) client over the pooled socket, shared by the data and
/// query adapters below — it connects lazily and caches the socket in a
/// module-level var, so the resident runtime keeps one connection across
/// requests. A concrete adapter appends its handler (`export default …`).
const RESP_CLIENT: &str = r#"
let SOCK = null;
let RBUF = new Uint8Array(0);

function connect(config) {
  if (SOCK) return;
  SOCK = RS2Socket.connect(config.host || "127.0.0.1", config.port | 0);
  RBUF = new Uint8Array(0);
}
function append(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0); out.set(b, a.length);
  return out;
}
function readLine() {
  let i = 0;
  for (;;) {
    while (i + 1 < RBUF.length) {
      if (RBUF[i] === 13 && RBUF[i + 1] === 10) {
        const line = new TextDecoder().decode(RBUF.slice(0, i));
        RBUF = RBUF.slice(i + 2);
        return line;
      }
      i++;
    }
    const chunk = SOCK.read(65536);
    if (chunk === null) throw new Error("redis: connection closed");
    RBUF = append(RBUF, chunk);
  }
}
function readN(n) {
  while (RBUF.length < n) {
    const chunk = SOCK.read(65536);
    if (chunk === null) throw new Error("redis: connection closed");
    RBUF = append(RBUF, chunk);
  }
  const out = RBUF.slice(0, n);
  RBUF = RBUF.slice(n);
  return out;
}
function readReply() {
  const line = readLine();
  const t = line[0], rest = line.slice(1);
  if (t === "+") return rest;
  if (t === "-") throw new Error("redis: " + rest);
  if (t === ":") return parseInt(rest, 10);
  if (t === "$") {
    const len = parseInt(rest, 10);
    if (len < 0) return null;
    const data = readN(len);
    readN(2);
    return new TextDecoder().decode(data);
  }
  if (t === "*") {
    const count = parseInt(rest, 10);
    if (count < 0) return null;
    const arr = [];
    for (let k = 0; k < count; k++) arr.push(readReply());
    return arr;
  }
  throw new Error("redis: bad reply " + line);
}
function cmd(...args) {
  let s = "*" + args.length + "\r\n";
  for (const a of args) {
    const str = String(a);
    s += "$" + new TextEncoder().encode(str).length + "\r\n" + str + "\r\n";
  }
  SOCK.write(s);
  return readReply();
}
"#;

/// The data adapter: a store-pattern surface (`GET/PUT/DELETE /{ds}/{key}`,
/// container + root listings) over the RESP client. Appended to `RESP_CLIENT`.
const DATA_HANDLER: &str = r#"
export default async (msg, ctx) => {
  connect(ctx.config);
  const url = String(msg.url);
  const path = url.split("?")[0];
  const params = new URLSearchParams(url.split("?")[1] || "");
  const skip = parseInt(params.get("$skip") || "0", 10);
  const take = parseInt(params.get("$take") || "1000", 10);
  const page = (all) => all.slice(skip, skip + take);
  const isContainer = path.endsWith("/");
  const segs = path.split("/").filter(Boolean);

  if (segs.length === 0) {
    if (msg.method !== "GET") return { status: 400, body: { detail: "root supports GET" } };
    const keys = cmd("KEYS", "*") || [];
    const datasets = {};
    for (const k of keys) datasets[k.split(":")[0]] = true;
    const names = Object.keys(datasets).sort();
    const entries = page(names).map((n) => ({ name: n + "/", dir: true }));
    return { status: 200, body: { path: "/", entries, total: names.length } };
  }

  const dataset = segs[0];
  if (segs.length === 1 && isContainer) {
    if (msg.method === "GET") {
      const keys = cmd("KEYS", dataset + ":*") || [];
      const names = keys
        .map((k) => k.slice(dataset.length + 1))
        .filter((n) => n !== ".schema.json")
        .sort();
      const entries = page(names).map((n) => ({ name: n, dir: false, contentType: "application/json" }));
      return { status: 200, body: { path: "/" + dataset + "/", entries, total: names.length } };
    }
    if (msg.method === "DELETE") {
      for (const k of cmd("KEYS", dataset + ":*") || []) cmd("DEL", k);
      return { status: 204 };
    }
    return { status: 400, body: { detail: "container supports GET, DELETE" } };
  }

  const key = segs.slice(1).join("/");
  const rkey = dataset + ":" + key;
  if (msg.method === "GET") {
    const v = cmd("GET", rkey);
    if (v === null) return { status: 404, body: { detail: "no record '" + key + "'" } };
    return { status: 200, body: JSON.parse(v) };
  }
  if (msg.method === "PUT") {
    const existed = cmd("EXISTS", rkey);
    cmd("SET", rkey, JSON.stringify(msg.body));
    return { status: existed ? 200 : 201 };
  }
  if (msg.method === "DELETE") {
    if (!cmd("DEL", rkey)) return { status: 404, body: { detail: "no record '" + key + "'" } };
    return { status: 204 };
  }
  return { status: 400, body: { detail: "method not supported" } };
};
"#;

/// The query adapter: executes a stored query (`POST /query` with
/// `{query, params, take, skip}`) by scanning the dataset over the RESP client
/// and filtering by the substituted `where` clause — query push-down to the
/// backend. Appended to `RESP_CLIENT`.
const QUERY_HANDLER: &str = r#"
function keep(record, where) {
  return Object.entries(where).every(([field, clause]) => {
    if (clause && typeof clause === "object" && clause.op) {
      const a = record[field], b = clause.value;
      switch (clause.op) {
        case ">=": return a >= b;
        case ">":  return a > b;
        case "<=": return a <= b;
        case "<":  return a < b;
        case "!=": return a !== b;
        default:    return a === b;
      }
    }
    return record[field] === clause;
  });
}
export default async (msg, ctx) => {
  connect(ctx.config);
  const { query, take, skip } = msg.body;
  const dataset = query.dataset;
  const where = query.where || {};
  const keys = cmd("KEYS", dataset + ":*") || [];
  let rows = [];
  for (const k of keys) {
    const name = k.slice(dataset.length + 1);
    if (name === ".schema.json") continue;
    const rec = JSON.parse(cmd("GET", k));
    rec._key = name;
    if (keep(rec, where)) rows.push(rec);
  }
  if (query.orderBy) rows.sort((a, b) => (a[query.orderBy] > b[query.orderBy] ? 1 : -1));
  const total = rows.length;
  return { status: 200, body: { rows: rows.slice(skip, skip + take), total } };
};
"#;

/// A self-contained `FileStore` adapter that keeps files in a module-level map
/// (no backend needed — the socket/pooling path is proven by the data/query
/// adapters). It implements the store pattern the host's `GuestFileStore`
/// speaks: `HEAD`/`GET`/`PUT`/`DELETE`/`MOVE` on `/{path}`, container listings
/// on `/{path}/`, content base64 across the JSON boundary.
const FILE_ADAPTER: &str = r#"
let FILES = {}; // path -> { data: base64, mediaType }

function b64len(b64) {
  if (!b64) return 0;
  let n = Math.floor((b64.length * 3) / 4);
  if (b64.endsWith("==")) n -= 2;
  else if (b64.endsWith("=")) n -= 1;
  return n;
}

export default async (msg) => {
  const url = String(msg.url);
  const path = url.split("?")[0];
  const qs = new URLSearchParams(url.split("?")[1] || "");
  const isDir = path.endsWith("/");
  const m = msg.method;

  if (m === "HEAD") {
    if (isDir) {
      const exists = path === "/" || Object.keys(FILES).some((k) => k.startsWith(path));
      return exists ? { status: 200, body: { size: 0, isDir: true } } : { status: 404, body: { detail: "no directory" } };
    }
    const f = FILES[path];
    return f
      ? { status: 200, body: { size: b64len(f.data), isDir: false, mediaType: f.mediaType } }
      : { status: 404, body: { detail: "not found" } };
  }

  if (m === "GET" && isDir) {
    const fileSet = new Set(), dirSet = new Set();
    for (const k of Object.keys(FILES)) {
      if (path !== "/" && !k.startsWith(path)) continue;
      const rest = path === "/" ? k.slice(1) : k.slice(path.length);
      if (!rest) continue;
      const slash = rest.indexOf("/");
      if (slash === -1) fileSet.add(rest);
      else dirSet.add(rest.slice(0, slash));
    }
    const base = path === "/" ? "/" : path;
    const entries = [];
    for (const d of [...dirSet].sort()) entries.push({ name: d + "/", dir: true, size: 0 });
    for (const f of [...fileSet].sort()) {
      entries.push({ name: f, dir: false, size: b64len(FILES[base + f].data), contentType: FILES[base + f].mediaType });
    }
    const total = entries.length;
    const skip = parseInt(qs.get("$skip") || "0", 10), take = parseInt(qs.get("$take") || "1000", 10);
    return { status: 200, body: { entries: entries.slice(skip, skip + take), total } };
  }

  if (m === "GET") {
    const f = FILES[path];
    return f
      ? { status: 200, body: { contentBase64: f.data, mediaType: f.mediaType } }
      : { status: 404, body: { detail: "not found" } };
  }

  if (m === "PUT") {
    const existed = !!FILES[path];
    FILES[path] = { data: msg.body.contentBase64 || "", mediaType: msg.body.mediaType || "application/octet-stream" };
    return { status: existed ? 200 : 201 };
  }

  if (m === "MOVE") {
    const f = FILES[path];
    if (!f) return { status: 404, body: { detail: "source missing" } };
    const existed = !!FILES[msg.body.to];
    FILES[msg.body.to] = f;
    delete FILES[path];
    return { status: existed ? 200 : 201 };
  }

  if (m === "DELETE") {
    if (isDir) {
      const under = Object.keys(FILES).filter((k) => k.startsWith(path));
      if (msg.body && msg.body.recursive) {
        for (const k of under) delete FILES[k];
        return { status: 204 };
      }
      if (under.length > 0) return { status: 409, body: { detail: "directory not empty" } };
      return { status: 204 };
    }
    if (!FILES[path]) return { status: 404, body: { detail: "not found" } };
    delete FILES[path];
    return { status: 204 };
  }

  return { status: 400, body: { detail: "unsupported method" } };
};
"#;

// ---- the mock Redis (RESP) -------------------------------------------------

/// A tiny RESP server supporting the subset the adapter uses (SET/GET/DEL/
/// EXISTS/KEYS). Returns the bound port and a counter of accepted connections
/// (so a test can observe pooling — one connection per resident runtime).
async fn spawn_mock_redis() -> (u16, Arc<AtomicUsize>) {
    spawn_mock_redis_with_delay(std::time::Duration::ZERO).await
}

/// As [`spawn_mock_redis`], but delays each reply by `delay` — so a call
/// occupies its runtime long enough for concurrent calls to overlap (forcing
/// the pool to grow).
async fn spawn_mock_redis_with_delay(delay: std::time::Duration) -> (u16, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let store: Arc<Mutex<BTreeMap<String, String>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let conns = Arc::new(AtomicUsize::new(0));
    let conns2 = conns.clone();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            conns2.fetch_add(1, Ordering::SeqCst);
            let store = store.clone();
            tokio::spawn(serve_conn(sock, store, delay));
        }
    });
    (port, conns)
}

async fn serve_conn(mut sock: TcpStream, store: Arc<Mutex<BTreeMap<String, String>>>, delay: std::time::Duration) {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(args) = read_command(&mut sock, &mut buf).await {
        let reply = exec(&args, &store);
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if sock.write_all(&reply).await.is_err() {
            break;
        }
    }
}

async fn fill(sock: &mut TcpStream, buf: &mut Vec<u8>) -> bool {
    let mut tmp = [0u8; 4096];
    match sock.read(&mut tmp).await {
        Ok(0) | Err(_) => false,
        Ok(n) => {
            buf.extend_from_slice(&tmp[..n]);
            true
        }
    }
}

async fn read_line(sock: &mut TcpStream, buf: &mut Vec<u8>) -> Option<String> {
    loop {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = String::from_utf8_lossy(&buf[..pos]).to_string();
            buf.drain(..pos + 2);
            return Some(line);
        }
        if !fill(sock, buf).await {
            return None;
        }
    }
}

async fn read_n(sock: &mut TcpStream, buf: &mut Vec<u8>, n: usize) -> Option<Vec<u8>> {
    while buf.len() < n {
        if !fill(sock, buf).await {
            return None;
        }
    }
    let out = buf[..n].to_vec();
    buf.drain(..n);
    Some(out)
}

async fn read_command(sock: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<String>> {
    let header = read_line(sock, buf).await?;
    let n: usize = header.strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        let len: usize = read_line(sock, buf).await?.strip_prefix('$')?.parse().ok()?;
        let data = read_n(sock, buf, len).await?;
        read_n(sock, buf, 2).await?; // trailing CRLF
        args.push(String::from_utf8_lossy(&data).to_string());
    }
    Some(args)
}

fn exec(args: &[String], store: &Mutex<BTreeMap<String, String>>) -> Vec<u8> {
    let bulk = |s: &str| format!("${}\r\n{}\r\n", s.len(), s).into_bytes();
    let integer = |n: i64| format!(":{n}\r\n").into_bytes();
    let mut store = store.lock().unwrap();
    match args[0].to_uppercase().as_str() {
        "SET" => {
            store.insert(args[1].clone(), args[2].clone());
            b"+OK\r\n".to_vec()
        }
        "GET" => match store.get(&args[1]) {
            Some(v) => bulk(v),
            None => b"$-1\r\n".to_vec(),
        },
        "DEL" => integer(store.remove(&args[1]).is_some() as i64),
        "EXISTS" => integer(store.contains_key(&args[1]) as i64),
        "KEYS" => {
            let prefix = args[1].strip_suffix('*').unwrap_or(&args[1]);
            let matched: Vec<Vec<u8>> =
                store.keys().filter(|k| k.starts_with(prefix)).map(|k| bulk(k)).collect();
            let mut out = format!("*{}\r\n", matched.len()).into_bytes();
            for m in matched {
                out.extend_from_slice(&m);
            }
            out
        }
        other => format!("-ERR unknown command '{other}'\r\n").into_bytes(),
    }
}

// ---- harness ---------------------------------------------------------------

struct StaticLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body.as_mut().expect("body").as_json(1024 * 1024).await.expect("json body")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_data_store_satisfies_the_store_contract() {
    let (port, conns) = spawn_mock_redis().await;
    let dir = tempfile::tempdir().unwrap();

    // Deploy the adapter bundle into the tenant file store at the code path the
    // `code:redis@v1` reference resolves to.
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    files
        .write(
            "t",
            ".rs2-code/redis/v1.js",
            Body::from_string(
                format!("{RESP_CLIENT}{DATA_HANDLER}"),
                MediaType::new("application/javascript"),
            ),
        )
        .await
        .unwrap();

    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [{
        "path": "/data",
        "service": "data",
        "config": { "store": {
            "adapter": "code:redis@v1",
            "host": "127.0.0.1",
            "port": port,
            "grants": { "redis": { "type": "socket", "hosts": [format!("127.0.0.1:{port}")] } }
        }}
    }]})));
    let rt = Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());

    // PUT create / overwrite, empty body, ETag.
    let resp = rt.handle(req(Method::PUT, "/data/things/alpha").with_json(&json!({ "n": 1 }))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "PUT create: {:?}", resp.body);
    assert!(resp.body.is_none(), "PUT returns no body");
    let resp = rt.handle(req(Method::PUT, "/data/things/alpha").with_json(&json!({ "n": 2 }))).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "PUT overwrite");

    // GET child: the resource, with a version ETag.
    let mut resp = rt.handle(req(Method::GET, "/data/things/alpha")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "GET child");
    assert!(resp.header("etag").is_some(), "child GET carries ETag");
    assert_eq!(body_json(&mut resp).await["n"], 2);

    // Keyless POST: 201 + Location, the new child is fetchable.
    let resp = rt.handle(req(Method::POST, "/data/things/").with_json(&json!({ "n": 3 }))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "keyless POST: {:?}", resp.body);
    let location = resp.header("location").expect("POST returns Location").to_string();
    assert!(location.starts_with("/data/things/"), "Location under container");
    assert_eq!(rt.handle(req(Method::GET, &location)).await.status, Some(StatusCode::OK));

    // Container listing: one shape, one media type, paginated.
    let mut resp = rt.handle(req(Method::GET, "/data/things/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "container GET");
    let ct = resp.body.as_ref().unwrap().media_type.essence().to_string();
    assert_eq!(ct, "application/vnd.rs2.dir+json", "listing media type");
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert!(total >= 2, "X-Total-Count counts both children");
    let listing = body_json(&mut resp).await;
    assert!(
        listing["entries"].as_array().unwrap().iter().any(|e| e["name"] == "alpha" && e["dir"] == false),
        "child appears as an entry: {listing}"
    );
    let mut resp = rt.handle(req(Method::GET, "/data/things/?$take=1")).await;
    let page = body_json(&mut resp).await;
    assert_eq!(page["entries"].as_array().unwrap().len(), 1, "$take pages");
    assert_eq!(page["total"].as_u64(), Some(total), "paged total is the full count");

    // Mount root lists the dataset as a directory entry.
    let mut resp = rt.handle(req(Method::GET, "/data/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        root["entries"].as_array().unwrap().iter().any(|e| e["name"] == "things/" && e["dir"] == true),
        "dataset is a dir entry at the root: {root}"
    );

    // Schema facet: install, read back, and it shows in the listing.
    let put = req(Method::PUT, "/data/things/.schema.json").with_json(&json!({ "type": "object" }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::OK));
    let mut listing = rt.handle(req(Method::GET, "/data/things/")).await;
    let listing = body_json(&mut listing).await;
    assert!(
        listing["entries"].as_array().unwrap().iter().any(|e| e["name"] == ".schema.json"),
        "schema is a fixed child: {listing}"
    );

    // DELETE child: 204, then gone.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/things/alpha")).await.status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::GET, "/data/things/alpha")).await.status,
        Some(StatusCode::NOT_FOUND),
        "deleted child is gone"
    );

    // Container guard: non-empty delete refuses with 409; confirm succeeds.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/things/")).await.status,
        Some(StatusCode::CONFLICT),
        "non-empty container delete is 409 without confirm"
    );
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/things/?confirm=things")).await.status,
        Some(StatusCode::NO_CONTENT),
        "confirmed delete"
    );

    // The resident runtime pooled one connection across every request above.
    assert_eq!(conns.load(Ordering::SeqCst), 1, "adapter pooled a single connection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_adapter_bundle_is_a_clear_error() {
    let (port, _) = spawn_mock_redis().await;
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [{
        "path": "/data",
        "service": "data",
        "config": { "store": {
            "adapter": "code:absent@v9",
            "port": port,
            "grants": { "redis": { "type": "socket", "hosts": [format!("127.0.0.1:{port}")] } }
        }}
    }]})));
    let rt = Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());
    let resp = rt.handle(req(Method::GET, "/data/things/alpha")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_FOUND), "undeployed adapter → 404");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_query_store_executes_a_stored_query() {
    let (port, conns) = spawn_mock_redis().await;
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));

    // Deploy both adapters against the same mock: data (to seed records) and
    // query (to execute stored queries by scanning the dataset).
    files.write("t", ".rs2-code/redis/v1.js", js(format!("{RESP_CLIENT}{DATA_HANDLER}"))).await.unwrap();
    files
        .write("t", ".rs2-code/redis-query/v1.js", js(format!("{RESP_CLIENT}{QUERY_HANDLER}")))
        .await
        .unwrap();

    let socket = json!({ "type": "socket", "hosts": [format!("127.0.0.1:{port}")] });
    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/data", "service": "data", "config": { "store": {
            "adapter": "code:redis@v1", "host": "127.0.0.1", "port": port,
            "grants": { "redis": socket.clone() }
        }}},
        { "path": "/q", "service": "query", "config": { "store": {
            "adapter": "code:redis-query@v1", "host": "127.0.0.1", "port": port,
            "grants": { "redis": socket }
        }}}
    ]})));
    let rt = Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());

    // Seed records through the guest data mount.
    for (k, status, total) in [("o1", "open", 50), ("o2", "closed", 200), ("o3", "open", 150)] {
        let resp = rt
            .handle(req(Method::PUT, &format!("/data/orders/{k}")).with_json(&json!({ "status": status, "total": total })))
            .await;
        assert_eq!(resp.status, Some(StatusCode::CREATED), "seed {k}");
    }

    // Author a stored query — normal SpecStore authoring, unchanged.
    let envelope = json!({
        "language": "json",
        "query": { "dataset": "orders", "where": { "status": "${status}" }, "orderBy": "_key" },
        "params": { "type": "object", "properties": { "status": { "type": "string" } } }
    });
    let put = req(Method::PUT, "/q/.queries/by-status").with_json(&envelope);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED), "author query");

    // Execute: the param substitutes structurally, the guest query store runs
    // it against the shared backend, and the matching rows come back.
    let mut resp = rt.handle(req(Method::GET, "/q/by-status?status=open")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "execute: {:?}", resp.body);
    assert_eq!(resp.header("x-total-count"), Some("2"), "X-Total-Count");
    let rows = body_json(&mut resp).await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2, "two open orders: {rows:?}");
    assert!(rows.iter().all(|r| r["status"] == "open"), "only open orders: {rows:?}");

    // Pagination narrows rows, not the reported total.
    let mut resp = rt.handle(req(Method::GET, "/q/by-status?status=open&$take=1")).await;
    assert_eq!(resp.header("x-total-count"), Some("2"), "paged total is the full count");
    assert_eq!(body_json(&mut resp).await.as_array().unwrap().len(), 1, "$take pages");

    // Each guest mount pooled its own single connection (data + query = 2).
    assert_eq!(conns.load(Ordering::SeqCst), 2, "one pooled connection per resident mount");
}

/// Build a single-`/data` runtime backed by the redis adapter, with extra
/// `store` keys (e.g. `maxRuntimes`, `idleMs`) merged in.
fn data_rt(port: u16, files: Arc<LocalFsFileStore>, extra: serde_json::Value) -> Arc<Runtime> {
    let mut store = json!({
        "adapter": "code:redis@v1", "host": "127.0.0.1", "port": port,
        "grants": { "redis": { "type": "socket", "hosts": [format!("127.0.0.1:{port}")] } }
    });
    for (k, v) in extra.as_object().unwrap() {
        store[k] = v.clone();
    }
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/data", "service": "data", "config": { "store": store } }
    ]})));
    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_grows_under_concurrency_and_caps_at_max_runtimes() {
    // Each reply is delayed, so four concurrent calls overlap and can't reuse a
    // single runtime. With maxRuntimes=2 the pool grows to 2 and no further.
    let (port, conns) = spawn_mock_redis_with_delay(std::time::Duration::from_millis(300)).await;
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));
    files.write("t", ".rs2-code/redis/v1.js", js(format!("{RESP_CLIENT}{DATA_HANDLER}"))).await.unwrap();
    // idleMs:0 disables eviction so it can't interfere with the count.
    let rt = data_rt(port, files, json!({ "maxRuntimes": 2, "idleMs": 0 }));

    let (a, b, c, d) = tokio::join!(
        rt.handle(req(Method::GET, "/data/things/k0")),
        rt.handle(req(Method::GET, "/data/things/k1")),
        rt.handle(req(Method::GET, "/data/things/k2")),
        rt.handle(req(Method::GET, "/data/things/k3")),
    );
    for r in [&a, &b, &c, &d] {
        assert_eq!(r.status, Some(StatusCode::NOT_FOUND), "missing key → 404");
    }
    assert_eq!(conns.load(Ordering::SeqCst), 2, "pool grew to maxRuntimes=2 and capped there");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_runtimes_are_evicted_and_respawn() {
    let (port, conns) = spawn_mock_redis().await;
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));
    files.write("t", ".rs2-code/redis/v1.js", js(format!("{RESP_CLIENT}{DATA_HANDLER}"))).await.unwrap();
    let rt = data_rt(port, files, json!({ "idleMs": 150 }));

    // First call spawns a runtime and opens one connection.
    let resp = rt.handle(req(Method::PUT, "/data/things/a").with_json(&json!({ "n": 1 }))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    assert_eq!(conns.load(Ordering::SeqCst), 1, "one connection after first call");

    // Idle past the eviction window + a few sweeper ticks: the runtime is
    // dropped, closing its socket.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // The next call finds an empty pool and re-spawns → a fresh connection. (If
    // the idle runtime had survived, this would reuse it and stay at 1.)
    let mut resp = rt.handle(req(Method::GET, "/data/things/a")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "record still in the backend");
    assert_eq!(body_json(&mut resp).await["n"], 1);
    assert_eq!(conns.load(Ordering::SeqCst), 2, "idle runtime was evicted; next call re-spawned");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_file_store_satisfies_the_store_contract() {
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));
    files.write("t", ".rs2-code/files/v1.js", js(FILE_ADAPTER.to_string())).await.unwrap();

    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/files", "service": "file", "config": { "store": { "adapter": "code:files@v1" } } }
    ]})));
    let rt = Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());
    let body = |i: u32| Body::from_string(format!("content-{i}"), MediaType::new("text/plain"));

    // PUT create / overwrite, empty body.
    let resp = rt.handle(req(Method::PUT, "/files/docs/alpha").with_body(body(1))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "PUT create");
    assert!(resp.body.is_none(), "PUT returns no body");
    let resp = rt.handle(req(Method::PUT, "/files/docs/alpha").with_body(body(2))).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "PUT overwrite");

    // GET child: content + ETag (from the store-reported version).
    let mut resp = rt.handle(req(Method::GET, "/files/docs/alpha")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "GET child");
    assert!(resp.header("etag").is_some(), "child GET carries ETag");
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"content-2");

    // Keyless POST → 201 + Location, fetchable.
    let resp = rt.handle(req(Method::POST, "/files/docs/").with_body(body(3))).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "keyless POST");
    let location = resp.header("location").expect("Location").to_string();
    assert!(location.starts_with("/files/docs/"), "Location under container");
    assert_eq!(rt.handle(req(Method::GET, &location)).await.status, Some(StatusCode::OK));

    // Container listing: dir+json shape, paginated.
    let mut resp = rt.handle(req(Method::GET, "/files/docs/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "container GET");
    let ct = resp.body.as_ref().unwrap().media_type.essence().to_string();
    assert_eq!(ct, "application/vnd.rs2.dir+json", "listing media type");
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert!(total >= 2, "X-Total-Count counts both children");
    let listing = body_json(&mut resp).await;
    assert!(
        listing["entries"].as_array().unwrap().iter().any(|e| e["name"] == "alpha" && e["dir"] == false),
        "child appears as an entry: {listing}"
    );
    let mut resp = rt.handle(req(Method::GET, "/files/docs/?$take=1")).await;
    let page = body_json(&mut resp).await;
    assert_eq!(page["entries"].as_array().unwrap().len(), 1, "$take pages");
    assert_eq!(page["total"].as_u64(), Some(total), "paged total is the full count");

    // Mount root lists the directory.
    let mut resp = rt.handle(req(Method::GET, "/files/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        root["entries"].as_array().unwrap().iter().any(|e| e["name"] == "docs/" && e["dir"] == true),
        "directory at the root: {root}"
    );

    // HEAD reports the size; a Range serves a 206 slice (the `range` facet).
    let resp = rt.handle(req(Method::HEAD, "/files/docs/alpha")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.header("content-length"), Some("9"), "HEAD content-length (content-2)");
    let mut ranged = req(Method::GET, "/files/docs/alpha");
    ranged.set_header("range", "bytes=0-6");
    let mut resp = rt.handle(ranged).await;
    assert_eq!(resp.status, Some(StatusCode::PARTIAL_CONTENT), "range → 206");
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"content", "first 7 bytes");

    // DELETE child → 204, gone.
    assert_eq!(rt.handle(req(Method::DELETE, "/files/docs/alpha")).await.status, Some(StatusCode::NO_CONTENT));
    assert_eq!(
        rt.handle(req(Method::GET, "/files/docs/alpha")).await.status,
        Some(StatusCode::NOT_FOUND),
        "deleted child is gone"
    );

    // Container guard: non-empty delete is 409; confirm succeeds.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/files/docs/")).await.status,
        Some(StatusCode::CONFLICT),
        "non-empty container delete is 409 without confirm"
    );
    assert_eq!(
        rt.handle(req(Method::DELETE, "/files/docs/?confirm=docs")).await.status,
        Some(StatusCode::NO_CONTENT),
        "confirmed delete"
    );
    let mut resp = rt.handle(req(Method::GET, "/files/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        !root["entries"].as_array().unwrap().iter().any(|e| e["name"] == "docs/"),
        "deleted directory left the root listing: {root}"
    );
}
