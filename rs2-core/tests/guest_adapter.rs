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
use serde_json::{json, Value};
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

/// A MongoDB `DataStore` adapter speaking the real wire protocol — OP_MSG
/// framing + a hand-written BSON codec — over the pooled socket. Datasets are
/// collections; a record's key is its string `_id`. **No auth**: SCRAM-SHA-256
/// needs HMAC/PBKDF2 which the JS prelude's `crypto` doesn't expose, so this
/// targets unauthenticated MongoDB (or a network-trusted deployment); adding a
/// WebCrypto `subtle` to the prelude would unlock SCRAM.
const MONGO_ADAPTER: &str = r#"
let SOCK = null, RBUF = new Uint8Array(0), DB = "test", REQID = 0;
const SCHEMA_COLL = "__rs2_schemas__";

// --- BSON encode (subset: doc/array/string/int32/double/bool/null) ---
function pushI32(a, v) { a.push(v & 0xff, (v >>> 8) & 0xff, (v >>> 16) & 0xff, (v >>> 24) & 0xff); }
function pushDouble(a, v) { const b = new Uint8Array(8); new DataView(b.buffer).setFloat64(0, v, true); for (const x of b) a.push(x); }
function pushCStr(a, s) { for (const x of new TextEncoder().encode(s)) a.push(x); a.push(0); }
function pushStr(a, s) { const b = new TextEncoder().encode(s); pushI32(a, b.length + 1); for (const x of b) a.push(x); a.push(0); }
function encodeInto(a, obj) {
  const body = [];
  for (const [k, v] of Object.entries(obj)) {
    if (v === null || v === undefined) { body.push(0x0a); pushCStr(body, k); }
    else if (typeof v === "boolean") { body.push(0x08); pushCStr(body, k); body.push(v ? 1 : 0); }
    else if (typeof v === "number") {
      if (Number.isInteger(v) && v >= -2147483648 && v <= 2147483647) { body.push(0x10); pushCStr(body, k); pushI32(body, v); }
      else { body.push(0x01); pushCStr(body, k); pushDouble(body, v); }
    } else if (typeof v === "string") { body.push(0x02); pushCStr(body, k); pushStr(body, v); }
    else if (Array.isArray(v)) { const o = {}; v.forEach((x, i) => (o[i] = x)); body.push(0x04); pushCStr(body, k); encodeInto(body, o); }
    else if (typeof v === "object") { body.push(0x03); pushCStr(body, k); encodeInto(body, v); }
    else throw new Error("bson encode: bad type " + typeof v);
  }
  pushI32(a, body.length + 5);
  for (const x of body) a.push(x);
  a.push(0);
}
function encodeDoc(obj) { const a = []; encodeInto(a, obj); return Uint8Array.from(a); }

// --- BSON decode ---
function decodeDoc(bytes, start) {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const end = start + dv.getInt32(start, true);
  let off = start + 4;
  const obj = {};
  while (off < end - 1) {
    const type = bytes[off++];
    let ns = off;
    while (bytes[off] !== 0) off++;
    const name = new TextDecoder().decode(bytes.subarray(ns, off));
    off++;
    let val;
    if (type === 0x01) { val = dv.getFloat64(off, true); off += 8; }
    else if (type === 0x02) { const l = dv.getInt32(off, true); off += 4; val = new TextDecoder().decode(bytes.subarray(off, off + l - 1)); off += l; }
    else if (type === 0x03) { const r = decodeDoc(bytes, off); val = r[0]; off = r[1]; }
    else if (type === 0x04) { const r = decodeDoc(bytes, off); val = Object.values(r[0]); off = r[1]; }
    else if (type === 0x08) { val = bytes[off++] !== 0; }
    else if (type === 0x0a) { val = null; }
    else if (type === 0x10) { val = dv.getInt32(off, true); off += 4; }
    else if (type === 0x12) { val = Number(dv.getBigInt64(off, true)); off += 8; }
    else if (type === 0x07) { off += 12; val = null; }
    else throw new Error("bson decode: unsupported type 0x" + type.toString(16));
    obj[name] = val;
  }
  return [obj, end];
}

// --- OP_MSG over the socket ---
function cat(a, b) { const o = new Uint8Array(a.length + b.length); o.set(a, 0); o.set(b, a.length); return o; }
function ensure(n) { while (RBUF.length < n) { const c = SOCK.read(65536); if (c === null) throw new Error("mongo: closed"); RBUF = cat(RBUF, c); } }
function command(doc) {
  doc["$db"] = DB;
  const body = encodeDoc(doc);
  const msg = new Uint8Array(21 + body.length);
  const dv = new DataView(msg.buffer);
  dv.setInt32(0, msg.length, true);
  dv.setInt32(4, ++REQID, true);
  dv.setInt32(12, 2013, true); // OP_MSG
  msg[20] = 0; // section kind 0
  msg.set(body, 21);
  SOCK.write(msg);
  ensure(4);
  const len = new DataView(RBUF.buffer, RBUF.byteOffset, RBUF.byteLength).getInt32(0, true);
  ensure(len);
  const reply = decodeDoc(RBUF.slice(0, len), 21)[0];
  RBUF = RBUF.slice(len);
  if (!reply.ok) throw new Error("mongo error: " + JSON.stringify(reply));
  return reply;
}

export default async (msg, ctx) => {
  if (!SOCK) { DB = ctx.config.db || "test"; SOCK = RS2Socket.connect(ctx.config.host || "127.0.0.1", ctx.config.port | 0); }
  const path = String(msg.url).split("?")[0];
  const qs = new URLSearchParams(String(msg.url).split("?")[1] || "");
  const segs = path.split("/").filter(Boolean);
  const skip = parseInt(qs.get("$skip") || "0", 10), take = parseInt(qs.get("$take") || "1000", 10);

  if (segs.length === 0) {
    if (msg.method !== "GET") return { status: 400, body: { detail: "root supports GET" } };
    const reply = command({ listCollections: 1, nameOnly: true });
    const names = (reply.cursor.firstBatch || []).map((c) => c.name).filter((n) => n !== SCHEMA_COLL).sort();
    const entries = names.slice(skip, skip + take).map((n) => ({ name: n + "/", dir: true }));
    return { status: 200, body: { path: "/", entries, total: names.length } };
  }

  const dataset = segs[0];
  if (segs.length === 1 && path.endsWith("/")) {
    if (msg.method === "GET") {
      const total = command({ count: dataset }).n || 0;
      const found = command({ find: dataset, filter: {}, skip, limit: take });
      const names = (found.cursor.firstBatch || []).map((d) => String(d._id)).sort();
      const entries = names.map((n) => ({ name: n, dir: false, contentType: "application/json" }));
      return { status: 200, body: { path: "/" + dataset + "/", entries, total } };
    }
    if (msg.method === "DELETE") { command({ drop: dataset }); return { status: 204 }; }
    return { status: 400, body: { detail: "container supports GET, DELETE" } };
  }

  const key = segs.slice(1).join("/");
  if (key === ".schema.json") {
    if (msg.method === "GET") {
      const doc = (command({ find: SCHEMA_COLL, filter: { _id: dataset }, limit: 1 }).cursor.firstBatch || [])[0];
      return doc ? { status: 200, body: doc.schema } : { status: 404, body: { detail: "no schema" } };
    }
    if (msg.method === "PUT") {
      command({ update: SCHEMA_COLL, updates: [{ q: { _id: dataset }, u: { _id: dataset, schema: msg.body }, upsert: true }] });
      return { status: 200 };
    }
    return { status: 400, body: { detail: "schema supports GET, PUT" } };
  }

  if (msg.method === "GET") {
    const doc = (command({ find: dataset, filter: { _id: key }, limit: 1 }).cursor.firstBatch || [])[0];
    if (!doc) return { status: 404, body: { detail: "no record '" + key + "'" } };
    delete doc._id;
    return { status: 200, body: doc };
  }
  if (msg.method === "PUT") {
    const reply = command({ update: dataset, updates: [{ q: { _id: key }, u: Object.assign({}, msg.body, { _id: key }), upsert: true }] });
    const created = reply.upserted && reply.upserted.length > 0;
    return { status: created ? 201 : 200 };
  }
  if (msg.method === "DELETE") {
    if (!command({ delete: dataset, deletes: [{ q: { _id: key }, limit: 1 }] }).n) return { status: 404, body: { detail: "no record" } };
    return { status: 204 };
  }
  return { status: 400, body: { detail: "unsupported" } };
};
"#;

// ---- a mock mongod (OP_MSG + a hand-rolled BSON codec) ---------------------
// A self-contained BSON subset (doc/array/string/int32/int64/double/bool/null)
// over serde_json::Value, mirroring the adapter's JS codec — no `bson` crate
// (it bloats the dependency graph past the MSVC linker's module limit).

fn cstr(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn bson_encode_doc(obj: &serde_json::Map<String, Value>) -> Vec<u8> {
    let mut body = Vec::new();
    for (k, v) in obj {
        match v {
            Value::Null => {
                body.push(0x0a);
                cstr(&mut body, k);
            }
            Value::Bool(b) => {
                body.push(0x08);
                cstr(&mut body, k);
                body.push(*b as u8);
            }
            Value::Number(n) => match n.as_i64() {
                Some(i) if (i32::MIN as i64..=i32::MAX as i64).contains(&i) => {
                    body.push(0x10);
                    cstr(&mut body, k);
                    body.extend_from_slice(&(i as i32).to_le_bytes());
                }
                Some(i) => {
                    body.push(0x12);
                    cstr(&mut body, k);
                    body.extend_from_slice(&i.to_le_bytes());
                }
                None => {
                    body.push(0x01);
                    cstr(&mut body, k);
                    body.extend_from_slice(&n.as_f64().unwrap().to_le_bytes());
                }
            },
            Value::String(s) => {
                body.push(0x02);
                cstr(&mut body, k);
                body.extend_from_slice(&((s.len() + 1) as i32).to_le_bytes());
                body.extend_from_slice(s.as_bytes());
                body.push(0);
            }
            Value::Array(a) => {
                let mut m = serde_json::Map::new();
                for (i, item) in a.iter().enumerate() {
                    m.insert(i.to_string(), item.clone());
                }
                body.push(0x04);
                cstr(&mut body, k);
                body.extend_from_slice(&bson_encode_doc(&m));
            }
            Value::Object(o) => {
                body.push(0x03);
                cstr(&mut body, k);
                body.extend_from_slice(&bson_encode_doc(o));
            }
        }
    }
    let mut out = ((body.len() + 5) as i32).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out.push(0);
    out
}

fn bson_decode_doc(b: &[u8], start: usize) -> (Value, usize) {
    let len = i32::from_le_bytes(b[start..start + 4].try_into().unwrap()) as usize;
    let end = start + len;
    let mut off = start + 4;
    let mut map = serde_json::Map::new();
    while off < end - 1 {
        let t = b[off];
        off += 1;
        let ns = off;
        while b[off] != 0 {
            off += 1;
        }
        let name = String::from_utf8_lossy(&b[ns..off]).to_string();
        off += 1;
        let val = match t {
            0x01 => {
                let v = f64::from_le_bytes(b[off..off + 8].try_into().unwrap());
                off += 8;
                json!(v)
            }
            0x02 => {
                let l = i32::from_le_bytes(b[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let s = String::from_utf8_lossy(&b[off..off + l - 1]).to_string();
                off += l;
                Value::String(s)
            }
            0x03 => {
                let (d, no) = bson_decode_doc(b, off);
                off = no;
                d
            }
            0x04 => {
                let (d, no) = bson_decode_doc(b, off);
                off = no;
                let o = d.as_object().unwrap();
                let mut items: Vec<(usize, Value)> = o
                    .iter()
                    .map(|(k, v)| (k.parse().unwrap_or(0), v.clone()))
                    .collect();
                items.sort_by_key(|(i, _)| *i);
                Value::Array(items.into_iter().map(|(_, v)| v).collect())
            }
            0x08 => {
                let v = b[off] != 0;
                off += 1;
                Value::Bool(v)
            }
            0x0a => Value::Null,
            0x10 => {
                let v = i32::from_le_bytes(b[off..off + 4].try_into().unwrap());
                off += 4;
                json!(v)
            }
            0x12 => {
                let v = i64::from_le_bytes(b[off..off + 8].try_into().unwrap());
                off += 8;
                json!(v)
            }
            _ => return (Value::Object(map), end),
        };
        map.insert(name, val);
    }
    (Value::Object(map), end)
}

/// A minimal MongoDB server: OP_MSG framing + the command subset the adapter
/// uses (find/count/update/delete/drop/listCollections), over an in-memory map.
/// Returns the bound port.
async fn spawn_mock_mongo() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // collection -> (_id -> document)
    let store: Arc<Mutex<BTreeMap<String, BTreeMap<String, Value>>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            tokio::spawn(serve_mongo(sock, store.clone()));
        }
    });
    port
}

async fn serve_mongo(
    mut sock: TcpStream,
    store: Arc<Mutex<BTreeMap<String, BTreeMap<String, Value>>>>,
) {
    loop {
        let mut header = [0u8; 16];
        if sock.read_exact(&mut header).await.is_err() {
            break;
        }
        let len = i32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        if len < 21 {
            break;
        }
        let mut rest = vec![0u8; len - 16];
        if sock.read_exact(&mut rest).await.is_err() {
            break;
        }
        // rest = flagBits(4) + section kind(1) + BSON command doc
        let (cmd, _) = bson_decode_doc(&rest, 5);
        let reply = mongo_dispatch(&cmd, &store);
        let body = bson_encode_doc(reply.as_object().unwrap());
        let total = (21 + body.len()) as i32;
        let mut out = total.to_le_bytes().to_vec();
        out.extend_from_slice(&0i32.to_le_bytes()); // requestID
        out.extend_from_slice(&0i32.to_le_bytes()); // responseTo
        out.extend_from_slice(&2013i32.to_le_bytes()); // OP_MSG
        out.extend_from_slice(&0u32.to_le_bytes()); // flagBits
        out.push(0u8); // section kind 0
        out.extend_from_slice(&body);
        if sock.write_all(&out).await.is_err() {
            break;
        }
    }
}

fn mongo_dispatch(cmd: &Value, store: &Mutex<BTreeMap<String, BTreeMap<String, Value>>>) -> Value {
    let str_of = |key: &str| cmd.get(key).and_then(|v| v.as_str());
    let id_of = |d: &Value| {
        d.get("q")
            .and_then(|q| q.get("_id"))
            .and_then(|v| v.as_str())
            .map(String::from)
    };
    let mut s = store.lock().unwrap();

    if let Some(coll) = str_of("find") {
        let skip = cmd.get("skip").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let limit = cmd
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX) as usize;
        let want_id = cmd
            .get("filter")
            .and_then(|f| f.get("_id"))
            .and_then(|v| v.as_str());
        let mut batch: Vec<Value> = Vec::new();
        if let Some(map) = s.get(coll) {
            match want_id {
                Some(id) => batch.extend(map.get(id).cloned()),
                None => batch.extend(map.values().skip(skip).take(limit).cloned()),
            }
        }
        return json!({ "ok": 1.0, "cursor": { "firstBatch": batch, "id": 0, "ns": format!("test.{coll}") } });
    }
    if let Some(coll) = str_of("count") {
        return json!({ "ok": 1.0, "n": s.get(coll).map(|m| m.len()).unwrap_or(0) as i64 });
    }
    if let Some(coll) = str_of("update") {
        let updates = cmd
            .get("updates")
            .and_then(|u| u.as_array())
            .cloned()
            .unwrap_or_default();
        let (mut n, mut nmod) = (0i64, 0i64);
        let mut upserted: Vec<Value> = Vec::new();
        let map = s.entry(coll.to_string()).or_default();
        for (i, u) in updates.iter().enumerate() {
            let id = id_of(u).unwrap_or_default();
            let doc = u.get("u").cloned().unwrap_or(Value::Null);
            let existed = map.contains_key(&id);
            map.insert(id.clone(), doc);
            n += 1;
            if existed {
                nmod += 1;
            } else {
                upserted.push(json!({ "index": i as i64, "_id": id }));
            }
        }
        let mut reply = json!({ "ok": 1.0, "n": n, "nModified": nmod });
        if !upserted.is_empty() {
            reply["upserted"] = json!(upserted);
        }
        return reply;
    }
    if let Some(coll) = str_of("delete") {
        let deletes = cmd
            .get("deletes")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();
        let mut n = 0i64;
        if let Some(map) = s.get_mut(coll) {
            for d in &deletes {
                if let Some(id) = id_of(d) {
                    if map.remove(&id).is_some() {
                        n += 1;
                    }
                }
            }
        }
        return json!({ "ok": 1.0, "n": n });
    }
    if let Some(coll) = str_of("drop") {
        s.remove(coll);
        return json!({ "ok": 1.0 });
    }
    if cmd.get("listCollections").is_some() {
        let batch: Vec<Value> = s
            .keys()
            .map(|name| json!({ "name": name, "type": "collection" }))
            .collect();
        return json!({ "ok": 1.0, "cursor": { "firstBatch": batch, "id": 0, "ns": "test.$cmd.listCollections" } });
    }
    json!({ "ok": 1.0 }) // hello / ping / unknown
}

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

async fn serve_conn(
    mut sock: TcpStream,
    store: Arc<Mutex<BTreeMap<String, String>>>,
    delay: std::time::Duration,
) {
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
        let len: usize = read_line(sock, buf)
            .await?
            .strip_prefix('$')?
            .parse()
            .ok()?;
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
            let matched: Vec<Vec<u8>> = store
                .keys()
                .filter(|k| k.starts_with(prefix))
                .map(|k| bulk(k))
                .collect();
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
    msg.body
        .as_mut()
        .expect("body")
        .as_json(1024 * 1024)
        .await
        .expect("json body")
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
        "config": { "access": "open", "store": {
            "adapter": "code:redis@v1",
            "host": "127.0.0.1",
            "port": port,
            "grants": { "redis": { "type": "socket", "hosts": [format!("127.0.0.1:{port}")] } }
        }}
    }]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );

    // PUT create / overwrite, empty body, ETag.
    let resp = rt
        .handle(req(Method::PUT, "/data/things/alpha").with_json(&json!({ "n": 1 })))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::CREATED),
        "PUT create: {:?}",
        resp.body
    );
    assert!(resp.body.is_none(), "PUT returns no body");
    let resp = rt
        .handle(req(Method::PUT, "/data/things/alpha").with_json(&json!({ "n": 2 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "PUT overwrite");

    // GET child: the resource, with a version ETag.
    let mut resp = rt.handle(req(Method::GET, "/data/things/alpha")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "GET child");
    assert!(resp.header("etag").is_some(), "child GET carries ETag");
    assert_eq!(body_json(&mut resp).await["n"], 2);

    // Keyless POST: 201 + Location, the new child is fetchable.
    let resp = rt
        .handle(req(Method::POST, "/data/things/").with_json(&json!({ "n": 3 })))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::CREATED),
        "keyless POST: {:?}",
        resp.body
    );
    let location = resp
        .header("location")
        .expect("POST returns Location")
        .to_string();
    assert!(
        location.starts_with("/data/things/"),
        "Location under container"
    );
    assert_eq!(
        rt.handle(req(Method::GET, &location)).await.status,
        Some(StatusCode::OK)
    );

    // Container listing: one shape, one media type, paginated.
    let mut resp = rt.handle(req(Method::GET, "/data/things/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "container GET");
    let ct = resp.body.as_ref().unwrap().media_type.essence().to_string();
    assert_eq!(ct, "application/vnd.rs2.dir+json", "listing media type");
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert!(total >= 2, "X-Total-Count counts both children");
    let listing = body_json(&mut resp).await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "alpha" && e["dir"] == false),
        "child appears as an entry: {listing}"
    );
    let mut resp = rt.handle(req(Method::GET, "/data/things/?$take=1")).await;
    let page = body_json(&mut resp).await;
    assert_eq!(page["entries"].as_array().unwrap().len(), 1, "$take pages");
    assert_eq!(
        page["total"].as_u64(),
        Some(total),
        "paged total is the full count"
    );

    // Mount root lists the dataset as a directory entry.
    let mut resp = rt.handle(req(Method::GET, "/data/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        root["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "things/" && e["dir"] == true),
        "dataset is a dir entry at the root: {root}"
    );

    // Schema facet: install, read back, and it shows in the listing.
    let put = req(Method::PUT, "/data/things/.schema.json").with_json(&json!({ "type": "object" }));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::OK));
    let mut listing = rt.handle(req(Method::GET, "/data/things/")).await;
    let listing = body_json(&mut listing).await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == ".schema.json"),
        "schema is a fixed child: {listing}"
    );

    // DELETE child: 204, then gone.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/things/alpha"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::GET, "/data/things/alpha"))
            .await
            .status,
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
        rt.handle(req(Method::DELETE, "/data/things/?confirm=things"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT),
        "confirmed delete"
    );

    // The resident runtime pooled one connection across every request above.
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "adapter pooled a single connection"
    );
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
        "config": { "access": "open", "store": {
            "adapter": "code:absent@v9",
            "port": port,
            "grants": { "redis": { "type": "socket", "hosts": [format!("127.0.0.1:{port}")] } }
        }}
    }]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );
    let resp = rt.handle(req(Method::GET, "/data/things/alpha")).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::NOT_FOUND),
        "undeployed adapter → 404"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_query_store_executes_a_stored_query() {
    let (port, conns) = spawn_mock_redis().await;
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));

    // Deploy both adapters against the same mock: data (to seed records) and
    // query (to execute stored queries by scanning the dataset).
    files
        .write(
            "t",
            ".rs2-code/redis/v1.js",
            js(format!("{RESP_CLIENT}{DATA_HANDLER}")),
        )
        .await
        .unwrap();
    files
        .write(
            "t",
            ".rs2-code/redis-query/v1.js",
            js(format!("{RESP_CLIENT}{QUERY_HANDLER}")),
        )
        .await
        .unwrap();

    let socket = json!({ "type": "socket", "hosts": [format!("127.0.0.1:{port}")] });
    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/data", "service": "data", "config": { "access": "open", "store": {
            "adapter": "code:redis@v1", "host": "127.0.0.1", "port": port,
            "grants": { "redis": socket.clone() }
        }}},
        { "path": "/q", "service": "query", "config": { "access": "open", "store": {
            "adapter": "code:redis-query@v1", "host": "127.0.0.1", "port": port,
            "grants": { "redis": socket }
        }}}
    ]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );

    // Seed records through the guest data mount.
    for (k, status, total) in [
        ("o1", "open", 50),
        ("o2", "closed", 200),
        ("o3", "open", 150),
    ] {
        let resp = rt
            .handle(
                req(Method::PUT, &format!("/data/orders/{k}"))
                    .with_json(&json!({ "status": status, "total": total })),
            )
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
    assert_eq!(
        rt.handle(put).await.status,
        Some(StatusCode::CREATED),
        "author query"
    );

    // Execute: the param substitutes structurally, the guest query store runs
    // it against the shared backend, and the matching rows come back.
    let mut resp = rt
        .handle(req(Method::GET, "/q/by-status?status=open"))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::OK),
        "execute: {:?}",
        resp.body
    );
    assert_eq!(resp.header("x-total-count"), Some("2"), "X-Total-Count");
    let rows = body_json(&mut resp).await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2, "two open orders: {rows:?}");
    assert!(
        rows.iter().all(|r| r["status"] == "open"),
        "only open orders: {rows:?}"
    );

    // Pagination narrows rows, not the reported total.
    let mut resp = rt
        .handle(req(Method::GET, "/q/by-status?status=open&$take=1"))
        .await;
    assert_eq!(
        resp.header("x-total-count"),
        Some("2"),
        "paged total is the full count"
    );
    assert_eq!(
        body_json(&mut resp).await.as_array().unwrap().len(),
        1,
        "$take pages"
    );

    // Each guest mount pooled its own single connection (data + query = 2).
    assert_eq!(
        conns.load(Ordering::SeqCst),
        2,
        "one pooled connection per resident mount"
    );
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
        { "path": "/data", "service": "data", "config": { "access": "open", "store": store } }
    ]})));
    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_grows_under_concurrency_and_caps_at_max_runtimes() {
    // Each reply is delayed, so four concurrent calls overlap and can't reuse a
    // single runtime. With maxRuntimes=2 the pool grows to 2 and no further.
    let (port, conns) = spawn_mock_redis_with_delay(std::time::Duration::from_millis(300)).await;
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));
    files
        .write(
            "t",
            ".rs2-code/redis/v1.js",
            js(format!("{RESP_CLIENT}{DATA_HANDLER}")),
        )
        .await
        .unwrap();
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
    assert_eq!(
        conns.load(Ordering::SeqCst),
        2,
        "pool grew to maxRuntimes=2 and capped there"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_runtimes_are_evicted_and_respawn() {
    let (port, conns) = spawn_mock_redis().await;
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));
    files
        .write(
            "t",
            ".rs2-code/redis/v1.js",
            js(format!("{RESP_CLIENT}{DATA_HANDLER}")),
        )
        .await
        .unwrap();
    let rt = data_rt(port, files, json!({ "idleMs": 150 }));

    // First call spawns a runtime and opens one connection.
    let resp = rt
        .handle(req(Method::PUT, "/data/things/a").with_json(&json!({ "n": 1 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "one connection after first call"
    );

    // Idle past the eviction window + a few sweeper ticks: the runtime is
    // dropped, closing its socket.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;

    // The next call finds an empty pool and re-spawns → a fresh connection. (If
    // the idle runtime had survived, this would reuse it and stay at 1.)
    let mut resp = rt.handle(req(Method::GET, "/data/things/a")).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::OK),
        "record still in the backend"
    );
    assert_eq!(body_json(&mut resp).await["n"], 1);
    assert_eq!(
        conns.load(Ordering::SeqCst),
        2,
        "idle runtime was evicted; next call re-spawned"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_file_store_satisfies_the_store_contract() {
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));
    files
        .write("t", ".rs2-code/files/v1.js", js(FILE_ADAPTER.to_string()))
        .await
        .unwrap();

    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/files", "service": "file", "config": { "access": "open", "store": { "adapter": "code:files@v1" } } }
    ]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );
    let body = |i: u32| Body::from_string(format!("content-{i}"), MediaType::new("text/plain"));

    // PUT create / overwrite, empty body.
    let resp = rt
        .handle(req(Method::PUT, "/files/docs/alpha").with_body(body(1)))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "PUT create");
    assert!(resp.body.is_none(), "PUT returns no body");
    let resp = rt
        .handle(req(Method::PUT, "/files/docs/alpha").with_body(body(2)))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "PUT overwrite");

    // GET child: content + ETag (from the store-reported version).
    let mut resp = rt.handle(req(Method::GET, "/files/docs/alpha")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "GET child");
    assert!(resp.header("etag").is_some(), "child GET carries ETag");
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"content-2");

    // Keyless POST → 201 + Location, fetchable.
    let resp = rt
        .handle(req(Method::POST, "/files/docs/").with_body(body(3)))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "keyless POST");
    let location = resp.header("location").expect("Location").to_string();
    assert!(
        location.starts_with("/files/docs/"),
        "Location under container"
    );
    assert_eq!(
        rt.handle(req(Method::GET, &location)).await.status,
        Some(StatusCode::OK)
    );

    // Container listing: dir+json shape, paginated.
    let mut resp = rt.handle(req(Method::GET, "/files/docs/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "container GET");
    let ct = resp.body.as_ref().unwrap().media_type.essence().to_string();
    assert_eq!(ct, "application/vnd.rs2.dir+json", "listing media type");
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert!(total >= 2, "X-Total-Count counts both children");
    let listing = body_json(&mut resp).await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "alpha" && e["dir"] == false),
        "child appears as an entry: {listing}"
    );
    let mut resp = rt.handle(req(Method::GET, "/files/docs/?$take=1")).await;
    let page = body_json(&mut resp).await;
    assert_eq!(page["entries"].as_array().unwrap().len(), 1, "$take pages");
    assert_eq!(
        page["total"].as_u64(),
        Some(total),
        "paged total is the full count"
    );

    // Mount root lists the directory.
    let mut resp = rt.handle(req(Method::GET, "/files/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        root["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "docs/" && e["dir"] == true),
        "directory at the root: {root}"
    );

    // HEAD reports the size; a Range serves a 206 slice (the `range` facet).
    let resp = rt.handle(req(Method::HEAD, "/files/docs/alpha")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.header("content-length"),
        Some("9"),
        "HEAD content-length (content-2)"
    );
    let mut ranged = req(Method::GET, "/files/docs/alpha");
    ranged.set_header("range", "bytes=0-6");
    let mut resp = rt.handle(ranged).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::PARTIAL_CONTENT),
        "range → 206"
    );
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"content", "first 7 bytes");

    // DELETE child → 204, gone.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/files/docs/alpha"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::GET, "/files/docs/alpha"))
            .await
            .status,
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
        rt.handle(req(Method::DELETE, "/files/docs/?confirm=docs"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT),
        "confirmed delete"
    );
    let mut resp = rt.handle(req(Method::GET, "/files/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        !root["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "docs/"),
        "deleted directory left the root listing: {root}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guest_backed_mongo_data_store_satisfies_the_store_contract() {
    let port = spawn_mock_mongo().await;
    let dir = tempfile::tempdir().unwrap();
    let files: Arc<LocalFsFileStore> = Arc::new(LocalFsFileStore::new(dir.path()));
    let js = |s: String| Body::from_string(s, MediaType::new("application/javascript"));
    files
        .write("t", ".rs2-code/mongo/v1.js", js(MONGO_ADAPTER.to_string()))
        .await
        .unwrap();

    let adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": [{
        "path": "/data", "service": "data", "config": { "access": "open", "store": {
            "adapter": "code:mongo@v1", "host": "127.0.0.1", "port": port, "db": "test",
            "grants": { "mongo": { "type": "socket", "hosts": [format!("127.0.0.1:{port}")] } }
        }}
    }]})));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    );

    // PUT create / overwrite over the real Mongo wire protocol.
    let resp = rt
        .handle(
            req(Method::PUT, "/data/orders/o1")
                .with_json(&json!({ "status": "open", "total": 50 })),
        )
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::CREATED),
        "PUT create: {:?}",
        resp.body
    );
    let resp = rt
        .handle(
            req(Method::PUT, "/data/orders/o1")
                .with_json(&json!({ "status": "open", "total": 55 })),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "PUT overwrite");

    // GET round-trips the record (BSON encode → mock → BSON decode).
    let mut resp = rt.handle(req(Method::GET, "/data/orders/o1")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "GET child");
    assert!(resp.header("etag").is_some(), "child GET carries ETag");
    let rec = body_json(&mut resp).await;
    assert_eq!(rec["status"], "open");
    assert_eq!(rec["total"], 55);
    assert!(rec.get("_id").is_none(), "_id is stripped from the record");

    // Keyless POST → 201 + Location, fetchable.
    let resp = rt
        .handle(req(Method::POST, "/data/orders/").with_json(&json!({ "status": "new" })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "keyless POST");
    let location = resp.header("location").expect("Location").to_string();
    assert_eq!(
        rt.handle(req(Method::GET, &location)).await.status,
        Some(StatusCode::OK)
    );

    // Container listing + pagination (find + count commands).
    let mut resp = rt.handle(req(Method::GET, "/data/orders/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "container GET");
    let total: u64 = resp.header("x-total-count").unwrap().parse().unwrap();
    assert!(total >= 2, "X-Total-Count");
    let listing = body_json(&mut resp).await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "o1" && e["dir"] == false),
        "record appears as an entry: {listing}"
    );
    let mut resp = rt.handle(req(Method::GET, "/data/orders/?$take=1")).await;
    let page = body_json(&mut resp).await;
    assert_eq!(page["entries"].as_array().unwrap().len(), 1, "$take pages");
    assert_eq!(
        page["total"].as_u64(),
        Some(total),
        "paged total is the full count"
    );

    // Mount root lists the collection as a dataset.
    let mut resp = rt.handle(req(Method::GET, "/data/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        root["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "orders/" && e["dir"] == true),
        "dataset at the root: {root}"
    );

    // Schema facet: install (a separate collection), read back, shown in listing.
    let put = req(Method::PUT, "/data/orders/.schema.json").with_json(&json!({ "type": "object" }));
    assert_eq!(
        rt.handle(put).await.status,
        Some(StatusCode::OK),
        "install schema"
    );
    let mut resp = rt
        .handle(req(Method::GET, "/data/orders/.schema.json"))
        .await;
    assert_eq!(
        body_json(&mut resp).await["type"],
        "object",
        "schema reads back"
    );
    let mut listing = rt.handle(req(Method::GET, "/data/orders/")).await;
    assert!(
        body_json(&mut listing).await["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == ".schema.json"),
        "schema is a fixed child"
    );

    // DELETE child → 204, gone.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/orders/o1"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::GET, "/data/orders/o1")).await.status,
        Some(StatusCode::NOT_FOUND),
        "deleted record is gone"
    );

    // Container guard + drop.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/orders/")).await.status,
        Some(StatusCode::CONFLICT),
        "non-empty container delete is 409 without confirm"
    );
    assert_eq!(
        rt.handle(req(Method::DELETE, "/data/orders/?confirm=orders"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT),
        "confirmed delete drops the collection"
    );
    let mut resp = rt.handle(req(Method::GET, "/data/")).await;
    let root = body_json(&mut resp).await;
    assert!(
        !root["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "orders/"),
        "dropped collection left the root listing: {root}"
    );
}
