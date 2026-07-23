// MongoDB `DataStore` adapter for RS2 — a deployable single-file ESM bundle
// speaking the real wire protocol (OP_MSG framing + a hand-written BSON codec)
// over the pooled socket capability. Datasets are collections; a record's key
// is its string `_id`. **No auth**: SCRAM-SHA-256 needs HMAC/PBKDF2 which the
// JS prelude's `crypto` doesn't expose, so this targets unauthenticated
// MongoDB (or a network-trusted deployment); adding a WebCrypto `subtle` to
// the prelude would unlock SCRAM.
//
// Config (ctx.config, from the mount's `store` block): `db` (default "test"),
// `host` (default 127.0.0.1), `port`. Requires a socket grant for host:port.
// Deploy: `rs2 deploy mongo-data.js --name mongo-data` → the tenant's
// `.rs2-code/mongo-data/<version>.js`; reference as
// `"store": { "adapter": "code:mongo-data@<version>", ... }` on a data mount.

// ============================================================================
// BSON + OP_MSG wire section — KEEP IN SYNC with guest-adapters/mongo-query.js.
// Bundles are single-file ESM with no build step, so this section is
// duplicated verbatim in both adapters; edit both or they diverge.
// ============================================================================
let SOCK = null, RBUF = new Uint8Array(0), DB = "test", REQID = 0;

function connect(config) {
  if (SOCK) return;
  DB = config.db || "test";
  SOCK = RS2Socket.connect(config.host || "127.0.0.1", config.port | 0);
  RBUF = new Uint8Array(0);
}

// --- BSON encode (subset: doc/array/string/int32/int64/double/bool/null) ---
function pushI32(a, v) { a.push(v & 0xff, (v >>> 8) & 0xff, (v >>> 16) & 0xff, (v >>> 24) & 0xff); }
function pushI64(a, v) { const b = new Uint8Array(8); new DataView(b.buffer).setBigInt64(0, BigInt(v), true); for (const x of b) a.push(x); }
function pushDouble(a, v) { const b = new Uint8Array(8); new DataView(b.buffer).setFloat64(0, v, true); for (const x of b) a.push(x); }
function pushCStr(a, s) { for (const x of new TextEncoder().encode(s)) a.push(x); a.push(0); }
function pushStr(a, s) { const b = new TextEncoder().encode(s); pushI32(a, b.length + 1); for (const x of b) a.push(x); a.push(0); }
function encodeInto(a, obj) {
  const body = [];
  for (const [k, v] of Object.entries(obj)) {
    if (v === null || v === undefined) { body.push(0x0a); pushCStr(body, k); }
    else if (typeof v === "boolean") { body.push(0x08); pushCStr(body, k); body.push(v ? 1 : 0); }
    else if (typeof v === "number") {
      if (!Number.isInteger(v)) { body.push(0x01); pushCStr(body, k); pushDouble(body, v); }
      else if (v >= -2147483648 && v <= 2147483647) { body.push(0x10); pushCStr(body, k); pushI32(body, v); }
      else { body.push(0x12); pushCStr(body, k); pushI64(body, v); } // int64 beyond int32
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
// v1 production data carries dates and ObjectIds, so decode must not lose
// them: 0x09 UTC datetime → ISO-8601 string, 0x07 ObjectId → 24-char hex
// string. 0x12 int64 → Number — values outside ±(2^53-1) lose precision (JSON
// numbers can't carry more); acceptable for real-world ids/counters.
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
    else if (type === 0x09) { val = new Date(Number(dv.getBigInt64(off, true))).toISOString(); off += 8; }
    else if (type === 0x07) { val = ""; for (let j = 0; j < 12; j++) val += bytes[off + j].toString(16).padStart(2, "0"); off += 12; }
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
// ============================================================================
// end of shared wire section
// ============================================================================

const SCHEMA_COLL = "__rs2_schemas__";

// Native listing pushdown: the host sends `$select`/`$sort` to this bundle
// instead of key-walking, because we advertise the feature here. See the
// projected-listing route below and guest-adapters/README.md for the
// documented sort-semantics deviation on MongoDB.
export const features = ["list-records"];

// Rebuild the listing contract's projected shape from a fetched doc: walk
// each dotted path; absent paths are omitted entirely. (Mongo's inclusion
// projection would keep empty intermediate objects — `{meta: {}}` when `meta`
// exists but `meta.date` doesn't — which the contract does not.) The server-
// side projection still trims the wire payload; this pass fixes the shape.
function pick(doc, paths) {
  const out = {};
  for (const p of paths) {
    const segs = p.split(".");
    let cur = doc, ok = true;
    for (const s of segs) {
      if (cur === null || typeof cur !== "object" || Array.isArray(cur) || !(s in cur)) { ok = false; break; }
      cur = cur[s];
    }
    if (!ok) continue;
    let o = out;
    for (let i = 0; i < segs.length - 1 && o; i++) {
      const seg = segs[i];
      if (!(seg in o)) o[seg] = {};
      o = (o[seg] !== null && typeof o[seg] === "object" && !Array.isArray(o[seg])) ? o[seg] : null;
    }
    if (o) o[segs[segs.length - 1]] = cur;
  }
  return out;
}

export default async (msg, ctx) => {
  connect(ctx.config);
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
    if (msg.method === "GET" && qs.get("$select") !== null) {
      // Projected listing (native `list-records` pushdown): one `find` with
      // the projection/sort/skip/limit pushed to the server, plus a `count`
      // for the unpaged total. The record key (`_id`) is appended ascending
      // as the final sort key — the contract's key tiebreak. Sort-semantics
      // deviation vs. the host fallback (documented in the README): MongoDB
      // collates missing and null together and brackets mixed types its own
      // way; the contract only pins homogeneous scalar sort fields.
      const fields = qs.get("$select").split(",").map((s) => s.trim()).filter(Boolean);
      const projection = { _id: 1 };
      for (const f of fields) projection[f] = 1;
      const sort = {};
      for (const k of (qs.get("$sort") || "").split(",").map((s) => s.trim()).filter(Boolean)) {
        if (k.startsWith("-")) sort[k.slice(1)] = -1; else sort[k] = 1;
      }
      if (!("_id" in sort)) sort._id = 1;
      const total = command({ count: dataset }).n || 0;
      const found = command({ find: dataset, filter: {}, projection, sort, skip, limit: take });
      const entries = (found.cursor.firstBatch || []).map((d) => ({
        name: String(d._id), dir: false, contentType: "application/json", fields: pick(d, fields),
      }));
      return { status: 200, body: { path: "/" + dataset + "/", entries, total } };
    }
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
