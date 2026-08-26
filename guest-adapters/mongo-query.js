// MongoDB `QueryStore` adapter for RS2 — a deployable single-file ESM bundle
// executing stored aggregation queries over the pooled socket capability. The
// host's `GuestQueryStore` POSTs `{query, params, take, skip}` to `/query`;
// `query` is the stored envelope's JSON with params **already substituted
// host-side** — this adapter must not (and does not) re-substitute. The query
// shape is `{ collection: string, pipeline: [...aggregation stages] }`; it runs
// one `aggregate` command with an appended `$facet` producing the page and the
// full count, and replies `{ rows, total }`.
//
// Config (ctx.config, from the mount's `store` block): `db` (default "test"),
// `host` (default 127.0.0.1), `port`. Requires a socket grant for host:port.
// Deploy: `rs2 deploy mongo-query.js --name mongo-query` → the tenant's
// `.rs2-code/mongo-query/<version>.js`; reference as
// `"store": { "adapter": "code:mongo-query@<version>", ... }` on a query mount.
// No auth (see mongo-data.js — SCRAM needs a prelude `crypto.subtle`).

// ============================================================================
// BSON + OP_MSG wire section — KEEP IN SYNC with guest-adapters/mongo-data.js.
// Bundles are single-file ESM with no build step, so this section is
// duplicated verbatim in both adapters; edit both or they diverge.
// ============================================================================
let SOCK = null, RBUF = new Uint8Array(0), DB = "test", REQID = 0, LOCK = Promise.resolve(), CONF = {};

// Serializes connect + command exchanges: on the Worker host, concurrent
// invocations share one isolate (and the one pooled socket), so wire
// exchanges must not interleave. On the Rust host jobs are serialized
// already; the lock is a no-op there.
function locked(fn) {
  const run = LOCK.then(fn);
  LOCK = run.then(() => undefined, () => undefined);
  return run;
}

// Host-portable sockets: RS2Socket is synchronous on the Rust host and
// async on the Worker (the `guest-async` facet) — awaiting a plain value
// is a no-op, so awaiting everywhere runs on both hosts unchanged.
// `connect` only remembers the config; the socket opens lazily inside
// `exchange` so a dead pooled socket can be replaced.
async function connect(config) {
  CONF = config;
  DB = config.db || "test";
}
async function ensureConnected() {
  if (SOCK) return;
  SOCK = await RS2Socket.connect(CONF.host || "127.0.0.1", CONF.port | 0);
  RBUF = new Uint8Array(0);
}
// Run one whole command exchange under the lock. On the Rust host the
// resident isolate pools the socket for its lifetime and the retry path
// never runs; on the Worker host I/O objects are request-scoped, so a
// socket pooled in module scope dies at the invocation boundary — the
// first use then throws, and the exchange reconnects and retries once.
async function exchange(fn) {
  return locked(async () => {
    try {
      await ensureConnected();
      return await fn();
    } catch (e) {
      try { if (SOCK) await SOCK.close(); } catch {}
      SOCK = null;
      await ensureConnected();
      return await fn();
    }
  });
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
async function ensure(n) { while (RBUF.length < n) { const c = await SOCK.read(65536); if (c === null) throw new Error("mongo: closed"); RBUF = cat(RBUF, c); } }
async function command(doc) {
  doc["$db"] = DB;
  const body = encodeDoc(doc);
  const msg = new Uint8Array(21 + body.length);
  const dv = new DataView(msg.buffer);
  dv.setInt32(0, msg.length, true);
  dv.setInt32(4, ++REQID, true);
  dv.setInt32(12, 2013, true); // OP_MSG
  msg[20] = 0; // section kind 0
  msg.set(body, 21);
  return exchange(async () => {
    await SOCK.write(msg);
    await ensure(4);
    const len = new DataView(RBUF.buffer, RBUF.byteOffset, RBUF.byteLength).getInt32(0, true);
    await ensure(len);
    const reply = decodeDoc(RBUF.slice(0, len), 21)[0];
    RBUF = RBUF.slice(len);
    if (!reply.ok) throw new Error("mongo error: " + JSON.stringify(reply));
    return reply;
  });
}
// ============================================================================
// end of shared wire section
// ============================================================================

export default async (msg, ctx) => {
  await connect(ctx.config);
  const path = String(msg.url).split("?")[0];
  if (msg.method !== "POST" || path !== "/query")
    return { status: 400, body: { detail: "mongo query adapter supports POST /query" } };

  const { query, take, skip } = msg.body || {};
  if (!query || typeof query !== "object" || Array.isArray(query))
    return { status: 400, body: { detail: "query must be an object { collection, pipeline }" } };
  if (typeof query.collection !== "string" || !query.collection)
    return { status: 400, body: { detail: "query.collection must be a collection name (string)" } };
  if (!Array.isArray(query.pipeline))
    return { status: 400, body: { detail: "query.pipeline must be an aggregation pipeline (array of stages)" } };

  // One aggregate: the stored pipeline (params already substituted host-side)
  // plus a $facet paging the rows and counting the unpaged total.
  const lim = Number.isInteger(take) && take > 0 ? take : 1000;
  const pipeline = query.pipeline.concat([{
    $facet: {
      rows: [{ $skip: skip | 0 }, { $limit: lim }],
      total: [{ $count: "n" }],
    },
  }]);
  const reply = await command({ aggregate: query.collection, pipeline, cursor: {} });
  const facet = ((reply.cursor && reply.cursor.firstBatch) || [])[0] || {};
  const counted = (facet.total || [])[0];
  return { status: 200, body: { rows: facet.rows || [], total: counted ? counted.n : 0 } };
};
