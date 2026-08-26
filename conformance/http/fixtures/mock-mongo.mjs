// A minimal in-process MongoDB server for the guest-adapter conformance
// suite — the Node port of the mock in `rs2-core/tests/guest_adapter.rs`:
// OP_MSG framing + a hand-rolled BSON subset (doc/array/string/int32/int64/
// double/bool/null, plus UTC datetime and ObjectId via the extended-JSON
// sentinels `{"$date": millis}` / `{"$oid": "24-hex"}`) over an in-memory
// map, and the command subset the shipped adapters use
// (find/count/update/delete/drop/listCollections/aggregate).
//
// The `find` sort is applied server-side with the **listing contract's**
// comparator (missing < null < false < true < number < string < array <
// object; strings by UTF-8 bytes) in the wire's key-priority order — a
// native-pushdown test is only meaningful if the mock actually sorts. The
// fixtures use homogeneous scalar sort fields only, so this comparator
// stands in for MongoDB's collation, exactly as in the Rust mock.

import net from "node:net";

/**
 * Start the mock. Returns `{ port, store, close() }`; `store` is the
 * backing `Map<collection, Map<_id, doc>>`, exposed so a test can pre-seed
 * wire types (dates, ObjectIds) that JSON PUTs can't produce.
 * @param {{ port?: number, host?: string }} [opts]
 */
export async function startMockMongo(opts = {}) {
  const host = opts.host ?? "127.0.0.1";
  /** @type {Map<string, Map<string, Record<string, unknown>>>} */
  const store = new Map();
  const sockets = new Set();

  const server = net.createServer((sock) => {
    sockets.add(sock);
    sock.on("close", () => sockets.delete(sock));
    sock.on("error", () => undefined);
    let buf = Buffer.alloc(0);
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        if (buf.length < 4) break;
        const len = buf.readInt32LE(0);
        if (buf.length < len) break;
        const frame = buf.subarray(0, len);
        buf = buf.subarray(len);
        // frame = header(16) + flagBits(4) + section kind(1) + BSON doc
        const cmd = decodeDoc(frame, 21)[0];
        const reply = dispatch(cmd, store);
        const body = encodeDoc(reply);
        const out = Buffer.alloc(21 + body.length);
        out.writeInt32LE(21 + body.length, 0);
        out.writeInt32LE(0, 4); // requestID
        out.writeInt32LE(0, 8); // responseTo
        out.writeInt32LE(2013, 12); // OP_MSG
        out.writeUInt32LE(0, 16); // flagBits
        out[20] = 0; // section kind 0
        body.copy(out, 21);
        if (!sock.destroyed) sock.write(out);
      }
    });
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(opts.port ?? 0, host, () => resolve(undefined));
  });
  const port = server.address().port;
  return {
    port,
    store,
    close: () =>
      new Promise((resolve) => {
        for (const s of sockets) s.destroy();
        server.close(() => resolve(undefined));
      }),
  };
}

// ---- BSON subset (mirrors the adapters' JS codec + the Rust mock) ----------

function encodeDoc(obj) {
  const parts = [];
  for (const [k, v] of Object.entries(obj)) {
    const name = Buffer.concat([Buffer.from(k, "utf8"), Buffer.from([0])]);
    if (v === null || v === undefined) {
      parts.push(Buffer.from([0x0a]), name);
    } else if (typeof v === "boolean") {
      parts.push(Buffer.from([0x08]), name, Buffer.from([v ? 1 : 0]));
    } else if (typeof v === "number") {
      if (!Number.isInteger(v)) {
        const b = Buffer.alloc(8);
        b.writeDoubleLE(v, 0);
        parts.push(Buffer.from([0x01]), name, b);
      } else if (v >= -2147483648 && v <= 2147483647) {
        const b = Buffer.alloc(4);
        b.writeInt32LE(v, 0);
        parts.push(Buffer.from([0x10]), name, b);
      } else {
        const b = Buffer.alloc(8);
        b.writeBigInt64LE(BigInt(v), 0);
        parts.push(Buffer.from([0x12]), name, b);
      }
    } else if (typeof v === "string") {
      const s = Buffer.from(v, "utf8");
      const len = Buffer.alloc(4);
      len.writeInt32LE(s.length + 1, 0);
      parts.push(Buffer.from([0x02]), name, len, s, Buffer.from([0]));
    } else if (Array.isArray(v)) {
      const o = {};
      v.forEach((x, i) => (o[i] = x));
      parts.push(Buffer.from([0x04]), name, encodeDoc(o));
    } else if (typeof v === "object") {
      // Sentinels for the non-JSON wire types.
      const keys = Object.keys(v);
      if (keys.length === 1 && keys[0] === "$date" && Number.isInteger(v.$date)) {
        const b = Buffer.alloc(8);
        b.writeBigInt64LE(BigInt(v.$date), 0);
        parts.push(Buffer.from([0x09]), name, b);
      } else if (keys.length === 1 && keys[0] === "$oid" && typeof v.$oid === "string") {
        parts.push(Buffer.from([0x07]), name, Buffer.from(v.$oid, "hex"));
      } else {
        parts.push(Buffer.from([0x03]), name, encodeDoc(v));
      }
    } else {
      throw new Error(`mock mongod: cannot encode ${typeof v}`);
    }
  }
  const body = Buffer.concat(parts);
  const out = Buffer.alloc(body.length + 5);
  out.writeInt32LE(body.length + 5, 0);
  body.copy(out, 4);
  out[out.length - 1] = 0;
  return out;
}

/** Decode one document; plain objects keep wire key order (used for sort
 *  key priority). Returns `[doc, endOffset]`. */
function decodeDoc(b, start) {
  const len = b.readInt32LE(start);
  const end = start + len;
  let off = start + 4;
  const obj = {};
  while (off < end - 1) {
    const type = b[off++];
    const ns = off;
    while (b[off] !== 0) off++;
    const name = b.toString("utf8", ns, off);
    off++;
    let val;
    if (type === 0x01) {
      val = b.readDoubleLE(off);
      off += 8;
    } else if (type === 0x02) {
      const l = b.readInt32LE(off);
      off += 4;
      val = b.toString("utf8", off, off + l - 1);
      off += l;
    } else if (type === 0x03) {
      const r = decodeDoc(b, off);
      val = r[0];
      off = r[1];
    } else if (type === 0x04) {
      const r = decodeDoc(b, off);
      val = Object.values(r[0]);
      off = r[1];
    } else if (type === 0x08) {
      val = b[off++] !== 0;
    } else if (type === 0x0a) {
      val = null;
    } else if (type === 0x10) {
      val = b.readInt32LE(off);
      off += 4;
    } else if (type === 0x12) {
      val = Number(b.readBigInt64LE(off));
      off += 8;
    } else if (type === 0x09) {
      val = { $date: Number(b.readBigInt64LE(off)) };
      off += 8;
    } else if (type === 0x07) {
      val = { $oid: b.toString("hex", off, off + 12) };
      off += 12;
    } else {
      throw new Error(`mock mongod: unsupported bson type 0x${type.toString(16)}`);
    }
    obj[name] = val;
  }
  return [obj, end];
}

// ---- the listing contract's comparator (rs2-core/src/listing.rs) -----------

function rank(v) {
  if (v === undefined) return 0;
  if (v === null) return 1;
  if (v === false) return 2;
  if (v === true) return 3;
  if (typeof v === "number") return 4;
  if (typeof v === "string") return 5;
  if (Array.isArray(v)) return 6;
  return 7;
}

function compareOptional(a, b) {
  const ra = rank(a),
    rb = rank(b);
  if (ra !== rb) return ra < rb ? -1 : 1;
  if (typeof a === "number" && typeof b === "number") return a < b ? -1 : a > b ? 1 : 0;
  if (typeof a === "string" && typeof b === "string") {
    return Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
  }
  if (ra === 6 || ra === 7) {
    const sa = JSON.stringify(a),
      sb = JSON.stringify(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  }
  return 0;
}

function lookup(doc, dotted) {
  let cur = doc;
  for (const seg of dotted.split(".")) {
    if (cur === null || typeof cur !== "object" || Array.isArray(cur) || !(seg in cur)) return undefined;
    cur = cur[seg];
  }
  return cur;
}

/** Inclusion projection over dotted paths, keeping `_id` and omitting
 *  absent paths entirely (the contract's shape, as in the Rust mock). */
function project(doc, paths) {
  const out = {};
  for (const p of paths) {
    const v = lookup(doc, p);
    if (v === undefined) continue;
    const segs = p.split(".");
    let o = out;
    for (let i = 0; i < segs.length - 1; i++) {
      if (!(segs[i] in o)) o[segs[i]] = {};
      o = o[segs[i]];
    }
    o[segs[segs.length - 1]] = v;
  }
  if ("_id" in doc) out._id = doc._id;
  return out;
}

// ---- command dispatch (mirrors `mongo_dispatch` + `run_pipeline`) ----------

function coll(store, name) {
  let m = store.get(name);
  if (!m) {
    m = new Map();
    store.set(name, m);
  }
  return m;
}

function dispatch(cmd, store) {
  const cursorReply = (batch, ns) => ({ ok: 1.0, cursor: { firstBatch: batch, id: 0, ns } });

  if (typeof cmd.find === "string") {
    const name = cmd.find;
    const map = store.get(name);
    const skip = Number.isInteger(cmd.skip) ? cmd.skip : 0;
    const limit = Number.isInteger(cmd.limit) && cmd.limit > 0 ? cmd.limit : Infinity;
    const wantId = cmd.filter && typeof cmd.filter === "object" ? cmd.filter._id : undefined;
    let batch = [];
    if (map) {
      if (typeof wantId === "string") {
        const doc = map.get(wantId);
        if (doc) batch = [doc];
      } else {
        let docs = [...map.values()];
        // Sort server-side, in the wire's key-priority order (plain JS
        // objects keep insertion order for string keys).
        if (cmd.sort && typeof cmd.sort === "object") {
          const keys = Object.keys(cmd.sort).map((k) => [k, cmd.sort[k]]);
          docs.sort((a, b) => {
            for (const [path, dir] of keys) {
              let ord = compareOptional(lookup(a, path), lookup(b, path));
              if (dir < 0) ord = -ord;
              if (ord !== 0) return ord;
            }
            return 0;
          });
        }
        docs = docs.slice(skip, limit === Infinity ? undefined : skip + limit);
        if (cmd.projection && typeof cmd.projection === "object") {
          const paths = Object.keys(cmd.projection).filter((k) => k !== "_id");
          docs = docs.map((d) => project(d, paths));
        }
        batch = docs;
      }
    }
    return cursorReply(batch, `test.${name}`);
  }
  if (typeof cmd.aggregate === "string") {
    const docs = store.has(cmd.aggregate) ? [...store.get(cmd.aggregate).values()] : [];
    const batch = runPipeline(docs, Array.isArray(cmd.pipeline) ? cmd.pipeline : []);
    return cursorReply(batch, `test.${cmd.aggregate}`);
  }
  if (typeof cmd.count === "string") {
    return { ok: 1.0, n: store.has(cmd.count) ? store.get(cmd.count).size : 0 };
  }
  if (typeof cmd.update === "string") {
    const map = coll(store, cmd.update);
    const updates = Array.isArray(cmd.updates) ? cmd.updates : [];
    let n = 0,
      nModified = 0;
    const upserted = [];
    updates.forEach((u, i) => {
      const id = u && u.q && typeof u.q._id === "string" ? u.q._id : "";
      const existed = map.has(id);
      map.set(id, u.u ?? null);
      n += 1;
      if (existed) nModified += 1;
      else upserted.push({ index: i, _id: id });
    });
    const reply = { ok: 1.0, n, nModified };
    if (upserted.length > 0) reply.upserted = upserted;
    return reply;
  }
  if (typeof cmd.delete === "string") {
    const map = store.get(cmd.delete);
    let n = 0;
    for (const d of Array.isArray(cmd.deletes) ? cmd.deletes : []) {
      const id = d && d.q && typeof d.q._id === "string" ? d.q._id : undefined;
      if (map && id !== undefined && map.delete(id)) n += 1;
    }
    return { ok: 1.0, n };
  }
  if (typeof cmd.drop === "string") {
    store.delete(cmd.drop);
    return { ok: 1.0 };
  }
  if (cmd.listCollections !== undefined) {
    const batch = [...store.keys()].map((name) => ({ name, type: "collection" }));
    return cursorReply(batch, "test.$cmd.listCollections");
  }
  return { ok: 1.0 }; // hello / ping / unknown
}

/** The aggregation-stage subset the query adapter produces: `$match`
 *  (top-level equality), `$sort` (single field, 1/-1), `$skip`/`$limit`/
 *  `$count`, and the paging `$facet`. */
function runPipeline(docs, stages) {
  let out = docs.slice();
  for (const stage of stages) {
    const op = Object.keys(stage)[0];
    const arg = stage[op];
    if (op === "$match") {
      out = out.filter((d) => Object.entries(arg).every(([f, want]) => deepEq(d[f], want)));
    } else if (op === "$sort") {
      const [field, dir] = Object.entries(arg)[0];
      const asc = !(typeof dir === "number" && dir < 0);
      out.sort((a, b) => {
        const ord = simpleCmp(a[field], b[field]);
        return asc ? ord : -ord;
      });
    } else if (op === "$skip") {
      out = out.slice(Math.min(arg | 0, out.length));
    } else if (op === "$limit") {
      out = out.slice(0, arg | 0);
    } else if (op === "$count") {
      out = [{ [typeof arg === "string" ? arg : "n"]: out.length }];
    } else if (op === "$facet") {
      const facet = {};
      for (const [k, sub] of Object.entries(arg)) facet[k] = runPipeline(out, sub);
      out = [facet];
    } else {
      throw new Error(`mock mongod: unsupported pipeline stage ${op}`);
    }
  }
  return out;
}

function simpleCmp(a, b) {
  if (typeof a === "string" && typeof b === "string") return a < b ? -1 : a > b ? 1 : 0;
  if (typeof a === "number" && typeof b === "number") return a < b ? -1 : a > b ? 1 : 0;
  return 0;
}

function deepEq(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}
