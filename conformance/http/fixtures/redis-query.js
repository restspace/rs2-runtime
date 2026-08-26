// Redis (RESP) `QueryStore` adapter — the guest-adapter conformance fixture
// (cloudflare.md P4b), extracted from `rs2-core/tests/guest_adapter.rs` and
// embedded back into it via `include_str!`. Executes a stored query
// (`POST /query` with `{query, params, take, skip}`) by scanning the
// dataset over the RESP client and filtering by the substituted `where`
// clause — query push-down to the backend, over the mount's own pooled
// connection.
//
// The RESP client section is duplicated verbatim from `redis-data.js`
// (single-file ESM bundles, no build step) — keep them in sync by hand.

// ---- RESP client (shared verbatim with redis-data.js) ----------------------
let SOCK = null;
let RBUF = new Uint8Array(0);
let LOCK = Promise.resolve();
let CONF = {};

function locked(fn) {
  const run = LOCK.then(fn);
  LOCK = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}
// Remember the connection config; the socket itself opens lazily inside
// `exchange` so a dead pooled socket can be replaced.
async function connect(config) {
  CONF = config;
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
      try {
        if (SOCK) await SOCK.close();
      } catch {}
      SOCK = null;
      await ensureConnected();
      return await fn();
    }
  });
}
function append(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}
async function fill() {
  const chunk = await SOCK.read(65536);
  if (chunk === null) throw new Error("redis: connection closed");
  RBUF = append(RBUF, chunk);
}
async function readLine() {
  for (;;) {
    for (let i = 0; i + 1 < RBUF.length; i++) {
      if (RBUF[i] === 13 && RBUF[i + 1] === 10) {
        const line = new TextDecoder().decode(RBUF.slice(0, i));
        RBUF = RBUF.slice(i + 2);
        return line;
      }
    }
    await fill();
  }
}
async function readN(n) {
  while (RBUF.length < n) await fill();
  const out = RBUF.slice(0, n);
  RBUF = RBUF.slice(n);
  return out;
}
async function readReply() {
  const line = await readLine();
  const t = line[0],
    rest = line.slice(1);
  if (t === "+") return rest;
  if (t === "-") throw new Error("redis: " + rest);
  if (t === ":") return parseInt(rest, 10);
  if (t === "$") {
    const len = parseInt(rest, 10);
    if (len < 0) return null;
    const data = await readN(len);
    await readN(2);
    return new TextDecoder().decode(data);
  }
  if (t === "*") {
    const count = parseInt(rest, 10);
    if (count < 0) return null;
    const arr = [];
    for (let k = 0; k < count; k++) arr.push(await readReply());
    return arr;
  }
  throw new Error("redis: bad reply " + line);
}
async function cmd(...args) {
  return exchange(async () => {
    let s = "*" + args.length + "\r\n";
    for (const a of args) {
      const str = String(a);
      s += "$" + new TextEncoder().encode(str).length + "\r\n" + str + "\r\n";
    }
    await SOCK.write(s);
    return readReply();
  });
}
// ---- end RESP client -------------------------------------------------------

function keep(record, where) {
  return Object.entries(where).every(([field, clause]) => {
    if (clause && typeof clause === "object" && clause.op) {
      const a = record[field],
        b = clause.value;
      switch (clause.op) {
        case ">=":
          return a >= b;
        case ">":
          return a > b;
        case "<=":
          return a <= b;
        case "<":
          return a < b;
        case "!=":
          return a !== b;
        default:
          return a === b;
      }
    }
    return record[field] === clause;
  });
}

export default async (msg, ctx) => {
  await connect(ctx.config);
  const { query, take, skip } = msg.body;
  const dataset = query.dataset;
  const where = query.where || {};
  const keys = (await cmd("KEYS", dataset + ":*")) || [];
  let rows = [];
  for (const k of keys) {
    const name = k.slice(dataset.length + 1);
    if (name === ".schema.json") continue;
    const rec = JSON.parse(await cmd("GET", k));
    rec._key = name;
    if (keep(rec, where)) rows.push(rec);
  }
  if (query.orderBy) rows.sort((a, b) => (a[query.orderBy] > b[query.orderBy] ? 1 : -1));
  const total = rows.length;
  return { status: 200, body: { rows: rows.slice(skip, skip + take), total } };
};
