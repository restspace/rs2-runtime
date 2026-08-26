// Redis (RESP) `DataStore` adapter — the guest-adapter conformance fixture
// (cloudflare.md P4b), extracted from `rs2-core/tests/guest_adapter.rs`'s
// inline bundle and embedded back into it via `include_str!`, so the same
// file is held to the store contract in-process (Rust) and over HTTP (both
// hosts). A store-pattern surface (`GET/PUT/DELETE /{ds}/{key}`, container
// + root listings) over a minimal RESP client with one pooled connection in
// module scope — the resident property under test.
//
// Host-portable: every socket call is awaited (RS2Socket is synchronous on
// the Rust host and async on the Worker — awaiting a plain value is a
// no-op), and a module-level promise lock serializes command exchanges so
// concurrent invocations sharing the Worker isolate cannot interleave on
// the one socket.

// ---- RESP client (shared verbatim with redis-query.js) ---------------------
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

export default async (msg, ctx) => {
  await connect(ctx.config);
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
    const keys = (await cmd("KEYS", "*")) || [];
    const datasets = {};
    for (const k of keys) datasets[k.split(":")[0]] = true;
    const names = Object.keys(datasets).sort();
    const entries = page(names).map((n) => ({ name: n + "/", dir: true }));
    return { status: 200, body: { path: "/", entries, total: names.length } };
  }

  const dataset = segs[0];
  if (segs.length === 1 && isContainer) {
    if (msg.method === "GET") {
      const keys = (await cmd("KEYS", dataset + ":*")) || [];
      const names = keys
        .map((k) => k.slice(dataset.length + 1))
        .filter((n) => n !== ".schema.json")
        .sort();
      const entries = page(names).map((n) => ({ name: n, dir: false, contentType: "application/json" }));
      return { status: 200, body: { path: "/" + dataset + "/", entries, total: names.length } };
    }
    if (msg.method === "DELETE") {
      for (const k of (await cmd("KEYS", dataset + ":*")) || []) await cmd("DEL", k);
      return { status: 204 };
    }
    return { status: 400, body: { detail: "container supports GET, DELETE" } };
  }

  const key = segs.slice(1).join("/");
  const rkey = dataset + ":" + key;
  if (msg.method === "GET") {
    const v = await cmd("GET", rkey);
    if (v === null) return { status: 404, body: { detail: "no record '" + key + "'" } };
    return { status: 200, body: JSON.parse(v) };
  }
  if (msg.method === "PUT") {
    const existed = await cmd("EXISTS", rkey);
    await cmd("SET", rkey, JSON.stringify(msg.body));
    return { status: existed ? 200 : 201 };
  }
  if (msg.method === "DELETE") {
    if (!(await cmd("DEL", rkey))) return { status: 404, body: { detail: "no record '" + key + "'" } };
    return { status: 204 };
  }
  return { status: 400, body: { detail: "method not supported" } };
};
