// A tiny in-process RESP (Redis) server for the guest-adapter conformance
// suite — the Node port of the mock in `rs2-core/tests/guest_adapter.rs`.
// Supports the subset the fixture adapters use (SET/GET/DEL/EXISTS/KEYS)
// over an in-memory map, and counts accepted connections so a test can
// observe pooling (one connection per resident adapter mount).
//
// Bind: 127.0.0.1 by default; port 0 picks an ephemeral port (the caller
// reads it back and writes it into the mount's store config). Under local
// `wrangler dev` the guest's `cloudflare:sockets` connect() reaches
// 127.0.0.1 directly — see conformance/http/README.md.

import net from "node:net";

/**
 * Start the mock. Returns `{ port, connections(), close() }`.
 * @param {{ port?: number, host?: string, delayMs?: number }} [opts]
 */
export async function startMockRedis(opts = {}) {
  const host = opts.host ?? "127.0.0.1";
  const delayMs = opts.delayMs ?? 0;
  /** @type {Map<string, string>} */
  const store = new Map();
  let connections = 0;
  const sockets = new Set();

  const server = net.createServer((sock) => {
    connections += 1;
    sockets.add(sock);
    sock.on("close", () => sockets.delete(sock));
    sock.on("error", () => undefined);
    let buf = Buffer.alloc(0);
    let queue = Promise.resolve();
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        const parsed = parseCommand(buf);
        if (!parsed) break;
        buf = parsed.rest;
        const args = parsed.args;
        // Replies stay in arrival order; the optional delay makes a call
        // occupy its connection long enough for concurrency tests.
        queue = queue.then(async () => {
          const reply = exec(args, store);
          if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
          if (!sock.destroyed) sock.write(reply);
        });
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
    connections: () => connections,
    close: () =>
      new Promise((resolve) => {
        for (const s of sockets) s.destroy();
        server.close(() => resolve(undefined));
      }),
  };
}

/** Parse one `*N\r\n$len\r\n<data>\r\n…` command; null when incomplete. */
function parseCommand(buf) {
  let off = 0;
  const line = () => {
    const idx = buf.indexOf("\r\n", off);
    if (idx < 0) return null;
    const s = buf.toString("utf8", off, idx);
    off = idx + 2;
    return s;
  };
  const header = line();
  if (header === null || !header.startsWith("*")) return null;
  const n = parseInt(header.slice(1), 10);
  if (!Number.isInteger(n) || n < 0) return null;
  const args = [];
  for (let i = 0; i < n; i++) {
    const lenLine = line();
    if (lenLine === null || !lenLine.startsWith("$")) return null;
    const len = parseInt(lenLine.slice(1), 10);
    if (buf.length < off + len + 2) return null;
    args.push(buf.toString("utf8", off, off + len));
    off += len + 2;
  }
  return { args, rest: buf.subarray(off) };
}

function bulk(s) {
  return Buffer.from(`$${Buffer.byteLength(s)}\r\n${s}\r\n`);
}

function exec(args, store) {
  switch ((args[0] ?? "").toUpperCase()) {
    case "SET":
      store.set(args[1], args[2]);
      return Buffer.from("+OK\r\n");
    case "GET": {
      const v = store.get(args[1]);
      return v === undefined ? Buffer.from("$-1\r\n") : bulk(v);
    }
    case "DEL":
      return Buffer.from(`:${store.delete(args[1]) ? 1 : 0}\r\n`);
    case "EXISTS":
      return Buffer.from(`:${store.has(args[1]) ? 1 : 0}\r\n`);
    case "KEYS": {
      const prefix = args[1].endsWith("*") ? args[1].slice(0, -1) : args[1];
      const matched = [...store.keys()].filter((k) => k.startsWith(prefix)).sort();
      return Buffer.concat([Buffer.from(`*${matched.length}\r\n`), ...matched.map(bulk)]);
    }
    default:
      return Buffer.from(`-ERR unknown command '${args[0]}'\r\n`);
  }
}
