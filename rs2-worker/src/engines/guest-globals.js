// The guest shim's globals module (cloudflare.md §E.2). Evaluated BEFORE the
// deployed bundle: `guest-shim.js` imports this module first and the bundle
// second, so by the time the bundle's module scope runs, `Buffer`, `global`,
// `process`, `RS2Socket` exist and `globalThis.fetch` is already the wrapped
// fetch — the same ordering as the Rust prelude, where the globals are
// installed before the bundle is evaluated. A bundle that captures `fetch`
// at module scope therefore captures the wrapper, and its egress is still
// attributed and gated.
//
// The platform already provides spec-correct web APIs, so this module does
// NOT shadow platform globals — it adds only what the platform lacks, wraps
// `fetch` (error identity + invocation stamping; egress still routes through
// `globalOutbound`), and routes `console.*` to the host log.
//
// Invocation attribution: the surfaces with no per-call handle (the fetch
// wrapper, console routing, `RS2Socket.connect`) find their invocation via
// an `AsyncLocalStorage` (`invocationContext`, loader flag `nodejs_als`).
// Concurrent invocations of one isolate interleave freely; each async chain
// carries its own `{rs2, invocationId}`, so egress and logs are attributed
// to the invocation that issued them — never to whichever invocation
// happened to enter last. Work that outlives its invocation (a stray timer)
// keeps the finished id, which the host no longer knows: denied.
//
// This file is embedded as text into the Worker (`npm run build:shim` →
// `guest-shim.bundled.ts`). It must stay a self-contained ESM module whose
// only imports are platform modules.

import { AsyncLocalStorage } from "node:async_hooks";

export const invocationContext = new AsyncLocalStorage();

const current = () => invocationContext.getStore() ?? null;

export const rethrow = (r) => {
  if (r && typeof r === "object" && r.__rs2_error === true) {
    const e = new Error(r.message);
    e.code = r.code;
    e.status = r.status;
    throw e;
  }
  return r;
};

// ---- shim-provided globals (platform gaps only) ---------------------------

function b64ToBytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function b64FromBytes(bytes) {
  let out = "";
  for (const b of bytes) out += String.fromCharCode(b);
  return btoa(out);
}

/// The prelude's Node `Buffer` subset (js_prelude.js), byte-identical API.
class Buffer extends Uint8Array {
  static isBuffer(v) {
    return v instanceof Buffer;
  }
  static from(value, encoding) {
    if (typeof value === "string") {
      if (encoding === "base64") return new Buffer(b64ToBytes(value));
      if (encoding === "hex") {
        const out = new Uint8Array(value.length / 2);
        for (let i = 0; i < out.length; i++) out[i] = parseInt(value.substr(i * 2, 2), 16);
        return new Buffer(out);
      }
      return new Buffer(new TextEncoder().encode(value));
    }
    return new Buffer(value);
  }
  static alloc(n) {
    return new Buffer(n);
  }
  static concat(list) {
    const total = list.reduce((n, b) => n + b.length, 0);
    const out = new Buffer(total);
    let offset = 0;
    for (const b of list) {
      out.set(b, offset);
      offset += b.length;
    }
    return out;
  }
  static byteLength(str) {
    return new TextEncoder().encode(String(str)).length;
  }
  toString(encoding = "utf8") {
    if (encoding === "base64") return b64FromBytes(this);
    if (encoding === "hex") return [...this].map((b) => b.toString(16).padStart(2, "0")).join("");
    return new TextDecoder().decode(this);
  }
}

/// Gated raw socket for non-HTTP wire protocols (the `socket` grant). Same
/// surface as the Rust prelude's RS2Socket, but async (guest-async facet):
/// `await RS2Socket.connect(host, port, {tls})`, `await s.write(...)`,
/// `await s.read(max)`, `await s.close()`. The allowlist is enforced
/// host-side before the platform socket opens.
class RS2Socket {
  constructor(socket) {
    this._socket = socket;
    this._reader = socket.readable.getReader();
    this._writer = socket.writable.getWriter();
    this._leftover = null;
    // I/O objects are request-scoped on this platform: a socket created in
    // one invocation cannot be used from another (the attempt does not
    // fail fast — it hangs until the runtime kills the worker). Record the
    // owning invocation and fail deterministically instead, so an adapter
    // pooling a socket in module scope can catch, reconnect, and retry —
    // the documented host difference vs. Rust's resident isolates.
    const c = current();
    this._owner = c ? c.invocationId : null;
  }
  _assertOwned() {
    const c = current();
    if (this._owner !== null && (!c || c.invocationId !== this._owner)) {
      throw new Error(
        "RS2Socket belongs to a previous invocation (I/O objects are request-scoped on this host) — reconnect",
      );
    }
  }
  static async connect(host, port, opts) {
    const tls = !!(opts && opts.tls);
    const c = current();
    if (!c || !c.rs2) {
      const e = new Error(`capability 'socket ${host}:${port | 0}' is not granted to this service`);
      e.code = "capability_denied";
      e.status = 403;
      throw e;
    }
    rethrow(await c.rs2.socketCheck(c.invocationId, String(host), port | 0, tls));
    const { connect } = await import("cloudflare:sockets");
    const socket = connect(
      { hostname: String(host), port: port | 0 },
      { secureTransport: tls ? "on" : "off", allowHalfOpen: false },
    );
    return new RS2Socket(socket);
  }
  async write(data) {
    this._assertOwned();
    const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
    await this._writer.write(bytes);
  }
  async read(max) {
    this._assertOwned();
    const n = (max | 0) || 65536;
    if (this._leftover && this._leftover.byteLength > 0) {
      const take = this._leftover.subarray(0, n);
      this._leftover = this._leftover.byteLength > n ? this._leftover.subarray(n) : null;
      return take;
    }
    const { value, done } = await this._reader.read();
    if (done || !value || value.byteLength === 0) return null; // EOF
    if (value.byteLength > n) {
      this._leftover = value.subarray(n);
      return value.subarray(0, n);
    }
    return value;
  }
  async close() {
    const c = current();
    if (this._owner !== null && (!c || c.invocationId !== this._owner)) {
      return; // another invocation's socket: nothing this context can do
    }
    try {
      await this._socket.close();
    } catch {
      /* already closed */
    }
  }
}

function installGlobals() {
  if (globalThis.Buffer === undefined) globalThis.Buffer = Buffer;
  if (globalThis.global === undefined) globalThis.global = globalThis;
  if (globalThis.process === undefined) {
    globalThis.process = {
      env: {},
      nextTick: (fn, ...args) => queueMicrotask(() => fn(...args)),
      version: "v22.12.0",
      versions: { node: "22.12.0" },
      platform: "rs2",
    };
  }
  globalThis.RS2Socket = RS2Socket;

  // fetch is wrapped, not replaced: the wrapper calls the platform fetch,
  // which `globalOutbound` routes to the host gateway (§E.4). The wrapper
  // stamps the invocation id for the gateway and rethrows a host error
  // marker response as an Error carrying `.code`/`.status`.
  const platformFetch = globalThis.fetch.bind(globalThis);
  globalThis.fetch = async (input, init) => {
    const req = new Request(input, init);
    const c = current();
    if (c) req.headers.set("x-rs2-invocation", c.invocationId);
    else req.headers.delete("x-rs2-invocation");
    const resp = await platformFetch(req);
    if (resp.headers.get("x-rs2-guest-error") === "1") {
      rethrow(await resp.json());
    }
    return resp;
  };

  // console.* additionally routes to the host log (fire-and-forget RPC).
  const stringify = (args) =>
    args
      .map((a) => {
        if (typeof a === "string") return a;
        try {
          return JSON.stringify(a);
        } catch {
          return String(a);
        }
      })
      .join(" ");
  for (const [method, level] of [
    ["debug", "debug"],
    ["log", "info"],
    ["info", "info"],
    ["warn", "warn"],
    ["error", "error"],
  ]) {
    const original = console[method] ? console[method].bind(console) : () => {};
    console[method] = (...args) => {
      original(...args);
      const c = current();
      if (c && c.rs2) {
        try {
          void c.rs2.log(c.invocationId, level, stringify(args));
        } catch {
          /* log must never fail the guest */
        }
      }
    };
  }
}

installGlobals();
