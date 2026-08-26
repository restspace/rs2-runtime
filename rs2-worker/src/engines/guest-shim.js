// The guest shim (cloudflare.md §E.2/§E.3): wraps a deployed bundle as a
// Dynamic Worker. The platform already provides spec-correct web APIs, so
// the shim does NOT shadow platform globals — it adds only what the
// platform lacks (`Buffer`, `global`, `process`, `RS2Socket`), wraps
// `fetch` (error identity + invocation stamping; egress still routes
// through `globalOutbound`), routes `console.*` to the host log, and
// exposes the host contract as `ctx` with every member that was a blocking
// op in V8 now a Promise (the declared `guest-async` facet).
//
// This file is embedded as text into the Worker (`npm run build:shim` →
// `guest-shim.bundled.ts`). It must stay a single self-contained ESM
// module: its only imports are the platform modules and the deployed
// bundle, which the loader supplies as `bundle.js`.

import { WorkerEntrypoint } from "cloudflare:workers";
import * as user from "./bundle.js";

// Ambient invocation context for the surfaces that have no per-call handle
// (the global fetch wrapper, console routing, RS2Socket). Set at `invoke`
// entry; concurrent invocations of the same isolate can interleave, in
// which case egress attribution may cross invocations of this same bundle
// (same code, same tenant — grants are looked up per invocation host-side).
let current = null;

const rethrow = (r) => {
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
  }
  static async connect(host, port, opts) {
    const tls = !!(opts && opts.tls);
    const c = current;
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
    const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
    await this._writer.write(bytes);
  }
  async read(max) {
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
    if (current) req.headers.set("x-rs2-invocation", current.invocationId);
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
      const c = current;
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

// ---- the guest ctx (js_prelude `__rs2_dispatch` shape, members async) -----

function makeCtx(rs2, config, invocationId) {
  const hostCall = async (fn) => {
    if (!rs2) {
      // No host binding (the `template` sandbox): default deny, like
      // `GrantedHost::deny_all`.
      const e = new Error("capability is not granted to this service");
      e.code = "capability_denied";
      e.status = 403;
      throw e;
    }
    return rethrow(await fn());
  };
  const ctx = {
    config,
    request: async (cap, req) => {
      if (!rs2) {
        const e = new Error(`capability '${String(cap)}' is not granted to this service`);
        e.code = "capability_denied";
        e.status = 403;
        throw e;
      }
      return rethrow(await rs2.request(invocationId, String(cap), req ?? {}));
    },
    log: (level, text) => {
      if (rs2) void rs2.log(invocationId, String(level), String(text));
    },
    state: {
      get: async (k) => (rs2 ? rethrow(await rs2.stateGet(invocationId, String(k))) : null),
      put: async (k, v) => {
        if (rs2) rethrow(await rs2.statePut(invocationId, String(k), String(v)));
      },
    },
    // Request streaming: pull the body chunk-by-chunk. `null` at EOF.
    readBody: () =>
      hostCall(async () => {
        const r = rethrow(await rs2.bodyRead(invocationId));
        if (r === null || r === undefined) return null; // EOF
        return r.data;
      }),
    // Async-iterable sugar over `readBody`.
    body: () => ({
      async *[Symbol.asyncIterator]() {
        for (;;) {
          const chunk = await ctx.readBody();
          if (chunk === null) return;
          yield chunk;
        }
      },
    }),
    // Response streaming: declare status + headers, then push the body.
    beginStream: async (envelope) => {
      await hostCall(async () => rethrow(await rs2.streamBegin(invocationId, envelope ?? {})));
      return {
        write: async (data) => {
          const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
          rethrow(await rs2.bodyWrite(invocationId, bytes));
        },
        end: () => {}, // the stream closes when the handler returns
      };
    },
  };
  return ctx;
}

/// `engines/js.rs` `normalize_envelope`: a bare value (not a
/// `{ status?, body? }` envelope) is the 200 JSON body; null/undefined → 204.
function normalizeEnvelope(value) {
  if (value === null || value === undefined) return { status: 204 };
  if (
    typeof value === "object" &&
    !Array.isArray(value) &&
    (value.status !== undefined || value.body !== undefined)
  ) {
    return value;
  }
  return { status: 200, body: value };
}

export const features = Array.isArray(user.features) ? user.features : [];

export class Rs2Guest extends WorkerEntrypoint {
  /// Deploy-time compile check: reaching this method proves the bundle's
  /// module graph evaluated. Mirrors Rust `JsEngine::compile_check`, which
  /// loads + evaluates only (no default-export shape check).
  async check() {
    return true;
  }

  /// Called by the host via RPC per invocation (§E.3, decision 10). A
  /// handler throw returns as a `__rs2_guest_throw` marker rather than an
  /// RPC rejection, so the failure text survives the boundary verbatim
  /// (platform kills — CPU/OOM — still reject).
  async invoke(msg, config, invocationId) {
    const rs2 = this.env ? this.env.RS2 : undefined;
    current = { rs2, invocationId };
    try {
      const ctx = makeCtx(rs2, config, invocationId);
      const h = typeof user.default === "function" ? user.default : user.default && user.default.handle;
      if (typeof h !== "function") {
        throw new Error("default export must be a function or { handle }");
      }
      const out = await h(msg, ctx);
      return normalizeEnvelope(out);
    } catch (e) {
      return { __rs2_guest_throw: true, message: e && e.message !== undefined ? String(e.message) : String(e) };
    }
  }
}

export default Rs2Guest;
