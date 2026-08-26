// The guest shim (cloudflare.md §E.2/§E.3): wraps a deployed bundle as a
// Dynamic Worker and exposes the host contract as `ctx` with every member
// that was a blocking op in V8 now a Promise (the declared `guest-async`
// facet). The globals (`Buffer`, `global`, `process`, `RS2Socket`, the
// wrapped `fetch`, console routing) live in `guest-globals.js`, imported
// FIRST so they are installed before the bundle's module scope evaluates
// — import order is evaluation order for a module graph, and the Rust
// prelude installs its globals before the bundle too.
//
// This file is embedded as text into the Worker (`npm run build:shim` →
// `guest-shim.bundled.ts`). It must stay a self-contained ESM module: its
// only imports are the platform modules, `./globals.js` (this shim's own
// globals module) and the deployed bundle, which the loader supplies as
// `bundle.js`.

import { WorkerEntrypoint } from "cloudflare:workers";
import { invocationContext, rethrow } from "./globals.js";
import * as user from "./bundle.js";

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

  /// The bundle's `features` export (`export const features = […]`), read
  /// over RPC — the Worker analogue of Rust's read-at-spawn handshake by
  /// which a resident adapter advertises native capabilities like
  /// `"list-records"` (loadable-adapters.md).
  async features() {
    return features;
  }

  /// Called by the host via RPC per invocation (§E.3, decision 10). A
  /// handler throw returns as a `__rs2_guest_throw` marker rather than an
  /// RPC rejection, so the failure text survives the boundary verbatim
  /// (platform kills — CPU/OOM — still reject). The handler runs inside
  /// `invocationContext` so the ambient surfaces (fetch, console, sockets)
  /// attribute to THIS invocation however many others share the isolate.
  async invoke(msg, config, invocationId) {
    const rs2 = this.env ? this.env.RS2 : undefined;
    try {
      const h = typeof user.default === "function" ? user.default : user.default && user.default.handle;
      if (typeof h !== "function") {
        throw new Error("default export must be a function or { handle }");
      }
      const out = await invocationContext.run({ rs2, invocationId }, () => h(msg, makeCtx(rs2, config, invocationId)));
      return normalizeEnvelope(out);
    } catch (e) {
      return { __rs2_guest_throw: true, message: e && e.message !== undefined ? String(e.message) : String(e) };
    }
  }
}

export default Rs2Guest;
