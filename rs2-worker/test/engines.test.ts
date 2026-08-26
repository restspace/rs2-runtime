// The Dynamic Worker engine (cloudflare.md §E, P4 acceptance): every
// `corpus/bundles/*.js` loads under the guest shim and answers a mocked
// call with `globalOutbound` pointed at a stub — the Worker analogue of
// `rs2-core/tests/sdk_corpus.rs` — plus the engine-level contract corners
// (envelope normalization, deny-all ctx, compile check, the template
// sandbox). Build the corpus first (`cd corpus; npm install; ./build.ps1`
// or the esbuild equivalent); the corpus block self-skips when absent.

import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import {
  DynamicWorkerEngine,
  cpuMsFromConfig,
  socketPatternMatches,
} from "../src/engines/dynamic-worker";
import type { EngineHost } from "../src/engines/dynamic-worker";
import { GUEST_SHIM, GUEST_SHIM_SOURCE_HASH } from "../src/engines/guest-shim.bundled";
import shimSource from "../src/engines/guest-shim.js?raw";
import { sha256Hex } from "../src/runtime/crypto";
import { RsError } from "../src/runtime/error";

const loader = (env as { LOADER?: WorkerLoader }).LOADER;

/// Entries that cannot pass and why — part of the corpus record
/// (`sdk_corpus.rs::KNOWN_BUILD_FAILURES`).
const KNOWN_BUILD_FAILURES: Record<string, string> = {
  slack: "@slack/web-api imports node:os/node:path (axios transport); needs node-builtin shims",
};

const bundles = import.meta.glob("../../corpus/bundles/*.js", {
  eager: true,
  query: "?raw",
  import: "default",
}) as Record<string, string>;

/// Canned upstream responses per mock host, shaped like the real APIs — a
/// static auxiliary worker (`test/mock-upstream.js`, wired in
/// vitest.config.ts) used as the guests' `globalOutbound`: a dynamic
/// worker's own entrypoints cannot be transferred as another dynamic
/// worker's outbound.
function outboundStub(): Fetcher {
  const stub = (env as { MOCK_UPSTREAM?: Fetcher }).MOCK_UPSTREAM;
  if (!stub) throw new Error("MOCK_UPSTREAM service binding missing (vitest.config.ts)");
  return stub;
}

interface GuestEnvelope {
  status?: number;
  headers?: Record<string, string>;
  body?: unknown;
  mediaType?: string;
}

/// Load `source` under the shim and invoke it once with the given message.
function invokeGuest(
  id: string,
  source: string,
  msg: Record<string, unknown>,
  config: Record<string, unknown> = {},
  outbound: Fetcher | null = null,
): Promise<GuestEnvelope> {
  const worker = loader!.get(id, () => ({
    compatibilityDate: "2026-08-22",
    mainModule: "shim.js",
    modules: { "shim.js": GUEST_SHIM, "bundle.js": { js: source } },
    env: {},
    globalOutbound: outbound,
    limits: { cpuMs: 15_000, subRequests: 16 },
    tails: [],
  }));
  const ep = worker.getEntrypoint("Rs2Guest") as unknown as {
    invoke(msg: unknown, config: unknown, invocationId: string): Promise<GuestEnvelope>;
  };
  return ep.invoke(msg, config, `test-${id}`);
}

const RUN_MSG = {
  method: "POST",
  url: "/run",
  headers: {},
  body: null,
  mediaType: null,
  bodyPassthrough: false,
  requestStreaming: false,
  responseStreaming: false,
  bodySize: null,
};

describe("guest shim", () => {
  it("generated shim module is current (run `npm run build:shim` after editing guest-shim.js)", async () => {
    expect(GUEST_SHIM).toBe(shimSource);
    expect(GUEST_SHIM_SOURCE_HASH).toBe((await sha256Hex(new TextEncoder().encode(shimSource))).slice(0, 16));
  });

  it("normalizes envelopes like js.rs (bare value → 200 body, null → 204)", async () => {
    expect(loader, "worker_loaders binding (wrangler.test.jsonc)").toBeDefined();
    const bare = await invokeGuest("test:bare", `export default async () => ({ answer: 42 });`, RUN_MSG);
    expect(bare).toEqual({ status: 200, body: { answer: 42 } });
    const nothing = await invokeGuest("test:nothing", `export default async () => null;`, RUN_MSG);
    expect(nothing).toEqual({ status: 204 });
    const envl = await invokeGuest(
      "test:envelope",
      `export default async () => ({ status: 201, headers: { "x-a": "b" }, body: "made" });`,
      RUN_MSG,
    );
    expect(envl.status).toBe(201);
  });

  it("supports { handle } exports and rejects shapeless defaults", async () => {
    const handled = await invokeGuest(
      "test:handle-obj",
      `export default { handle: async (msg) => ({ status: 200, body: msg.method }) };`,
      RUN_MSG,
    );
    expect(handled.body).toBe("POST");
    // A handler throw crosses back as the guest-throw marker the engine
    // maps to 502 `contract_violation` (`JS service failed: …`).
    const shapeless = (await invokeGuest("test:shapeless", `export default 42;`, RUN_MSG)) as {
      __rs2_guest_throw?: boolean;
      message?: string;
    };
    expect(shapeless.__rs2_guest_throw).toBe(true);
    expect(shapeless.message).toBe("default export must be a function or { handle }");
  });

  it("ctx is default-deny without a host binding (the template sandbox)", async () => {
    const out = await invokeGuest(
      "test:deny",
      `export default async (msg, ctx) => {
        try { await ctx.request("cache", { url: "/x" }); return { status: 500, body: "allowed?" }; }
        catch (e) { return { status: 200, body: { code: e.code, status: e.status } }; }
      };`,
      RUN_MSG,
    );
    expect(out.body).toEqual({ code: "capability_denied", status: 403 });
  });

  it("shim installs Buffer/global/process and platform fetch stays wrapped", async () => {
    const out = await invokeGuest(
      "test:globals",
      `export default async () => ({
        status: 200,
        body: {
          buf: Buffer.from("hi").toString("base64"),
          hex: Buffer.from("6869", "hex").toString(),
          // The platform provides its own \`process\` (not shadowed, §E.2);
          // the shim only fills the gap when it is absent.
          plat: typeof process.platform,
          hasEnv: typeof process.env,
          glob: global === globalThis,
          fetchWrapped: typeof fetch === "function",
          url: new URL("https://x.test/a?b=1").pathname,
        },
      });`,
      RUN_MSG,
    );
    expect(out.body).toEqual({
      buf: "aGk=",
      hex: "hi",
      plat: "string",
      hasEnv: "object",
      glob: true,
      fetchWrapped: true,
      url: "/a",
    });
  });

  it("guest fetch reaches the globalOutbound stub", async () => {
    const out = await invokeGuest(
      "test:fetch",
      `export default async () => {
        const r = await fetch("https://api.stripe.test/v1/customers", { method: "POST" });
        const body = await r.json();
        return { status: 200, body: { status: r.status, id: body.id } };
      };`,
      RUN_MSG,
      {},
      outboundStub(),
    );
    expect(out.body).toEqual({ status: 200, id: "cus_1" });
  });
});

describe("engine helpers", () => {
  it("cpuMs mount knob: default 5000, ceiling 30000", () => {
    expect(cpuMsFromConfig({})).toBe(5000);
    expect(cpuMsFromConfig({ limits: { cpuMs: 1200 } })).toBe(1200);
    expect(cpuMsFromConfig({ limits: { cpuMs: 90_000 } })).toBe(30_000);
    expect(cpuMsFromConfig({ limits: { cpuMs: -1 } })).toBe(5000);
  });

  it("socket allowlist patterns match like js.rs", () => {
    const m = (pat: string, host: string, port: number) => socketPatternMatches(pat, host, port, `${host}:${port}`);
    expect(m("db.test:6379", "db.test", 6379)).toBe(true);
    expect(m("db.test:6379", "db.test", 6380)).toBe(false);
    expect(m("db.test", "db.test", 1234)).toBe(true);
    expect(m("*", "anything", 1)).toBe(true);
    expect(m("*.db.test", "a.db.test", 5432)).toBe(true);
    expect(m("*.db.test", "db.test", 5432)).toBe(true);
    expect(m("*.db.test", "evil-db.test", 5432)).toBe(false);
    expect(m("*.db.test:5432", "a.db.test", 5432)).toBe(true);
    expect(m("*.db.test:5432", "a.db.test", 1)).toBe(false);
  });
});

describe("compile check + template sandbox", () => {
  const engineHost: EngineHost = {
    loader: loader!,
    invocations: new Map(),
    hostApiStub: () => {
      throw new Error("unused");
    },
    egressStub: () => {
      throw new Error("unused");
    },
    stateKv: {
      get: async () => undefined,
      put: async () => undefined,
    },
  };
  const engine = new DynamicWorkerEngine(engineHost);

  it("compileCheck: a broken bundle is 502 contract_violation, a fine one passes", async () => {
    await engine.compileCheck(`export default async () => ({ status: 200 });`, "okhash0000000000");
    const err = await engine
      .compileCheck(`export default ((((`, "badhash000000000")
      .then(() => undefined)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RsError);
    expect((err as RsError).status).toBe(502);
    expect((err as RsError).code).toBe("contract_violation");
    expect((err as RsError).detail).toContain("JS bundle failed to compile");
  });

  it("invokeResident: the template envelope round-trips; egress is cut", async () => {
    const source = `export default async (msg) => ({
      status: 200,
      headers: { "x-ignored": "yes" },
      body: "<h1>" + (msg.body && msg.body.title ? msg.body.title : "?") + "</h1>",
    });`;
    const { status, body } = await engine.invokeResident(
      "tpl:test0000000001",
      source,
      { method: "POST", url: "/", body: { title: "hi" }, mediaType: "application/json" },
      {},
      1_000,
      30_000,
    );
    expect(status).toBe(200);
    expect(body).toBe("<h1>hi</h1>");

    // globalOutbound: null — a template that tries to fetch fails.
    const fetching = `export default async () => {
      try { await fetch("https://api.stripe.test/x"); return { status: 200, body: "leaked" }; }
      catch (e) { return { status: 200, body: "cut: " + (e && e.message ? "yes" : "?") }; }
    };`;
    const cut = await engine.invokeResident(
      "tpl:test0000000002",
      fetching,
      { method: "POST", url: "/", body: {}, mediaType: "application/json" },
      {},
      1_000,
      30_000,
    );
    expect(cut.body).toContain("cut:");
  });
});

describe("sdk corpus (G5, mocked globalOutbound)", () => {
  const names = Object.keys(bundles)
    .map((p) => p.replace(/^.*\//, "").replace(/\.js$/, ""))
    .sort();

  it.skipIf(names.length === 0)("every built corpus bundle loads and answers a mocked call", async () => {
    const outbound = outboundStub();
    const failed: string[] = [];
    for (const [path, source] of Object.entries(bundles)) {
      const name = path.replace(/^.*\//, "").replace(/\.js$/, "");
      try {
        const out = await invokeGuest(`corpus:${name}`, source, RUN_MSG, {}, outbound);
        expect(out.status, `${name}: status`).toBe(200);
        const body = out.body as { sdk?: string };
        expect(body.sdk, `${name}: bundle identified itself`).toBe(name);
      } catch (e) {
        failed.push(`${name}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
    expect(failed, `corpus failures:\n${failed.join("\n")}`).toEqual([]);
  }, 120_000);

  it("records the known build failures", () => {
    for (const name of Object.keys(KNOWN_BUILD_FAILURES)) {
      expect(names, `${name} is a known build failure; its bundle should be absent`).not.toContain(name);
    }
  });
});
