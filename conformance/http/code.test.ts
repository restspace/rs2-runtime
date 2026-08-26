// `code:` mounts over HTTP — replaces the HTTP-visible parts of
// `rs2-core/tests/store_grant.rs` and `rs2-core/tests/conformance.rs`
// (spec F.3). Every bundle is deployed through `POST /services/code/<name>/`
// and mounted through `PUT /services/raw`, so the whole path from deploy to
// first request is the contract under test. The Rust files' inline bundles
// are reproduced here verbatim except that every host capability is
// `await`ed — a no-op on the Rust V8 engine (synchronous returns), required
// on the Worker (the declared `guest-async` facet) — and the echo cases use
// `fixtures/echo.js` as the spec prescribes.

import { readFileSync } from "node:fs";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, type Rs2Client, type Rs2Response } from "./src/client.ts";
import { type Mount, Seed } from "./src/seed.ts";

const CODE_BASE = "/services/code";

// ---- the bundles ----------------------------------------------------------

/** `store_grant_round_trips_and_stays_private`: the full private-store round trip. */
const STORE_ROUND_TRIP = `
export default async (msg, ctx) => {
  if (msg.url.includes("cleanup")) {
    const a = await ctx.request("cache", { method: "DELETE", url: "/a/?confirm=a" });
    const hit = await ctx.request("cache", { method: "DELETE", url: "/hit.txt" });
    return { status: 200, body: { a: a.status, hit: hit.status } };
  }
  const put = await ctx.request("cache", { method: "PUT", url: "/a/note.txt",
    body: "hello cache", mediaType: "text/plain" });
  const got = await ctx.request("cache", { url: "/a/note.txt" });
  const listing = await ctx.request("cache", { url: "/a/" });
  let denied = null;
  try { await ctx.request("elsewhere", { url: "/x" }); }
  catch (e) { denied = e.code; }
  return { status: 200, body: {
    putStatus: put.status,
    etag: (put.headers && (put.headers.etag || put.headers.ETag)) || null,
    read: got.body,
    listTotal: listing.body.total,
    denied,
    basePath: msg.headers["x-rs2-base-path"],
  } };
};
`;

/** `store_grant_rejects_traversal`: the guest sees a structured `path_unsafe`. */
const STORE_TRAVERSAL = `
export default async (msg, ctx) => {
  const r = await ctx.request("cache", { url: "/../../../escape.txt" });
  return { status: 200, body: { status: r.status, code: r.body && r.body.code } };
};
`;

/** `body_ref_attaches_a_granted_read_host_side`: `x-rs2-body-ref` resolution. */
const BODY_REF = `
export default async (msg, ctx) => {
  if (msg.url.includes("missing")) {
    return { status: 200, headers: { "x-rs2-body-ref": "cache:/nope.txt" } };
  }
  await ctx.request("cache", { method: "PUT", url: "/hit.txt",
    body: "derived bytes", mediaType: "text/plain" });
  return { status: 200,
           headers: { "x-rs2-body-ref": "cache:/hit.txt", "x-img-cache": "hit" } };
};
`;

/** `store_grant_requires_a_relative_root` / the httpOut hosts check: never reached. */
const UNREACHABLE = `export default async () => ({ status: 200, body: "unreachable" });`;

/** `ungranted_capability_is_denied` (uncaught half): the error keeps its identity. */
const DENY_UNCAUGHT = `
export default async (msg, ctx) => {
  await ctx.request("not-granted", { url: "/anything" });
  return { status: 200 };
};
`;

/** httpOut: a host outside the allowlist is `capability_denied`, caught or not. */
const HTTP_OUT = `
export default async (msg, ctx) => {
  if (msg.url.includes("uncaught")) {
    await ctx.request("api", { url: "https://other.example/x" });
    return { status: 200, body: "unreachable" };
  }
  try {
    await ctx.request("api", { url: "https://other.example/x" });
    return { status: 500, body: "expected denial" };
  } catch (e) {
    return { status: 200, body: { code: e.code, status: e.status, message: e.message } };
  }
};
`;

/** `memory_cap_kills_unbounded_allocation`. */
const MEMORY_HOG = `
export default () => {
  const hog = [];
  for (;;) { hog.push("x".repeat(1024 * 1024)); }
};
`;

/** `outbound_call_budget_is_enforced`: one call past the budget is fatal. */
const BUDGET = `
export default async (msg, ctx) => {
  for (let i = 0; i < 200; i++) {
    await ctx.request("api", { url: "/one" });
  }
  return { status: 200, body: "unreachable" };
};
`;

/** `capability_responses_are_typed`: JSON responses arrive parsed. */
const TYPED = `
export default async (msg, ctx) => {
  const resp = await ctx.request("api", { url: "/one" });
  return { status: 200, body: { got: resp.body.answer, status: resp.status } };
};
`;

/** `state_capability_persists_across_invocations` (globals half). */
const LEAK = `
globalThis.leak = (globalThis.leak ?? 0) + 1; // must NOT persist
export default async (msg, ctx) => {
  const n = parseInt((await ctx.state.get("count")) ?? "0", 10) + 1;
  await ctx.state.put("count", String(n));
  return { status: 200, body: { count: n, leak: globalThis.leak } };
};
`;

/** A Wasm module header: the code store files it as `.wasm` by media type. */
const WASM_BYTES = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);

const STORE_GRANT = { cache: { type: "store", root: "img-cache" } };

function status(res: Rs2Response, want: number, msg: string): void {
  expect(res.status, `${msg}: ${res.describe()}`).toBe(want);
}

describe("code mounts", () => {
  let seed: Seed | undefined;
  let admin: Rs2Client;
  let anon: Rs2Client;
  const deployed: string[] = [];

  /** `POST /services/code/<name>/` → the `code:<name>@<version>` ref. */
  async function deploy(name: string, source: string | Uint8Array, contentType = "application/javascript"): Promise<string> {
    const res = await admin.post(`${CODE_BASE}/${name}/`, { body: source, contentType });
    status(res, 201, `[deploy ${name}]`);
    const body = res.json<{ ref: string; validated: boolean }>();
    expect(body.ref, `[deploy ${name}] ref`).toBe(`code:${name}@${body.ref.split("@")[1]}`);
    deployed.push(name);
    return body.ref;
  }

  beforeAll(async () => {
    seed = await Seed.create();
    admin = seed.admin;
    anon = seed.anon;
    const echo = readFileSync(env().codeBundle, "utf8");

    const refs = {
      echo: await deploy("conf-echo", echo),
      store: await deploy("conf-store", STORE_ROUND_TRIP),
      traversal: await deploy("conf-traversal", STORE_TRAVERSAL),
      bodyRef: await deploy("conf-body-ref", BODY_REF),
      unreachable: await deploy("conf-unreachable", UNREACHABLE),
      denyUncaught: await deploy("conf-deny-uncaught", DENY_UNCAUGHT),
      httpOut: await deploy("conf-http-out", HTTP_OUT),
      hog: await deploy("conf-hog", MEMORY_HOG),
      budget: await deploy("conf-budget", BUDGET),
      typed: await deploy("conf-typed", TYPED),
      leak: await deploy("conf-leak", LEAK),
      wasm: await deploy("conf-wasm", WASM_BYTES, "application/wasm"),
    };

    const mounts: Mount[] = [
      // `conformance.rs`: echo with config delivered (`k: "v"`).
      { path: "/echo", service: refs.echo, config: { access: "open", k: "v" } },
      { path: "/leak", service: refs.leak, config: { access: "open" } },
      { path: "/deny", service: refs.denyUncaught, config: { access: "open" } },
      { path: "/hog", service: refs.hog, config: { access: "open" } },
      { path: "/wasm", service: refs.wasm, config: { access: "open" } },
      // `prefix` grants re-enter dispatch under the caller's principal.
      { path: "/typed", service: refs.typed, config: { access: "open", grants: { api: { prefix: "/data/notes" } } } },
      { path: "/budget", service: refs.budget, config: { access: "open", grants: { api: { prefix: "/data/notes" } } } },
      // `store_grant.rs`: private storage under `.rs2-store/img-cache`.
      { path: "/store", service: refs.store, config: { access: "open", grants: STORE_GRANT } },
      { path: "/traversal", service: refs.traversal, config: { access: "open", grants: STORE_GRANT } },
      { path: "/body-ref", service: refs.bodyRef, config: { access: "open", grants: STORE_GRANT } },
      { path: "/bad-root", service: refs.unreachable, config: { access: "open", grants: { cache: { type: "store", root: "/absolute" } } } },
      // `httpOut` grants: allowlisted hosts only; an empty allowlist is a config error.
      { path: "/http-out", service: refs.httpOut, config: { access: "open", grants: { api: { type: "httpOut", hosts: ["api.allowed.test"] } } } },
      { path: "/no-hosts", service: refs.unreachable, config: { access: "open", grants: { api: { type: "httpOut", hosts: [] } } } },
    ];
    await seed.applyMounts(mounts);
    seed.trackDataset("/data", "notes");
    const note = await admin.put("/data/notes/one", { json: { answer: 42 } });
    status(note, 201, "seed /data/notes/one");
  });

  afterAll(async () => {
    // The private store tree is only reachable through the guest: ask it
    // to clear what the suite wrote before the mount goes away.
    await anon.get("/store/cleanup");
    // Bundles cannot be deleted while a live mount references them, so the
    // base config goes back first.
    await seed?.restore();
    for (const name of deployed) {
      const res = await admin.delete(`${CODE_BASE}/${name}/?confirm=${name}`);
      if (![204, 404].includes(res.status)) throw new Error(`cleanup ${name}: ${res.describe()}`);
    }
  });

  // ---- conformance.rs: message semantics ----------------------------------

  test("message semantics round-trip: method, url, body, and config reach the guest", async () => {
    const res = await anon.post("/echo/x?y=1", { body: "hello", contentType: "text/plain" });
    status(res, 200, "[echo] POST");
    expect(res.header("x-engine"), "[echo] guest headers reach the client").toBe("js");
    expect(res.contentType(), "[echo] object bodies are JSON").toBe("application/json");
    const v = res.json();
    expect(v.method, "[echo] method").toBe("POST");
    expect(v.url, "[echo] url is path + query, no host").toBe("/echo/x?y=1");
    expect(v.body, "[echo] text body arrives as a string").toBe("hello");
    expect(v.config.k, "[echo] mount config is delivered").toBe("v");

    // No body → the empty string (the Wasm guest's shape, echo.js keeps it).
    const bare = await anon.get("/echo/plain");
    status(bare, 200, "[echo] GET");
    expect(bare.json().body, "[echo] no body → empty string").toBe("");
    expect(bare.json().method, "[echo] GET method").toBe("GET");
  });

  // ---- conformance.rs: invariant 2 (capability denial) ---------------------

  test("an ungranted capability is capability_denied inside the guest (caught)", async () => {
    const res = await anon.get("/echo/deny-check");
    status(res, 200, "[echo] deny-check");
    expect(res.contentType(), "[echo] string body → text/plain").toBe("text/plain");
    expect(res.text(), "[echo] the guest saw e.code === 'capability_denied'").toBe("denied-as-expected");
  });

  test("an uncaught capability denial keeps its identity out of the engine (403 capability_denied)", async () => {
    const res = await anon.get("/deny/x");
    status(res, 403, "[deny] uncaught denial");
    const p = res.problem();
    expect(p.code, "[deny] problem code").toBe("capability_denied");
    expect(p.status, "[deny] problem status").toBe(403);
    expect(p.capability, "[deny] the denied capability is named").toBe("not-granted");
    expect(p.detail, "[deny] detail wording").toBe("capability 'not-granted' is not granted to this service");
    expect(p.tenant, "[deny] tenant").toBe(env().tenant);
    expect(p.retryable, "[deny] not retryable").toBe(false);
  });

  // ---- conformance.rs: invariant 4 (state) ---------------------------------

  test("ctx.state persists across invocations; globals do not", async () => {
    // echo.js: `state` in the path bumps a counter.
    for (const expected of [1, 2, 3]) {
      const res = await anon.get("/echo/state");
      status(res, 200, `[echo] state call ${expected}`);
      expect(res.json().count, `[echo] count persists via the capability (${expected})`).toBe(expected);
    }
    // A fresh counter per service instance: the leak bundle's own key.
    for (const expected of [1, 2, 3]) {
      const res = await anon.get("/leak/c");
      status(res, 200, `[leak] call ${expected}`);
      const v = res.json();
      expect(v.count, `[leak] state persists via the capability (${expected})`).toBe(expected);
      expect(v.leak, `[leak] globals do not survive invocations (${expected})`).toBe(1);
    }
  });

  // ---- conformance.rs: capability round-trips and the outbound budget ------

  test("capability responses arrive typed: JSON bodies are parsed in the guest", async () => {
    const res = await anon.get("/typed/x");
    status(res, 200, "[typed] prefix grant round-trip");
    const v = res.json();
    expect(v.got, "[typed] resp.body.answer").toBe(42);
    expect(v.status, "[typed] resp.status").toBe(200);
  });

  test("the outbound-call budget is enforced per invocation → 503 limit_exceeded", async () => {
    const res = await anon.get("/budget/x");
    status(res, 503, "[budget] one call past the budget");
    const p = res.problem();
    expect(p.code, "[budget] code").toBe("limit_exceeded");
    expect(p.limit, "[budget] which limit").toBe("outbound_calls");
    expect(p.retryable, "[budget] retryable").toBe(true);
    expect(typeof p.cap, "[budget] cap is a number").toBe("number");
    expect(p.observed, "[budget] observed is one past the cap").toBe((p.cap as number) + 1);
  });

  // ---- conformance.rs: limits ----------------------------------------------

  // Cloudflare: the 128 MiB isolate heap cap is a production platform
  // limit; `wrangler dev`'s local workerd applies no per-isolate cap to
  // loader workers, so an allocation bomb OOMs (and can crash) the local
  // process instead of the guest. The spec's `@remote` provision (§G)
  // applies: this case runs against the Rust host always and against
  // Cloudflare only when `RS2_CF_REMOTE` marks a real-platform run.
  test.skipIf(env().hostKind === "cloudflare" && !process.env.RS2_CF_REMOTE)(
    "unbounded allocation hits the memory cap → 503 limit_exceeded (memory_bytes)",
    async () => {
      const res = await anon.get("/hog/x");
      status(res, 503, "[hog] memory cap");
      const p = res.problem();
      expect(p.code, "[hog] code").toBe("limit_exceeded");
      expect(p.limit, "[hog] which limit").toBe("memory_bytes");
      expect(p.retryable, "[hog] retryable").toBe(true);
      expect(p.retryAfterMs, "[hog] retryAfterMs").toBe(2000);
    },
  );

  test(
    "a guest that outlives the wall clock is killed → 503 limit_exceeded (wall_clock_ms), retryable",
    async () => {
      const limits = (await anon.getJson("/.well-known/rs2/services")).limits;
      const wallClockMs = Number(limits.wallClockMs);
      expect(wallClockMs, "[wall-clock] discovery reports wallClockMs").toBeGreaterThan(0);
      const started = Date.now();
      const res = await anon.get(`/echo/slow?sleep=${wallClockMs + 2000}`);
      status(res, 503, "[wall-clock] killed invocation");
      expect(Date.now() - started, "[wall-clock] answered at the limit, not after the sleep").toBeLessThan(wallClockMs + 2000);
      const p = res.problem();
      expect(p.code, "[wall-clock] code").toBe("limit_exceeded");
      expect(p.limit, "[wall-clock] which limit").toBe("wall_clock_ms");
      expect(p.retryable, "[wall-clock] retryable").toBe(true);
      expect(p.retryAfterMs, "[wall-clock] retryAfterMs").toBe(2000);
      expect(p.cap, "[wall-clock] cap is the advertised wall clock").toBe(wallClockMs);
    },
    90_000,
  );

  // ---- store_grant.rs --------------------------------------------------------

  test("store grant: conditional-capable writes, reads, listings; other names denied", async () => {
    // The private tree outlives the tenant config (and a previous run on
    // the same host), so start from empty.
    const clean = await anon.get("/store/cleanup");
    status(clean, 200, "[store] cleanup");
    const res = await anon.get("/store/run");
    status(res, 200, "[store] run");
    const out = res.json();
    expect(out.putStatus, `[store] first write creates: ${res.text()}`).toBe(201);
    expect(out.etag, "[store] the write carries an ETag").not.toBeNull();
    expect(out.read, "[store] read back").toBe("hello cache");
    expect(out.listTotal, "[store] listing total").toBe(1);
    expect(out.denied, "[store] an ungranted name is capability_denied").toBe("capability_denied");
    expect(out.basePath, "[store] the host stamps x-rs2-base-path with the matched mount").toBe("/store");
    // Second run: the write is an overwrite now, and the listing still holds one.
    const again = await anon.get("/store/run");
    status(again, 200, "[store] run again");
    expect(again.json().putStatus, "[store] overwrite is 200").toBe(200);
    expect(again.json().listTotal, "[store] still one child").toBe(1);
  });

  test("store grant: traversal cannot escape the private root (path_unsafe)", async () => {
    const res = await anon.get("/traversal/run");
    status(res, 200, "[traversal] the guest sees a structured response");
    const out = res.json();
    expect(out.code, `[traversal] path_unsafe: ${res.text()}`).toBe("path_unsafe");
    expect(out.status, "[traversal] as a 400").toBe(400);
  });

  test("x-rs2-body-ref: the host attaches a granted read after the guest returns", async () => {
    const hit = await anon.get("/body-ref/run");
    status(hit, 200, "[body-ref] hit");
    expect(hit.header("x-img-cache"), "[body-ref] guest headers kept").toBe("hit");
    expect(hit.header("x-rs2-body-ref"), "[body-ref] ref header is stripped").toBeNull();
    expect(hit.text(), "[body-ref] the referenced bytes are the body").toBe("derived bytes");
    expect(hit.contentType(), "[body-ref] media type of the stored file").toBe("text/plain");

    // A dangling reference is the service's bug: 502, not a silent empty 200.
    const miss = await anon.get("/body-ref/missing");
    status(miss, 502, "[body-ref] dangling ref");
    const p = miss.problem();
    expect(p.code, "[body-ref] code").toBe("contract_violation");
    expect(p.detail, "[body-ref] names the read and its status").toContain("cache:/nope.txt");
  });

  test("store grant without a usable relative root → 400 bad_request at invocation", async () => {
    const res = await anon.get("/bad-root/run");
    status(res, 400, "[bad-root]");
    const p = res.problem();
    expect(p.code, "[bad-root] code").toBe("bad_request");
    // `sanitized_store_root`'s wording: an absolute root is rejected as
    // such (the "requires a non-empty relative 'root'" wording is for an
    // absent/empty one).
    expect(p.detail, "[bad-root] names the offending root").toBe(
      "store root '/absolute' must be a relative path (no leading '/'/'\\' or drive letter)",
    );
  });

  // ---- httpOut ---------------------------------------------------------------

  test("httpOut: a host outside the allowlist is capability_denied naming the host", async () => {
    const caught = await anon.get("/http-out/run");
    status(caught, 200, "[http-out] caught");
    const out = caught.json();
    expect(out.code, "[http-out] e.code").toBe("capability_denied");
    expect(out.status, "[http-out] e.status").toBe(403);
    expect(out.message, "[http-out] e.message names the host").toBe(
      "capability 'httpOut to 'other.example'' is not granted to this service",
    );

    const uncaught = await anon.get("/http-out/uncaught");
    status(uncaught, 403, "[http-out] uncaught");
    const p = uncaught.problem();
    expect(p.code, "[http-out] problem code").toBe("capability_denied");
    expect(p.capability, "[http-out] the denied capability names the host").toBe("httpOut to 'other.example'");
  });

  test("httpOut: an empty hosts allowlist is a 400 at invocation", async () => {
    const res = await anon.get("/no-hosts/run");
    status(res, 400, "[no-hosts]");
    const p = res.problem();
    expect(p.code, "[no-hosts] code").toBe("bad_request");
    expect(p.detail, "[no-hosts] wording").toBe("httpOut grant 'api' requires a non-empty 'hosts' allowlist");
  });

  // ---- Wasm --------------------------------------------------------------------

  test("a .wasm bundle deploys unvalidated and answers 501 engine_unavailable", async () => {
    const listing = await admin.get(`${CODE_BASE}/conf-wasm/`);
    status(listing, 200, "[wasm] container listing");
    const entry = listing.listing().entries[0];
    expect(entry.name.endsWith(".wasm"), `[wasm] filed by media type: ${JSON.stringify(entry)}`).toBe(true);
    expect(entry.mountedAt, "[wasm] the live mount is reported").toEqual(["/wasm"]);

    const res = await anon.get("/wasm/x");
    status(res, 501, "[wasm] first request");
    const p = res.problem();
    expect(p.code, "[wasm] code").toBe("engine_unavailable");
    expect(p.detail, "[wasm] wording").toContain("is a wasm component but this build has no wasm engine");
  });
});
