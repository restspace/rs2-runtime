// Guest-backed store adapters (cloudflare.md P4b) — the Worker analogue of
// `rs2-core/src/engines/resident.rs`'s unit tests plus the capability
// mapping: request shape per method, error mapping, the `features`
// handshake, and the engine-level resident properties (module state
// persists across calls in one isolate; a build error surfaces as 502).

import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import {
  GuestDataStore,
  GuestFileStore,
  GuestQueryStore,
  GuestMessageGateway,
  ResidentAdapter,
  queryEncode,
  scalarQuote,
  storeError,
} from "../src/capabilities/guest-stores";
import type { AdapterCaller, GuestAdapterWiring } from "../src/capabilities/guest-stores";
import { ScopedFileStore } from "../src/capabilities/scoped";
import { DynamicWorkerEngine } from "../src/engines/dynamic-worker";
import type { EngineHost } from "../src/engines/dynamic-worker";
import { Body } from "../src/runtime/body";
import { RsError } from "../src/runtime/error";
import type { Json } from "../src/runtime/error";
import { FieldPath } from "../src/runtime/listing";
import { MediaType } from "../src/runtime/media-type";

const loader = (env as { LOADER?: WorkerLoader }).LOADER;
const upstream = (env as { MOCK_UPSTREAM?: Fetcher }).MOCK_UPSTREAM;

// ---- a recording fake for the capability → message mapping ----------------

interface Recorded {
  method: string;
  path: string;
  body: Json | undefined;
}

class FakeCaller implements AdapterCaller {
  calls: Recorded[] = [];
  features: string[] = [];
  reply: [number, Json] = [200, null];
  replies: Array<[number, Json]> | undefined;

  async call(method: string, path: string, body?: Json): Promise<[number, Json]> {
    this.calls.push({ method, path, body });
    if (this.replies && this.replies.length > 0) return this.replies.shift()!;
    return this.reply;
  }

  hasFeature(name: string): boolean {
    return this.features.includes(name);
  }

  last(): Recorded {
    return this.calls[this.calls.length - 1]!;
  }
}

describe("GuestDataStore mapping (resident.rs parity)", () => {
  it("maps each DataStore method to its store-pattern request", async () => {
    const fake = new FakeCaller();
    const store = new GuestDataStore(fake);

    fake.reply = [200, { n: 2 }];
    expect(await store.get("t", "things", "alpha")).toEqual({ n: 2 });
    expect(fake.last()).toEqual({ method: "GET", path: "/things/alpha", body: undefined });

    fake.reply = [201, null];
    expect(await store.put("t", "things", "alpha", { n: 1 })).toBe(true);
    expect(fake.last()).toEqual({ method: "PUT", path: "/things/alpha", body: { n: 1 } });
    fake.reply = [200, null];
    expect(await store.put("t", "things", "alpha", { n: 2 })).toBe(false);

    fake.reply = [204, null];
    await store.delete("t", "things", "alpha");
    expect(fake.last()).toEqual({ method: "DELETE", path: "/things/alpha", body: undefined });

    fake.reply = [200, { entries: [{ name: "a", dir: false }, { name: ".schema.json", dir: false }], total: 2 }];
    expect(await store.listKeys("t", "things", 10, 5)).toEqual([["a"], 2]);
    expect(fake.last().path).toBe("/things/?$take=10&$skip=5");

    fake.reply = [200, { entries: [{ name: "things/", dir: true }, { name: "x", dir: false }], total: 1 }];
    expect(await store.listDatasets("t", 3, 0)).toEqual([["things"], 1]);
    expect(fake.last().path).toBe("/?$take=3&$skip=0");

    fake.reply = [404, { detail: "no schema" }];
    expect(await store.getSchema("t", "things")).toBeUndefined();
    expect(fake.last().path).toBe("/things/.schema.json");
    fake.reply = [200, { type: "object" }];
    expect(await store.getSchema("t", "things")).toEqual({ type: "object" });

    fake.reply = [200, null];
    await store.putSchema("t", "things", { type: "object" });
    expect(fake.last()).toEqual({ method: "PUT", path: "/things/.schema.json", body: { type: "object" } });

    fake.reply = [204, null];
    await store.deleteDataset("t", "things");
    expect(fake.last()).toEqual({ method: "DELETE", path: "/things/?confirm=things", body: undefined });
  });

  it("maps non-2xx responses to the built-in stores' error identities", async () => {
    const fake = new FakeCaller();
    const store = new GuestDataStore(fake);
    const expectError = async (status: number, body: Json, wantStatus: number, wantCode: string) => {
      fake.reply = [status, body];
      const err = await store.get("t", "d", "k").then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(err).toBeInstanceOf(RsError);
      expect((err as RsError).status, `status for ${status}`).toBe(wantStatus);
      expect((err as RsError).code, `code for ${status}`).toBe(wantCode);
      return err as RsError;
    };
    const notFound = await expectError(404, { detail: "no record 'k'" }, 404, "not_found");
    expect(notFound.detail).toBe("no record 'k'");
    await expectError(400, { detail: "bad" }, 400, "bad_request");
    await expectError(401, {}, 401, "unauthorized");
    await expectError(403, {}, 403, "forbidden");
    await expectError(409, {}, 409, "conflict");
    await expectError(413, {}, 413, "payload_too_large");
    const invalid = await expectError(422, { detail: "nope", errors: [{ path: "/a", message: "m" }] }, 422, "validation_failed");
    expect(invalid.extra?.errors).toEqual([{ path: "/a", message: "m" }]);
    // Anything else is the adapter breaking its contract.
    const broken = await expectError(500, {}, 502, "contract_violation");
    expect(broken.detail).toBe("data adapter returned 500");
    // `message` is the fallback detail key.
    expect(storeError(409, { message: "from message" }).detail).toBe("from message");
  });

  it("listRecords falls back to the key-walk without the features handshake", async () => {
    const fake = new FakeCaller();
    const store = new GuestDataStore(fake);
    const spec = { fields: [FieldPath.parse("title")], sort: [], take: 10, skip: 0 };
    fake.replies = [
      [200, { entries: [{ name: "ka", dir: false }], total: 1 }],
      [200, { title: "apple", n: 1 }],
    ];
    const [page, total] = await store.listRecords("t", "posts", spec);
    expect(total).toBe(1);
    expect(page).toEqual([["ka", { title: "apple" }]]);
    // `$select`/`$sort` were never forwarded to the bundle.
    expect(fake.calls.map((c) => c.path)).toEqual(["/posts/?$take=10&$skip=0", "/posts/ka"]);
    expect(store.listingPushdown()).toBe(false);
  });

  it("listRecords pushes `$select`/`$sort` down when advertised, percent-encoded", async () => {
    const fake = new FakeCaller();
    fake.features = ["list-records"];
    const store = new GuestDataStore(fake);
    const spec = {
      fields: [FieldPath.parse("title"), FieldPath.parse("meta.date")],
      sort: [
        [FieldPath.parse("n"), "desc"],
        [FieldPath.parse("title"), "asc"],
      ] as Array<[FieldPath, "asc" | "desc"]>,
      take: 2,
      skip: 1,
    };
    fake.reply = [200, { entries: [{ name: "kd", fields: { title: "cherry" } }], total: 4 }];
    const [page, total] = await store.listRecords("t", "posts", spec);
    expect(total).toBe(4);
    expect(page).toEqual([["kd", { title: "cherry" }]]);
    expect(fake.last().path).toBe("/posts/?$select=title,meta.date&$sort=-n,title&$take=2&$skip=1");
    expect(store.listingPushdown()).toBe(true);
  });

  it("queryEncode escapes separators but keeps unreserved characters", () => {
    expect(queryEncode("meta.date")).toBe("meta.date");
    expect(queryEncode("a,b&c=d")).toBe("a%2Cb%26c%3Dd");
    expect(queryEncode("héllo")).toBe("h%C3%A9llo");
  });
});

describe("GuestQueryStore + GuestMessageGateway mapping", () => {
  it("runQuery ships {query, params, take, skip} to POST /query", async () => {
    const fake = new FakeCaller();
    const store = new GuestQueryStore(fake);
    fake.reply = [200, { rows: [{ a: 1 }], total: 7 }];
    expect(await store.runQuery("t", { dataset: "d" }, { p: 1 }, 5, 2)).toEqual([[{ a: 1 }], 7]);
    expect(fake.last()).toEqual({
      method: "POST",
      path: "/query",
      body: { query: { dataset: "d" }, params: { p: 1 }, take: 5, skip: 2 },
    });
    // total defaults to rows.length when absent.
    fake.reply = [200, { rows: [{}, {}] }];
    expect((await store.runQuery("t", {}, {}, 1, 0))[1]).toBe(2);
  });

  it("quote is the scalar default: composites are a 400", () => {
    expect(scalarQuote("s")).toBe("s");
    expect(scalarQuote(5)).toBe("5");
    expect(scalarQuote(true)).toBe("true");
    for (const [value, kind] of [
      [null, "null"],
      [[1], "array"],
      [{}, "object"],
    ] as const) {
      const err = (() => {
        try {
          scalarQuote(value as Json);
          return undefined;
        } catch (e) {
          return e;
        }
      })();
      expect(err).toBeInstanceOf(RsError);
      expect((err as RsError).detail).toBe(`cannot splice a ${kind} into a string query position`);
    }
  });

  it("message maps send/status, tags the channel, and requires a string id", async () => {
    const fake = new FakeCaller();
    const gw = new GuestMessageGateway(fake, "code:texter@v1", ["sms"], true);
    fake.reply = [200, { id: "m1" }];
    // The guest sees the same channel-tagged envelope an HTTP caller sends.
    expect(await gw.send("t", { channel: "sms", to: "+1555", text: "hi" })).toEqual({
      id: "m1",
      channel: "sms",
      provider: "code:texter@v1",
    });
    expect(fake.last()).toEqual({
      method: "POST",
      path: "/send",
      body: { channel: "sms", to: "+1555", text: "hi" },
    });
    fake.reply = [200, { status: "sent" }];
    expect(await gw.status("t", "m1")).toEqual({ status: "sent" });
    expect(fake.last()).toEqual({ method: "GET", path: "/status/m1", body: undefined });
    fake.reply = [200, {}];
    const err = await gw.send("t", { channel: "sms", to: "x", text: "y" }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect((err as RsError).detail).toBe("message adapter response missing string 'id'");
  });

  it("a guest adapter declares its channels in config, not by feature probe", () => {
    const fake = new FakeCaller();
    const gw = GuestMessageGateway.fromConfig("code:mailer@v1", { channels: ["email"], deliveryStatus: false }, {
      // fromConfig only reads the config; the wiring is never touched here.
    } as never);
    expect(gw.channels()).toEqual(["email"]);
    expect(gw.deliveryStatus()).toBe(false);
    // Default: every known channel, status reported.
    const all = new GuestMessageGateway(fake, "code:x@v1", ["email", "sms"], true);
    expect(all.channels()).toEqual(["email", "sms"]);
    expect(all.deliveryStatus()).toBe(true);
  });
});

describe("GuestFileStore mapping", () => {
  const b64 = (s: string) => btoa(s);

  it("maps the FileStore methods and decodes base64 content", async () => {
    const fake = new FakeCaller();
    const store = new GuestFileStore(fake, 1024 * 1024);

    fake.reply = [200, { size: 9, isDir: false }];
    expect(await store.head("t", "/docs/alpha")).toEqual({ size: 9, lastModified: undefined, isDir: false });
    expect(fake.last()).toEqual({ method: "HEAD", path: "/docs/alpha", body: undefined });

    fake.reply = [200, { contentBase64: b64("content-2"), mediaType: "text/plain" }];
    const body = await store.read("t", "/docs/alpha", undefined);
    expect(new TextDecoder().decode(await body.materialize(1024))).toBe("content-2");
    expect(body.provenance.kind).toBe("replayable");

    // A Range slices host-side (inclusive end); past-the-end is the 416.
    fake.reply = [200, { contentBase64: b64("content-2"), mediaType: "text/plain" }];
    const slice = await store.read("t", "/docs/alpha", { start: 0, end: 6 });
    expect(new TextDecoder().decode(await slice.materialize(1024))).toBe("content");
    fake.reply = [200, { contentBase64: b64("abc"), mediaType: "text/plain" }];
    const err = await store.read("t", "/docs/alpha", { start: 9, end: undefined }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect((err as RsError).status).toBe(416);
    expect((err as RsError).detail).toBe("range start 3 beyond resource size 3");

    fake.reply = [201, null];
    expect(await store.write("t", "/docs/beta", Body.fromString("hello", new MediaType("text/plain")))).toBe(true);
    expect(fake.last()).toEqual({
      method: "PUT",
      path: "/docs/beta",
      body: { contentBase64: b64("hello"), mediaType: "text/plain" },
    });

    fake.reply = [201, null];
    expect(await store.rename("t", "/docs/beta", "/docs/gamma")).toBe(true);
    expect(fake.last()).toEqual({ method: "MOVE", path: "/docs/beta", body: { to: "/docs/gamma" } });

    fake.reply = [204, null];
    await store.deleteDirAll("t", "/docs/");
    expect(fake.last()).toEqual({ method: "DELETE", path: "/docs/", body: { recursive: true } });

    fake.reply = [
      200,
      { entries: [{ name: "sub/", dir: true, size: 0 }, { name: "a", dir: false, size: 3, contentType: "text/plain" }], total: 2 },
    ];
    const [entries, total] = await store.list("t", "/docs/", 10, 0);
    expect(total).toBe(2);
    expect(entries).toEqual([
      { name: "sub/", size: 0, dir: true },
      { name: "a", size: 3, dir: false, contentType: "text/plain" },
    ]);
    expect(fake.last().path).toBe("/docs/?$take=10&$skip=0");
  });

  it("invalid base64 from the adapter is a contract violation", async () => {
    const fake = new FakeCaller();
    const store = new GuestFileStore(fake, 1024);
    fake.reply = [200, { contentBase64: "!!!not-base64!!!" }];
    const err = await store.read("t", "/x", undefined).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect((err as RsError).code).toBe("contract_violation");
    expect((err as RsError).detail).toContain("adapter returned invalid base64");
  });
});

describe("ResidentAdapter reference parsing", () => {
  const wiring = {} as GuestAdapterWiring; // never dereferenced by the constructor
  const parseError = (ref: string) => {
    try {
      new ResidentAdapter("data", ref, {}, wiring);
      return undefined;
    } catch (e) {
      return e as RsError;
    }
  };

  it("rejects malformed refs with the Rust wordings", () => {
    expect(parseError("builtin:mem")?.detail).toBe("data store adapter 'builtin:mem' must be 'code:<name>@<version>'");
    expect(parseError("code:noversion")?.detail).toBe("data store adapter 'code:noversion' must be 'code:<name>@<version>'");
    expect(parseError("code:@v1")?.detail).toBe("invalid data store adapter reference 'code:@v1'");
    expect(parseError("code:a/b@v1")?.detail).toBe("invalid data store adapter reference 'code:a/b@v1'");
    expect(parseError("code:ok@v1")).toBeUndefined();
  });
});

// ---- engine-level resident behavior (real loader) --------------------------

describe("resident adapter engine path", () => {
  const engineHost: EngineHost = {
    loader: loader!,
    invocations: new Map(),
    // The counter/features bundles below never call into env.RS2, so any
    // Fetcher satisfies the loader's env shape.
    hostApiStub: () => upstream!,
    egressStub: () => upstream!,
    stateKv: { get: async () => undefined, put: async () => undefined },
  };
  const engine = new DynamicWorkerEngine(engineHost);
  const base = {
    tenant: "t",
    storeConfig: {},
    socketAllowlist: [],
    serviceRef: "test@v1",
    materializeCap: 1024 * 1024,
    wallClockMs: 10_000,
    cpuMs: 5_000,
  };

  it("module state persists across calls in one isolate (the resident property)", async () => {
    const source = `
      let N = 0;
      export default async (msg, ctx) => { N += 1; return { status: 200, body: { n: N } }; };
    `;
    const args = { ...base, codeId: "t:/data:adapter:counter@v1", source, req: { method: "GET", url: "/" } };
    const a = await engine.invokeAdapter(args);
    const b = await engine.invokeAdapter(args);
    expect(a.body).toEqual({ n: 1 });
    expect((b.body as { n: number }).n, "second call reuses the same isolate").toBeGreaterThan(1);
  });

  it("the features export is read over RPC; absent → empty", async () => {
    const withFeatures = `
      export const features = ["list-records"];
      export default async () => ({ status: 200 });
    `;
    expect(await engine.adapterFeatures("t:/data:adapter:feat@v1", withFeatures, "t", 5_000)).toEqual(["list-records"]);
    const without = `export default async () => ({ status: 200 });`;
    expect(await engine.adapterFeatures("t:/data:adapter:nofeat@v1", without, "t", 5_000)).toEqual([]);
  });

  it("a bundle that fails to evaluate surfaces 502 contract_violation", async () => {
    const err = await engine
      .adapterFeatures("t:/data:adapter:broken@v1", "this is not valid javascript ===", "t", 5_000)
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    expect(err).toBeInstanceOf(RsError);
    expect((err as RsError).status).toBe(502);
    expect((err as RsError).code).toBe("contract_violation");
  });

  it("a handler throw is 502 contract_violation with the failure text", async () => {
    const source = `export default async () => { throw new Error("adapter blew up"); };`;
    const err = await engine
      .invokeAdapter({ ...base, codeId: "t:/data:adapter:throws@v1", source, req: { method: "GET", url: "/" } })
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    expect(err).toBeInstanceOf(RsError);
    expect((err as RsError).status).toBe(502);
    expect((err as RsError).code).toBe("contract_violation");
    expect((err as RsError).detail).toContain("adapter blew up");
  });

  it("the store config block reaches the bundle as ctx.config", async () => {
    const source = `export default async (msg, ctx) => ({ status: 200, body: { host: ctx.config.host } });`;
    const out = await engine.invokeAdapter({
      ...base,
      codeId: "t:/data:adapter:config@v1",
      source,
      req: { method: "GET", url: "/" },
      storeConfig: { adapter: "code:config@v1", host: "db.internal" },
    });
    expect(out.body).toEqual({ host: "db.internal" });
  });
});

// ---- guest wiring end-to-end against a scoped file store -------------------

describe("ResidentAdapter bundle loading", () => {
  it("a missing bundle is the Rust not-found error; deploying afterwards unpins", async () => {
    const files: Record<string, string> = {};
    const fakeStore = {
      read: async (_t: string, path: string) => {
        const text = files[path];
        if (text === undefined) throw RsError.notFound("resource does not exist");
        return Body.fromString(text, new MediaType("application/javascript"));
      },
    };
    const wiring: GuestAdapterWiring = {
      engine: new DynamicWorkerEngine({
        loader: loader!,
        invocations: new Map(),
        hostApiStub: () => upstream!,
        egressStub: () => upstream!,
        stateKv: { get: async () => undefined, put: async () => undefined },
      }),
      files: new ScopedFileStore(fakeStore as never, "t"),
      tenant: "t",
      mountBase: "/data",
      materializedBodyBytes: 1024 * 1024,
      wallClockMs: 10_000,
      cpuMs: 5_000,
    };
    const adapter = new ResidentAdapter("data", "code:absent@v9", {}, wiring);
    const err = await adapter.call("GET", "/things/alpha").then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(RsError);
    expect((err as RsError).status).toBe(404);
    expect((err as RsError).detail).toBe(
      "data adapter bundle 'code:absent@v9' not found — deploy it via PUT /code/absent",
    );
    // A failed load must not pin: deploy and the next call succeeds.
    files[".rs2-code/absent/v9.js"] = `export default async () => ({ status: 200, body: { ok: true } });`;
    const [status, body] = await adapter.call("GET", "/things/alpha");
    expect(status).toBe(200);
    expect(body).toEqual({ ok: true });
  });
});
