// Guest (`code:`) store adapters over HTTP — the cross-host successor to
// the HTTP-observable scenarios of `rs2-core/tests/guest_adapter.rs`
// (cloudflare.md P4b): a mount whose config says
// `"store": {"adapter": "code:<name>@<version>", ...}` is backed by the
// deployed bundle, speaking a real wire protocol (RESP / MongoDB OP_MSG)
// over the gated socket capability to in-process mock backends started by
// this file — so CI runs the suite on both hosts with no external services.
//
// Rust-only internals NOT observable over HTTP and so not covered here
// (they stay covered in-process by `guest_adapter.rs`): pool growth under
// concurrency (`maxRuntimes`), timer-based idle eviction (`idleMs`), and
// the BSON type-byte assertions (int64 vs double on the wire). The pooling
// property itself IS observable — the mock counts connections — with a
// declared divergence for platform-owned isolate eviction on the Worker.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { PACKAGE_ROOT, Rs2Client, entryNamed, type Rs2Response } from "./src/client.ts";
import { divergences } from "./src/divergences.ts";
import { Seed } from "./src/seed.ts";
import { assertListingContract, assertStoreContract, jsonBody, textBody } from "./src/store-contract.ts";
import { startMockMongo } from "./fixtures/mock-mongo.mjs";
import { startMockRedis } from "./fixtures/mock-redis.mjs";

const fixture = (name: string) => readFileSync(resolve(PACKAGE_ROOT, "fixtures", name), "utf8");
const shipped = (name: string) => readFileSync(resolve(PACKAGE_ROOT, "..", "..", "guest-adapters", name), "utf8");

function status(res: Rs2Response, want: number, msg: string): void {
  expect(res.status, `${msg}: ${res.describe()}`).toBe(want);
}

interface MockRedis {
  port: number;
  connections(): number;
  close(): Promise<void>;
}
interface MockMongo {
  port: number;
  store: Map<string, Map<string, Record<string, unknown>>>;
  close(): Promise<void>;
}

let seed: Seed | undefined;
let anon: Rs2Client;
let admin: Rs2Client;
let redis: MockRedis;
let redisPool: MockRedis;
let mongo: MockMongo;
const deployed: string[] = [];

/** `POST /services/code/<name>/` → the `code:<name>@<version>` ref. */
async function deploy(name: string, source: string): Promise<string> {
  const res = await admin.post(`/services/code/${name}/`, { body: source, contentType: "application/javascript" });
  status(res, 201, `[deploy ${name}]`);
  deployed.push(name);
  return res.json<{ ref: string }>().ref;
}

const socketGrant = (port: number) => ({ backend: { type: "socket", hosts: [`127.0.0.1:${port}`] } });

beforeAll(async () => {
  redis = (await startMockRedis()) as MockRedis;
  redisPool = (await startMockRedis()) as MockRedis;
  mongo = (await startMockMongo()) as MockMongo;
  seed = await Seed.create();
  anon = seed.anon;
  admin = seed.admin;

  const redisRef = await deploy("ga-redis", fixture("redis-data.js"));
  const redisQueryRef = await deploy("ga-redis-query", fixture("redis-query.js"));
  const fileRef = await deploy("ga-file", fixture("guest-file.js"));
  const mongoRef = await deploy("ga-mongo-data", shipped("mongo-data.js"));
  const mongoQueryRef = await deploy("ga-mongo-query", shipped("mongo-query.js"));

  // Every guest mount in one config write: a config PUT drops the built
  // tenant (and, on Rust, its resident isolates + pooled sockets), so the
  // pooling observation needs the mounts pinned for the whole suite.
  await seed.applyMounts([
    {
      path: "/rdata",
      service: "data",
      config: {
        access: "open",
        store: { adapter: redisRef, host: "127.0.0.1", port: redis.port, grants: socketGrant(redis.port) },
      },
    },
    {
      path: "/pool",
      service: "data",
      config: {
        access: "open",
        store: { adapter: redisRef, host: "127.0.0.1", port: redisPool.port, grants: socketGrant(redisPool.port) },
      },
    },
    {
      path: "/q",
      service: "query",
      config: {
        access: "open",
        store: { adapter: redisQueryRef, host: "127.0.0.1", port: redis.port, grants: socketGrant(redis.port) },
      },
    },
    { path: "/gfiles", service: "file", config: { access: "open", store: { adapter: fileRef } } },
    {
      path: "/mdata",
      service: "data",
      config: {
        access: "open",
        store: { adapter: mongoRef, host: "127.0.0.1", port: mongo.port, db: "test", grants: socketGrant(mongo.port) },
      },
    },
    {
      path: "/mq",
      service: "query",
      config: {
        access: "open",
        store: { adapter: mongoQueryRef, host: "127.0.0.1", port: mongo.port, db: "test", grants: socketGrant(mongo.port) },
      },
    },
    // An adapter ref nothing deployed: the mount builds (the bundle loads
    // lazily), the first request is the clear 404.
    {
      path: "/undeployed",
      service: "data",
      config: {
        access: "open",
        store: { adapter: "code:ga-absent@v9", host: "127.0.0.1", port: redis.port, grants: socketGrant(redis.port) },
      },
    },
    // A socket grant that does NOT cover the backend: the connect must be
    // denied with capability identity, not a generic failure.
    {
      path: "/denied",
      service: "data",
      config: {
        access: "open",
        store: { adapter: redisRef, host: "127.0.0.1", port: redis.port, grants: socketGrant(1) },
      },
    },
  ]);
});

afterAll(async () => {
  // Restore the base config first (unmounting the bundles), then delete
  // the deployed code, then stop the backends.
  await seed?.restore();
  for (const name of deployed) {
    const res = await admin.delete(`/services/code/${name}/?confirm=${name}`);
    if (![204, 404].includes(res.status)) throw new Error(`cleanup ${name}: ${res.describe()}`);
  }
  await Promise.all([redis?.close(), redisPool?.close(), mongo?.close()]);
});

describe("guest-backed data store (Redis over RESP)", () => {
  test("satisfies the store contract", async () => {
    await assertStoreContract(anon, "/rdata", "/things", (i) => jsonBody({ n: i }));
  });

  test("schema facet: install, read back, fixed child in the listing", async () => {
    let res = await anon.put("/rdata/posts/p1", { json: { title: "a" } });
    status(res, 201, "[/rdata] seed record");
    res = await anon.put("/rdata/posts/.schema.json", { json: { type: "object" } });
    status(res, 200, "[/rdata] schema PUT is 200 (never 201)");
    res = await anon.get("/rdata/posts/.schema.json");
    status(res, 200, "[/rdata] schema reads back");
    expect(res.json<{ type: string }>().type).toBe("object");
    res = await anon.get("/rdata/posts/");
    status(res, 200, "[/rdata] listing");
    const listing = res.listing();
    expect(
      listing.entries.some((e) => e.name === ".schema.json"),
      `[/rdata] schema is a fixed child: ${JSON.stringify(listing)}`,
    ).toBe(true);
    res = await anon.delete("/rdata/posts/?confirm=posts");
    status(res, 204, "[/rdata] cleanup");
  });

  test("a missing adapter bundle is a clear error", async () => {
    const res = await anon.get("/undeployed/things/alpha");
    status(res, 404, "[/undeployed] undeployed adapter → 404");
    const problem = res.problem();
    expect(problem.detail).toBe("data adapter bundle 'code:ga-absent@v9' not found — deploy it via PUT /code/ga-absent");
  });

  test("a socket outside the store grant allowlist is denied with capability identity", async () => {
    const res = await anon.get("/denied/things/alpha");
    status(res, 403, "[/denied] socket denial");
    const problem = res.problem();
    expect(problem.code, `[/denied] ${res.describe()}`).toBe("capability_denied");
    expect(problem.detail).toBe(`capability 'socket 127.0.0.1:${redis.port}' is not granted to this service`);
  });
});

describe("guest-backed file store", () => {
  test("satisfies the store contract", async () => {
    await assertStoreContract(anon, "/gfiles", "/docs", (i) => textBody(`content-${i}`));
  });

  test("HEAD reports the size; a Range serves a 206 slice", async () => {
    let res = await anon.put("/gfiles/docs/alpha", textBody("content-2"));
    status(res, 201, "[/gfiles] seed");
    res = await anon.head("/gfiles/docs/alpha");
    status(res, 200, "[/gfiles] HEAD");
    expect(res.header("content-length"), "[/gfiles] HEAD content-length").toBe("9");
    res = await anon.get("/gfiles/docs/alpha", { headers: { range: "bytes=0-6" } });
    status(res, 206, "[/gfiles] range → 206");
    expect(res.text(), "[/gfiles] first 7 bytes").toBe("content");
    res = await anon.delete("/gfiles/docs/?confirm=docs");
    status(res, 204, "[/gfiles] cleanup");
  });
});

describe("guest-backed query store", () => {
  test("executes a stored query against the shared backend", async () => {
    for (const [k, s, total] of [
      ["o1", "open", 50],
      ["o2", "closed", 200],
      ["o3", "open", 150],
    ] as const) {
      const res = await anon.put(`/rdata/orders/${k}`, { json: { status: s, total } });
      status(res, 201, `[/q] seed ${k}`);
    }
    const envelope = {
      language: "json",
      query: { dataset: "orders", where: { status: "${status}" }, orderBy: "_key" },
      params: { type: "object", properties: { status: { type: "string" } } },
    };
    let res = await admin.put("/q/.queries/by-status", { json: envelope });
    status(res, 201, "[/q] author query");

    res = await anon.get("/q/by-status?status=open");
    status(res, 200, "[/q] execute");
    expect(res.header("x-total-count"), "[/q] X-Total-Count").toBe("2");
    const rows = res.json<Array<{ status: string }>>();
    expect(rows.length, `[/q] two open orders: ${JSON.stringify(rows)}`).toBe(2);
    expect(rows.every((r) => r.status === "open"), `[/q] only open orders: ${JSON.stringify(rows)}`).toBe(true);

    // Pagination narrows rows, not the reported total.
    res = await anon.get("/q/by-status?status=open&$take=1");
    status(res, 200, "[/q] paged execute");
    expect(res.header("x-total-count"), "[/q] paged total is the full count").toBe("2");
    expect(res.json<unknown[]>().length, "[/q] $take pages").toBe(1);

    res = await admin.delete("/q/.queries/by-status");
    status(res, 204, "[/q] cleanup query");
    res = await anon.delete("/rdata/orders/?confirm=orders");
    status(res, 204, "[/q] cleanup orders");
  });
});

describe("guest-backed mongo data store (OP_MSG + BSON)", () => {
  test("satisfies the store contract over the real wire protocol", async () => {
    await assertStoreContract(anon, "/mdata", "/orders", (i) => jsonBody({ status: "open", total: 50 + i }));
  });

  test("records round-trip; _id is stripped; schema is a separate collection", async () => {
    let res = await anon.put("/mdata/orders/o1", { json: { status: "open", total: 55 } });
    status(res, 201, "[/mdata] PUT create");
    res = await anon.get("/mdata/orders/o1");
    status(res, 200, "[/mdata] GET child");
    expect(res.etag(), "[/mdata] child GET carries ETag").not.toBeNull();
    const rec = res.json<Record<string, unknown>>();
    expect(rec.status).toBe("open");
    expect(rec.total).toBe(55);
    expect(rec._id, "[/mdata] _id is stripped from the record").toBeUndefined();

    res = await anon.put("/mdata/orders/.schema.json", { json: { type: "object" } });
    status(res, 200, "[/mdata] install schema");
    res = await anon.get("/mdata/orders/.schema.json");
    status(res, 200, "[/mdata] schema reads back");
    expect(res.json<{ type: string }>().type).toBe("object");
    const listing = (await anon.get("/mdata/orders/")).listing();
    expect(
      listing.entries.some((e) => e.name === ".schema.json"),
      `[/mdata] schema is a fixed child: ${JSON.stringify(listing)}`,
    ).toBe(true);

    res = await anon.delete("/mdata/orders/?confirm=orders");
    status(res, 204, "[/mdata] confirmed delete drops the collection");
    const root = (await anon.get("/mdata/")).listing();
    expect(entryNamed(root, "orders"), `[/mdata] dropped collection left the root: ${JSON.stringify(root)}`).toBeUndefined();
  });

  test("round-trips int64 values and decodes dates/ObjectIds seeded on the wire", async () => {
    let res = await anon.put("/mdata/wire/big", { json: { big: 3_000_000_000, max: 9_007_199_254_740_991 } });
    status(res, 201, "[/mdata] PUT big ints");
    // The mock's backing store holds what actually crossed the wire.
    const stored = mongo.store.get("wire")?.get("big");
    expect(stored?.big, "[/mdata] backend received the value intact").toBe(3_000_000_000);
    expect(stored?.max, "[/mdata] 2^53-1 crossed the wire intact").toBe(9_007_199_254_740_991);
    res = await anon.get("/mdata/wire/big");
    status(res, 200, "[/mdata] GET big ints");
    const rec = res.json<{ big: number; max: number }>();
    expect(rec.big, "[/mdata] int64 round-trips").toBe(3_000_000_000);
    expect(rec.max, "[/mdata] 2^53-1 round-trips").toBe(9_007_199_254_740_991);

    // Wire types a JSON PUT can't produce: seed the backend directly with
    // a UTC datetime + ObjectId (what real v1 data holds) and assert the
    // adapter's decode doesn't lose them.
    mongo.store.set(
      "legacy",
      new Map([
        [
          "v1",
          { _id: "v1", created: { $date: 1_704_067_200_000 }, owner: { $oid: "507f1f77bcf86cd799439011" }, n: 1 },
        ],
      ]),
    );
    res = await anon.get("/mdata/legacy/v1");
    status(res, 200, "[/mdata] GET seeded record");
    const legacy = res.json<Record<string, unknown>>();
    expect(legacy.created, "[/mdata] datetime decodes to ISO-8601").toBe("2024-01-01T00:00:00.000Z");
    expect(legacy.owner, "[/mdata] ObjectId decodes to 24-char hex").toBe("507f1f77bcf86cd799439011");
    expect(legacy.n).toBe(1);

    res = await anon.delete("/mdata/wire/?confirm=wire");
    status(res, 204, "[/mdata] cleanup wire");
    mongo.store.delete("legacy");
  });
});

describe("guest-backed mongo query adapter", () => {
  test("runs a stored aggregation with paging pushed down", async () => {
    for (const [k, account, name] of [
      ["p1", "acc1", "gamma"],
      ["p2", "acc2", "alpha"],
      ["p3", "acc1", "alpha"],
      ["p4", "acc1", "beta"],
    ] as const) {
      const res = await anon.put(`/mdata/projectItem/${k}`, { json: { accountId: account, name } });
      status(res, 201, `[/mq] seed ${k}`);
    }
    const envelope = {
      language: "json",
      query: {
        collection: "projectItem",
        pipeline: [{ $match: { accountId: "${accountId}" } }, { $sort: { name: 1 } }],
      },
      params: { type: "object", properties: { accountId: { type: "string" } } },
    };
    let res = await admin.put("/mq/.queries/items", { json: envelope });
    status(res, 201, "[/mq] author query");

    res = await anon.get("/mq/items?accountId=acc1");
    status(res, 200, "[/mq] execute");
    expect(res.header("x-total-count"), "[/mq] X-Total-Count").toBe("3");
    const rows = res.json<Array<{ accountId: string; name: string }>>();
    expect(rows.every((r) => r.accountId === "acc1"), `[/mq] only acc1 items: ${JSON.stringify(rows)}`).toBe(true);
    expect(rows.map((r) => r.name), "[/mq] $sort by name").toEqual(["alpha", "beta", "gamma"]);

    // $take/$skip page inside the facet; the total stays the full count.
    res = await anon.get("/mq/items?accountId=acc1&$take=1&$skip=1");
    status(res, 200, "[/mq] paged execute");
    expect(res.header("x-total-count"), "[/mq] paged total is the full count").toBe("3");
    const page = res.json<Array<{ name: string }>>();
    expect(page.length, "[/mq] $take pages").toBe(1);
    expect(page[0]!.name, "[/mq] $skip offsets into the sorted rows").toBe("beta");

    // A malformed stored query (no pipeline) is a clear 400 from the adapter.
    res = await admin.put("/mq/.queries/bad", { json: { language: "json", query: { collection: "projectItem" }, params: { type: "object" } } });
    status(res, 201, "[/mq] author malformed query");
    res = await anon.get("/mq/bad");
    status(res, 400, "[/mq] missing pipeline → 400");

    for (const q of ["items", "bad"]) {
      const del = await admin.delete(`/mq/.queries/${q}`);
      status(del, 204, `[/mq] cleanup ${q}`);
    }
    res = await anon.delete("/mdata/projectItem/?confirm=projectItem");
    status(res, 204, "[/mq] cleanup projectItem");
  });
});

describe("projected listings over guest data mounts", () => {
  test("host fallback (no features export) and native pushdown produce one answer", async () => {
    // (a) The Redis fixture does NOT export `features`: the host key-walk
    // fallback serves `$select`/`$sort` — never forwarded to the bundle.
    await assertListingContract(anon, "/rdata");
    // (b) The shipped Mongo bundle advertises `"list-records"`: the same
    // requests push down to a native `find` with projection/sort/skip/
    // limit — the mock sorts server-side, so the pushdown is exercised.
    await assertListingContract(anon, "/mdata");
  });

  test("the services catalogue reports listProjection per mount", async () => {
    const res = await anon.get("/.well-known/rs2/services");
    status(res, 200, "GET /.well-known/rs2/services");
    const doc = res.json<{ services: Array<{ path: string; listProjection?: string }> }>();
    const projection = (path: string) => doc.services.find((s) => s.path === path)?.listProjection;
    expect(projection("/rdata"), "no features export → host key-walk").toBe("fallback");
    expect(projection("/mdata"), "advertised list-records → native pushdown").toBe("native");
  });
});

describe("resident pooling (observable effect)", () => {
  test("a serial store-contract run pools its backend connection where the host can", async () => {
    await assertStoreContract(anon, "/pool", "/things", (i) => jsonBody({ n: i }), { label: "/pool" });
    const connections = redisPool.connections();
    if (divergences().guestAdapterPooling === "pooled") {
      // The resident isolate pooled ONE connection across the whole run.
      expect(connections, "[/pool] adapter pooled a single connection").toBe(1);
    } else {
      // I/O objects are request-scoped on this host: the adapter
      // reconnects per invocation (declared divergence; the pool-growth /
      // idle-eviction scenarios are Rust-only internals).
      expect(connections, "[/pool] at least one connection").toBeGreaterThanOrEqual(1);
    }
  });
});
