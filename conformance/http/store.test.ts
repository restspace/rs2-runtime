// Store-pattern conformance over HTTP — replaces
// `rs2-core/tests/store_conformance.rs` for the cross-host contract
// (spec F.3, first row). Every assertion of that file's sections 1a–1d is
// here, driven through `src/store-contract.ts`.

import { readFileSync } from "node:fs";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, Rs2Client } from "./src/client.ts";
import { Seed } from "./src/seed.ts";
import {
  assertCodeStoreContract,
  assertListingContract,
  assertMetaSortContract,
  assertStoreContract,
  jsonBody,
  textBody,
} from "./src/store-contract.ts";

describe("store contract", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  let admin: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    // The Rust file's tenant: file, data, query, pipeline — all open.
    await seed.applyMounts([
      { path: "/files", service: "file", config: { access: "open" } },
      { path: "/q", service: "query", config: { access: "open" } },
      { path: "/pipes", service: "pipeline", config: { access: "open" } },
    ]);
    anon = seed.anon;
    admin = seed.admin;
    // Belt and braces: every contract run cleans up after itself, but a
    // failed assertion mid-way must not leak into the next suite.
    seed.trackDir("/files", "docs");
    seed.trackDir("/files", "msort");
    seed.trackDataset("/data", "things");
    seed.trackDataset("/data", "posts");
    seed.trackDir("/q/.queries", "reports");
    seed.trackDir("/q/.queries", "msort");
    seed.trackDir("/pipes/.pipelines", "flows");
  });

  afterAll(async () => {
    await seed?.restore();
  });

  test("file service satisfies the store contract", async () => {
    await assertStoreContract(anon, "/files", "/docs", (i) => textBody(`content-${i}`));
  });

  test("data service satisfies the store contract", async () => {
    await assertStoreContract(anon, "/data", "/things", (i) => jsonBody({ n: i }));
  });

  // Spec stores' authoring subtrees satisfy the same contract as file and
  // data — by construction: they delegate to an owned FileService (the
  // SpecStore facade), so this suite tests one implementation through
  // several doors.
  test("query authoring subtree satisfies the store contract", async () => {
    await assertStoreContract(anon, "/q/.queries", "/reports", (i) => jsonBody({ query: { dataset: "orders", v: i } }));
  });

  test("pipeline authoring subtree satisfies the store contract", async () => {
    await assertStoreContract(anon, "/pipes/.pipelines", "/flows", (i) => jsonBody({ pipeline: [`GET /step${i}`] }));
  });

  test("code store: keyless POST only, PUT must name the true hash", async () => {
    const bundle = readFileSync(env().codeBundle, "utf8");
    await assertCodeStoreContract(admin, "/services/code", "echo", bundle);
  });

  test("data service satisfies the listing contract", async () => {
    await assertListingContract(anon, "/data");
  });

  test("file service satisfies the meta-sort contract", async () => {
    await assertMetaSortContract(anon, "/files", (i) => textBody("x".repeat(i * 100)));
  });

  // Spec stores delegate their authoring subtrees to an owned FileService,
  // so they inherit the meta-sort facet — held to it here through the
  // query store's door.
  test("query authoring subtree satisfies the meta-sort contract", async () => {
    await assertMetaSortContract(anon, "/q/.queries", (i) =>
      jsonBody({ query: { dataset: "orders", pad: "x".repeat(i * 100) } }),
    );
  });

  // Facets are additive: they must not alter the shared shape.
  test("facets extend without forking the shape", async () => {
    // data "echo" facet: POST to a child upserts AND returns the stored
    // representation (PUT stays empty-bodied on every store).
    let res = await anon.post("/data/things/alpha", { json: { n: 9 } });
    expect(res.status, res.describe()).toBe(201);
    expect(res.json().n).toBe(9);

    // data "schema" facet: the schema is a fixed child visible in the listing.
    res = await anon.put("/data/things/.schema.json", { json: { type: "object" } });
    expect(res.status, res.describe()).toBe(200);
    const { listing } = await anon.listDir("/data/things/");
    expect(
      listing.entries.some((e) => e.name === ".schema.json"),
      JSON.stringify(listing),
    ).toBe(true);

    // The discovery surface declares pattern + facets per mount.
    const doc = await anon.getJson("/.well-known/rs2/services");
    const byPath = (p: string) => {
      const entry = (doc.services as any[]).find((s) => s.path === p);
      if (!entry) throw new Error(`mount ${p} missing from ${JSON.stringify(doc)}`);
      return entry;
    };
    expect(byPath("/files").pattern).toBe("store");
    expect(byPath("/data").pattern).toBe("store");
    expect(byPath("/data").facets).toContain("schema");
    expect(byPath("/files").facets).toContain("range");

    res = await anon.delete("/data/things/?confirm=things");
    expect(res.status, res.describe()).toBe(204);
  });
});
