// External catalogues over HTTP — replaces `rs2-core/tests/catalogue.rs`
// (spec F.3). The catalogue client is a HOST capability bounded by the
// operator's `catalogueHosts` allowlist, so what this suite can assert
// depends on how the host was started:
//
//   * with an empty allowlist (the conformance fixture, and every default
//     run) the feature is DORMANT: `/services/catalogues` reports
//     `enabled: false`, `/services/catalogue/available` lists built-ins
//     only, and `POST /services/catalogue/install` is 501
//     `engine_unavailable` — `catalogue_feature_dormant_without_allowlist`.
//   * with `RS2_CATALOGUE_URL` pointing at `fixtures/catalogue-server.mjs`
//     AND that server's host allowlisted, the live half runs instead: the
//     fetch → hash-pin → compile-check → store path of
//     `list_available_and_install_from_catalogue`,
//     `install_rejects_non_allowlisted_host` and
//     `install_rejects_content_hash_mismatch`.
//
// The two halves are mutually exclusive because `enabled` is a property of
// the node, not of a request — so each `describe` guards on the env, never
// on the host kind.

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, type Rs2Response } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

/** The fixture catalogue server, when the runner started one (spec F.3). */
const LIVE_URL = process.env.RS2_CATALOGUE_URL;
const LIVE = Boolean(LIVE_URL);

/** A registered catalogue on a host no operator would allowlist. */
const EVIL_URL = "https://evil.catalogues.invalid/cat.json";
const EVIL_HOST = "evil.catalogues.invalid";
/** The dormant half's registered catalogue (never fetched — the client is off). */
const DORMANT_URL = "https://catalogues.test/cat.json";

interface AvailableItem {
  name?: string;
  kind?: string;
  source?: string;
  ref?: string;
  adapterKind?: string;
  installed?: boolean;
  catalogue?: string;
  error?: string;
  configSchema?: unknown;
  description?: unknown;
  [extra: string]: unknown;
}

async function available(seed: Seed): Promise<{ items: AvailableItem[]; res: Rs2Response }> {
  const res = await seed.admin.get("/services/catalogue/available");
  expect(res.status, `GET /services/catalogue/available: ${res.describe()}`).toBe(200);
  const items = res.json<{ items: AvailableItem[] }>().items;
  expect(Array.isArray(items), `items is an array: ${res.text().slice(0, 200)}`).toBe(true);
  return { items, res };
}

function install(seed: Seed, body: unknown): Promise<Rs2Response> {
  return seed.admin.post("/services/catalogue/install", body === undefined ? {} : { json: body });
}

// ---------------------------------------------------------------------------
// Dormant: no operator allowlist ⇒ no catalogue client at all.
// `catalogue_feature_dormant_without_allowlist`.
// ---------------------------------------------------------------------------

describe.skipIf(LIVE)("catalogue (dormant — no operator host allowlist)", () => {
  let seed: Seed;

  beforeAll(async () => {
    seed = await Seed.create();
    // A catalogue is registered in tenant config; the operator has still
    // allowlisted nothing, so the whole feature stays off.
    await seed.applyMounts([], { catalogues: [{ name: "main", url: DORMANT_URL }] });
  });
  afterAll(async () => {
    await seed?.restore();
  });

  test("[catalogues] the listing reports the feature off, the entry unallowlisted", async () => {
    const res = await seed.admin.get("/services/catalogues");
    expect(res.status, res.describe()).toBe(200);
    const doc = res.json<{ enabled: boolean; catalogues: any[] }>();
    expect(doc.enabled, `enabled: ${res.text()}`).toBe(false);
    expect(doc.catalogues.length, `one registered catalogue: ${res.text()}`).toBe(1);
    expect(doc.catalogues[0].name).toBe("main");
    expect(doc.catalogues[0].url).toBe(DORMANT_URL);
    // `url_host` of the registered URL, annotated for the operator.
    expect(doc.catalogues[0].host).toBe("catalogues.test");
    expect(doc.catalogues[0].allowlisted, "no allowlist ⇒ not allowlisted").toBe(false);
  });

  test("[available] lists built-ins only — no remote items, no error entries", async () => {
    const { items, res } = await available(seed);
    expect(items.length, `available is not empty: ${res.text().slice(0, 200)}`).toBeGreaterThan(0);
    // The Rust assertion: with the client off, EVERY item is built-in.
    for (const i of items) {
      expect(i.source, `every item is builtin: ${JSON.stringify(i)}`).toBe("builtin");
    }
    expect(
      items.some((i) => i.error !== undefined),
      "no degraded catalogue entries",
    ).toBe(false);
    expect(
      items.some((i) => i.catalogue !== undefined),
      "no catalogue-sourced items",
    ).toBe(false);
  });

  test("[available] carries built-in service types and built-in adapters", async () => {
    const { items } = await available(seed);
    const services = items.filter((i) => i.kind === "service");
    expect(services.length, "built-in service types are listed").toBeGreaterThan(0);
    for (const name of ["file", "data", "services", "auth", "pipeline"]) {
      const item = services.find((i) => i.name === name);
      expect(item, `built-in service '${name}' is selectable`).toBeDefined();
      expect(item!.source).toBe("builtin");
      expect(item!.configSchema, `'${name}' carries its configSchema`).toBeTypeOf("object");
    }
    // Built-in adapters, selectable as `builtin:<name>` (registry.rs names).
    const adapters = items.filter((i) => i.kind === "adapter");
    expect(
      adapters.some((i) => i.ref === "builtin:mem"),
      "builtin:mem is selectable",
    ).toBe(true);
    for (const [name, kind] of [
      ["mem", "data"],
      ["file", "data"],
      ["local", "file"],
      ["reference", "query"],
    ] as const) {
      const item = adapters.find((i) => i.ref === `builtin:${name}` && i.adapterKind === kind);
      expect(item, `built-in ${kind} adapter '${name}'`).toBeDefined();
      expect(item!.name).toBe(name);
      expect(item!.source).toBe("builtin");
    }
  });

  test("[install] is unavailable — 501 engine_unavailable", async () => {
    const res = await install(seed, { catalogue: "main", name: "greeter", version: "x" });
    expect(res.status, res.describe()).toBe(501);
    const p = res.problem();
    expect(p.code).toBe("engine_unavailable");
    expect(p.title).toBe("Engine Unavailable");
    expect(p.status).toBe(501);
    expect(p.detail, `names the missing operator allowlist: ${p.detail}`).toContain(
      "external catalogues are not enabled on this node",
    );
    expect(p.tenant).toBe(env().tenant);
    expect(typeof p.traceId).toBe("string");
    expect(res.header("x-trace-id"), "the trace id is echoed as a header").toBe(p.traceId);
  });

  test("[install] the dormant check precedes body and name validation", async () => {
    // No body at all, and an unregistered catalogue name: still 501, because
    // the missing client is checked before either.
    const noBody = await install(seed, undefined);
    expect(noBody.status, noBody.describe()).toBe(501);
    const unknown = await install(seed, { catalogue: "nope", name: "greeter", version: "x" });
    expect(unknown.status, unknown.describe()).toBe(501);
    expect(unknown.problem().code).toBe("engine_unavailable");
  });

  test("[access] the catalogue endpoints sit behind the mount's access spec", async () => {
    for (const path of ["/services/catalogues", "/services/catalogue/available"]) {
      const res = await seed.anon.get(path);
      expect(res.status, `anonymous GET ${path}: ${res.describe()}`).toBe(401);
    }
    const res = await seed.anon.post("/services/catalogue/install", {
      json: { catalogue: "main", name: "greeter", version: "x" },
    });
    expect(res.status, `anonymous install: ${res.describe()}`).toBe(401);
  });
});

// ---------------------------------------------------------------------------
// Live: the operator allowlisted the fixture server's host.
// ---------------------------------------------------------------------------

describe.runIf(LIVE)("catalogue (live — fixture server allowlisted)", () => {
  let seed: Seed;
  let version: string;
  let tamperVersion: string;
  const installed: string[] = [];

  beforeAll(async () => {
    // Read the fixture catalogue directly, so the expectations come from the
    // document the host will fetch rather than from a copy in this file.
    const doc = await (await fetch(LIVE_URL!)).json();
    const greeter = doc.items.find((i: any) => i.name === "greeter");
    const tamper = doc.items.find((i: any) => i.name === "tamper");
    if (!greeter || !tamper) {
      throw new Error(
        `RS2_CATALOGUE_URL '${LIVE_URL}' must serve the fixtures/catalogue-server.mjs document ` +
          `(items 'greeter' and 'tamper'); got ${JSON.stringify(doc).slice(0, 200)}`,
      );
    }
    version = greeter.version;
    tamperVersion = tamper.version;

    seed = await Seed.create();
    await seed.applyMounts([], {
      catalogues: [
        { name: "main", url: LIVE_URL! },
        { name: "evil", url: EVIL_URL },
      ],
    });
  });

  afterAll(async () => {
    for (const path of installed) await seed?.admin.delete(path);
    await seed?.admin.delete("/services/code/greeter/?confirm=greeter");
    await seed?.restore();
  });

  test("[catalogues] registered catalogues are annotated with allowlist status", async () => {
    const res = await seed.admin.get("/services/catalogues");
    expect(res.status, res.describe()).toBe(200);
    const doc = res.json<{ enabled: boolean; catalogues: any[] }>();
    expect(doc.enabled, `the feature is on: ${res.text()}`).toBe(true);
    const main = doc.catalogues.find((c) => c.name === "main");
    expect(main.url).toBe(LIVE_URL);
    expect(main.allowlisted, "the fixture server's host is allowlisted").toBe(true);
    const evil = doc.catalogues.find((c) => c.name === "evil");
    expect(evil.host).toBe(EVIL_HOST);
    expect(evil.allowlisted, "an unallowlisted host is flagged").toBe(false);
  });

  test("[available] aggregates built-ins, built-in adapters and remote items", async () => {
    const { items } = await available(seed);
    expect(items.some((i) => i.source === "builtin" && i.kind === "service")).toBe(true);
    expect(items.some((i) => i.kind === "adapter" && i.ref === "builtin:mem")).toBe(true);
    const greeter = items.find((i) => i.name === "greeter");
    expect(greeter, "the remote item is aggregated").toBeDefined();
    expect(greeter!.source).toBe("catalogue");
    expect(greeter!.catalogue).toBe("main");
    expect(greeter!.installed, "not installed yet").toBe(false);
    expect(greeter!.ref).toBe(`code:greeter@${version}`);
    // A catalogue that cannot be fetched degrades to an error entry rather
    // than failing the whole listing (`list_available`).
    const degraded = items.find((i) => i.catalogue === "evil");
    expect(degraded, "the unallowlisted catalogue degrades to an entry").toBeDefined();
    expect(degraded!.source).toBe("catalogue");
    expect(String(degraded!.error), `error names the denied fetch: ${degraded!.error}`).toContain(
      "catalogue fetch to",
    );
  });

  test("[install] fetch → hash pin → compile-check → store", async () => {
    const res = await install(seed, { catalogue: "main", name: "greeter", version });
    expect(res.status, res.describe()).toBe(201);
    const body = res.json<{ name: string; version: string; ref: string; validated: boolean }>();
    expect(body.name).toBe("greeter");
    expect(body.version).toBe(version);
    expect(body.ref).toBe(`code:greeter@${version}`);
    installed.push(`/services/code/greeter/${version}`);

    // available now reports it installed.
    const { items } = await available(seed);
    expect(items.find((i) => i.name === "greeter")!.installed).toBe(true);

    // The bundle is readable from the code store at its content ref.
    const read = await seed.admin.get(`/services/code/greeter/${version}`);
    expect(read.status, read.describe()).toBe(200);
    expect(read.text().length, "the stored bundle has the fetched bytes").toBeGreaterThan(0);
    expect(read.etag()).toBe(`"${version}"`);
  });

  test("[install] refuses a catalogue on a host the operator did not allowlist", async () => {
    const res = await install(seed, { catalogue: "evil", name: "x", version: "y" });
    expect(res.status, res.describe()).toBe(403);
    const p = res.problem();
    expect(p.code).toBe("capability_denied");
    expect(String(p.detail), p.detail).toContain(`catalogue fetch to '${EVIL_HOST}'`);
  });

  test("[install] refuses a bundle whose content hash is not the pinned version", async () => {
    const res = await install(seed, { catalogue: "main", name: "tamper", version: tamperVersion });
    expect(res.status, res.describe()).toBe(400);
    const p = res.problem();
    expect(String(p.detail), p.detail).toContain("does not match catalogue version");
    // Nothing was stored.
    const read = await seed.admin.get(`/services/code/tamper/${tamperVersion}`);
    expect(read.status, `nothing stored for the refused install: ${read.describe()}`).toBe(404);
  });

  test("[install] unknown catalogue / unknown item / missing field", async () => {
    const noCat = await install(seed, { catalogue: "absent", name: "greeter", version });
    expect(noCat.status, noCat.describe()).toBe(404);
    expect(String(noCat.problem().detail)).toContain("no registered catalogue 'absent'");

    const noItem = await install(seed, { catalogue: "main", name: "greeter", version: "deadbeef" });
    expect(noItem.status, noItem.describe()).toBe(404);
    expect(String(noItem.problem().detail)).toContain("has no item 'greeter@deadbeef'");

    const noField = await install(seed, { catalogue: "main", name: "greeter" });
    expect(noField.status, noField.describe()).toBe(400);
    expect(String(noField.problem().detail)).toContain("install requires 'version'");

    const noBody = await install(seed, undefined);
    expect(noBody.status, noBody.describe()).toBe(400);
    expect(String(noBody.problem().detail)).toContain("install requires a JSON body");
  });
});
