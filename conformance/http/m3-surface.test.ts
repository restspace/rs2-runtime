// M3 surface conformance over HTTP — replaces `rs2-core/tests/m3_surface.rs`
// for the cross-host contract (spec F.3): the `query` service, OPTIONS
// capability probes, the generated discovery surface (`/services`,
// `/agent-surface`, `/openapi`), structured pipeline failures, and the
// custom-code deploy lifecycle. Same order, same status codes, same header
// names as the Rust file; the tenant is that file's `surface_config()` laid
// over `conf.base.json`.
//
// One deliberate difference from the Rust tenant: `/services` keeps the
// fixture's `read: A, write: A` gate instead of `access: open`, so the
// `control` block is asserted through the admin client and its absence
// (null) through the anonymous one — both are the Rust behaviour
// (`readable_mounts` filters the services mount like any other).

import { readFileSync } from "node:fs";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, entryNamed, Rs2Client, type Rs2Response } from "./src/client.ts";
import { baseConfig, Seed, type Mount, type TenantConfig } from "./src/seed.ts";

function status(res: Rs2Response, want: number, msg: string): void {
  expect(res.status, `${msg}: ${res.describe()}`).toBe(want);
}

/** `surface_config()` from the Rust file, minus the base mounts the seed keeps. */
const SURFACE_MOUNTS: Mount[] = [
  { path: "/q", service: "query", config: { access: "open", "x-expose": ["mcp"] } },
  { path: "/summary", service: "pipeline", config: { access: "open", "x-agent": { kind: "action", safe: true } } },
  { path: "/broken", service: "pipeline", config: { access: "open" } },
  { path: "/secret", service: "file", config: { access: { read: "A", write: "A" } } },
  // From the smaller per-test configs of the Rust file, folded into one
  // tenant: a file store (not a spec store), the log view (media types),
  // and the template spec store (JSX authoring descriptor).
  { path: "/files", service: "file", config: { access: "open" } },
  { path: "/logs", service: "log", config: { access: "open" } },
  { path: "/t", service: "template", config: { access: "open" } },
];

/** The reshaped tenant document (base + surface mounts), for mid-suite edits. */
function surfaceConfig(): TenantConfig {
  const config = baseConfig();
  config.mounts = [...config.mounts, ...SURFACE_MOUNTS];
  return config;
}

/**
 * The envelope used across the query tests: structural `${...}` params, a
 * schema with a default, and an optional clause.
 */
const openOrdersEnvelope = {
  query: {
    dataset: "orders",
    where: { status: "${status}", total: { op: ">=", value: "${min}" }, name: "${name?}" },
    orderBy: "total",
  },
  params: {
    type: "object",
    required: ["status"],
    properties: { status: { type: "string" }, min: { type: "number", default: 0 }, name: { type: "string" } },
  },
  output: { type: "array" },
};

const WELL_KNOWN = "/.well-known/rs2";

describe("m3 surface", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  let admin: Rs2Client;

  /** Author the two demo pipelines as `.root` specs (one DSL, one typed). */
  async function authorPipelines(): Promise<void> {
    const summary = { pipeline: ["GET /data/orders/${id}", { status: "$.status" }] };
    let res = await anon.put("/summary/.pipelines/.root", { json: summary });
    expect([200, 201], `PUT /summary/.pipelines/.root: ${res.describe()}`).toContain(res.status);
    const broken = {
      pipeline: {
        onFail: "stop",
        steps: [{ call: { method: "GET", url: "/data/orders/missing-one" } }, { transform: { x: "$" } }],
      },
    };
    res = await anon.put("/broken/.pipelines/.root", { json: broken });
    expect([200, 201], `PUT /broken/.pipelines/.root: ${res.describe()}`).toContain(res.status);
  }

  async function seedOrders(): Promise<void> {
    for (const [key, st, total] of [
      ["o1", "open", 50],
      ["o2", "open", 10],
      ["o3", "closed", 99],
    ] as const) {
      const res = await anon.put(`/data/orders/${key}`, { json: { status: st, total } });
      status(res, 201, `seed /data/orders/${key}`);
    }
  }

  /** `GET /.well-known/rs2/<doc>` as JSON (200 asserted). */
  async function discovery(client: Rs2Client, doc: string): Promise<any> {
    const res = await client.get(`${WELL_KNOWN}/${doc}`);
    status(res, 200, `GET ${WELL_KNOWN}/${doc}`);
    return res.json();
  }

  const servicePaths = (doc: any): string[] => (doc.services as any[]).map((s) => s.path as string);
  const serviceEntry = (doc: any, path: string): any => {
    const entry = (doc.services as any[]).find((s) => s.path === path);
    if (!entry) throw new Error(`mount ${path} missing from ${JSON.stringify(doc)}`);
    return entry;
  };

  beforeAll(async () => {
    seed = await Seed.create();
    await seed.applyMounts(SURFACE_MOUNTS);
    anon = seed.anon;
    admin = seed.admin;
    seed.trackDataset("/data", "orders");
    seed.trackDir("/q/.queries", "orders");
    seed.trackDir("/files", "nothing-here");
    await seedOrders();
  });

  afterAll(async () => {
    if (!seed) return;
    // Spec files and code bundles are not tracked by the seed: remove them
    // while their mounts still exist (404s are fine — a test may have
    // deleted them itself), then hand back to the seed's restore.
    for (const path of [
      "/q/.queries/open-orders",
      "/q/.queries/sql-orders",
      "/summary/.pipelines/.root",
      "/broken/.pipelines/.root",
    ]) {
      const res = await anon.delete(path);
      expect([204, 404], `cleanup DELETE ${path}: ${res.describe()}`).toContain(res.status);
    }
    await seed.restore();
    for (const name of ["echo", "order-view", "broken", "lookup"]) {
      const res = await admin.delete(`/services/code/${name}/?confirm=${name}`);
      expect([204, 404], `cleanup DELETE /services/code/${name}/: ${res.describe()}`).toContain(res.status);
    }
  });

  // -------------------------------------------------------------------------
  // query service (PRD §10.4)
  // -------------------------------------------------------------------------

  test("query store authors, validates and executes", async () => {
    // Authoring is a store write under the reserved subtree: invalid
    // envelopes are refused at PUT time.
    let res = await anon.put("/q/.queries/open-orders", { json: { notquery: 1 } });
    status(res, 400, "[/q] invalid envelope is refused");
    res = await anon.put("/q/.queries/open-orders", { json: { query: {}, params: { type: 42 } } });
    status(res, 400, "[/q] invalid params schema is refused");

    // PUT the spec like a file; read it back; it lists as a store child.
    res = await anon.put("/q/.queries/open-orders", { json: openOrdersEnvelope });
    status(res, 201, "[/q] PUT spec");
    res = await anon.get("/q/.queries/open-orders");
    status(res, 200, "[/q] GET spec");
    expect(res.etag(), "[/q] FileService ETag on the spec read").not.toBeNull();
    expect(res.json().query.dataset).toBe("orders");
    const { listing } = await anon.listDir("/q/.queries/");
    expect(entryNamed(listing, "open-orders"), `[/q] spec lists as a child: ${JSON.stringify(listing)}`).toBeDefined();

    // Execute: schema default applies (min → 0) and the optional `name`
    // clause elides when the param is absent.
    res = await anon.post("/q/open-orders", { json: { status: "open" } });
    status(res, 200, "[/q] execute");
    expect(res.header("x-total-count"), "[/q] X-Total-Count").toBe("2");
    let totals = (res.json() as any[]).map((r) => r.total);
    expect(totals, "[/q] orderBy applied; default min=0 applied").toEqual([10, 50]);

    // Explicit min narrows; numbers stay numbers (structural substitution).
    res = await anon.post("/q/open-orders", { json: { status: "open", min: 20 } });
    status(res, 200, "[/q] execute with min");
    expect(res.header("x-total-count")).toBe("1");
    expect(res.json()[0].total).toBe(50);

    // Pagination caps results but keeps the true total.
    res = await anon.post("/q/open-orders?$take=1", { json: { status: "open" } });
    status(res, 200, "[/q] execute paged");
    expect(res.header("x-total-count"), "[/q] true total under $take").toBe("2");
    expect((res.json() as any[]).length).toBe(1);

    // Any-verb execution: plain GET with query-string params, coerced to
    // the schema's declared types (min: number).
    res = await anon.get("/q/open-orders?status=open&min=20");
    status(res, 200, "[/q] GET execution");
    expect(res.header("x-total-count")).toBe("1");
    expect(res.json()[0].total).toBe(50);

    // Parameters are schema-validated before execution → 422 with details.
    res = await anon.post("/q/open-orders", { json: { status: 42 } });
    status(res, 422, "[/q] invalid params");
    const problem = res.problem();
    expect(problem.code).toBe("validation_failed");
    expect(Array.isArray(problem.errors) && (problem.errors as unknown[]).length > 0, `[/q] errors: ${res.text()}`).toBe(true);

    // Unknown stored query → 404.
    res = await anon.post("/q/nope", { json: {} });
    status(res, 404, "[/q] unknown stored query");

    // DELETE removes it like a file; execution then 404s too.
    res = await anon.delete("/q/.queries/open-orders");
    status(res, 204, "[/q] DELETE spec");
    res = await anon.get("/q/.queries/open-orders");
    status(res, 404, "[/q] deleted spec is gone");
    res = await anon.post("/q/open-orders", { json: { status: "open" } });
    status(res, 404, "[/q] execution of a deleted spec");
  });

  test("query positional URL params and SQL passthrough", async () => {
    // Positional params: trailing URL segments beyond the stored spec
    // become "0", "1", … (v1's subpath params, no silent failures).
    let res = await anon.put("/q/.queries/orders/by-status", {
      json: { query: { dataset: "orders", where: { status: "${0}" } } },
    });
    status(res, 201, "[/q] nested spec path");
    res = await anon.post("/q/orders/by-status/closed");
    status(res, 200, "[/q] positional execution");
    expect(res.header("x-total-count")).toBe("1");
    expect(res.json()[0].total).toBe(99);

    // A missing positional is a 400, never a silent empty string.
    res = await anon.post("/q/orders/by-status");
    status(res, 400, "[/q] missing positional");

    // String-language (SQL) templates store fine but pass through to the
    // adapter unsubstituted — the reference adapter declines them.
    res = await anon.put("/q/.queries/sql-orders", {
      json: { language: "sql", query: "SELECT * FROM orders WHERE status = ${status}" },
    });
    status(res, 201, "[/q] SQL template stores");
    res = await anon.post("/q/sql-orders", { json: { status: "open" } });
    status(res, 501, "[/q] reference adapter is JSON-only");

    // The Rust file runs each test on a fresh runtime; the discovery tests
    // below count stored queries, so put the store back to empty.
    res = await anon.delete("/q/.queries/orders/?confirm=orders");
    status(res, 204, "[/q] cleanup nested spec dir");
    res = await anon.delete("/q/.queries/sql-orders");
    status(res, 204, "[/q] cleanup SQL spec");
  });

  test("OPTIONS probes mount capabilities", async () => {
    // OPTIONS describes the resolved mount: pattern, facets, and an Allow
    // header — one round trip, no correlation against the services list.
    let res = await anon.options("/data");
    status(res, 200, "OPTIONS /data");
    const allow = res.header("allow") ?? "";
    expect(allow.includes("PATCH") && allow.includes("OPTIONS"), `allow: ${allow}`).toBe(true);
    let doc = res.json();
    expect(doc.pattern).toBe("store");
    expect(doc.path).toBe("/data");
    expect((doc.facets as string[]).includes("schema"), res.text()).toBe(true);
    expect(doc.schemaUrlPattern).toBe("/data/{dataset}/.schema.json");

    // A sub-path resolves to its governing mount (longest-prefix match).
    res = await anon.options("/data/people/alice");
    status(res, 200, "OPTIONS on a sub-path");

    // The pipeline mount reports its own conversation shape.
    res = await anon.options("/summary");
    status(res, 200, "OPTIONS /summary");
    doc = res.json();
    expect(doc.pattern).toBe("store-transform");

    // The probe is read-gated: an unreadable mount stays hidden.
    res = await anon.options("/secret");
    status(res, 401, "OPTIONS on an unreadable mount");
  });

  // -------------------------------------------------------------------------
  // agent surface + OpenAPI (PRD §12)
  // -------------------------------------------------------------------------

  test("discovery surface filters and advertises", async () => {
    // Stored specs surface from their stores, not config: author them first.
    let res = await anon.put("/q/.queries/open-orders", { json: openOrdersEnvelope });
    expect([200, 201], `PUT /q/.queries/open-orders: ${res.describe()}`).toContain(res.status);
    await authorPipelines();

    // /services catalogue: anonymous caller sees readable mounts only —
    // /secret (read: "A") is filtered out.
    let doc = await discovery(anon, "services");
    let paths = servicePaths(doc);
    expect(paths).toContain("/data");
    expect(paths).toContain("/summary");
    expect(paths, "unreadable mounts are hidden").not.toContain("/secret");
    // The gated services mount is filtered like any other, and takes the
    // control block with it for an anonymous caller.
    expect(paths, "the read-gated services mount is hidden").not.toContain("/services");
    expect(doc.control, "control is null when the caller cannot read the services mount").toBeNull();

    // The control surface (config/catalogue/code) is advertised explicitly,
    // so a generic admin client has one stable entry point and needn't scan
    // for the `services` mount by name.
    doc = await discovery(admin, "services");
    expect(servicePaths(doc), "the operator sees the gated mounts").toContain("/secret");
    expect(doc.control.path).toBe("/services");
    expect(doc.control.config).toBe("/services/raw");
    expect(doc.control.catalogue).toBe("/services/catalogue");
    expect(doc.control.mounts).toBe("/services/services");
    expect(doc.control.code).toBe("/services/code/");

    // agent-surface: entities, actions (with idempotency guidance), queries
    // (with the same param schema enforced at runtime).
    doc = await discovery(anon, "agent-surface");
    expect(doc.entities[0].path).toBe("/data");
    // Stored pipelines list as actions; the .root spec's path is the mount.
    const action = (doc.actions as any[]).find((a) => a.path === "/summary");
    expect(action, `stored pipeline missing from actions: ${JSON.stringify(doc)}`).toBeDefined();
    expect(action.idempotency.header).toBe("Idempotency-Key");
    expect(action["x-agent"].kind).toBe("action");
    expect(String(action.plan), JSON.stringify(action)).toContain("/.pipelines/.root?$plan");
    const query = doc.queries[0];
    expect(query.path).toBe("/q/open-orders");
    expect(query.params.required[0]).toBe("status");

    // x-expose filtering: /q is exposed on "mcp" only.
    doc = await discovery(anon, "agent-surface?surface=ui");
    expect(doc.queries, JSON.stringify(doc)).toEqual([]);
    doc = await discovery(anon, "agent-surface?surface=mcp");
    expect((doc.queries as any[]).length).toBe(1);

    // OpenAPI 3.1: generated paths + the problem schema; store-patterned
    // mounts reference one shared shape (client polymorphism), and spec
    // stores expose the authoring subtree + an execution path item.
    doc = await discovery(anon, "openapi");
    expect(doc.openapi).toBe("3.1.0");
    expect(doc.paths["/data/{dataset}/{key}"].$ref).toBe("#/components/pathItems/StoreChild");
    expect(doc.components.pathItems.StoreContainer.get).toBeTypeOf("object");
    expect(doc.paths["/q/.queries/{specPath}"].$ref).toBe("#/components/pathItems/SpecChild");
    expect(doc.paths["/summary/.pipelines/{specPath}"].$ref).toBe("#/components/pathItems/SpecChild");
    expect(doc.paths["/q/{queryPath}"].get, "any-verb query execution").toBeTypeOf("object");
    expect(doc.paths["/summary/{path}"].get, "pipeline execution").toBeTypeOf("object");
    expect(doc.components.pathItems.SpecChild.put).toBeTypeOf("object");
    expect(doc.components.schemas.Problem).toBeTypeOf("object");

    // The surface is read-only.
    res = await anon.post(`${WELL_KNOWN}/services`);
    status(res, 405, "POST on the discovery surface");
  });

  // `?surface=` prunes the services catalogue exactly like the agent surface:
  // a mount with `x-expose` appears only on its listed surfaces; a mount
  // without `x-expose` appears on every surface. No param = no filtering.
  test("services catalogue filters by surface", async () => {
    // No ?surface param: /q lists despite its x-expose (unfiltered).
    let paths = servicePaths(await discovery(admin, "services"));
    expect(paths).toContain("/q");

    // /q is exposed on "mcp" only: gone from the editor surface, while
    // x-expose-less mounts appear on every surface.
    const doc = await discovery(admin, "services?surface=editor");
    paths = servicePaths(doc);
    expect(paths).not.toContain("/q");
    expect(paths, "x-expose-less mounts appear on every surface").toContain("/data");
    expect(paths, "x-expose-less mounts appear on every surface").toContain("/summary");
    // The services mount carries no x-expose, so control survives the filter.
    expect(doc.control.path).toBe("/services");

    paths = servicePaths(await discovery(admin, "services?surface=mcp"));
    expect(paths).toContain("/q");
  });

  // When the mounts backing the `control` block are scoped off the requested
  // surface via `x-expose`, the control entries pointing at them go too.
  test("services control block respects surface", async () => {
    const config = surfaceConfig();
    const services = config.mounts.find((m) => m.service === "services")!;
    services.config = { ...(services.config ?? {}), "x-expose": ["admin"] };
    await seed!.putConfig(config);
    try {
      let doc = await discovery(admin, "services?surface=editor");
      expect(doc.control, `control follows its filtered-out mount: ${JSON.stringify(doc)}`).toBeNull();
      doc = await discovery(admin, "services?surface=admin");
      expect(doc.control.path).toBe("/services");
    } finally {
      await seed!.putConfig(surfaceConfig());
    }
  });

  // A config `If-Match` may name its version weakly. Any intermediary that
  // recompresses a response rewrites the strong ETag it was given to `W/"v"`
  // — Cloudflare does this whenever it compresses, so a gzip-accepting
  // client never even sees the strong form — and the client echoes back what
  // it received. The version named is the same one, so the write applies.
  test("config If-Match accepts the weak form of the version", async () => {
    const { config, etag } = await seed!.currentConfig();
    const version = etag.replace(/^W\//, "").replace(/^"+|"+$/g, "");
    const res = await seed!.tryPutConfig(config, `W/"${version}"`);
    status(res, 204, "[weak if-match] PUT /services/raw with W/ prefixed version");
    // …and a genuinely stale version is still refused, weak or not.
    const stale = await seed!.tryPutConfig(config, 'W/"0000000000000000"');
    status(stale, 409, "[weak if-match] a stale weak version is still a conflict");
    await seed!.currentConfig();
  });

  // -------------------------------------------------------------------------
  // schema + media-type self-containment (discovery == enforcement)
  // -------------------------------------------------------------------------

  // A data mount inlines each installed dataset schema into OpenAPI: the live
  // `.schema.json` is registered under components/schemas and a concrete
  // `{dataset}/{key}` path binds it — self-contained, and the same schema the
  // data service enforces on write (no drift). Schema-less datasets keep the
  // generic templated StoreChild shape.
  test("data dataset schema inlines into OpenAPI", async () => {
    const schema = {
      type: "object",
      required: ["status"],
      properties: { status: { type: "string" }, total: { type: "number" } },
    };
    const res = await anon.put("/data/orders/.schema.json", { json: schema });
    status(res, 200, "PUT /data/orders/.schema.json");

    const doc = await discovery(anon, "openapi");
    // The generic templated child remains (schema-less datasets).
    expect(doc.paths["/data/{dataset}/{key}"].$ref).toBe("#/components/pathItems/StoreChild");
    // The orders dataset gets a concrete, schema-bound child path.
    const putOp = doc.paths["/data/orders/{key}"]?.put;
    expect(putOp, JSON.stringify(doc.paths)).toBeTypeOf("object");
    const schemaRef = putOp.requestBody?.content?.["application/json"]?.schema?.$ref;
    expect(typeof schemaRef, `schema-bound request body: ${JSON.stringify(putOp)}`).toBe("string");
    expect(schemaRef.startsWith("#/components/schemas/"), schemaRef).toBe(true);
    const key = schemaRef.split("/").pop()!;
    // The inlined schema is the live one — resolvable in-document, no drift.
    expect(doc.components.schemas[key].required[0]).toBe("status");
  });

  // A pipeline spec's optional `input`/`output` schemas surface on both the
  // agent surface (the action's uniform `inputSchema`/`outputSchema`) and
  // OpenAPI (the execute path item's request/response), advisory by nature.
  test("pipeline io schema surfaces", async () => {
    const envelope = {
      pipeline: ["GET /data/orders/${id}", { status: "$.status" }],
      input: { type: "object", properties: { id: { type: "string" } } },
      output: { type: "object", properties: { status: { type: "string" } } },
    };
    const res = await anon.put("/summary/.pipelines/.root", { json: envelope });
    expect([200, 201], `PUT /summary/.pipelines/.root: ${res.describe()}`).toContain(res.status);

    let doc = await discovery(anon, "agent-surface");
    const action = (doc.actions as any[]).find((a) => a.path === "/summary");
    expect(action, `pipeline action missing: ${JSON.stringify(doc)}`).toBeDefined();
    expect(action.inputSchema.properties.id.type).toBe("string");
    expect(action.outputSchema.properties.status.type).toBe("string");

    doc = await discovery(anon, "openapi");
    const post = doc.paths["/summary/{path}"].post;
    expect(post.requestBody.content["application/json"].schema.properties.id.type, JSON.stringify(post)).toBe("string");
    expect(post.responses["200"].content["application/json"].schema.properties.status.type).toBe("string");
  });

  // Operations advertise their media types: the log view declares both its
  // JSON and NDJSON (text/plain) representations on the `200`, rather than a
  // bare description.
  test("operations advertise media types", async () => {
    const doc = await discovery(anon, "openapi");
    const content = doc.paths["/logs"]?.get?.responses?.["200"]?.content;
    expect(content?.["application/json"], JSON.stringify(doc.paths["/logs"])).toBeTypeOf("object");
    expect(content?.["text/plain"], `NDJSON via Accept: ${JSON.stringify(content)}`).toBeTypeOf("object");
  });

  // A deployed `code:` mount with a declared manifest appears on both the agent
  // surface (as an action carrying its I/O schemas) and OpenAPI (a path item
  // with the manifest's schemas + media types).
  test("code manifest surfaces on discovery", async () => {
    const bundle = `export default async (msg, ctx) => ({ status: 200, body: {} });`;
    const manifest = {
      inputSchema: { type: "object", properties: { q: { type: "string" } } },
      outputSchema: { type: "object", properties: { n: { type: "number" } } },
      effect: "idempotent",
      requestMediaType: "application/json",
      responseMediaType: "application/json",
    };
    let res = await admin.post("/services/code/lookup/", {
      body: bundle,
      contentType: "application/javascript",
      headers: { "x-rs2-manifest": JSON.stringify(manifest) },
    });
    status(res, 201, "deploy with x-rs2-manifest");
    const codeRef = res.json().ref as string;

    const config = surfaceConfig();
    config.mounts.push({ path: "/lookup", service: codeRef, config: { access: "open", grants: {} } });
    await seed!.putConfig(config);
    try {
      // Agent surface: the code mount lists as an action with its schemas.
      let doc = await discovery(anon, "agent-surface");
      const action = (doc.actions as any[]).find((a) => a.path === "/lookup");
      expect(action, `code action missing: ${JSON.stringify(doc)}`).toBeDefined();
      expect(action.effect).toBe("idempotent");
      expect(action.inputSchema.properties.q.type).toBe("string");
      expect(action.outputSchema.properties.n.type).toBe("number");

      // OpenAPI: a path item carrying the manifest's request/response schemas.
      doc = await discovery(anon, "openapi");
      const post = doc.paths["/lookup/{path}"]?.post;
      expect(post, `code path item missing: ${JSON.stringify(Object.keys(doc.paths))}`).toBeTypeOf("object");
      expect(post.requestBody.content["application/json"].schema.properties.q.type, JSON.stringify(post)).toBe("string");
      const get = doc.paths["/lookup/{path}"].get;
      expect(get.responses["200"].content["application/json"].schema.properties.n.type).toBe("number");
    } finally {
      await seed!.putConfig(surfaceConfig());
    }
  });

  // -------------------------------------------------------------------------
  // structured pipeline failures (PRD §12)
  // -------------------------------------------------------------------------

  test("pipeline failures name the failing step", async () => {
    await authorPipelines();
    const res = await anon.get("/broken");
    status(res, 404, "step failure propagates");
    const problem = res.problem();
    expect(problem.code).toBe("not_found");
    const pipeline = problem.pipeline as any;
    expect(pipeline?.failedStep, res.text()).toBe("/0");
    expect(pipeline.steps[0].kind).toBe("call");
    expect(pipeline.steps[0].status).toBe(404);
  });

  // -------------------------------------------------------------------------
  // custom code deployment (PRD §10.6, §11)
  // -------------------------------------------------------------------------

  // The Rust file's featureless-wasm branch: the host under test carries the
  // JS engine only, so a header-valid but bogus component is accepted
  // unvalidated and its mount serves a structured 501 at request time.
  test("code deploys content-addressed and mounts", async () => {
    const fakeComponent = new TextEncoder().encode("\0asm-fake-component");
    const wasm = { body: fakeComponent, contentType: "application/wasm" };

    // Deploy is the store contract's keyless POST: content-derived child
    // name + Location.
    let res = await admin.post("/services/code/echo/", wasm);
    status(res, 201, "[code] deploy a fake component");
    const location = res.header("location") ?? "";
    expect(location.startsWith("/services/code/echo/"), `Location: ${location}`).toBe(true);
    let body = res.json();
    expect(body.validated, "[code] no wasm engine: accepted unvalidated").toBe(false);
    const codeRef = body.ref as string;
    const version = body.version as string;
    expect(codeRef.startsWith("code:echo@"), codeRef).toBe(true);

    // Re-deploying identical bytes yields the same version (immutable,
    // content-addressed — PRD §14).
    res = await admin.post("/services/code/echo/", wasm);
    status(res, 201, "[code] re-deploy identical bytes");
    expect(res.json().ref).toBe(codeRef);

    // Store-contract listings: bundle names at the root (dir entries),
    // versions inside; the standard dir+json shape an editor UI walks.
    res = await admin.get("/services/code/");
    status(res, 200, "[code] root listing");
    const root = res.listing();
    expect(entryNamed(root, "echo")?.dir, JSON.stringify(root)).toBe(true);
    res = await admin.get("/services/code/echo/");
    status(res, 200, "[code] container listing");
    // The bundle just deployed is listed under its container, named by its
    // hash. Not "the container holds exactly one": a persistent instance (a
    // deployed Worker, as opposed to a host started fresh per run) keeps
    // every version an earlier suite deployed.
    let listing = res.listing();
    expect(Number(res.header("x-total-count")), JSON.stringify(listing)).toBeGreaterThanOrEqual(1);
    const entry = listing.entries.find((e) => e.name.startsWith(version));
    expect(entry, `no entry for ${version}: ${JSON.stringify(listing)}`).toBeDefined();
    const childName = entry!.name;
    expect(entry!.mountedAt, "nothing mounts it yet").toBeUndefined();

    // Read back via the listing's child name AND the bare version.
    for (const path of [`/services/code/echo/${childName}`, `/services/code/echo/${version}`]) {
      res = await admin.get(path);
      status(res, 200, `[code] GET ${path}`);
      expect(res.etag()).toBe(`"${version}"`);
      expect(res.header("cache-control") ?? "").toContain("immutable");
      expect(Array.from(res.bytes.slice(0, 4)), "the stored bytes round-trip").toEqual([0, 0x61, 0x73, 0x6d]);
    }
    res = await admin.get("/services/code/echo/feedf00ddeadbeef");
    status(res, 404, "[code] unknown version");

    // PUT child: content-addressed — only the true hash name is accepted.
    res = await admin.put(`/services/code/echo/${version}`, wasm);
    status(res, 200, "[code] idempotent re-upload");
    res = await admin.put("/services/code/echo/0000000000000000", wasm);
    status(res, 409, "[code] PUT at a wrong hash");

    // Mount it via self-config; without the wasm feature the mount builds
    // but serves a structured 501 at request time.
    const config = surfaceConfig();
    config.mounts.push({ path: "/custom", service: codeRef, config: { access: "open", grants: {} } });
    await seed!.putConfig(config);

    // The listing now annotates the live version with its mount path…
    res = await admin.get("/services/code/echo/");
    status(res, 200, "[code] container listing while mounted");
    listing = res.listing();
    expect(listing.entries[0].mountedAt?.[0], JSON.stringify(listing)).toBe("/custom");

    // …and a mounted version refuses deletion (repoint first).
    res = await admin.delete(`/services/code/echo/${version}`);
    status(res, 409, "[code] mounted version refuses deletion");

    res = await anon.get("/custom/hello");
    status(res, 501, "[code] wasm mount without a wasm engine");
    expect(res.problem().code).toBe("engine_unavailable");

    // Repoint away (back to the surface config), then deletion works and
    // the container can be confirm-deleted.
    await seed!.putConfig(surfaceConfig());
    res = await admin.delete(`/services/code/echo/${version}`);
    status(res, 204, "[code] DELETE unmounted version");
    res = await admin.delete("/services/code/echo/?confirm=echo");
    status(res, 204, "[code] confirmed container delete");
  });

  // Deploy a JS bundle through the self-config API, mount it with a
  // capability grant, and serve a request that round-trips through the
  // grant back into the data service.
  test("deployed JS bundle serves requests with grants", async () => {
    const bundle = `
      export default async (msg, ctx) => {
        const order = await ctx.request("orders", { url: "/o1" });
        ctx.log("info", \`loaded order o1: \${order.status}\`);
        return {
          status: 200,
          headers: { "x-engine": "js" },
          body: { engine: "js", orderStatus: order.body.status },
        };
      };
    `;
    let res = await admin.post("/services/code/order-view/", { body: bundle, contentType: "application/javascript" });
    status(res, 201, "[code] deploy JS bundle");
    const body = res.json();
    expect(body.validated, "compile smoke test ran").toBe(true);
    const codeRef = body.ref as string;

    // Mount it with a grant scoping the capability to /data/orders.
    const config = surfaceConfig();
    config.mounts.push({
      path: "/order-view",
      service: codeRef,
      config: { access: "open", grants: { orders: { prefix: "/data/orders" } } },
    });
    await seed!.putConfig(config);
    try {
      res = await anon.get("/order-view/anything");
      status(res, 200, "[code] JS mount serves");
      expect(res.header("x-engine")).toBe("js");
      expect(res.json().orderStatus, "grant round-tripped into the data service").toBe("open");
    } finally {
      await seed!.putConfig(surfaceConfig());
    }

    // A broken bundle is rejected at deploy time by the compile smoke test.
    res = await admin.post("/services/code/broken/", { body: "export default ((((", contentType: "application/javascript" });
    status(res, 502, "[code] broken bundle rejected at deploy");
  });

  // Spec stores advertise their reserved authoring subtree (`.queries`/
  // `.pipelines`/`.templates`) on discovery, so a generic client learns the
  // authoring root without special-casing service names. Non-spec stores
  // omit it.
  test("specSubtree and authoring advertised on spec stores", async () => {
    const doc = await discovery(anon, "services");
    expect(serviceEntry(doc, "/q").specSubtree).toBe(".queries");
    expect(serviceEntry(doc, "/summary").specSubtree).toBe(".pipelines");
    expect(serviceEntry(doc, "/files").specSubtree, "file is not a spec store").toBeUndefined();
    expect(serviceEntry(doc, "/data").specSubtree, "data is not a spec store").toBeUndefined();

    // query edits specs as plain JSON — no `authoring` descriptor; pipeline
    // advertises its DSL round-trip; file/data are not spec stores.
    expect(serviceEntry(doc, "/q").authoring, "query authors plain JSON").toBeUndefined();
    expect(serviceEntry(doc, "/summary").authoring).toEqual({
      kind: "pipeline-dsl",
      compiledField: "pipeline",
      sourceField: "x-source",
    });
    expect(serviceEntry(doc, "/files").authoring).toBeUndefined();
    expect(serviceEntry(doc, "/data").authoring).toBeUndefined();

    // Also folded into the OPTIONS capability probe.
    let res = await anon.options("/q");
    status(res, 200, "OPTIONS /q");
    const desc = res.json();
    expect(desc.specSubtree).toBe(".queries");
    expect(desc.authoring).toBeUndefined();
    res = await anon.options("/summary");
    status(res, 200, "OPTIONS /summary");
    expect(res.json().authoring.kind).toBe("pipeline-dsl");
  });

  // The `template` spec store (JS engine) advertises `.templates` and the JSX
  // authoring descriptor so a generic client knows how to edit/compile it.
  test("specSubtree and authoring advertised on template", async () => {
    const doc = await discovery(anon, "services");
    const t = serviceEntry(doc, "/t");
    expect(t.specSubtree).toBe(".templates");
    expect(t.authoring).toEqual({
      kind: "jsx",
      framework: "preact",
      compiledField: "source",
      sourceField: "jsxSource",
      render: "html",
    });

    // Also on the OPTIONS capability probe.
    const res = await anon.options("/t");
    status(res, 200, "OPTIONS /t");
    expect(res.json().authoring.kind).toBe("jsx");
  });
});
