// Pipeline authoring and execution over HTTP — replaces (spec F.3)
// `rs2-core/tests/pipeline_access.rs`, `pipeline_response.rs`,
// `pipeline_validation.rs`, `operator_authority.rs` and `wrapper.rs`. Each
// `describe` block reshapes the tenant into that Rust file's mount set and
// walks its assertions in order.
//
// Stored specs live in the tenant's file store under the mount path and
// outlive a config swap, so every block deletes what it authored while its
// mount (and the role that may delete there) still exists — and clears any
// leftover from an earlier run before it starts.

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, Rs2Client, seg } from "./src/client.ts";
import { type Mount, Seed } from "./src/seed.ts";

/** A self-contained spec that returns a constant (no downstream calls). */
function returnsConstant(): Record<string, unknown> {
  return { pipeline: { steps: [{ transform: { ok: true } }] } };
}

const U = { email: "u@conf.test", password: "u-pw", roles: "U" };
const E = { email: "e@conf.test", password: "e-pw", roles: "E" };
const DEV = { email: "dev@conf.test", password: "dev-pw", roles: "dev" };
const OP = { email: "op@conf.test", password: "op-pw", roles: "op" };

/** Delete a stored spec, tolerating its absence. */
async function dropSpec(client: Rs2Client, path: string): Promise<void> {
  const res = await client.delete(path);
  if (![204, 404].includes(res.status)) throw new Error(`cleanup ${res.describe()}`);
}

describe("pipeline", () => {
  let seed: Seed;
  let anon: Rs2Client;
  let admin: Rs2Client;
  let asU: Rs2Client;
  let asE: Rs2Client;
  let asDev: Rs2Client;
  let asOp: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    anon = seed.anon;
    admin = seed.admin;
    // Principals are minted once, under the base config (`operatorRoles: A`,
    // so the admin may write the user records); the blocks below only
    // reshape mounts, and user records live in `/data/users` regardless.
    await seed.createPrincipals([U, E, DEV, OP]);
    asU = await seed.clientAs(U);
    asE = await seed.clientAs(E);
    asDev = await seed.clientAs(DEV);
    asOp = await seed.clientAs(OP);
    seed.trackDataset("/data", "things");
    seed.trackDataset("/data", "widgets");
  });

  afterAll(async () => {
    await seed?.restore();
  });

  // ---- pipeline_access.rs ------------------------------------------------
  // A pipeline mount's execution surface is authorized per spec: the matched
  // spec's `access` overrides the mount's floor per key (`.root` is the
  // mount-wide floor), evaluated with the verb->action map (POST -> invoke).
  describe("per-spec access (pipeline_access.rs)", () => {
    const SPECS = ["/pa/.pipelines/.root", "/pa/.pipelines/login", "/pa/.pipelines/audit", "/pa/.pipelines/ok"];

    beforeAll(async () => {
      // Execution locked to admins by default; specs override per path.
      await seed.applyMounts([{ path: "/pa", service: "pipeline", config: { access: { invoke: "A", write: "A" } } }]);
      for (const p of SPECS) await dropSpec(admin, p);
    });
    afterAll(async () => {
      for (const p of SPECS) await dropSpec(admin, p);
    });

    test("a spec overrides the mount floor either way", async () => {
      // `.root`: no per-spec access -> inherits the mount floor (invoke "A").
      let res = await admin.put("/pa/.pipelines/.root", { json: returnsConstant() });
      expect(res.status, res.describe()).toBe(201);
      // `/login`: loosened to public.
      res = await admin.put("/pa/.pipelines/login", { json: { ...returnsConstant(), access: { invoke: "all" } } });
      expect(res.status, res.describe()).toBe(201);
      // `/audit`: tightened to a different role.
      res = await admin.put("/pa/.pipelines/audit", { json: { ...returnsConstant(), access: { invoke: "E" } } });
      expect(res.status, res.describe()).toBe(201);

      // Public spec: an anonymous POST runs it.
      res = await anon.post("/pa/login", { json: {} });
      expect(res.status, `[/pa/login anon] ${res.describe()}`).toBe(200);
      expect(res.json().ok).toBe(true);
      // Unmatched path falls to `.root` (floor invoke "A"): anonymous -> 401.
      res = await anon.post("/pa/other", { json: {} });
      expect(res.status, `[/pa/other anon] ${res.describe()}`).toBe(401);
      expect(res.problem().tenant).toBe(env().tenant);
      // Tightened spec: anonymous -> 401, wrong role -> 403, right role runs.
      res = await anon.post("/pa/audit", { json: {} });
      expect(res.status, `[/pa/audit anon] ${res.describe()}`).toBe(401);
      res = await asU.post("/pa/audit", { json: {} });
      expect(res.status, `[/pa/audit U] ${res.describe()}`).toBe(403);
      res = await asE.post("/pa/audit", { json: {} });
      expect(res.status, `[/pa/audit E] ${res.describe()}`).toBe(200);
      expect(res.json().ok).toBe(true);
    });

    test("the envelope's access shape is validated at write time", async () => {
      // `manage` is not a spec action.
      let res = await admin.put("/pa/.pipelines/m", { json: { ...returnsConstant(), access: { manage: "A" } } });
      expect(res.status, res.describe()).toBe(400);
      expect(res.problem().detail).toContain("unknown access key 'manage'");
      // Unknown key (typo guard).
      res = await admin.put("/pa/.pipelines/t", { json: { ...returnsConstant(), access: { invokeRoles: "all" } } });
      expect(res.status, res.describe()).toBe(400);
      expect(res.problem().detail).toContain("unknown access key 'invokeRoles'");
      // Non-string role spec.
      res = await admin.put("/pa/.pipelines/n", { json: { ...returnsConstant(), access: { invoke: ["all"] } } });
      expect(res.status, res.describe()).toBe(400);
      expect(res.problem().detail).toContain("access 'invoke' must be a role-spec string");
      // Nothing was stored by a rejected PUT.
      for (const name of ["m", "t", "n"]) {
        const missing = await admin.get(`/pa/.pipelines/${name}`);
        expect(missing.status, `[rejected spec ${name} not stored] ${missing.describe()}`).toBe(404);
      }
      // Valid access object.
      res = await admin.put("/pa/.pipelines/ok", { json: { ...returnsConstant(), access: { read: "all", invoke: "U" } } });
      expect(res.status, res.describe()).toBe(201);
    });
  });

  // ---- pipeline_response.rs ------------------------------------------------
  // A transform whose output is `{"$response": {...}}` sets the response
  // status/headers/mediaType/body; captured transforms and plain outputs keep
  // the default behaviour.
  describe("$response shaping (pipeline_response.rs)", () => {
    const ROOT = "/pipe/.pipelines/.root";

    beforeAll(async () => {
      await seed.applyMounts([{ path: "/pipe", service: "pipeline", config: { access: { invoke: "all", write: "A" } } }]);
      await dropSpec(admin, ROOT);
    });
    afterAll(async () => {
      await dropSpec(admin, ROOT);
    });

    async function author(pipeline: unknown): Promise<void> {
      const res = await admin.put(ROOT, { json: { pipeline } });
      expect([200, 201], `author: ${res.describe()}`).toContain(res.status);
    }

    test("the envelope sets status, headers and media type", async () => {
      await author([
        {
          $response: {
            status: "201",
            headers: { Location: "'/things/1'" },
            mediaType: "'application/hal+json'",
            body: { made: "true" },
          },
        },
      ]);
      const res = await anon.post("/pipe", { json: {} });
      expect(res.status, `status from envelope: ${res.describe()}`).toBe(201);
      expect(res.header("location")).toBe("/things/1");
      expect(res.contentType(), "mediaType applied").toBe("application/hal+json");
      expect(res.json().made).toBe(true);
    });

    test("a string body is raw text and plain objects stay 200", async () => {
      // v1 `to-text`: JSON in, text/plain out.
      await author([{ $response: { body: "'rendered text'" } }]);
      let res = await anon.post("/pipe", { json: {} });
      expect(res.status, res.describe()).toBe(200);
      expect(res.contentType()).toBe("text/plain");
      expect(res.text()).toBe("rendered text");

      // A plain transform output is unaffected: body + forced 200.
      await author([{ shaped: "false" }]);
      res = await anon.post("/pipe", { json: {} });
      expect(res.status, res.describe()).toBe(200);
      expect(res.json().shaped).toBe(false);
    });

    test("captured envelopes are data, not directives", async () => {
      // Typed steps: capture the envelope, then emit plain data. If capture
      // applied the directive, the response would be the envelope's 500.
      await author({
        steps: [
          { transform: { $response: { status: "500" } }, as: "$env" },
          { transform: { captured: "$exists($env)" } },
        ],
      });
      const res = await anon.post("/pipe", { json: {} });
      expect(res.status, `capture path shapes nothing: ${res.describe()}`).toBe(200);
      expect(res.json().captured).toBe(true);
    });

    test("an invalid status is a structured 400", async () => {
      await author([{ $response: { status: "1000" } }]);
      const res = await anon.post("/pipe", { json: {} });
      expect(res.status, `bad status surfaces: ${res.describe()}`).toBe(400);
      const problem = res.problem();
      expect(problem.code).toBe("bad_request");
      expect(problem.detail).toContain("$response.status 1000 is not a valid HTTP status");
      expect(problem.tenant).toBe(env().tenant);
      // Note: the shaping error is raised by the executor itself, not by a
      // failing step response, so the `pipeline: {failedStep, steps}` block
      // of PRD section 12 is (by design) not attached here.
    });
  });

  // ---- pipeline_validation.rs ----------------------------------------------
  // Write-time spec validation: an unparseable JSONata expression is rejected
  // with 422 `validation_failed` even in a branch execution would never take.
  describe("write-time validation (pipeline_validation.rs)", () => {
    const ROOT = "/pv/.pipelines/.root";

    beforeAll(async () => {
      await seed.applyMounts([{ path: "/pv", service: "pipeline", config: { access: { invoke: "all", write: "A" } } }]);
      await dropSpec(admin, ROOT);
    });
    afterAll(async () => {
      await dropSpec(admin, ROOT);
    });

    test("PUT rejects an invalid expression in a dead branch", async () => {
      // Conditional: the first arm always matches; the second arm is dead at
      // runtime but carries an unparseable expression. The PUT must still fail.
      let res = await admin.put(ROOT, {
        json: {
          pipeline: {
            mode: "conditional",
            steps: [
              { if: "status == 200", transform: { ok: "$" } },
              { if: "status == 999", transform: { bad: "$sum((" } },
            ],
          },
        },
      });
      expect(res.status, res.describe()).toBe(422);
      const problem = res.problem();
      expect(problem.code).toBe("validation_failed");
      const errors = JSON.stringify(problem.errors);
      expect(errors, `error names the dead branch: ${errors}`).toContain("steps[1]");
      expect(errors).toContain("invalid JSONata expression");

      // Nothing was stored: the mount root has no spec to execute.
      const missing = await anon.post("/pv", { json: {} });
      expect(missing.status, missing.describe()).toBe(404);
      expect(missing.problem().code).toBe("not_found");

      // The same spec with the expression fixed is accepted.
      res = await admin.put(ROOT, {
        json: {
          pipeline: {
            mode: "conditional",
            steps: [
              { if: "status == 200", transform: { ok: "$" } },
              { if: "status == 999", transform: { fine: "$sum(lines.price)" } },
            ],
          },
        },
      });
      expect([200, 201], res.describe()).toContain(res.status);
    });
  });

  // ---- operator_authority.rs -----------------------------------------------
  // Only a principal holding an `operatorRoles` role may set or change a
  // spec's `access` (authorization is operator-controlled config, not author
  // content). A non-operator may still author and edit the logic.
  describe("operator authority over spec access (operator_authority.rs)", () => {
    const JOB = "/po/.pipelines/job";

    test("only operators may set or change a spec's access", async () => {
      // `op` is the operator role; `dev` may author but not set authority.
      await seed.applyMounts(
        [{ path: "/po", service: "pipeline", config: { access: { invoke: "all", write: "dev op" } } }],
        { operatorRoles: "op" },
      );
      await dropSpec(asOp, JOB);
      try {
        // A non-operator author (`dev`) may write a spec with no `access`.
        let res = await asDev.put(JOB, { json: returnsConstant() });
        expect(res.status, `[dev plain] ${res.describe()}`).toBe(201);

        // ...but may not introduce an `access` field.
        const withAccess = { ...returnsConstant(), access: { invoke: "all" } };
        res = await asDev.put(JOB, { json: withAccess });
        expect(res.status, `[dev sets access] ${res.describe()}`).toBe(403);
        expect(res.problem().detail).toContain("requires an operator");

        // An operator may set it.
        res = await asOp.put(JOB, { json: withAccess });
        expect(res.status, `[op sets access] ${res.describe()}`).toBe(200);

        // The non-operator may keep editing the logic, resending the SAME access.
        const edited: Record<string, unknown> = {
          pipeline: { steps: [{ transform: { ok: false } }] },
          access: { invoke: "all" },
        };
        res = await asDev.put(JOB, { json: edited });
        expect(res.status, `logic edit preserving access: ${res.describe()}`).toBe(200);

        // ...but may not change the access while editing.
        edited.access = { invoke: "op" };
        res = await asDev.put(JOB, { json: edited });
        expect(res.status, `[dev tightens] ${res.describe()}`).toBe(403);

        // ...nor remove it.
        res = await asDev.put(JOB, { json: returnsConstant() });
        expect(res.status, `[dev removes] ${res.describe()}`).toBe(403);
      } finally {
        await dropSpec(asOp, JOB);
      }
    });

    test("no operatorRoles means no API operator", async () => {
      // With operatorRoles absent, nobody is an operator over the API: setting
      // a spec's access is refused for everyone (authority is file-only).
      await seed.applyMounts(
        [{ path: "/po", service: "pipeline", config: { access: { write: "all" } } }],
        { operatorRoles: undefined },
      );
      await dropSpec(anon, JOB);
      try {
        let res = await anon.put(JOB, { json: { ...returnsConstant(), access: { invoke: "all" } } });
        expect(res.status, `[anon sets access] ${res.describe()}`).toBe(403);
        // Even the tenant admin (role A) is no operator without operatorRoles.
        res = await admin.put(JOB, { json: { ...returnsConstant(), access: { invoke: "all" } } });
        expect(res.status, `[admin sets access] ${res.describe()}`).toBe(403);

        // A spec without access is still freely authorable (write = "all").
        res = await anon.put(JOB, { json: returnsConstant() });
        expect(res.status, `[anon plain] ${res.describe()}`).toBe(201);
      } finally {
        await dropSpec(anon, JOB);
      }
    });
  });

  // ---- wrapper.rs ------------------------------------------------------------
  // `wrapper`: one inline pipeline fronting another mount, exact-path
  // passthrough via `${url.rest}`, host-enforced access, and a config-declared
  // discovery pattern.
  describe("wrapper (wrapper.rs)", () => {
    const wrapperMount = (config: Record<string, unknown>, path = "/wrapper"): Mount => ({
      path,
      service: "wrapper",
      config,
    });

    test("a wrapper forwards the exact path", async () => {
      await seed.applyMounts([wrapperMount({ access: "open", pipeline: ["GET /data${url.rest}"] })]);

      // Seed a record straight on the wrapped mount.
      let res = await anon.put("/data/things/abc", { json: { v: 1 } });
      expect(res.status, res.describe()).toBe(201);

      // GET /wrapper/things/abc -> /data/things/abc (exact sub-path).
      res = await anon.get("/wrapper/things/abc");
      expect(res.status, res.describe()).toBe(200);
      expect(res.json().v).toBe(1);

      // GET /wrapper/things/ -> /data/things/ (directory listing - trailing slash kept).
      res = await anon.get("/wrapper/things/");
      expect(res.status, res.describe()).toBe(200);
      expect(res.listing().entries.map((e) => e.name)).toContain("abc");
    });

    test("a wrapper without access is denied at the host", async () => {
      await seed.applyMounts([wrapperMount({ pipeline: ["GET /data${url.rest}"] })]);
      const res = await anon.get("/wrapper/x");
      expect(res.status, res.describe()).toBe(401);
      expect(res.problem().tenant).toBe(env().tenant);
      // Authenticated: still no policy -> 403, never open by default.
      const authed = await asU.get("/wrapper/x");
      expect(authed.status, authed.describe()).toBe(403);
    });

    test("a wrapper declares its discovery pattern", async () => {
      await seed.applyMounts([
        wrapperMount({ access: "open", pattern: "store", facets: ["schema", "patch"], pipeline: ["GET /data${url.rest}"] }),
      ]);
      const res = await anon.get("/.well-known/rs2/services");
      expect(res.status, res.describe()).toBe(200);
      const doc = res.json();
      const wrapper = (doc.services as any[]).find((s) => s.path === "/wrapper");
      expect(wrapper, `wrapper in catalogue: ${res.text()}`).toBeDefined();
      expect(wrapper.pattern).toBe("store");
      expect(wrapper.facets, `facets: ${JSON.stringify(wrapper.facets)}`).toContain("schema");
    });

    test("a hashing store facade hashes on write and lists via ${url.rest}", async () => {
      // The seed-auth shape: a `store`-pattern wrapper fronting `/data/users`
      // with a conditional GET/PUT pipeline that hashes `password` on write.
      await seed.applyMounts([
        wrapperMount(
          {
            access: "open",
            pattern: "store",
            pipeline: {
              mode: "conditional",
              steps: [
                { if: "method == 'GET'", call: { method: "GET", url: "/data/users${url.rest}" } },
                {
                  if: "method == 'PUT'",
                  pipeline: {
                    mode: "serial",
                    steps: [
                      {
                        transform: {
                          passwordHash: "$hashPassword(password)",
                          roles: "roles ? roles : 'U'",
                          kind: "kind ? kind : 'user'",
                        },
                      },
                      { call: { method: "PUT", url: "/data/users${url.rest}", effect: "idempotent" } },
                    ],
                  },
                },
              ],
            },
          },
          "/users",
        ),
      ]);
      const ada = "ada@example.com";
      try {
        // PUT a user through the facade: plaintext password is hashed, never stored.
        let res = await anon.put(`/users/${seg(ada)}`, { json: { password: "ada-secret-pw", roles: "U" } });
        expect([200, 201], res.describe()).toContain(res.status);

        // GET the key back: a hashed record, no plaintext `password`.
        res = await anon.get(`/users/${seg(ada)}`);
        expect(res.status, res.describe()).toBe(200);
        const rec = res.json();
        expect(rec.password, `plaintext must not be stored: ${res.text()}`).toBeUndefined();
        expect(String(rec.passwordHash), `argon2id hash expected: ${res.text()}`).toMatch(/^\$argon2/);
        expect(rec.roles).toBe("U");
        expect(rec.kind).toBe("user");

        // The listing works via ${url.rest} = "/": GET /users/ -> GET /data/users/.
        res = await anon.get("/users/");
        expect(res.status, res.describe()).toBe(200);
        expect(res.listing().entries.map((e) => e.name)).toContain(ada);

        // The minted hash is a working credential on this host.
        const login = await anon.post("/auth/login", { json: { email: ada, password: "ada-secret-pw" } });
        expect(login.status, `login with the facade-minted hash: ${login.describe()}`).toBe(200);
      } finally {
        await dropSpec(admin, `/data/users/${seg(ada)}`);
      }
    });

    test("a declared inputSchema is enforced and surfaced", async () => {
      await seed.applyMounts([
        wrapperMount(
          {
            access: "open",
            pattern: "store",
            inputSchema: {
              type: "object",
              required: ["name"],
              properties: { name: { type: "string" } },
              additionalProperties: false,
            },
            outputSchema: { type: "object", properties: { name: { type: "string" } } },
            pipeline: {
              mode: "conditional",
              steps: [
                { if: "method == 'GET'", call: { method: "GET", url: "/data${url.rest}" } },
                { if: "method == 'PUT'", call: { method: "PUT", url: "/data${url.rest}", effect: "idempotent" } },
              ],
            },
          },
          "/things",
        ),
      ]);

      // A body that violates inputSchema is rejected 422 before the pipeline runs.
      let res = await anon.put("/things/widgets/x", { json: { nope: 1 } });
      expect(res.status, res.describe()).toBe(422);
      const problem = res.problem();
      expect(problem.code).toBe("validation_failed");
      expect(Array.isArray(problem.errors), `422 carries an errors array: ${res.text()}`).toBe(true);
      // ...and nothing reached the wrapped store.
      const untouched = await anon.get("/data/widgets/x");
      expect(untouched.status, untouched.describe()).toBe(404);

      // A valid body passes through to the wrapped store.
      res = await anon.put("/things/widgets/x", { json: { name: "ok" } });
      expect([200, 201], res.describe()).toContain(res.status);
      const stored = await anon.get("/data/widgets/x");
      expect(stored.status, stored.describe()).toBe(200);
      expect(stored.json().name).toBe("ok");

      // Discovery catalogue carries both schemas on the wrapper entry.
      const svcs = await anon.get("/.well-known/rs2/services");
      expect(svcs.status, svcs.describe()).toBe(200);
      const entry = (svcs.json().services as any[]).find((s) => s.path === "/things");
      expect(entry, `wrapper entry: ${svcs.text()}`).toBeDefined();
      expect(entry.inputSchema.required[0]).toBe("name");
      expect(entry.outputSchema).toBeTypeOf("object");

      // OpenAPI: the wrapper path's PUT carries the inputSchema as a requestBody.
      const oapi = await anon.get("/.well-known/rs2/openapi");
      expect(oapi.status, oapi.describe()).toBe(200);
      const api = oapi.json();
      const put = api.paths?.["/things/{path}"]?.put;
      expect(put, `openapi path item for /things/{path}: ${Object.keys(api.paths ?? {}).join(", ")}`).toBeDefined();
      expect(put.requestBody?.content?.["application/json"]?.schema?.required?.[0], `openapi requestBody schema: ${JSON.stringify(put)}`).toBe("name");

      // Agent surface: a store-pattern wrapper is an entity carrying the schemas.
      const agent = await anon.get("/.well-known/rs2/agent-surface");
      expect(agent.status, agent.describe()).toBe(200);
      const ent = (agent.json().entities as any[]).find((e) => e.path === "/things");
      expect(ent, `wrapper entity on agent surface: ${agent.text()}`).toBeDefined();
      expect(ent.kind).toBe("entity");
      expect(ent.inputSchema).toBeTypeOf("object");
    });

    // A malformed `inputSchema` / unknown `pattern` is a config error at tenant
    // build time. Over the wire the build is the dry run behind
    // `PUT /services/raw`, so the bad config is refused (400) and never
    // becomes the running tenant: the version is unchanged and the mount
    // does not exist.
    test("a malformed inputSchema is a config error", async () => {
      await seed.applyMounts([]);
      const before = seed.etag;
      const res = await seed.tryPutConfig({
        ...(await seed.currentConfig()).config,
        mounts: [
          ...(await seed.currentConfig()).config.mounts,
          wrapperMount({ access: "open", inputSchema: "not-a-schema", pipeline: ["GET /data${url.rest}"] }, "/things"),
        ],
      });
      expect(res.status, res.describe()).toBe(400);
      const problem = res.problem();
      expect(problem.code).toBe("bad_request");
      expect(problem.detail).toContain("wrapper 'inputSchema' is not a valid JSON Schema");
      expect(seed.etag, "config version unchanged").toBe(before);
      const probe = await anon.get("/things/x");
      expect(probe.status, `the rejected mount never exists: ${probe.describe()}`).toBe(404);
    });

    test("an unknown pattern is a config error", async () => {
      await seed.applyMounts([]);
      const before = seed.etag;
      const { config } = await seed.currentConfig();
      config.mounts.push(wrapperMount({ access: "open", pattern: "bogus", pipeline: ["GET /data${url.rest}"] }));
      const res = await seed.tryPutConfig(config);
      expect(res.status, res.describe()).toBe(400);
      const problem = res.problem();
      expect(problem.code).toBe("bad_request");
      expect(problem.detail).toContain("wrapper mount '/wrapper' declares unknown pattern 'bogus'");
      expect(seed.etag, "config version unchanged").toBe(before);
      const probe = await anon.get("/wrapper/x");
      expect(probe.status, `the rejected mount never exists: ${probe.describe()}`).toBe(404);
    });
  });
});
