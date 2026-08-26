// Access and URL-shape conformance over HTTP — replaces, for the cross-host
// contract (spec F.3), five Rust test files:
//
//   access_vocab.rs             verb→action defaults (read/write/delete/invoke)
//   field_authz.rs              per-field redaction / 403 on the data service
//   dir_listing_negotiation.rs  Accept-negotiated listings, Vary, operator bypass
//   dir_no_slash.rs             301 slash redirects (query preserved), dir beats probe
//   friendly_urls.rs            Content-Location, pinning, HEAD, conditional delete
//
// Each `describe` mirrors one Rust file: same tenant shape, same assertions
// in the same order. Where the Rust file spins a fresh runtime per test,
// the test here cleans up what it created (file mounts on one host share the
// tenant's file tree, so leftovers would shadow the next case).
//
// Principals: the Rust files stamp a principal straight onto the message
// (`as_role`). Over the wire the host mints them — users created through
// the seeding facade, then `POST /auth/login`. `A` is the bootstrap admin
// and the tenant's operator role (`operatorRoles: "A"` in `conf.base.json`).

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "vitest";

import { names, Rs2Client, type Rs2Response } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

const DIR_JSON = "application/vnd.rs2.dir+json";

/**
 * The host's authorization verdict, independent of whatever the service
 * does afterward (`access_vocab.rs::denial`): 401, 403, or `null` when the
 * request was admitted to the service.
 */
function denial(res: Rs2Response): number | null {
  return res.status === 401 || res.status === 403 ? res.status : null;
}

/** Assert a 401/403 with the matching problem+json `code`. */
function expectDenied(res: Rs2Response, status: 401 | 403): void {
  expect(res.status, res.describe()).toBe(status);
  const problem = res.problem();
  expect(problem.code, res.describe()).toBe(status === 401 ? "unauthorized" : "forbidden");
  expect(problem.status).toBe(status);
}

/** A PUT that must create (`put_ok` in the Rust files: 201, never 200). */
async function putOk(client: Rs2Client, path: string, content: string, contentType: string): Promise<Rs2Response> {
  const res = await client.put(path, { body: content, contentType });
  expect(res.status, `PUT ${path}: ${res.describe()}`).toBe(201);
  return res;
}

/** Delete files (404 tolerated) — the per-test cleanup a fresh tempdir gave the Rust tests. */
async function wipeFiles(client: Rs2Client, paths: string[]): Promise<void> {
  for (const p of paths) {
    const res = await client.delete(p);
    if (![204, 404].includes(res.status)) throw new Error(`cleanup DELETE ${p}: ${res.describe()}`);
  }
}

/** Confirm-delete directories (404 tolerated). */
async function wipeDirs(client: Rs2Client, dirs: string[]): Promise<void> {
  for (const d of dirs) {
    const leaf = d.replace(/\/$/, "").split("/").pop();
    const res = await client.delete(`${d.replace(/\/?$/, "/")}?confirm=${leaf}`);
    if (![204, 404].includes(res.status)) throw new Error(`cleanup DELETE ${d}: ${res.describe()}`);
  }
}

// ---------------------------------------------------------------------------
// access_vocab.rs — the v2 access vocabulary (PRD §5.2): `read`
// (GET/HEAD/OPTIONS), `write` (PUT/PATCH), `delete` (DELETE), `invoke` (POST
// and other non-idempotent verbs). `delete` and `invoke` default to `write`;
// `read` defaults to `"all"`, `write` to `"A"`.
// ---------------------------------------------------------------------------
describe("access vocabulary (access_vocab.rs)", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  let asA: Rs2Client;
  let asE: Rs2Client;
  let asU: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    await seed.applyMounts([
      {
        path: "/x",
        service: "data",
        config: { access: { read: "all", write: "A", delete: "all", invoke: "E" } },
      },
      { path: "/y", service: "data", config: { access: { write: "A" } } },
    ]);
    await seed.createPrincipals([
      { email: "e@conf.test", password: "e-pw", roles: "E" },
      { email: "u@conf.test", password: "u-pw", roles: "U" },
    ]);
    anon = seed.anon;
    asA = seed.admin;
    asE = await seed.clientAs({ email: "e@conf.test", password: "e-pw" });
    asU = await seed.clientAs({ email: "u@conf.test", password: "u-pw" });
    // `/x` and `/y` are doors onto the tenant's one data store; the dataset
    // is reachable (and confirm-deletable) through `/data` too.
    seed.trackDataset("/data", "things");
  });

  afterAll(async () => {
    await seed?.restore();
  });

  test("each action is gated independently", async () => {
    // read = "all": anonymous GET is admitted.
    expect(denial(await anon.get("/x/things/k"))).toBeNull();
    // delete = "all": anonymous DELETE is admitted (split out of write).
    expect(denial(await anon.delete("/x/things/k"))).toBeNull();
    // invoke = "E": anonymous POST must authenticate; an `E` principal
    // passes, a `U` principal is forbidden.
    expectDenied(await anon.post("/x/things/", { json: { n: 1 } }), 401);
    let res = await asE.post("/x/things/", { json: { n: 1 } });
    expect(denial(res), res.describe()).toBeNull();
    expect(res.status, res.describe()).toBe(201);
    expectDenied(await asU.post("/x/things/", { json: { n: 1 } }), 403);
    // write = "A": PUT needs A; a `U` principal is forbidden, `A` passes.
    expectDenied(await asU.put("/x/things/k", { json: { n: 2 } }), 403);
    res = await asA.put("/x/things/k", { json: { n: 2 } });
    expect(denial(res), res.describe()).toBeNull();
    expect(res.status, res.describe()).toBe(201);
  });

  test("delete and invoke default to write; read defaults open", async () => {
    // read defaults to "all": anonymous GET admitted.
    expect(denial(await anon.get("/y/things/k"))).toBeNull();
    // delete defaults to write ("A"): anonymous denied, U forbidden, A passes.
    expectDenied(await anon.delete("/y/things/k"), 401);
    expectDenied(await asU.delete("/y/things/k"), 403);
    const res = await asA.delete("/y/things/k");
    expect(denial(res), res.describe()).toBeNull();
    // invoke defaults to write ("A"): anonymous POST must authenticate.
    expectDenied(await anon.post("/y/things/", { json: { n: 1 } }), 401);
  });

  test("OPTIONS is a read: admitted anonymously where read is open", async () => {
    // `action_for` maps OPTIONS to `read`; the host answers the capability
    // probe itself once access passes.
    const res = await anon.options("/x/things/");
    expect(denial(res), res.describe()).toBeNull();
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("allow"), res.describe()).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// field_authz.rs — field-level authorization on the `data` service (PRD
// §5.2). With `fieldLevelAuthz: true`, per-field `x-rs-read` / `x-rs-write`
// rules in the dataset schema redact unreadable fields on read and reject
// changes to unwritable fields on write; editing the schema requires an
// operator. `/open` is the same store with the flag off (back-compat).
// ---------------------------------------------------------------------------
describe("field-level authorization (field_authz.rs)", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  let asA: Rs2Client;
  let asU: Rs2Client;

  /** `name` open; `roles` readable but admin-write-only; `ssn` admin-only both ways. */
  const schema = {
    type: "object",
    properties: {
      name: { type: "string" },
      roles: { type: "string", "x-rs-write": "A" },
      ssn: { type: "string", "x-rs-read": "A", "x-rs-write": "A" },
    },
  };

  /** Seed schema (as operator A) + one record (A may write every field). */
  async function reseed(mount = "/data", dataset = "people"): Promise<void> {
    const s = await asA.put(`${mount}/${dataset}/.schema.json`, { json: schema });
    expect(s.status, `seed schema: ${s.describe()}`).toBe(200);
    const r = await asA.put(`${mount}/${dataset}/p1`, { json: { name: "Ada", roles: "U", ssn: "123" } });
    expect(r.status >= 200 && r.status < 300, `seed record: ${r.describe()}`).toBe(true);
  }

  beforeAll(async () => {
    seed = await Seed.create();
    await seed.applyMounts([
      { path: "/data", service: "data", config: { fieldLevelAuthz: true, access: { read: "all", write: "all" } } },
      { path: "/open", service: "data", config: { fieldLevelAuthz: false, access: { read: "all", write: "all" } } },
    ]);
    await seed.createPrincipals([{ email: "u@conf.test", password: "u-pw", roles: "U" }]);
    anon = seed.anon;
    asA = seed.admin;
    asU = await seed.clientAs({ email: "u@conf.test", password: "u-pw" });
    seed.trackDataset("/data", "people");
    seed.trackDataset("/data", "folk");
  });

  beforeEach(async () => {
    await reseed();
  });

  afterAll(async () => {
    await seed?.restore();
  });

  test("reads redact unreadable fields", async () => {
    const asUres = await asU.get("/data/people/p1");
    expect(asUres.status, asUres.describe()).toBe(200);
    const uBody = asUres.json();
    expect(uBody.name).toBe("Ada");
    expect(uBody.roles, "roles is readable (write-only restriction)").toBe("U");
    expect(uBody.ssn, "ssn redacted for U").toBeUndefined();

    const asAres = await asA.get("/data/people/p1");
    expect(asAres.json().ssn, "admin sees ssn").toBe("123");
  });

  test("writes to unwritable fields are rejected", async () => {
    // U changing `roles` (readable, admin-write-only) → 403, by PUT and PATCH.
    const put = await asU.put("/data/people/p1", { json: { name: "Ada", roles: "U A" } });
    expect(put.status, `PUT role escalation: ${put.describe()}`).toBe(403);
    expect(put.problem().code).toBe("forbidden");
    const patch = await asU.patch("/data/people/p1", { json: { roles: "U A" } });
    expect(patch.status, `PATCH role escalation: ${patch.describe()}`).toBe(403);
    expect(patch.problem().code).toBe("forbidden");

    // A may change roles.
    const ok = await asA.patch("/data/people/p1", { json: { roles: "U E" } });
    expect(ok.status >= 200 && ok.status < 300, `admin sets roles: ${ok.describe()}`).toBe(true);
  });

  test("PATCH round trip preserves redacted fields", async () => {
    // U reads (no ssn), edits a visible field, PATCHes it back.
    const got = await asU.get("/data/people/p1");
    expect(got.json().ssn).toBeUndefined();
    const patch = await asU.patch("/data/people/p1", { json: { name: "Ada L" } });
    expect(patch.status >= 200 && patch.status < 300, `PATCH visible field: ${patch.describe()}`).toBe(true);
    // The PATCH response is redacted for the caller too.
    expect(patch.json().ssn, "PATCH response redacted").toBeUndefined();

    // ssn is intact; name updated.
    const rec = (await asA.get("/data/people/p1")).json();
    expect(rec.name).toBe("Ada L");
    expect(rec.ssn, "redacted ssn preserved across the round-trip").toBe("123");
  });

  test("PUT back a redacted record preserves hidden fields", async () => {
    // U GETs the redacted record and PUTs the exact body back (no ssn in it).
    const redacted = (await asU.get("/data/people/p1")).json();
    expect(redacted.ssn).toBeUndefined();
    const put = await asU.put("/data/people/p1", { json: redacted });
    expect(put.status >= 200 && put.status < 300, `PUT redacted body back: ${put.describe()}`).toBe(true);

    const after = await asA.get("/data/people/p1");
    expect(after.json().ssn, "ssn not dropped by full PUT").toBe("123");
  });

  test("schema writes require an operator", async () => {
    // A non-operator cannot edit the policy-bearing schema.
    const byU = await asU.put("/data/people/.schema.json", { json: schema });
    expect(byU.status, byU.describe()).toBe(403);
    expect(byU.problem().code).toBe("forbidden");
    // The operator can.
    const byA = await asA.put("/data/people/.schema.json", { json: schema });
    expect(byA.status, byA.describe()).toBe(200);
  });

  test("flag off is back-compat", async () => {
    // fieldLevelAuthz: false — seed the schema as anyone (no operator gate).
    const s = await anon.put("/open/folk/.schema.json", { json: schema });
    expect(s.status, `schema write ungated: ${s.describe()}`).toBe(200);
    const r = await anon.put("/open/folk/p1", { json: { name: "Ada", roles: "U", ssn: "123" } });
    expect(r.status >= 200 && r.status < 300, r.describe()).toBe(true);

    // No redaction, no field gate: U sees ssn and can change roles.
    const got = await asU.get("/open/folk/p1");
    expect(got.json().ssn, "no redaction when flag off").toBe("123");
    const patch = await asU.patch("/open/folk/p1", { json: { roles: "U A" } });
    expect(patch.status >= 200 && patch.status < 300, `no field gate when flag off: ${patch.describe()}`).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// dir_listing_negotiation.rs — Accept-negotiated directory listings on the
// `file` service. On a static-site mount (`defaultResource` set) a directory
// GET serves the default doc; a deliberate `Accept: application/vnd.rs2.dir+json`
// opts into the listing at the same URL; browsers (`*/*`) still get the
// default doc. `listings: false` is a hard suppression for any ordinary
// caller — tenant operators excepted (their listing is `no-store`) — and
// every directory-GET response carries `Vary: Accept`.
//
// Note: Node's fetch sends `Accept: */*` when a test sets none, which is
// exactly the wildcard case the Rust file separately pins as "not a listing
// request" — the no-Accept and browser cases coincide on the wire.
// ---------------------------------------------------------------------------
describe("negotiated directory listings (dir_listing_negotiation.rs)", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  let op: Rs2Client;
  let editor: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    // `/site` — static-site mount (default doc, listings on).
    // `/locked` — static-site mount with listings suppressed, and the public
    // caching a real anonymously-readable site would carry.
    await seed.applyMounts([
      { path: "/site", service: "file", config: { access: "open", defaultResource: "index.html" } },
      {
        path: "/locked",
        service: "file",
        config: {
          access: "open",
          defaultResource: "index.html",
          listings: false,
          caching: { mode: "cache", maxAgeSeconds: 300, public: true },
        },
      },
    ]);
    // A non-operator role, however privileged in the mount's `access`.
    await seed.createPrincipals([{ email: "editor@conf.test", password: "editor-pw", roles: "editor" }]);
    anon = seed.anon;
    op = seed.admin; // role `A` = `operatorRoles`
    editor = await seed.clientAs({ email: "editor@conf.test", password: "editor-pw" });
    seed.trackDir("/site", "docs");
  });

  // Both mounts front the same tenant tree, so wipe between cases.
  afterEach(async () => {
    await wipeFiles(op, ["/site/index.html", "/site/data.json", "/site/secret.json"]);
  });

  afterAll(async () => {
    await seed?.restore();
  });

  test("explicit dir+json Accept forces the listing", async () => {
    await putOk(anon, "/site/index.html", "<html>home</html>", "text/html");
    await putOk(anon, "/site/data.json", "{}", "application/json");

    // Explicit dir+json → the listing, not the default doc.
    const res = await anon.get("/site/", { headers: { accept: DIR_JSON } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.contentType()).toBe(DIR_JSON);
    expect(res.header("vary")).toBe("accept");
    const listed = names(res.listing());
    expect(listed.includes("index.html") && listed.includes("data.json"), JSON.stringify(listed)).toBe(true);
  });

  test("no Accept serves the default doc with Vary", async () => {
    await putOk(anon, "/site/index.html", "<html>home</html>", "text/html");

    const res = await anon.get("/site/");
    expect(res.status, res.describe()).toBe(200);
    expect(res.contentType()).toBe("text/html");
    // Negotiation is in play here, so the default-doc response advertises Vary too.
    expect(res.header("vary")).toBe("accept");
    expect(res.text()).toBe("<html>home</html>");
  });

  test("browser wildcard Accept serves the default doc", async () => {
    await putOk(anon, "/site/index.html", "<html>home</html>", "text/html");

    // A browser's Accept includes `*/*` — which must NOT be read as a listing request.
    const res = await anon.get("/site/", {
      headers: { accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8" },
    });
    expect(res.status, res.describe()).toBe(200);
    expect(res.contentType()).toBe("text/html");
    expect(res.text()).toBe("<html>home</html>");
  });

  test("listings:false suppresses even an explicit dir+json", async () => {
    await putOk(anon, "/locked/index.html", "<html>home</html>", "text/html");
    await putOk(anon, "/locked/secret.json", "{}", "application/json");

    // Anonymous: negotiation cannot bypass the suppression.
    let res = await anon.get("/locked/", { headers: { accept: DIR_JSON } });
    expect(res.status, res.describe()).toBe(404);

    // Nor can a non-operator role, however privileged in the mount's `access`.
    res = await editor.get("/locked/", { headers: { accept: DIR_JSON } });
    expect(res.status, res.describe()).toBe(404);

    // The default doc is still served to a normal request.
    const normal = await anon.get("/locked/");
    expect(normal.status, normal.describe()).toBe(200);
  });

  test("operator sees the listing through listings:false", async () => {
    await putOk(anon, "/locked/index.html", "<html>home</html>", "text/html");
    await putOk(anon, "/locked/secret.json", "{}", "application/json");

    // `listings: false` is concealment, not authorization: an operator can
    // flip the flag in tenant config anyway, so they get the listing on request.
    const res = await op.get("/locked/", { headers: { accept: DIR_JSON } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.contentType()).toBe(DIR_JSON);

    // The representation is principal-dependent, and this mount is publicly
    // cacheable — the operator's listing must never enter a shared cache.
    expect(res.header("cache-control")).toBe("no-store");
    const vary = (res.header("vary") ?? "").toLowerCase();
    expect(vary.includes("accept"), vary).toBe(true);
    expect(vary.includes("authorization") && vary.includes("cookie"), vary).toBe(true);

    const listed = names(res.listing());
    expect(listed.includes("secret.json"), JSON.stringify(listed)).toBe(true);
  });

  test("operator browsing a suppressed site still gets the default doc", async () => {
    await putOk(anon, "/locked/index.html", "<html>home</html>", "text/html");

    // The bypass sits at the `listings` gate, below the default-resource
    // branch — an operator with a browser still sees the site, not a listing.
    const res = await op.get("/locked/", { headers: { accept: "text/html,application/xhtml+xml,*/*;q=0.8" } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.contentType()).toBe("text/html");
    expect(res.text()).toBe("<html>home</html>");
  });

  test("listing available at subdirs too", async () => {
    await putOk(anon, "/site/docs/a.md", "# a", "text/markdown");
    await putOk(anon, "/site/docs/b.md", "# b", "text/markdown");

    const res = await anon.get("/site/docs/", { headers: { accept: DIR_JSON } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("x-total-count")).toBe("2");
    expect(res.listing().total).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// dir_no_slash.rs — DirectorySlash on the `file` service: a GET/HEAD whose
// path names a directory *without* the trailing slash 301-redirects to the
// slash form (preserving the query string). What the slash form then yields
// — default doc, listing, or 404 — is decided by the directory arm. A
// directory beats a same-named friendly-URL (`.html`) probe.
// ---------------------------------------------------------------------------
describe("directory slash redirects (dir_no_slash.rs)", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    // `/site` — static-site mount (default doc).
    // `/locked` — no default doc, listings suppressed.
    // `/plain` — bare file mount (listings on, no default doc).
    // `/f` — friendly URLs enabled.
    await seed.applyMounts([
      { path: "/site", service: "file", config: { access: "open", defaultResource: "index.html" } },
      { path: "/locked", service: "file", config: { access: "open", listings: false } },
      { path: "/plain", service: "file", config: { access: "open" } },
      { path: "/f", service: "file", config: { access: "open", friendlyUrls: true, extensionPriority: ["html", "md"] } },
    ]);
    anon = seed.anon;
    // One tenant tree behind every mount: the directories below are named
    // per case so no case sees another's.
    for (const d of ["abc", "lsub", "psub", "fabc", "readme.html"]) seed.trackDir("/plain", d);
  });

  afterAll(async () => {
    await wipeFiles(seed!.admin, ["/plain/index.html", "/plain/fabc.html", "/plain/readme.md"]);
    await seed?.restore();
  });

  test("no-slash dir redirects, then serves the default doc", async () => {
    await putOk(anon, "/site/abc/index.html", "<html>abc</html>", "text/html");

    const res = await anon.get("/site/abc");
    expect(res.status, res.describe()).toBe(301);
    expect(res.header("location")).toBe("/site/abc/");

    // Following the redirect serves the default doc.
    const followed = await anon.get("/site/abc/");
    expect(followed.status, followed.describe()).toBe(200);
    expect(followed.text()).toBe("<html>abc</html>");
  });

  test("redirect preserves the query string", async () => {
    const res = await anon.get("/site/abc?x=1&y=2");
    expect(res.status, res.describe()).toBe(301);
    expect(res.header("location")).toBe("/site/abc/?x=1&y=2");
  });

  test("HEAD gets the same redirect", async () => {
    const res = await anon.head("/site/abc");
    expect(res.status, res.describe()).toBe(301);
    expect(res.header("location")).toBe("/site/abc/");
  });

  test("a genuine miss is still 404", async () => {
    await putOk(anon, "/site/index.html", "<html>home</html>", "text/html");

    const res = await anon.get("/site/missing");
    expect(res.status, res.describe()).toBe(404);
    expect(res.problem().code).toBe("not_found");
  });

  test("redirect fires even when the slash form 404s", async () => {
    // `/locked` has no default doc and listings off: the slash form is a 404,
    // but the no-slash form still redirects — the directory arm stays the
    // single authority on what a directory URL yields.
    await putOk(anon, "/locked/lsub/file.txt", "x", "text/plain");

    const res = await anon.get("/locked/lsub");
    expect(res.status, res.describe()).toBe(301);
    expect(res.header("location")).toBe("/locked/lsub/");

    const followed = await anon.get("/locked/lsub/");
    expect(followed.status, followed.describe()).toBe(404);
  });

  test("redirect then listing on a plain mount", async () => {
    await putOk(anon, "/plain/psub/file.txt", "x", "text/plain");

    const res = await anon.get("/plain/psub");
    expect(res.status, res.describe()).toBe(301);
    expect(res.header("location")).toBe("/plain/psub/");

    const followed = await anon.get("/plain/psub/");
    expect(followed.status, followed.describe()).toBe(200);
    expect(followed.listing().entries[0].name).toBe("file.txt");
  });

  test("directory beats a friendly-URL probe", async () => {
    // Both a directory `fabc/` and a file `fabc.html` exist. The directory
    // wins: friendly resolution must not shadow a real container.
    await putOk(anon, "/f/fabc/inner.txt", "inner", "text/plain");
    await putOk(anon, "/f/fabc.html", "<html>page</html>", "text/html");

    const res = await anon.get("/f/fabc");
    expect(res.status, res.describe()).toBe(301);
    expect(res.header("location")).toBe("/f/fabc/");
  });

  test("friendly resolution skips directory candidates", async () => {
    // A *directory* named `readme.html` must not satisfy the friendly probe;
    // resolution moves on to the next candidate extension.
    await putOk(anon, "/f/readme.html/inner.txt", "inner", "text/plain");
    await putOk(anon, "/f/readme.md", "# readme", "text/markdown");

    const res = await anon.get("/f/readme");
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("content-location")).toBe("/f/readme.md");
    expect(res.text()).toBe("# readme");
  });
});

// ---------------------------------------------------------------------------
// friendly_urls.rs — friendly (extension-less) URLs on the `file` service:
// `GET /docs/readme` resolves a stored `docs/readme.<ext>`, choosing among
// collisions by `Accept` negotiation, serving in place with a
// `Content-Location` to the real path. Opt-in via `friendlyUrls`; off by
// default. An extension-less PUT pins to the first `extensionPriority`
// entry so a no-`Accept` GET of the slug always returns it.
// ---------------------------------------------------------------------------
describe("friendly URLs (friendly_urls.rs)", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  /** Files created by the running case (real stored paths), wiped after it. */
  let created: string[] = [];

  async function put(path: string, content: string, contentType: string): Promise<Rs2Response> {
    const res = await anon.put(path, { body: content, contentType });
    if (res.status === 201 || res.status === 200) created.push(res.header("content-location") ?? path);
    return res;
  }
  async function putCreated(path: string, content: string, contentType: string): Promise<Rs2Response> {
    const res = await put(path, content, contentType);
    expect(res.status, `PUT ${path}: ${res.describe()}`).toBe(201);
    return res;
  }

  beforeAll(async () => {
    seed = await Seed.create();
    // `/friendly` — friendly URLs with an `html, md, json` priority.
    // `/nopri` — friendly URLs, no priority (extension-less PUT must 400).
    // `/plain` — defaults (friendly off). `/spa` — friendly over SPA fallback.
    await seed.applyMounts([
      {
        path: "/friendly",
        service: "file",
        config: { access: "open", friendlyUrls: true, extensionPriority: ["html", "md", "json"] },
      },
      { path: "/nopri", service: "file", config: { access: "open", friendlyUrls: true } },
      { path: "/plain", service: "file", config: { access: "open" } },
      {
        path: "/spa",
        service: "file",
        config: { access: "open", friendlyUrls: true, spaFallback: true, listings: false, extensionPriority: ["html", "md"] },
      },
    ]);
    anon = seed.anon;
    seed.trackDir("/plain", "docs");
  });

  afterEach(async () => {
    await wipeFiles(anon, created.reverse());
    created = [];
  });

  afterAll(async () => {
    await seed?.restore();
  });

  test("friendly URL resolves and advertises the real path", async () => {
    await putCreated("/friendly/docs/readme.md", "# hello", "text/markdown");

    const res = await anon.get("/friendly/docs/readme");
    expect(res.status, res.describe()).toBe(200);
    expect(res.contentType()).toBe("text/markdown");
    expect(res.header("content-location")).toBe("/friendly/docs/readme.md");
    expect(res.text()).toBe("# hello");
  });

  test("collision resolved by Accept, then priority", async () => {
    // Write the variants directly (an extension-less PUT would pin, not branch).
    await putCreated("/friendly/page.md", "# md", "text/markdown");
    await putCreated("/friendly/page.html", "<h1>html</h1>", "text/html");

    // Accept prefers HTML.
    const html = await anon.get("/friendly/page", { headers: { accept: "text/html" } });
    expect(html.header("content-location"), html.describe()).toBe("/friendly/page.html");
    expect(html.text()).toBe("<h1>html</h1>");

    // Accept prefers markdown.
    const md = await anon.get("/friendly/page", { headers: { accept: "text/markdown" } });
    expect(md.header("content-location"), md.describe()).toBe("/friendly/page.md");
    expect(md.text()).toBe("# md");

    // No Accept → priority order (html is E1).
    const plain = await anon.get("/friendly/page");
    expect(plain.header("content-location"), plain.describe()).toBe("/friendly/page.html");
  });

  test("extension-less PUT pins to E1, ignoring Content-Type", async () => {
    // PUT an extension-less slug with a markdown body — pins to E1 (html).
    const pinned = await put("/friendly/page", "# hi", "text/markdown");
    expect(pinned.status, pinned.describe()).toBe(201);
    expect(pinned.header("location")).toBe("/friendly/page");
    expect(pinned.header("content-location")).toBe("/friendly/page.html");

    // No-Accept GET of the slug returns it, served as E1's type (html).
    const got = await anon.get("/friendly/page");
    expect(got.status, got.describe()).toBe(200);
    expect(got.contentType()).toBe("text/html");
    expect(got.header("content-location")).toBe("/friendly/page.html");
    expect(got.text()).toBe("# hi");

    // The real path is reachable directly too.
    const exact = await anon.get("/friendly/page.html");
    expect(exact.status, exact.describe()).toBe(200);
  });

  test("pin not dislodged by a later sibling", async () => {
    await putCreated("/friendly/page", "# hi", "text/markdown"); // → page.html (E1)
    await putCreated("/friendly/page.md", "# md", "text/markdown"); // a lower-priority sibling

    // No-Accept GET still returns the pinned html — not dislodged.
    const plain = await anon.get("/friendly/page");
    expect(plain.header("content-location"), plain.describe()).toBe("/friendly/page.html");

    // An explicit Accept is the caller opting out — they may get the sibling.
    const md = await anon.get("/friendly/page", { headers: { accept: "text/markdown" } });
    expect(md.header("content-location"), md.describe()).toBe("/friendly/page.md");
  });

  test("extension-less PUT without a priority is 400", async () => {
    // `/nopri` has friendlyUrls on but no extensionPriority → nowhere to pin.
    const res = await put("/nopri/page", "# hi", "text/markdown");
    expect(res.status, res.describe()).toBe(400);
    expect(res.problem().code).toBe("bad_request");
    // An extensioned PUT is unaffected.
    await putCreated("/nopri/page.md", "# md", "text/markdown");
  });

  test("friendly off stores extension-less verbatim", async () => {
    // friendlyUrls off: the slug is taken literally, no pin, no 400.
    await putCreated("/plain/page", "raw", "application/octet-stream");
    const got = await anon.get("/plain/page");
    expect(got.status, got.describe()).toBe(200);
    expect(got.header("content-location")).toBeNull();
    expect(got.text()).toBe("raw");
  });

  test("exact match wins over friendly", async () => {
    // A literal extension-less file plus a same-stem variant. The Rust test
    // writes `readme` straight to disk; over the wire the friendly-off
    // `/plain` mount (same tenant tree) stores the slug verbatim.
    await putCreated("/plain/readme", "raw", "application/octet-stream");
    await putCreated("/friendly/readme.md", "# md", "text/markdown");

    const res = await anon.get("/friendly/readme");
    expect(res.status, res.describe()).toBe(200);
    // Served directly — no friendly probe, no Content-Location.
    expect(res.header("content-location")).toBeNull();
    expect(res.text()).toBe("raw");
  });

  test("friendly off by default is 404", async () => {
    await putCreated("/plain/docs/readme.md", "# hello", "text/markdown");

    const res = await anon.get("/plain/docs/readme");
    expect(res.status, res.describe()).toBe(404);
  });

  test("friendly miss falls through to SPA", async () => {
    await putCreated("/spa/index.html", "<html>app</html>", "text/html");
    await putCreated("/spa/about.md", "# about", "text/markdown");

    // A real friendly file beats the SPA shell.
    const about = await anon.get("/spa/about");
    expect(about.header("content-location"), about.describe()).toBe("/spa/about.md");
    expect(about.text()).toBe("# about");

    // No friendly match → SPA fallback serves the index with 200.
    const route = await anon.get("/spa/users/42/profile");
    expect(route.status, route.describe()).toBe(200);
    expect(route.text()).toBe("<html>app</html>");
  });

  test("DELETE removes the pinned file, then falls through", async () => {
    await putCreated("/friendly/page", "# hi", "text/markdown"); // → page.html (pinned)
    await putCreated("/friendly/page.md", "# md", "text/markdown"); // sibling

    // DELETE of the slug removes the pinned E1 (page.html).
    const del = await anon.delete("/friendly/page");
    expect(del.status, del.describe()).toBe(204);

    // GET now falls through to the surviving lower-priority sibling.
    const after = await anon.get("/friendly/page");
    expect(after.header("content-location"), after.describe()).toBe("/friendly/page.md");

    // DELETE again removes the sibling; the slug is now gone.
    const del2 = await anon.delete("/friendly/page");
    expect(del2.status, del2.describe()).toBe(204);
    const gone = await anon.get("/friendly/page");
    expect(gone.status, gone.describe()).toBe(404);
  });

  test("conditional DELETE checks the resolved file", async () => {
    await putCreated("/friendly/docs/readme.md", "# hello", "text/markdown");

    const res = await anon.get("/friendly/docs/readme");
    const etag = res.etag();
    expect(etag, res.describe()).not.toBeNull();

    // A stale If-Match on the slug is checked against the resolved file:
    // 412, and the file survives.
    const refused = await anon.delete("/friendly/docs/readme", { headers: { "if-match": '"not-the-current-etag"' } });
    expect(refused.status, refused.describe()).toBe(412);
    expect(refused.problem().code).toBe("precondition_failed");
    const still = await anon.get("/friendly/docs/readme");
    expect(still.status, still.describe()).toBe(200);

    // The matching ETag deletes through the same resolution.
    const deleted = await anon.delete("/friendly/docs/readme", { headers: { "if-match": etag! } });
    expect(deleted.status, deleted.describe()).toBe(204);
    const gone = await anon.get("/friendly/docs/readme");
    expect(gone.status, gone.describe()).toBe(404);
  });

  test("HEAD resolves friendly", async () => {
    await putCreated("/friendly/docs/readme.md", "# hello", "text/markdown");

    const res = await anon.head("/friendly/docs/readme");
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("content-type")).toBe("text/markdown");
    expect(res.header("content-location")).toBe("/friendly/docs/readme.md");
    expect(res.header("content-length")).toBe("7");
    expect(res.bytes.length, "HEAD carries no body").toBe(0);
  });
});
