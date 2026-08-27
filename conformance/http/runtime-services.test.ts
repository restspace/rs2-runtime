// Runtime services over HTTP — replaces `rs2-core/tests/runtime_services.rs`
// for the cross-host contract (spec F.3, second row): the full dispatch path
// (router → wrapper → service) against the `file` and `data` services. The
// tests mirror that file test by test, same order, same status codes.
//
// Skipped from the Rust file because they are only observable in-process:
//   - `tenant_storage_is_host_scoped`'s on-disk check
//     (`<root>/alpha/x.txt` exists) — the HTTP half (the same path under
//     another tenant is 404) runs only when `RS2_SECOND_HOST` names a
//     second tenant, i.e. on the multi-tenant Worker; the Rust fixture is
//     single-tenant.

import { createConnection } from "node:net";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, type Rs2Client, type Rs2Response } from "./src/client.ts";
import { expectDivergent } from "./src/divergences.ts";
import { Seed } from "./src/seed.ts";

const U_USER = { email: "runtime-u@conf.test", password: "runtime-u-pw" };

function status(res: Rs2Response, want: number, msg: string): void {
  expect(res.status, `${msg}: ${res.describe()}`).toBe(want);
}

const text = (body: string, contentType = "text/plain") => ({ body, contentType });

/**
 * Send a request line verbatim over a raw socket. WHATWG `fetch` normalizes
 * `..` and `%2e%2e` segments away before the request leaves the client, so
 * the router's traversal guard can only be exercised from the wire with a
 * hand-written HTTP/1.1 request. Returns the status and the body.
 */
function rawRequest(client: Rs2Client, method: string, target: string): Promise<{ status: number; body: string }> {
  const url = new URL(client.baseUrl);
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    const sock = createConnection({ host: url.hostname, port: Number(url.port || 80) }, () => {
      sock.write(`${method} ${target} HTTP/1.1\r\nHost: ${client.host}\r\nConnection: close\r\n\r\n`);
    });
    sock.on("data", (c) => chunks.push(c));
    sock.on("error", reject);
    sock.on("close", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      const m = /^HTTP\/1\.[01] (\d{3})/.exec(raw);
      if (!m) return reject(new Error(`no status line in response to ${method} ${target}: ${raw.slice(0, 200)}`));
      const sep = raw.indexOf("\r\n\r\n");
      resolve({ status: Number(m[1]), body: sep >= 0 ? raw.slice(sep + 4) : "" });
    });
  });
}

describe("runtime services", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    // The Rust file's tenant (`test_runtime`) plus the two static-site
    // mounts its last tests build separately: everything on one tenant.
    // Each Rust test gets its own temp dir; here the two sites root
    // themselves (`store.root`) so `/site/index.html` and `/spa/index.html`
    // are distinct files rather than one shared node-store path.
    await seed.applyMounts([
      { path: "/files", service: "file", config: { access: "open" } },
      { path: "/data", service: "data", config: { enforceSchema: true, access: "open" } },
      { path: "/private", service: "file", config: { access: "authenticated" } },
      {
        path: "/site",
        service: "file",
        config: {
          access: "open",
          spaFallback: true,
          listings: false,
          caching: { mode: "cache", maxAgeSeconds: 300, public: true },
          store: { adapter: "builtin:local", root: "runtime-site" },
        },
      },
      {
        path: "/spa",
        service: "file",
        config: {
          access: "open",
          spaFallbackAll: true,
          listings: false,
          store: { adapter: "builtin:local", root: "runtime-spa" },
        },
      },
    ]);
    // The auth gate test needs any authenticated principal (the Rust test
    // stamps a `U` principal onto the message).
    await seed.createPrincipals([{ ...U_USER, roles: "U" }]);
    anon = seed.anon;
    seed.trackDir("/files", "docs");
    seed.trackDir("/site", "docs");
    seed.trackDir("/site", "assets");
    seed.trackDir("/spa", "assets");
    seed.trackDataset("/data", "widgets");
    seed.trackDataset("/data", "people");
  });

  afterAll(async () => {
    if (!seed) return;
    // Root-level files the tests author (no directory to confirm-delete).
    for (const p of ["/files/a.txt", "/files/b.txt", "/files/c.txt", "/site/index.html", "/site/app.js", "/spa/index.html"]) {
      await seed.admin.delete(p);
    }
    await seed.restore();
  });

  test("file: write, read, range, paginated listing, delete", async () => {
    // PUT creates → 201
    let res = await anon.put("/files/docs/a.txt", text("alpha"));
    status(res, 201, "[file] PUT create");
    // PUT overwrites → 200
    res = await anon.put("/files/docs/a.txt", text("alpha2"));
    status(res, 200, "[file] PUT overwrite");

    // GET streams back with ETag
    res = await anon.get("/files/docs/a.txt");
    status(res, 200, "[file] GET");
    expect(res.etag(), "[file] GET carries ETag").not.toBeNull();
    expect(res.text()).toBe("alpha2");

    // Range read → 206 partial
    res = await anon.get("/files/docs/a.txt", { headers: { range: "bytes=0-2" } });
    status(res, 206, "[file] Range GET");
    expect(res.text(), "[file] Range body").toBe("alp");

    // Directory listing is paginated dir+json
    res = await anon.put("/files/docs/b.txt", text("beta"));
    status(res, 201, "[file] PUT b.txt");
    res = await anon.get("/files/docs/?$take=1&$skip=1");
    status(res, 200, "[file] paged listing");
    const listing = res.listing();
    expect(listing.total, "[file] listing total").toBe(2);
    expect(listing.entries.length, "[file] one entry per page").toBe(1);
    expect(listing.entries[0].name).toBe("b.txt");

    // DELETE file then empty dir
    status(await anon.delete("/files/docs/a.txt"), 204, "[file] DELETE a.txt");
    status(await anon.delete("/files/docs/b.txt"), 204, "[file] DELETE b.txt");
    status(await anon.delete("/files/docs/"), 204, "[file] DELETE empty dir");
    status(await anon.get("/files/docs/a.txt"), 404, "[file] deleted file is gone");
  });

  test("file: MOVE renames, listing carries contentType", async () => {
    status(await anon.put("/files/a.txt", text("x")), 201, "[file] PUT a.txt");

    // MOVE to a fresh name: created → 201, Location points at the new path.
    let res = await anon.move("/files/a.txt", "/files/b.txt");
    status(res, 201, "[file] MOVE to a fresh name");
    expect(res.header("location"), "[file] MOVE Location").toBe("/files/b.txt");

    // Source gone, destination readable.
    status(await anon.get("/files/a.txt"), 404, "[file] MOVE source gone");
    res = await anon.get("/files/b.txt");
    status(res, 200, "[file] MOVE destination readable");
    expect(res.text()).toBe("x");

    // Listing carries per-entry contentType (G5).
    const { listing } = await anon.listDir("/files/");
    const entry = listing.entries.find((e) => e.name === "b.txt");
    expect(entry, `[file] b.txt listed: ${JSON.stringify(listing)}`).toBeDefined();
    expect(entry!.contentType, `[file] entry contentType: ${JSON.stringify(entry)}`).toMatch(/^text\/plain/);

    // MOVE over an existing file overwrites → 200.
    status(await anon.put("/files/c.txt", text("y")), 201, "[file] PUT c.txt");
    status(await anon.move("/files/c.txt", "/files/b.txt"), 200, "[file] MOVE over an existing file");

    // Missing source → 404.
    status(await anon.move("/files/nope.txt", "/files/x.txt"), 404, "[file] MOVE missing source");
  });

  test("data: .schemas index and record listing contentType", async () => {
    const schema = { type: "object", properties: { n: { type: "integer" } } };
    status(await anon.put("/data/widgets/.schema.json", { json: schema }), 200, "[data] install schema");
    status(await anon.put("/data/widgets/w1", { json: { n: 1 } }), 201, "[data] PUT record");

    // .schemas indexes every dataset's installed schema in one call (G6).
    const res = await anon.get("/data/.schemas");
    status(res, 200, "[data] GET .schemas");
    const doc = res.json();
    expect(doc.schemas?.widgets?.schemaUrl, `[data] .schemas entry: ${res.text()}`).toBe("/data/widgets/.schema.json");
    expect(doc.schemas.widgets.schema.properties.n.type).toBe("integer");

    // Record listings carry contentType (G5).
    const { listing } = await anon.listDir("/data/widgets/");
    const entry = listing.entries.find((e) => e.name === "w1");
    expect(entry, `[data] w1 listed: ${JSON.stringify(listing)}`).toBeDefined();
    expect(entry!.contentType).toBe("application/json");
  });

  test("path traversal is rejected at the router", async () => {
    // `..` / `%2e%2e` must hit the wire un-normalized (see rawRequest).
    // The Workers platform canonicalizes dot segments before the Worker
    // runs, so the guard is unreachable there (`dotSegmentTraversal`).
    for (const target of ["/files/../secret", "/files/%2e%2e/secret"]) {
      const { status: code, body } = await rawRequest(anon, "GET", target);
      expectDivergent("dotSegmentTraversal", code, `path ${target} must be rejected: ${body.slice(0, 200)}`);
      // Behind an edge the rejection can come from the intermediary rather
      // than the host (a plaintext request to an HTTPS listener is refused
      // at the port); only a body that is actually RS2's carries the code.
      if (code === 400 && body.trimStart().startsWith("{")) {
        expect(JSON.parse(body).code, `path ${target} problem code`).toBe("path_unsafe");
      }
    }
    const res = await anon.get("/files/a%00.txt");
    status(res, 400, "path /files/a%00.txt must be rejected");
    // Same again: Cloudflare rejects a NUL byte in the path at the edge with
    // its own 400 page, so the request never reaches the host. Rejected is
    // rejected; the wording is only assertable when RS2 answered.
    if (res.contentType() === "application/problem+json") {
      expect(res.problem().code).toBe("path_unsafe");
    }
  });

  test("data: CRUD, schema validation, PATCH, keyless POST, confirm delete", async () => {
    // Install a dataset schema.
    const schema = { type: "object", required: ["name"], properties: { name: { type: "string" } } };
    status(await anon.put("/data/people/.schema.json", { json: schema }), 200, "[data] install schema");

    // Valid write → 201; invalid write → 422 with structured errors.
    status(await anon.put("/data/people/ada", { json: { name: "Ada" } }), 201, "[data] valid PUT");
    let res = await anon.put("/data/people/bad", { json: { age: 7 } });
    status(res, 422, "[data] invalid PUT");
    let problem = res.problem();
    expect(problem.code).toBe("validation_failed");
    expect(Array.isArray(problem.errors), `[data] 422 carries errors[]: ${res.text()}`).toBe(true);

    // GET carries the schema reference in the media type (PRD 6.2).
    res = await anon.get("/data/people/ada");
    status(res, 200, "[data] GET record");
    expect(res.contentType()).toBe("application/json");
    expect(res.header("content-type"), "[data] schema parameter").toContain('schema="/data/people/.schema.json"');
    expect(res.json().name).toBe("Ada");

    // PATCH = JSON merge, result re-validated.
    res = await anon.patch("/data/people/ada", { json: { nick: "A" } });
    status(res, 200, "[data] PATCH merge");
    expect(res.json().nick).toBe("A");
    res = await anon.patch("/data/people/ada", { json: { name: null } });
    status(res, 422, "[data] PATCH removing a required field is re-validated");

    // Keyless POST generates a key and Location.
    res = await anon.post("/data/people", { json: { name: "Grace" } });
    status(res, 201, "[data] keyless POST");
    const location = res.header("location");
    expect(location, "[data] POST Location").not.toBeNull();
    expect(location!.startsWith("/data/people/"), `[data] Location under the dataset: ${location}`).toBe(true);

    // Listing paginates.
    res = await anon.get("/data/people/");
    status(res, 200, "[data] listing");
    expect(res.listing().total).toBe(2);

    // Dataset delete needs the explicit confirm token (409 per the store
    // contract's non-empty-container guard).
    status(await anon.delete("/data/people"), 409, "[data] unconfirmed dataset delete");
    status(await anon.delete("/data/people?confirm=people"), 204, "[data] confirmed dataset delete");
    status(await anon.get("/data/people/ada"), 404, "[data] record gone with the dataset");
  });

  test("errors are problem+json with the tenant and a trace id", async () => {
    const res = await anon.get("/nowhere/x");
    status(res, 404, "unmounted path");
    const problem = res.problem();
    expect(problem.code).toBe("not_found");
    expect(problem.tenant).toBe(env().tenant);
    expect(typeof problem.traceId, `traceId in ${res.text()}`).toBe("string");
    expect(res.header("x-trace-id"), "x-trace-id header matches the body's traceId").toBe(problem.traceId);
  });

  test("auth gates protected mounts", async () => {
    status(await anon.get("/private/x.txt"), 401, "[private] anonymous read");
    // Passes auth, then 404s on the missing file — proving the gate opened.
    const user = await seed!.clientAs(U_USER);
    status(await user.get("/private/missing.txt"), 404, "[private] authenticated read of a missing file");
  });

  test("file: static-site mode (default resource, SPA fallback, listings off)", async () => {
    // Author the site (the mount is still a store for writes).
    for (const [path, content, mt] of [
      ["/site/index.html", "<html>app</html>", "text/html"],
      ["/site/app.js", "boot()", "application/javascript"],
      ["/site/docs/index.html", "<html>docs</html>", "text/html"],
    ]) {
      status(await anon.put(path, text(content, mt)), 201, `[site] PUT ${path}`);
    }

    // Directory GETs serve the default resource, not a listing.
    let res = await anon.get("/site/");
    status(res, 200, "[site] root");
    expect(res.contentType()).toBe("text/html");
    expect(res.text()).toBe("<html>app</html>");
    expect(res.header("cache-control"), "[site] mount caching policy").toBe("public, max-age=300");
    res = await anon.get("/site/docs/");
    status(res, 200, "[site] docs/");
    expect(res.text(), "nearest default doc wins").toBe("<html>docs</html>");

    // SPA fallback: extension-less routes serve the root index with 200…
    res = await anon.get("/site/users/42/profile");
    status(res, 200, "[site] SPA route");
    expect(res.text()).toBe("<html>app</html>");

    // …while missing assets stay honest 404s.
    status(await anon.get("/site/missing.js"), 404, "[site] missing asset");
    // And real assets serve normally.
    status(await anon.get("/site/app.js"), 200, "[site] real asset");

    // Listings are suppressed on the site mount, intact on the plain one.
    // (A directory with no default doc 404s rather than listing.)
    status(await anon.put("/site/assets/x.png", text("png", "image/png")), 201, "[site] PUT asset");
    // /site/assets/ has no index.html → SPA fallback serves the app shell
    // (it is an extension-less navigation), proving routes under asset
    // dirs still work; the listing never leaks.
    res = await anon.get("/site/assets/");
    status(res, 200, "[site] assets/ falls back to the shell");
    expect(res.text()).toBe("<html>app</html>");
    res = await anon.get("/files/");
    status(res, 200, "[files] plain mount still lists");
    expect(res.contentType()).toBe("application/vnd.rs2.dir+json");

    // The discovery surface declares the facet.
    const doc = await anon.getJson("/.well-known/rs2/services");
    const site = doc.services.find((s: any) => s.path === "/site");
    expect(site, `[site] in discovery: ${JSON.stringify(doc)}`).toBeDefined();
    expect(site.facets, `[site] facets: ${JSON.stringify(site)}`).toContain("static-site");
  });

  test("file: spaFallbackAll rescues extensioned misses too", async () => {
    status(await anon.put("/spa/index.html", text("<html>app</html>", "text/html")), 201, "[spa] PUT index");

    // Extension-less miss: same behavior as plain spaFallback.
    status(await anon.get("/spa/users/42"), 200, "[spa] extension-less route");

    // A miss *with* an extension — e.g. rs2-ui's URL-as-path deep link
    // `/admin/files/docs/readme.md` — also falls back, unlike plain
    // spaFallback (which would 404 this).
    const res = await anon.get("/spa/files/docs/readme.md");
    status(res, 200, "[spa] extensioned deep link");
    expect(res.text()).toBe("<html>app</html>");

    // A real, existing asset still serves normally rather than being masked
    // by the catch-all.
    status(await anon.put("/spa/assets/app.js", text("boot()", "application/javascript")), 201, "[spa] PUT asset");
    status(await anon.get("/spa/assets/app.js"), 200, "[spa] real asset");
  });

  // Multi-tenant hosts only: the same path under another tenant does not
  // exist. The Rust fixture is single-tenant, so this needs
  // `RS2_SECOND_HOST` (spec F.3).
  test.skipIf(!env().secondHost)("tenant storage is host scoped", async () => {
    status(await anon.put("/files/x.txt", text("alpha-data")), 201, "[isolation] write on the first tenant");
    try {
      status(await anon.withHost(env().secondHost!).get("/files/x.txt"), 404, "[isolation] read under the second tenant");
    } finally {
      await anon.delete("/files/x.txt");
    }
  });
});
