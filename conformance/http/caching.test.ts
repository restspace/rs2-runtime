// Universal caching config — the HTTP conformance port of
// `rs2-core/tests/caching.rs`, step for step (spec §F.3).
//
// The Rust file builds a runtime with five mounts and drives them
// in-process; here the same five mounts are PUT through `/services/raw` and
// driven over the wire. Assertion messages keep the `[mount]` tag so a
// failure reads the same in either runner.
//
// Covered: the default `no-store` posture, every `Cache-Control` string the
// policy renders, the `public` clamp on non-open mounts (+ `Vary`), the
// `Set-Cookie` carve-out, and the store services' conditional-GET 304s
// (weak and list `If-None-Match` forms included).

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import type { Rs2Client, Rs2Response } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

const USER = { email: "cache-user@conf.test", password: "cache-pw" };

let seed: Seed;
let user: Rs2Client;

beforeAll(async () => {
  seed = await Seed.create();
  // The mount table of `caching.rs`'s `rt()`, plus two mounts that pin the
  // two policy renderings those five never produce (`public` + revalidate,
  // and `cache` without `public`).
  await seed.applyMounts([
    // No caching config: the default posture (this is the base `/data`).
    { path: "/data", service: "data", config: { access: "open" } },
    // Openly readable + public cache: the static-site/CDN case.
    {
      path: "/assets",
      service: "file",
      config: {
        access: "open",
        caching: { mode: "cache", maxAgeSeconds: 3600, public: true, immutable: true },
      },
    },
    // Revalidate mode: always fresh, 304s save bandwidth.
    { path: "/fresh", service: "data", config: { access: "open", caching: { mode: "revalidate" } } },
    // `public` requested on an authenticated mount: must clamp.
    {
      path: "/private",
      service: "file",
      config: {
        access: { read: "U", write: "A" },
        caching: { mode: "cache", maxAgeSeconds: 600, public: true },
      },
    },
    // Caching config on the auth mount must not leak onto cookies.
    {
      path: "/auth",
      service: "auth",
      config: { access: "open", caching: { mode: "cache", maxAgeSeconds: 600, public: true } },
    },
    // The other two branches of `CachePolicy::apply`.
    {
      path: "/pubfresh",
      service: "data",
      config: { access: "open", caching: { mode: "revalidate", public: true } },
    },
    {
      path: "/plain",
      service: "file",
      config: { access: "open", caching: { mode: "cache", maxAgeSeconds: 60 } },
    },
  ]);
  await seed.createPrincipals([{ ...USER, roles: "U A" }]);
  user = await seed.clientAs(USER);
  seed.trackDataset("/data", "things");
  seed.trackDataset("/fresh", "items");
  seed.trackDataset("/pubfresh", "pubitems");
}, 120_000);

afterAll(async () => {
  for (const path of ["/assets/app.js", "/private/doc.txt", "/plain/p.txt"]) {
    await seed?.admin.delete(path);
  }
  await seed?.admin.delete("/assets/site/?confirm=site");
  await seed?.restore();
});

/** `Cache-Control` of a response, for readable assertions. */
function cc(res: Rs2Response): string | null {
  return res.header("cache-control");
}

describe("default_posture_is_no_store_everywhere", () => {
  test("unconfigured mount, errors and the discovery surface are never stored", async () => {
    const put = await seed.admin.put("/data/things/x", { json: { n: 1 } });
    expect(put.status, `[/data] PUT: ${put.describe()}`).toBe(201);

    // Unconfigured mount.
    const get = await seed.admin.get("/data/things/x");
    expect(get.status, get.describe()).toBe(200);
    expect(cc(get), "[/data] unconfigured mount is no-store").toBe("no-store");

    // Errors.
    const missing = await seed.admin.get("/data/things/missing");
    expect(missing.status, missing.describe()).toBe(404);
    expect(cc(missing), "[/data] error is no-store").toBe("no-store");

    // The generated discovery surface (permission-filtered per caller).
    const services = await seed.admin.get("/.well-known/rs2/services");
    expect(services.status, services.describe()).toBe(200);
    expect(cc(services), "[discovery] is no-store").toBe("no-store");
  });
});

describe("opt_in_modes_render_and_clamp", () => {
  test("open mount + public cache renders the full header", async () => {
    const put = await seed.admin.put("/assets/site/app.css", {
      body: "body{}",
      contentType: "text/css",
    });
    expect(put.status, `[/assets] PUT: ${put.describe()}`).toBe(201);

    const res = await seed.anon.get("/assets/site/app.css");
    expect(res.status, res.describe()).toBe(200);
    expect(cc(res), "[/assets] public cache").toBe("public, max-age=3600, immutable");
    // `public` was honored, not clamped: no credential-keyed Vary.
    expect(
      res.header("vary") ?? "",
      "[/assets] a public response carries no credential Vary",
    ).not.toContain("authorization");
  });

  test("revalidate mode is private no-cache + Vary on credentials", async () => {
    const put = await seed.admin.put("/fresh/items/k", { json: { v: 1 } });
    expect(put.status, `[/fresh] PUT: ${put.describe()}`).toBe(201);

    const res = await seed.admin.get("/fresh/items/k");
    expect(res.status, res.describe()).toBe(200);
    expect(cc(res), "[/fresh] revalidate").toBe("private, no-cache");
    expect(res.header("vary"), "[/fresh] Vary on credentials").toContain("authorization");
    expect(res.header("vary"), "[/fresh] Vary on credentials").toContain("cookie");
  });

  test("public on an authenticated mount clamps to private + Vary", async () => {
    const put = await user.put("/private/doc.txt", { body: "secret", contentType: "text/plain" });
    expect(put.status, `[/private] PUT: ${put.describe()}`).toBe(201);

    const res = await user.get("/private/doc.txt");
    expect(res.status, res.describe()).toBe(200);
    expect(cc(res), "[/private] public clamped to private").toBe("private, max-age=600");
    expect(res.header("vary"), "[/private] Vary on credentials").toContain("authorization");
  });

  test("the remaining renderings: public revalidate, and a non-public max-age", async () => {
    const put = await seed.admin.put("/pubfresh/pubitems/k", { json: { v: 1 } });
    expect(put.status, `[/pubfresh] PUT: ${put.describe()}`).toBe(201);
    const fresh = await seed.anon.get("/pubfresh/pubitems/k");
    expect(fresh.status, fresh.describe()).toBe(200);
    expect(cc(fresh), "[/pubfresh] public revalidate").toBe("public, no-cache");
    expect(
      fresh.header("vary") ?? "",
      "[/pubfresh] a public response carries no credential Vary",
    ).not.toContain("authorization");

    const wrote = await seed.admin.put("/plain/p.txt", { body: "hi", contentType: "text/plain" });
    expect(wrote.status, `[/plain] PUT: ${wrote.describe()}`).toBe(201);
    const plain = await seed.anon.get("/plain/p.txt");
    expect(plain.status, plain.describe()).toBe(200);
    expect(cc(plain), "[/plain] non-public cache is private").toBe("private, max-age=60");
    expect(plain.header("vary"), "[/plain] Vary on credentials").toContain("authorization");
  });
});

describe("cookie_responses_are_never_cacheable", () => {
  test("the Set-Cookie carve-out beats the auth mount's public caching config", async () => {
    const res = await seed.anon.post("/auth/login", {
      json: { email: USER.email, password: USER.password },
      token: null,
    });
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("set-cookie"), "[/auth] login sets the session cookie").toBeTruthy();
    expect(cc(res), "[/auth] a Set-Cookie response is no-store").toBe("no-store");
  });
});

describe("conditional_gets_return_304", () => {
  test("file: ETag revalidation, and a stale validator gets the body again", async () => {
    const put = await seed.admin.put("/assets/app.js", {
      body: "x()",
      contentType: "application/javascript",
    });
    expect(put.status, `[/assets] PUT: ${put.describe()}`).toBe(201);

    const first = await seed.anon.get("/assets/app.js");
    expect(first.status, first.describe()).toBe(200);
    const etag = first.etag();
    expect(etag, "[/assets] GET carries an ETag").toBeTruthy();

    const revalidate = await seed.anon.get("/assets/app.js", {
      headers: { "if-none-match": etag as string },
    });
    expect(revalidate.status, revalidate.describe()).toBe(304);
    expect(revalidate.bytes.length, "[/assets] 304 carries no body").toBe(0);
    expect(revalidate.etag(), "[/assets] 304 repeats the ETag").toBe(etag);
    // 304s are exempt from both the mount policy and the catch-all: the
    // cached entry's own policy governs them.
    expect(cc(revalidate), "[/assets] 304 carries no Cache-Control").toBeNull();

    // A stale validator gets the full response again.
    const stale = await seed.anon.get("/assets/app.js", {
      headers: { "if-none-match": '"deadbeef"' },
    });
    expect(stale.status, stale.describe()).toBe(200);
    expect(stale.text(), "[/assets] a stale validator resends the body").toBe("x()");
  });

  test("data: content-hash ETag revalidation, weak and list forms accepted", async () => {
    const put = await seed.admin.put("/fresh/items/r", { json: { v: 1 } });
    expect(put.status, `[/fresh] PUT: ${put.describe()}`).toBe(201);

    const first = await seed.admin.get("/fresh/items/r");
    expect(first.status, first.describe()).toBe(200);
    const etag = first.etag();
    expect(etag, "[/fresh] GET carries an ETag").toBeTruthy();

    const revalidate = await seed.admin.get("/fresh/items/r", {
      headers: { "if-none-match": `"other", W/${etag}` },
    });
    expect(revalidate.status, revalidate.describe()).toBe(304);
    expect(revalidate.bytes.length, "[/fresh] 304 carries no body").toBe(0);
    expect(cc(revalidate), "[/fresh] 304 carries no Cache-Control").toBeNull();

    // The record changing invalidates the validator.
    const update = await seed.admin.put("/fresh/items/r", { json: { v: 2 } });
    expect(update.status, `[/fresh] update: ${update.describe()}`).toBe(200);
    const again = await seed.admin.get("/fresh/items/r", {
      headers: { "if-none-match": etag as string },
    });
    expect(again.status, "[/fresh] a changed record invalidates the validator").toBe(200);
  });

  test("If-None-Match: * revalidates any existing representation", async () => {
    const res = await seed.anon.get("/assets/app.js", { headers: { "if-none-match": "*" } });
    expect(res.status, res.describe()).toBe(304);
  });
});
