// CORS + trusted-origin cookies — the over-the-wire successor to
// `rs2-core/tests/cors.rs` (spec F.3). Same order, same statuses, same
// header names: preflights answered before routing, credentialed CORS for
// trusted origins, the cookie-CSRF guard, and the auth service's
// cookie-attribute matrix (`allowedLoginOrigins`, bearer-only login).

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, type Rs2Response } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

/** Trusted (credentialed CORS + cross-site cookie) and login-allowed. */
const TRUSTED = "https://app.acme.test";
/** Matches the `*.acme.dev` trusted pattern. */
const TRUSTED_WILDCARD = "https://x.acme.dev";
/** Allowed (plain CORS) but not trusted, and not a permitted login origin. */
const ALLOWED = "https://reader.example";
/** Neither trusted nor allowed. */
const UNLISTED = "https://evil.example";

/** The `Origin` a browser on the host under test sends (CORS not involved). */
const SAME_ORIGIN = `https://${env().host}`;

const USER = { email: "cors@conf.test", password: "cors-pw", roles: "A" };

/** The tenant shape of `cors.rs`'s `rt()`, on top of `conf.base.json`. */
const CORS_AUTH = {
  jwtSecret: "conf-secret",
  userDataset: "users",
  allowedLoginOrigins: [TRUSTED, "*.acme.dev"],
};
const CORS_POLICY = {
  trustedOrigins: [TRUSTED, "*.acme.dev"],
  allowedOrigins: [ALLOWED],
};

let seed: Seed | undefined;

/** `Set-Cookie` values (Node splits repeated headers via `getSetCookie`). */
function setCookies(res: Rs2Response): string[] {
  const headers = res.headers as Headers & { getSetCookie?: () => string[] };
  if (typeof headers.getSetCookie === "function") return headers.getSetCookie();
  const single = res.header("set-cookie");
  return single === null ? [] : [single];
}

function authCookie(res: Rs2Response): string | undefined {
  return setCookies(res).find((c) => c.startsWith("rs-auth="));
}

beforeAll(async () => {
  seed = await Seed.create();
  await seed.applyMounts([], { auth: CORS_AUTH, cors: CORS_POLICY });
  await seed.createPrincipals([USER]);
  seed.trackDataset("/data", "things");
});

afterAll(async () => {
  await seed?.restore();
});

describe("preflights answer before routing", () => {
  test("[trusted] OPTIONS + Access-Control-Request-Method → 204 with credentialed CORS", async () => {
    const res = await seed!.anon.options("/data/things", {
      headers: {
        origin: TRUSTED,
        "access-control-request-method": "PUT",
        "access-control-request-headers": "content-type, if-match",
      },
    });
    expect(res.status, res.describe()).toBe(204);
    expect(res.header("access-control-allow-origin")).toBe(TRUSTED);
    expect(res.header("access-control-allow-credentials")).toBe("true");
    expect(res.header("access-control-allow-methods")).toBe("PUT");
    expect(res.header("access-control-allow-headers")).toBe("content-type, if-match");
    // Answered before routing, so the preflight is cacheable and varies on Origin.
    expect(res.header("access-control-max-age")).toBe("86400");
    expect((res.header("vary") || "").toLowerCase()).toContain("origin");
  });

  test("[allowed] preflight gets CORS but no credentials", async () => {
    const res = await seed!.anon.options("/data/things", {
      headers: { origin: ALLOWED, "access-control-request-method": "GET" },
    });
    expect(res.status, res.describe()).toBe(204);
    expect(res.header("access-control-allow-origin")).toBe(ALLOWED);
    expect(res.header("access-control-allow-credentials")).toBeNull();
    // No Access-Control-Request-Headers: the default allow-list is sent.
    expect(res.header("access-control-allow-headers")).toBe(
      "authorization, content-type, idempotency-key, if-match",
    );
  });

  test("[unlisted] preflight is not answered — the request routes, with no CORS headers", async () => {
    const res = await seed!.anon.options("/data/things", {
      headers: { origin: UNLISTED, "access-control-request-method": "GET" },
    });
    expect(res.header("access-control-allow-origin")).toBeNull();
    expect(res.status, res.describe()).not.toBe(204);
  });
});

describe("responses and errors carry CORS for permitted origins", () => {
  beforeAll(async () => {
    const created = await seed!.anon.put("/data/things/x", { json: { n: 1 } });
    expect(created.status, created.describe()).toBe(201);
  });

  test("[allowed] success carries the origin echo and the exposed headers, no credentials", async () => {
    const res = await seed!.anon.get("/data/things/x", { headers: { origin: ALLOWED } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("access-control-allow-origin")).toBe(ALLOWED);
    expect(res.header("access-control-expose-headers")).toContain("x-total-count");
    expect(res.header("access-control-allow-credentials")).toBeNull();
  });

  test("[trusted] error responses are decorated too", async () => {
    const res = await seed!.anon.get("/data/things/nope", {
      headers: { origin: TRUSTED_WILDCARD },
    });
    expect(res.status, res.describe()).toBe(404);
    expect(res.header("access-control-allow-origin")).toBe(TRUSTED_WILDCARD);
    expect(res.header("access-control-allow-credentials")).toBe("true");
  });

  test("[unlisted] no CORS headers at all", async () => {
    const res = await seed!.anon.get("/data/things/x", { headers: { origin: UNLISTED } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("access-control-allow-origin")).toBeNull();
  });

  test("[same-origin] CORS is not involved", async () => {
    const res = await seed!.anon.get("/data/things/x", { headers: { origin: SAME_ORIGIN } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.header("access-control-allow-origin")).toBeNull();
  });
});

describe("the CSRF guard blocks untrusted cookie writes", () => {
  test("[unlisted] a cookie-authenticated unsafe request is 403 before routing", async () => {
    const res = await seed!.anon.put("/data/things/x", {
      json: { n: 1 },
      headers: { origin: UNLISTED, cookie: "rs-auth=whatever" },
    });
    expect(res.status, res.describe()).toBe(403);
  });

  test("[unlisted] the same write with a bearer fails on the bad token, not CSRF", async () => {
    const res = await seed!.anon.put("/data/things/x", {
      json: { n: 1 },
      headers: { origin: UNLISTED, authorization: "Bearer junk" },
    });
    expect(res.status, res.describe()).toBe(401);
  });

  test("[trusted] a cookie-bearing write from a trusted origin is routed", async () => {
    const res = await seed!.anon.put("/data/things/csrf-ok", {
      json: { n: 1 },
      headers: { origin: TRUSTED, cookie: "other=1" },
    });
    expect(res.status, res.describe()).toBe(201);
  });
});

describe("auth cookie attributes follow origin trust", () => {
  const login = { email: USER.email, password: USER.password };

  test("[no origin] a non-browser login gets a SameSite=Strict cookie", async () => {
    const res = await seed!.anon.post("/auth/login", { json: login, token: null });
    expect(res.status, res.describe()).toBe(200);
    const cookie = authCookie(res);
    expect(cookie, `no rs-auth cookie in ${JSON.stringify(setCookies(res))}`).toBeDefined();
    expect(cookie).toContain("SameSite=Strict");
    expect(cookie).toContain("HttpOnly");
  });

  test("[same-origin] SameSite=Strict", async () => {
    const res = await seed!.anon.post("/auth/login", {
      json: login,
      token: null,
      headers: { origin: SAME_ORIGIN },
    });
    expect(res.status, res.describe()).toBe(200);
    expect(authCookie(res)).toContain("SameSite=Strict");
  });

  test("[trusted] the cross-origin cookie is SameSite=None; Secure", async () => {
    const res = await seed!.anon.post("/auth/login", {
      json: login,
      token: null,
      headers: { origin: TRUSTED },
    });
    expect(res.status, res.describe()).toBe(200);
    const cookie = authCookie(res);
    expect(cookie).toContain("SameSite=None");
    expect(cookie).toContain("Secure");
  });

  test("[allowed] an origin outside allowedLoginOrigins may not log in at all", async () => {
    const res = await seed!.anon.post("/auth/login", {
      json: login,
      token: null,
      headers: { origin: ALLOWED },
    });
    expect(res.status, res.describe()).toBe(403);
    expect(res.problem().code).toBe("forbidden");
  });
});

describe("untrusted origin login is bearer-only", () => {
  // `cors.rs`'s second runtime: no `allowedLoginOrigins`, CORS-readable
  // from anywhere, nothing trusted.
  beforeAll(async () => {
    await seed!.applyMounts([], {
      auth: { jwtSecret: "conf-secret", userDataset: "users" },
      cors: { allowedOrigins: ["*"] },
    });
  });

  test("login succeeds with no cookie, a body token, and a CORS-readable response", async () => {
    const res = await seed!.anon.post("/auth/login", {
      json: { email: USER.email, password: USER.password },
      token: null,
      headers: { origin: "https://anywhere.example" },
    });
    expect(res.status, res.describe()).toBe(200);
    expect(setCookies(res), "no cookie for untrusted origins").toEqual([]);
    expect(typeof res.json<{ token?: unknown }>().token, "body token is the credential").toBe(
      "string",
    );
    expect(res.header("access-control-allow-origin"), "response is CORS-readable").toBe(
      "https://anywhere.example",
    );
  });
});
