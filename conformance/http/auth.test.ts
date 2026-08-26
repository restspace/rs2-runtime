// The `auth` service over the wire — the HTTP successor to
// `services/auth.rs`'s semantics and the auth half of
// `tests/m2_composition.rs::auth_login_rbac_and_lockout` (spec F.3, the
// `auth.test.ts` row): login/refresh (halfway rule)/logout/user, lockout
// after `maxAttempts`, a presented-but-bad token is 401 on every path (never
// anonymous), `roles` array vs string, `jwtUserProps` extra claims and the
// `{claim}` path grants they unlock, and the cross-host hash fixture.
//
// The fixture tenant's `jwtSecret` is public (`conf.base.json`), so the
// suite mints its own HS512 tokens where the Rust behaviour depends on
// claim timing (the halfway refresh rule, expiry) or on malformed input —
// the same secret the host verifies with, and nothing else about the host.

import { createHmac, timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, PACKAGE_ROOT, Rs2Client, seg, type Rs2Response } from "./src/client.ts";
import { baseConfig, Seed } from "./src/seed.ts";

// ---- fixture ---------------------------------------------------------------

/** `conf.base.json`'s signing key; every token in this file is verified against it. */
const SECRET = String((baseConfig().auth as { jwtSecret: string }).jwtSecret);
const SESSION_SECS = 60 * 60; // `sessionMinutes` default 60
const MAX_ATTEMPTS = 5; // `maxAttempts` default
const LOCK_SECS = 10 * 60; // `lockMinutes` default

interface HashEntry {
  mintedBy: string;
  algorithm: string;
  password: string;
  passwordHash: string | null;
}
const HASHES: HashEntry[] = JSON.parse(readFileSync(resolve(PACKAGE_ROOT, "fixtures", "auth-hashes.json"), "utf8")).hashes;
/** A host-minted argon2id hash for `cross-host-pw`, used wherever a record needs a real hash without the facade. */
const RUST_HASH = HASHES.find((h) => h.mintedBy === "rust")!;

const DEV = { email: "dev@conf.test", password: "dev-pw" };
const MULTI = { email: "multi@conf.test", password: "multi-pw" };
const NOTEAM = { email: "noteam@conf.test", password: "noteam-pw" };
const LOCK = { email: "lock@conf.test", password: "lock-pw" };
/** Lives in `/data/agents`, the `userDataset` of the overriding `/auth2` mount. */
const BOT = { email: "bot@conf.test", password: RUST_HASH.password };
const LOCKBOT = { email: "lockbot@conf.test", password: RUST_HASH.password };

let seed: Seed;
let anon: Rs2Client;
let admin: Rs2Client;
/** User records this suite PUT directly (no facade) — deleted in `afterAll`. */
const directRecords: string[] = [];

async function putUser(dataset: string, email: string, record: Record<string, unknown>): Promise<void> {
  const res = await admin.put(`/data/${dataset}/${seg(email)}`, { json: record });
  expect(res.status, `seed /data/${dataset}/${email}: ${res.describe()}`).toBe(201);
  directRecords.push(`/data/${dataset}/${seg(email)}`);
}

beforeAll(async () => {
  seed = await Seed.create();
  anon = seed.anon;
  admin = seed.admin;
  await seed.applyMounts(
    [
      // The m2 shape: open reads, operator writes.
      { path: "/admin", service: "file", config: { access: { read: "all", write: "A" } } },
      // `{email}` path grant (principal id).
      { path: "/boxes", service: "file", config: { access: { read: "all", write: "U /{email}" } } },
      // `{team}` path grant (a `jwtUserProps` claim).
      { path: "/teams", service: "file", config: { access: { read: "authenticated /{team}", write: "A" } } },
      // Mount config overrides the tenant `auth` settings (everything but `access`).
      {
        path: "/auth2",
        service: "auth",
        config: { access: "open", sessionMinutes: 2, maxAttempts: 3, userDataset: "agents" },
      },
    ],
    { auth: { jwtSecret: SECRET, userDataset: "users", jwtUserProps: ["team", "id"] } },
  );
  await seed.createPrincipals([
    { ...DEV, roles: "U", extra: { team: "blue", id: "spoof" } },
    { ...MULTI, roles: ["U", "E"] },
    { ...NOTEAM, roles: "U" },
    { ...LOCK, roles: "U" },
  ]);
  seed.trackDataset("/data", "agents");
  await putUser("agents", BOT.email, { passwordHash: RUST_HASH.passwordHash, roles: ["U", "E"], kind: "agent" });
  await putUser("agents", LOCKBOT.email, { passwordHash: RUST_HASH.passwordHash, roles: "U" });
  seed.trackDir("/admin", "notes");
  seed.trackDir("/teams", "blue");
  seed.trackDir("/teams", "red");
});

afterAll(async () => {
  // `/boxes` grants delete only to `U /{email}` — the admin (`A`) cannot
  // clear the dev box, so its owner does.
  const dev = await seed.clientAs(DEV);
  await dev.delete(`/boxes/${DEV.email}/?confirm=${DEV.email}`);
  for (const path of directRecords) await admin.delete(path);
  await seed?.restore();
});

// ---- helpers ---------------------------------------------------------------

const b64url = (bytes: Uint8Array | string): string => Buffer.from(bytes).toString("base64url");
const fromB64url = (s: string): Buffer => Buffer.from(s, "base64url");

/** Sign `header.payload` with HS512 exactly as `auth::sign` does (base64url, no padding). */
function hs512(signingInput: string, secret: string): string {
  return b64url(createHmac("sha512", secret).update(signingInput).digest());
}

interface Claims {
  sub: string;
  roles: string;
  kind: string;
  iat: number;
  exp: number;
  extra?: Record<string, unknown>;
}

/** Mint a token the way the host does; `header`/`secret` overridable for the negative cases. */
function mint(claims: Partial<Claims> & Record<string, unknown>, opts: { header?: string; secret?: string } = {}): string {
  const now = nowSecs();
  const payload = { sub: "minted@conf.test", roles: "U", kind: "user", iat: now, exp: now + SESSION_SECS, ...claims };
  const header = b64url(opts.header ?? '{"alg":"HS512","typ":"JWT"}');
  const body = b64url(JSON.stringify(payload));
  return `${header}.${body}.${hs512(`${header}.${body}`, opts.secret ?? SECRET)}`;
}

function nowSecs(): number {
  return Math.floor(Date.now() / 1000);
}

/** Decode a host token: the header must be byte-exact, the signature must verify. */
function decodeToken(token: string): { header: string; claims: Claims } {
  const parts = token.split(".");
  expect(parts.length, `token has three parts: ${token}`).toBe(3);
  const [h, p, s] = parts;
  const header = fromB64url(h).toString("utf8");
  const expected = hs512(`${h}.${p}`, SECRET);
  expect(timingSafeEqual(Buffer.from(expected), Buffer.from(s)), `HS512 signature verifies with the tenant secret`).toBe(true);
  return { header, claims: JSON.parse(fromB64url(p).toString("utf8")) as Claims };
}

async function loginRaw(client: Rs2Client, email: string, password: string, mount = "/auth", headers: Record<string, string> = {}) {
  return client.post(`${mount}/login`, { json: { email, password }, token: null, headers });
}

async function loginToken(email: string, password: string, mount = "/auth"): Promise<{ token: string; exp: number; res: Rs2Response }> {
  const res = await loginRaw(anon, email, password, mount);
  expect(res.status, `login ${email}: ${res.describe()}`).toBe(200);
  const body = res.json<{ token: string; exp: number }>();
  return { token: body.token, exp: body.exp, res };
}

function expectProblem(res: Rs2Response, status: number, code: string, detail?: string | RegExp): void {
  expect(res.status, res.describe()).toBe(status);
  const p = res.problem();
  expect(p.status, `problem.status mirrors the status line`).toBe(status);
  expect(p.code, `problem.code`).toBe(code);
  expect(p.type).toBe(`https://rs2.dev/errors#${code}`);
  expect(p.tenant).toBe(env().tenant);
  expect(p.traceId, `traceId present`).toMatch(/\S/);
  if (detail instanceof RegExp) expect(p.detail, `problem.detail`).toMatch(detail);
  else if (detail !== undefined) expect(p.detail, `problem.detail`).toBe(detail);
}

const setCookies = (res: Rs2Response): string[] => res.headers.getSetCookie();

// ---- login -----------------------------------------------------------------

describe("POST /auth/login", () => {
  test("issues an HS512 token, exp, and a Strict cookie for a non-browser caller", async () => {
    const before = nowSecs();
    const { token, exp, res } = await loginToken(env().adminEmail, env().adminPassword);
    expect(res.contentType()).toBe("application/json");
    expect(Object.keys(res.json()).sort(), "body is exactly {token, exp}").toEqual(["exp", "token"]);
    expect(exp, "exp is now + sessionMinutes").toBeGreaterThanOrEqual(before + SESSION_SECS);
    expect(exp).toBeLessThanOrEqual(nowSecs() + SESSION_SECS + 5);

    // No Origin → SameSite=Strict with Max-Age = sessionMinutes * 60.
    expect(setCookies(res), "one rs-auth cookie").toEqual([
      `rs-auth=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${SESSION_SECS}`,
    ]);

    // The token: literal header bytes, claims exactly {sub, roles, kind, iat, exp}
    // (no `extra` when there are no extra claims), space-joined roles.
    const { header, claims } = decodeToken(token);
    expect(header).toBe('{"alg":"HS512","typ":"JWT"}');
    expect(Object.keys(claims).sort()).toEqual(["exp", "iat", "kind", "roles", "sub"]);
    expect(claims.sub).toBe(env().adminEmail);
    expect(claims.roles, "bootstrap admin holds A").toBe("A");
    expect(claims.kind).toBe("user");
    expect(claims.exp).toBe(exp);
    expect(claims.exp - claims.iat).toBe(SESSION_SECS);
  });

  test("a bad password and an unknown user are the same 401 (no enumeration)", async () => {
    const bad = await loginRaw(anon, DEV.email, "not-the-password");
    expectProblem(bad, 401, "unauthorized", "invalid credentials");
    expect(bad.header("retry-after"), "no lockout yet").toBeNull();
    expect(bad.header("set-cookie")).toBeNull();
    const unknown = await loginRaw(anon, "nobody-here@conf.test", "whatever");
    expectProblem(unknown, 401, "unauthorized", "invalid credentials");
    const strip = (r: Rs2Response) => {
      const { traceId: _t, ...rest } = r.problem();
      return rest;
    };
    expect(strip(unknown), "bodies differ only by traceId").toEqual(strip(bad));
  });

  test("a record without a usable passwordHash never logs in", async () => {
    await putUser("users", "nohash@conf.test", { roles: "U" });
    expectProblem(await loginRaw(anon, "nohash@conf.test", ""), 401, "unauthorized", "invalid credentials");
    // An unknown hash format (neither $argon2* nor $2*) verifies nothing.
    await putUser("users", "plainhash@conf.test", { passwordHash: "plaintext", roles: "U" });
    expectProblem(await loginRaw(anon, "plainhash@conf.test", "plaintext"), 401, "unauthorized", "invalid credentials");
  });

  test("body validation: JSON with string email and password", async () => {
    expectProblem(await anon.post("/auth/login", { token: null }), 400, "bad_request", "login requires a JSON body");
    expectProblem(
      await anon.post("/auth/login", { body: "email=a&password=b", contentType: "application/x-www-form-urlencoded", token: null }),
      400,
      "bad_request",
      "expected a JSON body, got 'application/x-www-form-urlencoded'",
    );
    expectProblem(
      await anon.post("/auth/login", { body: "{not json", contentType: "application/json", token: null }),
      400,
      "bad_request",
      /^invalid JSON body: /,
    );
    expectProblem(await anon.post("/auth/login", { json: { password: "x" }, token: null }), 400, "bad_request", "login requires 'email'");
    expectProblem(await anon.post("/auth/login", { json: { email: 42, password: "x" }, token: null }), 400, "bad_request", "login requires 'email'");
    expectProblem(await anon.post("/auth/login", { json: { email: DEV.email }, token: null }), 400, "bad_request", "login requires 'password'");
    expectProblem(await anon.post("/auth/login", { json: { email: DEV.email, password: null }, token: null }), 400, "bad_request", "login requires 'password'");
  });

  test("unknown endpoints and verbs are 404 naming the four endpoints", async () => {
    const have = "(have: POST login/refresh/logout, GET user)";
    expectProblem(await anon.get("/auth/login"), 404, "not_found", `auth endpoint '/login' ${have}`);
    expectProblem(await anon.post("/auth/nope", { json: {} }), 404, "not_found", `auth endpoint '/nope' ${have}`);
    expectProblem(await anon.post("/auth/user", { json: {} }), 404, "not_found", `auth endpoint '/user' ${have}`);
    expectProblem(await anon.get("/auth/"), 404, "not_found", `auth endpoint '/' ${have}`);
    expectProblem(await anon.delete("/auth/logout"), 404, "not_found", `auth endpoint '/logout' ${have}`);
  });
});

// ---- user ------------------------------------------------------------------

describe("GET /auth/user", () => {
  test("reflects the verified principal from a bearer or the rs-auth cookie", async () => {
    const { token } = await loginToken(env().adminEmail, env().adminPassword);
    const expected = { id: env().adminEmail, roles: ["A"], kind: "user" };

    const bearer = await anon.get("/auth/user", { token });
    expect(bearer.status, bearer.describe()).toBe(200);
    expect(bearer.json()).toEqual(expected);

    const cookie = await anon.get("/auth/user", { headers: { cookie: `a=1; rs-auth=${token}; b=2` } });
    expect(cookie.status, cookie.describe()).toBe(200);
    expect(cookie.json()).toEqual(expected);

    // Extra whitespace after the prefix is trimmed.
    const padded = await anon.get("/auth/user", { headers: { authorization: `Bearer   ${token}  ` } });
    expect(padded.status, padded.describe()).toBe(200);

    // The header wins over the cookie: a bad cookie next to a good bearer is ignored.
    const both = await anon.get("/auth/user", { token, headers: { cookie: "rs-auth=garbage" } });
    expect(both.status, both.describe()).toBe(200);
    expect(both.json()).toEqual(expected);
  });

  test("anonymous is 401; the `Bearer ` prefix is case-sensitive (a lowercase scheme is anonymous, not malformed)", async () => {
    expectProblem(await anon.get("/auth/user"), 401, "unauthorized", "no authenticated principal");
    const { token } = await loginToken(DEV.email, DEV.password);
    expectProblem(
      await anon.get("/auth/user", { headers: { authorization: `bearer ${token}` } }),
      401,
      "unauthorized",
      "no authenticated principal",
    );
    expectProblem(await anon.get("/auth/user", { headers: { cookie: "rsauth=" + token } }), 401, "unauthorized", "no authenticated principal");
  });

  test("a self-minted token is a principal: roles split on whitespace, kind and extra claims carried", async () => {
    const token = mint({ sub: "agent-7", roles: "U  E\tops", kind: "agent", extra: { team: "green", accountId: "acc-1" } });
    const res = await anon.get("/auth/user", { token });
    expect(res.status, res.describe()).toBe(200);
    expect(res.json()).toEqual({ id: "agent-7", roles: ["U", "E", "ops"], kind: "agent", team: "green", accountId: "acc-1" });

    const none = await anon.get("/auth/user", { token: mint({ roles: "" }) });
    expect(none.status, none.describe()).toBe(200);
    expect(none.json().roles, "empty roles claim → no roles").toEqual([]);
  });
});

// ---- bad tokens are never anonymous ------------------------------------------

describe("a presented-but-bad token is 401 on every path", () => {
  const cases: [string, () => string, string][] = [
    ["garbage", () => "garbage", "malformed token"],
    ["two parts", () => "a.b", "malformed token"],
    ["four parts", () => mint({}) + ".x", "malformed token"],
    ["undecodable header", () => "!!!.x.y", "malformed token header"],
    ["header not JSON", () => `${b64url("nope")}.x.y`, "malformed token header"],
    ["alg HS256", () => mint({}, { header: '{"alg":"HS256","typ":"JWT"}' }), "unsupported token algorithm"],
    ["alg none", () => mint({}, { header: '{"alg":"none"}' }), "unsupported token algorithm"],
    ["undecodable signature", () => mint({}).replace(/\.[^.]+$/, ".!!!"), "malformed signature"],
    ["wrong secret", () => mint({}, { secret: "not-conf-secret" }), "invalid token signature"],
    ["tampered payload", () => {
      const [h, , s] = mint({}).split(".");
      return `${h}.${b64url(JSON.stringify({ sub: "x", roles: "A", kind: "user", iat: 0, exp: 9e9 }))}.${s}`;
    }, "invalid token signature"],
    ["payload missing claims", () => {
      const h = b64url('{"alg":"HS512","typ":"JWT"}');
      const p = b64url(JSON.stringify({ foo: 1 }));
      return `${h}.${p}.${hs512(`${h}.${p}`, SECRET)}`;
    }, "malformed token payload"],
    ["expired", () => mint({ iat: nowSecs() - 7200, exp: nowSecs() - 60 }), "token expired"],
  ];

  // An open store, the discovery surface, and the auth mount itself: the
  // host verifies the token before routing, so no path is exempt.
  const paths = ["/data/", "/.well-known/rs2/services", "/auth/user"];

  for (const [name, make, detail] of cases) {
    test(`${name} → 401 '${detail}' as bearer and as cookie`, async () => {
      const token = make();
      for (const path of paths) {
        expectProblem(await anon.get(path, { token }), 401, "unauthorized", detail);
      }
      expectProblem(await anon.get("/data/", { headers: { cookie: `rs-auth=${token}` } }), 401, "unauthorized", detail);
    });
  }

  test("a host token with a byte appended (m2's tampered case) is 401", async () => {
    const { token } = await loginToken(DEV.email, DEV.password);
    expectProblem(await anon.get("/admin/", { token: `${token}x` }), 401, "unauthorized");
    // The open mount answers the same request without the token.
    const plain = await anon.get("/data/");
    expect(plain.status, plain.describe()).toBe(200);
  });
});

// ---- refresh -----------------------------------------------------------------

describe("POST /auth/refresh — the halfway rule", () => {
  test("before halfway the same token comes back unchanged and no cookie is set", async () => {
    const { token, exp } = await loginToken(DEV.email, DEV.password);
    const res = await anon.post("/auth/refresh", { token });
    expect(res.status, res.describe()).toBe(200);
    expect(res.json()).toEqual({ token, exp });
    expect(res.header("set-cookie"), "no cookie before halfway").toBeNull();
  });

  test("past halfway a new token is issued with the same sub/roles/kind/extra and a fresh cookie", async () => {
    const now = nowSecs();
    const old = mint({ sub: DEV.email, roles: "U E", kind: "agent", iat: now - 3000, exp: now + 600, extra: { team: "blue" } });
    const res = await anon.post("/auth/refresh", { token: old });
    expect(res.status, res.describe()).toBe(200);
    const body = res.json<{ token: string; exp: number }>();
    expect(body.token, "re-issued").not.toBe(old);
    expect(body.exp).toBeGreaterThanOrEqual(now + SESSION_SECS);
    expect(body.exp).toBeLessThanOrEqual(nowSecs() + SESSION_SECS + 5);
    const { claims } = decodeToken(body.token);
    expect(claims.sub).toBe(DEV.email);
    expect(claims.roles).toBe("U E");
    expect(claims.kind).toBe("agent");
    expect(claims.extra).toEqual({ team: "blue" });
    expect(claims.exp - claims.iat).toBe(SESSION_SECS);
    expect(setCookies(res)).toEqual([`rs-auth=${body.token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${SESSION_SECS}`]);

    // Exactly at halfway is refreshable (`now < halfway` is the only bar).
    const at = mint({ iat: now - 1800, exp: now + 1800 });
    const atRes = await anon.post("/auth/refresh", { token: at });
    expect(atRes.status, atRes.describe()).toBe(200);
    expect(atRes.json().token).not.toBe(at);
  });

  test("the cookie is a credential for refresh too", async () => {
    const now = nowSecs();
    const old = mint({ sub: MULTI.email, iat: now - 3000, exp: now + 600 });
    const res = await anon.post("/auth/refresh", { headers: { cookie: `rs-auth=${old}` } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.json().token).not.toBe(old);
  });

  test("no token → 401; a bad or expired token → 401 with the verify wording", async () => {
    expectProblem(await anon.post("/auth/refresh"), 401, "unauthorized", "refresh requires a token");
    expectProblem(await anon.post("/auth/refresh", { token: "garbage" }), 401, "unauthorized", "malformed token");
    expectProblem(
      await anon.post("/auth/refresh", { token: mint({ iat: nowSecs() - 7200, exp: nowSecs() - 60 }) }),
      401,
      "unauthorized",
      "token expired",
    );
  });
});

// ---- logout ----------------------------------------------------------------

describe("POST /auth/logout", () => {
  test("204 with the cookie cleared, signed in or not", async () => {
    const cleared = "rs-auth=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0";
    const { token } = await loginToken(DEV.email, DEV.password);
    for (const opts of [{ token }, { token: null as string | null }, { headers: { cookie: `rs-auth=${token}` } }]) {
      const res = await anon.post("/auth/logout", opts);
      expect(res.status, res.describe()).toBe(204);
      expect(res.bytes.length, "no body").toBe(0);
      expect(setCookies(res)).toEqual([cleared]);
    }
  });
});

// ---- lockout ---------------------------------------------------------------

describe("login lockout", () => {
  test(`after ${MAX_ATTEMPTS} failures the account is locked with Retry-After; a success resets the count`, async () => {
    // Four failures, then a success: the counter clears...
    for (let i = 1; i < MAX_ATTEMPTS; i++) {
      expectProblem(await loginRaw(anon, LOCK.email, "wrong"), 401, "unauthorized", "invalid credentials");
    }
    await loginToken(LOCK.email, LOCK.password);
    // ...so four more do not lock, and the fifth failure itself still reads
    // as bad credentials.
    for (let i = 1; i <= MAX_ATTEMPTS; i++) {
      const res = await loginRaw(anon, LOCK.email, "wrong");
      expectProblem(res, 401, "unauthorized", "invalid credentials");
      expect(res.header("retry-after")).toBeNull();
    }
    // Now even the right password is refused, with Retry-After and retryAfterMs.
    const locked = await loginRaw(anon, LOCK.email, LOCK.password);
    expectProblem(locked, 401, "unauthorized", "account temporarily locked");
    const problem = locked.problem();
    expect(problem.retryable).toBe(false);
    expect(problem.retryAfterMs, "retryAfterMs is the remaining lock").toBeGreaterThan(0);
    expect(problem.retryAfterMs!).toBeLessThanOrEqual(LOCK_SECS * 1000);
    const retryAfter = Number(locked.header("retry-after"));
    expect(retryAfter, "Retry-After is ceil(retryAfterMs / 1000)").toBe(Math.ceil(problem.retryAfterMs! / 1000));
    expect(locked.header("set-cookie")).toBeNull();
    // Still locked on the next try; other accounts are unaffected.
    expectProblem(await loginRaw(anon, LOCK.email, LOCK.password), 401, "unauthorized", "account temporarily locked");
    await loginToken(DEV.email, DEV.password);
  });

  test("unknown accounts lock too (the check runs before the user lookup)", async () => {
    const ghost = "ghost@conf.test";
    for (let i = 1; i <= MAX_ATTEMPTS; i++) {
      expectProblem(await loginRaw(anon, ghost, "x"), 401, "unauthorized", "invalid credentials");
    }
    const locked = await loginRaw(anon, ghost, "x");
    expectProblem(locked, 401, "unauthorized", "account temporarily locked");
    expect(locked.header("retry-after")).not.toBeNull();
  });
});

// ---- mount-level settings override ------------------------------------------

describe("auth mount config overrides the tenant auth settings", () => {
  test("userDataset, sessionMinutes, and maxAttempts come from the mount", async () => {
    // `bot` exists only in `/data/agents`: `/auth2` finds it, `/auth` does not.
    expectProblem(await loginRaw(anon, BOT.email, BOT.password), 401, "unauthorized", "invalid credentials");
    const { token, exp, res } = await loginToken(BOT.email, BOT.password, "/auth2");
    const { claims } = decodeToken(token);
    expect(claims.exp - claims.iat, "sessionMinutes: 2").toBe(120);
    expect(exp).toBe(claims.exp);
    expect(setCookies(res)).toEqual([`rs-auth=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=120`]);
    // The principal is host-level: any auth mount reflects it.
    const who = await anon.get("/auth/user", { token });
    expect(who.status, who.describe()).toBe(200);
    expect(who.json()).toEqual({ id: BOT.email, roles: ["U", "E"], kind: "agent" });

    // maxAttempts: 3 on this mount.
    for (let i = 1; i <= 3; i++) {
      expectProblem(await loginRaw(anon, LOCKBOT.email, "wrong", "/auth2"), 401, "unauthorized", "invalid credentials");
    }
    expectProblem(await loginRaw(anon, LOCKBOT.email, LOCKBOT.password, "/auth2"), 401, "unauthorized", "account temporarily locked");
  });
});

// ---- roles: array vs string, defaults ----------------------------------------

describe("user record roles and kind", () => {
  test("a roles array joins with spaces; a string is taken verbatim; non-strings are dropped", async () => {
    const multi = await loginToken(MULTI.email, MULTI.password);
    expect(decodeToken(multi.token).claims.roles).toBe("U E");
    const who = await anon.get("/auth/user", { token: multi.token });
    expect(who.json()).toEqual({ id: MULTI.email, roles: ["U", "E"], kind: "user" });

    await putUser("users", "mixed@conf.test", { passwordHash: RUST_HASH.passwordHash, roles: ["U", 7, "E", null] });
    const mixed = await loginToken("mixed@conf.test", RUST_HASH.password);
    expect(decodeToken(mixed.token).claims.roles).toBe("U E");

    await putUser("users", "spaced@conf.test", { passwordHash: RUST_HASH.passwordHash, roles: "A  U" });
    const spaced = await loginToken("spaced@conf.test", RUST_HASH.password);
    expect(decodeToken(spaced.token).claims.roles, "string stored verbatim in the claim").toBe("A  U");
    const spacedWho = await anon.get("/auth/user", { token: spaced.token });
    expect(spacedWho.json().roles, "split on whitespace into the principal").toEqual(["A", "U"]);
  });

  test("missing roles default to U, missing kind to user, non-string kind to user", async () => {
    await putUser("users", "norole@conf.test", { passwordHash: RUST_HASH.passwordHash });
    const { token } = await loginToken("norole@conf.test", RUST_HASH.password);
    expect(decodeToken(token).claims.roles).toBe("U");
    const who = await anon.get("/auth/user", { token });
    expect(who.json()).toEqual({ id: "norole@conf.test", roles: ["U"], kind: "user" });

    await putUser("users", "badkind@conf.test", { passwordHash: RUST_HASH.passwordHash, roles: 5, kind: 5 });
    const bad = await loginToken("badkind@conf.test", RUST_HASH.password);
    expect(decodeToken(bad.token).claims).toMatchObject({ roles: "U", kind: "user" });
  });
});

// ---- jwtUserProps and {claim} path grants --------------------------------------

describe("jwtUserProps → extra claims → {claim} path grants", () => {
  test("named record fields ride in `extra`; null and absent fields are skipped; id/roles/kind win name clashes", async () => {
    const dev = await loginToken(DEV.email, DEV.password);
    const { claims } = decodeToken(dev.token);
    expect(claims.extra, "only the named props, absent ones skipped").toEqual({ team: "blue", id: "spoof" });
    const who = await anon.get("/auth/user", { token: dev.token });
    expect(who.json(), "id from the principal, not the extra claim").toEqual({ id: DEV.email, roles: ["U"], kind: "user", team: "blue" });

    const noteam = await loginToken(NOTEAM.email, NOTEAM.password);
    expect("extra" in decodeToken(noteam.token).claims, "no extra key when nothing resolved").toBe(false);

    await putUser("users", "nullteam@conf.test", { passwordHash: RUST_HASH.passwordHash, roles: "U", team: null });
    const nul = await loginToken("nullteam@conf.test", RUST_HASH.password);
    expect("extra" in decodeToken(nul.token).claims, "null values are skipped").toBe(false);
  });

  test("`{team}` scopes reads to the caller's own subtree; unresolved placeholders fail closed", async () => {
    for (const team of ["blue", "red"]) {
      const res = await admin.put(`/teams/${team}/hello.txt`, { body: `hi ${team}`, contentType: "text/plain" });
      expect(res.status, res.describe()).toBe(201);
    }
    const dev = await seed.clientAs(DEV);
    let res = await dev.get("/teams/blue/hello.txt");
    expect(res.status, res.describe()).toBe(200);
    expect(res.text()).toBe("hi blue");
    res = await dev.get("/teams/blue/");
    expect(res.status, `own subtree lists: ${res.describe()}`).toBe(200);
    expectProblem(
      await dev.get("/teams/red/hello.txt"),
      403,
      "forbidden",
      "principal lacks a role satisfying 'authenticated /{team}' for GET",
    );
    expectProblem(await dev.get("/teams/bluer/hello.txt"), 403, "forbidden", /^principal lacks a role/);
    expectProblem(await dev.get("/teams/"), 403, "forbidden", /^principal lacks a role/);
    // No `team` claim → `{team}` stays verbatim → matches nothing.
    const noteam = await seed.clientAs(NOTEAM);
    expectProblem(await noteam.get("/teams/blue/hello.txt"), 403, "forbidden", /^principal lacks a role/);
    expectProblem(await noteam.get("/teams/{team}/hello.txt"), 403, "forbidden", /^principal lacks a role/);
    // Anonymous: 401 rather than 403.
    expectProblem(await anon.get("/teams/blue/hello.txt"), 401, "unauthorized", "this mount requires authentication");
    // The `A` write grant is unaffected by the read scoping.
    expectProblem(await dev.put("/teams/blue/mine.txt", { body: "x", contentType: "text/plain" }), 403, "forbidden", "principal lacks a role satisfying 'A' for PUT");
  });

  test("`{email}` resolves to the principal id on segment boundaries", async () => {
    const dev = await seed.clientAs(DEV);
    let res = await dev.put(`/boxes/${DEV.email}/note.txt`, { body: "mine", contentType: "text/plain" });
    expect(res.status, res.describe()).toBe(201);
    res = await dev.put(`/boxes/${DEV.email}/deeper/note.txt`, { body: "mine", contentType: "text/plain" });
    expect(res.status, res.describe()).toBe(201);
    expectProblem(
      await dev.put(`/boxes/${NOTEAM.email}/note.txt`, { body: "theirs", contentType: "text/plain" }),
      403,
      "forbidden",
      "principal lacks a role satisfying 'U /{email}' for PUT",
    );
    expectProblem(await dev.put(`/boxes/${DEV.email}x/note.txt`, { body: "x", contentType: "text/plain" }), 403, "forbidden", /^principal lacks a role/);
    expectProblem(await dev.delete(`/boxes/${NOTEAM.email}/note.txt`), 403, "forbidden", "principal lacks a role satisfying 'U /{email}' for DELETE");
    expectProblem(await anon.put(`/boxes/${DEV.email}/note.txt`, { body: "x", contentType: "text/plain" }), 401, "unauthorized", "this mount requires authentication");
    // Reads are open, so anyone sees the note.
    res = await anon.get(`/boxes/${DEV.email}/note.txt`);
    expect(res.status, res.describe()).toBe(200);
    expect(res.text()).toBe("mine");
  });
});

// ---- RBAC on an ordinary mount (m2's /admin shape) ----------------------------

describe("role-gated mount (m2 shape)", () => {
  test("anonymous write 401, plain user 403, admin 201, reads open", async () => {
    const body = { body: "hello", contentType: "text/plain" };
    expectProblem(await anon.put("/admin/notes/notes.txt", body), 401, "unauthorized", "this mount requires authentication");
    const dev = await seed.clientAs(DEV);
    expectProblem(await dev.put("/admin/notes/notes.txt", body), 403, "forbidden", "principal lacks a role satisfying 'A' for PUT");
    const ok = await admin.put("/admin/notes/notes.txt", body);
    expect(ok.status, ok.describe()).toBe(201);
    const read = await anon.get("/admin/notes/notes.txt");
    expect(read.status, read.describe()).toBe(200);
    expect(read.text()).toBe("hello");
  });
});

// ---- cross-host hash fixture ------------------------------------------------

describe("cross-host password hashes (fixtures/auth-hashes.json)", () => {
  for (const entry of HASHES) {
    const label = `a ${entry.algorithm} hash minted by ${entry.mintedBy} logs in here`;
    test.skipIf(entry.passwordHash === null)(label, async () => {
      if (entry.algorithm === "argon2id") {
        expect(entry.passwordHash, "PHC argon2id with the agreed parameters").toMatch(/^\$argon2id\$v=19\$m=19456,t=2,p=1\$[A-Za-z0-9+/]+\$[A-Za-z0-9+/]+$/);
      }
      const email = `hash-${entry.mintedBy}@conf.test`;
      await putUser("users", email, { passwordHash: entry.passwordHash, roles: "U" });
      const { token } = await loginToken(email, entry.password);
      expect(decodeToken(token).claims.sub).toBe(email);
      expectProblem(await loginRaw(anon, email, entry.password + "x"), 401, "unauthorized", "invalid credentials");
    });
  }

  test("a hash minted by this host through the facade has the agreed shape", async () => {
    await seed.createPrincipals([{ email: "minted@conf.test", password: "minted-pw", roles: "U" }]);
    const rec = await admin.getJson<{ passwordHash: string }>(`/data/users/${seg("minted@conf.test")}`);
    expect(rec.passwordHash).toMatch(/^\$argon2id\$v=19\$m=19456,t=2,p=1\$/);
    await loginToken("minted@conf.test", "minted-pw");
  });
});
