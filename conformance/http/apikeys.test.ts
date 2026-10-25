// The `apikeys` service over HTTP (rs2-core/tests/authn.rs): long-lived,
// revocable keys that authenticate as `Bearer rs2k_…` through the host's
// authenticator chain, next to the `auth` service's JWTs.

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import type { Rs2Client } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

describe("apikeys service (apikeys.rs)", () => {
  let seed: Seed | undefined;
  let asA: Rs2Client;
  let asU: Rs2Client;
  let asV: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    await seed.applyMounts([
      {
        path: "/keys",
        service: "apikeys",
        config: {
          access: { read: "authenticated", invoke: "authenticated", delete: "authenticated" },
          adminRole: "A",
          maxDays: 30,
        },
      },
      { path: "/kdata", service: "data", config: { access: { read: "U A", write: "U A" } } },
    ]);
    await seed.createPrincipals([
      { email: "ku@conf.test", password: "ku-pw", roles: "U W" },
      { email: "kv@conf.test", password: "kv-pw", roles: "U" },
    ]);
    seed.trackDataset("/data", "apikeys");
    seed.trackDataset("/kdata", "knotes");
    asA = seed.admin;
    asU = await seed.clientAs({ email: "ku@conf.test", password: "ku-pw" });
    asV = await seed.clientAs({ email: "kv@conf.test", password: "kv-pw" });
  });

  afterAll(async () => {
    await seed?.restore();
  });

  async function mint(client: Rs2Client, body: unknown) {
    const res = await client.post("/keys", { json: body });
    expect(res.status, res.describe()).toBe(201);
    return res.json<{ id: string; key: string; roles: string; expires: number }>();
  }

  test("a key authenticates as its owner, with its own roles", async () => {
    const made = await mint(asU, { name: "ci", roles: "U" });
    expect(made.key.startsWith(`rs2k_${made.id}_`)).toBe(true);
    expect(made.roles).toBe("U");

    const me = await seed!.anon.get("/auth/user", { token: made.key });
    expect(me.status, me.describe()).toBe(200);
    expect(me.json()).toMatchObject({ id: "ku@conf.test", kind: "key", roles: ["U"], keyId: made.id });

    const put = await seed!.anon.put("/kdata/knotes/1", { json: { n: 1 }, token: made.key });
    expect([200, 201], put.describe()).toContain(put.status);

    // A key never mints keys, and never more roles than its maker holds.
    const again = await seed!.anon.post("/keys", { json: { name: "x", roles: "U" }, token: made.key });
    expect(again.status, again.describe()).toBe(403);
    const wider = await asU.post("/keys", { json: { name: "x", roles: "U A" } });
    expect(wider.status, wider.describe()).toBe(403);
    const tooLong = await asU.post("/keys", { json: { name: "x", expiresInDays: 31 } });
    expect(tooLong.status, tooLong.describe()).toBe(400);
  });

  test("revoked, unknown, and malformed keys are 401, never anonymous", async () => {
    const made = await mint(asU, { name: "short-lived" });
    const del = await asU.delete(`/keys/${made.id}`);
    expect(del.status, del.describe()).toBe(204);
    for (const bad of [made.key, "rs2k_00aa_unknown", "rs2k_nothex_x"]) {
      const res = await seed!.anon.get("/auth/user", { token: bad });
      expect(res.status, `${bad}: ${res.describe()}`).toBe(401);
    }
  });

  test("a record rewritten outside the service grants nothing", async () => {
    const made = await mint(asU, { name: "sealed" });
    const path = `/data/apikeys/${made.id}`;
    const record = (await asA.get(path)).json<Record<string, unknown>>();
    expect(record.seal).toBeTypeOf("string");
    const put = await asA.put(path, { json: { ...record, roles: "U A" } });
    expect(put.status, put.describe()).toBe(200);
    const res = await seed!.anon.get("/auth/user", { token: made.key });
    expect(res.status, res.describe()).toBe(401);
  });

  test("listing shows your own keys, all of them for the admin role, and no secrets", async () => {
    const mine = await mint(asV, { name: "v-key" });
    const list = await asV.get("/keys");
    expect(list.status, list.describe()).toBe(200);
    const keys = list.json<Record<string, unknown>[]>();
    expect(keys.map((k) => k.id)).toEqual([mine.id]);
    expect(keys[0]).not.toHaveProperty("key");
    expect(keys[0]).not.toHaveProperty("seal");
    const all = (await asA.get("/keys")).json<{ id: string }[]>();
    expect(all.map((k) => k.id)).toContain(mine.id);

    // Someone else's key is indistinguishable from no key.
    const steal = await asU.delete(`/keys/${mine.id}`);
    expect(steal.status, steal.describe()).toBe(404);
    const admin = await asA.delete(`/keys/${mine.id}`);
    expect(admin.status, admin.describe()).toBe(204);
  });

  test("the mount advertises the apikeys facet", async () => {
    const res = await asU.request("OPTIONS", "/keys");
    expect(res.status, res.describe()).toBe(200);
    expect(res.json().pattern).toBe("api");
    expect(res.json().facets).toEqual(["apikeys"]);
  });
});
