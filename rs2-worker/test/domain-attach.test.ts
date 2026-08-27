/// <reference types="@cloudflare/vitest-pool-workers/types" />
// The gated attachment flow end to end (cloudflare.md §B.5), through the real
// Worker and the real `RegistryObject`: a claim does not route, the challenge
// is what proves control, proving it promotes the host, and a second tenant
// cannot take a claim someone else is part-way through.
import { SELF, env, runInDurableObject } from "cloudflare:test";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { RegistryObject } from "../src/registry-object";
import type { Env as WorkerEnv } from "../src/env";
import type { JsonObject } from "../src/runtime/error";

declare module "cloudflare:test" {
  interface ProvidedEnv extends WorkerEnv {}
}

const TOKEN = "test-admin-token";
const HOST = "app.acme.com";
const TARGET = "saas.rs2.example";

function admin(path: string, init: RequestInit = {}): Promise<Response> {
  return SELF.fetch(`http://ops.local${path}`, {
    ...init,
    headers: { authorization: `Bearer ${TOKEN}`, ...(init.headers ?? {}) },
  });
}

async function body(resp: Response): Promise<JsonObject> {
  return (await resp.json()) as JsonObject;
}

function registry(): DurableObjectStub<RegistryObject> {
  return env.REGISTRY.get(env.REGISTRY.idFromName("registry"));
}

/// The token is never in a response — reading it out of the DO is how the
/// test plays the part of the customer's DNS.
async function pendingToken(host: string): Promise<string> {
  const claim = await runInDurableObject(registry(), (instance: RegistryObject) => instance.getPending(host));
  expect(claim, `no pending claim for ${host}`).toBeDefined();
  return claim!.token;
}

/// The customer's DNS, standing in for the internet: the Worker's own
/// `ManualProvider` resolves `fetch` off the global at construction, so
/// replacing it here is what decides whether the challenge can be reached.
/// Unreachable by default — a domain nobody has pointed anywhere.
let serving: { host: string; path: string; token: string } | undefined;
const realFetch = globalThis.fetch;

function pointDnsHere(host: string, token: string): void {
  serving = { host, path: `/.well-known/rs2/domain-challenge/${token}`, token };
}

describe("gated domain attachment", () => {
  beforeAll(() => {
    env.RS2_ADMIN_TOKEN = TOKEN;
    env.RS2_CNAME_TARGET = TARGET;
    // Multi-tenant mode: an unresolved host must 404 rather than falling
    // through to a default tenant, or "does not route yet" proves nothing.
    env.RS2_DEFAULT_TENANT = "";
    globalThis.fetch = (async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (serving && url.hostname === serving.host && url.pathname === serving.path) {
        return new Response(serving.token, { status: 200 });
      }
      throw new TypeError(`getaddrinfo ENOTFOUND ${url.hostname}`);
    }) as typeof fetch;
  });
  afterAll(() => {
    globalThis.fetch = realFetch;
    serving = undefined;
  });

  it("an unattached host does not route", async () => {
    const resp = await SELF.fetch(`http://${HOST}/anything`);
    expect(resp.status).toBe(404);
    expect(await resp.text()).toContain(`no tenant for host '${HOST}'`);
  });

  it("PUT claims the host: 202, pending, and the CNAME to publish", async () => {
    const resp = await admin(`/admin/domains/${HOST}`, {
      method: "PUT",
      body: JSON.stringify({ tenant: "acme" }),
    });
    expect(resp.status).toBe(202);
    const doc = await body(resp);
    expect(doc.status).toBe("pending");
    expect(doc.tenant).toBe("acme");
    expect(doc.provider).toMatchObject({ name: "manual" });
    expect(doc.dnsRecords).toEqual([
      { type: "CNAME", name: HOST, value: TARGET, required: true, purpose: "routes the domain to this deployment" },
    ]);
    expect(String(doc.nextStep)).toContain("publish the required DNS record");
  });

  it("a claim is not a mapping — the host still does not route", async () => {
    const resp = await SELF.fetch(`http://${HOST}/anything`);
    expect(resp.status).toBe(404);
    expect(await resp.text()).toContain(`no tenant for host '${HOST}'`);
  });

  it("the challenge answers only its own host, and only the right token", async () => {
    const token = await pendingToken(HOST);
    const ok = await SELF.fetch(`http://${HOST}/.well-known/rs2/domain-challenge/${token}`);
    expect(ok.status).toBe(200);
    expect((await ok.text()).trim()).toBe(token);

    const wrongToken = await SELF.fetch(`http://${HOST}/.well-known/rs2/domain-challenge/deadbeef`);
    expect(wrongToken.status).toBe(404);
    // Control of one domain must not prove control of another: the same
    // token asked for under a different Host is not an answer.
    const wrongHost = await SELF.fetch(`http://other.example/.well-known/rs2/domain-challenge/${token}`);
    expect(wrongHost.status).toBe(404);
  });

  it("a second tenant cannot displace an unproven claim", async () => {
    const resp = await admin(`/admin/domains/${HOST}`, {
      method: "PUT",
      body: JSON.stringify({ tenant: "squatter" }),
    });
    expect(resp.status).toBe(409);
    expect(await resp.text()).toContain("already claimed by tenant 'acme'");
    // …and the claim it tried to take is untouched.
    expect((await body(await admin(`/admin/domains/${HOST}`))).tenant).toBe("acme");
  });

  it("re-PUT by the same tenant is idempotent and keeps the token", async () => {
    const before = await pendingToken(HOST);
    const resp = await admin(`/admin/domains/${HOST}`, {
      method: "PUT",
      body: JSON.stringify({ tenant: "acme" }),
    });
    expect(resp.status).toBe(202);
    expect(await pendingToken(HOST)).toBe(before);
  });

  it("a host that cannot be reached stays pending, with the reason", async () => {
    const doc = await body(await admin(`/admin/domains/${HOST}`));
    expect(doc.status).toBe("pending");
    expect(JSON.stringify(doc.provider)).toContain("unreachable");
  });

  it("serving the challenge promotes the host, and then it routes", async () => {
    pointDnsHere(HOST, await pendingToken(HOST));
    const doc = await body(await admin(`/admin/domains/${HOST}`));
    expect(doc.status).toBe("active");
    expect(doc.tenant).toBe("acme");
    expect(String(doc.nextStep)).toContain("nothing to do");

    const routed = await SELF.fetch(`http://${HOST}/anything`);
    expect(await routed.text()).not.toContain("no tenant for host");
    // The claim is spent, not merely satisfied.
    await expect(
      runInDurableObject(registry(), (instance: RegistryObject) => instance.getPending(HOST)),
    ).resolves.toBeUndefined();
  });

  it("the listing shows live and pending side by side", async () => {
    await admin("/admin/domains/beta.acme.com", { method: "PUT", body: JSON.stringify({ tenant: "acme" }) });
    const doc = await body(await admin("/admin/domains"));
    expect(doc.domains).toEqual([
      { host: "app.acme.com", tenant: "acme", status: "active" },
      { host: "beta.acme.com", tenant: "acme", status: "pending" },
    ]);
  });

  it("DELETE detaches the mapping and any claim on the host", async () => {
    expect((await admin(`/admin/domains/${HOST}`, { method: "DELETE" })).status).toBe(204);
    expect((await admin("/admin/domains/beta.acme.com", { method: "DELETE" })).status).toBe(204);
    expect((await body(await admin("/admin/domains"))).domains).toEqual([]);
    const resp = await SELF.fetch(`http://${HOST}/anything`);
    expect(await resp.text()).toContain(`no tenant for host '${HOST}'`);
  });

  it("the admin gate and hostname validation still apply", async () => {
    const noToken = await SELF.fetch(`http://ops.local/admin/domains/${HOST}`);
    expect(noToken.status).toBe(401);
    const bad = await admin("/admin/domains/*.acme.com", {
      method: "PUT",
      body: JSON.stringify({ tenant: "acme" }),
    });
    expect(bad.status).toBe(400);
    expect(await bad.text()).toContain("is not a valid host name");
  });
});
