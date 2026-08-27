// Domain attachment (`/admin/domains`, spec section B.5). The **read** side
// is the same contract on both hosts: a host is either attached to a tenant
// or it is not, and an attached one reads `active` with nothing left to
// publish. The **write** side is a declared divergence (`domainAttachment`):
// the Worker attaches over HTTP behind a proof-of-control gate, the Rust host
// attaches through `serverConfig.tenancy.domainMap` and says so in a 501.
//
// The gate itself is what these cases pin down: a claim is not a mapping.
// Nothing a client can send makes a host route — only a provider reporting
// that it can see the DNS does that, which is why the pending domain here
// never goes active (nobody points `example.com` at a test deployment).

import { describe, expect, test } from "vitest";

import { env, Rs2Client } from "./src/client.ts";
import { divergences } from "./src/divergences.ts";

/// A host we can safely claim and detach: syntactically valid, not ours, and
/// never resolving to the deployment under test — so it stays `pending` and
/// the mapping is never written.
const HOST = "rs2-conformance.example.com";

const admin = () => new Rs2Client().withToken(env().adminToken);

describe.runIf(env().adminToken)("domain attachment", () => {
  test("the domains listing is a list of host/tenant/status", async () => {
    const res = await admin().get("/admin/domains");
    expect(res.status, res.describe()).toBe(200);
    const doc = res.json();
    expect(Array.isArray(doc.domains), `domains is an array in ${res.text()}`).toBe(true);
    for (const entry of doc.domains) {
      expect(typeof entry.host).toBe("string");
      expect(typeof entry.tenant).toBe("string");
      expect(["active", "pending"]).toContain(entry.status);
    }
  });

  test("an unattached host is a 404, not an empty attachment", async () => {
    const res = await admin().get("/admin/domains/never-attached.example.com");
    expect(res.status, res.describe()).toBe(404);
    expect(res.problem().code).toBe("not_found");
  });

  test("the admin gate applies", async () => {
    const anon = await new Rs2Client().get("/admin/domains");
    expect([401, 503]).toContain(anon.status);
    const wrong = await new Rs2Client().withToken("not-the-token").get("/admin/domains");
    expect(wrong.status, wrong.describe()).toBe(401);
  });

  test.runIf(divergences().domainAttachment === "config")(
    "a config-tenancy host refuses the write side and names the file",
    async () => {
      const res = await admin().put(`/admin/domains/${HOST}`, { json: { tenant: env().tenant } });
      expect(res.status, res.describe()).toBe(501);
      const problem = res.problem();
      expect(problem.code).toBe("provider_unavailable");
      expect(problem.detail).toContain("domainMap");
      const del = await admin().delete(`/admin/domains/${HOST}`);
      expect(del.status, del.describe()).toBe(501);
    },
  );

  test.runIf(divergences().domainAttachment === "api")(
    "PUT claims a host but does not make it route until control is proven",
    async () => {
      try {
        const res = await admin().put(`/admin/domains/${HOST}`, { json: { tenant: env().tenant } });
        // 202: accepted, not yet in effect.
        expect(res.status, res.describe()).toBe(202);
        const doc = res.json();
        expect(doc.host).toBe(HOST);
        expect(doc.tenant).toBe(env().tenant);
        expect(doc.status).toBe("pending");
        expect(typeof doc.nextStep, `nextStep in ${res.text()}`).toBe("string");
        expect(typeof doc.provider?.name).toBe("string");
        expect(Array.isArray(doc.dnsRecords)).toBe(true);
        for (const record of doc.dnsRecords) {
          expect(typeof record.type).toBe("string");
          expect(typeof record.name).toBe("string");
          expect(typeof record.value).toBe("string");
          expect(typeof record.required).toBe("boolean");
        }

        // The claim is readable, and still pending on a re-read.
        const read = await admin().get(`/admin/domains/${HOST}`);
        expect(read.status, read.describe()).toBe(200);
        expect(read.json().status).toBe("pending");

        // It shows in the listing as pending — never as a live mapping.
        const listed = (await admin().getJson("/admin/domains")).domains.find(
          (d: { host: string }) => d.host === HOST,
        );
        expect(listed, `${HOST} missing from the listing`).toBeDefined();
        expect(listed.status).toBe("pending");

        // And a second tenant cannot take it while the claim stands.
        const squat = await admin().put(`/admin/domains/${HOST}`, { json: { tenant: "someone-else" } });
        expect(squat.status, squat.describe()).toBe(409);
      } finally {
        await admin().delete(`/admin/domains/${HOST}`);
      }
    },
  );

  test.runIf(divergences().domainAttachment === "api")("DELETE releases the claim", async () => {
    await admin().put(`/admin/domains/${HOST}`, { json: { tenant: env().tenant } });
    const del = await admin().delete(`/admin/domains/${HOST}`);
    expect(del.status, del.describe()).toBe(204);
    const gone = await admin().get(`/admin/domains/${HOST}`);
    expect(gone.status, gone.describe()).toBe(404);
  });

  test.runIf(divergences().domainAttachment === "api")(
    "a host name that is not a host name is refused",
    async () => {
      for (const bad of ["*.example.com", "example.com:8080"]) {
        const res = await admin().put(`/admin/domains/${encodeURIComponent(bad)}`, {
          json: { tenant: env().tenant },
        });
        expect(res.status, res.describe()).toBe(400);
      }
    },
  );
});
