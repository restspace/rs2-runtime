// `src/domains.ts`: the Cloudflare for SaaS status mapping and the
// custom-hostnames client, run against mocked `fetch` responses — the real
// CF API is never reachable from tests.
import { describe, expect, it } from "vitest";
import { CfSaasApi, REGISTRY_ONLY_NOTE, cfApiFromEnv, cnameTarget, domainResponse, mapDomainStatus, parseCustomHostname, validHostname } from "../src/domains";
import type { Json, JsonObject } from "../src/runtime/error";
import { RsError } from "../src/runtime/error";

const OWNERSHIP = { type: "txt", name: "_cf-custom-hostname.app.acme.com", value: "ab-cd-ef" };

function cfResult(status: string, sslStatus: string): JsonObject {
  return {
    id: "chid-1",
    hostname: "app.acme.com",
    status,
    ssl: { status: sslStatus, method: "http", type: "dv" },
    ownership_verification: OWNERSHIP,
  };
}

/// A `fetch` returning canned CF envelopes and recording each call.
function mockFetch(handler: (method: string, url: string, body: Json | undefined) => { status?: number; body: Json }) {
  const calls: Array<{ method: string; url: string; body: Json | undefined }> = [];
  const fetchFn = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    const body = typeof init?.body === "string" ? (JSON.parse(init.body) as Json) : undefined;
    calls.push({ method, url, body });
    const out = handler(method, url, body);
    return new Response(JSON.stringify(out.body), {
      status: out.status ?? 200,
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
  return { fetchFn, calls };
}

function ok(result: Json): { body: Json } {
  return { body: { success: true, errors: [], result } };
}

describe("domain status mapping", () => {
  it("hostname not active -> pending_validation", () => {
    expect(mapDomainStatus("pending", "initializing")).toBe("pending_validation");
    expect(mapDomainStatus("pending", "pending_validation")).toBe("pending_validation");
    expect(mapDomainStatus("blocked", "active")).toBe("pending_validation");
    expect(mapDomainStatus(undefined, undefined)).toBe("pending_validation");
  });

  it("hostname active, ssl not active -> pending_certificate", () => {
    for (const ssl of ["initializing", "pending_validation", "pending_issuance", "pending_deployment", undefined]) {
      expect(mapDomainStatus("active", ssl)).toBe("pending_certificate");
    }
  });

  it("both active -> active", () => {
    expect(mapDomainStatus("active", "active")).toBe("active");
  });

  it("parses a result object, tolerating absent fields", () => {
    const parsed = parseCustomHostname(cfResult("pending", "pending_validation"));
    expect(parsed).toEqual({
      id: "chid-1",
      hostname: "app.acme.com",
      status: "pending_validation",
      cfStatus: "pending",
      cfSslStatus: "pending_validation",
      ownershipVerification: OWNERSHIP,
    });
    const bare = parseCustomHostname({ id: "x" });
    expect(bare.status).toBe("pending_validation");
    expect(bare.cfSslStatus).toBe("unknown");
    expect(bare.ownershipVerification).toBeNull();
  });
});

describe("CfSaasApi", () => {
  it("ensure creates with ssl method http when absent", async () => {
    const { fetchFn, calls } = mockFetch((method) =>
      method === "GET" ? ok([]) : ok(cfResult("pending", "pending_validation")),
    );
    const api = new CfSaasApi("tok", "zone1", fetchFn);
    const view = await api.ensure("app.acme.com");
    expect(view.status).toBe("pending_validation");
    expect(view.ownershipVerification).toEqual(OWNERSHIP);
    expect(calls.map((c) => c.method)).toEqual(["GET", "POST"]);
    expect(calls[0]!.url).toBe("https://api.cloudflare.com/client/v4/zones/zone1/custom_hostnames?hostname=app.acme.com");
    expect(calls[1]!.url).toBe("https://api.cloudflare.com/client/v4/zones/zone1/custom_hostnames");
    expect(calls[1]!.body).toEqual({ hostname: "app.acme.com", ssl: { method: "http", type: "dv" } });
  });

  it("ensure returns the existing hostname without creating", async () => {
    const { fetchFn, calls } = mockFetch(() => ok([cfResult("active", "pending_deployment")]));
    const view = await new CfSaasApi("tok", "zone1", fetchFn).ensure("app.acme.com");
    expect(view.status).toBe("pending_certificate");
    expect(calls.map((c) => c.method)).toEqual(["GET"]);
  });

  it("find polls by hostname; absent -> undefined", async () => {
    const { fetchFn } = mockFetch(() => ok([cfResult("active", "active")]));
    expect((await new CfSaasApi("tok", "z", fetchFn).find("app.acme.com"))?.status).toBe("active");
    const none = mockFetch(() => ok([]));
    expect(await new CfSaasApi("tok", "z", none.fetchFn).find("app.acme.com")).toBeUndefined();
  });

  it("remove looks up the id then deletes; absent -> false", async () => {
    const { fetchFn, calls } = mockFetch((method) => (method === "GET" ? ok([cfResult("active", "active")]) : ok(null)));
    expect(await new CfSaasApi("tok", "zone1", fetchFn).remove("app.acme.com")).toBe(true);
    expect(calls[1]!.method).toBe("DELETE");
    expect(calls[1]!.url).toBe("https://api.cloudflare.com/client/v4/zones/zone1/custom_hostnames/chid-1");
    const none = mockFetch(() => ok([]));
    expect(await new CfSaasApi("tok", "z", none.fetchFn).remove("app.acme.com")).toBe(false);
    expect(none.calls).toHaveLength(1);
  });

  it("maps API errors to a 502 with the CF messages", async () => {
    const { fetchFn } = mockFetch(() => ({
      status: 403,
      body: { success: false, errors: [{ code: 9109, message: "Unauthorized to access requested resource" }] },
    }));
    const err = await new CfSaasApi("tok", "z", fetchFn).find("app.acme.com").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RsError);
    expect((err as RsError).status).toBe(502);
    expect((err as RsError).detail).toContain("HTTP 403");
    expect((err as RsError).detail).toContain("9109: Unauthorized to access requested resource");
  });
});

describe("admin response shape", () => {
  const envBoth = { CF_API_TOKEN: "t", CF_ZONE_ID: "z", RS2_MAIN_DOMAIN: "rs2.example" };

  it("requires both secrets for the CF client", () => {
    expect(cfApiFromEnv({})).toBeUndefined();
    expect(cfApiFromEnv({ CF_API_TOKEN: "t" })).toBeUndefined();
    expect(cfApiFromEnv({ CF_ZONE_ID: "z" })).toBeUndefined();
    expect(cfApiFromEnv(envBoth)).toBeInstanceOf(CfSaasApi);
  });

  it("cname target prefers RS2_CNAME_TARGET, falls back to the main domain", () => {
    expect(cnameTarget({ RS2_CNAME_TARGET: "saas.rs2.example", RS2_MAIN_DOMAIN: "rs2.example" })).toBe("saas.rs2.example");
    expect(cnameTarget({ RS2_MAIN_DOMAIN: "rs2.example" })).toBe("rs2.example");
    expect(cnameTarget({})).toBeNull();
  });

  it("registry-only mode says so explicitly", () => {
    const body = domainResponse("app.acme.com", "acme", {}, false, undefined);
    expect(body.status).toBeNull();
    expect(body.provisioning).toBeNull();
    expect(body.note).toBe(REGISTRY_ONLY_NOTE);
  });

  it("provisioned mode reports the status and the ownership record", () => {
    const view = parseCustomHostname(cfResult("active", "pending_deployment"));
    const body = domainResponse("app.acme.com", "acme", envBoth, true, view) as {
      status: string;
      cnameTarget: string;
      provisioning: { id: string; ownershipVerification: typeof OWNERSHIP };
      note?: string;
    };
    expect(body.status).toBe("pending_certificate");
    expect(body.cnameTarget).toBe("rs2.example");
    expect(body.provisioning.id).toBe("chid-1");
    expect(body.provisioning.ownershipVerification).toEqual(OWNERSHIP);
    expect(body.note).toBeUndefined();
    // Secrets set but nothing provisioned yet: not the registry-only wording.
    const missing = domainResponse("app.acme.com", "acme", envBoth, true, undefined);
    expect(missing.note).toBe("no Cloudflare custom hostname exists for this host");
  });
});

describe("hostname validation (issue #2 item 5)", () => {
  it("accepts real host names", () => {
    for (const h of ["app.acme.com", "a.b.c.d.example", "xn--80ak6aa92e.com", "t1.localhost", "localhost", "a-b.io"]) {
      expect(validHostname(h), h).toBe(true);
    }
  });

  it("rejects what would otherwise reach the registry and the CF API verbatim", () => {
    for (const h of [
      "",
      " ",
      "https://app.acme.com",
      "app.acme.com/path",
      "app.acme.com:8080",
      "*.acme.com",
      "app..acme.com",
      "app.acme.com.",
      ".acme.com",
      "-acme.com",
      "acme-.com",
      "under_score.com",
      "APP.ACME.COM", // the routes lowercase before validating
      `${"a".repeat(64)}.com`,
      `${"a.".repeat(130)}com`,
    ]) {
      expect(validHostname(h), h).toBe(false);
    }
  });
});
