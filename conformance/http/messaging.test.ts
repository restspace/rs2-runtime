// Outbound messaging — the HTTP conformance port of
// `rs2-core/tests/message_gateway.rs` (spec §F.3).
//
// Deliberate scope: this suite asserts everything about the `message` surface
// that can be settled **without sending anything** — the declared channels, the
// 400s the service raises before an adapter is reached, the config errors
// raised at tenant build, and the 501 a provider gives when it cannot report
// delivery status. Actual provider wire shapes (Cloudflare's JSON body, SNS's
// signed form post) are covered by unit tests against stubs on each host,
// because a conformance run must not send real mail or real SMS, and a mock
// provider reachable from both hosts would be testing the mock.
//
// What that leaves here is exactly the cross-host contract: two hosts, one set
// of wordings, one answer to "what can this mount do".

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { Seed } from "./src/seed.ts";

// Syntactically valid, deliberately inert credentials: building an adapter
// never reaches the network, and no test here sends.
const SNS = {
  adapter: "builtin:aws-sns",
  region: "eu-west-1",
  accessKeyId: "AKIACONFORMANCE",
  secretAccessKey: "not-a-real-key",
};
const CF_EMAIL = {
  adapter: "builtin:cf-email",
  accountId: "conformance",
  from: "noreply@conf.test",
  auth: "bearer",
  token: "not-a-real-token",
};

let seed: Seed;

beforeAll(async () => {
  seed = await Seed.create();
  await seed.applyMounts([
    { path: "/msg-sms", service: "message", config: { access: "open", store: SNS } },
    { path: "/msg-mail", service: "message", config: { access: "open", store: CF_EMAIL } },
    {
      path: "/msg",
      service: "message",
      config: { access: "open", store: { adapters: { email: CF_EMAIL, sms: SNS } } },
    },
  ]);
});

afterAll(async () => {
  await seed?.restore();
});

describe("message mounts declare what they can do", () => {
  test("a single-adapter mount advertises that adapter's channel", async () => {
    const res = await seed.anon.get("/msg-sms/channels");
    expect(res.status, `GET /msg-sms/channels: ${res.describe()}`).toBe(200);
    expect(await res.json()).toEqual({
      channels: ["sms"],
      deliveryStatus: false,
      provider: "aws-sns",
    });
  });

  test("cf-email declares email and no delivery status", async () => {
    const res = await seed.anon.get("/msg-mail/channels");
    expect(res.status, res.describe()).toBe(200);
    expect(await res.json()).toEqual({
      channels: ["email"],
      deliveryStatus: false,
      provider: "cf-email",
    });
  });

  test("a per-channel map advertises the union under one provider", async () => {
    const res = await seed.anon.get("/msg/channels");
    expect(res.status, res.describe()).toBe(200);
    const doc = (await res.json()) as { channels: string[]; provider: string };
    expect(doc.channels).toEqual(["email", "sms"]);
    expect(doc.provider).toBe("routing");
  });

  test("the mount advertises the channels facet in discovery", async () => {
    const res = await seed.anon.get("/.well-known/rs2/services");
    expect(res.status, res.describe()).toBe(200);
    const doc = (await res.json()) as { services: Array<{ path: string; pattern: string; facets: string[] }> };
    const mount = doc.services.find((m) => m.path === "/msg");
    expect(mount, "the /msg mount is discoverable").toBeDefined();
    expect(mount!.pattern).toBe("api");
    expect(mount!.facets).toContain("channels");
  });
});

describe("a send is refused before any provider is reached", () => {
  test("an unserved channel names what the mount does serve", async () => {
    const res = await seed.anon.post("/msg-sms/send", {
      json: { channel: "email", to: "a@b.com", subject: "Hi", text: "hi" },
    });
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toBe(
      "this mount has no adapter for the 'email' channel (configured: sms)",
    );
  });

  test("an unknown channel names the known ones", async () => {
    const res = await seed.anon.post("/msg/send", { json: { channel: "carrier-pigeon", to: "x" } });
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toBe("unknown channel 'carrier-pigeon' (one of: email, sms)");
  });

  test("an email with no body is rejected", async () => {
    const res = await seed.anon.post("/msg/send", {
      json: { channel: "email", to: "a@b.com", subject: "Hi" },
    });
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toBe("an email needs 'text', 'html', or both");
  });

  test("a malformed address is a 400 at the edge, not a provider error later", async () => {
    const res = await seed.anon.post("/msg/send", {
      json: { channel: "email", to: "not-an-address", subject: "Hi", text: "hi" },
    });
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toBe("'to' is not an email address: 'not-an-address'");
  });

  test("recipients are capped across to/cc/bcc", async () => {
    const to = Array.from({ length: 40 }, (_, i) => `u${i}@b.com`);
    const cc = Array.from({ length: 11 }, (_, i) => `c${i}@b.com`);
    const res = await seed.anon.post("/msg/send", {
      json: { channel: "email", to, cc, subject: "Hi", text: "hi" },
    });
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toBe("51 recipients across to/cc/bcc exceeds the 50 limit");
  });

  test("sms requires a non-empty recipient and text", async () => {
    for (const [body, detail] of [
      [{ channel: "sms", text: "hi" }, "'to' (non-empty string) is required for sms"],
      [{ channel: "sms", to: "+1555" }, "'text' (non-empty string) is required for sms"],
    ] as const) {
      const res = await seed.anon.post("/msg/send", { json: body });
      expect(res.status, res.describe()).toBe(400);
      expect((await res.problem()).detail).toBe(detail);
    }
  });

  test("an unknown sub-path lists the endpoints", async () => {
    const res = await seed.anon.post("/msg/nope", { json: {} });
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toBe(
      "message endpoint: POST /send {channel, to, …}, GET /status/{id}, GET /channels",
    );
  });
});

describe("delivery status is a declared facet, not a guess", () => {
  test("a provider that cannot report status says so, naming itself", async () => {
    const res = await seed.anon.get("/msg-sms/status/abc-123");
    expect(res.status, res.describe()).toBe(501);
    const problem = await res.problem();
    expect(problem.detail).toBe("provider 'aws-sns' does not report per-message delivery status");
    // 501 here is a provider limitation, distinct from a missing engine.
    expect(problem.code).toBe("provider_unavailable");
  });

  test("a routing mount whose every route lacks status reports none", async () => {
    // Both configured providers answer at send time, so the mount admits it
    // rather than offering a lookup that cannot work.
    const res = await seed.anon.get("/msg/status/abc-123");
    expect(res.status, res.describe()).toBe(501);
    expect((await res.problem()).detail).toBe(
      "provider 'routing' does not report per-message delivery status",
    );
  });
});

describe("misconfiguration is caught at tenant build", () => {
  test("setting both store.adapter and store.adapters is refused", async () => {
    const res = await seed.tryApplyMounts([
      {
        path: "/msg-bad",
        service: "message",
        config: { access: "open", store: { adapter: "builtin:aws-sns", adapters: { sms: SNS } } },
      },
    ]);
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toContain(
      "a message mount sets either store.adapter or store.adapters, not both",
    );
  });

  test("an adapter routed at a channel it does not serve is refused", async () => {
    const res = await seed.tryApplyMounts([
      { path: "/msg-bad", service: "message", config: { access: "open", store: { adapters: { sms: CF_EMAIL } } } },
    ]);
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toContain(
      "adapter 'cf-email' is routed for the 'sms' channel but serves: email",
    );
  });

  test("a mount with no adapter at all names both config shapes", async () => {
    const res = await seed.tryApplyMounts([
      { path: "/msg-bad", service: "message", config: { access: "open" } },
    ]);
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toContain("message mount requires a store.adapter");
  });

  test("an unknown builtin provider lists the ones that exist", async () => {
    const res = await seed.tryApplyMounts([
      { path: "/msg-bad", service: "message", config: { access: "open", store: { adapter: "builtin:carrier" } } },
    ]);
    expect(res.status, res.describe()).toBe(400);
    expect((await res.problem()).detail).toBe(
      "message store adapter 'builtin:carrier' is unknown (available: aws-sns, cf-email)",
    );
  });

  test("the built-in providers are listed in the catalogue", async () => {
    const res = await seed.admin.get("/services/catalogue/available");
    expect(res.status, res.describe()).toBe(200);
    const doc = (await res.json()) as { items: Array<{ name: string; kind: string; adapterKind?: string }> };
    const providers = doc.items
      .filter((i) => i.kind === "adapter" && i.adapterKind === "message")
      .map((i) => i.name)
      .sort();
    expect(providers).toEqual(["aws-sns", "cf-email"]);
  });
});
