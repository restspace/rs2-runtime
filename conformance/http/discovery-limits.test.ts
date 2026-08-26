// The `limits` object on `GET /.well-known/rs2/services` (spec section A):
// both hosts emit the same keys; `host` names the kind under test.

import { describe, expect, test } from "vitest";

import { env, Rs2Client } from "./src/client.ts";

describe("discovery limits", () => {
  test("the services document carries a limits object naming the host", async () => {
    const res = await new Rs2Client().get("/.well-known/rs2/services");
    expect(res.status, res.describe()).toBe(200);
    const doc = res.json();
    const limits = doc.limits;
    expect(limits, `no limits object in ${res.text()}`).toBeTypeOf("object");
    for (const key of ["wallClockMs", "memoryBytes", "materializedBodyBytes", "outboundCalls", "maxDepth"]) {
      expect(typeof limits[key], `limits.${key} is a number`).toBe("number");
      expect(limits[key], `limits.${key} is positive`).toBeGreaterThan(0);
    }
    expect(limits.host, `limits.host names the host kind`).toBe(env().hostKind);
  });
});
