// `RS2_LIMITS`: operator overrides for the host limit table (the same names
// `/.well-known/rs2/services` reports). A deployment whose platform enforces
// a ceiling below RS2's own — Workers Free caps subrequests at 50, under the
// default 64-call outbound budget — lowers the matching limit so RS2's own
// is the one that binds and the breach is a `limit_exceeded` naming it.
import { describe, expect, it, vi } from "vitest";

import { defaultLimits, limitsFromJson } from "../src/runtime/wrapper";

/// `limitsFromJson` warns on anything it ignores; tests assert on that too,
/// so a typo'd var is never silently swallowed.
function withWarnings<T>(f: () => T): [T, string[]] {
  const warnings: string[] = [];
  const spy = vi.spyOn(console, "warn").mockImplementation((...args: unknown[]) => {
    warnings.push(args.map(String).join(" "));
  });
  try {
    return [f(), warnings];
  } finally {
    spy.mockRestore();
  }
}

describe("limitsFromJson", () => {
  it("unset or empty leaves every default in place", () => {
    expect(limitsFromJson(undefined)).toEqual(defaultLimits());
    expect(limitsFromJson("")).toEqual(defaultLimits());
    expect(limitsFromJson("   ")).toEqual(defaultLimits());
  });

  it("applies the overrides it is given and nothing else", () => {
    const limits = limitsFromJson('{"outboundCalls": 45, "maxDepth": 8}');
    expect(limits.outboundCalls).toBe(45);
    expect(limits.maxDepth).toBe(8);
    expect(limits.wallClockServiceMs).toBe(defaultLimits().wallClockServiceMs);
    expect(limits.tenantConcurrency).toBe(defaultLimits().tenantConcurrency);
  });

  it("takes every field of the table", () => {
    const all = {
      wallClockServiceMs: 10_000,
      wallClockPipelineMs: 20_000,
      materializedBodyBytes: 1024,
      tenantConcurrency: 4,
      outboundCalls: 2,
      maxDepth: 3,
      breakerThreshold: 5,
      breakerWindowMs: 6_000,
      breakerCooldownMs: 7_000,
    };
    expect(limitsFromJson(JSON.stringify(all))).toMatchObject(all);
  });

  it("ignores memoryBytes — the platform fixes it here", () => {
    const [limits, warnings] = withWarnings(() => limitsFromJson('{"memoryBytes": 999}'));
    expect(limits.memoryBytes).toBe(defaultLimits().memoryBytes);
    expect(warnings.join(" ")).toContain("memoryBytes");
  });

  it("a bad var never takes the Worker down: warn and keep the default", () => {
    for (const raw of ["not json", "[1,2]", '"a string"', "null"]) {
      const [limits, warnings] = withWarnings(() => limitsFromJson(raw));
      expect(limits, raw).toEqual(defaultLimits());
      expect(warnings.length, raw).toBeGreaterThan(0);
    }
  });

  it("rejects values that are not positive numbers, and unknown keys", () => {
    const [limits, warnings] = withWarnings(() =>
      limitsFromJson('{"outboundCalls": 0, "maxDepth": -1, "tenantConcurrency": "8", "outboundCallz": 45}'),
    );
    expect(limits).toEqual(defaultLimits());
    expect(warnings.join(" ")).toContain("outboundCallz");
    expect(warnings.filter((w) => w.includes("> 0")).length).toBe(3);
  });

  it("floors a fractional value rather than carrying it into a comparison", () => {
    expect(limitsFromJson('{"outboundCalls": 45.9}').outboundCalls).toBe(45);
  });
});
