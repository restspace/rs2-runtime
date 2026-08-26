// Port of the `#[cfg(test)]` module in `rs2-core/src/wrapper/mod.rs`.
import { describe, expect, it } from "vitest";
import { codes } from "../src/runtime/error";
import type { RsError } from "../src/runtime/error";
import { Message } from "../src/runtime/message";
import {
  CorsPolicy,
  TenantBreaker,
  TenantLimiter,
  checkAccess,
  defaultLimits,
  isSameOrigin,
  originMatches,
  satisfiesRoleSpec,
} from "../src/runtime/wrapper";

function errorOf(f: () => void): RsError {
  try {
    f();
  } catch (e) {
    return e as RsError;
  }
  throw new Error("expected a throw");
}

describe("wrapper", () => {
  it("concurrency admission fails fast at cap", () => {
    const limiter = new TenantLimiter();
    const p1 = limiter.admit("t1", 2);
    limiter.admit("t1", 2);
    expect(errorOf(() => limiter.admit("t1", 2)).code).toBe(codes.LIMIT_EXCEEDED);
    // Other tenants are unaffected.
    expect(() => limiter.admit("t2", 2)).not.toThrow();
    // Releasing a permit re-admits.
    p1();
    expect(() => limiter.admit("t1", 2)).not.toThrow();
  });

  it("origin patterns match origins", () => {
    expect(originMatches("*", "https://x.y")).toBe(true);
    expect(originMatches("https://app.acme.com", "https://app.acme.com")).toBe(true);
    expect(originMatches("https://app.acme.com", "http://app.acme.com")).toBe(false);
    expect(originMatches("app.acme.com", "https://app.acme.com:8443")).toBe(true);
    expect(originMatches("*.acme.dev", "https://x.acme.dev")).toBe(true);
    expect(originMatches("*.acme.dev", "https://acme.dev")).toBe(true);
    expect(originMatches("*.acme.dev", "https://evil-acme.dev")).toBe(false);
    expect(isSameOrigin("https://api.acme.com", "api.acme.com")).toBe(true);
    expect(isSameOrigin("https://api.acme.com:8443", "api.acme.com:8443")).toBe(true);
    expect(isSameOrigin("https://other.com", "api.acme.com")).toBe(false);
    expect(isSameOrigin("https://api.acme.com", undefined)).toBe(false);
  });

  it("csrf guard blocks untrusted cookie writes only", () => {
    const policy = new CorsPolicy();
    policy.trustedOrigins = ["https://app.acme.com"];
    const cookiePost = (originOk: boolean) => {
      const msg = Message.request("POST", "/data/x", "t");
      msg.setHeader("cookie", "rs-auth=tok");
      return () => policy.checkCookieCsrf(msg, originOk ? "https://app.acme.com" : "https://evil.com", "api.acme.com");
    };
    expect(cookiePost(true)).not.toThrow();
    expect(cookiePost(false)).toThrow();

    const read = Message.request("GET", "/data/x", "t");
    read.setHeader("cookie", "rs-auth=tok");
    expect(() => policy.checkCookieCsrf(read, "https://evil.com", "api.acme.com")).not.toThrow();
    const bearer = Message.request("POST", "/data/x", "t");
    bearer.setHeader("authorization", "Bearer tok");
    expect(() => policy.checkCookieCsrf(bearer, "https://evil.com", "api.acme.com")).not.toThrow();
    const same = Message.request("POST", "/data/x", "t");
    same.setHeader("cookie", "rs-auth=tok");
    expect(() => policy.checkCookieCsrf(same, "https://api.acme.com", "api.acme.com")).not.toThrow();
  });

  it("path scoped grants resolve extra claims", () => {
    const user = (account: string | undefined, path: string) => {
      const msg = Message.request("GET", path, "t1");
      msg.url.applyMount("");
      msg.principal = {
        id: "u1@x.com",
        roles: ["U"],
        kind: "user",
        extra: account !== undefined ? { accountId: account } : {},
      };
      return msg;
    };
    const spec = "authenticated /{accountId} A";
    expect(satisfiesRoleSpec(spec, user("acc1", "/acc1/photo.jpg"))).toBe(true);
    expect(satisfiesRoleSpec(spec, user("acc1", "/acc2/photo.jpg"))).toBe(false);
    expect(satisfiesRoleSpec(spec, user(undefined, "/acc1/photo.jpg"))).toBe(false);
    expect(satisfiesRoleSpec("U /{email}", user(undefined, "/u1@x.com/inbox"))).toBe(true);
  });

  it("breaker trips at threshold and recovers", async () => {
    const limits = { ...defaultLimits(), breakerThreshold: 3, breakerWindowMs: 10_000, breakerCooldownMs: 20 };
    const breaker = new TenantBreaker();
    expect(() => breaker.check("t1")).not.toThrow();
    breaker.recordBreach("t1", limits);
    breaker.recordBreach("t1", limits);
    expect(() => breaker.check("t1")).not.toThrow();
    breaker.recordBreach("t1", limits);
    const err = errorOf(() => breaker.check("t1"));
    expect(err.code).toBe(codes.LIMIT_EXCEEDED);
    expect(err.retryAfterMs).toBeDefined();
    expect(() => breaker.check("t2")).not.toThrow();
    await new Promise((r) => setTimeout(r, 30));
    expect(() => breaker.check("t1")).not.toThrow();
  });

  it("access stub requires principal", () => {
    const mount = (config: Record<string, never> | { access: string }) => ({ basePath: "", service: "file", config });
    const msg = Message.request("GET", "/x", "t1");
    expect(errorOf(() => checkAccess(msg, mount({}))).status).toBe(401);
    expect(() => checkAccess(msg, mount({ access: "open" }))).not.toThrow();
    expect(() => checkAccess(msg, mount({ access: "authenticated" }))).toThrow();
    const authed = Message.request("GET", "/x", "t1");
    authed.principal = { id: "u1", roles: ["U"], kind: "user", extra: {} };
    expect(() => checkAccess(authed, mount({ access: "authenticated" }))).not.toThrow();
  });
});
