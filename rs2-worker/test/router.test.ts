// Port of the `#[cfg(test)]` module in `rs2-core/src/router/mod.rs` (plus
// the Worker-only `RS2_DEFAULT_TENANT` fallback) and the `MsgUrl` tests
// from `message/message.rs`.
import { describe, expect, it } from "vitest";
import { MsgUrl } from "../src/runtime/message";
import { MountTable, resolveTenant, validatePath } from "../src/runtime/router";

describe("router", () => {
  it("resolves tenancy", () => {
    const single = { domainMap: new Map<string, string>(), mainDomain: undefined, defaultTenant: "main" };
    expect(resolveTenant(single, "anything.example:8080")).toBe("main");

    const multi = { domainMap: new Map([["api.acme.com", "acme"]]), mainDomain: "rs2.dev", defaultTenant: undefined };
    expect(resolveTenant(multi, "api.acme.com")).toBe("acme");
    expect(resolveTenant(multi, "beta.rs2.dev:443")).toBe("beta");
    expect(resolveTenant(multi, "a.b.rs2.dev")).toBeUndefined();
    expect(resolveTenant(multi, "other.com")).toBeUndefined();
    // The Worker-only default tenant catches unresolved hosts.
    expect(resolveTenant({ ...multi, defaultTenant: "conf" }, "other.com")).toBe("conf");
  });

  it("rejects unsafe paths", () => {
    expect(() => validatePath("/a/../b")).toThrow();
    expect(() => validatePath("/a/%2e%2e/b")).toThrow();
    expect(() => validatePath("/a/b%00.txt")).toThrow();
    expect(() => validatePath("/a\\b")).toThrow();
    expect(() => validatePath("/C:/windows")).toThrow();
    expect(() => validatePath("/a/b/c.txt")).not.toThrow();
    expect(() => validatePath("/a/file.with.dots.txt")).not.toThrow();
  });

  it("longest prefix wins on segment boundaries", () => {
    const table = new MountTable([
      { basePath: "/files", service: "file", config: {} },
      { basePath: "/files/special", service: "data", config: {} },
      { basePath: "/", service: "file", config: {} },
    ]);
    expect(table.route("/files/special/x")!.service).toBe("data");
    expect(table.route("/files/a.txt")!.service).toBe("file");
    expect(table.route("/filesystem")!.basePath).toBe("");
    expect(table.route("/files")!.basePath).toBe("/files");
  });

  it("rejects duplicate mounts", () => {
    expect(
      () =>
        new MountTable([
          { basePath: "/x", service: "file", config: {} },
          { basePath: "x/", service: "data", config: {} },
        ]),
    ).toThrow();
  });

  it("parses url and query", () => {
    const url = MsgUrl.parse("/data/orders/1?x=1&$take=5&q=a+b%21");
    expect(url.path).toBe("/data/orders/1");
    expect(url.queryParam("$take")).toBe("5");
    expect(url.queryParam("q")).toBe("a b!");
    expect(url.queryParam("missing")).toBeUndefined();
  });

  it("query param keys match percent encoded", () => {
    const url = MsgUrl.parse("/files/docs/?%24sort=-%40size&%24take=5");
    expect(url.queryParam("$sort")).toBe("-@size");
    expect(url.queryParam("$take")).toBe("5");
  });

  it("applies mount split", () => {
    const url = MsgUrl.parse("/files/docs/readme.md");
    url.applyMount("/files");
    expect(url.basePath).toBe("/files");
    expect(url.servicePath).toBe("/docs/readme.md");
    expect(url.serviceSegments()).toEqual(["docs", "readme.md"]);
  });

  it("mount root becomes directory path", () => {
    const url = MsgUrl.parse("/files");
    url.applyMount("/files");
    expect(url.servicePath).toBe("/");
    expect(url.isDirectory()).toBe(true);
  });
});
