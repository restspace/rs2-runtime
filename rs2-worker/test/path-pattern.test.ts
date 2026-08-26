// Port of the `#[cfg(test)]` module in `rs2-core/src/path_pattern.rs`.
import { describe, expect, it } from "vitest";
import { EMPTY_URL, resolve, validate } from "../src/runtime/path-pattern";
import type { UrlView } from "../src/runtime/path-pattern";

function url(path: string[], base: string[], query: string): UrlView {
  return { path, base, name: path[path.length - 1], query, rest: "" };
}

function r(pattern: string, u: UrlView): string {
  return resolve(pattern, u, {});
}

describe("path-pattern", () => {
  it("positional segments from start and end", () => {
    const u = url(["xyz", "qqq", "abc"], [], "");
    expect(r("a/${url.path[0]}/b", u)).toBe("a/xyz/b");
    expect(r("a/${url.path[1]}/b", u)).toBe("a/qqq/b");
    expect(r("a/${url.path[-1]}/b", u)).toBe("a/abc/b");
    expect(r("a/${url.path[-2]}/b", u)).toBe("a/qqq/b");
  });

  it("slices and whole section", () => {
    const u = url(["xyz", "qqq", "abc"], ["files"], "");
    expect(r("${url.path[1:]}", u)).toBe("qqq/abc");
    expect(r("${url.path[0:2]}", u)).toBe("xyz/qqq");
    expect(r("${url.path[:2]}", u)).toBe("xyz/qqq");
    expect(r("${url.path}", u)).toBe("xyz/qqq/abc");
    expect(r("${url.full}", u)).toBe("files/xyz/qqq/abc");
    expect(r("${url.base[0]}", u)).toBe("files");
    expect(r("${url.name}", u)).toBe("abc");
  });

  it("passthrough forwards the key", () => {
    const u = url(["ada@example.com"], [], "");
    expect(r("/data/users/${url.path[0]}", u)).toBe("/data/users/ada@example.com");
  });

  it("rest forwards verbatim remainder", () => {
    const rest = (s: string): UrlView => ({ path: [], base: [], name: undefined, query: "", rest: s });
    expect(r("/wrapped${url.rest}", rest("/"))).toBe("/wrapped/");
    expect(r("/wrapped${url.rest}", rest("/x"))).toBe("/wrapped/x");
    expect(r("/wrapped${url.rest}", rest("/a/b"))).toBe("/wrapped/a/b");
    expect(r("/wrapped${url.rest}", rest("/a/b/"))).toBe("/wrapped/a/b/");
    const u: UrlView = { path: [], base: [], name: undefined, query: "p=1", rest: "/a" };
    expect(r("/wrapped${url.rest}?${url.query}", u)).toBe("/wrapped/a?p=1");
  });

  it("rest takes no index", () => {
    expect(() => validate("${url.rest}")).not.toThrow();
    expect(() => validate("${url.rest[0]}")).toThrow();
    expect(() => validate("${url.rest.x}")).toThrow();
  });

  it("query selectors", () => {
    const u = url([], [], "page=3&q=a+b");
    expect(r("p/${url.query.page}", u)).toBe("p/3");
    expect(r("p/${url.query.q}", u)).toBe("p/a b");
    expect(r("p?${url.query}", u)).toBe("p?page=3&q=a+b");
  });

  it("optional elides and collapses slash", () => {
    const u = url(["only"], [], "");
    expect(r("a/${url.path[5]?}/b", u)).toBe("a/b");
    expect(r("a/${url.path[5]?}", u)).toBe("a");
  });

  it("defaults fill when absent", () => {
    expect(r("p/${url.query.page || '1'}", url([], [], ""))).toBe("p/1");
    expect(r("${url.path[0] || 'fallback'}", url(["x"], [], ""))).toBe("x");
  });

  it("required missing is an error", () => {
    expect(() => r("/x/${url.path[0]}", url([], [], ""))).toThrow();
  });

  it("data plane unchanged", () => {
    const data = { id: "o1", order: { customerId: 7 } };
    expect(resolve("/orders/${id}", EMPTY_URL, data)).toBe("/orders/o1");
    expect(resolve("/c/${order.customerId}", EMPTY_URL, data)).toBe("/c/7");
    expect(() => resolve("/x/${missing}", EMPTY_URL, data)).toThrow();
    expect(() => resolve("/x/${unclosed", EMPTY_URL, data)).toThrow();
  });

  it("validate rejects malformed patterns", () => {
    expect(() => validate("/data/users/${url.path[0]}")).not.toThrow();
    expect(() => validate("/data/${url.query.id || 'x'}")).not.toThrow();
    expect(() => validate("${url.path[}")).toThrow();
    expect(() => validate("${url.bogus}")).toThrow();
    expect(() => validate("${url.path[abc]}")).toThrow();
    expect(() => validate("${unterminated")).toThrow();
  });
});
