// Port of the `#[cfg(test)]` module in `rs2-core/src/listing.rs`.
import { describe, expect, it } from "vitest";
import type { DirEntry } from "../src/capabilities/types";
import {
  FieldPath,
  MetaSort,
  compare,
  compareOptional,
  compareRecords,
  parseJsonPreservingBigInts,
  parseListSpec,
  project,
  sortPageProject,
} from "../src/runtime/listing";
import type { Dir } from "../src/runtime/listing";

const path = (s: string) => FieldPath.parse(s);

describe("listing", () => {
  it("field path rejects malformed", () => {
    expect(() => FieldPath.parse("")).toThrow();
    expect(() => FieldPath.parse("a..b")).toThrow();
    expect(() => FieldPath.parse(".a")).toThrow();
    expect(() => FieldPath.parse("a.")).toThrow();
    expect(path("meta.date").segments).toEqual(["meta", "date"]);
  });

  it("strings compare by code point not dictionary", () => {
    expect(compare("Zebra", "apple")).toBeLessThan(0);
    expect(compare("é", "z")).toBeGreaterThan(0);
    expect(compare("é", "é")).not.toBe(0);
  });

  it("cross-type order is total and pinned", () => {
    const ladder = [null, false, true, -1.5, 0, 9, "", "a", [1], { a: 1 }];
    for (let i = 0; i + 1 < ladder.length; i++) {
      expect(compare(ladder[i]!, ladder[i + 1]!)).not.toBeGreaterThan(0);
    }
    expect(compareOptional(undefined, null)).toBeLessThan(0);
  });

  it("numbers keep integer precision beyond f64", () => {
    const a = parseJsonPreservingBigInts("9007199254740993"); // 2^53 + 1
    const b = parseJsonPreservingBigInts("9007199254740992"); // 2^53
    expect(compare(a, b)).toBeGreaterThan(0);
    expect(compare(1, 1.5)).toBeLessThan(0);
  });

  it("multi-key sort with direction and missing", () => {
    const sort: Array<[FieldPath, Dir]> = [
      [path("group"), "asc"],
      [path("n"), "desc"],
    ];
    const a = { group: "a", n: 1 };
    const b = { group: "a", n: 2 };
    const c = { group: "b", n: 9 };
    const d = { group: "a" };
    expect(compareRecords(b, a, sort)).toBeLessThan(0);
    expect(compareRecords(a, c, sort)).toBeLessThan(0);
    expect(compareRecords(a, d, sort)).toBeLessThan(0);
  });

  it("projection reconstructs nested shape and skips absent", () => {
    const rec = { title: "T", meta: { date: "2026-01-01", x: 1 } };
    expect(project(rec, [path("title"), path("meta.date"), path("nope.deep")])).toEqual({
      title: "T",
      meta: { date: "2026-01-01" },
    });
  });

  it("spec parses wire form", () => {
    const spec = parseListSpec("title,meta.date", "-meta.date,title", 25, 50);
    expect(spec.fields.length).toBe(2);
    expect(spec.sort[0]![0].dotted()).toBe("meta.date");
    expect(spec.sort[0]![1]).toBe("desc");
    expect(spec.sort[1]![0].dotted()).toBe("title");
    expect(spec.sort[1]![1]).toBe("asc");
    expect(() => parseListSpec("", undefined, 10, 0)).toThrow();
    expect(() => parseListSpec("a", "-", 10, 0)).toThrow();
  });

  it("meta sort orders entries with name tiebreak", () => {
    const entry = (name: string, size: number, dir: boolean, ct?: string, lm?: string): DirEntry => {
      const e: DirEntry = { name, size, dir };
      if (ct !== undefined) e.contentType = ct;
      if (lm !== undefined) e.lastModified = lm;
      return e;
    };
    const entries = [
      entry("b.txt", 10, false, "text/plain", "2026-07-02T00:00:00Z"),
      entry("a.json", 300, false, "application/json", "2026-07-01T00:00:00Z"),
      entry("sub/", 0, true, undefined, "2026-07-03T00:00:00Z"),
      entry("c.txt", 10, false, "text/plain", undefined),
    ];
    const names = (es: DirEntry[]) => es.map((e) => e.name);

    MetaSort.parse("-@size,@name").sort(entries);
    expect(names(entries)).toEqual(["a.json", "b.txt", "c.txt", "sub/"]);

    MetaSort.parse("@lastModified").sort(entries);
    expect(names(entries)).toEqual(["c.txt", "a.json", "b.txt", "sub/"]);

    MetaSort.parse("@contentType,@name").sort(entries);
    expect(names(entries)).toEqual(["sub/", "a.json", "b.txt", "c.txt"]);

    expect(() => MetaSort.parse("@nope")).toThrow();
    expect(() => MetaSort.parse("name")).toThrow();
    expect(() => MetaSort.parse("")).toThrow();
  });

  it("sort page project is deterministic with key tiebreak", () => {
    const recs: Array<[string, { n: number; t: string }]> = [
      ["k2", { n: 1, t: "b" }],
      ["k1", { n: 1, t: "a" }],
      ["k3", { n: 0, t: "c" }],
    ];
    const spec = parseListSpec("t", "n", 10, 0);
    const [page, total] = sortPageProject(recs, spec);
    expect(total).toBe(3);
    expect(page.map(([k]) => k)).toEqual(["k3", "k1", "k2"]);
    expect(page[0]![1]).toEqual({ t: "c" });
  });
});
