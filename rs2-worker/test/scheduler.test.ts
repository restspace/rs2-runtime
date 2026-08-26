/// <reference types="@cloudflare/vitest-pool-workers/types" />
// Port of the `#[cfg(test)]` module in `rs2-core/src/scheduler.rs`, plus the
// Worker-only alarm math (next due, due occurrence, `schedule_claims`). The
// claim tests run against the real DO SQLite via `runInDurableObject`.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { Env as WorkerEnv } from "../src/env";
import { RsError } from "../src/runtime/error";
import type { JsonObject } from "../src/runtime/error";
import {
  claimOccurrence,
  claimTtlMs,
  dueOccurrenceMs,
  earliestNextDueMs,
  intervalBucketMs,
  nextDueMs,
  parseCron,
  parseEvery,
  scheduledMounts,
  tickMessage,
} from "../src/runtime/scheduler";
import type { Schedule } from "../src/runtime/scheduler";

declare global {
  namespace Cloudflare {
    interface Env extends WorkerEnv {}
  }
}

/// ms epoch of a UTC calendar minute.
function ms(y: number, mo: number, d: number, h: number, mi: number): number {
  return Date.UTC(y, mo - 1, d, h, mi);
}

describe("scheduler parsing", () => {
  it("parses every, rejects zero/unknown/empty", () => {
    expect(parseEvery("500ms")).toBe(500);
    expect(parseEvery("30s")).toBe(30_000);
    expect(parseEvery("5m")).toBe(300_000);
    expect(parseEvery("2h")).toBe(7_200_000);
    for (const bad of ["", "0s", "10x", "s", "60", "-5s", " "]) {
      expect(() => parseEvery(bad), `should reject ${JSON.stringify(bad)}`).toThrow(RsError);
    }
  });

  it("parses cron, rejects out-of-range fields", () => {
    for (const ok of ["* * * * *", "0 9 * * *", "0,30 * * * *", "0-15 * * * *", "*/5 * * * *", "0-30/10 * * * *", "0 0 * * 7"]) {
      expect(() => parseCron(ok), `should accept ${JSON.stringify(ok)}`).not.toThrow();
    }
    for (const bad of ["* * * *", "60 * * * *", "* 24 * * *", "0 0 0 * *", "0 0 * 13 *", "a * * * *", "*/0 * * * *"]) {
      expect(() => parseCron(bad), `should reject ${JSON.stringify(bad)}`).toThrow(RsError);
    }
  });
});

describe("cron occurrence math", () => {
  it("next occurrence is strictly after, daily", () => {
    const c = parseCron("0 9 * * *");
    expect(c.nextOccurrenceAfter(new Date(ms(2026, 6, 16, 8, 59)))?.getTime()).toBe(ms(2026, 6, 16, 9, 0));
    expect(c.nextOccurrenceAfter(new Date(ms(2026, 6, 16, 9, 0)))?.getTime()).toBe(ms(2026, 6, 17, 9, 0));
  });

  it("rolls over the year", () => {
    const c = parseCron("0 0 1 1 *");
    expect(c.nextOccurrenceAfter(new Date(ms(2026, 12, 31, 23, 59)))?.getTime()).toBe(ms(2027, 1, 1, 0, 0));
  });

  it("dom/dow OR semantics", () => {
    // 2026-11-13 is a Friday; 2026-11-06 is also a Friday.
    const both = parseCron("0 0 13 * 5");
    expect(both.matches(new Date(ms(2026, 11, 13, 0, 0)))).toBe(true);
    expect(both.matches(new Date(ms(2026, 11, 6, 0, 0)))).toBe(true);
    const domOnly = parseCron("0 0 13 * *");
    expect(domOnly.matches(new Date(ms(2026, 11, 13, 0, 0)))).toBe(true);
    expect(domOnly.matches(new Date(ms(2026, 11, 6, 0, 0)))).toBe(false);
    const dowOnly = parseCron("0 0 * * 5");
    expect(dowOnly.matches(new Date(ms(2026, 11, 6, 0, 0)))).toBe(true);
    expect(dowOnly.matches(new Date(ms(2026, 11, 5, 0, 0)))).toBe(false);
  });

  it("interval bucket is epoch-aligned", () => {
    const a = intervalBucketMs(1_000_000_030_000, 60_000);
    const b = intervalBucketMs(1_000_000_059_000, 60_000);
    const c = intervalBucketMs(1_000_000_081_000, 60_000);
    expect(a).toBe(b);
    expect(b).not.toBe(c);
    expect(a % 60_000).toBe(0);
  });

  it("tick message is an internal POST with the trigger header", () => {
    const msg = tickMessage("t1", "/job");
    expect(msg.method).toBe("POST");
    expect(msg.source).toBe("system");
    expect(msg.principal).toBeUndefined();
    expect(msg.header("x-rs2-trigger")).toBe("schedule");
    expect(msg.url.path).toBe("/job");
    expect(tickMessage("t1", "").url.path).toBe("/");
  });
});

describe("alarm arming math", () => {
  const every2s: Schedule = { kind: "interval", everyMs: 2000 };
  const daily9: Schedule = { kind: "cron", cron: parseCron("0 9 * * *") };

  it("derives scheduled mounts from a raw config, skipping invalid schedules", () => {
    const config: JsonObject = {
      mounts: [
        { path: "/job", service: "pipeline", config: { schedule: { every: "2s" } } },
        { path: "/daily", service: "pipeline", config: { schedule: { cron: "0 9 * * *" } } },
        { path: "/bad", service: "pipeline", config: { schedule: { every: "0s" } } },
        { path: "/plain", service: "file", config: {} },
        { path: "/none", service: "file" },
      ],
    };
    const mounts = scheduledMounts(config);
    expect(mounts.map((m) => m.base)).toEqual(["/job", "/daily"]);
    expect(mounts[0]!.schedule).toEqual(every2s);
    expect(scheduledMounts({})).toEqual([]);
  });

  it("next due: intervals at the next epoch bucket boundary, crons at the next minute", () => {
    expect(nextDueMs(every2s, 10_500)).toBe(12_000);
    expect(nextDueMs(every2s, 12_000)).toBe(14_000); // strictly after
    expect(nextDueMs(daily9, ms(2026, 6, 16, 8, 59))).toBe(ms(2026, 6, 16, 9, 0));
  });

  it("earliest next due picks the soonest mount", () => {
    const now = ms(2026, 6, 16, 8, 59) + 500;
    const mounts = [
      { base: "/daily", schedule: daily9 },
      { base: "/job", schedule: every2s },
    ];
    expect(earliestNextDueMs(mounts, now)).toBe(nextDueMs(every2s, now));
    expect(earliestNextDueMs([], now)).toBeUndefined();
  });

  it("due occurrence: interval = current bucket; cron = most recent match within grace", () => {
    expect(dueOccurrenceMs(every2s, 12_001)).toBe(12_000);
    // Alarm fires exactly at, or slightly after, 09:00.
    expect(dueOccurrenceMs(daily9, ms(2026, 6, 16, 9, 0))).toBe(ms(2026, 6, 16, 9, 0));
    expect(dueOccurrenceMs(daily9, ms(2026, 6, 16, 9, 2) + 30_000)).toBe(ms(2026, 6, 16, 9, 0));
    // Outside the grace window nothing is due.
    expect(dueOccurrenceMs(daily9, ms(2026, 6, 16, 9, 30))).toBeUndefined();
  });

  it("claim ttl: two periods floored at 60s for intervals, 120s for crons", () => {
    expect(claimTtlMs(every2s)).toBe(60_000);
    expect(claimTtlMs({ kind: "interval", everyMs: 3_600_000 })).toBe(7_200_000);
    expect(claimTtlMs(daily9)).toBe(120_000);
  });
});

describe("schedule_claims (real DO SQLite)", () => {
  it("claims once per occurrence, expires by TTL", async () => {
    const stub = env.TENANTS.get(env.TENANTS.idFromName("sched-claims-test"));
    await runInDurableObject(stub, async (_instance, state) => {
      const sql = state.storage.sql;
      const ttl = 60_000;
      expect(claimOccurrence(sql, "t|/m", 1000, ttl, 0)).toBe(true); // first wins
      expect(claimOccurrence(sql, "t|/m", 1000, ttl, 1)).toBe(false); // repeat loses
      expect(claimOccurrence(sql, "t|/m", 2000, ttl, 2)).toBe(true); // new occurrence wins
      expect(claimOccurrence(sql, "t|/other", 1000, ttl, 3)).toBe(true); // different key wins
      // After the TTL the swept claim can be re-won (a very late retry — the
      // bounded keyspace is the point, not replay protection past the TTL).
      expect(claimOccurrence(sql, "t|/m", 1000, ttl, 70_000)).toBe(true);
    });
  });
});
