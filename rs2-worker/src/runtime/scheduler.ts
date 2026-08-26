// Host-driven scheduler (G1): schedule parsing, cron next-occurrence math,
// the interval occurrence bucket, and the tick message. Port of
// `rs2-core/src/scheduler.rs`; alarms wire it up in `tenant-object.ts`.

import { RsError } from "./error";
import type { Json, JsonObject } from "./error";
import { Message } from "./message";

/// The synthetic request the scheduler fires at a mount.
export function tickMessage(tenant: string, basePath: string): Message {
  const msg = Message.request("POST", basePath === "" ? "/" : basePath, tenant);
  msg.source = "system";
  msg.setHeader("x-rs2-trigger", "schedule");
  return msg;
}

export type Schedule = { kind: "interval"; everyMs: number } | { kind: "cron"; cron: CronSchedule };

/// Parse `config.schedule`. Exactly one of `every` / `cron` is required.
export function scheduleFromConfig(v: Json): Schedule {
  const obj = v && typeof v === "object" && !Array.isArray(v) ? v : {};
  const every = typeof obj.every === "string" ? obj.every : undefined;
  const cron = typeof obj.cron === "string" ? obj.cron : undefined;
  if (every !== undefined && cron !== undefined) {
    throw RsError.badRequest("schedule has both 'every' and 'cron' — set exactly one");
  }
  if (every !== undefined) return { kind: "interval", everyMs: parseEvery(every) };
  if (cron !== undefined) return { kind: "cron", cron: parseCron(cron) };
  throw RsError.badRequest('schedule requires \'every\' (e.g. "60s") or \'cron\' (e.g. "0 9 * * *")');
}

/// `"500ms" | "30s" | "5m" | "2h"` → milliseconds. Rejects 0, empty, unknown units.
export function parseEvery(sIn: string): number {
  const bad = () => RsError.badRequest(`invalid interval '${sIn}' (use e.g. 500ms, 30s, 5m, 2h)`);
  const s = sIn.trim();
  const m = /^(\d+)(ms|s|m|h)$/.exec(s);
  if (!m) throw bad();
  const n = Number(m[1]);
  if (!Number.isSafeInteger(n) || n === 0) throw bad();
  const mult = { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[m[2] as "ms" | "s" | "m" | "h"];
  const out = n * mult;
  if (!Number.isSafeInteger(out)) throw bad();
  return out;
}

/// The epoch-aligned occurrence bucket for an interval at `nowMs`.
export function intervalBucketMs(nowMs: number, everyMs: number): number {
  const period = Math.max(everyMs, 1);
  return Math.floor(nowMs / period) * period;
}

type CronField = { any: true } | { any: false; set: number[] };

function fieldMatches(f: CronField, v: number): boolean {
  return f.any || f.set.includes(v);
}

function parseField(spec: string, min: number, max: number): CronField {
  const bad = () => RsError.badRequest(`invalid cron field '${spec}'`);
  if (spec === "*") return { any: true };
  const set = new Set<number>();
  for (const part of spec.split(",")) {
    const slash = part.indexOf("/");
    const base = slash >= 0 ? part.slice(0, slash) : part;
    const stepStr = slash >= 0 ? part.slice(slash + 1) : "1";
    if (!/^\d+$/.test(stepStr)) throw bad();
    const step = Number(stepStr);
    if (step === 0 || step > 255) throw bad();
    let lo: number;
    let hi: number;
    if (base === "*") {
      lo = min;
      hi = max;
    } else if (base.includes("-")) {
      const [a, b] = base.split("-");
      if (!/^\d+$/.test(a ?? "") || !/^\d+$/.test(b ?? "")) throw bad();
      lo = Number(a);
      hi = Number(b);
    } else {
      if (!/^\d+$/.test(base)) throw bad();
      lo = hi = Number(base);
    }
    if (lo > 255 || hi > 255) throw bad();
    if (lo < min || hi > max || lo > hi) throw bad();
    for (let x = lo; x <= hi; x += step) set.add(x);
  }
  return { any: false, set: [...set].sort((a, b) => a - b) };
}

function parseDow(spec: string): CronField {
  const f = parseField(spec, 0, 7);
  if (f.any) return f;
  return { any: false, set: [...new Set(f.set.map((x) => (x === 7 ? 0 : x)))].sort((a, b) => a - b) };
}

/// A parsed 5-field cron expression, evaluated in UTC.
export class CronSchedule {
  constructor(
    private readonly min: CronField,
    private readonly hour: CronField,
    private readonly dom: CronField,
    private readonly month: CronField,
    private readonly dow: CronField,
  ) {}

  matches(dt: Date): boolean {
    return (
      fieldMatches(this.min, dt.getUTCMinutes()) &&
      fieldMatches(this.hour, dt.getUTCHours()) &&
      fieldMatches(this.month, dt.getUTCMonth() + 1) &&
      this.dayMatches(dt)
    );
  }

  private dayMatches(dt: Date): boolean {
    const domV = dt.getUTCDate();
    const dowV = dt.getUTCDay();
    if (this.dom.any && this.dow.any) return true;
    if (this.dow.any) return fieldMatches(this.dom, domV);
    if (this.dom.any) return fieldMatches(this.dow, dowV);
    return fieldMatches(this.dom, domV) || fieldMatches(this.dow, dowV);
  }

  /// First UTC minute strictly after `after` that matches (366-day bound).
  nextOccurrenceAfter(after: Date): Date | undefined {
    let t = Math.floor(after.getTime() / 60_000) * 60_000 + 60_000;
    const bound = after.getTime() + 366 * 24 * 3600 * 1000;
    while (t <= bound) {
      const d = new Date(t);
      if (this.matches(d)) return d;
      t += 60_000;
    }
    return undefined;
  }
}

/// `"min hour day-of-month month day-of-week"` (5 fields, UTC).
export function parseCron(s: string): CronSchedule {
  const f = s.split(/\s+/).filter((x) => x !== "");
  if (f.length !== 5) {
    throw RsError.badRequest(`cron '${s}' must have 5 fields: minute hour day-of-month month day-of-week`);
  }
  return new CronSchedule(
    parseField(f[0]!, 0, 59),
    parseField(f[1]!, 0, 23),
    parseField(f[2]!, 1, 31),
    parseField(f[3]!, 1, 12),
    parseDow(f[4]!),
  );
}

// ---- alarm arming (§B.6) -------------------------------------------------
// The Rust scheduler polls; the Worker arms one DO alarm at the earliest due
// time and, when it fires, derives the due occurrence statelessly — the
// `schedule_claims` table is the only persisted cursor, so a retried or
// duplicated alarm never double-fires an occurrence.

/// One scheduled mount derived from the raw config.
export interface ScheduledMount {
  base: string;
  schedule: Schedule;
}

/// Derive the scheduled mounts from a raw (unparsed) tenant config. Invalid
/// schedules are skipped — they were already rejected at `PUT /raw`; this is
/// the same defensive stance as `reconcile_schedules` in `runtime.rs`.
export function scheduledMounts(config: JsonObject): ScheduledMount[] {
  const out: ScheduledMount[] = [];
  const mounts = Array.isArray(config.mounts) ? config.mounts : [];
  for (const mount of mounts) {
    if (!mount || typeof mount !== "object" || Array.isArray(mount)) continue;
    const cfg = mount.config;
    if (!cfg || typeof cfg !== "object" || Array.isArray(cfg)) continue;
    const sched = cfg.schedule;
    if (sched === undefined) continue;
    const base = typeof mount.path === "string" ? mount.path : "";
    try {
      out.push({ base, schedule: scheduleFromConfig(sched) });
    } catch {
      /* rejected at PUT /raw; defensive */
    }
  }
  return out;
}

/// The next time (ms epoch) this schedule wants the alarm to fire, strictly
/// after `nowMs`. Intervals fire at epoch-aligned bucket boundaries (the
/// occurrence id doubles as the fire time); crons at the next matching UTC
/// minute. `undefined` when a cron matches nothing within 366 days.
export function nextDueMs(s: Schedule, nowMs: number): number | undefined {
  if (s.kind === "interval") return (Math.floor(nowMs / s.everyMs) + 1) * s.everyMs;
  return s.cron.nextOccurrenceAfter(new Date(nowMs))?.getTime();
}

/// The earliest `nextDueMs` across a mount set — what the alarm is armed to.
export function earliestNextDueMs(mounts: ScheduledMount[], nowMs: number): number | undefined {
  let earliest: number | undefined;
  for (const m of mounts) {
    const due = nextDueMs(m.schedule, nowMs);
    if (due !== undefined && (earliest === undefined || due < earliest)) earliest = due;
  }
  return earliest;
}

/// How far back a firing alarm looks for the cron occurrence it was armed
/// for (alarms deliver at-or-after their set time; retries can be late).
export const CRON_GRACE_MS = 5 * 60_000;

/// The occurrence (ms epoch) due when the alarm fires at `nowMs`, or
/// `undefined` when nothing is due. Intervals: the current epoch-aligned
/// bucket (the claim makes it fire-once). Crons: the most recent matching
/// minute within the grace window — stateless, so a DO evicted and woken by
/// its own alarm still fires the occurrence the alarm was armed for.
export function dueOccurrenceMs(s: Schedule, nowMs: number, graceMs: number = CRON_GRACE_MS): number | undefined {
  if (s.kind === "interval") return intervalBucketMs(nowMs, s.everyMs);
  let t = Math.floor(nowMs / 60_000) * 60_000;
  const floor = nowMs - graceMs;
  while (t >= floor) {
    if (s.cron.matches(new Date(t))) return t;
    t -= 60_000;
  }
  return undefined;
}

/// How long a claim is remembered — a couple of periods, floored at 60s
/// (`SchedEntry::claim_ttl` in `runtime.rs`).
export function claimTtlMs(s: Schedule): number {
  if (s.kind === "interval") return Math.max(s.everyMs * 2, 60_000);
  return 120_000;
}

/// The subset of `SqlStorage` the claim needs (unit-testable without a DO).
export interface ClaimSql {
  exec(query: string, ...bindings: (string | number)[]): { rowsWritten: number };
}

/// Atomically claim the right to fire `key`'s occurrence (`ScheduleStore::
/// claim` over the `schedule_claims` table, §C.3): `true` iff this caller
/// won. Expired claims are swept on the way in so the keyspace stays bounded.
export function claimOccurrence(sql: ClaimSql, key: string, occurrenceMs: number, ttlMs: number, nowMs: number): boolean {
  sql.exec("DELETE FROM schedule_claims WHERE expires_ms < ?", nowMs);
  const cur = sql.exec(
    "INSERT OR IGNORE INTO schedule_claims (key, occurrence_ms, expires_ms) VALUES (?, ?, ?)",
    key,
    occurrenceMs,
    nowMs + ttlMs,
  );
  // `rowsWritten` includes index writes (2 for a fresh insert); an ignored
  // duplicate writes nothing.
  return cur.rowsWritten > 0;
}
