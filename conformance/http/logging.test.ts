// Replaces the runtime half of `rs2-core/tests/logging.rs`
// (`boundary_logs_severity_otlp_and_reader`, `service_log_correlates_with_boundary`,
// `info_floor_suppresses_debug_internal`) plus the wire contract of
// `services/log_reader.rs` (spec §F.3 row `logging.test.ts`, §D row
// `services/log-reader.ts`).
//
// The direct `FileLogStore` cases in that file (round-trip/filters, tenant
// isolation, trace + time range, rotation) and the sandbox `console.log` case
// drive the adapter in process, below the HTTP surface, so they have no
// black-box equivalent here.
//
// Timing: `LogSink::emit` hands the record to a background writer, so every
// read is a poll with a deadline rather than a single GET.

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, type Rs2Client, type Rs2Response } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

/** One OTLP-shaped record as the reader emits it. */
interface Rec {
  timeUnixNano: string;
  severityNumber: number;
  severityText: string;
  body: string;
  traceId: string;
  spanId: string;
  attributes: Record<string, unknown>;
}

const LOGS = "/logs";
const DIR = "logsuite";

let seed: Seed | undefined;
let admin: Rs2Client;

/** Trace ids captured from the traffic the suite generates in `beforeAll`. */
const traces: Record<string, string> = {};
/** The unique identity whose failed login must produce the `auth` service WARN. */
const GHOST = `ghost-${Date.now()}@conf.test`;

/** `x-trace-id` off a response (the host stamps every reply with it). */
function traceOf(res: Rs2Response): string {
  const t = res.header("x-trace-id");
  expect(t, `no x-trace-id on ${res.describe()}`).toBeTruthy();
  return t as string;
}

async function fetchLogs(query: string): Promise<{ recs: Rec[]; res: Rs2Response }> {
  const res = await admin.get(`${LOGS}${query}`);
  expect(res.status, res.describe()).toBe(200);
  expect(res.contentType(), `[log] JSON by default: ${res.describe()}`).toBe("application/json");
  return { recs: res.json<Rec[]>(), res };
}

/**
 * Poll the reader until `want` is satisfied. The file sink writes on a
 * background task, so a record emitted during the request that just returned
 * may not be readable for a few milliseconds.
 */
async function until(
  query: string,
  want: (recs: Rec[]) => boolean,
  what: string,
  timeoutMs = 10_000,
): Promise<Rec[]> {
  const deadline = Date.now() + timeoutMs;
  let last: Rec[] = [];
  for (;;) {
    last = (await fetchLogs(query)).recs;
    if (want(last)) return last;
    if (Date.now() >= deadline) {
      throw new Error(
        `timed out waiting for ${what} in GET ${LOGS}${query} (${last.length} records, newest: ${JSON.stringify(last[0] ?? null).slice(0, 300)})`,
      );
    }
    await new Promise((r) => setTimeout(r, 100));
  }
}

const attr = (r: Rec, k: string) => r.attributes[k];
const byPath = (recs: Rec[], path: string, method?: string) =>
  recs.find(
    (r) => attr(r, "url.path") === path && (method === undefined || attr(r, "http.request.method") === method),
  );

beforeAll(async () => {
  seed = await Seed.create();
  await seed.applyMounts([
    { path: "/files", service: "file", config: { access: "open" } },
    { path: LOGS, service: "log", config: { access: { read: "A" } } },
  ]);
  admin = seed.admin;
  // Creating a principal runs the seed's hashing facade, whose `call` steps
  // are internal hops — the Debug-severity material the info floor drops.
  await seed.createPrincipals([{ email: `logs-${Date.now()}@conf.test`, password: "logs-pw", roles: "U" }]);
  seed.trackDir("/files", DIR);

  // The traffic every case reads back: the three dispatches of
  // `boundary_logs_…` plus the failed login of `service_log_correlates_…`.
  const put = await admin.put(`/files/${DIR}/a.txt`, { body: "hi", contentType: "text/plain" });
  expect([200, 201], put.describe()).toContain(put.status);
  traces.put = traceOf(put);

  const ok = await admin.get(`/files/${DIR}/a.txt`);
  expect(ok.status, ok.describe()).toBe(200);
  traces.ok = traceOf(ok);

  const missing = await admin.get(`/files/${DIR}/nope.txt`);
  expect(missing.status, missing.describe()).toBe(404);
  traces.missing = traceOf(missing);

  // One attempt only: `auth` locks an identity out after five.
  const login = await seed.anon.post("/auth/login", { json: { email: GHOST, password: "x" }, token: null });
  expect(login.status, login.describe()).toBe(401);
  traces.login = traceOf(login);
});

afterAll(async () => {
  await seed?.restore();
});

describe("boundary logs: OTLP shape, severity and the reader", () => {
  test("[log] every dispatch is one record in OTLP field names", async () => {
    const recs = await until(
      "?$take=200",
      (r) => byPath(r, `/files/${DIR}/nope.txt`) !== undefined && byPath(r, `/files/${DIR}/a.txt`, "GET") !== undefined,
      "the boundary logs for the seeded traffic",
    );
    expect(recs.length, "want >=3 boundary logs").toBeGreaterThanOrEqual(3);

    // OTLP field names / resource attribute present on the newest record.
    const first = recs[0];
    expect(typeof first.timeUnixNano, `timeUnixNano is a string: ${JSON.stringify(first)}`).toBe("string");
    expect(first.timeUnixNano, "timeUnixNano is decimal digits").toMatch(/^\d+$/);
    expect(typeof first.severityText, "severityText is a string").toBe("string");
    expect(typeof first.severityNumber, "severityNumber is a number").toBe("number");
    expect(typeof first.traceId, "traceId is a string").toBe("string");
    expect(typeof first.spanId, "spanId is a string").toBe("string");
    expect(attr(first, "rs2.tenant"), "rs2.tenant is the resource attribute").toBe(env().tenant);
  });

  test("[log] the 404 logs at WARN with error.type=not_found and a numeric status", async () => {
    const recs = await until(
      "?$take=200",
      (r) => byPath(r, `/files/${DIR}/nope.txt`) !== undefined,
      "the missing-file boundary log",
    );
    const warn = byPath(recs, `/files/${DIR}/nope.txt`)!;
    expect(warn.severityText).toBe("WARN");
    expect(warn.severityNumber, "WARN is OTLP severityNumber 13").toBe(13);
    expect(attr(warn, "error.type")).toBe("not_found");
    expect(attr(warn, "http.response.status_code"), "status is a JSON number").toBe(404);
    expect(typeof attr(warn, "http.response.status_code"), "…not a string").toBe("number");
    expect(attr(warn, "rs2.source"), "an external request").toBe("external");
    expect(attr(warn, "http.request.method")).toBe("GET");
    expect(warn.body, "body is `<METHOD> <path> -> <status>`").toBe(`GET /files/${DIR}/nope.txt -> 404`);
    expect(warn.traceId, "the record carries the request's x-trace-id").toBe(traces.missing);
  });

  test("[log] the successful GET logs at INFO", async () => {
    const recs = await until(
      "?$take=200",
      (r) => byPath(r, `/files/${DIR}/a.txt`, "GET") !== undefined,
      "the successful-GET boundary log",
    );
    const info = byPath(recs, `/files/${DIR}/a.txt`, "GET")!;
    expect(info.severityText).toBe("INFO");
    expect(info.severityNumber, "INFO is OTLP severityNumber 9").toBe(9);
    expect(attr(info, "http.response.status_code")).toBe(200);
    expect(attr(info, "rs2.source")).toBe("external");
    expect(info.traceId).toBe(traces.ok);
  });

  test("[log] /<traceId> returns exactly that trace's records", async () => {
    const tid = traces.missing;
    const recs = await until(`/${tid}`, (r) => r.length > 0, `records for trace ${tid}`);
    for (const r of recs) expect(r.traceId, `record outside trace ${tid}: ${JSON.stringify(r)}`).toBe(tid);
    expect(
      recs.some((r) => attr(r, "url.path") === `/files/${DIR}/nope.txt`),
      "the 404's own boundary record is in its trace",
    ).toBe(true);

    // `traceId=` overrides the positional segment (log_reader.rs).
    const overridden = await fetchLogs(`/${tid}?traceId=${traces.ok}`);
    expect(overridden.recs.length).toBeGreaterThan(0);
    for (const r of overridden.recs) expect(r.traceId).toBe(traces.ok);

    // An unknown trace is an empty page, not a 404.
    const none = await fetchLogs("/0000000000000000000000000000dead");
    expect(none.recs).toEqual([]);
    expect(none.res.totalCount()).toBe(0);
  });
});

describe("service logs", () => {
  test("[log] severity=warn surfaces the auth service's failed-login WARN", async () => {
    const recs = await until(
      "?$take=200&severity=warn",
      (r) => r.some((x) => attr(x, "rs2.source") === "service" && x.traceId === traces.login),
      "the auth service's failed-login log",
    );
    for (const r of recs) {
      expect(["WARN", "ERROR"], `severity=warn is a floor: ${JSON.stringify(r)}`).toContain(r.severityText);
    }
    const svc = recs.find((r) => attr(r, "rs2.source") === "service" && r.traceId === traces.login)!;
    expect(attr(svc, "rs2.service")).toBe("auth");
    expect(attr(svc, "rs2.mount"), "service logs carry their mount").toBe("/auth");
    expect(svc.severityText).toBe("WARN");
    expect(svc.body, `want a 'login failed' body, got '${svc.body}'`).toContain("login failed");

    // The boundary log for the same request shares the trace id.
    expect(
      recs.some((r) => attr(r, "rs2.source") === "external" && r.traceId === traces.login),
      "no external boundary record correlating with the service log",
    ).toBe(true);
  });

  test("[log] an unknown severity is a 400, not a silent pass-through", async () => {
    const res = await admin.get(`${LOGS}?severity=verbose`);
    expect(res.status, res.describe()).toBe(400);
    const problem = res.problem();
    expect(problem.code).toBe("bad_request");
    expect(problem.detail).toContain("verbose");
  });
});

describe("the info floor", () => {
  test("[log] no DEBUG record is readable when the node level is info", async () => {
    // The seeded traffic includes internal hops (the hashing facade's `call`
    // steps), which are the Debug-severity boundary logs the floor drops.
    const { recs } = await fetchLogs("?$take=500");
    expect(recs.length).toBeGreaterThan(0);
    for (const r of recs) {
      expect(r.severityText, `DEBUG record leaked past the info floor: ${JSON.stringify(r)}`).not.toBe("DEBUG");
    }
    // A successful internal hop logs at Debug and so is dropped; a failing one
    // (4xx/5xx) is Warn/Error and legitimately readable, so only the 2xx ones
    // must be absent.
    const quietHops = recs.filter(
      (r) => attr(r, "rs2.source") === "internal" && Number(attr(r, "http.response.status_code")) < 400,
    );
    expect(quietHops.map((r) => r.body), "successful internal hops are below the info floor").toEqual([]);
  });
});

describe("the reader's wire contract", () => {
  test("[log] X-Total-Count is the returned count and $take bounds it", async () => {
    const all = await fetchLogs("?$take=200");
    expect(all.res.totalCount(), "X-Total-Count matches the array length").toBe(all.recs.length);

    const two = await fetchLogs("?$take=2");
    expect(two.recs.length).toBe(2);
    expect(two.res.totalCount()).toBe(2);

    // Newest-first. (Each read is itself a dispatch and so logs a record, so
    // the later page's head is newer than the earlier page's, never older.)
    expect(BigInt(two.recs[0].timeUnixNano) >= BigInt(all.recs[0].timeUnixNano), "the later page's head is newer").toBe(
      true,
    );
    expect(BigInt(two.recs[1].timeUnixNano) <= BigInt(two.recs[0].timeUnixNano)).toBe(true);
    const times = all.recs.map((r) => BigInt(r.timeUnixNano));
    for (let i = 1; i < times.length; i++) {
      expect(times[i] <= times[i - 1], `records are newest-first at index ${i}`).toBe(true);
    }
  });

  test("[log] Accept: text/plain yields NDJSON, one OTLP record per line", async () => {
    const res = await admin.get(`${LOGS}?$take=5`, { headers: { accept: "text/plain" } });
    expect(res.status, res.describe()).toBe(200);
    expect(res.contentType()).toBe("text/plain");
    const lines = res
      .text()
      .split("\n")
      .filter((l) => l.trim().length > 0);
    expect(lines.length).toBe(res.totalCount());
    expect(lines.length).toBeGreaterThan(0);
    for (const line of lines) {
      const rec = JSON.parse(line) as Rec;
      expect(typeof rec.timeUnixNano).toBe("string");
      expect(typeof rec.severityText).toBe("string");
      expect(rec.attributes["rs2.tenant"]).toBe(env().tenant);
    }

    // An `Accept` naming JSON too keeps the array form (log_reader.rs).
    const both = await admin.get(`${LOGS}?$take=5`, { headers: { accept: "text/plain, application/json" } });
    expect(both.contentType()).toBe("application/json");
  });

  test("[log] the reader is read-only and gated by the mount's access", async () => {
    const post = await admin.post(LOGS, { json: {} });
    expect(post.status, post.describe()).toBe(405);
    expect(post.problem().code, "the 405 carries code bad_request").toBe("bad_request");

    const anon = await seed!.anon.get(`${LOGS}?$take=1`);
    expect(anon.status, `logs are operational data: ${anon.describe()}`).toBe(401);
  });
});
