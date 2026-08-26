// Idempotency keys and response replay over HTTP — replaces the G6 half of
// `rs2-core/tests/m2_composition.rs` for the cross-host contract (spec F.3,
// `idempotency.test.ts` row): dedupe + replay, payload reuse, key scoping,
// the 256-character key cap, the in-flight 409, and the segment-retry proof
// that keyed effects run once across whole-segment retries.
//
// Two behaviours here are wire facts the in-process Rust test cannot see,
// and the suite asserts the Rust host (the contract), not the prose:
//
//  1. Over HTTP a request body arrives as a STREAM, and `payload_hash`
//     (`rs2-core/src/idempotency.rs`) only hashes materialized bytes — so an
//     external duplicate carrying a DIFFERENT payload replays the stored
//     response instead of the 422 `idempotency_key_reuse` the in-process
//     test (and spec F.3) shows. 422 is reachable at the wire only where the
//     bodies are materialized: an internal call whose key is pinned by a
//     pipeline spec (`payload mismatch` test below).
//  2. Replay is capped at 1 MiB of response body (`DEFAULT_BODY_CAP`):
//     larger responses are abandoned, so the duplicate re-executes.

import { readFileSync } from "node:fs";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, Rs2Client } from "./src/client.ts";
import { Seed } from "./src/seed.ts";

/** Unique per run: the Rust idempotency store is in memory and lives for the
 * whole server process, so keys must not collide with an earlier run of this
 * file against the same host. */
const RUN = Math.random().toString(36).slice(2, 10);
const key = (name: string) => `k-${name}-${RUN}`;

/** The pinned key a pipeline spec puts on its internal call (below). */
const PINNED_INNER_KEY = key("pinned-inner");

const FLAKY_BUNDLE = new URL("./fixtures/idempotency-flaky.js", import.meta.url);

describe("idempotency (g6)", () => {
  let seed: Seed | undefined;
  let anon: Rs2Client;
  let admin: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    anon = seed.anon;
    admin = seed.admin;

    // Two JS guests: the shared echo (a `sleep=<ms>` step for the in-flight
    // race) and this suite's always-transient `notify` (the wire analogue of
    // the Rust test's `FlakyBackend`).
    const echo = await deployCode(admin, "echo", readFileSync(env().codeBundle, "utf8"));
    const flaky = await deployCode(admin, "idemflaky", readFileSync(FLAKY_BUNDLE, "utf8"));

    await seed.applyMounts([
      { path: "/files", service: "file", config: { access: "open" } },
      { path: "/slow", service: echo, config: { access: "open" } },
      { path: "/notify", service: flaky, config: { access: "open" } },
      { path: "/pipes", service: "pipeline", config: { access: "open" } },
      { path: "/inner", service: "pipeline", config: { access: "open" } },
      { path: "/seg", service: "pipeline", config: { access: "open" } },
    ]);

    // `.root` governs the mount root; a named spec governs `/<mount>/<name>`.
    await putSpec(admin, "/pipes/.pipelines/.root", {
      pipeline: { steps: [{ call: { method: "GET", url: "/slow?sleep=2000" } }] },
    });
    // A transform materializes the body, so the internal call the next step
    // makes carries BYTES (hashable) under a key pinned by the spec — the
    // only shape in which `payload_hash` sees two different payloads.
    await putSpec(admin, "/inner/.pipelines/.root", {
      pipeline: {
        steps: [
          { transform: { n: "n" } },
          {
            call: {
              method: "POST",
              url: "/data/gizmos",
              headers: { "idempotency-key": PINNED_INNER_KEY },
            },
          },
        ],
      },
    });
    // The segment-retry proof (`g6_segment_retry_dedupes_keyed_effects`):
    // a keyed charge followed by a transiently failing notify, one segment.
    const retry = { maxAttempts: 3, baseDelayMs: 1, maxDelayMs: 1, jitter: "none" };
    await putSpec(admin, "/seg/.pipelines/.root", {
      retry,
      pipeline: {
        onFail: "stop",
        steps: [
          { call: { method: "POST", url: "/data/charges", effect: "keyed" } },
          { call: { method: "POST", url: "/notify?fail=99", effect: "keyed" } },
        ],
      },
    });
    // The control: the same segment with the default (unkeyed) effect.
    await putSpec(admin, "/seg/.pipelines/control", {
      retry,
      pipeline: {
        onFail: "stop",
        steps: [
          { call: { method: "POST", url: "/data/plaincharges" } },
          { call: { method: "POST", url: "/notify?fail=99" } },
        ],
      },
    });

    for (const dataset of ["widgets", "gadgets", "gizmos", "charges", "plaincharges", "bounds", "empties"]) {
      seed.trackDataset("/data", dataset);
    }
    seed.trackDir("/files", "idem");
  });

  afterAll(async () => {
    await seed?.restore();
  });

  // The Rust file's g6 sequence, step for step.
  test("a keyed create executes once and the duplicate replays it", async () => {
    const k = key("widget-1");
    const first = await anon.post("/data/widgets", { json: { n: 1 }, headers: { "idempotency-key": k } });
    expect(first.status, `first keyed create: ${first.describe()}`).toBe(201);
    const location = first.header("location");
    expect(location, "keyed create returns Location").not.toBeNull();
    expect(first.header("idempotency-replayed"), "the original is not marked replayed").toBeNull();

    // The duplicate replays the stored response — same status, same
    // Location, same ETag, same body — marked `Idempotency-Replayed: true`.
    const replay = await anon.post("/data/widgets", { json: { n: 1 }, headers: { "idempotency-key": k } });
    expect(replay.status, `duplicate replays: ${replay.describe()}`).toBe(201);
    expect(replay.header("location"), "replay carries the original Location").toBe(location);
    expect(replay.header("idempotency-replayed"), "replay is marked").toBe("true");
    expect(replay.etag(), "replay carries the stored ETag").toBe(first.etag());
    expect(replay.text(), "replay carries the stored body").toBe(first.text());

    // Exactly one widget exists.
    const list = await anon.get("/data/widgets");
    expect(list.status, list.describe()).toBe(200);
    expect(list.listing().total, `one record after the replay: ${list.text()}`).toBe(1);
    expect(list.totalCount()).toBe(1);
  });

  // Rust wire behaviour, and a divergence from spec F.3's prose: external
  // bodies are streams, streams are not hashed, so a mismatched payload
  // under a used key replays rather than 422-ing. See the header comment.
  test("a different payload under the same key replays (streamed bodies are not hashed)", async () => {
    const k = key("widget-1");
    const reuse = await anon.post("/data/widgets", { json: { n: 2 }, headers: { "idempotency-key": k } });
    expect(reuse.status, `payload reuse over the wire: ${reuse.describe()}`).toBe(201);
    expect(reuse.header("idempotency-replayed"), "the stored response is replayed").toBe("true");
    expect(reuse.json(), "the FIRST payload comes back, not the new one").toEqual({ n: 1 });

    const list = await anon.get("/data/widgets");
    expect(list.listing().total, "no second record was created").toBe(1);
  });

  // The path half of the `tenant|mount|method|path` scope.
  test("the same key on another path is fresh", async () => {
    const k = key("widget-1");
    const other = await anon.post("/data/gadgets", { json: { n: 1 }, headers: { "idempotency-key": k } });
    expect(other.status, `same key, other path: ${other.describe()}`).toBe(201);
    expect(other.header("idempotency-replayed"), "a fresh key is not a replay").toBeNull();
  });

  // The method half of the same scope (`idempotency::scope_for`).
  test("the same key on the same path but another method is fresh", async () => {
    const k = key("widget-1");
    const list = await anon.get("/data/widgets", { headers: { "idempotency-key": k } });
    expect(list.status, `GET under a POST-used key: ${list.describe()}`).toBe(200);
    expect(list.header("idempotency-replayed"), "a different method is a different scope").toBeNull();
    expect(list.listing().total).toBe(1);
  });

  test("a key longer than 256 characters is a 400", async () => {
    // 256 is the cap, not the first rejected length (`MAX_KEY_LEN`).
    const atCap = await anon.post("/data/bounds", {
      json: { n: 1 },
      headers: { "idempotency-key": "k".repeat(256) },
    });
    expect(atCap.status, `a 256-character key is accepted: ${atCap.describe()}`).toBe(201);

    const tooLong = await anon.post("/data/bounds", {
      json: { n: 1 },
      headers: { "idempotency-key": "k".repeat(257) },
    });
    expect(tooLong.status, `a 257-character key: ${tooLong.describe()}`).toBe(400);
    const problem = tooLong.problem();
    expect(problem.code).toBe("bad_request");
    expect(problem.detail).toBe("Idempotency-Key exceeds 256 characters");
  });

  // Spec F.3: "in-flight 409 via a pipeline with a slow step (GET on a
  // `code:` echo that sleeps 2 s) fired twice concurrently".
  test("a duplicate that arrives mid-flight is a retryable 409", async () => {
    const k = key("inflight");
    const headers = { "idempotency-key": k };
    const first = anon.post("/pipes", { json: {}, headers });
    // Well inside the 2 s the slow step spends in the guest.
    await new Promise((r) => setTimeout(r, 400));
    const duplicate = await anon.post("/pipes", { json: {}, headers });

    expect(duplicate.status, `the in-flight duplicate: ${duplicate.describe()}`).toBe(409);
    const problem = duplicate.problem();
    expect(problem.code).toBe("conflict");
    expect(problem.detail).toBe("a request with this Idempotency-Key is still executing");
    expect(problem.retryable, "the 409 is retryable").toBe(true);
    expect(problem.retryAfterMs).toBe(1000);
    expect(duplicate.header("retry-after"), "retryAfterMs rounded up to seconds").toBe("1");
    expect(duplicate.header("idempotency-replayed"), "nothing to replay yet").toBeNull();

    const original = await first;
    expect(original.status, `the original completes: ${original.describe()}`).toBe(200);
    expect(original.json().url, "the slow step ran").toBe("/slow?sleep=2000");

    // Once it has completed the same key replays, as any duplicate does.
    const after = await anon.post("/pipes", { json: {}, headers });
    expect(after.status, `after completion: ${after.describe()}`).toBe(200);
    expect(after.header("idempotency-replayed")).toBe("true");
    expect(after.text()).toBe(original.text());
  });

  // The 422 the Rust unit test sees: reachable at the wire only where the
  // payload is materialized — an internal call under a spec-pinned key.
  test("a materialized payload under a used key is 422 idempotency_key_reuse", async () => {
    const first = await anon.post("/inner", { json: { n: 1 } });
    expect(first.status, `first run through the pinned-key pipeline: ${first.describe()}`).toBe(201);

    const mismatch = await anon.post("/inner", { json: { n: 2 } });
    expect(mismatch.status, `same inner key, different payload: ${mismatch.describe()}`).toBe(422);
    const problem = mismatch.problem();
    expect(problem.code).toBe("idempotency_key_reuse");
    expect(problem.detail).toBe("Idempotency-Key was already used with a different request payload");
    expect(problem.retryable).toBe(false);

    // The same payload as the first run replays instead: one record only.
    const same = await anon.post("/inner", { json: { n: 1 } });
    expect(same.status, `the identical payload replays: ${same.describe()}`).toBe(201);
    const list = await anon.get("/data/gizmos");
    expect(list.listing().total, "the inner key deduped: one record").toBe(1);
  });

  // `Begin::Fresh` + a service error → `abandon`: nothing is stored, so the
  // next request with that key executes rather than replaying an error.
  test("a failed request abandons its key", async () => {
    const k = key("abandon");
    const bad = await anon.post("/data/gadgets", {
      body: "not json at all",
      contentType: "application/json",
      headers: { "idempotency-key": k },
    });
    expect(bad.status, `the failing original: ${bad.describe()}`).toBe(400);
    expect(bad.header("idempotency-replayed")).toBeNull();

    const retry = await anon.post("/data/gadgets", { json: { n: 9 }, headers: { "idempotency-key": k } });
    expect(retry.status, `the retry executes: ${retry.describe()}`).toBe(201);
    expect(retry.header("idempotency-replayed"), "an abandoned key is fresh again").toBeNull();
  });

  // `DEFAULT_BODY_CAP` (1 MiB): a larger response is not recorded, so the
  // duplicate re-executes. The small file is the control.
  test("a response over the 1 MiB replay cap is not stored", async () => {
    const small = await anon.put("/files/idem/small.txt", { body: "y".repeat(16), contentType: "text/plain" });
    expect(small.status, small.describe()).toBe(201);
    const big = await anon.put("/files/idem/big.txt", {
      body: "x".repeat(1024 * 1024 + 1024),
      contentType: "text/plain",
    });
    expect(big.status, big.describe()).toBe(201);

    const kSmall = { "idempotency-key": key("small") };
    const s1 = await anon.get("/files/idem/small.txt", { headers: kSmall });
    expect(s1.status, s1.describe()).toBe(200);
    expect(s1.header("idempotency-replayed")).toBeNull();
    const s2 = await anon.get("/files/idem/small.txt", { headers: kSmall });
    expect(s2.status, s2.describe()).toBe(200);
    expect(s2.header("idempotency-replayed"), "a small response is replayable").toBe("true");

    const kBig = { "idempotency-key": key("big") };
    const b1 = await anon.get("/files/idem/big.txt", { headers: kBig });
    expect(b1.status, b1.describe()).toBe(200);
    const b2 = await anon.get("/files/idem/big.txt", { headers: kBig });
    expect(b2.status, b2.describe()).toBe(200);
    expect(b2.header("idempotency-replayed"), "an over-cap response is not replayed").toBeNull();
    expect(b2.bytes.length, "the duplicate re-executed and served the file").toBe(b1.bytes.length);
  });

  // `g6_segment_retry_dedupes_keyed_effects`: the segment retries as a unit,
  // the keyed call re-runs on every attempt under the SAME derived key, and
  // therefore takes effect exactly once.
  test("a keyed effect inside a retried segment executes once", async () => {
    await anon.get("/notify?reset=1");
    const run = await anon.post("/seg", { json: { amount: 10 } });
    // Every attempt's notify fails transiently (503 is in the default retry
    // status list), so the run ends 503 after `maxAttempts`.
    expect(run.status, `the segment exhausts its attempts: ${run.describe()}`).toBe(503);
    const trace = run.json<{ pipeline?: { steps?: { step: string; status: number }[] } }>().pipeline;
    const charges = (trace?.steps ?? []).filter((s) => s.step === "/0");
    expect(charges.length, `the charge ran on every segment attempt: ${run.text()}`).toBe(3);
    expect(charges.every((s) => s.status === 201), "each attempt saw the created response").toBe(true);

    const list = await admin.get("/data/charges");
    expect(list.status, list.describe()).toBe(200);
    expect(list.listing().total, "the same derived key on every attempt: one effect").toBe(1);

    // Control: the same segment with the default (unsafe) effect is not
    // retried at all — an unsafe step that is not last in its segment makes
    // the segment ineligible for retry (`GET <spec>?$plan` warns about it),
    // so `keyed` is what buys both the retry and the exactly-once effect.
    await anon.get("/notify?reset=1");
    const plain = await anon.post("/seg/control", { json: { amount: 10 } });
    expect(plain.status, `the unkeyed control fails: ${plain.describe()}`).toBe(503);
    const plainTrace = plain.json<{ pipeline?: { steps?: { step: string; status: number }[] } }>().pipeline;
    expect(
      (plainTrace?.steps ?? []).filter((s) => s.step === "/0").length,
      `an unkeyed segment is not retried: ${plain.text()}`,
    ).toBe(1);
    const plainList = await admin.get("/data/plaincharges");
    expect(plainList.listing().total, "one attempt, one effect").toBe(1);

    const planned = await admin.get("/seg/.pipelines/control?$plan");
    expect(planned.status, planned.describe()).toBe(200);
    expect(
      JSON.stringify(planned.json().plan?.warnings ?? []),
      "the plan warns that the unsafe step would re-execute on a retry",
    ).toContain("unsafe-effect step is not the last in its segment");
  });
});

/** Deploy a JS bundle to the code store and return the `code:<name>@<v>` ref. */
async function deployCode(client: Rs2Client, name: string, source: string): Promise<string> {
  const res = await client.post(`/services/code/${name}/`, {
    body: source,
    contentType: "application/javascript",
  });
  if (res.status !== 201) throw new Error(`deploying ${name}: ${res.describe()}`);
  const ref = res.json<{ ref?: string }>().ref;
  if (!ref) throw new Error(`deploying ${name}: no ref in ${res.text()}`);
  return ref;
}

/** PUT a pipeline spec envelope into a pipeline mount's authoring subtree. */
async function putSpec(client: Rs2Client, path: string, envelope: unknown): Promise<void> {
  const res = await client.put(path, { json: envelope });
  if (res.status !== 201 && res.status !== 200) throw new Error(`PUT ${path}: ${res.describe()}`);
}
