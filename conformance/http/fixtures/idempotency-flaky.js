// Fixture for `idempotency.test.ts` only: the wire analogue of the
// `FlakyBackend` in `rs2-core/tests/m2_composition.rs`
// (`g6_segment_retry_dedupes_keyed_effects`) — a step that answers with a
// retryable status so the pipeline segment containing it retries as a unit,
// which is what makes the keyed effect ahead of it observable exactly once.
//
// Query parameters:
//   fail=<n>   the first `n` calls answer 503 (default 2); `fail=99` is the
//              "always transient" mode the suite uses
//   reset=1    zero the counter and answer 200
//
// The counter lives in `ctx.state`, which persists across separate requests
// but is snapshotted per outer request on the Rust host — every attempt
// inside one pipeline run sees the same value. That is why the suite drives
// this with `fail=99` and asserts the retries from the pipeline trace rather
// than expecting a later attempt to succeed.
//
// Every host capability is awaited: a no-op on the Rust V8 engine, required
// on the Worker (the declared `guest-async` facet).

export default async (msg, ctx) => {
  const url = String(msg.url || "");
  const query = new URLSearchParams(url.includes("?") ? url.slice(url.indexOf("?") + 1) : "");

  if (query.get("reset")) {
    await ctx.state.put("calls", "0");
    return { status: 200, headers: { "x-engine": "js" }, body: { calls: 0, reset: true } };
  }

  const fail = Number(query.get("fail") || 2);
  const calls = Number((await ctx.state.get("calls")) || 0) + 1;
  await ctx.state.put("calls", String(calls));

  if (calls <= fail) {
    // 503 is in the default retry status list (408/429/500/502/503/504).
    return { status: 503, headers: { "x-engine": "js" }, body: { calls, transient: true } };
  }
  return { status: 200, headers: { "x-engine": "js" }, body: { calls } };
};
