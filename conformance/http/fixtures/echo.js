// JS port of `conformance/echo-guest` (the Wasm echo component) for the
// HTTP conformance runner's `code:` mount cases. Runs unchanged on both
// hosts: every host capability is `await`ed, which is a no-op on the Rust
// V8 engine (synchronous returns) and required on the Worker (Promises —
// the declared `guest-async` facet).
//
// Behaviour, keyed on the request URL:
//   - path contains `deny-check`  → calls an ungranted capability and
//     reports `denied-as-expected` (text/plain) when the host refuses it
//     with `capability_denied`; anything else is a thrown error (502).
//   - query `sleep=<ms>`          → waits that long before answering (real
//     time on both hosts — see `sleepFor`).
//   - path contains `state`       → bumps a counter in `ctx.state` and
//     returns `{ count }` so persistence across calls is observable.
//   - otherwise                   → 200 JSON `{ method, url, body, config }`
//     with `x-engine: js`, mirroring the Wasm guest's echo shape.

async function sleepFor(ms) {
  const start = Date.now();
  // Real timers on the Worker resolve after `ms`. The Rust V8 prelude's
  // timers are virtual (fast-forwarded), so the promise settles at once and
  // the busy loop below supplies the real delay. `Date.now()` is real on
  // both hosts.
  await new Promise((resolve) => setTimeout(resolve, ms));
  while (Date.now() - start < ms) {
    /* spin */
  }
}

export default async (msg, ctx) => {
  const url = String(msg.url || "");
  const path = url.split("?")[0];
  const query = new URLSearchParams(url.includes("?") ? url.slice(url.indexOf("?") + 1) : "");

  if (path.includes("deny-check")) {
    try {
      await ctx.request("not-granted", { method: "GET", url: "/anything" });
    } catch (e) {
      if (e && e.code === "capability_denied") {
        return { status: 200, body: "denied-as-expected", mediaType: "text/plain" };
      }
      throw e;
    }
    throw new Error("expected capability denial");
  }

  const sleep = Number(query.get("sleep") || 0);
  if (sleep > 0) await sleepFor(sleep);

  if (path.includes("state")) {
    const previous = await ctx.state.get("count");
    const count = Number(previous || 0) + 1;
    await ctx.state.put("count", String(count));
    return { status: 200, headers: { "x-engine": "js" }, body: { count } };
  }

  return {
    status: 200,
    headers: { "x-engine": "js" },
    body: {
      method: msg.method,
      url,
      // JSON bodies arrive parsed on both hosts; anything else is a string,
      // and no body is the empty string (as the Wasm guest reports it).
      body: msg.body === null || msg.body === undefined ? "" : msg.body,
      config: ctx.config === undefined ? null : ctx.config,
    },
  };
};
