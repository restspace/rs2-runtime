// rs2 JS-sandbox handler. Deployed via POST /services/code/bench/ and mounted
// at /bench. Kept byte-trivial so the measurement is dominated by the
// request path + (for rs2) per-invocation V8 isolate creation, not user work.
export default async (msg, ctx) => {
  // a touch of real JS work so the isolate isn't a complete no-op
  let acc = 0;
  for (let i = 0; i < 50; i++) acc += i;
  return {
    status: 200,
    headers: { "content-type": "application/json", "x-engine": "js" },
    body: { ok: true, engine: "rs2-js", acc },
  };
};
