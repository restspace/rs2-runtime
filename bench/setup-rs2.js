// Deploy bench/handler.js into a running rs2 node and mount it at /bench.
//   node setup-rs2.js [baseUrl]
const fs = require("fs");
const path = require("path");

const BASE = process.argv[2] || "http://127.0.0.1:3100";
const bundle = fs.readFileSync(path.join(__dirname, "handler.js"), "utf8");

async function main() {
  // 1. deploy the bundle (compile smoke test runs server-side)
  let r = await fetch(`${BASE}/services/code/bench/`, {
    method: "POST",
    headers: { "content-type": "application/javascript" },
    body: bundle,
  });
  if (!r.ok) throw new Error(`deploy failed ${r.status}: ${await r.text()}`);
  const dep = await r.json();
  const ref = dep.ref;
  console.log("deployed:", ref, "validated:", dep.validated);

  // 2. read current config + ETag for optimistic concurrency
  r = await fetch(`${BASE}/services/raw`);
  if (!r.ok) throw new Error(`get raw failed ${r.status}`);
  const etag = r.headers.get("etag");
  const config = await r.json();

  // 3. add the /bench mount (skip if already present, e.g. re-run)
  config.mounts = config.mounts.filter((m) => m.path !== "/bench");
  config.mounts.push({ path: "/bench", service: ref, config: { access: "open" } });

  // 4. write it back
  r = await fetch(`${BASE}/services/raw`, {
    method: "PUT",
    headers: { "content-type": "application/json", ...(etag ? { "if-match": etag } : {}) },
    body: JSON.stringify(config),
  });
  if (r.status !== 204 && !r.ok) throw new Error(`put raw failed ${r.status}: ${await r.text()}`);
  console.log("mounted /bench ->", ref);

  // 5. verify
  r = await fetch(`${BASE}/bench/`);
  console.log("verify GET /bench/ ->", r.status, await r.text());
}
main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
