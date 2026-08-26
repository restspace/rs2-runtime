#!/usr/bin/env node
// Start the Cloudflare Worker host locally (spec section G):
//
//   RS2_PORT=8787 RS2_ADMIN_TOKEN=dev npm run host:cf
//
// Runs `wrangler dev` in `rs2-worker/` with `RS2_DEFAULT_TENANT=conf` (so
// any Host resolves to the fixture tenant) and the admin token the runner's
// globalSetup uses for `PUT /admin/tenants/conf`. Local R2/DO/loader
// emulation; no account needed. One port per instance, like host:rust —
// wrangler keeps its local state under `rs2-worker/.wrangler/state`, so set
// `RS2_CF_PERSIST` to a per-port directory to run several at once.

import { spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "..", "..", "..");
const workerDir = resolve(repo, "rs2-worker");
const port = process.env.RS2_PORT || "8787";
const adminToken = process.env.RS2_ADMIN_TOKEN || "dev";
const persist = process.env.RS2_CF_PERSIST || resolve(tmpdir(), `rs2-conf-cf-${port}`);

if (!existsSync(resolve(workerDir, "wrangler.jsonc"))) {
  console.error(`[host:cf] ${workerDir} has no wrangler.jsonc — the Worker host is not built yet (cloudflare.md P2)`);
  process.exit(1);
}

const args = [
  "wrangler",
  "dev",
  "--port",
  port,
  "--ip",
  "127.0.0.1",
  "--persist-to",
  persist,
  "--var",
  "RS2_DEFAULT_TENANT:conf",
  "--var",
  `RS2_ADMIN_TOKEN:${adminToken}`,
];
// Catalogue live path (spec F.3 catalogue row): same env as host:rust — a
// comma list the Worker reads as the operator allowlist var.
if (process.env.RS2_CATALOGUE_HOSTS) {
  args.push("--var", `RS2_CATALOGUE_HOSTS:${process.env.RS2_CATALOGUE_HOSTS}`);
}
console.log(`[host:cf] port ${port}, state ${persist}`);
console.log(`[host:cf] npx ${args.join(" ")}`);
const child = spawn("npx", args, {
  cwd: workerDir,
  stdio: "inherit",
  env: process.env,
  shell: process.platform === "win32",
});

function killTree() {
  if (child.exitCode !== null) return;
  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
  } else {
    child.kill("SIGTERM");
  }
}
for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  process.on(sig, () => {
    killTree();
    process.exit(0);
  });
}
child.on("exit", (code) => {
  console.log(`[host:cf] wrangler exited (${code})`);
  process.exit(code ?? 1);
});

const base = `http://127.0.0.1:${port}`;
const deadline = Date.now() + 5 * 60 * 1000;
(async () => {
  while (Date.now() < deadline && child.exitCode === null) {
    try {
      const res = await fetch(`${base}/readyz`);
      if (res.status === 200) {
        console.log(`[host:cf] ready at ${base} (RS2_HOST_KIND=cloudflare RS2_ADMIN_TOKEN=${adminToken})`);
        return;
      }
    } catch {
      /* not up yet */
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  if (child.exitCode === null) {
    console.error(`[host:cf] ${base}/readyz never answered; stopping`);
    killTree();
    process.exit(1);
  }
})();
