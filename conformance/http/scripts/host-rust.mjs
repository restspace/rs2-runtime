#!/usr/bin/env node
// Start the Rust reference host on the conformance fixtures.
//
//   RS2_PORT=3101 npm run host:rust
//
// The port rule: one server per port, each with its own scratch data dir
// (`<tmpdir>/rs2-conf-<port>`), so several suites can run at once against
// independent hosts. The fixtures under `fixtures/rust/` are copied into
// that dir on every start (a fresh tenant each run) and `listen` is
// rewritten to the port.
//
// Binary: `RS2_SERVER_BIN` (a prebuilt `rs2-server` built with
// `--features js`) if set, otherwise `cargo run -p rs2-server --features js`
// from the repo root (cargo's own freshness check makes a warm rebuild
// cheap; concurrent starts serialize on cargo's lock).
//
// Ctrl-C / SIGTERM stops the server (the whole process tree on Windows).

import { spawn, spawnSync } from "node:child_process";
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const pkg = resolve(here, "..");
const repo = resolve(pkg, "..", "..");
const port = process.env.RS2_PORT || "3100";
const work = resolve(process.env.RS2_HOST_DIR || resolve(tmpdir(), `rs2-conf-${port}`));

// The port rule: refuse to start behind something already listening here
// (another host, a stale server, the developer's own node) — a foreign
// server would otherwise answer the readiness probe and the suite would run
// against the wrong tenant.
const base = `http://127.0.0.1:${port}`;
try {
  const probe = await fetch(`${base}/readyz`);
  console.error(`[host:rust] ${base} already answers (${probe.status}); pick another RS2_PORT`);
  process.exit(1);
} catch {
  /* nothing listening — good */
}

try {
  rmSync(work, { recursive: true, force: true });
} catch (e) {
  console.error(`cannot clear ${work} (is a previous server on port ${port} still running?): ${e.message}`);
  process.exit(1);
}
mkdirSync(work, { recursive: true });
cpSync(resolve(pkg, "fixtures", "rust"), work, { recursive: true });

const configPath = resolve(work, "serverConfig.json");
const config = JSON.parse(readFileSync(configPath, "utf8"));
config.listen = `127.0.0.1:${port}`;
// The operator endpoints (`/admin/*`) are gated by a token and disabled
// without one, so the fixture carries the same default as `host:cf` — the
// suites that exercise them read it from `RS2_ADMIN_TOKEN`.
config.adminToken = process.env.RS2_ADMIN_TOKEN || "dev";
// Catalogue live path (spec F.3 catalogue row): RS2_CATALOGUE_HOSTS is a
// comma list of operator-allowlisted hosts, the same env both host scripts
// accept. Unset keeps the fixture's empty allowlist (the dormant half).
if (process.env.RS2_CATALOGUE_HOSTS) {
  config.catalogueHosts = process.env.RS2_CATALOGUE_HOSTS.split(",")
    .map((h) => h.trim())
    .filter(Boolean);
}
writeFileSync(configPath, JSON.stringify(config, null, 2));

let cmd;
let args;
let cwd;
if (process.env.RS2_SERVER_BIN) {
  cmd = process.env.RS2_SERVER_BIN;
  args = [configPath];
  cwd = work;
  if (!existsSync(cmd)) {
    console.error(`RS2_SERVER_BIN '${cmd}' does not exist`);
    process.exit(1);
  }
} else {
  cmd = "cargo";
  args = ["run", "-p", "rs2-server", "--features", "js", "--", configPath];
  cwd = repo;
}

console.log(`[host:rust] port ${port}, data dir ${work}`);
console.log(`[host:rust] ${cmd} ${args.join(" ")}`);
const child = spawn(cmd, args, { cwd, stdio: "inherit", env: process.env });

function killTree() {
  if (child.exitCode !== null) return;
  if (process.platform === "win32") {
    // `cargo run` is a parent of the real server; taskkill /T takes both.
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
  console.log(`[host:rust] server exited (${code})`);
  process.exit(code ?? 1);
});

// Readiness: poll the discovery document until the fixture tenant answers
// (a cold cargo build can take minutes).
const deadline = Date.now() + 15 * 60 * 1000;
(async () => {
  while (Date.now() < deadline && child.exitCode === null) {
    try {
      const res = await fetch(`${base}/.well-known/rs2/services`);
      if (res.status === 200 && (await res.json()).tenant === "conf") {
        console.log(`[host:rust] ready at ${base} (tenant 'conf', admin admin@conf.test / conf-admin-pw)`);
        return;
      }
    } catch {
      /* not up yet */
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  if (child.exitCode === null) {
    console.error(`[host:rust] ${base}/readyz never answered; stopping`);
    killTree();
    process.exit(1);
  }
})();
