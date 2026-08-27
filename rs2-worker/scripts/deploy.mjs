#!/usr/bin/env node
// `npm run deploy` — deploy the Worker and prove the deployment answers.
//
//   npm run deploy                      # build shim → wrangler deploy → smoke
//   npm run deploy -- --verify          # …then the full remote conformance run
//   npm run deploy -- --dry-run         # wrangler's own dry run, no upload
//   npm run deploy -- -- --keep-vars    # everything after a bare `--` goes to wrangler
//
// Why a script rather than `wrangler deploy`: an upload is not a deployment
// that works. This waits for the new version to answer `/readyz`, checks the
// discovery document reports *this* host, and can run the conformance suite
// against the live URL — the only check that covers real R2, real DO SQLite
// and real Dynamic Workers (cloudflare.md decision 43/44 are both bugs that
// only a deployed run could show).
//
// Per-deployment vars live in `.deploy.vars.json` (gitignored, see
// `.deploy.vars.example.json`): wrangler's `--var` overrides are per-deploy,
// so a deployment that needs one — `RS2_LIMITS` on a Workers Free account —
// would silently lose it on the next plain `wrangler deploy`.

import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const workerDir = resolve(here, "..");
const conformanceDir = resolve(workerDir, "..", "conformance", "http");
const varsFile = resolve(workerDir, ".deploy.vars.json");

const argv = process.argv.slice(2);
const passIdx = argv.indexOf("--");
const flags = new Set(passIdx === -1 ? argv : argv.slice(0, passIdx));
const wranglerExtra = passIdx === -1 ? [] : argv.slice(passIdx + 1);
const verify = flags.has("--verify");
const dryRun = flags.has("--dry-run");

const say = (msg) => console.log(`[deploy] ${msg}`);
const die = (msg) => {
  console.error(`[deploy] ${msg}`);
  process.exit(1);
};

/// Run a Node program by its module path — never `npx`, never a shell. A
/// `--var RS2_LIMITS:{"outboundCalls": 45}` argument contains a space, and
/// `shell: true` on Windows concatenates argv into one string, so it split
/// into two vars and the deployment silently lost its limit override. With
/// the shell out of the loop, arguments reach the child verbatim.
function run(args, opts = {}) {
  const r = spawnSync(process.execPath, args, {
    cwd: opts.cwd ?? workerDir,
    encoding: "utf8",
    stdio: opts.capture ? ["inherit", "pipe", "inherit"] : "inherit",
    env: { ...process.env, ...(opts.env ?? {}) },
  });
  if (r.error) die(`node ${args[0]} failed to start: ${r.error.message}`);
  if (opts.capture && r.stdout) process.stdout.write(r.stdout);
  if (r.status !== 0) die(`${opts.label ?? args[0]} exited ${r.status}`);
  return r.stdout ?? "";
}

/// A dependency's own entry script, so it runs under this Node without a
/// package-manager shim in between.
function bin(dir, rel) {
  const p = resolve(dir, "node_modules", rel);
  if (!existsSync(p)) die(`${rel} not found under ${dir} — run npm ci there first`);
  return p;
}

/// `--var NAME:value` pairs from `.deploy.vars.json`, a flat string map.
function deployVars() {
  if (!existsSync(varsFile)) return [];
  let parsed;
  try {
    parsed = JSON.parse(readFileSync(varsFile, "utf8"));
  } catch (e) {
    die(`.deploy.vars.json is not valid JSON: ${e.message}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    die(".deploy.vars.json must be a JSON object of NAME → value");
  }
  const args = [];
  for (const [name, value] of Object.entries(parsed)) {
    if (typeof value !== "string") die(`.deploy.vars.json: '${name}' must be a string`);
    args.push("--var", `${name}:${value}`);
    say(`var ${name} (from .deploy.vars.json)`);
  }
  return args;
}

/// Discovery's name for each limit the table can override (the rest are not
/// advertised, so nothing to check them against).
const ADVERTISED = {
  wallClockServiceMs: "wallClockMs",
  materializedBodyBytes: "materializedBodyBytes",
  outboundCalls: "outboundCalls",
  maxDepth: "maxDepth",
};

/// A `--var` that does not arrive is the failure mode this script exists to
/// catch: an argument containing a space was once split in two, and the
/// deployment came up on the defaults while reporting success. So whatever
/// `RS2_LIMITS` asks for must be what the live host advertises.
function checkLimitsApplied(advertised) {
  if (!existsSync(varsFile)) return;
  const raw = JSON.parse(readFileSync(varsFile, "utf8")).RS2_LIMITS;
  if (typeof raw !== "string") return;
  let wanted;
  try {
    wanted = JSON.parse(raw);
  } catch {
    die(`.deploy.vars.json: RS2_LIMITS is not valid JSON — the Worker would ignore it: ${raw}`);
  }
  for (const [key, value] of Object.entries(wanted)) {
    const name = ADVERTISED[key];
    if (name === undefined) continue; // not advertised; nothing to compare
    if (advertised[name] !== value) {
      die(`RS2_LIMITS asked for ${key}=${value} but the deployment advertises ${name}=${advertised[name]}`);
    }
    say(`limit ${name}=${value} applied`);
  }
}

async function get(url, headers = {}) {
  try {
    const res = await fetch(url, { headers, redirect: "manual" });
    const text = await res.text();
    return { status: res.status, text, headers: res.headers };
  } catch (e) {
    return { status: 0, text: String(e), headers: new Headers() };
  }
}

/// Poll `/readyz` until the new version answers (a deploy propagates in
/// seconds, but not instantly).
async function waitReady(base, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const res = await get(`${base}/readyz`);
    if (res.status === 200) return;
    if (Date.now() > deadline) die(`${base}/readyz did not answer 200 within ${timeoutMs / 1000}s (last: ${res.status})`);
    await new Promise((r) => setTimeout(r, 2000));
  }
}

// ---- 1. the guest shim must match its sources ------------------------------

say("building the guest shim");
run([resolve(here, "build-shim.mjs")], { label: "build:shim" });
const shimDiff = spawnSync("git", ["diff", "--stat", "--", "src/engines/guest-shim.bundled.ts"], {
  cwd: workerDir,
  encoding: "utf8",
});
if ((shimDiff.stdout ?? "").trim() !== "") {
  say("NOTE: guest-shim.bundled.ts changed — the checked-in copy was stale; commit it (CI diffs this file)");
}

// ---- 2. deploy -------------------------------------------------------------

const wranglerArgs = [bin(workerDir, "wrangler/bin/wrangler.js"), "deploy", ...deployVars(), ...wranglerExtra];
if (dryRun) wranglerArgs.push("--dry-run", "--outdir", resolve(workerDir, ".wrangler", "dry"));
say(`wrangler ${wranglerArgs.slice(1).join(" ")}`);
const out = run(wranglerArgs, { capture: true, label: "wrangler deploy" });

if (dryRun) {
  say("dry run: nothing uploaded");
  process.exit(0);
}

const url = (out.match(/https:\/\/[^\s]+\.workers\.dev/) ?? [])[0] ?? process.env.RS2_BASE_URL;
const version = (out.match(/Current Version ID:\s*([0-9a-f-]+)/) ?? [])[1] ?? "unknown";
if (!url) die("could not find the deployed URL in wrangler's output (set RS2_BASE_URL to check a custom domain)");

// ---- 3. smoke: the deployment answers, and it is this host -----------------

say(`waiting for ${url}/readyz`);
await waitReady(url);
say("readyz 200");

const services = await get(`${url}/.well-known/rs2/services`);
if (services.status === 200) {
  let host;
  try {
    host = JSON.parse(services.text)?.limits?.host;
  } catch {
    die(`discovery did not return JSON: ${services.text.slice(0, 200)}`);
  }
  if (host !== "cloudflare") die(`discovery reports host '${host}', expected 'cloudflare'`);
  say("discovery 200, limits.host=cloudflare");
  checkLimitsApplied(JSON.parse(services.text)?.limits ?? {});
} else if (services.status === 404 && services.text.includes("unknown tenant")) {
  // A fresh deployment with no tenant provisioned yet: reachable, nothing to serve.
  say("discovery 404 'unknown tenant' — deployment is up; provision a tenant via PUT /admin/tenants/<name>");
} else {
  die(`discovery ${services.status}: ${services.text.slice(0, 200)}`);
}

// ---- 4. optional: the whole contract, against the live URL -----------------

if (verify) {
  const token = process.env.RS2_ADMIN_TOKEN;
  const tenant = process.env.RS2_TENANT;
  if (!token) die("--verify needs RS2_ADMIN_TOKEN (the deployment's admin secret)");
  if (!tenant) die("--verify needs RS2_TENANT (the tenant the suite may reshape)");
  if (!existsSync(conformanceDir)) die(`conformance runner not found at ${conformanceDir}`);
  say(`running conformance against ${url} (tenant '${tenant}')`);
  say("NOTE: this REWRITES that tenant's config and writes files/records — never point it at real data");
  // `guest-adapter.test.ts` is excluded: its mock Redis/Mongo backends are
  // 127.0.0.1 TCP servers a deployed Worker cannot dial (§F).
  run([bin(conformanceDir, "vitest/vitest.mjs"), "run", "--exclude", "**/guest-adapter.test.ts"], {
    cwd: conformanceDir,
    label: "conformance",
    env: {
      RS2_HOST_KIND: "cloudflare",
      RS2_BASE_URL: url,
      RS2_TENANT: tenant,
      RS2_ADMIN_TOKEN: token,
      RS2_CF_REMOTE: "1",
    },
  });
}

say(`done — ${url} (version ${version})${verify ? ", conformance green" : ""}`);
