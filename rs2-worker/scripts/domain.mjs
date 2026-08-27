#!/usr/bin/env node
// `npm run domain` — attach a customer's own domain to a tenant.
//
//   npm run domain -- add app.acme.com --tenant acme [--wait]
//   npm run domain -- status app.acme.com
//   npm run domain -- list
//   npm run domain -- remove app.acme.com
//
// Needs `RS2_BASE_URL` and `RS2_ADMIN_TOKEN` (the deployment's admin secret).
//
// The whole point of this script is that there is almost nothing in it. The
// host does the work — provisioning, verifying, promoting — because the gate
// has to live where the routing table is, not in an operator's terminal. What
// this adds is the two things a terminal is good for: printing the DNS
// records in a form a customer can act on, and *proving* the finished domain
// serves RS2 rather than trusting `status: "active"` (the same reason
// `deploy.mjs` re-checks a deployment it just uploaded).

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

const say = (msg) => console.log(`[domain] ${msg}`);
const die = (msg) => {
  console.error(`[domain] ${msg}`);
  process.exit(1);
};

const argv = process.argv.slice(2);
const flags = new Map();
const positional = [];
for (let i = 0; i < argv.length; i++) {
  const arg = argv[i];
  if (arg.startsWith("--")) {
    const [name, inline] = arg.slice(2).split("=");
    flags.set(name, inline ?? (argv[i + 1]?.startsWith("--") === false ? argv[++i] : "true"));
  } else {
    positional.push(arg);
  }
}

const [command, host] = positional;

/// This file's own header comment, which is the usage.
function usage() {
  const lines = readFileSync(resolve(here, "domain.mjs"), "utf8").split("\n");
  console.log(
    lines
      .slice(1, 9)
      .map((l) => l.replace(/^\/\/ ?/, ""))
      .join("\n"),
  );
}

if (command === undefined || command === "help") {
  usage();
  process.exit(command === undefined ? 1 : 0);
}

const base = (process.env.RS2_BASE_URL ?? "").replace(/\/+$/, "");
const token = process.env.RS2_ADMIN_TOKEN;
if (!base) die("set RS2_BASE_URL to the deployment (e.g. https://rs2-worker.example.workers.dev)");
if (!token) die("set RS2_ADMIN_TOKEN to the deployment's admin secret");

async function admin(method, path, body) {
  let res;
  try {
    res = await fetch(`${base}${path}`, {
      method,
      headers: {
        authorization: `Bearer ${token}`,
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
  } catch (e) {
    return die(`${method} ${base}${path} failed: ${e.message}`);
  }
  const text = await res.text();
  let doc;
  try {
    doc = text === "" ? {} : JSON.parse(text);
  } catch {
    doc = { raw: text };
  }
  return { status: res.status, doc, text };
}

/// The records a customer publishes, as something they can read off a screen.
function printRecords(doc) {
  const records = doc.dnsRecords ?? [];
  if (records.length === 0) return;
  const width = (key) => Math.max(...records.map((r) => String(r[key]).length), key.length);
  const w = { type: width("type"), name: width("name"), value: width("value") };
  console.log("");
  console.log(`  ${"TYPE".padEnd(w.type)}  ${"NAME".padEnd(w.name)}  ${"VALUE".padEnd(w.value)}`);
  for (const r of records) {
    const line = `  ${String(r.type).padEnd(w.type)}  ${String(r.name).padEnd(w.name)}  ${String(r.value).padEnd(w.value)}`;
    console.log(r.required ? line : `${line}   (optional: ${r.purpose})`);
  }
  console.log("");
}

function report(doc) {
  say(`${doc.host} → tenant '${doc.tenant}': ${doc.status}  [provider ${doc.provider?.name}]`);
  printRecords(doc);
  if (doc.nextStep) say(doc.nextStep);
  // Only when it is stuck: the raw provider state is diagnosis, not routine.
  if (doc.status !== "active" && doc.provider?.detail) {
    say(`provider detail: ${JSON.stringify(doc.provider.detail)}`);
  }
}

/// The check `status: "active"` cannot make: does the finished domain
/// actually serve RS2? A wrong fallback origin or a missing Worker route
/// both leave the attachment looking perfect and the domain answering
/// someone else's 404.
async function proveItServes(name) {
  const url = `https://${name}/.well-known/rs2/services`;
  try {
    const res = await fetch(url, { redirect: "manual" });
    if (res.status !== 200) return say(`WARNING: ${url} answered ${res.status} — the domain is attached but is not serving RS2`);
    const doc = await res.json();
    if (typeof doc?.limits?.host !== "string") return say(`WARNING: ${url} answered 200 but not an RS2 discovery document`);
    say(`verified: ${name} serves RS2 (host ${doc.limits.host})`);
  } catch (e) {
    say(`WARNING: ${url} unreachable (${e.message}) — DNS may not have propagated to you yet`);
  }
}

async function waitForActive(name, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const { status, doc, text } = await admin("GET", `/admin/domains/${encodeURIComponent(name)}`);
    if (status !== 200) die(`GET /admin/domains/${name} → ${status}: ${text.slice(0, 200)}`);
    if (doc.status === "active") return doc;
    if (Date.now() > deadline) {
      report(doc);
      die(`${name} was still pending after ${Math.round(timeoutMs / 1000)}s — the record may not have propagated yet; nothing is lost, poll again with 'status'`);
    }
    await new Promise((r) => setTimeout(r, 15_000));
  }
}

switch (command) {
  case "add": {
    if (!host) die("usage: npm run domain -- add <host> --tenant <name> [--wait]");
    const tenant = flags.get("tenant");
    if (!tenant) die("add needs --tenant <name>");
    const { status, doc, text } = await admin("PUT", `/admin/domains/${encodeURIComponent(host)}`, { tenant });
    if (status === 409) die(`${host} is already claimed: ${doc.detail ?? text}`);
    if (status !== 200 && status !== 202) die(`PUT /admin/domains/${host} → ${status}: ${doc.detail ?? text.slice(0, 300)}`);
    report(doc);
    if (doc.status === "active") {
      await proveItServes(host);
      break;
    }
    if (flags.has("wait")) {
      const minutes = Number(flags.get("wait")) || 10;
      say(`waiting up to ${minutes}m for the DNS to be visible…`);
      report(await waitForActive(host, minutes * 60_000));
      await proveItServes(host);
    } else {
      say(`re-run 'npm run domain -- status ${host}' once the record is published, or leave it: the host promotes it on its own`);
    }
    break;
  }
  case "status": {
    if (!host) die("usage: npm run domain -- status <host>");
    const { status, doc, text } = await admin("GET", `/admin/domains/${encodeURIComponent(host)}`);
    if (status === 404) die(`${host} is not attached`);
    if (status !== 200) die(`GET /admin/domains/${host} → ${status}: ${text.slice(0, 200)}`);
    report(doc);
    if (doc.status === "active") await proveItServes(host);
    break;
  }
  case "list": {
    const { status, doc, text } = await admin("GET", "/admin/domains");
    if (status !== 200) die(`GET /admin/domains → ${status}: ${text.slice(0, 200)}`);
    const domains = doc.domains ?? [];
    if (domains.length === 0) {
      say("no domains attached");
      break;
    }
    const w = Math.max(...domains.map((d) => d.host.length));
    for (const d of domains) console.log(`  ${d.host.padEnd(w)}  ${d.status.padEnd(7)}  ${d.tenant}`);
    break;
  }
  case "remove": {
    if (!host) die("usage: npm run domain -- remove <host>");
    const { status, text } = await admin("DELETE", `/admin/domains/${encodeURIComponent(host)}`);
    if (status !== 204) die(`DELETE /admin/domains/${host} → ${status}: ${text.slice(0, 200)}`);
    say(`${host} detached (both the mapping and any unproven claim)`);
    say("the customer's CNAME can stay or go — it resolves to nothing here now");
    break;
  }
  default:
    usage();
    die(`unknown command '${command}'`);
}
