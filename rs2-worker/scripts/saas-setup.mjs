#!/usr/bin/env node
// `npm run saas:setup` — the one-time Cloudflare wiring that lets customers
// attach their own domains, done once for the whole deployment.
//
//   CF_API_TOKEN=… node scripts/saas-setup.mjs --zone rs2.example --origin saas.rs2.example
//   CF_API_TOKEN=… node scripts/saas-setup.mjs --zone rs2.example --check
//
// Three things have to be true before `PUT /admin/domains/<host>` can work,
// and all three are dashboard scavenger hunts:
//
//   1. An **originless DNS record** in your zone (`AAAA 100::`, proxied) —
//      the fallback origin has to exist as a record before it can be set.
//   2. That record set as the zone's **fallback origin**, which is where
//      Cloudflare sends traffic for every custom hostname.
//   3. A **`*/*` Worker route** on the zone. One route covers every custom
//      hostname, present and future — there is deliberately no per-domain
//      route to create, which is what makes attaching a customer's domain a
//      single CNAME on their side and nothing at all on yours.
//
// Idempotent: it creates what is missing, leaves what is right, and reports
// what it found. `--check` changes nothing and just reports drift.
//
// The token needs, on that zone: Zone:Read, DNS:Edit, SSL and
// Certificates:Edit, Workers Routes:Edit. It is used *here* only — the
// deployment's own `CF_API_TOKEN` secret (which needs just SSL and
// Certificates:Edit) is what provisions each customer hostname later.

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const workerDir = resolve(here, "..");
const varsFile = resolve(workerDir, ".deploy.vars.json");

const say = (msg) => console.log(`[saas] ${msg}`);
const die = (msg) => {
  console.error(`[saas] ${msg}`);
  process.exit(1);
};

const argv = process.argv.slice(2);
const flags = new Map();
for (let i = 0; i < argv.length; i++) {
  if (!argv[i].startsWith("--")) continue;
  const [name, inline] = argv[i].slice(2).split("=");
  flags.set(name, inline ?? (argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : "true"));
}

const zoneName = flags.get("zone");
const check = flags.has("check");
const apiToken = process.env.CF_API_TOKEN;
if (!zoneName) die("--zone <your-zone.example> is required (the zone customers' domains CNAME into)");
if (!apiToken) die("set CF_API_TOKEN (Zone:Read, DNS:Edit, SSL and Certificates:Edit, Workers Routes:Edit on that zone)");

/// The Worker script the `*/*` route points at — whatever `wrangler.jsonc`
/// is named, so a renamed Worker cannot silently get a route to nowhere.
function workerName() {
  const raw = readFileSync(resolve(workerDir, "wrangler.jsonc"), "utf8").replace(/^\s*\/\/.*$/gm, "");
  const name = JSON.parse(raw).name;
  if (typeof name !== "string" || name === "") die("wrangler.jsonc has no 'name'");
  return name;
}

async function cf(method, path, body) {
  let res;
  try {
    res = await fetch(`https://api.cloudflare.com/client/v4${path}`, {
      method,
      headers: {
        authorization: `Bearer ${apiToken}`,
        ...(body !== undefined ? { "content-type": "application/json" } : {}),
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
  } catch (e) {
    return die(`Cloudflare API unreachable: ${e.message}`);
  }
  const doc = await res.json().catch(() => ({}));
  if (!res.ok || doc.success !== true) {
    const errors = (doc.errors ?? []).map((e) => `${e.code}: ${e.message}`).join("; ");
    die(`${method} ${path} failed (HTTP ${res.status})${errors ? `: ${errors}` : ""}`);
  }
  return doc.result;
}

// ---- 0. the zone ------------------------------------------------------------

const zones = await cf("GET", `/zones?name=${encodeURIComponent(zoneName)}`);
if (!Array.isArray(zones) || zones.length === 0) {
  die(`no zone '${zoneName}' on this account — add the domain to Cloudflare first (its nameservers must point at Cloudflare)`);
}
const zone = zones[0];
const zoneId = zone.id;
say(`zone ${zoneName} (${zoneId}), plan ${zone.plan?.name ?? "unknown"}`);

const origin = flags.get("origin") ?? `saas.${zoneName}`;
if (!origin.endsWith(zoneName)) die(`--origin '${origin}' must be inside the zone '${zoneName}'`);

// ---- 1. the originless record ----------------------------------------------

const records = await cf("GET", `/zones/${zoneId}/dns_records?name=${encodeURIComponent(origin)}`);
const existing = Array.isArray(records) ? records[0] : undefined;
// `100::` is the IPv6 discard prefix: the record exists so the hostname
// resolves and can be proxied, and nothing is ever actually dialled — the
// Worker answers before an origin is ever needed.
const wanted = { type: "AAAA", name: origin, content: "100::", proxied: true, ttl: 1 };
if (existing === undefined) {
  if (check) say(`MISSING: DNS record ${origin} AAAA 100:: (proxied)`);
  else {
    await cf("POST", `/zones/${zoneId}/dns_records`, wanted);
    say(`created ${origin} AAAA 100:: (proxied)`);
  }
} else if (existing.type !== "AAAA" || existing.content !== "100::" || existing.proxied !== true) {
  say(`WARNING: ${origin} exists as ${existing.type} ${existing.content} proxied=${existing.proxied}`);
  say("  leaving it alone — if that record is serving something real, pick another --origin");
} else {
  say(`ok: ${origin} AAAA 100:: (proxied)`);
}

// ---- 2. the fallback origin -------------------------------------------------

const fallback = await cf("GET", `/zones/${zoneId}/custom_hostnames/fallback_origin`).catch(() => undefined);
if (fallback?.origin === origin && fallback?.status === "active") {
  say(`ok: fallback origin is ${origin} (active)`);
} else if (check) {
  say(`MISSING: fallback origin is '${fallback?.origin ?? "unset"}' (${fallback?.status ?? "unset"}), wanted ${origin}`);
} else {
  const set = await cf("PATCH", `/zones/${zoneId}/custom_hostnames/fallback_origin`, { origin });
  say(`fallback origin set to ${origin} (${set?.status ?? "pending"}) — it takes a minute to go active`);
}

// ---- 3. the wildcard Worker route -------------------------------------------

const script = workerName();
const routes = await cf("GET", `/zones/${zoneId}/workers/routes`);
const wildcard = (routes ?? []).find((r) => r.pattern === "*/*");
if (wildcard && wildcard.script === script) {
  say(`ok: route */* → ${script}`);
} else if (wildcard) {
  say(`WARNING: route */* already points at '${wildcard.script}', not '${script}' — not changing it`);
} else if (check) {
  say(`MISSING: route */* → ${script}`);
} else {
  await cf("POST", `/zones/${zoneId}/workers/routes`, { pattern: "*/*", script });
  say(`created route */* → ${script} (covers every custom hostname, present and future)`);
}

// ---- 4. what the deployment itself needs ------------------------------------

if (check) {
  say("check only; nothing was changed");
  process.exit(0);
}

// `CF_ZONE_ID` and `RS2_CNAME_TARGET` are configuration, not secrets, so they
// live with the other per-deployment vars and every `npm run deploy` carries
// them. The API token is a secret and never goes in a file.
let vars = {};
if (existsSync(varsFile)) {
  try {
    vars = JSON.parse(readFileSync(varsFile, "utf8"));
  } catch (e) {
    die(`.deploy.vars.json is not valid JSON: ${e.message}`);
  }
}
vars.CF_ZONE_ID = zoneId;
vars.RS2_CNAME_TARGET = origin;
writeFileSync(varsFile, `${JSON.stringify(vars, null, 2)}\n`, "utf8");
say(`wrote CF_ZONE_ID and RS2_CNAME_TARGET into .deploy.vars.json`);

console.log("");
say("remaining, once each:");
say("  npx wrangler secret put CF_API_TOKEN   # a token with SSL and Certificates:Edit on this zone");
say("  npm run deploy");
console.log("");
say(`then a customer's whole side of it is one record:  CNAME <their host> → ${origin}`);
say(`and yours is:  npm run domain -- add <their host> --tenant <name>`);
