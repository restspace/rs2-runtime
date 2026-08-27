#!/usr/bin/env node
// A tenant worth pointing a UI at, and the way back out.
//
//   RS2_BASE_URL=https://…workers.dev RS2_ADMIN_TOKEN=… npm run ui:up
//   …poke at it in rs2-ui…
//   RS2_BASE_URL=… RS2_ADMIN_TOKEN=… npm run ui:down
//
// `up` backs the tenant's current config up to `.ui-fixture.backup.json`
// (gitignored) before touching anything, then mounts a spread of services with
// some content in them — enough to exercise the browse tree, the Form tab, the
// spec-store authoring panels, the Send console, the log viewer, and the config
// editor. `down` deletes what `up` created and PUTs the backed-up config back.
//
// Not a fixture for tests — `conformance/http` provisions its own mounts and
// would flatten this. It is scaffolding for looking at a UI by hand.
//
// Env:
//   RS2_BASE_URL     the node (default http://127.0.0.1:8787 — local wrangler dev)
//   RS2_ADMIN_TOKEN  the node admin token (gates /admin/*)                [required]
//   RS2_TENANT       tenant name (default "main")
//   RS2_UI_ORIGIN    e.g. http://localhost:5173 — added to cors.allowedOrigins so
//                    a UI running elsewhere may call this node cross-origin
//   RS2_UI_EMAIL     demo admin (default ui@demo.test)
//   RS2_UI_PASSWORD  its password (default ui-demo-pw)

import { existsSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const backupFile = resolve(here, "..", ".ui-fixture.backup.json");

const base = (process.env.RS2_BASE_URL || "http://127.0.0.1:8787").replace(/\/+$/, "");
const adminToken = process.env.RS2_ADMIN_TOKEN;
const tenant = process.env.RS2_TENANT || "main";
const uiOrigin = process.env.RS2_UI_ORIGIN;
const email = process.env.RS2_UI_EMAIL || "ui@demo.test";
const password = process.env.RS2_UI_PASSWORD || "ui-demo-pw";

const command = process.argv[2];
const say = (m) => console.log(`[ui-fixture] ${m}`);
const die = (m) => {
  console.error(`[ui-fixture] ${m}`);
  process.exit(1);
};

if (!["up", "down"].includes(command)) die("usage: ui-fixture.mjs up|down");
if (!adminToken) die("RS2_ADMIN_TOKEN is required (the node admin token that gates /admin/*)");

// ---- the shape this fixture installs --------------------------------------

/// Mounts the fixture owns. Anything else already in the tenant is left alone,
/// and `down` puts the whole config back as it was regardless.
const MOUNTS = [
  { path: "/files", service: "file", config: { access: "open" } },
  { path: "/private", service: "file", config: { access: "authenticated" } },
  { path: "/data", service: "data", config: { access: "open", enforceSchema: true } },
  { path: "/q", service: "query", config: { access: "open" } },
  { path: "/summary", service: "pipeline", config: { access: "open" } },
  { path: "/t", service: "template", config: { access: "open" } },
  { path: "/logs", service: "log", config: { access: { read: "A" } } },
  { path: "/auth", service: "auth", config: { access: "open" } },
  { path: "/services", service: "services", config: { access: { read: "A", write: "A" } } },
];

const OWNED_PATHS = new Set(MOUNTS.map((m) => m.path));

/// Datasets: `.schema.json` first (that is what materializes a dataset), then
/// records. The schemas exist so the UI's Form tab has something to render.
const DATASETS = {
  notes: {
    schema: {
      type: "object",
      title: "Note",
      required: ["title"],
      properties: {
        title: { type: "string", title: "Title" },
        body: { type: "string", title: "Body" },
        tags: { type: "array", title: "Tags", items: { type: "string" } },
        published: { type: "boolean", title: "Published", default: false },
      },
    },
    records: {
      welcome: { title: "Welcome", body: "First note. Edit me in the Form tab.", tags: ["demo"], published: true },
      "release-plan": { title: "Release plan", body: "Cut a build, run the suite, deploy.", tags: ["ops", "demo"], published: false },
      scratch: { title: "Scratch", body: "", tags: [], published: false },
    },
  },
  // The query below reads this dataset, so the two stay in step.
  orders: {
    schema: {
      type: "object",
      title: "Order",
      required: ["status", "total"],
      properties: {
        status: { type: "string", title: "Status", enum: ["open", "closed"] },
        total: { type: "number", title: "Total" },
        name: { type: "string", title: "Customer" },
      },
    },
    records: {
      "o-1001": { status: "open", total: 50, name: "Ada" },
      "o-1002": { status: "open", total: 10, name: "Grace" },
      "o-1003": { status: "closed", total: 99, name: "Alan" },
    },
  },
};

/// Files, as [path, body, content-type].
const FILES = [
  [
    "/files/docs/readme.md",
    "# rs2 UI demo\n\nThis tenant was seeded by `scripts/ui-fixture.mjs`.\n\n- `/files` — this tree (open)\n- `/private` — the same, but sign-in required\n- `/data` — `notes` and `orders`, both schema'd (try the **Form** tab)\n- `/q` — a stored query over `orders` (**Send** it)\n- `/summary` — a pipeline spec\n- `/t` — an empty template store to author into\n- `/logs` — the log viewer\n\nTear it all down with `npm run ui:down`.\n",
    "text/markdown",
  ],
  ["/files/docs/notes.txt", "Plain text, for the text viewer.\n", "text/plain"],
  ["/files/data/sample.json", JSON.stringify({ hello: "world", items: [1, 2, 3], nested: { a: true } }, null, 2), "application/json"],
  [
    "/files/img/logo.svg",
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 120 120" width="120" height="120"><rect width="120" height="120" rx="16" fill="#0f6f6f"/><text x="60" y="72" font-family="monospace" font-size="34" fill="#fff" text-anchor="middle">rs2</text></svg>\n',
    "image/svg+xml",
  ],
  ["/private/secret.txt", "Only a signed-in principal can read this.\n", "text/plain"],
];

/// A stored query over the `orders` dataset — authored the way the UI would
/// author it, under the mount's reserved `.queries` subtree.
const QUERY_SPEC = {
  path: "/q/.queries/open-orders",
  body: {
    query: {
      dataset: "orders",
      where: { status: "${status}", total: { op: ">=", value: "${min}" } },
      orderBy: "total",
    },
    params: {
      type: "object",
      required: ["status"],
      properties: { status: { type: "string", default: "open" }, min: { type: "number", default: 0 } },
    },
    output: { type: "array" },
  },
};

/// A pipeline spec at the mount root: `GET /summary/<id>` summarizes an order.
const PIPELINE_SPEC = {
  path: "/summary/.pipelines/.root",
  body: { pipeline: ["GET /data/orders/${url.rest}", { customer: "$.name", status: "$.status", total: "$.total" }] },
};

// ---- HTTP ------------------------------------------------------------------

async function call(method, path, opts = {}) {
  const headers = { ...(opts.headers ?? {}) };
  let body = opts.body;
  if (opts.json !== undefined) {
    body = JSON.stringify(opts.json);
    headers["content-type"] = "application/json";
  }
  if (opts.token) headers.authorization = `Bearer ${opts.token}`;
  if (opts.admin) headers.authorization = `Bearer ${adminToken}`;
  const res = await fetch(`${base}${path}`, { method, headers, body });
  const text = await res.text();
  return { status: res.status, text, etag: res.headers.get("etag"), json: () => JSON.parse(text) };
}

/// Expect one of `want`, else fail with the server's own words.
function expect(res, want, what) {
  const ok = Array.isArray(want) ? want.includes(res.status) : res.status === want;
  if (!ok) die(`${what}: expected ${JSON.stringify(want)}, got ${res.status} ${res.text.slice(0, 300)}`);
  return res;
}

/// Teardown must not stop at the first surprise: a fixture half-removed with
/// the old config not yet back is worse than a noisy success. Anything other
/// than "gone" is collected and reported at the end.
const warnings = [];
const tolerate = (res, what) => {
  if (![200, 201, 204, 404].includes(res.status)) warnings.push(`${what}: ${res.status} ${res.text.slice(0, 160)}`);
};

async function login() {
  const res = await call("POST", "/auth/login", { json: { email, password } });
  expect(res, 200, `login as ${email}`);
  return res.json().token;
}

/// The tenant's registered domains, so a config PUT does not drop them.
async function domainsOf() {
  const res = await call("GET", "/admin/tenants", { admin: true });
  expect(res, 200, "GET /admin/tenants");
  return res.json().tenants?.find((t) => t.name === tenant)?.domains ?? [];
}

async function putTenant(config, domains, opts = {}) {
  const current = await call("GET", `/admin/tenants/${tenant}`, { admin: true });
  const headers = current.status === 200 && current.etag ? { "if-match": current.etag } : {};
  const body = { config, domains };
  if (opts.bootstrapAdmin) body.bootstrapAdmin = { email, password };
  const res = await call("PUT", `/admin/tenants/${tenant}`, { admin: true, json: body, headers });
  expect(res, [200, 201], `PUT /admin/tenants/${tenant}`);
  return res.status === 201;
}

// ---- up --------------------------------------------------------------------

async function up() {
  if (existsSync(backupFile)) {
    die(`${backupFile} already exists — this tenant is already fixtured. Run 'npm run ui:down' first (or delete the file if you know it is stale).`);
  }

  const existing = await call("GET", `/admin/tenants/${tenant}`, { admin: true });
  const created = existing.status === 404;
  if (!created) expect(existing, 200, `GET /admin/tenants/${tenant}`);
  const previous = created ? null : existing.json();
  const domains = created ? [] : await domainsOf();
  writeFileSync(backupFile, JSON.stringify({ tenant, base, created, config: previous, domains }, null, 2));
  say(created ? `tenant '${tenant}' does not exist — it will be created (and deleted by 'down')` : `backed up '${tenant}' config to ${backupFile}`);

  // Build on what is there: keep foreign mounts, keep the auth secret (else a
  // fresh one), and add the UI origin to the CORS allow-list when given.
  const config = previous ? structuredClone(previous) : {};
  config.auth = { userDataset: "users", ...(config.auth ?? {}) };
  if (!config.auth.jwtSecret || config.auth.jwtSecret === "***") {
    config.auth.jwtSecret = [...crypto.getRandomValues(new Uint8Array(24))].map((b) => b.toString(16).padStart(2, "0")).join("");
    say("generated a new auth.jwtSecret");
  }
  config.operatorRoles = config.operatorRoles ?? "A";
  if (uiOrigin) {
    const cors = { ...(config.cors ?? {}) };
    cors.allowedOrigins = [...new Set([...(cors.allowedOrigins ?? []), uiOrigin])];
    config.cors = cors;
    say(`cors.allowedOrigins += ${uiOrigin} (bearer-token auth; no cross-site cookie)`);
  }
  const foreign = (config.mounts ?? []).filter((m) => !OWNED_PATHS.has(m.path));
  config.mounts = [...foreign, ...MOUNTS];

  await putTenant(config, domains, { bootstrapAdmin: true });
  say(`tenant '${tenant}' configured: ${MOUNTS.map((m) => m.path).join(" ")}`);

  const token = await login();
  say(`signed in as ${email}`);

  for (const [dataset, { schema, records }] of Object.entries(DATASETS)) {
    expect(await call("PUT", `/data/${dataset}/.schema.json`, { token, json: schema }), [200, 201], `PUT /data/${dataset}/.schema.json`);
    for (const [key, value] of Object.entries(records)) {
      expect(await call("PUT", `/data/${dataset}/${key}`, { token, json: value }), [200, 201], `PUT /data/${dataset}/${key}`);
    }
    say(`dataset ${dataset}: schema + ${Object.keys(records).length} records`);
  }

  for (const [path, body, contentType] of FILES) {
    expect(await call("PUT", path, { token, body, headers: { "content-type": contentType } }), [200, 201], `PUT ${path}`);
  }
  say(`${FILES.length} files`);

  for (const spec of [QUERY_SPEC, PIPELINE_SPEC]) {
    expect(await call("PUT", spec.path, { token, json: spec.body }), [200, 201], `PUT ${spec.path}`);
  }
  say("query + pipeline specs");

  // Prove the two executable surfaces actually run, so a UI that shows nothing
  // is the UI's problem and not the fixture's.
  const q = await call("GET", "/q/open-orders?status=open", { token });
  expect(q, 200, "GET /q/open-orders");
  say(`query returns ${q.json().length} open orders`);
  const p = await call("GET", "/summary/o-1001", { token });
  expect(p, 200, "GET /summary/o-1001");
  say(`pipeline returns ${p.text.slice(0, 60)}`);

  console.log("");
  say(`ready at ${base}`);
  say(`sign in with ${email} / ${password}`);
  console.log("");
  console.log("  rs2-ui/.env — through the dev proxy (no CORS involved):");
  console.log("    VITE_RS2_API=");
  console.log(`    RS2_DEV_PROXY_TARGET=${base}`);
  console.log("");
  console.log("  …or straight at the node (needs RS2_UI_ORIGIN set above):");
  console.log(`    VITE_RS2_API=${base}`);
  console.log("");
  say("tear down with: npm run ui:down");
}

// ---- down ------------------------------------------------------------------

async function down() {
  if (!existsSync(backupFile)) die(`no ${backupFile} — nothing recorded to restore (run 'up' first)`);
  const backup = JSON.parse(readFileSync(backupFile, "utf8"));
  if (backup.base !== base) die(`backup is for ${backup.base}, but RS2_BASE_URL is ${base}`);

  // Content first: once the config is restored the mounts may be gone.
  let token;
  const probe = await call("POST", "/auth/login", { json: { email, password } });
  token = probe.status === 200 ? probe.json().token : undefined;
  if (token) {
    for (const spec of [QUERY_SPEC, PIPELINE_SPEC]) {
      tolerate(await call("DELETE", spec.path, { token }), `DELETE ${spec.path}`);
    }
    for (const dataset of Object.keys(DATASETS)) {
      tolerate(await call("DELETE", `/data/${dataset}/?confirm=${dataset}`, { token }), `DELETE /data/${dataset}/`);
    }
    // Files one by one, not by directory: `/private/secret.txt` sits at a
    // mount root, and a mount root is not a container you may delete (409
    // "directory is not empty"). Keys are what exist in a store anyway —
    // remove them and the directories they implied go with them.
    for (const [path] of FILES) {
      tolerate(await call("DELETE", path, { token }), `DELETE ${path}`);
    }
    const userDataset = backup.config?.auth?.userDataset ?? "users";
    tolerate(await call("DELETE", `/data/${userDataset}/${encodeURIComponent(email)}`, { token }), `DELETE the ${email} record`);
    say("deleted the fixture's content");
  } else {
    say(`WARNING: could not sign in as ${email} — leaving content in place, restoring the config only`);
  }

  if (backup.created) {
    const res = await call("DELETE", `/admin/tenants/${tenant}?confirm=${tenant}`, { admin: true });
    expect(res, [204, 404], `DELETE /admin/tenants/${tenant}`);
    say(`deleted tenant '${tenant}' (this fixture created it)`);
  } else {
    await putTenant(backup.config, backup.domains ?? []);
    say(`restored '${tenant}' to its pre-fixture config`);
  }

  unlinkSync(backupFile);
  if (warnings.length) {
    say(`done, with ${warnings.length} thing(s) left behind:`);
    for (const w of warnings) console.log(`  - ${w}`);
  } else {
    say("done");
  }
}

await (command === "up" ? up() : down());
