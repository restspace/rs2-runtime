#!/usr/bin/env node
// The fixture catalogue server for `catalogue.test.ts` (spec F.3).
//
//   RS2_CATALOGUE_PORT=3411 node fixtures/catalogue-server.mjs
//
// It serves one catalogue document and the bundles it advertises, so the
// live half of the suite exercises the real fetch → hash-pin →
// compile-check → store path of `POST /services/catalogue/install`. The
// host under test must have this server's host on its operator allowlist
// (`catalogueHosts` in serverConfig.json) — otherwise the suite runs its
// dormant half instead.
//
// Contract the suite relies on (`/cat.json`):
//   greeter  a JS service item whose `version` IS the content hash of its
//            bundle  → install succeeds
//   tamper   a JS service item whose `version` is NOT its bundle's hash
//            → install must refuse with 400 (the hash pin)
//
// Point the runner at it with
//   RS2_CATALOGUE_URL=http://127.0.0.1:3411/cat.json

import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { argv } from "node:process";
import { fileURLToPath } from "node:url";

/** `rs2_core::services::code::version_of`: sha256, first 8 bytes, hex. */
export function versionOf(bytes) {
  return createHash("sha256").update(bytes).digest("hex").slice(0, 16);
}

const GREETER = 'export default (msg, ctx) => ({ status: 200, body: "hello from the catalogue" });\n';
const TAMPER = 'export default (msg, ctx) => ({ status: 200, body: "tampered" });\n';
/** Deliberately not `versionOf(TAMPER)` — the hash pin must catch it. */
const TAMPER_DECLARED = "0123456789abcdef";

export function catalogueDoc(base) {
  return {
    items: [
      {
        name: "greeter",
        kind: "service",
        engine: "js",
        version: versionOf(GREETER),
        bundleUrl: `${base}/greeter.js`,
        description: "a greeter",
      },
      {
        name: "tamper",
        kind: "service",
        engine: "js",
        version: TAMPER_DECLARED,
        bundleUrl: `${base}/tamper.js`,
        description: "a bundle whose declared version lies",
      },
    ],
  };
}

const port = Number(process.env.RS2_CATALOGUE_PORT || 3411);
const host = process.env.RS2_CATALOGUE_BIND || "127.0.0.1";

const server = createServer((req, res) => {
  const base = `http://${req.headers.host || `${host}:${port}`}`;
  const path = new URL(req.url, base).pathname;
  const send = (status, body, type) => {
    res.writeHead(status, { "content-type": type, "content-length": Buffer.byteLength(body) });
    res.end(body);
  };
  switch (path) {
    case "/cat.json":
      return send(200, JSON.stringify(catalogueDoc(base)), "application/json");
    case "/greeter.js":
      return send(200, GREETER, "application/javascript");
    case "/tamper.js":
      return send(200, TAMPER, "application/javascript");
    case "/not-json":
      return send(200, "this is not a catalogue document", "application/json");
    default:
      return send(404, "not found", "text/plain");
  }
});

// Only listen when run as a program: the suite imports `versionOf`/`catalogueDoc`.
if (argv[1] && fileURLToPath(import.meta.url) === argv[1]) {
  server.listen(port, host, () => {
    console.log(`[catalogue-server] http://${host}:${port}/cat.json`);
  });
}
