// Generate the extension <-> media type tables for BOTH hosts from `mime-db`
// (the IANA + Apache + nginx aggregate that also backs Express/`mime-types`):
//   rs2-core/src/message/media_type_table.rs
//   rs2-worker/src/runtime/media-type-table.ts
// Re-run after bumping `mime-db`, and commit both outputs together:
//   npm run gen:media-types
//
// Both outputs are generated: edit this script and the pins below, never them.
// Generating both from one source is the point — 1200 rows hand-synced across
// two languages would drift, and a divergence there is a divergence in what a
// client sees.
//
// mime-db is MIT (Douglas Christopher Wilson, Jonathan Ong); the data it
// aggregates comes from IANA, Apache and nginx.
import { writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const db = require("mime-db");
const dbVersion = require("mime-db/package.json").version;
const here = dirname(fileURLToPath(import.meta.url));
const repo = join(here, "..", "..");

// ---------------------------------------------------------------------------
// Pins — RS2's answer where it differs from the aggregate, or where mime-db
// has no opinion. These win over everything mime-db says.
// ---------------------------------------------------------------------------

/// Extensions RS2 decides itself. `ts`/`tsx` are the load-bearing ones: to
/// mime-db (following nginx) a `.ts` is an MPEG transport stream, but in a
/// tenant's files a `.ts` is TypeScript source, and serving someone's source
/// as video would be absurd. `xml` stays `application/xml` (the aggregate
/// prefers `text/xml`): it is what RS2 has always answered.
const EXTENSION_PINS = {
  html: "text/html",
  htm: "text/html",
  md: "text/markdown",
  txt: "text/plain",
  json: "application/json",
  xml: "application/xml",
  yaml: "application/yaml",
  yml: "application/yaml",
  csv: "text/csv",
  css: "text/css",
  js: "text/javascript",
  mjs: "text/javascript",
  jsx: "text/javascript",
  ts: "application/typescript",
  tsx: "application/typescript",
  svg: "image/svg+xml",
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  gif: "image/gif",
  webp: "image/webp",
  pdf: "application/pdf",
  zip: "application/zip",
  wasm: "application/wasm",
};

/// Conflicts the resolution rule below gets wrong for a file server. `.rtf`
/// and `.mpp` are obscure IANA registrations beating the type everyone
/// actually means (`text/rtf` makes a browser render RTF markup as text);
/// `.wav`, `.flac` and `.m4v` prefer the type browsers accept most widely
/// over Apache's `x-` spelling and Apple's variant.
const CONFLICT_PINS = {
  rtf: "application/rtf",
  mpp: "application/vnd.ms-project",
  wav: "audio/wav",
  flac: "audio/flac",
  m4v: "video/mp4",
};

/// The extension a server-named file gets for a media type (keyless POST),
/// where the first-listed extension is not the one people expect.
const CANONICAL_PINS = {
  "image/jpeg": "jpg",
  "audio/mpeg": "mp3", // not `.mpga`
  "audio/aac": "aac", // not `.adts`
  "audio/ogg": "ogg", // not `.oga`
  "video/quicktime": "mov", // not `.qt`
  "text/javascript": "js",
  "application/typescript": "ts",
  "application/xml": "xml",
  "application/yaml": "yaml",
  "text/html": "html",
  "text/markdown": "md",
  "text/plain": "txt",
  "application/json": "json",
};

/// The friendly-URL probe order: the extensions the file service appends to an
/// extension-less request, best first. Deliberately short — every entry costs
/// a store probe on a miss, and nobody writes `/clip` meaning `/clip.mp4`.
const NEGOTIABLE = [
  "html", "htm", "md", "txt", "json", "xml", "yaml", "yml", "csv", "css",
  "js", "mjs", "jsx", "ts", "tsx", "svg", "png", "jpg", "jpeg", "gif",
  "webp", "pdf", "zip", "wasm",
];

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

// An extension can be claimed by several types (47 are). Resolve as
// `mime-types` does — the higher-authority source wins (iana > undefined >
// apache > nginx), first listed wins a tie, `application/octet-stream` never
// wins — with one deliberate departure: on a tie a concrete top-level type
// (audio/video/image/font/text/model) beats `application/*`.
//
// That clause is why it is not a straight port. mime-db 1.53 gave
// `application/mp4` the `mp4` extension, and mime-types' rule keeps the
// incumbent `application/*`, so `.mp4` would serve as `application/mp4` and no
// browser would play it. What a client needs is the concrete type; the generic
// container registration is the wrong answer even when IANA lists both.
const PREFERENCE = ["nginx", "apache", undefined, "iana"];
const isApplication = (type) => type.startsWith("application/");

const types = Object.create(null); // extension -> essence
for (const [type, mime] of Object.entries(db)) {
  for (const ext of mime.extensions ?? []) {
    const held = types[ext];
    if (held !== undefined && held !== "application/octet-stream") {
      const from = PREFERENCE.indexOf(db[held].source);
      const to = PREFERENCE.indexOf(mime.source);
      if (from > to) continue;
      // On a tie, keep the incumbent unless this is an application/* -> concrete upgrade.
      if (from === to && !(isApplication(held) && !isApplication(type))) continue;
    }
    types[ext] = type;
  }
}
for (const [ext, essence] of Object.entries(CONFLICT_PINS)) types[ext] = essence;
for (const [ext, essence] of Object.entries(EXTENSION_PINS)) types[ext] = essence;

// The reverse map. A type's canonical extension must map back to that same
// type, or a keyless POST would name a file that then serves as something
// else — so take the first extension that round-trips, and if none does, the
// type gets no extension at all.
const canonical = Object.create(null); // essence -> extension
for (const [type, mime] of Object.entries(db)) {
  const pin = CANONICAL_PINS[type];
  const exts = pin ? [pin, ...(mime.extensions ?? [])] : (mime.extensions ?? []);
  const hit = exts.find((ext) => types[ext] === type);
  if (hit !== undefined) canonical[type] = hit;
}
for (const [type, ext] of Object.entries(CANONICAL_PINS)) {
  if (types[ext] !== type) {
    throw new Error(`canonical pin ${type} -> .${ext} contradicts the map (.${ext} is ${types[ext]})`);
  }
  canonical[type] = ext;
}
// A pinned type mime-db does not list at all still needs a reverse entry.
for (const [ext, type] of Object.entries({ ...CONFLICT_PINS, ...EXTENSION_PINS })) {
  if (canonical[type] === undefined) canonical[type] = CANONICAL_PINS[type] ?? ext;
}

for (const ext of NEGOTIABLE) {
  if (types[ext] === undefined) throw new Error(`negotiable .${ext} has no media type`);
}

// Sorted for binary search: a directory listing maps one path per entry, so
// lookup sits on the hot path.
const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
const extRows = Object.entries(types).sort(([a], [b]) => cmp(a, b));
const canonRows = Object.entries(canonical).sort(([a], [b]) => cmp(a, b));

const banner = [
  `// @generated by rs2-worker/scripts/gen-media-types.mjs from mime-db ${dbVersion}`,
  `// (IANA + Apache + nginx; MIT). Do not edit: run \`npm run gen:media-types\``,
  `// in rs2-worker/ and commit both hosts' tables together.`,
].join("\n");

const preamble = (marker) => `${banner}
${marker}
${marker} The extension map, generated for both hosts from one source so they
${marker} cannot disagree about what a client is served. The hand-written half
${marker} is media_type.rs / media-type.ts: this file is only the data.
`;

const rustOut = `${preamble("//!")}
/// \`(extension, essence)\`, sorted by extension for binary search.
pub(super) const EXTENSION_TABLE: &[(&str, &str)] = &[
${extRows.map(([ext, essence]) => `    ("${ext}", "${essence}"),`).join("\n")}
];

/// \`(essence, canonical extension)\`, sorted by essence for binary search.
/// Every entry round-trips: the extension maps back to the same essence.
pub(super) const CANONICAL_EXTENSIONS: &[(&str, &str)] = &[
${canonRows.map(([essence, ext]) => `    ("${essence}", "${ext}"),`).join("\n")}
];

/// The friendly-URL probe order for an extension-less request, best first.
pub(super) const NEGOTIABLE_EXTENSIONS: &[&str] = &[
${NEGOTIABLE.map((ext) => `    "${ext}",`).join("\n")}
];
`;

const tsOut = `${preamble("//")}
/// \`[extension, essence]\`, sorted by extension for binary search.
export const EXTENSION_TABLE: ReadonlyArray<readonly [string, string]> = [
${extRows.map(([ext, essence]) => `  ["${ext}", "${essence}"],`).join("\n")}
];

/// \`[essence, canonical extension]\`, sorted by essence for binary search.
/// Every entry round-trips: the extension maps back to the same essence.
export const CANONICAL_EXTENSIONS: ReadonlyArray<readonly [string, string]> = [
${canonRows.map(([essence, ext]) => `  ["${essence}", "${ext}"],`).join("\n")}
];

/// The friendly-URL probe order for an extension-less request, best first.
export const NEGOTIABLE_EXTENSIONS: readonly string[] = [
${NEGOTIABLE.map((ext) => `  "${ext}",`).join("\n")}
];
`;

const rustPath = join(repo, "rs2-core", "src", "message", "media_type_table.rs");
const tsPath = join(repo, "rs2-worker", "src", "runtime", "media-type-table.ts");
writeFileSync(rustPath, rustOut);
writeFileSync(tsPath, tsOut);
console.log(
  `mime-db ${dbVersion}: ${extRows.length} extensions, ${canonRows.length} canonical, ${NEGOTIABLE.length} negotiable`,
);
console.log(`  ${rustPath}`);
console.log(`  ${tsPath}`);
