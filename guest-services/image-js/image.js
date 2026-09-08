// rs2-image (JS) — query-string image resize/crop over the host `images`
// capability. The same mount contract as the Wasm component in
// `../image` (same URLs, params, config, headers, cache layout), but the
// pixel work happens in the host: this bundle only canonicalizes,
// derives the cache key, and asks the host to transform *by reference*.
// No image bytes ever enter the sandbox, so a JS bundle (string bodies)
// serves binary derivatives.
//
// Mount:
//   { "path": "/img", "service": "code:image@<version>", "config": {
//       "access": { "read": "all", "delete": "A" },
//       "grants": {
//         "source": { "prefix": "/files" },
//         "cache":  { "type": "store", "root": "img-cache" },
//         "images": { "type": "images", "source": "source", "cache": "cache" }
//       },
//       "widths": [320, 640, 960, 1280, 1920], "defaultQuality": 78,
//       "maxWidth": 4096, "maxHeight": 4096, "maxSourcePixels": 16000000,
//       "caching": { "mode": "cache", "maxAgeSeconds": 86400, "public": true }
//   } }
//
// Flow per GET (the steady state is free of pixel work):
//   1. parse + canonicalize (bad input is a 400 before any I/O)
//   2. HEAD the source through `source` (caller's authz; its ETag versions
//      the cache key)
//   3. derived ETag = sha256(path, source ETag, canonical params) — an
//      `If-None-Match` hit is a 304 with no further work
//   4. HEAD the derivative in `cache` — a hit answers with
//      `x-rs2-body-ref: cache:<path>`, streamed by the host
//   5. only a miss transforms: `images:/transform?…&store=<path>` reads the
//      original, transforms, and writes the derivative host-side; the
//      reply is then the same body-ref. If the cache write failed the
//      body-ref points at `images:/transform?…` without `store`, which
//      transforms again and streams inline (`x-img-cache: miss,nostore`).
//
// Every host capability is `await`ed: a no-op on the Rust V8 engine,
// required on the Cloudflare host (`guest-async`).

// ---- config -----------------------------------------------------------------

const DEFAULTS = { maxWidth: 4096, maxHeight: 4096, maxSourcePixels: 16_000_000, defaultQuality: 78, widths: [] };

/// Mount-config knobs (absent keys keep defaults; wrong-typed keys are a
/// config error). Mirrors `params.rs::Config::from_json`.
export function configFrom(cfg) {
  const out = { ...DEFAULTS, widths: [] };
  cfg = cfg && typeof cfg === "object" ? cfg : {};
  const uint = (key) => {
    const v = cfg[key];
    if (v === undefined) return undefined;
    if (!Number.isInteger(v) || v <= 0) throw new Error(`config '${key}' must be a positive integer`);
    return v;
  };
  for (const key of ["maxWidth", "maxHeight", "maxSourcePixels"]) {
    const v = uint(key);
    if (v !== undefined) out[key] = v;
  }
  const q = uint("defaultQuality");
  if (q !== undefined) {
    if (q > 100) throw new Error("config 'defaultQuality' must be 1..=100");
    out.defaultQuality = q;
  }
  if (cfg.widths !== undefined) {
    const arr = cfg.widths;
    if (!Array.isArray(arr) || !arr.every((v) => Number.isInteger(v) && v > 0)) {
      throw new Error("config 'widths' must be an array of positive integers");
    }
    out.widths = [...new Set(arr)].sort((a, b) => a - b);
  }
  return out;
}

// ---- params -----------------------------------------------------------------

const COMPASS = {
  center: [0.5, 0.5], c: [0.5, 0.5],
  n: [0.5, 0], ne: [1, 0], e: [1, 0.5], se: [1, 1], s: [0.5, 1], sw: [0, 1], w: [0, 0.5], nw: [0, 0],
};

function pctDecode(s) {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

function parseU32(key, v) {
  if (!/^\d+$/.test(v) || Number(v) === 0) throw new Error(`'${key}' must be a positive integer, got '${v}'`);
  return Number(v);
}

/// Parse a query string. `null` means no transform parameters at all — the
/// passthrough path. Unknown keys are a hard 400 (they would fragment the
/// cache and hide typos). Mirrors `params.rs::parse`.
export function parse(query, cfg) {
  let w, h, fit, g, rect, format, quality;
  let dpr = 1;
  let any = false;
  for (const pair of query.split("&").filter((p) => p !== "")) {
    const eq = pair.indexOf("=");
    const key = eq < 0 ? pair : pair.slice(0, eq);
    const value = pctDecode(eq < 0 ? "" : pair.slice(eq + 1));
    any = true;
    switch (key) {
      case "w":
        w = parseU32("w", value);
        break;
      case "h":
        h = parseU32("h", value);
        break;
      case "dpr": {
        const d = Number(value);
        if (value === "" || !Number.isFinite(d) || d <= 0) throw new Error(`'dpr' must be a positive number, got '${value}'`);
        dpr = Math.min(Math.max(d, 1), 3);
        break;
      }
      case "fit":
        if (!["scale-down", "contain", "cover", "fill"].includes(value)) throw new Error(`unknown fit '${value}'`);
        fit = value;
        break;
      case "g": {
        const compass = COMPASS[value];
        if (compass) {
          g = { x: compass[0], y: compass[1] };
          break;
        }
        const parts = value.split(",");
        const x = Number(parts[0]);
        const y = Number(parts[1]);
        if (parts.length !== 2 || parts[0] === "" || parts[1] === "" || !(x >= 0 && x <= 1) || !(y >= 0 && y <= 1)) {
          throw new Error(`'g' must be a compass point or 'x,y' fractions in 0..=1, got '${value}'`);
        }
        g = { x, y };
        break;
      }
      case "rect": {
        const parts = value.split(",");
        if (parts.length !== 4 || !/^\d+$/.test(parts[0]) || !/^\d+$/.test(parts[1])) {
          throw new Error(`'rect' must be 'x,y,w,h', got '${value}'`);
        }
        rect = { x: Number(parts[0]), y: Number(parts[1]), w: parseU32("rect", parts[2]), h: parseU32("rect", parts[3]) };
        break;
      }
      case "f":
      case "format":
        if (value === "auto") format = "auto";
        else if (value === "jpeg" || value === "jpg") format = "jpeg";
        else if (value === "png") format = "png";
        else if (value === "webp") format = "webp";
        else throw new Error(`unknown format '${value}'`);
        break;
      case "q":
        if (!/^\d+$/.test(value) || Number(value) < 1 || Number(value) > 100) throw new Error(`'q' must be 1..=100, got '${value}'`);
        quality = Number(value);
        break;
      default:
        throw new Error(`unknown parameter '${key}'`);
    }
  }
  if (!any) return null;

  // Fold dpr into the requested box, then clamp to the mount's ceiling.
  const scale = (v) => Math.max(Math.round(v * dpr), 1);
  if (w !== undefined) w = Math.min(scale(w), cfg.maxWidth);
  if (h !== undefined) h = Math.min(scale(h), cfg.maxHeight);

  fit = fit ?? "scale-down";
  if (fit === "cover" || fit === "fill") {
    if (w === undefined || h === undefined) throw new Error(`fit=${fit} requires both 'w' and 'h'`);
  } else if (w === undefined && h === undefined && rect === undefined) {
    throw new Error("resizing requires 'w' or 'h' (or a 'rect' crop)");
  }

  // Width-only requests snap up the configured ladder.
  if (h === undefined && w !== undefined && cfg.widths.length > 0) {
    const rung = cfg.widths.find((r) => r >= w) ?? cfg.widths[cfg.widths.length - 1];
    w = Math.min(rung, cfg.maxWidth);
  }

  return { w, h, fit, g: g ?? { x: 0.5, y: 0.5 }, rect, format: format ?? "auto", quality: quality ?? cfg.defaultQuality };
}

/// Resolve `f=auto` against the source media type without decoding:
/// sources that may carry alpha (png/gif/webp) stay PNG; photographic
/// sources become JPEG. Returns `[format, mediaType, extension]`.
export function resolveFormat(f, sourceMediaType) {
  const concrete = f === "auto" ? (["image/png", "image/gif", "image/webp"].includes(sourceMediaType) ? "png" : "jpeg") : f;
  return concrete === "jpeg" ? ["jpeg", "image/jpeg", "jpg"] : concrete === "png" ? ["png", "image/png", "png"] : ["webp", "image/webp", "webp"];
}

/// The canonical parameter string: fixed key order, defaults omitted, the
/// *resolved* format included — the cache-key component that makes every
/// equivalent request share one derivative. Byte-identical to the Wasm
/// component's, so the two bundles share cache entries and ETags.
export function canonical(p, resolved) {
  const parts = [];
  if (p.w !== undefined) parts.push(`w=${p.w}`);
  if (p.h !== undefined) parts.push(`h=${p.h}`);
  if (p.fit !== "scale-down") parts.push(`fit=${p.fit}`);
  if (p.g.x !== 0.5 || p.g.y !== 0.5) parts.push(`g=${p.g.x},${p.g.y}`);
  if (p.rect) parts.push(`rect=${p.rect.x},${p.rect.y},${p.rect.w},${p.rect.h}`);
  parts.push(`q=${p.quality}`, `f=${resolved}`);
  return parts.join("&");
}

// ---- sha-256 (pure JS: the Rust engine's prelude has no `crypto.subtle`) ----

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

export function sha256Hex(text) {
  const bytes = new TextEncoder().encode(text);
  const bitLen = bytes.length * 8;
  const padded = new Uint8Array(((bytes.length + 9 + 63) >> 6) << 6);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 4, bitLen >>> 0);
  view.setUint32(padded.length - 8, Math.floor(bitLen / 0x100000000));
  const H = new Uint32Array([0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19]);
  const W = new Uint32Array(64);
  const rotr = (x, n) => (x >>> n) | (x << (32 - n));
  for (let off = 0; off < padded.length; off += 64) {
    for (let i = 0; i < 16; i++) W[i] = view.getUint32(off + i * 4);
    for (let i = 16; i < 64; i++) {
      const s0 = rotr(W[i - 15], 7) ^ rotr(W[i - 15], 18) ^ (W[i - 15] >>> 3);
      const s1 = rotr(W[i - 2], 17) ^ rotr(W[i - 2], 19) ^ (W[i - 2] >>> 10);
      W[i] = (W[i - 16] + s0 + W[i - 7] + s1) >>> 0;
    }
    let [a, b, c, d, e, f, g, h] = H;
    for (let i = 0; i < 64; i++) {
      const S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
      const ch = (e & f) ^ (~e & g);
      const t1 = (h + S1 + ch + K[i] + W[i]) >>> 0;
      const S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const t2 = (S0 + maj) >>> 0;
      h = g; g = f; f = e; e = (d + t1) >>> 0; d = c; c = b; b = a; a = (t1 + t2) >>> 0;
    }
    H[0] = (H[0] + a) >>> 0; H[1] = (H[1] + b) >>> 0; H[2] = (H[2] + c) >>> 0; H[3] = (H[3] + d) >>> 0;
    H[4] = (H[4] + e) >>> 0; H[5] = (H[5] + f) >>> 0; H[6] = (H[6] + g) >>> 0; H[7] = (H[7] + h) >>> 0;
  }
  return Array.from(H, (x) => x.toString(16).padStart(8, "0")).join("");
}

// ---- the service --------------------------------------------------------------

function jsonError(status, code, detail) {
  return { status, body: { code, detail } };
}

function header(headers, name) {
  if (!headers) return undefined;
  for (const [k, v] of Object.entries(headers)) if (k.toLowerCase() === name) return v;
  return undefined;
}

function is2xx(r) {
  return r && r.status >= 200 && r.status < 300;
}

function ifNoneMatchHits(inm, etag) {
  return inm
    .split(",")
    .map((c) => c.trim())
    .some((c) => c === "*" || c.replace(/^W\//, "") === etag);
}

/// Capability call; host errors (thrown with `code`/`status`) become the
/// structured responses the Wasm component emits for the same failures.
async function call(ctx, capability, req) {
  try {
    return await ctx.request(capability, req);
  } catch (e) {
    const code = e && e.code;
    if (code === "capability_denied") return jsonError(403, "capability_denied", String(e.message));
    if (code === "limit_exceeded") return jsonError(503, "limit_exceeded", String(e.message));
    return jsonError(502, "internal", String(e && e.message ? e.message : e));
  }
}

/// The `images` capability's transform query for a canonicalized request:
/// the RS2 vocabulary with the resolved format, plus the guards.
function transformQuery(sub, p, resolved, cfg) {
  const q = [`source=${encodeURIComponent(sub)}`];
  if (p.w !== undefined) q.push(`w=${p.w}`);
  if (p.h !== undefined) q.push(`h=${p.h}`);
  q.push(`fit=${p.fit}`);
  if (p.fit === "cover") q.push(`g=${p.g.x},${p.g.y}`);
  if (p.rect) q.push(`rect=${p.rect.x},${p.rect.y},${p.rect.w},${p.rect.h}`);
  q.push(`f=${resolved}`, `q=${p.quality}`, `maxSourcePixels=${cfg.maxSourcePixels}`);
  return q.join("&");
}

export default async function handle(msg, ctx) {
  let cfg;
  try {
    cfg = configFrom(ctx.config);
  } catch (e) {
    return jsonError(500, "internal", e.message);
  }

  const qmark = msg.url.indexOf("?");
  const path = qmark < 0 ? msg.url : msg.url.slice(0, qmark);
  const query = qmark < 0 ? "" : msg.url.slice(qmark + 1);
  const base = header(msg.headers, "x-rs2-base-path") ?? "/";
  let sub = base === "/" ? path : path.startsWith(base) ? path.slice(base.length) : path;
  if (sub === "") sub = "/";

  // Operator purge of the derivative cache: the mount's `delete` access
  // role has already been enforced by dispatch.
  if (msg.method === "DELETE") {
    if (sub !== "/.cache") return jsonError(405, "bad_request", "only DELETE /.cache?confirm=");
    if (!query.startsWith("confirm=")) return jsonError(409, "conflict", "purging the derivative cache requires ?confirm=");
    const d = await call(ctx, "cache", { method: "DELETE", url: "/d/?confirm=d" });
    return is2xx(d) || d.status === 404 ? { status: 204 } : d;
  }
  if (msg.method !== "GET" && msg.method !== "HEAD") return jsonError(405, "bad_request", "image mounts serve GET/HEAD");
  const isHead = msg.method === "HEAD";

  // `?$info`: source metadata for pickers/asset libraries.
  if (query === "$info") {
    const info = await call(ctx, "images", { url: `/info?source=${encodeURIComponent(sub)}` });
    if (info.status === 415) return jsonError(415, "bad_request", "not a decodable image");
    if (!is2xx(info)) return info.status === 404 ? jsonError(404, "not_found", `no image at '${sub}'`) : info;
    return { status: 200, body: info.body };
  }

  let p;
  try {
    p = parse(query, cfg);
  } catch (e) {
    return jsonError(400, "bad_request", e.message);
  }

  // Either path starts by HEADing the source: existence, ETag, type.
  const head = await call(ctx, "source", { method: "HEAD", url: sub });
  if (!is2xx(head)) return jsonError(404, "not_found", `no image at '${sub}'`);
  const sourceEtag = header(head.headers, "etag") ?? "";
  const sourceType = (header(head.headers, "content-type") ?? "application/octet-stream").split(";")[0].trim();
  const inm = header(msg.headers, "if-none-match");

  if (p === null) {
    // Passthrough: the original, streamed host-side.
    const headers = {};
    if (sourceEtag !== "") {
      if (inm !== undefined && ifNoneMatchHits(inm, sourceEtag)) return { status: 304, headers: { etag: sourceEtag } };
      headers.etag = sourceEtag;
    }
    if (isHead) return { status: 200, headers: { ...headers, "content-type": sourceType } };
    return { status: 200, headers: { ...headers, "x-rs2-body-ref": `source:${sub}` } };
  }

  // Derived identity: the cache key (and strong ETag) covers the source
  // path + version and the canonical parameters, so a changed source
  // implicitly invalidates every derivative.
  const [resolved, mediaType, ext] = resolveFormat(p.format, sourceType);
  const canon = canonical(p, resolved);
  const key = sha256Hex(`${sub}\n${sourceEtag}\n${canon}`);
  const derivedEtag = `"${key.slice(0, 32)}"`;
  if (inm !== undefined && ifNoneMatchHits(inm, derivedEtag)) return { status: 304, headers: { etag: derivedEtag } };

  // Derivatives live under one `/d/` container (sharded by key prefix) so
  // a purge is a single confirm-delete of `/d/`.
  const cacheRel = `/d/${key.slice(0, 2)}/${key}.${ext}`;
  const headers = { etag: derivedEtag };

  const cached = await call(ctx, "cache", { method: "HEAD", url: cacheRel });
  if (is2xx(cached)) {
    headers["x-img-cache"] = "hit";
    if (isHead) return { status: 200, headers: { ...headers, "content-type": mediaType } };
    return { status: 200, headers: { ...headers, "x-rs2-body-ref": `cache:${cacheRel}` } };
  }

  // Miss: the one path that touches pixels — host-side, by reference.
  const tq = transformQuery(sub, p, resolved, cfg);
  const made = await call(ctx, "images", { url: `/transform?${tq}&store=${encodeURIComponent(cacheRel)}` });
  if (!is2xx(made)) {
    if (made.status === 404) return jsonError(404, "not_found", `no image at '${sub}'`);
    if (made.status === 413) return jsonError(413, "payload_too_large", made.body && made.body.detail ? made.body.detail : "source too large");
    if (made.status === 415) return jsonError(415, "bad_request", made.body && made.body.detail ? made.body.detail : "not a decodable image");
    if (made.status === 400) return jsonError(400, "bad_request", made.body && made.body.detail ? made.body.detail : "bad transform");
    return made;
  }
  const stored = !!(made.body && made.body.stored);
  headers["x-img-cache"] = stored ? "miss" : "miss,nostore";
  if (isHead) return { status: 200, headers: { ...headers, "content-type": mediaType } };
  // Stored: stream the derivative from the cache. Not stored: transform
  // again with no `store` and stream that — still host-side, no bytes here.
  const ref = stored ? `cache:${cacheRel}` : `images:/transform?${tq}`;
  return { status: 200, headers: { ...headers, "x-rs2-body-ref": ref } };
}
