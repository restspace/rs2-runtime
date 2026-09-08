// The image-transform mount (`code:image@<v>`, README in
// `guest-services/image`) over HTTP — the same contract on both hosts
// even though the pixel work differs: the Rust host runs the Wasm
// component (codecs in the bundle), the Worker runs the JS bundle in
// `guest-services/image-js` over the host's `images` capability (the
// Cloudflare Images binding). Which deployable serves the mount, and how
// faithfully its backend honours geometry, comes from `divergences()`;
// every assertion below is on dimensions, media types, and headers —
// never on encoded bytes, which legitimately differ between codecs.
//
// Rust leg: build the component, start the host with
// `RS2_RUST_FEATURES=js,wasm`, and point `RS2_IMAGE_COMPONENT` at the
// `.wasm`; the suite skips without it. Worker leg: needs the `images`
// binding (wrangler.jsonc); local dev emulates width/height/format only.

import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { PACKAGE_ROOT, env, type Rs2Client, type Rs2Response } from "./src/client.ts";
import { divergences } from "./src/divergences.ts";
import { Seed } from "./src/seed.ts";

const CODE_BASE = "/services/code";
const REPO = resolve(PACKAGE_ROOT, "..", "..");
const JS_BUNDLE = resolve(REPO, "guest-services", "image-js", "image.js");
const PHOTO = readFileSync(resolve(PACKAGE_ROOT, "fixtures", "photo.png")); // 64×48 RGB

const d = divergences();
const wasmComponent = process.env.RS2_IMAGE_COMPONENT;
const available = d.imageBundle === "js" ? existsSync(JS_BUNDLE) : wasmComponent !== undefined && existsSync(wasmComponent);
const full = d.imageFidelity === "full";

function status(res: Rs2Response, want: number, msg: string): void {
  expect(res.status, `${msg}: ${res.describe()}`).toBe(want);
}

/** Decode the dimensions from a PNG IHDR or a JPEG SOF marker. */
function dimensions(bytes: Uint8Array, contentType: string): { width: number; height: number } {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (contentType === "image/png") {
    expect(Array.from(bytes.slice(0, 4)), "PNG signature").toEqual([0x89, 0x50, 0x4e, 0x47]);
    return { width: view.getUint32(16), height: view.getUint32(20) };
  }
  if (contentType === "image/jpeg") {
    expect(view.getUint16(0), "JPEG SOI").toBe(0xffd8);
    let i = 2;
    while (i + 9 < bytes.length) {
      if (bytes[i] !== 0xff) throw new Error(`JPEG marker expected at ${i}`);
      const marker = bytes[i + 1]!;
      if (marker >= 0xc0 && marker <= 0xcf && marker !== 0xc4 && marker !== 0xc8 && marker !== 0xcc) {
        return { height: view.getUint16(i + 5), width: view.getUint16(i + 7) };
      }
      i += 2 + view.getUint16(i + 2);
    }
    throw new Error("JPEG without a SOF marker");
  }
  throw new Error(`cannot size '${contentType}'`);
}

describe.skipIf(!available)(`image mount (${d.imageBundle} bundle, ${d.imageFidelity} fidelity)`, () => {
  let seed: Seed | undefined;
  let admin: Rs2Client;
  let anon: Rs2Client;

  beforeAll(async () => {
    seed = await Seed.create();
    admin = seed.admin;
    anon = seed.anon;

    const [source, contentType] =
      d.imageBundle === "js"
        ? [readFileSync(JS_BUNDLE, "utf8"), "application/javascript"]
        : [new Uint8Array(readFileSync(wasmComponent!)), "application/wasm"];
    const deploy = await admin.post(`${CODE_BASE}/conf-image/`, { body: source, contentType });
    status(deploy, 201, "[deploy image]");
    const ref = deploy.json<{ ref: string }>().ref;

    await seed.applyMounts([
      { path: "/files", service: "file", config: { access: "open" } },
      {
        path: "/img",
        service: ref,
        config: {
          access: { read: "all", delete: "A" },
          grants: {
            source: { prefix: "/files" },
            cache: { type: "store", root: "img-cache" },
            images: { type: "images", source: "source", cache: "cache" },
          },
          widths: [320, 640],
        },
      },
    ]);
    const put = await admin.put("/files/photo.png", { body: PHOTO, contentType: "image/png" });
    expect([200, 201], `seed /files/photo.png: ${put.describe()}`).toContain(put.status);
  });

  afterAll(async () => {
    if (!seed) return;
    await admin.delete("/img/.cache?confirm=1");
    await admin.delete("/files/photo.png");
    await admin.delete(`${CODE_BASE}/conf-image/`);
    await seed.restore();
  });

  test("?$info reports the source's dimensions, type and size", async () => {
    const res = await anon.get("/img/photo.png?$info");
    status(res, 200, "[$info]");
    expect(res.json()).toEqual({ width: 64, height: 48, mediaType: "image/png", bytes: PHOTO.length });
  });

  test("no params is a passthrough of the original with its ETag", async () => {
    const res = await anon.get("/img/photo.png");
    status(res, 200, "[passthrough]");
    expect(res.contentType()).toBe("image/png");
    expect(Array.from(res.bytes)).toEqual(Array.from(PHOTO));
    const etag = res.header("etag");
    expect(etag, "[passthrough] etag").toBeTruthy();
    const again = await anon.get("/img/photo.png", { headers: { "if-none-match": etag! } });
    status(again, 304, "[passthrough] revalidate");
  });

  test("w= scales down; the derivative is cached and revalidates by strong ETag", async () => {
    const first = await anon.get("/img/photo.png?w=32&h=24");
    status(first, 200, "[w=32] first");
    // `f=auto` keeps PNG sources PNG (alpha-safe).
    expect(first.contentType()).toBe("image/png");
    expect(first.header("x-img-cache")).toMatch(/^miss/);
    const etag = first.header("etag");
    expect(etag, "[w=32] strong etag").toMatch(/^"[0-9a-f]{32}"$/);
    expect(dimensions(first.bytes, "image/png")).toEqual({ width: 32, height: 24 });

    const second = await anon.get("/img/photo.png?w=32&h=24");
    status(second, 200, "[w=32] second");
    expect(second.header("x-img-cache")).toBe("hit");
    expect(second.header("etag")).toBe(etag);
    expect(Array.from(second.bytes)).toEqual(Array.from(first.bytes));

    const revalidate = await anon.get("/img/photo.png?w=32&h=24", { headers: { "if-none-match": etag! } });
    status(revalidate, 304, "[w=32] revalidate");
    expect(revalidate.header("etag")).toBe(etag);
    expect(revalidate.bytes.length).toBe(0);

    // Equivalent requests share the derivative: dpr folds, order is free.
    const folded = await anon.get("/img/photo.png?h=12&dpr=2&w=16");
    status(folded, 200, "[w=32] dpr-folded");
    expect(folded.header("etag")).toBe(etag);
    expect(folded.header("x-img-cache")).toBe("hit");
  });

  test("HEAD answers the derivative's headers with no body", async () => {
    const res = await anon.request("HEAD", "/img/photo.png?w=32&h=24");
    status(res, 200, "[HEAD]");
    expect(res.contentType()).toBe("image/png");
    expect(res.header("etag")).toMatch(/^"[0-9a-f]{32}"$/);
    expect(res.bytes.length).toBe(0);
  });

  test("f=jpeg re-encodes; q is part of the identity", async () => {
    const jpeg = await anon.get("/img/photo.png?w=32&h=24&f=jpeg");
    status(jpeg, 200, "[jpeg]");
    expect(jpeg.contentType()).toBe("image/jpeg");
    expect(dimensions(jpeg.bytes, "image/jpeg")).toEqual({ width: 32, height: 24 });
    const q = await anon.get("/img/photo.png?w=32&h=24&f=jpeg&q=40");
    status(q, 200, "[jpeg q=40]");
    expect(q.header("etag")).not.toBe(jpeg.header("etag"));
    expect(q.header("x-img-cache")).toMatch(/^miss/);
  });

  test.runIf(full)("width-only requests snap up the ladder and never enlarge", async () => {
    const res = await anon.get("/img/photo.png?w=20");
    status(res, 200, "[ladder]");
    // 20 snaps to 320, which exceeds the 64px source: scale-down keeps 64×48.
    expect(dimensions(res.bytes, "image/png")).toEqual({ width: 64, height: 48 });
    const same = await anon.get("/img/photo.png?w=300");
    expect(same.header("etag")).toBe(res.header("etag"));
  });

  test.runIf(full)("fit=cover fills the box; rect crops before scaling", async () => {
    const cover = await anon.get("/img/photo.png?w=32&h=32&fit=cover&g=n");
    status(cover, 200, "[cover]");
    expect(dimensions(cover.bytes, "image/png")).toEqual({ width: 32, height: 32 });
    // `h` present ⇒ no ladder snapping, so the 32×48 crop scales to 16×24.
    const rect = await anon.get("/img/photo.png?rect=0,0,32,48&w=16&h=24");
    status(rect, 200, "[rect]");
    expect(dimensions(rect.bytes, "image/png")).toEqual({ width: 16, height: 24 });
  });

  test("unknown parameters and bad combinations are 400 before any work", async () => {
    const unknown = await anon.get("/img/photo.png?w=32&wat=1");
    status(unknown, 400, "[unknown param]");
    expect(unknown.json()).toEqual({ code: "bad_request", detail: "unknown parameter 'wat'" });
    const cover = await anon.get("/img/photo.png?w=32&fit=cover");
    status(cover, 400, "[cover without h]");
    expect(cover.json().detail).toBe("fit=cover requires both 'w' and 'h'");
    const missing = await anon.get("/img/nope.png?w=32");
    status(missing, 404, "[missing source]");
  });

  test("purging the derivative cache needs the mount's delete role and ?confirm=", async () => {
    const warm = await anon.get("/img/photo.png?w=32&h=24");
    expect(warm.header("x-img-cache")).toBe("hit");
    const denied = await anon.delete("/img/.cache?confirm=1");
    status(denied, 401, "[purge] anonymous");
    const unconfirmed = await admin.delete("/img/.cache");
    status(unconfirmed, 409, "[purge] unconfirmed");
    const purged = await admin.delete("/img/.cache?confirm=1");
    status(purged, 204, "[purge]");
    const cold = await anon.get("/img/photo.png?w=32&h=24");
    status(cold, 200, "[purge] after");
    expect(cold.header("x-img-cache")).toMatch(/^miss/);
    expect(cold.header("etag")).toBe(warm.header("etag"));
  });
});
