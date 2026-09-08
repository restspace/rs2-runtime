// Pure logic + the request flow against a scripted host. Run: `node --test`
// here. The param cases mirror `../image/src/params.rs` tests one for one,
// so the two bundles canonicalize identically (shared cache keys/ETags).
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import handle, { canonical, configFrom, parse, resolveFormat, sha256Hex } from "./image.js";

const cfg = () => configFrom({});

describe("params", () => {
  it("empty query is passthrough", () => assert.equal(parse("", cfg()), null));
  it("unknown keys are rejected", () => assert.throws(() => parse("w=100&wat=1", cfg()), /unknown parameter 'wat'/));
  it("dpr folds into dimensions", () => assert.equal(parse("w=320&dpr=2", cfg()).w, 640));
  it("dpr clamps to three", () => assert.equal(parse("w=100&dpr=10", cfg()).w, 300));
  it("dimensions clamp to the config ceiling", () => assert.equal(parse("w=99999", cfg()).w, 4096));
  it("width-only requests snap up the ladder", () => {
    const c = configFrom({ widths: [1280, 320, 640, 640] });
    assert.equal(parse("w=400", c).w, 640);
    assert.equal(parse("w=4000", c).w, 1280);
    assert.equal(parse("w=400&h=300", c).w, 400);
  });
  it("cover requires both dimensions", () => {
    assert.throws(() => parse("w=100&fit=cover", cfg()), /fit=cover requires both 'w' and 'h'/);
    assert.ok(parse("w=100&h=100&fit=cover", cfg()));
  });
  it("canonical is order-insensitive and drops defaults", () => {
    const a = parse("w=640&q=78&fit=scale-down", cfg());
    const b = parse("fit=scale-down&w=640", cfg());
    assert.equal(canonical(a, "jpeg"), canonical(b, "jpeg"));
    assert.equal(canonical(a, "jpeg"), "w=640&q=78&f=jpeg");
    const c = parse("w=300&h=200&fit=cover&g=ne&rect=1,2,30,40&q=50&f=png", cfg());
    assert.equal(canonical(c, "png"), "w=300&h=200&fit=cover&g=1,0&rect=1,2,30,40&q=50&f=png");
  });
  it("auto format resolves from the source type", () => {
    assert.equal(resolveFormat("auto", "image/jpeg")[0], "jpeg");
    assert.equal(resolveFormat("auto", "image/png")[0], "png");
    assert.equal(resolveFormat("webp", "image/jpeg")[0], "webp");
  });
  it("gravity parses compass and fractions", () => {
    assert.deepEqual(parse("w=10&h=10&fit=cover&g=ne", cfg()).g, { x: 1, y: 0 });
    assert.deepEqual(parse("w=10&h=10&fit=cover&g=0.3,0.7", cfg()).g, { x: 0.3, y: 0.7 });
    assert.throws(() => parse("w=10&h=10&fit=cover&g=up", cfg()), /'g' must be a compass point/);
  });
  it("config validation matches the component", () => {
    assert.throws(() => configFrom({ defaultQuality: 101 }), /'defaultQuality' must be 1..=100/);
    assert.throws(() => configFrom({ maxWidth: "big" }), /'maxWidth' must be a positive integer/);
    assert.throws(() => configFrom({ widths: [0] }), /'widths' must be an array of positive integers/);
  });
});

describe("sha256", () => {
  it("matches the reference vectors", () => {
    assert.equal(sha256Hex(""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    assert.equal(sha256Hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    assert.equal(
      sha256Hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
      "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
    );
  });
});

// ---- the flow against a scripted host ----------------------------------------

function host(script) {
  const calls = [];
  const ctx = {
    config: script.config ?? {},
    request: async (cap, req) => {
      calls.push([cap, req]);
      const r = script.on(cap, req);
      if (r instanceof Error) throw r;
      return r;
    },
  };
  return { ctx, calls };
}

const SOURCE_ETAG = '"src-v1"';
const msg = (url, headers = {}, method = "GET") => ({ method, url, headers: { "x-rs2-base-path": "/img", ...headers } });

function standard({ cached = false, stored = true } = {}) {
  return host({
    config: { widths: [320, 640] },
    on(cap, req) {
      if (cap === "source" && req.method === "HEAD") {
        return req.url === "/photo.jpg"
          ? { status: 200, headers: { etag: SOURCE_ETAG, "content-type": "image/jpeg" } }
          : { status: 404 };
      }
      if (cap === "cache" && req.method === "HEAD") return { status: cached ? 200 : 404 };
      if (cap === "cache" && req.method === "DELETE") return { status: 204 };
      if (cap === "images") {
        if (req.url.startsWith("/info?")) return { status: 200, body: { width: 800, height: 600, mediaType: "image/jpeg", bytes: 1234 } };
        return { status: 200, body: { mediaType: "image/jpeg", bytes: 99, stored } };
      }
      throw new Error(`unexpected ${cap} ${req.method ?? "GET"} ${req.url}`);
    },
  });
}

const expectedKey = () => sha256Hex(`/photo.jpg\n${SOURCE_ETAG}\nw=640&q=78&f=jpeg`);

describe("handle", () => {
  it("a miss transforms by reference, stores, and answers with a cache body-ref", async () => {
    const { ctx, calls } = standard();
    const r = await handle(msg("/img/photo.jpg?w=500"), ctx);
    const key = expectedKey();
    assert.equal(r.status, 200);
    assert.equal(r.headers.etag, `"${key.slice(0, 32)}"`);
    assert.equal(r.headers["x-img-cache"], "miss");
    assert.equal(r.headers["x-rs2-body-ref"], `cache:/d/${key.slice(0, 2)}/${key}.jpg`);
    const transform = calls.find(([cap]) => cap === "images")[1];
    assert.equal(
      transform.url,
      `/transform?source=%2Fphoto.jpg&w=640&fit=scale-down&f=jpeg&q=78&maxSourcePixels=16000000&store=${encodeURIComponent(`/d/${key.slice(0, 2)}/${key}.jpg`)}`,
    );
    // Sequence: HEAD source → HEAD cache → transform. No source GET.
    assert.deepEqual(calls.map(([cap, req]) => `${cap} ${req.method ?? "GET"}`), ["source HEAD", "cache HEAD", "images GET"]);
  });

  it("a hit never calls the images capability", async () => {
    const { ctx, calls } = standard({ cached: true });
    const r = await handle(msg("/img/photo.jpg?w=640"), ctx);
    assert.equal(r.headers["x-img-cache"], "hit");
    assert.ok(r.headers["x-rs2-body-ref"].startsWith("cache:/d/"));
    assert.ok(!calls.some(([cap]) => cap === "images"));
  });

  it("If-None-Match on the derived ETag is a 304 before any cache I/O", async () => {
    const { ctx, calls } = standard();
    const etag = `"${expectedKey().slice(0, 32)}"`;
    const r = await handle(msg("/img/photo.jpg?w=640", { "if-none-match": `W/${etag}` }), ctx);
    assert.equal(r.status, 304);
    assert.equal(r.headers.etag, etag);
    assert.deepEqual(calls.map(([cap]) => cap), ["source"]);
  });

  it("a failed cache write serves inline through a store-less transform ref", async () => {
    const { ctx } = standard({ stored: false });
    const r = await handle(msg("/img/photo.jpg?w=640"), ctx);
    assert.equal(r.headers["x-img-cache"], "miss,nostore");
    assert.ok(r.headers["x-rs2-body-ref"].startsWith("images:/transform?source=%2Fphoto.jpg&w=640"));
    assert.ok(!r.headers["x-rs2-body-ref"].includes("store="));
  });

  it("no params is passthrough of the original", async () => {
    const { ctx } = standard();
    const r = await handle(msg("/img/photo.jpg"), ctx);
    assert.deepEqual(r, { status: 200, headers: { etag: SOURCE_ETAG, "x-rs2-body-ref": "source:/photo.jpg" } });
    const head = await handle(msg("/img/photo.jpg", {}, "HEAD"), ctx);
    assert.deepEqual(head, { status: 200, headers: { etag: SOURCE_ETAG, "content-type": "image/jpeg" } });
    const notModified = await handle(msg("/img/photo.jpg", { "if-none-match": SOURCE_ETAG }), ctx);
    assert.equal(notModified.status, 304);
  });

  it("$info relays the host's metadata", async () => {
    const { ctx } = standard();
    const r = await handle(msg("/img/photo.jpg?$info"), ctx);
    assert.deepEqual(r, { status: 200, body: { width: 800, height: 600, mediaType: "image/jpeg", bytes: 1234 } });
  });

  it("bad params are 400 before any I/O; a missing source is 404", async () => {
    const { ctx, calls } = standard();
    const bad = await handle(msg("/img/photo.jpg?w=10&wat=1"), ctx);
    assert.deepEqual(bad, { status: 400, body: { code: "bad_request", detail: "unknown parameter 'wat'" } });
    assert.equal(calls.length, 0);
    const missing = await handle(msg("/img/nope.jpg?w=10"), ctx);
    assert.equal(missing.status, 404);
  });

  it("DELETE /.cache purges only with ?confirm=", async () => {
    const { ctx, calls } = standard();
    assert.equal((await handle(msg("/img/.cache", {}, "DELETE"), ctx)).status, 409);
    assert.equal((await handle(msg("/img/photo.jpg", {}, "DELETE"), ctx)).status, 405);
    assert.equal((await handle(msg("/img/.cache?confirm=1", {}, "DELETE"), ctx)).status, 204);
    assert.deepEqual(calls.at(-1), ["cache", { method: "DELETE", url: "/d/?confirm=d" }]);
  });

  it("host capability errors become structured responses", async () => {
    const { ctx } = host({
      on(cap) {
        if (cap === "source") return { status: 200, headers: { etag: SOURCE_ETAG, "content-type": "image/png" } };
        if (cap === "cache") return { status: 404 };
        const e = new Error("images");
        e.code = "capability_denied";
        return e;
      },
    });
    const r = await handle(msg("/img/photo.jpg?w=10"), ctx);
    assert.equal(r.status, 403);
    assert.equal(r.body.code, "capability_denied");
  });
});
