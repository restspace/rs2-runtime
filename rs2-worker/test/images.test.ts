// The `images` capability (`capabilities/images.ts`): the RS2 transform
// vocabulary maps onto the Images binding's options, sources are read
// through the sibling `source` grant under the caller's principal,
// derivatives land through the sibling `cache` grant, and every failure
// is a status response the guest can inspect.
import { describe, expect, it } from "vitest";

import type { ImagesBackend } from "../src/capabilities/images";
import { imagesGrantConfig, imagesGrantTarget, transformOptions } from "../src/capabilities/images";
import type { CapabilityTarget } from "../src/engines/host-api";
import { Body } from "../src/runtime/body";
import { RsError } from "../src/runtime/error";
import type { JsonObject } from "../src/runtime/error";
import { MediaType } from "../src/runtime/media-type";
import { Message } from "../src/runtime/message";

function req(pathAndQuery: string, method = "GET"): Message {
  const m = Message.request(method, pathAndQuery, "t");
  m.principal = { email: "u@t", roles: "U" } as unknown as Message["principal"];
  return m;
}

const PNG = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 1, 2, 3, 4]);

interface Seen {
  info: number;
  transforms: JsonObject[];
  outputs: JsonObject[];
}

function backend(seen: Seen, dims: { width: number; height: number } | "svg" | "broken" = { width: 800, height: 600 }): ImagesBackend {
  return {
    async info() {
      seen.info += 1;
      if (dims === "broken") throw new Error("IMAGES_DECODE_ERROR 9420: unsupported");
      if (dims === "svg") return { format: "image/svg+xml" };
      return { format: "image/png", ...dims, fileSize: PNG.byteLength };
    },
    input() {
      return {
        transform(t: JsonObject) {
          seen.transforms.push(t);
          return {
            async output(o: JsonObject) {
              seen.outputs.push(o);
              const format = String(o.format);
              return {
                contentType: () => format,
                image: () =>
                  new ReadableStream<Uint8Array>({
                    start(c) {
                      c.enqueue(new Uint8Array([7, 7, 7]));
                      c.close();
                    },
                  }),
              };
            },
          };
        },
      };
    },
  };
}

interface Grants {
  map: Map<string, CapabilityTarget>;
  sourceCalls: Message[];
  cacheCalls: Message[];
}

function grants(opts: { cacheStatus?: number; missing?: boolean } = {}): Grants {
  const sourceCalls: Message[] = [];
  const cacheCalls: Message[] = [];
  const map = new Map<string, CapabilityTarget>();
  map.set("source", async (m) => {
    sourceCalls.push(m);
    if (opts.missing || m.url.path !== "/photo.png") return m.response(404, undefined);
    return m.response(200, Body.fromBytes(PNG, new MediaType("image/png")));
  });
  map.set("cache", async (m) => {
    cacheCalls.push(m);
    return m.response(opts.cacheStatus ?? 201, undefined);
  });
  return { map, sourceCalls, cacheCalls };
}

function target(g: Grants, b: ImagesBackend | undefined, cfg = { source: "source", cache: "cache" }): CapabilityTarget {
  const t = imagesGrantTarget("images", cfg, g.map, b, 1 << 20);
  g.map.set("images", t);
  return t;
}

async function json(m: Message): Promise<JsonObject> {
  return JSON.parse(new TextDecoder().decode(await m.body!.materialize(1 << 20))) as JsonObject;
}

describe("transformOptions", () => {
  it("maps the RS2 vocabulary onto the binding's options", () => {
    const { transform, output } = transformOptions(req("/transform?w=300&h=200&fit=cover&g=0.3,0.7&rect=10,20,400,300&f=webp&q=70"));
    expect(transform).toEqual({
      width: 300,
      height: 200,
      fit: "cover",
      gravity: { x: 0.3, y: 0.7, mode: "remainder" },
      trim: { left: 10, top: 20, width: 400, height: 300 },
    });
    expect(output).toEqual({ format: "image/webp", quality: 70 });
  });

  it("defaults to scale-down + jpeg, and `fill` is the binding's `squeeze`", () => {
    expect(transformOptions(req("/transform?w=640")).transform).toEqual({ width: 640, fit: "scale-down" });
    expect(transformOptions(req("/transform?w=640")).output).toEqual({ format: "image/jpeg" });
    expect(transformOptions(req("/transform?w=10&h=10&fit=fill")).transform.fit).toBe("squeeze");
  });

  it("rejects what the wasm component rejects, with its wordings", () => {
    const bad = (q: string) => {
      try {
        transformOptions(req(`/transform?${q}`));
      } catch (e) {
        return (e as RsError).detail;
      }
      return "";
    };
    expect(bad("w=0")).toBe("'w' must be a positive integer, got '0'");
    expect(bad("w=10&fit=cover")).toBe("fit=cover requires both 'w' and 'h'");
    expect(bad("w=10&fit=wat")).toBe("unknown fit 'wat'");
    expect(bad("w=10&f=bmp")).toBe("unknown format 'bmp'");
    expect(bad("w=10&q=0")).toBe("'q' must be 1..=100, got '0'");
    expect(bad("w=10&rect=1,2,3")).toBe("'rect' must be 'x,y,w,h', got '1,2,3'");
    expect(bad("w=10&g=2,0")).toBe("'g' must be 'x,y' fractions in 0..=1, got '2'");
  });
});

describe("imagesGrantConfig", () => {
  it("defaults sibling names and rejects non-string ones", () => {
    expect(imagesGrantConfig("images", { type: "images" })).toEqual({ source: "source", cache: "cache" });
    expect(imagesGrantConfig("images", { type: "images", source: "orig" })).toEqual({ source: "orig", cache: "cache" });
    expect(() => imagesGrantConfig("images", { type: "images", cache: 3 })).toThrow(/'cache' must name a sibling grant/);
  });
});

describe("images grant target", () => {
  it("answers 501 provider_unavailable without a binding", async () => {
    const g = grants();
    const r = await target(g, undefined)(req("/info?source=/photo.png"));
    expect(r.status).toBe(501);
    expect((await json(r)).code).toBe("provider_unavailable");
    expect(g.sourceCalls.length).toBe(0);
  });

  it("/info reads the source under the caller's principal and reports dimensions", async () => {
    const g = grants();
    const seen: Seen = { info: 0, transforms: [], outputs: [] };
    const r = await target(g, backend(seen))(req("/info?source=/photo.png"));
    expect(r.status).toBe(200);
    expect(await json(r)).toEqual({ width: 800, height: 600, mediaType: "image/png", bytes: PNG.byteLength });
    expect(g.sourceCalls.length).toBe(1);
    expect(g.sourceCalls[0]!.method).toBe("GET");
    expect(g.sourceCalls[0]!.principal).toEqual({ email: "u@t", roles: "U" });
    expect(g.sourceCalls[0]!.source).toBe("internal");
  });

  it("/transform with store writes the derivative through the cache grant, no principal", async () => {
    const g = grants();
    const seen: Seen = { info: 0, transforms: [], outputs: [] };
    const r = await target(g, backend(seen))(req("/transform?source=/photo.png&w=640&f=jpeg&q=78&store=/d/ab/abcd.jpg"));
    expect(r.status).toBe(200);
    expect(await json(r)).toEqual({ mediaType: "image/jpeg", bytes: 3, stored: true });
    expect(seen.transforms).toEqual([{ width: 640, fit: "scale-down" }]);
    expect(seen.outputs).toEqual([{ format: "image/jpeg", quality: 78 }]);
    expect(g.cacheCalls.length).toBe(1);
    const put = g.cacheCalls[0]!;
    expect(put.method).toBe("PUT");
    expect(put.url.path).toBe("/d/ab/abcd.jpg");
    expect(put.principal).toBeUndefined();
    expect(put.body!.mediaType.toString()).toBe("image/jpeg");
  });

  it("a failed cache write reports stored:false instead of failing", async () => {
    const g = grants({ cacheStatus: 507 });
    const seen: Seen = { info: 0, transforms: [], outputs: [] };
    const r = await target(g, backend(seen))(req("/transform?source=/photo.png&w=640&store=/d/ab/abcd.jpg"));
    expect(r.status).toBe(200);
    expect((await json(r)).stored).toBe(false);
  });

  it("/transform without store returns the derivative bytes", async () => {
    const g = grants();
    const seen: Seen = { info: 0, transforms: [], outputs: [] };
    const r = await target(g, backend(seen))(req("/transform?source=/photo.png&w=640&f=png"));
    expect(r.status).toBe(200);
    expect(r.body!.mediaType.toString()).toBe("image/png");
    expect(Array.from(await r.body!.materialize(1 << 20))).toEqual([7, 7, 7]);
    expect(g.cacheCalls.length).toBe(0);
  });

  it("guards the source with maxSourcePixels before transforming (413)", async () => {
    const g = grants();
    const seen: Seen = { info: 0, transforms: [], outputs: [] };
    const r = await target(g, backend(seen, { width: 5000, height: 5000 }))(
      req("/transform?source=/photo.png&w=640&maxSourcePixels=16000000"),
    );
    expect(r.status).toBe(413);
    expect((await json(r)).detail).toBe("source is 25000000px, the mount allows 16000000px");
    expect(seen.transforms.length).toBe(0);
  });

  it("an undecodable or vector source is 415", async () => {
    const g = grants();
    const seen: Seen = { info: 0, transforms: [], outputs: [] };
    expect((await target(g, backend(seen, "broken"))(req("/info?source=/photo.png"))).status).toBe(415);
    expect((await target(g, backend(seen, "svg"))(req("/info?source=/photo.png"))).status).toBe(415);
  });

  it("a missing source is 404, a bad op is 400, a missing sibling grant is 400", async () => {
    const seen: Seen = { info: 0, transforms: [], outputs: [] };
    const g = grants({ missing: true });
    expect((await target(g, backend(seen))(req("/info?source=/nope.png"))).status).toBe(404);
    expect((await target(g, backend(seen))(req("/purge?source=/photo.png"))).status).toBe(400);
    expect((await target(g, backend(seen))(req("/info?source=/photo.png", "POST"))).status).toBe(400);
    const r = await target(grants(), backend(seen), { source: "originals", cache: "cache" })(req("/info?source=/photo.png"));
    expect(r.status).toBe(400);
    expect((await json(r)).detail).toContain("names source grant 'originals'");
  });
});
