// The `images` capability grant: host-side image transforms over the
// Cloudflare Images binding (cloudflare.md §E, decision 46). A `code:`
// mount declares `{"type": "images", "source": "<grant>", "cache":
// "<grant>"}` and its guest asks for derivatives by *reference*: the host
// reads the original through the named `source` grant (under the caller's
// principal, so restricted images stay restricted), transforms it, and
// writes the result through the named `cache` grant — no image bytes ever
// enter the sandbox, which is what lets a JS bundle (string bodies) serve
// the same mount the Wasm `guest-services/image` component serves on Rust.
//
// Two ops, both `GET`, parameters in the RS2 image vocabulary (the query
// string `guest-services/image` documents), so any guest can use them:
//
//   /info?source=/photo.jpg
//       → 200 `{width, height, mediaType, bytes}`; 415 when not decodable.
//   /transform?source=/photo.jpg&w=640[&h=…&fit=…&g=x,y&rect=x,y,w,h]
//              &f=jpeg|png|webp|avif&q=78[&maxSourcePixels=N][&store=/d/…]
//       → with `store`: the derivative is PUT to `<cache>:<store>` and the
//         reply is 200 `{mediaType, bytes, stored}` (`stored: false` when
//         the cache write failed — the guest then serves inline by
//         answering with `x-rs2-body-ref: images:/transform?…` minus
//         `store`, which re-runs the transform and streams it host-side);
//       → without `store`: the derivative bytes as the body.
//   413 `payload_too_large` when the source exceeds `maxSourcePixels`;
//   404 when the source grant has no such image.

import type { CapabilityTarget } from "../engines/host-api";
import { Body } from "../runtime/body";
import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import { Message } from "../runtime/message";

/// The Images binding surface this capability uses (the subset of
/// `ImagesBinding` from `@cloudflare/workers-types`), named here so the
/// capability is testable with a stub and stays decoupled from the types
/// package's churn.
export interface ImagesBackend {
  info(stream: ReadableStream<Uint8Array>): Promise<{ format: string; width?: number; height?: number; fileSize?: number }>;
  input(stream: ReadableStream<Uint8Array>): {
    transform(t: JsonObject): { output(o: JsonObject): Promise<{ contentType(): string; image(): ReadableStream<Uint8Array> }> };
  };
}

const FORMATS: Record<string, string> = {
  jpeg: "image/jpeg",
  jpg: "image/jpeg",
  png: "image/png",
  webp: "image/webp",
  avif: "image/avif",
  gif: "image/gif",
};

const FITS: Record<string, string> = {
  "scale-down": "scale-down",
  contain: "contain",
  cover: "cover",
  // RS2 `fill` stretches to the box ignoring aspect; Cloudflare calls that `squeeze`.
  fill: "squeeze",
};

function positiveInt(msg: Message, key: string): number | undefined {
  const raw = msg.url.queryParam(key);
  if (raw === undefined) return undefined;
  if (!/^\d+$/.test(raw) || Number(raw) === 0) {
    throw RsError.badRequest(`'${key}' must be a positive integer, got '${raw}'`);
  }
  return Number(raw);
}

function fraction(raw: string, key: string): number {
  const n = Number(raw);
  if (!/^\d*\.?\d+$/.test(raw) || !(n >= 0 && n <= 1)) {
    throw RsError.badRequest(`'${key}' must be 'x,y' fractions in 0..=1, got '${raw}'`);
  }
  return n;
}

/// Parse the transform vocabulary into the binding's `transform`/`output`
/// options. Exported for unit tests.
export function transformOptions(msg: Message): { transform: JsonObject; output: JsonObject } {
  const transform: JsonObject = {};
  const w = positiveInt(msg, "w");
  const h = positiveInt(msg, "h");
  if (w !== undefined) transform.width = w;
  if (h !== undefined) transform.height = h;
  const fit = msg.url.queryParam("fit") ?? "scale-down";
  const cfFit = FITS[fit];
  if (cfFit === undefined) throw RsError.badRequest(`unknown fit '${fit}'`);
  transform.fit = cfFit;
  if ((fit === "cover" || fit === "fill") && (w === undefined || h === undefined)) {
    throw RsError.badRequest(`fit=${fit} requires both 'w' and 'h'`);
  }
  const g = msg.url.queryParam("g");
  if (g !== undefined) {
    const parts = g.split(",");
    if (parts.length !== 2) throw RsError.badRequest(`'g' must be 'x,y' fractions in 0..=1, got '${g}'`);
    // Fractions of the leftover space — exactly the binding's `remainder` mode.
    transform.gravity = { x: fraction(parts[0]!, "g"), y: fraction(parts[1]!, "g"), mode: "remainder" };
  }
  const rect = msg.url.queryParam("rect");
  if (rect !== undefined) {
    const parts = rect.split(",");
    if (parts.length !== 4 || !parts.every((p) => /^\d+$/.test(p)) || Number(parts[2]) === 0 || Number(parts[3]) === 0) {
      throw RsError.badRequest(`'rect' must be 'x,y,w,h', got '${rect}'`);
    }
    // `trim` crops the source before any resize, which is what `rect` means.
    transform.trim = { left: Number(parts[0]), top: Number(parts[1]), width: Number(parts[2]), height: Number(parts[3]) };
  }
  const f = msg.url.queryParam("f") ?? "jpeg";
  const format = FORMATS[f];
  if (format === undefined) throw RsError.badRequest(`unknown format '${f}'`);
  const output: JsonObject = { format };
  const q = msg.url.queryParam("q");
  if (q !== undefined) {
    if (!/^\d+$/.test(q) || Number(q) < 1 || Number(q) > 100) throw RsError.badRequest(`'q' must be 1..=100, got '${q}'`);
    output.quality = Number(q);
  }
  return { transform, output };
}

function bytesStream(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

function unsupported(detail: string): RsError {
  return new RsError(415, codes.BAD_REQUEST, "Unsupported Media Type", `not a decodable image: ${detail}`);
}

function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export interface ImagesGrantConfig {
  source: string;
  cache: string;
}

/// Validate an `{"type": "images"}` grant's sibling-grant names. Both hosts
/// accept the shape at build time; only a host with a backend can serve it.
export function imagesGrantConfig(capability: string, grant: JsonObject): ImagesGrantConfig {
  const name = (key: string, fallback: string): string => {
    const v = grant[key];
    if (v === undefined) return fallback;
    if (typeof v !== "string" || v === "") {
      throw RsError.badRequest(`images grant '${capability}' '${key}' must name a sibling grant`);
    }
    return v;
  };
  return { source: name("source", "source"), cache: name("cache", "cache") };
}

/// Build the capability target. `grants` is the mount's (possibly still
/// filling) grant table — the sibling grants resolve at call time, so
/// declaration order in `config.grants` does not matter.
export function imagesGrantTarget(
  capability: string,
  cfg: ImagesGrantConfig,
  grants: Map<string, CapabilityTarget>,
  backend: ImagesBackend | undefined,
  materializeCap: number,
): CapabilityTarget {
  const sibling = (name: string, role: string): CapabilityTarget => {
    const t = grants.get(name);
    if (!t) throw RsError.badRequest(`images grant '${capability}' names ${role} grant '${name}', which is not granted`);
    return t;
  };

  return async (msg: Message): Promise<Message> => {
    const template = msg.response(200, undefined);
    try {
      if (!backend) {
        throw RsError.providerUnavailable(
          "this deployment has no images binding (wrangler.jsonc `images`); mount the wasm image component instead",
        );
      }
      const op = msg.url.path.replace(/^\/+/, "");
      if (msg.method !== "GET" || (op !== "info" && op !== "transform")) {
        throw RsError.badRequest("images capability serves GET /info and GET /transform");
      }
      const sourcePath = msg.url.queryParam("source");
      if (sourcePath === undefined || sourcePath === "") throw RsError.badRequest("'source' is required");

      // Read the original through the source grant under the caller's
      // identity — authz on the originals is the source mount's.
      const get = Message.request("GET", sourcePath, msg.tenant);
      get.principal = msg.principal ? { ...msg.principal } : undefined;
      get.trace = msg.trace.child();
      get.depth = Math.min(msg.depth + 1, 0xffff);
      get.source = "internal";
      const got = await sibling(cfg.source, "source")(get);
      if (!got.isOk() || !got.body) throw RsError.notFound(`no image at '${sourcePath}'`);
      const sourceType = got.body.mediaType.toString();
      const sourceBytes = await got.body.materialize(materializeCap);

      let info: Awaited<ReturnType<ImagesBackend["info"]>>;
      try {
        info = await backend.info(bytesStream(sourceBytes));
      } catch (e) {
        throw unsupported(errorText(e));
      }
      if (info.width === undefined || info.height === undefined) throw unsupported(info.format);

      if (op === "info") {
        return msg.response(
          200,
          Body.fromJson({ width: info.width, height: info.height, mediaType: sourceType, bytes: sourceBytes.byteLength }),
        );
      }

      const cap = positiveInt(msg, "maxSourcePixels");
      const pixels = info.width * info.height;
      if (cap !== undefined && pixels > cap) {
        throw RsError.payloadTooLarge(`source is ${pixels}px, the mount allows ${cap}px`);
      }
      const { transform, output } = transformOptions(msg);
      let out: Uint8Array;
      let mediaType: string;
      try {
        const result = await backend.input(bytesStream(sourceBytes)).transform(transform).output(output);
        mediaType = result.contentType();
        out = await collect(result.image());
      } catch (e) {
        throw RsError.badRequest(`image transform failed: ${errorText(e)}`);
      }
      const mt = MediaType.parse(mediaType);

      const store = msg.url.queryParam("store");
      if (store === undefined) return msg.response(200, Body.fromBytes(out, mt));

      // Best-effort cache write through the cache grant (service-private
      // storage: no principal); a failed write is reported, not fatal.
      const put = Message.request("PUT", store, msg.tenant);
      put.trace = msg.trace.child();
      put.depth = Math.min(msg.depth + 1, 0xffff);
      put.source = "internal";
      put.body = Body.fromBytes(out, mt);
      let stored = false;
      try {
        stored = (await sibling(cfg.cache, "cache")(put)).isOk();
      } catch {
        stored = false;
      }
      const reply: Json = { mediaType: mt.toString(), bytes: out.byteLength, stored };
      return msg.response(200, Body.fromJson(reply));
    } catch (e) {
      // Like the `store` grant, failures become status responses the guest
      // can inspect rather than sandbox throws.
      if (e instanceof RsError) return template.errorResponse(e);
      throw e;
    }
  };
}
