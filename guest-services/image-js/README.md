# rs2-image (JS) — query-string image resize/crop over the `images` grant

The same image-transform mount as the Wasm component in [`../image`](../image)
— identical URLs, parameters, config, response headers, strong ETags,
`x-img-cache` values and cache layout — for hosts where the pixel work is a
**host capability** rather than codecs inside the bundle. Today that is the
Cloudflare host, whose `images` grant fronts the Cloudflare Images binding.
See the component's README for the parameter reference and efficiency design;
this file covers only what differs.

```
GET /img/photo.jpg?w=640              scaled to 640 wide (never upscaled)
GET /img/photo.jpg?w=300&h=200&fit=cover&g=n
GET /img/photo.jpg?$info              {"width":…,"height":…,"mediaType":…,"bytes":…}
DELETE /img/.cache?confirm=1          purge every cached derivative
```

## How it works

The bundle never sees image bytes. Per request it canonicalizes the
parameters, `HEAD`s the source through the `source` grant (the caller's
authz applies; the ETag versions the cache key), answers `If-None-Match`
with a 304, `HEAD`s the derivative in the `cache` grant, and on a miss asks
the host to transform **by reference**:

```
GET images:/transform?source=/photo.jpg&w=640&fit=scale-down&f=jpeg&q=78
                     &maxSourcePixels=16000000&store=/d/ab/<key>.jpg
```

The host reads the original through `source`, transforms it, writes the
result through `cache`, and replies `{mediaType, bytes, stored}`. The
bundle then answers with `x-rs2-body-ref: cache:/d/ab/<key>.jpg` and the
host streams the derivative. If the cache write failed the body-ref is
`images:/transform?…` without `store`, which transforms again and streams
inline (`x-img-cache: miss,nostore`).

## Deploy and mount

No build step — the bundle is a dependency-free ESM module:

```
rs2 deploy guest-services/image-js/image.js --name image
```

```json
{ "path": "/img", "service": "code:image@<version>", "config": {
    "access": { "read": "all", "delete": "A" },
    "grants": {
      "source": { "prefix": "/files" },
      "cache":  { "type": "store", "root": "img-cache" },
      "images": { "type": "images", "source": "source", "cache": "cache" }
    },
    "widths": [320, 640, 960, 1280, 1920],
    "defaultQuality": 78,
    "maxWidth": 4096, "maxHeight": 4096, "maxSourcePixels": 16000000,
    "caching": { "mode": "cache", "maxAgeSeconds": 86400, "public": true }
} }
```

The only addition over the Wasm mount is the `images` grant naming its two
siblings. The Rust host accepts that grant (so one tenant file mounts on
both hosts) but has no transform backend: there, mount the Wasm component
under the same config. The Worker needs the `images` binding in
`wrangler.jsonc`; without it every transform answers 501
`provider_unavailable`.

## Differences from the Wasm component

- Output codecs are the platform's: lossy WebP and AVIF are available
  (`f=webp` is lossy here, lossless in the component), and encoded bytes
  differ. Compare derivatives by dimensions and type, never bytes.
- `maxSourcePixels` is checked against the platform's decoded header
  (`info()`), the same 413 wording.
- Local `wrangler dev` emulates the binding with width/height/format only;
  cover/gravity/rect geometry needs a deployed Worker (`--remote`).

## Tests

```
node --test            # params/canonical/sha256 + the flow against a scripted host
```

The param cases mirror `../image/src/params.rs` one for one, so both bundles
canonicalize identically and share cache keys. The end-to-end contract is
`conformance/http/image.test.ts`, run against both hosts.
