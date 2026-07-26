# rs2-image — query-string image resize/crop (Wasm component)

Responsive-design image transforms as a sandboxed RS2 service: mount it in
front of any file mount and request derivatives with query parameters. The
runtime stays image-free — codecs live in this bundle, paid for only by
tenants that mount it.

```
GET /img/photo.jpg?w=640              scaled to 640 wide (never upscaled)
GET /img/photo.jpg?w=320&dpr=2        srcset-friendly: 640 physical px
GET /img/photo.jpg?w=300&h=200&fit=cover&g=n   exact box, cropped from the top
GET /img/photo.jpg?rect=10,20,400,300&w=200    explicit crop, then scale
GET /img/photo.jpg?$info              {"width":…,"height":…,"mediaType":…,"bytes":…}
GET /img/photo.jpg                    passthrough of the original
DELETE /img/.cache?confirm=1          purge every cached derivative
```

## Parameters

| Param | Meaning |
| --- | --- |
| `w`, `h` | Target box in CSS px; one alone preserves aspect |
| `dpr` | Device-pixel multiplier 1–3, folded into `w`/`h` |
| `fit` | `scale-down` (default, never enlarges), `contain`, `cover`, `fill` |
| `g` | Crop gravity for `cover`: compass (`n`,`ne`,…,`center`) or fractions `g=0.3,0.7` |
| `rect` | `x,y,w,h` pre-resize crop in source pixels |
| `f` | `auto` (default), `jpeg`, `png`, `webp` (lossless) |
| `q` | JPEG quality 1–100 (default from config) |

`f=auto` resolves decode-free from the source type: png/gif/webp sources stay
PNG (alpha-safe), everything else becomes JPEG. Unknown parameters are a 400 —
they would fragment the cache and hide typos. Sources: JPEG, PNG, GIF (first
frame), WebP.

## Efficiency design

Every request canonicalizes its parameters (clamps, `dpr` folding, defaults
dropped, width-ladder snapping), then derives a strong ETag =
`sha256(source path, source ETag, canonical params)`. The flow per GET:

1. `HEAD` the source through the `source` grant (caller's authz applies).
2. `If-None-Match` hit on the derived ETag → **304, no further work**.
3. Cache `HEAD` in the `store` grant → hit answers with `x-rs2-body-ref`, so
   the host streams the derivative — **no image bytes enter the sandbox**.
4. Only a miss decodes: guarded by `maxSourcePixels` from the image header
   (decompression bombs refused before pixel decode), transform, best-effort
   cache `PUT` (a failed write serves inline instead of failing).

The cache key embeds the source ETag, so editing an image implicitly
invalidates its derivatives — no purge needed for correctness. Responses
carry `x-img-cache: hit | miss | miss,nostore` for observability.

## Build, deploy, mount

```powershell
cargo build --target wasm32-wasip2 --release
rs2 deploy target/wasm32-wasip2/release/rs2_image.wasm --name image
```

```json
{ "path": "/img", "service": "code:image@<version>", "config": {
    "access": { "read": "all", "delete": "A" },
    "grants": {
      "source": { "prefix": "/files" },
      "cache":  { "type": "store", "root": "img-cache" }
    },
    "widths": [320, 640, 960, 1280, 1920],
    "defaultQuality": 78,
    "maxWidth": 4096, "maxHeight": 4096, "maxSourcePixels": 16000000,
    "caching": { "mode": "cache", "maxAgeSeconds": 86400, "public": true }
} }
```

- **`source`** points at the mount holding originals; reads go through full
  dispatch under the caller's principal, so restricted images stay restricted.
- **`cache`** is a private store grant (`.rs2-store/img-cache`) — not a mount,
  no access surface, writable regardless of who the caller is.
- **`widths`** (recommended on public mounts) snaps width-only requests up the
  ladder, bounding both cache cardinality and the abuse surface of arbitrary
  `w` values. Requests with `h` are not snapped.
- **`maxSourcePixels`** defaults to 16 MP — sized to the sandbox's 128 MB
  memory cap (16 MP RGBA ≈ 64 MB decoded plus working buffers).
- The mount's universal `caching` config gives browsers/CDNs `Cache-Control`;
  the URL doesn't change when the source does, so prefer moderate `maxAge` +
  ETag revalidation (append `?v=<etag>` from templates if you want
  immutable-style caching).

Optionally attach a discovery manifest at deploy time (`X-RS2-Manifest`) with
`"effect": "idempotent"` so the mount lists cleanly on the agent surface.

## Deliberate limits (v1)

- **WebP output is lossless only** (no mature pure-Rust lossy encoder);
  `f=auto` therefore prefers JPEG for photos and there is no `Accept`
  negotiation yet — a future bundle upgrade adds lossy WebP behind the same
  URLs (new content hash, same mount config shape).
- AVIF: deferred (encode cost).
- No per-key request coalescing: N concurrent first requests for the same
  derivative each transform. Harmless at CMS scale; a host-side singleflight
  for `pure` GETs is the eventual fix.

## Tests

Pure param/geometry/transform logic tests natively (`cargo test` here). The
end-to-end suite lives in `rs2-core/tests/image_service.rs` (`--features
wasm`), gated on `RS2_IMAGE_COMPONENT` pointing at the built component — see
`docs/agents/testing.md`.
