# Testing — matrix, harness, conformance, benchmarks

## The matrix (run before declaring done)

```powershell
cargo test -p rs2-core              # native engine only (fast)
cargo test -p rs2-core --features js   # + V8 isolate engine
cargo test --features wasm          # + Wasmtime component engine
cargo build                         # whole workspace
```

Always run **both default and `--features js`** for any change touching the
runtime, services, or contract — the JS path exercises the sandbox host bridge
(including `HostApi::log`). `--features wasm` when you touch the engine/contract.

When a change alters anything a client can observe (status, header, JSON
shape), also run the HTTP conformance suite against **both hosts** — see below.

## HTTP conformance (both hosts)

`conformance/http/` is the black-box vitest suite that holds the Rust server
and the Cloudflare Worker (`rs2-worker/`) to one contract
(`docs/agents/cloudflare.md` §F; operator's card in `conformance/http/README.md`):

```sh
cd conformance/http && npm ci
# Rust host (terminal 1 / background), then the suite:
RS2_PORT=3100 RS2_SERVER_BIN=target/debug/rs2-server npm run host:rust
RS2_HOST_KIND=rust RS2_PORT=3100 npx vitest run
# Worker host (needs `npm ci` + `npm run build:shim` in rs2-worker/ first):
RS2_PORT=8787 RS2_ADMIN_TOKEN=dev npm run host:cf
RS2_HOST_KIND=cloudflare RS2_PORT=8787 RS2_ADMIN_TOKEN=dev npx vitest run
```

One host per port, one suite run per host (suites reshape the shared fixture
tenant sequentially). The Worker additionally has its own unit tests:
`cd rs2-worker && npm test && npm run typecheck`.

CI runs all of this: `conformance-rust` and `conformance-cf` (both required
from P2 on) plus `worker-unit` in `.github/workflows/ci.yml`. The only
per-host allowances live in `conformance/http/src/divergences.ts`, and the
memory-cap code test is skipped on local `wrangler dev` (no per-isolate heap
cap in local workerd) unless `RS2_CF_REMOTE` is set.

Wasm-component conformance needs a real guest (otherwise its e2e test is
skipped on the `RS2_CONFORMANCE_COMPONENT` env var):

```powershell
rustup target add wasm32-wasip2
cd conformance/echo-guest; cargo build --target wasm32-wasip2 --release; cd ../..
$env:RS2_CONFORMANCE_COMPONENT = "$PWD\conformance\echo-guest\target\wasm32-wasip2\release\conformance_echo.wasm"
cargo test --features wasm -p rs2-core --test conformance
```

The image service e2e (`tests/image_service.rs`) is gated the same way — its
pure param/geometry/transform logic also tests natively inside the guest crate:

```powershell
cd guest-services/image; cargo test; cargo build --target wasm32-wasip2 --release; cd ../..
$env:RS2_IMAGE_COMPONENT = "$PWD\guest-services\image\target\wasm32-wasip2\release\rs2_image.wasm"
cargo test --features wasm -p rs2-core --test image_service
```

## Integration harness

Tests build a `Runtime` over real adapters and drive it with `rt.handle(msg)`:

- A `ConfigLoader` impl returns the tenant config (`StaticLoader` for fixed,
  `MutableLoader` with a `Mutex<Value>` for hot-reload/self-config tests).
- `Adapters::new(file_store, data_store)` wires the rest with defaults — note
  the defaults are no-ops where it matters (`NullLogStore`, `http: None`), so
  opt in with `.with_http(...)` / `.with_logging(store, level)` when the test
  needs them.
- `Message::request(method, path, tenant)`; `.with_body(...)` / `.with_json(...)`;
  read responses with `resp.body.as_mut().unwrap().materialize(n).await`.
- Anything that emits logs asynchronously: grab the `Arc<FileLogStore>` and
  `store.flush().await` (a FIFO barrier through the writer channel) before
  querying, or the read races the background writer.

Copy the setup from `tests/runtime_services.rs` or `tests/logging.rs`.

## Test-file map

- `tests/conformance.rs` — engine-neutral contract (native/js/wasm); capability
  denial, limits, state.
- `tests/store_conformance.rs` — the normative store contract; every
  store-shaped surface (`file`, `data`, spec stores at `.queries`/`.pipelines`,
  code store) must pass it.
- `tests/runtime_services.rs` — full dispatch-path integration for file/data.
- `tests/m2_composition.rs` — demo tenant e2e, G6 idempotency, G7 streaming.
- `tests/m3_surface.rs` — query, discovery/agent surface, OpenAPI, code deploy.
- `tests/cors.rs`, `tests/caching.rs`, `tests/logging.rs` — the cross-cutting
  host concerns (each pins severities/headers/policy + tenant isolation).
- `tests/npm_compat.rs`, `tests/sdk_corpus.rs` — `--features js` API-surface (G5).

## Benchmarks (PRD goals)

`tests/g_benchmarks.rs` is `#[ignore]`d and meant for `--release`:

```powershell
cargo test -p rs2-core --release --test g_benchmarks -- --ignored --nocapture
```

- **G1** — dispatch overhead p99 < 1ms (added over a direct service call).
  Logging's `enabled()` fast-path keeps this ~20µs; watch it after any change to
  the `handle` hot path.
- **G3** — containment: the per-tenant breaker (`wrapper::TenantBreaker`) keeps a
  pathological tenant from pushing neighbor p99 beyond 2× baseline.
