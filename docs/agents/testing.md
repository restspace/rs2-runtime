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

Wasm-component conformance needs a real guest (otherwise its e2e test is
skipped on the `RS2_CONFORMANCE_COMPONENT` env var):

```powershell
rustup target add wasm32-wasip2
cd conformance/echo-guest; cargo build --target wasm32-wasip2 --release; cd ../..
$env:RS2_CONFORMANCE_COMPONENT = "$PWD\conformance\echo-guest\target\wasm32-wasip2\release\conformance_echo.wasm"
cargo test --features wasm -p rs2-core --test conformance
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
