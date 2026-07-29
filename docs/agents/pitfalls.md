# Pitfalls — Rust + engine traps that have bitten us

## `Body` is Send-not-Sync → don't hold `&Message` across `.await`

`Message` carries a `Body` whose payload stream is `Send` but **not `Sync`**.
Any future that holds a `&Message` (or a closure capturing one) across an
`.await` becomes non-`Send`, and the async service trait requires `Send`
futures. The compiler error is `future cannot be sent between threads safely`,
usually pointing at the `async fn handle` signature, not the real line.

**Fix pattern: extract owned values before the await.** Pull out what you need
as owned data — header values as `String`, the `Range`, a bodyless response
template via `msg.response(StatusCode::OK, None)` — then await. Concretely:

- Don't `let t = || msg.response(...)` (a closure capturing `&msg`) and call it
  after an await. Inline `msg.response(...)` at each call site so the borrow
  ends before the await. (This exact bug appeared in the static-site change.)
- In the pipeline executor, clone branch inputs eagerly and move owned bodies
  into retry closures.
- In `discovery::handle`, take `Message` by value.

When you see the non-`Send` error after a refactor, look for a value still
borrowing `msg` that lives across an await — not the function signature.

## Hot-path allocation

Code at the `Runtime::dispatch`/`handle` choke point runs per request. Gate
optional work behind a cheap predicate: e.g. boundary logging calls
`LogStore::enabled()` first and builds **no** `LogRecord` when the sink is the
no-op `NullLogStore`. Skipping this regressed the G1 p99 from ~20µs to ~250µs
(still under the 1ms target, but wasteful). Don't `format!` or allocate for
telemetry that may be discarded.

## wasmtime 37 (feature `wasm`)

- bindgen uses `imports: { default: async }` — **not** `async: true`.
- `wasmtime_wasi::WasiView` returns a `WasiCtxView`.
- Host `add_to_linker` needs `HasSelf<T>`.

## v8 150 (feature `js`)

- Pinned scopes: the `v8::scope!` / `v8::tc_scope!` macros; helpers take
  `&mut v8::PinScope<'s,'_>`.
- `TryCatch` is interrogated only through the concrete
  `PinnedRef<TryCatch<HandleScope>>` type.
- `Value::to_rust_string_lossy(&self, &PinScope)`.
- Sandbox `console.*` and the WIT `log()` both route to `HostApi::log`; the JS
  prelude (`engines/js_prelude.js`) maps `console.log/info→info`, `warn→warn`,
  `error→error`, `debug→debug`.
- **The prelude startup snapshot is OFF on every platform, and must stay off
  until deno_core is upgraded.** Booting an isolate from a custom V8 startup
  snapshot *while another isolate is alive in the process* aborts inside V8's
  `SharedHeapDeserializer` (hardened-libc++ `vector[]` OOB). It is a **fastfail
  — the whole process dies** with no unwind and no response (`0xC0000409` on
  Windows, SIGILL on Linux). Measured 72/252 aborted runs (16–43%) with the
  snapshot on and isolates overlapping; 0/192 with it off, or with isolate
  lifetimes serialized.
  - **Overlap is the trigger** — not concurrent *creation*, not load. It
    reproduces with creation behind a mutex, and a never-dropped anchor isolate
    makes it *worse* (that anchor guarantees the overlap).
  - **Both JS paths overlap in normal operation**: `invoke` builds an isolate
    per invocation (two concurrent requests suffice), and `engines::resident`
    keeps one alive for the life of the process, so on a node with a loadable
    adapter mounted *every* invocation overlaps it.
  - This was believed Windows-safe and was not — do not re-enable per-platform.
    Flip `USE_PRELUDE_SNAPSHOT` in `engines/js.rs` only together with a
    deno_core upgrade, and prove it with `tests/js_isolate_overlap.rs` under
    repeated parallel runs (a clean pass is the signal — the failure mode is a
    process abort, not a failed assertion).
- **Editing the bootstrap or `js_prelude.js` still means regenerating the
  prelude snapshot.** The blob (`engines/js_prelude.snapshot.bin`) is kept
  current for the eventual re-enable, and `tests/prelude_snapshot.rs` fails on
  drift; fix with
  `cargo run -p rs2-core --example gen-js-snapshot --features js`, then commit
  the regenerated `.bin` + `.hash`. The snapshot can't be built in the serving
  process (V8 inits in snapshot *or* normal mode per process, not both), which is
  why generation is a separate example, not automatic. When it is re-enabled it
  skips recompiling ~500 lines of prelude (~4× faster per invocation); running
  from source costs ~10 ms per isolate creation.

## Smaller traps

- `jsonschema` is pulled with `default-features = false` — don't assume default
  features are on.
- `ureq` has no `json` feature here: use `into_string()` + `serde_json::from_str`,
  not `into_json()`.
- The `rs2` CLI `main` is not a `Result`-returning fn — no bare `?`; chain with
  `and_then`/explicit match.
- `serde_json::Value` numeric `From`: `status as i64` / `n as i64` coerce
  cleanly into `Value`; `u128` (e.g. `timeUnixNano`) is serialized as a **string**
  to survive JSON number precision.
