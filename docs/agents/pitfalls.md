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
- **Editing the bootstrap or `js_prelude.js` means regenerating the prelude
  snapshot.** `build_runtime` boots each per-request isolate from a committed V8
  startup snapshot (`engines/js_prelude.snapshot.bin`) so it skips recompiling
  ~500 lines of prelude (~4× faster per invocation). The blob is a pure
  optimization — an empty blob falls back to running the prelude from source —
  but a stale blob bakes the *old* prelude and silently ignores your edit.
  `tests/prelude_snapshot.rs` fails on drift; fix with
  `cargo run -p rs2-core --example gen-js-snapshot --features js`, then commit
  the regenerated `.bin` + `.hash`. The snapshot can't be built in the serving
  process (V8 inits in snapshot *or* normal mode per process, not both), which is
  why generation is a separate example, not automatic.

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
