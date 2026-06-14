# Loadable adapters via deno_core (G13)

Bring-your-own-infra: let a tenant connect custom backends — including
**non-HTTP** wire protocols (Postgres/Mongo/Redis) — without recompiling
`rs2-core`. Today adapters (`FileStore`/`DataStore`/…) are compile-time Rust
impls wired into the global `Adapters` at process start; custom *services* are
loadable (`code:*` bundles) but adapters are not.

The keystone is the JS engine on **`deno_core`** (ops + resource table for
stateful handles, a real event loop, gated raw sockets). We **exclude full
Node/npm compat** (the heavy `deno_runtime` tier); adapters target Deno/Web
APIs or a raw-socket protocol implementation.

Phased; each phase is its own commit(s). Run the matrix before each commit:
`cargo test -p rs2-core` (native), `--features js`, `--features wasm` (build),
and `cargo build` (workspace). Per AGENTS.md, update `C:\dev\rs2-skill` for any
user-visible change in the same pass.

## Phase 0 — deno_core engine swap — DONE (`e8f1ec3`)

Replaced the hand-rolled `rusty_v8` (v8 150) engine with `deno_core 0.404`,
**preserving the guest contract byte-for-byte** (existing bundles run unchanged).
`engines/js.rs` now:

- Host bridge = `#[op2]` ops (`op_rs2_request`/`log`/`state_get`/`state_put`/
  `fetch`/`random`) reading an `InvocationState` from `OpState`, delegating to the
  existing `HostApi`/`GrantedHost` (unchanged).
- **Host calls are synchronous** (the contract): ops block the isolate thread on
  the host future via the main runtime + a channel (`block_on_main`), avoiding the
  nested-runtime panic `block_on` would cause inside the event loop.
- Structured host errors return a marker the bootstrap rethrows as a JS `Error`
  with `.code`/`.status` (caught path) and are recorded for the uncaught path
  (identity preserved).
- **Lean compat surface:** the proven `js_prelude.js` (console/fetch/TextEncoder/
  btoa/Buffer/URL/timers) is reused as-is; **virtual-time timers** fast-forward via
  a manual drive loop around `run_event_loop` (host ops are sync, so timers are the
  only async surface).

**Why not deno_web/deno_fetch/deno_crypto extensions** (the original plan): they
ship their JS as `lazy_loaded_js` needing `deno_runtime`'s bootstrap to wire the
globals — heavy node-compat-adjacent surface + more upgrade churn than the
hand-rolled prelude. So the compat surface stays in-tree. (`deno_crypto` also pulls
the `aws-lc-sys` Windows build hazard.)

**Deferred from Phase 0** (now Phase 3): dynamic ESM `import` (currently
`NoopModuleLoader`); a startup snapshot for faster boot; tightening `Deno.core`
exposure (capability gating already holds via `GrantedHost`).

## Phase 1 — gated socket capability — DONE (`ed2b3a1`)

Custom JS can speak non-HTTP protocols over a host-gated TCP/TLS socket.

- Socket ops (`op_rs2_sock_connect`/`write`/`read`/`close`) backed by host
  `tokio::TcpStream`, TLS via `tokio-rustls` with the **`ring`** provider (not
  `deno_net` — same bootstrap problem as deno_web; not aws-lc — Windows build).
- Per-mount allowlist: grant kind `{"type":"socket","hosts":[…]}` (`host:port`,
  host-only, or `*.suffix[:port]`), enforced host-side **before connect**,
  default-deny, `capability_denied` identity preserved.
- JS API: `RS2Socket.connect(host, port, {tls}) → {write, read, close}`,
  synchronous from the guest's view. Tested: plain, denial, TLS (rcgen).

Limitation carried into Phase 2: one runtime per invocation, so a connection lives
only for the request (reconnects each time). Pooling needs resident runtimes.

## Phase 2 — resident adapter runtimes + loadable `DataStore` — NOT STARTED

Goal: a deployed JS module backs a data mount's **persistence**, kept resident so
connections pool across requests. Largest phase; ~3 commits.

**A. Resident runtime subsystem** (new `rs2-core/src/engines/resident.rs`).
- `ResidentRuntime`: a dedicated OS thread (V8 is `!Send`) running a current-thread
  tokio runtime + one `deno_core::JsRuntime` built once from the adapter bundle
  (prelude+bootstrap+module evaluated once; `__rs2_dispatch` ready). The thread loops
  on `mpsc::Receiver<Job>`, where
  `Job { input: Value, config: Value, reply: oneshot::Sender<Result<Value, RsError>> }`.
- Per job: call `__rs2_dispatch(default, msg, config)` via the drive loop; reply.
- `InvocationState` (host, **socket registry**, allowlist) becomes long-lived in the
  runtime's `OpState`, so sockets opened in job N persist to N+1 → the adapter pools
  connections in a module-level JS var holding socket ids.
- `ResidentHandle { tx, last_used: AtomicInstant }`; `async fn call(input, config)`.
- `ResidentPool` on the `Runtime` (node-global, like `Adapters`), keyed by
  `(tenant, mount, code_ref)`; **idle eviction** (drop handle → thread exits → runtime
  dropped → sockets closed); re-spawn on next call; a new `code_ref` evicts the old.

**B. Refactor `engines/js.rs` to share with resident mode.** Extract
`build_runtime(source, host, limits) -> JsRuntime` (extensions+bootstrap+prelude+module
load/evaluate) and `dispatch_once(&mut runtime, input, config) -> Result<Value, String>`
(call + drive loop + extraction). Per-invocation `JsEngine::invoke` stays for
**services**; resident mode is for **adapters**.

**C. `GuestDataStore`** — impl the `DataStore` trait by store-pattern messages →
`resident.call`: `get`→`GET /{ds}/{key}`, `put`→`PUT …`, `delete`→`DELETE …`,
`list_keys`→`GET /{ds}/`, `list_datasets`→`GET /`, `get/put_schema`→`…/.schema.json`,
`delete_dataset`→`DELETE /{ds}/?confirm=`. Map response envelope/markers back to the
trait's returns / `RsError`. Built with a `GrantedHost` from the mount's `store.grants`
+ socket allowlist.

**D. Config seam + `Tenant::build` wiring** (the `"data"` arm, ~`tenant.rs:184-185`).
`"store": {"adapter":"code:my-mongo@v1","grants":{…}}` → load the bundle (like
`CodeService::load_code`), get/spawn the `ResidentRuntime` from the pool, wrap in
`GuestDataStore`, and pass it as the mount's `data` capability instead of the built-in
`ScopedDataStore`. The stock `DataService` then runs unchanged on top — inheriting
schema validation, the store contract, ETags, `.schemas`, etc.

**E. Proof case.** Validate the path with **Redis first** (RESP is trivial) against
`store_conformance` run over a guest-backed `DataStore`, then **MongoDB** (OP_MSG +
BSON + SCRAM-SHA-256 over the socket capability; vendor a minimal client or adapt
`deno_mongo`). The test spins a real backend (or mock), deploys the adapter bundle,
mounts data with `store.adapter`.

**Risks:** sharing engine code across per-invocation + resident without regressing
services; one resident runtime per mount serializes that mount's calls (a small
N-pool is a later throughput step); add a `store_conformance` variant over a
guest-backed store.

## Phase 3 — follow-ons

`QueryStore` adapter (push-down); `FileStore` adapter (streaming or
redirect/presigned mode); a host-capability tier (Rust `sql`/`kv`/`mongo` as
message-shaped capabilities, also serving the wasm tier); instruction-plane
multi-file ESM resolution (replace `NoopModuleLoader`); a startup snapshot for
faster per-invocation boot; tighten `Deno.core` exposure.
