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
and `cargo build` (workspace). Per AGENTS.md, update the `rs2-skill` repo for any
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

## Phase 2 — resident adapter runtimes + loadable `DataStore` — DONE

A deployed JS module backs a data mount's **persistence**, kept resident so
connections pool across requests. Shipped:

**A. Resident runtime subsystem** (`rs2-core/src/engines/resident.rs`).
- `spawn_resident` puts a `deno_core::JsRuntime` on a dedicated OS thread (V8 is
  `!Send`) with a current-thread tokio runtime; the bundle is built once
  (`build_runtime`) and the thread loops on `mpsc::UnboundedReceiver<Job>`, where
  `Job { input, config, reply: oneshot::Sender<Result<Value, RsError>> }`. Build
  errors surface at spawn (a `ready` oneshot), not at the first job.
- The long-lived `InvocationState` (host, **socket registry**, allowlist) lives in
  the runtime's `OpState`, so sockets opened in job N persist to N+1 — the adapter
  pools connections in a module-level JS var (proven: the Redis mock accepts exactly
  one connection across a full store-contract run). `dispatch_once` clears the
  per-call host-error slot + OOM flag and cancels any pending termination, so the
  isolate survives a killed call and is reused.
- `ResidentHandle { tx }` (clonable); `async fn call(input, config)`. Host socket I/O
  still re-enters the **main** runtime via `block_on_main`, as for per-invocation.

**B. Refactored `engines/js.rs` to share with resident mode.** Extracted
`build_runtime(source, inv, limits, oom) -> (JsRuntime, default_export)`
(extensions + heap cap + bootstrap + prelude + module load/evaluate, watchdog-guarded)
and `dispatch_once(&mut runtime, default_export, input, config, inv, limits, oom)
-> Result<Value, RsError>` (clear-state + watchdog + call + drive loop + envelope +
timeout/OOM/host-error mapping). Per-invocation `JsEngine::invoke` is `build_runtime`
+ one `dispatch_once`; the resident loop is `build_runtime` + many. Conformance +
npm-compat + socket suites unchanged (no service regression).

**C. `GuestDataStore`** (in `resident.rs`) impls `DataStore` by store-pattern messages
→ `ResidentHandle::call`: `get`→`GET /{ds}/{key}`, `put`→`PUT …` (201/200 = created/
updated), `delete`→`DELETE …`, `list_keys`→`GET /{ds}/?$take&$skip`,
`list_datasets`→`GET /`, `get/put_schema`→`…/.schema.json`,
`delete_dataset`→`DELETE /{ds}/?confirm=`. Non-2xx → `RsError` by status class
(`store_error`). Built with a `GrantedHost` + socket allowlist from `store.grants`.

**D. Config seam + `Tenant::build` wiring** (`data_capability`, `tenant.rs`). A `data`
mount with `"store": {"adapter":"code:…","grants":{…}}` gets a `GuestDataStore` as its
`data` capability instead of the built-in `ScopedDataStore`; the bundle loads lazily
on first request from `.rs2-code/<name>/<version>.js`. The stock `DataService` runs
unchanged on top (schema validation, ETags, `.schemas`, the store contract). A non-JS
build rejects such a mount with 501 at config time.

**E. Proof case** (`tests/guest_adapter.rs`, `--features js`). A Redis (RESP) adapter
bundle over the socket capability against an in-process mock Redis, held to the store
contract — the `store_conformance` shape over a guest-backed `DataStore` — plus the
single-connection pooling assertion. Resident-level unit tests in `resident.rs` cover
module-state persistence across jobs and build-error-at-spawn.

**Deviations from the original sketch (tracked):**
- **No `ResidentPool` on the `Runtime`.** Lifecycle is tied to the `GuestDataStore`,
  which the tenant owns: each holds a one-slot `Mutex<Option<ResidentHandle>>` (lazy
  spawn, re-spawn if the thread died). A config change rebuilds the tenant, dropping
  the old store + runtime + sockets — so "new `code_ref` evicts the old" holds without
  node-global state (keeps rs2-core state-free per `architecture.md`). Timer-based
  **idle eviction** of an unused-but-loaded mount is not implemented (tenants aren't
  idle-evicted either, so it's consistent) — a follow-on.
- **MongoDB adapter deferred.** Redis (RESP) is the shipped proof; the Mongo client
  (OP_MSG + BSON + SCRAM-SHA-256) is a larger vendoring job — Phase 3.

## Phase 3 — follow-ons

**`GuestQueryStore` — DONE.** A second guest-backed capability on the same resident
substrate: a `query` mount with `"store": {"adapter":"code:…"}` runs stored queries
through a resident JS adapter. The query service still substitutes JSON templates /
validates params first (unchanged), then `run_query` ships `{query, params, take,
skip}` to the adapter as `POST /query` and reads `{rows, total}` back. The shared
runtime machinery was extracted into `ResidentAdapter` (lazy spawn + re-spawn +
call), wrapped by both `GuestDataStore` and `GuestQueryStore`; wiring is
`query_capability` in `tenant.rs` (mirrors `data_capability`; `store.root` for the
authoring subtree and `store.adapter` for the execution backend coexist). `quote` is
synchronous so it can't round-trip to the isolate — it uses the reference adapter's
scalar default (SQL adapters bind via params and never hit it). Proof:
`guest_backed_query_store_executes_a_stored_query` in `tests/guest_adapter.rs` — a
Redis-backed query adapter scans + filters a dataset over its pooled socket (one
connection per resident mount, asserted). This validates the Phase 2 claim that the
substrate is generic, not data-specific.

**Resident N-pool + idle eviction — DONE.** `ResidentAdapter` is now a small
per-mount pool instead of a one-slot handle. It **grows lazily under concurrent
load** up to `maxRuntimes` (default 4, config `store.maxRuntimes`): each runtime
serializes its own jobs, so `acquire` dispatches each call to the **least-busy**
runtime, spawns a new one when all are busy and there's room, and a serial workload
stays at one (the spawn awaits under the pool lock so concurrent first-calls don't
over-spawn; an `InflightGuard` releases the reservation on completion/unwind). A
**background sweeper** (started once, holds a `Weak` to the pool so it stops when the
adapter drops) evicts runtimes idle longer than `store.idleMs`/`store.idleSeconds`
(default 60 s; `0` disables) — dropping the isolate and its pooled sockets;
`cached_source` (a `OnceCell`) reads the bundle once and reuses it for every spawn.
Proof: `pool_grows_under_concurrency_and_caps_at_max_runtimes` (four overlapping
calls against a delayed mock grow the pool to `maxRuntimes=2` and no further) and
`idle_runtimes_are_evicted_and_respawn` (an idle runtime is dropped, the next call
re-spawns a fresh connection) in `tests/guest_adapter.rs`.

**`GuestFileStore` — DONE.** The third guest-backed capability: a `file` mount with
`"store": {"adapter":"code:…"}` serves its files from a resident JS adapter.
`GuestFileStore` (in `resident.rs`) maps the eight `FileStore` methods to store-
pattern messages — `HEAD`/`GET`/`PUT`/`DELETE`/`MOVE` on `/{path}`, container
listings on `/{path}/` — wired by `file_capability` in `tenant.rs` (the bundle is
still loaded from the built-in store, so no circularity). File **contents cross the
JSON boundary base64-encoded** (the message envelope is JSON): `write` materializes +
encodes, `read` decodes and rebuilds a `Body` with a content-hash `Replayable`
version so the file service's ETags/304s keep working; a `Range` is sliced host-side
(a backend with native ranges, or a presigned-redirect mode, is the streaming
follow-on). Proof: `guest_backed_file_store_satisfies_the_store_contract` runs the
full store contract (PUT/GET/keyless-POST/list/pagination/DELETE + `?confirm=`) plus
`HEAD` and a `Range`→206 against an in-memory guest file adapter.

**MongoDB adapter — DONE (no-auth).** A real-wire-protocol `DataStore` adapter,
purely a JS bundle on the existing `GuestDataStore` (no rs2-core change): it speaks
**OP_MSG framing + a hand-written BSON codec** over the pooled socket. Datasets are
collections, a record's key is its string `_id`; CRUD maps to `find`/`update`(upsert)/
`delete`/`count`, listings to `find`+`count` and `listCollections`, dataset delete to
`drop`, schema to a `__rs2_schemas__` collection. Proof:
`guest_backed_mongo_data_store_satisfies_the_store_contract` runs the full store
contract against an **in-process mock `mongod`** (OP_MSG + a hand-rolled BSON
subset over `serde_json::Value` — the `bson` crate was tried but bloats the dep graph
past the MSVC linker's module limit, error 1318). **SCRAM-SHA-256 auth is the one gap**:
it needs HMAC/SHA-256/PBKDF2, which the JS prelude's `crypto` doesn't expose (no
WebCrypto `subtle`), so the adapter targets unauthenticated/network-trusted MongoDB;
adding a `crypto.subtle` to the prelude (the host already has `hmac`/`sha2`) would
unlock it — the only remaining piece.

**Outbound credential injection — DONE.** Host-side auth attached to an outbound
message before it leaves the host (the v1 "proxy adapter"), the secret never in
tenant config or guest-visible config. `adapters/credential.rs` defines
`AuthStrategy` (bearer/header/basic/query/HMAC/AWS SigV4) + `CredentialInjector`
(`apply` materializes the body only for signing strategies; the rest are
streaming-safe). Resolved at `Tenant::build` from an `httpOut` grant's (or a
`proxy` mount's) `inject` ref — `"infra:<name>"` (operator-supplied) or an inline
strategy whose `secret:<name>` leaves draw on granted tenant secrets — into
`ServiceContext.outbound_injectors`, applied in the `httpOut` grant closure
(`services/code.rs`) and the `proxy` service. Hex/HMAC/SHA-256 share `crypto.rs`
(factored out of `pipeline/transform.rs`). Proof: `tests/credential_inject.rs`
(infra bearer + inline `secret:` query, both asserting no leak to
`GET /services/raw` or guest `ctx`) and `tests/proxy.rs`. SigV4 golden vector in
`adapters/credential.rs`.

**Typed swappable provider capability (`MessageGateway`) — DONE (reference domain).**
The chosen model for "switch one external provider for another" is a typed Rust
trait per *domain* (the `QueryStore` precedent: one trait, many providers), with
dual impls — a Rust `builtin:`/embedder default and a loadable `code:` guest over
the resident substrate — so a new *provider* needs no recompile, only a new
*domain* touches the host. Outbound messaging is the shipped reference:
`MessageGateway` + `ScopedMessageGateway` (`capabilities/message.rs`),
`GuestMessageGateway` mapping `send`/`status` to `POST /send` /
`GET /status/{id}` envelopes (`resident.rs`), `message_capability` wiring
(`tenant.rs`, mirrors `data_capability`), two first-party builtins
(`adapters/cf_email.rs`, `adapters/aws_sns.rs`) and a stock `message` service
(`services/message.rs`). Proof: `tests/message_gateway.rs`,
`conformance/http/messaging.test.ts`. A `proxy` service (`services/proxy.rs`)
packages the forward-with-auth case as a no-code mount. Follow-on domains
(`SignerGateway`/KMS, an LLM trait — deferred as its surface is leaky and
fast-moving) repeat the same six seams.

**What building two providers at once taught the trait.** A domain trait written
against one provider encodes that provider's accidents. Both of these are real
and were found by writing the second adapter, not by design:

- **Delivery status is not universal.** AWS SNS has no per-message status API
  (it needs CloudWatch delivery logging); Cloudflare's REST send answers
  per-recipient *at send time*. So `status` sits behind a declared
  `delivery_status()` facet — the `listing_pushdown` pattern — and the service
  turns "no" into a 501 naming the provider, rather than every caller
  discovering it by trying.
- **A message id is not universal either.** SNS mints one; the Cloudflare REST
  send mints none, because it already told you what happened. `Receipt.id` is
  therefore optional, with the provider's own answer passed through in `detail`.
  Between `id` and `delivery_status()` a caller can tell which of the three
  provider shapes it is talking to.

The corollary for the next domain: build the second adapter *before* freezing
the trait, and prefer a declared facet to a method every provider must fake.

**Native listing pushdown — DONE.** The projected-listing contract
(`rs2-core/src/listing.rs`, G-series `$select`/`$sort`) reaches guest adapters
via a **feature handshake**: a bundle may `export const features =
["list-records"]`; `build_runtime`/`spawn_resident` read the export at module
evaluation and `ResidentAdapter` records it (lazy spawn, so
`listing_pushdown()` reads `false` until first use). With the feature,
`GuestDataStore::list_records` forwards
`GET /{ds}/?$select=…&$sort=…&$take=…&$skip=…` (values percent-encoded) and
parses `{entries: [{name, fields}], total}`; without it, the shared
`list_records_fallback` (`capabilities/mod.rs`) key-walks the guest's
get/list_keys — `$select`/`$sort` are never forwarded unadvertised.
`mongo-data.js` implements the native path (`find` + projection/sort/skip/limit
+ `count`, `_id` asc appended as the contract's key tiebreak; missing-vs-null
collation deviation documented in `guest-adapters/README.md`). Proof:
`guest_data_listing_fallback_and_native_pushdown_match_the_contract` in
`tests/guest_adapter.rs` — the store-conformance listing contract over both a
non-advertising Redis mount (fallback) and the Mongo bundle against a mock
mongod that sorts server-side, plus the `listProjection` catalogue signal.

## On the Worker host

The Cloudflare host ships the same guest-backed stores (P4b,
`rs2-worker/src/capabilities/guest-stores.ts` + `invokeAdapter` in
`engines/dynamic-worker.ts`): the mapping, error identities, `features`
handshake, and listing pushdown are this document's design, ported. What
differs is the runtime substrate — one Dynamic Worker isolate per mount
instead of the N-pool (`maxRuntimes`/`idleMs`/`idleSeconds` accepted and
ignored; the platform owns eviction), and I/O objects are request-scoped
there, so a bundle's module-scope socket dies at the invocation boundary
and portable adapters reconnect-and-retry (the shipped bundles and the
conformance fixtures do; the retry path is dormant on this host). See
`cloudflare.md` §A, §C.1, §D and decisions 35–40; the cross-host proof is
`conformance/http/guest-adapter.test.ts`.

**Remaining:** a **`GuestActor`** model for long-lived server-push connections
(Discord gateway / Slack socket-mode — needs a continuously driven runtime, real
wall-clock timers, and an inbound-event egress path: a sibling to the resident
*adapter*, not another `GuestXyz`); **SCRAM-SHA-256** for the Mongo adapter (prelude
`crypto.subtle`); a host-capability tier (Rust `sql`/`kv`/`mongo` as message-shaped
capabilities, also serving the wasm tier); instruction-plane multi-file ESM
resolution (replace `NoopModuleLoader`); a startup snapshot for faster
per-invocation boot; tighten `Deno.core` exposure.
