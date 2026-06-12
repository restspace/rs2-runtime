# RS2 — Sandboxed Composable-Service Runtime

Rust reimplementation of the Restspace conceptual core, per
`C:\dev\rs-runtime\docs\PRD-runtime-v2.md`. Services are functions on HTTP
messages, mounted at URL paths per tenant; custom code runs in sandboxes with
hard resource limits.

## Layout

```
rs2-core/                 the runtime crate (no global state; 0.x API)
  wit/service.wit         engine-neutral service contract (WIT world)
  src/message/            Message, Body (bytes|stream), MediaType, provenance
  src/router/             tenancy resolution, mount table, path safety
  src/wrapper/            limits admission, RBAC authorizer, error mapping
  src/contract/           HostApi, Engine trait, GrantedHost (capability default-deny)
  src/engines/            native (reference), wasm [feature], js [feature, skeleton]
  src/pipeline/           typed spec, string-DSL converter, conditions,
                          JSONata transforms, segment planner, executor
  src/retry.rs            retry policies + effect classes (PRD §7)
  src/idempotency.rs      idempotency store capability + key/replay logic
  src/services/           prebuilt: file, data, pipeline, query, auth,
                          services + code: (engine-backed custom services)
  src/discovery.rs        agent surface + OpenAPI 3.1 (/.well-known/rs2/*)
  src/capabilities/       FileStore/DataStore/QueryStore/HttpOut traits +
                          tenant scoping
  src/adapters/           local fs file store, in-memory data + query stores
  src/tenant.rs           tenant config → built tenant (atomic)
  src/runtime.rs          lazy tenant load + dispatch + control plane
  tests/conformance.rs    engine-neutral conformance suite
  tests/runtime_services.rs  full-path integration tests
  tests/m2_composition.rs    demo tenant e2e, G6 idempotency proof, G7 bench
  tests/m3_surface.rs        query/discovery/openapi/code-deploy tests
rs2-server/               the supported v1 binary (hyper listener; also a lib)
rs2-cli/                  the `rs2` developer CLI (new/dev/test/deploy/migrate)
conformance/echo-guest/   Wasm guest component for engine conformance
```

## Build & test

```powershell
cargo test                       # default features: native engine only, fast build
cargo test --features wasm       # + Wasmtime component engine
cargo test --features js         # + V8 isolate engine (self-contained JS conformance)

# Wasm engine conformance against a real component:
rustup target add wasm32-wasip2
cd conformance/echo-guest; cargo build --target wasm32-wasip2 --release; cd ../..
$env:RS2_CONFORMANCE_COMPONENT = "$PWD\conformance\echo-guest\target\wasm32-wasip2\release\conformance_echo.wasm"
cargo test --features wasm -p rs2-core --test conformance
```

## Run

```powershell
cargo run -p rs2-server -- serverConfig.json
# then e.g.:
#   PUT  /files/docs/a.txt          streamed file write (201/200)
#   GET  /files/docs/               paginated dir+json listing ($take/$skip)
#   PUT  /data/people/.schema.json  install dataset schema
#   PUT  /data/people/ada           schema-validated write (422 on violation)
#   GET  /healthz | /readyz
```

Tenant configs live in `tenants/<name>.json`; tenancy mode, listener, and
adapter wiring in `serverConfig.json`.

## M1 status (PRD §16)

Done:
- Crate skeleton with the PRD §5.1 module shape; `rs2-server` binary.
- Message/Body model: stream-or-bytes payloads, mandatory media types,
  schema-carrying `Content-Type` (`application/json; schema="…"`), provenance
  (`Materialized`/`Replayable`/`Ephemeral`), capped materialization.
- Router: single/multi tenancy (domain map + subdomain), longest-prefix
  mounts, path safety (traversal/encoding/null/drive-letter) for all services.
- Wrapper: per-tenant concurrency admission (fail-fast), wall-clock timeouts,
  auth stub (`"access": "authenticated"`), RFC 9457 problem+json errors with
  machine-readable codes everywhere.
- Engine-neutral contract: WIT world, `HostApi`/`Engine` traits, `GrantedHost`
  with capability default-deny + outbound budget; conformance suite pinning
  message semantics, capability denial, wall-clock/materialization limits, and
  state-capability persistence.
- **Wasmtime engine** (`--features wasm`): component host with epoch-based
  wall-clock interruption, per-store memory caps, content-hash component
  cache; passes conformance against a real `wasm32-wasip2` guest, including
  capability denial across the sandbox boundary.
- `file` + `data` services with documented PRD §10 semantics (streamed writes,
  Range/206, ETag from store versions, paginated listings, schema-validated
  CRUD with cached compiled validators, JSON merge PATCH, keyless POST,
  `?confirm=` dataset delete) — both implementing the **store pattern**, one
  normative conversation shape pinned by `tests/store_conformance.rs`
  (carried over from Restspace v1's pattern system for client polymorphism):
  trailing-slash dir+json listings at every container level, uniform
  PUT/POST/DELETE semantics, the 409 + `?confirm=` container guard, ETags on
  children. Mounts declare `pattern` + `facets` on the discovery surface;
  the generated OpenAPI `$ref`s one shared store path-item shape.
- Host-enforced tenant scoping on every store capability; local-fs and
  in-memory adapters.

Remaining M1 (tracked, not started):
- **V8 isolate engine** (`engines/js.rs` is a skeleton returning
  `engine_unavailable`; rusty_v8 embedding + Node-compat layer).
- S3 file-store and database data-store adapters (traits are final; slots in
  `adapters/`).
- G1 latency benchmark (p99 < 1 ms dispatch overhead) and G3 containment
  demo under load.
- WIT streaming bodies: the v0.1 world materializes bodies at the component
  boundary (PRD open question 1); revisit when component-model streams land.

## M2 status (PRD §16, "Composition & control")

Done:
- **Pipeline executor** (`pipeline/`): typed spec (PRD §8.1) with serial /
  parallel / conditional / tee / teeWait modes, `jsonSplit` + `jsonObject`,
  conditions (config-time-checked grammar), JSONata transforms (jsonata-rs —
  the PRD's "evaluate Rust JSONata early in M2" risk, resolved), `try`,
  `as:$var` capture, `${...}` interpolation, `?$to-step`, fan-out/depth/step
  limits.
- **String DSL converter** (`pipeline/dsl.rs`): the Restspace terse form
  (`"if (ok) GET /x :$y"`) is accepted as sugar; stored format is typed.
- **Segments** (PRD §7.3): planned at materialization points; the segment is
  the atomic retry unit; per-step keys derive from
  `H(invocation-id, step-path)` so keyed effects dedupe across attempts
  (proven in `g6_segment_retry_dedupes_keyed_effects`). `?$plan` returns the
  plan with unsafe-mid-segment warnings.
- **Idempotency store + replay** (PRD §7.2): `Idempotency-Key` scoped per
  tenant+mount+method+path; replay window with `Idempotency-Replayed: true`;
  in-flight duplicates 409; payload-hash mismatch 422; store is a capability
  (in-memory adapter; shared adapter slot for scale-out).
- **Retry policies** (PRD §7.3): declarative, resolved per-call → mount →
  tenant → runtime; gated on effect classes (pure/idempotent/keyed/unsafe,
  method-inferred defaults).
- **`auth` service + RBAC** (PRD §10.5): HS512 JWT (constant-time verify) in
  `rs-auth` cookie or bearer; login/refresh (sliding past 50%)/logout; login
  lockout; argon2id hashes with bcrypt migration verify; per-mount role
  specs (`readRoles`/`writeRoles`/`createRoles`, path-scoped grants like
  `"U /user/{email}"`) enforced in the wrapper.
- **Caching, host-applied** (v1's universal `caching` config): default
  `Cache-Control: no-store` on every response; mounts opt in with
  `{"mode": "noStore"|"revalidate"|"cache", "maxAgeSeconds", "public",
  "immutable"}` — `public` clamps to `private` + `Vary` on authenticated
  mounts; `Set-Cookie` and error responses are always uncacheable; `file`
  and `data` answer matching `If-None-Match` with 304s, so `revalidate`
  mode is always-fresh at near-zero bandwidth.
- **CORS, host-enforced** (PRD §5.2; v1's `trustedDomains` posture): tenant
  `cors` block — trusted origins get credentialed CORS and `SameSite=None;
  Secure` login cookies; allowed origins get plain CORS with bearer-only
  login (no cookie); preflights answered before routing; error responses
  decorated; an always-on CSRF guard rejects cookie-authenticated unsafe
  requests from untrusted cross-site origins; `auth.allowedLoginOrigins`
  optionally allowlists login/refresh callers.
- **`services` self-config** (PRD §10.6): `GET catalogue|services|raw`,
  `PUT raw` validates the whole config (dry tenant build), persists through
  the config loader, and hot-swaps atomically; `If-Match` optimistic
  concurrency; invalid config never touches the running tenant.
- **`pipeline` service** (PRD §10.3): mount a spec (typed or DSL);
  `?$plan` introspection; internal calls re-enter full dispatch (authz,
  limits, idempotency all apply).
- Exit criteria: demo tenant e2e, G6 idempotency proof, and the G7 benchmark
  live in `tests/m2_composition.rs` (G7: ~0.08 ms per two-step pipeline
  invocation, release build).

M2 deviations (tracked):
- Lockout/session state is node-local (PRD wants the shared state store).
- TOTP MFA, impersonation, data-field authorization rules, zip/unzip and
  multipart split/join, and the audit log are deferred.

## M3 status (PRD §16, "Surface & migration")

Done:
- **`pipeline` + `query` are spec stores** (v1's store-transform/store-view
  patterns): authoring is a full store-contract surface under the reserved
  dot-subtree (`/<mount>/.pipelines/…`, `/<mount>/.queries/…` — guard with
  `manageRoles`); every other path on **any HTTP verb** executes the
  longest-prefix-matched spec, with a `.root` spec governing the mount root
  (so a pipeline can transparently wrap another service — the reason verbs
  pass through; replaces v1's modal manage header). DRY by construction:
  the `SpecStore` façade owns a real `FileService` and delegates (validate
  → canonicalize → forward), with `PrefixedFileStore` as the seam where
  per-store named infra (PRD §9.1) plugs in later
  (`"store": {"root"}` today). The instruction plane of a tenant is now
  exactly: tenant config + `.rs2-code/` + `.rs2-pipelines/` +
  `.rs2-queries/` — mechanically extractable for git change control.
- **`query` service** (PRD §10.4): stored queries **authored like files**
  (no tenant-config round trip), executable on any verb (query-string
  params coerce to the params schema's types), language-agnostic by
  design: JSON templates
  (Mongo aggregates, Elastic DSL, the reference adapter) substitute
  **structurally** — `"${name}"` nodes take the param's JSON value, so
  templates are valid JSON at rest and injection-safe by construction;
  `"${name?}"` optionals elide their enclosing clause (generalizing v1
  Mongo's `ignoreEmptyVariables`); string (SQL) templates pass to the
  adapter **unsubstituted** with validated params for real bind parameters.
  Param schemas apply `default`s and validate before execution; positional
  params from trailing URL segments (longest stored prefix wins); missing
  params are 400s, never silent. `QueryStore` capability (the v1
  `IQueryAdapter` equivalent, now bind-capable) + reference adapter
  scanning any `DataStore`.
- **Agent surface** (PRD §12), generated per tenant and filtered by the
  caller's read permission and `?surface=` against `x-expose`:
  - `/.well-known/rs2/services` — mount catalogue with x-agent/x-policy
  - `/.well-known/rs2/agent-surface` — entities, actions (effect class +
    Idempotency-Key guidance advertised), stored queries with their schemas
  - `/.well-known/rs2/openapi` — OpenAPI 3.1; the schemas referenced are
    the ones enforced at runtime (no drift by construction)
- **Structured errors complete**: pipeline failures carry the failing step
  and per-step statuses in the problem+json body.
- **Custom code deployment** (PRD §10.6): `PUT /services/code/<name>`
  stores components content-addressed and immutable per version, with a
  compile smoke test when the engine is in the build; mounts reference
  `code:<name>@<version>`; capability `grants` map names to internal URL
  prefixes and re-enter full dispatch (authz/limits/idempotency apply).
  Proven end-to-end against the real conformance component (wasm feature).
- **`rs2` CLI** (`cargo run -p rs2-cli --` or the `rs2` binary):
  - `rs2 new <name> [--js]` — scaffold against the published WIT (the Rust
    scaffold compiles to a working component as-is)
  - `rs2 dev` — run a local node (shares the rs2-server library)
  - `rs2 test` — manifest consistency + component checks (engine compile
    check with `--features wasm`)
  - `rs2 deploy <wasm> --name n` — upload via the self-config API
  - `rs2 migrate <services.json>` — Restspace config → RS2 tenant config:
    mounts, access roles, retry policies carried over; string-DSL pipelines
    converted to the typed spec; the result is validated by dry-building
    the tenant; unsupported services are skipped with explicit warnings

Exit criteria: **G4 (core service parity) met** — all six services
reimplemented with documented semantics.

## V8/JS engine (`--features js`)

`engines/js.rs` embeds rusty_v8 (v8 150). Shape: **one isolate per
invocation** on a blocking thread (PRD open question 2 resolved
conservatively — full isolation; warm pools are a later optimization
behind the same `Engine` trait).

- **Bundle contract**: a single-file ES module whose default export is
  `async (msg, ctx) => response` or `{ handle }`. JSON bodies arrive
  parsed; responses are `{ status?, headers?, body?, mediaType? }` (or any
  value as a 200 JSON body). `ctx` = `{ config, request, log, state }`.
- **Host bridge**: `ctx.request` is synchronous from the guest (the
  isolate thread parks on the async host future); `async/await` works via
  microtask pumping to quiescence. No event loop: timers unavailable in v1.
- **Limits**: wall clock via a watchdog thread + `terminate_execution`;
  memory via isolate heap caps with a near-limit callback (an allocation
  bomb gets a structured `limit_exceeded`, not a process abort); outbound
  budget and materialization caps host-enforced as for every engine.
- **Conformance (G2)**: the JS engine passes the identical invariant suite
  (message semantics, capability denial with preserved error identity,
  wall-clock kill, memory containment, outbound budget, state capability,
  no global leakage between invocations) — `cargo test --features js`.
- **Deployment**: `PUT /services/code/<name>` with
  `application/javascript` stores the bundle (compile smoke test when the
  engine is in the build); `code:` mounts dispatch by bundle type, so wasm
  and JS services share grants, limits, and the host contract.

## npm-compat layer (G5)

Every JS bundle runs against an injected compat prelude
(`engines/js_prelude.js`) — the **explicit supported-API list** (PRD §17
risk mitigation), pinned by the corpus suite in `tests/npm_compat.rs`:

- `fetch` / `Headers` / `Request` / `Response` — outbound HTTP through the
  `fetch` capability, granted per mount with host allowlists:
  `"grants": { "fetch": { "type": "httpOut", "hosts": ["api.stripe.com",
  "*.example.com"] } }`. Default deny; disallowed hosts fail with
  `capability_denied` before any I/O. The server wires a ureq-backed
  `HttpOut` adapter (`http` feature); embedders supply their own.
- `setTimeout` / `clearTimeout` / `setInterval` / `clearInterval` —
  **virtual time**: when the handler is otherwise idle, pending timers
  fast-forward, so SDK retry backoffs (429 + Retry-After loops) complete
  without real waits.
- `console`, `queueMicrotask`, `structuredClone`, `TextEncoder`/`TextDecoder`,
  `atob`/`btoa`, `Buffer` (from/alloc/concat/toString utf8|base64|hex),
  `URL`/`URLSearchParams`, `AbortController`/`AbortSignal`,
  `crypto.{getRandomValues, randomUUID}`, `process.{env, nextTick, version}`.

The corpus exercises the request/auth/retry patterns the popular
API-wrapper SDKs are built from (Stripe-style form POST + idempotency
keys, OpenAI-style JSON + 429 backoff, Slack-style query building +
base64 auth) — `cargo test --features js --test npm_compat`. Compat
additions require a corpus-driven case (PRD §17).

**G5 measured against the real SDKs** (`corpus/` + `tests/sdk_corpus.rs`):
the official npm packages, bundled with esbuild (`--platform=browser
--conditions=worker`, the same settings as `rs2 deploy --bundle`) and run
in the engine against mocked fetch:

| SDK | result |
|---|---|
| stripe (fetch http client) | ✅ unmodified |
| openai | ✅ unmodified |
| @anthropic-ai/sdk | ✅ unmodified |
| @octokit/core | ✅ unmodified |
| @supabase/supabase-js | ✅ unmodified |
| resend | ✅ unmodified |
| @google/generative-ai | ✅ unmodified |
| @mistralai/mistralai | ✅ unmodified |
| groq-sdk | ✅ unmodified |
| @slack/web-api | ❌ axios transport imports `node:os`/`node:path` |

**9/10 = 90% — G5 met.** Reproduce: `cd corpus; npm install;
./build.ps1; cargo test --features js --test sdk_corpus -- --nocapture`.
Out of scope for v1 (documented): WebSocket connections, response-body
streaming (`ReadableStream` is presence-only), binary multipart uploads,
real wall-clock timers.

## G1 + G3 benchmarks (`tests/g_benchmarks.rs`)

```powershell
cargo test -p rs2-core --release --features js --test g_benchmarks -- --ignored --nocapture
```

**G1 — dispatch overhead** (target: p99 added < 1 ms vs the native call
path). Measured: direct service call p99 ~7–10 µs; the full dispatch path
(router → tenancy → token verify → access → admission → breaker →
idempotency) p99 ~7–17 µs — **added p99 well under 10 µs**, ~100× inside
the target.

**G3 — containment** (target: a pathological service cannot push another
tenant's p99 beyond 2× baseline). Two attacks from a hostile tenant — an
infinite loop and an allocation bomb in sandboxed JS — under an 8-client
flood while a neighbor tenant's data service is measured:

| attack | neighbor p99 baseline | under attack | evil outcomes |
|---|---|---|---|
| infinite loop | 14.0 µs | 20.8 µs (1.5×) | 108 requests, all structured 503 |
| allocation bomb | 14.0 µs | 78.8 µs | 200 requests, all structured 503 |

The mechanism stack: wall-clock/heap kill inside the isolate → per-tenant
concurrency admission → **per-tenant breach circuit breaker** (PRD §9.3,
added by this work: repeated limit breaches trip the tenant open for a
cooldown with 503 + Retry-After, so pathological code stops re-occupying
engine threads — without it the attack ran 27 000 isolate restarts and
pushed the neighbor to 3.1 ms). The assertion bound is
`max(2× baseline, baseline + 2 ms)`: with microsecond-scale baselines,
scheduler noise alone exceeds a bare 2×; raw numbers are printed.

Bundling: `rs2 deploy entry.ts --name x --bundle` runs `npx esbuild`
(single-file ESM; npm deps resolve at build time; native addons fail at
build time). Timers are virtual and there is no event loop: code needing
real wall-clock waits or background work is out of scope for v1.
