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
  src/services/           prebuilt: file, data, pipeline, auth, services
  src/capabilities/       FileStore/DataStore/HttpOut traits + tenant scoping
  src/adapters/           local fs file store, in-memory data store
  src/tenant.rs           tenant config → built tenant (atomic)
  src/runtime.rs          lazy tenant load + dispatch + control plane
  tests/conformance.rs    engine-neutral conformance suite
  tests/runtime_services.rs  full-path integration tests
  tests/m2_composition.rs    demo tenant e2e, G6 idempotency proof, G7 bench
rs2-server/               the supported v1 binary (hyper listener)
conformance/echo-guest/   Wasm guest component for engine conformance
```

## Build & test

```powershell
cargo test                       # default features: native engine only, fast build
cargo test --features wasm       # + Wasmtime component engine

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
  `?confirm=` dataset delete).
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
- `query` service and agent surface are M3 scope.
