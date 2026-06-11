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
  src/wrapper/            limits admission, auth stub, error mapping
  src/contract/           HostApi, Engine trait, GrantedHost (capability default-deny)
  src/engines/            native (reference), wasm [feature], js [feature, skeleton]
  src/services/           prebuilt: file, data
  src/capabilities/       FileStore/DataStore/HttpOut traits + tenant scoping
  src/adapters/           local fs file store, in-memory data store
  src/tenant.rs           tenant config → built tenant (atomic)
  src/runtime.rs          lazy tenant load + dispatch
  tests/conformance.rs    engine-neutral conformance suite
  tests/runtime_services.rs  full-path integration tests
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
