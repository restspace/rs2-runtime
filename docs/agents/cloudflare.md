# Cloudflare host — a second implementation of the RS2 HTTP API

RS2 has one API and, after this work, two hosts: the Rust server (`rs2-core` +
`rs2-server`) and a TypeScript Worker (`rs2-worker/`) running natively on
Cloudflare Workers. This document is the **single spec** for the Worker host
and for the black-box HTTP conformance runner (`conformance/http/`) that holds
both hosts to the same contract. Engineers implement from this doc plus the
Rust module it names; nothing here is aspirational — where the brief left a
choice open, the choice is made and logged in §I.

Read first: `architecture.md` (the throughline applies unchanged), `testing.md`
(the Rust test files this runner replaces over HTTP), `loadable-adapters.md`
(the guest contract the shim reproduces).

Contents: A goals · B topology + lifecycle · C primitive mapping + SQLite
schema · D module-by-module port plan · E guest contract · F conformance runner
· G wrangler/package layout · H phases · I decisions log.

---

## A. Goals, non-goals, and the "identical API" contract

**Goal.** A client (the `rs2` CLI, `rs2-ui`'s `src/lib/rs2-client.ts`, the
skill, an agent reading `/.well-known/rs2/`) cannot tell which host it is
talking to except by reading the discovery surface. Every status code, header
name, JSON field, error `code`, and listing shape in this doc is the Rust
behavior, and the conformance runner (§F) asserts it against both hosts.

**Non-goals.** Sharing a database between the two hosts (a tenant lives on one
host; the `transfer` operation in §H P5 moves it). Reproducing Rust internals
that are not observable (hash functions used for opaque versions, the V8
prelude's virtual timers). Wasm components on the Worker.

**What is allowed to differ**, and how each difference is declared:

| Difference | Declared via |
|---|---|
| Wasm component services unsupported | `code:` mount whose bundle is `.wasm` → **501** `engine_unavailable` at first request (same code path Rust uses without `--features wasm`); the `services` catalogue item for `code` lists `engines: ["js"]` |
| Per-invocation limits (memory 128 MiB fixed by the platform; materialized-body cap 32 MiB; CPU rather than wall-clock for guests) | `GET /.well-known/rs2/services` gains a top-level `limits` object on **both** hosts (`{"wallClockMs","memoryBytes","materializedBodyBytes","outboundCalls","maxDepth","host":"rust"|"cloudflare"}`); the Rust side adds it in P1 |
| Guest `ctx.request`/`ctx.state`/`ctx.readBody` are async (Promises) instead of synchronous | facet `guest-async` on every `code:` mount's catalogue entry (Worker only); §E |
| Timers inside guests are real, not virtual | same facet |
| Opaque validators (`ETag` values, config version) are different strings | none — they are opaque by contract; clients round-trip them |
| `conditional-write` is atomic on the Worker (DO-serialized), best-effort on Rust local-fs | already a declared facet strength (`FileStore::conditional_write_atomic`) |
| Guest (`code:`) store adapters: `store.maxRuntimes` / `store.idleMs` / `store.idleSeconds` are accepted and **ignored** (one Dynamic Worker isolate per mount; the platform owns eviction) | documented here + §I decision 36; surfaced the same way `limits.cpuMs` is (a Worker-only knob the other host ignores) |
| Guest adapters cannot pool a backend connection across requests: I/O objects are request-scoped on the Workers platform, so a socket pooled in a bundle's module scope dies at the invocation boundary and the adapter reconnects per invocation (on Rust the resident isolate pools it for the mount's lifetime) | `guestAdapterPooling` in `src/divergences.ts` (`"pooled"` \| `"perInvocation"`); §I decision 38 |
| Attaching a domain: the Worker attaches over HTTP behind the proof-of-control gate (§B.5); the Rust host's tenancy map is static config read at startup and its TLS belongs to a reverse proxy, so it answers the **read** side identically (`provider: "static"`) and refuses `PUT`/`DELETE` with 501 `provider_unavailable` naming `serverConfig.tenancy.domainMap` | `domainAttachment` in `src/divergences.ts` (`"api"` \| `"config"`); §B.5 |
| `DELETE` of a directory that never existed: Rust local-fs → 404; R2 has no directories → **204** | runner accepts `204|404` for this one case (§F); documented, not declared |
| Dot segments in the request target (`/files/../x`, `/files/%2e%2e/x`) | The Workers platform canonicalizes them before the Worker runs (`request.url` already reads `/x`), so the router's 400 `path_unsafe` is unreachable and the request routes on the normalized path (404 for an unmounted target); `%00`, `\`, control characters and drive letters still reach the router and are 400 | runner accepts `400|404` for the two dot-segment cases (`dotSegmentTraversal` in `src/divergences.ts`); documented, not declared |
| Log storage: per-tenant SQLite rows with a cap instead of rotated NDJSON files | not observable through the `log` reader contract |

Everything else — including the odd corners (405 responses that carry
`code: bad_request`, `If-Match` mismatch on `PUT /services/raw` being **409**
not 412, conditional headers silently ignored on data `PATCH` and keyless
`POST`) — is reproduced exactly. Do not "fix" Rust behavior in the port; file
it as a both-hosts change.

---

## B. Topology and request lifecycle

### B.1 Components

```
                 ┌──────────────────────────────────────────────┐
  client ──────► │ Worker (stateless, every colo)               │
  Host: x.y.z    │  index.ts: ops endpoints, hostname→tenant,   │
                 │  forward to TenantObject, cron fan-out       │
                 └───────┬──────────────────────────┬───────────┘
                         │ stub.fetch(req)          │ RegistryObject (1 instance)
                         ▼                          │  domainMap, tenant list,
        ┌────────────────────────────────┐          │  infras.json equivalent
        │ TenantObject (DO, 1 per tenant)│◄─────────┘
        │  = Runtime::dispatch + handle  │
        │  KV: tenant config + version   │      R2 bucket RS2_FILES
        │  SQLite: data, idempotency,    │────► keys `<tenant>/<path>`
        │          logs, schedule claims │      (FileStore, spec stores,
        │  memory: breaker, concurrency, │       .rs2-code/, .rs2-store/)
        │          auth lockout, caches  │
        │  alarms: scheduled mounts      │      env.LOADER (Dynamic Workers)
        │  LOADER: code: mounts          │────► one isolate per code ref,
        └────────────────────────────────┘      env = { RS2 }, globalOutbound = gateway
```

One Durable Object class `TenantObject`, `idFromName(tenant)`. One
`RegistryObject` (`idFromName("registry")`) is the operator table. Both are
SQLite-backed DO classes.

### B.2 The routing rule

**Every tenant request routes through the tenant's `TenantObject`.** The
stateless Worker does only what needs no tenant state: ops endpoints, admin
endpoints, hostname resolution, and the `x-trace-id` stamp. There is no
"fast path" that serves a mount from the Worker directly — the breaker,
concurrency admission, idempotency, boundary logging, and the config version
all live in the DO, and a request that skipped them would be a second dispatch
path (the exact thing `architecture.md` forbids). A cache-hit path for openly
readable `file` mounts is a possible P6 optimization and is out of scope here.

Consequence to design around: a DO runs in one location and processes events
on one thread with async interleaving. Per-tenant serialization is what the
Rust `TenantLimiter` (64 concurrent) approximates anyway; the DO enforces the
same cap in memory and fails fast with `limit_exceeded("tenant_concurrency")`.

### B.3 Step-by-step lifecycle

Steps marked **[W]** run in the stateless Worker; **[DO]** in `TenantObject`.

1. **[W] Ops endpoints** (outside tenant routing, verbatim from
   `rs2-server/src/lib.rs`): `GET /healthz`, `GET /readyz` → `200 ok`
   (`text/plain`). `POST /admin/reload-infras` → gated by the `RS2_ADMIN_TOKEN`
   secret via `Authorization: Bearer` or `X-Admin-Token` (constant-time
   compare); no token configured → 503 text `admin endpoint disabled: set
   RS2_ADMIN_TOKEN or serverConfig.adminToken`; bad token → 401 text `missing
   or invalid admin token`; non-POST → 405 text `POST only`. Plus the Worker-only
   tenant-lifecycle admin API (§B.5), same gate.
2. **[W] Hostname → tenant.** Port `router::Tenancy::resolve` exactly: strip
   the port, lowercase; explicit domain map first, then `<sub>.<mainDomain>`
   where `sub` is non-empty and contains no `.`. Sources: the `RegistryObject`
   domain map (cached in the Worker isolate for 30 s), `RS2_MAIN_DOMAIN` (var),
   and — new — `RS2_DEFAULT_TENANT` (var): if set, any host that resolves to
   nothing maps to it. This is the local-dev and single-tenant mode
   (`wrangler dev` ships with `RS2_DEFAULT_TENANT=main`); unset it in
   multi-tenant production. Unresolved → 404 problem+json
   `no tenant for host '<host>'` with `tenant: "-"`, `traceId: "-"`.
3. **[W] Forward.** `env.TENANTS.get(idFromName(tenant)).fetch(request)` with
   the original URL, method, headers, and **streaming body**; add
   `x-rs2-tenant: <tenant>` and `x-rs2-trace-id: <new trace id>` (32 lowercase
   hex, UUIDv4 simple). The DO trusts these two headers only because the
   Worker is the only caller (the DO is not addressable from the internet).
   The Worker copies `x-trace-id` onto the response exactly as the Rust server
   does.
4. **[DO] Build the `Message`** (`message/message.rs`): method, `MsgUrl::parse`
   of path+query, headers, body as a stream with `size` from `Content-Length`,
   `Provenance::Ephemeral`; no body for GET/HEAD or `Content-Length: 0`.
   `source = External`, `depth = 0`.
5. **[DO] `handle`** (`runtime.rs:273`): capture the template; run `dispatch`;
   on `Err` build the problem+json response (`Message::error_response`, with
   `Retry-After: ceil(retryAfterMs/1000)`); decorate CORS for external
   requests with an `Origin`; default `Cache-Control: no-store` if absent and
   status ≠ 304; emit the boundary log (severity by status; 5xx always,
   otherwise ≥ the node log level; attributes exactly as `emit_boundary_log`).
6. **[DO] `dispatch`** in this order — the order is contractual:
   1. `validate_path` (400 `path_unsafe`; traversal, `%2e%2e`, `%00`, `\`,
      control chars, drive letters, checked on raw and decoded forms).
   2. `depth > max_depth (16)` → 503 `limit_exceeded("call_depth")`.
   3. Breaker check (`wrapper::TenantBreaker`; open → 503
      `limit_exceeded("tenant_breaker")` with `retryAfterMs` = remaining).
   4. Tenant build: the DO holds the built `Tenant` in memory keyed by config
      version; rebuild from storage when absent (config PUT purges it).
      Unknown tenant (no config in storage) → 404 `unknown tenant '<t>'`.
   5. CORS (external + `Origin` only): a permitted preflight answers **204**
      before routing (`CorsPolicy::preflight`); then the cookie-CSRF guard
      (403 for an unsafe method + `rs-auth=` cookie from an untrusted
      cross-site origin).
   6. Principal: if `auth.jwtSecret` is set, `principal_from_token` — a
      presented-but-bad token is a **401**, never anonymous.
   7. `/.well-known/rs2/` → `discovery::handle` (405 non-GET, 404 unknown doc).
   8. Longest-prefix mount match (`MountTable::route`) → 404 `no service
      mounted at '<path>'`; `apply_mount`.
   9. `check_access` (fail closed: no `access` → 401/403; pipeline mounts
      defer non-authoring paths to the service).
   10. `OPTIONS` → `describe_mount` + `Allow` (a read-only capability probe).
   11. Declared body size (10 GiB absolute cap) and concurrency admission
       (64, fail fast).
   12. Idempotency (§C.3): `Idempotency-Key` > 256 chars → 400; scope
       `tenant|mount|method|path`; payload hash SHA-256 hex of a materialized
       body, `"empty"` for none, `null` for a stream; `Replay` →
       stored response + `Idempotency-Replayed: true`; `InFlight` → 409
       retryable `retryAfterMs: 1000`; `PayloadMismatch` → 422
       `idempotency_key_reuse`; `Fresh` → invoke, then `complete` (bodies
       ≤ 1 MiB) or `abandon`.
   13. Invoke the service under the service wall clock (30 s) — on the
       Worker this is a `Promise.race` against a timer; a timeout is
       `limit_exceeded("wall_clock_ms")` and feeds the breaker (as do all
       `limit_exceeded` errors except `tenant_concurrency`/`tenant_breaker`).
   14. Apply the mount's `CachePolicy` to successful responses that set no
       `Cache-Control` and no `Set-Cookie` (`public` clamps to `private` +
       `Vary: authorization, cookie` unless the mount is openly readable).
7. **[DO] Internal composition.** Pipeline steps, `prefix` grants, guest
   `ctx.request`, and `x-rs2-body-ref` reads call `handle` **recursively inside
   the same DO** (`source = Internal`, `depth + 1`, child span) — the
   `Requester` is a direct function reference, no network hop.
8. **[DO] Response** streams back to the Worker, which streams it to the
   client. Header sets are copied verbatim; the DO never lowercases a
   client-visible header name beyond what `Headers` does.

### B.4 Tenant config storage and hot reload

The tenant config document lives in the DO's KV storage under `config`
(the raw JSON exactly as PUT, secrets included) and `config.version`
(16 lowercase hex = first 8 bytes of SHA-256 over the stored JSON text —
the Worker's analogue of `FileConfigLoader::version_of`). `GET /services/raw`
returns the redacted document and `ETag: "<version>"`; `PUT /services/raw`
performs the same sequence as `RuntimeControl::put_config`: parse → dry-build
the whole tenant (all `from_config`, spec parsing, schema compilation, infra
expansion, `elevate` guard, wrapper `pattern` guard) → `If-Match` compare
(mismatch → **409** `conflict` `config version mismatch (If-Match): reload and
reapply`) → persist both keys in one `transactionSync` → drop the in-memory
`Tenant` → **204** with the new `ETag`. In-flight requests keep their
references to the old build.

### B.5 Operator surface (Worker-only; the Rust equivalent is files on disk)

Gated exactly like `/admin/reload-infras`. All bodies/responses JSON; errors
problem+json with `tenant: "-"`.

| Verb/path | Body | Result |
|---|---|---|
| `GET /admin/tenants` | — | `{"tenants":[{"name","domains":[…],"configVersion"}]}` |
| `PUT /admin/tenants/<name>` | `{"config": <tenant config>, "domains": ["api.acme.com"], "bootstrapAdmin": {"email","password"}?}` | Validates the name (`/`, `\`, `.` → 400 `invalid tenant name`), dry-builds the config (same errors as `PUT /raw`), writes it into the tenant DO, registers domains in the registry, seeds the admin **if absent** exactly as `seed_bootstrap_admin` (`{passwordHash, roles:"A", kind:"user"}` into `auth.userDataset`; requires `auth.jwtSecret` → 400 otherwise). 201 created / 200 replaced, `ETag` |
| `GET /admin/tenants/<name>` | — | The raw config, redacted like `/services/raw` |
| `DELETE /admin/tenants/<name>?confirm=<name>` | — | Removes registry entries and **deletes the DO's storage** (`storage.deleteAll()`); R2 objects under `<name>/` are **not** deleted (409 without `confirm`) |
| `GET /admin/domains` | — | `{"domains":[{"host","tenant","status":"active"|"pending"}]}`, sorted. Live mappings and unproven claims side by side |
| `PUT /admin/domains/<host>` | `{"tenant"}` | **Claims** the host for the tenant (lowercased; must be a syntactically valid name — LDH labels ≤63, ≤253 overall → 400 otherwise, the same gate as the `domains` array of `PUT /admin/tenants/<name>`). The provider is asked to attach it **before** any registry write, so a provider failure leaves routing untouched. Already proven ⇒ **200** and the host routes; not yet ⇒ **202** and it does not. Claimed by another tenant, or already routing to one, ⇒ 409. Body: the attachment shape below |
| `GET /admin/domains/<host>` | — | The attachment. A live host reads `active`; a pending one is re-polled through the provider, and a host that has since been proven is **promoted here** (200 either way). Unknown host → 404 |
| `DELETE /admin/domains/<host>` | — | 204. Drops the mapping **and** any unproven claim, and asks the provider to remove what it provisioned |
| `PUT /admin/infras` | the `infras.json` document | Stored in the registry; `POST /admin/reload-infras` re-snapshots it and purges every built tenant (each TenantObject drops its in-memory build on next request because the registry bumps an `infrasVersion` the DO compares) |

The `services` mount's `GET /infras` and `infra:` expansion read the registry
snapshot the DO fetched at build time (`InfraSet` semantics from `infra.rs`
unchanged, secrets never leave the DO).

**Domain attachment is gated on proof of control** (`src/domains.ts`). The
registry map is the routing truth and nothing reaches it on a caller's say-so:
a `PUT` records a *claim*, and only a provider reporting the host verified
promotes that claim into the map. So two tenants may both ask for
`app.acme.com` — first claim wins the claim, and only DNS wins the mapping.
A claim is released by `DELETE` or by deleting its tenant; the `*/5` cron
re-polls every pending claim, so a customer who publishes the record and never
comes back is promoted anyway.

The response shape is **host-neutral** — no client parses a provider's
vocabulary:

```json
{ "host": "app.acme.com", "tenant": "acme", "status": "pending",
  "dnsRecords": [ { "type": "CNAME", "name": "app.acme.com",
                    "value": "saas.rs2.example", "required": true,
                    "purpose": "routes the domain to this deployment" } ],
  "nextStep": "publish the required DNS record above at …",
  "provider": { "name": "cloudflare-saas", "detail": { "cfStatus": "pending" } } }
```

`status` is two-valued (`pending` | `active`) because a client's only question
is whether it can send traffic yet; everything a provider knows beyond that is
diagnosis and lives in `provider.detail`. Two providers ship:

- **`cloudflare-saas`** — both `CF_API_TOKEN` and `CF_ZONE_ID` set. Cloudflare
  validates the hostname over HTTP and issues the certificate, so the
  customer's whole side of it is one CNAME to `RS2_CNAME_TARGET`. The
  optional pre-validation TXT is offered while it would still help (before
  the CNAME moves) and dropped once the hostname is active. A single `*/*`
  Worker route on the SaaS zone covers every custom hostname — there is no
  per-domain route to create (`scripts/saas-setup.mjs` does the one-time
  zone wiring; `scripts/domain.mjs` is the per-customer command).
- **`manual`** — the fallback, and the portable one: RS2 mints a token per
  claim and fetches `http://<host>/.well-known/rs2/domain-challenge/<token>`,
  which only this deployment can answer. ACME's HTTP-01 in miniature, no
  external service, certificate someone else's problem. The Worker answers
  that path **before tenant routing** (a host being verified is by definition
  not routing yet) and binds the token to the asking `Host`, so proving
  control of one domain never proves control of another.

### B.6 Scheduler

Port `scheduler.rs` (parse `every`/`cron`, next-occurrence math, interval
bucket) unchanged. Each `TenantObject` owns its schedules: on build it derives
the desired set from config (`reconcile_schedules` logic), computes the
earliest due time, and `storage.setAlarm(due)`. `alarm()` fires every due
mount as `tick_message` (`POST <base>`, `source = System`,
`x-rs2-trigger: schedule`) through `handle`, with the same overlap guard (skip
while a previous fire of that mount is running) and the claim table
(`schedule_claims`, §C.3) so a retried alarm never double-fires an occurrence.
Alarms are **self-arming** (decision 15, as built): every config write and
every `alarm()` firing re-arms the next due time, and DO alarms survive
eviction and deploys, so the Worker cron trigger (`*/5 * * * *`) is only the
safety net for the rare lost alarm (retries exhausted): `scheduled()` asks
the registry for the tenants that carry scheduled mounts — never the whole
tenant list — and calls `stub.reconcileSchedules()` (an RPC method) on each.
A tenant with no scheduled mount has no alarm and costs nothing.

---

## C. Primitive mapping and DO SQLite schema

### C.1 Mapping table

| Rust capability / adapter / module | Cloudflare primitive | Notes |
|---|---|---|
| `FileStore` (`LocalFsFileStore`) | R2 bucket `RS2_FILES`, keys `<tenant>/<path>` | §C.2 |
| `ScopedFileStore` / `PrefixedFileStore` | key-prefix composition (`R2FileStore.prefixed(p)`) | identical `join` semantics: prefix `/`-trimmed, `/<prefix>/<rel>` |
| `DataStore` (`FileDataStore`, `MemDataStore`) | DO SQLite `records`/`schemas` | key order = SQLite `ORDER BY key` (BTreeMap semantics) |
| `QueryStore` (`MemQueryStore` = `builtin:reference`) | ported scan over `records` | §D notes; `builtin:reference` is the only built-in |
| `builtin:local` / `builtin:file` (with `root`) | `R2FileStore.prefixed(root)` / `SqliteDataStore` with a `root` namespace column | `require_store_root` rule kept |
| `builtin:mem` | `SqliteDataStore` namespace `mem` | no longer ephemeral; documented |
| `IdempotencyStore` (`MemIdempotencyStore`) | DO SQLite `idempotency` | transactional `begin` (§C.3) |
| `LogStore` (`FileLogStore`) | DO SQLite `logs`, row cap | Analytics Engine sink = follow-on `MultiSink` member |
| `ScheduleStore` (`MemScheduleStore`) | DO SQLite `schedule_claims` + DO alarms | §B.6 |
| `HttpOut` (`UreqHttpOut`) | `fetch()` from the DO | allowlist stays host-side (`outbound.rs` port); 30 s timeout via `AbortSignal.timeout`; 100 MiB caps become 32 MiB |
| `CredentialInjector` (`adapters/credential.rs`) | ported, WebCrypto HMAC/SHA-256 | SigV4 byte-exact (§D) |
| `TenantBreaker`, `TenantLimiter` | DO memory | correct by construction (one DO per tenant) |
| Auth lockout map (`auth.rs`) | DO memory | now cluster-correct (Rust's is node-local) |
| `ConfigLoader` / `TenantControl` | DO KV `config`, `config.version` | §B.4 |
| `Tenancy` + `serverConfig.json` | `RegistryObject` + vars `RS2_MAIN_DOMAIN`, `RS2_DEFAULT_TENANT` | §B.2, §B.5 |
| `InfraLoader` (`infras.json`) | `RegistryObject` KV `infras` | §B.5 |
| `CatalogueClient` (`HttpCatalogueClient`) | `fetch()` bounded by `RS2_CATALOGUE_HOSTS` (var, comma list) | 64 MiB cap kept |
| JS engine (`engines/js.rs`, prelude) | Dynamic Workers (`env.LOADER`) + guest shim | §E |
| Resident adapters (`engines/resident.rs`: `code:` store/message adapters) | **P4b: `capabilities/guest-stores.ts`** — `GuestDataStore`/`GuestQueryStore`/`GuestFileStore`/`GuestMessageGateway` over a Dynamic Worker per mount (id `<tenant>:<mount base>:adapter:<name>@<version>`), `env {RS2}` + the `EgressSockets` gateway; 501 `engine_unavailable` only when the deployment has no `worker_loaders` binding | pool knobs ignored (§A); sockets reconnect per invocation (§A); §I decisions 35–40 |
| `template` service (JS isolate, `deny_all`) | Dynamic Worker, `globalOutbound: null`, `env: {}` | §D |
| Wasm engine (`engines/wasm.rs`) | **dropped** → 501 | declared (§A) |
| `NativeEngine` | dropped (test-only reference binding) | — |
| `tls.rs` | dropped (platform TLS) | — |
| `crypto.rs` (`hmac`, `sha256`) | WebCrypto | — |
| argon2id / bcrypt | `hash-wasm` | §D auth |
| `jsonschema` (Rust, draft auto-detect) | `ajv` (`Ajv2020` + draft-07 when `$schema` says so) | §D data |
| `jsonata-rs` | `jsonata` (JS reference) | §D transform |
| `Message`/`Body` streams | `ReadableStream` | §D message |
| Boundary + service logging | DO SQLite `logs` | §C.3 |
| Ops endpoints (`/healthz`, `/readyz`, `/admin/reload-infras`) | Worker `index.ts` | §B.3 |

### C.2 R2 as `FileStore` — the semantics an engineer must get right

Key = `<tenant>/<path without leading slash>`. All calls below go through the
DO so per-key ordering is enforceable in memory (an `async` mutex keyed by
full R2 key wraps `write_cond`, `delete_cond`, `rename`).

- `head(path)`: `bucket.head(key)`; on `null`, `bucket.list({prefix: key + "/", limit: 1})` — any object → `{size: 0, is_dir: true}`; else 404
  `resource does not exist`. `size` = `object.size`, `last_modified` =
  `object.uploaded`.
- `read(path, range)`: `bucket.get(key, {range: {offset, length}})`; an
  offset ≥ size is **416** `RsError::new(416, "bad_request", "Range Not
  Satisfiable", "range start <s> beyond resource size <n>")` (R2 throws; catch
  and map). Body = `object.body` stream, media type from `httpMetadata.contentType`
  falling back to `MediaType::for_path`. Provenance `Replayable{url: path,
  version: object.etag}` so `current_etag` yields **`"<etag>"`** (R2's etag is
  already a hex MD5 for single-part uploads; use `object.httpEtag`, which is the
  quoted form, and strip/keep consistently — the contract is "a quoted opaque
  token"). Reading a "directory" (a prefix with no object) → 400
  `path is a directory` only if `head` says dir, else 404.
- `write(path, body)`: content type persisted in `httpMetadata.contentType`.
  A body with known length streams straight to `bucket.put`. A body with
  **unknown length** is buffered up to `materialized_body_bytes` (32 MiB) and
  put; larger unknown-length uploads use `createMultipartUpload` with 8 MiB
  parts. Returns `created` = `head(key)` was null before the put (inside the
  per-key mutex).
- `write_cond`: implemented (P2 outcome) as head-then-put **inside the
  per-key mutex** for both `IfMatch(list)` (`W/` stripped, comma list) and
  `IfNoneMatchStar` — the DO serializes, so this is atomic per tenant and
  sidesteps R2 `onlyIf` semantics questions entirely. A failed precondition
  is a 412 with the exact Rust detail strings (`If-Match does not match the
  current ETag — re-read and retry`, `If-Match given but the resource does
  not exist`, `If-None-Match: * given but the resource already exists`).
  `conditional_write_atomic() = true`.
- `delete(path)`: 404 if absent (head first, in the mutex); `delete_cond`
  follows RFC 9110 order: a missing resource is the 404, not a 412.
- `rename(from, to)`: copy via `get` + `put` streaming, then `delete`;
  `to` resolving to a directory prefix → 409 `destination is a directory`;
  missing source → 404 `source does not exist`; source dir → 400. Returns
  created-vs-overwrote of the destination.
- `delete_dir(path)`: if any object exists under the prefix → 409
  `directory is not empty`; else **204** (Rust would 404 for a never-existing
  directory; §A).
- `delete_dir_all(path)`: empty/all-empty segments → 400 `refusing to
  recursively delete the store root`; page `list` in 1000s and `delete` in
  batches of 1000 keys.
- `list(path, take, skip)`: `bucket.list({prefix: key + "/", delimiter: "/",
  include: ["httpMetadata"]})` looped over `cursor` until exhausted (R2 pages
  at 1000). Entries: objects → `{name, size, lastModified: uploaded.toISOString()
  (RFC 3339), dir: false, contentType}`; `delimitedPrefixes` → `{name: "<leaf>/",
  size: 0, dir: true}` (no `contentType`, no `lastModified`). Skip any name
  containing `.rs2tmp-` (parity). **Sort by decorated `name`**, then apply
  `skip`/`take`; `total` = full count. Tenant root (`path` trims to empty) with
  nothing under it → `([], 0)`; any other prefix with nothing → 404
  `directory does not exist`.

Media types: port `message/media_type.rs` verbatim, including the ordered
`EXTENSION_TABLE` (order is the friendly-URL preference) and `for_path` with
no sniffing.

### C.3 DO SQLite schema (`TenantObject`)

All tables are created idempotently in the constructor via
`ctx.blockConcurrencyWhile`. `ns` is the store namespace: `""` for the node
default, the `store.root` for an explicit `builtin:file` mount, `mem` for
`builtin:mem`.

```sql
CREATE TABLE IF NOT EXISTS records (
  ns TEXT NOT NULL, dataset TEXT NOT NULL, key TEXT NOT NULL,
  value TEXT NOT NULL,                      -- canonical JSON (JSON.stringify)
  etag TEXT NOT NULL,                       -- 16 hex: sha256(value)[0..8]
  PRIMARY KEY (ns, dataset, key));
CREATE TABLE IF NOT EXISTS datasets (       -- existence without records (schema-only)
  ns TEXT NOT NULL, dataset TEXT NOT NULL, schema TEXT,
  PRIMARY KEY (ns, dataset));

CREATE TABLE IF NOT EXISTS idempotency (
  scope TEXT NOT NULL, key TEXT NOT NULL,
  payload_hash TEXT,                        -- NULL when the request streamed
  state INTEGER NOT NULL,                   -- 0 in-flight, 1 done
  status INTEGER, headers TEXT,             -- JSON [[name,value],…]
  body BLOB, media_type TEXT,
  completed_at INTEGER,                     -- ms epoch; replay window 24h
  started_at INTEGER,                       -- ms epoch; in-flight lifetime 5 min
  PRIMARY KEY (scope, key));
CREATE INDEX IF NOT EXISTS idem_done ON idempotency(completed_at);

CREATE TABLE IF NOT EXISTS logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  time_unix_nano TEXT NOT NULL,             -- decimal string, as in OTLP JSON
  severity INTEGER NOT NULL,                -- 5/9/13/17
  body TEXT NOT NULL, trace_id TEXT NOT NULL, span_id TEXT NOT NULL,
  mount TEXT, url_path TEXT,                -- denormalized for the `service` filter
  attributes TEXT NOT NULL);                -- JSON object
CREATE INDEX IF NOT EXISTS logs_trace ON logs(trace_id);
CREATE INDEX IF NOT EXISTS logs_time ON logs(time_unix_nano);

CREATE TABLE IF NOT EXISTS schedule_claims (
  key TEXT NOT NULL, occurrence_ms INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
  PRIMARY KEY (key, occurrence_ms));
```

KV keys (`ctx.storage.get/put`): `config` (JSON), `config.version`,
`infras.version` (the registry version seen at last build), `state:<service>:<key>`
(guest `ctx.state`, bytes — replaces `GrantedHost.state`, now durable).

**Idempotency `begin` is one `transactionSync`**: sweep expired `done` rows
(`completed_at < now - 24h`) opportunistically every 4096 begins; select the
row; absent → insert in-flight, `Fresh`; present with both hashes non-null and
different → `PayloadMismatch`; in-flight → `InFlight`; done → `Replay`.
`complete` updates the row (body ≤ 1 MiB; otherwise `abandon` deletes it);
`abandon` deletes. Row cap 100 000 with oldest-done eviction (an eighth of the
cap per sweep), in-flight never evicted — `MemIdempotencyStore` semantics —
except that an in-flight row whose `started_at` is older than the in-flight
lifetime (5 min, far beyond the 30 s request wall clock) is **abandoned**
(deleted, the begin is `Fresh`): a DO reset mid-request would otherwise leave
the slot answering 409 forever, where Rust's in-memory store forgets on
restart. `migrateIdempotencySchema` adds the column to pre-existing tables;
rows without a start time count as abandoned.

**Logs**: `emit` inserts synchronously (SQLite writes in a DO are fast and
never block on I/O); after every 256 inserts delete rows below
`MAX(id) - 50_000`. `query` = `SELECT … ORDER BY id DESC LIMIT take` with the
`LogQuery` filters as `WHERE` clauses (`service` = `mount = ? OR url_path = ?
OR url_path LIKE ? || '/%'`; `contains` = `instr(body, ?) > 0`, case-sensitive).
`is_queryable() = true`; `enabled() = true` unless the tenant config sets
`"logging": {"sink": "none"}` (a Worker-only tenant-level knob; default level
`info`, `RS2_LOG_LEVEL` var overrides).

**Data records**: `put` returns `created` from `changes()` on `INSERT … ON
CONFLICT DO UPDATE` with a prior `SELECT 1`; `list_keys` = `ORDER BY key LIMIT
take OFFSET skip` + `COUNT(*)`; `list_datasets` = union of `datasets` and
`DISTINCT dataset FROM records`, ordered; `scan_matching` walks `SELECT key,
value ORDER BY key` in 500-row pages; `list_records` with sort keys loads the
dataset and applies `listing::sort_page_project` in JS (cross-type rank and
BigInt-precise integer compare, §D), `listing_pushdown() = true`.

---

## D. Module-by-module port plan

Target root `rs2-worker/src/`. "Port" = translate the Rust file one-to-one,
keeping function names in camelCase; "adapter" = reimplemented over a
Cloudflare primitive; "dropped" = no counterpart. Each row names the traps a
porter who has only read that Rust file needs to know. The detailed behaviors
(status codes, header names, JSON shapes) are in the Rust; this table says
what is *easy to lose*.

| `rs2-core/src/…` | `rs2-worker/src/…` | Mode | Traps and decisions |
|---|---|---|---|
| `lib.rs` | `runtime/index.ts` | port | re-exports only |
| `error.rs` | `runtime/error.ts` | port | `codes` are the contract. `RsError` carries `status, code, title, detail, retryable, retryAfterMs?, extra?`; `toProblemJson(tenant, traceId)` flattens `extra` at top level after `retryAfterMs`; `type` is `https://rs2.dev/errors#<code>`. Keep the 405s that carry `code: bad_request` (`file`, `log`, discovery). `From<io::Error>` has no analogue — R2 errors map explicitly. |
| `message/message.rs` | `runtime/message.ts` | port | `MsgUrl.queryParam` percent-decodes **both key and value** and maps `+`→space (`%24take` must match `$take`). `applyMount` makes an empty remainder `/`. `isDirectory` = service path ends with `/`. `Source` = `external|internal|system`. `Principal.extra` is a map. |
| `message/body.rs` | `runtime/body.ts` | port | `Body = {stream|bytes, mediaType, size?, lastModified?, provenance}`; `materialize(cap)` rejects early on declared size and while reading; `asJson` strips a UTF-8 BOM and requires a JSON media type. Use `ReadableStream` + `Uint8Array`. |
| `message/media_type.rs` | `runtime/media-type.ts` | port | Both read generated tables (`media_type_table.rs` / `media-type-table.ts`, ~1200 extensions from mime-db) emitted for **both hosts** by `rs2-worker/scripts/gen-media-types.mjs` — never hand-edit either, and commit them together. Sorted, so lookup is a binary search. Three tables: extension→essence, essence→canonical extension (every entry round-trips), and the short friendly-URL probe order (`NEGOTIABLE_EXTENSIONS`, ~24 — bulk media is served by name but never negotiated). `schema=` param round-trips in `Content-Type` (`application/json; schema="…"`); `essence` lowercased. |
| `router/mod.rs` | `runtime/router.ts` | port | `Tenancy.resolve` in the Worker (`index.ts`); `validatePath` and `MountTable` in the DO. Mount base normalization: `"/"` → `""`; duplicate → 400 `duplicate mount path '/'`. Longest prefix on segment boundaries. |
| `path_pattern.rs` | `runtime/path-pattern.ts` | port | Grammar verbatim (§ of the pipeline spec): `${url.path[a:b]}`, `?` elision pops a trailing `/`, `||` split at the **first** `||`, data-plane values: string raw, other JSON `JSON.stringify`, `null` → nothing. `validate` at spec-write time. |
| `wrapper/mod.rs` | `runtime/wrapper.ts` | port | `LimitTable` defaults except `materializedBodyBytes = 32 MiB`. `CachePolicy.apply` exact strings (`public, max-age=N, immutable`, `private, no-cache`); `appendHeaderValue` dedupes case-insensitively. CORS: `EXPOSED_HEADERS` string verbatim; preflight echoes `Access-Control-Request-Headers` or the default list; `Access-Control-Max-Age: 86400`. `origin_matches` forms; same-origin compares the `Host` header. `check_access` fail-closed; `action_for`; role-spec tokens with `/`-pattern scoping and `{email}`/`{claim}` substitution (unresolved stays verbatim). Breaker: threshold 8 / window 10 s / cooldown 5 s, `retryAfterMs` = remaining. Limiter: fail fast. |
| `runtime.rs` | `tenant-object.ts` (+ `runtime/dispatch.ts`) | port | §B.3 order. `RuntimeRequester` = recursive `handle`. `scheduler_loop` → alarms (§B.6). `purge_tenant` = drop the in-memory build. The boundary log body is `"<METHOD> <path> -> <status>"`. |
| `tenant.rs` | `runtime/tenant-build.ts` | port | The service-name `match` is the registry of services; unknown → 400 with the exact wording. `check_elevate_not_operator`, `KNOWN_PATTERNS`, `resolve_secrets` (default-deny), `resolve_outbound_injectors` (`inject: "infra:x"` or inline with `secret:name` leaves, host-side only), `expand_store` (ignored unless `mount.service == kind`), `require_store_root` for `builtin:file`/`builtin:local`. `code:` store adapters and `message` `code:` adapters → 501 `engine_unavailable` with the no-JS wording (P3/P4). `ServiceContext` fields are the grants: `log_store` only for `log`; `catalogue`/`builtin_adapters`/`infras` only for `services`; `messaging` only for `message`. A `message` mount takes either `store.adapter` or a per-channel `store.adapters` map (both ⇒ 400); a routed adapter that does not serve its channel is a build-time 400. |
| `infra.rs` | `runtime/infra.ts` | port | `expand_infra` merge order (tenant minus `adapter` → reject `infraOnly` trespass → overlay infra config, infra wins → set `adapter` → check `requires`); 403 for `allowedTenants`. Source is the registry snapshot. |
| `config_schema.rs` | `runtime/config-schema.ts` | port | The catalogue document (`{baseSchema, tenantSchema, services:[{name, description, configSchema}]}`) is JSON Schema **draft-07** derived from Rust types; the port ships the **same JSON as a checked-in fixture** generated by the Rust side (`cargo run -p rs2-cli -- catalogue-dump` — add in P1) so the two hosts cannot drift; `MountSpec` is strict (unknown keys 400), `AccessSpec` strict (`readRoles` 400). |
| `capabilities/mod.rs` | `capabilities/types.ts`, `capabilities/prefixed.ts`, `capabilities/scoped.ts` | port | Trait shapes; `WritePrecondition`, `WriteOutcome`, `if_match_hits` (`W/` stripped, `*`), `DirEntry` serialization (`lastModified`/`contentType` omitted when absent), `list_records_fallback`, `sanitized_store_root` (rejects leading `/`,`\`, drive letter, `..`). |
| `adapters/local_fs.rs` | `capabilities/r2-file-store.ts` | adapter | §C.2. |
| `adapters/file_data.rs`, `adapters/mem_data.rs` | `capabilities/sqlite-data-store.ts` | adapter | §C.3. Error strings: `no record '<k>' in dataset '<d>'`, `no dataset '<d>'`; `list_datasets` for an unknown namespace is `([],0)` not 404; `get_schema` absent → `null`. |
| `adapters/mem_query.rs` | `capabilities/reference-query-store.ts` | port | Ops `==`,`!=`,`<`,`>`,`<=`,`>=` (number/number as float, string/string lexicographic by UTF-16 code unit — **use a byte-wise comparator** to match Rust's UTF-8 `Ord`), `contains` (substring or array deep-equality); unknown op → 400; `_key` injected if absent; `orderBy` stable sort, mixed types Equal; `total` before paging; `quote`: string raw, number JSON, bool, else 400 `cannot splice a <kind> into a string query position`; string query → 501. Registered as `builtin:reference`. |
| `adapters/registry.rs` | `capabilities/builtin-registry.ts` | port | names: data `mem`, `file`; files `local`; query `reference`. Sorted name lists in error messages and `/catalogue/available`. |
| `adapters/http_out.rs` | `capabilities/fetch-http-out.ts` | adapter | `fetch` with `AbortSignal.timeout(30_000)`; non-2xx is a response, not an error; transport failure → `RsError{502, "internal", "Upstream Error", detail, retryable: true}`; empty upstream body → no `Body`; forward all headers; response media type from `Content-Type`. Request/response caps 32 MiB. |
| `adapters/credential.rs` | `capabilities/credential.ts` | port | Strategies `bearer|header|basic|query|hmac|awsSigV4`, exact 400 wordings. SigV4: signed headers `host;x-amz-date` only; `recanon` (decode then re-encode); non-`s3` services double-encode the path; sorted query pairs; golden vector `get-vanilla` → `5fa00fa3…fbf31` must pass in unit tests. `basic` uses padded base64. |
| `adapters/file_log.rs` | `capabilities/sqlite-log-store.ts` | adapter | §C.3. |
| `logging/mod.rs` | `runtime/logging.ts` | port | OTLP-ish flat record: `timeUnixNano` **string**, `severityNumber` 5/9/13/17, `severityText`, `body`, `traceId`, `spanId`, `attributes` object with `rs2.tenant` first. `Severity.parse` accepts `warning`. `LogQuery.matches` semantics reproduced in SQL. `ServiceLogger` stamps `rs2.mount`, `rs2.service`, `rs2.source: "service"`; guest logs `rs2.source: "custom"`. `now_unix_nano` = `BigInt(Date.now()) * 1_000_000n` (ms precision; monotonic tiebreak by `id`). |
| `idempotency.rs` | `runtime/idempotency.ts` + `capabilities/sqlite-idempotency.ts` | port + adapter | `scope_for`, `payload_hash` (SHA-256 hex, `"empty"`, `null` for streams), `segment_key = sha256(invocationId ‖ 0x00 ‖ u64le(segmentIndex))` — note the **little-endian 8-byte** index; `capture_response` materializes ≤ 1 MiB else abandons; `StoredResponse.into_message` re-adds every stored header then `idempotency-replayed: true`. |
| `scheduler.rs` | `runtime/scheduler.ts` | port | `parse_every` (`ms|s|m|h`, rejects 0), 5-field cron with DOM/DOW OR rule, `next_occurrence_after` minute-stepping with a 366-day bound, `interval_bucket_ms`; `tick_message`. Claims in `schedule_claims` with TTL. |
| `outbound.rs` | `runtime/outbound.ts` | port | `url_host` (userinfo/port stripped), `host_matches` (`*.suffix` matches apex; no bare `*`), grants sorted by name, `authorize` errors (400/403 with the allowed list/501), header defaults `accept: */*`, `user-agent: rs2/<version>` (same version string as the Rust crate — `RS2_VERSION` constant shared by both via the tag), `content-type` from the body. |
| `retry.rs` | `runtime/retry.ts` | port | Defaults; `resolve` is whole-object precedence; `no_retry()` when unconfigured; `delay` with full jitter; `parse_retry_after` seconds or HTTP date; `permits_retry(keyed needs key; unsafe never)`. |
| `crypto.rs` | `runtime/crypto.ts` | port | WebCrypto HMAC (`sha256|sha512` only), constant-time compare via `crypto.subtle.timingSafeEqual`, lowercase hex. |
| `listing.rs` | `runtime/listing.ts` | port | `FieldPath` (empty segment → 400), `ListSpec.parse`, comparator: rank missing < null < false < true < number < string < array < object; strings by **UTF-8 bytes** (encode and compare `Uint8Array`s, not `<` on JS strings); integers beyond 2^53 via `BigInt` when both sides are integer literals (parse the JSON text with a reviver that keeps big integers as `BigInt`, or compare the raw digit strings); arrays/objects by compact JSON text; `project` collision rule; `sort_page_project` key tiebreak; `MetaSort` `@name|@size|@lastModified|@contentType|@dir` with name tiebreak and the exact 400 wording. |
| `discovery.rs` | `runtime/discovery.ts` | port | Three docs, visibility filter (`check_role_spec(access,"read")` with a probe; **no `access` is visible**), `?surface=` and `x-expose`, `pattern_of` table + `conditional-write` for `store*`, `specSubtree`/`authoring`, `control` block (null when the `services` mount is filtered out), agent-surface shapes (actions from stored pipeline specs, cap 100 entries / 1 MiB reads; `code:` manifests), OpenAPI 3.1 skeleton with `components.pathItems` (`StoreContainer`, `StoreChild`, `SpecChild`), dataset schema inlining (`Dataset<base>_<dataset>` sanitized), `describe_mount` + `allowed_methods` for `OPTIONS`. Add the `limits` object (§A) on both hosts. |
| `contract/mod.rs` | `engines/host-api.ts` | port | `GrantedHost`: default deny, outbound budget counted **before** dispatch, child trace + depth, `LogContext` stamping; `state` now backed by DO KV (`state:<service>:<key>`). `InvocationLimits`. |
| `engines/mod.rs`, `engines/native.rs` | — | dropped | — |
| `engines/wasm.rs` | — | dropped | 501 at load (`… is a wasm component but this build has no wasm engine`). |
| `engines/js.rs` + `engines/js_prelude.js` | `engines/dynamic-worker.ts` + `engines/guest-shim.js` | adapter | §E. |
| `engines/resident.rs` | `capabilities/guest-stores.ts` | port | `ResidentAdapter` (ref parsing + bundle load + `features` handshake + the call helper) wrapped by `GuestDataStore`/`GuestQueryStore`/`GuestFileStore`/`GuestSmsGateway`. Traps: the error wordings are byte-for-byte Rust's (`"data adapter bundle 'code:<n>@<v>' not found — deploy it via PUT /code/<n>"`, `store_error`'s status mapping incl. the literal `data adapter returned <status>` fallback for every kind, the 400 ref-parse wordings); `$select`/`$sort` are **never** forwarded to a bundle that hasn't advertised `list-records` (the flag reads `false` until the first call's `features` RPC — same as Rust's lazy spawn); file contents cross base64-encoded, Ranges slice host-side, conditional writes are the interface defaults (best-effort, `conditionalWriteAtomic() = false`); `quote` is the scalar default. The engine side is `invokeAdapter`/`adapterFeatures` in `engines/dynamic-worker.ts` (deny-all `GrantedHost`, an invocation record per call for socket/log attribution, guest `ctx.state` durable in DO KV under the `<name>@<version>` identity). |
| `services/mod.rs` | `services/context.ts` | port | `ServiceContext`; `if_none_match_hits`; `write_precondition` (`If-None-Match: *` wins over `If-Match`); `pagination` (`$take` default 1000 max 10 000, `$skip` 0, unparseable → default). |
| `services/file.rs` | `services/file.ts` | port | `serve_file`: 200/206, `Accept-Ranges: bytes`, `ETag` from provenance, `Last-Modified` in **RFC 2822** (`toUTCString()` is IMF-fixdate; the Rust emits RFC 2822 — they differ only in the day/month punctuation; **match Rust**: use the `time` crate's RFC 2822 layout `Ddd, DD Mon YYYY HH:MM:SS +0000`), 304 on `If-None-Match` (which beats a `Range`). Ranges: a single `bytes=` range only, inclusive end, resolved against a `head` **before** the read so `Content-Range: bytes <first>-<last>/<total>` on the 206 and `bytes */<total>` on the 416 both name the size; `bytes=-n` suffix supported; a first-byte-pos at/past the end or a zero-length suffix is **416** (problem+json, `Accept-Ranges` still set); another unit, a multi-range set, or an invalid spec (`bytes=500-400`) is **ignored** → 200 with no `Content-Range`; `Range` is read on GET only. `If-Range` gates the partial read on a **strong** validator match (a `W/` tag never matches; a non-tag value compares against `Last-Modified`); a mismatch re-reads and serves the whole representation as 200. `MOVE` (`Destination` header, 201/200, `Location`). Directory GET decision order: forced listing (`Accept: application/vnd.rs2.dir+json` exactly, q>0) → `defaultResource` (+ SPA root fallback) → `listings:false` 404 unless operator → `$sort` meta-sort over the whole dir → `{path, entries, total}` + `X-Total-Count` + `Vary: accept` (+ `Cache-Control: no-store`, `Vary: authorization, cookie` when operator-only). File GET: serve → dir-without-slash **301** with query preserved → friendly URL (`Content-Location`) → SPA fallback. HEAD: `Content-Length`, `Accept-Ranges`, `Content-Type` by path, `Content-Location` only when resolved differs, **no ETag**. Keyless POST child = 32-hex uuid + `extension_for` (`canonical_extension`/`canonicalExtension` over the generated reverse table, so the named file serves back as what was posted; unknown type → no extension). PUT/POST child: pinned extension-less writes (`extensionPriority[0]`, 400 without), 201/200, `ETag`, `Location` + `Content-Location` on pins, no body. DELETE dir: preconditions → 400; `?confirm=<leaf>` recursive; file: `delete_cond` then friendly resolution. Other → 405 `code: bad_request`. |
| `services/data.rs` | `services/data.ts` | port | ETag = `"<sha256(JSON.stringify(value))[0..16]>"` computed over the **unredacted** value (opaque; differs from Rust's SipHash). Root GET `{path:"/", entries:[{name:"<ds>/", dir:true}], total}`; `.schemas` GET; dataset GET plain (appends `{name:".schema.json", dir:false, fixed:true}` **outside** the page/total when a schema exists) vs `$select` projected (`fields`, no schema entry; `$sort` without `$select` → 400); POST keyless 201 + body + `Location` + `ETag` + `Content-Type: application/json; schema="…"`; dataset DELETE requires `?confirm=<dataset>` else **409** (unconditional); record GET 200 with `Link: <…/.schema.json>; rel="describedby"`, 304 on `If-None-Match` with `ETag`; PUT (no body) / POST (echo) 201/200 with `ETag`, preconditions evaluated in the service against the content ETag; PATCH = RFC 7386 merge, **ignores conditionals, no ETag on response**, 200 with body; schema PUT 200 (never 201), compile-checked, operator-gated under `fieldLevelAuthz`; field rules top-level `x-rs-read`/`x-rs-write` (`redact_fields`, `enforce_write_rules` 403 wording); 422 `errors: [{path, message}]` from ajv (`instancePath` → `path`, ajv `message`). Ajv: `new Ajv2020({allErrors:true, strict:false})`, and a draft-07 instance when `$schema` matches `draft-07`; `jsonschema` 0.33 auto-detects the same way. |
| `services/spec_store.rs` | `services/spec-store.ts` | port | Owns a `FileService` with default `SiteOptions` over `files.prefixed(root)`; `store_root` precedence (`specStore.root` → `store.root` → `<prefix><base>`); `is_authoring` = first segment equals the subtree; authoring write: JSON body required, `access`-change operator gate (set/change/**remove**), validator output replaces the body; spec reads forced to `application/json`; empty listing synthesized (`{path, entries:[], total:0}`, `X-Total-Count: 0`); cache cap 1024 cleared wholesale; `resolve` longest prefix then `.root` (split 0). |
| `services/pipeline_service.rs` | `services/pipeline-service.ts` | port | `config.pipeline` → 400 wording; envelope validation (`pipeline` required, `retry`, `access` shape with exact 400s); canonical storage (typed spec replaces DSL, other keys pass through); `?$plan` document; execution: longest-prefix resolve, peeled plane + `rebuild_rest`, per-spec access merge (object∘object per-key, spec wins; none → 401/403 fail closed; `System` allowed), retry chain envelope → mount → tenant → none, `?$to-step`. |
| `pipeline/spec.rs` | `pipeline/spec.ts` | port | Field names/enums verbatim; `validate` returns **all** errors with `/steps[i]` paths → 422 `validation_failed` `pipeline spec failed validation`. `effect_class` inference by method. |
| `pipeline/dsl.rs` | `pipeline/dsl.ts` | port | Array-only; mode token only at index 0; `elevate `/`try ` prefixes; `if (…)` with quote-aware paren matching; ` :name` / ` :$capture` suffix (last occurrence, no spaces); METHOD must be uppercase; `unzip|zip|multipart` → 400. |
| `pipeline/condition.rs` | `pipeline/condition.ts` | port | Grammar and builtins verbatim; `status` defaults 200; truthiness rules (objects always true); comparisons non-associative; mixed-type ordering false. |
| `pipeline/transform.rs` | `pipeline/transform.ts` | adapter | `jsonata` npm: `jsonata(expr).evaluate(input, bindings)` with bindings `_status, _ok, _headers, _rawBody (only if the template text contains `_rawBody`), _user & principal, _url, <secrets>, <captures without $>`; register `hmac`, `hmacVerify`, `hashPassword`, `verifyPassword` via `expr.registerFunction` (async-capable — jsonata JS supports promise-returning functions, needed for hash-wasm/WebCrypto). Timeout 5 000 ms and depth 100 via `evaluate(...)` options `{timeout}`; string leaves parse-checked at write time (`invalid JSONata expression '<e>'`). `undefined` → `null`. |
| `pipeline/response.rs` | `pipeline/response.ts` | port | `$response` only when the object has exactly one key; field checks with exact 400 wordings; string body → `text/plain`; status default 200; headers replace. |
| `pipeline/segments.rs` | `pipeline/segments.ts` | port | Boundaries before `transform`/`split`; warnings text verbatim. |
| `pipeline/executor.rs` | `pipeline/executor.ts` | port | The big one. Keep: flow algebra (`Continue|Exit|Abort`), `fail_action`/`succeed_action` defaults, interpolation precedence query < body fields < variables, captures stored without `$`, auto keys `sha256(invocationId ‖ 0x00 ‖ keyPath)` hex, target selection **before** the retry loop, header interpolation once, principal only on internal targets, `elevate` from mount config only, `try`/`as` error shapes `{_errorStatus, _errorMessage}`, parallel via bounded concurrency (`p-limit`-style, default 12), conditional first-match → `Exit`, `tee` = `ctx.waitUntil` (detached, errors swallowed) / `teeWait` propagates errors, `jsonSplit` consumes the rest and re-sorts numerically, `jsonObject` join, segment snapshots (≤ 1 MiB streams force-materialized) and retries with cloned vars, limits names (`pipeline_steps`, `pipeline_depth`, `pipeline_fanout`, `pipeline_fanout_bytes`, `pipeline_wall_clock_ms`), the `pipeline: {failedStep, steps}` problem block. |
| `services/query.rs`, `services/query_template.rs` | `services/query.ts`, `services/query-template.ts` | port | Envelope parse (language inference, `params`/`output` compile), param merge positional < query-string (typed by `params.properties.<k>.type`; `$`-keys skipped) < body, defaults then ajv validation with **`errors: [{path, error}]`** (singular `error`), `substitute_json` rules (`$if`, key placeholders must be strings, elision, `$N` positional in spliced strings), SQL passthrough, 200 + `X-Total-Count`. |
| `services/template.rs` | `services/template.ts` | adapter | Envelope `{source, contentType}`; the compiled bundle runs as a Dynamic Worker (`LOADER.get("tpl:" + sha256(source)[0..16], …)`, `globalOutbound: null`, `env: {}`, `limits: {cpuMs: 1000}`), invoked with the resident envelope `{method:"POST", url:"/", body: props, mediaType:"application/json"}`; non-string body → 502; guest headers discarded. |
| `services/auth.rs` | `services/auth.ts` | port | Settings merge (tenant `auth` + mount config minus `access`; empty `jwtSecret` → 400). JWT **HS512**, header bytes literally `{"alg":"HS512","typ":"JWT"}`, base64url no padding, claims `sub, roles (space string), kind, iat, exp, extra (omitted when empty)`, verify error wordings and order, no skew. Token from `Authorization: Bearer ` (case-sensitive prefix) else cookie `rs-auth`. Endpoints `POST login|refresh|logout`, `GET user`, else 404 wording. Cookie matrix (`SameSite=Strict` / `SameSite=None; Secure` for trusted origins / none otherwise), `logout` 204 with `Max-Age=0`. Lockout: 5 attempts / 10 min, map bound 10 000, DO memory. Login-origin allowlist. **Hashes** (`hash-wasm`): produce `argon2id({password, salt: 16 random bytes, parallelism: 1, iterations: 2, memorySize: 19456, hashLength: 32, outputType: "encoded"})` → `$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`; verify with `argon2Verify({password, hash})` for `$argon2*` (parameters come from the hash — Rust `Argon2::default()` verifies any PHC params), `bcryptVerify` for `$2*`, else false. Cross-host test: a hash minted by each host verifies on the other (P3 acceptance). |
| `services/proxy.rs` | `services/proxy.ts` | port | `target` required; strip `host, authorization, cookie, connection, transfer-encoding, proxy-authorization`; injector under key `proxy`; no allowlist. |
| `services/message.rs` | `services/message.ts` | port | Routes and wordings: `POST /send {channel, to, …}` → 201 `{id?, channel, provider}`, `GET /status/{id}` → provider JSON or **501 `provider_unavailable`** when `deliveryStatus()` is false, `GET /channels` → `{channels, deliveryStatus, provider}`. An unserved channel is a 400 naming the configured ones, before the adapter is called. |
| `capabilities/message.rs` | `capabilities/message.ts` | port | `Outbound` is channel-tagged (a Rust enum, a TS discriminated union) so email-only fields cannot ride on an SMS; `Receipt.id` is optional because Cloudflare's REST send mints none. The parse 400s are byte-for-byte shared — the conformance suite reads them from both hosts. `RoutingGateway` composes per-channel adapters; `MAX_RECIPIENTS` is 50. |
| `adapters/cf_email.rs` | `capabilities/cf-email.ts` | port | `builtin:cf-email`, Cloudflare Email Service **REST API** (`POST /accounts/{id}/email/sending/send`), not the Workers `send_email` binding — the binding fixes its senders in `wrangler.jsonc` at deploy time and does not exist on the Rust host. Reports delivery in the send response, so `deliveryStatus()` is false and no id is minted. |
| `adapters/aws_sns.rs` | `capabilities/aws-sns.ts` | port | `builtin:aws-sns`, Query API `Action=Publish` signed with the existing `awsSigV4` strategy. Mints an id but has no per-message status API, so `deliveryStatus()` is false for the opposite reason. XML scanned for `<MessageId>` / `<Code>` / `<Message>`, not parsed. |
| `services/wrapper_service.rs` | `services/wrapper-service.ts` | port | Inline spec, `inputSchema` enforced on PUT/POST/PATCH (422 `errors: [{path, message}]`), `outputSchema` compile-only, `rest` byte-exact. |
| `services/services_config.rs` | `services/services-config.ts` | port | Every endpoint in the Rust table: `catalogue`, `catalogues`, `catalogue/available`, `catalogue/install` (hash pin, compile check via the Dynamic Worker loader's `get` with a throwaway id → `validated`), `services`, `infras`, `raw` GET/PUT (`<secret>` masking/restore, **409** on `If-Match`), the `/code/` subtree (content-addressed `version_of` = sha256[0..8] hex; POST keyless 201 `{name, version, ref, validated}` + `Location`; PUT must match the hash else 409; `mountedAt`; DELETE refuses mounted versions; `X-RS2-Manifest` sidecar; `Cache-Control: private, max-age=31536000, immutable` + `ETag: "<version>"` on reads). |
| `services/catalogue.rs` | `services/catalogue.ts` | port | `CatalogueItem` fields; host allowlist checked before I/O (400 no host / 403 `capability_denied` `catalogue fetch to '<host>'`); 502 `Catalogue Fetch Failed`; 64 MiB cap. |
| `services/code.rs` | `services/code.ts` | port | `from_ref` name rules; `CODE_PREFIX`, `STORE_GRANT_PREFIX`; load `.wasm` → 501, `.js` → Dynamic Worker; grants (`prefix` re-enters `handle` with the caller's principal; `httpOut` allowlist + injector; `store` = private `FileService` over `.rs2-store/<root>` with no principal, errors become status responses, `validate_path` applied); `x-rs2-base-path` stamped; `x-rs2-body-ref` resolution rules (502 wordings, header stripped, body spliced host-side). |
| `services/log_reader.rs` | `services/log-reader.ts` | port | GET only (405 `code: bad_request`); `$take` default 100; params; `since`/`until` ms-or-RFC3339 (unparseable ignored); `severity` unknown → 400; NDJSON when `Accept` contains `text/plain` and not `application/json`; `X-Total-Count` = returned count; 403 `capability_denied` `logStore`; 501 when not queryable. |
| `tls.rs` | — | dropped | — |
| `rs2-server/src/lib.rs` | `index.ts`, `registry-object.ts` | port + adapter | §B.1, §B.5. `hyper_request_to_message`/`message_to_hyper_response` become Request/Response conversions in `tenant-object.ts`. |

**Header names** the Worker must emit with these exact spellings (case is
insignificant on the wire but the runner compares case-insensitively and
tooling greps for them): `ETag`, `Location`, `Content-Location`, `Link`,
`X-Total-Count`, `X-Trace-Id`, `Idempotency-Replayed`, `Retry-After`,
`Accept-Ranges`, `Content-Range`, `Last-Modified`, `Vary`, `Cache-Control`, `Allow`,
`Set-Cookie`, `Access-Control-*`, `X-RS2-Manifest` (request), `Destination`
(request), `x-rs2-base-path` / `x-rs2-body-ref` (guest boundary),
`x-rs2-trigger` (scheduler).

---

## E. Guest contract for Dynamic Workers

### E.1 What exists today (from `js_prelude.js` + `engines/js.rs`)

A bundle is a single-file ESM module whose `export default` is a function or
`{handle}` with signature `async (msg, ctx) => envelope`, plus an optional
`export const features = ["list-records"]`. `msg` is
`{method, url (path+query, no host), headers (lowercased), body (parsed JSON
for JSON media types, else a lossy-UTF-8 string, else null), mediaType,
bodyPassthrough, requestStreaming, responseStreaming, bodySize}`; the host
stamps `x-rs2-base-path`. The envelope is `{status?, headers?, body?,
mediaType?}` (non-object → `{status:200, body}`; `null` → 204; string body →
`text/plain` unless `mediaType`). `ctx` is `{config, request(cap, req),
log(level, text), state: {get, put}, readBody(), body(), beginStream(env)}`.
Host errors surface as thrown `Error` with `.code` and `.status`. Globals the
prelude provides: `console`, `queueMicrotask`, `structuredClone`,
`setTimeout/setInterval/clear*` (virtual), `TextEncoder/TextDecoder`,
`btoa/atob`, `Buffer` (subset), `URL`, `URLSearchParams`, `AbortSignal/
AbortController` (inert), `crypto.getRandomValues/randomUUID` (no `subtle`),
`process` (`env: {}`, `version: "v22.12.0"`, `platform: "rs2"`), `global`,
`WebSocket` (throws), `Event/CustomEvent/EventTarget`, `ReadableStream`
(getReader throws), `WritableStream/TransformStream` (empty), `Blob/File`
(text-only), `FormData` (unserializable), `Headers`, `Request`, `Response`
(string bodies), `fetch` (capability `"fetch"`, synchronous under the hood),
`RS2Socket` (raw TCP/TLS over the socket allowlist).

### E.2 What the Worker provides

The guest runs as a Dynamic Worker. Cloudflare's runtime already provides
spec-correct `fetch`, `Request`, `Response`, `Headers`, `URL`,
`URLSearchParams`, `TextEncoder/Decoder`, `crypto` (with `subtle`),
`structuredClone`, `AbortController`, streams, `Blob`, `FormData`, `console`,
timers, `queueMicrotask`, `btoa/atob`, `Event*`, `WebSocket`. **The shim does
not shadow platform globals** (decision: upgrade, not fidelity — every
prelude global was a weaker subset of the platform one). The shim adds only
what the platform lacks:

- `Buffer` (the prelude's subset: `from(value, "base64"|"hex"|utf8)`,
  `alloc`, `concat`, `byteLength`, `isBuffer`, `toString(enc)`), `global`,
  `process` (`{env: {}, nextTick, version: "v22.12.0", versions, platform:
  "rs2"}`), `RS2Socket` (over `connect()` from `cloudflare:sockets`, same
  `connect(host, port, tls)`/`write`/`read(max)`/`close` surface, async).
- `fetch` is **wrapped**, not replaced: the wrapper calls the platform
  `fetch`, which `globalOutbound` routes to the host gateway (E.4). The
  gateway enforces the mount's `fetch` grant (`httpOut` hosts + injector) and
  the outbound budget, so `fetch("https://api.stripe.com/…")` behaves as
  today: `capability_denied` when there is no grant named `fetch`, the
  allowlist otherwise.

Shim modules (`engines/guest-shim.js` + `engines/guest-globals.js`, bundled
as text into the Worker). The globals module is imported **first**, so it is
evaluated before the bundle's module scope — as in the Rust prelude, a bundle
that uses `Buffer` or captures `fetch` at top level sees the installed
globals and the *wrapped* fetch:

```js
// globals.js — evaluated before the bundle
import { AsyncLocalStorage } from "node:async_hooks";   // loader flag nodejs_als
export const invocationContext = new AsyncLocalStorage();
installGlobals();                              // Buffer, global, process, RS2Socket, wrapped fetch, console

// shim.js — the main module
import { WorkerEntrypoint } from "cloudflare:workers";
import { invocationContext, rethrow } from "./globals.js";
import * as user from "./bundle.js";           // the deployed bundle, unchanged
export const features = Array.isArray(user.features) ? user.features : [];
export default class Rs2Guest extends WorkerEntrypoint {
  async invoke(msg, config, invocationId) {    // called by the host via RPC
    const h = typeof user.default === "function" ? user.default : user.default?.handle;
    if (typeof h !== "function") throw new Error("default export must be a function or { handle }");
    const out = await invocationContext.run({ rs2: this.env.RS2, invocationId },
      () => h(msg, makeCtx(this.env.RS2, config, invocationId)));
    return normalizeEnvelope(out);             // js.rs normalize_envelope semantics
  }
}
```

The surfaces with no per-call handle — the fetch wrapper (which stamps
`x-rs2-invocation`), console routing, `RS2Socket.connect` — find their
invocation through `invocationContext`, so concurrent invocations sharing an
isolate keep their own attribution (grants, principal, outbound budget) and
work that outlives its invocation carries a finished id the host no longer
knows (denied).

`makeCtx` mirrors `__rs2_dispatch`'s `ctx` exactly in shape; every member that
was a blocking op in V8 is now a Promise: `request`, `state.get/put`,
`readBody`, `body()` (async iterable), `beginStream(env).write`. `log` stays
synchronous (fire-and-forget RPC). A bundle that `await`s them works on both
hosts; one that used the return value synchronously does not — this is the
declared `guest-async` difference (§A). Host errors arrive over RPC as the
same `{__rs2_error, code, status, message}` marker and are rethrown as an
`Error` with `.code`/`.status`, so `e.code === "capability_denied"` holds.

### E.3 Service-binding protocol (guest → host)

The host passes exactly one binding, `env.RS2`, a `WorkerEntrypoint` stub
created per invocation with `ctx.exports.HostApi({props: {invocationId}})`.
The DO keeps an `invocations` map from id → `{grants, budget, logCtx,
principal, trace, depth, bodyStream, streamSink}` for the call's lifetime, so
the stub carries no authority except the id (props are invisible to the
guest, unforgeable). Methods:

| `env.RS2.…` | Host behavior (`GrantedHost` / `js.rs` op) |
|---|---|
| `request(capability, {method?, url, headers?, body?, mediaType?})` → `{status, headers, body, mediaType}` | `op_rs2_request`: grant lookup (default deny), budget debit, child trace/depth, `message_from_request` rules (`prefix` rewrites relative URLs; `httpOut` needs an absolute URL), response body parsed when JSON else string |
| `fetchOut(serialized request)` → serialized response | used by the gateway (E.4), grant name fixed to `"fetch"`; body always a string, `content-type` synthesized when absent |
| `log(level, text)` | stamps `rs2.mount`, `rs2.service`, `rs2.source: "custom"`, trace/span |
| `stateGet(key)` → `string|null`, `statePut(key, value)` | DO KV `state:<service>:<key>` (durable — an intentional upgrade over Rust's in-memory map) |
| `random(n)` → `Uint8Array` | not needed (platform `crypto`), kept for parity, clamps at 65 536 |
| `bodyRead()` → `Uint8Array|null` | `op_rs2_body_read` when `requestStreaming`; cumulative cap |
| `streamBegin(envelope)`, `bodyWrite(bytes)` | `responseStreaming`: the host resolves the client response on `streamBegin` with a `TransformStream` and pumps writes into it; twice → 502 `beginStream called more than once`; write before begin → 502 |
| `socketConnect(host, port, tls)` | **not RPC** — sockets stay in the guest via `cloudflare:sockets`; the gateway's `connect()` hook enforces the `{"type":"socket","hosts":[…]}` allowlist (`socket_allowed` patterns: `host:port`, host-only, `*`, `*.suffix[:port]`) and denies with `capability 'socket <host>:<port>' is not granted to this service` |

Host → guest: `const worker = env.LOADER.get(id, loadCode)` where `id =
"<tenant>:<mount base>:<name>@<version>"` (content-addressed — a redeploy is
a new id — and mount-addressed, so two mounts of one bundle with different
grants never share an isolate's module state) and `loadCode` returns
`{compatibilityDate: "2026-08-22", compatibilityFlags: ["nodejs_als"],
mainModule: "shim.js", modules: {"shim.js": SHIM, "globals.js": GLOBALS,
"bundle.js": {js: bundleText}}, env: {RS2: stub}, globalOutbound: gateway,
limits: {cpuMs, subRequests}, tails: []}` (`engines/dynamic-worker.ts
guestCodeBase`). Then
`worker.getEntrypoint("Rs2Guest").invoke(msg, config)` with a `Promise.race`
against the wall clock (30 s) as a backstop; a CPU-limit exception from the
loader maps to `limit_exceeded("wall_clock_ms")` (the closest contractual
name; `observed`/`cap` are the CPU budget in ms) and feeds the breaker. OOM
(the platform's 128 MiB) surfaces as an exception → `limit_exceeded("memory_bytes")`.
`compile_check` at deploy = `LOADER.get("check:" + hash, …)` +
`getEntrypoint()` (module evaluation errors → 502 `contract_violation`),
which also verifies the default export shape; `validated: true`.

Limits mapping (`InvocationLimits` → loader): `cpuMs` = the mount's
`limits.cpuMs` (Worker-only mount config, default 5 000, ceiling 30 000),
`subRequests` = `outbound_calls` (64). `memory_bytes` is the platform's 128 MiB
and is reported, not configurable.

### E.4 Egress gateway

`export class Egress extends WorkerEntrypoint` in the Worker, instantiated
per invocation as `ctx.exports.Egress({props: {invocationId}})` and passed as
`globalOutbound`. Its `fetch(request)` looks up the invocation in the DO
(via an RPC back to the tenant stub — the gateway runs in the Worker isolate,
not the DO; it forwards `{invocationId, request}` to `stub.guestFetch`), and
the DO applies `GrantedHost.request("fetch", …)`. Sockets go through the
`EgressSockets` subclass — the gateway dynamic workers actually get as
`globalOutbound` — whose `connect(socket)` hook sees only what was dialed,
in `socket.opened.localAddress` (raw TCP has no header channel). The bridge
is therefore a **nonce**: an allowed `socketCheck` mints a single-use
approval in the DO and returns `<nonce>.rs2-socket.invalid` as the name the
shim dials; the hook redeems the nonce for the real `host:port` (+ TLS,
which terminates host-side against the real hostname) and bridges, and
closes the socket when the nonce is missing, unknown, spent, or expired
(decision 38/39).
Guests therefore have no path to the network that bypasses grants, and
`tails: []` keeps their `console` output inside the host log bridge
(`console.*` in the guest is additionally routed to `env.RS2.log` by the shim).

### E.5 Existing bundles

`corpus/bundles/*.js` (esbuild `--platform=browser --conditions=worker`) run
unchanged: they use `fetch`, `Headers`, `URL`, `TextEncoder`, `crypto`,
`Buffer`, `process.env` — all present. `conformance/echo-guest` is a Wasm
component and does not apply; the HTTP runner's code-mount cases use a JS
echo bundle (§F). `guest-adapters/*.js` (Redis/Mongo over `RS2Socket`) are
resident adapters and wait for P4b.

---

## F. HTTP conformance runner (`conformance/http/`)

Node 22 + vitest. Black box: it only speaks HTTP to `RS2_BASE_URL`. It is
the over-the-wire successor to the Rust test files listed in `testing.md`;
those keep running in-process for Rust, this runner is the cross-host
contract.

### F.1 Parameters (env)

| Var | Meaning |
|---|---|
| `RS2_BASE_URL` | e.g. `http://127.0.0.1:3100` or `http://127.0.0.1:8787` |
| `RS2_HOST` | value sent as `Host` (and used as the CORS same-origin host); default from the URL |
| `RS2_TENANT` | tenant name expected in problem bodies; default `conf` |
| `RS2_ADMIN_EMAIL`, `RS2_ADMIN_PASSWORD` | the bootstrap admin of the fixture tenant (role `A`) |
| `RS2_ADMIN_TOKEN` | Worker only: enables `globalSetup` seeding through `/admin/tenants` |
| `RS2_HOST_KIND` | `rust` \| `cloudflare` — selects the allowed-divergence table (§A) |
| `RS2_CODE_BUNDLE` | path of the JS echo bundle (`conformance/http/fixtures/echo.js`) |

### F.2 Seeding

Both hosts start with one pre-provisioned tenant `conf` whose config is
`conformance/http/fixtures/conf.base.json`:

```json
{ "auth": { "jwtSecret": "conf-secret", "userDataset": "users" },
  "operatorRoles": "A",
  "cors": { "trustedOrigins": ["https://app.conf.test"], "allowedOrigins": ["https://reader.example"] },
  "mounts": [
    { "path": "/services", "service": "services", "config": { "access": { "read": "A", "write": "A" } } },
    { "path": "/auth",     "service": "auth",     "config": { "access": "open" } },
    { "path": "/data",     "service": "data",     "config": { "access": "open" } } ] }
```

- **Rust**: `conformance/http/fixtures/rust/serverConfig.json` (`tenancy:
  single conf`, `bootstrapAdmin`, `catalogueHosts: []`, temp `fileRoot`/`dataRoot`)
  + `tenants/conf.json`; `npm run host:rust` copies them to a temp dir and
  runs `cargo run -p rs2-server --features js -- <config>`.
- **Worker**: `npm run host:cf` runs `wrangler dev` with
  `RS2_DEFAULT_TENANT=conf`; vitest `globalSetup` calls `PUT /admin/tenants/conf`
  with the same JSON plus `bootstrapAdmin` (idempotent: seed-if-absent).

Every suite then reshapes the tenant through the API it is testing:
`POST /auth/login` → bearer, `GET /services/raw` → `ETag`, `PUT /services/raw`
with `If-Match` and the suite's mount set (always keeping `/services`, `/auth`,
`/data`), expecting **204**. Additional principals (`U`, `E`, `dev`, `op`,
`editor`) are created by `PUT /data/users/<email>` with a `passwordHash`
minted **by the host under test** through a pipeline `$hashPassword` step
(a `wrapper` mount identical to `tests/wrapper.rs`'s hashing façade) — so the
runner never depends on a local argon2 implementation. `afterAll` restores
`conf.base.json` and deletes what it created (datasets with `?confirm`,
directories with `?confirm`).

### F.3 Coverage (one vitest file per Rust test file it replaces)

| File | Replaces | Content |
|---|---|---|
| `store.test.ts` | `store_conformance.rs` | `assertStoreContract` at `/files`+`/docs`, `/data`+`/things`, `/q/.queries`+`/reports`, `/pipes/.pipelines`+`/flows`, `/services/code`+`/echo` (code store: keyless POST only, PUT must match hash → 409); listing contract (`$select`/`$sort` orders, missing-first, 400s); meta-sort; facets; every step of §1a–1d of the Rust file |
| `runtime-services.test.ts` | `runtime_services.rs` | file CRUD/Range/206/listing pagination, MOVE, data schema index, traversal 400s, data CRUD + 422 + PATCH + confirm 409/204, problem+json shape (`tenant`, `traceId`, `x-trace-id` header), static-site + SPA + `spaFallbackAll`, `listings:false`; tenant isolation is covered via two tenants on the Worker only (`RS2_SECOND_HOST`) and skipped on single-tenant Rust |
| `m3-surface.test.ts` | `m3_surface.rs` | query authoring/validation/execution (`X-Total-Count`, positional, coercion, 422 `errors`, SQL → 501), OPTIONS probes (`Allow`, 401 read-gated), `/services`, `/agent-surface` (`?surface=`), `/openapi` (`$ref`s, inlined dataset schema, io schemas), 405 on POST, code deploy lifecycle (`ref`, `version`, `mountedAt`, immutable `Cache-Control`, 409s, 501 for `.wasm`), JS bundle serves with a `prefix` grant (`x-engine: js`), broken bundle → 502, `x-rs2-manifest` surfacing, `specSubtree`/`authoring` |
| `caching.test.ts` | `caching.rs` | every `Cache-Control` string, `Vary`, clamp-to-private, `Set-Cookie` carve-out, 304 flows incl. `W/` and lists |
| `cors.test.ts` | `cors.rs` | preflights (204, echoed headers, credentials), decorated errors, CSRF 403, cookie attribute matrix, `allowedLoginOrigins` 403, bearer-only login |
| `auth.test.ts` | new (auth.rs semantics) | login/refresh (halfway rule)/logout/user, lockout after 5, bad token → 401 (never anonymous), `roles` array vs string, `jwtUserProps` → `{claim}` path grants, **cross-host hash**: a `passwordHash` fixture minted by the Rust host (checked in) must log in on the Worker and vice versa |
| `idempotency.test.ts` | `m2_composition.rs` g6 | fresh (no header) → replay (`Idempotency-Replayed: true`, same 201/`Location`, one record) → mismatch 422 `idempotency_key_reuse` → different path fresh → key > 256 → 400; in-flight 409 via a pipeline with a slow step (`GET` on a `code:` echo that sleeps 2 s) fired twice concurrently |
| `pipeline.test.ts` | `pipeline_access/response/validation.rs`, `operator_authority.rs`, `wrapper.rs` | per-spec access, `$response` shaping (201/`Location`/`hal+json`, text, 400 on 1000), 422 naming `steps[1]`, operator gate on `access` (set/change/remove), wrapper forwarding, hashing façade, `inputSchema` 422, discovery of wrapper/pattern, config-error responses |
| `access.test.ts` | `access_vocab.rs`, `field_authz.rs`, `dir_listing_negotiation.rs`, `dir_no_slash.rs`, `friendly_urls.rs` | verb→action defaults, field-level redaction/403, negotiated listings (`Vary`, operator bypass, `no-store`), 301 slash redirects with query, friendly URLs (`Content-Location`, pinning, HEAD, conditional delete) |
| `code.test.ts` | `store_grant.rs`, `conformance.rs` (HTTP-visible subset) | `store` grant round trip/traversal/`x-rs2-body-ref` (stripped, 502 dangling), `httpOut` denial (`capability_denied` with host), `capability_denied` for an ungranted name inside the guest, wall-clock/limit → 503 `limit_exceeded` retryable, `ctx.state` persistence across calls, 501 for `.wasm`. Uses `fixtures/echo.js` (a JS port of `conformance/echo-guest`: echoes method/url/body/config, `deny-check` path, `sleep=<ms>` query, `state` counter) |
| `logging.test.ts` | `logging.rs` (runtime half) | boundary records (`timeUnixNano` string, `severityText`, `rs2.tenant`, `error.type`, status number, `rs2.source`), trace-scoped read, `severity=warn` filter with a failed login (`rs2.source: service`, `rs2.service: auth`), info floor, NDJSON via `Accept: text/plain`, `X-Total-Count` |
| `discovery-limits.test.ts` | new | the `limits` object exists and `host` names the expected kind |
| `catalogue.test.ts` | `catalogue.rs` | only the dormant path (`enabled:false`, install → 501) unless `RS2_CATALOGUE_URL` points at a fixture server the runner starts (`fixtures/catalogue-server.mjs`) and the host was started with that host allowlisted |
| `guest-adapter.test.ts` | `guest_adapter.rs` (HTTP-visible subset, P4b) | guest (`code:`) store adapters over in-process mock Redis (RESP) + Mongo (OP_MSG/BSON) TCP backends the suite starts itself: data/file store contract, schema facet, missing-bundle 404 wording, socket denial identity, stored query over Redis, Mongo data + aggregation adapters, listing fallback vs native pushdown + catalogue `listProjection`, int64/date/ObjectId wire round-trips, the pooling observation (`guestAdapterPooling` divergence). Pool growth / idle eviction stay Rust-only in-process tests |
| `messaging.test.ts` | `message_gateway.rs` (the no-send subset) | the `message` surface as far as it can be settled **without sending**: `GET /channels` per adapter and for a routing mount, the `channels` discovery facet, every parse/route 400 wording, the 501 `provider_unavailable` when a provider has no per-message status, the build-time config 400s (`adapter` + `adapters` together, an adapter routed at a channel it does not serve, no adapter at all, an unknown `builtin:`), and the two first-party providers in `/services/catalogue/available`. Provider wire shapes (Cloudflare's JSON body, SNS's signed form post) stay in each host's unit tests against stubs — a conformance run must not send real mail or real SMS, and a mock provider reachable from both hosts would only be testing the mock |

Allowed divergences are a single table in `src/divergences.ts` keyed by
`RS2_HOST_KIND`; today it has three entries (absent-directory DELETE
204|404, dot-segment traversal 400|404, and `guestAdapterPooling` —
pooled backend connections on Rust vs. per-invocation on the Worker).

### F.4 CI

`.github/workflows/ci.yml` gains two jobs after `test-js`:

- `conformance-rust`: build `rs2-server --features js`, start it on the
  fixtures, `npm ci && npx vitest run` in `conformance/http` with
  `RS2_HOST_KIND=rust`.
- `conformance-cf`: `npm ci` in `rs2-worker`, `npx wrangler dev --port 8787`
  (local R2/DO/loader emulation; no account needed), wait for `/readyz`, run
  the same suite with `RS2_HOST_KIND=cloudflare RS2_ADMIN_TOKEN=ci`.

Both are required for merge from P2 on. `rs2-worker` also runs its own vitest
(`@cloudflare/vitest-pool-workers`) unit tests in a `worker-unit` job.

---

## G. Wrangler config, package layout, scripts

`rs2-worker/wrangler.jsonc`:

```jsonc
{
  "$schema": "./node_modules/wrangler/config-schema.json",
  "name": "rs2-worker",
  "main": "src/index.ts",
  "compatibility_date": "2026-08-26",
  "compatibility_flags": ["nodejs_compat"],
  "vars": { "RS2_DEFAULT_TENANT": "main", "RS2_MAIN_DOMAIN": "", "RS2_LOG_LEVEL": "info",
            "RS2_CATALOGUE_HOSTS": "" },
  "durable_objects": { "bindings": [
    { "name": "TENANTS",  "class_name": "TenantObject" },
    { "name": "REGISTRY", "class_name": "RegistryObject" } ] },
  "migrations": [ { "tag": "v1", "new_sqlite_classes": ["TenantObject", "RegistryObject"] } ],
  "r2_buckets": [ { "binding": "RS2_FILES", "bucket_name": "rs2-files" } ],
  "worker_loaders": [ { "binding": "LOADER" } ],
  "triggers": { "crons": ["*/5 * * * *"] },
  "workflows": [ { "name": "rs2-transfer", "binding": "TRANSFER", "class_name": "TransferWorkflow" } ],
  "observability": { "enabled": true }
}
```

Secrets (`wrangler secret put`): `RS2_ADMIN_TOKEN`; optionally `CF_API_TOKEN`
+ `CF_ZONE_ID` for custom-hostname provisioning (decision 26). Local dev:
`.dev.vars` with `RS2_ADMIN_TOKEN=dev`; `wrangler dev` provides local R2,
SQLite DOs, alarms, cron (`--test-scheduled` + `curl /__scheduled`), and the
loader (verified in P2: wrangler ≥ 4.126 runs `worker_loaders` locally). The
one thing local workerd does not enforce is the per-isolate heap cap, so the
memory-cap conformance case is skipped locally unless `RS2_CF_REMOTE` marks
a real-platform run (decision 25).

Package layout:

```
rs2-worker/
  package.json            wrangler ^4, typescript, vitest, @cloudflare/vitest-pool-workers,
                          jsonata, ajv, hash-wasm, esbuild (shim bundling)
  wrangler.jsonc  tsconfig.json  vitest.config.ts
  src/
    index.ts              fetch (ops, admin, resolve, forward), scheduled (fan-out)
    registry-object.ts    RegistryObject DO
    tenant-object.ts      TenantObject DO: handle/dispatch, alarms, invocations map, RPC
    egress.ts             Egress + HostApi WorkerEntrypoints
    runtime/              error message body media-type router path-pattern wrapper
                          dispatch tenant-build infra config-schema listing logging
                          idempotency scheduler outbound retry crypto discovery
    capabilities/         types prefixed scoped r2-file-store sqlite-data-store
                          reference-query-store sqlite-idempotency sqlite-log-store
                          fetch-http-out credential builtin-registry
    services/             context file data spec-store pipeline-service query
                          query-template template auth proxy message wrapper-service
                          services-config catalogue code log-reader
    pipeline/             spec dsl condition transform response segments executor
    engines/              dynamic-worker.ts host-api.ts guest-shim.js guest-globals.js
    workflows/            transfer.ts (P5)
    fixtures/             catalogue.json (generated by the Rust CLI, checked in)
  test/                   vitest-pool-workers unit tests (per module, mirrors rs2-core unit tests)
conformance/http/
  package.json  vitest.config.ts  src/{client,seed,divergences}.ts  fixtures/  *.test.ts
```

Scripts (`rs2-worker/package.json`): `dev` (`wrangler dev`), `deploy`,
`test` (unit), `typecheck`, `build:shim` (esbuild `src/engines/guest-shim.js`
+ `guest-globals.js` to a string module). `conformance/http`: `test`, `host:rust`, `host:cf`.
Versioning: the Worker's `RS2_VERSION` constant and `rs2-core`'s
`CARGO_PKG_VERSION` are set from the same git tag by the release workflow;
`user-agent: rs2/<version>` and `info.version` in OpenAPI therefore match.

---

## H. Phased delivery

**P1 — conformance runner green against Rust.** ✅ done.
Deliver `conformance/http/` (§F) and the small both-hosts changes it needs:
the `limits` object on `/.well-known/rs2/services`, `rs2-cli catalogue-dump`
(or an equivalent way to emit `config_schema::catalogue()` as JSON), and the
JS echo fixture. Accept: `conformance-rust` CI job green; every assertion in
§F.3 present; suite runs in < 3 min.

**P2 — Worker skeleton green on the store contract.** ✅ done.
`index.ts`, both DOs, admin API, `runtime/*` (dispatch order, router, wrapper,
error, message, media type, listing, logging, idempotency), `R2FileStore`,
`SqliteDataStore`, `file`, `data`, `spec-store` (with validators stubbed to
identity for `query`/`pipeline` authoring), `discovery`, `services-config`
(`raw`, `services`, `catalogue`, `code/` store). Accept: `store.test.ts`,
`runtime-services.test.ts`, `caching.test.ts`, `cors.test.ts`,
`idempotency.test.ts` (minus the in-flight case), `logging.test.ts`,
`discovery-limits.test.ts` green on `wrangler dev`; unit tests for
`path-pattern`, `listing`, `router`, `wrapper`, `credential` (SigV4 vector)
ported from the Rust `#[cfg(test)]` modules.

**P3 — all services.** ✅ done (WF2; the `rs2-ui`/`rs2` CLI manual checklist
remains a follow-up — the HTTP surface it uses is conformance-covered).
`auth` (hash-wasm; cross-host hash fixture), `query` + `query-template` +
`reference-query-store`, `pipeline` (all of `pipeline/*`, jsonata), `wrapper`,
`proxy`, `message` (routes + both first-party providers), `log` reader, `catalogue`,
`template` (Dynamic Worker, `globalOutbound: null`), scheduler alarms + cron
fan-out, `infra` expansion via the registry. Accept: the whole suite green on
both hosts except `code.test.ts`; `rs2-ui` built with `VITE_BASE_PATH=/admin`
and uploaded with `rs2 send` works against the Worker unchanged (manual
checklist: browse files, edit data, author a pipeline, view logs); the `rs2`
CLI's `login`, `send`, `service add`, `deploy` work against the Worker.

**P4 — code mounts.** ✅ done (WF2; the memory-cap case runs only against the
real platform, `RS2_CF_REMOTE` — local workerd has no per-isolate heap cap).
`engines/dynamic-worker.ts`, `guest-shim.js`, `Egress`/`HostApi`
entrypoints, `services/code.ts` grants (`prefix`, `httpOut`, `store`),
`x-rs2-body-ref`, request/response streaming, `compile_check` at deploy,
`guest-async` facet. Accept: `code.test.ts` green on both hosts; every
`corpus/bundles/*.js` loads and answers a mocked call (a Worker unit test
with `globalOutbound` pointed at a stub); `conformance/http` `@remote` tag
empty or documented.

**P4b — resident `code:` store adapters.** ✅ done.
`capabilities/guest-stores.ts` (the `resident.rs` port), the
`invokeAdapter`/`adapterFeatures` engine path, the `EgressSockets`
gateway with the §E.4 connect hook, the `features` RPC on `Rs2Guest`, and
the `tenant-build.ts` wiring for `code:` data/file/query/message adapters
(and `code:` spec-store backends). Proof: `conformance/http/
guest-adapter.test.ts` (14 tests — the HTTP-observable scenarios of
`rs2-core/tests/guest_adapter.rs` over in-process mock Redis/Mongo TCP
backends) green on BOTH hosts, with the full suite green alongside
(rust: 204 passed / 7 skipped; cloudflare: 203 passed / 8 skipped — the
extra skip is the `@remote` memory-cap case); `rs2-core --features js`
`guest_adapter` (10 tests) green on the extracted fixture bundles;
`rs2-worker` unit tests cover the capability mapping, error identities,
the features handshake, and the resident engine path. Pool-growth and
idle-eviction stay Rust-only internals (knobs ignored here, §A); the
pooling observation itself is conformance-covered with the
`guestAdapterPooling` divergence.

**P5 — `transfer` (follow-on).**
A Cloudflare Workflow (`TransferWorkflow`) that copies one store mount's
contents to another mount/store, resumable and ETag-preserving. API shape,
under the tenant's `services` mount (operator surface, gated by its `access`):

- `POST /services/transfers` body
  `{"from": {"mount": "/files", "path": "/docs/"}, "to": {"mount": "/archive", "path": "/2026/"}, "mode": "copy"|"move", "overwrite": "never"|"if-match"|"always"}`
  → **202** `{"id", "status": "queued", "statusUrl": "/services/transfers/<id>"}` + `Location`.
  `to` may name a mount on another RS2 host: `{"url": "https://other/files/2026/", "token": "…"}`.
- `GET /services/transfers/<id>` → `{"id","status": "queued"|"running"|"paused"|"done"|"failed","progress": {"listed","copied","skipped","failed","bytes"},"cursor","startedAt","finishedAt","errors": [{"path","status","detail"}]}`; `GET /services/transfers/` lists (dir+json, `total`).
- `POST /services/transfers/<id>/pause|resume|cancel` → 202.
- Semantics: the workflow lists the source through the **same HTTP store
  contract** (`GET <container>/?$take=1000&$skip=<cursor>`), reads each child
  with its `ETag`, writes with `If-None-Match: *` (`never`), `If-Match`
  (`if-match`, the destination's current ETag captured in a prior pass), or
  unconditional; each step is a Workflow step with durable retries; the cursor
  is the resume point. It runs as an internal caller with the operator's
  principal captured at submission. On the Rust host the same API is a
  tokio task with the status record in the data store (`.rs2-transfers`
  dataset) — designed here, built in P5 on both hosts together.

Accept: a 10 000-object mount copies across two `wrangler dev` tenants with
`ETag`s equal on both sides; pause/resume across a Worker restart.

---

## I. Decisions log (choices the brief left open)

1. **All tenant traffic passes through `TenantObject`**; no Worker-side fast
   path. Rationale: one dispatch path; the DO holds every cross-cutting store.
2. **Operator table = `RegistryObject` DO + two vars.** `RS2_DEFAULT_TENANT`
   gives zero-setup local dev and single-tenant deployments;
   `RS2_MAIN_DOMAIN` = `mainDomain`; explicit domains via `/admin/domains`.
3. **Tenant lifecycle is an admin API** (`/admin/tenants`, `/admin/domains`,
   `/admin/infras`) gated by `RS2_ADMIN_TOKEN`, mirroring the Rust
   `/admin/reload-infras` gate. Rust keeps files; the conformance runner
   handles both by pre-provisioning one tenant and reshaping it via
   `PUT /services/raw`.
4. **Bootstrap admin seeding is part of `PUT /admin/tenants/<name>`**,
   seed-if-absent, same record shape as `seed_bootstrap_admin`.
5. **Config version = sha256(text)[0..8] hex; data ETag = sha256(JSON)[0..8]
   hex; R2 ETag = R2's own.** All opaque; `PUT /services/raw` `If-Match`
   mismatch stays **409**.
6. **`materialized_body_bytes` = 32 MiB on the Worker** (128 MiB isolate);
   unknown-length uploads above it go multipart to R2. Declared in `limits`.
7. **Absent-directory DELETE → 204 on the Worker**; the only listed
   divergence the runner tolerates by host kind.
8. **`conditional-write` is atomic on the Worker** via a per-key async mutex
   inside the DO (plus R2 `onlyIf` where it applies).
9. **Guest capabilities are Promises; timers are real; platform globals are
   not shadowed.** Declared as the `guest-async` facet. `Buffer`, `process`,
   `global`, `RS2Socket` are the only shim-provided globals.
10. **Guest invocation is RPC** (`getEntrypoint("Rs2Guest").invoke(msg,
    config)`), not an HTTP `fetch` into the guest — the envelope stays a
    structured value, and host errors keep their marker shape.
11. **Egress gateway runs in the Worker isolate and defers to the DO** for
    grant decisions; sockets are enforced in the gateway's `connect()`.
12. **Guest `ctx.state` is durable** (DO KV) rather than per-instance memory.
13. **Resident (`code:`) store/message adapters shipped in P4b** (revised —
    they were 501 until then): a `store.adapter` of `code:<name>@<version>`
    on a data/file/query/message mount (and on a `specStore` block) is backed
    by the deployed bundle via `capabilities/guest-stores.ts`; the 501
    `engine_unavailable` (unchanged wording) remains only when the
    deployment has no `worker_loaders` binding. `builtin:mem` is
    SQLite-backed (durable), `builtin:reference` is the only *built-in*
    query adapter.
14. **Logs: SQLite table, 50 000-row cap per tenant**, trimmed every 256
    inserts; `emit` is synchronous (DO SQLite writes do not block on I/O);
    optional tenant `logging.sink: "none"`.
15. **Scheduler = DO alarms per tenant + a minutely cron reconcile** that
    re-arms lost alarms; claims stored in `schedule_claims`.
16. **Auth lockout, breaker, and concurrency admission live in DO memory** —
    per-tenant-correct by construction.
17. **`template` runs in a Dynamic Worker with `globalOutbound: null` and an
    empty `env`**, id keyed by the source hash; `cpuMs: 1000`.
18. **Loader limits**: `cpuMs` from a Worker-only mount field `limits.cpuMs`
    (default 5 000, ceiling 30 000) reported as `wall_clock_ms` breaches;
    `subRequests` = `outbound_calls` (64).
19. **Config catalogue JSON is generated by Rust and checked into the
    Worker** so `GET /services/catalogue` is byte-identical on both hosts.
20. **ajv** uses `Ajv2020` by default and a draft-07 instance when the
    schema declares it — matching `jsonschema` 0.33's auto-detection; error
    `path` = `instancePath`, `message` = ajv's message (wording differs from
    the Rust crate's; the runner asserts shape and non-emptiness, not text).
21. **jsonata** (JS reference) with the four host functions registered
    async-capable; 5 s timeout, depth 100.
22. **hash-wasm argon2id** with `m=19456, t=2, p=1, hashLength=32`, 16-byte
    salt, PHC output; verification honours the stored parameters on both
    hosts, so either host verifies the other's hashes.
23. **Both-hosts additions made by this work**: the `limits` discovery object,
    the `guest-async` facet name (emitted only by the Worker), the
    `catalogue-dump` CLI command, and the `transfer` API (P5). The
    `rs2-skill` `references/*.md` are updated in the same pass for each.

Decisions 24–31 were made during the P3/P4 build (WF2):

24. **Alarms self-arm; the cron is a 5-minute safety net** (amends 15).
    Every config write and every `alarm()` firing re-arms the next due time,
    and DO alarms survive eviction and deploys, so `scheduled()` only
    repairs the rare lost alarm — and pings just the tenants the registry
    knows carry scheduled mounts (`scheduledTenants()`), never the whole
    tenant list. Cron `*/5 * * * *`, not minutely.
25. **The memory-cap conformance case is the `@remote` mechanism** (amends
    §F/§H P4): local workerd enforces no per-isolate heap cap, so
    `code.test.ts` skips it on `RS2_HOST_KIND=cloudflare` unless
    `RS2_CF_REMOTE` marks a run against the real platform. No other case is
    remote-only.
26. **Custom domains can provision Cloudflare for SaaS** (extends §B.5):
    with `CF_API_TOKEN` + `CF_ZONE_ID` secrets set, `PUT/GET/DELETE
    /admin/domains/<host>` also manages a custom hostname
    (`src/domains.ts`) and reports provisioning status plus the CNAME
    target (`RS2_CNAME_TARGET`, falling back to `RS2_MAIN_DOMAIN`); without
    them the endpoints manage the registry map only and the response says so.
27. **Registry snapshot is cached in two layers** (extends §B.3 step 2): a
    30 s isolate cache plus the colo-local Cache API under a synthetic URL,
    so a cold isolate skips the DO round trip; registry writes drop both
    layers in the writing isolate, other colos converge within the TTL. The
    Worker stamps the snapshot's `infrasVersion` on every forward
    (`x-rs2-infras-version`) so the tenant DO detects an infras reload
    without its own registry RPC.
28. **Conditional writes are head-then-put inside the per-key mutex**
    (amends §C.2) — R2 `onlyIf` is not used for them; the DO's
    serialization makes the check-and-put atomic per tenant, which is what
    `conditional_write_atomic() = true` declares.
29. **The Worker claims only its exact operator routes**
    (`/admin/reload-infras`, `/admin/tenants[/…]`, `/admin/domains[/…]`,
    `/admin/infras[/…]`, `/healthz`, `/readyz`) and forwards every other
    path — including other `/admin/*` paths — to tenant routing, exactly as
    the Rust server claims only its own ops endpoints. A tenant mount at
    `/admin` works on both hosts.
30. **The `file` service persists the media type the *stored path* implies**
    (`MediaType.forPath(storePath)`), not the request's `Content-Type`:
    Rust local-fs keeps no content-type metadata and always serves
    `for_path`, and the difference is observable when a pinned
    extension-less write carries another type (a `text/markdown` PUT to
    `/page` pinned as `page.html` must serve as `text/html`).
31. **The guest shim ships as a generated module**
    (`engines/guest-shim.bundled.ts`, `npm run build:shim`) checked into the
    tree; a unit test asserts the bundle is fresh against
    `engines/guest-shim.js` + `guest-globals.js` so they cannot drift, and
    CI runs `build:shim` then `git diff --exit-code` so a stale checked-in
    bundle fails the build.
32. **Invocation attribution is an `AsyncLocalStorage`, not a module
    global** (`guest-globals.js`, loader flag `nodejs_als` — just
    `node:async_hooks`, not the full `nodejs_compat` surface). Concurrent
    invocations of one isolate interleave; a module-level "current
    invocation" would stamp one invocation's egress with another's id and
    run it under the wrong grants. The flag is the only Node surface the
    guest sees.
33. **Guest worker ids include the mount base**
    (`<tenant>:<mount base>:<name>@<version>`): grants are per mount, module
    state is per isolate, so isolates are per mount too.
34. **The globals module evaluates before the bundle** (`shim.js` imports
    `./globals.js` before `./bundle.js`): a bundle's module scope sees
    `Buffer`/`process`/`global`/`RS2Socket` and captures the wrapped
    `fetch`, matching the Rust prelude's install-then-evaluate order.

Decisions 35–40 were made during the P4b build:

35. **One adapter isolate per mount, platform-owned lifecycle** (extends
    33): a guest store adapter runs as the Dynamic Worker
    `<tenant>:<mount base>:adapter:<name>@<version>` — mount-addressed so
    two mounts of one bundle (or a mount's primary store and its
    spec-store backend, if they ever shared a ref) never share module
    state, and distinct from any `code:` *service* worker of the same
    bundle. There is no N-pool and no idle sweeper: `store.maxRuntimes`,
    `store.idleMs`, `store.idleSeconds` are accepted and ignored (§A) —
    the platform decides when the isolate is evicted, exactly as it does
    for code-mount workers.
36. **The `features` handshake is an RPC method on `Rs2Guest`**
    (`features()`, returning the bundle's `export const features`), read
    once per adapter object before its first call. `listProjection`
    reports `"fallback"` until then and `$select`/`$sort` are never
    forwarded unadvertised — the same observable laziness as Rust's
    read-at-spawn. No method was added to `HostApi`/`Egress` for this.
37. **Adapter invocations run under a deny-all `GrantedHost`** with an
    invocation record per call (grants: none; sockets via the store
    config's `{"type":"socket"}` grants only; `fetch` denied — as Rust's
    `GrantedHost::deny_all`), so `RS2Socket.connect` → `socketCheck`
    keeps the `capability_denied` identity and console/log attribution
    uses the same AsyncLocalStorage path as code mounts. Guest
    `ctx.state` is durable DO KV under the `<name>@<version>` identity
    (consistent with decision 12; Rust's resident state is in-memory).
    Error identity is Rust's byte-for-byte: bundle throw → 502
    `contract_violation`, denied socket → 403, non-2xx envelopes → the
    `store_error` status mapping, missing bundle → the `data adapter
    bundle … not found — deploy it via PUT /code/<name>` 404.
38. **Sockets bridge through `EgressSockets.connect`, and pooling is
    per-invocation** (the platform decided both): workerd dispatches a
    guest's `cloudflare:sockets` connect to the `globalOutbound`'s JS
    `connect(socket)` handler with the dialed target in
    `socket.opened.localAddress` and no channel for an invocation id — so
    an allowed `socketCheck` mints a single-use approval in the tenant DO
    and answers with `<nonce>.rs2-socket.invalid`, the name the shim
    dials; the hook takes the nonce off that name, redeems it for the
    real `host:port` (+ TLS, applied host-side against the real
    hostname — the guest could not validate a certificate for the
    synthetic name), and bridges. An unapproved connect gets a closed
    socket, never egress. The nonce, not the target, is what carries the
    grant: keying approvals by `<host>:<port>` let a same-tenant bundle
    that skipped `RS2Socket` race another mount to its approval (issue #2
    item 11), and `.invalid` is reserved (RFC 2606), so a dial that ever
    escapes the hook fails closed. And because I/O objects are
    request-scoped on Workers, a
    socket pooled in module scope dies at the invocation boundary: the
    shim's `RS2Socket` stamps its owning invocation and throws
    deterministically on cross-invocation use (the raw attempt hangs
    until the runtime kills the worker), and the shipped/fixture bundles
    catch, reconnect, and retry the exchange once — dormant on Rust,
    where the in-process pooling assertions still hold at exactly one
    connection. Declared as the `guestAdapterPooling` divergence (§A).
39. **`EgressSockets` is a subclass, not a widened `Egress`**: the base
    gateway stays pinned to exactly `["fetch"]`
    (`test/egress-surface.test.ts`), and the subclass adds only the §E.4
    `connect` hook — a platform event handler, not a guest-callable op.
    Dynamic workers get `EgressSockets` as `globalOutbound`; code-mount
    guests keep their existing behavior (mount-level `socket` grants are
    still a config-time 400, as on Rust).
40. **The conformance mock backends are in-process Node TCP servers
    started by the suite itself** (`fixtures/mock-redis.mjs`,
    `mock-mongo.mjs`, ephemeral ports written into the mount config), so
    the CI conformance jobs run the guest-adapter suite on both hosts
    with no extra services or workflow changes; local `wrangler dev`
    workerd reaches 127.0.0.1 backends directly. The Redis/file adapter
    bundles live in `conformance/http/fixtures/` (test scaffolding, not
    first-party adapters) and `guest_adapter.rs` embeds them via
    `include_str!` so one copy is held to the contract in-process and
    over HTTP; the Mongo bundles stay in `guest-adapters/` and are
    deployed from there by the suite.
41. **A guest hop advances call depth once, on both hosts.** `GrantedHost`
    (`.request`) is the single place depth advances; the message a guest
    op builds (`messageFromRequest` / `js.rs::message_from_request`, and
    the serialized-fetch path) carries the caller's depth unchanged.
    Adding one on both sides charged every guest hop two levels and
    halved the effective `maxDepth` for guest chains — the Rust host had
    the same double-count, so this is a matched fix, not a divergence
    (issue #2 item 9).
42. **The wall clock covers a streamed response, not just the phase
    before `beginStream`.** Once the guest begins streaming, `invoke`
    returns the response and leaves the handler running; the engine's
    timer keeps running with it and, on expiry, drops the invocation
    record (every further op then answers "unknown invocation") and
    aborts the sink writer, so the client's stream errors instead of
    hanging. Rust's watchdog already bounded the whole handler run; this
    matches it (issue #2 item 8). Related host-side hygiene from the same
    pass: the R2 per-key mutex is keyed by **bucket**, so ordering
    survives the store swap a config PUT causes (item 7), and idempotency
    capture uses `Body.capture` — a response over the 1 MiB replay cap
    comes back unconsumed (prefix + remainder) instead of cancelled, so
    it is unrecorded but still delivered (item 4).
43. **What the first real deploy changed** (2026-08-27, `rs2-worker.r-s.workers.dev`,
    account `jamesej@outlook.com`). Three things only a deployed Worker
    shows, none of them visible under `wrangler dev`:
    - **Cloudflare weakens ETags when it compresses.** The edge answers a
      gzip-accepting client with `W/"v"` where the host emitted `"v"` (zstd
      in practice; `curl` without `Accept-Encoding` still sees the strong
      form, which is why local runs never caught it). A client that echoes
      what it received then sends `If-Match: W/"v"` — and the config path
      compared quoted-but-not-weak-stripped, so **every** read-modify-write
      of `/services/raw` from a browser or SDK failed with 409. Fixed on
      both hosts (`services::etag_version` / `etagVersion`, used by
      `PUT /services/raw` and `PUT /admin/tenants/<name>`); the store paths
      already stripped `W/`. Conformance pins it (m3-surface: "config
      If-Match accepts the weak form of the version"), and the runner
      canonicalizes response ETags rather than asserting the edge's form.
    - **Workers Free caps subrequests at 50 per invocation**, below the
      advertised `outboundCalls: 64`, so the platform kills the guest
      (502 `contract_violation`, "Too many subrequests by single Worker
      invocation") before RS2's own budget answers 503. The remote
      conformance run fails exactly that one case on a Free-plan account;
      on Workers Paid (1 000) RS2's budget is the one that binds.
    - **The edge rejects some requests before the host sees them** — a NUL
      byte in the path is Cloudflare's own 400 (text/html), and a plaintext
      raw-socket probe never reaches a TLS listener. The traversal case
      therefore asserts the RS2 wording only when RS2 answered.
    Remote run (`RS2_BASE_URL` + `RS2_CF_REMOTE=1`, `guest-adapter.test.ts`
    excluded — its mock backends are `127.0.0.1` TCP servers a deployed
    Worker cannot dial): **190 passed, 7 skipped, 1 failed** (the subrequest
    cap above). The `@remote` memory-cap case passes, as designed.
44. **Host limits are operator-configurable, and the platform must never be
    the limit that binds.** Two halves, both from the first deploy:
    - The guest loader was given `limits: {subRequests: outboundBudget}`,
      which made workerd — not RS2 — reject the call one past the budget.
      The breach then reached the client as a 502 `contract_violation`
      quoting "Too many subrequests by single Worker invocation" instead of
      the 503 `limit_exceeded` naming `outbound_calls` that the contract
      (and Rust) promises. Local workerd does not enforce that loader
      limit, so only a deployed run showed it. The cap is gone: RS2 counts
      every guest egress itself, and the platform's ceiling sits behind it
      as a backstop. **Any** platform ceiling below an RS2 limit destroys
      the error's identity this way — that is the rule the fix encodes.
    - Which leaves the ceiling itself an operator concern, because it
      varies by plan (Workers Free 50 subrequests per invocation, Paid
      1 000) and the default budget is 64. So the limit table is now
      configurable on both hosts, under the names discovery reports:
      `serverConfig.limits` (Rust, unknown key ⇒ startup error) and the
      `RS2_LIMITS` JSON var (Worker, bad value ⇒ warn and keep the
      default — a var must not be able to take the Worker down).
      `memoryBytes` is refused on the Worker: the platform fixes it, and
      advertising a number nothing enforces would be a lie. A Free-plan
      deployment sets `{"outboundCalls": 45}` and the whole remote suite
      passes (191 passed, 7 skipped, 0 failed).

45. **A domain attaches only once its DNS proves it, and the contract is the
    lifecycle — not Cloudflare's API.** `PUT /admin/domains/<host>` used to
    write the routing map immediately, which made the endpoint a squatting
    tool: any caller could point a hostname it did not own at its own tenant
    and wait for the real owner's DNS to arrive. The write is now a *claim*,
    and a provider reporting the host verified is what promotes it (§B.5).
    Cloudflare's own domain validation is that proof where Cloudflare for SaaS
    is configured; where it is not, a self-check challenge
    (`/.well-known/rs2/domain-challenge/<token>`, bound to the asking `Host`)
    is — so no separate claim protocol had to be invented, and the
    provider-less "registry-only" mode, which accepted a domain and
    provisioned precisely nothing while reading as success, is gone.
    Two providers exist from the start deliberately: with one, the interface
    would have been Cloudflare's API with a coat of paint. What clients see is
    `status` (`pending`|`active`), `dnsRecords`, `nextStep` — portable — with
    every provider-specific string quarantined under `provider.detail`. The
    Rust host answers the read half of that contract from its static tenancy
    map and refuses the write half with 501 `provider_unavailable` (a new code
    in `error.rs`: the host understands the request and has nothing configured
    that could carry it out), naming `serverConfig.tenancy.domainMap` as the
    place to make the change. Giving Rust a writable map is a much larger
    change — mutable tenancy plus inbound TLS it does not own — and this
    leaves the door open for it: `ManualProvider` is exactly what it would
    use.
46. **A provider capability written against one provider is not a capability.**
    Outbound messaging replaced the `sms` service and `SmsGateway` with a
    `message` service over a `MessageGateway` covering email and SMS at one
    mount. The old surface had no users, so it was removed rather than
    aliased. Building **two** adapters before freezing the trait — Cloudflare
    Email Service and AWS SNS — falsified two assumptions that a single
    provider would have baked in permanently:
    - **`status` is not universal.** SNS has no per-message status API (it
      needs CloudWatch delivery logging, which is not a lookup); Cloudflare's
      REST send answers per-recipient *at send time*. So `delivery_status()`
      is a declared facet — the `listing_pushdown` pattern — and the service
      turns "no" into a 501 `provider_unavailable` naming the provider,
      rather than every caller discovering the gap by trying.
    - **A message id is not universal either.** SNS mints one; the Cloudflare
      REST send mints none, because it already reported what happened.
      `Receipt.id` is therefore optional, with the provider's own answer
      passed through in `detail`. Together `id` and `delivery_status()` let a
      caller tell which of the three provider shapes it is talking to without
      knowing the provider.
    The payload took the same lesson structurally: `Outbound` is
    channel-tagged (a Rust enum, a TS discriminated union), so `subject` on an
    SMS is not representable — one bag of optionals would have made every
    adapter re-validate the same invariants. One mount serves both channels
    through `store.adapters` (`channel → adapter`) composed behind a
    `RoutingGateway`; routing an adapter at a channel it does not serve is a
    build-time 400, not a first-send surprise. Both adapters call their
    provider through the host's `HttpOut`, so provider traffic crosses the one
    audited egress choke point, and both take credentials from operator infra
    (`cf-email` a bearer token, `aws-sns` the existing vector-tested
    `awsSigV4` strategy — no new cryptography). The rule for the next domain:
    **write the second adapter before freezing the trait, and prefer a
    declared facet to a method every provider has to fake.**
