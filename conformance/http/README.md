# HTTP conformance runner

Black-box conformance suite for the RS2 HTTP API. It only speaks HTTP to
`RS2_BASE_URL`, so the same suite holds both hosts — the Rust server
(`rs2-server`) and the Cloudflare Worker (`rs2-worker/`) — to one contract.
The spec is `docs/agents/cloudflare.md` §F; this README is the operator's
card.

Node 22+, vitest, ESM. No other test framework.

## Run against the Rust host

```sh
cd conformance/http
npm ci
# terminal 1 — builds (or reuses) rs2-server --features js, seeds a fresh
# temp data dir, listens on 127.0.0.1:3100
npm run host:rust
# terminal 2
npm test
```

`host:rust` copies `fixtures/rust/` (serverConfig + `tenants/conf.json`) to
`<tmpdir>/rs2-conf-<port>`, rewrites `listen` to the port, and runs
`cargo run -p rs2-server --features js -- <config>` from the repo root. Set
`RS2_SERVER_BIN=/path/to/rs2-server` to skip cargo and run a prebuilt
binary (it must be built with `--features js` — the code-store cases deploy
a JS bundle). It prints `ready` once `/readyz` answers.

## Run against the Cloudflare host

```sh
# terminal 1 (needs rs2-worker/ — P2 onwards)
RS2_ADMIN_TOKEN=dev npm run host:cf          # wrangler dev on 127.0.0.1:8787
# terminal 2
RS2_HOST_KIND=cloudflare RS2_PORT=8787 RS2_ADMIN_TOKEN=dev npm test
```

The runner's `globalSetup` then provisions the tenant through
`PUT /admin/tenants/conf` (seed-if-absent) before the first suite.

## The port rule

**One host per port, one suite run per host.** Suites reshape the single
shared tenant (`PUT /services/raw`), so vitest runs files strictly
sequentially (`fileParallelism: false` in `vitest.config.ts`) — never turn
that on. To run suites in parallel (several agents, CI shards) start
several hosts on different ports; `host:rust` keeps a per-port data dir so
they do not share state:

```sh
RS2_PORT=3101 npm run host:rust &
RS2_PORT=3102 npm run host:rust &
RS2_PORT=3101 npx vitest run store.test.ts
RS2_PORT=3102 npx vitest run caching.test.ts
```

`RS2_PORT` is read by both the host script and the runner (as the default
for `RS2_BASE_URL`).

## Environment (spec §F.1)

| Var | Default | Meaning |
|---|---|---|
| `RS2_PORT` | `3100` | Port for `host:rust`, and the default `RS2_BASE_URL` port |
| `RS2_BASE_URL` | `http://127.0.0.1:$RS2_PORT` | Where the host listens |
| `RS2_HOST` | host of `RS2_BASE_URL` | Sent as `Host` (and used as the CORS same-origin host) |
| `RS2_TENANT` | `conf` | Tenant name expected in problem bodies |
| `RS2_ADMIN_EMAIL` / `RS2_ADMIN_PASSWORD` | `admin@conf.test` / `conf-admin-pw` | The fixture tenant's bootstrap admin (role `A`) — must match what the host was started with |
| `RS2_ADMIN_TOKEN` | — | Worker only: gates `globalSetup` seeding through `/admin/tenants` |
| `RS2_HOST_KIND` | `rust` | `rust` \| `cloudflare` — selects the allowed-divergence table (`src/divergences.ts`) |
| `RS2_CODE_BUNDLE` | `fixtures/echo.js` | The JS echo bundle used by code-mount cases |
| `RS2_SECOND_HOST` | — | Worker only: a second tenant's `Host` for isolation cases |
| `RS2_SERVER_BIN` | — | `host:rust` only: prebuilt server binary instead of `cargo run` |
| `RS2_HOST_DIR` | `<tmpdir>/rs2-conf-<port>` | `host:rust` only: the scratch data dir |
| `RS2_CF_PERSIST` | `<tmpdir>/rs2-conf-cf-<port>` | `host:cf` only: wrangler's local state dir |

## Layout

```
package.json  vitest.config.ts  tsconfig.json
src/
  client.ts          Rs2Client (base URL, Host, bearer), Rs2Response (buffered;
                     json()/problem()/listing()/etag()/totalCount()), env(),
                     ETag + listing helpers
  seed.ts            Seed (login, GET/PUT /services/raw with If-Match, applyMounts,
                     createPrincipals via the host's hashing facade, restore),
                     provisionTenant() for either host kind
  divergences.ts     the allowed-divergence table keyed by RS2_HOST_KIND
  store-contract.ts  assertStoreContract / assertListingContract /
                     assertMetaSortContract / assertCodeStoreContract
  global-setup.ts    vitest globalSetup → provisionTenant()
fixtures/
  conf.base.json     the fixture tenant (spec §F.2) — every suite starts here
  rust/              serverConfig.json + tenants/conf.json for host:rust
  echo.js            JS port of conformance/echo-guest for code: mounts
  catalogue.json     `rs2 catalogue-dump` output (the Worker checks in a copy)
  redis-data.js      guest-adapter bundles (P4b): a RESP DataStore, a RESP
  redis-query.js     QueryStore, and an in-memory FileStore — ALSO embedded
  guest-file.js      into rs2-core/tests/guest_adapter.rs via include_str!,
                     so one copy is held to the contract on every host
  mock-redis.mjs     in-process Node TCP mock backends for the guest-adapter
  mock-mongo.mjs     suite (RESP subset with a connection counter; MongoDB
                     OP_MSG + a hand-rolled BSON codec)
scripts/
  host-rust.mjs  host-cf.mjs
*.test.ts            one file per Rust test file it replaces (spec §F.3)
```

## Adding a suite

1. Name it after the Rust file it replaces (`caching.test.ts` for
   `caching.rs`) and copy that file's assertions step by step — same order,
   same status codes, same header names. Keep the `[mount]`-style tags in
   assertion messages.
2. Shape the tenant in `beforeAll` and put it back in `afterAll`:

   ```ts
   import { afterAll, beforeAll, describe, expect, test } from "vitest";
   import { Seed } from "./src/seed.ts";

   let seed: Seed | undefined;
   beforeAll(async () => {
     seed = await Seed.create();                       // admin login + reset to conf.base.json
     await seed.applyMounts(                           // base mounts + yours, PUT /services/raw + If-Match → 204
       [{ path: "/files", service: "file", config: { access: "open" } }],
       { cors: { allowedOrigins: ["https://x.example"] } },   // optional top-level overrides
     );
     await seed.createPrincipals([                     // hashes minted by the host under test
       { email: "dev@conf.test", password: "dev-pw", roles: "U", extra: { team: "blue" } },
     ]);
     seed.trackDataset("/data", "orders");             // confirm-deleted in restore()
     seed.trackDir("/files", "docs");
   });
   afterAll(async () => { await seed?.restore(); });
   ```

   `seed.anon` is an unauthenticated client, `seed.admin` bears the admin
   token, `await seed.clientAs({ email, password })` any other principal.
   `seed.etag` tracks the config version; `seed.tryPutConfig(...)` is the
   raw form for suites that test `/services/raw` itself.
3. Never branch on the host kind inside a test. If the hosts legitimately
   differ, add a row to `src/divergences.ts` **and** to the table in
   `docs/agents/cloudflare.md` §A, then use `expectDivergent(...)`.
4. Bodies: `Rs2Response` is fully buffered — `res.text()`, `res.json()`,
   `res.problem()` (asserts `application/problem+json`), `res.listing()`
   (asserts `application/vnd.rs2.dir+json`), `res.etag()`,
   `res.totalCount()`. Use `client.stream(...)` for the streaming cases.
   Redirects are not followed (`redirect: "manual"`) so 301s are observable.
5. Run it alone first on its own port, then the whole suite, before opening
   a PR: `RS2_PORT=3105 npx vitest run yours.test.ts`.

## The guest-adapter suite and its mock backends

`guest-adapter.test.ts` (spec P4b) proves `store.adapter:
"code:<name>@<version>"` mounts — the guest-backed data/file/query stores —
over real wire protocols. Its backends are **in-process Node TCP servers**
the test file starts in `beforeAll` (`fixtures/mock-redis.mjs`,
`fixtures/mock-mongo.mjs`) on ephemeral 127.0.0.1 ports, written into the
mount configs it applies — so the suite is self-contained: no external
services, no extra env vars, and the CI conformance jobs run it on both
hosts unchanged. The Mongo bundles are deployed from `guest-adapters/`
(the shipped adapters); the Redis/file bundles are fixtures here and are
embedded into `rs2-core/tests/guest_adapter.rs` via `include_str!` (edit
them and re-run BOTH the Rust test and this suite).

Local `wrangler dev` workerd reaches 127.0.0.1 backends through the
worker's socket bridge without special configuration (the mocks bind
127.0.0.1; nothing needs 0.0.0.0). Note the `guestAdapterPooling`
divergence: the Rust host pools one backend connection per mount for the
whole run; the Worker reconnects per invocation (I/O objects are
request-scoped on that platform).

## Regenerating `fixtures/catalogue.json`

```sh
cargo run -p rs2-cli -- catalogue-dump > conformance/http/fixtures/catalogue.json
```

The Worker checks the same document in as `rs2-worker/src/fixtures/catalogue.json`
so `GET /services/catalogue` is byte-identical on both hosts.
