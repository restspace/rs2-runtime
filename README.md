# RS2 — Sandboxed Composable-Service Runtime

[![ci](https://github.com/restspace/rs2-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/restspace/rs2-runtime/actions/workflows/ci.yml)

RS2 is a backend you configure rather than write. A single Rust server hosts
**services** — file store, JSON data store, queries, auth, pipelines — that you
**mount at URL paths** with a JSON config, **compose into pipelines**, and
extend with **your own code running in a sandbox**. It is the second-generation
runtime for [Restspace](https://restspace.io), rebuilt in Rust.

```jsonc
// tenants/main.json — this is a working backend
{
  "mounts": [
    { "path": "/files",  "service": "file",  "config": { "access": "open" } },
    { "path": "/data",   "service": "data",  "config": { "access": "open", "enforceSchema": true } },
    { "path": "/auth",   "service": "auth",  "config": { "userDataset": "users" } },
    { "path": "/pipes",  "service": "pipeline", "config": { "access": "open" } }
  ]
}
```

```text
PUT  /files/docs/a.txt           streamed file write
GET  /files/docs/                paginated JSON directory listing
PUT  /data/people/.schema.json   install a JSON Schema for a dataset
PUT  /data/people/ada            schema-validated write (422 on violation)
POST /auth/login                 JWT login (cookie or bearer)
```

## Why RS2

- **Services are functions on HTTP messages.** Every service — built-in or
  yours — has the same shape: message in, message out. That makes everything
  composable: pipelines chain services, services wrap other services, and one
  client codepath (the *store pattern*) works against every store-like mount.
- **Untrusted code is contained, not trusted.** Custom services run as Wasm
  components (Wasmtime) or JavaScript bundles (V8 isolates) under hard
  wall-clock, memory, and outbound-call limits. All I/O goes through
  **capability grants** — default-deny allowlists for hosts, stores, and
  sockets. An infinite loop or allocation bomb in one tenant's code gets a
  structured 503; measured impact on a neighbor tenant's p99 is microseconds.
- **Multi-tenant from the ground up.** One node serves many tenants (by domain
  or subdomain), each with its own mounts, users, roles, and data — with
  host-enforced tenant scoping on every storage capability.
- **The whole backend is data.** A tenant is fully described by its config
  plus its stored pipelines, queries, and code bundles — inspectable,
  diffable, and hot-reloadable through the runtime's own HTTP API, with
  validation before anything goes live.
- **Agent- and client-friendly by construction.** Every tenant self-describes
  at `/.well-known/rs2/`: a mount catalogue, an agent surface (entities,
  actions, effect classes, idempotency guidance), and OpenAPI 3.1 generated
  from the same schemas the runtime actually enforces.

## What's in the box

| | |
|---|---|
| **`file`** | streamed uploads, Range/206, ETags, directory listings, static-site + SPA modes |
| **`data`** | schema-validated JSON CRUD, merge PATCH, dataset schemas, keyless POST |
| **`query`** | stored, parameterized queries; injection-safe JSON templates or bind-param SQL |
| **`pipeline`** | serial/parallel/conditional composition, JSONata transforms, retries, idempotency |
| **`auth`** | JWT login/refresh/logout, argon2id, lockout, per-mount RBAC with path-scoped grants |
| **`template`** | JSX templates rendering data to HTML |
| **custom code** | your Wasm components / JS bundles, deployed over HTTP, sandboxed under grants |

Cross-cutting and host-enforced (services never reimplement them): access
control, per-tenant concurrency limits + circuit breaker, retry policies with
effect classes, `Idempotency-Key` dedupe/replay, caching headers, CORS + CSRF,
structured problem+json errors, and OTel-shaped structured logging with a
queryable per-tenant log store.

The JS sandbox ships an npm-compat layer good enough that the official
`stripe`, `openai`, `@anthropic-ai/sdk`, `@octokit/core`, `@supabase/supabase-js`
and other SDK packages run **unmodified** (esbuild-bundled, mocked-fetch
corpus in `tests/sdk_corpus.rs`). Tenants can even bring their own storage
backend: a JS adapter speaking a raw wire protocol (Redis, MongoDB) over a
socket grant can back the stock `data`/`file`/`query` services.

## Install

Linux x86-64 (systemd service, from GitHub releases):

```bash
curl -fsSL https://github.com/restspace/rs2-runtime/releases/latest/download/install.sh | sudo bash
```

See [`deploy/`](deploy/README.md) for what the installer does, the Apache
vhost template, and build-from-source instructions. Two prebuilt variants
ship per release: `rs2-server` (native + Wasm engines) and `rs2-server-js`
(adds the V8 JS engine).

Or build from source on any platform (pinned toolchain via
[`rust-toolchain.toml`](rust-toolchain.toml)):

```bash
cargo build --release -p rs2-server --features wasm,js
```

## Quick start

```bash
cargo run -p rs2-server -- serverConfig.json
```

`serverConfig.json` sets the listener, tenancy mode, and data root; tenant
configs live in `tenants/<name>.json` (this repo ships a working example of
both). Then:

```bash
curl -X PUT localhost:3100/files/notes/hello.txt -d 'hi'
curl localhost:3100/files/notes/
curl localhost:3100/.well-known/rs2/services
curl localhost:3100/healthz
```

The **[User Manual](docs/manual/README.md)** is the full documentation — a
front-to-back course from first request to custom sandboxed services and
production operation, written for anyone comfortable with HTTP and JSON (no
Rust required). The `rs2` CLI (`rs2 new / dev / test / deploy / migrate`)
covers the developer loop, including migration from a Restspace v1
`services.json`.

## Repo layout

```
rs2-core/       the runtime library: router, pipeline executor, services,
                engines (native / wasm / js), capabilities, adapters
rs2-server/     the server binary (HTTP listener, config loading, wiring)
rs2-cli/        the `rs2` developer CLI
conformance/    Wasm guest component for the engine conformance suite
docs/manual/    the User Manual
docs/agents/    working guides for AI-agent development on this repo
deploy/         production install kit (installer, systemd unit, vhost)
```

## Build & test

```bash
cargo test                        # native engine only — fast build
cargo test --features wasm        # + Wasmtime component engine
cargo test --features js          # + V8 isolate engine (heavy build)
```

The engine conformance suite pins identical semantics (message model,
capability denial, limit enforcement, state, isolation) across all three
engines. Details, including running conformance against the real Wasm guest:
[`docs/agents/testing.md`](docs/agents/testing.md). Implementation status and
measured benchmarks: [`docs/implementation-status.md`](docs/implementation-status.md).

> **Status:** pre-1.0. The HTTP surface and config format are stable in
> practice; the Rust embedding API is 0.x and unstable. Only the server
> binary is supported.

## License

RS2 is **dual-licensed**: open source under **AGPL-3.0-only**
([`LICENSE`](LICENSE)), with a **commercial license** available for
closed-source embedding or hosted offerings — see
[`LICENSING.md`](LICENSING.md). Contributions: [`CONTRIBUTING.md`](CONTRIBUTING.md).
Security reports: [`SECURITY.md`](SECURITY.md).
