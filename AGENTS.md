# AGENTS.md — working in rs2-runtime

RS2 is a Rust reimplementation of the Restspace core: services are functions on
HTTP messages, mounted per tenant, composable into pipelines, with custom code
sandboxed under hard limits. PRD: `PRD-runtime-v2.md` (in the `rs-runtime` repo).
**Repo layout, build commands, and run instructions are in `README.md` — read
it first.** This file is the agent working guide.

## Deep topics (read the relevant one before you start)

- **[docs/agents/architecture.md](docs/agents/architecture.md)** — the design
  throughline (capabilities, host choke points, the store/spec patterns, the
  instruction plane). Read before adding a service, capability, or anything
  cross-cutting.
- **[docs/agents/conventions.md](docs/agents/conventions.md)** — editing,
  commit, and workflow rules. Read before your first edit or commit.
- **[docs/agents/pitfalls.md](docs/agents/pitfalls.md)** — Rust + engine traps
  that have bitten us (Body-not-Sync, wasmtime 37, v8 150, features).
- **[docs/agents/testing.md](docs/agents/testing.md)** — the test matrix,
  integration harness, conformance, and benchmarks.
- **[docs/agents/loadable-adapters.md](docs/agents/loadable-adapters.md)** — the
  deno_core engine + loadable-adapter arc (G13): Phase 0/1 done, Phase 2 design.
  Read before touching the JS engine, sockets, or adapter wiring.

## Five-second version

- **Test matrix:** `cargo test -p rs2-core` (native) and `--features js` (V8) —
  run **both** before declaring done; `--features wasm` when touching the
  engine/contract; `cargo build` for the workspace. Details: `testing.md`.
- **Never** bulk-edit `.rs` files with PowerShell `-replace`/`Set-Content` — PS
  5.1 re-encodes UTF-8 and mojibakes `§`/em-dashes (bit us twice). Use the Edit
  tool. → `conventions.md`
- **`Body` is Send-not-Sync:** never hold a `&Message` across an `.await`;
  extract owned values first. → `pitfalls.md`
- **Cross-cutting concerns live in the host** (`Runtime::dispatch`/`handle`),
  not in services. Service **variants are config, not forks**. Reuse is type
  composition (own a `FileService`, decorate with `PrefixedFileStore`), not
  duplication. → `architecture.md`
- **Commit** via `.git\COMMIT_MSG_TMP` + `git commit -F` (PowerShell mangles
  `-m`); trailer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; push
  only when asked. → `conventions.md`

## This repo vs. the skill

The `rs2-skill` repo is the **user-facing** skill (how to operate an RS2 server).
This file is for **developing** the runtime. When a change alters behavior a
user sees (a config field, endpoint, header, or facet), update the skill's
`references/*.md` in the same pass.
