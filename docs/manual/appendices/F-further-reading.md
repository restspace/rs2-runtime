# Appendix F — Further reading

This manual teaches you to *use* a running RS2 node. These resources go deeper or
serve adjacent audiences.

## In this repository

| Resource | Audience / contents |
| --- | --- |
| `README.md` | Repo layout, build/test/run commands, and milestone status — the authoritative summary of what's implemented |
| `AGENTS.md` | The agent working guide for developing the runtime itself |
| `docs/agents/architecture.md` | The design throughline: capabilities, host choke points, the store/spec patterns, the instruction plane (the *why* behind Part 3) |
| `docs/agents/conventions.md` | Editing, commit, and workflow rules for contributors |
| `docs/agents/pitfalls.md` | Rust + engine traps encountered building the runtime |
| `docs/agents/testing.md` | The test matrix, conformance suite, and benchmarks |

## The operator skill

`C:\dev\rs2-skill` is the **user-facing skill** — a concise operational reference
for driving a running RS2 server. It covers the same surface as this manual in
terse, lookup-oriented form:

| Reference | Topic |
| --- | --- |
| `references/services.md` | The prebuilt services |
| `references/http-api.md` | Discovery, auth, errors, idempotency, limits |
| `references/pipelines.md` | Spec, conditions, transforms, retries, segments |
| `references/custom-services.md` | Writing, validating, deploying, granting |
| `references/cli.md` | The `rs2` CLI, server config, v1 migration |
| `references/v1-patterns.md` | Restspace v1 patterns and their RS2 equivalents |

Use the manual to learn; use the skill to look things up once you know the
concepts.

## Specification

The **PRD** (`C:\dev\rs-runtime\docs\PRD-runtime-v2.md`) is the product
requirements document the runtime implements. Section references in this manual
(e.g. "PRD §9.2") point into it. It's the place to understand intended behavior at
the spec level, including open questions and milestone scope.

## How the pieces relate

- **This manual** — learn to build on RS2 (you are here).
- **The skill** — operate a running RS2 server (quick reference).
- **`docs/agents/` + the PRD** — develop and reason about the runtime itself.

If you find a discrepancy between this manual and the runtime's behavior, the
runtime's `README.md` and source are authoritative for *what's implemented*; the
PRD is authoritative for *what's intended*.

---

← [Appendix E — The rs2 CLI reference](E-cli-reference.md) · [Manual home](../README.md) · [Next: Appendix G — How the JavaScript engine works →](G-v8-isolate-engine.md)
