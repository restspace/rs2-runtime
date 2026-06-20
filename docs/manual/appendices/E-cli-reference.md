# Appendix E — The `rs2` CLI command reference

The `rs2` CLI is the developer loop — scaffold, run, validate, deploy, migrate.
It is **not** an API client; you call tenants over plain HTTP (Appendix C / Part
9). Run it as the installed `rs2` binary or `cargo run -p rs2-cli -- <args>`.

## Verbs

| Command | Does |
| --- | --- |
| `rs2 new <name> [--js]` | Scaffold a custom-service project. Default Rust/Wasm against the published WIT (compiles as-is with `cargo build --target wasm32-wasip2 --release`); `--js` is a single-file ESM scaffold with `manifest.json` |
| `rs2 dev [serverConfig.json]` | Run a local node (same code as `rs2-server`) |
| `rs2 test [projectDir] [--component <path>]` | Validate `manifest.json` and the built component (wasm header; engine compile-check when built `--features wasm`) |
| `rs2 deploy <file> --name <n> [--server <url>] [--token <t>] [--bundle]` | Upload to `PUT <server>/code/<n>`. `.js`/`.mjs` → JS bundle; `\0asm` → component. `--bundle` runs `npx esbuild … --bundle --format=esm --platform=browser` first |
| `rs2 migrate <services.json> [-o tenant.json]` | Convert a v1 Restspace config to an RS2 tenant config (Part 11) |
| `rs2 pull [--host <url>] [--dir <d>]` | Mirror the tenant's instruction plane (config + every `specSubtree` store + code pins) into a local `rs2/` directory for git-based editing; records baseline ETags in `rs2/mirror.json` (§3.6) |
| `rs2 push [--dir <d>] [--dry-run] [--allow-secret-rotation]` | Push local instruction-plane edits back through the validated APIs (config `If-Match`, spec `If-Match`/412); aborts on a remote change rather than clobbering. `--dry-run` shows the diff; refuses a real secret value in a `"<secret>"` slot without `--allow-secret-rotation` |

## `rs2 deploy` flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--name <n>` | — | The bundle name in the code store |
| `--server <url>` | `http://127.0.0.1:3100/services` | The `services` API base |
| `--token <t>` | — | Bearer token for a protected `services` mount |
| `--bundle` | off | esbuild the entry first (Node.js on PATH required) |

## Typical workflows

**A new JS service:**

```powershell
rs2 new my-svc --js
# edit my-svc/…
rs2 deploy my-svc/entry.ts --name my-svc --bundle --token $admin
# then mount code:my-svc@<version> via PUT /services/raw
```

**A new Wasm service:**

```powershell
rs2 new my-svc
cargo build --target wasm32-wasip2 --release
rs2 test my-svc
rs2 deploy target/wasm32-wasip2/release/my_svc.wasm --name my-svc
```

**Migrate from v1:**

```powershell
rs2 migrate services.json -o tenant.json
# read the warnings; follow the post-migration checklist (11.3)
```

## Windows notes

- `--bundle` shells to `npx` (`npx.cmd`) — Node.js must be on PATH.
- JSON files the CLI reads tolerate UTF-8 BOMs (PowerShell `-Encoding utf8` adds
  one).
- For scripting HTTP against a running node, prefer `Invoke-RestMethod` with
  `ConvertTo-Json` bodies, or `curl.exe` — PowerShell 5.1 mangles inline JSON
  quotes in some contexts.

---

← [Appendix D — Default limits](D-default-limits.md) · [Manual home](../README.md) · [Next: Appendix F — Further reading →](F-further-reading.md)
