# Appendix E — The `rs2` CLI command reference

The `rs2` CLI covers the developer loop plus a small set of admin/ops tasks. It
is not a general API client — use plain HTTP for everything outside these
commands. Run it as the installed `rs2` binary or
`cargo run -p rs2-cli -- <args>`.

The CLI is **built from source** — a release ships server binaries only:

```bash
cargo build --release -p rs2-cli    # binary at target/release/rs2
```

## Verbs

| Command | Does |
| --- | --- |
| `rs2 new <name> [--js]` | Scaffold a custom-service project. Default Rust/Wasm against the published WIT (compiles as-is with `cargo build --target wasm32-wasip2 --release`); `--js` is a single-file ESM scaffold with `manifest.json` |
| `rs2 dev [serverConfig.json]` | Run a local node (same code as `rs2-server`) |
| `rs2 test [projectDir] [--component <path>]` | Validate `manifest.json` and the built component (wasm header; engine compile-check when built `--features wasm`) |
| `rs2 deploy <file> --name <n> [--server <url>] [--token <t>] [--bundle]` | Keyless `POST` to `<server>/code/<n>/`; the content-addressed store derives the version. `.js`/`.mjs` → JS bundle; `\0asm` → component. Server and token default from `rsconfig.json` like the other admin verbs. `--bundle` runs `npx esbuild … --bundle --format=esm --platform=browser` first |
| `rs2 template build <entry.jsx|tsx> [--out <file>]` | Bundle a Preact component into the `{source, contentType}` envelope stored by a `template` mount (6.6) |
| `rs2 migrate <services.json> [-o tenant.json]` | Convert a v1 Restspace config to an RS2 tenant config (Part 11) |
| `rs2 login [--host <url>] [--email <e>] [--password <p>]` | Log in through `/auth/login` and save the token, expiry, and issuing host to `rsconfig.json` |
| `rs2 send <path> --file <local> [--content-type <ct>]` | PUT one local file to the configured host; content type is inferred when omitted; uses a saved valid token when present |
| `rs2 send <path> --dir <local-dir>` | Recursively PUT every file under `<local-dir>` to `<path>`, preserving the tree (e.g. a Vite `dist/` into a static-site mount — see 4.3); content type is inferred per file; stops at the first failed upload |
| `rs2 service add <mount.json> [--path <p>]` | ETag-safe read/append/write of one new mount; refuses an occupied path |
| `rs2 service set-access <path> --access <json> [--set k=v]…` | ETag-safe update of an existing mount's access and optional config keys |
| `rs2 auth init --admin-email <e> [options]` | One-shot auth bootstrap on a fresh open node: enable auth, create/login first admin, lock user store and services mount |
| `rs2 auth enable [options]` | Idempotently add auth settings/mount and temporarily open a user store for bootstrap |
| `rs2 auth create-admin --email <e> [options]` | Hash a password locally and seed the first operator while that user store is open; skips an existing visible record |
| `rs2 pull [--host <url>] [--dir <d>]` | Mirror the tenant's instruction plane (config + every `specSubtree` store + code pins) into a local `rs2/` directory for git-based editing; records baseline ETags in `rs2/mirror.json` (§3.6) |
| `rs2 push [--dir <d>] [--dry-run] [--allow-secret-rotation]` | Push local instruction-plane edits back through the validated APIs (config `If-Match`, spec `If-Match`/412); aborts on a remote change rather than clobbering. `--dry-run` shows the diff; refuses a real secret value in a `"<secret>"` slot without `--allow-secret-rotation` |
| `rs2 run <script>` | Run one `rs2` command per line, skipping blanks/comments and aborting on the first failure; `dev` is rejected because it never returns |

## Global flags

| Flag | Notes |
| --- | --- |
| `--ca-file <pem>` | Extra certificate authorities to trust, **added** to the roots RS2 already has. Accepted on any verb — see [TLS and certificate authorities](#tls-and-certificate-authorities) |

## TLS and certificate authorities

The CLI trusts the **union** of your operating system's trust store and a
bundled copy of the Mozilla root list. The OS store is what carries a private
CA — a corporate proxy, a CI TLS-inspection appliance, an internal PKI — so
installing the CA there is usually all that's needed. The bundled roots are
kept alongside it because some environments have no OS trust store at all (a
scratch container with no `ca-certificates` package) and because Windows fills
its store lazily; dropping either source would break hosts that work today.

When installing the CA system-wide isn't an option, name a PEM bundle:

```bash
rs2 --ca-file /etc/ssl/corp-root.pem send /files/report.pdf --file report.pdf
```

| Setting | Where | Effect |
| --- | --- | --- |
| `--ca-file <pem>` | flag, any verb | Adds the PEM bundle's authorities to the roots |
| `RS2_CA_FILE` | environment | The same, for a shell session or a CI job |
| `"caFile"` | `rsconfig.json` | The same, carried with the repo's server identity; a relative path resolves against the config file |
| `SSL_CERT_FILE` / `SSL_CERT_DIR` | environment | Honoured with their conventional OpenSSL meaning: they **replace** the OS trust store rather than adding to it |
| `RS2_CA_ROOTS` | environment | `native` for the OS store alone (so a root you distrusted there stays distrusted), `webpki` for the bundled roots alone. The default uses both |

A certificate that still can't be verified reports which roots were loaded and
how to add one:

```
error: request failed: … invalid peer certificate: UnknownIssuer
  the server's certificate was not issued by a trusted authority.
  Trust roots in use: 52 from the OS trust store, 157 bundled Mozilla roots.
  To trust a private CA, either install it in the OS trust store, or point RS2
  at the PEM file: --ca-file <path> (or RS2_CA_FILE=<path>), …
```

### Proxies

An explicit forward proxy is read from `ALL_PROXY`, `HTTPS_PROXY`, or
`HTTP_PROXY` (either case), with `NO_PROXY` honoured as a comma-separated list
of hosts and domain suffixes, or `*` for all. Loopback hosts are never proxied,
so a global proxy setting doesn't cut off `rs2 dev` on localhost. Transparent
interception needs no configuration — only the CA above.

## `rsconfig.json`

The admin commands walk upward from the current directory to the nearest
`rsconfig.json`:

```json
{
  "host": "https://api.example.com",
  "login": { "email": "admin@example.com" },
  "auth": { "token": "<jwt>", "exp": 1799999999,
            "host": "https://api.example.com" },
  "caFile": "corp-root.pem"
}
```

`login` writes `auth`; other commands reuse it only while it is unexpired and
matches `host`. Prefer `RS2_PASSWORD` (or the command flag) to storing
`login.password` in this file. `send` and `service add` also work anonymously
when the server's access policy permits it, which is useful during first-node
bootstrap.

## `rs2 deploy` flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--name <n>` | — | The bundle name in the code store |
| `--server <url>` | rsconfig `host` + `/services`, else `http://127.0.0.1:3100/services` | The `services` API base |
| `--token <t>` | the token saved by `rs2 login` (when unexpired and issued for that host) | Bearer token for a protected `services` mount |
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

**Bootstrap a fresh node, then make a small change:**

```powershell
rs2 auth init --admin-email admin@example.com
rs2 service add notes.mount.json --path /notes
rs2 send /notes/welcome.txt --file .\welcome.txt
```

For a multi-file/versioned config change, use `rs2 pull`, edit the local `rs2/`
mirror, inspect `rs2 push --dry-run`, then push. The mirror contains config,
specs, and code pins — not data, site assets, or bundle bytes. For site
assets in bulk (a built SPA, a docs site), use `rs2 send <path> --dir <dir>`
instead (§4.3).

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
