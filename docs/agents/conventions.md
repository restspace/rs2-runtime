# Conventions — editing, commits, workflow

## Editing Rust sources — use the Edit/Write tools, never PowerShell text rewrites

**Never** bulk-edit `.rs` files with PowerShell `-replace` / `Set-Content` /
`Out-File`. Windows PowerShell 5.1 re-encodes on write and **mojibakes UTF-8** —
`§`, em-dashes (`—`), and other non-ASCII in comments come back as `Â§` / `???`.
This has bitten the project twice (M1, and the static-site change). Always use
the `Edit` or `Write` tools, which preserve UTF-8.

If a file is already mojibaked, fix the specific characters with `Edit` (search
for `Â`, `???`, stray `?` at line starts). Don't "repair" by re-running a
PowerShell replace.

When writing comments, keep the existing density and idiom of the surrounding
file. Comments explain **why**, not what. Match naming and structure of nearby
code — new code should read like it was already there.

## Commits

PowerShell mangles embedded quotes in `git commit -m "…"` here-strings. Commit
through a file:

```powershell
# write the message to .git\COMMIT_MSG_TMP with the Write tool, then:
git add -A
git commit -F .git\COMMIT_MSG_TMP
```

- End every commit message with the trailer:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- **Commit and push only when the user asks.** This repo has been worked
  directly on `main` (private, pushed via `gh` as the restspace user). Don't
  push speculatively.
- Be aware `git add -A` will also stage `.git\COMMIT_MSG_TMP` if it's inside the
  work tree — harmless, but worth noting.

LF/CRLF warnings from git on Windows are expected noise; ignore them.

## Dependencies

The default build is intentionally lean (native engine only). New heavy
dependencies go behind a Cargo feature (`wasm`, `js`, `http`, and future
`otlp`). Don't add a workspace dependency for a single call site you can write
by hand.

## Keep the skill in sync

`C:\dev\rs2-skill` is the user-facing skill (operating an RS2 server). When a
change adds or alters a config field, endpoint, header, or facet, update the
matching `references/*.md` (and `SKILL.md` if the service list or triggers
change) in the same pass. This repo's `README.md` tracks build/run/status.

## Memory

Durable project state lives at
`C:\Users\james\.claude\projects\C--dev-rs2-runtime\memory\rs2-m1-status.md`
(indexed from `MEMORY.md`). Update it when milestones, gotchas, or wasmtime/v8
API notes change — it's the cross-session source of truth for "what's built".
