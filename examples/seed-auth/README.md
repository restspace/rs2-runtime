# Seed user auth on a fresh rs2 instance — over HTTP

A worked example that turns a **fresh, no-auth** rs2 node into one with working
authentication, a schema-enforced user store with field-level authorization,
and a `/users` management pipeline that **hashes passwords on write** — driven
entirely by the `rs2` CLI, with no startup-seeded admin and no node restart.

The node starts with *no auth at all* — just an open `/services` mount. The
script injects the signing secret, creates the first operator over HTTP, stands
up the user store + login pipeline, then locks everything down.

## What it builds

Running `rs2 run seed-auth.rs2` performs these steps in order:

1. `auth enable` — generate a `jwtSecret`, set `operatorRoles: A`, mount `/auth`,
   and open a **temporary** write-open, schema-free user-store `/data` mount.
2. `auth create-admin` — the CLI hashes the password locally (argon2id) and
   writes `{ passwordHash, roles: "A", kind: "user" }` straight to the user
   store while it's still open. (Password from `rsconfig.json`'s `login.password`
   or `RS2_ADMIN_PASSWORD`.)
3. `login` — authenticate as the new admin. **Everything after is operator-gated.**
4. `send /data/users/.schema.json` — install the `users` schema (field authz).
5. `service add users.pipeline.mount.json` — mount the `/users` pipeline.
6. `send /users/.pipelines/.root` — install the password-hashing pipeline spec.
7. `service set-access` — lock down: the user store becomes operator-write with
   schema + field-authz enforced, and `/services` becomes operator-only.

The equivalent one-liner for the auth core (without the `/users` pipeline) is:

```sh
rs2 auth init --admin-email admin@demo.local --admin-password demo-admin-pw
```

### The pieces

| File | Role |
| --- | --- |
| `serverConfig.json` | Demo node config — **no** bootstrap admin, no auth |
| `tenants/main.json` | Starting tenant: a single **open** `/services` mount, no auth |
| `rsconfig.json` | CLI config: server URL + the admin login to create |
| `users.schema.json` | `users` dataset schema with per-field access rules |
| `users.pipeline.mount.json` | `/users` pipeline mount |
| `users.root.pipeline.json` | `.root` spec: GET reads, PUT hashes then writes |
| `sample-user.json` | Example body for creating a user |
| `seed-auth.rs2` | The `rs2 run` script tying it together |

The `/auth` and user-store `/data` mounts aren't files here — `auth enable`
creates them, and the lockdown step tightens `/data`.

### Field-level authorization (`users.schema.json`)

```json
"passwordHash": { "x-rs-read": "A", "x-rs-write": "A" },
"roles":        { "x-rs-write": "A" }
```

- **`passwordHash`** is operator-only to read *and* write — so a logged-in
  non-admin reading a user record never sees the hash, and only an operator can
  set it (the pipeline runs the write as the admin).
- **`roles`** is operator-only to write — a user can read their roles but can
  never self-promote.

### The password-hashing pipeline (`users.root.pipeline.json`)

`.root` governs the whole `/users` mount on every verb. It forwards the
addressed key with the `${url.path[0]}` URL-pattern (the first peeled path
segment):

- `GET /users/` → lists the store (`GET /data/users/`).
- `GET /users/<email>` → `GET /data/users/${url.path[0]}`.
- `PUT /users/<email>` with `{ "password": "…", "roles"?: "…" }` → a transform
  replaces `password` with `passwordHash` via `$hashPassword(...)` (argon2id),
  then `PUT /data/users/${url.path[0]}`.

Permissions on the spec: `read` = any authenticated principal, `write`/`invoke`
= admin (`A`). Plaintext passwords never reach the store.

## Run it

From this directory (requires a build of the `rs2` CLI — see the repo README;
e.g. `cargo run -p rs2-cli --`):

```sh
# 1. Start the node (leave it running):
rs2 dev serverConfig.json

# 2. In another shell, from this directory, seed auth:
rs2 run seed-auth.rs2

# 3. Create a user (PUT through the pipeline → hashed on write):
rs2 send /users/ada@example.com --file sample-user.json

# 4. Log in as that user to confirm it works:
#    POST /auth/login { "email": "ada@example.com", "password": "ada-secret-pw" }

# 5. Read it back (the CLI sends PUTs only; use curl for GET). The admin token
#    is saved in rsconfig.json under auth.token:
curl -H "Authorization: Bearer <token-from-rsconfig.json>" \
     http://127.0.0.1:3100/users/ada@example.com
```

As admin you'll see `passwordHash` in the read; a non-admin authenticated reader
gets it redacted by the field-level rules.

To confirm the lockdown took, an **anonymous** `PUT /services/raw` should now be
rejected (401/403) — the open bootstrap window is closed.

## Security notes (for real use)

- **The bootstrap window.** Between steps 1–3 the user-store `/data` mount is
  briefly write-open to anonymous callers — unavoidable for a pure-HTTP bootstrap,
  since the very first operator can't be created by an operator. The example binds
  to `127.0.0.1` and the script locks down immediately after login. Don't run the
  bootstrap against a publicly reachable address.
- **Credentials are in plain files.** `rsconfig.json` carries a demo password. In
  practice omit `login.password` and supply it via `RS2_ADMIN_PASSWORD` /
  `RS2_PASSWORD` / `rs2 auth create-admin --password`.
- **The `jwtSecret` is generated** by `auth enable` and stored in the tenant
  config (masked on `GET /services/raw`). Re-running `auth enable` keeps the
  existing secret — it never rotates live sessions.
