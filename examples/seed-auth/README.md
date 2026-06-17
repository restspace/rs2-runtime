# Seed user auth on an rs2 instance

A worked example that turns a near-empty rs2 node into one with working
authentication, a schema-enforced user store with field-level authorization,
and a `/users` management pipeline that **hashes passwords on write** — driven
entirely by the `rs2` CLI (`login`, `send`, `service add`, `run`).

## What it builds

Running `rs2 run seed-auth.rs2` performs these steps in order:

1. `service add auth.mount.json` — mounts the `auth` service at `/auth` (login).
2. `login` — authenticates as the bootstrap admin (creds from `rsconfig.json`).
3. `service add userstore.mount.json` — mounts the user store at `/data`
   (`enforceSchema` + `fieldLevelAuthz`).
4. `send /data/users/.schema.json` — installs the `users` schema.
5. `service add users.pipeline.mount.json` — mounts the `/users` pipeline.
6. `send /users/.pipelines/.root` — installs the management pipeline spec.

### The pieces

| File | Role |
| --- | --- |
| `serverConfig.json` | Demo node config; seeds a bootstrap admin at startup |
| `tenants/main.json` | Starting tenant: `auth` settings + an (open) `/services` mount |
| `rsconfig.json` | CLI config: server URL + admin login |
| `auth.mount.json` | `/auth` mount (login endpoint open to all) |
| `userstore.mount.json` | `/data` mount — schema-enforced, field-level authz |
| `users.schema.json` | `users` dataset schema with per-field access rules |
| `users.pipeline.mount.json` | `/users` pipeline mount |
| `users.root.pipeline.json` | `.root` spec: GET reads, PUT hashes then writes |
| `sample-user.json` | Example body for creating a user |
| `seed-auth.rs2` | The `rs2 run` script tying it together |

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

## Demo-only shortcuts (harden for real use)

- **`/services` is open** (`write: "all"`) so step 1 can mount `/auth` before any
  admin exists. After seeding, lock it down — set
  `"access": { "read": "A", "write": "A" }` on the `/services` mount in
  `tenants/main.json` and restart the node. (`service add` only *adds* mounts; it
  can't modify an existing one.)
- **Credentials are in plain files.** `serverConfig.json` and `rsconfig.json`
  carry a demo password. In practice put the admin password in
  `RS2_ADMIN_PASSWORD` (node) and omit `login.password` from `rsconfig.json`,
  supplying it via `RS2_PASSWORD` or `rs2 login --password`.
- **`jwtSecret` is a placeholder** — replace it.
