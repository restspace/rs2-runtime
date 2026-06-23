# Appendix B — Tenant config reference

A consolidated reference for the tenant config document
(`tenants/<name>.json` or `PUT /services/raw`). Sections link to the full
explanation.

## Top-level keys

```json
{
  "auth":   { … },         // optional — enables token verification + the auth service
  "cors":   { … },         // optional — browser clients
  "retry":  { … },         // optional — tenant-default retry policy
  "secrets": { … },        // optional — write-only secret values
  "operatorRoles": "A",    // optional — roles that confer operator status
  "mounts": [ … ]          // required
}
```

| Key | Required | Notes |
| --- | --- | --- |
| `mounts` | yes | Array of mount objects (below) |
| `auth` | no | Presence + `jwtSecret` turns on auth (5.1) |
| `cors` | no | `trustedOrigins` / `allowedOrigins` (5.5) |
| `retry` | no | Default policy; overridden per mount/call (7.7) |
| `secrets` | no | Write-only block (10.6) |
| `operatorRoles` | no | Space-separated roles that confer **operator** status — the only principals who may change authorization (mount/spec `access`, role assignment). Absent ⇒ no API operator (5.0) |

## `auth`

```json
"auth": { "jwtSecret": "…", "sessionMinutes": 60, "maxAttempts": 5,
          "lockMinutes": 10, "userDataset": "users", "allowedLoginOrigins": [] }
```

See 5.1 (endpoints), 5.3 (sessions/lockout), 5.5 (`allowedLoginOrigins`).
`jwtSecret` reads back as `"<secret>"` (10.6).

## `cors`

```json
"cors": { "trustedOrigins": ["https://app.acme.com", "*.acme.dev"],
          "allowedOrigins": ["https://reader.example"] }
```

Trusted = credentialed CORS + cookie lane; allowed = bearer-only. See 5.5.

## A mount

```json
{ "path": "/data", "service": "data", "config": { … } }
```

| Field | Notes |
| --- | --- |
| `path` | URL prefix; longest-prefix routing on segment boundaries (3.2). Duplicates rejected |
| `service` | `file` \| `data` \| `pipeline` \| `query` \| `auth` \| `services` \| `log` \| `code:<name>@<version>` |
| `config` | Service-specific + standard keys (below) |

## Standard config keys (any mount)

| Key | Purpose | Section |
| --- | --- | --- |
| `access` | Role spec: `read`/`write`/`delete`/`invoke` (or `"open"`/`"authenticated"`) | 5.4 |
| `elevate` | (pipeline mounts) role an `elevate` step adds to the caller — operator-set, not an operator role | 5.0, 7.3 |
| `retry` | Retry policy for calls this mount makes | 7.7 |
| `caching` | `mode`/`maxAgeSeconds`/`public`/`immutable` | 9.4 |
| `grants` | Capability grants (custom-code mounts): `{prefix}`, `{type:"httpOut",hosts}`, `{type:"socket",hosts}` | 8.5 |
| `x-agent`, `x-policy`, `x-expose`, `x-render`, `x-context`, `description` | Agent-surface metadata | 9.1 |

## Per-service config highlights

| Service | Notable config |
| --- | --- |
| `file` | `defaultResource`, `spaFallback`, `listings`, `friendlyUrls`, `extensionPriority` (static-site, 4.3); `store: {adapter}` for a loadable backend (8.9) |
| `data` | `enforceSchema` (4.4); `fieldLevelAuthz` — per-field `x-rs-read`/`x-rs-write` schema rules (4.5); `store: {adapter}` for a loadable backend (8.9) |
| `pipeline` | `retry`, `store: {root}` (relocate spec storage) (7.1) |
| `query` | `store: {root}` (6.1); `store: {adapter}` for a loadable backend (8.9) |
| `auth` | usually just `access`; tenant-level `auth` holds the real settings (5.1) |
| `services` | `access` with `write: "A"`; config changes are operator-only (5.0, 10.4) |
| `log` | `access` (10.3) |
| `code:<name>@<version>` | `grants`, `access` (8.5) |

## Loadable adapter `store` block (`file` / `data` / `query`, 8.9)

```json
"store": {
  "adapter": "code:my-redis@v1",                 // required: the adapter code ref
  "grants": { "db": { "type": "socket",
                      "hosts": ["redis.internal:6379"] } },
  "maxRuntimes": 4,                               // optional, default 4 (1–64)
  "idleMs": 60000,                                // optional, default 60000; 0 disables eviction
  "host": "redis.internal", "port": 6379         // any extra keys are passed to the bundle as ctx.config
}
```

| Key | Notes |
| --- | --- |
| `adapter` | `code:<name>@<version>` of a deployed store-pattern bundle (JS engine only; `501` on a non-JS build) |
| `grants` | The adapter's capabilities — typically just its `socket` grant |
| `maxRuntimes` | Max warm runtimes per mount (resident pool); grows lazily under concurrency |
| `idleMs` / `idleSeconds` | Idle-eviction window; `0` keeps runtimes resident indefinitely |
| *(other keys)* | The whole block is handed to the bundle as `ctx.config` (connection params, secrets) |

---

← [Appendix A — Glossary](A-glossary.md) · [Manual home](../README.md) · [Next: Appendix C — Error code reference →](C-error-code-reference.md)
