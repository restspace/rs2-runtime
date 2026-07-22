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
  "catalogues": [ … ],     // optional — external service/adapter catalogues
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
| `catalogues` | no | Named external catalogue URLs; usable only when the operator allowlists their hosts (8.11, 10.7) |
| `secrets` | no | Write-only block (10.6) |
| `operatorRoles` | no | Space-separated roles that confer **operator** status — the only principals who may change authorization (mount/spec `access`, role assignment). Absent ⇒ no API operator (5.0) |

## `auth`

```json
"auth": { "jwtSecret": "…", "sessionMinutes": 60, "maxAttempts": 5,
          "lockMinutes": 10, "userDataset": "users", "allowedLoginOrigins": [],
          "jwtUserProps": ["accountId"] }
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
{ "path": "/data", "service": "data",
  "config": { "access": { "read": "all", "write": "A" }, … } }
```

| Field | Notes |
| --- | --- |
| `path` | URL prefix; longest-prefix routing on segment boundaries (3.2). Duplicates rejected |
| `service` | `file` \| `data` \| `pipeline` \| `wrapper` \| `query` \| `template` \| `auth` \| `proxy` \| `sms` \| `services` \| `log` \| `code:<name>@<version>` |
| `config` | Service-specific + standard keys (below) |

`access` is operationally essential: omitting it makes the mount unreachable
(fail closed). Within an access object, omitted action keys follow the defaults
in 5.4.

## Standard config keys (any mount)

| Key | Purpose | Section |
| --- | --- | --- |
| `access` | Role spec: `read`/`write`/`delete`/`invoke` (or `"open"`/`"authenticated"`) | 5.4 |
| `elevate` | (pipeline mounts) role an `elevate` step adds to the caller — operator-set, not an operator role | 5.0, 7.3 |
| `retry` | Retry policy for calls this mount makes | 7.7 |
| `caching` | `mode`/`maxAgeSeconds`/`public`/`immutable` | 9.4 |
| `schedule` | Exactly one of `{every: "60s"}` or `{cron: "0 9 * * *"}` (UTC) | 7.11 |
| `secrets` | Names from the top-level write-only secrets block this mount may use | 7.11, 10.6 |
| `grants` | Capability grants: `{prefix}`, `{type:"httpOut",hosts}`, `{type:"socket",hosts}` — code mounts (8.5); `httpOut` also on pipeline/wrapper mounts for external `call` steps (7.3) | 8.5 |
| `x-agent`, `x-policy`, `x-expose`, `x-render`, `x-context`, `description` | Agent-surface metadata | 9.1 |

## Per-service config highlights

| Service | Notable config |
| --- | --- |
| `file` | `defaultResource`, `spaFallback`, `listings`, `friendlyUrls`, `extensionPriority` (static-site, 4.3); `store: {adapter}` for a loadable backend (8.9) |
| `data` | `enforceSchema` (4.4); `fieldLevelAuthz` — per-field `x-rs-read`/`x-rs-write` schema rules (4.5); `store: {adapter}` for a loadable backend (8.9) |
| `pipeline` | `retry`, `store: {root}`, or `specStore: {adapter, root}` for authoring storage (7.1, 10.7); `grants` (`httpOut`) for external calls (7.3) |
| `wrapper` | Inline `pipeline`; declared `pattern`/`facets`; enforced `inputSchema`, advisory `outputSchema`; optional `elevate` (7.10) |
| `query` | `store: {adapter, root}` selects execution backend/legacy spec root; `specStore` selects authoring storage (6.1, 8.9, 10.7) |
| `template` | `store: {root}` or `specStore` for compiled template envelopes; requires JS (6.6) |
| `auth` | usually just `access`; tenant-level `auth` holds the real settings (5.1) |
| `proxy` | Fixed external `target` plus host-side credential `inject` (8.10) |
| `sms` | Provider under `store.adapter` (`code:` or `infra:`); no built-in provider (8.10) |
| `services` | `access` with `write: "A"`; config changes are operator-only (5.0, 10.4) |
| `log` | `access` (10.3) |
| `code:<name>@<version>` | `grants`, `access`, plus JS `bodyPassthrough` / `requestStreaming` / `responseStreaming` (8.2, 8.5) |

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
| `adapter` | `builtin:<name>`, `code:<name>@<version>`, or `infra:<name>` (4.7, 8.9, 10.7) |
| `grants` | The adapter's capabilities — typically just its `socket` grant |
| `maxRuntimes` | Max warm runtimes per mount (resident pool); grows lazily under concurrency |
| `idleMs` / `idleSeconds` | Idle-eviction window; `0` keeps runtimes resident indefinitely |
| *(other keys)* | The whole block is handed to the bundle as `ctx.config` (connection params, secrets) |

---

← [Appendix A — Glossary](A-glossary.md) · [Manual home](../README.md) · [Next: Appendix C — Error code reference →](C-error-code-reference.md)
