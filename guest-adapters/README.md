# guest-adapters — deployable JS adapter bundles

First-party loadable adapters (G13): single-file ESM bundles that back a
mount's persistence or query execution from inside the sandboxed JS engine,
speaking a real wire protocol over the gated socket capability. They are
checked in here as the source of truth and embedded into the contract tests
via `include_str!` (`rs2-core/tests/guest_adapter.rs`), so the shipped file is
exactly what the tests hold to the store contract.

## Bundles

| File | Backs | Protocol |
| --- | --- | --- |
| `mongo-data.js` | a `data` mount (`DataStore`) | MongoDB OP_MSG + hand-written BSON codec |
| `mongo-query.js` | a `query` mount (`QueryStore`) | MongoDB `aggregate` (one command, `$facet` paging) |

Both are **no-auth** (SCRAM-SHA-256 needs a prelude `crypto.subtle` — not yet
exposed), so they target unauthenticated or network-trusted MongoDB. There is
no build step: each bundle is self-contained, which is why the BSON/OP_MSG
wire section is duplicated between the two files — the marked sections must be
kept in sync by hand.

## Deploying

```
rs2 deploy guest-adapters/mongo-data.js --name mongo-data
```

uploads the bundle to the tenant's code store at
`.rs2-code/<name>/<version>.js`. Reference it from a mount's `store.adapter`
as `code:<name>@<version>`, with a socket grant for the backend and the
connection config alongside:

```json
{
  "path": "/data", "service": "data",
  "config": { "store": {
    "adapter": "code:mongo-data@v1",
    "host": "db.internal", "port": 27017, "db": "prod",
    "grants": { "mongo": { "type": "socket", "hosts": ["db.internal:27017"] } }
  }}
}
```

A `query` mount uses `code:mongo-query@v1` the same way; its stored queries
are envelopes whose `query` is `{ "collection": "...", "pipeline": [...] }`
(a MongoDB aggregation pipeline; `${param}` placeholders are substituted by
the query service before the adapter runs).
