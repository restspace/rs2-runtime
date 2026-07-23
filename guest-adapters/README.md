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

## Native listing pushdown (`features` export)

A data-adapter bundle may export `const features = ["list-records"]` alongside
its default export. The host reads the export when the resident runtime spawns;
with the feature advertised, projected dataset listings (`$select`/`$sort`/
`$take`/`$skip`) are forwarded to the bundle as
`GET /{dataset}/?$select=…&$sort=…&$take=…&$skip=…` and the bundle answers
`{"entries": [{"name": "<key>", "fields": {…projected…}}, …], "total": <n>}`.
Without the export, the host serves the same listings itself by key-walking the
adapter's `get`/`list_keys` (correct but O(dataset) per sorted page). The
services catalogue reports which path a mount is on as `listProjection`:
`"native"` or `"fallback"` (`"fallback"` until the lazy first spawn).

`mongo-data.js` advertises the feature and runs the listing as one `find` with
a projection/sort/skip/limit plus a `count`, appending the record key (`_id`)
ascending as the final sort key — the contract's key tiebreak. **Known
deviation** from the host fallback's pinned sort semantics: MongoDB collates
missing and null fields together (the contract says missing < null) and has its
own cross-type bracketing. The listing contract only guarantees homogeneous
scalar sort fields, so avoid sorting on fields that mix types or hold explicit
nulls when this adapter backs the mount.

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
