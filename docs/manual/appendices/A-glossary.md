# Appendix A — Glossary

Quick definitions of the terms used throughout the manual. Section references
point to where each is introduced.

| Term | Meaning |
| --- | --- |
| **Node** | The running RS2 process; hosts one or more tenants. Configured by `serverConfig.json` (10.1) |
| **Tenant** | A self-contained, isolated application: its own mounts, data, users, config (1.2) |
| **Mount** | A service placed at a URL prefix within a tenant: `{path, service, config}` (1.2, 2.3) |
| **Service** | A function on an HTTP message; prebuilt or custom (1.1) |
| **Message** | The HTTP-shaped value services act on: method, URL, headers, body, media type (3.1) |
| **Body** | A message's payload — materialized bytes or a stream (3.1) |
| **Provenance** | Whether a body can be re-read: materialized / replayable / ephemeral (3.1) |
| **Store** | A service that behaves as a tree of containers and resources under one contract (3.3) |
| **Container / Resource (child)** | A listable node (trailing slash) / an individual item in a store (3.3) |
| **Pattern** | A mount's conversation shape: `store`, `store-view`, `transform`, `api` (3.4) |
| **Facet** | An optional capability within a pattern (`range`, `schema`, `static-site`, …) (3.4) |
| **Capability** | A pre-scoped handle a service uses to reach infrastructure; default-deny (3.5) |
| **Grant** | A per-mount authorization of a capability — an internal prefix or outbound HTTP (8.5) |
| **Instruction plane** | What fully describes a tenant: config + `.rs2-code/` + `.rs2-pipelines/` + `.rs2-queries/` (3.6) |
| **Pipeline** | A declared multi-step flow composing service calls (Part 7) |
| **Step** | One unit of a pipeline: a call, transform, subpipeline, or split (7.3) |
| **In-flight message** | The value flowing through a pipeline, replaced or augmented by each step (7.3) |
| **Transform** | A step that reshapes the body with JSONata (7.6) |
| **Segment** | The atomic retry unit of a pipeline, bounded at materialization points (7.8) |
| **Effect class** | A call's retry-safety category: pure / idempotent / keyed / unsafe (7.7) |
| **Idempotency key** | A client- or runtime-supplied key that makes an operation at-most-once (9.3) |
| **Role spec** | A mount's `access` rules: read/write/create/manage roles (5.4) |
| **Principal** | An authenticated caller: `{id, roles, kind}` (5.1) |
| **Facet vs. service name** | Clients feature-detect facets; they never branch on the service name (3.4) |
| **Trace id** | A per-request id (`X-Trace-Id`) correlating responses, logs, and internal hops (9.6) |
| **Discovery surface** | The generated, permission-filtered `/.well-known/rs2/*` description of a tenant (9.1) |
| **Code ref** | An immutable, content-addressed custom-service version: `code:<name>@<version>` (8.7) |
| **Socket grant** | A capability letting custom JS open gated raw TCP/TLS connections for non-HTTP protocols (8.5) |
| **Loadable adapter** | Deployed code backing a `file`/`data`/`query` mount's persistence via the store pattern (8.9) |
| **Resident runtime** | A warm, pooled isolate kept alive between requests so an adapter's connections pool (8.9, App. G) |
| **Elevate** | A `call`-step flag that drops the caller's principal so a pipeline can mediate access to a locked mount (7.3) |

---

← [Part 11 — Migrating from v1](../part-11-migrating-from-v1/11.3-post-migration-checklist.md) · [Manual home](../README.md) · [Next: Appendix B — Tenant config reference →](B-tenant-config-reference.md)
