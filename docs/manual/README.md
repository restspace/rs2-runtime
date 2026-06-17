# Restspace v2 — User Manual

Comprehensive documentation for building on the Restspace v2 (RS2) runtime.

**Who this is for.** Anyone comfortable with HTTP, JSON, and a little
JavaScript — roughly a mid-range front-end developer — who wants to stand up a
backend out of composable services without writing a traditional server. No
Rust or systems-programming background is assumed. Each part builds on the one
before, so a first read works front-to-back; later it doubles as a reference.

---

## Table of Contents

### Part 1 — Orientation

- [1.1 What Restspace v2 is](part-1-orientation/1.1-what-is-restspace-v2.md) (services as functions on HTTP messages)
- [1.2 The mental model](part-1-orientation/1.2-the-mental-model.md): tenants, mounts, services, pipelines
- [1.3 What you can build with it](part-1-orientation/1.3-what-you-can-build.md) (and what it deliberately leaves out)
- [1.4 How this manual is organized & prerequisites](part-1-orientation/1.4-how-this-manual-is-organized.md)

### Part 2 — Getting Started

- [2.1 Installing the runtime and the `rs2` CLI](part-2-getting-started/2.1-installing.md)
- [2.2 Running your first node (`serverConfig.json`)](part-2-getting-started/2.2-running-your-first-node.md)
- [2.3 Writing a minimal tenant config](part-2-getting-started/2.3-minimal-tenant-config.md)
- [2.4 Making your first request (and reading the response)](part-2-getting-started/2.4-your-first-request.md)
- [2.5 Health checks and the developer loop](part-2-getting-started/2.5-health-checks.md)

### Part 3 — Core Concepts

- [3.1 Messages and bodies](part-3-core-concepts/3.1-messages-and-bodies.md) (bytes vs. streams, media types, provenance)
- [3.2 Tenants and mounts](part-3-core-concepts/3.2-tenants-and-mounts.md) (longest-prefix routing, path safety)
- [3.3 The store pattern](part-3-core-concepts/3.3-the-store-pattern.md): one client codepath for every store
- [3.4 Patterns vs. facets](part-3-core-concepts/3.4-patterns-and-facets.md) (feature-detection over special-casing)
- [3.5 Capabilities and the sandbox](part-3-core-concepts/3.5-capabilities-and-the-sandbox.md) (default-deny)
- [3.6 The instruction plane](part-3-core-concepts/3.6-the-instruction-plane.md): what fully describes a tenant

### Part 4 — Storing Files and Data

- [4.1 The `file` service: streamed reads and writes](part-4-storing-files-and-data/4.1-the-file-service.md)
- [4.2 Ranges, ETags, conditional GETs, and HEAD](part-4-storing-files-and-data/4.2-ranges-etags-conditional.md)
- [4.3 Static-site & SPA hosting from a file mount](part-4-storing-files-and-data/4.3-static-site-and-spa.md)
- [4.4 The `data` service: schema-validated JSON records](part-4-storing-files-and-data/4.4-the-data-service.md)
- [4.5 Schemas, validation errors, and JSON merge PATCH](part-4-storing-files-and-data/4.5-schemas-validation-patch.md)
- [4.6 Listings, pagination, and the `?confirm=` delete guard](part-4-storing-files-and-data/4.6-listings-pagination-delete.md)

### Part 5 — Authentication & Access Control

- [5.0 The RS2 security model](part-5-auth-and-access-control/5.0-security-model.md) (the three tiers, the authority invariant)
- [5.1 The `auth` service: login, refresh, logout, current user](part-5-auth-and-access-control/5.1-the-auth-service.md)
- [5.2 Users, password hashes, and roles](part-5-auth-and-access-control/5.2-users-and-roles.md)
- [5.3 Tokens: JWTs, cookies, and bearer headers](part-5-auth-and-access-control/5.3-tokens-cookies-bearer.md)
- [5.4 Role specs on mounts](part-5-auth-and-access-control/5.4-role-specs-on-mounts.md) (read/write/delete/invoke, path-scoped grants, per-spec access)
- [5.5 CORS, trusted vs. allowed origins, and the CSRF guard](part-5-auth-and-access-control/5.5-cors-and-csrf.md)

### Part 6 — Querying Data

- [6.1 The `query` service: stored queries authored like files](part-6-querying-data/6.1-the-query-service.md)
- [6.2 Parameters: URL segments, query strings, and request bodies](part-6-querying-data/6.2-parameters.md)
- [6.3 JSON templates (structural, injection-safe substitution)](part-6-querying-data/6.3-json-templates.md)
- [6.4 String/SQL templates and bind parameters](part-6-querying-data/6.4-string-sql-templates.md)
- [6.5 Param schemas, defaults, and validation](part-6-querying-data/6.5-param-schemas.md)

### Part 7 — Composing with Pipelines

- [7.1 The `pipeline` service: authored like files, run on any verb](part-7-pipelines/7.1-the-pipeline-service.md)
- [7.2 The typed spec and the string DSL (two inputs, one stored form)](part-7-pipelines/7.2-typed-spec-and-dsl.md)
- [7.3 Step kinds: call, transform, pipeline, split](part-7-pipelines/7.3-step-kinds.md)
- [7.4 Modes: serial, parallel, conditional, tee/teeWait](part-7-pipelines/7.4-modes.md)
- [7.5 Conditions, `${...}` interpolation, and captured variables](part-7-pipelines/7.5-conditions-interpolation-variables.md)
- [7.6 Transforms with JSONata](part-7-pipelines/7.6-transforms-jsonata.md)
- [7.7 Retries, effect classes, and idempotency](part-7-pipelines/7.7-retries-effects-idempotency.md)
- [7.8 Segments and the `?$plan` introspection](part-7-pipelines/7.8-segments-and-plan.md)
- [7.9 Debugging a pipeline](part-7-pipelines/7.9-debugging-a-pipeline.md)

### Part 8 — Custom Services (Your Own Code)

- [8.1 When to reach for custom code](part-8-custom-services/8.1-when-to-use-custom-code.md) (vs. a pipeline or query)
- [8.2 The JavaScript service contract](part-8-custom-services/8.2-the-js-contract.md) (`handle`, `msg`, `ctx`)
- [8.3 The supported API surface (the npm-compat prelude)](part-8-custom-services/8.3-supported-api-surface.md)
- [8.4 The WebAssembly service contract](part-8-custom-services/8.4-the-wasm-contract.md) (Rust components against the WIT world)
- [8.5 Capability grants: internal prefixes and outbound HTTP](part-8-custom-services/8.5-capability-grants.md)
- [8.6 Building, bundling, and deploying (`rs2 deploy`)](part-8-custom-services/8.6-building-bundling-deploying.md)
- [8.7 Versioning, rollback, and mounting `code:` refs](part-8-custom-services/8.7-versioning-and-mounting.md)
- [8.8 Limits inside the sandbox and diagnosing failures](part-8-custom-services/8.8-limits-and-diagnosing.md)
- [8.9 Loadable storage adapters (bring your own backend)](part-8-custom-services/8.9-loadable-storage-adapters.md)

### Part 9 — The HTTP API & Cross-Cutting Behavior

- [9.1 The discovery surface (`/.well-known/rs2/*`) and OpenAPI](part-9-http-api-and-cross-cutting/9.1-discovery-surface.md)
- [9.2 Structured errors (`application/problem+json`) and error codes](part-9-http-api-and-cross-cutting/9.2-structured-errors.md)
- [9.3 Idempotency keys and replay](part-9-http-api-and-cross-cutting/9.3-idempotency-keys.md)
- [9.4 Caching: the universal `caching` config and conditional revalidation](part-9-http-api-and-cross-cutting/9.4-caching.md)
- [9.5 Limits, containment, and the per-tenant circuit breaker](part-9-http-api-and-cross-cutting/9.5-limits-and-containment.md)
- [9.6 Tracing: `X-Trace-Id` and correlating to logs](part-9-http-api-and-cross-cutting/9.6-tracing-and-correlation.md)

### Part 10 — Operating a Node

- [10.1 Server config in depth](part-10-operating-a-node/10.1-server-config.md) (listener, file root, tenants dir)
- [10.2 Single- vs. multi-tenant deployments](part-10-operating-a-node/10.2-single-vs-multi-tenant.md) (domain maps, subdomains)
- [10.3 Logging & observability](part-10-operating-a-node/10.3-logging-and-observability.md) (the `log` service, sinks, severities)
- [10.4 Editing config safely](part-10-operating-a-node/10.4-editing-config-safely.md) (`PUT /services/raw`, dry-build, hot-swap)
- [10.5 Deploying and rotating custom code on a running node](part-10-operating-a-node/10.5-deploying-and-rotating-code.md)
- [10.6 Secrets handling (write-only config fields)](part-10-operating-a-node/10.6-secrets-handling.md)

### Part 11 — Migrating from v1 Restspace

- [11.1 What `rs2 migrate` carries over](part-11-migrating-from-v1/11.1-what-migrate-carries-over.md)
- [11.2 What is warned, adjusted, or skipped](part-11-migrating-from-v1/11.2-warned-adjusted-skipped.md)
- [11.3 Post-migration checklist](part-11-migrating-from-v1/11.3-post-migration-checklist.md) (secrets, users, pipelines, queries)

### Appendices

- [A. Glossary of terms](appendices/A-glossary.md)
- [B. Tenant config reference (every key)](appendices/B-tenant-config-reference.md)
- [C. Error code reference](appendices/C-error-code-reference.md)
- [D. Default limits and how to change them](appendices/D-default-limits.md)
- [E. The `rs2` CLI command reference](appendices/E-cli-reference.md)
- [F. Further reading (PRD, architecture notes, the operator skill)](appendices/F-further-reading.md)
- [G. How the JavaScript (V8 isolate) engine works](appendices/G-v8-isolate-engine.md)
- [H. How the WebAssembly (Wasmtime) engine works](appendices/H-wasmtime-engine.md)
