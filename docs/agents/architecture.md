# Architecture — how to think about changes

The whole codebase has one throughline: **cross-cutting concerns live in the
host; variants are config, not forks; reuse is type composition.** Most "where
does this go?" questions resolve by applying it.

## Capability model (PRD §9.2)

Services reach infrastructure only through capability traits
(`src/capabilities/mod.rs`: `FileStore`, `DataStore`, `QueryStore`, `HttpOut`;
plus `idempotency::IdempotencyStore` and `logging::LogStore`). The **host
constructs pre-scoped handles** and hands them to a service via
`ServiceContext` — a service never picks its tenant or names a raw path:

- `ScopedFileStore::new(adapter, tenant)` binds the tenant prefix.
- `PrefixedFileStore::new(inner, root)` roots every call under a prefix.
- `ScopedFileStore::prefixed(prefix)` composes both (tenant + subtree).

`ServiceContext` fields **are the grants**: `Some(handle)` = granted,
`None` = the capability never exists for that mount (default-deny). Sandboxed
code gets string-keyed grants through `GrantedHost` (`src/contract/mod.rs`),
also default-deny — an ungranted capability name fails `capability_denied`.

Adding a capability: define the trait in `capabilities/` (or a sibling module
like `logging`), give `Adapters` a field + a `with_*` builder
(`src/tenant.rs`), thread it into `ServiceContext`, and grant it per mount in
`Tenant::build`. Default to a no-op/None so embedders and tests opt in.

## Host choke points

`Runtime::dispatch` and `Runtime::handle` (`src/runtime.rs`) are the single
points every request passes through (internal pipeline calls route back through
`handle` too). CORS, universal caching, idempotency replay, the per-tenant
breaker, concurrency admission, and boundary logging all live here — **not** in
services. To add behavior that should apply to every response (a header, a log,
a policy), decorate at the choke point, not in each service.

Pitfall: anything you compute here runs on the hot path. Gate optional work
behind a cheap check (e.g. `LogStore::enabled()` skips building a record when
no sink is configured) so the G1 budget holds. See `testing.md`.

## The store pattern (client polymorphism)

`file`, `data`, the spec stores, and the code store all obey **one normative
contract** pinned by `tests/store_conformance.rs`: trailing-slash `dir+json`
listings at every container level (`{path, entries:[{name,dir,…}], total}` +
`X-Total-Count`), PUT upsert (201/200, ETag), keyless POST → 201 + `Location`,
the `409 + ?confirm=<name>` non-empty-container guard. Differences are declared
**facets** on the discovery surface (`range`, `patch`, `schema`, `echo`,
`confirm-delete`, `content-addressed`, `static-site`, …), feature-detected by
clients — never special-cased by service name. A new store-shaped surface joins
by passing the conformance suite and declaring facets; it does not invent a new
shape.

## Spec stores (reuse by ownership)

`SpecStore` (`src/services/spec_store.rs`) is the model for reuse: it **owns a
real `FileService` + a private `ServiceContext`** (a `PrefixedFileStore` over
`.rs2-pipelines{base}` / `.rs2-queries{base}`, no other caps) and delegates —
validate → canonicalize → forward. The store contract is implemented once, in
`FileService`; pipeline/query authoring inherit it rather than re-implementing.
Compose private services through the type system (owning + decorators), not
through config.

Authoring uses a reserved dot-subtree (`/<mount>/.pipelines/`, `/.queries/`);
every other path on **any verb** executes the longest-prefix-matched spec
(`.root` governs the mount root — verb-passthrough wrapping). This replaced
v1's modal `X-Restspace-Request-Mode` header.

## Variants are config, not new services

Before adding a service, ask whether it's a mode of an existing one:

- **static-site** = `file` config (`defaultResource`/`spaFallback`/`listings`).
- **caching** = per-mount `caching` config applied host-side.
- Named infra (S3, SQL) = an adapter selected by config (`"store": {...}`), not
  a service fork. `PrefixedFileStore` is the seam where named-infra plugs in.

## Adding a prebuilt service

1. Implement `Service` (`async fn handle(&self, msg, ctx) -> Result<Message>`).
2. `mod` + `pub use` in `src/services/mod.rs`.
3. Register the name in the `Tenant::build` match (`src/tenant.rs`); grant the
   capabilities it needs in the `ServiceContext` there.
4. Add a `pattern_of` arm + agent-surface/OpenAPI items in `src/discovery.rs`.
5. If it's store-shaped, hold it to `tests/store_conformance.rs`.
6. Document it in the skill (the `rs2-skill` repo's `references/services.md`).

## Instruction plane vs. data

A tenant's **instruction plane** is exactly: tenant config + `.rs2-code/` +
`.rs2-pipelines/` + `.rs2-queries/` (well-known file-store prefixes);
everything else is data/assets. This separation is deliberate — it is the basis
for the future `rs2 pull/push` git workflow (git never runs in-process). Don't
mix operational/runtime state into these prefixes.

## Embedding (PRD P2)

`rs2-core` has **no global state**. Embedders supply adapter trait impls and run
services in-process. Heavy dependencies are feature-gated (`wasm`, `js`,
`http`); the default build is the native engine only. Keep it that way: new
heavy deps go behind a feature.
