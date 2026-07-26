# CMS roadmap — outstanding items

The goal: configure the runtime and rs2-ui so they present a view of a site
that is close to as usable as a CMS front end for non-technical users. This
doc tracks the improvement list from the July 2026 CMS survey of both repos.

**Already done (for context, not part of the open list):**

- **Item 2 — collection list views**: the runtime projected-listing contract
  (`$select`/`$sort` with pinned binary-UTF-8 collation, native pushdown for
  mem + mongo, `list-projection` and `meta-sort` facets), the UI List panel
  with schema-derived columns, and the sidebar sibling drill-in.
- **Item 1 — editor role scoping** (2026-07-24): the catalogue's
  permission-filtered `control` block now gates every engineer surface — gear,
  Send, Raw, Code — so a content-only role sees no admin chrome (rs2-ui
  `1c2f6b4`).
- **Items 3–6 — authoring quick wins** (2026-07-25, rs2-ui `8434fef`):
  markdown editing (`format: "markdown"` / `x-editor`), the image picker
  (`x-media-mount`), live page preview beside the form (`x-preview` schema-root
  mapping, GET template or POST-self for unsaved edits), and plain-language
  errors (`friendlyError` mapping 412/422/409 into editor language). All hints
  ride the resource's **JSON Schema** — feature-detected, never keyed off a
  service name.

## The central enabler: Editor mode, derived — not a manifest

The original plan here was a per-tenant "editorial manifest" — a config
document describing the editor's view of the site. That plan is **dropped**:
RS2 is more generic than a CMS, and a `cms` config block would describe the
site to one particular UI only. Instead, Editor mode is a **derived
projection** of surfaces the runtime already serves, with the few irreducible
declarations living in the data's own JSON Schema (the pattern items 3–6
established). Everything a polymorphic client — rs2-ui, an AI agent on the
agent surface — needs, it gets from generic contracts.

The derivation rules:

- **Collections** = store-pattern mounts (and datasets one level below data
  mounts) visible in the caller's permission-filtered
  `/.well-known/rs2/services`, minus spec stores (self-identifying via
  `specSubtree`) and control surfaces. Roles do the pruning; `x-expose` is the
  generic per-mount opt-out for anything left over.
- **Labels** from the mount's `description` metadata, falling back to a
  humanized path segment; **icons** from pattern/facets/content-type; **rail
  order** from mount declaration order in the tenant config.
- **Columns and default sort**: already schema-derived (`lib/columns.ts`). An
  explicit `x-columns` schema-root override stays unbuilt until demanded.
- **Field editors, preview mapping**: the shipped schema annotations
  (`x-editor`, `x-media-mount`, `x-preview`).
- **Media mount**: `x-media-mount` where declared; otherwise feature-detect —
  the file mount decorated by `code:image`, else the only writable file mount.
- **Mode selection**: no `control` block visible → Editor mode (flat labelled
  collections rail); operators who see `control` get a toggle back to the full
  explorer (Admin mode). Zero config.

Runtime side: approximately nothing — optionally extend `x-expose` filtering
(today applied to the agent surface) to the services catalogue so
`?surface=editor` prunes the rail, and document the schema annotation
vocabulary in the manual + rs2-skill as a generic client contract rather than
a UI-private convention.

## Open items

### 7. Draft/publish and revisions (runtime — the biggest gap)

"Version" in RS2 today is an ETag, not history. Stage it:

1. Start as a *store convention*: a `status` field pattern plus a publish
   pipeline that copies draft → live — works with today's runtime.
2. Then proper runtime support: a revision-retaining store decorator (fits the
   existing `PrefixedFileStore`-style composition pattern) with
   list-revisions / restore endpoints the UI can hang "History" and "Publish"
   buttons on.

### 8. Media: multipart upload + image transforms (runtime)

Multipart form parsing is explicitly out of scope in v1 and there are no
resize/thumbnail transforms anywhere. A resize-on-query-param image service
(cached, decorating a file mount) unlocks thumbnails for the asset library
(item 4) and responsive images for templates.

**Transforms shipped 2026-07-26** as a sandboxed Wasm component rather than a
built-in (`guest-services/image` + the `store` grant and `x-rs2-body-ref`
splice in `services/code.rs`) — the runtime stays image-free; tenants mount
`code:image@<v>` decorating their media mount. Multipart upload remains open.

### 9. Change events / webhooks (runtime)

No outbound events exist on content writes. Needed eventually for cache
purge, static site rebuilds, and search indexing. A host-level write-event
hook at the `Runtime::dispatch` choke point fits the "cross-cutting concerns
live in the host" rule in `docs/agents/architecture.md`.

### 10. Full-text search (runtime — can wait)

Stored queries cover structured filtering; full-text can arrive later as a
service fed by the event hook from item 9.

## Related runtime work (not in the original ten)

- **Stage 2 content projection**: extend `$select` beyond data stores to
  file-pattern stores by reading + projecting file content (JSON/front-matter)
  server-side, so file-backed collections get real columns too. Stage 1
  (metadata sort) shipped; stage 2 is designed but not built.

## Suggested sequence

Editor mode navigation next — with the manifest dropped there is no schema to
design, version, or migrate, so it is pure UI derivation work plus the small
`x-expose` catalogue-filtering change. Then draft/publish (7), the first big
runtime investment; multipart upload (the open half of 8) second. Items 9–10
are infrastructure that can follow demand.
