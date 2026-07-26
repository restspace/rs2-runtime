# CMS roadmap — outstanding items

The goal: configure the runtime and rs2-ui so they present a view of a site
that is close to as usable as a CMS front end for non-technical users. This
doc tracks the improvement list from the July 2026 CMS survey of both repos.

**Already done (for context, not part of the open list):** collection list
views — the runtime projected-listing contract (`$select`/`$sort` with pinned
binary-UTF-8 collation, native pushdown for mem + mongo, `list-projection` and
`meta-sort` facets), the UI List panel with schema-derived columns, and the
sidebar sibling drill-in. That was item 2 of the original ten. Item 1 (editor
role scoping in the UI) shipped 2026-07-24: the catalogue's permission-filtered
`control` block now gates every engineer surface — gear, Send, Raw, Code — so
a content-only role sees no admin chrome (rs2-ui `1c2f6b4`).

## The central enabler: a CMS manifest + Editor mode

Everything below hangs off one idea: a per-tenant **editorial manifest** —
config describing the *editor's* view of the site, distinct from the
engineer's view. Natural home: mount metadata in the catalogue (alongside the
existing `x-agent`/`x-expose` blocks), perhaps plus a tenant-level `cms`
section, served through the already permission-filtered
`/.well-known/rs2/services`. It declares:

- **Collections**: label ("Blog posts"), icon, backing mount/dataset, list
  columns, default sort. (The UI's `lib/columns.ts` column-plan heuristics are
  deliberately the seam a manifest overrides.)
- **Field editor hints**: this string field is markdown/rich text, this one is
  an image reference into the media mount, this one is a slug derived from the
  title.
- **Preview mapping**: how a record maps to its rendered page URL (the
  pipeline/template that serves it).
- **A designated media mount** to treat as the asset library.

The UI reads the manifest and renders a content-first **Editor mode**: a flat
rail of labelled collections ("Pages", "Posts", "Media") instead of the mount
tree, with the full explorer remaining as Admin mode. When no manifest is
present, the UI falls back to today's explorer. Mode selection should be
role-driven by default with a toggle for operators, so one deployment serves
both audiences.

## Open items

### 3. Markdown / rich-text editing (quick win, UI only)

The Form tab renders plaintext for every string field. Honor
`"format": "markdown"` (or an `x-editor` annotation) in schemas with a proper
editor (TipTap/Milkdown, or CodeMirror with preview), and add markdown
rendering to the Preview tab. The single biggest authoring-experience gap in
the UI.

### 4. Image picker field widget (quick win, UI only)

A schema-form widget for image-URL fields that opens a browse/upload dialog
against the media mount, with thumbnails. Keyless POST upload already works;
it just needs a friendly surface. (Thumbnails want item 8 for quality, but a
first version can show full images scaled down.)

### 5. Live page preview (quick win, UI only)

The template-authoring panel already does server-rendered iframe previews —
reuse that machinery on the *content* side: with the manifest's preview
mapping, show the rendered page beside the form as the editor types.

### 6. Plain-language errors (quick win, UI only)

Map 412/422/409 into editor language ("Someone else saved this page — reload
or overwrite?", field-level validation messages) instead of surfacing status
codes and ETag vocabulary.

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

Manifest schema + Editor mode navigation first — it reframes the whole
experience for roughly a week or two of UI work. Then markdown + image picker
+ preview (3–5), which makes authoring genuinely pleasant. Draft/publish (7)
is the first big runtime investment; media transforms (8) second. Items 9–10
are infrastructure that can follow demand.
