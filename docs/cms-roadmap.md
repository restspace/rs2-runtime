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
- **Labels** humanize the mount path segment (or dataset name) — real
  `description` values are documentation prose, so they serve as the row
  tooltip, not the label; **icons** from pattern/facets/content-type; **rail
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

**Shipped 2026-07-26**: the runtime half in `8e96487` (`?surface=` on the
services catalogue, control block follows its mount; manual 4.8 documents the
annotation vocabulary; rs2-skill references updated) and the UI in rs2-ui
`7f8da23` (`lib/collections.ts` derivation, `lib/view-mode.ts` role-driven
mode + operator toggle, `sidebar/CollectionsRail.tsx`), verified in-browser
against a live node.

## Open items

### 7. Preview/publish — two parallel stores (config + UI, not runtime)

**Revisions are dropped.** The earlier plan here was a revision-retaining store
decorator with history/restore endpoints. In practice CMS editing does not need
version management, and having to think about it is worse than not having it:
ETag-as-version already gives conflict detection, which is the part editors
actually need, while history is a separate product with its own UI surface and
restore semantics. What is genuinely required is a **preview/publish
distinction**, and that is two parallel stores — no runtime work at all.

The shape:

- `/content` — the draft store; editors read/write, the Editor-mode collection.
- `/content-live` — the live twin, write locked to a `publisher` role, read by
  the public site and templates. Give it `"x-expose": []` so it does not appear
  as a second collection (`discovery.rs:142` — a mount *without* `x-expose`
  shows on every surface, so the twin needs the explicit opt-out).
- `/publish` — a **`wrapper`** mount (one fixed flow, so config not a stored
  spec — 7.10) with `elevate: "publisher"`, running
  `GET /content${url.rest}` → `PUT /content-live${url.rest}`. Editors publish
  without ever holding write access on live (the gateway pattern, 7.3).
  Unpublish is the same flow with DELETE. **Open `delete` alongside `invoke`**
  on it: access is gated by verb (`action_for`, `wrapper/mod.rs:407`), so
  `DELETE` needs the `delete` key, which otherwise defaults to `write` — with
  only `invoke` opened every unpublish is a 401.

**Publish state comes free from the ETag.** `record_etag` (`data.rs:103`) is a
content hash of the record, not a counter, so draft ETag == live ETag means
byte-identical content. Comparing the two yields draft / published /
**modified-since-publish** with no `status` field, no bookkeeping, and no way
for the state and the content to drift apart. Do not add a `status` field.

Preview needs nothing new: `x-preview` (shipped in items 3–6) resolves against
the draft mount while the public site reads live.

**The one real gap** is that the UI must present the pair as a *single*
collection carrying a Publish action, not two collections. That is an
irreducible declaration, so it rides the schema root like the rest of the
vocabulary — `x-publish: { "target": "/content-live", "via": "/publish" }`,
feature-detected. Keeping it on the schema (not in tenant config) keeps it a
generic contract: an agent on the agent surface can publish too, not just
rs2-ui.

**Verified end-to-end 2026-07-28** on a throwaway node against the mounts
above: schema enforced on the draft (422); anonymous direct write to the live
twin refused (401) while `POST /publish/...` succeeded through `elevate`;
ETags identical after publish, divergent after a draft edit, identical again
after re-publish; `DELETE /publish/...` removed the live record (204) and left
the draft intact; and `?surface=editor` pruned the twin from the catalogue
while the unfiltered catalogue still listed it. The `delete`-access trap above
was the one thing that did not work first time.

**Runtime half shipped 2026-07-28**: `x-publish` documented in manual 4.8
(vocabulary table + the state model + the mount recipe) and in the rs2-skill
`references/services.md`. Nothing else is owed by the runtime.

**UI half shipped 2026-07-28** (rs2-ui `e74c4c1`): a Status column in the List
panel and Publish/Unpublish controls above the record form, feature-detected
on `x-publish`. **Item 7 is done.**

One constraint worth recording, because it shaped the implementation and is
the obvious thing to "fix" later: the two surfaces derive state differently
because the runtime gives them different material. The record editor compares
**ETags** — exact, since a data ETag is a content hash. The list cannot:
`dir+json` listing entries carry **no ETag**, so it projects the schema's
declared fields from both mounts with `$select` and compares the projected
objects. That is exact for every declared field but would miss a change
confined to an *undeclared* one (closed schemas rule this out), and it costs a
content-bearing listing fetch, so badges are suppressed above a cap rather
than guessed.

The exact fix is a small runtime change — **project the record ETag into
listing entries** (as a `$select` pseudo-field, or unconditionally on data
listings) — after which the list compares ETags like the editor does and the
field projection goes away. Not worth doing until the approximation actually
bites: the cost is a full record read per entry, which is free on the host
fallback path but not on a native pushdown.

Assumptions taken (revisit if wrong): assets live in a **single** media mount
rather than getting their own draft/live split; **single-record** publish
first, bulk publish over a listing (via `split`) deferred; publishing a record
that links to unpublished records is **not** warned about in v1.

### 8. Media: image transforms (done) + multipart upload (not a CMS blocker)

**Transforms shipped 2026-07-26** as a sandboxed Wasm component rather than a
built-in (`guest-services/image` + the `store` grant and `x-rs2-body-ref`
splice in `services/code.rs`) — the runtime stays image-free; tenants mount
`code:image@<v>` decorating their media mount. That was the half that mattered
here: it unlocks thumbnails for the asset library (item 4) and responsive
images for templates.

**Multipart upload is still open, but it does not block the asset library** —
the original survey assumed it did. rs2-ui already uploads: `putResource` PUTs
a body with a content type, and the keyless POST → 201 + Location path is
annotated "Used for file upload" (`rs2-client.ts:454`). A `fetch` client never
needs multipart. So this is an **integration/compat** item, not a CMS one, and
nothing on the CMS list waits for it.

What it does enable: plain HTML forms with file inputs (no-JS pages posting
straight at a mount); **inbound webhooks that post multipart** — Mailgun
inbound email routes, Twilio media, hosted form services, all currently
unreceivable, which is the sharpest gap; batch upload as one request; and
proxying multipart onward to an external API.

Design notes for whenever it is picked up. The slot is already reserved: the
`Splitter` enum has one variant, `JsonSplit` (`spec.rs:44`), and the DSL
rejects `"multipart"` with "not yet supported" (`dsl.rs:48`) — so the intended
home is a **pipeline splitter** (multipart body → one message per part, the
remaining steps run per part). Two parts are not mechanical:

- **Part names are not addressable.** `interpolation_context`
  (`executor.rs:1096`) builds from query params, JSON-*object* body fields, and
  captured vars only. `jsonSplit` gets away with that because its elements are
  JSON objects whose fields become interpolable; a binary part contributes
  nothing, so `PUT /media/${filename}` will not resolve. Seed part metadata
  (filename, field name, content type) into the element's vars. Not optional —
  without it the splitter is useless.
- **Split means parallel fan-out, but a multipart stream cannot be seeked.**
  Either materialize the whole upload (bounded by `materialize_cap`, 100MB) and
  reuse the existing fan-out, or build a new sequential-streaming shape. Do the
  first and document the cap; the second is what large-media uploads would
  eventually want.

Then the safety work: part-count cap (`max_fanout` exists), per-part and total
size limits, and filename sanitization defanged at the store key boundary —
`filename="../../etc/x"` must never be trusted from the wire. The JS sandbox
(8.3) lists binary multipart as unsupported too; leave that as is.

### 9. Change events / webhooks — tee off publish, not off writes

Needed for cache purge, static site rebuilds, and search indexing. The original
plan was a host-level write-event hook at the `Runtime::dispatch` choke point.
That is still the right *eventual* answer, but it is no longer the first move,
because item 7 hands us a better trigger for free.

**The trigger is the publish pipeline.** `/publish` is already a deliberate
pipeline invocation, so the event is a `tee` branch on it (`executor.rs:586` —
fire-and-forget over a copy, original message continues) calling out through
the mount's `httpOut` grant. Zero runtime work. It is also the semantically
right moment: you want to index *published* content, not every draft save.

```jsonc
{ "pipeline": { "mode": "tee", "steps": [
    { "call": { "method": "POST", "url": "https://hooks.example.com/rs2",
                "headers": { "x-rs2-path": "${url.rest}" }, "effect": "keyed" } } ] } }
```

`${url.*}` lives on the `Executor`, which `run_tee` clones, so `${url.rest}`
resolves inside a fire-and-forget branch (captured `as` vars do not — they are
reset to an empty map). Place the tee **after** the forwarding call: `run_tee`
calls `materialize_if_needed`, so teeing ahead of the write would buffer the
whole body, whereas teeing after copies only the small response.

**Rejected: a `wrapper` facade over the store.** A `wrapper` mount running an
inline pipeline on every verb (`wrapper_service.rs`) looks like a way to emit
on *every* write with no runtime change, and the discovery half works — it
declares its own `pattern`/`facets`. It is wrong anyway: a call step builds a
**fresh** request (`executor.rs:866`) carrying only the step's declared
headers, and inbound headers are not reachable from `${...}` interpolation at
all (`header("x")` exists in the *condition* grammar only). So `If-Match` is
silently dropped — and both stores do enforce it (`file.rs:607`,
`data.rs:512`, the `conditional-write` facet), while rs2-ui sends it on save
and delete (`rs2-client.ts:446`). A conflicting save would land as a successful
overwrite: a lost-update bug, worse than the missing feature. A facade is also
bypassable by writing to the raw mount, so it never delivers the guarantee that
would justify the risk.

**When the host hook becomes necessary**: events on every write including
draft saves, or delivery guarantees. `tee` is `tokio::spawn` with the result
dropped — no retry, no dead-letter, failures invisible, and it dies on
shutdown; `teeWait` gets the retry policy and idempotency key but sits in the
request's latency. Neither is at-least-once. Fine for cache purge, not enough
for an index you would trust. Build the hook when that is the actual
requirement, not before.

### 10. Full-text search (runtime — can wait)

Stored queries cover structured filtering; full-text can arrive later as a
service fed by the event hook from item 9.

## Related runtime work (not in the original ten)

- **Stage 2 content projection**: extend `$select` beyond data stores to
  file-pattern stores by reading + projecting file content (JSON/front-matter)
  server-side, so file-backed collections get real columns too. Stage 1
  (metadata sort) shipped; stage 2 is designed but not built.

## Suggested sequence

**The CMS list is done.** Item 7 — preview/publish — shipped 2026-07-28 across
both repos, and it was the last item on it. The goal at the top of this page
is met: a content editor gets a labelled collections rail, schema-derived list
views, markdown/image/preview authoring, plain-language errors, and now
draft/publish with derived state — with no CMS-specific config anywhere, only
generic contracts a second client could implement.

The one piece still worth doing on this theme is **item 9's event tee** on the
publish pipeline — a few lines of tenant config, not code, and it needs a
consumer (a cache purge, a rebuild, an indexer) before it means anything.

Everything else on this page is either shipped or is not a CMS blocker:

- **Multipart upload** (8) is integration/compat work, not CMS — the asset
  library already uploads without it. Sequence it against inbound-webhook
  demand, not against this list.
- **Stage 2 content projection** matters only for file-backed collections;
  data-backed ones already have real columns.
- The **`Runtime::dispatch` write-event hook** and **full-text search** (10)
  stay parked until something actually demands durable, unbypassable events.

So: ship 7 (+9), and the CMS-usability goal at the top of this page is met.
