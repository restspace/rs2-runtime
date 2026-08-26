// The store-pattern contract over HTTP — a step-by-step mirror of
// `rs2-core/tests/store_conformance.rs`. Any mount declaring
// `pattern: "store"` must satisfy one conversation shape so a single client
// codepath can drive every store; this module IS that contract for both
// hosts. Assertion messages carry the `[mount]` tag the Rust file uses so a
// failure reads the same in either runner.
//
// Exports:
//   assertStoreContract    — section 1a: PUT/GET/conditional write/keyless
//                            POST/listing/pagination/root/conditional
//                            DELETE/container guard
//   assertListingContract  — section 1b: `$select`/`$sort` projection over
//                            data records (list-projection facet)
//   assertMetaSortContract — section 1c: `$sort=@…` metadata sort (meta-sort
//                            facet)
//   assertCodeStoreContract — the content-addressed code store's variant:
//                            keyless POST only, PUT must name the true hash

import { expect } from "vitest";

import { entryNamed, names, type Rs2Client, type Rs2Response } from "./client.ts";

/** A child body for a store: bytes + media type. */
export interface StoreBody {
  body: string;
  contentType: string;
}

export const textBody = (text: string): StoreBody => ({ body: text, contentType: "text/plain" });
export const jsonBody = (value: unknown): StoreBody => ({ body: JSON.stringify(value), contentType: "application/json" });

function withBody(b: StoreBody) {
  return { body: b.body, contentType: b.contentType };
}

function status(res: Rs2Response, want: number, msg: string): void {
  expect(res.status, `${msg}: ${res.describe()}`).toBe(want);
}

export interface StoreContractOptions {
  /** Tag for assertion messages; defaults to the mount. */
  label?: string;
}

/**
 * The parameterized contract. `makeBody` produces a valid child body for
 * this store; `container` has no trailing slash. `client` must be allowed
 * to read and write the mount.
 */
export async function assertStoreContract(
  client: Rs2Client,
  mount: string,
  container: string,
  makeBody: (i: number) => StoreBody,
  opts: StoreContractOptions = {},
): Promise<void> {
  const tag = `[${opts.label ?? mount}]`;
  const child = (name: string) => `${mount}${container}/${name}`;
  const containerPath = `${mount}${container}/`;

  // PUT child: 201 create, 200 overwrite, empty body, ETag.
  let res = await client.put(child("alpha"), withBody(makeBody(1)));
  status(res, 201, `${tag} PUT create`);
  expect(res.bytes.length, `${tag} PUT returns no body`).toBe(0);
  expect(res.etag(), `${tag} PUT create carries ETag`).not.toBeNull();
  res = await client.put(child("alpha"), withBody(makeBody(2)));
  status(res, 200, `${tag} PUT overwrite`);
  expect(res.etag(), `${tag} PUT overwrite carries ETag`).not.toBeNull();

  // GET child: the resource, with a version ETag.
  res = await client.get(child("alpha"));
  status(res, 200, `${tag} GET child`);
  let etag = res.etag();
  expect(etag, `${tag} child GET carries ETag`).not.toBeNull();

  // Conditional write (the `conditional-write` facet): a matching If-Match
  // succeeds; a stale one is 412; If-None-Match: * refuses to clobber.
  res = await client.put(child("alpha"), {
    ...withBody(makeBody(4)),
    headers: { "if-match": '"definitely-not-the-current-etag"' },
  });
  status(res, 412, `${tag} stale If-Match is 412`);
  res = await client.put(child("alpha"), { ...withBody(makeBody(5)), headers: { "if-match": etag! } });
  status(res, 200, `${tag} matching If-Match writes`);
  res = await client.put(child("alpha"), { ...withBody(makeBody(6)), headers: { "if-none-match": "*" } });
  status(res, 412, `${tag} If-None-Match: * refuses an existing child`);

  // POST container: keyless create with Location; the child is fetchable.
  res = await client.post(containerPath, withBody(makeBody(3)));
  status(res, 201, `${tag} keyless POST`);
  const location = res.header("location");
  expect(location, `${tag} keyless POST returns Location`).not.toBeNull();
  expect(location!.startsWith(containerPath), `${tag} Location under container: ${location}`).toBe(true);
  res = await client.get(location!);
  status(res, 200, `${tag} created child fetchable`);

  // Container listing: one shape, one media type, paginated.
  res = await client.get(containerPath);
  status(res, 200, `${tag} container GET`);
  expect(res.contentType(), `${tag} listing media type`).toBe("application/vnd.rs2.dir+json");
  const total = res.totalCount();
  expect(total, `${tag} X-Total-Count counts both children`).toBeGreaterThanOrEqual(2);
  const listing = res.listing();
  expect(typeof listing.path, `${tag} listing.path`).toBe("string");
  expect(listing.total, `${tag} listing.total`).toBe(total);
  expect(
    listing.entries.some((e) => e.name === "alpha" && e.dir === false),
    `${tag} child appears as an entry: ${JSON.stringify(listing)}`,
  ).toBe(true);

  // Pagination narrows entries, not the reported total.
  res = await client.get(`${containerPath}?$take=1`);
  status(res, 200, `${tag} $take page`);
  const page = res.listing();
  expect(page.entries.length, `${tag} $take pages`).toBe(1);
  expect(page.total, `${tag} paged total is the full count`).toBe(total);

  // Mount root also lists, and shows the container as a directory entry.
  res = await client.get(`${mount}/`);
  status(res, 200, `${tag} mount root lists`);
  let root = res.listing();
  const leaf = container.replace(/^\/+/, "");
  const rootEntry = entryNamed(root, leaf);
  expect(rootEntry?.dir, `${tag} container is a dir entry at the root: ${JSON.stringify(root)}`).toBe(true);

  // Conditional delete: the DELETE side of the `conditional-write`
  // contract — a stale If-Match refuses (412) and leaves the child alone.
  res = await client.delete(child("alpha"), { headers: { "if-match": '"definitely-not-the-current-etag"' } });
  status(res, 412, `${tag} stale If-Match DELETE is 412`);
  res = await client.get(child("alpha"));
  status(res, 200, `${tag} refused delete left the child in place`);
  etag = res.etag();
  expect(etag, `${tag} child GET carries ETag`).not.toBeNull();

  // DELETE child (matching If-Match): 204, then gone.
  res = await client.delete(child("alpha"), { headers: { "if-match": etag! } });
  status(res, 204, `${tag} DELETE child`);
  res = await client.get(child("alpha"));
  status(res, 404, `${tag} deleted child is gone`);

  // Container guard: non-empty delete refuses with 409; confirm succeeds.
  res = await client.delete(containerPath);
  status(res, 409, `${tag} non-empty container delete is 409 without confirm`);
  res = await client.delete(`${containerPath}?confirm=${leaf}`);
  status(res, 204, `${tag} confirmed delete`);
  res = await client.get(`${mount}/`);
  status(res, 200, `${tag} mount root lists after delete`);
  root = res.listing();
  expect(entryNamed(root, leaf), `${tag} deleted container left the root listing: ${JSON.stringify(root)}`).toBeUndefined();
}

/**
 * The projected-listing contract (the `list-projection` facet): `$select`
 * projects fields into entries, `$sort` orders by the pinned semantics
 * (binary UTF-8 code points; missing < null < false < true < numbers <
 * strings; key as final tiebreak), pagination pages the sorted whole.
 * Seeds and removes `<mount>/posts`.
 */
export async function assertListingContract(client: Rs2Client, mount: string): Promise<void> {
  const tag = `[${mount}]`;
  const put = async (key: string, val: unknown) => {
    const path = `${mount}/posts/${key}`;
    const res = await client.put(path, { json: val });
    status(res, 201, `seed ${path}`);
  };
  await put("ka", { title: "apple", n: 5, meta: { date: "2026-01-02" } });
  await put("kb", { title: "Zebra", n: 2, meta: { date: "2026-01-03" } });
  await put("kc", { title: "banana", n: 2 });
  await put("kd", { title: "cherry", n: 10, meta: { date: "2026-01-01" } });

  // $select: dir+json entries gain `fields` (projected, nested shape kept,
  // absent paths omitted); no `.schema.json` fixed entry in table data.
  let res = await client.get(`${mount}/posts/?$select=title,meta.date`);
  status(res, 200, `${tag} $select lists`);
  expect(res.contentType(), `${tag} projected listing keeps the listing media type`).toBe("application/vnd.rs2.dir+json");
  expect(res.totalCount(), `${tag} projected listing counts records`).toBe(4);
  let listing = res.listing();
  expect(listing.total).toBe(4);
  expect(
    listing.entries.every((e) => e.name !== ".schema.json"),
    `${tag} no fixed entries in a projected listing: ${JSON.stringify(listing)}`,
  ).toBe(true);
  const ka = listing.entries.find((e) => e.name === "ka");
  expect(ka?.fields, `${tag} projection keeps nested shape`).toEqual({ title: "apple", meta: { date: "2026-01-02" } });
  const kc = listing.entries.find((e) => e.name === "kc");
  expect(kc?.fields, `${tag} absent path omitted, not an error`).toEqual({ title: "banana" });

  // $sort asc: binary code-point order — "Zebra" before "apple".
  res = await client.get(`${mount}/posts/?$select=title&$sort=title`);
  status(res, 200, `${tag} $sort lists`);
  expect(names(res.listing()), `${tag} code-point ascending sort`).toEqual(["kb", "ka", "kc", "kd"]);

  // Multi-key with direction: -n then title; the n=2 tie breaks by title.
  res = await client.get(`${mount}/posts/?$select=title&$sort=-n,title`);
  status(res, 200, `${tag} multi-key $sort lists`);
  expect(names(res.listing()), `${tag} multi-key sort with descending first key`).toEqual(["kd", "ka", "kb", "kc"]);

  // A missing sort field is smallest: first ascending.
  res = await client.get(`${mount}/posts/?$select=title&$sort=meta.date`);
  status(res, 200, `${tag} nested $sort lists`);
  expect(names(res.listing()), `${tag} missing sort field sorts first ascending`).toEqual(["kc", "kd", "ka", "kb"]);

  // Pagination pages the *sorted* sequence; total stays the full count.
  res = await client.get(`${mount}/posts/?$select=title&$sort=title&$take=2&$skip=1`);
  status(res, 200, `${tag} paged $sort lists`);
  expect(res.totalCount(), `${tag} paged projected total is the full count`).toBe(4);
  expect(names(res.listing()), `${tag} pagination applies after the sort`).toEqual(["ka", "kc"]);

  // Malformed specs are client errors, never silently ignored.
  res = await client.get(`${mount}/posts/?$select=a..b`);
  status(res, 400, `${tag} malformed $select path is 400`);
  res = await client.get(`${mount}/posts/?$sort=title`);
  status(res, 400, `${tag} $sort without $select is 400, not ignored`);

  // A plain listing is unchanged by the feature existing: no `fields`.
  res = await client.get(`${mount}/posts/`);
  status(res, 200, `${tag} plain listing`);
  listing = res.listing();
  expect(
    listing.entries.every((e) => e.fields === undefined),
    `${tag} plain listing carries no fields objects: ${JSON.stringify(listing)}`,
  ).toBe(true);

  // Cleanup so the caller's store is reusable.
  res = await client.delete(`${mount}/posts/?confirm=posts`);
  status(res, 204, `${tag} cleanup`);
}

/**
 * The metadata-sort contract (the `meta-sort` facet): `$sort` over
 * `@`-prefixed listing metadata orders any file-pattern listing without
 * content reads — same comparison semantics as the projected-listing
 * contract (binary strings, missing-first ascending, name tiebreak),
 * pagination after the sort, unknown/unprefixed keys a 400. `makeBody(i)`
 * must produce a body whose size scales with `i`. Seeds and removes
 * `<mount>/msort`.
 */
export async function assertMetaSortContract(
  client: Rs2Client,
  mount: string,
  makeBody: (i: number) => StoreBody,
): Promise<void> {
  const tag = `[${mount}]`;
  // Distinct sizes (body length scales with i) + a subdirectory.
  for (const [name, i] of [["bb", 3], ["aa", 1], ["cc", 2]] as const) {
    const res = await client.put(`${mount}/msort/${name}`, withBody(makeBody(i)));
    status(res, 201, `seed ${name}`);
  }
  let res = await client.put(`${mount}/msort/sub/inner`, withBody(makeBody(1)));
  status(res, 201, "seed sub/inner");

  // @name descending (code-point order, dirs by their slashed name).
  res = await client.get(`${mount}/msort/?$sort=-@name`);
  status(res, 200, `${tag} -@name lists`);
  expect(names(res.listing()), `${tag} -@name order`).toEqual(["sub/", "cc", "bb", "aa"]);

  // -@size with @name tiebreak: sizes scale with the seed index; the dir
  // (size 0) and the smallest file tie region stays name-deterministic.
  res = await client.get(`${mount}/msort/?$sort=-@size,@name`);
  status(res, 200, `${tag} -@size lists`);
  const got = names(res.listing());
  expect(got[0], `${tag} largest first: ${JSON.stringify(got)}`).toBe("bb");
  expect(got[1], `${tag} then next: ${JSON.stringify(got)}`).toBe("cc");

  // Pagination applies after the sort; total is the full count.
  res = await client.get(`${mount}/msort/?$sort=-@name&$take=2&$skip=1`);
  status(res, 200, `${tag} paged meta sort lists`);
  expect(res.totalCount(), `${tag} meta-sorted total is the full count`).toBe(4);
  expect(names(res.listing()), `${tag} pagination after the meta sort`).toEqual(["cc", "bb"]);

  // @lastModified sorts without error and keeps every entry (mtime
  // granularity makes strict order assertions flaky; the name tiebreak
  // keeps the result deterministic per run).
  res = await client.get(`${mount}/msort/?$sort=-@lastModified`);
  status(res, 200, `${tag} -@lastModified lists`);
  expect(res.listing().entries.length, `${tag} -@lastModified keeps every entry`).toBe(4);

  // Unknown and unprefixed keys are client errors, never ignored.
  for (const bad of ["@nope", "name", "-size"]) {
    res = await client.get(`${mount}/msort/?$sort=${bad}`);
    status(res, 400, `${tag} $sort=${bad} is 400`);
  }

  // The plain listing is untouched by the feature existing.
  res = await client.get(`${mount}/msort/`);
  status(res, 200, `${tag} plain listing`);
  expect(res.listing().entries.length, `${tag} plain listing keeps every entry`).toBe(4);

  // Cleanup.
  res = await client.delete(`${mount}/msort/?confirm=msort`);
  status(res, 204, `${tag} cleanup`);
}

/**
 * The deployed-code store (`<services>/code/`): store-patterned with the
 * `content-addressed` facet, so the shape differs from the generic
 * contract in exactly two places — keyless POST is the only way to create
 * (the child name derives from the bytes), and PUT must name the true hash
 * (409 otherwise). Listings, reads, deletes, and the container guard are
 * the common shape. `client` must be an operator of the services mount.
 */
export async function assertCodeStoreContract(
  client: Rs2Client,
  codeBase: string,
  name: string,
  bundleText: string,
): Promise<void> {
  const tag = `[${codeBase}]`;
  const containerPath = `${codeBase}/${name}/`;
  const js = { body: bundleText, contentType: "application/javascript" };

  // Keyless POST deploys: 201, Location under the container, a body naming
  // the content-derived version and the `code:` ref a mount would use.
  let res = await client.post(containerPath, js);
  status(res, 201, `${tag} keyless POST deploys`);
  const location = res.header("location");
  expect(location, `${tag} deploy returns Location`).not.toBeNull();
  expect(location!.startsWith(containerPath), `${tag} Location under container: ${location}`).toBe(true);
  const deployed = res.json<{ name: string; version: string; ref: string; validated: boolean }>();
  expect(deployed.name, `${tag} deploy body names the bundle`).toBe(name);
  expect(deployed.version, `${tag} deploy body carries a version`).toMatch(/^[0-9a-f]{16}$/);
  expect(deployed.ref, `${tag} deploy body carries the code ref`).toBe(`code:${name}@${deployed.version}`);
  expect(typeof deployed.validated, `${tag} deploy body reports validation`).toBe("boolean");
  const version = deployed.version;

  // Re-deploying identical bytes yields the same version (immutable,
  // content-addressed).
  res = await client.post(containerPath, js);
  status(res, 201, `${tag} re-deploy of identical bytes`);
  expect(res.json().ref, `${tag} identical bytes → same ref`).toBe(deployed.ref);

  // GET child via the Location and the bare version: the bytes, an ETag
  // naming the version, and an immutable cache policy.
  for (const path of [location!, `${containerPath}${version}`]) {
    res = await client.get(path);
    status(res, 200, `${tag} GET ${path}`);
    expect(res.etag(), `${tag} child ETag is the version`).toBe(`"${version}"`);
    expect(res.header("cache-control") ?? "", `${tag} immutable cache policy`).toContain("immutable");
    expect(res.text(), `${tag} stored bytes round-trip`).toBe(bundleText);
  }
  res = await client.get(`${containerPath}feedf00ddeadbeef`);
  status(res, 404, `${tag} unknown version is 404`);

  // Container listing: the one dir+json shape; the child is named by its
  // version; nothing mounts it yet.
  res = await client.get(containerPath);
  status(res, 200, `${tag} container GET`);
  expect(res.contentType(), `${tag} listing media type`).toBe("application/vnd.rs2.dir+json");
  expect(res.totalCount(), `${tag} X-Total-Count`).toBe(1);
  let listing = res.listing();
  expect(listing.total, `${tag} listing.total`).toBe(1);
  const entry = listing.entries[0];
  expect(entry.name.startsWith(version), `${tag} child named by version: ${JSON.stringify(listing)}`).toBe(true);
  expect(entry.dir, `${tag} child is not a dir`).toBe(false);
  expect(entry.mountedAt, `${tag} nothing mounts it yet`).toBeUndefined();

  // Mount root lists the bundle as a directory entry.
  res = await client.get(`${codeBase}/`);
  status(res, 200, `${tag} code root lists`);
  let root = res.listing();
  expect(entryNamed(root, name)?.dir, `${tag} bundle is a dir entry at the root: ${JSON.stringify(root)}`).toBe(true);

  // PUT child: content-addressed — only the true hash name is accepted.
  res = await client.put(`${containerPath}${version}`, js);
  status(res, 200, `${tag} idempotent re-upload at the true hash`);
  expect(res.etag(), `${tag} PUT carries the version ETag`).toBe(`"${version}"`);
  res = await client.put(`${containerPath}0000000000000000`, js);
  status(res, 409, `${tag} PUT at a wrong hash is 409`);

  // Container guard: non-empty delete refuses with 409; child delete 204;
  // confirmed container delete 204; root listing no longer shows it.
  res = await client.delete(containerPath);
  status(res, 409, `${tag} non-empty container delete is 409 without confirm`);
  res = await client.delete(`${containerPath}${version}`);
  status(res, 204, `${tag} DELETE child`);
  res = await client.get(`${containerPath}${version}`);
  status(res, 404, `${tag} deleted child is gone`);
  res = await client.delete(`${containerPath}?confirm=${name}`);
  status(res, 204, `${tag} confirmed delete`);
  res = await client.get(`${codeBase}/`);
  status(res, 200, `${tag} code root lists after delete`);
  root = res.listing();
  expect(entryNamed(root, name), `${tag} deleted bundle left the root listing: ${JSON.stringify(root)}`).toBeUndefined();
}
