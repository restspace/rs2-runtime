// Capability interfaces (PRD §9.2): services reach infrastructure only
// through capability handles, pre-scoped to a tenant by the host. Port of
// the trait shapes in `rs2-core/src/capabilities/mod.rs`.

import { RsError } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import type { Body } from "../runtime/body";
import type { Message } from "../runtime/message";
import type { ListSpec } from "../runtime/listing";
import { project, sortPageProject } from "../runtime/listing";

/// A parsed write precondition (the store equivalent of HTTP conditional headers).
export type WritePrecondition = { kind: "none" } | { kind: "ifMatch"; value: string } | { kind: "ifNoneMatchStar" };

export const NO_PRECONDITION: WritePrecondition = { kind: "none" };

/// The result of a (conditional) write.
export interface WriteOutcome {
  created: boolean;
  etag: string | undefined;
}

/// Whether an `If-Match` header value (comma list, weak-aware; `*` matches
/// any existing resource) matches the resource's current quoted ETag.
export function ifMatchHits(ifMatch: string, etag: string): boolean {
  return ifMatch
    .split(",")
    .map((c) => c.trim())
    .some((c) => c === "*" || c.replace(/^W\//, "") === etag);
}

/// Inclusive byte range for partial reads (`Range: bytes=start-end`).
export interface ByteRange {
  start: number;
  /// Inclusive end; `undefined` means "to the end of the resource".
  end: number | undefined;
}

export interface FileMeta {
  size: number;
  lastModified: Date | undefined;
  isDir: boolean;
}

export interface DirEntry {
  name: string;
  size: number;
  /// RFC 3339; omitted from the JSON when absent.
  lastModified?: string;
  dir: boolean;
  /// Media type of a file entry; absent for directories.
  contentType?: string;
}

/// The wire form of a `DirEntry` (absent optionals omitted).
export function dirEntryJson(e: DirEntry): JsonObject {
  const out: JsonObject = { name: e.name, size: e.size };
  if (e.lastModified !== undefined) out.lastModified = e.lastModified;
  out.dir = e.dir;
  if (e.contentType !== undefined) out.contentType = e.contentType;
  return out;
}

/// Streamed file storage. `tenant` is supplied by the host-side scoping
/// wrapper, never by service code. Paths are tenant-relative.
export interface FileStore {
  head(tenant: string, path: string): Promise<FileMeta>;
  read(tenant: string, path: string, range: ByteRange | undefined): Promise<Body>;
  /// Returns `true` if the resource was created (vs. overwritten).
  write(tenant: string, path: string, body: Body): Promise<boolean>;
  /// The current content-version ETag (quoted), or `undefined` if absent.
  currentEtag(tenant: string, path: string): Promise<string | undefined>;
  writeCond(tenant: string, path: string, body: Body, precondition: WritePrecondition): Promise<WriteOutcome>;
  conditionalWriteAtomic(): boolean;
  delete(tenant: string, path: string): Promise<void>;
  deleteCond(tenant: string, path: string, precondition: WritePrecondition): Promise<void>;
  rename(tenant: string, from: string, to: string): Promise<boolean>;
  deleteDir(tenant: string, path: string): Promise<void>;
  deleteDirAll(tenant: string, path: string): Promise<void>;
  list(tenant: string, path: string, take: number, skip: number): Promise<[DirEntry[], number]>;
}

/// The default `currentEtag`: read the resource's `replayable` provenance.
export async function currentEtagFromRead(store: FileStore, tenant: string, path: string): Promise<string | undefined> {
  try {
    const body = await store.read(tenant, path, undefined);
    try {
      await body.intoStream().cancel();
    } catch {
      /* the stream is discarded either way */
    }
    return body.provenance.kind === "replayable" ? `"${body.provenance.version}"` : undefined;
  } catch (e) {
    if (e instanceof RsError && e.status === 404) return undefined;
    throw e;
  }
}

/// Best-effort check-then-write, the trait default in Rust.
export async function writeCondDefault(
  store: FileStore,
  tenant: string,
  path: string,
  body: Body,
  precondition: WritePrecondition,
): Promise<WriteOutcome> {
  if (precondition.kind === "ifMatch") {
    const cur = await store.currentEtag(tenant, path);
    if (cur === undefined) throw RsError.preconditionFailed("If-Match given but the resource does not exist");
    if (!ifMatchHits(precondition.value, cur)) {
      throw RsError.preconditionFailed("If-Match does not match the current ETag — re-read and retry");
    }
  } else if (precondition.kind === "ifNoneMatchStar") {
    if ((await store.currentEtag(tenant, path)) !== undefined) {
      throw RsError.preconditionFailed("If-None-Match: * given but the resource already exists");
    }
  }
  const created = await store.write(tenant, path, body);
  const etag = await store.currentEtag(tenant, path);
  return { created, etag };
}

/// Best-effort conditional delete: a missing resource is the store's 404,
/// not a 412 (RFC 9110 §13.1).
export async function deleteCondDefault(
  store: FileStore,
  tenant: string,
  path: string,
  precondition: WritePrecondition,
): Promise<void> {
  if (precondition.kind === "ifMatch") {
    const cur = await store.currentEtag(tenant, path);
    if (cur !== undefined && !ifMatchHits(precondition.value, cur)) {
      throw RsError.preconditionFailed("If-Match does not match the current ETag — re-read and retry");
    }
  } else if (precondition.kind === "ifNoneMatchStar") {
    if ((await store.currentEtag(tenant, path)) !== undefined) {
      throw RsError.preconditionFailed("If-None-Match: * given but the resource exists");
    }
  }
  await store.delete(tenant, path);
}

/// Schema-validated JSON storage keyed by dataset + key.
export interface DataStore {
  get(tenant: string, dataset: string, key: string): Promise<Json>;
  /// Returns `true` if the record was created (vs. updated).
  put(tenant: string, dataset: string, key: string, value: Json): Promise<boolean>;
  delete(tenant: string, dataset: string, key: string): Promise<void>;
  listKeys(tenant: string, dataset: string, take: number, skip: number): Promise<[string[], number]>;
  listDatasets(tenant: string, take: number, skip: number): Promise<[string[], number]>;
  getSchema(tenant: string, dataset: string): Promise<Json | undefined>;
  putSchema(tenant: string, dataset: string, schema: Json): Promise<void>;
  deleteDataset(tenant: string, dataset: string): Promise<void>;
  scanMatching(tenant: string, dataset: string, keep: (v: Json) => boolean): Promise<Array<[string, Json]>>;
  listRecords(tenant: string, dataset: string, spec: ListSpec): Promise<[Array<[string, Json]>, number]>;
  listingPushdown(): boolean;
}

/// The default `listRecords` pipeline over `listKeys` + `get`.
export async function listRecordsFallback(
  store: DataStore,
  tenant: string,
  dataset: string,
  spec: ListSpec,
): Promise<[Array<[string, Json]>, number]> {
  if (spec.sort.length === 0) {
    const [keys, total] = await store.listKeys(tenant, dataset, spec.take, spec.skip);
    const page: Array<[string, Json]> = [];
    for (const key of keys) page.push([key, project(await store.get(tenant, dataset, key), spec.fields)]);
    return [page, total];
  }
  const [keys] = await store.listKeys(tenant, dataset, Number.MAX_SAFE_INTEGER, 0);
  const records: Array<[string, Json]> = [];
  for (const key of keys) records.push([key, await store.get(tenant, dataset, key)]);
  return sortPageProject(records, spec);
}

/// Outbound HTTP, granted with allowed-host patterns; default deny.
export interface HttpOut {
  request(msg: Message): Promise<Message>;
}

/// Parameterized queries against a backing store (PRD §10.4).
export interface QueryStore {
  runQuery(tenant: string, query: Json, params: JsonObject, take: number, skip: number): Promise<[Json[], number]>;
  quote(value: Json): string;
}

/// Read an optional, **safe** storage root from a mount's expanded `store` config.
export function sanitizedStoreRoot(store: Json): string | undefined {
  if (!store || typeof store !== "object" || Array.isArray(store)) return undefined;
  const raw = store.root;
  if (typeof raw !== "string") return undefined;
  const root = raw.trim();
  if (root === "") return undefined;
  if (root.startsWith("/") || root.startsWith("\\") || (root.length >= 2 && root[1] === ":")) {
    throw RsError.badRequest(`store root '${root}' must be a relative path (no leading '/'/'\\' or drive letter)`);
  }
  if (root.split(/[/\\]/).some((c) => c === "..")) {
    throw RsError.badRequest(`store root '${root}' must not contain a '..' path segment`);
  }
  return root.replace(/^\/+|\/+$/g, "");
}

/// Resolve a **required, safe** storage root from an explicit `builtin:file`
/// / `builtin:local` primary mount store.
export function requireStoreRoot(store: Json): string {
  const root = sanitizedStoreRoot(store);
  if (root === undefined) {
    throw RsError.badRequest(
      "an explicit 'builtin:file'/'builtin:local' store requires a non-empty 'root' (each such mount gets its own physical store); omit the 'store' block to share the node default instead",
    );
  }
  return root;
}
