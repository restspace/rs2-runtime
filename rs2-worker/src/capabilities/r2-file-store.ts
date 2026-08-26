// R2 as `FileStore` (cloudflare.md §C.2). Keys are `<tenant>/<path without
// leading slash>`. Every call runs inside the tenant's DO, so per-key
// ordering is enforced in memory: an async mutex keyed by full R2 key wraps
// `writeCond`, `deleteCond`, and `rename`.

import { Body } from "../runtime/body";
import { RsError, codes } from "../runtime/error";
import { MediaType } from "../runtime/media-type";
import { validatePath } from "../runtime/router";
import { compareUtf8 } from "../runtime/listing";
import type { ByteRange, DirEntry, FileMeta, FileStore, WriteOutcome, WritePrecondition } from "./types";
import { ifMatchHits } from "./types";

/// Unknown-length uploads are buffered up to this many bytes; beyond it they
/// go multipart (`MULTIPART_PART` bytes per part).
const MATERIALIZE_CAP = 32 * 1024 * 1024;
const MULTIPART_PART = 8 * 1024 * 1024;

/// A minimal async mutex: `run` serializes callers per key.
class KeyedMutex {
  private readonly tails = new Map<string, Promise<void>>();

  async run<T>(key: string, f: () => Promise<T>): Promise<T> {
    const prev = this.tails.get(key) ?? Promise.resolve();
    let release!: () => void;
    const mine = new Promise<void>((r) => (release = r));
    const tail = prev.then(() => mine);
    this.tails.set(key, tail);
    await prev;
    try {
      return await f();
    } finally {
      release();
      if (this.tails.get(key) === tail) this.tails.delete(key);
    }
  }
}

/// The mutex is per **bucket**, not per store instance: a config PUT
/// rebuilds the tenant's services while requests are in flight, so two
/// `R2FileStore`s briefly address the same keys. Keying the mutex off the
/// bucket binding keeps ordering across that swap (issue #2 item 7). The
/// map is weak so a bucket that goes away takes its lock table with it.
const bucketMutexes = new WeakMap<R2Bucket, KeyedMutex>();

function mutexFor(bucket: R2Bucket): KeyedMutex {
  let m = bucketMutexes.get(bucket);
  if (!m) {
    m = new KeyedMutex();
    bucketMutexes.set(bucket, m);
  }
  return m;
}

export class R2FileStore implements FileStore {
  private readonly mutex: KeyedMutex;

  constructor(private readonly bucket: R2Bucket) {
    this.mutex = mutexFor(bucket);
  }

  /// `<tenant>/<path>` with the router's path safety re-applied (defense in
  /// depth: an adapter never trusts its caller with traversal).
  private key(tenant: string, path: string): string {
    validatePath(path);
    if (tenant === "" || /[/\\.]/.test(tenant)) throw RsError.internal("invalid tenant id for file store");
    const rel = path.split("/").filter((s) => s !== "").join("/");
    return rel === "" ? tenant : `${tenant}/${rel}`;
  }

  private async isDirPrefix(key: string): Promise<boolean> {
    const listed = await this.bucket.list({ prefix: `${key}/`, limit: 1 });
    return listed.objects.length > 0 || (listed.delimitedPrefixes?.length ?? 0) > 0;
  }

  async head(tenant: string, path: string): Promise<FileMeta> {
    const key = this.key(tenant, path);
    const obj = await this.bucket.head(key);
    if (obj) return { size: obj.size, lastModified: obj.uploaded, isDir: false };
    if (await this.isDirPrefix(key)) return { size: 0, lastModified: undefined, isDir: true };
    throw RsError.notFound("resource does not exist");
  }

  async read(tenant: string, path: string, range: ByteRange | undefined): Promise<Body> {
    const key = this.key(tenant, path);
    let obj: R2ObjectBody | null;
    if (range) {
      // R2 rejects an offset at/after the size; resolve against `head` so the
      // 416 carries the Rust detail wording.
      const meta = await this.bucket.head(key);
      if (!meta) return this.readMissing(key);
      const total = meta.size;
      const start = Math.min(range.start, total);
      const end = range.end !== undefined ? Math.min(range.end + 1, total) : total;
      if (start >= end) {
        throw new RsError(416, codes.BAD_REQUEST, "Range Not Satisfiable", `range start ${start} beyond resource size ${total}`);
      }
      obj = await this.bucket.get(key, { range: { offset: start, length: end - start } });
      if (!obj) return this.readMissing(key);
      const body = this.bodyOf(obj, path, end - start);
      return body;
    }
    obj = await this.bucket.get(key);
    if (!obj) return this.readMissing(key);
    return this.bodyOf(obj, path, obj.size);
  }

  private async readMissing(key: string): Promise<never> {
    if (await this.isDirPrefix(key)) throw RsError.badRequest("path is a directory");
    throw RsError.notFound("resource does not exist");
  }

  private bodyOf(obj: R2ObjectBody, path: string, len: number): Body {
    const ct = obj.httpMetadata?.contentType;
    const mediaType = ct ? MediaType.parse(ct) : MediaType.forPath(path);
    // R2's etag is the hex MD5 for single-part uploads: the opaque version.
    const body = Body.fromStream(obj.body, mediaType, len, { kind: "replayable", url: path, version: obj.etag });
    body.withLastModified(obj.uploaded);
    return body;
  }

  /// Stream/buffer `body` into `key`; returns whether the key was created.
  /// Callers hold the per-key mutex.
  private async putBody(key: string, body: Body): Promise<boolean> {
    const existed = (await this.bucket.head(key)) !== null;
    const httpMetadata = { contentType: body.mediaType.toString() };
    if (body.payload.kind === "bytes") {
      await this.bucket.put(key, body.payload.bytes, { httpMetadata });
      return !existed;
    }
    const stream = body.payload.stream;
    if (body.size !== undefined) {
      // A body with known length streams straight to R2.
      const fixed = new FixedLengthStream(body.size);
      const pump = stream.pipeTo(fixed.writable);
      await Promise.all([this.bucket.put(key, fixed.readable, { httpMetadata }), pump]);
      return !existed;
    }
    // Unknown length: buffer up to the cap, then fall over to multipart.
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    let upload: R2MultipartUpload | undefined;
    const parts: R2UploadedPart[] = [];
    let partNo = 1;
    const flushPart = async (bytes: Uint8Array) => {
      if (!upload) upload = await this.bucket.createMultipartUpload(key, { httpMetadata });
      parts.push(await upload.uploadPart(partNo++, bytes));
    };
    const concat = (): Uint8Array => {
      const buf = new Uint8Array(total);
      let off = 0;
      for (const c of chunks) {
        buf.set(c, off);
        off += c.byteLength;
      }
      chunks.length = 0;
      total = 0;
      return buf;
    };
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (!value) continue;
        chunks.push(value);
        total += value.byteLength;
        if (upload && total >= MULTIPART_PART) await flushPart(concat());
        else if (!upload && total > MATERIALIZE_CAP) {
          // Over the buffer cap: switch to multipart, draining in part-sized pieces.
          const all = concat();
          let off = 0;
          while (all.byteLength - off >= MULTIPART_PART) {
            await flushPart(all.subarray(off, off + MULTIPART_PART));
            off += MULTIPART_PART;
          }
          if (off < all.byteLength) {
            chunks.push(all.subarray(off));
            total = all.byteLength - off;
          }
        }
      }
      if (upload) {
        // The last part may be smaller than the minimum; R2 allows that only
        // for the final part.
        if (total > 0) await flushPart(concat());
        await upload.complete(parts);
      } else {
        await this.bucket.put(key, concat(), { httpMetadata });
      }
    } catch (e) {
      if (upload) await upload.abort().catch(() => undefined);
      if (e instanceof RsError) throw e;
      throw RsError.internal(`body stream error: ${e instanceof Error ? e.message : String(e)}`);
    }
    return !existed;
  }

  write(tenant: string, path: string, body: Body): Promise<boolean> {
    const key = this.key(tenant, path);
    return this.mutex.run(key, () => this.putBody(key, body));
  }

  async currentEtag(tenant: string, path: string): Promise<string | undefined> {
    const obj = await this.bucket.head(this.key(tenant, path));
    return obj ? `"${obj.etag}"` : undefined;
  }

  /// Conditional upsert: the check and the put run under the per-key mutex,
  /// which the DO's single-threaded event loop makes atomic per tenant.
  writeCond(tenant: string, path: string, body: Body, precondition: WritePrecondition): Promise<WriteOutcome> {
    const key = this.key(tenant, path);
    return this.mutex.run(key, async () => {
      const cur = await this.bucket.head(key);
      if (precondition.kind === "ifMatch") {
        if (!cur) throw RsError.preconditionFailed("If-Match given but the resource does not exist");
        if (!ifMatchHits(precondition.value, `"${cur.etag}"`)) {
          throw RsError.preconditionFailed("If-Match does not match the current ETag — re-read and retry");
        }
      } else if (precondition.kind === "ifNoneMatchStar" && cur) {
        throw RsError.preconditionFailed("If-None-Match: * given but the resource already exists");
      }
      const created = await this.putBody(key, body);
      const after = await this.bucket.head(key);
      return { created, etag: after ? `"${after.etag}"` : undefined };
    });
  }

  conditionalWriteAtomic(): boolean {
    return true;
  }

  private async deleteFile(key: string): Promise<void> {
    const cur = await this.bucket.head(key);
    if (!cur) {
      if (await this.isDirPrefix(key)) throw RsError.badRequest("path is a directory; use directory delete");
      throw RsError.notFound("resource does not exist");
    }
    await this.bucket.delete(key);
  }

  delete(tenant: string, path: string): Promise<void> {
    const key = this.key(tenant, path);
    return this.mutex.run(key, () => this.deleteFile(key));
  }

  /// RFC 9110 order: a missing resource is the 404, not a 412.
  deleteCond(tenant: string, path: string, precondition: WritePrecondition): Promise<void> {
    const key = this.key(tenant, path);
    return this.mutex.run(key, async () => {
      const cur = await this.bucket.head(key);
      if (precondition.kind === "ifMatch") {
        if (cur && !ifMatchHits(precondition.value, `"${cur.etag}"`)) {
          throw RsError.preconditionFailed("If-Match does not match the current ETag — re-read and retry");
        }
      } else if (precondition.kind === "ifNoneMatchStar" && cur) {
        throw RsError.preconditionFailed("If-None-Match: * given but the resource exists");
      }
      await this.deleteFile(key);
    });
  }

  rename(tenant: string, from: string, to: string): Promise<boolean> {
    const src = this.key(tenant, from);
    const dst = this.key(tenant, to);
    // Lock in a fixed order so two crossing renames cannot deadlock.
    const [first, second] = compareUtf8(src, dst) <= 0 ? [src, dst] : [dst, src];
    return this.mutex.run(first, () =>
      this.mutex.run(second, async () => {
        const smd = await this.bucket.head(src);
        if (!smd) {
          if (await this.isDirPrefix(src)) throw RsError.badRequest("source is a directory; file move only");
          throw RsError.notFound("source does not exist");
        }
        const existing = await this.bucket.head(dst);
        if (!existing && (await this.isDirPrefix(dst))) throw RsError.conflict("destination is a directory");
        const obj = await this.bucket.get(src);
        if (!obj) throw RsError.notFound("source does not exist");
        await this.bucket.put(dst, obj.body, {
          httpMetadata: obj.httpMetadata,
          customMetadata: obj.customMetadata,
        });
        await this.bucket.delete(src);
        return existing === null;
      }),
    );
  }

  async deleteDir(tenant: string, path: string): Promise<void> {
    const key = this.key(tenant, path);
    if (await this.isDirPrefix(key)) throw RsError.conflict("directory is not empty");
    // R2 has no directories: a never-existing directory deletes as 204
    // (the declared divergence from Rust's 404, cloudflare.md §A).
  }

  async deleteDirAll(tenant: string, path: string): Promise<void> {
    if (path.split("/").every((s) => s === "")) {
      throw RsError.badRequest("refusing to recursively delete the store root");
    }
    const key = this.key(tenant, path);
    let cursor: string | undefined;
    do {
      const page = await this.bucket.list({ prefix: `${key}/`, limit: 1000, cursor });
      const keys = page.objects.map((o) => o.key);
      if (keys.length) await this.bucket.delete(keys);
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor);
  }

  async list(tenant: string, path: string, take: number, skip: number): Promise<[DirEntry[], number]> {
    const key = this.key(tenant, path);
    const prefix = `${key}/`;
    const entries: DirEntry[] = [];
    let cursor: string | undefined;
    do {
      const page = await this.bucket.list({ prefix, delimiter: "/", limit: 1000, cursor, include: ["httpMetadata"] });
      for (const o of page.objects) {
        const name = o.key.slice(prefix.length);
        if (name === "" || name.includes(".rs2tmp-")) continue;
        entries.push({
          name,
          size: o.size,
          lastModified: o.uploaded.toISOString(),
          dir: false,
          contentType: MediaType.forPath(name).toString(),
        });
      }
      for (const p of page.delimitedPrefixes ?? []) {
        const leaf = p.slice(prefix.length).replace(/\/$/, "");
        if (leaf === "" || leaf.includes(".rs2tmp-")) continue;
        entries.push({ name: `${leaf}/`, size: 0, dir: true });
      }
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor);
    if (entries.length === 0) {
      // The tenant root not existing yet is an empty listing, not a 404.
      if (path.replace(/^\/+|\/+$/g, "") === "") return [[], 0];
      throw RsError.notFound("directory does not exist");
    }
    entries.sort((a, b) => compareUtf8(a.name, b.name));
    const total = entries.length;
    return [entries.slice(skip, skip + take), total];
  }
}
