// `R2FileStore` ordering across a config swap (issue #2 item 7): the per-key
// mutex belongs to the bucket, not to the store instance, so two stores built
// for one tenant — as a config PUT briefly produces — still serialize.
import { describe, expect, it } from "vitest";

import { R2FileStore } from "../src/capabilities/r2-file-store";
import { Body } from "../src/runtime/body";
import { RsError } from "../src/runtime/error";
import { MediaType } from "../src/runtime/media-type";

/// The two methods a bytes-body write touches, with a `put` the test can
/// hold open to force the interleaving the mutex has to prevent.
function fakeBucket() {
  const objects = new Map<string, { etag: string }>();
  let gate: Promise<void> = Promise.resolve();
  let n = 0;
  const bucket = {
    async head(key: string) {
      return objects.get(key) ?? null;
    },
    async put(key: string, _bytes: unknown) {
      await gate;
      const meta = { etag: `e${++n}` };
      objects.set(key, meta);
      return meta;
    },
    async list() {
      return { objects: [], delimitedPrefixes: [] };
    },
  } as unknown as R2Bucket;
  return {
    bucket,
    hold(): () => void {
      let release!: () => void;
      gate = new Promise<void>((r) => (release = r));
      return () => {
        gate = Promise.resolve();
        release();
      };
    },
  };
}

const text = () => Body.fromString("x", new MediaType("text/plain"));

describe("R2FileStore per-key ordering", () => {
  it("serializes two store instances over the same bucket (issue #2 item 7)", async () => {
    const { bucket, hold } = fakeBucket();
    const before = new R2FileStore(bucket);
    const after = new R2FileStore(bucket); // the post-config-PUT rebuild
    const release = hold();

    const first = before.writeCond("t", "/f.txt", text(), { kind: "ifNoneMatchStar" });
    // Give the first write time to take the lock and reach the held `put`.
    await Promise.resolve();
    const second = after.writeCond("t", "/f.txt", text(), { kind: "ifNoneMatchStar" });
    release();

    await expect(first).resolves.toMatchObject({ created: true });
    // The second store's check ran after the first write landed, so it sees
    // the object — with a per-instance mutex both would have seen an empty
    // bucket and both would have "created" the file.
    await expect(second).rejects.toMatchObject({ code: RsError.preconditionFailed("x").code });
  });
});
