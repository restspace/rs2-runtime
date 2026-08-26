// A `FileStore` decorator that roots every call under a path prefix — the
// seam for spec stores and `store` grants. Port of `PrefixedFileStore`.

import type { Body } from "../runtime/body";
import type { ByteRange, DirEntry, FileMeta, FileStore, WriteOutcome, WritePrecondition } from "./types";

export class PrefixedFileStore implements FileStore {
  private readonly prefix: string;

  constructor(
    private readonly inner: FileStore,
    prefix: string,
  ) {
    this.prefix = prefix.replace(/^\/+|\/+$/g, "");
  }

  private join(path: string): string {
    const rel = path.replace(/^\/+/, "");
    return rel === "" ? `/${this.prefix}` : `/${this.prefix}/${rel}`;
  }

  head(tenant: string, path: string): Promise<FileMeta> {
    return this.inner.head(tenant, this.join(path));
  }
  read(tenant: string, path: string, range: ByteRange | undefined): Promise<Body> {
    return this.inner.read(tenant, this.join(path), range);
  }
  write(tenant: string, path: string, body: Body): Promise<boolean> {
    return this.inner.write(tenant, this.join(path), body);
  }
  currentEtag(tenant: string, path: string): Promise<string | undefined> {
    return this.inner.currentEtag(tenant, this.join(path));
  }
  writeCond(tenant: string, path: string, body: Body, precondition: WritePrecondition): Promise<WriteOutcome> {
    return this.inner.writeCond(tenant, this.join(path), body, precondition);
  }
  conditionalWriteAtomic(): boolean {
    return this.inner.conditionalWriteAtomic();
  }
  delete(tenant: string, path: string): Promise<void> {
    return this.inner.delete(tenant, this.join(path));
  }
  deleteCond(tenant: string, path: string, precondition: WritePrecondition): Promise<void> {
    return this.inner.deleteCond(tenant, this.join(path), precondition);
  }
  rename(tenant: string, from: string, to: string): Promise<boolean> {
    return this.inner.rename(tenant, this.join(from), this.join(to));
  }
  deleteDir(tenant: string, path: string): Promise<void> {
    return this.inner.deleteDir(tenant, this.join(path));
  }
  deleteDirAll(tenant: string, path: string): Promise<void> {
    return this.inner.deleteDirAll(tenant, this.join(path));
  }
  list(tenant: string, path: string, take: number, skip: number): Promise<[DirEntry[], number]> {
    return this.inner.list(tenant, this.join(path), take, skip);
  }
}
