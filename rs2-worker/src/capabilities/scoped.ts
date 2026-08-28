// Tenant-scoped capability handles — the only form services see. The host
// constructs them pre-scoped, so a service can never choose its tenant.
// Port of the `Scoped*` wrappers in `rs2-core/src/capabilities/mod.rs`.

import type { Body } from "../runtime/body";
import type { Json, JsonObject } from "../runtime/error";
import type { ListSpec } from "../runtime/listing";
import type { Channel, MessageGateway, Outbound, Receipt } from "./message";
import { PrefixedFileStore } from "./prefixed";
import type {
  ByteRange,
  DataStore,
  DirEntry,
  FileMeta,
  FileStore,
  QueryStore,
  WriteOutcome,
  WritePrecondition,
} from "./types";

export class ScopedFileStore {
  constructor(
    private readonly inner: FileStore,
    private readonly tenant: string,
  ) {}

  /// A view of this handle rooted under a path prefix.
  prefixed(prefix: string): ScopedFileStore {
    return new ScopedFileStore(new PrefixedFileStore(this.inner, prefix), this.tenant);
  }

  head(path: string): Promise<FileMeta> {
    return this.inner.head(this.tenant, path);
  }
  read(path: string, range: ByteRange | undefined): Promise<Body> {
    return this.inner.read(this.tenant, path, range);
  }
  write(path: string, body: Body): Promise<boolean> {
    return this.inner.write(this.tenant, path, body);
  }
  currentEtag(path: string): Promise<string | undefined> {
    return this.inner.currentEtag(this.tenant, path);
  }
  writeCond(path: string, body: Body, precondition: WritePrecondition): Promise<WriteOutcome> {
    return this.inner.writeCond(this.tenant, path, body, precondition);
  }
  conditionalWriteAtomic(): boolean {
    return this.inner.conditionalWriteAtomic();
  }
  delete(path: string): Promise<void> {
    return this.inner.delete(this.tenant, path);
  }
  deleteCond(path: string, precondition: WritePrecondition): Promise<void> {
    return this.inner.deleteCond(this.tenant, path, precondition);
  }
  rename(from: string, to: string): Promise<boolean> {
    return this.inner.rename(this.tenant, from, to);
  }
  deleteDir(path: string): Promise<void> {
    return this.inner.deleteDir(this.tenant, path);
  }
  deleteDirAll(path: string): Promise<void> {
    return this.inner.deleteDirAll(this.tenant, path);
  }
  list(path: string, take: number, skip: number): Promise<[DirEntry[], number]> {
    return this.inner.list(this.tenant, path, take, skip);
  }
}

export class ScopedDataStore {
  constructor(
    private readonly inner: DataStore,
    private readonly tenant: string,
  ) {}

  get(dataset: string, key: string): Promise<Json> {
    return this.inner.get(this.tenant, dataset, key);
  }
  put(dataset: string, key: string, value: Json): Promise<boolean> {
    return this.inner.put(this.tenant, dataset, key, value);
  }
  delete(dataset: string, key: string): Promise<void> {
    return this.inner.delete(this.tenant, dataset, key);
  }
  listDatasets(take: number, skip: number): Promise<[string[], number]> {
    return this.inner.listDatasets(this.tenant, take, skip);
  }
  listKeys(dataset: string, take: number, skip: number): Promise<[string[], number]> {
    return this.inner.listKeys(this.tenant, dataset, take, skip);
  }
  getSchema(dataset: string): Promise<Json | undefined> {
    return this.inner.getSchema(this.tenant, dataset);
  }
  putSchema(dataset: string, schema: Json): Promise<void> {
    return this.inner.putSchema(this.tenant, dataset, schema);
  }
  deleteDataset(dataset: string): Promise<void> {
    return this.inner.deleteDataset(this.tenant, dataset);
  }
  listRecords(dataset: string, spec: ListSpec): Promise<[Array<[string, Json]>, number]> {
    return this.inner.listRecords(this.tenant, dataset, spec);
  }
  scanMatching(dataset: string, keep: (v: Json) => boolean): Promise<Array<[string, Json]>> {
    return this.inner.scanMatching(this.tenant, dataset, keep);
  }
  listingPushdown(): boolean {
    return this.inner.listingPushdown();
  }
}

export class ScopedQueryStore {
  constructor(
    private readonly inner: QueryStore,
    private readonly tenant: string,
  ) {}

  runQuery(query: Json, params: JsonObject, take: number, skip: number): Promise<[Json[], number]> {
    return this.inner.runQuery(this.tenant, query, params, take, skip);
  }
  quote(value: Json): string {
    return this.inner.quote(value);
  }
}

export class ScopedMessageGateway {
  constructor(
    private readonly inner: MessageGateway,
    private readonly tenant: string,
  ) {}

  send(out: Outbound): Promise<Receipt> {
    return this.inner.send(this.tenant, out);
  }
  status(id: string): Promise<Json> {
    return this.inner.status(this.tenant, id);
  }
  channels(): Channel[] {
    return this.inner.channels();
  }
  deliveryStatus(): boolean {
    return this.inner.deliveryStatus();
  }
  provider(): string {
    return this.inner.provider();
  }
}
