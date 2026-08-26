// Built-in adapter registry: per-kind name → factory maps so a mount can
// select a built-in backend with `store.adapter = "builtin:<name>"`. Port of
// `rs2-core/src/adapters/registry.rs`; names: data `mem`, `file`; files
// `local`; query `reference`.

import type { Json } from "../runtime/error";
import type { DataStore, FileStore, QueryStore } from "./types";

export type DataFactory = (config: Json) => DataStore;
export type FileFactory = (config: Json) => FileStore;
export type QueryFactory = (config: Json) => QueryStore;

export class BuiltinRegistry {
  private readonly data = new Map<string, DataFactory>();
  private readonly files = new Map<string, FileFactory>();
  private readonly query = new Map<string, QueryFactory>();

  registerData(name: string, f: DataFactory): void {
    this.data.set(name, f);
  }
  registerFiles(name: string, f: FileFactory): void {
    this.files.set(name, f);
  }
  registerQuery(name: string, f: QueryFactory): void {
    this.query.set(name, f);
  }

  dataNames(): string[] {
    return [...this.data.keys()].sort();
  }
  filesNames(): string[] {
    return [...this.files.keys()].sort();
  }
  queryNames(): string[] {
    return [...this.query.keys()].sort();
  }

  /// `undefined` means no such name for this kind.
  buildData(name: string, config: Json): DataStore | undefined {
    const f = this.data.get(name);
    return f ? f(config) : undefined;
  }
  buildFiles(name: string, config: Json): FileStore | undefined {
    const f = this.files.get(name);
    return f ? f(config) : undefined;
  }
  buildQuery(name: string, config: Json): QueryStore | undefined {
    const f = this.query.get(name);
    return f ? f(config) : undefined;
  }
}
