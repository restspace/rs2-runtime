// Built-in adapter registry: per-kind name → factory maps so a mount can
// select a built-in backend with `store.adapter = "builtin:<name>"`. Port of
// `rs2-core/src/adapters/registry.rs`; names: data `mem`, `file`; files
// `local`; query `reference`; message `aws-sns`, `cf-email`.

import { RsError } from "../runtime/error";
import type { Json } from "../runtime/error";
import type { MessageGateway } from "./message";
import type { DataStore, FileStore, HttpOut, QueryStore } from "./types";

export type DataFactory = (config: Json) => DataStore;
export type FileFactory = (config: Json) => FileStore;
export type QueryFactory = (config: Json) => QueryStore;
/// Builds an un-scoped `MessageGateway`. Unlike the store kinds, a provider
/// adapter reaches the network, so it is handed the node's `HttpOut` rather
/// than opening its own client.
export type MessageFactory = (config: Json, http: HttpOut) => MessageGateway;

export class BuiltinRegistry {
  private readonly data = new Map<string, DataFactory>();
  private readonly files = new Map<string, FileFactory>();
  private readonly query = new Map<string, QueryFactory>();
  private readonly message = new Map<string, MessageFactory>();

  registerData(name: string, f: DataFactory): void {
    this.data.set(name, f);
  }
  registerFiles(name: string, f: FileFactory): void {
    this.files.set(name, f);
  }
  registerQuery(name: string, f: QueryFactory): void {
    this.query.set(name, f);
  }
  registerMessage(name: string, f: MessageFactory): void {
    this.message.set(name, f);
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
  messageNames(): string[] {
    return [...this.message.keys()].sort();
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
  buildMessage(name: string, config: Json, http: HttpOut | undefined): MessageGateway | undefined {
    const f = this.message.get(name);
    if (!f) return undefined;
    if (!http) {
      throw RsError.badRequest(
        `message adapter 'builtin:${name}' calls a provider over HTTP, but this node has no outbound HTTP capability`,
      );
    }
    return f(config, http);
  }
}
