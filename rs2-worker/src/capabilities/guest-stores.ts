// Guest-backed capability adapters (cloudflare.md P4b): a deployed JS
// bundle backs a mount's persistence, run as a Dynamic Worker under the
// guest shim and speaking the **store pattern** over HTTP-shaped messages.
// Port of `rs2-core/src/engines/resident.rs`: `GuestDataStore`,
// `GuestQueryStore`, `GuestFileStore` (and `GuestSmsGateway`) map their
// capability interface onto `{method, url, body?, mediaType?}` requests
// dispatched with the mount's `store` config block as the guest config,
// and map non-2xx responses back to the same `RsError` a built-in store
// would raise.
//
// Where Rust keeps a per-mount pool of resident isolates (`maxRuntimes`)
// with idle eviction (`idleMs`/`idleSeconds`), this host runs **one
// Dynamic Worker isolate per mount** (id `<tenant>:<mount base>:adapter:
// <name>@<version>`) and the platform owns eviction — the pool knobs are
// accepted and ignored (a declared host difference, cloudflare.md §A/§I).
// A bundle still pools its backend connection in module scope, exactly as
// on Rust: the isolate is resident until the platform evicts it.

import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { Body } from "../runtime/body";
import { MediaType } from "../runtime/media-type";
import { sha256Hex } from "../runtime/crypto";
import type { ListSpec } from "../runtime/listing";
import { socketAllowlistFromConfig } from "../engines/dynamic-worker";
import type { DynamicWorkerEngine } from "../engines/dynamic-worker";
import { codePathJs } from "../services/code";
import type { ScopedFileStore } from "./scoped";
import {
  currentEtagFromRead,
  deleteCondDefault,
  listRecordsFallback,
  writeCondDefault,
} from "./types";
import type {
  ByteRange,
  DataStore,
  DirEntry,
  FileMeta,
  FileStore,
  QueryStore,
  SmsGateway,
  WriteOutcome,
  WritePrecondition,
} from "./types";

/// What a guest adapter needs from the host beyond its bundle: the engine
/// and the per-mount identity/limits. `files` is the tenant-scoped store
/// the bundle is read from (the built-in one — no circularity).
export interface GuestAdapterWiring {
  engine: DynamicWorkerEngine;
  files: ScopedFileStore;
  tenant: string;
  /// The mount base path — part of the isolate id, so two mounts of one
  /// bundle never share module state (decision 33 extended to adapters).
  mountBase: string;
  materializedBodyBytes: number;
  wallClockMs: number;
  cpuMs: number;
}

/// The engine-facing surface of [`ResidentAdapter`], split out so unit
/// tests can drive the capability mapping against a recording fake.
export interface AdapterCaller {
  call(method: string, path: string, body?: Json): Promise<[number, Json]>;
  hasFeature(name: string): boolean;
}

/// Shared machinery for guest-backed capability adapters — the Worker port
/// of Rust's `ResidentAdapter`: bundle loading (lazy, cached), the
/// `features` handshake (read over RPC on first use; `false` before it,
/// which is safe — the fallback path is always correct), and the
/// request/response call helper. The whole `store` config block is handed
/// to the bundle as `ctx.config`.
export class ResidentAdapter implements AdapterCaller {
  private readonly name: string;
  private readonly version: string;
  private readonly socketAllowlist: string[];
  private source: Promise<string> | undefined;
  private features: string[] | undefined;
  private featuresLoading: Promise<string[]> | undefined;

  constructor(
    kind: string,
    readonly adapterRef: string,
    private readonly storeConfig: JsonObject,
    private readonly wiring: GuestAdapterWiring,
  ) {
    const invalid = () =>
      RsError.badRequest(`${kind} store adapter '${adapterRef}' must be 'code:<name>@<version>'`);
    if (!adapterRef.startsWith("code:")) throw invalid();
    const rest = adapterRef.slice("code:".length);
    const at = rest.indexOf("@");
    if (at < 0) throw invalid();
    const name = rest.slice(0, at);
    const version = rest.slice(at + 1);
    if (name === "" || version === "" || /[/\\.]/.test(name)) {
      throw RsError.badRequest(`invalid ${kind} store adapter reference '${adapterRef}'`);
    }
    this.name = name;
    this.version = version;
    this.socketAllowlist = socketAllowlistFromConfig(storeConfig);
    // `maxRuntimes` / `idleMs` / `idleSeconds` are accepted and IGNORED on
    // this host: one isolate per mount, the platform owns eviction
    // (cloudflare.md §A). Reading them here documents the contract.
    void storeConfig.maxRuntimes;
    void storeConfig.idleMs;
    void storeConfig.idleSeconds;
  }

  /// `<tenant>:<mount base>:adapter:<name>@<version>` — mount-addressed
  /// (grants and module state are per mount) and content-addressed via the
  /// version (a redeploy is a new id).
  private codeId(): string {
    const base = this.wiring.mountBase === "" ? "/" : this.wiring.mountBase;
    return `${this.wiring.tenant}:${base}:adapter:${this.name}@${this.version}`;
  }

  /// The bundle source, loaded once from the tenant file store and cached
  /// (Rust `cached_source`). A failed load does not pin — deploying the
  /// bundle after a 404 must take effect.
  private loadSource(): Promise<string> {
    if (this.source) return this.source;
    const load = (async () => {
      let body: Body;
      try {
        body = await this.wiring.files.read(codePathJs(this.name, this.version), undefined);
      } catch {
        throw RsError.notFound(
          `data adapter bundle 'code:${this.name}@${this.version}' not found — deploy it via PUT /code/${this.name}`,
        );
      }
      const bytes = await body.materialize(this.wiring.materializedBodyBytes);
      try {
        return new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes);
      } catch {
        throw RsError.contractViolation("data adapter bundle is not valid UTF-8");
      }
    })();
    this.source = load;
    load.catch(() => {
      this.source = undefined;
    });
    return load;
  }

  /// Whether the bundle advertised `name` in its `features` export.
  /// `false` until the first call has read the export (lazy, as on Rust
  /// where the resident spawn is lazy).
  hasFeature(name: string): boolean {
    return this.features !== undefined && this.features.includes(name);
  }

  private async ensureFeatures(source: string): Promise<void> {
    if (this.features !== undefined) return;
    if (!this.featuresLoading) {
      this.featuresLoading = this.wiring.engine.adapterFeatures(
        this.codeId(),
        source,
        this.wiring.tenant,
        this.wiring.cpuMs,
      );
      this.featuresLoading.catch(() => {
        this.featuresLoading = undefined;
      });
    }
    this.features = await this.featuresLoading;
  }

  /// Send a request envelope to the adapter; return `(status, body)`.
  async call(method: string, path: string, body?: Json): Promise<[number, Json]> {
    const source = await this.loadSource();
    await this.ensureFeatures(source);
    const req: JsonObject = { method, url: path };
    if (body !== undefined) {
      req.body = body;
      req.mediaType = "application/json";
    }
    const { status, body: respBody } = await this.wiring.engine.invokeAdapter({
      codeId: this.codeId(),
      source,
      tenant: this.wiring.tenant,
      req,
      storeConfig: this.storeConfig,
      socketAllowlist: this.socketAllowlist,
      serviceRef: `${this.name}@${this.version}`,
      materializeCap: this.wiring.materializedBodyBytes,
      wallClockMs: this.wiring.wallClockMs,
      cpuMs: this.wiring.cpuMs,
    });
    return [status, respBody];
  }
}

/// Map a non-2xx store-pattern response to an `RsError`, preserving the
/// status class and detail so services and clients see the same identity a
/// built-in store would produce (Rust `store_error`).
export function storeError(status: number, body: Json): RsError {
  const obj = body && typeof body === "object" && !Array.isArray(body) ? body : {};
  const fromBody = typeof obj.detail === "string" ? obj.detail : typeof obj.message === "string" ? obj.message : undefined;
  const detail = fromBody ?? `data adapter returned ${status}`;
  switch (status) {
    case 400:
      return RsError.badRequest(detail);
    case 401:
      return RsError.unauthorized(detail);
    case 403:
      return RsError.forbidden(detail);
    case 404:
      return RsError.notFound(detail);
    case 409:
      return RsError.conflict(detail);
    case 413:
      return RsError.payloadTooLarge(detail);
    case 422:
      return RsError.validationFailed(detail, obj.errors ?? null);
    default:
      return RsError.contractViolation(detail);
  }
}

function ok(status: number): boolean {
  return status >= 200 && status < 300;
}

/// Child entries of a store listing whose `dir` flag matches `wantDir`,
/// returning each entry's `name` (the schema sentinel is dropped for keys).
function listingNames(body: Json, wantDir: boolean): string[] {
  const entries = body && typeof body === "object" && !Array.isArray(body) && Array.isArray(body.entries) ? body.entries : [];
  const out: string[] = [];
  for (const e of entries) {
    if (!e || typeof e !== "object" || Array.isArray(e)) continue;
    if ((e.dir === true) !== wantDir) continue;
    if (typeof e.name !== "string" || e.name === ".schema.json") continue;
    out.push(e.name.replace(/\/+$/, ""));
  }
  return out;
}

function listingTotal(body: Json): number {
  const t = body && typeof body === "object" && !Array.isArray(body) ? body.total : undefined;
  return typeof t === "number" && Number.isFinite(t) && t >= 0 ? Math.floor(t) : 0;
}

/// Percent-encode a `$select`/`$sort` value for the store-shaped wire form
/// (Rust `query_encode`): everything but `[A-Za-z0-9-_.~]` is escaped, so
/// a field path can't smuggle separators into the query. The guest decodes
/// with `URLSearchParams`, which reverses this exactly.
export function queryEncode(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let out = "";
  for (const b of bytes) {
    const c = String.fromCharCode(b);
    if (/[A-Za-z0-9\-_.~]/.test(c)) out += c;
    else out += `%${b.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return out;
}

/// A [`DataStore`] backed by a guest adapter. The stock `DataService` runs
/// unchanged on top — schema validation, ETags, `.schemas`, the store
/// contract stay in the host. The `tenant` argument is ignored — the
/// adapter is already the tenant's, connected to the tenant's backend.
export class GuestDataStore implements DataStore {
  constructor(private readonly inner: AdapterCaller) {}

  static fromConfig(adapterRef: string, storeConfig: JsonObject, wiring: GuestAdapterWiring): GuestDataStore {
    return new GuestDataStore(new ResidentAdapter("data", adapterRef, storeConfig, wiring));
  }

  async get(_tenant: string, dataset: string, key: string): Promise<Json> {
    const [status, body] = await this.inner.call("GET", `/${dataset}/${key}`);
    if (ok(status)) return body;
    throw storeError(status, body);
  }

  async put(_tenant: string, dataset: string, key: string, value: Json): Promise<boolean> {
    const [status, body] = await this.inner.call("PUT", `/${dataset}/${key}`, value);
    if (status === 201) return true;
    if (status === 200) return false;
    throw storeError(status, body);
  }

  async delete(_tenant: string, dataset: string, key: string): Promise<void> {
    const [status, body] = await this.inner.call("DELETE", `/${dataset}/${key}`);
    if (status !== 200 && status !== 204) throw storeError(status, body);
  }

  async listKeys(_tenant: string, dataset: string, take: number, skip: number): Promise<[string[], number]> {
    const [status, body] = await this.inner.call("GET", `/${dataset}/?$take=${take}&$skip=${skip}`);
    if (!ok(status)) throw storeError(status, body);
    return [listingNames(body, false), listingTotal(body)];
  }

  async listDatasets(_tenant: string, take: number, skip: number): Promise<[string[], number]> {
    const [status, body] = await this.inner.call("GET", `/?$take=${take}&$skip=${skip}`);
    if (!ok(status)) throw storeError(status, body);
    return [listingNames(body, true), listingTotal(body)];
  }

  async getSchema(_tenant: string, dataset: string): Promise<Json | undefined> {
    const [status, body] = await this.inner.call("GET", `/${dataset}/.schema.json`);
    if (ok(status)) return body;
    if (status === 404) return undefined;
    throw storeError(status, body);
  }

  async putSchema(_tenant: string, dataset: string, schema: Json): Promise<void> {
    const [status, body] = await this.inner.call("PUT", `/${dataset}/.schema.json`, schema);
    if (!ok(status)) throw storeError(status, body);
  }

  async deleteDataset(_tenant: string, dataset: string): Promise<void> {
    const [status, body] = await this.inner.call("DELETE", `/${dataset}/?confirm=${dataset}`);
    if (status !== 200 && status !== 204) throw storeError(status, body);
  }

  /// The Rust trait default: key-walk the dataset through the adapter.
  async scanMatching(tenant: string, dataset: string, keep: (v: Json) => boolean): Promise<Array<[string, Json]>> {
    const [keys] = await this.listKeys(tenant, dataset, Number.MAX_SAFE_INTEGER, 0);
    const out: Array<[string, Json]> = [];
    for (const key of keys) {
      const record = await this.get(tenant, dataset, key);
      if (keep(record)) out.push([key, record]);
    }
    return out;
  }

  async listRecords(tenant: string, dataset: string, spec: ListSpec): Promise<[Array<[string, Json]>, number]> {
    // Never forward `$select`/`$sort` to a bundle that hasn't advertised
    // `"list-records"` — it would misread them as a plain listing. The
    // key-walk fallback over the guest's own get/listKeys is always
    // correct (and is what the flag reads before the lazy first call).
    if (!this.inner.hasFeature("list-records")) {
      return listRecordsFallback(this, tenant, dataset, spec);
    }
    const select = spec.fields.map((f) => queryEncode(f.dotted())).join(",");
    let path = `/${dataset}/?$select=${select}`;
    if (spec.sort.length > 0) {
      const sort = spec.sort
        .map(([p, dir]) => `${dir === "desc" ? "-" : ""}${queryEncode(p.dotted())}`)
        .join(",");
      path += `&$sort=${sort}`;
    }
    path += `&$take=${spec.take}&$skip=${spec.skip}`;
    const [status, body] = await this.inner.call("GET", path);
    if (!ok(status)) throw storeError(status, body);
    const entries = body && typeof body === "object" && !Array.isArray(body) && Array.isArray(body.entries) ? body.entries : [];
    const page: Array<[string, Json]> = [];
    for (const e of entries) {
      if (!e || typeof e !== "object" || Array.isArray(e)) continue;
      const name = typeof e.name === "string" ? e.name : "";
      const fields = e.fields !== undefined && e.fields !== null ? e.fields : {};
      page.push([name, fields]);
    }
    return [page, listingTotal(body)];
  }

  listingPushdown(): boolean {
    // Reflects the bundle's `features` export; reads `false` until the
    // lazy first call has read the export.
    return this.inner.hasFeature("list-records");
  }
}

/// A [`QueryStore`] backed by a guest adapter: the query service
/// substitutes JSON templates / validates params first (unchanged), then
/// `runQuery` ships `{query, params, take, skip}` to the adapter as
/// `POST /query` and reads `{rows, total}` back.
export class GuestQueryStore implements QueryStore {
  constructor(private readonly inner: AdapterCaller) {}

  static fromConfig(adapterRef: string, storeConfig: JsonObject, wiring: GuestAdapterWiring): GuestQueryStore {
    return new GuestQueryStore(new ResidentAdapter("query", adapterRef, storeConfig, wiring));
  }

  async runQuery(_tenant: string, query: Json, params: JsonObject, take: number, skip: number): Promise<[Json[], number]> {
    const [status, resp] = await this.inner.call("POST", "/query", { query, params, take, skip });
    if (!ok(status)) throw storeError(status, resp);
    const obj = resp && typeof resp === "object" && !Array.isArray(resp) ? resp : {};
    const rows = Array.isArray(obj.rows) ? obj.rows : [];
    const total = typeof obj.total === "number" && Number.isFinite(obj.total) ? Math.floor(obj.total) : rows.length;
    return [rows, total];
  }

  quote(value: Json): string {
    // `quote` is synchronous, so it can't round-trip to the isolate;
    // adapter-specific quoting uses the same scalar default as the
    // reference adapter — SQL adapters bind via params and never hit it.
    return scalarQuote(value);
  }
}

/// Default scalar quoting for embedded string positions of a JSON template
/// (matches the reference `MemQueryStore`): scalars stringify, composites
/// are rejected rather than silently splicing structure into a string.
export function scalarQuote(value: Json): string {
  if (typeof value === "string") return value;
  if (typeof value === "number") return String(value);
  if (typeof value === "boolean") return String(value);
  const kind = value === null ? "null" : Array.isArray(value) ? "array" : "object";
  throw RsError.badRequest(`cannot splice a ${kind} into a string query position`);
}

/// A [`FileStore`] backed by a guest adapter. The adapter implements the
/// store pattern over its backend (`GET/PUT/DELETE /{path}`, `HEAD`,
/// `MOVE`, container listings `GET /{path}/`); file contents cross the
/// message boundary **base64-encoded** (the envelope is JSON), so this
/// path materializes — a streaming or presigned-redirect mode is a later
/// optimization. Conditional writes use the interface defaults
/// (best-effort check-then-write over `currentEtag`), exactly as the Rust
/// trait defaults apply to `GuestFileStore`.
export class GuestFileStore implements FileStore {
  constructor(
    private readonly inner: AdapterCaller,
    private readonly materializeCap: number,
  ) {}

  static fromConfig(adapterRef: string, storeConfig: JsonObject, wiring: GuestAdapterWiring): GuestFileStore {
    return new GuestFileStore(
      new ResidentAdapter("file", adapterRef, storeConfig, wiring),
      wiring.materializedBodyBytes,
    );
  }

  async head(_tenant: string, path: string): Promise<FileMeta> {
    const [status, body] = await this.inner.call("HEAD", path);
    if (!ok(status)) throw storeError(status, body);
    const obj = body && typeof body === "object" && !Array.isArray(body) ? body : {};
    return {
      size: typeof obj.size === "number" && obj.size >= 0 ? Math.floor(obj.size) : 0,
      lastModified: undefined,
      isDir: obj.isDir === true,
    };
  }

  async read(_tenant: string, path: string, range: ByteRange | undefined): Promise<Body> {
    const [status, body] = await this.inner.call("GET", path);
    if (!ok(status)) throw storeError(status, body);
    const obj = body && typeof body === "object" && !Array.isArray(body) ? body : {};
    const b64 = typeof obj.contentBase64 === "string" ? obj.contentBase64 : "";
    let bytes: Uint8Array;
    try {
      const bin = atob(b64);
      bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    } catch (e) {
      throw RsError.contractViolation(`adapter returned invalid base64: ${e instanceof Error ? e.message : String(e)}`);
    }
    const mediaType = typeof obj.mediaType === "string" ? MediaType.parse(obj.mediaType) : MediaType.forPath(path);
    // The whole-file version is the ETag; a Range serves the slice. (The
    // reference adapter returns the full body and we slice host-side; a
    // backend with native range support would push the range down.)
    const version = (await sha256Hex(bytes)).slice(0, 16);
    const total = bytes.length;
    let slice = bytes;
    if (range) {
      const start = Math.min(range.start, total);
      const end = range.end !== undefined ? Math.min(range.end + 1, total) : total;
      if (start >= end) {
        throw new RsError(
          416,
          codes.BAD_REQUEST,
          "Range Not Satisfiable",
          `range start ${start} beyond resource size ${total}`,
        );
      }
      slice = bytes.slice(start, end);
    }
    const out = Body.fromBytes(slice, mediaType);
    out.provenance = { kind: "replayable", url: path, version };
    return out;
  }

  async write(_tenant: string, path: string, body: Body): Promise<boolean> {
    const mediaType = body.mediaType.toString();
    const bytes = await body.materialize(this.materializeCap);
    let bin = "";
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]!);
    const payload = { contentBase64: btoa(bin), mediaType };
    const [status, resp] = await this.inner.call("PUT", path, payload);
    if (status === 201) return true;
    if (status === 200) return false;
    throw storeError(status, resp);
  }

  currentEtag(tenant: string, path: string): Promise<string | undefined> {
    return currentEtagFromRead(this, tenant, path);
  }

  writeCond(tenant: string, path: string, body: Body, precondition: WritePrecondition): Promise<WriteOutcome> {
    return writeCondDefault(this, tenant, path, body, precondition);
  }

  conditionalWriteAtomic(): boolean {
    // Best-effort check-then-write, like the Rust trait default on
    // `GuestFileStore` — the adapter's backend offers no compare-and-swap.
    return false;
  }

  async delete(_tenant: string, path: string): Promise<void> {
    const [status, body] = await this.inner.call("DELETE", path);
    if (status !== 200 && status !== 204) throw storeError(status, body);
  }

  deleteCond(tenant: string, path: string, precondition: WritePrecondition): Promise<void> {
    return deleteCondDefault(this, tenant, path, precondition);
  }

  async rename(_tenant: string, from: string, to: string): Promise<boolean> {
    const [status, body] = await this.inner.call("MOVE", from, { to });
    if (status === 201) return true;
    if (status === 200) return false;
    throw storeError(status, body);
  }

  async deleteDir(_tenant: string, path: string): Promise<void> {
    const [status, body] = await this.inner.call("DELETE", path);
    if (status !== 200 && status !== 204) throw storeError(status, body);
  }

  async deleteDirAll(_tenant: string, path: string): Promise<void> {
    const [status, body] = await this.inner.call("DELETE", path, { recursive: true });
    if (status !== 200 && status !== 204) throw storeError(status, body);
  }

  async list(_tenant: string, path: string, take: number, skip: number): Promise<[DirEntry[], number]> {
    const sep = path.includes("?") ? "&" : "?";
    const [status, body] = await this.inner.call("GET", `${path}${sep}$take=${take}&$skip=${skip}`);
    if (!ok(status)) throw storeError(status, body);
    const obj = body && typeof body === "object" && !Array.isArray(body) ? body : {};
    const raw = Array.isArray(obj.entries) ? obj.entries : [];
    const entries: DirEntry[] = [];
    for (const e of raw) {
      if (!e || typeof e !== "object" || Array.isArray(e)) continue;
      const entry: DirEntry = {
        name: typeof e.name === "string" ? e.name : "",
        size: typeof e.size === "number" && e.size >= 0 ? Math.floor(e.size) : 0,
        dir: e.dir === true,
      };
      if (typeof e.lastModified === "string") entry.lastModified = e.lastModified;
      if (typeof e.contentType === "string") entry.contentType = e.contentType;
      entries.push(entry);
    }
    return [entries, listingTotal(body)];
  }
}

/// An [`SmsGateway`] backed by a guest adapter: `send`/`status` map to
/// `POST /send` / `GET /status/{id}` envelopes (Rust `GuestSmsGateway`).
export class GuestSmsGateway implements SmsGateway {
  constructor(private readonly inner: AdapterCaller) {}

  static fromConfig(adapterRef: string, storeConfig: JsonObject, wiring: GuestAdapterWiring): GuestSmsGateway {
    return new GuestSmsGateway(new ResidentAdapter("sms", adapterRef, storeConfig, wiring));
  }

  async send(_tenant: string, to: string, body: string): Promise<string> {
    const [status, resp] = await this.inner.call("POST", "/send", { to, body });
    if (!ok(status)) throw storeError(status, resp);
    const id = resp && typeof resp === "object" && !Array.isArray(resp) ? resp.id : undefined;
    if (typeof id !== "string") throw RsError.contractViolation("sms adapter response missing string 'id'");
    return id;
  }

  async status(_tenant: string, id: string): Promise<Json> {
    const [status, resp] = await this.inner.call("GET", `/status/${id}`);
    if (ok(status)) return resp;
    throw storeError(status, resp);
  }
}
