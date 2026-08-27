// Typed fetch wrapper for the conformance runner. Black box: it only speaks
// HTTP to `RS2_BASE_URL`. Every suite and helper goes through `Rs2Client`,
// so the runner has exactly one place that knows about the base URL, the
// `Host` header, and bearer tokens.

import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/** Runner parameters (spec F.1), read once from the environment. */
export interface RunnerEnv {
  baseUrl: string;
  /** Value sent as `Host` (and the CORS same-origin host). */
  host: string;
  /** Tenant name expected in problem bodies. */
  tenant: string;
  adminEmail: string;
  adminPassword: string;
  /** Worker only: enables `globalSetup` seeding through `/admin/tenants`. */
  adminToken: string | undefined;
  hostKind: HostKind;
  /** Path of the JS echo bundle. */
  codeBundle: string;
  /** Worker only: a second tenant's host for tenant-isolation cases. */
  secondHost: string | undefined;
}

export type HostKind = "rust" | "cloudflare";

const HERE = dirname(fileURLToPath(import.meta.url));
/** `conformance/http/` — the package root, independent of the cwd. */
export const PACKAGE_ROOT = resolve(HERE, "..");

let cached: RunnerEnv | undefined;

/** The runner environment. `RS2_PORT` alone is enough for a local run. */
export function env(): RunnerEnv {
  if (cached) return cached;
  const port = process.env.RS2_PORT || "3100";
  const baseUrl = (process.env.RS2_BASE_URL || `http://127.0.0.1:${port}`).replace(/\/+$/, "");
  const kind = (process.env.RS2_HOST_KIND || "rust") as HostKind;
  if (kind !== "rust" && kind !== "cloudflare") {
    throw new Error(`RS2_HOST_KIND must be 'rust' or 'cloudflare', got '${kind}'`);
  }
  cached = {
    baseUrl,
    host: process.env.RS2_HOST || new URL(baseUrl).host,
    tenant: process.env.RS2_TENANT || "conf",
    adminEmail: process.env.RS2_ADMIN_EMAIL || "admin@conf.test",
    adminPassword: process.env.RS2_ADMIN_PASSWORD || "conf-admin-pw",
    adminToken: process.env.RS2_ADMIN_TOKEN || undefined,
    hostKind: kind,
    codeBundle: process.env.RS2_CODE_BUNDLE || resolve(PACKAGE_ROOT, "fixtures", "echo.js"),
    secondHost: process.env.RS2_SECOND_HOST || undefined,
  };
  if (!existsSync(cached.codeBundle)) {
    throw new Error(`RS2_CODE_BUNDLE '${cached.codeBundle}' does not exist`);
  }
  return cached;
}

/** RFC 9457 problem body as RS2 emits it (`error.rs`). */
export interface Problem {
  type: string;
  title: string;
  status: number;
  detail?: string;
  code: string;
  tenant: string;
  traceId: string;
  retryable?: boolean;
  retryAfterMs?: number;
  [extra: string]: unknown;
}

/** One `application/vnd.rs2.dir+json` listing entry. */
export interface DirEntry {
  name: string;
  dir: boolean;
  size?: number;
  lastModified?: string;
  contentType?: string;
  fields?: Record<string, unknown>;
  mountedAt?: string[];
  fixed?: boolean;
  [extra: string]: unknown;
}

export interface DirListing {
  path: string;
  entries: DirEntry[];
  total: number;
}

export type BodyInit = string | Uint8Array | ArrayBuffer | ReadableStream<Uint8Array>;

export interface RequestOptions {
  /** Raw body; set `contentType` alongside it. */
  body?: BodyInit;
  /** JSON body (serialized; sets `Content-Type: application/json` unless overridden). */
  json?: unknown;
  contentType?: string;
  /** Extra request headers; override anything the client would set. */
  headers?: Record<string, string>;
  /** Override the bearer for this request only (`null` sends none). */
  token?: string | null;
  /** Follow redirects? Defaults to `manual` so 301/302 are observable. */
  redirect?: RequestRedirect;
  /** Send a `Host` header other than the client's default. */
  host?: string;
}

/**
 * A fully-buffered response. Bodies are read eagerly so a test can look at
 * status and headers, then decide how to decode — conformance bodies are
 * small; use `Rs2Client.stream` for the streaming cases.
 */
export class Rs2Response {
  constructor(
    readonly status: number,
    readonly headers: Headers,
    readonly bytes: Uint8Array,
    readonly method: string,
    readonly path: string,
  ) {}

  header(name: string): string | null {
    return this.headers.get(name);
  }

  /**
   * The `ETag` header (quoted), or `null` — **canonicalized to the strong
   * form**: a `W/` prefix is stripped.
   *
   * Not a host difference to assert on. An RS2 host always emits the strong
   * tag, but an intermediary that recompresses the response rewrites it to
   * `W/"v"` — Cloudflare does exactly that whenever it compresses, so a
   * deployed Worker's ETags reach a gzip-accepting client weakened while the
   * same host behind `wrangler dev` hands them over strong. The version
   * inside is identical either way, and it is the version every assertion
   * here is about. That a host must *accept* the weak form back in
   * `If-Match` is a contract statement, and it has its own test
   * (m3-surface: "config If-Match accepts the weak form of the version").
   */
  etag(): string | null {
    const raw = this.headers.get("etag");
    return raw === null ? null : raw.replace(/^W\//, "");
  }

  /** `Content-Type` essence (lowercased, parameters stripped). */
  contentType(): string {
    return (this.headers.get("content-type") || "").split(";")[0].trim().toLowerCase();
  }

  text(): string {
    return new TextDecoder().decode(this.bytes);
  }

  json<T = any>(): T {
    const text = this.text();
    try {
      return JSON.parse(text) as T;
    } catch (e) {
      throw new Error(
        `${this.method} ${this.path} -> ${this.status}: body is not JSON (${this.contentType()}): ${text.slice(0, 300)}`,
      );
    }
  }

  /** The body as a problem+json document (throws if it is not one). */
  problem(): Problem {
    if (this.contentType() !== "application/problem+json") {
      throw new Error(
        `${this.method} ${this.path} -> ${this.status}: expected application/problem+json, got '${this.contentType()}': ${this.text().slice(0, 300)}`,
      );
    }
    return this.json<Problem>();
  }

  /** The body as a directory listing (asserts the media type). */
  listing(): DirListing {
    if (this.contentType() !== "application/vnd.rs2.dir+json") {
      throw new Error(
        `${this.method} ${this.path} -> ${this.status}: expected application/vnd.rs2.dir+json, got '${this.contentType()}': ${this.text().slice(0, 300)}`,
      );
    }
    return this.json<DirListing>();
  }

  /** `X-Total-Count` as a number (throws when absent). */
  totalCount(): number {
    const raw = this.headers.get("x-total-count");
    if (raw === null) throw new Error(`${this.method} ${this.path}: no X-Total-Count header`);
    return Number(raw);
  }

  /** Short description for assertion messages. */
  describe(): string {
    return `${this.method} ${this.path} -> ${this.status} ${this.text().slice(0, 400)}`;
  }
}

export interface ClientOptions {
  baseUrl?: string;
  host?: string;
  token?: string;
}

/** A client bound to a base URL, a `Host` value, and (optionally) a bearer. */
export class Rs2Client {
  readonly baseUrl: string;
  readonly host: string;
  readonly token: string | undefined;

  constructor(opts: ClientOptions = {}) {
    const e = env();
    this.baseUrl = opts.baseUrl ?? e.baseUrl;
    this.host = opts.host ?? e.host;
    this.token = opts.token;
  }

  /** The same client with a bearer token attached. */
  withToken(token: string | undefined): Rs2Client {
    return new Rs2Client({ baseUrl: this.baseUrl, host: this.host, token });
  }

  /** The same client sending a different `Host`. */
  withHost(host: string): Rs2Client {
    return new Rs2Client({ baseUrl: this.baseUrl, host, token: this.token });
  }

  private buildRequest(method: string, path: string, opts: RequestOptions): [string, RequestInit] {
    const url = this.baseUrl + path;
    const headers = new Headers();
    headers.set("host", opts.host ?? this.host);
    const token = opts.token === undefined ? this.token : opts.token;
    if (token) headers.set("authorization", `Bearer ${token}`);
    let body: globalThis.BodyInit | undefined;
    if (opts.json !== undefined) {
      body = JSON.stringify(opts.json);
      headers.set("content-type", opts.contentType ?? "application/json");
    } else if (opts.body !== undefined) {
      body = opts.body as globalThis.BodyInit;
      if (opts.contentType) headers.set("content-type", opts.contentType);
    }
    for (const [k, v] of Object.entries(opts.headers ?? {})) headers.set(k, v);
    const init: RequestInit & { duplex?: string } = {
      method,
      headers,
      body,
      redirect: opts.redirect ?? "manual",
    };
    if (body instanceof ReadableStream) init.duplex = "half";
    return [url, init];
  }

  /** Perform a request and buffer the response. */
  async request(method: string, path: string, opts: RequestOptions = {}): Promise<Rs2Response> {
    const [url, init] = this.buildRequest(method, path, opts);
    const res = await fetch(url, init);
    const bytes = new Uint8Array(await res.arrayBuffer());
    return new Rs2Response(res.status, res.headers, bytes, method, path);
  }

  /** Perform a request and hand back the raw `Response` (streaming body). */
  async stream(method: string, path: string, opts: RequestOptions = {}): Promise<Response> {
    const [url, init] = this.buildRequest(method, path, opts);
    return fetch(url, init);
  }

  get(path: string, opts?: RequestOptions) {
    return this.request("GET", path, opts);
  }
  head(path: string, opts?: RequestOptions) {
    return this.request("HEAD", path, opts);
  }
  put(path: string, opts?: RequestOptions) {
    return this.request("PUT", path, opts);
  }
  post(path: string, opts?: RequestOptions) {
    return this.request("POST", path, opts);
  }
  patch(path: string, opts?: RequestOptions) {
    return this.request("PATCH", path, opts);
  }
  delete(path: string, opts?: RequestOptions) {
    return this.request("DELETE", path, opts);
  }
  options(path: string, opts?: RequestOptions) {
    return this.request("OPTIONS", path, opts);
  }
  move(path: string, destination: string, opts: RequestOptions = {}) {
    return this.request("MOVE", path, {
      ...opts,
      headers: { destination, ...(opts.headers ?? {}) },
    });
  }

  /** GET a JSON document; throws unless 2xx. */
  async getJson<T = any>(path: string, opts?: RequestOptions): Promise<T> {
    const res = await this.get(path, opts);
    if (res.status < 200 || res.status >= 300) throw new Error(`getJson: ${res.describe()}`);
    return res.json<T>();
  }

  /**
   * GET a directory listing. Returns the parsed listing plus the header
   * total; throws unless the response is a 200 dir+json.
   */
  async listDir(path: string, opts?: RequestOptions): Promise<{ listing: DirListing; total: number; res: Rs2Response }> {
    const res = await this.get(path, opts);
    if (res.status !== 200) throw new Error(`listDir: ${res.describe()}`);
    return { listing: res.listing(), total: res.totalCount(), res };
  }
}

// ---- ETag helpers ---------------------------------------------------------

/** Strip a `W/` prefix and the surrounding quotes. */
export function etagValue(etag: string): string {
  return etag.replace(/^W\//, "").replace(/^"|"$/g, "");
}

/** Quote a bare validator (idempotent on an already-quoted one). */
export function quoteEtag(value: string): string {
  return value.startsWith('"') ? value : `"${value}"`;
}

/** Weak form of an ETag. */
export function weakEtag(etag: string): string {
  return etag.startsWith("W/") ? etag : `W/${quoteEtag(etag)}`;
}

/** Listing entry names in order. */
export function names(listing: DirListing): string[] {
  return listing.entries.map((e) => e.name);
}

/** Find a listing entry by name, ignoring a trailing `/` on directories. */
export function entryNamed(listing: DirListing, name: string): DirEntry | undefined {
  const leaf = name.replace(/\/$/, "");
  return listing.entries.find((e) => e.name.replace(/\/$/, "") === leaf);
}

/** Percent-encode a path segment the way a browser would (keeps `@`, `.`). */
export function seg(s: string): string {
  return encodeURIComponent(s).replace(/%40/g, "@");
}
