// Media types (PRD §6.2). Port of `rs2-core/src/message/media_type.rs`:
// every body has one; JSON bodies may carry a schema reference serialized
// as `application/json; schema="<url>"`.

export const DIR_JSON = "application/vnd.rs2.dir+json";
export const OCTET_STREAM = "application/octet-stream";
export const JSON_TYPE = "application/json";
export const SCHEMA_JSON = "application/schema+json";
export const PROBLEM_JSON = "application/problem+json";

import {
  CANONICAL_EXTENSIONS,
  EXTENSION_TABLE,
  NEGOTIABLE_EXTENSIONS,
} from "./media-type-table";

/// Index of `key` in a table sorted by its first column, or -1. The tables run
/// to ~1200 rows and a directory listing types one path per entry, so lookup
/// is a binary search rather than a scan (and needs no startup Map).
function lookup(table: ReadonlyArray<readonly [string, string]>, key: string): string | undefined {
  let lo = 0;
  let hi = table.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const at = table[mid]![0];
    if (at === key) return table[mid]![1];
    if (at < key) lo = mid + 1;
    else hi = mid - 1;
  }
  return undefined;
}

export class MediaType {
  /// The essence, e.g. `application/json` — always lowercase, no params.
  private readonly _essence: string;
  private _schema: string | undefined;

  constructor(essence: string, schema?: string) {
    this._essence = essence.trim().toLowerCase();
    this._schema = schema;
  }

  static json(): MediaType {
    return new MediaType(JSON_TYPE);
  }
  static octetStream(): MediaType {
    return new MediaType(OCTET_STREAM);
  }
  static dirJson(): MediaType {
    return new MediaType(DIR_JSON);
  }

  withSchema(schemaUrl: string): MediaType {
    return new MediaType(this._essence, schemaUrl);
  }

  /// Parse a `Content-Type` header value, extracting the `schema` parameter.
  static parse(headerValue: string): MediaType {
    const parts = headerValue.split(";");
    const essence = (parts[0] ?? OCTET_STREAM).trim().toLowerCase();
    let schema: string | undefined;
    for (const param of parts.slice(1)) {
      const i = param.indexOf("=");
      if (i < 0) continue;
      const name = param.slice(0, i).trim();
      const value = param.slice(i + 1).trim();
      if (name.toLowerCase() === "schema") {
        schema = value.replace(/^"+|"+$/g, "");
      }
    }
    return new MediaType(essence, schema);
  }

  essence(): string {
    return this._essence;
  }
  schema(): string | undefined {
    return this._schema;
  }
  isJson(): boolean {
    return this._essence.includes("/json") || this._essence.includes("+json");
  }
  isText(): boolean {
    const TEXT_TYPES = [
      "text/",
      "application/javascript",
      "application/typescript",
      "application/xml",
      "application/xhtml+xml",
    ];
    return TEXT_TYPES.some((t) => this._essence.startsWith(t));
  }
  isZip(): boolean {
    return this._essence.startsWith("application/") && this._essence.includes("zip");
  }
  isDir(): boolean {
    return this._essence === DIR_JSON;
  }

  /// Media type from a file extension; `undefined` if unknown.
  static fromExtension(ext: string): MediaType | undefined {
    const essence = lookup(EXTENSION_TABLE, ext.replace(/^\.+/, "").toLowerCase());
    return essence === undefined ? undefined : new MediaType(essence);
  }

  /// The `[extension-without-dot, media type]` pairs the file service probes
  /// for an extension-less request, in friendly-URL preference order.
  ///
  /// Deliberately a short list, not the whole map: every candidate costs a
  /// store probe on a miss, and nobody writes `/clip` meaning `/clip.mp4`.
  /// Bulk media is served when addressed by name and never negotiated.
  static knownExtensions(): Array<[string, MediaType]> {
    return NEGOTIABLE_EXTENSIONS.map((ext) => [
      ext,
      MediaType.fromExtension(ext) ?? MediaType.octetStream(),
    ]);
  }

  /// The canonical extension for this essence — the reverse of the extension
  /// map, used to name a server-named file (keyless POST) from its declared
  /// media type. Every entry round-trips (the extension maps back to this
  /// essence), so the file that gets named is served as what was posted.
  /// `undefined` when no extension claims the essence.
  canonicalExtension(): string | undefined {
    return lookup(CANONICAL_EXTENSIONS, this._essence);
  }

  /// Media type for a stored file path (extension map), falling back to
  /// `application/octet-stream` — sniffing is never used.
  static forPath(path: string): MediaType {
    const i = path.lastIndexOf(".");
    if (i < 0) return MediaType.octetStream();
    return MediaType.fromExtension(path.slice(i + 1)) ?? MediaType.octetStream();
  }

  equals(other: MediaType): boolean {
    return this._essence === other._essence && this._schema === other._schema;
  }

  /// Wire form, including the `schema` parameter when present.
  toString(): string {
    return this._schema !== undefined ? `${this._essence}; schema="${this._schema}"` : this._essence;
  }
}
