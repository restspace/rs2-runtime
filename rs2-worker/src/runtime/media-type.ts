// Media types (PRD §6.2). Port of `rs2-core/src/message/media_type.rs`:
// every body has one; JSON bodies may carry a schema reference serialized
// as `application/json; schema="<url>"`.

export const DIR_JSON = "application/vnd.rs2.dir+json";
export const OCTET_STREAM = "application/octet-stream";
export const JSON_TYPE = "application/json";
export const SCHEMA_JSON = "application/schema+json";
export const PROBLEM_JSON = "application/problem+json";

/// Known `(extension, essence)` pairs, in friendly-URL preference order.
/// The single source for `fromExtension` and `knownExtensions`.
const EXTENSION_TABLE: ReadonlyArray<readonly [string, string]> = [
  ["html", "text/html"],
  ["htm", "text/html"],
  ["md", "text/markdown"],
  ["txt", "text/plain"],
  ["json", JSON_TYPE],
  ["xml", "application/xml"],
  ["yaml", "application/yaml"],
  ["yml", "application/yaml"],
  ["csv", "text/csv"],
  ["css", "text/css"],
  ["js", "text/javascript"],
  ["mjs", "text/javascript"],
  ["jsx", "text/javascript"],
  ["ts", "application/typescript"],
  ["tsx", "application/typescript"],
  ["svg", "image/svg+xml"],
  ["png", "image/png"],
  ["jpg", "image/jpeg"],
  ["jpeg", "image/jpeg"],
  ["gif", "image/gif"],
  ["webp", "image/webp"],
  ["pdf", "application/pdf"],
  ["zip", "application/zip"],
  ["wasm", "application/wasm"],
];

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
    const e = ext.replace(/^\.+/, "").toLowerCase();
    const hit = EXTENSION_TABLE.find(([x]) => x === e);
    return hit ? new MediaType(hit[1]) : undefined;
  }

  /// Every known `(extension-without-dot, media type)` pair, in friendly-URL
  /// preference order.
  static knownExtensions(): Array<[string, MediaType]> {
    return EXTENSION_TABLE.map(([ext, essence]) => [ext, new MediaType(essence)]);
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
