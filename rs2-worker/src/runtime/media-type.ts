// Media types (PRD §6.2). Port of `rs2-core/src/message/media_type.rs`:
// every body has one; JSON bodies may carry a schema reference serialized
// as `application/json; schema="<url>"`.

export const DIR_JSON = "application/vnd.rs2.dir+json";
export const OCTET_STREAM = "application/octet-stream";
export const JSON_TYPE = "application/json";
export const SCHEMA_JSON = "application/schema+json";
export const PROBLEM_JSON = "application/problem+json";

/// Known `(extension, essence, negotiable)` triples, in friendly-URL
/// preference order. The single source for `fromExtension` and
/// `knownExtensions`.
///
/// `negotiable` marks the extensions the file service probes for an
/// extension-less request. Bulk media (video, audio, fonts) maps to a media
/// type when addressed by name but is never a friendly-URL candidate: nobody
/// writes `/clip` meaning `/clip.mp4`, and every candidate costs a store probe
/// on a miss.
const EXTENSION_TABLE: ReadonlyArray<readonly [string, string, boolean]> = [
  ["html", "text/html", true],
  ["htm", "text/html", true],
  ["md", "text/markdown", true],
  ["txt", "text/plain", true],
  ["json", JSON_TYPE, true],
  ["xml", "application/xml", true],
  ["yaml", "application/yaml", true],
  ["yml", "application/yaml", true],
  ["csv", "text/csv", true],
  ["css", "text/css", true],
  ["js", "text/javascript", true],
  ["mjs", "text/javascript", true],
  ["jsx", "text/javascript", true],
  ["ts", "application/typescript", true],
  ["tsx", "application/typescript", true],
  ["svg", "image/svg+xml", true],
  ["png", "image/png", true],
  ["jpg", "image/jpeg", true],
  ["jpeg", "image/jpeg", true],
  ["gif", "image/gif", true],
  ["webp", "image/webp", true],
  ["pdf", "application/pdf", true],
  ["zip", "application/zip", true],
  ["wasm", "application/wasm", true],
  // Images addressed by name only.
  ["avif", "image/avif", false],
  ["ico", "image/vnd.microsoft.icon", false],
  ["bmp", "image/bmp", false],
  ["tif", "image/tiff", false],
  ["tiff", "image/tiff", false],
  // Video.
  ["mp4", "video/mp4", false],
  ["m4v", "video/mp4", false],
  ["webm", "video/webm", false],
  ["ogv", "video/ogg", false],
  ["mov", "video/quicktime", false],
  ["mkv", "video/x-matroska", false],
  ["avi", "video/x-msvideo", false],
  ["mpeg", "video/mpeg", false],
  ["mpg", "video/mpeg", false],
  // Audio.
  ["mp3", "audio/mpeg", false],
  ["m4a", "audio/mp4", false],
  ["aac", "audio/aac", false],
  ["ogg", "audio/ogg", false],
  ["oga", "audio/ogg", false],
  ["opus", "audio/ogg", false],
  ["weba", "audio/webm", false],
  ["wav", "audio/wav", false],
  ["flac", "audio/flac", false],
  // Fonts.
  ["woff", "font/woff", false],
  ["woff2", "font/woff2", false],
  ["ttf", "font/ttf", false],
  ["otf", "font/otf", false],
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

  /// Every negotiable `(extension-without-dot, media type)` pair, in
  /// friendly-URL preference order.
  static knownExtensions(): Array<[string, MediaType]> {
    return EXTENSION_TABLE.filter(([, , negotiable]) => negotiable).map(([ext, essence]) => [
      ext,
      new MediaType(essence),
    ]);
  }

  /// The canonical extension for an essence — the reverse of the extension
  /// map, used to name a server-named file (keyless POST) from its declared
  /// media type. The first table entry wins, so `image/jpeg` is `jpg`, not
  /// `jpeg`. `undefined` when nothing in the table claims the essence.
  canonicalExtension(): string | undefined {
    return EXTENSION_TABLE.find(([, essence]) => essence === this._essence)?.[0];
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
