// `file` service (PRD §10.1): streamed file storage over a `FileStore`
// capability. Port of `rs2-core/src/services/file.rs` — decision order,
// status codes, and headers verbatim.

import type { ScopedFileStore } from "../capabilities/scoped";
import type { ByteRange } from "../capabilities/types";
import { dirEntryJson } from "../capabilities/types";
import { Body } from "../runtime/body";
import { RsError, codes } from "../runtime/error";
import type { Json, JsonObject } from "../runtime/error";
import { MetaSort } from "../runtime/listing";
import { DIR_JSON, MediaType } from "../runtime/media-type";
import { Message, simpleUuid } from "../runtime/message";
import { appendHeaderValue, isOperator } from "../runtime/wrapper";
import { ifNoneMatchHits, pagination, writePrecondition } from "./context";
import type { Service, ServiceContext } from "./context";

/// Static-site options on a file mount (`config_schema::FileConfig`).
interface SiteOptions {
  defaultResource: string | undefined;
  spaFallback: boolean;
  spaFallbackAll: boolean;
  listings: boolean;
  friendlyUrls: boolean;
  extensionPriority: string[];
}

function defaultSite(): SiteOptions {
  return {
    defaultResource: undefined,
    spaFallback: false,
    spaFallbackAll: false,
    listings: true,
    friendlyUrls: false,
    extensionPriority: [],
  };
}

/// `FileConfig` deserialization: any field of the wrong type fails the whole
/// parse, which falls back to defaults (`unwrap_or_default`).
function siteFromConfig(config: JsonObject): SiteOptions {
  const cfg = {
    defaultResource: undefined as string | undefined,
    spaFallback: false,
    spaFallbackAll: false,
    listings: true,
    friendlyUrls: false,
    extensionPriority: [] as string[],
  };
  const dr = config.defaultResource;
  if (dr !== undefined && dr !== null && typeof dr !== "string") return defaultSite();
  if (typeof dr === "string") cfg.defaultResource = dr;
  for (const k of ["spaFallback", "spaFallbackAll", "listings", "friendlyUrls"] as const) {
    const v = config[k];
    if (v !== undefined && typeof v !== "boolean") return defaultSite();
    if (typeof v === "boolean") cfg[k] = v;
  }
  const ep = config.extensionPriority;
  if (ep !== undefined) {
    if (!Array.isArray(ep) || !ep.every((x) => typeof x === "string")) return defaultSite();
    cfg.extensionPriority = ep as string[];
  }
  const store = config.store;
  if (store !== undefined && store !== null && (typeof store !== "object" || Array.isArray(store))) return defaultSite();
  const defaultResource = cfg.defaultResource ?? (cfg.spaFallback || cfg.spaFallbackAll ? "index.html" : undefined);
  return { ...cfg, defaultResource };
}

/// Extension for a server-named file from its declared media type (keyless
/// POST). Unknown types get no extension.
function extensionFor(mediaType: MediaType): string {
  switch (mediaType.essence()) {
    case "application/json":
      return ".json";
    case "text/plain":
      return ".txt";
    case "text/html":
      return ".html";
    case "text/css":
      return ".css";
    case "text/csv":
      return ".csv";
    case "application/javascript":
    case "text/javascript":
      return ".js";
    case "application/wasm":
      return ".wasm";
    case "image/png":
      return ".png";
    case "image/jpeg":
      return ".jpg";
    case "image/gif":
      return ".gif";
    case "image/svg+xml":
      return ".svg";
    case "application/pdf":
      return ".pdf";
    case "application/zip":
      return ".zip";
    default:
      return "";
  }
}

/// Whether `path`'s final segment carries no extension.
export function isExtensionless(path: string): boolean {
  const last = path.slice(path.lastIndexOf("/") + 1);
  return !last.includes(".");
}

/// Parse an `Accept` header into `(type, subtype, q)` ranges; malformed parts ignored.
function parseAccept(accept: string | undefined): Array<[string, string, number]> {
  if (accept === undefined) return [];
  const out: Array<[string, string, number]> = [];
  for (const part of accept.split(",")) {
    const params = part.split(";");
    const media = (params[0] ?? "").trim();
    const slash = media.indexOf("/");
    if (slash < 0) continue;
    let q = 1.0;
    for (const p of params.slice(1)) {
      const t = p.trim();
      if (t.startsWith("q=")) {
        const v = parseFloat(t.slice(2).trim());
        if (!Number.isNaN(v)) {
          q = v;
          break;
        }
      }
    }
    out.push([media.slice(0, slash).trim().toLowerCase(), media.slice(slash + 1).trim().toLowerCase(), q]);
  }
  return out;
}

/// Whether `Accept` explicitly names the directory-listing type with `q > 0`
/// (no wildcard match).
function wantsDirListing(accept: string | undefined): boolean {
  const [dtype, dsubtype] = DIR_JSON.split("/") as [string, string];
  return parseAccept(accept).some(([t, s, q]) => q > 0 && t === dtype && s === dsubtype);
}

function acceptMatchQ(essence: string, range: [string, string, number]): number {
  const [rtype, rsubtype, q] = range;
  const slash = essence.indexOf("/");
  const etype = slash < 0 ? essence : essence.slice(0, slash);
  const esubtype = slash < 0 ? "" : essence.slice(slash + 1);
  const matches = (rtype === "*" || rtype === etype) && (rsubtype === "*" || rsubtype === esubtype);
  return matches ? q : 0;
}

/// Order the servable extensions for an extension-less request by `Accept`
/// quality, the configured priority (then the built-in table) as the tiebreak.
export function negotiateExtensions(accept: string | undefined, priority: readonly string[]): string[] {
  const ranges = parseAccept(accept);
  const base: string[] = [];
  for (const raw of priority) {
    const ext = raw.replace(/^\.+/, "").toLowerCase();
    if (ext !== "" && !base.includes(ext)) base.push(ext);
  }
  for (const [ext] of MediaType.knownExtensions()) if (!base.includes(ext)) base.push(ext);
  const candidates: Array<[string, number]> = base.map((ext) => {
    const essence = (MediaType.fromExtension(ext) ?? MediaType.octetStream()).essence();
    const q = ranges.reduce((best, r) => Math.max(best, acceptMatchQ(essence, r)), 0);
    return [ext, q];
  });
  // Stable sort by descending quality; ties keep the base preference order.
  candidates.sort((a, b) => b[1] - a[1]);
  return candidates.map(([ext]) => ext);
}

function parseRange(header: string): ByteRange | undefined {
  if (!header.startsWith("bytes=")) return undefined;
  const spec = header.slice("bytes=".length);
  if (spec.includes(",")) return undefined;
  const dash = spec.indexOf("-");
  if (dash < 0) return undefined;
  const startStr = spec.slice(0, dash).trim();
  const endStr = spec.slice(dash + 1).trim();
  if (!/^\d+$/.test(startStr)) return undefined;
  if (endStr !== "" && !/^\d+$/.test(endStr)) return undefined;
  return { start: Number(startStr), end: endStr === "" ? undefined : Number(endStr) };
}

const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/// RFC 2822 layout as the Rust `time` crate emits it: `Wed, 26 Aug 2026
/// 10:00:00 +0000` (not `toUTCString()`'s IMF-fixdate `GMT`).
export function rfc2822(d: Date): string {
  const p = (n: number) => String(n).padStart(2, "0");
  return `${DAYS[d.getUTCDay()]}, ${p(d.getUTCDate())} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())} +0000`;
}

export class FileService implements Service {
  private readonly site: SiteOptions;

  constructor(site?: SiteOptions) {
    this.site = site ?? defaultSite();
  }

  static fromConfig(config: JsonObject): FileService {
    return new FileService(siteFromConfig(config));
  }

  /// Serve one stored file with Range/ETag/304 semantics.
  private async serveFile(
    template: Message,
    range: ByteRange | undefined,
    ifNoneMatch: string | undefined,
    files: ScopedFileStore,
    path: string,
  ): Promise<Message> {
    const body = await files.read(path, range);
    const resp = range ? template.response(206, body) : template.ok(body);
    resp.setHeader("accept-ranges", "bytes");
    const etag = body.provenance.kind === "replayable" ? `"${body.provenance.version}"` : undefined;
    const lastModified = body.lastModified ? rfc2822(body.lastModified) : undefined;
    if (etag !== undefined) resp.setHeader("etag", etag);
    if (lastModified !== undefined) resp.setHeader("last-modified", lastModified);
    if (etag !== undefined && ifNoneMatchHits(ifNoneMatch, etag)) {
      await body.intoStream().cancel().catch(() => undefined);
      const notModified = resp.response(304, undefined);
      notModified.setHeader("etag", etag);
      if (lastModified !== undefined) notModified.setHeader("last-modified", lastModified);
      notModified.setHeader("accept-ranges", "bytes");
      return notModified;
    }
    return resp;
  }

  /// Resolve an extension-less request to a stored file by probing the known
  /// servable extensions in `Accept`-negotiated order.
  private async resolveFriendly(
    path: string,
    accept: string | undefined,
    files: ScopedFileStore,
    priority: readonly string[],
  ): Promise<string | undefined> {
    for (const ext of negotiateExtensions(accept, priority)) {
      const candidate = `${path}.${ext}`;
      try {
        const meta = await files.head(candidate);
        if (meta.isDir) continue;
        return candidate;
      } catch (e) {
        if (e instanceof RsError && e.code === codes.NOT_FOUND) continue;
        throw e;
      }
    }
    return undefined;
  }

  /// 301 to the trailing-slash form of a directory addressed without one.
  private static slashRedirect(msg: Message): Message {
    const resp = msg.response(301, undefined);
    resp.setHeader("location", msg.url.query === "" ? `${msg.url.path}/` : `${msg.url.path}/?${msg.url.query}`);
    return resp;
  }

  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const files = ctx.files;
    if (!files) throw RsError.capabilityDenied("files");
    const path = msg.url.servicePath;
    const site = this.site;
    const rangeHeader = msg.header("range");
    const range = rangeHeader !== undefined ? parseRange(rangeHeader) : undefined;
    const ifNoneMatch = msg.header("if-none-match");
    const accept = msg.header("accept");

    // MOVE renames a file within this store (the `move` facet).
    if (msg.method === "MOVE") {
      if (msg.url.isDirectory()) throw RsError.badRequest("MOVE source must be a file, not a directory");
      const destRaw = msg.header("destination");
      if (destRaw === undefined) throw RsError.badRequest("MOVE requires a 'Destination' header");
      const base = msg.url.basePath;
      const rel = destRaw.startsWith(base) ? destRaw.slice(base.length) : destRaw;
      const dest = rel.startsWith("/") ? rel : `/${rel}`;
      if (dest.endsWith("/")) throw RsError.badRequest("MOVE destination must be a file path");
      const created = await files.rename(path, dest);
      const resp = msg.response(created ? 201 : 200, undefined);
      resp.setHeader("location", `${base}${dest}`);
      return resp;
    }

    switch (msg.method) {
      case "GET": {
        if (msg.url.isDirectory()) return this.getDirectory(msg, ctx, files, path, range, ifNoneMatch, accept);
        let e: RsError;
        try {
          return await this.serveFile(msg.response(200, undefined), range, ifNoneMatch, files, path);
        } catch (err) {
          if (!(err instanceof RsError)) throw err;
          e = err;
        }
        // The path may name a directory without the trailing slash.
        try {
          if ((await files.head(path)).isDir) return FileService.slashRedirect(msg);
        } catch {
          /* fall through */
        }
        if (e.code === codes.NOT_FOUND) {
          const extensionless = isExtensionless(path);
          if (extensionless && site.friendlyUrls) {
            const real = await this.resolveFriendly(path, accept, files, site.extensionPriority);
            if (real !== undefined) {
              const resp = await this.serveFile(msg.response(200, undefined), range, ifNoneMatch, files, real);
              resp.setHeader("content-location", `${msg.url.basePath}${real}`);
              return resp;
            }
          }
          if ((extensionless && site.spaFallback) || site.spaFallbackAll) {
            const dflt = site.defaultResource ?? "index.html";
            return this.serveFile(msg.response(200, undefined), range, ifNoneMatch, files, `/${dflt}`);
          }
        }
        throw e;
      }
      case "HEAD": {
        let resolved: [string, { size: number }] | undefined;
        try {
          const meta = await files.head(path);
          if (meta.isDir && !msg.url.isDirectory()) return FileService.slashRedirect(msg);
          resolved = [path, meta];
        } catch (err) {
          if (err instanceof RsError && err.code === codes.NOT_FOUND && site.friendlyUrls && isExtensionless(path)) {
            const real = await this.resolveFriendly(path, accept, files, site.extensionPriority);
            resolved = real !== undefined ? [real, await files.head(real)] : undefined;
          } else {
            throw err;
          }
        }
        if (!resolved) throw RsError.notFound(`'${path}' does not exist`);
        const [real, meta] = resolved;
        const resp = msg.response(200, undefined);
        resp.setHeader("content-length", String(meta.size));
        resp.setHeader("accept-ranges", "bytes");
        resp.setHeader("content-type", MediaType.forPath(real).toString());
        if (real !== path) resp.setHeader("content-location", `${msg.url.basePath}${real}`);
        return resp;
      }
      case "POST":
        if (msg.url.isDirectory()) {
          // Store contract: keyless POST to a container creates a
          // server-named child and returns its Location.
          const body = msg.body;
          if (!body) throw RsError.badRequest("write requires a body");
          const name = `${simpleUuid()}${extensionFor(body.mediaType)}`;
          const childPath = `${path}${name}`;
          await files.write(childPath, body);
          const etag = await files.currentEtag(childPath);
          const template = Message.request(msg.method, msg.url.path, msg.tenant);
          const resp = template.response(201, undefined);
          resp.trace = msg.trace.clone();
          resp.setHeader("location", `${msg.url.basePath}${childPath}`);
          if (etag !== undefined) resp.setHeader("etag", etag);
          return resp;
        }
      // falls through
      case "PUT": {
        if (msg.url.isDirectory()) throw RsError.badRequest("cannot PUT to a directory path");
        const precondition = writePrecondition(msg);
        const body = msg.body;
        if (!body) throw RsError.badRequest("write requires a body");
        let storePath = path;
        if (site.friendlyUrls && isExtensionless(path)) {
          const e1 = site.extensionPriority[0];
          if (e1 === undefined) throw RsError.badRequest("extension-less write requires a configured 'extensionPriority'");
          storePath = `${path}.${e1.replace(/^\.+/, "").toLowerCase()}`;
        }
        const outcome = await files.writeCond(storePath, body, precondition);
        const template = Message.request(msg.method, msg.url.path, msg.tenant);
        const resp = template.response(outcome.created ? 201 : 200, undefined);
        resp.trace = msg.trace.clone();
        if (outcome.etag !== undefined) resp.setHeader("etag", outcome.etag);
        if (storePath !== path) {
          resp.setHeader("location", `${msg.url.basePath}${path}`);
          resp.setHeader("content-location", `${msg.url.basePath}${storePath}`);
        }
        return resp;
      }
      case "DELETE": {
        const precondition = writePrecondition(msg);
        if (msg.url.isDirectory()) {
          if (precondition.kind !== "none") {
            throw RsError.badRequest("conditional headers are not supported on directory deletes");
          }
          const trimmed = path.replace(/\/+$/, "");
          const dirName = trimmed.slice(trimmed.lastIndexOf("/") + 1);
          if (msg.url.queryParam("confirm") === dirName && dirName !== "") await files.deleteDirAll(path);
          else await files.deleteDir(path);
        } else {
          try {
            await files.deleteCond(path, precondition);
          } catch (err) {
            if (err instanceof RsError && err.code === codes.NOT_FOUND && site.friendlyUrls && isExtensionless(path)) {
              const real = await this.resolveFriendly(path, undefined, files, site.extensionPriority);
              if (real === undefined) throw err;
              await files.deleteCond(real, precondition);
            } else {
              throw err;
            }
          }
        }
        return msg.noContent();
      }
      default:
        throw new RsError(405, codes.BAD_REQUEST, "Method Not Allowed", `file service does not support ${msg.method}`);
    }
  }

  private async getDirectory(
    msg: Message,
    ctx: ServiceContext,
    files: ScopedFileStore,
    path: string,
    range: ByteRange | undefined,
    ifNoneMatch: string | undefined,
    accept: string | undefined,
  ): Promise<Message> {
    const site = this.site;
    const forceListing = wantsDirListing(accept);
    // Static-site mode: directories serve the default resource.
    if (!forceListing && site.defaultResource !== undefined) {
      const dflt = site.defaultResource;
      try {
        const resp = await this.serveFile(msg.response(200, undefined), range, ifNoneMatch, files, `${path}${dflt}`);
        resp.setHeader("vary", "accept");
        return resp;
      } catch (e) {
        if (!(e instanceof RsError)) throw e;
        if (e.code !== codes.NOT_FOUND) throw e;
        if ((site.spaFallback || site.spaFallbackAll) && path !== "/") {
          const resp = await this.serveFile(msg.response(200, undefined), range, ifNoneMatch, files, `/${dflt}`);
          resp.setHeader("vary", "accept");
          return resp;
        }
      }
    }
    // `listings: false` conceals the inventory from the public; operators
    // still see it.
    if (!site.listings && !isOperator(msg.principal, ctx.operatorRoles ?? "")) {
      throw RsError.notFound(`'${path}' does not exist`);
    }
    const operatorOnly = !site.listings;
    const [take, skip] = pagination(msg);
    const sortParam = msg.url.queryParam("$sort");
    let entries: Json[];
    let total: number;
    if (sortParam !== undefined) {
      const sort = MetaSort.parse(sortParam);
      const [all, t] = await files.list(path, Number.MAX_SAFE_INTEGER, 0);
      sort.sort(all);
      entries = all.slice(skip, skip + take).map(dirEntryJson);
      total = t;
    } else {
      const [page, t] = await files.list(path, take, skip);
      entries = page.map(dirEntryJson);
      total = t;
    }
    const listing = { path, entries, total };
    const resp = msg.ok(Body.fromString(JSON.stringify(listing), MediaType.dirJson()));
    resp.setHeader("x-total-count", String(total));
    resp.setHeader("vary", "accept");
    if (operatorOnly) {
      resp.setHeader("cache-control", "no-store");
      appendHeaderValue(resp, "vary", "authorization, cookie");
    }
    return resp;
  }
}
