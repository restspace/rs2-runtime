// Router (PRD §5.2, §9.1): tenant resolution, mount tables with
// longest-prefix matching, and path safety enforced for all services.
// Port of `rs2-core/src/router/mod.rs`.

import { RsError } from "./error";
import type { Json, JsonObject } from "./error";

/// Tenancy model (PRD §9.1). Resolution order in multi mode: explicit domain
/// map → `{tenant}.{mainDomain}` subdomain → the default tenant (Worker-only).
export interface Tenancy {
  domainMap: Map<string, string>;
  mainDomain: string | undefined;
  defaultTenant: string | undefined;
}

export function resolveTenant(tenancy: Tenancy, hostHeader: string): string | undefined {
  // Strip any port from the Host header.
  const host = (hostHeader.split(":")[0] ?? hostHeader).toLowerCase();
  const mapped = tenancy.domainMap.get(host);
  if (mapped !== undefined) return mapped;
  if (tenancy.mainDomain) {
    const suffix = `.${tenancy.mainDomain}`;
    if (host.endsWith(suffix)) {
      const sub = host.slice(0, host.length - suffix.length);
      if (sub !== "" && !sub.includes(".")) return sub;
    }
  }
  return tenancy.defaultTenant;
}

function percentDecodeLossy(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s.replace(/%[0-9a-fA-F]{2}/g, (m) => {
      try {
        return decodeURIComponent(m);
      } catch {
        return "�";
      }
    });
  }
}

function hasControl(s: string): boolean {
  for (const ch of s) {
    const c = ch.codePointAt(0)!;
    // Rust `char::is_control`: Unicode Cc category (U+0000–U+001F, U+007F–U+009F).
    if (c < 0x20 || (c >= 0x7f && c <= 0x9f)) return true;
  }
  return false;
}

/// Path safety (PRD §10.1): enforced in the router for **all** services.
/// Rejects traversal, encoded traversal, null bytes, backslashes, drive
/// letters, and control characters, on the raw and decoded forms.
export function validatePath(path: string): void {
  const decoded = percentDecodeLossy(path);
  for (const candidate of [path, decoded]) {
    if (candidate.includes("\0") || candidate.includes("%00")) {
      throw RsError.pathUnsafe("path contains a null byte");
    }
    if (candidate.includes("\\")) {
      throw RsError.pathUnsafe("path contains a backslash");
    }
    if (hasControl(candidate)) {
      throw RsError.pathUnsafe("path contains control characters");
    }
    const segs = candidate.split("/");
    if (segs.some((seg) => seg.length >= 2 && /^\.+$/.test(seg))) {
      throw RsError.pathUnsafe("path contains a traversal segment");
    }
    // Windows drive letters, e.g. `C:` at a segment start.
    if (segs.some((seg) => seg.length >= 2 && seg[1] === ":" && /^[A-Za-z]$/.test(seg[0]!))) {
      throw RsError.pathUnsafe("path contains a drive letter");
    }
  }
}

/// A binding of a service + config to a URL path prefix within a tenant.
export interface Mount {
  /// Normalized mount prefix, no trailing slash; `""` is the root mount.
  basePath: string;
  /// Service reference: a prebuilt name (`file`, `data`) or `code:<name>@<version>`.
  service: string;
  config: JsonObject;
}

export function configGet(config: JsonObject, key: string): Json | undefined {
  return Object.prototype.hasOwnProperty.call(config, key) ? config[key] : undefined;
}

/// Mount table with longest-prefix matching on segment boundaries.
export class MountTable {
  /// Sorted by descending prefix length so the first match wins.
  private readonly _mounts: Mount[];

  constructor(mounts: Mount[]) {
    const normalized = mounts.map((m) => {
      const n = `/${m.basePath.replace(/^\/+|\/+$/g, "")}`;
      return { ...m, basePath: n === "/" ? "" : n };
    });
    const seen = new Set<string>();
    for (const m of normalized) {
      if (seen.has(m.basePath)) {
        throw RsError.badRequest(`duplicate mount path '${m.basePath === "" ? "/" : m.basePath}'`);
      }
      seen.add(m.basePath);
    }
    // Stable sort by descending prefix length (Rust `sort_by` is stable).
    normalized.sort((a, b) => b.basePath.length - a.basePath.length);
    this._mounts = normalized;
  }

  /// Longest-prefix match. A mount `/files` matches `/files` and `/files/x`
  /// but not `/filesystem`.
  route(path: string): Mount | undefined {
    return this._mounts.find((m) => m.basePath === "" || path === m.basePath || path.startsWith(`${m.basePath}/`));
  }

  mounts(): readonly Mount[] {
    return this._mounts;
  }
}
