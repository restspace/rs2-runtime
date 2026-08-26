// Outbound-call gating shared by every surface that egresses HTTP (PRD
// §9.2). Port of `rs2-core/src/outbound.rs`.

import { RsError } from "./error";
import type { JsonObject } from "./error";
import type { Message } from "./message";
import type { CredentialInjector } from "../capabilities/credential";
import type { HttpOut } from "../capabilities/types";
import { RS2_VERSION } from "./version";

/// Host of an absolute URL ("https://api.x.com:8443/v1" → "api.x.com").
export function urlHost(url: string): string | undefined {
  const i = url.indexOf("://");
  if (i < 0) return undefined;
  const rest = url.slice(i + 3);
  const authority = rest.split(/[/?#]/)[0] ?? "";
  const afterUser = authority.includes("@") ? authority.slice(authority.lastIndexOf("@") + 1) : authority;
  const host = afterUser.split(":")[0] ?? "";
  return host === "" ? undefined : host.toLowerCase();
}

/// Whether a resolved call URL leaves the node (case-insensitive scheme).
export function isExternalUrl(url: string): boolean {
  const l = url.slice(0, 8).toLowerCase();
  return l.startsWith("http://") || l.startsWith("https://");
}

/// Allowlist matching: exact host or a `*.suffix` wildcard (apex included).
export function hostMatches(patternIn: string, host: string): boolean {
  const pattern = patternIn.toLowerCase();
  if (pattern.startsWith("*.")) {
    const suffix = pattern.slice(2);
    return host === suffix || host.endsWith(`.${suffix}`);
  }
  return host === pattern;
}

export interface HostGrant {
  name: string;
  hosts: string[];
  injector: CredentialInjector | undefined;
}

/// The pipeline executor's external-call capability: the union of a mount's
/// `httpOut` grants over the node's outbound adapter.
export class ExternalDispatch {
  private constructor(
    private readonly http: HttpOut | undefined,
    readonly grants: HostGrant[],
    private readonly maxBodyBytes: number,
  ) {}

  static fromMount(
    config: JsonObject,
    http: HttpOut | undefined,
    injectors: Map<string, CredentialInjector>,
    maxBodyBytes: number,
  ): ExternalDispatch | undefined {
    const g = config.grants;
    if (!g || typeof g !== "object" || Array.isArray(g)) return undefined;
    const grants: HostGrant[] = [];
    for (const [name, grant] of Object.entries(g)) {
      if (!grant || typeof grant !== "object" || Array.isArray(grant)) continue;
      if (grant.type !== "httpOut") continue;
      const hosts = Array.isArray(grant.hosts) ? grant.hosts.filter((h): h is string => typeof h === "string") : [];
      grants.push({ name, hosts, injector: injectors.get(name) });
    }
    if (grants.length === 0) return undefined;
    grants.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    return new ExternalDispatch(http, grants, maxBodyBytes);
  }

  covers(host: string): boolean {
    return this.grants.some((g) => g.hosts.some((p) => hostMatches(p, host)));
  }

  /// Allowlist check for an absolute URL — no I/O. Returns the index of the
  /// first matching grant.
  authorize(url: string): number {
    const host = urlHost(url);
    if (host === undefined) throw RsError.badRequest(`outbound call needs an absolute URL, got '${url}'`);
    const grant = this.grants.findIndex((g) => g.hosts.some((p) => hostMatches(p, host)));
    if (grant < 0) {
      const allowed = this.grants.flatMap((g) => g.hosts);
      throw RsError.capabilityDenied(`httpOut to '${host}' (this mount's httpOut grants allow: ${allowed.join(", ")})`);
    }
    if (!this.http) throw RsError.engineUnavailable("this deployment has no outbound HTTP adapter configured");
    return grant;
  }

  async send(grant: number, msg: Message): Promise<Message> {
    if (!this.http) throw RsError.engineUnavailable("this deployment has no outbound HTTP adapter configured");
    if (msg.body && !msg.headers.has("content-type")) msg.setHeader("content-type", msg.body.mediaType.toString());
    if (!msg.headers.has("accept")) msg.setHeader("accept", "*/*");
    if (!msg.headers.has("user-agent")) msg.setHeader("user-agent", `rs2/${RS2_VERSION}`);
    const inj = this.grants[grant]?.injector;
    if (inj) await inj.apply(msg, this.maxBodyBytes);
    return this.http.request(msg);
  }
}
