// Custom domains (cloudflare.md §B.5): the Cloudflare for SaaS side of
// `PUT/GET/DELETE /admin/domains/<host>`. The registry map is the routing
// truth; this module only provisions/polls/removes the CF custom hostname
// (`/client/v4/zones/<zone>/custom_hostnames`, ssl method `http`) when both
// `CF_API_TOKEN` and `CF_ZONE_ID` are configured — without them the admin
// endpoints are registry-only and say so. Pure functions + an injectable
// `fetch` so the status mapping and response parsing are unit-testable.

import { RsError } from "./runtime/error";
import type { Json, JsonObject } from "./runtime/error";

/// The status a client sees: ownership first, then the certificate, then live.
export type DomainStatus = "pending_validation" | "pending_certificate" | "active";

/// Map CF's raw pair (custom-hostname `status`, `ssl.status`) onto ours:
/// the hostname is validated before the cert matters, so anything short of
/// hostname `active` is ownership validation; then anything short of ssl
/// `active` is certificate issuance.
export function mapDomainStatus(hostnameStatus: string | undefined, sslStatus: string | undefined): DomainStatus {
  if (hostnameStatus !== "active") return "pending_validation";
  if (sslStatus !== "active") return "pending_certificate";
  return "active";
}

/// The `ownership_verification` TXT record CF asks the customer to publish.
export interface OwnershipRecord {
  type: string;
  name: string;
  value: string;
}

/// One custom hostname as the admin API reports it.
export interface CustomHostname {
  id: string;
  hostname: string;
  status: DomainStatus;
  /// Raw CF statuses, kept for operators polling a stuck domain.
  cfStatus: string;
  cfSslStatus: string;
  ownershipVerification: OwnershipRecord | null;
}

function str(v: Json | undefined): string | undefined {
  return typeof v === "string" ? v : undefined;
}

function obj(v: Json | undefined): JsonObject | undefined {
  return v && typeof v === "object" && !Array.isArray(v) ? v : undefined;
}

/// Parse one `result` object from the custom-hostnames API.
export function parseCustomHostname(result: JsonObject): CustomHostname {
  const hostnameStatus = str(result.status);
  const ssl = obj(result.ssl);
  const sslStatus = ssl ? str(ssl.status) : undefined;
  const ov = obj(result.ownership_verification);
  const ownership =
    ov && str(ov.type) !== undefined && str(ov.name) !== undefined && str(ov.value) !== undefined
      ? { type: str(ov.type)!, name: str(ov.name)!, value: str(ov.value)! }
      : null;
  return {
    id: str(result.id) ?? "",
    hostname: str(result.hostname) ?? "",
    status: mapDomainStatus(hostnameStatus, sslStatus),
    cfStatus: hostnameStatus ?? "unknown",
    cfSslStatus: sslStatus ?? "unknown",
    ownershipVerification: ownership,
  };
}

/// The zone-scoped custom-hostnames client. `fetchFn` is injectable so unit
/// tests run against canned responses — the real API is never reachable from
/// tests or local dev.
export class CfSaasApi {
  constructor(
    private readonly apiToken: string,
    private readonly zoneId: string,
    private readonly fetchFn: typeof fetch = fetch,
    private readonly baseUrl = "https://api.cloudflare.com/client/v4",
  ) {}

  private async call(method: string, pathAndQuery: string, body?: JsonObject): Promise<JsonObject> {
    let resp: Response;
    try {
      resp = await this.fetchFn(`${this.baseUrl}/zones/${this.zoneId}${pathAndQuery}`, {
        method,
        headers: {
          authorization: `Bearer ${this.apiToken}`,
          ...(body !== undefined ? { "content-type": "application/json" } : {}),
        },
        ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
      });
    } catch (e) {
      throw new RsError(502, "internal", "Upstream Error", `Cloudflare API unreachable: ${e instanceof Error ? e.message : String(e)}`).asRetryable();
    }
    let parsed: Json;
    try {
      parsed = (await resp.json()) as Json;
    } catch {
      throw new RsError(502, "internal", "Upstream Error", `Cloudflare API returned ${resp.status} with a non-JSON body`);
    }
    const doc = obj(parsed) ?? {};
    if (!resp.ok || doc.success !== true) {
      const errors = Array.isArray(doc.errors)
        ? doc.errors
            .map((e) => {
              const eo = obj(e);
              return eo ? `${typeof eo.code === "number" ? `${eo.code}: ` : ""}${str(eo.message) ?? ""}` : "";
            })
            .filter((m) => m !== "")
            .join("; ")
        : "";
      throw new RsError(502, "internal", "Upstream Error", `Cloudflare API ${method} custom_hostnames failed (HTTP ${resp.status})${errors ? `: ${errors}` : ""}`);
    }
    return doc;
  }

  /// The existing custom hostname for `host`, if any.
  async find(host: string): Promise<CustomHostname | undefined> {
    const doc = await this.call("GET", `/custom_hostnames?hostname=${encodeURIComponent(host)}`);
    const first = Array.isArray(doc.result) ? obj(doc.result[0]) : undefined;
    return first ? parseCustomHostname(first) : undefined;
  }

  /// Create (or return the existing) custom hostname, ssl method `http`.
  async ensure(host: string): Promise<CustomHostname> {
    const existing = await this.find(host);
    if (existing) return existing;
    const doc = await this.call("POST", "/custom_hostnames", {
      hostname: host,
      ssl: { method: "http", type: "dv" },
    });
    const result = obj(doc.result);
    if (!result) throw new RsError(502, "internal", "Upstream Error", "Cloudflare API create returned no result");
    return parseCustomHostname(result);
  }

  /// Remove the custom hostname; `true` iff one existed.
  async remove(host: string): Promise<boolean> {
    const existing = await this.find(host);
    if (!existing || existing.id === "") return false;
    await this.call("DELETE", `/custom_hostnames/${existing.id}`);
    return true;
  }
}

/// The vars/secrets the domains admin endpoints read.
export interface DomainsEnv {
  CF_API_TOKEN?: string;
  CF_ZONE_ID?: string;
  RS2_CNAME_TARGET?: string;
  RS2_MAIN_DOMAIN?: string;
}

/// The CF client when both secrets are configured; otherwise `undefined`
/// (registry-only mode).
export function cfApiFromEnv(env: DomainsEnv, fetchFn: typeof fetch = fetch): CfSaasApi | undefined {
  if (!env.CF_API_TOKEN || !env.CF_ZONE_ID) return undefined;
  return new CfSaasApi(env.CF_API_TOKEN, env.CF_ZONE_ID, fetchFn);
}

/// Where the customer points their CNAME: `RS2_CNAME_TARGET` when set, else
/// the platform's main domain, else unknown (`null`).
export function cnameTarget(env: DomainsEnv): string | null {
  return env.RS2_CNAME_TARGET || env.RS2_MAIN_DOMAIN || null;
}

export const REGISTRY_ONLY_NOTE =
  "registry-only: CF_API_TOKEN/CF_ZONE_ID not configured, no Cloudflare custom hostname was provisioned";

/// The admin response body for `PUT`/`GET /admin/domains/<host>`.
/// `cfConfigured` distinguishes "no secrets" (registry-only, said explicitly)
/// from "secrets set but no custom hostname exists yet".
export function domainResponse(
  host: string,
  tenant: string,
  env: DomainsEnv,
  cfConfigured: boolean,
  provisioning: CustomHostname | undefined,
): JsonObject {
  const body: JsonObject = { host, tenant, cnameTarget: cnameTarget(env) };
  if (provisioning !== undefined) {
    body.status = provisioning.status;
    const ov = provisioning.ownershipVerification;
    body.provisioning = {
      id: provisioning.id,
      cfStatus: provisioning.cfStatus,
      cfSslStatus: provisioning.cfSslStatus,
      ownershipVerification: ov ? { type: ov.type, name: ov.name, value: ov.value } : null,
    };
  } else {
    body.status = null;
    body.provisioning = null;
    body.note = cfConfigured ? "no Cloudflare custom hostname exists for this host" : REGISTRY_ONLY_NOTE;
  }
  return body;
}
