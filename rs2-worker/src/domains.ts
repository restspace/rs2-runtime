// Custom domains (cloudflare.md §B.5): attaching a customer's own hostname
// to a tenant, behind `PUT/GET/DELETE /admin/domains/<host>`.
//
// Two halves, and the split is the point. The **provider layer** at the
// bottom of this file is host-neutral — an attachment is `pending` until
// control of the DNS is proven, then `active`, and the response says which
// DNS records to publish and what to do next. The **Cloudflare for SaaS
// client** above it (`/client/v4/zones/<zone>/custom_hostnames`, ssl method
// `http`) is one provider; the self-check `ManualProvider` is the other, and
// is what a deployment without `CF_API_TOKEN`/`CF_ZONE_ID` runs.
//
// The registry map remains the routing truth, and nothing reaches it until a
// provider reports `active` — so a tenant cannot claim a hostname it does not
// control. Pure functions + an injectable `fetch` throughout, so every path
// is unit-testable and the real CF API is never reachable from tests.

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

/// Is `host` a syntactically valid hostname (already lowercased)? Labels of
/// 1–63 LDH characters, no leading/trailing hyphen, no empty label, no
/// trailing dot, 253 characters overall. Rejects the shapes that otherwise
/// reach the registry and the Cloudflare API verbatim — a URL, a `host:port`,
/// an empty string, a wildcard (issue #2 item 5).
export function validHostname(host: string): boolean {
  if (host.length === 0 || host.length > 253) return false;
  return host.split(".").every((label) => label.length <= 63 && /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/.test(label));
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

// ---------------------------------------------------------------------------
// The provider layer
// ---------------------------------------------------------------------------
//
// Cloudflare for SaaS is *an* implementation of attaching a domain, not the
// shape of the feature. What every host shares is a resource with a lifecycle
// — `pending` until control of the DNS is proven, then `active` — plus the DNS
// records the customer has to publish. That much is portable; the verifier and
// the certificate are not. So clients read `status`/`dnsRecords`/`nextStep`
// and never parse a provider's own vocabulary, which stays under
// `provider.detail` for an operator debugging a stuck domain.

/// Where an attachment is in its lifecycle. Two states, deliberately: a
/// client's only question is "can I send traffic here yet?".
export type AttachmentStatus = "pending" | "active";

/// One record the customer publishes at their own DNS provider.
export interface DnsRecord {
  type: string;
  name: string;
  value: string;
  /// `false` for a record that only speeds things up rather than gates the
  /// attachment (Cloudflare's pre-validation TXT).
  required: boolean;
  purpose: string;
}

export interface Attachment {
  status: AttachmentStatus;
  dnsRecords: DnsRecord[];
  /// Provider-specific diagnosis, for humans only. Never routing input.
  detail: JsonObject | null;
}

/// Attaching a domain, abstracted over who verifies it and who issues the
/// certificate. `token` is the challenge a provider may prove control with;
/// providers that verify some other way ignore it.
export interface DomainProvider {
  readonly name: string;
  /// Idempotent: begin (or re-read) the attachment for `host`.
  ensure(host: string, token: string): Promise<Attachment>;
  /// Poll it. Returning `active` is what promotes the host into the routing
  /// map, so this is the gate — never a client's assertion.
  check(host: string, token: string): Promise<Attachment>;
  /// Best-effort teardown of whatever `ensure` created.
  remove(host: string): Promise<void>;
}

/// The CNAME every provider asks for: the customer's host, pointed here.
function cnameRecord(host: string, target: string | null): DnsRecord[] {
  if (target === null) return [];
  return [
    {
      type: "CNAME",
      name: host,
      value: target,
      required: true,
      purpose: "routes the domain to this deployment",
    },
  ];
}

/// Cloudflare for SaaS: CF validates the domain over HTTP and issues the
/// certificate, so the customer publishes one CNAME and nothing else.
export class CloudflareSaasProvider implements DomainProvider {
  readonly name = "cloudflare-saas";

  constructor(
    private readonly api: CfSaasApi,
    private readonly target: string | null,
  ) {}

  private attachment(host: string, ch: CustomHostname): Attachment {
    const records = cnameRecord(host, this.target);
    const ov = ch.ownershipVerification;
    // Only worth showing while it would still change anything: publishing it
    // lets a customer validate *before* switching a live domain over.
    if (ov && ch.status !== "active") {
      records.push({
        type: ov.type.toUpperCase(),
        name: ov.name,
        value: ov.value,
        required: false,
        purpose: "optional: pre-validates the domain before you move the CNAME",
      });
    }
    return {
      status: ch.status === "active" ? "active" : "pending",
      dnsRecords: records,
      detail: { stage: ch.status, cfStatus: ch.cfStatus, cfSslStatus: ch.cfSslStatus, id: ch.id },
    };
  }

  async ensure(host: string): Promise<Attachment> {
    return this.attachment(host, await this.api.ensure(host));
  }

  async check(host: string): Promise<Attachment> {
    const ch = await this.api.find(host);
    if (ch) return this.attachment(host, ch);
    // The hostname went away underneath us (deleted in the dashboard): still
    // pending, and the detail says why rather than pretending it is new.
    return { status: "pending", dnsRecords: cnameRecord(host, this.target), detail: { stage: "absent" } };
  }

  async remove(host: string): Promise<void> {
    await this.api.remove(host);
  }
}

/// The path where a pending host is asked for its token.
export const CHALLENGE_PREFIX = "/.well-known/rs2/domain-challenge/";

/// No provider API at all — control of the DNS is proven by asking the
/// hostname for a token only this deployment can serve. That is ACME's
/// HTTP-01 in miniature, it needs no external service, and it is what a host
/// without Cloudflare for SaaS uses (including the Rust host, the day it
/// gains a writable domain map). The certificate is then someone else's job:
/// a reverse proxy's, or the platform's.
export class ManualProvider implements DomainProvider {
  readonly name = "manual";

  constructor(
    private readonly target: string | null,
    private readonly fetchFn: typeof fetch = fetch,
  ) {}

  async ensure(host: string, token: string): Promise<Attachment> {
    // A re-attach of a host whose DNS already points here is active at once.
    return this.check(host, token);
  }

  async check(host: string, token: string): Promise<Attachment> {
    const records = cnameRecord(host, this.target);
    // Plain HTTP, following redirects: the point is to reach *this*
    // deployment, and a host mid-setup may have no certificate yet.
    const url = `http://${host}${CHALLENGE_PREFIX}${encodeURIComponent(token)}`;
    let detail: JsonObject;
    try {
      const resp = await this.fetchFn(url, { redirect: "follow" });
      const body = (await resp.text()).trim();
      if (resp.ok && body === token) {
        return { status: "active", dnsRecords: records, detail: { check: "ok" } };
      }
      detail = { check: `${url} answered ${resp.status}${resp.ok ? " with the wrong token" : ""}` };
    } catch (e) {
      detail = { check: `${url} unreachable: ${e instanceof Error ? e.message : String(e)}` };
    }
    return { status: "pending", dnsRecords: records, detail };
  }

  async remove(): Promise<void> {
    /* nothing was provisioned */
  }
}

/// The provider this deployment runs: Cloudflare for SaaS when both secrets
/// are set, otherwise the self-check. Never nothing — an unconfigured
/// deployment used to accept a domain and provision precisely nothing, which
/// looked like success and wasn't.
export function providerFromEnv(env: DomainsEnv, fetchFn: typeof fetch = fetch): DomainProvider {
  const api = cfApiFromEnv(env, fetchFn);
  const target = cnameTarget(env);
  return api ? new CloudflareSaasProvider(api, target) : new ManualProvider(target, fetchFn);
}

/// What a customer does next, in one sentence. Clients render this rather
/// than deriving their own from `status`, so the wording stays one thing.
export function nextStep(host: string, tenant: string, a: Attachment): string {
  if (a.status === "active") return `nothing to do: ${host} is live and serving tenant '${tenant}'`;
  const required = a.dnsRecords.filter((r) => r.required);
  if (required.length === 0) {
    return `waiting on DNS for ${host}: this deployment has no CNAME target configured (set RS2_CNAME_TARGET), so point ${host} at it and it goes live automatically`;
  }
  return `publish the required DNS record${required.length === 1 ? "" : "s"} above at ${host}'s DNS provider; the domain goes live on its own once we can see it, usually within minutes`;
}

/// The admin response body for `PUT`/`GET /admin/domains/<host>`, identical
/// on every host and every provider.
export function attachmentResponse(host: string, tenant: string, providerName: string, a: Attachment): JsonObject {
  return {
    host,
    tenant,
    status: a.status,
    dnsRecords: a.dnsRecords as unknown as Json,
    nextStep: nextStep(host, tenant, a),
    provider: { name: providerName, detail: a.detail },
  };
}

/// A host already in the routing map: active by definition, whatever the
/// provider would say. The read path on both hosts answers with this.
export function liveAttachment(): Attachment {
  return { status: "active", dnsRecords: [], detail: null };
}
