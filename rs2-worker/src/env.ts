// The Worker's bindings and vars (wrangler.jsonc, cloudflare.md §G).

import type { RegistryObject } from "./registry-object";
import type { TenantObject } from "./tenant-object";

export interface Env {
  TENANTS: DurableObjectNamespace<TenantObject>;
  REGISTRY: DurableObjectNamespace<RegistryObject>;
  RS2_FILES: R2Bucket;
  /// Dynamic Workers (§E): `code:`/`template` mounts. Optional so a
  /// deployment without the binding degrades to 501, not a build error.
  LOADER?: WorkerLoader;
  RS2_DEFAULT_TENANT?: string;
  RS2_MAIN_DOMAIN?: string;
  RS2_LOG_LEVEL?: string;
  RS2_CATALOGUE_HOSTS?: string;
  /// Host limit overrides as a JSON object, named as
  /// `/.well-known/rs2/services` reports them — e.g.
  /// `{"outboundCalls": 45}` on a Workers plan whose subrequest cap sits
  /// below RS2's default budget (`runtime/wrapper.ts` `limitsFromJson`).
  RS2_LIMITS?: string;
  /// Secret: gates `/admin/*`.
  RS2_ADMIN_TOKEN?: string;
  /// Secrets: Cloudflare for SaaS custom hostnames (`/admin/domains/*`,
  /// `src/domains.ts`). Both set → the endpoints provision/poll/remove a
  /// custom hostname; either missing → registry-only mode.
  CF_API_TOKEN?: string;
  CF_ZONE_ID?: string;
  /// Where customers point their domain's CNAME (falls back to
  /// `RS2_MAIN_DOMAIN`; reported by the domains admin endpoints).
  RS2_CNAME_TARGET?: string;
}

/// Headers the Worker stamps on the request it forwards to the tenant DO.
/// The DO trusts them only because the Worker is its sole caller.
export const TENANT_HEADER = "x-rs2-tenant";
export const TRACE_HEADER = "x-rs2-trace-id";
export const INFRAS_VERSION_HEADER = "x-rs2-infras-version";
