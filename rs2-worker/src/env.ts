// The Worker's bindings and vars (wrangler.jsonc, cloudflare.md §G).

import type { RegistryObject } from "./registry-object";
import type { TenantObject } from "./tenant-object";

export interface Env {
  TENANTS: DurableObjectNamespace<TenantObject>;
  REGISTRY: DurableObjectNamespace<RegistryObject>;
  RS2_FILES: R2Bucket;
  /// Dynamic Workers (P4); present in the config so its local emulation can
  /// be verified, unused until the engine lands.
  LOADER?: WorkerLoader;
  RS2_DEFAULT_TENANT?: string;
  RS2_MAIN_DOMAIN?: string;
  RS2_LOG_LEVEL?: string;
  RS2_CATALOGUE_HOSTS?: string;
  /// Secret: gates `/admin/*`.
  RS2_ADMIN_TOKEN?: string;
}

/// Headers the Worker stamps on the request it forwards to the tenant DO.
/// The DO trusts them only because the Worker is its sole caller.
export const TENANT_HEADER = "x-rs2-tenant";
export const TRACE_HEADER = "x-rs2-trace-id";
export const INFRAS_VERSION_HEADER = "x-rs2-infras-version";
