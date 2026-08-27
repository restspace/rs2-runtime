// Allowed divergences between hosts (spec section A), keyed by
// `RS2_HOST_KIND`. This is the ONLY place a suite may consult the host
// kind for an expected value; everything else is the same contract on
// both hosts. Add a row here (and to the table in section A of
// docs/agents/cloudflare.md) before a suite tolerates a difference.

import { env, type HostKind } from "./client.ts";

export interface Divergences {
  /**
   * `DELETE` of a directory that never existed: Rust local-fs answers 404,
   * R2 has no directories so the Worker answers 204.
   */
  absentDirectoryDelete: number[];
  /**
   * A request target with dot segments (`/files/../secret`,
   * `/files/%2e%2e/secret`): the Rust router sees the raw target and answers
   * 400 `path_unsafe`; the Workers platform canonicalizes dot segments
   * before the Worker runs (`request.url` is already `/secret`), so the
   * request is routed on the normalized path — 404 for an unmounted target.
   * `%00` and other unsafe forms still reach the router and are 400.
   */
  dotSegmentTraversal: number[];
  /**
   * How a guest (`code:`) store adapter's backend connection behaves
   * across requests. On Rust the resident isolate lives for the mount's
   * lifetime and I/O handles pool in module scope, so a serial run opens
   * exactly one connection (`"pooled"`). On the Worker, I/O objects are
   * request-scoped by the platform ("Cannot perform I/O on behalf of a
   * different request"), so a pooled socket dies at the invocation
   * boundary and adapters reconnect per invocation (`"perInvocation"`);
   * `store.maxRuntimes`/`idleMs` are accepted and ignored.
   */
  guestAdapterPooling: "pooled" | "perInvocation";
  /**
   * How a domain is attached to a tenant (`/admin/domains`). Both hosts
   * answer the read side identically. The Worker also writes (`"api"`):
   * `PUT` claims the host, a provider proves control of the DNS, and only
   * then does it route. The Rust host's tenancy map is static config loaded
   * at startup and its TLS is a reverse proxy's, so it has nothing to write
   * or provision (`"config"`) and refuses the write side with a 501
   * `provider_unavailable` naming `serverConfig.tenancy.domainMap`.
   */
  domainAttachment: "api" | "config";
}

const TABLE: Record<HostKind, Divergences> = {
  rust: {
    absentDirectoryDelete: [404],
    dotSegmentTraversal: [400],
    guestAdapterPooling: "pooled",
    domainAttachment: "config",
  },
  cloudflare: {
    absentDirectoryDelete: [204, 404],
    dotSegmentTraversal: [400, 404],
    guestAdapterPooling: "perInvocation",
    domainAttachment: "api",
  },
};

/** The divergence table for the host under test. */
export function divergences(): Divergences {
  return TABLE[env().hostKind];
}

/** The divergence rows that are status-code sets. */
type StatusDivergence = { [K in keyof Divergences]: Divergences[K] extends number[] ? K : never }[keyof Divergences];

/** Assert `status` is one of the tolerated values for a divergence. */
export function expectDivergent(name: StatusDivergence, status: number, context: string): void {
  const allowed = divergences()[name];
  if (!allowed.includes(status)) {
    throw new Error(`${context}: status ${status} is not in the allowed set ${JSON.stringify(allowed)} for '${name}' on host '${env().hostKind}'`);
  }
}
