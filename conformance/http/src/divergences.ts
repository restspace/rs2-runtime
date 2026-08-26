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
}

const TABLE: Record<HostKind, Divergences> = {
  rust: {
    absentDirectoryDelete: [404],
    dotSegmentTraversal: [400],
  },
  cloudflare: {
    absentDirectoryDelete: [204, 404],
    dotSegmentTraversal: [400, 404],
  },
};

/** The divergence table for the host under test. */
export function divergences(): Divergences {
  return TABLE[env().hostKind];
}

/** Assert `status` is one of the tolerated values for a divergence. */
export function expectDivergent(name: keyof Divergences, status: number, context: string): void {
  const allowed = divergences()[name];
  if (!allowed.includes(status)) {
    throw new Error(`${context}: status ${status} is not in the allowed set ${JSON.stringify(allowed)} for '${name}' on host '${env().hostKind}'`);
  }
}
