// Authentication as a chain of services. Port of `rs2-core/src/authn.rs`.
// The host knows no credential format: a service that can authenticate
// requests implements `Authenticator` and returns itself from
// `Service.authenticator()`, and the host offers every request without a
// principal to each such mount in config order until one accepts it.
//
// An authenticator recognises only its own credential format, so the order
// of the chain never changes an outcome:
// - `pass`: nothing here for me; try the next one.
// - `unrecognised`: something credential-shaped I could not use. It may be
//   another authenticator's; if none accepts it, this error is the answer
//   (a garbage bearer token is a 401, never anonymous).
// - `authenticated`: I verified it and set `msg.principal`.
// - throw: my format, but invalid (bad signature, expired, revoked): a 401
//   now, without asking anyone else.

import type { RsError } from "./error";
import type { Message } from "./message";
import type { ServiceContext } from "../services/context";
import type { Tenant } from "./tenant-build";

export type AuthOutcome =
  | { kind: "pass" }
  | { kind: "unrecognised"; error: RsError }
  /// `expires` (epoch seconds) bounds anything that outlives the request,
  /// such as an accepted WebSocket; undefined means no expiry of its own.
  | { kind: "authenticated"; expires: number | undefined };

export interface Authenticator {
  /// Whether the request carries a credential of this authenticator's that
  /// the browser attaches by itself (a cookie). The host's CSRF guard runs
  /// on this before authentication, so a cross-site forgery is refused
  /// whether or not its credential is valid.
  ambientCredential(msg: Message): boolean;
  /// Recognise, verify, and on success set `msg.principal`.
  tryAuthenticate(msg: Message, ctx: ServiceContext): Promise<AuthOutcome>;
}

/// Whether any authenticator in the tenant sees an ambient credential.
export function hasAmbientCredential(tenant: Tenant, msg: Message): boolean {
  return tenant.authenticators().some(([auth]) => auth.ambientCredential(msg));
}

/// Run the tenant's authenticator chain. Returns the accepted credential's
/// expiry; undefined when no one accepted (the request stays anonymous) or
/// the credential does not expire.
export async function authenticate(tenant: Tenant, msg: Message): Promise<number | undefined> {
  let unrecognised: RsError | undefined;
  for (const [auth, ctx] of tenant.authenticators()) {
    const outcome = await auth.tryAuthenticate(msg, ctx);
    if (outcome.kind === "authenticated") return outcome.expires;
    if (outcome.kind === "unrecognised") unrecognised ??= outcome.error;
  }
  if (unrecognised !== undefined) throw unrecognised;
  return undefined;
}
