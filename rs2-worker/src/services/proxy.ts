// `proxy` service (PRD §9.2, the v1 "proxy adapter" use case): forward a
// request to a fixed external `target`, attaching operator-supplied auth
// **host-side** on the way out. Port of `rs2-core/src/services/proxy.rs`;
// the credential is resolved at tenant build from the mount's `inject` ref
// and never appears in the mount config or reaches any guest. The
// destination is fixed by `target`, so there is no host allowlist to
// choose — the mount *is* the allowlist.

import { RsError } from "../runtime/error";
import { Message } from "../runtime/message";
import { PROXY_INJECTOR_KEY } from "./context";
import type { Service, ServiceContext } from "./context";

export class ProxyService implements Service {
  async handle(msg: Message, ctx: ServiceContext): Promise<Message> {
    const http = ctx.http;
    if (!http) {
      throw RsError.engineUnavailable("this deployment has no outbound HTTP adapter configured");
    }
    const target = ctx.config.target;
    if (typeof target !== "string") {
      throw RsError.badRequest("proxy mount requires a 'target' base URL");
    }

    // target + the remaining path (+ query) — the host stays the target's.
    let url = `${target.replace(/\/+$/, "")}${msg.url.servicePath}`;
    if (msg.url.query !== "") url += `?${msg.url.query}`;

    const out = Message.request(msg.method, url, msg.tenant);
    // Forward the client's headers, but strip anything that must not reach
    // the upstream target: the inbound Host (the transport sets it from the
    // target), the caller's *own* credentials to this gateway (RS2 reads
    // `Authorization: Bearer` and the `rs-auth` cookie — see `auth.ts`), and
    // hop-by-hop headers that only describe the client↔gateway connection.
    // The injector adds the target's own auth next.
    out.headers = new Headers(msg.headers);
    for (const h of ["host", "authorization", "cookie", "connection", "transfer-encoding", "proxy-authorization"]) {
      out.headers.delete(h);
    }
    out.body = msg.body;
    msg.body = undefined;

    const inj = ctx.outboundInjectors.get(PROXY_INJECTOR_KEY);
    if (inj) await inj.apply(out, ctx.limits.materializedBodyBytes);

    return http.request(out);
  }
}
