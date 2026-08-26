// Guest-boundary WorkerEntrypoints (cloudflare.md §E.3/§E.4). Both run in
// the Worker isolate and defer every decision to the tenant's DO:
//
// - `HostApi` is the guest's `env.RS2` — the op table (`request`, `log`,
//   `stateGet/Put`, `bodyRead`, `streamBegin`/`bodyWrite`, `socketCheck`,
//   `fetchOut`). Each method forwards `{invocationId, …}` to the tenant
//   stub, which holds the invocations map. Host errors return as
//   `{__rs2_error, code, status, message}` markers the shim rethrows.
// - `Egress` is every dynamic worker's `globalOutbound`: guest platform
//   `fetch` lands here; the invocation id travels on the
//   `x-rs2-invocation` header the shim's fetch wrapper stamps (stripped
//   before the request reaches the grant path).
//
// Instantiated by the DO via `ctx.exports.HostApi({props: {tenant}})` /
// `ctx.exports.Egress({props: {tenant}})` — the props are invisible to and
// unforgeable by the guest.

import { WorkerEntrypoint } from "cloudflare:workers";
import type { Json } from "./runtime/error";
import type { SerializedRequest, SerializedResponse } from "./engines/dynamic-worker";
import type { Env } from "./env";

export interface GuestBoundaryProps {
  tenant: string;
}

/// The tenant DO's guest RPC surface (`tenant-object.ts`), typed shallowly
/// here — the generated `DurableObjectStub<TenantObject>` type recurses
/// past the compiler's instantiation depth.
interface TenantGuestRpc {
  guestRequest(invocationId: string, capability: string, req: Json): Promise<Json>;
  guestLog(invocationId: string, level: string, text: string): Promise<void>;
  guestStateGet(invocationId: string, key: string): Promise<Json>;
  guestStatePut(invocationId: string, key: string, value: string): Promise<Json>;
  guestBodyRead(invocationId: string): Promise<Json | { data: Uint8Array }>;
  guestStreamBegin(invocationId: string, envelope: Json): Promise<Json>;
  guestBodyWrite(invocationId: string, data: Uint8Array): Promise<Json>;
  guestSocketCheck(invocationId: string, host: string, port: number): Promise<Json>;
  guestFetch(invocationId: string | null, req: SerializedRequest): Promise<SerializedResponse>;
}

const INVOCATION_HEADER = "x-rs2-invocation";
const GUEST_ERROR_HEADER = "x-rs2-guest-error";

/// Guest request bodies crossing the boundary are materialized under the
/// same cap as every other engine-boundary body.
const FETCH_BODY_CAP = 32 * 1024 * 1024;

function stub(env: Env, ctx: { props?: unknown }): TenantGuestRpc {
  const props = ctx.props as GuestBoundaryProps | undefined;
  const tenant = props?.tenant ?? "";
  return env.TENANTS.get(env.TENANTS.idFromName(tenant)) as unknown as TenantGuestRpc;
}

/// Every prototype method of a `WorkerEntrypoint` is callable over RPC by
/// whoever holds the binding — here, the guest. So this class carries ONLY
/// the op table: no helpers, no accessors, nothing that returns the tenant
/// stub (which would hand the guest `putConfig`/`rawConfig`/`deleteAll`).
/// `test/egress-surface.test.ts` pins the exact method set.
export class HostApi extends WorkerEntrypoint<Env> {
  request(invocationId: string, capability: string, req: Json): Promise<Json> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestRequest(invocationId, capability, req);
  }

  log(invocationId: string, level: string, text: string): Promise<void> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestLog(invocationId, level, text);
  }

  stateGet(invocationId: string, key: string): Promise<Json> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestStateGet(invocationId, key);
  }

  statePut(invocationId: string, key: string, value: string): Promise<Json> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestStatePut(invocationId, key, value);
  }

  bodyRead(invocationId: string): Promise<Json | { data: Uint8Array }> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestBodyRead(invocationId);
  }

  streamBegin(invocationId: string, envelope: Json): Promise<Json> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestStreamBegin(invocationId, envelope);
  }

  bodyWrite(invocationId: string, data: Uint8Array): Promise<Json> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestBodyWrite(invocationId, data);
  }

  socketCheck(invocationId: string, host: string, port: number, _tls: boolean): Promise<Json> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestSocketCheck(invocationId, host, port);
  }

  /// The serialized-fetch path (spec §E.3 `fetchOut`) for callers that hold
  /// an explicit invocation id; the gateway's `fetch` below is the normal
  /// route.
  async fetchOut(invocationId: string, req: SerializedRequest): Promise<SerializedResponse> {
    return stub(this.env, this.ctx as unknown as { props?: unknown }).guestFetch(invocationId, req);
  }
}

export class Egress extends WorkerEntrypoint<Env> {
  override async fetch(request: Request): Promise<Response> {
    const invocationId = request.headers.get(INVOCATION_HEADER);
    const headers: [string, string][] = [];
    request.headers.forEach((v, k) => {
      if (k.toLowerCase() !== INVOCATION_HEADER) headers.push([k, v]);
    });
    let body: Uint8Array | null = null;
    if (request.body) {
      const bytes = new Uint8Array(await request.arrayBuffer());
      if (bytes.byteLength > FETCH_BODY_CAP) {
        return new Response("guest fetch body exceeds the 32 MiB cap", { status: 413 });
      }
      body = bytes.byteLength > 0 ? bytes : null;
    }
    const serialized: SerializedRequest = { method: request.method, url: request.url, headers, body };
    const out = await stub(this.env, this.ctx as unknown as { props?: unknown }).guestFetch(invocationId, serialized);
    const respHeaders = new Headers();
    for (const [k, v] of out.headers) {
      try {
        respHeaders.append(k, v);
      } catch {
        /* drop invalid header */
      }
    }
    if (out.guestError) respHeaders.set(GUEST_ERROR_HEADER, "1");
    const status = out.status >= 200 && out.status <= 599 ? out.status : 502;
    const noBody = status === 204 || status === 304 || !out.body || out.body.byteLength === 0;
    return new Response(noBody ? null : (out.body as Uint8Array<ArrayBuffer>), { status, headers: respHeaders });
  }
}
