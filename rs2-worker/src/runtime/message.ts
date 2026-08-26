// Message (PRD §6.1): method, URL, headers, status, Body, plus runtime
// context that is never serialized to the wire. Port of
// `rs2-core/src/message/message.rs`.

import { Body } from "./body";
import { RsError } from "./error";
import type { Json, JsonObject } from "./error";
import { MediaType, PROBLEM_JSON } from "./media-type";

/// Where a message entered the runtime (PRD §5.2).
export type Source = "external" | "internal" | "system";

/// 32 lowercase hex — a UUIDv4 in "simple" form.
export function simpleUuid(): string {
  return crypto.randomUUID().replace(/-/g, "");
}

/// W3C-style trace context threaded through every internal/external call.
export class TraceContext {
  traceId: string;
  spanId: string;

  constructor(traceId?: string, spanId?: string) {
    this.traceId = traceId ?? simpleUuid();
    this.spanId = spanId ?? simpleUuid().slice(0, 16);
  }

  child(): TraceContext {
    return new TraceContext(this.traceId, simpleUuid().slice(0, 16));
  }

  clone(): TraceContext {
    return new TraceContext(this.traceId, this.spanId);
  }
}

/// Authenticated principal.
export interface Principal {
  id: string;
  roles: string[];
  /// `user` or `agent` (PRD §10.5).
  kind: string;
  /// Extra JWT claims (the auth mount's `jwtUserProps`).
  extra: JsonObject;
}

export function clonePrincipal(p: Principal | undefined): Principal | undefined {
  return p ? { id: p.id, roles: [...p.roles], kind: p.kind, extra: { ...p.extra } } : undefined;
}

function percentDecodeLossy(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    // Malformed escapes: decode what we can byte-wise, keep the rest.
    return s.replace(/%[0-9a-fA-F]{2}/g, (m) => {
      try {
        return decodeURIComponent(m);
      } catch {
        return "�";
      }
    });
  }
}

/// Path + query of a message URL, with the mount split applied by the router.
export class MsgUrl {
  path: string;
  query: string;
  basePath: string;
  servicePath: string;

  constructor(path = "", query = "", basePath = "", servicePath = "") {
    this.path = path;
    this.query = query;
    this.basePath = basePath;
    this.servicePath = servicePath;
  }

  /// Parse a path-and-query string like `/data/orders/1?x=1`.
  static parse(pathAndQuery: string): MsgUrl {
    const i = pathAndQuery.indexOf("?");
    const path = i < 0 ? pathAndQuery : pathAndQuery.slice(0, i);
    const query = i < 0 ? "" : pathAndQuery.slice(i + 1);
    return new MsgUrl(path, query, "", path);
  }

  clone(): MsgUrl {
    return new MsgUrl(this.path, this.query, this.basePath, this.servicePath);
  }

  /// First value of a query parameter, percent-decoded. The key is decoded
  /// too (`%24take` must match `$take`); `+` in values maps to a space.
  queryParam(name: string): string | undefined {
    for (const pair of this.query.split("&")) {
      const eq = pair.indexOf("=");
      const k = eq < 0 ? pair : pair.slice(0, eq);
      const v = eq < 0 ? "" : pair.slice(eq + 1);
      if (percentDecodeLossy(k) === name) {
        return percentDecodeLossy(v).replace(/\+/g, " ");
      }
    }
    return undefined;
  }

  /// Split the path at a matched mount prefix.
  applyMount(basePath: string): void {
    this.basePath = basePath.replace(/\/+$/, "");
    this.servicePath = this.path.slice(this.basePath.length);
    if (this.servicePath === "") this.servicePath = "/";
  }

  /// Non-empty segments of the service path.
  serviceSegments(): string[] {
    return this.servicePath.split("/").filter((s) => s !== "");
  }

  /// Non-empty segments of the mount prefix (`basePath`).
  baseSegments(): string[] {
    return this.basePath.split("/").filter((s) => s !== "");
  }

  /// The resource name: the last service-path segment, if any.
  name(): string | undefined {
    const segs = this.serviceSegments();
    return segs.length ? segs[segs.length - 1] : undefined;
  }

  /// Whether the request addresses a directory (trailing slash or mount root).
  isDirectory(): boolean {
    return this.servicePath.endsWith("/");
  }
}

export class Message {
  method: string;
  url: MsgUrl;
  headers: Headers;
  status: number | undefined;
  body: Body | undefined;
  // Runtime context — not serialized to the wire.
  tenant: string;
  principal: Principal | undefined;
  trace: TraceContext;
  source: Source;
  /// Pipeline message naming (`as: "$name"`).
  name: string | undefined;
  /// Internal-call depth, capped by limits to bound recursion.
  depth: number;

  constructor(method: string, url: MsgUrl, tenant: string) {
    this.method = method;
    this.url = url;
    this.headers = new Headers();
    this.status = undefined;
    this.body = undefined;
    this.tenant = tenant;
    this.principal = undefined;
    this.trace = new TraceContext();
    this.source = "external";
    this.name = undefined;
    this.depth = 0;
  }

  static request(method: string, pathAndQuery: string, tenant: string): Message {
    return new Message(method.toUpperCase(), MsgUrl.parse(pathAndQuery), tenant);
  }

  withBody(body: Body): Message {
    this.body = body;
    return this;
  }

  withJson(value: Json): Message {
    return this.withBody(Body.fromJson(value));
  }

  /// A response derived from this message's runtime context.
  response(status: number, body: Body | undefined): Message {
    const resp = new Message(this.method, this.url.clone(), this.tenant);
    resp.status = status;
    resp.body = body;
    resp.principal = clonePrincipal(this.principal);
    resp.trace = this.trace.clone();
    resp.source = this.source;
    resp.name = this.name;
    resp.depth = this.depth;
    return resp;
  }

  ok(body: Body | undefined): Message {
    return this.response(200, body);
  }

  okJson(value: Json): Message {
    return this.ok(Body.fromJson(value));
  }

  noContent(): Message {
    return this.response(204, undefined);
  }

  /// Map an `RsError` to a problem+json response (PRD §12).
  errorResponse(err: RsError): Message {
    const problem = err.toProblemJson(this.tenant, this.trace.traceId);
    const status = err.status >= 100 && err.status <= 999 ? err.status : 500;
    const resp = this.response(status, Body.fromString(JSON.stringify(problem), new MediaType(PROBLEM_JSON)));
    if (err.retryAfterMs !== undefined) {
      resp.headers.set("retry-after", String(Math.ceil(err.retryAfterMs / 1000)));
    }
    return resp;
  }

  isOk(): boolean {
    return this.status === undefined || (this.status >= 200 && this.status < 300);
  }

  header(name: string): string | undefined {
    return this.headers.get(name) ?? undefined;
  }

  setHeader(name: string, value: string): void {
    try {
      this.headers.set(name, value);
    } catch {
      // An invalid header value is dropped, as the Rust `HeaderValue::from_str` path does.
    }
  }
}
