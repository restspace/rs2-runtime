// Inbound WebSockets, guest side (websocket.md "Guests", cloudflare.md
// §E.6): a socket event routes to `onOpen`/`onMessage`/`onClose` instead of
// `default`, and the handler's `socket` handle reaches only its own mount's
// `/.sockets/` subtree. The routing half runs a real bundle under the shim
// in a dynamic worker; the `socket.send`/`socket.close` half drives the DO
// ops directly against a stub requester — the guest's `env.RS2` cannot be
// handed to a loader-built worker from a test.

import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import {
  DynamicWorkerEngine,
  guestSocketCloseOp,
  guestSocketSendOp,
} from "../src/engines/dynamic-worker";
import type { EngineHost, InvocationRecord, Invocations, SocketMount } from "../src/engines/dynamic-worker";
import { GrantedHost } from "../src/engines/host-api";
import type { LogContext } from "../src/engines/host-api";
import { Body } from "../src/runtime/body";
import { RsError } from "../src/runtime/error";
import type { Json, JsonObject } from "../src/runtime/error";
import { NullLogStore } from "../src/runtime/logging";
import { Message } from "../src/runtime/message";
import { SOCKET_EVENT_HEADER, SOCKET_ID_HEADER, TRIGGER_HEADER, TRIGGER_WEBSOCKET } from "../src/runtime/sockets";
import type { SocketAccept } from "../src/runtime/sockets";
import { socketEventMessage } from "../src/runtime/sockets";

const loader = (env as { LOADER?: WorkerLoader }).LOADER;

// ---- the routing half (real bundle, real shim) -----------------------------

/// An engine whose guests get no `env.RS2`: enough to prove which export a
/// socket event runs and what envelope comes back. (`socket.send` from such
/// a guest is `capability_denied`, exercised separately below over the ops.)
function engine(): DynamicWorkerEngine {
  const host: EngineHost = {
    loader: loader!,
    invocations: new Map(),
    hostApiStub: () => undefined as unknown as Fetcher,
    egressStub: () => undefined as unknown as Fetcher,
    stateKv: { get: async () => undefined, put: async () => undefined },
  };
  return new DynamicWorkerEngine(host);
}

const logCtx: LogContext = {
  sink: new NullLogStore(),
  tenant: "t",
  mount: "/chat",
  service: "chat@v1",
  traceId: "0".repeat(32),
  spanId: "0".repeat(16),
};

function accept(): SocketAccept {
  return {
    id: "sock-1",
    tenant: "t",
    mount: "/chat",
    path: "/chat/room/42",
    query: "",
    principal: undefined,
    exp: undefined,
    text: "json",
    events: ["open", "message", "close"],
    protocol: undefined,
    connectedAt: 0,
  };
}

function run(id: string, source: string, msg: Message): Promise<Message> {
  return engine().invoke({
    codeId: id,
    source,
    msg,
    config: {},
    grants: new Map(),
    serviceRef: "chat@v1",
    logCtx,
    outboundBudget: 4,
    materializeCap: 1 << 20,
    wallClockMs: 15_000,
    cpuMs: 15_000,
  });
}

describe.skipIf(!loader)("socket events route to the socket exports", () => {
  it("a message event runs onMessage and its envelope is the reply", async () => {
    const source = `
      export default async () => ({ status: 200, body: "default" });
      export const onMessage = async (msg, ctx, socket) =>
        ({ status: 200, body: { saw: msg.body, id: socket.id, path: msg.url, method: msg.method } });
    `;
    const msg = socketEventMessage(accept(), "message", '{"hi":"there"}');
    const resp = await run("ws:onmessage", source, msg);
    expect(resp.status).toBe(200);
    expect(await resp.body!.asAny(1 << 20)).toEqual({
      // A `text: "json"` mount hands the frame over parsed, like any other
      // JSON body at the engine boundary.
      saw: { hi: "there" },
      id: "sock-1",
      path: "/chat/room/42",
      method: "POST",
    });
  });

  it("open and close events run their own exports", async () => {
    const source = `
      export default async () => ({ status: 200, body: "default" });
      export const onOpen = async () => ({ status: 200, body: "opened" });
      export const onClose = async (msg) => ({ status: 200, body: msg.body });
    `;
    const opened = await run("ws:openclose", source, socketEventMessage(accept(), "open"));
    expect(await opened.body!.asAny(1 << 20)).toBe("opened");
    const closed = await run(
      "ws:openclose",
      source,
      socketEventMessage(accept(), "close", { code: 1001, reason: "bye", wasClean: true }),
    );
    expect(await closed.body!.asAny(1 << 20)).toEqual({ code: 1001, reason: "bye", wasClean: true });
  });

  it("a missing onOpen/onClose is a 204 no-op", async () => {
    const source = `export default async () => ({ status: 200, body: "default" });`;
    for (const event of ["open", "close"] as const) {
      const resp = await run("ws:noop", source, socketEventMessage(accept(), event));
      expect(resp.status, event).toBe(204);
      expect(resp.body, event).toBeUndefined();
    }
  });

  it("a missing onMessage is 502 contract_violation, not a no-op", async () => {
    const source = `export default async () => ({ status: 200, body: "default" });`;
    const err = await run("ws:nomessage", source, socketEventMessage(accept(), "message", '{"a":1}'))
      .then(() => undefined)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RsError);
    expect((err as RsError).status).toBe(502);
    expect((err as RsError).code).toBe("contract_violation");
    expect((err as RsError).detail).toContain("bundle has no onMessage export");
  });

  it("a text frame that is not JSON on a text:json mount is the existing 400", async () => {
    // No special case for socket events: the frame is an ordinary body and
    // a body typed as JSON that is not JSON fails loudly at the boundary.
    const source = `export const onMessage = async () => ({ status: 200, body: "ok" });`;
    const err = await run("ws:badjson", source, socketEventMessage(accept(), "message", "not json"))
      .then(() => undefined)
      .catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RsError);
    expect((err as RsError).status).toBe(400);
  });

  it("an external request forging the trigger headers still gets the default export", async () => {
    // `socketEventOf` checks the source, so the headers alone buy nothing:
    // a client cannot reach `onMessage` (and thus a socket handle) from the
    // wire.
    const source = `
      export default async () => ({ status: 200, body: "default" });
      export const onMessage = async () => ({ status: 200, body: "onMessage" });
    `;
    const msg = Message.request("POST", "/chat/room/42", "t");
    msg.source = "external";
    msg.setHeader(TRIGGER_HEADER, TRIGGER_WEBSOCKET);
    msg.setHeader(SOCKET_EVENT_HEADER, "message");
    msg.setHeader(SOCKET_ID_HEADER, "sock-1");
    const resp = await run("ws:forged", source, msg);
    expect(await resp.body!.asAny(1 << 20)).toBe("default");
  });
});

// ---- the send/close half (the DO ops, against a stub requester) ------------

interface Seen {
  method: string;
  path: string;
  query: string;
  source: string;
  mediaType: string | undefined;
  body: Json;
}

/// A `Requester` recording what reached dispatch and answering like the
/// `/.sockets/` interception does.
function stubRequester(reply?: (msg: Message) => Message): { mount: SocketMount; seen: Seen[] } {
  const seen: Seen[] = [];
  const mount: SocketMount = {
    base: "/chat",
    requester: {
      request: async (msg: Message) => {
        seen.push({
          method: msg.method,
          path: msg.url.path,
          query: msg.url.query,
          source: msg.source,
          mediaType: msg.body?.mediaType.toString(),
          body: msg.body ? await msg.body.asAny(1 << 20) : null,
        });
        return reply ? reply(msg) : msg.response(200, Body.fromJson({ sent: 1 }));
      },
    },
  };
  return { mount, seen };
}

function record(mount: SocketMount | undefined, outboundBudget = 4): InvocationRecord {
  return {
    host: new GrantedHost(new Map(), outboundBudget, undefined, "chat@v1"),
    tenant: "t",
    depth: 2,
    principal: undefined,
    materializeCap: 1 << 20,
    hostError: undefined,
    socketAllowlist: [],
    bodyReader: undefined,
    streamedIn: 0,
    sink: undefined,
    socketMount: mount,
  };
}

describe("guest socket ops address only the invocation's own mount", () => {
  it("send issues a system POST to <mount>/.sockets/?$id=… and returns {sent}", async () => {
    const { mount, seen } = stubRequester();
    const invocations: Invocations = new Map([["i1", record(mount)]]);
    const out = await guestSocketSendOp(invocations, "i1", "sock-1", "hello");
    expect(out).toEqual({ sent: 1 });
    expect(seen).toHaveLength(1);
    expect(seen[0]!).toMatchObject({
      method: "POST",
      path: "/chat/.sockets/",
      query: "$id=sock-1",
      // `system`: the guest is answering on its own mount, so the connected
      // user need not hold that mount's write role.
      source: "system",
      mediaType: "text/plain",
      body: "hello",
    });
  });

  it("bytes go as a binary frame", async () => {
    const { mount, seen } = stubRequester();
    const invocations: Invocations = new Map([["i1", record(mount)]]);
    await guestSocketSendOp(invocations, "i1", "sock-1", new Uint8Array([1, 2, 3]));
    expect(seen[0]!.mediaType).toBe("application/octet-stream");
  });

  it("close issues a DELETE carrying code and reason, and returns {closed}", async () => {
    const { mount, seen } = stubRequester((msg) => msg.response(200, Body.fromJson({ closed: 2 })));
    const invocations: Invocations = new Map([["i1", record(mount)]]);
    const out = await guestSocketCloseOp(invocations, "i1", "sock-1", 4001, "go away");
    expect(out).toEqual({ closed: 2 });
    expect(seen[0]!).toMatchObject({ method: "DELETE", path: "/chat/.sockets/" });
    expect(seen[0]!.query).toBe("$id=sock-1&code=4001&reason=go%20away");
  });

  it("the socket id cannot steer the request off the mount (item 4)", async () => {
    // `system` bypasses access checks, so the id is the one guest-supplied
    // value here and it only ever lands URL-encoded in `$id`.
    const { mount, seen } = stubRequester();
    const invocations: Invocations = new Map([["i1", record(mount)]]);
    await guestSocketSendOp(invocations, "i1", "../../admin/.sockets/?$user=root#x", "x");
    expect(seen[0]!.path).toBe("/chat/.sockets/");
    expect(seen[0]!.query).toBe("$id=..%2F..%2Fadmin%2F.sockets%2F%3F%24user%3Droot%23x");
  });

  it("an unknown or finished invocation is denied", async () => {
    const invocations: Invocations = new Map();
    const sent = (await guestSocketSendOp(invocations, "gone", "sock-1", "x")) as JsonObject;
    expect(sent.__rs2_error).toBe(true);
    expect(sent.status).toBe(500);
    const closed = (await guestSocketCloseOp(invocations, "gone", "sock-1", undefined, undefined)) as JsonObject;
    expect(closed.__rs2_error).toBe(true);
  });

  it("a mount with no socket surface is capability_denied", async () => {
    const invocations: Invocations = new Map([["i1", record(undefined)]]);
    const out = (await guestSocketSendOp(invocations, "i1", "sock-1", "x")) as JsonObject;
    expect(out.code).toBe("capability_denied");
  });

  it("a non-2xx from /.sockets/ keeps its identity as the guest's error", async () => {
    const { mount } = stubRequester((msg) =>
      msg.response(404, Body.fromJson({ code: "not_found", title: "Not Found", detail: "no such socket" })),
    );
    const invocations: Invocations = new Map([["i1", record(mount)]]);
    const out = (await guestSocketSendOp(invocations, "i1", "sock-9", "x")) as JsonObject;
    expect(out).toMatchObject({ __rs2_error: true, code: "not_found", status: 404 });
    expect(invocations.get("i1")!.hostError?.code).toBe("not_found");
  });

  it("frames count against the outbound budget, one budget per handler call", async () => {
    // The engine builds a fresh `GrantedHost` per invocation, so each
    // handler call gets its own budget; within one call a frame is an
    // ordinary outbound hop.
    const a = stubRequester();
    const b = stubRequester();
    const invocations: Invocations = new Map([
      ["call-a", record(a.mount, 1)],
      ["call-b", record(b.mount, 1)],
    ]);
    expect(await guestSocketSendOp(invocations, "call-a", "s", "one")).toEqual({ sent: 1 });
    const over = (await guestSocketSendOp(invocations, "call-a", "s", "two")) as JsonObject;
    expect(over.code).toBe("limit_exceeded");
    // The second invocation's budget is untouched by the first's breach.
    expect(await guestSocketSendOp(invocations, "call-b", "s", "one")).toEqual({ sent: 1 });
  });
});
