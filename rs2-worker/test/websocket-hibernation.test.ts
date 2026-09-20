/// <reference types="@cloudflare/vitest-pool-workers/types" />
// Inbound WebSockets on the platform host (docs/agents/websocket.md, §E.6),
// end to end through the real Worker and the real `TenantObject`: the
// upgrade is authorized before any 101, an accepted socket runs the mount's
// pipeline per frame, `/.sockets/` sends and lists from the hub, the whole
// thing survives **hibernation** (the DO is evicted with the socket open and
// every handler rebuilds from the attachment), and a breached per-frame
// limit closes with the mapped application code.
//
// Every response body here is read to completion. An unread body leaves the
// DO request in flight, and `evictDurableObject` waits for in-flight
// requests to drain — an unread 200 elsewhere in the file is enough to hang
// the hibernation test.
import { SELF, env, evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { Env as WorkerEnv } from "../src/env";
import type { TenantObject } from "../src/tenant-object";
import type { Json, JsonObject } from "../src/runtime/error";
import { defaultLimits } from "../src/runtime/wrapper";

declare module "cloudflare:test" {
  interface ProvidedEnv extends WorkerEnv {}
}

const TOKEN = "test-admin-token";
const HOST = "ws.test";
const ADMIN = { email: "a@ws.test", password: "a-pw" };

/// The tenant under test: an auth mount to mint the admin's token, a
/// `webSocket` pipeline mount that echoes each frame through a stored spec,
/// a plain mount that must ignore `Upgrade`, and one whose read role nobody
/// in the test holds.
function config(): JsonObject {
  return {
    // `jwtUserProps` is how a principal grows: the claim it copies is what
    // pushes a `SocketAccept` past the attachment cap (the spill test).
    auth: { jwtSecret: "ws-secret", userDataset: "users", jwtUserProps: ["bio"] },
    operatorRoles: "A",
    mounts: [
      { path: "/auth", service: "auth", config: { access: "open" } },
      { path: "/echo", service: "pipeline", config: { access: { read: "all", write: "A" }, webSocket: true } },
      { path: "/plain", service: "data", config: { access: "open" } },
      { path: "/locked", service: "data", config: { access: { read: "A", write: "A" }, webSocket: true } },
    ],
  };
}

/// The echo spec: the frame is the message body, so a JSONata transform over
/// it proves the mount's pipeline really ran on the socket event.
const ECHO_SPEC: Json = { pipeline: { steps: [{ transform: { echo: "text" } }] } };

let bearer: string;

function as(token: string | undefined): Record<string, string> {
  return token === undefined ? {} : { authorization: `Bearer ${token}` };
}

interface Read {
  status: number;
  headers: Headers;
  text: string;
}

/// An ordinary request, body consumed.
async function call(path: string, init: RequestInit = {}): Promise<Read> {
  const resp = await SELF.fetch(`http://${HOST}${path}`, init);
  return { status: resp.status, headers: resp.headers, text: await resp.text() };
}

function jsonOf(r: Read): JsonObject {
  return JSON.parse(r.text) as JsonObject;
}

function jsonCall(path: string, method: string, token: string | undefined, payload: Json): Promise<Read> {
  return call(path, { method, headers: { ...as(token), "content-type": "application/json" }, body: JSON.stringify(payload) });
}

interface Socket {
  ws: WebSocket;
  /// The next frame, or a rejection on timeout.
  next(timeoutMs?: number): Promise<string>;
  /// The close the host sent.
  closed(timeoutMs?: number): Promise<{ code: number; reason: string }>;
}

/// Accept the client half and queue what arrives: `webSocketMessage` runs
/// asynchronously in the DO, so every assertion waits on an event rather
/// than on a sleep.
function attach(ws: WebSocket): Socket {
  const frames: string[] = [];
  const waiters: Array<(f: string) => void> = [];
  let close: { code: number; reason: string } | undefined;
  const closeWaiters: Array<(c: { code: number; reason: string }) => void> = [];
  ws.accept();
  ws.addEventListener("message", (e: MessageEvent) => {
    const text = typeof e.data === "string" ? e.data : new TextDecoder().decode(e.data as ArrayBuffer);
    const waiter = waiters.shift();
    if (waiter) waiter(text);
    else frames.push(text);
  });
  ws.addEventListener("close", (e: CloseEvent) => {
    close = { code: e.code, reason: e.reason };
    for (const w of closeWaiters.splice(0)) w(close);
  });
  return {
    ws,
    next(timeoutMs = 5_000): Promise<string> {
      const queued = frames.shift();
      if (queued !== undefined) return Promise.resolve(queued);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`no frame within ${timeoutMs}ms`)), timeoutMs);
        waiters.push((f) => {
          clearTimeout(timer);
          resolve(f);
        });
      });
    },
    closed(timeoutMs = 5_000): Promise<{ code: number; reason: string }> {
      if (close !== undefined) return Promise.resolve(close);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`no close within ${timeoutMs}ms`)), timeoutMs);
        closeWaiters.push((c) => {
          clearTimeout(timer);
          resolve(c);
        });
      });
    },
  };
}

interface Upgraded extends Read {
  socket: Socket | undefined;
  protocol: string | null;
}

/// Ask for the upgrade exactly as a client does. A 101 carries no body and
/// cannot be read or cloned; a refusal's problem body is consumed here.
async function upgrade(path: string, token?: string, protocols?: string): Promise<Upgraded> {
  const resp = await SELF.fetch(`http://${HOST}${path}`, {
    headers: {
      upgrade: "websocket",
      ...(protocols !== undefined ? { "sec-websocket-protocol": protocols } : {}),
      ...as(token),
    },
  });
  const protocol = resp.headers.get("sec-websocket-protocol");
  if (resp.status === 101 && resp.webSocket) {
    return { status: 101, headers: resp.headers, text: "", protocol, socket: attach(resp.webSocket) };
  }
  return { status: resp.status, headers: resp.headers, text: await resp.text(), protocol, socket: undefined };
}

async function connect(path: string, token?: string): Promise<Socket> {
  const up = await upgrade(path, token);
  if (up.socket === undefined) throw new Error(`upgrade ${path} refused: ${up.status} ${up.text}`);
  return up.socket;
}

/// Close the client half and let the DO's `webSocketClose` run.
async function shut(...sockets: Socket[]): Promise<void> {
  for (const s of sockets) s.ws.close(1000, "done");
  await new Promise((r) => setTimeout(r, 50));
}

async function seedTenant(): Promise<void> {
  env.RS2_ADMIN_TOKEN = TOKEN;
  const put = await jsonCall("/admin/tenants/main", "PUT", TOKEN, {
    config: config(),
    domains: [],
    bootstrapAdmin: ADMIN,
  });
  expect([200, 201], put.text).toContain(put.status);
  const login = await jsonCall("/auth/login", "POST", undefined, ADMIN);
  expect(login.status, login.text).toBe(200);
  bearer = jsonOf(login).token as string;
  const spec = await jsonCall("/echo/.pipelines/.root", "PUT", bearer, ECHO_SPEC);
  expect([200, 201, 204], spec.text).toContain(spec.status);
}

function tenantStub(): DurableObjectStub<TenantObject> {
  return env.TENANTS.get(env.TENANTS.idFromName("main"));
}

/// Tear the object's instance down while hibernatable sockets stay open —
/// exactly what the platform does to an idle DO, and the only way to prove a
/// handler rebuilds from its attachment.
function evict(): Promise<void> {
  return evictDurableObject(tenantStub(), { webSockets: "hibernate" });
}

describe("inbound WebSockets", () => {
  beforeAll(seedTenant);

  // ---- authorization -------------------------------------------------------

  it("an upgrade the access policy refuses is a problem+json, never a 101", async () => {
    const up = await upgrade("/locked/anything");
    expect(up.status).toBe(401);
    expect(up.socket).toBeUndefined();
    expect(up.headers.get("content-type")).toContain("application/problem+json");
    // Nothing was registered: the hub has no socket on that mount.
    const list = await call("/locked/.sockets/", { headers: as(bearer) });
    if (list.status === 200) expect(jsonOf(list).total).toBe(0);
  });

  it("a mount without the flag serves the plain GET and ignores `Upgrade`", async () => {
    const up = await upgrade("/plain/nothing-here");
    expect(up.status).not.toBe(101);
    expect(up.socket).toBeUndefined();
  });

  // ---- accept + dispatch ---------------------------------------------------

  it("an accepted socket runs the mount's pipeline per frame", async () => {
    const sock = await connect("/echo/room/1", bearer);
    sock.ws.send(JSON.stringify({ text: "hello" }));
    expect(JSON.parse(await sock.next())).toEqual({ echo: "hello" });
    // A second frame is a second dispatch on the same connection.
    sock.ws.send(JSON.stringify({ text: "again" }));
    expect(JSON.parse(await sock.next())).toEqual({ echo: "again" });
    await shut(sock);
  });

  it("echoes the selected subprotocol, preferring a real one over the bearer entry", async () => {
    const up = await upgrade("/echo/room/proto", bearer, "rs2, rs2.bearer.ignored");
    expect(up.status, up.text).toBe(101);
    expect(up.protocol).toBe("rs2");
    await shut(up.socket!);
  });

  // ---- /.sockets/ ----------------------------------------------------------

  it("an internal POST to /.sockets/ delivers a frame; exact and subtree select differently", async () => {
    const a = await connect("/echo/room/a", bearer);
    const b = await connect("/echo/room/a/sub", bearer);

    const exact = await jsonCall("/echo/.sockets/room/a", "POST", bearer, { push: 1 });
    expect(exact.status, exact.text).toBe(200);
    expect(jsonOf(exact).sent).toBe(1);
    expect(JSON.parse(await a.next())).toEqual({ push: 1 });

    const subtree = await jsonCall("/echo/.sockets/room/a/", "POST", bearer, { push: 2 });
    expect(subtree.status, subtree.text).toBe(200);
    expect(jsonOf(subtree).sent).toBe(2);
    expect(JSON.parse(await a.next())).toEqual({ push: 2 });
    expect(JSON.parse(await b.next())).toEqual({ push: 2 });

    await shut(a, b);
  });

  it("a closed socket is not a recipient: the next socket on the DO still gets its frame", async () => {
    // Regression (conformance report): after one connect/close cycle, a send
    // to the *next* socket reported `sent: 1` and delivered nothing. A socket
    // whose close handshake is still in flight can linger in
    // `getWebSockets()` with its attachment intact, so the hub counts only
    // sockets that are actually open — and `webSocketClose` completes the
    // handshake before it dispatches anything.
    const first = await connect("/echo/cycle/one", bearer);
    first.ws.send(JSON.stringify({ text: "warm" }));
    expect(JSON.parse(await first.next())).toEqual({ echo: "warm" });
    first.ws.close(1000, "bye"); // deliberately not awaited: the race is the point

    const second = await connect("/echo/cycle/two", bearer);
    const push = await jsonCall("/echo/.sockets/cycle/two", "POST", bearer, { push: "second" });
    expect(push.status, push.text).toBe(200);
    expect(jsonOf(push).sent).toBe(1);
    expect(JSON.parse(await second.next())).toEqual({ push: "second" });

    // The whole-mount subtree sees exactly one socket, not the closed one.
    const all = await jsonCall("/echo/.sockets/", "POST", bearer, { push: "all" });
    expect(jsonOf(all).sent).toBe(1);
    expect(JSON.parse(await second.next())).toEqual({ push: "all" });
    // …and the second socket still dispatches its own frames.
    second.ws.send(JSON.stringify({ text: "alive" }));
    expect(JSON.parse(await second.next())).toEqual({ echo: "alive" });
    await shut(second);
  });

  // ---- hibernation ---------------------------------------------------------

  it("survives eviction: identity comes from the attachment, not from memory", async () => {
    const sock = await connect("/echo/room/42", bearer);
    sock.ws.send(JSON.stringify({ text: "before" }));
    expect(JSON.parse(await sock.next())).toEqual({ echo: "before" });

    // Every in-memory field of the object (`runtime`, `logStore`, the frame
    // meters, the limit table) is torn down while the connection stays open.
    await evict();

    sock.ws.send(JSON.stringify({ text: "after" }));
    expect(JSON.parse(await sock.next())).toEqual({ echo: "after" });

    // …and the rebuilt object still knows who is connected where: path and
    // principal come from the attachment, not from anything still in memory.
    const list = await call("/echo/.sockets/room/", { headers: as(bearer) });
    expect(list.status, list.text).toBe(200);
    expect(list.headers.get("content-type")).toContain("vnd.rs2.dir+json");
    const entries = jsonOf(list).entries as JsonObject[];
    const mine = entries.find((e) => e.path === "/echo/room/42");
    expect(mine, list.text).toBeDefined();
    expect(mine!.user).toBe(ADMIN.email);

    // A DELETE through the same subtree closes it.
    const del = await call("/echo/.sockets/room/42?code=1000&reason=bye", { method: "DELETE", headers: as(bearer) });
    expect(del.status, del.text).toBe(200);
    expect(jsonOf(del).closed).toBe(1);
    expect((await sock.closed()).code).toBe(1000);
  });

  it("a principal too big for the attachment spills to storage and reads back", async () => {
    // `principal.extra` is arbitrary JWT claims, so the attachment keeps only
    // the fields the hub matches on and the full record goes to `ws:<id>`.
    // Both halves have to work: the listing comes from the attachment, the
    // dispatch from the spill.
    // The user dataset lives in the tenant's data namespace, reachable
    // through any `data` mount — here the open one.
    const user = await call("/plain/users/a@ws.test");
    expect(user.status, user.text).toBe(200);
    const record = { ...jsonOf(user), bio: "b".repeat(4_000) };
    const saved = await jsonCall("/plain/users/a@ws.test", "PUT", undefined, record);
    expect([200, 201, 204], saved.text).toContain(saved.status);
    const login = await jsonCall("/auth/login", "POST", undefined, ADMIN);
    expect(login.status, login.text).toBe(200);
    const fat = jsonOf(login).token as string;

    const sock = await connect("/echo/room/spill", fat);
    const list = await call("/echo/.sockets/room/spill", { headers: as(bearer) });
    expect(list.status, list.text).toBe(200);
    const entry = (jsonOf(list).entries as JsonObject[])[0]!;
    expect(entry.user).toBe(ADMIN.email);
    const spilled = await runInDurableObject(tenantStub(), (_i, state) => state.storage.get<string>(`ws:${entry.name}`));
    expect(spilled, "the over-large accept should be in DO storage").toBeDefined();
    expect(spilled!).toContain("b".repeat(100));

    // The spill is what a frame reloads — across eviction, with nothing left
    // in memory to fall back on.
    await evict();
    sock.ws.send(JSON.stringify({ text: "fat" }));
    expect(JSON.parse(await sock.next())).toEqual({ echo: "fat" });

    // Closing releases the spill.
    await shut(sock);
    const gone = await runInDurableObject(tenantStub(), (_i, state) => state.storage.get<string>(`ws:${entry.name}`));
    expect(gone).toBeUndefined();
  });

  it("a long-lived connection is not inside a wall clock", async () => {
    // The connection itself is never timed: each event is its own dispatch,
    // so the 30 s service wall clock is per message. A full 30 s of wall time
    // is not worth a test's runtime, so this asserts the mechanism at the
    // smallest scale — a socket that idles between dispatches still answers,
    // and nothing closes it in between.
    const sock = await connect("/echo/room/long", bearer);
    for (let i = 0; i < 3; i++) {
      await new Promise((r) => setTimeout(r, 150));
      sock.ws.send(JSON.stringify({ text: `n${i}` }));
      expect(JSON.parse(await sock.next())).toEqual({ echo: `n${i}` });
    }
    expect(sock.ws.readyState).toBe(WebSocket.READY_STATE_OPEN);
    await shut(sock);
  });
});

// ---- per-frame limits ------------------------------------------------------
// `RS2_LIMITS` is read once per object, so the override lands by evicting the
// DO around it — the same teardown the hibernation test uses, here to rebuild
// the limit table.

describe("per-frame limits", () => {
  const OVERRIDE = JSON.stringify({
    wsMessageBytes: 64,
    wsMessagesPerSecond: 4,
    wsMessagesInFlight: 2,
    wsSocketsPerTenant: 2,
  });

  beforeAll(async () => {
    await seedTenant();
    env.RS2_LIMITS = OVERRIDE;
    await evict();
  });

  afterAll(async () => {
    env.RS2_LIMITS = "";
    await evict();
  });

  it("an oversized frame closes 4413 naming the limit", async () => {
    const sock = await connect("/echo/room/big", bearer);
    sock.ws.send(JSON.stringify({ text: "x".repeat(200) }));
    const close = await sock.closed();
    expect(close.code).toBe(4413);
    expect(close.reason).toBe("limit_exceeded:ws_message_bytes");
  });

  it("a frame burst over the per-second cap closes 4429", async () => {
    const sock = await connect("/echo/room/fast", bearer);
    for (let i = 0; i < 12; i++) sock.ws.send(JSON.stringify({ text: "t" }));
    const close = await sock.closed();
    expect(close.code).toBe(4429);
    // Either the rate or the in-flight cap may bind first in a burst this
    // tight; both are 4429 and both name themselves.
    expect(["limit_exceeded:ws_messages_per_second", "limit_exceeded:ws_messages_in_flight"]).toContain(close.reason);
  });

  it("the sockets-per-tenant cap refuses the upgrade rather than accepting it", async () => {
    const held: Socket[] = [];
    for (let i = 0; i < 2; i++) held.push(await connect(`/echo/room/cap${i}`, bearer));
    const over = await upgrade("/echo/room/cap-over", bearer);
    expect(over.status, over.text).toBe(503);
    expect(over.socket).toBeUndefined();
    expect(jsonOf(over).limit).toBe("ws_sockets_per_tenant");
    await shut(...held);
  });

  it("the overrides are what the defaults are not (the mechanism, not the values)", () => {
    expect(defaultLimits().wsMessageBytes).toBeGreaterThan(64);
    expect(defaultLimits().wsSocketsPerTenant).toBeGreaterThan(2);
  });
});
