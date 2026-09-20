// Inbound WebSockets over HTTP (docs/agents/websocket.md; spec
// docs/agents/cloudflare.md §E.6/§F). A socket is never a second dispatch
// path: every event is a synthetic `system` POST to the connect URL, and
// every outbound frame is an ordinary request to the mount's reserved
// `/.sockets/` subtree. This suite proves that contract black-box.
//
// Implemented on the Worker host only, so far (`divergences().webSocket`:
// `"served"` on cloudflare, `"absent"` on rust). The live-socket suite below
// is gated on `"served"` and skips cleanly on the Rust host. The one case
// that runs on `"absent"` pins the negative contract instead: a
// `webSocket: true` mount still answers `Upgrade` as a plain GET — a 101 is
// never sent when there is no `SocketHub` to hand the connection to.
//
// Two things `fetch` cannot do, so this file reaches past `Rs2Client`:
//   - `fetch` forbids setting the `Upgrade` header, so the rejection cases
//     (which must read the problem body of a *refused* upgrade) use
//     `node:http` directly and race its `'response'` (no 101) and
//     `'upgrade'` (101) events.
//   - actually opening a socket uses Node 22's global `WebSocket` (built in,
//     no dependency added). Auth rides the `Sec-WebSocket-Protocol` bearer
//     convention from the design doc (`rs2.bearer.<jwt>`), since neither
//     `fetch` nor the WHATWG `WebSocket` constructor can set `Authorization`
//     on the handshake — the same restriction a browser client has.
//
// Because the raw-socket helpers dial `RS2_BASE_URL` directly (converting
// `http(s)` to `ws(s)`), they send whatever `Host` the socket layer derives
// from the URL rather than `RS2_HOST` explicitly. In every run this repo's
// scripts set up, the two are the same value, so it is not asserted on here.

import { randomBytes } from "node:crypto";
import * as http from "node:http";
import type { IncomingHttpHeaders, IncomingMessage } from "node:http";
import type { Socket } from "node:net";

import { afterAll, beforeAll, describe, expect, test } from "vitest";

import { env, Rs2Client, type Rs2Response } from "./src/client.ts";
import { divergences } from "./src/divergences.ts";
import { type Mount, Seed } from "./src/seed.ts";

/// The subprotocol convention from docs/agents/websocket.md: a client that
/// cannot set `Authorization` offers `rs2.bearer.<jwt>` alongside a real
/// protocol. The host selects the first non-bearer entry.
const BEARER_PREFIX = "rs2.bearer.";
const CODE_BASE = "/services/code";

function wsUrl(path: string): string {
  return env().baseUrl.replace(/^http/, "ws") + path;
}

/// Wait for one event on a WebSocket (or any EventTarget-shaped emitter).
/// Frames are queued from the moment the socket exists: a frame pushed by the
/// host can land before the HTTP response that caused it is read, so a
/// listener armed after the request would miss it.
const frames = new WeakMap<WebSocket, { queue: MessageEvent[]; waiters: ((ev: MessageEvent) => void)[] }>();

function track(ws: WebSocket): WebSocket {
  const state = { queue: [] as MessageEvent[], waiters: [] as ((ev: MessageEvent) => void)[] };
  frames.set(ws, state);
  ws.addEventListener("message", (ev) => {
    const waiter = state.waiters.shift();
    if (waiter) waiter(ev);
    else state.queue.push(ev);
  });
  return ws;
}

function once<T = Event>(target: WebSocket, type: string, timeoutMs = 8000): Promise<T> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`[ws] timed out waiting for '${type}'`)), timeoutMs);
    const state = type === "message" ? frames.get(target) : undefined;
    if (state) {
      const done = (ev: MessageEvent): void => {
        clearTimeout(timer);
        resolve(ev as T);
      };
      const queued = state.queue.shift();
      if (queued) done(queued);
      else state.waiters.push(done);
      return;
    }
    target.addEventListener(
      type,
      (ev) => {
        clearTimeout(timer);
        resolve(ev as T);
      },
      { once: true },
    );
  });
}

function jsonFrame(ev: MessageEvent): unknown {
  if (typeof ev.data !== "string") throw new Error(`[ws] expected a text frame, got ${typeof ev.data}`);
  return JSON.parse(ev.data);
}

interface RawUpgradeResult {
  status: number;
  headers: IncomingHttpHeaders;
  body: string;
  /** Present only on a 101: the raw socket, so the caller can close it. */
  socket?: Socket;
}

/**
 * A raw HTTP upgrade request (`node:http`, not `fetch` — see file header).
 * Resolves on either the `'response'` event (host declined the upgrade; the
 * problem body is readable) or the `'upgrade'` event (host answered 101).
 */
function rawUpgradeRequest(path: string, opts: { token?: string; protocols?: string[] } = {}): Promise<RawUpgradeResult> {
  const e = env();
  const url = new URL(e.baseUrl);
  const headers: http.OutgoingHttpHeaders = {
    Host: e.host,
    Connection: "Upgrade",
    Upgrade: "websocket",
    "Sec-WebSocket-Key": randomBytes(16).toString("base64"),
    "Sec-WebSocket-Version": "13",
  };
  if (opts.token) headers.Authorization = `Bearer ${opts.token}`;
  if (opts.protocols) headers["Sec-WebSocket-Protocol"] = opts.protocols.join(", ");
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: url.hostname,
      port: url.port || (url.protocol === "https:" ? 443 : 80),
      path,
      method: "GET",
      headers,
    });
    req.on("response", (res: IncomingMessage) => {
      const chunks: Buffer[] = [];
      res.on("data", (c: Buffer) => chunks.push(c));
      res.on("end", () => {
        resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks).toString("utf8") });
      });
    });
    req.on("upgrade", (res: IncomingMessage, socket: Socket) => {
      resolve({ status: res.statusCode ?? 101, headers: res.headers, body: "", socket });
    });
    req.on("error", reject);
    req.end();
  });
}

function status(res: Rs2Response, want: number, msg: string): void {
  expect(res.status, `${msg}: ${res.describe()}`).toBe(want);
}

// ---------------------------------------------------------------------------
// Rust host: the negative contract only. No `SocketHub` is wired up yet, so
// an `Upgrade` request to a `webSocket: true` mount must fall back to the
// plain GET — a 101 is never sent before (or instead of) authorization.
// ---------------------------------------------------------------------------
describe.runIf(divergences().webSocket === "absent")("websocket (rust: no SocketHub yet)", () => {
  let seed: Seed | undefined;

  afterAll(async () => {
    await seed?.restore();
  });

  test("a webSocket:true mount answers Upgrade as a plain GET, never 101", async (ctx) => {
    seed = await Seed.create();
    const probe: Mount = { path: "/ws-probe", service: "pipeline", config: { access: "open", webSocket: true } };
    const applied = await seed.tryApplyMounts([probe]);
    if (applied.status !== 204) {
      // Config validation rejects the unknown `webSocket` field before an
      // upgrade path exists at all — nothing left to probe here.
      ctx.skip();
      return;
    }
    const spec = await seed.admin.put("/ws-probe/.pipelines/.root", {
      json: { pipeline: { steps: [{ transform: { ok: true } }] } },
    });
    status(spec, 201, "[ws-probe] author .root");

    const res = await rawUpgradeRequest("/ws-probe/anything");
    expect(res.status, `[ws-probe] upgrade must not succeed: ${res.status} ${res.body}`).not.toBe(101);
    // The mount ignores `Upgrade` and serves the ordinary GET (RFC 9110):
    // the pipeline ran and answered normally.
    expect(res.status, `[ws-probe] falls back to a plain GET: ${res.status} ${res.body}`).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ ok: true });
    res.socket?.destroy();
  });
});

// ---------------------------------------------------------------------------
// Cloudflare host: the live contract.
// ---------------------------------------------------------------------------
describe.runIf(divergences().webSocket === "served")("websocket (cloudflare: SocketHub)", () => {
  // A `pipeline` mount defers execution-path access to the matched spec,
  // which an upgrade never reaches — so the handshake is held to the
  // MOUNT's read role (websocket.md, "Pipeline mounts"). AUTH_MOUNT (`data`)
  // exercises rejection; PIPE_MOUNT declares `read: "all"` so anonymous
  // sockets connect and prove the message-dispatch path.
  const AUTH_MOUNT = "/ws-auth";
  const PIPE_MOUNT = "/ws-pipe";
  const SOCK_MOUNT = "/sock";
  const NOTIFY_MOUNT = "/notify";
  const GUEST_MOUNT = "/ws-guest";
  const GUEST_NAME = "conf-ws-guest";

  const DEV_U = { email: "wsdev@conf.test", password: "wsdev-pw", roles: "U" };
  const OTHER_E = { email: "wsother@conf.test", password: "wsother-pw", roles: "E" };

  /** onOpen greets; onMessage echoes the frame with a marker (code.test.ts pattern). */
  const GUEST_BUNDLE = `
export default async (msg, ctx) => {
  // A non-socket invocation (an HTTP request here; a scheduler tick is the
  // same shape) pushing to the mount's own sockets through ctx.sockets.
  if (msg.method === "POST") {
    const pushed = await ctx.sockets.send({ path: "/room/", subtree: true }, { pushed: msg.body });
    const listed = await ctx.sockets.list({ path: "/room" });
    return { status: 200, body: { sent: pushed.sent, total: listed.total } };
  }
  return { status: 200, body: { method: msg.method } };
};
export async function onOpen(msg, ctx, socket) {
  await socket.send(JSON.stringify({ greeting: "hello" }));
}
export async function onMessage(msg, ctx, socket) {
  await socket.send(JSON.stringify({ echo: msg.body, marker: "guest" }));
}
`;

  let seed: Seed;
  let admin: Rs2Client;
  let anon: Rs2Client;
  let devToken: string;
  let otherToken: string;

  beforeAll(async () => {
    seed = await Seed.create();
    admin = seed.admin;
    anon = seed.anon;

    const deployed = await admin.post(`${CODE_BASE}/${GUEST_NAME}/`, {
      body: GUEST_BUNDLE,
      contentType: "application/javascript",
    });
    status(deployed, 201, "[ws-guest] deploy bundle");
    const guestRef = deployed.json<{ ref: string }>().ref;

    await seed.applyMounts([
      // `data`-backed, read-role gated — mount-level `checkAccess` runs
      // unconditionally here, so this is the target for the rejection and
      // auth-via-subprotocol cases.
      { path: AUTH_MOUNT, service: "data", config: { access: { read: "U", write: "A" }, webSocket: true } },
      // Open pipeline mount — proves the message -> pipeline dispatch wiring
      // (no role gate; see the note above on pipeline mounts and checkAccess).
      { path: PIPE_MOUNT, service: "pipeline", config: { access: { read: "all", invoke: "all", write: "A" }, webSocket: true } },
      // Open connect side, write-gated `.sockets/` — the target for the
      // internal-send and pipeline-send cases.
      { path: SOCK_MOUNT, service: "pipeline", config: { access: { read: "all", write: "A", invoke: "all" }, webSocket: true } },
      // A plain pipeline mount (no socket of its own) whose spec posts into
      // SOCK_MOUNT's `.sockets/` subtree via an ordinary `call` step.
      { path: NOTIFY_MOUNT, service: "pipeline", config: { access: { invoke: "A", write: "A" } } },
      // `code:` mount with guest open/message handlers.
      { path: GUEST_MOUNT, service: guestRef, config: { access: { read: "all", invoke: "all", write: "A" }, webSocket: true } },
    ]);

    await seed.createPrincipals([DEV_U, OTHER_E]);
    devToken = await seed.login(DEV_U.email, DEV_U.password);
    otherToken = await seed.login(OTHER_E.email, OTHER_E.password);

    // Stored pipeline specs live under the mount path and outlive a config
    // swap (pipeline.test.ts), so a prior interrupted run of this suite can
    // leave one behind — PUT is create-or-update (201 the first time, 200
    // after), never asserted stricter than that here.
    const pipeRoot = await admin.put(`${PIPE_MOUNT}/.pipelines/.root`, {
      // JSONata transform: `$` is the whole current value (the frame body),
      // not a literal field named `body` (pipeline.test.ts / m3-surface.test.ts).
      json: { pipeline: { steps: [{ transform: { received: "$", via: "'pipeline'" } }] } },
    });
    expect([200, 201], `[ws-pipe] author .root: ${pipeRoot.describe()}`).toContain(pipeRoot.status);

    const notifyRoot = await admin.put(`${NOTIFY_MOUNT}/.pipelines/.root`, {
      json: { pipeline: { steps: [{ call: { method: "POST", url: `${SOCK_MOUNT}/.sockets/room/a` } }] } },
    });
    expect([200, 201], `[notify] author .root: ${notifyRoot.describe()}`).toContain(notifyRoot.status);
  });

  afterAll(async () => {
    // Drop the stored specs while their mounts still exist (pipeline.test.ts
    // pattern), then restore the base config.
    const dropPipe = await admin.delete(`${PIPE_MOUNT}/.pipelines/.root`);
    if (![204, 404].includes(dropPipe.status)) throw new Error(`cleanup [ws-pipe] .root: ${dropPipe.describe()}`);
    const dropNotify = await admin.delete(`${NOTIFY_MOUNT}/.pipelines/.root`);
    if (![204, 404].includes(dropNotify.status)) throw new Error(`cleanup [notify] .root: ${dropNotify.describe()}`);
    await seed?.restore();
    const del = await admin.delete(`${CODE_BASE}/${GUEST_NAME}/?confirm=${GUEST_NAME}`);
    if (![204, 404].includes(del.status)) throw new Error(`cleanup ${GUEST_NAME}: ${del.describe()}`);
  });

  // ---- discovery ------------------------------------------------------------

  test("discovery: the flagged mount carries the websocket facet and limits", async () => {
    // AUTH_MOUNT's read role is "U" (it is also the rejection target below),
    // and discovery only lists what the requesting principal can read — an
    // anonymous or admin (role "A", not "U") caller does not see it. Read as
    // the "U" principal instead; SOCK_MOUNT (read: "all") is checked as anon
    // for a second confirmation of the same facet/limits shape.
    const asDevU = seed.anon.withToken(devToken);
    const doc = await asDevU.getJson("/.well-known/rs2/services");
    const entry = doc.services.find((s: { path: string }) => s.path === AUTH_MOUNT);
    expect(entry, `[ws-auth] missing from discovery: ${JSON.stringify(doc.services)}`).toBeDefined();
    expect(entry.facets, `[ws-auth] facets: ${JSON.stringify(entry)}`).toContain("websocket");

    const limits = doc.limits?.webSocket;
    expect(limits, `[ws-auth] limits.webSocket missing: ${JSON.stringify(doc.limits)}`).toBeTypeOf("object");
    for (const key of ["messageBytes", "messagesInFlight", "messagesPerSecond", "socketsPerTenant"]) {
      expect(typeof limits[key], `[ws-auth] limits.webSocket.${key}`).toBe("number");
      expect(limits[key], `[ws-auth] limits.webSocket.${key} > 0`).toBeGreaterThan(0);
    }

    const opt = await asDevU.options(AUTH_MOUNT);
    status(opt, 200, "[ws-auth] OPTIONS");
    expect((opt.json().facets as string[]), `[ws-auth] OPTIONS facets: ${opt.text()}`).toContain("websocket");

    const sockDoc = await anon.getJson("/.well-known/rs2/services");
    const sockEntry = sockDoc.services.find((s: { path: string }) => s.path === SOCK_MOUNT);
    expect(sockEntry, `[sock] missing from discovery: ${JSON.stringify(sockDoc.services)}`).toBeDefined();
    expect(sockEntry.facets, `[sock] facets: ${JSON.stringify(sockEntry)}`).toContain("websocket");
  });

  // ---- upgrade rejection ------------------------------------------------------

  test("upgrade rejection: unauthenticated -> 401, wrong role -> 403, never a 101", async () => {
    const anonUp = await rawUpgradeRequest(`${AUTH_MOUNT}/x`);
    expect(anonUp.status, `[ws-auth] anon upgrade must not be 101: ${anonUp.status}`).not.toBe(101);
    expect(anonUp.status, `[ws-auth] anon upgrade: ${anonUp.status} ${anonUp.body}`).toBe(401);
    expect(anonUp.headers["content-type"], `[ws-auth] anon upgrade content-type`).toBe("application/problem+json");
    const anonProblem = JSON.parse(anonUp.body);
    expect(anonProblem.code, "[ws-auth] anon upgrade problem code").toBe("unauthorized");
    anonUp.socket?.destroy();

    const wrongUp = await rawUpgradeRequest(`${AUTH_MOUNT}/x`, { token: otherToken });
    expect(wrongUp.status, `[ws-auth] wrong-role upgrade must not be 101: ${wrongUp.status}`).not.toBe(101);
    expect(wrongUp.status, `[ws-auth] wrong-role upgrade: ${wrongUp.status} ${wrongUp.body}`).toBe(403);
    expect(wrongUp.headers["content-type"], `[ws-auth] wrong-role upgrade content-type`).toBe("application/problem+json");
    const wrongProblem = JSON.parse(wrongUp.body);
    expect(wrongProblem.code, "[ws-auth] wrong-role upgrade problem code").toBe("forbidden");
    wrongUp.socket?.destroy();
  });

  // ---- accept + pipeline trigger, auth via subprotocol -----------------------

  test("accept + pipeline trigger: a JSON text frame runs the mount's pipeline", async () => {
    const ws = track(new WebSocket(wsUrl(`${PIPE_MOUNT}/x`)));
    try {
      await once(ws, "open");
      ws.send(JSON.stringify({ n: 1 }));
      const ev = await once<MessageEvent>(ws, "message");
      expect(jsonFrame(ev), "[ws-pipe] reply frame is the pipeline output").toEqual({ received: { n: 1 }, via: "pipeline" });
    } finally {
      ws.close();
      await once(ws, "close").catch(() => {});
    }
  });

  test("auth via subprotocol: rs2.bearer.<jwt> alongside rs2 opens the role-gated mount and selects rs2", async () => {
    const ws = track(new WebSocket(wsUrl(`${AUTH_MOUNT}/y`), ["rs2", `${BEARER_PREFIX}${devToken}`]));
    try {
      await once(ws, "open");
      expect(ws.protocol, "[ws] selected subprotocol").toBe("rs2");
    } finally {
      ws.close();
      await once(ws, "close").catch(() => {});
    }
  });

  // ---- internal send: /.sockets/ ---------------------------------------------

  test("internal send: exact path, subtree, siblings, listing, close, and the write gate", async () => {
    const ws = track(new WebSocket(wsUrl(`${SOCK_MOUNT}/room/a`)));
    try {
      await once(ws, "open");

      // Exact match.
      let res = await admin.post(`${SOCK_MOUNT}/.sockets/room/a`, { json: { hello: 1 } });
      status(res, 200, "[sock] POST exact path");
      expect(res.json()).toEqual({ sent: 1 });
      let frame = await once<MessageEvent>(ws, "message");
      expect(jsonFrame(frame), "[sock] exact-path frame").toEqual({ hello: 1 });

      // Subtree (trailing slash).
      res = await admin.post(`${SOCK_MOUNT}/.sockets/room/`, { json: { hello: 2 } });
      status(res, 200, "[sock] POST subtree");
      expect(res.json()).toEqual({ sent: 1 });
      frame = await once<MessageEvent>(ws, "message");
      expect(jsonFrame(frame), "[sock] subtree frame").toEqual({ hello: 2 });

      // Sibling path: no socket there.
      res = await admin.post(`${SOCK_MOUNT}/.sockets/room/b`, { json: { hello: 3 } });
      status(res, 200, "[sock] POST sibling");
      expect(res.json(), "[sock] sibling path matches nothing").toEqual({ sent: 0 });

      // Listing.
      res = await admin.get(`${SOCK_MOUNT}/.sockets/room/`);
      status(res, 200, "[sock] GET listing");
      const listing = res.listing();
      expect(res.totalCount(), "[sock] X-Total-Count").toBe(1);
      expect(listing.total, "[sock] listing.total").toBe(1);
      expect(listing.entries.length, "[sock] one entry").toBe(1);
      const entry = listing.entries[0];
      expect(typeof entry.name, "[sock] entry.name (socket id)").toBe("string");
      expect(entry.dir, "[sock] a socket is never a directory entry").toBe(false);

      // Unauthenticated write against the write-restricted mount.
      res = await anon.post(`${SOCK_MOUNT}/.sockets/room/a`, { json: {} });
      status(res, 401, "[sock] unauthenticated POST .sockets/");
      expect(res.problem().code, "[sock] unauthenticated POST problem code").toBe("unauthorized");

      // Close by id.
      // Armed before the request: the close can land ahead of its response.
      const closed = once<CloseEvent>(ws, "close");
      res = await admin.delete(`${SOCK_MOUNT}/.sockets/?$id=${encodeURIComponent(entry.name)}`);
      status(res, 200, "[sock] DELETE by id");
      expect(res.json()).toEqual({ closed: 1 });
      // The close frame must arrive (readyState leaves OPEN). The `close`
      // EVENT additionally needs the transport torn down, which local workerd
      // does not do for a close issued from another request's context
      // (websocket.md, "Known issues") — so the code is asserted when the
      // event fires and the frame alone is required otherwise.
      const closeEv = await Promise.race([closed.catch(() => undefined), new Promise<undefined>((r) => setTimeout(r, 3000))]);
      expect(ws.readyState, "[sock] client saw the close frame").toBeGreaterThanOrEqual(ws.CLOSING);
      if (closeEv) expect(closeEv.code, "[sock] client observes the requested close code").toBe(1000);
    } finally {
      if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) ws.close();
    }
  });

  // ---- pipeline send: a `call` step posts into another mount's .sockets ------

  test("pipeline send: an HTTP request to a pipeline delivers a frame via a call step", async () => {
    const ws = track(new WebSocket(wsUrl(`${SOCK_MOUNT}/room/a`)));
    try {
      await once(ws, "open");
      const res = await admin.post(NOTIFY_MOUNT, { json: { via: "pipeline-call" } });
      status(res, 200, "[notify] POST triggers the call step");
      const frame = await once<MessageEvent>(ws, "message");
      expect(jsonFrame(frame), "[notify] the call step's body reaches the socket").toEqual({ via: "pipeline-call" });
    } finally {
      ws.close();
      await once(ws, "close").catch(() => {});
    }
  });

  // ---- guest handlers ---------------------------------------------------------

  test("guest handlers: onOpen greets, onMessage echoes with a marker", async () => {
    const ws = track(new WebSocket(wsUrl(`${GUEST_MOUNT}/x`)));
    try {
      await once(ws, "open");
      const greeting = await once<MessageEvent>(ws, "message");
      expect(jsonFrame(greeting), "[ws-guest] onOpen greeting").toEqual({ greeting: "hello" });

      ws.send(JSON.stringify({ n: 7 }));
      const echo = await once<MessageEvent>(ws, "message");
      expect(jsonFrame(echo), "[ws-guest] onMessage echo").toEqual({ echo: { n: 7 }, marker: "guest" });
    } finally {
      ws.close();
      await once(ws, "close").catch(() => {});
    }
  });

  test("guest push: a plain invocation reaches the mount's own sockets via ctx.sockets", async () => {
    const inRoom = track(new WebSocket(wsUrl(`${GUEST_MOUNT}/room/7`)));
    const elsewhere = track(new WebSocket(wsUrl(`${GUEST_MOUNT}/lobby`)));
    try {
      await Promise.all([once(inRoom, "open"), once(elsewhere, "open")]);
      expect(jsonFrame(await once<MessageEvent>(inRoom, "message"))).toEqual({ greeting: "hello" });
      expect(jsonFrame(await once<MessageEvent>(elsewhere, "message"))).toEqual({ greeting: "hello" });

      // Anonymous caller, no write role on the mount: the push is the
      // service's own act on its own mount, not the caller's.
      const res = await anon.post(`${GUEST_MOUNT}/notify`, { json: { n: 1 } });
      status(res, 200, "[ws-guest] push invocation");
      expect(res.json(), "[ws-guest] one socket in /room/, listed").toEqual({ sent: 1, total: 1 });
      expect(jsonFrame(await once<MessageEvent>(inRoom, "message"))).toEqual({ pushed: { n: 1 } });

      // …while the same caller cannot write to /.sockets/ directly.
      const direct = await anon.post(`${GUEST_MOUNT}/.sockets/room/`, { json: { n: 2 } });
      expect([401, 403], "[ws-guest] direct .sockets/ write is still gated").toContain(direct.status);
    } finally {
      for (const ws of [inRoom, elsewhere]) {
        ws.close();
        await once(ws, "close").catch(() => {});
      }
    }
  });

  // ---- limits -------------------------------------------------------------------

  test("limits: a frame over messageBytes is closed 4413 limit_exceeded:ws_message_bytes", async (ctx) => {
    const disc = await anon.getJson("/.well-known/rs2/services");
    const cap = Number(disc.limits?.webSocket?.messageBytes);
    if (!(cap > 0) || cap > 8 * 1024 * 1024) {
      ctx.skip(`[ws] messageBytes=${cap} exceeds the 8 MiB test budget`);
      return;
    }
    const ws = track(new WebSocket(wsUrl(`${PIPE_MOUNT}/z`)));
    await once(ws, "open");
    ws.send("x".repeat(cap + 1));
    const closeEv = await once<CloseEvent>(ws, "close", 15_000);
    expect(closeEv.code, "[ws] oversize frame close code").toBe(4413);
    expect(closeEv.reason, "[ws] oversize frame close reason").toBe("limit_exceeded:ws_message_bytes");
  });
});
