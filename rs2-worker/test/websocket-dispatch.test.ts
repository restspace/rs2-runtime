// Inbound WebSockets, the Runtime half (docs/agents/websocket.md): the
// upgrade handshake in `dispatch`, the reserved `/.sockets/` subtree, the
// CSWSH guard, and what discovery publishes. Everything here is
// platform-free — the hub is a fake, so these are the same assertions the
// Rust host will have to satisfy.

import { describe, expect, it } from "vitest";

import { seedBuiltins } from "../src/runtime/tenant-build";
import type { Adapters } from "../src/runtime/tenant-build";
import { Runtime } from "../src/runtime/dispatch";
import type { RuntimeHost } from "../src/runtime/dispatch";
import { Body } from "../src/runtime/body";
import { codes } from "../src/runtime/error";
import type { Json, JsonObject } from "../src/runtime/error";
import { InfraSet } from "../src/runtime/infra";
import { NullLogStore, Severity } from "../src/runtime/logging";
import { MediaType } from "../src/runtime/media-type";
import { Message, simpleUuid } from "../src/runtime/message";
import { selectorMatches, socketEventMessage } from "../src/runtime/sockets";
import type { SocketAccept, SocketHub, SocketInfo, SocketSelector } from "../src/runtime/sockets";
import { defaultLimits } from "../src/runtime/wrapper";
import type { LimitTable } from "../src/runtime/wrapper";
import { sign } from "../src/services/auth";
import type {
  ByteRange,
  DataStore,
  DirEntry,
  FileMeta,
  FileStore,
  WriteOutcome,
  WritePrecondition,
} from "../src/capabilities/types";

const JWT_SECRET = "socket-test-secret";

// ---- fakes ---------------------------------------------------------------

/// A hub over a plain array of accepts, matched with the shared
/// `selectorMatches` so the selector a test asserts on is the selector the
/// real hub would have used.
class FakeHub implements SocketHub {
  accepts: SocketAccept[] = [];
  sends: Array<{ sel: SocketSelector; frame: string | Uint8Array }> = [];
  closes: Array<{ sel: SocketSelector; code: number; reason: string }> = [];
  lists: SocketSelector[] = [];
  /// What `count()` reports, when a test wants to sit at the ceiling.
  countOverride: number | undefined;

  private matching(sel: SocketSelector): SocketAccept[] {
    return this.accepts.filter((a) => selectorMatches(sel, a));
  }

  add(mount: string, path: string, user?: string): SocketAccept {
    const a: SocketAccept = {
      id: simpleUuid(),
      tenant: "t",
      mount,
      path,
      query: "",
      principal: user === undefined ? undefined : { id: user, roles: [], kind: "user", extra: {} },
      exp: undefined,
      text: "json",
      events: ["message"],
      protocol: undefined,
      connectedAt: 1_700_000_000_000,
    };
    this.accepts.push(a);
    return a;
  }

  count(): number {
    return this.countOverride ?? this.accepts.length;
  }
  send(sel: SocketSelector, frame: string | Uint8Array): number {
    this.sends.push({ sel, frame });
    return this.matching(sel).length;
  }
  close(sel: SocketSelector, code: number, reason: string): number {
    this.closes.push({ sel, code, reason });
    return this.matching(sel).length;
  }
  list(sel: SocketSelector): SocketInfo[] {
    this.lists.push(sel);
    return this.matching(sel).map((a) => ({
      id: a.id,
      path: a.path,
      user: a.principal?.id,
      connectedAt: a.connectedAt,
    }));
  }
  /// Nothing reached the hub at all (an authorization failure must not).
  untouched(): boolean {
    return this.sends.length === 0 && this.closes.length === 0 && this.lists.length === 0;
  }
}

/// The only file-store operation the runtime layer needs here is `read` (the
/// spec store's stored-pipeline lookup); everything else throws loudly.
class MemFileStore implements FileStore {
  readonly files = new Map<string, string>();

  put(tenant: string, path: string, text: string): void {
    this.files.set(`${tenant}${path}`, text);
  }
  private unsupported(): never {
    throw new Error("MemFileStore: operation not used by these tests");
  }
  async head(): Promise<FileMeta> {
    this.unsupported();
  }
  async read(tenant: string, path: string, _range: ByteRange | undefined): Promise<Body> {
    const text = this.files.get(`${tenant}${path}`);
    if (text === undefined) throw new Error(`no file '${path}'`);
    return Body.fromString(text, MediaType.json());
  }
  async write(): Promise<boolean> {
    this.unsupported();
  }
  async currentEtag(): Promise<string | undefined> {
    return undefined;
  }
  async writeCond(): Promise<WriteOutcome> {
    this.unsupported();
  }
  conditionalWriteAtomic(): boolean {
    return false;
  }
  async delete(): Promise<void> {
    this.unsupported();
  }
  async deleteCond(): Promise<void> {
    this.unsupported();
  }
  async rename(): Promise<boolean> {
    this.unsupported();
  }
  async deleteDir(): Promise<void> {
    this.unsupported();
  }
  async deleteDirAll(): Promise<void> {
    this.unsupported();
  }
  async list(_tenant: string, _path: string, _take: number, _skip: number): Promise<[DirEntry[], number]> {
    return [[], 0];
  }
}

/// An empty data store — enough for the `data` mounts these tests route to.
const emptyData: DataStore = {
  async get() {
    throw new Error("no record");
  },
  async put() {
    return true;
  },
  async delete() {},
  async listKeys() {
    return [[], 0];
  },
  async listDatasets() {
    return [[], 0];
  },
  async getSchema() {
    return undefined;
  },
  async putSchema() {},
  async deleteDataset() {},
  async scanMatching() {
    return [];
  },
  async listRecords() {
    return [[], 0];
  },
  listingPushdown() {
    return false;
  },
};

interface Harness {
  runtime: Runtime;
  hub: FakeHub;
  files: MemFileStore;
  saved: JsonObject[];
}

function makeRuntime(
  config: JsonObject,
  opts: { hub?: FakeHub | undefined; limits?: Partial<LimitTable> } = {},
): Harness {
  const hub = opts.hub === undefined && !("hub" in opts) ? new FakeHub() : opts.hub;
  const files = new MemFileStore();
  const adapters: Adapters = {
    files,
    data: emptyData,
    query: undefined,
    http: undefined,
    log: new NullLogStore(),
    logLevel: Severity.Error,
    builtins: seedBuiltins(files, () => emptyData, undefined),
    catalogue: undefined,
    infras: new InfraSet(),
    engine: undefined,
    images: undefined,
  };
  const saved: JsonObject[] = [];
  let current = config;
  const host: RuntimeHost = {
    adapters,
    limits: { ...defaultLimits(), ...opts.limits },
    idempotency: {
      async begin() {
        return { kind: "fresh" };
      },
      async complete() {},
      async abandon() {},
    },
    sockets: hub ?? undefined,
    async loadRaw() {
      return [current, "v1"];
    },
    async saveRaw(_tenant, cfg) {
      saved.push(cfg);
      current = cfg;
      return "v2";
    },
  };
  return { runtime: new Runtime(host), hub: hub ?? new FakeHub(), files, saved };
}

/// The standard fixture: a socket-enabled `data` mount, a plain one, and a
/// socket-enabled pipeline mount.
function chatConfig(access: Json = { read: "all", write: "all" }): JsonObject {
  return {
    auth: { jwtSecret: JWT_SECRET },
    mounts: [
      { path: "/chat", service: "data", config: { webSocket: { text: "text" }, access } },
      { path: "/plain", service: "data", config: { access: "open" } },
      { path: "/hooks", service: "pipeline", config: { webSocket: { text: "text" }, access } },
    ],
  };
}

function get(path: string, headers: Record<string, string> = {}): Message {
  const msg = Message.request("GET", path, "t");
  for (const [k, v] of Object.entries(headers)) msg.setHeader(k, v);
  return msg;
}

function upgrade(path: string, headers: Record<string, string> = {}): Message {
  return get(path, { upgrade: "WebSocket", ...headers });
}

async function jsonOf(resp: Message): Promise<Json> {
  const bytes = await resp.body!.materialize(1 << 20);
  return JSON.parse(new TextDecoder().decode(bytes)) as Json;
}

async function token(sub: string, roles: string, expSecs: number): Promise<string> {
  return sign({ sub, roles, kind: "user", iat: Math.floor(Date.now() / 1000), exp: expSecs, extra: {} }, JWT_SECRET);
}

// ---- the upgrade handshake ----------------------------------------------

describe("websocket upgrade", () => {
  it("denies before it upgrades: no 101 is sent ahead of authorization", async () => {
    const h = makeRuntime(chatConfig({ read: "admin", write: "admin" }));
    const anon = await h.runtime.handle(upgrade("/chat/room/42"));
    expect(anon.status).toBe(401);
    expect(anon.body!.mediaType.essence()).toBe("application/problem+json");
    expect(anon.socketAccept).toBeUndefined();

    const wrongRole = upgrade("/chat/room/42", { authorization: `Bearer ${await token("u@x", "U", nowPlus(3600))}` });
    const denied = await h.runtime.handle(wrongRole);
    expect(denied.status).toBe(403);
    expect(denied.socketAccept).toBeUndefined();
    expect(h.hub.untouched()).toBe(true);
  });

  it("a pipeline mount's upgrade is held to the mount's read role, not deferred to the service", async () => {
    const h = makeRuntime(chatConfig({ read: "admin", write: "admin" }));
    const anon = await h.runtime.handle(upgrade("/hooks/anything"));
    expect(anon.status).toBe(401);
    expect(anon.socketAccept).toBeUndefined();

    const admin = upgrade("/hooks/anything", { authorization: `Bearer ${await token("a@x", "admin", nowPlus(3600))}` });
    const ok = await h.runtime.handle(admin);
    expect(ok.status).toBe(101);
    expect(ok.socketAccept?.mount).toBe("/hooks");
  });

  it("accepts with the identity the host needs, bearer smuggled in the subprotocol", async () => {
    const h = makeRuntime(chatConfig());
    const exp = nowPlus(3600);
    const jwt = await token("ada@x", "U admin", exp);
    const msg = upgrade("/chat/room/42?since=7", { "sec-websocket-protocol": `rs2, rs2.bearer.${jwt}` });
    const resp = await h.runtime.handle(msg);

    expect(resp.status).toBe(101);
    // A handshake is not a representation: no default caching posture.
    expect(resp.header("cache-control")).toBeUndefined();
    const a = resp.socketAccept!;
    expect(a.id).toMatch(/^[0-9a-f]{32}$/);
    expect(a.tenant).toBe("t");
    expect(a.mount).toBe("/chat");
    expect(a.path).toBe("/chat/room/42");
    expect(a.query).toBe("?since=7");
    expect(a.principal?.id).toBe("ada@x");
    expect(a.principal?.roles).toEqual(["U", "admin"]);
    expect(a.exp).toBe(exp);
    expect(a.text).toBe("text");
    expect(a.events).toEqual(["message"]);
    // The first offered non-bearer entry wins.
    expect(a.protocol).toBe("rs2");
    expect(typeof a.connectedAt).toBe("number");
  });

  it("echoes the bearer entry when it is the only protocol offered", async () => {
    const h = makeRuntime(chatConfig());
    const jwt = await token("ada@x", "U", nowPlus(3600));
    const resp = await h.runtime.handle(upgrade("/chat/room", { "sec-websocket-protocol": `rs2.bearer.${jwt}` }));
    expect(resp.status).toBe(101);
    expect(resp.socketAccept!.protocol).toBe(`rs2.bearer.${jwt}`);
    expect(resp.socketAccept!.principal?.id).toBe("ada@x");
  });

  it("a mount without the flag ignores Upgrade and serves the plain GET", async () => {
    const h = makeRuntime(chatConfig());
    const resp = await h.runtime.handle(upgrade("/plain"));
    expect(resp.status).toBe(200);
    expect(resp.socketAccept).toBeUndefined();
    expect(resp.body!.mediaType.essence()).toBe("application/vnd.rs2.dir+json");
  });

  it("the per-tenant socket ceiling refuses and feeds the breaker", async () => {
    const h = makeRuntime(chatConfig(), { limits: { wsSocketsPerTenant: 2, breakerThreshold: 1 } });
    h.hub.countOverride = 2;
    const resp = await h.runtime.handle(upgrade("/chat/room"));
    expect(resp.status).toBe(503);
    const problem = (await jsonOf(resp)) as JsonObject;
    expect(problem.code).toBe(codes.LIMIT_EXCEEDED);
    expect(problem.limit).toBe("ws_sockets_per_tenant");
    expect(problem.observed).toBe(2);
    expect(problem.cap).toBe(2);
    // The breach tripped the breaker, so the next request fails fast.
    const next = await h.runtime.handle(get("/plain"));
    expect(next.status).toBe(503);
    expect(((await jsonOf(next)) as JsonObject).limit).toBe("tenant_breaker");
  });

  it("a cookie-authenticated upgrade from an untrusted origin is cross-site hijacking", async () => {
    const h = makeRuntime(chatConfig());
    const evil = {
      origin: "https://evil.example",
      host: "app.example",
      cookie: `rs-auth=${await token("ada@x", "U", nowPlus(3600))}`,
    };
    const resp = await h.runtime.handle(upgrade("/chat/room", evil));
    expect(resp.status).toBe(403);
    expect(((await jsonOf(resp)) as JsonObject).code).toBe(codes.FORBIDDEN);
    expect(resp.socketAccept).toBeUndefined();
    // The same origin on a plain GET is still fine — only the upgrade is unsafe.
    const plain = await h.runtime.handle(get("/plain", evil));
    expect(plain.status).toBe(200);
  });
});

// ---- the `/.sockets/` subtree -------------------------------------------

describe("/.sockets/", () => {
  it("sending is a write even where the public may invoke", async () => {
    const h = makeRuntime(chatConfig({ read: "all", invoke: "all", delete: "all", write: "admin" }));
    const post = Message.request("POST", "/chat/.sockets/room/42", "t").withJson({ hi: 1 });
    expect((await h.runtime.handle(post)).status).toBe(401);
    expect((await h.runtime.handle(Message.request("DELETE", "/chat/.sockets/", "t"))).status).toBe(401);
    expect(h.hub.untouched()).toBe(true);
  });

  it("POST sends to exactly the connect path, or to the subtree with a trailing slash", async () => {
    const h = makeRuntime(chatConfig());
    h.hub.add("/chat", "/chat/room/42");
    h.hub.add("/chat", "/chat/room/42/side");
    h.hub.add("/chat", "/chat/lobby");

    const exact = await h.runtime.handle(post("/chat/.sockets/room/42", "hello"));
    expect(exact.status).toBe(200);
    expect(await jsonOf(exact)).toEqual({ sent: 1 });
    expect(h.hub.sends[0]!.sel).toEqual({
      mount: "/chat",
      path: "/chat/room/42",
      subtree: false,
      id: undefined,
      user: undefined,
    });
    expect(h.hub.sends[0]!.frame).toBe("hello");

    const sub = await h.runtime.handle(post("/chat/.sockets/room/42/", "hello"));
    expect(await jsonOf(sub)).toEqual({ sent: 2 });
    expect(h.hub.sends[1]!.sel.subtree).toBe(true);

    const whole = await h.runtime.handle(post("/chat/.sockets/", "hello"));
    expect(await jsonOf(whole)).toEqual({ sent: 3 });
    expect(h.hub.sends[2]!.sel).toMatchObject({ mount: "/chat", path: undefined, subtree: true });
  });

  it("$id and $user narrow the selection", async () => {
    const h = makeRuntime(chatConfig());
    const ada = h.hub.add("/chat", "/chat/room/42", "ada@x");
    h.hub.add("/chat", "/chat/room/42", "bob@x");

    const byId = await h.runtime.handle(post(`/chat/.sockets/?$id=${ada.id}`, "hi"));
    expect(await jsonOf(byId)).toEqual({ sent: 1 });
    expect(h.hub.sends[0]!.sel.id).toBe(ada.id);

    const byUser = await h.runtime.handle(post("/chat/.sockets/?$user=bob%40x", "hi"));
    expect(await jsonOf(byUser)).toEqual({ sent: 1 });
    expect(h.hub.sends[1]!.sel.user).toBe("bob@x");
  });

  it("frames are text for JSON/text media types and binary otherwise", async () => {
    const h = makeRuntime(chatConfig());
    h.hub.add("/chat", "/chat/room");
    await h.runtime.handle(post("/chat/.sockets/room", JSON.stringify({ a: 1 }), MediaType.json()));
    expect(h.hub.sends[0]!.frame).toBe('{"a":1}');
    const bin = Message.request("POST", "/chat/.sockets/room", "t");
    bin.body = Body.fromBytes(new Uint8Array([1, 2, 3]), MediaType.octetStream());
    await h.runtime.handle(bin);
    expect(h.hub.sends[1]!.frame).toBeInstanceOf(Uint8Array);
    expect([...(h.hub.sends[1]!.frame as Uint8Array)]).toEqual([1, 2, 3]);
  });

  it("a body over wsMessageBytes is 413, not a 503 limit", async () => {
    const h = makeRuntime(chatConfig(), { limits: { wsMessageBytes: 4 } });
    const resp = await h.runtime.handle(post("/chat/.sockets/room", "far too long"));
    expect(resp.status).toBe(413);
    expect(h.hub.sends.length).toBe(0);
  });

  it("GET lists the selection as a dir+json listing", async () => {
    const h = makeRuntime(chatConfig());
    const a = h.hub.add("/chat", "/chat/room/42", "ada@x");
    const b = h.hub.add("/chat", "/chat/room/7");
    h.hub.add("/other", "/other/x");

    const resp = await h.runtime.handle(get("/chat/.sockets/"));
    expect(resp.status).toBe(200);
    expect(resp.body!.mediaType.essence()).toBe("application/vnd.rs2.dir+json");
    expect(resp.header("x-total-count")).toBe("2");
    expect(await jsonOf(resp)).toEqual({
      path: "/chat/.sockets/",
      entries: [
        { name: a.id, dir: false, path: "/chat/room/42", user: "ada@x", connectedAt: a.connectedAt },
        { name: b.id, dir: false, path: "/chat/room/7", connectedAt: b.connectedAt },
      ],
      total: 2,
    });
  });

  it("DELETE closes the selection, defaulting to 1000, and validates the code", async () => {
    const h = makeRuntime(chatConfig());
    h.hub.add("/chat", "/chat/room/42");

    const plain = await h.runtime.handle(del("/chat/.sockets/room/42"));
    expect(await jsonOf(plain)).toEqual({ closed: 1 });
    expect(h.hub.closes[0]).toMatchObject({ code: 1000, reason: "" });

    const app = await h.runtime.handle(del("/chat/.sockets/room/42?code=4001&reason=bye"));
    expect(app.status).toBe(200);
    expect(h.hub.closes[1]).toMatchObject({ code: 4001, reason: "bye" });

    for (const bad of ["1006", "2000", "5000", "abc"]) {
      const resp = await h.runtime.handle(del(`/chat/.sockets/room/42?code=${bad}`));
      expect(resp.status, bad).toBe(400);
    }
    expect(h.hub.closes.length).toBe(2);
  });

  it("other methods are 405", async () => {
    const h = makeRuntime(chatConfig());
    const put = Message.request("PUT", "/chat/.sockets/room", "t").withJson({});
    const resp = await h.runtime.handle(put);
    expect(resp.status).toBe(405);
    expect(((await jsonOf(resp)) as JsonObject).code).toBe(codes.BAD_REQUEST);
  });

  it("a host with no registry answers 501", async () => {
    const h = makeRuntime(chatConfig(), { hub: undefined });
    const resp = await h.runtime.handle(post("/chat/.sockets/", "hi"));
    expect(resp.status).toBe(501);
    expect(((await jsonOf(resp)) as JsonObject).code).toBe(codes.PROVIDER_UNAVAILABLE);
    // …and an upgrade to the same mount never becomes a 101 either.
    expect((await h.runtime.handle(upgrade("/chat/room"))).status).not.toBe(101);
  });

  it("the mount's roles gate it: a send needs write, a listing read", async () => {
    const h = makeRuntime(chatConfig({ read: "all", write: "admin" }));
    h.hub.add("/chat", "/chat/room");
    expect((await h.runtime.handle(post("/chat/.sockets/", "hi"))).status).toBe(401);
    expect((await h.runtime.handle(del("/chat/.sockets/"))).status).toBe(401);
    expect(h.hub.untouched()).toBe(true);
    expect((await h.runtime.handle(get("/chat/.sockets/"))).status).toBe(200);

    const admin = post("/chat/.sockets/", "hi");
    admin.setHeader("authorization", `Bearer ${await token("root@x", "admin", nowPlus(3600))}`);
    expect((await h.runtime.handle(admin)).status).toBe(200);
  });

  it("an unflagged mount leaves `.sockets` to the service", async () => {
    const h = makeRuntime(chatConfig());
    h.hub.add("/plain", "/plain/room");
    // The data service owns the path (an ordinary, empty dataset listing);
    // the hub is never consulted.
    const resp = await h.runtime.handle(get("/plain/.sockets/"));
    expect(h.hub.untouched()).toBe(true);
    expect(await jsonOf(resp)).toMatchObject({ entries: [], total: 0 });
  });
});

// ---- a pipeline mount, end to end ---------------------------------------

describe("a pipeline mount on a socket", () => {
  it("runs its pipeline per event, and a call step reaches the hub", async () => {
    const h = makeRuntime(chatConfig());
    h.files.put(
      "t",
      "/.rs2-pipelines/hooks/.root",
      JSON.stringify({
        pipeline: { steps: [{ call: { method: "POST", url: "/chat/.sockets/room/42" } }] },
      }),
    );
    h.hub.add("/chat", "/chat/room/42");

    const accept: SocketAccept = {
      id: simpleUuid(),
      tenant: "t",
      mount: "/hooks",
      path: "/hooks",
      query: "",
      principal: { id: "ada@x", roles: ["U"], kind: "user", extra: {} },
      exp: undefined,
      text: "text",
      events: ["message"],
      protocol: undefined,
      connectedAt: Date.now(),
    };
    const resp = await h.runtime.handle(socketEventMessage(accept, "message", "ping"));

    expect(resp.status, JSON.stringify(await jsonOf(resp).catch(() => null))).toBe(200);
    expect(h.hub.sends.length).toBe(1);
    expect(h.hub.sends[0]!.sel.path).toBe("/chat/room/42");
    expect(h.hub.sends[0]!.frame).toBe("ping");
  });
});

// ---- discovery -----------------------------------------------------------

describe("websocket discovery", () => {
  it("publishes the facet on flagged mounts and the limit block", async () => {
    const h = makeRuntime(chatConfig(), { limits: { wsMessageBytes: 4096 } });
    const doc = (await jsonOf(await h.runtime.handle(get("/.well-known/rs2/services")))) as JsonObject;

    const byPath = new Map((doc.services as JsonObject[]).map((s) => [s.path as string, s]));
    expect((byPath.get("/chat")!.facets as string[]) ?? []).toContain("websocket");
    expect((byPath.get("/hooks")!.facets as string[]) ?? []).toContain("websocket");
    expect((byPath.get("/plain")!.facets as string[]) ?? []).not.toContain("websocket");

    const limits = (doc.limits as JsonObject).webSocket as JsonObject;
    const defaults = defaultLimits();
    expect(limits).toEqual({
      messageBytes: 4096,
      messagesInFlight: defaults.wsMessagesInFlight,
      messagesPerSecond: defaults.wsMessagesPerSecond,
      socketsPerTenant: defaults.wsSocketsPerTenant,
    });
  });

  it("the OPTIONS descriptor carries the facet too", async () => {
    const h = makeRuntime(chatConfig());
    const probe = Message.request("OPTIONS", "/chat", "t");
    const doc = (await jsonOf(await h.runtime.handle(probe))) as JsonObject;
    expect(doc.facets as string[]).toContain("websocket");
  });
});

// ---- config validation ---------------------------------------------------

describe("webSocket config", () => {
  it("a bad value is a 400 at config PUT", async () => {
    const h = makeRuntime(chatConfig());
    const control = h.runtime.tenantControl();
    const withWs = (ws: Json): JsonObject => ({ mounts: [{ path: "/chat", service: "data", config: { webSocket: ws } }] });
    for (const bad of ["yes", 1, [], { text: "binary" }, { events: [] }, { events: ["opened"] }, { nope: 1 }]) {
      await expect(control.putConfig("t", withWs(bad as Json), undefined), JSON.stringify(bad)).rejects.toMatchObject({
        status: 400,
      });
    }
    expect(h.saved.length).toBe(0);
    await expect(control.putConfig("t", withWs(true), undefined)).resolves.toBe("v2");
  });

  it("`code:` mounts default to all three events, everything else to message", async () => {
    // Observed through the accept, which is where the normalized config lands.
    const h = makeRuntime({
      auth: { jwtSecret: JWT_SECRET },
      mounts: [{ path: "/chat", service: "data", config: { webSocket: true, access: "open" } }],
    });
    const resp = await h.runtime.handle(upgrade("/chat/room"));
    expect(resp.socketAccept!.events).toEqual(["message"]);
    expect(resp.socketAccept!.text).toBe("json");
  });
});

// ---- helpers -------------------------------------------------------------

function nowPlus(secs: number): number {
  return Math.floor(Date.now() / 1000) + secs;
}

function post(path: string, text: string, media: MediaType = MediaType.parse("text/plain")): Message {
  const msg = Message.request("POST", path, "t");
  msg.body = Body.fromString(text, media);
  return msg;
}

function del(path: string): Message {
  return Message.request("DELETE", path, "t");
}
