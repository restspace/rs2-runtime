// Inbound WebSockets (cloudflare.md §E.6): the shared contract between the
// dispatch path (`dispatch.ts`), the platform host (`tenant-object.ts`) and
// the guest engine. A socket is never a second dispatch path — every socket
// event is a synthetic `system` message through `Runtime.handle` (the same
// shape as the scheduler tick), and every outbound frame is a request to the
// mount's reserved `/.sockets/` subtree, answered by the host's `SocketHub`.
// So the per-message wall clock, the breaker, admission and boundary logging
// all apply per message with no special case.

import { Body } from "./body";
import { RsError } from "./error";
import type { Json, JsonObject } from "./error";
import { MediaType } from "./media-type";
import { Message } from "./message";
import type { Principal } from "./message";

export type SocketEvent = "open" | "message" | "close";

/// The reserved subtree on a `webSocket` mount: `/<mount>/.sockets/<path>`.
export const SOCKETS_SEGMENT = ".sockets";
export const TRIGGER_HEADER = "x-rs2-trigger";
export const TRIGGER_WEBSOCKET = "websocket";
export const SOCKET_EVENT_HEADER = "x-rs2-socket-event";
export const SOCKET_ID_HEADER = "x-rs2-socket-id";
/// Subprotocols: a client that cannot set `Authorization` (a browser) offers
/// `rs2.bearer.<jwt>` alongside a real protocol (conventionally `rs2`). The
/// host selects the first non-bearer protocol offered, or echoes the bearer
/// entry when it is the only one (a handshake must select an offered value).
export const BEARER_SUBPROTOCOL_PREFIX = "rs2.bearer.";

/// The mount's `"webSocket": true | {…}` config, normalized.
export interface WebSocketConfig {
  /// How a text frame is typed: `"json"` → `application/json` (default),
  /// `"text"` → `text/plain`. Binary frames are `application/octet-stream`.
  text: "json" | "text";
  /// Which events dispatch. `code:` mounts default to all three; every other
  /// service defaults to `["message"]`.
  events: SocketEvent[];
}

const ALL_EVENTS: SocketEvent[] = ["open", "message", "close"];
const WEBSOCKET_KEY = "webSocket";

function badConfig(detail: string): RsError {
  return RsError.badRequest(`invalid 'webSocket' config: ${detail}`);
}

/// Normalize a mount's `"webSocket"` value. `undefined`/`false` ⇒ the mount
/// is not socket-enabled; anything the contract does not name is a 400 at
/// tenant build, so a bad value is rejected at config PUT rather than at the
/// first upgrade. `events` defaults to all three for a `code:` mount (which
/// has an `onOpen`/`onClose` export to route them to) and `["message"]` for
/// every other service.
export function parseWebSocketConfig(value: Json | undefined, service: string): WebSocketConfig | undefined {
  if (value === undefined || value === null || value === false) return undefined;
  const defaults = (): WebSocketConfig => ({
    text: "json",
    events: service.startsWith("code:") ? [...ALL_EVENTS] : ["message"],
  });
  if (value === true) return defaults();
  if (typeof value !== "object" || Array.isArray(value)) {
    throw badConfig("expected `true` or an object {text, events}");
  }
  for (const k of Object.keys(value)) {
    if (k !== "text" && k !== "events") throw badConfig(`unknown field \`${k}\`, expected \`text\` or \`events\``);
  }
  const out = defaults();
  const text = value.text;
  if (text !== undefined) {
    if (text !== "json" && text !== "text") throw badConfig("'text' must be \"json\" or \"text\"");
    out.text = text;
  }
  const events = value.events;
  if (events !== undefined) {
    if (!Array.isArray(events)) throw badConfig("'events' must be an array");
    const seen: SocketEvent[] = [];
    for (const e of events) {
      if (e !== "open" && e !== "message" && e !== "close") {
        throw badConfig(`unknown event '${String(e)}' (one of: ${ALL_EVENTS.join(", ")})`);
      }
      if (!seen.includes(e)) seen.push(e);
    }
    if (seen.length === 0) throw badConfig("'events' must name at least one event");
    out.events = seen;
  }
  return out;
}

/// The normalized `webSocket` config of a routed mount, or `undefined` when
/// the mount is not socket-enabled. The value was validated at tenant build,
/// so re-parsing here cannot surprise the dispatch path.
export function webSocketConfigOf(config: JsonObject, service: string): WebSocketConfig | undefined {
  const raw = Object.prototype.hasOwnProperty.call(config, WEBSOCKET_KEY) ? config[WEBSOCKET_KEY] : undefined;
  return parseWebSocketConfig(raw, service);
}

/// Is this request a WebSocket upgrade? `Upgrade: websocket` is
/// case-insensitive (RFC 9110 §7.8); the method is checked by the caller.
export function isWebSocketUpgrade(msg: Message): boolean {
  return (msg.header("upgrade") ?? "").trim().toLowerCase() === "websocket";
}

/// The serializable identity of one accepted socket — the hibernation
/// attachment. Everything a handler needs after eviction is here.
export interface SocketAccept {
  id: string;
  tenant: string;
  /// The mount's base path (`/chat`).
  mount: string;
  /// The full connect path (`/chat/room/42`) and query (`?x=1` or `""`).
  path: string;
  query: string;
  principal: Principal | undefined;
  /// Token expiry (epoch seconds); the host closes the socket with 4401 then.
  exp: number | undefined;
  text: "json" | "text";
  events: SocketEvent[];
  /// The selected `Sec-WebSocket-Protocol`, echoed on the 101.
  protocol: string | undefined;
  connectedAt: number;
}

/// Which sockets a `/.sockets/` request addresses.
export interface SocketSelector {
  mount: string;
  /// Full connect path to match; `undefined` = every socket on the mount.
  path: string | undefined;
  /// true (trailing slash) = `path` and everything beneath it.
  subtree: boolean;
  /// `?$id=` / `?$user=` narrowing.
  id: string | undefined;
  user: string | undefined;
}

export interface SocketInfo {
  id: string;
  path: string;
  user: string | undefined;
  connectedAt: number;
}

/// The host's socket registry. On this host: the `TenantObject`'s hibernating
/// sockets (`ctx.getWebSockets`). Methods return how many sockets matched.
export interface SocketHub {
  count(): number;
  send(sel: SocketSelector, frame: string | Uint8Array): number;
  close(sel: SocketSelector, code: number, reason: string): number;
  list(sel: SocketSelector): SocketInfo[];
}

/// Does `a` fall under selector `sel`? Shared so every hub agrees.
export function selectorMatches(sel: SocketSelector, a: SocketAccept): boolean {
  if (a.mount !== sel.mount) return false;
  if (sel.id !== undefined && a.id !== sel.id) return false;
  if (sel.user !== undefined && a.principal?.id !== sel.user) return false;
  if (sel.path === undefined) return true;
  const want = sel.path.replace(/\/+$/, "");
  const have = a.path.replace(/\/+$/, "");
  if (have === want) return true;
  return sel.subtree && have.startsWith(`${want}/`);
}

/// The synthetic message for one socket event: an internal `POST` to the
/// connect URL, `system`-sourced (unforgeable from the wire; the upgrade was
/// already authorized) and carrying the principal captured at connect, so
/// downstream pipeline calls (which drop to `internal`) are authorized as
/// that user. `close` carries `{code, reason, wasClean}`.
export function socketEventMessage(
  a: SocketAccept,
  event: SocketEvent,
  payload?: string | Uint8Array | { code: number; reason: string; wasClean: boolean },
): Message {
  const msg = Message.request("POST", `${a.path}${a.query}`, a.tenant);
  msg.source = "system";
  msg.principal = a.principal;
  msg.setHeader(TRIGGER_HEADER, TRIGGER_WEBSOCKET);
  msg.setHeader(SOCKET_EVENT_HEADER, event);
  msg.setHeader(SOCKET_ID_HEADER, a.id);
  if (typeof payload === "string") {
    msg.body = Body.fromString(payload, a.text === "json" ? MediaType.json() : MediaType.parse("text/plain"));
  } else if (payload instanceof Uint8Array) {
    msg.body = Body.fromBytes(payload, MediaType.octetStream());
  } else if (payload !== undefined) {
    msg.body = Body.fromJson(payload);
  }
  return msg;
}

/// Is this a socket-event message? Checks the source, so a client sending
/// the trigger header cannot reach `onMessage` paths.
export function socketEventOf(msg: Message): SocketEvent | undefined {
  if (msg.source !== "system" || msg.header(TRIGGER_HEADER) !== TRIGGER_WEBSOCKET) return undefined;
  const e = msg.header(SOCKET_EVENT_HEADER);
  return e === "open" || e === "message" || e === "close" ? e : undefined;
}

/// Application close code for an RS2 error: 4000 + the HTTP status, reason =
/// the RS2 code (`limit_exceeded:<limit>` for limits), ≤ 123 bytes.
export function closeFor(err: RsError): { code: number; reason: string } {
  const limit = err.extra?.limit;
  const reason = typeof limit === "string" ? `${err.code}:${limit}` : err.code;
  return { code: 4000 + err.status, reason: reason.slice(0, 123) };
}
