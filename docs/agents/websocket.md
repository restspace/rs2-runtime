# Inbound WebSockets — design (Cloudflare host first)

Status: implemented on the Worker host (`rs2-worker/`); the Rust host is a
follow-up and must reproduce this contract exactly. The shared types live in
`rs2-worker/src/runtime/sockets.ts` — read it with this doc.

## The idea

RS2 services are functions on HTTP messages, so a socket is never a second
dispatch path:

- **Inbound:** every socket event (`open`/`message`/`close`) becomes a
  synthetic `system` `POST` to the connect URL through `Runtime.handle` — the
  scheduler-tick shape (`x-rs2-trigger: websocket`, `x-rs2-socket-event`,
  `x-rs2-socket-id`), carrying the principal captured at connect. A pipeline
  mount therefore runs its pipeline per message; a `code:` mount routes the
  event to its `onOpen`/`onMessage`/`onClose` export.
- **Outbound:** a reserved dot-subtree on the mount, `/<mount>/.sockets/…`
  (the `.pipelines/` precedent), answered by the host from its `SocketHub`. A
  pipeline sends a frame with an ordinary `call` step.

Because each event is its own dispatch, the service wall clock (30 s), the
breaker, concurrency admission and boundary logging are **per message** with
no special case. The connection itself is never inside a wall clock.

## Opt-in and discovery

Mount config `"webSocket": true` or `{"text": "json"|"text", "events":
["open","message","close"]}` (any service). Defaults: `text: "json"`; `events`
all three for `code:` mounts, `["message"]` otherwise. Invalid values are a 400
at config PUT. A flagged mount gains the `websocket` facet in
`/.well-known/rs2/services` and the OPTIONS descriptor. The discovery `limits`
object gains `webSocket: {messageBytes, messagesInFlight, messagesPerSecond,
socketsPerTenant}` (LimitTable keys `wsMessageBytes`… , overridable through
`RS2_LIMITS`).

## Upgrade (in `dispatch`, as the GET it is)

A request with `Upgrade: websocket` runs `dispatch` unchanged through
`checkAccess` (read role — it is a GET). Any failure is the normal
problem+json; **a 101 is never sent before authorization**. Then, if the
message is `external`, a `GET`, the mount has `webSocket`, and the host has a
`SocketHub`:

1. `hub.count() >= wsSocketsPerTenant` → 503 `limit_exceeded("ws_sockets_per_tenant")` (feeds the breaker).
2. Return `status 101` with `msg.socketAccept` set (`SocketAccept`: fresh id,
   tenant, mount base path, connect path + query, principal, token `exp`,
   normalized `text`/`events`, selected subprotocol). Concurrency admission is
   not held; idempotency/caching do not apply.

**Pipeline mounts.** `checkAccess` defers a pipeline mount's execution paths
to the service (per-spec `access`), which an upgrade never reaches — so
`dispatch` holds the upgrade to the **mount's** `access` read role
(`checkMountAccess`), failing closed when the mount declares none. A spec's
own `access` override does not apply to the handshake.

A mount without the flag ignores `Upgrade` and serves the plain GET (RFC 9110).

**Auth.** Tokens come from `Authorization`, the `rs-auth` cookie, or — on
upgrade requests only — a `Sec-WebSocket-Protocol` entry `rs2.bearer.<jwt>`.
The selected protocol is the first offered non-bearer entry, else the bearer
entry itself. **Origin:** WebSocket is exempt from CORS, so a cookie-authenticated
upgrade with an `Origin` that the tenant CORS policy does not trust is refused
403 (cross-site WebSocket hijacking) — the cookie-CSRF guard treats an upgrade
as an unsafe method.

## Host (TenantObject)

On a 101 marker: `new WebSocketPair()`, `ctx.acceptWebSocket(server, tags)`
(Hibernation API — no `addEventListener`), `serializeAttachment(SocketAccept)`
(spill to DO KV `ws:<id>` if it exceeds the attachment cap), respond
`101 {webSocket: client}` with `Sec-WebSocket-Protocol` when selected. Then
dispatch the `open` event if enabled. `webSocketMessage`/`webSocketClose`/
`webSocketError` rebuild everything from the attachment, so they survive
eviction. Per frame, before dispatch:

| Breach | Close |
|---|---|
| frame > `wsMessageBytes` | `4413 limit_exceeded:ws_message_bytes` |
| in-flight > `wsMessagesInFlight` (per socket) | `4429 limit_exceeded:ws_messages_in_flight` |
| rate > `wsMessagesPerSecond` (per socket) | `4429 limit_exceeded:ws_messages_per_second` |
| token `exp` passed | `4401 unauthorized` |

Close code = 4000 + the RS2 error's HTTP status; reason = the RS2 code
(`closeFor`). Limit breaches call `runtime.recordBreach`. An event whose
dispatch fails with `limit_exceeded` (breaker open, wall clock) also closes;
any other error sends the problem JSON as a text frame and keeps the socket.

**Reply rule.** Event response 2xx with a body → sent to that socket (text
frame for JSON/text media types, binary otherwise); 204/no body → nothing.

## `/.sockets/` (host-intercepted in `dispatch`, after `checkAccess`)

`<rest>` is the connect path relative to the mount.

| Request | Effect |
|---|---|
| `POST /<m>/.sockets/<rest>` | send body as a frame to sockets connected at exactly that path → `200 {"sent": n}` |
| `POST /<m>/.sockets/<rest>/` | …and everything beneath (trailing slash = container); `/<m>/.sockets/` = whole mount |
| `?$id=<socketId>`, `?$user=<principalId>` | narrow the selection |
| `GET /<m>/.sockets/<rest>/` | `application/vnd.rs2.dir+json` listing `{path, entries: [{name: id, dir: false, path, user, connectedAt}], total}` + `X-Total-Count` |
| `DELETE …[?code=&reason=]` | close the selection (default 1000) → `200 {"closed": n}` |

Access: GET = the mount's read role; POST and DELETE = its **write** role —
enforced explicitly (`checkMountAccess(…, "write")`), never `invoke`/`delete`,
so a mount whose pipelines the public may invoke (`invoke: "all"`) does not
thereby let the public write to its sockets. `system` bypasses; pipelines use
their principal or `elevate`. The subtree
exists only on `webSocket` mounts (otherwise the path reaches the service); no
hub → 501 `provider_unavailable`. Frame typing mirrors the reply rule. Bodies
over `wsMessageBytes` → 413.

## Guests (`code:` mounts)

Optional exports beside `default`: `onOpen(msg, ctx, socket)`,
`onMessage(msg, ctx, socket)`, `onClose(msg, ctx, socket)`. `msg` is the usual
guest message (body = the frame; for `close`, `{code, reason, wasClean}`);
`socket` is `{id, send(data), close(code?, reason?)}` — never the raw socket.
`send`/`close` are `env.RS2.socketSend/socketClose` → the host issues a
`system` request to the **invocation's own mount's** `/.sockets/?$id=<id>`
(any socket id on that mount; never another mount). The returned envelope is
the reply, as for pipelines.

Every invocation on a `webSocket` `code:` mount — a handler, a plain request
to `default`, a scheduler tick — also gets `ctx.sockets`: `send(sel, data)`,
`close(sel, code?, reason?)`, `list(sel)` with `sel = {path?, subtree?, id?,
user?}` (`path` relative to the mount; `{}` = the whole mount). It is the same
`system` op as the handle with a selection instead of one id
(`env.RS2.socketSend/socketClose/socketList`): the URL is built host-side from
the invocation's own mount base, `path` is admitted segment by segment (no dot
segments, separators, `?`/`#`, control characters), and `id`/`user` are only
ever encoded query values — so it cannot leave that mount's `/.sockets/`.
This is deliberately **not** a general escalation: a guest's `ctx.request`
still runs as its caller and never inherits a tick's `system` source (socket
events are `system` too, and carry a user's principal — propagating it would
let any connected user bypass access on every grant). A mount without the
flag gets no socket surface (`capability_denied`), since `/.sockets/` there is
an ordinary path of the service itself. Missing `onOpen`/`onClose` → 204 no-op; missing
`onMessage` → 502 `contract_violation`. Each handler call is one ordinary
invocation: own CPU budget, own wall clock, own outbound budget.

## Implementation notes and known issues

- `"webSocket": false` means "not enabled"; duplicate `events` entries are
  de-duplicated. The `/.sockets/` listing's `path` echoes the request path.
- A `/.sockets/` body over `wsMessageBytes` is **413** `payload_too_large`
  (the materialize limit is re-mapped from the generic 503).
- Frames can reach a client **before** the HTTP response of the request that
  pushed them; clients and tests must listen first.
- The hub considers only `OPEN` sockets, so `sent`/`closed` count real
  deliveries; `webSocketClose` completes the handshake before dispatching
  the `close` event.
- The per-socket rate window and in-flight counters are in memory; eviction
  resets them (they bound bursts, not quotas). Expiry is enforced lazily on
  the next frame, plus a best-effort sweep when an alarm fires anyway.
- **Known issue (local workerd; unverified on a deployed Worker):** a close
  issued from another event's context (`DELETE /.sockets/…` over HTTP)
  completes the close-frame handshake — the server sees a clean
  `webSocketClose` — but the client's transport is not torn down, so its
  `close` event lags (the client sits in `CLOSING`). A close from the
  socket's own event (limit breach, guest `socket.close()` inside a handler)
  tears down promptly. Keeping the originating request alive until the
  handshake completes does not help. `conformance/http/websocket.test.ts`
  therefore requires the close frame and asserts the code only when the
  event fires. Re-check after the first deploy.
