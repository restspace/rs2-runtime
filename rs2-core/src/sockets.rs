//! Inbound WebSockets (`docs/agents/websocket.md`): the shared contract
//! between the dispatch path (`runtime.rs`), the listener that owns the wire
//! (`rs2-server`) and the guest engine. A socket is never a second dispatch
//! path — every socket event is a synthetic `System` message through
//! `Runtime::handle` (the scheduler-tick shape), and every outbound frame is a
//! request to the mount's reserved `/.sockets/` subtree, answered from the
//! node's [`SocketHub`]. So the per-message wall clock, the breaker, admission
//! and boundary logging all apply per message with no special case.
//!
//! This module is transport-free: the hub hands each accepted socket a queue
//! of [`Outgoing`] items and the listener pumps them onto the wire, feeding
//! inbound frames to a [`SocketSession`]. Anything that can speak WebSocket
//! can host it; the tests drive it with no network at all.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::error::{codes, RsError};
use crate::message::{Body, MediaType, Message, Principal, Source};
use crate::runtime::Runtime;

/// The reserved subtree on a `webSocket` mount: `/<mount>/.sockets/<path>`.
pub const SOCKETS_SEGMENT: &str = ".sockets";
pub const TRIGGER_HEADER: &str = "x-rs2-trigger";
pub const TRIGGER_WEBSOCKET: &str = "websocket";
pub const SOCKET_EVENT_HEADER: &str = "x-rs2-socket-event";
pub const SOCKET_ID_HEADER: &str = "x-rs2-socket-id";
/// Subprotocols: a client that cannot set `Authorization` (a browser) offers
/// `rs2.bearer.<jwt>` alongside a real protocol (conventionally `rs2`). The
/// host selects the first non-bearer protocol offered, or echoes the bearer
/// entry when it is the only one (a handshake must select an offered value).
pub const BEARER_SUBPROTOCOL_PREFIX: &str = "rs2.bearer.";

/// Frames queued per socket before it counts as a consumer that cannot keep
/// up; the socket is then dropped rather than buffering without bound.
const OUTGOING_QUEUE: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketEvent {
    Open,
    Message,
    Close,
}

impl SocketEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            SocketEvent::Open => "open",
            SocketEvent::Message => "message",
            SocketEvent::Close => "close",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "open" => Some(SocketEvent::Open),
            "message" => Some(SocketEvent::Message),
            "close" => Some(SocketEvent::Close),
            _ => None,
        }
    }
}

/// How a text frame is typed: `Json` → `application/json` (default), `Text` →
/// `text/plain`. Binary frames are `application/octet-stream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextMode {
    Json,
    Text,
}

/// The mount's `"webSocket": true | {…}` config, normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSocketConfig {
    pub text: TextMode,
    /// Which events dispatch. `code:` mounts default to all three; every other
    /// service defaults to `[Message]`.
    pub events: Vec<SocketEvent>,
}

fn bad_config(detail: impl std::fmt::Display) -> RsError {
    RsError::bad_request(format!("invalid 'webSocket' config: {detail}"))
}

/// Normalize a mount's `"webSocket"` value. Absent/`null`/`false` ⇒ the mount
/// is not socket-enabled; anything the contract does not name is a 400 at
/// tenant build, so a bad value is rejected at config PUT rather than at the
/// first upgrade.
pub fn parse_web_socket_config(
    value: Option<&Value>,
    service: &str,
) -> Result<Option<WebSocketConfig>, RsError> {
    let defaults = || WebSocketConfig {
        text: TextMode::Json,
        events: if service.starts_with("code:") {
            vec![SocketEvent::Open, SocketEvent::Message, SocketEvent::Close]
        } else {
            vec![SocketEvent::Message]
        },
    };
    let obj = match value {
        None | Some(Value::Null) | Some(Value::Bool(false)) => return Ok(None),
        Some(Value::Bool(true)) => return Ok(Some(defaults())),
        Some(Value::Object(obj)) => obj,
        Some(_) => return Err(bad_config("expected `true` or an object {text, events}")),
    };
    if let Some(k) = obj.keys().find(|k| *k != "text" && *k != "events") {
        return Err(bad_config(format!(
            "unknown field `{k}`, expected `text` or `events`"
        )));
    }
    let mut out = defaults();
    if let Some(text) = obj.get("text") {
        out.text = match text.as_str() {
            Some("json") => TextMode::Json,
            Some("text") => TextMode::Text,
            _ => return Err(bad_config("'text' must be \"json\" or \"text\"")),
        };
    }
    if let Some(events) = obj.get("events") {
        let list = events
            .as_array()
            .ok_or_else(|| bad_config("'events' must be an array"))?;
        let mut seen = Vec::new();
        for e in list {
            let parsed = e.as_str().and_then(SocketEvent::parse).ok_or_else(|| {
                bad_config(format!(
                    "unknown event '{}' (one of: open, message, close)",
                    e.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| e.to_string())
                ))
            })?;
            if !seen.contains(&parsed) {
                seen.push(parsed);
            }
        }
        if seen.is_empty() {
            return Err(bad_config("'events' must name at least one event"));
        }
        out.events = seen;
    }
    Ok(Some(out))
}

/// The normalized `webSocket` config of a routed mount, or `None` when the
/// mount is not socket-enabled. The value was validated at tenant build, so a
/// parse failure here cannot happen; it reads as "not enabled".
pub fn web_socket_config_of(config: &Value, service: &str) -> Option<WebSocketConfig> {
    parse_web_socket_config(config.get("webSocket"), service)
        .ok()
        .flatten()
}

/// Is this request a WebSocket upgrade? `Upgrade: websocket` is
/// case-insensitive (RFC 9110 §7.8); the method is checked by the caller.
pub fn is_websocket_upgrade(msg: &Message) -> bool {
    msg.header("upgrade")
        .is_some_and(|u| u.trim().eq_ignore_ascii_case("websocket"))
}

/// The `Sec-WebSocket-Protocol` entries a client offered, in order.
pub fn offered_subprotocols(msg: &Message) -> Vec<&str> {
    msg.headers
        .get_all("sec-websocket-protocol")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect()
}

/// The subprotocol a WebSocket handshake selects: the first offered entry
/// that is not a bearer carrier, else the bearer entry itself (a handshake
/// may only select a value the client offered). `None` when none was offered.
pub fn select_subprotocol(msg: &Message) -> Option<String> {
    let offered = offered_subprotocols(msg);
    offered
        .iter()
        .find(|p| !p.starts_with(BEARER_SUBPROTOCOL_PREFIX))
        .or(offered.first())
        .map(|p| p.to_string())
}

/// The identity of one accepted socket — everything an event needs.
#[derive(Debug, Clone)]
pub struct SocketAccept {
    pub id: String,
    pub tenant: String,
    /// The mount's base path (`/chat`).
    pub mount: String,
    /// The full connect path (`/chat/room/42`) and query (`?x=1` or `""`).
    pub path: String,
    pub query: String,
    pub principal: Option<Principal>,
    /// Token expiry (epoch seconds); the host closes the socket with 4401 then.
    pub exp: Option<u64>,
    pub text: TextMode,
    pub events: Vec<SocketEvent>,
    /// The selected `Sec-WebSocket-Protocol`, echoed on the 101.
    pub protocol: Option<String>,
    /// Epoch milliseconds.
    pub connected_at: u64,
}

/// Which sockets a `/.sockets/` request addresses.
#[derive(Debug, Clone)]
pub struct SocketSelector {
    pub mount: String,
    /// Full connect path to match; `None` = every socket on the mount.
    pub path: Option<String>,
    /// true (trailing slash) = `path` and everything beneath it.
    pub subtree: bool,
    /// `?$id=` / `?$user=` narrowing.
    pub id: Option<String>,
    pub user: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SocketInfo {
    pub id: String,
    pub path: String,
    pub user: Option<String>,
    pub connected_at: u64,
}

/// Does `a` fall under selector `sel`? Shared so every host agrees.
pub fn selector_matches(sel: &SocketSelector, a: &SocketAccept) -> bool {
    if a.mount != sel.mount {
        return false;
    }
    if sel.id.as_ref().is_some_and(|id| id != &a.id) {
        return false;
    }
    if let Some(user) = &sel.user {
        if a.principal.as_ref().map(|p| &p.id) != Some(user) {
            return false;
        }
    }
    let Some(path) = &sel.path else { return true };
    let want = path.trim_end_matches('/');
    let have = a.path.trim_end_matches('/');
    have == want || (sel.subtree && have.starts_with(&format!("{want}/")))
}

/// One WebSocket data frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Text(String),
    Binary(Bytes),
}

impl Frame {
    pub fn len(&self) -> usize {
        match self {
            Frame::Text(t) => t.len(),
            Frame::Binary(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The reply rule's frame typing: JSON and text media types go out as text
    /// frames, everything else as binary.
    pub fn typed(bytes: Bytes, media_type: &MediaType) -> Frame {
        if media_type.is_json() || media_type.is_text() {
            Frame::Text(String::from_utf8_lossy(&bytes).into_owned())
        } else {
            Frame::Binary(bytes)
        }
    }
}

/// What the listener writes to one socket, in order. After `Close` the queue
/// ends: send the close frame and let the connection go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outgoing {
    Frame(Frame),
    Close { code: u16, reason: String },
}

/// The body of a socket event.
#[derive(Debug, Clone)]
pub enum EventPayload {
    None,
    Frame(Frame),
    Close {
        code: u16,
        reason: String,
        was_clean: bool,
    },
}

/// The synthetic message for one socket event: a `POST` to the connect URL,
/// `System`-sourced (unforgeable from the wire; the upgrade was already
/// authorized) and carrying the principal captured at connect, so downstream
/// pipeline calls (which drop to `Internal`) are authorized as that user.
/// `close` carries `{code, reason, wasClean}`.
pub fn socket_event_message(
    a: &SocketAccept,
    event: SocketEvent,
    payload: EventPayload,
) -> Message {
    let mut msg = Message::request(
        http::Method::POST,
        &format!("{}{}", a.path, a.query),
        &a.tenant,
    );
    msg.source = Source::System;
    msg.principal = a.principal.clone();
    msg.set_header(TRIGGER_HEADER, TRIGGER_WEBSOCKET);
    msg.set_header(SOCKET_EVENT_HEADER, event.as_str());
    msg.set_header(SOCKET_ID_HEADER, &a.id);
    msg.body = match payload {
        EventPayload::None => None,
        EventPayload::Frame(Frame::Text(text)) => Some(Body::from_string(
            text,
            match a.text {
                TextMode::Json => MediaType::json(),
                TextMode::Text => MediaType::new("text/plain"),
            },
        )),
        EventPayload::Frame(Frame::Binary(bytes)) => {
            Some(Body::from_bytes(bytes, MediaType::octet_stream()))
        }
        EventPayload::Close {
            code,
            reason,
            was_clean,
        } => Some(Body::from_json(
            &json!({ "code": code, "reason": reason, "wasClean": was_clean }),
        )),
    };
    msg
}

/// Is this a socket-event message? Checks the source, so a client sending the
/// trigger header cannot reach `onMessage` paths.
pub fn socket_event_of(msg: &Message) -> Option<SocketEvent> {
    if msg.source != Source::System || msg.header(TRIGGER_HEADER) != Some(TRIGGER_WEBSOCKET) {
        return None;
    }
    msg.header(SOCKET_EVENT_HEADER).and_then(SocketEvent::parse)
}

/// Application close code for an RS2 error: 4000 + the HTTP status, reason =
/// the RS2 code (`limit_exceeded:<limit>` for limits), ≤ 123 bytes.
pub fn close_for(err: &RsError) -> (u16, String) {
    let limit = err
        .extra
        .as_ref()
        .and_then(|e| e.get("limit"))
        .and_then(|l| l.as_str());
    let mut reason = match limit {
        Some(limit) => format!("{}:{limit}", err.code),
        None => err.code.to_string(),
    };
    reason.truncate(123); // codes and limit names are ASCII
    (4000 + err.status, reason)
}

struct Meter {
    window_start: Instant,
    window_count: u32,
}

struct Entry {
    accept: SocketAccept,
    tx: mpsc::Sender<Outgoing>,
    in_flight: AtomicU32,
    meter: Mutex<Meter>,
    /// Set once a close is queued or the socket is dropped: no more frames
    /// are accepted from it and no more are sent to it.
    closed: AtomicBool,
    /// Fired when the socket must be dropped without a close frame (its
    /// outgoing queue overflowed).
    kill: tokio::sync::Notify,
}

/// The node's socket registry: every accepted socket, selected by tenant and
/// [`SocketSelector`]. Methods return how many sockets matched.
#[derive(Default)]
pub struct SocketHub {
    sockets: Mutex<HashMap<String, Arc<Entry>>>,
}

impl SocketHub {
    pub fn new() -> Arc<Self> {
        Arc::new(SocketHub::default())
    }

    /// Open sockets of one tenant.
    pub fn count(&self, tenant: &str) -> usize {
        let map = self.sockets.lock().unwrap();
        map.values().filter(|e| e.accept.tenant == tenant).count()
    }

    fn matching(&self, tenant: &str, sel: &SocketSelector) -> Vec<Arc<Entry>> {
        let map = self.sockets.lock().unwrap();
        map.values()
            .filter(|e| {
                e.accept.tenant == tenant
                    && !e.closed.load(Ordering::Acquire)
                    && selector_matches(sel, &e.accept)
            })
            .cloned()
            .collect()
    }

    fn remove(&self, id: &str) {
        self.sockets.lock().unwrap().remove(id);
    }

    /// Queue one item; a socket whose queue is full is dropped.
    fn push(&self, entry: &Entry, item: Outgoing) -> bool {
        if entry.closed.load(Ordering::Acquire) {
            return false;
        }
        match entry.tx.try_send(item) {
            Ok(()) => true,
            Err(_) => {
                entry.closed.store(true, Ordering::Release);
                self.remove(&entry.accept.id);
                entry.kill.notify_one();
                false
            }
        }
    }

    fn close_entry(&self, entry: &Entry, code: u16, reason: &str) -> bool {
        if entry.closed.swap(true, Ordering::AcqRel) {
            return false;
        }
        // Released now, so the socket is no longer a recipient while the
        // close handshake is in flight.
        self.remove(&entry.accept.id);
        let reason = reason.to_string();
        if entry.tx.try_send(Outgoing::Close { code, reason }).is_err() {
            entry.kill.notify_one();
        }
        true
    }

    pub fn send(&self, tenant: &str, sel: &SocketSelector, frame: &Frame) -> usize {
        self.matching(tenant, sel)
            .iter()
            .filter(|e| self.push(e, Outgoing::Frame(frame.clone())))
            .count()
    }

    pub fn close(&self, tenant: &str, sel: &SocketSelector, code: u16, reason: &str) -> usize {
        self.matching(tenant, sel)
            .iter()
            .filter(|e| self.close_entry(e, code, reason))
            .count()
    }

    pub fn list(&self, tenant: &str, sel: &SocketSelector) -> Vec<SocketInfo> {
        let mut out: Vec<SocketInfo> = self
            .matching(tenant, sel)
            .iter()
            .map(|e| SocketInfo {
                id: e.accept.id.clone(),
                path: e.accept.path.clone(),
                user: e.accept.principal.as_ref().map(|p| p.id.clone()),
                connected_at: e.accept.connected_at,
            })
            .collect();
        out.sort_by(|a, b| (a.connected_at, &a.id).cmp(&(b.connected_at, &b.id)));
        out
    }

    /// Close every socket of a tenant (its config was deleted).
    pub fn close_tenant(&self, tenant: &str, code: u16, reason: &str) -> usize {
        let all: Vec<Arc<Entry>> = {
            let map = self.sockets.lock().unwrap();
            map.values()
                .filter(|e| e.accept.tenant == tenant)
                .cloned()
                .collect()
        };
        all.iter()
            .filter(|e| self.close_entry(e, code, reason))
            .count()
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// One accepted socket, as the listener drives it: call [`Self::opened`] once
/// the upgrade completes, [`Self::frame`] per inbound data frame, and
/// [`Self::closed`] when the connection ends, while writing everything the
/// paired receiver yields.
pub struct SocketSession {
    runtime: Arc<Runtime>,
    hub: Arc<SocketHub>,
    entry: Arc<Entry>,
}

impl SocketSession {
    /// Register an accepted upgrade (`Message::socket_accept` on a 101) with
    /// the runtime's hub. Fails — the listener then answers the problem
    /// instead of upgrading — when the node has no hub or the tenant's socket
    /// ceiling was reached while this handshake was in flight.
    pub fn start(
        runtime: &Arc<Runtime>,
        accept: SocketAccept,
    ) -> Result<(Arc<SocketSession>, mpsc::Receiver<Outgoing>), RsError> {
        let hub = runtime
            .sockets()
            .cloned()
            .ok_or_else(|| RsError::provider_unavailable("this host has no socket registry"))?;
        let cap = runtime.limits().ws_sockets_per_tenant;
        let (tx, rx) = mpsc::channel(OUTGOING_QUEUE);
        let entry = Arc::new(Entry {
            tx,
            in_flight: AtomicU32::new(0),
            meter: Mutex::new(Meter {
                window_start: Instant::now(),
                window_count: 0,
            }),
            closed: AtomicBool::new(false),
            kill: tokio::sync::Notify::new(),
            accept,
        });
        let observed = {
            let mut map = hub.sockets.lock().unwrap();
            let observed = map
                .values()
                .filter(|e| e.accept.tenant == entry.accept.tenant)
                .count();
            if observed < cap {
                map.insert(entry.accept.id.clone(), entry.clone());
            }
            observed
        };
        if observed >= cap {
            runtime.record_breach(&entry.accept.tenant);
            return Err(RsError::limit_exceeded(
                "ws_sockets_per_tenant",
                observed as u64,
                cap as u64,
            ));
        }
        let session = SocketSession {
            runtime: runtime.clone(),
            hub,
            entry,
        };
        Ok((Arc::new(session), rx))
    }

    pub fn accept(&self) -> &SocketAccept {
        &self.entry.accept
    }

    /// How long until the connect token expires (the listener calls
    /// [`Self::expire`] then); `None` for a socket with no expiry.
    pub fn expires_in(&self) -> Option<std::time::Duration> {
        self.entry
            .accept
            .exp
            .map(|exp| std::time::Duration::from_secs(exp.saturating_sub(now_secs())))
    }

    /// Resolves when the socket must be dropped without a close handshake.
    pub async fn killed(&self) {
        self.entry.kill.notified().await
    }

    /// Dispatch the `open` event, if subscribed. Await it before reading
    /// frames, so a service sees `open` before `message`.
    pub async fn opened(&self) {
        if self.entry.accept.events.contains(&SocketEvent::Open) {
            let resp = self.dispatch(SocketEvent::Open, EventPayload::None).await;
            drain(resp).await;
        }
    }

    async fn dispatch(&self, event: SocketEvent, payload: EventPayload) -> Message {
        self.runtime
            .handle(socket_event_message(&self.entry.accept, event, payload))
            .await
    }

    /// A limit breached on the wire, outside `invoke`: it feeds the tenant
    /// breaker exactly as an in-band breach does, then closes. A per-frame
    /// breach is a payload/rate problem (4413/4429), not the 503 the
    /// `limit_exceeded` status would give.
    fn breach(&self, limit: &str, observed: u64, cap: u64) {
        self.runtime.record_breach(&self.entry.accept.tenant);
        let (_, reason) = close_for(&RsError::limit_exceeded(limit, observed, cap));
        let code = if limit == "ws_message_bytes" {
            4413
        } else {
            4429
        };
        self.hub.close_entry(&self.entry, code, &reason);
    }

    /// The token expired: close with `4401 unauthorized`.
    pub fn expire(&self) {
        self.hub.close_entry(&self.entry, 4401, codes::UNAUTHORIZED);
    }

    /// One inbound data frame. The limit checks run here, in order; the
    /// event itself dispatches on its own task, so frames are handled
    /// concurrently up to `ws_messages_in_flight`.
    pub fn frame(self: &Arc<Self>, frame: Frame) {
        let entry = &self.entry;
        if entry.closed.load(Ordering::Acquire) {
            return;
        }
        let limits = self.runtime.limits();
        let size = frame.len() as u64;
        if size > limits.ws_message_bytes {
            return self.breach("ws_message_bytes", size, limits.ws_message_bytes);
        }
        let in_flight = entry.in_flight.load(Ordering::Acquire);
        if in_flight >= limits.ws_messages_in_flight {
            return self.breach(
                "ws_messages_in_flight",
                in_flight as u64 + 1,
                limits.ws_messages_in_flight as u64,
            );
        }
        let count = {
            let mut meter = entry.meter.lock().unwrap();
            if meter.window_start.elapsed() >= std::time::Duration::from_secs(1) {
                meter.window_start = Instant::now();
                meter.window_count = 0;
            }
            meter.window_count += 1;
            meter.window_count
        };
        if count > limits.ws_messages_per_second {
            return self.breach(
                "ws_messages_per_second",
                count as u64,
                limits.ws_messages_per_second as u64,
            );
        }
        if entry.accept.exp.is_some_and(|exp| now_secs() >= exp) {
            return self.expire();
        }
        // Limits bind every frame; dispatch only happens for a subscribed event.
        if !entry.accept.events.contains(&SocketEvent::Message) {
            return;
        }
        entry.in_flight.fetch_add(1, Ordering::AcqRel);
        let me = self.clone();
        tokio::spawn(async move {
            let resp = me
                .dispatch(SocketEvent::Message, EventPayload::Frame(frame))
                .await;
            me.deliver_reply(resp).await;
            me.entry.in_flight.fetch_sub(1, Ordering::AcqRel);
        });
    }

    /// The reply rule: 2xx with a body → one frame on this socket (text for
    /// JSON/text media types, binary otherwise); 204/no body → nothing; an
    /// error that is a `limit_exceeded` → close; any other error → the problem
    /// JSON as a text frame, socket kept.
    async fn deliver_reply(&self, mut resp: Message) {
        let status = resp.status.map(|s| s.as_u16()).unwrap_or(200);
        if status == 204 || resp.body.is_none() {
            return drain(resp).await;
        }
        let cap = self.runtime.limits().ws_message_bytes;
        let mut body = resp.body.take().expect("checked above");
        let bytes = match body.materialize(cap).await {
            Ok(bytes) => bytes.clone(),
            // A reply too large for a frame is the service's problem, not the
            // client's: send nothing rather than tearing the socket down.
            Err(_) => return,
        };
        if (200..300).contains(&status) {
            let frame = Frame::typed(bytes, &body.media_type);
            self.hub.push(&self.entry, Outgoing::Frame(frame));
            return;
        }
        let problem: Option<Value> = serde_json::from_slice(&bytes).ok();
        let field = |name: &str| {
            problem
                .as_ref()
                .and_then(|p| p.get(name))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };
        if field("code").as_deref() == Some(codes::LIMIT_EXCEEDED) {
            let reason = match field("limit") {
                Some(limit) => format!("{}:{limit}", codes::LIMIT_EXCEEDED),
                None => codes::LIMIT_EXCEEDED.to_string(),
            };
            self.hub.close_entry(&self.entry, 4000 + status, &reason);
            return;
        }
        let text = String::from_utf8_lossy(&bytes).into_owned();
        self.hub
            .push(&self.entry, Outgoing::Frame(Frame::Text(text)));
    }

    /// The connection ended (`code`/`reason` from the peer's close frame, or
    /// 1006 when it just went away). Releases the socket first, so it is no
    /// stale match for a concurrent `/.sockets/` send, then dispatches `close`.
    pub async fn closed(&self, code: u16, reason: &str, was_clean: bool) {
        self.entry.closed.store(true, Ordering::Release);
        self.hub.remove(&self.entry.accept.id);
        if self.entry.accept.events.contains(&SocketEvent::Close) {
            let payload = EventPayload::Close {
                code,
                reason: reason.to_string(),
                was_clean,
            };
            drain(self.dispatch(SocketEvent::Close, payload).await).await;
        }
    }
}

/// Nothing to reply to: consume the response body so a streaming service is
/// not left blocked on a reader that never comes.
async fn drain(resp: Message) {
    use futures::StreamExt;
    if let Some(body) = resp.body {
        let mut stream = body.into_stream();
        while stream.next().await.is_some() {}
    }
}
