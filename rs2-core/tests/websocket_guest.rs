//! Inbound WebSockets, guest side (`docs/agents/websocket.md`, "Guests"): on a
//! `code:` mount a socket event routes to `onOpen`/`onMessage`/`onClose`
//! instead of `default`, and the guest's socket surface — the handler's
//! `socket` handle and every invocation's `ctx.sockets` — reaches exactly its
//! own mount's `/.sockets/` subtree, as a `System` request the guest cannot
//! steer anywhere else.
//!
//! No network: sockets are registered straight with the hub
//! (`SocketSession::start`) and the frames a real listener would write are
//! read off the returned queue.

#![cfg(feature = "js")]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use tokio::sync::mpsc::Receiver;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Body, MediaType, Message, Principal, Source};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::sockets::{
    socket_event_message, EventPayload, Frame, Outgoing, SocketAccept, SocketEvent, SocketSession,
    TextMode, SOCKET_EVENT_HEADER, SOCKET_ID_HEADER, TRIGGER_HEADER, TRIGGER_WEBSOCKET,
};
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// The chat bundle. `default` is an ordinary HTTP surface that drives
/// `ctx.sockets` from a JSON command (and reports a thrown host error by its
/// code); the socket exports answer frames and use the `socket` handle.
const CHAT: &str = r#"
    const guarded = (fn) => {
        try { return { status: 200, body: fn() }; }
        catch (e) { return { status: 200, body: { error: e.code, status: e.status } }; }
    };
    export default async (msg, ctx) => {
        const b = msg.body || {};
        if (b.op === "send") return guarded(() => ctx.sockets.send(b.sel, b.data));
        if (b.op === "close") return guarded(() => ctx.sockets.close(b.sel, b.code, b.reason));
        if (b.op === "list") return guarded(() => ctx.sockets.list(b.sel));
        if (b.op === "spend") return guarded(() => {
            const out = [];
            for (let i = 0; i < b.times; i++) {
                try { out.push(ctx.sockets.send({}, "x").sent); } catch (e) { out.push(e.code); }
            }
            return out;
        });
        return { status: 200, body: { ran: "default" } };
    };
    export const onOpen = async (msg, ctx, socket) =>
        ({ status: 200, body: { opened: socket.id } });
    export const onMessage = async (msg, ctx, socket) => {
        const b = msg.body || {};
        if (b.cmd === "push") { await socket.send({ pushed: b.value }); return null; }
        if (b.cmd === "text") { await socket.send("plain text"); return null; }
        if (b.cmd === "bin") { await socket.send(new Uint8Array([1, 2, 3])); return null; }
        if (b.cmd === "bye") { await socket.close(4001, "bye"); return null; }
        if (b.cmd === "others") return { status: 200, body: ctx.sockets.send({ path: "room/2" }, "hi 2") };
        return { status: 200, body: { echo: b, id: socket.id, url: msg.url, method: msg.method } };
    };
    export const onClose = async (msg) => ({ status: 200, body: msg.body });
"#;

/// A bundle with no socket exports at all.
const BARE: &str = r#"export default async () => ({ status: 200, body: "default" });"#;

struct Loader(Value);

#[async_trait]
impl ConfigLoader for Loader {
    async fn load_tenant(&self, _t: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
    async fn load_raw(&self, _t: &str) -> Result<(Value, String), RsError> {
        Ok((self.0.clone(), "v".into()))
    }
    async fn save_raw(&self, _t: &str, _c: &Value, _v: Option<&str>) -> Result<String, RsError> {
        Ok("v".into())
    }
}

struct Node {
    rt: Arc<Runtime>,
    _dir: tempfile::TempDir,
}

/// `/chat` and `/other` are `webSocket` mounts of the chat bundle, `/plain` is
/// the same bundle without the flag, `/bare` a `webSocket` mount of a bundle
/// with no socket exports. The public may invoke each; only `A` may write.
async fn node(limits: LimitTable) -> Node {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_sockets();
    let chat = rs2_core::services::code::version_of(CHAT.as_bytes());
    let bare = rs2_core::services::code::version_of(BARE.as_bytes());
    let access = json!({ "read": "all", "invoke": "all", "write": "A" });
    let config = json!({ "mounts": [
        { "path": "/services", "service": "services", "config": { "access": "open" } },
        { "path": "/chat", "service": format!("code:chat@{chat}"),
          "config": { "access": access, "webSocket": true } },
        { "path": "/other", "service": format!("code:chat@{chat}"),
          "config": { "access": access, "webSocket": true } },
        { "path": "/plain", "service": format!("code:chat@{chat}"),
          "config": { "access": access } },
        { "path": "/bare", "service": format!("code:bare@{bare}"),
          "config": { "access": access, "webSocket": true } }
    ]});
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        limits,
    );
    for (name, source) in [("chat", CHAT), ("bare", BARE)] {
        let deploy = Message::request(Method::POST, &format!("/services/code/{name}/"), "t")
            .with_body(Body::from_bytes(
                source.as_bytes().to_vec(),
                MediaType::new("application/javascript"),
            ));
        assert_eq!(rt.handle(deploy).await.status, Some(StatusCode::CREATED));
    }
    Node { rt, _dir: dir }
}

fn accept(id: &str, mount: &str, path: &str, user: Option<&str>) -> SocketAccept {
    SocketAccept {
        id: id.into(),
        tenant: "t".into(),
        mount: mount.into(),
        path: path.into(),
        query: String::new(),
        principal: user.map(|id| Principal {
            id: id.into(),
            roles: vec!["U".into()],
            kind: "user".into(),
            extra: Default::default(),
        }),
        exp: None,
        text: TextMode::Json,
        events: vec![SocketEvent::Open, SocketEvent::Message, SocketEvent::Close],
        protocol: None,
        connected_at: 0,
    }
}

fn connect(node: &Node, a: SocketAccept) -> (Arc<SocketSession>, Receiver<Outgoing>) {
    SocketSession::start(&node.rt, a).expect("socket registers")
}

async fn next(rx: &mut Receiver<Outgoing>) -> Outgoing {
    tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .expect("a frame within the deadline")
        .expect("the socket queue is open")
}

fn json_frame(out: Outgoing) -> Value {
    match out {
        Outgoing::Frame(Frame::Text(text)) => serde_json::from_str(&text).expect("a JSON frame"),
        other => panic!("expected a text frame, got {other:?}"),
    }
}

/// An anonymous external `POST <path>` with a JSON body → `(status, body)`.
async fn post(node: &Node, path: &str, body: Value) -> (u16, Value) {
    let msg = Message::request(Method::POST, path, "t").with_body(Body::from_json(&body));
    let mut resp = node.rt.handle(msg).await;
    let status = resp.status.map(|s| s.as_u16()).unwrap_or(0);
    let payload = match resp.body.as_mut() {
        Some(b) => b.as_any(1 << 20).await.unwrap(),
        None => Value::Null,
    };
    (status, payload)
}

#[tokio::test]
async fn on_message_envelope_is_the_reply() {
    let node = node(LimitTable::default()).await;
    let (session, mut rx) = connect(&node, accept("s1", "/chat", "/chat/room/1", None));
    session.frame(Frame::Text(r#"{"hi":"there"}"#.into()));
    assert_eq!(
        json_frame(next(&mut rx).await),
        // A `text: "json"` mount hands the frame over parsed, like any other
        // JSON body at the engine boundary.
        json!({ "echo": { "hi": "there" }, "id": "s1", "url": "/chat/room/1", "method": "POST" })
    );
}

#[tokio::test]
async fn open_and_close_events_run_their_own_exports() {
    let node = node(LimitTable::default()).await;
    let a = accept("s1", "/chat", "/chat/room/1", None);
    let mut opened = node
        .rt
        .handle(socket_event_message(
            &a,
            SocketEvent::Open,
            EventPayload::None,
        ))
        .await;
    assert_eq!(opened.status, Some(StatusCode::OK));
    assert_eq!(
        opened.body.as_mut().unwrap().as_any(65536).await.unwrap(),
        json!({ "opened": "s1" })
    );
    let payload = EventPayload::Close {
        code: 1001,
        reason: "bye".into(),
        was_clean: true,
    };
    let mut closed = node
        .rt
        .handle(socket_event_message(&a, SocketEvent::Close, payload))
        .await;
    assert_eq!(
        closed.body.as_mut().unwrap().as_any(65536).await.unwrap(),
        json!({ "code": 1001, "reason": "bye", "wasClean": true })
    );
}

#[tokio::test]
async fn socket_send_reaches_the_same_socket_typed_by_its_data() {
    let node = node(LimitTable::default()).await;
    let (session, mut rx) = connect(&node, accept("s1", "/chat", "/chat/room/1", None));
    let (_bystander, mut other_rx) = connect(&node, accept("s2", "/chat", "/chat/room/1", None));

    // A JSON value is stringified into a text frame; the handler returned
    // nothing, so the pushed frame is the only one.
    session.frame(Frame::Text(r#"{"cmd":"push","value":7}"#.into()));
    assert_eq!(json_frame(next(&mut rx).await), json!({ "pushed": 7 }));
    session.frame(Frame::Text(r#"{"cmd":"text"}"#.into()));
    assert_eq!(
        next(&mut rx).await,
        Outgoing::Frame(Frame::Text("plain text".into()))
    );
    session.frame(Frame::Text(r#"{"cmd":"bin"}"#.into()));
    assert_eq!(
        next(&mut rx).await,
        Outgoing::Frame(Frame::Binary(vec![1u8, 2, 3].into()))
    );
    // The handle is the socket the event came from — not its neighbours.
    assert!(other_rx.try_recv().is_err());
}

#[tokio::test]
async fn socket_close_closes_with_the_given_code() {
    let node = node(LimitTable::default()).await;
    let (session, mut rx) = connect(&node, accept("s1", "/chat", "/chat/room/1", None));
    session.frame(Frame::Text(r#"{"cmd":"bye"}"#.into()));
    assert_eq!(
        next(&mut rx).await,
        Outgoing::Close {
            code: 4001,
            reason: "bye".into()
        }
    );
}

#[tokio::test]
async fn a_handler_reaches_other_sockets_through_ctx_sockets() {
    let node = node(LimitTable::default()).await;
    // The connected user holds no write role on the mount: the send works
    // because it is the mount answering on its own sockets.
    let (one, mut rx1) = connect(&node, accept("s1", "/chat", "/chat/room/1", Some("u1")));
    let (_two, mut rx2) = connect(&node, accept("s2", "/chat", "/chat/room/2", Some("u2")));
    one.frame(Frame::Text(r#"{"cmd":"others"}"#.into()));
    assert_eq!(
        next(&mut rx2).await,
        Outgoing::Frame(Frame::Text("hi 2".into()))
    );
    assert_eq!(json_frame(next(&mut rx1).await), json!({ "sent": 1 }));
}

#[tokio::test]
async fn ctx_sockets_broadcasts_from_a_plain_anonymous_request() {
    let node = node(LimitTable::default()).await;
    let (_a, mut rx1) = connect(&node, accept("s1", "/chat", "/chat/room/1", Some("u1")));
    let (_b, mut rx2) = connect(&node, accept("s2", "/chat", "/chat/room/2", Some("u2")));
    let (_c, mut other) = connect(&node, accept("s3", "/other", "/other/room/1", None));

    // The public may not write to the mount's sockets directly…
    let (status, problem) = post(&node, "/chat/.sockets/", json!("nope")).await;
    assert_eq!(status, 401, "{problem}");
    assert!(rx1.try_recv().is_err());

    // …but may invoke the service, which answers on its own mount.
    let (status, body) = post(
        &node,
        "/chat/notify",
        json!({ "op": "send", "sel": {}, "data": { "n": 1 } }),
    )
    .await;
    assert_eq!((status, body), (200, json!({ "sent": 2 })));
    assert_eq!(json_frame(next(&mut rx1).await), json!({ "n": 1 }));
    assert_eq!(json_frame(next(&mut rx2).await), json!({ "n": 1 }));

    // Narrowing: an exact path, a subtree, one user, one id (string or field).
    for (sel, sent) in [
        (json!({ "path": "room/1" }), 1),
        (json!({ "path": "/room/1" }), 1),
        (json!({ "path": "room" }), 0),
        (json!({ "path": "room", "subtree": true }), 2),
        (json!({ "path": "room/" }), 2),
        (json!({ "user": "u2" }), 1),
        (json!({ "id": "s1" }), 1),
        (json!("s2"), 1),
        (json!({ "id": "s3" }), 0), // another mount's socket is never a match
    ] {
        let (_, body) = post(
            &node,
            "/chat/notify",
            json!({ "op": "send", "sel": sel, "data": "x" }),
        )
        .await;
        assert_eq!(body, json!({ "sent": sent }), "selection {sel}");
    }
    assert!(other.try_recv().is_err(), "nothing crossed to /other");
}

#[tokio::test]
async fn a_selection_cannot_leave_the_mounts_sockets_subtree() {
    let node = node(LimitTable::default()).await;
    let (_mine, mut mine) = connect(&node, accept("s1", "/chat", "/chat/room/1", None));
    let (_theirs, mut theirs) = connect(&node, accept("s2", "/other", "/other/room/1", None));
    for path in [
        "../other",
        "../../other/.sockets/room/1",
        "a/../../x",
        "./room/1",
        "x?$id=s2",
        "x#frag",
        "a\\..\\b",
        "%2e%2e/other",
        "%2E%2E/%2e%2e/other/.sockets",
        "room%2f..%2f..%2fother",
        "room%3f$id=s2",
        "room%0a1",
        "room/%zz",
        "room/%ff",
    ] {
        for op in ["send", "close", "list"] {
            let (status, body) = post(
                &node,
                "/chat/notify",
                json!({ "op": op, "sel": { "path": path }, "data": "escaped" }),
            )
            .await;
            assert_eq!(status, 200);
            assert_eq!(
                body,
                json!({ "error": "bad_request", "status": 400 }),
                "{op} with path {path:?}"
            );
        }
    }
    // A non-string path is refused rather than coerced.
    let (_, body) = post(
        &node,
        "/chat/notify",
        json!({ "op": "send", "sel": { "path": ["..", "other"] }, "data": "x" }),
    )
    .await;
    assert_eq!(body["error"], "bad_request");
    // `id`/`user` are only ever query *values*: text that would add a path or
    // a parameter selects nothing instead.
    for sel in [
        json!({ "id": "s1&$id=s2" }),
        json!({ "id": "x/../../other" }),
        json!({ "user": "u&$user=" }),
        json!("../../other/.sockets/?$id=s2"),
    ] {
        let (_, body) = post(
            &node,
            "/chat/notify",
            json!({ "op": "send", "sel": sel, "data": "escaped" }),
        )
        .await;
        assert_eq!(body, json!({ "sent": 0 }), "selection {sel}");
    }
    assert!(theirs.try_recv().is_err(), "nothing reached /other");
    assert!(mine.try_recv().is_err());
}

#[tokio::test]
async fn an_unflagged_mount_has_no_socket_surface() {
    let node = node(LimitTable::default()).await;
    let (_a, mut rx) = connect(&node, accept("s1", "/chat", "/chat/room/1", None));
    for op in ["send", "close", "list"] {
        let (status, body) = post(
            &node,
            "/plain/notify",
            json!({ "op": op, "sel": {}, "data": "x" }),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            body,
            json!({ "error": "capability_denied", "status": 403 }),
            "{op}"
        );
    }
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn missing_handlers_are_a_noop_except_on_message() {
    let node = node(LimitTable::default()).await;
    let a = accept("s1", "/bare", "/bare/room/1", None);
    let opened = node
        .rt
        .handle(socket_event_message(
            &a,
            SocketEvent::Open,
            EventPayload::None,
        ))
        .await;
    assert_eq!(opened.status, Some(StatusCode::NO_CONTENT));
    let payload = EventPayload::Close {
        code: 1000,
        reason: String::new(),
        was_clean: true,
    };
    let closed = node
        .rt
        .handle(socket_event_message(&a, SocketEvent::Close, payload))
        .await;
    assert_eq!(closed.status, Some(StatusCode::NO_CONTENT));

    let frame = EventPayload::Frame(Frame::Text("{}".into()));
    let mut resp = node
        .rt
        .handle(socket_event_message(&a, SocketEvent::Message, frame))
        .await;
    assert_eq!(resp.status, Some(StatusCode::BAD_GATEWAY));
    let problem = resp.body.as_mut().unwrap().as_any(65536).await.unwrap();
    assert_eq!(problem["code"], "contract_violation", "{problem}");
    assert!(
        problem["detail"].as_str().unwrap().contains("onMessage"),
        "{problem}"
    );
}

#[tokio::test]
async fn forged_trigger_headers_reach_default_not_on_message() {
    let node = node(LimitTable::default()).await;
    let mut msg = Message::request(Method::POST, "/chat/room/1", "t")
        .with_body(Body::from_json(&json!({ "cmd": "bye" })));
    msg.set_header(TRIGGER_HEADER, TRIGGER_WEBSOCKET);
    msg.set_header(SOCKET_EVENT_HEADER, "message");
    msg.set_header(SOCKET_ID_HEADER, "s1");
    assert_eq!(msg.source, Source::External);
    let (_victim, mut rx) = connect(&node, accept("s1", "/chat", "/chat/room/1", None));
    let mut resp = node.rt.handle(msg).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_mut().unwrap().as_any(65536).await.unwrap(),
        json!({ "ran": "default" })
    );
    assert!(rx.try_recv().is_err(), "the forged event closed nothing");
}

#[tokio::test]
async fn ctx_sockets_lists_and_closes_a_selection() {
    let node = node(LimitTable::default()).await;
    let (_a, mut rx1) = connect(&node, accept("s1", "/chat", "/chat/room/1", Some("u1")));
    let (_b, mut rx2) = connect(&node, accept("s2", "/chat", "/chat/room/2", None));

    let (_, all) = post(&node, "/chat/x", json!({ "op": "list", "sel": {} })).await;
    assert_eq!(all["total"], 2, "{all}");
    assert_eq!(all["entries"][0]["name"], "s1");
    assert_eq!(all["entries"][0]["path"], "/chat/room/1");
    assert_eq!(all["entries"][0]["user"], "u1");
    assert_eq!(all["entries"][1]["name"], "s2");
    // A selection with a `path` lists that subtree.
    let (_, one) = post(
        &node,
        "/chat/x",
        json!({ "op": "list", "sel": { "path": "room/2" } }),
    )
    .await;
    assert_eq!(one["total"], 1, "{one}");
    assert_eq!(one["entries"][0]["name"], "s2");
    let (_, room) = post(
        &node,
        "/chat/x",
        json!({ "op": "list", "sel": { "path": "room" } }),
    )
    .await;
    assert_eq!(room["total"], 2, "{room}");

    // Close: the default code is 1000; a reason survives its encoding; a code
    // outside what a caller may send is the host's 400, identity intact.
    let (_, bad) = post(
        &node,
        "/chat/x",
        json!({ "op": "close", "sel": {}, "code": 1006 }),
    )
    .await;
    assert_eq!(bad, json!({ "error": "bad_request", "status": 400 }));
    let (_, closed) = post(
        &node,
        "/chat/x",
        json!({ "op": "close", "sel": { "user": "u1" }, "code": 4002, "reason": "a&b=c /?#" }),
    )
    .await;
    assert_eq!(closed, json!({ "closed": 1 }));
    assert_eq!(
        next(&mut rx1).await,
        Outgoing::Close {
            code: 4002,
            reason: "a&b=c /?#".into()
        }
    );
    let (_, closed) = post(&node, "/chat/x", json!({ "op": "close", "sel": "s2" })).await;
    assert_eq!(closed, json!({ "closed": 1 }));
    assert_eq!(
        next(&mut rx2).await,
        Outgoing::Close {
            code: 1000,
            reason: String::new()
        }
    );
    let (_, none) = post(&node, "/chat/x", json!({ "op": "list", "sel": {} })).await;
    assert_eq!(none["total"], 0);
}

#[tokio::test]
async fn socket_ops_spend_the_outbound_call_budget() {
    let limits = LimitTable {
        outbound_calls: 2,
        ..LimitTable::default()
    };
    let node = node(limits).await;
    let (_a, mut rx) = connect(&node, accept("s1", "/chat", "/chat/room/1", None));
    let (_, out) = post(&node, "/chat/x", json!({ "op": "spend", "times": 3 })).await;
    assert_eq!(out, json!([1, 1, "limit_exceeded"]));
    assert_eq!(
        next(&mut rx).await,
        Outgoing::Frame(Frame::Text("x".into()))
    );
    assert_eq!(
        next(&mut rx).await,
        Outgoing::Frame(Frame::Text("x".into()))
    );
    assert!(rx.try_recv().is_err());
}
