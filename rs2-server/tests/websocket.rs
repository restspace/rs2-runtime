//! Inbound WebSockets end to end on the hyper host (`docs/agents/websocket.md`):
//! a real `tokio-tungstenite` client against `rs2_server::serve` on an
//! ephemeral port. `rs2-core/tests/websocket.rs` pins the transport-free
//! contract; these tests pin the wire — the handshake, who gets a 101 and who
//! gets problem+json instead, frames in both directions, `/.sockets/` over
//! HTTP, and that a close reaches the client promptly with the right code.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use http::{Method, StatusCode};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::{
    ClientRequestBuilder, Error as WsError, Message as WsMessage,
};
use tokio_tungstenite::WebSocketStream;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Message, Principal};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::services::auth::{sign, Claims};
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const SECRET: &str = "rs2-server-ws-secret";

struct StaticLoader(Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

/// A node with a socket registry (as `run` wires it) over a temp file root, so
/// pipeline specs have somewhere to live. The `TempDir` is returned because it
/// deletes itself on drop.
fn node(config: Value, limits: LimitTable) -> (Arc<Runtime>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_sockets();
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(StaticLoader(config)),
        limits,
    );
    (rt, dir)
}

/// Serve the runtime on an ephemeral port and return its address.
async fn serve(rt: Arc<Runtime>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = rs2_server::serve(rt, listener, None).await;
    });
    addr
}

fn chat_config(access: Value, web_socket: Value) -> Value {
    json!({
        "auth": { "jwtSecret": SECRET },
        "mounts": [
            { "path": "/auth", "service": "auth" },
            { "path": "/chat", "service": "pipeline",
              "config": { "access": access, "webSocket": web_socket } }
        ]
    })
}

fn as_admin(mut msg: Message) -> Message {
    msg.principal = Some(Principal {
        id: "admin-1".into(),
        roles: vec!["A".into()],
        kind: "user".into(),
        extra: Default::default(),
    });
    msg
}

/// Author the mount's root pipeline spec through the runtime (no wire needed).
async fn author_root(rt: &Runtime, mount: &str, pipeline: Value) {
    let put = as_admin(Message::request(
        Method::PUT,
        &format!("{mount}/.pipelines/.root"),
        "t",
    ))
    .with_json(&json!({ "pipeline": pipeline }));
    let resp = rt.handle(put).await;
    assert!(
        matches!(
            resp.status,
            Some(StatusCode::CREATED) | Some(StatusCode::OK)
        ),
        "author {mount}: {:?}",
        resp.status
    );
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn token(sub: &str, roles: &str, ttl_secs: u64) -> String {
    sign(
        &Claims {
            sub: sub.to_string(),
            roles: roles.to_string(),
            kind: "user".into(),
            iat: now_secs(),
            exp: now_secs() + ttl_secs,
            extra: Default::default(),
        },
        SECRET,
    )
    .unwrap()
}

type Client = WebSocketStream<TcpStream>;

/// Connect a real client. `headers` are added to the generated handshake, so a
/// test can offer a subprotocol or an `Authorization` header.
async fn connect(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, String)],
) -> Result<(Client, http::Response<Option<Vec<u8>>>), WsError> {
    let uri: http::Uri = format!("ws://{addr}{path}").parse().unwrap();
    let mut request = ClientRequestBuilder::new(uri);
    for (name, value) in headers {
        request = request.with_header(*name, value.clone());
    }
    let stream = TcpStream::connect(addr).await.unwrap();
    tokio_tungstenite::client_async(request, stream).await
}

async fn connect_ok(addr: SocketAddr, path: &str, headers: &[(&str, String)]) -> Client {
    match connect(addr, path, headers).await {
        Ok((ws, _)) => ws,
        Err(e) => panic!("expected an upgrade, got {e:?}"),
    }
}

/// The next data frame, as text.
async fn next_text(ws: &mut Client) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let item = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("a frame within the timeout");
        match item {
            Some(Ok(WsMessage::Text(t))) => return t.as_str().to_string(),
            Some(Ok(WsMessage::Binary(b))) => return String::from_utf8_lossy(&b).into_owned(),
            Some(Ok(_)) => continue,
            other => panic!("expected a data frame, got {other:?}"),
        }
    }
}

/// Read until the server's close frame arrives (data frames on the way are
/// ignored) and return its code and reason.
async fn expect_close(ws: &mut Client, within: Duration) -> (u16, String) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let item = tokio::time::timeout_at(deadline, ws.next())
            .await
            .expect("a close frame within the timeout");
        match item {
            Some(Ok(WsMessage::Close(Some(frame)))) => {
                return (u16::from(frame.code), frame.reason.as_str().to_string())
            }
            Some(Ok(WsMessage::Close(None))) => return (1005, String::new()),
            Some(Ok(_)) => continue,
            other => panic!("expected a close frame, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// A minimal HTTP/1.1 client, so `/.sockets/` can be driven over the wire
// without pulling a client stack into the dev dependencies.
// ---------------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl HttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("body {:?}: {e}", self.body))
    }
}

async fn http(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: Option<(&str, &str)>,
) -> HttpResponse {
    let mut head = format!("{method} {target} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some((content_type, payload)) = body {
        head.push_str(&format!(
            "Content-Type: {content_type}\r\nContent-Length: {}\r\n",
            payload.len()
        ));
    }
    head.push_str("\r\n");
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    if let Some((_, payload)) = body {
        stream.write_all(payload.as_bytes()).await.unwrap();
    }
    stream.flush().await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut raw))
        .await
        .expect("response within the timeout")
        .unwrap();
    parse_response(&raw)
}

fn parse_response(raw: &[u8]) -> HttpResponse {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("headers end");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let body = &raw[split + 4..];
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("status line");
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let chunked = headers
        .iter()
        .any(|(n, v)| n == "transfer-encoding" && v.contains("chunked"));
    let body = if chunked {
        dechunk(body)
    } else {
        String::from_utf8_lossy(body).into_owned()
    };
    HttpResponse {
        status,
        headers,
        body,
    }
}

/// Just enough de-chunking for a streamed response body (no trailers).
fn dechunk(mut body: &[u8]) -> String {
    let mut out = Vec::new();
    loop {
        let Some(eol) = body.windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size = usize::from_str_radix(
            String::from_utf8_lossy(&body[..eol])
                .split(';')
                .next()
                .unwrap_or("0")
                .trim(),
            16,
        )
        .unwrap_or(0);
        if size == 0 {
            break;
        }
        let start = eol + 2;
        out.extend_from_slice(&body[start..start + size]);
        body = &body[start + size + 2..];
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The handshake headers a raw HTTP client needs to look like an upgrade.
fn upgrade_headers() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Upgrade", "websocket"),
        ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ("Sec-WebSocket-Version", "13"),
    ]
}

// ---------------------------------------------------------------------------
// Handshake: who gets a 101 and who does not
// ---------------------------------------------------------------------------

/// A mount the caller may not read is refused as the GET it is: the handshake
/// never completes, and the client sees problem+json — never a 101 followed by
/// a close.
#[tokio::test]
async fn an_unauthorized_upgrade_is_problem_json_not_a_101() {
    let (rt, _dir) = node(
        chat_config(json!({ "read": "A", "write": "A" }), json!(true)),
        LimitTable::default(),
    );
    let addr = serve(rt).await;

    match connect(addr, "/chat", &[]).await {
        Err(WsError::Http(resp)) => {
            assert!(
                resp.status() == StatusCode::UNAUTHORIZED || resp.status() == StatusCode::FORBIDDEN,
                "expected 401/403, got {}",
                resp.status()
            );
            assert_eq!(
                resp.headers()
                    .get(http::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok()),
                Some("application/problem+json")
            );
        }
        other => panic!("expected an HTTP refusal, got {other:?}"),
    }

    // And nothing was registered: the mount is unreadable, so the listing is
    // refused too (never an empty success that implies a live socket).
    let listing = http(addr, "GET", "/chat/.sockets/", &[], None).await;
    assert!(
        listing.status == 401 || listing.status == 403,
        "listing: {} {}",
        listing.status,
        listing.body
    );
}

/// A mount without the flag ignores `Upgrade` and serves the plain GET
/// (RFC 9110): no 101, an ordinary representation.
#[tokio::test]
async fn an_upgrade_on_an_unflagged_mount_serves_the_plain_get() {
    let config = json!({
        "mounts": [
            { "path": "/plain", "service": "data",
              "config": { "access": { "read": "all", "write": "all" } } }
        ]
    });
    let (rt, _dir) = node(config, LimitTable::default());
    let seed = Message::request(Method::PUT, "/plain/ds/k", "t").with_json(&json!({ "v": 1 }));
    assert!(rt.handle(seed).await.is_ok());
    let addr = serve(rt).await;

    let resp = http(addr, "GET", "/plain/ds/k", &upgrade_headers(), None).await;
    assert_eq!(resp.status, 200, "{}", resp.body);
    assert_eq!(resp.json()["v"], 1);
    assert!(resp.header("sec-websocket-accept").is_none());

    // The tungstenite client agrees: no handshake happened.
    match connect(addr, "/plain/ds/k", &[]).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), StatusCode::OK),
        other => panic!("expected the plain GET, got {other:?}"),
    }
}

/// A handshake we cannot complete is a 400 — the authorized 101 is dropped
/// rather than sent to a client that never asked in version 13.
#[tokio::test]
async fn a_malformed_handshake_is_a_400_problem() {
    let (rt, _dir) = node(
        chat_config(json!({ "read": "all", "write": "all" }), json!(true)),
        LimitTable::default(),
    );
    let addr = serve(rt).await;

    let no_key = http(
        addr,
        "GET",
        "/chat",
        &[("Upgrade", "websocket"), ("Sec-WebSocket-Version", "13")],
        None,
    )
    .await;
    assert_eq!(no_key.status, 400, "{}", no_key.body);
    assert_eq!(
        no_key.header("content-type"),
        Some("application/problem+json")
    );

    let old_version = http(
        addr,
        "GET",
        "/chat",
        &[
            ("Upgrade", "websocket"),
            ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
            ("Sec-WebSocket-Version", "8"),
        ],
        None,
    )
    .await;
    assert_eq!(old_version.status, 400, "{}", old_version.body);
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// The mount's pipeline runs per message and its 2xx JSON body comes back on
/// the same socket as a text frame.
#[tokio::test]
async fn a_pipeline_mount_answers_every_message_with_a_text_frame() {
    let (rt, _dir) = node(
        chat_config(json!({ "read": "all", "write": "A" }), json!(true)),
        LimitTable::default(),
    );
    author_root(&rt, "/chat", json!([{ "echoed": "$" }])).await;
    let addr = serve(rt).await;

    let mut ws = connect_ok(addr, "/chat", &[]).await;
    ws.send(WsMessage::Text(json!({ "hi": "there" }).to_string().into()))
        .await
        .unwrap();
    let raw = next_text(&mut ws).await;
    let reply: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(reply["echoed"]["hi"], "there", "{raw}");

    // A second message is its own dispatch.
    ws.send(WsMessage::Text(json!({ "hi": "again" }).to_string().into()))
        .await
        .unwrap();
    let reply: Value = serde_json::from_str(&next_text(&mut ws).await).unwrap();
    assert_eq!(reply["echoed"]["hi"], "again");
}

/// `/.sockets/` over HTTP: push a frame, list the connected sockets, then close
/// one — and the client observes the close code promptly (the Worker host's
/// known CLOSING issue must not reproduce here).
#[tokio::test]
async fn the_sockets_subtree_pushes_lists_and_closes() {
    let (rt, _dir) = node(
        chat_config(json!({ "read": "all", "write": "all" }), json!(true)),
        LimitTable::default(),
    );
    let addr = serve(rt).await;

    let mut ws = connect_ok(addr, "/chat/room", &[]).await;

    let pushed = http(
        addr,
        "POST",
        "/chat/.sockets/room",
        &[],
        Some(("text/plain", "hello room")),
    )
    .await;
    assert_eq!(pushed.status, 200, "{}", pushed.body);
    assert_eq!(pushed.json()["sent"], 1);
    assert_eq!(next_text(&mut ws).await, "hello room");

    let listing = http(addr, "GET", "/chat/.sockets/", &[], None).await;
    assert_eq!(listing.status, 200, "{}", listing.body);
    let listing = listing.json();
    assert_eq!(listing["total"], 1, "{listing}");
    assert_eq!(listing["entries"][0]["path"], "/chat/room");

    let closed = http(
        addr,
        "DELETE",
        "/chat/.sockets/room?code=4000&reason=bye",
        &[],
        None,
    )
    .await;
    assert_eq!(closed.status, 200, "{}", closed.body);
    assert_eq!(closed.json()["closed"], 1);

    let (code, reason) = expect_close(&mut ws, Duration::from_secs(3)).await;
    assert_eq!((code, reason.as_str()), (4000, "bye"));
    // The transport goes away too, rather than leaving the client in CLOSING.
    let ended = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .expect("the connection to be torn down");
    assert!(
        matches!(ended, None | Some(Err(_))),
        "expected the stream to end, got {ended:?}"
    );
}

/// A client-initiated close is echoed, so the closing handshake completes from
/// either side, and the socket leaves the hub (a later push finds nothing).
#[tokio::test]
async fn a_client_close_is_echoed_and_releases_the_socket() {
    let (rt, _dir) = node(
        chat_config(json!({ "read": "all", "write": "all" }), json!(true)),
        LimitTable::default(),
    );
    let addr = serve(rt).await;

    let mut ws = connect_ok(addr, "/chat/room", &[]).await;
    ws.close(None).await.unwrap();
    // `close` on the client already consumed the echo; the stream then ends.
    let ended = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .expect("the handshake to complete");
    assert!(
        matches!(ended, None | Some(Ok(WsMessage::Close(_)))),
        "expected a clean end, got {ended:?}"
    );

    // Give the host a moment to dispatch `close`, then prove the socket is gone.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pushed = http(
        addr,
        "POST",
        "/chat/.sockets/room",
        &[],
        Some(("text/plain", "anyone?")),
    )
    .await;
    assert_eq!(pushed.json()["sent"], 0, "{}", pushed.body);
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// A frame over `wsMessageBytes` closes with `4413
/// limit_exceeded:ws_message_bytes` — it must reach the session, so the
/// transport ceiling sits well above the RS2 limit.
#[tokio::test]
async fn an_oversize_frame_closes_with_4413() {
    let limits = LimitTable {
        ws_message_bytes: 64,
        ..Default::default()
    };
    let (rt, _dir) = node(
        chat_config(json!({ "read": "all", "write": "A" }), json!(true)),
        limits,
    );
    author_root(&rt, "/chat", json!([{ "echoed": "$" }])).await;
    let addr = serve(rt).await;

    let mut ws = connect_ok(addr, "/chat", &[]).await;
    ws.send(WsMessage::Text("x".repeat(200).into()))
        .await
        .unwrap();
    let (code, reason) = expect_close(&mut ws, Duration::from_secs(3)).await;
    assert_eq!(
        (code, reason.as_str()),
        (4413, "limit_exceeded:ws_message_bytes")
    );
}

/// Too many frames a second closes with `4429`, naming the rate limit.
#[tokio::test]
async fn a_rate_breach_closes_with_4429() {
    let limits = LimitTable {
        ws_messages_per_second: 2,
        // Keep the in-flight ceiling out of the way, so the breach we observe
        // is unambiguously the rate one.
        ws_messages_in_flight: 64,
        ..Default::default()
    };
    let (rt, _dir) = node(
        chat_config(json!({ "read": "all", "write": "A" }), json!(true)),
        limits,
    );
    author_root(&rt, "/chat", json!([{ "echoed": "$" }])).await;
    let addr = serve(rt).await;

    let mut ws = connect_ok(addr, "/chat", &[]).await;
    for i in 0..4 {
        ws.send(WsMessage::Text(format!("{{\"n\":{i}}}").into()))
            .await
            .unwrap();
    }
    let (code, reason) = expect_close(&mut ws, Duration::from_secs(3)).await;
    assert_eq!(
        (code, reason.as_str()),
        (4429, "limit_exceeded:ws_messages_per_second")
    );
}

// ---------------------------------------------------------------------------
// Auth over the handshake
// ---------------------------------------------------------------------------

/// A browser cannot set `Authorization` on an upgrade, so the token rides in
/// `Sec-WebSocket-Protocol` beside a real protocol; the host authenticates from
/// it and selects the non-bearer entry on the 101.
#[tokio::test]
async fn a_bearer_subprotocol_authenticates_and_the_real_protocol_is_selected() {
    let (rt, _dir) = node(
        // Reading (which is what an upgrade is) needs `U`; authoring the spec
        // stays with `A`.
        chat_config(json!({ "read": "U", "write": "A" }), json!(true)),
        LimitTable::default(),
    );
    author_root(&rt, "/chat", json!([{ "echoed": "$" }])).await;
    let addr = serve(rt).await;

    // Without a token the same upgrade is refused.
    match connect(addr, "/chat", &[]).await {
        Err(WsError::Http(resp)) => assert!(
            resp.status() == StatusCode::UNAUTHORIZED || resp.status() == StatusCode::FORBIDDEN,
            "got {}",
            resp.status()
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }

    let offered = format!("rs2, rs2.bearer.{}", token("alice", "U", 300));
    let (mut ws, resp) = connect(addr, "/chat", &[("Sec-WebSocket-Protocol", offered)])
        .await
        .expect("the bearer subprotocol to authenticate");
    assert_eq!(
        resp.headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok()),
        Some("rs2"),
        "the selected protocol must be the offered non-bearer one"
    );
    ws.send(WsMessage::Text(json!({ "hi": "there" }).to_string().into()))
        .await
        .unwrap();
    let reply: Value = serde_json::from_str(&next_text(&mut ws).await).unwrap();
    assert_eq!(reply["echoed"]["hi"], "there");
}

/// The connect token's expiry ends the socket: `4401 unauthorized`, without
/// waiting for the client to send anything.
#[tokio::test]
async fn an_expiring_token_closes_the_socket_with_4401() {
    let (rt, _dir) = node(
        chat_config(json!({ "read": "U", "write": "U" }), json!(true)),
        LimitTable::default(),
    );
    let addr = serve(rt).await;

    let offered = format!("rs2, rs2.bearer.{}", token("alice", "U", 2));
    let mut ws = connect_ok(addr, "/chat", &[("Sec-WebSocket-Protocol", offered)]).await;
    let (code, reason) = expect_close(&mut ws, Duration::from_secs(8)).await;
    assert_eq!((code, reason.as_str()), (4401, "unauthorized"));
}
