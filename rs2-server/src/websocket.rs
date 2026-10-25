//! Inbound WebSockets on the hyper host (`docs/agents/websocket.md`): the
//! listener half of the transport-free `rs2_core::sockets` contract.
//!
//! `Runtime::dispatch` authorizes an upgrade as the GET it is and hands back a
//! `101` carrying a `SocketAccept` — so **a 101 is never sent before
//! authorization**, and a denial is the ordinary problem+json. This module
//! turns that marker into a real RFC 6455 handshake, then pumps one task per
//! connection: the hub's `Outgoing` queue onto the wire, and inbound frames
//! into [`SocketSession`] (which owns every limit, the dispatch of each event
//! and the reply rule). Nothing about RS2 semantics lives here.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use http::{HeaderMap, Response, StatusCode};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use tokio::sync::{mpsc, Notify};
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use tokio_tungstenite::WebSocketStream;

use rs2_core::message::Message;
use rs2_core::sockets::{Frame, Outgoing, SocketSession};
use rs2_core::{RsError, Runtime};

use crate::OutBody;

/// How long we keep reading after the close frame has gone out, waiting for the
/// peer's echo. The Worker host has a known issue where a close issued from
/// another request leaves the client stuck in `CLOSING`; here the connection
/// task always drops both halves of the socket when this loop ends, which
/// shuts the TCP connection down, so a client that never echoes still sees the
/// transport go away within the grace period.
const CLOSE_GRACE: Duration = Duration::from_secs(5);

/// The transport's own frame ceiling, comfortably above `ws_message_bytes`: an
/// oversize frame must still *reach* `SocketSession::frame`, which is what
/// closes with `4413 limit_exceeded:ws_message_bytes`. A frame tungstenite
/// refuses to read is a bare 1009 with no RS2 reason, so we make that the rare
/// case rather than the normal one.
fn transport_cap(ws_message_bytes: u64) -> usize {
    let generous = ws_message_bytes.saturating_mul(4).max(16 * 1024 * 1024);
    usize::try_from(generous).unwrap_or(usize::MAX)
}

/// Is this a WebSocket upgrade? Answered on the hyper request, before it is
/// converted into a `Message`, because taking the upgrade handle needs
/// `&mut req` while the conversion consumes it. The method is part of the test
/// (RFC 6455 §4.1); `Upgrade` is case-insensitive (RFC 9110 §7.8).
pub(crate) fn is_upgrade(req: &hyper::Request<Incoming>) -> bool {
    req.method() == http::Method::GET
        && req
            .headers()
            .get(http::header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|u| u.trim().eq_ignore_ascii_case("websocket"))
}

/// The handshake fields, captured before the request is consumed.
pub(crate) struct Handshake {
    key: Option<String>,
    version: Option<String>,
}

impl Handshake {
    pub(crate) fn capture(headers: &HeaderMap) -> Self {
        let get = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.trim().to_string())
        };
        Handshake {
            key: get("sec-websocket-key").filter(|k| !k.is_empty()),
            version: get("sec-websocket-version"),
        }
    }

    /// `Sec-WebSocket-Accept` for this handshake (RFC 6455 §4.2.2). A missing
    /// key or a version other than 13 is a 400 — an authorized upgrade we
    /// cannot complete is still an ordinary HTTP failure, never a 101 followed
    /// by a close.
    fn accept_key(&self) -> Result<String, RsError> {
        let Some(key) = &self.key else {
            return Err(RsError::bad_request(
                "a WebSocket upgrade requires a 'Sec-WebSocket-Key' header",
            ));
        };
        match self.version.as_deref() {
            Some("13") => {}
            other => {
                return Err(RsError::bad_request(format!(
                    "unsupported WebSocket version {:?}: RS2 speaks 13",
                    other.unwrap_or("(absent)")
                )))
            }
        }
        Ok(tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes()))
    }
}

/// Complete an authorized upgrade: validate the handshake, register the socket
/// with the hub, answer `101`, and spawn the task that owns the wire. Any
/// `Err` is answered as problem+json by the caller — registration can still
/// fail (the tenant's socket ceiling), and failing *before* the 101 is the
/// whole point of `SocketSession::start` being separate from the dispatch.
pub(crate) fn accept(
    runtime: &Arc<Runtime>,
    resp: Message,
    handshake: Handshake,
    upgrade: hyper::upgrade::OnUpgrade,
) -> Result<Response<OutBody>, RsError> {
    let accept_key = handshake.accept_key()?;
    let mut resp = resp;
    let accept = *resp
        .socket_accept
        .take()
        .expect("caller checked socket_accept");
    let protocol = accept.protocol.clone();
    let cap = transport_cap(runtime.limits().ws_message_bytes);
    let (session, rx) = SocketSession::start(runtime, accept)?;

    let mut builder = Response::builder().status(StatusCode::SWITCHING_PROTOCOLS);
    if let Some(headers) = builder.headers_mut() {
        // Whatever the runtime decorated the 101 with (the CORS headers a
        // tenant policy adds) travels with the handshake; a 101 carries no
        // representation, so content-type/length are not among them.
        *headers = resp.headers.clone();
        headers.remove(http::header::CONTENT_TYPE);
        headers.remove(http::header::CONTENT_LENGTH);
    }
    let mut response = builder
        .header(http::header::UPGRADE, "websocket")
        .header(http::header::CONNECTION, "Upgrade")
        .header("sec-websocket-accept", accept_key)
        .body(crate::empty_body())
        .expect("static handshake headers are valid");
    if let Some(protocol) = protocol {
        if let Ok(v) = http::HeaderValue::from_str(&protocol) {
            response.headers_mut().insert("sec-websocket-protocol", v);
        }
    }

    tokio::spawn(pump(session, rx, upgrade, cap));
    Ok(response)
}

/// How the connection ended, as `SocketSession::closed` wants it.
struct Ending {
    code: u16,
    reason: String,
    clean: bool,
}

impl Ending {
    /// The peer's close frame. Tungstenite reports a close with no payload as
    /// `None`, which is the wire's "no code" — `1005` (RFC 6455 §7.1.5).
    fn from_peer(frame: Option<CloseFrame>) -> Ending {
        match frame {
            Some(f) => Ending {
                code: u16::from(f.code),
                reason: f.reason.as_str().to_string(),
                clean: true,
            },
            None => Ending {
                code: 1005,
                reason: String::new(),
                clean: true,
            },
        }
    }

    /// The transport just went away: no close handshake happened.
    fn abnormal(code: u16) -> Ending {
        Ending {
            code,
            reason: String::new(),
            clean: false,
        }
    }
}

type Socket = WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

/// Drain the hub's queue onto the wire. A `Close` is the last item by
/// construction, so the loop ends there; `closing` then tells the reader to
/// start its grace period for the peer's echo. Dropping the sink on the way
/// out is what lets the TCP connection go once the reader is done too.
async fn pump_writer(
    mut sink: futures::stream::SplitSink<Socket, WsMessage>,
    mut rx: mpsc::Receiver<Outgoing>,
    closing: Arc<Notify>,
) {
    while let Some(item) = rx.recv().await {
        let (msg, last) = match item {
            Outgoing::Frame(Frame::Text(text)) => (WsMessage::Text(text.into()), false),
            Outgoing::Frame(Frame::Binary(bytes)) => (WsMessage::Binary(bytes), false),
            Outgoing::Close { code, reason } => (
                WsMessage::Close(Some(CloseFrame {
                    code: code.into(),
                    reason: reason.into(),
                })),
                true,
            ),
        };
        if sink.send(msg).await.is_err() {
            break;
        }
        if last {
            let _ = sink.flush().await;
            break;
        }
    }
    closing.notify_one();
}

/// One connection, start to finish: await the upgrade, frame it, run the
/// writer beside the reader (so a reply to `open` can leave before the first
/// inbound frame is read), then report exactly one `closed`.
async fn pump(
    session: Arc<SocketSession>,
    rx: mpsc::Receiver<Outgoing>,
    upgrade: hyper::upgrade::OnUpgrade,
    cap: usize,
) {
    let upgraded = match upgrade.await {
        Ok(upgraded) => upgraded,
        Err(e) => {
            // The 101 went out but the connection never materialized; the
            // socket is registered, so it still needs its `close`.
            eprintln!("websocket upgrade failed: {e}");
            session.closed(1006, "", false).await;
            return;
        }
    };
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(cap);
    config.max_frame_size = Some(cap);
    let socket =
        WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(config)).await;
    let (sink, mut stream) = socket.split();
    let closing = Arc::new(Notify::new());
    let writer = tokio::spawn(pump_writer(sink, rx, closing.clone()));

    // `open` before the first frame is read, but with the writer already
    // pumping, so a handler that replies on the socket is not deadlocked.
    session.opened().await;

    let mut ending: Option<Ending> = None;
    let mut teardown: Option<tokio::time::Instant> = None;
    let mut expired = false;
    loop {
        // The token expiry is only armed while the socket is live: once a close
        // is in flight the deadline is the grace period instead.
        let expires_in = if teardown.is_none() && ending.is_none() && !expired {
            session.expires_in()
        } else {
            None
        };
        let until_expiry = async {
            match expires_in {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending().await,
            }
        };
        let until_teardown = async {
            match teardown {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            // The hub gave up on this socket (its queue overflowed): drop the
            // connection, no close handshake.
            _ = session.killed() => {
                ending.get_or_insert_with(|| Ending::abnormal(1006));
                break;
            }
            _ = closing.notified(), if teardown.is_none() => {
                teardown = Some(tokio::time::Instant::now() + CLOSE_GRACE);
            }
            _ = until_teardown => break,
            _ = until_expiry => {
                expired = true;
                session.expire();
            }
            item = stream.next() => match item {
                Some(Ok(WsMessage::Text(text))) => {
                    session.frame(Frame::Text(text.as_str().to_string()));
                }
                Some(Ok(WsMessage::Binary(bytes))) => {
                    session.frame(Frame::Binary(bytes));
                }
                // Ping is answered by tungstenite itself; a Pong or a raw
                // frame is nothing to dispatch.
                Some(Ok(WsMessage::Close(frame))) => {
                    // Keep polling: the next read flushes our close echo and
                    // ends the stream, which completes the handshake.
                    ending = Some(Ending::from_peer(frame));
                    teardown = Some(tokio::time::Instant::now() + CLOSE_GRACE);
                }
                Some(Ok(_)) => {}
                // A frame past the transport ceiling never reached the session,
                // so there is no RS2 reason to give: a plain "message too big".
                Some(Err(WsError::Capacity(_))) => {
                    ending = Some(Ending::abnormal(1009));
                    break;
                }
                Some(Err(_)) | None => break,
            },
        }
    }

    // Dropping the sink releases the other half of the socket, so the TCP
    // connection is torn down as soon as this task returns — a client we closed
    // never lingers in CLOSING.
    writer.abort();
    let ending = ending.unwrap_or_else(|| Ending::abnormal(1006));
    session
        .closed(ending.code, &ending.reason, ending.clean)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The transport ceiling must leave room for an oversize frame to arrive,
    /// because the 4413 close is the session's job, not tungstenite's.
    #[test]
    fn transport_cap_sits_above_the_rs2_limit() {
        assert_eq!(transport_cap(1024), 16 * 1024 * 1024);
        assert_eq!(transport_cap(64 * 1024 * 1024), 256 * 1024 * 1024);
        assert_eq!(transport_cap(u64::MAX), usize::MAX);
    }

    /// Only version 13, and only with a key: both are 400s rather than a 101
    /// we could not honour. The digest is RFC 6455 §1.3's worked example.
    #[test]
    fn handshake_validation_and_accept_key() {
        let hs = Handshake {
            key: Some("dGhlIHNhbXBsZSBub25jZQ==".into()),
            version: Some("13".into()),
        };
        assert_eq!(hs.accept_key().unwrap(), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
        let no_key = Handshake {
            key: None,
            version: Some("13".into()),
        };
        assert_eq!(no_key.accept_key().unwrap_err().status, 400);
        let old = Handshake {
            key: Some("dGhlIHNhbXBsZSBub25jZQ==".into()),
            version: Some("8".into()),
        };
        assert_eq!(old.accept_key().unwrap_err().status, 400);
    }
}
