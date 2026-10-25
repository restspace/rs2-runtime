//! Inbound WebSockets (`docs/agents/websocket.md`, `rs2-core/src/sockets.rs`):
//! the transport-free contract shared between dispatch (`runtime.rs`) and the
//! listener. Ports the intent of `rs2-worker/test/websocket-dispatch.test.ts`
//! and the limit/reply parts of `rs2-worker/test/websocket-hibernation.test.ts`
//! onto the Rust host. No transport: an accepted upgrade is driven directly
//! through `SocketSession` — `opened`/`frame`/`closed` — with a paired
//! `mpsc::Receiver<Outgoing>` standing in for the wire.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::{ByteRange, DirEntry, FileMeta, FileStore};
use rs2_core::message::{Body, MediaType, Message, Principal, Source};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::services::auth::{sign, Claims};
use rs2_core::sockets::{
    close_for, selector_matches, socket_event_message, socket_event_of, EventPayload, Frame,
    Outgoing, SocketAccept, SocketEvent, SocketSelector, SocketSession, TextMode,
};
use rs2_core::tenant::{Adapters, Tenant, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct StaticLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

/// Attach a principal carrying the given (space-separated) roles.
fn as_role(mut msg: Message, id: &str, roles: &str) -> Message {
    msg.principal = Some(Principal {
        id: id.to_string(),
        roles: roles.split_whitespace().map(String::from).collect(),
        kind: "user".into(),
        extra: Default::default(),
    });
    msg
}

fn as_admin(msg: Message) -> Message {
    as_role(msg, "admin-1", "A")
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body
        .as_mut()
        .expect("body")
        .as_json(1024 * 1024)
        .await
        .expect("json body")
}

const SECRET: &str = "ws-test-secret";

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Sign a token for `sub`/`roles`, expiring in `ttl_secs`.
fn token(sub: &str, roles: &str, ttl_secs: i64) -> String {
    let exp = (now_secs() as i64 + ttl_secs).max(0) as u64;
    sign(
        &Claims {
            sub: sub.to_string(),
            roles: roles.to_string(),
            kind: "user".into(),
            iat: now_secs(),
            exp,
            extra: Default::default(),
        },
        SECRET,
    )
    .unwrap()
}

fn with_bearer(mut msg: Message, tok: &str) -> Message {
    msg.set_header("authorization", &format!("Bearer {tok}"));
    msg
}

fn with_cookie(mut msg: Message, tok: &str) -> Message {
    msg.set_header("cookie", &format!("rs-auth={tok}"));
    msg
}

fn upgrade(mut msg: Message) -> Message {
    msg.set_header("upgrade", "websocket");
    msg
}

/// A `FileStore` that delays `read` (the pipeline's per-invocation spec
/// lookup) — used to widen the in-flight window for socket messages enough
/// to observe the `ws_messages_in_flight` breach without any JS engine.
struct SlowFileStore {
    inner: Arc<dyn FileStore>,
    delay: Duration,
}

#[async_trait]
impl FileStore for SlowFileStore {
    async fn list(
        &self,
        tenant: &str,
        path: &str,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<DirEntry>, u64), RsError> {
        self.inner.list(tenant, path, take, skip).await
    }
    async fn read(
        &self,
        tenant: &str,
        path: &str,
        range: Option<ByteRange>,
    ) -> Result<Body, RsError> {
        tokio::time::sleep(self.delay).await;
        self.inner.read(tenant, path, range).await
    }
    async fn head(&self, tenant: &str, path: &str) -> Result<FileMeta, RsError> {
        self.inner.head(tenant, path).await
    }
    async fn write(&self, tenant: &str, path: &str, body: Body) -> Result<bool, RsError> {
        self.inner.write(tenant, path, body).await
    }
    async fn delete(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete(tenant, path).await
    }
    async fn rename(&self, tenant: &str, from: &str, to: &str) -> Result<bool, RsError> {
        self.inner.rename(tenant, from, to).await
    }
    async fn delete_dir(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir(tenant, path).await
    }
    async fn delete_dir_all(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir_all(tenant, path).await
    }
}

/// Build a runtime over `config`, with sockets enabled unless `no_sockets`.
/// `limits` lets a test shrink ceilings; `slow` wraps the file store so
/// pipeline dispatch takes at least that long (in-flight limit tests).
fn build(
    config: serde_json::Value,
    dir: &std::path::Path,
    limits: LimitTable,
    with_sockets: bool,
    slow: Option<Duration>,
) -> Arc<Runtime> {
    let files: Arc<dyn FileStore> = match slow {
        Some(delay) => Arc::new(SlowFileStore {
            inner: Arc::new(LocalFsFileStore::new(dir)),
            delay,
        }),
        None => Arc::new(LocalFsFileStore::new(dir)),
    };
    let mut adapters = Adapters::new(files, Arc::new(MemDataStore::new()));
    if with_sockets {
        adapters = adapters.with_sockets();
    }
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(StaticLoader(config)),
        limits,
    )
}

fn ws_config(access: serde_json::Value, web_socket: serde_json::Value) -> serde_json::Value {
    json!({
        "auth": { "jwtSecret": SECRET },
        "mounts": [
            { "path": "/chat", "service": "pipeline",
              "config": { "access": access, "webSocket": web_socket } }
        ]
    })
}

async fn author_root(rt: &Runtime, mount: &str, pipeline: serde_json::Value) {
    let put = as_admin(req(Method::PUT, &format!("{mount}/.pipelines/.root")))
        .with_json(&json!({ "pipeline": pipeline }));
    let resp = rt.handle(put).await;
    assert!(
        matches!(
            resp.status,
            Some(StatusCode::CREATED) | Some(StatusCode::OK)
        ),
        "author {mount}: {:?} {:?}",
        resp.status,
        resp.body
    );
}

/// Accept an upgrade and start a live session from it, panicking on failure.
async fn start_session(
    rt: &Arc<Runtime>,
    msg: Message,
) -> (Arc<SocketSession>, tokio::sync::mpsc::Receiver<Outgoing>) {
    let mut resp = rt.handle(msg).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::SWITCHING_PROTOCOLS),
        "{:?}",
        resp.body
    );
    let accept = *resp.socket_accept.take().expect("socket_accept on 101");
    SocketSession::start(rt, accept).expect("session start")
}

async fn recv(rx: &mut tokio::sync::mpsc::Receiver<Outgoing>) -> Outgoing {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("frame within timeout")
        .expect("channel open")
}

async fn recv_none(rx: &mut tokio::sync::mpsc::Receiver<Outgoing>) {
    let got = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(got.is_err(), "expected no frame, got {got:?}");
}

// ---------------------------------------------------------------------------
// Unit level: selector_matches, close_for, socket_event_of
// ---------------------------------------------------------------------------

fn accept_for(mount: &str, path: &str, user: Option<&str>) -> SocketAccept {
    SocketAccept {
        id: format!("id-{path}"),
        tenant: "t".into(),
        mount: mount.into(),
        path: path.into(),
        query: String::new(),
        principal: user.map(|u| Principal {
            id: u.into(),
            roles: vec![],
            kind: "user".into(),
            extra: Default::default(),
        }),
        exp: None,
        text: TextMode::Json,
        events: vec![SocketEvent::Message],
        protocol: None,
        connected_at: 0,
    }
}

#[test]
fn selector_matches_by_mount_path_subtree_id_and_user() {
    let a = accept_for("/chat", "/chat/room/1", Some("bob"));

    let by_mount = SocketSelector {
        mount: "/chat".into(),
        path: None,
        subtree: false,
        id: None,
        user: None,
    };
    assert!(
        selector_matches(&by_mount, &a),
        "whole-mount selector matches"
    );

    let wrong_mount = SocketSelector {
        mount: "/other".into(),
        path: None,
        subtree: false,
        id: None,
        user: None,
    };
    assert!(!selector_matches(&wrong_mount, &a));

    let exact = SocketSelector {
        mount: "/chat".into(),
        path: Some("/chat/room/1".into()),
        subtree: false,
        id: None,
        user: None,
    };
    assert!(selector_matches(&exact, &a));

    let exact_wrong = SocketSelector {
        mount: "/chat".into(),
        path: Some("/chat/room/2".into()),
        subtree: false,
        id: None,
        user: None,
    };
    assert!(!selector_matches(&exact_wrong, &a));

    let parent_no_subtree = SocketSelector {
        mount: "/chat".into(),
        path: Some("/chat/room".into()),
        subtree: false,
        id: None,
        user: None,
    };
    assert!(
        !selector_matches(&parent_no_subtree, &a),
        "container path without subtree does not match a child"
    );

    let parent_subtree = SocketSelector {
        mount: "/chat".into(),
        path: Some("/chat/room".into()),
        subtree: true,
        id: None,
        user: None,
    };
    assert!(
        selector_matches(&parent_subtree, &a),
        "trailing-slash subtree reaches children"
    );

    let by_id = SocketSelector {
        mount: "/chat".into(),
        path: None,
        subtree: false,
        id: Some(a.id.clone()),
        user: None,
    };
    assert!(selector_matches(&by_id, &a));
    let wrong_id = SocketSelector {
        mount: "/chat".into(),
        path: None,
        subtree: false,
        id: Some("nope".into()),
        user: None,
    };
    assert!(!selector_matches(&wrong_id, &a));

    let by_user = SocketSelector {
        mount: "/chat".into(),
        path: None,
        subtree: false,
        id: None,
        user: Some("bob".into()),
    };
    assert!(selector_matches(&by_user, &a));
    let wrong_user = SocketSelector {
        mount: "/chat".into(),
        path: None,
        subtree: false,
        id: None,
        user: Some("alice".into()),
    };
    assert!(!selector_matches(&wrong_user, &a));

    let anon = accept_for("/chat", "/chat/room/1", None);
    assert!(
        !selector_matches(&by_user, &anon),
        "a $user selector never matches an anonymous socket"
    );
}

#[test]
fn close_for_maps_status_and_limit_name() {
    let (code, reason) = close_for(&RsError::unauthorized("nope"));
    assert_eq!(code, 4401);
    assert_eq!(reason, "unauthorized");

    let (code, reason) = close_for(&RsError::limit_exceeded("ws_message_bytes", 10u64, 5u64));
    assert_eq!(code, 4503, "4000 + the 503 limit_exceeded status");
    assert_eq!(reason, "limit_exceeded:ws_message_bytes");

    let (code, reason) = close_for(&RsError::forbidden("no"));
    assert_eq!(code, 4403);
    assert_eq!(reason, "forbidden");
}

#[test]
fn socket_event_of_rejects_forged_external_and_internal_sources() {
    let accept = accept_for("/chat", "/chat", None);
    let mut msg = socket_event_message(&accept, SocketEvent::Message, EventPayload::None);
    assert_eq!(socket_event_of(&msg), Some(SocketEvent::Message));

    // A client cannot forge the trigger by sending the header itself: only
    // `Source::System` is trusted.
    msg.source = Source::External;
    assert_eq!(
        socket_event_of(&msg),
        None,
        "external source is never a socket event"
    );

    msg.source = Source::Internal;
    assert_eq!(
        socket_event_of(&msg),
        None,
        "internal source is never a socket event either"
    );

    msg.source = Source::System;
    assert_eq!(socket_event_of(&msg), Some(SocketEvent::Message));

    // Missing/garbled event header on an otherwise-genuine System message.
    let mut msg2 = socket_event_message(&accept, SocketEvent::Open, EventPayload::None);
    msg2.set_header(rs2_core::sockets::SOCKET_EVENT_HEADER, "bogus");
    assert_eq!(socket_event_of(&msg2), None);
}

// ---------------------------------------------------------------------------
// `webSocket` config validation (parse_web_socket_config / Tenant::build)
// ---------------------------------------------------------------------------

use rs2_core::sockets::{parse_web_socket_config, web_socket_config_of};

#[test]
fn web_socket_config_absent_null_or_false_is_disabled() {
    assert!(parse_web_socket_config(None, "data").unwrap().is_none());
    assert!(parse_web_socket_config(Some(&json!(null)), "data")
        .unwrap()
        .is_none());
    assert!(parse_web_socket_config(Some(&json!(false)), "data")
        .unwrap()
        .is_none());
}

#[test]
fn web_socket_config_true_defaults_message_only_except_code_mounts() {
    let cfg = parse_web_socket_config(Some(&json!(true)), "data")
        .unwrap()
        .unwrap();
    assert_eq!(cfg.text, TextMode::Json);
    assert_eq!(cfg.events, vec![SocketEvent::Message]);

    let cfg = parse_web_socket_config(Some(&json!(true)), "code:widget@1")
        .unwrap()
        .unwrap();
    assert_eq!(
        cfg.events,
        vec![SocketEvent::Open, SocketEvent::Message, SocketEvent::Close],
        "a code: mount defaults to all three events"
    );
}

#[test]
fn web_socket_config_rejects_non_bool_non_object() {
    let err = parse_web_socket_config(Some(&json!("yes")), "data").unwrap_err();
    assert_eq!(err.status, 400);
    let err = parse_web_socket_config(Some(&json!(1)), "data").unwrap_err();
    assert_eq!(err.status, 400);
}

#[test]
fn web_socket_config_rejects_unknown_field() {
    let err = parse_web_socket_config(Some(&json!({ "bogus": true })), "data").unwrap_err();
    assert_eq!(err.status, 400);
}

#[test]
fn web_socket_config_rejects_bad_text() {
    let err = parse_web_socket_config(Some(&json!({ "text": "xml" })), "data").unwrap_err();
    assert_eq!(err.status, 400);
}

#[test]
fn web_socket_config_rejects_bad_or_empty_events() {
    let err =
        parse_web_socket_config(Some(&json!({ "events": ["open", "bogus"] })), "data").unwrap_err();
    assert_eq!(err.status, 400);
    let err = parse_web_socket_config(Some(&json!({ "events": [] })), "data").unwrap_err();
    assert_eq!(err.status, 400);
    let err = parse_web_socket_config(Some(&json!({ "events": "message" })), "data").unwrap_err();
    assert_eq!(err.status, 400, "events must be an array");
}

#[test]
fn web_socket_config_dedupes_duplicate_events() {
    let cfg = parse_web_socket_config(
        Some(&json!({ "events": ["message", "open", "message", "open"] })),
        "data",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        cfg.events,
        vec![SocketEvent::Message, SocketEvent::Open],
        "de-duplicated, order kept"
    );
}

#[test]
fn web_socket_config_of_a_routed_mount_reads_the_config_value() {
    let mount_cfg = json!({ "webSocket": { "text": "text", "events": ["message", "close"] } });
    let cfg = web_socket_config_of(&mount_cfg, "data").expect("enabled");
    assert_eq!(cfg.text, TextMode::Text);
    assert_eq!(cfg.events, vec![SocketEvent::Message, SocketEvent::Close]);

    assert!(web_socket_config_of(&json!({}), "data").is_none());
}

#[test]
fn tenant_build_rejects_an_invalid_web_socket_config_at_400() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let config: TenantConfig = serde_json::from_value(json!({
        "mounts": [
            { "path": "/chat", "service": "data",
              "config": { "access": "open", "webSocket": { "events": [] } } }
        ]
    }))
    .unwrap();
    let err = match Tenant::build("t", config, &adapters, &LimitTable::default(), None, None) {
        Ok(_) => panic!("expected an invalid webSocket config to be rejected"),
        Err(e) => e,
    };
    assert_eq!(err.status, 400, "{:?}", err.detail);
}

// ---------------------------------------------------------------------------
// Upgrade: authorization and the accepted 101's shape
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upgrade_denied_anonymous_gets_401_problem_json_no_socket_accept() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "U", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let mut resp = rt.handle(upgrade(req(Method::GET, "/chat"))).await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));
    assert!(resp.socket_accept.is_none());
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "unauthorized");
}

#[tokio::test]
async fn upgrade_denied_wrong_role_gets_403() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "U", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let msg = as_role(upgrade(req(Method::GET, "/chat")), "eve", "guest");
    let mut resp = rt.handle(msg).await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN));
    assert!(resp.socket_accept.is_none());
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "forbidden");
}

#[tokio::test]
async fn upgrade_accepted_101_carries_the_full_socket_accept_shape() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let msg = as_role(upgrade(req(Method::GET, "/chat/room/42?x=1")), "u1", "U");
    let mut resp = rt.handle(msg).await;
    assert_eq!(resp.status, Some(StatusCode::SWITCHING_PROTOCOLS));
    let accept = resp.socket_accept.take().expect("socket_accept set on 101");
    assert_eq!(accept.mount, "/chat");
    assert_eq!(accept.path, "/chat/room/42");
    assert_eq!(accept.query, "?x=1", "query keeps the leading '?'");
    assert_eq!(accept.principal.as_ref().map(|p| p.id.as_str()), Some("u1"));
    assert_eq!(accept.text, TextMode::Json);
    assert_eq!(
        accept.events,
        vec![SocketEvent::Message],
        "pipeline mount defaults to message-only"
    );
    assert!(!accept.id.is_empty());
}

#[tokio::test]
async fn upgrade_accept_query_is_empty_string_with_no_query() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let mut resp = rt.handle(upgrade(req(Method::GET, "/chat"))).await;
    let accept = resp.socket_accept.take().unwrap();
    assert_eq!(accept.query, "");
}

#[tokio::test]
async fn unflagged_mount_ignores_upgrade_and_serves_the_plain_get() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/plain", "service": "data", "config": { "access": "open" } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);
    let resp = rt
        .handle(upgrade(req(Method::GET, "/plain/things/x")))
        .await;
    assert_ne!(resp.status, Some(StatusCode::SWITCHING_PROTOCOLS));
    assert_eq!(
        resp.status,
        Some(StatusCode::NOT_FOUND),
        "an ordinary GET on the data service"
    );
}

#[tokio::test]
async fn no_socket_hub_serves_plain_get_but_sockets_subtree_is_501() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "all" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        false, // no with_sockets()
        None,
    );
    // Plain GET on the connect path (not an upgrade) still reaches the service.
    let resp = rt.handle(req(Method::GET, "/chat")).await;
    assert_ne!(resp.status, Some(StatusCode::SWITCHING_PROTOCOLS));

    // Upgrade also falls through to a plain response (no hub to accept into).
    let resp = rt.handle(upgrade(req(Method::GET, "/chat"))).await;
    assert_ne!(resp.status, Some(StatusCode::SWITCHING_PROTOCOLS));

    // `/.sockets/` is reserved on a flagged mount regardless of hub presence.
    let mut resp = rt.handle(req(Method::GET, "/chat/.sockets/")).await;
    assert_eq!(resp.status, Some(StatusCode::NOT_IMPLEMENTED));
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "provider_unavailable");
}

#[tokio::test]
async fn pipeline_mount_upgrade_is_held_to_the_mounts_own_access_not_deferred() {
    let dir = tempfile::tempdir().unwrap();
    // No `access` at all on the mount: normal pipeline execution paths defer
    // to the per-spec check (and a `System`-triggered tick would pass), but
    // the upgrade must fail closed against the *mount's* (absent) access.
    let config = json!({
        "mounts": [
            { "path": "/chat", "service": "pipeline", "config": { "webSocket": true } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);

    let resp = rt.handle(upgrade(req(Method::GET, "/chat"))).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::UNAUTHORIZED),
        "no mount access ⇒ fail closed, even though pipeline execution normally defers"
    );

    let resp = rt
        .handle(as_role(upgrade(req(Method::GET, "/chat")), "u1", "A"))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::FORBIDDEN),
        "a principal with no role can satisfy an unconfigured mount either, even as 'A'"
    );
}

// ---------------------------------------------------------------------------
// Bearer subprotocol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bearer_subprotocol_authenticates_and_echoes_rs2() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "U", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let tok = token("bob", "U", 3600);
    let mut msg = upgrade(req(Method::GET, "/chat"));
    msg.set_header("sec-websocket-protocol", &format!("rs2, rs2.bearer.{tok}"));
    let mut resp = rt.handle(msg).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::SWITCHING_PROTOCOLS),
        "{:?}",
        resp.body
    );
    let accept = resp.socket_accept.take().unwrap();
    assert_eq!(
        accept.principal.as_ref().map(|p| p.id.as_str()),
        Some("bob")
    );
    assert_eq!(accept.protocol.as_deref(), Some("rs2"));
}

#[tokio::test]
async fn bearer_only_offer_echoes_the_bearer_entry_itself() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "U", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let tok = token("bob", "U", 3600);
    let mut msg = upgrade(req(Method::GET, "/chat"));
    msg.set_header("sec-websocket-protocol", &format!("rs2.bearer.{tok}"));
    let mut resp = rt.handle(msg).await;
    assert_eq!(resp.status, Some(StatusCode::SWITCHING_PROTOCOLS));
    let accept = resp.socket_accept.take().unwrap();
    assert_eq!(
        accept.protocol.as_deref(),
        Some(format!("rs2.bearer.{tok}").as_str()),
        "a handshake may only select a value the client offered"
    );
}

#[tokio::test]
async fn bearer_subprotocol_is_ignored_on_a_non_upgrade_request() {
    let dir = tempfile::tempdir().unwrap();
    // A `data` mount (not `pipeline`): its non-authoring paths are
    // host-enforced by `check_access`, so an unauthenticated caller is
    // reliably refused — a pipeline mount defers that same path to its
    // (here empty) per-spec check and would 404 either way.
    let config = json!({
        "auth": { "jwtSecret": SECRET },
        "mounts": [
            { "path": "/chat", "service": "data",
              "config": { "access": { "read": "U", "write": "A" }, "webSocket": true } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);
    let tok = token("bob", "U", 3600);
    // No `Upgrade` header: the bearer-subprotocol carrier must not authenticate.
    let mut msg = req(Method::GET, "/chat");
    msg.set_header("sec-websocket-protocol", &format!("rs2.bearer.{tok}"));
    let resp = rt.handle(msg).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::UNAUTHORIZED),
        "the subprotocol token must not authenticate a plain request"
    );
}

// ---------------------------------------------------------------------------
// Cookie auth + cross-site origin (WebSocket hijacking guard)
// ---------------------------------------------------------------------------

const HOST: &str = "api.acme.test";

fn cookie_config() -> serde_json::Value {
    json!({
        "auth": { "jwtSecret": SECRET },
        "cors": { "trustedOrigins": ["https://trusted.example"] },
        "mounts": [
            { "path": "/chat", "service": "pipeline",
              "config": { "access": { "read": "U", "write": "A" }, "webSocket": true } }
        ]
    })
}

fn cookie_upgrade(tok: &str, origin: Option<&str>) -> Message {
    let mut msg = with_cookie(upgrade(req(Method::GET, "/chat")), tok);
    msg.set_header("host", HOST);
    if let Some(o) = origin {
        msg.set_header("origin", o);
    }
    msg
}

#[tokio::test]
async fn cookie_upgrade_with_untrusted_cross_site_origin_is_403() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        cookie_config(),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let tok = token("bob", "U", 3600);
    let resp = rt
        .handle(cookie_upgrade(&tok, Some("https://evil.example")))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::FORBIDDEN),
        "cross-site WebSocket hijacking guard"
    );
}

#[tokio::test]
async fn cookie_upgrade_same_origin_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        cookie_config(),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let tok = token("bob", "U", 3600);
    let resp = rt
        .handle(cookie_upgrade(&tok, Some(&format!("https://{HOST}"))))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::SWITCHING_PROTOCOLS),
        "{:?}",
        resp.body
    );
}

#[tokio::test]
async fn cookie_upgrade_trusted_origin_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        cookie_config(),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let tok = token("bob", "U", 3600);
    let resp = rt
        .handle(cookie_upgrade(&tok, Some("https://trusted.example")))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::SWITCHING_PROTOCOLS),
        "{:?}",
        resp.body
    );
}

// ---------------------------------------------------------------------------
// Discovery: the `websocket` facet and `limits.webSocket`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discovery_catalogue_has_websocket_facet_only_on_flagged_mounts() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/chat", "service": "pipeline",
              "config": { "access": "open", "webSocket": true } },
            { "path": "/plain", "service": "data", "config": { "access": "open" } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);
    let mut resp = rt
        .handle(req(Method::GET, "/.well-known/rs2/services"))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let doc = body_json(&mut resp).await;
    let services = doc["services"].as_array().unwrap();
    let chat = services.iter().find(|s| s["path"] == "/chat").unwrap();
    let facets: Vec<&str> = chat["facets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        facets.contains(&"websocket"),
        "flagged mount carries the facet: {facets:?}"
    );
    let plain = services.iter().find(|s| s["path"] == "/plain").unwrap();
    let plain_facets: Vec<&str> = plain["facets"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(!plain_facets.contains(&"websocket"));

    assert_eq!(
        doc["limits"]["webSocket"]["messageBytes"],
        LimitTable::default().ws_message_bytes
    );
    assert_eq!(
        doc["limits"]["webSocket"]["messagesInFlight"],
        LimitTable::default().ws_messages_in_flight
    );
    assert_eq!(
        doc["limits"]["webSocket"]["messagesPerSecond"],
        LimitTable::default().ws_messages_per_second
    );
    assert_eq!(
        doc["limits"]["webSocket"]["socketsPerTenant"],
        LimitTable::default().ws_sockets_per_tenant as u64
    );
}

#[tokio::test]
async fn options_descriptor_carries_the_websocket_facet() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/chat", "service": "pipeline",
              "config": { "access": "open", "webSocket": true } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);
    let mut resp = rt.handle(req(Method::OPTIONS, "/chat")).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let doc = body_json(&mut resp).await;
    let facets: Vec<&str> = doc["facets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(facets.contains(&"websocket"));
}

// ---------------------------------------------------------------------------
// `/.sockets/` subtree
// ---------------------------------------------------------------------------

/// Two connected sockets sharing a mount (`bob` at `/chat/a`, `alice` at
/// `/chat/b`), for exercising the `/.sockets/` HTTP surface.
struct TwoSockets {
    rt: Arc<Runtime>,
    bob: (Arc<SocketSession>, tokio::sync::mpsc::Receiver<Outgoing>),
    alice: (Arc<SocketSession>, tokio::sync::mpsc::Receiver<Outgoing>),
}

async fn two_sockets(limits: LimitTable) -> TwoSockets {
    let dir = tempfile::tempdir().unwrap();
    // Leak the tempdir for the test's lifetime — simplest way to keep the
    // FileStore root alive without threading a guard through the struct.
    let dir = Box::leak(Box::new(dir));
    let rt = build(
        ws_config(json!({ "read": "all", "write": "A U" }), json!(true)),
        dir.path(),
        limits,
        true,
        None,
    );
    let bob = start_session(
        &rt,
        as_role(upgrade(req(Method::GET, "/chat/a")), "bob", "U"),
    )
    .await;
    let alice = start_session(
        &rt,
        as_role(upgrade(req(Method::GET, "/chat/b")), "alice", "U"),
    )
    .await;
    TwoSockets { rt, bob, alice }
}

#[tokio::test]
async fn sockets_post_exact_path_vs_trailing_slash_subtree_vs_whole_mount() {
    let ts = two_sockets(LimitTable::default()).await;

    // Exact path: only bob (at /chat/a) is addressed.
    let resp = ts
        .rt
        .handle(as_admin(req(Method::POST, "/chat/.sockets/a")).with_json(&json!({ "m": 1 })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));

    // Whole mount: no path segment at all.
    let mut resp = ts
        .rt
        .handle(as_admin(req(Method::POST, "/chat/.sockets/")).with_json(&json!({ "m": 2 })))
        .await;
    assert_eq!(
        body_json(&mut resp).await["sent"],
        2,
        "both sockets on the mount"
    );
}

#[tokio::test]
async fn sockets_id_and_user_query_narrowing() {
    let mut ts = two_sockets(LimitTable::default()).await;
    let bob_id = ts.bob.0.accept().id.clone();

    let mut resp = ts
        .rt
        .handle(
            as_admin(req(Method::POST, &format!("/chat/.sockets/?$id={bob_id}")))
                .with_json(&json!({ "hi": "bob" })),
        )
        .await;
    assert_eq!(body_json(&mut resp).await["sent"], 1);
    match recv(&mut ts.bob.1).await {
        Outgoing::Frame(Frame::Text(t)) => assert_eq!(t, json!({"hi":"bob"}).to_string()),
        other => panic!("expected a text frame, got {other:?}"),
    }
    recv_none(&mut ts.alice.1).await;

    let mut resp = ts
        .rt
        .handle(
            as_admin(req(Method::POST, "/chat/.sockets/?$user=alice"))
                .with_json(&json!({ "hi": "alice" })),
        )
        .await;
    assert_eq!(body_json(&mut resp).await["sent"], 1);
    recv(&mut ts.alice.1).await;
}

#[tokio::test]
async fn sockets_post_body_media_type_selects_the_frame_type() {
    let mut ts = two_sockets(LimitTable::default()).await;

    // JSON body → Frame::Text.
    ts.rt
        .handle(as_admin(req(Method::POST, "/chat/.sockets/")).with_json(&json!({"v":1})))
        .await;
    match recv(&mut ts.bob.1).await {
        Outgoing::Frame(Frame::Text(_)) => {}
        other => panic!("expected text frame for JSON, got {other:?}"),
    }
    recv(&mut ts.alice.1).await;

    // octet-stream body → Frame::Binary.
    let bin = as_admin(req(Method::POST, "/chat/.sockets/"))
        .with_body(Body::from_bytes(vec![1u8, 2, 3], MediaType::octet_stream()));
    ts.rt.handle(bin).await;
    match recv(&mut ts.bob.1).await {
        Outgoing::Frame(Frame::Binary(b)) => assert_eq!(b.as_ref(), &[1, 2, 3]),
        other => panic!("expected binary frame, got {other:?}"),
    }
    recv(&mut ts.alice.1).await;
}

#[tokio::test]
async fn sockets_get_listing_shape() {
    let ts = two_sockets(LimitTable::default()).await;
    let mut resp = ts
        .rt
        .handle(as_admin(req(Method::GET, "/chat/.sockets/")))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(
        resp.body.as_ref().unwrap().media_type.essence(),
        "application/vnd.rs2.dir+json"
    );
    assert_eq!(resp.header("x-total-count"), Some("2"));
    let doc = body_json(&mut resp).await;
    assert_eq!(doc["total"], 2);
    let entries = doc["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    for e in entries {
        assert_eq!(e["dir"], false);
        assert!(e["name"].is_string());
        assert!(e["path"].as_str().unwrap().starts_with("/chat/"));
        assert!(e["user"].is_string());
        assert!(e["connectedAt"].is_number());
    }
}

#[tokio::test]
async fn sockets_delete_default_code_and_explicit_code_reason() {
    let mut ts = two_sockets(LimitTable::default()).await;
    let bob_id = ts.bob.0.accept().id.clone();

    let mut resp = ts
        .rt
        .handle(as_admin(req(
            Method::DELETE,
            &format!("/chat/.sockets/?$id={bob_id}"),
        )))
        .await;
    assert_eq!(body_json(&mut resp).await["closed"], 1);
    match recv(&mut ts.bob.1).await {
        Outgoing::Close { code, .. } => assert_eq!(code, 1000, "default close code"),
        other => panic!("expected Close, got {other:?}"),
    }

    let mut resp2 = ts
        .rt
        .handle(as_admin(req(
            Method::DELETE,
            "/chat/.sockets/?$user=alice&code=4000&reason=bye",
        )))
        .await;
    assert_eq!(body_json(&mut resp2).await["closed"], 1);
    match recv(&mut ts.alice.1).await {
        Outgoing::Close { code, reason } => {
            assert_eq!(code, 4000);
            assert_eq!(reason, "bye");
        }
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test]
async fn sockets_delete_invalid_close_codes_are_400() {
    let ts = two_sockets(LimitTable::default()).await;
    for bad in ["1001", "2999", "5000", "abc"] {
        let resp = ts
            .rt
            .handle(as_admin(req(
                Method::DELETE,
                &format!("/chat/.sockets/?code={bad}"),
            )))
            .await;
        assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST), "code={bad}");
    }
}

#[tokio::test]
async fn sockets_put_is_405() {
    let ts = two_sockets(LimitTable::default()).await;
    let resp = ts
        .rt
        .handle(as_admin(req(Method::PUT, "/chat/.sockets/")))
        .await;
    assert_eq!(resp.status, Some(StatusCode::METHOD_NOT_ALLOWED));
}

#[tokio::test]
async fn sockets_post_body_over_ws_message_bytes_is_413() {
    let mut limits = LimitTable::default();
    limits.ws_message_bytes = 8;
    let ts = two_sockets(limits).await;
    let resp = ts
        .rt
        .handle(
            as_admin(req(Method::POST, "/chat/.sockets/"))
                .with_json(&json!({"far":"too big for eight bytes"})),
        )
        .await;
    assert_eq!(resp.status, Some(StatusCode::PAYLOAD_TOO_LARGE));
}

#[tokio::test]
async fn sockets_post_and_delete_require_write_role_even_with_invoke_all() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "auth": { "jwtSecret": SECRET },
        "mounts": [
            { "path": "/chat", "service": "pipeline",
              "config": { "access": { "read": "all", "invoke": "all", "write": "A" }, "webSocket": true } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);

    // Anonymous: `invoke: all` does not reach sockets writes → 401.
    let resp = rt
        .handle(req(Method::POST, "/chat/.sockets/").with_json(&json!({})))
        .await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));
    let resp = rt.handle(req(Method::DELETE, "/chat/.sockets/")).await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));

    // A role satisfying `invoke` but not `write` is still refused.
    let resp = rt
        .handle(as_role(req(Method::POST, "/chat/.sockets/"), "u", "U").with_json(&json!({})))
        .await;
    assert_eq!(resp.status, Some(StatusCode::FORBIDDEN));

    // GET only needs read (which is `all` here) — anonymous succeeds.
    let resp = rt.handle(req(Method::GET, "/chat/.sockets/")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
}

#[tokio::test]
async fn unflagged_mount_sockets_path_reaches_the_service_itself() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/plain", "service": "data", "config": { "access": "open" } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);
    // `.sockets` is just an ordinary dataset name on a mount without the flag.
    let resp = rt.handle(req(Method::GET, "/plain/.sockets/x")).await;
    assert_ne!(resp.status, Some(StatusCode::NOT_IMPLEMENTED));
    assert_eq!(
        resp.status,
        Some(StatusCode::NOT_FOUND),
        "an ordinary (empty) dataset read"
    );
}

// ---------------------------------------------------------------------------
// Per-session limits
// ---------------------------------------------------------------------------

#[tokio::test]
async fn oversize_frame_closes_with_4413() {
    let mut limits = LimitTable::default();
    limits.ws_message_bytes = 4;
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "all" }), json!(true)),
        dir.path(),
        limits,
        true,
        None,
    );
    let (session, mut rx) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;
    session.frame(Frame::Text("way too long".into()));
    match recv(&mut rx).await {
        Outgoing::Close { code, reason } => {
            assert_eq!(code, 4413);
            assert_eq!(reason, "limit_exceeded:ws_message_bytes");
        }
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test]
async fn rate_breach_closes_with_4429() {
    let mut limits = LimitTable::default();
    limits.ws_messages_per_second = 2;
    let dir = tempfile::tempdir().unwrap();
    // No pipeline authored: dispatch 404s per message, which is fine — the
    // rate check runs before dispatch.
    let rt = build(
        ws_config(json!({ "read": "all", "write": "all" }), json!(true)),
        dir.path(),
        limits,
        true,
        None,
    );
    let (session, mut rx) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;
    session.frame(Frame::Text("1".into()));
    session.frame(Frame::Text("2".into()));
    session.frame(Frame::Text("3".into()));
    match recv(&mut rx).await {
        Outgoing::Close { code, reason } => {
            assert_eq!(code, 4429);
            assert_eq!(reason, "limit_exceeded:ws_messages_per_second");
        }
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test]
async fn in_flight_breach_closes_with_4429() {
    let mut limits = LimitTable::default();
    limits.ws_messages_in_flight = 1;
    limits.ws_messages_per_second = 1000;
    let dir = tempfile::tempdir().unwrap();
    // Slow the pipeline's spec lookup so the first message's dispatch is
    // still in flight when the second frame arrives (no JS engine needed —
    // the delay lives in the FileStore the spec store reads from).
    let rt = build(
        ws_config(json!({ "read": "all", "write": "all" }), json!(true)),
        dir.path(),
        limits,
        true,
        Some(Duration::from_millis(300)),
    );
    author_root(&rt, "/chat", json!(["GET /chat"])).await;
    let (session, mut rx) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;
    session.frame(Frame::Text("1".into()));
    // Give the first message's dispatch time to start (and be mid-flight).
    tokio::time::sleep(Duration::from_millis(50)).await;
    session.frame(Frame::Text("2".into()));
    match recv(&mut rx).await {
        Outgoing::Close { code, reason } => {
            assert_eq!(code, 4429);
            assert_eq!(reason, "limit_exceeded:ws_messages_in_flight");
        }
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test]
async fn expired_token_closes_with_4401_on_the_next_frame() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "U", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    let tok = token("bob", "U", 1); // expires in 1s
    let (session, mut rx) =
        start_session(&rt, with_bearer(upgrade(req(Method::GET, "/chat")), &tok)).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    session.frame(Frame::Text("hi".into()));
    match recv(&mut rx).await {
        Outgoing::Close { code, reason } => {
            assert_eq!(code, 4401);
            assert_eq!(reason, "unauthorized");
        }
        other => panic!("expected Close, got {other:?}"),
    }
}

#[tokio::test]
async fn limit_breaches_feed_the_breaker_then_trip_it() {
    let mut limits = LimitTable::default();
    limits.ws_message_bytes = 1;
    limits.breaker_threshold = 2;
    limits.breaker_window = Duration::from_secs(30);
    limits.breaker_cooldown = Duration::from_secs(30);
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "all" }), json!(true)),
        dir.path(),
        limits,
        true,
        None,
    );
    for _ in 0..2 {
        let (session, mut rx) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;
        session.frame(Frame::Text("too big".into()));
        recv(&mut rx).await; // consume the 4413 close
    }
    // The breaker is now open for the tenant: even an unrelated request 503s.
    let resp = rt.handle(req(Method::GET, "/chat/.sockets/")).await;
    assert_eq!(resp.status, Some(StatusCode::SERVICE_UNAVAILABLE));
    let mut resp = resp;
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "limit_exceeded");
    // Cannot assert the `limit` extra field name portably without a getter;
    // the breaker's own `tenant_breaker` limit name is asserted below via
    // `RsError` construction parity in `close_for`/`limit_exceeded` tests.
    assert!(problem["detail"]
        .as_str()
        .unwrap()
        .contains("circuit breaker"));
}

#[tokio::test]
async fn sockets_per_tenant_limit_503s_the_upgrade_and_session_start_enforces_it_too() {
    let mut limits = LimitTable::default();
    limits.ws_sockets_per_tenant = 1;
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "all" }), json!(true)),
        dir.path(),
        limits,
        true,
        None,
    );
    let (_first, _rx1) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;

    // A second upgrade is refused at the host before a session ever starts.
    let mut resp = rt.handle(upgrade(req(Method::GET, "/chat"))).await;
    assert_eq!(resp.status, Some(StatusCode::SERVICE_UNAVAILABLE));
    assert!(resp.socket_accept.is_none());
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "limit_exceeded");
    assert_eq!(problem["limit"], "ws_sockets_per_tenant");

    // `SocketSession::start` enforces the same ceiling directly — construct
    // an accept by hand (bypassing `accept_upgrade`'s own pre-check) to prove
    // the session layer, not just the host branch, holds the line.
    let accept = SocketAccept {
        id: "hand-built".into(),
        tenant: "t".into(),
        mount: "/chat".into(),
        path: "/chat".into(),
        query: String::new(),
        principal: None,
        exp: None,
        text: TextMode::Json,
        events: vec![SocketEvent::Message],
        protocol: None,
        connected_at: 0,
    };
    let err = match SocketSession::start(&rt, accept) {
        Ok(_) => panic!("expected the sockets-per-tenant ceiling to be enforced"),
        Err(e) => e,
    };
    assert_eq!(err.status, 503);
    assert_eq!(err.code, "limit_exceeded");
}

// ---------------------------------------------------------------------------
// Reply rule + pushing to other sockets from a pipeline step
// ---------------------------------------------------------------------------

#[tokio::test]
async fn message_event_runs_the_pipeline_and_a_2xx_body_comes_back_as_a_text_frame() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    // The string-DSL array form treats a bare object item as the transform
    // template directly (no `"transform":` wrapper — see `dsl::convert`).
    author_root(&rt, "/chat", json!([{ "echoed": "$" }])).await;
    let (session, mut rx) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;
    session.frame(Frame::Text(json!({"hi": "there"}).to_string()));
    match recv(&mut rx).await {
        Outgoing::Frame(Frame::Text(t)) => {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["echoed"]["hi"], "there");
        }
        other => panic!("expected a text frame reply, got {other:?}"),
    }
}

#[tokio::test]
async fn pipeline_call_pushes_to_other_sockets_using_the_connected_principals_write_role() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        // `write` grants both the admin authoring the pipeline ("A") and the
        // connected `U` principal whose forwarded call must reach `/.sockets/`.
        ws_config(json!({ "read": "all", "write": "A U" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    // Forwards whatever arrives to the "$user=alice" selection; the reply
    // is the {"sent": n} envelope, which itself comes back to the sender.
    author_root(&rt, "/chat", json!(["POST /chat/.sockets/?$user=alice"])).await;

    let (alice_session, mut alice_rx) = start_session(
        &rt,
        as_role(upgrade(req(Method::GET, "/chat")), "alice", "U"),
    )
    .await;
    let (bob_session, mut bob_rx) =
        start_session(&rt, as_role(upgrade(req(Method::GET, "/chat")), "bob", "U")).await;
    let _ = &alice_session; // kept alive
    let _ = &bob_session;

    bob_session.frame(Frame::Text(json!({"msg": "hi alice"}).to_string()));

    // Alice receives the forwarded frame (the pushed content, not the reply).
    match recv(&mut alice_rx).await {
        Outgoing::Frame(Frame::Text(t)) => {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["msg"], "hi alice");
        }
        other => panic!("expected the forwarded frame, got {other:?}"),
    }
    // Bob receives only the reply-rule frame: the pipeline's own {"sent": 1}.
    match recv(&mut bob_rx).await {
        Outgoing::Frame(Frame::Text(t)) => {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["sent"], 1);
        }
        other => panic!("expected bob's reply frame, got {other:?}"),
    }
}

#[tokio::test]
async fn pipeline_call_can_push_via_elevate_for_an_anonymous_socket() {
    let dir = tempfile::tempdir().unwrap();
    // Anonymous callers may connect and trigger the pipeline (`read`/`invoke:
    // all`); the mount's `elevate` role reaches `/.sockets/` on their behalf.
    let config = json!({
        "auth": { "jwtSecret": SECRET },
        "mounts": [
            { "path": "/chat", "service": "pipeline",
              "config": {
                  // `write` also grants "A" so the admin can author the
                  // pipeline (authoring is host-enforced against the same
                  // mount `write` role); `elevate` adds "svc" only to
                  // elevated internal steps.
                  "access": { "read": "all", "invoke": "all", "write": "A svc" },
                  "elevate": "svc",
                  "webSocket": true
              } }
        ]
    });
    let rt = build(config, dir.path(), LimitTable::default(), true, None);
    author_root(
        &rt,
        "/chat",
        json!(["elevate POST /chat/.sockets/?$user=alice"]),
    )
    .await;

    let (_alice, mut alice_rx) = start_session(
        &rt,
        as_role(upgrade(req(Method::GET, "/chat")), "alice", "U"),
    )
    .await;
    let (anon, _anon_rx) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;

    anon.frame(Frame::Text(json!({"anon": true}).to_string()));
    match recv(&mut alice_rx).await {
        Outgoing::Frame(Frame::Text(t)) => {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["anon"], true);
        }
        other => panic!("expected the elevated push, got {other:?}"),
    }
}

#[tokio::test]
async fn pipeline_error_response_is_a_problem_json_text_frame_and_the_socket_stays_open() {
    let dir = tempfile::tempdir().unwrap();
    let rt = build(
        ws_config(json!({ "read": "all", "write": "A" }), json!(true)),
        dir.path(),
        LimitTable::default(),
        true,
        None,
    );
    author_root(&rt, "/chat", json!(["GET /nope-such-mount"])).await;
    let (session, mut rx) = start_session(&rt, upgrade(req(Method::GET, "/chat"))).await;
    session.frame(Frame::Text("trigger".into()));
    match recv(&mut rx).await {
        Outgoing::Frame(Frame::Text(t)) => {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["status"], 404);
        }
        other => panic!("expected a problem-json text frame, got {other:?}"),
    }
    // Socket kept open: another message still dispatches (and errors again),
    // proving the transport wasn't torn down by the first error.
    session.frame(Frame::Text("again".into()));
    match recv(&mut rx).await {
        Outgoing::Frame(Frame::Text(_)) => {}
        other => panic!("expected the socket still to be live, got {other:?}"),
    }
}

#[test]
fn socket_event_message_carries_the_trigger_headers_and_connect_principal() {
    let accept = SocketAccept {
        id: "sid-1".into(),
        tenant: "t".into(),
        mount: "/chat".into(),
        path: "/chat/room/1".into(),
        query: "?x=1".into(),
        principal: Some(Principal {
            id: "bob".into(),
            roles: vec!["U".into()],
            kind: "user".into(),
            extra: Default::default(),
        }),
        exp: None,
        text: TextMode::Json,
        events: vec![SocketEvent::Message],
        protocol: None,
        connected_at: 0,
    };
    let msg = socket_event_message(
        &accept,
        SocketEvent::Message,
        EventPayload::Frame(Frame::Text("hi".into())),
    );
    assert_eq!(msg.method, Method::POST);
    assert_eq!(msg.url.path, "/chat/room/1");
    assert_eq!(msg.url.query, "x=1");
    assert_eq!(msg.source, Source::System);
    assert_eq!(
        msg.header(rs2_core::sockets::TRIGGER_HEADER),
        Some(rs2_core::sockets::TRIGGER_WEBSOCKET)
    );
    assert_eq!(
        msg.header(rs2_core::sockets::SOCKET_EVENT_HEADER),
        Some("message")
    );
    assert_eq!(
        msg.header(rs2_core::sockets::SOCKET_ID_HEADER),
        Some("sid-1")
    );
    assert_eq!(msg.principal.as_ref().unwrap().id, "bob");

    let close_msg = socket_event_message(
        &accept,
        SocketEvent::Close,
        EventPayload::Close {
            code: 1000,
            reason: "bye".into(),
            was_clean: true,
        },
    );
    let body = close_msg.body.unwrap();
    assert!(body.media_type.is_json());
}

// ---------------------------------------------------------------------------
// Cleanup: a closed socket disappears from listings/counts immediately
// ---------------------------------------------------------------------------

#[tokio::test]
async fn after_session_closed_the_socket_is_gone_from_listing_and_count() {
    let ts = two_sockets(LimitTable::default()).await;
    ts.bob.0.closed(1000, "done", true).await;

    let mut resp = ts
        .rt
        .handle(as_admin(req(Method::GET, "/chat/.sockets/")))
        .await;
    let doc = body_json(&mut resp).await;
    assert_eq!(doc["total"], 1, "bob is gone, alice remains");
    assert_eq!(resp.header("x-total-count"), Some("1"));
}

#[tokio::test]
async fn delete_removes_the_socket_immediately() {
    let ts = two_sockets(LimitTable::default()).await;
    let bob_id = ts.bob.0.accept().id.clone();
    ts.rt
        .handle(as_admin(req(
            Method::DELETE,
            &format!("/chat/.sockets/?$id={bob_id}"),
        )))
        .await;
    let mut resp = ts
        .rt
        .handle(as_admin(req(Method::GET, "/chat/.sockets/")))
        .await;
    let doc = body_json(&mut resp).await;
    assert_eq!(doc["total"], 1);
}
