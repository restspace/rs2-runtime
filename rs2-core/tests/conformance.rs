//! Engine-neutral conformance suite (PRD §5.4, §11): pins down the contract
//! invariants every engine binding must satisfy. It runs unconditionally
//! against the native reference engine; the Wasm engine runs the same checks
//! when built with `--features wasm` and pointed at a component via the
//! `RS2_CONFORMANCE_COMPONENT` env var; the V8 engine joins when implemented.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::contract::{
    CapabilityTarget, Engine, GrantedHost, HostApi, InvocationLimits, ServiceCode,
};
use rs2_core::engines::NativeEngine;
use rs2_core::error::codes;
use rs2_core::message::{Body, MediaType, Message};

fn echo_service() -> ServiceCode {
    ServiceCode::Native(Arc::new(|mut msg: Message, config, _host| {
        Box::pin(async move {
            let body_text = match &mut msg.body {
                Some(b) => String::from_utf8_lossy(b.materialize(1024 * 1024).await?).to_string(),
                None => String::new(),
            };
            let out = json!({
                "method": msg.method.as_str(),
                "path": msg.url.path,
                "body": body_text,
                "config": config,
            });
            Ok(msg.ok_json(&out))
        })
    }))
}

fn limits() -> InvocationLimits {
    InvocationLimits {
        wall_clock: Duration::from_millis(500),
        memory_bytes: 16 * 1024 * 1024,
        outbound_calls: 2,
        materialized_body_bytes: 1024 * 1024,
    }
}

/// Invariant: message semantics — method, url, body, and config reach the
/// service; its response comes back with status and typed body intact.
#[tokio::test]
async fn message_semantics_round_trip() {
    let engine = NativeEngine::new();
    let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("echo"));
    let msg = Message::request(Method::POST, "/echo/x?y=1", "t1")
        .with_body(Body::from_string("hello", MediaType::new("text/plain")));
    let mut resp = engine
        .invoke(&echo_service(), msg, &json!({"k": "v"}), host, &limits())
        .await
        .unwrap();
    assert_eq!(resp.status, Some(StatusCode::OK));
    let body = resp.body.as_mut().unwrap();
    assert!(body.media_type.is_json());
    let v = body.as_json(1024 * 1024).await.unwrap();
    assert_eq!(v["method"], "POST");
    assert_eq!(v["body"], "hello");
    assert_eq!(v["config"]["k"], "v");
}

/// Invariant 2: an ungranted capability name fails with `capability_denied`.
#[tokio::test]
async fn ungranted_capability_is_denied() {
    let engine = NativeEngine::new();
    let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("svc"));
    let code = ServiceCode::Native(Arc::new(|msg: Message, _config, host: Arc<dyn HostApi>| {
        Box::pin(async move {
            let call = Message::request(Method::GET, "/anything", &msg.tenant);
            host.request("not-granted", call).await
        })
    }));
    let err = engine
        .invoke(&code, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &limits())
        .await
        .unwrap_err();
    assert_eq!(err.code, codes::CAPABILITY_DENIED);
}

/// Invariant 4 + §9.3: wall-clock breach kills the invocation with a
/// structured `limit_exceeded` error.
#[tokio::test]
async fn wall_clock_limit_kills_invocation() {
    let engine = NativeEngine::new();
    let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("spin"));
    let code = ServiceCode::Native(Arc::new(|msg: Message, _config, _host| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(msg.ok(None))
        })
    }));
    let err = engine
        .invoke(&code, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &limits())
        .await
        .unwrap_err();
    assert_eq!(err.code, codes::LIMIT_EXCEEDED);
    assert!(err.retryable);
}

/// §9.3: the outbound-call budget is enforced by the host, per invocation.
#[tokio::test]
async fn outbound_call_budget_is_enforced() {
    let engine = NativeEngine::new();
    let target: CapabilityTarget = Arc::new(|msg: Message| {
        Box::pin(async move { Ok(msg.ok(None)) })
    });
    let grants = HashMap::from([("api".to_string(), target)]);
    let host: Arc<dyn HostApi> = Arc::new(GrantedHost::new(
        grants,
        2,
        Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        "svc",
    ));
    let code = ServiceCode::Native(Arc::new(|msg: Message, _config, host: Arc<dyn HostApi>| {
        Box::pin(async move {
            for _ in 0..3 {
                host.request("api", Message::request(Method::GET, "/up", &msg.tenant)).await?;
            }
            Ok(msg.ok(None))
        })
    }));
    let err = engine
        .invoke(&code, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &limits())
        .await
        .unwrap_err();
    assert_eq!(err.code, codes::LIMIT_EXCEEDED);
}

/// Invariant 1: materialization is subject to host size limits.
#[tokio::test]
async fn body_materialization_respects_cap() {
    let engine = NativeEngine::new();
    let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("svc"));
    let code = ServiceCode::Native(Arc::new(|mut msg: Message, _config, _host| {
        Box::pin(async move {
            // Service asks for more than the host allows: must fail.
            let cap = 8;
            msg.body.as_mut().unwrap().materialize(cap).await?;
            Ok(msg.ok(None))
        })
    }));
    let msg = Message::request(Method::POST, "/x", "t1")
        .with_body(Body::from_string("0123456789abcdef", MediaType::new("text/plain")));
    let err = engine.invoke(&code, msg, &json!({}), host, &limits()).await.unwrap_err();
    assert_eq!(err.code, codes::LIMIT_EXCEEDED);
}

/// Invariant 4: no state survives an invocation except via the `state`
/// capability, which is keyed per service instance.
#[tokio::test]
async fn state_capability_persists_across_invocations() {
    let engine = NativeEngine::new();
    let state = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let host: Arc<dyn HostApi> =
        Arc::new(GrantedHost::new(HashMap::new(), 0, state.clone(), "counter"));
    let code = ServiceCode::Native(Arc::new(|msg: Message, _config, host: Arc<dyn HostApi>| {
        Box::pin(async move {
            let n = host
                .state_get("count")
                .await
                .and_then(|v| String::from_utf8(v).ok())
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0)
                + 1;
            host.state_put("count", n.to_string().into_bytes()).await;
            Ok(msg.ok_json(&json!({ "count": n })))
        })
    }));
    for expected in 1..=3 {
        let mut resp = engine
            .invoke(&code, Message::request(Method::GET, "/c", "t1"), &json!({}), host.clone(), &limits())
            .await
            .unwrap();
        let v = resp.body.as_mut().unwrap().as_json(1024).await.unwrap();
        assert_eq!(v["count"], expected);
    }
}

/// Wasm engine conformance: runs when a component implementing the WIT world
/// is supplied. Build one with `cargo component build` against
/// `rs2-core/wit/service.wit`, then:
/// `$env:RS2_CONFORMANCE_COMPONENT = "path\to\service.wasm"; cargo test --features wasm`
#[cfg(feature = "wasm")]
#[tokio::test]
async fn wasm_engine_message_semantics() {
    let Ok(path) = std::env::var("RS2_CONFORMANCE_COMPONENT") else {
        eprintln!("skipped: set RS2_CONFORMANCE_COMPONENT to a built conformance component");
        return;
    };
    let bytes = std::fs::read(&path).expect("component file readable");
    let engine = rs2_core::engines::wasm::WasmEngine::new().unwrap();
    let code = ServiceCode::WasmComponent(Arc::new(bytes));

    // Message semantics round-trip through the sandbox.
    let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("wasm-echo"));
    let msg = Message::request(Method::POST, "/echo", "t1")
        .with_body(Body::from_string("hello", MediaType::new("text/plain")));
    let mut resp = engine
        .invoke(&code, msg, &json!({"k": "v"}), host.clone(), &limits())
        .await
        .unwrap();
    assert_eq!(resp.status, Some(StatusCode::OK));
    assert_eq!(resp.header("x-engine"), Some("wasm"));
    let v = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(v["method"], "POST");
    assert_eq!(v["body"], "hello");
    assert_eq!(v["config"]["k"], "v");

    // Invariant 2 inside the sandbox: an ungranted capability surfaces as
    // capability-denied to the guest.
    let msg = Message::request(Method::GET, "/echo/deny-check", "t1");
    let mut resp = engine.invoke(&code, msg, &json!({}), host, &limits()).await.unwrap();
    let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
    assert_eq!(&bytes[..], b"denied-as-expected");
}
