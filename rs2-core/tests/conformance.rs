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

/// V8 engine conformance (G2: both languages pass the identical contract):
/// the same invariants the native and Wasm engines satisfy, exercised with
/// inline ES-module bundles. JS needs no build step, so these run fully
/// self-contained under `--features js`.
#[cfg(feature = "js")]
mod js_engine {
    use super::*;
    use rs2_core::engines::js::JsEngine;

    /// Invariant: message semantics round-trip, including config delivery,
    /// async handlers, and typed responses.
    #[tokio::test]
    async fn message_semantics_round_trip() {
        let engine = JsEngine::new();
        let code = ServiceCode::JsBundle(Arc::new(
            r#"
            export default {
                async handle(msg, ctx) {
                    return {
                        status: 200,
                        headers: { "x-engine": "js" },
                        body: {
                            method: msg.method,
                            url: msg.url,
                            body: msg.body,
                            config: ctx.config,
                        },
                    };
                }
            };
            "#
            .to_string(),
        ));
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("js-echo"));
        let msg = Message::request(Method::POST, "/echo/x?y=1", "t1")
            .with_body(Body::from_string("hello", MediaType::new("text/plain")));
        let mut resp = engine
            .invoke(&code, msg, &json!({"k": "v"}), host, &limits())
            .await
            .unwrap();
        assert_eq!(resp.status, Some(StatusCode::OK));
        assert_eq!(resp.header("x-engine"), Some("js"));
        let v = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
        assert_eq!(v["method"], "POST");
        assert_eq!(v["url"], "/echo/x?y=1");
        assert_eq!(v["body"], "hello");
        assert_eq!(v["config"]["k"], "v");
    }

    /// Invariant 2: an ungranted capability fails with `capability_denied` —
    /// catchable in the guest, and identity-preserving when uncaught.
    #[tokio::test]
    async fn ungranted_capability_is_denied() {
        let engine = JsEngine::new();
        // Uncaught: the structured error propagates out of the engine.
        let uncaught = ServiceCode::JsBundle(Arc::new(
            r#"
            export default async (msg, ctx) => {
                ctx.request("not-granted", { url: "/anything" });
                return { status: 200 };
            };
            "#
            .to_string(),
        ));
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("js-deny"));
        let err = engine
            .invoke(&uncaught, Message::request(Method::GET, "/x", "t1"), &json!({}), host.clone(), &limits())
            .await
            .unwrap_err();
        assert_eq!(err.code, codes::CAPABILITY_DENIED);

        // Caught: the guest sees a structured error with the code.
        let caught = ServiceCode::JsBundle(Arc::new(
            r#"
            export default async (msg, ctx) => {
                try {
                    ctx.request("not-granted", { url: "/anything" });
                    return { status: 500, body: "expected denial" };
                } catch (e) {
                    return { status: 200, body: e.code === "capability_denied"
                        ? "denied-as-expected" : `wrong code: ${e.code}` };
                }
            };
            "#
            .to_string(),
        ));
        let mut resp = engine
            .invoke(&caught, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &limits())
            .await
            .unwrap();
        let bytes = resp.body.as_mut().unwrap().materialize(1024).await.unwrap();
        assert_eq!(&bytes[..], b"denied-as-expected");
    }

    /// Invariant 4 + §9.3: an infinite loop is terminated at the wall-clock
    /// limit with a structured `limit_exceeded`.
    #[tokio::test]
    async fn wall_clock_limit_kills_invocation() {
        let engine = JsEngine::new();
        let code = ServiceCode::JsBundle(Arc::new(
            "export default () => { for (;;) {} };".to_string(),
        ));
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("js-spin"));
        let err = engine
            .invoke(&code, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &limits())
            .await
            .unwrap_err();
        assert_eq!(err.code, codes::LIMIT_EXCEEDED);
        assert!(err.retryable);
    }

    /// §9.3: unbounded allocation hits the isolate heap cap and is
    /// terminated with `limit_exceeded`, not a process abort.
    #[tokio::test]
    async fn memory_cap_kills_unbounded_allocation() {
        let engine = JsEngine::new();
        let code = ServiceCode::JsBundle(Arc::new(
            r#"
            export default () => {
                const hog = [];
                for (;;) { hog.push("x".repeat(1024 * 1024)); }
            };
            "#
            .to_string(),
        ));
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("js-hog"));
        let mut lim = limits();
        lim.wall_clock = Duration::from_secs(30); // memory, not time, must trip
        lim.memory_bytes = 64 * 1024 * 1024;
        let err = engine
            .invoke(&code, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &lim)
            .await
            .unwrap_err();
        assert_eq!(err.code, codes::LIMIT_EXCEEDED);
    }

    /// §9.3: the outbound-call budget applies identically inside JS.
    #[tokio::test]
    async fn outbound_call_budget_is_enforced() {
        let engine = JsEngine::new();
        let target: CapabilityTarget =
            Arc::new(|msg: Message| Box::pin(async move { Ok(msg.ok(None)) }));
        let grants = HashMap::from([("api".to_string(), target)]);
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::new(
            grants,
            2,
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            "js-budget",
        ));
        let code = ServiceCode::JsBundle(Arc::new(
            r#"
            export default (msg, ctx) => {
                for (let i = 0; i < 3; i++) {
                    ctx.request("api", { url: "/up" });
                }
                return { status: 200 };
            };
            "#
            .to_string(),
        ));
        let err = engine
            .invoke(&code, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &limits())
            .await
            .unwrap_err();
        assert_eq!(err.code, codes::LIMIT_EXCEEDED);
    }

    /// Invariant 4: no state survives an invocation except via the `state`
    /// capability — fresh isolates plus persistent host state.
    #[tokio::test]
    async fn state_capability_persists_across_invocations() {
        let engine = JsEngine::new();
        let state = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        let host: Arc<dyn HostApi> =
            Arc::new(GrantedHost::new(HashMap::new(), 0, state.clone(), "js-counter"));
        let code = ServiceCode::JsBundle(Arc::new(
            r#"
            globalThis.leak = (globalThis.leak ?? 0) + 1; // must NOT persist
            export default (msg, ctx) => {
                const n = parseInt(ctx.state.get("count") ?? "0", 10) + 1;
                ctx.state.put("count", String(n));
                return { status: 200, body: { count: n, leak: globalThis.leak } };
            };
            "#
            .to_string(),
        ));
        for expected in 1..=3 {
            let mut resp = engine
                .invoke(&code, Message::request(Method::GET, "/c", "t1"), &json!({}), host.clone(), &limits())
                .await
                .unwrap();
            let v = resp.body.as_mut().unwrap().as_json(1024).await.unwrap();
            assert_eq!(v["count"], expected, "state persists via the capability");
            assert_eq!(v["leak"], 1, "globals do not survive invocations");
        }
    }

    /// A capability round-trip carries response bodies back typed: JSON
    /// responses arrive parsed in the guest.
    #[tokio::test]
    async fn capability_responses_are_typed() {
        let engine = JsEngine::new();
        let target: CapabilityTarget = Arc::new(|msg: Message| {
            Box::pin(async move { Ok(msg.ok_json(&json!({ "answer": 42 }))) })
        });
        let grants = HashMap::from([("api".to_string(), target)]);
        let host: Arc<dyn HostApi> = Arc::new(GrantedHost::new(
            grants,
            8,
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            "js-typed",
        ));
        let code = ServiceCode::JsBundle(Arc::new(
            r#"
            export default async (msg, ctx) => {
                const resp = ctx.request("api", { method: "POST", url: "/calc",
                                                  body: { q: "life" } });
                return { status: 200, body: { got: resp.body.answer, status: resp.status } };
            };
            "#
            .to_string(),
        ));
        let mut resp = engine
            .invoke(&code, Message::request(Method::GET, "/x", "t1"), &json!({}), host, &limits())
            .await
            .unwrap();
        let v = resp.body.as_mut().unwrap().as_json(1024).await.unwrap();
        assert_eq!(v["got"], 42);
        assert_eq!(v["status"], 200);
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
