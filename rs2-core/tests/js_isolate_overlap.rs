//! Overlapping JS isolates (the V8 startup-snapshot abort).
//!
//! Booting an isolate from a custom startup snapshot while another isolate is
//! alive in the same process aborts inside V8's `SharedHeapDeserializer` — a
//! hardened-libc++ `vector[]` OOB that fastfails the process (`0xC0000409` on
//! Windows, SIGILL on Linux). `engines/js.rs` therefore keeps
//! `USE_PRELUDE_SNAPSHOT = false` on every platform.
//!
//! This test holds several isolates alive at once, which is exactly the
//! condition that trips it. It is the guard for the deno_core upgrade (G13
//! Phase 2): flip `USE_PRELUDE_SNAPSHOT` back on and run this. Note the failure
//! mode is a **process abort**, not an assertion — the suite dies rather than
//! reporting a failed test, so a clean run is the pass signal.
//!
//! Overlap is what matters, not load: with isolate lifetimes serialized the
//! same work never aborted (0/96 runs), while two overlapping isolates aborted
//! 26/60.
#![cfg(feature = "js")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::contract::{Engine, GrantedHost, HostApi, InvocationLimits, ServiceCode};
use rs2_core::engines::js::JsEngine;
use rs2_core::message::Message;

fn limits() -> InvocationLimits {
    InvocationLimits {
        wall_clock: Duration::from_secs(10),
        memory_bytes: 64 * 1024 * 1024,
        outbound_calls: 4,
        materialized_body_bytes: 1024 * 1024,
    }
}

fn host() -> Arc<dyn HostApi> {
    Arc::new(GrantedHost::new(
        HashMap::new(),
        4,
        Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        "isolate-overlap",
    ))
}

/// A bundle that does enough work to keep its isolate alive while its peers are
/// still building theirs — the window in which a snapshot-booted isolate aborts.
const BUNDLE: &str = r#"
    export default async (msg, ctx) => {
        let acc = 0;
        for (let i = 0; i < 200000; i++) acc = (acc + i) % 97;
        await Promise.resolve();
        return { ok: true, acc };
    };
"#;

async fn invoke_once() -> serde_json::Value {
    let engine = JsEngine::new();
    let code = ServiceCode::JsBundle(Arc::new(BUNDLE.to_string()));
    let mut resp = engine
        .invoke(
            &code,
            Message::request(Method::POST, "/svc", "t1"),
            &json!({}),
            host(),
            &limits(),
        )
        .await
        .expect("invocation");
    assert_eq!(resp.status, Some(StatusCode::OK));
    resp.body.as_mut().unwrap().as_json(65536).await.unwrap()
}

/// Several isolates built and running concurrently, repeatedly. Each round
/// starts its isolates together so their build phases overlap each other's live
/// isolates.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn concurrent_isolates_do_not_abort_the_process() {
    for _ in 0..4 {
        let results = futures::future::join_all((0..4).map(|_| invoke_once())).await;
        for out in results {
            assert_eq!(out["ok"], json!(true), "{out}");
        }
    }
}

/// The resident-adapter shape: one long-lived isolate overlapping every
/// short-lived one. `engines::resident` keeps an isolate for the life of the
/// process, so on a node with a loadable adapter mounted *every* invocation
/// meets the overlap condition — not just concurrent ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_lived_isolate_overlapping_short_lived_ones() {
    let long_running = tokio::spawn(async {
        // Kept deliberately slower than the peers below so its isolate stays
        // alive across their whole build/dispatch cycle.
        let engine = JsEngine::new();
        let code = ServiceCode::JsBundle(Arc::new(
            r#"export default async () => {
                   let acc = 0;
                   for (let i = 0; i < 3000000; i++) acc = (acc + i) % 97;
                   return { ok: true, acc };
               };"#
            .to_string(),
        ));
        engine
            .invoke(
                &code,
                Message::request(Method::POST, "/svc", "t1"),
                &json!({}),
                host(),
                &limits(),
            )
            .await
            .expect("long-running invocation")
            .status
    });

    for _ in 0..3 {
        let out = invoke_once().await;
        assert_eq!(out["ok"], json!(true), "{out}");
    }

    assert_eq!(long_running.await.unwrap(), Some(StatusCode::OK));
}
