//! G5 measurement: the real-SDK corpus. Bundles built from `corpus/`
//! (official npm packages, esbuild, same settings as `rs2 deploy --bundle`)
//! run in the JS engine against a mocked `fetch` capability. The G5 measure
//! is the fraction of the curated corpus that runs unmodified: ≥ 90%.
//!
//! Build the bundles first: `cd corpus; npm install; ./build.ps1`.
//! The test self-skips when the bundles are absent.

#![cfg(feature = "js")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::contract::{
    CapabilityTarget, Engine, GrantedHost, HostApi, InvocationLimits, ServiceCode,
};
use rs2_core::engines::js::JsEngine;
use rs2_core::message::Message;

/// Entries that cannot pass and why — part of the corpus record.
const KNOWN_BUILD_FAILURES: &[(&str, &str)] = &[(
    "slack",
    "@slack/web-api imports node:os/node:path (axios transport); needs node-builtin shims",
)];

fn limits() -> InvocationLimits {
    InvocationLimits {
        wall_clock: Duration::from_secs(15),
        memory_bytes: 256 * 1024 * 1024,
        outbound_calls: 16,
        materialized_body_bytes: 32 * 1024 * 1024,
    }
}

/// Canned upstream responses per mock host, shaped like the real APIs.
fn mock_response(msg: Message) -> Message {
    let url = msg.url.path.clone();
    let body = if url.contains("api.stripe.test") {
        json!({ "id": "cus_1", "object": "customer", "email": "ada@example.com" })
    } else if url.contains("api.openai.test") {
        json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1700000000,
            "model": "gpt-4o",
            "choices": [ { "index": 0, "finish_reason": "stop",
                           "message": { "role": "assistant", "content": "hi" } } ],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 }
        })
    } else if url.contains("api.anthropic.test") {
        json!({
            "id": "msg_1", "type": "message", "role": "assistant",
            "model": "claude-sonnet-4-6",
            "content": [ { "type": "text", "text": "hi" } ],
            "stop_reason": "end_turn", "stop_sequence": null,
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        })
    } else if url.contains("api.github.test") {
        json!({ "id": 1, "full_name": "octo/hello", "private": false })
    } else if url.contains("proj.supabase.test") {
        json!([ { "id": 1, "name": "first" } ])
    } else if url.contains("api.resend.test") {
        json!({ "id": "email_1" })
    } else if url.contains("generativelanguage.google.test") {
        json!({
            "candidates": [ { "index": 0, "finishReason": "STOP",
                "content": { "role": "model", "parts": [ { "text": "hi" } ] } } ],
            "usageMetadata": { "promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2 }
        })
    } else if url.contains("api.mistral.test") {
        json!({
            "id": "cmpl-1", "object": "chat.completion", "created": 1700000000,
            "model": "mistral-small-latest",
            "choices": [ { "index": 0, "finish_reason": "stop",
                           "message": { "role": "assistant", "content": "hi" } } ],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 }
        })
    } else if url.contains("api.groq.test") {
        json!({
            "id": "chatcmpl-1", "object": "chat.completion", "created": 1700000000,
            "model": "llama-3.3-70b-versatile",
            "choices": [ { "index": 0, "finish_reason": "stop",
                           "message": { "role": "assistant", "content": "hi" } } ],
            "usage": { "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2 }
        })
    } else {
        let mut resp = msg.response(StatusCode::NOT_FOUND, None);
        resp.set_header("content-type", "application/json");
        return resp;
    };
    let mut resp = msg.ok_json(&body);
    resp.set_header("request-id", "req_mock");
    resp.set_header("x-request-id", "req_mock");
    resp.set_header("content-range", "0-0/1"); // supabase pagination metadata
    resp
}

fn host() -> Arc<dyn HostApi> {
    let target: CapabilityTarget =
        Arc::new(move |msg: Message| Box::pin(async move { Ok(mock_response(msg)) }));
    Arc::new(GrantedHost::new(
        HashMap::from([("fetch".to_string(), target)]),
        16,
        Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        "sdk-corpus",
    ))
}

#[tokio::test]
async fn real_sdk_corpus_meets_g5() {
    let corpus = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../corpus");
    let entries_dir = corpus.join("entries");
    let bundles_dir = corpus.join("bundles");
    if !bundles_dir.exists() {
        eprintln!("skipping: build the corpus first (cd corpus; npm install; ./build.ps1)");
        return;
    }

    let entries: Vec<String> = std::fs::read_dir(&entries_dir)
        .expect("corpus entries dir")
        .filter_map(|e| {
            let name = e.ok()?.file_name().to_string_lossy().into_owned();
            name.strip_suffix(".mjs").map(String::from)
        })
        .collect();
    assert!(
        entries.len() >= 10,
        "curated corpus has {} entries",
        entries.len()
    );

    let engine = JsEngine::new();
    let mut passed = Vec::new();
    let mut failed = Vec::new();

    for name in &entries {
        let bundle_path = bundles_dir.join(format!("{name}.js"));
        let source = match std::fs::read_to_string(&bundle_path) {
            Ok(s) => s,
            Err(_) => {
                let reason = KNOWN_BUILD_FAILURES
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, r)| *r)
                    .unwrap_or("bundle missing (esbuild failed?)");
                failed.push((name.clone(), format!("build: {reason}")));
                continue;
            }
        };
        let code = ServiceCode::JsBundle(Arc::new(source));
        let started = std::time::Instant::now();
        let result = engine
            .invoke(
                &code,
                Message::request(Method::POST, "/run", "corpus"),
                &json!({}),
                host(),
                &limits(),
            )
            .await;
        match result {
            Ok(mut resp) if resp.status == Some(StatusCode::OK) => {
                let body = resp
                    .body
                    .as_mut()
                    .unwrap()
                    .as_json(1024 * 1024)
                    .await
                    .unwrap();
                assert_eq!(
                    body["sdk"].as_str(),
                    Some(name.as_str()),
                    "{name}: bundle identified itself: {body}"
                );
                println!("PASS {name:<10} {:>6.0?}  {body}", started.elapsed());
                passed.push(name.clone());
            }
            Ok(mut resp) => {
                let detail = match &mut resp.body {
                    Some(b) => String::from_utf8_lossy(
                        b.materialize(65536).await.unwrap_or(&Default::default()),
                    )
                    .into_owned(),
                    None => String::new(),
                };
                println!("FAIL {name:<10} status {:?}: {detail}", resp.status);
                failed.push((name.clone(), format!("status {:?}: {detail}", resp.status)));
            }
            Err(e) => {
                println!("FAIL {name:<10} {e}");
                failed.push((name.clone(), e.to_string()));
            }
        }
    }

    let rate = passed.len() as f64 / entries.len() as f64;
    println!(
        "\nG5 corpus: {}/{} SDKs ran unmodified ({:.0}%)",
        passed.len(),
        entries.len(),
        rate * 100.0
    );
    for (name, reason) in &failed {
        println!("  failed: {name} — {reason}");
    }
    // Every failure must be a documented known failure…
    for (name, _) in &failed {
        assert!(
            KNOWN_BUILD_FAILURES.iter().any(|(n, _)| n == name),
            "undocumented corpus failure: {name}"
        );
    }
    // …and the documented set must still clear the G5 bar.
    assert!(rate >= 0.9, "G5 requires ≥90%, got {:.0}%", rate * 100.0);
}
