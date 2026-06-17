//! M2 integration tests (PRD §16, "Composition & control"): a migrated demo
//! tenant runs end-to-end (pipelines + auth + self-config hot reload), the
//! G6 idempotency proof, and the G7 streaming pipeline benchmark.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Body, MediaType, Message, Source};
use rs2_core::pipeline::{Executor, PipelineLimits, PipelineSpec, Requester};
use rs2_core::retry::{Jitter, RetryPolicy};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::services::auth::hash_password;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// In-memory mutable config store: the self-config hot-reload seam.
struct MutableLoader(Mutex<serde_json::Value>);

impl MutableLoader {
    fn version(value: &serde_json::Value) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        value.to_string().hash(&mut h);
        format!("{:016x}", h.finish())
    }
}

#[async_trait]
impl ConfigLoader for MutableLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.lock().unwrap().clone())
            .map_err(|e| RsError::internal(e.to_string()))
    }

    async fn load_raw(&self, _tenant: &str) -> Result<(serde_json::Value, String), RsError> {
        let value = self.0.lock().unwrap().clone();
        let version = Self::version(&value);
        Ok((value, version))
    }

    async fn save_raw(
        &self,
        _tenant: &str,
        config: &serde_json::Value,
        expected_version: Option<&str>,
    ) -> Result<String, RsError> {
        let mut current = self.0.lock().unwrap();
        if let Some(expected) = expected_version {
            if Self::version(&current) != expected {
                return Err(RsError::conflict("config version mismatch"));
            }
        }
        *current = config.clone();
        Ok(Self::version(&current))
    }
}

/// The migrated Restspace demo tenant: files, schema'd data, auth, a
/// pipeline written in the terse string DSL, and the self-config surface.
fn demo_config() -> serde_json::Value {
    json!({
        "auth": { "jwtSecret": "demo-secret", "maxAttempts": 3, "lockMinutes": 1 },
        "mounts": [
            { "path": "/files", "service": "file" },
            { "path": "/data", "service": "data" },
            { "path": "/auth", "service": "auth" },
            { "path": "/admin", "service": "file",
              "config": { "access": { "read": "all", "write": "A" } } },
            { "path": "/order-summary", "service": "pipeline" },
            { "path": "/services", "service": "services",
              "config": { "access": { "read": "all", "write": "A" } } }
        ]
    })
}

fn demo_runtime(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(MutableLoader(Mutex::new(demo_config())));
    Runtime::new(Tenancy::Single { tenant: "demo".into() }, adapters, loader, LimitTable::default())
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "demo")
}

/// Author the demo pipeline (string DSL envelope) as the mount-root spec.
async fn author_order_summary(rt: &Runtime) {
    let envelope = json!({ "pipeline": [
        "GET /data/orders/${id} :$order",
        { "total": "$sum($order.lines.price)", "status": "$order.status" }
    ]});
    let put = req(Method::PUT, "/order-summary/.pipelines/.root").with_json(&envelope);
    let resp = rt.handle(put).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body.as_mut().expect("body").as_json(10 * 1024 * 1024).await.expect("json body")
}

// ---------------------------------------------------------------------------
// Demo tenant end-to-end (M2 exit criterion 1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demo_tenant_pipeline_runs_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let rt = demo_runtime(dir.path());

    // Seed an order through the data service.
    let seed = req(Method::PUT, "/data/orders/o1").with_json(&json!({
        "status": "open",
        "lines": [ { "price": 2.5 }, { "price": 4.5 } ]
    }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED));

    // Author the pipeline like a file (the .root spec governs the mount),
    // then plain GET executes it — verb passthrough intact.
    author_order_summary(&rt).await;
    let mut resp = rt.handle(req(Method::GET, "/order-summary?id=o1")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    let summary = body_json(&mut resp).await;
    assert_eq!(summary["total"].as_f64(), Some(7.0), "{summary}");
    assert_eq!(summary["status"], "open");

    // ?$plan introspection on the authoring path: the transform forces a
    // segment boundary, and the stored form is typed (DSL canonicalized).
    let mut plan_resp =
        rt.handle(req(Method::GET, "/order-summary/.pipelines/.root?$plan")).await;
    assert_eq!(plan_resp.status, Some(StatusCode::OK));
    let plan = body_json(&mut plan_resp).await;
    let segments = plan["plan"]["segments"].as_array().unwrap();
    assert_eq!(segments.len(), 2, "call | transform = two segments: {plan}");
    assert!(plan["pipeline"]["steps"][0]["call"].is_object(), "stored form is typed: {plan}");

    // The .root spec governs every subpath of the mount (wrap-the-mount):
    // arbitrary deeper paths still execute it, verb and URL intact.
    let wrapped = rt.handle(req(Method::GET, "/order-summary/any/deeper/path?id=o1")).await;
    assert_eq!(wrapped.status, Some(StatusCode::OK), "{:?}", wrapped.body);
}

#[tokio::test]
async fn auth_login_rbac_and_lockout() {
    let dir = tempfile::tempdir().unwrap();
    let rt = demo_runtime(dir.path());

    // Seed users: an admin and a plain user (argon2id hashes).
    let admin = req(Method::PUT, "/data/users/ada@demo.test").with_json(&json!({
        "passwordHash": hash_password("adapw").unwrap(),
        "roles": "A U"
    }));
    assert_eq!(rt.handle(admin).await.status, Some(StatusCode::CREATED));
    let user = req(Method::PUT, "/data/users/uma@demo.test").with_json(&json!({
        "passwordHash": hash_password("umapw").unwrap(),
        "roles": "U"
    }));
    assert_eq!(rt.handle(user).await.status, Some(StatusCode::CREATED));

    // Anonymous write to the admin mount → 401.
    let denied = req(Method::PUT, "/admin/notes.txt")
        .with_body(Body::from_string("x", MediaType::new("text/plain")));
    assert_eq!(rt.handle(denied).await.status, Some(StatusCode::UNAUTHORIZED));

    // Login as admin → JWT.
    let mut login = rt
        .handle(req(Method::POST, "/auth/login").with_json(&json!({
            "email": "ada@demo.test", "password": "adapw"
        })))
        .await;
    assert_eq!(login.status, Some(StatusCode::OK));
    assert!(login.header("set-cookie").unwrap().starts_with("rs-auth="));
    let token = body_json(&mut login).await["token"].as_str().unwrap().to_string();

    // Admin can write.
    let mut write = req(Method::PUT, "/admin/notes.txt")
        .with_body(Body::from_string("hello", MediaType::new("text/plain")));
    write.set_header("authorization", &format!("Bearer {token}"));
    assert_eq!(rt.handle(write).await.status, Some(StatusCode::CREATED));

    // The plain user lacks the A role → 403.
    let mut user_login = rt
        .handle(req(Method::POST, "/auth/login").with_json(&json!({
            "email": "uma@demo.test", "password": "umapw"
        })))
        .await;
    let user_token = body_json(&mut user_login).await["token"].as_str().unwrap().to_string();
    let mut forbidden = req(Method::PUT, "/admin/notes.txt")
        .with_body(Body::from_string("nope", MediaType::new("text/plain")));
    forbidden.set_header("authorization", &format!("Bearer {user_token}"));
    assert_eq!(rt.handle(forbidden).await.status, Some(StatusCode::FORBIDDEN));

    // Reads stay open ("read": "all").
    assert_eq!(
        rt.handle(req(Method::GET, "/admin/notes.txt")).await.status,
        Some(StatusCode::OK)
    );

    // GET /auth/user reflects the verified principal.
    let mut who = req(Method::GET, "/auth/user");
    who.set_header("authorization", &format!("Bearer {token}"));
    let mut who_resp = rt.handle(who).await;
    assert_eq!(body_json(&mut who_resp).await["id"], "ada@demo.test");

    // Lockout: maxAttempts(3) bad passwords lock the account...
    for _ in 0..3 {
        let resp = rt
            .handle(req(Method::POST, "/auth/login").with_json(&json!({
                "email": "uma@demo.test", "password": "wrong"
            })))
            .await;
        assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));
    }
    // ...so even the right password is refused while locked, with Retry-After.
    let locked = rt
        .handle(req(Method::POST, "/auth/login").with_json(&json!({
            "email": "uma@demo.test", "password": "umapw"
        })))
        .await;
    assert_eq!(locked.status, Some(StatusCode::UNAUTHORIZED));
    assert!(locked.header("retry-after").is_some(), "lockout advertises Retry-After");

    // A tampered token is rejected outright (401), not treated as anonymous.
    let mut tampered = req(Method::GET, "/admin/notes.txt");
    tampered.set_header("authorization", &format!("Bearer {token}x"));
    assert_eq!(rt.handle(tampered).await.status, Some(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn self_config_hot_reload_swaps_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let rt = demo_runtime(dir.path());

    // Config writes need the A role: seed an admin and log in.
    let admin = req(Method::PUT, "/data/users/ada@demo.test").with_json(&json!({
        "passwordHash": hash_password("adapw").unwrap(),
        "roles": "A"
    }));
    assert_eq!(rt.handle(admin).await.status, Some(StatusCode::CREATED));
    let mut login = rt
        .handle(req(Method::POST, "/auth/login").with_json(&json!({
            "email": "ada@demo.test", "password": "adapw"
        })))
        .await;
    let token = body_json(&mut login).await["token"].as_str().unwrap().to_string();
    let bearer = format!("Bearer {token}");

    // The new mount does not exist yet.
    assert_eq!(
        rt.handle(req(Method::GET, "/notes/x.txt")).await.status,
        Some(StatusCode::NOT_FOUND)
    );

    // Read current config + version. Secrets are write-only (PRD §9.2):
    // the signing key reads back masked.
    let mut raw = rt.handle(req(Method::GET, "/services/raw")).await;
    assert_eq!(raw.status, Some(StatusCode::OK));
    let etag = raw.header("etag").unwrap().trim_matches('"').to_string();
    let mut config = body_json(&mut raw).await;
    assert_eq!(config["auth"]["jwtSecret"], "<secret>", "secret never readable back");

    // An invalid config is rejected wholesale; the running tenant is intact.
    let mut broken = config.clone();
    broken["mounts"].as_array_mut().unwrap().push(json!({
        "path": "/broken", "service": "no-such-service"
    }));
    let mut put = req(Method::PUT, "/services/raw").with_json(&broken);
    put.set_header("authorization", &bearer);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::BAD_REQUEST));
    assert_eq!(
        rt.handle(req(Method::GET, "/order-summary/.pipelines/")).await.status,
        Some(StatusCode::OK),
        "running tenant untouched after invalid PUT"
    );

    // Add a valid mount with If-Match → 204 + new version; hot-swapped.
    config["mounts"].as_array_mut().unwrap().push(json!({
        "path": "/notes", "service": "file"
    }));
    let mut put = req(Method::PUT, "/services/raw").with_json(&config);
    put.set_header("if-match", &format!("\"{etag}\""));
    put.set_header("authorization", &bearer);
    let resp = rt.handle(put).await;
    assert_eq!(resp.status, Some(StatusCode::NO_CONTENT));
    let new_etag = resp.header("etag").unwrap().trim_matches('"').to_string();
    assert_ne!(etag, new_etag);

    let write = req(Method::PUT, "/notes/x.txt")
        .with_body(Body::from_string("note", MediaType::new("text/plain")));
    assert_eq!(rt.handle(write).await.status, Some(StatusCode::CREATED));

    // The masked secret round-tripped: the real signing key survived the
    // GET → edit → PUT cycle, so logins still verify after the hot swap.
    let relogin = rt
        .handle(req(Method::POST, "/auth/login").with_json(&json!({
            "email": "ada@demo.test", "password": "adapw"
        })))
        .await;
    assert_eq!(relogin.status, Some(StatusCode::OK), "secret preserved through round trip");

    // A stale If-Match now conflicts.
    let mut stale = req(Method::PUT, "/services/raw").with_json(&config);
    stale.set_header("if-match", &format!("\"{etag}\""));
    stale.set_header("authorization", &bearer);
    assert_eq!(rt.handle(stale).await.status, Some(StatusCode::CONFLICT));

    // Catalogue lists the built-ins.
    let mut cat = rt.handle(req(Method::GET, "/services/catalogue")).await;
    let names: Vec<String> = body_json(&mut cat).await["services"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"pipeline".to_string()) && names.contains(&"auth".to_string()));
}

// ---------------------------------------------------------------------------
// G6: idempotency proof
// ---------------------------------------------------------------------------

#[tokio::test]
async fn g6_idempotency_key_dedupes_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let rt = demo_runtime(dir.path());

    // First keyed create executes and stores the response.
    let mut create = req(Method::POST, "/data/widgets").with_json(&json!({ "n": 1 }));
    create.set_header("idempotency-key", "k-widget-1");
    let first = rt.handle(create).await;
    assert_eq!(first.status, Some(StatusCode::CREATED));
    let location = first.header("location").unwrap().to_string();
    assert!(first.header("idempotency-replayed").is_none());

    // The duplicate replays the stored response — same Location, no second
    // record — marked Idempotency-Replayed.
    let mut dup = req(Method::POST, "/data/widgets").with_json(&json!({ "n": 1 }));
    dup.set_header("idempotency-key", "k-widget-1");
    let replay = rt.handle(dup).await;
    assert_eq!(replay.status, Some(StatusCode::CREATED));
    assert_eq!(replay.header("location").unwrap(), location);
    assert_eq!(replay.header("idempotency-replayed").unwrap(), "true");

    // Exactly one widget exists.
    let mut list = rt.handle(req(Method::GET, "/data/widgets")).await;
    let listing = body_json(&mut list).await;
    assert_eq!(listing["total"], 1, "{listing}");

    // The same key with a different payload → 422 idempotency_key_reuse.
    let mut reuse = req(Method::POST, "/data/widgets").with_json(&json!({ "n": 2 }));
    reuse.set_header("idempotency-key", "k-widget-1");
    let mut conflict = rt.handle(reuse).await;
    assert_eq!(conflict.status, Some(StatusCode::UNPROCESSABLE_ENTITY));
    assert_eq!(body_json(&mut conflict).await["code"], "idempotency_key_reuse");

    // Keys are scoped per path: the same key elsewhere is fresh.
    let mut other = req(Method::POST, "/data/gadgets").with_json(&json!({ "n": 1 }));
    other.set_header("idempotency-key", "k-widget-1");
    assert_eq!(rt.handle(other).await.status, Some(StatusCode::CREATED));
}

/// The segment-retry half of G6 (PRD §7.3): a segment containing a keyed
/// call retries as a unit, and the keyed call dedupes across attempts —
/// exactly-once-effective without the author writing key plumbing.
#[tokio::test]
async fn g6_segment_retry_dedupes_keyed_effects() {
    #[derive(Default)]
    struct FlakyBackend {
        charges: Mutex<Vec<String>>, // idempotency keys of executed charges
        fails_remaining: Mutex<u32>,
    }

    struct MockRequester(Arc<FlakyBackend>);

    #[async_trait]
    impl Requester for MockRequester {
        async fn request(&self, msg: Message) -> Message {
            match msg.url.path.as_str() {
                "/payments/charge" => {
                    let key = msg.header("idempotency-key").expect("keyed call has a key").to_string();
                    let mut charges = self.0.charges.lock().unwrap();
                    if !charges.contains(&key) {
                        charges.push(key); // dedupe: same key executes once
                    }
                    msg.response(StatusCode::OK, None)
                }
                // The segment's last step fails transiently.
                "/notify" => {
                    let mut fails = self.0.fails_remaining.lock().unwrap();
                    if *fails > 0 {
                        *fails -= 1;
                        let err = RsError::limit_exceeded("synthetic", 1u64, 1u64);
                        return msg.error_response(&err); // 503, retryable
                    }
                    msg.response(StatusCode::OK, None)
                }
                other => panic!("unexpected call {other}"),
            }
        }
    }

    let backend = Arc::new(FlakyBackend::default());
    *backend.fails_remaining.lock().unwrap() = 2;

    // charge (keyed) then notify: one segment. The notify failures force
    // two whole-segment retries, re-running the charge each attempt.
    let spec: PipelineSpec = serde_json::from_value(json!({
        "onFail": "stop",
        "steps": [
            { "call": { "method": "POST", "url": "/payments/charge", "effect": "keyed" } },
            { "call": { "method": "POST", "url": "/notify", "effect": "keyed" } }
        ]
    }))
    .unwrap();

    let retry = RetryPolicy {
        max_attempts: 3,
        base_delay_ms: 1,
        max_delay_ms: 1,
        jitter: Jitter::None,
        ..Default::default()
    };
    let executor =
        Executor::new(Arc::new(MockRequester(backend.clone())), PipelineLimits::default(), retry);

    let mut input = Message::request(Method::POST, "/run", "demo").with_json(&json!({}));
    input.source = Source::Internal;
    let out = executor.run(&spec, input, None).await.unwrap();
    assert_eq!(out.status, Some(StatusCode::OK), "third attempt succeeds");

    // The charge endpoint was invoked three times (once per segment
    // attempt) but with the SAME derived key every time → one effect.
    let charges = backend.charges.lock().unwrap();
    assert_eq!(charges.len(), 1, "same key on every attempt: {charges:?}");
}

// ---------------------------------------------------------------------------
// G7: streaming pipeline benchmark (run with `cargo test -- --ignored`)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "benchmark: cargo test -p rs2-core --test m2_composition -- --ignored --nocapture"]
async fn g7_pipeline_dispatch_benchmark() {
    let dir = tempfile::tempdir().unwrap();
    let rt = demo_runtime(dir.path());

    let seed = req(Method::PUT, "/data/orders/o1").with_json(&json!({
        "status": "open",
        "lines": [ { "price": 1.0 } ]
    }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED));
    author_order_summary(&rt).await;

    // Warm the tenant + pipeline plan.
    let _ = rt.handle(req(Method::GET, "/order-summary?id=o1")).await;

    let n = 1000u32;
    let start = std::time::Instant::now();
    for _ in 0..n {
        let resp = rt.handle(req(Method::GET, "/order-summary?id=o1")).await;
        assert_eq!(resp.status, Some(StatusCode::OK));
    }
    let elapsed = start.elapsed();
    println!(
        "G7: {n} two-step pipeline invocations in {elapsed:?} ({:.2} ms/invocation)",
        elapsed.as_secs_f64() * 1000.0 / n as f64
    );
}

// ---------------------------------------------------------------------------
// G2: event fan-out — a trigger launches a pipeline (no runtime machinery
// beyond the pipeline service: any verb from any source executes a stored spec)
// ---------------------------------------------------------------------------

/// An unauthenticated external POST (a provider webhook) at a `pipeline` mount
/// runs its `.root` flow, which fans out to a downstream mount and acks the
/// provider — proving "trigger → pipeline" end-to-end with no new runtime code.
#[tokio::test]
async fn webhook_post_launches_a_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "mounts": [
            { "path": "/data", "service": "data" },
            // Open for the provider's unauthenticated POST.
            { "path": "/hooks", "service": "pipeline",
              "config": { "access": { "invoke": "all", "write": "all" } } }
        ]
    });
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(dir.path())), Arc::new(MemDataStore::new()));
    let loader = Arc::new(MutableLoader(Mutex::new(config)));
    let rt =
        Runtime::new(Tenancy::Single { tenant: "demo".into() }, adapters, loader, LimitTable::default());

    // The trigger flow: store the event downstream, then ack 200 to the provider.
    let root = json!({ "pipeline": [ "PUT /data/events/incoming", { "received": true } ] });
    let author = req(Method::PUT, "/hooks/.pipelines/.root").with_json(&root);
    assert_eq!(rt.handle(author).await.status, Some(StatusCode::CREATED), "author .root");

    // A real external webhook POST (default Source::External, no principal).
    let hook = req(Method::POST, "/hooks/stripe").with_json(&json!({ "id": "evt_1", "amount": 100 }));
    assert_eq!(hook.source, Source::External, "a webhook is an external request");
    let mut resp = rt.handle(hook).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    assert_eq!(body_json(&mut resp).await["received"], true, "the pipeline acked");

    // The flow fanned out: the event was stored downstream by the pipeline.
    let mut stored = rt.handle(req(Method::GET, "/data/events/incoming")).await;
    assert_eq!(stored.status, Some(StatusCode::OK));
    let event = body_json(&mut stored).await;
    assert_eq!(event["id"], "evt_1");
    assert_eq!(event["amount"], 100);
}

/// G3: a webhook→pipeline flow verifies the request's HMAC signature inline
/// (`$hmacVerify` over `$_rawBody`, secret bound host-side) — valid passes and
/// fans out; a wrong signature is gated to a 4xx, with the secret never exposed.
#[tokio::test]
async fn webhook_hmac_signature_is_verified_inline() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    const KEY: &str = "whsec_test_key";

    let dir = tempfile::tempdir().unwrap();
    let config = json!({
        "secrets": { "wh": KEY },
        "mounts": [
            { "path": "/data", "service": "data" },
            { "path": "/hooks", "service": "pipeline",
              "config": { "access": { "invoke": "all", "write": "all" },
                          "secrets": ["wh"] } }
        ]
    });
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(dir.path())), Arc::new(MemDataStore::new()));
    let rt = Runtime::new(
        Tenancy::Single { tenant: "demo".into() },
        adapters,
        Arc::new(MutableLoader(Mutex::new(config))),
        LimitTable::default(),
    );

    // Gate on the signature over the raw body, then fan out. Typed steps: a
    // bare-string DSL step is a call, so the whole-body gate uses `transform`.
    let root = json!({ "pipeline": { "steps": [
        { "transform": "$hmacVerify('sha256', $wh, $_rawBody, $_headers.\"x-signature\") ? $ : $error('invalid signature')" },
        { "call": { "method": "PUT", "url": "/data/events/incoming" } },
        { "transform": { "received": true } }
    ] } });
    assert_eq!(
        rt.handle(req(Method::PUT, "/hooks/.pipelines/.root").with_json(&root)).await.status,
        Some(StatusCode::CREATED),
        "author .root"
    );

    // The exact bytes the provider signs are the serialized body.
    let body = json!({ "id": "evt_1", "amount": 100 });
    let raw = serde_json::to_vec(&body).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(&raw);
    let sig: String = mac.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect();

    // Valid signature → the flow runs and stores the event.
    let mut valid = req(Method::POST, "/hooks/stripe").with_json(&body);
    valid.set_header("x-signature", &sig);
    let mut resp = rt.handle(valid).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    assert_eq!(body_json(&mut resp).await["received"], true);
    let mut stored = rt.handle(req(Method::GET, "/data/events/incoming")).await;
    assert_eq!(stored.status, Some(StatusCode::OK));
    assert_eq!(body_json(&mut stored).await["id"], "evt_1");

    // Wrong signature → gated before any downstream effect.
    let mut forged = req(Method::POST, "/hooks/stripe").with_json(&body);
    forged.set_header("x-signature", &"0".repeat(64));
    assert_eq!(rt.handle(forged).await.status, Some(StatusCode::BAD_REQUEST), "forged signature rejected");
}
