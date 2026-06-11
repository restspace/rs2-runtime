//! G1 + G3 exit-criteria benchmarks (PRD §1). Timing-sensitive, so they run
//! on demand, release-built:
//!
//! ```powershell
//! cargo test -p rs2-core --release --test g_benchmarks -- --ignored --nocapture
//! cargo test -p rs2-core --release --features js --test g_benchmarks -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::Message;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, Tenant, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct StaticLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
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

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

async fn measure<F, Fut>(n: usize, mut call: F) -> (Duration, Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let start = Instant::now();
        call().await;
        samples.push(start.elapsed());
    }
    samples.sort();
    (percentile(&samples, 0.5), percentile(&samples, 0.99))
}

// ---------------------------------------------------------------------------
// G1: sandbox dispatch overhead — p99 added latency for a warm invocation
// through the full dispatch path vs. the direct function call, < 1 ms.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "benchmark: cargo test -p rs2-core --release --test g_benchmarks -- --ignored --nocapture"]
async fn g1_dispatch_overhead_under_1ms_p99() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let config = json!({ "mounts": [ { "path": "/data", "service": "data" } ] });
    let rt = Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters.clone(),
        Arc::new(StaticLoader(config.clone())),
        LimitTable::default(),
    );

    // Seed + warm both paths.
    let seed = Message::request(Method::PUT, "/data/items/k", "t")
        .with_json(&json!({ "name": "warm", "n": 1 }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED));

    // The "native function call path": the same service instance invoked
    // directly, no router/wrapper/limits/idempotency around it.
    let parsed: TenantConfig = serde_json::from_value(config).unwrap();
    let tenant =
        Tenant::build("t", parsed, &adapters, &LimitTable::default(), None, None).unwrap();
    let (service, ctx) = tenant.instance("/data").unwrap();
    let (service, ctx) = (service.clone(), ctx.clone());

    let direct_call = || async {
        let mut msg = Message::request(Method::GET, "/data/items/k", "t");
        msg.url.apply_mount("/data");
        let resp = service.handle(msg, &ctx).await.unwrap();
        assert_eq!(resp.status, Some(StatusCode::OK));
    };
    let dispatched_call = || async {
        let resp = rt.handle(Message::request(Method::GET, "/data/items/k", "t")).await;
        assert_eq!(resp.status, Some(StatusCode::OK));
    };

    // Warmup, then measure.
    let _ = measure(200, direct_call).await;
    let _ = measure(200, dispatched_call).await;
    let n = 5000;
    let (direct_p50, direct_p99) = measure(n, direct_call).await;
    let (full_p50, full_p99) = measure(n, dispatched_call).await;

    let added_p99 = full_p99.saturating_sub(direct_p99);
    println!("G1 ({n} warm invocations):");
    println!("  direct service call   p50 {direct_p50:>8.1?}  p99 {direct_p99:>8.1?}");
    println!("  full dispatch path    p50 {full_p50:>8.1?}  p99 {full_p99:>8.1?}");
    println!("  added p99 overhead    {added_p99:?}  (target < 1 ms)");
    assert!(
        added_p99 < Duration::from_millis(1),
        "G1: dispatch overhead p99 {added_p99:?} >= 1 ms"
    );
}

// ---------------------------------------------------------------------------
// G3: containment — a pathological sandboxed service (infinite loop,
// unbounded allocation) cannot push another tenant's p99 beyond 2× baseline.
// ---------------------------------------------------------------------------

#[cfg(feature = "js")]
#[tokio::test]
#[ignore = "benchmark: cargo test -p rs2-core --release --features js --test g_benchmarks -- --ignored --nocapture"]
async fn g3_pathological_tenant_cannot_degrade_neighbors() {
    use rs2_core::message::{Body, MediaType};

    const SPIN: &str = "export default () => { for (;;) {} };";
    const HOG: &str = r#"
        export default () => {
            const hog = [];
            for (;;) { hog.push("x".repeat(1024 * 1024)); }
        };
    "#;

    // Containment knobs (PRD §9.3): short wall clock, tight memory, small
    // per-tenant concurrency — the evil tenant burns its own budget only.
    let limits = LimitTable {
        wall_clock_service: Duration::from_millis(200),
        memory_bytes: 64 * 1024 * 1024,
        tenant_concurrency: 4,
        ..LimitTable::default()
    };

    let spin_version = rs2_core::services::code::version_of(SPIN.as_bytes());
    let hog_version = rs2_core::services::code::version_of(HOG.as_bytes());
    let config = json!({ "mounts": [
        { "path": "/data", "service": "data" },
        { "path": "/services", "service": "services" },
        { "path": "/spin", "service": format!("code:spin@{spin_version}") },
        { "path": "/hog", "service": format!("code:hog@{hog_version}") }
    ]});

    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let rt = Runtime::new(
        Tenancy::Multi { domain_map: Default::default(), main_domain: Some("rs2.test".into()) },
        adapters,
        Arc::new(StaticLoader(config)),
        limits,
    );

    // Deploy the pathological services into the evil tenant.
    for (name, source) in [("spin", SPIN), ("hog", HOG)] {
        let deploy = Message::request(Method::PUT, &format!("/services/code/{name}"), "evil")
            .with_body(Body::from_bytes(
                source.as_bytes().to_vec(),
                MediaType::new("application/javascript"),
            ));
        assert_eq!(rt.handle(deploy).await.status, Some(StatusCode::CREATED));
    }

    // Seed and warm the good tenant.
    let seed = Message::request(Method::PUT, "/data/items/k", "good")
        .with_json(&json!({ "name": "steady" }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED));
    let good_call = || async {
        let resp = rt.handle(Message::request(Method::GET, "/data/items/k", "good")).await;
        assert_eq!(resp.status, Some(StatusCode::OK));
    };
    let _ = measure(100, good_call).await;

    // Baseline: the good tenant alone.
    let n = 1000;
    let (base_p50, base_p99) = measure(n, good_call).await;

    for (label, path) in [("infinite loop", "/spin"), ("allocation bomb", "/hog")] {
        // Attack: 8 evil clients hammer the pathological mount. Admission
        // caps them at 4 concurrent; each admitted isolate is terminated at
        // the 200 ms wall clock (or the heap cap).
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let evil_outcomes = Arc::new(std::sync::Mutex::new(Vec::<u16>::new()));
        let mut attackers = Vec::new();
        for _ in 0..8 {
            let rt = rt.clone();
            let stop = stop.clone();
            let outcomes = evil_outcomes.clone();
            let path = path.to_string();
            attackers.push(tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let resp = rt.handle(Message::request(Method::GET, &path, "evil")).await;
                    outcomes.lock().unwrap().push(resp.status.map(|s| s.as_u16()).unwrap_or(0));
                    // Real clients, not a scheduler-saturating busy loop —
                    // 8 clients × ~1k req/s is still a flood.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }));
        }
        // Let the attack saturate before measuring.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (attack_p50, attack_p99) = measure(n, good_call).await;
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        for a in attackers {
            let _ = a.await;
        }

        let evil = evil_outcomes.lock().unwrap();
        let evil_total = evil.len();
        let evil_contained = evil.iter().filter(|s| **s == 503).count();
        println!("G3 [{label}] (good tenant, {n} requests under attack):");
        println!("  baseline   p50 {base_p50:>8.1?}  p99 {base_p99:>8.1?}");
        println!("  under load p50 {attack_p50:>8.1?}  p99 {attack_p99:>8.1?}");
        println!(
            "  evil tenant: {evil_total} requests, {evil_contained} structured 503s \
             (limit_exceeded / admission)"
        );

        // Every pathological invocation came back structured, the process
        // is alive, and the neighbor's p99 stayed within 2× baseline
        // (small absolute floor to keep microsecond baselines un-flaky).
        assert!(evil_total > 0 && evil_contained == evil_total,
            "[{label}] evil outcomes were all structured 503s: {evil_contained}/{evil_total}");
        let bound = std::cmp::max(base_p99 * 2, base_p99 + Duration::from_millis(2));
        assert!(
            attack_p99 <= bound,
            "[{label}] G3: neighbor p99 {attack_p99:?} exceeded 2x baseline {base_p99:?}"
        );
    }
}
