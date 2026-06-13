//! Logging subsystem (PRD §14): FileLogStore round-trip, filters, rotation and
//! tenant isolation; host boundary/error logs and prebuilt-service application
//! logs through the full dispatch path, read back via the `log` service.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use rs2_core::adapters::{FileLogStore, LocalFsFileStore, MemDataStore};
use rs2_core::logging::{LogQuery, LogRecord, LogSink, LogStore, Severity};
use rs2_core::message::{Body, MediaType, Message, TraceContext};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

// ---- direct adapter tests ----

fn rec(sev: Severity, tenant: &str, body: &str) -> LogRecord {
    LogRecord::now(sev, tenant, &TraceContext::new(), body)
}

#[tokio::test]
async fn file_store_round_trip_and_filters() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileLogStore::with_defaults(dir.path());
    store.emit(rec(Severity::Info, "acme", "first").attr("rs2.mount", "/a"));
    store.emit(rec(Severity::Warn, "acme", "second warn").attr("rs2.mount", "/b"));
    store.emit(rec(Severity::Error, "acme", "third boom").attr("rs2.mount", "/a"));
    store.flush().await;

    // newest-first tail
    let all = store.query("acme", &LogQuery { take: 10, ..Default::default() }).await.unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].body, "third boom");
    assert_eq!(all[2].body, "first");

    // severity floor
    let warns = store
        .query("acme", &LogQuery { take: 10, min_severity: Some(Severity::Warn), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(warns.len(), 2);

    // service (mount) filter
    let a = store
        .query("acme", &LogQuery { take: 10, service: Some("/a".into()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(a.len(), 2);

    // body substring
    let boom = store
        .query("acme", &LogQuery { take: 10, contains: Some("boom".into()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(boom.len(), 1);

    // take limit (newest)
    let one = store.query("acme", &LogQuery { take: 1, ..Default::default() }).await.unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].body, "third boom");
}

#[tokio::test]
async fn file_store_tenant_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileLogStore::with_defaults(dir.path());
    store.emit(rec(Severity::Info, "acme", "acme-line"));
    store.emit(rec(Severity::Info, "globex", "globex-line"));
    store.flush().await;

    let a = store.query("acme", &LogQuery { take: 10, ..Default::default() }).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].body, "acme-line");
    let g = store.query("globex", &LogQuery { take: 10, ..Default::default() }).await.unwrap();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].body, "globex-line");
}

#[tokio::test]
async fn file_store_trace_filter_and_time_range() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileLogStore::with_defaults(dir.path());
    let t = TraceContext::new();
    store.emit(LogRecord::now(Severity::Info, "acme", &t, "traced"));
    store.emit(rec(Severity::Info, "acme", "other"));
    store.flush().await;

    let traced = store
        .query("acme", &LogQuery { take: 10, trace_id: Some(t.trace_id.clone()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(traced.len(), 1);
    assert_eq!(traced[0].body, "traced");

    // A `since` in the far future excludes everything.
    let future = rs2_core::logging::now_unix_nano() + 1_000_000_000_000;
    let none = store
        .query("acme", &LogQuery { take: 10, since: Some(future), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(none.len(), 0);
}

#[tokio::test]
async fn file_store_rotation_preserves_newest() {
    let dir = tempfile::tempdir().unwrap();
    // Tiny cap forces rotation; keep 2 backups.
    let store = FileLogStore::new(dir.path(), 256, 2);
    for i in 0..50 {
        store.emit(rec(Severity::Info, "acme", &format!("line-{i:03}")));
    }
    store.flush().await;

    let newest = store.query("acme", &LogQuery { take: 1, ..Default::default() }).await.unwrap();
    assert_eq!(newest.len(), 1);
    assert_eq!(newest[0].body, "line-049");

    let backups = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("acme.ndjson."))
        .count();
    assert!(backups >= 1, "expected rotated backup files, found {backups}");
}

// ---- boundary + service logs through the runtime ----

struct StaticLoader(Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _t: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn logging_runtime(
    file_root: &std::path::Path,
    log_root: &std::path::Path,
    level: Severity,
) -> (Arc<Runtime>, Arc<FileLogStore>) {
    let log = Arc::new(FileLogStore::with_defaults(log_root));
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    )
    .with_logging(log.clone(), level);
    let loader = Arc::new(StaticLoader(json!({
        "auth": { "jwtSecret": "log-secret" },
        "mounts": [
            { "path": "/files", "service": "file" },
            { "path": "/auth", "service": "auth" },
            { "path": "/logs", "service": "log" }
        ]
    })));
    let rt = Runtime::new(Tenancy::Single { tenant: "t1".into() }, adapters, loader, LimitTable::default());
    (rt, log)
}

async fn read_logs(rt: &Runtime, path: &str) -> Vec<Value> {
    let mut resp = rt.handle(Message::request(Method::GET, path, "t1")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "reader should return 200");
    let bytes = resp.body.as_mut().unwrap().materialize(1 << 20).await.unwrap();
    serde_json::from_slice::<Value>(&bytes).unwrap().as_array().cloned().unwrap()
}

#[tokio::test]
async fn boundary_logs_severity_otlp_and_reader() {
    let fdir = tempfile::tempdir().unwrap();
    let ldir = tempfile::tempdir().unwrap();
    let (rt, log) = logging_runtime(fdir.path(), ldir.path(), Severity::Debug);

    rt.handle(
        Message::request(Method::PUT, "/files/a.txt", "t1")
            .with_body(Body::from_string("hi", MediaType::new("text/plain"))),
    )
    .await;
    let ok = rt.handle(Message::request(Method::GET, "/files/a.txt", "t1")).await;
    assert_eq!(ok.status, Some(StatusCode::OK));
    let missing = rt.handle(Message::request(Method::GET, "/files/nope.txt", "t1")).await;
    assert_eq!(missing.status, Some(StatusCode::NOT_FOUND));
    log.flush().await;

    let records = read_logs(&rt, "/logs?$take=50").await;
    assert!(records.len() >= 3, "want >=3 boundary logs, got {}", records.len());

    // OTLP field names / resource attribute present.
    let first = &records[0];
    assert!(first["timeUnixNano"].is_string());
    assert!(first["severityText"].is_string());
    assert_eq!(first["attributes"]["rs2.tenant"], "t1");

    // The 404 logged at WARN with error.type=not_found.
    let warn = records
        .iter()
        .find(|r| r["attributes"]["url.path"] == "/files/nope.txt")
        .expect("missing-file boundary log");
    assert_eq!(warn["severityText"], "WARN");
    assert_eq!(warn["attributes"]["error.type"], "not_found");
    assert_eq!(warn["attributes"]["http.response.status_code"], 404);
    assert_eq!(warn["attributes"]["rs2.source"], "external");

    // The successful GET logged at INFO.
    let info = records
        .iter()
        .find(|r| {
            r["attributes"]["url.path"] == "/files/a.txt"
                && r["attributes"]["http.request.method"] == "GET"
        })
        .expect("GET boundary log");
    assert_eq!(info["severityText"], "INFO");

    // Trace-scoped read: every record shares the requested trace id.
    let tid = warn["traceId"].as_str().unwrap().to_string();
    let scoped = read_logs(&rt, &format!("/logs/{tid}")).await;
    assert!(!scoped.is_empty());
    assert!(scoped.iter().all(|r| r["traceId"] == tid.as_str()));
}

#[tokio::test]
async fn service_log_correlates_with_boundary() {
    let fdir = tempfile::tempdir().unwrap();
    let ldir = tempfile::tempdir().unwrap();
    let (rt, log) = logging_runtime(fdir.path(), ldir.path(), Severity::Info);

    // A failed login: the auth service emits its own WARN, and the host emits
    // a 401 boundary WARN — same trace, distinguishable by rs2.source.
    let login = Message::request(Method::POST, "/auth/login", "t1")
        .with_json(&json!({ "email": "nobody@t1.test", "password": "x" }));
    let resp = rt.handle(login).await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));
    log.flush().await;

    let records = read_logs(&rt, "/logs?$take=50&severity=warn").await;
    let svc = records
        .iter()
        .find(|r| r["attributes"]["rs2.source"] == "service")
        .expect("auth service log");
    assert_eq!(svc["attributes"]["rs2.service"], "auth");
    assert_eq!(svc["severityText"], "WARN");
    assert!(svc["body"].as_str().unwrap().contains("login failed"));

    // Boundary log for the same request shares the trace id.
    let tid = svc["traceId"].as_str().unwrap();
    assert!(records
        .iter()
        .any(|r| r["attributes"]["rs2.source"] == "external" && r["traceId"] == tid));
}

#[tokio::test]
async fn info_floor_suppresses_debug_internal() {
    let fdir = tempfile::tempdir().unwrap();
    let ldir = tempfile::tempdir().unwrap();
    // Info floor: external success logs at Info (kept). There are no internal
    // hops here, but this pins the floor behavior for the common config.
    let (rt, log) = logging_runtime(fdir.path(), ldir.path(), Severity::Info);
    rt.handle(Message::request(Method::GET, "/files/", "t1")).await;
    log.flush().await;
    let records = read_logs(&rt, "/logs?$take=50").await;
    assert!(records.iter().all(|r| r["severityText"] != "DEBUG"));
}

// ---- sandbox logging (un-stubbed HostApi::log) ----

#[cfg(feature = "js")]
#[tokio::test]
async fn js_console_log_reaches_sink_stamped_custom() {
    use std::collections::HashMap;

    use rs2_core::contract::{Engine, GrantedHost, HostApi, InvocationLimits, LogContext, ServiceCode};
    use rs2_core::engines::js::JsEngine;

    let dir = tempfile::tempdir().unwrap();
    let store: Arc<FileLogStore> = Arc::new(FileLogStore::with_defaults(dir.path()));
    let log_ctx = LogContext {
        sink: store.clone(),
        tenant: "t1".into(),
        mount: "/svc".into(),
        service: "demo@v1".into(),
        trace_id: "trace123".into(),
        span_id: "span123".into(),
    };
    let host: Arc<dyn HostApi> = Arc::new(
        GrantedHost::new(
            HashMap::new(),
            0,
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            "demo@v1",
        )
        .with_log_context(log_ctx),
    );

    let engine = JsEngine::new();
    let bundle = r#"
        export default async (msg, ctx) => {
            console.log("hello from sandbox");
            console.warn("careful now");
            return { status: 200, body: "ok" };
        };
    "#;
    let code = ServiceCode::JsBundle(Arc::new(bundle.to_string()));
    let limits = InvocationLimits {
        wall_clock: std::time::Duration::from_secs(5),
        memory_bytes: 64 << 20,
        outbound_calls: 0,
        materialized_body_bytes: 8 << 20,
    };
    let resp = engine
        .invoke(&code, Message::request(Method::POST, "/svc", "t1"), &json!({}), host, &limits)
        .await
        .unwrap();
    assert_eq!(resp.status, Some(StatusCode::OK));

    store.flush().await;
    let records = store.query("t1", &LogQuery { take: 10, ..Default::default() }).await.unwrap();
    let hello = records.iter().find(|r| r.body == "hello from sandbox").expect("console.log line");
    assert_eq!(hello.attr_str("rs2.source"), Some("custom"));
    assert_eq!(hello.attr_str("rs2.service"), Some("demo@v1"));
    assert_eq!(hello.attr_str("rs2.mount"), Some("/svc"));
    assert_eq!(hello.trace_id, "trace123");
    let warn = records.iter().find(|r| r.body == "careful now").expect("console.warn line");
    assert_eq!(warn.severity, Severity::Warn);
}
