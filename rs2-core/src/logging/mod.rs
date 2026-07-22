//! OTel-compatible structured logging (PRD §14).
//!
//! Logs flow through a [`LogSink`] capability. The default [`crate::adapters::FileLogStore`]
//! writes per-tenant NDJSON and reads back for the `log` reader service;
//! observability-platform exporters are write-only sinks composed in via
//! [`MultiSink`]. The record model mirrors the OTLP LogRecord data model
//! (`timeUnixNano`, `severityNumber`/`severityText`, `body`, `traceId`,
//! `spanId`, `attributes`) so an OTLP/HTTP exporter is a thin follow-on
//! adapter.
//!
//! Attribute keys follow OTel semantic conventions where they exist:
//! `http.request.method`, `url.path`, `http.response.status_code`,
//! `enduser.id`, `error.type`/`error.message`; RS2-specific dimensions use
//! the `rs2.` prefix (`rs2.tenant`, `rs2.mount`, `rs2.service`, `rs2.source`).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::contract::LogLevel;
use crate::error::RsError;
use crate::message::TraceContext;

/// Log severity. Serialized as the OTLP `severityNumber`/`severityText` pair.
/// `Ord` follows declaration order (Debug < Info < Warn < Error), so a
/// `min_severity` filter is a simple `>=`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Debug,
    Info,
    Warn,
    Error,
}

impl Severity {
    /// OTLP `severityNumber` (1..24): DEBUG=5, INFO=9, WARN=13, ERROR=17.
    pub fn number(self) -> u8 {
        match self {
            Severity::Debug => 5,
            Severity::Info => 9,
            Severity::Warn => 13,
            Severity::Error => 17,
        }
    }

    pub fn text(self) -> &'static str {
        match self {
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Warn => "WARN",
            Severity::Error => "ERROR",
        }
    }

    /// Bridge the engine contract's [`LogLevel`] (sandbox logs).
    pub fn from_log_level(level: LogLevel) -> Severity {
        match level {
            LogLevel::Debug => Severity::Debug,
            LogLevel::Info => Severity::Info,
            LogLevel::Warn => Severity::Warn,
            LogLevel::Error => Severity::Error,
        }
    }

    /// Parse a severity name (the reader's `severity=` filter and the server
    /// config `level` floor); case-insensitive.
    pub fn parse(s: &str) -> Option<Severity> {
        match s.trim().to_ascii_lowercase().as_str() {
            "debug" => Some(Severity::Debug),
            "info" => Some(Severity::Info),
            "warn" | "warning" => Some(Severity::Warn),
            "error" => Some(Severity::Error),
            _ => None,
        }
    }

    /// Recover a severity from a stored OTLP `severityNumber`.
    fn from_number(n: u64) -> Severity {
        match n {
            0..=8 => Severity::Debug,
            9..=12 => Severity::Info,
            13..=16 => Severity::Warn,
            _ => Severity::Error,
        }
    }
}

/// One log entry, modeled on the OTLP LogRecord.
#[derive(Debug, Clone)]
pub struct LogRecord {
    /// Nanoseconds since the Unix epoch (OTLP `timeUnixNano`).
    pub time_unix_nano: u128,
    pub severity: Severity,
    /// Human-readable message (OTLP `body`).
    pub body: String,
    /// Resource scope and the read-isolation key (per-tenant files).
    pub tenant: String,
    pub trace_id: String,
    pub span_id: String,
    /// OTLP attributes (semantic-convention keys; see the module docs).
    pub attributes: Vec<(String, Value)>,
}

impl LogRecord {
    /// A record stamped with the current wall-clock time and no attributes.
    pub fn now(
        severity: Severity,
        tenant: &str,
        trace: &TraceContext,
        body: impl Into<String>,
    ) -> LogRecord {
        LogRecord {
            time_unix_nano: now_unix_nano(),
            severity,
            body: body.into(),
            tenant: tenant.to_string(),
            trace_id: trace.trace_id.clone(),
            span_id: trace.span_id.clone(),
            attributes: Vec::new(),
        }
    }

    /// Builder: append an attribute.
    pub fn attr(mut self, key: &str, value: impl Into<Value>) -> LogRecord {
        self.attributes.push((key.to_string(), value.into()));
        self
    }

    /// Builder: append an attribute only when `value` is `Some`.
    pub fn attr_opt(mut self, key: &str, value: Option<impl Into<Value>>) -> LogRecord {
        if let Some(v) = value {
            self.attributes.push((key.to_string(), v.into()));
        }
        self
    }

    /// String value of an attribute (reader filters on `rs2.mount` etc.).
    pub fn attr_str(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| v.as_str())
    }

    /// Serialize to one OTLP-shaped JSON object (one NDJSON line). `tenant`
    /// is surfaced as the `rs2.tenant` attribute for portability to a merged
    /// exporter; per-tenant files don't otherwise need it.
    pub fn to_otlp_json(&self) -> Value {
        let mut attrs = serde_json::Map::new();
        attrs.insert("rs2.tenant".to_string(), json!(self.tenant));
        for (k, v) in &self.attributes {
            attrs.insert(k.clone(), v.clone());
        }
        json!({
            "timeUnixNano": self.time_unix_nano.to_string(),
            "severityNumber": self.severity.number(),
            "severityText": self.severity.text(),
            "body": self.body,
            "traceId": self.trace_id,
            "spanId": self.span_id,
            "attributes": Value::Object(attrs),
        })
    }

    /// Parse one NDJSON line back into a record (reader path). `tenant` comes
    /// from the per-tenant file name. Returns `None` for an unparseable line
    /// (a truncated tail line is skipped, not fatal).
    pub fn from_ndjson_line(tenant: &str, line: &str) -> Option<LogRecord> {
        let v: Value = serde_json::from_str(line).ok()?;
        let time_unix_nano = v
            .get("timeUnixNano")
            .and_then(|t| {
                t.as_str()
                    .and_then(|s| s.parse::<u128>().ok())
                    .or_else(|| t.as_u64().map(u128::from))
            })
            .unwrap_or(0);
        let severity = v
            .get("severityNumber")
            .and_then(|n| n.as_u64())
            .map(Severity::from_number)
            .unwrap_or(Severity::Info);
        let body = v
            .get("body")
            .and_then(|b| b.as_str())
            .unwrap_or("")
            .to_string();
        let trace_id = v
            .get("traceId")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
        let span_id = v
            .get("spanId")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
        let attributes = v
            .get("attributes")
            .and_then(|a| a.as_object())
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        Some(LogRecord {
            time_unix_nano,
            severity,
            body,
            tenant: tenant.to_string(),
            trace_id,
            span_id,
            attributes,
        })
    }
}

/// Wall-clock nanoseconds since the Unix epoch.
pub fn now_unix_nano() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// A logging sink: fire-and-forget emit. Implementations MUST NOT block the
/// caller — the request path calls this (the file adapter hands off to a
/// background writer task).
pub trait LogSink: Send + Sync {
    fn emit(&self, record: LogRecord);

    /// Whether anything actually consumes emitted records. A no-op sink
    /// returns `false` so callers can skip building a record on the hot path
    /// (the per-request boundary log). Default `true`.
    fn enabled(&self) -> bool {
        true
    }
}

/// Reader-side filter for [`LogStore::query`].
#[derive(Debug, Clone, Default)]
pub struct LogQuery {
    /// Maximum records to return (newest-first).
    pub take: usize,
    /// Inclusive lower/upper bounds on `timeUnixNano`.
    pub since: Option<u128>,
    pub until: Option<u128>,
    /// Minimum severity (keep records `>=`).
    pub min_severity: Option<Severity>,
    pub trace_id: Option<String>,
    /// Mount filter, matched against the `rs2.mount` attribute.
    pub service: Option<String>,
    /// Case-sensitive substring on the message body.
    pub contains: Option<String>,
}

impl LogQuery {
    /// Whether a record passes every set filter.
    pub fn matches(&self, r: &LogRecord) -> bool {
        if let Some(s) = self.since {
            if r.time_unix_nano < s {
                return false;
            }
        }
        if let Some(u) = self.until {
            if r.time_unix_nano > u {
                return false;
            }
        }
        if let Some(min) = self.min_severity {
            if r.severity < min {
                return false;
            }
        }
        if let Some(t) = &self.trace_id {
            if &r.trace_id != t {
                return false;
            }
        }
        if let Some(svc) = &self.service {
            // Match service-tagged logs by their mount, and host boundary logs
            // (which don't carry a mount) by request-path prefix.
            let by_mount = r.attr_str("rs2.mount") == Some(svc.as_str());
            let by_path = r
                .attr_str("url.path")
                .map(|p| p == svc || p.starts_with(&format!("{}/", svc.trim_end_matches('/'))))
                .unwrap_or(false);
            if !by_mount && !by_path {
                return false;
            }
        }
        if let Some(q) = &self.contains {
            if !r.body.contains(q.as_str()) {
                return false;
            }
        }
        true
    }
}

/// A queryable log store: a [`LogSink`] that can also read back. The reader
/// service requires this; write-only exporters implement only `LogSink` and
/// are composed in via [`MultiSink`].
#[async_trait]
pub trait LogStore: LogSink {
    /// Most-recent-first records for `tenant` matching `q` (up to `q.take`).
    async fn query(&self, tenant: &str, q: &LogQuery) -> Result<Vec<LogRecord>, RsError>;

    /// Whether read-back is supported. A write-only configuration returns
    /// `false` so the reader service can report that logs are exported, not
    /// locally queryable.
    fn is_queryable(&self) -> bool {
        true
    }

    /// Barrier: resolve once every record emitted before this call is durably
    /// written. Default no-op (for synchronous/Null sinks); the file adapter
    /// overrides it to drain its background writer. Used by tests and graceful
    /// shutdown — not on the request path.
    async fn flush(&self) {}
}

/// A no-op store: drops emits, reports not-queryable. The default node sink
/// when no logging is configured (embedders/tests opt in via
/// [`crate::tenant::Adapters::with_logging`]).
pub struct NullLogStore;

impl LogSink for NullLogStore {
    fn emit(&self, _record: LogRecord) {}
    fn enabled(&self) -> bool {
        false
    }
}

#[async_trait]
impl LogStore for NullLogStore {
    async fn query(&self, _tenant: &str, _q: &LogQuery) -> Result<Vec<LogRecord>, RsError> {
        Ok(Vec::new())
    }
    fn is_queryable(&self) -> bool {
        false
    }
}

/// Fan-out sink: emits to a queryable store and any number of write-only
/// exporters, so a node can store locally **and** forward to observability
/// platforms. Reads delegate to the queryable member; a write-only
/// composition reports not-queryable.
pub struct MultiSink {
    queryable: Option<Arc<dyn LogStore>>,
    extra: Vec<Arc<dyn LogSink>>,
}

impl MultiSink {
    /// Back the reader with `store`, and emit to it.
    pub fn new(store: Arc<dyn LogStore>) -> MultiSink {
        MultiSink {
            queryable: Some(store),
            extra: Vec::new(),
        }
    }

    /// Write-only: forward to `sinks` only; reads report not-queryable.
    pub fn write_only(sinks: Vec<Arc<dyn LogSink>>) -> MultiSink {
        MultiSink {
            queryable: None,
            extra: sinks,
        }
    }

    /// Add another sink (e.g. an OTLP exporter) alongside.
    pub fn with_sink(mut self, sink: Arc<dyn LogSink>) -> MultiSink {
        self.extra.push(sink);
        self
    }
}

impl LogSink for MultiSink {
    fn emit(&self, record: LogRecord) {
        // Clone for every target but the last; move into the final emit.
        match (&self.queryable, self.extra.as_slice()) {
            (Some(q), []) => q.emit(record),
            (Some(q), extra) => {
                q.emit(record.clone());
                emit_each(extra, record);
            }
            (None, extra) => emit_each(extra, record),
        }
    }
    fn enabled(&self) -> bool {
        self.queryable
            .as_ref()
            .map(|q| q.enabled())
            .unwrap_or(false)
            || self.extra.iter().any(|s| s.enabled())
    }
}

/// Emit `record` to each sink, cloning for all but the last (may be empty).
fn emit_each(sinks: &[Arc<dyn LogSink>], record: LogRecord) {
    let n = sinks.len();
    for (i, s) in sinks.iter().enumerate() {
        if i + 1 == n {
            s.emit(record);
            return;
        }
        s.emit(record.clone());
    }
}

#[async_trait]
impl LogStore for MultiSink {
    async fn query(&self, tenant: &str, q: &LogQuery) -> Result<Vec<LogRecord>, RsError> {
        match &self.queryable {
            Some(s) => s.query(tenant, q).await,
            None => Ok(Vec::new()),
        }
    }
    fn is_queryable(&self) -> bool {
        self.queryable
            .as_ref()
            .map(|s| s.is_queryable())
            .unwrap_or(false)
    }
    async fn flush(&self) {
        if let Some(s) = &self.queryable {
            s.flush().await;
        }
    }
}

/// A mount-bound emit handle stored on `ServiceContext`. Tenant/mount/service
/// are fixed at mount-build time; the per-invocation trace is bound via
/// [`ServiceLogger::at`]. Always present (a no-op when the node sink is
/// [`NullLogStore`]) — emitting your own stamped line is not a privilege
/// boundary, so it is granted unconditionally; the host stamps identity so a
/// service cannot forge another's logs.
#[derive(Clone)]
pub struct ServiceLogger {
    sink: Arc<dyn LogStore>,
    tenant: String,
    mount: String,
    service: String,
}

impl ServiceLogger {
    pub fn new(sink: Arc<dyn LogStore>, tenant: &str, mount: &str, service: &str) -> ServiceLogger {
        ServiceLogger {
            sink,
            tenant: tenant.to_string(),
            mount: mount.to_string(),
            service: service.to_string(),
        }
    }

    /// The underlying node sink (for the host log bridge, which stamps its
    /// own per-invocation context onto sandbox logs).
    pub fn sink(&self) -> Arc<dyn LogStore> {
        self.sink.clone()
    }

    /// Bind the per-invocation trace; cheap (clones two short ids).
    pub fn at(&self, trace: &TraceContext) -> BoundLogger<'_> {
        BoundLogger {
            logger: self,
            trace_id: trace.trace_id.clone(),
            span_id: trace.span_id.clone(),
        }
    }

    pub fn debug(&self, trace: &TraceContext, body: impl Into<String>) {
        self.at(trace).debug(body)
    }
    pub fn info(&self, trace: &TraceContext, body: impl Into<String>) {
        self.at(trace).info(body)
    }
    pub fn warn(&self, trace: &TraceContext, body: impl Into<String>) {
        self.at(trace).warn(body)
    }
    pub fn error(&self, trace: &TraceContext, body: impl Into<String>) {
        self.at(trace).error(body)
    }
}

/// A [`ServiceLogger`] with trace/span bound for one invocation.
pub struct BoundLogger<'a> {
    logger: &'a ServiceLogger,
    trace_id: String,
    span_id: String,
}

impl BoundLogger<'_> {
    pub fn log(&self, severity: Severity, body: impl Into<String>) {
        let l = self.logger;
        if !l.sink.enabled() {
            return;
        }
        let record = LogRecord {
            time_unix_nano: now_unix_nano(),
            severity,
            body: body.into(),
            tenant: l.tenant.clone(),
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            attributes: vec![
                ("rs2.mount".to_string(), json!(l.mount)),
                ("rs2.service".to_string(), json!(l.service)),
                ("rs2.source".to_string(), json!("service")),
            ],
        };
        l.sink.emit(record);
    }
    pub fn debug(&self, body: impl Into<String>) {
        self.log(Severity::Debug, body)
    }
    pub fn info(&self, body: impl Into<String>) {
        self.log(Severity::Info, body)
    }
    pub fn warn(&self, body: impl Into<String>) {
        self.log(Severity::Warn, body)
    }
    pub fn error(&self, body: impl Into<String>) {
        self.log(Severity::Error, body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_orders_ascending() {
        assert!(Severity::Debug < Severity::Info);
        assert!(Severity::Info < Severity::Warn);
        assert!(Severity::Warn < Severity::Error);
    }

    #[test]
    fn otlp_json_round_trips() {
        let trace = TraceContext::new();
        let rec = LogRecord::now(Severity::Warn, "acme", &trace, "GET /x -> 404")
            .attr("rs2.mount", "/x")
            .attr("http.response.status_code", 404);
        let line = rec.to_otlp_json().to_string();
        let back = LogRecord::from_ndjson_line("acme", &line).expect("parse");
        assert_eq!(back.tenant, "acme");
        assert_eq!(back.severity, Severity::Warn);
        assert_eq!(back.body, "GET /x -> 404");
        assert_eq!(back.trace_id, trace.trace_id);
        assert_eq!(back.span_id, trace.span_id);
        assert_eq!(back.attr_str("rs2.mount"), Some("/x"));
        // OTLP field names present.
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["severityText"], "WARN");
        assert_eq!(v["severityNumber"], 13);
        assert!(v["timeUnixNano"].is_string());
    }

    #[test]
    fn query_filters() {
        let trace = TraceContext::new();
        let rec =
            LogRecord::now(Severity::Info, "acme", &trace, "hello world").attr("rs2.mount", "/api");
        assert!(LogQuery {
            take: 10,
            ..Default::default()
        }
        .matches(&rec));
        assert!(LogQuery {
            min_severity: Some(Severity::Info),
            ..Default::default()
        }
        .matches(&rec));
        assert!(!LogQuery {
            min_severity: Some(Severity::Warn),
            ..Default::default()
        }
        .matches(&rec));
        assert!(LogQuery {
            service: Some("/api".into()),
            ..Default::default()
        }
        .matches(&rec));
        assert!(!LogQuery {
            service: Some("/other".into()),
            ..Default::default()
        }
        .matches(&rec));
        assert!(LogQuery {
            contains: Some("world".into()),
            ..Default::default()
        }
        .matches(&rec));
        assert!(!LogQuery {
            contains: Some("xyz".into()),
            ..Default::default()
        }
        .matches(&rec));
        assert!(!LogQuery {
            trace_id: Some("nope".into()),
            ..Default::default()
        }
        .matches(&rec));
    }
}
