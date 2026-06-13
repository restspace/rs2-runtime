//! `log` reader service (PRD §14): query the node log store, scoped to the
//! requesting tenant. Read-only; the URL-params idiom mirrors the `query`
//! service. Logs are operational data — guard the mount with `access` /
//! `manageRoles` like any other.
//!
//! - `GET /<mount>?$take=100&severity=warn&traceId=..&service=/orders&since=..&until=..&q=substr`
//!   → newest-first records (JSON array, or `text/plain` NDJSON via `Accept`).
//! - `GET /<mount>/<traceId>` → all records for one trace (request debugging).

use async_trait::async_trait;
use http::{Method, StatusCode};

use super::{Service, ServiceContext};
use crate::error::RsError;
use crate::logging::{LogQuery, Severity};
use crate::message::{Body, MediaType, Message};

#[derive(Default)]
pub struct LogReaderService;

impl LogReaderService {
    pub fn new() -> Self {
        LogReaderService
    }
}

#[async_trait]
impl Service for LogReaderService {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError> {
        if msg.method != Method::GET {
            return Err(RsError::new(
                405,
                crate::error::codes::BAD_REQUEST,
                "Method Not Allowed",
                "the log reader is read-only (GET)",
            ));
        }
        let store = ctx
            .log_store
            .as_ref()
            .ok_or_else(|| RsError::capability_denied("logStore"))?;
        if !store.is_queryable() {
            return Err(RsError::engine_unavailable(
                "logs are exported to an external sink, not locally queryable on this node",
            ));
        }

        let mut q = LogQuery {
            take: msg
                .url
                .query_param("$take")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(100)
                .min(10_000),
            ..Default::default()
        };
        // Positional `/<traceId>` (request-debugging view); the `traceId`
        // query param overrides it if both are given.
        if let Some(first) = msg.url.service_segments().first() {
            q.trace_id = Some((*first).to_string());
        }
        if let Some(t) = msg.url.query_param("traceId") {
            q.trace_id = Some(t);
        }
        if let Some(s) = msg.url.query_param("severity") {
            match Severity::parse(&s) {
                Some(sev) => q.min_severity = Some(sev),
                None => {
                    return Err(RsError::bad_request(format!(
                        "unknown severity '{s}' (debug|info|warn|error)"
                    )))
                }
            }
        }
        q.service = msg.url.query_param("service");
        q.contains = msg.url.query_param("q");
        q.since = msg.url.query_param("since").and_then(|s| parse_time(&s));
        q.until = msg.url.query_param("until").and_then(|s| parse_time(&s));

        let wants_text = msg
            .header("accept")
            .map(|a| a.contains("text/plain") && !a.contains("application/json"))
            .unwrap_or(false);
        let tenant = msg.tenant.clone();

        let records = store.query(&tenant, &q).await?;
        let total = records.len();

        let body = if wants_text {
            // One OTLP-JSON record per line (NDJSON).
            let text =
                records.iter().map(|r| r.to_otlp_json().to_string()).collect::<Vec<_>>().join("\n");
            Body::from_bytes(text, MediaType::new("text/plain; charset=utf-8"))
        } else {
            let arr = serde_json::Value::Array(records.iter().map(|r| r.to_otlp_json()).collect());
            Body::from_bytes(arr.to_string(), MediaType::json())
        };
        let mut resp = msg.response(StatusCode::OK, Some(body));
        resp.set_header("x-total-count", &total.to_string());
        Ok(resp)
    }
}

/// Parse a `since`/`until` value: Unix milliseconds (integer) or an RFC 3339
/// timestamp. Returns nanoseconds since the epoch to match `timeUnixNano`.
fn parse_time(s: &str) -> Option<u128> {
    if let Ok(ms) = s.parse::<u128>() {
        return Some(ms.saturating_mul(1_000_000));
    }
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp_nanos().max(0) as u128)
}
