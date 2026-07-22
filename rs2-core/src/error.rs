//! Structured errors (PRD §12): every failure maps to an RFC 9457
//! problem-details body with a machine-readable `code`, so agents and tools
//! can recover instead of guessing.

use serde::Serialize;
use serde_json::json;

/// Machine-readable error codes. Stable strings, part of the contract.
pub mod codes {
    pub const BAD_REQUEST: &str = "bad_request";
    pub const UNAUTHORIZED: &str = "unauthorized";
    pub const FORBIDDEN: &str = "forbidden";
    pub const NOT_FOUND: &str = "not_found";
    pub const CONFLICT: &str = "conflict";
    pub const PRECONDITION_FAILED: &str = "precondition_failed";
    pub const PAYLOAD_TOO_LARGE: &str = "payload_too_large";
    pub const VALIDATION_FAILED: &str = "validation_failed";
    pub const IDEMPOTENCY_KEY_REUSE: &str = "idempotency_key_reuse";
    pub const LIMIT_EXCEEDED: &str = "limit_exceeded";
    pub const CAPABILITY_DENIED: &str = "capability_denied";
    pub const CONTRACT_VIOLATION: &str = "contract_violation";
    pub const ENGINE_UNAVAILABLE: &str = "engine_unavailable";
    pub const PATH_UNSAFE: &str = "path_unsafe";
    pub const INTERNAL: &str = "internal";
}

/// The single error type crossing module boundaries in rs2-core.
#[derive(Debug, Clone, thiserror::Error, Serialize)]
#[error("{status} {code}: {detail}")]
pub struct RsError {
    pub status: u16,
    /// Machine-readable code from [`codes`].
    pub code: &'static str,
    pub title: String,
    pub detail: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    /// Optional structured extras (e.g. which limit, observed value, cap).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

impl RsError {
    pub fn new(status: u16, code: &'static str, title: &str, detail: impl Into<String>) -> Self {
        RsError {
            status,
            code,
            title: title.to_string(),
            detail: detail.into(),
            retryable: false,
            retry_after_ms: None,
            extra: None,
        }
    }

    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(400, codes::BAD_REQUEST, "Bad Request", detail)
    }

    pub fn unauthorized(detail: impl Into<String>) -> Self {
        Self::new(401, codes::UNAUTHORIZED, "Unauthorized", detail)
    }

    pub fn forbidden(detail: impl Into<String>) -> Self {
        Self::new(403, codes::FORBIDDEN, "Forbidden", detail)
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::new(404, codes::NOT_FOUND, "Not Found", detail)
    }

    pub fn conflict(detail: impl Into<String>) -> Self {
        Self::new(409, codes::CONFLICT, "Conflict", detail)
    }

    /// An `If-Match`/`If-None-Match` precondition on a store write was not met
    /// (the resource changed, is missing, or already exists). The standard
    /// optimistic-concurrency signal: re-read and reapply.
    pub fn precondition_failed(detail: impl Into<String>) -> Self {
        Self::new(
            412,
            codes::PRECONDITION_FAILED,
            "Precondition Failed",
            detail,
        )
    }

    pub fn payload_too_large(detail: impl Into<String>) -> Self {
        Self::new(413, codes::PAYLOAD_TOO_LARGE, "Payload Too Large", detail)
    }

    pub fn validation_failed(detail: impl Into<String>, errors: serde_json::Value) -> Self {
        let mut e = Self::new(422, codes::VALIDATION_FAILED, "Validation Failed", detail);
        e.extra = Some(json!({ "errors": errors }));
        e
    }

    /// Same `Idempotency-Key`, different request payload (PRD §7.2).
    pub fn idempotency_key_reuse(detail: impl Into<String>) -> Self {
        Self::new(
            422,
            codes::IDEMPOTENCY_KEY_REUSE,
            "Idempotency Key Reuse",
            detail,
        )
    }

    pub fn path_unsafe(detail: impl Into<String>) -> Self {
        Self::new(400, codes::PATH_UNSAFE, "Unsafe Path", detail)
    }

    pub fn capability_denied(capability: &str) -> Self {
        let mut e = Self::new(
            403,
            codes::CAPABILITY_DENIED,
            "Capability Denied",
            format!("capability '{capability}' is not granted to this service"),
        );
        e.extra = Some(json!({ "capability": capability }));
        e
    }

    pub fn contract_violation(detail: impl Into<String>) -> Self {
        Self::new(502, codes::CONTRACT_VIOLATION, "Contract Violation", detail)
    }

    /// A resource limit was breached. Attributed to the tenant; retryable.
    pub fn limit_exceeded(
        limit: &str,
        observed: impl Into<serde_json::Value>,
        cap: impl Into<serde_json::Value>,
    ) -> Self {
        let mut e = Self::new(
            503,
            codes::LIMIT_EXCEEDED,
            "Limit Exceeded",
            format!("limit '{limit}' exceeded"),
        );
        e.retryable = true;
        e.retry_after_ms = Some(2000);
        e.extra = Some(json!({ "limit": limit, "observed": observed.into(), "cap": cap.into() }));
        e
    }

    pub fn engine_unavailable(detail: impl Into<String>) -> Self {
        Self::new(501, codes::ENGINE_UNAVAILABLE, "Engine Unavailable", detail)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::new(500, codes::INTERNAL, "Internal Error", detail)
    }

    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }

    /// RFC 9457 problem-details body for this error.
    pub fn to_problem_json(&self, tenant: &str, trace_id: &str) -> serde_json::Value {
        let mut problem = json!({
            "type": format!("https://rs2.dev/errors#{}", self.code),
            "title": self.title,
            "status": self.status,
            "code": self.code,
            "detail": self.detail,
            "tenant": tenant,
            "traceId": trace_id,
            "retryable": self.retryable,
        });
        if let Some(ms) = self.retry_after_ms {
            problem["retryAfterMs"] = json!(ms);
        }
        if let Some(extra) = &self.extra {
            if let (Some(obj), Some(ex)) = (problem.as_object_mut(), extra.as_object()) {
                for (k, v) in ex {
                    obj.insert(k.clone(), v.clone());
                }
            }
        }
        problem
    }
}

impl From<std::io::Error> for RsError {
    fn from(e: std::io::Error) -> Self {
        match e.kind() {
            std::io::ErrorKind::NotFound => RsError::not_found("resource does not exist"),
            std::io::ErrorKind::AlreadyExists => RsError::conflict("resource already exists"),
            _ => RsError::internal(format!("io error: {e}")),
        }
    }
}
