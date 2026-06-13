//! Retry policies and effect classes (PRD §7.1, §7.3).
//!
//! Retry policy is declarative and uniform across internal and external
//! calls. The runtime only auto-retries calls whose target effect class
//! permits it: `pure`/`idempotent` always, `keyed` with a key, `unsafe` never.

use std::future::Future;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::RsError;
use crate::message::Message;

/// Effect class of an endpoint (PRD §7.1). Manifests may declare one;
/// otherwise the runtime infers a default from the method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffectClass {
    /// No side effects: freely retried, cacheable.
    Pure,
    /// Repeating has the same effect: retried per policy without a key.
    Idempotent,
    /// Safe to repeat iff the same `Idempotency-Key`: key required for retry.
    Keyed,
    /// Repeating may duplicate effects: never auto-retried.
    Unsafe,
}

impl EffectClass {
    /// Default inference from method (PRD §7.1): GET/HEAD → pure,
    /// PUT/DELETE → idempotent, POST/PATCH → unsafe.
    pub fn default_for_method(method: &http::Method) -> Self {
        match *method {
            http::Method::GET | http::Method::HEAD | http::Method::OPTIONS => EffectClass::Pure,
            http::Method::PUT | http::Method::DELETE => EffectClass::Idempotent,
            _ => EffectClass::Unsafe,
        }
    }

    /// Whether auto-retry is permitted for this effect class (PRD §7.3).
    pub fn permits_retry(&self, has_idempotency_key: bool) -> bool {
        match self {
            EffectClass::Pure | EffectClass::Idempotent => true,
            EffectClass::Keyed => has_idempotency_key,
            EffectClass::Unsafe => false,
        }
    }
}

/// Declarative retry policy (PRD §7.3, Restspace shape retained).
/// Resolution: per-call override → mount config → tenant default → runtime
/// default ([`RetryPolicy::resolve`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct RetryPolicy {
    pub enabled: bool,
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
    pub jitter: Jitter,
    pub retry_statuses: Vec<u16>,
    pub retry_on_network_error: bool,
    pub respect_retry_after: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Jitter {
    None,
    Full,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            enabled: true,
            max_attempts: 4,
            base_delay_ms: 250,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
            jitter: Jitter::Full,
            retry_statuses: vec![408, 429, 500, 502, 503, 504],
            retry_on_network_error: true,
            respect_retry_after: true,
        }
    }
}

impl RetryPolicy {
    /// A policy that never retries (the runtime default when no policy is
    /// configured anywhere — retries are opt-in per PRD §7.3 resolution).
    pub fn no_retry() -> Self {
        RetryPolicy { enabled: false, max_attempts: 1, ..Default::default() }
    }

    /// Resolve the effective policy from the override chain: first present
    /// wins (per-call → mount → tenant default → runtime default).
    pub fn resolve(chain: &[Option<&RetryPolicy>]) -> RetryPolicy {
        chain
            .iter()
            .find_map(|p| p.cloned())
            .unwrap_or_else(RetryPolicy::no_retry)
    }

    /// Parse a policy from a JSON config value (e.g. a mount's `retry` key).
    pub fn from_config(value: &serde_json::Value) -> Option<RetryPolicy> {
        if value.is_null() {
            return None;
        }
        serde_json::from_value(value.clone()).ok()
    }

    /// Whether a response status is retryable under this policy.
    pub fn retryable_status(&self, status: u16) -> bool {
        self.enabled && self.retry_statuses.contains(&status)
    }

    /// Backoff delay before retrying after `failed_attempt` (1-based).
    /// A `Retry-After` from the server wins (capped at maxDelayMs) when
    /// `respectRetryAfter` is set.
    pub fn delay(&self, failed_attempt: u32, retry_after: Option<Duration>) -> Duration {
        if self.respect_retry_after {
            if let Some(ra) = retry_after {
                return ra.min(Duration::from_millis(self.max_delay_ms));
            }
        }
        let raw = self.base_delay_ms as f64
            * self.backoff_multiplier.powi(failed_attempt.saturating_sub(1) as i32);
        let capped = (raw as u64).min(self.max_delay_ms);
        match self.jitter {
            Jitter::None => Duration::from_millis(capped),
            Jitter::Full => {
                use rand::Rng;
                Duration::from_millis(rand::thread_rng().gen_range(0..=capped))
            }
        }
    }
}

/// Parse a `Retry-After` header value (seconds or HTTP date).
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    if let Ok(secs) = trimmed.parse::<f64>() {
        if secs.is_finite() && secs >= 0.0 {
            return Some(Duration::from_millis((secs * 1000.0) as u64));
        }
        return None;
    }
    let date = time::OffsetDateTime::parse(
        trimmed,
        &time::format_description::well_known::Rfc2822,
    )
    .ok()?;
    let now = time::OffsetDateTime::now_utc();
    if date > now {
        Some(Duration::try_from(date - now).unwrap_or(Duration::ZERO))
    } else {
        Some(Duration::ZERO)
    }
}

/// Drive an attempt function under a retry policy, gated on the target's
/// effect class. The attempt closure receives the 1-based attempt number and
/// produces a response `Message` or a transport-level `RsError`.
///
/// Retried-on conditions: retryable status in the policy list, or a
/// transport error marked retryable when `retryOnNetworkError` is set.
pub async fn retry_request<F, Fut>(
    policy: &RetryPolicy,
    effect: EffectClass,
    has_idempotency_key: bool,
    mut attempt_fn: F,
) -> Result<Message, RsError>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<Message, RsError>>,
{
    let max_attempts = if policy.enabled && effect.permits_retry(has_idempotency_key) {
        policy.max_attempts.max(1)
    } else {
        1
    };
    let mut attempt = 1u32;
    loop {
        match attempt_fn(attempt).await {
            Ok(resp) => {
                let status = resp.status.map(|s| s.as_u16()).unwrap_or(200);
                if attempt >= max_attempts || !policy.retryable_status(status) {
                    return Ok(resp);
                }
                let retry_after = resp
                    .header("retry-after")
                    .and_then(parse_retry_after);
                tokio::time::sleep(policy.delay(attempt, retry_after)).await;
            }
            Err(err) => {
                let transport_retryable =
                    policy.enabled && policy.retry_on_network_error && err.retryable;
                if attempt >= max_attempts || !transport_retryable {
                    return Err(err);
                }
                tokio::time::sleep(policy.delay(attempt, None)).await;
            }
        }
        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, StatusCode};
    use std::sync::atomic::{AtomicU32, Ordering};

    fn resp(status: u16) -> Message {
        let req = Message::request(Method::GET, "/x", "t1");
        req.response(StatusCode::from_u16(status).unwrap(), None)
    }

    #[test]
    fn effect_defaults_follow_method() {
        assert_eq!(EffectClass::default_for_method(&Method::GET), EffectClass::Pure);
        assert_eq!(EffectClass::default_for_method(&Method::PUT), EffectClass::Idempotent);
        assert_eq!(EffectClass::default_for_method(&Method::POST), EffectClass::Unsafe);
    }

    #[test]
    fn resolution_chain_prefers_first_present() {
        let mount = RetryPolicy { max_attempts: 2, ..Default::default() };
        let resolved = RetryPolicy::resolve(&[None, Some(&mount), None]);
        assert_eq!(resolved.max_attempts, 2);
        // Nothing configured → no retry.
        assert_eq!(RetryPolicy::resolve(&[None, None]).max_attempts, 1);
    }

    #[test]
    fn delay_backs_off_and_respects_retry_after() {
        let p = RetryPolicy { jitter: Jitter::None, ..Default::default() };
        assert_eq!(p.delay(1, None), Duration::from_millis(250));
        assert_eq!(p.delay(2, None), Duration::from_millis(500));
        assert_eq!(p.delay(10, None), Duration::from_millis(5000)); // capped
        assert_eq!(p.delay(1, Some(Duration::from_secs(1))), Duration::from_secs(1));
        assert_eq!(p.delay(1, Some(Duration::from_secs(60))), Duration::from_millis(5000));
    }

    #[tokio::test]
    async fn retries_retryable_status_until_success() {
        let policy = RetryPolicy {
            base_delay_ms: 1,
            max_delay_ms: 1,
            jitter: Jitter::None,
            ..Default::default()
        };
        let calls = AtomicU32::new(0);
        let out = retry_request(&policy, EffectClass::Pure, false, |_| {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(if n < 3 { resp(503) } else { resp(200) }) }
        })
        .await
        .unwrap();
        assert_eq!(out.status, Some(StatusCode::OK));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn unsafe_effect_is_never_retried() {
        let policy = RetryPolicy { base_delay_ms: 1, jitter: Jitter::None, ..Default::default() };
        let calls = AtomicU32::new(0);
        let out = retry_request(&policy, EffectClass::Unsafe, false, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(resp(503)) }
        })
        .await
        .unwrap();
        assert_eq!(out.status.unwrap().as_u16(), 503);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn keyed_effect_retries_only_with_key() {
        let policy = RetryPolicy { base_delay_ms: 1, jitter: Jitter::None, ..Default::default() };
        let calls = AtomicU32::new(0);
        let _ = retry_request(&policy, EffectClass::Keyed, true, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(resp(503)) }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), policy.max_attempts);
    }
}
