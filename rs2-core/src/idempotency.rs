//! Idempotency keys and response replay (PRD §7.2).
//!
//! The idempotency store is a capability: the default in-memory adapter
//! suits single-node deployments and tests; shared adapters (Redis, the
//! data store) make scale-out behave correctly.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::error::RsError;
use crate::message::{Body, MediaType, Message};

/// Max accepted key length (PRD §7.2: opaque ≤256 chars).
pub const MAX_KEY_LEN: usize = 256;

/// Default replay window (PRD §7.2: 24 h).
pub const DEFAULT_REPLAY_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// Default cap on stored response bodies; larger bodies are not replayed
/// (the PRD stores them by reference in the blob store — v2 here).
pub const DEFAULT_BODY_CAP: u64 = 1024 * 1024;

/// A recorded response: status, headers, body bytes up to the size cap.
#[derive(Debug, Clone)]
pub struct StoredResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Option<(Bytes, String)>,
}

impl StoredResponse {
    /// Rebuild a response `Message` from the stored form, derived from the
    /// incoming duplicate's context, marked `Idempotency-Replayed: true`.
    pub fn into_message(self, template: &Message) -> Message {
        let status = http::StatusCode::from_u16(self.status)
            .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
        let body = self.body.map(|(bytes, mt)| {
            Body::from_bytes(bytes, MediaType::parse(&mt))
        });
        let mut resp = template.response(status, body);
        for (k, v) in &self.headers {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::try_from(k.as_str()),
                http::HeaderValue::from_str(v),
            ) {
                resp.headers.insert(name, value);
            }
        }
        resp.set_header("idempotency-replayed", "true");
        resp
    }
}

/// Outcome of registering a key before executing.
pub enum Begin {
    /// First arrival: execute, then `complete` (or `abandon` on host error).
    Fresh,
    /// Original still executing: respond 409 + Retry-After.
    InFlight,
    /// Duplicate within the window: replay the stored response.
    Replay(StoredResponse),
    /// Same key, different request payload: 422 `idempotency_key_reuse`.
    PayloadMismatch,
}

#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    async fn begin(&self, scope: &str, key: &str, payload_hash: Option<&str>) -> Result<Begin, RsError>;
    async fn complete(&self, scope: &str, key: &str, response: StoredResponse) -> Result<(), RsError>;
    /// Drop an in-flight registration (host-side failure: nothing to replay).
    async fn abandon(&self, scope: &str, key: &str) -> Result<(), RsError>;
}

enum EntryState {
    InFlight,
    Done(StoredResponse, Instant),
}

struct Entry {
    payload_hash: Option<String>,
    state: EntryState,
}

/// Sweep cadence: every N `begin` calls, drop all expired entries. Expiry is
/// otherwise only opportunistic on the addressed slot, so distinct keys —
/// scope includes the request path — would accumulate for the process
/// lifetime, each holding up to `DEFAULT_BODY_CAP` of stored response.
const SWEEP_EVERY: u64 = 4096;

/// Hard cap on retained entries: past it, the oldest completed entries are
/// evicted early. A duplicate whose entry was evicted re-executes instead of
/// replaying — safe for idempotent effects, and strictly better than
/// unbounded growth. In-flight entries are never evicted.
const MAX_ENTRIES: usize = 100_000;

/// In-memory idempotency store with a fixed replay window.
pub struct MemIdempotencyStore {
    window: Duration,
    max_entries: usize,
    entries: Mutex<HashMap<(String, String), Entry>>,
    begins: std::sync::atomic::AtomicU64,
}

impl MemIdempotencyStore {
    pub fn new(window: Duration) -> Self {
        Self::with_capacity(window, MAX_ENTRIES)
    }

    fn with_capacity(window: Duration, max_entries: usize) -> Self {
        MemIdempotencyStore {
            window,
            max_entries,
            entries: Mutex::new(HashMap::new()),
            begins: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Drop every expired `Done` entry, then if still over the cap, evict the
    /// oldest completed entries in a batch (an eighth of the cap, so the
    /// O(n log n) scan amortizes instead of running per insert at saturation).
    fn sweep(&self, entries: &mut HashMap<(String, String), Entry>, force_evict: bool) {
        let window = self.window;
        entries.retain(|_, e| match &e.state {
            EntryState::InFlight => true,
            EntryState::Done(_, at) => at.elapsed() <= window,
        });
        if force_evict && entries.len() >= self.max_entries {
            let mut done: Vec<((String, String), Instant)> = entries
                .iter()
                .filter_map(|(k, e)| match &e.state {
                    EntryState::Done(_, at) => Some((k.clone(), *at)),
                    EntryState::InFlight => None,
                })
                .collect();
            done.sort_by_key(|(_, at)| *at);
            for (k, _) in done.into_iter().take(self.max_entries / 8 + 1) {
                entries.remove(&k);
            }
        }
    }
}

impl Default for MemIdempotencyStore {
    fn default() -> Self {
        Self::new(DEFAULT_REPLAY_WINDOW)
    }
}

#[async_trait]
impl IdempotencyStore for MemIdempotencyStore {
    async fn begin(&self, scope: &str, key: &str, payload_hash: Option<&str>) -> Result<Begin, RsError> {
        let n = self.begins.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut entries = self.entries.lock().unwrap();
        if n % SWEEP_EVERY == 0 || entries.len() >= self.max_entries {
            self.sweep(&mut entries, true);
        }
        // Opportunistic expiry of the addressed slot.
        let slot = (scope.to_string(), key.to_string());
        if let Some(entry) = entries.get(&slot) {
            if let EntryState::Done(_, at) = &entry.state {
                if at.elapsed() > self.window {
                    entries.remove(&slot);
                }
            }
        }
        match entries.get(&slot) {
            None => {
                entries.insert(
                    slot,
                    Entry { payload_hash: payload_hash.map(String::from), state: EntryState::InFlight },
                );
                Ok(Begin::Fresh)
            }
            Some(entry) => {
                // Hash comparison only when both sides have one (streamed
                // request bodies are not hashed).
                if let (Some(stored), Some(incoming)) = (&entry.payload_hash, payload_hash) {
                    if stored != incoming {
                        return Ok(Begin::PayloadMismatch);
                    }
                }
                match &entry.state {
                    EntryState::InFlight => Ok(Begin::InFlight),
                    EntryState::Done(resp, _) => Ok(Begin::Replay(resp.clone())),
                }
            }
        }
    }

    async fn complete(&self, scope: &str, key: &str, response: StoredResponse) -> Result<(), RsError> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(&(scope.to_string(), key.to_string())) {
            entry.state = EntryState::Done(response, Instant::now());
        }
        Ok(())
    }

    async fn abandon(&self, scope: &str, key: &str) -> Result<(), RsError> {
        self.entries.lock().unwrap().remove(&(scope.to_string(), key.to_string()));
        Ok(())
    }
}

/// Scope a key per tenant + mount + method + path (PRD §7.2).
pub fn scope_for(msg: &Message, mount_base: &str) -> String {
    format!("{}|{}|{}|{}", msg.tenant, mount_base, msg.method, msg.url.path)
}

/// SHA-256 hex of a materialized request payload; `None` for streams.
pub fn payload_hash(msg: &Message) -> Option<String> {
    match &msg.body {
        Some(body) => match &body.payload {
            crate::message::Payload::Bytes(b) => {
                let mut hasher = Sha256::new();
                hasher.update(b);
                Some(hex(&hasher.finalize()))
            }
            crate::message::Payload::Stream(_) => None,
        },
        None => Some("empty".to_string()),
    }
}

/// Deterministic per-segment idempotency key (PRD §7.3):
/// `H(pipeline-invocation-id, segment-index)` — identical on every retry of
/// the same segment, so keyed effects inside it dedupe across attempts.
pub fn segment_key(invocation_id: &str, segment_index: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(invocation_id.as_bytes());
    hasher.update([0u8]);
    hasher.update(segment_index.to_le_bytes());
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Capture a response into stored form, materializing its body up to
/// `body_cap`. Returns the (possibly materialized) response and the stored
/// form, or `None` when the body is too large to record.
pub async fn capture_response(
    mut resp: Message,
    body_cap: u64,
) -> (Message, Option<StoredResponse>) {
    let headers: Vec<(String, String)> = resp
        .headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    let status = resp.status.map(|s| s.as_u16()).unwrap_or(200);
    let body = match &mut resp.body {
        None => None,
        Some(b) => {
            let mt = b.media_type.to_string();
            match b.materialize(body_cap).await {
                Ok(bytes) => Some((bytes.clone(), mt)),
                // Too large (or unreadable) to record: skip storage so a
                // duplicate re-executes rather than replaying wrongly.
                Err(_) => return (resp, None),
            }
        }
    };
    let stored = StoredResponse { status, headers, body };
    (resp, Some(stored))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fresh_then_replay_then_mismatch() {
        let store = MemIdempotencyStore::default();
        assert!(matches!(store.begin("s", "k1", Some("h1")).await.unwrap(), Begin::Fresh));
        assert!(matches!(store.begin("s", "k1", Some("h1")).await.unwrap(), Begin::InFlight));
        store
            .complete("s", "k1", StoredResponse { status: 201, headers: vec![], body: None })
            .await
            .unwrap();
        match store.begin("s", "k1", Some("h1")).await.unwrap() {
            Begin::Replay(r) => assert_eq!(r.status, 201),
            _ => panic!("expected replay"),
        }
        assert!(matches!(
            store.begin("s", "k1", Some("other")).await.unwrap(),
            Begin::PayloadMismatch
        ));
        // Different scope is independent.
        assert!(matches!(store.begin("s2", "k1", Some("h1")).await.unwrap(), Begin::Fresh));
    }

    #[tokio::test]
    async fn window_expiry_frees_the_key() {
        let store = MemIdempotencyStore::new(Duration::ZERO);
        assert!(matches!(store.begin("s", "k", None).await.unwrap(), Begin::Fresh));
        store
            .complete("s", "k", StoredResponse { status: 200, headers: vec![], body: None })
            .await
            .unwrap();
        // Window elapsed (zero): fresh again, not a replay.
        assert!(matches!(store.begin("s", "k", None).await.unwrap(), Begin::Fresh));
    }

    #[tokio::test]
    async fn completed_entries_are_evicted_at_the_cap() {
        let store = MemIdempotencyStore::with_capacity(Duration::from_secs(3600), 16);
        for i in 0..64 {
            let key = format!("k{i}");
            assert!(matches!(store.begin("s", &key, None).await.unwrap(), Begin::Fresh));
            store
                .complete("s", &key, StoredResponse { status: 200, headers: vec![], body: None })
                .await
                .unwrap();
        }
        // Unbounded before the cap landed: 64 distinct keys, none expired,
        // yet the map stays at the cap with the oldest entries evicted.
        assert!(store.entries.lock().unwrap().len() <= 16);
    }

    #[test]
    fn segment_keys_are_deterministic_and_distinct() {
        assert_eq!(segment_key("inv1", 0), segment_key("inv1", 0));
        assert_ne!(segment_key("inv1", 0), segment_key("inv1", 1));
        assert_ne!(segment_key("inv1", 0), segment_key("inv2", 0));
    }
}
