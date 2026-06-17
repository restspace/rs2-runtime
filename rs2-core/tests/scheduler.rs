//! Host scheduler (G1): a mount with `config.schedule` is fired on cadence by a
//! synthetic internal request, and firing is gated on the swappable
//! `ScheduleStore` (the HA seam). Uses short real intervals — the occurrence id
//! is wall-clock-derived (for cross-node determinism), so `tokio::time::pause`
//! can't drive it; the deterministic message-shape assertion is a unit test in
//! `scheduler.rs`. Fires are observed via the boundary log (every dispatch logs,
//! regardless of what the mount does).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::capabilities::{ByteRange, DirEntry, FileMeta, FileStore};
use rs2_core::logging::{LogQuery, LogRecord, LogSink, LogStore, Severity};
use rs2_core::message::Body;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::scheduler::ScheduleStore;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// Captures boundary-log message bodies (`"POST /job -> 404"` etc.).
#[derive(Clone, Default)]
struct RecordingLog {
    lines: Arc<Mutex<Vec<String>>>,
}

impl LogSink for RecordingLog {
    fn emit(&self, record: LogRecord) {
        self.lines.lock().unwrap().push(record.body);
    }
    fn enabled(&self) -> bool {
        true
    }
}

#[async_trait]
impl LogStore for RecordingLog {
    async fn query(&self, _tenant: &str, _q: &LogQuery) -> Result<Vec<LogRecord>, RsError> {
        Ok(vec![])
    }
}

impl RecordingLog {
    fn fires(&self) -> usize {
        self.lines.lock().unwrap().iter().filter(|l| l.contains("POST /job")).count()
    }
}

/// A `ScheduleStore` that always loses the claim (simulating another node that
/// already won), counting attempts.
#[derive(Clone, Default)]
struct DenyStore {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ScheduleStore for DenyStore {
    async fn claim(&self, _key: &str, _occ: i64, _ttl: Duration) -> Result<bool, RsError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(false)
    }
}

/// A `FileStore` that delays `list`/`read` (the pipeline's spec lookup on a
/// tick) and records the maximum number of concurrent gated calls — so the test
/// can prove the overlap guard never runs two fires of one mount at once.
struct SlowFileStore {
    inner: Arc<dyn FileStore>,
    delay: Duration,
    concurrent: Arc<AtomicUsize>,
    max_concurrent: Arc<AtomicUsize>,
}

impl SlowFileStore {
    async fn gate(&self) {
        let now = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
    }
    fn ungate(&self) {
        self.concurrent.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl FileStore for SlowFileStore {
    async fn list(
        &self,
        tenant: &str,
        path: &str,
        take: usize,
        skip: usize,
    ) -> Result<(Vec<DirEntry>, u64), RsError> {
        self.gate().await;
        let r = self.inner.list(tenant, path, take, skip).await;
        self.ungate();
        r
    }
    async fn read(
        &self,
        tenant: &str,
        path: &str,
        range: Option<ByteRange>,
    ) -> Result<Body, RsError> {
        self.gate().await;
        let r = self.inner.read(tenant, path, range).await;
        self.ungate();
        r
    }
    async fn head(&self, tenant: &str, path: &str) -> Result<FileMeta, RsError> {
        self.inner.head(tenant, path).await
    }
    async fn write(&self, tenant: &str, path: &str, body: Body) -> Result<bool, RsError> {
        self.inner.write(tenant, path, body).await
    }
    async fn delete(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete(tenant, path).await
    }
    async fn rename(&self, tenant: &str, from: &str, to: &str) -> Result<bool, RsError> {
        self.inner.rename(tenant, from, to).await
    }
    async fn delete_dir(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir(tenant, path).await
    }
    async fn delete_dir_all(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        self.inner.delete_dir_all(tenant, path).await
    }
}

struct Loader(Value);

#[async_trait]
impl ConfigLoader for Loader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
    async fn load_raw(&self, _tenant: &str) -> Result<(Value, String), RsError> {
        Ok((self.0.clone(), "v1".into()))
    }
}

/// A runtime with one `pipeline` mount at `/job` scheduled every 40ms. The
/// pipeline has no specs, so a tick 404s — but the dispatch still boundary-logs,
/// which is all we assert. Debug log floor so even a success would be recorded.
fn build_with(
    files: Arc<dyn FileStore>,
    rec: RecordingLog,
    schedule_store: Option<Arc<dyn ScheduleStore>>,
) -> Arc<Runtime> {
    let mut adapters = Adapters::new(files, Arc::new(MemDataStore::new()))
        .with_logging(Arc::new(rec), Severity::Debug);
    if let Some(store) = schedule_store {
        adapters = adapters.with_schedule_store(store);
    }
    let config = json!({
        "mounts": [
            { "path": "/job", "service": "pipeline", "config": { "schedule": { "every": "40ms" } } }
        ]
    });
    Runtime::new(
        Tenancy::Single { tenant: "t1".into() },
        adapters,
        Arc::new(Loader(config)),
        LimitTable::default(),
    )
}

fn build(
    file_root: &std::path::Path,
    rec: RecordingLog,
    schedule_store: Option<Arc<dyn ScheduleStore>>,
) -> Arc<Runtime> {
    build_with(Arc::new(LocalFsFileStore::new(file_root)), rec, schedule_store)
}

#[tokio::test]
async fn scheduler_fires_a_mount_on_cadence() {
    let dir = tempfile::tempdir().unwrap();
    let rec = RecordingLog::default();
    let rt = build(dir.path(), rec.clone(), None);
    rt.spawn_scheduler_with(Duration::from_millis(20), Duration::from_millis(20));

    tokio::time::sleep(Duration::from_millis(400)).await;

    let fires = rec.fires();
    assert!(fires >= 2, "expected repeated scheduled fires, got {fires}");
}

#[tokio::test]
async fn claim_denied_skips_firing() {
    let dir = tempfile::tempdir().unwrap();
    let rec = RecordingLog::default();
    let deny = DenyStore::default();
    let rt = build(dir.path(), rec.clone(), Some(Arc::new(deny.clone())));
    rt.spawn_scheduler_with(Duration::from_millis(20), Duration::from_millis(20));

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(deny.calls.load(Ordering::SeqCst) >= 1, "scheduler should attempt to claim");
    assert_eq!(rec.fires(), 0, "a lost claim must not fire");
}

#[tokio::test]
async fn overlap_guard_serializes_fires_of_one_mount() {
    let dir = tempfile::tempdir().unwrap();
    let rec = RecordingLog::default();
    // Each fire's spec lookup takes ~60ms — long enough that several 40ms due
    // ticks land while a fire is still running.
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));
    let slow: Arc<dyn FileStore> = Arc::new(SlowFileStore {
        inner: Arc::new(LocalFsFileStore::new(dir.path())),
        delay: Duration::from_millis(60),
        concurrent: concurrent.clone(),
        max_concurrent: max_concurrent.clone(),
    });
    let rt = build_with(slow, rec.clone(), None);
    rt.spawn_scheduler_with(Duration::from_millis(10), Duration::from_millis(10));

    tokio::time::sleep(Duration::from_millis(500)).await;

    // The slow path was exercised (fires happened), and the overlap guard kept
    // them from ever running concurrently — exactly one fire of /job at a time.
    let peak = max_concurrent.load(Ordering::SeqCst);
    assert!(peak >= 1, "expected the scheduled fire to exercise the (slow) store");
    assert_eq!(peak, 1, "overlap guard must serialize a mount's fires, saw {peak} concurrent");
    assert!(rec.fires() >= 1, "expected at least one completed fire");
}
