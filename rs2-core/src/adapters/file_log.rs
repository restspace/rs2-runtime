//! Local-file [`LogStore`]: per-tenant NDJSON with a background writer and a
//! backward-scan reader (PRD §14). Each tenant's records land in
//! `<root>/<tenant>.ndjson`, one OTLP-JSON [`LogRecord`] per line, rotated by
//! size with `N` numbered backups. `emit` never touches disk on the caller's
//! thread: records cross a bounded channel to a single background task, so the
//! request path pays only a channel send (drop-on-full under saturation).
//!
//! Constructing a `FileLogStore` requires a running Tokio runtime (it spawns
//! the writer task) — the server builds it inside `run()`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use crate::error::RsError;
use crate::logging::{LogQuery, LogRecord, LogSink, LogStore};

/// Bounded handoff queue depth. Beyond this, `emit` drops (and counts) rather
/// than block the request path.
const CHANNEL_CAP: usize = 4096;

const DEFAULT_MAX_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_BACKUPS: usize = 5;

enum LogMsg {
    Record(Box<LogRecord>),
    /// Flush barrier: the writer drains everything ahead of this marker, then
    /// replies (FIFO guarantees prior records are written).
    Flush(oneshot::Sender<()>),
}

pub struct FileLogStore {
    tx: mpsc::Sender<LogMsg>,
    dropped: Arc<AtomicU64>,
    root: PathBuf,
    backups: usize,
}

impl FileLogStore {
    /// Open a store under `root` with explicit rotation knobs. Spawns the
    /// background writer (needs a Tokio runtime).
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64, backups: usize) -> FileLogStore {
        let root = root.into();
        std::fs::create_dir_all(&root).ok();
        let (tx, rx) = mpsc::channel(CHANNEL_CAP);
        let dropped = Arc::new(AtomicU64::new(0));
        let writer = Writer {
            root: root.clone(),
            max_bytes,
            backups,
            files: HashMap::new(),
            dropped: dropped.clone(),
            last_reported: 0,
        };
        tokio::spawn(writer.run(rx));
        FileLogStore {
            tx,
            dropped,
            root,
            backups,
        }
    }

    /// Open a store under `root` with default rotation (8 MiB × 5 backups).
    pub fn with_defaults(root: impl Into<PathBuf>) -> FileLogStore {
        Self::new(root, DEFAULT_MAX_BYTES, DEFAULT_BACKUPS)
    }

    /// Records dropped under write pressure since startup (diagnostics/tests).
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl LogSink for FileLogStore {
    fn emit(&self, record: LogRecord) {
        if self.tx.try_send(LogMsg::Record(Box::new(record))).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl LogStore for FileLogStore {
    async fn query(&self, tenant: &str, q: &LogQuery) -> Result<Vec<LogRecord>, RsError> {
        if q.take == 0 {
            return Ok(Vec::new());
        }
        let name = sanitize(tenant);
        let mut out: Vec<LogRecord> = Vec::new();
        // Newest file first: current, then .1, .2, … (each read newest-line
        // first). Stop once `take` matches are collected.
        let mut paths = vec![self.root.join(format!("{name}.ndjson"))];
        for i in 1..=self.backups {
            paths.push(self.root.join(format!("{name}.ndjson.{i}")));
        }
        for path in paths {
            if out.len() >= q.take {
                break;
            }
            let content = match tokio::fs::read_to_string(&path).await {
                Ok(c) => c,
                Err(_) => continue, // missing backup / no logs yet
            };
            for line in content.lines().rev() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Some(rec) = LogRecord::from_ndjson_line(tenant, line) {
                    if q.matches(&rec) {
                        out.push(rec);
                        if out.len() >= q.take {
                            break;
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(LogMsg::Flush(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }
}

/// File-name component for a tenant. Tenants are host-controlled, but a stray
/// separator or dot would let one write to another's file (or a backup name),
/// so collapse them defensively.
fn sanitize(tenant: &str) -> String {
    if tenant.is_empty() {
        return "_invalid".to_string();
    }
    if tenant.chars().any(|c| matches!(c, '/' | '\\' | '.')) {
        tenant
            .chars()
            .map(|c| {
                if matches!(c, '/' | '\\' | '.') {
                    '_'
                } else {
                    c
                }
            })
            .collect()
    } else {
        tenant.to_string()
    }
}

struct FileHandle {
    file: tokio::fs::File,
    size: u64,
}

struct Writer {
    root: PathBuf,
    max_bytes: u64,
    backups: usize,
    files: HashMap<String, FileHandle>,
    dropped: Arc<AtomicU64>,
    last_reported: u64,
}

impl Writer {
    async fn run(mut self, mut rx: mpsc::Receiver<LogMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                LogMsg::Record(rec) => {
                    self.write_record(&rec).await;
                    self.report_drops();
                }
                LogMsg::Flush(reply) => {
                    self.flush_all().await;
                    let _ = reply.send(());
                }
            }
        }
    }

    async fn write_record(&mut self, rec: &LogRecord) {
        let name = sanitize(&rec.tenant);
        let line = format!("{}\n", rec.to_otlp_json());
        let bytes = line.as_bytes();

        if !self.files.contains_key(&name) {
            let path = self.root.join(format!("{name}.ndjson"));
            match open_append(&path).await {
                Ok(fh) => {
                    self.files.insert(name.clone(), fh);
                }
                Err(e) => {
                    eprintln!("rs2 logging: cannot open {}: {e}", path.display());
                    return;
                }
            }
        }
        let fh = self.files.get_mut(&name).expect("just inserted");
        if fh.file.write_all(bytes).await.is_err() {
            return;
        }
        // Flush so a concurrent reader sees the line promptly; this is the
        // background task, not the request path.
        let _ = fh.file.flush().await;
        fh.size += bytes.len() as u64;
        if fh.size >= self.max_bytes {
            self.files.remove(&name); // close before renaming
            self.rotate(&name).await;
        }
    }

    /// Size-based rotation: drop the oldest backup, shift the rest up, move the
    /// live file to `.1`. The next write reopens a fresh live file.
    async fn rotate(&self, name: &str) {
        if self.backups == 0 {
            tokio::fs::remove_file(self.root.join(format!("{name}.ndjson")))
                .await
                .ok();
            return;
        }
        let oldest = self.root.join(format!("{name}.ndjson.{}", self.backups));
        tokio::fs::remove_file(&oldest).await.ok();
        for i in (1..self.backups).rev() {
            let from = self.root.join(format!("{name}.ndjson.{i}"));
            let to = self.root.join(format!("{name}.ndjson.{}", i + 1));
            tokio::fs::rename(&from, &to).await.ok();
        }
        let from = self.root.join(format!("{name}.ndjson"));
        let to = self.root.join(format!("{name}.ndjson.1"));
        tokio::fs::rename(&from, &to).await.ok();
    }

    async fn flush_all(&mut self) {
        for fh in self.files.values_mut() {
            let _ = fh.file.flush().await;
        }
    }

    /// On growth of the dropped counter, warn the operator (stderr). Drops are
    /// cross-tenant and only happen under sustained write saturation.
    fn report_drops(&mut self) {
        let total = self.dropped.load(Ordering::Relaxed);
        if total > self.last_reported {
            let n = total - self.last_reported;
            self.last_reported = total;
            eprintln!(
                "rs2 logging: dropped {n} log record(s) under write pressure (total {total})"
            );
        }
    }
}

async fn open_append(path: &Path) -> std::io::Result<FileHandle> {
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let size = file.metadata().await?.len();
    Ok(FileHandle { file, size })
}
