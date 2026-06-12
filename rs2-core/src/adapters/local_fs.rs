//! Local filesystem [`FileStore`]: streamed reads and writes, range support,
//! paginated listings. Layout: `<root>/<tenant>/<path>` — the tenant prefix
//! is applied here from the host-supplied tenant, never from request data.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::StreamExt;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::capabilities::{ByteRange, DirEntry, FileMeta, FileStore};
use crate::error::RsError;
use crate::message::{Body, MediaType, Provenance};
use crate::router::validate_path;

pub struct LocalFsFileStore {
    root: PathBuf,
}

impl LocalFsFileStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        LocalFsFileStore { root: root.into() }
    }

    fn resolve(&self, tenant: &str, path: &str) -> Result<PathBuf, RsError> {
        // Defense in depth: the router already validated, but an adapter
        // must never trust its caller with traversal.
        validate_path(path)?;
        if tenant.is_empty() || tenant.contains(['/', '\\', '.']) {
            return Err(RsError::internal("invalid tenant id for file store"));
        }
        let mut full = self.root.join(tenant);
        for seg in path.split('/').filter(|s| !s.is_empty()) {
            full.push(seg);
        }
        Ok(full)
    }

    async fn meta_of(path: &Path) -> Result<FileMeta, RsError> {
        let md = tokio::fs::metadata(path).await?;
        Ok(FileMeta {
            size: md.len(),
            last_modified: md.modified().ok().map(OffsetDateTime::from),
            is_dir: md.is_dir(),
        })
    }
}

#[async_trait]
impl FileStore for LocalFsFileStore {
    async fn head(&self, tenant: &str, path: &str) -> Result<FileMeta, RsError> {
        let full = self.resolve(tenant, path)?;
        Self::meta_of(&full).await
    }

    async fn read(&self, tenant: &str, path: &str, range: Option<ByteRange>) -> Result<Body, RsError> {
        let full = self.resolve(tenant, path)?;
        let md = tokio::fs::metadata(&full).await?;
        if md.is_dir() {
            return Err(RsError::bad_request("path is a directory"));
        }
        let mut file = tokio::fs::File::open(&full).await?;
        let total = md.len();
        let (start, len) = match range {
            None => (0, total),
            Some(r) => {
                let start = r.start.min(total);
                let end = r.end.map(|e| (e + 1).min(total)).unwrap_or(total);
                if start >= end {
                    return Err(RsError::new(
                        416,
                        crate::error::codes::BAD_REQUEST,
                        "Range Not Satisfiable",
                        format!("range start {start} beyond resource size {total}"),
                    ));
                }
                file.seek(SeekFrom::Start(start)).await?;
                (start, end - start)
            }
        };
        let _ = start;
        let limited = file.take(len);
        let stream = futures::stream::unfold(limited, |mut rdr| async move {
            let mut buf = vec![0u8; 64 * 1024];
            match rdr.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok(bytes::Bytes::from(buf)), rdr))
                }
                Err(e) => Some((Err(e), rdr)),
            }
        })
        .boxed();
        // Content-version ETag from mtime+size enables Replayable provenance.
        let modified = md.modified().ok().map(OffsetDateTime::from);
        let version = format!(
            "{}-{}",
            modified.map(|m| m.unix_timestamp_nanos()).unwrap_or(0),
            total
        );
        let mut body = Body::from_stream(
            stream,
            MediaType::for_path(path),
            Some(len),
            Provenance::Replayable { url: path.to_string(), version },
        );
        if let Some(m) = modified {
            body = body.with_last_modified(m);
        }
        Ok(body)
    }

    async fn write(&self, tenant: &str, path: &str, body: Body) -> Result<bool, RsError> {
        let full = self.resolve(tenant, path)?;
        if let Some(parent) = full.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let existed = tokio::fs::metadata(&full).await.is_ok();
        // Write to a temp file then rename, so a failed streamed upload
        // never leaves a truncated resource visible.
        let tmp = full.with_extension(format!("rs2tmp-{}", uuid::Uuid::new_v4().simple()));
        let mut file = tokio::fs::File::create(&tmp).await?;
        let mut stream = body.into_stream();
        let result: Result<(), RsError> = async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| RsError::internal(format!("body stream error: {e}")))?;
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            Ok(())
        }
        .await;
        drop(file);
        if let Err(e) = result {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
        // On Windows rename-over-existing fails; remove the target first.
        if existed {
            let _ = tokio::fs::remove_file(&full).await;
        }
        tokio::fs::rename(&tmp, &full).await?;
        Ok(!existed)
    }

    async fn delete(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        let full = self.resolve(tenant, path)?;
        let md = tokio::fs::metadata(&full).await?;
        if md.is_dir() {
            return Err(RsError::bad_request("path is a directory; use directory delete"));
        }
        tokio::fs::remove_file(&full).await?;
        Ok(())
    }

    async fn delete_dir(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        let full = self.resolve(tenant, path)?;
        // remove_dir only removes empty directories, matching the contract.
        tokio::fs::remove_dir(&full).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                RsError::not_found("directory does not exist")
            } else {
                RsError::conflict("directory is not empty")
            }
        })
    }

    async fn delete_dir_all(&self, tenant: &str, path: &str) -> Result<(), RsError> {
        if path.split('/').all(|s| s.is_empty()) {
            return Err(RsError::bad_request("refusing to recursively delete the store root"));
        }
        let full = self.resolve(tenant, path)?;
        tokio::fs::remove_dir_all(&full).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                RsError::not_found("directory does not exist")
            } else {
                RsError::internal(format!("recursive delete failed: {e}"))
            }
        })
    }

    async fn list(&self, tenant: &str, path: &str, take: usize, skip: usize) -> Result<(Vec<DirEntry>, u64), RsError> {
        let full = self.resolve(tenant, path)?;
        let mut rd = match tokio::fs::read_dir(&full).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // The tenant root not existing yet is an empty listing, not a 404.
                if path.trim_matches('/').is_empty() {
                    return Ok((vec![], 0));
                }
                return Err(RsError::not_found("directory does not exist"));
            }
            Err(e) => return Err(e.into()),
        };
        let mut entries = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.contains(".rs2tmp-") {
                continue;
            }
            let md = entry.metadata().await?;
            let is_dir = md.is_dir();
            entries.push(DirEntry {
                name: if is_dir { format!("{name}/") } else { name },
                size: if is_dir { 0 } else { md.len() },
                last_modified: md
                    .modified()
                    .ok()
                    .map(OffsetDateTime::from)
                    .and_then(|t| t.format(&time::format_description::well_known::Rfc3339).ok()),
                dir: is_dir,
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let total = entries.len() as u64;
        let page = entries.into_iter().skip(skip).take(take).collect();
        Ok((page, total))
    }
}
