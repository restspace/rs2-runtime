//! Body: stream or bytes, always media-typed, carrying provenance (PRD §6.2–6.3).

use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::StreamExt;
use time::OffsetDateTime;

use super::media_type::MediaType;
use crate::error::RsError;

/// How a body could be reproduced — metadata only in v1, consumed by the
/// v2 durable journal (PRD §6.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provenance {
    /// Bytes held in memory; journal-eligible, snapshot directly.
    Materialized,
    /// Stream sourced from a versioned GET on an RS2 store; re-fetch by reference.
    Replayable { url: String, version: String },
    /// Stream with no replayable source (e.g. incoming request stream).
    Ephemeral,
}

pub type ByteStream = BoxStream<'static, Result<Bytes, std::io::Error>>;

pub enum Payload {
    Bytes(Bytes),
    Stream(ByteStream),
}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::Bytes(b) => write!(f, "Bytes({} bytes)", b.len()),
            Payload::Stream(_) => write!(f, "Stream"),
        }
    }
}

#[derive(Debug)]
pub struct Body {
    pub payload: Payload,
    pub media_type: MediaType,
    pub size: Option<u64>,
    pub last_modified: Option<OffsetDateTime>,
    pub provenance: Provenance,
}

impl Body {
    pub fn from_bytes(bytes: impl Into<Bytes>, media_type: MediaType) -> Self {
        let bytes = bytes.into();
        Body {
            size: Some(bytes.len() as u64),
            payload: Payload::Bytes(bytes),
            media_type,
            last_modified: None,
            provenance: Provenance::Materialized,
        }
    }

    pub fn from_string(text: impl Into<String>, media_type: MediaType) -> Self {
        Self::from_bytes(Bytes::from(text.into()), media_type)
    }

    pub fn from_json(value: &serde_json::Value) -> Self {
        Self::from_bytes(Bytes::from(value.to_string()), MediaType::json())
    }

    pub fn from_stream(
        stream: ByteStream,
        media_type: MediaType,
        size: Option<u64>,
        provenance: Provenance,
    ) -> Self {
        Body {
            payload: Payload::Stream(stream),
            media_type,
            size,
            last_modified: None,
            provenance,
        }
    }

    pub fn with_last_modified(mut self, when: OffsetDateTime) -> Self {
        self.last_modified = Some(when);
        self
    }

    pub fn with_schema(mut self, schema_url: impl Into<String>) -> Self {
        self.media_type = self.media_type.with_schema(schema_url);
        self
    }

    pub fn is_stream(&self) -> bool {
        matches!(self.payload, Payload::Stream(_))
    }

    /// Materialize the payload into bytes, enforcing the host size limit
    /// (PRD §9.3: bounded materialization; streamed bodies are unbounded
    /// only when they flow to stores without materializing).
    pub async fn materialize(&mut self, max_bytes: u64) -> Result<&Bytes, RsError> {
        if let Payload::Stream(stream) = &mut self.payload {
            // Reject early when the declared size already exceeds the cap.
            if let Some(size) = self.size {
                if size > max_bytes {
                    return Err(RsError::limit_exceeded(
                        "materialized_body_bytes",
                        size,
                        max_bytes,
                    ));
                }
            }
            let mut buf = BytesMut::new();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|e| RsError::internal(format!("body stream error: {e}")))?;
                if (buf.len() + chunk.len()) as u64 > max_bytes {
                    return Err(RsError::limit_exceeded(
                        "materialized_body_bytes",
                        (buf.len() + chunk.len()) as u64,
                        max_bytes,
                    ));
                }
                buf.extend_from_slice(&chunk);
            }
            self.size = Some(buf.len() as u64);
            self.payload = Payload::Bytes(buf.freeze());
            self.provenance = Provenance::Materialized;
        }
        match &self.payload {
            // The cap applies uniformly: bytes already in memory still may
            // not cross an engine boundary above the host limit.
            Payload::Bytes(b) if b.len() as u64 > max_bytes => Err(RsError::limit_exceeded(
                "materialized_body_bytes",
                b.len() as u64,
                max_bytes,
            )),
            Payload::Bytes(b) => Ok(b),
            Payload::Stream(_) => unreachable!(),
        }
    }

    /// Materialize and parse as JSON (only for JSON-family media types).
    pub async fn as_json(&mut self, max_bytes: u64) -> Result<serde_json::Value, RsError> {
        if !self.media_type.is_json() {
            return Err(RsError::bad_request(format!(
                "expected a JSON body, got '{}'",
                self.media_type.essence()
            )));
        }
        let bytes = self.materialize(max_bytes).await?;
        // Strip a UTF-8 BOM, which may appear on bodies read from files.
        let slice: &[u8] = if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
            &bytes[3..]
        } else {
            bytes
        };
        serde_json::from_slice(slice)
            .map_err(|e| RsError::bad_request(format!("invalid JSON body: {e}")))
    }

    /// Consume the body as a stream regardless of representation.
    pub fn into_stream(self) -> ByteStream {
        match self.payload {
            Payload::Stream(s) => s,
            Payload::Bytes(b) => futures::stream::once(async move { Ok(b) }).boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunked(chunks: Vec<&'static [u8]>) -> ByteStream {
        futures::stream::iter(chunks.into_iter().map(|c| Ok(Bytes::from_static(c)))).boxed()
    }

    #[tokio::test]
    async fn materializes_stream_within_limit() {
        let mut body = Body::from_stream(
            chunked(vec![b"{\"a\":", b"1}"]),
            MediaType::json(),
            None,
            Provenance::Ephemeral,
        );
        let v = body.as_json(1024).await.unwrap();
        assert_eq!(v["a"], 1);
        assert_eq!(body.provenance, Provenance::Materialized);
    }

    #[tokio::test]
    async fn enforces_materialization_cap() {
        let mut body = Body::from_stream(
            chunked(vec![b"0123456789", b"0123456789"]),
            MediaType::octet_stream(),
            None,
            Provenance::Ephemeral,
        );
        let err = body.materialize(15).await.unwrap_err();
        assert_eq!(err.code, crate::error::codes::LIMIT_EXCEEDED);
    }
}
