//! Body: stream or bytes, always media-typed, carrying provenance (PRD §6.2–6.3).

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
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
        serde_json::from_slice(strip_bom(bytes))
            .map_err(|e| RsError::bad_request(format!("invalid JSON body: {e}")))
    }

    /// The media-type-directed conversion every body-consuming boundary uses
    /// (Restspace v1 `MessageBody.asJson`): JSON parses to a value, text
    /// becomes a string, and anything else becomes a base64 string. The result
    /// is always a valid JSON value, so a transform or a code service never
    /// fails purely because the body was not JSON, and binary bodies survive
    /// the crossing intact instead of being mangled by lossy UTF-8 decoding.
    pub async fn as_any(&mut self, max_bytes: u64) -> Result<serde_json::Value, RsError> {
        let is_json = self.media_type.is_json();
        let is_text = self.media_type.is_text();
        let bytes = self.materialize(max_bytes).await?;
        let slice = strip_bom(bytes);
        if is_json {
            // A body typed as JSON but holding something else is the one case
            // that still fails loudly: it is a producer bug, not a shape the
            // pipeline should paper over.
            return serde_json::from_slice(slice)
                .map_err(|e| RsError::bad_request(format!("invalid JSON body: {e}")));
        }
        if is_text {
            return match std::str::from_utf8(slice) {
                Ok(text) => Ok(serde_json::Value::String(text.to_owned())),
                // Declared text but not decodable: fall back to base64 rather
                // than losing bytes to replacement characters.
                Err(_) => Ok(serde_json::Value::String(B64.encode(slice))),
            };
        }
        Ok(serde_json::Value::String(B64.encode(slice)))
    }

    /// The raw payload rendered as a string, the way v1's `asString` did it:
    /// the UTF-8 text when the bytes decode, standard base64 when they do
    /// not. Unlike `as_any` it never parses JSON — it is the byte-faithful
    /// view a signature is computed over, so a lossy decode here would
    /// silently break HMAC verification on a non-UTF-8 payload.
    pub async fn as_raw_string(&mut self, max_bytes: u64) -> Result<String, RsError> {
        let bytes = self.materialize(max_bytes).await?;
        Ok(match std::str::from_utf8(bytes) {
            Ok(text) => text.to_owned(),
            Err(_) => B64.encode(bytes),
        })
    }

    /// Consume the body as a stream regardless of representation.
    pub fn into_stream(self) -> ByteStream {
        match self.payload {
            Payload::Stream(s) => s,
            Payload::Bytes(b) => futures::stream::once(async move { Ok(b) }).boxed(),
        }
    }
}

/// Strip a leading UTF-8 BOM, which bodies read from files often carry.
fn strip_bom(bytes: &[u8]) -> &[u8] {
    bytes.strip_prefix(&[0xEF, 0xBB, 0xBF][..]).unwrap_or(bytes)
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

    /// The three-way conversion Restspace v1 did in `MessageBody.asJson`:
    /// JSON parses, text is a string, binary is base64.
    #[tokio::test]
    async fn as_any_converts_by_media_type() {
        let mut json = Body::from_string(r#"{"a":1}"#, MediaType::json());
        assert_eq!(json.as_any(1024).await.unwrap()["a"], 1);

        let mut dir = Body::from_string("[1,2]", MediaType::dir_json());
        assert_eq!(dir.as_any(1024).await.unwrap(), serde_json::json!([1, 2]));

        let mut html = Body::from_string("<p>hi</p>", MediaType::new("text/html"));
        assert_eq!(html.as_any(1024).await.unwrap(), "<p>hi</p>");

        let mut xml = Body::from_string("<a/>", MediaType::new("application/xml"));
        assert_eq!(xml.as_any(1024).await.unwrap(), "<a/>");

        // Binary survives as base64 rather than being mangled into U+FFFD.
        const PNG: [u8; 8] = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let mut bin = Body::from_bytes(Bytes::from_static(&PNG), MediaType::octet_stream());
        let encoded = bin.as_any(1024).await.unwrap();
        assert_eq!(encoded, "iVBORw0KGgo=");
        assert_eq!(B64.decode(encoded.as_str().unwrap()).unwrap(), PNG);
    }

    /// A body typed text but holding undecodable bytes falls back to base64
    /// instead of losing them to replacement characters.
    #[tokio::test]
    async fn as_any_falls_back_for_undecodable_text() {
        let mut body =
            Body::from_bytes(Bytes::from_static(&[0xFF, 0xFE]), MediaType::new("text/plain"));
        let v = body.as_any(1024).await.unwrap();
        assert_eq!(B64.decode(v.as_str().unwrap()).unwrap(), [0xFF, 0xFE]);
    }

    /// A body that claims JSON but is not stays a hard error — that is a
    /// producer bug, not a shape to paper over.
    #[tokio::test]
    async fn as_any_rejects_malformed_json() {
        let mut body = Body::from_string("not json", MediaType::json());
        let err = body.as_any(1024).await.unwrap_err();
        assert!(err.detail.contains("invalid JSON body"), "{}", err.detail);
    }

    #[tokio::test]
    async fn as_any_strips_a_bom() {
        let mut body = Body::from_bytes(
            Bytes::from_static("\u{feff}{\"a\":1}".as_bytes()),
            MediaType::json(),
        );
        assert_eq!(body.as_any(1024).await.unwrap()["a"], 1);

        let mut text = Body::from_bytes(
            Bytes::from_static("\u{feff}hello".as_bytes()),
            MediaType::new("text/plain"),
        );
        assert_eq!(text.as_any(1024).await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn as_any_honours_the_materialization_cap() {
        let mut body = Body::from_stream(
            chunked(vec![b"0123456789", b"0123456789"]),
            MediaType::octet_stream(),
            None,
            Provenance::Ephemeral,
        );
        let err = body.as_any(15).await.unwrap_err();
        assert_eq!(err.code, crate::error::codes::LIMIT_EXCEEDED);
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
