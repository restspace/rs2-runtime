//! Media-type-directed body conversion at pipeline and engine boundaries.
//!
//! Restspace v1 converted an incoming body for a transform by media type
//! (`MessageBody.asJson`): JSON parsed, text became a string, binary became
//! base64. RS2 classified the same three ways but only ever admitted JSON, so
//! a transform over an HTML or CSV body was a 400. These tests pin the v1
//! semantics at every boundary that consumes a body as a value.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use rs2_core::message::{Body, MediaType, Message, Source};
use rs2_core::pipeline::{Executor, PipelineLimits, PipelineSpec, Requester};
use rs2_core::retry::RetryPolicy;

struct NotCalled;

#[async_trait]
impl Requester for NotCalled {
    async fn request(&self, msg: Message) -> Message {
        panic!("unexpected call to {}", msg.url.path)
    }
}

/// Serves back whatever body the test asked for, so a `call | transform`
/// pipeline sees a non-JSON upstream response.
struct Serves(&'static str, &'static [u8]);

#[async_trait]
impl Requester for Serves {
    async fn request(&self, msg: Message) -> Message {
        let body = Body::from_bytes(self.1, MediaType::new(self.0));
        msg.response(StatusCode::OK, Some(body))
    }
}

fn executor(requester: Arc<dyn Requester>) -> Executor {
    Executor::new(requester, PipelineLimits::default(), RetryPolicy::default())
}

fn input(media_type: &str, bytes: &'static [u8]) -> Message {
    let mut msg = Message::request(Method::POST, "/run", "demo");
    msg.body = Some(Body::from_bytes(bytes, MediaType::new(media_type)));
    msg.source = Source::Internal;
    msg
}

async fn out_json(mut msg: Message) -> serde_json::Value {
    msg.body
        .as_mut()
        .expect("body")
        .as_json(10 * 1024 * 1024)
        .await
        .expect("json body")
}

/// A transform over a text body gets the string, not a 400. `$` is the whole
/// body, so this is the minimal proof the conversion happened.
#[tokio::test]
async fn transform_over_a_text_body_sees_a_string() {
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [ { "transform": { "page": "$", "len": "$length($)" } } ]
    }))
    .unwrap();
    let out = executor(Arc::new(NotCalled))
        .run(&spec, input("text/html", b"<p>hi</p>"), None)
        .await
        .expect("a text body is not an error");
    assert_eq!(out.status, Some(StatusCode::OK));
    let v = out_json(out).await;
    assert_eq!(v["page"], "<p>hi</p>");
    assert_eq!(v["len"], 9);
}

/// CSV is the case that motivated this: parse a text body inside JSONata.
#[tokio::test]
async fn transform_can_parse_a_csv_body() {
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [ { "transform": { "rows": "$count($split($, '\n'))" } } ]
    }))
    .unwrap();
    let out = executor(Arc::new(NotCalled))
        .run(&spec, input("text/csv", b"a,b\n1,2\n3,4"), None)
        .await
        .expect("a csv body is not an error");
    assert_eq!(out_json(out).await["rows"], 3);
}

/// A binary body arrives base64-encoded and round-trips exactly — the old
/// lossy UTF-8 decode replaced every non-UTF-8 byte with U+FFFD.
#[tokio::test]
async fn transform_over_a_binary_body_sees_lossless_base64() {
    const PNG: &[u8] = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0xFF, 0xD8];
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [ { "transform": { "b64": "$" } } ]
    }))
    .unwrap();
    let out = executor(Arc::new(NotCalled))
        .run(&spec, input("image/png", PNG), None)
        .await
        .expect("a binary body is not an error");
    let v = out_json(out).await;
    // Pinned literal, not just a round-trip: the Worker host asserts the same
    // string in `rs2-worker/test/body-conversion.test.ts`, which locks the two
    // hosts to one encoding (standard base64, padded).
    assert_eq!(v["b64"], "iVBORw0KGgr/2A==");
    assert_eq!(
        B64.decode(v["b64"].as_str().unwrap()).unwrap(),
        PNG,
        "binary must survive the crossing byte-for-byte"
    );
}

/// JSON keeps parsing to a value — the common path is unchanged.
#[tokio::test]
async fn transform_over_a_json_body_still_parses() {
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [ { "transform": { "doubled": "a * 2" } } ]
    }))
    .unwrap();
    let out = executor(Arc::new(NotCalled))
        .run(&spec, input("application/json", br#"{"a":21}"#), None)
        .await
        .unwrap();
    assert_eq!(out_json(out).await["doubled"], 42);
}

/// A body that claims JSON but is not remains a hard 400: that is a producer
/// bug, and silently handing the transform a string would hide it.
#[tokio::test]
async fn a_body_lying_about_being_json_is_still_rejected() {
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [ { "transform": { "x": "$" } } ]
    }))
    .unwrap();
    let err = executor(Arc::new(NotCalled))
        .run(&spec, input("application/json", b"not json"), None)
        .await
        .expect_err("malformed JSON is an error");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
}

/// Capturing a non-JSON step result used to bind null, silently losing the
/// response; it now binds the converted value.
#[tokio::test]
async fn capture_binds_a_text_response_as_a_string() {
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [
            { "call": { "method": "GET", "url": "/page" }, "as": "$page" },
            { "transform": { "captured": "$page" } }
        ]
    }))
    .unwrap();
    let out = executor(Arc::new(Serves("text/html", b"<h1>x</h1>")))
        .run(&spec, input("application/json", b"{}"), None)
        .await
        .unwrap();
    assert_eq!(out_json(out).await["captured"], "<h1>x</h1>");
}

/// `$_rawBody` is byte-faithful: a non-UTF-8 payload arrives as base64, not
/// as replacement characters. A lossy decode here would silently break HMAC
/// verification over a binary webhook payload.
#[tokio::test]
async fn raw_body_is_byte_faithful() {
    const BYTES: &[u8] = &[b'{', 0xFF, b'}'];
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [ { "transform": { "raw": "$_rawBody" } } ]
    }))
    .unwrap();
    let out = executor(Arc::new(NotCalled))
        .run(&spec, input("application/octet-stream", BYTES), None)
        .await
        .unwrap();
    let v = out_json(out).await;
    assert_eq!(B64.decode(v["raw"].as_str().unwrap()).unwrap(), BYTES);
}

/// A UTF-8 payload still binds `$_rawBody` as its exact text, so the signing
/// case every webhook actually uses is unchanged.
#[tokio::test]
async fn raw_body_keeps_utf8_text_verbatim() {
    let spec: PipelineSpec = serde_json::from_value(json!({
        "steps": [ { "transform": { "raw": "$_rawBody" } } ]
    }))
    .unwrap();
    let out = executor(Arc::new(NotCalled))
        .run(&spec, input("application/json", br#"{"id":"evt_1"}"#), None)
        .await
        .unwrap();
    assert_eq!(out_json(out).await["raw"], r#"{"id":"evt_1"}"#);
}
