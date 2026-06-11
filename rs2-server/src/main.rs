//! rs2-server: the supported v1 packaging of `rs2-core` (PRD 5.1) —
//! a hyper HTTP listener + config loading + adapter wiring + ops endpoints.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use http::{Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::TcpListener;

use rs2_core::message::{Body, MediaType, Message, Provenance};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

/// Successor to Restspace's `serverConfig.json` (PRD 13), v1 subset.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerConfig {
    #[serde(default = "default_listen")]
    listen: String,
    tenancy: TenancyConfig,
    /// Root directory for the local-fs file store adapter.
    #[serde(default = "default_file_root")]
    file_root: String,
    /// Directory holding `<tenant>.json` tenant configs.
    #[serde(default = "default_tenants_dir")]
    tenants_dir: String,
}

fn default_listen() -> String {
    "127.0.0.1:3100".to_string()
}
fn default_file_root() -> String {
    "./data".to_string()
}
fn default_tenants_dir() -> String {
    "./tenants".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "mode")]
enum TenancyConfig {
    #[serde(rename = "single")]
    Single { tenant: String },
    #[serde(rename = "multi")]
    Multi {
        #[serde(default)]
        domain_map: HashMap<String, String>,
        main_domain: Option<String>,
    },
}

/// File-backed tenant config store: `<tenants_dir>/<tenant>.json`.
struct FileConfigLoader {
    dir: PathBuf,
}

#[async_trait]
impl ConfigLoader for FileConfigLoader {
    async fn load_tenant(&self, tenant: &str) -> Result<TenantConfig, RsError> {
        if tenant.contains(['/', '\\', '.']) {
            return Err(RsError::bad_request("invalid tenant name"));
        }
        let path = self.dir.join(format!("{tenant}.json"));
        let text = tokio::fs::read_to_string(&path)
            .await
            .map_err(|_| RsError::not_found(format!("unknown tenant '{tenant}'")))?;
        serde_json::from_str(&text)
            .map_err(|e| RsError::internal(format!("tenant config for '{tenant}' is invalid: {e}")))
    }
}

fn hyper_request_to_message(req: hyper::Request<Incoming>, tenant: &str) -> Message {
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let mut msg = Message::request(parts.method, &path_and_query, tenant);
    let media_type = parts
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(MediaType::parse)
        .unwrap_or_else(MediaType::octet_stream);
    let size = parts
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    msg.headers = parts.headers;
    let has_body = size.map(|s| s > 0).unwrap_or(true);
    if has_body && msg.method != http::Method::GET && msg.method != http::Method::HEAD {
        let stream = http_body_util::BodyStream::new(body)
            .filter_map(|frame| async move {
                match frame {
                    Ok(f) => f.into_data().ok().map(Ok),
                    Err(e) => Some(Err(std::io::Error::other(e))),
                }
            })
            .boxed();
        // Ingress bodies are Ephemeral: no replayable source (PRD 6.3).
        msg.body = Some(Body::from_stream(stream, media_type, size, Provenance::Ephemeral));
    }
    msg
}

type OutBody = http_body_util::combinators::UnsyncBoxBody<Bytes, std::io::Error>;

fn message_to_hyper_response(msg: Message) -> Response<OutBody> {
    let status = msg.status.unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(status);
    if let Some(headers) = builder.headers_mut() {
        *headers = msg.headers.clone();
    }
    match msg.body {
        None => builder
            .body(http_body_util::Empty::new().map_err(std::io::Error::other).boxed_unsync())
            .unwrap(),
        Some(body) => {
            let media_type = body.media_type.to_string();
            let size = body.size;
            let stream = body
                .into_stream()
                .map(|chunk| chunk.map(hyper::body::Frame::data));
            let mut resp = builder
                .body(BodyExt::boxed_unsync(StreamBody::new(stream)))
                .unwrap();
            resp.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_str(&media_type)
                    .unwrap_or(http::HeaderValue::from_static("application/octet-stream")),
            );
            if let Some(s) = size {
                resp.headers_mut()
                    .insert(http::header::CONTENT_LENGTH, http::HeaderValue::from(s));
            }
            resp
        }
    }
}

async fn serve_request(runtime: Arc<Runtime>, req: hyper::Request<Incoming>) -> Response<OutBody> {
    let path = req.uri().path();
    // Ops endpoints (PRD 14), outside tenant routing.
    if path == "/healthz" || path == "/readyz" {
        return Response::builder()
            .status(StatusCode::OK)
            .body(http_body_util::Full::new(Bytes::from_static(b"ok")).map_err(std::io::Error::other).boxed_unsync())
            .unwrap();
    }
    let host = req
        .headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Some(tenant) = runtime.resolve_tenant(host) else {
        let problem = RsError::not_found(format!("no tenant for host '{host}'"))
            .to_problem_json("-", "-")
            .to_string();
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(http::header::CONTENT_TYPE, "application/problem+json")
            .body(http_body_util::Full::new(Bytes::from(problem)).map_err(std::io::Error::other).boxed_unsync())
            .unwrap();
    };
    let msg = hyper_request_to_message(req, &tenant);
    let trace_id = msg.trace.trace_id.clone();
    let mut resp = message_to_hyper_response(runtime.handle(msg).await);
    if let Ok(v) = http::HeaderValue::from_str(&trace_id) {
        resp.headers_mut().insert("x-trace-id", v);
    }
    resp
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = std::env::args().nth(1).unwrap_or_else(|| "serverConfig.json".to_string());
    let config_text = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("cannot read server config '{config_path}': {e}"))?;
    let config: ServerConfig = serde_json::from_str(&config_text)
        .map_err(|e| format!("invalid server config '{config_path}': {e}"))?;

    let tenancy = match config.tenancy {
        TenancyConfig::Single { tenant } => Tenancy::Single { tenant },
        TenancyConfig::Multi { domain_map, main_domain } => Tenancy::Multi { domain_map, main_domain },
    };
    let adapters = Adapters {
        files: Arc::new(rs2_core::adapters::LocalFsFileStore::new(&config.file_root)),
        data: Arc::new(rs2_core::adapters::MemDataStore::new()),
    };
    let loader = Arc::new(FileConfigLoader { dir: PathBuf::from(&config.tenants_dir) });
    let runtime = Arc::new(Runtime::new(tenancy, adapters, loader, LimitTable::default()));

    let addr: SocketAddr = config.listen.parse()?;
    let listener = TcpListener::bind(addr).await?;
    println!("rs2-server listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |req| {
                let runtime = runtime.clone();
                async move { Ok::<_, std::convert::Infallible>(serve_request(runtime, req).await) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                eprintln!("connection error: {e}");
            }
        });
    }
}

