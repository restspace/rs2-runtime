//! Prebuilt services (PRD §5.5): host-native Rust, trusted runtime
//! components running in-process. They consume the same capability traits
//! the sandbox host exposes, so swapping adapters affects prebuilt and
//! custom services identically.

mod data;
mod file;

pub use data::DataService;
pub use file::FileService;

use async_trait::async_trait;

use crate::capabilities::{ScopedDataStore, ScopedFileStore};
use crate::contract::InvocationLimits;
use crate::error::RsError;
use crate::message::Message;

/// What a service instance was granted at mount time.
pub struct ServiceContext {
    pub config: serde_json::Value,
    pub files: Option<ScopedFileStore>,
    pub data: Option<ScopedDataStore>,
    pub limits: InvocationLimits,
}

/// A unit of behavior handling messages at a mount: Message → Message.
#[async_trait]
pub trait Service: Send + Sync {
    async fn handle(&self, msg: Message, ctx: &ServiceContext) -> Result<Message, RsError>;
}

/// Parse `$take`/`$skip` pagination query params with bounded defaults.
pub(crate) fn pagination(msg: &Message) -> (usize, usize) {
    let take = msg
        .url
        .query_param("$take")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000)
        .min(10_000);
    let skip = msg.url.query_param("$skip").and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
    (take, skip)
}
