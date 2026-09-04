//! The message model (PRD §6): the universal value services map over.

mod body;
pub mod media_type;
mod media_type_table;
#[allow(clippy::module_inception)]
mod message;

pub use body::{Body, ByteStream, Payload, Provenance};
pub use media_type::MediaType;
pub use message::{Message, MsgUrl, Principal, Source, TraceContext};
