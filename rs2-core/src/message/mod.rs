//! The message model (PRD §6): the universal value services map over.

mod body;
pub mod media_type;
#[allow(clippy::module_inception)]
mod message;

pub use body::{Body, Payload, Provenance};
pub use media_type::MediaType;
pub use message::{Message, MsgUrl, Principal, Source, TraceContext};
