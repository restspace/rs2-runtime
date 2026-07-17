//! # rs2-core
//!
//! Core crate of RS2, a sandboxed composable-service runtime: services are
//! functions on HTTP messages, mounted at URL paths per tenant, chainable into
//! pipelines, with all custom code running inside sandboxes with hard limits.
//!
//! This crate has no global state. The supported packaging in v1 is the
//! `rs2-server` binary; the crate API is explicitly 0.x/unstable.
//!
//! Module layout follows the PRD (§5.1):
//!
//! - [`message`] — `Message`, `Body`, provenance, media types
//! - [`router`] — tenant resolution, mounts, longest-prefix matching, path safety
//! - [`wrapper`] — auth (stub), limits admission, structured error mapping
//! - [`contract`] — engine-neutral service contract (host API, engine trait)
//! - [`engines`] — sandbox engines: Wasmtime components (`wasm` feature),
//!   V8 isolates (`js` feature, skeleton)
//! - [`services`] — prebuilt host-native services: `file`, `data`
//! - [`capabilities`] — traits: `FileStore`, `DataStore`, `HttpOut`, tenant scoping
//! - [`adapters`] — built-in capability implementations: local fs, in-memory data
//! - [`tenant`] / [`runtime`] — tenant config, lazy loading, dispatch

pub mod adapters;
pub mod capabilities;
pub mod config_schema;
pub mod contract;
pub mod crypto;
pub mod discovery;
pub mod engines;
pub mod error;
pub mod idempotency;
pub mod infra;
pub mod logging;
pub mod message;
pub(crate) mod outbound;
pub mod path_pattern;
pub mod pipeline;
pub mod retry;
pub mod router;
pub mod runtime;
pub mod scheduler;
pub mod services;
pub mod tenant;
pub mod wrapper;

pub use error::RsError;
pub use message::{Body, MediaType, Message, Provenance};
pub use runtime::Runtime;
