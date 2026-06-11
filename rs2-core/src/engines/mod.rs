//! Sandbox engines (PRD §5.3). The hybrid decision record: Wasmtime
//! components for Rust/compiled languages, V8 isolates for TS/JS (npm SDK
//! compatibility). Both satisfy the one engine-neutral contract in
//! [`crate::contract`], verified by the shared conformance suite.
//!
//! `native` is the in-process reference binding the conformance suite runs
//! against unconditionally; the sandboxed engines are feature-gated.

mod native;
pub use native::NativeEngine;

#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "js")]
pub mod js;
