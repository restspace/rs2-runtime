//! RS2 image transform service (see the repo's `docs/cms-roadmap.md` item 8):
//! query-string resize/crop for responsive design, as a sandboxed Wasm
//! component. Reads originals through a `source` prefix grant (caller authz
//! preserved), keeps derivatives in a `cache` store grant, and serves both
//! via `x-rs2-body-ref` so no image bytes cross the sandbox on a hit.
//!
//! `params`/`transform` are pure and test natively; `service` is the
//! sandbox-facing glue, compiled only for wasm32.

pub mod params;
pub mod transform;

#[cfg(target_arch = "wasm32")]
mod service;
