//! Pipelines (PRD §8): typed spec, condition language, JSONata transforms,
//! string-DSL conversion, segment planning (PRD §7.3), and the executor.

pub mod condition;
pub mod dsl;
pub mod executor;
pub mod segments;
pub mod spec;
pub mod transform;

pub use executor::{Executor, PipelineLimits, Requester};
pub use segments::{plan, SegmentPlan};
pub use spec::{Action, CallSpec, Joiner, Mode, PipelineSpec, Splitter, Step};
