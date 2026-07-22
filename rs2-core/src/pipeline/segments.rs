//! Segment planning (PRD §7.3, §8.3).
//!
//! The executor partitions a serial pipeline into *segments*: maximal runs
//! of steps between materialization points (a transform or split requires
//! materialized bytes; a segment boundary is exactly where the in-flight
//! body is known to be materialized). The segment is the atomic unit of
//! retry, and each segment attempt derives a deterministic idempotency key,
//! so keyed/idempotent effects inside it dedupe across attempts.
//!
//! v1 deliberately fixes the partitioning rule, provenance tracking, and
//! key derivation so the v2 durable journal is purely additive (PRD §7.4).

use serde::Serialize;

use crate::retry::EffectClass;

use super::spec::{Mode, PipelineSpec};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    /// Step index range `[start, end)` into the pipeline's steps.
    pub start: usize,
    pub end: usize,
    /// Whether the segment's input body is materialized at entry — a
    /// checkpoint-eligible boundary for the v2 journal.
    pub checkpoint_eligible: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentPlan {
    pub segments: Vec<Segment>,
    /// Config-time validation warnings (PRD §7.3): e.g. an `unsafe` step
    /// that is not its segment's last step (a retry would re-execute it).
    pub warnings: Vec<String>,
}

/// Compute the segment plan for a pipeline. Only serial pipelines have a
/// meaningful linear segmentation; other modes are a single segment.
pub fn plan(spec: &PipelineSpec) -> SegmentPlan {
    let mut warnings = Vec::new();
    if spec.mode != Mode::Serial || spec.steps.is_empty() {
        collect_warnings(spec, "", &mut warnings);
        return SegmentPlan {
            segments: vec![Segment {
                start: 0,
                end: spec.steps.len(),
                checkpoint_eligible: false,
            }],
            warnings,
        };
    }

    // A boundary falls *before* each materialization point: transform and
    // split steps need materialized bytes, so their inputs are snapshots.
    let mut boundaries = vec![0usize];
    for (i, step) in spec.steps.iter().enumerate() {
        let materializes = step.transform.is_some() || step.split.is_some();
        if materializes && i > 0 && *boundaries.last().unwrap() != i {
            boundaries.push(i);
        }
    }
    boundaries.push(spec.steps.len());

    let segments: Vec<Segment> = boundaries
        .windows(2)
        .map(|w| Segment {
            start: w[0],
            end: w[1],
            // The pipeline's own input is materialized at entry by the
            // executor (snapshot threshold permitting); every later
            // boundary is materialized by construction.
            checkpoint_eligible: true,
        })
        .collect();

    // Unsafe-effect steps that are not the last step of their segment:
    // a segment retry would re-execute them (PRD §7.3 config-time analysis).
    for seg in &segments {
        for i in seg.start..seg.end {
            let step = &spec.steps[i];
            if step.effect_class() == Some(EffectClass::Unsafe) && i + 1 != seg.end {
                warnings.push(format!(
                    "steps[{i}]: unsafe-effect step is not the last in its segment \
                     ({}..{}) — a segment retry would re-execute it; mark it 'keyed', \
                     materialize before it, or suppress this warning",
                    seg.start, seg.end
                ));
            }
        }
    }
    collect_warnings(spec, "", &mut warnings);

    SegmentPlan { segments, warnings }
}

/// Warnings from nested pipelines (each is planned independently at run time).
fn collect_warnings(spec: &PipelineSpec, at: &str, warnings: &mut Vec<String>) {
    for (i, step) in spec.steps.iter().enumerate() {
        if let Some(sub) = &step.pipeline {
            let here = format!("{at}/steps[{i}]/pipeline");
            let sub_plan = plan(sub);
            warnings.extend(
                sub_plan
                    .warnings
                    .into_iter()
                    .map(|w| format!("{here}: {w}")),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::spec::Step;
    use serde_json::json;

    fn call(method: &str) -> Step {
        Step::call(method, "/x")
    }

    fn transform() -> Step {
        Step {
            transform: Some(json!({"a": "$"})),
            ..Default::default()
        }
    }

    #[test]
    fn transforms_partition_segments() {
        let spec = PipelineSpec {
            steps: vec![
                call("GET"),
                call("PUT"),
                transform(),
                call("GET"),
                transform(),
            ],
            ..Default::default()
        };
        let plan = plan(&spec);
        let ranges: Vec<(usize, usize)> = plan.segments.iter().map(|s| (s.start, s.end)).collect();
        assert_eq!(ranges, vec![(0, 2), (2, 4), (4, 5)]);
        assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);
    }

    #[test]
    fn unsafe_step_mid_segment_warns() {
        let spec = PipelineSpec {
            steps: vec![call("POST"), call("GET"), transform()],
            ..Default::default()
        };
        let plan = plan(&spec);
        assert_eq!(plan.warnings.len(), 1);
        assert!(plan.warnings[0].contains("steps[0]"));
        // Marking the call keyed clears the warning.
        let mut keyed = spec.clone();
        keyed.steps[0].call.as_mut().unwrap().effect = Some(EffectClass::Keyed);
        assert!(super::plan(&keyed).warnings.is_empty());
    }

    #[test]
    fn unsafe_step_at_segment_end_is_fine() {
        let spec = PipelineSpec {
            steps: vec![call("GET"), call("POST"), transform(), call("GET")],
            ..Default::default()
        };
        assert!(plan(&spec).warnings.is_empty());
    }
}
