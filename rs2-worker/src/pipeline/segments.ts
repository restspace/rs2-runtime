// Segment planning (PRD §7.3, §8.3): a serial pipeline is partitioned into
// maximal runs of steps between materialization points (a transform or
// split); the segment is the atomic unit of retry. Port of
// `rs2-core/src/pipeline/segments.rs`; warning text verbatim.

import type { JsonObject } from "../runtime/error";
import { stepEffectClass } from "./spec";
import type { PipelineSpec } from "./spec";

export interface Segment {
  /// Step index range `[start, end)` into the pipeline's steps.
  start: number;
  end: number;
  /// Whether the segment's input body is materialized at entry.
  checkpointEligible: boolean;
}

export interface SegmentPlan {
  segments: Segment[];
  warnings: string[];
}

export function planToJson(p: SegmentPlan): JsonObject {
  return {
    segments: p.segments.map((s) => ({ start: s.start, end: s.end, checkpointEligible: s.checkpointEligible })),
    warnings: [...p.warnings],
  };
}

/// Compute the segment plan for a pipeline. Only serial pipelines have a
/// meaningful linear segmentation; other modes are a single segment.
export function plan(spec: PipelineSpec): SegmentPlan {
  const warnings: string[] = [];
  if (spec.mode !== "serial" || spec.steps.length === 0) {
    collectWarnings(spec, "", warnings);
    return { segments: [{ start: 0, end: spec.steps.length, checkpointEligible: false }], warnings };
  }

  // A boundary falls *before* each materialization point.
  const boundaries = [0];
  spec.steps.forEach((step, i) => {
    const materializes = step.transform !== undefined || step.split !== undefined;
    if (materializes && i > 0 && boundaries[boundaries.length - 1] !== i) boundaries.push(i);
  });
  boundaries.push(spec.steps.length);

  const segments: Segment[] = [];
  for (let i = 0; i + 1 < boundaries.length; i++) {
    segments.push({ start: boundaries[i]!, end: boundaries[i + 1]!, checkpointEligible: true });
  }

  // Unsafe-effect steps that are not the last step of their segment.
  for (const seg of segments) {
    for (let i = seg.start; i < seg.end; i++) {
      if (stepEffectClass(spec.steps[i]!) === "unsafe" && i + 1 !== seg.end) {
        warnings.push(
          `steps[${i}]: unsafe-effect step is not the last in its segment (${seg.start}..${seg.end}) — a segment retry would re-execute it; mark it 'keyed', materialize before it, or suppress this warning`,
        );
      }
    }
  }
  collectWarnings(spec, "", warnings);
  return { segments, warnings };
}

/// Warnings from nested pipelines (each is planned independently at run time).
function collectWarnings(spec: PipelineSpec, at: string, warnings: string[]): void {
  spec.steps.forEach((step, i) => {
    if (step.pipeline) {
      const here = `${at}/steps[${i}]/pipeline`;
      for (const w of plan(step.pipeline).warnings) warnings.push(`${here}: ${w}`);
    }
  });
}
