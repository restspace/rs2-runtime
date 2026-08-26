// Restspace string-DSL → typed-spec converter (PRD §8.1). Port of
// `rs2-core/src/pipeline/dsl.rs`: the terse form is accepted as sugar; the
// stored format is always the structured `PipelineSpec`.

import { RsError } from "../runtime/error";
import type { Json } from "../runtime/error";
import { emptySpec, emptyStep, isValidMethod } from "./spec";
import type { Action, Mode, PipelineSpec, Step } from "./spec";

/// Convert a string-DSL pipeline (JSON array) to the typed spec.
export function convert(value: Json): PipelineSpec {
  if (!Array.isArray(value)) throw RsError.badRequest("string-DSL pipeline must be a JSON array");
  const spec = emptySpec();
  let first = true;
  for (const item of value) {
    if (typeof item === "string") {
      const s = item.trim();
      if (first) {
        const mode = parseMode(s);
        if (mode) {
          spec.mode = mode[0];
          spec.onFail = mode[1];
          spec.onSucceed = mode[2];
          first = false;
          continue;
        }
      }
      first = false;
      if (s === "jsonSplit") spec.steps.push({ ...emptyStep(), split: "jsonSplit" });
      else if (s === "jsonObject") spec.join = "jsonObject";
      else if (s === "unzip" || s === "zip" || s === "multipart") {
        throw RsError.badRequest(`pipeline operator '${s}' is not yet supported in RS2`);
      } else spec.steps.push(parseStep(s));
    } else if (Array.isArray(item)) {
      first = false;
      spec.steps.push({ ...emptyStep(), pipeline: convert(item) });
    } else if (item && typeof item === "object") {
      first = false;
      spec.steps.push({ ...emptyStep(), transform: item });
    } else {
      throw RsError.badRequest(`invalid pipeline element: ${JSON.stringify(item)}`);
    }
  }
  return spec;
}

/// `"parallel"`, `"conditional"`, `"serial stop"`, `"tee"`, … — mode token
/// with optional fail and succeed actions.
function parseMode(s: string): [Mode, Action | undefined, Action | undefined] | undefined {
  const parts = s.split(/\s+/).filter((p) => p !== "");
  const head = parts[0];
  if (head === undefined) return undefined;
  let mode: Mode;
  switch (head) {
    case "serial":
    case "parallel":
    case "conditional":
    case "tee":
    case "teeWait":
      mode = head;
      break;
    default:
      return undefined;
  }
  return [mode, parseAction(parts[1]), parseAction(parts[2])];
}

function parseAction(s: string | undefined): Action | undefined {
  return s === "stop" || s === "next" || s === "end" ? s : undefined;
}

/// `elevate? try? if (cond)? METHOD url ( :$var | :name)?`
function parseStep(s: string): Step {
  const step = emptyStep();
  let rest = s.trim();

  if (rest.startsWith("elevate ")) {
    step.elevate = true;
    rest = rest.slice("elevate ".length).trimStart();
  } else if (rest === "elevate") {
    throw RsError.badRequest("'elevate' must be followed by a step");
  }

  if (rest.startsWith("try ")) {
    step.tryMode = true;
    rest = rest.slice("try ".length).trimStart();
  } else if (rest === "try") {
    throw RsError.badRequest("'try' must be followed by a step");
  }

  if (rest.startsWith("if")) {
    const after = rest.slice(2).trimStart();
    if (after.startsWith("(")) {
      const inner = after.slice(1);
      const close = findCloseParen(inner);
      if (close === undefined) throw RsError.badRequest(`unbalanced parentheses in condition: '${s}'`);
      step.condition = inner.slice(0, close).trim();
      rest = inner.slice(close + 1).trimStart();
    }
  }

  let callPart = rest;
  const renamed = splitRename(rest);
  if (renamed) {
    callPart = renamed[0].trim();
    if (renamed[1].startsWith("$")) step.capture = renamed[1];
    else step.name = renamed[1];
  }

  const sp = callPart.indexOf(" ");
  if (sp < 0) throw RsError.badRequest(`step '${s}' must be 'METHOD url' (got '${callPart}')`);
  const method = callPart.slice(0, sp).trim();
  const url = callPart.slice(sp + 1).trim();
  if (!isValidMethod(method) || !/^[A-Z]+$/.test(method)) {
    throw RsError.badRequest(`invalid method '${method}' in step '${s}'`);
  }
  step.call = { method, url, effect: undefined, headers: undefined };
  return step;
}

/// Find ` :suffix` at the end of a step (Restspace rename marker). The
/// suffix runs to the end and contains no spaces.
function splitRename(s: string): [string, string] | undefined {
  const idx = s.lastIndexOf(" :");
  if (idx < 0) return undefined;
  const suffix = s.slice(idx + 2);
  if (suffix === "" || suffix.includes(" ")) return undefined;
  return [s.slice(0, idx), suffix];
}

/// Find the matching close paren, honoring nesting and quotes.
function findCloseParen(s: string): number | undefined {
  let depth = 1;
  let inQuote: string | undefined;
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    if (inQuote !== undefined) {
      if (c === inQuote) inQuote = undefined;
    } else if (c === "'" || c === '"') inQuote = c;
    else if (c === "(") depth += 1;
    else if (c === ")") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  return undefined;
}
