//! Restspace string-DSL → typed-spec converter (PRD §8.1).
//!
//! The terse form is accepted as sugar (CLI and config ingestion); the
//! stored format is always the structured [`PipelineSpec`]. A pipeline is a
//! JSON array whose elements are:
//!
//! - a mode token: `"parallel"`, `"serial"`, `"conditional"`, `"tee"`,
//!   `"teeWait"`, optionally with fail/succeed actions (`"serial stop end"`)
//! - a step string: `elevate? try? if (cond)? METHOD url ( :$var | :name)?`
//! - a transform: a JSON object (JSONata template)
//! - a splitter/joiner token: `"jsonSplit"`, `"jsonObject"`
//! - a nested array: a subpipeline

use serde_json::Value;

use crate::error::RsError;

use super::spec::{Action, CallSpec, Joiner, Mode, PipelineSpec, Splitter, Step};

/// Convert a string-DSL pipeline (JSON array) to the typed spec.
pub fn convert(value: &Value) -> Result<PipelineSpec, RsError> {
    let items = value
        .as_array()
        .ok_or_else(|| RsError::bad_request("string-DSL pipeline must be a JSON array"))?;
    let mut spec = PipelineSpec::default();
    let mut first = true;

    for item in items {
        match item {
            Value::String(s) => {
                let s = s.trim();
                if first {
                    if let Some((mode, fail, succeed)) = parse_mode(s) {
                        spec.mode = mode;
                        spec.on_fail = fail;
                        spec.on_succeed = succeed;
                        first = false;
                        continue;
                    }
                }
                first = false;
                match s {
                    "jsonSplit" => {
                        spec.steps.push(Step { split: Some(Splitter::JsonSplit), ..Default::default() })
                    }
                    "jsonObject" => spec.join = Some(Joiner::JsonObject),
                    "unzip" | "zip" | "multipart" => {
                        return Err(RsError::bad_request(format!(
                            "pipeline operator '{s}' is not yet supported in RS2"
                        )))
                    }
                    _ => spec.steps.push(parse_step(s)?),
                }
            }
            Value::Array(_) => {
                first = false;
                spec.steps.push(Step {
                    pipeline: Some(Box::new(convert(item)?)),
                    ..Default::default()
                });
            }
            Value::Object(_) => {
                first = false;
                spec.steps.push(Step { transform: Some(item.clone()), ..Default::default() });
            }
            other => {
                return Err(RsError::bad_request(format!(
                    "invalid pipeline element: {other}"
                )))
            }
        }
    }
    Ok(spec)
}

/// `"parallel"`, `"conditional"`, `"serial stop"`, `"tee"`, … — mode token
/// with optional fail and succeed actions (Restspace `PipelineMode` grammar).
fn parse_mode(s: &str) -> Option<(Mode, Option<Action>, Option<Action>)> {
    let mut parts = s.split_whitespace();
    let mode = match parts.next()? {
        "serial" => Mode::Serial,
        "parallel" => Mode::Parallel,
        "conditional" => Mode::Conditional,
        "tee" => Mode::Tee,
        "teeWait" => Mode::TeeWait,
        _ => return None,
    };
    let fail = parts.next().and_then(parse_action);
    let succeed = parts.next().and_then(parse_action);
    Some((mode, fail, succeed))
}

fn parse_action(s: &str) -> Option<Action> {
    match s {
        "stop" => Some(Action::Stop),
        "next" => Some(Action::Next),
        "end" => Some(Action::End),
        _ => None,
    }
}

/// `elevate? try? if (cond)? METHOD url ( :$var | :name)?`
fn parse_step(s: &str) -> Result<Step, RsError> {
    let mut step = Step::default();
    let mut rest = s.trim();

    // `elevate` (run the call as trusted internal composition) precedes `try`.
    if let Some(after) = rest.strip_prefix("elevate ") {
        step.elevate = true;
        rest = after.trim_start();
    } else if rest == "elevate" {
        return Err(RsError::bad_request("'elevate' must be followed by a step"));
    }

    if let Some(after) = rest.strip_prefix("try ") {
        step.try_mode = true;
        rest = after.trim_start();
    } else if rest == "try" {
        return Err(RsError::bad_request("'try' must be followed by a step"));
    }

    if let Some(after) = rest.strip_prefix("if") {
        let after = after.trim_start();
        if let Some(inner) = after.strip_prefix('(') {
            let close = find_close_paren(inner).ok_or_else(|| {
                RsError::bad_request(format!("unbalanced parentheses in condition: '{s}'"))
            })?;
            step.condition = Some(inner[..close].trim().to_string());
            rest = inner[close + 1..].trim_start();
        }
    }

    // ` :$var` / ` :name` suffix — capture or rename.
    let (call_part, suffix) = match split_rename(rest) {
        Some((call, suffix)) => (call.trim(), Some(suffix)),
        None => (rest, None),
    };
    if let Some(suffix) = suffix {
        if suffix.starts_with('$') {
            step.capture = Some(suffix.to_string());
        } else {
            step.name = Some(suffix.to_string());
        }
    }

    let (method, url) = call_part
        .split_once(' ')
        .map(|(m, u)| (m.trim(), u.trim()))
        .ok_or_else(|| {
            RsError::bad_request(format!("step '{s}' must be 'METHOD url' (got '{call_part}')"))
        })?;
    if http::Method::from_bytes(method.as_bytes()).is_err() || !method.chars().all(|c| c.is_ascii_uppercase()) {
        return Err(RsError::bad_request(format!("invalid method '{method}' in step '{s}'")));
    }
    step.call = Some(CallSpec {
        method: method.to_string(),
        url: url.to_string(),
        effect: None,
        headers: None,
    });
    Ok(step)
}

/// Find ` :suffix` at the end of a step (Restspace rename marker). The
/// suffix runs to the end and contains no spaces.
fn split_rename(s: &str) -> Option<(&str, &str)> {
    let idx = s.rfind(" :")?;
    let suffix = &s[idx + 2..];
    if suffix.is_empty() || suffix.contains(' ') {
        return None;
    }
    Some((&s[..idx], suffix))
}

/// Find the matching close paren, honoring nesting and quotes.
fn find_close_paren(s: &str) -> Option<usize> {
    let mut depth = 1usize;
    let mut in_quote: Option<char> = None;
    for (i, c) in s.char_indices() {
        match (in_quote, c) {
            (Some(q), c) if c == q => in_quote = None,
            (Some(_), _) => {}
            (None, '\'') | (None, '"') => in_quote = Some(c),
            (None, '(') => depth += 1,
            (None, ')') => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn converts_a_restspace_style_pipeline() {
        let spec = convert(&json!([
            "GET /data/orders/${id} :$order",
            "try if ($order.status == 'open') POST /payments/charge",
            { "total": "$sum($order.lines.price)" },
            [
                "parallel",
                "GET /data/customers/${order.customerId} :customer",
                "GET /stock/check :stock",
                "jsonObject"
            ]
        ]))
        .unwrap();

        assert_eq!(spec.steps.len(), 4);
        assert_eq!(spec.steps[0].capture.as_deref(), Some("$order"));
        assert_eq!(spec.steps[0].call.as_ref().unwrap().url, "/data/orders/${id}");
        assert!(spec.steps[1].try_mode);
        assert_eq!(spec.steps[1].condition.as_deref(), Some("$order.status == 'open'"));
        assert!(spec.steps[2].transform.is_some());
        let sub = spec.steps[3].pipeline.as_ref().unwrap();
        assert_eq!(sub.mode, Mode::Parallel);
        assert_eq!(sub.join, Some(Joiner::JsonObject));
        assert_eq!(sub.steps[0].name.as_deref(), Some("customer"));
        assert!(spec.validate().is_empty(), "{:?}", spec.validate());
    }

    #[test]
    fn mode_token_with_actions() {
        let spec = convert(&json!(["serial stop end", "GET /x"])).unwrap();
        assert_eq!(spec.mode, Mode::Serial);
        assert_eq!(spec.on_fail, Some(Action::Stop));
        assert_eq!(spec.on_succeed, Some(Action::End));
        // Mode tokens only count at the head; later "serial" would be a step.
        assert!(convert(&json!(["GET /x", "serial"])).is_err());
    }

    #[test]
    fn split_and_join_tokens() {
        let spec = convert(&json!(["GET /list", "jsonSplit", "GET /detail/${id}", "jsonObject"]))
            .unwrap();
        assert_eq!(spec.steps.len(), 3);
        assert_eq!(spec.steps[1].split, Some(Splitter::JsonSplit));
        assert_eq!(spec.join, Some(Joiner::JsonObject));
    }

    #[test]
    fn rejects_bad_steps() {
        assert!(convert(&json!(["NOSUCHMETHOD"])).is_err());
        assert!(convert(&json!(["if (unclosed GET /x"])).is_err());
        assert!(convert(&json!([42])).is_err());
    }

    #[test]
    fn elevate_token_sets_the_flag() {
        let spec = convert(&json!(["elevate GET /secret/x", "elevate try POST /y"])).unwrap();
        assert!(spec.steps[0].elevate);
        assert!(spec.steps[1].elevate);
        assert!(spec.steps[1].try_mode);
        assert!(!convert(&json!(["GET /z"])).unwrap().steps[0].elevate);
        assert!(convert(&json!(["elevate"])).is_err());
    }
}
