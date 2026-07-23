//! Projected listing contract (`$select`/`$sort` on dataset listings): the
//! shared semantics every `DataStore` implementation — the host fallback and
//! native pushdowns alike — must produce identically, so the store-conformance
//! suite can hold them to one answer.
//!
//! Pinned semantics (portable across host JSON, SQL binary collations, and
//! MongoDB's default comparison):
//! - Strings compare by **binary UTF-8 code points** — case-sensitive, no
//!   Unicode normalization, no locale. (`memcmp` on UTF-8 bytes equals
//!   code-point order.)
//! - Cross-type total order: missing < null < false < true < numbers
//!   (numerically, integer-precision-aware) < strings < arrays < objects.
//!   Arrays/objects order by their serialized JSON — a total order, but only
//!   the homogeneous scalar cases are contractual for native pushdown.
//! - Multi-key sort; `-` prefix = descending. A missing field is smallest, so
//!   it sorts first ascending, last descending.
//! - Projection paths are dot-paths (`meta.date`); an absent path is simply
//!   absent from the projected object, never an error.

use std::cmp::Ordering;

use serde_json::{Map, Value};

use crate::error::RsError;

/// A parsed dot-path into a JSON record (`meta.date` → `["meta", "date"]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPath(pub Vec<String>);

impl FieldPath {
    pub fn parse(s: &str) -> Result<FieldPath, RsError> {
        if s.is_empty() || s.split('.').any(|seg| seg.is_empty()) {
            return Err(RsError::bad_request(format!(
                "invalid field path '{s}': expected dot-separated non-empty segments"
            )));
        }
        Ok(FieldPath(s.split('.').map(str::to_string).collect()))
    }

    /// The value at this path, or `None` where any segment is missing or a
    /// non-object intervenes.
    pub fn lookup<'a>(&self, record: &'a Value) -> Option<&'a Value> {
        let mut cur = record;
        for seg in &self.0 {
            cur = cur.as_object()?.get(seg)?;
        }
        Some(cur)
    }

    /// The wire form (`meta.date`).
    pub fn dotted(&self) -> String {
        self.0.join(".")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Asc,
    Desc,
}

/// The standardised listing request: what `$select`/`$sort`/`$take`/`$skip`
/// parse into, and what every `DataStore::list_records` receives.
#[derive(Debug, Clone)]
pub struct ListSpec {
    /// Fields to project into each entry. Never empty — an empty `$select`
    /// is a parse error (a plain listing doesn't build a `ListSpec` at all).
    pub fields: Vec<FieldPath>,
    /// Sort keys in priority order; empty = adapter's natural key order.
    pub sort: Vec<(FieldPath, Dir)>,
    pub take: usize,
    pub skip: usize,
}

impl ListSpec {
    /// Parse the wire form: `$select=title,meta.date`, `$sort=-date,title`.
    pub fn parse(select: &str, sort: Option<&str>, take: usize, skip: usize) -> Result<ListSpec, RsError> {
        let fields = select
            .split(',')
            .map(|f| FieldPath::parse(f.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        if fields.is_empty() {
            return Err(RsError::bad_request("$select requires at least one field"));
        }
        let sort = match sort {
            None => Vec::new(),
            Some(s) => s
                .split(',')
                .map(|k| {
                    let k = k.trim();
                    let (dir, path) = match k.strip_prefix('-') {
                        Some(rest) => (Dir::Desc, rest),
                        None => (Dir::Asc, k),
                    };
                    Ok((FieldPath::parse(path)?, dir))
                })
                .collect::<Result<Vec<_>, RsError>>()?,
        };
        Ok(ListSpec { fields, sort, take, skip })
    }
}

/// Rank in the cross-type total order (missing is handled by the caller —
/// [`compare_optional`] — since it has no `Value` representation).
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(false) => 1,
        Value::Bool(true) => 2,
        Value::Number(_) => 3,
        Value::String(_) => 4,
        Value::Array(_) => 5,
        Value::Object(_) => 6,
    }
}

/// The contractual comparison for two present values.
pub fn compare(a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => compare_numbers(x, y),
        // str::cmp is code-point order == UTF-8 byte order.
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(_), Value::Array(_)) | (Value::Object(_), Value::Object(_)) => {
            a.to_string().cmp(&b.to_string())
        }
        _ => Ordering::Equal, // same-rank null/bools are equal
    }
}

/// Numeric comparison preserving i64/u64 precision where both sides have it
/// (f64 round-trip loses integers beyond 2^53).
fn compare_numbers(x: &serde_json::Number, y: &serde_json::Number) -> Ordering {
    if let (Some(a), Some(b)) = (x.as_i64(), y.as_i64()) {
        return a.cmp(&b);
    }
    if let (Some(a), Some(b)) = (x.as_u64(), y.as_u64()) {
        return a.cmp(&b);
    }
    let (a, b) = (x.as_f64().unwrap_or(f64::NAN), y.as_f64().unwrap_or(f64::NAN));
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Comparison with missing-is-smallest (`None` = field absent from record).
pub fn compare_optional(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => compare(a, b),
    }
}

/// Multi-key record comparison per the spec's sort keys.
pub fn compare_records(a: &Value, b: &Value, sort: &[(FieldPath, Dir)]) -> Ordering {
    for (path, dir) in sort {
        let ord = compare_optional(path.lookup(a), path.lookup(b));
        let ord = match dir {
            Dir::Asc => ord,
            Dir::Desc => ord.reverse(),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Project a record to the spec's fields. Nested paths reconstruct their
/// object shape (`meta.date` → `{"meta": {"date": …}}`); absent paths are
/// omitted.
pub fn project(record: &Value, fields: &[FieldPath]) -> Value {
    let mut out = Map::new();
    for path in fields {
        if let Some(v) = path.lookup(record) {
            insert_path(&mut out, &path.0, v.clone());
        }
    }
    Value::Object(out)
}

fn insert_path(obj: &mut Map<String, Value>, path: &[String], value: Value) {
    match path {
        [] => {}
        [leaf] => {
            obj.insert(leaf.clone(), value);
        }
        [head, rest @ ..] => {
            let entry = obj
                .entry(head.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            if !entry.is_object() {
                // A shorter projected path already claimed this segment with a
                // scalar; the longer path loses deterministically.
                return;
            }
            insert_path(entry.as_object_mut().unwrap(), rest, value);
        }
    }
}

/// The host-side reference pipeline over a full dataset: sort (stably, by key
/// order as the final tiebreak for determinism), page, project. `records` are
/// `(key, record)` pairs; returns `(projected page, total)`. This is what the
/// default `DataStore::list_records` and any adapter without native pushdown
/// produce; native implementations must match it.
pub fn sort_page_project(
    mut records: Vec<(String, Value)>,
    spec: &ListSpec,
) -> (Vec<(String, Value)>, u64) {
    let total = records.len() as u64;
    records.sort_by(|(ka, a), (kb, b)| {
        compare_records(a, b, &spec.sort).then_with(|| ka.cmp(kb))
    });
    let page = records
        .into_iter()
        .skip(spec.skip)
        .take(spec.take)
        .map(|(k, r)| (k, project(&r, &spec.fields)))
        .collect();
    (page, total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn path(s: &str) -> FieldPath {
        FieldPath::parse(s).unwrap()
    }

    #[test]
    fn field_path_rejects_malformed() {
        assert!(FieldPath::parse("").is_err());
        assert!(FieldPath::parse("a..b").is_err());
        assert!(FieldPath::parse(".a").is_err());
        assert!(FieldPath::parse("a.").is_err());
        assert_eq!(path("meta.date").0, vec!["meta", "date"]);
    }

    #[test]
    fn strings_compare_by_code_point_not_dictionary() {
        // Uppercase before lowercase; non-ASCII after all ASCII.
        assert_eq!(compare(&json!("Zebra"), &json!("apple")), Ordering::Less);
        assert_eq!(compare(&json!("é"), &json!("z")), Ordering::Greater);
        // NFC vs NFD é differ bytewise — unequal by design.
        assert_ne!(compare(&json!("\u{e9}"), &json!("e\u{301}")), Ordering::Equal);
    }

    #[test]
    fn cross_type_order_is_total_and_pinned() {
        let ladder = [
            json!(null),
            json!(false),
            json!(true),
            json!(-1.5),
            json!(0),
            json!(9),
            json!(""),
            json!("a"),
            json!([1]),
            json!({"a": 1}),
        ];
        for w in ladder.windows(2) {
            assert_ne!(
                compare(&w[0], &w[1]),
                Ordering::Greater,
                "{} should not sort after {}",
                w[0],
                w[1]
            );
        }
        // Missing sorts before everything, including null.
        assert_eq!(compare_optional(None, Some(&json!(null))), Ordering::Less);
    }

    #[test]
    fn numbers_keep_integer_precision_beyond_f64() {
        let a = json!(9_007_199_254_740_993_i64); // 2^53 + 1
        let b = json!(9_007_199_254_740_992_i64); // 2^53 (== as f64)
        assert_eq!(compare(&a, &b), Ordering::Greater);
        assert_eq!(compare(&json!(1), &json!(1.5)), Ordering::Less);
    }

    #[test]
    fn multi_key_sort_with_direction_and_missing() {
        let sort = vec![(path("group"), Dir::Asc), (path("n"), Dir::Desc)];
        let a = json!({"group": "a", "n": 1});
        let b = json!({"group": "a", "n": 2});
        let c = json!({"group": "b", "n": 9});
        let d = json!({"group": "a"}); // missing n: smallest, so Desc puts it last
        assert_eq!(compare_records(&b, &a, &sort), Ordering::Less);
        assert_eq!(compare_records(&a, &c, &sort), Ordering::Less);
        assert_eq!(compare_records(&a, &d, &sort), Ordering::Less);
    }

    #[test]
    fn projection_reconstructs_nested_shape_and_skips_absent() {
        let rec = json!({"title": "T", "meta": {"date": "2026-01-01", "x": 1}});
        let out = project(&rec, &[path("title"), path("meta.date"), path("nope.deep")]);
        assert_eq!(out, json!({"title": "T", "meta": {"date": "2026-01-01"}}));
    }

    #[test]
    fn spec_parses_wire_form() {
        let spec = ListSpec::parse("title,meta.date", Some("-meta.date,title"), 25, 50).unwrap();
        assert_eq!(spec.fields.len(), 2);
        assert_eq!(spec.sort[0], (path("meta.date"), Dir::Desc));
        assert_eq!(spec.sort[1], (path("title"), Dir::Asc));
        assert!(ListSpec::parse("", None, 10, 0).is_err());
        assert!(ListSpec::parse("a", Some("-"), 10, 0).is_err());
    }

    #[test]
    fn sort_page_project_is_deterministic_with_key_tiebreak() {
        let recs = vec![
            ("k2".to_string(), json!({"n": 1, "t": "b"})),
            ("k1".to_string(), json!({"n": 1, "t": "a"})),
            ("k3".to_string(), json!({"n": 0, "t": "c"})),
        ];
        let spec = ListSpec::parse("t", Some("n"), 10, 0).unwrap();
        let (page, total) = sort_page_project(recs, &spec);
        assert_eq!(total, 3);
        let keys: Vec<_> = page.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["k3", "k1", "k2"]); // equal n=1 tie broken by key
        assert_eq!(page[0].1, json!({"t": "c"}));
    }
}
