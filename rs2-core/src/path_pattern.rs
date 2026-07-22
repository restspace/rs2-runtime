//! Path-pattern resolution: build a string (typically a call URL) from `${…}`
//! placeholders over two planes — the **URL plane** (`${url.…}`: the incoming
//! request's path segments, name, and query) and the **data plane**
//! (`${field.path}`: dot-paths over a JSON scope of query/body/captured vars).
//!
//! This is the rs2 successor to Restspace v1's `resolvePathPattern`. The v1
//! grammar (`$>0`, `$<0`, `$P*`, `$N*`, `$?(q)`, the overloaded `$$`) is
//! replaced by one delimiter and a readable, parser-checked selector syntax:
//!
//! - `${url.path[0]}`   first peeled service-path segment
//! - `${url.path[-1]}`  last segment
//! - `${url.path[1:]}`  segments 1..end, `/`-joined (Python-style slice)
//! - `${url.path}`      whole service path, segments `/`-joined (no leading/trailing slash)
//! - `${url.rest}`      verbatim service-path remainder (leading slash, exact
//!   trailing slash) — for transparent path forwarding
//! - `${url.base[0]}`   mount-prefix segments; `${url.full}` = base+path
//! - `${url.name}`      last service segment (resource name)
//! - `${url.query.id}`  one query param; `${url.query}` the whole query string
//! - `${url.path[5]?}`  optional — elides (and collapses a neighboring `/`)
//! - `${url.query.page || '1'}`  literal/selector default when empty
//! - `${order.customerId}`  data-plane dot-path (unchanged from before)
//!
//! A selector that resolves to nothing is a 400 unless marked optional (`?`) or
//! given a default (`||`). Patterns are validated at spec-write time via
//! [`validate`].

use serde_json::{Map, Value};

use crate::error::RsError;

/// Borrowed view of the URL plane a pattern reads. `path` is the segment list
/// the pattern indexes against — for a pipeline this is the *peeled* sub-path
/// (segments beyond the matched spec prefix), matching the positional
/// convention of the query/template services.
#[derive(Default, Clone, Copy)]
pub struct UrlView<'a> {
    pub path: &'a [&'a str],
    pub base: &'a [&'a str],
    pub name: Option<&'a str>,
    pub query: &'a str,
    /// The verbatim service-path remainder (`MsgUrl::service_path`): leading
    /// slash, exact trailing slash, mount root normalized to `/`. Unlike
    /// `path` (segment-joined) this is byte-exact, for transparent forwarding.
    pub rest: &'a str,
}

impl<'a> UrlView<'a> {
    /// An empty view — for resolving data-only patterns (no URL plane).
    pub const EMPTY: UrlView<'static> = UrlView {
        path: &[],
        base: &[],
        name: None,
        query: "",
        rest: "",
    };
}

/// Resolve all `${…}` placeholders in `pattern`.
pub fn resolve(pattern: &str, url: &UrlView, data: &Map<String, Value>) -> Result<String, RsError> {
    let mut out = String::with_capacity(pattern.len());
    let mut rest = pattern;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| RsError::bad_request(format!("unterminated ${{…}} in '{pattern}'")))?;
        let interior = &after[..end];
        match eval_token(interior, url, data)
            .map_err(|e| RsError::bad_request(format!("in pattern '{pattern}': {e}")))?
        {
            Token::Value(s) => out.push_str(&s),
            Token::Elide => {
                // Collapse a separator the elision would have left dangling
                // (e.g. `a/${x?}/b` → `a/b`), so we never emit `//`.
                if out.ends_with('/') {
                    out.pop();
                }
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Validate every `${…}` placeholder parses (config-time check, e.g. when a
/// pipeline spec is stored). Does not require any URL/data to be present.
pub fn validate(pattern: &str) -> Result<(), RsError> {
    let mut rest = pattern;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| RsError::bad_request(format!("unterminated ${{…}} in '{pattern}'")))?;
        parse_token(&after[..end])
            .map_err(|e| RsError::bad_request(format!("in pattern '{pattern}': {e}")))?;
        rest = &after[end + 1..];
    }
    Ok(())
}

enum Token {
    Value(String),
    Elide,
}

/// A parsed `${…}` interior: a primary expression, an optional `?`, and an
/// optional `|| default`.
struct ParsedToken<'a> {
    primary: Expr<'a>,
    optional: bool,
    default: Option<Expr<'a>>,
}

enum Expr<'a> {
    /// A single-quoted literal `'…'`.
    Literal(&'a str),
    /// A `url.…` selector.
    Url(UrlSel<'a>),
    /// A data-plane dot-path (`order.customerId`).
    Data(&'a str),
}

enum UrlSel<'a> {
    Segments {
        section: Section,
        index: Option<Index>,
    },
    Name,
    Rest,
    QueryAll,
    QueryKey(&'a str),
}

#[derive(Clone, Copy)]
enum Section {
    Path,
    Base,
    Full,
}

enum Index {
    At(i64),
    Slice(Option<i64>, Option<i64>),
}

fn parse_token(interior: &str) -> Result<ParsedToken<'_>, String> {
    // Split off `|| default` at the first top-level `||`.
    let (primary_src, default_src) = match interior.find("||") {
        Some(i) => (interior[..i].trim(), Some(interior[i + 2..].trim())),
        None => (interior.trim(), None),
    };
    // A trailing `?` on the primary marks it optional.
    let (primary_src, optional) = match primary_src.strip_suffix('?') {
        Some(s) => (s.trim_end(), true),
        None => (primary_src, false),
    };
    Ok(ParsedToken {
        primary: parse_expr(primary_src)?,
        optional,
        default: default_src.map(parse_expr).transpose()?,
    })
}

fn parse_expr(s: &str) -> Result<Expr<'_>, String> {
    if s.is_empty() {
        return Err("empty placeholder".to_string());
    }
    if let Some(inner) = s.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')) {
        return Ok(Expr::Literal(inner));
    }
    if s == "url" || s.starts_with("url.") {
        return Ok(Expr::Url(parse_url_sel(s)?));
    }
    Ok(Expr::Data(s))
}

fn parse_url_sel(s: &str) -> Result<UrlSel<'_>, String> {
    let rest = s.strip_prefix("url.").ok_or_else(|| {
        format!("'{s}': use url.path / url.base / url.full / url.name / url.query")
    })?;
    // Section identifier up to the first `.` or `[`.
    let head_end = rest.find(['.', '[']).unwrap_or(rest.len());
    let section = &rest[..head_end];
    let tail = &rest[head_end..];
    match section {
        "name" => {
            require_empty(tail, "url.name")?;
            Ok(UrlSel::Name)
        }
        "rest" => {
            require_empty(tail, "url.rest")?;
            Ok(UrlSel::Rest)
        }
        "query" => {
            if tail.is_empty() {
                Ok(UrlSel::QueryAll)
            } else if let Some(key) = tail.strip_prefix('.') {
                if key.is_empty() || key.contains(['.', '[']) {
                    return Err(format!("'{s}': expected url.query.<key>"));
                }
                Ok(UrlSel::QueryKey(key))
            } else {
                Err(format!("'{s}': query takes a .<key>, not an index"))
            }
        }
        "path" | "base" | "full" => {
            let sec = match section {
                "path" => Section::Path,
                "base" => Section::Base,
                _ => Section::Full,
            };
            let index = if tail.is_empty() {
                None
            } else {
                Some(parse_index(tail)?)
            };
            Ok(UrlSel::Segments {
                section: sec,
                index,
            })
        }
        other => Err(format!("'{s}': unknown url section '{other}'")),
    }
}

fn parse_index(tail: &str) -> Result<Index, String> {
    let inner = tail
        .strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .ok_or_else(|| format!("malformed index '{tail}' (expected [n], [-1], or [a:b])"))?;
    if let Some((a, b)) = inner.split_once(':') {
        Ok(Index::Slice(parse_opt_int(a)?, parse_opt_int(b)?))
    } else {
        Ok(Index::At(parse_int(inner)?))
    }
}

fn parse_opt_int(s: &str) -> Result<Option<i64>, String> {
    let s = s.trim();
    if s.is_empty() {
        Ok(None)
    } else {
        parse_int(s).map(Some)
    }
}

fn parse_int(s: &str) -> Result<i64, String> {
    s.trim()
        .parse::<i64>()
        .map_err(|_| format!("'{s}' is not an integer index"))
}

fn require_empty(tail: &str, what: &str) -> Result<(), String> {
    if tail.is_empty() {
        Ok(())
    } else {
        Err(format!("{what} takes no index or key"))
    }
}

fn eval_token(interior: &str, url: &UrlView, data: &Map<String, Value>) -> Result<Token, String> {
    let parsed = parse_token(interior)?;
    if let Some(v) = eval_expr(&parsed.primary, url, data)? {
        if !v.is_empty() {
            return Ok(Token::Value(v));
        }
    }
    if let Some(def) = &parsed.default {
        return Ok(Token::Value(eval_expr(def, url, data)?.unwrap_or_default()));
    }
    if parsed.optional {
        return Ok(Token::Elide);
    }
    Err(format!(
        "'{interior}' resolved to nothing (mark it optional with `?` or give a `|| default`)"
    ))
}

fn eval_expr(
    expr: &Expr,
    url: &UrlView,
    data: &Map<String, Value>,
) -> Result<Option<String>, String> {
    match expr {
        Expr::Literal(s) => Ok(Some(s.to_string())),
        Expr::Data(path) => Ok(eval_data(path, data)),
        Expr::Url(sel) => Ok(eval_url(sel, url)),
    }
}

fn eval_url(sel: &UrlSel, url: &UrlView) -> Option<String> {
    match sel {
        UrlSel::Name => url.name.filter(|s| !s.is_empty()).map(str::to_string),
        UrlSel::Rest => (!url.rest.is_empty()).then(|| url.rest.to_string()),
        UrlSel::QueryAll => (!url.query.is_empty()).then(|| url.query.to_string()),
        UrlSel::QueryKey(key) => query_get(url.query, key),
        UrlSel::Segments { section, index } => {
            let segs: Vec<&str> = match section {
                Section::Path => url.path.to_vec(),
                Section::Base => url.base.to_vec(),
                Section::Full => url.base.iter().chain(url.path.iter()).copied().collect(),
            };
            match index {
                None => join_nonempty(&segs),
                Some(Index::At(i)) => norm_index(*i, segs.len()).map(|n| segs[n].to_string()),
                Some(Index::Slice(a, b)) => {
                    let len = segs.len() as i64;
                    let start = norm_bound(a.unwrap_or(0), len);
                    let end = norm_bound(b.unwrap_or(len), len);
                    if start >= end {
                        None
                    } else {
                        join_nonempty(&segs[start as usize..end as usize])
                    }
                }
            }
        }
    }
}

fn join_nonempty(segs: &[&str]) -> Option<String> {
    if segs.is_empty() {
        None
    } else {
        Some(segs.join("/"))
    }
}

fn norm_index(i: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let idx = if i < 0 { len + i } else { i };
    (idx >= 0 && idx < len).then_some(idx as usize)
}

fn norm_bound(x: i64, len: i64) -> i64 {
    let v = if x < 0 { len + x } else { x };
    v.clamp(0, len)
}

/// Data-plane dot-path walk over the scope (query/body/captured vars). Returns
/// `None` if any segment is missing or the value is null; a string value is
/// returned raw, other JSON values via `to_string()`.
fn eval_data(path: &str, data: &Map<String, Value>) -> Option<String> {
    let mut parts = path.split('.');
    let head = parts.next().unwrap_or("");
    let mut value = data.get(head)?.clone();
    for key in parts {
        value = match value {
            Value::Object(mut o) => o.remove(key)?,
            _ => return None,
        };
    }
    match value {
        Value::Null => None,
        Value::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

/// First value of `key` in a raw query string, percent-decoded (`+` → space).
fn query_get(query: &str, key: &str) -> Option<String> {
    query.split('&').filter(|p| !p.is_empty()).find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (k == key).then(|| {
            percent_encoding::percent_decode_str(v)
                .decode_utf8_lossy()
                .replace('+', " ")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn url<'a>(path: &'a [&'a str], base: &'a [&'a str], query: &'a str) -> UrlView<'a> {
        UrlView {
            path,
            base,
            name: path.last().copied(),
            query,
            rest: "",
        }
    }

    fn r(pattern: &str, u: &UrlView) -> Result<String, RsError> {
        resolve(pattern, u, &Map::new())
    }

    #[test]
    fn positional_segments_from_start_and_end() {
        let segs = ["xyz", "qqq", "abc"];
        let u = url(&segs, &[], "");
        assert_eq!(r("a/${url.path[0]}/b", &u).unwrap(), "a/xyz/b");
        assert_eq!(r("a/${url.path[1]}/b", &u).unwrap(), "a/qqq/b");
        assert_eq!(r("a/${url.path[-1]}/b", &u).unwrap(), "a/abc/b");
        assert_eq!(r("a/${url.path[-2]}/b", &u).unwrap(), "a/qqq/b");
    }

    #[test]
    fn slices_and_whole_section() {
        let segs = ["xyz", "qqq", "abc"];
        let u = url(&segs, &["files"], "");
        assert_eq!(r("${url.path[1:]}", &u).unwrap(), "qqq/abc");
        assert_eq!(r("${url.path[0:2]}", &u).unwrap(), "xyz/qqq");
        assert_eq!(r("${url.path[:2]}", &u).unwrap(), "xyz/qqq");
        assert_eq!(r("${url.path}", &u).unwrap(), "xyz/qqq/abc");
        assert_eq!(r("${url.full}", &u).unwrap(), "files/xyz/qqq/abc");
        assert_eq!(r("${url.base[0]}", &u).unwrap(), "files");
        assert_eq!(r("${url.name}", &u).unwrap(), "abc");
    }

    #[test]
    fn passthrough_forwards_the_key() {
        // The .root passthrough case: peeled segment → forwarded URL.
        let segs = ["ada@example.com"];
        let u = url(&segs, &[], "");
        assert_eq!(
            r("/data/users/${url.path[0]}", &u).unwrap(),
            "/data/users/ada@example.com"
        );
    }

    #[test]
    fn rest_forwards_verbatim_remainder() {
        // `${url.rest}` is the byte-exact service-path suffix, for transparent
        // forwarding: `/wrapped${url.rest}` reproduces the path beyond the mount.
        let rest = |s: &'static str| UrlView {
            path: &[],
            base: &[],
            name: None,
            query: "",
            rest: s,
        };
        assert_eq!(r("/wrapped${url.rest}", &rest("/")).unwrap(), "/wrapped/");
        assert_eq!(r("/wrapped${url.rest}", &rest("/x")).unwrap(), "/wrapped/x");
        assert_eq!(
            r("/wrapped${url.rest}", &rest("/a/b")).unwrap(),
            "/wrapped/a/b"
        );
        // Trailing slash within a sub-path is preserved.
        assert_eq!(
            r("/wrapped${url.rest}", &rest("/a/b/")).unwrap(),
            "/wrapped/a/b/"
        );
        // Combined with the query (forwarded separately, as documented).
        let u = UrlView {
            path: &[],
            base: &[],
            name: None,
            query: "p=1",
            rest: "/a",
        };
        assert_eq!(
            r("/wrapped${url.rest}?${url.query}", &u).unwrap(),
            "/wrapped/a?p=1"
        );
    }

    #[test]
    fn rest_takes_no_index() {
        assert!(validate("${url.rest}").is_ok());
        assert!(validate("${url.rest[0]}").is_err());
        assert!(validate("${url.rest.x}").is_err());
    }

    #[test]
    fn query_selectors() {
        let u = url(&[], &[], "page=3&q=a+b");
        assert_eq!(r("p/${url.query.page}", &u).unwrap(), "p/3");
        assert_eq!(r("p/${url.query.q}", &u).unwrap(), "p/a b");
        assert_eq!(r("p?${url.query}", &u).unwrap(), "p?page=3&q=a+b");
    }

    #[test]
    fn optional_elides_and_collapses_slash() {
        let segs = ["only"];
        let u = url(&segs, &[], "");
        assert_eq!(r("a/${url.path[5]?}/b", &u).unwrap(), "a/b");
        assert_eq!(r("a/${url.path[5]?}", &u).unwrap(), "a");
    }

    #[test]
    fn defaults_fill_when_absent() {
        let u = url(&[], &[], "");
        assert_eq!(r("p/${url.query.page || '1'}", &u).unwrap(), "p/1");
        let segs = ["x"];
        let u2 = url(&segs, &[], "");
        assert_eq!(r("${url.path[0] || 'fallback'}", &u2).unwrap(), "x");
    }

    #[test]
    fn required_missing_is_an_error() {
        let u = url(&[], &[], "");
        assert!(r("/x/${url.path[0]}", &u).is_err());
    }

    #[test]
    fn data_plane_unchanged() {
        let data = json!({ "id": "o1", "order": { "customerId": 7 } });
        let data = data.as_object().unwrap();
        assert_eq!(
            resolve("/orders/${id}", &UrlView::EMPTY, data).unwrap(),
            "/orders/o1"
        );
        assert_eq!(
            resolve("/c/${order.customerId}", &UrlView::EMPTY, data).unwrap(),
            "/c/7"
        );
        assert!(resolve("/x/${missing}", &UrlView::EMPTY, data).is_err());
        assert!(resolve("/x/${unclosed", &UrlView::EMPTY, data).is_err());
    }

    #[test]
    fn validate_rejects_malformed_patterns() {
        assert!(validate("/data/users/${url.path[0]}").is_ok());
        assert!(validate("/data/${url.query.id || 'x'}").is_ok());
        assert!(validate("${url.path[}").is_err());
        assert!(validate("${url.bogus}").is_err());
        assert!(validate("${url.path[abc]}").is_err());
        assert!(validate("${unterminated").is_err());
    }
}
