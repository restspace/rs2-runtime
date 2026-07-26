//! Query-parameter parsing and canonicalization. Everything here is pure:
//! parse → clamp → apply defaults → snap to the width ladder → emit a
//! canonical string, so every equivalent request shares one cache entry and
//! bad input fails before any I/O.

/// Mount-config knobs (all optional in config; defaults here).
#[derive(Debug, Clone)]
pub struct Config {
    pub max_width: u32,
    pub max_height: u32,
    pub max_source_pixels: u64,
    pub default_quality: u8,
    /// Snap ladder for width-only requests (`w` given, `h` absent): the
    /// requested width rounds up to the next rung (or the top rung), bounding
    /// cache cardinality on public mounts. Empty ⇒ no snapping.
    pub widths: Vec<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_width: 4096,
            max_height: 4096,
            max_source_pixels: 16_000_000,
            default_quality: 78,
            widths: Vec::new(),
        }
    }
}

impl Config {
    /// Read the knobs from the mount config JSON (absent keys keep defaults;
    /// wrong-typed keys are a config error).
    pub fn from_json(cfg: &serde_json::Value) -> Result<Config, String> {
        let mut out = Config::default();
        let uint = |v: &serde_json::Value, key: &str| -> Result<Option<u64>, String> {
            match v.get(key) {
                None => Ok(None),
                Some(n) => n
                    .as_u64()
                    .filter(|n| *n > 0)
                    .map(Some)
                    .ok_or_else(|| format!("config '{key}' must be a positive integer")),
            }
        };
        if let Some(n) = uint(cfg, "maxWidth")? {
            out.max_width = n as u32;
        }
        if let Some(n) = uint(cfg, "maxHeight")? {
            out.max_height = n as u32;
        }
        if let Some(n) = uint(cfg, "maxSourcePixels")? {
            out.max_source_pixels = n;
        }
        if let Some(n) = uint(cfg, "defaultQuality")? {
            if n > 100 {
                return Err("config 'defaultQuality' must be 1..=100".to_string());
            }
            out.default_quality = n as u8;
        }
        if let Some(w) = cfg.get("widths") {
            let arr = w
                .as_array()
                .ok_or("config 'widths' must be an array of positive integers")?;
            let mut widths = Vec::with_capacity(arr.len());
            for v in arr {
                widths.push(
                    v.as_u64()
                        .filter(|n| *n > 0)
                        .ok_or("config 'widths' must be an array of positive integers")?
                        as u32,
                );
            }
            widths.sort_unstable();
            widths.dedup();
            out.widths = widths;
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    /// Shrink to fit within the box, never enlarge (the responsive default).
    ScaleDown,
    /// Fit within the box, enlarging if needed.
    Contain,
    /// Fill the box exactly, cropping overflow (gravity picks the window).
    Cover,
    /// Stretch to the box exactly, ignoring aspect.
    Fill,
}

impl Fit {
    fn name(self) -> &'static str {
        match self {
            Fit::ScaleDown => "scale-down",
            Fit::Contain => "contain",
            Fit::Cover => "cover",
            Fit::Fill => "fill",
        }
    }
}

/// Crop focal point as fractions of the leftover space (0,0 = top-left,
/// 1,1 = bottom-right; 0.5,0.5 = center).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gravity {
    pub x: f32,
    pub y: f32,
}

pub const CENTER: Gravity = Gravity { x: 0.5, y: 0.5 };

/// Explicit pre-resize crop in source pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rect {
    pub x: u32,
    pub y: u32,
    pub w: u32,
    pub h: u32,
}

/// Output format request; `Auto` resolves against the source media type
/// (decode-free, so cache hits never decode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Auto,
    Jpeg,
    Png,
    Webp,
}

/// A parsed, clamped, snapped transform request.
#[derive(Debug, Clone, PartialEq)]
pub struct Params {
    pub w: Option<u32>,
    pub h: Option<u32>,
    pub fit: Fit,
    pub g: Gravity,
    pub rect: Option<Rect>,
    pub format: Format,
    pub quality: u8,
}

/// Percent-decode (RFC 3986); invalid escapes pass through literally.
fn pct_decode(s: &str) -> String {
    fn hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push(hi * 16 + lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_u32(key: &str, v: &str) -> Result<u32, String> {
    v.parse::<u32>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| format!("'{key}' must be a positive integer, got '{v}'"))
}

/// Parse a query string. `Ok(None)` means no transform parameters at all —
/// the passthrough path. Unknown keys are a hard 400 (they would fragment
/// the cache and hide typos).
pub fn parse(query: &str, cfg: &Config) -> Result<Option<Params>, String> {
    let mut w: Option<u32> = None;
    let mut h: Option<u32> = None;
    let mut dpr: f32 = 1.0;
    let mut fit: Option<Fit> = None;
    let mut g: Option<Gravity> = None;
    let mut rect: Option<Rect> = None;
    let mut format: Option<Format> = None;
    let mut quality: Option<u8> = None;
    let mut any = false;

    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let value = pct_decode(value);
        let value = value.as_str();
        any = true;
        match key {
            "w" => w = Some(parse_u32("w", value)?),
            "h" => h = Some(parse_u32("h", value)?),
            "dpr" => {
                let d = value
                    .parse::<f32>()
                    .ok()
                    .filter(|d| d.is_finite() && *d > 0.0)
                    .ok_or_else(|| format!("'dpr' must be a positive number, got '{value}'"))?;
                dpr = d.clamp(1.0, 3.0);
            }
            "fit" => {
                fit = Some(match value {
                    "scale-down" => Fit::ScaleDown,
                    "contain" => Fit::Contain,
                    "cover" => Fit::Cover,
                    "fill" => Fit::Fill,
                    other => return Err(format!("unknown fit '{other}'")),
                })
            }
            "g" => {
                g = Some(match value {
                    "center" | "c" => CENTER,
                    "n" => Gravity { x: 0.5, y: 0.0 },
                    "ne" => Gravity { x: 1.0, y: 0.0 },
                    "e" => Gravity { x: 1.0, y: 0.5 },
                    "se" => Gravity { x: 1.0, y: 1.0 },
                    "s" => Gravity { x: 0.5, y: 1.0 },
                    "sw" => Gravity { x: 0.0, y: 1.0 },
                    "w" => Gravity { x: 0.0, y: 0.5 },
                    "nw" => Gravity { x: 0.0, y: 0.0 },
                    other => match other.split_once(',') {
                        Some((xs, ys)) => {
                            let fx = xs.parse::<f32>().ok();
                            let fy = ys.parse::<f32>().ok();
                            match (fx, fy) {
                                (Some(x), Some(y))
                                    if (0.0..=1.0).contains(&x) && (0.0..=1.0).contains(&y) =>
                                {
                                    Gravity { x, y }
                                }
                                _ => {
                                    return Err(format!(
                                        "'g' must be a compass point or 'x,y' fractions in 0..=1, got '{other}'"
                                    ))
                                }
                            }
                        }
                        None => {
                            return Err(format!(
                                "'g' must be a compass point or 'x,y' fractions in 0..=1, got '{other}'"
                            ))
                        }
                    },
                })
            }
            "rect" => {
                let parts: Vec<&str> = value.split(',').collect();
                if parts.len() != 4 {
                    return Err(format!("'rect' must be 'x,y,w,h', got '{value}'"));
                }
                let x = parts[0]
                    .parse::<u32>()
                    .map_err(|_| format!("'rect' must be 'x,y,w,h', got '{value}'"))?;
                let y = parts[1]
                    .parse::<u32>()
                    .map_err(|_| format!("'rect' must be 'x,y,w,h', got '{value}'"))?;
                let rw = parse_u32("rect", parts[2])?;
                let rh = parse_u32("rect", parts[3])?;
                rect = Some(Rect { x, y, w: rw, h: rh });
            }
            "f" | "format" => {
                format = Some(match value {
                    "auto" => Format::Auto,
                    "jpeg" | "jpg" => Format::Jpeg,
                    "png" => Format::Png,
                    "webp" => Format::Webp,
                    other => return Err(format!("unknown format '{other}'")),
                })
            }
            "q" => {
                let q = value
                    .parse::<u8>()
                    .ok()
                    .filter(|q| (1..=100).contains(q))
                    .ok_or_else(|| format!("'q' must be 1..=100, got '{value}'"))?;
                quality = Some(q);
            }
            other => return Err(format!("unknown parameter '{other}'")),
        }
    }
    if !any {
        return Ok(None);
    }

    // Fold dpr into the requested box, then clamp to the mount's ceiling.
    let scale = |v: u32| -> u32 { ((v as f32 * dpr).round() as u32).max(1) };
    let mut w = w.map(scale).map(|v| v.min(cfg.max_width));
    let h = h.map(scale).map(|v| v.min(cfg.max_height));

    let fit = fit.unwrap_or(Fit::ScaleDown);
    match fit {
        Fit::Cover | Fit::Fill => {
            if w.is_none() || h.is_none() {
                return Err(format!("fit={} requires both 'w' and 'h'", fit.name()));
            }
        }
        Fit::Contain | Fit::ScaleDown => {
            if w.is_none() && h.is_none() && rect.is_none() {
                return Err("resizing requires 'w' or 'h' (or a 'rect' crop)".to_string());
            }
        }
    }

    // Width-only requests snap up the configured ladder.
    if h.is_none() {
        if let (Some(req), false) = (w, cfg.widths.is_empty()) {
            let snapped = cfg
                .widths
                .iter()
                .copied()
                .find(|rung| *rung >= req)
                .unwrap_or(*cfg.widths.last().unwrap());
            w = Some(snapped.min(cfg.max_width));
        }
    }

    Ok(Some(Params {
        w,
        h,
        fit,
        g: g.unwrap_or(CENTER),
        rect,
        format: format.unwrap_or(Format::Auto),
        quality: quality.unwrap_or(cfg.default_quality),
    }))
}

/// Resolve `f=auto` against the source media type without decoding: sources
/// that may carry alpha (png/gif/webp) stay PNG; photographic sources become
/// JPEG. Returns the concrete format plus its media type and extension.
pub fn resolve_format(f: Format, source_media_type: &str) -> (Format, &'static str, &'static str) {
    let concrete = match f {
        Format::Auto => match source_media_type {
            "image/png" | "image/gif" | "image/webp" => Format::Png,
            _ => Format::Jpeg,
        },
        other => other,
    };
    match concrete {
        Format::Jpeg => (Format::Jpeg, "image/jpeg", "jpg"),
        Format::Png => (Format::Png, "image/png", "png"),
        Format::Webp => (Format::Webp, "image/webp", "webp"),
        Format::Auto => unreachable!("auto resolves above"),
    }
}

/// The canonical parameter string: fixed key order, defaults omitted, the
/// *resolved* format included — the cache-key component that makes every
/// equivalent request (param order, dpr folding, snapping, auto format)
/// share one derivative.
pub fn canonical(p: &Params, resolved: Format) -> String {
    let mut out = String::new();
    let mut push = |k: &str, v: String| {
        if !out.is_empty() {
            out.push('&');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(&v);
    };
    if let Some(w) = p.w {
        push("w", w.to_string());
    }
    if let Some(h) = p.h {
        push("h", h.to_string());
    }
    if p.fit != Fit::ScaleDown {
        push("fit", p.fit.name().to_string());
    }
    if p.g != CENTER {
        push("g", format!("{},{}", p.g.x, p.g.y));
    }
    if let Some(r) = p.rect {
        push("rect", format!("{},{},{},{}", r.x, r.y, r.w, r.h));
    }
    push("q", p.quality.to_string());
    push(
        "f",
        match resolved {
            Format::Jpeg => "jpeg",
            Format::Png => "png",
            Format::Webp => "webp",
            Format::Auto => "auto",
        }
        .to_string(),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config::default()
    }

    #[test]
    fn empty_query_is_passthrough() {
        assert_eq!(parse("", &cfg()).unwrap(), None);
    }

    #[test]
    fn unknown_keys_are_rejected() {
        assert!(parse("w=100&wat=1", &cfg()).is_err());
    }

    #[test]
    fn dpr_folds_into_dimensions() {
        let p = parse("w=320&dpr=2", &cfg()).unwrap().unwrap();
        assert_eq!(p.w, Some(640));
    }

    #[test]
    fn dpr_clamps_to_three() {
        let p = parse("w=100&dpr=10", &cfg()).unwrap().unwrap();
        assert_eq!(p.w, Some(300));
    }

    #[test]
    fn dimensions_clamp_to_config_ceiling() {
        let p = parse("w=99999", &cfg()).unwrap().unwrap();
        assert_eq!(p.w, Some(4096));
    }

    #[test]
    fn width_only_requests_snap_up_the_ladder() {
        let mut c = cfg();
        c.widths = vec![320, 640, 1280];
        let p = parse("w=400", &c).unwrap().unwrap();
        assert_eq!(p.w, Some(640));
        // Above the top rung: the top rung.
        let p = parse("w=4000", &c).unwrap().unwrap();
        assert_eq!(p.w, Some(1280));
        // With h present the ladder does not apply.
        let p = parse("w=400&h=300", &c).unwrap().unwrap();
        assert_eq!(p.w, Some(400));
    }

    #[test]
    fn cover_requires_both_dimensions() {
        assert!(parse("w=100&fit=cover", &cfg()).is_err());
        assert!(parse("w=100&h=100&fit=cover", &cfg()).is_ok());
    }

    #[test]
    fn canonical_is_order_insensitive_and_drops_defaults() {
        let a = parse("w=640&q=78&fit=scale-down", &cfg()).unwrap().unwrap();
        let b = parse("fit=scale-down&w=640", &cfg()).unwrap().unwrap();
        assert_eq!(
            canonical(&a, Format::Jpeg),
            canonical(&b, Format::Jpeg),
        );
        assert_eq!(canonical(&a, Format::Jpeg), "w=640&q=78&f=jpeg");
    }

    #[test]
    fn auto_format_resolves_from_source_type() {
        assert_eq!(resolve_format(Format::Auto, "image/jpeg").0, Format::Jpeg);
        assert_eq!(resolve_format(Format::Auto, "image/png").0, Format::Png);
        assert_eq!(resolve_format(Format::Webp, "image/jpeg").0, Format::Webp);
    }

    #[test]
    fn gravity_parses_compass_and_fractions() {
        let p = parse("w=10&h=10&fit=cover&g=ne", &cfg()).unwrap().unwrap();
        assert_eq!(p.g, Gravity { x: 1.0, y: 0.0 });
        let p = parse("w=10&h=10&fit=cover&g=0.3,0.7", &cfg()).unwrap().unwrap();
        assert_eq!(p.g, Gravity { x: 0.3, y: 0.7 });
        assert!(parse("w=10&h=10&fit=cover&g=up", &cfg()).is_err());
    }
}
