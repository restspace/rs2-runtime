//! Media types (PRD §6.2): every body has one; JSON bodies may carry a
//! schema reference serialized as `application/json; schema="<url>"`.

use std::fmt;

/// Structured directory listing type, replacing Restspace's `inode/directory+json`.
pub const DIR_JSON: &str = "application/vnd.rs2.dir+json";
pub const OCTET_STREAM: &str = "application/octet-stream";
pub const JSON: &str = "application/json";
pub const SCHEMA_JSON: &str = "application/schema+json";
pub const PROBLEM_JSON: &str = "application/problem+json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaType {
    /// The essence, e.g. `application/json` — always lowercase, no params.
    essence: String,
    /// JSON Schema reference carried as the `schema` parameter.
    schema: Option<String>,
}

impl MediaType {
    pub fn new(essence: &str) -> Self {
        MediaType {
            essence: essence.trim().to_ascii_lowercase(),
            schema: None,
        }
    }

    pub fn json() -> Self {
        Self::new(JSON)
    }

    pub fn octet_stream() -> Self {
        Self::new(OCTET_STREAM)
    }

    pub fn dir_json() -> Self {
        Self::new(DIR_JSON)
    }

    pub fn with_schema(mut self, schema_url: impl Into<String>) -> Self {
        self.schema = Some(schema_url.into());
        self
    }

    /// Parse a `Content-Type` header value, extracting the `schema` parameter.
    pub fn parse(header_value: &str) -> Self {
        let mut parts = header_value.split(';');
        let essence = parts
            .next()
            .unwrap_or(OCTET_STREAM)
            .trim()
            .to_ascii_lowercase();
        let mut schema = None;
        for param in parts {
            if let Some((name, value)) = param.split_once('=') {
                if name.trim().eq_ignore_ascii_case("schema") {
                    schema = Some(value.trim().trim_matches('"').to_string());
                }
            }
        }
        MediaType { essence, schema }
    }

    pub fn essence(&self) -> &str {
        &self.essence
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn is_json(&self) -> bool {
        self.essence.contains("/json") || self.essence.contains("+json")
    }

    pub fn is_text(&self) -> bool {
        const TEXT_TYPES: [&str; 5] = [
            "text/",
            "application/javascript",
            "application/typescript",
            "application/xml",
            "application/xhtml+xml",
        ];
        TEXT_TYPES.iter().any(|t| self.essence.starts_with(t))
    }

    pub fn is_zip(&self) -> bool {
        self.essence.starts_with("application/") && self.essence.contains("zip")
    }

    pub fn is_dir(&self) -> bool {
        self.essence == DIR_JSON
    }

    /// Known `(extension, essence, negotiable)` triples, in friendly-URL
    /// preference order (human-readable docs first, then structured data, then
    /// assets). This is the single source for both
    /// [`MediaType::from_extension`] and [`MediaType::known_extensions`], so
    /// the two can't drift.
    ///
    /// `negotiable` marks the extensions the file service probes for an
    /// extension-less request. Bulk media (video, audio, fonts) maps to a
    /// media type when addressed by name but is never a friendly-URL
    /// candidate: nobody writes `/clip` meaning `/clip.mp4`, and every
    /// candidate costs a store probe on a miss.
    const EXTENSION_TABLE: &'static [(&'static str, &'static str, bool)] = &[
        ("html", "text/html", true),
        ("htm", "text/html", true),
        ("md", "text/markdown", true),
        ("txt", "text/plain", true),
        ("json", JSON, true),
        ("xml", "application/xml", true),
        ("yaml", "application/yaml", true),
        ("yml", "application/yaml", true),
        ("csv", "text/csv", true),
        ("css", "text/css", true),
        ("js", "text/javascript", true),
        ("mjs", "text/javascript", true),
        ("jsx", "text/javascript", true),
        ("ts", "application/typescript", true),
        ("tsx", "application/typescript", true),
        ("svg", "image/svg+xml", true),
        ("png", "image/png", true),
        ("jpg", "image/jpeg", true),
        ("jpeg", "image/jpeg", true),
        ("gif", "image/gif", true),
        ("webp", "image/webp", true),
        ("pdf", "application/pdf", true),
        ("zip", "application/zip", true),
        ("wasm", "application/wasm", true),
        // Images addressed by name only.
        ("avif", "image/avif", false),
        ("ico", "image/vnd.microsoft.icon", false),
        ("bmp", "image/bmp", false),
        ("tif", "image/tiff", false),
        ("tiff", "image/tiff", false),
        // Video.
        ("mp4", "video/mp4", false),
        ("m4v", "video/mp4", false),
        ("webm", "video/webm", false),
        ("ogv", "video/ogg", false),
        ("mov", "video/quicktime", false),
        ("mkv", "video/x-matroska", false),
        ("avi", "video/x-msvideo", false),
        ("mpeg", "video/mpeg", false),
        ("mpg", "video/mpeg", false),
        // Audio.
        ("mp3", "audio/mpeg", false),
        ("m4a", "audio/mp4", false),
        ("aac", "audio/aac", false),
        ("ogg", "audio/ogg", false),
        ("oga", "audio/ogg", false),
        ("opus", "audio/ogg", false),
        ("weba", "audio/webm", false),
        ("wav", "audio/wav", false),
        ("flac", "audio/flac", false),
        // Fonts.
        ("woff", "font/woff", false),
        ("woff2", "font/woff2", false),
        ("ttf", "font/ttf", false),
        ("otf", "font/otf", false),
    ];

    /// Media type from a file extension; `None` if unknown.
    pub fn from_extension(ext: &str) -> Option<Self> {
        let ext = ext.trim_start_matches('.').to_ascii_lowercase();
        Self::EXTENSION_TABLE
            .iter()
            .find(|(e, _, _)| *e == ext)
            .map(|(_, essence, _)| Self::new(essence))
    }

    /// Every negotiable `(extension-without-dot, media type)` pair, in
    /// friendly-URL preference order. Used to probe candidate files for an
    /// extension-less request.
    pub fn known_extensions() -> impl Iterator<Item = (&'static str, MediaType)> {
        Self::EXTENSION_TABLE
            .iter()
            .filter(|(_, _, negotiable)| *negotiable)
            .map(|(ext, essence, _)| (*ext, Self::new(essence)))
    }

    /// The canonical extension for an essence — the reverse of the extension
    /// map, used to name a server-named file (keyless POST) from its declared
    /// media type. The first table entry wins, so `image/jpeg` is `jpg`, not
    /// `jpeg`. `None` when nothing in the table claims the essence.
    pub fn canonical_extension(&self) -> Option<&'static str> {
        Self::EXTENSION_TABLE
            .iter()
            .find(|(_, essence, _)| *essence == self.essence)
            .map(|(ext, _, _)| *ext)
    }

    /// Determine a media type for a stored file path (extension map),
    /// falling back to `application/octet-stream` — sniffing is never used.
    pub fn for_path(path: &str) -> Self {
        path.rsplit_once('.')
            .and_then(|(_, ext)| Self::from_extension(ext))
            .unwrap_or_else(Self::octet_stream)
    }
}

impl fmt::Display for MediaType {
    /// Wire form, including the `schema` parameter when present.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.schema {
            Some(s) => write!(f, "{}; schema=\"{}\"", self.essence, s),
            None => write!(f, "{}", self.essence),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_schema_parameter() {
        let mt = MediaType::parse("application/json; schema=\"https://t.example/ds/.schema.json\"");
        assert_eq!(mt.essence(), "application/json");
        assert_eq!(mt.schema(), Some("https://t.example/ds/.schema.json"));
        assert!(mt.is_json());
    }

    #[test]
    fn round_trips_to_wire_form() {
        let mt = MediaType::json().with_schema("/orders/.schema.json");
        assert_eq!(
            mt.to_string(),
            "application/json; schema=\"/orders/.schema.json\""
        );
        assert_eq!(MediaType::parse(&mt.to_string()), mt);
    }

    #[test]
    fn classifies_types() {
        assert!(MediaType::new("application/vnd.rs2.dir+json").is_json());
        assert!(MediaType::new("text/html").is_text());
        assert!(MediaType::new("application/zip").is_zip());
        assert!(!MediaType::new("image/png").is_text());
    }

    #[test]
    fn maps_media_and_font_extensions() {
        for (path, essence) in [
            ("clips/intro.mp4", "video/mp4"),
            ("clips/intro.webm", "video/webm"),
            ("clips/intro.MOV", "video/quicktime"),
            ("audio/theme.mp3", "audio/mpeg"),
            ("audio/theme.ogg", "audio/ogg"),
            ("fonts/inter.woff2", "font/woff2"),
            ("favicon.ico", "image/vnd.microsoft.icon"),
        ] {
            assert_eq!(MediaType::for_path(path).essence(), essence, "{path}");
        }
    }

    /// Bulk media maps by name but must not join the extension-less probe
    /// list — each candidate there costs a store probe on a miss.
    #[test]
    fn media_extensions_are_not_negotiated() {
        let negotiable: Vec<&str> = MediaType::known_extensions().map(|(e, _)| e).collect();
        assert!(negotiable.contains(&"html"));
        assert!(negotiable.contains(&"png"));
        for ext in ["mp4", "webm", "mp3", "woff2", "ico"] {
            assert!(!negotiable.contains(&ext), "{ext} should not be negotiable");
            assert!(MediaType::from_extension(ext).is_some(), "{ext} unmapped");
        }
    }

    #[test]
    fn path_mapping_never_sniffs() {
        assert_eq!(
            MediaType::for_path("a/b/c.json").essence(),
            "application/json"
        );
        assert_eq!(
            MediaType::for_path("a/b/mystery").essence(),
            "application/octet-stream"
        );
    }
}
