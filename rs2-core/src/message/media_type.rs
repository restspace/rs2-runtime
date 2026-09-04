//! Media types (PRD §6.2): every body has one; JSON bodies may carry a
//! schema reference serialized as `application/json; schema="<url>"`.

use std::fmt;

use super::media_type_table::{CANONICAL_EXTENSIONS, EXTENSION_TABLE, NEGOTIABLE_EXTENSIONS};

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

    /// Media type from a file extension; `None` if unknown. The table is
    /// sorted, so this is a binary search — a directory listing types one path
    /// per entry, which puts it on the hot path.
    pub fn from_extension(ext: &str) -> Option<Self> {
        let ext = ext.trim_start_matches('.').to_ascii_lowercase();
        EXTENSION_TABLE
            .binary_search_by(|(e, _)| (*e).cmp(ext.as_str()))
            .ok()
            .map(|i| Self::new(EXTENSION_TABLE[i].1))
    }

    /// The `(extension-without-dot, media type)` pairs the file service probes
    /// for an extension-less request, in friendly-URL preference order.
    ///
    /// Deliberately a short list, not the whole map: every candidate costs a
    /// store probe on a miss, and nobody writes `/clip` meaning `/clip.mp4`.
    /// Bulk media is served when addressed by name and never negotiated.
    pub fn known_extensions() -> impl Iterator<Item = (&'static str, MediaType)> {
        NEGOTIABLE_EXTENSIONS
            .iter()
            .map(|ext| (*ext, Self::from_extension(ext).unwrap_or_else(Self::octet_stream)))
    }

    /// The canonical extension for this essence — the reverse of the extension
    /// map, used to name a server-named file (keyless POST) from its declared
    /// media type. Every entry round-trips (the extension maps back to this
    /// essence), so the file that gets named is served as what was posted.
    /// `None` when no extension claims the essence.
    pub fn canonical_extension(&self) -> Option<&'static str> {
        CANONICAL_EXTENSIONS
            .binary_search_by(|(essence, _)| (*essence).cmp(self.essence.as_str()))
            .ok()
            .map(|i| CANONICAL_EXTENSIONS[i].1)
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
            // The long tail the generated map brought in.
            ("docs/report.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            ("book.epub", "application/epub+zip"),
            ("captions.vtt", "text/vtt"),
            ("stream.m3u8", "application/vnd.apple.mpegurl"),
            ("archive.tar.gz", "application/gzip"),
            ("photo.heic", "image/heic"),
        ] {
            assert_eq!(MediaType::for_path(path).essence(), essence, "{path}");
        }
    }

    /// A `.ts` in a tenant's files is TypeScript source, not the MPEG
    /// transport stream nginx (and so mime-db) calls it.
    #[test]
    fn rs2_pins_beat_the_aggregate() {
        assert_eq!(MediaType::for_path("src/app.ts").essence(), "application/typescript");
        assert_eq!(MediaType::for_path("src/app.tsx").essence(), "application/typescript");
        assert_eq!(MediaType::for_path("feed.xml").essence(), "application/xml");
        assert_eq!(MediaType::for_path("app.js").essence(), "text/javascript");
    }

    /// Bulk media maps by name but must not join the extension-less probe
    /// list — each candidate there costs a store probe on a miss.
    #[test]
    fn media_extensions_are_not_negotiated() {
        let negotiable: Vec<&str> = MediaType::known_extensions().map(|(e, _)| e).collect();
        assert_eq!(negotiable.first(), Some(&"html"));
        assert!(negotiable.contains(&"png"));
        assert_eq!(negotiable.len(), NEGOTIABLE_EXTENSIONS.len());
        for ext in ["mp4", "webm", "mp3", "woff2", "ico", "docx"] {
            assert!(!negotiable.contains(&ext), "{ext} should not be negotiable");
            assert!(MediaType::from_extension(ext).is_some(), "{ext} unmapped");
        }
        // Every negotiable extension must actually map.
        for (ext, mt) in MediaType::known_extensions() {
            assert_ne!(mt.essence(), OCTET_STREAM, "negotiable .{ext} has no media type");
        }
    }

    /// The reverse map names a server-named file (keyless POST). Every entry
    /// must map back to its own essence, or the file would be served as
    /// something other than what was posted.
    #[test]
    fn canonical_extensions_round_trip() {
        for (essence, ext) in CANONICAL_EXTENSIONS {
            let back = MediaType::from_extension(ext);
            assert_eq!(
                back.as_ref().map(MediaType::essence),
                Some(*essence),
                "{essence} -> .{ext} does not round-trip"
            );
        }
        // The ones people actually post.
        for (essence, ext) in [
            ("video/mp4", "mp4"),
            ("audio/mpeg", "mp3"),
            ("image/jpeg", "jpg"),
            ("text/javascript", "js"),
            ("application/pdf", "pdf"),
        ] {
            assert_eq!(MediaType::new(essence).canonical_extension(), Some(ext));
        }
        assert_eq!(MediaType::new("application/x-not-a-real-type").canonical_extension(), None);
    }

    /// The tables are binary-searched, so a sort slip would silently lose rows.
    #[test]
    fn generated_tables_are_sorted_and_whole() {
        assert!(EXTENSION_TABLE.len() > 1000, "the map should be exhaustive");
        assert!(EXTENSION_TABLE.windows(2).all(|w| w[0].0 < w[1].0), "EXTENSION_TABLE unsorted");
        assert!(
            CANONICAL_EXTENSIONS.windows(2).all(|w| w[0].0 < w[1].0),
            "CANONICAL_EXTENSIONS unsorted"
        );
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
