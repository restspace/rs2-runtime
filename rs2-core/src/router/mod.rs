//! Router (PRD §5.2, §9.1): tenant resolution, mount tables with
//! longest-prefix matching, and path safety enforced for all services.

use std::collections::HashMap;

use percent_encoding::percent_decode_str;

use crate::error::RsError;

/// Tenancy model (PRD §9.1). Resolution order in multi mode:
/// explicit domain map → `{tenant}.{main_domain}` subdomain.
#[derive(Debug, Clone)]
pub enum Tenancy {
    Single {
        tenant: String,
    },
    Multi {
        domain_map: HashMap<String, String>,
        main_domain: Option<String>,
    },
}

impl Tenancy {
    pub fn resolve(&self, host: &str) -> Option<String> {
        // Strip any port from the Host header.
        let host = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
        match self {
            Tenancy::Single { tenant } => Some(tenant.clone()),
            Tenancy::Multi {
                domain_map,
                main_domain,
            } => {
                if let Some(tenant) = domain_map.get(&host) {
                    return Some(tenant.clone());
                }
                if let Some(main) = main_domain {
                    let suffix = format!(".{main}");
                    if let Some(sub) = host.strip_suffix(suffix.as_str()) {
                        if !sub.is_empty() && !sub.contains('.') {
                            return Some(sub.to_string());
                        }
                    }
                }
                None
            }
        }
    }
}

/// Path safety (PRD §10.1): enforced in the router for **all** services,
/// not per-service. Rejects traversal, encoded traversal, null bytes,
/// backslashes, drive letters, and control characters.
pub fn validate_path(path: &str) -> Result<(), RsError> {
    let decoded = percent_decode_str(path).decode_utf8_lossy();
    for candidate in [path, decoded.as_ref()] {
        if candidate.contains('\0') || candidate.contains("%00") {
            return Err(RsError::path_unsafe("path contains a null byte"));
        }
        if candidate.contains('\\') {
            return Err(RsError::path_unsafe("path contains a backslash"));
        }
        if candidate.chars().any(|c| c.is_control()) {
            return Err(RsError::path_unsafe("path contains control characters"));
        }
        if candidate
            .split('/')
            .any(|seg| seg.len() >= 2 && seg.chars().all(|c| c == '.'))
        {
            return Err(RsError::path_unsafe("path contains a traversal segment"));
        }
        // Windows drive letters, e.g. `C:` anywhere in a segment start.
        if candidate.split('/').any(|seg| {
            seg.len() >= 2 && seg.as_bytes()[1] == b':' && seg.as_bytes()[0].is_ascii_alphabetic()
        }) {
            return Err(RsError::path_unsafe("path contains a drive letter"));
        }
    }
    Ok(())
}

/// A binding of a service + config to a URL path prefix within a tenant.
#[derive(Debug, Clone)]
pub struct Mount {
    /// Normalized mount prefix, no trailing slash; `""` is the root mount.
    pub base_path: String,
    /// Service reference: a prebuilt name (`file`, `data`) or `code:<name>@<version>`.
    pub service: String,
    pub config: serde_json::Value,
}

/// Mount table with longest-prefix matching on segment boundaries.
#[derive(Debug, Default)]
pub struct MountTable {
    /// Sorted by descending prefix length so the first match wins.
    mounts: Vec<Mount>,
}

impl MountTable {
    pub fn new(mut mounts: Vec<Mount>) -> Result<Self, RsError> {
        for m in &mut mounts {
            let normalized = format!("/{}", m.base_path.trim_matches('/'));
            m.base_path = if normalized == "/" {
                String::new()
            } else {
                normalized
            };
        }
        let mut seen = std::collections::HashSet::new();
        for m in &mounts {
            if !seen.insert(m.base_path.clone()) {
                return Err(RsError::bad_request(format!(
                    "duplicate mount path '{}'",
                    if m.base_path.is_empty() {
                        "/"
                    } else {
                        &m.base_path
                    }
                )));
            }
        }
        mounts.sort_by(|a, b| b.base_path.len().cmp(&a.base_path.len()));
        Ok(MountTable { mounts })
    }

    /// Longest-prefix match. A mount `/files` matches `/files` and
    /// `/files/x` but not `/filesystem`.
    pub fn route(&self, path: &str) -> Option<&Mount> {
        self.mounts.iter().find(|m| {
            m.base_path.is_empty()
                || path == m.base_path
                || path.starts_with(&format!("{}/", m.base_path))
        })
    }

    pub fn mounts(&self) -> &[Mount] {
        &self.mounts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_tenancy() {
        let single = Tenancy::Single {
            tenant: "main".into(),
        };
        assert_eq!(
            single.resolve("anything.example:8080").as_deref(),
            Some("main")
        );

        let multi = Tenancy::Multi {
            domain_map: HashMap::from([("api.acme.com".to_string(), "acme".to_string())]),
            main_domain: Some("rs2.dev".to_string()),
        };
        assert_eq!(multi.resolve("api.acme.com").as_deref(), Some("acme"));
        assert_eq!(multi.resolve("beta.rs2.dev:443").as_deref(), Some("beta"));
        assert_eq!(multi.resolve("a.b.rs2.dev"), None);
        assert_eq!(multi.resolve("other.com"), None);
    }

    #[test]
    fn rejects_unsafe_paths() {
        assert!(validate_path("/a/../b").is_err());
        assert!(validate_path("/a/%2e%2e/b").is_err());
        assert!(validate_path("/a/b%00.txt").is_err());
        assert!(validate_path("/a\\b").is_err());
        assert!(validate_path("/C:/windows").is_err());
        assert!(validate_path("/a/b/c.txt").is_ok());
        assert!(validate_path("/a/file.with.dots.txt").is_ok());
    }

    #[test]
    fn longest_prefix_wins_on_segment_boundaries() {
        let table = MountTable::new(vec![
            Mount {
                base_path: "/files".into(),
                service: "file".into(),
                config: json!({}),
            },
            Mount {
                base_path: "/files/special".into(),
                service: "data".into(),
                config: json!({}),
            },
            Mount {
                base_path: "/".into(),
                service: "file".into(),
                config: json!({}),
            },
        ])
        .unwrap();
        assert_eq!(table.route("/files/special/x").unwrap().service, "data");
        assert_eq!(table.route("/files/a.txt").unwrap().service, "file");
        assert_eq!(table.route("/filesystem").unwrap().base_path, "");
        assert_eq!(table.route("/files").unwrap().base_path, "/files");
    }

    #[test]
    fn rejects_duplicate_mounts() {
        assert!(MountTable::new(vec![
            Mount {
                base_path: "/x".into(),
                service: "file".into(),
                config: json!({})
            },
            Mount {
                base_path: "x/".into(),
                service: "data".into(),
                config: json!({})
            },
        ])
        .is_err());
    }
}
