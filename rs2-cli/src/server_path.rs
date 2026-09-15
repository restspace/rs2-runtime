//! Server-path arguments, undoing Git Bash's path conversion.
//!
//! MSYS (Git Bash) rewrites any argument that starts with `/` into a Windows
//! path under its install root before the process sees it, so
//! `rs2 send /sites/index.html` arrives as `C:/Program Files/Git/sites/index.html`.
//! A server path never legitimately starts with a drive letter, so when an
//! argument does, and some prefix of it is an MSYS root (it holds
//! `usr/bin/msys-2.0.dll`), the rest is the path the user typed.

use std::path::Path;

/// clap `value_parser` for an argument naming a path on the server.
pub fn parse(arg: &str) -> Result<String, String> {
    Ok(unmangle(arg))
}

/// Recover the `/…` path MSYS converted, or return `arg` unchanged.
pub fn unmangle(arg: &str) -> String {
    let bytes = arg.as_bytes();
    let drive = bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'/' || bytes[2] == b'\\');
    if !drive {
        return arg.to_string();
    }
    let normalized = arg.replace('\\', "/");
    // Each `/` after the drive is a candidate root boundary; the shortest
    // prefix that is an MSYS install wins (roots don't nest).
    for (i, _) in normalized.match_indices('/').skip(1) {
        if is_msys_root(&normalized[..i]) {
            return normalized[i..].to_string();
        }
    }
    // A lone `/` converts to the root itself, with or without a trailing slash.
    let root = normalized.trim_end_matches('/');
    if is_msys_root(root) {
        return "/".to_string();
    }
    arg.to_string()
}

fn is_msys_root(dir: &str) -> bool {
    Path::new(dir).join("usr/bin/msys-2.0.dll").is_file()
}

#[cfg(test)]
mod tests {
    use super::unmangle;

    fn fake_root() -> (tempfile::TempDir, String) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("Git");
        std::fs::create_dir_all(root.join("usr/bin")).unwrap();
        std::fs::write(root.join("usr/bin/msys-2.0.dll"), b"").unwrap();
        let root = root.to_string_lossy().replace('\\', "/");
        (tmp, root)
    }

    #[test]
    fn plain_paths_pass_through() {
        assert_eq!(unmangle("/sites/index.html"), "/sites/index.html");
        assert_eq!(unmangle("sites"), "sites");
        assert_eq!(unmangle("C:/not/an/msys/root/x"), "C:/not/an/msys/root/x");
    }

    #[cfg(windows)]
    #[test]
    fn converted_paths_are_recovered() {
        let (_tmp, root) = fake_root();
        assert_eq!(
            unmangle(&format!("{root}/sites/index.html")),
            "/sites/index.html"
        );
        assert_eq!(unmangle(&format!("{root}/sites/")), "/sites/");
        assert_eq!(unmangle(&format!("{root}/")), "/");
        assert_eq!(unmangle(&root), "/");
    }
}
