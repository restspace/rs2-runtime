//! Outbound TLS trust roots — the one place RS2 decides which certificate
//! authorities it will accept.
//!
//! Every outbound TLS connection in the workspace resolves its roots here: the
//! CLI's `ureq` agent, the `httpOut` adapter's agent, and the JS engine's
//! socket connector. Each of those used to compile Mozilla's root list into
//! the binary (`webpki-roots`) and read nothing else, so a server behind a
//! corporate proxy, a CI TLS-inspection appliance, or any locally trusted CA
//! was unreachable — no environment variable and no flag could add a root, and
//! the only remedy was to recompile.
//!
//! ## Where roots come from
//!
//! By default, the **union** of two sources:
//!
//! 1. The **OS trust store** (`rustls-native-certs`) — where a private CA an
//!    operator has installed comes from, and the whole point of this module.
//!    That crate also implements the conventional OpenSSL variables:
//!    `SSL_CERT_FILE` / `SSL_CERT_DIR`, when set, *replace* the platform store
//!    rather than adding to it.
//! 2. The **bundled Mozilla roots** (`webpki-roots`) — what every RS2 binary
//!    trusted before this module existed.
//!
//! Union rather than "OS store, falling back to the bundle when it is empty",
//! which reads like the more principled rule. Two measurements killed it. A
//! scratch container with no `ca-certificates` package has no OS store at all,
//! so the bundle has to be there as a floor. And Windows populates its root
//! store *lazily* — a freshly imaged machine here carried 52 roots against the
//! bundle's ~150, with the rest fetched by the OS on demand during chain
//! building, which `rustls` does not do. Preferring the OS store on either
//! platform would drop roots that work today.
//!
//! The cost of the union is that a public root an operator has explicitly
//! distrusted in the OS store is still trusted here. That is the rarer case
//! and it has an answer: `RS2_CA_ROOTS=native` uses the OS store alone.
//! `RS2_CA_ROOTS=webpki` restores the old compiled-in behaviour.
//!
//! On top of whichever applied, **`RS2_CA_FILE`** (or `rs2 --ca-file`) adds a
//! PEM bundle. Additive is deliberate, and is why this variable exists next to
//! `SSL_CERT_FILE`: an operator reaches for it to trust one extra host, and
//! silently dropping the public roots to do that turns a small problem into a
//! confusing outage.

use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use rustls::pki_types::CertificateDer;
use rustls::RootCertStore;

static CA_FILE_OVERRIDE: OnceLock<PathBuf> = OnceLock::new();

/// Add a PEM bundle to the trust roots, as `rs2 --ca-file` does — equivalent
/// to setting `RS2_CA_FILE`, for callers that take the path as an argument.
///
/// The root store is built once and shared, so this must run before the first
/// outbound TLS connection; a later call has no effect and returns `false`.
pub fn set_ca_file(path: impl Into<PathBuf>) -> bool {
    CA_FILE_OVERRIDE.set(path.into()).is_ok()
}

/// The shared client config — ring provider, TLS 1.2 and 1.3, no client auth.
/// Cheap to clone: everything behind it is shared.
pub fn client_config() -> Arc<rustls::ClientConfig> {
    trust().config.clone()
}

/// A one-line account of which trust roots are loaded and where they came
/// from, for reporting alongside a certificate failure.
pub fn trust_summary() -> &'static str {
    &trust().summary
}

/// The full explanation to attach to a certificate that could not be verified:
/// what RS2 currently trusts, and the two ways to add to it. A bare
/// `UnknownIssuer` names neither, which leaves an operator with no next step.
pub fn unknown_issuer_help() -> String {
    format!(
        "the server's certificate was not issued by a trusted authority.\n  \
         Trust roots in use: {}.\n  \
         To trust a private CA, either install it in the OS trust store, or point RS2 at the \
         PEM file: --ca-file <path> (or RS2_CA_FILE=<path>), which adds it to the roots above.",
        trust_summary()
    )
}

/// True if a transport error looks like a rejected server certificate, so the
/// caller can attach [`unknown_issuer_help`]. Matched on the rendered message
/// because `ureq` erases the `rustls` error type behind its own transport
/// error, leaving the text as the only signal.
pub fn is_certificate_error(message: &str) -> bool {
    message.contains("UnknownIssuer")
        || message.contains("invalid peer certificate")
        || message.contains("CaUsedAsEndEntity")
        || message.contains("BadSignature")
}

/// Which root sources to consult (`RS2_CA_ROOTS`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// The default: the OS trust store and the bundled roots together.
    Both,
    /// The OS trust store alone — honours a public root distrusted there.
    Native,
    /// The bundled Mozilla roots alone — the pre-`tls`-module behaviour.
    Webpki,
}

impl Mode {
    fn from_env() -> Self {
        Mode::parse(&std::env::var("RS2_CA_ROOTS").unwrap_or_default())
    }

    fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "native" | "os" | "system" => Mode::Native,
            "webpki" | "bundled" | "mozilla" => Mode::Webpki,
            _ => Mode::Both,
        }
    }

    fn wants_native(self) -> bool {
        matches!(self, Mode::Both | Mode::Native)
    }
}

/// What the platform yielded, kept separate from the assembly step below so
/// the fallback rule can be tested without a machine that happens to have (or
/// happens to lack) a populated OS trust store.
#[derive(Default)]
struct Native {
    certs: Vec<CertificateDer<'static>>,
    /// Why the store could not be read, if it could not be.
    error: Option<String>,
}

struct Trust {
    config: Arc<rustls::ClientConfig>,
    /// Where the roots came from, phrased for an error message.
    summary: String,
}

fn trust() -> &'static Trust {
    static TRUST: OnceLock<Trust> = OnceLock::new();
    TRUST.get_or_init(|| {
        let mode = Mode::from_env();
        let native = if mode.wants_native() {
            load_native()
        } else {
            Native::default()
        };
        let (store, summary) = assemble(mode, native, ca_file_path());
        let config = client_config_from(store);
        Trust { config, summary }
    })
}

fn load_native() -> Native {
    let result = rustls_native_certs::load_native_certs();
    let error = (!result.errors.is_empty()).then(|| {
        result
            .errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    });
    Native {
        certs: result.certs,
        error,
    }
}

fn ca_file_path() -> Option<PathBuf> {
    CA_FILE_OVERRIDE.get().cloned().or_else(|| {
        std::env::var("RS2_CA_FILE")
            .ok()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    })
}

/// Combine the sources into a root store, returning it with a description of
/// what went in. Pure apart from reading `ca_file` off disk, so the branch a
/// given environment takes is testable.
fn assemble(mode: Mode, native: Native, ca_file: Option<PathBuf>) -> (RootCertStore, String) {
    let mut store = RootCertStore::empty();
    let mut sources: Vec<String> = Vec::new();

    let (native_count, _) = store.add_parsable_certificates(native.certs);
    if native_count > 0 {
        // Name the override when one is in play: with `SSL_CERT_FILE` set,
        // these roots are *not* the platform store, and an operator debugging
        // a rejected certificate needs to know which of the two they got.
        match env_cert_override() {
            Some(from) => sources.push(format!("{native_count} from {from}")),
            None => sources.push(format!("{native_count} from the OS trust store")),
        }
    } else if let Some(detail) = native.error {
        sources.push(format!("the OS trust store could not be read ({detail})"));
    }

    if matches!(mode, Mode::Both | Mode::Webpki) {
        let before = store.roots.len();
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        sources.push(format!(
            "{} bundled Mozilla roots",
            store.roots.len() - before
        ));
    }

    if let Some(path) = ca_file {
        match load_ca_file(&path) {
            Ok(certs) => {
                let (added, _) = store.add_parsable_certificates(certs);
                sources.push(format!("{added} from {}", path.display()));
            }
            // A bad path is reported rather than fatal: the roots already
            // gathered may well be enough, and failing hard here would break
            // every command instead of only the connection that needed the CA.
            Err(e) => eprintln!("rs2: {e}"),
        }
    }

    let summary = if sources.is_empty() {
        "none — no certificate authority could be loaded".to_string()
    } else {
        sources.join(", ")
    };
    (store, summary)
}

fn client_config_from(store: RootCertStore) -> Arc<rustls::ClientConfig> {
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("ring supports the default protocol versions")
    .with_root_certificates(store)
    .with_no_client_auth();
    Arc::new(config)
}

/// The OpenSSL-style override `rustls-native-certs` honours internally, if the
/// operator has set one.
fn env_cert_override() -> Option<String> {
    for var in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
        if let Ok(v) = std::env::var(var) {
            if !v.is_empty() {
                return Some(format!("{var}={v}"));
            }
        }
    }
    None
}

fn load_ca_file(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = std::fs::File::open(path)
        .map_err(|e| format!("cannot read CA file {}: {e}", path.display()))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(file))
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        return Err(format!(
            "no PEM certificates found in CA file {}",
            path.display()
        ));
    }
    Ok(certs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PEM-encode a DER certificate. `rcgen` is built without its `pem`
    /// feature here, and this is the format an operator's CA bundle arrives
    /// in, so the tests exercise the same parse path they will.
    fn to_pem(der: &[u8]) -> String {
        use base64::Engine as _;
        let body = base64::engine::general_purpose::STANDARD.encode(der);
        let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
        for line in body.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(line).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
        pem
    }

    fn one_cert() -> Native {
        let ca = rcgen::generate_simple_self_signed(vec!["test-ca".to_string()]).unwrap();
        Native {
            certs: vec![ca.cert.der().clone()],
            error: None,
        }
    }

    /// The regression this whole design is arranged around: a machine with no
    /// readable trust store — a scratch container with no `ca-certificates` —
    /// must still trust the public web.
    #[test]
    fn empty_os_store_still_leaves_the_bundled_roots() {
        let native = Native {
            certs: vec![],
            error: Some("no such file or directory".to_string()),
        };
        let (store, summary) = assemble(Mode::Both, native, None);
        assert!(
            store.roots.len() > 50,
            "expected the bundled roots as a floor, got {}",
            store.roots.len()
        );
        assert!(summary.contains("bundled Mozilla roots"), "{summary}");
        assert!(summary.contains("could not be read"), "{summary}");
    }

    /// The other half: an OS store that answered is added to the bundle, never
    /// substituted for it, so a lazily-populated store (Windows) cannot drop a
    /// public root that works today.
    #[test]
    fn os_store_is_added_to_the_bundled_roots() {
        let (store, summary) = assemble(Mode::Both, one_cert(), None);
        let (bundled, _) = assemble(Mode::Webpki, Native::default(), None);
        assert_eq!(store.roots.len(), bundled.roots.len() + 1, "{summary}");
        assert!(summary.contains("OS trust store"), "{summary}");
        assert!(summary.contains("bundled"), "{summary}");
    }

    /// `native` is the escape hatch for an operator who has distrusted a
    /// public root and needs that to stick.
    #[test]
    fn native_mode_excludes_the_bundled_roots() {
        let (store, summary) = assemble(Mode::Native, one_cert(), None);
        assert_eq!(store.roots.len(), 1);
        assert!(!summary.contains("bundled"), "{summary}");
    }

    #[test]
    fn webpki_mode_ignores_the_os_store() {
        assert!(!Mode::Webpki.wants_native());
        let (store, summary) = assemble(Mode::Webpki, Native::default(), None);
        assert!(store.roots.len() > 50);
        assert!(!summary.contains("OS trust store"), "{summary}");
    }

    #[test]
    fn mode_parses_its_spellings_and_defaults_to_the_union() {
        assert_eq!(Mode::parse("native"), Mode::Native);
        assert_eq!(Mode::parse(" OS "), Mode::Native);
        assert_eq!(Mode::parse("webpki"), Mode::Webpki);
        assert_eq!(Mode::parse(""), Mode::Both);
        assert_eq!(Mode::parse("nonsense"), Mode::Both);
    }

    #[test]
    fn ca_file_adds_to_the_existing_roots() {
        let ca = rcgen::generate_simple_self_signed(vec!["extra-ca".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, to_pem(ca.cert.der())).unwrap();

        // `native` keeps the count readable; the additive rule is the point.
        let (store, summary) = assemble(Mode::Native, one_cert(), Some(path.clone()));
        // The OS root is still there; the file was added, not substituted.
        assert_eq!(store.roots.len(), 2, "{summary}");
        assert!(summary.contains("OS trust store"), "{summary}");
        assert!(summary.contains("ca.pem"), "{summary}");
    }

    #[test]
    fn unreadable_ca_file_leaves_the_other_roots_intact() {
        let (store, summary) = assemble(
            Mode::Native,
            one_cert(),
            Some(PathBuf::from("no/such/ca-bundle.pem")),
        );
        assert_eq!(store.roots.len(), 1, "{summary}");
    }

    #[test]
    fn certificate_errors_are_recognised() {
        assert!(is_certificate_error(
            "tls connection init failed: invalid peer certificate: UnknownIssuer"
        ));
        assert!(!is_certificate_error(
            "Connection Failed: connection refused"
        ));
    }

    #[test]
    fn help_text_names_the_roots_and_the_remedy() {
        let help = unknown_issuer_help();
        assert!(help.contains("--ca-file"), "{help}");
        assert!(help.contains("RS2_CA_FILE"), "{help}");
        assert!(help.contains("Trust roots in use:"), "{help}");
    }

    /// End to end over a real handshake: a server presenting a certificate
    /// from a private CA is rejected by the default roots and accepted once
    /// that CA is supplied as a PEM file — the bug, and the fix, exactly.
    #[test]
    fn private_ca_is_trusted_only_when_supplied() {
        use std::io::{Read, Write};

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().clone();
        let cert_pem_der = cert_der.to_vec();
        let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());

        let server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
        let server_config = Arc::new(server_config);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        // Two connections: the rejected attempt, then the accepted one.
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut tcp, _)) = listener.accept() else {
                    return;
                };
                let mut conn = rustls::ServerConnection::new(server_config.clone())
                    .expect("server connection");
                let _ = conn.complete_io(&mut tcp);
                let _ = conn.writer().write_all(b"ok");
                let _ = conn.complete_io(&mut tcp);
            }
        });

        let connect = |config: Arc<rustls::ClientConfig>| -> Result<(), String> {
            let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
            let mut conn =
                rustls::ClientConnection::new(config, name).map_err(|e| e.to_string())?;
            let mut tcp =
                std::net::TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
            let mut tls = rustls::Stream::new(&mut conn, &mut tcp);
            let mut buf = [0u8; 2];
            tls.read(&mut buf).map_err(|e| e.to_string())?;
            Ok(())
        };

        // Public roots only: the handshake fails, and the failure is one the
        // CLI will recognise and explain.
        let (public_only, _) = assemble(Mode::Webpki, Native::default(), None);
        let err = connect(client_config_from(public_only))
            .expect_err("a private CA must not be trusted by the public roots");
        assert!(is_certificate_error(&err), "unexpected failure: {err}");

        // Same server, with the CA supplied as a PEM file.
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, to_pem(&cert_pem_der)).unwrap();
        let (with_ca, summary) = assemble(Mode::Webpki, Native::default(), Some(ca_path));
        connect(client_config_from(with_ca))
            .unwrap_or_else(|e| panic!("private CA should be trusted ({summary}): {e}"));

        server.join().unwrap();
    }
}
