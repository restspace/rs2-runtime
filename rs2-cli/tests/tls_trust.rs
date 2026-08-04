//! `rs2` against a server whose certificate comes from a private CA — the
//! corporate-proxy / TLS-inspection / locally-trusted-CA case.
//!
//! Until the trust roots moved to `rs2_core::tls`, the CLI carried Mozilla's
//! root list compiled into the binary and consulted nothing else, so this was
//! unreachable and no environment variable or flag could change that. The test
//! drives the real binary against a real TLS server, once per way an operator
//! can name a CA, plus the bare case that must still fail (and must say why).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;

/// PEM-encode a DER certificate — the form a CA bundle reaches an operator in,
/// and the parse path `--ca-file` actually takes.
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

/// A TLS server on loopback presenting `cert`, answering anything with `201`.
/// Returns the port and the CA in PEM form. The accept loop runs detached: the
/// test makes several connections and the process ends when it does.
fn serve() -> (u16, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])
        .expect("self-signed cert for loopback");
    let cert_der = cert.cert.der().clone();
    let pem = to_pem(&cert_der);
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());

    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert_der], key)
    .unwrap();
    let config = Arc::new(config);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for tcp in listener.incoming() {
            let Ok(mut tcp) = tcp else { continue };
            let config = config.clone();
            std::thread::spawn(move || {
                let Ok(mut conn) = rustls::ServerConnection::new(config) else {
                    return;
                };
                let mut tls = rustls::Stream::new(&mut conn, &mut tcp);
                let mut buf = [0u8; 8192];
                // One read is enough: the request and its few-byte body arrive
                // together, and the reply is the same either way.
                if tls.read(&mut buf).is_err() {
                    return;
                }
                let _ = tls.write_all(b"HTTP/1.1 201 Created\r\ncontent-length: 0\r\n\r\n");
                let _ = tls.flush();
            });
        }
    });
    (port, pem)
}

/// A working directory holding `rsconfig.json` pointing at the test server,
/// plus the file `rs2 send` will upload.
fn workspace(port: u16, ca_file: Option<&str>) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let ca = match ca_file {
        Some(path) => format!(",\n  \"caFile\": {}", serde_json::to_string(path).unwrap()),
        None => String::new(),
    };
    std::fs::write(
        dir.path().join("rsconfig.json"),
        format!("{{\n  \"host\": \"https://127.0.0.1:{port}\"{ca}\n}}\n"),
    )
    .unwrap();
    std::fs::write(dir.path().join("payload.txt"), b"hello").unwrap();
    dir
}

struct Outcome {
    ok: bool,
    output: String,
}

fn run_send(dir: &tempfile::TempDir, args: &[&str], env: &[(&str, &str)]) -> Outcome {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rs2"));
    cmd.current_dir(dir.path()).args(args).args([
        "send",
        "/f/payload.txt",
        "--file",
        "payload.txt",
    ]);
    for (k, v) in env {
        cmd.env(k, v);
    }
    // Inherited proxy settings would send these loopback requests elsewhere.
    cmd.env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy");
    let out = cmd.output().expect("run rs2");
    Outcome {
        ok: out.status.success(),
        output: format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    }
}

#[test]
fn private_ca_reachable_by_flag_env_and_config() {
    let (port, ca_pem) = serve();
    let ca_dir = tempfile::tempdir().unwrap();
    let ca_path = ca_dir.path().join("corporate-ca.pem");
    std::fs::write(&ca_path, &ca_pem).unwrap();
    let ca = ca_path.to_string_lossy().to_string();

    // Nothing supplied: still refused, as it must be — but the message now
    // names the roots in play and the two ways to add one. The old text was
    // `UnknownIssuer` and nothing else, which left no next step.
    let bare = run_send(&workspace(port, None), &[], &[]);
    assert!(
        !bare.ok,
        "an untrusted CA must not be accepted: {}",
        bare.output
    );
    assert!(
        bare.output.contains("Trust roots in use:"),
        "the failure should name the trust source: {}",
        bare.output
    );
    assert!(
        bare.output.contains("--ca-file"),
        "the failure should name the remedy: {}",
        bare.output
    );

    // `--ca-file`
    let by_flag = run_send(&workspace(port, None), &["--ca-file", &ca], &[]);
    assert!(
        by_flag.ok,
        "--ca-file should trust the CA: {}",
        by_flag.output
    );

    // `RS2_CA_FILE`
    let by_env = run_send(&workspace(port, None), &[], &[("RS2_CA_FILE", &ca)]);
    assert!(
        by_env.ok,
        "RS2_CA_FILE should trust the CA: {}",
        by_env.output
    );

    // `caFile` in rsconfig.json, so a repo carries it with its server identity.
    let by_config = run_send(&workspace(port, Some(&ca)), &[], &[]);
    assert!(
        by_config.ok,
        "rsconfig caFile should trust the CA: {}",
        by_config.output
    );

    // `SSL_CERT_FILE`, which is what operators reach for first — honoured by
    // `rustls-native-certs` with its conventional replace-the-store meaning.
    let by_openssl_var = run_send(&workspace(port, None), &[], &[("SSL_CERT_FILE", &ca)]);
    assert!(
        by_openssl_var.ok,
        "SSL_CERT_FILE should trust the CA: {}",
        by_openssl_var.output
    );
}

/// The regression risk in preferring the OS trust store: an ordinary public
/// certificate must still verify, both from the OS store (`auto` on a normal
/// host) and from the bundled roots (`webpki`, standing in for a scratch
/// container with no `ca-certificates` installed).
///
/// Ignored by default because it is the one test here that reaches the public
/// internet — the hermetic half of this guarantee is
/// `rs2_core::tls::tests::empty_os_store_falls_back_to_bundled_roots`. Run it
/// with `cargo test -p rs2-cli -- --ignored`.
#[test]
#[ignore = "reaches the public internet"]
fn public_certificates_verify_under_every_root_mode() {
    for env in [
        vec![],
        vec![("RS2_CA_ROOTS", "webpki")],
        vec![("RS2_CA_ROOTS", "native")],
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("rsconfig.json"),
            "{ \"host\": \"https://example.com\" }\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("payload.txt"), b"hello").unwrap();
        let out = run_send(&dir, &[], &env);
        // A public host with a public certificate: the handshake must succeed,
        // so whatever comes back is an HTTP answer rather than a transport
        // failure. The upload itself is of course refused.
        assert!(
            !out.output.contains("Trust roots in use:") && !out.output.contains("UnknownIssuer"),
            "public roots should verify under {env:?}: {}",
            out.output
        );
    }
}
