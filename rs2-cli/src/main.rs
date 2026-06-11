//! `rs2` — the developer CLI (PRD §11, §15).
//!
//! Verbs: `new` (scaffold a service), `dev` (run a local node), `test`
//! (validate a manifest + component), `deploy` (upload via the self-config
//! API), `migrate` (convert a Restspace `services.json`).

mod migrate;
mod scaffold;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "rs2", version, about = "RS2 sandboxed composable-service runtime CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scaffold a new custom service project.
    New {
        /// Project (and service) name.
        name: String,
        /// Scaffold a JS service instead of Rust. (The JS engine is not in
        /// this build yet; the scaffold targets the published contract.)
        #[arg(long)]
        js: bool,
    },
    /// Run a local development node.
    Dev {
        /// Server config path.
        #[arg(default_value = "serverConfig.json")]
        config: String,
    },
    /// Validate a service: manifest consistency + component check.
    Test {
        /// Path to the service project (containing manifest.json).
        #[arg(default_value = ".")]
        path: String,
        /// Compiled component to check (defaults to the standard target path).
        #[arg(long)]
        component: Option<String>,
    },
    /// Deploy a compiled component through the self-config API.
    Deploy {
        /// Path to the .wasm component.
        component: String,
        /// Deployed bundle name (mounts reference `code:<name>@<version>`).
        #[arg(long)]
        name: String,
        /// Server base URL (the tenant's `services` mount).
        #[arg(long, default_value = "http://127.0.0.1:3100/services")]
        server: String,
        /// Bearer token for an authenticated `services` mount.
        #[arg(long)]
        token: Option<String>,
    },
    /// Convert a Restspace `services.json` to an RS2 tenant config.
    Migrate {
        /// Path to the Restspace services.json.
        input: String,
        /// Output path for the RS2 tenant config.
        #[arg(short, long, default_value = "tenant.json")]
        output: String,
    },
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::New { name, js } => scaffold::new_service(&name, js),
        Command::Dev { config } => {
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            rt.block_on(rs2_server::run(&config)).map_err(|e| e.to_string())
        }
        Command::Test { path, component } => test_service(&path, component.as_deref()),
        Command::Deploy { component, name, server, token } => {
            deploy(&component, &name, &server, token.as_deref())
        }
        Command::Migrate { input, output } => migrate::migrate(&input, &output),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

/// Manifest consistency + component validation (PRD §11): the in-process
/// half of `rs2 test`. Engine-level conformance runs in CI via the shared
/// suite; this catches broken manifests and non-components before deploy.
fn test_service(path: &str, component: Option<&str>) -> Result<(), String> {
    let manifest_path = std::path::Path::new(path).join("manifest.json");
    let manifest_text = std::fs::read_to_string(&manifest_path)
        .map_err(|e| format!("cannot read {}: {e}", manifest_path.display()))?;
    let manifest: serde_json::Value =
        serde_json::from_str(manifest_text.trim_start_matches('\u{feff}'))
            .map_err(|e| format!("manifest.json is not valid JSON: {e}"))?;

    let mut problems = Vec::new();
    for key in ["name", "engine"] {
        if manifest.get(key).and_then(|v| v.as_str()).is_none() {
            problems.push(format!("manifest is missing required string '{key}'"));
        }
    }
    match manifest.get("engine").and_then(|v| v.as_str()) {
        Some("wasm") | Some("js") | None => {}
        Some(other) => problems.push(format!("unknown engine '{other}' (wasm | js)")),
    }
    if let Some(endpoints) = manifest.get("endpoints").and_then(|e| e.as_array()) {
        for (i, ep) in endpoints.iter().enumerate() {
            if let Some(effect) = ep.get("effect").and_then(|v| v.as_str()) {
                if !["pure", "idempotent", "keyed", "unsafe"].contains(&effect) {
                    problems.push(format!("endpoints[{i}]: unknown effect class '{effect}'"));
                }
            }
        }
    }
    if let Some(caps) = manifest.get("capabilities") {
        if !caps.is_object() {
            problems.push("'capabilities' must be an object".to_string());
        }
    }

    // Component check: explicit path, or the standard cargo target.
    let name = manifest.get("name").and_then(|v| v.as_str()).unwrap_or("service");
    let default_component = format!(
        "{path}/target/wasm32-wasip2/release/{}.wasm",
        name.replace('-', "_")
    );
    let component_path = component.map(String::from).unwrap_or(default_component);
    match std::fs::read(&component_path) {
        Err(_) => println!("note: no component at {component_path} (build with `cargo build --target wasm32-wasip2 --release`)"),
        Ok(bytes) => {
            if !bytes.starts_with(b"\0asm") {
                problems.push(format!("{component_path} is not a WebAssembly binary"));
            } else {
                #[cfg(feature = "wasm")]
                if let Err(e) = rs2_core::engines::wasm::WasmEngine::new()
                    .map_err(|e| e.to_string())
                    .and_then(|eng| eng.compile_check(&bytes).map_err(|e| e.to_string()))
                {
                    problems.push(format!("component failed to compile: {e}"));
                }
                #[cfg(not(feature = "wasm"))]
                println!("note: built without --features wasm; skipping engine compile check");
            }
        }
    }

    if problems.is_empty() {
        println!("ok: manifest and component checks passed for '{name}'");
        Ok(())
    } else {
        for p in &problems {
            eprintln!("fail: {p}");
        }
        Err(format!("{} problem(s) found", problems.len()))
    }
}

fn deploy(component: &str, name: &str, server: &str, token: Option<&str>) -> Result<(), String> {
    let bytes = std::fs::read(component).map_err(|e| format!("cannot read {component}: {e}"))?;
    // JS bundles deploy as source; anything else must be a wasm component.
    let content_type = if component.ends_with(".js") || component.ends_with(".mjs") {
        "application/javascript"
    } else if bytes.starts_with(b"\0asm") {
        "application/wasm"
    } else {
        return Err(format!("{component} is neither a .js bundle nor a WebAssembly binary"));
    };
    let url = format!("{}/code/{name}", server.trim_end_matches('/'));
    let mut req = ureq::put(&url).set("content-type", content_type);
    if let Some(token) = token {
        req = req.set("authorization", &format!("Bearer {token}"));
    }
    match req.send_bytes(&bytes) {
        Ok(resp) => {
            let text = resp
                .into_string()
                .map_err(|e| format!("unreadable deploy response: {e}"))?;
            let body: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| format!("deploy response was not JSON: {e}"))?;
            println!(
                "deployed {} → {}\nmount it with: {{ \"path\": \"/my-service\", \"service\": {} }}",
                name,
                body.get("ref").and_then(|v| v.as_str()).unwrap_or("?"),
                body.get("ref").map(|v| v.to_string()).unwrap_or_default(),
            );
            if body.get("validated") == Some(&serde_json::Value::Bool(false)) {
                println!("note: the server skipped engine validation (built without wasm)");
            }
            Ok(())
        }
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            Err(format!("deploy failed: {code} {body}"))
        }
        Err(e) => Err(format!("deploy failed: {e}")),
    }
}
