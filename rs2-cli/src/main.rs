//! `rs2` — the developer CLI (PRD §11, §15).
//!
//! Verbs: `new` (scaffold a service), `dev` (run a local node), `test`
//! (validate a manifest + component), `deploy` (upload via the self-config
//! API), `migrate` (convert a Restspace `services.json`).

mod client;
mod commands;
mod config;
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
    /// Deploy a compiled component or JS bundle through the self-config API.
    Deploy {
        /// Path to the .wasm component or .js/.ts entry point.
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
        /// Bundle the entry point first (npx esbuild: npm deps resolved at
        /// build time, single-file ESM output for the sandbox).
        #[arg(long)]
        bundle: bool,
    },
    /// Build a JSX template into a deployable bundle envelope.
    Template {
        #[command(subcommand)]
        action: TemplateCommand,
    },
    /// Convert a Restspace `services.json` to an RS2 tenant config.
    Migrate {
        /// Path to the Restspace services.json.
        input: String,
        /// Output path for the RS2 tenant config.
        #[arg(short, long, default_value = "tenant.json")]
        output: String,
    },
    /// Authenticate to a server and save the token to `rsconfig.json`.
    Login {
        /// Server base URL (else `host` from rsconfig.json).
        #[arg(long)]
        host: Option<String>,
        /// Login email (else `login.email` from rsconfig.json).
        #[arg(long)]
        email: Option<String>,
        /// Password (else `RS2_PASSWORD`, else `login.password`).
        #[arg(long)]
        password: Option<String>,
    },
    /// Send a local file to a path on the server (PUT).
    Send {
        /// Destination path on the server, e.g. `/files/report.pdf`.
        path: String,
        /// Local file to upload.
        #[arg(long)]
        file: String,
        /// Content-Type (else inferred from the file extension).
        #[arg(long)]
        content_type: Option<String>,
    },
    /// Manage service mounts on the server.
    Service {
        #[command(subcommand)]
        action: ServiceCommand,
    },
    /// Run a script of `rs2` commands (one per line; aborts on first failure).
    Run {
        /// Path to the script file.
        script: String,
    },
}

#[derive(Subcommand)]
enum ServiceCommand {
    /// Add a new mount from a JSON file. Fails if the path is already mounted.
    Add {
        /// Path to the mount-spec JSON (`{ path?, service, config? }`).
        file: String,
        /// Override / supply the mount path (e.g. `/notes`).
        #[arg(long)]
        path: Option<String>,
    },
}

#[derive(Subcommand)]
enum TemplateCommand {
    /// Bundle a JSX entry (Preact) into a template envelope. The entry's
    /// default export is the component; the output is `{ "source": "…" }` to
    /// PUT to a template mount's `/<mount>/.templates/<name>`.
    Build {
        /// Path to the .jsx/.tsx entry whose default export is the component.
        entry: String,
        /// Output path (defaults to <entry-stem>.template.json).
        #[arg(long)]
        out: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = dispatch(cli.command) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

/// Execute one parsed command. Factored out of `main` so `rs2 run` can re-enter
/// it for each line of a script.
fn dispatch(command: Command) -> Result<(), String> {
    match command {
        Command::New { name, js } => scaffold::new_service(&name, js),
        Command::Dev { config } => {
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            rt.block_on(rs2_server::run(&config)).map_err(|e| e.to_string())
        }
        Command::Test { path, component } => test_service(&path, component.as_deref()),
        Command::Deploy { component, name, server, token, bundle } => {
            (if bundle { esbuild(&component) } else { Ok(component) })
                .and_then(|artifact| deploy(&artifact, &name, &server, token.as_deref()))
        }
        Command::Template { action } => match action {
            TemplateCommand::Build { entry, out } => template_build(&entry, out.as_deref()),
        },
        Command::Migrate { input, output } => migrate::migrate(&input, &output),
        Command::Login { host, email, password } => {
            commands::login(host.as_deref(), email.as_deref(), password.as_deref())
        }
        Command::Send { path, file, content_type } => {
            commands::send(&path, &file, content_type.as_deref())
        }
        Command::Service { action } => match action {
            ServiceCommand::Add { file, path } => commands::service_add(&file, path.as_deref()),
        },
        Command::Run { script } => run_script(&script),
    }
}

/// `rs2 run` — execute a script where each non-blank, non-`#` line is an `rs2`
/// invocation with `rs2` omitted. Lines run in order; the first failure aborts.
fn run_script(script: &str) -> Result<(), String> {
    let text = std::fs::read_to_string(script).map_err(|e| format!("cannot read {script}: {e}"))?;
    for (i, raw) in text.trim_start_matches('\u{feff}').lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let lineno = i + 1;
        let mut args = shlex::split(line)
            .ok_or_else(|| format!("line {lineno}: unbalanced quotes: {line}"))?;
        if matches!(args.first().map(String::as_str), Some("dev")) {
            return Err(format!("line {lineno}: `dev` runs a server and cannot be used in a script"));
        }
        args.insert(0, "rs2".to_string());
        let cli = Cli::try_parse_from(&args)
            .map_err(|e| format!("line {lineno}: {}: {}", line, e.to_string().trim()))?;
        println!("$ {line}");
        dispatch(cli.command).map_err(|e| format!("line {lineno} failed: {line}\n  {e}"))?;
    }
    Ok(())
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

/// Bundle a JS/TS entry point with esbuild (PRD §11: npm packages resolve
/// at bundle time; the sandbox runs single-file ESM against the compat
/// prelude). Native addons fail here, at build time, with esbuild's error.
fn esbuild(entry: &str) -> Result<String, String> {
    let out = std::env::temp_dir().join("rs2-bundle.js");
    let out_str = out.to_string_lossy().into_owned();
    let npx = if cfg!(windows) { "npx.cmd" } else { "npx" };
    let status = std::process::Command::new(npx)
        .args([
            "--yes",
            "esbuild",
            entry,
            "--bundle",
            "--format=esm",
            "--platform=browser",
            "--target=es2022",
            "--define:process.env.NODE_ENV=\"production\"",
            &format!("--outfile={out_str}"),
        ])
        .status()
        .map_err(|e| format!("cannot run npx esbuild (is Node.js installed?): {e}"))?;
    if !status.success() {
        return Err("esbuild failed (see its output above)".to_string());
    }
    println!("bundled {entry} → {out_str}");
    Ok(out_str)
}

/// Build a JSX template into a deployable bundle envelope (`rs2 template
/// build`). The entry's default export is a Preact component; this wraps it in
/// the render harness, bundles with esbuild (JSX → single-file ESM, Preact +
/// `preact-render-to-string` inlined), verifies it loads in the engine (with
/// `--features js`), and writes `{ "source": "<bundle>" }` — bundle, check, and
/// write in one operation. PUT that file to `/<mount>/.templates/<name>`.
fn template_build(entry: &str, out: Option<&str>) -> Result<(), String> {
    let entry_path = std::path::Path::new(entry);
    if !entry_path.exists() {
        return Err(format!("template entry '{entry}' does not exist"));
    }
    let dir = entry_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let file_name = entry_path
        .file_name()
        .ok_or_else(|| format!("invalid entry path '{entry}'"))?
        .to_string_lossy()
        .into_owned();

    // The render harness: import the user's default-exported component and
    // render it to HTML with the request body as props. Written next to the
    // entry so esbuild resolves both the relative import and the project's
    // node_modules (Preact). The harness uses `h(...)` directly (no JSX), so
    // the user's component is the only JSX, transpiled via the automatic
    // runtime against the bundled Preact.
    let harness = String::from(
        "import { h } from \"preact\";\n\
         import { renderToString } from \"preact-render-to-string\";\n",
    ) + &format!("import Template from \"./{file_name}\";\n")
        + "export default async (msg) => {\n\
         \x20 const props = (msg && msg.body) || {};\n\
         \x20 const html = renderToString(h(Template, props));\n\
         \x20 return { status: 200, headers: { \"content-type\": \"text/html; charset=utf-8\" }, body: \"<!doctype html>\" + html };\n\
         };\n";
    let harness_path = dir.join(format!(".rs2-template-harness-{}.jsx", std::process::id()));
    std::fs::write(&harness_path, harness)
        .map_err(|e| format!("cannot write build harness: {e}"))?;

    let bundled = esbuild_jsx(&harness_path.to_string_lossy());
    let _ = std::fs::remove_file(&harness_path);
    let bundle_path = bundled?;

    let source = std::fs::read_to_string(&bundle_path)
        .map_err(|e| format!("cannot read bundled template: {e}"))?;

    // Verify the bundle loads/evaluates as ESM in the engine before writing it.
    // esbuild already caught transpile/resolve errors; this catches a bundle
    // that parses but fails at module evaluation (the same smoke test the
    // server runs on deploy). Opt-in via `--features js`, like the wasm check.
    #[cfg(feature = "js")]
    {
        rs2_core::engines::js::JsEngine::new()
            .compile_check(&source)
            .map_err(|e| format!("template bundle failed the engine compile check: {e}"))?;
        println!("compile check passed");
    }
    #[cfg(not(feature = "js"))]
    println!("note: built without --features js; skipping engine compile check");

    let envelope = serde_json::json!({ "source": source });

    let out_path = out.map(String::from).unwrap_or_else(|| {
        let stem = entry_path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "template".to_string());
        dir.join(format!("{stem}.template.json")).to_string_lossy().into_owned()
    });
    std::fs::write(&out_path, serde_json::to_string(&envelope).unwrap())
        .map_err(|e| format!("cannot write {out_path}: {e}"))?;
    println!("built template {entry} → {out_path}  (PUT it to /<mount>/.templates/<name>)");
    Ok(())
}

/// Bundle a JSX entry with esbuild for the template service: same single-file
/// ESM target as [`esbuild`], plus the automatic JSX runtime resolved against
/// `preact` (so output has no imports for the sandbox's `NoopModuleLoader`).
fn esbuild_jsx(entry: &str) -> Result<String, String> {
    let out = std::env::temp_dir().join("rs2-template-bundle.js");
    let out_str = out.to_string_lossy().into_owned();
    let npx = if cfg!(windows) { "npx.cmd" } else { "npx" };
    let status = std::process::Command::new(npx)
        .args([
            "--yes",
            "esbuild",
            entry,
            "--bundle",
            "--format=esm",
            "--platform=browser",
            "--target=es2022",
            "--jsx=automatic",
            "--jsx-import-source=preact",
            "--define:process.env.NODE_ENV=\"production\"",
            &format!("--outfile={out_str}"),
        ])
        .status()
        .map_err(|e| format!("cannot run npx esbuild (is Node.js installed?): {e}"))?;
    if !status.success() {
        return Err("esbuild failed (see its output above) — are `preact` and \
                    `preact-render-to-string` installed? (npm i preact preact-render-to-string)"
            .to_string());
    }
    println!("bundled {entry} → {out_str}");
    Ok(out_str)
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
    // Deploy = keyless POST to the bundle's container (the code store is
    // content-addressed: the server derives the version name).
    let url = format!("{}/code/{name}/", server.trim_end_matches('/'));
    let mut req = ureq::post(&url).set("content-type", content_type);
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
