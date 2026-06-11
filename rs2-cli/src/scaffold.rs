//! `rs2 new` — service scaffolding (PRD §11).

use std::fs;
use std::path::Path;

/// The published WIT contract, embedded so scaffolds match the runtime.
const SERVICE_WIT: &str = include_str!("../../rs2-core/wit/service.wit");

pub fn new_service(name: &str, js: bool) -> Result<(), String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err("service names are kebab-case alphanumerics".to_string());
    }
    let root = Path::new(name);
    if root.exists() {
        return Err(format!("'{name}' already exists"));
    }
    if js {
        scaffold_js(root, name)
    } else {
        scaffold_rust(root, name)
    }?;
    println!("created ./{name}");
    println!("next: cd {name}; see README.md for build/test/deploy steps");
    Ok(())
}

fn write(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::write(path, content).map_err(|e| format!("cannot write {}: {e}", path.display()))
}

fn manifest(name: &str, engine: &str) -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "name": name,
        "description": "A custom RS2 service",
        "engine": engine,
        "endpoints": [
            { "method": "GET", "path": "/", "effect": "pure" }
        ],
        "capabilities": {},
        "configSchema": { "type": "object", "properties": {} }
    }))
    .unwrap()
}

fn scaffold_rust(root: &Path, name: &str) -> Result<(), String> {
    let crate_name = name.replace('-', "_");
    write(
        &root.join("Cargo.toml"),
        &format!(
            r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.46"
serde_json = "1"
"#
        ),
    )?;
    write(&root.join("wit/service.wit"), SERVICE_WIT)?;
    write(
        &root.join("src/lib.rs"),
        &format!(
            r#"//! {name}: an RS2 custom service (Wasm component).

wit_bindgen::generate!({{ path: "wit", world: "service" }});

use rs2::service::types::{{BodyData, Header}};

struct {type_name};

impl Guest for {type_name} {{
    fn init(_config: String) -> Result<(), String> {{
        Ok(())
    }}

    fn handle(msg: Message, _config: String) -> Result<Message, String> {{
        let body = serde_json::json!({{
            "service": "{name}",
            "youCalled": format!("{{}} {{}}", msg.method, msg.url),
        }});
        Ok(Message {{
            method: msg.method,
            url: msg.url,
            headers: vec![Header {{ name: "x-service".into(), value: "{name}".into() }}],
            status: 200,
            body: Some(BodyData {{
                media_type: "application/json".into(),
                schema_ref: None,
                bytes: body.to_string().into_bytes(),
            }}),
        }})
    }}
}}

export!({type_name});
"#,
            type_name = pascal(name),
        ),
    )?;
    write(&root.join("manifest.json"), &manifest(name, "wasm"))?;
    write(
        &root.join("README.md"),
        &format!(
            r#"# {name}

An RS2 custom service (Wasm component).

```powershell
rustup target add wasm32-wasip2
cargo build --target wasm32-wasip2 --release
rs2 test .
rs2 deploy target/wasm32-wasip2/release/{crate_name}.wasm --name {name}
```

Then mount it in your tenant config:

```json
{{ "path": "/{name}", "service": "code:{name}@<version>",
   "config": {{ "grants": {{}} }} }}
```
"#
        ),
    )?;
    Ok(())
}

fn scaffold_js(root: &Path, name: &str) -> Result<(), String> {
    write(
        &root.join("service.js"),
        &format!(
            r#"// {name}: an RS2 custom service (JS).
// NOTE: the RS2 JS engine is not yet shipped; this scaffold targets the
// published contract so the service is ready when it lands.

export default {{
    async handle(msg, ctx) {{
        return {{
            status: 200,
            headers: {{ "x-service": "{name}" }},
            body: {{ service: "{name}", youCalled: `${{msg.method}} ${{msg.url}}` }},
        }};
    }}
}};
"#
        ),
    )?;
    write(&root.join("manifest.json"), &manifest(name, "js"))?;
    write(
        &root.join("README.md"),
        &format!("# {name}\n\nAn RS2 custom JS service. The JS engine ships in a later RS2 build; `rs2 deploy` will bundle with esbuild + compat shims.\n"),
    )?;
    Ok(())
}

fn pascal(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}
