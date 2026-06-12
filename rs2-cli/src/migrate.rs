//! `rs2 migrate` — Restspace `services.json` → RS2 tenant config (PRD §15).
//!
//! Carries over: mounts (basePath → path), access roles (same role-spec
//! strings), retry policies (same shape), pipeline specs (string DSL
//! converted to the typed form and validated). Deno/TS service
//! implementations do NOT carry over — they re-bundle against the RS2
//! contract; unsupported services are skipped with warnings.

use std::sync::Arc;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::pipeline::PipelineSpec;
use rs2_core::tenant::{Adapters, Tenant, TenantConfig};
use rs2_core::wrapper::LimitTable;

pub fn migrate(input: &str, output: &str) -> Result<(), String> {
    let text =
        std::fs::read_to_string(input).map_err(|e| format!("cannot read {input}: {e}"))?;
    // Windows editors love UTF-8 BOMs; serde_json does not.
    let text = text.trim_start_matches('\u{feff}');
    let source: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("{input} is not valid JSON: {e}"))?;

    // Restspace's services map: either an object keyed by base path or an
    // array of IServiceConfig.
    let services: Vec<serde_json::Value> = match source.get("services") {
        Some(serde_json::Value::Array(items)) => items.clone(),
        Some(serde_json::Value::Object(map)) => map.values().cloned().collect(),
        _ => return Err("expected a top-level 'services' array or object".to_string()),
    };

    let mut mounts = Vec::new();
    let mut warnings = Vec::new();

    for svc in &services {
        let base_path = svc
            .get("basePath")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let source_ref = svc.get("source").and_then(|v| v.as_str()).unwrap_or("");
        let Some(service) = map_source(source_ref) else {
            warnings.push(format!(
                "{base_path}: service source '{source_ref}' has no RS2 equivalent yet — skipped \
                 (custom services re-bundle against the RS2 contract: rs2 new + rs2 deploy)"
            ));
            continue;
        };

        let mut config = serde_json::Map::new();

        // Access roles: the role-spec string grammar carries over directly.
        if let Some(access) = svc.get("access").and_then(|a| a.as_object()) {
            let mut out = serde_json::Map::new();
            for key in ["readRoles", "writeRoles", "createRoles", "manageRoles"] {
                if let Some(v) = access.get(key) {
                    out.insert(key.to_string(), v.clone());
                }
            }
            if !out.is_empty() {
                config.insert("access".to_string(), serde_json::Value::Object(out));
            }
        }

        if let Some(retry) = svc.get("retry") {
            config.insert("retry".to_string(), retry.clone());
        }

        // v1 caching ({maxAge?, cache?, sendETag?}) → the RS2 caching block.
        // sendETag is dropped: RS2 stores always emit ETags.
        if let Some(caching) = svc.get("caching").and_then(|c| c.as_object()) {
            let cache_on = caching.get("cache").and_then(|v| v.as_bool()).unwrap_or(false)
                || caching.get("maxAge").is_some();
            let mut block = serde_json::Map::new();
            if cache_on {
                block.insert("mode".to_string(), serde_json::json!("cache"));
                if let Some(max_age) = caching.get("maxAge") {
                    block.insert("maxAgeSeconds".to_string(), max_age.clone());
                }
            } else {
                block.insert("mode".to_string(), serde_json::json!("noStore"));
            }
            config.insert("caching".to_string(), serde_json::Value::Object(block));
        }

        // Pipelines are stored specs now, not config: convert the string
        // DSL to the typed envelope and hand it to the user to PUT (a v1
        // single-pipeline mount maps to the `.root` spec, preserving its
        // any-verb surface exactly).
        if service == "pipeline" {
            let spec_value = svc
                .get("pipeline")
                .or_else(|| svc.get("adapterConfig").and_then(|a| a.get("pipeline")));
            match spec_value {
                Some(value) => match PipelineSpec::from_value(value) {
                    Ok(spec) => {
                        let envelope = serde_json::json!({
                            "pipeline": serde_json::to_value(&spec).unwrap()
                        });
                        warnings.push(format!(
                            "{base_path}: pipelines are stored specs in RS2 — after deploying \
                             this config, PUT the converted envelope below to \
                             {base_path}/.pipelines/.root\n{}",
                            serde_json::to_string_pretty(&envelope).unwrap()
                        ));
                    }
                    Err(e) => {
                        warnings.push(format!("{base_path}: pipeline failed conversion: {e}"));
                        continue;
                    }
                },
                None => {
                    warnings.push(format!("{base_path}: pipeline service has no spec — skipped"));
                    continue;
                }
            }
        }

        if service == "data" {
            if svc.get("datasetName").is_some() {
                warnings.push(format!(
                    "{base_path}: 'dataset' single-dataset mode maps to a plain data mount; \
                     fix dataset paths in clients"
                ));
            }
            config.insert("enforceSchema".to_string(), serde_json::json!(true));
        }

        if service == "query" {
            warnings.push(format!(
                "{base_path}: v1 query files live in the service's private file store and cannot \
                 be migrated automatically — re-PUT each as an RS2 envelope \
                 {{\"query\": <template>, \"params\": <schema>}} to the new mount; \
                 ${{name}} placeholders carry over (now structural in JSON templates), \
                 $0 subpath params carry over as trailing URL segments"
            ));
        }

        if service == "auth" {
            if let Some(pattern) = svc.get("userUrlPattern").and_then(|v| v.as_str()) {
                warnings.push(format!(
                    "{base_path}: userUrlPattern '{pattern}' → RS2 resolves users from the \
                     'users' dataset (set auth.userDataset); password hashes migrate as-is \
                     (bcrypt verified, argon2id for new hashes); JWTs do not carry over"
                ));
            }
        }

        for ignored in ["prePipeline", "postPipeline", "infraName", "adapterSource"] {
            if svc.get(ignored).is_some() {
                warnings.push(format!(
                    "{base_path}: '{ignored}' is not carried over (see the migration guide)"
                ));
            }
        }

        mounts.push(serde_json::json!({
            "path": base_path.trim_end_matches('/'),
            "service": service,
            "config": config,
        }));
    }

    let tenant_config = serde_json::json!({ "mounts": mounts });

    // Validate by dry-building the tenant exactly as the runtime would.
    let parsed: TenantConfig = serde_json::from_value(tenant_config.clone())
        .map_err(|e| format!("migrated config failed to parse: {e}"))?;
    let tmp = std::env::temp_dir().join("rs2-migrate-check");
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(&tmp)),
        Arc::new(MemDataStore::new()),
    );
    Tenant::build("migrated", parsed, &adapters, &LimitTable::default(), None, None)
        .map_err(|e| format!("migrated config failed validation: {e}"))?;

    std::fs::write(output, serde_json::to_string_pretty(&tenant_config).unwrap())
        .map_err(|e| format!("cannot write {output}: {e}"))?;

    println!("migrated {} service(s) → {output}", mounts.len());
    for w in &warnings {
        println!("warning: {w}");
    }
    if !warnings.is_empty() {
        println!("({} warning(s) — review before deploying)", warnings.len());
    }
    Ok(())
}

/// Map a Restspace service source reference to an RS2 prebuilt service.
fn map_source(source: &str) -> Option<&'static str> {
    // Sources look like "./services/file.rsm.json" or full URLs.
    let stem = source
        .rsplit('/')
        .next()?
        .trim_end_matches(".rsm.json")
        .trim_end_matches(".json");
    match stem {
        "file" | "files" => Some("file"),
        "data" | "dataset" => Some("data"),
        "pipeline" => Some("pipeline"),
        "query" => Some("query"),
        "auth" | "user-data" | "user-filter" => Some("auth"),
        "services" => Some("services"),
        _ => None,
    }
}
