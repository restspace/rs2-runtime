//! M3 integration tests (PRD §16, "Surface & migration"): the `query`
//! service, the generated agent surface + OpenAPI, pipeline failure
//! context, and custom code deployment.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Body, MediaType, Message};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct MutableLoader(Mutex<serde_json::Value>);

#[async_trait]
impl ConfigLoader for MutableLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.lock().unwrap().clone())
            .map_err(|e| RsError::internal(e.to_string()))
    }

    async fn load_raw(&self, _tenant: &str) -> Result<(serde_json::Value, String), RsError> {
        Ok((self.0.lock().unwrap().clone(), "v".to_string()))
    }

    async fn save_raw(
        &self,
        _tenant: &str,
        config: &serde_json::Value,
        _expected_version: Option<&str>,
    ) -> Result<String, RsError> {
        *self.0.lock().unwrap() = config.clone();
        Ok("v2".to_string())
    }
}

fn surface_config() -> serde_json::Value {
    json!({
        "mounts": [
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/q", "service": "query", "config": { "access": "open", "x-expose": ["mcp"] } },
            { "path": "/summary", "service": "pipeline", "config": {
                "access": "open",
                "x-agent": { "kind": "action", "safe": true }
            } },
            { "path": "/broken", "service": "pipeline", "config": { "access": "open" } },
            { "path": "/services", "service": "services", "config": { "access": "open" } },
            { "path": "/secret", "service": "file",
              "config": { "access": { "read": "A", "write": "A" } } }
        ]
    })
}

fn rt_with(file_root: &std::path::Path, config: serde_json::Value) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(file_root)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(MutableLoader(Mutex::new(config)));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body
        .as_mut()
        .expect("body")
        .as_json(10 * 1024 * 1024)
        .await
        .expect("json body")
}

/// Author the two demo pipelines as `.root` specs (one DSL, one typed).
async fn author_pipelines(rt: &Runtime) {
    let summary = json!({ "pipeline": [ "GET /data/orders/${id}", { "status": "$.status" } ] });
    let put = req(Method::PUT, "/summary/.pipelines/.root").with_json(&summary);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    let broken = json!({ "pipeline": { "onFail": "stop", "steps": [
        { "call": { "method": "GET", "url": "/data/orders/missing-one" } },
        { "transform": { "x": "$" } }
    ] } });
    let put = req(Method::PUT, "/broken/.pipelines/.root").with_json(&broken);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
}

async fn seed_orders(rt: &Runtime) {
    for (key, status, total) in [
        ("o1", "open", 50.0),
        ("o2", "open", 10.0),
        ("o3", "closed", 99.0),
    ] {
        let put = req(Method::PUT, &format!("/data/orders/{key}"))
            .with_json(&json!({ "status": status, "total": total }));
        assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    }
}

// ---------------------------------------------------------------------------
// query service (PRD §10.4)
// ---------------------------------------------------------------------------

/// The envelope used across the query tests: structural `${...}` params,
/// a schema with a default, and an optional clause.
fn open_orders_envelope() -> serde_json::Value {
    json!({
        "query": { "dataset": "orders",
                   "where": { "status": "${status}",
                              "total": { "op": ">=", "value": "${min}" },
                              "name": "${name?}" },
                   "orderBy": "total" },
        "params": { "type": "object", "required": ["status"],
                    "properties": { "status": { "type": "string" },
                                    "min": { "type": "number", "default": 0 },
                                    "name": { "type": "string" } } },
        "output": { "type": "array" }
    })
}

#[tokio::test]
async fn query_store_authors_validates_and_executes() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());
    seed_orders(&rt).await;

    // Authoring is a store write under the reserved subtree: invalid
    // envelopes are refused at PUT time.
    let bad = req(Method::PUT, "/q/.queries/open-orders").with_json(&json!({ "notquery": 1 }));
    assert_eq!(rt.handle(bad).await.status, Some(StatusCode::BAD_REQUEST));
    let bad_schema = req(Method::PUT, "/q/.queries/open-orders")
        .with_json(&json!({ "query": {}, "params": { "type": 42 } }));
    assert_eq!(
        rt.handle(bad_schema).await.status,
        Some(StatusCode::BAD_REQUEST)
    );

    // PUT the spec like a file; read it back; it lists as a store child.
    let put = req(Method::PUT, "/q/.queries/open-orders").with_json(&open_orders_envelope());
    let resp = rt.handle(put).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let mut read = rt.handle(req(Method::GET, "/q/.queries/open-orders")).await;
    assert_eq!(read.status, Some(StatusCode::OK));
    assert!(
        read.header("etag").is_some(),
        "FileService ETag on the spec read"
    );
    assert_eq!(body_json(&mut read).await["query"]["dataset"], "orders");
    let mut listing = rt.handle(req(Method::GET, "/q/.queries/")).await;
    let listing = body_json(&mut listing).await;
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "open-orders"),
        "{listing}"
    );

    // Execute: schema default applies (min → 0) and the optional `name`
    // clause elides when the param is absent.
    let mut resp = rt
        .handle(req(Method::POST, "/q/open-orders").with_json(&json!({ "status": "open" })))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    assert_eq!(resp.header("x-total-count"), Some("2"));
    let rows = body_json(&mut resp).await;
    let totals: Vec<f64> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["total"].as_f64().unwrap())
        .collect();
    assert_eq!(
        totals,
        vec![10.0, 50.0],
        "orderBy applied; default min=0 applied"
    );

    // Explicit min narrows; numbers stay numbers (structural substitution).
    let mut resp = rt
        .handle(req(Method::POST, "/q/open-orders").with_json(&json!({
            "status": "open", "min": 20
        })))
        .await;
    assert_eq!(resp.header("x-total-count"), Some("1"));
    assert_eq!(body_json(&mut resp).await[0]["total"], 50.0);

    // Pagination caps results but keeps the true total.
    let mut page = rt
        .handle(
            req(Method::POST, "/q/open-orders?$take=1").with_json(&json!({
                "status": "open"
            })),
        )
        .await;
    assert_eq!(page.header("x-total-count"), Some("2"));
    assert_eq!(body_json(&mut page).await.as_array().unwrap().len(), 1);

    // Any-verb execution: plain GET with query-string params, coerced to
    // the schema's declared types (min: number).
    let mut got = rt
        .handle(req(Method::GET, "/q/open-orders?status=open&min=20"))
        .await;
    assert_eq!(got.status, Some(StatusCode::OK), "{:?}", got.body);
    assert_eq!(got.header("x-total-count"), Some("1"));
    assert_eq!(body_json(&mut got).await[0]["total"], 50.0);

    // Parameters are schema-validated before execution → 422 with details.
    let mut invalid = rt
        .handle(req(Method::POST, "/q/open-orders").with_json(&json!({ "status": 42 })))
        .await;
    assert_eq!(invalid.status, Some(StatusCode::UNPROCESSABLE_ENTITY));
    let problem = body_json(&mut invalid).await;
    assert_eq!(problem["code"], "validation_failed");
    assert!(problem["errors"].as_array().is_some_and(|e| !e.is_empty()));

    // Unknown stored query → 404.
    let missing = rt
        .handle(req(Method::POST, "/q/nope").with_json(&json!({})))
        .await;
    assert_eq!(missing.status, Some(StatusCode::NOT_FOUND));

    // DELETE removes it like a file; execution then 404s too.
    assert_eq!(
        rt.handle(req(Method::DELETE, "/q/.queries/open-orders"))
            .await
            .status,
        Some(StatusCode::NO_CONTENT)
    );
    assert_eq!(
        rt.handle(req(Method::GET, "/q/.queries/open-orders"))
            .await
            .status,
        Some(StatusCode::NOT_FOUND)
    );
    assert_eq!(
        rt.handle(req(Method::POST, "/q/open-orders").with_json(&json!({ "status": "open" })))
            .await
            .status,
        Some(StatusCode::NOT_FOUND)
    );
}

#[tokio::test]
async fn query_positional_url_params_and_sql_passthrough() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());
    seed_orders(&rt).await;

    // Positional params: trailing URL segments beyond the stored spec
    // become "0", "1", … (v1's subpath params, no silent failures).
    let by_status = json!({
        "query": { "dataset": "orders", "where": { "status": "${0}" } }
    });
    let put = req(Method::PUT, "/q/.queries/orders/by-status").with_json(&by_status);
    assert_eq!(
        rt.handle(put).await.status,
        Some(StatusCode::CREATED),
        "nested spec path"
    );
    let mut resp = rt
        .handle(req(Method::POST, "/q/orders/by-status/closed"))
        .await;
    assert_eq!(resp.status, Some(StatusCode::OK), "{:?}", resp.body);
    assert_eq!(resp.header("x-total-count"), Some("1"));
    assert_eq!(body_json(&mut resp).await[0]["total"], 99.0);

    // A missing positional is a 400, never a silent empty string.
    let missing = rt.handle(req(Method::POST, "/q/orders/by-status")).await;
    assert_eq!(missing.status, Some(StatusCode::BAD_REQUEST));

    // String-language (SQL) templates store fine but pass through to the
    // adapter unsubstituted — the reference adapter declines them.
    let sql =
        json!({ "language": "sql", "query": "SELECT * FROM orders WHERE status = ${status}" });
    assert_eq!(
        rt.handle(req(Method::PUT, "/q/.queries/sql-orders").with_json(&sql))
            .await
            .status,
        Some(StatusCode::CREATED)
    );
    let resp = rt
        .handle(req(Method::POST, "/q/sql-orders").with_json(&json!({ "status": "open" })))
        .await;
    assert_eq!(
        resp.status,
        Some(StatusCode::NOT_IMPLEMENTED),
        "reference adapter is JSON-only"
    );
}

#[tokio::test]
async fn options_probes_mount_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    // OPTIONS describes the resolved mount: pattern, facets, and an Allow
    // header — one round trip, no correlation against the services list.
    let mut data = rt.handle(req(Method::OPTIONS, "/data")).await;
    assert_eq!(data.status, Some(StatusCode::OK));
    let allow = data.header("allow").unwrap().to_string();
    assert!(
        allow.contains("PATCH") && allow.contains("OPTIONS"),
        "allow: {allow}"
    );
    let doc = body_json(&mut data).await;
    assert_eq!(doc["pattern"], "store");
    assert_eq!(doc["path"], "/data");
    assert!(
        doc["facets"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f == "schema"),
        "{doc}"
    );
    assert_eq!(doc["schemaUrlPattern"], "/data/{dataset}/.schema.json");

    // A sub-path resolves to its governing mount (longest-prefix match).
    let sub = rt.handle(req(Method::OPTIONS, "/data/people/alice")).await;
    assert_eq!(sub.status, Some(StatusCode::OK));

    // The pipeline mount reports its own conversation shape.
    let mut summary = rt.handle(req(Method::OPTIONS, "/summary")).await;
    let doc = body_json(&mut summary).await;
    assert_eq!(doc["pattern"], "store-transform");

    // The probe is read-gated: an unreadable mount stays hidden.
    let secret = rt.handle(req(Method::OPTIONS, "/secret")).await;
    assert_eq!(
        secret.status,
        Some(StatusCode::UNAUTHORIZED),
        "{:?}",
        secret.status
    );
}

// ---------------------------------------------------------------------------
// agent surface + OpenAPI (PRD §12)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discovery_surface_filters_and_advertises() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    // Stored specs surface from their stores, not config: author them first.
    let put = req(Method::PUT, "/q/.queries/open-orders").with_json(&open_orders_envelope());
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
    author_pipelines(&rt).await;

    // /services catalogue: anonymous caller sees readable mounts only —
    // /secret (read: "A") is filtered out.
    let mut services = rt
        .handle(req(Method::GET, "/.well-known/rs2/services"))
        .await;
    assert_eq!(services.status, Some(StatusCode::OK));
    let doc = body_json(&mut services).await;
    let paths: Vec<&str> = doc["services"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["path"].as_str().unwrap())
        .collect();
    assert!(paths.contains(&"/data") && paths.contains(&"/summary"));
    assert!(
        !paths.contains(&"/secret"),
        "unreadable mounts are hidden: {paths:?}"
    );

    // The control surface (config/catalogue/code) is advertised explicitly,
    // so a generic admin client has one stable entry point and needn't scan
    // for the `services` mount by name.
    assert_eq!(doc["control"]["path"], "/services");
    assert_eq!(doc["control"]["config"], "/services/raw");
    assert_eq!(doc["control"]["catalogue"], "/services/catalogue");
    assert_eq!(doc["control"]["mounts"], "/services/services");
    assert_eq!(doc["control"]["code"], "/services/code/");

    // agent-surface: entities, actions (with idempotency guidance), queries
    // (with the same param schema enforced at runtime).
    let mut surface = rt
        .handle(req(Method::GET, "/.well-known/rs2/agent-surface"))
        .await;
    let doc = body_json(&mut surface).await;
    assert_eq!(doc["entities"][0]["path"], "/data");
    // Stored pipelines list as actions; the .root spec's path is the mount.
    let action = doc["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["path"] == "/summary")
        .unwrap_or_else(|| panic!("stored pipeline missing from actions: {doc}"));
    assert_eq!(action["idempotency"]["header"], "Idempotency-Key");
    assert_eq!(action["x-agent"]["kind"], "action");
    assert!(
        action["plan"]
            .as_str()
            .unwrap()
            .contains("/.pipelines/.root?$plan"),
        "{action}"
    );
    let query = &doc["queries"][0];
    assert_eq!(query["path"], "/q/open-orders");
    assert_eq!(query["params"]["required"][0], "status");

    // x-expose filtering: /q is exposed on "mcp" only.
    let mut ui = rt
        .handle(req(
            Method::GET,
            "/.well-known/rs2/agent-surface?surface=ui",
        ))
        .await;
    let doc = body_json(&mut ui).await;
    assert!(doc["queries"].as_array().unwrap().is_empty(), "{doc}");
    let mut mcp = rt
        .handle(req(
            Method::GET,
            "/.well-known/rs2/agent-surface?surface=mcp",
        ))
        .await;
    let doc = body_json(&mut mcp).await;
    assert_eq!(doc["queries"].as_array().unwrap().len(), 1);

    // OpenAPI 3.1: generated paths + the problem schema; store-patterned
    // mounts reference one shared shape (client polymorphism), and spec
    // stores expose the authoring subtree + an execution path item.
    let mut openapi = rt
        .handle(req(Method::GET, "/.well-known/rs2/openapi"))
        .await;
    let doc = body_json(&mut openapi).await;
    assert_eq!(doc["openapi"], "3.1.0");
    assert_eq!(
        doc["paths"]["/data/{dataset}/{key}"]["$ref"],
        "#/components/pathItems/StoreChild"
    );
    assert!(doc["components"]["pathItems"]["StoreContainer"]["get"].is_object());
    assert_eq!(
        doc["paths"]["/q/.queries/{specPath}"]["$ref"],
        "#/components/pathItems/SpecChild"
    );
    assert_eq!(
        doc["paths"]["/summary/.pipelines/{specPath}"]["$ref"],
        "#/components/pathItems/SpecChild"
    );
    assert!(
        doc["paths"]["/q/{queryPath}"]["get"].is_object(),
        "any-verb query execution"
    );
    assert!(
        doc["paths"]["/summary/{path}"]["get"].is_object(),
        "pipeline execution"
    );
    assert!(doc["components"]["pathItems"]["SpecChild"]["put"].is_object());
    assert!(doc["components"]["schemas"]["Problem"].is_object());

    // The surface is read-only.
    let post = rt
        .handle(req(Method::POST, "/.well-known/rs2/services"))
        .await;
    assert_eq!(post.status.unwrap().as_u16(), 405);
}

// ---------------------------------------------------------------------------
// schema + media-type self-containment (discovery == enforcement)
// ---------------------------------------------------------------------------

/// A data mount inlines each installed dataset schema into OpenAPI: the live
/// `.schema.json` is registered under components/schemas and a concrete
/// `{dataset}/{key}` path binds it — self-contained, and the same schema the
/// data service enforces on write (no drift). Schema-less datasets keep the
/// generic templated StoreChild shape.
#[tokio::test]
async fn data_dataset_schema_inlines_into_openapi() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    let schema = json!({
        "type": "object",
        "required": ["status"],
        "properties": { "status": { "type": "string" }, "total": { "type": "number" } }
    });
    let put = req(Method::PUT, "/data/orders/.schema.json").with_json(&schema);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::OK));

    let mut openapi = rt
        .handle(req(Method::GET, "/.well-known/rs2/openapi"))
        .await;
    let doc = body_json(&mut openapi).await;

    // The generic templated child remains (schema-less datasets).
    assert_eq!(
        doc["paths"]["/data/{dataset}/{key}"]["$ref"],
        "#/components/pathItems/StoreChild"
    );
    // The orders dataset gets a concrete, schema-bound child path.
    let put_op = &doc["paths"]["/data/orders/{key}"]["put"];
    assert!(put_op.is_object(), "{doc}");
    let schema_ref = put_op["requestBody"]["content"]["application/json"]["schema"]["$ref"]
        .as_str()
        .unwrap_or_else(|| panic!("schema-bound request body: {put_op}"));
    assert!(
        schema_ref.starts_with("#/components/schemas/"),
        "{schema_ref}"
    );
    let key = schema_ref.rsplit('/').next().unwrap();
    // The inlined schema is the live one — resolvable in-document, no drift.
    assert_eq!(doc["components"]["schemas"][key]["required"][0], "status");
}

/// A pipeline spec's optional `input`/`output` schemas surface on both the
/// agent surface (the action's uniform `inputSchema`/`outputSchema`) and
/// OpenAPI (the execute path item's request/response), advisory by nature.
#[tokio::test]
async fn pipeline_io_schema_surfaces() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    let envelope = json!({
        "pipeline": [ "GET /data/orders/${id}", { "status": "$.status" } ],
        "input": { "type": "object", "properties": { "id": { "type": "string" } } },
        "output": { "type": "object", "properties": { "status": { "type": "string" } } }
    });
    let put = req(Method::PUT, "/summary/.pipelines/.root").with_json(&envelope);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));

    let mut surface = rt
        .handle(req(Method::GET, "/.well-known/rs2/agent-surface"))
        .await;
    let doc = body_json(&mut surface).await;
    let action = doc["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["path"] == "/summary")
        .unwrap_or_else(|| panic!("pipeline action missing: {doc}"));
    assert_eq!(action["inputSchema"]["properties"]["id"]["type"], "string");
    assert_eq!(
        action["outputSchema"]["properties"]["status"]["type"],
        "string"
    );

    let mut openapi = rt
        .handle(req(Method::GET, "/.well-known/rs2/openapi"))
        .await;
    let doc = body_json(&mut openapi).await;
    let post = &doc["paths"]["/summary/{path}"]["post"];
    assert_eq!(
        post["requestBody"]["content"]["application/json"]["schema"]["properties"]["id"]["type"],
        "string",
        "{post}"
    );
    assert_eq!(
        post["responses"]["200"]["content"]["application/json"]["schema"]["properties"]["status"]
            ["type"],
        "string"
    );
}

/// Operations advertise their media types: the log view declares both its
/// JSON and NDJSON (text/plain) representations on the `200`, rather than a
/// bare description.
#[tokio::test]
async fn operations_advertise_media_types() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(
        dir.path(),
        json!({ "mounts": [
            { "path": "/logs", "service": "log", "config": { "access": "open" } },
        ] }),
    );

    let mut openapi = rt
        .handle(req(Method::GET, "/.well-known/rs2/openapi"))
        .await;
    let doc = body_json(&mut openapi).await;
    let content = &doc["paths"]["/logs"]["get"]["responses"]["200"]["content"];
    assert!(content["application/json"].is_object(), "{doc}");
    assert!(
        content["text/plain"].is_object(),
        "NDJSON via Accept: {content}"
    );
}

/// A deployed `code:` mount with a declared manifest appears on both the agent
/// surface (as an action carrying its I/O schemas) and OpenAPI (a path item
/// with the manifest's schemas + media types). Needs the JS engine for the
/// deploy compile-check and to build the code mount.
#[cfg(feature = "js")]
#[tokio::test]
async fn code_manifest_surfaces_on_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    let bundle = r#"export default async (msg, ctx) => ({ status: 200, body: {} });"#;
    let manifest = json!({
        "inputSchema": { "type": "object", "properties": { "q": { "type": "string" } } },
        "outputSchema": { "type": "object", "properties": { "n": { "type": "number" } } },
        "effect": "idempotent",
        "requestMediaType": "application/json",
        "responseMediaType": "application/json"
    });
    let mut deploy = req(Method::POST, "/services/code/lookup/").with_body(Body::from_bytes(
        bundle.as_bytes().to_vec(),
        MediaType::new("application/javascript"),
    ));
    deploy.set_header("x-rs2-manifest", &manifest.to_string());
    let mut resp = rt.handle(deploy).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let code_ref = body_json(&mut resp).await["ref"]
        .as_str()
        .unwrap()
        .to_string();

    let mut config = surface_config();
    config["mounts"].as_array_mut().unwrap().push(json!({
        "path": "/lookup", "service": code_ref, "config": { "access": "open", "grants": {} }
    }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    // Agent surface: the code mount lists as an action with its schemas.
    let mut surface = rt
        .handle(req(Method::GET, "/.well-known/rs2/agent-surface"))
        .await;
    let doc = body_json(&mut surface).await;
    let action = doc["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["path"] == "/lookup")
        .unwrap_or_else(|| panic!("code action missing: {doc}"));
    assert_eq!(action["effect"], "idempotent");
    assert_eq!(action["inputSchema"]["properties"]["q"]["type"], "string");
    assert_eq!(action["outputSchema"]["properties"]["n"]["type"], "number");

    // OpenAPI: a path item carrying the manifest's request/response schemas.
    let mut openapi = rt
        .handle(req(Method::GET, "/.well-known/rs2/openapi"))
        .await;
    let doc = body_json(&mut openapi).await;
    let post = &doc["paths"]["/lookup/{path}"]["post"];
    assert!(post.is_object(), "code path item missing: {doc}");
    assert_eq!(
        post["requestBody"]["content"]["application/json"]["schema"]["properties"]["q"]["type"],
        "string",
        "{post}"
    );
    let get = &doc["paths"]["/lookup/{path}"]["get"];
    assert_eq!(
        get["responses"]["200"]["content"]["application/json"]["schema"]["properties"]["n"]["type"],
        "number"
    );
}

// ---------------------------------------------------------------------------
// structured pipeline failures (PRD §12)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_failures_name_the_failing_step() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());
    author_pipelines(&rt).await;

    let mut resp = rt.handle(req(Method::GET, "/broken")).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::NOT_FOUND),
        "step failure propagates"
    );
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "not_found");
    assert_eq!(problem["pipeline"]["failedStep"], "/0", "{problem}");
    let steps = problem["pipeline"]["steps"].as_array().unwrap();
    assert_eq!(steps[0]["kind"], "call");
    assert_eq!(steps[0]["status"], 404);
}

// ---------------------------------------------------------------------------
// custom code deployment (PRD §10.6, §11)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn code_deploys_content_addressed_and_mounts() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    // A header-valid but bogus component: with the engine in the build the
    // deployment-time smoke test rejects it outright (PRD §10.6).
    let fake_component = b"\0asm-fake-component".to_vec();
    let deploy = req(Method::POST, "/services/code/echo/").with_body(Body::from_bytes(
        fake_component.clone(),
        MediaType::new("application/wasm"),
    ));
    let mut resp = rt.handle(deploy).await;
    if cfg!(feature = "wasm") {
        assert_eq!(
            resp.status.unwrap().as_u16(),
            502,
            "smoke test rejects non-components"
        );
        return;
    }
    // Featureless build: accepted unvalidated. Deploy is the store
    // contract's keyless POST: content-derived child name + Location.
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    let location = resp.header("location").unwrap().to_string();
    assert!(location.starts_with("/services/code/echo/"), "{location}");
    let body = body_json(&mut resp).await;
    let code_ref = body["ref"].as_str().unwrap().to_string();
    let version = body["version"].as_str().unwrap().to_string();
    assert!(code_ref.starts_with("code:echo@"), "{code_ref}");

    // Re-deploying identical bytes yields the same version (immutable,
    // content-addressed — PRD §14).
    let again = req(Method::POST, "/services/code/echo/").with_body(Body::from_bytes(
        fake_component.clone(),
        MediaType::new("application/wasm"),
    ));
    let mut resp2 = rt.handle(again).await;
    assert_eq!(
        body_json(&mut resp2).await["ref"].as_str().unwrap(),
        code_ref
    );

    // Store-contract listings: bundle names at the root (dir entries),
    // versions inside; the standard dir+json shape an editor UI walks.
    let mut root = rt.handle(req(Method::GET, "/services/code/")).await;
    let root_listing = body_json(&mut root).await;
    assert!(
        root_listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |e| e["name"].as_str().unwrap().trim_end_matches('/') == "echo" && e["dir"] == true
            ),
        "{root_listing}"
    );
    let mut list = rt.handle(req(Method::GET, "/services/code/echo/")).await;
    assert_eq!(list.header("x-total-count"), Some("1"));
    let listing = body_json(&mut list).await;
    let entry = &listing["entries"][0];
    let child_name = entry["name"].as_str().unwrap().to_string();
    assert!(child_name.starts_with(&version), "{listing}");
    assert!(entry.get("mountedAt").is_none(), "nothing mounts it yet");

    // Read back via the listing's child name AND the bare version.
    for path in [
        format!("/services/code/echo/{child_name}"),
        format!("/services/code/echo/{version}"),
    ] {
        let mut read = rt.handle(req(Method::GET, &path)).await;
        assert_eq!(read.status, Some(StatusCode::OK), "{path}");
        assert_eq!(read.header("etag").unwrap(), format!("\"{version}\""));
        assert!(read.header("cache-control").unwrap().contains("immutable"));
        let bytes = read
            .body
            .as_mut()
            .unwrap()
            .materialize(65536)
            .await
            .unwrap();
        assert_eq!(&bytes[..4], b"\0asm", "the stored bytes round-trip");
    }
    let missing = rt
        .handle(req(Method::GET, "/services/code/echo/feedf00ddeadbeef"))
        .await;
    assert_eq!(missing.status, Some(StatusCode::NOT_FOUND));

    // PUT child: content-addressed — only the true hash name is accepted.
    let put_ok = req(Method::PUT, &format!("/services/code/echo/{version}")).with_body(
        Body::from_bytes(fake_component.clone(), MediaType::new("application/wasm")),
    );
    assert_eq!(
        rt.handle(put_ok).await.status,
        Some(StatusCode::OK),
        "idempotent re-upload"
    );
    let put_lie = req(Method::PUT, "/services/code/echo/0000000000000000").with_body(
        Body::from_bytes(fake_component, MediaType::new("application/wasm")),
    );
    assert_eq!(rt.handle(put_lie).await.status, Some(StatusCode::CONFLICT));

    // Mount it via self-config; without the wasm feature the mount builds
    // but serves a structured 501 at request time.
    let (mut config, _) = (surface_config(), ());
    config["mounts"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "path": "/custom", "service": code_ref, "config": { "access": "open", "grants": {} } }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    // The listing now annotates the live version with its mount path…
    let mut list = rt.handle(req(Method::GET, "/services/code/echo/")).await;
    let listing = body_json(&mut list).await;
    assert_eq!(
        listing["entries"][0]["mountedAt"][0], "/custom",
        "{listing}"
    );

    // …and a mounted version refuses deletion (repoint first).
    let blocked = rt
        .handle(req(
            Method::DELETE,
            &format!("/services/code/echo/{version}"),
        ))
        .await;
    assert_eq!(blocked.status, Some(StatusCode::CONFLICT));

    let mut hit = rt.handle(req(Method::GET, "/custom/hello")).await;
    assert_eq!(hit.status, Some(StatusCode::NOT_IMPLEMENTED));
    assert_eq!(body_json(&mut hit).await["code"], "engine_unavailable");

    // Repoint away (back to the base config), then deletion works and the
    // container can be confirm-deleted.
    let put = req(Method::PUT, "/services/raw").with_json(&surface_config());
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));
    let gone = rt
        .handle(req(
            Method::DELETE,
            &format!("/services/code/echo/{version}"),
        ))
        .await;
    assert_eq!(gone.status, Some(StatusCode::NO_CONTENT));
    let dir_gone = rt
        .handle(req(Method::DELETE, "/services/code/echo/?confirm=echo"))
        .await;
    assert_eq!(dir_gone.status, Some(StatusCode::NO_CONTENT));
}

/// With the JS engine in the build: deploy a JS bundle through the
/// self-config API, mount it with a capability grant, and serve a request
/// that round-trips through the grant back into the data service.
#[cfg(feature = "js")]
#[tokio::test]
async fn deployed_js_bundle_serves_requests_with_grants() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());
    seed_orders(&rt).await;

    let bundle = r#"
        export default async (msg, ctx) => {
            const order = ctx.request("orders", { url: "/o1" });
            ctx.log("info", `loaded order o1: ${order.status}`);
            return {
                status: 200,
                headers: { "x-engine": "js" },
                body: { engine: "js", orderStatus: order.body.status },
            };
        };
    "#;
    let deploy = req(Method::POST, "/services/code/order-view/").with_body(Body::from_bytes(
        bundle.as_bytes().to_vec(),
        MediaType::new("application/javascript"),
    ));
    let mut resp = rt.handle(deploy).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED), "{:?}", resp.body);
    let body = body_json(&mut resp).await;
    assert_eq!(body["validated"], true, "compile smoke test ran");
    let code_ref = body["ref"].as_str().unwrap().to_string();

    // Mount it with a grant scoping the capability to /data/orders.
    let mut config = surface_config();
    config["mounts"].as_array_mut().unwrap().push(json!({
        "path": "/order-view", "service": code_ref,
        "config": { "access": "open", "grants": { "orders": { "prefix": "/data/orders" } } }
    }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    let mut hit = rt.handle(req(Method::GET, "/order-view/anything")).await;
    assert_eq!(hit.status, Some(StatusCode::OK), "{:?}", hit.body);
    assert_eq!(hit.header("x-engine"), Some("js"));
    let out = body_json(&mut hit).await;
    assert_eq!(
        out["orderStatus"], "open",
        "grant round-tripped into the data service"
    );

    // A broken bundle is rejected at deploy time by the compile smoke test.
    let bad = req(Method::POST, "/services/code/broken/").with_body(Body::from_bytes(
        b"export default ((((".to_vec(),
        MediaType::new("application/javascript"),
    ));
    assert_eq!(rt.handle(bad).await.status.unwrap().as_u16(), 502);
}

/// With the wasm engine in the build: deploy the real conformance echo
/// component end-to-end and invoke it through a mount. Self-skips unless
/// `RS2_CONFORMANCE_COMPONENT` points at the built guest.
#[cfg(feature = "wasm")]
#[tokio::test]
async fn deployed_wasm_component_serves_requests() {
    let Ok(component_path) = std::env::var("RS2_CONFORMANCE_COMPONENT") else {
        eprintln!("skipping: RS2_CONFORMANCE_COMPONENT not set");
        return;
    };
    let bytes = std::fs::read(&component_path).expect("read conformance component");

    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(dir.path(), surface_config());

    let deploy = req(Method::POST, "/services/code/echo/")
        .with_body(Body::from_bytes(bytes, MediaType::new("application/wasm")));
    let mut resp = rt.handle(deploy).await;
    assert_eq!(resp.status, Some(StatusCode::CREATED));
    let body = body_json(&mut resp).await;
    assert_eq!(body["validated"], true);
    let code_ref = body["ref"].as_str().unwrap().to_string();

    let mut config = surface_config();
    config["mounts"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "path": "/custom", "service": code_ref, "config": { "access": "open", "grants": {} } }));
    let put = req(Method::PUT, "/services/raw").with_json(&config);
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::NO_CONTENT));

    let mut hit = rt.handle(req(Method::GET, "/custom/hello?x=1")).await;
    assert_eq!(hit.status, Some(StatusCode::OK));
    assert_eq!(hit.header("x-engine"), Some("wasm"));
    let echoed = body_json(&mut hit).await;
    assert_eq!(echoed["method"], "GET");
}

/// Spec stores advertise their reserved authoring subtree (`.queries`/
/// `.pipelines`/`.templates`) on discovery, so a generic client learns the
/// authoring root without special-casing service names. Non-spec stores omit it.
#[tokio::test]
async fn spec_subtree_advertised_on_spec_stores() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(
        dir.path(),
        json!({ "mounts": [
            { "path": "/files", "service": "file", "config": { "access": "open" } },
            { "path": "/data", "service": "data", "config": { "access": "open" } },
            { "path": "/q", "service": "query", "config": { "access": "open" } },
            { "path": "/p", "service": "pipeline", "config": { "access": "open" } },
        ] }),
    );

    let mut resp = rt
        .handle(req(Method::GET, "/.well-known/rs2/services"))
        .await;
    let doc = body_json(&mut resp).await;
    let entry = |path: &str| {
        doc["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["path"] == path)
            .cloned()
            .unwrap_or_else(|| panic!("mount {path} missing: {doc}"))
    };
    assert_eq!(entry("/q")["specSubtree"], ".queries");
    assert_eq!(entry("/p")["specSubtree"], ".pipelines");
    assert!(
        entry("/files").get("specSubtree").is_none(),
        "file is not a spec store"
    );
    assert!(
        entry("/data").get("specSubtree").is_none(),
        "data is not a spec store"
    );

    // query edits specs as plain JSON — no `authoring` descriptor; pipeline
    // advertises its DSL round-trip; file/data are not spec stores.
    assert!(
        entry("/q").get("authoring").is_none(),
        "query authors plain JSON"
    );
    assert_eq!(
        entry("/p")["authoring"],
        json!({ "kind": "pipeline-dsl", "compiledField": "pipeline", "sourceField": "x-source" })
    );
    assert!(entry("/files").get("authoring").is_none());
    assert!(entry("/data").get("authoring").is_none());

    // Also folded into the OPTIONS capability probe (describe_mount uses meta).
    let mut opts = rt.handle(req(Method::OPTIONS, "/q")).await;
    let desc = body_json(&mut opts).await;
    assert_eq!(desc["specSubtree"], ".queries");
    assert!(desc.get("authoring").is_none());
    let mut popts = rt.handle(req(Method::OPTIONS, "/p")).await;
    assert_eq!(
        body_json(&mut popts).await["authoring"]["kind"],
        "pipeline-dsl"
    );
}

/// The `template` spec store (JS engine) advertises `.templates` and the JSX
/// authoring descriptor so a generic client knows how to edit/compile it.
#[cfg(feature = "js")]
#[tokio::test]
async fn spec_subtree_and_authoring_advertised_on_template() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt_with(
        dir.path(),
        json!({ "mounts": [{ "path": "/t", "service": "template", "config": { "access": "open" } }] }),
    );

    let mut resp = rt
        .handle(req(Method::GET, "/.well-known/rs2/services"))
        .await;
    let doc = body_json(&mut resp).await;
    let t = doc["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["path"] == "/t")
        .cloned()
        .unwrap_or_else(|| panic!("template mount missing: {doc}"));
    assert_eq!(t["specSubtree"], ".templates");
    assert_eq!(
        t["authoring"],
        json!({
            "kind": "jsx", "framework": "preact",
            "compiledField": "source", "sourceField": "jsxSource", "render": "html"
        })
    );

    // Also on the OPTIONS capability probe.
    let mut opts = rt.handle(req(Method::OPTIONS, "/t")).await;
    let desc = body_json(&mut opts).await;
    assert_eq!(desc["authoring"]["kind"], "jsx");
}
