//! Write-time spec validation end-to-end: a PUT whose pipeline contains an
//! unparseable JSONata expression is rejected with 422 `validation_failed`,
//! even when the bad expression sits in a branch execution would never take
//! (a dead `if` arm of a conditional). Previously this only surfaced as a
//! runtime 400 when the branch was actually evaluated.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::json;

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::{Message, Principal};
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

struct StaticLoader(serde_json::Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn req(method: Method, path: &str) -> Message {
    Message::request(method, path, "t")
}

fn as_admin(mut msg: Message) -> Message {
    msg.principal = Some(Principal {
        id: "admin-1".into(),
        roles: vec!["A".into()],
        kind: "user".into(),
        extra: Default::default(),
    });
    msg
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body
        .as_mut()
        .expect("body")
        .as_json(1024 * 1024)
        .await
        .expect("json body")
}

fn runtime(dir: &std::path::Path) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/pipe", "service": "pipeline",
          "config": { "access": { "invoke": "all", "write": "A" } } }
    ]})));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

#[tokio::test]
async fn put_rejects_invalid_expression_in_dead_branch() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path());

    // Conditional: the first arm always matches; the second arm is dead at
    // runtime but carries an unparseable expression. The PUT must still fail.
    let put = as_admin(req(Method::PUT, "/pipe/.pipelines/.root")).with_json(&json!({
        "pipeline": {
            "mode": "conditional",
            "steps": [
                { "if": "status == 200", "transform": { "ok": "$" } },
                { "if": "status == 999", "transform": { "bad": "$sum((" } }
            ]
        }
    }));
    let mut resp = rt.handle(put).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::UNPROCESSABLE_ENTITY),
        "{:?}",
        resp.body
    );
    let problem = body_json(&mut resp).await;
    assert_eq!(problem["code"], "validation_failed");
    let errors = problem["errors"].to_string();
    assert!(
        errors.contains("steps[1]"),
        "error names the dead branch: {errors}"
    );
    assert!(errors.contains("invalid JSONata expression"), "{errors}");

    // Nothing was stored: the mount root has no spec to execute.
    let missing = rt
        .handle(req(Method::POST, "/pipe").with_json(&json!({})))
        .await;
    assert_eq!(
        missing.status,
        Some(StatusCode::NOT_FOUND),
        "{:?}",
        missing.body
    );

    // The same spec with the expression fixed is accepted.
    let put = as_admin(req(Method::PUT, "/pipe/.pipelines/.root")).with_json(&json!({
        "pipeline": {
            "mode": "conditional",
            "steps": [
                { "if": "status == 200", "transform": { "ok": "$" } },
                { "if": "status == 999", "transform": { "fine": "$sum(lines.price)" } }
            ]
        }
    }));
    let resp = rt.handle(put).await;
    assert!(
        matches!(
            resp.status,
            Some(StatusCode::CREATED) | Some(StatusCode::OK)
        ),
        "{:?}",
        resp.status
    );
}
