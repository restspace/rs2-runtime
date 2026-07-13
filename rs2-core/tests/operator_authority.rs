//! The operator tier (PRD §5.2): only a principal holding an `operatorRoles`
//! role may set or change a spec's `access` field (authorization is
//! operator-controlled config, not author content). A non-operator may still
//! author and edit the spec's logic, as long as it leaves `access` untouched.

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

fn as_role(mut msg: Message, role: &str) -> Message {
    msg.principal = Some(Principal {
        id: format!("{role}-1"),
        roles: vec![role.to_string()],
        kind: "user".into(),
        extra: Default::default(),
    });
    msg
}

fn logic() -> serde_json::Value {
    json!({ "pipeline": { "steps": [ { "transform": { "ok": true } } ] } })
}

fn rt(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(file_root)), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({
        // `op` is the operator role; `dev` may author but not set authority.
        "operatorRoles": "op",
        "mounts": [
            { "path": "/p", "service": "pipeline",
              "config": { "access": { "invoke": "all", "write": "dev op" } } }
        ]
    })));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
}

#[tokio::test]
async fn only_operators_may_set_or_change_spec_access() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    // A non-operator author (`dev`) may write a spec with no `access`.
    let plain = as_role(req(Method::PUT, "/p/.pipelines/job"), "dev").with_json(&logic());
    assert_eq!(rt.handle(plain).await.status, Some(StatusCode::CREATED));

    // …but may not introduce an `access` field.
    let mut with_access = logic();
    with_access["access"] = json!({ "invoke": "all" });
    let attempt =
        as_role(req(Method::PUT, "/p/.pipelines/job"), "dev").with_json(&with_access);
    assert_eq!(rt.handle(attempt).await.status, Some(StatusCode::FORBIDDEN));

    // An operator may set it.
    let by_op =
        as_role(req(Method::PUT, "/p/.pipelines/job"), "op").with_json(&with_access);
    assert_eq!(rt.handle(by_op).await.status, Some(StatusCode::OK));

    // The non-operator may keep editing the logic, resending the SAME access.
    let mut edited = json!({
        "pipeline": { "steps": [ { "transform": { "ok": false } } ] },
        "access": { "invoke": "all" }
    });
    let edit = as_role(req(Method::PUT, "/p/.pipelines/job"), "dev").with_json(&edited);
    assert_eq!(rt.handle(edit).await.status, Some(StatusCode::OK), "logic edit preserving access");

    // …but may not change the access while editing.
    edited["access"] = json!({ "invoke": "op" });
    let tighten = as_role(req(Method::PUT, "/p/.pipelines/job"), "dev").with_json(&edited);
    assert_eq!(rt.handle(tighten).await.status, Some(StatusCode::FORBIDDEN));

    // …nor remove it.
    let removed = as_role(req(Method::PUT, "/p/.pipelines/job"), "dev").with_json(&logic());
    assert_eq!(rt.handle(removed).await.status, Some(StatusCode::FORBIDDEN));
}

#[tokio::test]
async fn no_operator_roles_means_no_api_operator() {
    // With operatorRoles absent, nobody is an operator over the API: setting a
    // spec's access is refused for everyone (authority is file-only).
    let dir = tempfile::tempdir().unwrap();
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(dir.path())), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({
        "mounts": [
            { "path": "/p", "service": "pipeline",
              "config": { "access": { "write": "all" } } }
        ]
    })));
    let rt =
        Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());

    let mut with_access = logic();
    with_access["access"] = json!({ "invoke": "all" });
    let attempt = req(Method::PUT, "/p/.pipelines/job").with_json(&with_access);
    assert_eq!(rt.handle(attempt).await.status, Some(StatusCode::FORBIDDEN));

    // A spec without access is still freely authorable (write = "all").
    let plain = req(Method::PUT, "/p/.pipelines/job").with_json(&logic());
    assert_eq!(rt.handle(plain).await.status, Some(StatusCode::CREATED));
}
