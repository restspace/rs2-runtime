//! Inline per-spec pipeline access (PRD §5.2 / §10.3). A pipeline mount's
//! execution surface is authorized per spec: the matched spec's `access`
//! overrides the mount's `access` floor per key (`.root` is the mount-wide
//! floor), evaluated with the verb→action map (POST→invoke = "run it"). The
//! host defers the execution surface to the service; authoring stays
//! host-enforced. The envelope's `access` shape is validated at write time.

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

/// The host/service authorization verdict, separate from execution: `401`,
/// `403`, or `None` (admitted — the pipeline ran).
async fn denial(rt: &Runtime, msg: Message) -> Option<StatusCode> {
    match rt.handle(msg).await.status {
        Some(StatusCode::UNAUTHORIZED) => Some(StatusCode::UNAUTHORIZED),
        Some(StatusCode::FORBIDDEN) => Some(StatusCode::FORBIDDEN),
        _ => None,
    }
}

/// A self-contained spec that returns a constant (no downstream calls).
fn returns_constant() -> serde_json::Value {
    json!({ "pipeline": { "steps": [ { "transform": { "ok": true } } ] } })
}

fn rt(file_root: &std::path::Path) -> Arc<Runtime> {
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(file_root)), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({
        // `A` is the operator role — only operators may set a spec's `access`.
        "operatorRoles": "A",
        "mounts": [
            // Execution locked to admins by default; specs override per-path.
            { "path": "/p", "service": "pipeline",
              "config": { "access": { "invoke": "A", "write": "A" } } }
        ]
    })));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
}

#[tokio::test]
async fn a_spec_overrides_the_mount_floor_either_way() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());

    // Author specs as admin (authoring is host-enforced via write = "A").
    let put = |path: &str, doc: serde_json::Value| {
        as_role(req(Method::PUT, path), "A").with_json(&doc)
    };
    // `.root`: no per-spec access → inherits the mount floor (invoke "A").
    assert_eq!(
        rt.handle(put("/p/.pipelines/.root", returns_constant())).await.status,
        Some(StatusCode::CREATED)
    );
    // `/login`: loosened to public.
    let mut login = returns_constant();
    login["access"] = json!({ "invoke": "all" });
    assert_eq!(
        rt.handle(put("/p/.pipelines/login", login)).await.status,
        Some(StatusCode::CREATED)
    );
    // `/audit`: tightened to a different role.
    let mut audit = returns_constant();
    audit["access"] = json!({ "invoke": "E" });
    assert_eq!(
        rt.handle(put("/p/.pipelines/audit", audit)).await.status,
        Some(StatusCode::CREATED)
    );

    // Public spec: an anonymous POST runs it.
    assert_eq!(denial(&rt, req(Method::POST, "/p/login")).await, None);
    // Unmatched path falls to `.root` (floor invoke "A"): anonymous → 401.
    assert_eq!(
        denial(&rt, req(Method::POST, "/p/other")).await,
        Some(StatusCode::UNAUTHORIZED)
    );
    // Tightened spec: anonymous → 401, wrong role → 403, right role runs.
    assert_eq!(
        denial(&rt, req(Method::POST, "/p/audit")).await,
        Some(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(
        denial(&rt, as_role(req(Method::POST, "/p/audit"), "U")).await,
        Some(StatusCode::FORBIDDEN)
    );
    assert_eq!(denial(&rt, as_role(req(Method::POST, "/p/audit"), "E")).await, None);
}

#[tokio::test]
async fn envelope_access_shape_is_validated() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path());
    let put = |path: &str, doc: serde_json::Value| {
        as_role(req(Method::PUT, path), "A").with_json(&doc)
    };

    // `manage` is not a spec action.
    let mut bad = returns_constant();
    bad["access"] = json!({ "manage": "A" });
    assert_eq!(
        rt.handle(put("/p/.pipelines/m", bad)).await.status,
        Some(StatusCode::BAD_REQUEST)
    );
    // Unknown key (typo guard).
    let mut typo = returns_constant();
    typo["access"] = json!({ "invokeRoles": "all" });
    assert_eq!(
        rt.handle(put("/p/.pipelines/t", typo)).await.status,
        Some(StatusCode::BAD_REQUEST)
    );
    // Non-string role spec.
    let mut nonstr = returns_constant();
    nonstr["access"] = json!({ "invoke": ["all"] });
    assert_eq!(
        rt.handle(put("/p/.pipelines/n", nonstr)).await.status,
        Some(StatusCode::BAD_REQUEST)
    );
    // Valid access object.
    let mut ok = returns_constant();
    ok["access"] = json!({ "read": "all", "invoke": "U" });
    assert_eq!(
        rt.handle(put("/p/.pipelines/ok", ok)).await.status,
        Some(StatusCode::CREATED)
    );
}
