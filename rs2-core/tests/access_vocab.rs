//! The v2 access vocabulary (PRD §5.2): `read` (GET/HEAD/OPTIONS), `write`
//! (PUT/PATCH), `delete` (DELETE), `invoke` (POST and other non-idempotent
//! verbs). `delete` and `invoke` default to `write`; `read` defaults to `"all"`,
//! `write` to `"A"`. These tests pin the per-action split and the default chain.

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

fn rt(file_root: &std::path::Path, mounts: serde_json::Value) -> Arc<Runtime> {
    let adapters =
        Adapters::new(Arc::new(LocalFsFileStore::new(file_root)), Arc::new(MemDataStore::new()));
    let loader = Arc::new(StaticLoader(json!({ "mounts": mounts })));
    Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default())
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

/// The host's authorization verdict, independent of whatever the service does
/// afterward: `401` (authenticate), `403` (forbidden), or `None` (admitted).
async fn denial(rt: &Runtime, msg: Message) -> Option<StatusCode> {
    match rt.handle(msg).await.status {
        Some(StatusCode::UNAUTHORIZED) => Some(StatusCode::UNAUTHORIZED),
        Some(StatusCode::FORBIDDEN) => Some(StatusCode::FORBIDDEN),
        _ => None,
    }
}

#[tokio::test]
async fn each_action_is_gated_independently() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(
        dir.path(),
        json!([
            { "path": "/x", "service": "data",
              "config": { "access": {
                  "read": "all", "write": "A", "delete": "all", "invoke": "E" } } }
        ]),
    );

    // read = "all": anonymous GET is admitted.
    assert_eq!(denial(&rt, req(Method::GET, "/x/things/k")).await, None);
    // delete = "all": anonymous DELETE is admitted (split out of write).
    assert_eq!(denial(&rt, req(Method::DELETE, "/x/things/k")).await, None);
    // invoke = "E": anonymous POST must authenticate; an `E` principal passes,
    // a `U` principal is forbidden.
    assert_eq!(
        denial(&rt, req(Method::POST, "/x/things/")).await,
        Some(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(denial(&rt, as_role(req(Method::POST, "/x/things/"), "E")).await, None);
    assert_eq!(
        denial(&rt, as_role(req(Method::POST, "/x/things/"), "U")).await,
        Some(StatusCode::FORBIDDEN)
    );
    // write = "A": PUT needs A; a `U` principal is forbidden, `A` passes.
    assert_eq!(
        denial(&rt, as_role(req(Method::PUT, "/x/things/k"), "U")).await,
        Some(StatusCode::FORBIDDEN)
    );
    assert_eq!(denial(&rt, as_role(req(Method::PUT, "/x/things/k"), "A")).await, None);
}

#[tokio::test]
async fn delete_and_invoke_default_to_write_and_read_defaults_open() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(
        dir.path(),
        json!([
            { "path": "/y", "service": "data", "config": { "access": { "write": "A" } } }
        ]),
    );

    // read defaults to "all": anonymous GET admitted.
    assert_eq!(denial(&rt, req(Method::GET, "/y/things/k")).await, None);
    // delete defaults to write ("A"): anonymous denied, U forbidden, A passes.
    assert_eq!(
        denial(&rt, req(Method::DELETE, "/y/things/k")).await,
        Some(StatusCode::UNAUTHORIZED)
    );
    assert_eq!(
        denial(&rt, as_role(req(Method::DELETE, "/y/things/k"), "U")).await,
        Some(StatusCode::FORBIDDEN)
    );
    assert_eq!(denial(&rt, as_role(req(Method::DELETE, "/y/things/k"), "A")).await, None);
    // invoke defaults to write ("A"): anonymous POST must authenticate.
    assert_eq!(
        denial(&rt, req(Method::POST, "/y/things/")).await,
        Some(StatusCode::UNAUTHORIZED)
    );
}
