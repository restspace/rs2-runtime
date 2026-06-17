//! Pipeline `elevate` (the gateway pattern, PRD §5.2): a service can be locked
//! so a caller's roles can't reach it directly, yet a caller-accessible
//! pipeline whose mount is configured with an `elevate` role reaches it on the
//! caller's behalf. Elevation **adds** the mount's operator-configured role to
//! the call's principal (synthesizing an anonymous principal carrying just that
//! role when the caller is unauthenticated — the login gateway), keeping the
//! caller's identity intact. The authority is the mount config, never the spec;
//! without `elevate` the caller's principal is forwarded and re-checked at the
//! target (the safe default).

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

/// Attach a principal carrying a single role.
fn as_role(mut msg: Message, role: &str) -> Message {
    msg.principal = Some(Principal {
        id: format!("{role}-1"),
        roles: vec![role.to_string()],
        kind: "user".into(),
    });
    msg
}

async fn body_json(msg: &mut Message) -> serde_json::Value {
    msg.body.as_mut().expect("body").as_json(1024 * 1024).await.expect("json body")
}

#[tokio::test]
async fn elevate_lets_a_pipeline_front_a_locked_service() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        // Read needs the dedicated `svc` role; admins seed it.
        { "path": "/secret", "service": "data",
          "config": { "access": { "read": "svc", "write": "A" } } },
        // `/gate` adds `svc` on elevated steps (operator-configured); execution
        // is open, authoring is admin-only.
        { "path": "/gate", "service": "pipeline",
          "config": { "elevate": "svc", "access": { "invoke": "all", "write": "A" } } },
        // `/plain` has no elevate role; the caller's principal is forwarded.
        { "path": "/plain", "service": "pipeline",
          "config": { "access": { "invoke": "all", "write": "A" } } }
    ]})));
    let rt = Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());

    // Author the fronting pipelines (`.root` governs the mount).
    let gate = as_role(req(Method::PUT, "/gate/.pipelines/.root"), "A")
        .with_json(&json!({ "pipeline": ["elevate GET /secret/things/x"] }));
    assert_eq!(rt.handle(gate).await.status, Some(StatusCode::CREATED), "author /gate");
    let plain = as_role(req(Method::PUT, "/plain/.pipelines/.root"), "A")
        .with_json(&json!({ "pipeline": ["GET /secret/things/x"] }));
    assert_eq!(rt.handle(plain).await.status, Some(StatusCode::CREATED), "author /plain");

    // Seed the locked dataset (admins only).
    let seed = as_role(req(Method::PUT, "/secret/things/x"), "A").with_json(&json!({ "v": 1 }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED), "seed");

    // A U user is refused direct access to the locked service.
    assert_eq!(
        rt.handle(as_role(req(Method::GET, "/secret/things/x"), "U")).await.status,
        Some(StatusCode::FORBIDDEN),
        "direct access without the `svc` role is forbidden"
    );

    // …but reaches it through the elevating pipeline: the U principal gains
    // `svc` for the inner call (its `U` identity is kept).
    let mut resp = rt.handle(as_role(req(Method::GET, "/gate"), "U")).await;
    assert_eq!(resp.status, Some(StatusCode::OK), "elevating pipeline reaches it: {:?}", resp.body);
    assert_eq!(body_json(&mut resp).await["v"], 1, "and returns the record");

    // The login-gateway case: even an anonymous caller is given a synthetic
    // principal carrying `svc`, so the gate reaches the locked service.
    let mut anon = rt.handle(req(Method::GET, "/gate")).await;
    assert_eq!(anon.status, Some(StatusCode::OK), "anonymous reaches it via elevate: {:?}", anon.body);
    assert_eq!(body_json(&mut anon).await["v"], 1);

    // The non-elevating pipeline forwards the U principal → still forbidden.
    assert_eq!(
        rt.handle(as_role(req(Method::GET, "/plain"), "U")).await.status,
        Some(StatusCode::FORBIDDEN),
        "without elevate, the forwarded principal is re-checked and rejected"
    );
}

/// Closing the confused-deputy hole: an anonymously-triggered pipeline that
/// does NOT elevate cannot reach a locked mount. Its internal call carries no
/// principal and is no longer auto-admitted as "trusted internal" — internal
/// composition is authorized by the principal it carries, like any call.
#[tokio::test]
async fn anonymous_pipeline_cannot_reach_a_locked_mount() {
    let dir = tempfile::tempdir().unwrap();
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir.path())),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({ "mounts": [
        { "path": "/secret", "service": "data",
          "config": { "access": { "read": "A", "write": "A" } } },
        // A public pipeline (anyone may run it) with no elevate role.
        { "path": "/open", "service": "pipeline",
          "config": { "access": { "invoke": "all", "write": "A" } } }
    ]})));
    let rt = Runtime::new(Tenancy::Single { tenant: "t".into() }, adapters, loader, LimitTable::default());

    let seed = as_role(req(Method::PUT, "/secret/things/x"), "A").with_json(&json!({ "v": 1 }));
    assert_eq!(rt.handle(seed).await.status, Some(StatusCode::CREATED), "seed");
    let author = as_role(req(Method::PUT, "/open/.pipelines/.root"), "A")
        .with_json(&json!({ "pipeline": ["GET /secret/things/x"] }));
    assert_eq!(rt.handle(author).await.status, Some(StatusCode::CREATED), "author /open");

    // Anonymous trigger: the pipeline runs, but its no-principal internal call
    // to the locked mount is refused — no ambient trust.
    let resp = rt.handle(req(Method::GET, "/open")).await;
    assert_eq!(
        resp.status,
        Some(StatusCode::UNAUTHORIZED),
        "an anonymous pipeline must not reach the locked mount: {:?}",
        resp.body
    );
}
