//! Field-level authorization on the `data` service (PRD §5.2). With
//! `fieldLevelAuthz: true`, per-field `x-rs-read` / `x-rs-write` rules in the
//! dataset schema redact unreadable fields on read and reject changes to
//! unwritable fields on write; editing the schema requires an operator. This
//! also realizes role-assignment gating (protect a `roles` field).

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

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

async fn body_json(msg: &mut Message) -> Value {
    msg.body
        .as_mut()
        .expect("body")
        .as_json(1 << 20)
        .await
        .expect("json")
}

fn rt(dir: &std::path::Path, flag: bool) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir)),
        Arc::new(MemDataStore::new()),
    );
    let loader = Arc::new(StaticLoader(json!({
        "operatorRoles": "A",
        "mounts": [
            { "path": "/data", "service": "data",
              "config": { "fieldLevelAuthz": flag, "access": { "read": "all", "write": "all" } } }
        ]
    })));
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        loader,
        LimitTable::default(),
    )
}

/// `name` open; `roles` readable but admin-write-only; `ssn` admin-only both ways.
fn schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "name":  { "type": "string" },
            "roles": { "type": "string", "x-rs-write": "A" },
            "ssn":   { "type": "string", "x-rs-read": "A", "x-rs-write": "A" }
        }
    })
}

/// Seed schema (as operator A) + one record (A may write every field).
async fn seed(rt: &Runtime) {
    let s = as_role(req(Method::PUT, "/data/people/.schema.json"), "A").with_json(&schema());
    assert_eq!(
        rt.handle(s).await.status,
        Some(StatusCode::OK),
        "seed schema"
    );
    let r = as_role(req(Method::PUT, "/data/people/p1"), "A")
        .with_json(&json!({ "name": "Ada", "roles": "U", "ssn": "123" }));
    assert!(
        rt.handle(r).await.status.unwrap().is_success(),
        "seed record"
    );
}

#[tokio::test]
async fn reads_redact_unreadable_fields() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path(), true);
    seed(&rt).await;

    let mut as_u = rt
        .handle(as_role(req(Method::GET, "/data/people/p1"), "U"))
        .await;
    let u_body = body_json(&mut as_u).await;
    assert_eq!(u_body["name"], "Ada");
    assert_eq!(
        u_body["roles"], "U",
        "roles is readable (write-only restriction)"
    );
    assert!(u_body.get("ssn").is_none(), "ssn redacted for U");

    let mut as_a = rt
        .handle(as_role(req(Method::GET, "/data/people/p1"), "A"))
        .await;
    assert_eq!(body_json(&mut as_a).await["ssn"], "123", "admin sees ssn");
}

#[tokio::test]
async fn writes_to_unwritable_fields_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path(), true);
    seed(&rt).await;

    // U changing `roles` (readable, admin-write-only) → 403, by PUT and PATCH.
    let put = as_role(req(Method::PUT, "/data/people/p1"), "U")
        .with_json(&json!({ "name": "Ada", "roles": "U A" }));
    assert_eq!(
        rt.handle(put).await.status,
        Some(StatusCode::FORBIDDEN),
        "PUT role escalation"
    );
    let patch =
        as_role(req(Method::PATCH, "/data/people/p1"), "U").with_json(&json!({ "roles": "U A" }));
    assert_eq!(
        rt.handle(patch).await.status,
        Some(StatusCode::FORBIDDEN),
        "PATCH role escalation"
    );

    // A may change roles.
    let ok =
        as_role(req(Method::PATCH, "/data/people/p1"), "A").with_json(&json!({ "roles": "U E" }));
    assert!(
        rt.handle(ok).await.status.unwrap().is_success(),
        "admin sets roles"
    );
}

#[tokio::test]
async fn patch_round_trip_preserves_redacted_fields() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path(), true);
    seed(&rt).await;

    // U reads (no ssn), edits a visible field, PATCHes it back.
    let mut got = rt
        .handle(as_role(req(Method::GET, "/data/people/p1"), "U"))
        .await;
    assert!(body_json(&mut got).await.get("ssn").is_none());
    let patch =
        as_role(req(Method::PATCH, "/data/people/p1"), "U").with_json(&json!({ "name": "Ada L" }));
    let resp = rt.handle(patch).await;
    assert!(
        resp.status.unwrap().is_success(),
        "PATCH visible field: {:?}",
        resp.status
    );

    // ssn is intact; name updated.
    let mut after = rt
        .handle(as_role(req(Method::GET, "/data/people/p1"), "A"))
        .await;
    let rec = body_json(&mut after).await;
    assert_eq!(rec["name"], "Ada L");
    assert_eq!(
        rec["ssn"], "123",
        "redacted ssn preserved across the round-trip"
    );
}

#[tokio::test]
async fn put_back_a_redacted_record_preserves_hidden_fields() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path(), true);
    seed(&rt).await;

    // U GETs the redacted record and PUTs the exact body back (no ssn in it).
    let mut got = rt
        .handle(as_role(req(Method::GET, "/data/people/p1"), "U"))
        .await;
    let redacted = body_json(&mut got).await;
    assert!(redacted.get("ssn").is_none());
    let put = as_role(req(Method::PUT, "/data/people/p1"), "U").with_json(&redacted);
    assert!(
        rt.handle(put).await.status.unwrap().is_success(),
        "PUT redacted body back"
    );

    let mut after = rt
        .handle(as_role(req(Method::GET, "/data/people/p1"), "A"))
        .await;
    assert_eq!(
        body_json(&mut after).await["ssn"],
        "123",
        "ssn not dropped by full PUT"
    );
}

#[tokio::test]
async fn schema_writes_require_an_operator() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path(), true);
    seed(&rt).await;

    // A non-operator cannot edit the policy-bearing schema.
    let by_u = as_role(req(Method::PUT, "/data/people/.schema.json"), "U").with_json(&schema());
    assert_eq!(rt.handle(by_u).await.status, Some(StatusCode::FORBIDDEN));
    // The operator can.
    let by_a = as_role(req(Method::PUT, "/data/people/.schema.json"), "A").with_json(&schema());
    assert_eq!(rt.handle(by_a).await.status, Some(StatusCode::OK));
}

#[tokio::test]
async fn flag_off_is_back_compat() {
    let dir = tempfile::tempdir().unwrap();
    let rt = rt(dir.path(), false); // fieldLevelAuthz: false
                                    // Seed schema as anyone (no operator gate when the flag is off).
    let s = req(Method::PUT, "/data/people/.schema.json").with_json(&schema());
    assert_eq!(
        rt.handle(s).await.status,
        Some(StatusCode::OK),
        "schema write ungated"
    );
    let r = req(Method::PUT, "/data/people/p1")
        .with_json(&json!({ "name": "Ada", "roles": "U", "ssn": "123" }));
    assert!(rt.handle(r).await.status.unwrap().is_success());

    // No redaction, no field gate: U sees ssn and can change roles.
    let mut got = rt
        .handle(as_role(req(Method::GET, "/data/people/p1"), "U"))
        .await;
    assert_eq!(
        body_json(&mut got).await["ssn"],
        "123",
        "no redaction when flag off"
    );
    let patch =
        as_role(req(Method::PATCH, "/data/people/p1"), "U").with_json(&json!({ "roles": "U A" }));
    assert!(
        rt.handle(patch).await.status.unwrap().is_success(),
        "no field gate when flag off"
    );
}
