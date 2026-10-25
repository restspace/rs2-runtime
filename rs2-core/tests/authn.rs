//! Authentication is the tenant's chain of authenticating mounts (`authn`):
//! the host only offers each request to them. These pin the host's side of
//! that contract; the JWT format itself is covered by the auth tests.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use rs2_core::adapters::{LocalFsFileStore, MemDataStore};
use rs2_core::message::Message;
use rs2_core::router::Tenancy;
use rs2_core::runtime::ConfigLoader;
use rs2_core::services::auth::{sign, Claims};
use rs2_core::tenant::{Adapters, TenantConfig};
use rs2_core::wrapper::LimitTable;
use rs2_core::{RsError, Runtime};

const SECRET: &str = "authn-secret";

struct StaticLoader(Value);

#[async_trait]
impl ConfigLoader for StaticLoader {
    async fn load_tenant(&self, _tenant: &str) -> Result<TenantConfig, RsError> {
        serde_json::from_value(self.0.clone()).map_err(|e| RsError::internal(e.to_string()))
    }
}

fn runtime(dir: &std::path::Path, config: Value) -> Arc<Runtime> {
    let adapters = Adapters::new(
        Arc::new(LocalFsFileStore::new(dir)),
        Arc::new(MemDataStore::new()),
    );
    Runtime::new(
        Tenancy::Single { tenant: "t".into() },
        adapters,
        Arc::new(StaticLoader(config)),
        LimitTable::default(),
    )
}

fn token() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = Claims {
        sub: "ann@example.com".into(),
        roles: "U".into(),
        kind: "user".into(),
        iat: now,
        exp: now + 600,
        extra: Default::default(),
    };
    sign(&claims, SECRET).unwrap()
}

fn whoami(bearer: Option<&str>) -> Message {
    let mut msg = Message::request(Method::GET, "/auth/user", "t");
    if let Some(b) = bearer {
        msg.set_header("authorization", &format!("Bearer {b}"));
    }
    msg
}

fn with_auth_mount() -> Value {
    json!({
        "auth": { "jwtSecret": SECRET },
        "mounts": [
            { "path": "/data", "service": "data",
              "config": { "access": { "read": "all", "write": "U" } } },
            { "path": "/auth/", "service": "auth", "config": { "access": "open" } }
        ]
    })
}

#[tokio::test]
async fn an_auth_mount_authenticates_wherever_it_is_mounted() {
    let dir = tempfile::tempdir().unwrap();
    // Mounted second, and with a trailing slash the table normalises away.
    let rt = runtime(dir.path(), with_auth_mount());
    let mut resp = rt.handle(whoami(Some(&token()))).await;
    assert_eq!(resp.status, Some(StatusCode::OK));
    let body = resp.body.as_mut().unwrap().as_json(1 << 20).await.unwrap();
    assert_eq!(body["id"], "ann@example.com");

    // The principal reaches every mount, not just the authenticator's.
    let mut put = Message::request(Method::PUT, "/data/notes/1", "t").with_json(&json!({}));
    put.set_header("authorization", &format!("Bearer {}", token()));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::CREATED));
}

#[tokio::test]
async fn a_credential_no_one_accepts_is_401_never_anonymous() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path(), with_auth_mount());
    // Not JWT-shaped: unrecognised, and no other authenticator takes it.
    let resp = rt.handle(whoami(Some("not-a-jwt"))).await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));
    // JWT-shaped but forged: the auth service refuses it outright.
    let forged = format!("{}x", token());
    let resp = rt.handle(whoami(Some(&forged))).await;
    assert_eq!(resp.status, Some(StatusCode::UNAUTHORIZED));
    // No credential at all is simply anonymous (the endpoint itself 401s).
    let open = Message::request(Method::GET, "/data/notes", "t");
    assert_ne!(rt.handle(open).await.status, Some(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn without_an_authenticating_mount_tokens_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    // Tenant auth settings alone no longer verify anything.
    let rt = runtime(
        dir.path(),
        json!({
            "auth": { "jwtSecret": SECRET },
            "mounts": [ { "path": "/data", "service": "data",
                          "config": { "access": { "read": "all", "write": "U" } } } ]
        }),
    );
    let mut put = Message::request(Method::PUT, "/data/notes/1", "t").with_json(&json!({}));
    put.set_header("authorization", &format!("Bearer {}", token()));
    assert_eq!(rt.handle(put).await.status, Some(StatusCode::UNAUTHORIZED));
    // A garbage token is not an error either: nothing claims it.
    let mut read = Message::request(Method::GET, "/data/notes", "t");
    read.set_header("authorization", "Bearer garbage");
    assert_ne!(rt.handle(read).await.status, Some(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn csrf_guard_asks_the_authenticators_about_cookies() {
    let dir = tempfile::tempdir().unwrap();
    let cross_site_write = || {
        let mut msg = Message::request(Method::PUT, "/data/notes/1", "t").with_json(&json!({}));
        msg.set_header("origin", "https://evil.example");
        msg.set_header("host", "api.acme.test");
        msg.set_header("cookie", "rs-auth=whatever");
        msg
    };
    // The auth service owns the `rs-auth` cookie: a forged cross-site write is
    // a 403 before its (bad) token is even looked at.
    let rt = runtime(dir.path(), with_auth_mount());
    assert_eq!(
        rt.handle(cross_site_write()).await.status,
        Some(StatusCode::FORBIDDEN)
    );
    // With no auth mount the cookie is nobody's credential: no CSRF refusal,
    // just an anonymous write the mount's roles deny.
    let rt = runtime(
        dir.path(),
        json!({ "mounts": [ { "path": "/data", "service": "data",
                              "config": { "access": { "read": "all", "write": "U" } } } ] }),
    );
    assert_eq!(
        rt.handle(cross_site_write()).await.status,
        Some(StatusCode::UNAUTHORIZED)
    );
}

// ---- apikeys: a second authenticator ------------------------------------

fn token_for(sub: &str, roles: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = Claims {
        sub: sub.into(),
        roles: roles.into(),
        kind: "user".into(),
        iat: now,
        exp: now + 600,
        extra: [("accountId".to_string(), json!("acme"))]
            .into_iter()
            .collect(),
    };
    sign(&claims, SECRET).unwrap()
}

/// `/keys` before or after `/auth`: the outcome must not depend on order.
fn keys_config(keys_first: bool) -> Value {
    let auth = json!({ "path": "/auth", "service": "auth", "config": { "access": "open" } });
    let keys = json!({ "path": "/keys", "service": "apikeys",
        "config": { "access": { "read": "authenticated", "invoke": "authenticated",
                                "delete": "authenticated" },
                    "adminRole": "A", "maxDays": 30 } });
    let data = json!({ "path": "/data", "service": "data",
        "config": { "access": { "read": "U", "write": "U" } } });
    let mounts = if keys_first {
        vec![keys, auth, data]
    } else {
        vec![auth, keys, data]
    };
    json!({ "auth": { "jwtSecret": SECRET }, "mounts": mounts })
}

async fn send(
    rt: &Runtime,
    method: Method,
    path: &str,
    bearer: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut msg = Message::request(method, path, "t");
    if let Some(b) = body {
        msg = msg.with_json(&b);
    }
    msg.set_header("authorization", &format!("Bearer {bearer}"));
    let mut resp = rt.handle(msg).await;
    let status = resp.status.unwrap();
    let body = match resp.body.as_mut() {
        Some(b) => b.as_json(1 << 20).await.unwrap_or(Value::Null),
        None => Value::Null,
    };
    (status, body)
}

#[tokio::test]
async fn an_api_key_authenticates_as_its_owner_with_its_roles() {
    for keys_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let rt = runtime(dir.path(), keys_config(keys_first));
        let ann = token_for("ann@example.com", "U W");

        let made = json!({ "name": "ci", "roles": "U" });
        let (status, made) = send(&rt, Method::POST, "/keys", &ann, Some(made)).await;
        assert_eq!(status, StatusCode::CREATED, "{made}");
        let key = made["key"].as_str().unwrap().to_string();
        assert!(key.starts_with("rs2k_"));
        assert_eq!(made["roles"], "U");
        assert!(made.get("seal").is_none());

        let (status, me) = send(&rt, Method::GET, "/auth/user", &key, None).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        assert_eq!(me["id"], "ann@example.com");
        assert_eq!(me["kind"], "key");
        assert_eq!(me["roles"], json!(["U"]));
        // The owner's extra claims ride along, plus which key it was.
        assert_eq!(me["accountId"], "acme");
        assert_eq!(me["keyId"], made["id"]);

        // It works on every mount, with only its own roles.
        let (status, _) = send(&rt, Method::PUT, "/data/notes/1", &key, Some(json!({}))).await;
        assert_eq!(status, StatusCode::CREATED);

        // A key cannot mint keys, even with a subset of its roles.
        let again = json!({ "name": "again", "roles": "U" });
        let (status, _) = send(&rt, Method::POST, "/keys", &key, Some(again)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // Revoked: the same key is now refused, not anonymous.
        let id = made["id"].as_str().unwrap();
        let (status, _) = send(&rt, Method::DELETE, &format!("/keys/{id}"), &ann, None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, err) = send(&rt, Method::GET, "/auth/user", &key, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(
            err["detail"]
                .as_str()
                .unwrap()
                .contains("unknown or revoked"),
            "{err}"
        );
    }
}

#[tokio::test]
async fn key_roles_are_a_subset_of_the_callers() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path(), keys_config(false));
    let ann = token_for("ann@example.com", "U");
    let wider = json!({ "name": "x", "roles": "U A" });
    let (status, _) = send(&rt, Method::POST, "/keys", &ann, Some(wider)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let long = json!({ "name": "x", "expiresInDays": 31 });
    let (status, _) = send(&rt, Method::POST, "/keys", &ann, Some(long)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "past maxDays");
    let (status, _) = send(&rt, Method::POST, "/keys", &ann, Some(json!({}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a name is required");
}

#[tokio::test]
async fn a_record_edited_outside_the_service_grants_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path(), keys_config(false));
    let ann = token_for("ann@example.com", "U");
    let (_, made) = send(
        &rt,
        Method::POST,
        "/keys",
        &ann,
        Some(json!({ "name": "ci" })),
    )
    .await;
    let key = made["key"].as_str().unwrap().to_string();
    let id = made["id"].as_str().unwrap();

    // The key records sit in the shared data store, so a user with write on a
    // `data` mount can rewrite one. Granting itself `A` breaks the seal.
    let path = format!("/data/apikeys/{id}");
    let (status, mut record) = send(&rt, Method::GET, &path, &ann, None).await;
    assert_eq!(status, StatusCode::OK);
    record["roles"] = json!("U A");
    let (status, _) = send(&rt, Method::PUT, &path, &ann, Some(record)).await;
    assert!(status.is_success());
    let (status, _) = send(&rt, Method::GET, "/auth/user", &key, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Nor can it plant a record of its own making for a secret it chose.
    let forged = json!({ "name": "evil", "owner": "root@example.com", "roles": "A",
        "extra": {}, "created": 1, "expires": 4_000_000_000u64, "seal": "AAAA" });
    let (status, _) = send(&rt, Method::PUT, "/data/apikeys/00ff", &ann, Some(forged)).await;
    assert!(status.is_success());
    let (status, _) = send(&rt, Method::GET, "/auth/user", "rs2k_00ff_chosen", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn listing_shows_your_own_keys_and_never_a_secret() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path(), keys_config(false));
    let ann = token_for("ann@example.com", "U");
    let bob = token_for("bob@example.com", "U");
    let admin = token_for("root@example.com", "A U");
    let (_, a) = send(
        &rt,
        Method::POST,
        "/keys",
        &ann,
        Some(json!({ "name": "a" })),
    )
    .await;
    send(
        &rt,
        Method::POST,
        "/keys",
        &bob,
        Some(json!({ "name": "b" })),
    )
    .await;

    let (status, mine) = send(&rt, Method::GET, "/keys", &ann, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(mine.as_array().unwrap().len(), 1);
    assert_eq!(mine[0]["name"], "a");
    assert!(mine[0].get("key").is_none() && mine[0].get("seal").is_none());
    let (_, all) = send(&rt, Method::GET, "/keys", &admin, None).await;
    assert_eq!(all.as_array().unwrap().len(), 2);

    // Bob cannot revoke Ann's key, and cannot tell it exists.
    let id = a["id"].as_str().unwrap();
    let (status, _) = send(&rt, Method::DELETE, &format!("/keys/{id}"), &bob, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // The admin role can.
    let (status, _) = send(&rt, Method::DELETE, &format!("/keys/{id}"), &admin, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn bad_credentials_fail_the_same_way_in_either_order() {
    for keys_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let rt = runtime(dir.path(), keys_config(keys_first));
        for bad in ["rs2k_nothex_x", "rs2k_00aa_unknown", "garbage", "a.b.c"] {
            let (status, _) = send(&rt, Method::GET, "/auth/user", bad, None).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{bad} (keys first: {keys_first})"
            );
        }
        // A JWT still works alongside keys.
        let (status, _) = send(&rt, Method::GET, "/auth/user", &token(), None).await;
        assert_eq!(status, StatusCode::OK);
    }
}

#[tokio::test]
async fn without_the_apikeys_mount_a_key_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(dir.path(), with_auth_mount());
    let (status, _) = send(&rt, Method::GET, "/auth/user", "rs2k_00aa_secret", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn apikeys_needs_the_tenant_signing_secret() {
    let dir = tempfile::tempdir().unwrap();
    let rt = runtime(
        dir.path(),
        json!({ "mounts": [ { "path": "/keys", "service": "apikeys",
                              "config": { "access": "open" } } ] }),
    );
    let resp = rt.handle(Message::request(Method::GET, "/keys", "t")).await;
    assert_eq!(resp.status, Some(StatusCode::BAD_REQUEST));
}
