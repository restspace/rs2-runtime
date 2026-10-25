//! Authentication as a chain of services. The host knows no credential
//! format: a service that can authenticate requests says so by returning
//! itself from [`crate::services::Service::authenticator`], and the host offers every
//! external request to each such mount in config order until one accepts it.
//!
//! An authenticator recognises only its own credential format, so the order
//! of the chain never changes an outcome:
//! - [`AuthOutcome::Pass`]: nothing here for me; try the next one.
//! - [`AuthOutcome::Unrecognised`]: something credential-shaped that I could
//!   not use. It may belong to another authenticator; if none accepts it,
//!   this error is the answer (a garbage bearer token is a 401, never
//!   anonymous).
//! - [`AuthOutcome::Authenticated`]: I verified it and set `msg.principal`.
//! - `Err`: my format, but invalid (bad signature, expired, revoked): a 401
//!   now, without asking anyone else.

use async_trait::async_trait;

use crate::error::RsError;
use crate::message::Message;
use crate::services::ServiceContext;
use crate::tenant::Tenant;

/// What one authenticator made of a request.
pub enum AuthOutcome {
    Pass,
    Unrecognised(RsError),
    /// `expires` (epoch seconds) bounds anything that outlives the request,
    /// such as an accepted WebSocket. `None` means the credential has no
    /// expiry of its own.
    Authenticated {
        expires: Option<u64>,
    },
}

#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Whether the request carries a credential of this authenticator's that
    /// the browser attaches by itself (a cookie). The host's CSRF guard runs
    /// on this before authentication, so a cross-site forgery is refused
    /// whether or not its credential is valid.
    fn ambient_credential(&self, _msg: &Message) -> bool {
        false
    }

    /// Recognise, verify, and on success set `msg.principal`.
    async fn try_authenticate(
        &self,
        msg: &mut Message,
        ctx: &ServiceContext,
    ) -> Result<AuthOutcome, RsError>;
}

/// Whether any authenticator in the tenant sees an ambient credential.
pub fn has_ambient_credential(tenant: &Tenant, msg: &Message) -> bool {
    tenant
        .authenticators()
        .any(|(auth, _)| auth.ambient_credential(msg))
}

/// Run the tenant's authenticator chain. Returns the accepted credential's
/// expiry; `Ok(None)` when no one accepted (the request stays anonymous) or
/// the credential does not expire.
pub async fn authenticate(tenant: &Tenant, msg: &mut Message) -> Result<Option<u64>, RsError> {
    let mut unrecognised: Option<RsError> = None;
    for (auth, ctx) in tenant.authenticators() {
        match auth.try_authenticate(msg, ctx).await? {
            AuthOutcome::Pass => {}
            AuthOutcome::Unrecognised(err) => {
                unrecognised.get_or_insert(err);
            }
            AuthOutcome::Authenticated { expires } => return Ok(expires),
        }
    }
    match unrecognised {
        Some(err) => Err(err),
        None => Ok(None),
    }
}
