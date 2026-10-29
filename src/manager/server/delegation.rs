//! Honoring an app's `legion-on-behalf-of` header (docs/auth-and-acls.md,
//! "Delegated identities"; app-actions-as-viewer plan §6.3): when an app
//! presents an action's viewer assertion beside its own credential,
//! [`ManagerDaemonService::authenticate_acl`] answers with a delegated
//! identity, `<viewer> via app <app>`, built by
//! [`crate::manager::delegation::delegated_identity`] once this module has
//! checked that:
//!
//! 1. the caller is the app the assertion was signed for (its credential
//!    carries a job-grant reference to an allocation of
//!    `legion-system/<app>`), so nobody else can replay it;
//! 2. the assertion verifies against the cluster's keys for `aud =
//!    app:<app>`, has not expired (no leeway), and is an action assertion
//!    carrying a delegation;
//! 3. the viewer's ACL token, when the assertion names one, still exists,
//!    has not expired, and still holds the delegated permission.
//!
//! A failure is refused and audited (`acl_audit`), never answered with the
//! app's own identity.

use super::*;
use crate::manager::delegation::{delegated_identity, presented_assertion, refusal_message};
use crate::structs::acl::AclSource;
use crate::structs::app_ui::{
    assertion_audience, AssertionKind, ViewerAssertion, ASSERTION_CLAIM, ASSERTION_TTL_SECS,
};

impl ManagerDaemonService {
    /// The app a credential speaks for: the first job-grant reference among
    /// its policies (`grant:alloc/<id>`) whose allocation runs an app's job,
    /// `legion-system/<app>`, at a version the reconciler stamped
    /// `managedBy` that app ([`crate::storage::store::Store::allocation_app`]):
    /// a version someone submitted straight to the job is not the app's. A
    /// bearer `legion` token and an ACL token minted from one (`legion login
    /// --method legion`) both carry the reference. `None` for everyone
    /// else, management tokens and delegated identities included.
    pub(super) fn app_of_identity(&self, identity: &AclIdentity) -> Option<String> {
        if identity.source == AclSource::Delegated {
            return None;
        }
        identity.policies.iter().find_map(|policy| {
            let allocation = crate::structs::grant::parse_grant_policy_ref(policy)?;
            self.store.allocation_app(allocation).ok().flatten()
        })
    }

    /// `authenticate_acl`'s second step: `caller` (authenticated as usual)
    /// with the request's `legion-on-behalf-of` assertion, if it sent one.
    /// With ACLs off the header is ignored: every caller is `acl-disabled`.
    pub(super) fn with_delegation(
        &self,
        caller: AclIdentity,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<AclIdentity, Status> {
        if !self.auth.enabled {
            return Ok(caller);
        }
        match presented_assertion(metadata) {
            Ok(None) => Ok(caller),
            Ok(Some(token)) => self.delegate(&caller, &token),
            Err(reason) => Err(refuse(
                &caller,
                None,
                Status::unauthenticated(refusal_message(&reason)),
            )),
        }
    }

    fn delegate(&self, caller: &AclIdentity, token: &str) -> Result<AclIdentity, Status> {
        if caller.source == AclSource::Anonymous {
            return Err(refuse(
                caller,
                None,
                Status::unauthenticated(refusal_message(
                    "an app presents a viewer assertion with its own credential (its `legion` \
                     token, which the API socket attaches); this request has none",
                )),
            ));
        }
        let Some(app) = self.app_of_identity(caller) else {
            return Err(refuse(
                caller,
                None,
                Status::permission_denied(refusal_message(&format!(
                    "only an app's own identity presents a viewer assertion; this credential \
                     ({}) is not an app's",
                    caller.principal
                ))),
            ));
        };
        let Some(state) = self.workload_identity.as_ref() else {
            return Err(refuse(
                caller,
                Some(&app),
                Status::unauthenticated(refusal_message(
                    "this manager has workload identity off (`workloadIdentity.enabled`), so it \
                     cannot verify viewer assertions",
                )),
            ));
        };
        let claims = state
            .verify(token, &assertion_audience(&app))
            .map_err(|err| {
                refuse(
                    caller,
                    Some(&app),
                    Status::unauthenticated(refusal_message(&format!(
                        "the viewer assertion does not verify for app {app}: {err:#}"
                    ))),
                )
            })?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs() as i64)
            .unwrap_or_default();
        let number = |name: &str| claims.get(name).and_then(Value::as_i64);
        let (Some(issued), Some(expires)) = (number("iat"), number("exp")) else {
            return Err(refuse(
                caller,
                Some(&app),
                Status::unauthenticated(refusal_message("the viewer assertion has no iat or exp")),
            ));
        };
        if expires <= now {
            return Err(refuse(
                caller,
                Some(&app),
                Status::unauthenticated(refusal_message(&format!(
                    "the viewer assertion expired at {expires} (manager clock {now})"
                ))),
            ));
        }
        if expires - issued > (ASSERTION_TTL_SECS as i64) * 2 {
            return Err(refuse(
                caller,
                Some(&app),
                Status::unauthenticated(refusal_message(
                    "the token lives longer than a viewer assertion does",
                )),
            ));
        }
        let assertion: ViewerAssertion = claims
            .get(ASSERTION_CLAIM)
            .cloned()
            .ok_or_else(|| format!("the token carries no {ASSERTION_CLAIM} claim"))
            .and_then(|claim| {
                serde_json::from_value(claim)
                    .map_err(|err| format!("the {ASSERTION_CLAIM} claim is malformed: {err}"))
            })
            .map_err(|reason| {
                refuse(
                    caller,
                    Some(&app),
                    Status::unauthenticated(refusal_message(&reason)),
                )
            })?;
        let denied = |reason: String| {
            refuse(
                caller,
                Some(&app),
                Status::permission_denied(refusal_message(&reason)),
            )
        };
        if assertion.app != app {
            return Err(denied(format!(
                "the assertion is for app {}, not {app}",
                assertion.app
            )));
        }
        if assertion.kind != AssertionKind::Action {
            return Err(denied(format!(
                "a {} assertion delegates nothing; only an action's does",
                assertion.kind.as_str()
            )));
        }
        let Some(delegation) = assertion.delegation.as_ref() else {
            return Err(denied(
                "the action's assertion carries no delegation (signed by a manager that \
                 predates delegation?)"
                    .to_string(),
            ));
        };
        if let Err(reason) = delegation.parsed() {
            return Err(denied(reason));
        }
        let scope = delegation.scope.acl_scope();

        // The viewer as they are now: a revoked or expired token ends the
        // delegation at once. Without an accessor (a JWT viewer) the
        // assertion's 60 seconds bound it.
        let viewer_now = match assertion.accessor.as_deref() {
            None => None,
            Some(accessor) => match self.auth.identity_for_accessor(accessor, &self.store) {
                Ok(Some(viewer)) => Some(viewer),
                Ok(None) => {
                    return Err(refuse(
                        caller,
                        Some(&app),
                        Status::unauthenticated(refusal_message(&format!(
                            "the viewer's ACL token (accessor {accessor}) was revoked"
                        ))),
                    ))
                }
                Err(status) => {
                    return Err(refuse(
                        caller,
                        Some(&app),
                        Status::new(
                            status.code(),
                            refusal_message(&format!(
                                "the viewer's ACL token (accessor {accessor}): {}",
                                status.message()
                            )),
                        ),
                    ))
                }
            },
        };
        let expires_at = OffsetDateTime::from_unix_timestamp(expires)
            .ok()
            .and_then(|at| {
                at.format(&time::format_description::well_known::Rfc3339)
                    .ok()
            })
            .unwrap_or_default();
        let identity = delegated_identity(
            caller,
            viewer_now.as_ref(),
            &app,
            &assertion.principal,
            delegation,
            &scope,
            expires_at,
        )
        .map_err(|status| refuse(caller, Some(&app), status))?;
        debug!(
            app = %app,
            principal = %identity.principal,
            app_principal = %caller.principal,
            viewer_accessor = assertion.accessor.as_deref().unwrap_or("-"),
            delegation = %delegation.label(),
            "Acting on a viewer's behalf"
        );
        Ok(identity)
    }

    /// The credential a long-lived stream re-checks, for `identity` (what
    /// `authenticate_acl` answered). A stream refuses a delegated identity:
    /// it re-authenticates every 30 seconds from the bearer credential
    /// alone, which would turn the delegation into the app's own identity,
    /// and a delegation lives 60 seconds anyway.
    pub(super) fn stream_credential<T>(
        &self,
        request: &Request<T>,
        identity: &AclIdentity,
    ) -> Result<acl::StreamCredential, Status> {
        if identity.source == AclSource::Delegated {
            return Err(refuse(
                identity,
                None,
                Status::permission_denied(refusal_message(
                    "a delegation covers single calls for the assertion's 60 seconds; streams \
                     take the caller's own credential",
                )),
            ));
        }
        Ok(
            acl::StreamCredential::new(self.auth.clone(), request.metadata())
                .with_interval(self.stream_reauth_interval),
        )
    }
}

/// Log a refused delegation the way every ACL denial is logged, and hand the
/// status back.
fn refuse(caller: &AclIdentity, app: Option<&str>, status: Status) -> Status {
    warn!(
        target: "acl_audit",
        principal = %caller.principal,
        accessor_id = caller.accessor_id.as_deref().unwrap_or("-"),
        source = caller.source.as_str(),
        resource = "delegation",
        action = "on-behalf-of",
        app = app.unwrap_or("-"),
        reason = %status.message(),
        enforced = true,
        "ACL denial"
    );
    status
}
