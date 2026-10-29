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
//!
//! The same step honors `legion-for-object` (docs/app-objects.md, "Acting
//! for an object"): the caller must be the app that owns the named object,
//! the object must exist under that uid (a deleting one included), and the
//! identity is `<app> for <Kind> <ns>/<name>`, holding the object's recorded
//! authority within the app's grant
//! ([`crate::manager::delegation::object_scoped_identity`]). A call may
//! carry one of the two headers, never both.

use super::*;
use crate::manager::delegation::{
    delegated_identity, for_object_refusal, names_a_viewer, names_an_object,
    object_scoped_identity, presented_assertion, presented_object, refusal_message,
};
use crate::structs::acl::{ActingFor, AclSource};
use crate::structs::app_object::ObjectAuthority as AuthorityRecord;
use crate::structs::app_ui::{FOR_OBJECT_HEADER, ON_BEHALF_OF_HEADER};
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
        if identity.source.is_acting() {
            return None;
        }
        identity.policies.iter().find_map(|policy| {
            let allocation = crate::structs::grant::parse_grant_policy_ref(policy)?;
            self.store.allocation_app(allocation).ok().flatten()
        })
    }

    /// `authenticate_acl`'s second step: `caller` (authenticated as usual)
    /// with the request's `legion-on-behalf-of` assertion or
    /// `legion-for-object` reference, if it sent one. With ACLs off both
    /// headers are ignored: every caller is `acl-disabled`.
    pub(super) fn with_delegation(
        &self,
        caller: AclIdentity,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<AclIdentity, Status> {
        if !self.auth.enabled {
            return Ok(caller);
        }
        if names_a_viewer(metadata) && names_an_object(metadata) {
            return Err(refuse(
                &caller,
                None,
                ON_BEHALF,
                Status::invalid_argument(format!(
                    "a call acts for a viewer ({ON_BEHALF_OF_HEADER}) or for an object \
                     ({FOR_OBJECT_HEADER}), not both"
                )),
            ));
        }
        match presented_object(metadata) {
            Ok(Some(actor)) => return self.act_for_object(&caller, actor),
            Ok(None) => {}
            Err(reason) => {
                return Err(refuse(
                    &caller,
                    None,
                    FOR_OBJECT,
                    Status::unauthenticated(for_object_refusal(&reason)),
                ))
            }
        }
        match presented_assertion(metadata) {
            Ok(None) => Ok(caller),
            Ok(Some(token)) => self.delegate(&caller, &token),
            Err(reason) => Err(refuse(
                &caller,
                None,
                ON_BEHALF,
                Status::unauthenticated(refusal_message(&reason)),
            )),
        }
    }

    /// The identity `caller` acts with for `actor`'s object: refused unless
    /// the caller is the app that owns it and the object exists under that
    /// uid. The generation the app names is advisory: the object's current
    /// recorded authority applies (it changes only on an apply).
    fn act_for_object(&self, caller: &AclIdentity, actor: ActingFor) -> Result<AclIdentity, Status> {
        let label = actor.header_value();
        if caller.source == AclSource::Anonymous {
            return Err(refuse(
                caller,
                None,
                FOR_OBJECT,
                Status::unauthenticated(for_object_refusal(
                    "an app names one of its objects with its own credential (its `legion` \
                     token, which the API socket attaches); this request has none",
                )),
            ));
        }
        let Some(app) = self.app_of_identity(caller) else {
            return Err(refuse(
                caller,
                None,
                FOR_OBJECT,
                Status::permission_denied(for_object_refusal(&format!(
                    "only the app that owns an object acts for it, with its own identity; this \
                     credential ({}) is not an app's",
                    caller.principal
                ))),
            ));
        };
        let denied = |reason: String| {
            refuse(
                caller,
                Some(&app),
                FOR_OBJECT,
                Status::permission_denied(for_object_refusal(&reason)),
            )
        };
        // The app is checked against the reference before anything is read,
        // so another app learns nothing about which objects exist.
        if actor.app != app {
            return Err(denied(format!(
                "the {app} app acts only for its own objects; {label} is the {} app's",
                actor.app
            )));
        }
        let row = self
            .store
            .app_object_by_uid(actor.uid)
            .map_err(|e| map_store_error("Error reading the object an app acts for", e))?;
        let Some(row) = row else {
            return Err(denied(format!(
                "{label}: no object has uid {} (deleted?)",
                actor.uid
            )));
        };
        let stored = ActingFor {
            app: row.key.app.clone(),
            kind: row.key.kind.clone(),
            namespace: row.key.namespace.clone(),
            name: row.key.name.clone(),
            uid: row.uid,
            generation: actor.generation,
        };
        if stored != actor {
            return Err(denied(format!(
                "{label}: uid {} is {}, not {}",
                actor.uid,
                stored.object_ref(),
                actor.object_ref()
            )));
        }
        let registry = self.object_registry()?;
        if !registry
            .get(&row.key.app, &row.key.kind)
            .is_some_and(|kind| kind.is_active())
        {
            return Err(denied(format!(
                "{label}: kind {}/{} is orphaned (the app no longer declares it)",
                row.key.app, row.key.kind
            )));
        }
        let authority: AuthorityRecord = serde_json::from_str(&row.authority).unwrap_or_default();
        let scoped = object_scoped_identity(caller, &actor, &authority.permissions);
        if actor.generation != row.generation {
            debug!(
                app = %app,
                object = %actor.object_ref(),
                named_generation = actor.generation,
                generation = row.generation,
                "Acting for an object at another generation: its current authority applies"
            );
        }
        if !scoped.dropped.is_empty() {
            debug!(
                app = %app,
                object = %actor.object_ref(),
                dropped = %scoped.dropped.join("; "),
                "The app's grant does not cover part of the object's authority"
            );
        }
        debug!(
            app = %app,
            principal = %scoped.identity.principal,
            app_principal = %caller.principal,
            uid = row.uid,
            "Acting for an object"
        );
        Ok(scoped.identity)
    }

    fn delegate(&self, caller: &AclIdentity, token: &str) -> Result<AclIdentity, Status> {
        if caller.source == AclSource::Anonymous {
            return Err(refuse(
                caller,
                None,
                ON_BEHALF,
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
                ON_BEHALF,
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
                ON_BEHALF,
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
                    ON_BEHALF,
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
                ON_BEHALF,
                Status::unauthenticated(refusal_message("the viewer assertion has no iat or exp")),
            ));
        };
        if expires <= now {
            return Err(refuse(
                caller,
                Some(&app),
                ON_BEHALF,
                Status::unauthenticated(refusal_message(&format!(
                    "the viewer assertion expired at {expires} (manager clock {now})"
                ))),
            ));
        }
        if expires - issued > (ASSERTION_TTL_SECS as i64) * 2 {
            return Err(refuse(
                caller,
                Some(&app),
                ON_BEHALF,
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
                    ON_BEHALF,
                    Status::unauthenticated(refusal_message(&reason)),
                )
            })?;
        let denied = |reason: String| {
            refuse(
                caller,
                Some(&app),
                ON_BEHALF,
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
                        ON_BEHALF,
                        Status::unauthenticated(refusal_message(&format!(
                            "the viewer's ACL token (accessor {accessor}) was revoked"
                        ))),
                    ))
                }
                Err(status) => {
                    return Err(refuse(
                        caller,
                        Some(&app),
                        ON_BEHALF,
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
        .map_err(|status| refuse(caller, Some(&app), ON_BEHALF, status))?;
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
    /// and a delegation lives 60 seconds anyway. An identity acting for an
    /// object is refused for the same reason: the re-check would turn it
    /// into the app's own.
    pub(super) fn stream_credential<T>(
        &self,
        request: &Request<T>,
        identity: &AclIdentity,
    ) -> Result<acl::StreamCredential, Status> {
        if identity.source == AclSource::Delegated {
            return Err(refuse(
                identity,
                None,
                ON_BEHALF,
                Status::permission_denied(refusal_message(
                    "a delegation covers single calls for the assertion's 60 seconds; streams \
                     take the caller's own credential",
                )),
            ));
        }
        if identity.source == AclSource::ForObject {
            return Err(refuse(
                identity,
                None,
                FOR_OBJECT,
                Status::permission_denied(for_object_refusal(
                    "acting for an object covers single calls; streams take the app's own \
                     credential (send the call without the header)",
                )),
            ));
        }
        Ok(
            acl::StreamCredential::new(self.auth.clone(), request.metadata())
                .with_interval(self.stream_reauth_interval),
        )
    }
}

/// The audit `action` of a refused `legion-on-behalf-of`.
const ON_BEHALF: &str = "on-behalf-of";
/// The audit `action` of a refused `legion-for-object`.
const FOR_OBJECT: &str = "for-object";

/// Log a refused delegation the way every ACL denial is logged, and hand the
/// status back.
fn refuse(caller: &AclIdentity, app: Option<&str>, action: &str, status: Status) -> Status {
    warn!(
        target: "acl_audit",
        principal = %caller.principal,
        accessor_id = caller.accessor_id.as_deref().unwrap_or("-"),
        source = caller.source.as_str(),
        resource = "delegation",
        action = action,
        app = app.unwrap_or("-"),
        reason = %status.message(),
        enforced = true,
        "ACL denial"
    );
    status
}
