//! On-behalf-of delegation for app actions (docs/auth-and-acls.md,
//! "Delegated identities"; app-actions-as-viewer plan §6.3).
//!
//! When a viewer presses an app's action, `InvokeAppAction` signs a viewer
//! assertion carrying a [`Delegation`]: the action's declared permission
//! (plus `read` on the same resource when the viewer holds it) on exactly
//! the scope the manager checked. The app presents that assertion back on
//! its own calls in [`ON_BEHALF_OF_HEADER`], beside its own credential, and
//! the manager authenticates those calls as a **delegated identity**,
//! `<viewer> via app <app>`, built here:
//!
//! - one synthesized allow rule: the delegated resource and the actions
//!   that survive, with exact selectors for the scope;
//! - an action survives only if the app's own identity allows it on that
//!   scope (the app's grant bounds the delegation), and, when the viewer's
//!   token could be re-resolved, only if the viewer still holds it;
//! - the app's and the viewer's deny rules, so the rule never reaches what
//!   either is denied.
//!
//! Verifying the assertion and deciding that the caller is the app happen in
//! the manager service (`authenticate_acl`), which has the keys and the
//! store; this module is the pure part, so it can be tested on its own.

// tonic::Status is ~176 bytes; returning it directly matches the rest of the
// gRPC surface.
#![allow(clippy::result_large_err)]

use crate::structs::acl::{AclAction, AclIdentity, AclScope, AclSource};
use crate::structs::app_ui::{Delegation, ON_BEHALF_OF_HEADER};
use tonic::metadata::MetadataMap;
use tonic::Status;

/// Every refusal names the header, so a caller knows which credential failed.
pub fn refusal_message(reason: &str) -> String {
    format!("{ON_BEHALF_OF_HEADER}: {reason}")
}

/// The assertion a request presents in [`ON_BEHALF_OF_HEADER`]: `Ok(None)`
/// without the header, an error for one that is empty or not text.
pub fn presented_assertion(metadata: &MetadataMap) -> Result<Option<String>, String> {
    let Some(value) = metadata.get(ON_BEHALF_OF_HEADER) else {
        return Ok(None);
    };
    let token = value
        .to_str()
        .map_err(|_| "the header is not ASCII".to_string())?
        .trim();
    if token.is_empty() {
        return Err("the header is empty".to_string());
    }
    Ok(Some(token.to_string()))
}

/// The principal a delegated identity is recorded as.
pub fn delegated_principal(viewer: &str, app: &str) -> String {
    format!("{viewer} via app {app}")
}

/// Build the identity `caller` (the app's own) acts with on `viewer`'s
/// behalf, holding `delegation` on `scope` (the delegation's scope, as the
/// evaluator reads it), bounded by `caller`'s own rules and, when the
/// viewer's token was re-resolved, by `viewer_now`'s. `expires_at` is when
/// the assertion expires (RFC3339); the identity expires then or when the
/// caller's credential does, whichever is first.
pub fn delegated_identity(
    caller: &AclIdentity,
    viewer_now: Option<&AclIdentity>,
    app: &str,
    viewer_principal: &str,
    delegation: &Delegation,
    scope: &AclScope,
    expires_at: String,
) -> Result<AclIdentity, Status> {
    let (resource, actions) = delegation
        .parsed()
        .map_err(|err| Status::permission_denied(refusal_message(&err)))?;
    let declared = actions[0];
    let target = delegation.scope.label();
    let mut kept: Vec<AclAction> = Vec::new();
    for action in actions {
        if let Some(viewer) = viewer_now {
            if !viewer.would_allow(resource, action, scope) {
                if action == declared {
                    return Err(Status::permission_denied(refusal_message(&format!(
                        "{viewer_principal} no longer holds {}:{} on {target}",
                        resource.as_str(),
                        action.as_str()
                    ))));
                }
                continue;
            }
        }
        if caller.would_allow(resource, action, scope) {
            kept.push(action);
        }
    }
    if kept.is_empty() {
        return Err(Status::permission_denied(refusal_message(&format!(
            "app {app}'s own grant does not hold {}:{} on {target}, so it cannot act there as \
             {viewer_principal}",
            resource.as_str(),
            declared.as_str()
        ))));
    }
    let mut rules = vec![delegation.rule(&kept)];
    rules.extend(caller.rules.iter().filter(|rule| rule.is_deny()).cloned());
    if let Some(viewer) = viewer_now {
        rules.extend(viewer.rules.iter().filter(|rule| rule.is_deny()).cloned());
    }
    let parse = |text: &str| {
        time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()
    };
    let expires_at = match caller.expires_at.as_deref() {
        Some(caller_expiry)
            if matches!(
                (parse(caller_expiry), parse(&expires_at)),
                (Some(caller), Some(assertion)) if caller < assertion
            ) =>
        {
            caller_expiry.to_string()
        }
        _ => expires_at,
    };
    Ok(AclIdentity {
        principal: delegated_principal(viewer_principal, app),
        // Never the viewer's: records keyed by accessor (exec tickets,
        // idempotency scoping) mean "this person's own token".
        accessor_id: None,
        is_management: false,
        is_worker: false,
        policies: Vec::new(),
        rules,
        source: AclSource::Delegated,
        expires_at: Some(expires_at),
        audit_only: caller.audit_only,
    })
}

#[cfg(test)]
mod delegation_test {
    use super::*;
    use crate::structs::acl::{parse_rules, AclResource};

    /// Whether `identity` can do anything with `resource` anywhere.
    fn holds_any(identity: &AclIdentity, resource: AclResource) -> bool {
        [AclAction::Read, AclAction::Write]
            .into_iter()
            .any(|action| identity.would_allow(resource, action, &AclScope::any()))
    }

    fn identity(principal: &str, rules: &str) -> AclIdentity {
        AclIdentity {
            principal: principal.into(),
            accessor_id: Some(format!("acc-{principal}")),
            is_management: false,
            is_worker: false,
            policies: vec!["grant:alloc/7".into()],
            rules: parse_rules(rules).expect("rules parse"),
            source: AclSource::Token,
            expires_at: None,
            audit_only: false,
        }
    }

    /// The autoscaler's grant: scale read and write in every namespace.
    fn autoscaler() -> AclIdentity {
        identity(
            "autoscaler",
            r#"[{"resources": ["scale"], "actions": ["read", "write"], "namespaces": ["*"]}]"#,
        )
    }

    fn pause_on(job: &str) -> (Delegation, AclScope) {
        let scope = AclScope::job(job);
        let delegation = Delegation::new(
            AclResource::Scale,
            &[AclAction::Write, AclAction::Read],
            &scope,
        )
        .expect("delegation");
        let scope = delegation.scope.acl_scope();
        (delegation, scope)
    }

    const LATER: &str = "2030-01-01T00:00:00Z";

    /// The synthesized rule allows exactly the delegated resource, actions,
    /// and job, and nothing else the app could do.
    #[test]
    fn the_rule_covers_exactly_the_delegation_test() {
        let (delegation, scope) = pause_on("default/web");
        let delegated = delegated_identity(
            &autoscaler(),
            None,
            "autoscaler",
            "alice",
            &delegation,
            &scope,
            LATER.into(),
        )
        .expect("delegated");
        assert_eq!(delegated.principal, "alice via app autoscaler");
        assert_eq!(delegated.source, AclSource::Delegated);
        assert!(delegated.accessor_id.is_none() && delegated.policies.is_empty());
        assert!(!delegated.is_management && !delegated.is_worker);
        assert_eq!(delegated.expires_at.as_deref(), Some(LATER));
        let allows = |resource, action, scope: &AclScope| delegated.allows(resource, action, scope);
        let web = AclScope::job("web");
        assert!(allows(AclResource::Scale, AclAction::Write, &web));
        assert!(allows(AclResource::Scale, AclAction::Read, &web));
        // Template provenance on the request does not matter to a rule
        // without one.
        assert!(allows(
            AclResource::Scale,
            AclAction::Write,
            &web.clone().with_template(Some("t".into()))
        ));
        // Another job, another namespace, another resource: no.
        assert!(!allows(
            AclResource::Scale,
            AclAction::Write,
            &AclScope::job("api")
        ));
        assert!(!allows(
            AclResource::Scale,
            AclAction::Write,
            &AclScope::job("team-b/web")
        ));
        assert!(!allows(AclResource::Job, AclAction::Write, &web));
        assert!(!allows(AclResource::Job, AclAction::Read, &web));
        // A host-level write is never covered.
        assert!(!allows(
            AclResource::Job,
            AclAction::Write,
            &web.clone().with_host(true)
        ));
        assert!(!holds_any(&delegated, AclResource::Node));
    }

    /// Delegation never exceeds the app's grant: an action the app lacks on
    /// the scope is dropped, and with nothing left the call is refused.
    #[test]
    fn the_apps_grant_bounds_the_delegation_test() {
        // An app that may only read scale, and only in `default`.
        let reader = identity(
            "reader",
            r#"[{"resources": ["scale"], "actions": ["read"], "namespaces": ["default"]}]"#,
        );
        let (delegation, scope) = pause_on("default/web");
        let delegated = delegated_identity(
            &reader,
            None,
            "reader",
            "alice",
            &delegation,
            &scope,
            LATER.into(),
        )
        .expect("read survives");
        let web = AclScope::job("web");
        assert!(delegated.allows(AclResource::Scale, AclAction::Read, &web));
        assert!(
            !delegated.allows(AclResource::Scale, AclAction::Write, &web),
            "the app does not hold write, so neither does the delegation"
        );

        // A job outside the app's grant: nothing survives.
        let (elsewhere, scope) = pause_on("team-b/web");
        let refused = delegated_identity(
            &reader,
            None,
            "reader",
            "alice",
            &elsewhere,
            &scope,
            LATER.into(),
        )
        .unwrap_err();
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
        assert!(
            refused
                .message()
                .contains("own grant does not hold scale:write on team-b/web"),
            "{}",
            refused.message()
        );

        // The reserved namespace is outside a `*` grant.
        let (system, scope) = pause_on("legion-system/autoscaler");
        assert!(delegated_identity(
            &autoscaler(),
            None,
            "autoscaler",
            "alice",
            &system,
            &scope,
            LATER.into()
        )
        .is_err());
    }

    /// The app's and the viewer's deny rules carry over, and a viewer who
    /// lost the declared permission since the press is refused; one who
    /// lost only `read` keeps the write.
    #[test]
    fn deny_rules_and_the_viewers_current_rights_bound_it_test() {
        let denying = identity(
            "app",
            r#"[{"resources": ["scale"], "actions": ["read", "write"], "namespaces": ["*"]},
                {"resources": ["scale"], "actions": ["write"], "effect": "deny",
                 "templates": ["frozen"]}]"#,
        );
        let scope = AclScope::job("web");
        let delegation =
            Delegation::new(AclResource::Scale, &[AclAction::Write], &scope).expect("delegation");
        let delegated = delegated_identity(
            &denying,
            None,
            "app",
            "alice",
            &delegation,
            &delegation.scope.acl_scope(),
            LATER.into(),
        )
        .expect("delegated");
        assert!(delegated.allows(AclResource::Scale, AclAction::Write, &scope));
        assert!(
            !delegated.allows(
                AclResource::Scale,
                AclAction::Write,
                &scope.clone().with_template(Some("frozen".into()))
            ),
            "the app's deny carries over"
        );

        let (delegation, scope) = pause_on("default/web");
        let writer_only = identity(
            "alice",
            r#"[{"resources": ["scale"], "actions": ["write"], "namespaces": ["default"]}]"#,
        );
        let delegated = delegated_identity(
            &autoscaler(),
            Some(&writer_only),
            "autoscaler",
            "alice",
            &delegation,
            &scope,
            LATER.into(),
        )
        .expect("the write survives");
        assert!(!delegated.allows(AclResource::Scale, AclAction::Read, &AclScope::job("web")));
        let nothing = identity(
            "alice",
            r#"[{"resources": ["scale"], "actions": ["read"], "namespaces": ["default"]}]"#,
        );
        let refused = delegated_identity(
            &autoscaler(),
            Some(&nothing),
            "autoscaler",
            "alice",
            &delegation,
            &scope,
            LATER.into(),
        )
        .unwrap_err();
        assert!(
            refused
                .message()
                .contains("alice no longer holds scale:write"),
            "{}",
            refused.message()
        );
    }

    /// The identity expires with the assertion or the app's credential,
    /// whichever is first, and keeps the app's audit-only mode.
    #[test]
    fn expiry_and_audit_mode_follow_the_caller_test() {
        let mut caller = autoscaler();
        caller.expires_at = Some("2026-01-01T00:00:00Z".into());
        caller.audit_only = true;
        let (delegation, scope) = pause_on("web");
        let delegated = delegated_identity(
            &caller,
            None,
            "autoscaler",
            "alice",
            &delegation,
            &scope,
            LATER.into(),
        )
        .expect("delegated");
        assert_eq!(
            delegated.expires_at.as_deref(),
            Some("2026-01-01T00:00:00Z")
        );
        assert!(delegated.audit_only);
    }

    #[test]
    fn the_header_is_read_as_text_test() {
        let mut metadata = MetadataMap::new();
        assert_eq!(presented_assertion(&metadata), Ok(None));
        metadata.insert(ON_BEHALF_OF_HEADER, "  ".parse().unwrap());
        assert!(presented_assertion(&metadata).is_err());
        metadata.insert(ON_BEHALF_OF_HEADER, "a.b.c".parse().unwrap());
        assert_eq!(presented_assertion(&metadata), Ok(Some("a.b.c".into())));
    }
}
