//! Grants: what a job's `legion` identity asks to be allowed, and whether
//! the token submitting the job may give it that (apps plan §6.4, calls
//! entry 4; docs/workload-identity.md, "Permissions").
//!
//! A job declares, on the reserved `legion` identity of a task, inline ACL
//! rules (`permissions`) and names of existing policies (`policies`). The
//! manager grants them at submit only when the submitting token already
//! holds every one: "you can grant what you hold", Kubernetes' RBAC
//! escalation-prevention rule.
//!
//! This module is pure logic, like [`crate::structs::acl`]: what a job asks
//! for ([`requested_grants`]), and whether a set of held rules covers a
//! requested rule ([`first_uncovered`]). Storage of the grants and their
//! resolution at token verification live in `crate::storage::acl_store`.
//!
//! ## Coverage
//!
//! Deciding exactly whether one rule set contains another is set algebra
//! over globs. Legion decides a sufficient condition instead, which is
//! sound and sometimes conservative (a legitimate grant may need an admin to
//! submit it):
//!
//! - A requested `(resource, action)` is covered by a held **allow** rule
//!   that covers the same pair, and whose every selector covers the
//!   requested one. A rule with `*` resources or actions asks for each one.
//! - A requested glob is covered when the requested pattern, as a string,
//!   matches a held glob. For Legion's `*`-only globs that is sound: if
//!   `payments-*` as a string matches `pay*`, every name `payments-*`
//!   matches, `pay*` matches too.
//! - **Reserved names first.** A requested pattern that can reach a
//!   reserved name (it starts with `legion-` for namespaces, or `legion/`
//!   for KV keys) is covered only by a held pattern with the same prefix, so
//!   a held `*` (which skips reserved names, calls entry 6) never covers a
//!   requested `legion-*`.
//! - A selector the request leaves out reaches everything a held rule's
//!   selector may not, so it is covered only by a held rule that leaves it
//!   out too.
//! - **Deny rules.** When the submitter holds any deny rule on the resource,
//!   subtracting it is not attempted: the request also needs `acl:write`,
//!   which can write any policy anyway.
//! - Management tokens hold everything. A requested deny rule only takes
//!   access away and is never checked.
//! - **`via = "objects"`** (an app's rule usable only when it acts for one
//!   of its objects) is checked like any other: the submitter must hold it
//!   plainly. A held `via` rule covers nothing, since the holder could not
//!   use it on its own.

use serde::{Deserialize, Serialize};

use crate::structs::acl::{
    glob_match, AclAction, AclIdentity, AclResource, AclRule, AclScope, RESERVED_KV_PREFIX,
    RESERVED_POOL_PREFIX,
};
use crate::structs::namespace::RESERVED_NAMESPACE_PREFIX;
use crate::structs::workload_identity::CONTROL_PLANE_IDENTITY_NAME;

/// What one task's `legion` identity asks for: the unit a grant stores and
/// a token resolves.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskGrant {
    pub group: String,
    pub task: String,
    /// Inline rules, in the shape `legion acl policy apply` takes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permissions: Vec<AclRule>,
    /// Names of ACL policies, resolved by name when a token is verified.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policies: Vec<String>,
}

impl TaskGrant {
    /// Whether this task asks for anything at all.
    pub fn is_empty(&self) -> bool {
        self.permissions.is_empty() && self.policies.is_empty()
    }
}

/// Prefix of the policy names that stand for a grant rather than a stored
/// policy. A stored policy may not be named with it, so the two can never be
/// confused.
pub const GRANT_POLICY_PREFIX: &str = "grant:";

/// The policy-name reference a token carries for the grant of the job an
/// allocation runs: `grant:alloc/<id>`. Resolved when the token is used, not
/// when it is minted, so a revoked grant stops working at the next request
/// even for an ACL token minted from the identity by `legion login`.
pub fn grant_policy_ref(allocation_id: i64) -> String {
    format!("{GRANT_POLICY_PREFIX}alloc/{allocation_id}")
}

/// The allocation a [`grant_policy_ref`] names, if `name` is one.
pub fn parse_grant_policy_ref(name: &str) -> Option<i64> {
    name.strip_prefix(GRANT_POLICY_PREFIX)?
        .strip_prefix("alloc/")?
        .parse()
        .ok()
}

/// Each task's request, in task order, leaving out tasks that ask for
/// nothing. Only the reserved `legion` identity can ask (admission refuses
/// `permissions` and `policies` elsewhere).
pub fn requested_grants(tasks: &[crate::structs::TaskConfig]) -> Vec<TaskGrant> {
    tasks
        .iter()
        .filter_map(|task| {
            let identity = task
                .identities
                .iter()
                .find(|identity| identity.name == CONTROL_PLANE_IDENTITY_NAME)?;
            let grant = TaskGrant {
                group: task.group.clone(),
                task: task.name.clone(),
                permissions: identity.permissions.clone(),
                policies: identity.policies.clone(),
            };
            (!grant.is_empty()).then_some(grant)
        })
        .collect()
}

/// Why a requested permission is not covered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UncoveredReason {
    /// No held allow rule covers it.
    NotHeld,
    /// No held allow rule covers it, and one would if it named the reserved
    /// prefix: a held `*` does not reach `legion-*` or `legion/`.
    Reserved,
    /// Held, but the submitter also holds deny rules on the resource and no
    /// `acl:write`.
    DenyRules,
}

/// The first requested permission the submitter does not hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uncovered {
    /// `kv:write on certs/*`, `job:read on maximal-* in default`.
    pub what: String,
    pub resource: &'static str,
    pub reason: UncoveredReason,
}

/// The first rule in `requested` that `identity` does not cover, or `None`
/// when it covers them all (see the module docs for what "covers" means).
/// Ignores audit-only mode: the caller decides what a miss means there.
pub fn first_uncovered(identity: &AclIdentity, requested: &[AclRule]) -> Option<Uncovered> {
    if identity.is_management {
        return None;
    }
    let acl_writer = identity.would_allow(AclResource::Acl, AclAction::Write, &AclScope::none());
    for rule in requested.iter().filter(|rule| !rule.is_deny()) {
        for resource in expand_resources(rule) {
            for action in expand_actions(rule) {
                for namespace in namespace_atoms(rule) {
                    for path in path_atoms(rule) {
                        let atom = Atom {
                            rule,
                            resource,
                            action,
                            namespace: namespace.clone(),
                            path: path.clone(),
                        };
                        if !identity.rules.iter().any(|held| held_covers(held, &atom)) {
                            let reserved = reserved_is_why(&identity.rules, &atom);
                            return Some(Uncovered {
                                what: atom.describe(),
                                resource: resource.as_str(),
                                reason: if reserved {
                                    UncoveredReason::Reserved
                                } else {
                                    UncoveredReason::NotHeld
                                },
                            });
                        }
                        if !acl_writer && holds_deny_on(&identity.rules, resource) {
                            return Some(Uncovered {
                                what: atom.describe(),
                                resource: resource.as_str(),
                                reason: UncoveredReason::DenyRules,
                            });
                        }
                    }
                }
            }
        }
    }
    None
}

/// The resource named by a rule string, when it is one.
pub fn resource_named(name: &str) -> Option<AclResource> {
    const ALL: &[AclResource] = &[
        AclResource::Job,
        AclResource::Node,
        AclResource::Kv,
        AclResource::State,
        AclResource::Operator,
        AclResource::Acl,
        AclResource::Exec,
        AclResource::Action,
        AclResource::Template,
        AclResource::Scale,
        AclResource::Boot,
        AclResource::Net,
        AclResource::Snapshot,
        AclResource::Namespace,
        AclResource::App,
        AclResource::Object,
    ];
    ALL.iter()
        .copied()
        .find(|resource| resource.as_str() == name)
}

fn expand_resources(rule: &AclRule) -> Vec<AclResource> {
    if rule.resources.iter().any(|resource| resource == "*") {
        return AclResource::all()
            .iter()
            .filter_map(|name| resource_named(name))
            .collect();
    }
    rule.resources
        .iter()
        .filter_map(|name| resource_named(name))
        .collect()
}

/// `write` first: when both are missing, the error names the one that
/// matters more.
fn expand_actions(rule: &AclRule) -> Vec<AclAction> {
    let mut actions = Vec::new();
    for action in [AclAction::Write, AclAction::Read] {
        if rule
            .actions
            .iter()
            .any(|name| name == "*" || name == action.as_str())
        {
            actions.push(action);
        }
    }
    actions
}

/// One requested pattern of a multi-valued selector, or the selector left
/// out.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pattern {
    Unset,
    Glob(String),
}

fn namespace_atoms(rule: &AclRule) -> Vec<Pattern> {
    match &rule.namespaces {
        Some(patterns) => patterns.iter().cloned().map(Pattern::Glob).collect(),
        None => vec![Pattern::Unset],
    }
}

fn path_atoms(rule: &AclRule) -> Vec<Pattern> {
    match &rule.paths {
        Some(patterns) => patterns.iter().cloned().map(Pattern::Glob).collect(),
        None => vec![Pattern::Unset],
    }
}

/// One requested (resource, action) at one namespace pattern and one path
/// pattern; the rule's other selectors apply whole.
struct Atom<'a> {
    rule: &'a AclRule,
    resource: AclResource,
    action: AclAction,
    namespace: Pattern,
    path: Pattern,
}

impl Atom<'_> {
    fn describe(&self) -> String {
        let mut text = format!("{}:{}", self.resource.as_str(), self.action.as_str());
        if let Pattern::Glob(path) = &self.path {
            text.push_str(&format!(" on {path}"));
        }
        if let Some(names) = &self.rule.names {
            text.push_str(&format!(" on jobs {}", names.join(", ")));
        }
        if let Some(names) = &self.rule.action_names {
            text.push_str(&format!(" for actions {}", names.join(", ")));
        }
        if let Some(templates) = &self.rule.templates {
            text.push_str(&format!(" through templates {}", templates.join(", ")));
        }
        if let Pattern::Glob(namespace) = &self.namespace {
            text.push_str(&format!(" in {namespace}"));
        }
        if let Some(pools) = &self.rule.pools {
            text.push_str(&format!(" in pools {}", pools.join(", ")));
        }
        if let Some(bands) = &self.rule.bands {
            text.push_str(&format!(" in bands {}", bands.join(", ")));
        }
        if let Some(kinds) = &self.rule.kinds {
            text.push_str(&format!(" on kinds {}", kinds.join(", ")));
        }
        if self.rule.host == Some(true) {
            text.push_str(" (host = true)");
        }
        if self.rule.is_via_objects() {
            text.push_str(" (via objects)");
        }
        text
    }
}

/// The namespace an allow rule with no `namespaces` selector reaches, as a
/// pattern: what `*` reaches for a `*`-resource rule, and `default` for one
/// that names its resources (a rule from before namespaces; see
/// `acl::namespace_selector_matches`).
fn default_namespace_reach(rule: &AclRule) -> &'static str {
    if rule.resources.iter().any(|resource| resource == "*") {
        "*"
    } else {
        crate::structs::namespace::DEFAULT_NAMESPACE
    }
}

/// Whether the held glob `held` covers the requested glob `requested`
/// under the reserved-prefix rule for `prefix`.
fn pattern_covers(held: &str, requested: &str, prefix: &str) -> bool {
    if requested.starts_with(prefix) && !held.starts_with(prefix) {
        return false;
    }
    glob_match(held, requested)
}

fn held_covers(held: &AclRule, atom: &Atom<'_>) -> bool {
    // A held `via = "objects"` rule is usable only through an object-scoped
    // call, so it covers nothing a grant would let a plain credential do.
    if held.is_deny()
        || held.is_via_objects()
        || !rule_covers_pair(held, atom.resource, atom.action)
    {
        return false;
    }
    let requested = atom.rule;
    if requested.host == Some(true) && held.host != Some(true) {
        return false;
    }
    namespace_covered(held, requested, &atom.namespace)
        && path_covered(held, &atom.path)
        && plain_covered(&held.names, &requested.names)
        && pools_covered(&held.pools, &requested.pools)
        && plain_covered(&held.action_names, &requested.action_names)
        && plain_covered(&held.templates, &requested.templates)
        && plain_covered(&held.bands, &requested.bands)
        && plain_covered(&held.kinds, &requested.kinds)
}

fn rule_covers_pair(rule: &AclRule, resource: AclResource, action: AclAction) -> bool {
    rule.resources
        .iter()
        .any(|name| name == "*" || name == resource.as_str())
        && rule
            .actions
            .iter()
            .any(|name| name == "*" || name == action.as_str())
}

/// A requested `namespaces` pattern (or the selector left out) against a
/// held rule. A rule without the selector also matches requests that carry
/// no namespace at all, which a rule with one never does, so a request
/// without it needs a held rule without it.
fn namespace_covered(held: &AclRule, requested: &AclRule, atom: &Pattern) -> bool {
    let held_patterns: Vec<&str> = match &held.namespaces {
        Some(patterns) => patterns.iter().map(String::as_str).collect(),
        None => vec![default_namespace_reach(held)],
    };
    let wanted = match atom {
        Pattern::Glob(pattern) => pattern.as_str(),
        Pattern::Unset if held.namespaces.is_some() => return false,
        Pattern::Unset => default_namespace_reach(requested),
    };
    held_patterns
        .iter()
        .any(|held| pattern_covers(held, wanted, RESERVED_NAMESPACE_PREFIX))
}

/// A requested `paths` pattern (or the selector left out) against a held
/// rule. Without the selector an allow rule matches requests with no key,
/// and every key `*` reaches (none under `legion/`).
fn path_covered(held: &AclRule, atom: &Pattern) -> bool {
    match (&held.paths, atom) {
        (None, Pattern::Unset) => true,
        (None, Pattern::Glob(pattern)) => !pattern.starts_with(RESERVED_KV_PREFIX),
        (Some(_), Pattern::Unset) => false,
        (Some(patterns), Pattern::Glob(pattern)) => patterns
            .iter()
            .any(|held| pattern_covers(held, pattern, RESERVED_KV_PREFIX)),
    }
}

/// A requested `pools` selector against a held one, under the reserved-
/// prefix rule for `legion-*` pools (the managers' system workers): a held
/// rule without the selector reaches what `*` reaches, so it covers a
/// request without one and every requested pattern that reaches no
/// reserved pool; one with it covers a requested pattern only through a
/// held pattern that covers it under the rule, and never a request without
/// it.
fn pools_covered(held: &Option<Vec<String>>, requested: &Option<Vec<String>>) -> bool {
    match (held, requested) {
        (None, None) => true,
        (None, Some(requested)) => requested
            .iter()
            .all(|pattern| !pattern.starts_with(RESERVED_POOL_PREFIX)),
        (Some(_), None) => false,
        (Some(held), Some(requested)) => requested.iter().all(|pattern| {
            held.iter()
                .any(|glob| pattern_covers(glob, pattern, RESERVED_POOL_PREFIX))
        }),
    }
}

/// A plain selector (`names`, `actionNames`, `templates`, `bands`): a held
/// rule without it covers anything; one with it covers only requested
/// patterns it matches, and never a request without it.
fn plain_covered(held: &Option<Vec<String>>, requested: &Option<Vec<String>>) -> bool {
    match (held, requested) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(held), Some(requested)) => requested
            .iter()
            .all(|pattern| held.iter().any(|glob| glob_match(glob, pattern))),
    }
}

fn holds_deny_on(rules: &[AclRule], resource: AclResource) -> bool {
    rules.iter().any(|rule| {
        rule.is_deny()
            && rule
                .resources
                .iter()
                .any(|name| name == "*" || name == resource.as_str())
    })
}

/// Whether the reserved-prefix rule is what stands between the submitter
/// and the request: some held rule would cover it if the reserved prefix
/// were not special.
fn reserved_is_why(rules: &[AclRule], atom: &Atom<'_>) -> bool {
    let reserved_namespace =
        matches!(&atom.namespace, Pattern::Glob(p) if p.starts_with(RESERVED_NAMESPACE_PREFIX));
    let reserved_path = matches!(&atom.path, Pattern::Glob(p) if p.starts_with(RESERVED_KV_PREFIX));
    let reserved_pool = atom.rule.pools.as_ref().is_some_and(|pools| {
        pools
            .iter()
            .any(|pool| pool.starts_with(RESERVED_POOL_PREFIX))
    });
    if !reserved_namespace && !reserved_path && !reserved_pool {
        return false;
    }
    let unreserved_rule = AclRule {
        pools: atom.rule.pools.as_ref().map(|pools| {
            pools
                .iter()
                .map(|pool| pool.trim_start_matches(RESERVED_POOL_PREFIX).to_string())
                .collect()
        }),
        ..atom.rule.clone()
    };
    let plain = |pattern: &Pattern| match pattern {
        Pattern::Glob(p) => Pattern::Glob(
            p.trim_start_matches(RESERVED_NAMESPACE_PREFIX)
                .trim_start_matches(RESERVED_KV_PREFIX)
                .to_string(),
        ),
        Pattern::Unset => Pattern::Unset,
    };
    let unreserved = Atom {
        rule: &unreserved_rule,
        resource: atom.resource,
        action: atom.action,
        namespace: plain(&atom.namespace),
        path: plain(&atom.path),
    };
    rules.iter().any(|held| held_covers(held, &unreserved))
}

#[cfg(test)]
mod grant_test {
    use super::*;
    use crate::structs::acl::{parse_rules, AclSource};

    /// Every ACL resource is one a grant can name: a resource missing here
    /// would drop out of the coverage check, so a grant asking for it would
    /// pass unchecked.
    #[test]
    fn every_acl_resource_is_a_grant_resource_test() {
        for name in AclResource::all() {
            assert!(resource_named(name).is_some(), "{name}");
        }
    }

    fn identity(rules_json: &str) -> AclIdentity {
        AclIdentity {
            principal: "tester".into(),
            accessor_id: None,
            is_management: false,
            is_worker: false,
            policies: vec!["held".into()],
            rules: parse_rules(rules_json).expect("held rules parse"),
            source: AclSource::Token,
            expires_at: None,
            audit_only: false,
            acting_for: None,
        }
    }

    fn requested(rules_json: &str) -> Vec<AclRule> {
        parse_rules(rules_json).expect("requested rules parse")
    }

    fn covers(held: &str, wanted: &str) -> bool {
        first_uncovered(&identity(held), &requested(wanted)).is_none()
    }

    /// The coverage table (apps plan §9 A2): sound cases, conservative
    /// refusals, deny rules, and the reserved prefix.
    #[test]
    fn glob_coverage_table_test() {
        let cases: &[(&str, &str, bool, &str)] = &[
            // Sound: the requested pattern, as a string, matches a held glob.
            (
                r#"[{"resources":["kv"],"actions":["read","write"],"paths":["certs/*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                true,
                "the same glob",
            ),
            (
                r#"[{"resources":["kv"],"actions":["*"],"paths":["certs/*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/app.example.com"]}]"#,
                true,
                "a literal under a held glob",
            ),
            (
                r#"[{"resources":["job"],"actions":["read"],"namespaces":["pay*"]}]"#,
                r#"[{"resources":["job"],"actions":["read"],"namespaces":["payments-*"]}]"#,
                true,
                "a narrower glob",
            ),
            (
                r#"[{"resources":["*"],"actions":["*"],"namespaces":["*"]}]"#,
                r#"[{"resources":["job","exec"],"actions":["*"],"namespaces":["team-a"]}]"#,
                true,
                "every namespaced resource in every tenant namespace",
            ),
            (
                r#"[{"resources":["kv"],"actions":["write"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                true,
                "a held rule without paths reaches every key `*` reaches",
            ),
            // Not held at all.
            (
                r#"[{"resources":["job"],"actions":["write"],"namespaces":["*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                false,
                "another resource",
            ),
            (
                r#"[{"resources":["kv"],"actions":["read"],"paths":["certs/*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                false,
                "another action",
            ),
            (
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["*"]}]"#,
                false,
                "a wider glob",
            ),
            // Conservative refusals: sound to refuse, though some are
            // covered in fact.
            (
                r#"[{"resources":["kv"],"actions":["write"],"paths":["a*","b*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["*"]}]"#,
                false,
                "a union of held globs never covers a wider request",
            ),
            (
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"]}]"#,
                false,
                "a request without paths needs a held rule without paths",
            ),
            (
                r#"[{"resources":["job"],"actions":["read"],"namespaces":["*"],"names":["*"]}]"#,
                r#"[{"resources":["job"],"actions":["read"],"namespaces":["*"]}]"#,
                false,
                "a request without names needs a held rule without names",
            ),
            (
                r#"[{"resources":["*"],"actions":["*"],"namespaces":["*"]}]"#,
                r#"[{"resources":["kv"],"actions":["read"]}]"#,
                false,
                "a held namespaced rule never matches a cluster-scoped request",
            ),
            // Host-level access must be held as host-level access.
            (
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["*"]}]"#,
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["*"],"host":true}]"#,
                false,
                "host = true",
            ),
            // The reserved prefix: a held `*` covers neither `legion-*` nor
            // `legion/`.
            (
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["*"]}]"#,
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["legion-*"]}]"#,
                false,
                "a held * does not cover a requested legion-*",
            ),
            (
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["*"]}]"#,
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["legion-system"]}]"#,
                false,
                "nor legion-system",
            ),
            (
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["*","legion-*"]}]"#,
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["legion-system"]}]"#,
                true,
                "a held legion-* does",
            ),
            (
                r#"[{"resources":["kv"],"actions":["write"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["legion/leases/*"]}]"#,
                false,
                "a held rule without paths does not reach legion/",
            ),
            (
                r#"[{"resources":["kv"],"actions":["write"],"paths":["*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["legion/*"]}]"#,
                false,
                "a held * path does not cover legion/*",
            ),
            (
                r#"[{"resources":["kv"],"actions":["write"],"paths":["legion/*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["legion/leases/*"]}]"#,
                true,
                "a held legion/* does",
            ),
            (
                r#"[{"resources":["kv"],"actions":["write"],"paths":["*"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["leg*"]}]"#,
                true,
                "leg* reaches no reserved key, so plain coverage applies",
            ),
            // Deny rules: any deny on the resource needs acl:write.
            (
                r#"[{"resources":["kv"],"actions":["*"],"paths":["*"]},
                    {"resources":["kv"],"actions":["write"],"paths":["secret/*"],"effect":"deny"}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                false,
                "a deny on the resource, without acl:write",
            ),
            (
                r#"[{"resources":["kv"],"actions":["*"],"paths":["*"]},
                    {"resources":["kv"],"actions":["write"],"paths":["secret/*"],"effect":"deny"},
                    {"resources":["acl"],"actions":["write"]}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                true,
                "a deny on the resource, with acl:write",
            ),
            (
                r#"[{"resources":["kv"],"actions":["*"],"paths":["*"]},
                    {"resources":["job"],"actions":["write"],"namespaces":["prod"],"effect":"deny"}]"#,
                r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#,
                true,
                "a deny on another resource does not matter",
            ),
            // Requested deny rules only take access away.
            (
                r#"[{"resources":["kv"],"actions":["read"],"paths":["certs/*"]}]"#,
                r#"[{"resources":["*"],"actions":["*"],"effect":"deny"}]"#,
                true,
                "a requested deny",
            ),
            // `*` resources ask for each resource.
            (
                r#"[{"resources":["kv"],"actions":["*"]}]"#,
                r#"[{"resources":["*"],"actions":["read"],"namespaces":["default"]}]"#,
                false,
                "a requested * resource needs every resource",
            ),
        ];
        for (held, wanted, expected, case) in cases {
            assert_eq!(covers(held, wanted), *expected, "case: {case}");
        }
    }

    #[test]
    fn management_tokens_cover_everything_test() {
        let mut admin = identity("[]");
        admin.is_management = true;
        assert!(first_uncovered(
            &admin,
            &requested(r#"[{"resources":["*"],"actions":["*"],"namespaces":["legion-*"]}]"#)
        )
        .is_none());
    }

    #[test]
    fn misses_say_what_and_why_test() {
        let miss = first_uncovered(
            &identity(r#"[{"resources":["job"],"actions":["write"],"namespaces":["*"]}]"#),
            &requested(r#"[{"resources":["kv"],"actions":["read","write"],"paths":["certs/*"]}]"#),
        )
        .expect("not held");
        assert_eq!(miss.what, "kv:write on certs/*");
        assert_eq!(miss.reason, UncoveredReason::NotHeld);

        let miss = first_uncovered(
            &identity(r#"[{"resources":["exec"],"actions":["write"],"namespaces":["*"]}]"#),
            &requested(
                r#"[{"resources":["exec"],"actions":["write"],"namespaces":["legion-system"]}]"#,
            ),
        )
        .expect("reserved");
        assert_eq!(miss.what, "exec:write in legion-system");
        assert_eq!(miss.reason, UncoveredReason::Reserved);

        let miss = first_uncovered(
            &identity(
                r#"[{"resources":["kv"],"actions":["*"],"paths":["*"]},
                    {"resources":["kv"],"actions":["read"],"paths":["secret/*"],"effect":"deny"}]"#,
            ),
            &requested(r#"[{"resources":["kv"],"actions":["write"],"paths":["certs/*"]}]"#),
        )
        .expect("deny rules");
        assert_eq!(miss.reason, UncoveredReason::DenyRules);
    }

    #[test]
    fn grant_policy_refs_round_trip_test() {
        assert_eq!(grant_policy_ref(42), "grant:alloc/42");
        assert_eq!(parse_grant_policy_ref("grant:alloc/42"), Some(42));
        assert_eq!(parse_grant_policy_ref("grant:alloc/x"), None);
        assert_eq!(parse_grant_policy_ref("cert-writer"), None);
    }
}
