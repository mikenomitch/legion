//! Namespaces: the name scope, permission scope and policy anchor for jobs
//! (docs/namespaces.md).
//!
//! A namespace is **not** an isolation boundary. It separates names (two
//! namespaces may each hold a job called `web`), scopes ACL rules (the
//! `namespaces` selector), and is what namespace policies cap. It does not
//! move work onto other machines, networks or kernels; node pools, `gated`
//! networking and the `sandbox` runtime do that.
//!
//! # Job ids
//!
//! A job's id is its **canonical id**: the bare job name for a job in the
//! `default` namespace (`web`), and `<namespace>/<name>` for every other
//! namespace (`payments-prod/web`). Every key derived from a job id (job
//! versions, slot instances, volume claims, seal keys, content-store paths,
//! events) therefore carries the namespace by construction, and a cluster
//! that only ever uses `default` sees exactly the ids it always did.
//!
//! Job names may not contain `/` (`validate_object_name`) and namespace
//! names may not either, so the split is total and unambiguous:
//! [`split_job_id`] never fails. `default/web` is accepted as *input*
//! anywhere a job is named and canonicalized to `web`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The namespace every cluster starts with. It always exists, cannot be
/// deleted, and is where a job lands when nothing names a namespace.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Longest namespace name: one DNS label (`<svc>.<ns>.svc.legion`).
pub const MAX_NAMESPACE_NAME_LEN: usize = 63;

/// Prefix reserved for namespaces Legion itself may create, so a future
/// built-in cannot collide with a tenant's.
pub const RESERVED_NAMESPACE_PREFIX: &str = "legion-";

/// Env var a task receives naming its namespace, and the CLI's
/// active-namespace default.
pub const NAMESPACE_ENV: &str = "LEGION_NAMESPACE";

/// A stored namespace. Carries nothing that governs anything: quotas and
/// allowlists live in namespace policies, which a namespace writer cannot
/// edit (docs/namespaces.md, "Governance").
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Namespace {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
}

/// The file form `legion namespace apply -f` reads.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NamespaceInput {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub meta: BTreeMap<String, String>,
}

impl From<NamespaceInput> for Namespace {
    fn from(input: NamespaceInput) -> Self {
        Namespace {
            name: input.name,
            description: input.description,
            meta: input.meta,
        }
    }
}

/// Validates a namespace name's grammar: a DNS label (lowercase letters,
/// digits and `-`, starting and ending alphanumeric, at most 63 characters).
/// Used for every *reference* to a namespace; [`validate_new_namespace_name`]
/// adds the rules that only apply when one is created.
pub fn validate_namespace_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAMESPACE_NAME_LEN {
        return Err(format!(
            "namespace {name:?} must be 1-{MAX_NAMESPACE_NAME_LEN} characters"
        ));
    }
    let valid_chars = name
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-');
    let alnum_ends = name
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphanumeric())
        && name
            .chars()
            .last()
            .is_some_and(|ch| ch.is_ascii_alphanumeric());
    if !valid_chars || !alnum_ends {
        return Err(format!(
            "namespace {name:?} must be lowercase letters, digits and '-', starting and ending \
             with a letter or digit (it becomes a DNS label: <service>.<namespace>.svc.legion)"
        ));
    }
    Ok(())
}

/// [`validate_namespace_name`] plus the creation-only rules: the
/// `legion-` prefix is reserved for system namespaces.
pub fn validate_new_namespace_name(name: &str) -> Result<(), String> {
    validate_namespace_name(name)?;
    if name.starts_with(RESERVED_NAMESPACE_PREFIX) {
        return Err(format!(
            "namespace {name:?} uses the reserved prefix {RESERVED_NAMESPACE_PREFIX:?}"
        ));
    }
    Ok(())
}

/// The canonical id of job `name` in `namespace`: the bare name in
/// `default`, `namespace/name` everywhere else.
pub fn job_id(namespace: &str, name: &str) -> String {
    if namespace.is_empty() || namespace == DEFAULT_NAMESPACE {
        name.to_string()
    } else {
        format!("{namespace}/{name}")
    }
}

/// Splits a canonical job id into `(namespace, name)`. Total: an id with no
/// `/` is a job in `default`.
pub fn split_job_id(id: &str) -> (&str, &str) {
    match id.split_once('/') {
        Some((namespace, name)) => (namespace, name),
        None => (DEFAULT_NAMESPACE, id),
    }
}

/// The namespace of a canonical job id.
pub fn job_namespace(id: &str) -> &str {
    split_job_id(id).0
}

/// The bare job name of a canonical job id.
pub fn job_name(id: &str) -> &str {
    split_job_id(id).1
}

/// The display form of a job id: always `namespace/name`, so it is
/// unambiguous cluster-wide (`default/web` for a job in `default`).
pub fn qualified_display(id: &str) -> String {
    let (namespace, name) = split_job_id(id);
    format!("{namespace}/{name}")
}

/// Resolves a reference made *from inside* `namespace` — a service an
/// upstream dials, a job an intention admits — to a canonical id: a bare
/// name means `namespace` itself, and `ns/name` names another namespace
/// explicitly. Crossing a namespace is therefore always visible in the
/// config that does it.
pub fn qualify_in(namespace: &str, reference: &str) -> String {
    match reference.split_once('/') {
        Some((other, name)) => job_id(other, name),
        None => job_id(namespace, reference),
    }
}

/// The canonical id of an instance a caller names `key` in a job of
/// `namespace`: qualified exactly like a job id (the key in `default`,
/// `namespace/key` elsewhere), so caller-chosen keys — and their
/// never-recycled tombstones — are unique per namespace. A server-minted id
/// is cluster-unique by construction and stays bare.
pub fn instance_id(namespace: &str, key: &str) -> String {
    if crate::structs::is_minted_instance_id(key) {
        return key.to_string();
    }
    job_id(namespace, key)
}

/// [`instance_id`] for a value that may already be canonical: an id with a
/// `/` is kept as written, anything else is a key qualified in
/// `namespace`. For internal paths that hand over either form; a caller's
/// input goes through [`resolve_instance_ref`] instead.
pub fn qualify_instance_in(namespace: &str, id: &str) -> String {
    if id.contains('/') {
        id.to_string()
    } else {
        instance_id(namespace, id)
    }
}

/// The key of a canonical instance id: what follows the namespace.
pub fn instance_key(id: &str) -> &str {
    job_name(id)
}

/// Resolves a user-typed instance reference to a canonical instance id, the
/// way [`resolve_job_ref`] resolves jobs: `namespace/key` as written, a
/// minted id as written, else the key qualified in `active` (`default` when
/// `None`).
pub fn resolve_instance_ref(input: &str, active: Option<&str>) -> Result<String, String> {
    let input = input.trim();
    if input.contains('/') {
        return resolve_job_ref(input, active);
    }
    if input.is_empty() {
        return Err("instance id must not be empty".into());
    }
    let namespace = active
        .filter(|active| !active.is_empty())
        .unwrap_or(DEFAULT_NAMESPACE);
    validate_namespace_name(namespace)?;
    Ok(instance_id(namespace, input))
}

/// Whether a job pattern matches a canonical job id. A pattern with a `/`
/// is `namespace-glob/name-glob` (`payments-*/web*`); one without matches
/// names in `default` only. So `*` never reaches into another namespace,
/// and crossing one is written into the pattern.
pub fn job_pattern_matches(pattern: &str, id: &str) -> bool {
    let (namespace, name) = split_job_id(id);
    match pattern.split_once('/') {
        Some((namespace_glob, name_glob)) => {
            crate::structs::acl::glob_match(namespace_glob, namespace)
                && crate::structs::acl::glob_match(name_glob, name)
        }
        None => namespace == DEFAULT_NAMESPACE && crate::structs::acl::glob_match(pattern, name),
    }
}

/// Resolves a user-typed job reference to a canonical job id.
///
/// `input` is either a bare name (resolved in `active`, or `default` when
/// `active` is `None`) or `namespace/name`. A qualified reference that names
/// a different namespace than an explicit `active` is an error rather than
/// a precedence rule: two inputs written in the same breath that disagree
/// have no correct winner.
pub fn resolve_job_ref(input: &str, active: Option<&str>) -> Result<String, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("job name must not be empty".into());
    }
    match input.split_once('/') {
        Some((namespace, name)) => {
            validate_namespace_name(namespace)?;
            if name.is_empty() || name.contains('/') {
                return Err(format!(
                    "{input:?} is not a job reference: expected `name` or `namespace/name`"
                ));
            }
            if let Some(active) = active.filter(|active| !active.is_empty()) {
                if active != namespace {
                    return Err(format!(
                        "{input:?} names namespace '{namespace}' but the request names \
                         '{active}'; drop one, a qualified name does not need the other"
                    ));
                }
            }
            Ok(job_id(namespace, name))
        }
        None => {
            let namespace = active
                .filter(|active| !active.is_empty())
                .unwrap_or(DEFAULT_NAMESPACE);
            validate_namespace_name(namespace)?;
            Ok(job_id(namespace, input))
        }
    }
}

/// A set of namespaces a list request selects: names or `*`-globs.
/// Empty means `default`. `*` means every namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceSelection {
    patterns: Vec<String>,
}

impl NamespaceSelection {
    /// Parses list-request selectors. Each entry may itself be a
    /// comma-separated list, so `-n 'a,b'` and `-n a -n b` mean the same.
    pub fn parse<S: AsRef<str>>(selectors: &[S]) -> Result<Self, String> {
        let mut patterns = Vec::new();
        for selector in selectors {
            for part in selector.as_ref().split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                if !part.contains('*') {
                    validate_namespace_name(part)?;
                } else if !part.chars().all(|ch| {
                    ch == '*' || ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-'
                }) {
                    return Err(format!(
                        "namespace selector {part:?} may only contain lowercase letters, digits, \
                         '-' and '*'"
                    ));
                }
                patterns.push(part.to_string());
            }
        }
        if patterns.is_empty() {
            patterns.push(DEFAULT_NAMESPACE.to_string());
        }
        Ok(NamespaceSelection { patterns })
    }

    /// Every namespace.
    pub fn all() -> Self {
        NamespaceSelection {
            patterns: vec!["*".into()],
        }
    }

    /// Exactly one namespace.
    pub fn one(namespace: &str) -> Self {
        NamespaceSelection {
            patterns: vec![namespace.to_string()],
        }
    }

    pub fn matches(&self, namespace: &str) -> bool {
        self.patterns
            .iter()
            .any(|pattern| crate::structs::acl::glob_match(pattern, namespace))
    }

    /// Whether the selection is the single exact namespace `namespace`.
    pub fn is_exactly(&self, namespace: &str) -> bool {
        self.patterns.len() == 1 && self.patterns[0] == namespace
    }

    /// Whether the selection can match more than one namespace.
    pub fn is_multi(&self) -> bool {
        self.patterns.len() > 1 || self.patterns.iter().any(|p| p.contains('*'))
    }

    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }
}

#[cfg(test)]
mod namespace_test {
    use super::*;

    #[test]
    fn instance_ids_qualify_keys_but_not_minted_ids() {
        assert_eq!(instance_id("default", "user-42"), "user-42");
        assert_eq!(instance_id("payments", "user-42"), "payments/user-42");
        let minted = crate::structs::mint_instance_id("workspace");
        assert_eq!(instance_id("payments", &minted), minted);
        assert_eq!(instance_key("payments/user-42"), "user-42");
        assert_eq!(
            resolve_instance_ref("user-42", Some("payments")).unwrap(),
            "payments/user-42"
        );
        assert_eq!(
            resolve_instance_ref("payments/user-42", None).unwrap(),
            "payments/user-42"
        );
        assert_eq!(
            resolve_instance_ref(&minted, Some("payments")).unwrap(),
            minted
        );
        assert!(resolve_instance_ref("other/user-42", Some("payments")).is_err());
    }

    #[test]
    fn job_patterns_cross_a_namespace_only_when_they_say_so() {
        assert!(job_pattern_matches("agents-*", "agents-1"));
        assert!(!job_pattern_matches("agents-*", "payments/agents-1"));
        assert!(!job_pattern_matches("*", "payments/web"));
        assert!(job_pattern_matches(
            "payments-*/web*",
            "payments-prod/web-2"
        ));
        assert!(!job_pattern_matches("payments-*/web*", "payments-prod/api"));
        assert!(job_pattern_matches("*/*", "payments/web"));
        assert!(
            job_pattern_matches("*/*", "web"),
            "default is a namespace too"
        );
        assert!(job_pattern_matches("default/web", "web"));
    }

    #[test]
    fn namespace_grammar_is_a_dns_label() {
        assert!(validate_namespace_name("default").is_ok());
        assert!(validate_namespace_name("payments-prod").is_ok());
        assert!(validate_namespace_name("a").is_ok());
        assert!(validate_namespace_name("0team").is_ok());
        assert!(validate_namespace_name(&"a".repeat(63)).is_ok());
        assert!(validate_namespace_name(&"a".repeat(64)).is_err());
        assert!(validate_namespace_name("").is_err());
        assert!(validate_namespace_name("Prod").is_err());
        assert!(validate_namespace_name("-prod").is_err());
        assert!(validate_namespace_name("prod-").is_err());
        assert!(validate_namespace_name("pro_d").is_err());
        assert!(validate_namespace_name("pro.d").is_err());
        assert!(validate_namespace_name("a/b").is_err());
        assert!(validate_namespace_name("*").is_err());
        assert!(validate_namespace_name("a,b").is_err());
    }

    #[test]
    fn the_legion_prefix_is_reserved_only_at_creation() {
        assert!(validate_namespace_name("legion-system").is_ok());
        assert!(validate_new_namespace_name("legion-system").is_err());
        assert!(validate_new_namespace_name("legionnaire").is_ok());
    }

    #[test]
    fn default_namespace_ids_are_bare_names() {
        assert_eq!(job_id("default", "web"), "web");
        assert_eq!(job_id("", "web"), "web");
        assert_eq!(job_id("prod", "web"), "prod/web");
        assert_eq!(split_job_id("web"), ("default", "web"));
        assert_eq!(split_job_id("prod/web"), ("prod", "web"));
        assert_eq!(qualified_display("web"), "default/web");
        assert_eq!(qualified_display("prod/web"), "prod/web");
        // Distinct namespaces never share an id.
        assert_ne!(job_id("prod", "web"), job_id("dev", "web"));
        assert_ne!(job_id("prod", "web"), job_id("default", "web"));
    }

    #[test]
    fn job_refs_resolve_in_the_active_namespace() {
        assert_eq!(resolve_job_ref("web", None).unwrap(), "web");
        assert_eq!(resolve_job_ref("web", Some("prod")).unwrap(), "prod/web");
        assert_eq!(resolve_job_ref("prod/web", None).unwrap(), "prod/web");
        assert_eq!(
            resolve_job_ref("prod/web", Some("prod")).unwrap(),
            "prod/web"
        );
        assert_eq!(resolve_job_ref("default/web", None).unwrap(), "web");
        assert_eq!(
            resolve_job_ref("default/web", Some("default")).unwrap(),
            "web"
        );
        let conflict = resolve_job_ref("prod/web", Some("dev")).unwrap_err();
        assert!(conflict.contains("names namespace 'prod'"), "{conflict}");
        assert!(resolve_job_ref("Prod/web", None).is_err());
        assert!(resolve_job_ref("prod/", None).is_err());
        assert!(resolve_job_ref("a/b/c", None).is_err());
        assert!(resolve_job_ref("", None).is_err());
        assert!(resolve_job_ref("web", Some("Bad")).is_err());
    }

    #[test]
    fn references_made_from_a_namespace_stay_in_it_unless_qualified() {
        assert_eq!(qualify_in("prod", "redis"), "prod/redis");
        assert_eq!(qualify_in("default", "redis"), "redis");
        assert_eq!(qualify_in("prod", "shared/auth"), "shared/auth");
        assert_eq!(qualify_in("prod", "default/auth"), "auth");
    }

    #[test]
    fn selections_glob_and_default_to_default() {
        let empty: &[&str] = &[];
        let default = NamespaceSelection::parse(empty).unwrap();
        assert!(default.is_exactly("default"));
        assert!(!default.is_multi());
        let multi = NamespaceSelection::parse(&["payments-*,shared"]).unwrap();
        assert!(multi.matches("payments-prod"));
        assert!(multi.matches("shared"));
        assert!(!multi.matches("default"));
        assert!(multi.is_multi());
        assert!(NamespaceSelection::parse(&["*"])
            .unwrap()
            .matches("anything"));
        assert!(NamespaceSelection::parse(&["Bad"]).is_err());
        assert!(NamespaceSelection::parse(&["a/b*"]).is_err());
    }
}
