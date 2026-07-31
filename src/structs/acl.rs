//! ACL policy model and evaluation engine.
//!
//! Legion ACLs are token -> policies -> rules: a request resolves to a set of
//! named policies, each policy is a list of allow/deny rules over (resource,
//! action) pairs, rules may be narrowed with glob scope selectors
//! (namespaces, job names, node pools, KV paths), evaluation is default-deny,
//! and a matching deny rule always overrides any allow.
//!
//! This module is pure logic (no storage, no transport) so it can be unit
//! tested exhaustively. Credential resolution lives in `crate::manager::acl`.

use serde::{Deserialize, Serialize};

/// Resource types ACL rules can cover.
///
/// Deliberately a short, closed list: every gRPC endpoint maps onto exactly
/// one of these (see the endpoint table in `tests/acl_endpoints.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclResource {
    /// Jobs, rollouts, job specs, and plans.
    Job,
    /// Worker nodes, node pools, and the node/allocation ledger.
    Node,
    /// KV entries and KV locks.
    Kv,
    /// State-store queries, schema, and saved queries (raw DB read access).
    State,
    /// Manager/cluster operations: raft, manager configs, observability.
    Operator,
    /// ACL administration: tokens, policies, users.
    Acl,
    /// Interactive/one-shot exec into a running allocation. `write` opens a
    /// session; the `host` selector gates host-level (unsandboxed) kinds.
    Exec,
    /// Running a declared action against an allocation. `write` runs it;
    /// narrower than `exec` (the command is fixed and server-rendered).
    Action,
    /// Job templates: `read` lists/fetches them, `write` creates new
    /// versions and deletes them. Submitting a job *through* a template is
    /// gated separately, by a `job` rule carrying the `templates` selector.
    Template,
    /// Task count overrides (`legion job scale`, docs/autoscaling.md):
    /// `read` inspects scale status, `write` sets/resets overrides.
    /// Deliberately separate from `job` so a principal (an autoscaler, a
    /// teammate) can resize a job without the right to redeploy it. Scope
    /// with the job-name selector, like `job` rules.
    Scale,
}

impl AclResource {
    pub fn as_str(&self) -> &'static str {
        match self {
            AclResource::Job => "job",
            AclResource::Node => "node",
            AclResource::Kv => "kv",
            AclResource::State => "state",
            AclResource::Operator => "operator",
            AclResource::Acl => "acl",
            AclResource::Exec => "exec",
            AclResource::Action => "action",
            AclResource::Template => "template",
            AclResource::Scale => "scale",
        }
    }

    pub fn all() -> &'static [&'static str] {
        &[
            "job", "node", "kv", "state", "operator", "acl", "exec", "action", "template", "scale",
        ]
    }
}

/// Actions ACL rules can grant. `write` does not imply `read`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclAction {
    Read,
    Write,
}

impl AclAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            AclAction::Read => "read",
            AclAction::Write => "write",
        }
    }
}

/// One rule inside a policy document.
///
/// Serialized as JSON inside `AclPolicy.rules_json`. Example:
/// `{"resources": ["job"], "actions": ["read"], "namespaces": ["default"]}`.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AclRule {
    /// Resource types: `job`, `node`, `kv`, `state`, `operator`, `acl`,
    /// `exec`, `action`, `template`, `scale`, `*`.
    pub resources: Vec<String>,
    /// Actions: `read`, `write`, `*`.
    pub actions: Vec<String>,
    /// `allow` (default) or `deny`. A matching deny overrides any allow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    /// Glob selectors over job namespaces (jobs live in `default` today).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<String>>,
    /// Glob selectors over job names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
    /// Glob selectors over node pools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pools: Option<Vec<String>>,
    /// Glob selectors over KV keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paths: Option<Vec<String>>,
    /// Glob selectors over action names (on `action` rules). Named
    /// `action_names` rather than `actions` to avoid colliding with the
    /// read/write `actions` field above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_names: Option<Vec<String>>,
    /// Glob selectors over job-template names. On `template` rules this
    /// scopes which templates the rule covers. On `job` rules it scopes the
    /// rule to requests that involve a matching template: submissions made
    /// *through* the template (RunJobTemplate/RenderJobTemplate), and reads
    /// or lifecycle writes of jobs whose latest config *came from* the
    /// template. Because a rule with a selector never matches a request
    /// without the attribute, a `job` rule with `templates` grants nothing
    /// to plain RunJob submissions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub templates: Option<Vec<String>>,
    /// On `exec` rules: when true, the rule also grants exec into host-level
    /// (unsandboxed) kinds. When absent/false, it matches only sandboxed
    /// kinds. Mirrors Nomad's `alloc-exec` vs `alloc-node-exec` split.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<bool>,
}

impl AclRule {
    pub fn is_deny(&self) -> bool {
        matches!(self.effect.as_deref(), Some("deny"))
    }

    fn has_scope_selectors(&self) -> bool {
        self.namespaces.is_some()
            || self.names.is_some()
            || self.pools.is_some()
            || self.paths.is_some()
            || self.action_names.is_some()
            || self.templates.is_some()
    }
}

/// The instance-level attributes of a request, matched against rule selectors.
///
/// `AclScope::any()` is used to gate list endpoints ("could the caller read
/// any instance at all?"); concrete scopes are used everywhere else and for
/// per-item filtering of list results.
#[derive(Debug, Clone, Default)]
pub struct AclScope {
    /// Matches every selector. Only for list-gating reads.
    pub any: bool,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub pool: Option<String>,
    pub path: Option<String>,
    /// Action name, for `action`-resource scopes.
    pub action_name: Option<String>,
    /// Job-template name. On `template`-resource scopes: the template being
    /// read or written. On `job` scopes: the template a submission is going
    /// through (write path) or the template that produced the job's latest
    /// config (read/lifecycle path). Never populated from provenance on the
    /// spec-changing write path — that would let a template-scoped identity
    /// push arbitrary specs to a job its template once created.
    pub template: Option<String>,
    /// True when the exec target is a host-level (unsandboxed) kind. Only an
    /// `exec` rule with `host: true` matches such a scope.
    pub host: bool,
}

/// Namespace assigned to all jobs until Legion grows real namespaces.
pub const DEFAULT_NAMESPACE: &str = "default";

impl AclScope {
    /// Scope with no instance attributes (endpoints with nothing to select on).
    pub fn none() -> Self {
        AclScope::default()
    }

    /// Wildcard scope for gating list endpoints before per-item filtering.
    pub fn any() -> Self {
        AclScope {
            any: true,
            ..Default::default()
        }
    }

    /// Scope for a job, in the implicit default namespace.
    pub fn job(name: &str) -> Self {
        AclScope {
            namespace: Some(DEFAULT_NAMESPACE.to_string()),
            name: Some(name.to_string()),
            ..Default::default()
        }
    }

    /// Scope for a node in a pool.
    pub fn pool(pool: &str) -> Self {
        AclScope {
            pool: Some(pool.to_string()),
            ..Default::default()
        }
    }

    /// Scope for a KV key.
    pub fn path(path: &str) -> Self {
        AclScope {
            path: Some(path.to_string()),
            ..Default::default()
        }
    }

    /// Scope for an exec session into a job. `host` marks host-level kinds.
    pub fn exec(job: &str, host: bool) -> Self {
        AclScope {
            namespace: Some(DEFAULT_NAMESPACE.to_string()),
            name: Some(job.to_string()),
            host,
            ..Default::default()
        }
    }

    /// Scope for running an action on a job.
    pub fn action(job: &str, action_name: &str) -> Self {
        AclScope {
            namespace: Some(DEFAULT_NAMESPACE.to_string()),
            name: Some(job.to_string()),
            action_name: Some(action_name.to_string()),
            ..Default::default()
        }
    }

    /// Scope for a job template (template CRUD endpoints).
    pub fn template(name: &str) -> Self {
        AclScope {
            template: Some(name.to_string()),
            ..Default::default()
        }
    }

    /// Scope for submitting a job through a template (RunJobTemplate /
    /// RenderJobTemplate): the rendered job name plus the template it came
    /// through, so both `names` and `templates` selectors apply.
    pub fn job_via_template(job: &str, template: &str) -> Self {
        AclScope {
            namespace: Some(DEFAULT_NAMESPACE.to_string()),
            name: Some(job.to_string()),
            template: Some(template.to_string()),
            ..Default::default()
        }
    }

    /// Attaches a template attribute (a job's stored provenance) to an
    /// existing scope. Used on read/lifecycle endpoints so `templates`-scoped
    /// rules cover the jobs their templates created.
    pub fn with_template(mut self, template: Option<String>) -> Self {
        self.template = template;
        self
    }
}

/// Glob match supporting `*` (any run of characters, including empty).
///
/// This is the only wildcard the ACL system supports; keeping the matcher
/// trivial keeps its security behavior easy to reason about.
pub fn glob_match(pattern: &str, value: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }

    let mut remaining = value;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            match remaining.strip_prefix(part) {
                Some(rest) => remaining = rest,
                None => return false,
            }
        } else if i == parts.len() - 1 {
            return remaining.ends_with(part) && remaining.len() >= part.len();
        } else {
            match remaining.find(part) {
                Some(idx) => remaining = &remaining[idx + part.len()..],
                None => return false,
            }
        }
    }
    // Pattern ends with '*' (or all middle parts matched).
    true
}

fn any_glob_matches(patterns: &[String], value: &str) -> bool {
    patterns.iter().any(|p| glob_match(p, value))
}

/// Whether a selector list accepts the given scope attribute.
///
/// A rule that names a selector only matches requests carrying that
/// attribute; a missing attribute fails the selector. The wildcard scope
/// (list gating) passes allow-rule selectors but is handled separately for
/// deny rules in `evaluate`.
fn selector_matches(selector: &Option<Vec<String>>, attr: &Option<String>) -> bool {
    match (selector, attr) {
        (None, _) => true,
        (Some(patterns), Some(value)) => any_glob_matches(patterns, value),
        (Some(_), None) => false,
    }
}

fn rule_covers(rule: &AclRule, resource: AclResource, action: AclAction) -> bool {
    let resource_ok = rule
        .resources
        .iter()
        .any(|r| r == "*" || r == resource.as_str());
    let action_ok = rule
        .actions
        .iter()
        .any(|a| a == "*" || a == action.as_str());
    resource_ok && action_ok
}

fn rule_matches(
    rule: &AclRule,
    resource: AclResource,
    action: AclAction,
    scope: &AclScope,
) -> bool {
    if !rule_covers(rule, resource, action) {
        return false;
    }
    if scope.any {
        // List gating: a scoped allow rule proves some instance is visible.
        // Scoped deny rules do not deny the whole list (items are filtered
        // individually); blanket deny rules are handled by the caller.
        return true;
    }
    // A host-level exec target requires an exec rule that opts into `host`.
    // Sandboxed targets (scope.host == false) are unaffected by the flag.
    if resource == AclResource::Exec && scope.host && rule.host != Some(true) {
        return false;
    }
    selector_matches(&rule.namespaces, &scope.namespace)
        && selector_matches(&rule.names, &scope.name)
        && selector_matches(&rule.pools, &scope.pool)
        && selector_matches(&rule.paths, &scope.path)
        && selector_matches(&rule.action_names, &scope.action_name)
        && selector_matches(&rule.templates, &scope.template)
}

/// Default-deny evaluation over a flattened rule set.
///
/// Returns true only when at least one allow rule matches and no deny rule
/// matches. Deny rules with scope selectors only apply to concrete scopes
/// (a deny on `paths: ["secret/*"]` must not block listing unrelated keys);
/// unscoped deny rules also veto wildcard-scope (list) checks.
pub fn evaluate(
    rules: &[AclRule],
    resource: AclResource,
    action: AclAction,
    scope: &AclScope,
) -> bool {
    let mut allowed = false;
    for rule in rules {
        if rule.is_deny() {
            if !rule_covers(rule, resource, action) {
                continue;
            }
            let deny_hits = if scope.any {
                !rule.has_scope_selectors()
            } else {
                rule_matches(rule, resource, action, scope)
            };
            if deny_hits {
                return false;
            }
        } else if rule_matches(rule, resource, action, scope) {
            allowed = true;
        }
    }
    allowed
}

/// Parses and validates a policy `rules_json` document.
pub fn parse_rules(rules_json: &str) -> Result<Vec<AclRule>, String> {
    let rules: Vec<AclRule> = serde_json::from_str(rules_json)
        .map_err(|e| format!("rules must be a JSON array of rule objects: {e}"))?;

    for (i, rule) in rules.iter().enumerate() {
        if rule.resources.is_empty() {
            return Err(format!("rule {i}: `resources` must not be empty"));
        }
        if rule.actions.is_empty() {
            return Err(format!("rule {i}: `actions` must not be empty"));
        }
        for resource in &rule.resources {
            if resource != "*" && !AclResource::all().contains(&resource.as_str()) {
                return Err(format!(
                    "rule {i}: unknown resource `{resource}` (expected one of {}, or `*`)",
                    AclResource::all().join(", ")
                ));
            }
        }
        for action in &rule.actions {
            if action != "*" && action != "read" && action != "write" {
                return Err(format!(
                    "rule {i}: unknown action `{action}` (expected `read`, `write`, or `*`)"
                ));
            }
        }
        if let Some(effect) = &rule.effect {
            if effect != "allow" && effect != "deny" {
                return Err(format!(
                    "rule {i}: unknown effect `{effect}` (expected `allow` or `deny`)"
                ));
            }
        }
    }

    Ok(rules)
}

/// How a request was authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclSource {
    /// ACLs are disabled; every caller has full access.
    Disabled,
    /// A Legion ACL token was presented and resolved.
    Token,
    /// An external JWT was validated by a configured auth method.
    Jwt,
    /// No credential was presented.
    Anonymous,
}

impl AclSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            AclSource::Disabled => "disabled",
            AclSource::Token => "token",
            AclSource::Jwt => "jwt",
            AclSource::Anonymous => "anonymous",
        }
    }
}

/// The caller's resolved identity: principal, policy set, and flattened rules.
///
/// Produced once per request by `ManagerAgentService::check_acl` /
/// `authenticate`; `allows` can then be used for per-item filtering of list
/// replies without further storage lookups.
#[derive(Debug, Clone)]
pub struct AclIdentity {
    /// Display name: token name, `user:<name>`, JWT subject, or `anonymous`.
    pub principal: String,
    /// Accessor ID when a Legion ACL token was presented (safe to log).
    pub accessor_id: Option<String>,
    /// True for management tokens and when ACLs are disabled.
    pub is_management: bool,
    /// True for per-node worker tokens minted at registration. Worker
    /// identities may call worker-plane endpoints (heartbeats, health) but
    /// carry no policies for the user-facing API.
    pub is_worker: bool,
    /// Names of the policies attached to this identity.
    pub policies: Vec<String>,
    /// Flattened rules from all attached policies.
    pub rules: Vec<AclRule>,
    /// How the caller authenticated.
    pub source: AclSource,
    /// RFC3339 expiry of the credential, when it has one.
    pub expires_at: Option<String>,
    /// True when ACLs run in audit-only mode (denials logged, not enforced).
    pub audit_only: bool,
}

impl AclIdentity {
    /// Full-access identity used when ACLs are disabled.
    pub fn acl_disabled() -> Self {
        AclIdentity {
            principal: "acl-disabled".into(),
            accessor_id: None,
            is_management: true,
            is_worker: false,
            policies: Vec::new(),
            rules: Vec::new(),
            source: AclSource::Disabled,
            expires_at: None,
            audit_only: false,
        }
    }

    /// Whether this identity may perform `action` on `resource` within `scope`.
    pub fn allows(&self, resource: AclResource, action: AclAction, scope: &AclScope) -> bool {
        if self.is_management || self.audit_only {
            return true;
        }
        evaluate(&self.rules, resource, action, scope)
    }

    /// Like `allows`, but ignores audit-only mode (used for audit logging).
    pub fn would_allow(&self, resource: AclResource, action: AclAction, scope: &AclScope) -> bool {
        if self.is_management {
            return true;
        }
        evaluate(&self.rules, resource, action, scope)
    }

    /// The identity's effective rules as JSON, for self-introspection
    /// (`WhoAmI.rules_json`). Management tokens (and disabled ACLs) report a
    /// single wildcard allow; identities with no rules report `None`.
    pub fn effective_rules_json(&self) -> Option<String> {
        if self.is_management {
            return Some(r#"[{"resources":["*"],"actions":["*"]}]"#.to_string());
        }
        if self.rules.is_empty() {
            return None;
        }
        serde_json::to_string(&self.rules).ok()
    }
}

#[cfg(test)]
mod acl_rules_test {
    use super::*;

    fn rules(json: &str) -> Vec<AclRule> {
        parse_rules(json).expect("rules should parse")
    }

    #[test]
    fn glob_match_basics() {
        assert!(glob_match("web", "web"));
        assert!(!glob_match("web", "web-2"));
        assert!(glob_match("web-*", "web-2"));
        assert!(glob_match("web-*", "web-"));
        assert!(!glob_match("web-*", "web"));
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*-prod", "api-prod"));
        assert!(!glob_match("*-prod", "api-prod-2"));
        assert!(glob_match("a*b*c", "a-x-b-y-c"));
        assert!(!glob_match("a*b*c", "a-x-y-c"));
        // The CVE-2025-4922 lesson: exact patterns must not prefix-match.
        assert!(!glob_match("test-job", "test-job-2"));
    }

    #[test]
    fn default_deny_with_no_rules() {
        assert!(!evaluate(
            &[],
            AclResource::Job,
            AclAction::Read,
            &AclScope::any()
        ));
    }

    #[test]
    fn allow_rule_grants_only_listed_actions() {
        let r = rules(r#"[{"resources": ["job"], "actions": ["read"]}]"#);
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Read,
            &AclScope::any()
        ));
        assert!(!evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::any()
        ));
        assert!(!evaluate(
            &r,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::any()
        ));
    }

    #[test]
    fn wildcards_cover_all_resources_and_actions() {
        let r = rules(r#"[{"resources": ["*"], "actions": ["*"]}]"#);
        for resource in [
            AclResource::Job,
            AclResource::Node,
            AclResource::Kv,
            AclResource::State,
            AclResource::Operator,
            AclResource::Acl,
        ] {
            assert!(evaluate(&r, resource, AclAction::Read, &AclScope::none()));
            assert!(evaluate(&r, resource, AclAction::Write, &AclScope::none()));
        }
    }

    #[test]
    fn deny_overrides_allow() {
        let r = rules(
            r#"[
                {"resources": ["job"], "actions": ["*"]},
                {"resources": ["job"], "actions": ["write"], "effect": "deny"}
            ]"#,
        );
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Read,
            &AclScope::any()
        ));
        assert!(!evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::any()
        ));
    }

    #[test]
    fn name_selector_scopes_job_access() {
        let r = rules(r#"[{"resources": ["job"], "actions": ["write"], "names": ["web-*"]}]"#);
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("web-api")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("db")
        ));
        // Exact-name policies must not leak onto longer names.
        let exact = rules(r#"[{"resources": ["job"], "actions": ["write"], "names": ["web"]}]"#);
        assert!(!evaluate(
            &exact,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("web-2")
        ));
    }

    #[test]
    fn namespace_selector_matches_default_namespace() {
        let r =
            rules(r#"[{"resources": ["job"], "actions": ["read"], "namespaces": ["default"]}]"#);
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job("any")
        ));
        let other =
            rules(r#"[{"resources": ["job"], "actions": ["read"], "namespaces": ["prod"]}]"#);
        assert!(!evaluate(
            &other,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job("any")
        ));
    }

    #[test]
    fn path_selector_scopes_kv_access() {
        let r = rules(
            r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["team-a/*"]}]"#,
        );
        assert!(evaluate(
            &r,
            AclResource::Kv,
            AclAction::Write,
            &AclScope::path("team-a/config")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Kv,
            AclAction::Write,
            &AclScope::path("team-b/config")
        ));
    }

    #[test]
    fn pool_selector_scopes_node_access() {
        let r = rules(r#"[{"resources": ["node"], "actions": ["read"], "pools": ["gpu-*"]}]"#);
        assert!(evaluate(
            &r,
            AclResource::Node,
            AclAction::Read,
            &AclScope::pool("gpu-east")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Node,
            AclAction::Read,
            &AclScope::pool("default")
        ));
    }

    #[test]
    fn template_selector_scopes_template_resource() {
        let r = rules(
            r#"[{"resources": ["template"], "actions": ["read"], "templates": ["render-*"]}]"#,
        );
        assert!(evaluate(
            &r,
            AclResource::Template,
            AclAction::Read,
            &AclScope::template("render-video")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Template,
            AclAction::Read,
            &AclScope::template("deploy")
        ));
        // read does not grant write
        assert!(!evaluate(
            &r,
            AclResource::Template,
            AclAction::Write,
            &AclScope::template("render-video")
        ));
    }

    #[test]
    fn template_selector_on_job_rules_gates_submission_channel() {
        let r = rules(r#"[{"resources": ["job"], "actions": ["write"], "templates": ["render"]}]"#);
        // Submitting through the template is allowed for any rendered name...
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job_via_template("render-abc123", "render")
        ));
        // ...but a plain RunJob carries no template attribute and is denied.
        assert!(!evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("render-abc123")
        ));
        // A read scope carrying the job's provenance matches too.
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("render-abc123").with_template(Some("render".into()))
        ));
        // Composes with `names`: both selectors must match.
        let both = rules(
            r#"[{"resources": ["job"], "actions": ["write"],
                 "names": ["render-*"], "templates": ["render"]}]"#,
        );
        assert!(evaluate(
            &both,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job_via_template("render-ok", "render")
        ));
        assert!(!evaluate(
            &both,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job_via_template("web-prod", "render")
        ));
    }

    #[test]
    fn selector_on_missing_attribute_does_not_match() {
        // A rule restricted to a pool cannot grant access to requests that
        // carry no pool attribute (for example a job write).
        let r = rules(r#"[{"resources": ["*"], "actions": ["*"], "pools": ["gpu"]}]"#);
        assert!(!evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("web")
        ));
        assert!(evaluate(
            &r,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool("gpu")
        ));
    }

    #[test]
    fn scoped_allow_passes_list_gate_and_filters_items() {
        let r = rules(r#"[{"resources": ["kv"], "actions": ["read"], "paths": ["team-a/*"]}]"#);
        // Gate: caller can list (some keys are visible)...
        assert!(evaluate(
            &r,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::any()
        ));
        // ...but item filtering only exposes matching keys.
        assert!(evaluate(
            &r,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::path("team-a/x")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::path("other/x")
        ));
    }

    #[test]
    fn scoped_deny_does_not_block_list_gate_but_blanket_deny_does() {
        let scoped = rules(
            r#"[
                {"resources": ["kv"], "actions": ["read"]},
                {"resources": ["kv"], "actions": ["read"], "effect": "deny", "paths": ["secret/*"]}
            ]"#,
        );
        assert!(evaluate(
            &scoped,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::any()
        ));
        assert!(!evaluate(
            &scoped,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::path("secret/x")
        ));
        assert!(evaluate(
            &scoped,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::path("open/x")
        ));

        let blanket = rules(
            r#"[
                {"resources": ["kv"], "actions": ["read"]},
                {"resources": ["kv"], "actions": ["read"], "effect": "deny"}
            ]"#,
        );
        assert!(!evaluate(
            &blanket,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::any()
        ));
    }

    #[test]
    fn parse_rejects_unknown_fields_resources_actions_and_effects() {
        assert!(parse_rules(r#"[{"resources": ["job"], "actions": []}]"#).is_err());
        assert!(parse_rules(r#"[{"resources": [], "actions": ["read"]}]"#).is_err());
        assert!(parse_rules(r#"[{"resources": ["jobs"], "actions": ["read"]}]"#).is_err());
        assert!(parse_rules(r#"[{"resources": ["job"], "actions": ["execute"]}]"#).is_err());
        assert!(
            parse_rules(r#"[{"resources": ["job"], "actions": ["read"], "effect": "block"}]"#)
                .is_err()
        );
        assert!(
            parse_rules(r#"[{"resources": ["job"], "actions": ["read"], "typo": true}]"#).is_err()
        );
        assert!(parse_rules("not json").is_err());
        assert!(parse_rules(r#"[]"#)
            .expect("empty rule set is valid")
            .is_empty());
    }

    #[test]
    fn management_identity_allows_everything() {
        let ident = AclIdentity::acl_disabled();
        assert!(ident.allows(AclResource::Acl, AclAction::Write, &AclScope::none()));
    }

    #[test]
    fn audit_only_identity_allows_but_reports_would_deny() {
        let ident = AclIdentity {
            principal: "anonymous".into(),
            accessor_id: None,
            is_management: false,
            is_worker: false,
            policies: Vec::new(),
            rules: Vec::new(),
            source: AclSource::Anonymous,
            expires_at: None,
            audit_only: true,
        };
        assert!(ident.allows(AclResource::Job, AclAction::Write, &AclScope::any()));
        assert!(!ident.would_allow(AclResource::Job, AclAction::Write, &AclScope::any()));
    }
}
