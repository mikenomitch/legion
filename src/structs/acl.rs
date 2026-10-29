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

use crate::structs::namespace::{split_job_id, RESERVED_NAMESPACE_PREFIX};

/// The KV prefix reserved for Legion itself (app leases and state). Like the
/// reserved `legion-` namespaces, a key under it is reached only by an allow
/// rule whose `paths` pattern itself starts with `legion/` (see
/// [`reserved_glob_match`]).
pub const RESERVED_KV_PREFIX: &str = "legion/";

/// Resource types ACL rules can cover.
///
/// Deliberately a short, closed list: every gRPC endpoint maps onto exactly
/// one of these (see the endpoint table in `tests/integration/acl_endpoints.rs`).
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
    /// Boot tickets (`legion boot-ticket`, docs/edge-boot.md): `read` lists
    /// outstanding tickets, `write` mints one. Separate from `job` for the
    /// same reason `scale` is — a platform's ticket minter should be able to
    /// authorize individual starts without holding `job:write`, which means
    /// "redeploy the fleet". Scope with the job-name selector.
    Boot,
    /// Network policy objects (`legion net policy`, docs/network-handlers.md):
    /// `write` applies and deletes stored policies, `read` lists them and
    /// shows/tests a job's effective policy. Separate from `job` so a pool's
    /// or namespace's admin can set posture without cluster-wide `job:write`;
    /// scope with the `pools` / `namespaces` selectors, which match a
    /// policy's own selector.
    Net,
    /// Snapshots of instances (`legion snapshot`, docs/suspend-and-resume.md):
    /// `read` lists and fetches them and restores an instance from one,
    /// `write` captures and deletes them. Scope with the job-name and
    /// namespace selectors, like `job` rules. Separate from `job` because a
    /// memory snapshot holds everything the workload had in memory, secrets
    /// included: `snapshot:write` on a job is equivalent to reading its
    /// in-memory secrets, which `job:write` alone should not grant.
    Snapshot,
    /// Namespace objects (`legion namespace`, docs/namespaces.md): `read`
    /// fetches one, `write` creates, updates and deletes namespaces whose
    /// names match the rule's `namespaces` selector — so a team lead can be
    /// handed `payments-*` without being a cluster administrator. Separate
    /// from `operator` for the reason `scale` and `boot` are: "may create a
    /// namespace" must not imply "may reconfigure raft".
    Namespace,
    /// Apps (`legion app`, docs/apps.md): `read` lists and shows them,
    /// `write` installs, upgrades, uninstalls, enables, disables, and changes
    /// their settings and config. Cluster-scoped; scope with the `names`
    /// selector, which matches app names. Holding `app:write` is not holding
    /// the app's grant: installing, upgrading, and changing an app's code
    /// fields also need every permission the app asks for.
    App,
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
            AclResource::Boot => "boot",
            AclResource::Net => "net",
            AclResource::Snapshot => "snapshot",
            AclResource::Namespace => "namespace",
            AclResource::App => "app",
        }
    }

    pub fn all() -> &'static [&'static str] {
        &[
            "job",
            "node",
            "kv",
            "state",
            "operator",
            "acl",
            "exec",
            "action",
            "template",
            "scale",
            "boot",
            "net",
            "snapshot",
            "namespace",
            "app",
        ]
    }

    /// Resources whose scopes always carry a namespace. An allow rule naming
    /// one of these must say which namespaces it covers (see
    /// [`parse_rules`]); the rest are cluster-scoped and never carry one.
    pub fn namespaced() -> &'static [&'static str] {
        &[
            "job",
            "exec",
            "action",
            "scale",
            "boot",
            "snapshot",
            "namespace",
        ]
    }

    /// Whether this resource's scopes carry a namespace.
    pub fn is_namespaced(&self) -> bool {
        Self::namespaced().contains(&self.as_str())
    }

    /// The resource a rule names (`job`, `scale`, ...), if it is one.
    pub fn parse(name: &str) -> Option<AclResource> {
        [
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
        ]
        .into_iter()
        .find(|resource| resource.as_str() == name)
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

    /// `read` or `write`.
    pub fn parse(name: &str) -> Option<AclAction> {
        match name {
            "read" => Some(AclAction::Read),
            "write" => Some(AclAction::Write),
            _ => None,
        }
    }
}

/// One rule inside a policy document.
///
/// Serialized as JSON inside `AclPolicy.rules_json`. Example:
/// `{"resources": ["job"], "actions": ["read"], "namespaces": ["default"]}`.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AclRule {
    /// Resource types: `job`, `node`, `kv`, `state`, `operator`, `acl`,
    /// `exec`, `action`, `template`, `scale`, `boot`, `net`, `snapshot`,
    /// `namespace`, `app`, `*`.
    pub resources: Vec<String>,
    /// Actions: `read`, `write`, `*`.
    pub actions: Vec<String>,
    /// `allow` (default) or `deny`. A matching deny overrides any allow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    /// Glob selectors over namespaces. Required on allow rules that name a
    /// namespaced resource (`job`, `exec`, `action`, `scale`, `boot`,
    /// `snapshot`, `namespace`), and rejected on rules that name only
    /// cluster-scoped ones: `["*"]` is how a rule says "every namespace,
    /// including ones created later", so that reach is always written, never
    /// implied by leaving the selector out (docs/namespaces.md).
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
    /// Glob selectors over job priority bands (`critical`, `high`,
    /// `elevated`, `normal`, `low`, `background`) on `job` rules.
    ///
    /// This is how "who may claim `critical`" is expressed. It is a selector
    /// rather than a coarse `operator:write` check because `operator` means
    /// "raft, manager configs, observability" — gating bands on it would make
    /// everyone who may submit a critical job a raft administrator, and it
    /// could not express "team A may use `critical` in their namespace",
    /// which is the first thing any shared cluster asks for.
    ///
    /// Standard selector semantics apply, and they cut both ways here: a rule
    /// that names `bands` grants nothing to a request with no band attribute,
    /// and a rule that omits `bands` covers every band. So bands are open by
    /// default and become a privilege exactly where a policy says so — which
    /// is why the write path normalizes the band (absent = `normal`) onto the
    /// scope before enforcing, rather than leaving it unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bands: Option<Vec<String>>,
    /// On `exec` rules: when true, the rule also grants exec into host-level
    /// (unsandboxed) kinds. When absent/false, it matches only sandboxed
    /// kinds. Mirrors Nomad's `alloc-exec` vs `alloc-node-exec` split.
    ///
    /// On `job` rules: when true, `write` also covers jobs with a host-level
    /// task (an `exec` task, a host-network or floor-less container, a `dev`
    /// sandbox), which run on the host as root or close to it
    /// (worker-plane-auth plan §6.16). Reads are unaffected.
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
            || self.bands.is_some()
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
    /// Priority band of the job being submitted, on `job` write scopes.
    /// Always populated (defaulted to `normal`) on the submit path, never on
    /// reads — a `bands` rule is about what you may *claim*, not about what
    /// you may look at.
    pub band: Option<String>,
    /// True when the exec target is a host-level (unsandboxed) kind, or a
    /// submitted job has a host-level task. Only an `exec` (or `job`) rule
    /// with `host: true` matches such a scope.
    pub host: bool,
}

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

    /// Scope for a job, from its canonical id (`web` in `default`,
    /// `payments-prod/web` elsewhere; see `structs::namespace`). The scope
    /// carries the namespace and the *bare* name as separate attributes, so
    /// a `names: ["web-*"]` rule means the same thing in every namespace.
    pub fn job(job_id: &str) -> Self {
        let (namespace, name) = split_job_id(job_id);
        AclScope {
            namespace: Some(namespace.to_string()),
            name: Some(name.to_string()),
            ..Default::default()
        }
    }

    /// Scope for a job-kind object that no namespace owns (a volume created
    /// with no job). It carries the namespace `*` — not a legal namespace
    /// name, so it never collides with one — which only a rule whose
    /// `namespaces` selector reaches every namespace (`["*"]`) matches: such
    /// an object takes cluster-wide authority, exactly as before namespaces.
    pub fn unowned() -> Self {
        AclScope {
            namespace: Some("*".to_string()),
            ..Default::default()
        }
    }

    /// Scope for a namespace object (namespace CRUD endpoints).
    pub fn namespace(name: &str) -> Self {
        AclScope {
            namespace: Some(name.to_string()),
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

    /// Scope for an exec session into a job (by canonical id). `host` marks
    /// host-level kinds.
    pub fn exec(job_id: &str, host: bool) -> Self {
        AclScope {
            host,
            ..Self::job(job_id)
        }
    }

    /// Scope for running an action on a job (by canonical id).
    pub fn action(job_id: &str, action_name: &str) -> Self {
        AclScope {
            action_name: Some(action_name.to_string()),
            ..Self::job(job_id)
        }
    }

    /// Scope for an app (`legion app`): its name, matched by the `names`
    /// selector. Apps are cluster-scoped, so the scope carries no namespace.
    pub fn app(name: &str) -> Self {
        AclScope {
            name: Some(name.to_string()),
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
    /// RenderJobTemplate): the rendered job (by canonical id, so it carries
    /// the target namespace) plus the template it came through, so the
    /// `namespaces`, `names` and `templates` selectors all apply.
    pub fn job_via_template(job_id: &str, template: &str) -> Self {
        AclScope {
            template: Some(template.to_string()),
            ..Self::job(job_id)
        }
    }

    /// Attaches a template attribute (a job's stored provenance) to an
    /// existing scope. Used on read/lifecycle endpoints so `templates`-scoped
    /// rules cover the jobs their templates created.
    pub fn with_template(mut self, template: Option<String>) -> Self {
        self.template = template;
        self
    }

    /// Attaches the priority band a submission is claiming.
    ///
    /// Called on **every** job write path with the band already defaulted, so
    /// a `bands` rule matches consistently. Leaving it unset for an
    /// un-banded submit would mean band-scoped policies silently granted
    /// nothing — the exact failure the selector docs warn about.
    pub fn with_band(mut self, band: crate::structs::Band) -> Self {
        self.band = Some(band.as_str().to_string());
        self
    }

    /// Marks a job write as host-level: only `job` rules with `host = true`
    /// match it.
    pub fn with_host(mut self, host: bool) -> Self {
        self.host = host;
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

/// [`glob_match`] with the reserved-prefix rule: a value that starts with
/// `reserved` is matched only by a pattern that itself starts with
/// `reserved`, the way a shell's `*` skips dotfiles. So `*` matches
/// `payments-prod` but not `legion-system`, while `legion-*` and
/// `legion-system` match it. Every other value is plain [`glob_match`].
///
/// The rule only ever narrows what a pattern matches. It keeps the broad
/// grants operators write every day (`namespaces = ["*"]`,
/// `paths = ["*"]`) from reaching what Legion runs for itself; reaching it
/// takes a pattern that names the prefix (docs/namespaces.md, "Reserved
/// namespaces").
pub fn reserved_glob_match(pattern: &str, value: &str, reserved: &str) -> bool {
    if value.starts_with(reserved) && !pattern.starts_with(reserved) {
        return false;
    }
    glob_match(pattern, value)
}

/// [`reserved_glob_match`] for namespace names: `legion-*` is reserved.
/// Used by ACL `namespaces` selectors, namespace-policy selectors, and the
/// node-side `namespaces` restriction alike.
pub fn namespace_glob_match(pattern: &str, namespace: &str) -> bool {
    reserved_glob_match(pattern, namespace, RESERVED_NAMESPACE_PREFIX)
}

/// [`reserved_glob_match`] for KV keys: `legion/` is reserved.
pub fn kv_path_glob_match(pattern: &str, path: &str) -> bool {
    reserved_glob_match(pattern, path, RESERVED_KV_PREFIX)
}

/// The node-pool prefix reserved for Legion's own workers: a manager's
/// system worker runs in pool `legion-system`. The same prefix as reserved
/// namespaces.
pub const RESERVED_POOL_PREFIX: &str = RESERVED_NAMESPACE_PREFIX;

/// [`reserved_glob_match`] for node pools: `legion-*` is reserved, so a
/// `pools = ["*"]` rule (or a `node` rule with no `pools`) does not reach
/// the managers' system workers; `pools = ["legion-*"]` does.
pub fn pool_glob_match(pattern: &str, pool: &str) -> bool {
    reserved_glob_match(pattern, pool, RESERVED_POOL_PREFIX)
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
    if matches!(resource, AclResource::Exec | AclResource::Job)
        && scope.host
        && rule.host != Some(true)
    {
        return false;
    }
    scope
        .namespace
        .as_deref()
        .map_or(rule.namespaces.is_none(), |namespace| {
            namespace_selector_matches(rule, namespace)
        })
        && selector_matches(&rule.names, &scope.name)
        && pools_selector_matches(rule, &scope.pool)
        && paths_selector_matches(rule, &scope.path)
        && selector_matches(&rule.action_names, &scope.action_name)
        && selector_matches(&rule.templates, &scope.template)
        && selector_matches(&rule.bands, &scope.band)
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

/// Whether an allow rule grants anything inside `namespace` (see
/// [`AclIdentity::reaches_namespace`]). Deny rules reach nothing.
pub fn rule_reaches_namespace(rule: &AclRule, namespace: &str) -> bool {
    if rule.is_deny() {
        return false;
    }
    let namespaced = rule
        .resources
        .iter()
        .any(|resource| resource == "*" || AclResource::namespaced().contains(&resource.as_str()));
    namespaced && namespace_selector_matches(rule, namespace)
}

/// Whether a rule's `namespaces` selector matches `namespace`.
///
/// An allow rule's patterns follow the reserved-prefix rule
/// ([`namespace_glob_match`]): `*` does not reach `legion-system`, only a
/// pattern starting `legion-` does. A deny rule's patterns are plain globs,
/// because the reserved rule exists to keep grants from reaching too far,
/// and a deny that reached less would grant more.
///
/// Without the selector: an allow rule naming namespaced resources
/// explicitly — one stored before namespaces existed, since validation now
/// refuses to write one — reaches `default` only: that is what it meant
/// when every job lived there, and it must not silently widen to every
/// namespace created since. A `*` resource rule reaches what `*` reaches
/// (every namespace but the reserved ones), and a deny rule every namespace.
fn namespace_selector_matches(rule: &AclRule, namespace: &str) -> bool {
    match &rule.namespaces {
        Some(patterns) if rule.is_deny() => any_glob_matches(patterns, namespace),
        Some(patterns) => patterns
            .iter()
            .any(|pattern| namespace_glob_match(pattern, namespace)),
        None if rule.is_deny() => true,
        None if rule.resources.iter().any(|resource| resource == "*") => {
            namespace_glob_match("*", namespace)
        }
        None => namespace == crate::structs::namespace::DEFAULT_NAMESPACE,
    }
}

/// Whether a rule's `paths` selector accepts a KV key. Allow rules follow
/// the reserved-prefix rule ([`kv_path_glob_match`]): a key under `legion/`
/// is reached only by a pattern starting `legion/`, and an allow rule with
/// no `paths` reaches what `*` reaches. Deny rules are plain globs, for the
/// reason [`namespace_selector_matches`] gives. A scope without a key (every
/// resource but `kv`) is decided by the generic selector rule.
fn paths_selector_matches(rule: &AclRule, path: &Option<String>) -> bool {
    match (&rule.paths, path.as_deref()) {
        (_, None) => selector_matches(&rule.paths, path),
        _ if rule.is_deny() => selector_matches(&rule.paths, path),
        (None, Some(path)) => kv_path_glob_match("*", path),
        (Some(patterns), Some(path)) => patterns
            .iter()
            .any(|pattern| kv_path_glob_match(pattern, path)),
    }
}

/// Whether a rule's `pools` selector accepts a node pool. Allow rules follow
/// the reserved-prefix rule ([`pool_glob_match`]): a `legion-*` pool (the
/// managers' system workers) is reached only by a pattern starting
/// `legion-`, and an allow rule with no `pools` reaches what `*` reaches.
/// Deny rules are plain globs, for the reason [`namespace_selector_matches`]
/// gives. A scope without a pool is decided by the generic selector rule.
fn pools_selector_matches(rule: &AclRule, pool: &Option<String>) -> bool {
    match (&rule.pools, pool.as_deref()) {
        (_, None) => selector_matches(&rule.pools, pool),
        _ if rule.is_deny() => selector_matches(&rule.pools, pool),
        (None, Some(pool)) => pool_glob_match("*", pool),
        (Some(patterns), Some(pool)) => patterns
            .iter()
            .any(|pattern| pool_glob_match(pattern, pool)),
    }
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
        validate_namespace_selector(i, rule)?;
    }

    Ok(rules)
}

/// The two namespace rules `parse_rules` enforces (docs/namespaces.md,
/// "Writing policies"):
///
/// - An **allow** rule that names a namespaced resource must carry
///   `namespaces`. "Every namespace, including future ones" is `["*"]`, so
///   that reach is always something an admin wrote. Deny rules may omit it
///   (deny everywhere): leaving a selector off a deny can only take access
///   away. A `*` resource rule may omit it too — `resources = ["*"]` is
///   already the explicit "everything".
/// - A rule that names only cluster-scoped resources must not carry
///   `namespaces`: their scopes never carry one, so the rule could never
///   match. Mixing the two kinds in one rule is rejected for the same
///   reason, with a hint to split it.
fn validate_namespace_selector(index: usize, rule: &AclRule) -> Result<(), String> {
    if rule.resources.iter().any(|resource| resource == "*") {
        return Ok(());
    }
    let namespaced: Vec<&str> = rule
        .resources
        .iter()
        .map(String::as_str)
        .filter(|resource| AclResource::namespaced().contains(resource))
        .collect();
    let cluster: Vec<&str> = rule
        .resources
        .iter()
        .map(String::as_str)
        .filter(|resource| !AclResource::namespaced().contains(resource))
        .collect();
    if !namespaced.is_empty() && !cluster.is_empty() {
        return Err(format!(
            "rule {index} mixes namespaced resources ({}) with cluster-scoped ones ({});              split it into two rules, one with a `namespaces` selector and one without",
            namespaced.join(", "),
            cluster.join(", ")
        ));
    }
    match (&rule.namespaces, namespaced.is_empty()) {
        (Some(_), true) => Err(format!(
            "rule {index} ({}) has a `namespaces` selector, but {} cluster-scoped, so the rule              could never match; remove the selector",
            cluster.join(", "),
            if cluster.len() == 1 { "it is" } else { "they are" }
        )),
        (None, false) if !rule.is_deny() => Err(format!(
            "rule {index} ({}) must say which namespaces it covers: add              `namespaces = [\"default\"]` for one namespace, or `[\"*\"]` for every namespace,              including ones created later",
            namespaced.join(", ")
        )),
        (Some(patterns), false) if patterns.is_empty() => Err(format!(
            "rule {index}: `namespaces` must not be empty (use [\"*\"] for every namespace)"
        )),
        _ => Ok(()),
    }
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

    /// Whether this identity holds any allow in namespace `namespace`: the
    /// rule `ListNamespaces` and `WhoAmI.readable_namespaces` filter by
    /// (docs/namespaces.md, "Seeing namespaces"). True for management
    /// identities. A rule reaches a namespace when it names a namespaced
    /// resource (or `*`) and its `namespaces` selector matches — other
    /// selectors are ignored, because a `names: ["web"]` grant still means
    /// the caller has something to do there.
    pub fn reaches_namespace(&self, namespace: &str) -> bool {
        if self.is_management || self.audit_only {
            return true;
        }
        self.rules
            .iter()
            .any(|rule| rule_reaches_namespace(rule, namespace))
    }

    /// The namespace selectors under which this identity's allow rules grant
    /// `resource:action`, sorted: where it *can* act, for a denial's hint.
    /// Read off the caller's own rules, so it names nothing the caller could
    /// not read from its own policy.
    pub fn namespaces_granting(&self, resource: AclResource, action: AclAction) -> Vec<String> {
        let mut globs: Vec<String> = self
            .rules
            .iter()
            .filter(|rule| !rule.is_deny() && rule_covers(rule, resource, action))
            .filter_map(|rule| rule.namespaces.clone())
            .flatten()
            .collect();
        globs.sort();
        globs.dedup();
        globs
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

    /// `app` is cluster-scoped with a `names` selector (docs/apps.md):
    /// `app:read` on `auto*` sees the autoscaler and nothing else, a rule
    /// may not give it namespaces, and `app:write` does not imply `read`.
    #[test]
    fn app_rules_scope_by_app_name() {
        let held = rules(r#"[{"resources": ["app"], "actions": ["read"], "names": ["auto*"]}]"#);
        assert!(evaluate(
            &held,
            AclResource::App,
            AclAction::Read,
            &AclScope::app("autoscaler")
        ));
        assert!(!evaluate(
            &held,
            AclResource::App,
            AclAction::Read,
            &AclScope::app("gitops")
        ));
        assert!(!evaluate(
            &held,
            AclResource::App,
            AclAction::Write,
            &AclScope::app("autoscaler")
        ));
        assert!(evaluate(
            &held,
            AclResource::App,
            AclAction::Read,
            &AclScope::any()
        ));
        assert!(!AclResource::App.is_namespaced());
        let err =
            parse_rules(r#"[{"resources": ["app"], "actions": ["read"], "namespaces": ["*"]}]"#)
                .expect_err("app is cluster-scoped");
        assert!(err.contains("cluster-scoped"), "{err}");
        // A namespaced `*` rule reaches no cluster-scoped resource.
        let namespaced = rules(r#"[{"resources": ["*"], "actions": ["*"], "namespaces": ["*"]}]"#);
        assert!(!evaluate(
            &namespaced,
            AclResource::App,
            AclAction::Read,
            &AclScope::app("x")
        ));
        let everything = rules(r#"[{"resources": ["*"], "actions": ["*"]}]"#);
        assert!(evaluate(
            &everything,
            AclResource::App,
            AclAction::Write,
            &AclScope::app("x")
        ));
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

    /// `*` skips the reserved prefix the way a shell's `*` skips dotfiles;
    /// a pattern that names the prefix reaches it.
    #[test]
    fn reserved_prefix_is_matched_only_by_patterns_that_name_it() {
        assert!(namespace_glob_match("*", "payments-prod"));
        assert!(!namespace_glob_match("*", "legion-system"));
        assert!(!namespace_glob_match("l*", "legion-system"));
        assert!(!namespace_glob_match("legion*", "legion-system"));
        assert!(namespace_glob_match("legion-*", "legion-system"));
        assert!(namespace_glob_match("legion-system", "legion-system"));
        assert!(!namespace_glob_match("legion-*", "payments"));
        // Only the prefix is reserved, not every name that starts alike.
        assert!(namespace_glob_match("*", "legionnaire"));

        assert!(kv_path_glob_match("*", "team-a/config"));
        assert!(!kv_path_glob_match("*", "legion/apps/autoscaler/lease"));
        assert!(!kv_path_glob_match(
            "legion*",
            "legion/apps/autoscaler/lease"
        ));
        assert!(kv_path_glob_match(
            "legion/*",
            "legion/apps/autoscaler/lease"
        ));
        assert!(kv_path_glob_match(
            "legion/apps/autoscaler/*",
            "legion/apps/autoscaler/lease"
        ));
        assert!(
            kv_path_glob_match("*", "legion"),
            "the bare word is no prefix"
        );
    }

    /// A tenant admin's `namespaces = ["*"]` reaches every tenant namespace
    /// but not `legion-system`, for every namespaced resource; naming the
    /// prefix reaches it, and management tokens bypass rules as always.
    #[test]
    fn star_namespace_rules_do_not_reach_reserved_namespaces() {
        let tenant_admin =
            rules(r#"[{"resources": ["*"], "actions": ["*"], "namespaces": ["*"]}]"#);
        for resource in [
            AclResource::Job,
            AclResource::Exec,
            AclResource::Action,
            AclResource::Scale,
            AclResource::Snapshot,
        ] {
            assert!(evaluate(
                &tenant_admin,
                resource,
                AclAction::Write,
                &AclScope::job("payments/web")
            ));
            assert!(
                !evaluate(
                    &tenant_admin,
                    resource,
                    AclAction::Write,
                    &AclScope::job("legion-system/autoscaler")
                ),
                "{resource:?}"
            );
        }
        assert!(!evaluate(
            &tenant_admin,
            AclResource::Exec,
            AclAction::Write,
            &AclScope::exec("legion-system/autoscaler", false)
        ));
        let reaches: Vec<AclRule> = rules(
            r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*", "legion-*"]}]"#,
        );
        assert!(evaluate(
            &reaches,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("legion-system/autoscaler")
        ));
        // A `*` resource rule with no selector reaches what `*` reaches.
        let everything = rules(r#"[{"resources": ["*"], "actions": ["*"]}]"#);
        assert!(evaluate(
            &everything,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("payments/web")
        ));
        assert!(!evaluate(
            &everything,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("legion-system/autoscaler")
        ));
        let identity = AclIdentity {
            rules: tenant_admin.clone(),
            is_management: false,
            ..AclIdentity::acl_disabled()
        };
        assert!(!identity.reaches_namespace("legion-system"));
        assert!(identity.reaches_namespace("payments"));
        let management = AclIdentity::acl_disabled();
        assert!(management.allows(
            AclResource::Exec,
            AclAction::Write,
            &AclScope::exec("legion-system/autoscaler", true)
        ));
    }

    /// `paths = ["*"]`, and a KV rule with no `paths`, stop short of
    /// `legion/`; a pattern starting `legion/` reaches it.
    #[test]
    fn star_kv_rules_do_not_reach_the_reserved_prefix() {
        for broad in [
            r#"[{"resources": ["kv"], "actions": ["read", "write"], "paths": ["*"]}]"#,
            r#"[{"resources": ["kv"], "actions": ["read", "write"]}]"#,
        ] {
            let r = rules(broad);
            assert!(evaluate(
                &r,
                AclResource::Kv,
                AclAction::Write,
                &AclScope::path("team-a/x")
            ));
            assert!(
                !evaluate(
                    &r,
                    AclResource::Kv,
                    AclAction::Write,
                    &AclScope::path("legion/apps/autoscaler/lease")
                ),
                "{broad}"
            );
            // The list gate still passes; items under legion/ filter out.
            assert!(evaluate(
                &r,
                AclResource::Kv,
                AclAction::Read,
                &AclScope::any()
            ));
        }
        let named =
            rules(r#"[{"resources": ["kv"], "actions": ["write"], "paths": ["legion/apps/*"]}]"#);
        assert!(evaluate(
            &named,
            AclResource::Kv,
            AclAction::Write,
            &AclScope::path("legion/apps/autoscaler/lease")
        ));
    }

    /// `node:write` with `pools = ["*"]`, or with no `pools`, cannot cordon,
    /// drain, or mint a join token for the managers' system workers (pool
    /// `legion-system`); a pattern naming the prefix can, a deny on `*`
    /// still denies there, and management tokens bypass rules as always.
    #[test]
    fn star_pool_rules_do_not_reach_reserved_pools() {
        assert!(pool_glob_match("*", "default"));
        assert!(pool_glob_match("gpu-*", "gpu-a100"));
        assert!(!pool_glob_match("*", "legion-system"));
        assert!(!pool_glob_match("legion*", "legion-system"));
        assert!(pool_glob_match("legion-*", "legion-system"));
        assert!(pool_glob_match("legion-system", "legion-system"));
        // Only the prefix is reserved.
        assert!(pool_glob_match("*", "legionnaire"));

        for broad in [
            r#"[{"resources": ["node"], "actions": ["read", "write"], "pools": ["*"]}]"#,
            r#"[{"resources": ["node"], "actions": ["read", "write"]}]"#,
            r#"[{"resources": ["*"], "actions": ["*"]}]"#,
        ] {
            let r = rules(broad);
            assert!(evaluate(
                &r,
                AclResource::Node,
                AclAction::Write,
                &AclScope::pool("default")
            ));
            for action in [AclAction::Read, AclAction::Write] {
                assert!(
                    !evaluate(
                        &r,
                        AclResource::Node,
                        action,
                        &AclScope::pool("legion-system")
                    ),
                    "{broad} {action:?}"
                );
            }
            // The list gate still passes; system workers filter out.
            assert!(evaluate(
                &r,
                AclResource::Node,
                AclAction::Read,
                &AclScope::any()
            ));
            // A scope with no pool is unaffected: a rule without the
            // selector matches it, as before.
            if !broad.contains("pools") {
                assert!(evaluate(
                    &r,
                    AclResource::Node,
                    AclAction::Write,
                    &AclScope::none()
                ));
            }
        }
        let named =
            rules(r#"[{"resources": ["node"], "actions": ["write"], "pools": ["*", "legion-*"]}]"#);
        assert!(evaluate(
            &named,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool("legion-system")
        ));
        let denied = rules(
            r#"[
                {"resources": ["node"], "actions": ["write"], "pools": ["legion-*"]},
                {"resources": ["node"], "actions": ["write"], "pools": ["*"], "effect": "deny"}
            ]"#,
        );
        assert!(!evaluate(
            &denied,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool("legion-system")
        ));
        let management = AclIdentity::acl_disabled();
        assert!(management.allows(
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool("legion-system")
        ));
    }

    /// The reserved rule only narrows: a deny's patterns are plain globs, so
    /// a deny on `*` still denies inside the reserved space.
    #[test]
    fn deny_rules_keep_plain_globs_over_reserved_names() {
        let r = rules(
            r#"[
                {"resources": ["job"], "actions": ["read"], "namespaces": ["legion-*"]},
                {"resources": ["job"], "actions": ["read"], "namespaces": ["*"], "effect": "deny"},
                {"resources": ["kv"], "actions": ["read"], "paths": ["legion/*"]},
                {"resources": ["kv"], "actions": ["read"], "paths": ["*"], "effect": "deny"}
            ]"#,
        );
        assert!(!evaluate(
            &r,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job("legion-system/autoscaler")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::path("legion/apps/x")
        ));
    }

    /// A stored rule written before namespaces (no `namespaces`, which
    /// validation would now refuse) keeps its old meaning, `default` only;
    /// `*`-resource and deny rules without the selector reach everywhere.
    #[test]
    fn legacy_rules_without_namespaces_reach_default_only() {
        let legacy: Vec<AclRule> = serde_json::from_str(
            r#"[{"resources": ["job"], "actions": ["write"], "names": ["web*"]}]"#,
        )
        .unwrap();
        let write = |rules: &[AclRule], id: &str| {
            evaluate(
                rules,
                AclResource::Job,
                AclAction::Write,
                &AclScope::job(id),
            )
        };
        assert!(write(&legacy, "web"));
        assert!(
            !write(&legacy, "payments/web"),
            "never widens to a new namespace"
        );
        assert!(rule_reaches_namespace(&legacy[0], "default"));
        assert!(!rule_reaches_namespace(&legacy[0], "payments"));

        let everything: Vec<AclRule> =
            serde_json::from_str(r#"[{"resources": ["*"], "actions": ["*"]}]"#).unwrap();
        assert!(write(&everything, "payments/web"));

        let mut denied = rules(
            r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*"]},
                {"resources": ["job"], "actions": ["write"], "effect": "deny", "names": ["web"]}]"#,
        );
        assert!(
            !write(&denied, "payments/web"),
            "a selector-less deny denies everywhere"
        );
        denied.truncate(1);
        assert!(write(&denied, "payments/web"));
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
        let r = rules(r#"[{"resources": ["job"], "actions": ["read"], "namespaces": ["*"]}]"#);
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
                {"resources": ["job"], "actions": ["*"], "namespaces": ["*"]},
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
        let r = rules(
            r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*"], "names": ["web-*"]}]"#,
        );
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
        let exact = rules(
            r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*"], "names": ["web"]}]"#,
        );
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
    fn job_scopes_split_canonical_ids_into_namespace_and_bare_name() {
        let scope = AclScope::job("payments-prod/web");
        assert_eq!(scope.namespace.as_deref(), Some("payments-prod"));
        assert_eq!(scope.name.as_deref(), Some("web"));
        let default = AclScope::job("web");
        assert_eq!(default.namespace.as_deref(), Some("default"));
        assert_eq!(default.name.as_deref(), Some("web"));
        let exec = AclScope::exec("prod/api", true);
        assert_eq!(exec.namespace.as_deref(), Some("prod"));
        assert!(exec.host);
        let action = AclScope::action("prod/api", "flush");
        assert_eq!(action.name.as_deref(), Some("api"));
        assert_eq!(action.action_name.as_deref(), Some("flush"));
        let via = AclScope::job_via_template("prod/render-1", "render");
        assert_eq!(via.namespace.as_deref(), Some("prod"));
        assert_eq!(via.template.as_deref(), Some("render"));
    }

    #[test]
    fn namespace_selector_confines_a_rule_to_its_namespaces() {
        let r = rules(
            r#"[{"resources": ["job", "exec"], "actions": ["read", "write"],
                 "namespaces": ["payments-*"]}]"#,
        );
        for (job, allowed) in [
            ("payments-prod/web", true),
            ("payments-dev/web", true),
            ("checkout-prod/web", false),
            ("web", false),
        ] {
            assert_eq!(
                evaluate(&r, AclResource::Job, AclAction::Write, &AclScope::job(job)),
                allowed,
                "{job}"
            );
            assert_eq!(
                evaluate(
                    &r,
                    AclResource::Exec,
                    AclAction::Write,
                    &AclScope::exec(job, false)
                ),
                allowed,
                "{job}"
            );
        }
        // `names` means the bare name in every namespace.
        let named = rules(
            r#"[{"resources": ["job"], "actions": ["read"], "namespaces": ["*"],
                 "names": ["web"]}]"#,
        );
        assert!(evaluate(
            &named,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job("prod/web")
        ));
        assert!(!evaluate(
            &named,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job("prod/api")
        ));
    }

    #[test]
    fn a_namespaced_star_rule_never_reaches_cluster_scoped_resources() {
        // A team admin's `*` rule, scoped to its namespaces, must not grant
        // nodes, ACLs or KV: their scopes carry no namespace.
        let r = rules(r#"[{"resources": ["*"], "actions": ["*"], "namespaces": ["team-*"]}]"#);
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Write,
            &AclScope::job("team-a/web")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Acl,
            AclAction::Write,
            &AclScope::none()
        ));
        assert!(!evaluate(
            &r,
            AclResource::Node,
            AclAction::Write,
            &AclScope::pool("default")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Kv,
            AclAction::Read,
            &AclScope::path("team-a/x")
        ));
    }

    #[test]
    fn unowned_objects_need_a_rule_that_reaches_every_namespace() {
        let star = rules(r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*"]}]"#);
        assert!(evaluate(
            &star,
            AclResource::Job,
            AclAction::Write,
            &AclScope::unowned()
        ));
        let team =
            rules(r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["team-*"]}]"#);
        assert!(!evaluate(
            &team,
            AclResource::Job,
            AclAction::Write,
            &AclScope::unowned()
        ));
        let named = rules(
            r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*"], "names": ["acl-*"]}]"#,
        );
        assert!(!evaluate(
            &named,
            AclResource::Job,
            AclAction::Write,
            &AclScope::unowned()
        ));
    }

    #[test]
    fn deny_can_carve_namespaces_out_of_star() {
        let r = rules(
            r#"[
                {"resources": ["job"], "actions": ["read"], "namespaces": ["*"]},
                {"resources": ["job"], "actions": ["read"], "namespaces": ["legion-*"],
                 "effect": "deny"}
            ]"#,
        );
        assert!(evaluate(
            &r,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job("prod/web")
        ));
        assert!(!evaluate(
            &r,
            AclResource::Job,
            AclAction::Read,
            &AclScope::job("legion-system/agent")
        ));
    }

    #[test]
    fn allow_rules_on_namespaced_resources_must_name_namespaces() {
        let missing = parse_rules(r#"[{"resources": ["job"], "actions": ["write"]}]"#)
            .expect_err("a job allow rule without namespaces is rejected");
        assert!(missing.contains("must say which namespaces"), "{missing}");
        assert!(missing.contains(r#"["*"]"#), "{missing}");
        for resource in AclResource::namespaced() {
            let json = format!(r#"[{{"resources": ["{resource}"], "actions": ["read"]}}]"#);
            assert!(parse_rules(&json).is_err(), "{resource}");
        }
        // Deny rules and `*` resource rules may omit it.
        assert!(
            parse_rules(r#"[{"resources": ["job"], "actions": ["write"], "effect": "deny"}]"#)
                .is_ok()
        );
        assert!(parse_rules(r#"[{"resources": ["*"], "actions": ["*"]}]"#).is_ok());
        assert!(
            parse_rules(r#"[{"resources": ["job"], "actions": ["read"], "namespaces": []}]"#)
                .is_err()
        );
    }

    #[test]
    fn cluster_scoped_rules_reject_namespaces_and_mixed_rules_are_split() {
        for resource in ["node", "kv", "state", "operator", "acl", "template", "net"] {
            let json = format!(
                r#"[{{"resources": ["{resource}"], "actions": ["read"], "namespaces": ["*"]}}]"#
            );
            let err = parse_rules(&json).expect_err(resource);
            assert!(err.contains("could never match"), "{resource}: {err}");
            let plain = format!(r#"[{{"resources": ["{resource}"], "actions": ["read"]}}]"#);
            assert!(parse_rules(&plain).is_ok(), "{resource}");
        }
        let mixed = parse_rules(
            r#"[{"resources": ["job", "node"], "actions": ["read"], "namespaces": ["*"]}]"#,
        )
        .expect_err("mixed rule");
        assert!(mixed.contains("split it into two rules"), "{mixed}");
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
        let r = rules(
            r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*"], "templates": ["render"]}]"#,
        );
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
            r#"[{"resources": ["job"], "actions": ["write"], "namespaces": ["*"],
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
        assert!(
            parse_rules(r#"[{"resources": ["job"], "namespaces": ["*"], "actions": []}]"#).is_err()
        );
        assert!(parse_rules(r#"[{"resources": [], "actions": ["read"]}]"#).is_err());
        assert!(parse_rules(r#"[{"resources": ["jobs"], "actions": ["read"]}]"#).is_err());
        assert!(parse_rules(
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["execute"]}]"#
        )
        .is_err());
        assert!(
            parse_rules(r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"], "effect": "block"}]"#)
                .is_err()
        );
        assert!(parse_rules(
            r#"[{"resources": ["job"], "namespaces": ["*"], "actions": ["read"], "typo": true}]"#
        )
        .is_err());
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
