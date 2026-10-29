//! Namespace policies (docs/namespaces.md, "Namespace policies").
//!
//! A namespace policy is a cluster object that selects namespaces by glob
//! and governs what jobs in them may do: an aggregate quota per priority
//! band, a job count, a namespace count, allowed and default node pools,
//! allowed task runtimes, and allowed host volumes. It lives apart from the
//! namespace object on purpose: whoever may write a namespace must not be
//! able to raise its own limits, and a quota on one namespace is escaped by
//! creating a sibling. Writes need `acl:write`.
//!
//! Every policy matching a namespace applies. Quotas and counts are checked
//! independently, each against its own aggregate; allowlists intersect, so a
//! second policy can only narrow. There is no precedence to learn.
//!
//! This module holds the object, its validation, and the checks that need
//! only the jobspec ([`admit_job`]). The checks that need cluster state —
//! the quota, `maxJobs` and `maxNamespaces` — live in the store, where they
//! run inside the replicated write they guard.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::structs::acl::{glob_match, namespace_glob_match};
use crate::structs::namespace::{job_namespace, validate_namespace_name};
use crate::structs::{Band, JobConfig};

/// The longest policy name.
pub const MAX_POLICY_NAME_LEN: usize = 128;

/// One namespace policy object, as `legion namespace policy apply -f` reads
/// it (Nickel or JSON).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NamespacePolicy {
    /// Unique name: letters, digits, `-`, `_` and `.`.
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// The namespaces this policy governs: names or `*`-globs
    /// (`payments`, `payments-*`). Required and non-empty.
    pub namespaces: Vec<String>,
    /// Aggregate reservations across every matching namespace, per priority
    /// band. A band not listed is uncapped.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub quota: BTreeMap<String, QuotaLimit>,
    /// Jobs (any state) across every matching namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_jobs: Option<u64>,
    /// Namespaces matching the policy. Caps delegated creation: a team that
    /// may create `payments-*` creates at most this many.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_namespaces: Option<u64>,
    /// Which node pools a task may land in, and the pool a task that names
    /// none lands in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pools: Option<PoolRule>,
    /// Allowed task runtimes (`container`, `sandbox`, ...), as names or
    /// globs. Unset allows every runtime; an empty list allows none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtimes: Option<Vec<String>>,
    /// Allowed host volumes (`driver = "host"`, by `params.name`), as names
    /// or globs. Unset allows every host volume; an empty list allows none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_volumes: Option<Vec<String>>,
    /// Autoscaling defaults for every task in the governed namespaces
    /// (docs/autoscaling.md, "Overriding the defaults"): the jobspec
    /// `autoscale` record without `checks`, overriding the cluster's
    /// defaults field by field. Two governing policies that set one field
    /// differently leave it unresolvable for tasks that do not set it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autoscale: Option<crate::autoscaler::AutoscaleRecord>,
}

/// One band's cap. Each dimension is optional; an unset one is uncapped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuotaLimit {
    /// CPU in MHz, the unit of `resources.cpu`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<f64>,
    /// Memory in MiB, the unit of `resources.memory`. Also accepts a size
    /// string (`"512Gi"`, `"64Mi"`), stored as MiB.
    #[serde(
        default,
        deserialize_with = "memory_mib",
        skip_serializing_if = "Option::is_none"
    )]
    pub memory: Option<f64>,
    /// Exclusive cores (`resources.cores`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cores: Option<f64>,
}

/// Pool rules: every task's pool must match `allowed`; a task whose permit
/// names no pool gets `default` instead of the implicit `default` pool.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PoolRule {
    /// Pool names or globs. Unset allows every pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed: Option<Vec<String>>,
    /// The pool a task without a `pool` permit lands in, or a list of pools
    /// it may land in, any of them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<PoolDefault>,
}

/// `pools.default`: one pool (`"gpu"`), or several (`["legion-system",
/// "default"]`), any of which a task that names no pool may run in. Several
/// render a `pool matches ^(a|b)$` permit, which is what lets the reserved
/// `legion-system` namespace run on the managers' system workers and on a
/// `--run-worker` manager's worker alike.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PoolDefault {
    One(String),
    Many(Vec<String>),
}

impl PoolDefault {
    /// The pools, in order.
    pub fn pools(&self) -> Vec<&str> {
        match self {
            PoolDefault::One(pool) => vec![pool.as_str()],
            PoolDefault::Many(pools) => pools.iter().map(String::as_str).collect(),
        }
    }

    /// `gpu`, or `legion-system, default`.
    pub fn label(&self) -> String {
        self.pools().join(", ")
    }

    /// The same pools, whatever their order.
    fn same_pools(&self, other: &PoolDefault) -> bool {
        let mut mine = self.pools();
        let mut theirs = other.pools();
        mine.sort_unstable();
        mine.dedup();
        theirs.sort_unstable();
        theirs.dedup();
        mine == theirs
    }

    /// `permit` with this default's `pool` rule added.
    pub fn apply_to(&self, permit: &Value) -> Value {
        match self.pools().as_slice() {
            [one] => crate::constraints::with_permit_pool(permit, one),
            pools => crate::constraints::with_permit_pools(permit, pools),
        }
    }
}

fn memory_mib<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<f64>, D::Error> {
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_f64()
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("memory must be a number of MiB")),
        Some(Value::String(text)) => crate::structs::netpolicy::parse_bytes(&Value::String(text))
            .map(|bytes| Some(bytes as f64 / (1024.0 * 1024.0)))
            .map_err(|err| serde::de::Error::custom(format!("memory: {err:#}"))),
        Some(_) => Err(serde::de::Error::custom(
            "memory must be MiB (a number) or a size like \"64Gi\"",
        )),
    }
}

/// A namespace glob: a namespace name in which `*` may stand for any run.
pub(crate) fn validate_namespace_glob(glob: &str) -> Result<(), String> {
    if glob == "*" {
        return Ok(());
    }
    let literal = glob.replace('*', "a");
    validate_namespace_name(&literal).map_err(|err| format!("namespace glob {glob:?}: {err}"))
}

fn validate_globs(what: &str, globs: &[String]) -> Result<(), String> {
    for glob in globs {
        if glob.trim().is_empty() {
            return Err(format!("{what} entries must not be empty"));
        }
    }
    Ok(())
}

impl NamespacePolicy {
    /// Parse and validate a policy object from JSON.
    pub fn from_json(raw: &str) -> Result<Self, String> {
        let mut policy: NamespacePolicy =
            serde_json::from_str(raw).map_err(|err| format!("invalid namespace policy: {err}"))?;
        policy.normalize()?;
        Ok(policy)
    }

    /// Trim, sort and validate in place.
    pub fn normalize(&mut self) -> Result<(), String> {
        self.name = self.name.trim().to_string();
        if self.name.is_empty() || self.name.len() > MAX_POLICY_NAME_LEN {
            return Err(format!(
                "namespace policy name must be 1-{MAX_POLICY_NAME_LEN} characters"
            ));
        }
        if let Some(bad) = self
            .name
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')))
        {
            return Err(format!(
                "namespace policy name {:?} contains {bad:?}; allowed: letters, digits, '-', '_', '.'",
                self.name
            ));
        }
        let what = format!("namespace policy '{}'", self.name);
        if self.namespaces.is_empty() {
            return Err(format!(
                "{what}: namespaces is required (names or globs, e.g. [\"payments-*\"])"
            ));
        }
        for glob in self.namespaces.iter_mut() {
            *glob = glob.trim().to_string();
            validate_namespace_glob(glob).map_err(|err| format!("{what}: {err}"))?;
        }
        self.namespaces.sort();
        self.namespaces.dedup();
        let mut quota = BTreeMap::new();
        for (band, limit) in std::mem::take(&mut self.quota) {
            let parsed = Band::parse(band.trim()).map_err(|err| format!("{what}: quota: {err}"))?;
            for (dimension, value) in [
                ("cpu", limit.cpu),
                ("memory", limit.memory),
                ("cores", limit.cores),
            ] {
                if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
                    return Err(format!(
                        "{what}: quota.{band}.{dimension} must be a non-negative number"
                    ));
                }
            }
            quota.insert(parsed.as_str().to_string(), limit);
        }
        self.quota = quota;
        if let Some(pools) = self.pools.as_mut() {
            if let Some(allowed) = pools.allowed.as_mut() {
                for glob in allowed.iter_mut() {
                    *glob = glob.trim().to_string();
                }
                validate_globs(&format!("{what}: pools.allowed"), allowed)?;
            }
            if let Some(default) = pools.default.as_mut() {
                let mut names: Vec<String> = default
                    .pools()
                    .into_iter()
                    .map(|pool| pool.trim().to_string())
                    .collect();
                names.dedup();
                if names.is_empty() || names.iter().any(String::is_empty) {
                    return Err(format!(
                        "{what}: pools.default must name a pool (or a list of pools)"
                    ));
                }
                for name in &names {
                    if name.contains('*') {
                        return Err(format!(
                            "{what}: pools.default '{name}' must be a pool name, not a glob"
                        ));
                    }
                    if let Some(allowed) = pools.allowed.as_ref() {
                        if !allowed.iter().any(|glob| glob_match(glob, name)) {
                            return Err(format!(
                                "{what}: pools.default '{name}' is not in pools.allowed"
                            ));
                        }
                    }
                }
                *default = match <[String; 1]>::try_from(names) {
                    Ok([one]) => PoolDefault::One(one),
                    Err(names) => PoolDefault::Many(names),
                };
            }
        }
        for (field, list) in [
            ("runtimes", self.runtimes.as_mut()),
            ("hostVolumes", self.host_volumes.as_mut()),
        ] {
            if let Some(list) = list {
                for glob in list.iter_mut() {
                    *glob = glob.trim().to_string();
                }
                validate_globs(&format!("{what}: {field}"), list)?;
            }
        }
        if let Some(autoscale) = self.autoscale.as_ref() {
            autoscale.validate_inherited(&what)?;
        }
        Ok(())
    }

    /// Whether the policy governs `namespace`. Selectors follow the ACL
    /// reserved-prefix rule ([`namespace_glob_match`]): a `*` policy governs
    /// every tenant namespace but not `legion-system`, so a tenant-wide
    /// quota or default pool never lands on what Legion runs for itself. A
    /// selector that starts with `legion-` governs reserved namespaces.
    pub fn matches(&self, namespace: &str) -> bool {
        self.namespaces
            .iter()
            .any(|glob| namespace_glob_match(glob, namespace))
    }

    /// `payments, payments-*`, for messages.
    pub fn selector(&self) -> String {
        self.namespaces.join(", ")
    }
}

/// The policies, of `all`, that govern `namespace`.
pub fn governing<'a>(all: &'a [NamespacePolicy], namespace: &str) -> Vec<&'a NamespacePolicy> {
    all.iter()
        .filter(|policy| policy.matches(namespace))
        .collect()
}

/// The host volumes a task claims: `(where, params.name)` for every
/// `driver = "host"` volume, its `volumes[]` claims and its stateful
/// storage alike.
fn host_volume_names(task: &crate::structs::TaskConfig) -> Vec<(String, String)> {
    let name_of = |params: Option<&Value>| {
        params
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let mut names = Vec::new();
    for volume in &task.volumes {
        if volume.driver_name() == "host" {
            names.push((
                format!("volumes '{}'", volume.name),
                name_of(volume.params.as_ref()),
            ));
        }
    }
    if let Some(storage) = task
        .stateful
        .as_ref()
        .and_then(|stateful| stateful.storage.as_ref())
    {
        if storage.driver_name() == "host" {
            names.push((
                "stateful.storage".to_string(),
                name_of(storage.params.as_ref()),
            ));
        }
    }
    names
}

/// The static half of admission: runtimes, pools and host volumes, which
/// need only the jobspec. Applies every policy governing the job's
/// namespace, and fills a task's pool from a policy's `pools.default` when
/// its permit names none — so the stored config says where it will run.
///
/// Errors name the policy, the selector it governs, and what to change.
pub fn admit_job(policies: &[NamespacePolicy], config: &mut JobConfig) -> Result<(), String> {
    let namespace = job_namespace(&config.name).to_string();
    let governing = governing(policies, &namespace);
    if governing.is_empty() {
        return Ok(());
    }
    let mut defaults = governing.iter().filter_map(|policy| {
        policy
            .pools
            .as_ref()
            .and_then(|pools| pools.default.as_ref())
            .map(|pool| (policy.name.as_str(), pool))
    });
    let default_pool = defaults.next();
    if let (Some((first, pool)), Some((second, other))) = (default_pool, defaults.next()) {
        if !pool.same_pools(other) {
            return Err(format!(
                "namespace policies '{first}' and '{second}' both govern namespace {namespace} \
                 and set different default pools ({}; {}); a task that names no pool has no \
                 single place to go. Hint: name the pool in the task's permit, or make the \
                 policies agree",
                pool.label(),
                other.label()
            ));
        }
    }
    for task in config.tasks.iter_mut() {
        for policy in &governing {
            if let Some(allowed) = policy.runtimes.as_ref() {
                if !allowed.iter().any(|glob| glob_match(glob, &task.runtime)) {
                    return Err(format!(
                        "namespace policy '{}' allows runtimes [{}] in {}; task {} uses {}",
                        policy.name,
                        allowed.join(", "),
                        policy.selector(),
                        task.name,
                        task.runtime
                    ));
                }
            }
            if let Some(allowed) = policy.host_volumes.as_ref() {
                for (place, name) in host_volume_names(task) {
                    if !allowed.iter().any(|glob| glob_match(glob, &name)) {
                        return Err(format!(
                            "namespace policy '{}' allows host volumes [{}] in {}; task {} \
                             {place} claims host volume '{name}'",
                            policy.name,
                            allowed.join(", "),
                            policy.selector(),
                            task.name
                        ));
                    }
                }
            }
        }
        let mut pool = crate::constraints::permit_pool(&task.permit)
            .map_err(|err| format!("task {} permit: {err:#}", task.name))?;
        // The pools a policy default put in the permit, each checked below.
        let mut defaulted: Vec<String> = Vec::new();
        if pool == crate::constraints::PermitPool::Implicit {
            if let Some((_, default)) = default_pool {
                task.permit = default.apply_to(&task.permit);
                defaulted = default.pools().into_iter().map(str::to_string).collect();
                pool = match default.pools().as_slice() {
                    [one] => crate::constraints::PermitPool::Exact(one.to_string()),
                    _ => crate::constraints::PermitPool::Other,
                };
            }
        }
        for policy in &governing {
            let Some(allowed) = policy
                .pools
                .as_ref()
                .and_then(|pools| pools.allowed.as_ref())
            else {
                continue;
            };
            if defaulted.len() > 1 {
                if let Some(name) = defaulted
                    .iter()
                    .find(|name| !allowed.iter().any(|glob| glob_match(glob, name)))
                {
                    return Err(format!(
                        "namespace policy '{}' allows pools [{}] in {}; task {} would run in \
                         pool {name} (a default pool). Hint: set the task's permit to an \
                         allowed pool",
                        policy.name,
                        allowed.join(", "),
                        policy.selector(),
                        task.name
                    ));
                }
                continue;
            }
            let name = match &pool {
                crate::constraints::PermitPool::Implicit => "default".to_string(),
                crate::constraints::PermitPool::Exact(name) => name.clone(),
                crate::constraints::PermitPool::Other => {
                    return Err(format!(
                        "namespace policy '{}' restricts pools in {}; task {}'s permit must \
                         name its pool exactly (pool = \"...\") so admission can check it",
                        policy.name,
                        policy.selector(),
                        task.name
                    ))
                }
            };
            if !allowed.iter().any(|glob| glob_match(glob, &name)) {
                return Err(format!(
                    "namespace policy '{}' allows pools [{}] in {}; task {} would run in pool \
                     {name}. Hint: set the task's permit to an allowed pool",
                    policy.name,
                    allowed.join(", "),
                    policy.selector(),
                    task.name
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod namespace_policy_test {
    use super::*;
    use serde_json::json;

    fn policy(value: Value) -> NamespacePolicy {
        NamespacePolicy::from_json(&value.to_string()).expect("valid policy")
    }

    fn job(name: &str, runtime: &str, permit: Value) -> JobConfig {
        JobConfig {
            name: name.to_string(),
            tasks: vec![crate::structs::TaskConfig {
                permit,
                ..crate::structs::TaskConfig::new("main", runtime)
            }],
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
            pending_extensions: Default::default(),
            extensions: Default::default(),
        }
    }

    #[test]
    fn policies_parse_normalize_and_reject_mistakes() {
        let parsed = policy(json!({
            "name": "payments",
            "namespaces": ["payments-*", "payments"],
            "quota": { "high": { "cpu": 64000, "memory": "128Gi" } },
            "maxJobs": 300,
            "pools": { "allowed": ["shared-*"], "default": "shared-general" },
            "runtimes": ["container", "sandbox"],
        }));
        assert_eq!(parsed.namespaces, ["payments", "payments-*"]);
        assert_eq!(parsed.quota["high"].memory, Some(128.0 * 1024.0));
        assert!(parsed.matches("payments-loadtest"));
        assert!(!parsed.matches("shared"));

        for (bad, expect) in [
            (
                json!({ "name": "p", "namespaces": [] }),
                "namespaces is required",
            ),
            (
                json!({ "name": "p", "namespaces": ["Bad"] }),
                "namespace glob",
            ),
            (
                json!({ "name": "p", "namespaces": ["a"], "quota": { "urgent": {} } }),
                "quota",
            ),
            (
                json!({ "name": "p", "namespaces": ["a"], "quota": { "high": { "cpu": -1 } } }),
                "non-negative",
            ),
            (
                json!({ "name": "p", "namespaces": ["a"],
                        "pools": { "allowed": ["gpu"], "default": "cpu" } }),
                "not in pools.allowed",
            ),
            (json!({ "name": "p q", "namespaces": ["a"] }), "contains"),
            (
                json!({ "name": "p", "namespaces": ["a"], "maxJob": 1 }),
                "unknown field",
            ),
        ] {
            let err = NamespacePolicy::from_json(&bad.to_string()).unwrap_err();
            assert!(err.contains(expect), "{expect}: {err}");
        }
    }

    #[test]
    fn admission_checks_runtimes_pools_and_fills_the_default_pool() {
        let policies = vec![policy(json!({
            "name": "payments",
            "namespaces": ["payments-*"],
            "pools": { "allowed": ["shared-*"], "default": "shared-general" },
            "runtimes": ["container"],
        }))];

        let mut ok = job("payments-prod/web", "container", json!({}));
        admit_job(&policies, &mut ok).unwrap();
        assert_eq!(ok.tasks[0].permit, json!({ "pool": "shared-general" }));

        let mut exec = job("payments-prod/web", "exec", json!({}));
        let err = admit_job(&policies, &mut exec).unwrap_err();
        assert!(err.contains("allows runtimes [container]"), "{err}");

        let mut gpu = job("payments-prod/web", "container", json!({ "pool": "gpu" }));
        let err = admit_job(&policies, &mut gpu).unwrap_err();
        assert!(err.contains("would run in pool gpu"), "{err}");

        let mut vague = job(
            "payments-prod/web",
            "container",
            json!([{ "key": "pool", "operator": "!=", "value": "gpu" }]),
        );
        let err = admit_job(&policies, &mut vague).unwrap_err();
        assert!(err.contains("name its pool exactly"), "{err}");

        // Outside the selector nothing applies, and nothing is filled in.
        let mut other = job("shared/web", "exec", json!({}));
        admit_job(&policies, &mut other).unwrap();
        assert_eq!(other.tasks[0].permit, json!({}));
    }

    /// `pools.default` may name several pools: a task that names none may
    /// run in any of them (the `legion-system` policy's two pools).
    #[test]
    fn a_default_of_several_pools_permits_any_of_them_test() {
        let system = policy(json!({
            "name": "legion-system",
            "namespaces": ["legion-system"],
            "pools": { "default": ["legion-system", "default"] },
        }));
        let policies = vec![system.clone()];
        let mut plain = job("legion-system/agent", "exec", json!({}));
        admit_job(&policies, &mut plain).unwrap();
        assert_eq!(
            plain.tasks[0].permit,
            json!([{ "key": "pool", "operator": "matches", "value": "^(legion\\-system|default)$" }])
        );
        // An object permit keeps its other rules.
        let mut zoned = job("legion-system/agent", "exec", json!({ "zone": "a" }));
        admit_job(&policies, &mut zoned).unwrap();
        assert_eq!(
            zoned.tasks[0].permit,
            json!([
                { "key": "zone", "value": "a" },
                { "key": "pool", "operator": "matches", "value": "^(legion\\-system|default)$" },
            ])
        );
        // A task that names its pool is untouched.
        let mut named = job(
            "legion-system/agent",
            "exec",
            json!({ "pool": "legion-system" }),
        );
        admit_job(&policies, &mut named).unwrap();
        assert_eq!(named.tasks[0].permit, json!({ "pool": "legion-system" }));

        // One name normalizes to the single form; a glob and an empty list
        // are refused; each default must be allowed.
        let one = policy(json!({ "name": "p", "namespaces": ["*"],
                                 "pools": { "default": ["gpu"] } }));
        assert_eq!(
            one.pools.unwrap().default,
            Some(PoolDefault::One("gpu".into()))
        );
        for bad in [json!([]), json!(["gpu-*"])] {
            let mut raw: NamespacePolicy = serde_json::from_value(json!({
                "name": "p", "namespaces": ["*"], "pools": { "default": bad },
            }))
            .unwrap();
            assert!(raw.normalize().is_err());
        }
        let mut outside: NamespacePolicy = serde_json::from_value(json!({
            "name": "p", "namespaces": ["*"],
            "pools": { "allowed": ["gpu"], "default": ["gpu", "cpu"] },
        }))
        .unwrap();
        let err = outside.normalize().unwrap_err();
        assert!(err.contains("'cpu' is not in pools.allowed"), "{err}");

        // Two policies agree when they name the same pools in any order.
        let reordered = policy(json!({
            "name": "again",
            "namespaces": ["legion-*"],
            "pools": { "default": ["default", "legion-system"] },
        }));
        let mut agent = job("legion-system/agent", "exec", json!({}));
        admit_job(&[system, reordered], &mut agent).unwrap();
    }

    #[test]
    fn allowlists_intersect_and_defaults_must_agree() {
        let policies = vec![
            policy(
                json!({ "name": "a", "namespaces": ["payments-*"], "runtimes": ["container", "exec"] }),
            ),
            policy(json!({ "name": "b", "namespaces": ["*"], "runtimes": ["container"] })),
        ];
        let mut exec = job("payments-prod/web", "exec", json!({}));
        let err = admit_job(&policies, &mut exec).unwrap_err();
        assert!(err.contains("namespace policy 'b'"), "{err}");

        let disagree = vec![
            policy(json!({ "name": "a", "namespaces": ["p"], "pools": { "default": "x" } })),
            policy(json!({ "name": "b", "namespaces": ["p"], "pools": { "default": "y" } })),
        ];
        let mut web = job("p/web", "container", json!({}));
        let err = admit_job(&disagree, &mut web).unwrap_err();
        assert!(err.contains("different default pools"), "{err}");
    }

    #[test]
    fn host_volumes_are_checked_by_name() {
        let policies = vec![policy(json!({
            "name": "payments",
            "namespaces": ["payments"],
            "hostVolumes": ["payments-*"],
        }))];
        let mut web = job("payments/web", "container", json!({}));
        web.tasks[0].volumes = vec![serde_json::from_value(json!({
            "name": "data", "driver": "host", "params": { "name": "models" }
        }))
        .unwrap()];
        let err = admit_job(&policies, &mut web).unwrap_err();
        assert!(err.contains("claims host volume 'models'"), "{err}");
        web.tasks[0].volumes = vec![serde_json::from_value(json!({
            "name": "data", "driver": "host", "params": { "name": "payments-models" }
        }))
        .unwrap()];
        admit_job(&policies, &mut web).unwrap();
    }

    /// A tenant-wide `*` policy never governs `legion-system`, so its default
    /// pool cannot collide with the system namespace's own; a selector that
    /// names the prefix does govern it.
    #[test]
    fn star_policies_skip_reserved_namespaces() {
        let tenants = policy(json!({
            "name": "tenants",
            "namespaces": ["*"],
            "pools": { "allowed": ["shared"], "default": "shared" },
        }));
        let system = policy(json!({
            "name": "legion-system",
            "namespaces": ["legion-system"],
            "pools": { "default": "legion-system" },
        }));
        assert!(tenants.matches("payments"));
        assert!(!tenants.matches("legion-system"));
        assert!(
            policy(json!({ "name": "all", "namespaces": ["*", "legion-*"] }))
                .matches("legion-system")
        );
        let policies = vec![tenants, system];
        let mut agent = job("legion-system/agent", "exec", json!({}));
        admit_job(&policies, &mut agent).unwrap();
        assert_eq!(agent.tasks[0].permit, json!({ "pool": "legion-system" }));
    }
}
