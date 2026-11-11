//! Runtime disables (docs/runtime-disable.md): the incident switch that
//! refuses every start of one task runtime, cluster-wide or for the
//! namespaces matching a list of globs.
//!
//! A disable is one record per runtime. The managers hold the records in
//! their store and refuse placements; every worker holds the current set
//! (from each heartbeat reply and from `StreamWork` pushes) and refuses
//! starts itself, so a start with no manager on the path is refused too.
//! This module is the shared half: the record, how its scope matches a
//! namespace, the refusal's wording, and the blocked-eval key.

use serde::{Deserialize, Serialize};

use crate::structs::acl::namespace_glob_match;
use crate::structs::namespace::job_namespace;
use crate::structs::namespace_policy::validate_namespace_glob;

/// The worker capability (`WorkerRegisterRequest.capabilities`) that says a
/// worker holds the runtime-disable set and refuses starts from it. A node
/// without it is listed in `ListRuntimeStates` as unaware.
pub const RUNTIME_DISABLE_CAPABILITY: &str = "runtime-disable";

/// Prefix of the key an eval blocks under when its task's runtime is
/// disabled: `runtime_disabled:<runtime>`.
pub const BLOCKED_KEY_PREFIX: &str = "runtime_disabled:";

/// The error code a refused start carries (`legion-error-code` metadata, so
/// `error.code` in the CLI and MCP envelopes), on a retryable `UNAVAILABLE`.
pub const RUNTIME_DISABLED_CODE: &str = "RUNTIME_DISABLED";

/// Longest `reason` accepted, in bytes.
pub const MAX_REASON_BYTES: usize = 512;

/// One runtime disable.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeDisable {
    /// The task runtime that may not start.
    pub runtime: String,
    /// Namespace globs the disable covers; empty means the whole cluster.
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Why, as the operator gave it.
    #[serde(default)]
    pub reason: String,
    /// When the runtime was first disabled, RFC 3339 UTC.
    #[serde(default)]
    pub since: String,
    /// Who disabled it: `accessor:<id>`, or the caller's principal.
    #[serde(default)]
    pub by: String,
    /// Whether nodes also drain their sessions of the runtime in scope.
    #[serde(default)]
    pub drain: bool,
}

impl RuntimeDisable {
    /// Whether the disable covers the whole cluster.
    pub fn cluster_wide(&self) -> bool {
        self.namespaces.is_empty()
    }

    /// Whether `namespace` is in scope. A cluster-wide disable covers every
    /// namespace, reserved `legion-` ones included; a glob follows the
    /// namespace-policy rule (it never covers a reserved namespace unless
    /// it starts with `legion-` itself).
    pub fn covers_namespace(&self, namespace: &str) -> bool {
        self.cluster_wide()
            || self
                .namespaces
                .iter()
                .any(|glob| namespace_glob_match(glob, namespace))
    }

    /// Whether a start of `runtime` for job `job_id` (canonical) is refused.
    pub fn covers(&self, runtime: &str, job_id: &str) -> bool {
        self.runtime == runtime && self.covers_namespace(job_namespace(job_id))
    }

    /// `cluster`, or the globs joined with `, `.
    pub fn scope(&self) -> String {
        if self.cluster_wide() {
            "cluster".to_string()
        } else {
            self.namespaces.join(", ")
        }
    }

    /// The refusal for a start in `namespace`, without a status code:
    ///
    /// ```text
    /// runtime "sandbox" is disabled in namespace tenants-acme since 2026-11-07T18:04:11Z: firecracker GHSA-... (legion operator runtime status)
    /// ```
    pub fn refusal(&self, namespace: &str) -> String {
        let reason = if self.reason.is_empty() {
            String::new()
        } else {
            format!(": {}", self.reason)
        };
        format!(
            "runtime {:?} is disabled in namespace {namespace} since {}{reason} \
             (legion operator runtime status)",
            self.runtime, self.since
        )
    }

    pub fn to_proto(&self) -> crate::structs::RuntimeState {
        crate::structs::RuntimeState {
            runtime: self.runtime.clone(),
            namespaces: self.namespaces.clone(),
            reason: self.reason.clone(),
            since: self.since.clone(),
            by: self.by.clone(),
            drain: self.drain,
        }
    }

    pub fn from_proto(state: &crate::structs::RuntimeState) -> Self {
        RuntimeDisable {
            runtime: state.runtime.clone(),
            namespaces: state.namespaces.clone(),
            reason: state.reason.clone(),
            since: state.since.clone(),
            by: state.by.clone(),
            drain: state.drain,
        }
    }
}

/// Every disable in force, as one replica or one worker holds it.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(transparent)]
pub struct RuntimeDisableSet {
    pub states: Vec<RuntimeDisable>,
}

impl RuntimeDisableSet {
    pub fn new(mut states: Vec<RuntimeDisable>) -> Self {
        states.sort_by(|left, right| left.runtime.cmp(&right.runtime));
        RuntimeDisableSet { states }
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// The disable that refuses a start of `runtime` for job `job_id`.
    pub fn refusing(&self, runtime: &str, job_id: &str) -> Option<&RuntimeDisable> {
        self.states
            .iter()
            .find(|state| state.covers(runtime, job_id))
    }

    /// The record for `runtime`, whatever its scope.
    pub fn get(&self, runtime: &str) -> Option<&RuntimeDisable> {
        self.states.iter().find(|state| state.runtime == runtime)
    }

    pub fn to_proto(&self) -> crate::structs::RuntimeStateSet {
        crate::structs::RuntimeStateSet {
            states: self.states.iter().map(RuntimeDisable::to_proto).collect(),
        }
    }

    pub fn from_proto(set: &crate::structs::RuntimeStateSet) -> Self {
        Self::new(set.states.iter().map(RuntimeDisable::from_proto).collect())
    }
}

/// A start refused because its runtime is disabled. Carried as an error so
/// every start path can map it to `UNAVAILABLE` (retryable) by downcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDisabledRefusal {
    pub runtime: String,
    pub message: String,
}

impl RuntimeDisabledRefusal {
    pub fn new(disable: &RuntimeDisable, job_id: &str) -> Self {
        RuntimeDisabledRefusal {
            runtime: disable.runtime.clone(),
            message: disable.refusal(job_namespace(job_id)),
        }
    }
}

impl RuntimeDisabledRefusal {
    /// The gRPC status every refused start answers with: `UNAVAILABLE`
    /// (retryable), the disable's own sentence, a hint, and the
    /// [`RUNTIME_DISABLED_CODE`] error code.
    pub fn to_status(&self) -> tonic::Status {
        let mut status = tonic::Status::unavailable(format!(
            "{self}\nHint: retry once the runtime is enabled again; `legion operator runtime \
             status` shows what is disabled and why"
        ));
        status.metadata_mut().insert(
            crate::structs::worker_protocol::ERROR_CODE_METADATA,
            tonic::metadata::MetadataValue::from_static(RUNTIME_DISABLED_CODE),
        );
        status
    }

    /// The refusal anywhere in an error's chain.
    pub fn in_chain(error: &anyhow::Error) -> Option<&RuntimeDisabledRefusal> {
        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<RuntimeDisabledRefusal>())
    }
}

impl std::fmt::Display for RuntimeDisabledRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RuntimeDisabledRefusal {}

/// The key an eval blocks under while `runtime` is disabled for its job.
pub fn blocked_key(runtime: &str) -> String {
    format!("{BLOCKED_KEY_PREFIX}{runtime}")
}

/// The runtime a blocked-eval key names, when it is a runtime-disable key.
pub fn blocked_key_runtime(key: &str) -> Option<&str> {
    key.strip_prefix(BLOCKED_KEY_PREFIX)
}

/// A runtime name as a jobspec's `runtime` can spell it: non-empty, at most
/// 64 bytes, ASCII letters, digits, `-`, `_` and `.`. `envelope` is refused:
/// it is the placeholder that reserves a session task's room, and disabling
/// the session task's own runtime is what stops its sessions.
pub fn validate_runtime_name(runtime: &str) -> Result<(), String> {
    if runtime.is_empty() {
        return Err("runtime must not be empty".into());
    }
    if runtime.len() > 64
        || !runtime
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(format!(
            "runtime {runtime:?} is not a runtime name (letters, digits, `-`, `_`, `.`, at most \
             64 characters)"
        ));
    }
    if runtime == crate::structs::session::ENVELOPE_RUNTIME {
        return Err(format!(
            "runtime {runtime:?} only reserves room for a session task; disable the session \
             task's own runtime (for example `sandbox`) to stop its sessions"
        ));
    }
    Ok(())
}

/// Trim, validate, sort and deduplicate a list of namespace globs.
pub fn normalize_namespaces(namespaces: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for glob in namespaces {
        let glob = glob.trim();
        if glob.is_empty() {
            return Err("a namespace glob must not be empty".into());
        }
        validate_namespace_glob(glob)?;
        out.push(glob.to_string());
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// Validate a disable's `reason`.
pub fn validate_reason(reason: &str) -> Result<(), String> {
    if reason.len() > MAX_REASON_BYTES {
        return Err(format!(
            "reason is {} bytes; at most {MAX_REASON_BYTES}",
            reason.len()
        ));
    }
    if reason.chars().any(|c| c.is_control()) {
        return Err("reason must not contain control characters".into());
    }
    Ok(())
}

#[cfg(test)]
mod runtime_state_test {
    use super::*;

    fn disable(runtime: &str, namespaces: &[&str]) -> RuntimeDisable {
        RuntimeDisable {
            runtime: runtime.into(),
            namespaces: namespaces.iter().map(|s| s.to_string()).collect(),
            reason: "CVE-1".into(),
            since: "2026-11-07T18:04:11Z".into(),
            by: "anonymous".into(),
            drain: false,
        }
    }

    #[test]
    fn a_cluster_wide_disable_covers_every_namespace() {
        let state = disable("sandbox", &[]);
        assert!(state.covers("sandbox", "web"));
        assert!(state.covers("sandbox", "tenants-acme/web"));
        assert!(state.covers("sandbox", "legion-system/ingress"));
        assert!(!state.covers("exec", "web"));
        assert_eq!(state.scope(), "cluster");
    }

    #[test]
    fn a_scoped_disable_covers_only_matching_namespaces() {
        let state = disable("sandbox", &["tenants-*", "ci"]);
        assert!(state.covers("sandbox", "tenants-acme/web"));
        assert!(state.covers("sandbox", "ci/build"));
        assert!(!state.covers("sandbox", "web"), "the default namespace");
        assert!(!state.covers("sandbox", "payments/web"));
        assert_eq!(state.scope(), "tenants-*, ci");
        // A tenant glob never reaches a reserved namespace.
        let all = disable("sandbox", &["*"]);
        assert!(all.covers("sandbox", "payments/web"));
        assert!(!all.covers("sandbox", "legion-system/ingress"));
    }

    #[test]
    fn the_refusal_names_runtime_namespace_since_and_reason() {
        let state = disable("sandbox", &["tenants-*"]);
        assert_eq!(
            state.refusal("tenants-acme"),
            "runtime \"sandbox\" is disabled in namespace tenants-acme since \
             2026-11-07T18:04:11Z: CVE-1 (legion operator runtime status)"
        );
        let mut silent = state.clone();
        silent.reason.clear();
        assert!(!silent.refusal("tenants-acme").contains(": "));
    }

    #[test]
    fn the_set_finds_the_refusing_disable_and_round_trips() {
        let set = RuntimeDisableSet::new(vec![
            disable("sandbox", &["tenants-*"]),
            disable("container", &[]),
        ]);
        assert_eq!(set.states[0].runtime, "container", "sorted by runtime");
        assert!(set.refusing("container", "web").is_some());
        assert!(set.refusing("sandbox", "web").is_none());
        assert!(set.refusing("sandbox", "tenants-a/web").is_some());
        assert!(set.refusing("exec", "tenants-a/web").is_none());
        assert_eq!(RuntimeDisableSet::from_proto(&set.to_proto()), set);
        let json = serde_json::to_string(&set).unwrap();
        assert_eq!(
            serde_json::from_str::<RuntimeDisableSet>(&json).unwrap(),
            set
        );
    }

    #[test]
    fn names_globs_and_reasons_are_validated() {
        assert!(validate_runtime_name("sandbox").is_ok());
        assert!(validate_runtime_name("my-plugin_2").is_ok());
        assert!(validate_runtime_name("").is_err());
        assert!(validate_runtime_name("a b").is_err());
        assert!(validate_runtime_name("envelope")
            .unwrap_err()
            .contains("session task"));
        assert_eq!(
            normalize_namespaces(&[" tenants-* ".into(), "ci".into(), "ci".into()]).unwrap(),
            vec!["ci".to_string(), "tenants-*".to_string()]
        );
        assert!(normalize_namespaces(&["Bad Name".into()]).is_err());
        assert!(normalize_namespaces(&["".into()]).is_err());
        assert!(validate_reason(&"x".repeat(MAX_REASON_BYTES + 1)).is_err());
        assert!(validate_reason("line\nbreak").is_err());
        assert!(validate_reason("CVE-2024-21626, runc < 1.1.12").is_ok());
    }

    #[test]
    fn blocked_keys_round_trip() {
        assert_eq!(blocked_key("sandbox"), "runtime_disabled:sandbox");
        assert_eq!(
            blocked_key_runtime("runtime_disabled:sandbox"),
            Some("sandbox")
        );
        assert_eq!(blocked_key_runtime("class:pool=default"), None);
    }
}
