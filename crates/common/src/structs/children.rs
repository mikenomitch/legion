//! Child sessions and ownership: the `children` block of a task, the typed
//! owner reference a child carries, and the rules the manager, the worker,
//! and the CLI all agree on (docs/child-sessions.md).
//!
//! A child is a session (or, later, a keyed instance) of one task that
//! another unit of the same job started and owns. The reference to the
//! owner is stored with the child; whoever holds the child's record ends it
//! when the owner's authority says the owner reached a terminal state.

use crate::structs::{InstanceMode, JobConfig, OwnerRef, TaskConfig};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// `OwnerRef.kind` for an owner that is a session: its node decides when it
/// ended.
pub const OWNER_SESSION: &str = "session";
/// `OwnerRef.kind` for a keyed or slot instance: the managers decide.
pub const OWNER_INSTANCE: &str = "instance";
/// `OwnerRef.kind` for an allocation with no instance identity: the
/// managers decide.
pub const OWNER_ALLOCATION: &str = "allocation";

/// `--owner self`: the calling allocation's own unit, on the API socket.
pub const OWNER_SELF: &str = "self";

/// `ChildInfo.kind`: a session child, whose record is its node's.
pub const CHILD_SESSION: &str = "session";
/// `ChildInfo.kind`: a keyed child, whose record is a replicated row.
pub const CHILD_INSTANCE: &str = "instance";

/// `OwnerNotice.event` values.
pub const EVENT_STOPPED: &str = "stopped";
pub const EVENT_DESTROYED: &str = "destroyed";
pub const EVENT_SUSPENDED: &str = "suspended";
pub const EVENT_PROMOTED: &str = "promoted";

/// How a node's refusal of a child for its owner's sake starts (the owner's
/// task does not declare the child task, the owner is at `max`, the owner
/// is itself a child). A manager routing the start reads it as final: no
/// other node would answer differently.
pub const REFUSED_FOR_OWNER: &str = "child refused: ";

/// The default `children.max`.
pub const DEFAULT_MAX_CHILDREN: u32 = 1;

/// The default `children.orphanAfter`: long enough to ride out a worker
/// restart, short enough that a leak costs minutes.
pub const DEFAULT_ORPHAN_AFTER: Duration = Duration::from_secs(600);

/// The shortest `children.orphanAfter`: the owner is asked about once a
/// minute, so anything shorter would end children between two answers.
pub const MIN_ORPHAN_AFTER: Duration = Duration::from_secs(60);

/// How often the managers ask about a session owner that is not on its
/// child's node.
pub const ORPHAN_CHECK_EVERY: Duration = Duration::from_secs(60);

/// How long a node remembers a child that ended, so its owner can learn
/// why (`child_gone`).
pub const GONE_CHILD_TTL: Duration = Duration::from_secs(600);

fn default_max() -> u32 {
    DEFAULT_MAX_CHILDREN
}

/// `children` on a task: the sibling tasks whose sessions (or keyed
/// instances) a unit of this task may start and own.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChildrenConfig {
    /// Session or keyed tasks of the same job this task's units may own.
    #[serde(default)]
    pub tasks: Vec<String>,
    /// The most children one owner holds at once. Default 1.
    #[serde(default = "default_max")]
    pub max: u32,
    /// Whether an owner's children suspend when it does. Default false:
    /// each child follows its own task's idle policy.
    #[serde(default)]
    pub suspend_with_owner: bool,
    /// Session owners only: how long a child on another node outlives an
    /// owner nobody can reach, e.g. `"10m"` (the default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orphan_after: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

crate::structs::spec_fields::spec_block!(
    ChildrenConfig,
    ["tasks", "max", "suspendWithOwner", "orphanAfter"]
);

impl Default for ChildrenConfig {
    fn default() -> Self {
        Self {
            tasks: Vec::new(),
            max: DEFAULT_MAX_CHILDREN,
            suspend_with_owner: false,
            orphan_after: None,
            unknown_fields: Default::default(),
        }
    }
}

impl ChildrenConfig {
    /// Whether this block lets its task own children of `task_name`.
    pub fn declares(&self, task_name: &str) -> bool {
        self.tasks.iter().any(|name| name == task_name)
    }

    /// `orphanAfter`, parsed; the default when unset or unreadable (submit
    /// refuses an unreadable one, so that is only a stored spec from before
    /// a rule changed).
    pub fn orphan_after(&self) -> Duration {
        self.orphan_after
            .as_deref()
            .and_then(|raw| humantime::parse_duration(raw.trim()).ok())
            .unwrap_or(DEFAULT_ORPHAN_AFTER)
    }
}

/// How a task's units can be children: `Some("session")`, `Some("keyed")`,
/// or `None` for a task whose units the scheduler places from `count` (a
/// service, a batch task, a slot task, an envelope).
pub fn child_kind(task: &TaskConfig) -> Option<&'static str> {
    if session_like(task) {
        return Some("session");
    }
    if crate::structs::session::envelope_serves(task).is_some() {
        return None;
    }
    task.stateful
        .as_ref()
        .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed)
        .then_some("keyed")
}

/// A session task, as written (`mode = "session"`) or as stored (keyed,
/// with an envelope block).
fn session_like(task: &TaskConfig) -> bool {
    task.stateful.as_ref().is_some_and(|stateful| {
        stateful.mode == InstanceMode::Session
            || (stateful.mode == InstanceMode::Keyed && stateful.envelope.is_some())
    })
}

/// What a task is, for the refusal that says why it cannot be a child.
fn describe(task: &TaskConfig) -> &'static str {
    match task.stateful.as_ref().map(|stateful| stateful.mode) {
        Some(InstanceMode::Slot) => "a slot task",
        _ if task.lifecycle.to_string() == "batch" => "a batch task",
        _ => "a service task",
    }
}

/// Submit-time check of every task's `children` block. Cross-task, so it
/// runs once per job, after session tasks were normalized.
pub fn validate_children(config: &JobConfig) -> Result<(), String> {
    let job = config.name.as_str();
    let declared: std::collections::HashSet<&str> = config
        .tasks
        .iter()
        .filter_map(|task| task.children.as_ref())
        .flat_map(|children| children.tasks.iter().map(String::as_str))
        .collect();
    for owner in &config.tasks {
        let Some(children) = owner.children.as_ref() else {
            continue;
        };
        let prefix = format!("[children] {job}/{}", owner.name);
        if crate::structs::session::envelope_serves(owner).is_some() {
            return Err(format!("{prefix}: an envelope cannot own children"));
        }
        if declared.contains(owner.name.as_str()) {
            return Err(format!(
                "{prefix}: a child task cannot declare children (depth 1 only)"
            ));
        }
        if children.tasks.is_empty() {
            return Err(format!(
                "{prefix}: children.tasks is required: the session or keyed tasks of this job \
                 its units may own"
            ));
        }
        if children.max == 0 {
            return Err(format!("{prefix}: children.max must be at least 1"));
        }
        if let Some(raw) = children.orphan_after.as_deref() {
            let after = humantime::parse_duration(raw.trim()).map_err(|err| {
                format!("{prefix}: children.orphanAfter {raw:?} is not a duration: {err}")
            })?;
            if after < MIN_ORPHAN_AFTER {
                return Err(format!(
                    "{prefix}: children.orphanAfter {raw} is below the 1m minimum (an owner on \
                     another node is asked about once a minute)"
                ));
            }
        }
        let mut seen = std::collections::HashSet::new();
        for name in &children.tasks {
            if !seen.insert(name.as_str()) {
                return Err(format!("{prefix}: children.tasks names {name:?} twice"));
            }
            if name == &owner.name {
                return Err(format!(
                    "{prefix}: a child task cannot declare children (depth 1 only)"
                ));
            }
            let Some(child) = config.tasks.iter().find(|task| &task.name == name) else {
                return Err(format!(
                    "{prefix}: children.tasks names {name:?}, which is not a task in this job"
                ));
            };
            match child_kind(child) {
                None => {
                    return Err(format!(
                        "{prefix}: children.tasks names {name:?}, {}; children are sessions \
                         (stateful.mode = \"session\") or keyed instances (stateful.mode = \
                         \"keyed\")",
                        describe(child)
                    ));
                }
                Some("keyed") if session_like(owner) => {
                    return Err(format!(
                        "{prefix}: children.tasks names {name:?}, a keyed task, but {} is a \
                         session task; a session cannot own a durable child (promote the \
                         session, or make {} keyed)",
                        owner.name, owner.name
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// A validated owner reference: `kind` is one of the three kinds and `id`
/// is not empty.
pub fn owner_ref(kind: &str, id: &str) -> OwnerRef {
    OwnerRef {
        kind: kind.to_string(),
        id: id.to_string(),
    }
}

/// Whether the managers, not a node, decide when this owner ended.
pub fn raft_owned(owner: &OwnerRef) -> bool {
    owner.kind == OWNER_INSTANCE || owner.kind == OWNER_ALLOCATION
}

/// `kind:id`, for logs and map keys.
pub fn owner_label(owner: &OwnerRef) -> String {
    format!("{}:{}", owner.kind, owner.id)
}

/// Check an owner reference a caller sent: a known kind (or none, which
/// the receiver resolves) and an id.
pub fn check_owner_request(owner: &OwnerRef) -> Result<(), String> {
    if owner.id.trim().is_empty() {
        return Err("owner.id is required: the id of the session, instance, or allocation".into());
    }
    match owner.kind.as_str() {
        "" | OWNER_SESSION | OWNER_INSTANCE | OWNER_ALLOCATION => Ok(()),
        other => Err(format!(
            "owner.kind {other:?} is not one of session, instance, allocation"
        )),
    }
}

/// The refusal for an owner whose task does not declare the child task.
pub fn not_declared(job_id: &str, owner_task: &str, child_task: &str) -> String {
    format!(
        "task {owner_task} of job {job_id} does not declare {child_task} as a child task; add \
         `children = {{ tasks = [\"{child_task}\"] }}` to {owner_task}"
    )
}

/// The refusal for an owner that is itself a child.
pub fn owner_is_child(owner: &OwnerRef) -> String {
    format!(
        "{} {} is itself a child; a child cannot own children (depth 1 only)",
        owner.kind, owner.id
    )
}

/// The refusal for a session that would own a keyed child.
pub fn keyed_child_of_session(owner_task: &str, child_task: &str) -> String {
    format!(
        "{child_task} is a keyed task and {owner_task} is a session task; a session cannot own \
         a durable child (promote the session first)"
    )
}

/// The refusal at `children.max`.
pub fn at_max(owner: &OwnerRef, max: u32) -> String {
    format!(
        "{} {} already owns {max} children (children.max = {max}); stop one first",
        owner.kind, owner.id
    )
}

/// The refusal for an owner nothing runs. Deliberately the same whether the
/// id never existed or names something the caller may not see.
pub fn owner_not_found(id: &str) -> String {
    format!("no running session, instance, or allocation {id} to own the child")
}

#[cfg(test)]
mod children_test {
    use super::*;
    use serde_json::json;

    fn job(tasks: serde_json::Value) -> JobConfig {
        let mut config: JobConfig =
            serde_json::from_value(json!({ "name": "agents", "tasks": tasks })).expect("job");
        crate::structs::session::normalize_session_tasks(&mut config).expect("normalize");
        config
    }

    fn session_task(name: &str) -> serde_json::Value {
        json!({
            "name": name,
            "runtime": "exec",
            "config": { "command": "sleep", "args": ["60"] },
            "resources": { "cpu": 100, "memory": 64 },
            "stateful": { "mode": "session", "envelope": { "resources": { "cpu": 1000, "memory": 1024 } } },
        })
    }

    fn with_children(
        mut task: serde_json::Value,
        children: serde_json::Value,
    ) -> serde_json::Value {
        task["children"] = children;
        task
    }

    #[test]
    fn a_session_may_own_sessions_of_a_sibling_task_test() {
        let config = job(json!([
            with_children(
                session_task("agent"),
                json!({ "tasks": ["machine"], "max": 2 })
            ),
            session_task("machine"),
        ]));
        validate_children(&config).expect("valid");
        let children = config.tasks[0].children.as_ref().expect("children");
        assert!(children.declares("machine"));
        assert!(!children.declares("agent"));
        assert_eq!(children.max, 2);
        assert_eq!(children.orphan_after(), DEFAULT_ORPHAN_AFTER);
    }

    #[test]
    fn the_four_submit_checks_are_worded_as_documented_test() {
        let missing = job(json!([with_children(
            session_task("agent"),
            json!({ "tasks": ["machine"] })
        )]));
        assert_eq!(
            validate_children(&missing).unwrap_err(),
            "[children] agents/agent: children.tasks names \"machine\", which is not a task in this job"
        );

        let service = job(json!([
            with_children(session_task("agent"), json!({ "tasks": ["web"] })),
            { "name": "web", "runtime": "exec", "config": { "command": "sleep" } },
        ]));
        assert_eq!(
            validate_children(&service).unwrap_err(),
            "[children] agents/agent: children.tasks names \"web\", a service task; children are \
             sessions (stateful.mode = \"session\") or keyed instances (stateful.mode = \"keyed\")"
        );

        let keyed = job(json!([
            with_children(session_task("agent"), json!({ "tasks": ["workspace"] })),
            { "name": "workspace", "runtime": "exec", "config": { "command": "sleep" },
              "stateful": { "mode": "keyed" } },
        ]));
        assert_eq!(
            validate_children(&keyed).unwrap_err(),
            "[children] agents/agent: children.tasks names \"workspace\", a keyed task, but agent \
             is a session task; a session cannot own a durable child (promote the session, or \
             make agent keyed)"
        );

        let nested = job(json!([
            with_children(session_task("agent"), json!({ "tasks": ["machine"] })),
            with_children(session_task("machine"), json!({ "tasks": ["scratch"] })),
            session_task("scratch"),
        ]));
        assert_eq!(
            validate_children(&nested).unwrap_err(),
            "[children] agents/machine: a child task cannot declare children (depth 1 only)"
        );
    }

    #[test]
    fn every_owner_kind_may_declare_children_test() {
        // A service and a keyed task own sessions; a keyed task owns a keyed
        // task; none of them is a session owning a keyed child.
        let config = job(json!([
            with_children(
                json!({ "name": "ci", "runtime": "exec", "config": { "command": "sleep" } }),
                json!({ "tasks": ["machine"], "max": 8 })
            ),
            with_children(
                json!({ "name": "keeper", "runtime": "exec", "config": { "command": "sleep" },
                        "stateful": { "mode": "keyed" } }),
                json!({ "tasks": ["machine", "workspace"], "suspendWithOwner": true })
            ),
            session_task("machine"),
            { "name": "workspace", "runtime": "exec", "config": { "command": "sleep" },
              "stateful": { "mode": "keyed" } },
        ]));
        validate_children(&config).expect("valid");
    }

    #[test]
    fn limits_are_checked_test() {
        let zero = job(json!([
            with_children(
                session_task("agent"),
                json!({ "tasks": ["machine"], "max": 0 })
            ),
            session_task("machine"),
        ]));
        assert!(validate_children(&zero)
            .unwrap_err()
            .contains("children.max"));
        let short = job(json!([
            with_children(
                session_task("agent"),
                json!({ "tasks": ["machine"], "orphanAfter": "5s" })
            ),
            session_task("machine"),
        ]));
        assert!(validate_children(&short)
            .unwrap_err()
            .contains("1m minimum"));
        let envelope = job(json!([
            with_children(
                session_task("agent"),
                json!({ "tasks": ["machine-envelope"] })
            ),
            session_task("machine"),
        ]));
        assert!(validate_children(&envelope).is_err());
    }

    #[test]
    fn owner_requests_are_checked_test() {
        assert!(check_owner_request(&owner_ref("", "agent-1")).is_ok());
        assert!(check_owner_request(&owner_ref(OWNER_SESSION, "agent-1")).is_ok());
        assert!(check_owner_request(&owner_ref("job", "agent-1")).is_err());
        assert!(check_owner_request(&owner_ref(OWNER_INSTANCE, " ")).is_err());
        assert!(raft_owned(&owner_ref(OWNER_ALLOCATION, "7")));
        assert!(!raft_owned(&owner_ref(OWNER_SESSION, "s")));
    }
}
