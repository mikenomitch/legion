//! Session mode (`stateful.mode = "session"`, sandbox-scaling plan §6.2;
//! public doc `docs/session-mode.md`).
//!
//! A session task is a keyed task whose unnamed instances never touch the
//! replicated store. The jobspec surface is one task:
//!
//! ```nickel
//! stateful = {
//!   mode = "session",
//!   envelope = { resources = { cpu = 30000, memory = 61440 } },
//! },
//! resources = { cpu = 500, memory = 512 },   # per session
//! ```
//!
//! Parsing normalizes it into two tasks ([`normalize_session_tasks`]):
//!
//! - the task itself, as a **keyed** task that keeps its `envelope` block.
//!   Named and promoted instances of it are ordinary keyed instances: rows,
//!   tombstones, the scheduler, suspend, all unchanged.
//! - its **envelope task**, `<task>-envelope`, on the built-in
//!   [`ENVELOPE_RUNTIME`]: a stateless task placed once per eligible node
//!   (`count = { num = 1, per = "id" }`) with the envelope's resources. The
//!   scheduler places it like any allocation, which keeps quotas,
//!   preemption, and `node usage` honest about the capacity sessions use.
//!   The worker runs nothing for it: it is the room the node fills with
//!   sessions.
//!
//! Unnamed sessions are started by a manager's session router straight onto
//! a node with envelope room, and live in that node's memory and the
//! managers' session index, never in the store.

use crate::prelude::*;
use crate::structs::{InstanceMode, JobConfig, TaskConfig, TaskCount};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Worker capability: the node runs envelopes and admits sessions into them.
/// Envelope tasks carry an implicit `capabilities contains "sessions"`
/// constraint, so one is never placed on a worker that predates sessions.
pub const SESSIONS_CAPABILITY: &str = "sessions";

/// A node that starts sessions on a session ticket (`Worker.StartSession`),
/// advertised when `edgeBoot.acceptClaims` is on.
pub const EDGE_SESSIONS_CAPABILITY: &str = "edge-sessions";

/// The built-in runtime of a generated envelope task. It starts nothing:
/// the allocation is a reservation. Reserved, so a jobspec cannot declare a
/// task on it.
pub const ENVELOPE_RUNTIME: &str = "envelope";

/// Suffix of a session task's generated envelope task (`workspace-envelope`).
pub const ENVELOPE_SUFFIX: &str = "-envelope";

/// The claim id a worker files a session under. Derived from the session id,
/// so a promotion or a stop can name the claim without asking the node.
pub fn session_claim_id(session_id: &str) -> String {
    format!("session:{session_id}")
}

/// The per-node capacity reservation a session task's sessions fill
/// (`stateful.envelope`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionEnvelopeConfig {
    /// What one envelope reserves on its node, in the units of a task's
    /// `resources` (`cpu` in MHz, `memory` in MiB). Must set `cpu` and
    /// `memory`, and every amount a session's own `resources` declares.
    /// Sessions are admitted against exactly the keys set here.
    #[serde(default = "empty_object")]
    pub resources: Value,
    /// Extra placement constraints for the envelopes, in the same shape as a
    /// task's `constraints`, added to the session task's own.
    #[serde(default = "empty_object")]
    pub constraints: Value,
}

/// When a session becomes a keyed instance (`stateful.promote`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionPromoteConfig {
    /// Whether a caller-chosen id is accepted, on `instance create --id` (a
    /// keyed create) or `instance promote --id` (a rename). Default true.
    /// With false, a create always starts an unnamed session and a promotion
    /// keeps the session's own id.
    #[serde(default = "default_true")]
    pub on_name: bool,
}

impl Default for SessionPromoteConfig {
    fn default() -> Self {
        SessionPromoteConfig { on_name: true }
    }
}

fn default_true() -> bool {
    true
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

/// The name of a session task's envelope task.
pub fn envelope_task_name(task: &str) -> String {
    format!("{task}{ENVELOPE_SUFFIX}")
}

/// The session task an envelope task serves, from its name. `None` for a
/// task that is not an envelope.
pub fn envelope_serves(task: &TaskConfig) -> Option<&str> {
    if task.runtime != ENVELOPE_RUNTIME {
        return None;
    }
    task.name.strip_suffix(ENVELOPE_SUFFIX)
}

/// Whether a stored task is a session task (normalized: keyed, with an
/// envelope block).
pub fn is_session_task(task: &TaskConfig) -> bool {
    task.stateful
        .as_ref()
        .is_some_and(|stateful| stateful.mode == InstanceMode::Keyed && stateful.envelope.is_some())
}

/// Whether a session task accepts caller-chosen ids (`promote.onName`).
pub fn accepts_names(task: &TaskConfig) -> bool {
    task.stateful
        .as_ref()
        .and_then(|stateful| stateful.promote.as_ref())
        .is_none_or(|promote| promote.on_name)
}

/// The session id an idempotent create derives (sandbox-scaling plan §6.7,
/// "Idempotency without raft"): the minted shape `{task}-{26 base32}`, from
/// a SHA-256 over the job, the task, and the caller's scoped idempotency key
/// (which already names the caller). A retry derives the same id on any
/// manager with no record anywhere; another caller, job, or key derives a
/// different one.
pub fn derived_session_id(task_name: &str, job_id: &str, scoped_key: &str) -> String {
    use sha2::{Digest, Sha256};
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut hasher = Sha256::new();
    hasher.update(b"legion-session-id\0");
    hasher.update(job_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(task_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(scoped_key.as_bytes());
    let digest = hasher.finalize();
    // 26 characters of 5 bits each: 130 bits, read from the first 17 bytes.
    let random: String = (0..crate::structs::MINTED_ID_RANDOM_LEN)
        .map(|index| {
            let bit = index * 5;
            let (byte, offset) = (bit / 8, bit % 8);
            let window = (u16::from(digest[byte]) << 8) | u16::from(digest[byte + 1]);
            let value = (window >> (11 - offset)) & 0x1f;
            ALPHABET[value as usize] as char
        })
        .collect();
    let max_prefix =
        crate::structs::KEYED_INSTANCE_ID_MAX_LEN - crate::structs::MINTED_ID_RANDOM_LEN - 1;
    let prefix: String = task_name.chars().take(max_prefix).collect();
    format!("{prefix}-{random}")
}

/// Turn every session task in `config` into its stored form: the task as
/// keyed (keeping its `envelope` block), plus a generated envelope task.
///
/// Idempotent: an envelope task already generated for one of the job's
/// session tasks is dropped and generated again, so a normalized config
/// normalizes to itself. Any other task on [`ENVELOPE_RUNTIME`] is refused.
pub fn normalize_session_tasks(config: &mut JobConfig) -> R<()> {
    let session_tasks: Vec<String> = config
        .tasks
        .iter()
        .filter(|task| {
            task.stateful.as_ref().is_some_and(|stateful| {
                stateful.mode == InstanceMode::Session || stateful.envelope.is_some()
            })
        })
        .map(|task| task.name.clone())
        .collect();
    for task in &config.tasks {
        if task.runtime == ENVELOPE_RUNTIME
            && !envelope_serves(task)
                .is_some_and(|served| session_tasks.iter().any(|name| name == served))
        {
            bail!(
                "task {} uses runtime \"{ENVELOPE_RUNTIME}\", which is reserved for the envelope \
                 tasks Legion generates for session tasks\nHint: declare stateful.mode = \
                 \"session\" with an envelope on the task whose sessions it should hold",
                task.name
            );
        }
    }
    config.tasks.retain(|task| task.runtime != ENVELOPE_RUNTIME);

    let mut envelopes = Vec::new();
    for task in &mut config.tasks {
        let Some(stateful) = task.stateful.as_mut() else {
            continue;
        };
        match (stateful.mode, stateful.envelope.is_some()) {
            (InstanceMode::Session, false) => bail!(
                "task {} sets stateful.mode = \"session\" without stateful.envelope\nHint: add \
                 envelope = {{ resources = {{ cpu = ..., memory = ... }} }}: the room each node \
                 keeps for this task's sessions",
                task.name
            ),
            (InstanceMode::Session, true) => stateful.mode = InstanceMode::Keyed,
            (InstanceMode::Keyed, true) => {}
            (InstanceMode::Slot, true) => bail!(
                "task {} sets stateful.envelope, which only a session task has\nHint: set \
                 stateful.mode = \"session\", or remove envelope",
                task.name
            ),
            (_, false) => {
                if stateful.promote.is_some() {
                    bail!(
                        "task {} sets stateful.promote, which only a session task has\nHint: set \
                         stateful.mode = \"session\", or remove promote",
                        task.name
                    );
                }
                continue;
            }
        }
        envelopes.push(envelope_task(task)?);
    }
    for envelope in envelopes {
        if config.tasks.iter().any(|task| task.name == envelope.name) {
            bail!(
                "task name {} is reserved for the envelope of session task {}\nHint: rename the \
                 task",
                envelope.name,
                envelope_serves(&envelope).unwrap_or_default()
            );
        }
        config.tasks.push(envelope);
    }
    Ok(())
}

/// The envelope task of a (normalized) session task.
fn envelope_task(task: &TaskConfig) -> R<TaskConfig> {
    let envelope = task
        .stateful
        .as_ref()
        .and_then(|stateful| stateful.envelope.clone())
        .unwrap_or_default();
    let mut constraints = constraint_rules(&task.constraints)
        .with_context(|| format!("task {} constraints", task.name))?;
    constraints.extend(
        constraint_rules(&envelope.constraints)
            .with_context(|| format!("task {} stateful.envelope.constraints", task.name))?,
    );
    constraints.push(json!({
        "key": "capabilities",
        "operator": "contains",
        "value": SESSIONS_CAPABILITY,
    }));
    let name = envelope_task_name(&task.name);
    let mut envelope_task = TaskConfig::new(&name, ENVELOPE_RUNTIME);
    envelope_task.group = name;
    envelope_task.count = TaskCount::per(1, "id");
    envelope_task.resources = envelope.resources;
    envelope_task.constraints = Value::Array(constraints);
    envelope_task.permit = task.permit.clone();
    envelope_task.affinities = task.affinities.clone();
    Ok(envelope_task)
}

/// Constraints in array form, from either the map shorthand
/// (`{ pool = "agents" }`) or the array form.
fn constraint_rules(value: &Value) -> R<Vec<Value>> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Object(map) => Ok(map
            .iter()
            .map(|(key, value)| json!({ "key": key, "operator": "=", "value": value }))
            .collect()),
        Value::Array(items) => Ok(items.clone()),
        _ => bail!("constraints must be an object or an array"),
    }
}

/// The numeric amounts of a `resources` object, by key, with the `cpus`
/// alias read as `cpu`. `memoryMax` is a ceiling, not a reservation, so it
/// is left out.
pub fn resource_amounts(resources: &Value) -> BTreeMap<String, f64> {
    resources
        .as_object()
        .map(|map| {
            map.iter()
                .filter(|(key, _)| key.as_str() != crate::resources::MEMORY_MAX_KEY)
                .filter_map(|(key, value)| {
                    let key = if key == "cpus" { "cpu" } else { key.as_str() };
                    value.as_f64().map(|amount| (key.to_string(), amount))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Submit-time checks for a (normalized) session task. Called from
/// `TaskConfig::validate_stateful` for every stateful task; a task that is
/// not a session task passes.
pub fn validate_session_task(task: &TaskConfig) -> R<()> {
    let Some(stateful) = task.stateful.as_ref() else {
        return Ok(());
    };
    let Some(envelope) = stateful.envelope.as_ref() else {
        return Ok(());
    };
    let name = &task.name;
    if stateful.require_id {
        bail!(
            "task {name} is a session task and sets stateful.requireId\nHint: a session that \
             must be named from birth is a keyed instance: use stateful.mode = \"keyed\""
        );
    }
    if stateful.initial_instances.is_some() {
        bail!(
            "task {name} is a session task and sets stateful.initialInstances\nHint: seed named \
             instances on a keyed task, or start sessions with `legion instance create`"
        );
    }
    if stateful.storage.is_some() || !task.volumes.is_empty() {
        bail!(
            "task {name} is a session task and declares storage or volumes. A session runs \
             inside its node's envelope and has no volume of its own\nHint: use stateful.mode \
             = \"keyed\" for instances that need a volume"
        );
    }
    let room = resource_amounts(&envelope.resources);
    for required in ["cpu", "memory"] {
        if !room.contains_key(required) {
            bail!(
                "task {name} stateful.envelope.resources does not set {required}\nHint: set what \
                 one node keeps for this task's sessions, such as {{ cpu = 30000, memory = \
                 61440 }}"
            );
        }
    }
    if let Some(resources) = task.resources.as_object() {
        for (key, value) in resources {
            if key == crate::resources::MEMORY_MAX_KEY {
                continue;
            }
            let Some(amount) = value.as_f64() else {
                bail!(
                    "task {name} is a session task and sets resources.{key} = {value}, which is \
                     not an amount. Sessions are admitted into their envelope by amount\nHint: \
                     remove resources.{key}, or use stateful.mode = \"keyed\""
                );
            };
            let key = if key == "cpus" { "cpu" } else { key.as_str() };
            match room.get(key) {
                Some(room) if *room >= amount => {}
                Some(room) => bail!(
                    "task {name}: one session needs resources.{key} = {amount}, but \
                     stateful.envelope.resources.{key} is {room}, so no session fits in an \
                     envelope\nHint: raise envelope.resources.{key}, or lower resources.{key}"
                ),
                None => bail!(
                    "task {name}: one session needs resources.{key} = {amount}, but \
                     stateful.envelope.resources does not set {key}\nHint: add \
                     envelope.resources.{key}"
                ),
            }
        }
    }
    let network = task.network.as_ref();
    if network.is_some_and(|network| !network.ports.is_empty() || network.declares_policy())
        || task.service.is_some()
        || task.connect.is_some()
        || network.is_some_and(|network| {
            network
                .mode
                .trim()
                .eq_ignore_ascii_case(crate::structs::netpolicy::MODE_GATED)
        })
        || crate::structs::network::task_runs_gated(
            task,
            crate::structs::network::sandbox_gated_by_default(),
        )
    {
        bail!(
            "task {name} is a session task and declares ports, a service, connect upstreams, a \
             network policy, or gated networking. The node datapath serves allocations, and an \
             unnamed session has none\nHint: remove them and set network.mode = \"host\" (or \
             give a sandbox its own config.network), or use stateful.mode = \"keyed\""
        );
    }
    if !task.identities.is_empty() {
        bail!(
            "task {name} is a session task and declares identities. Workload identity for \
             unnamed sessions is not available yet\nHint: remove identities, or use \
             stateful.mode = \"keyed\""
        );
    }
    if task.speculative_start.is_some() {
        bail!(
            "task {name} is a session task and sets speculativeStart. A session already starts \
             without a raft write\nHint: remove speculativeStart"
        );
    }
    if let Some(suspend) = task.suspend.as_ref() {
        let resolved = suspend
            .resolve()
            .map_err(|err| anyhow!("task {name}: {err}"))?;
        if resolved.durability != crate::structs::snapshot::SnapshotDurability::Store {
            bail!(
                "task {name} is a session task and sets suspend with durability = \"local\". A \
                 promoted session's node is wherever its envelope had room, so its checkpoint \
                 must be published\nHint: set suspend.durability = \"store\""
            );
        }
        if resolved.reserve != crate::structs::suspend::SuspendReserve::NONE {
            bail!(
                "task {name} is a session task and sets suspend.reserve. The envelope is the \
                 reservation\nHint: remove suspend.reserve"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod session_test {
    use super::*;

    fn session_task() -> TaskConfig {
        let mut task = TaskConfig::new("workspace", "exec");
        task.config = json!({ "command": ["sleep", "600"] });
        task.resources = json!({ "cpu": 500, "memory": 512 });
        task.constraints = json!({ "pool": "agents" });
        task.stateful = Some(crate::structs::StatefulConfig {
            mode: InstanceMode::Session,
            require_id: false,
            initial_instances: None,
            storage: None,
            on_lost: None,
            envelope: Some(SessionEnvelopeConfig {
                resources: json!({ "cpu": 30000, "memory": 61440 }),
                constraints: json!([{ "key": "rack", "operator": "=", "value": "r1" }]),
            }),
            promote: None,
        });
        task
    }

    fn job(tasks: Vec<TaskConfig>) -> JobConfig {
        JobConfig {
            name: "agents".into(),
            tasks,
            priority: Default::default(),
            rollout: None,
            rollout_by_group: Default::default(),
            version: None,
        }
    }

    #[test]
    fn a_session_task_becomes_keyed_plus_one_envelope_per_node_test() -> R<()> {
        let mut config = job(vec![session_task()]);
        normalize_session_tasks(&mut config)?;
        assert_eq!(config.tasks.len(), 2);
        let task = &config.tasks[0];
        assert_eq!(task.stateful.as_ref().unwrap().mode, InstanceMode::Keyed);
        assert!(is_session_task(task));
        let envelope = &config.tasks[1];
        assert_eq!(envelope.name, "workspace-envelope");
        assert_eq!(envelope.group, "workspace-envelope");
        assert_eq!(envelope.runtime, ENVELOPE_RUNTIME);
        assert_eq!(envelope_serves(envelope), Some("workspace"));
        assert_eq!(envelope.count, TaskCount::per(1, "id"));
        assert_eq!(envelope.resources, json!({ "cpu": 30000, "memory": 61440 }));
        assert!(envelope.stateful.is_none());
        let rules = envelope.constraints.as_array().unwrap();
        assert_eq!(rules.len(), 3, "{rules:?}");
        assert_eq!(rules[0]["key"], "pool");
        assert_eq!(rules[1]["key"], "rack");
        assert_eq!(rules[2]["value"], SESSIONS_CAPABILITY);
        Ok(())
    }

    #[test]
    fn a_jobspec_normalizes_at_parse_and_again_to_itself_test() -> R<()> {
        let spec = json!({
            "name": "agents",
            "task": {
                "name": "workspace",
                "runtime": "exec",
                "config": { "command": ["sleep", "600"] },
                "resources": { "cpu": 500, "memory": 512 },
                "stateful": {
                    "mode": "session",
                    "envelope": { "resources": { "cpu": 30000, "memory": 61440 } },
                },
            },
        });
        let config: JobConfig = serde_json::from_value(spec)?;
        assert_eq!(config.tasks.len(), 2);
        crate::task_plugins::validate_job_config(&config)?;
        let mut again = config.clone();
        normalize_session_tasks(&mut again)?;
        assert_eq!(again, config);
        Ok(())
    }

    #[test]
    fn the_envelope_name_and_runtime_are_reserved_test() {
        let mut config = job(vec![
            session_task(),
            TaskConfig::new("workspace-envelope", "exec"),
        ]);
        let err = normalize_session_tasks(&mut config).expect_err("name");
        assert!(err.to_string().contains("reserved"), "{err}");

        let mut config = job(vec![TaskConfig::new("mine", ENVELOPE_RUNTIME)]);
        let err = normalize_session_tasks(&mut config).expect_err("runtime");
        assert!(err.to_string().contains("reserved"), "{err}");
    }

    #[test]
    fn envelope_and_promote_need_session_mode_test() {
        let mut task = session_task();
        task.stateful.as_mut().unwrap().mode = InstanceMode::Slot;
        let err = normalize_session_tasks(&mut job(vec![task])).expect_err("slot");
        assert!(err.to_string().contains("only a session task"), "{err}");

        let mut task = session_task();
        task.stateful.as_mut().unwrap().envelope = None;
        let err = normalize_session_tasks(&mut job(vec![task])).expect_err("no envelope");
        assert!(
            err.to_string().contains("without stateful.envelope"),
            "{err}"
        );
    }

    #[test]
    fn a_session_must_fit_its_envelope_test() -> R<()> {
        let mut config = job(vec![session_task()]);
        normalize_session_tasks(&mut config)?;
        validate_session_task(&config.tasks[0])?;

        let mut task = config.tasks[0].clone();
        task.resources = json!({ "cpu": 500, "memory": 99999 });
        let err = validate_session_task(&task).expect_err("too big");
        assert!(err.to_string().contains("no session fits"), "{err}");

        let mut task = config.tasks[0].clone();
        task.resources = json!({ "cpu": 500, "memory": 512, "disk": 10 });
        let err = validate_session_task(&task).expect_err("undeclared");
        assert!(err.to_string().contains("does not set disk"), "{err}");

        let mut task = config.tasks[0].clone();
        task.stateful.as_mut().unwrap().require_id = true;
        let err = validate_session_task(&task).expect_err("requireId");
        assert!(err.to_string().contains("keyed instance"), "{err}");

        let mut task = config.tasks[0].clone();
        task.stateful.as_mut().unwrap().envelope = Some(SessionEnvelopeConfig {
            resources: json!({ "memory": 61440 }),
            constraints: json!({}),
        });
        let err = validate_session_task(&task).expect_err("no cpu");
        assert!(err.to_string().contains("does not set cpu"), "{err}");
        Ok(())
    }

    #[test]
    fn derived_ids_are_stable_minted_and_distinct_test() {
        let a = derived_session_id("workspace", "agents", "acc-1:key-1");
        assert_eq!(a, derived_session_id("workspace", "agents", "acc-1:key-1"));
        assert!(crate::structs::is_minted_for_task(&a, "workspace"), "{a}");
        assert_ne!(a, derived_session_id("workspace", "agents", "acc-2:key-1"));
        assert_ne!(a, derived_session_id("workspace", "other", "acc-1:key-1"));
        assert_ne!(a, derived_session_id("workspace", "agents", "acc-1:key-2"));
    }
}
