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
//!
//! An envelope with `envelope.max` is **elastic** (elastic-session-envelopes
//! plan): its allocation starts at `envelope.resources`, and its node grows
//! and shrinks it between the two ([`ElasticBounds`],
//! `src/worker/envelope_elastic.rs`), each resize an in-place update of the
//! allocation row (`src/storage/store/envelope_ops.rs`).

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
#[serde(rename_all = "camelCase")]
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
    /// The ceiling of an elastic envelope (elastic-session-envelopes plan
    /// §6.1): the envelope starts at `resources`, its floor, and each node
    /// grows its own envelope up to this as its sessions fill it, then gives
    /// room back after `shrink_after`. Sizes exactly the keys `resources`
    /// sizes, each at or above the floor. Absent (or equal to `resources`):
    /// a fixed envelope.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub max: Value,
    /// How long an elastic envelope's sessions must use less than half of
    /// it before the node shrinks it. Default [`DEFAULT_SHRINK_AFTER`], at
    /// least [`MIN_SHRINK_AFTER`]. Only with `max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shrink_after: Option<String>,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

/// Default `stateful.envelope.shrinkAfter`.
pub const DEFAULT_SHRINK_AFTER: std::time::Duration = std::time::Duration::from_secs(600);

/// Shortest `stateful.envelope.shrinkAfter`. Every shrink is a raft entry,
/// and a shorter wait shrinks and regrows an envelope on ordinary churn.
pub const MIN_SHRINK_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// An elastic envelope's bounds, read from its session task
/// (`stateful.envelope`).
#[derive(Debug, Clone, PartialEq)]
pub struct ElasticBounds {
    /// `envelope.resources`: what the envelope always holds.
    pub floor: BTreeMap<String, f64>,
    /// `envelope.max`: what it may grow to. Same keys as `floor`.
    pub max: BTreeMap<String, f64>,
    /// `envelope.shrinkAfter`.
    pub shrink_after: std::time::Duration,
}

impl ElasticBounds {
    /// Whether `size` sizes exactly the bounded keys, each within its bounds.
    pub fn admits(&self, size: &BTreeMap<String, f64>) -> bool {
        size.len() == self.floor.len()
            && self.floor.iter().all(|(key, floor)| {
                size.get(key).is_some_and(|amount| {
                    *amount >= *floor && *amount <= self.max.get(key).copied().unwrap_or(*floor)
                })
            })
    }
}

/// The bounds of a session task's elastic envelope. `None` for a fixed
/// envelope (no `max`, or `max` equal to `resources`), for a task that is
/// not a session task, and for bounds `validate_session_task` would refuse.
pub fn elastic_bounds(task: &TaskConfig) -> Option<ElasticBounds> {
    let envelope = task.stateful.as_ref()?.envelope.as_ref()?;
    if envelope.max.is_null() {
        return None;
    }
    let floor = resource_amounts(&envelope.resources);
    let max = resource_amounts(&envelope.max);
    let keys_match = floor.len() == max.len() && floor.keys().all(|key| max.contains_key(key));
    if !keys_match
        || floor
            .iter()
            .any(|(key, amount)| max.get(key).is_some_and(|ceiling| ceiling < amount))
        || floor == max
    {
        return None;
    }
    let shrink_after = envelope
        .shrink_after
        .as_deref()
        .and_then(|raw| humantime::parse_duration(raw.trim()).ok())
        .unwrap_or(DEFAULT_SHRINK_AFTER)
        .max(MIN_SHRINK_AFTER);
    Some(ElasticBounds {
        floor,
        max,
        shrink_after,
    })
}

/// How a manager's session router picks the node a session starts on
/// (`stateful.placement`). The node stays authoritative either way: a pick
/// that turns out full costs a retry on the next node, never an over-full
/// envelope.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SessionPlacement {
    /// The node with the most room left, counting the router's own starts
    /// still in flight, with ties broken at random. Spreads sessions as
    /// evenly as the router's view allows. Each pick reads every node that
    /// holds an envelope for the task.
    #[default]
    Spread,
    /// Power of two choices: the roomier of two nodes sampled at random from
    /// the ones with room. A pick costs the same on ten nodes or ten
    /// thousand, and managers sharing a fleet rarely pick the same node, for
    /// a less even spread. Falls back to `spread` when the samples find no
    /// room, which happens only near a full fleet.
    Fast,
}

/// When a session becomes a keyed instance (`stateful.promote`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionPromoteConfig {
    /// Whether a caller-chosen id is accepted, on `instance create --id` (a
    /// keyed create) or `instance promote --id` (a rename). Default true.
    /// With false, a create always starts an unnamed session and a promotion
    /// keeps the session's own id.
    #[serde(default = "default_true")]
    pub on_name: bool,
    /// Keys this block did not recognize, captured for the submit's
    /// unknown-field check (`structs::spec_fields`); empty once it ran.
    #[serde(flatten)]
    pub unknown_fields: crate::structs::spec_fields::UnknownFields,
}

impl Default for SessionPromoteConfig {
    fn default() -> Self {
        SessionPromoteConfig {
            on_name: true,
            unknown_fields: Default::default(),
        }
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

/// Whether a session task's sessions publish their checkpoints to the
/// content store (`suspend.durability = "store"`, session durability plan
/// §6.6): such sessions can resume on any node, claim a generation on every
/// resume, and fence themselves when cut off.
pub fn publishes_checkpoints(task: &TaskConfig) -> bool {
    task.suspend
        .as_ref()
        .and_then(|suspend| suspend.resolve().ok())
        .is_some_and(|suspend| {
            suspend.durability == crate::structs::snapshot::SnapshotDurability::Store
        })
}

/// Default `stateful.fenceAfter`.
pub const DEFAULT_FENCE_AFTER: std::time::Duration = std::time::Duration::from_secs(120);

/// A session task's `stateful.fenceAfter` (default [`DEFAULT_FENCE_AFTER`]).
pub fn fence_after(task: &TaskConfig) -> std::time::Duration {
    task.stateful
        .as_ref()
        .and_then(|stateful| stateful.fence_after.as_deref())
        .and_then(|raw| humantime::parse_duration(raw.trim()).ok())
        .filter(|after| !after.is_zero())
        .unwrap_or(DEFAULT_FENCE_AFTER)
}

/// How a session task's router picks nodes (`stateful.placement`).
pub fn placement(task: &TaskConfig) -> SessionPlacement {
    task.stateful
        .as_ref()
        .and_then(|stateful| stateful.placement)
        .unwrap_or_default()
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
                if stateful.placement.is_some() {
                    bail!(
                        "task {} sets stateful.placement, which only a session task has: it picks \
                         the node a session starts on\nHint: set stateful.mode = \"session\", or \
                         remove placement (the scheduler places every other task)",
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

/// Resource amounts as a `resources` object, whole amounts written as
/// integers (`{"cpu":6000,"memory":6144}`), the shape a jobspec declares and
/// an envelope's allocation row stores.
pub fn amounts_json(amounts: &BTreeMap<String, f64>) -> Value {
    Value::Object(
        amounts
            .iter()
            .map(|(key, amount)| {
                let value = if amount.fract() == 0.0 && amount.abs() < 9.0e15 {
                    json!(*amount as i64)
                } else {
                    json!(amount)
                };
                (key.clone(), value)
            })
            .collect(),
    )
}

/// Submit-time checks for a (normalized) session task. Called from
/// `TaskConfig::validate_stateful` for every stateful task; a task that is
/// not a session task passes.
pub fn validate_session_task(task: &TaskConfig) -> R<()> {
    let Some(stateful) = task.stateful.as_ref() else {
        return Ok(());
    };
    let name = &task.name;
    let Some(envelope) = stateful.envelope.as_ref() else {
        if stateful.fence_after.is_some() {
            bail!(
                "task {name} sets stateful.fenceAfter, which applies to session tasks only \
                 (stateful.mode = \"session\" with suspend.durability = \"store\")\nHint: \
                 remove stateful.fenceAfter; keyed and slot instances fence with \
                 stateful.onLost.selfTerminateAfter"
            );
        }
        return Ok(());
    };
    if let Some(raw) = stateful.fence_after.as_deref() {
        let after = humantime::parse_duration(raw.trim()).map_err(|err| {
            anyhow!(
                "task {name}: stateful.fenceAfter {raw:?} is not a duration ({err})\nHint: use \
                 a duration such as \"2m\""
            )
        })?;
        if after < std::time::Duration::from_secs(10) {
            bail!(
                "task {name}: stateful.fenceAfter {raw:?} is shorter than 10s. A node fences its \
                 sessions after this long without reaching a manager or its content store, so \
                 a value this short suspends sessions on every brief network blip\nHint: use \
                 a duration such as \"2m\" (the default)"
            );
        }
        if !publishes_checkpoints(task) {
            bail!(
                "task {name} sets stateful.fenceAfter without suspend.durability = \"store\". \
                 Only a session that can move (its checkpoint is in the content store) can \
                 have a newer copy elsewhere, so only those fence themselves\nHint: set \
                 suspend = {{ durability = \"store\" }}, or remove stateful.fenceAfter"
            );
        }
    }
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
    validate_elastic_envelope(name, envelope, &room)?;
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
        // `durability = "local"` is valid (session durability plan §6.6): an
        // unpromoted session sleeps on its own node, pinned until
        // `suspend.keepFor`, and a promoted one is a keyed instance whose
        // local checkpoint resumes on its node like any other.
        if resolved.reserve != crate::structs::suspend::SuspendReserve::NONE {
            bail!(
                "task {name} is a session task and sets suspend.reserve. The envelope is the \
                 reservation\nHint: remove suspend.reserve"
            );
        }
    }
    Ok(())
}

/// Submit-time checks for `envelope.max` and `envelope.shrinkAfter`
/// (elastic-session-envelopes plan §6.1). `floor` is `envelope.resources`.
fn validate_elastic_envelope(
    name: &str,
    envelope: &SessionEnvelopeConfig,
    floor: &BTreeMap<String, f64>,
) -> R<()> {
    if envelope.max.is_null() {
        if envelope.shrink_after.is_some() {
            bail!(
                "task {name} sets stateful.envelope.shrinkAfter without envelope.max. A fixed \
                 envelope never shrinks\nHint: set envelope.max to let each node grow and shrink \
                 its envelope, or remove shrinkAfter"
            );
        }
        return Ok(());
    }
    let Some(max) = envelope.max.as_object() else {
        bail!(
            "task {name}: stateful.envelope.max must be an object of amounts, like \
             envelope.resources\nHint: max = {{ cpu = 30000, memory = 61440 }}"
        );
    };
    let floor_keys = floor.keys().cloned().collect::<Vec<_>>().join(", ");
    for (key, value) in max {
        let key = if key == "cpus" { "cpu" } else { key.as_str() };
        if value.as_f64().is_none() {
            bail!(
                "task {name}: stateful.envelope.max.{key} = {value} is not an amount. An \
                 envelope grows by amount\nHint: set max.{key} to a number"
            );
        }
        if !floor.contains_key(key) {
            bail!(
                "task {name}: stateful.envelope.max must size exactly the keys \
                 envelope.resources sizes ({floor_keys}), and it sizes {key}\nHint: remove \
                 max.{key}, or add resources.{key} as its floor"
            );
        }
    }
    let ceiling = resource_amounts(&envelope.max);
    for (key, amount) in floor {
        match ceiling.get(key) {
            None => bail!(
                "task {name}: stateful.envelope.max must size exactly the keys \
                 envelope.resources sizes ({floor_keys}), and it does not size {key}\nHint: set \
                 max.{key}, at or above resources.{key}"
            ),
            Some(max) if max < amount => bail!(
                "task {name}: stateful.envelope.max.{key} = {max} is below the floor \
                 envelope.resources.{key} = {amount}\nHint: raise max.{key}, or lower \
                 resources.{key}"
            ),
            Some(_) => {}
        }
    }
    if let Some(raw) = envelope.shrink_after.as_deref() {
        let after = humantime::parse_duration(raw.trim()).map_err(|err| {
            anyhow!(
                "task {name}: stateful.envelope.shrinkAfter {raw:?} is not a duration \
                 ({err})\nHint: use a duration such as \"10m\" (the default)"
            )
        })?;
        if after < MIN_SHRINK_AFTER {
            bail!(
                "task {name}: stateful.envelope.shrinkAfter {raw:?} is shorter than 1m. Every \
                 shrink is a raft entry, and a shorter wait shrinks and regrows an envelope on \
                 ordinary churn\nHint: use a duration such as \"10m\" (the default)"
            );
        }
    }
    Ok(())
}

// Unknown-field capture (`structs::spec_fields`).
crate::structs::spec_fields::spec_block!(
    SessionEnvelopeConfig,
    ["resources", "constraints", "max", "shrinkAfter"]
);
crate::structs::spec_fields::spec_block!(SessionPromoteConfig, ["onName"]);

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
                ..Default::default()
            }),
            promote: None,
            placement: None,
            fence_after: None,
            unknown_fields: Default::default(),
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
            pending_extensions: Default::default(),
            extensions: Default::default(),
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
    fn placement_is_a_session_task_field_test() -> R<()> {
        let mut task = session_task();
        task.stateful.as_mut().unwrap().placement = Some(SessionPlacement::Fast);
        let mut config = job(vec![task]);
        normalize_session_tasks(&mut config)?;
        assert_eq!(placement(&config.tasks[0]), SessionPlacement::Fast);
        assert_eq!(placement(&session_task()), SessionPlacement::Spread);

        let mut task = session_task();
        let stateful = task.stateful.as_mut().unwrap();
        stateful.mode = InstanceMode::Keyed;
        stateful.envelope = None;
        stateful.placement = Some(SessionPlacement::Spread);
        let err = normalize_session_tasks(&mut job(vec![task])).expect_err("keyed");
        assert!(err.to_string().contains("sets stateful.placement"), "{err}");

        let parsed: crate::structs::StatefulConfig =
            serde_json::from_value(json!({ "mode": "session", "placement": "fast" }))?;
        assert_eq!(parsed.placement, Some(SessionPlacement::Fast));
        assert!(serde_json::from_value::<crate::structs::StatefulConfig>(
            json!({ "mode": "session", "placement": "bin_pack" })
        )
        .is_err());
        Ok(())
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
            ..Default::default()
        });
        let err = validate_session_task(&task).expect_err("no cpu");
        assert!(err.to_string().contains("does not set cpu"), "{err}");
        Ok(())
    }

    /// Session durability plan §6.6: a session may sleep on its own node.
    #[test]
    fn a_session_may_suspend_locally_but_not_reserve_test() -> R<()> {
        let mut config = job(vec![session_task()]);
        normalize_session_tasks(&mut config)?;
        let mut task = config.tasks[0].clone();
        task.suspend = Some(serde_json::from_value(json!({
            "durability": "local",
            "idle": { "after": "3m" },
            "keepFor": "2h",
            "onWorkerStop": "checkpoint",
        }))?);
        validate_session_task(&task)?;
        crate::structs::suspend::validate_suspend(&task).map_err(|err| anyhow!(err))?;

        task.suspend = Some(serde_json::from_value(json!({ "reserve": 0.5 }))?);
        let err = validate_session_task(&task).expect_err("reserve");
        assert!(err.to_string().contains("envelope is the"), "{err}");
        Ok(())
    }

    /// Elastic-session-envelopes plan §6.1: `max` and `shrinkAfter`.
    #[test]
    fn an_elastic_envelope_sizes_the_floor_keys_within_bounds_test() -> R<()> {
        let mut config = job(vec![session_task()]);
        normalize_session_tasks(&mut config)?;
        let base = config.tasks[0].clone();
        assert_eq!(elastic_bounds(&base), None, "no max: fixed");
        let with = |max: Value, shrink_after: Option<&str>| {
            let mut task = base.clone();
            let envelope = task.stateful.as_mut().unwrap().envelope.as_mut().unwrap();
            envelope.max = max;
            envelope.shrink_after = shrink_after.map(str::to_string);
            task
        };

        let task = with(json!({ "cpu": 60000, "memory": 122880 }), None);
        validate_session_task(&task)?;
        let bounds = elastic_bounds(&task).expect("elastic");
        assert_eq!(bounds.shrink_after, DEFAULT_SHRINK_AFTER);
        assert_eq!(bounds.floor["cpu"], 30000.0);
        assert_eq!(bounds.max["memory"], 122880.0);
        assert!(bounds.admits(&resource_amounts(&json!({ "cpu": 45000, "memory": 61440 }))));
        assert!(!bounds.admits(&resource_amounts(&json!({ "cpu": 70000, "memory": 61440 }))));
        assert!(!bounds.admits(&resource_amounts(&json!({ "cpu": 45000 }))));

        // `max` equal to the floor is a fixed envelope.
        let task = with(json!({ "cpu": 30000, "memory": 61440 }), None);
        validate_session_task(&task)?;
        assert_eq!(elastic_bounds(&task), None);

        let task = with(json!({ "cpu": 60000, "memory": 122880 }), Some("30m"));
        validate_session_task(&task)?;
        assert_eq!(
            elastic_bounds(&task).unwrap().shrink_after,
            std::time::Duration::from_secs(1800)
        );

        for (max, shrink_after, needle) in [
            (json!({ "cpu": 60000 }), None, "does not size memory"),
            (
                json!({ "cpu": 60000, "memory": 122880, "disk": 10 }),
                None,
                "it sizes disk",
            ),
            (
                json!({ "cpu": 20000, "memory": 122880 }),
                None,
                "below the floor",
            ),
            (json!({ "cpu": "lots", "memory": 1 }), None, "not an amount"),
            (json!(7), None, "must be an object"),
            (
                json!({ "cpu": 60000, "memory": 122880 }),
                Some("30s"),
                "shorter than 1m",
            ),
            (
                json!({ "cpu": 60000, "memory": 122880 }),
                Some("soon"),
                "not a duration",
            ),
            (Value::Null, Some("10m"), "without envelope.max"),
        ] {
            let err = validate_session_task(&with(max.clone(), shrink_after))
                .expect_err(&format!("{max} {shrink_after:?}"));
            assert!(err.to_string().contains(needle), "{needle}: {err}");
        }
        Ok(())
    }

    #[test]
    fn a_fixed_envelope_spec_serializes_as_before_test() -> R<()> {
        let envelope = SessionEnvelopeConfig {
            resources: json!({ "cpu": 1 }),
            constraints: json!({}),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&envelope)?,
            json!({ "resources": { "cpu": 1 }, "constraints": {} })
        );
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
