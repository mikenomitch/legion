//! Stable instance identity for stateful workloads.
//!
//! See `internal_docs/plans/stateful-workloads-and-instance-identity-plan.md` and
//! `docs/stateful-workloads.md`. An [`Instance`] is a logical entity one level
//! above an allocation: it owns a stable id that survives reschedules, binds to
//! persistent state via an opaque `storage_handle`, and feeds the
//! reschedule-stable workload-identity `sub` (see
//! `crate::structs::workload_identity`).
//!
//! Allocations are ephemeral placements *of* an instance: an instance has at
//! most one live allocation at a time but many over its life.

use serde::{Deserialize, Serialize};

/// How instance ids are assigned for a stateful task.
///
/// The recycle policy is encoded in the mode on purpose: slot ids are reused
/// within a fixed pool (replica 3 coming back must be replica 3), while keyed
/// ids are never reused so a new sandbox can never inherit a dead one's
/// identity or state.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum InstanceMode {
    /// Fixed pool of ordinal slots `0..count.num-1` (StatefulSet style). Slot
    /// ids are deterministic and reused across reschedules and scale cycles.
    /// The default: `stateful: { storage: { path: … } }` alone gives a slot
    /// pool, which is the StatefulSet-shaped common case; keyed mode is the
    /// one that changes the whole management model and so must be asked for.
    #[default]
    Slot,
    /// Instances created on demand via the CreateInstance/DestroyInstance
    /// management RPCs with never-recycled ids (sandbox / dispatch style):
    /// a caller id is kept as a tombstone after destroy, and a server-minted
    /// id is never reused because it is 130 random bits in a shape callers
    /// may not choose.
    /// `count.num` does not drive keyed tasks: live instances do.
    Keyed,
    /// Session mode (`docs/session-mode.md`): unnamed instances run inside a
    /// per-node envelope with no row in the store, and a caller id or a
    /// promotion makes one keyed. Only ever seen in a jobspec as written:
    /// parsing normalizes a session task into a keyed task that keeps its
    /// `stateful.envelope` block, plus its generated envelope task (see
    /// [`crate::structs::session::normalize_session_tasks`]), so a stored
    /// config never carries it.
    Session,
}

impl InstanceMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceMode::Slot => "slot",
            InstanceMode::Keyed => "keyed",
            InstanceMode::Session => "session",
        }
    }
}

impl std::fmt::Display for InstanceMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for InstanceMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "slot" => Ok(InstanceMode::Slot),
            "keyed" => Ok(InstanceMode::Keyed),
            other => Err(format!("Unknown instance mode {other}")),
        }
    }
}

/// Lifecycle state of an instance.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    /// Created but never placed (no allocation yet).
    Pending,
    /// Has a live allocation fulfilling it.
    Active,
    /// Lost its allocation and is awaiting a replacement that will reuse the
    /// same instance id and storage handle.
    Rescheduling,
    /// Keyed only: paused via StopInstance (or the whole job being stopped).
    /// The allocation is stopped but the row AND storage handle are retained,
    /// and the id stays owned (creating it again still collides). Not desired
    /// by the scheduler; StartInstance moves it back to `Pending`. Unlike
    /// `Terminated` this is fully reversible.
    Stopped,
    /// Keyed only: checkpointed and stopped by SuspendInstance. Like
    /// `Stopped` it holds no allocation (unless `suspend.reserve` keeps its
    /// reservation) and does not count toward desired. Unlike `Stopped`, its
    /// `checkpoint_id` names a snapshot, and StartInstance restores from it
    /// instead of cold-booting (see docs/suspend-and-resume.md).
    Suspended,
    /// No longer desired (scaled in, job stopped, or destroyed). Slot-mode
    /// rows are retained so a later scale-out re-materializes the same id and
    /// reattaches the retained storage handle; named keyed rows are retained
    /// for the job's life so their ids are never recycled. An unnamed
    /// (server-minted) keyed row is deleted on destroy instead: its id is
    /// unique by randomness and reserved in shape ([`is_minted_instance_id`]).
    Terminated,
}

impl InstanceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceState::Pending => "pending",
            InstanceState::Active => "active",
            InstanceState::Rescheduling => "rescheduling",
            InstanceState::Stopped => "stopped",
            InstanceState::Suspended => "suspended",
            InstanceState::Terminated => "terminated",
        }
    }
}

impl std::fmt::Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for InstanceState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(InstanceState::Pending),
            "active" => Ok(InstanceState::Active),
            "rescheduling" => Ok(InstanceState::Rescheduling),
            "stopped" => Ok(InstanceState::Stopped),
            "suspended" => Ok(InstanceState::Suspended),
            "terminated" => Ok(InstanceState::Terminated),
            other => Err(format!("Unknown instance state {other}")),
        }
    }
}

/// A stable instance of a stateful task. Persisted in the `instance` table;
/// allocations reference it via `Allocation::instance_id`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    /// Stable, unique within (job, group); survives reschedules. In slot mode
    /// this is the deterministic `{job_id}-{group}-{slot}` id (see
    /// [`slot_instance_id`]); in keyed mode it is a caller-supplied key or a
    /// server-minted id.
    pub instance_id: String,
    pub job_id: String,
    pub group: String,
    /// The task this instance currently belongs to (informational; identity is
    /// keyed by `instance_id`).
    pub task_name: String,
    pub mode: InstanceMode,
    pub state: InstanceState,
    /// Ordinal slot `0..count.num-1`; `Some` only in slot mode.
    pub slot_index: Option<u32>,
    /// The allocation currently fulfilling this instance, if any.
    pub current_alloc_id: Option<i64>,
    /// Opaque reference to bound persistent state. Minted on first placement
    /// when the task declares `stateful.storage` and reattached on every
    /// subsequent allocation. Concrete drivers are a follow-up (see
    /// `internal_docs/plans/volume-plugin-system-plan.md`).
    pub storage_handle: Option<String>,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 timestamp of the last state/binding change.
    pub updated_at: String,
    /// The snapshot this instance is restoring from (RestoreInstance), until
    /// its node reports the outcome. Written only by the snapshot restore
    /// ops, never by a generic upsert (see `INSTANCE_SELECT_ATTRS`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_from: Option<String>,
    /// The workload-identity epoch its tokens carry (`legion.io/epoch`).
    /// Raised when a fork or snapshot of the instance is captured, so
    /// Legion's own verifiers refuse the tokens copied into the checkpoint
    /// (docs/workload-identity.md, "Forks and snapshots"). Written only by
    /// the capture ops, never by a generic upsert.
    #[serde(default)]
    pub identity_epoch: u64,
    /// Whether the id is a name: caller-chosen, a slot ordinal, or derived
    /// from a named origin. A named keyed instance keeps a `terminated`
    /// tombstone after destroy so its id is never reused. An unnamed one
    /// (server-minted, [`mint_instance_id`]) is unique by 130 random bits
    /// and its row is deleted on destroy (sandbox-scaling plan, Phase 4).
    #[serde(default = "named_default")]
    pub named: bool,
    /// The session this instance was promoted from (PromoteInstance,
    /// docs/session-mode.md). Written once, when the row is inserted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_from: Option<String>,
}

fn named_default() -> bool {
    true
}

/// Why an instance's last allocation stopped (`instance.last_stop`, JSON).
/// Served as `InstanceInfo.last_stop`, so "stopped by node pressure" reads
/// on the instance row without `operator:read` on events (suspend/resume UI
/// plan §7.3).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct InstanceLastStop {
    /// Stop-cause wire form ([`crate::structs::stop::StopCause::as_str`]).
    pub cause: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hint: String,
    /// Unix seconds.
    pub at: i64,
    pub allocation_id: i64,
    pub node_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub preemptor_job: String,
}

impl InstanceLastStop {
    /// The record for a stop the manager decided, with a sentence built from
    /// the cause. The pressure path replaces `detail` and `hint` with the
    /// worker's own words.
    pub fn from_cause(
        cause: crate::structs::stop::StopCause,
        allocation_id: i64,
        node_id: &str,
        preemptor_job: Option<&str>,
        at: i64,
    ) -> Self {
        use crate::structs::stop::StopCause;
        let detail = match cause {
            StopCause::Stop => "Stopped by a user (instance stop or job stop).".to_string(),
            StopCause::Update => "Replaced by a newer job config.".to_string(),
            StopCause::Drain => format!("Moved off node {node_id}, which is draining."),
            StopCause::Reschedule => "Rescheduled by the manager.".to_string(),
            StopCause::Destroy => "Destroyed.".to_string(),
            StopCause::NodeLost => format!("Node {node_id} was lost or removed."),
            StopCause::ScaleIn => "Removed by a scale-in.".to_string(),
            StopCause::Preempted => match preemptor_job {
                Some(job) if !job.is_empty() => {
                    format!("Preempted to make room for higher-priority job {job}.")
                }
                _ => "Preempted to make room for higher-priority work.".to_string(),
            },
            StopCause::NodePressure => {
                format!("Stopped by node {node_id}'s memory pressure response.")
            }
            StopCause::Restore => "Replaced to roll back to a snapshot.".to_string(),
            StopCause::Suspended => "Suspended with its state checkpointed.".to_string(),
            StopCause::Unknown => "Stopped.".to_string(),
        };
        Self {
            cause: cause.as_str().to_string(),
            detail,
            hint: String::new(),
            at,
            allocation_id,
            node_id: node_id.to_string(),
            preemptor_job: preemptor_job.unwrap_or_default().to_string(),
        }
    }
}

/// Deterministic slot-mode instance id: `{job_id}-{group}-{slot}`.
///
/// Recomputable and human-legible so replica N's identity (and its storage
/// binding and workload-identity `sub`) is stable across reschedules, config
/// updates, and scale cycles.
pub fn slot_instance_id(job_id: &str, group: &str, slot: u32) -> String {
    format!("{job_id}-{group}-{slot}")
}

/// Maximum length of a keyed instance id.
pub const KEYED_INSTANCE_ID_MAX_LEN: usize = 128;

/// Length of the random part of a server-minted instance id: 26 base32
/// characters, 130 bits (sandbox-scaling plan, Phase 4). At a trillion
/// minted ids the chance that any two collide is about 10^-15, which is what
/// lets an unnamed instance leave no tombstone on destroy.
pub const MINTED_ID_RANDOM_LEN: usize = 26;

/// The base32 alphabet minted ids draw from (RFC 4648, lowercase).
const MINTED_ID_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// Mint an unnamed instance id: `{task}-{26 base32 chars}`.
///
/// Ids of this shape are reserved: [`validate_keyed_instance_id`] refuses
/// them as caller ids, so an id of this shape can only ever come from a mint,
/// and nobody can re-create a destroyed unnamed instance by typing its id.
/// That is what makes "no tombstone" safe for them. A task name long enough
/// to push the id past [`KEYED_INSTANCE_ID_MAX_LEN`] is truncated, so the id
/// stays valid and keeps all 130 random bits.
pub fn mint_instance_id(task_name: &str) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random: String = (0..MINTED_ID_RANDOM_LEN)
        .map(|_| MINTED_ID_ALPHABET[rng.gen_range(0..MINTED_ID_ALPHABET.len())] as char)
        .collect();
    let max_prefix = KEYED_INSTANCE_ID_MAX_LEN - MINTED_ID_RANDOM_LEN - 1;
    let prefix: String = task_name.chars().take(max_prefix).collect();
    format!("{prefix}-{random}")
}

/// Length of the node tag a session id minted on a node carries
/// (session durability plan §6.3): 8 base32 characters, 40 bits.
pub const NODE_TAG_LEN: usize = 8;

/// The tag of a node in the session ids it mints: 8 base32 characters of a
/// SHA-256 over the node id. It does not spell the node's name, and 10^4
/// nodes collide with probability about 5 × 10^-5; a collision costs one
/// extra probe, never a wrong answer, because the node answers for itself.
pub fn node_tag(node_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"legion-node-tag\0");
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    base32_from_bits(&digest, NODE_TAG_LEN)
}

/// `len` lowercase base32 characters from the leading bits of `bytes`.
fn base32_from_bits(bytes: &[u8], len: usize) -> String {
    (0..len)
        .map(|index| {
            let bit = index * 5;
            let (byte, offset) = (bit / 8, bit % 8);
            let next = bytes.get(byte + 1).copied().unwrap_or(0);
            let window = (u16::from(bytes[byte]) << 8) | u16::from(next);
            let value = (window >> (11 - offset)) & 0x1f;
            MINTED_ID_ALPHABET[value as usize] as char
        })
        .collect()
}

/// Mint a session id on (or for) a node: `{task}-{node tag}.{26 base32}`.
///
/// The tag lets anything holding the id route straight to the node that
/// runs it, with no lookup and no write anywhere (session durability plan
/// §6.3). The 130 random bits are the same as [`mint_instance_id`]'s, and
/// the shape is reserved the same way.
pub fn mint_session_id(task_name: &str, node_id: &str) -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random: String = (0..MINTED_ID_RANDOM_LEN)
        .map(|_| MINTED_ID_ALPHABET[rng.gen_range(0..MINTED_ID_ALPHABET.len())] as char)
        .collect();
    let max_prefix = KEYED_INSTANCE_ID_MAX_LEN - MINTED_ID_RANDOM_LEN - NODE_TAG_LEN - 2;
    let prefix: String = task_name.chars().take(max_prefix).collect();
    format!("{prefix}-{}.{random}", node_tag(node_id))
}

fn is_base32(raw: &str, len: usize) -> bool {
    raw.len() == len && raw.bytes().all(|b| MINTED_ID_ALPHABET.contains(&b))
}

/// The minted suffix of `id`, split: `(prefix, tag, random)`, with `tag`
/// empty for an untagged id. `None` when `id` is not minted.
fn minted_parts(id: &str) -> Option<(&str, &str, &str)> {
    let (prefix, suffix) = id.rsplit_once('-')?;
    if prefix.is_empty() {
        return None;
    }
    match suffix.split_once('.') {
        None => is_base32(suffix, MINTED_ID_RANDOM_LEN).then_some((prefix, "", suffix)),
        Some((tag, random)) => (is_base32(tag, NODE_TAG_LEN)
            && is_base32(random, MINTED_ID_RANDOM_LEN))
        .then_some((prefix, tag, random)),
    }
}

/// Whether `id` has the reserved shape of a server-minted id: a non-empty
/// prefix, a `-`, and exactly [`MINTED_ID_RANDOM_LEN`] characters of the
/// lowercase base32 alphabet at the end, optionally preceded by a
/// [`NODE_TAG_LEN`]-character node tag and a `.`.
pub fn is_minted_instance_id(id: &str) -> bool {
    minted_parts(id).is_some()
}

/// The node tag a session id carries, when it was minted on a node.
pub fn session_node_tag(id: &str) -> Option<&str> {
    minted_parts(id)
        .map(|(_, tag, _)| tag)
        .filter(|tag| !tag.is_empty())
}

/// Whether `id` is a server-minted id of task `task_name`: the minted shape
/// with the prefix [`mint_instance_id`] (or [`mint_session_id`]) gives that
/// task.
pub fn is_minted_for_task(id: &str, task_name: &str) -> bool {
    let Some((prefix, tag, _)) = minted_parts(id) else {
        return false;
    };
    let tag_len = if tag.is_empty() { 0 } else { NODE_TAG_LEN + 1 };
    let max_prefix = KEYED_INSTANCE_ID_MAX_LEN - MINTED_ID_RANDOM_LEN - 1 - tag_len;
    prefix.chars().eq(task_name.chars().take(max_prefix))
}

/// Validate a keyed instance id: `[A-Za-z0-9._-]{1,128}`.
///
/// The charset is deliberately conservative — keyed ids flow into env vars
/// (`LEGION_INSTANCE_ID`), workload-identity subjects, file-ish storage
/// handles, and CLI arguments, so anything beyond URL-safe punctuation is a
/// foot-gun. Enforced for caller-supplied ids on CreateInstance; server-minted
/// ids satisfy it by construction.
pub fn validate_keyed_instance_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("keyed instance id cannot be empty".to_string());
    }
    if id.len() > KEYED_INSTANCE_ID_MAX_LEN {
        return Err(format!(
            "keyed instance id is {} characters; max is {KEYED_INSTANCE_ID_MAX_LEN}",
            id.len()
        ));
    }
    if let Some(bad) = id
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(format!(
            "keyed instance id contains invalid character {bad:?}; allowed: [A-Za-z0-9._-]"
        ));
    }
    if is_minted_instance_id(id) {
        return Err(format!(
            "keyed instance id {id} has the reserved shape of a server-minted id \
             (`<prefix>-<{MINTED_ID_RANDOM_LEN} lowercase base32 characters>`); minted ids leave \
             no tombstone, so a caller may not choose one. Pick an id that does not end in \
             {MINTED_ID_RANDOM_LEN} characters of [a-z2-7] after its last `-`, or omit the id \
             to have one minted"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::R;

    #[test]
    fn slot_instance_id_is_deterministic_and_stable() {
        let first = slot_instance_id("db", "default", 2);
        let second = slot_instance_id("db", "default", 2);
        assert_eq!(first, "db-default-2");
        assert_eq!(first, second);
        assert_ne!(first, slot_instance_id("db", "default", 3));
        assert_ne!(first, slot_instance_id("db", "replicas", 2));
        assert_ne!(first, slot_instance_id("cache", "default", 2));
    }

    #[test]
    fn keyed_instance_id_validation_enforces_charset_and_length() {
        assert!(validate_keyed_instance_id("sandbox-user.42_A").is_ok());
        assert!(validate_keyed_instance_id(&"a".repeat(KEYED_INSTANCE_ID_MAX_LEN)).is_ok());

        assert!(validate_keyed_instance_id("").is_err());
        assert!(validate_keyed_instance_id(&"a".repeat(KEYED_INSTANCE_ID_MAX_LEN + 1)).is_err());
        for bad in ["has space", "slash/y", "emoji-✨", "colon:id", "at@id"] {
            assert!(
                validate_keyed_instance_id(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn minted_ids_have_the_reserved_shape_and_callers_cannot_use_it() {
        let id = mint_instance_id("workspace");
        assert!(id.starts_with("workspace-"), "{id}");
        assert_eq!(id.len(), "workspace-".len() + MINTED_ID_RANDOM_LEN);
        assert!(is_minted_instance_id(&id));
        assert_ne!(id, mint_instance_id("workspace"), "two mints differ");
        // A caller may not choose the shape...
        let err = validate_keyed_instance_id(&id).unwrap_err();
        assert!(err.contains("reserved shape"), "{err}");
        // ...but every other id stays valid, including near misses.
        for ok in [
            "user-42",
            "workspace-abcdefghijklmnopqrstuvwxy", // 25 characters
            "workspace-abcdefghijklmnopqrstuvwxyz2", // 27 characters
            "workspace-ABCDEFGHIJKLMNOPQRSTUVWXYZ", // uppercase
            "workspace-abcdefghijklmnopqrstuvwx18", // 1 and 8 are not base32
            "abcdefghijklmnopqrstuvwxyz",          // no prefix
        ] {
            assert!(validate_keyed_instance_id(ok).is_ok(), "{ok:?}");
        }
        // A task name too long to fit is truncated, never the random part.
        let long = mint_instance_id(&"t".repeat(200));
        assert_eq!(long.len(), KEYED_INSTANCE_ID_MAX_LEN);
        assert!(is_minted_instance_id(&long));
        // A minted id names the task that minted it.
        assert!(is_minted_for_task(&id, "workspace"));
        assert!(!is_minted_for_task(&id, "work"));
        assert!(!is_minted_for_task("user-42", "user"));
        assert!(is_minted_for_task(&long, &"t".repeat(200)));
    }

    #[test]
    fn session_ids_carry_their_node_tag_and_stay_reserved_test() {
        let id = mint_session_id("workspace", "w-17");
        let tag = node_tag("w-17");
        assert_eq!(tag.len(), NODE_TAG_LEN);
        assert_eq!(node_tag("w-17"), tag, "the tag is stable");
        assert_ne!(node_tag("w-18"), tag);
        assert!(id.starts_with(&format!("workspace-{tag}.")), "{id}");
        assert!(is_minted_instance_id(&id));
        assert_eq!(session_node_tag(&id), Some(tag.as_str()));
        assert!(is_minted_for_task(&id, "workspace"));
        assert!(
            validate_keyed_instance_id(&id).is_err(),
            "callers may not pick one"
        );
        // An untagged minted id has no tag; a near miss is not minted.
        assert_eq!(session_node_tag(&mint_instance_id("workspace")), None);
        assert!(!is_minted_instance_id("workspace-q4n7c2rd.short"));
        assert!(!is_minted_instance_id(
            "workspace-q4n7c2r.abcdefghijklmnopqrstuvwxyz"
        ));
        assert!(validate_keyed_instance_id("workspace-v1.2").is_ok());
        // A long task name is truncated so the id stays valid.
        let long = mint_session_id(&"t".repeat(200), "w-17");
        assert_eq!(long.len(), KEYED_INSTANCE_ID_MAX_LEN);
        assert!(is_minted_for_task(&long, &"t".repeat(200)));
    }

    #[test]
    fn instance_mode_round_trips_serde_and_str() -> R<()> {
        assert_eq!(serde_json::to_string(&InstanceMode::Slot)?, "\"slot\"");
        assert_eq!(serde_json::to_string(&InstanceMode::Keyed)?, "\"keyed\"");
        assert_eq!(
            serde_json::from_str::<InstanceMode>("\"slot\"")?,
            InstanceMode::Slot
        );
        assert_eq!("keyed".parse::<InstanceMode>(), Ok(InstanceMode::Keyed));
        assert_eq!(InstanceMode::Slot.to_string(), "slot");
        assert!("ordinal".parse::<InstanceMode>().is_err());
        Ok(())
    }

    #[test]
    fn instance_state_round_trips_serde_and_str() -> R<()> {
        for state in [
            InstanceState::Pending,
            InstanceState::Active,
            InstanceState::Rescheduling,
            InstanceState::Stopped,
            InstanceState::Suspended,
            InstanceState::Terminated,
        ] {
            let json = serde_json::to_string(&state)?;
            assert_eq!(json, format!("\"{state}\""));
            assert_eq!(serde_json::from_str::<InstanceState>(&json)?, state);
            assert_eq!(state.as_str().parse::<InstanceState>(), Ok(state));
        }
        assert!("gone".parse::<InstanceState>().is_err());
        Ok(())
    }
}
