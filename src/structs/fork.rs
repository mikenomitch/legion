//! Fork (suspend plan §6.9, Phase 6; docs/suspend-and-resume.md, "Fork").
//!
//! A fork is three existing pieces: a live checkpoint of the origin, N new
//! keyed instances restoring from it, and a per-fork identity. This module
//! holds the small, pure rules the manager and the worker share: how fork
//! ids are derived, how many forks one call may create, and how a forked
//! guest gets a NIC identity of its own.

use sha2::{Digest, Sha256};

/// `instance.pending_op` while a fork waits for its origin's checkpoint to
/// land. The scheduler neither counts nor binds such an instance, so it is
/// never placed against a checkpoint that does not exist yet.
pub const PENDING_FORK: &str = "fork";

/// The most instances one ForkInstance or `from_snapshot` create may make.
/// Every one of them is pinned to the node holding the checkpoint, so a
/// larger fan-out mostly queues pending placements on one node.
pub const MAX_FORK_COUNT: u32 = 64;

/// The longest instance id (see `validate_keyed_instance_id`).
const MAX_INSTANCE_ID_LEN: usize = 128;

/// The `n`th candidate id for a fork of `origin`: `<origin>-f<n>`. `None`
/// when the result would exceed the instance id length limit, in which case
/// the caller mints an id instead.
pub fn fork_instance_id(origin: &str, n: u32) -> Option<String> {
    let id = format!("{origin}-f{n}");
    (id.len() <= MAX_INSTANCE_ID_LEN).then_some(id)
}

/// A locally administered, unicast MAC for a forked guest, derived from its
/// allocation id. Allocation ids are never reused, so two forks never share
/// one. The `02:4c` prefix ("L") marks it as Legion's.
pub fn fork_mac(allocation_id: i64) -> String {
    let id = allocation_id.unsigned_abs();
    format!(
        "02:4c:{:02x}:{:02x}:{:02x}:{:02x}",
        (id >> 24) & 0xff,
        (id >> 16) & 0xff,
        (id >> 8) & 0xff,
        id & 0xff
    )
}

/// The host TAP device Legion owns for one sandbox instance: `lgf` plus 12
/// hex characters of the instance id's SHA-256, which fits Linux's
/// 15-character interface name limit.
///
/// Two features share it, so one instance always asks for one device:
///
/// - a forked sandbox's NIC is moved to it on restore, and
/// - `config.network = { mode = "tap", tap = "auto", bridge = ... }` boots
///   every instance on it.
///
/// Derived from the instance id (not the allocation) so an instance that is
/// suspended and resumed, or forked and later resumed, asks for the same
/// device again. A task without instances names it after its allocation
/// ([`allocation_tap_owner`]).
pub fn own_tap_name(instance_id: &str) -> String {
    let digest = Sha256::digest(instance_id.as_bytes());
    let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
    format!("lgf{hex}")
}

/// What [`own_tap_name`] hashes for an allocation that serves no instance.
pub fn allocation_tap_owner(allocation_id: i64) -> String {
    format!("alloc-{allocation_id}")
}

/// The host TAP device a sandbox task names by hand
/// (`config.network = { mode = "tap", tap = "tap0" }`). `None` for every
/// other task, including one on `tap = "auto"`, whose device is per instance.
///
/// A fixed device serves one guest per node, which is the predicate behind
/// [`validate_fixed_tap`] at submit and the disk-fork refusal at the API.
pub fn sandbox_fixed_tap(task: &crate::structs::TaskConfig) -> Option<&str> {
    if task.runtime != "sandbox" {
        return None;
    }
    let network = task.config.get("network")?;
    if network.get("mode").and_then(serde_json::Value::as_str) != Some("tap") {
        return None;
    }
    network
        .get("tap")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|tap| !tap.is_empty() && *tap != "auto")
}

/// Submit-time check: a sandbox on a fixed TAP must never have two guests
/// that could land on one node, because the second one fails to boot with
/// `sandbox needs TAP device tap0 ..., but another process holds it`.
///
/// That rules out a keyed task (any number of instances, plus forks that
/// boot cold and disk snapshot seeds), a task whose `count` is above one,
/// and a rollout with `replacement = "createFirst"`, which places the new
/// allocation before the old one stops (preferring the old one's node).
/// `count` is the count to check, so `job scale` can ask the same question
/// of the count it is about to set.
pub fn validate_fixed_tap(
    task: &crate::structs::TaskConfig,
    count: u32,
    create_first: bool,
) -> Result<(), String> {
    let Some(tap) = sandbox_fixed_tap(task) else {
        return Ok(());
    };
    let keyed = task
        .stateful
        .as_ref()
        .is_some_and(|stateful| stateful.mode == crate::structs::InstanceMode::Keyed);
    let why = if keyed {
        "it is a keyed task (stateful.mode = \"keyed\"), so a second instance, a fork that boots \
         cold, or a disk snapshot seed on the same node fails to boot"
            .to_string()
    } else if count > 1 {
        format!(
            "it runs count = {count}, and two of its allocations on one node would both need \
             the device, so the second fails to boot"
        )
    } else if create_first {
        "its rollout uses replacement = \"createFirst\", which starts the replacement next to \
         the old allocation (on its node when it fits), so the replacement fails to boot"
            .to_string()
    } else {
        return Ok(());
    };
    Err(format!(
        "task {:?} attaches its sandbox to the fixed TAP device {tap:?} (config.network.tap), \
         which serves one guest per node, but {why}\nHint: set config.network = {{ mode = \
         \"tap\", tap = \"auto\", bridge = \"<the bridge {tap} is on>\" }}, so Legion creates a \
         TAP per instance",
        task.name
    ))
}

/// Whether a Firecracker version can move a restored NIC to a different TAP
/// device (`network_overrides` on `PUT /snapshot/load`, added in 1.12).
/// An unparseable version is assumed not to.
pub fn firecracker_can_override_tap(version: &str) -> bool {
    let mut parts = version
        .trim()
        .trim_start_matches('v')
        .split('.')
        .map(|part| part.parse::<u32>().ok());
    match (parts.next().flatten(), parts.next().flatten()) {
        (Some(major), Some(minor)) => (major, minor) >= (1, 12),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_ids_suffix_the_origin_and_respect_the_length_limit() {
        assert_eq!(fork_instance_id("run-17", 1).as_deref(), Some("run-17-f1"));
        assert_eq!(
            fork_instance_id("run-17", 12).as_deref(),
            Some("run-17-f12")
        );
        let long = "x".repeat(126);
        assert!(fork_instance_id(&long, 1).is_none());
    }

    #[test]
    fn fork_macs_are_distinct_locally_administered_unicast() {
        let macs: std::collections::HashSet<String> = (1..=1000).map(fork_mac).collect();
        assert_eq!(macs.len(), 1000);
        let mac = fork_mac(0x0102_0304);
        assert_eq!(mac, "02:4c:01:02:03:04");
        // Bit 1 of the first octet set (local), bit 0 clear (unicast).
        assert_eq!(u8::from_str_radix(&mac[..2], 16).unwrap() & 0b11, 0b10);
    }

    #[test]
    fn fork_taps_fit_ifnamsiz_and_are_per_instance() {
        let a = own_tap_name("run-17-f1");
        let b = own_tap_name("run-17-f2");
        assert_ne!(a, b);
        assert_eq!(a.len(), 15);
        assert!(a.starts_with("lgf"));
        assert_eq!(a, own_tap_name("run-17-f1"), "stable per instance");
    }

    #[test]
    fn firecracker_tap_override_needs_1_12() {
        assert!(!firecracker_can_override_tap("1.7.0"));
        assert!(!firecracker_can_override_tap("1.11.2"));
        assert!(firecracker_can_override_tap("1.12.0"));
        assert!(firecracker_can_override_tap("v1.13.1"));
        assert!(firecracker_can_override_tap("2.0"));
        assert!(!firecracker_can_override_tap("unknown"));
    }

    fn sandbox(network: serde_json::Value, stateful: Option<&str>) -> crate::structs::TaskConfig {
        let mut value = serde_json::json!({
            "name": "workspace",
            "runtime": "sandbox",
            "config": { "image": "alpine", "command": ["sleep", "1"], "network": network },
        });
        if let Some(mode) = stateful {
            value["stateful"] = serde_json::json!({ "mode": mode });
        }
        serde_json::from_value(value).expect("task parses")
    }

    #[test]
    fn a_fixed_tap_is_refused_wherever_two_guests_could_share_a_node() {
        let fixed = serde_json::json!({ "mode": "tap", "tap": "tap0" });
        let keyed = sandbox(fixed.clone(), Some("keyed"));
        assert_eq!(sandbox_fixed_tap(&keyed), Some("tap0"));
        let err = validate_fixed_tap(&keyed, 1, false).unwrap_err();
        assert!(err.contains("keyed task"), "{err}");
        assert!(err.contains("Hint: set config.network"), "{err}");
        assert!(err.contains("tap = \"auto\""), "{err}");

        // Slot mode and stateless tasks: only a count above one, or a
        // createFirst rollout, puts two guests on one node.
        for stateful in [Some("slot"), None] {
            let task = sandbox(fixed.clone(), stateful);
            assert!(validate_fixed_tap(&task, 1, false).is_ok());
            let err = validate_fixed_tap(&task, 3, false).unwrap_err();
            assert!(err.contains("count = 3"), "{err}");
            let err = validate_fixed_tap(&task, 1, true).unwrap_err();
            assert!(err.contains("createFirst"), "{err}");
        }
    }

    #[test]
    fn auto_taps_and_tapless_sandboxes_pass_the_fixed_tap_check() {
        let auto = sandbox(
            serde_json::json!({ "mode": "tap", "tap": "auto", "bridge": "br0" }),
            Some("keyed"),
        );
        assert_eq!(sandbox_fixed_tap(&auto), None);
        assert!(validate_fixed_tap(&auto, 8, true).is_ok());
        let none = sandbox(serde_json::json!({ "mode": "none" }), Some("keyed"));
        assert!(validate_fixed_tap(&none, 8, true).is_ok());
    }
}
