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
/// - `config.network = { mode = "tap", bridge = ... }` boots every instance
///   on it.
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

/// A VMM version as `(major, minor, patch)`, from `1.16.1`, `v1.13`, or
/// `1.12.0-dev`. A missing patch reads as 0. `None` when the major or minor
/// part is not a number.
pub fn parse_vmm_semver(version: &str) -> Option<(u32, u32, u32)> {
    let mut parts = version.trim().trim_start_matches('v').split('.');
    let number = |part: Option<&str>| -> Option<u32> {
        let digits: String = part?.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    };
    let major = number(parts.next())?;
    let minor = number(parts.next())?;
    Some((major, minor, number(parts.next()).unwrap_or(0)))
}

/// Whether a Firecracker version can move a restored NIC to a different TAP
/// device (`network_overrides` on `PUT /snapshot/load`, added in 1.12).
/// An unparseable version is assumed not to.
pub fn firecracker_can_override_tap(version: &str) -> bool {
    parse_vmm_semver(version).is_some_and(|version| version >= (1, 12, 0))
}

/// The oldest Firecracker that can move a restored gated guest's vsock to
/// the restoring allocation's socket: 1.16.0 added `vsock_override` on
/// `PUT /snapshot/load`, and 1.16.1 fixed guest-to-host connections timing
/// out after a restore with a TX descriptor in flight, which is exactly the
/// gated datapath's traffic (gated plan §6.2, calls entry 18).
pub const FIRECRACKER_VSOCK_OVERRIDE_MIN: (u32, u32, u32) = (1, 16, 1);

/// Whether a Firecracker version can take a memory checkpoint of a gated
/// sandbox. An unparseable version is assumed not to.
pub fn firecracker_can_override_vsock(version: &str) -> bool {
    parse_vmm_semver(version).is_some_and(|version| version >= FIRECRACKER_VSOCK_OVERRIDE_MIN)
}

/// Why a gated sandbox cannot take a memory checkpoint on this VMM, as the
/// refusal the manager and the worker both give. `None` when it can: any
/// cloud-hypervisor, and Firecracker 1.16.1 or later. `subject` names the
/// instance or allocation, `fix` is the hint's fallback (`use --checkpoint
/// disk`, `use --disk`).
pub fn gated_memory_checkpoint_refusal(
    subject: &str,
    node: &str,
    vmm: &str,
    version: Option<&str>,
    fix: &str,
) -> Option<String> {
    if vmm != "firecracker" {
        return None;
    }
    let version = version.unwrap_or("unknown");
    if firecracker_can_override_vsock(version) {
        return None;
    }
    let (major, minor, patch) = FIRECRACKER_VSOCK_OVERRIDE_MIN;
    Some(format!(
        "{subject} is gated, and node {node} runs Firecracker {version}, which cannot move a \
         restored guest's vsock to a new socket (that needs Firecracker {major}.{minor}.{patch} \
         or later)\nHint: upgrade Firecracker on the node, or {fix}"
    ))
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
    fn vmm_versions_parse_with_or_without_a_patch() {
        assert_eq!(parse_vmm_semver("1.16.1"), Some((1, 16, 1)));
        assert_eq!(parse_vmm_semver("v1.13"), Some((1, 13, 0)));
        assert_eq!(parse_vmm_semver("1.12.0-dev"), Some((1, 12, 0)));
        assert_eq!(parse_vmm_semver("52.0"), Some((52, 0, 0)));
        assert_eq!(parse_vmm_semver("unknown"), None);
        assert_eq!(parse_vmm_semver("1"), None);
    }

    /// Gated plan §6.2: 1.16.0 added the override, 1.16.1 fixed the restore
    /// timeout on the gated traffic pattern, so the floor is 1.16.1.
    #[test]
    fn gated_memory_checkpoints_need_firecracker_1_16_1() {
        for (version, ok) in [
            ("1.7.0", false),
            ("1.12.0", false),
            ("1.15.0", false),
            ("1.16.0", false),
            ("1.16.1", true),
            ("v1.16.2", true),
            ("1.17.0", true),
            ("2.0", true),
            ("unknown", false),
        ] {
            assert_eq!(firecracker_can_override_vsock(version), ok, "{version}");
            let refusal = gated_memory_checkpoint_refusal(
                "instance user-42",
                "w-03",
                "firecracker",
                Some(version),
                "use --checkpoint disk",
            );
            assert_eq!(refusal.is_none(), ok, "{version}: {refusal:?}");
        }
        let refusal = gated_memory_checkpoint_refusal(
            "instance user-42",
            "w-03",
            "firecracker",
            Some("1.15.0"),
            "use --checkpoint disk",
        )
        .unwrap();
        assert_eq!(
            refusal,
            "instance user-42 is gated, and node w-03 runs Firecracker 1.15.0, which cannot move \
             a restored guest's vsock to a new socket (that needs Firecracker 1.16.1 or \
             later)\nHint: upgrade Firecracker on the node, or use --checkpoint disk"
        );
        // cloud-hypervisor has no floor beyond Legion's own.
        assert!(
            gated_memory_checkpoint_refusal("x", "n", "cloud-hypervisor", Some("35.0"), "f")
                .is_none()
        );
        // A node that reports no version is refused rather than trusted.
        assert!(gated_memory_checkpoint_refusal("x", "n", "firecracker", None, "f").is_some());
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
}
