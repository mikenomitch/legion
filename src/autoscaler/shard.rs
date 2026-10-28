//! Which shard a policy belongs to (autoscaler-v2 plan §6.9, calls entry
//! 13): the manager filters `WatchScalingPolicies` by it, and each
//! autoscaler replica evaluates the shards it holds a lease on.
//!
//! The hash is FNV-1a (64-bit) of the policy's key, so every build and
//! every process agrees on it without a seed:
//!
//! | Policy | Key |
//! | --- | --- |
//! | an autoscaled task | `<job id>/<group>/<task>` |
//! | an external-target standalone policy | `policy:<id>` |
//! | every node group | `nodegroups`: one shard, so the expander sees every group that could host the same work |

/// How many shards policies hash to.
pub const SHARDS: u32 = 64;

/// The key every node group shards by.
pub const NODE_GROUPS_KEY: &str = "nodegroups";

/// FNV-1a, 64-bit.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The shard of `key` among `count` shards (0 when `count` is 0).
pub fn shard_of(key: &str, count: u32) -> u32 {
    if count == 0 {
        return 0;
    }
    (fnv1a(key.as_bytes()) % u64::from(count)) as u32
}

/// An autoscaled task's shard key.
pub fn task_key(job_id: &str, group: &str, task: &str) -> String {
    format!("{job_id}/{group}/{task}")
}

/// A standalone policy's shard key: its own for an external target, the
/// shared [`NODE_GROUPS_KEY`] for a node group.
pub fn standalone_key(kind: &str, id: &str) -> String {
    if kind == "nodes" {
        NODE_GROUPS_KEY.to_string()
    } else {
        format!("policy:{id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_are_stable_and_spread_test() {
        // FNV-1a's published test vectors.
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(shard_of("web/default/web", 0), 0);
        assert_eq!(
            shard_of("web/default/web", SHARDS),
            shard_of("web/default/web", SHARDS)
        );
        let mut counts = vec![0u32; SHARDS as usize];
        for n in 0..10_000 {
            counts[shard_of(&task_key(&format!("job-{n}"), "default", "web"), SHARDS) as usize] +=
                1;
        }
        // 10,000 keys over 64 shards: about 156 each.
        assert!(
            counts.iter().all(|count| (100..220).contains(count)),
            "{counts:?}"
        );
        assert_eq!(standalone_key("nodes", "a"), standalone_key("nodes", "b"));
        assert_eq!(
            standalone_key("external", "media/render"),
            "policy:media/render"
        );
    }
}
