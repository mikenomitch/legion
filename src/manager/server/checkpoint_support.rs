//! Which checkpoints a running instance can take right now
//! (`InstanceInfo.checkpoint_support`, suspend/resume UI plan §7.3).
//!
//! Clients used to re-derive "can this take a memory snapshot?" from the job
//! spec, which cannot see the node's VMM, its volume driver's ops, its
//! worker version, or a stored network policy that gates the task. This runs
//! the same precondition functions CreateSnapshot, SuspendInstance, and
//! ForkInstance run, so the answer and the RPC never disagree, and each
//! blocker is the exact message the RPC would return.

use super::fork_rpcs::{fork_precondition, spread_precondition};
use super::snapshot_rpcs::{
    node_build_version, node_has_capability, publish_precondition, snapshot_precondition,
    CHECKPOINT_CAPABILITY,
};
use super::suspend_rpcs::{default_suspend_kind, suspend_memory_precondition};
use super::*;
use crate::structs::checkpoint::SnapshotKind;
use crate::structs::CheckpointSupport;

/// The support matrix for one running allocation. Pure over its inputs.
pub(crate) fn checkpoint_support(
    task: &TaskConfig,
    instance: &Instance,
    allocation: &crate::structs::Allocation,
    node: &Node,
) -> CheckpointSupport {
    // A worker without the capability refuses every kind, with one message.
    if !node_has_capability(node, CHECKPOINT_CAPABILITY) {
        let reason = format!(
            "node {} runs worker version {}, which cannot take checkpoints (it does not \
             advertise the `{CHECKPOINT_CAPABILITY}` capability); upgrade the worker",
            node.id,
            node_build_version(node)
        );
        return CheckpointSupport {
            snapshot_memory_blocker: reason.clone(),
            snapshot_disk_blocker: reason.clone(),
            suspend_memory_blocker: reason.clone(),
            suspend_blocker: reason.clone(),
            fork_memory_blocker: reason.clone(),
            fork_disk_blocker: reason.clone(),
            spread_blocker: reason.clone(),
            publish_blocker: reason,
            ..Default::default()
        };
    }
    let mut support = CheckpointSupport::default();
    let set = |allowed: &mut bool, blocker: &mut String, reason: Option<String>| match reason {
        Some(reason) => *blocker = reason,
        None => *allowed = true,
    };

    let snapshot_memory =
        snapshot_precondition(SnapshotKind::Memory, task, instance, allocation, node);
    set(
        &mut support.snapshot_memory,
        &mut support.snapshot_memory_blocker,
        snapshot_memory.clone(),
    );
    set(
        &mut support.snapshot_disk,
        &mut support.snapshot_disk_blocker,
        snapshot_precondition(SnapshotKind::Disk, task, instance, allocation, node),
    );
    // Publishing needs a memory snapshot first, then a reachable store.
    set(
        &mut support.publish,
        &mut support.publish_blocker,
        snapshot_memory.or_else(|| publish_precondition(task, instance, node, "--publish")),
    );

    if instance.mode != InstanceMode::Keyed {
        let reason = format!(
            "instance {} is slot-mode; suspend and fork apply to keyed instances \
             (stateful.mode = \"keyed\")",
            instance.instance_id
        );
        support.suspend_memory_blocker = reason.clone();
        support.suspend_blocker = reason.clone();
        support.fork_memory_blocker = reason.clone();
        support.fork_disk_blocker = reason.clone();
        support.spread_blocker = reason;
        return support;
    }

    set(
        &mut support.suspend_memory,
        &mut support.suspend_memory_blocker,
        suspend_memory_precondition(task, instance, allocation, node),
    );
    support.suspend_default = default_suspend_kind(task, instance, allocation, node)
        .as_str()
        .to_string();
    set(
        &mut support.fork_memory,
        &mut support.fork_memory_blocker,
        fork_precondition(SnapshotKind::Memory, task, instance, allocation, node),
    );
    set(
        &mut support.fork_disk,
        &mut support.fork_disk_blocker,
        fork_precondition(SnapshotKind::Disk, task, instance, allocation, node),
    );
    // ForkInstance checks spread's own refusals first, then the memory
    // fork's.
    set(
        &mut support.spread,
        &mut support.spread_blocker,
        spread_precondition(SnapshotKind::Memory, task, instance, node)
            .or_else(|| fork_precondition(SnapshotKind::Memory, task, instance, allocation, node)),
    );
    support
}

/// [`checkpoint_support`] for an instance as the store holds it: `None`
/// unless it is `active` with a running allocation whose node and task load.
/// Lookup failures degrade to `None` rather than failing an instance read.
pub(crate) fn load_checkpoint_support(
    store: &Store,
    instance: &Instance,
) -> Option<CheckpointSupport> {
    if instance.state != InstanceState::Active {
        return None;
    }
    let allocation = store
        .get_allocation_by_id(instance.current_alloc_id?)
        .ok()??;
    let node = store.get_node(&allocation.node_id).ok()?;
    let task = store
        .cached_task_configs_for_job_config(&allocation.job_config_id)
        .ok()?
        .into_iter()
        .find(|(id, _)| *id == allocation.task_config_id)
        .map(|(_, task)| task)?;
    Some(checkpoint_support(&task, instance, &allocation, &node))
}
