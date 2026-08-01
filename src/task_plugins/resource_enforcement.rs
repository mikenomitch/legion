//! The contract task runtimes use to participate in the node's CPU and
//! memory accounting.
//!
//! Enforcement itself is a **worker-core subsystem** (see
//! `src/worker/resources.rs`): it fingerprints the node's usable CPUs, owns
//! the share/reserve cgroup partition, allocates exclusive cores, persists
//! assignments across worker restarts, and releases them on teardown. This
//! module is the small surface a *runtime* sees:
//!
//! - [`ResourceEnforcement`] — how (or whether) a runtime participates,
//!   returned from [`crate::task_plugins::TaskPlugin::resource_enforcement`];
//! - [`ResourceAssignment`] — what worker core decided for one allocation,
//!   hung on the [`crate::task_plugins::TaskInstance`] before `prepare()`;
//! - [`join_assignment_cgroup`] — the two-line adoption path for a
//!   `WorkerCgroup` runtime.
//!
//! The split mirrors the one Legion already made for volumes: worker core
//! resolves and accounts, the runtime consumes the result its own way. See
//! `docs/resource-enforcement.md` and the "Resource enforcement" section of
//! `docs/task-runtime-plugins.md`.

use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// How a task runtime participates in worker-enforced resource placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResourceEnforcement {
    /// Not governed: worker core computes no assignment and touches no cgroup
    /// for this runtime's tasks, and their `resources` stay scheduling-only.
    ///
    /// The default, and the honest answer both for runtimes with nothing to
    /// confine (`noop`) and for third-party plugins compiled before this
    /// method existed — they behave exactly as they do today.
    #[default]
    Unsupported,
    /// Worker core creates the assignment's cgroup, writes its limits
    /// (`cpuset.cpus` / `cpu.weight` / `memory.max` + `memory.swap.max`), and
    /// the runtime spawns its process into it.
    ///
    /// For runtimes whose task *is* a host process. Adopting is two lines:
    /// return this, and spawn through [`join_assignment_cgroup`].
    WorkerCgroup,
    /// The runtime applies the assignment itself, by its own means — the
    /// `container` runtime writes it into the generated OCI spec; a microVM
    /// runtime would pin vCPU threads and size the guest. Worker core still
    /// computes and accounts, but touches no cgroup file for this task.
    RuntimeApplied,
}

/// Which slice of the node's CPUs a task lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Slice {
    /// The shared pool: every governed task that did not reserve cores.
    /// Tenants compete on `cpu.weight`, and the pool's cpuset shrinks as
    /// other tasks reserve cores.
    Share,
    /// Exclusively reserved whole CPUs, one cgroup per reserving allocation.
    Reserve,
}

impl Slice {
    /// Directory name under the hierarchy root.
    pub fn dir_name(self) -> &'static str {
        match self {
            Slice::Share => "share",
            Slice::Reserve => "reserve",
        }
    }
}

/// What worker core decided for one allocation, computed before `prepare()`
/// for every runtime whose [`ResourceEnforcement`] is not `Unsupported`.
///
/// Serialized into the worker's persisted allocation record, so a worker
/// restart rebuilds the core allocator and both slice masks from exactly what
/// it handed out — a future adopting runtime needs no state of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceAssignment {
    /// Which slice this task lives in.
    pub slice: Slice,
    /// This task's cgroup, relative to the cgroup root — e.g.
    /// `legion/share/legion-42` or `legion/reserve/legion-42`. Explicit
    /// (cgroupfs form) so an operator can find a task's cgroup by allocation
    /// id and see the share/reserve split in the hierarchy.
    pub cgroup_path: PathBuf,
    /// Logical CPUs assigned exclusively to this task, when it declared
    /// `cores`. `None` for share-slice tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cores: Option<Vec<u32>>,
    /// Proportional CPU weight, as an OCI `cpu.shares` value (Nomad's
    /// convention: shares == declared MHz). `None` when the task declared no
    /// `cpu`, which leaves it at the cgroup default weight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_weight: Option<u64>,
    /// Hard memory ceiling in bytes, from declared `memory` (MiB). `None`
    /// when the task declared none — an undeclared job is left unconstrained,
    /// mirroring the macOS guest-sizing rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,
}

impl ResourceAssignment {
    /// Absolute path of this task's cgroup on the node.
    pub fn absolute_cgroup_path(&self) -> PathBuf {
        PathBuf::from(CGROUP_ROOT).join(&self.cgroup_path)
    }

    /// True when nothing was actually declared — the task is governed (it
    /// lives in the hierarchy, so reserved cores are off-limits to it and its
    /// process tree is reapable) but carries no limits of its own.
    pub fn is_unlimited(&self) -> bool {
        self.cores.is_none() && self.cpu_weight.is_none() && self.memory_limit_bytes.is_none()
    }
}

/// Mount point of the cgroup v2 unified hierarchy.
pub const CGROUP_ROOT: &str = crate::task_plugins::exec_isolation::CGROUP_ROOT;

/// Spawn a command into its worker-assigned cgroup — the `WorkerCgroup`
/// adoption path.
///
/// The join happens in a `pre_exec` hook, so the process is inside the cgroup
/// before it execs and there is no window in which it runs outside its
/// limits. The cgroup itself was created and sized by worker core; this only
/// writes the pid into `cgroup.procs`.
///
/// A no-op when the instance carries no assignment (enforcement off, a
/// non-Linux worker, or an `Unsupported` runtime), so a plugin can call it
/// unconditionally.
#[cfg(target_os = "linux")]
pub fn join_assignment_cgroup(
    cmd: &mut std::process::Command,
    instance: &crate::task_plugins::TaskInstance,
) -> R<()> {
    use std::os::unix::process::CommandExt;

    let Some(assignment) = instance.resource_assignment.as_ref() else {
        return Ok(());
    };
    let procs = assignment.absolute_cgroup_path().join("cgroup.procs");
    // SAFETY: the closure only writes a pid to a file; it allocates a String
    // but performs no other async-signal-unsafe work beyond what the standard
    // library's own pre_exec examples do.
    unsafe {
        cmd.pre_exec(move || {
            std::fs::write(&procs, std::process::id().to_string())
                .map_err(std::io::Error::other)?;
            Ok(())
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn join_assignment_cgroup(
    _cmd: &mut std::process::Command,
    _instance: &crate::task_plugins::TaskInstance,
) -> R<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignments_round_trip_through_the_persisted_record() -> R<()> {
        let assignment = ResourceAssignment {
            slice: Slice::Reserve,
            cgroup_path: PathBuf::from("legion/reserve/legion-42"),
            cores: Some(vec![3, 7]),
            cpu_weight: None,
            memory_limit_bytes: Some(268_435_456),
        };
        let encoded = serde_json::to_string(&assignment)?;
        assert_eq!(
            serde_json::from_str::<ResourceAssignment>(&encoded)?,
            assignment
        );
        assert_eq!(
            assignment.absolute_cgroup_path(),
            PathBuf::from("/sys/fs/cgroup/legion/reserve/legion-42")
        );
        assert!(!assignment.is_unlimited());
        Ok(())
    }

    /// A governed task that declared nothing still gets a cgroup — that is
    /// what makes reserved cores exclusive against it and its descendants
    /// reapable — but carries no limits.
    #[test]
    fn an_undeclared_task_is_governed_but_unlimited() {
        let assignment = ResourceAssignment {
            slice: Slice::Share,
            cgroup_path: PathBuf::from("legion/share/legion-1"),
            cores: None,
            cpu_weight: None,
            memory_limit_bytes: None,
        };
        assert!(assignment.is_unlimited());
        assert_eq!(Slice::Share.dir_name(), "share");
        assert_eq!(Slice::Reserve.dir_name(), "reserve");
    }

    #[test]
    fn the_default_mode_is_ungoverned() {
        assert_eq!(
            ResourceEnforcement::default(),
            ResourceEnforcement::Unsupported
        );
    }
}
