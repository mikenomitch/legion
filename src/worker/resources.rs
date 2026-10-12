//! Worker-owned resource enforcement: the node's CPU/memory accounting, the
//! share/reserve cgroup partition, and the core allocator behind
//! `resources.cores`.
//!
//! The model is Nomad's. The worker partitions the node's usable CPUs into
//! two parent cgroups it creates and owns:
//!
//! ```text
//! legion/
//! ├── share/     cpuset.cpus = usable − reserved   ← unpinned tasks live here
//! │   └── legion-<alloc>   cpu.weight from declared MHz
//! └── reserve/   cpuset.cpus = union of reservations
//!     └── legion-<alloc>   cpuset.cpus = its assigned cores, exclusively
//! ```
//!
//! A task declaring `cores` gets whole CPUs moved out of the share slice;
//! every other governed task lives under `share/` and inherits its shrinking
//! cpuset automatically — cgroup v2 cpusets are hierarchical, so one write to
//! `legion/share/cpuset.cpus` re-fences every tenant at once.
//!
//! Runtimes never touch any of this. They declare a
//! [`ResourceEnforcement`] mode and receive a [`ResourceAssignment`]; see
//! `src/task_plugins/resource_enforcement.rs` and
//! `docs/resource-enforcement.md`.
//!
//! **Exclusivity scope, stated honestly.** The fence governs every runtime
//! that opts in. Processes outside Legion entirely (node daemons, SSH
//! sessions) and tasks on `Unsupported` runtimes are beyond it and may still
//! touch reserved cores. That residue is inherent to a shared host.

use crate::prelude::*;
use crate::structs::TaskConfig;
use crate::task_plugins::{ResourceAssignment, ResourceEnforcement, Slice, CGROUP_ROOT};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// cgroup v2 controllers the parents must delegate for the limits below to
/// exist in their children.
const REQUIRED_CONTROLLERS: [&str; 3] = ["cpuset", "cpu", "memory"];

/// Per-allocation cgroup leaf name. Matches the container name the runtimes
/// already use, so `legion-<alloc>` is one identifier across the container
/// list, the process table, and the cgroup tree.
fn leaf_name(allocation_id: i64) -> String {
    format!("legion-{allocation_id}")
}

/// Parse a kernel CPU list (`"0-3,7"`) into individual logical CPU numbers.
pub fn parse_cpu_list(raw: &str) -> Vec<u32> {
    let mut cpus = Vec::new();
    for part in raw.trim().split(',').filter(|part| !part.is_empty()) {
        match part.split_once('-') {
            Some((start, end)) => {
                if let (Ok(start), Ok(end)) =
                    (start.trim().parse::<u32>(), end.trim().parse::<u32>())
                {
                    cpus.extend(start..=end);
                }
            }
            None => {
                if let Ok(cpu) = part.trim().parse::<u32>() {
                    cpus.push(cpu);
                }
            }
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

/// Render CPU numbers as a kernel CPU list, collapsing runs into ranges — the
/// form `cpuset.cpus` reads back, and the form an operator expects to see.
pub fn format_cpu_list(cpus: &[u32]) -> String {
    let mut sorted = cpus.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut parts: Vec<String> = Vec::new();
    let mut index = 0;
    while index < sorted.len() {
        let start = sorted[index];
        let mut end = start;
        while index + 1 < sorted.len() && sorted[index + 1] == end + 1 {
            index += 1;
            end = sorted[index];
        }
        parts.push(if start == end {
            start.to_string()
        } else {
            format!("{start}-{end}")
        });
        index += 1;
    }
    parts.join(",")
}

/// Declared `memory` (MiB) as a byte ceiling. Absent or non-positive means
/// the job declared none, which stays unconstrained.
pub fn memory_limit_bytes(resources: &Value) -> Option<u64> {
    resources
        .get("memory")
        .and_then(Value::as_f64)
        .filter(|mib| *mib > 0.0)
        .map(|mib| (mib * 1024.0 * 1024.0).round() as u64)
}

/// The memory ceiling in bytes: declared `memoryMax` when set, otherwise
/// declared `memory` (today's behavior, where the reservation is the limit).
/// `None` when neither is declared.
pub fn memory_ceiling_bytes(resources: &Value) -> Option<u64> {
    resources
        .get(crate::resources::MEMORY_MAX_KEY)
        .and_then(Value::as_f64)
        .filter(|mib| *mib > 0.0)
        .map(|mib| (mib * 1024.0 * 1024.0).round() as u64)
        .or_else(|| memory_limit_bytes(resources))
}

/// Whether this node can enforce at all: Linux with the cgroup v2 unified
/// hierarchy mounted. The same test [`NodeResourceGovernor::from_env`] makes,
/// for callers that need the answer before a governor exists (registration
/// decides whether to advertise oversubscription with it).
pub fn enforcement_available() -> bool {
    cfg!(target_os = "linux") && Path::new(CGROUP_ROOT).join("cgroup.controllers").exists()
}

/// Whether this node can enforce under `cgroup_parent`: everything
/// [`enforcement_available`] checks, plus write access to create the parent.
/// The worker checks once at start, so a node without access (a non-root
/// worker, a container with a read-only `/sys/fs/cgroup`) runs unconfined
/// instead of failing every allocation at its first cgroup write.
pub fn cgroup_parent_usable(cgroup_parent: &str) -> R<()> {
    if !enforcement_available() {
        bail!("no cgroup v2 unified hierarchy at {CGROUP_ROOT}");
    }
    let dir = Path::new(CGROUP_ROOT).join(cgroup_parent);
    std::fs::create_dir_all(&dir).with_context(|| format!("create cgroup {}", dir.display()))?;
    Ok(())
}

/// Declared `cores`: whole exclusive CPUs.
///
/// Deliberately *not* `cpus` — the scheduler treats `cpus` as an alias for
/// `cpu` (MHz), not for `cores` (`ResourcePolicy::normalize_task_resources`),
/// and reading it here would turn every `cpus: 0.5` job in the wild into a
/// fractional-cpuset error.
pub fn declared_cores(resources: &Value) -> Option<f64> {
    resources
        .get("cores")
        .and_then(Value::as_f64)
        .filter(|cores| *cores > 0.0)
}

/// Declared `cpu` (MHz) as an OCI `cpu.shares` value, honoring the scheduler's
/// `cpus` alias. Nomad's convention: shares == MHz, which cgroup-v2 runtimes
/// convert to `cpu.weight` with the standard mapping
/// (`weight = 1 + ((shares − 2) × 9999) / 262142`).
pub fn cpu_weight_shares(resources: &Value) -> Option<u64> {
    resources
        .get("cpu")
        .and_then(Value::as_f64)
        .or_else(|| resources.get("cpus").and_then(Value::as_f64))
        .filter(|mhz| *mhz > 0.0)
        .map(|mhz| mhz.round().max(1.0) as u64)
}

/// Convert an OCI `cpu.shares` value to the cgroup v2 `cpu.weight` the kernel
/// takes, with the mapping systemd and Nomad both use. Exposed so the worker
/// can write `cpu.weight` directly for `WorkerCgroup` runtimes, where there
/// is no OCI runtime in between to do the conversion.
pub fn shares_to_weight(shares: u64) -> u64 {
    let shares = shares.clamp(2, 262_144);
    (1 + ((shares - 2) * 9999) / 262_142).clamp(1, 10_000)
}

/// The node's CPU/memory governor: what is usable, what is reserved, and the
/// cgroup hierarchy that makes both real.
pub struct NodeResourceGovernor {
    /// Whether the worker was started with `resourceEnforcement.enabled` and
    /// this node can actually enforce (cgroup v2 present).
    enabled: bool,
    /// Hierarchy root relative to [`CGROUP_ROOT`] — the worker config's
    /// `cgroupParent`, `legion` by default.
    root: PathBuf,
    /// Logical CPUs this worker may hand out, fingerprinted from its own
    /// `cpuset.cpus.effective` (not `nproc` — the worker itself may be
    /// confined).
    usable_cores: Vec<u32>,
    /// Cores currently reserved, by allocation.
    reserved: BTreeMap<i64, Vec<u32>>,
}

impl Default for NodeResourceGovernor {
    fn default() -> Self {
        Self::disabled()
    }
}

impl NodeResourceGovernor {
    /// A governor that governs nothing — every allocation gets `None`, and no
    /// cgroup file is ever touched. The state of every worker that has not
    /// opted in, and of every non-Linux worker.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            root: PathBuf::from("legion"),
            usable_cores: Vec::new(),
            reserved: BTreeMap::new(),
        }
    }

    /// Build the governor from the env the daemon exports for task runtimes
    /// (`LEGION_RESOURCE_ENFORCEMENT`, `LEGION_CGROUP_PARENT`). The worker
    /// itself configures its governor explicitly with
    /// [`Self::from_settings`]; this is for code that only has the env.
    pub fn from_env() -> Self {
        Self::from_settings(
            crate::task_plugins::exec_isolation::enforcement_enabled(),
            &crate::task_plugins::exec_isolation::cgroup_parent(),
        )
    }

    /// Build the governor from the worker's `resourceEnforcement` settings:
    /// whether enforcement is on, and the cgroup parent (relative to
    /// [`CGROUP_ROOT`]) its hierarchy lives under.
    ///
    /// Enforcement is on by default. A node that cannot enforce (not Linux,
    /// no cgroup v2, no usable CPUs) degrades to `disabled` rather than
    /// failing every allocation, and says so. The "enabled" line is logged
    /// here and only here, once the governor really is enabled, so the
    /// worker log never claims enforcement a node is not doing.
    pub fn from_settings(enabled: bool, cgroup_parent: &str) -> Self {
        if !enabled {
            return Self::disabled();
        }
        if !cfg!(target_os = "linux") {
            // The daemon logged what the switch means here at start.
            return Self::disabled();
        }
        if !Path::new(CGROUP_ROOT).join("cgroup.controllers").exists() {
            warn!(
                "Resource enforcement is enabled but this node has no cgroup v2 unified \
                 hierarchy; tasks run unconfined"
            );
            return Self::disabled();
        }

        let parent = cgroup_parent.trim().trim_matches('/');
        let root = PathBuf::from(if parent.is_empty() { "legion" } else { parent });
        let usable_cores = fingerprint_usable_cores();
        if usable_cores.is_empty() {
            warn!(
                "Resource enforcement is enabled but this worker could not fingerprint any \
                 usable CPUs; tasks run unconfined"
            );
            return Self::disabled();
        }
        info!(
            cgroup_root = %root.display(),
            usable_cores = %format_cpu_list(&usable_cores),
            "Resource enforcement enabled: governed tasks run in worker-owned cgroups"
        );
        Self {
            enabled: true,
            root,
            usable_cores,
            reserved: BTreeMap::new(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// CPUs not currently reserved by any allocation — the share slice's mask.
    pub fn share_cores(&self) -> Vec<u32> {
        let taken = self.reserved_cores();
        self.usable_cores
            .iter()
            .copied()
            .filter(|cpu| !taken.contains(cpu))
            .collect()
    }

    /// Every reserved CPU on the node, sorted.
    pub fn reserved_cores(&self) -> Vec<u32> {
        let mut cores: Vec<u32> = self.reserved.values().flatten().copied().collect();
        cores.sort_unstable();
        cores.dedup();
        cores
    }

    /// Compute (and, for `WorkerCgroup`, materialize) the assignment for one
    /// allocation. Returns `None` when nothing is governed — enforcement off,
    /// or an `Unsupported` runtime — which is the "worker touches nothing"
    /// contract those runtimes rely on.
    ///
    /// Called by worker core before `prepare()`, at the same lifecycle point
    /// volumes are mounted: a task must never observe its limits arriving
    /// after it started.
    ///
    /// `memory_override` replaces the declared `memory` as the ceiling, for
    /// runtimes whose cgroup holds more than the workload (see
    /// [`crate::task_plugins::TaskPlugin::enforced_memory_limit_bytes`]).
    pub fn assign(
        &mut self,
        allocation_id: i64,
        task: &TaskConfig,
        mode: ResourceEnforcement,
        memory_override: Option<u64>,
    ) -> R<Option<ResourceAssignment>> {
        let Some(mut assignment) = self.plan(allocation_id, task, mode)? else {
            return Ok(None);
        };
        if memory_override.is_some() {
            assignment.memory_limit_bytes = memory_override;
        }
        // The parents must exist (and delegate their controllers) before any
        // leaf can carry a limit, and the share mask must shrink before the
        // reserving task starts — otherwise its cores are briefly shared.
        //
        // A failure here rolls the reservation back: a task that could not be
        // confined must not also strand the cores it never got.
        let applied = self
            .ensure_hierarchy()
            .and_then(|()| self.write_slice_masks())
            .and_then(|()| {
                if mode == ResourceEnforcement::WorkerCgroup {
                    create_task_cgroup(&assignment)?;
                }
                Ok(())
            });
        if let Err(err) = applied {
            self.reserved.remove(&allocation_id);
            let _ = self.write_slice_masks();
            return Err(err);
        }
        Ok(Some(assignment))
    }

    /// The bookkeeping half of [`Self::assign`]: decide what this allocation
    /// gets, updating the allocator's view, without touching a cgroup file.
    /// Split out so the allocator's arithmetic — the part with the exhaustion
    /// and release edge cases — is testable on any machine.
    fn plan(
        &mut self,
        allocation_id: i64,
        task: &TaskConfig,
        mode: ResourceEnforcement,
    ) -> R<Option<ResourceAssignment>> {
        if !self.enabled || mode == ResourceEnforcement::Unsupported {
            return Ok(None);
        }

        // A re-dispatch of an id we already hold cores for releases them
        // first, so an in-place relaunch does not leak a reservation.
        self.reserved.remove(&allocation_id);

        let cores = match declared_cores(&task.resources) {
            Some(count) => Some(self.reserve_cores(allocation_id, count)?),
            None => None,
        };
        let slice = if cores.is_some() {
            Slice::Reserve
        } else {
            Slice::Share
        };
        Ok(Some(ResourceAssignment {
            slice,
            cgroup_path: self
                .root
                .join(slice.dir_name())
                .join(leaf_name(allocation_id)),
            cores,
            cpu_weight: cpu_weight_shares(&task.resources),
            // The ceiling is `memoryMax` when the task bursts, and the
            // reservation is protected with `memory.low` either way, so the
            // kernel reclaims from tasks above their reservation before it
            // touches tasks within it (docs/resource-enforcement.md,
            // "Burstable memory").
            memory_limit_bytes: memory_ceiling_bytes(&task.resources),
            memory_low_bytes: memory_limit_bytes(&task.resources),
        }))
    }

    /// Re-adopt an assignment handed out before a worker restart, so the
    /// allocator's view matches the cgroups that are still on disk and the
    /// tasks still running inside them.
    pub fn restore(&mut self, allocation_id: i64, assignment: &ResourceAssignment) {
        if !self.enabled {
            return;
        }
        if let Some(cores) = assignment.cores.as_ref() {
            self.reserved.insert(allocation_id, cores.clone());
        }
    }

    /// Rewrite both slice masks after restoring every persisted assignment.
    /// Separate from [`Self::restore`] so restart recovery pays for one write
    /// rather than one per allocation.
    pub fn resync_masks(&self) {
        if !self.enabled {
            return;
        }
        if let Err(err) = self
            .ensure_hierarchy()
            .and_then(|()| self.write_slice_masks())
        {
            warn!(error = ?err, "Failed to re-establish cgroup slice masks after restart");
        }
    }

    /// Release an allocation's reservation and remove its cgroup. Idempotent:
    /// releasing an allocation that reserved nothing, or releasing twice, is
    /// a no-op.
    pub fn release(&mut self, allocation_id: i64, assignment: Option<&ResourceAssignment>) {
        if !self.enabled {
            return;
        }
        let had_reservation = self.reserved.remove(&allocation_id).is_some();
        if let Some(assignment) = assignment {
            remove_task_cgroup(assignment);
        }
        if had_reservation {
            // The cores are the share slice's again.
            if let Err(err) = self.write_slice_masks() {
                warn!(
                    allocation_id,
                    error = ?err,
                    "Failed to return released cores to the share slice"
                );
            }
        }
    }

    /// Assign whole CPUs to an allocation, lowest-numbered free first.
    ///
    /// Exhaustion — the scheduler's count said yes but no whole cores are
    /// free (fingerprint drift, offlined CPUs) — fails the allocation with an
    /// error naming both sets, feeding the normal `on_unhealthy` move
    /// machinery rather than wedging.
    fn reserve_cores(&mut self, allocation_id: i64, count: f64) -> R<Vec<u32>> {
        if count.fract() != 0.0 {
            bail!(
                "resources.cores must be a whole number on a governed runtime (got {count}): a \
                 cpuset cannot hold a fraction of a CPU. Use `cpu` (MHz) for a proportional \
                 share instead"
            );
        }
        let count = count as usize;
        let free = self.share_cores();
        if free.len() == count && !free.is_empty() {
            bail!(
                "reserving {count} core(s) for allocation {allocation_id} would leave the share \
                 slice empty, and every unpinned task on this node runs in it; leave at least \
                 one core unreserved, or lower the reservation"
            );
        }
        if free.len() < count {
            bail!(
                "cannot reserve {count} exclusive core(s) for allocation {allocation_id}: this \
                 node has {} free ({}), with {} already reserved ({}) out of {} usable ({})",
                free.len(),
                format_cpu_list(&free),
                self.reserved_cores().len(),
                format_cpu_list(&self.reserved_cores()),
                self.usable_cores.len(),
                format_cpu_list(&self.usable_cores),
            );
        }
        let assigned: Vec<u32> = free.into_iter().take(count).collect();
        self.reserved.insert(allocation_id, assigned.clone());
        Ok(assigned)
    }

    fn slice_path(&self, slice: Slice) -> PathBuf {
        PathBuf::from(CGROUP_ROOT)
            .join(&self.root)
            .join(slice.dir_name())
    }

    /// Create the hierarchy root and both slices, delegating the controllers
    /// the limits need. Idempotent — every start calls it.
    fn ensure_hierarchy(&self) -> R<()> {
        if !self.enabled {
            return Ok(());
        }
        let root = PathBuf::from(CGROUP_ROOT).join(&self.root);
        for dir in [
            root.clone(),
            root.join(Slice::Share.dir_name()),
            root.join(Slice::Reserve.dir_name()),
        ] {
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("create cgroup {}", dir.display()))?;
        }
        // Controllers are delegated top-down: a child can only use what its
        // parent put in `subtree_control`.
        enable_controllers(Path::new(CGROUP_ROOT))?;
        enable_controllers(&root)?;
        // `memory.low` is hierarchical: a leaf's protection is capped by its
        // ancestors' effective protection. "max" on the cgroups Legion owns
        // passes each leaf's own `memory.low` (its reservation) through
        // untouched. Ancestors above `cgroupParent` are the operator's: with
        // systemd's default `memory_recursiveprot` mount option they pass it
        // through too. Best-effort, like the controller writes: a kernel that
        // refuses leaves the reservation unprotected, which is today's
        // behavior, not a broken node.
        for dir in [
            root.clone(),
            root.join(Slice::Share.dir_name()),
            root.join(Slice::Reserve.dir_name()),
        ] {
            if let Err(err) = write_cgroup_file(&dir.join("memory.low"), "max") {
                debug!(path = %dir.display(), error = ?err, "Could not set memory.low on a parent cgroup");
            }
        }
        Ok(())
    }

    /// Write both slice cpusets: the share slice gets everything not
    /// reserved, the reserve slice the union of reservations. One write each,
    /// because cgroup v2 cpusets are hierarchical — every tenant is re-fenced
    /// by its parent without being touched itself.
    fn write_slice_masks(&self) -> R<()> {
        if !self.enabled {
            return Ok(());
        }
        let share = self.share_cores();
        write_cgroup_file(
            &self.slice_path(Slice::Share).join("cpuset.cpus"),
            &format_cpu_list(&share),
        )?;
        let reserved = self.reserved_cores();
        // An empty reserve slice is written as the empty string, which means
        // "inherit" — correct, since it has no tenants to fence.
        write_cgroup_file(
            &self.slice_path(Slice::Reserve).join("cpuset.cpus"),
            &format_cpu_list(&reserved),
        )?;
        Ok(())
    }
}

/// Usable logical CPUs, from the worker's own `cpuset.cpus.effective` — not
/// `nproc`: a worker that is itself confined to a subset must not hand out
/// CPUs it cannot use. Falls back to the root cgroup's effective set, then to
/// the machine's online CPUs.
fn fingerprint_usable_cores() -> Vec<u32> {
    let candidates = [
        PathBuf::from("/proc/self/cpuset"),
        PathBuf::from(CGROUP_ROOT).join("cpuset.cpus.effective"),
        PathBuf::from("/sys/devices/system/cpu/online"),
    ];
    // /proc/self/cpuset names a cgroup path, not a CPU list — resolve it.
    if let Ok(relative) = std::fs::read_to_string(&candidates[0]) {
        let relative = relative.trim().trim_start_matches('/');
        let effective = PathBuf::from(CGROUP_ROOT)
            .join(relative)
            .join("cpuset.cpus.effective");
        if let Ok(raw) = std::fs::read_to_string(&effective) {
            let cpus = parse_cpu_list(&raw);
            if !cpus.is_empty() {
                return cpus;
            }
        }
    }
    for candidate in &candidates[1..] {
        if let Ok(raw) = std::fs::read_to_string(candidate) {
            let cpus = parse_cpu_list(&raw);
            if !cpus.is_empty() {
                return cpus;
            }
        }
    }
    Vec::new()
}

/// Put the controllers the limits need into a cgroup's `subtree_control`.
/// A controller that is already enabled writes as an error the kernel reports
/// but that changes nothing, so failures are logged rather than fatal — the
/// missing-controller case surfaces as the limit write failing, with a path
/// in the message.
fn enable_controllers(dir: &Path) -> R<()> {
    let path = dir.join("cgroup.subtree_control");
    for controller in REQUIRED_CONTROLLERS {
        if let Err(err) = std::fs::write(&path, format!("+{controller}")) {
            debug!(
                path = %path.display(),
                controller,
                error = ?err,
                "Could not delegate cgroup controller (already enabled is normal)"
            );
        }
    }
    Ok(())
}

/// Create one task's cgroup and write its limits — the `WorkerCgroup` path.
/// `RuntimeApplied` runtimes never reach here: the OCI runtime creates the
/// cgroup at `cgroupsPath` itself.
fn create_task_cgroup(assignment: &ResourceAssignment) -> R<()> {
    let dir = assignment.absolute_cgroup_path();
    std::fs::create_dir_all(&dir).with_context(|| format!("create cgroup {}", dir.display()))?;

    if let Some(cores) = assignment.cores.as_ref() {
        write_cgroup_file(&dir.join("cpuset.cpus"), &format_cpu_list(cores))?;
    }
    if let Some(shares) = assignment.cpu_weight {
        write_cgroup_file(
            &dir.join("cpu.weight"),
            &shares_to_weight(shares).to_string(),
        )?;
    }
    if let Some(bytes) = assignment.memory_limit_bytes {
        write_cgroup_file(&dir.join("memory.max"), &bytes.to_string())?;
        // No swap on top of the limit, so a leaking task cannot escape it by
        // swapping (the K8s semantic; docker's default of 2x-limit-in-swap
        // makes limits soft in practice, which defeats scheduling against
        // them). cgroup v2's `memory.swap.max` counts *swap only*, so the
        // equivalent of the OCI spec's `swap == limit` is 0 here.
        write_cgroup_file(&dir.join("memory.swap.max"), "0")?;
    }
    if let Some(bytes) = assignment.memory_low_bytes {
        write_cgroup_file(&dir.join("memory.low"), &bytes.to_string())?;
    }
    Ok(())
}

/// Remove a task's cgroup. Best-effort: an rmdir that fails because a process
/// is still leaving is normal during teardown, and the next start of the same
/// allocation reuses the directory anyway.
fn remove_task_cgroup(assignment: &ResourceAssignment) {
    let dir = assignment.absolute_cgroup_path();
    if dir.exists() {
        if let Err(err) = std::fs::remove_dir(&dir) {
            debug!(path = %dir.display(), error = ?err, "Could not remove task cgroup");
        }
    }
}

/// Kill every process in a task's cgroup, descendants included.
///
/// This is what `WorkerCgroup` buys over `kill(pid)`: a double-forked child
/// that reparented away from the tracked pid still lives in the cgroup, so
/// `cgroup.kill` reaps it. Best-effort — a kernel older than 5.14 has no
/// `cgroup.kill`, and the tracked-pid kill still runs either way.
pub fn kill_cgroup(assignment: &ResourceAssignment) -> bool {
    let path = assignment.absolute_cgroup_path().join("cgroup.kill");
    match std::fs::write(&path, "1") {
        Ok(()) => true,
        Err(err) => {
            debug!(path = %path.display(), error = ?err, "cgroup.kill unavailable");
            false
        }
    }
}

/// Whether the kernel OOM-killed anything in this task's cgroup, read from
/// `memory.events`. Consulted on a nonzero exit so the failure record can say
/// *which limit fired* — exit code 137 alone sends the author hunting through
/// dmesg.
pub fn oom_kill_count(assignment: &ResourceAssignment) -> u64 {
    let path = assignment.absolute_cgroup_path().join("memory.events");
    let Ok(contents) = std::fs::read_to_string(path) else {
        return 0;
    };
    contents
        .lines()
        .find_map(|line| line.strip_prefix("oom_kill "))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn write_cgroup_file(path: &Path, value: &str) -> R<()> {
    std::fs::write(path, value).with_context(|| format!("write {} = {value:?}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn task_with(resources: Value) -> TaskConfig {
        TaskConfig {
            resources,
            ..TaskConfig::new("web", "container")
        }
    }

    /// A governor built with a known CPU set, without touching /sys. Every
    /// allocator assertion below runs on this, so the arithmetic is testable
    /// on any machine and in CI.
    fn governor(cores: &[u32]) -> NodeResourceGovernor {
        NodeResourceGovernor {
            enabled: true,
            root: PathBuf::from("legion"),
            usable_cores: cores.to_vec(),
            reserved: BTreeMap::new(),
        }
    }

    #[test]
    fn cpu_lists_round_trip_through_the_kernel_format() {
        assert_eq!(parse_cpu_list("0-3,7"), vec![0, 1, 2, 3, 7]);
        assert_eq!(parse_cpu_list(" 2 "), vec![2]);
        assert_eq!(parse_cpu_list(""), Vec::<u32>::new());
        assert_eq!(format_cpu_list(&[0, 1, 2, 3, 7]), "0-3,7");
        assert_eq!(format_cpu_list(&[3, 7]), "3,7");
        assert_eq!(format_cpu_list(&[]), "");
        // Runs collapse regardless of input order or duplicates.
        assert_eq!(format_cpu_list(&[5, 4, 4, 6]), "4-6");
    }

    #[test]
    fn declared_resources_map_to_kernel_units() {
        assert_eq!(
            memory_limit_bytes(&json!({ "memory": 256 })),
            Some(256 * 1024 * 1024)
        );
        assert_eq!(memory_limit_bytes(&json!({ "memory": 0 })), None);
        assert_eq!(memory_limit_bytes(&json!({})), None);

        assert_eq!(cpu_weight_shares(&json!({ "cpu": 500 })), Some(500));
        // `cpus` is the scheduler's alias for `cpu` (MHz) — NOT for `cores`.
        assert_eq!(cpu_weight_shares(&json!({ "cpus": 500 })), Some(500));
        assert_eq!(cpu_weight_shares(&json!({})), None);

        assert_eq!(declared_cores(&json!({ "cores": 2 })), Some(2.0));
        assert_eq!(
            declared_cores(&json!({ "cpus": 1.5 })),
            None,
            "a fractional `cpus` declaration is a CPU share, not a cpuset request"
        );
        assert_eq!(declared_cores(&json!({})), None);
    }

    /// The shares→weight mapping systemd and Nomad use, at both ends and in
    /// the middle.
    #[test]
    fn shares_convert_to_cgroup_v2_weights() {
        assert_eq!(shares_to_weight(2), 1);
        // 1 + ((1024 - 2) * 9999) / 262142, integer division.
        assert_eq!(shares_to_weight(1024), 39);
        assert_eq!(shares_to_weight(262_144), 10_000);
        // Below the floor is clamped rather than underflowing.
        assert_eq!(shares_to_weight(0), 1);
    }

    /// Lowest free first, and the share slice loses exactly what was
    /// reserved.
    #[test]
    fn cores_are_assigned_lowest_free_first_and_leave_the_share_slice() -> R<()> {
        let mut governor = governor(&[0, 1, 2, 3]);
        let assignment = governor
            .plan(
                7,
                &task_with(json!({ "cores": 2 })),
                ResourceEnforcement::RuntimeApplied,
            )
            .expect("assignment computed")
            .expect("governed");
        assert_eq!(assignment.cores.as_deref(), Some([0, 1].as_slice()));
        assert_eq!(assignment.slice, Slice::Reserve);
        assert_eq!(
            assignment.cgroup_path,
            PathBuf::from("legion/reserve/legion-7")
        );
        assert_eq!(governor.share_cores(), vec![2, 3]);
        assert_eq!(governor.reserved_cores(), vec![0, 1]);

        // The next reservation takes the next-lowest free pair.
        let second = governor
            .plan(
                8,
                &task_with(json!({ "cores": 1 })),
                ResourceEnforcement::RuntimeApplied,
            )?
            .expect("governed");
        assert_eq!(second.cores.as_deref(), Some([2].as_slice()));
        assert_eq!(governor.share_cores(), vec![3]);
        Ok(())
    }

    /// Release returns the cores and is idempotent — a double release (stop
    /// then destroy) must not corrupt the mask.
    #[test]
    fn releasing_returns_cores_and_is_idempotent() -> R<()> {
        let mut governor = governor(&[0, 1, 2, 3]);
        let assignment = governor
            .plan(
                7,
                &task_with(json!({ "cores": 2 })),
                ResourceEnforcement::RuntimeApplied,
            )?
            .expect("governed");

        governor.release(7, Some(&assignment));
        assert_eq!(governor.share_cores(), vec![0, 1, 2, 3]);
        governor.release(7, Some(&assignment));
        assert_eq!(governor.share_cores(), vec![0, 1, 2, 3]);
        Ok(())
    }

    /// Exhaustion fails the allocation with an error naming what is free and
    /// what is taken, rather than silently under-pinning or wedging.
    #[test]
    fn core_exhaustion_names_the_free_and_reserved_sets() -> R<()> {
        let mut governor = governor(&[0, 1, 2, 3]);
        governor.plan(
            7,
            &task_with(json!({ "cores": 3 })),
            ResourceEnforcement::RuntimeApplied,
        )?;

        let err = governor
            .plan(
                8,
                &task_with(json!({ "cores": 2 })),
                ResourceEnforcement::RuntimeApplied,
            )
            .expect_err("only one core is free");
        let message = err.to_string();
        assert!(message.contains("cannot reserve 2"), "{message}");
        assert!(message.contains("0-2"), "reserved set named: {message}");
        assert!(message.contains("0-3"), "usable set named: {message}");
        Ok(())
    }

    /// Fractional cores cannot be a cpuset, and the error says what to use
    /// instead.
    #[test]
    fn fractional_cores_are_rejected_by_the_allocator() {
        let mut governor = governor(&[0, 1, 2, 3]);
        let err = governor
            .plan(
                7,
                &task_with(json!({ "cores": 1.5 })),
                ResourceEnforcement::RuntimeApplied,
            )
            .expect_err("half a CPU is not a cpuset");
        let message = err.to_string();
        assert!(message.contains("whole number"), "{message}");
        assert!(
            message.contains("cpu"),
            "points at the alternative: {message}"
        );
    }

    /// A share-slice task carries its weight and memory limit, no cores, and
    /// lands under `share/`.
    #[test]
    fn a_share_slice_task_gets_weight_and_memory_only() -> R<()> {
        let mut governor = governor(&[0, 1]);
        let assignment = governor
            .plan(
                12,
                &task_with(json!({ "cpu": 500, "memory": 128 })),
                ResourceEnforcement::RuntimeApplied,
            )?
            .expect("governed");
        assert_eq!(assignment.slice, Slice::Share);
        assert_eq!(
            assignment.cgroup_path,
            PathBuf::from("legion/share/legion-12")
        );
        assert_eq!(assignment.cores, None);
        assert_eq!(assignment.cpu_weight, Some(500));
        assert_eq!(assignment.memory_limit_bytes, Some(128 * 1024 * 1024));
        Ok(())
    }

    /// `memoryMax` raises the ceiling, and the reservation becomes the
    /// protected floor (`memory.low`). Without it the two are the same.
    #[test]
    fn memory_max_is_the_ceiling_and_memory_the_protected_floor() -> R<()> {
        let mut governor = governor(&[0, 1]);
        let bursting = governor
            .plan(
                13,
                &task_with(json!({ "memory": 400, "memoryMax": 2048 })),
                ResourceEnforcement::WorkerCgroup,
            )?
            .expect("governed");
        assert_eq!(bursting.memory_limit_bytes, Some(2048 * 1024 * 1024));
        assert_eq!(bursting.memory_low_bytes, Some(400 * 1024 * 1024));

        let fixed = governor
            .plan(
                14,
                &task_with(json!({ "memory": 400 })),
                ResourceEnforcement::WorkerCgroup,
            )?
            .expect("governed");
        assert_eq!(fixed.memory_limit_bytes, Some(400 * 1024 * 1024));
        assert_eq!(fixed.memory_low_bytes, Some(400 * 1024 * 1024));
        Ok(())
    }

    /// The `Unsupported` contract: no assignment computed, nothing touched.
    /// This is what keeps unported dylib plugins behaving exactly as today.
    #[test]
    fn an_unsupported_runtime_is_never_assigned() -> R<()> {
        let mut governor = governor(&[0, 1]);
        assert!(governor
            .plan(
                1,
                &task_with(json!({ "cores": 1, "memory": 64 })),
                ResourceEnforcement::Unsupported,
            )?
            .is_none());
        assert_eq!(governor.share_cores(), vec![0, 1], "no cores were taken");
        Ok(())
    }

    /// And a disabled governor governs nothing at all, whatever the runtime
    /// reports — the opt-in promise.
    #[test]
    fn a_disabled_governor_assigns_nothing() -> R<()> {
        let mut governor = NodeResourceGovernor::disabled();
        assert!(!governor.enabled());
        assert!(governor
            .plan(
                1,
                &task_with(json!({ "cores": 1 })),
                ResourceEnforcement::WorkerCgroup,
            )?
            .is_none());
        Ok(())
    }

    /// Restart recovery: assignments read back from the persisted records
    /// rebuild the allocator's view, so a restarted worker does not hand out
    /// a core a running task already owns.
    #[test]
    fn restore_rebuilds_the_allocator_from_persisted_assignments() -> R<()> {
        let mut governor = governor(&[0, 1, 2, 3]);
        let persisted = ResourceAssignment {
            slice: Slice::Reserve,
            cgroup_path: PathBuf::from("legion/reserve/legion-5"),
            cores: Some(vec![0, 1]),
            cpu_weight: None,
            memory_limit_bytes: None,
            memory_low_bytes: None,
        };
        governor.restore(5, &persisted);
        assert_eq!(governor.share_cores(), vec![2, 3]);

        let next = governor
            .plan(
                6,
                &task_with(json!({ "cores": 1 })),
                ResourceEnforcement::RuntimeApplied,
            )?
            .expect("governed");
        assert_eq!(
            next.cores.as_deref(),
            Some([2].as_slice()),
            "a restored reservation is not handed out twice"
        );

        // A record with no assignment (written before the field existed)
        // restores as nothing rather than panicking.
        let unpinned = ResourceAssignment {
            slice: Slice::Share,
            cgroup_path: PathBuf::from("legion/share/legion-9"),
            cores: None,
            cpu_weight: None,
            memory_limit_bytes: None,
            memory_low_bytes: None,
        };
        governor.restore(9, &unpinned);
        assert_eq!(governor.reserved_cores(), vec![0, 1, 2]);
        Ok(())
    }

    /// Re-dispatching an allocation id releases its old reservation instead
    /// of leaking it — in-place relaunch is a normal path.
    #[test]
    fn reassigning_the_same_allocation_reuses_its_cores() -> R<()> {
        let mut governor = governor(&[0, 1, 2, 3]);
        let first = governor
            .plan(
                7,
                &task_with(json!({ "cores": 2 })),
                ResourceEnforcement::RuntimeApplied,
            )?
            .expect("governed");
        let second = governor
            .plan(
                7,
                &task_with(json!({ "cores": 2 })),
                ResourceEnforcement::RuntimeApplied,
            )?
            .expect("governed");
        assert_eq!(first.cores, second.cores);
        assert_eq!(governor.share_cores(), vec![2, 3]);
        Ok(())
    }

    /// The share slice must never be emptied: every unpinned governed task on
    /// the node lives in it.
    #[test]
    fn reserving_every_core_is_refused() {
        let mut governor = governor(&[0, 1]);
        let err = governor
            .plan(
                7,
                &task_with(json!({ "cores": 2 })),
                ResourceEnforcement::RuntimeApplied,
            )
            .expect_err("that would leave the share slice empty");
        assert!(err.to_string().contains("share slice"), "{err}");
    }

    /// The burstable limits land in a real cgroup: `memory.max` is the
    /// ceiling, `memory.low` the reservation, and `memory.reclaim` accepts a
    /// write (Linux 5.19+). Needs root on a cgroup v2 host with the memory
    /// controller delegable at the root. See
    /// internal_docs/suspend-resume-linux-verification.md, "Phase 2".
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "needs root and a cgroup v2 host"]
    fn burstable_limits_land_in_a_real_cgroup() -> R<()> {
        let root = format!("legion-phase2-test-{}", std::process::id());
        let mut governor = NodeResourceGovernor {
            enabled: true,
            root: PathBuf::from(&root),
            usable_cores: fingerprint_usable_cores(),
            reserved: BTreeMap::new(),
        };
        let assignment = governor
            .assign(
                77,
                &task_with(json!({ "memory": 64, "memoryMax": 128 })),
                ResourceEnforcement::WorkerCgroup,
                None,
            )?
            .expect("governed");
        let dir = assignment.absolute_cgroup_path();
        let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap_or_default();
        assert_eq!(read("memory.max").trim(), (128u64 << 20).to_string());
        assert_eq!(read("memory.low").trim(), (64u64 << 20).to_string());
        let reclaim = std::fs::write(dir.join("memory.reclaim"), "1048576");
        assert!(
            reclaim.is_ok() || reclaim.as_ref().err().and_then(|e| e.raw_os_error()) == Some(11),
            "{reclaim:?}"
        );
        governor.release(77, Some(&assignment));
        for leaf in ["share", "reserve", ""] {
            let _ = std::fs::remove_dir(PathBuf::from(CGROUP_ROOT).join(&root).join(leaf));
        }
        Ok(())
    }
}
