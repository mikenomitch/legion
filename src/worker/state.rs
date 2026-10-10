use crate::prelude::*;
use crate::structs::{Allocation, TaskConfig};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use sysinfo::{Pid, System};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedAllocation {
    pub allocation: Allocation,
    pub task: TaskConfig,
    pub pid: Option<u32>,
    /// Start time of `pid` (seconds since the Unix epoch) when it was
    /// persisted. A restarted worker re-adopts `pid` only when the live
    /// process with that pid started at this same time, so a recycled pid
    /// (after a host reboot, say) is never mistaken for the task.
    ///
    /// Absent in records written before this field existed; those fall back
    /// to a plain liveness check for the one restart that upgrades them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid_start_time: Option<u64>,
    /// Self-fence window in seconds (`TaskAllocation.self_terminate_after_seconds`):
    /// kill this allocation after losing manager contact for this long.
    /// Persisted so the watchdog keeps protecting restored allocations after
    /// a worker restart. `None`/absent = stateless allocation, no fencing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub self_terminate_after_secs: Option<u64>,
    /// Volume mounts the manager last dispatched for this allocation (see
    /// docs/volumes.md). Persisted so a restarted worker re-mounts exactly
    /// this storage instead of waiting for the next dispatch — a restored
    /// task is still running and still needs its data directory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<crate::structs::VolumeMount>,
    /// Manager-minted allocation id for work that started life as a
    /// provisional allocation (docs/edge-boot.md).
    ///
    /// A provisional start has no allocation id — the manager mints those
    /// inside the apply transaction — so the worker keys it by a negative
    /// local key and records the real id here once the claim is ratified.
    /// Persisting the pair is what lets a restarted worker keep serving the
    /// running task under the id the cluster knows it by, instead of
    /// relaunching it under a key nothing matches.
    ///
    /// Absent for every ordinary allocation, where `allocation.id` already is
    /// the manager's id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<i64>,
    /// The CPU/memory placement worker core handed this allocation (see
    /// `src/worker/resources.rs`). Persisted because it is the *unit of
    /// recovery*: a restarted worker rebuilds its core allocator and both
    /// slice masks from these records, so a running task's exclusive cores
    /// are never handed out twice.
    ///
    /// Absent in records written before enforcement existed, and in every
    /// record on a worker that does not enforce — both load unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_assignment: Option<crate::task_plugins::ResourceAssignment>,
}

#[derive(Debug, Clone)]
pub struct WorkerStateStore {
    path: PathBuf,
}

impl WorkerStateStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Location of the worker state file; per-allocation directories live
    /// beside it (`<parent>/allocs/`).
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn default_path() -> PathBuf {
        std::env::var("LEGION_WORKER_STATE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| Self::path_for_worker("default"))
    }

    pub fn path_for_worker(worker_id: &str) -> PathBuf {
        std::env::var("LEGION_WORKER_STATE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| worker_state_dir().join(worker_id).join("worker_state.json"))
    }

    /// Base directory under which per-allocation log directories live.
    ///
    /// Logs sit beside the worker state file (`<state-dir>/logs`) so they share
    /// the worker's data directory and survive worker restarts.
    pub fn logs_base_dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(|parent| parent.join("logs"))
            .unwrap_or_else(|| PathBuf::from("logs"))
    }

    /// Directory holding the captured stdout/stderr files for one allocation.
    pub fn allocation_log_dir(&self, allocation_id: i64) -> PathBuf {
        self.logs_base_dir().join(allocation_id.to_string())
    }

    pub fn load(&self) -> R<Vec<PersistedAllocation>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let contents = fs::read_to_string(&self.path)?;
        if contents.trim().is_empty() {
            return Ok(Vec::new());
        }
        let allocations = serde_json::from_str(&contents)?;
        Ok(allocations)
    }

    pub fn save(&self, allocations: &[PersistedAllocation]) -> R<()> {
        if let Some(parent) = self.path.parent() {
            ensure_dir(parent)?;
            // The state file embeds each task's full env — including any
            // env-delivered workload-identity token — so keep it owner-only.
            // The directory guard denies traversal to the file regardless of
            // its own mode; the atomic 0600 write below is defense in depth.
            if !parent.as_os_str().is_empty() {
                crate::util::restrict_dir_to_owner(parent)?;
            }
        }
        let contents = serde_json::to_string_pretty(allocations)?;
        write_owner_only(&self.path, contents.as_bytes())?;
        Ok(())
    }
}

/// Atomically write `contents` to `path` with mode 0600 (temp file created at
/// that mode from the start, then renamed), so token bytes in the worker state
/// are never observable at the process umask. Off Unix this is a plain write.
fn write_owner_only(path: &Path, contents: &[u8]) -> R<()> {
    use std::io::Write;

    let tmp = path.with_file_name(format!(
        ".{}.tmp-{}",
        path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "worker_state.json".into()),
        crate::util::random_string(6)
    ));

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let write_result = (|| -> R<()> {
        let mut file = options
            .open(&tmp)
            .with_context(|| format!("failed to create worker state temp file {tmp:?}"))?;
        file.write_all(contents)
            .with_context(|| format!("failed to write worker state {tmp:?}"))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync worker state {tmp:?}"))?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&tmp);
        return Err(err);
    }
    if let Err(err) = fs::rename(&tmp, path)
        .with_context(|| format!("failed to move worker state into place at {path:?}"))
    {
        let _ = fs::remove_file(&tmp);
        return Err(err);
    }
    Ok(())
}

/// Whether `pid` names a live (non-zombie) process.
///
/// Called once per restored allocation per supervisor tick, under the
/// task-manager mutex, so it reads only this one pid rather than rescanning
/// the whole process table.
pub fn pid_is_running(pid: u32) -> bool {
    process_start_time(pid).is_some()
}

/// When the live process `pid` started, in seconds since the Unix epoch, or
/// `None` when no such process is running (a zombie counts as gone: it will
/// never run again).
///
/// Paired with the pid in [`PersistedAllocation::pid_start_time`] it names
/// one process rather than a number the kernel recycles: after a host reboot
/// or enough churn the same pid belongs to something else, which starts at a
/// different time.
pub fn process_start_time(pid: u32) -> Option<u64> {
    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
        sysinfo::ProcessRefreshKind::new(),
    );
    system
        .process(pid)
        .filter(|process| process.status() != sysinfo::ProcessStatus::Zombie)
        .map(|process| process.start_time())
}

fn ensure_dir(path: &Path) -> R<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    fs::create_dir_all(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn child_survives_parent_kill() -> R<()> {
        let temp_dir = TempDir::new()?;
        let pid_path = temp_dir.path().join("pid");
        let pid_path_str = pid_path.display().to_string();
        let mut parent = Command::new("sh")
            .arg("-c")
            .arg(format!("sleep 1000 & echo $! > {pid_path_str}; wait"))
            .spawn()?;

        for _ in 0..20 {
            if pid_path.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let pid: u32 = fs::read_to_string(&pid_path)?.trim().parse()?;

        parent.kill()?;
        thread::sleep(Duration::from_millis(100));

        assert!(pid_is_running(pid));

        let mut system = System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
        if let Some(process) = system.process(Pid::from_u32(pid)) {
            process.kill();
        }

        Ok(())
    }
}
