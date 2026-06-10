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
            .unwrap_or_else(|_| PathBuf::from(DEF_WORKER_STATE_PATH))
    }

    pub fn path_for_worker(worker_id: &str) -> PathBuf {
        std::env::var("LEGION_WORKER_STATE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(DEF_WORKER_STATE_DIR)
                    .join(worker_id)
                    .join("worker_state.json")
            })
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
        }
        let contents = serde_json::to_string_pretty(allocations)?;
        fs::write(&self.path, contents)?;
        Ok(())
    }
}

pub fn pid_is_running(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
    system.process(Pid::from_u32(pid)).is_some()
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
