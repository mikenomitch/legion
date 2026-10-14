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
    /// A promoted session (docs/session-mode.md): it still runs inside its
    /// task's envelope, so a restarted worker keeps counting it against the
    /// envelope's room.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub in_envelope: bool,
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
    /// Set while the allocation is stopping: out of the manager's desired
    /// set, but its process may still be alive and its volumes mounted
    /// (draining, or inside its stop grace window; see docs/graceful-stop.md).
    ///
    /// Persisted so a restarted worker finishes the stop instead of losing
    /// track of a task that ignored its signal, and keeps that task's volume
    /// mounts recorded as in use until the process is really gone. `None`
    /// for running work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopping: Option<PersistedStop>,
}

/// Where a stopping allocation was in the stop ladder when it was persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedStop {
    pub cause: crate::structs::StopCause,
    /// Whether teardown removes the allocation directory.
    pub remove_dirs: bool,
    /// When the grace window closes and the task is hard-killed, in Unix
    /// seconds. `None` while it is still draining and has not been
    /// signalled yet; a restarted worker signals it straight away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kill_deadline_unix: Option<u64>,
}

/// Directories beside the state file that hold node data no task may reach
/// by path (`<state dir>/<name>`): captured logs, the content store, volume
/// data, and the node's key. Each is owner-only, because the state dir
/// itself is traverse-only.
const STATE_PRIVATE_SUBDIRS: [&str; 4] = ["logs", "content", "volumes", "identity"];
/// The per-allocation directories (`<state dir>/allocs/`), traverse-only.
const STATE_ALLOCS_SUBDIR: &str = "allocs";

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

    /// Root of the node content store (`<state-dir>/content`). Beside the
    /// state file for the same reason logs are: it is node state that must
    /// survive worker restarts.
    pub fn content_dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(|parent| parent.join("content"))
            .unwrap_or_else(|| PathBuf::from("content"))
    }

    /// Root of the sandbox root disk cache (`<state-dir>/rootfs-cache`). It
    /// must share a filesystem with the allocation dirs, which sit beside
    /// it, so a sandbox's disk can be a reflink of its cached base.
    pub fn rootfs_cache_dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(|parent| parent.join("rootfs-cache"))
            .unwrap_or_else(|| PathBuf::from("rootfs-cache"))
    }

    /// The node's session log (`<state-dir>/sessions.db`, session
    /// durability plan §6.2).
    pub fn sessions_db_path(&self) -> PathBuf {
        self.path.with_file_name("sessions.db")
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
        self.ensure_state_dir()?;
        // The state file embeds each task's full env, including any
        // env-delivered workload-identity token. The state dir is
        // traverse-only (see `prepare_layout`), so the file's own 0600 mode,
        // set atomically by the write, is what keeps it private.
        let contents = serde_json::to_string_pretty(allocations)?;
        write_owner_only(&self.path, contents.as_bytes(), true)?;
        Ok(())
    }

    /// Lay out the worker state directory with each entry's guard, before
    /// anything is written into it. Called once at worker start.
    ///
    /// The state dir and `allocs/` are traverse-only (`0711`): a
    /// user-namespaced container's root is a shifted host id, and an `exec`
    /// task's `user` is not the worker's, yet both must reach their own
    /// allocation directory by path (runc resolves the bundle's `rootfs` as
    /// container root). The allocation directory below is the real boundary
    /// (owner-only, or `0710` to the allocation's own id range; see
    /// `userns::grant_traversal`). Every other entry guards itself: the
    /// state files are written `0600`, and the directories here are created
    /// owner-only up front, so a later `create_dir_all` beneath one never
    /// leaves it at the umask.
    pub fn prepare_layout(&self) -> R<()> {
        self.ensure_state_dir()?;
        let Some(dir) = self.dir() else {
            return Ok(());
        };
        for private in STATE_PRIVATE_SUBDIRS {
            let path = dir.join(private);
            ensure_dir(&path)?;
            crate::util::restrict_dir_to_owner(&path)?;
        }
        let allocs = dir.join(STATE_ALLOCS_SUBDIR);
        ensure_dir(&allocs)?;
        crate::util::restrict_dir_to_traverse_only(&allocs)?;
        Ok(())
    }

    /// File holding the wall-clock time of this worker's last successful
    /// manager contact (registration or heartbeat), beside the state file.
    pub fn last_contact_path(&self) -> PathBuf {
        self.path.with_file_name("last_manager_contact")
    }

    /// Record a successful manager contact at `at`.
    ///
    /// Written on every successful heartbeat, so it skips the fsync: losing
    /// the last few writes to a host crash only makes the recorded contact
    /// older, which fences earlier, never later.
    pub fn record_manager_contact(&self, at: std::time::SystemTime) -> R<()> {
        let secs = at
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_secs();
        write_owner_only(
            &self.last_contact_path(),
            secs.to_string().as_bytes(),
            false,
        )
    }

    /// The last successful manager contact a previous worker process
    /// recorded, or `None` when none was (a fresh node, or state written by
    /// a worker that predates the record).
    pub fn last_manager_contact(&self) -> Option<std::time::SystemTime> {
        let raw = fs::read_to_string(self.last_contact_path()).ok()?;
        let secs = raw.trim().parse::<u64>().ok()?;
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
    }

    /// How long ago, as of `now`, the recorded last contact was.
    ///
    /// A wall clock is the only clock that survives a restart, so this is
    /// exposed to clock jumps. A clock stepped backwards past the record
    /// reads as zero (no time lost: the fence waits longer, as if the worker
    /// had just heard from its manager); a clock stepped forwards reads as
    /// more silence than really passed and fences early. Early is the safe
    /// side of the at-most-one guarantee.
    pub fn since_last_manager_contact(
        &self,
        now: std::time::SystemTime,
    ) -> Option<std::time::Duration> {
        let last = self.last_manager_contact()?;
        Some(now.duration_since(last).unwrap_or_default())
    }

    /// File holding this node's manager-issued node token, beside the state
    /// file. Mode 0600: the state dir itself is traverse-only.
    pub fn node_token_path(&self) -> PathBuf {
        self.path.with_file_name("node_token")
    }

    /// Persist the node token so a restarted worker re-registers with it
    /// instead of its join token, which may have expired or been revoked
    /// (docs/auth-and-acls.md). Fsynced: losing it costs a rejoin.
    pub fn save_node_token(&self, token: &str) -> R<()> {
        self.ensure_state_dir()?;
        write_owner_only(&self.node_token_path(), token.as_bytes(), true)
    }

    /// The node token a previous worker process persisted, if any.
    pub fn load_node_token(&self) -> Option<String> {
        let raw = fs::read_to_string(self.node_token_path()).ok()?;
        let token = raw.trim();
        (!token.is_empty()).then(|| token.to_string())
    }

    /// Forget the persisted node token (the manager refused it).
    pub fn clear_node_token(&self) -> R<()> {
        match fs::remove_file(self.node_token_path()) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// File holding every manager address this worker has learned
    /// (`HeartbeatReply.manager_addresses`), beside the state file.
    pub fn known_managers_path(&self) -> PathBuf {
        self.path.with_file_name("managers.json")
    }

    /// Persist the manager set so a restarted worker can reach the cluster
    /// even when the manager its join token names is down.
    pub fn save_known_managers(&self, addresses: &[String]) -> R<()> {
        self.ensure_state_dir()?;
        let contents = serde_json::to_vec_pretty(addresses)?;
        write_owner_only(&self.known_managers_path(), &contents, false)
    }

    /// The manager addresses a previous worker process learned; empty when
    /// none were recorded or the file is unreadable.
    pub fn load_known_managers(&self) -> Vec<String> {
        fs::read(self.known_managers_path())
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Vec<String>>(&bytes).ok())
            .unwrap_or_default()
    }

    /// The directory holding the state file, when it has one.
    fn dir(&self) -> Option<&Path> {
        self.path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
    }

    /// Create the state dir and re-assert its traverse-only mode (see
    /// [`Self::prepare_layout`]) before writing a file into it.
    fn ensure_state_dir(&self) -> R<()> {
        if let Some(dir) = self.dir() {
            ensure_dir(dir)?;
            crate::util::restrict_dir_to_traverse_only(dir)?;
        }
        Ok(())
    }
}

/// Atomically write `contents` to `path` with mode 0600 (temp file created at
/// that mode from the start, then renamed), so token bytes in the worker state
/// are never observable at the process umask. Off Unix this is a plain write.
/// `sync` fsyncs the bytes before the rename.
fn write_owner_only(path: &Path, contents: &[u8], sync: bool) -> R<()> {
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
        if sync {
            file.sync_all()
                .with_context(|| format!("failed to fsync worker state {tmp:?}"))?;
        }
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

/// SIGKILL what is left of a restored task's process group when the
/// tracked process itself is gone. Returns whether anything was signalled.
///
/// Every task is spawned as its own session and process-group leader
/// (`setsid` in `spawn_task_process`), so `pid` is also the group id. When
/// the leader dies while the worker is down — a `sh -c` wrapper killed by a
/// service manager, say — its children keep the group alive: still running,
/// still holding the task's ports and volumes, and no longer in any record
/// once restore skips the dead pid. That is how a re-joined worker ended up
/// next to unmanaged processes that collided with the replacements the
/// manager placed (spot-check P1-5). Linux never hands out a pid that is in
/// use as a process-group id, so a live group with this id can only be the
/// task's own descendants.
pub fn kill_orphaned_task_group(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // `kill(-1, ...)` would signal every process we may signal. Never.
        if pid <= 1 || pid > i32::MAX as u32 {
            return false;
        }
        let group = -(pid as i32);
        // Signal 0 only checks that the group still has a member.
        // SAFETY: plain libc calls with a validated negative group id.
        if unsafe { libc::kill(group, 0) } != 0 {
            return false;
        }
        unsafe { libc::kill(group, libc::SIGKILL) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
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
    #[cfg(target_os = "linux")]
    {
        linux_process_start_time(pid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        sysinfo_process_start_time(pid)
    }
}

/// [`process_start_time`] from `/proc/<pid>/stat` alone. `sysinfo` asked for
/// one pid still lists all of `/proc` and every process's threads to find
/// it, so on a node running a few hundred microVMs each call cost
/// milliseconds of the worker's CPU, and it runs per session start and per
/// stopping allocation on every state write. Same formula as `sysinfo`
/// (boot time plus start ticks over `CLK_TCK`, whole seconds), so a start
/// time recorded by either compares equal.
#[cfg(target_os = "linux")]
fn linux_process_start_time(pid: u32) -> Option<u64> {
    use std::sync::OnceLock;
    static BOOT_TIME: OnceLock<Option<u64>> = OnceLock::new();
    static CLOCK_TICKS: OnceLock<u64> = OnceLock::new();
    let boot_time = (*BOOT_TIME.get_or_init(|| {
        std::fs::read_to_string("/proc/stat")
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("btime "))?
            .trim()
            .parse()
            .ok()
    }))?;
    // SAFETY: sysconf reads a constant.
    let ticks_per_second =
        *CLOCK_TICKS.get_or_init(|| (unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).max(1) as u64);

    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // `pid (comm) state ppid ...`: comm may hold spaces and parentheses, so
    // the fields are counted from the last `)`.
    let mut fields = stat[stat.rfind(')')? + 1..].split_whitespace();
    let state = fields.next()?;
    if state == "Z" || state == "X" {
        return None;
    }
    // Field 22 overall (`starttime`), 19 after the state.
    let started_ticks: u64 = fields.nth(18)?.parse().ok()?;
    Some(boot_time + started_ticks / ticks_per_second)
}

#[cfg(not(target_os = "linux"))]
fn sysinfo_process_start_time(pid: u32) -> Option<u64> {
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

    /// The /proc reading agrees with `sysinfo`, so a start time recorded
    /// before the switch still identifies its process.
    #[cfg(target_os = "linux")]
    #[test]
    fn proc_stat_start_time_matches_sysinfo_test() {
        let pid = std::process::id();
        let mut system = System::new();
        system.refresh_processes(
            sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
            true,
        );
        let expected = system
            .process(Pid::from_u32(pid))
            .map(|process| process.start_time());
        assert!(expected.is_some());
        assert_eq!(linux_process_start_time(pid), expected);
        // No such process.
        assert_eq!(linux_process_start_time(u32::MAX - 7), None);
    }
    use std::process::Command;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn last_manager_contact_round_trips_and_ignores_backward_clocks() -> R<()> {
        let temp_dir = TempDir::new()?;
        let store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        let now = std::time::SystemTime::now();
        assert!(
            store.since_last_manager_contact(now).is_none(),
            "no record, nothing to measure from"
        );

        store.record_manager_contact(now - Duration::from_secs(90))?;
        let since = store.since_last_manager_contact(now).expect("recorded");
        assert!((89..=91).contains(&since.as_secs()), "{since:?}");

        // A clock stepped back past the record reads as no silence at all.
        assert_eq!(
            store.since_last_manager_contact(now - Duration::from_secs(600)),
            Some(Duration::ZERO)
        );
        Ok(())
    }

    /// The node token survives a worker restart, owner-only, and can be
    /// forgotten when the manager refuses it (spot-check P0-8).
    #[test]
    fn node_token_round_trips_owner_only_and_clears() -> R<()> {
        let temp_dir = TempDir::new()?;
        let store = WorkerStateStore::new(temp_dir.path().join("w1").join("worker_state.json"));
        assert_eq!(store.load_node_token(), None);
        store.save_node_token("lgntok_secret")?;
        assert_eq!(store.load_node_token().as_deref(), Some("lgntok_secret"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(store.node_token_path())?.permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "the node token must be owner-only");
        }
        store.clear_node_token()?;
        assert_eq!(store.load_node_token(), None);
        // Clearing twice is fine.
        store.clear_node_token()?;
        Ok(())
    }

    /// A task whose tracked leader died while the worker was down must not
    /// leave its children running unmanaged (spot-check P1-5).
    #[cfg(unix)]
    #[test]
    fn an_orphaned_task_group_is_killed() -> R<()> {
        let temp_dir = TempDir::new()?;
        let child_path = temp_dir.path().join("child");
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(format!(
            "sleep 1000 & echo $! > {}; exit 0",
            child_path.display()
        ));
        let mut leader = crate::task_plugins::spawn_task_process(&mut cmd)?;
        let leader_pid = leader.id();
        leader.wait()?;
        for _ in 0..40 {
            if fs::read_to_string(&child_path).is_ok_and(|raw| !raw.trim().is_empty()) {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let child: u32 = fs::read_to_string(&child_path)?.trim().parse()?;
        assert!(!pid_is_running(leader_pid), "the leader exited");
        assert!(pid_is_running(child), "its child outlived it");

        assert!(kill_orphaned_task_group(leader_pid));
        for _ in 0..40 {
            if !pid_is_running(child) {
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        assert!(!pid_is_running(child), "the orphaned child was killed");
        // (A second call may still find the group: an unreaped zombie stays a
        // member until its new parent reaps it, and signalling it is
        // harmless.)
        assert!(!kill_orphaned_task_group(1), "never signal every process");
        assert!(!kill_orphaned_task_group(0));
        Ok(())
    }

    /// A user-namespaced container's root must be able to traverse the state
    /// dir to its own allocation (spot check: runc's `remount-propagation`
    /// of the bundle rootfs failed with EACCES under a 0700 state dir), and
    /// that must not expose anything else: state files stay 0600 and every
    /// other subdirectory owner-only.
    #[cfg(unix)]
    #[test]
    fn state_layout_is_traverse_only_with_private_entries() -> R<()> {
        use std::os::unix::fs::PermissionsExt;
        let mode = |path: &Path| -> R<u32> { Ok(fs::metadata(path)?.permissions().mode() & 0o777) };
        let temp_dir = TempDir::new()?;
        let dir = temp_dir.path().join("w1");
        let store = WorkerStateStore::new(dir.join("worker_state.json"));
        store.prepare_layout()?;
        store.save(&[])?;
        store.save_node_token("lgntok_secret")?;

        assert_eq!(mode(&dir)?, 0o711, "the state dir is traverse-only");
        assert_eq!(
            mode(&dir.join("allocs"))?,
            0o711,
            "allocs/ is traverse-only"
        );
        for private in STATE_PRIVATE_SUBDIRS {
            assert_eq!(mode(&dir.join(private))?, 0o700, "{private}/ is owner-only");
        }
        for file in [store.path().to_path_buf(), store.node_token_path()] {
            assert_eq!(mode(&file)?, 0o600, "{} is owner-only", file.display());
        }

        // A dir left wider (or narrower) by an older worker is corrected.
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755))?;
        fs::set_permissions(dir.join("content"), fs::Permissions::from_mode(0o755))?;
        store.prepare_layout()?;
        assert_eq!(mode(&dir)?, 0o711);
        assert_eq!(mode(&dir.join("content"))?, 0o700);
        Ok(())
    }

    #[test]
    fn known_managers_round_trip() -> R<()> {
        let temp_dir = TempDir::new()?;
        let store = WorkerStateStore::new(temp_dir.path().join("worker_state.json"));
        assert!(store.load_known_managers().is_empty());
        let managers = vec![
            "http://10.0.0.1:4747".to_string(),
            "http://10.0.0.2:4747".to_string(),
        ];
        store.save_known_managers(&managers)?;
        assert_eq!(store.load_known_managers(), managers);
        Ok(())
    }

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

#[cfg(test)]
mod persist_cost_probe {
    // TEMPORARY measurement, removed before commit.
    use super::*;
    use crate::structs::{Allocation, TaskConfig};

    #[test]
    #[ignore]
    fn persist_cost_by_allocation_count() -> R<()> {
        let temp = tempfile::TempDir::new()?;
        let store = WorkerStateStore::new(temp.path().join("worker_state.json"));
        let mut env = serde_json::Map::new();
        for i in 0..12 {
            env.insert(
                format!("APP_SETTING_{i}"),
                serde_json::json!(format!("value-{i}-{}", "x".repeat(24))),
            );
        }
        let task = TaskConfig {
            config: serde_json::json!({
                "image": "ghcr.io/acme/agent-base:2026-10",
                "command": ["/usr/local/bin/harness", "--listen", "0.0.0.0:8080"],
                "env": env,
                "vcpus": 2,
                "memoryMib": 1024,
            }),
            ..TaskConfig::new("box", "sandbox")
        };
        for n in [10usize, 100, 1000, 5000] {
            let records: Vec<PersistedAllocation> = (0..n)
                .map(|i| PersistedAllocation {
                    allocation: Allocation {
                        id: i as i64,
                        status: "running".into(),
                        resources: serde_json::json!({"cpu": 1000, "memory": 1024}),
                        node_id: "node-1".into(),
                        job_config_id: "job-1-config-abcdef".into(),
                        task_config_id: 1,
                        job_id: "sandboxes".into(),
                        instance_id: Some(format!("inst-{i:012}")),
                        network: None,
                    },
                    task: task.clone(),
                    pid: Some(std::process::id()),
                    pid_start_time: Some(1_700_000_000),
                    self_terminate_after_secs: None,
                    volumes: Vec::new(),
                    external_id: None,
                    in_envelope: false,
                    resource_assignment: None,
                    stopping: None,
                })
                .collect();
            let bytes = serde_json::to_string_pretty(&records)?.len();
            let rounds = if n >= 5000 { 5 } else { 20 };
            let started = std::time::Instant::now();
            for _ in 0..rounds {
                store.save(&records)?;
            }
            let per = started.elapsed() / rounds;
            let started = std::time::Instant::now();
            for _ in 0..rounds {
                let _ = serde_json::to_string_pretty(&records)?;
            }
            let ser = started.elapsed() / rounds;
            let started = std::time::Instant::now();
            for _ in 0..rounds {
                for r in &records {
                    let _ = process_start_time(r.pid.unwrap());
                }
            }
            let proc_reads = started.elapsed() / rounds;
            let started = std::time::Instant::now();
            for _ in 0..rounds {
                let _ = records.clone();
            }
            let clones = started.elapsed() / rounds;
            eprintln!(
                "n={n} bytes={bytes} save={per:?} serialize={ser:?} start_time_reads={proc_reads:?} clone={clones:?}"
            );
        }
        Ok(())
    }
}
