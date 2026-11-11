//! Reading and rotating captured task stdout/stderr.
//!
//! Workers redirect each task's stdout/stderr into `stdout.log` / `stderr.log`
//! under a per-allocation directory (see [`crate::task_plugins`]). This module:
//!
//! - **reads** those files back for the `Worker.GetTaskLogs` RPC, supporting a
//!   byte cap, a head/tail window, and an `offset` for incremental tailing; and
//! - **rotates/prunes** them so a chatty task cannot fill a worker's disk.
//!
//! ## Why copy-truncate rotation is safe here
//!
//! Tasks are spawned with their stdout/stderr pointed at a file opened with
//! `O_APPEND` (see [`crate::task_plugins::configure_task_log_capture`]). Under
//! `O_APPEND` every write atomically seeks to the *current* end of file first,
//! so after the worker copies the active file aside and truncates it to zero,
//! the task's next write lands at offset 0 — no sparse hole, no lost offset.
//! This lets the worker own rotation without being in the task's write path,
//! which in turn preserves the property that tasks keep logging across worker
//! restarts (the child holds a real file fd, not a pipe to the worker).

use crate::prelude::*;
use crate::task_plugins::{STDERR_LOG_FILE, STDOUT_LOG_FILE};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// One stream's captured content, ready to place into a `TaskLogStream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedStream {
    pub stream: String,
    pub content: String,
    /// Total size of the underlying active log file, in bytes.
    pub size_bytes: i64,
    /// Byte offset within the file where `content` begins.
    pub start_offset: i64,
    /// Byte offset to resume from on the next incremental read
    /// (`start_offset` + bytes actually returned).
    pub next_offset: i64,
    /// Whether `content` was truncated to satisfy the byte cap.
    pub truncated: bool,
}

/// Worker-side task log rotation/retention policy.
#[derive(Debug, Clone, Copy)]
pub struct LogConfig {
    /// Rotate a stream's active file once it grows beyond this many bytes.
    pub max_file_bytes: u64,
    /// Number of rotated generations to keep per stream (`stdout.log.1..N`).
    pub max_files: u32,
    /// Delete rotated files older than this many days.
    pub retention_days: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: DEF_TASK_LOG_MAX_FILE_BYTES,
            max_files: DEF_TASK_LOG_MAX_FILES,
            retention_days: DEF_TASK_LOG_RETENTION_DAYS,
        }
    }
}

impl LogConfig {
    /// Resolve the policy from env vars, falling back to built-in defaults so
    /// log rotation works with zero configuration.
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        if let Some(v) = env_u64("LEGION_LOG_MAX_FILE_BYTES") {
            cfg.max_file_bytes = v.max(1);
        }
        if let Some(v) = env_u64("LEGION_LOG_MAX_FILES") {
            cfg.max_files = v as u32;
        }
        if let Some(v) = env_u64("LEGION_LOG_RETENTION_DAYS") {
            cfg.retention_days = v;
        }
        cfg
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.trim().parse::<u64>().ok()
}

/// Parse the requested stream filter. Empty / "all" selects both streams.
fn streams_for_filter(filter: &str) -> Vec<(&'static str, &'static str)> {
    match filter.trim().to_ascii_lowercase().as_str() {
        "stdout" => vec![("stdout", STDOUT_LOG_FILE)],
        "stderr" => vec![("stderr", STDERR_LOG_FILE)],
        _ => vec![("stdout", STDOUT_LOG_FILE), ("stderr", STDERR_LOG_FILE)],
    }
}

/// Read the captured streams for one allocation from `log_dir`.
///
/// - `offset` > 0 reads forward from that byte position to EOF (incremental
///   tail / follow). If `offset` is past the current end the file is treated as
///   rotated and the read restarts from the beginning.
/// - `offset` <= 0 reads a one-shot window: the leading bytes, or — when `tail`
///   is set — the trailing bytes.
///
/// `max_bytes` caps each stream's returned content (non-positive falls back to
/// [`DEF_TASK_LOG_MAX_BYTES`]). Streams whose file does not exist are skipped.
pub fn read_allocation_logs(
    log_dir: &Path,
    stream_filter: &str,
    offset: i64,
    max_bytes: i64,
    tail: bool,
) -> R<Vec<CapturedStream>> {
    // Clamp the per-stream cap to a hard ceiling so an arbitrarily large
    // `max_bytes` cannot drive a huge buffer allocation. The actual read is
    // additionally bounded by the file size in `read_stream`.
    let cap = if max_bytes > 0 {
        (max_bytes as u64).min(MAX_TASK_LOG_READ_BYTES)
    } else {
        DEF_TASK_LOG_MAX_BYTES as u64
    };

    let mut out = Vec::new();
    for (name, file_name) in streams_for_filter(stream_filter) {
        let path = log_dir.join(file_name);
        if !path.exists() {
            continue;
        }
        out.push(read_stream(name, &path, offset, cap, tail)?);
    }
    Ok(out)
}

fn read_stream(name: &str, path: &Path, offset: i64, cap: u64, tail: bool) -> R<CapturedStream> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let size = file
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?
        .len();

    // Decide the read window [start, start + to_read).
    let (start, to_read, truncated) = if offset > 0 {
        // Incremental/follow read forward from `offset`.
        let off = offset as u64;
        if off > size {
            // File was rotated/truncated underneath us; restart from the front.
            let to_read = size.min(cap);
            (0u64, to_read, size > cap)
        } else {
            let remaining = size - off;
            let to_read = remaining.min(cap);
            (off, to_read, remaining > cap)
        }
    } else if tail {
        let to_read = size.min(cap);
        (size - to_read, to_read, size > cap)
    } else {
        let to_read = size.min(cap);
        (0u64, to_read, size > cap)
    };

    if start > 0 {
        file.seek(SeekFrom::Start(start))
            .with_context(|| format!("failed to seek {}", path.display()))?;
    }

    let mut buf = vec![0u8; to_read as usize];
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = file
            .read(&mut buf[filled..])
            .with_context(|| format!("failed to read {}", path.display()))?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);

    Ok(CapturedStream {
        stream: name.to_string(),
        content: String::from_utf8_lossy(&buf).into_owned(),
        size_bytes: size as i64,
        start_offset: start as i64,
        next_offset: (start + filled as u64) as i64,
        truncated,
    })
}

/// Run rotation + retention for one allocation's captured streams.
///
/// Safe to call repeatedly (it is a no-op when nothing exceeds the size cap and
/// no rotated files are stale). Errors on individual streams are surfaced so the
/// caller can log them, but the function attempts every stream before returning.
pub fn maintain_logs(log_dir: &Path, cfg: &LogConfig) -> R<()> {
    let mut first_err: Option<anyhow::Error> = None;
    for file_name in [STDOUT_LOG_FILE, STDERR_LOG_FILE] {
        if let Err(err) = maintain_stream(log_dir, file_name, cfg) {
            first_err.get_or_insert(err);
        }
    }
    match first_err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn maintain_stream(log_dir: &Path, file_name: &str, cfg: &LogConfig) -> R<()> {
    let active = log_dir.join(file_name);
    if active.exists() {
        let size = std::fs::metadata(&active)?.len();
        if size > cfg.max_file_bytes {
            rotate_stream(log_dir, file_name, cfg)?;
        }
    }
    prune_stream(log_dir, file_name, cfg)?;
    Ok(())
}

/// Copy-truncate rotation: `stdout.log.{N-1}` -> `.N`, …, `.1` -> `.2`,
/// active -> `.1`, then truncate the active file to zero. See module docs for
/// why truncating an `O_APPEND` file the task still holds open is safe.
fn rotate_stream(log_dir: &Path, file_name: &str, cfg: &LogConfig) -> R<()> {
    let max_files = cfg.max_files.max(1);
    let active = log_dir.join(file_name);

    // Drop the oldest generation, then shift the rest up by one.
    let oldest = log_dir.join(format!("{file_name}.{max_files}"));
    if oldest.exists() {
        let _ = std::fs::remove_file(&oldest);
    }
    for index in (1..max_files).rev() {
        let from = log_dir.join(format!("{file_name}.{index}"));
        if from.exists() {
            let to = log_dir.join(format!("{file_name}.{}", index + 1));
            let _ = std::fs::rename(&from, &to);
        }
    }

    // Copy the active file aside, then truncate it in place.
    let rotated = log_dir.join(format!("{file_name}.1"));
    std::fs::copy(&active, &rotated)
        .with_context(|| format!("failed to copy {} for rotation", active.display()))?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(&active)
        .with_context(|| format!("failed to open {} for truncation", active.display()))?
        .set_len(0)
        .with_context(|| format!("failed to truncate {}", active.display()))?;
    Ok(())
}

/// Remove rotated generations beyond `max_files` or older than `retention_days`.
///
/// A log directory that does not exist has nothing to prune. An allocation is
/// registered before its runtime opens its log files, and one that fails
/// before then (an image pull, a staging error) never gets a directory at
/// all, yet stays registered until the manager removes it — so the periodic
/// sweep must not treat the missing directory as an error.
fn prune_stream(log_dir: &Path, file_name: &str, cfg: &LogConfig) -> R<()> {
    let max_files = cfg.max_files.max(1);
    let now = std::time::SystemTime::now();
    let retention = std::time::Duration::from_secs(cfg.retention_days.max(1) * 24 * 60 * 60);
    let prefix = format!("{file_name}.");

    let entries = match std::fs::read_dir(log_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to list {}", log_dir.display()))
        }
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(index) = name.strip_prefix(&prefix) else {
            continue;
        };
        let Ok(index) = index.parse::<u32>() else {
            continue;
        };

        let path = entry.path();
        let over_count = index > max_files;
        let too_old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .map(|modified| now.duration_since(modified).unwrap_or_default() > retention)
            .unwrap_or(false);
        if over_count || too_old {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

/// File in each allocation's log directory naming the allocation that owns
/// it. Written before the task starts, so the worker can tell its own earlier
/// output apart from a directory left behind by a different allocation.
pub const LOG_OWNER_FILE: &str = "owner.json";

/// The allocation a log directory belongs to. Every field is fixed for an
/// allocation's whole life, so a relaunch of the same allocation (an in-place
/// re-dispatch, a re-dispatch after a worker restart) matches and keeps
/// appending, while anything else with the same id does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogDirOwner {
    pub allocation_id: i64,
    pub job_id: String,
    pub job_config_id: String,
    pub task_config_id: i64,
    pub task_name: String,
}

impl LogDirOwner {
    pub fn for_allocation(allocation: &crate::structs::Allocation, task_name: &str) -> Self {
        Self {
            allocation_id: allocation.id,
            job_id: allocation.job_id.clone(),
            job_config_id: allocation.job_config_id.clone(),
            task_config_id: allocation.task_config_id,
            task_name: task_name.to_string(),
        }
    }
}

/// Make `log_dir` safe for a fresh launch of `owner`, and return it.
///
/// Allocation ids are never reused within a cluster, but a worker can outlive
/// its cluster's state: rebuild the managers (or wipe their data dir) and ids
/// start from 1 again while the worker still holds `logs/<id>/` from before.
/// A new task must never append to, or be served with, that old output. So
/// when the directory exists and its [`LOG_OWNER_FILE`] names a different
/// allocation — or it has content and no owner file at all — it is renamed
/// aside to `<id>.replaced-<unix seconds>` (pruned later like any inactive
/// directory) and the launch gets an empty one. A matching owner keeps the
/// directory: that is the same allocation starting again.
///
/// Never fails the launch: a directory that cannot be inspected or moved is
/// logged and used as it is, which is no worse than before this check.
pub fn claim_allocation_log_dir(
    log_dir: std::path::PathBuf,
    owner: &LogDirOwner,
) -> std::path::PathBuf {
    if let Err(err) = claim_log_dir_inner(&log_dir, owner) {
        warn!(
            allocation_id = owner.allocation_id,
            log_dir = %log_dir.display(),
            error = ?err,
            "Could not check the allocation's log directory for output from an earlier allocation"
        );
    }
    log_dir
}

fn claim_log_dir_inner(log_dir: &Path, owner: &LogDirOwner) -> R<()> {
    let owner_path = log_dir.join(LOG_OWNER_FILE);
    if log_dir.exists() {
        let recorded = std::fs::read(&owner_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<LogDirOwner>(&bytes).ok());
        match recorded {
            Some(recorded) if &recorded == owner => return Ok(()),
            None if dir_is_empty(log_dir)? => {}
            previous => {
                let stamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let name = log_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("allocation");
                let mut aside = log_dir.with_file_name(format!("{name}.replaced-{stamp}"));
                let mut suffix = 1;
                while aside.exists() {
                    aside = log_dir.with_file_name(format!("{name}.replaced-{stamp}-{suffix}"));
                    suffix += 1;
                }
                std::fs::rename(log_dir, &aside).with_context(|| {
                    format!(
                        "failed to move {} aside to {}",
                        log_dir.display(),
                        aside.display()
                    )
                })?;
                info!(
                    allocation_id = owner.allocation_id,
                    job_id = %owner.job_id,
                    previous_job_id = previous.as_ref().map(|p| p.job_id.as_str()).unwrap_or("-"),
                    moved_to = %aside.display(),
                    "Log directory held output from a different allocation with the same id; \
                     moved it aside"
                );
            }
        }
    }
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("failed to create log dir {}", log_dir.display()))?;
    crate::util::restrict_dir_to_owner(log_dir)?;
    std::fs::write(&owner_path, serde_json::to_vec_pretty(owner)?)
        .with_context(|| format!("failed to write {}", owner_path.display()))?;
    Ok(())
}

fn dir_is_empty(dir: &Path) -> R<bool> {
    Ok(std::fs::read_dir(dir)?.next().is_none())
}

/// Delete the log directories of allocations this worker no longer runs once
/// nothing has been written to them for `retention`.
///
/// This is what bounds `legion job logs` for removed allocations: the manager
/// remembers where a removed allocation ran for a while, and the files answer
/// for as long as they are here. `active` holds the worker-local keys of every
/// allocation still registered; their directories are never touched, however
/// old. Directories moved aside by [`claim_allocation_log_dir`] are pruned by
/// the same rule. Returns how many directories were removed.
pub fn prune_inactive_log_dirs(
    logs_base: &Path,
    active: &std::collections::HashSet<i64>,
    retention: std::time::Duration,
    now: std::time::SystemTime,
) -> R<usize> {
    let entries = match std::fs::read_dir(logs_base) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to list {}", logs_base.display()))
        }
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let key = name.split('.').next().and_then(|id| id.parse::<i64>().ok());
        let Some(key) = key else { continue };
        // A live allocation's own directory (not one moved aside from it).
        if !name.contains('.') && active.contains(&key) {
            continue;
        }
        let path = entry.path();
        let newest = newest_mtime(&path).unwrap_or(None);
        let stale = match newest {
            Some(modified) => now.duration_since(modified).unwrap_or_default() > retention,
            // Nothing inside to date it: fall back to the directory itself.
            None => entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|modified| now.duration_since(modified).unwrap_or_default() > retention)
                .unwrap_or(false),
        };
        if stale {
            match std::fs::remove_dir_all(&path) {
                Ok(()) => removed += 1,
                Err(err) => warn!(
                    log_dir = %path.display(),
                    error = ?err,
                    "Failed to remove an expired allocation log directory"
                ),
            }
        }
    }
    Ok(removed)
}

fn newest_mtime(dir: &Path) -> R<Option<std::time::SystemTime>> {
    let mut newest: Option<std::time::SystemTime> = None;
    for entry in std::fs::read_dir(dir)? {
        let modified = entry?.metadata()?.modified()?;
        newest = Some(newest.map_or(modified, |current| current.max(modified)));
    }
    Ok(newest)
}

/// How often [`maybe_prune_inactive_log_dirs`] actually scans: the retention
/// is measured in days, so a scan every few minutes is plenty, and it keeps
/// the 10-second log sweep from listing the whole logs directory each time.
const INACTIVE_PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// [`prune_inactive_log_dirs`] at most once per [`INACTIVE_PRUNE_INTERVAL`]
/// per logs directory, with the retention from `cfg`. Called from the
/// worker's periodic log sweep; failures are logged, never returned.
pub fn maybe_prune_inactive_log_dirs(
    logs_base: &Path,
    active: &std::collections::HashSet<i64>,
    cfg: &LogConfig,
) {
    static LAST_RUN: std::sync::Mutex<Option<HashMap<std::path::PathBuf, std::time::Instant>>> =
        std::sync::Mutex::new(None);
    {
        let mut last = LAST_RUN.lock().unwrap_or_else(|p| p.into_inner());
        let map = last.get_or_insert_with(HashMap::new);
        if let Some(at) = map.get(logs_base) {
            if at.elapsed() < INACTIVE_PRUNE_INTERVAL {
                return;
            }
        }
        map.insert(logs_base.to_path_buf(), std::time::Instant::now());
    }
    let retention = std::time::Duration::from_secs(cfg.retention_days.max(1) * 24 * 60 * 60);
    match prune_inactive_log_dirs(logs_base, active, retention, std::time::SystemTime::now()) {
        Ok(0) => {}
        Ok(removed) => info!(
            removed,
            retention_days = cfg.retention_days.max(1),
            "Removed expired log directories of allocations this worker no longer runs"
        ),
        Err(err) => warn!(error = ?err, "Failed to prune expired allocation log directories"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(dir: &Path, name: &str, contents: &str) {
        std::fs::write(dir.join(name), contents).unwrap();
    }

    #[test]
    fn reads_both_streams_by_default() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "out");
        write(dir.path(), STDERR_LOG_FILE, "err");

        let streams = read_allocation_logs(dir.path(), "", 0, 0, false)?;
        assert_eq!(streams.len(), 2);
        let stdout = streams.iter().find(|s| s.stream == "stdout").unwrap();
        assert_eq!(stdout.content, "out");
        assert!(!stdout.truncated);
        assert_eq!(stdout.next_offset, 3);
        Ok(())
    }

    #[test]
    fn filters_to_single_stream() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "out");
        write(dir.path(), STDERR_LOG_FILE, "err");

        let streams = read_allocation_logs(dir.path(), "stderr", 0, 0, false)?;
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream, "stderr");
        assert_eq!(streams[0].content, "err");
        Ok(())
    }

    #[test]
    fn missing_files_are_skipped() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "only stdout");

        let streams = read_allocation_logs(dir.path(), "all", 0, 0, false)?;
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream, "stdout");
        Ok(())
    }

    #[test]
    fn truncates_head_and_tail() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "0123456789");

        let head = read_allocation_logs(dir.path(), "stdout", 0, 4, false)?;
        assert_eq!(head[0].content, "0123");
        assert!(head[0].truncated);
        assert_eq!(head[0].size_bytes, 10);

        let tail = read_allocation_logs(dir.path(), "stdout", 0, 4, true)?;
        assert_eq!(tail[0].content, "6789");
        assert!(tail[0].truncated);
        assert_eq!(tail[0].start_offset, 6);
        Ok(())
    }

    #[test]
    fn incremental_offset_reads_only_new_bytes() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "hello");

        // Caught up: offset == size yields nothing new.
        let none = read_allocation_logs(dir.path(), "stdout", 5, 0, false)?;
        assert_eq!(none[0].content, "");
        assert_eq!(none[0].next_offset, 5);

        // Append, then read forward from the prior offset.
        write(dir.path(), STDOUT_LOG_FILE, "hello world");
        let more = read_allocation_logs(dir.path(), "stdout", 5, 0, false)?;
        assert_eq!(more[0].content, " world");
        assert_eq!(more[0].start_offset, 5);
        assert_eq!(more[0].next_offset, 11);
        Ok(())
    }

    #[test]
    fn offset_past_eof_restarts_from_front() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "abc");
        // Pretend the follower had read to offset 50 before a rotation shrank
        // the file; the read should reset to the front.
        let reset = read_allocation_logs(dir.path(), "stdout", 50, 0, false)?;
        assert_eq!(reset[0].content, "abc");
        assert_eq!(reset[0].start_offset, 0);
        Ok(())
    }

    #[test]
    fn rotation_caps_active_file_and_keeps_generations() -> R<()> {
        let dir = TempDir::new()?;
        let cfg = LogConfig {
            max_file_bytes: 4,
            max_files: 2,
            retention_days: 7,
        };
        write(dir.path(), STDOUT_LOG_FILE, "AAAAAAAA"); // 8 bytes > cap

        maintain_logs(dir.path(), &cfg)?;
        // Active truncated, generation .1 holds the rotated content.
        assert_eq!(
            std::fs::read_to_string(dir.path().join(STDOUT_LOG_FILE))?,
            ""
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("stdout.log.1"))?,
            "AAAAAAAA"
        );

        // Second rotation shifts .1 -> .2 and creates a fresh .1.
        write(dir.path(), STDOUT_LOG_FILE, "BBBBBBBB");
        maintain_logs(dir.path(), &cfg)?;
        assert_eq!(
            std::fs::read_to_string(dir.path().join("stdout.log.1"))?,
            "BBBBBBBB"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("stdout.log.2"))?,
            "AAAAAAAA"
        );

        // Third rotation drops the oldest generation (max_files = 2).
        write(dir.path(), STDOUT_LOG_FILE, "CCCCCCCC");
        maintain_logs(dir.path(), &cfg)?;
        assert!(!dir.path().join("stdout.log.3").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("stdout.log.1"))?,
            "CCCCCCCC"
        );
        Ok(())
    }

    #[test]
    fn maintain_is_noop_below_cap() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "small");
        maintain_logs(dir.path(), &LogConfig::default())?;
        assert_eq!(
            std::fs::read_to_string(dir.path().join(STDOUT_LOG_FILE))?,
            "small"
        );
        assert!(!dir.path().join("stdout.log.1").exists());
        Ok(())
    }

    #[test]
    fn maintenance_of_an_allocation_without_a_log_dir_is_a_noop() -> R<()> {
        // An allocation that failed before its runtime opened its log files
        // stays registered with no directory; the 10s sweep must not report
        // `No such file or directory` for it forever.
        let dir = TempDir::new()?;
        let missing = dir.path().join("never-created");
        maintain_logs(&missing, &LogConfig::default())?;
        assert!(!missing.exists(), "maintenance must not create the dir");
        Ok(())
    }

    fn owner(allocation_id: i64, job: &str) -> LogDirOwner {
        LogDirOwner {
            allocation_id,
            job_id: job.to_string(),
            job_config_id: format!("{job}-1"),
            task_config_id: 7,
            task_name: "web".to_string(),
        }
    }

    fn aside_dirs(base: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(base)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".replaced-"))
            .collect();
        names.sort();
        names
    }

    #[test]
    fn a_reused_allocation_id_never_appends_to_the_old_output() -> R<()> {
        let base = TempDir::new()?;
        let log_dir = base.path().join("3");

        // The first allocation 3 (say, a DAG step) runs and writes output.
        let first = claim_allocation_log_dir(log_dir.clone(), &owner(3, "dag"));
        write(&first, STDOUT_LOG_FILE, "fetch: downloading\n");

        // A rebuilt cluster hands id 3 to a different job.
        let second = claim_allocation_log_dir(log_dir.clone(), &owner(3, "sandbox"));
        assert_eq!(second, log_dir);
        assert!(
            !second.join(STDOUT_LOG_FILE).exists(),
            "the new allocation starts with no output"
        );
        let recorded: LogDirOwner =
            serde_json::from_slice(&std::fs::read(second.join(LOG_OWNER_FILE))?)?;
        assert_eq!(recorded, owner(3, "sandbox"));

        // The old output was moved aside, not deleted.
        let aside = aside_dirs(base.path());
        assert_eq!(aside.len(), 1, "{aside:?}");
        assert!(aside[0].starts_with("3.replaced-"));
        assert_eq!(
            std::fs::read_to_string(base.path().join(&aside[0]).join(STDOUT_LOG_FILE))?,
            "fetch: downloading\n"
        );
        Ok(())
    }

    #[test]
    fn the_same_allocation_starting_again_keeps_its_output() -> R<()> {
        let base = TempDir::new()?;
        let log_dir = base.path().join("9");
        claim_allocation_log_dir(log_dir.clone(), &owner(9, "web"));
        write(&log_dir, STDOUT_LOG_FILE, "first run\n");

        claim_allocation_log_dir(log_dir.clone(), &owner(9, "web"));
        assert_eq!(
            std::fs::read_to_string(log_dir.join(STDOUT_LOG_FILE))?,
            "first run\n"
        );
        assert!(aside_dirs(base.path()).is_empty());
        Ok(())
    }

    #[test]
    fn output_with_no_owner_record_is_moved_aside_but_an_empty_dir_is_kept() -> R<()> {
        let base = TempDir::new()?;
        // Output written before owner records existed (or by anything else).
        let legacy = base.path().join("4");
        std::fs::create_dir_all(&legacy)?;
        write(&legacy, STDERR_LOG_FILE, "old\n");
        claim_allocation_log_dir(legacy.clone(), &owner(4, "web"));
        assert!(!legacy.join(STDERR_LOG_FILE).exists());
        assert_eq!(aside_dirs(base.path()).len(), 1);

        // An empty directory has nothing to mix in.
        let empty = base.path().join("5");
        std::fs::create_dir_all(&empty)?;
        claim_allocation_log_dir(empty.clone(), &owner(5, "web"));
        assert!(empty.join(LOG_OWNER_FILE).exists());
        assert_eq!(aside_dirs(base.path()).len(), 1);
        Ok(())
    }

    #[test]
    fn inactive_log_dirs_are_kept_for_the_retention_then_pruned() -> R<()> {
        let base = TempDir::new()?;
        for id in ["1", "2", "3.replaced-100"] {
            std::fs::create_dir_all(base.path().join(id))?;
            write(&base.path().join(id), STDOUT_LOG_FILE, "x");
        }
        // Not an allocation directory: never touched.
        std::fs::create_dir_all(base.path().join("notes"))?;
        let active: std::collections::HashSet<i64> = [1].into_iter().collect();
        let retention = std::time::Duration::from_secs(3600);

        // Within the retention a removed allocation's logs stay readable.
        let now = std::time::SystemTime::now();
        assert_eq!(
            prune_inactive_log_dirs(base.path(), &active, retention, now)?,
            0
        );
        assert!(base.path().join("2").exists());

        // Past it, only the live allocation's directory survives.
        let later = now + std::time::Duration::from_secs(7200);
        assert_eq!(
            prune_inactive_log_dirs(base.path(), &active, retention, later)?,
            2
        );
        assert!(
            base.path().join("1").exists(),
            "live allocations are never pruned"
        );
        assert!(!base.path().join("2").exists());
        assert!(!base.path().join("3.replaced-100").exists());
        assert!(base.path().join("notes").exists());

        // A missing logs directory is not an error.
        assert_eq!(
            prune_inactive_log_dirs(&base.path().join("absent"), &active, retention, later)?,
            0
        );
        Ok(())
    }
}
