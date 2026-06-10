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
fn prune_stream(log_dir: &Path, file_name: &str, cfg: &LogConfig) -> R<()> {
    let max_files = cfg.max_files.max(1);
    let now = std::time::SystemTime::now();
    let retention = std::time::Duration::from_secs(cfg.retention_days.max(1) * 24 * 60 * 60);
    let prefix = format!("{file_name}.");

    for entry in std::fs::read_dir(log_dir)? {
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
}
