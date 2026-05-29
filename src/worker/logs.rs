//! Reading captured task stdout/stderr from per-allocation log files.
//!
//! Workers redirect each task's stdout/stderr into `stdout.log` / `stderr.log`
//! under a per-allocation directory (see [`crate::task_plugins`]). This module
//! reads those files back for the `Worker.GetTaskLogs` RPC, applying a byte cap
//! and an optional tail so a single allocation cannot flood an agent's context.

use crate::prelude::*;
use crate::task_plugins::{STDERR_LOG_FILE, STDOUT_LOG_FILE};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// One stream's captured content, ready to place into a `TaskLogStream`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedStream {
    pub stream: String,
    pub content: String,
    pub size_bytes: i64,
    pub truncated: bool,
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
/// `max_bytes` caps each stream's returned content (a non-positive value falls
/// back to [`DEF_TASK_LOG_MAX_BYTES`]). When `tail` is true the trailing bytes
/// are returned, otherwise the leading bytes. Streams whose file does not exist
/// are silently skipped (a not-yet-started or never-logging task yields none).
pub fn read_allocation_logs(
    log_dir: &Path,
    stream_filter: &str,
    max_bytes: i64,
    tail: bool,
) -> R<Vec<CapturedStream>> {
    let cap = if max_bytes > 0 {
        max_bytes as u64
    } else {
        DEF_TASK_LOG_MAX_BYTES as u64
    };

    let mut out = Vec::new();
    for (name, file_name) in streams_for_filter(stream_filter) {
        let path = log_dir.join(file_name);
        if !path.exists() {
            continue;
        }
        out.push(read_stream(name, &path, cap, tail)?);
    }
    Ok(out)
}

fn read_stream(name: &str, path: &Path, cap: u64, tail: bool) -> R<CapturedStream> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let size = file
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?
        .len();

    let truncated = size > cap;
    let to_read = size.min(cap);

    if truncated && tail {
        file.seek(SeekFrom::Start(size - to_read))
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
        truncated,
    })
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

        let streams = read_allocation_logs(dir.path(), "", 0, false)?;
        assert_eq!(streams.len(), 2);
        let stdout = streams.iter().find(|s| s.stream == "stdout").unwrap();
        assert_eq!(stdout.content, "out");
        assert!(!stdout.truncated);
        Ok(())
    }

    #[test]
    fn filters_to_single_stream() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "out");
        write(dir.path(), STDERR_LOG_FILE, "err");

        let streams = read_allocation_logs(dir.path(), "stderr", 0, false)?;
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream, "stderr");
        assert_eq!(streams[0].content, "err");
        Ok(())
    }

    #[test]
    fn missing_files_are_skipped() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "only stdout");

        let streams = read_allocation_logs(dir.path(), "all", 0, false)?;
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].stream, "stdout");
        Ok(())
    }

    #[test]
    fn truncates_head_and_tail() -> R<()> {
        let dir = TempDir::new()?;
        write(dir.path(), STDOUT_LOG_FILE, "0123456789");

        let head = read_allocation_logs(dir.path(), "stdout", 4, false)?;
        assert_eq!(head[0].content, "0123");
        assert!(head[0].truncated);
        assert_eq!(head[0].size_bytes, 10);

        let tail = read_allocation_logs(dir.path(), "stdout", 4, true)?;
        assert_eq!(tail[0].content, "6789");
        assert!(tail[0].truncated);
        Ok(())
    }
}
