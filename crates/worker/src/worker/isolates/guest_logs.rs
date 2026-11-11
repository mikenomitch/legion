//! Guest output in the task's log stream (plan §6.9): the lines a host
//! forwards over `IsolateWorker.Log` are appended to the ENVELOPE
//! allocation's `stdout.log` / `stderr.log`, where `legion job logs` (the
//! `GetAllocationLogs` → `Worker.GetTaskLogs` path) reads them.
//!
//! The task log format is plain lines with no fields, so each line carries
//! a compact prefix naming the isolate (and its tenant, when it has one):
//!
//! ```text
//! [isolate run-3f2a.1] hello from a session isolate
//! [isolate 7c1e… tenant=acme] handled GET /
//! ```
//!
//! The host decides what is forwarded: handler (HTTP) and session
//! isolates' output, once per invocation; a request isolate's run output
//! goes in its reply and is never forwarded. What arrives here is bounded
//! the way other task logs are: the active file rotates once it passes
//! `LEGION_LOG_MAX_FILE_BYTES` (checked after every batch, not only by the
//! worker's periodic sweep, so a burst of 10^5 isolates cannot outrun the
//! sweep), keeping `LEGION_LOG_MAX_FILES` generations; one line is cut at
//! [`MAX_LINE_BYTES`].

use crate::prelude::*;
use crate::structs::isolate_host::LogLine;
use crate::task_plugins::{STDERR_LOG_FILE, STDOUT_LOG_FILE};
use crate::worker::logs::LogConfig;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The longest single guest line kept; the rest is replaced by a marker.
pub const MAX_LINE_BYTES: usize = 16 << 10;

/// Appends guest lines to envelope allocations' task logs.
#[derive(Debug, Clone)]
pub struct GuestLogs {
    /// `<state dir>/logs`: one directory per allocation (local key).
    root: PathBuf,
    config: LogConfig,
}

/// Who wrote a batch: the prefix every line gets.
pub fn line_prefix(isolate_id: &str, tenant: &str) -> String {
    if tenant.is_empty() {
        format!("[isolate {isolate_id}] ")
    } else {
        format!("[isolate {isolate_id} tenant={tenant}] ")
    }
}

/// Render one batch into the bytes for stdout and for stderr.
pub fn render(prefix: &str, lines: &[LogLine]) -> (Vec<u8>, Vec<u8>) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    for line in lines {
        let out = if line.stderr { &mut stderr } else { &mut stdout };
        let mut text: &[u8] = &line.line;
        while let Some(rest) = text.strip_suffix(b"\n") {
            text = rest;
        }
        if let Some(rest) = text.strip_suffix(b"\r") {
            text = rest;
        }
        // A guest's one "line" may itself hold newlines: each gets the
        // prefix, so a reader can always tell whose a line is.
        for part in text.split(|b| *b == b'\n') {
            out.extend_from_slice(prefix.as_bytes());
            if part.len() > MAX_LINE_BYTES {
                out.extend_from_slice(&part[..MAX_LINE_BYTES]);
                out.extend_from_slice(
                    format!(" [line cut at {MAX_LINE_BYTES} bytes]").as_bytes(),
                );
            } else {
                out.extend_from_slice(part);
            }
            out.push(b'\n');
        }
    }
    (stdout, stderr)
}

impl GuestLogs {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            config: LogConfig::from_env(),
        }
    }

    pub fn with_config(root: PathBuf, config: LogConfig) -> Self {
        Self { root, config }
    }

    /// The envelope allocation's log directory.
    pub fn dir(&self, envelope_allocation_id: i64) -> PathBuf {
        self.root.join(envelope_allocation_id.to_string())
    }

    /// Append one batch (blocking file I/O: call from a blocking thread).
    pub fn append(
        &self,
        envelope_allocation_id: i64,
        prefix: &str,
        lines: &[LogLine],
    ) -> R<()> {
        if lines.is_empty() {
            return Ok(());
        }
        let dir = self.dir(envelope_allocation_id);
        let (stdout, stderr) = render(prefix, lines);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create {}", dir.display()))?;
        let mut rotate = false;
        for (bytes, file) in [(stdout, STDOUT_LOG_FILE), (stderr, STDERR_LOG_FILE)] {
            if bytes.is_empty() {
                continue;
            }
            rotate |= append_to(&dir.join(file), &bytes)? > self.config.max_file_bytes;
        }
        if rotate {
            crate::worker::logs::maintain_logs(&dir, &self.config)?;
        }
        Ok(())
    }
}

/// `O_APPEND` write, as task processes write their own logs (so the
/// copy-truncate rotation stays safe); returns the file's size after.
fn append_to(path: &Path, bytes: &[u8]) -> R<u64> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to append to {}", path.display()))?;
    Ok(file.metadata().map(|m| m.len()).unwrap_or(0))
}

#[cfg(test)]
mod guest_logs_test {
    use super::*;

    fn line(text: &str, stderr: bool) -> LogLine {
        LogLine {
            stderr,
            line: text.as_bytes().to_vec(),
            unix_ms: 1,
        }
    }

    #[test]
    fn lines_land_in_the_envelopes_streams_with_their_isolate_test() {
        let dir = tempfile::tempdir().unwrap();
        let logs = GuestLogs::new(dir.path().to_path_buf());
        logs.append(
            42,
            &line_prefix("run-a.1", ""),
            &[line("hello\n", false), line("oops", true)],
        )
        .unwrap();
        logs.append(
            42,
            &line_prefix("inv-9", "acme"),
            &[line("two\nlines", false)],
        )
        .unwrap();
        let read = crate::worker::logs::read_allocation_logs(&logs.dir(42), "", 0, 0, false)
            .unwrap();
        assert_eq!(read[0].stream, "stdout");
        assert_eq!(
            read[0].content,
            "[isolate run-a.1] hello\n[isolate inv-9 tenant=acme] two\n[isolate inv-9 \
             tenant=acme] lines\n"
        );
        assert_eq!(read[1].stream, "stderr");
        assert_eq!(read[1].content, "[isolate run-a.1] oops\n");
    }

    #[test]
    fn a_flood_rotates_and_stays_bounded_test() {
        let dir = tempfile::tempdir().unwrap();
        let config = LogConfig {
            max_file_bytes: 4096,
            max_files: 2,
            retention_days: 1,
        };
        let logs = GuestLogs::with_config(dir.path().to_path_buf(), config);
        let long = "x".repeat(MAX_LINE_BYTES * 2);
        for i in 0..1000 {
            logs.append(7, &line_prefix(&format!("inv-{i}"), ""), &[line(&long, false)])
                .unwrap();
        }
        let total: u64 = std::fs::read_dir(logs.dir(7))
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum();
        // The active file plus two generations, each at most one batch over
        // the cap.
        assert!(total < 3 * (4096 + MAX_LINE_BYTES as u64 + 128), "{total}");
        let read = crate::worker::logs::read_allocation_logs(&logs.dir(7), "stdout", 0, 0, true)
            .unwrap();
        assert!(read[0].content.contains("[line cut at 16384 bytes]"));
    }
}
