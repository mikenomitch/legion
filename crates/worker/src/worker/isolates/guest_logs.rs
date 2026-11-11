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

/// The longest single guest line kept, after escaping; the rest is
/// replaced by a marker.
pub const MAX_LINE_BYTES: usize = 16 << 10;
/// The most lines one batch keeps, counted after embedded newlines are
/// split (one guest "line" holding a million `\n` is a million lines).
pub const MAX_LINES_PER_BATCH: usize = 4096;
/// The most escaped text one batch keeps; with the prefix (at most a few
/// hundred bytes, from a checked isolate id and tenant) per line, a batch
/// renders to at most `MAX_BATCH_BYTES + MAX_LINES_PER_BATCH * 420` bytes.
pub const MAX_BATCH_BYTES: usize = 1 << 20;

/// Appends guest lines to envelope allocations' task logs.
#[derive(Debug, Clone)]
pub struct GuestLogs {
    /// `<state dir>/logs`: one directory per allocation (local key).
    root: PathBuf,
    config: LogConfig,
}

/// One guest line ready to write: split on newlines, escaped, cut; without
/// its prefix and newline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Line {
    pub stderr: bool,
    pub text: Vec<u8>,
}

/// Who wrote a batch: the prefix every line gets. The worker's own record
/// of the isolate id and tenant, both checked (`service::check_origin`).
pub fn line_prefix(isolate_id: &str, tenant: &str) -> String {
    if tenant.is_empty() {
        format!("[isolate {isolate_id}] ")
    } else {
        format!("[isolate {isolate_id} tenant={tenant}] ")
    }
}

/// The bytes `line` takes in the log file with `prefix`: what the rate
/// budget is charged.
pub fn rendered_len(prefix: &str, line: &Line) -> usize {
    prefix.len() + line.text.len() + 1
}

/// Escape one line's body so it cannot pass for another line or drive a
/// terminal (security review N5): every C0 control character but tab
/// (`\r` included, so a guest cannot return to the start of the line and
/// print another isolate's prefix over its own), DEL, and the UTF-8 C1
/// controls (U+0080..U+009F, among them CSI) are written as visible `\xHH`
/// escapes; everything else is kept as is. The result is cut at
/// [`MAX_LINE_BYTES`] with a marker, without reading past what it keeps.
pub fn escape_line(raw: &[u8]) -> Vec<u8> {
    const CUT: &str = " [line cut at 16384 bytes]";
    const _: () = assert!(MAX_LINE_BYTES == 16384, "the marker names the cut");
    let mut out = Vec::with_capacity(raw.len().min(MAX_LINE_BYTES + CUT.len()));
    let mut i = 0;
    while i < raw.len() {
        let b = raw[i];
        let push = |token: &[u8], out: &mut Vec<u8>| -> bool {
            if out.len() + token.len() > MAX_LINE_BYTES {
                return false;
            }
            out.extend_from_slice(token);
            true
        };
        let fits = if b == b'\t' || (b >= 0x20 && b != 0x7f && b != 0xc2) {
            push(&[b], &mut out)
        } else if b == 0xc2 && raw.get(i + 1).is_some_and(|n| (0x80..=0x9f).contains(n)) {
            let token = format!("\\x{b:02x}\\x{:02x}", raw[i + 1]);
            i += 1;
            push(token.as_bytes(), &mut out)
        } else if b == 0xc2 {
            push(&[b], &mut out)
        } else {
            push(format!("\\x{b:02x}").as_bytes(), &mut out)
        };
        if !fits {
            out.extend_from_slice(CUT.as_bytes());
            break;
        }
        i += 1;
    }
    out
}

/// Split a batch into lines: each guest line on its embedded newlines (a
/// trailing newline or CRLF ends a line, it does not start an empty one),
/// each part escaped and cut ([`escape_line`]). Keeps at most `max_lines`
/// lines and `max_bytes` of escaped text, in order; returns them and how
/// many lines were dropped. Never allocates more than it keeps.
pub fn split_lines(batch: &[LogLine], max_lines: usize, max_bytes: usize) -> (Vec<Line>, u64) {
    let mut lines = Vec::new();
    let mut bytes = 0usize;
    let mut dropped = 0u64;
    for line in batch {
        let mut text: &[u8] = &line.line;
        while let Some(rest) = text.strip_suffix(b"\n") {
            text = rest;
        }
        for part in text.split(|b| *b == b'\n') {
            if lines.len() >= max_lines || bytes >= max_bytes {
                dropped += 1;
                continue;
            }
            let part = part.strip_suffix(b"\r").unwrap_or(part);
            let escaped = escape_line(part);
            bytes += escaped.len();
            lines.push(Line {
                stderr: line.stderr,
                text: escaped,
            });
        }
    }
    (lines, dropped)
}

/// Render lines into the bytes for stdout and for stderr: each line gets
/// the prefix, so a reader can always tell whose a line is.
pub fn render(prefix: &str, lines: &[Line]) -> (Vec<u8>, Vec<u8>) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    for line in lines {
        let out = if line.stderr {
            &mut stderr
        } else {
            &mut stdout
        };
        out.extend_from_slice(prefix.as_bytes());
        out.extend_from_slice(&line.text);
        out.push(b'\n');
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

    /// Append one batch of raw guest lines, split and escaped with the
    /// batch limits (blocking file I/O: call from a blocking thread).
    pub fn append(&self, envelope_allocation_id: i64, prefix: &str, lines: &[LogLine]) -> R<()> {
        let (lines, _) = split_lines(lines, MAX_LINES_PER_BATCH, MAX_BATCH_BYTES);
        self.append_lines(envelope_allocation_id, prefix, &lines)
    }

    /// Append lines already split and escaped ([`split_lines`]). Blocking.
    pub fn append_lines(&self, envelope_allocation_id: i64, prefix: &str, lines: &[Line]) -> R<()> {
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
        let read =
            crate::worker::logs::read_allocation_logs(&logs.dir(42), "", 0, 0, false).unwrap();
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
    fn a_forged_prefix_or_terminal_escape_is_rendered_inert_test() {
        let (lines, dropped) = split_lines(
            &[line(
                "x\r\x1b[2K[isolate other-id tenant=bob] payment ok\u{9b}31m\x07\x7f\tend\r\n",
                false,
            )],
            MAX_LINES_PER_BATCH,
            MAX_BATCH_BYTES,
        );
        assert_eq!(dropped, 0);
        let (stdout, _) = render(&line_prefix("inv-1", "acme"), &lines);
        assert_eq!(
            String::from_utf8(stdout).unwrap(),
            "[isolate inv-1 tenant=acme] x\\x0d\\x1b[2K[isolate other-id tenant=bob] payment \
             ok\\xc2\\x9b31m\\x07\\x7f\tend\n"
        );
        // A newline inside starts a line of its own, with the real prefix.
        let (lines, _) = split_lines(
            &[line("a\n[isolate other-id] forged", true)],
            MAX_LINES_PER_BATCH,
            MAX_BATCH_BYTES,
        );
        let (_, stderr) = render(&line_prefix("inv-1", ""), &lines);
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "[isolate inv-1] a\n[isolate inv-1] [isolate other-id] forged\n"
        );
    }

    #[test]
    fn a_newline_bomb_is_cut_before_anything_is_built_test() {
        // One "line" of 8 MiB of newlines: 8 M parts.
        let bomb = LogLine {
            stderr: false,
            line: vec![b'\n'; 8 << 20].into_iter().chain(*b"a").collect(),
            unix_ms: 0,
        };
        let many = LogLine {
            stderr: false,
            line: b"ab\n".repeat(1 << 20),
            unix_ms: 0,
        };
        let (lines, dropped) = split_lines(&[bomb, many], MAX_LINES_PER_BATCH, MAX_BATCH_BYTES);
        assert_eq!(lines.len(), MAX_LINES_PER_BATCH);
        assert_eq!(
            dropped as usize,
            (8 << 20) + 1 + (1 << 20) - MAX_LINES_PER_BATCH
        );
        // Long lines: the batch keeps at most MAX_BATCH_BYTES of text.
        let long = LogLine {
            stderr: false,
            line: [vec![b'x'; 100 << 10], b"\n".to_vec()].concat().repeat(200),
            unix_ms: 0,
        };
        let (lines, dropped) = split_lines(&[long], MAX_LINES_PER_BATCH, MAX_BATCH_BYTES);
        let text: usize = lines.iter().map(|l| l.text.len()).sum();
        assert!(text < MAX_BATCH_BYTES + MAX_LINE_BYTES + 64, "{text}");
        assert_eq!(lines.len() as u64 + dropped, 200);
        // An escaped line stays within its cap: escaping expands 4x.
        let controls = escape_line(&vec![0x01; 1 << 20]);
        assert!(controls.len() <= MAX_LINE_BYTES + 32, "{}", controls.len());
        assert!(controls.ends_with(b" [line cut at 16384 bytes]"));
        // So the rendered batch is bounded by constants, whatever the host
        // sent.
        let prefix = line_prefix(&"i".repeat(256), &"t".repeat(128));
        let (lines, _) = split_lines(
            &[LogLine {
                stderr: false,
                line: b"\x01\n".repeat(1 << 20),
                unix_ms: 0,
            }],
            MAX_LINES_PER_BATCH,
            MAX_BATCH_BYTES,
        );
        let (stdout, _) = render(&prefix, &lines);
        assert!(
            stdout.len() <= MAX_BATCH_BYTES + MAX_LINES_PER_BATCH * (prefix.len() + 1),
            "{}",
            stdout.len()
        );
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
            logs.append(
                7,
                &line_prefix(&format!("inv-{i}"), ""),
                &[line(&long, false)],
            )
            .unwrap();
        }
        let total: u64 = std::fs::read_dir(logs.dir(7))
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum();
        // The active file plus two generations, each at most one batch over
        // the cap.
        assert!(total < 3 * (4096 + MAX_LINE_BYTES as u64 + 128), "{total}");
        // Every batch passed the cap, so each was rotated out at once.
        let rotated = std::fs::read_to_string(logs.dir(7).join("stdout.log.1")).unwrap();
        assert!(
            rotated.starts_with("[isolate inv-999] xxx"),
            "{}",
            &rotated[..40]
        );
        assert!(rotated.ends_with(" [line cut at 16384 bytes]\n"));
    }
}
