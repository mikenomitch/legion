//! Live exec sessions: a process spawned inside (or alongside) a running
//! allocation that the worker pumps stdin/stdout/stderr/resize frames through.
//!
//! Two concrete backends, both object-safe behind [`ExecSession`]:
//!
//! - [`PtyExecSession`] (interactive, `tty: true`) runs the command on a
//!   pseudo-terminal via `portable-pty` (openpty on Unix, ConPTY on Windows).
//!   Output is a single combined stream; window resize is supported.
//! - [`PipeExecSession`] (non-interactive) runs the command with piped
//!   stdio via `std::process::Command`, keeping stdout and stderr separate.
//!
//! Task runtimes build a command + env and call [`spawn_pty`] / [`spawn_pipe`];
//! they do not depend on the backend types directly.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::process::Stdio;

use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};

use crate::prelude::*;

/// What to run for an exec session.
#[derive(Debug, Clone)]
pub struct ExecOptions {
    /// Argv; first element is the program.
    pub command: Vec<String>,
    /// Allocate a PTY (interactive).
    pub tty: bool,
    /// Initial terminal size (cols, rows). Ignored for pipe sessions.
    pub winsize: (u16, u16),
    /// Working directory, when the runtime wants to set one.
    pub cwd: Option<std::path::PathBuf>,
    /// Extra environment for the exec'd process.
    pub env: BTreeMap<String, String>,
    /// A cgroup v2 directory (absolute) the process joins before it runs the
    /// command, so a session into a governed host-process task runs under
    /// that task's memory/CPU limits rather than in the worker's cgroup.
    /// `None` leaves the process where the worker is.
    pub cgroup: Option<std::path::PathBuf>,
}

impl ExecOptions {
    pub fn new(command: Vec<String>, tty: bool) -> Self {
        Self {
            command,
            tty,
            winsize: (80, 24),
            cwd: None,
            env: BTreeMap::new(),
            cgroup: None,
        }
    }
}

/// The `sh -c` script a PTY session joins its cgroup with: `$0` is the
/// cgroup's `cgroup.procs`, `$@` the command. The shell writes its own pid,
/// then `exec`s the command under that same pid, so the command never runs
/// outside the cgroup. A failed join exits 126 with the shell's error on the
/// terminal rather than running the command unconfined.
///
/// A shell and not a `pre_exec` hook because `portable-pty` exposes none; the
/// pipe path, built on `std::process::Command`, uses the hook directly.
const PTY_CGROUP_JOIN: &str = "echo $$ > \"$0\" || exit 126; exec \"$@\"";

/// The output streams of a session. `stderr` is `None` for PTY sessions,
/// where the terminal merges both onto the master.
pub struct ExecOutputStreams {
    pub stdout: Box<dyn Read + Send>,
    pub stderr: Option<Box<dyn Read + Send>>,
}

/// A live exec session. The worker owns one of these and drives it: writing
/// client stdin, forwarding resize, reading output, and waiting for exit.
///
/// Methods are blocking; the worker runs them off the async runtime.
pub trait ExecSession: Send {
    /// Take the output reader(s). Called exactly once.
    fn take_output(&mut self) -> R<ExecOutputStreams>;
    /// Write bytes to the process stdin.
    fn write_stdin(&mut self, data: &[u8]) -> std::io::Result<()>;
    /// Half-close stdin (send EOF) without killing the session.
    fn close_stdin(&mut self);
    /// Resize the terminal (no-op for pipe sessions).
    fn resize(&mut self, cols: u16, rows: u16) -> std::io::Result<()>;
    /// Block until the process exits and return its exit code.
    fn wait(&mut self) -> R<i32>;
    /// Non-blocking exit check; `Ok(Some(code))` once the process has exited.
    fn try_wait(&mut self) -> R<Option<i32>>;
    /// Force-terminate the process.
    fn kill(&mut self);
}

fn apply_common(builder: &mut CommandBuilder, opts: &ExecOptions) {
    for (k, v) in &opts.env {
        builder.env(k, v);
    }
    if let Some(cwd) = &opts.cwd {
        builder.cwd(cwd);
    }
}

/// Spawn an interactive PTY-backed session.
pub fn spawn_pty(opts: &ExecOptions) -> R<Box<dyn ExecSession>> {
    let (program, args) = opts
        .command
        .split_first()
        .context("exec command must include at least one entry")?;

    let pty = NativePtySystem::default();
    let pair = pty
        .openpty(PtySize {
            rows: opts.winsize.1.max(1),
            cols: opts.winsize.0.max(1),
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("failed to open pty")?;

    let mut builder = match &opts.cgroup {
        Some(cgroup) => {
            let mut builder = CommandBuilder::new("/bin/sh");
            builder.arg("-c");
            builder.arg(PTY_CGROUP_JOIN);
            builder.arg(cgroup.join("cgroup.procs"));
            builder.arg(program);
            builder.args(args);
            builder
        }
        None => {
            let mut builder = CommandBuilder::new(program);
            builder.args(args);
            builder
        }
    };
    apply_common(&mut builder, opts);

    let child = pair
        .slave
        .spawn_command(builder)
        .context("failed to spawn pty command")?;
    // Slave handle is held by the child; drop ours so EOF propagates on exit.
    drop(pair.slave);

    let writer = pair
        .master
        .take_writer()
        .context("failed to take pty writer")?;
    let reader = pair
        .master
        .try_clone_reader()
        .context("failed to clone pty reader")?;

    Ok(Box::new(PtyExecSession {
        master: pair.master,
        child,
        writer: Some(writer),
        reader: Some(reader),
    }))
}

/// Spawn a non-interactive pipe-backed session (stdout/stderr separate).
pub fn spawn_pipe(opts: &ExecOptions) -> R<Box<dyn ExecSession>> {
    let (program, args) = opts
        .command
        .split_first()
        .context("exec command must include at least one entry")?;

    let mut cmd = std::process::Command::new(program);
    cmd.args(args);
    for (k, v) in &opts.env {
        cmd.env(k, v);
    }
    if let Some(cwd) = &opts.cwd {
        cmd.current_dir(cwd);
    }
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    #[cfg(unix)]
    if let Some(cgroup) = &opts.cgroup {
        use std::os::unix::process::CommandExt;
        let procs = cgroup.join("cgroup.procs");
        // SAFETY: the closure only writes a pid to a file, the same work
        // `join_assignment_cgroup` does in its hook. In the child, before
        // exec, so the command never runs outside the cgroup.
        unsafe {
            cmd.pre_exec(move || {
                std::fs::write(&procs, std::process::id().to_string())
                    .map_err(std::io::Error::other)
            });
        }
    }

    let mut child = cmd.spawn().with_context(|| match &opts.cgroup {
        Some(cgroup) => format!(
            "failed to spawn exec command in cgroup {}",
            cgroup.display()
        ),
        None => "failed to spawn exec command".to_string(),
    })?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdin = child.stdin.take();

    Ok(Box::new(PipeExecSession {
        child,
        stdin,
        stdout: stdout.map(|s| Box::new(s) as Box<dyn Read + Send>),
        stderr: stderr.map(|s| Box::new(s) as Box<dyn Read + Send>),
    }))
}

struct PtyExecSession {
    master: Box<dyn portable_pty::MasterPty + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    writer: Option<Box<dyn Write + Send>>,
    reader: Option<Box<dyn Read + Send>>,
}

impl ExecSession for PtyExecSession {
    fn take_output(&mut self) -> R<ExecOutputStreams> {
        let stdout = self.reader.take().context("pty reader already taken")?;
        Ok(ExecOutputStreams {
            stdout,
            stderr: None,
        })
    }

    fn write_stdin(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self.writer.as_mut() {
            Some(w) => {
                w.write_all(data)?;
                w.flush()
            }
            None => Ok(()),
        }
    }

    fn close_stdin(&mut self) {
        // Dropping the writer closes the master write side -> EOF to the pty.
        self.writer = None;
    }

    fn resize(&mut self, cols: u16, rows: u16) -> std::io::Result<()> {
        self.master
            .resize(PtySize {
                rows: rows.max(1),
                cols: cols.max(1),
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(std::io::Error::other)
    }

    fn wait(&mut self) -> R<i32> {
        let status = self.child.wait().context("pty child wait failed")?;
        Ok(status.exit_code() as i32)
    }

    fn try_wait(&mut self) -> R<Option<i32>> {
        match self.child.try_wait().context("pty child try_wait failed")? {
            Some(status) => Ok(Some(status.exit_code() as i32)),
            None => Ok(None),
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
    }
}

struct PipeExecSession {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    stdout: Option<Box<dyn Read + Send>>,
    stderr: Option<Box<dyn Read + Send>>,
}

impl ExecSession for PipeExecSession {
    fn take_output(&mut self) -> R<ExecOutputStreams> {
        let stdout = self.stdout.take().context("pipe stdout already taken")?;
        Ok(ExecOutputStreams {
            stdout,
            stderr: self.stderr.take(),
        })
    }

    fn write_stdin(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self.stdin.as_mut() {
            Some(w) => {
                w.write_all(data)?;
                w.flush()
            }
            None => Ok(()),
        }
    }

    fn close_stdin(&mut self) {
        self.stdin = None;
    }

    fn resize(&mut self, _cols: u16, _rows: u16) -> std::io::Result<()> {
        Ok(())
    }

    fn wait(&mut self) -> R<i32> {
        let status = self.child.wait().context("exec child wait failed")?;
        Ok(status.code().unwrap_or(-1))
    }

    fn try_wait(&mut self) -> R<Option<i32>> {
        match self
            .child
            .try_wait()
            .context("exec child try_wait failed")?
        {
            Some(status) => Ok(Some(status.code().unwrap_or(-1))),
            None => Ok(None),
        }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn pipe_session_runs_command_and_captures_stdout() -> R<()> {
        let mut session = spawn_pipe(&ExecOptions::new(
            vec!["/bin/echo".into(), "hello-exec".into()],
            false,
        ))?;
        let mut out = session.take_output()?;
        let mut buf = String::new();
        out.stdout.read_to_string(&mut buf)?;
        let code = session.wait()?;
        assert_eq!(code, 0);
        assert!(buf.contains("hello-exec"), "got: {buf:?}");
        Ok(())
    }

    #[test]
    fn pipe_session_propagates_nonzero_exit() -> R<()> {
        let mut session = spawn_pipe(&ExecOptions::new(
            vec!["/bin/sh".into(), "-c".into(), "exit 7".into()],
            false,
        ))?;
        let _ = session.take_output()?;
        assert_eq!(session.wait()?, 7);
        Ok(())
    }

    /// A session given a cgroup writes the pid that runs the command into
    /// its `cgroup.procs` (a plain directory stands in for cgroupfs), before
    /// the command runs: `echo $$` prints the pid that was written.
    #[cfg(unix)]
    #[test]
    fn sessions_join_their_cgroup_before_the_command_runs_test() -> R<()> {
        for tty in [false, true] {
            let cgroup = tempfile::tempdir()?;
            let mut opts = ExecOptions::new(
                vec!["/bin/sh".into(), "-c".into(), "echo pid=$$".into()],
                tty,
            );
            opts.cgroup = Some(cgroup.path().to_path_buf());
            let mut session = if tty {
                spawn_pty(&opts)?
            } else {
                spawn_pipe(&opts)?
            };
            let mut out = session.take_output()?;
            let mut buf = Vec::new();
            // PTY read returns EIO on some platforms at EOF.
            let _ = out.stdout.read_to_end(&mut buf);
            assert_eq!(session.wait()?, 0, "tty={tty}");
            let printed = String::from_utf8_lossy(&buf);
            let joined = std::fs::read_to_string(cgroup.path().join("cgroup.procs"))?;
            assert!(
                printed.contains(&format!("pid={}", joined.trim())),
                "tty={tty}: the command ran as pid {printed:?}, but {joined:?} joined"
            );
        }
        Ok(())
    }

    /// A join that fails does not run the command unconfined.
    #[cfg(unix)]
    #[test]
    fn a_session_whose_cgroup_join_fails_does_not_run_test() -> R<()> {
        let missing = std::path::PathBuf::from("/nonexistent/legion-cgroup");
        let mut pipe = ExecOptions::new(vec!["/bin/echo".into(), "ran".into()], false);
        pipe.cgroup = Some(missing.clone());
        let err = spawn_pipe(&pipe).err().expect("the pre-exec join fails");
        assert!(format!("{err:#}").contains("legion-cgroup"), "{err:#}");

        let mut tty = ExecOptions::new(vec!["/bin/echo".into(), "ran".into()], true);
        tty.cgroup = Some(missing);
        let mut session = spawn_pty(&tty)?;
        let mut out = session.take_output()?;
        let mut buf = Vec::new();
        let _ = out.stdout.read_to_end(&mut buf);
        assert_eq!(session.wait()?, 126);
        assert!(!String::from_utf8_lossy(&buf).contains("ran\r\n"));
        Ok(())
    }

    #[test]
    fn pty_session_runs_interactively() -> R<()> {
        let mut session = spawn_pty(&ExecOptions::new(
            vec!["/bin/echo".into(), "pty-hello".into()],
            true,
        ))?;
        let mut out = session.take_output()?;
        let mut buf = Vec::new();
        // PTY read returns EIO on some platforms at EOF; tolerate a short read.
        let _ = out.stdout.read_to_end(&mut buf);
        let code = session.wait()?;
        assert_eq!(code, 0);
        assert!(String::from_utf8_lossy(&buf).contains("pty-hello"));
        Ok(())
    }
}
