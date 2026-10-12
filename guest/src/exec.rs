//! Exec into a microVM sandbox (sandbox-exec plan §6.1;
//! docs/task-runtime-sandbox.md, "Exec").
//!
//! The agent listens on guest vsock port [`EXEC_PORT`]. The worker connects
//! through the VMM's host socket (`CONNECT 1100`), and the two speak the
//! frames below: a version hello each way, then a [`Start`], then stdin,
//! resize, and output frames until the process exits and the agent sends
//! [`frame::EXIT`]. Closing the connection kills the session's process
//! group, which is how both the worker's `kill()` and a vanished client
//! land.
//!
//! The frames are a five-byte header (kind, then a big-endian `u32` payload
//! length) and the payload. Not protobuf: this crate builds into a static
//! binary that must stay small enough for any image, and the worker
//! translates these frames to and from the `ExecClientFrame` /
//! `ExecServerFrame` protobufs every other exec leg speaks.
//!
//! The guest is untrusted. The worker bounds every frame it reads to
//! [`MAX_FRAME`] bytes and parses nothing but these frames; the agent bounds
//! its sessions to [`MAX_SESSIONS`].

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The guest vsock port the agent's exec listener binds.
pub const EXEC_PORT: u32 = 1100;
/// The exec protocol this agent and worker speak. Raised when a frame
/// changes meaning; each side refuses a peer older than it can serve.
pub const PROTOCOL_VERSION: u32 = 1;
/// The largest frame either side accepts.
pub const MAX_FRAME: usize = 1 << 20;
/// The most exec sessions one guest runs at once.
pub const MAX_SESSIONS: usize = 64;

/// Frame kinds.
pub mod frame {
    /// `u32` protocol version, both directions, first.
    pub const HELLO: u8 = 1;
    /// JSON [`super::Start`], worker to agent, once.
    pub const START: u8 = 2;
    /// Bytes for the process's stdin.
    pub const STDIN: u8 = 3;
    /// Half-close stdin (EOT on a pty).
    pub const STDIN_EOF: u8 = 4;
    /// `u16` cols then `u16` rows.
    pub const RESIZE: u8 = 5;
    /// Process output.
    pub const STDOUT: u8 = 6;
    pub const STDERR: u8 = 7;
    /// `i32` exit code then a UTF-8 message; the last frame of a session.
    pub const EXIT: u8 = 8;
    /// A UTF-8 reason the session could not start; the agent closes after.
    pub const ERROR: u8 = 9;
}

/// What to run.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Start {
    pub argv: Vec<String>,
    pub tty: bool,
    #[serde(default)]
    pub cols: u16,
    #[serde(default)]
    pub rows: u16,
    /// Set on top of the task environment the guest sources.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<String>,
}

/// Writes one frame.
pub fn write_frame(writer: &mut impl Write, kind: u8, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("frame of {} bytes is above {MAX_FRAME}", payload.len()),
        ));
    }
    let mut header = [0u8; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    writer.flush()
}

/// Reads one frame: `None` on a clean end of stream between frames, an
/// error on a truncated or oversized one.
pub fn read_frame(reader: &mut impl Read) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0u8; 5];
    let mut filled = 0;
    while filled < header.len() {
        match reader.read(&mut header[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the stream ended inside a frame header",
                ))
            }
            Ok(read) => filled += read,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a frame of {length} bytes is above the {MAX_FRAME}-byte limit"),
        ));
    }
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;
    Ok(Some((header[0], payload)))
}

pub fn hello_payload(version: u32) -> [u8; 4] {
    version.to_be_bytes()
}

pub fn parse_hello(payload: &[u8]) -> Option<u32> {
    Some(u32::from_be_bytes(payload.get(..4)?.try_into().ok()?))
}

pub fn exit_payload(code: i32, message: &str) -> Vec<u8> {
    let mut payload = code.to_be_bytes().to_vec();
    payload.extend_from_slice(message.as_bytes());
    payload
}

pub fn parse_exit(payload: &[u8]) -> Option<(i32, String)> {
    let code = i32::from_be_bytes(payload.get(..4)?.try_into().ok()?);
    Some((code, String::from_utf8_lossy(&payload[4..]).into_owned()))
}

pub fn resize_payload(cols: u16, rows: u16) -> [u8; 4] {
    let mut payload = [0u8; 4];
    payload[..2].copy_from_slice(&cols.to_be_bytes());
    payload[2..].copy_from_slice(&rows.to_be_bytes());
    payload
}

pub fn parse_resize(payload: &[u8]) -> Option<(u16, u16)> {
    Some((
        u16::from_be_bytes(payload.get(..2)?.try_into().ok()?),
        u16::from_be_bytes(payload.get(2..4)?.try_into().ok()?),
    ))
}

/// How the agent runs a command: through `/bin/sh`, sourcing the task's
/// environment the way init does (and the resume handler's refresh after a
/// restore), then `exec`ing the argv so the exit status is the process's
/// own.
const PRELUDE: &str = "[ -r /legion/env ] && . /legion/env; \
     [ -r /legion/resume.env ] && . /legion/resume.env; \
     exec \"$@\"";

/// Sessions running in this agent.
static SESSIONS: AtomicUsize = AtomicUsize::new(0);

/// The connection of every running session, so a restore can end them all
/// (a memory capture resets them, and some VMMs leave them half open).
static OPEN: Mutex<Vec<(u64, UnixStream)>> = Mutex::new(Vec::new());
static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Ends every running session, as a vanished worker would. Returns how
/// many there were. Called by the agent's resume watch after a restore.
pub fn close_all() -> usize {
    let open = std::mem::take(&mut *OPEN.lock().unwrap_or_else(|p| p.into_inner()));
    for (_, stream) in &open {
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }
    open.len()
}

/// Removes a session from [`OPEN`] when it ends.
struct Registered(u64);

impl Registered {
    fn new(stream: &UnixStream) -> io::Result<Self> {
        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        OPEN.lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((id, stream.try_clone()?));
        Ok(Registered(id))
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        OPEN.lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(id, _)| *id != self.0);
    }
}

/// Accepts exec connections forever on `listener`'s connections, one thread
/// per session.
pub fn serve(connections: impl Iterator<Item = io::Result<UnixStream>>) {
    for connection in connections {
        match connection {
            Ok(stream) => {
                std::thread::spawn(move || {
                    if let Err(err) = handle_session(stream) {
                        eprintln!("legion-guest-agent: exec session ended: {err}");
                    }
                });
            }
            Err(err) => {
                eprintln!("legion-guest-agent: exec accept failed: {err}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Counts a session against [`MAX_SESSIONS`] while held.
struct Slot;

impl Slot {
    fn take() -> Option<Slot> {
        let previous = SESSIONS.fetch_add(1, Ordering::SeqCst);
        if previous >= MAX_SESSIONS {
            SESSIONS.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(Slot)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        SESSIONS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Runs one session on `stream`: hello, start, relay, exit.
pub fn handle_session(stream: UnixStream) -> io::Result<()> {
    let _registered = Registered::new(&stream)?;
    let mut reader = stream.try_clone()?;
    let writer = Arc::new(Mutex::new(stream.try_clone()?));
    let send = |kind: u8, payload: &[u8]| -> io::Result<()> {
        write_frame(
            &mut *writer.lock().unwrap_or_else(|p| p.into_inner()),
            kind,
            payload,
        )
    };

    match read_frame(&mut reader)? {
        Some((frame::HELLO, payload)) if parse_hello(&payload).is_some_and(|v| v >= 1) => {}
        _ => {
            let _ = send(
                frame::ERROR,
                b"expected a hello with exec protocol 1 or later",
            );
            return Ok(());
        }
    }
    send(frame::HELLO, &hello_payload(PROTOCOL_VERSION))?;
    let Some(_slot) = Slot::take() else {
        send(
            frame::ERROR,
            format!("this guest already runs {MAX_SESSIONS} exec sessions").as_bytes(),
        )?;
        return Ok(());
    };
    let start: Start = match read_frame(&mut reader)? {
        Some((frame::START, payload)) => match serde_json::from_slice(&payload) {
            Ok(start) => start,
            Err(err) => {
                send(frame::ERROR, format!("bad start frame: {err}").as_bytes())?;
                return Ok(());
            }
        },
        _ => {
            send(frame::ERROR, b"expected a start frame")?;
            return Ok(());
        }
    };
    if start.argv.is_empty() {
        send(frame::ERROR, b"exec needs a command")?;
        return Ok(());
    }
    let mut spawned = match spawn(&start) {
        Ok(spawned) => spawned,
        Err(err) => {
            send(
                frame::ERROR,
                format!("could not start {:?}: {err}", start.argv[0]).as_bytes(),
            )?;
            return Ok(());
        }
    };

    // Output relays.
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let mut relays = 0;
    for (source, kind) in spawned.outputs.drain(..) {
        relays += 1;
        let writer = writer.clone();
        let done = done_tx.clone();
        std::thread::spawn(move || {
            relay_output(source, kind, &writer);
            let _ = done.send(());
        });
    }
    drop(done_tx);

    // The waiter: once the process exits, drain output (briefly: a
    // background job can hold a pty open forever), send the exit, and close.
    let exited = Arc::new(AtomicBool::new(false));
    let pid = spawned.child.id() as libc::pid_t;
    {
        let writer = writer.clone();
        let exited = exited.clone();
        let mut child = spawned.child;
        let stream = stream.try_clone()?;
        std::thread::spawn(move || {
            let code = match child.wait() {
                Ok(status) => exit_code(status),
                Err(_) => -1,
            };
            exited.store(true, Ordering::SeqCst);
            for _ in 0..relays {
                if done_rx.recv_timeout(Duration::from_millis(500)).is_err() {
                    break;
                }
            }
            let _ = write_frame(
                &mut *writer.lock().unwrap_or_else(|p| p.into_inner()),
                frame::EXIT,
                &exit_payload(code, ""),
            );
            let _ = stream.shutdown(std::net::Shutdown::Both);
        });
    }

    // Input from the worker, until it hangs up.
    let mut stdin = spawned.stdin;
    let master = spawned.master;
    loop {
        match read_frame(&mut reader) {
            Ok(Some((frame::STDIN, data))) => {
                if let Some(input) = stdin.as_mut() {
                    let _ = input.write_all(&data);
                }
            }
            Ok(Some((frame::STDIN_EOF, _))) => {
                if master.is_some() {
                    // A pty has no half-close: send EOT, as a terminal does.
                    if let Some(input) = stdin.as_mut() {
                        let _ = input.write_all(&[0x04]);
                    }
                } else {
                    stdin = None;
                }
            }
            Ok(Some((frame::RESIZE, payload))) => {
                if let (Some(fd), Some((cols, rows))) = (master, parse_resize(&payload)) {
                    set_winsize(fd, cols, rows);
                }
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }
    if !exited.load(Ordering::SeqCst) {
        // The worker went away (a kill, a vanished client, a restore that
        // reset the connection): end the whole process group.
        // SAFETY: kill(2) on the session's own process group.
        unsafe { libc::kill(-pid, libc::SIGKILL) };
    }
    Ok(())
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    status
        .code()
        .or_else(|| status.signal().map(|signal| 128 + signal))
        .unwrap_or(-1)
}

fn relay_output(mut source: Box<dyn Read + Send>, kind: u8, writer: &Mutex<UnixStream>) {
    let mut buffer = [0u8; 16 * 1024];
    loop {
        match source.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                let mut guard = writer.lock().unwrap_or_else(|p| p.into_inner());
                if write_frame(&mut *guard, kind, &buffer[..read]).is_err() {
                    break;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            // EIO when a pty's last slave closes: the end.
            Err(_) => break,
        }
    }
}

struct Spawned {
    child: std::process::Child,
    stdin: Option<Box<dyn Write + Send>>,
    outputs: Vec<(Box<dyn Read + Send>, u8)>,
    /// The pty master, for resizes.
    master: Option<i32>,
}

/// Where the worker writes the task's working directory, so a session
/// starts where the command did.
pub const GUEST_CWD_PATH: &str = "/legion/cwd";

fn command(start: &Start) -> std::process::Command {
    let mut command = std::process::Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(PRELUDE)
        .arg("legion-exec")
        .args(&start.argv)
        .envs(&start.env);
    let cwd = start.cwd.clone().filter(|cwd| !cwd.is_empty()).or_else(|| {
        std::fs::read_to_string(GUEST_CWD_PATH)
            .ok()
            .map(|cwd| cwd.trim().to_string())
            .filter(|cwd| !cwd.is_empty() && std::path::Path::new(cwd).is_dir())
    });
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
}

#[cfg(target_os = "linux")]
fn spawn(start: &Start) -> io::Result<Spawned> {
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;

    let mut command = command(start);
    if !start.tty {
        // SAFETY: setsid is async-signal-safe; it makes the session its own
        // process group, so a kill reaches everything it started.
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .map(|s| Box::new(s) as Box<dyn Write + Send>);
        let mut outputs: Vec<(Box<dyn Read + Send>, u8)> = Vec::new();
        if let Some(out) = child.stdout.take() {
            outputs.push((Box::new(out), frame::STDOUT));
        }
        if let Some(err) = child.stderr.take() {
            outputs.push((Box::new(err), frame::STDERR));
        }
        return Ok(Spawned {
            child,
            stdin,
            outputs,
            master: None,
        });
    }

    let (master, slave) = open_pty(start.cols.max(1), start.rows.max(1))?;
    // SAFETY: both fds were just opened and are owned here.
    let slave_owned = unsafe { OwnedFd::from_raw_fd(slave) };
    let stdio = |fd: &OwnedFd| -> io::Result<Stdio> { Ok(Stdio::from(fd.try_clone()?)) };
    command
        .stdin(stdio(&slave_owned)?)
        .stdout(stdio(&slave_owned)?)
        .stderr(stdio(&slave_owned)?);
    // SAFETY: setsid and TIOCSCTTY are async-signal-safe; the child takes
    // the pty as its controlling terminal, as a login shell expects.
    unsafe {
        command.pre_exec(|| {
            libc::setsid();
            libc::ioctl(0, libc::TIOCSCTTY as _, 0);
            Ok(())
        });
    }
    let child = command.spawn();
    drop(slave_owned);
    let child = match child {
        Ok(child) => child,
        Err(err) => {
            // SAFETY: closing our own master on failure.
            unsafe { libc::close(master) };
            return Err(err);
        }
    };
    // SAFETY: dup of our own master fd, one owner per handle.
    let reader = unsafe { std::fs::File::from_raw_fd(libc::dup(master)) };
    let writer = unsafe { std::fs::File::from_raw_fd(libc::dup(master)) };
    Ok(Spawned {
        child,
        stdin: Some(Box::new(writer)),
        outputs: vec![(Box::new(reader), frame::STDOUT)],
        master: Some(master),
    })
}

#[cfg(not(target_os = "linux"))]
fn spawn(_start: &Start) -> io::Result<Spawned> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "exec runs inside a Linux sandbox guest only",
    ))
}

/// A new pty pair, sized, as (master, slave) fds.
#[cfg(target_os = "linux")]
fn open_pty(cols: u16, rows: u16) -> io::Result<(i32, i32)> {
    // SAFETY: plain libc pty calls on fds this function owns until return.
    unsafe {
        let master = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC);
        if master < 0 {
            return Err(io::Error::last_os_error());
        }
        if libc::grantpt(master) != 0 || libc::unlockpt(master) != 0 {
            let err = io::Error::last_os_error();
            libc::close(master);
            return Err(err);
        }
        let mut name = [0 as libc::c_char; 128];
        if libc::ptsname_r(master, name.as_mut_ptr(), name.len()) != 0 {
            let err = io::Error::last_os_error();
            libc::close(master);
            return Err(err);
        }
        let slave = libc::open(
            name.as_ptr(),
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
        );
        if slave < 0 {
            let err = io::Error::last_os_error();
            libc::close(master);
            return Err(err);
        }
        set_winsize(master, cols, rows);
        Ok((master, slave))
    }
}

fn set_winsize(fd: i32, cols: u16, rows: u16) {
    let size = libc::winsize {
        ws_row: rows.max(1),
        ws_col: cols.max(1),
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCSWINSZ on a pty fd with a valid winsize.
    unsafe { libc::ioctl(fd, libc::TIOCSWINSZ as _, &size) };
}

/// Listens on guest vsock `port` (any CID) and serves exec sessions on a
/// background thread.
#[cfg(target_os = "linux")]
pub fn serve_vsock(port: u32) -> io::Result<()> {
    use std::os::fd::FromRawFd;
    // SAFETY: a plain AF_VSOCK listening socket owned by the thread below.
    let fd = unsafe {
        let fd = libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut addr: libc::sockaddr_vm = std::mem::zeroed();
        addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
        addr.svm_cid = libc::VMADDR_CID_ANY;
        addr.svm_port = port;
        if libc::bind(
            fd,
            &addr as *const libc::sockaddr_vm as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        ) != 0
            || libc::listen(fd, 64) != 0
        {
            let err = io::Error::last_os_error();
            libc::close(fd);
            return Err(err);
        }
        fd
    };
    std::thread::spawn(move || {
        let connections = std::iter::from_fn(move || {
            // SAFETY: accept on our own listening fd; the new fd is owned by
            // the stream it is wrapped in.
            let accepted = unsafe {
                libc::accept4(
                    fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_CLOEXEC,
                )
            };
            Some(if accepted < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(unsafe { UnixStream::from_raw_fd(accepted) })
            })
        });
        serve(connections);
    });
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn serve_vsock(_port: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "AF_VSOCK is Linux-only",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worker's half, minimal: hello, start, then collect until exit.
    fn run(start: Start, stdin: &[u8]) -> (i32, Vec<u8>, Vec<u8>) {
        let (mut worker, agent) = UnixStream::pair().unwrap();
        let session = std::thread::spawn(move || handle_session(agent));
        write_frame(&mut worker, frame::HELLO, &hello_payload(PROTOCOL_VERSION)).unwrap();
        let (kind, payload) = read_frame(&mut worker).unwrap().unwrap();
        assert_eq!(kind, frame::HELLO);
        assert_eq!(parse_hello(&payload), Some(PROTOCOL_VERSION));
        write_frame(
            &mut worker,
            frame::START,
            &serde_json::to_vec(&start).unwrap(),
        )
        .unwrap();
        if !stdin.is_empty() {
            write_frame(&mut worker, frame::STDIN, stdin).unwrap();
        }
        write_frame(&mut worker, frame::STDIN_EOF, &[]).unwrap();
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        loop {
            match read_frame(&mut worker).unwrap() {
                Some((frame::STDOUT, data)) => stdout.extend(data),
                Some((frame::STDERR, data)) => stderr.extend(data),
                Some((frame::EXIT, payload)) => {
                    let (code, _) = parse_exit(&payload).unwrap();
                    session.join().unwrap().unwrap();
                    return (code, stdout, stderr);
                }
                Some((frame::ERROR, payload)) => {
                    panic!("agent error: {}", String::from_utf8_lossy(&payload))
                }
                Some(_) => {}
                None => panic!("closed before exit"),
            }
        }
    }

    fn argv(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| word.to_string()).collect()
    }

    #[test]
    fn frames_round_trip_and_bound_their_size() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, frame::STDOUT, b"hello").unwrap();
        write_frame(&mut buffer, frame::EXIT, &exit_payload(3, "done")).unwrap();
        let mut reader = &buffer[..];
        assert_eq!(
            read_frame(&mut reader).unwrap(),
            Some((frame::STDOUT, b"hello".to_vec()))
        );
        let (_, exit) = read_frame(&mut reader).unwrap().unwrap();
        assert_eq!(parse_exit(&exit), Some((3, "done".to_string())));
        assert_eq!(read_frame(&mut reader).unwrap(), None);

        // A hostile length is refused before anything is allocated.
        let mut hostile = vec![frame::STDOUT];
        hostile.extend_from_slice(&(u32::MAX).to_be_bytes());
        let err = read_frame(&mut &hostile[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // A truncated header is an error, not a clean end.
        assert!(read_frame(&mut &[frame::STDOUT, 0][..]).is_err());
        assert_eq!(parse_resize(&resize_payload(120, 40)), Some((120, 40)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_pipe_session_runs_the_command_with_its_own_exit_status() {
        let (code, stdout, stderr) = run(
            Start {
                argv: argv(&["sh", "-c", "cat; echo err >&2; exit 3"]),
                ..Default::default()
            },
            b"piped-in\n",
        );
        assert_eq!(code, 3);
        assert_eq!(stdout, b"piped-in\n");
        assert_eq!(stderr, b"err\n");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_tty_session_gets_a_terminal_of_the_asked_size() {
        let (code, stdout, _) = run(
            Start {
                argv: argv(&["sh", "-c", "stty size; test -t 0 && echo is-a-tty"]),
                tty: true,
                cols: 100,
                rows: 30,
                ..Default::default()
            },
            b"",
        );
        assert_eq!(code, 0);
        let text = String::from_utf8_lossy(&stdout);
        assert!(text.contains("30 100"), "{text:?}");
        assert!(text.contains("is-a-tty"), "{text:?}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_vanished_worker_kills_the_process_group() {
        let (mut worker, agent) = UnixStream::pair().unwrap();
        let session = std::thread::spawn(move || handle_session(agent));
        write_frame(&mut worker, frame::HELLO, &hello_payload(PROTOCOL_VERSION)).unwrap();
        read_frame(&mut worker).unwrap();
        let marker = std::env::temp_dir().join(format!("legion-exec-kill-{}", std::process::id()));
        let start = Start {
            argv: argv(&[
                "sh",
                "-c",
                &format!("sleep 30 & echo $! > {}; wait", marker.display()),
            ]),
            ..Default::default()
        };
        write_frame(
            &mut worker,
            frame::START,
            &serde_json::to_vec(&start).unwrap(),
        )
        .unwrap();
        let started = std::time::Instant::now();
        while !marker.exists() || std::fs::read_to_string(&marker).unwrap().trim().is_empty() {
            assert!(started.elapsed() < Duration::from_secs(10));
            std::thread::sleep(Duration::from_millis(20));
        }
        let sleeper: i32 = std::fs::read_to_string(&marker)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        drop(worker);
        session.join().unwrap().unwrap();
        // The background `sleep` died with its group.
        let started = std::time::Instant::now();
        // SAFETY: signal 0 only probes for existence.
        while unsafe { libc::kill(sleeper, 0) } == 0 {
            assert!(started.elapsed() < Duration::from_secs(5), "sleep survived");
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = std::fs::remove_file(&marker);
    }

    #[test]
    fn an_old_or_silent_peer_gets_a_reason() {
        let (mut worker, agent) = UnixStream::pair().unwrap();
        let session = std::thread::spawn(move || handle_session(agent));
        write_frame(&mut worker, frame::HELLO, &hello_payload(0)).unwrap();
        let (kind, payload) = read_frame(&mut worker).unwrap().unwrap();
        assert_eq!(kind, frame::ERROR);
        assert!(String::from_utf8_lossy(&payload).contains("exec protocol 1"));
        session.join().unwrap().unwrap();
    }
}
