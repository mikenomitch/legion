//! Exec into a microVM over vsock (sandbox-exec plan §6.1;
//! docs/task-runtime-sandbox.md, "Exec").
//!
//! [`open`] dials the guest agent's exec listener through the VMM's host
//! socket, using the convention Firecracker and cloud-hypervisor share for
//! host-initiated vsock: connect to `<alloc>/.sandbox/vsock.sock`, send
//! `CONNECT 1100\n`, read `OK <port>\n`. Then the agent's frames
//! (`legion_guest::exec`): a version hello each way, the start, and the
//! session.
//!
//! The guest is untrusted plumbing, as in Kata: every frame read from it is
//! bounded ([`legion_guest::exec::MAX_FRAME`]), anything that is not a
//! frame ends the session, and this side never interprets output.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_guest::exec::{
    frame, hello_payload, parse_exit, parse_hello, read_frame, resize_payload, write_frame, Start,
    EXEC_PORT, PROTOCOL_VERSION,
};

use crate::prelude::*;
use crate::task_plugins::{ExecOptions, ExecOutputStreams, ExecSession};

/// How long the VMM's `CONNECT` handshake and the agent's hello may take.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The message for a guest whose agent is not listening.
fn no_agent(detail: &str) -> anyhow::Error {
    anyhow!(
        "sandbox exec needs the guest agent, which is not listening in this guest ({detail})\n\
         Hint: the agent starts with the guest; a sandbox resumed from a checkpoint captured \
         before exec support has none until its next cold boot, and an image the agent cannot \
         run on (no static legion-guest on this worker) cannot take exec sessions"
    )
}

/// Opens an exec session in the guest behind `vsock_socket`.
pub fn open(vsock_socket: &Path, opts: &ExecOptions) -> R<Box<dyn ExecSession>> {
    let stream = UnixStream::connect(vsock_socket).map_err(|err| {
        no_agent(&format!(
            "the VMM's vsock socket {} refused: {err}",
            vsock_socket.display()
        ))
    })?;
    open_on(stream, opts)
}

/// Whether the guest agent behind `vsock_socket` accepts exec connections
/// right now: the VMM's socket answers `CONNECT 1100` with `OK`, which it
/// does only once the agent listens. The connection is closed before any
/// frame, which the agent treats as a session that never started.
pub fn agent_listening(vsock_socket: &Path, timeout: Duration) -> bool {
    let Ok(stream) = UnixStream::connect(vsock_socket) else {
        return false;
    };
    if stream.set_read_timeout(Some(timeout)).is_err() {
        return false;
    }
    let Ok(mut writer) = stream.try_clone() else {
        return false;
    };
    if writer
        .write_all(format!("CONNECT {EXEC_PORT}\n").as_bytes())
        .is_err()
    {
        return false;
    }
    let mut reply = [0u8; 3];
    let mut reader = stream;
    reader.read_exact(&mut reply).is_ok() && &reply == b"OK "
}

/// [`open`] on an already connected host socket.
pub fn open_on(stream: UnixStream, opts: &ExecOptions) -> R<Box<dyn ExecSession>> {
    open_within(stream, opts, HANDSHAKE_TIMEOUT)
}

/// Runs one short command in the guest behind `vsock_socket` and returns its
/// exit status, all within `timeout`: the dial, the handshake, and the run.
/// Its output is discarded.
///
/// This is for the worker's own requests on the stop path (a stop signal for
/// init, a `sync`), which run under the task-manager lock: a wedged guest
/// costs `timeout`, never a hang.
pub fn run(vsock_socket: &Path, command: Vec<String>, timeout: Duration) -> R<i32> {
    let stream = UnixStream::connect(vsock_socket).map_err(|err| {
        no_agent(&format!(
            "the VMM's vsock socket {} refused: {err}",
            vsock_socket.display()
        ))
    })?;
    run_on(stream, command, timeout)
}

/// [`run`] on an already connected host socket.
pub fn run_on(stream: UnixStream, command: Vec<String>, timeout: Duration) -> R<i32> {
    let deadline = std::time::Instant::now() + timeout;
    let shown = command.join(" ");
    let mut session = open_within(
        stream,
        &ExecOptions::new(command, false),
        timeout.min(HANDSHAKE_TIMEOUT),
    )?;
    session.close_stdin();
    // Held, not read: these commands print next to nothing, which the pipes
    // buffer. Dropping the readers would end the session early, since the
    // demux thread treats a closed stdout as the end.
    let _output = session.take_output()?;
    loop {
        if let Some(code) = session.try_wait()? {
            return Ok(code);
        }
        if std::time::Instant::now() >= deadline {
            session.kill();
            bail!(
                "the guest did not finish `{shown}` within {}ms",
                timeout.as_millis()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// [`open_on`] with an explicit bound on the `CONNECT` and hello exchange.
fn open_within(
    stream: UnixStream,
    opts: &ExecOptions,
    handshake: Duration,
) -> R<Box<dyn ExecSession>> {
    stream.set_read_timeout(Some(handshake))?;
    let mut writer = stream.try_clone()?;
    writer.write_all(format!("CONNECT {EXEC_PORT}\n").as_bytes())?;
    // Read the VMM's reply one byte at a time: anything after the newline is
    // the agent's, and must not be swallowed by a buffer.
    let mut reply = Vec::new();
    let mut byte = [0u8; 1];
    let mut reader = stream.try_clone()?;
    loop {
        match reader.read(&mut byte) {
            Ok(0) => return Err(no_agent("the VMM closed the connection on CONNECT")),
            Ok(_) if byte[0] == b'\n' => break,
            Ok(_) if reply.len() < 64 => reply.push(byte[0]),
            Ok(_) => return Err(no_agent("the VMM's reply to CONNECT was not a line")),
            Err(err) => return Err(no_agent(&format!("no reply to CONNECT: {err}"))),
        }
    }
    let reply = String::from_utf8_lossy(&reply);
    if !reply.starts_with("OK ") {
        return Err(no_agent(&format!("the VMM answered {reply:?}")));
    }

    write_frame(&mut writer, frame::HELLO, &hello_payload(PROTOCOL_VERSION))?;
    let version = match read_frame(&mut reader) {
        Ok(Some((frame::HELLO, payload))) => parse_hello(&payload).unwrap_or(0),
        Ok(Some((frame::ERROR, payload))) => {
            bail!(
                "the guest agent refused: {}",
                String::from_utf8_lossy(&payload)
            )
        }
        Ok(_) => return Err(no_agent("the agent did not say hello")),
        Err(err) => return Err(no_agent(&format!("the agent did not say hello: {err}"))),
    };
    if version < PROTOCOL_VERSION {
        bail!(
            "the guest agent speaks exec protocol {version}, and this worker needs \
             {PROTOCOL_VERSION}\nHint: restart the sandbox from a cold boot to give it this \
             worker's agent"
        );
    }

    let start = Start {
        argv: opts.command.clone(),
        tty: opts.tty,
        cols: opts.winsize.0,
        rows: opts.winsize.1,
        env: opts.env.clone(),
        cwd: opts
            .cwd
            .as_ref()
            .map(|cwd| cwd.to_string_lossy().into_owned()),
    };
    write_frame(&mut writer, frame::START, &serde_json::to_vec(&start)?)?;
    stream.set_read_timeout(None)?;
    Ok(Box::new(VsockExecSession::start(stream, writer, opts.tty)?))
}

/// What the demux thread learned about how the session ended.
#[derive(Debug, Clone)]
enum Ending {
    Exited(i32),
    Failed(String),
}

/// A live session in a guest.
pub struct VsockExecSession {
    writer: Mutex<UnixStream>,
    control: UnixStream,
    output: Option<ExecOutputStreams>,
    ending: Arc<Mutex<Option<Ending>>>,
    done: std::sync::mpsc::Receiver<()>,
    finished: bool,
}

impl VsockExecSession {
    fn start(stream: UnixStream, writer: UnixStream, tty: bool) -> R<Self> {
        let (stdout_reader, mut stdout_writer) = std::io::pipe()?;
        let (stderr_reader, mut stderr_writer) = std::io::pipe()?;
        let ending = Arc::new(Mutex::new(None));
        let (done_tx, done) = std::sync::mpsc::channel();
        let mut reader = stream.try_clone()?;
        let demux_ending = ending.clone();
        std::thread::spawn(move || {
            let outcome = loop {
                match read_frame(&mut reader) {
                    Ok(Some((frame::STDOUT, data))) => {
                        if stdout_writer.write_all(&data).is_err() {
                            break Ending::Failed("the session's output closed".into());
                        }
                    }
                    Ok(Some((frame::STDERR, data))) => {
                        let _ = stderr_writer.write_all(&data);
                    }
                    Ok(Some((frame::EXIT, payload))) => {
                        break match parse_exit(&payload) {
                            Some((code, _)) => Ending::Exited(code),
                            None => Ending::Failed("the guest agent sent a bad exit".into()),
                        };
                    }
                    Ok(Some((frame::ERROR, payload))) => {
                        break Ending::Failed(format!(
                            "the guest agent refused: {}",
                            String::from_utf8_lossy(&payload)
                        ));
                    }
                    // Nothing else is ours to read; a hostile agent gets the
                    // session ended, not a parse.
                    Ok(Some((kind, _))) => {
                        break Ending::Failed(format!(
                            "the guest agent sent an unexpected frame ({kind})"
                        ));
                    }
                    Ok(None) | Err(_) => {
                        break Ending::Failed(
                            "session ended: the guest closed it without an exit status (a \
                             suspend or a restore resets open exec sessions)"
                                .into(),
                        );
                    }
                }
            };
            *demux_ending.lock().unwrap_or_else(|p| p.into_inner()) = Some(outcome);
            // Closing the pipes ends the readers.
            drop(stdout_writer);
            drop(stderr_writer);
            let _ = done_tx.send(());
        });
        Ok(VsockExecSession {
            writer: Mutex::new(writer),
            control: stream,
            output: Some(ExecOutputStreams {
                stdout: Box::new(stdout_reader),
                stderr: (!tty).then(|| Box::new(stderr_reader) as Box<dyn Read + Send>),
            }),
            ending,
            done,
            finished: false,
        })
    }

    fn send(&self, kind: u8, payload: &[u8]) -> std::io::Result<()> {
        write_frame(
            &mut *self.writer.lock().unwrap_or_else(|p| p.into_inner()),
            kind,
            payload,
        )
    }

    fn outcome(&mut self) -> Option<R<i32>> {
        let ending = self
            .ending
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()?;
        self.finished = true;
        Some(match ending {
            Ending::Exited(code) => Ok(code),
            Ending::Failed(message) => Err(anyhow!(message)),
        })
    }
}

impl ExecSession for VsockExecSession {
    fn take_output(&mut self) -> R<ExecOutputStreams> {
        self.output
            .take()
            .context("the session's output was already taken")
    }

    fn write_stdin(&mut self, data: &[u8]) -> std::io::Result<()> {
        for chunk in data.chunks(64 * 1024) {
            self.send(frame::STDIN, chunk)?;
        }
        Ok(())
    }

    fn close_stdin(&mut self) {
        let _ = self.send(frame::STDIN_EOF, &[]);
    }

    fn resize(&mut self, cols: u16, rows: u16) -> std::io::Result<()> {
        self.send(frame::RESIZE, &resize_payload(cols, rows))
    }

    fn wait(&mut self) -> R<i32> {
        let _ = self.done.recv();
        self.outcome()
            .unwrap_or_else(|| Err(anyhow!("the session ended without an outcome")))
    }

    fn try_wait(&mut self) -> R<Option<i32>> {
        match self.done.try_recv() {
            Ok(()) | Err(std::sync::mpsc::TryRecvError::Disconnected) => self.outcome().transpose(),
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if self.finished {
                    return self.outcome().transpose();
                }
                Ok(None)
            }
        }
    }

    fn kill(&mut self) {
        // The agent kills the session's process group when the connection
        // closes.
        let _ = self.control.shutdown(std::net::Shutdown::Both);
    }
}

impl Drop for VsockExecSession {
    fn drop(&mut self) {
        let _ = self.control.shutdown(std::net::Shutdown::Both);
    }
}

/// A VMM vsock socket with a scripted guest agent behind it, for tests of
/// the worker's own requests into a guest ([`run`]). It speaks the real
/// frames but runs nothing: it reports the argv it was asked to run, then
/// answers with `exit`, or holds the session open with `None` (a wedged
/// guest) until the worker hangs up.
#[cfg(test)]
pub(crate) mod fake_agent {
    use super::*;
    use std::io::BufRead;
    use std::sync::mpsc::{channel, Receiver};

    /// Serves one session on `vmm`, the VMM's end of a host connection.
    pub(crate) fn serve(vmm: UnixStream, exit: Option<i32>) -> Receiver<Vec<String>> {
        let (seen_tx, seen) = channel();
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(vmm.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            assert_eq!(line, format!("CONNECT {EXEC_PORT}\n"));
            let mut writer = vmm.try_clone().unwrap();
            writer.write_all(b"OK 1073741824\n").unwrap();
            let mut raw = vmm.try_clone().unwrap();
            read_frame(&mut raw).unwrap();
            write_frame(&mut writer, frame::HELLO, &hello_payload(PROTOCOL_VERSION)).unwrap();
            let Ok(Some((frame::START, payload))) = read_frame(&mut raw) else {
                panic!("no start frame");
            };
            let start: Start = serde_json::from_slice(&payload).unwrap();
            let _ = seen_tx.send(start.argv);
            match exit {
                Some(code) => {
                    let payload = legion_guest::exec::exit_payload(code, "");
                    write_frame(&mut writer, frame::EXIT, &payload).unwrap();
                }
                None => while let Ok(Some(_)) = read_frame(&mut raw) {},
            }
        });
        seen
    }

    /// A VMM socket listening at `path` that serves one session.
    pub(crate) fn listen(path: &Path, exit: Option<i32>) -> Receiver<Vec<String>> {
        let listener = std::os::unix::net::UnixListener::bind(path).unwrap();
        let (seen_tx, seen) = channel();
        std::thread::spawn(move || {
            let (vmm, _) = listener.accept().unwrap();
            if let Ok(argv) = serve(vmm, exit).recv() {
                let _ = seen_tx.send(argv);
            }
        });
        seen
    }
}

#[cfg(test)]
mod vsock_exec_test {
    use super::*;
    use std::io::BufRead;

    /// A VMM socket at `path` that answers one `CONNECT` with `reply`.
    fn answering(path: &Path, reply: &'static str) {
        let listener = std::os::unix::net::UnixListener::bind(path).unwrap();
        std::thread::spawn(move || {
            let (vmm, _) = listener.accept().unwrap();
            let mut line = String::new();
            std::io::BufReader::new(vmm.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert_eq!(line, "CONNECT 1100\n");
            let _ = (&vmm).write_all(reply.as_bytes());
        });
    }

    #[test]
    fn the_agent_is_listening_once_the_vmm_answers_ok_test() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("vsock.sock");
        // No VMM socket yet: the VMM has not created it.
        assert!(!agent_listening(&socket, Duration::from_millis(50)));
        answering(&socket, "OK 1073741824\n");
        assert!(agent_listening(&socket, Duration::from_secs(1)));
    }

    #[test]
    fn a_vmm_that_refuses_the_connect_is_not_ready_test() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("vsock.sock");
        // A guest with no listener yet: the VMM closes the connection.
        answering(&socket, "");
        assert!(!agent_listening(&socket, Duration::from_secs(1)));
    }

    /// A VMM's host socket in miniature: answers `CONNECT 1100` with `OK`
    /// and hands the rest of the stream to the guest agent's real session
    /// handler.
    fn fake_vmm(reply: &'static str, agent: bool) -> UnixStream {
        let (host, vmm) = UnixStream::pair().unwrap();
        std::thread::spawn(move || {
            let mut line = String::new();
            let mut reader = std::io::BufReader::new(vmm.try_clone().unwrap());
            reader.read_line(&mut line).unwrap();
            assert_eq!(line, "CONNECT 1100\n");
            let mut writer = vmm.try_clone().unwrap();
            writer.write_all(reply.as_bytes()).unwrap();
            if agent {
                let _ = legion_guest::exec::handle_session(vmm);
            }
        });
        host
    }

    fn drain(session: &mut Box<dyn ExecSession>) -> (i32, String) {
        let mut output = session.take_output().unwrap();
        let mut text = String::new();
        output.stdout.read_to_string(&mut text).unwrap();
        (session.wait().unwrap(), text)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_session_runs_in_the_guest_with_its_own_status_test() {
        let mut opts = ExecOptions::new(
            vec![
                "sh".into(),
                "-c".into(),
                "read line; echo got-$line; exit 4".into(),
            ],
            false,
        );
        opts.env.insert("TERM".into(), "dumb".into());
        let mut session = open_on(fake_vmm("OK 1073741824\n", true), &opts).unwrap();
        session.write_stdin(b"hi\n").unwrap();
        session.close_stdin();
        let (code, text) = drain(&mut session);
        assert_eq!(code, 4);
        assert_eq!(text, "got-hi\n");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_tty_session_resizes_test() {
        let opts = ExecOptions::new(
            vec!["sh".into(), "-c".into(), "sleep 0.3; stty size".into()],
            true,
        );
        let mut session = open_on(fake_vmm("OK 5\n", true), &opts).unwrap();
        session.resize(132, 50).unwrap();
        let (code, text) = drain(&mut session);
        assert_eq!(code, 0);
        assert!(text.contains("50 132"), "{text:?}");
    }

    /// The worker's own requests (a stop signal, a `sync`) run to an exit
    /// status, with the argv exactly as given.
    #[test]
    fn a_request_runs_to_its_exit_status_test() {
        let (host, vmm) = UnixStream::pair().unwrap();
        let seen = fake_agent::serve(vmm, Some(5));
        let code = run_on(host, vec!["sync".into()], Duration::from_secs(5)).unwrap();
        assert_eq!(code, 5);
        assert_eq!(seen.recv().unwrap(), vec!["sync".to_string()]);
    }

    /// A wedged guest costs the request's timeout, never a hang: the stop
    /// path runs under the worker's task-manager lock.
    #[test]
    fn a_wedged_guest_times_the_request_out_test() {
        let (host, vmm) = UnixStream::pair().unwrap();
        let _seen = fake_agent::serve(vmm, None);
        let started = std::time::Instant::now();
        let err = run_on(host, vec!["sync".into()], Duration::from_millis(300))
            .expect_err("a wedged guest never answers");
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(
            format!("{err:#}").contains("did not finish `sync`"),
            "{err:#}"
        );
    }

    /// A guest that never answers `CONNECT` is bounded by the request's
    /// timeout too, not the 5s interactive handshake.
    #[test]
    fn a_silent_vmm_times_the_request_out_test() {
        let (host, _vmm) = UnixStream::pair().unwrap();
        let started = std::time::Instant::now();
        let err = run_on(host, vec!["sync".into()], Duration::from_millis(200))
            .expect_err("nothing answers");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(
            format!("{err:#}").contains("no reply to CONNECT"),
            "{err:#}"
        );
    }

    #[test]
    fn a_guest_without_an_agent_fails_by_name_test() {
        let opts = ExecOptions::new(vec!["true".into()], false);
        let err = open_on(fake_vmm("", false), &opts).err().expect("no agent");
        assert!(
            format!("{err:#}").contains("needs the guest agent"),
            "{err:#}"
        );
    }

    #[test]
    fn an_older_agent_is_refused_with_a_hint_test() {
        let (host, vmm) = UnixStream::pair().unwrap();
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(vmm.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut writer = vmm.try_clone().unwrap();
            writer.write_all(b"OK 7\n").unwrap();
            let mut raw = vmm.try_clone().unwrap();
            read_frame(&mut raw).unwrap();
            write_frame(&mut writer, frame::HELLO, &hello_payload(0)).unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });
        let err = open_on(host, &ExecOptions::new(vec!["true".into()], false))
            .err()
            .expect("old agent");
        assert!(format!("{err:#}").contains("exec protocol 0"), "{err:#}");
    }

    #[test]
    fn a_hostile_agent_ends_the_session_instead_of_being_parsed_test() {
        let (host, vmm) = UnixStream::pair().unwrap();
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(vmm.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut writer = vmm.try_clone().unwrap();
            writer.write_all(b"OK 7\n").unwrap();
            let mut raw = vmm.try_clone().unwrap();
            read_frame(&mut raw).unwrap();
            write_frame(&mut writer, frame::HELLO, &hello_payload(PROTOCOL_VERSION)).unwrap();
            read_frame(&mut raw).unwrap();
            // A 4 GiB frame header.
            writer
                .write_all(&[frame::STDOUT, 0xff, 0xff, 0xff, 0xff])
                .unwrap();
            std::thread::sleep(Duration::from_millis(500));
        });
        let mut session = open_on(host, &ExecOptions::new(vec!["true".into()], false)).unwrap();
        let _ = session.take_output().unwrap();
        let err = session.wait().unwrap_err();
        assert!(
            format!("{err:#}").contains("without an exit status"),
            "{err:#}"
        );
    }
}
