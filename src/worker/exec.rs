//! Worker-side exec pump.
//!
//! Drives a live [`ExecSession`](crate::task_plugins::ExecSession) for the
//! `Worker.ExecTask` streaming RPC: reads the opening `ExecStart` frame, opens
//! the session through the task manager, then bridges bytes both ways —
//! client stdin/resize frames into the process, process stdout/stderr and the
//! final exit code back out.
//!
//! The session's blocking I/O runs on dedicated OS threads; the async side
//! only shuffles frames between the gRPC stream and channels.

use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};

use crate::prelude::*;
use crate::structs::exec_ticket::ExecTicketClaims;
use crate::structs::{
    exec_client_frame, exec_server_frame, ExecClientFrame, ExecExit, ExecOutput, ExecServerFrame,
    ExecStarted,
};
use crate::task_plugins::{ExecOptions, ExecOutputStreams, ExecSession};
use crate::worker::direct_exec::{DirectSession, ExecTicketVerifier};
use crate::worker::task_manager::WorkerTaskManager;

/// Output-channel item type (already in the shape tonic wants).
type ServerItem = Result<ExecServerFrame, Status>;

/// The concrete stream type returned to tonic for `Worker.ExecTask`.
pub type ExecTaskStream = ReceiverStream<ServerItem>;

/// Open exec sessions per allocation (the manager's id), for the idle
/// policy's `exec` signal (docs/suspend-and-resume.md, "Idle policy"): an
/// instance someone is working in is never idle, however quiet the task.
static OPEN_SESSIONS: std::sync::LazyLock<Mutex<std::collections::HashMap<i64, u32>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

/// Counts one exec session as open until dropped.
pub struct OpenSession {
    allocation_id: i64,
}

impl OpenSession {
    pub fn open(allocation_id: i64) -> Self {
        *OPEN_SESSIONS
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(allocation_id)
            .or_default() += 1;
        OpenSession { allocation_id }
    }
}

impl Drop for OpenSession {
    fn drop(&mut self) {
        let mut sessions = OPEN_SESSIONS.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(count) = sessions.get_mut(&self.allocation_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                sessions.remove(&self.allocation_id);
            }
        }
    }
}

/// Exec sessions open right now, per allocation.
pub fn open_sessions() -> std::collections::HashMap<i64, u32> {
    OPEN_SESSIONS
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// Commands the async frame reader hands to the blocking driver thread.
enum InputCmd {
    Stdin(Vec<u8>),
    CloseStdin,
    Resize(u16, u16),
}

fn stdout_frame(data: Vec<u8>) -> ExecServerFrame {
    ExecServerFrame {
        frame: Some(exec_server_frame::Frame::Stdout(ExecOutput { data })),
    }
}

fn stderr_frame(data: Vec<u8>) -> ExecServerFrame {
    ExecServerFrame {
        frame: Some(exec_server_frame::Frame::Stderr(ExecOutput { data })),
    }
}

fn exit_frame(code: i32, message: String) -> ExecServerFrame {
    ExecServerFrame {
        frame: Some(exec_server_frame::Frame::Exit(ExecExit { code, message })),
    }
}

fn started_frame(session_id: String) -> ExecServerFrame {
    ExecServerFrame {
        frame: Some(exec_server_frame::Frame::Started(ExecStarted {
            session_id,
        })),
    }
}

fn pong_frame() -> ExecServerFrame {
    ExecServerFrame {
        frame: Some(exec_server_frame::Frame::Pong(crate::structs::ExecPing {})),
    }
}

/// Which caller class opened an `ExecTask` stream.
pub enum Admission {
    /// A manager certificate: the manager authorized the session and
    /// rendered its argv.
    Manager,
    /// No client certificate: the first frame must carry an exec ticket.
    /// `None` when this worker does not admit tickets (`exec.direct` off).
    Ticket(Option<Arc<ExecTicketVerifier>>),
}

/// Handle the worker's `ExecTask` RPC. Returns a stream of server frames.
pub async fn handle_exec_task(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    mut inbound: Streaming<ExecClientFrame>,
    admission: Admission,
) -> Result<ExecTaskStream, Status> {
    // First frame must be ExecStart.
    let first = inbound
        .message()
        .await
        .map_err(|e| Status::internal(format!("exec stream error: {e}")))?
        .ok_or_else(|| Status::invalid_argument("exec stream closed before start frame"))?;
    let mut start = match first.frame {
        Some(exec_client_frame::Frame::Start(start)) => start,
        _ => {
            return Err(Status::invalid_argument(
                "first exec frame must be ExecStart",
            ))
        }
    };

    // The ticket class: verify offline, then run exactly what the ticket
    // names. The ticket stays redeemed only if the session opens.
    let direct = match admission {
        Admission::Manager => None,
        Admission::Ticket(verifier) => {
            let (claims, verifier) = verify_ticket(&start, verifier).await?;
            admit_ticket(&task_manager, &mut start, &claims, &verifier)?;
            Some((claims, verifier))
        }
    };

    if start.command.is_empty() {
        return Err(Status::invalid_argument(
            "worker exec requires a resolved command (manager renders actions)",
        ));
    }

    let mut opts = ExecOptions::new(start.command.clone(), start.tty);
    if let Some(ws) = &start.winsize {
        opts.winsize = (ws.cols.max(1) as u16, ws.rows.max(1) as u16);
    }
    if start.tty {
        // A sane default terminal type; the task env can still override.
        opts.env.insert("TERM".into(), "xterm-256color".into());
    }

    let task_name = if start.task_name.is_empty() {
        None
    } else {
        Some(start.task_name.clone())
    };

    // Open the session (spawns a process) off the async runtime.
    let opened = {
        let tm = task_manager.clone();
        tokio::task::spawn_blocking(move || {
            tm.lock()
                .map_err(|_| anyhow!("task manager lock poisoned"))?
                .open_exec_session(start.allocation_id, task_name.as_deref(), opts)
        })
        .await
        .map_err(|e| Status::internal(format!("exec open panicked: {e}")))?
    };
    let session = match opened {
        Ok(session) => session,
        Err(err) => {
            // A ticket whose session never opened can be presented again
            // inside its redeem window.
            if let Some((claims, verifier)) = &direct {
                verifier.release(&claims.jti);
            }
            return Err(Status::failed_precondition(err.to_string()));
        }
    };
    let direct = direct.map(|(claims, _)| {
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        info!(
            jti = %claims.jti,
            allocation_id = claims.allocation_id,
            job_id = %claims.job,
            task = %claims.task,
            "Direct exec session opened"
        );
        DirectSession::open(&claims, &start.command, now)
    });

    let (out_tx, out_rx) = mpsc::channel::<ServerItem>(128);
    let (in_tx, in_rx) = std_mpsc::channel::<InputCmd>();

    // Ack: session is live.
    let session_id = format!("exec-{}", start.allocation_id);
    let _ = out_tx.send(Ok(started_frame(session_id))).await;

    // Driver thread owns the session and does all blocking I/O. It also
    // holds the open-session count, so the count drops when the session ends.
    spawn_driver(
        session,
        out_tx.clone(),
        in_rx,
        OpenSession::open(start.allocation_id),
        direct,
    );

    // Async reader: pull client frames, forward to the driver / answer pings.
    tokio::spawn(async move {
        while let Ok(Some(frame)) = inbound.message().await {
            match frame.frame {
                Some(exec_client_frame::Frame::Stdin(input)) => {
                    if !input.data.is_empty() {
                        let _ = in_tx.send(InputCmd::Stdin(input.data));
                    }
                    if input.eof {
                        let _ = in_tx.send(InputCmd::CloseStdin);
                    }
                }
                Some(exec_client_frame::Frame::Resize(resize)) => {
                    if let Some(ws) = resize.winsize {
                        let _ = in_tx.send(InputCmd::Resize(ws.cols as u16, ws.rows as u16));
                    }
                }
                Some(exec_client_frame::Frame::Ping(_)) => {
                    let _ = out_tx.send(Ok(pong_frame())).await;
                }
                // A second Start frame is a protocol error; ignore it.
                _ => {}
            }
        }
        // On stream end/error, dropping `in_tx` tears the session down.
    });

    Ok(ReceiverStream::new(out_rx))
}

/// Verifies a ticket-class `ExecStart`'s ticket. Off the async runtime: a
/// verification can refresh the JWKS, which blocks.
async fn verify_ticket(
    start: &crate::structs::ExecStart,
    verifier: Option<Arc<ExecTicketVerifier>>,
) -> Result<(ExecTicketClaims, Arc<ExecTicketVerifier>), Status> {
    if start.exec_ticket.is_empty() {
        return Err(Status::unauthenticated(
            "ExecTask admits a manager certificate or an exec ticket; this request carried \
             neither",
        ));
    }
    let Some(verifier) = verifier else {
        return Err(Status::failed_precondition(
            "this worker does not admit exec tickets\nHint: set exec = { direct = true } in its \
             worker config, or exec through the manager (drop --direct)",
        ));
    };
    let token = start.exec_ticket.clone();
    let checking = verifier.clone();
    let claims = tokio::task::spawn_blocking(move || {
        checking.verify(&token, time::OffsetDateTime::now_utc().unix_timestamp())
    })
    .await
    .map_err(|e| Status::internal(format!("ticket verification panicked: {e}")))?
    .map_err(|err| Status::permission_denied(format!("exec ticket rejected: {err:#}")))?;
    Ok((claims, verifier))
}

/// Checks a verified ticket against this node and the `ExecStart`, redeems
/// it, and rewrites the start to what the ticket allows: its allocation,
/// its task, and its argv.
fn admit_ticket(
    task_manager: &Arc<Mutex<WorkerTaskManager>>,
    start: &mut crate::structs::ExecStart,
    claims: &ExecTicketClaims,
    verifier: &ExecTicketVerifier,
) -> Result<(), Status> {
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let refuse = |why: String| Status::permission_denied(format!("exec ticket rejected: {why}"));

    if start.allocation_id != 0 && start.allocation_id != claims.allocation_id {
        return Err(refuse(format!(
            "it is for allocation {}, not {}",
            claims.allocation_id, start.allocation_id
        )));
    }
    let target = task_manager
        .lock()
        .map_err(|_| Status::internal("task manager lock poisoned"))?
        .exec_target(claims.allocation_id);
    let Some((job, task)) = target else {
        return Err(refuse(format!(
            "allocation {} is not running on this node",
            claims.allocation_id
        )));
    };
    if job != claims.job || task != claims.task {
        return Err(refuse(format!(
            "allocation {} runs {job}/{task}, not {}/{}",
            claims.allocation_id, claims.job, claims.task
        )));
    }
    if !start.task_name.is_empty() && start.task_name != claims.task {
        return Err(refuse(format!(
            "it is for task {}, not {}",
            claims.task, start.task_name
        )));
    }
    if start.command.is_empty() {
        match &claims.argv {
            Some(argv) => start.command = argv.clone(),
            None => {
                return Err(Status::invalid_argument(
                    "the ticket allows any command; name one in ExecStart.command",
                ))
            }
        }
    }
    claims.allows(&start.command, start.tty).map_err(&refuse)?;
    verifier
        .redeem(claims, now)
        .map_err(|err| refuse(format!("{err:#}")))?;

    start.allocation_id = claims.allocation_id;
    start.task_name = claims.task.clone();
    Ok(())
}

/// Spawn the OS thread that owns the session: starts reader threads, applies
/// input commands, and emits the terminal exit frame. A direct session also
/// ends when its `max_session` runs out or a manager revokes it.
fn spawn_driver(
    mut session: Box<dyn ExecSession>,
    out_tx: mpsc::Sender<ServerItem>,
    in_rx: std_mpsc::Receiver<InputCmd>,
    open: OpenSession,
    direct: Option<DirectSession>,
) {
    std::thread::spawn(move || {
        let _open = open;
        let end = direct.as_ref().map(|direct| direct.end.clone());
        let mut ended_by = String::new();
        // Start output readers.
        let reader_handles = match session.take_output() {
            Ok(ExecOutputStreams { stdout, stderr }) => {
                let mut handles = Vec::new();
                handles.push(spawn_reader(stdout, false, out_tx.clone()));
                if let Some(stderr) = stderr {
                    handles.push(spawn_reader(stderr, true, out_tx.clone()));
                }
                handles
            }
            Err(err) => {
                let _ = out_tx.blocking_send(Ok(exit_frame(-1, err.to_string())));
                if let Some(direct) = direct {
                    direct.finish(-1, "the session's output could not be read");
                }
                return;
            }
        };

        // Pump input + poll for exit.
        let exit = loop {
            let mut disconnected = false;
            loop {
                match in_rx.try_recv() {
                    Ok(InputCmd::Stdin(data)) => {
                        let _ = session.write_stdin(&data);
                    }
                    Ok(InputCmd::CloseStdin) => session.close_stdin(),
                    Ok(InputCmd::Resize(cols, rows)) => {
                        let _ = session.resize(cols, rows);
                    }
                    Err(std_mpsc::TryRecvError::Empty) => break,
                    Err(std_mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            match session.try_wait() {
                Ok(Some(code)) => break Ok(code),
                Ok(None) => {}
                Err(err) => break Err(DriverEnd::Failed(err.to_string())),
            }

            if disconnected {
                // Client went away before the process exited: kill it.
                session.kill();
                ended_by = "client disconnected".into();
                break Ok(-1);
            }

            if let Some(end) = &end {
                if let Some(reason) = end.revoked() {
                    session.kill();
                    ended_by = format!("revoked: {reason}");
                    break Err(DriverEnd::Revoked(reason));
                }
                if std::time::Instant::now() >= end.deadline {
                    session.kill();
                    ended_by = "max_session".into();
                    break Err(DriverEnd::Expired);
                }
            }

            std::thread::sleep(Duration::from_millis(5));
        };

        // Drain any output still buffered in the reader threads before the
        // terminal frame, so exit never races ahead of the last bytes.
        for handle in reader_handles {
            let _ = handle.join();
        }

        let (item, code) = match exit {
            Ok(code) => (Ok(exit_frame(code, String::new())), code),
            Err(DriverEnd::Failed(message)) => (Ok(exit_frame(-1, message)), -1),
            Err(DriverEnd::Expired) => (
                Ok(exit_frame(
                    -1,
                    "session ended: the exec ticket's max_session elapsed".into(),
                )),
                -1,
            ),
            Err(DriverEnd::Revoked(reason)) => (
                Err(Status::permission_denied(format!(
                    "session ended: {reason}"
                ))),
                -1,
            ),
        };
        let _ = out_tx.blocking_send(item);
        if let Some(direct) = direct {
            direct.finish(code, &ended_by);
        }
    });
}

/// Why the driver stopped other than the process exiting by itself.
enum DriverEnd {
    Failed(String),
    Expired,
    Revoked(String),
}

/// Reader thread: blocking-read a stream and push output frames.
fn spawn_reader(
    mut reader: Box<dyn std::io::Read + Send>,
    is_stderr: bool,
    out_tx: mpsc::Sender<ServerItem>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let data = buf[..n].to_vec();
                    let frame = if is_stderr {
                        stderr_frame(data)
                    } else {
                        stdout_frame(data)
                    };
                    if out_tx.blocking_send(Ok(frame)).is_err() {
                        break;
                    }
                }
                // EIO at PTY EOF is normal on Linux; treat any error as EOF.
                Err(_) => break,
            }
        }
    })
}
