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
use crate::structs::{
    exec_client_frame, exec_server_frame, ExecClientFrame, ExecExit, ExecOutput, ExecServerFrame,
    ExecStarted,
};
use crate::task_plugins::{ExecOptions, ExecOutputStreams, ExecSession};
use crate::worker::task_manager::WorkerTaskManager;

/// Output-channel item type (already in the shape tonic wants).
type ServerItem = Result<ExecServerFrame, Status>;

/// The concrete stream type returned to tonic for `Worker.ExecTask`.
pub type ExecTaskStream = ReceiverStream<ServerItem>;

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
        frame: Some(exec_server_frame::Frame::Started(ExecStarted { session_id })),
    }
}

fn pong_frame() -> ExecServerFrame {
    ExecServerFrame {
        frame: Some(exec_server_frame::Frame::Pong(
            crate::structs::ExecPing {},
        )),
    }
}

/// Handle the worker's `ExecTask` RPC. Returns a stream of server frames.
pub async fn handle_exec_task(
    task_manager: Arc<Mutex<WorkerTaskManager>>,
    mut inbound: Streaming<ExecClientFrame>,
) -> Result<ExecTaskStream, Status> {
    // First frame must be ExecStart.
    let first = inbound
        .message()
        .await
        .map_err(|e| Status::internal(format!("exec stream error: {e}")))?
        .ok_or_else(|| Status::invalid_argument("exec stream closed before start frame"))?;
    let start = match first.frame {
        Some(exec_client_frame::Frame::Start(start)) => start,
        _ => {
            return Err(Status::invalid_argument(
                "first exec frame must be ExecStart",
            ))
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
    let session = {
        let tm = task_manager.clone();
        tokio::task::spawn_blocking(move || {
            tm.lock()
                .map_err(|_| anyhow!("task manager lock poisoned"))?
                .open_exec_session(start.allocation_id, task_name.as_deref(), opts)
        })
        .await
        .map_err(|e| Status::internal(format!("exec open panicked: {e}")))?
        .map_err(|e| Status::failed_precondition(e.to_string()))?
    };

    let (out_tx, out_rx) = mpsc::channel::<ServerItem>(128);
    let (in_tx, in_rx) = std_mpsc::channel::<InputCmd>();

    // Ack: session is live.
    let session_id = format!("exec-{}", start.allocation_id);
    let _ = out_tx.send(Ok(started_frame(session_id))).await;

    // Driver thread owns the session and does all blocking I/O.
    spawn_driver(session, out_tx.clone(), in_rx);

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

/// Spawn the OS thread that owns the session: starts reader threads, applies
/// input commands, and emits the terminal exit frame.
fn spawn_driver(
    mut session: Box<dyn ExecSession>,
    out_tx: mpsc::Sender<ServerItem>,
    in_rx: std_mpsc::Receiver<InputCmd>,
) {
    std::thread::spawn(move || {
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
                Err(err) => break Err(err.to_string()),
            }

            if disconnected {
                // Client went away before the process exited: kill it.
                session.kill();
                break Ok(-1);
            }

            std::thread::sleep(Duration::from_millis(5));
        };

        // Drain any output still buffered in the reader threads before the
        // terminal frame, so exit never races ahead of the last bytes.
        for handle in reader_handles {
            let _ = handle.join();
        }

        let frame = match exit {
            Ok(code) => exit_frame(code, String::new()),
            Err(message) => exit_frame(-1, message),
        };
        let _ = out_tx.blocking_send(Ok(frame));
    });
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
