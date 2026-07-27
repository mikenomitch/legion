//! `legion exec` — run a command inside a running allocation.
//!
//! One-shot (`legion exec 42 -- cat /etc/hostname`) or interactive
//! (`legion exec 42 -it -- /bin/sh`). Streams stdin/stdout/stderr over the
//! manager's `ExecAllocation` bidi RPC and exits with the remote exit code.

use clap::{Arg, ArgAction, ArgMatches, Command};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::cmd::helpers::{connect_manager_client, friendly_status};
use crate::prelude::*;
use crate::structs::{
    exec_client_frame, exec_server_frame, ExecClientFrame, ExecInput, ExecResize, ExecStart,
    ExecWinsize, GetLedgerRequest,
};

pub fn config() -> Command {
    Command::new("exec")
        .about("Run a command inside a running allocation")
        .arg(
            Arg::new("allocation")
                .help("Allocation id to exec into (or use --job)")
                .required(false),
        )
        .arg(
            Arg::new("job")
                .long("job")
                .help("Target the newest running allocation of this job")
                .required(false),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .help("Task name within the allocation (for multi-task allocations)")
                .required(false),
        )
        .arg(
            Arg::new("stdin")
                .short('i')
                .long("stdin")
                .help("Forward local stdin to the remote process")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("tty")
                .short('t')
                .long("tty")
                .help("Allocate an interactive pseudo-terminal")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("command")
                .help("Command to run (after --)")
                .num_args(1..)
                .last(true)
                .required(true),
        )
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    let command: Vec<String> = matches
        .get_many::<String>("command")
        .map(|vals| vals.cloned().collect())
        .unwrap_or_default();
    if command.is_empty() {
        bail!("a command is required: legion exec <alloc> -- <cmd> [args...]");
    }
    let tty = matches.get_flag("tty");
    let want_stdin = matches.get_flag("stdin") || tty;
    let task = matches
        .get_one::<String>("task")
        .cloned()
        .unwrap_or_default();

    let mut client = connect_manager_client().await?;

    let allocation_id = resolve_allocation(&mut client, matches).await?;

    let winsize = if tty { current_winsize() } else { None };

    let start = ExecStart {
        allocation_id,
        task_name: task,
        command,
        action: String::new(),
        action_params: Default::default(),
        tty,
        winsize,
        bearer_token: String::new(),
    };

    run_session(&mut client, start, want_stdin, tty).await
}

/// Resolve which allocation to target: an explicit id, or the newest running
/// allocation of `--job`.
async fn resolve_allocation(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    matches: &ArgMatches,
) -> R<i64> {
    if let Some(raw) = matches.get_one::<String>("allocation") {
        return raw
            .parse::<i64>()
            .with_context(|| format!("invalid allocation id `{raw}`"));
    }
    let Some(job) = matches.get_one::<String>("job") else {
        bail!("provide an allocation id or --job <name>");
    };

    let ledger = client
        .get_ledger(GetLedgerRequest {})
        .await
        .map_err(|e| friendly_status("exec", e))?
        .into_inner();

    // Newest (highest id) running allocation for the job.
    let mut best: Option<i64> = None;
    for node in ledger.nodes {
        for alloc in node.allocations {
            if &alloc.job_id == job
                && (alloc.status == "placed"
                    || alloc.status == "active"
                    || alloc.status == "running")
            {
                best = Some(best.map_or(alloc.id, |b| b.max(alloc.id)));
            }
        }
    }
    best.ok_or_else(|| anyhow!("no running allocation found for job `{job}`"))
}

/// Shared entry for `legion action run`: drives the same bidi stream with an
/// action-bearing `ExecStart`.
pub(crate) async fn run_action_session(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    start: ExecStart,
    want_stdin: bool,
    tty: bool,
) -> R<()> {
    run_session(client, start, want_stdin, tty).await
}

/// Drive the bidi exec stream: pump local stdin/resize up, remote output down,
/// and exit with the remote code.
async fn run_session(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    start: ExecStart,
    want_stdin: bool,
    tty: bool,
) -> R<()> {
    let (tx, rx) = mpsc::channel::<ExecClientFrame>(64);

    // First frame: start.
    tx.send(ExecClientFrame {
        frame: Some(exec_client_frame::Frame::Start(start)),
    })
    .await
    .ok();

    // Raw mode for interactive terminals; restored on drop via the guard.
    let _raw_guard = if tty {
        Some(RawModeGuard::enable()?)
    } else {
        None
    };

    if want_stdin {
        spawn_stdin_forwarder(tx.clone());
    } else {
        // One-shot: no stdin, signal EOF immediately.
        tx.send(ExecClientFrame {
            frame: Some(exec_client_frame::Frame::Stdin(ExecInput {
                data: Vec::new(),
                eof: true,
            })),
        })
        .await
        .ok();
    }

    if tty {
        spawn_resize_watcher(tx.clone());
    }

    let mut inbound = client
        .exec_allocation(ReceiverStream::new(rx))
        .await
        .map_err(|e| friendly_status("exec", e))?
        .into_inner();

    let mut exit_code = 0;
    use std::io::Write;
    loop {
        match inbound.message().await {
            Ok(Some(frame)) => match frame.frame {
                Some(exec_server_frame::Frame::Stdout(out)) => {
                    let mut stdout = std::io::stdout();
                    stdout.write_all(&out.data).ok();
                    stdout.flush().ok();
                }
                Some(exec_server_frame::Frame::Stderr(out)) => {
                    let mut stderr = std::io::stderr();
                    stderr.write_all(&out.data).ok();
                    stderr.flush().ok();
                }
                Some(exec_server_frame::Frame::Exit(exit)) => {
                    exit_code = exit.code;
                    if !exit.message.is_empty() {
                        eprintln!("\r\nexec ended: {}", exit.message);
                    }
                    break;
                }
                Some(exec_server_frame::Frame::Started(_))
                | Some(exec_server_frame::Frame::Pong(_))
                | None => {}
            },
            Ok(None) => break,
            Err(status) => {
                drop(_raw_guard);
                return Err(friendly_status("exec", status));
            }
        }
    }

    drop(_raw_guard);
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

/// Read local stdin on a blocking thread and forward it as stdin frames.
fn spawn_stdin_forwarder(tx: mpsc::Sender<ExecClientFrame>) {
    std::thread::spawn(move || {
        use std::io::Read;
        let mut stdin = std::io::stdin();
        let mut buf = [0u8; 4096];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    let _ = tx.blocking_send(ExecClientFrame {
                        frame: Some(exec_client_frame::Frame::Stdin(ExecInput {
                            data: Vec::new(),
                            eof: true,
                        })),
                    });
                    break;
                }
                Ok(n) => {
                    if tx
                        .blocking_send(ExecClientFrame {
                            frame: Some(exec_client_frame::Frame::Stdin(ExecInput {
                                data: buf[..n].to_vec(),
                                eof: false,
                            })),
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

/// Poll the local terminal size and send resize frames when it changes.
fn spawn_resize_watcher(tx: mpsc::Sender<ExecClientFrame>) {
    tokio::spawn(async move {
        let mut last = crossterm::terminal::size().unwrap_or((80, 24));
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            let Ok(size) = crossterm::terminal::size() else {
                continue;
            };
            if size != last {
                last = size;
                if tx
                    .send(ExecClientFrame {
                        frame: Some(exec_client_frame::Frame::Resize(ExecResize {
                            winsize: Some(ExecWinsize {
                                cols: size.0 as u32,
                                rows: size.1 as u32,
                            }),
                        })),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
}

fn current_winsize() -> Option<ExecWinsize> {
    crossterm::terminal::size()
        .ok()
        .map(|(cols, rows)| ExecWinsize {
            cols: cols as u32,
            rows: rows as u32,
        })
}

/// Restores cooked terminal mode when dropped.
struct RawModeGuard;

impl RawModeGuard {
    fn enable() -> R<Self> {
        crossterm::terminal::enable_raw_mode().context("failed to enable raw terminal mode")?;
        Ok(RawModeGuard)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}
