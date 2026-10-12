//! `legion exec` — run a command inside a running allocation.
//!
//! One-shot (`legion exec 42 -- cat /etc/hostname`) or interactive
//! (`legion exec 42 -it -- /bin/sh`). Streams stdin/stdout/stderr over the
//! manager's `ExecAllocation` bidi RPC and exits with the remote exit code.
//!
//! With `--direct` (docs/exec-and-actions.md, "Direct exec") it mints an
//! exec ticket from the manager instead, and runs the session against the
//! worker's own `ExecTask`, verifying the worker by the cluster CA and its
//! node name.
//!
//! With `--json` a one-shot exec collects the output instead of streaming it
//! and prints one envelope: `{ok:true,data:{allocationId, exitCode, stdout,
//! stderr, message}}`. The process still exits with the remote exit code, so
//! `&&` chains behave the same in both formats.

use clap::{Arg, ArgAction, ArgMatches, Command};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::cmd::helpers::{connect_manager_client, json_flag};
use crate::cmd::output::{self, CliError, OutputFormat};
use crate::prelude::*;
use crate::structs::worker_client::WorkerClient;
use crate::structs::{
    exec_client_frame, exec_server_frame, ExecClientFrame, ExecInput, ExecResize, ExecStart,
    ExecWinsize, GetLedgerRequest, MintExecTicketRequest,
};
use tonic::transport::Channel;

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
            Arg::new("instance")
                .long("instance")
                .value_name("ID")
                .conflicts_with_all(["allocation", "job"])
                .help("Target an instance: its current allocation, across reschedules and resumes")
                .required(false),
        )
        .arg(
            Arg::new("task")
                .long("task")
                .help("Task name within the allocation (for multi-task allocations)")
                .required(false),
        )
        .arg(
            Arg::new("direct")
                .long("direct")
                .help(
                    "Mint an exec ticket and run the session against the worker directly, \
                     with no manager on the data path (the worker must set exec.direct)",
                )
                .action(ArgAction::SetTrue),
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
        .arg(json_flag().help(
            "Collect the output and print one JSON envelope with the exit code (one-shot \
             only; not with --tty or --stdin)",
        ))
}

/// `--json` is a one-shot capture; an interactive session has no single
/// result to report.
pub(crate) fn reject_interactive_json(
    format: OutputFormat,
    tty: bool,
    stdin: bool,
) -> Result<(), anyhow::Error> {
    if format.is_json() && (tty || stdin) {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                "--json collects the output of a one-shot exec; it cannot be combined with \
                 --tty or --stdin",
                Some("drop --json for an interactive session"),
            ),
        ));
    }
    Ok(())
}

pub async fn handle(matches: &ArgMatches) -> R<()> {
    let format = OutputFormat::from_matches(matches);
    let command: Vec<String> = matches
        .get_many::<String>("command")
        .map(|vals| vals.cloned().collect())
        .unwrap_or_default();
    if command.is_empty() {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                "a command is required: legion exec <alloc> -- <cmd> [args...]",
                None,
            ),
        ));
    }
    let tty = matches.get_flag("tty");
    reject_interactive_json(format, tty, matches.get_flag("stdin"))?;
    let want_stdin = matches.get_flag("stdin") || tty;
    let task = matches
        .get_one::<String>("task")
        .cloned()
        .unwrap_or_default();

    let mut client = connect_manager_client().await?;

    let instance_id = matches
        .get_one::<String>("instance")
        .cloned()
        .unwrap_or_default();
    let allocation_id = if instance_id.is_empty() {
        resolve_allocation(&mut client, matches, format).await?
    } else {
        0
    };

    let winsize = if tty { current_winsize() } else { None };

    if matches.get_flag("direct") {
        let reply = crate::cmd::exec_ticket::mint(
            &mut client,
            MintExecTicketRequest {
                allocation_id,
                instance_id,
                task_name: task.clone(),
                command: command.clone(),
                tty,
                ..Default::default()
            },
        )
        .await;
        let reply = output::resolve(format, "exec", reply)?;
        let worker = crate::cmd::exec_ticket::worker_client(&reply).await?;
        let start = ExecStart {
            allocation_id: reply.allocation_id,
            task_name: task,
            command,
            tty,
            winsize,
            exec_ticket: reply.ticket,
            ..Default::default()
        };
        return run_session(ExecTarget::Worker(worker), start, want_stdin, tty, format).await;
    }

    let start = ExecStart {
        allocation_id,
        task_name: task,
        command,
        action: String::new(),
        action_params: Default::default(),
        tty,
        winsize,
        bearer_token: String::new(),
        instance_id,
        exec_ticket: String::new(),
    };

    run_session(
        ExecTarget::Manager(&mut client),
        start,
        want_stdin,
        tty,
        format,
    )
    .await
}

/// Where a session's stream goes: the manager's proxy, or a worker
/// directly with an exec ticket.
pub(crate) enum ExecTarget<'a> {
    Manager(&'a mut crate::cmd::helpers::AuthedManagerClient),
    Worker(WorkerClient<Channel>),
}

/// Resolve which allocation to target: an explicit id, or the newest running
/// allocation of `--job`.
async fn resolve_allocation(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    matches: &ArgMatches,
    format: OutputFormat,
) -> R<i64> {
    if let Some(raw) = matches.get_one::<String>("allocation") {
        return raw.parse::<i64>().map_err(|_| {
            output::fail_local(
                format,
                CliError::local(
                    "InvalidArgument",
                    format!("invalid allocation id `{raw}`"),
                    None,
                ),
            )
        });
    }
    let Some(job) = matches.get_one::<String>("job") else {
        return Err(output::fail_local(
            format,
            CliError::local(
                "InvalidArgument",
                "provide an allocation id or --job <name>",
                None,
            ),
        ));
    };
    newest_running_allocation(client, job, format).await
}

/// The newest (highest id) running allocation of `job`. Ids are never
/// reused, so the highest is the most recently placed.
pub(crate) async fn newest_running_allocation(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    job: &str,
    format: OutputFormat,
) -> R<i64> {
    let ledger =
        output::resolve(format, "exec", client.get_ledger(GetLedgerRequest {}).await)?.into_inner();

    // Newest (highest id) running allocation for the job.
    let mut best: Option<i64> = None;
    for node in ledger.nodes {
        for alloc in node.allocations {
            if alloc.job_id == job
                && (alloc.status == "placed"
                    || alloc.status == "active"
                    || alloc.status == "running")
            {
                best = Some(best.map_or(alloc.id, |b| b.max(alloc.id)));
            }
        }
    }
    best.ok_or_else(|| {
        output::fail_local(
            format,
            CliError::local(
                "NotFound",
                format!("no running allocation found for job `{job}`"),
                Some("pass an allocation id explicitly; `legion job info <job>` lists them"),
            ),
        )
    })
}

/// Shared entry for `legion action run`: drives the same bidi stream with an
/// action-bearing `ExecStart`.
pub(crate) async fn run_action_session(
    client: &mut crate::cmd::helpers::AuthedManagerClient,
    start: ExecStart,
    want_stdin: bool,
    tty: bool,
    format: OutputFormat,
) -> R<()> {
    run_session(ExecTarget::Manager(client), start, want_stdin, tty, format).await
}

/// Drive the bidi exec stream: pump local stdin/resize up, remote output down,
/// and exit with the remote code.
async fn run_session(
    target: ExecTarget<'_>,
    start: ExecStart,
    want_stdin: bool,
    tty: bool,
    format: OutputFormat,
) -> R<()> {
    let allocation_id = start.allocation_id;
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

    // The worker client lives as long as the session: dropping the last
    // handle to its channel cancels the stream.
    let mut worker_client = None;
    let opened = match target {
        ExecTarget::Manager(client) => client.exec_allocation(ReceiverStream::new(rx)).await,
        ExecTarget::Worker(worker) => {
            let worker = worker_client.insert(worker);
            worker.exec_task(ReceiverStream::new(rx)).await
        }
    };
    let mut inbound = match opened {
        Ok(response) => response.into_inner(),
        Err(status) => {
            drop(_raw_guard);
            return output::resolve(format, "exec", Err(status));
        }
    };

    let mut exit_code = 0;
    // `--json` collects instead of streaming.
    let mut captured_stdout: Vec<u8> = Vec::new();
    let mut captured_stderr: Vec<u8> = Vec::new();
    let mut exit_message = String::new();
    use std::io::Write;
    loop {
        match inbound.message().await {
            Ok(Some(frame)) => match frame.frame {
                Some(exec_server_frame::Frame::Stdout(out)) if format.is_json() => {
                    captured_stdout.extend_from_slice(&out.data);
                }
                Some(exec_server_frame::Frame::Stderr(out)) if format.is_json() => {
                    captured_stderr.extend_from_slice(&out.data);
                }
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
                    if !exit.message.is_empty() && !format.is_json() {
                        eprintln!("\r\nexec ended: {}", exit.message);
                    }
                    exit_message = exit.message;
                    break;
                }
                Some(exec_server_frame::Frame::Started(_))
                | Some(exec_server_frame::Frame::Pong(_))
                | None => {}
            },
            Ok(None) => break,
            Err(status) => {
                drop(_raw_guard);
                return output::resolve(format, "exec", Err(status));
            }
        }
    }

    drop(_raw_guard);
    if format.is_json() {
        output::print_success(&exec_result(
            allocation_id,
            exit_code,
            &captured_stdout,
            &captured_stderr,
            &exit_message,
        ));
    }
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}

/// The `--json` result of a one-shot exec. Output is decoded as UTF-8 with
/// invalid bytes replaced; use the streaming form for binary output.
fn exec_result(
    allocation_id: i64,
    exit_code: i32,
    stdout: &[u8],
    stderr: &[u8],
    message: &str,
) -> Value {
    json!({
        "allocationId": allocation_id,
        "exitCode": exit_code,
        "stdout": String::from_utf8_lossy(stdout),
        "stderr": String::from_utf8_lossy(stderr),
        "message": message,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_exec_result_is_camel_case_with_the_exit_code() {
        let result = exec_result(42, 3, b"out\n", b"err\n", "");
        assert_eq!(result["allocationId"], 42);
        assert_eq!(result["exitCode"], 3);
        assert_eq!(result["stdout"], "out\n");
        assert_eq!(result["stderr"], "err\n");
    }

    #[test]
    fn json_is_one_shot_only() {
        assert!(reject_interactive_json(OutputFormat::Human, true, true).is_ok());
        assert!(reject_interactive_json(OutputFormat::Json, false, false).is_ok());
        let matches = config()
            .try_get_matches_from(["exec", "7", "--json", "--", "true"])
            .expect("--json parses");
        assert!(matches.get_flag("json"));
    }
}
