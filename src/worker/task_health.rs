// handle_unhealthy threads the full task/health context by parameter.
#![allow(clippy::too_many_arguments)]

use crate::prelude::*;
use crate::structs::{Allocation, TaskCheck, TaskCheckKind, TaskCheckType, TaskConfig};
use crate::worker::task_manager::WorkerTaskManager;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

/// The first startup probe's delay: how soon after a (re)start a check that
/// has not passed yet is tried again, ahead of its `every` cadence.
const STARTUP_PROBE_FIRST: Duration = Duration::from_millis(100);
/// The longest gap between startup probes. The delay doubles from
/// [`STARTUP_PROBE_FIRST`] up to this (or to the check's `every`, if that is
/// shorter).
const STARTUP_PROBE_MAX: Duration = Duration::from_secs(1);
/// A startup probe's own timeout, so a probe never holds up the check's
/// `every` cadence for long. A probe that times out is simply not a pass;
/// the cadence check still runs with the check's full `timeout`.
const STARTUP_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Whether a check is probed at startup (docs/jobs-tasks-and-scheduling.md,
/// "Startup probing").
///
/// Only network checks are: a TCP connect or an HTTP GET is cheap and
/// side-effect free, so trying it a few extra times while the workload boots
/// costs nothing. An `exec` check runs an arbitrary host command whose cost
/// Legion cannot know, so it keeps its configured cadence. A readiness check
/// never changes the allocation's health, so passing it early gains nothing.
fn probes_at_startup(check: &TaskCheck) -> bool {
    matches!(check.kind, TaskCheckKind::Http | TaskCheckKind::Tcp)
        && !matches!(check.check_type, TaskCheckType::Readiness)
}

/// The delay before the next startup probe, after one at `current` failed.
fn next_probe_delay(current: Duration, every: Duration) -> Duration {
    (current * 2).min(STARTUP_PROBE_MAX).min(every)
}

#[derive(Debug)]
pub struct HealthMonitorHandle {
    shutdown: watch::Sender<bool>,
}

impl HealthMonitorHandle {
    pub fn shutdown(self) {
        let _ = self.shutdown.send(true);
    }
}

#[derive(Debug, Clone)]
struct CheckRuntime {
    check: TaskCheck,
    name: String,
    boot_grace_time: Duration,
    /// Concrete port for http/tcp checks, with label references already
    /// resolved against the allocation's assigned ports.
    resolved_port: Option<u16>,
}

#[derive(Debug)]
struct CheckState {
    runtime: CheckRuntime,
    failures: Vec<Instant>,
}

#[derive(Debug)]
struct CheckResult {
    name: String,
    check_type: TaskCheckType,
    ok: bool,
    error: Option<String>,
}

pub fn spawn_health_monitor(
    manager: Arc<Mutex<WorkerTaskManager>>,
    allocation: Allocation,
    task: TaskConfig,
) {
    if task.checks.is_empty() {
        let mut guard = manager.lock().expect("task manager");
        guard.set_allocation_health(allocation.id, true, "healthy (no checks configured)");
        return;
    }

    // The monitor is a tokio task. Every caller is expected to run inside the
    // worker's runtime (directly, or on a thread that entered it — see
    // `task_manager::enter_runtime`). A caller that does not would make
    // `tokio::spawn` panic and take its thread down silently, leaving the
    // allocation at "health checks starting" forever; say so instead.
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        error!(
            allocation_id = allocation.id,
            task_name = %task.name,
            "Cannot start health checks: no async runtime on this thread"
        );
        manager.lock().expect("task manager").set_allocation_health(
            allocation.id,
            false,
            "health checks could not start (worker bug: no async runtime); see the worker log",
        );
        return;
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    {
        let mut guard = manager.lock().expect("task manager");
        guard.register_health_monitor(
            allocation.id,
            HealthMonitorHandle {
                shutdown: shutdown_tx.clone(),
            },
        );
        guard.set_allocation_health(allocation.id, false, "health checks starting");
    }

    runtime.spawn(async move {
        run_health_monitor(manager, allocation, task, shutdown_rx).await;
    });
}

async fn run_health_monitor(
    manager: Arc<Mutex<WorkerTaskManager>>,
    allocation: Allocation,
    task: TaskConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let has_boot_checks = task
        .checks
        .iter()
        .any(|check| matches!(check.check_type, TaskCheckType::Boot));
    let default_boot_grace = if has_boot_checks {
        Duration::from_secs(0)
    } else {
        Duration::from_secs(1)
    };

    let mut checks = Vec::new();
    for check in task.checks.iter().cloned() {
        let name = check.default_name();
        let boot_grace_time = check.boot_grace_time.unwrap_or(default_boot_grace);
        let resolved_port = check
            .port
            .as_ref()
            .and_then(|port| port.resolve(allocation.network.as_ref()));
        checks.push(CheckRuntime {
            check,
            name,
            boot_grace_time,
            resolved_port,
        });
    }

    let boot_ready = Arc::new(AtomicBool::new(!has_boot_checks));
    let mut boot_status: HashMap<String, bool> = checks
        .iter()
        .filter(|runtime| matches!(runtime.check.check_type, TaskCheckType::Boot))
        .map(|runtime| (runtime.name.clone(), false))
        .collect();

    let (result_tx, mut result_rx) = mpsc::channel::<CheckResult>(32);
    // Bumped each time the task (re)starts under this monitor, so every check
    // goes back to startup probing (see `probes_at_startup`).
    let (restarts_tx, restarts_rx) = watch::channel(0u64);

    for runtime in checks.clone() {
        let mut shutdown_rx = shutdown_rx.clone();
        let mut restarts_rx = restarts_rx.clone();
        let boot_ready = Arc::clone(&boot_ready);
        let result_tx = result_tx.clone();
        tokio::spawn(async move {
            let probes = probes_at_startup(&runtime.check);
            let first_probe = STARTUP_PROBE_FIRST.min(runtime.check.every);
            let probe_runtime = {
                let mut probe = runtime.clone();
                probe.check.timeout = probe.check.timeout.min(STARTUP_PROBE_TIMEOUT);
                probe
            };
            // Until the check first passes after a (re)start, it is also
            // tried between its `every` ticks, on a short backoff. Only a
            // pass is reported: a failed probe is not counted, so failures
            // are still counted on the configured cadence exactly as before.
            let mut probe = probes.then_some(first_probe);
            let mut interval = tokio::time::interval(runtime.check.every);
            loop {
                let probe_due = async {
                    match probe {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    _ = interval.tick() => {
                        if matches!(runtime.check.check_type, TaskCheckType::Health)
                            && !boot_ready.load(Ordering::SeqCst) {
                            continue;
                        }
                        let result = run_check(&runtime).await;
                        if result.ok {
                            probe = None;
                        }
                        if result_tx.send(result).await.is_err() {
                            break;
                        }
                    }
                    _ = probe_due => {
                        if matches!(runtime.check.check_type, TaskCheckType::Health)
                            && !boot_ready.load(Ordering::SeqCst) {
                            continue;
                        }
                        let result = run_check(&probe_runtime).await;
                        if result.ok {
                            probe = None;
                            if result_tx.send(result).await.is_err() {
                                break;
                            }
                        } else {
                            probe = probe.map(|delay| next_probe_delay(delay, runtime.check.every));
                        }
                    }
                    changed = restarts_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        probe = probes.then_some(first_probe);
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
    }

    let mut start_time = Instant::now();
    // The first time this (re)start turned healthy has been observed as the
    // `health` start stage.
    let mut health_observed = false;
    let mut state: HashMap<String, CheckState> = checks
        .into_iter()
        .map(|runtime| {
            (
                runtime.name.clone(),
                CheckState {
                    runtime,
                    failures: Vec::new(),
                },
            )
        })
        .collect();

    // Set while the task's process is down (exited, awaiting the
    // supervisor's relaunch or terminal for good).
    let mut process_was_down = false;

    loop {
        tokio::select! {
            result = result_rx.recv() => {
                let Some(result) = result else { break; };
                // A check against a process that has exited can only fail,
                // and the exit is the supervisor's to act on: it restarts the
                // task under its `restart` policy, or fails the allocation
                // with the exit status and the task's last stderr lines. If
                // these failures counted too, the allocation would fail as
                // `tcp check failed: Connection refused` — the symptom, with
                // the cause (a refused image pull, a missing input) hidden in
                // the task's logs.
                let process_down = manager
                    .lock()
                    .expect("task manager")
                    .process_exit(allocation.id)
                    .is_some();
                if process_down {
                    if !process_was_down {
                        // Re-arm startup probing now, so the relaunched
                        // process is seen passing as soon as it does, not
                        // at the next `every` tick. Probes fail (uncounted)
                        // until the supervisor relaunches it.
                        restarts_tx.send_modify(|generation| *generation += 1);
                    }
                    process_was_down = true;
                    continue;
                }
                if process_was_down {
                    // Relaunched by the supervisor: the same fresh start an
                    // unhealthy restart gets (boot grace, boot checks again).
                    process_was_down = false;
                    for check_state in state.values_mut() {
                        check_state.failures.clear();
                    }
                    for value in boot_status.values_mut() {
                        *value = false;
                    }
                    if !boot_status.is_empty() {
                        boot_ready.store(false, Ordering::SeqCst);
                    }
                    start_time = Instant::now();
                    // Not timed: this result is the first sign of the
                    // relaunch, so there is no start to measure from.
                    health_observed = true;
                }
                let Some(check_state) = state.get_mut(&result.name) else {
                    continue;
                };

                if matches!(result.check_type, TaskCheckType::Readiness) {
                    if !result.ok {
                        warn!(
                            allocation_id = allocation.id,
                            check = %result.name,
                            error = ?result.error,
                            "Readiness check failed"
                        );
                    }
                    continue;
                }

                {
                    let mut guard = manager.lock().expect("task manager");
                    let status = if result.ok {
                        format!("{} passed", result.name)
                    } else {
                        let reason = result
                            .error
                            .clone()
                            .unwrap_or_else(|| "unknown error".to_string());
                        format!("{} failed: {}", result.name, reason)
                    };
                    guard.set_allocation_health(allocation.id, result.ok, status);
                }
                if result.ok && !health_observed {
                    health_observed = true;
                    crate::worker::start_timing::observe(
                        &task.runtime,
                        crate::worker::start_timing::stage::HEALTH,
                        start_time.elapsed(),
                    );
                }

                if matches!(result.check_type, TaskCheckType::Boot) && result.ok {
                    if let Some(entry) = boot_status.get_mut(&result.name) {
                        *entry = true;
                    }
                    if boot_status.values().all(|value| *value) {
                        boot_ready.store(true, Ordering::SeqCst);
                    }
                }

                let now = Instant::now();
                if !result.ok {
                    if matches!(result.check_type, TaskCheckType::Health)
                        && now.duration_since(start_time) < check_state.runtime.boot_grace_time {
                        continue;
                    }

                    track_failure(&mut check_state.failures, now, &check_state.runtime);
                    if check_state.failures.len() as u32 >= check_state.runtime.check.unhealthy_after_count {
                        let reason = result.error.clone().unwrap_or_else(|| "unknown error".into());
                        let before = start_time;
                        if let Err(err) = handle_unhealthy(
                            &manager,
                            &allocation,
                            &task,
                            &mut state,
                            &mut boot_status,
                            &boot_ready,
                            &mut start_time,
                            reason,
                        ).await {
                            warn!(
                                allocation_id = allocation.id,
                                error = ?err,
                                "Failed handling unhealthy allocation"
                            );
                        }
                        if start_time != before {
                            // Restarted in place: a fresh start, probed again.
                            health_observed = false;
                            restarts_tx.send_modify(|generation| *generation += 1);
                        }
                        if manager
                            .lock()
                            .expect("task manager")
                            .allocations_snapshot()
                            .contains(&allocation.id) {
                            continue;
                        } else {
                            break;
                        }
                    }
                } else {
                    check_state.failures.clear();
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }
}

fn track_failure(failures: &mut Vec<Instant>, now: Instant, runtime: &CheckRuntime) {
    failures.push(now);
    if let Some(span) = runtime.check.unhealthy_after_count_timespan {
        failures.retain(|time| now.duration_since(*time) <= span);
    }
}

async fn handle_unhealthy(
    manager: &Arc<Mutex<WorkerTaskManager>>,
    allocation: &Allocation,
    task: &TaskConfig,
    state: &mut HashMap<String, CheckState>,
    boot_status: &mut HashMap<String, bool>,
    boot_ready: &Arc<AtomicBool>,
    start_time: &mut Instant,
    reason: String,
) -> R<()> {
    // The restart budget lives in the task manager, shared with the exit
    // supervisor: a flapping task gets `restart_count` local restarts in
    // total, not that many per path (see task_supervisor.rs).
    let budget_available = {
        let mut guard = manager.lock().expect("task manager");
        guard.can_restart_allocation(
            allocation.id,
            task.on_unhealthy.restart_count,
            task.on_unhealthy.restart_period,
        )
    };
    if budget_available {
        {
            let mut guard = manager.lock().expect("task manager");
            guard.record_restart_attempt(allocation.id);
        }
        tokio::time::sleep(task.on_unhealthy.restart_delay).await;
        let restart_result = {
            let mut guard = manager.lock().expect("task manager");
            guard.restart_allocation(allocation.id)
        };
        match restart_result {
            Ok(()) => {
                for state in state.values_mut() {
                    state.failures.clear();
                }
                for value in boot_status.values_mut() {
                    *value = false;
                }
                if !boot_status.is_empty() {
                    boot_ready.store(false, Ordering::SeqCst);
                }
                *start_time = Instant::now();
                return Ok(());
            }
            Err(err) => {
                warn!(
                    allocation_id = allocation.id,
                    error = ?err,
                    "Restart failed; reporting allocation unhealthy"
                );
            }
        }
    }

    let mut guard = manager.lock().expect("task manager");
    // What the task itself last said is often the real story behind a check
    // that never passed (an image pull the registry refused, a missing
    // config file), so it rides along in the reason the user reads.
    let reason = match guard.recent_stderr(allocation.id) {
        Some(stderr) => format!("health check failed: {reason}; last stderr: {stderr}"),
        None => format!("health check failed: {reason}"),
    };
    guard.record_failure(allocation.id, reason);
    Ok(())
}

async fn run_check(runtime: &CheckRuntime) -> CheckResult {
    let name = runtime.name.clone();
    let check_type = runtime.check.check_type.clone();
    let outcome = tokio::task::spawn_blocking({
        let runtime = runtime.clone();
        move || match runtime.check.kind {
            TaskCheckKind::Http => run_http_check(&runtime),
            TaskCheckKind::Tcp => run_tcp_check(&runtime),
            TaskCheckKind::Exec => run_exec_check(&runtime),
        }
    })
    .await;

    match outcome {
        Ok(Ok(())) => CheckResult {
            name,
            check_type,
            ok: true,
            error: None,
        },
        Ok(Err(err)) => CheckResult {
            name,
            check_type,
            ok: false,
            error: Some(err),
        },
        Err(err) => CheckResult {
            name,
            check_type,
            ok: false,
            error: Some(format!("check execution failed: {err}")),
        },
    }
}

fn run_http_check(runtime: &CheckRuntime) -> Result<(), String> {
    let port = runtime
        .resolved_port
        .ok_or_else(|| "http check requires a port (or an assigned port label)".to_string())?;
    let path = runtime
        .check
        .path
        .as_deref()
        .ok_or_else(|| "http check requires a path".to_string())?;
    let url = format!("http://127.0.0.1:{port}{path}");
    let client = Client::builder()
        .timeout(runtime.check.timeout)
        .build()
        .map_err(|err| format!("failed to build http client: {err}"))?;
    let response = client
        .get(url)
        .send()
        .map_err(|err| format!("http check request failed: {err}"))?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("http check returned {}", response.status()))
    }
}

fn run_tcp_check(runtime: &CheckRuntime) -> Result<(), String> {
    let port = runtime
        .resolved_port
        .ok_or_else(|| "tcp check requires a port (or an assigned port label)".to_string())?;
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    TcpStream::connect_timeout(&addr, runtime.check.timeout)
        .map(|_| ())
        .map_err(|err| format!("tcp check failed: {err}"))
}

fn run_exec_check(runtime: &CheckRuntime) -> Result<(), String> {
    let command = runtime
        .check
        .command
        .as_ref()
        .ok_or_else(|| "exec check requires a command".to_string())?;
    let (program, args) = command
        .split_first()
        .ok_or_else(|| "exec check requires a command".to_string())?;
    let mut cmd = Command::new(program);
    cmd.args(args);
    // A host command, run as the worker: never with the worker's whole
    // environment (it can hold a `LEGION_TOKEN` or cloud credentials).
    cmd.env_clear();
    cmd.envs(crate::task_plugins::host_command_env());
    let status = cmd
        .status()
        .map_err(|err| format!("exec check failed to run: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("exec check failed with status {status}"))
    }
}
