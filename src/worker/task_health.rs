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
        return;
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    {
        let mut guard = manager.lock().expect("task manager");
        guard.register_health_monitor(
            allocation.id,
            HealthMonitorHandle {
                shutdown: shutdown_tx.clone(),
            },
        );
    }

    tokio::spawn(async move {
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
        checks.push(CheckRuntime {
            check,
            name,
            boot_grace_time,
        });
    }

    let boot_ready = Arc::new(AtomicBool::new(!has_boot_checks));
    let mut boot_status: HashMap<String, bool> = checks
        .iter()
        .filter(|runtime| matches!(runtime.check.check_type, TaskCheckType::Boot))
        .map(|runtime| (runtime.name.clone(), false))
        .collect();

    let (result_tx, mut result_rx) = mpsc::channel::<CheckResult>(32);

    for runtime in checks.clone() {
        let mut shutdown_rx = shutdown_rx.clone();
        let boot_ready = Arc::clone(&boot_ready);
        let result_tx = result_tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(runtime.check.every);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if matches!(runtime.check.check_type, TaskCheckType::Health)
                            && !boot_ready.load(Ordering::SeqCst) {
                            continue;
                        }
                        let result = run_check(&runtime).await;
                        if result_tx.send(result).await.is_err() {
                            break;
                        }
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
    let mut restart_history = Vec::new();

    loop {
        tokio::select! {
            result = result_rx.recv() => {
                let Some(result) = result else { break; };
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
                        if let Err(err) = handle_unhealthy(
                            &manager,
                            &allocation,
                            &task,
                            &mut restart_history,
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
    restart_history: &mut Vec<Instant>,
    state: &mut HashMap<String, CheckState>,
    boot_status: &mut HashMap<String, bool>,
    boot_ready: &Arc<AtomicBool>,
    start_time: &mut Instant,
    reason: String,
) -> R<()> {
    if task.on_unhealthy.restart_count > 0
        && can_restart(
            restart_history,
            task.on_unhealthy.restart_period,
            task.on_unhealthy.restart_count,
        )
    {
        restart_history.push(Instant::now());
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

    manager
        .lock()
        .expect("task manager")
        .record_failure(allocation.id, format!("health check failed: {reason}"));
    Ok(())
}

fn can_restart(history: &mut Vec<Instant>, window: Duration, max: u32) -> bool {
    let now = Instant::now();
    history.retain(|time| now.duration_since(*time) <= window);
    history.len() < max as usize
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
        .check
        .port
        .ok_or_else(|| "http check requires a port".to_string())?;
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
        .check
        .port
        .ok_or_else(|| "tcp check requires a port".to_string())?;
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
    let mut cmd = Command::new(command);
    if let Some(args) = runtime.check.args.as_ref() {
        cmd.args(args);
    }
    let status = cmd
        .status()
        .map_err(|err| format!("exec check failed to run: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("exec check failed with status {status}"))
    }
}
