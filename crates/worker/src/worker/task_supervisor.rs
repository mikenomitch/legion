//! Task process supervision: notice when a task's process exits, reap it, and
//! apply the task's `restart` policy.
//!
//! Before this loop existed the worker had no way to learn that a task had
//! died: health checks were the only signal, so a task without checks was
//! reported healthy forever and a batch task's success was assumed at
//! placement. See `internal_docs/plans/host-process-supervision-plan.md`.
//!
//! Failures leave through the existing `record_failure` path — the manager
//! already knows how to reschedule those. Terminal outcomes (a success, or
//! any exit of a `restart.policy = "never"` task) leave through
//! `record_termination`, which the heartbeat reports separately.

use crate::prelude::*;
use crate::structs::{RestartPolicy, TaskConfig};
use crate::worker::task_manager::{Reap, RestartFinish, TaskExit, WorkerTaskManager};
use std::time::Duration;

/// How often the supervisor polls tracked task processes. A second is well
/// under any restart delay and costs a `waitpid` per allocation.
pub const SUPERVISOR_TICK: Duration = Duration::from_secs(1);

/// What the supervisor decided to do about one observed exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitDecision {
    /// Relaunch the task in place after the task's restart delay.
    Restart,
    /// Terminal, and the task said this exit status is a success.
    TerminalSuccess,
    /// Terminal, and the exit was not a success, but the task's policy is
    /// `never`: it asked to run once, so the allocation completes as failed
    /// (`TerminatedAllocation.success = false`) instead of being moved.
    TerminalFailure,
    /// Failure: hand it to the manager's reschedule/move machinery.
    Fail,
}

/// Decide what an exit means for a task. Pure so the policy matrix is
/// testable without processes, clocks, or a manager.
///
/// `budget_available` is the shared local restart budget
/// (`on_unhealthy.restart_count` within `restart_period`), which health-check
/// restarts spend from too.
pub fn decide(task: &TaskConfig, exit: &TaskExit, budget_available: bool) -> ExitDecision {
    let success = task.exit_code_is_success(exit.exit_code);
    let policy = task.restart_policy();
    let wants_restart = match policy {
        RestartPolicy::Always => true,
        RestartPolicy::OnFailure => !success,
        RestartPolicy::Never => false,
    };

    if wants_restart && budget_available {
        return ExitDecision::Restart;
    }
    // A restart the budget refuses is a failure even when the exit itself was
    // clean: the task asked to keep running and we are out of attempts.
    if wants_restart {
        return ExitDecision::Fail;
    }
    if success {
        ExitDecision::TerminalSuccess
    } else if policy == RestartPolicy::Never {
        // `never` means run once: an unsuccessful exit ends the allocation
        // as failed rather than moving it to run again elsewhere.
        ExitDecision::TerminalFailure
    } else {
        ExitDecision::Fail
    }
}

/// Poll for task exits forever, acting on each one, and reap allocations that
/// are inside their stop grace window.
///
/// The reap lives here rather than on the heartbeat path on purpose: it runs
/// `destroy`, unmounts volumes and removes directories, any of which can block
/// on a hung driver. The heartbeat must not be behind that work — that is the
/// whole reason graceful stop needed a terminating set instead of a blocking
/// `stop()` (see `docs/graceful-stop.md`).
#[instrument(skip_all)]
///
/// Teardowns a stop, a failure, or a self-fence queues under the lock wake
/// the loop at once rather than on the next tick
/// ([`WorkerTaskManager::enable_off_lock_teardown`]), so the node's room
/// comes back as soon as the destroy is done.
pub async fn start_task_supervisor(task_manager: Arc<Mutex<WorkerTaskManager>>) -> R<()> {
    let wake = Arc::new(tokio::sync::Notify::new());
    crate::worker::daemon::lock_manager(&task_manager).enable_off_lock_teardown(wake.clone());
    loop {
        tokio::select! {
            _ = tokio::time::sleep(SUPERVISOR_TICK) => {}
            _ = wake.notified() => {}
        }
        let (exits, reaps) = {
            let mut manager = crate::worker::daemon::lock_manager(&task_manager);
            // Reap first: an allocation that just exited inside its grace
            // window is not a supervised exit to restart, and clearing it
            // here is what releases the node for allocations the reconcile
            // is holding back.
            manager.reap_draining();
            let (_, reaps) = manager.take_reapable(true);
            (manager.poll_task_exits(), reaps)
        };
        reap_unlocked(&task_manager, reaps).await;
        for exit in exits {
            handle_exit(&task_manager, exit).await;
        }
    }
}

/// Destroy stops with the lock released, all at once on the blocking pool,
/// then release them under one lock.
async fn reap_unlocked(task_manager: &Arc<Mutex<WorkerTaskManager>>, reaps: Vec<Reap>) {
    if reaps.is_empty() {
        return;
    }
    let destroys: Vec<_> = reaps
        .into_iter()
        .map(|mut reap| {
            tokio::task::spawn_blocking(move || {
                reap.destroy();
                reap
            })
        })
        .collect();
    let mut destroyed = Vec::with_capacity(destroys.len());
    for destroy in destroys {
        match destroy.await {
            Ok(reap) => destroyed.push(reap),
            // A panicking destroy leaves its id in the reaping set, so it
            // stays stopping rather than being released under a live VMM.
            Err(err) => error!(error = ?err, "Allocation teardown panicked"),
        }
    }
    let mut manager = crate::worker::daemon::lock_manager(task_manager);
    for reap in destroyed {
        manager.finish_reap(reap);
    }
}

/// Apply one exit. Kept separate from the loop so the lock is taken in short,
/// obvious spans — the restart delay in particular must never be slept while
/// holding the task-manager lock, which the heartbeat path also needs.
pub async fn handle_exit(task_manager: &Arc<Mutex<WorkerTaskManager>>, exit: TaskExit) {
    let (task, budget_available) = {
        let mut manager = crate::worker::daemon::lock_manager(task_manager);
        let Some(task) = manager.task_config(exit.allocation_id).cloned() else {
            // Already torn down (stopped, or failed by another path) — the
            // exit is expected and there is nothing left to decide.
            return;
        };
        let budget_available = manager.can_restart_allocation(
            exit.allocation_id,
            task.on_unhealthy.restart_count,
            task.on_unhealthy.restart_period,
        );
        (task, budget_available)
    };

    match decide(&task, &exit, budget_available) {
        ExitDecision::Restart => {
            let delay = task.restart_delay();
            info!(
                allocation_id = exit.allocation_id,
                task_name = %exit.task_name,
                status = %exit.describe(),
                delay_secs = delay.as_secs(),
                "Task process exited; restarting"
            );
            {
                let mut manager = crate::worker::daemon::lock_manager(task_manager);
                manager.record_restart_attempt(exit.allocation_id);
                manager.set_allocation_health(
                    exit.allocation_id,
                    false,
                    format!("{}; restarting", exit.describe_with_output()),
                );
            }
            tokio::time::sleep(delay).await;
            let begun = {
                let mut manager = crate::worker::daemon::lock_manager(task_manager);
                // The allocation can have been stopped while we slept.
                if !manager.is_allocation_active(exit.allocation_id) {
                    return;
                }
                manager.begin_restart(exit.allocation_id)
            };
            // The runtime's `stop` and `start` run with the lock released:
            // a container or a VMM restart holds up no other start, stop, or
            // heartbeat on the node.
            let result = match begun {
                Err(err) => Err(err),
                Ok(mut restart) => {
                    let ran = tokio::task::spawn_blocking(move || {
                        let result = restart.run();
                        (restart, result)
                    })
                    .await;
                    let Ok((restart, result)) = ran else {
                        error!(
                            allocation_id = exit.allocation_id,
                            "Restart panicked; the allocation stays starting until it is stopped"
                        );
                        return;
                    };
                    let finish = crate::worker::daemon::lock_manager(task_manager)
                        .finish_restart(restart, result);
                    match finish {
                        RestartFinish::Started => Ok(()),
                        RestartFinish::Failed(err) => Err(err),
                        RestartFinish::Cancelled(mut restart) => {
                            let _ = tokio::task::spawn_blocking(move || {
                                restart.destroy();
                                restart
                            })
                            .await
                            .map(|restart| {
                                crate::worker::daemon::lock_manager(task_manager)
                                    .finish_cancelled_launch(
                                        restart.allocation_id(),
                                        restart.resource_assignment(),
                                    );
                            });
                            return;
                        }
                    }
                }
            };
            if let Err(err) = result {
                warn!(
                    allocation_id = exit.allocation_id,
                    task_name = %exit.task_name,
                    error = ?err,
                    "Failed to restart exited task"
                );
                let mut manager = crate::worker::daemon::lock_manager(task_manager);
                manager.record_failure(
                    exit.allocation_id,
                    format!("{}; restart failed: {err}", exit.describe_with_output()),
                );
            }
        }
        ExitDecision::TerminalSuccess => {
            info!(
                allocation_id = exit.allocation_id,
                task_name = %exit.task_name,
                status = %exit.describe(),
                "Task completed"
            );
            let mut manager = crate::worker::daemon::lock_manager(task_manager);
            manager.record_termination(exit.allocation_id, &exit, true);
        }
        ExitDecision::TerminalFailure => {
            warn!(
                allocation_id = exit.allocation_id,
                task_name = %exit.task_name,
                status = %exit.describe(),
                "Task failed; restart policy `never`, so it is not moved"
            );
            let mut manager = crate::worker::daemon::lock_manager(task_manager);
            manager.record_termination(exit.allocation_id, &exit, false);
        }
        ExitDecision::Fail => {
            warn!(
                allocation_id = exit.allocation_id,
                task_name = %exit.task_name,
                status = %exit.describe(),
                "Task failed"
            );
            let mut manager = crate::worker::daemon::lock_manager(task_manager);
            // The reason the user reads (`allocation.failed`), so it carries
            // the task's own last words; the log line above does not.
            manager.record_failure(exit.allocation_id, exit.describe_with_output());
        }
    }
}

#[cfg(test)]
mod task_supervisor_test {
    use super::*;
    use crate::structs::{TaskLifecycle, TaskRestartConfig};

    fn task(lifecycle: TaskLifecycle, restart: Option<TaskRestartConfig>) -> TaskConfig {
        let mut task = TaskConfig::new("api", "exec");
        task.lifecycle = lifecycle;
        task.restart = restart;
        task
    }

    fn exit(code: Option<i32>) -> TaskExit {
        TaskExit {
            allocation_id: 1,
            task_name: "api".into(),
            exit_code: code,
            signal: None,
            oom_killed_at_bytes: None,
            stderr_tail: None,
        }
    }

    #[test]
    fn service_restarts_on_any_exit_by_default() {
        let task = task(TaskLifecycle::Service, None);
        assert_eq!(decide(&task, &exit(Some(0)), true), ExitDecision::Restart);
        assert_eq!(decide(&task, &exit(Some(1)), true), ExitDecision::Restart);
    }

    #[test]
    fn batch_only_restarts_on_failure_by_default() {
        let task = task(TaskLifecycle::Batch, None);
        assert_eq!(
            decide(&task, &exit(Some(0)), true),
            ExitDecision::TerminalSuccess
        );
        assert_eq!(decide(&task, &exit(Some(1)), true), ExitDecision::Restart);
    }

    #[test]
    fn exhausted_budget_turns_a_wanted_restart_into_a_failure() {
        let task = task(TaskLifecycle::Service, None);
        assert_eq!(decide(&task, &exit(Some(0)), false), ExitDecision::Fail);
        assert_eq!(decide(&task, &exit(Some(1)), false), ExitDecision::Fail);
    }

    #[test]
    fn policy_never_is_terminal_either_way() {
        let task = task(
            TaskLifecycle::Batch,
            Some(TaskRestartConfig {
                policy: Some(RestartPolicy::Never),
                ..Default::default()
            }),
        );
        assert_eq!(
            decide(&task, &exit(Some(0)), true),
            ExitDecision::TerminalSuccess
        );
        assert_eq!(
            decide(&task, &exit(Some(1)), true),
            ExitDecision::TerminalFailure
        );
        // The budget is irrelevant: `never` asks for no restart to spend it on.
        assert_eq!(
            decide(&task, &exit(Some(1)), false),
            ExitDecision::TerminalFailure
        );
    }

    #[test]
    fn on_failure_still_moves_a_failure_once_the_budget_is_spent() {
        let task = task(TaskLifecycle::Batch, None);
        assert_eq!(decide(&task, &exit(Some(1)), false), ExitDecision::Fail);
    }

    #[test]
    fn success_exit_codes_widen_what_counts_as_done() {
        let task = task(
            TaskLifecycle::Batch,
            Some(TaskRestartConfig {
                policy: Some(RestartPolicy::OnFailure),
                success_exit_codes: vec![0, 2],
                ..Default::default()
            }),
        );
        assert_eq!(
            decide(&task, &exit(Some(2)), true),
            ExitDecision::TerminalSuccess
        );
        assert_eq!(decide(&task, &exit(Some(3)), true), ExitDecision::Restart);
    }

    #[test]
    fn a_signal_death_is_never_a_success() {
        let mut killed = exit(None);
        killed.signal = Some(9);
        let task = task(
            TaskLifecycle::Batch,
            Some(TaskRestartConfig {
                policy: Some(RestartPolicy::Never),
                ..Default::default()
            }),
        );
        assert_eq!(decide(&task, &killed, true), ExitDecision::TerminalFailure);
        assert_eq!(killed.describe(), "killed by signal 9");
    }

    #[test]
    fn restart_delay_falls_back_to_the_shared_on_unhealthy_delay() {
        let mut task = task(TaskLifecycle::Service, None);
        task.on_unhealthy.restart_delay = Duration::from_secs(7);
        assert_eq!(task.restart_delay(), Duration::from_secs(7));

        task.restart = Some(TaskRestartConfig {
            delay: Some(Duration::from_secs(2)),
            ..Default::default()
        });
        assert_eq!(task.restart_delay(), Duration::from_secs(2));
    }
}
