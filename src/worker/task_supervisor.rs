//! Task process supervision: notice when a task's process exits, reap it, and
//! apply the task's `restart` policy.
//!
//! Before this loop existed the worker had no way to learn that a task had
//! died: health checks were the only signal, so a task without checks was
//! reported healthy forever and a batch task's success was assumed at
//! placement. See `internal_docs/plans/host-process-supervision-plan.md`.
//!
//! Failures leave through the existing `record_failure` path — the manager
//! already knows how to reschedule those. Terminal *successes* leave through
//! `record_termination`, which the heartbeat reports separately.

use crate::prelude::*;
use crate::structs::{RestartPolicy, TaskConfig};
use crate::worker::task_manager::{TaskExit, WorkerTaskManager};
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
    /// Terminal failure: hand it to the manager's reschedule/move machinery.
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
    let wants_restart = match task.restart_policy() {
        RestartPolicy::Always => true,
        RestartPolicy::OnFailure => !success,
        RestartPolicy::Never => false,
    };

    if wants_restart && budget_available {
        return ExitDecision::Restart;
    }
    // A restart the budget refuses is a failure even when the exit itself was
    // clean: the task asked to keep running and we are out of attempts.
    if success && !wants_restart {
        ExitDecision::TerminalSuccess
    } else {
        ExitDecision::Fail
    }
}

/// Poll for task exits forever, acting on each one.
#[instrument(skip_all)]
pub async fn start_task_supervisor(task_manager: Arc<Mutex<WorkerTaskManager>>) -> R<()> {
    loop {
        tokio::time::sleep(SUPERVISOR_TICK).await;
        let exits = {
            let mut manager = task_manager.lock().expect("task manager");
            // Reap allocations whose grace period has run out (or whose
            // process already exited) before polling for crashes. Stopping is
            // two-phase precisely so this wait lives here, on the
            // supervisor's tick, and not on the heartbeat path — see
            // `WorkerTaskManager::begin_stop_allocation`.
            manager.finish_ready_terminations();
            manager.poll_task_exits()
        };
        for exit in exits {
            handle_exit(&task_manager, exit).await;
        }
    }
}

/// Apply one exit. Kept separate from the loop so the lock is taken in short,
/// obvious spans — the restart delay in particular must never be slept while
/// holding the task-manager lock, which the heartbeat path also needs.
pub async fn handle_exit(task_manager: &Arc<Mutex<WorkerTaskManager>>, exit: TaskExit) {
    let (task, budget_available) = {
        let mut manager = task_manager.lock().expect("task manager");
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
                let mut manager = task_manager.lock().expect("task manager");
                manager.record_restart_attempt(exit.allocation_id);
                manager.set_allocation_health(
                    exit.allocation_id,
                    false,
                    format!("{}; restarting", exit.describe()),
                );
            }
            tokio::time::sleep(delay).await;
            let result = {
                let mut manager = task_manager.lock().expect("task manager");
                // The allocation can have been stopped while we slept.
                if !manager.is_allocation_active(exit.allocation_id) {
                    return;
                }
                manager.restart_allocation(exit.allocation_id)
            };
            if let Err(err) = result {
                warn!(
                    allocation_id = exit.allocation_id,
                    task_name = %exit.task_name,
                    error = ?err,
                    "Failed to restart exited task"
                );
                let mut manager = task_manager.lock().expect("task manager");
                manager.record_failure(
                    exit.allocation_id,
                    format!("{}; restart failed: {err}", exit.describe()),
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
            let mut manager = task_manager.lock().expect("task manager");
            manager.record_termination(exit.allocation_id, &exit, true);
        }
        ExitDecision::Fail => {
            warn!(
                allocation_id = exit.allocation_id,
                task_name = %exit.task_name,
                status = %exit.describe(),
                "Task failed"
            );
            let mut manager = task_manager.lock().expect("task manager");
            manager.record_failure(exit.allocation_id, exit.describe());
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
                policy: RestartPolicy::Never,
                ..Default::default()
            }),
        );
        assert_eq!(
            decide(&task, &exit(Some(0)), true),
            ExitDecision::TerminalSuccess
        );
        assert_eq!(decide(&task, &exit(Some(1)), true), ExitDecision::Fail);
    }

    #[test]
    fn success_exit_codes_widen_what_counts_as_done() {
        let task = task(
            TaskLifecycle::Batch,
            Some(TaskRestartConfig {
                policy: RestartPolicy::OnFailure,
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
                policy: RestartPolicy::Never,
                ..Default::default()
            }),
        );
        assert_eq!(decide(&task, &killed, true), ExitDecision::Fail);
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
