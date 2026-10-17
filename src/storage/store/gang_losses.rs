//! A gang member is lost: what happens to the rest (docs/gang-scheduling.md,
//! "When a member is lost").
//!
//! A gang is *broken* when fewer than its quorum (`gang.min`, else its
//! count) still run. A loss that leaves the quorum running is replaced alone,
//! as for any task, so an elastic gang keeps going. A loss that breaks the
//! gang applies its `onMemberLoss`:
//!
//! - `restart` stops the survivors (stop cause `gang`) so the whole gang is
//!   placed again as one unit;
//! - `replace` keeps them, and the missing members place together once they
//!   fit;
//! - `fail` marks the job failed, then stops every allocation of it.
//!
//! A member whose move budget ran out fails the job whatever the policy: the
//! gang cannot be whole again.
//!
//! The losses are a member failing (a worker's report, or a memory-pressure
//! eviction) and its node being lost or drained. Every caller runs
//! [`Store::gang_member_lost`] after the lost row is gone and before it
//! queues the replacement, so no plan can top the gang up before the
//! decision. Worker reports land on whichever manager the worker dials, so
//! the decision is a scheduler-lane op that forwards to the leader, which
//! reads the gang's members from its own state: it is the decision, made
//! where the authoritative copy lives.

use super::*;
use crate::structs::MemberLossPolicy;

/// What one member loss did to its gang.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GangLoss {
    pub job_id: String,
    pub job_config_id: String,
    pub task_name: String,
    /// What the loss led to: `replace` when the gang still runs its quorum
    /// (or its policy says so), else the gang's `onMemberLoss`, or `fail`
    /// when the member's move budget ran out.
    pub action: MemberLossPolicy,
    /// Members still running after the loss, before the action.
    pub running: u32,
    /// The gang's quorum.
    pub min: u32,
    /// Allocations the action stopped (the survivors of a restart, every
    /// allocation of the job for a fail).
    pub stopped: usize,
    /// For a restart: the survivors' stop grace (`stop.killAfter`), which a
    /// caller that queues the re-placement itself waits out first, so new
    /// members never start beside old ones still shutting down.
    pub grace_secs: u64,
}

impl Store {
    /// Apply the gang policy of `lost`'s task, if it is a gang
    /// (module docs). `cause` is why the member went (for the event);
    /// `exhausted` says its move budget ran out. Returns what happened, or
    /// `None` when the task is not a gang or its job no longer wants it.
    #[instrument(err, skip_all, fields(allocation_id = lost.id, job_id = %lost.job_id))]
    pub fn gang_member_lost(
        &self,
        lost: &Allocation,
        cause: StopCause,
        exhausted: bool,
    ) -> R<Option<GangLoss>> {
        // Free for every task that is not a gang: one read-model lookup.
        let is_gang = self
            .cached_task_configs_for_job_config(&lost.job_config_id)?
            .iter()
            .any(|(id, task)| *id == lost.task_config_id && task.gang.is_some());
        if !is_gang {
            return Ok(None);
        }
        let Some(mut loss) = self.gang_loss_op(
            &lost.job_id,
            &lost.job_config_id,
            lost.task_config_id,
            exhausted,
            false,
        )?
        else {
            return Ok(None);
        };
        if loss.action == MemberLossPolicy::Fail {
            // Failed first, so a plan racing this one finds the job
            // unwanted and places nothing; then everything stops.
            self.mark_job_failed(&lost.job_id)?;
            if let Some(stopped) = self.gang_loss_op(
                &lost.job_id,
                &lost.job_config_id,
                lost.task_config_id,
                exhausted,
                true,
            )? {
                loss.stopped = stopped.stopped;
            }
        }
        self.events().job_gang_member_lost(lost, cause, &loss);
        crate::manager::telemetry::metrics()
            .gang_outcomes_total
            .inc(&[loss.action.as_str()]);
        info!(
            job_id = %loss.job_id,
            task = %loss.task_name,
            action = loss.action.as_str(),
            running = loss.running,
            min = loss.min,
            stopped = loss.stopped,
            cause = cause.as_str(),
            "Gang member lost"
        );
        Ok(Some(loss))
    }

    /// The gang half of a node-scoped removal (a lost or drained node):
    /// apply each gang's policy once per task that lost members, and return
    /// the configs still to reconcile now. A restarted gang re-places after
    /// its survivors' stop grace instead (a delayed `job_reschedule`), so new
    /// members never start beside old ones still shutting down, and a failed
    /// gang job places nothing. A decision that cannot be made is logged and
    /// the config reconciled as before: the lost members are replaced.
    pub(crate) fn gang_members_lost_on_node(
        &self,
        removed: &[Allocation],
        cause: StopCause,
        mut reconcile: HashSet<String>,
    ) -> HashSet<String> {
        let mut seen: HashSet<(String, i64)> = HashSet::new();
        for allocation in removed {
            if !seen.insert((allocation.job_config_id.clone(), allocation.task_config_id)) {
                continue;
            }
            let loss = match self.gang_member_lost(allocation, cause, false) {
                Ok(Some(loss)) => loss,
                Ok(None) => continue,
                Err(err) => {
                    warn!(
                        allocation_id = allocation.id,
                        job_id = %allocation.job_id,
                        error = %format!("{err:#}"),
                        "Gang member-loss policy failed; replacing the member"
                    );
                    continue;
                }
            };
            match loss.action {
                MemberLossPolicy::Replace => {}
                MemberLossPolicy::Fail => {
                    reconcile.remove(&allocation.job_config_id);
                }
                MemberLossPolicy::Restart if loss.stopped > 0 => {
                    reconcile.remove(&allocation.job_config_id);
                    if let Err(err) = self.enqueue_eval_after_delay(
                        "job_reschedule",
                        Some(&allocation.job_id),
                        Some(&allocation.job_config_id),
                        None,
                        None,
                        Duration::from_secs(loss.grace_secs),
                    ) {
                        warn!(
                            job_id = %allocation.job_id,
                            error = %format!("{err:#}"),
                            "Failed to queue a gang restart; reconciling now"
                        );
                        reconcile.insert(allocation.job_config_id.clone());
                    }
                }
                MemberLossPolicy::Restart => {}
            }
        }
        reconcile
    }

    /// [`Self::gang_loss_op_local`] on the scheduler-lane leader, forwarded
    /// there from a follower.
    fn gang_loss_op(
        &self,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        exhausted: bool,
        stop_job: bool,
    ) -> R<Option<GangLoss>> {
        match self.gang_loss_op_local(job_id, job_config_id, task_config_id, exhausted, stop_job) {
            Err(err) if crate::storage::raft::driver::NotRaftLeader::in_chain(&err) => self
                .forward_after_rejection(
                    crate::storage::raft::forward::ForwardableLaneWrite::GangMemberLost {
                        job_id: job_id.to_owned(),
                        job_config_id: job_config_id.to_owned(),
                        task_config_id,
                        exhausted,
                        stop_job,
                    },
                    err,
                ),
            other => other,
        }
    }

    /// Decide a member loss and act on it (`stop_job: false`), or stop every
    /// allocation of a job being failed for one (`stop_job: true`, called
    /// after the job is marked failed). One scheduler-lane op either way.
    pub(crate) fn gang_loss_op_local(
        &self,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        exhausted: bool,
        stop_job: bool,
    ) -> R<Option<GangLoss>> {
        let (job_id, job_config_id) = (job_id.to_owned(), job_config_id.to_owned());
        self.replicated(Lane::Scheduler, "gang_member_lost", move |s| {
            if stop_job {
                s.stop_failed_gang_job_apply(&job_id, &job_config_id)
            } else {
                s.decide_gang_loss_apply(&job_id, &job_config_id, task_config_id, exhausted)
            }
        })
    }

    fn decide_gang_loss_apply(
        &self,
        job_id: &str,
        job_config_id: &str,
        task_config_id: i64,
        exhausted: bool,
    ) -> R<Option<GangLoss>> {
        let Some(task) = self
            .cached_task_configs_for_job_config(job_config_id)?
            .into_iter()
            .find(|(id, _)| *id == task_config_id)
            .map(|(_, task)| task)
        else {
            return Ok(None);
        };
        let Some(gang) = task.gang else {
            return Ok(None);
        };
        // A stopped, superseded, or already-failed job has nothing to
        // restart or fail.
        if self.job_config_unwanted_reason(job_config_id)?.is_some() {
            return Ok(None);
        }
        let survivors: Vec<Allocation> = self
            .allocs_for_job_config(job_config_id)?
            .into_iter()
            .filter(|allocation| allocation.task_config_id == task_config_id)
            .collect();
        let desired = self.effective_task_count(job_id, &task.group, &task.name, task.count.num)?;
        let min = gang.quorum(desired);
        let running = survivors.len() as u32;
        let action = if exhausted {
            MemberLossPolicy::Fail
        } else if running >= min {
            MemberLossPolicy::Replace
        } else {
            gang.on_member_loss
        };
        let mut loss = GangLoss {
            job_id: job_id.to_string(),
            job_config_id: job_config_id.to_string(),
            task_name: task.name.clone(),
            action,
            running,
            min,
            stopped: 0,
            grace_secs: 0,
        };
        if action == MemberLossPolicy::Restart && !survivors.is_empty() {
            self.remove_allocations(&survivors, StopCause::Gang)?;
            loss.stopped = survivors.len();
            loss.grace_secs = task.stop.clone().unwrap_or_default().kill_after.as_secs();
        }
        Ok(Some(loss))
    }

    fn stop_failed_gang_job_apply(&self, job_id: &str, job_config_id: &str) -> R<Option<GangLoss>> {
        let allocations = self.allocs_for_job(job_id)?;
        if !allocations.is_empty() {
            self.remove_allocations(&allocations, StopCause::Gang)?;
        }
        {
            let conn = self.write_connection()?;
            Self::delete_gang_waits_for_job_tx(&conn, job_id)?;
        }
        Ok(Some(GangLoss {
            job_id: job_id.to_string(),
            job_config_id: job_config_id.to_string(),
            task_name: String::new(),
            action: MemberLossPolicy::Fail,
            running: 0,
            min: 0,
            stopped: allocations.len(),
            grace_secs: 0,
        }))
    }
}
