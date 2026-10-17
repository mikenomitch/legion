//! Gang scheduling end to end (docs/gang-scheduling.md): a task with `gang`
//! places all of its replicas in one plan or none of them, waits blocked
//! (holding nothing) when they do not fit, and places whole when capacity
//! that could help appears.
//!
//! The manager runs as the real RPC service over a local store, and the eval
//! worker and the wake pass are the daemon's write sequences driven from the
//! test thread, as in `tests/blocked_evals.rs`.

use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store};
use legion::storage::store::{FailedMove, MovePolicy, MoveVerdict};
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{GangConfig, JobConfig, MemberLossPolicy, TaskConfig, TaskCount};
use legion::structs::manager_server::Manager;
use legion::structs::{
    GetJobInfoRequest, Node, NodeMsg, PlanJobRequest, StopCause, StopConfig, Submission,
};
use std::time::Duration;
use tonic::Request;

const LIMITS: EvalBatchLimits = EvalBatchLimits {
    max: 16,
    workers: 1,
};

/// A node with room for exactly one 3,000 MiB replica.
fn node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "node_id": id, "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

/// `count` replicas of 3,000 MiB each, all-or-nothing when `gang` is set.
fn job(name: &str, count: u32, gang: Option<GangConfig>) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(count),
            resources: json!({ "memory": 3000, "cpu": 500 }),
            group: "worker".into(),
            gang,
            // A built-in runtime, so `job plan`'s validation passes on a
            // checkout without built plugins.
            config: json!({ "command": ["true"] }),
            ..TaskConfig::new("worker", "exec")
        }],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn gang() -> Option<GangConfig> {
    Some(GangConfig::default())
}

struct Cluster {
    _dir: tempfile::TempDir,
    svc: ManagerDaemonService,
    runtime: tokio::runtime::Runtime,
}

impl Cluster {
    fn new(nodes: &[&str]) -> R<Self> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        for id in nodes {
            store.insert_node(NodeMsg::from(node(id)))?;
        }
        Ok(Cluster {
            _dir: dir,
            svc: ManagerDaemonService::new(store),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    /// `RunJob` for a new job: the rows, then the submit eval.
    fn submit(&self, job: &JobConfig) -> R<()> {
        self.store().handle_job_submit(
            job,
            &[Submission {
                path: format!("{}.json", job.name),
                content: "{}".to_string(),
                is_primary: true,
            }],
        )?;
        let config_id = self.store().get_job_config(&job.name)?.id();
        self.store()
            .enqueue_eval("job_submit", Some(&job.name), Some(&config_id), None, None)?;
        Ok(())
    }

    /// What worker registration does after inserting the node.
    fn join(&self, id: &str) -> R<()> {
        self.store().insert_node(NodeMsg::from(node(id)))?;
        self.store().request_capacity_reconcile()?;
        Ok(())
    }

    /// The eval worker, then a wake pass, until neither has work. Returns
    /// how many evals woke.
    fn settle(&self) -> R<usize> {
        let mut woken = 0;
        loop {
            let store = self.store();
            let mut batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
            while !batch.is_empty() {
                let completions = batch
                    .iter()
                    .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
                    .collect();
                batch =
                    store.complete_evals_and_claim_next(completions, LIMITS, ClaimScope::Any)?;
            }
            let now = store.wake_blocked_evals()?;
            if now == 0 {
                return Ok(woken);
            }
            woken += now;
        }
    }

    fn running(&self, job: &str) -> R<usize> {
        let mut count = 0;
        for node in self.store().get_nodes()? {
            count += self
                .store()
                .allocs_for_node(&node.id)?
                .iter()
                .filter(|allocation| allocation.job_id == job)
                .count();
        }
        Ok(count)
    }

    fn job_info(&self, name: &str) -> R<legion::structs::Job> {
        let reply = self
            .runtime
            .block_on(self.svc.get_job_info(Request::new(GetJobInfoRequest {
                name: name.to_string(),
            })))?
            .into_inner();
        reply.job.context("job")
    }

    fn plan(&self, job: &JobConfig) -> R<legion::structs::PlanJobReply> {
        Ok(self
            .runtime
            .block_on(self.svc.plan_job(Request::new(PlanJobRequest {
                parsed_jobspec: serde_json::to_string(job)?,
            })))?
            .into_inner())
    }
}

/// S1: a gang of three on room for two waits, holding nothing, and says how
/// far it got; a node joining makes room and all three start together.
#[test]
fn a_gang_waits_whole_and_starts_whole() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2"])?;
    let trainer = job("trainer", 3, gang());

    // The dry run says the same thing the submit is about to do.
    let plan = cluster.plan(&trainer)?;
    assert!(plan.allocations.is_empty());
    let report = plan.gang_reports.first().context("a gang report")?;
    assert!(!report.would_place);
    assert_eq!((report.placeable, report.min), (2, 3));
    assert!(
        plan.message.starts_with("Job would wait (blocked)"),
        "{}",
        plan.message
    );

    cluster.submit(&trainer)?;
    cluster.settle()?;
    assert_eq!(
        cluster.running("trainer")?,
        0,
        "a waiting gang holds nothing"
    );
    let blocked = cluster.store().blocked_evals()?;
    assert_eq!(blocked.len(), 1, "the eval blocked instead of failing");
    let info = cluster.job_info("trainer")?;
    assert_eq!(info.status, "blocked");
    let detail = info.blocked.context("the blocked block")?;
    assert_eq!(detail.task, "worker");
    let gang = detail.gang.context("the gang report")?;
    assert_eq!(
        (gang.desired, gang.min, gang.running, gang.placeable),
        (3, 3, 0, 2)
    );
    assert!(!gang.would_place);
    assert!(
        detail.message.starts_with("Gang worker waits"),
        "{}",
        detail.message
    );

    cluster.join("n-3")?;
    assert_eq!(cluster.settle()?, 1, "the new node wakes the gang");
    assert_eq!(
        cluster.running("trainer")?,
        3,
        "and all three place together"
    );
    assert!(cluster.store().blocked_evals()?.is_empty());
    assert!(cluster.job_info("trainer")?.blocked.is_none());
    Ok(())
}

/// S2: two gangs and room for one. One places whole; the other waits at
/// zero, and a small job still places beside it, because a waiting gang
/// holds nothing. When the winner stops, the loser starts whole.
#[test]
fn a_waiting_gang_holds_nothing_and_takes_freed_capacity_whole() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2", "n-3"])?;
    cluster.submit(&job("first", 2, gang()))?;
    cluster.settle()?;
    assert_eq!(cluster.running("first")?, 2);

    cluster.submit(&job("second", 2, gang()))?;
    cluster.settle()?;
    assert_eq!(cluster.running("second")?, 0, "one free node is not enough");

    // Nothing is reserved for the waiting gang: a one-replica job takes the
    // node it could not use.
    cluster.submit(&job("small", 1, None))?;
    cluster.settle()?;
    assert_eq!(cluster.running("small")?, 1);

    // The first gang finishes and frees two nodes: the second places whole.
    cluster.store().remove_allocs_for_job("first")?;
    assert_eq!(
        cluster.settle()?,
        1,
        "freed capacity wakes the waiting gang"
    );
    assert_eq!(cluster.running("second")?, 2);
    Ok(())
}

fn gang_on_loss(policy: MemberLossPolicy) -> Option<GangConfig> {
    Some(GangConfig {
        on_member_loss: policy,
        ..GangConfig::default()
    })
}

/// A gang job whose members stop within a second, so a restart's re-place
/// (which waits out the survivors' stop grace) is quick to observe.
fn quick_stop(mut job: JobConfig) -> JobConfig {
    job.tasks[0].stop = Some(StopConfig {
        kill_after: Duration::from_secs(1),
        ..StopConfig::default()
    });
    job
}

fn lose_node(cluster: &Cluster, node: &str) -> R<()> {
    let store = cluster.store();
    store.mark_node_lost(node)?;
    store.reschedule_allocations_for_lost_node(node)
}

fn gang_events(cluster: &Cluster) -> R<Vec<serde_json::Value>> {
    Ok(cluster
        .store()
        .list_scheduler_events(&legion::storage::events::SchedulerEventFilter {
            kind: Some("job.gang_member_lost".into()),
            ..Default::default()
        })?
        .into_iter()
        .map(|event| serde_json::from_str(&event.metadata_json))
        .collect::<Result<_, _>>()?)
}

/// S3 with `onMemberLoss = "replace"`: a node dies under a running gang on a
/// full cluster. The top-up waits (blocked, survivors still running), and
/// capacity arriving places it.
#[test]
fn a_lost_member_tops_up_when_room_appears() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2", "n-3"])?;
    cluster.submit(&job("trainer", 3, gang_on_loss(MemberLossPolicy::Replace)))?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 3);

    lose_node(&cluster, "n-3")?;
    let store = cluster.store();
    // The lost-node reconcile queued its retry for later; run it now.
    let config_id = store.get_job_config("trainer")?.id();
    store.enqueue_eval(
        "job_reschedule",
        Some("trainer"),
        Some(&config_id),
        None,
        None,
    )?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 2, "the survivors keep running");
    let info = cluster.job_info("trainer")?;
    assert_eq!(info.status, "running");
    let gang = info
        .blocked
        .and_then(|blocked| blocked.gang)
        .context("the top-up is blocked as a gang")?;
    assert_eq!((gang.running, gang.placeable, gang.min), (2, 0, 3));

    cluster.join("n-4")?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 3);
    assert!(cluster.job_info("trainer")?.blocked.is_none());
    Ok(())
}

fn gang_with_timeout(timeout: std::time::Duration) -> Option<GangConfig> {
    Some(GangConfig {
        placement_timeout: Some(timeout),
        ..GangConfig::default()
    })
}

/// The wait clock starts when the gang first blocks and is kept across
/// every wake and re-block: capacity that frees but not enough wakes the
/// gang, it re-plans, blocks again, and still counts from the first block.
#[test]
fn a_gang_wait_counts_from_its_first_block() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2"])?;
    cluster.submit(&job("filler", 1, None))?;
    cluster.settle()?;
    let trainer = job(
        "trainer",
        2,
        gang_with_timeout(std::time::Duration::from_secs(3600)),
    );
    cluster.submit(&trainer)?;
    cluster.settle()?;
    let config_id = cluster.store().get_job_config("trainer")?.id();
    let first = cluster
        .store()
        .gang_wait(&config_id)?
        .context("the first block starts the wait")?;
    assert_eq!(first.deadline_unix, Some(first.since_unix + 3600));
    let info = cluster.job_info("trainer")?;
    let gang = info
        .blocked
        .and_then(|blocked| blocked.gang)
        .context("gang report")?;
    assert_eq!(gang.placement_timeout_seconds, 3600);
    assert_eq!(gang.waiting_since, first.since_unix);
    assert_eq!(gang.deadline, first.since_unix + 3600);

    // A third node joins: the gang wakes, re-plans, and (the filler still
    // holding one node) fits now. Before that, make the wake useless by
    // filling the new node first.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    cluster.store().insert_node(NodeMsg::from(node("n-3")))?;
    cluster.submit(&job("filler-2", 1, None))?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 0);
    let again = cluster
        .store()
        .gang_wait(&config_id)?
        .context("still waiting")?;
    assert_eq!(again.since_unix, first.since_unix, "the clock was kept");

    // Room for both: the gang places and its wait ends.
    cluster.store().remove_allocs_for_job("filler")?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 2);
    assert!(cluster.store().gang_wait(&config_id)?.is_none());
    Ok(())
}

/// A gang still short past its `placementTimeout` fails its job: the
/// leader's timer wakes it, the plan comes up short again, and instead of
/// blocking the job is failed. A failed gang job then places nothing, even
/// when room appears, until `job run` restarts it.
#[test]
fn a_gang_past_its_placement_timeout_fails_its_job() -> R<()> {
    let cluster = Cluster::new(&["n-1"])?;
    let trainer = job(
        "trainer",
        2,
        gang_with_timeout(std::time::Duration::from_secs(1)),
    );
    cluster.submit(&trainer)?;
    cluster.settle()?;
    assert_eq!(cluster.store().blocked_evals()?.len(), 1);
    assert_eq!(cluster.store().wake_timed_out_gangs()?, 0, "not yet");

    std::thread::sleep(std::time::Duration::from_millis(2100));
    assert_eq!(cluster.store().wake_timed_out_gangs()?, 1);
    cluster.settle()?;
    let info = cluster.job_info("trainer")?;
    assert_eq!(info.status, "failed");
    assert!(cluster.store().blocked_evals()?.is_empty());
    let config_id = cluster.store().get_job_config("trainer")?.id();
    assert!(cluster.store().gang_wait(&config_id)?.is_none());
    let timeout_events =
        cluster
            .store()
            .list_scheduler_events(&legion::storage::events::SchedulerEventFilter {
                kind: Some("job.gang_timeout".into()),
                ..Default::default()
            })?;
    assert_eq!(timeout_events.len(), 1, "{timeout_events:?}");

    // Room appears: a failed gang job stays down.
    cluster.join("n-2")?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 0);

    // `job run` clears the failure and restarts it.
    cluster.store().clear_job_failed_status("trainer")?;
    cluster
        .store()
        .enqueue_eval("job_update", Some("trainer"), Some(&config_id), None, None)?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 2);
    Ok(())
}

/// An elastic gang that reaches its quorum has stopped waiting, even while
/// the members above its quorum still wait for room: its placement timeout
/// no longer applies, and the timer leaves it alone.
#[test]
fn a_gang_at_its_quorum_is_not_timed_out() -> R<()> {
    let cluster = Cluster::new(&["n-1"])?;
    let elastic = Some(GangConfig {
        min: Some(2),
        placement_timeout: Some(Duration::from_secs(1)),
        ..GangConfig::default()
    });
    cluster.submit(&job("trainer", 3, elastic))?;
    cluster.settle()?;
    let config_id = cluster.store().get_job_config("trainer")?.id();
    assert!(
        cluster.store().gang_wait(&config_id)?.is_some(),
        "short: waiting"
    );

    cluster.join("n-2")?;
    cluster.settle()?;
    // The plan placed the quorum and left the third member unplaced, for the
    // capacity pass: a partial placement, so the eval succeeded.
    assert_eq!(cluster.running("trainer")?, 2, "the quorum placed");
    assert!(cluster.store().blocked_evals()?.is_empty());
    assert!(
        cluster.store().gang_wait(&config_id)?.is_none(),
        "the wait ended"
    );

    std::thread::sleep(Duration::from_millis(2100));
    assert_eq!(cluster.store().wake_timed_out_gangs()?, 0);
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 2);
    assert_ne!(cluster.job_info("trainer")?.status, "failed");
    Ok(())
}

/// The default `onMemberLoss = "restart"`: a node dies under a gang that
/// cannot run without it. The survivors stop (stop cause `gang`), and once
/// their stop grace has passed the whole gang is placed again, together.
#[test]
fn a_broken_gang_restarts_whole_by_default() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2", "n-3", "n-4"])?;
    cluster.submit(&quick_stop(job("trainer", 3, gang())))?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 3);
    let lost_node = cluster
        .store()
        .allocs_for_job("trainer")?
        .first()
        .map(|allocation| allocation.node_id.clone())
        .context("a member")?;

    lose_node(&cluster, &lost_node)?;
    assert_eq!(
        cluster.running("trainer")?,
        0,
        "the survivors stop with the lost member"
    );
    let events = gang_events(&cluster)?;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["action"], "restart");
    assert_eq!(events[0]["running"], 2);
    assert_eq!(events[0]["stopped"], 2);
    assert_eq!(events[0]["cause"], "node_lost");

    // Not before the survivors' stop grace: nothing re-places yet.
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 0);
    std::thread::sleep(Duration::from_millis(1100));
    cluster.settle()?;
    assert_eq!(
        cluster.running("trainer")?,
        3,
        "the whole gang placed again"
    );
    assert!(cluster
        .store()
        .allocs_for_job("trainer")?
        .iter()
        .all(|allocation| allocation.node_id != lost_node));
    Ok(())
}

/// An elastic gang (`min` below `count`) that still runs its quorum after a
/// loss is not broken: the member is replaced alone and nothing restarts.
#[test]
fn a_loss_above_the_quorum_replaces_the_member_alone() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2", "n-3", "n-4"])?;
    let elastic = Some(GangConfig {
        min: Some(2),
        ..GangConfig::default()
    });
    cluster.submit(&job("trainer", 3, elastic))?;
    cluster.settle()?;
    let lost_node = cluster.store().allocs_for_job("trainer")?[0]
        .node_id
        .clone();
    lose_node(&cluster, &lost_node)?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 3, "replaced inline");
    let events = gang_events(&cluster)?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["action"], "replace");
    assert_eq!(events[0]["stopped"], 0);
    Ok(())
}

/// `onMemberLoss = "fail"`: a lost member fails the job and stops every
/// allocation of it, and the failed gang job places nothing until restarted.
#[test]
fn on_member_loss_fail_fails_the_job() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2", "n-3", "n-4"])?;
    cluster.submit(&job("trainer", 3, gang_on_loss(MemberLossPolicy::Fail)))?;
    cluster.settle()?;
    let lost_node = cluster.store().allocs_for_job("trainer")?[0]
        .node_id
        .clone();
    lose_node(&cluster, &lost_node)?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 0);
    assert_eq!(cluster.job_info("trainer")?.status, "failed");
    assert_eq!(gang_events(&cluster)?[0]["action"], "fail");
    cluster.join("n-5")?;
    cluster.settle()?;
    assert_eq!(
        cluster.running("trainer")?,
        0,
        "a failed gang job stays down"
    );
    Ok(())
}

/// A member that fails (a worker's report) restarts the gang too, and the
/// failed member's move backoff holds the whole gang's re-placement. Once the
/// task's move budget is spent, the next failure fails the job with every
/// member stopped, whatever the policy.
#[test]
fn failure_driven_restarts_spend_the_move_budget() -> R<()> {
    let cluster = Cluster::new(&["n-1", "n-2", "n-3"])?;
    cluster.submit(&job("trainer", 3, gang()))?;
    cluster.settle()?;
    assert_eq!(cluster.running("trainer")?, 3);
    let store = cluster.store();
    // What the manager does with a worker's failure report.
    let fail_a_member = || -> R<MoveVerdict> {
        let member = store.allocs_for_job("trainer")?[0].clone();
        let removal = store
            .remove_failed_allocation(
                member.id,
                StopCause::Unknown,
                None,
                FailedMove {
                    policy: MovePolicy {
                        move_delay_ms: 1_000,
                        move_count: Some(1),
                        move_period_ms: 3_600_000,
                    },
                    reason: "exit status 1".into(),
                },
            )?
            .context("removed")?;
        let exhausted = matches!(removal.verdict, MoveVerdict::Exhausted { .. });
        store.gang_member_lost(&removal.allocation, StopCause::Unknown, exhausted)?;
        Ok(removal.verdict)
    };
    let reschedule = || -> R<()> {
        let config_id = store.get_job_config("trainer")?.id();
        store.enqueue_eval(
            "job_reschedule",
            Some("trainer"),
            Some(&config_id),
            None,
            None,
        )?;
        cluster.settle()?;
        Ok(())
    };

    assert!(matches!(fail_a_member()?, MoveVerdict::Deferred(_)));
    assert_eq!(cluster.running("trainer")?, 0, "the gang restarts");
    reschedule()?;
    assert_eq!(
        cluster.running("trainer")?,
        0,
        "the failed member's backoff holds the whole gang"
    );
    std::thread::sleep(Duration::from_millis(1_100));
    reschedule()?;
    assert_eq!(
        cluster.running("trainer")?,
        3,
        "placed whole after the backoff"
    );

    assert!(matches!(fail_a_member()?, MoveVerdict::Exhausted { .. }));
    assert_eq!(cluster.running("trainer")?, 0);
    assert_eq!(cluster.job_info("trainer")?.status, "failed");
    let actions: Vec<_> = gang_events(&cluster)?
        .iter()
        .map(|event| event["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(actions, ["restart", "fail"]);
    Ok(())
}
