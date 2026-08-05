//! The belt-and-braces scheduling reconciler (plan §7-C2, JC-41;
//! adversarial hardening 2026-07-24).
//!
//! Since JC-41 the eval queue is REPLICATED, so a failover no longer loses
//! it — `tests/raft_eval_failover.rs` proves that on a real cluster, and it
//! is what actually keeps acknowledged work alive. `regenerate_scheduling_
//! evals` survives as the SECOND line of defence: an enqueue op that was
//! rejected under divergent lane leadership, a row `prune_stale_evals`
//! dropped as superseded, or an operator who cleared the queue by hand all
//! leave replicated state describing work with nothing queued to do it.
//!
//! These tests exercise exactly that input — replicated state that still
//! needs work, plus an EMPTY queue (see `clear_eval_queue`, which models the
//! gap, not a failover any more) — and assert the acquisition hook
//! (`prune_stale_evals` + `requeue_in_flight_evals` +
//! `regenerate_scheduling_evals`) converges without over-executing. The
//! adversarial pass proved the original pass was broken for stop tails,
//! config-update cleanups, rollout ticks, KV restarts, and — the
//! over-execution case — no-evict cordons.

use legion::prelude::{Context, R};
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, TaskConfig, TaskCount};
use legion::structs::{Node, NodeMsg, Submission};
use serde_json::json;

fn setup_store() -> R<Store> {
    let dir = tempfile::Builder::new()
        .prefix("legion-eval-regen-")
        .tempdir()?
        .keep();
    let config = ManagerConfigFull {
        store_path: dir.to_str().context("path")?.to_string(),
        ..Default::default()
    };
    let store = Store::start_storage(&config)?;
    store.reset_all_tables()?;
    Ok(store)
}

fn test_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn job(name: &str, msg: &str, count: u32) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            config: json!({ "message": msg }),
            resources: json!({ "memory": 64 }),
            count: TaskCount::new(count),
            ..TaskConfig::new("svc", "noop")
        }],
        version: None,
    }
}

fn submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.ncl"),
        content: format!("job {name}"),
        is_primary: true,
    }
}

/// Drain the queue through the real handlers, as the daemon's eval workers
/// would, returning `(kind, ok)` per processed eval.
fn process_queue(store: &Store) -> R<Vec<(String, bool)>> {
    let mut out = Vec::new();
    let mut n = 0;
    while let Some(eval) = store.claim_eval()? {
        let res = eval.handle(store);
        let ok = res.is_ok();
        if let Err(e) = &res {
            eprintln!("eval {} ({}) failed: {e:#}", eval.id, eval.kind);
        }
        store.complete_eval(&eval.id, ok)?;
        out.push((eval.kind.clone(), ok));
        n += 1;
        assert!(n < 50, "queue not draining");
    }
    Ok(out)
}

/// Empty the queue without doing its work, leaving replicated state
/// describing work that nothing is queued to perform.
///
/// Under JC-27 this modelled a failover (the local queue died with the
/// leader). JC-41 replicates the queue, so it no longer does: it models the
/// reconciler's actual input — an enqueue that never landed, a row dropped
/// as superseded, or an operator-cleared queue.
fn clear_eval_queue(store: &Store) -> R<()> {
    let mut n = 0;
    while let Some(eval) = store.claim_eval()? {
        store.complete_eval(&eval.id, true)?;
        n += 1;
        assert!(n < 50);
    }
    Ok(())
}

/// The full leadership-acquisition hook, in daemon order.
fn acquisition_hook(store: &Store) -> R<u64> {
    store.prune_stale_evals()?;
    store.requeue_in_flight_evals()?;
    store.regenerate_scheduling_evals()
}

/// A stopped job whose `job_stop` eval never made it onto the queue: the
/// reconciler must re-derive the teardown tail from replicated state — the
/// stopped job's allocations must not survive forever. (With JC-41 the eval
/// normally survives on its own; this is the gap-filling path.)
#[test]
fn stop_tail_is_reconciled_when_its_eval_is_missing() -> R<()> {
    let store = setup_store()?;
    store.insert_node(NodeMsg::from(test_node("w1")))?;

    store.handle_job_submit(&job("stopper", "v1", 2), &[submission("stopper")])?;
    let cfg = store.get_job_config("stopper")?.id();
    store.enqueue_eval("job_submit", Some("stopper"), Some(&cfg), None, None)?;
    process_queue(&store)?;
    assert_eq!(store.allocs_for_job("stopper")?.len(), 2, "placed");

    // Acked stop: replicated status flip + its own scheduler-lane eval —
    // then the eval is lost before anything ran it.
    store.stop_job("stopper")?;
    store.enqueue_eval("job_stop", Some("stopper"), Some(&cfg), None, None)?;
    clear_eval_queue(&store)?;

    let regenerated = acquisition_hook(&store)?;
    assert!(regenerated > 0, "the stop tail must be regenerated");
    let kinds: Vec<String> = store.get_evals()?.into_iter().map(|e| e.kind).collect();
    assert!(kinds.contains(&"job_stop".to_string()), "kinds: {kinds:?}");
    process_queue(&store)?;

    assert!(
        store.allocs_for_job("stopper")?.is_empty(),
        "a stopped job's allocations must be torn down by the reconciler"
    );
    // Idempotent: a second hook pass finds nothing left to do for the job.
    assert_eq!(acquisition_hook(&store)?, 0);
    Ok(())
}

/// Acknowledged config update (v2 committed) whose job_submit eval is
/// missing: reconciliation must converge to v2-only, not run full v1
/// alongside full v2.
#[test]
fn submit_tail_reconciliation_converges_to_the_latest_config() -> R<()> {
    let store = setup_store()?;
    store.insert_node(NodeMsg::from(test_node("w1")))?;
    store.insert_node(NodeMsg::from(test_node("w2")))?;

    store.handle_job_submit(&job("updater", "v1", 2), &[submission("updater")])?;
    let v1 = store.get_job_config("updater")?.id();
    store.enqueue_eval("job_submit", Some("updater"), Some(&v1), None, None)?;
    process_queue(&store)?;
    assert_eq!(store.allocs_for_job("updater")?.len(), 2);

    // Acked v2 submit (changed resources => not schedule-equivalent) whose
    // eval never ran.
    let mut v2_job = job("updater", "v2", 2);
    v2_job.tasks[0].resources = json!({ "memory": 96 });
    store.handle_job_submit(&v2_job, &[submission("updater-v2")])?;
    let v2 = store.get_job_config("updater")?.id();
    assert_ne!(v1, v2);
    store.enqueue_eval("job_submit", Some("updater"), Some(&v2), None, None)?;
    clear_eval_queue(&store)?;

    acquisition_hook(&store)?;
    let processed = process_queue(&store)?;
    eprintln!("processed: {processed:?}");

    let allocs = store.allocs_for_job("updater")?;
    let v1_count = allocs.iter().filter(|a| a.job_config_id == v1).count();
    let v2_count = allocs.iter().filter(|a| a.job_config_id == v2).count();
    assert_eq!(
        (v1_count, v2_count),
        (0, 2),
        "after failover the job must run v2 only (got v1={v1_count}, v2={v2_count})"
    );
    Ok(())
}

/// The PARTIAL submit case: v2 was fully placed but the v1 cleanup never
/// happened. Reconciliation must sweep the stale config's allocations
/// without doubling v2.
#[test]
fn partially_run_submit_tail_sweeps_stale_config_allocations() -> R<()> {
    let store = setup_store()?;
    store.insert_node(NodeMsg::from(test_node("w1")))?;
    store.insert_node(NodeMsg::from(test_node("w2")))?;

    store.handle_job_submit(&job("partial", "v1", 2), &[submission("partial")])?;
    let v1 = store.get_job_config("partial")?.id();
    store.enqueue_eval("job_submit", Some("partial"), Some(&v1), None, None)?;
    process_queue(&store)?;
    assert_eq!(store.allocs_for_job("partial")?.len(), 2);

    let mut v2_job = job("partial", "v2", 2);
    v2_job.tasks[0].resources = json!({ "memory": 96 });
    store.handle_job_submit(&v2_job, &[submission("partial-v2")])?;
    let v2 = store.get_job_config("partial")?.id();
    // Simulate the handler having placed v2 (the first half of the lost
    // eval) without the prev-config removal (its second half).
    let v2_tasks = store.get_task_configs_for_job_config(&v2)?;
    let (task_id, _) = *v2_tasks.first().context("task config")?;
    store.place_alloc("w1", task_id)?;
    store.place_alloc("w2", task_id)?;
    clear_eval_queue(&store)?;

    acquisition_hook(&store)?;
    process_queue(&store)?;

    let allocs = store.allocs_for_job("partial")?;
    let v1_count = allocs.iter().filter(|a| a.job_config_id == v1).count();
    let v2_count = allocs.iter().filter(|a| a.job_config_id == v2).count();
    assert_eq!(
        (v1_count, v2_count),
        (0, 2),
        "stale v1 allocations must be swept and v2 must not double (v1={v1_count}, v2={v2_count})"
    );
    Ok(())
}

/// A mid-flight rollout with no queued tick gets one back, and the job is
/// EXCLUDED from the blanket reschedule that would bypass the rollout's
/// gating.
#[test]
fn live_rollouts_regenerate_ticks_not_blanket_reschedules() -> R<()> {
    let store = setup_store()?;
    store.insert_node(NodeMsg::from(test_node("w1")))?;

    store.handle_job_submit(&job("roller", "v1", 2), &[submission("roller")])?;
    let v1 = store.get_job_config("roller")?.id();
    store.enqueue_eval("job_submit", Some("roller"), Some(&v1), None, None)?;
    process_queue(&store)?;

    let mut v2_job = job("roller", "v2", 2);
    v2_job.tasks[0].resources = json!({ "memory": 96 });
    store.handle_job_submit(&v2_job, &[submission("roller-v2")])?;
    let v2 = store.get_job_config("roller")?.id();
    store.create_rollout(
        "roller",
        Some(&v1),
        &v2,
        "running",
        Some("linear"),
        None,
        None,
        None,
        "{}",
    )?;
    // The rollout's next tick is missing from the queue.
    clear_eval_queue(&store)?;

    acquisition_hook(&store)?;
    let kinds: Vec<String> = store.get_evals()?.into_iter().map(|e| e.kind).collect();
    assert!(
        kinds.contains(&"rollout_tick".to_string()),
        "a live rollout's tick must be regenerated: {kinds:?}"
    );
    assert!(
        !kinds
            .iter()
            .any(|k| k == "job_reschedule" || k == "job_submit"),
        "a job with a live rollout must not get a blanket reschedule (it would place the \
         to-config at full count, bypassing the rollout's gating): {kinds:?}"
    );

    // An operator-wait rollout regenerates NOTHING (it waits for a human).
    clear_eval_queue(&store)?;
    let rollout = store
        .latest_rollout_for_job_config(&v2)?
        .context("rollout")?;
    store.update_rollout_status(rollout.id, "waiting_manual_advance", false, None)?;
    assert_eq!(acquisition_hook(&store)?, 0);
    Ok(())
}

/// A KV-triggered restart of an unchanged config is recoverable from the
/// replicated restart marker even with no queued eval: the hook regenerates
/// job_update, the eval consumes the marker, and a second pass regenerates
/// nothing.
#[test]
fn kv_restart_marker_is_reconcilable_and_consumed_once() -> R<()> {
    let store = setup_store()?;
    store.insert_node(NodeMsg::from(test_node("w1")))?;

    store.handle_job_submit(&job("restarter", "v1", 2), &[submission("restarter")])?;
    let cfg = store.get_job_config("restarter")?.id();
    store.enqueue_eval("job_submit", Some("restarter"), Some(&cfg), None, None)?;
    process_queue(&store)?;
    let before: Vec<i64> = store
        .allocs_for_job("restarter")?
        .iter()
        .map(|a| a.id)
        .collect();
    assert_eq!(before.len(), 2);

    // Acked restart: the marker is replicated, its eval is not queued.
    store.request_job_restart("restarter")?;
    store.enqueue_eval("job_update", Some("restarter"), Some(&cfg), None, None)?;
    clear_eval_queue(&store)?;

    acquisition_hook(&store)?;
    let kinds: Vec<String> = store.get_evals()?.into_iter().map(|e| e.kind).collect();
    assert!(
        kinds.contains(&"job_update".to_string()),
        "the restart must be regenerated from the replicated marker: {kinds:?}"
    );
    let processed = process_queue(&store)?;
    assert!(
        processed.contains(&("job_update".to_string(), true)),
        "the regenerated restart must execute successfully: {processed:?}"
    );
    assert_eq!(
        store.allocs_for_job("restarter")?.len(),
        2,
        "restart re-placed the job (SQLite may reuse the freed rowids, so \
         only the count and the successful job_update run are asserted)"
    );

    // Marker consumed: no further job_update regenerates; the follow-up
    // pass is at most a no-op reschedule.
    let kinds_after: Vec<String> = {
        acquisition_hook(&store)?;
        store.get_evals()?.into_iter().map(|e| e.kind).collect()
    };
    assert!(
        !kinds_after.contains(&"job_update".to_string()),
        "a consumed restart marker must not regenerate another restart: {kinds_after:?}"
    );
    Ok(())
}

/// The over-execution fix (JC-41): reconciliation honours the operator's
/// EVICT intent. `availability = 'drained'` alone cannot tell
/// `node drain --evict` from a plain cordon, so regenerating on
/// availability evicted allocations off nodes the operator had merely
/// cordoned. Only nodes drained WITH `--evict` may get a `node_drain`
/// eviction eval.
#[test]
fn drain_regeneration_honors_the_evict_intent() -> R<()> {
    let store = setup_store()?;
    store.insert_node(NodeMsg::from(test_node("cordoned")))?;
    store.insert_node(NodeMsg::from(test_node("evicted")))?;

    // Operator: plain no-evict cordon-drain vs. evicting drain.
    store.set_node_availability_with_evict("cordoned", "drained", false)?;
    store.set_node_availability_with_evict("evicted", "drained", true)?;
    clear_eval_queue(&store)?;

    acquisition_hook(&store)?;
    let drain_targets: Vec<String> = store
        .get_evals()?
        .into_iter()
        .filter(|e| e.kind == "node_drain")
        .map(|e| e.node_name)
        .collect();
    assert_eq!(
        drain_targets,
        vec!["evicted".to_string()],
        "only the evicting drain may regenerate an eviction pass"
    );

    // Uncordoning clears the intent: nothing regenerates afterwards.
    clear_eval_queue(&store)?;
    store.set_node_availability("evicted", "available")?;
    acquisition_hook(&store)?;
    assert!(
        !store.get_evals()?.iter().any(|e| e.kind == "node_drain"),
        "an uncordoned node must not regenerate drain evictions"
    );
    Ok(())
}

/// Stale evals from a previous leadership stint (superseded configs) are
/// purged at acquisition instead of being revived — and their absence
/// un-blocks regeneration for the same job. This is the one place evals are
/// deliberately DROPPED, and only because a later replicated write already
/// superseded what they described (JC-41).
#[test]
fn stale_evals_for_superseded_configs_are_purged_at_acquisition() -> R<()> {
    let store = setup_store()?;
    store.insert_node(NodeMsg::from(test_node("w1")))?;

    store.handle_job_submit(&job("staler", "v1", 1), &[submission("staler")])?;
    let v1 = store.get_job_config("staler")?.id();
    // A pending eval from the previous stint, referencing v1...
    store.enqueue_eval("job_submit", Some("staler"), Some(&v1), None, None)?;
    // ...and the job has since moved on to v2.
    let mut v2_job = job("staler", "v2", 1);
    v2_job.tasks[0].resources = json!({ "memory": 96 });
    store.handle_job_submit(&v2_job, &[submission("staler-v2")])?;
    let v2 = store.get_job_config("staler")?.id();

    let pruned = store.prune_stale_evals()?;
    assert_eq!(pruned, 1, "the superseded-config eval must be dropped");
    assert!(
        store.get_evals()?.is_empty(),
        "no stale eval may survive the acquisition purge"
    );

    // Regeneration now targets the LATEST config.
    store.regenerate_scheduling_evals()?;
    let evals = store.get_evals()?;
    assert!(
        evals.iter().all(|e| e.job_config_id == v2),
        "regenerated work must reference the latest config"
    );
    Ok(())
}

/// Evals reference `job` / `job_config` / `node` ADVISORILY — the cross-lane
/// FKs are gone (a control-lane `DELETE FROM job` must not cascade into this
/// scheduler-lane table, JC-32/JC-41). This pins the claim that a dangling
/// reference is absorbed WITHOUT a retry loop, on both defences:
///
/// - one referencing a config that no longer exists is dropped by
///   `prune_stale_evals` at leadership acquisition;
/// - one the prune cannot see (no config scope) fails in its handler, and a
///   failed eval is TERMINAL — the claim query only selects `pending`, so
///   nothing ever re-claims it. No requeue, no re-enqueue, no spin.
#[test]
fn evals_for_vanished_scopes_terminate_instead_of_spinning() -> R<()> {
    let store = setup_store()?;

    // (1) A config-scoped ghost: the prune sweeps it before anything runs.
    store.enqueue_eval(
        "job_reschedule",
        Some("ghost-job"),
        Some("ghost-config"),
        None,
        None,
    )?;
    assert_eq!(store.prune_stale_evals()?, 1);
    assert!(store.get_evals()?.is_empty());

    // (2) A ghost the prune cannot classify (no config scope): it must fail
    // once and stay failed.
    let orphan = store.enqueue_eval("job_reschedule", Some("ghost-job"), None, None, None)?;
    assert_eq!(store.prune_stale_evals()?, 0, "no config scope to judge");
    let processed = process_queue(&store)?;
    assert_eq!(
        processed,
        vec![("job_reschedule".to_string(), false)],
        "the handler must fail on a vanished scope"
    );

    let after: Vec<(String, String)> = store
        .get_evals()?
        .into_iter()
        .map(|eval| (eval.id, eval.status))
        .collect();
    assert_eq!(
        after,
        vec![(orphan.id, "failed".to_string())],
        "a failed eval is terminal"
    );
    assert!(
        store.claim_eval()?.is_none(),
        "a failed eval must never be re-claimed — that would be the spin"
    );
    // Neither recovery path revives it: the in-flight requeue only touches
    // `in_flight`, and reconciliation has no replicated state to work from.
    assert_eq!(store.requeue_in_flight_evals()?, 0);
    assert_eq!(acquisition_hook(&store)?, 0);
    assert!(store.claim_eval()?.is_none());
    Ok(())
}

/// `release_eval_for_retry` (the NotRaftLeader eval path): a claimed eval
/// goes back to pending — with its delay honored — instead of terminally
/// failing.
#[test]
fn released_evals_return_to_pending_and_are_reclaimable() -> R<()> {
    let store = setup_store()?;
    store.enqueue_eval("job_reschedule", Some("j"), Some("jc"), None, None)?;
    let eval = store.claim_eval()?.context("claimable")?;
    assert!(store.claim_eval()?.is_none(), "single eval, now in flight");

    // Delayed release: pending again but not claimable until ready_at.
    store.release_eval_for_retry(&eval.id, std::time::Duration::from_secs(3600))?;
    assert!(
        store.claim_eval()?.is_none(),
        "released eval must honor its retry delay"
    );
    assert_eq!(
        store.get_evals()?.first().map(|e| e.status.clone()),
        Some("pending".to_string())
    );

    // Immediate release: claimable right away.
    store.release_eval_for_retry(&eval.id, std::time::Duration::ZERO)?;
    let reclaimed = store.claim_eval()?.context("reclaimable after release")?;
    assert_eq!(reclaimed.id, eval.id);
    Ok(())
}

/// `complete_and_claim_next` (the fused eval-loop write): one call must
/// finish the eval it was given AND hand back the next claimable one, so
/// the scheduler lane pays one quorum round per eval instead of two.
///
/// The AWS phase-3 campaign put the scheduler lane, not the planner, at
/// the throughput ceiling — each eval spent ~86% of its wall time outside
/// the measured plan phases — so this fusion is a throughput change, and
/// these assertions are what keep it from silently regressing to two ops.
#[test]
fn complete_and_claim_next_finishes_one_eval_and_claims_the_next() -> R<()> {
    use legion::storage::events::drafts;
    use legion::storage::store::EvalCompletion;

    let store = setup_store()?;
    store.enqueue_eval("job_reschedule", Some("a"), Some("ac"), None, None)?;
    store.enqueue_eval("job_reschedule", Some("b"), Some("bc"), None, None)?;

    let first = store.claim_eval()?.context("first claimable")?;
    let completion = EvalCompletion::Finished {
        succeeded: true,
        event: drafts::eval_completed(&first, "done", std::time::Duration::from_millis(1)),
    };
    let second = store
        .complete_and_claim_next(&first.id, completion)?
        .context("the fused call must hand back the next eval")?;

    assert_ne!(second.id, first.id, "must claim a different eval");
    assert_eq!(second.status, "in_flight");
    // The first is gone (success deletes the row), and only the second
    // remains — in flight, not pending, so no other worker takes it.
    let remaining: Vec<(String, String)> = store
        .get_evals()?
        .into_iter()
        .map(|eval| (eval.id, eval.status))
        .collect();
    assert_eq!(
        remaining,
        vec![(second.id.clone(), "in_flight".to_string())]
    );

    // Draining the queue returns None rather than spinning.
    let completion = EvalCompletion::Finished {
        succeeded: true,
        event: drafts::eval_completed(&second, "done", std::time::Duration::from_millis(1)),
    };
    assert!(
        store
            .complete_and_claim_next(&second.id, completion)?
            .is_none(),
        "an empty queue must report None so the worker parks"
    );
    assert!(store.get_evals()?.is_empty());
    Ok(())
}

/// The `Released` arm of the fused write must behave exactly like the
/// standalone release it replaces: back to pending, delay honored, and
/// still able to claim a *different* eval in the same op.
#[test]
fn complete_and_claim_next_release_honors_the_retry_delay() -> R<()> {
    use legion::storage::store::EvalCompletion;

    let store = setup_store()?;
    store.enqueue_eval("job_reschedule", Some("a"), Some("ac"), None, None)?;
    store.enqueue_eval("job_reschedule", Some("b"), Some("bc"), None, None)?;

    let first = store.claim_eval()?.context("first claimable")?;
    let next = store.complete_and_claim_next(
        &first.id,
        EvalCompletion::Released {
            delay: std::time::Duration::from_secs(3600),
            event: None,
        },
    )?;

    // The released eval is pending-but-not-due, so the eval handed back
    // must be the OTHER one, never the one just released.
    let next = next.context("the other eval is still claimable")?;
    assert_ne!(
        next.id, first.id,
        "a delayed release must not re-claim itself"
    );

    let released = store
        .get_evals()?
        .into_iter()
        .find(|eval| eval.id == first.id)
        .context("released eval still present")?;
    assert_eq!(released.status, "pending");
    Ok(())
}

/// `enqueue_evals` writes every row in one op. The fan-out loops it
/// replaces (leadership regeneration, node-loss storms) were one quorum
/// round per row.
#[test]
fn enqueue_evals_creates_every_row_in_one_op() -> R<()> {
    use legion::storage::store::EvalRequest;

    let store = setup_store()?;
    let requests: Vec<EvalRequest> = (0..5)
        .map(|i| EvalRequest::for_job("job_reschedule", &format!("job-{i}"), &format!("cfg-{i}")))
        .collect();

    let created = store.enqueue_evals(requests)?;
    assert_eq!(created.len(), 5);
    assert_eq!(store.get_evals()?.len(), 5);

    // Every one is independently claimable, so batching changed only how
    // many times quorum was paid, not what was queued.
    let mut claimed = 0;
    while store.claim_eval()?.is_some() {
        claimed += 1;
    }
    assert_eq!(claimed, 5);

    // An empty batch proposes nothing rather than an empty op.
    assert!(store.enqueue_evals(Vec::new())?.is_empty());
    Ok(())
}
