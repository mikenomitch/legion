//! How claiming and enqueueing evals scale with queue depth (architecture
//! review finding SCH3).
//!
//! A claim runs under the store's claims mutex, so whatever it costs, every
//! eval worker waits for. Before the fix a claim counted every pending eval
//! and ran one duplicate-collapse query per claimed eval whose
//! `COALESCE(job_id, '')` predicate could not use an index, so both grew with
//! the queue. The measurement fills the queue with evals of distinct scopes
//! (nothing collapses, so every claim pays the full duplicate lookup), then
//! times fair-share batches and single enqueues at that depth.
//!
//! ```bash
//! cargo test --test integration eval_claim_scale:: -- --ignored --nocapture
//! LEGION_CLAIM_SCALE_DEPTH=10000 cargo test --test integration eval_claim_scale:: -- --ignored --nocapture
//! ```
//!
//! The non-ignored test keeps the claim's contract pinned at a depth the
//! default suite can afford.

use legion::prelude::R;
use legion::storage::store::{ClaimScope, EvalBatchLimits, EvalRequest, Store};
use legion::structs::config::ManagerConfigFull;
use std::time::{Duration, Instant};

fn store(dir: &tempfile::TempDir) -> R<Store> {
    Store::start_storage(&ManagerConfigFull {
        store_path: dir.path().join("store").to_str().unwrap().to_string(),
        ..Default::default()
    })
}

/// Fill the queue with `depth` ready evals, each for its own job, in
/// batches of one scheduler-lane op.
fn fill(store: &Store, depth: usize) -> R<()> {
    let mut next = 0usize;
    while next < depth {
        let batch_end = (next + 1_000).min(depth);
        let requests = (next..batch_end)
            .map(|index| EvalRequest {
                kind: "job_submit".to_string(),
                job_id: Some(format!("job-{index:06}")),
                job_config_id: Some(format!("jc-{index:06}")),
                node_id: None,
                priority: None,
                delay: Duration::ZERO,
            })
            .collect();
        store.enqueue_evals(requests)?;
        next = batch_end;
    }
    Ok(())
}

fn limits() -> EvalBatchLimits {
    EvalBatchLimits {
        max: 16,
        workers: 1,
    }
}

#[test]
#[ignore = "measurement; run with --ignored --nocapture (see module docs)"]
fn claim_cost_at_queue_depth() -> R<()> {
    let depth: usize = std::env::var("LEGION_CLAIM_SCALE_DEPTH")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(50_000);
    let dir = tempfile::tempdir()?;
    let store = store(&dir)?;

    let fill_started = Instant::now();
    fill(&store, depth)?;
    let fill_elapsed = fill_started.elapsed();

    // A fair-share batch of 16 at depth, released between rounds so every
    // round claims at the same depth.
    let rounds = 20;
    let mut batch_total = Duration::ZERO;
    for _ in 0..rounds {
        let started = Instant::now();
        let claimed = store.claim_evals(limits(), ClaimScope::Any)?;
        batch_total += started.elapsed();
        assert_eq!(claimed.len(), 16, "distinct scopes: nothing collapses");
        store.clear_eval_claims();
    }

    // The throttled worker's claim: only non-planning kinds, none of which
    // are queued, so it has to look past every planning eval to say so.
    let started = Instant::now();
    let non_planning = store.claim_evals(limits(), ClaimScope::NonPlanning)?;
    let non_planning_elapsed = started.elapsed();
    assert!(non_planning.is_empty());

    // One enqueue at depth: the op body's own cost, at this queue size.
    let samples = 20;
    let mut enqueue_total = Duration::ZERO;
    for index in 0..samples {
        let started = Instant::now();
        store.enqueue_eval(
            "job_submit",
            Some(&format!("extra-{index}")),
            Some(&format!("extra-jc-{index}")),
            None,
            None,
        )?;
        enqueue_total += started.elapsed();
    }

    println!(
        "eval_claim_scale depth={depth} fill_s={:.2} claim_batch16_ms={:.3} \
         claim_non_planning_ms={:.3} enqueue_one_ms={:.3}",
        fill_elapsed.as_secs_f64(),
        batch_total.as_secs_f64() * 1e3 / rounds as f64,
        non_planning_elapsed.as_secs_f64() * 1e3,
        enqueue_total.as_secs_f64() * 1e3 / samples as f64,
    );
    Ok(())
}

/// At a modest depth, a fair-share claim takes the head of the queue in
/// order, collapses true duplicates, and skips what it already holds.
#[test]
fn claims_take_the_head_in_order_and_collapse_duplicates() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = store(&dir)?;
    fill(&store, 200)?;
    // Two duplicates of the first job's eval, and one node-scoped pair.
    store.enqueue_eval(
        "job_submit",
        Some("job-000000"),
        Some("jc-000000"),
        None,
        None,
    )?;
    store.enqueue_eval(
        "job_submit",
        Some("job-000000"),
        Some("jc-000000"),
        None,
        None,
    )?;
    store.enqueue_eval("node_lost_reschedule", None, None, Some("n1"), None)?;
    store.enqueue_eval("node_lost_reschedule", None, None, Some("n1"), None)?;

    let first = store.claim_evals(limits(), ClaimScope::Any)?;
    assert_eq!(first.len(), 16);
    let ids: Vec<i64> = first.iter().map(|eval| eval.id.parse().unwrap()).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "claimed in id order at equal priority");
    assert_eq!(first[0].job_id, "job-000000");

    // Drain everything; the duplicates never come back as claims.
    let mut claimed = first.len();
    loop {
        let batch = store.claim_evals(limits(), ClaimScope::Any)?;
        if batch.is_empty() {
            break;
        }
        claimed += batch.len();
    }
    // 200 distinct + one node-scoped eval; the three duplicates collapsed.
    assert_eq!(claimed, 201);
    Ok(())
}
