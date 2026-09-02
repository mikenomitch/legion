use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::{Builder, TempDir};

use legion::storage::store::{PlanApplyOutcome, Store};
use legion::structs::config::ManagerConfigFull;

const WORKER_COUNTS: &[usize] = &[1, 4, 8];
const OPS_PER_WORKER: usize = 100;

fn percentile_us(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64 - 1.0) * pct).round() as usize;
    sorted[rank]
}

/// The `TempDir` guard rides alongside the store so the on-disk store is
/// deleted when the iteration's tuple drops. The previous `.keep()` leaked
/// one store directory per iteration, which filled the disk over a long run
/// and skewed late samples with filesystem pressure.
fn setup_store() -> anyhow::Result<(Store, TempDir)> {
    let store_dir = Builder::new().prefix("legion-eval-path-bench-").tempdir()?;
    let store_path = store_dir
        .path()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Error parsing store path"))?
        .to_string();

    let store_config = ManagerConfigFull {
        store_path,
        scheduling_policy: "bin_pack".into(),
        task_resource_defaults: Some(json!({
            "cpu": 0,
            "memory": 0,
            "allocations": 1
        })),
        ..Default::default()
    };

    let store = Store::start_storage(&store_config)?;
    store.reset_all_tables()?;
    Ok((store, store_dir))
}

fn eval_critical_path_latency_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_claim_apply_complete_latency");
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(8));

    for &workers in WORKER_COUNTS {
        group.bench_function(format!("workers_{workers}"), |b| {
            // Store creation sits in the SETUP closure, not the timed
            // routine: it runs the full migration suite (twice, via
            // `reset_all_tables`), so timing it would charge every
            // migration ever appended to a benchmark named
            // claim/apply/complete latency. The routine returns the store
            // and its tempdir so their drops (connection close + directory
            // removal) land outside the measurement too.
            b.iter_batched(
                || setup_store().expect("store setup"),
                |(store, dir)| {
                    let store = Arc::new(store);
                    let barrier = Arc::new(Barrier::new(workers));
                    let samples = Arc::new(Mutex::new(Vec::<u128>::with_capacity(
                        workers * OPS_PER_WORKER,
                    )));

                    let mut joins = Vec::with_capacity(workers);
                    for worker_idx in 0..workers {
                        let store = Arc::clone(&store);
                        let barrier = Arc::clone(&barrier);
                        let samples = Arc::clone(&samples);
                        joins.push(thread::spawn(move || {
                            barrier.wait();
                            for op in 0..OPS_PER_WORKER {
                                store
                                    .enqueue_eval("bench", None, None, None, Some(10))
                                    .expect("enqueue eval");
                                let job_config_id = format!("bench-job-{}", worker_idx);
                                let expected_versions = store
                                    .allocation_version_vector(&job_config_id, &HashSet::new())
                                    .expect("version vector");
                                // Deliberately the STAGED two-step form (insert,
                                // then apply after the claim) so the claim sits
                                // between two store writes, maximizing write-lock
                                // interleaving. The production scheduler path
                                // fuses these into one op
                                // (`Store::submit_and_apply_plan`).
                                let submission = store
                                    .insert_plan_submission(
                                        &job_config_id,
                                        "fresh",
                                        &[],
                                        &expected_versions,
                                    )
                                    .expect("insert plan submission");

                                let started = Instant::now();
                                let claimed = loop {
                                    if let Some(eval) = store.claim_eval().expect("claim eval") {
                                        break eval;
                                    }
                                    std::thread::sleep(Duration::from_micros(200));
                                };
                                let apply_result = store
                                    .apply_plan_submission(&submission)
                                    .expect("apply plan submission");
                                assert!(
                                    matches!(apply_result, PlanApplyOutcome::Applied(_)),
                                    "plan submission should apply with empty placements"
                                );
                                store
                                    .complete_eval(&claimed.id, true)
                                    .expect("complete eval");
                                let elapsed = started.elapsed().as_micros();

                                if op % 2 == 0 {
                                    let _ =
                                        store.get_evals().expect("concurrent read during writes");
                                }

                                samples.lock().expect("samples lock").push(elapsed);
                            }
                        }));
                    }

                    for handle in joins {
                        handle.join().expect("worker join");
                    }

                    let mut samples = samples.lock().expect("samples lock").clone();
                    samples.sort_unstable();
                    let p50 = percentile_us(&samples, 0.50);
                    let p99 = percentile_us(&samples, 0.99);
                    assert!(p50 > 0, "p50 should be measurable");
                    assert!(p99 >= p50, "p99 should be >= p50");
                    (store, dir)
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, eval_critical_path_latency_benchmark);
criterion_main!(benches);
