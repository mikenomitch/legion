//! The autoscaler at 10,000 task policies (autoscaler-v2 plan acceptance P7;
//! docs/benchmarks.md, "The autoscaler at 10,000 policies").
//!
//! Opt-in (`--ignored`): it submits 10,000 autoscaled jobs to an in-process
//! manager, heartbeats usage for every allocation (every tenth job hot, so
//! about a thousand scale out), and runs one engine (one replica, the
//! built-in `legion` source) for several evaluation ticks, printing how
//! long each part of a tick takes. Run it with
//!
//! ```console
//! $ cargo test --release --test integration autoscaler_load:: -- --ignored --nocapture
//! ```
//!
//! `LEGION_AUTOSCALER_LOAD_POLICIES` changes the policy count (default
//! 10,000).

use std::time::{Duration, Instant};

use legion::apps::autoscaler::EngineOptions;
use legion::prelude::{json, R};
use legion::structs::{RunJobRequest, WatchScalingPoliciesRequest};

use crate::autoscaler::{Harness, T0, TICK_MS};

fn policies() -> usize {
    std::env::var("LEGION_AUTOSCALER_LOAD_POLICIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "load test: 10,000 autoscaled jobs; run with --ignored --nocapture"]
async fn one_replica_sustains_ten_thousand_task_policies_test() -> R<()> {
    let count = policies();
    let nodes = count.div_ceil(100).max(1);
    let mut h = Harness::start_with(nodes, 160).await?;
    // Every tenth job runs hot: its policy scales it out.
    let hot = |job: &str| job.ends_with('0');

    let started = Instant::now();
    for index in 0..count {
        let spec = json!({
            "name": format!("svc-{index:05}"),
            "task": {
                "name": "web", "runtime": "exec", "count": 1,
                "config": { "command": ["sleep", "3600"] },
                "resources": { "cpu": 20, "memory": 16 },
                "autoscale": { "max": 4 },
            }
        });
        h.client
            .run_job(RunJobRequest {
                parsed_jobspec: spec.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|status| anyhow::anyhow!("RunJob {index}: {}", status.message()))?;
        if index % 500 == 499 {
            h.drain_evals()?;
        }
    }
    h.drain_evals()?;
    let submitted = started.elapsed();
    eprintln!("load: {count} jobs submitted and placed on {nodes} nodes in {submitted:.1?}");

    // The watch's first set: every policy, resolved, in parts of at most
    // 1,000 (`more`).
    let watch_started = Instant::now();
    let mut stream = h
        .client
        .watch_scaling_policies(WatchScalingPoliciesRequest {
            deltas: true,
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("watch: {}", status.message()))?
        .into_inner();
    let (mut received, mut bytes, mut parts, mut largest) = (0, 0, 0, 0);
    loop {
        let part = stream
            .message()
            .await
            .map_err(|status| anyhow::anyhow!("watch: {}", status.message()))?
            .ok_or_else(|| anyhow::anyhow!("the watch ended"))?;
        let size = prost::Message::encoded_len(&part);
        received += part.policies.len();
        bytes += size;
        largest = largest.max(size);
        parts += 1;
        if !part.more {
            break;
        }
    }
    let watch_first = watch_started.elapsed();
    assert_eq!(received, count);
    drop(stream);
    eprintln!(
        "load: first WatchScalingPolicies set: {received} policies, {} KiB in {parts} \
         messages (largest {} KiB), in {watch_first:.1?}",
        bytes / 1024,
        largest / 1024
    );

    let (mut engine, watch) = h.engine(EngineOptions::default()).await?;
    let deadline = Instant::now() + Duration::from_secs(120);
    while engine.policy_count().unwrap_or(0) < count {
        anyhow::ensure!(Instant::now() < deadline, "not every policy arrived");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut ticks = Vec::new();
    for tick in 0..6 {
        let at = T0 + tick * TICK_MS;
        let heartbeat = Instant::now();
        h.report_all(|job| if hot(job) { 1.6 } else { 0.5 }, at).await?;
        let heartbeats = heartbeat.elapsed();
        let begun = Instant::now();
        let report = engine.tick(at).await;
        let took = begun.elapsed();
        assert!(report.errors.is_empty(), "tick {tick}: {:?}", report.errors.first());
        assert_eq!(report.evaluated, count);
        eprintln!(
            "load: tick {tick}: {count} policies evaluated in {took:.2?} \
             (inputs {:.2?}, decisions {:.2?}, report {:.2?}; {} scaled); usage heartbeats {heartbeats:.1?}",
            report.timings.inputs, report.timings.decide, report.timings.report, report.scaled
        );
        ticks.push(took);
        h.drain_evals()?;
    }
    watch.abort();
    ticks.sort();
    let median = ticks[ticks.len() / 2];
    let worst = *ticks.last().expect("ticks ran");
    eprintln!("load: tick median {median:.2?}, worst {worst:.2?}, against a 15s interval");
    assert!(
        worst < Duration::from_secs(15),
        "a tick of {count} policies took {worst:?}, longer than the 15s interval"
    );
    Ok(())
}
