//! The autoscaler scaled out (docs/autoscaling.md, "Replicas"; autoscaler-v2
//! plan §9 Phase 7, acceptance P7).
//!
//! Several engines run in process against one manager, each with its own
//! instance name and lease controller (with a 2-second lease TTL so the
//! test runs in seconds):
//!
//! - three replicas split the 64 shards and rebalance within about one
//!   lease TTL after one is killed (it stops renewing; nothing releases);
//! - two replicas forced to overlap (both evaluating everything, as during
//!   a lease hand-over) land exactly one `ScaleTask` per decision, and
//!   exactly one `target/scale` per node-group decision: the
//!   compare-and-set writes, not the leases, are what keep them single;
//! - the policy watch sends deltas, filtered to the shards asked for.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use legion::apps::autoscaler::shards::{LeaseTiming, Ownership};
use legion::apps::autoscaler::{Engine, EngineOptions};
use legion::autoscaler::shard::SHARDS;
use legion::autoscaler::shard::{shard_of, task_key};
use legion::prelude::{json, R};
use legion::structs::{
    ApplyScalingPolicyRequest, GetScaleHistoryRequest, ScalingPolicySet, StopJobRequest,
    WatchScalingPoliciesRequest,
};

use crate::autoscaler::{Harness, T0, TICK_MS};
use crate::node_groups::{boot, group_policy, have_python, Provider};

const TIMING: LeaseTiming = LeaseTiming {
    ttl: Duration::from_secs(2),
    renew: Duration::from_millis(500),
};

fn options(instance: &str) -> EngineOptions {
    EngineOptions {
        instance: instance.to_string(),
        ..Default::default()
    }
}

fn disjoint_cover(owned: &[Ownership]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for ownership in owned {
        let shards = ownership
            .shards
            .clone()
            .unwrap_or_else(|| (0..SHARDS).collect());
        for shard in shards {
            if !seen.insert(shard) {
                return Err(format!("shard {shard} owned twice: {owned:?}"));
            }
        }
    }
    if seen.len() != SHARDS as usize {
        return Err(format!(
            "{} of {SHARDS} shards owned: {owned:?}",
            seen.len()
        ));
    }
    Ok(())
}

/// Three replicas split the shards; one is killed; the other two cover it
/// within about one lease TTL.
#[tokio::test(flavor = "multi_thread")]
async fn replicas_split_the_shards_and_rebalance_when_one_dies_test() -> R<()> {
    let h = Harness::start(1).await?;
    let engines: Vec<Engine> = ["alloc-1", "alloc-2", "alloc-3"]
        .iter()
        .map(|name| Engine::new(h.client.clone(), options(name), T0))
        .collect();
    let mut leases: Vec<_> = engines.iter().map(|engine| engine.shards(TIMING)).collect();

    // A lone replica holds everything with one lease.
    let alone = leases[0].step().await;
    assert_eq!(alone.shards, None, "{alone:?}");

    let deadline = Instant::now() + Duration::from_secs(20);
    let owned = loop {
        let mut owned = Vec::new();
        for lease in leases.iter_mut() {
            owned.push(lease.step().await);
        }
        // ceil(64 / 3) = 22 each at most: 22, 22 and 20.
        if disjoint_cover(&owned).is_ok()
            && owned.iter().all(|o| {
                o.shards
                    .as_ref()
                    .is_some_and(|s| (20..=22).contains(&s.len()))
            })
        {
            break owned;
        }
        assert!(Instant::now() < deadline, "no balance: {owned:?}");
        tokio::time::sleep(TIMING.renew).await;
    };
    assert!(owned.iter().all(|o| o.members == 3), "{owned:?}");

    // Kill the third: it never renews again, and releases nothing.
    let killed = Instant::now();
    leases.truncate(2);
    let rebalanced = loop {
        let mut owned = Vec::new();
        for lease in leases.iter_mut() {
            owned.push(lease.step().await);
        }
        if disjoint_cover(&owned).is_ok() {
            break killed.elapsed();
        }
        assert!(
            killed.elapsed() < Duration::from_secs(20),
            "no rebalance after a replica died: {owned:?}"
        );
        tokio::time::sleep(TIMING.renew).await;
    };
    eprintln!(
        "rebalanced {rebalanced:?} after the kill (lease TTL {:?})",
        TIMING.ttl
    );
    // The dead replica's leases expire after one TTL; the survivors take
    // its shards on their next round (KV TTLs round to whole seconds).
    assert!(
        rebalanced <= TIMING.ttl + Duration::from_secs(1) + TIMING.renew * 2,
        "took {rebalanced:?}"
    );
    Ok(())
}

/// Two replicas evaluating the same task at once (a lease overlap): one
/// `ScaleTask` lands per decision.
#[tokio::test(flavor = "multi_thread")]
async fn an_overlap_lands_one_scale_task_per_decision_test() -> R<()> {
    let mut h = Harness::start(4).await?;
    h.run(json!({
        "name": "web",
        "task": {
            "name": "web", "runtime": "exec", "count": 2,
            "config": { "command": ["sleep", "3600"] },
            "resources": { "cpu": 500, "memory": 64 },
            "autoscale": true,
        }
    }))
    .await?;
    let (mut a, watch_a) = h.engine(options("alloc-1")).await?;
    let (mut b, watch_b) = h.engine(options("alloc-2")).await?;
    let mut at = T0;
    for tick in 0..6 {
        at += TICK_MS;
        h.report("web", 1.6, at).await?;
        let (ra, rb) = tokio::join!(a.tick(at), b.tick(at));
        h.drain_evals()?;
        assert!(
            ra.scaled + rb.scaled <= 1,
            "tick {tick}: both replicas scaled: {ra:?} {rb:?}"
        );
    }
    watch_a.abort();
    watch_b.abort();
    let history = h.history("web").await?;
    let writes: Vec<_> = history
        .iter()
        .filter(|entry| entry.source == "autoscaler" && entry.kind == "scaled")
        .collect();
    assert!(!writes.is_empty(), "{history:?}");
    let decisions: BTreeSet<&str> = writes.iter().map(|w| w.decision_id.as_str()).collect();
    assert_eq!(
        decisions.len(),
        writes.len(),
        "a decision landed twice: {writes:?}"
    );
    // Every write moved on from the one before: none repeated a count.
    for pair in writes.windows(2) {
        assert_ne!(
            (pair[0].from, pair[0].to),
            (pair[1].from, pair[1].to),
            "{writes:?}"
        );
    }
    Ok(())
}

/// Two replicas evaluating the same node group at once: exactly one
/// `target/scale` reaches the provider, and the loser follows the
/// winner's intent.
#[tokio::test(flavor = "multi_thread")]
async fn an_overlap_calls_the_provider_once_per_decision_test() -> R<()> {
    if !have_python() {
        eprintln!("python3 is not installed: skipping the scripted node provider");
        return Ok(());
    }
    let mut h = Harness::start(0).await?;
    let provider = Provider::new()?;
    h.install_plugins(provider.plugin_config())?;
    h.client
        .apply_scaling_policy(ApplyScalingPolicyRequest {
            spec_json: group_policy("workers", 6).to_string(),
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("apply: {}", status.message()))?;
    h.run(json!({
        "name": "batch",
        "task": {
            "name": "work", "runtime": "exec", "count": 4,
            "config": { "command": ["sleep", "3600"] },
            "resources": { "cpu": 2000, "memory": 1024 },
        }
    }))
    .await?;
    let (mut a, watch_a) = h.engine(provider.options("alloc-1")?).await?;
    let (mut b, watch_b) = h.engine(provider.options("alloc-2")?).await?;
    let (ra, rb) = tokio::join!(a.tick(T0), b.tick(T0));
    let scales: Vec<String> = provider
        .calls()?
        .into_iter()
        .filter(|call| call.starts_with("scale"))
        .collect();
    assert_eq!(scales.len(), 1, "{scales:?} {ra:?} {rb:?}");
    assert_eq!(ra.scaled + rb.scaled, 1, "{ra:?} {rb:?}");
    // The machines join; both replicas see the group settle, and neither
    // asks again.
    boot(&mut h, &provider).await?;
    h.drain_evals()?;
    for tick in 1..4 {
        let at = T0 + tick * TICK_MS;
        let _ = tokio::join!(a.tick(at), b.tick(at));
        h.drain_evals()?;
    }
    watch_a.abort();
    watch_b.abort();
    assert_eq!(
        provider
            .calls()?
            .iter()
            .filter(|c| c.starts_with("scale"))
            .count(),
        1
    );
    let history = h
        .client
        .get_scale_history(GetScaleHistoryRequest {
            policy: "workers".into(),
            limit: 100,
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("history: {}", status.message()))?
        .into_inner()
        .entries;
    assert_eq!(
        history
            .iter()
            .filter(|entry| entry.kind == "scaled")
            .count(),
        1,
        "{history:?}"
    );
    Ok(())
}

async fn next_set(stream: &mut tonic::Streaming<ScalingPolicySet>) -> R<ScalingPolicySet> {
    tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .map_err(|_| anyhow::anyhow!("no message in 10s"))?
        .map_err(|status| anyhow::anyhow!("watch: {}", status.message()))?
        .ok_or_else(|| anyhow::anyhow!("the watch ended"))
}

/// The policy watch with `deltas` sends the full set once, then only what
/// changed; with `shard_count` and `shards`, only those shards' policies.
#[tokio::test(flavor = "multi_thread")]
async fn the_policy_watch_sends_deltas_for_its_shards_test() -> R<()> {
    let mut h = Harness::start(4).await?;
    let spec = |name: &str| {
        json!({
            "name": name,
            "task": {
                "name": "web", "runtime": "exec", "count": 1,
                "config": { "command": ["sleep", "3600"] },
                "resources": { "cpu": 100, "memory": 32 },
                "autoscale": true,
            }
        })
    };
    for name in ["a", "b"] {
        h.run(spec(name)).await?;
    }
    let mut stream = h
        .client
        .watch_scaling_policies(WatchScalingPoliciesRequest {
            deltas: true,
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("watch: {}", status.message()))?
        .into_inner();
    let first = next_set(&mut stream).await?;
    assert!(first.full && !first.more);
    assert_eq!(first.policies.len(), 2);

    // A new job: one policy in the delta, nothing removed.
    h.run(spec("c")).await?;
    let delta = next_set(&mut stream).await?;
    assert!(!delta.full, "{delta:?}");
    assert_eq!(delta.policies.len(), 1, "{delta:?}");
    assert!(delta.removed.is_empty());
    let c_key = task_key(
        &delta.policies[0].job_id,
        &delta.policies[0].task_group,
        &delta.policies[0].task_name,
    );

    // Stopped: its key in `removed`.
    h.client
        .stop_job(StopJobRequest {
            name: "c".into(),
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("stop: {}", status.message()))?;
    h.drain_evals()?;
    let delta = next_set(&mut stream).await?;
    assert!(delta.policies.is_empty(), "{delta:?}");
    assert_eq!(delta.removed, vec![c_key]);
    drop(stream);

    // A shard filter: only the policy whose key hashes to the shard asked.
    let a = first
        .policies
        .iter()
        .find(|policy| policy.job_id.ends_with('a'))
        .expect("a");
    let shard = shard_of(&task_key(&a.job_id, &a.task_group, &a.task_name), SHARDS);
    let mut filtered = h
        .client
        .watch_scaling_policies(WatchScalingPoliciesRequest {
            shard_count: SHARDS,
            shards: vec![shard],
            deltas: true,
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("watch: {}", status.message()))?
        .into_inner();
    let set = next_set(&mut filtered).await?;
    assert!(set.policies.iter().any(|p| p.job_id == a.job_id), "{set:?}");
    assert!(set
        .policies
        .iter()
        .all(|p| { shard_of(&task_key(&p.job_id, &p.task_group, &p.task_name), SHARDS) == shard }));
    // A shard out of range is refused.
    let status = h
        .client
        .watch_scaling_policies(WatchScalingPoliciesRequest {
            shard_count: SHARDS,
            shards: vec![SHARDS],
            ..Default::default()
        })
        .await
        .expect_err("shard 64 of 64");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    Ok(())
}
