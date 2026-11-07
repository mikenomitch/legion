//! Scaling-input rollups (docs/autoscaling.md, "Scaling inputs";
//! `src/manager/scaling_inputs.rs`): what each step costs at fleet sizes the
//! autoscaler plan sizes for (§6.7: 10,000 tasks on about 3 nodes each).
//!
//! Groups:
//! - `edge_record`: the one thing the heartbeat path gained, folding one
//!   heartbeat's service-graph samples into the pending map. Per heartbeat.
//! - `rollup_build`: one manager's 10-second fold of its nodes' latest
//!   reports into per-(node, task) rollups, with every allocation already
//!   cached (the steady state).
//! - `sender_sync`: building the delta sync for a steady fleet (nothing
//!   changed: the empty confirming delta) and for a fleet where every rollup
//!   changed (the worst case, a full re-send).
//! - `leader_merge`: the status-lane leader applying one peer's full sync,
//!   then a steady delta; and its 10-second history pass over every task.
//! - `totals_read`: one `GetScalingInputs` target's sum across its nodes.
//!
//! ```bash
//! cargo bench --bench scaling_rollup_bench
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use legion::manager::scaling_inputs::{
    build_rollups, AllocMeta, BuildContext, CpuReservation, EdgeAccumulator, NodeSample, RollupKey,
    RollupMerge, RollupSender, TargetKey,
};
use legion::structs::NetEdgeSample;

const MIB: u64 = 1024 * 1024;

/// `nodes` nodes, `per_node` replicas on each, tasks spread so each task
/// runs on about three nodes.
struct Fleet {
    samples: Vec<NodeSample>,
    allocations: HashMap<i64, Arc<AllocMeta>>,
    clocks: HashMap<String, Option<f64>>,
}

fn fleet(nodes: usize, per_node: usize) -> Fleet {
    let tasks = (nodes * per_node / 3).max(1);
    let mut allocations = HashMap::new();
    let mut samples = Vec::with_capacity(nodes);
    let mut clocks = HashMap::new();
    let mut id: i64 = 0;
    for node in 0..nodes {
        let node_id = format!("node-{node:05}");
        clocks.insert(node_id.clone(), Some(2400.0));
        let mut rows = Vec::with_capacity(per_node);
        for slot in 0..per_node {
            id += 1;
            let task = (node * per_node + slot) % tasks;
            allocations.insert(
                id,
                Arc::new(AllocMeta {
                    target: TargetKey::new(&format!("job-{task:05}"), "default", "web"),
                    cpu: CpuReservation::Mhz(1000.0),
                    memory_mib: 512.0,
                    upstreams: HashMap::new(),
                }),
            );
            rows.push((id, 100 + (id as u64 % 900), (200 + id as u64 % 300) * MIB));
        }
        samples.push(NodeSample {
            node_id,
            sampled_at_unix_ms: 1_700_000_000_000,
            allocations: rows,
        });
    }
    Fleet {
        samples,
        allocations,
        clocks,
    }
}

fn build(fleet: &Fleet) -> HashMap<RollupKey, legion::manager::scaling_inputs::Rollup> {
    let services = HashMap::new();
    build_rollups(
        &fleet.samples,
        &HashMap::new(),
        &BuildContext {
            allocations: &fleet.allocations,
            mhz_per_core: &fleet.clocks,
            services: &services,
            edge_window_ms: 10_000,
            now_unix_ms: 1_700_000_000_000,
        },
    )
}

fn edge_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("edge_record");
    for samples in [4usize, 32] {
        let batch: Vec<NetEdgeSample> = (0..samples)
            .map(|n| NetEdgeSample {
                allocation_id: n as i64,
                service: format!("svc-{}", n % 4),
                protocol: "http".into(),
                requests: 10,
                errors: 1,
                latency_ms_sum: 200,
                latency_ms_max: 40,
                ..Default::default()
            })
            .collect();
        group.throughput(Throughput::Elements(samples as u64));
        group.bench_with_input(BenchmarkId::new("samples", samples), &batch, |b, batch| {
            let mut accumulator = EdgeAccumulator::default();
            let mut beats = 0u32;
            b.iter(|| {
                accumulator.record("node-00001", batch);
                beats += 1;
                // Drain as the 10-second fold would, so the map stays at
                // its steady-state size rather than growing for the run.
                if beats.is_multiple_of(5) {
                    accumulator.drain();
                }
            })
        });
    }
    group.finish();
}

fn rollup_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("rollup_build");
    group.sample_size(20);
    for (nodes, per_node) in [(333usize, 10usize), (3_333, 10)] {
        let fleet = fleet(nodes, per_node);
        group.throughput(Throughput::Elements((nodes * per_node) as u64));
        group.bench_function(BenchmarkId::new("replicas", nodes * per_node), |b| {
            b.iter(|| build(&fleet))
        });
    }
    group.finish();
}

fn sender_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("sender_sync");
    group.sample_size(20);
    let fleet = fleet(3_333, 10);
    let current = build(&fleet);
    let mut changed = current.clone();
    for rollup in changed.values_mut() {
        rollup.cpu_used_millicores += 1;
    }
    group.throughput(Throughput::Elements(current.len() as u64));
    group.bench_function(BenchmarkId::new("steady", current.len()), |b| {
        let mut sender = RollupSender::new(1);
        sender.next_sync(&current);
        sender.acked();
        b.iter(|| {
            let sync = sender.next_sync(&current);
            sender.acked();
            sync
        })
    });
    group.bench_function(BenchmarkId::new("all_changed", current.len()), |b| {
        let mut sender = RollupSender::new(1);
        sender.next_sync(&current);
        sender.acked();
        let mut flip = false;
        b.iter(|| {
            flip = !flip;
            let sync = sender.next_sync(if flip { &changed } else { &current });
            sender.acked();
            sync
        })
    });
    group.finish();
}

fn leader_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("leader_merge");
    group.sample_size(20);
    let fleet = fleet(3_333, 10);
    let current = build(&fleet);
    let mut sender = RollupSender::new(7);
    let full = sender.next_sync(&current);
    sender.acked();
    let steady = sender.next_sync(&current);
    group.throughput(Throughput::Elements(current.len() as u64));
    group.bench_function(BenchmarkId::new("full_sync", current.len()), |b| {
        b.iter(|| {
            let mut merge = RollupMerge::default();
            merge.become_responsible(Instant::now(), true);
            merge.apply("mgr-b", &full, Instant::now())
        })
    });
    group.bench_function(BenchmarkId::new("steady_delta", current.len()), |b| {
        let mut merge = RollupMerge::default();
        merge.become_responsible(Instant::now(), true);
        merge.apply("mgr-b", &full, Instant::now());
        let mut seq = steady.seq;
        let mut delta = steady.clone();
        b.iter(|| {
            delta.base_seq = seq - 1;
            delta.seq = seq;
            seq += 1;
            merge.apply("mgr-b", &delta, Instant::now())
        })
    });
    group.bench_function(BenchmarkId::new("history_pass", current.len()), |b| {
        let mut merge = RollupMerge::default();
        merge.become_responsible(Instant::now(), true);
        merge.apply("mgr-b", &full, Instant::now());
        let mut at = 1_700_000_000_000i64;
        b.iter(|| {
            at += 10_000;
            merge.record_history(Instant::now(), at)
        })
    });
    group.finish();
}

fn totals_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("totals_read");
    let fleet = fleet(3_333, 10);
    let current = build(&fleet);
    let mut merge = RollupMerge::default();
    merge.become_responsible(Instant::now(), true);
    merge.apply_local("leader", &current, Instant::now());
    let target = TargetKey::new("job-00042", "default", "web");
    group.bench_function("one_target", |b| {
        b.iter(|| merge.totals(&target, Instant::now()))
    });
    group.finish();
}

criterion_group!(
    benches,
    edge_record,
    rollup_build,
    sender_sync,
    leader_merge,
    totals_read
);
criterion_main!(benches);
