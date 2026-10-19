//! What a manager's session index costs at fleet scale: the memory each
//! running session holds, and the CPU of the heartbeats that keep it current.
//!
//! The session index is the only per-session state a control plane keeps for
//! session mode (docs/session-mode.md): nothing is in the replicated store,
//! so these two numbers are what bound how many sessions a cluster can run.
//!
//! Measurement only, so it is ignored by default:
//!
//! ```text
//! cargo test --release --test session_index_scale_measure -- --ignored --nocapture
//! ```
//!
//! Memory comes from a counting allocator wrapped around the system one, so
//! it is the index's own heap, not the process's resident set.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::{Duration, Instant};

use legion::storage::session_index::SessionIndex;
use legion::structs::{QuerySessionsRequest, SessionReport, SessionStatus};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// A session as a node reports it: the benchmark rig's sandbox task, with
/// a node-minted id of the real length.
fn status(node: usize, index: usize) -> SessionStatus {
    SessionStatus {
        session_id: format!("vm-{node:08x}.{index:026}"),
        job_id: "bench-vm".into(),
        job_config_id: "bench-vm-7c1f0d2e9a4b".into(),
        task_name: "vm".into(),
        envelope_allocation_id: 1_000 + node as i64,
        state: "running".into(),
        lease_issued_at_unix_ms: 1_791_950_000_000 + index as i64,
        resources: r#"{"cpu":60,"memory":128}"#.into(),
        generation: 1,
        origin: "manager".into(),
        ticket_jti: String::new(),
        checkpoint: None,
    }
}

fn full_report(node: usize, sessions: usize) -> SessionReport {
    SessionReport {
        epoch: 1,
        base_epoch: 0,
        full: true,
        upserts: (0..sessions).map(|index| status(node, index)).collect(),
        removed: Vec::new(),
    }
}

/// One fleet shape: `nodes` nodes running `per_node` sessions each.
fn measure(nodes: usize, per_node: usize) {
    let index = SessionIndex::new();
    let now = Instant::now();
    let before = LIVE.load(Ordering::Relaxed);
    let mut full_ingest = Duration::ZERO;
    for node in 0..nodes {
        let report = full_report(node, per_node);
        let started = Instant::now();
        index.ingest(&format!("node-{node:05}"), &report, now);
        full_ingest += started.elapsed();
    }
    let total = nodes * per_node;
    let bytes = LIVE.load(Ordering::Relaxed) - before;

    // Steady state: every node's heartbeat carries a small delta (two
    // sessions started, two ended), as a node churning sessions sends.
    let mut epochs = vec![1u64; nodes];
    let rounds = 3;
    let started = Instant::now();
    for round in 0..rounds {
        for (node, epoch) in epochs.iter_mut().enumerate() {
            let base = *epoch;
            let fresh = per_node + round * 2;
            let report = SessionReport {
                epoch: base + 1,
                base_epoch: base,
                full: false,
                upserts: vec![status(node, fresh), status(node, fresh + 1)],
                removed: vec![
                    status(node, round * 2).session_id,
                    status(node, round * 2 + 1).session_id,
                ],
            };
            index.ingest(&format!("node-{node:05}"), &report, now);
            *epoch = base + 1;
        }
    }
    let beats = (nodes * rounds) as f64;
    let per_beat = started.elapsed().as_secs_f64() / beats;

    // The router's room read (once a second per session task per manager).
    let started = Instant::now();
    let reply = index.query(
        &QuerySessionsRequest {
            job_id: "bench-vm".into(),
            task_name: "vm".into(),
            usage_only: true,
            ..Default::default()
        },
        now,
    );
    let usage_read = started.elapsed();
    assert_eq!(reply.node_usage.len(), nodes);

    println!(
        "{nodes:>6} nodes x {per_node:>5} sessions = {total:>9}: {:>6.0} B/session, {:>7.1} MiB; \
         full report {:>6.1} us/session; delta heartbeat {:>6.1} us ({:>8.0} beats/s on one core); \
         usage read {:>7.2} ms",
        bytes as f64 / total as f64,
        bytes as f64 / (1024.0 * 1024.0),
        full_ingest.as_secs_f64() * 1e6 / total as f64,
        per_beat * 1e6,
        1.0 / per_beat,
        usage_read.as_secs_f64() * 1e3,
    );
}

#[test]
#[ignore = "measurement; prints numbers"]
fn session_index_scale_measure() {
    for (nodes, per_node) in [
        (100, 100),
        (1_000, 100),
        (1_000, 1_000),
        (10_000, 100),
        (2_000, 500),
    ] {
        measure(nodes, per_node);
    }
}
