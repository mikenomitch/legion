//! Legion's integration tests, linked as one test binary.
//!
//! Every file here used to be its own `tests/*.rs` crate, and each of those
//! linked all of Legion and its ~520 dependencies: ~200 MB and a full link per
//! file, every time the library changed. As modules of one crate they share a
//! single link.
//!
//! Run one former file with a module filter:
//!
//! ```text
//! cargo test --test integration acl_endpoints::
//! cargo test --test integration scheduler_benchmark_slo:: -- --ignored --nocapture
//! ```
//!
//! # One process
//!
//! The tests here run on libtest's thread pool inside one process, beside the
//! tests of every other module. A test that depends on process-wide state —
//! exact deltas of the global metrics, the scheduler's concurrency gate, a
//! `#[global_allocator]` — does not belong here; give it its own `tests/*.rs`
//! file, as `tests/raft_lifecycle_ops.rs` and
//! `tests/adaptive_scheduler_concurrency.rs` do. Tests that bring up full
//! raft clusters take [`support::cluster_gate`] so only one runs at a time.

mod benchmark_support;
mod support;

mod acl_default_on;
mod acl_endpoints;
mod apple_container_idle;
mod archive_integration;
mod artifact_staging_integration;
mod blocked_evals;
mod capacity_retry;
mod cli_closed_pipe;
mod config_jobs;
mod content_locality;
mod edge_boot_integration;
mod eval_loop;
mod eval_regen_failover;
mod exec_session_benchmark;
mod fork_integration;
mod gang_scheduling;
mod gated_container_default;
mod gated_sandbox_default;
mod heartbeat_scale_benchmark;
mod index_truncation_spread;
mod instance_scale_measure;
mod kv_encryption;
mod manager_shutdown;
mod metrics_endpoint;
mod node_drain;
mod node_liveness;
mod node_pressure;
mod node_usage;
mod plan_contention;
mod published_resume_integration;
mod raft_apply_roundtrip;
mod raft_cascade_delete;
mod raft_changeset_capture;
mod raft_durability;
mod raft_eval_failover;
mod raft_follower_writes;
mod raft_lanes;
mod raft_lanes_durability;
mod raft_membership;
mod raft_ownership_guard;
mod raft_perf_smoke;
mod raft_seam;
mod raft_snapshot_recovery;
mod raft_topology_stamp;
mod raft_upgrade_bootstrap;
mod ratify_throughput_measure;
mod restore_placement_indexed;
mod rollout_integration;
mod sandbox_bridge_placement;
mod scaling_inputs;
mod scheduler_benchmark_slo;
mod scheduler_integration;
mod scheduler_scale_benchmark;
mod scheduling_index;
mod session_integration;
mod snapshot_integration;
mod spread_under_raft;
mod suspend_integration;
mod volume_integration;
mod wake_integration;
mod work_stream;
mod worker_plane_classes;
mod worker_plane_e2e;
mod worker_simulation;
