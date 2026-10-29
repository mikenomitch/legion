//! Node liveness and heartbeat cadence, end to end against a real store.
//!
//! Covers the cases from
//! `internal_docs/plans/liveness-off-the-raft-path-plan.md` §9 and
//! `internal_docs/plans/heartbeat-cadence-at-scale-plan.md` §9 that need a
//! store rather than a snapshot. The pure arithmetic — the ladder, the
//! freshness ratio, the suppression rules — is unit-tested next to the code in
//! `src/manager/cadence.rs` and `src/storage/liveness.rs`; the raft-level
//! election and digest cases live in `tests/integration/raft_lanes.rs`.
//!
//! The tests inject `Instant`s rather than sleeping: a liveness window is
//! seconds long, and a suite that waits them out is a suite nobody runs.

use legion::manager::cadence::{
    cadence_report, decide_lost_nodes, freshness_ratio, LossContext, SuppressionReason,
};
use legion::manager::daemon::liveness_snapshot;
use legion::prelude::R;
use legion::storage::store::Store;
use legion::structs::{
    HeartbeatConfigFull, LivenessConfigFull, ManagerConfigFull, NodeMsg, PersistenceConfigFull,
};
use serde_json::json;
use std::time::{Duration, Instant};

fn setup_store(missed_beats: u32) -> R<Store> {
    // The directory must outlive the store; the scratch root removes it when
    // the test process exits.
    let dir = tempfile::tempdir_in(crate::support::scratch_root())?.keep();
    let config = ManagerConfigFull {
        store_path: dir.join("store.sqlite").to_string_lossy().into_owned(),
        persistence: PersistenceConfigFull::default(),
        heartbeat: HeartbeatConfigFull {
            missed_beats_before_lost: missed_beats,
            ..HeartbeatConfigFull::default()
        },
        ..ManagerConfigFull::default()
    };
    let store = Store::start_storage(&config)?;
    store.reset_all_tables()?;
    Ok(store)
}

/// A registered node reporting `heartbeat_seconds` as its cadence, the way a
/// real worker does in its registration `config_attrs`.
fn register(store: &Store, id: &str, heartbeat_seconds: Option<u64>) -> R<()> {
    let config_attrs = match heartbeat_seconds {
        Some(seconds) => json!({ "heartbeatSeconds": seconds }).to_string(),
        None => "{}".to_string(),
    };
    store.insert_node(NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs,
        fingerprint_attrs: "{}".into(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    })?;
    Ok(())
}

fn settled() -> LossContext {
    LossContext {
        responsible_for: Duration::from_secs(3600),
        ..Default::default()
    }
}

/// Rewind a node's observed beat so it looks silent, without waiting.
fn silent_for(store: &Store, id: &str, seconds: u64) {
    store.liveness().observe_at(
        id,
        Instant::now() - Duration::from_secs(seconds),
        time::OffsetDateTime::now_utc().unix_timestamp() - seconds as i64,
    );
}

/// A steady-state heartbeat writes nothing. This is the whole performance
/// claim of the liveness plan, asserted as a property rather than a number:
/// the persisted clock does not move, and the beat is visible only in memory.
#[test]
fn a_steady_state_heartbeat_reaches_no_replicated_state() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "node-1", None)?;
    store.liveness().become_responsible(&["node-1".into()]);

    for _ in 0..50 {
        store.update_node_heartbeat("node-1".to_string())?;
    }
    let persisted: Option<i64> = store
        .active_node_liveness_rows()?
        .into_iter()
        .find(|row| row.id == "node-1")
        .and_then(|row| row.last_heartbeat_unix);
    assert_eq!(
        persisted, None,
        "50 heartbeats must not stamp the replicated heartbeat clock"
    );
    assert!(store.liveness().last_beat("node-1").is_some());

    // The display checkpoint is what makes it readable, and it records the
    // OBSERVED time rather than the write time.
    let beats: Vec<(String, i64)> = store
        .liveness()
        .snapshot()
        .into_iter()
        .map(|(id, beat)| (id, beat.unix))
        .collect();
    let observed = beats[0].1;
    store.checkpoint_node_heartbeats(&beats)?;
    let persisted = store
        .active_node_liveness_rows()?
        .into_iter()
        .find(|row| row.id == "node-1")
        .and_then(|row| row.last_heartbeat_unix);
    assert_eq!(persisted, Some(observed));
    Ok(())
}

/// A heartbeat from a node the store believes is lost is NEW information, so
/// it stays replicated — and it lands the node back in `active`, where the
/// liveness scan can see it.
#[test]
fn a_recovered_node_transitions_through_replicated_state() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "node-1", None)?;
    store.mark_nodes_as_lost(&["node-1".to_string()])?;
    assert_eq!(store.get_node("node-1")?.status, "lost");
    assert!(store.active_node_liveness_rows()?.is_empty());

    store.update_node_heartbeat("node-1".to_string())?;
    assert_eq!(
        store.get_node("node-1")?.status,
        "active",
        "the recovery must be visible in replicated state, not only in memory"
    );
    assert_eq!(store.node_lost_at("node-1")?, None);
    assert_eq!(store.active_node_liveness_rows()?.len(), 1);
    Ok(())
}

/// A node that comes back must ACTUALLY come back — schedulable, not merely
/// `active`.
///
/// Node loss is a cross-lane composite (status lane says `lost`, control lane
/// says `unavailable`), and the scheduler gates placement on `availability`.
/// Recovering only the status half left a returned node reading `active` in
/// `legion node list` while never receiving work again.
#[test]
fn a_returning_node_becomes_schedulable_again() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "node-1", None)?;
    assert!(store.available_node_ids()?.contains("node-1"));

    store.mark_nodes_as_lost(&["node-1".to_string()])?;
    assert_eq!(store.get_node("node-1")?.availability, "unavailable");
    assert!(
        !store.available_node_ids()?.contains("node-1"),
        "a lost node must not keep receiving placements"
    );

    // The worker simply resumes heartbeating — it does NOT restart, so it does
    // not re-register. That is the ordinary case after a network partition
    // heals, and it has to be enough.
    store.update_node_heartbeat("node-1".to_string())?;
    let node = store.get_node("node-1")?;
    assert_eq!(node.status, "active");
    assert_eq!(
        node.availability, "available",
        "a recovered node that is active but unschedulable is a silent hole in the fleet"
    );
    assert!(store.available_node_ids()?.contains("node-1"));
    Ok(())
}

/// ...but recovery must not undo an OPERATOR's cordon. A heartbeat may clear
/// the loss the manager set; it may never clear intent a human expressed.
#[test]
fn recovery_does_not_uncordon_a_drained_node() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "node-1", None)?;
    store.set_node_availability("node-1", "drained")?;
    assert_eq!(store.get_node("node-1")?.availability, "drained");

    // A drained node still heartbeats, and those beats must leave the drain
    // alone — including the beat that would otherwise look like a recovery.
    store.update_node_heartbeat("node-1".to_string())?;
    assert_eq!(
        store.get_node("node-1")?.availability,
        "drained",
        "a heartbeat must never un-cordon a node"
    );
    assert!(!store.available_node_ids()?.contains("node-1"));
    Ok(())
}

/// A heartbeat from a node that was never registered is still rejected: a
/// ghost worker must not think it is healthy, because its stateful self-fence
/// clock feeds off heartbeat success.
#[test]
fn a_ghost_node_still_gets_not_found() -> R<()> {
    let store = setup_store(5)?;
    let err = store
        .update_node_heartbeat("never-registered".to_string())
        .expect_err("a ghost heartbeat must fail");
    assert!(
        format!("{err:#}").contains("never-registered"),
        "the rejection must name the node: {err:#}"
    );
    assert_eq!(store.liveness().tracked(), 0);
    Ok(())
}

/// Two worker types at 2s and 60s coexist, each with its own window. This is
/// the test that would have failed under one global cutoff.
#[test]
fn per_worker_type_cadences_get_their_own_windows() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "fast", Some(2))?;
    register(&store, "slow", Some(60))?;
    store
        .liveness()
        .become_responsible(&["fast".into(), "slow".into()]);

    silent_for(&store, "fast", 55);
    silent_for(&store, "slow", 55);
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    let window = |id: &str| {
        snapshot
            .iter()
            .find(|node| node.id == id)
            .expect("node in snapshot")
            .window
    };
    assert_eq!(window("fast"), Duration::from_secs(10));
    assert_eq!(window("slow"), Duration::from_secs(300));
    assert_eq!(
        decide_lost_nodes(&snapshot, &settled()).lost,
        vec!["fast".to_string()],
        "A is lost at 10s; B is alive at 55s"
    );

    // ...and B goes at 305.
    silent_for(&store, "slow", 305);
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    let mut lost = decide_lost_nodes(&snapshot, &settled()).lost;
    lost.sort();
    assert_eq!(lost, vec!["fast".to_string(), "slow".to_string()]);
    Ok(())
}

/// A row written before `lost_after_seconds` existed behaves exactly as it
/// always did: `DEF_LOST_NODE_AT_SEC`, no special case anywhere.
#[test]
fn a_null_lost_window_falls_back_to_the_historical_constant() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "legacy", None)?;
    // A pre-migration row: no recorded window at all.
    store.set_node_lost_windows(&[("legacy".to_string(), 0)])?;
    store.liveness().become_responsible(&["legacy".into()]);
    silent_for(&store, "legacy", 9);
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    assert_eq!(
        snapshot[0].window,
        Duration::from_secs(legion::constants::DEF_LOST_NODE_AT_SEC as u64)
    );
    assert!(decide_lost_nodes(&snapshot, &settled()).lost.is_empty());

    silent_for(&store, "legacy", 11);
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    assert_eq!(
        decide_lost_nodes(&snapshot, &settled()).lost,
        vec!["legacy".to_string()]
    );
    Ok(())
}

/// A `missedBeatsBeforeLost` of 5 against the 2s default reproduces the
/// historical fixed 10s window exactly — bit for bit, for a cluster that
/// configures nothing.
#[test]
fn the_default_window_reproduces_the_historical_constant() -> R<()> {
    let store = setup_store(legion::constants::DEF_MISSED_BEATS_BEFORE_LOST)?;
    register(
        &store,
        "node-1",
        Some(legion::constants::WORKER_HEARTBEAT_DURATION_SEC),
    )?;
    let rows = store.active_node_liveness_rows()?;
    assert_eq!(
        rows[0].window(),
        Duration::from_secs(legion::constants::DEF_LOST_NODE_AT_SEC as u64)
    );
    Ok(())
}

/// A checkpoint written with observed times well in the past never produces a
/// loss decision on a leader that has just become responsible. The rule the
/// whole design leans on: a checkpoint is display state and is never read back
/// as liveness.
#[test]
fn a_stale_checkpoint_is_never_read_as_liveness() -> R<()> {
    let store = setup_store(5)?;
    for index in 0..30 {
        register(&store, &format!("node-{index}"), None)?;
    }
    // A checkpoint from the previous leader, an hour stale — three hundred
    // lost windows old.
    let ancient = time::OffsetDateTime::now_utc().unix_timestamp() - 3_600;
    let beats: Vec<(String, i64)> = (0..30)
        .map(|index| (format!("node-{index}"), ancient))
        .collect();
    store.checkpoint_node_heartbeats(&beats)?;

    // Acquisition seeds at `now`, not from the column.
    let active: Vec<String> = store
        .active_node_liveness_rows()?
        .into_iter()
        .map(|row| row.id)
        .collect();
    store.liveness().become_responsible(&active);
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    assert_eq!(snapshot.len(), 30);
    assert!(
        decide_lost_nodes(&snapshot, &settled()).lost.is_empty(),
        "a stale checkpoint must not become a fleet-wide kill signal"
    );
    Ok(())
}

/// The mass-disruption circuit breaker, over a real fleet: 60% going at once
/// marks none; 40% marks exactly those.
#[test]
fn the_circuit_breaker_holds_a_fleet_wide_loss_and_counts_it() -> R<()> {
    let store = setup_store(5)?;
    for index in 0..100 {
        register(&store, &format!("node-{index}"), None)?;
    }
    let active: Vec<String> = (0..100).map(|index| format!("node-{index}")).collect();
    store.liveness().become_responsible(&active);

    for index in 0..60 {
        silent_for(&store, &format!("node-{index}"), 600);
    }
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    let decision = decide_lost_nodes(&snapshot, &settled());
    assert!(decision.lost.is_empty(), "60% at once must mark none");
    assert_eq!(
        decision.suppressed_count(SuppressionReason::MassDisruption),
        60
    );

    // Twenty of them check back in: now it is an ordinary 40% failure.
    for index in 0..20 {
        store.liveness().observe(&format!("node-{index}"));
    }
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    let decision = decide_lost_nodes(&snapshot, &settled());
    assert_eq!(decision.lost.len(), 40);
    assert_eq!(
        decision.suppressed_count(SuppressionReason::MassDisruption),
        0
    );
    // ...and the losses actually land.
    store.mark_nodes_as_lost(&decision.lost)?;
    assert_eq!(store.active_node_liveness_rows()?.len(), 60);
    Ok(())
}

/// The negative control for every "marks zero nodes lost" assertion in this
/// suite: a genuinely dead node is still marked lost, and its liveness entry
/// is dropped so the tracked count stays the live set.
#[test]
fn a_genuinely_dead_node_is_still_marked_lost() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "alive", None)?;
    register(&store, "dead", None)?;
    store
        .liveness()
        .become_responsible(&["alive".into(), "dead".into()]);
    silent_for(&store, "dead", 600);

    let snapshot = liveness_snapshot(&store, Instant::now())?;
    let decision = decide_lost_nodes(&snapshot, &settled());
    assert_eq!(decision.lost, vec!["dead".to_string()]);
    store.mark_nodes_as_lost(&decision.lost)?;
    for id in &decision.lost {
        store.liveness().forget(id);
    }
    assert_eq!(store.get_node("dead")?.status, "lost");
    assert_eq!(store.get_node("dead")?.availability, "unavailable");
    assert_eq!(store.get_node("alive")?.status, "active");
    assert_eq!(store.liveness().tracked(), 1);
    Ok(())
}

/// The freshness ratio and the cadence report, read off a real fleet.
#[test]
fn freshness_reads_the_fleet_and_computes_a_remedy() -> R<()> {
    let store = setup_store(5)?;
    for index in 0..100 {
        register(&store, &format!("node-{index}"), None)?;
    }
    let active: Vec<String> = (0..100).map(|index| format!("node-{index}")).collect();
    store.liveness().become_responsible(&active);

    // Everyone on time.
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    assert_eq!(
        freshness_ratio(&snapshot, Duration::from_secs(2), 20),
        Some(1.0)
    );

    // Three quarters of the fleet slips to a 9s effective interval: the
    // control plane is behind, and the report says what to set.
    for index in 0..75 {
        silent_for(&store, &format!("node-{index}"), 9);
    }
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    let report = cadence_report(&snapshot, Duration::from_secs(2), 5, 2.0, 20, false, false);
    assert_eq!(report.active_nodes, 100);
    assert_eq!(report.lost_window, Duration::from_secs(10));
    let ratio = report.freshness_ratio.expect("measurable");
    assert!((ratio - 0.25).abs() < 1e-9, "ratio was {ratio}");
    assert_eq!(report.sustainable_nodes, Some(25));
    // 100 nodes at an observed 12.5/sec, H=2 -> 16s.
    assert_eq!(report.suggested_heartbeat_seconds, Some(16));
    // Still no loss: 9s of silence is inside a 10s window. The ratio warns
    // BEFORE the collapse, which is the whole point of measuring it.
    assert!(decide_lost_nodes(&snapshot, &settled()).lost.is_empty());
    Ok(())
}

/// Config validation rejects the combinations that cannot hold together, and
/// names the offending values.
#[test]
fn config_validation_names_the_offending_values() {
    let short_grace = LivenessConfigFull {
        failover_grace: "5s".into(),
        ..LivenessConfigFull::default()
    };
    let err = short_grace
        .validate(Duration::from_secs(10))
        .expect_err("a grace shorter than the window is not a grace");
    let message = format!("{err:#}");
    assert!(message.contains("5s"), "{message}");
    assert!(message.contains("10s"), "{message}");

    let eager_digest = LivenessConfigFull {
        digest_interval: "45s".into(),
        ..LivenessConfigFull::default()
    };
    let message = format!(
        "{:#}",
        eager_digest
            .validate(Duration::from_secs(10))
            .expect_err("a digest interval past the peer grace is not survivable")
    );
    assert!(
        message.contains("45s") && message.contains("30s"),
        "{message}"
    );

    let fast_checkpoint = LivenessConfigFull {
        checkpoint_interval: "1s".into(),
        ..LivenessConfigFull::default()
    };
    let message = format!(
        "{:#}",
        fast_checkpoint
            .validate(Duration::from_secs(10))
            .expect_err("a 1s checkpoint re-creates the load this design removes")
    );
    assert!(message.contains("1s"), "{message}");

    // Disabled checkpoints are legal, and so are the defaults.
    let disabled = LivenessConfigFull {
        checkpoint_interval: "0s".into(),
        ..LivenessConfigFull::default()
    };
    assert!(disabled.validate(Duration::from_secs(10)).is_ok());
    assert!(LivenessConfigFull::default()
        .validate(Duration::from_secs(10))
        .is_ok());

    // Heartbeat side.
    let no_beats = HeartbeatConfigFull {
        missed_beats_before_lost: 0,
        ..HeartbeatConfigFull::default()
    };
    assert!(no_beats.validate().is_err());
    let inverted = HeartbeatConfigFull {
        min_interval: "60s".into(),
        max_interval: "10s".into(),
        ..HeartbeatConfigFull::default()
    };
    let message = format!("{:#}", inverted.validate().expect_err("max below min"));
    assert!(
        message.contains("60s") && message.contains("10s"),
        "{message}"
    );
    assert!(HeartbeatConfigFull::default().validate().is_ok());
}

/// A peer manager that goes quiet does not kill the nodes it was reporting —
/// and the suppression is bounded, so a manager that is genuinely gone does
/// not protect its nodes forever.
#[test]
fn a_silent_peer_manager_does_not_kill_its_nodes() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "mine", None)?;
    register(&store, "theirs", None)?;
    store
        .liveness()
        .become_responsible(&["mine".into(), "theirs".into()]);

    let start = Instant::now();
    let unix = time::OffsetDateTime::now_utc().unix_timestamp();
    store.liveness().observe_peer_at(
        "mgr-c",
        vec!["theirs".into()],
        start - Duration::from_secs(20),
        unix,
    );
    silent_for(&store, "mine", 600);
    silent_for(&store, "theirs", 600);

    // 20s into a 30s peer grace: mgr-c's node is held, ours is not.
    let snapshot = liveness_snapshot(&store, Instant::now())?;
    let gap = store.liveness().peer_gap_at(start);
    assert_eq!(gap.stale_peers, vec!["mgr-c".to_string()]);
    let decision = decide_lost_nodes(
        &snapshot,
        &LossContext {
            peer_gap_nodes: gap.suppressed,
            ..settled()
        },
    );
    assert_eq!(decision.lost, vec!["mine".to_string()]);
    assert_eq!(
        decision.suppressed_count(SuppressionReason::PeerDigestGap),
        1
    );

    // Past the grace, mgr-c is gone rather than slow, and its nodes are
    // eligible again — otherwise a dead manager would be a permanent hole in
    // failure detection.
    let gap = store
        .liveness()
        .peer_gap_at(start + Duration::from_secs(15));
    assert!(gap.suppressed.is_empty());
    let mut lost = decide_lost_nodes(
        &snapshot,
        &LossContext {
            peer_gap_nodes: gap.suppressed,
            ..settled()
        },
    )
    .lost;
    lost.sort();
    assert_eq!(lost, vec!["mine".to_string(), "theirs".to_string()]);
    Ok(())
}

/// A node with real capacity, so the scheduler can actually place on it.
fn register_with_capacity(store: &Store, id: &str, availability: &str) -> R<()> {
    store.insert_node(NodeMsg {
        id: id.to_string(),
        pool: "default".into(),
        status: "active".into(),
        availability: availability.into(),
        config_attrs: json!({ "maxAllocations": 20 }).to_string(),
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } }
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
        last_heartbeat: String::new(),
        heartbeat_source: String::new(),
    })?;
    Ok(())
}

/// Submit a one-task service job and run its `job_submit` eval inline.
fn submit_and_place(store: &Store, name: &str) -> R<usize> {
    use legion::structs::job::{JobConfig, TaskConfig, TaskCount};
    use legion::structs::{Eval, Submission};
    let job = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(1),
            config: json!({ "message": "hi" }),
            resources: json!({ "memory": 128, "cpu": 100 }),
            ..TaskConfig::new(format!("{name}-task"), "service")
        }],
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    };
    let submissions = vec![Submission {
        path: format!("{name}.json"),
        content: name.to_string(),
        is_primary: true,
    }];
    store.handle_job_submit(&job, &submissions)?;
    let job_config_id = store.get_job_config(name)?.id();
    let eval = Eval {
        id: format!("eval-{name}"),
        status: "pending".into(),
        kind: "job_submit".into(),
        job_id: name.to_string(),
        job_config_id,
        node_name: String::new(),
        priority: 50,
    };
    // A placement failure is the thing under test, not a harness error.
    let _ = eval.handle(store);
    Ok(store.allocs_for_job(name)?.len())
}

/// Spot-check P0-6: an old leader marked the node lost (`unavailable`), the
/// node's liveness came back on a path that did not also restore
/// schedulability (an older manager's re-registration kept `unavailable`),
/// and a NEW leader — which holds liveness only in memory and replicates
/// only transitions — never wrote the missing half. `legion node list`
/// said active; every placement said "No nodes available".
///
/// The liveness owner now reconciles the two halves every tick, on evidence:
/// only once it has actually heard from the node.
#[test]
fn a_new_leader_returns_a_heartbeating_node_to_the_pool() -> R<()> {
    let store = setup_store(5)?;
    register_with_capacity(&store, "node-1", "available")?;
    // The old leader's loss: both halves.
    store.mark_nodes_as_lost(&["node-1".to_string()])?;
    // Liveness came back without the availability half.
    register_with_capacity(&store, "node-1", "unavailable")?;
    let node = store.get_node("node-1")?;
    assert_eq!(
        (node.status.as_str(), node.availability.as_str()),
        ("active", "unavailable")
    );
    assert_eq!(
        submit_and_place(&store, "before")?,
        0,
        "the scheduler skips it"
    );

    // The new leader takes responsibility. A seed is not evidence.
    store.liveness().become_responsible(&["node-1".into()]);
    assert_eq!(store.reconcile_node_availability()?, (0, 0));
    assert_eq!(store.get_node("node-1")?.availability, "unavailable");

    // It hears from the node (directly, or through a peer's digest)...
    std::thread::sleep(Duration::from_millis(5));
    store.liveness().observe("node-1");
    // ...and the stored row now agrees with liveness.
    assert_eq!(store.reconcile_node_availability()?, (1, 0));
    assert_eq!(store.get_node("node-1")?.availability, "available");
    assert_eq!(submit_and_place(&store, "after")?, 1, "placement succeeds");
    // Idempotent: nothing left to repair.
    assert_eq!(store.reconcile_node_availability()?, (0, 0));
    Ok(())
}

/// The reconcile pass never overrides an operator: a cordoned (`drained`)
/// node stays cordoned however its liveness moves.
#[test]
fn the_availability_reconcile_leaves_a_cordon_alone() -> R<()> {
    let store = setup_store(5)?;
    register_with_capacity(&store, "node-1", "available")?;
    store.set_node_availability("node-1", "drained")?;
    store.liveness().become_responsible(&["node-1".into()]);
    std::thread::sleep(Duration::from_millis(5));
    store.liveness().observe("node-1");
    assert_eq!(store.reconcile_node_availability()?, (0, 0));
    assert_eq!(store.get_node("node-1")?.availability, "drained");
    Ok(())
}

/// A half-applied LOSS (status `lost`, availability still `available`) is
/// withdrawn from placement, unless this manager is hearing from the node.
#[test]
fn a_half_applied_loss_is_withdrawn_from_placement() -> R<()> {
    let store = setup_store(5)?;
    register_with_capacity(&store, "node-1", "available")?;
    store.mark_nodes_as_lost(&["node-1".to_string()])?;
    // Simulate the control half never landing.
    store.set_node_availability("node-1", "available")?;
    assert_eq!(store.get_node("node-1")?.status, "lost");
    store.liveness().become_responsible(&[]);
    assert_eq!(store.reconcile_node_availability()?, (0, 1));
    assert_eq!(store.get_node("node-1")?.availability, "unavailable");
    Ok(())
}

/// Loss and recovery own `available`/`unavailable` only. Before, a cordoned
/// node that went lost was rewritten to `unavailable`, and the recovery then
/// rewrote that to `available` — silently undoing the cordon.
#[test]
fn a_cordoned_node_stays_cordoned_through_loss_and_recovery() -> R<()> {
    let store = setup_store(5)?;
    register(&store, "node-1", None)?;
    store.set_node_availability("node-1", "drained")?;
    store.mark_nodes_as_lost(&["node-1".to_string()])?;
    let node = store.get_node("node-1")?;
    assert_eq!(node.status, "lost");
    assert_eq!(node.availability, "drained", "loss must not erase a cordon");

    store.update_node_heartbeat("node-1".to_string())?;
    let node = store.get_node("node-1")?;
    assert_eq!(node.status, "active");
    assert_eq!(node.availability, "drained", "recovery must not uncordon");
    Ok(())
}
