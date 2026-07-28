//! In-process integration coverage for the real raft engine (phase A-1 of
//! `docs/plans/durability-and-ops-execution-plan.md` §2): a single-node
//! bootstrap cluster with `persistence.type = raft` runs representative store
//! mutations through the full propose → quorum-commit → state-machine-apply
//! path and behaves exactly like the local store from the caller's point of
//! view (read-your-writes, live event stream), while every effect is
//! recorded through the replicated changeset log.
//!
//! Also covered: restart on the same data dir (state and applied index
//! survive log replay), and the raft-mode `write_connection` bypass guard
//! (a direct write path fails loudly instead of silently diverging this
//! replica).
//!
//! The roundtrip and restart tests run under BOTH topologies (`single` and
//! `lanes`, phase C-3): the caller-visible behavior must be identical
//! whether one group carries every table or three lanes split them.
//!
//! The 3-node in-process cluster (real transport between peers, membership
//! joins, failover) lands with the next phase's `tests/raft_membership.rs` —
//! deliberately not built here.

use legion::prelude::{json, Context, R};
use legion::storage::store::Store;
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, TaskConfig};
use legion::structs::{KvValueType, Node, NodeMsg, Submission};

/// Manager config for a single-node bootstrap raft cluster rooted at `root`
/// (store DB under `<root>/store`, raft log/secret under `<root>/raft`).
/// `topology` is `"single"` or `"lanes"` — the roundtrip tests run under
/// both, since a caller must observe identical behavior either way.
fn raft_config(root: &std::path::Path, topology: &str) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        ..Default::default()
    };
    config.persistence.r#type = PersistenceType::Raft;
    config.persistence.raft.topology = topology.to_string();
    config.persistence.raft.bootstrap = true;
    config.persistence.raft.node_id = "mgr-a".to_string();
    config.persistence.raft.data_dir = root
        .join("raft")
        .to_str()
        .context("raft data dir is not utf-8")?
        .to_string();
    Ok(config)
}

fn test_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        config_attrs: json!({ "maxAllocations": 10, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn test_job(name: &str) -> JobConfig {
    JobConfig {
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig::new("svc", "noop")],
        version: None,
    }
}

fn submission(name: &str) -> Submission {
    Submission {
        path: format!("{name}.jsonnet"),
        content: format!("job {name}"),
        is_primary: true,
    }
}

/// Drain everything currently buffered on a live-events subscription.
fn drain_event_kinds(
    events: &mut tokio::sync::broadcast::Receiver<legion::storage::events::SchedulerEventRecord>,
) -> Vec<String> {
    let mut kinds = Vec::new();
    while let Ok(event) = events.try_recv() {
        kinds.push(event.kind);
    }
    kinds
}

/// (a) Representative mutations flow through propose → commit → apply on a
/// bootstrapped single-node cluster: callers get their return values and
/// read-your-writes, the live event stream still emits (projected from the
/// committed changeset at apply time), and the raft log/applied indexes
/// advance.
#[test]
fn single_node_mutations_commit_through_raft_apply() -> R<()> {
    mutations_commit_through_raft_apply("single")
}

/// Same roundtrip under `topology = lanes`: the mutations spread across the
/// three groups (KV/job = control, placement = scheduler, heartbeat =
/// status), and the caller-visible behavior is identical.
#[test]
fn single_node_mutations_commit_through_raft_apply_lanes() -> R<()> {
    mutations_commit_through_raft_apply("lanes")
}

fn mutations_commit_through_raft_apply(topology: &str) -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&raft_config(dir.path(), topology)?)?;
    let mut events = store.subscribe_events();

    // Bootstrap elected itself: this node is the leader of a 1-voter group.
    let status = store
        .raft_status()
        .context("raft driver must report status")?;
    assert!(status.enabled);
    assert_eq!(status.backend, "raft");
    assert_eq!(status.leader_id.as_deref(), Some("mgr-a"));
    assert_eq!(status.voters, vec!["mgr-a".to_string()]);
    let applied_at_start = status.applied_index;

    // Control lane: KV write. The closure's return value (the KvEntry the
    // leader computed at capture time) comes back to the caller.
    let entry = store.put_kv("cfg/answer", "42", KvValueType::String, None, None)?;
    assert_eq!(entry.version, 1);
    // Read-your-writes: the caller unblocked only after local apply.
    let read_back = store.get_kv("cfg/answer")?.context("kv must exist")?;
    assert_eq!(read_back.value, "42");

    // Control + Status lanes: node registration and heartbeat.
    let node = store.insert_node(NodeMsg::from(test_node("node-1")))?;
    assert_eq!(node.id, "node-1");
    store.update_node_heartbeat("node-1".to_string())?;
    assert_eq!(store.get_node("node-1")?.status, "active");

    // Control lane composite write (job + configs + submissions in one
    // capture), then Scheduler lane placement against it.
    let outcome = store.handle_job_submit(&test_job("rt-job"), &[submission("rt-job")])?;
    let job_config_id = outcome.job_config_id().to_string();
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
    let (task_id, _) = tasks.first().context("job must have a task config")?;
    let alloc = store.place_alloc("node-1", *task_id)?;
    assert_eq!(alloc.node_id, "node-1");
    assert_eq!(
        store.allocs_for_node("node-1")?.len(),
        1,
        "placement visible after apply"
    );

    // Control lane: stop the job.
    store.stop_job("rt-job")?;
    assert_eq!(store.get_job("rt-job")?.status, "stopped");

    // The live event stream still emits under raft: event rows ride the
    // changeset and are published at apply time on every node.
    let kinds = drain_event_kinds(&mut events);
    assert!(
        kinds.iter().any(|k| k == "node.registered"),
        "expected node.registered in {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "job.stopped"),
        "expected job.stopped in {kinds:?}"
    );

    // Every mutation above was a committed raft entry.
    let status = store.raft_status().context("status after writes")?;
    assert!(
        status.applied_index > applied_at_start,
        "applied index must advance ({} -> {})",
        applied_at_start,
        status.applied_index
    );
    assert!(status.commit_index >= status.applied_index);

    // Operator history records real engine activity (startup + election).
    let history = store.raft_history(50);
    assert!(
        history.iter().any(|e| e.event_type == "startup"),
        "history must record engine startup"
    );
    Ok(())
}

/// (b) Restarting the store/driver on the same data dir replays the raft log
/// into the recorded applied state: committed data and the applied index
/// survive, and the reopened node accepts new writes.
#[test]
fn restart_preserves_state_and_applied_index() -> R<()> {
    restart_preserves_state("single")
}

/// Same restart under `topology = lanes`: each of the three lanes replays
/// its own log/applied marker and the recovered store behaves identically.
#[test]
fn restart_preserves_state_and_applied_index_lanes() -> R<()> {
    restart_preserves_state("lanes")
}

fn restart_preserves_state(topology: &str) -> R<()> {
    let dir = tempfile::tempdir()?;
    let config = raft_config(dir.path(), topology)?;

    let applied_before = {
        let store = Store::start_storage(&config)?;
        store.put_kv("persist/one", "first", KvValueType::String, None, None)?;
        store.insert_node(NodeMsg::from(test_node("node-r")))?;
        store.handle_job_submit(&test_job("restart-job"), &[submission("restart-job")])?;
        store.stop_job("restart-job")?;
        let status = store.raft_status().context("status before restart")?;
        assert!(status.applied_index > 0);
        status.applied_index
        // Store (and with it the engine, its runtime, and the log store)
        // drops here, releasing the data dir for the successor.
    };

    let store = Store::start_storage(&config)?;

    // Committed state survived the restart (log replay + recorded applied
    // marker; bootstrap re-initialize is an idempotent no-op).
    assert_eq!(
        store
            .get_kv("persist/one")?
            .context("kv survives restart")?
            .value,
        "first"
    );
    assert_eq!(store.get_node("node-r")?.id, "node-r");
    assert_eq!(store.get_job("restart-job")?.status, "stopped");

    let status = store.raft_status().context("status after restart")?;
    assert_eq!(status.leader_id.as_deref(), Some("mgr-a"));
    assert!(
        status.applied_index >= applied_before,
        "applied index must survive restart ({} < {applied_before})",
        status.applied_index
    );

    // And the reopened node keeps accepting replicated writes.
    let entry = store.put_kv("persist/two", "second", KvValueType::String, None, None)?;
    assert_eq!(entry.version, 1);
    assert_eq!(
        store
            .get_kv("persist/two")?
            .context("post-restart write")?
            .value,
        "second"
    );
    // `put_kv` returns after local apply, but `raft_status()` reads the
    // openraft metrics watch channel, which updates asynchronously — poll
    // instead of asserting a single racy sample (slow CI runners lag).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let status_after = store.raft_status().context("status after new write")?;
        if status_after.applied_index > status.applied_index {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "applied index never advanced past {} after post-restart write",
            status.applied_index
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    Ok(())
}

/// (c) The raft-mode `write_connection` bypass guard is armed: a write path
/// that grabs the write connection without going through `replicated()` (or
/// the apply path) fails loudly instead of mutating this replica alone.
/// `reset_all_tables` is exactly such a direct path.
#[test]
fn write_connection_bypass_guard_rejects_direct_writes() -> R<()> {
    let dir = tempfile::tempdir()?;
    let store = Store::start_storage(&raft_config(dir.path(), "single")?)?;

    let err = store
        .reset_all_tables()
        .expect_err("direct write path must be rejected in raft mode");
    assert!(
        format!("{err:#}")
            .contains("write_connection called outside capture/apply/local-write in raft mode"),
        "unexpected error: {err:#}"
    );

    // Replicated writes still work — the guard only blocks unclassified
    // paths.
    store.put_kv("guard/ok", "yes", KvValueType::String, None, None)?;
    assert_eq!(
        store
            .get_kv("guard/ok")?
            .context("replicated write ok")?
            .value,
        "yes"
    );
    Ok(())
}
