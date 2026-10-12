//! In-process integration tests for node cordon / drain / uncordon.
//!
//! Uses the same harness style as `tests/scheduler_integration.rs`
//! (`Store::start_storage` + eval handlers driving the `Scheduler`), plus
//! direct `ManagerDaemonService` trait calls for the `SetNodeAvailability`
//! RPC surface (ACLs disabled).

use anyhow::anyhow;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::events::SchedulerEventFilter;
use legion::storage::store::Store;
use legion::structs::manager_server::Manager;
use legion::structs::{
    config::ManagerConfigFull, job::JobConfig, job::TaskConfig, job::TaskCount, Eval, Node,
    NodeMsg, SetNodeAvailabilityReply, SetNodeAvailabilityRequest, Submission,
};
use tempfile::Builder;
use tonic::Request;

struct DrainCluster {
    svc: ManagerDaemonService,
    eval_seq: u64,
}

impl DrainCluster {
    fn new() -> R<Self> {
        let store_dir = Builder::new()
            .prefix("legion-node-drain-")
            .tempdir()
            .context("error creating temp store directory")?
            .keep();

        let store_config = ManagerConfigFull {
            store_path: store_dir
                .to_str()
                .context("error parsing store path")?
                .to_string(),
            scheduling_policy: "spread".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        };

        let store = Store::start_storage(&store_config)?;
        Ok(Self {
            svc: ManagerDaemonService::new(store),
            eval_seq: 0,
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn add_node(&self, id: &str) -> R<()> {
        self.store().insert_node(NodeMsg::from(make_node(id)))?;
        Ok(())
    }

    /// Submit a job config and run its `job_submit` eval synchronously.
    fn submit_job(&mut self, job_config: JobConfig) -> R<()> {
        let name = job_config.name.clone();
        let submissions = vec![Submission {
            path: format!("{name}.json"),
            content: name.clone(),
            is_primary: true,
        }];
        self.store()
            .handle_job_submit(&job_config, &submissions)
            .with_context(|| format!("submit job {name}"))?;
        let job_config_id = self.store().get_job_config(&name)?.id();
        self.handle_eval("job_submit", Some(&name), Some(&job_config_id), None)?;
        Ok(())
    }

    /// Build and run one eval handler directly (no queue round-trip).
    fn handle_eval(
        &mut self,
        kind: &str,
        job_id: Option<&str>,
        job_config_id: Option<&str>,
        node_name: Option<&str>,
    ) -> R<String> {
        self.eval_seq += 1;
        let eval = Eval {
            id: format!("test-{}", self.eval_seq),
            status: "pending".into(),
            kind: kind.into(),
            job_id: job_id.unwrap_or_default().to_string(),
            job_config_id: job_config_id.unwrap_or_default().to_string(),
            node_name: node_name.unwrap_or_default().to_string(),
            priority: 50,
        };
        eval.handle(self.store())
    }

    /// Call the SetNodeAvailability RPC through the Manager trait.
    async fn set_availability(
        &self,
        node_id: &str,
        availability: &str,
        evict: bool,
        idempotency_key: Option<&str>,
    ) -> R<SetNodeAvailabilityReply> {
        let reply = self
            .svc
            .set_node_availability(Request::new(SetNodeAvailabilityRequest {
                node_id: node_id.into(),
                availability: availability.into(),
                evict,
                idempotency_key: idempotency_key.map(str::to_string),
            }))
            .await
            .map_err(|status| anyhow!("SetNodeAvailability failed: {status}"))?;
        Ok(reply.into_inner())
    }

    /// Pop the eval the drain RPC enqueued and drive its handler directly.
    fn run_queued_node_drain(&self, node_id: &str) -> R<()> {
        let eval = self
            .store()
            .get_next_eval()?
            .context("expected a queued node_drain eval")?;
        assert_eq!(eval.kind, "node_drain");
        assert_eq!(eval.node_name, node_id);
        eval.handle(self.store())?;
        self.store().set_eval_status(&eval.id, "complete")?;
        Ok(())
    }

    fn nodes_for_job(&self, job: &str) -> R<Vec<String>> {
        Ok(self
            .store()
            .allocs_for_job(job)?
            .into_iter()
            .map(|alloc| alloc.node_id)
            .collect())
    }

    fn availability(&self, node_id: &str) -> R<String> {
        Ok(self.store().get_node(node_id)?.availability)
    }

    /// Node ids targeted by pending `node_drain` (eviction) evals.
    fn queued_drain_targets(&self) -> R<Vec<String>> {
        let mut targets: Vec<String> = self
            .store()
            .get_evals()?
            .into_iter()
            .filter(|eval| eval.kind == "node_drain")
            .map(|eval| eval.node_name)
            .collect();
        targets.sort();
        Ok(targets)
    }

    fn event_kinds(&self, node_id: &str) -> R<Vec<String>> {
        let filter = SchedulerEventFilter {
            node_id: Some(node_id.to_string()),
            ..Default::default()
        };
        Ok(self
            .store()
            .list_scheduler_events(&filter)?
            .into_iter()
            .map(|event| event.kind)
            .collect())
    }
}

fn make_node(id: &str) -> Node {
    Node {
        id: id.to_string(),
        status: "ready".into(),
        pool: "default".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "node_id": id, "maxAllocations": 20 }),
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": {
                "cpu-1": { "frequency": "2400" },
                "cpu-2": { "frequency": "2400" },
            }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn stateless_job(name: &str, count: u32, constraints: serde_json::Value) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: name.to_string(),
        tasks: vec![TaskConfig {
            count: TaskCount::new(count),
            config: json!({ "message": "hi" }),
            resources: json!({ "memory": 128, "cpu": 100 }),
            constraints,
            ..TaskConfig::new(format!("{name}-task"), "service")
        }],
        version: None,
    }
}

/// (a) A cordoned node receives no new placements while the other node does;
/// the cordon leaves status/heartbeat untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cordoned_node_receives_no_new_placements() -> R<()> {
    let mut cluster = DrainCluster::new()?;
    cluster.add_node("node-a")?;
    cluster.add_node("node-b")?;

    let reply = cluster
        .set_availability("node-a", "drained", false, None)
        .await?;
    assert_eq!(reply.message, "Cordoned node node-a");
    assert!(!reply.deduplicated);
    let node = reply.node.context("cordon reply carries the node")?;
    assert_eq!(node.availability, "drained");
    assert_eq!(node.status, "ready", "cordon must not touch status");
    assert_eq!(cluster.availability("node-a")?, "drained");

    cluster.submit_job(stateless_job("job-cordon", 2, json!({})))?;
    let nodes = cluster.nodes_for_job("job-cordon")?;
    assert_eq!(nodes.len(), 2);
    assert!(
        nodes.iter().all(|node| node == "node-b"),
        "all placements must avoid the cordoned node, got {nodes:?}"
    );

    let kinds = cluster.event_kinds("node-a")?;
    assert!(kinds.contains(&"node.cordoned".to_string()), "{kinds:?}");
    Ok(())
}

/// (b) Draining a node moves its existing stateless allocations to the other
/// node (eviction driven through the queued `node_drain` eval handler).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_moves_stateless_allocations_to_other_node() -> R<()> {
    let mut cluster = DrainCluster::new()?;
    cluster.add_node("node-a")?;
    cluster.add_node("node-b")?;

    cluster.submit_job(stateless_job("job-drain", 2, json!({})))?;
    let initial_nodes = cluster.nodes_for_job("job-drain")?;
    assert_eq!(initial_nodes.len(), 2);

    // Placement spread is policy-driven, so drain whichever node actually
    // holds an allocation and expect everything to end up on the other one.
    let drained = initial_nodes[0].clone();
    let survivor = if drained == "node-a" {
        "node-b"
    } else {
        "node-a"
    };

    let reply = cluster
        .set_availability(&drained, "drained", true, None)
        .await?;
    assert_eq!(reply.message, format!("Draining node {drained}"));
    cluster.run_queued_node_drain(&drained)?;

    let nodes = cluster.nodes_for_job("job-drain")?;
    assert_eq!(nodes.len(), 2, "allocation count must be preserved");
    assert!(
        nodes.iter().all(|node| node == survivor),
        "all allocations must have moved off {drained}, got {nodes:?}"
    );
    assert!(cluster.store().allocs_for_node(&drained)?.is_empty());

    let kinds = cluster.event_kinds(&drained)?;
    assert!(kinds.contains(&"node.cordoned".to_string()), "{kinds:?}");
    assert!(kinds.contains(&"node.drained".to_string()), "{kinds:?}");
    Ok(())
}

/// (c) Uncordoning restores the node to the scheduling pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncordon_restores_placements() -> R<()> {
    let mut cluster = DrainCluster::new()?;
    cluster.add_node("node-a")?;
    cluster.add_node("node-b")?;

    cluster
        .set_availability("node-a", "drained", false, None)
        .await?;
    cluster.submit_job(stateless_job("job-one", 1, json!({})))?;
    assert_eq!(cluster.nodes_for_job("job-one")?, vec!["node-b"]);

    let reply = cluster
        .set_availability("node-a", "available", false, None)
        .await?;
    assert_eq!(reply.message, "Uncordoned node node-a");
    assert_eq!(cluster.availability("node-a")?, "available");

    // A job pinned to the uncordoned node proves it is schedulable again.
    cluster.submit_job(stateless_job("job-two", 1, json!({ "node_id": "node-a" })))?;
    assert_eq!(cluster.nodes_for_job("job-two")?, vec!["node-a"]);

    let kinds = cluster.event_kinds("node-a")?;
    assert!(kinds.contains(&"node.uncordoned".to_string()), "{kinds:?}");
    Ok(())
}

/// (d) Drain is idempotent: an idempotency key replays the recorded reply
/// without enqueueing another eval, and re-running the drain pass leaves
/// allocations unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_is_idempotent() -> R<()> {
    let mut cluster = DrainCluster::new()?;
    cluster.add_node("node-a")?;
    cluster.add_node("node-b")?;

    cluster.submit_job(stateless_job("job-idem", 2, json!({})))?;
    let drained = cluster.nodes_for_job("job-idem")?[0].clone();

    let first = cluster
        .set_availability(&drained, "drained", true, Some("drain-key-1"))
        .await?;
    assert!(!first.deduplicated);
    cluster.run_queued_node_drain(&drained)?;
    let after_drain = cluster.nodes_for_job("job-idem")?;
    assert_eq!(after_drain.len(), 2);

    // Retry with the same idempotency key: replayed reply, no new eval.
    let second = cluster
        .set_availability(&drained, "drained", true, Some("drain-key-1"))
        .await?;
    assert!(second.deduplicated);
    assert_eq!(second.message, first.message);
    assert!(
        cluster.store().get_next_eval()?.is_none(),
        "deduplicated drain must not enqueue another node_drain eval"
    );

    // Re-running the drain pass itself is a no-op for placements.
    cluster
        .store()
        .reschedule_allocations_for_drained_node(&drained)?;
    assert_eq!(cluster.nodes_for_job("job-idem")?, after_drain);
    assert_eq!(cluster.availability(&drained)?, "drained");
    Ok(())
}

/// (e) A CORDON is never turned into an eviction — not by the RPC and not
/// by the scheduling reconciler (plan JC-41's over-execution fix).
///
/// `availability = 'drained'` is set by both `node cordon` and
/// `node drain --evict`, so a reconciler keying off availability alone
/// evicted every cordoned node's allocations — a destructive action the
/// operator never asked for. The evict intent is therefore persisted with
/// the availability write (`node.drain_evict`) and cleared whenever
/// availability leaves `drained`; the RPC/CLI surface is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cordon_only_nodes_are_never_evicted_by_reconciliation() -> R<()> {
    let mut cluster = DrainCluster::new()?;
    cluster.add_node("node-cordon")?;
    cluster.add_node("node-evict")?;
    cluster.add_node("node-spare")?;
    cluster.submit_job(stateless_job("job-intent", 3, json!({})))?;

    // Plain cordon: no eviction eval from the RPC.
    let reply = cluster
        .set_availability("node-cordon", "drained", false, None)
        .await?;
    assert_eq!(reply.message, "Cordoned node node-cordon");
    assert!(
        cluster.queued_drain_targets()?.is_empty(),
        "a cordon must not enqueue an eviction pass"
    );

    // Evicting drain: exactly one, for that node.
    let reply = cluster
        .set_availability("node-evict", "drained", true, None)
        .await?;
    assert_eq!(reply.message, "Draining node node-evict");
    assert_eq!(cluster.queued_drain_targets()?, vec!["node-evict"]);

    // Now the reconciler runs against replicated state alone (as it does on
    // leadership acquisition). Drop the queued eval first so the "already
    // pending" skip cannot be what protects the cordoned node.
    while let Some(eval) = cluster.store().claim_eval()? {
        cluster.store().complete_eval(&eval.id, true)?;
    }
    assert!(cluster.queued_drain_targets()?.is_empty());
    cluster.store().regenerate_scheduling_evals()?;
    assert_eq!(
        cluster.queued_drain_targets()?,
        vec!["node-evict"],
        "reconciliation may only re-derive the EVICTING drain; a cordoned node's allocations \
         must be left alone"
    );

    // The cordoned node keeps its allocations; the evicting drain's pass
    // still works when it runs.
    assert!(
        !cluster.store().allocs_for_node("node-cordon")?.is_empty(),
        "a cordoned node must keep running its existing allocations"
    );

    // Uncordoning clears the intent: nothing regenerates afterwards.
    while let Some(eval) = cluster.store().claim_eval()? {
        cluster.store().complete_eval(&eval.id, true)?;
    }
    cluster
        .set_availability("node-evict", "available", false, None)
        .await?;
    cluster.store().regenerate_scheduling_evals()?;
    assert!(
        cluster.queued_drain_targets()?.is_empty(),
        "an uncordoned node must not regenerate drain evictions"
    );
    Ok(())
}
