//! Work that could not place for lack of room is retried when room appears
//! (sandbox-scaling plan §6.15).
//!
//! The manager runs as the real RPC service over a local store, and the
//! eval worker is the daemon loop's write sequence (`claim_evals`,
//! `resolve_claimed_eval`, `complete_evals_and_claim_next`).

use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::{EvalBatchLimits, Store};
use legion::structs::config::ManagerConfigFull;
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig};
use legion::structs::{InstanceMode, Node, NodeMsg, Submission};

const JOB: &str = "agents";
const TASK: &str = "workspace";

struct Cluster {
    svc: ManagerDaemonService,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-capacity-retry-")
            .tempdir()?
            .keep();
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Cluster {
            svc: ManagerDaemonService::new(store),
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    /// A node with room for exactly one 3,000 MiB task.
    fn add_node(&self, id: &str, pool: &str) -> R<()> {
        self.store().insert_node(NodeMsg::from(Node {
            id: id.to_string(),
            pool: pool.into(),
            status: "ready".into(),
            availability: "available".into(),
            last_heartbeat: "2023-01-01T00:00:00Z".into(),
            heartbeat_source: String::new(),
            config_attrs: json!({ "node_id": id, "maxAllocations": 10, "resources": {} }),
            fingerprint_attrs: json!({
                "memory_total": "4294967296",
                "cpus": { "cpu-1": { "frequency": 2400.0 } }
            }),
            dynamic_attrs: json!({}),
            health_checks: json!({}),
        }))?;
        Ok(())
    }

    /// What worker registration does after inserting the node.
    fn join(&self, id: &str, pool: &str) -> R<()> {
        self.add_node(id, pool)?;
        self.store().request_capacity_reconcile()?;
        Ok(())
    }

    fn submit_keyed(&self) -> R<()> {
        let task = TaskConfig {
            resources: json!({ "memory": 3000 }),
            stateful: Some(StatefulConfig {
                mode: InstanceMode::Keyed,
                require_id: false,
                initial_instances: None,
                on_lost: None,
                storage: None,
            }),
            ..TaskConfig::new(TASK, "noop")
        };
        self.store().handle_job_submit(
            &JobConfig {
                priority: Default::default(),
                name: JOB.to_string(),
                tasks: vec![task],
                rollout: None,
                rollout_by_group: Default::default(),
                version: None,
            },
            &[Submission {
                path: format!("{JOB}.json"),
                content: "{}".to_string(),
                is_primary: true,
            }],
        )?;
        Ok(())
    }

    /// `CreateInstance` as the RPC does it: the row, then a reschedule.
    fn create(&self, id: &str) -> R<()> {
        self.store()
            .create_keyed_instance(JOB, "default", TASK, Some(id))?;
        let config_id = self.store().get_job_config(JOB)?.id();
        self.store()
            .enqueue_eval("job_reschedule", Some(JOB), Some(&config_id), None, None)?;
        Ok(())
    }

    fn run_eval_worker(&self) -> R<()> {
        let limits = EvalBatchLimits {
            max: 16,
            workers: 1,
        };
        let mut batch = self.store().claim_evals(limits)?;
        while !batch.is_empty() {
            let completions = batch
                .iter()
                .map(|eval| (eval.id.clone(), resolve_claimed_eval(self.store(), 0, eval)))
                .collect();
            batch = self
                .store()
                .complete_evals_and_claim_next(completions, limits)?;
        }
        Ok(())
    }

    fn placed(&self, id: &str) -> R<bool> {
        Ok(self
            .store()
            .get_instance(id)?
            .context("instance")?
            .current_alloc_id
            .is_some())
    }
}

/// Phase 0's open question, answered: a keyed instance that could not place
/// is retried when a node joins, even when its task already runs another
/// instance. The capacity pass compared allocations against the task's
/// `count`, which is 1 for every keyed task, so a task with one placed
/// instance read as satisfied and its pending siblings waited for a
/// resubmit or a leader change.
#[test]
fn a_pending_keyed_instance_places_when_a_node_joins() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.add_node("node-a", "default")?;
    cluster.submit_keyed()?;
    cluster.run_eval_worker()?;

    cluster.create("user-1")?;
    cluster.run_eval_worker()?;
    assert!(cluster.placed("user-1")?, "the first instance fits");

    cluster.create("user-2")?;
    cluster.run_eval_worker()?;
    assert!(
        !cluster.placed("user-2")?,
        "the second instance has no room yet"
    );

    cluster.join("node-b", "default")?;
    cluster.run_eval_worker()?;
    assert!(
        cluster.placed("user-2")?,
        "the new node's capacity places the waiting keyed instance"
    );
    Ok(())
}
