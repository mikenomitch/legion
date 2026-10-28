//! Node-side namespace restrictions (apps plan §6.2, phase A1;
//! docs/namespaces.md, "Nodes that accept only some namespaces").
//!
//! A worker type's `namespaces` list rides on its nodes as the `namespaces`
//! config attribute. The scheduler refuses a job whose namespace the list
//! does not match, whatever the job's permit says; the blocked-eval wake
//! pass applies the same check, so a restricted node never wakes a job it
//! would refuse; and `job plan` names the restriction when it is why a task
//! did not place. Lists follow the reserved-prefix rule: `["*"]` does not
//! accept `legion-system`.
//!
//! The manager runs as the real RPC service over a local store (ACLs off),
//! with `legion-system` bootstrapped the way the first control leader does
//! it; the eval worker and the wake pass are driven from the test thread.

use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store};
use legion::structs::config::ManagerConfigFull;
use legion::structs::manager_server::Manager;
use legion::structs::{Node, NodeMsg, PlanJobRequest, RunJobRequest};
use tonic::Request;

const LIMITS: EvalBatchLimits = EvalBatchLimits {
    max: 16,
    workers: 1,
};

/// A node in `pool` with room for a few small tasks, accepting
/// `namespaces` (every namespace when `None`).
fn node(id: &str, pool: &str, namespaces: Option<Value>) -> Node {
    let mut config_attrs = json!({ "maxAllocations": 10, "resources": {} });
    if let Some(namespaces) = namespaces {
        config_attrs["namespaces"] = namespaces;
    }
    Node {
        id: id.to_string(),
        pool: pool.into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs,
        fingerprint_attrs: json!({
            "memory_total": "4294967296",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

/// A single-task exec service job, optionally permitting `pool`. It runs
/// as `nobody`, which a reserved namespace requires; no worker ever starts
/// it here, only the scheduler places it.
fn spec(name: &str, permit_pool: Option<&str>) -> String {
    let mut task = json!({
        "name": "main", "runtime": "exec", "lifecycle": "service",
        "config": { "command": ["sleep", "600"], "user": "nobody" },
        "resources": { "memory": 64, "cpu": 100 },
    });
    if let Some(pool) = permit_pool {
        task["permit"] = json!({ "pool": pool });
    }
    json!({ "name": name, "tasks": [task] }).to_string()
}

struct Cluster {
    _dir: tempfile::TempDir,
    svc: ManagerDaemonService,
    runtime: tokio::runtime::Runtime,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-namespace-restrictions-")
            .tempdir_in(crate::support::scratch_root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        assert!(store.ensure_system_namespace()?);
        Ok(Cluster {
            _dir: dir,
            svc: ManagerDaemonService::new(store),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        })
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    /// What worker registration does: the node, then the capacity pass.
    fn join(&self, node: Node) -> R<()> {
        self.store().insert_node(NodeMsg::from(node))?;
        self.store().request_capacity_reconcile()?;
        Ok(())
    }

    fn run(&self, namespace: &str, spec: String) -> R<()> {
        self.runtime
            .block_on(self.svc.run_job(Request::new(RunJobRequest {
                namespace: Some(namespace.to_string()),
                parsed_jobspec: spec,
                submissions: vec![],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
            })))?;
        Ok(())
    }

    fn plan(&self, namespace: &str, spec: String) -> Result<String, tonic::Status> {
        self.runtime
            .block_on(self.svc.plan_job(Request::new(PlanJobRequest {
                namespace: Some(namespace.to_string()),
                parsed_jobspec: spec,
                ..Default::default()
            })))
            .map(|reply| {
                let reply = reply.into_inner();
                reply
                    .allocations
                    .iter()
                    .map(|allocation| allocation.node_id.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            })
    }

    /// The eval worker, then a wake pass, until neither has work. Returns
    /// how many blocked evals woke.
    fn settle(&self) -> R<usize> {
        let store = self.store();
        let mut woken = 0;
        loop {
            let mut batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
            while !batch.is_empty() {
                let completions = batch
                    .iter()
                    .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
                    .collect();
                batch =
                    store.complete_evals_and_claim_next(completions, LIMITS, ClaimScope::Any)?;
            }
            let now = store.wake_blocked_evals()?;
            if now == 0 {
                return Ok(woken);
            }
            woken += now;
        }
    }

    /// The nodes `job`'s allocations run on.
    fn placed_on(&self, job: &str) -> R<Vec<String>> {
        let mut nodes: Vec<String> = self
            .store()
            .allocs_for_job(job)?
            .into_iter()
            .map(|allocation| allocation.node_id)
            .collect();
        nodes.sort();
        Ok(nodes)
    }
}

/// A node restricted to `legion-system` turns away a `default` job even
/// when the job permits its pool, and `job plan` says why; a
/// `legion-system` job that names no pool lands there, through the
/// namespace policy the bootstrap wrote (`pools.default = "legion-system"`).
#[test]
fn a_restricted_node_refuses_other_namespaces_even_when_permitted_test() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.join(node("m-1", "legion-system", Some(json!(["legion-system"]))))?;

    let refused = cluster
        .plan("default", spec("web", Some("legion-system")))
        .expect_err("nothing can place the default job");
    assert!(
        refused
            .message()
            .contains("1 node accepts only namespaces [legion-system]"),
        "{}",
        refused.message()
    );

    cluster.run("default", spec("web", Some("legion-system")))?;
    cluster.settle()?;
    assert!(cluster.placed_on("web")?.is_empty(), "permit is not enough");
    let blocked = cluster.store().blocked_evals()?;
    assert_eq!(blocked.len(), 1);
    assert!(
        blocked[0]
            .detail
            .to_string()
            .contains("accepts only namespaces [legion-system]"),
        "{}",
        blocked[0].detail
    );

    assert_eq!(
        cluster.plan("legion-system", spec("agent", None))?,
        "m-1",
        "the system namespace's policy sends it to the legion-system pool"
    );
    cluster.run("legion-system", spec("agent", None))?;
    cluster.settle()?;
    assert_eq!(cluster.placed_on("legion-system/agent")?, vec!["m-1"]);
    Ok(())
}

/// The wake pass re-runs the namespace check: a node that accepts only
/// another namespace joining the job's pool wakes nothing, and a node that
/// accepts it does.
#[test]
fn the_wake_pass_does_not_wake_onto_a_node_that_refuses_the_namespace_test() -> R<()> {
    let cluster = Cluster::new()?;
    // A node elsewhere, so the plan blocks under the task's class key
    // (`pool=shared`) rather than `escaped`, which any capacity wakes.
    cluster.join(node("o-1", "other", None))?;
    cluster.run("default", spec("web", Some("shared")))?;
    cluster.settle()?;
    let blocked = cluster.store().blocked_evals()?;
    assert_eq!(blocked.len(), 1, "no node in the pool yet");
    assert_eq!(blocked[0].key, "class:pool=shared");

    cluster.join(node("ci-1", "shared", Some(json!(["ci-*"]))))?;
    assert_eq!(cluster.settle()?, 0, "a ci-only node wakes nothing");
    assert!(cluster.placed_on("web")?.is_empty());
    assert_eq!(cluster.store().blocked_evals()?.len(), 1);

    cluster.join(node("w-1", "shared", None))?;
    assert_eq!(cluster.settle()?, 1, "an unrestricted node wakes it");
    assert_eq!(cluster.placed_on("web")?, vec!["w-1"]);
    Ok(())
}

/// `["*"]` on a node accepts every tenant namespace but not
/// `legion-system`, which takes a pattern starting `legion-`.
#[test]
fn a_star_node_does_not_accept_legion_system_test() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.join(node("s-1", "default", Some(json!(["*"]))))?;
    cluster.run("default", spec("web", None))?;
    cluster.run("legion-system", spec("agent", Some("default")))?;
    cluster.settle()?;
    assert_eq!(cluster.placed_on("web")?, vec!["s-1"]);
    assert!(cluster.placed_on("legion-system/agent")?.is_empty());

    cluster.join(node("both-1", "default", Some(json!(["*", "legion-*"]))))?;
    cluster.settle()?;
    assert_eq!(cluster.placed_on("legion-system/agent")?, vec!["both-1"]);
    Ok(())
}
