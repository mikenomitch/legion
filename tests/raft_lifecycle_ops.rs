//! Raft entries per keyed-instance lifecycle stage (sandbox-scaling plan,
//! Phase 0; `internal_docs/plans/sandbox-scaling-plan.md` §5.1 and §9).
//!
//! One keyed sandbox is driven through create, place, run, suspend, resume,
//! fork, and destroy on a bootstrapped single-voter raft cluster, under both
//! topologies (`single` and `lanes`). Every stage records how many raft
//! entries it committed — the sum of every lane's applied index, which counts
//! exactly the proposals that reached quorum, since an empty changeset never
//! becomes an entry — and which ops they were (`legion_raft_ops_total`
//! deltas, `outcome="applied"`).
//!
//! The counts are asserted exactly, so this is the regression guard the plan
//! asks for: a change that adds a raft entry to a lifecycle stage fails here
//! and has to update the table on purpose, which puts the delta in the diff.
//!
//! The manager runs as the real RPC service over the raft store. The worker
//! is the heartbeat, as in `suspend_integration.rs`: it reports allocation
//! health, capture reports, and restore reports exactly as a sandbox worker
//! would. The eval worker is the real loop's write sequence — `claim_evals`,
//! `resolve_claimed_eval` per eval, `complete_evals_and_claim_next` — so a
//! placement pays the claim, plan apply, status projection, and settle the
//! daemon pays on an idle cluster (the projection runs inline here; the
//! daemon defers it to a drain that merges concurrent plans, which is the
//! "amortized under load" figure, not the idle one).
//!
//! The measured lifecycle is an instance of a task that already runs
//! another one, which is the steady state of a sandbox platform (thousands
//! of instances of one task). The task's first placement also flips its
//! `task_status` from `pending` to `running`, one more status-lane entry
//! that a task pays once; it is reported as its own stage, `first instance`.
//!
//! Everything runs in one `#[test]` because the op counter is process-wide:
//! a second test in this binary would add its own ops to the deltas.

use std::collections::BTreeMap;

use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store};
use legion::structs::config::{ManagerConfigFull, PersistenceType};
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig};
use legion::structs::manager_server::Manager;
use legion::structs::suspend::SuspendConfig;
use legion::structs::{
    AllocationHealth, CreateInstanceRequest, DestroyInstanceRequest, ForkInstanceRequest,
    GetInstanceRequest, HeartbeatReply, HeartbeatRequest, InstanceInfo, InstanceMode, Node,
    NodeMsg, SnapshotReport, StartInstanceRequest, Submission, SuspendInstanceRequest,
};
use tonic::Request;

const NODE: &str = "ops-node";
const JOB: &str = "agents";
const TASK: &str = "workspace";
const FORKS: u32 = 2;

/// Raft entries one stage committed, and the ops that made them.
#[derive(Debug, Default, Clone)]
struct StageCount {
    entries: u64,
    ops: BTreeMap<String, u64>,
}

impl StageCount {
    fn ops_line(&self) -> String {
        self.ops
            .iter()
            .map(|(op, n)| {
                if *n == 1 {
                    op.clone()
                } else {
                    format!("{op}×{n}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The measured lifecycle, stage by stage, in order.
type Lifecycle = Vec<(&'static str, StageCount)>;

/// Expected entries per stage, identical under both topologies: an op never
/// spans lanes, so splitting the groups moves entries between logs without
/// changing how many there are. Update this table, on purpose, when a change
/// adds or removes a write.
///
/// `run (first beat)` is the first heartbeat after placement, which records
/// the allocation's health for the first time. `run (steady beat)` is the
/// next one, reporting the same health: the per-node, per-beat write the
/// plan's §5.1 found.
///
/// Phase 0 measured, on the code before Phase 1: first instance 6, create
/// 2, place 4, run 1 and 1, suspend 4, resume 8, fork ×2 12, destroy 4 — a
/// lifecycle of 22 entries, 26 with idempotency keys (each keyed RPC added a
/// control-lane `idempotency_record`). Phase 1 brought it to 8 with or
/// without keys: see `internal_docs/plans/sandbox-scaling-plan.md`, Phase 1.
const EXPECTED_NO_KEYS: &[(&str, u64)] = &[
    ("first instance", 3),
    ("create", 1),
    ("place", 1),
    ("run (first beat)", 1),
    ("run (steady beat)", 0),
    ("suspend", 2),
    ("resume", 3),
    ("fork x2", 4),
    ("destroy", 1),
];

/// The same lifecycle with an idempotency key on every mutating RPC. The
/// record rides each op's own entry, so keys cost nothing extra.
const EXPECTED_WITH_KEYS: &[(&str, u64)] = EXPECTED_NO_KEYS;

/// The stages the plan sums as "one lifecycle" (§5.1): create, place,
/// suspend, resume (which includes its own placement and restore report),
/// destroy. Heartbeats and forks are reported beside it, not in it.
const LIFECYCLE_STAGES: &[&str] = &["create", "place", "suspend", "resume", "destroy"];

fn raft_config(root: &std::path::Path, topology: &str) -> R<ManagerConfigFull> {
    let mut config = ManagerConfigFull {
        store_path: root
            .join("store")
            .to_str()
            .context("store path is not utf-8")?
            .to_string(),
        scheduling_policy: "bin_pack".to_string(),
        scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
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

fn sandbox_node() -> Node {
    Node {
        id: NODE.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "node_id": NODE,
            "maxAllocations": 64,
            "resources": { "memory": 65536, "cpu": 40000 },
        }),
        fingerprint_attrs: json!({
            "arch": "x86_64",
            "memory_total": "68719476736",
            "disk_total_space": "214748364800",
            "disk_available_space": "107374182400",
            "cpus": { "cpu-1": { "frequency": 2000.0 } },
            "cpu.vendor": "GenuineIntel",
            "cpu.model": "6:106:6",
            "cpu.features": "3f9a0c21",
            "sandbox.vmm": "firecracker",
            "sandbox.vmm.version": "1.12.0",
        }),
        dynamic_attrs: json!({ "buildVersion": "0.9.0", "capabilities": ["checkpoint"] }),
        health_checks: json!({}),
    }
}

fn sandbox_job() -> JobConfig {
    let task = TaskConfig {
        config: json!({ "image": "alpine", "command": ["sleep", "60"] }),
        resources: json!({ "memory": 512, "cpu": 500 }),
        stateful: Some(StatefulConfig {
            mode: InstanceMode::Keyed,
            require_id: false,
            initial_instances: None,
            on_lost: None,
            storage: None,
            envelope: None,
            promote: None,
            placement: None,
            fence_after: None,
        }),
        suspend: Some(SuspendConfig {
            checkpoint: Some("memory".into()),
            ..Default::default()
        }),
        ..TaskConfig::new(TASK, "sandbox")
    };
    JobConfig {
        priority: Default::default(),
        name: JOB.to_string(),
        tasks: vec![task],
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

fn manifest(snapshot_id: &str, instance_id: &str, allocation_id: i64) -> String {
    json!({
        "apiVersion": 1,
        "id": snapshot_id,
        "jobId": JOB,
        "task": TASK,
        "originInstance": instance_id,
        "originAllocId": allocation_id,
        "runtime": "sandbox",
        "kind": "memory",
        "createdAt": "2026-10-10T00:00:00Z",
    })
    .to_string()
}

fn capture_ok(snapshot_id: &str, instance_id: &str, allocation_id: i64) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: "capture".into(),
        ok: true,
        manifest_json: manifest(snapshot_id, instance_id, allocation_id),
        duration_ms: 420,
        total_bytes: 536_870_912,
        allocation_id,
        ..Default::default()
    }
}

fn restore_ok(snapshot_id: &str, allocation_id: i64) -> SnapshotReport {
    SnapshotReport {
        snapshot_id: snapshot_id.into(),
        op: "restore".into(),
        ok: true,
        duration_ms: 180,
        allocation_id,
        ..Default::default()
    }
}

struct Harness {
    rt: tokio::runtime::Runtime,
    svc: ManagerDaemonService,
    keys: bool,
    key_seq: std::cell::Cell<u32>,
}

impl Harness {
    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn key(&self) -> Option<String> {
        if !self.keys {
            return None;
        }
        let next = self.key_seq.get() + 1;
        self.key_seq.set(next);
        Some(format!("ops-key-{next}"))
    }

    /// Committed raft entries so far, across every lane this store runs.
    fn entries(&self) -> R<u64> {
        let status = self.store().raft_status().context("raft status")?;
        Ok(status.lanes.iter().map(|lane| lane.applied_index).sum())
    }

    /// Run `body` and count what it committed.
    fn stage(&self, body: impl FnOnce(&Self) -> R<()>) -> R<StageCount> {
        let entries_before = self.entries()?;
        let ops_before = applied_ops();
        body(self)?;
        let entries = self.entries()? - entries_before;
        let mut ops = BTreeMap::new();
        for (op, after) in applied_ops() {
            let delta = after - ops_before.get(&op).copied().unwrap_or(0);
            if delta > 0 {
                ops.insert(op, delta);
            }
        }
        Ok(StageCount { entries, ops })
    }

    /// The real eval worker's write sequence until the queue is empty: one
    /// claim, each eval resolved in turn, then the fused settle-and-claim.
    fn run_eval_worker(&self) -> R<()> {
        let limits = EvalBatchLimits {
            max: 16,
            workers: 1,
        };
        let mut batch = self.store().claim_evals(limits, ClaimScope::Any)?;
        while !batch.is_empty() {
            let completions = batch
                .iter()
                .map(|eval| (eval.id.clone(), resolve_claimed_eval(self.store(), 0, eval)))
                .collect();
            batch =
                self.store()
                    .complete_evals_and_claim_next(completions, limits, ClaimScope::Any)?;
        }
        Ok(())
    }

    fn heartbeat(&self, request: HeartbeatRequest) -> R<HeartbeatReply> {
        Ok(self
            .rt
            .block_on(self.svc.heartbeat(Request::new(HeartbeatRequest {
                id: NODE.into(),
                ..request
            })))?
            .into_inner())
    }

    /// A worker beat reporting `allocations` as running and healthy.
    fn steady_beat(&self, allocations: &[i64]) -> R<HeartbeatReply> {
        self.heartbeat(HeartbeatRequest {
            allocation_ids: allocations.to_vec(),
            allocation_health: allocations
                .iter()
                .map(|id| AllocationHealth {
                    allocation_id: *id,
                    healthy: true,
                    status: "running".into(),
                    artifact_states: None,
                })
                .collect(),
            ..Default::default()
        })
    }

    fn info(&self, instance_id: &str) -> R<InstanceInfo> {
        self.rt
            .block_on(self.svc.get_instance(Request::new(GetInstanceRequest {
                instance_id: instance_id.into(),
            })))?
            .into_inner()
            .instance
            .context("instance")
    }

    fn alloc_of(&self, instance_id: &str) -> R<i64> {
        self.info(instance_id)?
            .current_alloc_id
            .with_context(|| format!("{instance_id} should be placed"))
    }
}

/// `legion_raft_ops_total{op, outcome="applied"}`, summed over lanes.
fn applied_ops() -> BTreeMap<String, u64> {
    let mut ops = BTreeMap::new();
    for line in legion::manager::telemetry::render().lines() {
        let Some(rest) = line.strip_prefix("legion_raft_ops_total{") else {
            continue;
        };
        let Some((labels, value)) = rest.split_once("} ") else {
            continue;
        };
        if !labels.contains("outcome=\"applied\"") {
            continue;
        }
        let Some(op) = labels
            .split(',')
            .find_map(|label| label.strip_prefix("op=\""))
            .and_then(|op| op.strip_suffix('"'))
        else {
            continue;
        };
        let value = value.trim().parse::<f64>().unwrap_or(0.0) as u64;
        *ops.entry(op.to_string()).or_insert(0) += value;
    }
    ops
}

/// Drive one keyed sandbox through its whole life and count every stage.
fn measure_lifecycle(topology: &str, keys: bool) -> R<Lifecycle> {
    let dir = tempfile::tempdir()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let store = Store::start_storage(&raft_config(dir.path(), topology)?)?;
    let harness = Harness {
        rt,
        svc: ManagerDaemonService::new(store),
        keys,
        key_seq: std::cell::Cell::new(0),
    };
    let h = &harness;
    h.store().insert_node(NodeMsg::from(sandbox_node()))?;
    h.store().handle_job_submit(
        &sandbox_job(),
        &[Submission {
            path: format!("{JOB}.json"),
            content: "{}".to_string(),
            is_primary: true,
        }],
    )?;
    // The submit's own placement eval has nothing to place (no instances
    // yet), but it must not leak into the create stage's count.
    h.run_eval_worker()?;
    // Liveness: the node's first beat flips nothing it has to record later.
    h.steady_beat(&[])?;

    let mut stages = Lifecycle::new();

    // The task's first instance: created and placed, then reported healthy
    // once, so the measured instance below is one of a running task.
    stages.push((
        "first instance",
        h.stage(|h| {
            h.rt.block_on(h.svc.create_instance(Request::new(CreateInstanceRequest {
                job_id: JOB.into(),
                instance_id: Some("warm-up".into()),
                idempotency_key: h.key(),
                ..Default::default()
            })))?;
            h.run_eval_worker()
        })?,
    ));
    let warm_alloc = h.alloc_of("warm-up")?;
    h.steady_beat(&[warm_alloc])?;

    let mut instance_id = String::new();

    stages.push((
        "create",
        h.stage(|h| {
            let reply =
                h.rt.block_on(h.svc.create_instance(Request::new(CreateInstanceRequest {
                    job_id: JOB.into(),
                    idempotency_key: h.key(),
                    ..Default::default()
                })))?
                .into_inner();
            instance_id = reply.instance_id;
            Ok(())
        })?,
    ));
    stages.push(("place", h.stage(|h| h.run_eval_worker())?));
    let first_alloc = h.alloc_of(&instance_id)?;

    stages.push((
        "run (first beat)",
        h.stage(|h| h.steady_beat(&[warm_alloc, first_alloc]).map(drop))?,
    ));
    stages.push((
        "run (steady beat)",
        h.stage(|h| h.steady_beat(&[warm_alloc, first_alloc]).map(drop))?,
    ));

    stages.push((
        "suspend",
        h.stage(|h| {
            let reply =
                h.rt.block_on(h.svc.suspend_instance(Request::new(SuspendInstanceRequest {
                    instance_id: instance_id.clone(),
                    checkpoint: None,
                    idempotency_key: h.key(),
                })))?
                .into_inner();
            h.heartbeat(HeartbeatRequest {
                snapshot_reports: vec![capture_ok(&reply.snapshot_id, &instance_id, first_alloc)],
                ..Default::default()
            })?;
            Ok(())
        })?,
    ));
    let suspended = h.info(&instance_id)?;
    assert_eq!(suspended.state, "suspended", "the capture report suspends");
    let checkpoint = suspended.checkpoint_id.context("checkpoint")?;

    stages.push((
        "resume",
        h.stage(|h| {
            h.rt.block_on(h.svc.start_instance(Request::new(StartInstanceRequest {
                instance_id: instance_id.clone(),
                idempotency_key: h.key(),
                cold: false,
                force_move: false,
            })))?;
            h.run_eval_worker()?;
            let resumed_alloc = h.alloc_of(&instance_id)?;
            h.heartbeat(HeartbeatRequest {
                allocation_ids: vec![resumed_alloc],
                snapshot_reports: vec![restore_ok(&checkpoint, resumed_alloc)],
                ..Default::default()
            })?;
            Ok(())
        })?,
    ));
    let resumed_alloc = h.alloc_of(&instance_id)?;
    assert_ne!(resumed_alloc, first_alloc, "a resume is a new allocation");
    assert_eq!(h.info(&instance_id)?.state, "active");

    stages.push((
        "fork x2",
        h.stage(|h| {
            let reply =
                h.rt.block_on(h.svc.fork_instance(Request::new(ForkInstanceRequest {
                    instance_id: instance_id.clone(),
                    count: FORKS,
                    idempotency_key: h.key(),
                    ..Default::default()
                })))?
                .into_inner();
            h.heartbeat(HeartbeatRequest {
                allocation_ids: vec![resumed_alloc],
                snapshot_reports: vec![capture_ok(&reply.snapshot, &instance_id, resumed_alloc)],
                ..Default::default()
            })?;
            h.run_eval_worker()?;
            let mut reports = Vec::new();
            let mut running = vec![resumed_alloc];
            for fork in &reply.instances {
                let alloc = h.alloc_of(fork)?;
                running.push(alloc);
                reports.push(restore_ok(&reply.snapshot, alloc));
            }
            h.heartbeat(HeartbeatRequest {
                allocation_ids: running,
                snapshot_reports: reports,
                ..Default::default()
            })?;
            Ok(())
        })?,
    ));

    stages.push((
        "destroy",
        h.stage(|h| {
            h.rt.block_on(h.svc.destroy_instance(Request::new(DestroyInstanceRequest {
                instance_id: instance_id.clone(),
                idempotency_key: h.key(),
            })))?;
            h.run_eval_worker()?;
            Ok(())
        })?,
    ));
    // The measured instance's id was minted, so it is unnamed and its row
    // goes with the destroy instead of staying as a tombstone
    // (sandbox-scaling plan, Phase 4), in the same entry.
    let gone =
        h.rt.block_on(h.svc.get_instance(Request::new(GetInstanceRequest {
            instance_id: instance_id.clone(),
        })))
        .expect_err("an unnamed instance leaves no row after destroy");
    assert_eq!(gone.code(), tonic::Code::NotFound);
    Ok(stages)
}

fn render(topology: &str, keys: bool, stages: &Lifecycle) -> String {
    let mut out =
        format!("\n== raft entries per stage: topology={topology} idempotency_keys={keys} ==\n");
    for (stage, count) in stages {
        out.push_str(&format!(
            "{stage:<20} {:>3}  {}\n",
            count.entries,
            count.ops_line()
        ));
    }
    let lifecycle: u64 = stages
        .iter()
        .filter(|(stage, _)| LIFECYCLE_STAGES.contains(stage))
        .map(|(_, count)| count.entries)
        .sum();
    out.push_str(&format!(
        "{:<20} {lifecycle:>3}  (create + place + suspend + resume + destroy)\n",
        "lifecycle"
    ));
    out
}

fn check(topology: &str, keys: bool, expected: &[(&str, u64)]) -> R<()> {
    let stages = measure_lifecycle(topology, keys)?;
    let report = render(topology, keys, &stages);
    println!("{report}");
    let measured: Vec<(&str, u64)> = stages
        .iter()
        .map(|(stage, count)| (*stage, count.entries))
        .collect();
    assert_eq!(
        measured, expected,
        "raft entries per lifecycle stage changed (topology={topology}, keys={keys}); \
         if the change is intended, update the expected table in this file:{report}"
    );
    Ok(())
}

#[test]
fn raft_entries_per_keyed_lifecycle_stage() -> R<()> {
    for topology in ["single", "lanes"] {
        check(topology, false, EXPECTED_NO_KEYS)?;
        check(topology, true, EXPECTED_WITH_KEYS)?;
    }
    Ok(())
}
