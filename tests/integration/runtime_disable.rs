//! Runtime disables (docs/runtime-disable.md): `SetRuntimeEnabled` refuses
//! every new start of a task runtime, cluster-wide or for the namespaces a
//! glob matches, leaves running work alone unless asked to drain, and
//! `enable` lets blocked work place without a resubmit.
//!
//! The manager runs as the real RPC service over a local store; the eval
//! worker and the wake pass are the daemon loop's own steps, driven from
//! the test thread (as in `blocked_evals.rs`). The worker half runs a real
//! `WorkerDaemonService` with the `exec` runtime and a real ticket verifier
//! (as in `worker_edge_overload.rs`), handed the set of disables the way a
//! heartbeat reply hands it over.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::manager::workload_identity::Issuer;
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::{BlockedEval, ClaimScope, EvalBatchLimits, EvalCompletion, Store};
use legion::structs::config::ManagerConfigFull;
use legion::structs::edge_boot::SpeculativeStartConfig;
use legion::structs::job::{JobConfig, StatefulConfig, TaskConfig, TaskCount};
use legion::structs::manager_server::Manager;
use legion::structs::namespace::Namespace;
use legion::structs::runtime_state::{RuntimeDisableSet, RUNTIME_DISABLE_CAPABILITY};
use legion::structs::suspend::SuspendConfig;
use legion::structs::worker_server::Worker;
use legion::structs::{
    GetJobInfoRequest, HeartbeatRequest, InstanceMode, ListRuntimeStatesRequest, Node, NodeMsg,
    RatifyClaimReply, RatifyClaimRequest, RuntimeRefusals, SetRuntimeEnabledReply,
    SetRuntimeEnabledRequest, StartClaimRequest, Submission,
};
use legion::worker::boot_ticket::{
    BootTicketVerifier, JwksSource, DEFAULT_CLOCK_SKEW, DEFAULT_JWKS_REFRESH,
};
use legion::worker::edge;
use legion::worker::server::*;
use legion::worker::task_manager::WorkerTaskManager;
use tonic::{Code, Request, Status};

const LIMITS: EvalBatchLimits = EvalBatchLimits {
    max: 16,
    workers: 1,
};

fn node(id: &str, capabilities: &[&str]) -> Node {
    Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2023-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({ "node_id": id, "maxAllocations": 50, "resources": {} }),
        fingerprint_attrs: json!({
            "memory_total": "17179869184",
            "cpus": { "cpu-1": { "frequency": 2400.0 } }
        }),
        dynamic_attrs: json!({ "buildVersion": "0.9.0", "capabilities": capabilities }),
        health_checks: json!({}),
    }
}

fn job(name: &str, tasks: Vec<TaskConfig>) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: name.to_string(),
        tasks,
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
        pending_extensions: Default::default(),
        extensions: Default::default(),
    }
}

fn service(name: &str, runtime: &str, count: u32) -> TaskConfig {
    TaskConfig {
        // Its own group: tasks of one group place together.
        group: name.to_string(),
        count: TaskCount::new(count),
        resources: json!({ "memory": 64, "cpu": 100 }),
        ..TaskConfig::new(name, runtime)
    }
}

fn keyed(name: &str, runtime: &str, suspend: bool) -> TaskConfig {
    TaskConfig {
        resources: json!({ "memory": 64, "cpu": 100 }),
        suspend: suspend.then(SuspendConfig::default),
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
            unknown_fields: Default::default(),
        }),
        ..TaskConfig::new(name, runtime)
    }
}

struct Cluster {
    _dir: tempfile::TempDir,
    svc: ManagerDaemonService,
    runtime: tokio::runtime::Runtime,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::tempdir()?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("store path")?.to_string(),
            scheduling_policy: "bin_pack".to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        let cluster = Cluster {
            _dir: dir,
            svc: ManagerDaemonService::new(store),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?,
        };
        cluster.store().insert_node(NodeMsg::from(node(
            "n-1",
            &["checkpoint", RUNTIME_DISABLE_CAPABILITY],
        )))?;
        Ok(cluster)
    }

    fn store(&self) -> &Store {
        &self.svc.store
    }

    fn submit(&self, job: &JobConfig) -> R<()> {
        self.store().handle_job_submit(
            job,
            &[Submission {
                path: format!("{}.json", job.name),
                content: "{}".to_string(),
                is_primary: true,
            }],
        )?;
        let config_id = self.store().get_job_config(&job.name)?.id();
        self.store()
            .enqueue_eval("job_submit", Some(&job.name), Some(&config_id), None, None)?;
        Ok(())
    }

    fn run_eval_worker(&self) -> R<()> {
        let mut batch = self.store().claim_evals(LIMITS, ClaimScope::Any)?;
        while !batch.is_empty() {
            let completions = batch
                .iter()
                .map(|eval| (eval.id.clone(), resolve_claimed_eval(self.store(), 0, eval)))
                .collect();
            batch =
                self.store()
                    .complete_evals_and_claim_next(completions, LIMITS, ClaimScope::Any)?;
        }
        Ok(())
    }

    /// The eval worker, then the wake pass, until neither has work. Returns
    /// how many evals the wake passes returned to the queue.
    fn settle(&self) -> R<usize> {
        let mut woken = 0;
        loop {
            self.run_eval_worker()?;
            let now = self.store().wake_blocked_evals()?;
            if now == 0 {
                // A wake pass can queue a capacity pass without waking a
                // blocked eval (a job that placed part of its work).
                self.run_eval_worker()?;
                return Ok(woken);
            }
            woken += now;
        }
    }

    fn set(&self, request: SetRuntimeEnabledRequest) -> Result<SetRuntimeEnabledReply, Status> {
        self.runtime
            .block_on(self.svc.set_runtime_enabled(Request::new(request)))
            .map(|reply| reply.into_inner())
    }

    fn disable(
        &self,
        runtime: &str,
        namespaces: &[&str],
        drain: bool,
    ) -> R<SetRuntimeEnabledReply> {
        Ok(self.set(SetRuntimeEnabledRequest {
            runtime: runtime.into(),
            enabled: false,
            namespaces: namespaces.iter().map(|glob| glob.to_string()).collect(),
            reason: "CVE-2024-21626".into(),
            drain,
            idempotency_key: None,
        })?)
    }

    fn enable(&self, runtime: &str, namespaces: &[&str]) -> Result<SetRuntimeEnabledReply, Status> {
        self.set(SetRuntimeEnabledRequest {
            runtime: runtime.into(),
            enabled: true,
            namespaces: namespaces.iter().map(|glob| glob.to_string()).collect(),
            ..Default::default()
        })
    }

    fn status(&self) -> R<Vec<legion::structs::RuntimeStateStatus>> {
        Ok(self
            .runtime
            .block_on(
                self.svc
                    .list_runtime_states(Request::new(ListRuntimeStatesRequest {})),
            )?
            .into_inner()
            .disabled)
    }

    /// Live allocations of `job_id`.
    fn running(&self, job_id: &str) -> R<usize> {
        Ok(self
            .store()
            .cached_allocations_by_node()?
            .values()
            .flatten()
            .filter(|allocation| allocation.job_id == job_id)
            .count())
    }

    fn blocked(&self, job_id: &str) -> R<Option<BlockedEval>> {
        Ok(self
            .store()
            .blocked_evals()?
            .into_iter()
            .find(|eval| eval.job_id == job_id))
    }

    fn event_kinds(&self) -> R<Vec<String>> {
        Ok(self
            .store()
            .list_scheduler_events(&Default::default())?
            .into_iter()
            .map(|event| event.kind)
            .collect())
    }
}

/// Phase 1 acceptance: a disable leaves running allocations alone, blocks a
/// new placement with the reason, is not woken by capacity, and after
/// `enable` the blocked work places without a resubmit.
#[test]
fn a_disable_blocks_placement_with_the_reason_and_enable_places_it() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.submit(&job("web", vec![service("web", "noop", 2)]))?;
    cluster.settle()?;
    assert_eq!(cluster.running("web")?, 2);

    let reply = cluster.disable("noop", &[], false)?;
    assert!(reply.changed);
    let state = reply.state.clone().context("state")?;
    assert_eq!(state.runtime, "noop");
    assert!(state.namespaces.is_empty(), "cluster-wide");
    assert_eq!(state.reason, "CVE-2024-21626");
    assert_eq!(state.by, "acl-disabled", "this cluster runs with ACLs off");
    assert!(!state.since.is_empty());
    assert_eq!(reply.running.context("running")?.allocations, 2);
    assert!(reply.drained.is_none());
    // The same disable again changes nothing.
    assert!(!cluster.disable("noop", &[], false)?.changed);

    // Running work is untouched.
    cluster.settle()?;
    assert_eq!(cluster.running("web")?, 2);

    // A job admitted during the incident is accepted and blocks.
    cluster.submit(&job("api", vec![service("api", "noop", 1)]))?;
    assert_eq!(cluster.settle()?, 0);
    assert_eq!(cluster.running("api")?, 0);
    let blocked = cluster.blocked("api")?.context("the eval blocked")?;
    assert_eq!(blocked.key, "runtime_disabled:noop");
    let message = blocked.detail["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("runtime \"noop\" is disabled in namespace default since")
            && message.contains("CVE-2024-21626")
            && message.contains("legion operator runtime status"),
        "{message}"
    );
    let info = cluster
        .runtime
        .block_on(cluster.svc.get_job_info(Request::new(GetJobInfoRequest {
            name: "api".to_string(),
        })))?
        .into_inner()
        .job
        .context("job")?;
    assert_eq!(
        info.blocked.context("job info says why")?.key,
        "runtime_disabled:noop"
    );

    // Capacity does not wake it: no node can help.
    cluster.store().insert_node(NodeMsg::from(node(
        "n-2",
        &["checkpoint", RUNTIME_DISABLE_CAPABILITY],
    )))?;
    cluster.store().request_capacity_reconcile()?;
    assert_eq!(cluster.settle()?, 0);
    assert_eq!(cluster.running("api")?, 0);

    // An update of a job that is already running waits whole: nothing new
    // starts, and the version that is running is not replaced.
    cluster.submit(&job("web", vec![service("web", "noop", 3)]))?;
    cluster.settle()?;
    assert_eq!(cluster.running("web")?, 2, "the running version stays");
    assert_eq!(
        cluster.blocked("web")?.context("the update blocked")?.key,
        "runtime_disabled:noop"
    );

    let status = cluster.status()?;
    assert_eq!(status.len(), 1);
    assert!(status[0].blocked_evals >= 1);
    assert_eq!(status[0].running.clone().context("running")?.allocations, 2);
    assert!(status[0].unaware_nodes.is_empty());

    // Enable: blocked work places on its own.
    let enabled = cluster.enable("noop", &[]).map_err(anyhow::Error::new)?;
    assert!(enabled.changed);
    assert!(enabled.state.is_none());
    assert!(cluster.settle()? >= 1, "the blocked eval woke");
    assert_eq!(cluster.running("api")?, 1);
    assert_eq!(cluster.running("web")?, 3);
    assert!(cluster.blocked("api")?.is_none());
    assert!(cluster.status()?.is_empty());
    // Enabling again is a no-op, not an error.
    assert!(
        !cluster
            .enable("noop", &[])
            .map_err(anyhow::Error::new)?
            .changed
    );

    let kinds = cluster.event_kinds()?;
    assert!(
        kinds.iter().any(|kind| kind == "runtime.disabled"),
        "{kinds:?}"
    );
    assert!(
        kinds.iter().any(|kind| kind == "runtime.enabled"),
        "{kinds:?}"
    );
    Ok(())
}

/// A scoped disable covers only the namespaces its globs match, and is
/// lifted one glob at a time.
#[test]
fn a_scoped_disable_leaves_other_namespaces_alone() -> R<()> {
    let cluster = Cluster::new()?;
    for name in ["tenants-acme", "ci"] {
        cluster.store().apply_namespace(Namespace {
            name: name.into(),
            ..Default::default()
        })?;
    }
    let reply = cluster.disable("noop", &["tenants-*"], false)?;
    assert_eq!(reply.state.context("state")?.namespaces, ["tenants-*"]);

    cluster.submit(&job("tenants-acme/web", vec![service("web", "noop", 1)]))?;
    cluster.submit(&job("ci/build", vec![service("build", "noop", 1)]))?;
    cluster.submit(&job("web", vec![service("web", "noop", 1)]))?;
    cluster.settle()?;
    assert_eq!(cluster.running("tenants-acme/web")?, 0);
    assert_eq!(cluster.running("ci/build")?, 1, "another namespace");
    assert_eq!(cluster.running("web")?, 1, "the default namespace");
    let blocked = cluster.blocked("tenants-acme/web")?.context("blocked")?;
    assert!(
        blocked.detail["message"]
            .as_str()
            .unwrap_or_default()
            .contains("disabled in namespace tenants-acme"),
        "{blocked:?}"
    );

    // Another runtime in a disabled namespace starts as ever.
    cluster.submit(&job(
        "tenants-acme/other",
        vec![service("other", "exec", 1)],
    ))?;
    cluster.settle()?;
    assert_eq!(cluster.running("tenants-acme/other")?, 1);

    // A glob the record does not hold is refused by name.
    let err = cluster.enable("noop", &["payments"]).unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("is not disabled for"), "{err}");
    // Widen to the cluster, and a namespace can no longer be carved out.
    assert!(cluster
        .disable("noop", &[], false)?
        .state
        .context("state")?
        .namespaces
        .is_empty());
    let err = cluster.enable("noop", &["tenants-*"]).unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("disabled cluster-wide"), "{err}");

    cluster.enable("noop", &[]).map_err(anyhow::Error::new)?;
    cluster.settle()?;
    assert_eq!(cluster.running("tenants-acme/web")?, 1);
    Ok(())
}

/// A job with one task on a disabled runtime still places its other tasks,
/// and the disabled one places when the runtime is enabled.
#[test]
fn a_mixed_job_places_what_it_can_and_the_rest_on_enable() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.disable("noop", &[], false)?;
    cluster.submit(&job(
        "mixed",
        vec![service("front", "exec", 1), service("back", "noop", 2)],
    ))?;
    cluster.settle()?;
    assert_eq!(cluster.running("mixed")?, 1, "the exec task placed");
    cluster.enable("noop", &[]).map_err(anyhow::Error::new)?;
    cluster.settle()?;
    assert_eq!(cluster.running("mixed")?, 3);
    Ok(())
}

/// Work that would replace running allocations waits whole while one of the
/// job's tasks is on a disabled runtime: a rollout tick pauses and looks
/// again on the clock without failing its rollout, a restart blocks before
/// it stops anything, and the running version is untouched throughout.
#[test]
fn a_rollout_pauses_and_a_restart_waits_while_the_runtime_is_disabled() -> R<()> {
    let cluster = Cluster::new()?;
    let store = cluster.store();
    cluster.submit(&job("web", vec![service("web", "noop", 2)]))?;
    cluster.settle()?;
    assert_eq!(cluster.running("web")?, 2);
    cluster.disable("noop", &[], false)?;

    // The next version, and the rollout tick an update queues for it.
    let next = job("web", vec![service("web", "noop", 3)]);
    store.handle_job_submit(
        &next,
        &[Submission {
            path: "web.json".into(),
            content: "{}".into(),
            is_primary: true,
        }],
    )?;
    let config_id = store.get_job_config("web")?.id();
    store.enqueue_eval("rollout_tick", Some("web"), Some(&config_id), None, None)?;
    let batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
    assert_eq!(batch.len(), 1);
    let completion = resolve_claimed_eval(store, 0, &batch[0]);
    match &completion {
        EvalCompletion::Released { delay, .. } => {
            assert_eq!(
                *delay,
                Duration::from_secs(15),
                "it looks again on the clock"
            )
        }
        other => panic!("the tick should be requeued, not settled: {other:?}"),
    }
    store.complete_evals_and_claim_next(
        vec![(batch[0].id.clone(), completion)],
        LIMITS,
        ClaimScope::Any,
    )?;
    assert_eq!(cluster.running("web")?, 2, "nothing was replaced");
    assert!(
        store.latest_rollout_for_job_config(&config_id)?.is_none(),
        "no rollout was failed"
    );

    // A restart of the job waits too, before it stops anything.
    store.enqueue_eval("job_update", Some("web"), Some(&config_id), None, None)?;
    let batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
    let restart = batch
        .iter()
        .find(|eval| eval.kind == "job_update")
        .context("the restart eval")?;
    let completion = resolve_claimed_eval(store, 0, restart);
    assert!(
        matches!(&completion, EvalCompletion::Blocked { key, .. } if key == "runtime_disabled:noop"),
        "{completion:?}"
    );
    assert_eq!(cluster.running("web")?, 2, "nothing was stopped");
    Ok(())
}

/// Invalid requests are refused before anything is written.
#[test]
fn invalid_requests_are_refused() -> R<()> {
    let cluster = Cluster::new()?;
    for request in [
        SetRuntimeEnabledRequest::default(),
        SetRuntimeEnabledRequest {
            runtime: "envelope".into(),
            ..Default::default()
        },
        SetRuntimeEnabledRequest {
            runtime: "noop".into(),
            namespaces: vec!["Not A Namespace".into()],
            ..Default::default()
        },
        SetRuntimeEnabledRequest {
            runtime: "noop".into(),
            enabled: true,
            drain: true,
            ..Default::default()
        },
        SetRuntimeEnabledRequest {
            runtime: "noop".into(),
            reason: "x".repeat(600),
            ..Default::default()
        },
    ] {
        let err = cluster.set(request.clone()).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{request:?}: {err}");
    }
    assert!(cluster.status()?.is_empty());
    Ok(())
}

/// A retried write with the same idempotency key is answered without
/// writing or draining again.
#[test]
fn a_retried_disable_is_deduplicated() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.submit(&job("web", vec![service("web", "noop", 2)]))?;
    cluster.settle()?;
    let request = SetRuntimeEnabledRequest {
        runtime: "noop".into(),
        reason: "CVE-1".into(),
        drain: true,
        idempotency_key: Some("incident-42".into()),
        ..Default::default()
    };
    let first = cluster.set(request.clone()).map_err(anyhow::Error::new)?;
    assert!(first.changed && !first.deduplicated);
    assert_eq!(first.drained.context("drained")?.stopping, 2);
    let second = cluster.set(request).map_err(anyhow::Error::new)?;
    assert!(second.deduplicated && !second.changed);
    assert!(second.drained.is_none(), "the drain ran once");
    assert_eq!(second.state, first.state);
    Ok(())
}

/// Phase 2: `--drain` gracefully stops allocations that cannot be
/// suspended (cause `runtime_disabled`), starts a suspend for a keyed
/// instance whose task has a `suspend` block, and leaves no replacement
/// running until the runtime is enabled.
#[test]
fn a_drain_stops_services_and_suspends_what_can_be_suspended() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.submit(&job("web", vec![service("web", "noop", 2)]))?;
    cluster.submit(&job("agents", vec![keyed("workspace", "noop", true)]))?;
    cluster.submit(&job("plain", vec![keyed("scratch", "noop", false)]))?;
    cluster.submit(&job("other", vec![service("other", "exec", 1)]))?;
    cluster.settle()?;
    for (job_id, task, id) in [
        ("agents", "workspace", "user-1"),
        ("plain", "scratch", "tmp-1"),
    ] {
        cluster
            .store()
            .create_keyed_instance(job_id, "default", task, Some(id))?;
        let config_id = cluster.store().get_job_config(job_id)?.id();
        cluster.store().enqueue_eval(
            "job_reschedule",
            Some(job_id),
            Some(&config_id),
            None,
            None,
        )?;
    }
    cluster.settle()?;
    assert_eq!(cluster.running("web")?, 2);
    assert_eq!(cluster.running("agents")?, 1);
    assert_eq!(cluster.running("plain")?, 1);

    let reply = cluster.disable("noop", &[], true)?;
    let state = reply.state.clone().context("state")?;
    assert!(state.drain);
    let drained = reply.drained.context("drained")?;
    assert!(drained.failed.is_empty(), "{:?}", drained.failed);
    assert_eq!(
        drained.suspending, 1,
        "the keyed instance with a suspend block"
    );
    assert_eq!(
        drained.stopping, 3,
        "two service replicas and the plain instance"
    );

    // The suspend is a capture in flight on the instance's node; the
    // allocation leaves when the node reports it, as for any suspend.
    assert_eq!(
        cluster
            .store()
            .instance_suspend_state("user-1")?
            .pending_op
            .as_deref(),
        Some("suspend")
    );
    // Stopped work is gone from the desired set, with its cause recorded.
    assert_eq!(cluster.running("web")?, 0);
    assert_eq!(cluster.running("plain")?, 0);
    let stops = cluster.store().stop_notices_for_node("n-1")?;
    assert_eq!(stops.len(), 3);
    assert!(
        stops
            .iter()
            .all(|notice| notice.cause.as_str() == "runtime_disabled"),
        "{stops:?}"
    );
    // Another runtime is untouched.
    assert_eq!(cluster.running("other")?, 1);

    // Nothing replaces what was stopped: the jobs block on the runtime.
    cluster.settle()?;
    assert_eq!(cluster.running("web")?, 0);
    assert_eq!(
        cluster.blocked("web")?.context("web blocked")?.key,
        "runtime_disabled:noop"
    );
    let kinds = cluster.event_kinds()?;
    assert!(
        kinds.iter().any(|kind| kind == "allocation.stopping"),
        "{kinds:?}"
    );

    // Enable: the stopped service comes back on its own.
    cluster.enable("noop", &[]).map_err(anyhow::Error::new)?;
    cluster.settle()?;
    assert_eq!(cluster.running("web")?, 2);
    assert_eq!(cluster.running("plain")?, 1);
    Ok(())
}

/// Workers learn the set on every heartbeat reply, and their refusal counts
/// reach `ListRuntimeStates`. A node whose worker predates runtime disables
/// is named.
#[test]
fn heartbeats_carry_the_set_and_report_refusals() -> R<()> {
    let cluster = Cluster::new()?;
    cluster
        .store()
        .insert_node(NodeMsg::from(node("old-1", &["checkpoint"])))?;
    let beat = |refusals: Vec<RuntimeRefusals>| -> R<legion::structs::HeartbeatReply> {
        Ok(cluster
            .runtime
            .block_on(cluster.svc.heartbeat(Request::new(HeartbeatRequest {
                id: "n-1".into(),
                runtime_refusals: refusals,
                ..Default::default()
            })))?
            .into_inner())
    };
    let before = beat(Vec::new())?.runtime_states.context("always set")?;
    assert!(before.states.is_empty());

    cluster.disable("exec", &["tenants-*"], true)?;
    let reply = beat(vec![RuntimeRefusals {
        runtime: "exec".into(),
        count: 7,
    }])?;
    let set = RuntimeDisableSet::from_proto(&reply.runtime_states.context("set")?);
    let disable = set.refusing("exec", "tenants-a/web").context("in scope")?;
    assert!(disable.drain);
    assert!(set.refusing("exec", "web").is_none());

    let status = cluster.status()?;
    assert_eq!(status[0].edge_refusals, 7);
    assert_eq!(status[0].unaware_nodes, ["old-1"]);

    cluster
        .enable("exec", &["tenants-*"])
        .map_err(anyhow::Error::new)?;
    assert!(beat(Vec::new())?
        .runtime_states
        .context("set")?
        .states
        .is_empty());
    Ok(())
}

// --- The worker half: a real node refusing starts itself ------------------

const ISSUER: &str = "https://manager.example.com:4747";
const JOB_CONFIG: &str = "jc-runtime-disable";
const EDGE_JOB: &str = "tenants-acme/agent-sandboxes";

struct Jwks(Value);

impl JwksSource for Jwks {
    fn fetch(&self) -> R<Value> {
        Ok(self.0.clone())
    }
}

/// A ratifier nothing should reach: a refused start never asks a manager.
struct NoRatifier;

#[tonic::async_trait]
impl ClaimRatifier for NoRatifier {
    async fn ratify(&self, _request: RatifyClaimRequest) -> R<RatifyClaimReply> {
        Ok(RatifyClaimReply {
            confirmed: false,
            detail: "the test ratifies nothing".into(),
            ..Default::default()
        })
    }
}

fn edge_task() -> TaskConfig {
    TaskConfig {
        speculative_start: Some(SpeculativeStartConfig {
            mode: Some("serve".into()),
            ratify_timeout: Some("2s".into()),
            on_refused: None,
            unknown_fields: Default::default(),
        }),
        config: json!({ "command": ["sleep", "30"] }),
        resources: json!({ "memory": 64 }),
        ..TaskConfig::new("workspace", "exec")
    }
}

struct EdgeNode {
    service: WorkerDaemonService,
    issuer: Issuer,
    dir: tempfile::TempDir,
}

fn edge_node() -> R<EdgeNode> {
    let dir = tempfile::tempdir()?;
    let issuer = Issuer::new(
        ISSUER,
        "/.well-known/jwks.json",
        Duration::from_secs(3600),
        None,
        SystemTime::now(),
    )?;
    let verifier = BootTicketVerifier::new(
        Box::new(Jwks(issuer.jwks())),
        ISSUER,
        "edge-eu",
        DEFAULT_JWKS_REFRESH,
        DEFAULT_CLOCK_SKEW,
    );
    let mut manager = WorkerTaskManager::with_state_store(
        legion::worker::state::WorkerStateStore::new(dir.path().join("worker_state.json")),
    );
    manager.load_builtin_plugins();
    let spec_cache: edge::SpecCache = Arc::new(Mutex::new(
        [(JOB_CONFIG.to_string(), vec![edge_task()])]
            .into_iter()
            .collect(),
    ));
    let service = WorkerDaemonService {
        node_id: "edge-1".into(),
        task_manager: Arc::new(Mutex::new(manager)),
        identity: None,
        edge_boot: Some(Arc::new(EdgeBootRuntime {
            verifier: Arc::new(verifier),
            spec_cache,
            node_id: "edge-1".into(),
            ratifier: Arc::new(NoRatifier),
        })),
        direct_exec: None,
        launch_pool: Arc::new(legion::worker::launch_pool::LaunchPool::new(
            8,
            Duration::from_secs(1),
        )),
        node_identity: None,
        netd: None,
    };
    Ok(EdgeNode {
        service,
        issuer,
        dir,
    })
}

impl EdgeNode {
    fn ticket(&self, instance: &str) -> R<String> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let claims = json!({
            "legion.io/scope": "boot",
            "legion.io/job": EDGE_JOB,
            "legion.io/job_config": JOB_CONFIG,
            "legion.io/task": "workspace",
            "legion.io/instance": instance,
            "legion.io/pool": "edge-eu",
            "legion.io/gate": "serve",
            "legion.io/uses": 1,
        });
        Ok(self
            .issuer
            .issue_boot_ticket(
                &format!("tkt_{instance}"),
                "boot:agent-sandboxes/workspace",
                claims.as_object().cloned().unwrap_or_default(),
                now,
                now + 300,
            )?
            .token)
    }

    async fn start(&self, instance: &str, ticket: &str) -> Result<(), Status> {
        self.service
            .start_claim(Request::new(StartClaimRequest {
                ticket: ticket.to_string(),
                instance_id: instance.to_string(),
                claim_id: format!("clm_{instance}"),
                gate: 0,
                dry_run: false,
            }))
            .await
            .map(|_| ())
    }

    fn claims(&self) -> usize {
        self.service
            .task_manager
            .lock()
            .expect("task manager")
            .claims_snapshot()
            .len()
    }

    fn gate(&self) -> Arc<legion::worker::runtime_state::RuntimeGate> {
        self.service
            .task_manager
            .lock()
            .expect("task manager")
            .runtime_gate()
    }
}

/// An edge start with no manager on the path is refused by the node itself
/// once it holds the disable, with the disable's own sentence, a retryable
/// code, and the ticket unspent; out of scope and after `enable` it starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_refuses_an_edge_start_of_a_disabled_runtime_test() -> R<()> {
    let node = edge_node()?;
    // The set a heartbeat reply (or a StreamWork push) hands the worker.
    let disables = |namespaces: &[&str]| {
        RuntimeDisableSet::new(vec![legion::structs::runtime_state::RuntimeDisable {
            runtime: "exec".into(),
            namespaces: namespaces.iter().map(|glob| glob.to_string()).collect(),
            reason: "GHSA-test".into(),
            since: "2026-11-07T18:04:11Z".into(),
            by: "accessor:7f3c".into(),
            drain: false,
        }])
    };
    node.gate().replace(disables(&["tenants-*"]));

    let ticket = node.ticket("user-a")?;
    let refused = node.start("user-a", &ticket).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unavailable, "{refused}");
    assert!(
        refused.message().contains(
            "runtime \"exec\" is disabled in namespace tenants-acme since \
             2026-11-07T18:04:11Z: GHSA-test (legion operator runtime status)"
        ),
        "{refused}"
    );
    assert_eq!(
        refused
            .metadata()
            .get("legion-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("RUNTIME_DISABLED")
    );
    assert_eq!(node.claims(), 0, "nothing was admitted");
    let reported = node.gate().take_refusals();
    assert_eq!(reported.len(), 1);
    assert_eq!(
        (reported[0].runtime.as_str(), reported[0].count),
        ("exec", 1)
    );

    // A worker that restarts with no manager in reach still holds the set.
    let restarted = legion::worker::runtime_state::RuntimeGate::beside(
        &node.dir.path().join("worker_state.json"),
    );
    assert!(restarted.check("exec", EDGE_JOB).is_err());

    // Enabled again: the same ticket, never spent, starts the work.
    node.gate().replace(RuntimeDisableSet::default());
    node.start("user-a", &ticket)
        .await
        .map_err(anyhow::Error::new)?;

    // A disable scoped elsewhere does not touch this namespace.
    node.gate().replace(disables(&["payments"]));
    node.start("user-b", &node.ticket("user-b")?)
        .await
        .map_err(anyhow::Error::new)?;

    let mut guard = node.service.task_manager.lock().expect("task manager");
    for claim in guard.claims_snapshot() {
        guard.kill_claim(&claim.claim_id, "test over");
    }
    Ok(())
}
