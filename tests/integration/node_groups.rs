//! Node groups end to end (docs/autoscaling.md, "Node groups"; autoscaler-v2
//! plan §9 Phase 5, acceptance P5).
//!
//! The autoscaler's engine runs in process against a real manager served
//! over gRPC (the [`Harness`] of `autoscaler.rs`), with a scripted `exec`
//! provider: a Python script that keeps its instances in a state file, the
//! way a cloud keeps an Auto Scaling group. The test plays the machines:
//! every instance the provider starts joins as a worker with the group's
//! join token and `--attr legion.provider-id=<instance>`.
//!
//! - Sustained work no node can host provisions the simulated node count
//!   from zero; the nodes join carrying `legion.nodegroup` and their
//!   provider id, and the work lands on them.
//! - When the work goes, empty nodes drain, terminate, and vanish from
//!   `legion node list`, at most one per `down.every`; a node hosting a
//!   `legion-system` allocation is never chosen.
//! - Every step is an action intent in the group's history, and exactly
//!   one `target/scale` reaches the provider per decision.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use legion::apps::autoscaler::EngineOptions;
use legion::prelude::{json, Context, Value, R};
use legion::structs::token::JoinToken;
use legion::structs::{
    ApplyScalingPolicyRequest, GetNodesRequest, GetScaleHistoryRequest, GetScalingStatusRequest,
    StopJobRequest, WorkerRegisterRequest,
};

use crate::autoscaler::{Harness, T0, TICK_MS};

const MIB: u64 = 1024 * 1024;

/// The scripted provider: `<script> <dir> status|scale|terminate`, the
/// request on stdin. State (desired count, instances, the last join token)
/// in `<dir>/state.json`; every scale and terminate appended to
/// `<dir>/calls.log`.
const PROVIDER: &str = r#"
import json, os, sys
directory, op = sys.argv[1], sys.argv[2]
request = json.loads(sys.stdin.readline() or "{}")
path = os.path.join(directory, "state.json")
state = {"desired": 0, "next": 0, "instances": [], "token": None}
if os.path.exists(path):
    with open(path) as f:
        state = json.load(f)
log = open(os.path.join(directory, "calls.log"), "a")
if op == "status":
    print(json.dumps({"count": state["desired"], "ready": len(state["instances"]),
                      "members": [{"id": i, "state": "running"} for i in state["instances"]]}))
elif op == "scale":
    desired = int(request["desired"])
    log.write("scale %d %s\n" % (desired, request.get("decision", "")))
    join = request.get("join") or {}
    if join.get("token"):
        state["token"] = join["token"]
    while len(state["instances"]) < desired:
        state["next"] += 1
        state["instances"].append("i-%d" % state["next"])
    state["desired"] = desired
    print("{}")
elif op == "terminate":
    for instance in request.get("ids", []):
        log.write("terminate %s\n" % instance)
        if instance in state["instances"]:
            state["instances"].remove(instance)
            state["desired"] -= 1
    print("{}")
else:
    sys.stderr.write("unknown op %s\n" % op)
    sys.exit(2)
with open(path, "w") as f:
    json.dump(state, f)
"#;

pub(crate) struct Provider {
    pub(crate) dir: tempfile::TempDir,
    pub(crate) script: PathBuf,
}

impl Provider {
    pub(crate) fn new() -> R<Self> {
        let dir = tempfile::tempdir_in(crate::support::scratch_root())?;
        let script = dir.path().join("provider.py");
        std::fs::write(&script, PROVIDER)?;
        Ok(Self { dir, script })
    }

    pub(crate) fn plugin_config(&self) -> Value {
        json!({ "plugins": { "provider": { "builtin": "exec", "config": {
            "command": ["python3", self.script.display().to_string(), self.dir.path().display().to_string()],
        } } } })
    }

    /// An engine's options with this provider installed, as `instance`.
    pub(crate) fn options(&self, instance: &str) -> R<EngineOptions> {
        Ok(EngineOptions {
            plugins: legion::autoscaler::plugins::PluginCatalog::from_app_config(
                &self.plugin_config(),
            )
            .map_err(|err| anyhow::anyhow!(err))?,
            instance: instance.to_string(),
            ..Default::default()
        })
    }

    pub(crate) fn state(&self) -> R<Value> {
        let path = self.dir.path().join("state.json");
        if !path.exists() {
            return Ok(json!({ "desired": 0, "instances": [] }));
        }
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }

    pub(crate) fn instances(&self) -> R<Vec<String>> {
        Ok(self.state()?["instances"]
            .as_array()
            .map(|items| items.iter().filter_map(|i| i.as_str().map(str::to_string)).collect())
            .unwrap_or_default())
    }

    pub(crate) fn calls(&self) -> R<Vec<String>> {
        let path = self.dir.path().join("calls.log");
        if !path.exists() {
            return Ok(Vec::new());
        }
        Ok(std::fs::read_to_string(path)?.lines().map(str::to_string).collect())
    }
}

pub(crate) fn have_python() -> bool {
    std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success())
}

/// A two-core, 2400 MHz, 8 GiB machine, as its worker reports it.
fn fingerprint() -> String {
    json!({
        "memory_total": (8192 * MIB).to_string(),
        "cpus": { "cpu-1": { "frequency": "2400" }, "cpu-2": { "frequency": "2400" } }
    })
    .to_string()
}

/// Every instance the provider started that has no node yet joins, with
/// the token the provider was handed and its instance id.
pub(crate) async fn boot(h: &mut Harness, provider: &Provider) -> R<Vec<String>> {
    let state = provider.state()?;
    let Some(token) = state["token"].as_str() else {
        return Ok(Vec::new());
    };
    let secret = JoinToken::decode(token)
        .map_err(|err| anyhow::anyhow!("decoding the join token: {err}"))?
        .secret;
    let mut joined = Vec::new();
    for instance in provider.instances()? {
        let name = format!("w-{instance}");
        if h.store.get_node(&name).is_ok() {
            continue;
        }
        h.client
            .worker_register(WorkerRegisterRequest {
                name: name.clone(),
                pool: "default".into(),
                config_attrs: json!({ "maxAllocations": 20 }).to_string(),
                fingerprint_attrs: fingerprint(),
                health_checks: "[]".into(),
                join_token: Some(secret.clone()),
                join_attributes: [("legion.provider-id".to_string(), instance.clone())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            })
            .await
            .map_err(|status| anyhow::anyhow!("registering {name}: {}", status.message()))?;
        joined.push(name);
    }
    Ok(joined)
}

pub(crate) async fn node_ids(h: &mut Harness) -> R<BTreeSet<String>> {
    Ok(h.client
        .get_nodes(GetNodesRequest {
            page_size: Some(100),
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("GetNodes: {}", status.message()))?
        .into_inner()
        .nodes
        .into_iter()
        .map(|node| node.id)
        .collect())
}

pub(crate) fn group_policy(name: &str, max: u32) -> Value {
    json!({
        "name": name,
        "target": { "nodes": {
            "pool": "default",
            "shape": { "cpu": 4800, "memory": 8192 },
            "plugin": "provider",
        } },
        "min": 0,
        "max": max,
        "up": { "step": { "replicas": 10 }, "every": "1m", "joinTimeout": "5m" },
        // Underutilized, so a member that is busy but nearly idle is a
        // candidate too: a legion-system allocation must still protect it.
        "down": { "when": "underutilized", "for": "1m", "step": { "replicas": 1 },
                  "every": "2m", "drainDeadline": "5m" },
    })
}

/// Acceptance P5.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_group_grows_from_zero_and_drains_empty_nodes_test() -> R<()> {
    if !have_python() {
        eprintln!("python3 is not installed: skipping the scripted node provider");
        return Ok(());
    }
    let mut h = Harness::start(0).await?;
    let provider = Provider::new()?;
    h.install_plugins(provider.plugin_config())?;
    let applied = h
        .client
        .apply_scaling_policy(ApplyScalingPolicyRequest {
            spec_json: group_policy("workers", 6).to_string(),
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("apply: {}", status.message()))?
        .into_inner();
    assert_eq!(applied.kind, "nodes", "{applied:?}");

    // Six replicas of 2000 MHz: two fit on a 4800 MHz member, so three
    // members. Nothing can host them now.
    h.run(json!({
        "name": "batch",
        "task": {
            "name": "work", "runtime": "exec", "count": 6,
            "config": { "command": ["sleep", "3600"] },
            "resources": { "cpu": 2000, "memory": 1024 },
        }
    }))
    .await?;
    assert!(h.allocation_ids("batch")?.is_empty());

    let (mut engine, watch) = h.engine(provider.options("autoscaler")?).await?;
    let mut at = T0;
    let report = engine.tick(at).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(provider.state()?["desired"], 3, "{report:?}");
    assert_eq!(provider.calls()?.iter().filter(|c| c.starts_with("scale")).count(), 1);

    // The machines join, with the group's attribute and their ids.
    let joined = boot(&mut h, &provider).await?;
    assert_eq!(joined.len(), 3);
    for name in &joined {
        let attrs = h.store.get_node(name)?.config_attrs;
        assert_eq!(attrs["legion.nodegroup"], "workers", "{attrs}");
        assert_eq!(
            attrs["legion.provider-id"].as_str(),
            name.strip_prefix("w-"),
            "{attrs}"
        );
    }
    h.drain_evals()?;
    assert_eq!(h.allocation_ids("batch")?.len(), 6, "the work landed");

    // Steady: no more scale-ups while the work runs.
    for _ in 0..4 {
        at += TICK_MS;
        let report = engine.tick(at).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        h.drain_evals()?;
    }
    assert_eq!(provider.calls()?.iter().filter(|c| c.starts_with("scale")).count(), 1);

    // A legion-system allocation on one member: never a victim.
    let protected = joined[0].clone();
    let protected_id = protected.trim_start_matches("w-").to_string();
    h.store.ensure_system_namespace()?;
    h.run(json!({
        "name": "agent",
        "namespace": "legion-system",
        "task": {
            "name": "agent", "runtime": "exec", "count": 1,
            "config": { "command": ["sleep", "3600"], "user": "nobody" },
            "resources": { "cpu": 100, "memory": 64 },
            "constraints": { "legion.provider-id": protected_id },
            "permit": [{ "key": "pool", "operator": "equal", "value": "default" }],
        }
    }))
    .await?;
    assert_eq!(
        h.store.allocs_for_job("legion-system/agent")?.len(),
        1,
        "the system job landed on {protected}"
    );

    // The work goes: the members empty out.
    h.client
        .stop_job(StopJobRequest {
            name: "batch".into(),
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("stop: {}", status.message()))?;
    h.drain_evals()?;

    // Drains start at most one per 2 minutes; every terminated node is
    // forgotten once the provider stops listing it.
    let mut gone_at: BTreeMap<String, i64> = BTreeMap::new();
    for _ in 0..80 {
        at += TICK_MS;
        let report = engine.tick(at).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        h.drain_evals()?;
        let nodes = node_ids(&mut h).await?;
        for name in &joined {
            if !nodes.contains(name) {
                gone_at.entry(name.clone()).or_insert(at);
            }
        }
        if gone_at.len() == joined.len() - 1 {
            break;
        }
    }
    watch.abort();
    assert_eq!(gone_at.len(), 2, "{gone_at:?}");
    assert!(!gone_at.contains_key(&protected), "the legion-system node stays");
    assert!(node_ids(&mut h).await?.contains(&protected));
    let times: Vec<i64> = gone_at.values().copied().collect();
    assert!(
        (times[1] - times[0]).abs() >= 120_000,
        "two drains inside one down.every: {gone_at:?}"
    );
    let terminated: Vec<String> = provider
        .calls()?
        .into_iter()
        .filter(|call| call.starts_with("terminate"))
        .collect();
    assert_eq!(terminated.len(), 2, "{terminated:?}");
    assert_eq!(provider.instances()?.len(), 1);

    // The record: one intent per step, in the group's history.
    let history = h
        .client
        .get_scale_history(GetScaleHistoryRequest {
            policy: "workers".into(),
            limit: 200,
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("history: {}", status.message()))?
        .into_inner()
        .entries;
    let kinds: Vec<&str> = history.iter().map(|entry| entry.kind.as_str()).collect();
    for kind in ["scaled", "drain", "terminate", "forget"] {
        assert!(kinds.contains(&kind), "{kinds:?}");
    }
    assert!(!history
        .iter()
        .any(|entry| entry.summary.contains(&protected) && entry.kind == "drain"));

    let status = h
        .client
        .get_scaling_status(GetScalingStatusRequest {
            policy: "workers".into(),
            detail: true,
            ..Default::default()
        })
        .await
        .map_err(|status| anyhow::anyhow!("status: {}", status.message()))?
        .into_inner();
    let target = status.targets.first().context("the group's status")?;
    assert_eq!(target.kind, "nodes");
    assert_eq!(target.now, 1);
    assert!(
        target.decision_json.contains("legion-system"),
        "the protection is explained: {}",
        target.decision_json
    );
    Ok(())
}
