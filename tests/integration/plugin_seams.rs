//! Job admission rules and OCI runtime classes, through the manager's real
//! RPCs (device-model-and-plugin-seams plan; review findings PLG3, PLG4):
//!
//! - a namespace policy's CEL `jobRules` refuse a job at `job plan` and
//!   `job run`, naming the policy and the rule, and admit one that passes;
//! - a container task that names a runtime class places only on a node that
//!   advertises it (`container.runtimeClasses`).
//!
//! The manager runs as the RPC service over a local store with ACLs off.

use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::manager_server::Manager;
use legion::structs::{Node, NodeMsg, PlanJobRequest, RunJobRequest};
use tonic::Request;

fn node(id: &str, config_attrs: Value) -> Node {
    Node {
        id: id.to_string(),
        pool: "default".into(),
        status: "ready".into(),
        availability: "available".into(),
        last_heartbeat: "2026-01-01T00:00:00Z".into(),
        heartbeat_source: String::new(),
        config_attrs,
        fingerprint_attrs: json!({
            "memory_total": (8192u64 * 1024 * 1024).to_string(),
            "cpus": { "0": { "frequency": "3000" }, "1": { "frequency": "3000" } },
        }),
        dynamic_attrs: json!({}),
        health_checks: json!({}),
    }
}

fn container_job(name: &str, image: &str, extra: Value) -> String {
    let mut config = json!({ "image": image, "command": ["sleep", "600"] });
    if let Value::Object(extra) = extra {
        for (key, value) in extra {
            config[key] = value;
        }
    }
    json!({
        "name": name,
        "tasks": [{
            "name": "main",
            "runtime": "container",
            "config": config,
            "resources": { "memory": 64, "cpu": 100 },
        }],
    })
    .to_string()
}

struct Cluster {
    _dir: tempfile::TempDir,
    svc: ManagerDaemonService,
    runtime: tokio::runtime::Runtime,
}

impl Cluster {
    fn new() -> R<Self> {
        let dir = tempfile::Builder::new()
            .prefix("legion-plugin-seams-")
            .tempdir_in(crate::support::scratch_root())?;
        let store = Store::start_storage(&ManagerConfigFull {
            store_path: dir.path().to_str().context("store path")?.to_string(),
            scheduler_settings: Some(json!({ "nodes_considered": "100%" })),
            ..Default::default()
        })?;
        Ok(Cluster {
            _dir: dir,
            svc: ManagerDaemonService::new(store),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        })
    }

    fn join(&self, node: Node) -> R<()> {
        self.svc.store.insert_node(NodeMsg::from(node))?;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn plan(&self, spec: String) -> Result<Vec<String>, tonic::Status> {
        self.runtime
            .block_on(self.svc.plan_job(Request::new(PlanJobRequest {
                namespace: Some("default".into()),
                parsed_jobspec: spec,
                allow_unknown_fields: false,
            })))
            .map(|reply| {
                reply
                    .into_inner()
                    .allocations
                    .iter()
                    .map(|allocation| allocation.node_id.clone())
                    .collect()
            })
    }

    #[allow(clippy::result_large_err)]
    fn run(&self, spec: String) -> Result<(), tonic::Status> {
        self.runtime
            .block_on(self.svc.run_job(Request::new(RunJobRequest {
                namespace: Some("default".into()),
                parsed_jobspec: spec,
                submissions: vec![],
                priority: None,
                force_reschedule: false,
                idempotency_key: None,
                allow_unknown_fields: false,
                expected_version: None,
            })))
            .map(|_| ())
    }
}

/// PLG4: `jobRules` refuse at plan and run, naming the policy and the rule.
#[test]
fn job_rules_refuse_at_plan_and_run() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.join(node("n1", json!({ "maxAllocations": 10 })))?;
    let policy = legion::structs::namespace_policy::NamespacePolicy::from_json(
        &json!({
            "name": "house-rules",
            "namespaces": ["*"],
            "jobRules": [{
                "rule": "self.tasks.all(t, !has(t.config.image) || !t.config.image.endsWith(':latest'))",
                "message": "pin images by tag or digest, not :latest",
            }],
        })
        .to_string(),
    )
    .map_err(|err| anyhow::anyhow!(err))?;
    cluster.svc.store.apply_namespace_policy(policy)?;

    let latest = container_job("web", "nginx:latest", json!({}));
    let status = cluster.plan(latest.clone()).unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
    assert!(
        status
            .message()
            .contains("namespace policy 'house-rules' jobRules[0]: pin images"),
        "{}",
        status.message()
    );
    let status = cluster.run(latest).unwrap_err();
    assert!(
        status.message().contains("jobRules[0]"),
        "{}",
        status.message()
    );

    cluster
        .run(container_job("web", "nginx:1.27", json!({})))
        .map_err(|status| anyhow::anyhow!("a pinned image is admitted: {status}"))?;
    Ok(())
}

/// PLG3: a task that names a runtime class places only where the class is.
#[test]
fn a_runtime_class_task_places_only_where_the_class_is() -> R<()> {
    let cluster = Cluster::new()?;
    cluster.join(node("plain", json!({ "maxAllocations": 10 })))?;
    cluster.join(node(
        "sandboxed",
        json!({ "maxAllocations": 10, "container.runtimeClasses": ["gvisor"] }),
    ))?;
    for _ in 0..4 {
        let placed = cluster
            .plan(container_job(
                "agent",
                "python:3.12",
                json!({ "runtimeClass": "gvisor" }),
            ))
            .map_err(|status| anyhow::anyhow!("plan: {status}"))?;
        assert_eq!(placed, vec!["sandboxed".to_string()]);
    }
    // A class no node has places nowhere.
    let nowhere = cluster.plan(container_job(
        "agent",
        "python:3.12",
        json!({ "runtimeClass": "kata" }),
    ));
    assert!(
        nowhere.as_ref().map(Vec::is_empty).unwrap_or(true),
        "{nowhere:?}"
    );
    // A bad class name is refused at submit.
    let status = cluster
        .plan(container_job(
            "agent",
            "python:3.12",
            json!({ "runtimeClass": "not a name" }),
        ))
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    // The manager reports the whole context chain, so the runtime's reason
    // (which names the field) reaches the caller, not just "config invalid".
    assert!(
        status.message().contains("runtimeClass"),
        "{}",
        status.message()
    );
    Ok(())
}
