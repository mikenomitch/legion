//! `GetCapacityDemand` end to end (autoscaler v2 plan §6.11, calls entry 7;
//! docs/autoscaling.md, "Capacity demand"): jobs submitted to a cluster with
//! no nodes are left pending by the real scheduler, and the demand read
//! sizes them against node-group templates.
//!
//! - a pool-restricted job counts only toward the template of its pool;
//! - a gang's quorum packs together;
//! - a replica larger than any template is `unfittable`, with each
//!   template's rule (`resources`, `permit`, `namespace`);
//! - `distinct` on a template-wide attribute hosts one replica;
//! - a node-namespace-restricted template hosts only its namespaces;
//! - upcoming nodes absorb work before any template is sized;
//! - a job whose eval has not run is not demand yet.

use crate::support::temp_store;
use legion::manager::daemon::resolve_claimed_eval;
use legion::manager::server::ManagerDaemonService;
use legion::prelude::{json, Context, Value, R};
use legion::storage::store::{ClaimScope, EvalBatchLimits, Store};
use legion::structs::manager_server::Manager;
use legion::structs::namespace::Namespace;
use legion::structs::{
    GetCapacityDemandReply, GetCapacityDemandRequest, NodeGroupDemand, NodeGroupShape,
    NodeGroupTemplate, PendingCapacity, RunJobRequest, WorkerConfig,
};
use tonic::Request;

const LIMITS: EvalBatchLimits = EvalBatchLimits {
    max: 16,
    workers: 1,
};

fn settle(store: &Store) -> R<()> {
    let mut batch = store.claim_evals(LIMITS, ClaimScope::Any)?;
    while !batch.is_empty() {
        let completions = batch
            .iter()
            .map(|eval| (eval.id.clone(), resolve_claimed_eval(store, 0, eval)))
            .collect();
        batch = store.complete_evals_and_claim_next(completions, LIMITS, ClaimScope::Any)?;
    }
    Ok(())
}

/// An exec service task.
fn task(name: &str, count: u32, cpu: u64, memory: u64) -> Value {
    json!({
        "name": name, "runtime": "exec", "lifecycle": "service", "count": count,
        "config": { "command": ["sleep", "600"], "user": "nobody" },
        "resources": { "cpu": cpu, "memory": memory },
    })
}

async fn run(svc: &ManagerDaemonService, namespace: &str, name: &str, task: Value) -> R<()> {
    svc.run_job(Request::new(RunJobRequest {
        namespace: Some(namespace.to_string()),
        parsed_jobspec: json!({ "name": name, "tasks": [task] }).to_string(),
        ..Default::default()
    }))
    .await?;
    Ok(())
}

fn template(name: &str, worker_type: &str, cpu: u64, memory: u64) -> NodeGroupTemplate {
    NodeGroupTemplate {
        name: name.into(),
        worker_type: worker_type.into(),
        shape: Some(NodeGroupShape {
            cpu: Some(cpu),
            memory: Some(memory),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn demand(
    svc: &ManagerDaemonService,
    templates: Vec<NodeGroupTemplate>,
) -> R<GetCapacityDemandReply> {
    Ok(svc
        .get_capacity_demand(Request::new(GetCapacityDemandRequest { templates }))
        .await?
        .into_inner())
}

fn group<'a>(reply: &'a GetCapacityDemandReply, name: &str) -> &'a NodeGroupDemand {
    reply
        .groups
        .iter()
        .find(|group| group.name == name)
        .unwrap_or_else(|| panic!("no group {name}"))
}

fn pending<'a>(reply: &'a GetCapacityDemandReply, job: &str) -> (u32, &'a PendingCapacity) {
    reply
        .pending
        .iter()
        .enumerate()
        .find(|(_, pending)| pending.job_id == job)
        .map(|(index, pending)| (index as u32, pending))
        .unwrap_or_else(|| panic!("no pending entry for {job}"))
}

/// The rule `group` refused `job` with.
fn refusal(reply: &GetCapacityDemandReply, group_name: &str, job: &str) -> Option<String> {
    let (index, _) = pending(reply, job);
    group(reply, group_name)
        .refusals
        .iter()
        .find(|refusal| refusal.pending_index == index)
        .map(|refusal| format!("{}: {}", refusal.rule, refusal.detail))
}

async fn cluster() -> R<ManagerDaemonService> {
    let store = temp_store("legion-capacity-demand-")?;
    for (name, pool, attributes, namespaces) in [
        ("default", "default", vec![], vec![]),
        ("gpu", "gpu", vec![("accel", "a100")], vec![]),
        ("tenant", "default", vec![], vec!["team-a".to_string()]),
    ] {
        store.insert_worker_type(&WorkerConfig {
            name: name.into(),
            pool: pool.into(),
            attributes: attributes
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            namespaces,
            ..Default::default()
        })?;
    }
    store.apply_namespace(Namespace {
        name: "team-a".into(),
        ..Default::default()
    })?;
    let svc = ManagerDaemonService::new(store);

    run(&svc, "default", "web", task("web", 5, 1000, 1024)).await?;
    let mut train = task("train", 2, 2000, 4096);
    train["permit"] = json!({ "pool": "gpu" });
    train["constraints"] = json!({ "accel": "a100" });
    run(&svc, "default", "train", train).await?;
    let mut mpi = task("mpi", 4, 1000, 512);
    mpi["gang"] = json!(true);
    run(&svc, "default", "mpi", mpi).await?;
    run(&svc, "default", "huge", task("huge", 1, 1000, 65536)).await?;
    let mut spread = task("spread", 3, 100, 64);
    spread["constraints"] = json!([{ "key": "zone", "operator": "distinct" }]);
    run(&svc, "default", "spread", spread).await?;
    run(&svc, "team-a", "team-app", task("app", 2, 500, 256)).await?;
    // No nodes: every eval ends blocked.
    settle(&svc.store)?;
    Ok(svc)
}

#[tokio::test]
async fn demand_sizes_pending_work_per_template_with_the_schedulers_rules_test() -> R<()> {
    let svc = cluster().await?;
    let mut general = template("general", "default", 4000, 8192);
    general
        .shape
        .as_mut()
        .context("shape")?
        .attributes
        .insert("zone".into(), "a".into());
    let reply = demand(
        &svc,
        vec![
            general,
            template("gpu", "gpu", 16000, 65536),
            template("tenant", "tenant", 4000, 8192),
        ],
    )
    .await?;
    assert!(reply.awaiting_scheduler.is_empty(), "{reply:#?}");

    // general: the mpi gang (4), web (5), team-app (2), and one spread
    // replica pack onto 3 nodes of 4000 MHz.
    let general = group(&reply, "general");
    assert_eq!(general.nodes, 3, "{general:#?}");
    assert_eq!(general.replicas, 12, "{general:#?}");
    assert_eq!(general.node_pool, "default");
    assert!(general
        .node_attributes_json
        .contains(r#""legion.nodegroup":"general""#));
    assert!(general.cpu_capacity_mhz >= 12000.0);

    // gpu: only the pool-restricted job, whose constraint the type's
    // attribute satisfies; the rest are refused by their permit.
    let gpu = group(&reply, "gpu");
    assert_eq!((gpu.nodes, gpu.replicas), (1, 2), "{gpu:#?}");
    assert_eq!(gpu.node_pool, "gpu");
    let web_on_gpu = refusal(&reply, "gpu", "web").context("web refused on gpu")?;
    assert!(web_on_gpu.starts_with("permit: "), "{web_on_gpu}");
    let train_on_general =
        refusal(&reply, "general", "train").context("train refused on general")?;
    assert!(
        train_on_general.starts_with("permit: "),
        "{train_on_general}"
    );

    // tenant: a namespace-restricted node hosts only team-a.
    let tenant = group(&reply, "tenant");
    assert_eq!((tenant.nodes, tenant.replicas), (1, 2), "{tenant:#?}");
    let web_on_tenant = refusal(&reply, "tenant", "web").context("web refused on tenant")?;
    assert!(
        web_on_tenant.starts_with("namespace: ") && web_on_tenant.contains("[team-a]"),
        "{web_on_tenant}"
    );

    // The gang and its quorum.
    let (_, mpi) = pending(&reply, "mpi");
    assert_eq!((mpi.pending, mpi.gang_quorum), (4, Some(4)));
    let (_, web) = pending(&reply, "web");
    assert_eq!((web.desired, web.placed, web.pending), (5, 0, 5));
    let resources: Value = serde_json::from_str(&web.resources_json)?;
    assert_eq!(resources["cpu"], 1000.0);

    // huge fits nowhere, and every template says why.
    let (huge_index, _) = pending(&reply, "huge");
    let huge = reply
        .unfittable
        .iter()
        .find(|entry| entry.pending_index == huge_index)
        .context("huge is unfittable")?;
    assert_eq!(huge.replicas, 1);
    let rules: Vec<(&str, &str)> = huge
        .reasons
        .iter()
        .map(|reason| (reason.template.as_str(), reason.rule.as_str()))
        .collect();
    assert_eq!(
        rules,
        vec![
            ("general", "resources"),
            ("gpu", "permit"),
            ("tenant", "namespace")
        ]
    );
    assert!(
        huge.reasons[0]
            .detail
            .contains("memory 65536 > 8192 per node"),
        "{}",
        huge.reasons[0].detail
    );

    // distinct on the template's zone hosts one replica; the other two
    // have nowhere to go.
    let (spread_index, _) = pending(&reply, "spread");
    let spread = reply
        .unfittable
        .iter()
        .find(|entry| entry.pending_index == spread_index)
        .context("spread is partly unfittable")?;
    assert_eq!(spread.replicas, 2);
    assert_eq!(spread.reasons[0].rule, "distinct");
    Ok(())
}

#[tokio::test]
async fn upcoming_nodes_absorb_work_and_unrun_evals_are_not_demand_test() -> R<()> {
    let svc = cluster().await?;
    let mut general = template("general", "default", 4000, 8192);
    general.upcoming = 2;
    let reply = demand(&svc, vec![general]).await?;
    // Two upcoming nodes take the mpi gang and four web replicas; one more
    // node hosts the rest of web and team-app.
    let general = group(&reply, "general");
    assert_eq!(general.absorbed, 8, "{general:#?}");
    assert_eq!(general.nodes, 1, "{general:#?}");
    let (_, web) = pending(&reply, "web");
    assert_eq!(web.absorbed, 4);
    let (_, mpi) = pending(&reply, "mpi");
    assert_eq!(mpi.absorbed, 4, "a gang is absorbed whole");

    // A job whose eval has not run is listed, not sized.
    run(&svc, "default", "late", task("late", 1, 100, 64)).await?;
    let reply = demand(&svc, vec![template("general", "default", 4000, 8192)]).await?;
    assert_eq!(reply.awaiting_scheduler, vec!["late".to_string()]);
    assert!(reply.pending.iter().all(|pending| pending.job_id != "late"));

    // Validation.
    let empty = svc
        .get_capacity_demand(Request::new(GetCapacityDemandRequest { templates: vec![] }))
        .await
        .expect_err("no templates");
    assert_eq!(empty.code(), tonic::Code::InvalidArgument);
    let twice = svc
        .get_capacity_demand(Request::new(GetCapacityDemandRequest {
            templates: vec![
                template("g", "default", 1, 1),
                template("g", "default", 1, 1),
            ],
        }))
        .await
        .expect_err("duplicate names");
    assert_eq!(twice.code(), tonic::Code::InvalidArgument);
    let missing = svc
        .get_capacity_demand(Request::new(GetCapacityDemandRequest {
            templates: vec![NodeGroupTemplate {
                name: "g".into(),
                like_node: Some("no-such-node".into()),
                ..Default::default()
            }],
        }))
        .await
        .expect_err("like_node must exist");
    assert_eq!(missing.code(), tonic::Code::NotFound);
    Ok(())
}
