//! Integration coverage for declarable scheduling indexes
//! (docs/scheduling-indexes.md): the equivalence property (index-served
//! planning equals scan planning), truncation, shadow mode, explain output,
//! arming, and read-model lifecycle maintenance.

use serde_json::{json, Value};
use std::collections::HashMap;
use tempfile::Builder;

use legion::manager::scheduler::{PlanMode, Scheduler};
use legion::scheduling_index::IndexQueryOutcome;
use legion::storage::store::Store;
use legion::structs::config::ManagerConfigFull;
use legion::structs::{JobConfig, NodeMsg, Submission, TaskConfig, TaskCount};

fn store_with(scheduler_settings: Option<Value>) -> Store {
    let store_dir = Builder::new()
        .prefix("legion-index-test-")
        .tempdir()
        .expect("tempdir")
        .keep();
    let config = ManagerConfigFull {
        store_path: store_dir.to_str().expect("path").to_string(),
        scheduling_policy: "bin_pack".into(),
        task_resource_defaults: Some(json!({ "cpu": 0, "memory": 0, "allocations": 1 })),
        scheduler_settings,
        ..Default::default()
    };
    let store = Store::start_storage(&config).expect("store");
    store.reset_all_tables().expect("reset");
    store
}

fn insert_node(store: &Store, id: &str, pool: &str, config_attrs: Value) {
    let node = NodeMsg {
        id: id.to_string(),
        pool: pool.to_string(),
        status: "active".into(),
        availability: "available".into(),
        config_attrs: config_attrs.to_string(),
        fingerprint_attrs: json!({
            "memory_total": "8589934592",
            "cpus": { "cpu-1": { "frequency": "2400" } },
        })
        .to_string(),
        dynamic_attrs: json!({}).to_string(),
        health_checks: "{}".into(),
        last_heartbeat: "2023-01-01 00:00:00 +00:00:00".into(),
        heartbeat_source: String::new(),
    };
    store.insert_node(node).expect("insert node");
}

/// A 60-node fleet across 4 regions / 3 zones with mixed custom resources.
fn insert_fleet(store: &Store) {
    for idx in 0..60 {
        let attrs = json!({
            "region": format!("region-{}", idx % 4),
            "zone": format!("zone-{}", idx % 3),
            "tier": if idx % 2 == 0 { "web" } else { "batch" },
            // Custom resource capacity (schedulable) and a plain numeric
            // attribute (constrainable) — deliberately both.
            "resources": { "vram": (idx % 5) * 1024 },
            "vram_mb": (idx % 5) * 1024,
        });
        insert_node(store, &format!("node-{idx:03}"), "default", attrs);
    }
}

fn submit_job(store: &Store, job: &JobConfig) -> String {
    let submission = Submission {
        path: "test.ncl".into(),
        content: "{}".into(),
        is_primary: true,
    };
    store
        .handle_job_submit(job, &[submission])
        .expect("submit job");
    store.get_job_config(&job.name).expect("job config").id()
}

fn job_named(name: &str, tasks: Vec<TaskConfig>) -> JobConfig {
    JobConfig {
        priority: Default::default(),
        name: name.to_string(),
        tasks,
        rollout: None,
        rollout_by_group: Default::default(),
        version: None,
    }
}

/// The load-bearing equivalence property, asserted on the live path: in
/// shadow mode the scheduler computes BOTH the index candidate set and the
/// authoritative scan for every task and compares them — so
/// `shadow_agreement == true` across constraint shapes (plain equality,
/// list-form operators with unbindable rules, resource-requesting tasks
/// against a dims index) is exactly "index set == scan set". Placement
/// itself breaks score ties randomly, so sets — not picks — are the
/// meaningful invariant.
#[test]
fn shadow_equivalence_holds_across_constraint_shapes() {
    let settings = json!({
        "index_min_nodes": 1,
        "indexes": {
            "fit": false,
            "topo": { "partition_by": ["pool", "region"], "mode": "shadow" },
            "vram": { "partition_by": ["pool"], "dims": ["resources.vram"], "mode": "shadow" },
        },
    });

    let jobs = vec![
        job_named(
            "eq-constrained",
            // region = idx % 4 and tier alternates by idx % 2, so an even
            // region is the one that coexists with tier "web".
            vec![TaskConfig {
                count: TaskCount::new(5),
                constraints: json!({ "region": "region-2", "tier": "web" }),
                ..TaskConfig::new("web".to_string(), "noop")
            }],
        ),
        job_named(
            "list-form",
            // Mixes an equality binding (region) with a non-equality rule
            // (vram_mb >=) the index cannot bind — re-verification filters.
            vec![TaskConfig {
                count: TaskCount::new(3),
                constraints: json!([
                    { "key": "region", "value": "region-3" },
                    { "key": "vram_mb", "operator": ">=", "value": 2048 },
                ]),
                ..TaskConfig::new("batchy".to_string(), "noop")
            }],
        ),
        job_named(
            "resource-request",
            // A custom-resource request served by the dims index: the
            // orthant walk orders candidates but must never shrink the
            // untruncated set (ordering-not-filtering), or shadow disagrees.
            vec![TaskConfig {
                count: TaskCount::new(3),
                resources: json!({ "vram": 2048 }),
                ..TaskConfig::new("gpuish".to_string(), "noop")
            }],
        ),
        job_named(
            "per-zone",
            vec![TaskConfig {
                count: TaskCount {
                    num: 1,
                    per: Some("zone".to_string()),
                },
                constraints: json!({ "region": "region-1" }),
                ..TaskConfig::new("perzone".to_string(), "noop")
            }],
        ),
    ];

    let store = store_with(Some(settings));
    insert_fleet(&store);
    let scheduler = Scheduler::new(&store).expect("scheduler");
    let mut shadowed_tasks = 0;
    for job in &jobs {
        let id = submit_job(&store, job);
        let (plan, report, _preemptions) = scheduler
            .plan_only_with_report(&id, PlanMode::Fresh)
            .expect("plan");
        assert!(!plan.is_empty(), "{} must place", job.name);
        for usage in &report {
            if usage.mode == Some("shadow") {
                shadowed_tasks += 1;
                assert_eq!(
                    usage.shadow_agreement,
                    Some(true),
                    "{}/{}: index candidates diverged from the scan",
                    job.name,
                    usage.task_name
                );
            }
        }
    }
    assert!(
        shadowed_tasks >= 3,
        "expected most tasks to exercise a shadow index, got {shadowed_tasks}"
    );
}

#[test]
fn truncation_kicks_in_and_still_places() {
    // 200 nodes in one region: the candidate budget is well below the
    // eligible count, so the index truncates.
    //
    // The budget is `max(8*needed, 64, decorrelation_window)` and the
    // window is 128 at defaults, so it is the window that binds here.
    // That widening is deliberate (plan-apply-conflict-reduction-plan.md):
    // the fallback tail is sampled from the top-W by score, and a budget
    // narrower than W would hand every concurrent planner the same short
    // list — which is the conflict this test's subject, truncation, must
    // not reintroduce. What this test is actually for is unchanged: with
    // 200 eligible against a 128 budget the index still truncates, and
    // all three placements still land.
    let store = store_with(Some(json!({
        "index_min_nodes": 1,
        "indexes": { "topo": ["pool", "region"] },
    })));
    for idx in 0..200 {
        insert_node(
            &store,
            &format!("node-{idx:03}"),
            "default",
            json!({ "region": "region-0" }),
        );
    }
    let job = job_named(
        "truncated",
        vec![TaskConfig {
            count: TaskCount::new(3),
            constraints: json!({ "region": "region-0" }),
            ..TaskConfig::new("web".to_string(), "noop")
        }],
    );
    let id = submit_job(&store, &job);
    let scheduler = Scheduler::new(&store).expect("scheduler");
    let (plan, report, _preemptions) = scheduler
        .plan_only_with_report(&id, PlanMode::Fresh)
        .expect("plan");
    assert_eq!(plan.len(), 3);
    assert_eq!(report.len(), 1);
    let usage = &report[0];
    assert_eq!(usage.index.as_deref(), Some("topo"));
    assert_eq!(usage.mode, Some("on"));
    assert!(usage.truncated, "200 eligible vs budget 128 must truncate");
    assert_eq!(usage.candidates, 128);
    assert_eq!(usage.refills, 0);
}

#[test]
fn shadow_mode_stays_authoritative_and_agrees() {
    let shadow_settings = json!({
        "index_min_nodes": 1,
        "indexes": {
            "fit": false,
            "topo": { "partition_by": ["pool", "region"], "mode": "shadow" },
        },
    });
    let store = store_with(Some(shadow_settings));
    insert_fleet(&store);
    let job = job_named(
        "shadowed",
        vec![TaskConfig {
            count: TaskCount::new(4),
            constraints: json!({ "region": "region-0" }),
            ..TaskConfig::new("web".to_string(), "noop")
        }],
    );
    let id = submit_job(&store, &job);
    let scheduler = Scheduler::new(&store).expect("scheduler");
    let (plan, report, _preemptions) = scheduler
        .plan_only_with_report(&id, PlanMode::Fresh)
        .expect("plan");
    assert_eq!(plan.len(), 4);
    let usage = &report[0];
    assert_eq!(usage.index.as_deref(), Some("topo"));
    assert_eq!(usage.mode, Some("shadow"));
    assert_eq!(usage.shadow_agreement, Some(true));
}

#[test]
fn unusable_index_reports_a_reason_and_scans() {
    let store = store_with(Some(json!({
        "index_min_nodes": 1,
        "indexes": { "fit": false, "topo": ["region"] },
    })));
    insert_fleet(&store);
    // No region constraint: `topo` cannot bind its partition key.
    let job = job_named(
        "unbound",
        vec![TaskConfig {
            count: TaskCount::new(2),
            ..TaskConfig::new("free".to_string(), "noop")
        }],
    );
    let id = submit_job(&store, &job);
    let scheduler = Scheduler::new(&store).expect("scheduler");
    let (plan, report, _preemptions) = scheduler
        .plan_only_with_report(&id, PlanMode::Fresh)
        .expect("plan");
    assert_eq!(plan.len(), 2);
    let usage = &report[0];
    assert_eq!(usage.index, None);
    let reason = usage.reason.as_deref().expect("reason recorded");
    assert!(reason.contains("topo needs [region]"), "{reason}");
}

#[test]
fn default_fit_index_arms_only_above_the_threshold() {
    // Default settings: fit exists but index_min_nodes is 128, and the fleet
    // is 10 nodes — nothing arms, nothing is reported.
    let store = store_with(None);
    for idx in 0..10 {
        insert_node(&store, &format!("node-{idx}"), "default", json!({}));
    }
    let job = job_named(
        "small",
        vec![TaskConfig {
            count: TaskCount::new(2),
            ..TaskConfig::new("web".to_string(), "noop")
        }],
    );
    let id = submit_job(&store, &job);
    let scheduler = Scheduler::new(&store).expect("scheduler");
    let (_, report, _preemptions) = scheduler
        .plan_only_with_report(&id, PlanMode::Fresh)
        .expect("plan");
    let usage = &report[0];
    assert_eq!(usage.index, None);
    assert_eq!(usage.reason, None, "unarmed indexes stay silent");

    // Same fleet with the threshold lowered: the built-in fit serves.
    let store = store_with(Some(json!({ "index_min_nodes": 1 })));
    for idx in 0..10 {
        insert_node(&store, &format!("node-{idx}"), "default", json!({}));
    }
    let id = submit_job(&store, &job);
    let scheduler = Scheduler::new(&store).expect("scheduler");
    let (plan, report, _preemptions) = scheduler
        .plan_only_with_report(&id, PlanMode::Fresh)
        .expect("plan");
    assert_eq!(plan.len(), 2);
    let usage = &report[0];
    assert_eq!(usage.index.as_deref(), Some("fit"));
    assert_eq!(usage.mode, Some("on"));
}

#[test]
fn node_lifecycle_flows_into_the_index() {
    let store = store_with(Some(json!({
        "index_min_nodes": 1,
        "indexes": { "fit": false, "topo": ["region"] },
    })));
    insert_node(&store, "a", "default", json!({ "region": "east" }));
    insert_node(&store, "b", "default", json!({ "region": "east" }));

    let bindings = HashMap::from([("region".to_string(), json!("east"))]);
    let query = |store: &Store| -> Vec<String> {
        match store
            .scheduling_index_query(&bindings, &HashMap::new(), None, true)
            .expect("query")
        {
            IndexQueryOutcome::Candidates(candidates) => candidates.node_ids,
            other => panic!("expected candidates, got {other:?}"),
        }
    };

    assert_eq!(query(&store), vec!["a", "b"]);

    // Draining drops the node from the index; uncordoning restores it.
    store.set_node_availability("a", "drained").expect("drain");
    assert_eq!(query(&store), vec!["b"]);
    store
        .set_node_availability("a", "available")
        .expect("uncordon");
    assert_eq!(query(&store), vec!["a", "b"]);

    // Removal is permanent.
    store.remove_node("b").expect("remove");
    assert_eq!(query(&store), vec!["a"]);
}
