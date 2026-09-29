//! Edge boot: ratification, the lost race, and the kill path
//! (docs/edge-boot.md, `internal_docs/plans/edge-boot-plan.md` § Testing).
//!
//! The claim this suite has to hold up is the one the whole design rests on:
//! **the durable commit remains the only authority on ownership.** Two nodes
//! may both boot a sandbox; exactly one may end up owning it, and the loser
//! must be told so with a reason it can act on.
//!
//! Ratification is deliberately *not* a new consistency mechanism — it is the
//! existing plan apply with the node already chosen — so these tests drive it
//! through the real store, not a mock, and assert against the same instance
//! binding every other placement path goes through.

use anyhow::Context;
use legion::manager::edge_boot::{ratify, ClaimRatification, Verdict};
use legion::storage::store::Store;
use legion::structs::edge_boot::{Gate, RefusalReason};
use legion::structs::{
    InstanceMode, JobConfig, ManagerConfigFull, NodeMsg, StatefulConfig, TaskConfig,
};
use serde_json::json;
use tempfile::Builder;

type R<T> = anyhow::Result<T>;

const JOB: &str = "agent-sandboxes";
const TASK: &str = "workspace";

fn setup_store() -> R<Store> {
    let dir = Builder::new()
        .prefix("legion-edge-boot-")
        .tempdir()
        .context("temp dir")?
        .keep();
    let config = ManagerConfigFull {
        store_path: dir.to_str().context("store path")?.to_string(),
        ..Default::default()
    };
    Store::start_storage(&config)
}

fn node(id: &str) -> NodeMsg {
    NodeMsg {
        id: id.into(),
        status: "active".into(),
        pool: "edge-eu".into(),
        availability: "available".into(),
        last_heartbeat: "now".into(),
        heartbeat_source: String::new(),
        config_attrs: json!({
            "workerAddress": format!("http://{id}:4848"),
            "protocolVersion": 2,
            "resources": { "memory": 8192, "cores": 8 },
        })
        .to_string(),
        fingerprint_attrs: json!({
            "memory_mb": 8192,
            "cpu": { "cores": 8, "mhz": 2400 },
        })
        .to_string(),
        dynamic_attrs: "{}".into(),
        health_checks: "{}".into(),
    }
}

/// A keyed sandbox task that opts into edge boot at gate `boot`.
fn sandbox_task() -> TaskConfig {
    TaskConfig {
        stateful: Some(StatefulConfig {
            mode: InstanceMode::Keyed,
            id_source: None,
            initial_instances: None,
            on_lost: None,
            storage: None,
        }),
        speculative_start: Some(legion::structs::edge_boot::SpeculativeStartConfig {
            mode: Some("boot".into()),
            ratify_timeout: Some("5s".into()),
            on_refused: None,
        }),
        resources: json!({ "memory": 512, "cores": 1 }),
        // Edge nodes live in their own pool; without this the task's implicit
        // `pool=default` permit would keep it off them.
        permit: json!({ "pool": "edge-eu" }),
        ..TaskConfig::new(TASK, "noop")
    }
}

fn submit(store: &Store) -> R<String> {
    let config = JobConfig {
        priority: Default::default(),
        rollout: None,
        rollout_by_group: Default::default(),
        name: JOB.into(),
        tasks: vec![sandbox_task()],
        version: None,
    };
    store.handle_job_submit(
        &config,
        &[legion::structs::Submission {
            path: "job.ncl".into(),
            content: "{}".into(),
            is_primary: true,
        }],
    )?;
    Ok(store.get_job_config(JOB)?.id())
}

fn claim<'a>(
    node_id: &'a str,
    claim_id: &'a str,
    job_config_id: &'a str,
    instance_id: &'a str,
) -> ClaimRatification<'a> {
    ClaimRatification {
        node_id,
        claim_id,
        job_config_id,
        task_name: TASK,
        instance_id,
        gate: Gate::Boot,
    }
}

#[test]
fn speculative_start_survives_persistence() -> R<()> {
    // A dropped field would silently turn the gate into `off` — the feature
    // quietly not happening, which is exactly the failure mode the strict
    // parsing elsewhere exists to prevent.
    let store = setup_store()?;
    let job_config_id = submit(&store)?;
    let tasks = store.get_task_configs_for_job_config(&job_config_id)?;
    let (_, task) = tasks.first().context("task config")?;
    let block = task
        .speculative_start
        .as_ref()
        .context("speculativeStart was dropped in persistence")?;
    let resolved = block.resolve()?;
    assert_eq!(resolved.mode, Gate::Boot);
    assert_eq!(resolved.ratify_timeout, std::time::Duration::from_secs(5));
    Ok(())
}

#[test]
fn ratification_binds_the_instance_it_raced_for() -> R<()> {
    let store = setup_store()?;
    store.insert_node(node("node-a"))?;
    let job_config_id = submit(&store)?;
    // Two unbound sandboxes exist. A ratification must bind the one it names,
    // not "any unbound instance" — otherwise a caller that asked for `user-42`
    // could be handed `user-7` and would route traffic to the wrong workload.
    store.create_keyed_instance(JOB, "default", TASK, Some("user-42"))?;
    store.create_keyed_instance(JOB, "default", TASK, Some("user-7"))?;

    let verdict = ratify(&store, &claim("node-a", "clm_a", &job_config_id, "user-42"))?;
    let allocation_id = match verdict {
        Verdict::Confirmed { allocation_id } => allocation_id,
        Verdict::Refused { reason, detail } => {
            panic!("expected confirmation, got {reason}: {detail}")
        }
    };

    let allocation = store
        .get_allocation_by_id(allocation_id)?
        .context("allocation")?;
    assert_eq!(allocation.instance_id.as_deref(), Some("user-42"));
    assert_eq!(allocation.node_id, "node-a");

    let bound = store.get_instance("user-42")?.context("instance")?;
    assert_eq!(bound.current_alloc_id, Some(allocation_id));
    // The sandbox nobody claimed is untouched.
    let untouched = store.get_instance("user-7")?.context("instance")?;
    assert_eq!(untouched.current_alloc_id, None);
    Ok(())
}

#[test]
fn two_nodes_racing_for_one_sandbox_leave_exactly_one_owner() -> R<()> {
    let store = setup_store()?;
    store.insert_node(node("node-a"))?;
    store.insert_node(node("node-b"))?;
    let job_config_id = submit(&store)?;
    store.create_keyed_instance(JOB, "default", TASK, Some("user-42"))?;

    // Both edges booted; both now ask to be confirmed.
    let first = ratify(&store, &claim("node-a", "clm_a", &job_config_id, "user-42"))?;
    let second = ratify(&store, &claim("node-b", "clm_b", &job_config_id, "user-42"))?;

    let winner = match first {
        Verdict::Confirmed { allocation_id } => allocation_id,
        other => panic!("first ratification should win, got {other:?}"),
    };
    match second {
        Verdict::Refused { reason, .. } => {
            // The reason an on-call responder needs: this is a routing race,
            // not a capacity problem.
            assert_eq!(reason, RefusalReason::InstanceClaimed);
            assert!(!reason.retryable(), "a lost race is final, not retried");
        }
        Verdict::Confirmed { allocation_id } => {
            panic!("second ratification must not bind (got allocation {allocation_id})")
        }
    }

    // The invariant, stated as the store sees it: one live allocation for the
    // instance, on the winner's node.
    let instance = store.get_instance("user-42")?.context("instance")?;
    assert_eq!(instance.current_alloc_id, Some(winner));
    let allocations = store.get_allocs()?;
    let for_instance = allocations
        .iter()
        .filter(|allocation| allocation.instance_id.as_deref() == Some("user-42"))
        .count();
    assert_eq!(
        for_instance, 1,
        "exactly one live allocation for the sandbox"
    );
    Ok(())
}

#[test]
fn ratifying_an_unknown_task_is_refused_not_an_error() -> R<()> {
    // A node holding a stale spec must get a verdict it can kill on, not an
    // exception that leaves the claim to time out.
    let store = setup_store()?;
    store.insert_node(node("node-a"))?;
    let job_config_id = submit(&store)?;

    let mut request = claim("node-a", "clm_a", &job_config_id, "user-42");
    request.task_name = "nope";
    match ratify(&store, &request)? {
        Verdict::Refused { reason, .. } => assert_eq!(reason, RefusalReason::UnknownTarget),
        other => panic!("expected a refusal, got {other:?}"),
    }
    Ok(())
}

#[test]
fn ratifying_an_instance_that_does_not_exist_is_refused() -> R<()> {
    let store = setup_store()?;
    store.insert_node(node("node-a"))?;
    let job_config_id = submit(&store)?;
    // No CreateInstance ever ran for `user-99`.
    match ratify(&store, &claim("node-a", "clm_a", &job_config_id, "user-99"))? {
        Verdict::Refused { reason, .. } => assert_eq!(reason, RefusalReason::InstanceClaimed),
        other => panic!("expected a refusal, got {other:?}"),
    }
    Ok(())
}

#[test]
fn ratifying_onto_a_cordoned_node_is_refused_as_ineligible() -> R<()> {
    // Availability is re-validated on the FRESH node row inside the apply, so
    // a node cordoned while a sandbox was booting cannot keep it.
    let store = setup_store()?;
    store.insert_node(node("node-a"))?;
    let job_config_id = submit(&store)?;
    store.create_keyed_instance(JOB, "default", TASK, Some("user-42"))?;
    store.set_node_availability("node-a", "drained")?;

    match ratify(&store, &claim("node-a", "clm_a", &job_config_id, "user-42"))? {
        Verdict::Refused { reason, .. } => assert_eq!(reason, RefusalReason::Ineligible),
        other => panic!("expected a refusal, got {other:?}"),
    }
    // Nothing bound.
    let instance = store.get_instance("user-42")?.context("instance")?;
    assert_eq!(instance.current_alloc_id, None);
    Ok(())
}

#[test]
fn a_ratified_sandbox_can_be_rescheduled_like_any_other_allocation() -> R<()> {
    // A ratified claim is an ordinary allocation, not a second-class one: the
    // instance rebinds on a later placement exactly as it would have if the
    // sandbox had been started the slow way.
    let store = setup_store()?;
    store.insert_node(node("node-a"))?;
    store.insert_node(node("node-b"))?;
    let job_config_id = submit(&store)?;
    store.create_keyed_instance(JOB, "default", TASK, Some("user-42"))?;

    let first = match ratify(&store, &claim("node-a", "clm_a", &job_config_id, "user-42"))? {
        Verdict::Confirmed { allocation_id } => allocation_id,
        other => panic!("expected a confirmation, got {other:?}"),
    };

    store.remove_allocs_for_node("node-a")?;

    let second = match ratify(&store, &claim("node-b", "clm_b", &job_config_id, "user-42"))? {
        Verdict::Confirmed { allocation_id } => allocation_id,
        other => panic!("the freed sandbox should rebind, got {other:?}"),
    };
    // Ids may be reused by the store; what matters is that the sandbox came
    // back under the same identity, on the new node.
    let _ = first;
    let allocation = store
        .get_allocation_by_id(second)?
        .context("rebound allocation")?;
    assert_eq!(allocation.node_id, "node-b");
    let instance = store.get_instance("user-42")?.context("instance")?;
    assert_eq!(instance.current_alloc_id, Some(second));
    Ok(())
}
