//! Node-group engine tests: the spec and its defaults, the expander,
//! scale-up from demand (step, bounds, join timeout, provider
//! reconciliation), scale-in victims (empty, underutilized, the
//! never-victim rules, the budget, the floor), and the drain state machine
//! (plan §9, Phase 5).

use super::*;
use serde_json::json;

/// 2026-01-01T00:00:00Z.
const T0: i64 = 1_767_225_600_000;

fn at(secs: i64) -> i64 {
    T0 + secs * 1000
}

fn min(m: i64) -> i64 {
    at(m * 60)
}

fn policy(spec: Value) -> EffectiveNodeGroup {
    let policy: NodeGroupPolicy = serde_json::from_value(spec).unwrap();
    policy.resolve().unwrap()
}

fn group() -> EffectiveNodeGroup {
    policy(json!({
        "name": "workers",
        "target": { "nodes": { "plugin": "exec", "shape": { "cpu": 8000, "memory": 16384 } } },
        "min": 1,
        "max": 20,
    }))
}

fn demand(nodes: u32) -> GroupDemand {
    GroupDemand {
        nodes,
        replicas: nodes * 4,
        cpu_requested_mhz: f64::from(nodes) * 6000.0,
        memory_requested_mib: f64::from(nodes) * 12000.0,
        cpu_capacity_mhz: f64::from(nodes) * 8000.0,
        memory_capacity_mib: f64::from(nodes) * 16384.0,
    }
}

fn provider(desired: u32) -> Option<ProviderStatus> {
    Some(ProviderStatus {
        desired,
        running: desired,
        instances: Vec::new(),
    })
}

fn node(id: &str) -> NodeView {
    NodeView {
        id: id.to_string(),
        provider_id: Some(format!("i-{id}")),
        ready: true,
        ..Default::default()
    }
}

fn busy(id: &str) -> NodeView {
    let mut node = node(id);
    node.allocations.push(AllocationView {
        id: 1,
        namespace: "default".into(),
        job: "web".into(),
        task: "web".into(),
        ..Default::default()
    });
    node.reserved_cpu = 0.8;
    node.reserved_memory = 0.7;
    node
}

fn obs(joined: u32, desired: u32) -> NodeGroupObservation {
    NodeGroupObservation {
        joined,
        provider: provider(desired),
        ..Default::default()
    }
}

fn no_fit(_: &NodeView) -> bool {
    false
}

fn fits(_: &NodeView) -> bool {
    true
}

// ── The spec ─────────────────────────────────────────────────────────────

#[test]
fn the_plans_example_policy_parses_and_resolves_test() {
    let effective = policy(json!({
        "name": "workers-general",
        "target": { "nodes": {
            "pool": "default", "workerType": "default",
            "shape": { "cpu": 8000, "memory": 16384, "attributes": { "arch": "amd64" } },
            "plugin": "aws-asg", "config": { "asg": "legion-workers" },
        } },
        "min": 1, "max": 20,
        "up": { "step": { "replicas": 5 }, "every": "2m", "joinTimeout": "10m" },
        "down": { "when": "empty", "for": "10m", "step": { "percent": 10, "replicas": 1 },
                  "every": "5m", "drainDeadline": "10m" },
        "expander": "least-waste",
    }));
    assert_eq!(effective.name, "workers-general");
    assert_eq!(effective.pool.as_deref(), Some("default"));
    assert_eq!(effective.plugin, "aws-asg");
    assert_eq!(effective.config, json!({ "asg": "legion-workers" }));
    // Writing `step` replaces both halves: percent is 0, not the default.
    assert_eq!(effective.up.step.percent, 0.0);
    assert_eq!(effective.up.step.replicas, 5);
    assert_eq!(effective.down.when, ScaleInWhen::Empty);
    assert_eq!(effective.down.drain_deadline, Duration::from_secs(600));
    assert_eq!(
        effective.member_attributes().get(NODEGROUP_ATTR),
        Some(&"workers-general".to_string())
    );
}

/// `configs/scaling/nodes-aws.ncl` evaluates, validates, and writes out
/// exactly the defaults it says it does.
#[test]
fn the_example_config_is_a_valid_policy_of_defaults_test() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/configs/scaling/nodes-aws.ncl");
    let rendered = crate::config_lang::eval_file(path).expect("the example evaluates");
    let policy: NodeGroupPolicy = serde_json::from_str(&rendered).expect("a policy");
    let effective = policy.resolve().expect("valid");
    let mut bare: Value = serde_json::from_str(&rendered).unwrap();
    for field in ["min", "up", "down", "expander", "mode"] {
        bare.as_object_mut().unwrap().remove(field);
    }
    let defaults: NodeGroupPolicy = serde_json::from_value(bare).unwrap();
    assert_eq!(defaults.resolve().unwrap(), effective);
}

#[test]
fn defaults_fill_every_unset_field_test() {
    let effective = policy(json!({
        "name": "g", "target": { "nodes": { "plugin": "exec" } }, "max": 3,
    }));
    assert_eq!(effective.min, 0);
    assert_eq!(effective.worker_type, "default");
    assert_eq!(effective.pool, None);
    assert_eq!(effective.up.step, DEFAULT_UP_STEP);
    assert_eq!(effective.up.every, DEFAULT_UP_EVERY);
    assert_eq!(effective.up.join_timeout, DEFAULT_JOIN_TIMEOUT);
    assert_eq!(effective.down.for_, DEFAULT_DOWN_FOR);
    assert_eq!(effective.down.step, DEFAULT_DOWN_STEP);
    assert_eq!(effective.down.every, DEFAULT_DOWN_EVERY);
    assert_eq!(effective.expander, Expander::LeastWaste);
    assert_eq!(effective.mode, Mode::Enforce);
}

#[test]
fn validation_names_the_field_test() {
    let base = || json!({ "name": "g", "target": { "nodes": { "plugin": "exec" } }, "max": 3 });
    let refuse = |patch: &dyn Fn(&mut Value), needle: &str| {
        let mut spec = base();
        patch(&mut spec);
        let err = match serde_json::from_value::<NodeGroupPolicy>(spec) {
            Ok(policy) => policy.resolve().unwrap_err(),
            Err(err) => err.to_string(),
        };
        assert!(err.contains(needle), "{needle:?} not in {err:?}");
    };
    refuse(&|s| s["name"] = json!("Workers_1"), "lowercase");
    refuse(&|s| s["name"] = json!("-x"), "lowercase");
    refuse(&|s| s["max"] = json!(0), "max must be at least 1");
    refuse(&|s| s["min"] = json!(5), "min = 5 is above max = 3");
    refuse(
        &|s| s["target"]["nodes"]["plugin"] = json!(" "),
        "plugin is empty",
    );
    refuse(
        &|s| s["target"]["nodes"]["pool"] = json!(""),
        "pool is empty",
    );
    refuse(
        &|s| s["up"] = json!({ "step": {} }),
        "up.step names neither",
    );
    refuse(
        &|s| s["up"] = json!({ "every": "0s" }),
        "up.every must be longer",
    );
    refuse(
        &|s| s["down"] = json!({ "for": "0s" }),
        "down.for must be longer",
    );
    refuse(
        &|s| s["down"] = json!({ "drainDeadline": 0 }),
        "down.drainDeadline must be longer",
    );
    refuse(
        &|s| s["down"] = json!({ "when": "idle" }),
        "unknown variant",
    );
    refuse(&|s| s["expander"] = json!("random"), "unknown variant");
    refuse(&|s| s["surprise"] = json!(1), "unknown field");
    refuse(
        &|s| s["target"]["nodes"]["shape"] = json!({ "cpu": 0 }),
        "shape.cpu must be more than 0",
    );
    refuse(
        &|s| s["target"]["nodes"]["shape"] = json!({ "attributes": { "legion.nodegroup": "x" } }),
        "every member gets from the group's name",
    );
    refuse(
        &|s| s["target"]["nodes"]["shape"] = json!({ "attributes": { "namespaces": "x" } }),
        "structure",
    );
    refuse(
        &|s| s["target"]["nodes"]["shape"] = json!({ "resources": { "gpu": -1 } }),
        "shape.resources.gpu",
    );
}

#[test]
fn the_schema_describes_the_policy_test() {
    let schema = NodeGroupPolicy::json_schema_value();
    let text = schema.to_string();
    for field in [
        "joinTimeout",
        "drainDeadline",
        "least-waste",
        "underutilized",
    ] {
        assert!(text.contains(field), "{field} missing from the schema");
    }
}

// ── The expander ─────────────────────────────────────────────────────────

#[test]
fn least_waste_chooses_the_snuggest_group_test() {
    let tight = GroupDemand {
        nodes: 2,
        replicas: 8,
        cpu_requested_mhz: 15000.0,
        memory_requested_mib: 30000.0,
        cpu_capacity_mhz: 16000.0,
        memory_capacity_mib: 32768.0,
    };
    let loose = GroupDemand {
        nodes: 1,
        replicas: 8,
        cpu_requested_mhz: 15000.0,
        memory_requested_mib: 30000.0,
        cpu_capacity_mhz: 64000.0,
        memory_capacity_mib: 262144.0,
    };
    let choice = expand(
        Expander::LeastWaste,
        &[
            ExpanderOption {
                group: "large".into(),
                demand: loose,
                blocked: None,
            },
            ExpanderOption {
                group: "small".into(),
                demand: tight.clone(),
                blocked: None,
            },
            ExpanderOption {
                group: "gpu".into(),
                demand: GroupDemand::default(),
                blocked: None,
            },
            ExpanderOption {
                group: "full".into(),
                demand: tight,
                blocked: Some("at max 4".into()),
            },
        ],
    );
    assert_eq!(choice.chosen.as_deref(), Some("small"));
    assert_eq!(choice.ranking[0].group, "small");
    assert_eq!(choice.ranking[1].group, "large");
    assert!(
        choice.summary.contains("least-waste chose small"),
        "{}",
        choice.summary
    );
    let gpu = choice.ranking.iter().find(|r| r.group == "gpu").unwrap();
    assert_eq!(gpu.note, "no pending work it can host");
    let full = choice.ranking.iter().find(|r| r.group == "full").unwrap();
    assert_eq!(full.note, "cannot grow: at max 4");
}

#[test]
fn the_expander_chooses_nothing_without_a_group_that_can_grow_test() {
    let choice = expand(
        Expander::LeastWaste,
        &[ExpanderOption {
            group: "full".into(),
            demand: demand(2),
            blocked: Some("backing off".into()),
        }],
    );
    assert_eq!(choice.chosen, None);
    assert_eq!(
        choice.summary,
        "no group that could host the pending work can grow"
    );
    assert_eq!(expand(Expander::LeastWaste, &[]).summary, "no pending work");
}

#[test]
fn waste_is_measured_over_the_dimensions_a_template_bounds_test() {
    let mut d = demand(1);
    d.cpu_requested_mhz = 4000.0;
    d.memory_requested_mib = 16384.0;
    assert_eq!(d.waste(), Some(0.25));
    d.memory_capacity_mib = 0.0;
    assert_eq!(d.waste(), Some(0.5));
    d.cpu_capacity_mhz = 0.0;
    assert_eq!(d.waste(), None);
}

// ── Scale-up ─────────────────────────────────────────────────────────────

#[test]
fn demand_grows_the_chosen_group_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut o = obs(2, 2);
    o.demand = Some(demand(3));
    o.chosen = true;
    let decision = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert_eq!((decision.from, decision.to), (2, 5));
    assert_eq!(decision.kind, DecisionKind::Applied);
    assert_eq!(decision.intents, vec![NodeIntent::SetDesired { count: 5 }]);
    assert!(
        decision.summary.contains("pending work needs 3 nodes"),
        "{}",
        decision.summary
    );
    assert!(decision.summary.ends_with("2 → 5"), "{}", decision.summary);
}

#[test]
fn a_group_the_expander_passed_over_holds_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut o = obs(2, 2);
    o.demand = Some(demand(3));
    o.expander_note = "least-waste chose gpu".into();
    let decision = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert_eq!(decision.to, 2);
    assert_eq!(decision.kind, DecisionKind::Held);
    assert!(decision
        .stage(NodeStage::Demand)
        .unwrap()
        .note
        .contains("another group grows this tick (least-waste chose gpu)"));
}

#[test]
fn the_step_limits_scale_up_per_window_test() {
    // Default up.step: 100% or 5 nodes per 2m.
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut o = obs(2, 2);
    o.demand = Some(demand(30));
    o.chosen = true;
    let first = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert_eq!(first.to, 7, "max(100% of 2, 5) = +5");
    assert!(first.stage(NodeStage::Step).unwrap().changed());
    state.record_scale(min(0), 2, 7);

    // Inside the window the budget is spent.
    o.provider = provider(7);
    o.joined = 7;
    let held = decide(&policy, &mut state, &o, at(60), &no_fit);
    assert_eq!(held.to, 7);
    assert!(held
        .stage(NodeStage::Step)
        .unwrap()
        .note
        .contains("+5 used"));

    // A new window starts from the count at its start: 100% of 7.
    let next = decide(&policy, &mut state, &o, min(3), &no_fit);
    assert_eq!(next.to, 14);
}

#[test]
fn bounds_clamp_and_the_floor_applies_without_demand_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let decision = decide(&policy, &mut state, &obs(0, 0), min(0), &no_fit);
    assert_eq!((decision.from, decision.to), (0, 1), "raised to min 1");
    assert_eq!(decision.intents, vec![NodeIntent::SetDesired { count: 1 }]);

    let mut o = obs(18, 18);
    o.demand = Some(demand(5));
    o.chosen = true;
    let mut state = NodeGroupState::default();
    let capped = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert_eq!(capped.to, 20);
    assert!(capped
        .flags
        .iter()
        .any(|flag| matches!(flag, NodeFlag::Limited { .. })));
    // One `scale.limited` per episode.
    let mut o2 = o.clone();
    o2.provider = provider(20);
    o2.joined = 20;
    let again = decide(&policy, &mut state, &o2, min(5), &no_fit);
    assert!(!again
        .flags
        .iter()
        .any(|flag| matches!(flag, NodeFlag::Limited { .. })));
}

#[test]
fn a_group_above_max_is_never_lowered_through_the_provider_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut o = obs(25, 25);
    o.demand = Some(demand(1));
    o.chosen = true;
    let decision = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert_eq!(decision.to, 25);
    assert!(decision
        .stage(NodeStage::Bounds)
        .unwrap()
        .note
        .contains("scale-in brings it down"));
    assert!(decision.intents.is_empty());
}

#[test]
fn join_timeout_reports_and_backs_off_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut o = obs(2, 2);
    o.demand = Some(demand(3));
    o.chosen = true;
    let up = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert_eq!(up.to, 5);
    state.record_scale(min(0), 2, 5);

    // Waiting: one node joined.
    o.provider = provider(5);
    o.joined = 3;
    o.demand = None;
    let waiting = decide(&policy, &mut state, &o, min(5), &no_fit);
    assert!(waiting
        .stage(NodeStage::JoinTimeout)
        .unwrap()
        .note
        .contains("waiting for 2 nodes"));

    // Past joinTimeout (10m): reported once, and the group backs off.
    let timed_out = decide(&policy, &mut state, &o, min(10), &no_fit);
    let flag = timed_out
        .flags
        .iter()
        .find_map(|flag| match flag {
            NodeFlag::JoinTimeout {
                missing,
                backoff_until_ms,
                ..
            } => Some((*missing, *backoff_until_ms)),
            _ => None,
        })
        .expect("a join timeout");
    assert_eq!(flag, (2, min(20)));
    assert!(
        timed_out.summary.contains("did not join"),
        "{}",
        timed_out.summary
    );

    // Backing off: demand does not grow the group.
    o.demand = Some(demand(4));
    let held = decide(&policy, &mut state, &o, min(12), &no_fit);
    assert_eq!(held.to, 5);
    assert!(held
        .stage(NodeStage::Demand)
        .unwrap()
        .note
        .contains("held while backing off"));
    // After the backoff, it grows again.
    let resumed = decide(&policy, &mut state, &o, min(21), &no_fit);
    assert!(resumed.to > 5);
}

#[test]
fn a_scale_up_that_joins_clears_the_join_clock_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    state.record_scale(min(0), 2, 5);
    let decision = decide(&policy, &mut state, &obs(5, 5), min(3), &no_fit);
    assert!(decision
        .stage(NodeStage::JoinTimeout)
        .unwrap()
        .note
        .contains("has joined"));
    assert_eq!(state.pending_up(), None);
}

#[test]
fn the_provider_count_is_adopted_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    state.record_scale(min(0), 2, 5);
    // Someone changed the provider's count by hand.
    let decision = decide(&policy, &mut state, &obs(7, 7), min(1), &no_fit);
    assert_eq!(decision.from, 7);
    assert!(decision
        .stage(NodeStage::Provider)
        .unwrap()
        .note
        .contains("adopting the provider's count"));
    // No provider answer: the last asked-for count.
    let blind = NodeGroupObservation {
        joined: 6,
        ..Default::default()
    };
    let decision = decide(&policy, &mut state, &blind, min(2), &no_fit);
    assert_eq!(decision.from, 7);
}

#[test]
fn dry_run_and_pause_act_on_nothing_test() {
    let mut dry = group();
    dry.mode = Mode::DryRun;
    let mut state = NodeGroupState::default();
    let mut o = obs(2, 2);
    o.demand = Some(demand(3));
    o.chosen = true;
    let decision = decide(&dry, &mut state, &o, min(0), &no_fit);
    assert_eq!(decision.kind, DecisionKind::Would);
    assert_eq!(decision.to, 5);
    assert!(decision.intents.is_empty());
    assert!(decision.summary.contains("dry-run: would scale 2 → 5"));

    let policy = group();
    o.pause = Some(Pause {
        until_ms: None,
        by: "ops".into(),
        reason: String::new(),
    });
    let decision = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert_eq!(decision.to, 2);
    assert_eq!(decision.kind, DecisionKind::Held);
    assert!(decision.intents.is_empty());
}

// ── Scale-in ─────────────────────────────────────────────────────────────

fn scale_in_obs(nodes: Vec<NodeView>) -> NodeGroupObservation {
    let n = nodes.len() as u32;
    NodeGroupObservation {
        joined: n,
        provider: provider(n),
        nodes,
        ..Default::default()
    }
}

#[test]
fn an_empty_node_is_a_victim_after_down_for_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let o = scale_in_obs(vec![busy("a"), node("b"), busy("c")]);
    let first = decide(&policy, &mut state, &o, min(0), &no_fit);
    assert!(first.victims.is_empty(), "not empty for 10m yet");
    assert_eq!(state.empty_since("b"), Some(min(0)));

    let later = decide(&policy, &mut state, &o, min(10), &no_fit);
    assert_eq!(later.victims.len(), 1);
    assert_eq!(later.victims[0].node, "b");
    assert_eq!(later.victims[0].reason, "empty for 10m");
    assert_eq!(later.to, 3, "the count falls as victims terminate");
    assert_eq!(
        later.intents,
        vec![NodeIntent::Cordon {
            node: "b".to_string()
        }]
    );
    assert!(
        later.summary.contains("scale-in: b empty for 10m"),
        "{}",
        later.summary
    );

    // A node that gets work restarts its clock.
    let mut state = NodeGroupState::default();
    let idle = scale_in_obs(vec![busy("a"), node("b"), busy("c")]);
    decide(&policy, &mut state, &idle, min(0), &no_fit);
    let working = scale_in_obs(vec![busy("a"), busy("b"), busy("c")]);
    decide(&policy, &mut state, &working, min(5), &no_fit);
    assert_eq!(state.empty_since("b"), None);
    decide(&policy, &mut state, &idle, min(6), &no_fit);
    assert!(decide(&policy, &mut state, &idle, min(12), &no_fit)
        .victims
        .is_empty());
}

#[test]
fn envelopes_without_sessions_and_per_node_work_do_not_count_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut envelope = node("env");
    envelope.allocations.push(AllocationView {
        id: 7,
        namespace: "default".into(),
        job: "sbx".into(),
        task: "envelope".into(),
        kind: AllocationKind::SessionEnvelope { sessions: 0 },
        ..Default::default()
    });
    envelope.allocations.push(AllocationView {
        id: 8,
        namespace: "default".into(),
        job: "logs".into(),
        task: "shipper".into(),
        kind: AllocationKind::PerNode,
        ..Default::default()
    });
    let mut live = node("live");
    live.allocations.push(AllocationView {
        id: 9,
        namespace: "default".into(),
        job: "sbx".into(),
        task: "envelope".into(),
        kind: AllocationKind::SessionEnvelope { sessions: 2 },
        ..Default::default()
    });
    assert!(envelope.is_empty());
    assert!(!live.is_empty());
    let o = scale_in_obs(vec![envelope, live, busy("x")]);
    decide(&policy, &mut state, &o, min(0), &no_fit);
    let decision = decide(&policy, &mut state, &o, min(10), &no_fit);
    let victims: Vec<&str> = decision.victims.iter().map(|v| v.node.as_str()).collect();
    assert_eq!(victims, vec!["env"]);
}

#[test]
fn never_victims_are_protected_and_named_test() {
    let mut state = NodeGroupState::default();
    let mut tagged = node("tagged");
    tagged.scale_in = Some("never".into());
    let mut system = node("system");
    system.allocations.push(AllocationView {
        id: 1,
        namespace: "legion-system".into(),
        job: "legion-system/autoscaler".into(),
        task: "app".into(),
        kind: AllocationKind::PerNode,
        ..Default::default()
    });
    // A per-node system allocation alone does not protect a node: every
    // node has one. A placed system app does.
    assert_eq!(system.protection(), None);
    system.allocations[0].kind = AllocationKind::Work;
    let mut stateful = node("stateful");
    stateful.allocations.push(AllocationView {
        id: 2,
        namespace: "default".into(),
        job: "db".into(),
        task: "pg".into(),
        kind: AllocationKind::PerNode,
        node_local_storage: true,
        ..Default::default()
    });
    let mut pdb = node("pdb");
    pdb.allocations.push(AllocationView {
        id: 3,
        namespace: "default".into(),
        job: "api".into(),
        task: "api".into(),
        kind: AllocationKind::PerNode,
        breaks_min_available: true,
        ..Default::default()
    });
    // Treat the system node as underutilized-empty for the test: it has
    // work, so it can only qualify as underutilized.
    let mut underused = group();
    underused.down.when = ScaleInWhen::Underutilized;
    let o = scale_in_obs(vec![tagged, system, stateful, pdb, busy("x")]);
    decide(&underused, &mut state, &o, min(0), &fits);
    let decision = decide(&underused, &mut state, &o, min(10), &fits);
    assert!(decision.victims.is_empty(), "{:?}", decision.victims);
    let rules: BTreeMap<&str, &Protection> = decision
        .protected
        .iter()
        .map(|p| (p.node.as_str(), &p.protection))
        .collect();
    assert_eq!(rules["tagged"], &Protection::Tagged);
    assert!(matches!(
        rules["system"],
        Protection::SystemAllocation { .. }
    ));
    assert!(matches!(
        rules["stateful"],
        Protection::NodeLocalStorage { .. }
    ));
    assert!(matches!(rules["pdb"], Protection::MinAvailable { .. }));
    let note = &decision.stage(NodeStage::ScaleIn).unwrap().note;
    assert!(
        note.contains("tagged legion.scale-in = \"never\""),
        "{note}"
    );
    assert!(note.contains("below disruption.minAvailable"), "{note}");
}

#[test]
fn underutilized_nodes_go_only_when_their_work_fits_elsewhere_test() {
    let mut policy = group();
    policy.down.when = ScaleInWhen::Underutilized;
    let mut quiet = busy("quiet");
    quiet.reserved_cpu = 0.2;
    quiet.reserved_memory = 0.3;
    let o = scale_in_obs(vec![quiet, busy("a"), busy("b")]);

    let mut state = NodeGroupState::default();
    decide(&policy, &mut state, &o, min(0), &no_fit);
    let stuck = decide(&policy, &mut state, &o, min(10), &no_fit);
    assert!(stuck.victims.is_empty());
    assert!(stuck
        .stage(NodeStage::ScaleIn)
        .unwrap()
        .note
        .contains("1 node underutilized but not movable"));

    let moved = decide(&policy, &mut state, &o, min(11), &fits);
    assert_eq!(moved.victims.len(), 1);
    assert!(
        moved.victims[0].reason.starts_with("30% reserved"),
        "{}",
        moved.victims[0].reason
    );

    // `when = "empty"` never takes a busy node.
    let mut state = NodeGroupState::default();
    let empty_only = group();
    decide(&empty_only, &mut state, &o, min(0), &fits);
    assert!(decide(&empty_only, &mut state, &o, min(30), &fits)
        .victims
        .is_empty());
}

#[test]
fn the_budget_and_the_floor_limit_victims_test() {
    let mut policy = group();
    policy.min = 2;
    let nodes: Vec<NodeView> = (0..6).map(|i| node(&format!("n{i}"))).collect();
    let o = scale_in_obs(nodes);
    let mut state = NodeGroupState::default();
    decide(&policy, &mut state, &o, min(0), &no_fit);
    // 10% of 6 floors to 0; replicas = 1: one victim per 5m.
    let first = decide(&policy, &mut state, &o, min(10), &no_fit);
    assert_eq!(first.victims.len(), 1);
    assert!(first
        .stage(NodeStage::ScaleIn)
        .unwrap()
        .note
        .contains("held by the budget"));
    // The victim is cordoned now, as the cordon intent made it.
    let mut o = o;
    o.nodes[0].cordoned = true;
    let inside = decide(&policy, &mut state, &o, min(12), &no_fit);
    assert!(inside.victims.is_empty(), "the window's budget is spent");
    // With a bigger budget, the floor holds: 6 - 1 draining - min 2 = 3.
    policy.down.step = Step {
        percent: 0.0,
        replicas: 10,
    };
    let rest = decide(&policy, &mut state, &o, min(16), &no_fit);
    assert_eq!(rest.victims.len(), 3);
    assert!(rest
        .stage(NodeStage::ScaleIn)
        .unwrap()
        .note
        .contains("held at min 2"));
}

#[test]
fn scale_in_holds_while_the_group_has_demand_or_is_growing_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut o = scale_in_obs(vec![node("a"), busy("b")]);
    decide(&policy, &mut state, &o, min(0), &no_fit);
    o.demand = Some(demand(1));
    let held = decide(&policy, &mut state, &o, min(10), &no_fit);
    assert!(held.victims.is_empty());
    assert!(held
        .stage(NodeStage::ScaleIn)
        .unwrap()
        .note
        .contains("held: pending work this group could host needs 1 node"));
    // The clock kept running through the hold.
    o.demand = None;
    let released = decide(&policy, &mut state, &o, min(10) + 1000, &no_fit);
    assert_eq!(released.victims.len(), 1);
}

// ── The drain ────────────────────────────────────────────────────────────

fn drain() -> Drain {
    Drain {
        node: "b".into(),
        provider_id: Some("i-b".into()),
        started_ms: min(0),
        phase: DrainPhase::Draining,
    }
}

fn seen(cordoned: bool, occupied_by: u32) -> DrainObservation {
    DrainObservation {
        exists: true,
        cordoned,
        occupied_by,
        terminated: false,
    }
}

#[test]
fn a_drain_runs_cordon_terminate_forget_in_order_test() {
    let deadline = DEFAULT_DRAIN_DEADLINE;
    let (d, intents, flag) = step_drain(&drain(), &seen(true, 2), deadline, min(1));
    assert_eq!(d.phase, DrainPhase::Draining);
    assert!(intents.is_empty() && flag.is_none());

    let (d, intents, _) = step_drain(&d, &seen(true, 0), deadline, min(2));
    assert_eq!(
        d.phase,
        DrainPhase::Terminating {
            asked_at_ms: min(2)
        }
    );
    assert_eq!(
        intents,
        vec![NodeIntent::Terminate {
            node: "b".into(),
            provider_id: Some("i-b".into())
        }]
    );

    // Not confirmed yet: asked again only after TERMINATE_RETRY.
    let (d, intents, _) = step_drain(&d, &seen(true, 0), deadline, min(3));
    assert!(intents.is_empty());
    let (d, intents, _) = step_drain(&d, &seen(true, 0), deadline, min(4));
    assert_eq!(intents.len(), 1, "terminate retried");

    let mut gone = seen(true, 0);
    gone.terminated = true;
    let (d, intents, _) = step_drain(&d, &gone, deadline, min(5));
    assert_eq!(d.phase, DrainPhase::Forgetting);
    assert_eq!(intents, vec![NodeIntent::Forget { node: "b".into() }]);

    let (d, intents, flag) = step_drain(&d, &DrainObservation::default(), deadline, min(6));
    assert_eq!(d.phase, DrainPhase::Done);
    assert!(intents.is_empty());
    assert_eq!(
        flag,
        Some(NodeFlag::ScaleIn {
            node: "b".into(),
            outcome: "completed".into(),
            detail: "terminated and forgotten".into()
        })
    );
}

#[test]
fn a_drain_past_its_deadline_uncordons_and_aborts_test() {
    let (d, intents, flag) =
        step_drain(&drain(), &seen(true, 3), Duration::from_secs(600), min(10));
    assert_eq!(d.phase, DrainPhase::Aborted);
    assert_eq!(intents, vec![NodeIntent::Uncordon { node: "b".into() }]);
    match flag {
        Some(NodeFlag::ScaleIn {
            outcome, detail, ..
        }) => {
            assert_eq!(outcome, "aborted");
            assert!(detail.contains("3 allocations still on it"), "{detail}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_cordon_that_did_not_land_is_retried_then_an_uncordon_aborts_test() {
    let deadline = DEFAULT_DRAIN_DEADLINE;
    let (d, intents, _) = step_drain(&drain(), &seen(false, 1), deadline, at(15));
    assert_eq!(d.phase, DrainPhase::Draining);
    assert_eq!(intents, vec![NodeIntent::Cordon { node: "b".into() }]);
    // An empty node that is not cordoned is never terminated.
    let (d, intents, _) = step_drain(&d, &seen(false, 0), deadline, at(30));
    assert_eq!(intents, vec![NodeIntent::Cordon { node: "b".into() }]);
    let (d, intents, flag) = step_drain(&d, &seen(false, 1), deadline, min(2));
    assert_eq!(d.phase, DrainPhase::Aborted);
    assert!(intents.is_empty());
    assert!(matches!(flag, Some(NodeFlag::ScaleIn { outcome, .. }) if outcome == "aborted"));
}

#[test]
fn decide_moves_drains_and_a_pause_aborts_them_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let o = scale_in_obs(vec![node("a"), busy("b"), busy("c")]);
    decide(&policy, &mut state, &o, min(0), &no_fit);
    let started = decide(&policy, &mut state, &o, min(10), &no_fit);
    assert_eq!(started.victims[0].node, "a");
    assert_eq!(state.drains().count(), 1);

    // Next tick: cordoned and empty, so terminate.
    let mut next = o.clone();
    next.nodes[0].cordoned = true;
    let decision = decide(&policy, &mut state, &next, min(11), &no_fit);
    assert!(decision.intents.contains(&NodeIntent::Terminate {
        node: "a".into(),
        provider_id: Some("i-a".into())
    }));
    assert!(
        decision.victims.is_empty(),
        "a draining node is not chosen again"
    );

    // A second drain, then a pause: the one not yet terminating aborts.
    let mut state = NodeGroupState::default();
    decide(&policy, &mut state, &o, min(0), &no_fit);
    decide(&policy, &mut state, &o, min(10), &no_fit);
    let mut paused = o.clone();
    paused.nodes[0].cordoned = true;
    paused.nodes[0]
        .allocations
        .push(busy("x").allocations[0].clone());
    paused.pause = Some(Pause {
        until_ms: None,
        by: "ops".into(),
        reason: String::new(),
    });
    let decision = decide(&policy, &mut state, &paused, min(11), &no_fit);
    assert_eq!(
        decision.intents,
        vec![NodeIntent::Uncordon { node: "a".into() }]
    );
    assert!(decision.flags.iter().any(|flag| matches!(
        flag,
        NodeFlag::ScaleIn { outcome, detail, .. } if outcome == "aborted" && detail.contains("paused")
    )));
}

// ── Phase 5b: the provider-id cross-check, adopted drains, digests ──────

/// A member whose provider id the provider does not vouch for is never a
/// victim, and the rule says why.
#[test]
fn an_unverified_provider_id_protects_the_node_test() {
    let policy = group();
    let mut state = NodeGroupState::default();
    let mut stranger = node("b");
    stranger.provider_problem =
        Some("the provider does not list its instance i-b".to_string());
    let o = scale_in_obs(vec![busy("a"), stranger]);
    decide(&policy, &mut state, &o, min(0), &no_fit);
    let later = decide(&policy, &mut state, &o, min(10), &no_fit);
    assert!(later.victims.is_empty(), "{later:?}");
    assert_eq!(later.protected.len(), 1);
    assert_eq!(
        later.protected[0].protection.to_string(),
        "the provider does not list its instance i-b"
    );
}

/// Drains another replica recorded replace this one's; finished ones are
/// dropped.
#[test]
fn drains_from_an_action_row_replace_the_ones_in_flight_test() {
    let mut state = NodeGroupState::default();
    state.adopt_drain(drain());
    let mut done = drain();
    done.node = "c".into();
    done.phase = DrainPhase::Done;
    let mut terminating = drain();
    terminating.node = "d".into();
    terminating.phase = DrainPhase::Terminating {
        asked_at_ms: min(1),
    };
    state.replace_drains(vec![done, terminating.clone()]);
    let drains: Vec<&Drain> = state.drains().collect();
    assert_eq!(drains, vec![&terminating]);
    // The JSON an intent carries round-trips.
    let json = serde_json::to_string(&vec![terminating.clone()]).unwrap();
    let back: Vec<Drain> = serde_json::from_str(&json).unwrap();
    assert_eq!(back, vec![terminating]);
}

/// The effective record's digest is stable and changes with any field; the
/// dry-run diff names the fields.
#[test]
fn a_node_group_digests_and_diffs_by_field_test() {
    let before = group();
    assert_eq!(before.digest(), group().digest());
    assert_eq!(before.digest().len(), 64);
    let after = policy(json!({
        "name": "workers",
        "target": { "nodes": { "plugin": "exec", "shape": { "cpu": 8000, "memory": 16384 } } },
        "min": 1,
        "max": 30,
        "down": { "for": "20m" },
    }));
    assert_ne!(before.digest(), after.digest());
    assert_eq!(
        changed_fields(&before, &after),
        vec![
            "down.for: \"10m\" -> \"20m\"".to_string(),
            "max: 20 -> 30".to_string()
        ]
    );
    assert_eq!(
        before.summary(),
        "nodes 1..20 via exec, up 100% or 5 nodes per 2m, down when empty for 10m, enforce"
    );
}
