//! Pipeline tests: every stage's pass and change behavior, the holds, the
//! rolling step windows, bounds after step, and the pin/pause matrix
//! (plan §9, Phase 2 "Pipeline unit tests").

use super::*;
use crate::autoscaler::defaults::{resolve, DefaultsLayers};
use crate::autoscaler::spec::AutoscaleSpec;
use serde_json::{json, Value};

/// 2026-01-01T00:00:00Z.
const T0: i64 = 1_767_225_600_000;

fn at(secs: i64) -> i64 {
    T0 + secs * 1000
}

fn policy(task: Value, count: u32) -> EffectivePolicy {
    let spec: AutoscaleSpec = serde_json::from_value(task).unwrap();
    resolve(&spec, count, &DefaultsLayers::default(), "web/default/web")
        .unwrap()
        .unwrap()
        .policy
}

/// A state whose cold start ended long ago.
fn warm(_: &EffectivePolicy) -> PolicyState {
    PolicyState::new(at(-3600))
}

fn changed(decision: &Decision) -> Vec<Stage> {
    decision
        .stages
        .iter()
        .filter(|note| note.changed())
        .map(|note| note.stage)
        .collect()
}

struct Case {
    name: &'static str,
    policy: Value,
    count: u32,
    state: fn(&EffectivePolicy) -> PolicyState,
    obs: Observation,
    now: i64,
    to: u32,
    kind: DecisionKind,
    changed: &'static [Stage],
    note: (Stage, &'static str),
}

#[test]
fn every_stage_pass_and_change_table_test() {
    let cases = vec![
        Case {
            name: "merge: one check moves the count",
            policy: json!(true),
            count: 2,
            state: warm,
            obs: Observation::new(4).reading("cpu", 0.91),
            now: at(0),
            to: 6,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge],
            note: (Stage::Merge, "cpu wants 6"),
        },
        Case {
            name: "merge: max of checks",
            policy: json!({ "cpu": 0.7, "memory": 0.8 }),
            count: 2,
            state: warm,
            obs: Observation::new(4)
                .reading("cpu", 0.91)
                .reading("memory", 0.95),
            now: at(0),
            to: 6,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge],
            note: (Stage::Merge, "max of 2 checks: cpu wants 6, memory wants 5"),
        },
        Case {
            name: "headroom adds after the merge",
            policy: json!({ "headroom": 1 }),
            count: 2,
            state: warm,
            obs: Observation::new(4).reading("cpu", 0.91),
            now: at(0),
            to: 7,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge, Stage::Headroom],
            note: (Stage::Headroom, "+1 headroom"),
        },
        Case {
            name: "stabilize: scale-in takes the highest recommendation in the window",
            policy: json!(true),
            count: 2,
            state: |policy| {
                let mut state = PolicyState::new(at(-3600));
                evaluate(
                    policy,
                    &mut state,
                    &Observation::new(6).reading("cpu", 0.7),
                    at(-60),
                );
                state
            },
            obs: Observation::new(6).reading("cpu", 0.2),
            now: at(0),
            to: 6,
            kind: DecisionKind::Held,
            changed: &[Stage::Merge, Stage::Stabilize],
            note: (Stage::Stabilize, "down: 5m max is 6"),
        },
        Case {
            name: "stabilize: scale-out takes the lowest recommendation in the window",
            policy: json!({ "up": { "stabilize": "30s" } }),
            count: 2,
            state: |policy| {
                let mut state = PolicyState::new(at(-3600));
                evaluate(
                    policy,
                    &mut state,
                    &Observation::new(4).reading("cpu", 0.7),
                    at(-10),
                );
                state
            },
            obs: Observation::new(4).reading("cpu", 0.91),
            now: at(0),
            to: 4,
            kind: DecisionKind::Held,
            changed: &[Stage::Merge, Stage::Stabilize],
            note: (Stage::Stabilize, "up: 30s min is 4"),
        },
        Case {
            name: "hold: cold start blocks scale-in",
            policy: json!(true),
            count: 2,
            state: |_| PolicyState::new(at(-60)),
            obs: Observation::new(6).reading("cpu", 0.1),
            now: at(0),
            to: 6,
            kind: DecisionKind::Held,
            changed: &[Stage::Merge, Stage::Hold],
            note: (Stage::Hold, "held: collecting 5m of history after restart"),
        },
        Case {
            name: "hold: a missing check blocks scale-in (onMissing = hold)",
            policy: json!({ "cpu": 0.7, "memory": 0.8, "down": { "stabilize": "0s" } }),
            count: 2,
            state: warm,
            obs: Observation::new(4)
                .reading("cpu", 0.2)
                .missing("memory", "timeout"),
            now: at(0),
            to: 4,
            kind: DecisionKind::Held,
            changed: &[Stage::Merge, Stage::Hold],
            note: (Stage::Hold, "held: memory has no data (timeout)"),
        },
        Case {
            name: "hold: a missing check does not block scale-out",
            policy: json!({ "cpu": 0.7, "memory": 0.8 }),
            count: 2,
            state: warm,
            obs: Observation::new(4).reading("cpu", 0.91),
            now: at(0),
            to: 6,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge],
            note: (Stage::Hold, "memory has no data (no data)"),
        },
        Case {
            name: "onMissing = ignore drops the check and scales in",
            policy: json!({ "cpu": 0.7, "memory": 0.8, "down": { "stabilize": "0s" },
                            "onMissing": "ignore" }),
            count: 2,
            state: warm,
            obs: Observation::new(4)
                .reading("cpu", 0.2)
                .missing("memory", "timeout"),
            now: at(0),
            to: 3,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge, Stage::Step],
            note: (Stage::Merge, "memory ignored: no data"),
        },
        Case {
            name: "no data at all holds",
            policy: json!(true),
            count: 2,
            state: warm,
            obs: Observation::new(4).missing("cpu", "source down"),
            now: at(0),
            to: 4,
            kind: DecisionKind::Held,
            changed: &[],
            note: (Stage::Merge, "no check has data; holding at 4"),
        },
        Case {
            name: "step: up is capped at max(+100%, +4) per minute",
            policy: json!(true),
            count: 2,
            state: warm,
            obs: Observation::new(2).reading("cpu", 3.5),
            now: at(0),
            to: 6,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge, Stage::Step],
            note: (Stage::Step, "rate-limited 10 → 6 (up ≤ +4 per 1m; +0 used)"),
        },
        Case {
            name: "bounds: clamped to max",
            policy: json!(true),
            count: 2,
            state: warm,
            obs: Observation::new(7).reading("cpu", 1.4),
            now: at(0),
            to: 8,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge, Stage::Bounds],
            note: (Stage::Bounds, "clamped to max 8"),
        },
        Case {
            name: "bounds: a schedule's floor applies at once, past the step limit",
            policy: json!({ "max": 20,
                            "schedule": [{ "cron": "0 8 * * *", "for": "10h", "min": 10 }] }),
            count: 2,
            state: warm,
            obs: Observation::new(2).reading("cpu", 0.7),
            now: at(9 * 3600),
            to: 10,
            kind: DecisionKind::Applied,
            changed: &[Stage::Bounds],
            note: (
                Stage::Bounds,
                "raised to min 10 (schedule '0 8 * * *' until 18:00 UTC)",
            ),
        },
        Case {
            name: "bounds: a lowered max applies at once",
            policy: json!({ "max": 6 }),
            count: 2,
            state: warm,
            obs: Observation::new(10).reading("cpu", 0.7),
            now: at(0),
            to: 6,
            kind: DecisionKind::Applied,
            changed: &[Stage::Bounds],
            note: (Stage::Bounds, "clamped to max 6"),
        },
        Case {
            name: "quota caps an increase",
            policy: json!(true),
            count: 2,
            state: warm,
            obs: Observation {
                guards: Guards {
                    quota: Some(QuotaGuard {
                        headroom: 1,
                        detail: "namespace quota payments-prod (normal: 12/12 cpu)".to_string(),
                    }),
                    ..Default::default()
                },
                ..Observation::new(4).reading("cpu", 0.91)
            },
            now: at(0),
            to: 5,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge, Stage::Quota],
            note: (
                Stage::Quota,
                "limited by namespace quota payments-prod (normal: 12/12 cpu)",
            ),
        },
        Case {
            name: "capacity holds while the last scale-out is unplaced",
            policy: json!(true),
            count: 2,
            state: |_| {
                let mut state = PolicyState::new(at(-3600));
                state.record_applied(at(-120), 2, 6);
                state
            },
            obs: Observation {
                guards: Guards {
                    capacity: Some(CapacityGuard {
                        unplaced: 3,
                        node_group_fits: false,
                        last_scale_out: None,
                    }),
                    ..Default::default()
                },
                ..Observation::new(6).reading("cpu", 0.91)
            },
            now: at(0),
            to: 6,
            kind: DecisionKind::Held,
            changed: &[Stage::Merge, Stage::Capacity],
            note: (
                Stage::Capacity,
                "held: 3 replicas unplaced, no node group fits",
            ),
        },
        Case {
            name: "capacity passes when a node group can host the work",
            policy: json!(true),
            count: 2,
            state: |_| {
                let mut state = PolicyState::new(at(-3600));
                state.record_applied(at(-120), 2, 6);
                state
            },
            obs: Observation {
                guards: Guards {
                    capacity: Some(CapacityGuard {
                        unplaced: 3,
                        node_group_fits: true,
                        last_scale_out: None,
                    }),
                    ..Default::default()
                },
                ..Observation::new(6).reading("cpu", 0.91)
            },
            now: at(0),
            to: 8,
            kind: DecisionKind::Applied,
            changed: &[Stage::Merge],
            note: (Stage::Capacity, "a node group can host them"),
        },
        Case {
            name: "an active rollout defers scale-in",
            policy: json!({ "down": { "stabilize": "0s" } }),
            count: 2,
            state: warm,
            obs: Observation {
                guards: Guards {
                    rollout: Some("web-7".to_string()),
                    ..Default::default()
                },
                ..Observation::new(6).reading("cpu", 0.1)
            },
            now: at(0),
            to: 6,
            kind: DecisionKind::Held,
            changed: &[Stage::Merge, Stage::Step, Stage::Rollout],
            note: (Stage::Rollout, "deferred until rollout web-7 completes"),
        },
        Case {
            name: "dry-run records a would",
            policy: json!({ "mode": "dry-run" }),
            count: 2,
            state: warm,
            obs: Observation::new(4).reading("cpu", 0.91),
            now: at(0),
            to: 6,
            kind: DecisionKind::Would,
            changed: &[Stage::Merge],
            note: (Stage::Mode, "dry-run: would scale 4 → 6"),
        },
        Case {
            name: "tolerance holds",
            policy: json!(true),
            count: 2,
            state: warm,
            obs: Observation::new(4).reading("cpu", 0.66),
            now: at(0),
            to: 4,
            kind: DecisionKind::Held,
            changed: &[],
            note: (Stage::Merge, "within tolerance"),
        },
    ];
    for case in cases {
        let policy = policy(case.policy.clone(), case.count);
        let mut state = (case.state)(&policy);
        let decision = evaluate(&policy, &mut state, &case.obs, case.now);
        let name = case.name;
        assert_eq!(decision.to, case.to, "{name}: {decision:#?}");
        assert_eq!(decision.kind, case.kind, "{name}: {decision:#?}");
        assert_eq!(changed(&decision), case.changed, "{name}: {decision:#?}");
        let (stage, text) = case.note;
        let note = &decision.stage(stage).unwrap().note;
        assert!(
            note.contains(text),
            "{name}: {stage} note {note:?} lacks {text:?}"
        );
        let order: Vec<Stage> = decision.stages.iter().map(|s| s.stage).collect();
        assert_eq!(order, Stage::ORDER, "{name}");
        assert_eq!(decision.policy_digest, policy.digest(), "{name}");
        assert_eq!(decision.from, case.obs.current, "{name}");
    }
}

#[test]
fn summary_is_the_driver_plus_changed_stages_test() {
    let policy = policy(json!(true), 2);
    let mut state = warm(&policy);
    let decision = evaluate(
        &policy,
        &mut state,
        &Observation::new(2).reading("cpu", 3.5),
        at(0),
    );
    assert_eq!(
        decision.summary,
        "cpu 3.50 vs 0.70 (2 ready); rate-limited from 10"
    );
    let decision = evaluate(
        &policy,
        &mut warm(&policy),
        &Observation::new(4).reading("cpu", 0.66),
        at(0),
    );
    assert_eq!(
        decision.summary,
        "cpu 0.66 vs 0.70 (4 ready), within tolerance"
    );
    let decision = evaluate(
        &policy,
        &mut warm(&policy),
        &Observation::new(4).missing("cpu", "timeout"),
        at(0),
    );
    assert_eq!(decision.summary, "no check has data; holding at 4");
    // Every decision serializes, for scale_history.
    let json = serde_json::to_value(&decision).unwrap();
    assert_eq!(json["kind"], "held");
    assert_eq!(json["stages"][0]["stage"], "merge");
    for stage in Stage::ORDER {
        assert_eq!(serde_json::to_value(stage).unwrap(), stage.to_string());
    }
}

#[test]
fn scale_in_false_checks_only_vote_up_test() {
    let policy = policy(
        json!({
            "down": { "stabilize": "0s" },
            "checks": [
                { "name": "latency", "source": "legion", "query": "service.latency_ms.mean",
                  "strategy": { "name": "threshold", "steps": [{ "above": 250, "add": 2 }] },
                  "scaleIn": false },
                { "name": "queue", "source": "prom", "query": "q",
                  "strategy": { "name": "per-replica", "value": 100 } }
            ]
        }),
        2,
    );
    // Latency is fine (wants the current 4): not counted; queue wants 2.
    let obs = Observation::new(4)
        .reading("latency", 100.0)
        .reading("queue", 200.0);
    let decision = evaluate(&policy, &mut warm(&policy), &obs, at(0));
    let merge = decision.stage(Stage::Merge).unwrap();
    assert_eq!(merge.after, 2, "{decision:#?}");
    assert!(
        merge
            .note
            .contains("latency (scaleIn = false) wants 4: not counted"),
        "{merge:?}"
    );
    assert!(!decision.checks[0].counted);
    // Latency is high: it votes up and wins.
    let obs = Observation::new(4)
        .reading("latency", 600.0)
        .reading("queue", 200.0);
    let decision = evaluate(&policy, &mut warm(&policy), &obs, at(0));
    assert_eq!(decision.stage(Stage::Merge).unwrap().after, 6);
    assert_eq!(decision.to, 6);
    assert!(
        decision.summary.starts_with("latency 600 above 250: +2"),
        "{}",
        decision.summary
    );
}

#[test]
fn warmup_rule_damps_scale_out_through_the_pipeline_test() {
    let policy = policy(json!(true), 2);
    // 6 desired, 4 ready at 0.91, 2 warming: the warming replicas count as
    // idle, which reverses the move, so the pipeline holds.
    let obs = Observation::new(6).reading("cpu", 0.91).readiness(4, 2);
    let decision = evaluate(&policy, &mut warm(&policy), &obs, at(0));
    assert_eq!(decision.kind, DecisionKind::Held, "{decision:#?}");
    assert!(decision.checks[0].within_tolerance);
    assert!(decision.checks[0].detail.contains("4 ready, 2 not ready"));
    assert_eq!(decision.warming, 2);
    // Nothing ready to measure: missing data, which holds scale-in.
    let obs = Observation::new(3).reading("cpu", 0.0).readiness(0, 3);
    let decision = evaluate(&policy, &mut warm(&policy), &obs, at(0));
    assert_eq!(decision.kind, DecisionKind::Held);
    assert_eq!(
        decision.checks[0].missing.as_deref(),
        Some("no ready replicas to measure")
    );
}

#[test]
fn rolling_step_window_up_test() {
    let policy = policy(json!({ "max": 20 }), 2);
    let mut state = warm(&policy);
    // 2 → 6: +4 (the larger of 100% of 2 and 4).
    let first = evaluate(
        &policy,
        &mut state,
        &Observation::new(2).reading("cpu", 3.5),
        at(0),
    );
    assert_eq!(first.to, 6);
    state.record_applied(at(0), 2, 6);
    // 20s later the window still holds +4 of a +4 budget.
    let held = evaluate(
        &policy,
        &mut state,
        &Observation::new(6).reading("cpu", 1.6),
        at(20),
    );
    assert_eq!(held.to, 6, "{held:#?}");
    assert!(
        held.stage(Stage::Step)
            .unwrap()
            .note
            .contains("rate-limited 14 → 6 (up ≤ +4 per 1m; +4 used)"),
        "{held:#?}"
    );
    // A minute after the change it has left the window: +100% of 6.
    let next = evaluate(
        &policy,
        &mut state,
        &Observation::new(6).reading("cpu", 1.6),
        at(61),
    );
    assert_eq!(next.to, 12, "{next:#?}");
}

#[test]
fn rolling_step_window_with_both_directions_test() {
    let policy = policy(json!({ "max": 20, "down": { "stabilize": "0s" } }), 2);
    let mut state = warm(&policy);
    state.record_applied(at(0), 4, 8);
    state.record_applied(at(20), 8, 7);
    // Up: the scale-in does not refund the scale-out's budget: +4 of +4 used
    // (base 4, the count at the start of the window).
    let up = evaluate(
        &policy,
        &mut state,
        &Observation::new(7).reading("cpu", 1.2),
        at(40),
    );
    assert_eq!(up.to, 7, "{up:#?}");
    assert!(
        up.stage(Stage::Step).unwrap().note.contains("+4 used"),
        "{up:#?}"
    );
    // Down: -1 of a -1 budget (the larger of 10% of 4, rounded down, and 1).
    let down = evaluate(
        &policy,
        &mut state,
        &Observation::new(7).reading("cpu", 0.3),
        at(45),
    );
    assert_eq!(down.to, 7, "{down:#?}");
    assert!(
        down.stage(Stage::Step)
            .unwrap()
            .note
            .contains("down ≤ -1 per 1m; -1 used"),
        "{down:#?}"
    );
    // Once both leave the window each direction has its budget again.
    let later = evaluate(
        &policy,
        &mut state,
        &Observation::new(7).reading("cpu", 0.3),
        at(81),
    );
    assert_eq!(later.to, 6, "{later:#?}");
}

#[test]
fn partial_step_record_zeroes_the_other_half_test() {
    // (policy, current, cpu, expected): the default would allow max(100%, 4).
    let cases = [
        (json!({ "max": 40 }), 10, 2.1, 20),
        (
            json!({ "max": 40, "up": { "step": { "replicas": 2 } } }),
            10,
            2.1,
            12,
        ),
        (
            json!({ "max": 40, "up": { "step": { "percent": 50 } } }),
            2,
            3.5,
            3,
        ),
        (
            json!({ "max": 40, "up": { "step": { "replicas": 0 } } }),
            4,
            2.1,
            4,
        ),
    ];
    for (task, current, cpu, expected) in cases {
        let policy = policy(task.clone(), 2);
        let decision = evaluate(
            &policy,
            &mut warm(&policy),
            &Observation::new(current).reading("cpu", cpu),
            at(0),
        );
        assert_eq!(decision.to, expected, "{task}: {decision:#?}");
    }
}

#[test]
fn flap_guard_holds_scale_in_and_flags_once_test() {
    let policy = policy(json!({ "down": { "stabilize": "1m" } }), 2);
    let mut state = warm(&policy);
    state.record_applied(at(-500), 2, 4);
    state.record_applied(at(-400), 4, 3);
    state.record_applied(at(-300), 3, 5);
    state.record_applied(at(-200), 5, 4);
    let quiet = Observation::new(4).reading("cpu", 0.1);
    let first = evaluate(&policy, &mut state, &quiet, at(0));
    assert_eq!(
        first.flags,
        vec![Flag::Flapping {
            reversals: 3,
            until_ms: at(120)
        }]
    );
    assert_eq!(first.kind, DecisionKind::Held, "{first:#?}");
    assert!(first
        .stage(Stage::Hold)
        .unwrap()
        .note
        .contains("held: flapping (3 reversals in 10m)"));
    // Still held, flagged only once.
    let second = evaluate(&policy, &mut state, &quiet, at(10));
    assert!(second.flags.is_empty());
    assert_eq!(second.kind, DecisionKind::Held);
    // Scale-out is never held by the flap guard.
    let busy = evaluate(
        &policy,
        &mut state,
        &Observation::new(4).reading("cpu", 0.91),
        at(20),
    );
    assert_eq!(busy.to, 6);
    // After 2 × down.stabilize the hold lifts, with no second flag for the
    // same reversals.
    let after = evaluate(&policy, &mut state, &quiet, at(130));
    assert!(after.flags.is_empty(), "{after:#?}");
    assert_eq!(after.to, 3, "{after:#?}");
}

#[test]
fn limited_flag_fires_on_the_first_hit_only_test() {
    let policy = policy(json!(true), 2);
    let mut state = warm(&policy);
    let busy = Observation::new(8).reading("cpu", 1.4);
    let first = evaluate(&policy, &mut state, &busy, at(0));
    assert_eq!(
        first.flags,
        vec![Flag::Limited {
            reason: "at max 8 (raise autoscale.max)".to_string()
        }]
    );
    let second = evaluate(&policy, &mut state, &busy, at(10));
    assert!(second.flags.is_empty());
    // A tick held for another reason does not end the episode.
    let steady = evaluate(
        &policy,
        &mut state,
        &Observation::new(8).reading("cpu", 0.7),
        at(20),
    );
    assert_eq!(steady.kind, DecisionKind::Held);
    let third = evaluate(&policy, &mut state, &busy, at(30));
    assert!(third.flags.is_empty(), "{third:#?}");
    // A move no bound held back ends it; the next hit flags again.
    let moved = evaluate(
        &policy,
        &mut state,
        &Observation::new(5).reading("cpu", 0.91),
        at(40),
    );
    assert_eq!((moved.kind, moved.to), (DecisionKind::Applied, 7));
    let fourth = evaluate(&policy, &mut state, &busy, at(50));
    assert_eq!(fourth.flags.len(), 1, "{fourth:#?}");
}

#[test]
fn pin_and_pause_precedence_matrix_test() {
    let policy = policy(json!(true), 2);
    let pin = |count: Option<u32>, min: Option<u32>, max: Option<u32>, until: i64| Pin {
        count,
        min,
        max,
        until_ms: until,
        by: "alice".to_string(),
        reason: "bad deploy".to_string(),
    };
    let pause = |until: Option<i64>| Pause {
        until_ms: until,
        by: "bob".to_string(),
        reason: "incident".to_string(),
    };
    // The signal wants 6 (cpu 0.91 at 4); max is 8.
    // (pin, pause, to, kind, pin note, pause note)
    let cases = vec![
        (None, None, 6, DecisionKind::Applied, "", ""),
        (
            Some(pin(Some(12), None, None, at(3600))),
            None,
            12,
            DecisionKind::Applied,
            "pinned at 12 (above max 8: an operator override) by alice until 01:00 UTC: \"bad deploy\"",
            "",
        ),
        (Some(pin(Some(4), None, None, at(3600))), None, 4, DecisionKind::Held, "pinned at 4", ""),
        (Some(pin(Some(12), None, None, at(-1))), None, 6, DecisionKind::Applied, "", ""),
        (Some(pin(None, Some(7), None, at(3600))), None, 7, DecisionKind::Applied, "pinned floor 7", ""),
        (Some(pin(None, None, Some(5), at(3600))), None, 5, DecisionKind::Applied, "pinned ceiling 5", ""),
        (None, Some(pause(Some(at(3600)))), 4, DecisionKind::Held, "", "paused by bob until 01:00 UTC: \"incident\""),
        (None, Some(pause(None)), 4, DecisionKind::Held, "", "paused by bob: \"incident\""),
        (None, Some(pause(Some(at(-1)))), 6, DecisionKind::Applied, "", ""),
        // A pause wins over a pin: nothing is written, and both notes show.
        (
            Some(pin(Some(12), None, None, at(3600))),
            Some(pause(None)),
            4,
            DecisionKind::Held,
            "pinned at 12",
            "paused by bob",
        ),
    ];
    for (pin, pause, to, kind, pin_note, pause_note) in cases {
        let obs = Observation {
            pin: pin.clone(),
            pause: pause.clone(),
            ..Observation::new(4).reading("cpu", 0.91)
        };
        let decision = evaluate(&policy, &mut warm(&policy), &obs, at(0));
        let label = format!("pin {pin:?} pause {pause:?}");
        assert_eq!(decision.to, to, "{label}: {decision:#?}");
        assert_eq!(decision.kind, kind, "{label}");
        let pin_stage = &decision.stage(Stage::Pin).unwrap().note;
        assert!(pin_stage.contains(pin_note), "{label}: {pin_stage}");
        let pause_stage = &decision.stage(Stage::Pause).unwrap().note;
        assert!(pause_stage.contains(pause_note), "{label}: {pause_stage}");
        if pin_note.is_empty() {
            assert!(pin_stage.is_empty(), "{label}: {pin_stage}");
        }
    }
}

#[test]
fn scale_to_zero_wakes_by_activation_and_sleeps_when_idle_test() {
    let policy = policy(
        json!({
            "min": 0,
            "down": { "stabilize": "0s" },
            "checks": [{ "name": "lag", "source": "team-prom", "query": "sum(lag)",
                         "strategy": { "name": "per-replica", "value": 500 },
                         "activation": 1 }]
        }),
        2,
    );
    // Asleep: lag at or below activation keeps it at zero.
    let asleep = evaluate(
        &policy,
        &mut warm(&policy),
        &Observation::new(0).reading("lag", 1.0),
        at(0),
    );
    assert_eq!((asleep.to, asleep.kind), (0, DecisionKind::Held));
    assert!(asleep.summary.contains("asleep"), "{}", asleep.summary);
    // Woken: above activation, ceil(600 / 500) = 2.
    let woken = evaluate(
        &policy,
        &mut warm(&policy),
        &Observation::new(0).reading("lag", 600.0),
        at(0),
    );
    assert_eq!(
        (woken.to, woken.kind),
        (2, DecisionKind::Applied),
        "{woken:#?}"
    );
    assert!(woken
        .stage(Stage::Merge)
        .unwrap()
        .note
        .contains("woken by lag"));
    // A small but active backlog never drops below 1.
    let small = evaluate(
        &policy,
        &mut warm(&policy),
        &Observation::new(2).reading("lag", 3.0),
        at(0),
    );
    assert_eq!(small.stage(Stage::Merge).unwrap().after, 1);
    // Idle: goes to zero at the scale-in rate (one step from 1).
    let idle = evaluate(
        &policy,
        &mut warm(&policy),
        &Observation::new(1).reading("lag", 0.0),
        at(0),
    );
    assert_eq!(
        (idle.to, idle.kind),
        (0, DecisionKind::Applied),
        "{idle:#?}"
    );
    assert!(idle.stage(Stage::Merge).unwrap().note.contains("idle"));
    // Missing data is never idle.
    let unknown = evaluate(
        &policy,
        &mut warm(&policy),
        &Observation::new(1).missing("lag", "timeout"),
        at(0),
    );
    assert_eq!(unknown.to, 1);
}
