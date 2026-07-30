//! Coverage gate for the raft replication seam (phase A-0/A-2 of
//! `docs/plans/durability-and-ops-execution-plan.md`).
//!
//! Every public mutating `Store` method must be classified into a raft lane
//! and routed through `Store::replicated()`, with its body renamed to a
//! private `<name>_apply` fn (or delegating into another replicated method's
//! body). These tests make that classification *total*: they parse
//! `src/storage/store.rs` + `src/storage/acl_store.rs` and assert that the
//! set of `fn <name>_apply` bodies and the set of `replicated(Lane::…, "…")`
//! call sites exactly match the hardcoded classification below. Adding a
//! mutating method without classifying it here fails these tests.
//!
//! Documented exemptions (never replicated, by design):
//! - `reset_all_tables` — test-only, runs pre-engine.
//! - migrations — schema is per-binary, deterministic, local.
//! - node-local maintenance routed through the scoped `Store::local_write`
//!   seam. Since JC-41 returned the eval queue to raft this is exactly ONE
//!   op — the JC-28 commutative journal prune. `LOCAL_WRITE_OPS` below
//!   makes that classification total: local-write call sites must exactly
//!   match the list, so the exemption can never silently grow back.

use std::collections::{BTreeMap, BTreeSet};

use regex::Regex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lane {
    Control,
    Scheduler,
    Status,
}

impl Lane {
    fn name(self) -> &'static str {
        match self {
            Lane::Control => "Control",
            Lane::Scheduler => "Scheduler",
            Lane::Status => "Status",
        }
    }
}

/// Every public mutating store method (all conversion batches + the original
/// samples), in one place. The `lane_for` match below is the classification;
/// this list drives it and the source-grep assertions.
const CLASSIFIED_MUTATIONS: &[&str] = &[
    // Control: user/operator intent (jobs, nodes admin, KV, signing keys,
    // saved queries, state-store writes, ACL). `sync_node_availability` is
    // the control-lane half of the C-2 status composites (mark ready/lost
    // flip `node.availability` through it).
    "acquire_kv_lock",
    "bootstrap_acl_token",
    "clear_job_failed_status",
    "clear_job_restart_request",
    "clear_job_stopped_status",
    "delete_acl_policy",
    "delete_acl_token",
    "delete_acl_user",
    "delete_join_token",
    "delete_kv",
    "delete_node_tokens",
    "delete_saved_state_query",
    "delete_signing_key",
    "destroy_job",
    "get_kv_lock",
    "handle_job_submit",
    "idempotency_record",
    "insert_acl_token",
    "insert_join_token",
    "insert_node",
    "insert_signing_key",
    "insert_worker_type",
    "mark_job_failed",
    "mark_job_failed_for_kv",
    "patch_kv",
    "prune_expired_credentials",
    "put_kv",
    "query_state_store",
    "release_kv_lock",
    "remove_node",
    "renew_kv_lock",
    "replace_job_kv_references",
    "request_job_restart",
    "set_node_availability",
    "stop_job",
    "sync_node_availability",
    "update_node_dyamic",
    "update_signing_key_state",
    "upsert_acl_policy",
    "upsert_acl_user",
    "upsert_manager_config",
    "upsert_saved_state_query",
    // Scheduler: placement, instances, allocation reconciliation, the EVAL
    // QUEUE (replicated again since JC-41 — evals are not regenerable work)
    // and — since C-2's rollout lane consolidation — every
    // rollout/rollout_event mutation.
    "append_rollout_event",
    "claim_eval",
    "complete_eval",
    "complete_eval_with_event",
    "enqueue_eval_after_delay",
    "insert_eval",
    "prune_stale_evals",
    "release_eval_for_retry",
    "release_eval_for_retry_with_event",
    "requeue_in_flight_evals",
    "set_eval_status",
    "apply_allocation_plan",
    "apply_plan_submission",
    "submit_and_apply_plan",
    "submit_and_apply_plan_with_candidates",
    "create_keyed_instance",
    "create_rollout",
    "destroy_job_scheduler_cleanup",
    "create_keyed_instances",
    "destroy_keyed_instance",
    "get_or_create_slot_instance",
    "insert_plan_submission",
    "insert_plan_submission_with_candidates",
    "migrate_allocations_to_job_config",
    "place_alloc",
    "prune_rollouts",
    "reap_orphaned_volumes",
    "record_volume_error",
    "record_volume_mounted",
    "remove_allocation_by_id",
    "remove_allocations",
    "remove_allocs_for_job",
    "remove_allocs_for_job_config",
    "remove_allocs_for_node",
    "seed_keyed_instances",
    "set_instance_current_alloc",
    "set_instance_state",
    "stamp_allocations_failed_for_kv",
    "start_keyed_instance",
    "stop_instances_for_job",
    "stop_keyed_instance",
    "update_rollout_config_json",
    "update_rollout_status",
    "upsert_instance",
    // Volumes: the `volume` table is scheduler-owned (its lifecycle is bound
    // to the scheduler-owned `instance`, and the reclaim reaper drives its
    // state machine — see `storage::raft::ownership`).
    "delete_volume",
    "mark_volume_destroyed",
    "sync_task_volume_claims",
    // Status: worker-reported liveness/health/status and event emission.
    // (`seed_node_health` / `prune_node_health_orphans` are the C-1 node
    // split's status-lane halves; `seed_task_statuses`,
    // `destroy_job_status_cleanup`, and `prune_allocation_health_orphans`
    // are the C-2 composite splits' status-lane halves — `node_health`,
    // `task_status`, and `allocation_health` are single-written by this
    // lane, plan §7-C2.)
    "destroy_job_status_cleanup",
    "emit_event",
    "mark_node_lost",
    "mark_node_ready",
    "mark_nodes_as_lost",
    "prune_allocation_health_orphans",
    "prune_node_health_orphans",
    "seed_missing_task_statuses",
    "seed_node_health",
    "seed_task_statuses",
    "set_node_lost_at",
    "set_task_status",
    "update_node_health_checks",
    "update_node_heartbeat",
    "update_task_statuses_for_allocations",
    "upsert_allocation_health_snapshots",
];

/// Replicated public methods with no same-named `_apply` fn — either
/// because they share another body or because the closure inlines it:
/// - `complete_eval` / `complete_eval_with_event` share `complete_eval_impl`;
/// - `request_job_restart` / `clear_job_restart_request` are one-statement
///   marker flips inlined in their closures.
///
/// (`enqueue_eval` is absent from `CLASSIFIED_MUTATIONS` for a different
/// reason: it is a pure convenience wrapper that forwards to
/// `enqueue_eval_after_delay` with a zero delay, so it has no lane, no
/// closure, and no op name of its own.)
const DELEGATED_BODIES: &[&str] = &[
    "clear_job_restart_request",
    "complete_eval",
    "complete_eval_with_event",
    // Serializes its candidate placements in the closure and shares
    // `insert_plan_submission_apply` with the format-1 wrapper.
    "insert_plan_submission_with_candidates",
    "request_job_restart",
    // Plan §8 D1 reconciler: inlines its INSERT..SELECT closure and shares
    // seed_task_statuses_apply's semantics (ON CONFLICT DO NOTHING seed).
    "seed_missing_task_statuses",
];

/// Public mutating methods that are deliberately NOT replicated — they route
/// through `Store::local_write` and touch only node-local tables. EMPTY
/// since JC-41 returned the eval queue to raft: no public store mutation is
/// exempt from replication any more. An entry here is a durability decision
/// (its rows are lost on failover), not a classification detail.
const LOCAL_WRITE_MUTATIONS: &[&str] = &[];

/// Local-write methods with no same-named `_apply` body. Empty with
/// `LOCAL_WRITE_MUTATIONS`.
const LOCAL_DELEGATED_BODIES: &[&str] = &[];

/// Every op name passed to `Store::local_write` in the store sources — the
/// full local-write surface, now a single internal maintenance op.
/// `prune_scheduler_events` is the JC-28 local journal maintenance
/// (commutative tables replicate INSERTs only; each node prunes its own
/// journal).
const LOCAL_WRITE_OPS: &[&str] = &["prune_scheduler_events"];

/// The lane classification. Exhaustive over `CLASSIFIED_MUTATIONS`; any
/// method name outside the list panics, so a newly discovered `_apply` fn or
/// `replicated()` call site that was not classified here fails the tests.
fn lane_for(method: &str) -> Lane {
    match method {
        "acquire_kv_lock"
        | "bootstrap_acl_token"
        | "clear_job_failed_status"
        | "clear_job_restart_request"
        | "clear_job_stopped_status"
        | "delete_acl_policy"
        | "delete_acl_token"
        | "delete_acl_user"
        | "delete_join_token"
        | "delete_kv"
        | "delete_node_tokens"
        | "delete_saved_state_query"
        | "delete_signing_key"
        | "destroy_job"
        | "get_kv_lock"
        | "handle_job_submit"
        | "idempotency_record"
        | "insert_acl_token"
        | "insert_join_token"
        | "insert_node"
        | "insert_signing_key"
        | "insert_worker_type"
        | "mark_job_failed"
        | "mark_job_failed_for_kv"
        | "patch_kv"
        | "prune_expired_credentials"
        | "put_kv"
        | "query_state_store"
        | "release_kv_lock"
        | "remove_node"
        | "renew_kv_lock"
        | "replace_job_kv_references"
        | "request_job_restart"
        | "set_node_availability"
        | "stop_job"
        | "sync_node_availability"
        | "update_node_dyamic"
        | "update_signing_key_state"
        | "upsert_acl_policy"
        | "upsert_acl_user"
        | "upsert_manager_config"
        | "upsert_saved_state_query" => Lane::Control,
        "append_rollout_event"
        | "apply_allocation_plan"
        | "claim_eval"
        | "complete_eval"
        | "complete_eval_with_event"
        | "enqueue_eval_after_delay"
        | "insert_eval"
        | "prune_stale_evals"
        | "release_eval_for_retry"
        | "release_eval_for_retry_with_event"
        | "requeue_in_flight_evals"
        | "set_eval_status"
        | "apply_plan_submission"
        | "submit_and_apply_plan"
        | "submit_and_apply_plan_with_candidates"
        | "create_keyed_instance"
        | "create_rollout"
        | "destroy_job_scheduler_cleanup"
        | "create_keyed_instances"
        | "destroy_keyed_instance"
        | "get_or_create_slot_instance"
        | "insert_plan_submission"
        | "insert_plan_submission_with_candidates"
        | "migrate_allocations_to_job_config"
        | "place_alloc"
        | "prune_rollouts"
        | "reap_orphaned_volumes"
        | "record_volume_error"
        | "record_volume_mounted"
        | "remove_allocation_by_id"
        | "remove_allocations"
        | "remove_allocs_for_job"
        | "remove_allocs_for_job_config"
        | "remove_allocs_for_node"
        | "seed_keyed_instances"
        | "set_instance_current_alloc"
        | "set_instance_state"
        | "stamp_allocations_failed_for_kv"
        | "start_keyed_instance"
        | "stop_instances_for_job"
        | "stop_keyed_instance"
        | "update_rollout_config_json"
        | "update_rollout_status"
        | "upsert_instance"
        | "delete_volume"
        | "mark_volume_destroyed"
        | "sync_task_volume_claims" => Lane::Scheduler,
        "destroy_job_status_cleanup"
        | "emit_event"
        | "mark_node_lost"
        | "mark_node_ready"
        | "mark_nodes_as_lost"
        | "prune_allocation_health_orphans"
        | "prune_node_health_orphans"
        | "seed_missing_task_statuses"
        | "seed_node_health"
        | "seed_task_statuses"
        | "set_node_lost_at"
        | "set_task_status"
        | "update_node_health_checks"
        | "update_node_heartbeat"
        | "update_task_statuses_for_allocations"
        | "upsert_allocation_health_snapshots" => Lane::Status,
        other => panic!(
            "mutating store method `{other}` is not classified into a raft lane; \
             add it to CLASSIFIED_MUTATIONS and lane_for in tests/raft_seam.rs"
        ),
    }
}

fn store_sources() -> String {
    let root = env!("CARGO_MANIFEST_DIR");
    let mut combined = String::new();
    for path in ["src/storage/store.rs", "src/storage/acl_store.rs"] {
        combined.push_str(
            &std::fs::read_to_string(format!("{root}/{path}"))
                .unwrap_or_else(|e| panic!("failed reading {path}: {e}")),
        );
        combined.push('\n');
    }
    combined
}

fn diff(label_missing: &str, missing: &BTreeSet<&str>, label_extra: &str, extra: &BTreeSet<&str>) {
    assert!(
        missing.is_empty() && extra.is_empty(),
        "{label_missing}: {missing:?}\n{label_extra}: {extra:?}"
    );
}

/// The hardcoded classification exactly matches the set of `fn <name>_apply`
/// bodies in store.rs + acl_store.rs: every classified or local-write method
/// (except the documented delegating ones) has an `_apply` body, and every
/// `_apply` body is classified or a documented local-write body.
#[test]
fn apply_fn_set_matches_classification() {
    let source = store_sources();
    let apply_re = Regex::new(r"fn ([a-z0-9_]+)_apply\s*\(").expect("valid regex");
    let found: BTreeSet<String> = apply_re
        .captures_iter(&source)
        .map(|caps| caps[1].to_string())
        .collect();
    let found: BTreeSet<&str> = found.iter().map(String::as_str).collect();

    let delegated: BTreeSet<&str> = DELEGATED_BODIES.iter().copied().collect();
    let local: BTreeSet<&str> = LOCAL_WRITE_MUTATIONS.iter().copied().collect();
    let local_delegated: BTreeSet<&str> = LOCAL_DELEGATED_BODIES.iter().copied().collect();
    let expected: BTreeSet<&str> = CLASSIFIED_MUTATIONS
        .iter()
        .copied()
        .filter(|name| !delegated.contains(name))
        .chain(
            LOCAL_WRITE_MUTATIONS
                .iter()
                .copied()
                .filter(|name| !local_delegated.contains(name)),
        )
        .collect();

    // Every discovered `_apply` body must map to a lane (panics on an
    // unclassified name) — unless it is a documented local-write body.
    for name in &found {
        if !local.contains(name) {
            let _ = lane_for(name);
        }
    }

    let missing: BTreeSet<&str> = expected.difference(&found).copied().collect();
    let extra: BTreeSet<&str> = found.difference(&expected).copied().collect();
    diff(
        "classified methods with no `_apply` body in the source (conversion incomplete?)",
        &missing,
        "`_apply` bodies in the source that are neither in CLASSIFIED_MUTATIONS nor \
         LOCAL_WRITE_MUTATIONS (unclassified mutation?)",
        &extra,
    );
}

/// The local-write surface is total and exclusive: every `local_write("op")`
/// call site in the store sources is in LOCAL_WRITE_OPS (and vice versa),
/// and no local-write method is also proposed through `replicated()`. This
/// pins the unreplicated exemption — since JC-41, JC-28 journal pruning
/// alone — so it cannot silently grow back into user-visible state.
#[test]
fn local_write_call_sites_match_the_exemption_list() {
    let source = store_sources();
    let call_re = Regex::new(r#"local_write\(\s*"([a-z0-9_]+)""#).expect("valid regex");
    let found: BTreeSet<String> = call_re
        .captures_iter(&source)
        .map(|caps| caps[1].to_string())
        .collect();
    let found: BTreeSet<&str> = found.iter().map(String::as_str).collect();
    let expected: BTreeSet<&str> = LOCAL_WRITE_OPS.iter().copied().collect();

    let missing: BTreeSet<&str> = expected.difference(&found).copied().collect();
    let extra: BTreeSet<&str> = found.difference(&expected).copied().collect();
    diff(
        "LOCAL_WRITE_OPS entries with no `local_write(…)` call site",
        &missing,
        "`local_write(…)` ops not in LOCAL_WRITE_OPS (undocumented local-write exemption?)",
        &extra,
    );

    // A method is local-write XOR replicated, never both.
    let replicated_re =
        Regex::new(r#"replicated\(\s*Lane::\w+,\s*"([a-z0-9_]+)""#).expect("valid regex");
    for caps in replicated_re.captures_iter(&source) {
        let op = caps[1].to_string();
        assert!(
            !LOCAL_WRITE_MUTATIONS.contains(&op.as_str()),
            "op `{op}` is both a local-write mutation and a replicated() op"
        );
    }
}

/// Ops proposed with a RUNTIME-chosen lane instead of a literal `Lane::…`.
/// Legal only for writes confined to JC-28 commutative tables (any lane may
/// carry the insert): `emit_event` picks a lane this node currently leads
/// so standalone events are not silently dropped under divergent lane
/// leadership (plan §8). An entry here must justify itself the same way —
/// the capture-time ownership guard still rejects a non-commutative write
/// proposed on the wrong lane, so this list is about classification
/// coverage, not safety.
const DYNAMIC_LANE_OPS: &[&str] = &["emit_event"];

/// Every `replicated(Lane::…, "op", …)` call site agrees with the hardcoded
/// lane classification, and every classified method has exactly such a call
/// site — i.e. the classification is total over the replication seam.
/// `DYNAMIC_LANE_OPS` are matched by op name with the lane check skipped.
#[test]
fn replicated_call_sites_match_classification() {
    let source = store_sources();
    let call_re =
        Regex::new(r#"replicated\(\s*Lane::(\w+),\s*"([a-z0-9_]+)""#).expect("valid regex");
    let dynamic_re = Regex::new(r#"replicated\(\s*lane,\s*"([a-z0-9_]+)""#).expect("valid regex");

    let mut ops: BTreeMap<String, String> = BTreeMap::new();
    for caps in call_re.captures_iter(&source) {
        let lane = caps[1].to_string();
        let op = caps[2].to_string();
        if let Some(previous) = ops.insert(op.clone(), lane.clone()) {
            assert_eq!(
                previous, lane,
                "op `{op}` is proposed on two different lanes ({previous} vs {lane})"
            );
        }
    }

    for (op, lane) in &ops {
        assert_eq!(
            lane_for(op).name(),
            lane,
            "op `{op}` uses Lane::{lane} in the source but is classified differently \
             in tests/raft_seam.rs"
        );
    }

    let mut found: BTreeSet<&str> = ops.keys().map(String::as_str).collect();
    for caps in dynamic_re.captures_iter(&source) {
        let op = caps[1].to_string();
        let known = DYNAMIC_LANE_OPS
            .iter()
            .find(|name| **name == op)
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "op `{op}` is proposed with a runtime lane but is not in \
                     DYNAMIC_LANE_OPS; dynamic lanes are legal only for JC-28 \
                     commutative writes"
                )
            });
        found.insert(known);
    }
    let expected: BTreeSet<&str> = CLASSIFIED_MUTATIONS.iter().copied().collect();
    let missing: BTreeSet<&str> = expected.difference(&found).copied().collect();
    let extra: BTreeSet<&str> = found.difference(&expected).copied().collect();
    diff(
        "classified methods with no `replicated(…)` call site (conversion incomplete?)",
        &missing,
        "`replicated(…)` ops not in CLASSIFIED_MUTATIONS (unclassified mutation?)",
        &extra,
    );
}

/// The classification list has no duplicate entries (a duplicate would let a
/// lane mismatch hide behind set semantics).
#[test]
fn classification_has_no_duplicates() {
    let mut seen = BTreeSet::new();
    for name in CLASSIFIED_MUTATIONS {
        assert!(seen.insert(*name), "duplicate classification entry: {name}");
    }
    // Local-write mutations are disjoint from the replicated classification.
    for name in LOCAL_WRITE_MUTATIONS {
        assert!(
            seen.insert(*name),
            "method `{name}` is listed as both replicated and local-write"
        );
    }
    for name in LOCAL_DELEGATED_BODIES {
        assert!(
            LOCAL_WRITE_MUTATIONS.contains(name),
            "LOCAL_DELEGATED_BODIES entry `{name}` is not a local-write mutation"
        );
    }
    // And every replicated entry is accepted by the lane match.
    for name in CLASSIFIED_MUTATIONS {
        let _ = lane_for(name);
    }
}
