# Handoff notes: validation state at the handoff checkpoint

Written by the session that coordinated the six fix tracks, at the moment it
handed the work off. Everything below was run locally in that session's
container, with CI's environment (`CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_TEST_DEBUG=0`, `LEGION_READ_MODEL_SHADOW_CHECK=1`).

## What is in this directory

- `findings.md`: the spot-check report as the tracks received it.
- `RULES.md`: the coordination rules the six parallel tracks followed.
- `report-<track>.md`: each track's hand-back report (root causes, fixes,
  files, tests, deferred items). The six tracks were lifecycle, scheduler,
  ha, containers, workerplane, and cli.
- `calls-<track>.md`: each track's judgement-call draft. They are already
  merged and ranked into
  `internal_docs/calls/launch-spot-check-fixes-plan_calls.md`. The
  workerplane calls live in `worker-plane-auth-plan_calls.md`, entries 10,
  11, 15, and 20 to 25.
- `pr-description-draft.md`: a draft PR #17 description.
  - `@@TESTS@@` and `@@BENCH@@` are placeholders.
  - It was never posted.

## Already done (in the commits)

- **Umbrella plan:** `internal_docs/plans/launch-spot-check-fixes-plan.md`,
  with §8 (per-finding outcome) and §8.7 (deferred).
- **Findings record:** `internal_docs/spot-check-2026-10-11-findings.md`.
- **Merged calls doc:** `internal_docs/calls/launch-spot-check-fixes-plan_calls.md`
  (52 entries).
- **Registrations:** both are registered in `internal_docs/README.md`.
- **`internal_docs/TODO.md`:**
  - Shipped work is checked off under Done, "Pre-launch spot check
    (2026-10-11)".
  - Deferred work is open under "Follow-ups from the 2026-10-11 spot-check
    fixes".
  - The In-flight table lists #17 and drops the merged #14.
- **UI:** `ui/src/gen` was regenerated with `npm run gen`, and `npm run build`
  passes. The dead `done` special-case in `JobDetail.tsx` is removed.
- **Shipped worker config:** `configs/workers/default.ncl` allows exactly the
  default 13 capabilities. `NET_RAW` is commented out, a coordinator call
  that tightens the containers track's version. The test
  `the_default_worker_config_allow_caps_are_the_default_set_test` matches.

## Validation results

### Checks that pass

- `cargo fmt --all -- --check` is clean.
- `cargo clippy --all --all-features --locked -- -D warnings -A dead_code -A unused_variables`
  is clean. This is CI's exact invocation.

### Full suite, run 1 (`cargo test --all --all-features --locked --no-fail-fast`)

About 2,170 tests passed and 6 failed. All six are fixed in this checkpoint:

| Failure | Cause | Fix |
| --- | --- | --- |
| `manager::server::tests::failed_allocations_from_non_owner_node_are_ignored` | `handle_failed_allocations` / `handle_terminated_allocations` queued a capacity pass even when every report was ignored | Both now track `freed_capacity` |
| `raft_seam::replicated_call_sites_match_classification` | The new `reset_task_statuses` (status lane, inlined closure) was unclassified | Classified it |
| `raft_durability::minority_partition_rejects_writes` | The rejection must contain "retry", and the ha track's rewording had dropped it | `NotRaftLeader`'s display now reads "retry on its leader {name} (raft advertise address {addr})" |
| `scheduler_integration` (3 tests) | The harness minted its own numeric eval ids, which collided with evals the store now queues itself (capacity passes) | The harness uses `store.enqueue_eval` and runs the loop until its eval is handled (`process_eval`) |

After these fixes, the following passed:
- `--test raft_durability`, `--test raft_follower_writes`, `--test raft_seam`
  and `--test scheduler_integration`.
- `--lib` for `manager::server::tests`, `storage::raft`, `leader_forward` and
  the `manager::daemon` default-worker-config tests.

### Full suite, run 2

Stopped at the handoff while it was in `tests/node_liveness.rs`. The lib
suite finished: 1909 passed, 2 failed, 29 ignored. Both failures are still
open:

1. **`manager::server::tests::failed_allocations_enqueue_reschedule`**
   - It asserts on the *first* eval and expects `job_reschedule`. It got
     `node_join`: the capacity pass that `request_capacity_reconcile_if_waiting`
     now queues, because `capacity_waiters` starts set.
   - The order is not deterministic; the test passed in run 1.
   - Fix the test to find the `job_reschedule` eval among all evals rather
     than take the first one.
2. **`artifacts::fetchers::oci::tests::digest_references_and_storeless_pulls`**
   - The in-process fake registry answered a blob request with a 503.
   - It passed in run 1 and in the containers track's runs, so this looks
     like a flake in the fake registry under load.
   - It needs a root cause before CI ("flake" is not one). Check whether the
     fake registry's request counter or 503 path can fire on a retry.

All integration test binaries that ran in run 2 passed, from `acl_endpoints`
through `node_drain`. The rest of run 2 did not run.

## Not done yet

- **Scheduler benchmarks.** AGENTS.md requires before and after numbers for
  the groups `scheduler_speed`, `candidate_construction`, `plan_under_fill`
  and `plan_fleet_scaling` in `benchmarks/micro/scheduler_bench.rs`. A
  detached worktree of the base (`da1c811`) was at `/home/user/legion-base`
  in the old container. Its bench build had died with ENOSPC and was never
  rerun.
- **Proto-breaking label.** No `proto-breaking-ok` label has been applied.
  The proto changes reserve `IssueNodeCertificateReply.key_pem`'s number, so
  `buf breaking` (WIRE) should pass. It passed on the earlier checkpoint
  commit `fbbc188`.
- **PR #17 title and description** have not been updated. They still read
  "Worker plane: mutual TLS, node identity, and manager failover".
- **Disk.** The container's disk allowance was about 40 GB. A full-debuginfo
  `cargo build --tests` fills it (each test binary is ~700 MB), so build with
  CI's `CARGO_PROFILE_*_DEBUG=0`.
