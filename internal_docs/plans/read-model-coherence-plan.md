# Plan: One Way Into the Read Model

> **Judgement calls:** [`../calls/read-model-coherence-plan_calls.md`](../calls/read-model-coherence-plan_calls.md)

**Status: IN EXECUTION (2026-10-10).** All five phases are built on
`read-model-coherence`. Deferred: a shadow check for raft replicas (§ 9),
and widening the scrub to task configs.

## 1. Problem

The manager plans from an in-memory `SchedulingReadModel`
(`src/storage/store.rs`): nodes, each node's allocations, task configs,
free pools, and the scope-epoch mirror. SQLite is the truth. Every write
has to reach both, and nothing checked that it did.

How a write reaches the cache depends on the storage mode:

- **Raft mode** derives the cache from the committed changeset
  (`project_changeset_side_effects`). A forgotten hook cannot drift the
  cache, because no method body updates it.
- **`local_disk` mode** relies on about 30 hand-placed `cache_*` and
  `refresh_read_model` calls after the SQL. A write method that forgets
  one drifts silently until restart. Nothing scrubs nodes or task configs,
  and allocations are only scrubbed when a plan applies.
- **Outside writers** (an operator's `sqlite3` against the live file, a
  second process) are invisible in both modes until restart.

A stale cache cannot oversubscribe a node. Plan-apply rebuilds free
capacity from SQLite inside the write transaction (`store.rs`, the
capacity recheck in `submit_and_apply_plan_with_candidates_apply`). It
does break things:

- a node the cache believes full is never picked, so its capacity is
  stranded;
- a node the cache believes free is picked by every plan and skipped by
  every apply, which burns retries and can exhaust the recheck budget;
- readers that never consult SQLite (node listing, cached task configs)
  serve stale data;
- in raft, each replica's cache can drift differently, so a failover
  changes scheduling behavior.

### Evidence

A shadow check (Phase 1) that compares the cache with a fresh load after
every `local_disk` mutation failed 95 of 1,468 unit tests on `main`, in
four classes:

| Op | What drifted | Why |
| --- | --- | --- |
| `insert_node` (78 tests) | cached `healthChecks` ahead of SQLite | the first op caches the registration message, and SQLite only gets health from the second op (`seed_node_health`) |
| `mark_nodes_as_lost` (14) | cached `availability = unavailable`, SQLite `available` | `cache_nodes_lost` writes what a *later* op (`sync_node_availability`) will commit |
| `complete_allocation` (1) | cached allocation status stale | no cache call at all |
| `destroy_job` (2) | destroyed job's task configs still cached | `DELETE FROM job` cascades into `task_config` and nothing drops the cache entry |

The first two are a cache that runs *ahead* of SQLite, which the
changeset path cannot do. The last two are plain misses: a leaked memory
entry per destroyed job config, and a stale status.

Turning the check on for the integration tests found a fifth, in every
mode: the full read-model load hardcoded `speculative_start: None` while
the incremental path read the column, so a restart, snapshot install, or
(after Phase 2) outside-write rebuild silently dropped every cached task's
speculative-start block (`tests/edge_boot_integration.rs`).

## 2. Prior art

- **Nomad** keeps state in an in-memory `go-memdb` that *is* the state
  store, fed only by FSM applies of raft log entries
  ([`nomad/fsm.go`](https://github.com/hashicorp/nomad/blob/main/nomad/fsm.go)).
  There is one way in. We take that shape: the changeset is our log entry.
- **Kubernetes** scheduler caches are fed by informers watching the API
  server
  ([`pkg/scheduler/backend/cache`](https://github.com/kubernetes/kubernetes/blob/master/pkg/scheduler/backend/cache/cache.go)),
  and the cache has a debugger that compares it with the informer's
  listers and logs differences on `SIGUSR2`
  ([`backend/cache/debugger`](https://github.com/kubernetes/kubernetes/tree/master/pkg/scheduler/backend/cache/debugger)).
  We take the comparator (Phase 1) and the periodic re-list idea (Phase 3).
- **SQLite** gives us the pieces: the
  [session extension](https://sqlite.org/sessionintro.html) records every
  row change on a connection, cascades included, and
  [`PRAGMA data_version`](https://sqlite.org/pragma.html#pragma_data_version)
  changes when another connection commits. We reject
  [`sqlite3_update_hook`](https://sqlite.org/c3ref/update_hook.html): it
  skips truncate-optimized `DELETE`s and `REPLACE` conflict deletes.

## 3. Jobs to be done

**Cluster operator**
- When I run a single manager on local disk, I want it to schedule from
  the same state it stores, so a node I can see as empty actually gets
  work.
- When I fix a row by hand in an emergency, I want the manager to notice,
  so I do not have to restart it to make the fix take effect.

**Contributor**
- When I add a store method that writes nodes or allocations, I want the
  cache to follow without me remembering a hook, so the scheduler cannot
  drift because of what I forgot.
- When I do break coherence, I want a test to fail and name the op, so I
  find it before a user does.

**On-call responder**
- When scheduling looks wrong, I want a metric that says whether the
  cache ever disagreed with disk, so I can rule it in or out quickly.

*Not in scope:* job authors and agents. Nothing they type changes.

## 4. Goals and non-goals

Goals:

- `local_disk` and raft maintain the read model through **one** code path:
  projection from the committed changeset.
- A test fails, naming the op, whenever a mutation leaves the cache
  disagreeing with SQLite.
- Drift from any source is repaired within a bounded time and counted.

Non-goals:

- Supporting outside writers as a workflow. We detect them and recover,
  and document "do not write the state DB while the manager runs".
- Changing event broadcast or eval wake-ups in `local_disk`. Only the
  read model moves to the changeset path (calls entry 2).
- Making the raft path cheaper. It is unchanged.

## 5. Proposed design

### 5.1 Phase 1: shadow check

`Store::read_model_divergence()` loads a fresh model and lists every
mismatch: nodes (minus heartbeat timestamps, which are deliberately
unprojected), allocation lists, task configs, free pools, and the epoch
mirror. `shadow_check_read_model(op)` panics with that list.

It runs after every outermost `local_disk` op, always in unit tests, and
anywhere with `LEGION_READ_MODEL_SHADOW_CHECK=1`.

```text
read model diverged from SQLite after `destroy_job` (1 mismatches):
  task configs of job-one-1: cached ids [1] != committed ids []
```

### 5.2 Phase 2: outside-writer guard

`Store::check_external_writes()` reads `PRAGMA data_version` on the write
connection. It changes only when *another* connection commits, so our own
writes never trip it. The status checker calls it every tick. On a change
it rebuilds the read model, logs a warning, and increments
`legion_read_model_external_writes_total`.

### 5.3 Phase 3: timed scrub and drift metric

The status checker reloads a rotating slice of nodes in full (node row and
allocation list) every tick in both modes, so drift the deltas never touch
heals within one sweep of the fleet. Every heal, from the scrub or from
the delta path's contradiction fallback, increments
`legion_read_model_drift_repaired_total{source}`.

### 5.4 Phase 4: `local_disk` mutations project from a changeset

`Store::replicated` in `local_disk` mode runs the body through
`capture::local_projected_mutation`:

1. Take the global capture mutex, so a second local op cannot interleave
   rows into this one's session.
2. Set `in_local_projection`, which makes every `cache_*` hook and
   `refresh_read_model` defer, exactly as `in_capture` does in raft.
3. Attach a session filtered to the read-model tables (`node`,
   `node_health`, `allocation`, `task_config`,
   `allocation_epoch_partition`).
4. Run the body. It commits normally.
5. Extract the changeset, even when the body failed partway, because what
   it committed is still committed.
6. Project the read-model half of `project_changeset_side_effects`, still
   under the mutex, so projections apply in commit order.
7. Run the shadow check.

Nested `replicated` calls compose into the outer session, like nested
captures. The job-intake coalescers and `local_write` treat
`in_local_projection` like `in_capture`, because waiting on another
thread while holding the capture mutex would deadlock.

The per-method `cache_*` calls stay as no-ops under the flag. They are
deleted in a follow-up once a release has run with the shadow check in CI
(calls entry 3).

### 5.5 Phase 5: loose ends

- A test that a session changeset records `ON DELETE CASCADE` child rows,
  which Phase 4 relies on for `destroy_job`.
- Projection drops a job config's task-config entry when it has no rows
  left, instead of caching an empty list forever (the raft half of the
  `destroy_job` leak).
- Audit `local_write` callers: they touch only `scheduler_event`, which
  the read model does not hold.

## 6. Surface

No jobspec, CLI, or API change. New operator-visible surface:

- `LEGION_READ_MODEL_SHADOW_CHECK=1` (test and soak runs only; reloads the
  whole model after every mutation).
- `legion_read_model_external_writes_total` and
  `legion_read_model_drift_repaired_total{source="scrub"|"delta"}`.
- A `warn!` naming the outside write when one is detected.

## 7. Persistence

No schema or wire change. Raft changesets are unchanged.

## 8. Phases

Each phase ships alone: 1 finds bugs, 2 and 3 bound them in production,
4 removes the cause, 5 closes the gaps 4 depends on.

## 9. Testing

- Phase 1 is itself the test: the whole unit suite runs with it on.
- Phase 2: a test commits from a second connection and asserts the cache
  follows after one `check_external_writes`.
- Phase 3: a test corrupts the cache and asserts a sweep repairs it and
  counts the repair.
- Phase 4: the suite with the shadow check on and zero divergences; a
  regression test per drift class in § 1; `plan_and_apply` before/after in
  `scheduler_bench`.
- Deferred: the shadow check on raft replicas. Lanes apply concurrently,
  so "after the op" is not a quiescent point there. It would need a
  per-lane barrier.

## 10. Rollout

Default-on, no flag. `local_disk` behavior changes in two visible ways:
the cache no longer runs ahead of SQLite between the two halves of
`insert_node` and `mark_nodes_as_lost` (raft already behaved this way),
and `local_disk` mutations now serialize end to end on the capture mutex
instead of per transaction. Their bodies are SQL only, since planning
runs before the op, so the cost is small. The bench numbers are in the PR.

## 11. Acceptance criteria

- `cargo test --all` passes with the shadow check on.
- Each § 1 drift class has a regression test that fails on `main`.
- An outside commit is reflected in the cache after one status-checker
  tick, and counted.
- `plan_and_apply` in `scheduler_bench` is within noise of `main`.
