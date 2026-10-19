# Plan: The Scheduler Lane's Per-Entry Cost

> **Judgement calls:** [`calls/scheduler-lane-entry-cost-plan_calls.md`](../calls/scheduler-lane-entry-cost-plan_calls.md)

**Status: IN EXECUTION** (written 2026-10-13). Phase 1 shipped: a keyed
placement binds its instance from two indexes instead of scanning the
task's whole population, which was the largest piece of serial lane time in
the profile (§5, §14). Phases 2 and 3 are designed here and not built: a
pipelined lane that captures entry N+1 while entry N commits, and one fsync
per entry instead of two. Each needs its own gate before it ships (§9).
Deliberately deferred: binding in O(log n) of the live population (§11),
and the AWS-rig run of every number here (all are from one loaded laptop,
§14.1).

## 1. Problem statement

The replicated store's scheduler lane commits one entry at a time, and it
is the measured ceiling for keyed sandboxes: about 50 full create →
suspend → resume → destroy lifecycles a second on the 4-vCPU reference
host, against the sandbox-scaling plan's target of about 125
([sandbox-scaling plan §14.3](sandbox-scaling-plan.md#143-phase-1-group-commit-and-write-trimming),
[calls #17](../calls/sandbox-scaling-plan_calls.md)). That run measured
about 6 ms of serial lane time per entry: changeset capture 3.2 ms and
propose → apply 2.7 ms, with the lane's pipeline thread about 54% busy and
the manager at 1.1 of 4 cores. `TODO.md` named three levers: more
placements per entry, a cheaper capture, or a pipelined lane.

Nobody had looked inside the 3.2 ms. A capture runs the write's body
against the leader's database inside a savepoint, so its cost is whatever
the body costs. What a platform sees is that keyed starts get slower as it
grows, and nothing on the operator's side explains why.

## 2. Prior art

- **SQLite partial and covering indexes.** A partial index holds only the
  rows its `WHERE` admits, and the planner uses it only when the query's
  `WHERE` provably implies the index's
  ([partial indexes](https://www.sqlite.org/partialindex.html)). A covering
  index answers a lookup without reading the table
  ([query planner](https://www.sqlite.org/queryplanner.html#covidx)).
  **Take:** both, for Phase 1.
- **Nomad's plan applier** ([`nomad/plan_apply.go`](https://github.com/hashicorp/nomad/blob/main/nomad/plan_apply.go))
  pipelines exactly the way Phase 2 proposes: while plan N's raft apply is
  in flight, it verifies plan N+1 against an optimistic snapshot that
  already includes the pending plans' results, and refreshes that snapshot
  from committed state once they land. A failed commit only wastes work
  done while the applier would otherwise have been idle. **Take:** the
  shape. **Differs:** Nomad
  verifies a plan against an in-memory state store whose reads all see the
  optimistic snapshot. Legion's capture bodies run SQL and can also read
  through the read pool and the in-memory read model, which do not see an
  uncommitted entry (§6.2).
- **dqlite** ([replication](https://canonical.com/dqlite/docs/explanation/replication))
  executes a write once on the leader, replicates the pages it modified,
  and writes them to its WAL only after quorum, holding the database's
  write lock throughout. **Reject for now:** Legion's leader rolls the
  capture back and re-applies the committed changeset. Holding the write
  connection for a whole quorum round would stall every other lane's apply
  behind it.
- **Raft state machines over a durable log** (etcd, Consul) keep the state
  machine at weaker durability than the log and rebuild it from the last
  snapshot plus log replay after a crash. **Take:** for Phase 3.
- **Kubernetes StatefulSets** bind ordinals in the controller from an
  informer cache, never by scanning the store on the write path. Legion's
  keyed binding runs in the write path by design (the bind is atomic with
  the allocation insert), so the scan has to be cheap rather than absent.

## 3. Jobs to be done, by persona

**Platform builder running sandboxes (job author)**
- When thousands of my users' sandboxes are parked and a few hundred are
  running, I want a new one to start as fast as it would on an empty
  cluster, so my idle users never slow down my active ones.

**Cluster operator**
- When I size managers for a sandbox fleet, I want the start rate to
  depend on my hardware, not on how many sandboxes exist, so the capacity
  I planned for stays true as the platform grows.
- When starts slow down, I want the write path's metrics to say which
  part grew, so I know whether to add disk, CPU, or nothing.

**Autonomous agent**
- When I create sandboxes in a burst, I want each to place in
  milliseconds, so my tool calls do not time out behind each other.

*Not in scope:* the on-call responder beyond the metrics already exported.

### User scenarios

1. An agent platform keeps 5,000 sandboxes suspended and 500 running, and
   creates 50 a second. Each create's placement binds in the time it takes
   to look at the 500 live rows, not the 5,500.
2. An operator rolls a new image to a keyed task. Every live instance is
   unbound for the new config, so each placement still finds the oldest
   one, as before.

## 4. Goals and non-goals

**Goals**
- Find where the lane's per-entry time goes, phase by phase and op by op.
- Remove the largest piece that grows with the cluster from the serial
  lane path, without changing which instance a placement binds.
- Keep every raft-entry count in `tests/raft_lifecycle_ops.rs` and leave
  the raft entry format alone.
- Lay out the pipelined lane and the fsync cut precisely enough that each
  can be built and gated on its own.

**Non-goals**
- How many placements go into a plan, or eval batching (the multi-eval
  planning work).
- The plan coalescer's batch cap and the ratification path.
- Terms in the plan apply that grow with the node count.
- Choosing a different instance than today's binding would.

## 5. Background: what exists today

**The write path.** `Store::replicated` hands a closure to the lane's
pipeline thread (`src/storage/raft/driver.rs`, `pipeline_loop`), which runs
`execute_replicated`: confirm leadership, `capture::capture_mutation` (open
`SAVEPOINT legion_capture`, attach a SQLite session, run the body, extract
the changeset, roll back), check ownership and peer schemas, then
`raft.client_write`, which returns after the log append (fsync), quorum,
and the local state machine's apply (`capture::apply_changeset_with`:
changeset apply, applied marker, commit with `synchronous = FULL`, then
side-effect projection). Captures and applies share one write connection
and exclude each other through `CAPTURE_MUTEX` (`src/storage/raft/capture.rs`).

**The keyed bind.** Every placement of a keyed task goes through
`insert_allocation_with_data` → `assign_stateful_instance_tx` →
`assign_keyed_instance_tx` (`src/storage/store.rs`), which picks the oldest
unbound live instance of the task, with node-local volume, restore, and
reschedule-hold preferences, and binds it in the same transaction as the
allocation insert. Before Phase 1 the query searched
`idx_instance_job_group (job_id, group_name)`, which holds every instance
of the job in every state, and tested "unbound for this config" with
`instance_id NOT IN (SELECT instance_id FROM allocation WHERE
task_config_id = ?)`, which SQLite materializes as a temporary B-tree of
every allocation of the config, rebuilt on every call. It also re-parsed
its SQL on every call (`query_row` rather than `prepare_cached`).

**The profile** (§14.2) put that one statement at about half of the
pipeline thread's wall time under the sandbox lifecycle load, and about
two thirds of all capture time.

## 6. Proposed design

### 6.1 Phase 1: bind keyed placements from indexes (shipped)

Three changes, no change to which instance binds:

- A partial index over live keyed instances only:

  ```sql
  CREATE INDEX idx_instance_keyed_live
      ON instance(job_id, group_name, task_name, instance_id)
      WHERE mode = 'keyed' AND state NOT IN ('terminated', 'stopped', 'suspended');
  ```

  The binding query's `mode` and `state` terms are the index's `WHERE`
  word for word, so SQLite uses it, and suspended, stopped, and terminated
  rows (most of a large sandbox population) are never read. The same
  terms make `count_live_keyed_instances`, which the planner runs per
  keyed eval, an index-only count.
- "Unbound for this config" becomes a correlated probe,
  `NOT EXISTS (SELECT 1 FROM allocation WHERE allocation.instance_id =
  instance.instance_id AND allocation.task_config_id = ?4)`, answered by a
  new covering index `idx_allocation_instance_config (instance_id,
  task_config_id)`. It replaces `idx_allocation_instance_id`, whose
  lookups it serves as a prefix, so allocation writes maintain the same
  number of indexes. A bound live row now costs two index lookups and no
  table read. `instance_id IS NOT NULL` keeps the rewrite exact: `NOT IN`
  never matched a NULL id, and `NOT EXISTS` alone would.
- The SQL moves to a `const` and goes through `prepare_cached`.

A placement's bind is still proportional to the task's *live* instances.
§11 says why that is acceptable for now and what would remove it.

There is no knob. Nothing about the change is a trade a user would want to
make differently: the index costs a little on every instance write (§10)
and nothing else.

### 6.2 Phase 2: a pipelined lane (designed, not built)

The pipeline thread waits out every entry's propose → apply before it
captures the next. Nomad's applier shows the alternative: capture entry
N+1 while N commits.

- **Overlay.** Entry N+1's capture first replays the changesets of every
  in-flight entry the local state machine has not applied yet into its
  capture savepoint, then attaches the session, so N+1's changeset is
  relative to the state after N. The state machine records, under the
  capture mutex, which proposal it applied last, so the overlay is exact.
- **Apply-time validity.** Every entry carries the term it was captured
  in. The state machine applies an entry only if its log term equals its
  capture term, and otherwise skips it (no rows, applied marker advances,
  the proposer gets a retryable error). In one term only this leader's
  pipeline appends, in order, and Raft's log matching means N+1 committed
  in term T implies N committed before it, so a skipped predecessor can
  never leave a successor applied on top of state it assumed. The same
  rule also closes today's narrow window where a capture taken in one term
  is appended in a later one.
- **Foreign keys.** The real apply runs with `foreign_keys = OFF`, but a
  capture savepoint cannot turn them off, so overlay replay sees cascades
  fire. The replay treats a missing row on DELETE as already deleted and
  falls back to a non-speculative capture (drain, then capture) on any
  other conflict.
- **Stale reads are the hard part.** The overlay is visible only on the
  write connection. A body that reads through the read pool or the
  in-memory read model sees state without the in-flight entries. Such
  bodies exist: `stop_keyed_instance_apply` reads its instance with
  `get_instance`, which uses the read pool. Phase 2 therefore speculates
  only for ops on an audited allowlist (the instance group commit, plan
  applies, eval enqueue and settle), drains the pipeline before any other
  op, and fails closed on a speculative capture that touches the read
  pool or the read model.
- **Knob.** `raft.pipelineDepth`, default 1 (today's behavior) until the
  allowlist audit and a multi-voter measurement say otherwise.

The win is bounded by the share of propose → apply that is not itself
under the capture mutex: the log fsync, replication, and raft task hops.
On one voter that was about 1 to 2 ms of the reference host's 2.7 ms. On a
three-voter cluster the follower round trip adds to it.

### 6.3 Phase 3: one fsync per entry (designed, not built)

Each entry fsyncs twice: the raft log append (`synchronous = FULL`, needed
for Raft safety) and the state DB commit, which runs at `FULL` in raft mode
only so the applied marker cannot fall behind the log's purge horizon on a
power loss (`Store::configure_connection`). The standard alternative is to
run the state DB at `NORMAL` and, at startup, restore the newest local
snapshot whenever the applied marker is behind the purged log, then replay
the log from there. That removes one fsync from every entry's critical
path. The local profile cannot show its value (macOS `fsync` does not flush
the drive cache), so it waits for a Linux measurement.

## 7. Proto, struct, config, and persistence changes

Phase 1: one migration, indexes only. It creates `idx_instance_keyed_live`
and `idx_allocation_instance_config` and drops `idx_allocation_instance_id`.
No table changes shape, so raft changesets, the mixed-version write guard
(which compares table shapes), and lane snapshots (which carry rows, not
indexes) are unaffected. No proto, config, or job-config change.

Phase 2 would add a capture term to `StoreCommand` (a raft log format
change) and the `raft.pipelineDepth` setting.

## 8. Implementation phases

1. **Phase 1, keyed binding from indexes** (this change). Ships alone.
2. **Phase 2, pipelined lane.** Gated on the allowlist audit and on a
   three-voter measurement. Independent of Phase 3.
3. **Phase 3, one fsync per entry.** Gated on a Linux fsync measurement
   and a crash test that power-cuts a node behind the purge horizon.

## 9. Testing plan

- Phase 1: `keyed_bind_query_uses_the_live_partial_index` asserts the
  query plan uses both indexes and no materialized list, so an edit that
  breaks the partial index's implication fails the build.
  `keyed_binding_skips_parked_instances_and_rebinds_across_configs` pins
  the semantics the rewrite keeps. The existing keyed binding tests,
  `tests/raft_lifecycle_ops.rs` (entry counts), and `tests/integration/raft_seam.rs`
  run unchanged. The `keyed_bind` group in `benchmarks/micro/scheduler_bench.rs`
  measures one keyed placement against a resident population.
- Phase 2: a forced-speculation mode for the test suite, with the
  fail-closed taint check as an assertion, plus leader-kill and
  snapshot-install tests with entries in flight.
- Phase 3: a crash test that kills a node after a purge with unsynced
  state.

## 10. Rollout and backward compatibility

The migration runs at startup. Building the two indexes reads the
`instance` and `allocation` tables once: seconds at 10^6 rows. A manager
on the older schema can still apply every changeset a newer leader writes,
since no table changed shape. The partial index adds one index entry per
live keyed instance and moves it out as the instance parks, which is a
small fixed cost on instance writes.

## 11. Open questions

- **Bind in O(log n) of the live population.** The bind still visits every
  live instance of the task, two index probes each. Making it independent
  of the live count needs either a maintained "unbound for config" marker
  or the planner naming the instance it placed for. Both change more than
  a query. Lean: wait for a measurement where live instances per task, not
  parked ones, dominate.
- **Does the reference host show the same share?** Every number here is
  from a loaded laptop. Lean: the AWS sandbox campaign runs the ladder
  before and after.

## 12. Acceptance criteria

- Phase 1: the binding query plan uses `idx_instance_keyed_live` and a
  covering probe of `idx_allocation_instance_config` (test). The
  `keyed_bind` benchmark's apply time grows much less from 1,000 to 5,000
  resident instances than before. Raft entry counts are unchanged. The
  lifecycle ladder's ceiling rises on the same host.

## 13. Public documentation

`docs/sandboxes-at-scale.md`, "Limits to know about": a keyed placement's
cost grows with the task's live instances, not its parked ones.

## 14. Results

### 14.1 Method

`cargo bench --bench scheduler_bench -- keyed_bind --warm-up-time 1
--measurement-time 3`, two alternating rounds of `origin/main` and this
change, on one shared 10-core laptop (load average 6 to 8 from other
builds), 2026-10-14. The store in the bench is plain SQLite, so `apply` is
one execution of the plan's transaction. Under raft the capture runs that
body and the apply runs its changeset, and the bind sits in the capture.

### 14.2 `keyed_bind`

One new keyed placement against `resident` placed instances and as many
stopped ones (criterion median, then the `apply` phase mean):

| resident | base | this change | base apply | this change apply |
| --- | --- | --- | --- | --- |
| 1,000 | 4.03 / 4.24 ms | 3.55 / 3.64 ms | 1,699 / 1,791 µs | 1,011 / 1,035 µs |
| 5,000 | 10.06 / 10.65 ms | 7.70 / 8.05 ms | 4,392 / 4,725 µs | 1,937 / 2,069 µs |

The apply phase, which is the serial lane time this change targets, drops
by about 40% at 1,000 resident and 55% at 5,000. It still roughly doubles
from 1,000 to 5,000 resident, because the bind still visits every *live*
instance (§11). The rest of the plan (`tasks`, `build`) grows with the
live population too and is untouched here.

The bench's first version planned its fill in `Fresh` mode, which asks for
one placement per live instance and fails once some are bound. It now
plans in `Reschedule` mode, as a create's `job_reschedule` eval does.
