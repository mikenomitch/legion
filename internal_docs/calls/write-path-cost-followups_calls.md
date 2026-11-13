# Judgement calls: Plan, commit and write-path cost follow-ups (2026-11-12)

The judgement calls from working through the "Plan, commit and write-path
cost" group of `TODO.md` that do not belong to one plan. Calls that do are
in their plan's file:

- node rows in the apply walk: [placement-and-commit-cost #2](placement-and-commit-cost-plan_calls.md) (follow-up);
- the keyed bind: [scheduler-lane-entry-cost #2](scheduler-lane-entry-cost-plan_calls.md);
- the `STRING`/`TIMESTAMP` re-type: [jsonb-text-affinity #7–#9](jsonb-text-affinity-plan_calls.md);
- per-lane files, pipelining, one execution under `lanes`: [lane-isolation](lane-isolation-plan_calls.md).

Entries are ranked by project impact, highest first; an entry's number is its
rank.

## 1. Every store write waits for quorum off the async runtime, at one choke point

**Call.** `Store::replicated` waits for its op's reply under
`block_in_place` (`storage::block_off_runtime`) whenever the caller is a
multi-thread tokio worker, in raft mode (the quorum wait) and in
`local_disk` mode (the commit). `run_blocking_admission` now calls the same
helper.

**Alternatives.** (a) Wrap each mutating handler in `run_blocking_admission`
"as they show up", as the TODO item said. (b) Move every handler body to
`spawn_blocking`.

**Trade-off.** Every replicated write from a worker thread pays a
`block_in_place` handoff (a few microseconds, a thread wake when the
worker had queued work), including writes that would have been quick. A
handler that does several writes hands off several times instead of once.

**Why (simplicity, and it closes the class).** (a) leaves the class open:
the next handler written without the wrapper reintroduces the starvation the
AWS rig measured (14–78 ms per eval spent waiting for a worker). There are
~290 handlers and the store has one wait. (b) needs `'static` bodies and
cloned state per handler. A quorum round is milliseconds, so the handoff is
noise against it. Pinned by `a_blocked_caller_does_not_hold_the_only_worker_test`
(one worker thread; the waiter would deadlock without it).

## 2. Job stop/destroy: index every foreign key and every per-job statement, pinned by schema tests, before profiling on the rig

**Call.** The "~4 evals/s on the AWS 10k cleanup" item was answered by two
schema tests and seven indexes, not by a rig profile:
`every_foreign_key_is_indexed_on_its_child_side` (SQLite's
`.lint fkey-indexes` as code) and `job_stop_and_destroy_statements_seek_by_job`
(no `SCAN` in any stop/destroy statement). They found `job.name` (every
by-name job lookup), `task_status.job_id`, and five unindexed foreign keys
under `job` and `job_config`, each scanned once per destroyed job or
deleted config.

**Alternatives.** (a) Profile the rig first and fix what shows. (b) Batch
job stops (one eval for many jobs).

**Trade-off.** The 4/s number itself is not re-measured. The local
`job_stop_drain` cell (release, 10k nodes, stop and destroy 100 jobs) shows
the shape the scans predicted:

| jobs on the cluster | destroy, base | destroy, after | stop + destroy, base | after |
| ---: | ---: | ---: | ---: | ---: |
| 1,500 | 1,751 µs | 884 µs | 2,471 µs | 1,997 µs |
| 4,000 | 3,219 µs | 1,022 µs | 4,519 µs | 1,800 µs |

Before, a destroy grew with the number of jobs; after, it does not. The
rig's raft round trips, eval-loop admission and heartbeat load are not in
it. The Verify
entry stays open. Seven more indexes cost every insert into those tables a
B-tree write.

**Why (performance, and the class again).** A scan per parent delete is
quadratic over a cleanup and invisible at small scale, which matches "fine
in tests, 4/s at 10k". The tests stop the next unindexed foreign key from
landing. (b) changes the eval model for a cleanup path. The tables indexed
are write-light (one row per submit or config), so the insert cost is
negligible against the scans removed.

## 3. Plan-commit cost past 10k: the local profile rules out the in-commit suspects, so the open item is lane sharing, measured on the rig

**Call.** The commit was profiled locally at 5k and 50k nodes
(`plan_fleet_scaling`, release, `local_disk`, fill 5): the apply phase,
which holds the free-pool rebuild and the read-model reprojection, grows
2,102 → 2,332 µs (+11%) for ten times the fleet, against 2.5x on AWS. The
item stays open with that narrowed: what grows on AWS is outside the plan's
own transaction, and the leading suspect is everything else committing
through the same pipeline under `single` (status-lane writes, job intake),
which is what the lane-isolation plan's per-lane files address. The same
profile showed the eval's `dispatch` phase growing 360 → 2,992 µs; that was
the bench, not the manager: with no runtime to spawn on, every dispatch
built a Tokio runtime even with nothing to send. Dispatch now returns
before building anything when it has nothing to push.

**Alternatives.** (a) Tune the coalescer bounds blind. (b) Declare the item
done.

**Trade-off.** No AWS number moves in this change.

**Why (honesty over activity).** The two in-commit suspects can be ruled
out on one host; lane sharing cannot be reproduced without the rig's
heartbeat and intake load. "Batch more plans per commit" and "plan N /
apply N" stay open: plan apply already settles its own eval in the same
entry (`arm_eval_settle`) and claims are fused with completes, so the
eval's own writes are already one entry per plan, and the remaining lever
is how many *plans* share an entry, which only the rig's 8-thread run can
measure.

## 4. The power-cut test runs on loop-mounted ext4 images with writeback off, not dm-flakey or a VM

**Call.** `scripts/power-cut-test.sh` mounts two ext4 images (state DB, raft
log) through loop devices, sets `vm.dirty_writeback_centisecs = 0` and a
one-hour `commit=`, and "cuts power" by copying the images. Two `#[ignore]`
tests drive it; the second is a control without the barrier, which must
lose the marker.

**Alternatives.** (a) dm-flakey, as the TODO suggested. (b) A VM with an
unsynced virtual disk. (c) A SQLite VFS shim that discards unsynced
writes.

**Trade-off.** Linux and root only; it does not model a directory entry
never synced, torn writes, or a disk reordering writes within an fsync.
Memory pressure could still force writeback, which would make the control,
not the real test, fail.

**Why (simplicity, and it needs no kernel module).** The barrier fsyncs with
`std::fs`, not through SQLite, so (c) cannot see it. (a) needs device-mapper,
absent in the build containers. (b) is a rig. The loop setup is what an
unsynced VM block device is, made with stock tools, and the control proves
the harness detects the bug it exists to catch (marker 0 against a purge
of 15 without the barrier; 20 with it).

## 5. `scheduler_bench` selects groups by environment variable

**Call.** `LEGION_BENCH_ONLY=group,group` runs a subset of groups; the
bench's `main` iterates a `GROUPS` table instead of `criterion_main!`.

**Alternative.** Keep relying on Criterion's name filter.

**Trade-off.** One more variable to know; a new group must be added to the
table.

**Why (ease).** Each group builds its fleet before Criterion's filter is
consulted, so a filtered run still paid every group's setup — tens of
minutes, including 20k-node fleets, to measure one cell.
