# Plan: Lane Isolation — Projection From the Changeset, a Pipelined Lane, and a Database File per Lane

> **Judgement calls:** [`calls/lane-isolation-plan_calls.md`](../calls/lane-isolation-plan_calls.md)

**Status: PLANNED** (written 2026-11-12). Nothing here is built. It is the
plan three TODO items in "Plan, commit and write-path cost" asked for, because
they are one piece of work in three phases rather than three independent
fixes:

- *Pipeline the scheduler lane* — blocked on projecting the read model from
  the changeset's own values ([raft-write-path §6.2.4](raft-write-path-plan.md),
  [scheduler-lane-entry-cost §6.2](scheduler-lane-entry-cost-plan.md)).
- *A database file per lane* — "needs its own plan"
  ([raft-write-path §6.4](raft-write-path-plan.md), calls #6 there).
- *The leader still executes twice under `lanes`* — unblocked by per-lane
  files ([raft-write-path §6.3.6](raft-write-path-plan.md)).

Deliberately deferred even here: logical commands instead of row changesets
(raft-write-path §6.5.5), and anything that changes what a lane owns.

## 1. Problem statement

Every replicated write goes through one pipeline thread per raft group, and
on each node through one process-wide capture mutex and one SQLite write
connection. Three costs follow, each measured:

1. **A lane commits one entry at a time.** The pipeline captures entry N,
   proposes it, waits for quorum *and* the local apply, then captures N+1.
   Keyed sandbox lifecycles are bound at ~50/s on the 4-vCPU reference host
   against a ~125/s target; the log fsync, replication and raft task hops
   are 1–2 ms of the 2.7 ms an entry costs on one voter
   ([scheduler-lane-entry-cost §6.2](scheduler-lane-entry-cost-plan.md)).
   Overlap is unsafe today because the apply re-reads committed rows to
   project the read model, the event stream and the worker stream: with
   N+1's speculative rows already in the database, N's projection would
   publish them.
2. **Lanes do not run in parallel on a node.** Under `lanes` the three
   groups have independent logs and leaders, but every capture and every
   apply takes the same `CAPTURE_MUTEX` and the same write connection, so
   the scheduler lane's plan applies queue behind status-lane heartbeat
   checkpoints and control-lane job submits. A per-lane mutex over one
   connection is unsound: a capture holds a savepoint open across many
   statements, and another lane's statements would land inside it.
3. **The leader executes every write twice under `lanes`.** `single`
   commits the batch locally with an undo record and skips the re-apply
   ([raft-write-path §6.3](raft-write-path-plan.md)); `lanes` cannot,
   because lane C's capture would read lane S's speculative rows and commit
   a decision that a leadership change undoes.

## 2. Prior art

- **Nomad / Consul.** The FSM applies to `go-memdb`, an immutable radix-tree
  store; readers take a snapshot, and the plan applier evaluates plan N+1
  against an *optimistic snapshot* that already contains N's result while N
  is still being replicated, discarding it if N fails
  ([plan_apply.go](https://github.com/hashicorp/nomad/blob/main/nomad/plan_apply.go)).
  Projection is free there because the state store *is* the in-memory model.
  What we take: overlap N+1 with N, verify on apply, discard on failure.
  What we reject: a second in-memory source of truth beside SQLite.
- **Kubernetes / etcd.** One bbolt file, one writer, MVCC revisions; watch
  events are generated from the applied operation itself, not by re-reading
  keys. What we take: project from what the entry says, not from a re-read.
- **CockroachDB / TiKV.** One Raft group per range, many groups per store
  sharing one RocksDB/Pebble engine with per-range key prefixes and a
  write batch per apply. What we take: independent groups commit
  independent batches; what we note: they get parallelism from an LSM with
  concurrent writers, which SQLite does not have, so for us the unit of
  write parallelism has to be a file.
- **SQLite itself.** One writer per database file; `ATTACH` makes several
  files one connection's namespace; a transaction spanning attached WAL
  databases is atomic per file, not across files.

## 3. Jobs to be done, by persona

**Cluster operator**
- When a burst of sandbox creates lands, I want the scheduler to keep up
  with it on the hardware I already run, so I do not size managers for the
  peak of one queue.
- When I run three lanes on a large fleet, I want heartbeats and job
  submits not to slow down placement, so splitting the log actually buys
  the throughput it promised.
- When I back up or restore a manager, I want one command and one artifact
  regardless of how the store is laid out on disk, so the runbook does not
  change.

**Job author / app developer**
- When I create sandboxes in a loop, I want each create acknowledged as fast
  as a single write can be, so my client does not need its own batching.

**On-call responder**
- When the write path is the bottleneck, I want a metric that says which
  lane and which stage (capture, quorum, apply), so I know what to scale.

*Not in scope:* the security/platform-admin persona (no boundary moves) and
the autonomous agent persona (no new surface beyond what operators get).

### User scenarios

1. A 4-vCPU single-manager cluster runs 10,000 keyed sandboxes that suspend
   and resume all day. Lifecycles go from ~50/s to the ~125/s target with
   `raft.pipelineDepth = 2`, and the same cluster at depth 1 behaves exactly
   as today.
2. A three-manager `lanes` cluster with 20k nodes: the scheduler lane's
   p99 commit no longer moves when heartbeat checkpoints spike, because the
   status lane commits to its own file under its own lock.
3. `legion state backup` produces one artifact whether the store is one
   file or four, and `--restore-from` takes it back.

## 4. Goals and non-goals

Goals:
- The apply-time projection reads nothing but the changeset and the read
  model it is patching (phase 1).
- A lane can have up to `raft.pipelineDepth` entries in flight, with the
  default unchanged (phase 2).
- Each lane's owned tables live in their own SQLite file with their own WAL
  lock, capture mutex and write connection (phase 3).
- The leader executes once under `lanes` too (phase 4).

Non-goals:
- Changing which lane owns which table (`raft/ownership.rs` stays the
  authority; this plan only gives each owner a file).
- Cross-lane transactions. They do not exist today (the ownership guard
  rejects them) and a per-file layout makes them impossible by construction.
- Logical commands (re-executing ops on followers instead of shipping rows).
- A memdb-style in-memory state store.

## 5. Background — what exists today

- `Store::replicated` (`crates/manager/src/storage/store.rs`) hands an op to
  its group's pipeline thread (`raft/driver.rs`, `pipeline_loop`), which
  group-commits up to 64 ops / 128 KiB per entry
  (`capture::capture_batch`), proposes, and waits for the apply.
- `capture.rs` holds `CAPTURE_MUTEX`; the write connection is one
  `Mutex<Connection>` (`Store::write_connection`).
- `Store::project_changeset` walks the changeset and *collects keys*
  (node ids, allocation ids, task config ids, eval wakes, event ids), then
  re-reads the rows by key from the committed database to patch the read
  model, the work log and the event broadcast. UPDATE records carry only the
  changed columns (SQLite sessions omit unchanged non-PK columns), which is
  why it re-reads. The allocation path already uses a delta
  (`AllocationDelta`) where it can name rows exactly.
- Ownership: `raft/ownership.rs` classifies every table as one lane's, as
  `Commutative` (`scheduler_event`, `idempotency_record`; any lane inserts),
  or `Internal` (`raft_applied`, `raft_cluster`, `raft_speculative`).
- Snapshots: `single` takes a whole-DB `VACUUM INTO` image; `lanes` dumps the
  lane's tables (`SnapshotScope::LaneTables`). Backups copy the file and
  scrub speculative rows (`speculation::scrub_image`).

## 6. Proposed design

### 6.1 Phase 1: project from the changeset

The projection becomes a pure function of `(changeset, read model before)`:

- **INSERT** records carry every column: build the row from them.
- **DELETE** records carry every old column: remove by key.
- **UPDATE** records carry the PK and the changed columns' old and new
  values. The unchanged columns come from the read model's current copy of
  the row, which is exactly the pre-entry state because entries are
  projected in log order. For tables the read model does not mirror (most
  of them), the projection only ever needed keys — eval wakes, work-log
  marks, event ids — and the changeset has those.
- Event broadcast decodes `scheduler_event` inserts from the record instead
  of `SELECT … WHERE id IN (…)`.

A read-model miss on an UPDATE (the row is not mirrored, or the mirror is
known stale) falls back to today's re-read, counted by
`legion_store_projection_rereads_total{table}` so a regression is visible.
The shadow checker (`read_model_check.rs`) already compares the projected
model with the database and stays on in tests.

**Ships alone:** removes the per-apply re-reads (most of the projection's
cost on followers, where it is pure overhead), and is the precondition for
6.2.

### 6.2 Phase 2: a pipelined lane

As sketched in scheduler-lane-entry-cost §6.2, now unblocked:

- The pipeline may capture entry N+1 while N is in flight, up to
  `raft.pipelineDepth` (default 1, today's behavior). Under `single` the
  leader already commits each batch speculatively with an undo record, so
  N+1's capture sees N's rows on the write connection.
- Each entry carries its capture term; the state machine skips an entry
  whose log term differs (no rows, marker advances, proposer gets a
  retryable error). Raft's log matching then guarantees a successor never
  applies on top of a skipped predecessor.
- The projection of N reads only N's changeset (6.1), so N+1's speculative
  rows never reach the read model, the event stream or the worker stream
  before N+1 applies.
- Ops that read through the read pool or the in-memory model inside their
  body would read state without the in-flight entries. Speculation is
  therefore limited to an audited allowlist (plan applies, the keyed
  instance group commit, eval enqueue/settle); any other op drains the
  pipeline first. A capture that touches the read pool while speculating
  fails closed (a debug assertion in tests, a drain-and-retry in release).

```nickel
# manager config (configs/manager-raft.ncl)
persistence = {
  type = "raft",
  raft = { topology = "single", pipelineDepth = 2 },   # default 1
},
```

**Ships alone:** the throughput win for `single`, which is the default
topology and the one the sandbox target is measured on.

### 6.3 Phase 3: a database file per lane

```text
<store>/store.sqlite            # control lane's tables, raft_* bookkeeping
<store>/lane-status.sqlite      # status lane's tables
<store>/lane-scheduler.sqlite   # scheduler lane's tables
```

- Under `single` nothing changes: one file, one lock. The layout is a
  property of the `lanes` topology (`persistence.raft.topology =
  "lanes"`), chosen when the store is created; there is no in-place conversion
  (AGENTS.md: no installed base to protect, and a fresh cluster picks its
  topology).
- Every connection `ATTACH`es the lane files under their lane names. Every
  query that names a table resolves through SQLite's schema search order
  (`main`, then attached databases in order), so table names stay
  unqualified and the store's statements do not change. A test pins that
  no table name exists in two files.
- Each lane has its own write connection, capture mutex and pipeline. A
  lane's capture opens its savepoint on its own connection; the ownership
  guard already proves its ops only write its own tables, so every write
  lands in the file that connection holds the WAL lock for.
- **Commutative tables** get one copy per lane file, and readers see their
  union through a `TEMP VIEW` per connection. Inserts go to the writing
  lane's copy; ids already carry the lane's residue (`Lane::ID_STRIDE`), so
  the union has no duplicates.
- **Cross-lane reads** (a scheduler-lane plan apply reading control-lane
  `job_config`) read another file. Within one statement SQLite reads each
  attached WAL file at its own snapshot, consistent per file, not across
  files. That is what `lanes` already promises (independent logs apply in
  different orders on different replicas); what changes is that it is now
  visible on one node as well. The plan apply's fences (epochs, the node
  row check) are all on scheduler-lane tables or re-validated inside the
  apply, so no fence relies on cross-file atomicity; the audit is part of
  the phase.
- **Snapshots** become per-file images (`VACUUM INTO` of one attached
  database), replacing the lane-table dumps. **Backup** becomes a tar of the
  files plus a manifest naming the topology and each file's applied
  marker; `legion state backup` and `--restore-from` keep their surface.
- `raft_applied` moves into each lane's file, so a lane's rows and its
  applied marker still commit in one transaction (the property the state
  machine relies on).

**Ships alone:** lanes stop serializing on one lock; the scheduler lane's
commit time stops depending on heartbeat volume on other lanes.

### 6.4 Phase 4: one execution under `lanes`

With per-file speculation (each lane's undo records in its own file), lane
C's capture can no longer read lane S's speculative rows, because a capture
reads other lanes' files only through the read pool — committed state
only. The `single`-only restriction in raft-write-path §6.3.6 is lifted.

**Ships alone:** halves the leader's apply cost under `lanes`.

## 7. Proto, struct, config, and persistence changes

- Config: `persistence.raft.pipelineDepth` (integer ≥ 1, default 1) — manager
  config, not job config, so nothing goes in `configs/jobs/maximal.ncl`.
- On disk (phase 3, `lanes` only): `lane-<name>.sqlite` beside
  `store.sqlite`; snapshot images per lane file; backup archive with a
  manifest. No proto change: `StoreCommand` gains a `capture_term` field
  (phase 2), which is a raft log format change between managers of
  different builds — covered by the existing mixed-version guard.
- Metrics: `legion_store_projection_rereads_total{table}` (phase 1),
  `legion_raft_pipeline_in_flight{lane}` (phase 2).

## 8. Implementation phases

| Phase | Ships | Depends on |
| --- | --- | --- |
| 1. Projection from the changeset | Cheaper applies on every replica | — |
| 2. Pipelined lane (`pipelineDepth`) | Throughput under `single` | 1 |
| 3. Database file per lane | Lanes commit in parallel on a node | — |
| 4. One execution under `lanes` | Half the leader's apply under `lanes` | 3 |

Phases 1 and 3 are independent and can be built in either order.

## 9. Testing plan

- Phase 1: the shadow read-model checker on every store test; a property
  test that projects random changesets both ways (re-read vs. changeset)
  and compares the models; `legion_store_projection_rereads_total` is zero
  across the scheduler suite.
- Phase 2: the speculation tests in `state_machine.rs` extended to depth 2
  (leadership loss with two entries in flight, a skipped predecessor, a
  failed proposal); the `keyed_bind` and sandbox-lifecycle benches at depth
  1 and 2, before/after; the allowlist enforced by a test that captures
  every op type with the read pool instrumented.
- Phase 3: every existing raft test under `lanes` with per-file layout; a
  test that no table exists in two files; backup/restore round trip; the
  three-lane chaos suite in `tests/integration/` with heartbeats saturating
  the status lane while the scheduler bench runs.
- Phase 4: the existing `single` speculation suite run under `lanes`.
- The power-cut harness (`scripts/power-cut-test.sh`) extended to several
  files: after a cut, each lane's marker is at or above its own log's purge.

## 10. Rollout and backward compatibility

- Phase 1: no surface; on by default.
- Phase 2: `pipelineDepth = 1` default until the multi-voter measurement
  says otherwise; raising it is a manager config change and a rolling
  restart.
- Phase 3: only for clusters created with `topology = "lanes"` after it
  ships; an existing `lanes` store is not converted (no installed base —
  the PR says so).
- Phase 4: automatic under `lanes` once phase 3's layout is in use.

## 11. Open questions

- **Does phase 2 pay on three voters?** The follower round trip adds to the
  overlappable share; lean: yes, but measure before changing the default.
- **Cross-file read consistency in the plan apply.** Lean: no fence depends
  on it; the phase-3 audit has to show that table by table.
- **Do commutative tables want their own file instead of one copy per
  lane?** Lean: one per lane, because a shared file would be a shared write
  lock again.

## 12. Acceptance criteria

- Phase 1: no `SELECT` in `project_changeset` on the hot tables (node,
  node_health, allocation, task_config, scheduler_event, eval); shadow
  checker clean; follower apply time down on the AWS sandbox ladder.
- Phase 2: keyed lifecycles ≥ 100/s on the reference host at depth 2; depth
  1 within noise of today.
- Phase 3: scheduler-lane p99 commit flat while status-lane write rate
  doubles; backup/restore round trip passes.
- Phase 4: one execution per write on a `lanes` leader (an apply-count
  metric, as `single` has).
