# The replicated write path: group commit, one execution, one fsync, a byte-sized log tail, replicated data migrations

> **Judgement calls:** [raft-write-path-plan_calls.md](../calls/raft-write-path-plan_calls.md)

**Status: IN EXECUTION** (2026-11-08). Fixes architecture review findings
SCH1, ST1 and ST4 ([review](../architecture-review-2026-11-07.md)).
Phases 1–5 are built in this change. **Deliberately deferred:** a capture
mutex per lane (needs one database file per lane, §6.4), true pipelining of
a lane (capturing entry N+1 while N's quorum wait is in flight, §6.2.4),
speculative execution under the `lanes` topology (§6.3.6), and logical
commands / name-keyed changesets (§6.5.5).
The first three are now planned together in
[lane-isolation-plan.md](lane-isolation-plan.md) (2026-11-12). The power-cut
test of the purge barrier (§6.1) ran on loop-mounted ext4 images
(`scripts/power-cut-test.sh`, 2026-11-12): marker 20 ≥ purge 15 with the
barrier, 0 without it.

## 1. Problem statement

Placement is cheap; committing it is the ceiling. Every replicated write
(`Store::replicated`) runs on one pipeline thread per raft group, and that
thread does, strictly in sequence, for **one** op at a time:

1. take the process-wide `CAPTURE_MUTEX`, run the op inside a savepoint with
   a SQLite session attached, extract the changeset, **roll back**;
2. propose the changeset and block until quorum commit **and** the local
   state machine has applied it — which **runs the write a second time**
   (`apply_strm`) on the leader, under the same mutex;
3. reply.

The leader therefore executes every write twice, holds the global mutex
twice per write, and pays **two fsyncs per entry** on every node: the raft
log append (`synchronous = FULL`, required) and the state DB commit, which
runs at `FULL` in raft mode only so the applied marker can never regress
below the log's purge horizon after a power loss. Ops that queue behind an
in-flight entry wait for the whole cycle; nothing amortizes.

Two neighbouring problems in the same code:

- **ST4.** The log tail kept behind a snapshot is `snapshotEntries / 2`
  clamped to 1,000 entries. At 10× today's write rate that is seconds of
  history, so every follower restart becomes a full snapshot install.
- **ST1.** Data migrations (`UPDATE job SET updated_at = created_at`, the
  app uid backfill, the scaling-policy cleanup) run locally on each node at
  startup. Replicas that migrate at different applied indexes backfill
  different rows — silent divergence the content hash would flag only if
  someone ran it.

## 2. Prior art

- **etcd/raft** batches proposals: everything queued while the previous
  `Ready` is being persisted goes out in the next one, so a burst shares one
  log fsync and one round trip. We take the batching; we do not take etcd's
  MVCC backend.
- **Nomad's plan applier** overlaps the commit of plan N with evaluating
  plan N+1 against an optimistic snapshot that includes N. We do *not* take
  the overlap in this change (§6.2.4).
- **CockroachDB** evaluates a write once on the leaseholder and replicates
  the resulting effects ("proposer-evaluated KV"), applying them below raft
  without re-evaluation. Our changeset replication is already
  proposer-evaluated; the leader's second execution is the part we remove.
- **rqlite / dqlite** keep SQLite at `synchronous = NORMAL` and lean on the
  raft log for durability, snapshotting before truncation. We keep the
  same split, with an explicit fsync barrier before any log purge.
- **etcd** retains its log by count (`--snapshot-count`), **Consul/Nomad**
  by `raft_trailing_logs` (count). We retain by bytes because Legion's
  entries vary from hundreds of bytes to megabytes, so a count says nothing
  about how long a tail lasts or how much disk it takes.
- **Kubernetes** never migrates stored objects in place behind the API: it
  converts on read and runs storage-version migration as ordinary API
  writes. That is the shape we take for data migrations: they are ordinary
  replicated writes, run once, by the leader.

## 3. Jobs to be done, by persona

**Cluster operator**
- When I scale a cluster up and submit work in bursts, I want the managers
  to commit as fast as the disks and network allow, so the control plane is
  not the reason placements queue.
- When I restart a manager for maintenance, I want it to catch up from the
  other managers' logs in seconds, not by copying the whole database, so a
  rolling restart does not stall everyone else's writes.
- When I upgrade the managers one at a time, I want every manager to end up
  with exactly the same data, so a later failover does not surface rows that
  differ from node to node.
- When the power goes out, I want every manager to come back, so I never
  have to rebuild one from a backup.

**On-call responder**
- When writes are slow, I want to see whether they are waiting on the disk,
  the network, or each other, so I know which to fix.

*Not in scope:* job authors and agents. Nothing they type changes; they get
lower write latency under load.

### User scenarios

1. A burst of 2,000 job submissions arrives. Instead of 2,000 sequential
   capture → quorum → apply cycles, the control lane commits them in a few
   dozen entries, each one log fsync and one quorum round trip.
2. A follower is restarted after ten minutes. Its log position is still
   inside the retained tail (sized in bytes, default 256 MiB per group), so
   the leader replays entries to it instead of shipping a snapshot.
3. A three-manager cluster is upgraded to a build whose migration backfills
   a new column. Each manager adds the column at startup; once the last one
   advertises the new schema, the leader runs the backfill as one raft
   entry, and every manager applies the same rows at the same log index.

## 4. Goals and non-goals

**Goals**
- One state-DB fsync per log purge instead of one per entry, without ever
  letting the applied marker fall below the purge horizon.
- Group commit: ops queued behind an in-flight entry share the next entry's
  capture, quorum round trip, log fsync and apply transaction, with per-op
  failure isolation.
- The leader executes each write once.
- Log retention by bytes, observable as a time span.
- Data migrations as replicated, exactly-once, cluster-wide entries.

**Non-goals**
- Per-lane database files (§6.4) — recorded as a design sketch and a TODO.
- Overlapping capture N+1 with N's quorum wait (§6.2.4).
- Changing what a changeset is (logical commands, column-name binding) —
  §6.5.5.
- Any compatibility path for old logs, snapshots or state DBs. There is no
  installed base (AGENTS.md); the log format changes.

## 5. Background — what exists today

- `crates/manager/src/storage/raft/driver.rs`: `pipeline_loop` (one OS thread per group)
  receives one `PipelineJob` and calls `execute_replicated`, which checks
  leadership, runs `ensure_linearizable` once per term (R1), calls
  `capture::capture_mutation`, runs the ownership guard
  (`ownership::verify_changeset_ownership`) and the mixed-version guard
  (`check_members_can_apply`, R2), then `block_on(raft.client_write(..))`.
- `crates/manager/src/storage/raft/capture.rs`: `CAPTURE_MUTEX` serializes every capture
  and every apply on the one write connection; `capture_mutation` always
  rolls back; `apply_changeset_with` applies under the mutex with
  `foreign_keys = OFF` and projects side effects after commit.
- `crates/manager/src/storage/raft/state_machine.rs`: `apply` writes the changeset and the
  `raft_applied` marker in one savepoint; snapshot build is lock-free on its
  own read-only connection (`VACUUM INTO` for `single`, a lane-table dump
  for `lanes`); install holds the capture mutex.
- `crates/manager/src/storage/raft/log_store.rs`: one `log.sqlite` per group at
  `synchronous = FULL`; `purge` records the marker then deletes.
- `crates/manager/src/storage/store.rs` `configure_connection`: the state DB runs `FULL`
  in raft mode (`durable_sync`). openraft 0.9's `get_initial_state` re-applies
  `(last_applied, committed]` from the log, which fails permanently if the
  applied marker regressed below `last_purged_log_id`.
- `driver.rs` `start_engine`: `max_in_snapshot_log_to_keep =
  (snapshotEntries / 2).clamp(1, 1000)`.
- `crates/manager/src/storage/migrations.rs`: one `Vec<M>` of schema steps and data steps,
  run with `rusqlite_migration` on every node at `Store::start_storage`.

## 6. Proposed design

### 6.1 One state-DB fsync per purge (SCH1 part 1)

The state DB runs `synchronous = NORMAL` in every mode. The raft log stays
at `FULL`. The invariant that `FULL` protected — *the durable applied
marker is never below the log's purge horizon* — is kept by a **purge
barrier**: `RaftLogStore::purge` first calls a hook the driver installs,
which fsyncs the state DB's WAL, main file and directory
(`Store::sync_state_db_to_disk`). Every purge, from any source (the
retention loop, openraft's own post-install purge, the bootstrap seed purge,
startup clean-up), passes through it. A purge to index *i* only happens once
the state machine has *committed* a marker ≥ *i* (a purge never passes the
snapshot, and a snapshot is read from committed state), so after the
barrier that marker is durable. Cost: three fsyncs per purge, not one per
entry. A barrier failure fails the purge, and the log is kept.

WAL-mode facts this relies on: a commit at `NORMAL` has written every frame
to the WAL file before it returns, so an `fsync` of the WAL inode after it
makes it durable; a checkpoint only resets the WAL after it has backfilled
and synced the main file. The directory fsync covers a freshly created WAL.

### 6.2 Group commit (SCH1 part 2)

The pipeline thread drains every job queued for its group, up to
`GROUP_COMMIT_MAX_OPS` (64) ops, and stops adding ops once the batch's
changesets reach `GROUP_COMMIT_MAX_BYTES` (128 KiB). With 32 entries per
append-entries RPC, that keeps a worst-case replication batch near the
few MiB the 250 ms append-entries deadline was calibrated for. Ops left
over head the next batch.

The batch runs in **one capture** (`capture::capture_batch`):

```text
SAVEPOINT legion_capture
  for each op:
    session_k = new session (all tables)
    SAVEPOINT legion_op
      run op_k                               # panics caught per op
      cs_k = session_k.changeset()
      ownership guard(op_k.lane, cs_k)       # per op
      mixed-version guard(cs_k)              # per op
    RELEASE legion_op   |  ROLLBACK TO legion_op (error/panic/guard)
entry changeset = changegroup(cs_1 .. cs_n)  # net effect, applied in order
```

Each op's changeset is relative to the state after the ops before it, so the
changegroup is exactly the batch's net effect. A failing op rolls back only
itself; its caller gets the error, the others commit. A lone op behaves as
before. The entry carries one combined changeset (not a list): apply, schema
check and side-effect projection stay "one entry = one changeset", the
contract the plan-apply coalescer already relies on (calls #3).

Followers apply the combined changeset in one transaction. Every caller in
the batch is answered after the one quorum commit.

#### 6.2.4 Not in this change: overlapping capture with the quorum wait

Capturing N+1 while N waits for quorum needs N's effects visible to N+1's
capture without being committed — which is what §6.3 provides on the write
connection — but the read model, the event stream and the worker stream are
projected at apply time by re-reading committed rows. Projecting N while
N+1's speculative rows are already in the database would push N+1's
uncommitted rows into the read model and the worker stream. Batching gets
the amortization without that hazard; overlap stays a TODO (calls #5).

### 6.3 The leader executes once (SCH1 part 3)

Under the `single` topology (the default), the batch is **committed
locally** instead of rolled back, together with a node-local undo record:

```text
... ops as in 6.2 ...
INSERT INTO raft_speculative (lane, seq, undo) VALUES (?, ?, invert(entry changeset))
RELEASE legion_capture                       # commits batch + undo record
propose StoreCommand { changeset, proposal: { node: self, seq } }
```

`raft_speculative` is `ownership::Owner::Internal`: never in a changeset,
never applied from one, never in a lane dump, excluded from the content
hash. The pipeline and the state machine share a `SpeculationLedger`: the
outstanding `seq`s in proposal order.

**Apply.** For an entry whose `proposal` is `{this node, oldest outstanding
seq}`, the state machine skips the changeset apply and, in one transaction,
writes the applied marker and deletes the undo record; it still projects
side effects from the entry's changeset. Any other entry while speculation
is outstanding (another leader's entry, a blank entry of a new term, a
membership entry, an entry this node proposed in an earlier incarnation)
first **undoes every outstanding batch, newest first** (inverse changesets,
`foreign_keys = OFF`, conflict = abort), rebuilds the node-local caches, and
then applies normally. That covers leadership loss with an entry in flight
and log truncation: in raft a new term always begins with a blank entry,
and a leader's own log is append-only within its term, so its proposals
arrive in proposal order or not at all.

**A failed proposal undoes its batch at once** (`undo_outstanding`, under
the capture mutex): a deposed leader must not serve uncommitted rows for as
long as a partition lasts. That is safe even if the entry commits later
under another leader — the ledger no longer knows it, so the state machine
applies it normally onto the state without it — because this node captures
again only as leader of a later term, after `ensure_linearizable` has
applied everything before that term's blank entry.

**Before capturing**, the pipeline requires the ledger to be empty (it
always is after a successful `client_write`, which resolves only after the
apply). After a failed proposal it forces `ensure_linearizable` again (a
new term's blank entry undoes what a leadership change left). If a batch
is still outstanding after that — its proposal never reached the log — the
pipeline proposes an empty `speculation_flush` entry: anything this leader
appended earlier applies first and matches, and the empty entry undoes the
rest. It never stacks new writes on rows that may vanish. It does not undo
such a batch on its own while it may still be leader: if the entry was
appended but not yet committed, the next capture would be taken against
state without it, and that capture's entry would then apply after it.

**Startup.** `Store::start_storage` undoes every leftover undo record
(newest first per lane) before openraft reads `applied_state`, so a crash
between the speculative commit and the apply leaves exactly the committed
state; if the entry did commit, openraft re-applies it from the log
normally.

**Copies of the database never carry speculative rows.** Snapshot build
(`VACUUM INTO`) and `state backup` copy the database and then apply the
image's own undo records to the copy (`speculation::scrub_image`); the undo
record and the rows it reverts always commit together, so the image is
exactly committed state. Lane dumps (`lanes`) apply the undo records of
their own tables to the dump. Snapshot install clears the lane's
speculation (whole-DB install replaces the file; a lane install undoes the
lane's batches inside its transaction).

**Dirty-read window.** Between the speculative commit and the apply —
the quorum round trip — a direct SQL read on the leader (the read pool)
can see the batch. The scheduling read model, the event stream and the
worker stream do not: they are projected at apply, as before. A write
cannot see it either: the pipeline captures only when nothing is
outstanding. Documented in `docs/multi-raft.md`.

#### 6.3.6 Not under `lanes`

Under `lanes`, three pipelines share one database, so lane C's capture
could read lane S's in-flight speculative rows and commit a decision based
on rows that a leadership loss later undoes. That is a new cross-lane
anomaly, not a stale read. `lanes` keeps capture → rollback → apply until
per-lane database files (§6.4) give each lane its own speculative state.

### 6.4 One capture mutex for every lane (SCH1 part 4) — deferred

One SQLite file has one writer at a time, and a capture holds a savepoint
open across many statements, so a per-lane mutex over one shared write
connection is unsound. 6.2 and 6.3 shrink what the mutex covers (one capture
per batch; on the leader, the apply is a marker write). Splitting it for
real means **one database file per lane**: each lane's owned tables move to
`<store>/lane-<name>.sqlite`, attached to every connection (`ATTACH`), each
lane's pipeline and state machine write only through their own file's
connection, so lanes get independent WAL write locks. Commutative tables
need a home (one per lane, unioned by a view), cross-lane reads become
cross-file reads (consistent per file, not across files), snapshots become
per-file images (simpler than today's dumps), and backup becomes a set of
files. That is a storage-layout change touching every query that names a
table; it is recorded as a TODO with this sketch (calls #6).

### 6.5 Replicated data migrations (ST1)

**Schema steps vs data steps.** `migrations.rs` keeps only schema steps:
DDL, deterministic table rebuilds (`INSERT INTO new SELECT * FROM old`) and
constant seed rows. They run locally on every node at startup, as now. A
test (`schema_migrations_carry_no_data_steps`) fails the build if an `up`
step contains `UPDATE … SET` or `DELETE FROM`.

**Data steps** live in `crates/manager/src/storage/data_migrations.rs`:

```rust
pub(crate) const DATA_MIGRATIONS: &[DataMigration] = &[
    DataMigration { id: "2026-09-job-updated-at-backfill", run: backfill_job_updated_at },
    DataMigration { id: "2026-10-app-uid-backfill",        run: backfill_app_uids },
    DataMigration { id: "2026-10-scaling-policy-cleanup",  run: drop_scaling_policy_state },
];
```

and run as **one control-lane replicated op** (`run_data_migrations`) that
runs every pending step and records each in the replicated
`data_migration(id, applied_at)` table, in one entry — one log index for the
whole barrier. Pending is read inside the capture, so a step runs exactly
once cluster-wide even across failovers.

- **Raft:** a driver task on every manager retries every 2 s while any step
  is pending; it acts only on the control-lane leader, and only when every
  voter advertises at least this build's schema version
  (`voters_missing_schema_version`). It stops once nothing is pending.
- **`local_disk`:** `start_storage` runs pending steps inline.
- **A brand-new database** records every step as done when it is created:
  it has no rows to migrate, and every replica descends from the bootstrap
  node's database through its seed snapshot (calls #10).

The existing steps were rewritten to be correct when run late, after the
new build has written rows of its own (calls #11): the app uid backfill
assigns the lowest free uid, and the scaling cleanup keeps `object:`-keyed
actions.

`StoreCommand` also carries the `schema_version` it was captured under, for
diagnostics (the apply-side shape check stays the authority).

#### 6.5.5 Not in this change: logical commands

Changesets bind columns by position, so every migration is a log-format
change and table rebuilds must keep column order. The alternatives —
logical commands (Nomad's FSM messages), or rows encoded by column name —
are recorded as a deferred call (calls #8) with a TODO.

### 6.6 Log retention by bytes (ST4)

openraft's own purge is disabled (`max_in_snapshot_log_to_keep = u64::MAX`)
and a driver task per group purges instead: after each new snapshot it walks
the log newest first and purges everything behind the newest
`raft.logRetainBytes` bytes (default 256 MiB), capped at the snapshot
index, via `raft.trigger().purge_log(upto)` (so the purge barrier of 6.1
applies). Entries not yet in a snapshot are never purged. The scan runs on
its own read-only connection to the log, on the blocking pool. The log records each
entry's append time, and `legion_raft_log_retained_seconds{lane}` reports
how far back the retained log reaches — how long a follower can be down and
still catch up by replay.

## 7. Proto, struct, config, and persistence changes

- **Config:** `raft.logRetainBytes` (u64, default 268435456). Documented in
  `crates/common/src/structs/config.rs` and `docs/raft-operations.md`.
- **Log format (breaking):** `StoreCommand { lane, ops: Vec<String>,
  changeset, proposal: Option<Proposal>, schema_version }`; the log table
  gains `appended_ms`.
- **State DB (schema steps):** `raft_speculative(lane, seq, undo)` (Internal)
  and `data_migration(id, applied_at)` (control lane).
- **Metrics:** `legion_raft_entry_ops{lane}` (ops per entry),
  `legion_raft_log_retained_seconds{lane}`,
  `legion_raft_state_syncs_total{lane,outcome}`,
  `legion_raft_speculation_total{lane,outcome}` (`committed`, `matched`,
  `undone`).
- No proto changes.

## 8. Implementation phases

1. **Purge barrier + `NORMAL`.** Ships alone.
2. **Byte-sized log retention.** Ships alone.
3. **Group commit.** Ships alone; lone ops unchanged.
4. **Execute once (speculative leader commit), `single` only.** Builds on 3.
5. **Replicated data migrations.** Ships alone.

## 9. Testing plan

- 1: `purge_runs_the_barrier_before_deleting`, `purge_refuses_when_the_barrier_fails`
  (log store); the state DB runs `NORMAL` in raft mode; a raft node that
  snapshots and purges counts barrier syncs.
- 2: retention purge point by bytes (log store unit); a single-node raft
  store with a small byte budget keeps a tail and reports its span.
- 3: batch capture unit tests: per-op isolation (error, panic, ownership
  violation), combined changeset equals sequential effects, byte cap leaves
  the remainder; a concurrent burst on a raft store commits in fewer entries
  than ops (`raft_perf_smoke` and a unit/integration check).
- 4: adversarial: leadership loss with an entry in flight (non-matching
  entry undoes), truncation (a different leader's entry at the same index),
  crash between speculative commit and apply (reopen with a leftover undo
  record), snapshot and backup taken while speculative (image has no
  speculative rows), lane install with outstanding speculation.
- 5: data step runs once and records; a re-run is a no-op; raft single node
  records through the log; the schema-step lint.
- Existing: `storage::raft`, `storage::store`, integration `raft_*`,
  `eval_regen_failover::`, `spread_under_raft::`, `raft_job_eval_ops`,
  `raft_lifecycle_ops`.

## 10. Rollout and backward compatibility

No installed base: the log format, the state schema and the `durable_sync`
behavior change without a compatibility path. **What breaks:** a raft data
dir or state DB from a previous build cannot be resumed (start fresh, or
`state backup` → restore into a fresh bootstrap); mixed-version manager
planes across this change are not supported. `local_disk` stores migrate in
place.

## 11. Open questions

- Should group-commit bounds be operator knobs? Lean no until a measurement
  says the constants are wrong (calls #4).

## 12. Acceptance criteria

1. Raft-mode state DB reports `synchronous = 1`; every purge is preceded by
   a barrier sync (counter); no barrier, no purge.
2. With a byte budget, a snapshot leaves the newest ≤ budget bytes of log;
   the retained-seconds gauge is non-zero on a busy group.
3. A concurrent burst commits in fewer entries than ops; per-op errors
   isolate; `legion_raft_entry_ops` shows the batch sizes.
4. On a `single` leader, a committed write is not applied twice (the
   apply-skip counter moves; `apply_strm` is not called); every adversarial
   test in §9 passes.
5. A data step's effect lands on every replica at one log index and runs
   once.

## 13. Results (2026-11-08)

`tests/integration/raft_perf_smoke.rs`, single-node `single` topology, debug
build, on a shared 4-vCPU VM whose other cores were compiling (load
average ~4) — so ranges, not points. Before is the base commit (one run);
after is three runs.

| Workload | Before | After |
| --- | --- | --- |
| Sequential `put_kv`, p50 | 3.74 ms | 2.90–3.93 ms |
| Sequential, throughput | 258 ops/s | 185–268 ops/s |
| Burst (8 × 100), p50 / p99 | 27.2 / 36.2 ms | 3.4–11.8 / 15.4–27.8 ms |
| Burst, throughput | 289 ops/s | 666–1570 ops/s |
| Burst, ops per entry | 1 | 4.5–5.7 |

`raft_write_path::concurrent_writes_share_entries_*`: 320 concurrent writes
in 51 entries (`single`) and 54 (`lanes`). The lone sequential writer is
unchanged within noise, as expected (§6.2.4: no overlap). Not measured: a
three-voter cluster on real disks, where the removed state-DB fsync per
entry and the shared quorum round should show more (TODO).
