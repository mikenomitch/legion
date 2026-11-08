# Judgement calls: the replicated write path

Calls made while building [raft-write-path-plan.md](../plans/raft-write-path-plan.md)
(review findings SCH1, ST1, ST4). **Ordered by project impact, highest
first; an entry's number is its rank.**

## 1. The leader commits its batch before the quorum round, with an undo record — under `single` only

**The call.** Under the `single` topology (the default) the pipeline commits
each captured batch to the state DB *before* proposing it, together with a
node-local undo record (the batch's inverse changeset) in `raft_speculative`,
and tags the entry with `Proposal { node, seq }`. The state machine
recognizes its own oldest outstanding batch and only advances the applied
marker; any other entry undoes every outstanding batch (newest first) and
applies normally. Under `lanes` the pipeline keeps capture → roll back →
apply.

**Alternatives.**
- *Keep executing twice* (capture, roll back, apply on commit). Simplest and
  proven; costs the leader a second execution and a second hold of the
  capture mutex per write.
- *Apply-time skip without commit*: keep the capture's savepoint open across
  the quorum round. Impossible — the state machine must apply other
  entries on the same connection meanwhile.
- *Speculate under `lanes` too.* Three pipelines share one database, so lane
  C's capture could read lane S's in-flight speculative rows and commit a
  decision that a leadership loss on S later undoes — a phantom read that is
  not reachable today (with co-located leadership C reads S's *committed*
  state).

**Trade-off.** New machinery on the most critical path: an undo table, a
ledger shared by the pipeline and the state machine, a crash-recovery step
at startup, and a scrub of every database copy (snapshot images, backups).
A dirty-read window: between the local commit and the apply, direct SQL
reads on the leader (the read pool) can see the batch; the read model and
the event and worker streams cannot, because they are still projected at
apply. `lanes` keeps paying the double execution.

**Why.** The leader's second execution and second mutex hold are the cost
the review named; removing them is pure **performance** on the critical
path. The design stays sound because three facts hold: a leader's log is
append-only within its term (its proposals come back in order or not at
all), a new term always begins with a blank entry (which undoes anything
left), and the undo record commits atomically with the rows it reverts (so
crashes and copies can always restore committed state). Restricting it to
`single` keeps the cross-lane anomaly out until per-lane database files
(#6) give each lane its own speculative state.

## 2. The state DB runs `NORMAL`; a purge barrier fsyncs it before every log purge

**The call.** `synchronous = NORMAL` for the state DB in every mode. The
raft log store calls a barrier before every purge that fsyncs the state
DB's WAL, main file and directory (`Store::sync_state_db_to_disk`).

**Alternatives.**
- *Keep `FULL`*: one extra fsync per entry on every node.
- *`NORMAL` plus a startup restore* (scheduler-lane-entry-cost plan §6.3):
  when the applied marker is found below the purged log at startup, restore
  the newest local snapshot and replay from there. A second recovery path
  that runs only after power loss — the least-tested code in the system,
  exactly when it matters — and it needs a local snapshot to exist on every
  node, which `lanes` dumps do not give a whole-DB image for.
- *Barrier at snapshot build/install* instead of at purge. Covers the
  common paths but not every purge (openraft's post-install purge, the
  bootstrap seed purge, startup clean-up all purge on their own).

**Trade-off.** Three fsyncs per purge (WAL, main file, directory), on the
raft core's log I/O path; a failed barrier fails the purge and openraft
treats that as a storage error. The fsync is by path, not through SQLite,
so it relies on POSIX `fsync` flushing the inode whatever descriptor wrote
it (true on Linux and macOS).

**Why.** One choke point that every purge passes through is the simplest
thing that keeps the invariant (**simplicity**) and costs one fsync batch
per purge instead of one fsync per entry (**performance**). It takes no
SQLite lock, so it can never wait on the capture mutex from the raft core.

## 3. A batch's entry carries ONE combined changeset, not the per-op list

**The call.** Each op runs in its own savepoint with its own session; the
per-op changesets feed the per-op guards; the entry carries their
`sqlite3changegroup` combination plus the op names.

**Alternative.** Carry the per-op changesets and have followers apply them
in order inside one transaction (the coordinator's direction).

**Trade-off.** Followers cannot tell which op wrote which row (only the op
names travel). A batch that inserts and deletes the same row carries
nothing for it.

**Why.** Side-effect projection re-reads committed rows. Projecting op 1's
changeset after ops 1..n have all committed reads op n's rows — the
allocation-delta path would see an insert whose row is already gone and
fall back, the event publisher could miss rows a later op pruned. One
combined changeset keeps "one entry = one changeset = one projection", the
contract the plan-apply coalescer already relies on, and is smaller and
cheaper to apply (**simplicity**, **performance**).

## 4. Group-commit bounds are constants: 64 ops, 128 KiB

**The call.** `GROUP_COMMIT_MAX_OPS = 64`, `GROUP_COMMIT_MAX_BYTES = 128 KiB`
(the op that crosses the byte budget is still included). No config knobs.

**Alternatives.** Operator knobs (`raft.groupCommitMaxOps/Bytes`); a larger
byte budget (~1 MiB, the size the coordinator suggested).

**Trade-off.** Clusters with unusual entry sizes cannot tune them. 128 KiB
caps how much a burst amortizes per entry.

**Why.** The byte budget is bounded by the append-entries deadline, not by
taste: with `MAX_PAYLOAD_ENTRIES = 32` entries per RPC, 1 MiB entries would
make a 32 MiB replication batch, far past the few MiB the 250 ms deadline
was calibrated for (and an RPC that misses its deadline is retried
verbatim forever). 128 KiB keeps the worst case at ~4 MiB. A knob would
let an operator break that coupling without knowing it exists
(**simplicity**); revisit with a three-voter measurement.

## 5. No overlap of capture N+1 with N's quorum wait

**The call.** Batching only: the pipeline still waits for entry N to apply
before capturing N+1.

**Alternative.** Capture N+1 on top of N's speculative state while N's
quorum round is in flight (Nomad's plan applier).

**Trade-off.** A lane's throughput is still bounded by one capture plus one
round trip per entry; batching amortizes the round trip across everything
queued, but a lone writer sees no gain.

**Why.** Side effects are projected at apply time by re-reading committed
rows. With N+1's rows already in the database when N applies, N's
projection would push N+1's uncommitted rows into the read model and the
worker stream — exactly the leak speculation (#1) is careful to avoid. It
needs projection from the changeset's own values, or a per-entry overlay,
first. Tracked in TODO.

## 6. The capture mutex stays shared by every lane; per-lane database files are a design sketch, not code

**The call.** Keep one `CAPTURE_MUTEX` and one write connection.

**Alternative.** One database file per lane (`ATTACH`), each lane writing
only its own file's tables, so lanes get independent WAL write locks and a
mutex each.

**Trade-off.** Under `lanes`, captures and applies of different lanes still
serialize locally; only quorum waits overlap.

**Why.** A per-lane mutex over one shared write connection is unsound (a
capture holds a savepoint open across many statements; another lane's
statements would land inside it). The real split is a storage-layout change
touching every query that names a table, cross-file reads (consistent per
file, not across), commutative tables needing a home per lane, snapshot and
backup becoming file sets — too large to do safely inside this change.
#1 and group commit already shrink what the mutex covers. Design sketch in
plan §6.4; TODO filed.

## 7. Log retention is a byte budget, enforced by a driver task, with a time gauge

**The call.** `raft.logRetainBytes` (default 256 MiB per group). openraft's
own purge is off (`max_in_snapshot_log_to_keep = u64::MAX`); a driver task
purges behind each new snapshot to the newest `logRetainBytes` bytes and
reports the span as `legion_raft_log_retained_seconds`.

**Alternatives.**
- *Time-based retention* (`logRetainSeconds`). Unbounded disk at a high
  write rate (10 MB/s × 5 min = 3 GB per group).
- *Both knobs.* Two interacting limits to explain.
- *Translate bytes to openraft's entry count* from a measured average entry
  size. openraft's config is fixed at start, so the translation could not
  follow the workload.

**Trade-off.** The operator asks "how long can a follower be down?" and the
answer depends on the write rate; the gauge tells them, the knob is in
bytes. The retention task scans the log's sizes once per snapshot.

**Why.** Bytes bound what the tail costs whatever the entries look like,
and one knob plus one gauge is easier to run than two limits
(**simplicity**). Purging through `trigger().purge_log` keeps openraft's
own guarantees (never past the snapshot, never under an in-flight
replication) and passes the purge barrier (#2).

## 8. Logical commands and name-keyed changesets: deferred

**The call.** Changesets still bind columns by position, so every migration
remains a log-format change and table rebuilds must keep column order.

**Alternatives.** Replicate logical commands (Nomad's FSM messages); or
encode rows by column name.

**Trade-off.** Mixed-version clusters stay limited to schema changes that
append columns (guarded by R2).

**Why.** Either alternative replaces the replication model (JC-1) and every
apply path; it is not a fix to this finding. Recorded with a TODO.

## 9. Data steps run once the voters run this build's whole schema, on the control lane, recorded in a control-lane table

**The call.** `run_data_migrations` is a control-lane op; the driver task
proposes it only when every voter advertises at least this build's
`schema_version()`. The record table `data_migration` is control-owned,
so every data step must touch control-lane tables only.

**Alternatives.** A per-step minimum schema version; a commutative record
table any lane could write.

**Trade-off.** Data steps wait for the whole rolling upgrade, not just the
migration they pair with. A future step on scheduler or status tables needs
the mechanism extended (a record table per lane).

**Why.** A per-step version is a number someone must keep right as other
engineers append migrations; "every voter runs this build's schema" is
always sufficient (**simplicity**). A commutative record would not survive
a sibling-lane snapshot install (commutative rows ride only the control
lane's snapshot), so a step could re-run. All three existing steps touch
control tables.

## 10. A brand-new database records every data step as done

**The call.** When the state DB is created from version 0 (and in
`reset_all_tables`), every data step is recorded as done without running.

**Alternative.** Let the leader run them through the log after start (an
extra entry on every fresh cluster).

**Trade-off.** A local write outside the log at creation time.

**Why.** There are no rows to migrate, and every replica's state descends
from the bootstrap node's database (joiners install its seed snapshot,
which carries the records). Without this, every fresh cluster grows an
extra entry seconds after start, which shifts the entry counts tests and
operators rely on (**simplicity**).

## 11. Data steps were rewritten to be safe to run late

**The call.** The converted steps run long after their schema step: the
app uid backfill assigns the lowest *free* uid (not `base + rank`), and the
scaling cleanup deletes `scaling_action` rows only when they are not
`object:`-keyed.

**Alternative.** Move the SQL verbatim.

**Trade-off.** The steps no longer match the original migrations' text.

**Why.** A verbatim `DELETE FROM scaling_action` run after the new build
wrote object-keyed actions would delete live state; a rank-based uid would
collide with uids the new build already handed out.

## 12. Entries carry their capture schema version for diagnostics only

**The call.** `StoreCommand.schema_version` is recorded but the apply side
still decides by table shape (R2).

**Alternative.** Refuse entries captured under a newer schema.

**Why.** That would refuse entries an older replica can apply perfectly
well (any entry that only touches unchanged tables), breaking the rolling
upgrade R2 was designed for.
