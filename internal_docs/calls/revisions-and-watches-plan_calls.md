# Judgement calls: revisions, watches, and blocking queries

Plan: [revisions-and-watches-plan.md](../plans/revisions-and-watches-plan.md).
Public docs: [watches-and-blocking-queries.md](../../docs/watches-and-blocking-queries.md).

Entries are ranked by project impact, highest first; an entry's number is its
rank.

## 1. A revision is the replica's applied position, a vector under `lanes`

**Call.** A revision is each raft group's applied index, read from
`raft_applied` (committed in the same transaction as the rows it describes).
Under `single` topology and in `local_disk` mode it is one number; under
`lanes` it is three, Control, Scheduler, Status, written `c.s.t`. Revisions
compare component by component, and a change in the journal is tagged with
the one group and index it committed at. It is an opaque string on the wire:

```console
$ legion job info web --json | jq -r .revision
4812
$ legion job info web --index 4812 --wait 60
```

**Alternatives.** (a) One `u64` per object kind: the owning lane's index
(jobs and KV the Control lane's, allocations the Scheduler lane's). Simpler
on the wire, and what Nomad's per-table index looks like. (b) A hybrid
logical clock minted at capture. (c) The sum of the lanes' indexes.

**Trade-off.** A string the client must treat as opaque, and a three-part
form under `lanes` that people will be tempted to parse. A job's state spans
all three lanes (its row is Control, its allocations Scheduler, its task
outcomes Status: `job_status_ops.rs`), so (a) cannot describe "this job as of
now" at all; (c) is monotonic on one replica but two replicas can report the
same sum for different states, which breaks resuming on another manager;
(b) would need the index at capture time, which the stop-and-wait pipeline
knows but the pipelined one being built (SCH1) will not.

**Why (simplicity, extensibility).** One definition that is exact in every
topology, costs nothing to compute (a three-row table), and means the same
thing on every manager, so a watch or a worker resumes anywhere. Under the
default `single` topology it is exactly Nomad's index.

## 2. The change journal is in memory, bounded, per manager; no per-row modify revision

**Call.** Each manager keeps the newest 65,536 changes (jobs, allocations, KV
keys) in memory, fed by the changeset projection every replica already runs.
A resume or blocking read older than the journal's start relists (watch:
`resync_required`) or answers at once (read). Rows do not get a persisted
`modify_index` column.

**Alternatives.** A `modify_index` column on `job`, `allocation`, and
`kv_entry` stamped at apply time, plus tombstones, so "changed since X" is a
store query that survives restarts (what the coordinator suggested, and
etcd's MVCC model).

**Trade-off.** A manager restart empties its journal, so watches and worker
streams that reconnect to *that* manager relist or resync; a watch can only
resume as far back as the window. A persisted column would survive restarts
but needs the index at write time (only known at apply, so a state-machine
hook that writes rows outside the captured changeset), tombstone retention,
and a schema change to three hot tables, all inside the raft pipeline another
change is rewriting.

**Why (simplicity, performance).** The projection already decodes every
committed changeset on every replica; recording what it saw costs a few map
inserts and no extra write on the commit path. Relisting after a restart is
the Kubernetes contract (`410 Gone`) and is always correct.

## 3. Blocking-read options are request headers, not request fields

**Call.** `x-legion-min-revision`, `x-legion-wait`, and
`x-legion-consistency` are gRPC metadata on `GetJobInfo`, `ListJobs`,
`ListInstances`, `GetLedger`, `GetKv`, and `ListKv`, documented on each RPC
and in one block above `WatchKind` in `proto/legion.proto`. Replies carry a
`revision` field.

**Alternatives.** A `ReadOptions read = N` field on each request message
(drafted first, and the more discoverable API).

**Trade-off.** Headers are less visible in generated clients than a field.
The field version forced edits at ~40 struct literals across 20 test files
that five other branches are editing in parallel, and a header also lets the
leader-forward layer and future reads honor the options with no proto
change. Nomad passes the same options as query parameters, not body fields.

**Why (simplicity).** One mechanism every read takes without a schema change
per RPC; the CLI (`--index/--wait/--consistent`) and MCP hide it.

## 4. `RunJob` compare-and-set is on the spec version and fails `ABORTED`

**Call.** `RunJobRequest.expected_version` is compared with the job's latest
config `version` (`Job.version`, now on every job read), inside the
Control-lane entry that writes the new version; 0 means "must not exist". A
mismatch is `ABORTED` with the current version and a hint:

```console
$ legion job run web.ncl --expected-version 6
Error: job web is at version 7, not 6: it changed since you read it
Hint: read it again (legion job info web) and resubmit on top of it with --expected-version 7
```

**Alternatives.** (a) Compare with the job's revision (finding ST2's
modify revision). (b) `FAILED_PRECONDITION`, as `ScaleTask`'s CAS answers.

**Trade-off.** The spec version does not move when only the job's status or
allocations change, so CAS guards the spec, not the job's whole state (which
is what an editor wants). `ABORTED` is reported `retryable: true` by the CLI
envelope; a blind retry of the same request fails again, so the hint says to
read first.

**Why (simplicity).** The version already exists, is what `job spec
history` shows, and is what an agent edits against. `ABORTED` is gRPC's code
for a lost read-modify-write, and it is what `ApplyObject`'s CAS and the app
reconciler's CAS on this very submit path already return.

## 5. `StreamWork` resumes from the journal; payloads load lazily

**Call.** Every work message carries the journal position it reflects; a
worker reconnecting from another epoch sends it, and a manager whose
journal still holds every change on that node since then sends one resume
message with just those changes. The manager then knows the node's desired
ids but not every payload; the heartbeat's feeds load the missing payloads
the first time they need them.

**Alternatives.** (a) Rebuild the node's full payload set on the new manager
but not send it (saves the wire and the worker, not the store reads). (b)
Have the worker send its held ids and diff against them.

**Trade-off.** The first heartbeat after a resume on a node with connect or
policy feeds pays the payload build the resync used to, spread over the
heartbeat interval instead of in one burst at reconnect. A manager that
restarted (empty journal) still resyncs the workers that reconnect to it; a
resume only helps workers that land on a manager that was running.

**Why (performance).** At failover the cost that hurts is 10k+ full-set
builds and sends in the same second; this removes the send and the worker
churn entirely and spreads the reads. (b) costs O(held) on every reconnect,
forever.

## 6. Work-log notes and journal records publish under one lock

**Call.** The projection resolves the work log's allocation changes and the
journal's record against the DB first, then publishes both under a small
"publish" mutex; tagging a work message and attaching a resuming stream take
the same mutex.

**Alternatives.** Order the two updates (notes then record, or the reverse)
without a lock.

**Trade-off.** One more mutex on the projection path (held for in-memory
updates only).

**Why (correctness).** Either order alone has a race: notes-first lets a
resume miss a change committed while it attaches; record-first lets a
message claim a revision whose change it does not carry. Both silently lose
an allocation change on a worker, which is the one thing delivery must not do.

## 7. Event ids never go backwards: a committed-id floor, not a log-index id

**Call.** The event-id format is unchanged; the minting high-water mark is
raised from every committed event a replica applies and seeded from the
journal at start. A new leader applies everything committed before it
captures (the per-term `ensure_linearizable` barrier), so its ids sort above
every committed id.

**Alternatives.** Derive the id from the raft log index, as the finding
suggests.

**Trade-off.** Ids are still clock-shaped and sparse, and ids minted by two
*live* leaders in a split brain could interleave (one of them cannot commit,
so its events never exist). An id is minted inside the capture, before the
log index is known; deriving it from the index would need apply-time rewriting
of a commutative table's primary key, inside the pipeline being rewritten.

**Why (simplicity).** It fixes exactly what users see (a cursor that skips
events after a failover) with a one-line hook in an existing loop, and keeps
every existing cursor valid.

## 8. Linearizable reads use the existing read index and leader forwarding

**Call.** `x-legion-consistency: linearizable` runs openraft's
`ensure_linearizable` on every lane's group on the leader; a follower
answers with the typed not-the-leader rejection, which the leader-forward
layer already replays on the leader.

**Alternatives.** A follower read index (ask the leader for its commit index,
wait for local apply), which openraft 0.9 does not expose.

**Trade-off.** Linearizable reads always run on the leader.

**Why (simplicity).** No new RPC, no new forwarding path.

## 9. Read-after-write waits on the follower, bounded at 5 seconds

**Call.** The leader stamps a forwarded reply with its applied revision; the
follower holds the reply until its own replica covers it, up to 5 seconds,
then replies anyway.

**Alternatives.** Return at once and document the staleness (the old
behavior), or wait without a bound.

**Trade-off.** A forwarded write's latency includes the follower's apply lag
(normally milliseconds). A follower more than 5 seconds behind still returns
a reply its next read may not reflect.

**Why (simplicity).** "I wrote it, then I read it from the same place, and it
was there" is what every user assumes; the bound keeps a sick follower from
hanging writes.

## 10. The heartbeat's writes run in one `spawn_blocking`, in their old order

**Call.** Snapshot reports, the heartbeat update, failures, terminations,
health, volume reports, and pressure actions all run in one blocking task,
in the order they ran inline.

**Alternatives.** One `spawn_blocking` per call (finer-grained), or making
`Store::replicated` async (the review's "eventually").

**Trade-off.** One thread-pool hop per heartbeat instead of up to six, and
the reply waits for all of them, as it did.

**Why (performance, simplicity).** Ordering is load-bearing (a failed restore
must clear before the failure reschedules), and one hop keeps it trivially.

## 11. `WatchObjects` stays on its fingerprint poll (deferred)

**Call.** App object watches are not moved onto the journal in this change.

**Alternatives.** Record `app_object` changes in the journal and give
`WatchObjects` a resume version.

**Trade-off.** App controllers keep polling a fingerprint once a second and
receive a full snapshot on reconnect.

**Why.** It is a separate surface with sharding and per-app scoping; doing it
properly is its own phase. Tracked in `internal_docs/TODO.md`.
