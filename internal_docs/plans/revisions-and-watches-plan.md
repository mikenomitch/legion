# Revisions, watches, and blocking queries for jobs, allocations, and KV

> **Judgement calls:** [revisions-and-watches-plan_calls.md](../calls/revisions-and-watches-plan_calls.md)

**Status: IN EXECUTION.** Phases 1–8 are built (revisions on replies, blocking
reads, the `Watch` RPC, failover-safe event ids, follower read-after-write,
linearizable reads, worker stream resume, the heartbeat offload, and job
compare-and-set with a field diff and an MCP wait). Deferred: moving
`WatchObjects` onto the revision journal (it still polls and resends a
snapshot), a persisted per-row modify revision, a revision on every reply
outside the six reads and three writes named below, and watches on kinds
other than jobs, allocations, and KV.

Findings: [architecture review ST2, ST3, ST5, AI5](../architecture-review-2026-11-07.md).

## Problem

A controller or an agent that wants to know when something changed has one
tool today: poll.

- **Reads carry no index.** `GetJobInfo`, `ListJobs`, `GetKv`, `ListKv`,
  `ListInstances`, and `GetLedger` return the state of whichever replica
  answered, with nothing saying how new that state is, and no way to ask
  "give me this once it is newer than what I have".
- **There is no watch on jobs, allocations, or KV.** `WatchObjects` exists for
  app objects, but it polls a fingerprint about once a second and resends a
  full snapshot on every reconnect.
- **Event ids can go backwards after a failover.** They are wall clock plus a
  per-process sequence (`src/storage/store.rs`, `next_scheduler_event_id`), so
  a new leader whose clock is behind mints ids below a poller's cursor
  (`docs/event-stream.md`, "ID ordering is per manager process").
- **Read-after-write through a follower is not guaranteed.** A follower that
  forwards a write to the leader (`src/manager/leader_forward.rs`) returns
  the leader's reply at once; the client's next read on the same follower can
  run before the follower applied its own write.
- **There is no linearizable read.** `ensure_linearizable` exists in the raft
  driver but only guards a new leader's first capture.
- **A manager restart or failover resyncs every worker it served.**
  `StreamWork` positions hang off a random per-process epoch
  (`src/storage/work_log.rs`), so a worker that reconnects anywhere else is
  sent its whole desired set, rebuilt from store reads. Losing a manager at
  10k–50k nodes means thousands of full-set builds at once (ST3).
- **The heartbeat handler parks a runtime thread on raft writes.**
  `handle_failed_allocations`, `handle_terminated_allocations`, and
  `update_allocation_health_snapshots` run inline (`src/manager/server.rs`,
  `heartbeat`), and each can make a blocking replicated write (ST5).
- **Jobs are less declarative than objects.** `RunJobRequest` has no
  `expected_version`, so two agents silently overwrite each other's jobs;
  `PlanJobReply` says `same_content` but not *what* changed; and the MCP
  `legion_job_run` tool cannot wait for the job to come up (AI5).

What users do instead: loop over `legion job info` and diff the output
themselves; tail `legion events` and hope no cursor regressed; re-read after a
write until it shows up; and rely on social agreement that only one agent
edits a given job.

## Prior art

- **Kubernetes list-and-watch** ([API concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes)).
  Every object and every list carries a `resourceVersion` (an etcd revision).
  A client lists, then watches "everything after version X". When X has been
  compacted away the watch fails with `410 Gone` and the client relists.
  Writes take `metadata.resourceVersion` as a compare-and-set precondition.
  *We take:* the list-then-watch shape, an opaque revision string, the typed
  "too old, relist" answer, and resume-from-a-revision on reconnect.
  *We reject:* storing every object version (etcd MVCC). Legion's journal is
  in memory and bounded; a resume older than the window relists.
- **Nomad blocking queries** ([docs](https://developer.hashicorp.com/nomad/api-docs#blocking-queries)).
  `?index=N&wait=5m` on any read returns as soon as the relevant table's
  index passes N, or after the wait with the current index. `X-Nomad-Index`
  is on every reply. Default wait 5 minutes, maximum 10. Reads are stale-ok
  by default; `?stale=false` (the default) forwards to the leader, and
  `consistent` adds a quorum check.
  *We take:* `min_revision` + `wait` on the main reads, a 5-minute default,
  and an opt-in consistency mode. *We tighten:* a read wakes only when an
  object *it would return* changed, not when anything in the table did.
- **etcd** ([watch API](https://etcd.io/docs/v3.5/learning/api/#watch-api)):
  a revision per write, prefix watches, `compact_revision` in the cancel
  reason. *We take* the prefix watch for KV and the "oldest servable
  revision" in the resync answer.
- **Consul `?index=`** is Nomad's model; its X-Consul-Index is the raft index.
  Like Consul and Nomad, our revision *is* the raft applied index.

## Jobs to be done

**Autonomous agent**
- When I start a deploy, I want to wait until it is up or has failed, in one
  call that cannot hang forever, so I don't have to write a polling loop.
- When I edit a job someone else may also be editing, I want my write to fail
  if theirs landed first, so I never overwrite work I did not read.
- When I'm about to change a job, I want to see exactly which fields change,
  so I can check my edit did what I meant before it runs.
- When I lose my connection, I want to pick up where I left off, so I don't
  miss changes or reprocess everything.

**Controller author (app or external operator)**
- When I react to jobs, allocations, or KV keys changing, I want to be told
  when they change rather than polling, so my controller is cheap and fast.
- When my watch drops or my manager fails over, I want to resume from the
  last thing I saw, and be told plainly when that is too old so I can relist.

**Job author**
- When I write a key and then read it, or submit a job and then look at it,
  I want to see my own write, whichever manager answers.

**Cluster operator**
- When a manager restarts or the leader fails over, I don't want every
  worker it served to rebuild and re-receive its whole work set at once.
- When heartbeats report failures, I don't want those writes to stall the
  manager's request threads.

**On-call responder**
- When I'm following an incident, I want `legion job watch web` to print each
  change as it happens, so I see the status flip without re-running a command.

## Goals and non-goals

Goals:

1. A revision on the replies of the main reads and writes for jobs,
   allocations, and KV, derived from the replicated log position, so it
   means the same thing on every manager and never goes backwards across a
   failover.
2. Blocking reads (`min_revision` + `wait`) on those reads.
3. A server-streaming `Watch` for jobs, allocations, and KV prefixes that
   resumes from a revision, and says "resync required" when it cannot.
4. Event ids that never go backwards across a failover.
5. Read-after-write through a follower.
6. Opt-in linearizable reads.
7. `StreamWork` resumes from a revision on any manager (ST3).
8. The heartbeat's failure, termination, and health writes run off the async
   runtime (ST5).
9. `RunJob` compare-and-set, a structured plan diff, and a bounded wait on the
   MCP `legion_job_run` tool (AI5).

Non-goals:

- **An intent layer above jobs** (AI5's "serve model X at p99 < 200 ms").
  App object kinds are the place for it; nothing here builds it.
- **Storing object history.** The journal is in memory and bounded. Resuming
  older than it relists, like Kubernetes after compaction.
- **A watch on every kind.** Jobs, allocations, and KV only. Nodes, rollouts,
  and evals are not covered.
- **Rewriting `WatchObjects`.** It keeps its fingerprint poll for now
  (deferred, below).
- **Making `Store::replicated` async.** ST5's "eventually" half is out of
  scope; this plan moves the three calls off the runtime.

## Background: what exists today

- **The applied index commits with the data.** The raft state machine writes
  each lane's applied log id to `raft_applied` inside the same savepoint as
  the entry's rows (`src/storage/raft/state_machine.rs`, `apply`). In
  `single` topology one group (`control`) carries every lane; under `lanes`
  there is one group, and one row, per lane (`physical_group`,
  `src/storage/raft/mod.rs`).
- **Every replica projects every committed changeset.**
  `Store::project_changeset_side_effects` (`src/storage/store.rs`) runs after
  each apply on every replica, and already feeds the read model, the event
  broadcast, and the `StreamWork` log. In `local_disk` mode,
  `capture::local_projected_mutation` projects a session changeset filtered
  to `READ_MODEL_TABLES`.
- **Single-writer table ownership.** `job`, `job_config`, `task_config`, and
  `kv_entry` are Control-lane tables; `allocation`, `instance`, `volume` are
  Scheduler; `task_status` is Status (`src/storage/raft/ownership.rs`).
- **The derived job status spans all three lanes**
  (`src/storage/store/job_status_ops.rs`): the job row, its latest version's
  tasks, live allocations, task outcomes, and keyed instances.
- **`StreamWork`** (`src/storage/work_log.rs`, `src/manager/server/work_stream.rs`,
  `src/worker/work_stream.rs`): per-node `dirty`/`held`/`unacked` state on the
  serving manager, an order-independent digest checked on each heartbeat, and
  a random epoch per manager process.
- **Leader forwarding** (`src/manager/leader_forward.rs`): a follower runs a
  request locally and replays it verbatim to the leader only when the reply
  is the typed not-the-leader rejection.
- **CAS elsewhere:** `ApplyObjectRequest.expected_version` (`ABORTED` on
  conflict, with a hint to read again), `ScaleTaskRequest.expected_version`,
  and the app reconciler's submit (CAS on the app record, `ABORTED`).
- **Job versions:** each changed submit writes a `job_config` row with
  `version = previous + 1` (`handle_job_submit_tx`); an unchanged submit
  writes nothing.

## Proposed design

### 1. The revision

A revision is the replica's **applied position**, written as an opaque string.

- `local_disk` mode: a store-wide counter, bumped by every committed write
  that touches a watched table, and persisted in blocks (`store_revision`) so
  it never goes backwards across a restart.
- Raft, `single` topology: the one group's applied index, e.g. `"4812"`.
- Raft, `lanes` topology: the three groups' applied indexes, Control,
  Scheduler, Status: `"4812.90211.120034"`.

Revisions compare **component by component**: `A` covers `B` when every
component of A is at least B's. A `"4812"` from a single-topology cluster
reads as `"4812.0.0"`. Because each component is a raft log index, every
manager agrees on what a revision means: a replica that reports `R` holds
every change committed at or below `R`, and a new leader's indexes only grow.

Clients must treat the string as opaque: carry it, pass it back, compare two
for equality. The CLI and docs say so.

Replies that carry `revision`: `GetJobInfo`, `ListJobs`, `ListInstances`,
`GetLedger`, `GetKv`, `ListKv` (reads), and `RunJob`, `PutKv`, `DeleteKv`
(writes; the revision at which the write is visible on the answering
manager).

### 2. Blocking reads

One shared request message on each of those six reads:

```proto
message ReadOptions {
  string min_revision = 1;          // block until something this read returns changed after it
  optional uint32 wait_seconds = 2; // default 300, max 300
  ReadConsistency consistency = 3;  // DEFAULT | LINEARIZABLE
}
```

With `min_revision` set, the manager waits until an object the read would
return changed at a revision after `min_revision`, then answers with the
current state and `revision`. On timeout it answers anyway with the current
state and revision (Nomad's contract): the caller compares revisions. When
`min_revision` is older than the manager's journal, it answers at once.

What counts as "would return":

| Read | Wakes on |
| --- | --- |
| `GetJobInfo name` | the job's row or versions, its allocations, its task outcomes |
| `ListJobs` | any job change (as above, for any job) |
| `ListInstances job` | the job's allocations or instances |
| `GetLedger` | any allocation change |
| `GetKv key` | that key |
| `ListKv prefix` | any key under the prefix |

```console
$ legion job info web --json | jq -r .revision
4812
$ legion job info web --index 4812 --wait 60s --json   # returns when web changes
```

### 3. `Watch`

```proto
rpc Watch (WatchRequest) returns (stream WatchEvent);

message WatchRequest {
  WatchKind kind = 1;          // JOB | ALLOCATION | KV
  string namespace = 2;        // jobs/allocations: only this namespace
  string job = 3;              // jobs/allocations: only this job
  string node_id = 4;          // allocations: only this node
  string prefix = 5;           // KV: only keys under this prefix
  string from_revision = 6;    // resume after this; empty = from now
}

message WatchEvent {
  string revision = 1;               // resume from here
  repeated WatchChange changes = 2;
  bool resync_required = 3;          // from_revision is older than the journal: relist
  string oldest_revision = 4;        // with resync_required: the oldest servable
  bool progress = 5;                 // a keep-alive carrying the current revision
}
```

Each `WatchChange` names the object (`kind`, `key`, `job`, `namespace`,
`node_id`, `allocation_id`), whether it was `deleted`, the revision it
changed at, and the object as it reads when the event is sent (`Job` for
jobs, `KvEntry` for KV, a compact `WatchAllocation` for allocations). The
first message on every stream is a progress event with the current
revision, so a client that lists and then watches knows exactly where it
started. Every 30 seconds without a change, another progress event.

When `from_revision` is older than the journal (a restart emptied it, or the
window rolled), the first and only message is `resync_required = true` with
the oldest revision this manager can serve, and the stream ends. The client
relists and watches again from the list's revision — the Kubernetes 410.

```console
$ legion job watch web
4813  job web  updated  status=pending  version=7
4820  job web  updated  status=running  version=7
$ legion kv watch app/config/ --from 4812 --json
```

The journal is in memory, per manager, bounded at 65,536 changes. It starts
empty at the manager's applied revision when the process starts, and is
emptied (with a resync for every open watch) when a raft snapshot install
replaces the database under it.

### 4. Event ids that never go backwards

The id format stays (milliseconds, sequence, lane residue). The minting
high-water mark is raised from every **committed** event a replica applies,
and seeded from the journal at start. A new leader applies everything
committed before it captures (the existing per-term `ensure_linearizable`
barrier), so its first id is above every id the old leader committed,
whatever its clock says.

### 5. Read-after-write through a follower

When a follower forwards a request, the leader stamps its reply with the
applied position after the write (`x-legion-applied` header). The follower
waits, up to 5 seconds, until its own applied position covers it before
returning the reply. A client that writes and then reads the same follower
sees its write.

### 6. Linearizable reads

`ReadOptions.consistency = LINEARIZABLE`: the leader confirms leadership with
a quorum (`ensure_linearizable`) and waits until it has applied everything
committed before it answers. A follower answers such a read with the
not-the-leader rejection, which the leader-forward layer replays on the
leader. In `local_disk` mode every read is already linearizable.

### 7. `StreamWork` resumes from a revision (ST3)

Every `StreamWorkReply` carries the `revision` its contents reflect. A worker
remembers the revision of the last message it applied and sends it on open
(`StreamWorkOpen.last_revision`). A manager that cannot replay (another
epoch: a restart, a failover, a different manager) but whose journal covers
`last_revision` for that node answers with **one resume message**:
`resumed_from = last_revision`, the allocations that changed on the node
since then as upserts and stops, and the digest of the node's whole desired
set. The worker applies it on top of what it holds; the heartbeat digest
check is unchanged and still catches any disagreement with a full resync.
When the journal does not cover it, the full resync is the fallback, as
today.

### 8. Heartbeat offload (ST5)

Failure, termination, health, volume, and pressure reports run on the
blocking pool in one `spawn_blocking`, in the same order as before, after
the heartbeat update.

### 9. Jobs: compare-and-set, diff, and wait (AI5)

- `RunJobRequest.expected_version`: the job's spec version (the `version` of
  its latest config; 0 means "must not exist"), checked inside the
  Control-lane entry that writes the new version. A mismatch is `ABORTED`:

  ```console
  $ legion job run web.ncl --expected-version 6
  Error: job web is at version 7, not 6: it changed since you read it
  Hint: read it again (legion job info web) and resubmit with --expected-version 7
  ```

  `Job.version` and `RunJobReply.version` report the version.
- `PlanJobReply.diff`: one `JobFieldChange` per changed path
  (`tasks.web.resources.memory: 256 → 512`), added, removed, or changed, with
  old and new values as JSON; values rendered from KV the caller cannot read
  are redacted. `legion job plan` prints it and includes it in `--json`.
- MCP `legion_job_run` takes `expected_version` and `wait_seconds` (≤ 300):
  it waits, with blocking reads, until the job reads `running`, `complete`,
  `scheduled`, or `idle`, or reads `failed`/`stopped`, and returns the final
  status.

## Proto, struct, config, and persistence changes

- Proto: `ReadOptions`, `ReadConsistency`, `read` on six read requests,
  `revision` on six read replies and three write replies; `Watch`,
  `WatchRequest`, `WatchEvent`, `WatchChange`, `WatchKind`,
  `WatchAllocation`; `StreamWorkOpen.last_revision`,
  `StreamWorkReply.revision`/`resumed_from`; `RunJobRequest.expected_version`,
  `RunJobReply.version`/`revision`; `Job.version`; `PlanJobReply.diff`,
  `JobFieldChange`.
- Migration (DDL only): `store_revision` (one row: the `local_disk` revision
  block reserved so far). Classified `Internal` in the ownership map: it is
  written only in `local_disk` mode and never rides a changeset.
- No jobspec fields, so nothing in `configs/jobs/maximal.ncl`.

## Implementation phases

1. **Revisions + journal** (`src/storage/revisions.rs`, projection hook):
   revisions on replies. Valuable alone: a client can tell how new a reply is.
2. **Blocking reads.** Valuable alone: polling loops become one call.
3. **`Watch`** + CLI `job watch`/`kv watch`/`alloc watch` + MCP `legion_watch`.
4. **Event id high-water mark.**
5. **Follower read-after-write** + **linearizable reads.**
6. **`StreamWork` resume** (ST3).
7. **Heartbeat offload** (ST5).
8. **Jobs CAS, plan diff, MCP wait** (AI5).

Each phase stands alone; phases 2, 3, and 6 need phase 1.

## Testing plan

- Unit (`src/storage/revisions.rs`): revision parse/format/cover; journal
  window, horizons, per-node changes, resets; changes-since filtering.
- Unit (`src/storage/work_log.rs`): a resume attaches without a resync and
  carries the changed allocations; a too-old revision resyncs.
- Unit (server): the heartbeat handler does not block the runtime while its
  writes wait; RunJob CAS success, conflict, must-not-exist; plan diff paths.
- Integration (`tests/integration/revisions_watch.rs`): revision monotonic
  across writes and across a raft failover; a blocking read returns on change
  and times out otherwise; a watch resumes after reconnect with no full
  resend; `resync_required` when too old; read-after-write through a
  follower; a linearizable read through a follower.
- Integration (`tests/integration/work_stream.rs`): a worker whose manager is
  killed resumes on another manager with no full resync.
- Regression: `acl_endpoints::`, `work_stream::`, `raft_follower_writes::`,
  `raft_eval_failover::`, `scheduler_integration::`.

## Rollout and backward compatibility

Legion has no installed base, so nothing is shimmed. What changes:

- `StreamWork` gains fields; a worker that does not send `last_revision`
  gets today's behavior (a resync on a new epoch).
- `RunJob` without `expected_version` behaves as before.
- Event ids keep their format; only the minting floor moves.
- Blocking reads only block when `min_revision` is set.
- Breaking: none on the wire. `docs/event-stream.md`'s "ids can go backwards
  after a failover" caveat is removed.

## Open questions

- *Should the journal size be configurable?* Lean: not yet; 65,536 changes
  is a few minutes of heavy churn, and a resync is correct, only slower.
- *Should `WatchObjects` move onto the journal?* Lean: yes, as a follow-up;
  its objects are Control-lane rows and the projection hook already sees
  them.

## Acceptance criteria

1. `legion job info web --json` and `legion kv get k --json` show a
   `revision`; it grows with each write, and is never lower on a new leader.
2. `legion job info web --index R --wait 30s` returns within a second of a
   change to `web`, and after 30 s otherwise.
3. `legion job watch web` prints status transitions; killing its manager and
   resuming from the last revision on another prints nothing twice and skips
   nothing; resuming from `1` prints a resync-required error.
4. A worker whose manager is killed resumes on another with no full resync
   (`work_stream::` asserts zero resync messages).
5. `legion job run --expected-version 3` against a job at version 4 fails
   with `ABORTED` naming 4.
6. `legion job plan` prints the changed fields.
