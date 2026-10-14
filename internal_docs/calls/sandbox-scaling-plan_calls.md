# Judgement Calls: Sandbox Scaling to Very Large Session Counts

Decisions made while writing
[`plans/sandbox-scaling-plan.md`](../plans/sandbox-scaling-plan.md), and
while implementing its Phases 0–3 (entries 5, 14, 15, 17, 20, 21, 23, 24,
31, 38–48, 59, and 60) and Phases 4–6 (entries 6, 8, 10, 11, 18, 19, 26,
27, 34, and 49–58). **Entries are ranked by project impact, highest first. The
number is the rank.** Implementation-time calls get inserted at their rank
as the phases execute. If a citation elsewhere names an entry by number,
fix it when you renumber. The plan cites entries by number throughout.

The exec entries that started here moved to
[`sandbox-exec-plan_calls.md`](sandbox-exec-plan_calls.md) with the exec
work.

## 1. Running sessions are node-owned soft state, and raft keeps only what must be durable

**Call.** The recommended architecture combines brainstorm options 1, 2,
and 3. A running session in `session` mode is recorded on its node, in a
local log, and in a leader-held index rebuilt from node reports. An idle
session is a manifest in the content store. Raft holds jobs, envelopes,
and promoted (named or durable) instances. A session's start, suspend,
resume, and stop write nothing to raft.

**Alternatives.**
- (a) Stop at group commit and write trimming (option 5). It is the
  cheapest change and keeps every guarantee. It is Phase 1 either way.
- (b) Write-behind for keyed instances: persist the record asynchronously
  and let nodes re-report after a crash. It keeps one instance model.
- (c) Shard instance rows across raft groups first (option 4), keeping
  exact semantics everywhere.
- (d) An external registry (option 6).

**Trade-off.** Unnamed sessions get weaker guarantees than keyed
instances: probabilistic id uniqueness, converge-to-one instead of
at-most-one, an eventually consistent list, and loss on node failure while
running (plan §6.7). Platforms have to know which mode they are in. The
code gains a second source of truth for "what is running", scoped to one
mode.

**Why (performance / scale, then simplicity).** (a) still costs raft
entries and rows per session, so it raises the ceiling by a constant and
leaves the resident-count wall (tombstones, in-memory snapshots) where it
is. (b) makes keyed instances lie about guarantees they document, which is
worse than a separate mode that states its own. (c) multiplies operational
surface (N groups, scatter-gather, cross-cell moves) to keep per-session
records nobody needs to be exact. Every system in the prior art past about
10^5 resident units (Borg, flyd, Modal, Lambda slots) made the host
authoritative. Legion already did the same for liveness, the location
index, and warms, so this extends a pattern operators already run.

**As built (Phase 6).** Nodes own running sessions as planned, but the
index is per manager rather than on the leader (entry 6), and the node keeps
no local log (entry 19). Idle sessions are not built yet (entry 18).

## 2. Sessions are a third instance mode, not a new noun

**Call.** `stateful.mode = "session"` beside `slot` and `keyed`. The
existing `legion instance` commands, RPCs, and MCP tools work on sessions,
and replies carry `durable` and, for lists, `complete`.

```console
$ legion instance create agents -j
{"ok":true,"data":{"instanceId":"workspace-7k2qd9x4m1b8c3rj5t0v6wye2h",
 "mode":"session","durable":false, ...}}
```

**Alternative.** A separate `legion session start|list|get|suspend|resume`
surface and `Session` RPCs, which makes the different guarantees
impossible to miss.

**Trade-off.** A caller can read `instance list` and assume exactness when
some rows are best effort. The `durable` and `complete` fields carry that
distinction, and an agent that ignores them can be wrong.

**Why (simplicity).** Promotion turns a session into a keyed instance in
place. With two nouns, promotion would move an object between APIs and
every consumer (UI, MCP, catalog, ACL tests) would handle both. One noun
keeps one set of tools for agents and one ACL vocabulary.

## 3. Naming promotes, and unnamed ids are 130 random bits with no tombstone

**Call.** A caller-chosen id always makes a durable keyed instance. A
server-minted id is `{task}-{26 base32}`, 130 random bits, never checked
and never tombstoned, and caller ids may not take that shape. Phase 4
applies this to server-minted keyed ids too, so it changes an existing
documented guarantee from "never recycled, by record" to "never recycled,
by randomness".

**Alternatives.**
- (a) Keep tombstones for every id, as today.
- (b) Make untombstoned server ids opt-in per job.
- (c) Let sessions take caller names without promotion, checked against
  the soft index.

**Trade-off.** At a trillion ids the collision chance is about 10^-15, not
zero. A `subTemplate` with `{instance}` puts that probabilistic id inside
a security boundary. Phase 4 needs an upgrade note because it changes a
guarantee existing users read.

**Why (scale, then simplicity).** (a) makes raft size grow with history,
which is the resident-count wall. (b) adds a knob whose right answer is
the same for almost everyone. (c) cannot work, because a soft index can
answer "is this name taken" wrongly during a rebuild, and a name is
exactly what a platform keys its own data on. Cloudflare's split between
`newUniqueId` and `idFromName` is the precedent: random ids skip the
uniqueness check, and names pay it once. The reserved shape closes the
remaining hole, a caller typing in a dead unnamed id, with the same kind
of rule slot ids already have.

## 4. Work reaches workers as a sequenced delta stream held in leader memory, with the full set as resync

**Call.** Phase 2 replaces "best-effort push plus the full desired set on
every heartbeat" with a per-node delta log in the leader's memory, built
from the changesets it already applies. Each delta carries `(leader_epoch,
seq)`. The worker holds `Manager.StreamWork`, acks its last applied `seq`,
and gets today's full desired set once on failover, regression, or a gap.

**Alternatives.**
- (a) Tensorlake's outbox: write each command durably in the placement
  transaction, so delivery survives a leader change without a resync.
- (b) Keep the full set and add a desired-state version, so an unchanged
  node gets a cheap "unchanged" reply (the open TODO).
- (c) Make the push reliable (retry until acked) and keep the full set as
  is.

**Trade-off.** A leader failover makes every worker resync once, which on
a 10,000-node fleet is a burst of full sets. Two delivery paths exist
during the version-skew window. The stream is one more long-lived
connection per node on the leader.

**Why (performance, then simplicity).** (a) adds rows to every placement
entry, and every ack and prune becomes a raft write per node per beat, the
cost Phase 1 removes from allocation health. Legion's allocation rows are
already the durable record, so a durable outbox would store everything
twice. (b) fixes the steady state but still rebuilds and sends thousands of
payloads on any change, and still leaves a lost push waiting up to a
heartbeat interval (21 s at fleet scale). (c) keeps the per-beat full set.
Sequenced deltas with a full resync give round-trip latency and constant
heartbeat size, and the fallback is the code that runs today.

## 5. The delivery epoch is per manager process, and any manager serves the stream

**Call.** Phase 2 deviates from entry 4 in two places. The epoch a stream
position belongs to is a random value chosen when the manager process
starts, not the raft term the leader took office in. And the delivery log
lives on whichever manager the worker streams from, leader or follower,
fed by the changeset projection every replica already runs, not only in
the leader's memory.

```text
StreamWorkOpen { node_id, last_epoch, last_seq }
  last_epoch == this process's epoch and last_seq still held -> replay
  anything else                                             -> full resync
```

**Alternatives.**
- (a) The plan's shape: the leader holds every stream, the epoch is its
  term, and followers redirect workers to it.
- (b) A replicated per-node sequence, so any manager could continue any
  stream after a failover.

**Trade-off.** A manager restart resyncs its workers even if it was not
the leader, and two managers serving the same node at different times give
it unrelated sequences (the epoch tells them apart, so the cost is a
resync). Each manager pays the projection's per-change lookup for the nodes
streaming from it.

**Why (simplicity, then performance).** Workers already dial one fixed
manager forever and heartbeat to it (liveness plan §6.3); (a) would make
the stream the one RPC that has to find the leader, and a leader failover
would resync every worker in the fleet at once, which is the burst entry 4
accepted. With the log on every replica, a failover disturbs only the
streams attached to the manager that died
(`tests/work_stream.rs::a_leader_kill_resyncs_only_the_workers_that_lost_their_manager`:
the surviving manager's worker saw zero resyncs, the orphaned one exactly
one). The raft term would also not be a valid epoch on a follower, and a
process-random epoch costs nothing. (b) puts a write on every delivery,
the cost entry 4 rejected.

## 6. The session index is per manager, and reads fan out to every peer

**Call.** Each manager indexes, in memory, the sessions of the nodes that
heartbeat to it. A read that needs the cluster's view (`instance get`,
`list`, `stop`, an idempotent create, the router's room count) merges this
manager's index with every peer's answer to `RaftTransport.QuerySessions`,
preferring the manager that heard from a node most recently. Plan §6.4 put
one index on the scheduler-lane leader, and §12 leaned that way, with other
managers forwarding reads to it.

**Alternatives.**
- (a) The leader-held index of §6.4: forward every node's session report
  to the leader, or move every worker's heartbeat to the leader.
- (b) Replicate session reports through raft, so every manager holds the
  whole index.

**Trade-off.** A read of a session this manager does not index costs one
round trip per peer. A peer that does not answer makes the view incomplete
(`complete = false` on a list) rather than slow. Two copies of one session
on nodes that report to different managers never meet in one index, which
is why duplicates need more than the index to converge (entry 26).

**Why (simplicity, then scale).** Heartbeats already land on whichever
manager a worker talks to, and any manager serves the stream (entry 5), so
the reports are already spread out. (a) adds a forwarding path for every
report, or concentrates every heartbeat on one process, and rebuilds the
whole index on every leader change. (b) is a raft write per report, which
session mode exists to avoid. Fan-out is paid by reads only, and the hot
read, the router's, is cached for a second (entry 27). Memory per manager
scales with its own nodes' sessions, not the cluster's.

## 7. Idle sessions are manifests, and archiving keyed instances ships before session mode

**Call.** A suspended session is a manifest in
`<contentStore.url>/sessions/<job>/...`, with no raft row. Phase 5 brings
the same idea to keyed instances first (`suspend.archive`), keeping a slim
row for named ones.

**Alternatives.**
- (a) Keep idle instances in raft and shard later.
- (b) Put every idle instance in the bucket, named ones too, and use the
  object store's conditional put as the uniqueness authority.
- (c) Ship session mode first, since that is where the zero-write promise
  lives.

**Trade-off.** Listing idle sessions becomes a paginated bucket listing,
slow at 10^8 objects. An idle session's existence depends on bucket
lifecycle rules Legion does not own. A named archived instance still costs
one slim row.

**Why (scale, then simplicity).** Idle sessions are the majority on an
agent platform, so they set the row count. Published checkpoints already
put the bytes in the bucket, so the manifest adds identity, not a new
storage system. (b) makes S3 conditional writes part of Legion's
correctness story for names, a dependency the `file://` content store
cannot honor. (c) delays the bigger win behind the bigger design: Phase 5
works on today's keyed model and is valuable even if Phase 6 never ships.

**As built (Phase 5).** Archiving rides the publish report rather than
costing its own entry (entry 8), and manifests live under
`manifests/<job>/<group>/<task>/` (entry 50). Session manifests are
deferred with session suspend (entry 18).

## 8. Archiving rides the publish report and writes no entry of its own

**Call.** The manager's publish request for a suspend checkpoint of a task
with `suspend.archive` names the manifest object and carries the
instance's identity. The worker writes the archive manifest there after the
parts, and puts its URL in the publish report. The manager drops the snapshot row,
and with `manifestOnly` the instance row, in the entry that records the
publish. A resume puts the snapshot row back in the entry that starts it.
Plan §6.5 had archiving cost one raft entry per instance, and unarchiving
one more.

```nickel
suspend = { checkpoint = "memory", durability = "store", archive = "slim" },
```

**Alternatives.**
- (a) A separate archive step: the manager writes the manifest after the
  publish commits and proposes its own entry, as §6.5 sketched.
- (b) Archive on a policy (`archive.after = "1h"`), sweeping suspended
  instances older than a threshold.

**Trade-off.** An instance is archived the moment its checkpoint is
published; there is no window in which it is published but still fully in
the store. A manifest written for a report that never commits is an orphan
object until bucket lifecycle removes it. The manager, not just workers,
needs access to the bucket (entry 49).

**Why (performance).** The publish report is already one scheduler-lane
entry, so folding the drop into it makes a million archives cost no more
entries than a million publishes, where (a) doubles them. The manifest is
the publish's own data, which only the worker holds in full at that
moment. (b) is a knob and a sweeper whose right answer, for fleets where
idle instances are the majority, is "immediately".

## 9. Raft places capacity envelopes, and a router fills them

**Call.** A session-mode task declares `stateful.envelope`, a per-node
reservation that the scheduler places as an ordinary allocation. Sessions
fill envelopes through a manager-side router (first fit, locality-aware)
or edge admission. No raft write per session.

```nickel
envelope = {
  resources = { cpu = 30000, memory = 61440 },
  constraint = [{ attribute = "pool", value = "agents" }],
},
```

**Alternatives.**
- (a) No envelope. The router places sessions anywhere with free capacity
  from live node usage (pure option 2, Modal's shape).
- (b) Raft places pools of warm VMs (option 7 as a VM pool).

**Trade-off.** Envelopes reserve capacity whether or not sessions fill
them, so a cluster mixing sessions and services has to size envelopes.
Spare envelope room is invisible to other jobs except through
oversubscription. The job author writes one more block.

**Why (simplicity).** (a) makes the scheduler blind to most of the
cluster's load, so quotas, preemption, `node usage`, and bin packing for
everything else become wrong. Envelopes keep every existing scheduler
feature correct with zero changes to the scheduler, at the cost of one
allocation per node per pool. (b) ties the envelope to one VM and to one
isolation tier. Borg's alloc and Lambda's slots are the precedent: place
the container of capacity slowly and durably, fill it quickly and softly.

## 10. A session task becomes a keyed task plus a generated `<task>-envelope` task

**Call.** Submit normalizes a `session` task: the task itself is stored
as `keyed` with the envelope and promote settings beside it, and Legion
generates a sibling task `<task>-envelope` on a built-in `envelope` runtime,
`count = { num = 1, per = "id" }`, sized by `envelope.resources` and
constrained by `envelope.constraints` plus the `sessions` worker
capability. The envelope allocation runs nothing; it is the reservation.

**Alternatives.**
- (a) A new allocation kind the scheduler places per node in a dedicated
  pass.
- (b) Have the job author write the envelope task by hand.

**Trade-off.** The job shows a task its author did not write, in
`job status`, `alloc list`, and the UI, and the name `<task>-envelope` and
the `envelope` runtime are reserved:

```text
Error: task workspace-envelope uses runtime "envelope", which is reserved for the envelope tasks Legion generates for session tasks
```

Every eligible node gets the same envelope size.

**Why (simplicity).** A generated task is placed, preempted, blocked when
nothing fits, rolled out, and counted in `node usage` by code that already
exists, so the scheduler changed by zero lines, which entry 9 promised.
Storing the task as `keyed` means promotion produces an ordinary keyed
instance of the same task, with no mode switch. (a) is a second placement
path to keep correct. (b) asks every author to keep two tasks' resources
and constraints consistent by hand.

## 11. A promoted session's allocation reserves nothing, and the session keeps its envelope share

**Call.** Promotion inserts an allocation with zero resources, bound to
the new instance row on the session's node. The session keeps counting
against its envelope until it stops. Plan §6.6 shrank the envelope by the
session's size and inserted a full-size allocation in the same apply.

**Alternatives.**
- (a) §6.6's shrink and insert.
- (b) A full-size allocation without shrinking the envelope.

**Trade-off.** Until it stops, a promoted instance's resources show under
its envelope in `node usage`, not under its own allocation. Promoted
instances take envelope room that new sessions could have used. When a
promoted instance moves (node loss, replace, resume elsewhere), the new
placement reserves the task's full `resources` like any keyed placement,
so it changes how it is counted the first time it moves.

**Why (simplicity).** Legion has no in-place allocation resize, and (a)
needs one plus a worker that moves the session's share between two
ledgers atomically. The worker already admits the session against the
envelope, so a zero-resource allocation counts capacity exactly once, the
property §6.6 asked for, with nothing new. (b) counts it twice and can
overfill a full node.

## 12. This plan owns wake on request for idle sessions, at a gateway, not on the node

**Call.** Wake for a session that exists only as a bucket manifest is
built in a stateless gateway (`legion start gateway`, Phase 7). The suspend
plan's Phase 8 keeps node-local wake on connect for local checkpoints, and
the gated plan's Phase 5 keeps the last hop into gated guests.

```nickel
suspend = { durability = "store", wake = { on = "request", port = 8080, timeout = "10s" } },
```

**Alternatives.**
- (a) Extend the suspend plan's parked listener: keep the port reserved on
  the node that suspended the session and resume there.
- (b) Leave waking to platforms, which already call `instance start`.

**Trade-off.** Operators who want wake on request run one more stateless
tier, and requests take one extra hop. Two wake mechanisms exist, node-local
and gateway, chosen by where the checkpoint lives.

**Why (scale).** (a) pins every idle session to a node and its port, which
is the cost manifests exist to remove: an idle session in the bucket has no
node. (b) is the status quo and is what makes every platform build its own
proxy, the way Fly and Tensorlake did. The gateway also hosts the exec
plan's gateway phase, so one tier serves both.

## 13. Unplaceable work blocks under the key that blocked it, and only matching capacity wakes it

**Call.** Phase 3 makes an eval that cannot place `blocked` with a key
(node class, resource shape, checkpoint pin, or `escaped`), indexed in
leader memory, one per job. Node joins, uncordons, freed capacity,
attribute changes, budget reports, and warms wake only matching keys.

```console
$ legion job status agents -j
{"ok":true,"data":{"status":"blocked",
 "blocked":{"key":"class:pool=agents,cpu.arch=x86_64","shortfall":{"memory":"48GiB"}}}}
```

**Alternatives.**
- (a) Keep the cluster-wide capacity pass and add freed capacity as a
  trigger.
- (b) Retry failed evals on a timer with backoff.
- (c) Wait for the gang plan to build its blocked-eval queue and reuse it.

**Trade-off.** Evals that end `failed` today end `blocked`, which is a
visible change of default. Computing node classes adds work at
registration and on attribute changes. The escaped bucket means some jobs
still wake on everything.

**Why (performance, then simplicity).** (a) re-plans every short job in the
cluster on each event, so freed capacity on a dense sandbox fleet would turn
every suspend into a cluster-wide pass. (b) burns evals while nothing
changed and still waits a backoff when something did. (c) puts a generic
need behind a gang-specific plan. Nomad's blocked evals are the proven
shape, with escaped and missed-unblock tracking, and the gang plan becomes
a consumer instead of a second implementation.

## 14. Unplaceable evals end `blocked`, not `failed`, and the job reports it as a read overlay

**Call.** A `job_submit`, `job_update`, `job_reschedule`, or `job_scale`
whose plan finds no room is written `status = 'blocked'` with its key and
detail, in the settle that used to write `failed`. A job with a blocked
eval reports a `blocked { key, task, shortfall, message, since, evalId }`
block from `GetJobInfo` and `ListJobs`, and `status: blocked` where the
derived job status (job-status plan, merged alongside this work) would
read `pending`. A job with live allocations keeps `running`. Nothing is
stored on the job.

```console
$ legion job info agents -j
{"ok":true,"data":{"status":"blocked","blocked":{"key":"class:pool=agents",
 "task":"workspace","shortfall":{"cpu":"500MHz","memory":"3000MiB"}, ...}}}
```

**Alternatives.**
- (a) Store `blocked` on the job row (a control-lane write when an eval
  blocks and another when it wakes).
- (b) A `legion job status` verb, as the plan's console sketch shows.
- (c) A cluster-wide shortfall: the job's total unmet request against the
  class's free capacity.
- (d) Let `blocked` outrank `running` in the derived status, so any job
  with a waiting placement reads `blocked`.

**Trade-off.** `failed` no longer means "no room" for these evals, so a
script that read `eval.failed` as a capacity signal must read
`eval.blocked` or the job's `blocked` block (the upgrade note in the PR
says so). `shortfall` is what one placement asks for, which tells an
operator what kind of node to add but not how many. Rollout ticks and the
node-scoped passes keep their old outcome (entry 41).

**Why (simplicity).** The job row is control-lane state and the eval is
scheduler-lane state; (a) would make every block and wake a cross-lane
composite for a value derivable from one indexed read. There is no `job
status` verb today and `job info` is where a job's state already lives, so
(b) would have added a second command for the same record. (c) needs a
capacity model of the class the scheduler does not keep, and computing it
on every read of `ListJobs` is the cost a list should not pay. (d) would
make a 10,000-instance sandbox job serving traffic read `blocked` because
one create is waiting; the job-status plan's rule that live work reads
`running` is the right first answer, and the block carries the rest.

## 15. Eval claims are leader memory, not a raft entry

**Call.** Phase 1 removes the claim write. `claim_evals` marks evals in
the claiming manager's memory and subsumes their pending duplicates there;
the rows stay `pending`, and the settle deletes the eval and its
duplicates in one write. `get_evals` overlays `in_flight` on the leader.
A manager that steps down or starts forgets its claims.

**Alternatives.**
- (a) Keep `pending → in_flight` durable and fuse the claim into the
  enqueue when a worker is idle.
- (b) Keep the claim durable and rely on batching to amortize it, as
  before.

**Trade-off.** Another manager's `eval list` shows a claimed eval as
`pending`. A failover re-runs the evals the old leader had claimed, as
`requeue_in_flight_evals` did before; there is no durable record of which
evals were mid-flight, so `scheduler.recovered` no longer fires for them.
Two eval workers on one manager still never share an eval (one lock), but
the exclusivity is now process-local, which is only correct because one
manager runs evals at a time.

**Why (performance, then simplicity).** On an idle cluster every placement
paid the claim as an entry of its own: 1 of the 4 the Phase 0 count found
for "place". The only thing the durable claim bought was recovery, and an
unclaimed `pending` row gets the same recovery for free: the next leader
runs it, and handlers are idempotent reconciles, which the old recovery
also relied on. (a) still writes when the queue is idle, which is exactly
when the claim was a round of its own. "Place" went from 4 entries to 1
with this and entry 23 together.

## 16. Measure, then group commit, before any new semantics

**Call.** Phase 0 is a gate that measures entries per lifecycle, rates,
and bytes per row, and leaves an op-count test in CI. Phase 1 trims and
batches writes inside today's model: allocation health only on change,
coalesced creates and snapshot reports, fused events, ratification that
creates its own row and spends its ticket in one apply, and no claim and
settle for evals with nothing to place.

**Alternatives.** Skip to session mode, since only it removes per-session
writes. Or make raft itself faster (pipelining more than one proposal per
lane).

**Trade-off.** Two phases of work that do not change the asymptote. Phase
0 also needs loadgen work (an instance mode, simulated allocation health)
before it produces anything.

**Why (performance).** The static count found writes nobody had measured,
including a per-node, per-beat allocation-health write that grows with
sandboxes per node. Session mode would inherit some of these (envelopes
are allocations with health). Phase 1 helps every keyed user now, without
a new mode, and the eval-path plan already showed that fewer rounds beat
faster rounds on a serial lane.

## 17. Phase 1's rate target was missed at about 2×, and Phases 0–3 ship with the bound named

**Call.** Phase 1 met its entry targets (8 entries per lifecycle on an idle
cluster, down from 22, and about 3 per session under load) but not its
rate target. The sustained lifecycle ceiling on the substitute host went
from about 25 to about 50 per second, not the ≥ 5× (≈ 125) the plan set.
Phases 0–3 ship anyway, with the gap in §14, in `TODO.md`, and in the
public limit:

```text
docs/sandboxes-at-scale.md, "Limits to know about":
  One scheduler lane orders every placement ... about 50 lifecycles a
  second on the 4-vCPU reference host
```

**Alternatives.** (a) Hold the PR and widen the plan-apply group commit.
Concurrent plans already share an entry, but each sandbox placement is its
own eval, and they averaged 1.09 plans per entry here. More placements per
eval, or more evals in flight, would raise that. (b) Cut the per-entry cost:
pipeline more than one proposal per lane, or stop executing each write
twice (once to capture the changeset, once to apply it). (c) Restate the
target to match the result.

**Trade-off.** A platform that needs more than about 50 sandbox lifecycles
a second from one cluster on comparable hardware has to wait for that
follow-up or for session mode (Phase 6), which takes unnamed starts off
raft entirely.

**Why (performance, honestly measured).** Profiling at an offered 100/s
shows what bounds it. The manager used 1.1 of 4 cores. The busiest thread
was the scheduler lane's raft pipeline, at about 54%. Each entry costs
about 6 ms of wall time on that lane (changeset capture 3.2 ms, propose to
apply 2.7 ms). At about 160 entries a second and 3 entries per lifecycle,
that is about 52 lifecycles a second. `lanes` measures the same as
`single` (52.0 vs 51.8/s), because instance writes and plans share the
scheduler lane in both. The lane is latency-bound, not CPU-bound, so the
remaining lever is fewer or cheaper entries, which means changing the plan
apply's unit or raft's write path. Both are new semantics, past the "no new
semantics" scope of Phase 1, and each deserves its own measured change (the
plan apply has its own failure isolation to keep). Phase 0's gate exists so
that numbers like this reorder the plan instead of being hidden: (c) would
hide one. Placement latency on an idle cluster dropped too (p99 220 → 68 ms,
create to running), which is the number users feel below the ceiling.

## 18. Phase 6 ships running sessions and promotion, and defers idle sessions, identity, the session log, and edge starts

**Call.** Phase 6 shipped envelopes, the session router, node session
reports, the per-manager index, duplicate convergence, derived-id
idempotency, `PromoteInstance`, and the CLI, MCP, catalog, and docs
surface. It did not ship:
- suspending an unpromoted session to a bucket manifest (§6.5), so idle
  sessions do not exist yet;
- workload identity for sessions (§6.8);
- the node's session log and its shipper (§6.4, entries 32 and 37);
- edge-started sessions (§6.3, entry 30);
- `promote.onVolume` and promotion by `snapshot create --name` (§6.6);
- ports, services, and network policy on sessions;
- sessions on simulated workers, and a `legion-loadgen` session mode.

A session task that asks for any of them is refused at submit:

```text
Error: task workspace is a session task and declares identities. Workload identity for unnamed sessions is not available yet
Hint: remove identities, or use stateful.mode = "keyed"
```

**Alternatives.**
- (a) Hold the change until all of §6 is built.
- (b) Let `suspend` on a session promote it implicitly, then archive it.

**Trade-off.** Session mode today suits disposable units only. A sandbox
that must sleep has to be promoted first, which costs one entry and a row,
so the "hundreds of millions idle" target (entry 35) is reachable through
Phase 5's `manifestOnly` keyed instances, not through sessions. There is no
per-session audit trail, and platforms that need per-sandbox credentials
cannot use sessions.

**Why (simplicity).** Each deferral is a separable piece whose absence the
binary states (refused at submit, listed in `docs/session-mode.md`,
"Limitations"), so nothing half-works. Running sessions and promotion are
the core every deferred piece builds on, and they already deliver the
zero-entry start. (b) hides a raft write and a row behind a verb whose
point is to avoid them. Identity from leases moves a security boundary
(entry 33) and deserves its own review rather than riding this change.
Session suspend is the next piece, since waking idle sessions (Phase 7)
needs idle sessions to exist.

## 19. Sessions end when their worker restarts, and nothing is written to the node's disk

**Call.** The worker holds its sessions in memory. A worker restart ends
its unpromoted sessions; promoted sessions are allocations and are
re-adopted. Plan §6.4 and entry 37 had a `sessions.db` SQLite log on the
node, from which a restarted worker restores or kills sessions per their
gate.

**Alternatives.**
- (a) The SQLite session log of entry 37.
- (b) Record sessions in the JSON `WorkerStateStore`.

**Trade-off.** A worker upgrade, crash, or supervisor restart drops every
unpromoted session on that node, the same as losing the node.
`docs/session-mode.md` lists it under "Limitations", and `docs/upgrades.md`
tells operators to promote the sessions they need before a worker
downgrade.

**Why (simplicity).** Without session suspend (entry 18) a session cannot
outlive its process anyway, so surviving a worker restart would cover only
the minutes the worker is down, and the log's other purpose, a shipped
audit trail, is deferred with it. (b) is the whole-file rewrite per change
that entry 37 rejected. Writing nothing keeps the node's disk off the
start path. The log returns with session suspend, where a restart must not
lose an idle session's state.

## 20. Phase 0 was measured on a stated single-host substitute, not the AWS rig

**Call.** Every Phase 0 and later number in this PR comes from one 4-vCPU
Intel Xeon (2.1 GHz, 15 GiB, no KVM), a release build, a single-voter raft
manager (`single` topology), and `legion-loadgen --mode instances` with 50
embedded simulated nodes. Runtime boot and restore are simulated (no KVM),
so the §7.2 runtime rows record the control-plane share only. The op-count
test is exact on any host and runs both topologies.

**Alternatives.** The AWS rig (3 × m6id.xlarge, `lanes`), or skipping the
latency SLOs until the rig runs.

**Trade-off.** Absolute numbers are lower than the rig's and the loadgen,
the fleet, and the manager share four cores; a single voter pays fsync but
no network quorum, so raft latency is understated and the scheduler lane's
serial cost is the bound instead. Results are not comparable with AWS
cells.

**Why (simplicity).** The plan allows "the AWS rig or a stated local
substitute", and this session had no cloud access. The phase targets are
ratios on the same box (≥ 5× rate, ≤ 8 entries), which a single host
measures fairly when every phase runs on it in the same way, and the
entry counts that drive the rest of the plan do not depend on the host.

## 21. The blocked-eval index is the rows, and every replica keeps the capacity-event log

**Call.** Entry 9 put the key → eval index in leader memory, rebuilt from
rows on election. The implementation keeps no second index: the wake pass
reads the `blocked` rows (at most one per job) only when it has capacity
events to match. The events themselves are recorded by the changeset
projection on every manager, numbered, for five minutes. A manager that
takes the scheduler lane replays the last 60 seconds of them; a manager
process that starts as the scheduler's writer, with no log to replay,
returns every blocked eval to `pending` once (cause `startup`).

**Alternatives.**
- (a) The plan's leader-memory index from key to eval ids.
- (b) Replicate the capacity events, so a new leader has an exact log.
- (c) On election, wake every blocked eval (as on startup).

**Trade-off.** A pass reads every blocked row even when only one class
changed, which at 10^4 blocked jobs is a 10^4-row read per pass with
events (passes coalesce bursts to one per 50 ms). A restart costs one
re-plan per blocked job. Capacity that appeared more than 60 seconds
before an election and was never acted on is missed (a leader that lived
past it acted on it).

**Why (simplicity, then performance).** The rows are already replicated
and indexed by status; a memory index is a copy that must be kept
consistent with every write that blocks, replaces, deletes, prunes, or
wakes an eval, on every leader change. Matching is per task anyway (the
key string cannot express `>` or `matches`), so the index would only have
narrowed which rows to test. (b) adds a raft write per stop. (c) turns
every election into a re-plan of every blocked job; the replay window is
exact for the window that matters
(`tests/blocked_evals.rs::a_new_leader_rebuilds_from_rows_and_replays_a_missed_wake`).

## 22. Every phase carries p50 and p99 latency targets, measured first in Phase 0

**Call.** The plan sets latency SLOs (placement decision, dispatch, running
from cold boot and from restore, resume from local memory, published
memory, and disk, wake on request, and exec open) beside its throughput
targets, measured at p50 and p99 on an idle cluster and at 80% of the
throughput ceiling.

**Alternative.** Keep throughput and resident counts as the only targets,
as the scheduler benchmarks have, and treat latency as a side effect.

**Trade-off.** Phase 0 gets bigger, and several targets are set before any
measurement, from published numbers (Modal, Fly, Google, Tensorlake) that
were not taken on Legion's hardware. Some will be rewritten.

**Why (performance).** An agent platform's user feels the start and the
resume, not the cluster's placements per second, and the benchmark history shows the two moving apart: the 2026-08-04 AWS campaign cut plan p99 by
42% while throughput did not move (eval-path-raft-cost plan §1). A
phase that raises throughput by queuing longer would pass a
throughput-only bar.

## 23. An eval settles inside the plan that finishes it

**Call.** A handler whose plan is its last write arms its settle
(`arm_eval_settle`); the plan apply then deletes the eval row, deletes the
duplicates its claim subsumed, and records `eval.completed` in its own
entry. Today only `job_reschedule` without a scheduled batch follow-up
arms it, which is the placement path every keyed create and resume takes.
(2026-10-13: `job_submit`, `job_update`, and `job_scale` arm it too, and a
partial plan never settles. See
[multi-eval-plans calls](multi-eval-plans-plan_calls.md) #3 and #4.)

**Alternatives.**
- (a) A deferred settle piggybacked on the next write the scheduler lane
  makes.
- (b) Keep the separate settle and amortize it by batching.

**Trade-off.** Two code paths settle an eval (the fused one and the batch
settle), and the eval worker must not write a second outcome for a fused
one (`EvalCompletion::Settled`). A handler that arms and then writes again
after its plan would settle too early, so arming is opt-in per handler.

**Why (performance).** At true idle (a), still costs one entry per eval,
only later; (b) amortizes nothing when the queue holds one eval, which is
the steady state for interactive creates. Fused, a placement is one entry.

## 24. Stop and destroy write directly and queue no eval

**Call.** `StopInstance` and `DestroyInstance` remove the allocation and
change the instance row in one group-committed scheduler-lane write, and
enqueue no eval. Capacity they free reaches waiting work through the
blocked-eval wake pass (`freed` events).

**Alternatives.** Keep the eval (destroy, enqueue, claim, settle: the
Phase 0 count's 4 entries) and skip only its claim and settle when the
eval finds nothing to place, as the plan's Phase 1 bullet says.

**Trade-off.** The removal no longer passes through the eval loop, so a
job's other evals are not re-run by a stop; anything that relied on the
stop's eval to re-plan the job (a job short elsewhere) now waits for its
own blocked eval.

**Why (performance).** The eval had no placement work: removing an
instance never places anything. Skipping claim and settle would still have
paid the enqueue; writing directly makes destroy 1 entry instead of 4,
and Phase 3's wake pass already covers the one thing the eval could have
done for other jobs (use the freed room).

## 25. Unnamed sessions converge to one, and idempotency derives the id

**Call.** Two nodes may briefly run the same unnamed session id. The
leader stops the one with the later lease within a heartbeat. A create
with an `idempotency_key` derives its id as an HMAC of the accessor, job,
and key, so a retry lands on the same id without a raft record.

**Alternatives.** Record idempotency keys in raft for sessions too. Or
route every create for a key through the leader to dedupe in memory.

**Trade-off.** A retry that races to two nodes boots twice for up to one
heartbeat interval, and a side effect from the loser is possible. The
at-most-one guarantee is only for promoted sessions.

**Why (scale).** A raft idempotency record per create is a raft write per
session, which is what the mode exists to avoid. Leader dedupe works but
makes the leader a per-create hop. The derived id gives the agent the one
property it needs, the same id on retry, and a duplicate is rare and
bounded.

**As built (Phase 6).** The rule holds, but with a per-manager index the
heartbeat reply is one of three places a duplicate is stopped, and
convergence can take up to 20 seconds (entry 26).

## 26. Duplicate sessions are stopped at three points, not only by the index

**Call.** Of two copies of one session id, the one with the earlier
`leaseIssuedAt` (ties broken by node id) is kept, and the other is stopped
by:
1. the manager whose index holds both copies, in that node's heartbeat
   reply (`HeartbeatReply.stop_sessions`);
2. any manager that reads both copies while answering `get`, `stop`, or an
   idempotent create;
3. the manager that served an idempotent create, which looks again 20
   seconds later.

Plan §6.4 had only the first.

**Alternative.** Only the index, as planned.

**Trade-off.** Three code paths enforce one rule. When the two copies'
nodes report to different managers, only the second and third catch it, so
convergence can take up to 20 seconds rather than the one heartbeat that
entry 25 states.

**Why (correctness, forced by entry 6).** With a per-manager index, two
copies on nodes that report to different managers never meet in one index,
so the index alone would leave both running forever. The delayed check
bounds that case with no reader involved, and the read-time check closes it
sooner whenever someone looks.

## 27. The router picks the node with the most room, from a one-second view, and the node decides

**Call.** The session router ranks envelopes by free room, most first,
from a view of the cluster's sessions that it caches for one second and
updates with its own starts. It sends the start to the first node; the node
admits against its envelope and refuses when it is full, and the router
tries the next, up to three nodes. Plan §6.4 said first fit, preferring
nodes whose content store already holds the image or checkpoint.

**Alternatives.**
- (a) First fit with locality, as planned.
- (b) Read the cluster's view (with its peer fan-out) on every start.

**Trade-off.** Most-room spreads sessions, so no envelope empties for a
drain and a start may pull its image cold. Two managers can pick the same
emptiest node within a second, and the loser pays a refusal and a retry.
After three refusals the create fails:

```text
no node accepted the session (w-03: envelope full: 0 cpu free, the session needs 500; ...); retry once sessions end, or raise stateful.envelope.resources
```

**Why (performance, then simplicity).** Reading on every start would put a
round trip per peer (entry 6) on the hot path; a second of staleness costs
at most a retry, because the node is authoritative and never overfills.
Concurrent routers using first fit all pick the same first node; most-room
keeps them apart. Locality matters for restores, and sessions do not
restore yet (entry 18).

## 28. No external session registry, not even as a seam

**Call.** Option 6 is rejected as a default and is not built as a plugin
seam in this plan.

**Alternative.** A `SessionRegistry` plugin with DynamoDB, FoundationDB,
or Spanner backends, keeping raft as the control plane only. GKE's move to
a Spanner-backed store is the strongest argument for it.

**Trade-off.** A platform that already runs FoundationDB cannot point
Legion at it. If the node-owned design fails at some scale, there is no
escape hatch short of new work.

**Why (simplicity, extensibility deliberately closed).** Legion's pitch is
one binary. A registry backend makes correctness depend on a system the
operator must run or rent, and a seam built before a second implementation
exists usually fits the first one only. The node-owned design needs no
external store beyond the content store that published checkpoints already
require.

## 29. Cells are deferred behind a measured trigger

**Call.** Key-range sharding of instance rows across raft groups is Phase
7, built only when Phase 0's harness shows the scheduler lane above 70%
busy from instance writes, or instance rows above 4 GiB of store.

**Alternative.** Build cells now, as the general answer to raft limits,
on top of the multi-raft lanes that already run several groups.

**Trade-off.** If promoted instances grow faster than expected, cells
arrive late. The thresholds are guesses until Phase 0 replaces them.

**Why (simplicity).** After Phases 4 to 6, raft holds envelopes and named
instances, which grow with users who name things, not with sessions.
Cells add scatter-gather lists, cross-cell renames, and per-cell
leadership to every operator's mental model. That cost is worth paying
only with evidence.

## 30. Prefix tickets stand in for leases at the edge, and spend once

**Call.** An edge-started session uses a multi-use, instance-prefix boot
ticket as its authorization. The node mints the session id. The ticket is
spent per ticket, not per session.

**Alternative.** Keep one single-use ticket per session, so every session
has a manager-recorded capability.

**Trade-off.** A compromised edge node holding a prefix ticket can start
any number of sessions under that job until the ticket expires. Audit
links sessions to the ticket `jti`, not to a per-session mint.

**Why (scale).** A single-use ticket is a control-lane write to mint and
another to spend, two raft writes per session. Edge boot already allowed
prefix and multi-use tickets and already accepted "runs anything a live
ticket names" as the edge blast radius.

## 31. The ticket spend rides the ratification, and an edge start can be the create

**Call.** `boot_ticket` moves to the scheduler lane. A ratification spends
its ticket, creates the keyed instance when the ticket names an id that has
never existed, places it, and records `provisional.ratified`, all inside
one savepoint of one group-committed entry. A refused ratification
therefore leaves a single-use ticket unspent.

**Alternatives.**
- (a) Keep the spend on the control lane and add a spend coalescer.
- (b) Keep requiring `CreateInstance` before an edge start.

**Trade-off.** A behavior change: before, a refused ratification still
burned the ticket. Under `lanes` the ticket table now follows scheduler
leadership. A tombstoned id is still refused (`instance_claimed`), so
"never existed" means no row at all.

**Why (performance, then simplicity).** (a) leaves two serial commits per
start on two lanes. One entry is what the plan's Phase 1 target asks for
(an edge start is one scheduler-lane entry plus the status projection),
and the double-spend refusal still holds because the spend is a
conditional update inside the same transaction
(`tests/edge_boot_integration.rs::a_ticket_is_spent_only_by_a_ratification_that_commits`).
(b) costs a create and its placement eval per start, the very writes edge
boot was meant to skip.

## 32. Per-session events stay off the replicated journal

**Call.** Session lifecycle events go to the live `StreamEvents`
broadcast and to the node's session log, shipped in batches to the bucket.
Only promotion, envelope changes, and misconfiguration refusals are
journaled.

**Alternative.** Journal every session event, as keyed instances do.

**Trade-off.** `legion events` after the fact does not show a given
unnamed session's history. An investigator reads the shipped log or the
node, which is slower. A node that dies before shipping loses its last
interval of log.

**Why (scale).** The journal keeps about 10,000 rows. At 20,000 session
starts per second it would roll over in under a second, so journaling
costs raft writes and still loses the record. The shipped log is the
honest audit trail at that rate.

## 33. Session workload identity comes from the lease, and the boundary moves to the envelope

**Call.** The manager issues tokens for a session after verifying the
node, the lease (or ticket), and the envelope allocation, not an
allocation row per session. `legion.io/alloc` names the envelope and
`legion.io/instance` the session.

**Alternative.** Promote any session that asks for a workload identity,
so tokens always come from a row.

**Trade-off.** A compromised node with a prefix ticket can mint identity
tokens for session ids it invents under that job. Today it can mint them
only for allocations placed on it. The default `sub` has no instance, so
federation policies keyed on `sub` see no change, but per-instance
policies on unnamed sessions are weaker.

**Why (scale, with the security cost stated).** Most agent sandboxes need
a credential, so promote-on-identity would put nearly every session back
in raft. The blast radius matches what edge boot already accepted, and
promoted sessions keep today's boundary for workloads that need it.

**As built (Phase 6).** Not built: a session task may not declare
`identities`, and the lease is a start time rather than a signed token
(entries 18 and 34).

## 34. The session lease is its start time, not a signed JWT

**Call.** A session carries `leaseIssuedAt`, the router's start time in
milliseconds, which orders duplicates. Plan §6.3 specified a JWT lease
signed with the workload-identity key.

**Alternative.** The signed JWT of §6.3.

**Trade-off.** Nothing proves after the fact which credential authorized a
session, and a node could report any start time. A node is already trusted
to run what it reports, so nothing is lost today, but identity from leases
(entry 33) needs the signed form back.

**Why (simplicity).** The JWT's only reader was identity issuance, which
is deferred (entry 18). Duplicates need an order, not a capability, and
signing a token per start would be a key-use path nothing reads.

## 35. The target is millions running and hundreds of millions idle per cluster, not billions running

**Call.** The plan's per-cluster targets are at least 10^6 running
sessions and 10^8 idle ones, with billions reached through federation.

**Alternative.** Set a billion-session target for one cluster to match
the claims that prompted the plan.

**Trade-off.** The plan's headline is less exciting than the claims it
answers.

**Why.** A billion running sessions at about 10 MiB is about 10 PB of
RAM. The research found no primary source for "billions of concurrent
agents". The largest primary claims are Cloudflare's tens of millions of
agents and Google's tens to hundreds of millions of instances, mostly
idle. A target the design can be measured against beats one it cannot.

## 36. Snapshot-clone density before a sub-VM tier

**Call.** Phase 8 densifies with one microVM per session sharing pages
from a warm base checkpoint (option 8). Phase 9 adds only a seam for
many sessions per VM (option 7), with no runtime.

**Alternative.** Ship a process or WASM tier inside a pooled VM first,
which gives 10 to 100 times more sessions per node.

**Trade-off.** Tier 1 density is bounded by per-VM overhead (about 5 MiB
of VMM plus the guest's private pages), so some workloads that would fit
in an isolate pay for a VM.

**Why (simplicity and security).** Legion's sandbox users run arbitrary
Linux code, and a shared kernel is a different security promise. Tier 1
keeps the promise and reuses fork machinery that already shares pages on
Firecracker. Tier 2 belongs to a runtime plugin that can make its own
isolation claim.

## 37. The worker's session log is a new SQLite database, not the JSON state file

**Call.** Sessions are recorded in `sessions.db` on the worker. The JSON
`WorkerStateStore` keeps allocations only.

**Alternative.** Extend `WorkerStateStore`, which already holds the
restart record.

**Trade-off.** The worker has two local stores to back up and reason
about.

**Why (performance).** `WorkerStateStore::save` rewrites the whole file
on every change (`src/worker/state.rs:151`). At 10,000 sessions per node
with churn that is quadratic I/O. Fly's `flyd` and Nomad's client state
both use an embedded database for this reason.

**As built (Phase 6).** Not built: the worker keeps sessions in memory
and a restart ends them (entry 19). The reasoning here stands for when the
log comes back with session suspend.

## 38. A streamed allocation starts through the push path, and everything else through the reconcile

**Call.** The worker starts an allocation that a delta added at once, by
calling its own `PostWork` handler (identity delivery, launch, refreshers,
exactly as a push). Stops, payload changes, and resyncs wake the heartbeat
loop, whose reconcile runs against the stream's mirror instead of a desired
set in the reply. A resync's additions always go through the reconcile.

**Alternatives.**
- (a) Wake the heartbeat for every message and let the reconcile start
  new work too.
- (b) Apply every delta directly (start, stop, update) and drop the
  reconcile for streaming workers.

**Trade-off.** Two paths start allocations on a worker, as push and
heartbeat already did. The direct start skips an allocation the task
manager already runs, launches, stops, or holds back, and one whose task
has an unratified provisional start on the node (a payload names no
instance, so two instances of one task are not told apart and the direct
start defers to the reconcile).

**Why (performance, then simplicity).** (a) costs a heartbeat round trip
per placement before the start, which is what "within one round trip"
excludes. (b) reimplements the reconcile's stop ordering, restore
replacement, and protected-claim logic. Starting new work is the latency
that matters, and it is exactly what the push path already does.

## 39. Edge-boot provisional deltas are deferred

**Call.** Phase 2 does not move manager-side speculative dispatch and its
ratification payload onto the stream; they stay `PostWork` pushes, and an
edge start's verdict stays the `RatifyClaim` reply.

**Alternative.** The plan's provisional delta and verdict on the stream.

**Trade-off.** Speculative dispatch still needs the worker listener and a
push per claim, and the stream and the push can race for the same work
(handled by the guard in entry 38).

**Why (simplicity).** The edge-boot suites pass unmodified this way, which
is one of Phase 2's acceptance tests, and the verdict path is already one
RPC. Moving it is a protocol change to a security-relevant flow that
deserves its own change. Open in TODO.md.

## 40. A streaming heartbeat carries a position, a digest, and changed health only

**Call.** A worker on the stream sends `work_stream { epoch, seq, digest }`,
no `allocation_ids`, and only the allocation health that changed since a
reply that said `desired_via_stream`; any other reply makes the next beat
report everything again. The digest is `count:xor(splitmix64(id))`, kept
incrementally on both sides. A mismatch at the same `seq`, with nothing in
flight, queues one resync.

**Alternatives.**
- (a) Keep sending allocation ids (the plan says "only when they change").
- (b) A cryptographic digest (sha256 over the sorted ids).

**Trade-off.** The xor digest can collide by accident (two different id
sets with the same count and xor), which a cryptographic hash would make
negligible; a collision hides drift until the next real change. The
manager still makes a few per-node reads on a steady beat that do not grow
with the allocation count (checkpoint requests, content resync), so the
"0 store reads" target holds for the desired set, not for every read.

**Why (performance).** The manager ignored the reported ids already
(`build_desired_allocations` never read them), so (a) was bytes for
nothing. (b) needs the sorted set on every check; the xor updates in O(1)
per delta on both sides. Measured: 42-byte request and 7-byte reply at 300
allocations against 5,154 and 102,670 bytes for the full set.

## 41. Only job evals that place work block

**Call.** `job_submit`, `job_update`, `job_reschedule`, and `job_scale`
block. A `rollout_tick` that cannot place still fails its rollout, and the
node-scoped passes keep queuing a per-job `job_reschedule` for each job
they could not place, which is what blocks.

**Alternative.** Block every eval kind whose error carries
`PlacementBlocked`.

**Trade-off.** A rollout that runs out of room still ends `failed` and
needs an operator.

**Why (simplicity).** A rollout tick stamps its rollout `failed` before
returning, so a blocked tick would wake to a terminal rollout and do
nothing; changing rollout semantics is outside this plan. Node-scoped
evals carry no job, so they cannot be one-per-job, and their per-job retry
already lands in the right place.

## 42. The wake pass matches constraints, not capacity, and budget reports do not wake

**Call.** An event wakes a blocked eval when the node is available and the
blocked task's permit and constraints accept it (`node_could_help`), or
the key is `escaped`, or the node is the one a `node:` key names. Whether
the node has enough free room is left to the plan that runs next.
Checkpoint budget reports and `snapshot warm` completions are not events.

**Alternatives.** Check the freed resources against the task's request
before waking; add budget reports as events.

**Trade-off.** A small freed allocation on a big node of the right class
wakes a blocked eval that then blocks again: one wasted plan. An eval
blocked on checkpoint room waits for a store change on a node (an
allocation leaving it) rather than for the worker's report that its disk
freed up.

**Why (simplicity).** The plan is the only place capacity is computed
correctly (reservations, preemption, ports, disk); a second, approximate
check in the wake pass would drift from it, and a false negative would
strand work, which is worse than a wasted plan. Budget reports are worker
heartbeat data, not committed changes, so they would need a second event
source. Open in TODO.md.

## 43. Simulated workers send allocation health and stream by default

**Call.** `--simulate` nodes report every acknowledged allocation as
healthy, every beat off the stream and once on it, and take their work
over `StreamWork` unless `--sim-no-work-stream` is set.

**Alternative.** Flags defaulting off, to keep historical AWS cells
comparable.

**Trade-off.** Heartbeat-path load in scale runs from before this change
is not comparable with runs after it; `docs/worker-simulation.md` says so.

**Why (simplicity).** Real workers do both. The per-beat health write was
invisible to every scale run because the simulator never sent health (plan
§5.1), which is how it went unmeasured. A simulator that is faithful by
default is the one the next regression shows up in.

## 44. `allocation_health.updated_at` and `task_status.updated_at` mean "last changed"

**Call.** Health and status upserts write only when a value changed, so
their `updated_at` is when the value last changed, not when it was last
reported.

**Alternative.** Keep a "last reported" timestamp current on every beat.

**Trade-off.** Anyone reading `updated_at` as freshness (via `legion state
query`) now sees an old timestamp for a healthy allocation. Node liveness,
not these rows, is the freshness signal.

**Why (performance).** Writing a timestamp is what made every beat a
10,000-row changeset on a dense node; a steady beat now writes nothing.

## 45. One generic scheduler-lane group commit for instance operations

**Call.** Creates, stops, starts, destroys, resumes, suspends, forks,
ratifications, and snapshot reports all go through
`Store::scheduler_group_commit`: concurrent requests share one entry, each
inside its own savepoint, so a failing request rolls back alone
(`schedulerSettings.instance_group_commit`, default on).

**Alternative.** One coalescer per operation, as job intake and heartbeats
have.

**Trade-off.** Unrelated operations share a batch, so one slow request
delays the others in its batch.

**Why (performance, then simplicity).** The scheduler lane is one serial
pipeline whatever the op, so mixing ops is what fills batches under a real
mix (creates arriving while resumes report); per-op coalescers would split
the same traffic into smaller batches. One mechanism is one place to get
savepoint isolation right.

## 46. Op counts sum every lane's applied index, and the measured lifecycle is an instance of a running task

**Call.** `tests/raft_lifecycle_ops.rs` asserts entries per stage as the
sum of every lane's raft applied index, and prints
`legion_raft_ops_total` op names alongside. Its stages measure an instance
of a task that already runs one instance; the task's first placement (its
`task_status` flip to running) is reported as a stage of its own.

**Alternative.** Assert on the metric alone; measure a job's first
instance.

**Trade-off.** The applied index also counts entries the test did not
cause (none on an in-process store, but a background loop would show up).
The first-instance cost (3 entries) is not in the lifecycle total.

**Why (simplicity).** The metric is process-global, so parallel tests
would bleed into it; the applied index is per store and exact. Platforms
run thousands of instances of one task, so the steady-state lifecycle is
the number that scales.

## 47. The keyed-pending capacity bug was fixed in Phase 0, and the capacity pass stays as a backstop

**Call.** Phase 0's test confirmed the bug (a keyed task's `count` is
stored as 1, so only tasks with zero allocations were retried) and the fix
landed with it, in `job_configs_waiting_on_capacity`'s SQL. Phase 3 keeps
the `node_join` capacity pass, but it skips jobs with a pending or blocked
eval.

**Alternative.** Remove the capacity pass now that blocked evals exist.

**Trade-off.** A registration or uncordon still queues one cluster-wide
eval, which reads every active job; it re-plans only jobs short without an
eval, which is normally none.

**Why (simplicity).** A job can be short with no eval at all (a crash
between an instance row and its eval, a store migrated from an older
release), and nothing else would ever look at it. The pass costs one read
per join burst; dropping it trades that for a class of permanently stuck
jobs.

## 48. The raft seam test still parses only `store.rs` and `acl_store.rs`

**Call.** `tests/raft_seam.rs` was updated for the ops this work removed
(`claim_eval{,s}`, the `complete_*_and_claim_next` pair) and added
(`complete_evals`), but its file list was left alone, so the new
`unblock_evals` op in `store/blocked_evals.rs` is not classified there.

**Alternative.** Extend the test to every file under `src/storage/store/`.

**Trade-off.** Ops in the store's submodules (`fork_ops`, `keyring_ops`,
`snapshot_ops`, `suspend_ops`, and now `blocked_evals`) escape the
classification check; the capture-time ownership guard still rejects a
write on the wrong lane at runtime.

**Why (simplicity).** Extending it means classifying about a dozen
existing ops from earlier work, which is its own change. Open in TODO.md.

## 49. The manager reaches the bucket with the worker type's credential, or its own

**Call.** The manager reads, lists, and deletes archive manifests itself.
It uses the credential a worker type's `contentStore.auth.kv` names when
the URL is under that store, and its ambient credentials (environment, web
identity, instance profile) otherwise.

**Alternatives.**
- (a) Ask a worker to read and delete on the manager's behalf.
- (b) A separate manager-side bucket configuration.

**Trade-off.** The manager reads a KV secret that was written for workers,
and a cluster that granted bucket access to workers only finds archived
resumes, `--include-idle`, and destroys failing on the manager with an
access error.

**Why (simplicity).** Resume, idle listing, and destroy of a
`manifestOnly` instance happen when no worker holds the instance; (a)
needs a new worker RPC and a rule for which worker to ask. (b) is a second
place to configure the same bucket. The manager already uses its own
identity for streaming persistence to S3.

## 50. Archive manifests are keyed by job, group, task, and id, and an id-only lookup probes the candidate jobs

**Call.** A manifest lives at
`<contentStore.url>/manifests/<job>/<group>/<task>/<instance>.json`. A
`manifestOnly` instance has no row, so `start`, `get`, and `destroy` find
it by probing one object per job that has a keyed task of the id's task
name with `durability = "store"`, in each content store the worker types
name. Plan §6.5 put manifests at `sessions/<job>/<id-prefix>/<id>.json`.

**Alternatives.**
- (a) The plan's path.
- (b) An index from id to job, as an object or a row.

**Trade-off.** A lookup of an unknown id costs one object read per
candidate job per content store, and two jobs with the same task name
double it. A lifecycle rule on `manifests/` expires every job's idle
instances alike unless the operator writes one per job prefix.

**Why (simplicity, performance).** A minted id names its task, so the task
narrows the candidates to a handful of jobs with no index; (b) is a write
per archive, which archiving exists to avoid. A per-job prefix lets
`--include-idle` list one job with a prefix listing. The prefix is
`manifests/` rather than `sessions/` because keyed instances use it first.

## 51. `instance list -j` returns an object, so idle listings can page

**Call.** Phase 5 changed the data from a bare array to
`{ "instances": [...], "nextIdlePageToken": "..." }`, and Phase 6 added
`complete` and `asOf`. `--include-idle` pages the bucket with
`--idle-page-token`, 1,000 per page by default and at most 10,000.

**Alternative.** Keep the array, and list bucket instances with a separate
`instance list-idle` command.

**Trade-off.** A breaking change: scripts reading `.data[]` now read
`.data.instances[]`, as `docs/upgrades.md` says.

**Why (simplicity).** One listing covers every state an instance can be
in, and the object has room for the fields session mode needed anyway.
`AGENTS.md` prefers a cleaner surface to a compatibility shim.

## 52. The upgrade marks every existing instance named, so only new mints go untombstoned

**Call.** The migration that adds `instance.named` sets it on every
existing row, including tombstones of ids minted by older releases
(`<task>-<16 alphanumerics>`).

**Alternative.** Mark rows unnamed when their id looks minted, and delete
old minted tombstones in the migration.

**Trade-off.** A cluster that already churned through minted ids keeps
those tombstones; its store shrinks only as instances minted afterwards
replace them.

**Why (simplicity, safety).** Old minted ids are 16 characters, so the
130-bit argument that lets a new mint skip its tombstone does not cover
them; their tombstones are what keeps them unique. "An upgrade never
releases an id" is a rule a reader can check in one sentence.

## 53. A fork of an unnamed instance is unnamed, with a minted id of its own

**Call.** Forking an unnamed instance mints a fresh `<task>-<26 base32>` id
for each fork instead of `<origin>-f<n>`, and the fork is unnamed: destroy
deletes its row.

**Alternative.** `<origin>-f<n>`, as for named instances.

**Trade-off.** A fork's id no longer shows its origin; the
`instance.forked` event names both.

**Why (scale).** `<origin>-f<n>` is predictable, so it would need a
tombstone to stay unique, which brings back a row per destroyed fork, and
forks of sandboxes are a churn case.

## 54. `suspend.archive` needs a published memory checkpoint and no reserve

**Call.** Submit refuses `archive` unless `durability = "store"`,
`checkpoint = "memory"`, and `suspend.reserve` is unset:

```text
Error: task "workspace" sets suspend.archive = "slim", but its checkpoints stay on the node that took them (durability = "local")
Hint: set suspend.durability = "store" so the checkpoint is published to the worker's contentStore before it is archived
```

**Alternative.** Accept the combinations and archive only what can be,
silently.

**Trade-off.** A disk-checkpoint task cannot archive at all.

**Why (simplicity).** A disk checkpoint stays in the node's volume and is
never published, so there is nothing to put in a manifest, and an archived
instance holds no capacity anywhere, so a reserve would hold room for
nothing. Refusing at submit says so once, instead of an archive that never
happens.

## 55. Ending a session writes nothing, and a retried end is answered from a per-manager record

**Call.** `instance stop` and `instance destroy` on a running session are
the same: the session ends on its node and nothing is written. The manager
that ended it remembers the id for 10 minutes, so a retry answers
`Session <id> already ended` rather than `NOT_FOUND`.

**Alternatives.**
- (a) Record the end in raft, as a tombstone.
- (b) Answer `NOT_FOUND` to every retry.
- (c) Share the record across managers.

**Trade-off.** A retry that reaches another manager, or comes after 10
minutes, gets `NOT_FOUND`, so an agent must treat `NOT_FOUND` on an end as
done. `docs/session-mode.md` says so.

**Why (scale, then ease).** (a) is a write per session. (b) makes the
common retry, on the same connection to the same manager, look like a
failure. (c) adds a replication path for a convenience.

## 56. `instance start` on a running session is a no-op, and verbs that need a row say "promote first"

**Call.** `StartInstance` on a running session answers that it is already
running, as for a running keyed instance. `suspend`, `fork`,
`snapshot create`, `snapshot restore`, and `replace` refuse a session with
`FAILED_PRECONDITION`, saying the id is a running session and to promote it
first with `legion instance promote <id>`. An id with neither a session nor a row stays `NOT_FOUND`.

**Alternative.** `NOT_FOUND` for any verb on an id without a row, which is
what the handlers answered before.

**Trade-off.** Each verb that reads a row looks for a session before its
not-found answer, which costs a fan-out read on that error path.

**Why (ease).** An agent that reads `NOT_FOUND` forgets the sandbox; one
that reads the hint promotes it and retries. A start that succeeds on a
running unit is what every other mode does.

## 57. Only minted-shaped ids are looked up as sessions, and batch creates are refused on session tasks

**Call.** A session lookup returns nothing at once for an id that is not
`<task>-<26 base32>`, with no peer fan-out, so `instance get user-42`
costs what it did. `CreateInstance` with `count` on a session task is
refused:

```text
task workspace is a session task: unnamed instances are sessions, started one per CreateInstance call without an id. Use count only for keyed tasks, or list instance_ids to create named instances
```

**Alternatives.** Look up every id; route `count` through the router as
N starts.

**Trade-off.** A platform starting many sessions makes one call each.

**Why (performance, simplicity).** Every session id is minted (a derived
idempotent id has the same shape), so the shortcut loses nothing and keeps
the fan-out off named instances. A batch reply is a list of rows created
in one entry, which sessions do not have, and its partial-failure shape
does not fit starts that fail node by node.

## 58. Phase 6 is tested with fake nodes on real gRPC, not the planned 10^4-session `raft_lanes` run

**Call.** `tests/session_integration.rs` runs a real raft-backed manager
against fake worker gRPC servers that admit, refuse, adopt, and report
sessions, and counts raft entries as the applied index summed over lanes:
zero for unnamed starts, one for a promotion. An ignored
`session_start_latency_measure` starts 4,000 sessions on 16 fake nodes.
The plan's §10 asked for 10^4 starts on simulated nodes and a leader-kill
test.

**Alternative.** The plan's harness, which needs simulated workers to run
sessions (deferred, entry 18).

**Trade-off.** Nothing tests more than one manager: the peer fan-out and
the rebuild after failover are covered by unit tests of merge and ingest
only, and §13's "complete within one full-report period after a leader
kill" is not measured. Both are open in `TODO.md`.

**Why (simplicity).** A fake node is the smallest thing that speaks the
real worker protocol, so the test exercises the real router, index,
heartbeat ingest, and promotion apply without a runtime.

## 59. Session mode's user documentation is drafted in the plan, not published

**Call.** The documentation for `stateful.mode = "session"` (when to use
it, what it gives up against `keyed`, its use cases with sandboxes among
them, and the paragraph the sandbox guides gain) is written now as plan
§15, ready to move into `docs/` unchanged when Phase 6 ships. What is
public now is what exists: `docs/sandboxes-at-scale.md` and the "Choosing
a mode" section of `docs/stateful-workloads.md`, covering `count`, `slot`,
and `keyed`.

**Alternatives.**
- (a) Publish the session-mode page now, marked "planned".
- (b) Write nothing about session mode until Phase 6.

**Trade-off.** A reader of the public docs cannot learn today that a
session mode is coming or what it will cost them; the owner asked for the
trade-offs to be clear, and they are clear only in the plan until Phase 6.

**Why (simplicity).** `AGENTS.md` keeps promises of unshipped work out of
`docs/`: a page describing a mode the binary rejects would be read as a
feature that exists. (b) would leave the trade-off discussion to be
reconstructed at Phase 6 from code, after the decisions it should have
shaped. Writing the public text now, in the plan, settles the trade-offs
before the code and costs only a move when it ships.

**Outcome (Phase 6).** Published as `docs/session-mode.md`, plus the
"Choosing a mode" row and the guide paragraphs, rewritten to what shipped
rather than moved unchanged: §15 described idle sessions, identity, and an
event log that Phase 6 deferred (entry 18), and the public page states
those as limitations instead.

## 60. The AWS sandbox campaign embeds its nodes on the load box and labels every projection

**Call.** `benchmarks/aws/scripts/run-sandbox-campaign.sh` runs on the rig
with no spot fleet: `legion-loadgen --mode instances` embeds its simulated
nodes (checkpoint channel faked) on the load box, against the real
3-voter control plane. It climbs a rate ladder and stops at the first rung
that does not hold, then resets the control plane and holds a density cell
on fewer, denser nodes. `analysis/sandbox_report.py` reports measured
numbers and, separately, projections (lifecycles per day, cost per million
lifecycles, resident capacity per manager), each with the measured number
it scales and its assumption.

**Alternatives.**
- (a) Run the sim nodes on the spot fleet, as the job campaigns do.
- (b) Add a KVM-capable (bare-metal) worker so sandbox boot and restore are
  real.
- (c) Report only measured numbers, no projections.

**Trade-off.** The load box runs the fleet and the load, so a very high
rung can be bound by the load box rather than the control plane (the
report's busiest-manager CPU column is how to tell). Boot and restore are
simulated, so every latency is the control plane's share. Projections can
be quoted out of context.

**Why (simplicity, then cost).** The owner asked for a baseline on a few
real servers that can run on its own. The control plane is what the
sandbox-scaling work changes, and the embedded fleet speaks the real
protocol over the real network, so four on-demand boxes (about $1.07/hour)
measure it; (a) adds a fleet and its quota for no extra signal, and (b)
costs a metal instance (about $4/hour) to measure the runtime, which is a
separate number (Phase 8 measures density per node). (c) would leave the
owner to extrapolate by hand, which is where unlabelled numbers come from;
the report does it once, with the basis printed beside every figure. Open
in TODO.md: run it on AWS.
