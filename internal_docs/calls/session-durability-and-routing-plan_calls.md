# Judgement Calls: Session Durability, Routing, and Edge Starts

Calls made in [`plans/session-durability-and-routing-plan.md`](../plans/session-durability-and-routing-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Managers leave the start path, and a picker outside Legion chooses the node

**Call.** For high start rates, a platform's own load balancer or router
picks a node per start from `ListSessionNodes` (a consistent-hash ring in
the common case) and calls the node's `Worker.StartSession` with a
session ticket. The node admits or refuses on its own; managers learn of
the session from heartbeats. No manager handles the start, and nothing is
ratified.

```console
$ legion session nodes agents -j          # polled by the picker, not per start
$ legion session start agents --node https://10.0.1.7:4848 --ticket "$T" -j
```

**Alternatives.**
- (a) Keep every start on a manager and scale managers: a release build,
  more managers, cheaper routing.
- (b) Ship a picker inside Legion (`legion start edge-router`) holding the
  ring.
- (c) Let any node accept any start and forward it node to node to one
  with room.

**Trade-off.** The highest-rate path needs a component Legion does not
ship, configured by the platform. Admission is only exact per node: two
pickers can race for a node's last slot and one is refused and hops.
A leaked session ticket starts sessions until it expires (entry 6).

**Why (performance, then simplicity).** Every manager-routed start costs a
manager some CPU and an mTLS call, so the rate is bounded by managers
however fast each one gets, and a few managers cannot carry tens of
thousands a second with headroom. With the node as the only party on the
path, the rate grows with the fleet. Consistent-hash balancers are
commodity (Envoy ring_hash and Maglev, HAProxy and nginx consistent hash)
and already sit in front of most platforms, so the "outside logic" is
configuration. (a) stays as the default for low rates and is measured and
tuned in the same phase. (b) is a later plan if platforms ask; shipping
it now would put Legion in the business of an L7 proxy. (c) adds a hop to
every refused start and node-to-node trust the worker plane does not have.

## 2. Durability lives on the node, and raft gets no per-session rows by default

**Call.** Sessions survive a worker restart through a node-local
`sessions.db`, and sleep on their node's disk. Raft stores nothing per
session in Phases 1 to 5.

```text
$ legion instance start workspace-q4n7c2rd.9m3x... -j
{"ok":false,"error":{"code":"NOT_FOUND","message":"session ... was evicted from w-17 at ... (disk pressure); a local-only session cannot be restored", ...}}
```

**Alternatives.**
- (a) The proposal that prompted the plan: managers batch each session's
  location into a raft table, eventually consistent, and route from it.
- (b) A raft row per session written synchronously at start, which is
  keyed mode.

**Trade-off.** A session on a node that is down and never returns is
lost, and while the node is silent its sleeping sessions are missing from
listings (`complete = false`). A replicated directory would at least list
them.

**Why (performance, then simplicity).** Today's gap is that a session is
a process in memory, so a worker restart ends it; a raft row pointing at
it would not have saved it. Batching makes (a) cheap in entries (one per
manager per second) but not in rows: every manager stores every
node-held session and every snapshot carries them, which at the plan's
10^8 idle target is past sandbox-scaling §6.11's 4 GiB trigger. The node
already knows exactly what it holds. (a) stays as Phase 4 behind a
measured trigger (plan §6.8).

## 3. The ring places, the id routes

**Call.** A picker's consistent-hash ring decides only where a new
session starts. Every later request for the session is addressed by its
id (tag, then the picker's `id → node` cache, then the manifest or a
manager), never by re-hashing the user's key. `ListSessionNodes` feeds two
structures: the ring (accepting nodes) and the tag map (every node with
an envelope).

**Alternatives.**
- (a) Route every request by hashing the user key, as a stateless sticky
  balancer would.
- (b) Route every request through a lookup (index or directory).

**Trade-off.** Pickers keep a small map beside the ring, and a platform
that hashes keys for follow-up requests is wrong by construction (it gets
`wrong_node`, plan §6.7 row 4).

**Why (simplicity, performance).** Under (a), adding one node moves ~1/N
of all live sessions' traffic to a node that does not hold them, so every
scale-out is a partial outage. Splitting placement from addressing makes
ring changes affect only where new sessions go, which is the property
consistent hashing was meant to buy. (b) puts a lookup back on every
request.

## 4. Moves are fenced by a generation claimed with a conditional write to the session manifest

**Call.** Every resume of a `store` session claims generation g+1 by an
`If-Match` write to its manifest before its gate opens. When two copies
exist, the higher generation wins, then the earliest lease. A stale copy
ends itself with `superseded` when its own conditional write fails, or is
stopped by a manager that sees both.

**Alternatives.**
- (a) Keep sandbox-scaling's earliest-lease-wins rule.
- (b) Fence moves through raft (one entry per move) or a manager-held
  lease.

**Trade-off.** Moves depend on the bucket supporting conditional writes
(S3 does, with ETag preconditions; `file://` in tests). A bucket outage
blocks moves (plan §6.7 row 12). In a full partition two copies still run
until the stale one next touches the bucket or a manager, or fences itself
(entry 5 bounds it).

**Why (performance, correctness).** (a) keeps the stale copy after a
move, because it has the earlier lease. (b) adds a write or a manager to
the edge resume path for every move. The manifest is already the record
of a `store` session and already lives in the bucket, so a conditional
write there is the cheapest authority that exists.

## 5. No split brain: duplicates are prevented by conditional writes where possible, and converged by four overlapping mechanisms with a stated bound

**Call.** Every way two copies of a session can exist is either prevented
(a `store` idempotent start creates the manifest with `If-None-Match: *`;
a move or resume claims a generation with `If-Match`) or converged: the
stale copy fails its own next conditional write, its node reads a fence
marker from the bucket every 30 s, managers sweep newly reported derived
ids across peers once a second in one batched call, and a node cut off
from both managers and the bucket for `stateful.fenceAfter` suspends its
`store` sessions. The survivor is the higher generation, then the earlier
lease, then the lower node id, so after a move the old copy always stops.

**Alternatives.**
- (a) Strict at-most-one for every session: a raft entry (or a per-session
  bucket lease renewed by the node) before any copy runs.
- (b) Converge only through managers' indexes, as #30 does today.
- (c) Self-fence every session, `local` ones included, when a node loses
  managers.

**Trade-off.** Not at-most-one at every instant: in a full partition a
stale copy can act on the outside world for up to `fenceAfter` (or one
fence-list interval when it can reach the bucket). An idempotent `store`
start pays one bucket write in its latency.

**Why (correctness at no per-start raft cost).** (a) is keyed mode, and a
bucket lease per session is a renewal per session per interval, millions
of requests a minute at the target. (b) misses duplicates whose nodes
report to different managers and never converges while managers are down.
The four mechanisms each end a duplicate alone and each fails
differently, so together they leave only the "cut off from everything"
case, which self-fencing bounds. (c) would end `local` sessions during a
manager outage although a `local` session can never have moved, trading
availability for nothing. Promotion stays the answer for at-most-one.

## 6. A session ticket names no instance, prefix, or job config, and is not spent per session

**Call.** `legion boot-ticket create <job> --task <task> --scope session`
mints a JWT that lets its holder start sessions of that task on nodes in
its pool, unlimited uses, until it expires. The node mints the id and
uses the job config of the envelope it holds.

**Alternatives.**
- (a) Edge boot's existing rule: every ticket names an instance or a
  non-empty prefix, pinned to an exact job config, minutes long.
- (b) One single-use ticket per session.

**Trade-off.** A wider capability than any edge boot ticket: a leak lets
the holder fill the task's envelopes until the ticket expires, with
revocation by TTL only. Audit ties a session to the ticket's `jti`, not to
a per-session grant.

**Why (performance).** (b) is a mint per start, which puts the manager
back on the path. (a)'s prefix exists to scope keyed ids, which sessions
do not have, and pinning a job config would break every ticket on each
deploy. The envelope already bounds what any holder can start, so the
ticket's blast radius is the envelope's, the same trade sandbox-scaling
calls entry 30 accepted for prefix tickets.

## 7. The node is in the session id

**Call.** A minted session id carries an 8-character keyed hash of its
start node: `workspace-q4n7c2rd.7k2qd9x4m1b8c3rj5t0v6wye2h`. Any manager
routes `get`, `stop`, `start`, and `promote` to that node with no lookup.

**Alternatives.**
- (a) Keep the per-manager index plus peer fan-out for every lookup (today).
- (b) Route from a replicated directory (entry 2's alternative).
- (c) Tag with the envelope allocation id, which is already a raft row.

**Trade-off.** Ids grow by 9 characters, and the shape changes, which
breaks any platform that parsed it. A session resumed on another node
keeps a stale tag and pays the fan-out. Idempotent creates cannot be
tagged, because their id is derived before placement.

**Why (performance, simplicity).** The tag is exact at mint and costs no
write, so the common read is one worker-plane call instead of one per
manager. A collision or stale tag costs a probe, never a wrong answer,
because the node answers for itself. (c) goes stale on every job update
that replaces envelopes in place.

## 8. Running sessions move only after a probe; idle ones move at once

**Call.** A resume of a running session on another node first probes the
tagged node and asks it to stop. It moves without that only when the probe
fails and managers mark the node down, or the caller passes
`--force-move`. Idle sessions move immediately.

**Alternative.** Move on the first connect failure for every session.

**Trade-off.** A running session on a dead node takes the probe timeout
plus manager detection before it comes back elsewhere.

**Why (correctness).** Moving an idle session is always safe, because the
conditional write fences any later resume of the old copy. Moving a
running one on a picker's connect failure turns every picker-to-node blip
into two live copies with real side effects. The probe from a second
vantage point, plus the managers' view, narrows that to full partitions.

## 9. Periodic checkpoints and checkpoint-on-stop are per-job options, off by default

**Call.** `suspend.checkpointEvery` (optional) and `suspend.onWorkerStop
= "keep" | "checkpoint"` (default `"keep"`) are set in the job. Periodic
checkpoints are staggered, skipped rather than queued when a node falls
behind, and published with a conditional write that doubles as a fence.

**Alternatives.**
- (a) Always checkpoint periodically for `store` sessions.
- (b) Always checkpoint on worker stop.

**Trade-off.** A job that sets neither loses a running `store` session's
state since its last suspend if its node dies, and a worker stop whose
processes do not survive (a `KillMode=control-group` unit, a host
shutdown) ends `"keep"` sessions.

**Why (performance, then simplicity).** Until diff snapshots, a periodic
checkpoint is a full memory image per session per interval: at 10^5
sessions of 512 MiB every 10 minutes that is about 90 GB/s of capture and
upload cluster-wide, which only some workloads should pay. A checkpoint
at stop delays every worker stop by up to its timeout. Both are the right
call for some jobs and the wrong default for disposable ones, which are
most sessions.

## 10. The node's start path is fixed first, before edge starts

**Call.** Phase 1 takes the runtime launch out from under the worker's
single task-manager lock, stops the per-start fsync of the JSON state
file for sessions, and bounds launch concurrency with a retryable
refusal. Edge starts come in Phase 4.

**Alternative.** Ship edge starts first, since they remove the visible
~1k/s manager limit.

**Trade-off.** The headline change (no manager on the path) lands three
phases later.

**Why (performance).** Read on `main` after #30, a node serializes every launch
behind one lock (`src/worker/task_manager.rs:4550`, `:2796`), so its rate
is about one over the launch time. Edge starts with that lock in place
would move the ceiling from a few managers to each node, at roughly 30
starts a second per node for a 30 ms launch, and the target needs the
per-node number. Phase 1 also speeds the manager-routed path, so it pays
off on its own.

## 11. Sessions still refuse network volumes, and a volume still means promotion

**Call.** Session tasks keep refusing `volumes`. A session that needs a
network volume is promoted (one raft entry), and the keyed path moves it
on node loss.

**Alternative.** Let unpromoted sessions attach network volumes and move
them with the generation fence.

**Trade-off.** A platform that wants a per-session network disk pays a
promotion per such session.

**Why (correctness).** The generation fence stops a stale *compute* copy
eventually; it cannot stop two copies writing one block device during a
partition. Safe re-attach needs an owner that declares the old node lost
first, which is the durable record sessions give up. A session's own disk
travels in its checkpoint under `durability = "store"`, which covers the
common case.

## 12. Session tasks may suspend with `durability = "local"`

**Call.** Relax the submit refusal at `src/structs/session.rs:408`: a
session may sleep on its node only, pinned until `suspend.keepFor`
(default `24h`) or disk pressure evicts it.

**Alternatives.**
- (a) Require `durability = "store"` for session suspend, as
  sandbox-scaling §6.2 does, so no idle session is ever pinned to a node.
- (b) Keep refusing suspend and make platforms promote.

**Trade-off.** A local-only idle session is lost with its node or on
eviction, and its checkpoint takes node disk that would otherwise be
cache.

**Why (simplicity, performance).** The motivating case is "come back to
the same sandbox while its node is still there", which needs no bucket.
A bucket is extra infrastructure and an upload per suspend; `store`
stays available for platforms that want node-loss survival, and the
`evicted` answer makes the loss explicit rather than silent.

## 13. An evicted or unreachable session is answered before its job's ACL scope is known

**Call.** `instance get`, `instance start`, and `instance suspend` on a
session id answer NOT_FOUND "evicted" or UNAVAILABLE "node X holds session
Y" after only the handler's coarse check (`job:read` or `job:write` on any
job), because the job is on the node that evicted the session or cannot
be reached.

```text
NOT_FOUND: session workspace-q4n7c2rd.9m3x... was evicted from w-17 at 2026-10-12T09:14:03Z (keepFor expired); a local-only session cannot be restored
Hint: start a new session, or set suspend.durability = "store" so idle sessions are kept in the content store
```

**Alternatives.**
- (a) Answer plain NOT_FOUND unless the caller's precise scope can be
  checked, losing the eviction account.
- (b) Have the node return the job id with its evicted and absent answers
  so the manager can enforce the precise scope first.

**Trade-off.** A caller with access to some job can learn that a session
id it already holds was evicted from, or lives on, a named node. It
learns nothing it did not send except the node name and eviction time.

**Why (simplicity, ease).** The id already carries its node's tag, so the
node name is no secret from anyone holding the id, and session ids carry
130 random bits, so they cannot be enumerated. (b) is the right follow-up
when the evicted row grows a job field on the wire (Phase 5b touches the
same answer); (a) would turn the plan's headline failure answer into the
"never existed" one it exists to replace.

## 14. The worker now handles SIGTERM and SIGINT, and exits 0 after its stop checkpoints

**Call.** Every worker installs a SIGTERM/SIGINT handler. It checkpoints
the running sessions whose task sets `suspend.onWorkerStop =
"checkpoint"` (least recently checkpointed first, until
`edgeBoot.stopCheckpointTimeout`), flushes `sessions.db`, and exits 0.
Workloads are left running, as before. On restart, a session whose
process died meanwhile and that has a checkpoint is restored from it.

**Alternatives.**
- (a) Install the handler only when some running session asks for it.
- (b) Stop the sessions after checkpointing them, so a restart always
  restores.

**Trade-off.** A worker stop that used to die by the signal now exits with
status 0, which a supervisor that inspects the exit status sees
differently. A restore after a stop that the processes survived never
happens: the live process wins, so a checkpoint taken at stop is wasted
work for every session that outlives the worker.

**Why (simplicity, performance).** One handler is easier to reason about
than one that appears and disappears with the sessions on a node, and it
costs nothing when no session asks for checkpoints. (b) would turn every
worker restart (an upgrade, a config change) into a restore of every
session, which is slower and lossier than re-adopting the live process,
for the benefit of the host-reboot case only.

## 15. Edge idempotency is node-local, and derived ids stay untagged

**Call.** An edge start with an idempotency key derives the untagged
session id on the node and dedupes against the node's own claim table.
Pickers are told to hash on the idempotency key, so retries reach the
same node.

**Alternatives.**
- (a) A tagged derived id, so even idempotent sessions route by tag.
- (b) Record keys centrally, which needs a manager or raft per start.

**Trade-off.** Idempotent sessions keep the fan-out path for `get`/`stop`
through a manager. A `local` retry that crosses a ring change briefly runs
a second copy until the managers' sweep stops it; a `store` one is
prevented by the manifest create (entry 5).

**Why (simplicity).** A tag in a derived id would differ per node, so a
retry on another node would start a differently named duplicate that
nothing converges. Untagged, the id is the same everywhere, and the
existing converge-to-one rule covers the rare cross-node retry.

## 16. Phase 2 group-commits the session log instead of fsyncing each start

**Call.** `sessions.db` flushes every 50 ms on a writer thread
(`src/worker/session_log.rs`, `GROUP_COMMIT`).

**Alternative.** Fsync every start before acknowledging it.

**Trade-off.** A session started in the last 50 ms before a host crash is
not on disk; on a host crash its process is gone anyway. *Deviation in
#36:* the plan also had a restarted worker adopt live session processes
the log missed. That is not built: such a process (a worker crash, not a
host crash, inside the 50 ms window) keeps running unreported until it
exits. Finding it would mean scanning the host's processes for Legion's
environment markers, and the window is small. The public doc states it
(`docs/session-mode.md`, "Worker restarts").

**Why (performance).** The start path is the feature's whole point
(about 2.5 ms p50 in sandbox-scaling §16.3); a per-start fsync would
roughly double it on ordinary disks for no durability gain.

## 17. A session suspends only when its task has a `suspend` block, and `store` is refused at the node

**Call.** `instance suspend` on a session routes to its node only when its
task sets `suspend`; without one it keeps answering FAILED_PRECONDITION
"promote it first". A task with `suspend.durability = "store"` passes
submit, but its node refuses to suspend an unpromoted session with
FAILED_PRECONDITION until Phase 5b.

```text
FAILED_PRECONDITION: task workspace sets suspend.durability = "store", which unpromoted sessions do not support yet. Use durability = "local" (the checkpoint stays on this node), or promote the session first: `legion instance promote workspace-q4n7c2rd.9m3x...`
```

**Alternatives.**
- (a) Suspend every session with defaults when asked, as keyed instances
  do without a block.
- (b) Refuse `store` on session tasks at submit.

**Trade-off.** A job with `durability = "store"` submits and then cannot
suspend its sessions, which is discovered at the first suspend.

**Why (simplicity).** A session with no `suspend` block has no `keepFor`
and no idle policy, and a suspend that pins node disk for a default 24h on
a task that never asked for it would be a surprise. (b) would be a submit
refusal that Phase 5b removes again, and a job written for `store` keeps
working once it lands; the node's answer says what to do now.

## 18. A suspended session stays in the session index as `suspended`, with no envelope usage

**Call.** The node keeps reporting a suspended session on its heartbeats,
with `state = "suspended"`. The managers' index lists it (so `get`,
`list`, and `stop` find it), and counts it in `Usage.sessions` but adds
none of its resources to the envelope's used room.

**Alternatives.**
- (a) Drop suspended sessions from the report and answer them only by
  asking the node.
- (b) Count them in neither the session count nor the amounts.

**Trade-off.** `ListSessionNodes` and `legion session nodes` show a session
count that includes sleeping sessions, which can read as busier than the
node is.

**Why (ease).** `instance list` must show suspended sessions or users lose
track of them, and the router's room arithmetic is driven by amounts, so
leaving the count alone kept the index change to one function
(`session_amounts`).

## 19. An `exec` session's checkpoint is a disk one, so its resume is a fresh start, and `checkpointEvery` needs memory

**Call.** A session suspends with its task's checkpoint kind. For `exec`
(and anything but `sandbox`) that is a disk checkpoint, which holds the
spec only, so resume boots the same spec under the same id.
`checkpointEvery` is refused at submit unless the checkpoint is a memory
one; `onWorkerStop = "checkpoint"` is accepted on any runtime.

**Alternatives.**
- (a) Refuse `suspend` on session tasks that cannot take a memory
  checkpoint.
- (b) Accept `checkpointEvery` everywhere and capture whatever the kind
  holds.

**Trade-off.** An `exec` session "resumes" without its process state, the
same as a keyed `exec` instance does.

**Why (simplicity).** Suspend on `exec` still frees envelope room for an
idle session and keeps its id, which is most of the value; keyed
instances already work this way, so sessions match them. A periodic disk
checkpoint captures nothing that changes, so (b) would be pure cost.

## 20. Periodic captures run one at a time, and the skip rule uses a moving average of capture time

**Call.** The node runs a `checkpointEvery` round every 5 seconds from
its session maintenance loop: due sessions sorted most overdue first, a
session skipped (and counted) when the backlog before it would exceed one
interval, captures run sequentially, and the per-capture time is an
exponential moving average of the last captures.

**Alternatives.**
- (a) Run captures concurrently, bounded by the launch pool.
- (b) Queue every due capture and never skip.

**Trade-off.** A node with many sessions and a short interval checkpoints
fewer of them per interval than it could with parallel captures on fast
disks.

**Why (performance, simplicity).** Captures compete for the same disk and
memory bandwidth as the sessions they pause, so parallel captures mostly
lengthen each pause. (b) is the plan's rejected "queue, don't skip": a
backlog that only grows.

## 21. Idle sessions are suspended from the usage report, so idle detection needs resource enforcement

**Call.** The node feeds sessions (under their negative local keys) into
the same usage sampler and idle policy as keyed instances, takes their
entries back out of the report before it is sent, and suspends those past
`idle.after`.

**Alternative.** A separate idle tracker for sessions that does not need
cgroups.

**Trade-off.** On a node without resource enforcement, `idle.after` never
fires for sessions (manual suspend still works).

**Why (simplicity).** The idle signals (CPU, network bytes, open exec
sessions) are the ones keyed instances use and are documented once; a
second tracker would drift from them. Production session nodes run with
enforcement anyway, since the envelope is only a promise without it.

## 22. A session ticket is a boot ticket row with gate `session`

**Call.** Session tickets reuse the boot-ticket store: the row's gate
column holds `"session"` to mark the scope (`SESSION_TICKET_GATE` in
`src/manager/edge_boot.rs`), and `ListBootTickets` reports `scope`
from it.

**Alternative.** A new `scope` column (a store migration) or a separate
table.

**Trade-off.** The gate column means two things. A future third scope
needs the column after all.

**Why (simplicity).** No migration and no new listing; the gate column
was unused for a ticket that has no gate.

## 23. Full session reports are not chunked; the manager's decode limit is raised instead

**Call.** The manager accepts messages up to 64 MiB
(`MANAGER_MAX_DECODE_BYTES`), which holds a full report of about 200,000
sessions per node.

**Alternative.** Chunk full reports across heartbeats (plan §8 item 2).

**Trade-off.** One node with more sessions than that cannot report them
all. No envelope realistically holds that many.

**Why (simplicity).** Chunking needs a multi-part epoch protocol on both
sides; the raised limit covers every envelope size we can imagine for
now. It is open in TODO.md.

## 24. Envelope payloads carry the session task's spec

**Call.** `TaskAllocation.session_task_spec` (the session task's config
as JSON) and `session_task_config_id` ride on every envelope payload, so a
node builds an edge start without a manager.

**Alternative.** A separate spec fetch by the node on its first edge start
(a manager call), or the edge-boot spec cache keyed by job config.

**Trade-off.** Every envelope payload grows by one task config.

**Why (simplicity, performance).** The spec is always current with the
envelope the start lands in, and the first edge start is as fast as the
hundredth.

## 25. The node tag is an unkeyed hash of the node id

**Call.** `node_tag` is the first 40 bits of SHA-256 over a fixed prefix
and the node id, in base32.

**Alternative.** A keyed hash (a cluster secret), or a random tag stored
on the node row.

**Trade-off.** Anyone who can guess node names can match a session id to
its node. The tag is not a secret, and the worker plane authorizes every
call anyway.

**Why (simplicity).** Every manager and every picker computes it from the
node list with no shared secret and no store write.

## 26. `ListSessionNodes` reads fresh usage on each call

**Call.** Each call reads the session index's usage across managers
(`QuerySessions` with `usage_only`), not the router's one-second cache.

**Alternative.** Serve from the router's cached room view.

**Trade-off.** One peer query per call; a picker polling every few
seconds per manager is cheap, a picker polling per start is not (the doc
says to poll).

**Why (simplicity).** The list is advisory and polled; a separate cache
with its own staleness rules buys nothing at that rate.

## 27. Balancer weights are clamped

**Call.** `--format envoy-eds` clamps a node's weight to 1..128 and
`--format haproxy` to 1..256, and a node not accepting keeps weight 1 and
is marked `DRAINING` or `disabled`.

**Alternative.** Scale weights proportionally across the list.

**Trade-off.** Two big nodes that both fit more than 128 sessions look
equal to Envoy.

**Why (simplicity).** Each balancer's own range, and a node with room for
over 128 sessions is not the one to steer away from.
