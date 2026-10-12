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
A leaked session ticket starts sessions until it expires (entry 5).

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
until the stale one next touches the bucket or a manager.

**Why (performance, correctness).** (a) keeps the stale copy after a
move, because it has the earlier lease. (b) adds a write or a manager to
the edge resume path for every move. The manifest is already the record
of a `store` session and already lives in the bucket, so a conditional
write there is the cheapest authority that exists.

## 5. A session ticket names no instance, prefix, or job config, and is not spent per session

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

## 6. The node is in the session id

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

## 7. Running sessions move only after a probe; idle ones move at once

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

## 8. The node's start path is fixed first, before edge starts

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

## 9. Sessions still refuse network volumes, and a volume still means promotion

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

## 10. Session tasks may suspend with `durability = "local"`

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

## 11. Edge idempotency is node-local, and derived ids stay untagged

**Call.** An edge start with an idempotency key derives the untagged
session id on the node and dedupes against the node's own claim table.
Pickers are told to hash on the idempotency key, so retries reach the
same node.

**Alternatives.**
- (a) A tagged derived id, so even idempotent sessions route by tag.
- (b) Record keys centrally, which needs a manager or raft per start.

**Trade-off.** Idempotent sessions keep the fan-out path for `get`/`stop`
through a manager, and a retry that crosses a ring change briefly runs a
second copy until convergence stops it.

**Why (simplicity).** A tag in a derived id would differ per node, so a
retry on another node would start a differently named duplicate that
nothing converges. Untagged, the id is the same everywhere, and the
existing converge-to-one rule covers the rare cross-node retry.

## 12. Phase 2 group-commits the session log instead of fsyncing each start

**Call.** `sessions.db` flushes every 50 ms, and a restarted worker also
adopts live session processes the log missed.

**Alternative.** Fsync every start before acknowledging it.

**Trade-off.** A session started in the last 50 ms before a host crash
(not a worker restart) is not on disk; on a host crash its process is gone
anyway.

**Why (performance).** The start path is the feature's whole point
(about 2.5 ms p50 in sandbox-scaling §16.3); a per-start fsync would
roughly double it on ordinary disks for no durability gain.

