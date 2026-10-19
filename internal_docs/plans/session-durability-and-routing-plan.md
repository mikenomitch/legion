# Plan: Session Durability, Routing, and Edge Starts

> **Judgement calls:** [`calls/session-durability-and-routing-plan_calls.md`](../calls/session-durability-and-routing-plan_calls.md)

**Status: IN EXECUTION** in [#36](https://github.com/mikenomitch/legion/pull/36):
Phases 1–4 and Phase 5a (sessions sleeping on their own node with
`durability = "local"`, [session-mode.md](../../docs/session-mode.md#idle-sessions))
are built and tested; Phase 5b (`store` sessions: publishing, generations
claimed with conditional manifest writes, moves, the fence list, and
self-fencing,
[session-mode.md](../../docs/session-mode.md#moving-a-session)) is built and
unit-tested on the `file://` backend, with real-S3, `sandbox`-memory, and
edge-probe runs still open in `TODO.md`; Phase 6 stays conditional. Deviations are recorded in the
calls doc. Written 2026-10-12. Builds on session mode from
[sandbox-scaling-plan.md](sandbox-scaling-plan.md) Phase 6, shipped in
[#30](https://github.com/mikenomitch/legion/pull/30); `src/` citations
are at `main` after that merge (`4a2fee3`). This
plan replaces the parts of sandbox-scaling §6.3, §6.4 and §6.5 that its
Phase 6 deferred (calls entries 18 and 19 there) with a narrower order:
let a node start sessions concurrently, survive a worker restart, route
by id without a lookup, start sessions with no manager on the path, then
sleep on the node. The start-rate goal
is tens of thousands of starts a second per cluster, reached by taking
managers off the start path: a picker outside Legion (a platform's load
balancer or edge router, with a consistent-hash ring) chooses the node,
and Legion supplies the node list and the authorization (§6.4).
Deliberately deferred and named: a replicated session directory in raft
(Phase 6 here, built only past a trigger), workload identity for sessions
(sandbox-scaling §6.8), a picker shipped inside Legion (§6.4, calls entry
1), and waking sessions from a gateway, which
[wake-on-request-plan.md](wake-on-request-plan.md) Phase 3 owns and which
this plan feeds.

---

## 1. Problem statement

Session mode buys a start with no raft entry by keeping nothing about a
session anywhere but the process that runs it. That makes a session less
durable than it needs to be, in three ways a platform builder hits on day
one:

- **A worker restart ends every unpromoted session on the node**, even
  though the node, its envelope, and the processes themselves are fine.
  The worker holds sessions in memory only (sandbox-scaling calls entry 19;
  `docs/session-mode.md:371`), while ordinary tasks already outlive a worker
  restart and are re-adopted by pid (`docs/architecture-managers-workers.md:234`).
  A rolling worker upgrade is therefore a rolling session massacre.
- **A session cannot sleep.** Suspend, fork, and snapshot are refused on
  an unpromoted session (`docs/session-mode.md:139`), and a session task
  may not use `suspend.durability = "local"` at all
  (`src/structs/session.rs:408`). An agent sandbox that is idle for an
  hour either burns its envelope share the whole hour or is thrown away.
  "Come back to the same sandbox later" means promoting it, which is the
  raft write session mode exists to avoid.
- **Finding a session costs a cluster-wide fan-out.** Each manager indexes
  only the sessions of nodes that heartbeat to it
  (`src/storage/session_index.rs:1-14`), so `get`, `stop`, `start`, and an
  idempotent create on an id this manager does not index ask every peer
  (`src/manager/sessions.rs:71`, `:173`). A node silent for 120 s drops out
  of the index (`src/storage/session_index.rs:45`), and its sessions answer
  `NOT_FOUND`, indistinguishable from an id that never existed.
- **Every start goes through a manager.** The router builds a room view,
  ranks envelopes, and calls the chosen worker
  (`src/manager/sessions.rs:477-606`), and edge starts are refused
  (`docs/session-mode.md:365`). Starts scale with how fast managers can
  route, not with the fleet. The one measurement, about 970 starts a
  second, ran one manager, 16 fake nodes and the load generator on four
  cores in a debug build (sandbox-scaling §16.3), so it is a floor, not a
  ceiling, but no amount of tuning puts a few managers in front of tens of
  thousands of starts a second with headroom. Platforms that start a
  sandbox per agent turn or per tool call need that rate at peak.

The question that prompted this plan was whether to write each session's
location to raft in batches, eventually consistent, so sessions get basic
durability and can be woken and routed back to their node if it is still
there. The answer this plan gives: the location is the cheap part, and it
can be carried in the id for free. What is missing is a session that still
exists to be routed back to, and that has to live on the node.

## 2. Prior art

- **Nomad client state.** The Nomad client keeps a local BoltDB of its
  allocations and task handles, and a restarted client reattaches to
  running tasks instead of restarting them. Legion's worker already does
  this for allocations through its JSON `WorkerStateStore`.
  - **Take:** a node-local record is what makes work survive the agent's
    restart. Raft is not involved.
- **Kubernetes kubelet.** Containers keep running across a kubelet
  restart, and the kubelet rebuilds its view from the runtime (CRI) and its
  checkpoint files.
  - **Take:** rebuild from what the runtime still holds, verify, adopt.
- **Fly Machines and `fly-proxy`.** A stopped or suspended Machine stays on
  its host and starts there again on the next request; a Machine on a dead
  host is gone unless it had a volume snapshot. Fly's routing state is
  replicated by gossip into SQLite on every proxy (Corrosion), eventually
  consistent, rather than through a consensus store.
  - **Take:** sleep on the host, wake on the host, and accept that the
    host's loss is the session's loss for the cheap tier.
  - **Take, for Phase 6 only:** if a cluster-wide directory is ever needed,
    it is a routing hint that tolerates staleness, not a record of truth.
- **Cloudflare Durable Objects.** An object takes a location hint when it
  is first created and lives in one place from then on; requests are
  routed to that home rather than to wherever the caller happens to be.
  - **Take:** decide the home at creation and make it cheap to find from
    the id. This plan goes one step further and puts the home in the id.
- **Consistent hashing in load balancers.** Envoy's ring-hash and Maglev
  load balancers, HAProxy's `balance hdr(...)` with `hash-type consistent`,
  and nginx's `hash ... consistent` all pick a backend from a key with
  minimal reshuffling when backends come and go. Google's Maglev paper
  describes the table; "consistent hashing with bounded loads" (Mirrokni,
  Thorup, Zadimoghaddam, used in HAProxy and Vimeo's proxy) caps any one
  backend's share and spills to the next on the ring (HAProxy's
  `hash-balance-factor`).
  - **Take:** the picker is commodity infrastructure a platform already
    runs. Legion's job is a backend list such a balancer can consume and a
    node that refuses cleanly when full, so the balancer can spill.
- **Legion's own edge boot.** Boot tickets already authorize a start on a
  node with no manager round trip, verified offline against the JWKS
  (`docs/edge-boot.md`, "Verification on the worker is entirely
  offline"). What it still pays per start is a ratification, one
  scheduler-lane entry, because an edge-booted keyed instance must become a
  row.
  - **Take:** the ticket and the offline check. **Reject** the per-start
    ratification for sessions, which have no row to ratify.
- **Borg and Omega.** Cells keep task state on the Borglet, and the master
  rebuilds from Borglet reports after failover.
  - **Take:** the node is the authority for what it runs; the control plane
    holds a rebuildable view. Session mode already follows this
    (sandbox-scaling calls entry 1).

## 3. Jobs to be done, by persona

**Platform builder (job author)**
- When a user comes back to an agent sandbox after a coffee break, I want
  them to land in the same sandbox with its files and processes, so the
  agent does not start over.
- When a sandbox has been idle for a while, I want it to stop costing CPU
  and memory without losing it, so I can afford to keep thousands of them
  around per node.
- When I upgrade the Legion worker, I want running sandboxes to keep
  running, so an upgrade is not an outage for my users.
- When a node dies, I want to be told clearly that its sandboxes are gone,
  so I can start fresh ones instead of retrying forever.

- When a launch or a traffic spike sends tens of thousands of new
  sandboxes a second, I want them to start from my own load balancer
  straight onto the machines, so the control plane is never the thing
  that queues my users.
- When a user's requests keep coming, I want each one to land on the same
  machine as their earlier sandboxes, so the cached image and checkpoint
  are already there.

**Cluster operator**
- When the start rate climbs, I want manager CPU and raft traffic to stay
  flat, so adding nodes is the only knob I turn.
- When session counts grow, I want the managers' disk and replication load
  to stay flat, so sessions cannot take the control plane down.
- When I roll workers, I want sessions to survive the same way tasks do,
  so the runbook has one rule.

**Autonomous agent**
- When I hold a session id from an earlier call, I want one call that
  either resumes it or tells me exactly why it cannot (`evicted`,
  `node lost`), so I can decide whether to retry or recreate without
  guessing.

*Not in scope:* the security admin persona. Nothing here changes who may
start, stop, or wake a session; the ACL scopes of session mode stay as
they are.

### User scenarios

1. **Back after lunch.** An agent sandbox goes idle for 3 minutes and the
   node suspends it to local disk, freeing its envelope share. Forty minutes
   later the platform calls `legion instance start <id>`. The manager reads
   the node from the id, the node restores from its local checkpoint, and
   the sandbox is back with its memory intact. No raft entry was written at
   any step.
2. **A worker upgrade.** The operator restarts the worker on `w-17`, which
   runs 800 sessions. The worker comes back, reads its session log, finds
   every process still alive, and re-adopts them. The managers see the
   sessions again on the first full report.
3. **A launch burst.** A platform fronts 2,000 session nodes with its
   Envoy fleet, ring-hashing on the user id. Envoy's endpoint list comes
   from `legion session nodes agents -j`, refreshed every few seconds. At
   peak, 40,000 starts a second arrive; each Envoy sends the start straight
   to a node's worker port with the platform's session ticket, and each
   node admits about 20 a second against its envelope. A node that is full
   answers `RESOURCE_EXHAUSTED` with its room, and Envoy retries the next
   host on the ring. No manager handles a start; managers see the sessions
   on the nodes' next heartbeats, and raft writes nothing.
4. **A dead node.** `w-17` loses power. A platform calls `start` on one of
   its idle sessions. The manager answers `UNAVAILABLE` with "node w-17 is
   down; sessions held only on it are lost unless it returns". With
   `durability = "store"`, the manager instead finds the session's
   manifest in the bucket and resumes it on another node.

## 4. Goals and non-goals

**Goals**
- A node starts sessions concurrently, so the per-node rate is set by the
  runtime and the host's cores, not by a lock or an fsync (Phase 1).
- Unpromoted sessions survive a worker restart (Phase 2).
- Any manager routes a session id to its node with no index lookup and no
  peer fan-out in the common case (Phase 3).
- Session starts with no manager on the path, at a rate that grows with
  the node count: a target of 50,000 starts a second across 2,000 nodes,
  with manager CPU and raft entries flat as the rate climbs (Phase 4).
- A node list and refusal shape that an off-the-shelf consistent-hash load
  balancer can drive, so the "picker" a platform runs is configuration,
  not code (Phase 4).
- The manager-routed path stays, measured in a release build across three
  managers, and fixed where the measurement points (Phase 4).
- Unpromoted sessions can suspend on their node and resume there, with
  `durability = "local"` (node-held, lost with the node or on eviction) or
  `"store"` (also published, resumable anywhere) (Phase 5).
- Every failure in the catalog of §6.7 (node loss, partitions, ring
  changes, racing resumes, a bucket or manager outage) has a defined
  outcome, a test, and an answer the caller can act on.
- Every answer about an unreachable session says why: `evicted`,
  `node lost`, `node unreachable`, or `not found`.
- Zero raft entries per session for start, suspend, resume, and stop,
  unchanged from session mode.

**Non-goals**
- **A raft row per session**, batched or not, in the default path. Phase 6
  describes one, gated behind a measured trigger (§6.8).
- **A picker inside Legion.** Legion does not ship a load balancer or a
  ring; it publishes what one needs. A built-in edge router is a later
  plan if platforms ask for it.
- **Exact global admission.** Two pickers can each send a node its last
  slot; the node refuses one. Capacity is arbitrated by the node, never
  across nodes.
- **Surviving node loss without a published checkpoint.** A session held
  only on a node dies with it; that is what makes it cheap.
- **At-most-one copy.** Converge-to-one stays (sandbox-scaling calls
  entry 25). Promote for exactness.
- **Gateway wake on request.** Owned by wake-on-request Phase 3; this plan
  gives it a node to route to.

## 5. Background: what exists today

- **Starts go through a manager.** The router picks the envelope with the
  most room from a one-second view built from raft allocation rows and the
  merged session index, then calls the worker's `PostWork`
  (`src/manager/sessions.rs:369-407`, `:477-606`). Edge starts are refused
  (`docs/session-mode.md:365`).
- **The session index is soft, per manager.** Nodes report deltas against
  an epoch with a full report every 30 beats
  (`src/storage/session_index.rs:16-24`). Reads merge this manager's index
  with every peer's (`src/manager/sessions.rs:71-150`).
- **Ids are minted with no record.** `{task}-{26 base32}`, 130 random bits
  (`src/structs/instance.rs:296`); `is_minted_instance_id`
  (`:310`) gates every session lookup, so a caller id never fans out.
  Idempotent creates derive the id from the key
  (`src/structs/session.rs:135`).
- **Start on a running session is a no-op**, and on an unknown id is
  `NOT_FOUND` (`src/manager/server.rs:6050-6064`).
- **The worker holds sessions in memory.** Ending one is routine
  (`src/worker/task_manager.rs:3008`); a session whose envelope leaves the
  node is ended after 30 s (`:355`). Nothing is written to disk.
- **Allocations survive a worker restart** through the JSON
  `WorkerStateStore`, re-adopted by pid and start time
  (`docs/architecture-managers-workers.md:234-256`).
- **Published checkpoints already do "same node if cached, else any
  node".** After upload the local parts become evictable cache entries,
  and a resume prefers the node that still holds them through the
  `locality` score (`docs/suspend-and-resume.md:733-775`).
- **A manager-routed session start is already a provisional claim.** The
  router sends `PostWork` with a `ProvisionalAllocation` whose `origin` is
  `"manager"`, an empty `ticket_jti`, and a `SessionStart` lease
  (`src/manager/sessions.rs:609-681`). The worker admits it against the
  envelope, not node free capacity. An edge start is the same claim with
  `origin = "edge"` and a ticket.
- **Edge boot already has the edge half.** `Worker.StartClaim` on the
  worker port (4848, mTLS server side, the router presents no client
  certificate), offline ticket verification against a cached JWKS, a
  local single-use `jti` set, a local claim table, and a spec cache that
  never fetches on the start path (`docs/edge-boot.md`, "Redeeming" to
  "The spec cache"). Prefix tickets with many uses already exist; a ticket
  must name an instance or a non-empty prefix.
- **Session suspend is refused** at submit, both `durability = "local"`
  and a `reserve` (`src/structs/session.rs:404-420`).

## 6. Proposed design

### 6.1 The node's start path stops serializing (Phase 1)

Taking managers off the path only helps if a node can absorb its share.
Today it cannot run starts side by side, for three reasons found reading
the worker on `main`:

- **One lock covers the whole launch.** Every worker RPC and the heartbeat
  share one `Arc<Mutex<WorkerTaskManager>>` (`src/worker/server.rs:58`).
  A session start takes it to admit (`src/worker/edge.rs:96-103`), then
  takes it again for the launch itself
  (`src/worker/task_manager.rs:4550`, `begin_task_allocation_gated`), and
  the runtime's `plugin.start()` runs inside it
  (`src/worker/task_manager.rs:2796`). A sandbox start that takes 30 ms
  holds every other start, stop, heartbeat, and exec on the node for 30 ms,
  so a node tops out near 1 / (launch time), about 30 starts a second,
  however many cores it has.
- **Every start rewrites and fsyncs the worker's whole state file.**
  `run_allocation` ends in `persist_state()`
  (`src/worker/task_manager.rs:2805`), which serializes every allocation
  to pretty JSON and writes it with `sync_all`
  (`src/worker/state.rs:156-168`, `:313-315`), under the same lock. It
  skips unratified claims' contents (`:4148-4159`) but still pays the
  write and the fsync for each session start, and the file grows with
  everything else on the node.
- **Every start spawns an OS thread** (`src/worker/edge.rs:121`). Cheap
  one at a time, but a burst of a few hundred concurrent starts on one node
  becomes a few hundred threads blocked on the lock above.

The fix:

1. Split admission from launch. Admission (envelope room, claim table,
   `jti` set) stays a short critical section on a dedicated
   `SessionLedger` lock; the launch runs outside any shared lock and takes
   the task manager only to register the started instance.
2. Session starts never call `persist_state()`. Their record is
   `sessions.db` (§6.2), group-committed.
3. Launches go to a bounded pool (default `2 × cores`) with a queue; a
   start that cannot get a launch slot within `edgeBoot.queueTimeout`
   (default 1 s) is refused with `RESOURCE_EXHAUSTED` and `retryable`, so
   the picker spills to the next node instead of piling up threads.
4. `session_usage` (`src/worker/task_manager.rs:3095`) scans every
   session on each admission; the ledger keeps running totals instead.

This phase is valuable alone: the manager-routed path gets the same
per-node concurrency.

### 6.2 The session log on the node (Phase 2)

The worker writes each session's lifecycle to `sessions.db`, a SQLite
database beside its state directory, as sandbox-scaling calls entry 37
proposed: one row per live or sleeping session (id, job, job config, task,
envelope allocation, state, pid and start time, lease `iat`, checkpoint
key if suspended), written in WAL mode and fsynced in groups every 50 ms,
so a start costs no synchronous disk flush on the start path. A start the
log has not flushed when the worker dies is re-adopted anyway if its
process is alive: the worker scans its runtime's children on boot and
adopts any session-shaped claim it can verify against the envelope.

On restart the worker:
1. Reads `sessions.db` and verifies each running session's pid and start
   time, as it does for allocations.
2. Adopts the live ones, ends the rows whose process is gone, and keeps
   suspended rows as they are.
3. Sends a full session report on its first heartbeat; managers already
   ask for one from a node they have not heard from.

A session whose envelope is gone after the restart follows the existing
30 s orphan rule.

### 6.3 The node is in the id (Phase 3)

A minted session id carries a short tag of the node it started on:

```text
workspace-q4n7c2rd.7k2qd9x4m1b8c3rj5t0v6wye2h
          └ node ┘ └──── 130 random bits ───┘
```

- The tag is 8 base32 characters (40 bits) of a keyed hash of the node id,
  so it does not spell the node's name and 10^4 nodes collide with
  probability about 5 × 10^-5. A collision costs one extra probe, never a
  wrong answer, because the node is asked and answers for itself.
- Every manager resolves a tag from the `nodes` table it already holds
  (a local read of replicated rows, cached).
- `get`, `stop`, `start`, and `promote` on a session id go to the tagged
  node first, over the worker plane. The node's answer is authoritative.
  The index fan-out stays as the fallback for a tag that resolves to no
  node, a node that answers "not here", and listings.
- Derived ids (idempotency keys) cannot know their node before placement.
  They keep the untagged shape and the fan-out path, which is the price of
  a retry-safe create; a platform that wants O(1) routing reads the
  returned id and uses it.

This answers "can I route back to the same node while it is alive" with
no write anywhere.

### 6.4 Starts that skip the manager (Phase 4)

The shape: Legion publishes which nodes can take a task's sessions and
how full they are; something outside Legion (a load balancer with a
consistent-hash ring, or a few lines of the platform's own router) picks
a node per request; the node admits or refuses on its own; managers learn
about the session from the node's heartbeat. No manager handles the start
and raft writes nothing.

```
platform request ──► picker (Envoy ring_hash / HAProxy consistent / own code)
                       │  backend list: legion session nodes agents -j  (poll, or watch)
                       │  key: user id, or the idempotency key
                       ▼
                 node w-17:4848  StartSession(ticket, job, task, key?)
                       │  verify ticket offline · admit into envelope · launch
                       ├─► 200 {sessionId, node}            ──► picker routes traffic
                       └─► RESOURCE_EXHAUSTED {room}         ──► picker tries next on ring
                 heartbeat (batched deltas) ──► managers' session index
```

**The node list.** A read-only RPC, `ListSessionNodes(job, task)`, and
its CLI and MCP forms, answer every node holding an envelope for the task:

```console
$ legion session nodes agents -j
{"ok":true,"data":{"job":"agents","task":"workspace","asOf":"2026-11-02T18:04:11Z",
 "nodes":[
  {"node":"w-17","address":"https://10.0.1.7:4848","tag":"q4n7c2rd",
   "accepting":true,"room":{"cpu":21500,"memory":44032},"weight":43},
  {"node":"w-18","address":"https://10.0.1.8:4848","tag":"h2m5x9pe",
   "accepting":false,"reason":"draining"}]}}
```

- `weight` is free room in sessions of this task's size, for weighted
  rings; `accepting = false` covers draining, ineligible, and silent nodes.
- The list comes from raft rows (envelope allocations, node status) plus
  the soft index's usage, so any manager answers it locally; it is a
  per-picker poll every few seconds, not a per-start call. `--format
  envoy-eds` and `--format haproxy` render it for those balancers, and
  `StreamEvents` carries `envelope.placed` and `node.draining` so a picker
  can watch instead of poll.
- The list is advisory. A stale entry costs a refusal or a connect
  timeout and a hop to the next node, never a wrong start.

**Authorization: a session ticket.** Edge boot tickets must name an
instance or a prefix, are pinned to an exact job config, and live
minutes (`docs/edge-boot.md`, "Minting"). A session ticket is the same
JWT with a new scope:

```console
$ legion boot-ticket create agents --task workspace --scope session --ttl 1h
```

```jsonc
{ "legion.io/scope": "session", "legion.io/job": "agents",
  "legion.io/task": "workspace", "legion.io/pool": "agents",
  "legion.io/uses": 0 }              // 0: unlimited; the envelope bounds it
```

It names no instance, prefix, or job config: the node mints the id and
uses the job config of the envelope it holds, whose payload already
carries the session task's spec, so the spec cache is warm by
construction. Minting needs `boot:write` on the job. Nothing is spent per
session. The blast radius of a leaked session ticket is "start sessions
of this task until it expires, within the envelopes' room", which is what
the platform's own edge router can already do; sandbox-scaling calls
entry 30 accepted the same trade for prefix tickets.

**The start.** `Worker.StartSession` on the worker port, beside
`StartClaim`. The node verifies the ticket offline, mints the id with its
own tag (§6.3), admits into its envelope (§6.1), launches, and answers
when the gate opens:

```console
$ legion session start agents --node https://10.0.1.7:4848 --ticket "$T" --key req-81 -j
{"ok":true,"data":{"sessionId":"workspace-q4n7c2rd.7k2q...","node":"w-17",
 "state":"running","deduplicated":false}}
```

- With an idempotency key, the node derives the untagged id (§6.3) and
  answers a retry that lands on it from its own claim table, exactly. A
  consistent-hash picker keyed on the idempotency key sends retries to
  the same node, so dedupe is local in the common case. A retry that
  crossed a ring change lands elsewhere; the derived id is the same, so
  the existing duplicate convergence stops the later copy.
- `Worker.StopSession` and `Worker.GetSession` accept the same ticket, so
  a platform can end and inspect the sessions it started without a
  manager either. `legion instance stop|get` through a manager keep
  working, routed by tag.
- The heartbeat delta is the only thing the managers see. There is no
  ratification: a session has no row to ratify.

**The manager-routed path stays,** for platforms with no load balancer
and for low rates, and gets faster where measurement says. Known costs on
`main`: an idempotent create calls `find()` before placing, which
fans out to every peer (`src/manager/sessions.rs:487-499`); and one
manager process per start does an ACL check, a config lookup, and an mTLS
`PostWork` (`:609-681`). Phase 4 measures it in a release build with three
managers on separate hosts, then fixes what dominates; the derived-id
fan-out is replaced by asking only the node the room view would pick
first, with the 20 s duplicate check as the backstop.

### 6.5 Everything else between here and tens of thousands a second

An audit of the rest of the path on `main`, at the target of 50,000
starts and 50,000 ends a second across 2,000 nodes and three managers
(25 starts a second per node):

| Ceiling | Where | At the target | Fix | Phase |
| --- | --- | --- | --- | --- |
| Manager routes every start | `src/manager/sessions.rs:477` | ~1k/s measured on 4 cores, debug | Edge starts | 4 |
| Launch under the node-wide lock | `src/worker/task_manager.rs:4550`, `:2796` | ~1 / launch time per node | Admission/launch split | 1 |
| Full-state JSON fsync per start | `src/worker/task_manager.rs:2805`, `src/worker/state.rs:156` | one fsync per start, under the lock | `sessions.db` group commit | 1, 2 |
| Thread per start | `src/worker/edge.rs:121` | unbounded under bursts | bounded launch pool, `RESOURCE_EXHAUSTED` | 1 |
| Admission scans every session | `src/worker/task_manager.rs:3095` | O(sessions) per start | running totals | 1 |
| Idempotent create fans out | `src/manager/sessions.rs:487` | one peer RPC per manager per start | node-local dedupe (edge), single probe (routed) | 4 |
| `get`/`stop` fan out | `src/manager/sessions.rs:173` | one peer RPC per manager per call | tag routing | 3 |
| Heartbeat ingest | `src/storage/session_index.rs:286` | ~100k deltas/s cluster-wide, ~33k/s per manager under one `RwLock` write | fine at the target (map updates); shard the lock by node if profiling shows contention | measure in 4 |
| Full report size | `src/manager/daemon.rs:605` (tonic's 4 MiB default decode limit) | ~200 B a session, so about 20,000 sessions per node per full report | chunk full reports across beats, or raise the limit for heartbeats | 2 |
| Index memory | `src/storage/session_index.rs` | ~300 B a session; 10^6 running is ~300 MB across managers | none needed | — |
| Idempotent `store` starts | §6.7 | one conditional bucket `PUT` per idempotent start (tens of ms on S3), in the start's latency | random ids skip it; only callers that ask for idempotency pay | 5 |
| Raft | none on the path | 0 entries per session | keep it so | — |
| Ticket verification | `docs/edge-boot.md`, "Verification" | one ES256 verify a start, ~0.1 ms of node CPU | cache verified tickets by `jti` | 4 |
| TLS from picker to node | worker port 4848 | a handshake per new connection | pickers keep connections pooled (HTTP/2); document it | 4 |
| Runtime boot | task runtime | snapshot restore tens of ms, cold boot more | not this plan: sandbox-scaling Phase 8 density and snapshot-clone | — |

Two of these are inferred from reading the code, not measured: the
per-node lock ceiling and the ingest cost. Phase 1 and Phase 4 each open
with the measurement that confirms or retires them.

What does not limit it: the scheduler (envelopes are placed once per
node, not per session), the eval queue (sessions create none), the
replicated event journal (per-session events stay off it, sandbox-scaling
calls entry 32), and ACL checks on the manager (an edge start makes none;
the ticket is the check).

### 6.6 Sleeping on the node (Phase 5)

Session tasks may suspend. The jobspec surface is today's `suspend` block,
with the session refusals at `src/structs/session.rs:404-420` relaxed:

```nickel
stateful = {
  mode = "session",
  envelope = { resources = { cpu = 30000, memory = 61440 } },
  fenceAfter = "2m",             # new: store sessions only (§6.7)
},
suspend = {
  checkpoint = "memory",
  durability = "store",          # or "local": kept on the node only
  idle = { after = "3m" },
  keepFor = "24h",               # new: how long a node holds a local-only checkpoint
  checkpointEvery = "10m",       # new, optional: checkpoint running sessions periodically
  onWorkerStop = "checkpoint",   # new: 'keep' (default) | 'checkpoint'
},
```

- **Periodic checkpoints (`checkpointEvery`, optional, off by default).**
  The node checkpoints each running session on that interval without
  stopping it (a pause for the capture, then the session continues) and,
  with `durability = "store"`, publishes it with a conditional write on
  the manifest's ETag. That write is also a fence: a copy whose
  generation was superseded fails it and ends itself (§6.7). Checkpoints
  are staggered across a node's sessions so a node never captures them
  all at once, and a node whose checkpoint backlog exceeds one interval
  skips rounds and reports `legion_session_checkpoints_skipped_total`
  rather than falling further behind. With `"local"`, a periodic
  checkpoint replaces the previous local one and only helps a worker
  that crashed with its processes (row 15). Its cost until diff
  snapshots land (sandbox-scaling Phase 8) is a full memory image per
  interval per session, so the public doc gives the bytes-per-hour
  arithmetic and the default stays off.
- **Checkpoint on worker stop (`onWorkerStop`).** `"keep"` (the default)
  relies on Phase 2: the processes outlive the worker and are re-adopted.
  `"checkpoint"` makes a gracefully stopping worker (SIGTERM, `legion node
  drain`, an upgrade) checkpoint every running session first, within
  `edgeBoot.stopCheckpointTimeout` (worker config, default 60 s, sessions
  in order of least recently checkpointed). Sessions keep running after
  the checkpoint; the restarted worker re-adopts the ones whose process
  survived and restores the rest from the checkpoint on the same node.
  With `durability = "store"` the checkpoint is published, so a node that
  never comes back still leaves the session resumable elsewhere (row 6).
  A drain with `"checkpoint"` suspends the sessions instead of ending them.

- **Suspend is decided and done on the node.** The node's idle policy (or
  `legion instance suspend <id>`, routed by tag) captures the checkpoint,
  frees the session's envelope share, records `suspended` in `sessions.db`,
  and reports it in the next delta. The checkpoint is charged to the node's
  `snapshots` content budget, as a keyed instance's local checkpoint is.
- **Eviction.** A local-only checkpoint is pinned until `keepFor` expires,
  the job stops, or `minFreeDisk` pressure forces it out, oldest first.
  The node keeps the evicted id in `sessions.db` for `keepFor` again, so
  the answer is `evicted`, not `not found`. With `durability = "store"` the
  checkpoint is also published to
  `<contentStore.url>/sessions/<job>/<id-prefix>/<id>.json`
  (sandbox-scaling §6.5); after publish the local copy is an ordinary cache
  entry, and eviction just means the next resume fetches the parts.
- **Resume.** `legion instance start <id>`:

```console
$ legion instance start workspace-q4n7c2rd.7k2q... -j
{"ok":true,"data":{"message":"Instance workspace-q4n7c2rd.7k2q... resumed",
 "node":"w-17","resumingFrom":"local","deduplicated":false}}

$ legion instance start workspace-q4n7c2rd.9m3x... -j   # evicted, local only
{"ok":false,"error":{"code":"NOT_FOUND",
 "message":"session workspace-q4n7c2rd.9m3x... was evicted from w-17 at 2026-11-02T18:04:11Z (disk pressure); a local-only session cannot be restored",
 "hint":"start a new session, or set suspend.durability = \"store\" to keep idle sessions in the content store","retryable":false}}

$ legion instance start workspace-r8p2k5ta.4h6w... -j   # its node is down
{"ok":false,"error":{"code":"UNAVAILABLE",
 "message":"node w-23 holds session workspace-r8p2k5ta.4h6w... and has not been heard from for 3m12s",
 "hint":"the session is lost if w-23 does not return; with durability = \"store\" it would resume elsewhere","retryable":true}}
```

  The manager routes by tag (§6.3). The node restores from its local
  checkpoint if its envelope has room, and refuses with `ENVELOPE_FULL`
  otherwise. On a full envelope, an eviction, or a lost node, a `store`
  session falls back to its manifest and the router places it like a new
  start, preferring nodes that hold its parts. A resumed session keeps its
  id; when it lands on another node the tag is stale, and §6.3's fallback
  finds it.
- **Duplicates.** A resume that raced to two nodes, or a resume while the
  old node was only unreachable, is settled by the session's generation
  (§6.7, "Fencing a moved session"), not by the earliest lease.

### 6.7 Failure modes, and how routing handles them

One rule carries most of this section: **the ring places, the id
routes.** A picker's hash ring decides where a *new* session starts. Every
later request for that session (traffic, resume, stop, exec) is addressed
by its session id, whose tag names the node it lives on, never by hashing
the user's key again. A picker keeps a `tag → address` map from
`ListSessionNodes` (every node with an envelope, accepting or not) beside
its ring (accepting nodes only). Most of the failures below are harmless
because of that split; the ones that are not are where a session has
moved.

**Generations.** Every session carries a generation, starting at 1.
With `durability = "store"` the session manifest records it, and every
resume of the session, on its old node or a new one, claims the next
generation with a conditional write to the manifest (`If-Match` on its
ETag; the S3 client already speaks it for the streaming lease,
`src/storage/s3_credentials.rs:213`) before its gate opens. The bucket is
the fencing authority, so a move needs no raft entry and no manager, and
a stale resume anywhere fails its write and aborts. The generation rides
on the session's reports and in its `SessionStatus`. A `local` session
never moves, so its generation stays 1.

**Moving is cautious for running sessions, eager for idle ones.** An
idle session has nothing running, so resuming it elsewhere the moment its
node is unreachable is safe: any later resume on the old node loses the
conditional write. A *running* session might still be running. Before
moving one, the resuming node probes the tagged node over the worker
plane (`GetSession`, 2 s) and, if it answers, asks it to stop its copy
first. It moves without that only if the probe fails *and* the managers
mark the node down or unreachable in `ListSessionNodes`, or the caller
passes `--force-move`. This narrows row 8 below to partitions where
neither the new node nor any manager can reach the old one.

#### No split brain: every duplicate converges, and the old copy stops

Two live copies of one session id can arise in exactly three ways:
two simultaneous idempotent starts (the derived id is the same on both
nodes), a move while the old node was alive, and two racing resumes. The
design closes each one, preventing it where a conditional write can and
converging it with a stated bound everywhere else. The rule for which copy
survives is the same everywhere: **the higher generation wins, then the
earlier lease, then the lower node id.** After a move the old copy always
has the lower generation, so the old copy is always the one that stops.
This replaces sandbox-scaling's earliest-lease-wins rule, which keeps
exactly the wrong copy after a move.

| Source of a duplicate | `durability = "store"` | `durability = "local"` |
| --- | --- | --- |
| Two simultaneous idempotent starts | **Prevented.** A derived-id start creates the session manifest with `If-None-Match: *` before its gate opens; the loser aborts and answers `ALREADY_EXISTS` with the winner's node. One bucket write per *idempotent* start; random ids cannot collide and skip it | **Converged** by the manager sweep (below), within one heartbeat plus one second once both nodes report |
| A move while the old node lives | **Prevented** where anyone can reach the old node (probe-before-move); otherwise **converged** by the fence list or self-fencing (below) | Cannot happen: a `local` session never moves |
| Two racing resumes | **Prevented.** The generation's conditional write lets one win | Cannot happen: a `local` resume runs only on its own node, under its claim table |

Four mechanisms do the converging. Each one alone ends a duplicate; they
overlap on purpose.

1. **Conditional writes stop the stale copy on its own.** A copy whose
   generation was superseded fails its next write to the manifest (a
   periodic checkpoint, a suspend, or a checkpoint on worker stop) and
   ends itself with `superseded`. With `checkpointEvery` set (§6.6), this
   bounds a stale copy's life to one checkpoint interval whenever its node
   can reach the bucket.
2. **The fence list tells the old node directly.** A move writes a marker
   to `<contentStore.url>/sessions/fence/<old-node>/<id>` (the generation
   it superseded). Every node lists its own fence prefix every
   `sessions.fenceListEvery` (worker config, default 30 s), stops any
   listed session at or below the fenced generation, and deletes the
   marker. One `LIST` per node per interval, so 2,000 nodes cost about 67
   requests a second cluster-wide, whatever the session count.
3. **Managers sweep for duplicates, batched.** Each manager collects the
   derived ids newly reported to it and, once a second, asks its peers
   about all of them in one `QuerySessions` call; any id held twice is
   resolved by the rule above and the loser is stopped through its node's
   next heartbeat reply. A full report re-checks every derived id on that
   node. This costs O(managers) calls a second, not one per start, and it
   is what converges `local` duplicates.
4. **A node cut off from everything fences itself.** A node that has
   reached neither a manager nor the bucket for `stateful.fenceAfter`
   (default `2m`, session tasks with `durability = "store"` only) suspends
   its running sessions locally and stops serving them, because a newer
   generation may exist elsewhere that it cannot see. When it reconnects
   it asks the bucket which generations it still owns and resumes those.
   A `local` session never fences itself: it cannot have moved, so
   keeping it running is always safe.

**The bound.** Once the network heals, or after `fenceAfter` if it does
not, at most one copy of any session id keeps running: within
`max(fenceListEvery, one heartbeat + 1 s)` for a node that can reach the
bucket or a manager, and within `fenceAfter` for one that can reach
neither. During that window a stale copy may still act on the outside
world. That window is the price of starting with no raft entry; a session
that needs at-most-one *at every instant* is promoted, and the keyed path
binds its single allocation through raft.

#### The failure catalog

| # | What happened | What the platform sees | What Legion does | What is lost |
| --- | --- | --- | --- | --- |
| 1 | **Nodes added to the ring** | ~1/N of keys now hash to new nodes | Nothing. Existing sessions are routed by id tag, which did not change. New starts for moved keys land on new nodes | Cache locality for moved keys' *new* sessions, once |
| 2 | **Nodes removed or draining** | The node leaves the ring (`accepting = false`) but stays in the tag map | Draining ends or suspends its sessions on its own schedule; resumes of its `store` sessions land elsewhere (row 5) | Nothing for `store`; running `local` sessions end at drain |
| 3 | **Idempotent retry across a ring change**, or two simultaneous idempotent starts | The retry hashes to a different node | `store`: the manifest create lets one start win; the other aborts before its gate opens and returns the winner. `local`: both start; the manager sweep stops one within a heartbeat plus a second | `store`: nothing. `local`: seconds of a second copy |
| 4 | **Picker routes by key instead of id** (a platform bug) | The node answers `NOT_FOUND` with `wrong_node` and the id's tag | Nothing to do; the hint says to route by the id | One extra hop |
| 5 | **Node dies; session was idle with `durability = "store"`** | Connect to the tagged node fails, or the tag map shows it down | The picker (or a manager) sends a resume to any node in its ring. That node reads the manifest at its deterministic path from the id, claims generation g+1 by conditional write, fetches parts, restores, and answers with its own node | Nothing since the checkpoint |
| 6 | **Node dies; session was running with `durability = "store"`** | Same as 5 | Same as 5, from the newest published checkpoint: a periodic one (`checkpointEvery`), the one taken at worker stop (`onWorkerStop`), or the last suspend | Everything since that checkpoint |
| 7 | **Node dies; `durability = "local"`** | Tagged node down | Managers answer `node lost` once the node is marked lost; the picker can answer the same from the tag map | The session |
| 8 | **Node unreachable from the picker but alive** (a partial partition) | Same as 5 to the picker | An idle session moves at once (safe, above). A running one moves only after the new node's probe fails and managers mark the node down; if the new node or a manager can reach it, the old copy is stopped first. Otherwise the old copy stops through the fence list, its next checkpoint, a manager, or self-fencing, whichever comes first | Side effects from the stale copy within the bound above |
| 9 | **Node returns after a move** | Its fence list and its full report both name the stale copy | The node stops it on its first fence listing; a manager that sees both stops it in the heartbeat reply, whichever is first | Nothing more |
| 10 | **Two resumes race** (two pickers, or a retry) | Both nodes start restoring | One conditional write wins; the loser aborts before the gate opens and answers `ABORTED` with the winner's node | Nothing |
| 11 | **Resume to the same node finds its envelope full** | `RESOURCE_EXHAUSTED` | `store`: resume elsewhere (row 5). `local`: refused, retryable; the checkpoint stays put | Nothing |
| 12 | **Bucket unavailable** | Suspends still succeed locally; moves and idempotent `store` starts fail | Parts stay pinned locally and publish retries; rows 5 and 6 fail with `UNAVAILABLE` until it is back. Nodes still reach managers, so nothing self-fences | Nothing, unless the node also dies (then as row 7) |
| 13 | **Managers unavailable** | Nothing on the edge path | Edge starts, routing by tag, and moves all work without a manager while tickets and the cached JWKS last; the fence list still converges moves. `local` idempotent duplicates wait for the managers' sweep. Managers rebuild their index from full reports when back | Listings and manager-routed calls during the outage |
| 14 | **Stale node list at the picker** | Connect timeouts or refusals | Picker hops to the next ring node, and refreshes | Latency |
| 15 | **Worker restart or stop** | A brief refusal window on that node | With `onWorkerStop = "checkpoint"`, the stopping worker checkpoints every running session first. Phase 2 re-adopts the processes that outlived it and restores the rest from those checkpoints on the same node | Nothing; with `"keep"`, sessions whose process did not outlive the worker are lost |
| 16 | **Platform loses its id → node cache** | Only the session id is left | The tag routes it; a stale tag after a move falls back to the manifest (it names the current node) or a manager lookup | One extra hop |
| 17 | **A node is cut off from managers and the bucket** | Its sessions are unreachable | After `fenceAfter`, its `store` sessions suspend locally and stop serving; on reconnect it resumes only the generations the bucket says it still owns. `local` sessions keep running | `store`: the running state since the local suspend, if the session moved meanwhile |

**Where the current node is written down after a move.** The tag goes
stale after row 5. The session keeps its id (it is the session's name to
the platform). The resume reply returns the new node, and a picker should
cache `id → node` and prefer it over the tag. A picker that lost the
cache finds the current node in the manifest, which every move rewrites,
or by asking a manager. Re-minting the id on every move would keep the tag
exact, but would break every reference the platform holds to the session.

#### Network volumes

Sessions refuse `volumes` today (`docs/session-mode.md:178`), and this plan
keeps that. A session that needs a network volume is promoted, because
moving a volume safely needs exactly what sessions give up: a durable
owner that can declare the old node lost before attaching the volume
elsewhere. Without that, row 8's two live copies would both write the same
volume. The keyed path already has it (reschedule on node loss, with the
volume driver's attach fencing), so `promote.onVolume` from sandbox-scaling
§6.6 stays the answer. It costs one raft entry per session that attaches a
volume, not per start.

A session's checkpoint already holds its local disk, so "persisted
remotely" in the common case is `durability = "store"` (rows 5 and 6),
not a volume.

#### Summary by durability

| Event | `durability = "local"` session | `durability = "store"` session |
| --- | --- | --- |
| Worker restart | Running and sleeping sessions kept (Phase 2) | Same |
| Manager failover or outage | Nothing lost; routing by tag needs no manager | Same, and moves still work |
| Ring change | Nothing; the id routes | Same |
| Node silent, comes back | Sessions kept; answers `UNAVAILABLE` meanwhile | May have moved meanwhile (g+1); the returning copy is fenced |
| Node lost | Lost, answered as `node lost` | Resumes elsewhere from the last checkpoint |
| Eviction | Lost, answered as `evicted` for `keepFor` | Parts fetched back on the next resume |

### 6.8 The replicated directory, and why it is Phase 6 (conditional)

The proposal on the table: managers fold session reports into a `session`
table in raft, flushed in batches, eventually consistent, so any manager
knows where every session lives and that it existed.

**The write path that would work.** Each manager buffers the deltas of its
own nodes and flushes one status-lane entry per second or per 5,000
changes, whichever comes first, as an upsert/delete changeset. Status-lane
writes do not contend with the scheduler lane that bounds keyed
lifecycles (`docs/multi-raft.md`, lanes). A node's full report reconciles
its rows. A node marked lost has its rows flipped to `lost` in one entry.

**What it costs.** At 1,000 starts and 1,000 ends a second, that is one
entry a second per manager carrying about 2,000 row changes (roughly
150 KB of changeset), which raft handles easily. The cost is the rows:
every manager stores every node-held session, and every snapshot carries
them. At 10^6 sessions that is about 150 MB per manager; at the plan's
10^8 idle target (sandbox-scaling calls entry 35) it is 15 GB, past the
4 GiB store trigger in sandbox-scaling §6.11. Idle sessions at that count
must stay in the bucket, so the directory could only ever cover node-held
sessions.

**What it would buy, once Phases 1 to 5 exist.**
- Routing: nothing over the id tag, which is exact at mint and needs no
  write. A resumed-elsewhere session is the only stale-tag case, and the
  fan-out covers it.
- Durability: nothing. The row points at a session; if the node lost it,
  the row is a pointer to nothing.
- "It existed, and its node is gone" for a session on a silent node: the
  tag already says which node, and the node table says it is down.
- An exact listing of sleeping sessions while their node is unreachable:
  the one real gain.

**Trigger.** Build Phase 6 if a platform needs complete listings across
node outages, or if fan-out reads measurably load managers (more than 10%
of a manager's CPU at the target rate in the three-manager run). Until
then, every session-sized write stays off raft.

## 7. Proto, struct, config, and persistence changes

- **Worker disk:** `sessions.db` (new, SQLite, WAL), owned by the worker.
  Session starts stop writing the JSON state file.
- **Ids:** the minted session id gains the `<tag>.` segment; derived ids
  are unchanged. `is_minted_instance_id` accepts both shapes, and caller
  ids may not take either (breaking, per `AGENTS.md`).
- **Proto, manager:** `ListSessionNodes(ListSessionNodesRequest) →
  ListSessionNodesReply` (read-only, `job:read`, in
  `tests/integration/acl_endpoints.rs`, the capability catalog, and MCP as
  `legion_session_nodes`). `CreateBootTicketRequest.scope` gains
  `session`.
- **Proto, worker plane:** `Worker.StartSession`, `Worker.StopSession`
  and `Worker.GetSession` accept a session ticket (a new caller class
  beside `StartClaim`'s); `Worker.ResumeSession` and
  `Worker.SuspendSessions` are manager-only like `StopSessions`.
  `SessionStatus.state` gains `suspended`; `SessionStatus.checkpoint`
  (durability, bytes, published URL), `SessionStatus.origin`
  (`manager` | `edge`, plus the ticket `jti`) and
  `SessionStatus.generation` are new. `StartSessionRequest.resume_id`
  resumes an existing `store` session on the receiving node, and
  `force_move` skips the probe. Refusals gain `wrong_node` (with the id's
  tag) and `superseded` (with the winning generation and node). The
  session manifest gains `generation` and `node`.
- **Worker config:** `edgeBoot.launchConcurrency` (default `2 × cores`),
  `edgeBoot.queueTimeout` (default `1s`),
  `edgeBoot.stopCheckpointTimeout` (default `60s`), and
  `sessions.fenceListEvery` (default `30s`).
- **Bucket layout:** `sessions/fence/<node>/<id>` markers beside the
  session manifests; manifests are created with `If-None-Match: *` for
  idempotent `store` starts and updated with `If-Match` thereafter.
- **Jobspec:** `suspend.keepFor` (new, default `"24h"`, session tasks
  only), `suspend.checkpointEvery` (new, optional, off by default),
  `suspend.onWorkerStop` (new, `"keep"` | `"checkpoint"`, default
  `"keep"`), and `stateful.fenceAfter` (new, default `"2m"`, session tasks
  with `durability = "store"`), all in `configs/jobs/maximal.ncl`. `durability = "local"` becomes
  valid on session tasks.
- **CLI:** `legion session nodes`, `legion session start|stop|get --node`
  (the edge forms, for testing and scripts), and `legion boot-ticket
  create --scope session`, all with `--json`.
- **No raft schema change** in Phases 1 to 5.

## 8. Implementation phases

1. **The node starts sessions concurrently.** Admission split from
   launch, a bounded launch pool with a retryable refusal, no state-file
   fsync for sessions, running usage totals. Opens with a per-node start
   rate measurement on the base. Valuable alone: every start path,
   routed or edge, gets per-node concurrency.
2. **Sessions survive a worker restart.** `sessions.db`, adopt on boot,
   full report after, full reports chunked past the message limit.
   Valuable alone: worker upgrades stop killing sessions.
3. **The node in the id.** Tagged mint, tag resolution, tag-first routing
   for `get`/`stop`/`start`/`promote`, fallback to fan-out, and the
   `wrong_node` answer (§6.7 row 4). Valuable alone:
   per-request cost no longer grows with the manager count, and a silent
   node's sessions answer `UNAVAILABLE` naming the node instead of
   `NOT_FOUND`.
4. **Starts that skip the manager.** `ListSessionNodes` and its balancer
   formats (with the ring and tag-map split of §6.7), session tickets,
   `Worker.StartSession|StopSession|GetSession`, node-local idempotency; the routed path measured across three managers
   and its derived-id fan-out removed. Valuable alone: start rate scales
   with nodes.
5. **Sleeping on the node, and moving.** Session suspend (`local` and
   `store`), the node's idle policy for sessions, `keepFor` and eviction
   answers, resume by tag with manifest fallback, edge resume on any node,
   generations with conditional manifest writes, probe-before-move,
   generation-first duplicate resolution, the fence list, the managers'
   batched duplicate sweep, self-fencing, `checkpointEvery`, and
   `onWorkerStop = "checkpoint"` (§6.6, §6.7). Valuable alone: idle sessions stop
   costing envelope room. Unblocks wake-on-request Phase 3.
6. **Replicated directory (conditional, §6.8).** Only past its trigger.

## 9. Testing plan

- **Phase 1:** a per-node start-rate measure (fake runtime with a fixed
  30 ms launch, then the `exec` runtime) before and after: concurrent
  starts must overlap; stops, heartbeats and exec stay responsive during a
  burst; a burst past the pool answers `RESOURCE_EXHAUSTED`, not a pile of
  threads.
- **Phase 2:** a worker-restart integration test in
  `tests/integration/session_integration.rs` (fake node over the real worker plane,
  restarted mid-run): sessions adopted, ended processes dropped, full
  report on first beat. Unit tests for the log's group commit and recovery
  from a truncated WAL; a full report of 50,000 sessions crosses the
  heartbeat in chunks.
- **Phase 3:** routing tests asserting zero `QuerySessions` peer calls for
  a tagged id in a three-manager cluster; tag collision forced in a unit
  test; stale tag after a resume falls back.
- **Phase 4:** an integration test that starts, gets, and stops sessions
  through `Worker.StartSession` with a session ticket and asserts zero
  manager RPCs on the path and zero raft entries; ticket refusals (wrong
  scope, pool, expiry, task); an idempotent retry to the same node
  answered `deduplicated`, and to another node converged. A load run:
  `legion-loadgen` gains an edge mode that keeps a consistent-hash ring
  over `ListSessionNodes` and drives starts at nodes directly; run it on
  the AWS rig across as many nodes as it has, reporting starts per second
  per node and manager CPU, and extrapolate to the 2,000-node target only
  from a per-node number that stays flat as nodes are added.
- **Failure catalog (§6.7):** one integration test per row, in
  `tests/session_failures.rs`, with fake nodes over the real worker plane
  and a `file://` bucket that honours conditional writes. Rows 5 to 10
  kill, partition (a node the test's picker cannot reach but managers can,
  and one nobody can), and revive nodes, and assert the surviving copy,
  its generation, and that the stale copy is stopped within the stated
  bound. A property test drives random interleavings of starts, moves,
  partitions, and heals against the four convergence mechanisms and
  asserts that after healing, exactly one copy of every id runs. Ring changes (rows
  1 to 3) run against a reference ring in the test harness.
- **Phase 5:** suspend and resume round trip on the same node (`exec`
  runtime with a test checkpoint; KVM memory checkpoints in the KVM suite);
  eviction under `minFreeDisk` answers `evicted`; `store` fallback when the
  node is fenced; zero raft entries across start, suspend, resume, stop
  (applied index summed over lanes, as sandbox-scaling §16.3 measures).

## 10. Rollout

No installed base: tagged ids replace the current shape outright, and
existing sessions (memory only) do not survive the worker upgrade that
installs Phase 2, which is the last time that happens. Managers before
workers, as always; a worker without `StartSession` does not advertise
the `edge-sessions` capability, and `ListSessionNodes` marks it
`accepting = false` for edge starts. Public docs: `docs/session-mode.md`
gains "Worker restarts", "Routing", "Starting at the edge" (with Envoy,
HAProxy, and plain-code picker examples), and "Idle sessions" sections,
and its "What you give up" table and Limitations change with each phase.
`docs/edge-boot.md` gains the session ticket scope.

## 11. Open questions

- **Should Legion ship a picker?** A small `legion start edge-router`
  holding the ring would make the high-rate path work out of the box.
  Lean: no in this plan; publish the list and the refusal shape, and
  revisit if platforms keep writing the same twenty lines.
- **Checkpoint seal keys without raft.** Published keyed checkpoints are
  sealed with a key the manager sends and records. A session has no row
  to record it in. Lean: derive it, `HMAC(cluster key, session id ‖
  checkpoint seq)`, so any manager can re-derive it for a resume.
- **Should the tag be the envelope allocation id instead of a node hash?**
  It is already a replicated row, so no new mapping is needed, but a job
  update replaces envelopes in place and would stale every tag on the
  node. Lean: node hash.
- **`keepFor` default.** Lean: 24 h, matching the gateway's typical
  "come back later" window; operators shorten it under disk pressure.

## 12. Acceptance criteria

- **Phase 1:** with a 30 ms fake launch, one node sustains at least
  `launchConcurrency / 0.03` starts a second (about 500 on 8 cores), up
  from about 30; a heartbeat during a burst is not delayed past one launch.
- **Phase 2:** restarting a worker with 1,000 sessions leaves all 1,000
  running and listed within one full-report period; the §16.3 start rate
  is within 5% of the base.
- **Phase 3:** a tagged `get` in a three-manager cluster makes no peer
  call; a session on a silent node answers `UNAVAILABLE` naming its node.
- **Phase 4:** edge starts make zero manager RPCs and zero raft entries;
  starts per second grow linearly with nodes on the rig while manager CPU
  stays within 10% of idle heartbeat load; the routed path's release-build
  rate across three managers is measured and published.
- **Phase 5:** an idle session suspends on its node, frees its envelope
  share (`legion node usage`), and resumes on the same node from local
  disk; an evicted one answers `evicted`; a `store` session on a fenced
  node resumes elsewhere with no manager on the path; two racing resumes
  leave exactly one copy; a revived node's stale copy is stopped within
  one heartbeat; two simultaneous idempotent starts leave one copy
  (`store`: before either gate opens; `local`: within one heartbeat plus a
  second); a node partitioned from managers and the bucket stops its
  `store` sessions within `fenceAfter`; a `checkpointEvery` session whose
  node dies resumes elsewhere from a checkpoint no older than the
  interval; `onWorkerStop = "checkpoint"` leaves every running session
  restorable after a worker stop whose processes did not survive; zero
  raft entries for the whole cycle.
