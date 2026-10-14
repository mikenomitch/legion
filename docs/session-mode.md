# Session Mode

`stateful.mode = "session"` runs very large numbers of short-lived,
on-demand units, such as an agent's sandbox, a tool execution, a CI runner,
or a function invocation, without writing anything to the control plane's
replicated store when one starts or stops. It trades durability per unit
for start rate and count: a running session is held by the node running it,
not by raft.

A session that turns out to matter can be **promoted** to a keyed instance
without stopping it. From then on it has a row and every keyed guarantee.

**Why it exists.** A keyed instance is durable because every step of its
life is a write to the replicated log: about 8 writes for a create, run,
suspend, resume, and destroy, ordered one at a time by the log's scheduler
lane. That lane caps keyed sandboxes at about 50 full lifecycles a second
on a 4-vCPU manager, and every instance is a row that every manager keeps.
For a platform that starts a sandbox per agent task or per tool call, most
of those units are thrown away within minutes and never need to survive a
node failure, so they pay for durability they do not use. A session skips
the log entirely. Measured on AWS with four 4-vCPU workers and one 2-vCPU
manager, sessions started about 1,300 a second through that manager when
each one was a host process. As microVMs, 160 were booted and ready for exec
in 2.3 seconds, the cluster sustained 40 boots a second with none refused,
and one boot took 180 to 230 ms from the create to a guest that runs code
([Sandboxes at scale](sandboxes-at-scale.md#measured-on-aws)). None of it
writes to the replicated store. Start capacity comes from the nodes'
envelopes and from whichever manager routes the start, so it grows with the
fleet rather than with the log.

> Related: [stateful workloads](stateful-workloads.md) (the `slot` and
> `keyed` modes this builds on), [sandboxes at scale](sandboxes-at-scale.md)
> (what each keyed lifecycle step costs), [suspend and
> resume](suspend-and-resume.md), [auth and ACLs](auth-and-acls.md), and
> [metrics](metrics.md#sessions).

## The short version

```nickel
{
  name = "agents",
  tasks = [{
    name = "workspace",
    runtime = "exec",
    config = { command = ["/usr/local/bin/agent-worker"] },
    resources = { cpu = 500, memory = 512 },          # per session
    stateful = {
      mode = "session",
      envelope = {                                      # per node
        resources = { cpu = 30000, memory = 61440 },
      },
    },
  }],
}
```

```console
$ legion instance create agents --idempotency-key req-81 -j
{"ok":true,"data":{"instanceIds":["workspace-7k2qd9x4m1b8c3rj5t0v6wye2h"],
 "mode":"session","durable":false,"node":"w-17","state":"starting",
 "deduplicated":false}}

$ legion instance list agents -j
{"ok":true,"data":{"instances":[...],"complete":true,
 "asOf":"2026-11-02T18:04:11Z","nextIdlePageToken":""}}

$ legion instance promote workspace-7k2qd9x4m1b8c3rj5t0v6wye2h --id user-42 -j
{"ok":true,"data":{"instanceId":"user-42",
 "promotedFrom":"workspace-7k2qd9x4m1b8c3rj5t0v6wye2h","node":"w-17",
 "allocationId":88214,"durable":true,"deduplicated":false}}

$ legion instance stop workspace-2m9...       # ends a session; nothing remains
```

## How it works

**Envelopes reserve the room.** A session task's `stateful.envelope` is the
capacity each node keeps for its sessions. When the job is submitted,
Legion generates a second task, `<task>-envelope`, on the built-in
`envelope` runtime: one allocation per eligible node
(`count = { num = 1, per = "id" }`), sized by `envelope.resources`, and
constrained to workers that advertise the `sessions` capability. An
[elastic envelope](#elastic-envelopes) starts at that size and each node
grows and shrinks its own as its sessions need. The
scheduler places envelopes like any allocation, so preemption, bin
packing, and `legion node usage` count the room sessions use. An envelope runs nothing:
the allocation is the reservation.

**A create goes straight to a node.** `CreateInstance` without an
`instance_id` on a session task does not touch the store. The manager's
session router reads which nodes hold an envelope for the task and how much
of each is in use, picks one by the task's `stateful.placement` ([Picking a
node](#picking-a-node)), and sends the start to that node directly over the
worker's `PostWork` RPC. The node
admits the session against its envelope's free room (never against the
node's general free capacity, which the envelope already holds) and starts
it. A node whose envelope turned out to be full refuses, and the router
tries the next one, up to three nodes. There is no raft entry, no
evaluation, and no plan.

**The node is in the id.** An unnamed session's id carries an 8-character
tag naming the node it was started on:
`workspace-q4n7c2rd.7k2qd9x4m1b8c3rj5t0v6wye2h`. The tag is the first 40
bits of a SHA-256 of the node id, so a tag never spells a node's name, and
any manager or picker can map it back to the node from the node list
(`legion session nodes` prints each node's tag). An id derived from an
idempotency key carries no tag, because a retry must derive the same id
whichever node it lands on.

**Nodes start sessions concurrently.** A node admits a session against its
envelope under a short lock and launches the runtime with the lock
released, so one node's starts overlap. At most `edgeBoot.launchConcurrency`
launches run at once (default twice the node's cores); a start that waits
longer than `edgeBoot.queueTimeout` (default `1s`) for a launch slot is
refused with `RESOURCE_EXHAUSTED`, which is retryable, and the router or
picker tries the next node. A sandbox session holds its slot until its
guest takes exec, which is also when it reports `running`
([How fast a sandbox boots](task-runtime-sandbox.md#how-fast-a-sandbox-boots)),
so the setting also bounds how many guests boot on the node at once.
Neither a session's start nor its stop rewrites the worker's state file.

**Nodes report what they run.** Each worker reports its sessions on its
heartbeats, as deltas against an epoch, with a full report every 30
heartbeats and whenever the manager asks for one. Each manager keeps an
in-memory **session index** of the nodes that heartbeat to it. Nothing in
it is persisted or replicated; a manager that starts, or takes a node over
after a failover, asks the node for a full report on its first heartbeat.
A manager answering for a session it does not index asks its peers over the
raft transport and merges their answers, preferring whichever manager heard
from a node most recently.

**The router counts its own starts.** A start holds its share of the
chosen node's room in the router's view from the moment the router picks
the node, before the node answers, so creates in flight at once spread
across nodes instead of all taking the one with the most room. A node
that accepts keeps the share: for about 30 seconds, until the node's next
report lists the session, the router counts it from its own record. A
node that refuses gives it back. Nodes with the same room left are picked
at random, so managers reading the same view do not all send their next
start to one node. The router caches its view of envelope room for one
second, and when it expires one start reads it again while the others
keep routing on the old view. The node stays authoritative: an over-count
costs a retry on the next node, never an over-full envelope. Because the
view does not see stops until a refresh, a view that says every envelope is
full is not the last word: the start still asks the node it thinks least
full, once, and is refused only if that node refuses too.

## Picking a node

`stateful.placement` chooses how a manager's router picks the node for each
start. Edge starts are picked by the caller ([Starting at the
edge](#starting-at-the-edge)), so it does not apply to them.

| `placement` | Picks | A pick costs | Use it for |
| --- | --- | --- | --- |
| `"spread"` (default) | The node with the most room left, counting the router's own starts in flight. Ties at random | One read of every node holding an envelope for the task | The evenest spread, on fleets up to about a thousand nodes |
| `"fast"` | The roomier of two nodes sampled at random from those with room | The same on any fleet size | Very large fleets, and many managers routing one task |

Measured in the router (one core, half the fleet half full):

| Nodes with an envelope | `spread` | `fast` |
| --- | --- | --- |
| 100 | 4 µs a pick | 0.2 µs |
| 1,000 | 31 µs (about 32,000 picks a second) | 0.2 µs |
| 10,000 | 330 µs (about 3,000 a second) | 0.2 µs |
| 100,000 | 5.2 ms (about 190 a second) | 0.5 µs |

A pick runs under the router's lock, so on a fleet of ten thousand nodes
`spread` alone caps a manager's starts at about 3,000 a second, and `fast`
does not. `fast` spreads less evenly: of two samples, both may land on
fuller nodes. Two managers routing one task rarely pick the same node with
it, where `spread`'s pick depends only on the shared view. Either way the
node decides: a pick that turns out full costs a retry, and `fast` falls
back to `spread`'s full read when its samples find no room, which happens
only near a full fleet.

## When to use it

- Many units created by an API, at rates of thousands per second, each
  living seconds to hours: agent sandboxes and tool executions, per-request
  workers, CI runners, function-style invocations, preview environments.
- Units that are disposable on node loss.
- Counts where a row per unit would make the replicated store, and every
  snapshot of it, grow with the running set.

**When not to use it.** Anything that must be rescheduled when its node
dies, keep a volume, expose ports or services, or appear in an exact
listing at every instant. Use `keyed` (units by API, durable) or `slot` (a
fixed replica set) for those, or promote the sessions that turn out to need
it.

## What you give up, against `keyed`

| Guarantee | `keyed` | `session` |
| --- | --- | --- |
| Durable record | A raft row from create on; a named id keeps a tombstone after destroy | None while running: the node holds it, and the managers' index lists it |
| Node loss while running | Rescheduled elsewhere, bound to its storage | Lost, unless its task sets `suspend.durability = "store"`: then `instance start` resumes it on another node from its last published checkpoint ([Moving a session](#moving-a-session)) |
| Worker restart | The allocation is re-adopted | Running sessions are re-adopted from the node's session log ([Worker restarts](#worker-restarts)); one whose process died meanwhile ends, unless `suspend.onWorkerStop = "checkpoint"` left a checkpoint to restore it from ([Idle sessions](#idle-sessions)) |
| At most one live copy | Exact | Converges to one: a retried idempotent create can briefly run on two nodes ([Duplicates](#duplicates)) |
| Who starts it | A manager, through raft | A manager's router, or the node itself on a session ticket ([Starting at the edge](#starting-at-the-edge)) |
| Ids never recycled | Exact for the job's life | Probabilistic: 130 random bits, no tombstone |
| `instance list` | An exact read | From the session index, with `complete` and `asOf` ([Listing](#listing-sessions)) |
| Idempotent create | A replicated record | A derived id: a retry gets the same id, with no record |
| Suspend and resume | Yes, with a published or local checkpoint | On its node with `durability = "local"`; on any node with an envelope with `durability = "store"` ([Idle sessions](#idle-sessions)) |
| Fork, snapshot | Yes | No. Promote first |
| Events | The replicated journal | None per session. `instance.promoted` is journaled, and counters cover starts and stops |
| Capacity | Reserved per instance at placement | Reserved per node as envelopes, whether or not sessions fill them. An [elastic envelope](#elastic-envelopes) follows its sessions between a floor and a ceiling |
| Control-plane writes | About 8 raft entries per full lifecycle, about 3 under load | 0 for an unnamed session; 1 to promote |

**What stays the same.** ACLs (a session belongs to its job and
namespace), the `legion instance` commands and RPCs, which report a
`durable` field saying which column above applies, and the workload's
environment: a session sees `LEGION_INSTANCE_ID` (its session id) and
`LEGION_INSTANCE_MODE=session`.

## The jobspec

```nickel
stateful = {
  mode = "session",
  envelope = {
    resources = { cpu = 30000, memory = 61440 },
    constraints = [{ key = "rack", operator = "=", value = "r1" }],
  },
  promote = { onName = true },
  placement = "spread",
},
resources = { cpu = 500, memory = 512 },
```

| Field | Default | Meaning |
| --- | --- | --- |
| `stateful.mode` | | `"session"` |
| `stateful.envelope.resources` | required | What each node keeps for this task's sessions. Must set `cpu` and `memory`, and every amount the session's own `resources` declares. Sessions are admitted against exactly these keys |
| `stateful.envelope.constraints` | none | Extra placement constraints for the envelopes, added to the task's own `constraints` |
| `stateful.envelope.max` | none (fixed) | The ceiling of an [elastic envelope](#elastic-envelopes): each node grows its envelope from `envelope.resources` up to this as its sessions fill it, and gives room back after they drain. Sizes exactly the keys `envelope.resources` sizes, each at or above it |
| `stateful.envelope.shrinkAfter` | `"10m"` | Elastic envelopes only. How long a node's sessions must use less than half of its envelope before the node shrinks it. At least `1m` |
| `stateful.promote.onName` | `true` | Whether a caller-chosen id is accepted: `instance create --id` (a keyed instance from birth) and `instance promote --id` (a rename). With `false`, a create always starts an unnamed session and a promotion keeps the session's id |
| `stateful.placement` | `"spread"` | How a manager's router picks the node a session starts on: `"spread"` (the most room left) or `"fast"` (the roomier of two random nodes with room). See [Picking a node](#picking-a-node) |
| `stateful.fenceAfter` | `"2m"` | Session tasks with `suspend.durability = "store"` only. A node that reaches neither a manager nor its content store for this long suspends its running sessions of the task on its own disk, so a copy cut off from the cluster stops serving before a moved copy can start ([No split brain](#no-split-brain)). At least 10 seconds |
| `resources` | | One session's size, in the envelope's units |

`job validate` refuses, each with a hint:

- `envelope` without `mode = "session"`, `mode = "session"` without
  `envelope`, and `promote` on any other mode.
- A session that does not fit its envelope, or that declares a resource the
  envelope does not size.
- An `envelope.max` that does not size exactly the keys
  `envelope.resources` sizes, or sets one below it, and an
  `envelope.shrinkAfter` without `envelope.max`, shorter than `1m`, or not
  a duration.
- `requireId`, `initialInstances`, `stateful.storage`, and `volumes`.
- Ports, a `service`, `connect` upstreams, a network policy, or gated
  networking. The node's datapath serves allocations, and an unnamed
  session has none. Give a sandbox its own `config.network`.
- `identities`. Workload identity for unnamed sessions is not available.
- `speculativeStart`: a session already starts without a raft write.
- `suspend` with a `reserve`: a session's room is its envelope share, and
  a suspended session holds none of it.
- `stateful.fenceAfter` on a task that is not a session task, on a session
  task without `suspend.durability = "store"`, shorter than 10 seconds, or
  not a duration.
- `suspend.keepFor`, `suspend.checkpointEvery`, or `suspend.onWorkerStop`
  on a task that is not a session task, `keepFor` or `checkpointEvery`
  shorter than 60 seconds, and `checkpointEvery` on a task whose
  checkpoint is a disk one (it has no memory to capture).
- A task named `<task>-envelope`, or any task on the reserved `envelope`
  runtime.

`legion job status` and `legion alloc list` show the envelope task's
allocations like any other.

## Starting sessions

`legion instance create <JOB>` (`CreateInstance` with no `instance_id`)
starts an unnamed session. The reply carries `mode = "session"`,
`durable = false`, the node, and `state = "starting"`.

- **Idempotency.** With `--idempotency-key`, the session id is derived from
  the key, the caller, the job, and the task. A retry, on any manager,
  derives the same id, finds the running session, and answers it with
  `deduplicated = true`. Nothing is recorded anywhere.
- **Naming.** `--id user-42` creates an ordinary keyed instance: a named
  instance is durable from birth. It reserves the task's full `resources`
  through the scheduler, outside the envelopes. `promote.onName = false`
  refuses it.
- **Not available for sessions:** `--from-snapshot` and `--count` (start
  sessions one per call, or create named instances in a batch).

| Error | Meaning |
| --- | --- |
| `FAILED_PRECONDITION` "no node runs an envelope" | The envelopes have not been placed yet. Check `legion job status <JOB>`: are there eligible workers with the `sessions` capability? |
| `RESOURCE_EXHAUSTED` "every envelope ... is full" | No envelope has room. Retryable once sessions end; raise `envelope.resources` or add nodes. When the envelopes are [elastic](#elastic-envelopes), the message says so: they grow within a heartbeat, so retry in a few seconds |
| `RESOURCE_EXHAUSTED` "no node accepted the session" | The three nodes tried all refused, with each node's reason |

A create that finds no room is refused rather than queued, so size
envelopes for the peak you want to absorb, or make them
[elastic](#elastic-envelopes).

## Listing sessions

`legion instance list <JOB>` lists the job's rows, then its running
sessions, read from the session index of this manager and its peers.
Session rows show `mode = "session"`, `durable = false`, the node, the
envelope allocation, and when the starting manager issued the session.

The reply adds two fields:

- **`complete`**: whether every node holding an envelope for the job has
  reported its sessions to some manager. It is `false` right after a manager
  starts or fails over, for about one heartbeat per node, and while a node
  is unreachable. Rows are always exact; `complete` speaks only for
  sessions. It is always `true` for a job with no session task.
- **`asOf`**: when the session part was read. A session's own entry can be
  up to one heartbeat older.

**Paging and counts.** A reply carries at most `--session-limit` sessions
(default 5,000, at most 50,000), lowest ids first, and
`nextSessionPageToken` when more remain: pass it back as
`--session-page-token`. Every reply also carries `sessionCount` and
`sessionsByNode`, read from the per-node totals each manager keeps rather
than from the sessions themselves, so `--session-limit 0` counts a job's
sessions at any scale without listing one. A page is read from every
manager's index at that moment, so a session that starts or ends between
pages can be missed or not; a session id that two nodes briefly share can
show only one copy at a page boundary.

A session that started a moment ago appears from the router's record of
the start until its node reports it. A node silent for 120 seconds is
dropped from the index, and its sessions disappear from listings.

`legion instance get <ID>` works on a session id too. The manager gates it
with a coarse `job:read` check before looking the id up, and enforces
`job:read` on the session's own job after, so a caller without access to a
job cannot learn which session ids exist.

## Ending sessions

`legion instance stop <ID>` and `legion instance destroy <ID>` are the same
thing on a session: the manager asks the session's node to end it
(`Worker.StopSessions`), and nothing remains. Neither writes anything, so
`--idempotency-key` records nothing for them. Instead, a repeat on the
manager that ended the session within the last ten minutes answers
`Session <id> already ended`; a repeat on another manager answers
`NOT_FOUND`.

`legion instance start <ID>` on a running session answers
`Instance <id> already running`, as it does for a live keyed instance. On
a suspended one it resumes it ([Idle sessions](#idle-sessions)).

Ending a `store` session also deletes its manifest from the content store,
when the manifest still names the ending node at the session's generation.
A copy that ended because it was stale (superseded, fenced, or a duplicate)
leaves the manifest to the copy that won.

Sessions also end when:

- **The workload exits.** The node forgets it, and reports nothing as a
  failed allocation: it never had one.
- **Its envelope leaves the node,** because the job stopped, the node was
  drained, or an update moved the envelope. The node ends the session 30
  seconds after its envelope goes, so a job update that replaces the
  envelope in place keeps its sessions. A session keeps running the job
  version it started with.
- **The node is lost,** or the worker restarts after the session's process
  died with no checkpoint to restore it from.
- **Its checkpoint is evicted** while it is suspended.

## Worker restarts

A node records each running session in `sessions.db`, a SQLite file beside
the worker's state file (`<state dir>/sessions.db`), when it starts and
removes it when it ends. Writes are batched every 50 ms on a background
thread, so a start never waits on a disk sync.

A worker that restarts reads the file before it serves anything: each
session whose process is still alive is re-adopted under its old id and
claim, with its share of the envelope, and reported on the first
heartbeat; each whose process died is forgotten. Session processes are not
children the worker kills on exit, so a worker restart or upgrade (a
crash, `systemctl restart`, a new binary) keeps them running.

- **What is lost.** A session started in the last 50 ms before a crash may
  not be in the file. Its process keeps running, but the restarted worker
  does not know it and does not report it, until it exits.
- **A machine reboot** kills the processes, so the sessions are gone.
- **The envelope** comes back from the manager's next desired set; until it
  does, re-adopted sessions keep running and count against it once it
  arrives.

## Routing by id

`legion instance get`, `stop`, and `destroy` on a tagged session id go to
the node the tag names (`Worker.GetSession`), one call instead of asking
every manager. The node's answer is final when it holds the session or
says it never did. The manager falls back to the merged view of every
manager's session index when:

- no live node has the tag (the node was removed),
- the node does not answer (the session may still be running; the index
  answers from the node's last report, and when no manager's index has it
  either, the answer is `UNAVAILABLE` naming the node and how long it has
  been silent),
- the node answers `wrong_node` (another node shares the tag, 1 in 2^40
  per pair), or
- the id carries no tag (it was derived from an idempotency key).

A node asked for a session it does not hold answers `NOT_FOUND` with a
reason the caller can act on: `wrong_node:` (the id carries another node's
tag: route by the id, not by a hash of the user's key) or `not_found:`.

## Starting at the edge

A manager's router handles every start by default. A platform that starts
sessions faster than a few managers can route them can start them at the
nodes instead, with no manager on the path and nothing written anywhere:

1. Mint a **session ticket** once, with `boot:write` on the job:

   ```console
   $ legion boot-ticket create agents --task workspace --pool agents \
       --scope session --ttl 1h -j
   ```

   It names the job, the task, and the pool, and no instance or job
   config. Its uses are unlimited (the envelopes bound it), it is never
   spent, and, like every boot ticket, it is revocable only by expiry
   ([edge boot](edge-boot.md#session-tickets)).

2. Keep the node list, refreshed every few seconds:

   ```console
   $ legion session nodes agents -j
   {"ok":true,"data":{"job":"agents","task":"workspace","asOf":"2026-11-02T18:04:11.204Z",
    "nodes":[
     {"node":"w-17","address":"https://10.0.1.7:4848","tag":"q4n7c2rd",
      "accepting":true,"room":{"cpu":21500,"memory":44032},"weight":43,"sessions":17},
     {"node":"w-18","address":"https://10.0.1.8:4848","tag":"h2m5x9pe",
      "accepting":false,"reason":"draining","room":{"cpu":30000,"memory":61440},
      "weight":60,"sessions":0}]}}
   ```

   Every node holding an envelope is listed. `accepting` is false for a
   node that is `lost`, `down`, `draining`, `ineligible`, has `no-address`,
   is `full`, or does not serve edge starts (`no-edge-sessions`: its
   worker config does not set `edgeBoot.acceptClaims`). `weight` is the
   room left in sessions of this task's size. The list is read from the
   manager's own state and is advisory: a stale entry costs a refusal and
   a hop, never a wrong start.

   `--format envoy-eds` prints an Envoy `ClusterLoadAssignment` (nodes not
   accepting are `DRAINING`) and `--format haproxy` prints `server` lines
   (nodes not accepting are `disabled`).

3. Pick a node per start (a consistent-hash ring over the accepting nodes,
   keyed by user or request, is the common choice) and call
   `Worker.StartSession` on its worker-plane port with the ticket. The
   worker plane is TLS: verify the node against the cluster CA
   (`legion net ca show`). The CLI form, for testing and scripts:

   ```console
   $ legion session start agents --task workspace \
       --node https://10.0.1.7:4848 --ticket "$T" -j
   {"ok":true,"data":{"sessionId":"workspace-q4n7c2rd.01jbx...","node":"w-17",
    "state":"running","deduplicated":false}}
   ```

   The node verifies the ticket offline (its signature against the cached
   JWKS, its expiry, and its pool), builds the start from the session
   task's spec, which its envelope carries, admits it against the
   envelope, and launches. By default the reply waits up to 30 seconds
   for the session to be running, and answers as soon as it is.
   `--no-wait` (`wait = false`) answers once admitted. A refusal (`RESOURCE_EXHAUSTED`: the envelope is full or
   every launch slot is busy) is retryable on the next node.

4. Route every later request for the session by its id, never by hashing
   the user's key again: map the id's tag to an address from the node
   list (keep every listed node in that map, accepting or not). `legion
   session get <ID>` and `legion session stop <ID>` (`Worker.GetSession`,
   `Worker.StopSession`) take the same ticket. Managers learn about the
   session from the node's next heartbeat, so `legion instance get`,
   `list`, `stop`, and `promote` work on it as on any session.

**Idempotency at the edge is per node.** `--idempotency-key` derives the
id from the job, the task, and the key, so a retry to the same node finds
the running session (`deduplicated = true`). A retry that lands on another
node starts a second copy, which the managers stop as a
[duplicate](#duplicates). Keep a retry on the node the first attempt
went to.

| Error | Meaning |
| --- | --- |
| `UNAUTHENTICATED` | No ticket on the request |
| `PERMISSION_DENIED` "session ticket refused" | The ticket's signature, expiry, or pool does not hold on this node, or it is not a session ticket |
| `PERMISSION_DENIED` "does not cover" | The ticket names another job or task |
| `FAILED_PRECONDITION` "does not accept edge starts" | The node's worker config does not set `edgeBoot.acceptClaims` |
| `FAILED_PRECONDITION` "no envelope" | This node holds no envelope for the task (yet) |
| `RESOURCE_EXHAUSTED` | The envelope is full, or no launch slot freed within `edgeBoot.queueTimeout`. Retry on the next node |
| `NOT_FOUND` `wrong_node:` | `get`/`stop` reached a node the id's tag does not name |
| `ALREADY_EXISTS` "already runs on node N" | An idempotent start of a `store` session that another node created first. The message ends in `[winner node=<id> generation=<g>]`: send the session's requests there |

**`store` sessions at the edge.** For a task with
`suspend.durability = "store"`, an idempotent edge start creates the
session's manifest in the node's `contentStore` before the session's gate
opens, with a conditional create (`If-None-Match: *`). A retry that lands
on another node therefore does not start a second copy: it answers
`ALREADY_EXISTS` naming the node that won. A minted id cannot collide, so a
start without a key writes nothing until the session's first checkpoint.

`legion session start --resume-id <ID>` (`StartSession.resume_id`) resumes
an existing `store` session on the node it is sent to, from its manifest
([Moving a session](#moving-a-session)). The reply adds `resumedFrom`,
`generation`, and `movedFrom`.

## Promotion

`legion instance promote <SESSION> [--id NAME]` (`PromoteInstance`) turns a
running session into a keyed instance in place:

1. One scheduler-lane raft entry writes an instance row (`state = active`,
   `promotedFrom` set to the session id) and an allocation bound to it on
   the session's node, together with the `instance.promoted` event and the
   idempotency record.
2. The manager pushes the allocation to the node, which **adopts** the
   running session as that allocation instead of starting anything. A node
   the push misses adopts it from its next desired set.
3. From then on the instance is keyed: listed exactly, rescheduled on node
   loss (a replacement reserves the task's full `resources`, like any keyed
   placement), suspendable, forkable, and re-adopted across a worker
   restart.

Without `--id` the instance keeps the session id, which stays unnamed: its
row is deleted on destroy, like any minted id. With `--id` it takes a name
that no instance has ever had, and that name keeps a tombstone after
destroy. The running workload keeps the `LEGION_INSTANCE_ID` it started
with until it restarts.

The promoted allocation reserves nothing of its own. The session already
sits inside its node's envelope, and keeps that share of the envelope until
it stops, so capacity is never counted twice.

Promotion is retry-safe without a key: when the instance a request would
create already exists and was promoted from the same session, the reply
describes it with `deduplicated = true`.

| Error | Meaning |
| --- | --- |
| `NOT_FOUND` | No running session has the id |
| `ALREADY_EXISTS` | The id names an instance (including a tombstone) not promoted from this session |
| `INVALID_ARGUMENT` | `--id` is not a valid keyed id, or the task sets `promote.onName = false` |
| `FAILED_PRECONDITION` | The session's job config no longer has its task |

The commands that need a row (`fork`, `snapshot create`,
`snapshot restore`, `replace`, and `suspend` on a session whose task has no
`suspend` block) refuse a session id with `FAILED_PRECONDITION` and a hint
to promote it first.

## Idle sessions

A session whose task has a `suspend` block can sleep on its own node: the
node checkpoints it into its local content store, stops it, frees its
share of the envelope, and keeps the checkpoint. Resuming boots it from
that checkpoint on the same node, under the same id. None of this writes
the replicated store. It is the cheap middle ground between a running
session (which holds envelope room while idle) and a promoted one (which
pays a raft row to survive its node).

```nickel
suspend = {
  durability = "local",          # or "store": publish to the contentStore
  idle = { after = "5m" },       # suspend after 5 minutes of no activity
  keepFor = "24h",               # how long the checkpoint is kept
  checkpointEvery = "10m",       # optional; memory checkpoints only
  onWorkerStop = "checkpoint",   # or "keep" (the default)
},
```

| Field | Default | Meaning |
| --- | --- | --- |
| `suspend.durability` | `"local"` | Where the checkpoint lives. `"local"` keeps it on the session's node. `"store"` also publishes it, sealed, to the node's `contentStore`, so the session can resume on any node with an envelope for the task ([Moving a session](#moving-a-session)). A node without a `contentStore` refuses to suspend a `store` session with `FAILED_PRECONDITION` |
| `suspend.idle.after` | none | Suspend the session after this long with no activity, measured by the node as for keyed instances ([idle policy](suspend-and-resume.md#idle-policy)) |
| `suspend.keepFor` | `"24h"` | How long a suspended session's checkpoint is kept before it is evicted. At least 60 seconds |
| `suspend.checkpointEvery` | off | Take a live checkpoint of a running session this often, replacing the previous one. At least 60 seconds; memory checkpoints only |
| `suspend.onWorkerStop` | `"keep"` | What the worker does to running sessions when it is stopped: `"keep"` leaves them running untouched, `"checkpoint"` checkpoints each one first (they keep running) |

With `durability = "store"`, every checkpoint of the session (a suspend, a
`checkpointEvery` checkpoint, an `onWorkerStop` checkpoint) is published and
recorded in the session's manifest ([Session manifests](#session-manifests)).

**Suspending.** `legion instance suspend <ID>` (`SuspendInstance`) on a
session routes, by the node tag in the id, to that node
(`Worker.SuspendSessions`). The reply names the checkpoint, its kind, the
node, and `durability = "local"`; a second suspend answers `already
suspended`. The node also suspends a session on its own when its
`idle.after` passes. `instance get` and `instance list` then show
`state = "suspended"` and the session no longer counts against its
envelope.

**Resuming.** `legion instance start <ID>` (`StartInstance`) on a suspended
session routes to its node (`Worker.ResumeSession`), which admits it into
the envelope again and boots it from the checkpoint. The reply says
`Instance <id> resumed on <node>` with `resumingFrom = "local"`. The id
and its generation (1) are unchanged, and the checkpoint is released once
the session runs. A session on the `exec` runtime has no memory to
capture, so its checkpoint is a disk one and its resume is a fresh start
of the same spec under the same id.

| Answer | When |
| --- | --- |
| `RESOURCE_EXHAUSTED` (retryable) | The envelope has no room for the session right now. It stays suspended with its checkpoint; retry when another session ends or suspends |
| `NOT_FOUND` "session X was evicted from NODE at TIME (reason); a local-only session cannot be restored" | The checkpoint was evicted. The hint suggests starting a new session, or `durability = "store"` once sessions support it. The node gives this answer for another `keepFor` after the eviction, then forgets the id |
| `UNAVAILABLE` "node NODE holds session X and ..." | The node the id names does not answer and no manager's index lists the session. A local session lives only on its node: retry when the node is back. The message says what the cluster last knew of the node |
| `FAILED_PRECONDITION` | The task has no `suspend` block (promote the session first), or sets `durability = "store"` on a node with no `contentStore` |
| `ABORTED` `superseded:` | A `store` session's manifest names a newer generation on another node: this node's copy was stale and has ended. The message names the winner |
| `UNAVAILABLE` "the content store ... could not be reached" | A `store` session's resume could not claim its generation. Nothing started; retry |
| `ABORTED` | Another capture or resume of the session is in progress |

`instance get` on an evicted session, or on one whose node is silent, gives
the same `NOT_FOUND` and `UNAVAILABLE` answers.

**Eviction.** A suspended session's checkpoint is pinned in the node's
content store until one of these evicts it:

- `keepFor` passes since it was suspended;
- its envelope leaves the node (the job stopped or the node was drained)
  and stays gone for 30 seconds;
- the content filesystem drops below `content.minFreeDisk`: the node evicts
  suspended sessions oldest first until it is back above the floor;
- `instance stop` or `instance destroy` on the suspended session, which
  ends it outright.

Evictions are counted in `legion_session_evictions_total{reason}`
([metrics](metrics.md#sessions)).

**Periodic checkpoints (`checkpointEvery`).** With it set, the node pauses
each running session at that interval, captures its memory, and resumes
it, replacing the session's previous checkpoint. The first checkpoint of
each session lands at a stable point in the second half of the interval
after it starts, so sessions started together do not capture together. The
node runs one capture at a time; when its backlog would delay a round by
more than one interval, it skips the most recently checkpointed sessions
for that round and counts them in
`legion_session_checkpoints_skipped_total`. A periodic checkpoint is what a
restarted worker restores from (below); it is not a backup that survives
the node.

The cost is write volume. Each capture writes about the session's guest
memory, and the content store keeps one checkpoint per session. For 40
sessions of 512 MiB with `checkpointEvery = "10m"`:

- writes: 40 x 512 MiB x 6 an hour = 120 GiB an hour, about 34 MiB/s
  sustained;
- disk held: 40 x 512 MiB = 20 GiB, plus the checkpoints of suspended
  sessions;
- capture time: at about 2 s per 512 MiB capture, 80 s of every 600 s is
  spent capturing, each session paused for its own 2 s.

Halving the interval doubles the writes; size `content.minFreeDisk` and
the interval together.

**Worker stops (`onWorkerStop`).** A worker that gets SIGTERM or SIGINT
leaves its workloads running, as it always has, and a restarted worker
re-adopts the sessions still running. With `onWorkerStop = "checkpoint"`
the worker first checkpoints each running session, least recently
checkpointed first, until `edgeBoot.stopCheckpointTimeout` (default 60s)
runs out, and then exits. When it starts again, a session whose process
died meanwhile (the host rebooted, or the process was killed) is restored
from that checkpoint, or from its latest periodic one, under the same id.
A session with no checkpoint, or with `onWorkerStop = "keep"`, ends as
before. Sessions the timeout did not reach are logged as left.

**What is not covered.** A `local` suspended session lives on one node.
Losing the node, or its disk, loses the session: there is no copy
elsewhere and no other node can resume it. Use `durability = "store"` for
sessions that must survive their node, or promote them.

| Not available for sessions | Instead |
| --- | --- |
| `suspend.reserve` | Size the envelope for the sessions you want running |
| `checkpointEvery` with a disk checkpoint (`exec`) | `onWorkerStop = "checkpoint"` alone, or the sandbox runtime |
| Wake on traffic (`suspend.wake`) | `instance start` from the caller |

## Moving a session

A session whose task sets `suspend.durability = "store"` is not tied to its
node. `legion instance start <ID>` first asks the node the id's tag names.
When that node no longer holds the session (it dropped its published local
copy, or was replaced), is full, or does not answer, the manager resumes
the session from its manifest on another node with room, in the router's
order:

```console
$ legion instance start workspace-q4n7c2rd.01jbx... -j
{"ok":true,"data":{"message":"Instance workspace-q4n7c2rd.01jbx... moved from w-17
  and resumed on w-18 (generation 3)","nodeId":"w-18","resumingFrom":"store"}}
```

The new node admits the session into its envelope, claims the next
generation in the manifest, leaves a fence marker for the old node, and
restores the session from the published checkpoint (fetching any part it
does not have, and opening it with the job's session sealing key). The id
keeps its original node tag, so a later request asks that node first, gets
`not_found`, and is answered from the manifest and the managers' index.

**Moving is cautious for running sessions.** When the manifest says the
session is *running* on another node, the receiving node refuses with
`running_elsewhere`, and the manager probes that node itself
(`Worker.GetSession`, 2 seconds):

- it runs the session there: the answer is `Instance <id> already running
  on <node>`;
- it answers without the session: the move goes ahead;
- it does not answer: the move goes ahead only when the managers have
  marked that node `down` or `lost`. Otherwise the answer is `UNAVAILABLE`
  `running_elsewhere`, naming the node. Pass `--force-move`
  (`StartInstance.force_move`) to move anyway once you know the node is
  gone; until it is fenced the old copy may still run.

A suspended session moves at once. A move writes nothing to raft.

**At the edge.** `legion session start <JOB> --task T --node <ADDR>
--ticket "$T" --resume-id <ID>` (`StartSession.resume_id`) moves a session
to the node the request is sent to. The node probes the node the manifest
names with the same ticket (`GetSession`, 2 seconds): when the session
runs there, it claims the next generation, asks that node to stop its copy
(`StopSession`), and launches. When that node does not answer, it refuses
`UNAVAILABLE` unless the request sets `--force-move`; a picker sets it when
`legion session nodes` lists the old node as `down` or `lost`. A
`StartSession` that resumes answers with `resumedFrom = "store"` (or
`"cold"`), the `generation` it claimed, and `movedFrom`.

## Session manifests

A `store` session's record lives beside its checkpoints in the worker's
`contentStore`, one JSON object per session:

```
<contentStore.url>/sessions/<job>/<id-prefix>/<id>.json
```

`<id-prefix>` is the first two characters of the id's random part, so a
job's manifests spread over many prefixes. A manifest holds the session's
job, job config, task, `generation`, the `node` and node address that hold
it, its `state` (`running` or `suspended`), its first lease, its resources,
and its latest published checkpoint (the snapshot manifest, every part with
its `url` and sealing `keyId`).

- **Sealing.** Every published part is sealed (AES-256-GCM). A store
  session's envelope payload carries the job's session sealing key, derived
  by the manager from the cluster's checkpoint key for that job, and the
  node derives each part's key from it. Any node with an envelope of the
  job can open the job's session checkpoints, and no other job's. The key
  is never written to disk or logged.
- **Chunks.** Each part is published as chunks, and the node derives the
  job's chunk key from the same session sealing key, so a periodic
  checkpoint uploads only the chunks the session changed since the last
  one, and sessions of one job share their image's chunks with each other
  and with the job's snapshots ([How published parts are
  stored](suspend-and-resume.md#how-published-parts-are-stored)). A bucket
  lifecycle rule on `sealed/` must be older than the longest-lived
  checkpoint plus the workers' `contentStore.dedupWindow`.
- **Status.** `SessionStatus.checkpoint` (in `instance get`) reports the
  session's checkpoint: its `durability`, snapshot id, kind, size, when it
  was taken, and, once published, `publishedUrl`, the manifest's URL.
- **After publishing,** the node's local copy of the parts is an ordinary
  cache entry: content-store pressure can evict it, and a suspended
  session whose local copy is evicted is dropped from the node quietly (no
  `evicted` answer), so the next start resumes it from the manifest.
- **When the content store is unreachable,** a suspend still suspends the
  session on its node; the publish is retried every few seconds, and
  `publishedUrl` stays empty until it lands. A resume needs the store (it
  must claim its generation) and answers `UNAVAILABLE` until it is back.
- **Ending** the session deletes the manifest (see [Ending
  sessions](#ending-sessions)). A manifest of a session that is suspended
  forever stays until the job's sessions are ended; use a bucket lifecycle
  rule for anything older than you care to keep.

Writes are conditional: a manifest is created with `If-None-Match: *` and
updated with `If-Match` on the ETag it was read at. On `s3://` this needs a
store that supports conditional writes (AWS S3, MinIO, R2); on `file://`
the node takes a lock file beside the manifest and compares a content hash.

## No split brain

A `store` session can run on one node at a time even across partitions:
every resume claims a new **generation** before its gate opens, and four
mechanisms stop an older copy.

1. **Superseded on write.** Every manifest write a copy makes (a periodic
   checkpoint, a suspend, an `onWorkerStop` checkpoint) checks that the
   manifest still names its node at its generation. A copy that finds a
   newer generation ends itself with reason `superseded`.
2. **Fence list.** A move leaves `sessions/fence/<old-node>/<id>` in the
   content store. Every node lists its own fence prefix every
   `sessions.fenceListEvery` (worker config, default `30s`), stops each
   session at or below the fenced generation, and deletes the marker.
3. **Probe before move.** A running session moves only after its old node
   answered that it stopped (or does not have it), or after that node is
   marked down or lost, or with `force_move` ([Moving a
   session](#moving-a-session)).
4. **Self-fencing.** A node that has reached neither a manager nor its
   content store for `stateful.fenceAfter` (default `2m`) suspends its
   running `store` sessions on its own disk, without publishing. When the
   content store answers again it resumes, claiming the next generation,
   only the ones whose manifest still names it, and ends the rest (they
   moved meanwhile). Set `stateful.fenceAfter` shorter than the time after
   which you would force a move.

Every claim is a conditional write, so of two racing resumes exactly one
wins; the other answers `ABORTED` naming the winner:
`superseded: another resume claimed the session first; generation 3 runs
on node w-18 [winner node=w-18 generation=3]`. The bracketed tail is
stable for programs to parse.

The worker config's `sessions` block:

```nickel
sessions = {
  fenceListEvery = "30s",   # one LIST of this node's fence prefix per interval
},
```

Counters: `legion_session_superseded_total{via}` (copies ended because a
newer generation runs elsewhere; `via` is `write`, `fence_list`, `resume`,
or `reconnect`), `legion_session_moves_total`, and
`legion_session_self_fenced_total` ([metrics](metrics.md#sessions)).

**What this does not cover.** Between a forced move and the old node's
next manifest write or fence listing, two copies can run; mechanism 4
bounds that for a node that is cut off. A periodic checkpoint is the
newest state a move restores: work since then is lost. A session on the
`exec` runtime has a disk checkpoint, which holds only its spec, so it
moves as a fresh start under the same id.

## Duplicates

Two nodes can briefly run one session id: an idempotent create retried
through two managers before either node reported the first start, or an
idempotent edge start retried on another node. The copy with the higher
`generation` is kept, then the earlier start (`leaseIssuedAt`), then the
lower node id, and the other node is asked to end its copy:

- by the manager whose index holds both copies, in that node's heartbeat
  reply (`HeartbeatReply.stop_sessions`), until the node stops listing it;
- by any manager that reads both copies while answering `get`, `stop`, or
  an idempotent create;
- by every manager's duplicate sweep: once a second, each manager asks its
  peers, in one call, about the idempotency-derived ids newly reported to
  it, and asks the node of each losing copy to end it. This is what
  catches two copies whose nodes heartbeat to different managers, including
  a retried edge start. Only derived ids are swept: an id minted by a node
  carries that node's tag and cannot run on two.

This is converge-to-one, not at-most-one. A session that needs at most one
copy at every instant should be promoted (or created with `--id`).

## Elastic envelopes

A fixed envelope makes you pick one size for the whole day. Sized for the
peak, it holds room all night that keyed sandboxes, services, and batch
work on the same nodes cannot use. Sized for the average, it refuses the
peak while the node has room nobody is using. Set `envelope.max` and each
node sizes its own envelope between the two:

```nickel
stateful = {
  mode = "session",
  envelope = {
    resources = { cpu = 6000, memory = 6144 },     # the floor: always held
    max = { cpu = 30000, memory = 30720 },          # the ceiling
    shrinkAfter = "10m",                            # the default
  },
},
resources = { cpu = 500, memory = 512 },            # one session
```

**How a node sizes it.** At each heartbeat the node compares what its
running sessions hold with its envelope's size, per resource:

- **It grows** when its sessions fill more than 90% of the envelope, or one
  more session would not fit, to the size they would fill to 75%, with room
  for one more session, up to `max`.
- **It shrinks** when its sessions have used less than half of the envelope
  for `shrinkAfter`, to the same 75% size, down to `resources`.

Only running sessions count. A suspended session holds no room, so an
envelope can shrink while its sessions sleep, and a resume into a shrunk
envelope is answered `RESOURCE_EXHAUSTED` (retryable) and grows it.

**Every resize goes through the scheduler's records.** The node asks on
its heartbeat, and the manager rewrites the envelope's allocation in one
scheduler-lane raft entry. Plan apply, preemption, bin packing, `legion
node usage`, and `legion session nodes` all read that allocation, so they
show the size the node admits against. A grow takes only the node's free
room: when the node has less free than the grow wants, the envelope takes
what is there, and when it has none the grow is refused, writes nothing,
and the node waits 30 seconds before asking again. A grow never preempts
other work. A shrink frees room the way an allocation ending does, so work
blocked on the node is woken to place again.

**A shrink never strands a session.** Before the node asks to shrink, it
caps its own admission at the smaller size, and keeps the cap until the
manager answers. Sessions started at the edge in the meantime are admitted
against the cap, so when the shrink commits, the node's sessions fit it.

**What it costs.**

- **Burst room.** A fixed envelope at its ceiling absorbs a burst at once.
  An elastic one absorbs bursts up to its free room (about a quarter of its
  size after a resize) and grows within one heartbeat (2 seconds on fleets
  up to 2,500 nodes) for the rest. Starts in between are refused with a
  retryable `RESOURCE_EXHAUSTED` whose message says the envelopes are
  growing. Size the floor for the burst you must absorb instantly, at any
  hour.
- **Control-plane writes.** One raft entry per resize. With the defaults,
  under a load that swings 4 to 1 over a day, a node resizes its envelope
  about 26 times a day, so 1,000 nodes add about 26,000 entries a day, or
  0.3 a second. A refused grow writes nothing. Lengthen `shrinkAfter` for
  fewer resizes and more held room.
- **Room held for others.** Room an envelope grows into is held until its
  sessions drain for `shrinkAfter`, even when a higher-priority job wants
  it. Preemption can still evict the whole envelope, as for any
  allocation, but it cannot take back only the grown part.

A fixed envelope (no `max`, or `max` equal to `resources`) never resizes
and behaves exactly as before. `legion_envelope_resizes_total{direction,
outcome}` counts resizes on the managers ([metrics](metrics.md#sessions)).

## Capacity planning

- Burst headroom is the envelopes' free room. Envelope capacity is reserved
  whether sessions use it or not, and counts against node capacity like any
  allocation. An [elastic envelope](#elastic-envelopes) reserves its floor
  and grows toward its ceiling with its sessions.
- Watch `legion_envelope_resizes_total{outcome="refused"}`: a steady rate
  means elastic envelopes want room their nodes do not have.
- A larger envelope admits more sessions per node but leaves less room for
  other jobs. Several session tasks can share a node, each with its own
  envelope.
- Watch `legion_session_starts_total{outcome}`: a rising `no_room` or
  `refused` count means the envelopes are too small for the arrival rate.
- Watch `legion_worker_launch_refusals_total` on the nodes: a rising count
  means starts arrive faster than `edgeBoot.launchConcurrency` launches
  finish. Raise it if the runtime's boot is I/O-bound, or add nodes.

## In the UI

The job page's Instances card lists sessions beside keyed instances, with a
`session` badge, the node and envelope each runs in, and **Promote** and
**End** per row (`job:write`). Above the table it counts sessions by node and
says whether every node has reported (`complete`) and when the index was read
(`asOf`). Envelope allocations say what they reserve and link to their
sessions. See [UI architecture](ui-architecture.md#sessions) for every page.

Because `ListInstances` returns every session in one reply, the UI renders
100 rows at a time, stops re-polling a listing of 5,000 or more, and does not
fetch at all, until asked, when the job's envelopes could hold more than
50,000 sessions. For the full list use `legion instance list <job> -j`.

## Agents

Sessions use the same surfaces as keyed instances: `legion_instance_create`
(omit `instance_id`), `legion_instance_list`, `legion_instance_get`,
`legion_instance_stop`, and `legion_instance_promote` over MCP, plus
`legion_session_nodes` for the edge node list and
`legion_boot_ticket_create` with `scope = "session"`, and the
`--json` envelope on the CLI. An agent should always pass an idempotency
key on create, read `durable` before relying on a unit surviving a node,
and treat `complete = false` on a listing as "some sessions may be
missing", not as "these are all of them". See
[agent readiness](agent-readiness.md).

## Upgrades

Managers and workers must both run a version with session support. A
worker without it does not advertise `sessions`, so no envelope is placed
on it and no session is sent to it. See [upgrades](upgrades.md).

## Limitations

- **Unnamed sessions cannot be forked or snapshotted.** Promote one for
  that.
- **A `local` suspended session dies with its node.** Its checkpoint is on
  that node only, until `keepFor` or disk pressure evicts it. A `store`
  session survives it, from its last published checkpoint.
- **Moving needs a content store with conditional writes** (`If-Match`,
  `If-None-Match`) on every node of the job, all pointing at the same
  bucket. A node with another `contentStore` cannot see the session.
- **A forced move can briefly leave two copies** until the old node's next
  manifest write, fence listing (`sessions.fenceListEvery`), or self-fence
  (`stateful.fenceAfter`) stops it.
- **Manifests of sessions that never end stay in the bucket.** A suspended
  `store` session past `keepFor` is dropped from its node but its manifest
  is kept, so it can still resume; clean up with a bucket lifecycle rule.
- **Idle detection needs resource enforcement** (cgroups) on the node, as
  for keyed instances. Without it, `idle.after` never fires; `instance
  suspend` still works.
- **No workload identity** for sessions: a session task may not declare
  `identities`, so a promoted session has none either.
- **No per-session events or audit log.** Starts and stops are counted
  (`legion_session_starts_total`, `legion_session_stops_total`), not
  journaled, and no node-side session log is kept or shipped.
- **Edge starts need `edgeBoot.acceptClaims`** on the node, and a session
  ticket is revocable only by expiry. Idempotency at the edge is per node.
- **Simulated workers** (`legion start worker --simulate`) do not run
  sessions.
- **Ending a session is remembered per manager,** so a retried stop that
  lands on another manager answers `NOT_FOUND`.
- **A crash can lose the last 50 ms of starts from `sessions.db`.** Those
  sessions keep running unreported until they exit. A machine reboot ends
  the node's sessions.
- **A worker restart ends only the sessions whose process died** while it
  was down, unless `suspend.onWorkerStop = "checkpoint"` or
  `checkpointEvery` left a checkpoint to restore them from.
- **Elastic envelopes resize no faster than heartbeats.** A burst larger
  than an envelope's free room waits one heartbeat for a grow, and the
  growth thresholds (90%, 75%, 50%) are fixed. The grown part of an
  envelope is not preemptible on its own: a higher-priority job waits for
  `shrinkAfter`, or preempts the whole envelope.
- **A job update places each new elastic envelope at its floor.** The node
  grows the new envelope on its next heartbeat, once the old one is gone.
  Until then the node's running sessions can hold more than their envelope,
  and work the scheduler places on the node in that window can leave it
  overcommitted until sessions end. A fixed envelope has a shorter version
  of the same window, between the old envelope stopping and the new one
  being placed.
- **A worker restart forgets an elastic envelope's shrink in flight.** The
  restarted worker admits against the size its next desired set carries. If
  that desired set comes from a manager whose replica has not yet applied a
  shrink committed just before the restart, the node can admit past the
  shrunk size until its next desired set, which is one heartbeat.
