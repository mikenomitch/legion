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
the log entirely. Measured on the same 4-vCPU host, with one manager and 16
test nodes in one process and the runtime's own boot left out, the control
plane started about 970 sessions a second with 64 in flight, and about 380
a second one at a time at 2.5 ms each, with no replicated write for any of
them. Start capacity comes from the nodes' envelopes and from whichever
manager routes the start, so it grows with the fleet rather than with the
log.

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
constrained to workers that advertise the `sessions` capability. The
scheduler places envelopes like any allocation, so preemption, bin
packing, and `legion node usage` count the room sessions use. An envelope runs nothing:
the allocation is the reservation.

**A create goes straight to a node.** `CreateInstance` without an
`instance_id` on a session task does not touch the store. The manager's
session router reads which nodes hold an envelope for the task and how much
of each is in use, picks the node with the most room left, and sends the
start to that node directly over the worker's `PostWork` RPC. The node
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
picker tries the next node. A session start never rewrites the worker's
state file.

**Nodes report what they run.** Each worker reports its sessions on its
heartbeats, as deltas against an epoch, with a full report every 30
heartbeats and whenever the manager asks for one. Each manager keeps an
in-memory **session index** of the nodes that heartbeat to it. Nothing in
it is persisted or replicated; a manager that starts, or takes a node over
after a failover, asks the node for a full report on its first heartbeat.
A manager answering for a session it does not index asks its peers over the
raft transport and merges their answers, preferring whichever manager heard
from a node most recently.

**The router counts its own starts.** For about 30 seconds after a start,
until the node's next report lists it, the router counts the session from
its own record, so back-to-back creates do not all land on the same node.
The router caches its view of envelope room for one second. The node stays
authoritative: an over-count costs a retry on the next node, never an
over-full envelope.

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
| Node loss while running | Rescheduled elsewhere, bound to its storage | Lost. A running session is not rescheduled |
| Worker restart | The allocation is re-adopted | Re-adopted too: the node records running sessions on its own disk ([Worker restarts](#worker-restarts)) |
| At most one live copy | Exact | Converges to one: a retried idempotent create can briefly run on two nodes ([Duplicates](#duplicates)) |
| Who starts it | A manager, through raft | A manager's router, or the node itself on a session ticket ([Starting at the edge](#starting-at-the-edge)) |
| Ids never recycled | Exact for the job's life | Probabilistic: 130 random bits, no tombstone |
| `instance list` | An exact read | From the session index, with `complete` and `asOf` ([Listing](#listing-sessions)) |
| Idempotent create | A replicated record | A derived id: a retry gets the same id, with no record |
| Suspend, resume, fork, snapshot | Yes | No. Promote first |
| Events | The replicated journal | None per session. `instance.promoted` is journaled, and counters cover starts and stops |
| Capacity | Reserved per instance at placement | Reserved per node as envelopes, whether or not sessions fill them |
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
},
resources = { cpu = 500, memory = 512 },
```

| Field | Default | Meaning |
| --- | --- | --- |
| `stateful.mode` | | `"session"` |
| `stateful.envelope.resources` | required | What each node keeps for this task's sessions. Must set `cpu` and `memory`, and every amount the session's own `resources` declares. Sessions are admitted against exactly these keys |
| `stateful.envelope.constraints` | none | Extra placement constraints for the envelopes, added to the task's own `constraints` |
| `stateful.promote.onName` | `true` | Whether a caller-chosen id is accepted: `instance create --id` (a keyed instance from birth) and `instance promote --id` (a rename). With `false`, a create always starts an unnamed session and a promotion keeps the session's id |
| `resources` | | One session's size, in the envelope's units |

`job validate` refuses, each with a hint:

- `envelope` without `mode = "session"`, `mode = "session"` without
  `envelope`, and `promote` on any other mode.
- A session that does not fit its envelope, or that declares a resource the
  envelope does not size.
- `requireId`, `initialInstances`, `stateful.storage`, and `volumes`.
- Ports, a `service`, `connect` upstreams, a network policy, or gated
  networking. The node's datapath serves allocations, and an unnamed
  session has none. Give a sandbox its own `config.network`.
- `identities`. Workload identity for unnamed sessions is not available.
- `speculativeStart`: a session already starts without a raft write.
- `suspend` with `durability = "local"` or a `reserve`. A promoted session
  can be suspended, and its node is wherever its envelope had room, so its
  checkpoint must be published.
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
| `RESOURCE_EXHAUSTED` "every envelope ... is full" | No envelope has room. Retryable once sessions end; raise `envelope.resources` or add nodes |
| `RESOURCE_EXHAUSTED` "no node accepted the session" | The three nodes tried all refused, with each node's reason |

A create that finds no room is refused rather than queued, so size
envelopes for the peak you want to absorb.

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
`Instance <id> already running`, as it does for a live keyed instance.

Sessions also end when:

- **The workload exits.** The node forgets it, and reports nothing as a
  failed allocation: it never had one.
- **Its envelope leaves the node,** because the job stopped, the node was
  drained, or an update moved the envelope. The node ends the session 30
  seconds after its envelope goes, so a job update that replaces the
  envelope in place keeps its sessions. A session keeps running the job
  version it started with.
- **The node is lost or the worker restarts.**

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
  answers from the node's last report),
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
   for the session to be running; `--no-wait` (`wait = false`) answers
   once admitted. A refusal (`RESOURCE_EXHAUSTED`: the envelope is full or
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

The commands that need a row (`suspend`, `fork`, `snapshot create`,
`snapshot restore`, `replace`) refuse a session id with
`FAILED_PRECONDITION` and a hint to promote it first.

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
- by the manager that served an idempotent create, which looks again 20
  seconds later.

This is converge-to-one, not at-most-one. A session that needs at most one
copy at every instant should be promoted (or created with `--id`).

## Capacity planning

- Burst headroom is the envelopes' free room. Envelope capacity is reserved
  whether sessions use it or not, and counts against node capacity like any
  allocation.
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

- **Unnamed sessions cannot be suspended, forked, or snapshotted.** Promote
  one first. Idle sessions kept only as bucket manifests are not available.
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
