# Child Sessions

A **child** is a [session](session-mode.md) or a
[keyed instance](stateful-workloads.md) that another unit of the same job
started and owns. It ends when its owner ends, is charged to its own task's
envelope or reservation, runs under its own task's resources and network
policy, carries a reference to its owner, and is listed under it.

```console
$ legion instance create agents --task machine --owner agent-q4n7c2rd.01jbx... -j
{"ok":true,"data":{"instanceIds":["machine-q4n7c2rd.01jbz..."],"mode":"session","durable":false,
 "node":"w-17","state":"starting","deduplicated":false,
 "owner":{"kind":"session","id":"agent-q4n7c2rd.01jbx..."}}}
```

It exists for workloads that need another sandbox for part of their work:
an agent that wants a scratch machine for one build, a CI run that fans out
into test sandboxes, a front-end session that hands heavy work to a bigger
one. Without ownership, the application has to hold a credential to start
the second unit, remember to stop it on every failure path, and work out
for itself which unit asked for which.

- [Declaring children](#declaring-children)
- [Owners](#owners)
- [Starting a child](#starting-a-child)
- [Keyed children](#keyed-children)
- [When an owner ends](#when-an-owner-ends)
- [Promotion](#promotion)
- [Listing, and learning that a child is gone](#listing-and-learning-that-a-child-is-gone)
- [What a child runs as](#what-a-child-runs-as)
- [Addressing](#addressing)
- [Connection handoff](#connection-handoff)
- [Failure modes](#failure-modes)
- [Limitations](#limitations)

## Declaring children

The owner's task names the tasks it may own children of, in a top-level
`children` block. The block is not under `stateful`, because a service or
batch task, which has no `stateful` block, can own children too.

```nickel
{
  name = "agents",
  tasks = [
    {
      name = "agent",
      runtime = "exec",
      config = { command = ["agent-loop"] },
      resources = { cpu = 500, memory = 512 },
      stateful = { mode = "session", envelope = { resources = { cpu = 16000, memory = 32768 } } },
      children = {
        tasks = ["machine"],        # session tasks of this job
        max = 2,                    # per owner
        suspendWithOwner = false,
        orphanAfter = "10m",
      },
    },
    {
      name = "machine",
      runtime = "sandbox",
      config = { image = "python:3.13-slim" },
      resources = { cpu = 2000, memory = 4096 },
      stateful = { mode = "session", envelope = { resources = { cpu = 32000, memory = 65536 } } },
    },
  ],
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `tasks` | required | The tasks of this job whose units this task's units may own. |
| `max` | `1` | The most children one owner holds at once. |
| `suspendWithOwner` | `false` | Whether an owner's children suspend when it does. By default each child follows its own task's [idle policy](session-mode.md#idle-sessions). |
| `orphanAfter` | `10m` | For an owner that is a session: how long a child outlives an owner that cannot be reached (see [When an owner ends](#when-an-owner-ends)). At least `1m`. |

The child task is an ordinary session task with its own envelope,
resources, network, and suspend policy. Nothing on it says it is a child
task, and any of its sessions may be a child or not.

Job validation checks the block when the job is submitted:

```text
[children] agents/agent: children.tasks names "machine", which is not a task in this job
[children] agents/agent: children.tasks names "web", a service task; children are sessions (stateful.mode = "session") or keyed instances (stateful.mode = "keyed")
[children] agents/agent: children.tasks names "workspace", a keyed task, but agent is a session task; a session cannot own a durable child (promote the session, or make agent keyed)
[children] agents/machine: a child task cannot declare children (depth 1 only)
```

A keyed task is accepted in `tasks` when the owner is not a session task,
but only sessions can be started as children today; see
[Limitations](#limitations).

## Owners

Any running unit of the job can be an owner. A child stores one reference
to it, `{ kind, id }`, and the kind decides who says the owner has ended:

| `kind` | `id` | Who decides it ended |
| --- | --- | --- |
| `session` | a session id | the node running it |
| `instance` | a keyed or slot instance id, which outlives its allocations | the managers |
| `allocation` | an allocation id, for an owner with no instance identity (a service replica, a batch run) | the managers |
| `isolate` | an invocation id: a request or tenant [isolate](task-runtime-isolate.md#child-vms) that called `vm.acquire()` | the node running it; the children end with the invocation |

Callers may leave the kind out; Legion works it out from the id. The
`isolate` kind is set only by the node, when an isolate starts a child VM;
a session isolate owns its children as an ordinary `session` owner, found
on the node its id names. Managers keep no record of isolates, so they skip
`isolate` owners and leave judging them to the node. The node binds an
isolate owner to its job and task, and acts on a child only for an
isolate it is running for that job and task. An
allocation that belongs to an instance is always owned through the
instance, so the reference survives a reschedule.

The reference is stored **with the child**, wherever the child's own
record lives:

| Child | Where the reference lives | Raft cost |
| --- | --- | --- |
| session | its node's session table (`sessions.db`, so a restarted worker still knows it) and the session report the node sends the managers | none |
| keyed instance | an indexed `owner` column on its instance row, written by the create | none beyond the create |

## Starting a child

There are three ways in, and the same checks behind each. An
[isolate](task-runtime-isolate.md#child-vms) has a fourth: `vm.acquire()`
starts a child of its task's VM task on its own node, checked against the
isolate task's `children` block like a start from inside.

| Who | How | Authorized by |
| --- | --- | --- |
| An operator or platform, through a manager | `legion instance create <job> --task <child task> --owner <id>` (`CreateInstanceRequest.owner`; MCP `legion_instance_create` with `owner`) | `job:write` on the job, and the owner task's `children` block |
| A caller holding a [session ticket](session-mode.md#starting-at-the-edge), at a node | `legion session start <job> --task <child task> --node <addr> --ticket <t> --owner <id>` (`StartSessionRequest.owner`) | the ticket, and the `children` block. The owner must run on that node |
| The owner's own workload | `legion session start <child task> --owner self` inside the task (`Workload.StartChildSession` on the [API socket](workload-identity.md#the-api-socket)) | the `children` block alone. No token is read |

A start from inside needs no credential beyond being the owner: the worker
attributes the socket's connections to its allocation and makes that
allocation's unit the owner, whatever the request says. A prompt-injected
agent therefore cannot use its own access to start machines the jobspec did
not name.

```console
$ legion session start machine --owner self -j
{"ok":true,"data":{"sessionId":"machine-q4n7c2rd.01jbz...","node":"w-17","state":"running",
 "deduplicated":false,"resumedFrom":"","generation":1,"movedFrom":"",
 "owner":{"kind":"allocation","id":"88213"}}}
```

`self` is the caller's session when it is a session, its instance when it
is a keyed or slot instance, and its allocation otherwise.

**Placement.** A child goes to its owner's node when that node's envelope
for the child task has room, so the owner reaches it without leaving the
node. A start through a manager falls back to another node with room, as
the [session router](session-mode.md#picking-a-node) picks it. A start at a
node (a ticket, or the API socket) only ever uses that node, and is refused
when it has no room.

**Accounting.** A child is admitted against its own task's envelope and
counts there. Its owner's share does not change. A session child writes
nothing to raft.

**Idempotency.** `--idempotency-key` works as for any session start. On
the API socket the key is scoped to the owner, so two owners that retry the
same key get two children.

Refusals:

| Code | When |
| --- | --- |
| `RESOURCE_EXHAUSTED` (retryable) | The owner already has `max` children, or no envelope for the child task has room (an [elastic envelope](session-mode.md#elastic-envelopes) grows, and the next try may fit) |
| `FAILED_PRECONDITION` | The owner's task does not declare the child task; the owner is itself a child; or a session would own a keyed child |
| `NOT_FOUND` | Nothing with the owner's id runs in this job (through a manager) or on this node (at a node). The message is the same whether the id never existed or belongs to another job |
| `UNAVAILABLE` (retryable) | The child task's runtime is [disabled](runtime-disable.md) |

```text
FAILED_PRECONDITION: task agent of job agents does not declare scratch as a child task; add `children = { tasks = ["scratch"] }` to agent
RESOURCE_EXHAUSTED: session agent-q4n7c2rd.01jbx... already owns 2 children (children.max = 2); stop one first
```

## Keyed children

A keyed task named in `children.tasks` gives **keyed children**: durable
instances with a row, a volume if the task declares one, and an identity,
owned by a keyed or slot instance or by an allocation.

```console
$ legion instance create agents --task workspace --id ws-42 --owner user-42 -j
{"ok":true,"data":{"instanceIds":["ws-42"],"mode":"keyed","durable":true,"deduplicated":false,
 "owner":{"kind":"instance","id":"user-42"}}}

$ legion instance create workspace --owner self        # inside a task, no token
Created instance workspace-5h3k..., a child of instance user-42 (stopped when its owner is stopped, destroyed when it is destroyed)
```

- **Who may own one.** An `instance` or an `allocation`. A session cannot:
  it can vanish with its node, and nothing durable would know. Promote the
  session first, or make the owner task keyed.

  ```text
  FAILED_PRECONDITION: task agent is a session task and workspace is keyed: a session cannot own a durable child (promote the session, or make agent keyed)
  ```

- **From inside a task.** `legion instance create <task> --owner self`
  calls `Workload.CreateChildInstance` on the
  [API socket](workload-identity.md#the-api-socket). A keyed instance is a
  replicated row, so the worker asks a manager, vouching for which of its
  allocations asked (`Manager.CreateChildInstance`, a worker-plane call).
  No token of the task's is read, and the manager checks that the
  allocation really is placed on the calling node.
- **`max` counts both kinds.** An owner's session children and keyed
  children share one `children.max`. Keyed children are counted in the
  create's own write, so two concurrent creates cannot both pass.
- **Placement.** The scheduler places a keyed child like any keyed
  instance and prefers its owner's node for the first placement. It is a
  preference: when that node cannot take the child, it goes wherever the
  task fits, and a later reschedule does not move it back.
- **The cascade.** `legion instance stop <owner>` stops the owner's keyed
  children and `legion instance destroy <owner>` destroys them, in the same
  replicated entry as the owner, children first. A stopped child keeps its
  row, its volume, and its owner, and can be started again on its own; the
  owner's volumes stay, and so do its children's. Suspending, archiving, or
  rescheduling the owner ends nothing.
- **Other ends.** An owner can also end without an instance stop or
  destroy: an allocation completes, fails, or is lost; a job is stopped; a
  slot is scaled away. The manager leading the scheduler lane looks for
  keyed children of ended owners every 5 seconds and ends them in one
  entry (stopped, or destroyed when the owner's row is gone). The event
  `instance.child_ended` records each child either path ends, and
  `legion_keyed_children_ended_total{end}` counts the sweep's.
- **Identity.** A keyed child's
  [workload identity](workload-identity.md) tokens carry two extra claims,
  `legion.io/owner` and `legion.io/root`, both `kind:id`
  (`instance:user-42`), so an upstream can attribute a request to the unit
  that owns the caller. With children one level deep the root is the
  owner.
- **`from_snapshot` and batch creates** (`--count`, several `--id`) cannot
  be combined with `--owner`.

## When an owner ends

"Ended" is the owner's terminal state as its authority decides it, never
just a process going away:

| Owner | Ended when | Not ended when |
| --- | --- | --- |
| session | stopped, destroyed, its workload exited, or its checkpoint was evicted | suspended, or moved to another node |
| instance | stopped or destroyed | suspended, archived, rescheduled, or resumed elsewhere |
| allocation | complete, failed, stopped, or lost | |

Whoever holds the child's record ends it:

- **A session owner on the child's node.** The node ends the children
  first and the owner second, so no child outlives the report of its
  owner's stop. An owner whose workload exits on its own takes its children
  with it at the node's next heartbeat.
- **An instance or allocation owner of keyed children.** See
  [Keyed children](#keyed-children): in the owner's own entry for an
  instance stop or destroy, within 5 seconds otherwise.
- **An instance or allocation owner of session children.** The managers
  read the owner's state from the store. Every heartbeat reply to a node that reports a child of
  an ended owner carries an **owner notice**, and the same notice is pushed
  on the node's work stream within a second. The notice is repeated until
  the node's report no longer lists the child, so a lost one is corrected
  by the next beat.
- **A session owner on another node.** This happens when a child was
  placed on another node, or when the owner
  [moved](session-mode.md#moving-a-session). The managers ask where the
  owner is, once a minute per owner. When no node and no manager has
  listed the owner for 20 seconds, the children end at their node's next
  beat (the wait covers a session that just moved and is not yet in any
  report). When the owner's node does not answer, the children end once the
  owner has been unreachable for `orphanAfter`.

A partition longer than `orphanAfter` can therefore end the children of an
owner that is still alive on the far side. The owner sees them as gone and
decides whether to start others.

**Sleep is not an end.** A suspended owner keeps its children, and each
child follows its own task's `suspend.idle`. With `suspendWithOwner =
true`, children whose task has a `suspend` block are suspended when their
owner is, and wake on their own paths (a start, a wake on connect) like any
suspended session. For an instance owner the suspend reaches the children's
node as an owner notice.

**A child that ends does not affect its owner.**

## Promotion

[Promotion](session-mode.md#promotion) turns a session into a keyed
instance, so ownership has to survive it.

- **Promoting an owner** re-keys its children's references from
  `{ session, <old id> }` to `{ instance, <new id> }`. The children keep
  running; from then on the managers decide when the owner ends, wherever
  either of them runs. Children on the owner's node are re-keyed when the
  node adopts the promoted session; children on other nodes by an owner
  notice. No extra raft write is made.
- **Promoting a child of an instance or an allocation** keeps the
  reference: the promotion writes it into the new instance row in its own
  entry, and the child is a keyed child from then on, ended as
  [keyed children](#keyed-children) are.
- **Promoting a child of a session** detaches it. It becomes an ordinary
  keyed instance with no owner, and its former owner's end no longer
  touches it (a durable child of an ephemeral owner is not allowed).

## Listing, and learning that a child is gone

`legion instance get <owner>` lists what a unit owns, and `legion instance
get <child>` names its owner. Both are in the `-j` output as `children` and
`owner`:

```console
$ legion instance get agent-q4n7c2rd.01jbx...
...
children (2):
  machine-q4n7c2rd.01jbz...  machine  w-17  active
  machine-7k2qd9x4.01jc1...  machine  w-03  active
```

Each child in the `-j` output has a `kind`: `session`, or `instance` for a
keyed child, whose `state` is then its instance state (`pending`, `active`,
`suspended`, `rescheduling`, `stopped`).

The instance page in the UI shows the same two things.

Inside the owner, `legion session children` (`Workload.ListChildren`) lists
its session children on its node (a node knows the sessions it holds, not
the store's rows, so keyed children are listed by `legion instance get`), including the ones that ended in the last ten
minutes, with the reason:

```console
$ legion session children -j
{"ok":true,"data":{"owner":{"kind":"session","id":"agent-q4n7c2rd.01jbx..."},"max":2,
 "children":[{"sessionId":"machine-q4n7c2rd.01jbz...","task":"machine","node":"w-17",
              "state":"gone","reason":"evicted"}]}}
```

| Reason | Meaning |
| --- | --- |
| `stopped`, `stopped by its owner`, or the stop's own reason | Someone ended it |
| `exited` | Its workload exited |
| `evicted` | It was suspended and its checkpoint was evicted |
| `owner ended`, or the owner notice's text | Its owner ended |

`legion session stop <child> --local` (`Workload.StopChildSession`) ends
one of the caller's own children. Any other id answers `NOT_FOUND`, so an
owner cannot probe for units that are not its own.

A read of an ended child by id (`legion session get`, or `legion instance
get` on its node tag) answers `NOT_FOUND` with `child_gone:` and the reason
for the same ten minutes.

## What a child runs as

A child runs under **its own task's** `resources`, `network`, `connect`,
and suspend policy. Declaring children grants the owner the ability to
start, list, and stop them, and nothing else.

In particular, **an owner with no egress can own a child with egress**.
The jobspec author wrote both tasks, so the author granted both. If that is
not what you want, give the child task the network policy the owner should
be held to.

## Addressing

A logical unit's address is its owner's id. The [gateway](gateway.md), the
session router, and edge pickers route to owners. A child has its own id,
which it needs to be stopped, exec'd into, and listed, and is reached:

1. **through its owner**, which knows its id and its node; or
2. **by an operator** with the usual permissions on the job (`legion
   instance get`, `legion exec`, `legion instance stop` on the child's id).

Legion does not re-point an owner's address at a child, and the gateway
never routes to one. For the bytes of one connection, an owner can hand the
connection over instead.

## Connection handoff

Forwarding every byte through the owner is wrong for a large upload or a
long-lived stream. An owner can **hand an inbound connection it accepted to
a port of one of its children**: the worker joins the client's connection
to the child's port and moves the bytes with the
[datapath](network-handlers.md)'s own copy (`splice(2)` on Linux once a
flow turns bulk, a plain copy elsewhere), and the owner leaves the byte
path. The owner keeps the decision; the worker moves the bytes.

```console
$ legion session handoff machine-q4n7c2rd.01jbz... --port http --fd 5 --wait -j
{"ok":true,"data":{"handedOff":true,"closed":{"sent":10485760,"received":211,"error":null}}}
```

`--fd` is the accepted connection, a file descriptor the command inherits
from the process that accepted it. Without `--wait` the command returns as
soon as the worker has taken the connection; with it, when the connection
has closed, with the bytes moved to the child (`sent`) and back
(`received`).

A descriptor cannot ride a gRPC call, so handoff has its own socket,
`legion-handoff.sock`, beside `legion.sock` in `$LEGION_SECRETS_DIR` and
admitted by the same rule (the caller is the socket's allocation). A
workload that does not want to run the CLI speaks it directly; one
connection is one handoff:

1. Connect, and send one line of JSON with the descriptor attached to its
   first byte as `SCM_RIGHTS`:
   `{"child":"<session id>","port":"<port label>"}`. Close your own copy of
   the descriptor afterwards, or the client never sees the child's close.
2. Read one line: `{"ok":true}`, or
   `{"ok":false,"code":"not_found","message":"..."}` and the socket closes.
3. Read one more line when the connection ends:
   `{"closed":true,"sent":N,"received":M}`, with `"error"` when the flow
   ended early.

```python
import json, socket
control = socket.socket(socket.AF_UNIX)
control.connect(os.environ["LEGION_SECRETS_DIR"] + "/legion-handoff.sock")
line = json.dumps({"child": child_id, "port": "http"}).encode() + b"\n"
socket.send_fds(control, [line], [conn.fileno()])
conn.close()
accepted = json.loads(control.makefile().readline())
```

| Refusal `code` | When |
| --- | --- |
| `not_found` | The id is not a child of the caller on this node. The answer is the same for another owner's child and for an id that never existed |
| `failed_precondition` | The child is suspended or not running yet, or declares no port with that label (the message lists the ports it has) |
| `invalid_argument` | No descriptor came with the line, the descriptor is not a connected TCP or Unix stream socket, or the line is not the JSON above |
| `unavailable` | The worker could not connect to the child's port |

Bytes the owner already read from the connection are not replayed: hand
the connection off before reading the body, or send what you read to the
child yourself first. `legion_child_handoffs_total` counts handoffs on a
node.

## Failure modes

| What happens | What you see | What to do |
| --- | --- | --- |
| The owner's node restarts | Children and their owner references are restored from `sessions.db` with the sessions themselves | Nothing |
| The owner's workload crashes | Its children end at the node's next heartbeat, with reason `owner ended` | Start a new owner |
| A notice to a node is lost | The next heartbeat reply repeats it | Nothing |
| No manager is reachable from a child's node | Children of instance and allocation owners, and of session owners on other nodes, keep running until a manager answers. Children of a session owner on the same node still end with it | Restore the managers |
| The owner's node is lost | Children on that node are lost with it. Children on other nodes end after `orphanAfter` (a session owner), or when the store marks the owner's allocation lost (an allocation owner). An instance owner is rescheduled, which is not an end | Nothing |
| A start is refused with `child refused: ...` | The owner's node refused the child for the owner's sake: not declared, at `max`, or the owner is a child. No other node is tried | Fix the jobspec or stop a child |

## Limitations

- **Depth 1.** A child cannot own children, and a child task cannot
  declare a `children` block.
- **Same job.** An owner and its children are tasks of one job, so the
  jobspec that grants the right also defines what is granted.
- **Children do not move with their owner.** An owner that resumes on
  another node keeps its children where they are, and a keyed child placed
  beside its owner is not moved after the owner is rescheduled.
- **No gateway route to a child.** A child is reached through its owner, by
  an operator on its id, or by a [handoff](#connection-handoff).
- **A child that moves is detached.** A child with `suspend.durability =
  "store"` that is resumed on another node becomes an ordinary session
  there: the session manifest does not carry its owner.
- **Session children have no identity claims.** Session tasks cannot
  declare `identities` ([session mode](session-mode.md#limitations)), so a
  session child has no token to carry its owner in; its attribution comes
  from the session report and `legion instance get`. Keyed children carry
  `legion.io/owner` and `legion.io/root`.
- **No tenant label and no usage rollup by owner.** Sessions carry no
  tenant label for a child to inherit; group usage by the `owner` of each
  session in `legion instance list -j`.
- **A session task has no API socket** (the socket needs the `legion`
  identity), so a session owner's children are started from outside, with
  `--owner <session id>`, and a session owner cannot hand a connection
  off. `--owner self` and handoff work for service, batch, slot, and keyed
  tasks that declare the `legion` identity.
- **`max` is exact on one node and approximate across nodes.** The owner's
  node counts its own children exactly. Children on other nodes are counted
  from the session index of the manager that routes the start, which can
  lag by a heartbeat.
- **Children are never restarted.** The owner decides whether to start
  another.
- **Handoff reaches a running session child on the owner's own node.** A
  child the router placed on another node, a suspended child, and a keyed
  child are refused; proxy to those, or start the child where the owner
  can hand off. Handoff throughput was not benchmarked on its own: it is
  the datapath's L4 copy.
- **A keyed child of an allocation owner outlives it by up to 5 seconds**,
  the sweep's interval. An instance owner's stop and destroy have no such
  gap.
- **A keyed child's own stop or destroy is not refused.** An operator with
  `job:write` can stop, start, or destroy a child by its id; starting a
  stopped child of a stopped owner leaves it running until the owner is
  stopped or destroyed again, or the sweep sees the owner's row gone.
- **`suspendWithOwner` for an instance owner is sent once**, when a manager
  first sees the instance suspended. A manager failover at that moment can
  miss it; the children then follow their own idle policy.
