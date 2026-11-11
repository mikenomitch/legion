# Plan: Child Sessions and Ownership

> **Judgement calls:** [child-sessions-plan_calls.md](../calls/child-sessions-plan_calls.md)

**Status: SHIPPED** (2026-11-10; written 2026-10-28, ownership generalized
2026-11-07, see calls entry 1). All four phases are built:

- **Phases 1 and 2:** the `children` block, the typed owner reference for
  all three owner kinds, session children started from outside
  (`CreateInstance.owner`, a ticket) and from inside any runtime over the
  API socket, the cascade for session owners on the node and for instance,
  allocation, and remote session owners through the managers' owner
  notices, promotion that carries ownership, the router's fallback to
  another node, and listing.
- **Phase 3:** keyed children. An indexed `owner` column on the instance
  row; `legion instance create --owner <id|self>`; the cascade in the
  owner's own entry for an instance stop or destroy, and a leader sweep for
  every other end; `max` across both kinds; the owner's node preferred at
  first placement; `owner` and `root` identity claims; a promoted child of
  an instance or allocation keeps its owner.
- **Phase 4:** connection handoff, on `legion-handoff.sock` beside the API
  socket (`legion session handoff`).

Public doc: [docs/child-sessions.md](../../docs/child-sessions.md).

Not built, and why: the isolate host's start path (§6.3, first row), which
belongs to the isolate runtime and does not exist without it. *Update
2026-11-11:* built with the [isolate runtime](isolate-runtime-plan.md):
`vm.acquire()` starts a child of the VM task on the isolate's node, a
session isolate owns it as `session`, and a request or tenant isolate
under a new owner kind, `isolate`, that managers leave to the node
([isolate calls entry 50](../calls/isolate-runtime-plan_calls.md#50-child-vms-of-an-isolate-are-owned-per-isolate-and-judged-by-its-node)). No
throughput benchmark was run for Phase 4; the handoff uses the datapath's
own copy, whose numbers are the datapath plan's.

Where the build differs from the text below (each is a calls entry):

- A session owner on another node is asked about by the managers, not by
  the child's node (§6.4; calls entry 10).
- Only an instance owner's stop and destroy cascade to keyed children in
  the same entry; an allocation owner's end, and an instance ended by a job
  stop or scale-down, are swept by the leader within 5 seconds (§6.4;
  entry 11).
- Raft-owned owners are judged on every heartbeat and pushed on a
  one-second stream tick, not from the owner's apply (§6.4; entry 12).
- A task creates a keyed child through its worker, which vouches for the
  allocation (`Manager.CreateChildInstance`); the task's identity token is
  not involved (§6.3; entry 13).
- Handoff has its own socket and a line protocol, because a descriptor
  cannot ride gRPC (§6.8; entry 14), and reaches only a running session
  child on the owner's node (entry 25).
- Through a manager a child is started with `legion instance create
  --owner`, not `legion session start --owner` (§6.3; entry 15).
- Keyed children carry the `owner` and `root` claims; session children
  carry none and no tenant label, because sessions have neither identities
  nor labels (§6.6, §6.9; entry 16).
- A child that moves nodes is detached (entry 18).
- A keyed child's owner node is a preference, not a pin (§6.3; entry 20).
- A promoted child of a session is detached; of an instance or allocation
  it keeps its owner (§6.5; entry 21). The Phase 1 refusal to promote an
  owner with children was never shipped: Phase 2's carrying landed with it.
- `child_gone` is a ten-minute record the owner lists, not an event
  (§6.4; entry 22).

A **child** is a session or keyed instance of one task that another unit
started and owns. It ends when its owner ends, is charged to its own task's
envelope or reservation, carries a typed reference to its owner for
attribution, and is reached through its owner. The owner's address stays
the one the outside world uses (the **front door**).

Ownership is one model for every kind of owner. The reference is stored
with the child, wherever the child's own record lives, and the cascade is
run by whoever holds that record, when the owner's authority says the owner
reached a terminal state. So a session owning sessions needs no raft at
all, a keyed instance owning keyed instances cascades in one raft
transaction, and the two mix without a second mechanism.

The first user is the [isolate runtime](isolate-runtime-plan.md): an isolate
that needs a real machine starts a child session of a sibling `sandbox`
task. Nothing here is isolate-specific: an `exec` agent spawning tool
sandboxes, a CI batch allocation fanning out test sandboxes, and a durable
agent instance owning a durable workspace all use the same thing.

Deliberately deferred, and named in §4: grandchildren (depth 1 only),
children in another job, exposing a child through the gateway, and moving
children with their owner.

Companion plans: [isolate-runtime-plan.md](isolate-runtime-plan.md) (the
first user), [resize-plan.md](resize-plan.md) (how a child, like any unit,
changes size), and [session-runs-plan.md](session-runs-plan.md) (one-shot
runs, which use a single-use session and need no ownership).

---

## 1. Problem statement

A session or instance today is a leaf. When the thing running in it needs
another sandbox (an agent loop that wants a scratch VM to run a build, an
isolate that hits something only Linux can do, a CI job that fans out), the
application has to:

1. **Hold a credential to start it.** A session ticket or a token with
   `job:write`, inside the workload that wants the child, which is exactly
   where a prompt-injected agent should not have one.
2. **Track and clean it up.** Nothing ends the second unit when the first
   one does. An owner that crashes leaks its children until someone notices
   the envelope is full.
3. **Route to it.** The second unit has its own id and node. Callers who
   should only ever know "user-42" now have to learn and cache a second
   address, and keep it right across suspend, wake, and moves.
4. **Attribute it.** Usage and events of the second unit say nothing about
   which unit asked for it, so a platform cannot bill one user for both.

## 2. Prior art

- **Cloudflare Durable Objects in front of Containers and Dynamic Workers**
  ([Containers](https://developers.cloudflare.com/containers/),
  [Dynamic Workers](https://blog.cloudflare.com/dynamic-workers/)). A
  Durable Object starts its container with `ctx.container.start()` and
  forwards to it with `container.fetch()`; requests address the Durable
  Object, never the container. Dynamic Workers are loaded by a parent
  Worker and run with only the bindings it passes.
  - **Take:** the owner is the address; the child is a capability the
    owner holds.
- **Kubernetes `ownerReferences` and garbage collection**
  ([docs](https://kubernetes.io/docs/concepts/architecture/garbage-collection/)).
  A dependent stores a typed reference to its owner; when the owner is
  deleted, its dependents are deleted.
  - **Take:** the reference lives on the dependent, typed by the owner's
    kind, and the cascade follows the owner's end.
  - **Reject:** one cluster-wide garbage collector watching every object.
    Legion's cascade runs where the child's record lives: on the node for a
    session, in the owner's raft transaction for an instance.
- **Erlang/OTP links and supervision.** A linked process dies with its
  partner; supervisors restart children by policy.
  - **Take:** the link (children end with their owner).
  - **Reject:** restart policies for children. The owner decides whether
    to start another.
- **systemd `BindsTo=` / `PartOf=`; Unix sessions and process groups.**
  Lifetimes bound to another unit; signals delivered to a group.
  - **Take:** stopping an owner stops its group.
- **Nomad parameterized jobs** dispatch child *jobs* from a parent job, each
  a raft object with its own allocation.
  - **Reject:** a control-plane object per ephemeral child.

## 3. Jobs to be done, by persona

**Agent platform builder**
- When my agent needs a real machine for one step, I want it to get one
  without holding any credential, so a prompt injection cannot use the
  agent's own access to start machines of its choosing.
- When the agent's session ends or crashes, I want its machines to end with
  it, so I am not paying for orphans I have to hunt down.
- When an agent matters enough to keep, I want to promote it and have its
  machines follow, so nothing it started is orphaned or killed by the
  promotion.
- When I bill a user, I want every machine their session started to be
  attributed to them, so the bill adds up.

**Job author**
- When my front-end session hands work to a bigger sandbox, I want my users
  to keep talking to the one address they already have, so I never publish
  or cache a second address.
- When my CI job fans out into sandboxes, I want them gone when the job's
  run finishes, so a cancelled build does not leave machines behind.

**Cluster operator**
- When I look at a node or an instance, I want to see what owns what, so I
  can tell a leak from a busy owner.

**Security / platform admin**
- When a workload can start others, I want what it may start written in the
  jobspec, and the children to run under their own task's limits and
  network policy, so the owner cannot grant more than the spec allows.

**Autonomous agent**
- When I start a child, I want a structured handle back and a structured
  error when I am over my limit, so I can retry or give up deliberately.

### User scenarios

1. **Isolate burst.** An isolate session `user-42` calls `vm.acquire()`. The
   node starts a child session of the job's `machine` task on the same node,
   charged to `machine`'s envelope, and returns a handle. Requests keep
   arriving for `user-42`; the isolate forwards the ones that need the
   machine. When `user-42` is stopped, the machine stops too.
2. **A promoted agent keeps its machine.** An `exec` agent session owns a
   tool sandbox. The platform promotes the agent to the keyed instance
   `user-42`. The sandbox keeps running; its owner reference now names the
   instance, and from then on the managers end it when `user-42` is stopped
   or destroyed, wherever either of them runs.
3. **CI fan-out.** A batch allocation running a test suite starts eight
   sandbox sessions through its local API socket, with no token. When the
   allocation completes, fails, or is cancelled, the managers tell the
   nodes holding those sessions, and they end.
4. **A durable agent with a durable workspace.** A keyed agent instance
   creates a keyed workspace instance as its child. Destroying the agent
   destroys the workspace in the same raft transaction; suspending the agent
   leaves the workspace to its own idle policy.

## 4. Goals and non-goals

### Goals

- One ownership model for every owner kind (a session, a keyed or slot
  instance, an allocation) and both child kinds (a session, a keyed
  instance).
- Starting a child from inside the owner needs no credential beyond being
  the owner.
- Children end with their owner, always, using the owner's own definition
  of "ended"; a lost session owner's children end after a bounded time.
- Children are charged to their own task's envelope (sessions) or
  reservation (instances) and run under their own task's resources,
  network policy, and identity.
- The owner's address remains the only one the gateway, router, and edge
  pickers route to.
- No raft write per session child, whatever its owner.

### Non-goals

- **Count-placed children.** A service, batch, or slot allocation is placed
  by the scheduler from the task's `count`; an owner creating them would
  fight the scheduler. Children are sessions or keyed instances.
- **A durable child of an ephemeral owner.** A keyed instance cannot be a
  session's child: the session can vanish with its node, and nothing
  durable would know. Promote the owner first (§6.5).
- **Grandchildren.** Depth 1 only: a child cannot own children.
- **Children in another job.** Same job only, so the jobspec that grants the
  right also defines what is granted.
- **Restarting children.** The owner decides.
- **Moving children with a moved owner.** An owner that resumes on another
  node keeps its children where they are and reaches them by id.
- **Exposing a child through the gateway.** Through the owner, or by an
  operator; connection handoff (§6.8) covers the bulk-bytes case.

## 5. Background: what exists today

- **Sessions** ([docs/session-mode.md](../../docs/session-mode.md)) are
  node-owned soft state with ids that carry their node's tag, envelopes per
  node, edge starts with session tickets, idle suspend, and `store`
  manifests with generations. A moved session's tag is stale; callers cache
  `session id -> node` from the resume reply (`StartSessionRequest.resume_id`
  proto comment, `proto/legion.proto:10128-10143`).
- **Sandbox sessions are reported** to the managers' in-memory session
  index through heartbeat session reports, with a full report every 30
  beats as a backstop (`HeartbeatRequest.session_report`,
  `proto/legion.proto:6101-6108`).
- **Promotion** (`legion instance promote`, `PromoteInstance`) turns a
  running session into a keyed instance in place, optionally renaming it,
  in one raft entry (docs/session-mode.md, "Promotion").
- **Keyed instances** are raft rows whose stop and destroy are raft
  transactions; a suspended or rescheduled instance keeps its row and is
  not ended.
- **Work reaches nodes** on the `StreamWork` stream, with `PostWork` and
  the heartbeat as fallbacks (`proto/legion.proto:1331`, `:9845`).
- **The per-allocation API socket** ([legion-apps-plan.md](legion-apps-plan.md)
  §6.4, built in `src/worker/api_socket.rs`): a unix socket per allocation
  whose connections the worker attributes by `SO_PEERCRED` and cgroup where
  every task has a leaf, and by file modes otherwise (apps calls entry 31).
  It proxies the Legion API to the managers today; this plan adds
  worker-local calls to it (apps calls entry 130). The isolate host needs no
  such socket: it talks to the worker over its own socketpair under the same
  attribution rule.
- **Splice.** The datapath already moves bulk L4 bytes with `splice(2)` on
  Linux ([datapath-performance-plan.md](datapath-performance-plan.md)),
  which connection handoff (§6.8) reuses.

## 6. Proposed design

### 6.1 Declaring children

On the owner task, a top-level `children` block. It is not under
`stateful`, because a service or batch task, which has no `stateful`
block, can own children too:

```nickel
{
  name = "agent",
  runtime = "isolate",             # any runtime and any mode
  stateful = { mode = "session", envelope = { resources = { cpu = 16000, memory = 32768 } } },
  children = {
    tasks = ["machine"],           # session or keyed tasks in this job
    max = 2,                       # per owner (default 1)
    suspendWithOwner = false,      # default
    orphanAfter = "10m",           # session owners only (§6.4)
  },
}
```

Submit checks:

```text
[children] agents/agent: children.tasks names "machine", which is not a task in this job
[children] agents/agent: children.tasks names "web", a service task; children are sessions (stateful.mode = "session") or keyed instances (stateful.mode = "keyed")
[children] agents/agent: children.tasks names "workspace", a keyed task, but agent is a session task; a session cannot own a durable child (promote the session, or make agent keyed)
[children] agents/machine: a child task cannot declare children (depth 1 only)
```

The child task is an ordinary session or keyed task with its own envelope
or reservation, resources, network, and suspend policy. Nothing on it says
it is a child task; any of its units may be a child or not.

### 6.2 The owner reference

Every child stores one reference to its owner:

```text
owner = { kind = "session" | "instance" | "allocation", id = "<id>" }
```

| Owner kind | What it names | Its authority (who decides it ended) |
| --- | --- | --- |
| `session` | a session id | the node running it |
| `instance` | a keyed or slot instance id, the logical unit that outlives its allocations | the managers |
| `allocation` | an allocation id, for owners with no instance identity (a service replica, a batch run) | the managers |

The reference is stored **with the child**, in whatever holds the child's
own record:

| Child kind | Where the reference lives | Raft cost |
| --- | --- | --- |
| session | the node's session table (`sessions.db`), and the child's session report | none |
| keyed instance | an `owner` column on the instance row, written in the create | none beyond the create |

### 6.3 Starting a child

| Who | How | Authorized by |
| --- | --- | --- |
| An isolate | `vm.acquire()` and friends ([isolate plan §7.4](isolate-runtime-plan.md)) over the host's socketpair | The worker checks the named owner is local and its task declares the child task |
| Any other runtime's workload (Phase 2) | `legion session start machine --owner self` or `legion instance create workspace --owner self` on the per-allocation API socket | The worker attributes the connection to its allocation by cgroup and checks the declaration |
| An operator or platform outside | `--owner <id>` on `session start` or `instance create` (`owner` on the request) | `job:write` on the job, plus the declaration |

```console
$ legion session start machine --owner agent-q4n7c2rd.01jbx... -j
{"ok":true,"data":{"sessionId":"machine-q4n7c2rd.01jbz...",
 "owner":{"kind":"session","id":"agent-q4n7c2rd.01jbx..."},
 "node":"w-17","state":"running","deduplicated":false}}
```

Refusals:

| Code | When |
| --- | --- |
| `RESOURCE_EXHAUSTED` (retryable) | The child task's envelope on the node has no room (an elastic envelope grows and the next try may fit), or the owner already has `max` children |
| `FAILED_PRECONDITION` | The owner's task does not declare the child task, or the pair is a session owning a keyed child |
| `NOT_FOUND` | The owner is not running where its id says (and existence is not otherwise revealed) |

**Placement.** A session child goes to its owner's node first, so the owner
reaches it without leaving the node; Phase 2 falls back to the session
router for another node with room, and Phase 1 refuses instead. A keyed
child is placed by the scheduler like any keyed instance, with an affinity
for its owner's node.

### 6.4 When an owner ends

**"Ended" is the owner's terminal state, decided by its authority**, never
a process exit:

| Owner | Ended when | Not ended when |
| --- | --- | --- |
| session | stopped, destroyed, or its request finished (an isolate with `lifetime = "request"`) | suspended, or moved to another node |
| instance | stopped or destroyed | suspended, archived, rescheduled, or resumed elsewhere |
| allocation | terminal: complete, failed, stopped, or lost and replaced | |

The cascade applies the owner's verb: an owner **stopped** stops its
children; an owner **destroyed** destroys them (a session child has no
difference between the two).

**Whoever holds the child's record runs the cascade**, through a channel
that already exists:

| Owner → child | How the children learn | How they end |
| --- | --- | --- |
| session → session | the owner's node, at once | locally; `Worker.StopSession` to a remote node (Phase 2) |
| instance or allocation → session | the managers apply the owner's terminal transition, and the same apply sends "owner ended" on `StreamWork` to the nodes the session index lists for the owner's children (or, when the index has none, every node holding the child task's envelope) | the node ends them; every full session report is also checked against owners' states, so a missed notice is corrected within 30 beats |
| instance or allocation → keyed instance | the owner's terminal transition | in the same raft transaction |
| session → keyed instance | not allowed (§4) | |

- **Sleep is not an end.** Each child follows its own task's
  `suspend.idle`; `suspendWithOwner = true` suspends an owner's children
  when it suspends and resumes them on demand.
- **A child that ends tells its owner:** an event, and the next call
  through the handle answers `child_gone` with why (`stopped`, `evicted`,
  `node lost`).
- **A lost session owner's remote children end** after `orphanAfter`
  (default 10 m): the child's node asks the owner's node about the owner
  every minute and ends the child when the owner is reported gone, or its
  node has not answered for that long. A partition longer than
  `orphanAfter` ends children whose owner is alive; the owner sees
  `child_gone` and decides. Raft-owned owners need no orphan sweep: the
  full-report check covers them.
- **An owner that moves** keeps its children in place. The handle holds the
  child's id; the owner re-attaches by id, and a moved child's current node
  comes from the resume reply as for any session.

### 6.5 Promotion carries ownership

Promotion already exists, so ownership has to survive it:

- **Promoting an owner** re-keys its children's references from
  `{ session, <old id> }` to `{ instance, <new id> }`. The children keep
  running; their node updates its session table and its next report carries
  the new reference. From then on the managers decide when the owner ends.
  No extra raft write: the promotion is the one it always was.
- **Promoting a child** detaches it when its owner is a session (a durable
  child of an ephemeral owner is not allowed), and keeps the reference when
  its owner is an instance or allocation, writing it into the new instance
  row in the promotion's transaction.
- **Until Phase 2** builds raft-owned owners, promoting a session that owns
  children is refused rather than orphaning them:

```text
FAILED_PRECONDITION: session agent-q4n7c2rd.01jbx... owns 2 children; promoting an owner that carries its children arrives with child sessions Phase 2. Stop the children first, or wait for it.
```

### 6.6 Accounting and attribution

- A child is charged to its own task's envelope (a session) or reservation
  (a keyed instance), never to its owner's. The owner's charge does not
  change when it starts one.
- A child's session report or instance row carries its owner reference, and
  a session child inherits its owner's `tenant` label when the owner has
  one (an isolate's tenant key, or a label set at start). `legion session
  get` and `legion instance get` list an owner's children; usage rollups
  group by owner.
- No session child writes to raft, whatever its owner.

### 6.7 Addressing: the front door

**A logical unit's address is its root's id.** The gateway, the session
router, and edge pickers route to owners. A child has its own id (it needs
one to be stopped, exec'd into, and moved), but from outside it is reached:

1. **through its owner**, which holds the handle (exec, files, and `fetch`
   to a child port, carried by the worker datapath: local on the same node,
   over the worker plane otherwise); or
2. **by an operator** with `job:write`, through `get`, `exec`, and `stop`
   on the child's id.

Re-pointing an owner's address at its child after a burst was rejected
(calls entry 4): it needs a mutable mapping (a raft write or a manifest
compare-and-set), two kinds of address, a window where requests reach the
old target, and a VM's wake time on every request.

### 6.8 Connection handoff (Phase 4)

Forwarding every byte through the owner is wrong for a large upload or a
long-lived stream. The owner can instead hand an inbound connection to a
child's port: it accepts the connection (an isolate gets it from ingress),
decides, and asks the worker to splice it to the child. The worker joins
the client's connection to the child's port with the datapath's existing
`splice(2)` path and the owner leaves the byte path; the owner learns when
the connection closes. The owner keeps the decision, the worker moves the
bytes.

### 6.9 Identity and security

- A child's workload identity is its own allocation's, with two extra
  claims, `owner` (the typed reference) and `root`, so an upstream can
  attribute a request to the tree that made it.
- A child runs under its own task's `connect`, `network`, and `resources`.
  Declaring children grants the owner only the ability to start, drive, and
  stop them. An owner with no egress can still own a child with egress,
  because the jobspec author granted both; the public doc shows this in so
  many words.
- An owner reaches only its own children through the handle; a handle for
  another owner's child is refused (`NOT_FOUND`, so existence is not
  probeable).

## 7. Proto, struct, config, and persistence changes

- **Jobspec:** a top-level `children = { tasks, max, suspendWithOwner,
  orphanAfter }` on any task; into `configs/jobs/maximal.ncl`.
- **Proto:** an `OwnerRef { kind, id }` message; `owner` on
  `StartSessionRequest`, `CreateInstanceRequest`, `SessionStatus`, and
  instance messages; `Worker.StartChildSession` (local socket); an "owners
  ended" item on `StreamWork`; `child_gone` reasons; identity claims
  `owner` and `root`. Full proto comments.
- **Store (Phase 3):** an `owner` column on the instance table, indexed, in
  one migration.
- **Worker state:** the owner reference lives with each session in
  `sessions.db`, so a restarted worker knows its children.
- **ACL:** `--owner` from outside is `job:write`; inside starts are
  worker-plane, marked with an `// ACL:` comment and added to
  `tests/integration/acl_endpoints.rs`.

## 8. Implementation phases

- **Phase 1: sessions owning sessions, same node.** The declaration, the
  typed owner reference (only `session` owners accepted yet), starts from
  outside and from the isolate host, the cascade, accounting and
  attribution, listing under the owner, `child_gone`, promotion of a child
  (detaches), and the refusal to promote an owner with children. Enough for
  the isolate runtime's burst (its Phase 5).
- **Phase 2: any owner, any node.** `instance` and `allocation` owners of
  session children with the managers' "owners ended" notice and the
  full-report check; promotion that carries ownership; router fallback to
  another node and the orphan sweep for session owners; starts from inside
  any runtime through the per-allocation API socket.
- **Phase 3: keyed children.** The `owner` column and the in-transaction
  cascade.
- **Phase 4: connection handoff** (§6.8).
- **Later, if asked:** depth > 1, cross-job children.

## 9. Testing plan

- **Unit:** declaration validation for every owner and child kind; the
  terminal-state table (a suspended or rescheduled owner never cascades);
  admission against the child task's envelope with the owner's `max`;
  cascade ordering (children stopped before the owner's stop is reported);
  orphan timing; promotion re-keying.
- **Integration (`tests/integration/child_sessions.rs`):** start a child
  from outside with `--owner`; stop the owner and see the child end;
  suspend the owner and see the child keep running; refuse an undeclared
  child task, a keyed child of a session, and over `max`; a child's
  identity token carries `owner` and `root`; a worker restart keeps the
  owner references; promote a child and see it detach; refuse promoting an
  owner with children (Phase 1); ACL table entries.
- **Phase 2:** a batch allocation's completion ends its session children on
  two nodes; a dropped "owners ended" notice is corrected by the next full
  report; a promoted owner's child ends when the instance is stopped; a
  child on another node and the orphan sweep; a start from inside an `exec`
  workload through the local socket with no token.
- **Phase 3:** destroying a keyed owner destroys its keyed child in the
  same transaction; stopping it stops the child.
- **Phase 4:** a handed-off connection streams at datapath speed with the
  owner's process idle.

## 10. Rollout and backward compatibility

New, opt-in fields. A task without `children` behaves as today. Workers
without the capability refuse child starts with `FAILED_PRECONDITION`
naming the worker version, and the router does not place children there.
Promotion of sessions without children is unchanged.

## 11. Open questions

1. **Should `suspendWithOwner` default to true for isolate owners?** Lean:
   no; an agent's machine is often the expensive half and should follow its
   own idle policy.
2. **Should the gateway ever route to a child directly?** Lean: no;
   connection handoff gives the bytes path without a second address.
3. **`orphanAfter` default.** Lean: 10 m, long enough to ride out a worker
   restart, short enough that leaks cost minutes.
4. **Should a stopped keyed owner's keyed children stop, or only end on
   destroy?** Lean: stop, mirroring the owner's verb, so a stopped agent
   costs nothing; the children's volumes stay, as the owner's do.

## 12. Acceptance criteria

- **Phase 1:** stopping an owner session ends its children on the same node
  within one second, and suspending it does not; a child's session report
  carries its owner reference; an undeclared, over-limit, or keyed-under-
  session child is refused with the codes in §6.3; promoting an owner with
  children is refused with the message in §6.5.
- **Phase 2:** a batch allocation's completion ends its session children on
  other nodes within one heartbeat, or within 30 beats when the push is
  lost; a promoted owner keeps its children running and they end when the
  instance stops; with a session owner's node killed, its remote children
  end within `orphanAfter` plus a minute; a child starts from inside an
  `exec` workload with no credential in the task.
- **Phase 3:** destroying a keyed owner destroys its keyed children in the
  same raft transaction.
- **Phase 4:** a handed-off stream runs at the datapath's measured L4
  throughput.
