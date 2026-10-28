# Plan: Child Sessions and Front-Door Addressing

> **Judgement calls:** [child-sessions-plan_calls.md](../calls/child-sessions-plan_calls.md)

**Status: PLANNED** (written 2026-10-28). Nothing here is built.

A **child session** is a session of one task that another session started
and owns. It ends when its parent ends, is charged to its own task's
envelope, carries its parent's id for attribution, and is reached through
its parent. The parent's id stays the one address the outside world uses
(the **front door**). This is the primitive the
[isolate runtime](isolate-runtime-plan.md) builds burst on: an isolate that
needs a real machine starts a child session of a sibling `sandbox` task.
Nothing in this plan is isolate-specific; an `exec` agent session that
spawns tool sandboxes uses the same thing.

Deliberately deferred, and named in §4: children of keyed instances,
grandchildren (depth 1 only), children in another job, exposing a child
through the gateway, and moving children with their parent.

Companion plans: [isolate-runtime-plan.md](isolate-runtime-plan.md) (the
first user) and [resize-plan.md](resize-plan.md) (how a child, like any
unit, changes size).

---

## 1. Problem statement

A session today is a leaf. When the thing running in it needs another
sandbox (an agent loop that wants a scratch VM to run a build, an isolate
that hits something only Linux can do, an orchestrator that fans out), the
application has to:

1. **Hold a credential to start it.** A session ticket or a token with
   `job:write`, inside the workload that wants the child, which is exactly
   where a prompt-injected agent should not have one.
2. **Track and clean it up.** Nothing ends the second session when the
   first one does. A parent that crashes leaks its children until someone
   notices the envelope is full.
3. **Route to it.** The second session has its own id and node. Callers who
   should only ever know "user-42" now have to learn and cache a second
   address, and keep it right across suspend, wake, and moves.
4. **Attribute it.** Usage and events of the second session say nothing
   about which session asked for it, so a platform cannot bill one user for
   both.

## 2. Prior art

- **Cloudflare Durable Objects in front of Containers and Dynamic Workers**
  ([Containers](https://developers.cloudflare.com/containers/),
  [Dynamic Workers](https://blog.cloudflare.com/dynamic-workers/)). A
  Durable Object starts its container with `ctx.container.start()` and
  forwards to it with `container.fetch()`; requests address the Durable
  Object, never the container. Dynamic Workers are loaded by a parent
  Worker and run with only the bindings it passes.
  - **Take:** the parent is the address; the child is a capability the
    parent holds.
- **Kubernetes `ownerReferences` and garbage collection**
  ([docs](https://kubernetes.io/docs/concepts/architecture/garbage-collection/)).
  A dependent names its owner; deleting the owner cascades.
  - **Take:** cascade on the owner's end.
  - **Reject:** owner references as API objects reconciled by a
    controller. Children here are node-owned soft state, like sessions.
- **Erlang/OTP links and supervision.** A linked process dies with its
  partner; supervisors restart children by policy.
  - **Take:** the link (children end with the parent).
  - **Reject:** restart policies for children. The parent decides whether
    to start another.
- **systemd `BindsTo=` / `PartOf=`; Unix sessions and process groups.**
  Lifetimes bound to another unit; signals delivered to a group.
  - **Take:** stopping a parent stops its group.
- **Nomad parameterized jobs** dispatch child *jobs* from a parent job, each
  a raft object with its own allocation.
  - **Reject:** a control-plane object per child.

## 3. Jobs to be done, by persona

**Agent platform builder**
- When my agent needs a real machine for one step, I want it to get one
  without holding any credential, so a prompt injection cannot use the
  agent's own access to start machines of its choosing.
- When the agent's session ends or crashes, I want its machines to end with
  it, so I am not paying for orphans I have to hunt down.
- When I bill a user, I want every machine their session started to be
  attributed to them, so the bill adds up.

**Job author**
- When my front-end session hands work to a bigger sandbox, I want my users
  to keep talking to the one address they already have, so I never publish
  or cache a second address.

**Cluster operator**
- When I look at a node, I want to see which sessions own which, so I can
  tell a leak from a busy parent.

**Security / platform admin**
- When a session can start others, I want what it may start written in the
  jobspec, and the children to run under their own task's limits and
  network policy, so the parent cannot grant more than the spec allows.

**Autonomous agent**
- When I start a child, I want a structured handle back and a structured
  error when I am over my limit, so I can retry or give up deliberately.

### User scenarios

1. **Isolate burst.** An isolate session `user-42` calls `vm.acquire()`. The
   node starts a child session of the job's `machine` task on the same node,
   charged to `machine`'s envelope, and returns a handle. Requests keep
   arriving for `user-42`; the isolate forwards the ones that need the
   machine. When `user-42` is stopped, the machine stops too.
2. **Tool sandboxes from an exec agent.** An `exec` session running an agent
   loop calls `legion session start tools --child-of self` through its
   local API socket. No token is involved; the worker knows the caller's
   allocation by its cgroup. The tool sandbox ends when the agent session
   ends.
3. **A leak that cannot happen.** A parent's node is lost. Its children on
   other nodes notice the parent's node is gone and end themselves after
   `orphanAfter`.

## 4. Goals and non-goals

### Goals

- A session task can declare which sibling session tasks its sessions may
  start, and how many each.
- Starting a child from inside the parent needs no credential beyond being
  the parent.
- Children end with their parent, always; a lost parent's children end
  after a bounded time.
- Children are charged to their own task's envelope and run under their own
  task's resources, network policy, and identity.
- The parent's id remains the only address the gateway, router, and edge
  pickers route to.
- Nothing per child is written to raft beyond what any session of its task
  already writes (none for an unnamed session).

### Non-goals

- **Children of keyed instances.** A keyed instance is a raft object; its
  children's lifetime would need a durable owner record. Later, if asked.
- **Grandchildren.** Depth 1 only: a child cannot start children.
- **Children in another job.** Same job only, so the jobspec that grants the
  right also defines what is granted.
- **Restarting children.** The parent decides.
- **Moving children with a moved parent.** A `store` parent that resumes on
  another node keeps its children where they are and reaches them by id.
- **Exposing a child through the gateway.** Through the parent, or by an
  operator; connection handoff (§6.7) covers the bulk-bytes case.

## 5. Background: what exists today

- **Sessions** ([docs/session-mode.md](../../docs/session-mode.md)) are
  node-owned soft state with ids that carry their node's tag, envelopes per
  node, edge starts with session tickets, idle suspend, and `store`
  manifests with generations. A moved session's tag is stale; callers cache
  `session id -> node` from the resume reply (`StartSessionRequest.resume_id`
  proto comment, `proto/legion.proto:10128-10143`).
- **Sandbox sessions are reported** to the managers' session index through
  heartbeat session reports (`HeartbeatRequest.session_report`,
  `proto/legion.proto:6101-6108`).
- **The per-allocation API socket** is planned in
  [legion-apps-plan.md](legion-apps-plan.md) §6.4: a unix socket per
  allocation whose connections the worker attributes by `SO_PEERCRED` and
  cgroup. It is not built. The isolate host needs no such socket: it already
  talks to the worker over its own socketpair.
- **Splice.** The datapath already moves bulk L4 bytes with `splice(2)` on
  Linux ([datapath-performance-plan.md](datapath-performance-plan.md)),
  which connection handoff (§6.7) reuses.

## 6. Proposed design

### 6.1 Declaring children

On the parent session task:

```nickel
{
  name = "agent",
  runtime = "isolate",                       # or exec, container, sandbox
  stateful = {
    mode = "session",
    envelope = { resources = { cpu = 16000, memory = 32768 } },
    children = {
      tasks = ["machine"],                   # sibling session tasks in this job
      max = 2,                               # per parent session (default 1)
      suspendWithParent = false,             # default
    },
  },
}
```

Submit checks:

```text
[children] agents/agent: stateful.children.tasks names "machine", which is not a task in this job
[children] agents/agent: stateful.children.tasks names "machine", which is not a session task (children are sessions; set stateful.mode = "session" on machine)
[children] agents/machine: a child task cannot declare stateful.children (depth 1 only)
```

The child task is an ordinary session task with its own envelope,
resources, network, and suspend policy. Nothing on it says it is a child
task; any of its sessions may be a child or not.

### 6.2 Starting a child

| Who | How | Authorized by |
| --- | --- | --- |
| An isolate | `vm.acquire()` and friends ([isolate plan §7.4](isolate-runtime-plan.md)) over the host's socketpair | The host process names the parent session; the worker checks the parent is local and its task declares the child task |
| Any other runtime (Phase 2) | `legion session start machine --child-of self` (or `Worker.StartChildSession`) on the per-allocation API socket | The worker attributes the connection to the allocation by cgroup and checks the declaration |
| An operator or platform outside | `legion session start machine --parent <id>` (`StartSessionRequest.parent_id`) | `job:write` on the job, plus the declaration |

```console
$ legion session start machine --parent agent-q4n7c2rd.01jbx... -j
{"ok":true,"data":{"sessionId":"machine-q4n7c2rd.01jbz...","parent":"agent-q4n7c2rd.01jbx...",
 "node":"w-17","state":"running","deduplicated":false}}
```

Refusals:

| Code | When |
| --- | --- |
| `RESOURCE_EXHAUSTED` (retryable) | The child task's envelope on the node has no room (the node grows an elastic envelope and the next try may fit), or the parent already has `max` children |
| `FAILED_PRECONDITION` | The parent's task does not declare the child task |
| `NOT_FOUND` | The parent session is not running on the node its id names |

### 6.3 Placement

Same node first: the parent's node admits the child against the child
task's envelope there, so the parent reaches it without leaving the node
and files move over local channels. When that envelope is full and cannot
grow (Phase 2), the node asks the session router for another node with
room, and the child runs there with its own node tag. Phase 1 refuses
instead.

### 6.4 Lifetime

- **The parent ends, its children end.** Stop, destroy, or (for an isolate
  parent with `lifetime = "request"`) the end of its request. The node stops
  local children at once and sends `Worker.StopSession` to remote ones.
- **The parent sleeps, its children do not** by default. Each child follows
  its own task's `suspend.idle`; `suspendWithParent = true` suspends a
  parent's children when it suspends and resumes them on demand.
- **A child ends, the parent is told.** An event on the parent's node, and
  the next call through the handle answers `child_gone` with why
  (`stopped`, `evicted`, `node lost`).
- **A lost parent's remote children end** after `orphanAfter` (default
  10 m): the child's node asks the parent's node about the parent every
  minute and ends the child when the parent is reported gone or its node
  has not answered for that long. A partition longer than `orphanAfter`
  ends children whose parent is alive; the parent sees `child_gone` and
  decides.
- **A `store` parent that moves** keeps its children in place. The handle
  holds the child's id; the parent re-attaches by id, and a moved child's
  current node comes from the resume reply as for any session.

### 6.5 Accounting and attribution

- A child is charged to its own task's envelope on its node, never to its
  parent's. The parent's charge does not change when it starts one.
- A child's session report carries `parent_id`, and the child inherits the
  parent's `tenant` label when the parent has one (an isolate's tenant key,
  or a label set at start). `legion session get <parent>` lists children on
  the parent's node; usage rollups group by it.
- Starting a child writes nothing to raft that a session of the child task
  would not write anyway: none for an unnamed session.

### 6.6 Addressing: the front door

**A logical session's address is its root's id.** The gateway, the session
router, and edge pickers route to roots. A child has its own id (it needs
one to be stopped, exec'd into, and moved), but from outside it is reached:

1. **through its parent**, which holds the handle (exec, files, and
   `fetch` to a child port, carried by the worker datapath: local on the
   same node, over the worker plane otherwise); or
2. **by an operator** with `job:write`, through `legion session get`,
   `exec`, and `stop` on the child's id.

Re-pointing a parent's address at its child after a burst was rejected
(calls entry 3): it needs a mutable mapping (a raft write or a manifest
compare-and-set), two kinds of address, a window where requests reach the
old target, and a VM's wake time on every request.

### 6.7 Connection handoff (Phase 3)

Forwarding every byte through the parent is wrong for a large upload or a
long-lived stream. The parent can instead hand an inbound connection to a
child's port: it accepts the connection (an isolate gets it from ingress),
decides, and asks the worker to splice it to the child. The worker joins
the client's connection to the child's port with the datapath's existing
`splice(2)` path and the parent leaves the byte path; the parent learns
when the connection closes. The parent keeps the decision, the worker moves
the bytes.

### 6.8 Identity and security

- A child's workload identity is its own allocation's, with two extra
  claims, `parent` (the parent session id) and `root`, so an upstream can
  attribute a request to the session tree that made it.
- A child runs under its own task's `connect`, `network`, and `resources`.
  Declaring children grants the parent only the ability to start, drive,
  and stop them. A parent with no egress can still own a child with egress,
  because the jobspec author granted both; the public doc shows this in so
  many words.
- The parent reaches only its own children through the handle; a handle
  for another session's child is refused (`NOT_FOUND`, so existence is not
  probeable).

## 7. Proto, struct, config, and persistence changes

- **Jobspec:** `stateful.children = { tasks, max, suspendWithParent,
  orphanAfter }` on session tasks; into `configs/jobs/maximal.ncl`.
- **Proto:** `StartSessionRequest.parent_id`; `Worker.StartChildSession`
  (local socket); `SessionStatus.parent_id`; `child_gone` reasons;
  identity claims `parent` and `root`. Full proto comments.
- **Worker state:** the child table lives with the node's session table
  (`sessions.db`), so a restarted worker knows its children.
- **ACL:** `--parent` from outside is `job:write`; inside starts are
  worker-plane, marked with an `// ACL:` comment and added to
  `tests/integration/acl_endpoints.rs`.

## 8. Implementation phases

- **Phase 1: same-node children.** The declaration, `parent_id` on starts
  from outside and from the isolate host, the lifetime cascade, accounting
  and attribution, listing under the parent, `child_gone`. Enough for the
  isolate runtime's burst (its Phase 5).
- **Phase 2: anywhere.** Router fallback to another node, the orphan
  sweep, and starts from inside other runtimes through the per-allocation
  API socket (built here if the apps plan's has not landed).
- **Phase 3: connection handoff** (§6.7).
- **Later, if asked:** keyed parents, depth > 1, cross-job children.

## 9. Testing plan

- **Unit:** declaration validation; admission against the child task's
  envelope with the parent's `max`; cascade ordering (children stopped
  before the parent's stop is reported); orphan timing.
- **Integration (`tests/integration/child_sessions.rs`):** start a child
  from outside with `--parent`; stop the parent and see the child end;
  refuse an undeclared child task; refuse over `max`; a child's identity
  token carries `parent` and `root`; a worker restart keeps the child table;
  ACL table entries.
- **Phase 2:** a child placed on another node; the parent's node killed and
  the child ending after `orphanAfter`; a start from inside an `exec`
  session through the local socket with no token.
- **Phase 3:** a handed-off connection streams at datapath speed with the
  parent's process idle.

## 10. Rollout and backward compatibility

New, opt-in fields. A session task without `stateful.children` behaves as
today. Workers without the capability refuse child starts with
`FAILED_PRECONDITION` naming the worker version, and the router does not
place children there.

## 11. Open questions

1. **Should `suspendWithParent` default to true for isolate parents?**
   Lean: no; an agent's machine is often the expensive half and should
   follow its own idle policy.
2. **Should the gateway ever route to a child directly?** Lean: no;
   connection handoff gives the bytes path without a second address.
3. **`orphanAfter` default.** Lean: 10 m, long enough to ride out a worker
   restart, short enough that leaks cost minutes.

## 12. Acceptance criteria

- **Phase 1:** stopping a parent ends its children on the same node within
  one second; a child's session report names its parent; an undeclared or
  over-limit child is refused with the codes in §6.2.
- **Phase 2:** with the parent's node killed, its remote children end within
  `orphanAfter` plus a minute; a child starts from inside an `exec` session
  with no credential in the task.
- **Phase 3:** a handed-off stream runs at the datapath's measured L4
  throughput.
