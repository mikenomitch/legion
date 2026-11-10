# Judgement Calls: child-sessions-plan

Decisions made while designing
[`plans/child-sessions-plan.md`](../plans/child-sessions-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** These are design-time calls; implementation inserts
its own at their rank. Entry 1 revises the first draft (2026-10-28), which
limited ownership to sessions; it says what it replaced, and entries 5 to 7
follow from it.

---

## 1. Ownership is a typed reference stored with the child, for every owner kind (revised)

**Call.** A child stores `owner = { kind, id }`, where `kind` is
`session`, `instance`, or `allocation`, in whatever holds the child's own
record: the node's session table for a session child, an `owner` column on
the instance row for a keyed child. Whoever holds that record runs the
cascade when the owner's authority (its node, or the managers) says the
owner reached a terminal state.

```console
$ legion session start machine --owner agent-q4n7c2rd.01jbx... -j
{"ok":true,"data":{"sessionId":"machine-q4n7c2rd.01jbz...",
 "owner":{"kind":"session","id":"agent-q4n7c2rd.01jbx..."}, ...}}
```

**This replaces the first draft,** which had sessions only, a `parent_id`,
a child table in the node's `sessions.db` as the only record, a node-local
cascade on the parent's session ending, and "children of keyed instances:
later, if asked" as a non-goal. Review asked whether that built Legion into
a corner, and it did, three ways: the model assumed a node-owned owner, so
a keyed instance or allocation could never own anything without a second
mechanism; "the parent ends" meant its session ending, which for an
instance would have killed children whenever it was merely suspended or
rescheduled; and promotion, which exists today, would have left a promoted
owner's children pointing at an id that no longer exists.

**Alternatives.** (a) The draft: sessions only, extend later. (b) Owner
records as their own raft objects, reconciled by a manager-side collector
(Kubernetes' garbage collector).

**Trade-off.** Phase 1 still builds only session owners, but its wire
shapes, names, and terminal-state rules are the general ones, so the plan
carries more model than Phase 1 uses. Raft-owned owners need a manager
push plus a full-report check, two paths where node-local needed one.

**Why (extensibility without cost on the hot path).** Storing the reference
with the child keeps session children entirely off raft, whatever owns
them, while keyed children cascade atomically in the owner's transaction.
(b) would put an object in raft per ephemeral child, which is what session
mode exists to avoid.

---

## 2. The right to own children is granted by the jobspec, not by a credential

**Call.** A task lists the sibling tasks its units may start as children
(a top-level `children` block, with `max`). An owner starts a child with no
token: the isolate host names the owner over its socketpair, and other
runtimes are attributed by cgroup on the local API socket.

```nickel
children = { tasks = ["machine"], max = 2 }
```

**Alternatives.** (a) Give the workload a session ticket or token scoped to
the child task. (b) Let any unit start any session in its job.

**Trade-off.** Changing what a task may spawn is a job update, not a token
issue.

**Why (security, simplicity).** The workload that wants a child is the one
most likely to be prompt-injected; a credential inside it is usable for
anything the credential allows, for as long as it lives. A declaration in
the spec is reviewed with the spec, bounded by `max`, and impossible to
exfiltrate. (b) makes every unit's blast radius the whole job. The block is
top-level rather than under `stateful` because service and batch tasks,
which have no `stateful` block, can own children.

---

## 3. A child runs under its own task's policy and envelope, never its owner's

**Call.** A child gets its own task's `resources`, `connect`, `network`,
and identity, and is charged to its own task's envelope or reservation. The
owner's charge does not change.

**Alternatives.** Children inherit the owner's network policy and draw from
the owner's envelope, so "the session" is one budget.

**Trade-off.** An owner with no egress can own a child with egress, which a
reader might not expect; the public doc must say so plainly. Two envelopes
to size.

**Why (simplicity, composability).** Inheritance would make a child's
behavior depend on which owner started it, so the same task would behave
differently per caller, and a small isolate's envelope would have to hold a
1 GiB VM. With own-task policy, a child is just a unit of its task, every
existing rule applies unchanged, and accounting at burst is "a second unit
appears in another pool", never "a unit changes size".

---

## 4. The owner's id is the only address; children are reached through it

**Call.** The gateway, the router, and edge pickers route only to owners. A
child is reached through its owner's handle, or by an operator with
`job:write`. A burst never re-points the owner's address.

**Alternatives.** (a) Re-point the owner's address at the child after a
burst, through a mutable mapping (a raft write, or a compare-and-set on the
session manifest). (b) Expose children through the gateway under their own
ids.

**Trade-off.** Every forwarded request takes one more local hop through the
owner (tens of microseconds), and a network hop when the child landed on
another node. Bulk streams need connection handoff (Phase 4) to avoid the
owner in the byte path.

**Why (simplicity, correctness).** (a) needs two kinds of address, a window
in which requests reach the old target while the switch is in flight, and a
VM's wake time on every request instead of an isolate's. (b) publishes a
second address callers must cache and keep right across moves. The owner
costs almost nothing to keep, so keeping it as the address is the cheap
option. Cloudflare's Durable Objects in front of Containers is the same
shape.

---

## 5. "Ended" is the owner's terminal state; sleep and moves never cascade

**Call.** An owner has ended when its authority says so: a session stopped,
destroyed, or its request finished; an instance stopped or destroyed; an
allocation terminal. Suspend, archive, reschedule, and move are not ends.
The cascade applies the owner's verb (stop stops, destroy destroys).
Children do not sleep with their owner unless `suspendWithOwner = true`.

**Alternatives.** (a) Tie children to the owner's process or allocation, so
anything that stops it stops them. (b) Full OTP-style linking: children
suspend, resume, and move with the owner.

**Trade-off.** A sleeping owner can leave an idle child running until the
child's own `idle.after`. A partition longer than `orphanAfter` ends
children of a live session owner.

**Why (correctness, ease).** (a) is what the first draft effectively did,
and it would end a keyed agent's machines every time the agent was
suspended or rescheduled. Ending is the one transition where a leak costs
money and nobody notices, so it cascades unconditionally; sleep is where
owner and child often want different policies (a cheap isolate waits for
hours, its VM should sleep after minutes), so each follows its own.

---

## 6. Children are sessions or keyed instances; never count-placed allocations, never a durable child of a session

**Call.** A child task must be a session task or a keyed task. A session
cannot own a keyed child.

```text
[children] agents/agent: children.tasks names "workspace", a keyed task, but agent is a session task; a session cannot own a durable child (promote the session, or make agent keyed)
```

**Alternatives.** (a) Let owners create service or batch allocations. (b)
Let a session own a keyed instance, with `orphanAfter` as the safety net.

**Trade-off.** A platform that wants an ephemeral agent to create a durable
workspace either promotes the agent first or creates the workspace without
an owner.

**Why (simplicity, correctness).** The scheduler owns how many service,
batch, and slot allocations exist; an owner creating them would fight it.
A durable child of an ephemeral owner has no durable record of who owns it,
so a lost node would leave a raft object whose owner nothing can ever
confirm ended.

---

## 7. Promotion carries ownership; until it can, an owner with children is not promoted

**Call.** Promoting an owner re-keys its children's references to the new
instance, with no extra raft write. Promoting a child detaches it when its
owner is a session, and keeps the reference otherwise. Phase 1, which has
no raft-owned owners yet, refuses to promote a session that owns children.

**Alternatives.** (a) Ignore promotion until a later phase. (b) In Phase 1,
detach or stop the children of a promoted owner.

**Trade-off.** A Phase 1 platform that promotes agents with machines must
stop the machines first or wait for Phase 2.

**Why (correctness).** Promotion exists today, so (a) ships an orphan
generator. (b) silently ends or abandons work the owner was using. A clear
refusal that names the phase is the honest Phase 1 answer.

---

## 8. Depth 1, same job

**Call.** Children cannot have children, and must be in their owner's job.

**Alternatives.** Arbitrary trees; cross-job children.

**Trade-off.** An orchestrator that wants a tree must flatten it.

**Why (simplicity).** Every limit and cascade rule above is one level deep.
Trees need recursive cascades across nodes and raft, and cross-job grants
need a second spec to agree. Each is a later plan if a real user asks.

---

## 9. Same node first; another node only in Phase 2

**Call.** A session child is admitted on its owner's node. Phase 1 refuses
when the child task's envelope there is full; Phase 2 falls back to the
session router.

**Alternatives.** Place children anywhere from the start.

**Trade-off.** Phase 1 children fail on a full node that a cluster-wide
placement would have served.

**Why (performance, phasing).** Same-node children make the owner's
forwarding and file moves local, which is the common case and the one the
isolate burst needs first. Cross-node children need the orphan sweep and
worker-plane forwarding, which are their own phase.

---

## 10. The managers ask about a session owner on another node; the child's node does not

**Call.** A child whose `session` owner is not on its node (it was placed
elsewhere, or the owner moved) is ended by an owner notice from the
managers. A manager asks where the owner is (`Worker.GetSession` by its
node tag, then every manager's index) at most once a minute per owner, off
the heartbeat path, and ends the children when the owner's node says it
holds no such session, or after `orphanAfter` when that node does not
answer.

**Alternatives.** The plan's text (§6.4): the child's node asks the owner's
node directly every minute.

**Trade-off.** With no manager reachable, a remote session owner's children
are not ended until one is. The public doc says so under "Failure modes".

**Why (simplicity, security).** Workers do not call each other's worker
plane with their own identity today: a peer call presents no client
certificate and relies on a caller's ticket. Node-to-node probing would
have needed a new caller class and an address book on every worker. The
managers already hold the node list, the tag map, the mTLS client, and
`sessions::locate`, which returns exactly the three answers the rule needs
(found, not found, unreachable).

---

## 11. An instance owner's stop and destroy end its keyed children in the same entry; every other end is swept by the leader

*Added 2026-11-10, with Phase 3.*

**Call.** `stop_keyed_instance` and `destroy_keyed_instance` end the
owner's keyed children inside their own apply, before the owner's rows
change, applying the owner's verb. Owners that end any other way (an
`allocation` owner that completes, fails, or is lost; an instance ended by
its job's stop or a scale-down) are found by a sweep the scheduler lane's
leader runs every 5 seconds, which ends their children in one entry.

```console
$ legion instance stop user-42      # its keyed children are stopped in this entry
$ legion instance destroy user-42   # ... and destroyed in this one
```

**Alternatives.** (a) The plan's letter (§6.4): every terminal transition of
every owner kind cascades in its own transaction. (b) Sweep only, no
in-entry cascade.

**Trade-off.** A keyed child of an allocation owner outlives it by up to 5
seconds, and one more scheduler-lane entry is written when that happens. A
manager that loses leadership mid-sweep leaves the rest to the next leader.

**Why (simplicity, performance).** An allocation reaches a terminal state
through a dozen store paths (node loss, completion, eviction, job stop,
rollouts), most of them hot; an indexed lookup for children in each would
tax every allocation removal for a feature few jobs use, and one missed
path would leak a child for ever. The two explicit instance verbs are the
ones an operator types and expects to be atomic, so they are. The sweep is
one indexed read of a partial index that is empty in almost every cluster.

---

## 12. Raft-owned owners are judged on every heartbeat, and pushed on a one-second stream tick

**Call.** `owner_notices(store, node)` reads the owners of the children a
node's last report lists and the owners' rows in the store, on every
heartbeat of that node. The same function runs on the node's work stream
whenever the stream wakes and once a second while any session in the index
carries an owner. A notice is repeated until the report stops listing the
child.

**Alternatives.** The plan's text: send "owners ended" from the apply of
the owner's terminal transition, with the 30-beat full report as the
backstop.

**Trade-off.** One indexed read per distinct owner per beat for nodes that
hold children (nothing for every other node: the index keeps a count of
owned sessions). The push can trail the transition by up to a second.

**Why (simplicity).** An instance or allocation reaches a terminal state
through many store paths (stop, destroy, job stop, node loss, completion,
failure, preemption). Hooking each would have left the cascade one missed
path away from a leak. Reading the owner's state where the notice is sent
has one code path, is correct on every manager, and makes the backstop
every beat instead of every thirtieth.

---

## 13. A worker vouches for its allocation to create a keyed child; the task's identity token is not used

*Added 2026-11-10, with Phase 3.*

**Call.** `legion instance create <task> --owner self` inside a task calls
`Workload.CreateChildInstance` on the API socket. The worker attributes the
connection to its allocation, as for every workload-channel call, and asks a
manager with its own node credential (`Manager.CreateChildInstance`,
worker-plane). The manager checks the allocation is placed on that node and
applies the `children` block.

**Alternatives.** (a) Proxy `Manager.CreateInstance` with the allocation's
`legion` identity token and teach the manager to read `owner = self` from
the token. (b) Require a token with `job:write` in the task.

**Trade-off.** One more Manager RPC, and a compromised worker can create
keyed children for any allocation placed on it (bounded by each job's
`children` block and `max`).

**Why (security, simplicity).** The plan's point is that the owner needs no
credential (calls entry 2). (a) would make the right depend on the task
declaring a `legion` identity and on a grant for it, a second way to get
attribution wrong; (b) puts `job:write` where a prompt-injected agent can
use it. A worker already holds everything on its node, so vouching for its
own allocations adds no new trust.

---

## 14. Connection handoff passes the descriptor on a second socket with a line protocol, not through gRPC

*Added 2026-11-10, with Phase 4.*

**Call.** The owner sends one JSON line and the accepted connection's
descriptor (`SCM_RIGHTS`) on `legion-handoff.sock`, beside `legion.sock` and
admitted by the same rule. The worker answers one line, splices, and writes
a last line when the connection closes. `legion session handoff <child>
--port <label> --fd <n> [--wait]` wraps it.

```text
-> {"child":"machine-q4n7c2rd.01jc0","port":"http"}      (+ the descriptor)
<- {"ok":true}
<- {"closed":true,"sent":10485760,"received":211}
```

**Alternatives.** (a) A gRPC call that returns a one-time token, then a
second connection that presents the token with the descriptor. (b) The
worker accepts the client connection itself and the owner only decides
(an L7 router in the worker). (c) The owner proxies through the worker
(no handoff; the owner stays in the path).

**Trade-off.** One call of the workload channel is not gRPC, so it has its
own tiny client (the CLI, or ten lines in any language with `sendmsg`).

**Why (performance, simplicity).** A descriptor cannot ride HTTP/2, and (a)
is two round trips and a token store to pass one fd. (b) moves the routing
decision into the worker, which the plan refuses: the owner keeps the
decision, the worker moves the bytes. (c) is what handoff exists to avoid.

---

## 15. A child is started through a manager with `CreateInstance.owner`, not a new RPC

**Call.** `legion instance create <job> --task <child> --owner <id>`.
`legion session start ... --owner <id>` is the edge form, with a ticket,
and `--owner self` is the API socket form.

**Alternatives.** The plan's console example (`legion session start machine
--owner <id>` through a manager), which would have needed a manager-routed
`session start`.

**Trade-off.** The plan's example command differs from the shipped one.

**Why (simplicity).** `legion session` is the edge surface (a node and a
ticket); `legion instance create` without `--id` is how every unnamed
session is started through a manager. A child is that call with one more
field, so it inherits its ACL, idempotency, and MCP tool.

---

## 16. Keyed children carry `owner` and `root` identity claims; session children carry none, and no tenant label

*Revised 2026-11-10 with Phase 3: the claims now exist for keyed children.*

**Call.** A keyed child's workload identity token carries `legion.io/owner`
and `legion.io/root` (`instance:user-42`), read from its row. A session
child has no token to carry them: sessions cannot declare `identities`
(the managers mint tokens for allocations in the store, and a session has
none), and no unit carries a tenant label to inherit.

**Alternatives.** Issue identities to sessions first.

**Trade-off.** An upstream cannot attribute a session child's request to
its owner from a token. Attribution is in the session report and `legion
instance get`.

**Why (scope).** Workload identity for sessions is its own limitation with
its own design (tokens for units with no raft allocation).

---

## 17. The owner reference rides in the session's launch payload

**Call.** `ProvisionalAllocation.session` (`SessionStart`) carries `owner`,
`owner_remote`, and `suspend_with_owner`. The worker already persists a
session's launch payload in its `sessions.db` row, so the reference
survives a restart with no schema change; an in-memory index
(`session_owners`) is rebuilt from the payloads.

**Alternatives.** A column on the session row, or a field on `ClaimRecord`.

**Trade-off.** Re-keying an owner rewrites its children's payloads and
rows. `SessionStart` is no longer `Copy`.

**Why (simplicity).** The payload is the one record every path already
carries from admission to the log to a resume.

---

## 18. A child that moves to another node is detached

**Call.** A `store` child resumed on another node becomes an ordinary
session there. The session manifest does not carry the owner.

**Alternatives.** Add the owner to the manifest format.

**Trade-off.** A moved child no longer ends with its owner.

**Why (scope).** The manifest is a published, versioned format that other
nodes and the gateway read. Changing it for a case the plan lists beside
"moving children with their owner" (a non-goal) was not worth a format
version.

---

## 19. `children.max` is exact on the owner's node and approximate across nodes

**Call.** The owner's node counts the children it holds and adds the count
of children elsewhere that the routing manager's index reported
(`SessionStart.owner_children_elsewhere`).

**Alternatives.** A raft counter per owner.

**Trade-off.** Two managers routing children of one owner to two other
nodes within a heartbeat can exceed `max` by the overlap.

**Why (performance).** No session child writes to raft (the plan's goal).
The common case, children on the owner's node, is exact.

*Added 2026-11-10 with Phase 3:* keyed children are counted in the create's
own write, so two concurrent keyed creates cannot both pass; the session
children an owner has are still the index's count, and a session start is
told `max` less the keyed count.

---

## 20. A keyed child's owner node is a placement preference bound to that child, not a pin

*Added 2026-11-10, with Phase 3.*

**Call.** For a keyed child awaiting its first placement the scheduler
narrows the eligible nodes to its owner's node when that node is eligible,
and binds the placement to that child, the way a floating restore is bound.
When the owner's node is not eligible the child goes wherever the task
fits.

**Alternatives.** (a) A hard pin, failing the placement when the owner's
node is full. (b) A scoring term, so the owner's node is merely likelier.

**Trade-off.** A child can land away from its owner, and a later reschedule
does not bring it back. The lookup adds one indexed read per eval of a
keyed task (empty unless a child is pending).

**Why (simplicity).** (a) turns a full node into an unplaceable child for
something that is a convenience (the owner reaches the child either way).
(b) needs the scoring cache to carry a per-instance term, where every
existing term is per task; the floating-restore path already binds one
instance to one narrowed candidate set.

---

## 21. Promoting a child keeps its owner when the managers judge that owner, and detaches it otherwise

*Revised 2026-11-10 with Phase 3, which added the `owner` column.*

**Call.** A promoted child of an `instance` or `allocation` owner keeps the
reference: the promotion writes it into the new row, in its own entry. A
promoted child of a `session` owner is detached.

**Alternatives.** Refuse to promote a child of a session.

**Trade-off.** A promoted child of a session no longer ends with it, and
nothing says so beyond the docs.

**Why (simplicity).** A durable child of an ephemeral owner is not allowed
(entry 6); detaching is what promotion means for it ("keep this one").

---

## 22. `child_gone` is a record the owner reads, not an event

**Call.** A node remembers each ended child for ten minutes with a reason.
`Workload.ListChildren` lists them as `gone`, and a read of the id answers
`NOT_FOUND` with `child_gone:` and the reason. The `Workload` service also
gained `ListChildren` and `StopChildSession`, which the plan did not name.

**Alternatives.** An event to the owner.

**Trade-off.** An owner polls; nothing is pushed to it.

**Why (scope).** Sessions have no events or journal at all (session mode's
limitations), and an owner inside a task has no stream to receive one on.
Listing and stopping are the smallest surface that lets an owner manage
what the jobspec lets it start.

---

## 23. A node's refusal for the owner's sake is final for the router

**Call.** A refusal that starts with `child refused: ` (not declared, at
`max`, the owner is a child) ends the start; the router does not try
another node.

**Alternatives.** Treat it like a full envelope and try the next node.

**Trade-off.** The refusal's text carries a marker the router matches.

**Why (correctness).** Another node would not know the owner and would
admit the child on the manager's word, defeating the owner node's check.

---

## 24. `suspendWithOwner` for an instance owner is sent on the change, once per manager

**Call.** A manager sends the `suspended` notice the first time it sees an
owner instance suspended, and not again until it has seen it not suspended.

**Alternatives.** Repeat it every beat like the terminal notices.

**Trade-off.** A manager failover at that moment can miss the suspend.

**Why (correctness).** A child that was resumed on demand must stay
resumed; repeating the notice would suspend it again every beat.

---

## 25. A handoff reaches only a running session child on the owner's own node

*Added 2026-11-10, with Phase 4.*

**Call.** The handoff socket resolves a child among the sessions this node
holds for the calling owner. A child on another node, a suspended child,
and a keyed child are refused.

**Alternatives.** Dial a remote child over the worker plane; wake a
suspended child first; look keyed children up through a manager.

**Trade-off.** An owner whose child the router placed on another node must
proxy to it, or start it where it can hand off.

**Why (performance, scope).** Across nodes the worker would be a proxy on
the worker plane, not a splice between two local sockets, which is not the
thing the plan measured. Waking on handoff is wake-on-connect, which
belongs to the suspend plan's Phase 8.

---

## 26. `legion.io/root` equals `legion.io/owner`

*Added 2026-11-10, with Phase 3.*

**Call.** A keyed child's identity token carries both claims with the same
value, `kind:id`.

**Alternatives.** Emit only `owner` until grandchildren exist.

**Trade-off.** Two claims say one thing today.

**Why (extensibility).** An upstream that attributes by `root` keeps working
if depth ever grows; adding the claim later would change tokens under
verifiers that already pin their claim set.
