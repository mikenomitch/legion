# Judgement Calls: child-sessions-plan

Decisions made while designing
[`plans/child-sessions-plan.md`](../plans/child-sessions-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** These are design-time calls; implementation inserts
its own at their rank. Entry 1 revises the first draft (2026-10-28), which
limited ownership to sessions; it says what it replaced, and entries 5 to 7
follow from it.

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
