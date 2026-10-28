# Judgement Calls: child-sessions-plan

Decisions made while designing
[`plans/child-sessions-plan.md`](../plans/child-sessions-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** These are design-time calls; implementation inserts
its own at their rank.

## 1. The right to start children is granted by the jobspec, not by a credential

**Call.** A session task lists the sibling session tasks its sessions may
start (`stateful.children.tasks`, with `max`). A parent starts a child with
no token: the isolate host names the parent over its socketpair, and other
runtimes are attributed by cgroup on the local API socket.

```nickel
stateful = { mode = "session", children = { tasks = ["machine"], max = 2 } }
```

**Alternatives.** (a) Give the workload a session ticket or token scoped to
the child task. (b) Let any session start any session in its job.

**Trade-off.** Changing what a session may spawn is a job update, not a
token issue.

**Why (security, simplicity).** The workload that wants a child is the one
most likely to be prompt-injected; a credential inside it is usable for
anything the credential allows, for as long as it lives. A declaration in
the spec is reviewed with the spec, bounded by `max`, and impossible to
exfiltrate. (b) makes every session's blast radius the whole job.

---

## 2. A child runs under its own task's policy and envelope, never its parent's

**Call.** A child gets its own task's `resources`, `connect`, `network`,
and identity, and is charged to its own task's envelope. The parent's
charge does not change.

**Alternatives.** Children inherit the parent's network policy and draw
from the parent's envelope, so "the session" is one budget.

**Trade-off.** A parent with no egress can own a child with egress, which a
reader might not expect; the public doc must say so plainly. Two envelopes
to size.

**Why (simplicity, composability).** Inheritance would make a child's
behavior depend on which parent started it, so the same task would behave
differently per caller, and a small isolate's envelope would have to hold a
1 GiB VM. With own-task policy, a child is just a session of its task, every
existing rule applies unchanged, and accounting at burst is "a second unit
appears in another pool", never "a unit changes size".

---

## 3. The parent's id is the only address; children are reached through it

**Call.** The gateway, the router, and edge pickers route only to root
sessions. A child is reached through its parent's handle, or by an operator
with `job:write`. A burst never re-points the parent's address.

**Alternatives.** (a) Re-point the parent's address at the child after a
burst, through a mutable mapping (a raft write, or a compare-and-set on the
session manifest). (b) Expose children through the gateway under their own
ids.

**Trade-off.** Every forwarded request takes one more local hop through the
parent (tens of microseconds), and a network hop when the child landed on
another node. Bulk streams need connection handoff (Phase 3) to avoid the
parent in the byte path.

**Why (simplicity, correctness).** (a) needs two kinds of address, a window
in which requests reach the old target while the switch is in flight, and a
VM's wake time on every request instead of an isolate's. (b) publishes a
second address callers must cache and keep right across moves. The parent
costs almost nothing to keep, so keeping it as the address is the cheap
option. Cloudflare's Durable Objects in front of Containers is the same
shape.

---

## 4. Children end with their parent but do not sleep with it by default

**Call.** A parent's end ends its children (local at once, remote by
`StopSession`, lost-parent orphans after `orphanAfter`). A parent's suspend
leaves children to their own idle policy unless `suspendWithParent = true`.

**Alternatives.** (a) Full OTP-style linking: children suspend, resume, and
move with the parent. (b) No cascade: the parent cleans up.

**Trade-off.** A sleeping parent can leave an idle child running until the
child's own `idle.after`. A partition longer than `orphanAfter` ends
children of a live parent.

**Why (ease).** Ending is the one transition where a leak costs money and
nobody notices, so it cascades unconditionally. Sleep is where the parent
and child often want different policies (a cheap isolate waits for hours;
its VM should sleep after minutes), so each follows its own.

---

## 5. Depth 1, same job, sessions only

**Call.** Children cannot have children, must be in the parent's job, and
both parent and child are session-mode tasks.

**Alternatives.** Arbitrary trees; cross-job children; keyed parents.

**Trade-off.** An orchestrator that wants a tree must flatten it.

**Why (simplicity).** Every limit and cascade rule above is one level deep
and node-local. Trees need recursive cascades across nodes, cross-job grants
need a second spec to agree, and keyed parents need a durable owner record.
Each is a later plan if a real user asks.

---

## 6. Same node first; another node only in Phase 2

**Call.** A child is admitted on its parent's node. Phase 1 refuses when the
child task's envelope there is full; Phase 2 falls back to the session
router.

**Alternatives.** Place children anywhere from the start.

**Trade-off.** Phase 1 children fail on a full node that a cluster-wide
placement would have served.

**Why (performance, phasing).** Same-node children make the parent's
forwarding and file moves local, which is the common case and the one the
isolate burst needs first. Cross-node children need the orphan sweep and
worker-plane forwarding, which are their own phase.
