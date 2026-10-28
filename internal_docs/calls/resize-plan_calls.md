# Judgement Calls: resize-plan

Decisions made while designing
[`plans/resize-plan.md`](../plans/resize-plan.md). **Entries are ordered
by project impact, highest first, and an entry's number is its rank.**
These are design-time calls; implementation inserts its own at their rank.

## 1. A resize is a runtime override over the spec, not a new job version

**Call.** Resizing a unit or a task writes an override (on the allocation
row, in the node's session table, or as a task-level record beside the
count override), never a job config version, and starts no rollout. A
submit that changes the task's `resources` drops the override.

```console
$ legion alloc resize 88213 --cpu 2000 --reason "batch week"
alloc 88213: cpu 1000 -> 2000 (override active; spec 1000)
```

**Alternatives.** Resize by editing `resources` and resubmitting, with a
rollout that replaces or updates allocations in place.

**Trade-off.** The running size can differ from the spec, so "what is this
allocation's size" needs both numbers shown; an override is state a
GitOps-managed spec does not see.

**Why (simplicity, consistency).** Count overrides already made exactly this
trade for `count`, with the same ACL resource, idempotency, and submit
arbitration; resources follow the same rules, so there is one model of "the
spec plus what operators and the autoscaler changed".

---

## 2. Inside an envelope a resize is node-local; a top-level allocation is one compare-and-set

**Call.** Sessions, isolates, and child sessions resize by admission against
their envelope on their node, with no raft write. Top-level allocations
resize by a scheduler-lane compare-and-set on the allocation row, as
elastic envelopes do.

**Alternatives.** Every resize through the scheduler lane, so the store
always knows every unit's size.

**Trade-off.** The managers do not know a session's current size, only its
envelope's.

**Why (performance).** Sessions exist so that starts and stops do not touch
raft; a resize that did would bring the lane back onto the hot path. The
envelope is already the unit the scheduler packs, so a resize within it is
invisible to placement by construction.

---

## 3. A resize that does not fit is refused; moving is opt-in

**Call.** A grow takes the node's free room only. When it cannot fit, the
answer is `resource_exhausted` with the room available, and `--move`
(movable units only) is how the caller asks for a resize by moving.

**Alternatives.** Move automatically; or preempt lower-priority work.

**Trade-off.** The caller handles a refusal.

**Why (ease of reasoning).** A resize that sometimes silently suspends and
resumes the unit somewhere else is a different operation with different
failure modes (a closed connection, a restore failure). Preemption for a
grow would let any `scale:write` holder evict other work. Envelope grows
already refuse rather than preempt.

---

## 4. Runtimes answer `needs_restart` or `not_supported`; they never restart to resize

**Call.** A runtime declares what it can change live. Anything else (a
sandbox ceiling above its boot size, any change on a runtime without the
trait) is refused by name.

**Alternatives.** Kubernetes' per-resource resize policy, where a resize may
restart the container (`RestartContainer`).

**Trade-off.** Growing a Firecracker guest past its boot size needs an
explicit restart or move.

**Why (simplicity).** One knob fewer, and no resize ever loses a unit's
state behind the caller's back.

---

## 5. Resize reuses the `scale` ACL resource

**Call.** `scale:write` on the job authorizes every resize.

**Alternatives.** A new `resize` resource.

**Trade-off.** A principal allowed to change counts can also change sizes.

**Why (simplicity).** The `scale` resource's own doc says it exists "so a
principal can resize without redeploy rights"; both are capacity changes
without a deploy, and the autoscaler needs both.

---

## 6. Changing tier is not a resize

**Call.** Resize changes the size of a unit within its runtime. An isolate
that needs a real machine starts a child session of a sandbox task
([child sessions](child-sessions-plan_calls.md)); it does not "grow into" a
VM.

**Alternatives.** A size threshold past which an isolate is converted into
a VM, making burst a special case of vertical scaling.

**Trade-off.** Two mechanisms where one might seem to do.

**Why (composability).** Burst is triggered mostly by capability (a
subprocess, a native package), not size, and an isolate that only needs
more memory gets a higher cap in microseconds. Converting a unit's runtime
mid-life would merge size and capability into one operation that nothing
else can build on; keeping them apart lets resize, child sessions, and
move combine freely.
