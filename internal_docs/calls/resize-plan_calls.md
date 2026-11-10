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

## 3. The allocation's row holds the effective size; the override column is only the record

*Added 2026-11-10, building Phase 1.*

**Call.** A resize rewrites `allocation.resources` to the effective amounts
and writes `allocation.resources_override` beside it with what differs from
the spec, who set it, and why. Nothing reads the override to learn a size.

```text
resources          = {"cpu":2000,"memory":1024}
resources_override = {"cpu":2000.0,"spec":{"cpu":1000.0,"memory":1024.0},
                      "reason":"batch week","by":"accessor:7f3c...","at":"2026-11-10T..."}
```

**Alternatives.** (a) Keep the row at the spec and make every reader
(free pools, plan apply's recheck, preemption, `node usage`, the ledger)
overlay the override. (b) A separate `allocation_override` table.

**Trade-off.** The row no longer says what the spec asked for; a reader who
wants that reads `resources_override.spec` or the task config. A cluster
default that changes after placement is indistinguishable from an override
by the row alone, which is why the payload overlay reads the record before
trusting a difference (entry 17).

**Why (simplicity, performance).** Elastic envelopes already resize by
rewriting the row, and every consumer of capacity already reads it. (a)
would add an overlay to a dozen hot readers and one missed reader would
double-book a node. With the row effective, the scheduler needed no change
at all, so this shipped with no scheduler diff and no benchmark delta to
explain.

---

## 4. The manager calls the node directly, and the desired set is the backstop

*Added 2026-11-10, building Phase 1.*

**Call.** `ResizeAllocation` asks the node's runtime first (a dry run),
commits the compare-and-set, and then calls the node again to apply (the
reverse order for a shrink). Separately, a node that sees a running
allocation's desired payload carry a different size resizes it in place.

**Alternatives.** (a) Commit the row and let the heartbeat's desired set
deliver the resize, the way a placement is delivered. (b) Call the node
only, with no convergence path.

**Trade-off.** Three steps instead of one for a grow (pre-flight, commit,
apply), and a rollback write on the rare apply that fails after a
pre-flight that passed. Two delivery paths have to agree on what a size is.

**Why (ease of use).** The plan's answers are synchronous:
`needs_restart` has to come back from the call that asked, and (a) can only
answer "committed" and leave the runtime's refusal in a log. The pre-flight
makes `needs_restart` and `not_supported` cost nothing and leave nothing to
undo. (b) would strand a committed grow whenever one RPC is lost; the
desired set already carries every allocation's resources on every
heartbeat, so convergence was a comparison away.

---

## 5. A resize that does not fit is refused; moving is opt-in

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

## 6. Runtimes answer `needs_restart` or `not_supported`; they never restart to resize

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

## 7. An allocation's override ends with the allocation, including across a suspend and resume

*Added 2026-11-10, building Phase 1.*

**Call.** The override is on the allocation row and nowhere else. A keyed
instance that is suspended and resumed, rescheduled after node loss, or
replaced by a rollout gets a new allocation at the spec.

```text
$ legion instance get user-42
resources: cpu 500, memory 1024, memoryMax 2048 (effective)
resized: override active; spec memory 512; reason: bigger cart; by accessor:7f3c at 2026-11-10T12:00:00Z
```

**Alternatives.** Record the size on the instance row so it follows the
instance across allocations.

**Trade-off.** An operator who resized a keyed sandbox and then suspended
it finds it back at the spec on resume, and has to resize again. A session
does keep its size across a suspend, because its size lives in its own
launch payload (plan open question 2), so the two kinds differ.

**Why (simplicity).** The plan puts the unit override "on the allocation
row" and says it "ends with the unit". An instance-level size is exactly
what Phase 4's `--move` needs (suspend, record the new size, resume
elsewhere), and it should be designed there with placement of the resumed
size, not added here as a second place a size can live.

---

## 8. Resize reuses the `scale` ACL resource

**Call.** `scale:write` on the job authorizes every resize.

**Alternatives.** A new `resize` resource.

**Trade-off.** A principal allowed to change counts can also change sizes.

**Why (simplicity).** The `scale` resource's own doc says it exists "so a
principal can resize without redeploy rights"; both are capacity changes
without a deploy, and the autoscaler needs both.

---

## 9. Changing tier is not a resize

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

---

## 10. One Worker RPC resizes both sessions and committed allocations

*Added 2026-11-10, building Phase 1.*

**Call.** `Worker.ResizeSession` takes `session_id` or `allocation_id`.
The plan names it for sessions only and says nothing about how a top-level
allocation's node is told.

**Alternatives.** A second RPC, `Worker.ResizeAllocation`.

**Trade-off.** The name undersells it: a reader of the proto sees
"session" on an RPC that also takes an allocation id.

**Why (simplicity).** Both targets run the same code on the node
(`WorkerTaskManager::resize_unit`) with one caller class (`manager`). A
second RPC would have been a second handler, a second caller-class row, and
four more test stubs for no difference in behavior.

---

## 11. A session's resize emits no event and records no idempotency key

*Added 2026-11-10, building Phase 1.*

**Call.** `instance.resized` is emitted for keyed and slot instances only,
and `idempotency_key` is honored for allocations and keyed instances only.
The plan lists both for every form.

**Alternatives.** Emit the event and record the key for sessions too.

**Trade-off.** A session's resize is visible in the node's log, the
`legion_resizes_total{unit="session"}` counter, and the session's own
`resources`, but not in the event journal. A retried session resize answers
`applied` with `from` equal to `to`, not `deduplicated: true`.

**Why (performance).** Both are replicated writes, and the plan's own
acceptance criterion is that "a session grow inside its envelope writes no
raft entry". A session resize sets absolute amounts on one node, so
repeating it is already safe.

---

## 12. A sandbox's cgroup ceiling stays at its boot size; the balloon alone moves the guest's ceiling

*Added 2026-11-10, building Phase 2.*

**Call.** A sandbox resize writes `memory.low` and `cpu.weight` on the VMM
cgroup and sets the balloon; it leaves the cgroup's `memory.max` at the
boot size plus the VMM overhead. The balloon record gains `floor_mib`, the
size the resize holds, and the pressure response deflates back to it.

**Alternatives.** Lower the cgroup's `memory.max` with the balloon, so the
kernel enforces the new ceiling.

**Trade-off.** The new ceiling is enforced by the guest's balloon driver,
not by the host kernel. Firecracker boots the balloon with
`deflate_on_oom`, so a guest under its own memory pressure can take pages
back above the resized ceiling.

**Why (reliability).** Inflation is asynchronous: the guest hands pages
back over seconds. A cgroup ceiling lowered at once would sit under the
VMM's resident memory and the kernel would kill the VMM, which is the
restart the plan forbids a resize from causing.

---

## 13. A deferred session grow counts toward its elastic envelope's size for 60 seconds

*Added 2026-11-10, building Phase 1.*

**Call.** A grow an elastic envelope could hold but has no room for yet
answers `deferred` and registers the difference as demand on the node. The
envelope's next sizing counts it as held, the node asks its manager for the
room on its heartbeat, and the caller's retry is applied.

**Alternatives.** (a) Answer `refused` and let the caller wait for the
envelope to grow for other reasons. (b) Hold the call open until the
envelope has grown.

**Trade-off.** A caller that never retries holds the envelope larger for up
to a minute before it shrinks back.

**Why (ease of use).** An elastic envelope is sized from what its sessions
hold, so with (a) a refused grow never makes room for itself and retries
fail forever. (b) ties an RPC to a heartbeat round trip and a scheduler
write. The plan asks for "`RESOURCE_EXHAUSTED` (retryable) until it has";
`deferred` is that answer with the wait made explicit.

---

## 14. A resize that grows one reservation and shrinks another takes the grow order

*Added 2026-11-10, building Phase 1.*

**Call.** When any of `memory` or `cpu` goes up, the whole resize reserves
first and applies second, even if the other goes down.

**Alternatives.** Split it into a shrink and a grow, each in its own order;
or refuse mixed resizes, as elastic envelopes do.

**Trade-off.** The amount that shrinks is released to the scheduler a
moment before the node lowers it.

**Why (simplicity).** The early release is of `cpu.weight` or
`memory.low`, neither of which is a hard limit, so nothing can be stranded
by it. Two writes for one request would make a half-applied resize
possible.

---

## 15. Where resources are not enforced, a resize is an accounting change and answers `applied`

*Added 2026-11-10, building Phase 1.*

**Call.** A unit with no cgroup assignment (enforcement off, macOS), or one
that declared no resources and so started without limits, is resized on the
row and in the worker's record only.

**Alternatives.** Answer `not_supported`; or start confining a unit that
ran unconfined.

**Trade-off.** `applied` means "the reservation moved", not always "a limit
was written".

**Why (ease of use).** Resources are already scheduling-only in those
configurations, and the reservation is what an operator on such a cluster
is resizing. Starting to confine a running, previously unlimited process is
a behavior change a resize should not smuggle in.

---

## 16. A refused grow is `resource_exhausted` on the CLI, a dry run exits zero, and the `needs_restart` hint does not name `--move`

*Added 2026-11-10, building Phase 1.*

**Call.** The RPC reply carries the plan's five outcomes. The CLI reports a
grow that found no room under the code `resource_exhausted` (the plan's
example), every other non-applied outcome under its own name, and a
`--dry-run` as a success envelope whatever its outcome. The `needs_restart`
hint says to restart or redeploy.

```text
{"ok":false,"error":{"code":"needs_restart","message":"sandbox ws-7 booted with a 4096 MiB guest; ...",
 "hint":"restart the instance to start it at the new size (a redeploy with the new resources does the same for every allocation of the task)","retryable":false}}
```

**Alternatives.** A gRPC error status per outcome; a non-zero exit for a
dry run that would be refused; the plan's hint text, which offers `--move`.

**Trade-off.** An agent reads `outcome` on success and `error.code` on
failure for the same fact. The hint is less helpful than the plan's until
Phase 4.

**Why (ease of use).** Outcomes are answers, and the reply has to carry
`from`, `to`, and `free` with them, which a status cannot. A dry run is a
question; failing the command for its answer makes scripts treat "it would
not fit" as "the command broke". A hint must not offer a flag that does not
exist yet.

---

## 17. The payload overlay reads the override only when the row differs from the declared size

*Added 2026-11-10, building Phase 1.*

**Call.** Building an allocation's payload compares the row's `memory`,
`memoryMax`, and `cpu` with the task's declared-or-default amounts. Only a
difference reads `resources_override`, and only the amounts the record
names are laid over the declared resources.

**Alternatives.** Read the override for every allocation on every
heartbeat; or overlay the row's amounts whenever they differ.

**Trade-off.** A comparison per allocation per heartbeat, and a second
place that knows the cluster defaults.

**Why (performance).** The heartbeat's desired set is built for every node
every few seconds, and a read per allocation there was measured as a cost
before (`desired_payload`'s comment). Overlaying on difference alone would
turn a changed cluster default into a declared size on a task that declared
none, which would start confining it.

---

## 18. `legion alloc` exists only to hold `resize`

*Added 2026-11-10, building Phase 1.*

**Call.** The plan's `legion alloc resize` needed a top-level `alloc`
command, which did not exist. It was added with one subcommand.

**Alternatives.** `legion job resize-alloc`; or only `instance resize`.

**Trade-off.** A command group with one verb.

**Why (ease of use).** The plan shows the command as users will type it,
and a service replica has no instance id to address it by.

---

## 19. An owner resizes its children over the API socket, on its own node only

*Added 2026-11-10, building Phase 1.*

**Call.** `Workload.ResizeChildSession` (`legion instance resize <child>
--local`) resizes a child session the caller owns, when it runs on the
caller's node. A child on another node answers `FAILED_PRECONDITION`.

**Alternatives.** Forward a remote child's resize through a manager with
the node's credential, as keyed-child creation does.

**Trade-off.** An owner whose child the router placed elsewhere needs a
token to resize it.

**Why (scope).** A same-node resize is one function call under the lock
the socket already holds. The remote path needs a worker-plane manager RPC
with its own attribution rule, which is more surface than "a child grows
after it starts" justified here.
