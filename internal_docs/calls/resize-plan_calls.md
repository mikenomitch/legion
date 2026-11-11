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
trusting a difference (entry 22).

**Why (simplicity, performance).** Elastic envelopes already resize by
rewriting the row, and every consumer of capacity already reads it. (a)
would add an overlay to a dozen hot readers and one missed reader would
double-book a node. With the row effective, the scheduler needed no change
at all, so this shipped with no scheduler diff and no benchmark delta to
explain.

---

## 4. A task-level override is one record beside the count override, bound to the spec it was laid over

*Added 2026-11-10, building Phase 3.*

**Call.** `legion job resize` writes one `task_resize` row per task
(control lane, beside `task_scale`). It records the task's declared size
when it was set and applies only while the spec still declares that size.
The scheduler overlays it on the task before it packs, plan apply's
capacity recheck overlays it, and the allocation insert writes the row at
it with the override record.

```console
$ legion job resize web --memory 1GiB
shop/web: memory 512MiB -> 1GiB (override active; spec memory 512)
  allocations: 1 resized in place, 0 kept their size, 0 replaced
```

**Alternatives.** (a) Rewrite the stored task config's `resources`.
(b) Keep only per-allocation overrides and have `job resize` loop over
what runs, with nothing for new allocations. (c) Delete the override at
submit only, with no binding check at read time.

**Trade-off.** Three readers overlay the same record (the scheduler, plan
apply, the insert), and each plan pays one primary-key read per task. The
binding compares sizes with cluster defaults folded in, so a change to a
cluster default that alters a task's declared size also unbinds its
override.

**Why (simplicity, ease of use).** (a) is a new job version by another
name, which is what the plan exists to avoid. (b) would make a scale-out or
a rescheduled replica come up at the old size. The binding check makes a
dry-run plan of an edited spec honest and means a stale row can never size
anything, the property `base_count` gives count overrides.

---

## 5. The manager calls the node directly, and the desired set is the backstop


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

## 6. A resize that does not fit is refused; moving is opt-in


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

## 7. `job resize` never replaces on its own; `--replace` is asked for and is not a rollout

*Added 2026-11-10, building Phase 3.*

**Call.** The plan says a task resize is applied "in place where it fits
and by the normal replacement path where it does not". As built, an
allocation that cannot change in place keeps its size and is reported
(`pending`), and takes the new size when something else replaces it.
`--replace` stops those allocations in one write so the scheduler places
their replacements at the new size.

```text
hint: 1 allocation(s) kept their old size and take the new one when they are next replaced; pass --replace to stop them now so the scheduler places their replacements at it
```

**Alternatives.** (a) Replace automatically, as the plan's sentence reads.
(b) Run a rollout over the allocations that need replacing, with the
task's rollout strategy.

**Trade-off.** Without the flag a task can run at two sizes for as long as
its old allocations live. With it, every allocation that needs replacing
restarts at once: there is no batch size, health gate, or pause.

**Why (ease of use).** The plan's own rule for a unit is that
`needs_restart` and `not_supported` "are answers, never actions"; a task
resize that silently restarted half a service would break it. (b) is a
deploy, which already exists and paces replacements properly; pointing at
it is better than a second, weaker rollout engine.

---

## 8. Runtimes answer `needs_restart` or `not_supported`; they never restart to resize


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

## 9. An instance's size is a record on the instance, and the insert sizes every new allocation

*Added 2026-11-10, building Phase 1; reversed the same day building Phase 4.*

**Call.** A resize of a keyed or slot instance is written to the instance
row (`instance.resources_override`) as well as to its allocation. Every new
allocation is sized at one place, the insert all placement paths go
through: the instance's own size, over the task-level override, over the
spec. The scheduler packs a keyed task at the largest size any of its
instances still waiting for an allocation carries, and the insert skips a
node the bound instance's own size does not fit.

```text
$ legion instance get user-42          # suspended
resized: override active; spec memory 512; reason: bigger cart; by accessor:7f3c at 2026-11-10T12:00:00Z
$ legion instance start user-42        # resumes at memory 1024, not 512
```

**Alternatives.** (a) Phase 1's choice: the override lives on the
allocation only, so a resumed instance is back at its spec. (b) Size each
instance in the scheduler, with a request per instance rather than per
task. (c) Place at the task's size and resize in place afterwards.

**Trade-off.** While a resized instance waits, every allocation of its task
planned in that pass is packed at the largest waiting size, which can pass
over a node a smaller instance would fit. A bound instance larger than the
node's room costs a skipped candidate. A plain (stateless) allocation's own
override still ends with it, so the two kinds differ.

**Why (ease of use, simplicity).** (a) made an operator resize again after
every suspend, and `--move` cannot exist without a size that outlives the
allocation. (b) is the correct long-term shape but rewrites the scheduler's
per-task plan (candidates, the ledger, and the capacity recheck all carry
one request per task); the bound (c) would overcommit a node for the window
between the two writes. Packing at the largest waiting size is
conservative and needs two store reads per task per plan.

---

## 10. A keyed move is the existing suspend and start, under the caller's grants, and puts the unit back

*Added 2026-11-10, building Phase 4.*

**Call.** `--move` on a keyed instance records the new size on the
instance, calls the `SuspendInstance` handler, waits for the instance to be
suspended (up to 3 minutes), calls the `StartInstance` handler, and waits
for a new allocation (up to 60 seconds). Both handlers run with the
caller's credentials, so a move needs `job:write` as well as `scale:write`.
An instance nobody places at the new size has its previous size record
restored, a reschedule eval enqueued, and the call answers `refused`.

**Alternatives.** (a) A new store operation that suspends, resizes, and
resumes in one entry. (b) Return as soon as the suspend is requested and
let the caller start the instance. (c) Let `scale:write` alone authorize
the move.

**Trade-off.** The call can hold for minutes. A suspend that outlasts the
wait leaves the instance suspended with its new size recorded. An instance
whose checkpoint lives only on its node resumes on that node, so a move
helps a `needs_restart` there but not a full node.

A sandbox moved to a different guest size is suspended to a disk
checkpoint, because a memory checkpoint restores only into a guest of its
own size: it boots fresh with its disk kept. A sandbox session, which has
only its memory checkpoint, is refused such a move.

**Why (simplicity, security).** Suspend and resume are long, multi-step
procedures with their own preconditions (checkpoint kind, publish
preconditions, restore failure policy); (a) would fork all of it. (b)
leaves the unit down until someone remembers it. (c) would let a principal
with resize rights interrupt a workload, which `scale:write` was never
described as granting.

---

## 11. A session moves with its size on the resume request, and a resized session's size travels in its manifest

*Added 2026-11-10, building Phase 4.*

**Call.** `--move` on a `store` session suspends it on its node and resumes
it from its manifest on another node, passing the new size on
`ResumeSessionRequest.resize`; the router reserves that size. Separately, a
resume from a manifest now lays the manifest's recorded resources over the
task's, so a session resized in place keeps its size when it later resumes
on another node. A session no other node takes is resumed where it was, at
the size it had.

**Alternatives.** Write the new size into the manifest before suspending;
try the session's own node first after the suspend freed its room.

**Trade-off.** The session's own node is skipped, so a move never lands
where an in-place resize was just refused, even if the suspend freed
enough room for it. A session's move emits no event (entry 15).

**Why (simplicity).** The resume request already names the job and task
for a move; one optional field carries the size without a second write to
the manifest, whose compare-and-set a failed resume would then have to
undo.

---

## 12. Resize reuses the `scale` ACL resource


**Call.** `scale:write` on the job authorizes every resize.

**Alternatives.** A new `resize` resource.

**Trade-off.** A principal allowed to change counts can also change sizes.

**Why (simplicity).** The `scale` resource's own doc says it exists "so a
principal can resize without redeploy rights"; both are capacity changes
without a deploy, and the autoscaler needs both.

---

## 13. Changing tier is not a resize


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

## 14. One Worker RPC resizes both sessions and committed allocations


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

## 15. A session's resize emits no event, and its idempotency key is its node's, in memory

*Added 2026-11-10, building Phase 1; the key added the same day with Phase 4.*

**Call.** `instance.resized` is emitted for keyed and slot instances only.
A session's `idempotency_key` is honored by the session's node: it keeps an
applied answer for 10 minutes in memory and returns it with
`deduplicated: true`. The plan lists the event and the key for every form.

**Alternatives.** Emit the event for sessions too; record the key in the
managers' idempotency table; persist the node's answers in `sessions.db` as
session runs do.

**Trade-off.** A session's resize is visible in the node's log, the
`legion_resizes_total{unit="session"}` counter, and the session's own
`resources`, but not in the event journal. A retry that reaches the node
after a worker restart is not deduplicated; it answers `applied` with
`from` equal to `to`.

**Why (performance).** An event and a managers' key are replicated writes,
and the plan's own acceptance criterion is that "a session grow inside its
envelope writes no raft entry". A session resize sets absolute amounts on
one node, so the one case the in-memory table misses is harmless, which is
why it does not earn a table on disk.

---

## 16. A sandbox's cgroup ceiling stays at its boot size; the balloon alone moves the guest's ceiling


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

## 17. A deferred session grow counts toward its elastic envelope's size for 60 seconds


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

## 18. A resize that grows one reservation and shrinks another takes the grow order


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

## 19. A task resize clears the own size of each instance it resizes

*Added 2026-11-10, building Phase 3.*

**Call.** When `legion job resize` (or `--reset`) resizes an instance's
allocation in place, it removes that instance's own size record, so the
task's size governs it from then on.

**Alternatives.** Skip instances that carry their own size; keep both and
let the instance's win at its next placement.

**Trade-off.** An operator's per-instance size is overwritten by a
task-wide resize without a separate warning; the reply lists the
allocation as `applied`.

**Why (simplicity).** The last writer wins everywhere else in resize, and
an instance that ran at the task's size but resumed at an older size of its
own would be a surprise nobody asked for.

---

## 20. Where resources are not enforced, a resize is an accounting change and answers `applied`


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

## 21. A refused grow is `resource_exhausted` on the CLI, a dry run exits zero, and hints name `--move` and `--replace`


*Added 2026-11-10, building Phase 1; the hints changed the same day with Phases 3 and 4.*

**Call.** The RPC reply carries the plan's five outcomes. The CLI reports a
grow that found no room under the code `resource_exhausted` (the plan's
example), every other non-applied outcome under its own name, and a
`--dry-run` as a success envelope whatever its outcome. The hints offer the
next step that exists for the unit: `--move` for an instance or session,
`legion job resize --replace` for a plain allocation.

```text
{"ok":false,"error":{"code":"needs_restart","message":"sandbox ws-7 booted with a 4096 MiB guest; ...",
 "hint":"restart the instance to start it at the new size, or pass --move to suspend it and resume it at the new size on a node with room (...)","retryable":false}}
```

**Alternatives.** A gRPC error status per outcome; a non-zero exit for a
dry run that would be refused; the plan's hint text, which offers `--move`.

**Trade-off.** An agent reads `outcome` on success and `error.code` on
failure for the same fact. The `--move` hint is shown for units that turn
out not to be movable, which the move itself then says.

**Why (ease of use).** Outcomes are answers, and the reply has to carry
`from`, `to`, and `free` with them, which a status cannot. A dry run is a
question; failing the command for its answer makes scripts treat "it would
not fit" as "the command broke". Phase 1 withheld the `--move` hint because
the flag did not exist; it does now.

---

## 22. The payload overlay reads the override only when the row differs from the declared size


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

## 23. `legion alloc` exists only to hold `resize`


*Added 2026-11-10, building Phase 1.*

**Call.** The plan's `legion alloc resize` needed a top-level `alloc`
command, which did not exist. It was added with one subcommand.

**Alternatives.** `legion job resize-alloc`; or only `instance resize`.

**Trade-off.** A command group with one verb.

**Why (ease of use).** The plan shows the command as users will type it,
and a service replica has no instance id to address it by.

---

## 24. An owner resizes its children over the API socket, on its own node only


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
