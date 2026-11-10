# Judgement Calls: runtime-disable-plan

Decisions made while designing
[`plans/runtime-disable-plan.md`](../plans/runtime-disable-plan.md).
**Entries are ordered by project impact, highest first, and an entry's
number is its rank.** Entries marked *Implementation call* were made while
building it (2026-11-10); the rest are design-time calls.

## 1. One switch for every runtime, enforced at start, separate from namespace policy

**Call.** `legion operator runtime disable <runtime> [--namespace ...]`
writes a replicated record that refuses every start of the runtime in scope
(placement, router and edge session starts, claims, restarts), on the
workers as well as the managers. Namespace policy keeps gating submit.

**Alternatives.** (a) A kill switch per runtime (the isolate plan's first
draft had `legion operator isolate disable`). (b) Extend namespace policy
with a cluster-level policy and enforce `runtimes` at placement too.

**Trade-off.** Two mechanisms touch "which runtimes may run here": a
standing rule (policy) and an incident switch (this).

**Why (simplicity in an incident).** Escapes happen in every isolating
runtime (runc, the VMMs, wasmtime), and the response is the same, so one
command fits all. (b) would make a temporary incident action an edit to a
permanent rule, across every policy, and would still miss edge starts unless
workers enforced policy too.

---

## 2. One record per runtime: a disable widens, never narrows, and a cluster-wide disable is not lifted one namespace at a time

*Implementation call, 2026-11-10.*

**Call.** The store holds one `runtime_state` row per runtime. `disable R
-n A` then `disable R -n B` covers `A, B`; `disable R` replaces any scope
with the cluster; a scoped disable on top of a cluster-wide one stays
cluster-wide. `enable R -n A` removes `A` from a scoped record and is
refused against a cluster-wide one:

```text
FAILED_PRECONDITION: runtime "sandbox" is disabled cluster-wide since 2026-11-07T18:04:11Z; a cluster-wide disable cannot be lifted one namespace at a time
Hint: `legion operator runtime enable sandbox`, then `legion operator runtime disable sandbox --namespace <glob>` for the namespaces that should stay off
```

**Alternatives.** (a) One row per `disable` call, as the plan's §7 words it
("one row per disable"), each with its own reason and scope, removed by an
id. (b) Let `enable -n` carve an exception out of a cluster-wide disable
(an allow list beside the deny list).

**Trade-off.** One `reason` and one `since` per runtime: two advisories
against the same runtime share a line, and the second call's reason
replaces the first. An operator who wants "everything except namespace X"
writes the complement as globs.

**Why (simplicity in an incident).** The question a responder asks is "is
`sandbox` off for this namespace, yes or no", and one record answers it
without summing rows. (a) needs ids nobody has to hand at 3am and makes
`enable sandbox` ambiguous. (b) makes a disable's meaning depend on a
second list, and an exception that outlives the incident is a hole nobody
remembers; refusing it by name costs one extra command.

---

## 3. Running work is untouched unless asked; `--drain` suspends before it stops

**Call.** A disable refuses new starts only. `--drain` suspends units whose
task has a `suspend` block and gracefully stops the rest.

**Alternatives.** Stop everything on the runtime at once.

**Trade-off.** Work already running on a vulnerable boundary keeps running
until the operator drains it.

**Why (ease, correctness).** Whether running work must come off depends on
the advisory (some need a malicious workload already inside, some do not),
and that judgement is the operator's. When it must come off, suspending
keeps state, so enabling again loses nothing.

---

## 4. The managers drain allocations once, when the disable is written; each node drains its own sessions for as long as the record says `drain`

*Implementation call, 2026-11-10.*

**Call.** `--drain` is two mechanisms. The manager that handles the call
walks the live allocations of the runtime in scope: a keyed instance whose
task has a `suspend` block gets the same automatic suspend the idle sweep
starts (reason `runtime_disabled`), and everything else is removed with
stop cause `runtime_disabled`, followed by one `job_reschedule` per job so
the job shows as blocked. Sessions are node-owned soft state, so the
record carries `drain = true` and every node, on every set it receives,
suspends its running sessions of the runtime that can be suspended and
stops the rest.

**Alternatives.** (a) Drain everything from the managers, calling
`Worker.SuspendSessions` / `StopSessions` per node. (b) Drain everything
from the workers, reusing the memory-pressure path (the worker stops an
allocation itself and reports it). (c) A leader loop that keeps draining
allocations while the record stands.

**Trade-off.** The allocation half runs once: an allocation that could not
be drained is listed in `drained.failed` and needs the command run again.
The session half is level-triggered, so a worker spends a pass per
heartbeat while a draining record stands.

**Why (correctness, simplicity).** (a) cannot reach the sessions the
managers do not know about (only sandbox sessions are reported), and adds
a fan-out with its own partial failures to an incident command. (b) would
route a deliberate stop through the failure path, spending each task's
`onUnhealthy` move budget and failing jobs whose budget is small. (c) is
not needed: placement and the workers already refuse every new start, so
nothing can reappear on the runtime after the one pass.

---

## 5. Submit warns; placement refuses

**Call.** A job submitted for a disabled runtime is accepted with a warning,
and its evaluation is blocked with `runtime_disabled` until enable.

**Alternatives.** Refuse at submit.

**Trade-off.** A job can be admitted that cannot place until the runtime is
enabled.

**Why (ease).** An author deploying during the incident does not lose their
deploy, and it places on its own when the runtime comes back.

---

## 6. A job's other groups still place, an update or restart waits whole, and the eval blocks on the runtime only when nothing could place

*Implementation call, 2026-11-10. Narrows plan §6.2 ("the eval is blocked").*

**Call.** The planner removes tasks whose runtime is disabled from the
plan, with every task of their group (a group places together), and
reports them unplaced after everything short of room. A job with an `exec`
front in one group and a `sandbox` back in another keeps placing its front.
The eval is written `blocked` under `runtime_disabled:<runtime>` only when
the plan placed nothing and nothing else was waiting for capacity. Work
that would replace running allocations waits whole, before it touches
anything, when any placed task of the job is on a disabled runtime: a
restart (and a resubmit that replaces a previous version without a
rollout) blocks under the runtime key, and a rollout pauses, its tick
requeued every 15 seconds without failing the rollout. So the version that
is running keeps running. On `enable`, the wake pass returns to the queue every eval
blocked on the runtime, every eval blocked for another reason whose job
has a task of it, and queues one capacity pass for jobs that placed part
of their work.

**Alternatives.** (a) Block the whole job's eval as soon as one task's
runtime is disabled, first submits included. (b) Report disabled tasks
first, so the key is always `runtime_disabled`. (c) Let an update plan
partially like a first submit.

**Trade-off.** `job status` shows `runtime_disabled:<runtime>` only for
jobs with nothing else to place; a mixed job that also waits for room
shows its capacity key. A deploy to the healthy half of a mixed job waits
for the enable too. A paused rollout polls (one cheap eval per rolling job
every 15 seconds) instead of sleeping until the enable: a blocked rollout
tick could be replaced by another blocked eval of the same job, which
would strand the rollout. The enable has to wake three kinds of waiting job
instead of one.

**Why (correctness, ease).** (a) would keep the healthy half of every new
mixed job from starting during an incident about one runtime. (b) would
stop capacity events from waking a job that is also short of room, since a
runtime key is woken only by the enable. (c) is the dangerous one: an
update removes the previous version's allocations after its plan, so a
partial plan would stop the running work of the disabled runtime with
nothing able to replace it, which is the opposite of "running work is
untouched".

---

## 7. A placement that raced the disable, and a refused restart, are reported as failures

*Implementation call, 2026-11-10.*

**Call.** The worker's check is in the three places a runtime's `start` is
reached (`begin_task_allocation_gated`, `start_registered_allocation`,
`restart_allocation`), plus early checks where a caller is waiting for an
answer (edge claims, session admission, session resume). An allocation the
manager placed just before it applied the disable is refused by the node
and reported failed, as any start that fails is; the replacement eval then
blocks on the runtime. A task that exits and whose `restart` policy would
relaunch it is refused the same way.

**Alternatives.** A new report from worker to manager ("refused, runtime
disabled") that removes the allocation without counting a failure.

**Trade-off.** Each such refusal spends one move from the task's
`onUnhealthy` budget and shows as a failed allocation in the job's history.

**Why (simplicity).** The window is one heartbeat wide and the count is one
per allocation, because the scheduler refuses the replacement. A new report
type is a wire change and a manager code path for a race that settles
itself.

---

## 8. A worker writes the set to disk and reads it back at start

*Implementation call, 2026-11-10. Extends plan §6.3.*

**Call.** The worker stores the set in
`<worker state dir>/runtime-states.json` whenever it changes, and loads it
before it reaches a manager.

**Alternatives.** Keep the set in memory only, as the plan's "keeps the
last set it held" reads.

**Trade-off.** One more file in the state dir, and a stale disable on a
worker that was down across the enable, until its first heartbeat.

**Why (security).** A worker that restarts during a manager outage would
otherwise come up with an empty set and accept edge tickets for the
runtime the incident is about. Failing closed for one heartbeat is the
right side to err on.

---

## 9. Not an MCP tool

**Call.** The switch is in the CLI, the API, and the catalog, but not in the
MCP server's tools.

**Alternatives.** A write-gated MCP tool like every other mutating
operation.

**Trade-off.** An agent operating the cluster cannot flip it through MCP.

**Why (security).** A cluster-wide stop of a runtime is an incident action
with a blast radius over every tenant; it should take a human with an
`operator` token, not an agent that was given `--allow-write` for routine
work.

---

## 10. Refusals are `UNAVAILABLE` with error code `RUNTIME_DISABLED`, and the session router refuses before asking any node

*Implementation call, 2026-11-10.*

**Call.** Every refused start answers gRPC `UNAVAILABLE` with the
disable's sentence, a `Hint:` line, and `legion-error-code:
RUNTIME_DISABLED`, so the CLI and MCP envelopes carry `code:
"RUNTIME_DISABLED"`, `retryable: true`. The manager's session router
checks its own copy of the set first.

**Alternatives.** `FAILED_PRECONDITION` (not retryable); letting each node
refuse a routed session start.

**Trade-off.** A client with a tight retry loop on `UNAVAILABLE` retries
for the length of the incident.

**Why (ease for agents).** The request is right and will work later, which
is what retryable means; the code lets an agent tell this from a manager
being down. Refusing in the router names the reason once instead of once
per node tried, and does not count the nodes as full.

---

## 11. A new top-level `legion operator` group

*Implementation call, 2026-11-10.*

**Call.** The plan's surface is `legion operator runtime ...`, and no
`operator` command existed (operator-gated commands live under `raft`,
`state`, `scaling`). This adds the group with `runtime` as its only member.

**Alternatives.** `legion runtime disable`, or `legion node runtime ...`.

**Trade-off.** A top-level group with one subcommand today.

**Why (ease).** The name says which ACL resource it needs, and it is where
the next cluster-wide incident control goes.

---

## 12. `edgeRefusals` and `running.sessions` are best-effort counts

*Implementation call, 2026-11-10.*

**Call.** Workers report refusal counts as deltas on their heartbeat, and
each manager sums what it received, in memory. `running.sessions` counts
the sessions in the managers' session index.

**Alternatives.** Replicate the refusal count; have `status` fan out to
every node.

**Trade-off.** The count restarts with the manager and differs between
managers; sessions of runtimes that are not reported to managers (`exec`,
`container`) are not counted.

**Why (performance).** A raft write per refused edge start would put the
control plane on the path the feature exists to keep it off. The numbers
answer "is anything still hitting this", not an audit.

---

## 13. Any runtime name is accepted, except `envelope`

*Implementation call, 2026-11-10.*

**Call.** `<RUNTIME>` is validated as a name, not against a list, so a
runtime plugin is disabled like a built-in. `envelope` is refused with a
message that says to disable the session task's runtime.

**Alternatives.** Accept only runtimes some node reports; accept
`envelope` and stop placing envelopes.

**Trade-off.** A typo (`sandbx`) is accepted and disables nothing; `status`
shows it with zero running, zero blocked.

**Why (extensibility).** The runtime with the advisory may be a plugin no
node has loaded yet. Disabling `envelope` would stop every session task's
room from being placed whatever its runtime, which is never what an
advisory against one runtime means.

---

## 14. A drain that cannot suspend an instance stops it

*Implementation call, 2026-11-10.*

**Call.** When the automatic suspend cannot start (the node cannot take
checkpoints, a `store` checkpoint could not be published), the drain
removes the allocation for a graceful stop instead of leaving it running.

**Alternatives.** Leave it running and list it in `failed`.

**Trade-off.** In-memory state of that instance is lost; its volumes stay.

**Why (security).** The operator asked for nothing to keep running on the
runtime. Plan §6.2 says the same: what can be suspended is, "everything
else gets a graceful stop".
