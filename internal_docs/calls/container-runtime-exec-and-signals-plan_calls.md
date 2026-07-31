# Judgement Calls: Container Runtime Exec & Signals Plan

Decisions made while writing
[the plan](../plans/container-runtime-exec-and-signals-plan.md). Ordered by
project impact, highest first; the entry number is its rank. (Planning-stage
calls — implementation will append its own as phases execute. Entries 4 and 5
record reversals of earlier planning calls, made at the product owner's
direction; the superseded designs appear as the losing alternatives.)

## 1. One plan carries all the container runtime's core gaps, with exec + signals sequenced first

**The call.** The original request was "add exec — it's also missing some
other core feature," and the plan's first cut scoped exactly that pair
(signal delivery as the identified second feature), naming cgroup limits and
the task-dir mount as non-goals. The scope was then widened twice on review:
first to include both alternatives, then to Nomad-parity CPU enforcement and
the graceful-kill jobspec knobs. The plan now covers it all as phases 1–6 of
a single document — sequenced exec → signals → stop knobs → memory/weight →
pinning → files.

**The alternatives.** (a) The original two-gap scope, with the rest as
separate future plans — smaller document, each plan one review surface;
(b) leading with cgroup limits or the task-dir mount instead of signals as
"the other core feature."

**The trade-off.** One plan now spans three mechanical seams (runtime
subcommands, bundle/cgroup generation plus a worker-side allocator, and mount
plumbing), so its phases will land as several PRs against one design doc, and
a stall in one workstream makes the whole plan read as stalled. The phase
independence requirement does the containment.

**Why.** The gaps share one user-facing story — the container runtime's
capability table, and macOS-dev/Linux-prod parity — and splitting that story
across documents would scatter the argument for how the pieces compose (file
delivery is only fully valuable *with* the signal seam; pinning is only
honest *with* the share-slice weighting). Signals still lead the sequencing
because they are the only gap that was ❌ on every platform and the only one
actively data-losing (Linux stop was an unconditional SIGKILL), and because
they ride the same mechanism exec revives. Simplicity of the story won over
smallness of the document.

## 2. The task dir mounts at one canonical path, and container file destinations must resolve under it

**The call.** Every container task gets its host task directory bind-mounted
read-write at `/legion/task` (a fixed constant), worker env is translated to
in-container values (`LEGION_TASK_DIR=/legion/task`, `LEGION_ARTIFACT_*`
prefix-swapped), and file-delivery destinations (`kv_injections.files[].path`,
identity `file.path`) for container tasks are admission-rejected unless they
resolve under `/legion/task`.

**The alternatives.** (a) Mount the task dir at its *host* path inside the
container, so existing absolute paths and untranslated env stay valid;
(b) honor arbitrary destinations (like the identity examples'
`/var/run/legion/...`) via per-file or per-parent-directory bind mounts;
(c) Nomad's three-mount split (`/alloc`, `/local`, `/secrets`) from day one.

**The trade-off.** A jobspec using file delivery is no longer verbatim-portable
between the `exec` and `container` runtimes (exec keeps host-absolute paths),
and jobs that want a token at an FHS-conventional path like `/var/run/...`
must symlink from their entrypoint or use env delivery. `/legion/task` is
also effectively permanent once jobs hardcode it. Evidence to revisit: real
jobs repeatedly fighting the path rule.

**Why.** Simplicity and honesty of the contract. Mirroring host paths (a)
leaks worker filesystem layout into every container and turns an operational
detail (where a worker keeps its allocs) into user-visible API. Per-file
mounts (b) silently break refresh — atomic-rename replacement changes the
inode and the container keeps the stale file (the known docker pitfall);
directory-level mounting is what makes KV re-render and identity rotation
work with zero projection machinery (the K8s-style symlink apparatus is the
cost we're refusing). The three-way split (c) triples the mount surface for a
system that today has one task dir — `/legion/task` leaves the namespace room
(`/legion/secrets`) if that ever changes. The env translation follows the
exact precedent volumes already set (in-task destinations for isolating
runtimes), so this extends an existing convention rather than inventing one.

## 3. Resource enforcement is a worker-core subsystem behind a three-mode plugin interface, not container-runtime code

**The call.** CPU/memory enforcement lives in worker core — CPU
fingerprinting, the core allocator, both slice masks, persistence, restart
recovery, release — and task runtimes consume it through one defaulted trait
method, `resource_enforcement() -> Unsupported | WorkerCgroup |
RuntimeApplied`, plus a worker-computed `ResourceAssignment` hung on the
instance. `WorkerCgroup` runtimes (the `exec` runtime, folded in by this
plan) get their cgroup created and sized by the worker and adopt with two
lines: return the mode, spawn through `join_assignment_cgroup`.
`RuntimeApplied` runtimes (the `container` runtime; `sandbox` next) receive
the assignment and apply it their own way.

**The alternatives.** (a) Keep enforcement inside the container runtime, and
have the exec runtime grow its own parallel implementation when its turn
comes — smallest diff now, and the two runtimes' mechanisms genuinely differ
(OCI spec field vs. `pre_exec` cgroup join); (b) one mode, not three: worker
core creates and populates the cgroup for *every* governed runtime, and the
container runtime simply points `cgroupsPath` at it — fewer concepts, but it
forces every runtime into the cgroup-shaped hole and leaves a microVM
runtime (which pins vCPU threads, and whose "memory limit" is guest size) no
honest way to participate; (c) a full `ResourcePlugin` ABI in the style of
the volume plugin system — maximum extensibility, and vastly more surface
than two consumers justify.

**The trade-off.** Three modes is one more concept than the minimum, and the
boundary between `WorkerCgroup` and `RuntimeApplied` is a judgement each
plugin author has to make (the docs answer it with a single question: does
your task run as a host process the worker can spawn into a cgroup?). The
interface is also being designed against two consumers, so the sandbox
adoption may still force a refinement — deliberately left as plan Q4 rather
than pre-solved with a speculative fourth mode.

**Why.** Extensibility, and correctness that only a shared subsystem can
deliver. Alternative (a) is the one that actually fails: exclusivity is a
*node-wide* property, so two runtimes each keeping their own core accounting
cannot promise it — a core "reserved" by a container and handed out again by
the exec runtime is worse than no pinning at all, because the jobspec now
lies. Once accounting must be shared, the only question is what the runtimes
receive, and the three modes are the smallest honest answer: `Unsupported`
keeps unported plugins working exactly as today (the default, so the dylib
ABI stays source-compatible), `WorkerCgroup` makes adoption nearly free for
the common host-process case, and `RuntimeApplied` keeps the worker's hands
off cgroup files a runtime already owns. This mirrors the split Legion
already made for volumes — core resolves and accounts, the runtime consumes
`MountedVolume` its own way — so it adds a familiar seam rather than a new
paradigm.

## 4. CPU enforcement is Nomad's model: `cores` → exclusive cpuset via share/reserve slices, `cpu` (MHz) → `cpu.weight`

**The call.** On Linux the worker partitions usable CPUs into a `legion/share`
and a `legion/reserve` parent cgroup. `resources.cores` means whole CPUs
assigned by a worker-side core allocator and removed from the share slice's
cpuset (exclusive among Legion's container workloads); `resources.cpu` (MHz)
becomes proportional `cpu.weight` (via OCI `cpu.shares` = MHz, the standard
shares→weight conversion) for share-slice tasks. Two validation rules ride
along: container `cores` must be whole numbers, and a container task declares
`cores` *or* `cpu`, never both. This **reverses the plan's earlier call**
(CFS bandwidth for `cores`, MHz staying scheduling-only), at the product
owner's direction.

**The alternatives.** (a) The superseded design: `cores` as docker-style CFS
quota (`quota = cores × period`), no pinning, no weight — one spec-generation
change, no worker subsystem, fractional cores supported, MHz left as
scheduling arithmetic; (b) pinning without exclusion — write `cpu.cpus` on
the pinned container only, skip the share-slice maintenance — cheaper, but
"exclusive" would be a polite fiction; (c) weight *and* quota together
(K8s-ish requests/limits) — rejected with the same one-`resources`-block
argument as before.

**The trade-off.** The worker grows a real subsystem: CPU fingerprinting,
an assignment map persisted in worker state and rebuilt on restart, two
slice masks rewritten on every pinned start/stop, and an exhaustion failure
path (entry 3 is the interface that keeps it from also becoming per-runtime
duplication). Fractional `cores` and `cores`+`cpu` specs that were
previously accepted now fail validation. Exclusivity holds across governed
runtimes but not against processes outside Legion entirely — node daemons
and SSH sessions can still land on reserved cores, which no node-level
mechanism short of full-machine partitioning would fix. And `cores` now
means something different on Linux (exclusivity) than on macOS (VM sizing) —
the asymmetry the CFS design avoided.

**Why.** Performance, and making the jobspec mean what it says. Bandwidth
throttling admits the exact tail-latency jitter that people declare `cores`
to escape, and leaving MHz unenforced meant a saturated node degraded the
4000-MHz service and the 100-MHz cron job equally — the scheduler's
arithmetic and the kernel's behavior told different stories. Nomad's model is
the proven shape of "declared CPU is real CPU," and the hierarchical-cpuset
trick (one write to the share slice re-fences every tenant) keeps the
implementation a fraction of what it looks like. The simplicity cost is real
and was consciously paid — recorded here because the earlier entry argued the
other side.

## 5. `stopSignal` / `stopGracePeriod` ship now as task-level jobspec fields, defaults SIGTERM / 10 s, capped at admission

**The call.** Graceful teardown is declarable per task: `stopSignal`
(validated against the existing signal vocabulary; on the Linux container
path the default chain is jobspec → image `StopSignal` → SIGTERM) and
`stopGracePeriod` (duration string, default `10s`, admission-capped — lean
5 m). Honored by the container runtime on both platforms and by the exec
runtime; inert on runtimes with nothing to signal. This **reverses the
plan's earlier call** (a fixed SIGTERM/10 s constant, knobs deferred), at the
product owner's direction.

**The alternatives.** (a) The superseded design: constants only, fields
deferred until demanded — no new surface, no cap to pick, but a database
needing a 90-second checkpoint simply couldn't have it; (b) Nomad's exact
names and shape (`kill_signal`/`kill_timeout` + a per-client
`max_kill_timeout` config) — familiar, but "kill signal" describes the
mechanism, "stop" describes the intent, and a per-client cap is a knob per
node where one admission rule does; (c) runtime-`config`-level fields —
would make the semantics container-specific when they are not.

**The trade-off.** Two more fields in the universal task schema that every
current and future runtime must define semantics for (even if "inert"), a
cap value that is now API surface, and `maximal.jsonnet`/docs/validation
carrying them forever. Defaults preserve existing behavior, so the cost is
surface, not migration.

**Why.** Real workloads genuinely need non-default drains (the checkpoint
case), and the fields land at task level next to `resources` because the
*intent* — "give my process this signal and this long" — is
runtime-independent, extensibility's cheapest win. Keeping docker's
SIGTERM/10 s defaults means a job that declares nothing behaves exactly as
the fixed-constant design would have; the knobs only appear when reached
for. The admission cap keeps the operator promise the constant used to make:
no task can hold a drain hostage.

## 6. Signals become a defaulted `TaskPlugin::signal()` trait method, not a container special case

**The call.** `WorkerTaskManager::signal_allocation` routes through a new
trait method whose default is today's `kill(2)`-the-tracked-PID behavior;
the container plugin overrides it with `container kill` / `<runtime> kill`.

**The alternative.** Branch inside the task manager: `if runtime ==
"container" { shell out } else { kill(pid) }`. Smaller diff, no ABI change, no
dylib-rebuild consequence.

**The trade-off.** A trait addition touches the dylib ABI (source-compatible
via the default, but third-party plugins rebuild under the registry's version
gate), and every future plugin author now sees a method they may not need.

**Why.** Extensibility — the exact shape `supports_signal()` was already
promising: the boolean is a trait capability, so the delivery mechanism
belongs on the trait too, not in worker core. The sandbox runtime's future
guest-agent signal path implements the same method instead of growing a second
special case, and worker core stays runtime-agnostic (the same argument that
put `exec()` on the trait in the exec plan). Simplicity of the *system* beat
smallness of the *diff*.

## 7. OCI exec support is reported true without probing the runtime binary

**The call.** `ExecSupport{exec: true, pty: true}` for the Linux OCI path is
asserted from runtime detection alone; a hypothetical OCI runtime lacking
`exec` fails at session open with the runtime's own error, not at
`exec_support()` time.

**The alternative.** Probe once per process (e.g. `<runtime> exec --help`)
and cache, keeping the "fail fast and honestly" property of the exec plan's S4
principle for exotic runtimes.

**The trade-off.** Against a runtime without `exec`, the CLI's clean
"runtime does not support exec" error degrades to a session that opens and
immediately surfaces the binary's usage error.

**Why.** Performance and simplicity over a hypothetical: youki, runc, and
crun — the three runtimes Legion documents — all implement `exec`, the
failure remains clearly attributed when it happens, and a startup probe adds a
subprocess launch on every worker for a case no supported configuration hits.
Kept as open question Q1 in the plan so implementation can flip if the e2e
matrix says otherwise.

---

## Execution-stage calls (2026-07-31)

The planning entries above are ranked 1–7. The entries below were made while
executing all seven phases; they are ranked among themselves, highest project
impact first, and each records a deviation from the plan as written.

## 8. Enforcement reuses the existing opt-in `resourceEnforcement` worker config as its gate

**The call.** The share/reserve hierarchy, the core allocator, memory limits,
and CPU weight are all active only on a worker started with
`resourceEnforcement: { enabled: true }` — the config knob the exec runtime's
cgroup support already shipped with. The plan's §10 said "no feature gate" and
release-noted declared limits becoming real for every Linux worker on upgrade.

**The alternatives.** (a) The plan as written: ungated, so every Linux worker
starts OOM-killing over-consuming tasks the day it upgrades; (b) a *new*
gate specific to container enforcement, leaving exec on its old one; (c) gate
only the pieces that can kill a workload (memory) and leave pinning and weight
ungated.

**The trade-off.** The plan's headline — "no `resources` field is
scheduling-only on Linux anymore" — is true only for operators who opt in, and
the capability table has to say "with `resourceEnforcement.enabled`" rather
than an unqualified ✅. Clusters that never flip the flag keep the exact gap
the plan set out to close, and nothing surfaces that to them beyond the docs.

**Why.** Simplicity of the *rule*, and honesty about an installed base. The
repository had already made this exact call once, in the config comment the
knob ships with: "a declaration that was only ever a scheduling estimate
should not start OOM-killing workloads because the cluster upgraded." Shipping
container enforcement ungated while exec's stayed gated would have left one
node behaving two ways depending on which runtime a task used — the incoherence
entry 3 exists to prevent. Alternative (c) splits the mental model along a line
users cannot see (which limits are "dangerous"). One gate, one hierarchy, one
sentence in the docs.

## 9. `TaskAllocation` gains two fields; the plan's "no wire changes" was wrong

**The call.** `stopSignal` and `stopGracePeriod` ride the dispatch payload as
`task_stop_signal` (string) and `task_stop_grace_period_seconds` (uint64), both
additive and both empty/0 when the task declares nothing.

**The alternative.** The plan's §7 claim — that the fields "ride as JSON inside
the existing job-config payload" and need no proto change.

**The trade-off.** Two more fields on a message that is already large, and a
third representation of a duration (the jobspec's string, the struct's
`Duration`, the wire's seconds).

**Why.** The claim was simply false, and finding out at the worker would have
been worse than finding out now: the worker never receives a jobspec. It
receives a `TaskAllocation` whose `task_config` is the *runtime* config block
(`image`, `command`, …), and rebuilds a `TaskConfig` from individual scalar
and JSON fields. Task-level fields that are not on the message do not exist on
the worker. Encoding the grace period as seconds rather than the raw duration
string keeps the wire self-describing and the worker parse-free; "undeclared"
stays distinguishable from "declared the default", which the Linux container
path needs in order to consult the image's `STOPSIGNAL` only when the jobspec
was silent.

## 10. `resources.cpus` stays an alias for `cpu` (MHz), not for `cores`

**The call.** The core allocator reads `cores` only. `cpus` continues to mean
what the scheduler has always made it mean — an alias for `cpu` — and so
contributes to `cpu.weight`, never to a cpuset.

**The alternative.** Treat `cpus` as the historical spelling of `cores`, which
is what the exec runtime's pre-existing `limit_files` did (mapping either onto
a CFS quota).

**The trade-off.** Two spellings that look like they mean cores (`cores`,
`cpus`) mean different things, which is a real papercut for someone reading a
jobspec cold. The container runtime's own `--cpus` flag on macOS adds to the
confusion.

**Why.** Correctness, discovered by the shipped example configs failing
validation: `ResourcePolicy::normalize_task_resources` rewrites `cpus` to
`cpu`, and `configs/jobs/*.jsonnet` are full of `cpus: 0.1` / `cpus: 1.5`.
Reading `cpus` as `cores` would have turned every one of those into a
"fractional cores" rejection at submit — breaking specs that predate this work
and mean nothing of the kind. The scheduler is the authority on what a
resource field means; the enforcement layer follows it rather than inventing a
second interpretation.

## 11. The `cores`-xor-`cpu` rule is stated twice, deliberately

**The call.** `validate_governed_resources` rejects `cores` + `cpu` on a
governed runtime with a message about the two CPU contracts, even though
`ResourcePolicy::normalize_task_resources` already rejects the same
combination cluster-wide.

**The alternative.** Rely on the scheduler's existing rule and skip the
runtime-level one.

**The trade-off.** Two error paths for one bad spec, and whichever runs first
wins — so the message a user sees depends on validation order rather than on
which rule is more specific.

**Why.** The two rules justify themselves differently and could diverge. The
scheduler's exists because it cannot price a task that declares CPU twice; the
runtime's exists because a cpuset and a weight are different kernel
mechanisms, and its message says so with the fix ("declare `cores` for
latency-sensitive work, or `cpu` for everything else"). Keeping the
runtime-level rule also means a future runtime that opts into enforcement
inherits the check from the interface rather than depending on a scheduler
invariant it never reads. Cheap redundancy on a rule that must not silently
lapse.

## 12. `stopGracePeriod: '0s'` is rejected rather than meaning "kill immediately"

**The call.** A zero grace period fails at submit ("must be greater than 0"),
pointing at omitting the field for the 10s default.

**The alternative.** Accept `0s` as the escape hatch for "I want today's
instant SIGKILL back", which the implementation supports internally — the
graceful reaper treats a zero window as the degenerate case.

**The trade-off.** An operator who genuinely wants instant teardown for a
specific task has no way to declare it, and must live with the smallest
positive duration instead.

**Why.** Simplicity, and the failure mode of the alternative. `0s` reads as a
typo far more often than as intent — it is what a template renders when a
variable is unset — and silently disabling a data-loss protection on a typo is
exactly the outcome `stopGracePeriod` exists to prevent. The internal
degenerate case stays, because it is what makes the ladder's code path
uniform; it is simply not reachable from a jobspec.

## 13. `SIGKILL` is not in the declarable signal vocabulary

**The call.** `crate::signals` accepts eight signals (`SIGTERM`, `SIGINT`,
`SIGQUIT`, `SIGHUP`, `SIGUSR1`, `SIGUSR2`, `SIGCONT`, `SIGWINCH`) — the set
that already existed for `changeSignal` — and `stopSignal: 'SIGKILL'` is
rejected at submit with the vocabulary in the message.

**The alternative.** Allow it, since `stopSignal` reaches a `kill(2)` that
would accept it.

**The trade-off.** An operator who wants a hard kill as the *first* step
cannot say so.

**Why.** `SIGKILL` cannot be caught, so declaring it as the stop signal
disables the drain and makes `stopGracePeriod` meaningless — a spec that reads
like it configures graceful shutdown and does the opposite. The hard kill is
already the ladder's second step and needs no declaration. Reusing one
vocabulary across `stopSignal` and `changeSignal` also means one place to
extend and one error message shape.

## 14. Signal *and* graceful-stop policy both flow through `StopContext`

**The call.** `StopContext` grew `signal` and `grace_period` alongside its
existing `cause`, resolved once by the worker from the task config, and
`stop_with` is the form both in-tree runtimes override.

**The alternative.** Let each runtime read `task.stop_signal` /
`task.stop_grace_period` itself (the `TaskConfig` is already a parameter to
every stop hook), leaving `StopContext` carrying only the cause.

**The trade-off.** The struct now carries three things a plugin author has to
understand instead of one, and a runtime that only cares about the cause reads
past two fields it ignores.

**Why.** Extensibility, and the resolution rules being non-obvious. "Which
signal, and how long" involves a default chain (jobspec → runtime-specific
middle layer → `SIGTERM`) and a cap; having each runtime re-derive it would
guarantee they drift, and a third-party plugin would almost certainly get it
wrong or skip it. Resolving once in worker core and handing runtimes the
answer is the same shape as `ResourceAssignment` (entry 3): core decides,
runtimes apply. The plan's own `StopContext` comment anticipated exactly this
("Phase 1b extends the context with a signal and a deadline"), so this is the
predicted extension rather than a new idea.

## 15. `join_assignment_cgroup` ships as the documented adoption path, but the exec runtime does not call it

**The call.** The shared helper exists, is exported, is documented as the
two-line `WorkerCgroup` recipe, and is unit-tested for its no-op case — while
the `exec` runtime instead reads `instance.resource_assignment` and places the
cgroup write inside its own existing `pre_exec` sequence.

**The alternative.** Have `exec` call the helper too, so the plan's "adopting
is two lines" claim is demonstrated by its first adopter.

**The trade-off.** The recipe the docs teach is not the one the flagship
adopter uses, which is a smell a careful reader will notice — and the helper's
own correctness rests on a unit test plus the container-side e2e rather than
on being exercised by an in-tree runtime.

**Why.** Ordering, which the helper cannot express. The exec runtime's
`pre_exec` is a privilege ladder: `oom_score_adj`, then the cgroup join, then
private `/tmp` and capability drops, then `setuid`, then `no_new_privs` and
landlock. The cgroup the worker creates is not writable by the task user, so
the join *must* precede the user switch; a second, independent `pre_exec`
closure from the helper would run in registration order with no way to
interleave. Forcing the helper to fit would have meant giving it an ordering
parameter — more interface surface than the two-line case deserves. The docs
state the escape hatch explicitly rather than hiding it.

## 16. Worker-core enforcement replaces the exec runtime's own cgroup path

**The call.** `prepare_managed_cgroup` / `limit_files` (the exec runtime's
per-allocation `legion/alloc-<id>-<task>` cgroup with a CFS `cpu.max` quota)
are no longer called; exec tasks get their cgroup from the shared hierarchy.
`remove_managed_cgroup` stays, and only runs for tasks that have no assignment
— cleaning up cgroups a previous worker version created.

**The alternatives.** (a) Keep both, with the old path as a fallback when the
new one declines to assign; (b) delete the legacy functions outright,
including the teardown.

**The trade-off.** The exec runtime's cgroup path *moves*: an operator who
scripted against `/sys/fs/cgroup/legion/alloc-<id>-<task>/` now finds
`/sys/fs/cgroup/legion/share/legion-<id>/`. `cpu.max` quota semantics are
replaced by `cpu.weight` proportional semantics, so a task that was throttled
to 1.5 CPUs is now weighted instead — different behavior under contention,
same declaration.

**Why.** Two subsystems writing cgroups for the same task with contradictory
CPU models is exactly the incoherence this plan exists to remove — and the old
model conflated `cpus` with `cores` (entry 10), so it was also wrong. Keeping
a fallback (a) would preserve the contradiction and double the states to
reason about. Keeping the *removal* function (rejecting (b)) is what stops a
worker upgrade from orphaning cgroup directories a previous version created.
The path change is release-noted.

## 8b. Stopping became two-phase so the grace wait never blocks a heartbeat

*(Ranked with entry 8 — the two together are the execution-stage calls with
real operational consequences. Numbered 8b rather than renumbering the entries
below, which are cited by number in the plan's status header.)*

**The call.** `reconcile_allocations` no longer tears an allocation down. It
signals it, records a deadline, and returns; the task supervisor's tick reaps
whatever has exited or expired. A draining allocation stays in the worker's
maps, is excluded from the restart policy, and an in-place re-dispatch drains
its predecessor on the launch thread with the lock released between polls.

**The alternatives.** (a) Wait synchronously inside `stop_with`, as the first
implementation did — the smallest diff, and the shape the plan implies;
(b) cap the synchronous wait at something small (say 2s) and hard-kill the
rest, keeping one phase; (c) spawn a thread per stop that owns the instance,
so the wait happens off-lock without a state machine.

**The trade-off.** Teardown is no longer atomic with the reconcile that
requested it, so there is a window in which an allocation is signaled but
still present — which the heartbeat reports, and which every test that
asserted "gone after reconcile" had to learn about. The worker also grows a
`terminating` map and a second entry point (`finish_ready_terminations`) that a
reader has to find before the stop path makes sense.

**Why.** Alternative (a) is a genuine regression, and the repository had
already written down why: `internal_docs/TODO.md`'s preemption Phase 1b entry
says a blocking grace wait "would stall heartbeats and the self-fence watchdog
and get the node declared lost", and names exactly the fix taken here
("`stop_with` must signal and return, with a reaper doing the SIGKILL"). With
a 5-minute admission cap, one stubborn task could have frozen a worker's
heartbeats for five minutes and self-fenced its own stateful neighbors —
strictly worse than the unconditional SIGKILL the plan set out to replace.
Alternative (b) makes `stopGracePeriod` quietly mean something other than what
it says, which is the failure the field exists to eliminate. Alternative (c)
does work, but hands a live `TaskInstance` to a detached thread while worker
core still believes it owns the allocation, and gives release-ordering
(volumes, cores, dirs) no single owner. Draining through the supervisor keeps
one owner and reuses a loop that already exists — and it is the same shape the
preemption plan's eviction path will need, so that work inherits it rather
than building a third mechanism.
