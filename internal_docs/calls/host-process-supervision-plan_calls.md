# Judgement Calls: Host Process Supervision

Plan: [../plans/host-process-supervision-plan.md](../plans/host-process-supervision-plan.md).
Entries are ranked by project impact, highest first; an entry's number is its
rank. Covers phases 1–3 (exit semantics, worker-owned cgroups, `exec`
hardening).

## 1. A batch task's `Success` now comes from an exit code, gated per node on the worker's protocol version

`task_status_for_lifecycle` used to stamp `Batch => Success` at *placement*
(`src/storage/store.rs`), so `dependsOn: { condition: "success" }` meant "was
placed". It now stamps `Running`, and only a worker's exit report promotes it to
`Success` or `Failed` — except on nodes whose advertised `protocolVersion` is
below 2, which keep the old optimistic answer.

**Alternatives.** (a) Flip the semantics unconditionally and rely on the
managers-first upgrade order to make the window short. (b) Keep the optimistic
status and only *downgrade* to `Failed` on a bad exit, which fixes the dangerous
half without changing what `Success` means. (c) Have workers advertise a
capability flag in their fingerprint rather than reusing the protocol version.

**Trade-off.** The per-node check costs one `node` row lookup per distinct node
in a status update (memoized within the call), and it means two behaviors coexist
in one cluster mid-upgrade — a state a reader of the code has to keep in mind.
Option (b) would have been simpler still, but leaves the headline bug in place:
"success" that means "we placed it" is exactly what makes a broken migration
unblock its dependents.

**Why.** Correctness of the dependency vocabulary is the whole point of the
phase, so (b) was too small. (a) was rejected because `docs/upgrades.md` mandates
managers first: a new manager will *routinely* place batch tasks on old workers,
and stalled DAGs during every upgrade is a worse regression than the bug being
fixed. (c) duplicates information the handshake already carries — the protocol
version exists to answer "what behavior can I expect from this peer", and
`PROTOCOL_VERSION` 1 → 2 is precisely the "new required behavior an old peer
cannot satisfy" its own comment describes.

## 2. Exits and health checks share one restart budget

The restart history moved out of the health monitor's local `Vec<Instant>` into
`WorkerTaskManager`, keyed by allocation, and both the exit supervisor and the
health monitor spend from it via `can_restart_allocation` /
`record_restart_attempt`.

**Alternative.** Give the exit path its own `restart.attempts`/`restart.period`,
as Nomad's `restart` block and systemd's `StartLimitBurst` both do — arguably
clearer, since "the process keeps dying" and "the process is up but sick" are
different failures.

**Trade-off.** A task cannot tune the two independently: a service that should
tolerate ten crashes but only three failed health checks has to pick one number.
The counter is also per allocation rather than per process generation, so a
restart does not reset it (which is the point, but it surprises people who read
`restart_count` as "consecutive").

**Why.** Simplicity, and honesty about the promise. Two budgets means a flapping
task can burn `restart_count` restarts on the exit path *and* `restart_count`
again on the health path — double what the operator wrote down, at exactly the
moment they care most. One budget keeps `on_unhealthy.restart_count` meaning
what it says: this many local restarts, then move. If per-path tuning is ever
asked for, adding `restart.attempts` later is additive.

## 3. `restart` only decides *whether*; `on_unhealthy` still decides *how many*

The new block is deliberately three fields (`policy`, `delay`,
`successExitCodes`), not a full restart policy.

```jsonnet
restart: { policy: "always", delay: "5s", successExitCodes: [0] }
on_unhealthy: { restart_count: 3, restart_period: "30m", restart_delay: "30s", … }
```

**Alternative.** A complete `restart` block (`attempts`, `interval`, `delay`,
`mode`) mirroring Nomad, with `on_unhealthy` reduced to health-check tuning —
or systemd's fuller matrix (`on-success`, `on-abnormal`, `on-abort`,
`on-watchdog`).

**Trade-off.** Two blocks now govern restarting, and a reader has to know that
the budget lives in the older one. `delay` defaulting to
`on_unhealthy.restart_delay` softens this but does not remove it.

**Why.** Entry 2 forced the budget to live in exactly one place, and moving it
would have been a breaking rename of a shipped field for no user-visible gain.
On systemd's matrix: `on-abnormal`/`on-abort` are `successExitCodes` plus a
signal, and every extra enum value is a thing an operator has to look up at 3am.

## 4. Resource enforcement is opt-in per worker, not on by default

`resourceEnforcement: { enabled: false, cgroupParent: "legion" }` in the worker
config; a task's `resources` stay advisory for `exec` until an operator turns it
on.

**Alternatives.** (a) Default-on wherever cgroup v2 is available — that is what
Kubernetes `limits` and Nomad's `memory` do, and it makes capacity accounting
honest by default. (b) Per-task opt-in (`resources.enforce: true`), so a job
author chooses.

**Trade-off.** The cluster's capacity math stays an honour system until someone
opts in, which is the exact gap the phase set out to close. A mixed fleet is
also possible (some workers enforce, some do not) and nothing in the scheduler
prefers an enforcing node.

**Why.** Every existing jobspec's `resources` was written as a *scheduling
estimate*, because that is all it ever was. Turning those into `memory.max` on
upgrade day would OOM-kill workloads that had been running fine for months —
a self-inflicted incident during the operation operators trust least. (b) puts
the choice on the wrong person: whether a node's limits are enforced is an
operator's decision about their machine, not an app developer's about their job.
Revisit once a release of real fleet evidence exists (plan §11).

## 5. Enforcement failures warn and run unconfined; they never fail the task

No cgroup v2, no permission on `cgroupParent`, an unwritable `memory.max`: each
logs a warning and the task starts anyway.

**Alternative.** Fail the allocation. A task that asked for a memory cap and did
not get one is running under a false promise, and silent under-enforcement is
how capacity plans rot.

**Trade-off.** An operator who enables enforcement on a node with a misconfigured
`cgroup.subtree_control` gets working tasks and a log line, not a loud failure —
so the misconfiguration can persist unnoticed.

**Why.** Enforcement is an operator convenience applied to jobs that never asked
for it; making it a *new way for workloads to fail to start* inverts the risk of
turning it on. The whole reason it is opt-in (entry 4) is to avoid upgrade-day
incidents, and a hard failure would reintroduce them by another door. Explicit
`config.cgroup`, which the *task* asked for, does still fail hard — that is a
promise the job author made to themselves.

## 6. The plugin takes over the `user` switch when hardening needs privilege

`std` applies `Command::uid`/`gid` before pre-exec closures run, so anything
needing privilege in a closure — joining a worker-owned cgroup, unsharing a
mount namespace, dropping bounding-set capabilities — would run *after* the
process had already dropped to the task user and fail. When managed cgroups or
privileged hardening are configured, the plugin skips `cmd.uid/gid` and performs
`setgid`/`setuid` itself, in the middle of its own ordered sequence.

**Alternatives.** (a) Keep `cmd.uid/gid` and do the privileged work in the parent
after spawn — write the child's pid into `cgroup.procs` from the worker. (b)
Require these tasks to run as the worker's user (no `user` field).

**Trade-off.** Legion now owns a privilege drop that used to be `std`'s, in a
`pre_exec` closure where the rules are strict (async-signal-safety, no
allocation-heavy work) and a mistake is a security bug rather than a crash. It is
also two code paths for the same field depending on what else the task declared.

**Why.** (a) leaves a window where the process runs outside its cgroup before the
parent can move it — a task can allocate a lot of memory in a few milliseconds,
and the mount namespace cannot be done from the parent at all. (b) would make
hardening and running as a non-root user mutually exclusive, which is backwards:
dropping to an unprivileged user is *the* thing hardening exists to support.

## 7. Terminal allocations keep their row; only failures delete one

`complete_allocation` sets `allocation.status` to `complete`/`failed` and leaves
the row in place. `build_desired_allocations` skips terminal rows so a worker is
never told to run a finished allocation again.

**Alternative.** Delete the row the way `handle_failed_allocations` does, which
keeps "an allocation row means live work" true and needs no status filter.

**Trade-off.** `allocation.status` is now load-bearing where it was nearly
decorative, and every future query over allocations has to decide whether it
means live ones. Terminal rows also accumulate until the job is stopped or
destroyed.

**Why.** Deleting a completed batch allocation leaves the task's desired count
unmet, and the very next reconcile places it again — an infinite re-run loop for
every batch job in the cluster. The slot a finished batch task occupied is
genuinely still spent; the row is the honest representation of that.

## 8. Runtimes declare `supervises_process()`, and unsupervisable batch tasks self-report at start

The supervisor only polls runtimes that claim to track a host process
(`exec`, `container`, `sandbox`). For a batch task on any other runtime, the
worker reports completion immediately after a successful start, with the message
`runtime \`noop\` reports no exit status; treated as complete at start`.

**Alternatives.** (a) Poll everything and let runtimes that track nothing simply
never report — batch tasks on them would hang forever. (b) Default the capability
to `true`, since the framework's polling is generic. (c) Let such batch tasks
stay `Running` forever and document it.

**Trade-off.** The optimistic answer is still optimistic: a `noop`-style batch
task is "successful" the moment it starts, which is a lie about a thing that
never ran. Third-party plugin authors also have to know to opt in.

**Why.** (a) and (c) turn a third-party plugin into a stalled DAG, which is a
worse failure than an optimistic status and one the job author cannot debug.
(b) makes the dangerous direction the default for code we do not control. Saying
it out loud in the allocation's status message is the compromise: the same
behavior Legion always had, now visible as a statement rather than an assumption.

## 9. `exec` validates its typed config at submit, not just its JSON schema

`ExecTaskPlugin::validate_config` now runs `ExecTaskConfig::from_value` (the full
typed parse plus `validate()`), matching what the sandbox runtime already does.

**Alternative.** Validate only the new `hardening` block at submit and leave the
rest as it was, avoiding any change to what admission accepts.

**Trade-off.** Specs that used to submit and then fail at start — both `oom_adj`
and `oom_score_adj` set, an empty `cgroup` string, an unparseable landlock
block — are now rejected at submit. That is a *stricter* admission gate arriving
with an unrelated feature, and someone's stored-but-broken job could fail to
resubmit.

**Why.** The failures being moved are all cases where the job could never have
run anywhere; catching them in `legion job validate` is what the agent-readiness
rules ask for (self-correct before mutating the cluster). Validating only the new
block would have left `hardening` as the one field in `exec` with submit-time
checking, which is a worse thing to explain than a consistent rule.

## 10. `privateTmp` gives a mount namespace only — no PID, IPC, or UTS namespaces, no seccomp

**Alternative.** Ship the full namespace set and a seccomp profile, matching what
`ProtectSystem`/`PrivateNetwork`-style systemd hardening or a container runtime
would give.

**Trade-off.** A hardened `exec` task can still see every process on the node
(`/proc`), and a compromised binary can make any syscall its capabilities allow.
That is materially weaker than a container.

**Why.** `unshare(CLONE_NEWPID)` does not move the calling process into the new
namespace — the *next* fork lands there — so a PID namespace needs a double-fork,
which changes the pid the worker tracks and therefore interacts with the exit
supervisor, self-fencing, and `legion exec` all at once. That is its own phase,
not a field on this one. Seccomp needs a profile format, a default policy, and a
compatibility story; the honest v1 is the cheap, high-value subset, with the gap
stated in `docs/task-supervision.md`.
