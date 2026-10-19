# Judgement Calls: Task Start Critical Path

Calls made for [task-start-critical-path-plan.md](../plans/task-start-critical-path-plan.md).
Entries are ranked by project impact, highest first. The number is the rank.

## 1. Startup probes report only passes, and need no knob

**Call.** Until a `tcp` or `http` check first passes after a (re)start, the
worker probes it between its `every` ticks on a 100 ms doubling backoff capped
at 1 s. A passing probe counts as a passing check. A failing probe is dropped.
Failures are counted on the configured cadence only, so every failure knob
(`unhealthy_after_count`, `boot_grace_time`, the timespan) means what it did.
There is no jobspec field to turn it off.

**Alternatives.**
- (a) Kubernetes' shape: a separate `startupProbe` block with its own period
  and failure threshold.
- (b) Shorten the first `every` interval (run the cadence itself faster until
  the first pass), counting those failures too.
- (c) A per-check `startupProbe = false` opt-out.

**Trade-off.** A task that is slow to open its port gets up to about ten extra
connects (or GETs) in its first `every` window. An HTTP health endpoint that
does real work sees them. A probe that is slower than 1 s never passes early,
so such an endpoint keeps the old latency.

**Why (simplicity, then performance).** (a) adds a block job authors must
learn to get a behavior that should be the default, and K8s still waits a full
period for a fast starter. (b) changes failure semantics: with the default
`unhealthy_after_count = 1`, a counted failure past the boot grace restarts
the task, so faster counted checks would restart slow starters that work
today. Dropping failed probes makes probing invisible except for the earlier
pass, which is why it needs no knob (c).

## 2. `exec` checks are not probed

**Call.** Only `tcp` and `http` checks probe at startup. `exec` checks keep
their cadence. Readiness checks are not probed either.

**Alternative.** Probe every kind, with a timeout around the probe.

**Trade-off.** A task whose only check is `exec` still waits a full `every`
after a failed first check.

**Why (simplicity).** An `exec` check runs an arbitrary host command. Its
cost is unknown, and `run_exec_check` has no timeout, so a hung probe would
hold the check's loop and delay the cadence check behind it. A TCP connect or
an HTTP GET with a 1 s timeout is cheap and bounded. A readiness result never
changes health, so probing it gains nothing.

## 3. Stage timings ride the task instance, and the worker logs one line per start

**Call.** `TaskInstance.start_timings` carries the breakdown through every
start path (off-lock launch, locked launch, staging threads, restarts), each
stage is observed in `legion_task_start_stage_seconds{runtime,stage}` as it
ends, and `finish` logs one `Task allocation started` line with the stages.

**Alternatives.**
- (a) Tracing spans per stage only, left to a trace backend.
- (b) Metrics only, no log line.
- (c) Separate histogram families per stage.

**Trade-off.** One more info line per start on a node, and a new field on
`TaskInstance` that every runtime can write to.

**Why (simplicity, then performance).** Most operators run without a trace
backend, so (a) leaves them blind. The metric says which stage is slow on a
node, but not why one start was slow, which (b) cannot answer and the log line
can. One family with a `stage` label (c rejected) keeps the registry small and
lets one query compare stages. The runtime records its own sub-stages
(`network`, `image`, `root_disk`, `vmm_config`, `vmm_spawn`) into the same
breakdown, because only the runtime knows where its `start` spends time.

## 4. Start reports and the streamed datapath stay on the heartbeat for now

**Call.** Health transitions still reach the manager on the next heartbeat,
and a streamed placement's datapath (listeners, network policy, endpoints,
credentials) still arrives on the next beat. Both are deferred (plan phases 4
and 5).

**Alternatives.**
- (a) Wake the heartbeat on every health transition and every streamed
  placement that needs a datapath.
- (b) Bind a streamed placement's listeners from the work-stream mirror before
  launching it.

**Trade-off.** A healthy transition waits up to one heartbeat interval (about
1 s on average at the default 2 s, more on a large fleet's adaptive cadence)
before the manager sees it. A streamed gated placement waits a beat for its
network.

**Why (performance at scale).** (a) adds one full heartbeat per start, so a
churning fleet's heartbeat rate follows its placement rate rather than the
cadence the manager serves to bound it. (b) binds listeners, but the network
policy that lets a gated connection through still arrives on the beat, so it
moves the wait without removing it. Both belong on `StreamWork` as
worker-to-manager reports and policy-carrying deltas.

## 5. Gated namespaces move ahead of the datapath sync in the heartbeat

**Call.** `heartbeat_tick` creates gated namespaces before `apply_upstreams`
binds the listeners inside them, from the reply's whole desired set.

**Alternative.** Keep the order and make each listener sync create its
namespace on demand.

**Trade-off.** An allocation whose identity delivery failed on this beat now
keeps its namespace until it leaves the desired set (one more beat), where
before it was removed on the same beat.

**Why (simplicity).** One place creates namespaces, and it now runs before
everything that needs them. Teaching four listener types to create namespaces
would spread the namespace lifecycle across the datapath.

## 6. A `boot` check's failures in its first `every` are not counted

*Follow-up, 2026-10-19.*

**The call.** A `boot` check's failed result is not counted until one
`every` (or the check's `boot_grace_time`, if longer) has passed since the
task started or restarted. A pass in that window still counts. `health`
checks keep exactly their `boot_grace_time`.

**Alternatives.** Start a boot check's cadence one `every` late (the shape
the TODO suggested); apply `boot_grace_time` to boot checks with its current
default of zero.

**Trade-off.** A task that really is broken from the first instant reports
unhealthy one `every` later than before.

**Why.** Ease. A late cadence would also delay an `exec` boot check's first
pass by one `every` (those are not startup-probed), and a zero default grace
fixes nothing; forgiving only the early failure keeps every early pass.
