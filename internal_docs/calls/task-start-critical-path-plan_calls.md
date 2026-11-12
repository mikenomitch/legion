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

## 2. Teardown leaves the lock for the supervisor, and only where the daemon turns it on

*Follow-up, 2026-11-12 (architecture audit W3).*

**Call.** A stop's notice and signal, and every destroy (a reap, a stop
the runtime could not signal, a failure, a self-fence), are queued as a
`Reap` under the lock and run by the task supervisor with the lock
released, woken at once by a `Notify`. The existing `reaping` set is the
"tearing down" state: its ids count as stopping (starts wait behind them)
and every start path refuses them (`BeginOutcome::InFlight`) until
`finish_reap` releases their volumes, cores, and directories. Only a stop
that removes its directories is deferred: an in-place re-dispatch
(`remove_dirs: false`) relaunches the same id in the same call, so it tears
down inline. The worker daemon turns this on
(`enable_off_lock_teardown`); a task manager without a supervisor (tests,
tools) tears down inline, as before. Restarts, a memory capture, and a
`stage`-gated claim's held start take the same shape as a launch: the
instance moves out to the thread, a placeholder stays registered as
launching (keeping its pid for a capture, so the state file still lists the
live VMM), and a stop that lands meanwhile cancels instead of racing.

**Alternatives.** (a) An `async` stop path end to end. (b) Defer every
teardown, in-place re-dispatches included, with a generation check in
`finish_teardown`. (c) Defer by default, tests included.

**Trade-off.** Two ways to tear down, chosen by a flag the daemon sets, and
a reaping id holds back the node's starts until the supervisor gets to it
(it is woken at once, but a slow destroy now delays starts it would have
delayed under the lock anyway). A failure recorded while its allocation is
being torn down is dropped rather than reported twice. A failure deferred
to the supervisor leaves the process in the state file until the reap
finishes, so a worker that dies mid-reap re-adopts it and stops it again.

**Why (performance, then simplicity).** `destroy` shells out (`runc
delete`, `container stop`, a VMM kill and its root disk) and the lock gates
every start, stop, and heartbeat on the node. (a) rewrites the stop
machinery for the same effect. (b) needs a generation on every release
path for a case (a re-dispatch over a live id) that is rare and already
logged as a forced teardown. (c) would make the hundreds of task-manager
tests that assert state right after a stop run a supervisor each.

## 3. Start reports ride the work stream as hints, and only on a flip

*Follow-up, 2026-11-12 (plan phase 4). Supersedes the deferral recorded
here before.*

**Call.** `StreamWorkRequest` gains a `report` frame
(`StreamWorkReport { allocation_health }`). The worker sends a manager
allocation's health when its `healthy` flag flips (or a new start is
healthy at once), and the manager writes it as it writes a heartbeat's
health. Reports queued while no stream is up are dropped when it reopens.
The heartbeat keeps carrying every allocation's health.

```text
StreamWorkRequest { report: { allocation_health: [{ allocation_id: 4312, healthy: true, status: "healthy" }] } }
```

**Alternatives.** (a) Wake the heartbeat on every transition. (b) A new
unary `ReportHealth` RPC. (c) Report every status change, `starting` and
check-detail text included.

**Trade-off.** A heartbeat whose snapshot was taken just before a flip can
land after the flip's report and set the older health back until the next
beat. Unratified edge and session starts are not reported: the manager has
no allocation for them.

**Why (performance at scale).** (a) makes a churning fleet's heartbeat rate
follow its placement rate, which is what the adaptive cadence exists to
bound. (b) costs a request and an authentication per report where the
stream is already open and authenticated for the node. (c) sends a frame
per start for a state (`starting`) nothing waits on. A flip is what
rollouts and discovery wait for.

## 4. The state file is one newest-snapshot writer, not a record per allocation

*Follow-up, 2026-11-12.*

**Call.** The task manager builds the whole snapshot under its lock, as
before, and hands it to one writer thread per state file
(`WorkerStateStore::submit`), which writes and fsyncs the newest snapshot
it holds and drops older ones. Process start times come from a
per-allocation cache, read from `/proc` once per process. Every store for a
path in the process shares the writer, and a load waits for it. A
gracefully stopping worker waits up to 5 seconds for the last write.

**Alternatives.** (a) A record per allocation (a directory of files, or a
SQLite table like the session log), so a start writes one row. (b) Keep the
synchronous write and only cache the `/proc` reads.

**Trade-off.** The snapshot is still O(allocations) of clones under the
lock (3.8 ms at 1,000 allocations, 28 ms at 5,000, on this box), and a
worker that dies between a change and its write loses that change, a window
up to one write longer than before.

**Why (performance, then simplicity).** The serialization, the write, and
the fsync were 70 to 85% of the lock's share (`persist_cost_probe`: 24.5 ms
before at 1,000, 117 ms at 5,000). (a) changes the on-disk format and the
restore path, which reads one file record by record so an unreadable record
does not hide the rest; it is the next step if the clone ever matters. (b)
leaves the fsync under the lock.

## 5. A streamed placement's policy assignment rides its delta; its body does not

*Follow-up, 2026-11-12 (plan phase 5). Supersedes the deferral recorded
here before.*

**Call.** `StreamWorkReply.net_policies` carries each upsert's policy hash
from the same memoized merge the heartbeat uses (moved to a cloneable
`NetPolicyAssigner` the stream task holds). The launch applies it without
clearing other assignments and binds the allocation's ingress listeners. A
body the node has not cached still comes from `FetchDatapathMaterial`, on a
heartbeat the launch wakes. The heartbeat's `net_policies` replaces it all
on the next beat.

**Alternatives.** (a) Carry the policy body on the delta. (b) Bind ingress
at launch without an assignment, as the egress listeners are. (c) Wake the
heartbeat for every gated push, as before.

**Trade-off.** A node's first allocation of a policy still waits a material
round trip for the body, and a heartbeat built just before the delta can
leave the new allocation `policy-pending` for one beat.

**Why (simplicity, then performance).** Bodies are shared by content hash
across a node's allocations, so after the first the body is already there;
(a) would duplicate it on every delta and add a second body path beside
the material fetch. (b) would open ports that judge nothing until the
assignment lands. (c) adds a heartbeat per gated start.

## 6. A held `stage` start that fails at ratification fails the ratified allocation

*Follow-up, 2026-11-12.*

**Call.** With off-lock starts on (the worker daemon), ratifying a
`stage`-gated claim ratifies it and hands its held `start` to run with the
lock released (`Ratified.start`). A `start` that then fails is recorded as
the ratified allocation's failure, which the manager reschedules, instead
of killing the claim before it is ratified.

**Alternatives.** Run the start before ratifying, and ratify only after it
succeeds.

**Trade-off.** The cluster briefly believes a claim is running that then
fails: one failed allocation in the event stream instead of a claim killed
locally.

**Why (simplicity).** Ratification is the manager's verdict and already
committed; reporting the failure of committed work is the normal path, and
it is what `apply_verdict` says the kill was standing in for ("a
half-started one that never reports would be worse than a reschedule").
Starting before ratifying would hold the claim's verdict behind a runtime
call.

## 7. `exec` checks are not probed

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

## 8. Stage timings ride the task instance, and the worker logs one line per start

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

## 9. TAP devices are driven with raw ioctls and rtnetlink, not a netlink crate

*Follow-up, 2026-11-12.*

**Call.** `tap_link` makes the calls `ip` makes: `TUNSETIFF` and
`TUNSETPERSIST` to create, `SIOCSIFFLAGS` for up and down, and one
`RTM_NEWLINK` (`IFLA_MASTER`) or `RTM_DELLINK` over a fresh rtnetlink
socket. Every error names the `ip` command that does the same by hand:

```text
`ip link set dev lgf3a9 master br0` failed: No such device (os error 19)
```

**Alternatives.** (a) The `rtnetlink` / `netlink-packet-route` crates. (b)
Keep spawning `ip`.

**Trade-off.** About 150 lines of hand-built netlink messages to keep
right, tested by parsing replies and, where the test runs with
`CAP_NET_ADMIN`, against a real TAP.

**Why (performance, then simplicity).** A spawn per call copies the
worker's page tables (the reason VMMs moved to `posix_spawn`), on a path a
gated boot takes twice. Two messages do not justify an async netlink stack
and its dependency tree. The operator-facing text stays what (b) printed.

## 10. The heartbeat keeps the std mutex and takes it with `block_in_place`

*Follow-up, 2026-11-12.*

**Call.** `heartbeat_tick` takes the task-manager lock through
`lock_manager`, which uses `tokio::task::block_in_place` on a
multi-threaded runtime, and runs the reconcile (which stops what the node
no longer runs) inside `block_in_place` too. A current-thread runtime runs
them directly.

**Alternatives.** A `tokio::sync::Mutex` for the task manager.

**Trade-off.** Each acquisition hands the worker thread's other tasks to
another thread first, a small cost per lock taken.

**Why (simplicity).** Most of the lock's holders are blocking threads
(launches, staging, reaps), which an async mutex would make harder, not
easier. What was wrong was a runtime worker parked on the lock; this moves
that wait off the runtime without touching the other holders.

## 11. Gated namespaces move ahead of the datapath sync in the heartbeat

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

## 12. A `boot` check's failures in its first `every` are not counted

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
