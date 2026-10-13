# Judgement calls: launch spot-check fixes

Calls made while fixing the 2026-10-11 pre-launch spot-check findings
([plan](../plans/launch-spot-check-fixes-plan.md),
[findings](../launch-spot-check-findings.md)). The worker-plane half
(P0-1) executes Phase 1 of the worker-plane plan, so its calls live in that
plan's doc: [worker-plane-auth-plan_calls.md](worker-plane-auth-plan_calls.md),
entries 10, 11, 15, and 20 to 25.

**Entries are ranked by project impact, highest first; an entry's number is
its rank.** Each names the workstream it came from. Anything here a user needs
to know (a default, a limit, a rejected shape) is also stated in the public
doc the entry names.

## 1. Capability adds are gated by a per-worker-type allowlist (`container.allowCaps`, Nomad's `allow_caps`) that defaults to the default set; jobs adjust with `security.capabilities`

*Workstream: Linux containers.*

**The call.** A job adjusts its set with `security = { capabilities = { add,
drop } }` (Docker `--cap-add`/`--cap-drop` semantics, validated at submit).
Each worker type carries `container = { allowCaps = [...] }`, which the
manager validates and normalizes at config load and hands to the worker at
registration (`WorkerConfig.container`, a JSON column on `worker_type`, then
`LEGION_CONTAINER_ALLOW_CAPS` for the in-process runtime, like
`resourceEnforcement`). A Linux container whose effective set (default, less
drop, plus add) holds anything outside its node's allowlist fails at start.
The allowlist defaults to exactly the 13-capability default set, so out of the
box drops always work and adds never do. `job run` / `job plan` also reject a
task whose set *no* worker type allows.

```nickel
# configs/workers/privileged.ncl
{ workerType = "privileged", pool = "privileged",
  container = { allowCaps = ["ALL"] } }
```

```text
task tool would run with capabilities this node does not allow container tasks to have: SYS_ADMIN. This node allows: AUDIT_WRITE, CHOWN, ... (the worker type's container.allowCaps). An operator can allow them by adding "SYS_ADMIN" to container.allowCaps in this worker type's config; otherwise take them out of the task (remove them from security.capabilities.add, or list them in security.capabilities.drop)
```

**Alternatives.**
- No node allowlist; gate adds on the ACL `host` grant
  (worker-plane-auth-plan §6.16) later. This was the first version of this
  call.
- Nomad's exec-driver behavior: silently trim the default set to a narrowed
  allowlist instead of failing.
- Make placement capability-aware, so a task that adds a capability only
  places on nodes that allow it.
- A worker-local setting (agent flag or env) instead of the worker-type
  config the manager serves.

**Trade-off.** A task that adds a capability places like any other and fails
at start on a node that refuses it; only the "no worker type allows it" case
is caught at submit, and authors pin such tasks to the right pool by hand. The
list is the whole allowlist, so an operator who wants "defaults plus NET_RAW"
writes all 14 names. An operator who narrows it below the default set makes
every container job there drop capabilities explicitly. Any `job:write` holder
can still use whatever a node allows: the allowlist bounds the node, not the
author (the ACL `host` grant remains the per-author gate, still a TODO).

**Why.** Before this change a job could not add capabilities at all, so
shipping `add` without a gate would have opened a new path to host root for
every `job:write` holder (`add = ["SYS_ADMIN"]` on an image that runs as
root). The `host` grant does not exist yet, and the allowlist closes the hole
now with the boundary in the right place: the node operator decides what a
node can be made to do. Failing instead of trimming (Nomad's Docker driver,
not its exec driver) means a job never runs silently without a capability it
asked for, which is the 3am-debuggable behavior. Following the worker-type
pattern keeps one place an operator configures a class of nodes, and lets the
manager check submissions against every type without asking the nodes.
Capability-aware placement is the better long-run UX but is a scheduler
change (a node attribute per capability or a new filter), so it is a TODO.
Security won over the extra config surface; simplicity kept it to one list
with Nomad's semantics, not a policy language.

## 2. The default capability set is Docker's, less `CAP_NET_RAW`

*Workstream: Linux containers.*

**The call.** A Linux container gets Docker's default capability set (moby
`oci/caps/defaults.go`) without `CAP_NET_RAW`: 13 capabilities instead of the
previous 3. The set goes into bounding, effective, and permitted; inheritable
and ambient stay empty, as Docker does since 20.10.14.

**Alternatives.**
- Docker's 14 exactly, including `NET_RAW` (what the request listed).
- Keep the three and require every jobspec that runs a stock image to add
  `CHOWN`, `SETUID`, `SETGID`, `DAC_OVERRIDE`, `FOWNER`.

**Trade-off.** Images that ping or traceroute at start need
`security.capabilities.add = ["NET_RAW"]`, on a node whose
`container.allowCaps` admits it (entry 1). Container root (still host uid 0,
no user namespace yet) gains `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `FSETID`,
`MKNOD`, `SETFCAP`, `SETPCAP`, `SETUID`, `SETGID`, `SYS_CHROOT` over what it
had. `MKNOD` is harmless behind the device-cgroup deny rule; the rest act only
on what the mount namespace shows it (its rootfs, `/legion/task`, volumes).

**Why.** Stock images are written against Docker's set. nginx's master
`chown`s its cache and `setuid`s its workers; the Postgres entrypoint `chown`s
PGDATA and `su-exec`s. Keeping three capabilities made every official image a
jobspec puzzle, which defeats "simplest scheduler to run". `NET_RAW` is the one
place Legion differs from Docker on purpose: a Docker container has its own
network namespace, and a Legion container shares the node's unless `gated`, so
raw sockets there sniff and spoof the node's interfaces. Nomad's Docker driver
drops `NET_RAW` from its `allow_caps` default for the same reason. Simplicity
won over Docker parity for that one capability; security won over parity
because the network model differs.

Surface:

```nickel
config = {
  image = "alpine:3.20",
  command = ["ping", "-c", "3", "1.1.1.1"],
  security = { capabilities = { add = ["NET_RAW"] } },
},
```

The worker-plane plan (§6.14, "the three capabilities ... stay as they are")
was annotated with a dated update rather than rewritten.

## 3. A follower replays a rejected write to the leader itself; it does not pre-route writes, and the CLI does not chase leaders

*Workstream: Manager availability.*

Any unary Manager RPC that a follower rejects with the typed not-the-leader
error is replayed, verbatim (headers, bearer token, idempotency key, deadline,
body), to the leader named in the rejection, and the leader's reply is
returned. A tower layer (`src/manager/leader_forward.rs`) buffers the request,
runs it locally, and forwards only when the reply is `FAILED_PRECONDITION`
carrying the `x-legion-raft-leader` metadata that `map_store_error` now
attaches. One hop (`x-legion-forwarded-by`), never `ExecAllocation` (the one
client-streaming RPC; a unit test checks the list against the proto), never
the RaftTransport service.

```console
$ LEGION_ADDR=http://follower:4747 legion job run web.ncl     # now just works
# only when the leader is unreachable from the follower:
Error setting node availability: this manager is not the raft leader; retry on its leader mgr-a
(raft advertise address 172.31.90.175:4747) (this manager tried to forward the request to
the leader at 172.31.90.175:4747, its raft advertise address, and failed: ...)
```

- **Alternatives.** (a) Pre-route by method: keep a list of write RPCs and proxy
  them before running anything locally. (b) Make the CLI retry against the hint
  (P2-5's fallback suggestion), and reword the hint. (c) Forward per store op
  (extend `ForwardLaneWrite` to every client write).
- **Trade-off.** Every write on a follower runs its handler up to the first
  rejected proposal, then runs again on the leader: one wasted capture-free
  pass plus a manager-to-manager hop. A handler that performed an external
  side effect (a worker-plane call) before its first store write would repeat
  it; none of the write paths reviewed do, and a client retry against the
  leader has the same property. Under `lanes`, a composite whose first half
  committed on a lane this manager leads is replayed whole, which is the
  documented converging client-retry shape. The forward is plaintext h2c
  between managers, like the raft transport.
- **Why.** Simplicity/ease: one manager address works for every caller —
  CLI, UI, agents (MCP), and workers (`WorkerRegister` on a follower is fixed
  by the same layer, closing the TODO item). (a) needs a write list that drifts
  as RPCs are added and would forward reads that happen to call a writer;
  rejection-driven forwarding needs no list because the store already knows
  exactly when a write cannot run here. (b) still fails from a laptop that
  cannot reach the leader's private advertise address, which was the actual
  complaint. (c) is a closed enum per op — fine for the worker plane's dozen
  writes, unmaintainable for the whole API.

## 4. Every API message serializes camelCase, set once in build.rs

*Workstream: CLI and diagnostics.*

The finding was "instance JSON is snake_case (`current_alloc_id`) while
hand-built envelopes are camelCase (`instanceIds`)". The fix is one line in
`build.rs`: `message_attribute(".", "#[serde(rename_all = \"camelCase\")]")`,
so every prost message (all `--json` output that prints an API reply, and
every MCP tool result) emits camelCase. The few hand-built envelopes that were
snake_case were renamed to match (`instance fork|start|suspend -j`:
`pauseMs`, `checkpointState`, `resumedFrom`, `restoreMs`, `coldBoot`,
`durationMs`; `net handler test -j`: `latencyMs`; MCP exec tools: `exitCode`).

```console
$ legion instance get user-42 -j | jq .data.instance.currentAllocId   # was .current_alloc_id
$ legion job logs web -j | jq '.data[0].removalCause'
```

- **Alternatives.** (a) Make the hand-built envelopes snake_case to match the
  proto (fewer lines touched, but the jobspec and every documented envelope —
  `allocationId`, `latestId`, `readOnlySafe` — are camelCase, so that would
  break more of the published surface); (b) per-struct `rename_all` only on
  the types the finding named (leaves the inconsistency everywhere else);
  (c) convert keys at print time in `output::print_success` (mangles data
  maps: node attributes, env var names, status-count maps).
- **Trade-off.** A large, silent JSON change: every script that reads a
  snake_case key from `-j` or MCP output now gets `null`. MCP tool *arguments*
  stay snake_case (`allocation_id`, `idempotency_key`), so an MCP call's input
  and output use different conventions. gRPC (and the UI over gRPC-Web) is
  unaffected. Any JSON written by serde from a proto type before this change
  (idempotency replies, 24h) no longer parses; no installed base.
- **Why.** One rule ("every key Legion emits is camelCase") is the simplest
  thing an agent can rely on, and matches the jobspec. AGENTS.md says break
  surfaces for a cleaner one. Enums, oneof variants, and map keys are left
  alone, so data is never renamed.

## 5. Dependencies are enforced by the manager holding tasks, not by worker-side gating

*Workstream: Scheduler.*

`dependency_satisfied` (src/manager/scheduler.rs) no longer counts a batch
dependency that is merely *planned in the same pass* as `complete`/`success`.
Under the new `DependencyGate::Live` gate only a status a worker reported
(`task_status`, written by `Store::complete_allocation`) satisfies them. A
dependent task is **held**: not placed, reported on `PlanOutcome.held_tasks`,
and placed by an eval that `Store::queue_dependency_progress` queues when the
dependency's terminal report lands (`handle_terminated_allocations`).

- **Alternative:** gate starts on the worker (place the whole DAG at once, and
  have the worker wait for its dependencies' status before starting a task),
  which the findings also offered.
- **Trade-off:** each DAG step costs one extra eval and plan (one per terminal
  report of a task that has dependents), and a step starts one heartbeat plus
  one eval after its dependency exits instead of immediately. Resources for a
  held step are not reserved, so a later step can find its node full.
- **Why:** simplicity. The worker never sees the jobspec and has no view of
  other nodes' tasks; worker gating would need a cross-node status feed and a
  new "placed but waiting" allocation state that every surface (UI, rollouts,
  health gates, capacity accounting) would have to learn. Holding in the
  planner reuses the existing readiness filter and the existing eval kinds.
  Surface: `eval.completed` reads `Job pipeline submitted with 1 placements;
  waiting on dependencies: validate, serve`.

## 6. A plan places what fits and reports the rest as unplaced

*Workstream: Scheduler.*

`build_plan` no longer fails the whole plan when one replica has no node. For
a task outside a multi-task group, the remaining replicas are recorded as
`UnplacedTask { task_name, count, reason, static_port }`, the rest of the plan
applies, and the eval succeeds with `...; 1 unplaced (Failed to place task web:
static port 18080 is already in use on every node it is allowed on ...)`. A
plan that places nothing still fails, now with a typed `PlacementBlocked`
error.

- **Alternative:** keep all-or-nothing and only fix the static-port
  accounting, so `count = 2` with a static port on one node fails cleanly
  (no hot loop) but places nothing.
- **Trade-off:** a job can now run below its count with only an eval outcome
  and a `warn` log saying so; the remainder waits for the capacity pass
  (a node joining, or freed capacity: entry 21). Multi-task groups
  and `job plan` previews stay all-or-nothing, so there are two behaviors to
  explain. Preemption now also runs for a plan that ends up partial: a
  high-band job that can get one of two replicas by evicting one low-band
  allocation now does so (the `disruption.minAvailable` floor still binds),
  where before the whole plan failed and nothing was evicted.
- **Why:** the finding asked for it, and it is what Nomad and Kubernetes do:
  one replica that cannot fit should not keep the other from running.
  Groups stay all-or-nothing because splitting a co-placed group would start
  a sidecar without its primary; previews stay all-or-nothing so `job plan`
  keeps reporting the first replica that would not fit as an error.

## 7. Workers learn every manager from raft membership on each reply and fail over one manager at a time

*Workstream: Manager availability.*

`WorkerRegisterReply.manager_addresses` and `HeartbeatReply.manager_addresses`
(new proto fields) carry every raft member's `advertiseAddr` with the
answering manager's API scheme. The worker persists the set
(`managers.json` beside its state), always keeps its token's address, talks
to one active manager, and after two consecutive unreachable heartbeats moves
to the next, wrapping around. The switch happens under one tonic balance
`Channel` whose endpoint set always holds exactly the active manager, so every
`ManagerClient` already handed out follows it.

- **Alternatives.** (a) Load-balance across all managers (tonic
  `balance_list`). (b) An operator-supplied manager list (worker flag, or a
  multi-address join token). (c) DNS / a load balancer in front of managers.
- **Trade-off.** The addresses are raft advertise addresses; a deployment
  whose managers peer on a network workers cannot reach gets no failover
  benefit (the worker tries each, then keeps its token's address). A failover
  costs two failed beats plus up to a 5s connect timeout (or 30s request
  timeout for a hung manager), inside the leader's 30s peer-digest grace for
  stopped managers. `HeartbeatReply` grows by a few addresses per beat.
- **Why.** Simplicity: zero new config, and it self-updates as managers join
  or leave. (a) spreads every worker's beats across a dead manager too: tonic
  keeps a failed lazy endpoint in the set and its calls keep failing, so a
  third of heartbeats would fail while one manager is down. (b) is a new knob
  to keep in sync with membership. (c) is a dependency Legion's "one binary"
  story avoids.

## 8. Lifecycle and restart ride the dispatch as two discrete fields, not a serialized TaskConfig

*Workstream: Task lifecycle.*

The worker learns a task's lifecycle and restart policy from two new
`TaskAllocation` fields: `task_lifecycle` (`"service" | "batch" | "sidecar"`,
always set) and `task_restart` (the declared `restart` block as JSON, empty when
undeclared). `task_from_allocation` folds them into the TaskConfig the
supervisor already reads, so the existing supervisor, `decide()` and the
manager's `terminated_allocations` path work unchanged.

**Alternative.** Serialize the whole task config onto the payload (the
spot-check report's second suggestion), so no task-level block can ever again
be silently missing on the worker.

**Trade-off.** Every future task-level block the worker needs still has to be
added by hand (this is the third time: `task_stop`, `task_suspend`, now these).
A whole-config field would have closed the class of bug, at the cost of
shipping manager-only fields (constraints, `dependsOn`, `schedule`, rollout,
affinities) to every node on every heartbeat, duplicating `task_config` /
`task_checks` / ..., and making the worker's view of a task two competing
sources.

**Why.** Simplicity and performance: the payload stays "what the worker acts
on", the per-heartbeat desired set does not grow by the jobspec, and the
convention the message already documents (empty = undeclared, byte-identical
for jobs that do not use a block) still holds. The proto comment on
`task_stop` already warns that a block not on the message does not exist on the
worker; the new comments say the same and name the consequence.

Surface: `proto/legion.proto` `TaskAllocation.task_lifecycle = 26`,
`task_restart = 27`.

## 9. A node-token re-registration keeps the token instead of rotating it

*Workstream: Manager availability.*

`WorkerRegister` authenticated by the node's own node token (no join secret)
returns no `node_token`, meaning "keep the one you presented". A join-token or
management registration still mints and rotates.

- **Alternative.** Keep rotating on every registration and have the worker
  persist each new token.
- **Trade-off.** A leaked node token is not invalidated by the node's next
  restart; revocation is by removing the node (or resetting ACLs). Behavior
  change in `tests/acl_endpoints.rs` (`re-registration rotates the node token`
  for node-token registrations is gone).
- **Why.** Simplicity and robustness: with rotation, a crash between the
  manager's commit and the worker's fsync leaves the worker holding a revoked
  token and (with the join token expired) unable to ever rejoin — exactly the
  P0-8 failure, rarer. Rotation on self-presented credentials gains nothing:
  the caller already proved possession, and an attacker holding the token can
  rotate it too.

## 10. Job status: main's derivation replaces this branch's at merge

*Workstream: Scheduler. Revised when main was merged in.*

This branch first derived `running` / `pending` / `complete` / `failed` from
live allocations and task statuses at read time
(`Store::derived_job_statuses`), leaving the stored `done` untouched. While
the work was in flight, main shipped its own read-time derivation
([job-status-plan.md](../plans/job-status-plan.md), #21): the stored `done`
became `active`, and the derived set adds `scheduled`, `suspended`, and
`idle`. At merge, main's `job_status_ops` won and this branch's derivation
was deleted. What the branch keeps is the `RunJob` change: an unchanged spec
starts a stopped, failed, or idle job again.

```console
$ legion job run nightly.ncl
Job config unchanged; job was not running, starting it again
```

- **Alternative:** keep this branch's derivation and drop main's, or run both
  (one for the API, one for the UI).
- **Trade-off:** the branch's tests and docs that named the stored `done`
  were rewritten to main's `active`. Users see no difference beyond what
  main already shipped: both derivations put the lifecycle flag first, then
  live allocations, then task outcomes.
- **Why:** simplicity. One status vocabulary on every surface, and main's
  covers cron and keyed jobs this branch's did not. Two derivations would
  disagree on exactly the jobs an on-call responder looks at.

## 11. A failed exit carries the task's last stderr lines into its status, failure reason and event

*Workstream: Task lifecycle.*

`TaskExit` gained `stderr_tail`: for an exit the task does not count as success,
the last 3 non-empty stderr lines (at most 400 characters, from the current run
only: the worker records the `stderr.log` length at each launch). It shows up
as `exited with status 1; last stderr: Error: ... 429 Too Many Requests` in the
allocation status, the `allocation.failed` reason and the event. A health check
that fails an allocation whose process is still running appends the same tail
(`health check failed: tcp check failed: Connection refused; last stderr: ...`).
Worker log lines keep `describe()` (status only).

**Alternatives.** (a) Point at the logs: `exited with status 1; see legion job
logs`. (b) Kubernetes' `terminationMessagePolicy: FallbackToLogsOnError`, an
opt-in per container.

**Trade-off.** Task output is copied into events, so a task that prints a secret
to stderr as it dies exposes it to event readers (the same people who can read
its logs). Bounded and only on failure, but not zero. Documented as a
limitation in docs/task-supervision.md.

**Why.** Ease: the spot check's whole complaint was that the cause existed only
in task stderr. Opt-in would leave the default experience unchanged, which is
the failure the finding describes; a pointer still sends every user on the
same hunt. Keeping it out of worker tracing follows the "never trace task
output/secrets" rule.

## 12. Removed allocations: a 24h manager-side history plus worker-side retention

*Workstream: CLI and diagnostics.*

`legion job logs` used to resolve a job to its live allocation rows only.
Now every removal path records the allocation (job, job config, task name,
node, status, stop cause, time) in a new scheduler-lane table
`allocation_history`, written in the same transaction as the DELETE (from the
`record_stop_notices_with_preemptor_tx` choke point every removal already
calls) and expired there after `ALLOCATION_HISTORY_RETENTION_SECS` (24h).
`GetAllocationLogs` serves them: by id, or for a job with no live allocation
(its newest 10 removed ones), or with `include_removed`. The worker keeps a
removed allocation's log directory until it has been idle for
`LEGION_LOG_RETENTION_DAYS` (default 7), then deletes it (checked every 5
minutes; live allocations' dirs never).

```console
$ legion job logs web
== allocation 31 (task web) on w-1 [removed, stop at 2026-10-12T01:49:13Z] ==
$ legion job logs web --include-removed
```

- **Alternatives.** (a) Keep terminal allocation rows in `allocation` with a
  status (Nomad's model, GC later): every scheduler/capacity query would have
  to filter them, a large change to the scheduler track's code; (b) ask every
  worker "which log dirs do you have for job X" (new worker RPC, fan-out to
  the whole fleet per request); (c) `--node` + `--allocation` passthrough with
  no manager memory (the user has to know the node, which is exactly what
  they lost); (d) reuse `allocation_stop_notice` with a longer retention (it
  has no job or task, and its semantics are "deliver to the worker").
- **Trade-off.** One more replicated table and one INSERT per removed
  allocation on the scheduler lane (plus one indexed DELETE for expiry); a
  churny cluster holds a day of rows. Two retentions (manager 24h, worker 7d)
  that a user has to understand: past 24h the files exist but `job logs` can
  no longer find them. Implicit fallback (no live allocations → show removed)
  is a small piece of magic, documented and marked in the header.
- **Why.** Simplicity for the user: the obvious command works for the case
  that matters most (the thing failed and is gone), with no new concept in
  the scheduler. Performance: one insert per removal, no fleet fan-out.

## 13. A finished allocation keeps its row but stops holding capacity

*Workstream: Scheduler.*

A `complete` or `failed` allocation (its process exited) is left out of every
usage sum: the read model's free pools and usage memo, the plan-time ledger
and port view, and the plan-apply transaction's pool, cached usage, and port
check (`resources::capacity_rows`, `status NOT IN ('complete', 'failed')` in
the apply's SQL). Completing an allocation now bumps its node's epoch.

- **Alternative:** delete the row when a batch task finishes (it frees
  everything with no new rule), or zero out its `resources`.
- **Trade-off:** one more rule every capacity reader has to apply (eight
  call sites today), and a completion now fences in-flight plans on that
  node (it bumps the node epoch, so a racing plan conflicts and retries once).
- **Why:** the row is what keeps a finished batch task from running again
  (its slot stays spent, and `job_configs_waiting_on_capacity` and the
  desired-count math read it), so deleting it would reintroduce the re-run
  loop `complete_allocation` was written to prevent. Zeroing `resources`
  would lose what the allocation used from every read surface. Bumping the
  epoch reuses the existing contract ("every capacity-bearing change bumps
  the node") instead of adding a status component to the
  `node_alloc_usage` fingerprint, which would have to read row bodies on
  the apply's hot path. Surface: a 3-step DAG whose steps each need 6 GiB
  runs on one 8 GiB node.

## 14. `complete` means "finished either way"; `success` means exit 0

*Workstream: Scheduler.*

`complete` is now met by a dependency that succeeded *or* failed for good;
before, it excluded `Failed` and so behaved exactly like `success` (the
`Complete` task status is never written).

- **Alternative:** keep `complete` excluding failures (the previous code, and
  one reading of jobs-tasks-and-scheduling.md's "complete or successful").
- **Trade-off:** a behavior change for any spec that used `complete` to mean
  `success`: such a dependent now starts after a failed dependency.
- **Why:** docs/task-supervision.md already documented `complete` as
  "migrate finished, either way", and with the old semantics the two
  conditions were synonyms, so `complete` had no purpose (Airflow's
  `all_done` vs `all_success` is the familiar split). Surface:
  `{ task = "migrate", condition = "complete" }  # runs after migrate, exit 0 or not`.

## 15. A `restart` block without `policy` keeps the lifecycle's default policy

*Workstream: Task lifecycle.*

`TaskRestartConfig.policy` became `Option<RestartPolicy>`. A block that only
tunes `delay` or `successExitCodes` now keeps `onFailure` on a batch task;
before, the field defaulted to `always`, so

```nickel
{ name = "fetch", lifecycle = "batch", restart = { delay = "5s" } }
```

quietly became a batch task that restarts on success, exactly the P0-2
symptom, once lifecycle and restart started reaching the worker.

**Alternative.** Keep "a declared block means `always` unless it says
otherwise" and document it.

**Trade-off.** A behavior change for anyone who relied on the old reading (a
service task is unaffected: its default is `always` either way). Stored configs
deserialize unchanged; only the effective policy of a policy-less block on a
batch task moves. No shim, per AGENTS.md.

**Why.** Simplicity/ease: the struct comment, the docs table and
`maximal.ncl` all said "defaults per lifecycle"; the old serde default
contradicted them, and fixing P0-2 would have made the contradiction bite.

## 16. The availability reconcile runs every tick on the liveness owner, on evidence

*Workstream: Manager availability.*

The status-lane leader's tick returns `active` + `unavailable` nodes to
`available` only when it has heard from them since taking responsibility (a
beat or a peer digest, not the acquisition seed), and withdraws `lost` +
`available` nodes it has not heard from. It reads the scheduler's read model
and never touches `drained`.

- **Alternatives.** (a) Reconcile once, on leadership acquisition. (b) Only
  fix the root causes (re-registration keeping `unavailable`; follower
  forwarding) and add nothing.
- **Trade-off.** One read-model scan per tick (O(nodes) in memory) on the
  leader, forever, for a state that should be rare once the root causes are
  fixed.
- **Why.** Liveness is in memory and only transitions replicate, so any lost
  half-write (a leadership change between the two lanes' writes) is otherwise
  permanent and invisible ("No nodes available" with every node `active`).
  (a) misses disagreements created after acquisition; (b) leaves the next
  half-write bug as silent as this one. Evidence-gating keeps the pass from
  returning a dead node to the pool on a stale row.

## 17. A restarted raft member starts quiet instead of implementing pre-vote

*Workstream: Manager availability.*

A restarted voter in a multi-voter group starts with openraft's `elect`
switch off, and turns it on when an AppendEntries reaches its transport or
after a quiet window (5 x `electionTimeoutMaxMs`, at least 3s). It also skips
the up-to-15s leader wait at engine start.

- **Alternatives.** (a) Implement pre-vote in Legion's network layer. (b)
  Upgrade openraft. (c) Raise the election timings.
- **Trade-off.** A whole-cluster restart elects up to one quiet window later.
  A member cut off by a partition (not restarted) still campaigns and still
  deposes the leader when the partition heals; documented.
- **Why.** Code reading plus a control run showed the restart case: a
  restarted member waits one leader lease plus its election timeout for a
  leader to reach it, but the manager binds its listener only after raft
  starts, so a slow start used up that window; the member then campaigned
  repeatedly while unreachable, and its higher term deposed the healthy
  leader (`a_restarted_follower_does_not_depose_the_leader` fails without the
  fix and passes with it). That matches "term 10 after two leader stops".
  (a) is a
  protocol change inside openraft's vote handling; (b) is a large upgrade;
  (c) does not address the restart case at all. Performance: also removes up
  to 15s per lane from a restarted member's startup.

## 18. Non-root stdio: chown the log files to the image user, not pipes plus a drain

*Workstream: Linux containers.*

**The call.** At every start the worker chowns the allocation's `stdout.log`
and `stderr.log` to the uid/gid the container runs as (the image `USER`,
resolved against its rootfs). The log directory stays `0700` root. Reopening
`/dev/stdout` (`/proc/self/fd/1`) checks only the file's inode, verified on
this host: a root-owned file in a 0700 dir refuses uid 101 with `EACCES`; after
chown, uid 101 reopens it.

**Alternatives.**
- Docker's model: hand the container pipes `fchown`ed to its user and drain
  them into the files from a worker thread.
- The same pipes drained by a per-task shim process (conmon-style) that
  outlives the worker.
- Make the files `0666` (any uid in the container can reopen).

**Trade-off.** A process that switches to *another* user inside the container
(`su`, `gosu`) and reopens `/dev/stdout` still gets `EACCES` (exactly as under
Docker, whose pipes belong to the spec user). A reopen with `O_TRUNC`
(`echo x > /dev/stdout`) truncates the captured log, because it is a file and
not a pipe; that was already true for root containers and `exec` tasks. Both
are in the public doc.

**Why.** Tasks deliberately survive worker restarts (own session, re-adopted by
pid). A worker-thread drain would close the read end on every worker restart
and SIGPIPE every container's next write. A shim process is the robust form of
pipes, but it is a new long-lived process per task, a hidden subcommand, and
an upgrade story, for the narrow su-then-reopen case. Copy-truncate rotation
keeps the inode, so the chown survives rotation. `0666` would also cover
su-then-reopen but makes "world-writable log file" a thing to explain.
Simplicity and restart-robustness won over full pipe semantics.

## 19. The image cache is the node content store, per blob, with a HEAD freshness check and no TTL

*Workstream: Linux containers.*

**The call.** Image manifests, configs, and layers go into the existing node
content store (`artifacts` class) by digest, on both pull paths. A digest
reference the node holds costs no request. A tag costs one `HEAD` per start;
if the registry fails (429, down), the tag's last digest on this node (a new
`image_ref` table in the store's index) is used with a warning. Layers are
hard-linked into each allocation's layout. A process-wide claim makes
concurrent pulls of one reference share the download.

**Alternatives.**
- A short TTL during which a tag skips even the `HEAD` (the request offered
  this).
- Docker/Nomad's "present locally means use it" (never re-check a tag).
- A separate image store (own directory, own budget) from the content store.

**Trade-off.** Every start of a tag makes one request, so an image whose
registry is unreachable *and* has never been resolved on the node still fails.
Images share the `artifacts` budget (default 10% of disk, 1 to 20 GiB) with
checksummed artifacts, so a node with many large images evicts more. The
extracted rootfs is still rebuilt per allocation.

**Why.** `HEAD` is free under Docker Hub's anonymous limit and picks up a moved
tag on the next start, which is what someone who just pushed `:dev` expects; a
TTL would serve the old image for its duration, and "never re-check" makes
`:latest` mean "whatever this node first saw". The registry-failure fallback
covers the spot-check's 429 case without either. Reusing the content store
gets LRU, TTL, budgets, `minFreeDisk`, integrity checks, locality
advertisement, and `legion node content` for free, where a second store would
duplicate all of it (simplicity, and extensibility: the cache plan's per-blob
OCI storage is exactly this). The built-in container and sandbox runtimes
reach the store through a process-wide registration, because threading a
handle through `TaskInstance` would bump the task-plugin ABI for two built-in
callers.

## 20. A reschedule avoids the failed node with a two-pass plan, not a score penalty

*Workstream: Scheduler.*

`Eval::schedule_job_allocations` plans a node-carrying `job_reschedule` first
without the failed node; whatever that pass cannot place (an error, or
`unplaced`) is planned again with the node allowed.

- **Alternative:** a scoring penalty for the failed node inside the
  scheduler's policies (one pass).
- **Trade-off:** on a cluster where the failed node is the only fit, the eval
  plans twice. The first pass's failure is logged at `info`, not surfaced.
- **Why:** simplicity and extensibility. A penalty would have to be threaded
  through `bin_pack`, `spread`, and every scheduler plugin's scoring context,
  and a big enough weight is just an exclusion in disguise. Two passes give the
  exact semantics ("anywhere else first, this node only as a last resort")
  with no policy changes.

## 21. A plan that places nothing blocks on main's blocked evals; a partial remainder still rides the capacity pass

*Workstream: Scheduler. Revised at the merge with main's blocked evals
(sandbox-scaling plan §6.15).*

When this branch was written, a plan that placed nothing failed its eval and
a 5 s `job_reschedule` retried it, and the unplaced remainder of a partial
plan waited for the capacity pass. Main then shipped blocked evals: an eval
whose plan finds no room is stored `blocked` under a key (pool, class,
pinned node) and woken only by capacity that could help. At the merge:

- **A plan that places nothing** returns main's
  `blocked::PlacementBlocked` for the first unplaced task (with this
  branch's `scheduler::PlacementBlocked { unplaced }` as context, which the
  static-port rollout logic reads), so the eval blocks and wakes precisely.
  The message is this branch's port-aware reason.
- **A partial plan** still succeeds, and its remainder still waits for the
  capacity pass: when an allocation finishes or is removed, a job stops, or a
  job scales in, `Store::request_capacity_reconcile_if_waiting` queues a
  `node_join` eval with no node, but only while some plan since the last pass
  left replicas unplaced (`capacity_waiters`, an `AtomicBool` on the
  scheduler leader, set at start and cleared at the start of each pass). The
  pass also considers unfinished batch tasks, and skips a job with a pending,
  in-flight or blocked eval.

- **Alternative:** block the partial remainder too: let a partial plan
  apply what fits and then store the eval `blocked` under the remainder's
  key, retiring `capacity_waiters` and the free-triggered pass.
- **Trade-off:** two retry mechanisms coexist. A free re-plans every job
  short of its count, not only those the freed room could help; the flag is
  per process, so the first free after a restart or failover runs one pass.
  `job info` shows `blocked` only for a job that placed nothing, not for one
  running below its count.
- **Why:** simplicity at merge time. Blocking a *succeeded* eval needs a new
  completion path (apply, then block) through the eval loop and the blocked
  store, which is a scheduler change of its own with its own benchmark;
  the capacity pass already worked and is cheap when nobody waits. The
  follow-up is open in TODO.md.

## 22. A static-port rollout that cannot surge stops the old allocation (destructive update)

*Workstream: Scheduler.*

When the new version's replacement is a `PlacementBlocked` with a static port
held by old allocations of the same task, `try_place_for_group` stops up to
that many old holders and places on the next tick:
`Group web cannot surge: static port 8080 is held by the version being
replaced; stopped 1 old allocation(s) before placing the new one`.

- **Alternative:** reject `createFirst` for static-port tasks at submit, or
  keep waiting and rely on `capacityBlockTimeout`.
- **Trade-off:** the step is briefly down (old stopped before the new is
  healthy), and the new process may start while the old is still in its
  graceful stop and fail to bind once (its `restart` policy covers it). Only
  the port that blocked is considered; a port held by another job remains a
  plain capacity block.
- **Why:** the finding asked for it; a submit-time rejection cannot know the
  cluster shape (with enough nodes, `createFirst` does work for a static
  port), and waiting is exactly the reported failure.

## 23. `transfer-leader` and `step-down` are built on remote elections

*Workstream: Manager availability.*

`transfer-leader --target X` asks X (new `RaftTransport.TriggerElection`,
cluster-secret auth, no Manager ACL row) to campaign, re-asking once per
election timeout + 250ms until X leads the control group or 10 election
timeouts (min 10s) pass, while the asking manager turns its own `elect` off.
`step-down` picks the voter with the furthest replication progress and runs
on the leader (a follower forwards it there via entry 3).

- **Alternatives.** (a) Return `FailedPrecondition`/`Unimplemented` with an
  actionable message (the minimum the finding allowed). (b) Have the leader
  stop heartbeating to let leases expire.
- **Trade-off.** A transfer costs a few terms and is best-effort under load:
  the command reports a warning (not an error) when leadership has not moved
  by the deadline. Disabling `elect` on the asking manager is a no-op unless
  it is the incumbent.
- **Why.** Simplicity/ease: decommissioning the leader and planned
  maintenance are common operator tasks, and "run trigger-election on the
  other host" was the workaround the spot check needed. (b) stalls writes for
  a full lease and does not choose the successor.

## 24. `legion start manager` without `-c` means built-in defaults, including a built-in worker type

*Workstream: CLI and diagnostics.*

No `-c` no longer reads `./configs/manager.ncl`; the manager runs on
`ManagerConfig` defaults. `workerConfigPath` now defaults to unset, which
registers one built-in worker type `default` (pool `default`). A configured
directory that does not exist is a startup error naming it. `legion job run`
with no path (which read `./configs/jobs/demo.ncl`) now requires the path.

- **Alternatives.** Fall back to defaults only when the default file is
  missing (two behaviors depending on the cwd — the bug's shape); keep
  `./configs/workers/` as the worker-type default and skip it when missing
  (same cwd magic).
- **Trade-off.** Inside a repo checkout, `legion start manager` with no `-c`
  no longer picks up `configs/manager.ncl` or `configs/workers/*`; dev flows
  must pass `-c configs/manager.ncl` (AGENTS.md already does).
- **Why.** Simplicity: the README quickstart has to work from any directory,
  and "no file means defaults" is the explainable rule.

## 25. Precondition failures are a typed `StorageError::FailedPrecondition`; untyped errors keep their cause

*Workstream: CLI and diagnostics.*

Added `StorageError::FailedPrecondition { message }` (Display = the message),
mapped to `FAILED_PRECONDITION`. Converted: job destroy on a running job,
keyring rotate/rewrap refusals, and the fork/snapshot sites that used
`StorageError::ForeignKey` to get the right code (their messages read
"foreign key constraint failed: …"). Two untyped "not found" bails became
`StorageError::NotFound`. Any remaining untyped error is still `INTERNAL` but
now carries `"{action}: {err:#}"` instead of the bare action.

```console
$ legion job destroy web
Error: failed to destroy job: Error deleting job: Job web must be stopped before it can be destroyed (it is running); run `legion job stop web` first (the request is valid, but the cluster's current state does not allow it; the message says what has to change first) [code=FailedPrecondition]
```

- **Alternatives.** Map by message text in `map_store_error` (fragile); fix
  the one destroy call site (the finding asked for the class).
- **Trade-off.** Internal messages can now reach clients verbatim (the same
  text the server already logged; logs must not hold secrets either).
- **Why.** The caller sees the reason without reading server logs (ease).

## 26. Liveness never overwrites `drained`

*Workstream: Manager availability.*

`sync_node_availability` (the liveness half of loss and recovery) now updates
only rows that are not `drained`. A cordoned node that goes lost stays
`drained`, so it is still cordoned when it returns.

- **Alternative.** Keep loss writing `unavailable` over anything, and teach
  recovery to remember the prior value.
- **Trade-off.** A drained-and-lost node reads `lost` / `drained` rather than
  `lost` / `unavailable` in `legion node list`. Placement is identical (only
  `available` places).
- **Why.** Before, loss rewrote `drained` to `unavailable` and recovery then
  rewrote that to `available`: an outage silently undid an operator's cordon.
  The owner-of-the-value rule (liveness owns `available`/`unavailable`,
  operators own `drained`) is one sentence and needs no extra column.

## 27. Re-registration turns `unavailable` into `available`

*Workstream: Manager availability.*

`WorkerRegister` keeps an existing `drained`, but maps `unavailable` (set by
node loss) to `available`.

- **Alternative.** Keep the stored value and rely on the next heartbeat's
  transition (which was the design).
- **Trade-off.** None found: a registering worker is alive by definition.
- **Why.** The heartbeat repair only works where the transition write can
  run; on a follower in `single` topology it could not (P0-5's lane-mismatch
  bug), so the node stayed `active` and unschedulable, which is what the
  spot check observed. The registration log line ("it is schedulable again")
  was also false before.

## 28. The macOS 200 MiB floor: reject at submit only when the task is pinned to macOS; otherwise fail early on the macOS worker

*Workstream: Linux containers.*

**The call.** `check_resources` (submit) rejects a `container` or `sandbox`
task whose constraints pin `os = "macos"` and whose guest is under 200 MiB.
Every other task is checked at start on a macOS worker, which fails before
invoking Apple `container`, with the same message naming the floor and the
field to raise. Linux is unaffected.

**Alternatives.**
- Warn at submit for every container task under 200 MiB.
- A placement-time constraint that keeps small container tasks off macOS
  nodes.
- Round the VM up to 200 MiB on macOS and let it run.

**Trade-off.** An unpinned task on a mixed or macOS-only cluster learns about
the floor at start, not submit (but from the allocation's failure reason, not
buried in stderr).

**Why.** At submit the target OS is unknown unless the task pins it. A blanket
warning would fire on every small Linux container job (128 MiB containers are
common), which trains people to ignore warnings. A placement filter would turn
a fixable spec error into `No nodes available` on a macOS-only dev cluster,
hiding the reason, and lives in the scheduler track's code. Rounding up makes
the declared memory a lie and overcommits the Mac by the difference per task.
Simplicity: one rule ("macOS VMs are at least 200 MiB"), enforced where the
fact is known, with one message.

```text
task web sizes its macOS VM at 128 MiB (resources.memoryMax, else resources.memory), below the 200 MiB Apple `container` requires; raise it to at least 200, or constrain the task to Linux nodes
```

## 29. Layer ownership is kept only when the worker is root; xattrs are best effort and skip `security.selinux` and `trusted.*`

*Workstream: Linux containers.*

**The call.** Extraction keeps each entry's owner exactly when the worker's
euid is 0. Legion applies it itself after the tar crate unpacks (`lchown`,
then re-applying a setuid/setgid mode that `chown` clears), because the tar
crate's own `preserve_ownerships` fails the whole layer on an empty uid/gid
field, which Go's `archive/tar` (Docker) reads as 0; an existing test layer hit
exactly that. PAX `SCHILY.xattr.*` records are re-applied as root, skipping
SELinux labels and overlay bookkeeping and logging (not failing) any attribute
the filesystem refuses.

**Alternatives.** Always try to chown and fail the start when it fails; or
apply every xattr and fail on error (the tar crate's `unpack_xattrs`).

**Trade-off.** A rootless worker produces a rootfs owned by its user, so an
image with a non-root `USER` may not be able to write its own directories
there; a host that refuses `user.*` xattrs loses them silently (debug log).

**Why.** A non-root worker cannot chown at all, so "try and fail" would break
every image for rootless development. Failing a start over an SELinux label
the host's policy would replace anyway helps nobody. Simplicity: the root path
is Docker's behavior, the non-root path degrades instead of failing.

## 30. `RLIMIT_NOFILE` defaults to 1048576 (soft = hard), capped at the worker's own hard limit, with a Compose-style `ulimits` override

*Workstream: Linux containers.*

**The call.** The bundle sets `nofile` soft = hard = 1048576, clamped to
`fs.nr_open` and to the worker's own hard limit (root or not). `ulimits` is a
map of lowercase names to a number, `"unlimited"`, or `{ soft, hard }`; each
declared entry replaces the default of that name.

**Alternatives.** containerd's own default (hard 1024, the bug) or systemd's
1024 soft / 524288 hard, which leaves programs to raise their soft limit.

**Trade-off.** Old software that loops over every possible descriptor (some
`close_fds` implementations) is slower with a million-descriptor limit, which
is why systemd and containerd moved to low soft limits.

**Why.** It is the value Docker containers ran with for years and what CRI-O
sets, so images are tuned for it; Redis's warning and a proxy's exhaustion go
away without a jobspec change. The override covers the rest in one familiar
shape (Compose's `ulimits`). Simplicity over the corner case.

The cap at the worker's hard limit came from the end-to-end run: on the test
host even root (with `CAP_SYS_RESOURCE`) could not raise its hard limit above
20000, so `runc` failed every start with `error setting rlimits for ready
process: operation not permitted`. Capping first means a start never fails on
the default; the node-wide knob is the worker unit's `LimitNOFILE=` (systemd's
default 524288 is still far above Redis's 10032). The alternative, probing
whether raising works, is a fork per start to answer a question the operator
already answered in the unit file.

## 31. Health checks stand aside while the process is down instead of racing the supervisor

*Workstream: Task lifecycle.*

The health monitor now asks the task manager whether the allocation's process
has exited (`process_exit`) before counting a result. While it is down, failed
checks are ignored; after the supervisor relaunches it, the monitor resets its
failure windows and boot grace, as it does after its own restart.

**Alternative.** Let both paths keep racing and make whichever fails the
allocation merge in the other's facts (exit status into a health failure, check
results into an exit failure).

**Trade-off.** There is still a window of up to one supervisor tick (1s)
between the process dying and `poll_task_exits` noticing, during which a check
can count; with default thresholds (3 failures after a 1s boot grace) it rarely
matters. A check that would have caught a hung-but-alive process is unaffected.

**Why.** Simplicity: one owner per fact. The supervisor owns exits and already
applies the restart policy with the shared budget; two paths restarting the
same dead process also double-spent that budget.

## 32. A gated start creates its own namespace rather than failing or waiting for a heartbeat

*Workstream: Task lifecycle.*

Root cause of P1-4: the manager pushes new allocations straight to the worker
(`Worker.PostWork`), and that path never ran `apply_net_capture`, which only
the heartbeat calls. Whether the namespace existed at start depended on whether
a heartbeat happened to land before staging finished. Now
`task_plugins::ensure_gated_namespace` (used by the `exec` and `container`
starts) creates it when missing; `netd::netns::create` is serialized with a
process-wide lock so the heartbeat and a start cannot stack two bind mounts;
`record_failure` releases a namespace for a failed gated allocation, so one
created by a pushed start that failed before any heartbeat listed it does not
leak.

**Alternatives.** (a) Treat "not ready" as a retryable start: hold the
allocation and retry on the next heartbeat. (b) Have PostWork call the
heartbeat's capture setup.

**Trade-off.** The datapath listeners inside the namespace are still bound by
the next heartbeat, so a pushed gated task has loopback only for up to one
heartbeat interval (documented in docs/network-handlers.md). (a) would have
avoided that window but added a new "deferred start" state to the launch
machinery, and (b) would have put datapath setup on an RPC handler owned by
another track.

**Why.** Simplicity: creation is idempotent and cheap, the start is where the
namespace is needed, and the failure mode goes from "allocation failed and
rescheduled (and P1-3 then strands it)" to a short window without egress.

## 33. Allocation ids: carry `sqlite_sequence` through lane snapshots; key nothing differently

*Workstream: CLI and diagnostics.*

`allocation` already had AUTOINCREMENT (it landed after the spot-check base),
so deletes and restarts no longer reuse ids. The remaining path was the
`lanes` topology's table-scoped snapshot: it copies rows, so an installing
replica re-seeds its sequence to the largest *surviving* id, and after it
wins an election it can reissue an id the old leader used and deleted. The
dump now carries each dumped table's `sqlite_sequence` mark and install
raises (never lowers) the local mark. Whole-DB (`single`) snapshots
(`VACUUM INTO`) and changeset apply (explicit-id inserts) already preserve
it; both are now tested.

- **Alternatives.** A replicated id-allocator table (`id_sequence`) bumped in
  the placement transaction (independent of SQLite internals, but one more
  write on the hottest scheduler path and another table to own); keying ids
  by a UUID/ULID (unbounded ripple through every `i64` allocation id in the
  proto, worker state, and paths).
- **Trade-off.** Relies on SQLite's AUTOINCREMENT bookkeeping, and edits a
  file owned by the ha track (`src/storage/raft/state_machine.rs`, two small
  hunks plus tests).
- **Why.** Smallest correct change on the path that was actually wrong;
  zero cost on the placement path (performance).

## 34. A stale worker log dir is moved aside on an `owner.json` mismatch

*Workstream: CLI and diagnostics.*

Before a fresh launch the worker compares `logs/<id>/owner.json` (allocation
id, job, job config id, task config id, task name) with the allocation it is
starting; a mismatch, or output with no owner file, renames the directory to
`<id>.replaced-<unix>` and starts empty. A match (the same allocation
relaunched) keeps appending.

- **Alternatives.** Key the directory by job + allocation (`logs/<job>/<id>`;
  breaks the shipping glob and the manager's read path, and still collides
  for the same job name on a rebuilt cluster); always rotate the dir on any
  fresh launch (splits the logs of one allocation across a worker restart).
- **Trade-off.** An identical rebuilt cluster (same job name, version, task,
  task config id, and allocation id) is indistinguishable and appends; the
  limitation is documented. Two call sites in `task_manager.rs` (lifecycle
  track's file) changed to call the claim before the stderr mark is taken.
- **Why.** Within a cluster ids are no longer reused, so this only guards
  against a worker outliving its cluster's state; the owner-file check is
  cheap, never fails a launch, and keeps the data rather than deleting it.

## 35. Kill an orphaned process group at restore; document `KillMode` rather than ship a unit

*Workstream: Manager availability.*

When a restored allocation's tracked pid is dead but its process group still
has members, the worker SIGKILLs the group (`kill(-pgid)`; pid 0/1 refused).
`docs/architecture-managers-workers.md` documents `KillMode=process`, a fixed
`LEGION_DATA_DIR`, and `--name`.

- **Alternatives.** (a) Ship `legion-worker.service` with
  `KillMode=control-group` (tasks die with the worker). (b) Adopt orphaned
  children as the task.
- **Trade-off.** P1-5's root cause is not confirmed from the rig; the fix
  covers the two causes the code allows (dead group leader with live children;
  a different data dir / identity). A task restarted under a new identity
  still cannot see its old processes.
- **Why.** Tasks outliving the worker is a design property (re-adoption);
  (a) would quietly turn every worker restart into a full task restart. (b)
  cannot know which child is "the task". Linux never reuses a pid while it is
  a live process-group id, so killing the group touches only the task's own
  descendants.

## 36. Blocked rollouts back off instead of suppressing eval events

*Workstream: Scheduler.*

A capacity-blocked group re-ticks after `clamp(blocked_for / 2, 2s, 30s)`
(never past `capacityBlockTimeout`), and a dependency-held group after 15s.

- **Alternative:** stop emitting `eval.enqueued`/`eval.completed` for
  `rollout_tick` evals that changed nothing.
- **Trade-off:** a blocked rollout still produces some eval events (about two
  every 30s after the first minute), and capacity that appears is noticed up
  to 30s late.
- **Why:** event emission is generic eval-loop code (src/manager/daemon.rs,
  another track's file) and a per-kind exception there is a special case
  every reader of the stream would have to know. The rollout's own event log
  was already change-gated.

## 37. The planner mirrors the apply's port view, including a Fresh plan's dropped rows

*Workstream: Scheduler.*

The planner's `PortLedger` counts ports held by the rows a Fresh plan dropped
from its capacity snapshot (the job's own previous allocations), because the
apply transaction still sees those rows.

- **Alternative:** ignore them, like capacity does, and change the apply to
  ignore them too.
- **Trade-off:** a Fresh resubmit of a static-port job whose old allocations
  still exist cannot reuse the port on the same node in one plan (it fails
  with a clear static-port error). In practice `job_update` deletes first and
  rollouts use Reschedule plans, so this only affects the rare
  `job_submit`-with-a-previous-config path.
- **Why:** the planner and apply must agree, or the plan is rejected and the
  eval loops; changing the apply would have allowed two rows holding the same
  static port on one node.

## 38. Unchanged resubmit restarts the job when nothing of it is running

*Workstream: Scheduler.*

`RunJob` with an unchanged spec queues a `job_update` when the job was
stopped, had failed, or is idle (no live allocation and no placing eval due
now), with a message saying so.

- **Alternative:** restart only stopped/failed jobs, and keep "no update
  needed" for a finished batch job or a pending one.
- **Trade-off:** re-running a finished batch job's spec runs it again, and a
  resubmit of a pending job retries placement immediately.
- **Why:** "run" on something not running should run it; the old message gave
  no way to retry besides `--force-reschedule`, which the user had to know.

## 39. `kv get` on a missing key fails in the CLI; the RPC is unchanged

*Workstream: CLI and diagnostics.*

The CLI turns `GetKvReply { entry: None }` into a `NotFound` envelope and
exit 1. The RPC keeps returning an empty entry.

- **Alternative.** Make the RPC return NOT_FOUND (consistent for gRPC
  clients, but the UI and other callers rely on the empty reply).
- **Why.** Fixes the scripting idiom without touching the UI contract.

## 40. `--json` on every command, enforced by a tree-walking test; `exec --json` is a one-shot capture

*Workstream: CLI and diagnostics.*

`cli()` moved from `main.rs` to `crate::cmd::cli()` so a unit test walks the
real clap tree and fails when a runnable command lacks `--json` (exempt:
`start`, `start manager|worker`, `join`, `mcp serve`, `guest-agent`,
`guest-resume`). Added `--json` to `raft status|history|command|join-token`,
`exec`, `action list|run`, `docs task-schemas`. `exec --json` buffers output
into `{allocationId, exitCode, stdout, stderr, message}` and still exits with
the remote code; it refuses `-t`/`-i`.

- **Alternatives.** Exempt `exec`/`action run` as interactive (agents would
  keep scraping stdout); NDJSON frames for exec (streamable, but not one
  envelope).
- **Trade-off.** `ok: true` with a non-zero process exit when the remote
  command fails; output decoded as lossy UTF-8.
- **Why.** Agents get one envelope per call (ease), and the rule is enforced
  rather than remembered.

## 41. `--run-worker` advertises the manager's routable host, not a new knob

*Workstream: Manager availability.*

The in-process worker reaches its manager over loopback, so the worker
plane's route-based detection advertised `127.0.0.1`, unreachable from other
managers. The manager now sets the worker's advertise flag to the host of
`server.advertiseAddr`, else the raft `advertiseAddr`, when that host is
routable; `LEGION_WORKER_ADVERTISE_ADDR` still wins.

- **Alternative.** A manager-config field for the co-located worker's
  advertise address, or detecting toward a peer manager.
- **Trade-off.** A manager whose only advertised addresses are loopback keeps
  the old behavior (correct for a single box).
- **Why.** Simplicity: the manager already states where it can be reached;
  reusing it needs no new setting.

## 42. The receiver checks the forwarded lane in group space

*Workstream: Manager availability.*

`ForwardLaneWrite`'s receiver now compares the envelope's `group_id` against
the group that commits the op's lane on this manager (`control` under
`single`), via the same `physical_group` helper the sender uses.

- **Alternative.** Have the sender put the logical lane in `group_id`.
- **Trade-off.** None on the wire: senders already send the physical group,
  so new receivers accept what every existing sender sends, and no
  `PROTOCOL_VERSION` bump is needed.
- **Why.** `group_id` means "raft group" on every other RaftTransport RPC;
  changing the sender would have made one RPC's field mean something else.

## 43. Failover thresholds: two failures, 5s connect, 30s request, 10s keepalive

*Workstream: Manager availability.*

- **Alternative.** Fail over on the first failure; no request timeout.
- **Trade-off.** Up to ~two retry intervals before moving; a 30s cap on any
  worker->manager unary call.
- **Why.** One dropped request (a manager restarting its listener) should not
  move a fleet; without the timeouts a manager whose host vanished hangs a
  heartbeat for the OS TCP timeout (minutes), far past the lost window.

## 44. The staging thread enters the caller's tokio runtime; the monitor refuses to start without one

*Workstream: Task lifecycle.*

`spawn_staging_thread`, `spawn_provisional_staging_thread` and edge boot's
`provisional-launch` thread capture `Handle::try_current()` where they are
spawned (always inside the worker runtime: a `spawn_blocking` closure or an RPC
handler) and enter it after staging, which gives them the same context a
`spawn_blocking` closure has. `spawn_health_monitor` itself now uses
`Handle::try_current()`: without a runtime it logs an error and sets the
allocation's status to `health checks could not start (worker bug: no async
runtime)` instead of panicking its thread.

**Alternative.** Hand the "start the monitor" step back to the runtime through a
channel, or run staging on `spawn_blocking`.

**Trade-off.** Entering a runtime on an OS thread is implicit context a reader
has to know about; the helper pair (`current_runtime` / `enter_runtime`) and its
doc comment carry that. `spawn_blocking` for staging was rejected because a
slow pull would pin a blocking-pool thread for minutes.

**Why.** Smallest change that makes every tokio call reachable from those
threads correct (the audit found the same bug in the edge-boot launch thread),
plus a guard so the next missed call site is visible instead of silent.

## 45. The resource governor takes its configuration explicitly

*Workstream: Task lifecycle.*

`WorkerTaskManager` now starts with a disabled governor and the daemon calls
`configure_resource_enforcement(&config)` (via the new
`NodeResourceGovernor::from_settings(enabled, cgroup_parent)`) before restoring
or starting anything. The env is still exported for the task runtimes that read
it. The Linux "Resource enforcement enabled" line is now logged only by the
governor, once it has checked the node.

**Alternative.** Move `apply_resource_enforcement_env` above the task manager's
construction and keep `from_env()` in the constructor.

**Trade-off.** Two sources of the same setting remain (explicit for worker
core, env for runtimes); a runtime and the governor could in principle
disagree. They are written from the same config in the same function.

**Why.** An ordering dependency is what broke this; the reorder would have
kept it. Passing the config makes the dependency a call the compiler sees.

## 46. The default plugin root is found next to the binary, and the build checkout only if it exists

*Workstream: Task lifecycle.*

`plugin_root_dir()` without `LEGION_PLUGIN_DIR` now tries
`<exe dir>/plugins`, `<exe dir>/../lib/legion/plugins`, then the build
checkout's `plugins/`, taking the first that exists (falling back to
`<exe dir>/plugins`). `validate_manifest` no longer logs its own error at
`ERROR` (`#[instrument(err)]` removed); callers that need a plugin still fail
with the path in the error.

**Alternatives.** The worker's data directory; or keep the build path and only
downgrade the log.

**Trade-off.** The bundled layout is a convention we now support but do not
ship a package for. A binary whose directory happens to contain an unrelated
`plugins/` would look there.

**Why.** Ease: an installed binary must not carry its build machine's path, and
the two layouts cover a tarball and an FHS install without a new knob; `cargo
run`/`cargo test` keep working because the checkout exists for them.

## 47. Exec sessions into `exec` tasks get the task's env, and stay in the host network namespace

*Workstream: Task lifecycle.*

`ExecTaskPlugin::exec` now layers `allocation_env` (`LEGION_ALLOC_ID`,
`LEGION_PORT_*`, `LEGION_ADDR_*`, upstreams) under the worker-injected and task
env, matching `start`. Raw exec and actions share this path, so both get it.
An exec session for a `network.mode = "gated"` exec task still runs in the
host namespace.

**Alternative.** Also `setns` exec sessions into the gated namespace.

**Trade-off.** Inside a gated exec task, `legion exec -- curl $LEGION_UPSTREAM_DB`
reaches the host, not the namespace's listeners. Noted in
docs/exec-and-actions.md and as a TODO.

**Why.** Scope: P2-8 is the env; namespace entry for exec sessions needs
`ExecOptions` to carry a namespace and the PTY spawner to honor it, which is a
change to a shared runtime interface.

## 48. A ratified `stage`-gated claim gets its health monitor

*Workstream: Task lifecycle.*

Found in the P0-3 audit: `edge::apply_verdict` discarded
`Ratified.start_health_monitor`, so a `stage`-gated provisional start that was
ratified never got a health monitor and was never reported healthy. It now
spawns one after releasing the task-manager lock.

**Alternative.** Leave it to the edge-boot owner as a separate item.

**Trade-off.** Touches edge boot, which the spot check did not cover, with no
end-to-end test of its own here (the existing ratify unit tests assert the
field is set).

**Why.** It is the same "health monitor never starts" bug class this track
owns, and a one-line consumer of a field that existed for exactly this.

## 49. `map_store_error` logs by severity; expected lookups are debug

*Workstream: CLI and diagnostics.*

Every mapped store error used to be logged at ERROR, and `#[instrument(err)]`
on `get_node`/`get_job`/`destroy_job` logged every NotFound/refusal at ERROR.
That is where the fresh-start `ERROR … node not found` came from: registration
looks the joining node up first, and a new node always misses. Now
Internal/DataLoss/Unknown log at error, Unavailable at warn, everything else
(NotFound, FailedPrecondition, non-leader redirects) at debug; those three
store functions record errors at debug.

- **Alternatives.** Add a `find_node -> Option<Node>` for registration only
  (fixes the one line, leaves `legion job info typo` logging ERROR).
- **Trade-off.** Refused requests disappear from default-level server logs.
- **Why.** ERROR has to mean "the server is broken" for operators to trust it.

## 50. Human error lines print the code name; per-code hints shared with JSON

*Workstream: CLI and diagnostics.*

`friendly_status` printed tonic's prose for the code
(`[code=The system is not in a state required for the operation's execution]`);
it now prints `[code=FailedPrecondition]`, and one `hint_for(code, message)`
serves both formats. `Unavailable` distinguishes a transport failure (the
manager is unreachable) from a manager answer (a worker or leader behind it).

## 51. Stored CLI tokens are keyed by manager address, and a first start replaces the file for its address

*Workstream: CLI and diagnostics (2026-10-12 re-run).*

**The call.** The CLI token file moved from one `~/.legion/token` to
`~/.legion/tokens/<host>_<port>`, named from `LEGION_ADDR` with the scheme
dropped. `legion login`, `legion logout`, the CLI's lookup and the manager's
first-start write all use that one name. The manager writes under its
*local* address (`server.host`/`server.port`, `0.0.0.0` as `127.0.0.1`),
not `advertiseAddr`, and it now replaces a file already there instead of
leaving it alone. An `Unauthenticated` reply's hint names the file (or
`LEGION_TOKEN`) and the address the CLI used:

```text
Hint: the CLI sent the token stored for http://127.0.0.1:4757 in
/home/me/.legion/tokens/127.0.0.1_4757, which this manager does not accept ...
```

**Alternatives.**
- Keep one file and warn louder. The second cluster's operator still gets
  the wrong token, which is the bug.
- Key by cluster name, as the federation plan's contexts would
  (`~/.legion/tokens/<name>`). The CLI does not know a cluster's name before
  its first call, so that needs contexts first.
- Fall back to the old `~/.legion/token` when no per-address file exists.
  AGENTS.md says no compatibility shims, and a fallback reintroduces the
  cross-cluster leak for any address without its own file.
- Keep "leave an existing file alone" for the per-address file. A cluster
  rebuilt on the same port (the common dev loop) would then present the dead
  cluster's token, the same bug one level down.

**Trade-off.** A raft cluster reached through several managers needs a file
per address (`legion login` against each, or `LEGION_TOKEN`). The rig's
`rig_install_cli_token` now writes one per control-plane address. Two
clusters that take turns on one address replace each other's file, and the
older one's CLI then fails `Unauthenticated` with a hint naming the file to
fix. A manager behind `advertiseAddr` is reached from its own machine by its
listen address, which is what the name uses.

**Why.** Simplicity: the name comes from what the CLI already knows, so there
is nothing new to configure, and two local clusters on different ports just
work. The mint only runs on a cluster with no management token, so a file it
replaces held a token that cluster would reject anyway (or a lesser login to
it). Documented in [auth-and-acls.md](../../docs/auth-and-acls.md#where-the-initial-token-goes).

## 52. Linux `command` still replaces `ENTRYPOINT`; examples name the entrypoint instead

*Workstream: Linux containers.*

**The call.** The divergence (Linux ignores the image's `ENTRYPOINT` and
`CMD`; Apple `container` keeps `ENTRYPOINT`) is documented and the examples
call `docker-entrypoint.sh` explicitly, which works on both. Changing the
semantics is left as a TODO.

**Alternative.** Make Linux honor `ENTRYPOINT` (and `CMD` when `command` is
omitted), with an `entrypoint` override, as Docker does.

**Trade-off.** `command = ["postgres"]` (the natural Docker habit) still fails
on Linux, with Postgres refusing to run as root.

**Why.** It changes what every existing Linux container jobspec runs and makes
`command` optional: a user-facing surface change that needs its own plan, not
a fix folded into a capability change.

## 53. `postgres.ncl` drops `to = 5432` and listens on the assigned port

*Workstream: Linux containers.*

**The call.** The example runs `docker-entrypoint.sh postgres -p
"$LEGION_PORT_PG"` and declares the port without `to`.

**Alternative.** Keep `to = 5432` so consumers' VIPs listen on the well-known
port, and listen on 5432 inside.

**Trade-off.** A VIP for this service gets the dynamic port number rather than
5432.

**Why.** On Linux the container shares the host network, so listening on 5432
collides with a second Postgres on the node and is not the port the service
and health check use; on macOS, `to` would publish the host port to 5432 while
Postgres listens on the assigned one. Without `to`, one command works on both.

## 54. A worker's manager trust is shared per cluster by aliasing every reported address to the token's verifier

*Workstream: Manager availability (found on the AWS spot check, after entry 7
shipped).*

**The call.** After a leader failover a worker's active manager was one its
token never named. Heartbeats kept working (the failover connector fell back
to the token's verifier), but every call that dials a manager by address
failed:

```text
artifact staging failed: ... credentials from kv secrets/ecr-pull: connect to manager for
artifact credential: no cluster CA pin for the manager at http://172.31.80.241:4747; ...
```

The registry in `src/worker/plane.rs` stays keyed by address, but every
address of a cluster now maps to the *same* `PinnedManagerVerifier`.
`trust_cluster_managers(seed, addrs)` adds the addresses a manager reported
(and the persisted `managers.json` set) under the token's verifier, from
`ManagerEndpoints::new` and `learn`, before the known set changes. The
connector's seed fallback is gone, and the bundle swap now moves the whole
cluster at once because the verifier is shared. The JWKS source resolves the
active manager per fetch instead of dialing the token's address forever.

**Alternatives.**
- One process-wide trust used for any address.
- Have `ManagerEndpoints` own the verifier, and route every caller through
  it instead of through an address lookup.

**Trade-off.** A manager address the cluster never reported is still
refused, even though the CA check alone would make it safe to dial. A worker
can reach a new manager by address only after a reply has reported it, which
happens on registration and every heartbeat.

**Why.** Process-wide trust breaks processes that hold several clusters (the
simulator and the tests run workers for several side by side), and it would
drop the defense-in-depth rule that the worker dials only addresses its
cluster vouched for. Routing every caller through `ManagerEndpoints` would
mean changing each call site's signature (artifact credentials, the JWKS,
`connect_manager_with_retry`, the simulator) for no change in behavior.
Aliasing fixes every caller that dials by address in one place, so
simplicity won. The security property does not change: an address only says
where to dial, and the manager there must still present a `manager.legion`
certificate chained to the CA the token pinned.

## 55. The reschedule backoff is a replicated hold the planner honors, not a timer

*Workstream: Scheduling (found on the AWS spot-check re-run, after the
backoff shipped).*

**The call.** On the rig, `sandbox-store` got a new allocation about every
minute (1195, 1202, 1212, 1214, 1221) while each failure logged
`delay_secs: 1800`, and its instance sat `rescheduling` with nothing saying
why. The backoff only delayed the `job_reschedule` the failure queued. Any
other `Reschedule` eval for the job placed every missing replica at once: the
capacity pass that each failure's freed room triggered, a node join, the
divergent-leadership regeneration. Now the failure's scheduler-lane removal op
also writes a `reschedule_hold` row (`Store::remove_failed_allocation`), and
`plan_and_apply` subtracts one replacement per active hold from every
`Reschedule` plan's required counts. The keyed and slot binders put held
instances last. The rows are also the move history, so the in-memory
`AllocationFailureTracker` history is gone, and the replacement is a
replicated delayed eval instead of a `tokio::sleep`. Operators see it as
follows:

```text
detail: rescheduling: attempt 4 failed (artifact staging failed: 404 Not Found);
next attempt at 2026-10-12T02:31:05Z (backoff 4m); `legion instance start sb-1` retries now
```

There's also an `allocation.reschedule_deferred` event and
`Job.reschedule_backoffs` on `job info`.

**Alternatives.**
- Keep the in-memory tracker, and only add the event and an in-memory
  "deferred until" for display. The display would read empty on every
  manager but one, and nothing would stop the early re-placements.
- Have only the capacity pass and the regeneration skip held jobs. Every
  other path that places (node join, scale, blocked-eval wake) would still
  bypass the backoff, and each future path would have to remember to.
- Put "not before" on the instance row only. It covers stateful tasks and
  leaves stateless ones, which fail the same way, unprotected.

**Trade-off.** The scheduler-lane removal op got a new forwarded variant
(`RemoveFailedAllocation`), a new table, and one more indexed read per
`Reschedule` plan. A stateless task's holds are counts, not identities. While
a stateless task holds N replacements, its next N missing replicas wait,
whatever made them go missing (a lost node included). The rows prune lazily,
on the next failure's removal.

**Why.** Honoring the hold in the planner is the only place that covers
every path that places, now and later. That's the property the user made a
condition of keeping the backoff at all. Recording it in the op that removes
the allocation means no eval can slip in between the removal and the hold.
Replicating it makes the operator-facing state truthful on every manager and
makes the backoff survive the restarts and leader changes the rig does
routinely. Correctness and operator clarity won over a smaller diff.

## 56. The backoff doubles per consecutive failure, per instance, and resets once a replacement survives `move_period`

*Workstream: Scheduling.*

**The call.** `attempt` counts a chain. A failure continues the chain when it
comes within `move_period` of the previous hold's release (`created_at +
delay`, so a lifted hold still continues it). The chain is keyed by instance
for stateful tasks and by task for stateless ones. `move_count`, the
job-failing budget, still counts every move of the task inside
`move_period`, as before.

**Alternatives.**
- Keep doubling per move inside `move_period`, which is what the previous fix
  did. With the default 30m period, the count of moves in the window drops as
  the delays grow, so the backoff oscillated
  (30s, 1m, 2m, 4m, 8m, 16m, 4m, 4m, 4m, 8m, ...) and never reached the
  documented 30m cap.
- Key the chain per task for keyed tasks too.

**Trade-off.** The backoff and the budget now count different things, which
the docs have to explain. A slow failure cadence (a replacement that runs
29 minutes before failing) keeps doubling.

**Why.** A chain gives the monotone 30s, 1m, ... 30m the docs promise, and
"survives a whole `move_period`" is a clear reset rule. Keying it per
instance keeps one broken sandbox from making every other sandbox of the task
wait 30 minutes on its first failure. That would have turned a backoff meant
for safety into an outage for a keyed sandbox fleet.

## 57. `--force-reschedule` drops the job's move history, `instance start` keeps it

*Workstream: Scheduling.*

**The call.** A `job_update` eval (`job run --force-reschedule`, restarting a
stopped or failed job) deletes the job's holds, so it places everything now
and starts the budget over. `instance start <id>` on a `rescheduling` keyed
instance only lifts that instance's hold (`not_before_unix = 0`). It answers
`Retrying instance <id> now: lifted its reschedule backoff`, and another
failure continues its chain.

**Alternatives.** Have both keep history, or have both reset it. A new
`legion instance retry` verb was the other option.

**Trade-off.** A job restart forgets that the task was failing. A restarted
job with `move_count` gets the full budget again.

**Why.** A job restart already means "run it from the top" (task statuses
reset too), and a restarted failed job must not fail again on its first
failure because of the old budget. `instance start` is a nudge on one
instance of a job that may still be broken, so keeping the chain stops a
script that calls start in a loop from defeating the backoff. Reusing
`start` instead of a new verb keeps the surface small. Start already meant
"make this instance run", and for a `rescheduling` instance it used to do
nothing.

## 58. A one-shot CLI command dies by SIGPIPE (status 141) on a closed pipe

*Workstream: CLI and diagnostics (2026-10-12 re-run).*

**The call.** `legion raft status | head` panicked with "failed printing to
stdout: Broken pipe". `main` now restores the default SIGPIPE disposition
for every command except the long-running ones (`start`, `join`, `mcp`,
`guest-agent`, `guest-resume`), so the CLI ends the way `cat` and `grep` do:
killed by the signal, which the shell reports as 141.

**Alternatives.** Handle `EPIPE` in the output layer and exit 0. Every
`println!` in `src/cmd` (hundreds of them, plus `print_table`) would have to
become a fallible write, or the panic would have to be caught and
classified.

**Trade-off.** Under `set -o pipefail`, `legion job list | head -1` reports
141 instead of 0, exactly as `cat file | head -1` does. Servers keep ignoring
SIGPIPE, so a dropped peer stays an error they handle.

**Why.** Simplicity: one call at startup covers every print site, present
and future, and 141 is the convention pipeline scripts already know.

## 59. CLI handlers drop `#[instrument(err)]` across `src/cmd`, not only on `state query`

*Workstream: CLI and diagnostics (2026-10-12 re-run).*

**The call.** The ANSI `ERROR legion::cmd::state: error: …` line the re-run
saw came from `#[instrument(err, skip_all)]` on the command handler, and it
went to stdout on top of the `Error: …` line `main` prints on stderr. Every
human-mode error of every command printed twice. All 74 CLI handlers now use
`#[instrument(skip_all)]`, and docs/codebase-conventions.md says why.

**Alternatives.** Fix only `state.rs` (the reported command), or raise the
console filter for `legion::cmd` in one-shot mode, which would also hide
the `warn!` lines commands print on purpose.

**Trade-off.** A command's error no longer reaches `./logs/events.log` as a
JSON event. It still reaches the user, once, on stderr.

**Why.** The double print is the same bug everywhere, and one convention is
easier to keep than a list of exceptions.

## 60. User SQL errors are `InvalidArgument` only on the `state query` path

*Workstream: CLI and diagnostics (2026-10-12 re-run).*

**The call.** In `query_state_store`, a statement that does not compile
(`SqlInputError`), any other `SQLITE_ERROR`, and a second statement map to
`StorageError::InvalidInput` with SQLite's message (`no such column: x in
SELECT x FROM job at offset 7`), so the RPC answers `InvalidArgument`. The
store function also stops logging its errors at ERROR.

**Alternatives.** Classify `SQLITE_ERROR` as `InvalidInput` in
`StorageError::from_rusqlite` for every caller.

**Trade-off.** A second classifier next to the shared one.

**Why.** From Legion's own SQL, `SQLITE_ERROR` is a bug. The re-run's
`allocation.job_id` lookup was exactly that, and it must stay `Internal`
and logged at ERROR. Only raw user SQL makes it the caller's mistake.

## 61. Server disambiguation errors name the CLI flag first, the API field second

*Workstream: CLI and diagnostics (2026-10-12 re-run).*

**The call.** "name one with task_name" now reads `name one with --task
(task_name in the API): ws, idler`, and the same for `job scale` (`--group`
and `--task`), session task selection, and `state query`'s `--allow-write`.

**Alternatives.** Have the CLI pre-check (it would need the job's task list
first, an extra RPC per call), or rewrite server messages in the CLI by
string matching.

**Trade-off.** API, MCP and UI callers see a CLI flag before the field they
set. The MCP tools' argument is `task` too, so the flag name still reads
right there.

**Why.** Simplicity: the CLI is the surface people and agents use most, the
server already puts CLI fixes in its hints ("use --disk"), and keeping the
field name in parentheses leaves API callers a clear path.

## 62. MCP decodes node attributes in the tool layer; the proto keeps JSON text

*Workstream: CLI and diagnostics (2026-10-12 re-run).*

**The call.** `legion_node_list`, `legion_node_claims` and `legion_node_drain`
decode `configAttrs`, `fingerprintAttrs`, `dynamicAttrs` and `healthChecks`
from JSON text into objects before returning. `legion node list --json`
already decoded them (through `Node::from`), and a test now pins both shapes.

**Alternatives.** Change the `NodeMsg` fields to `google.protobuf.Struct` or
a typed message, a wire change for every client including the UI.

**Trade-off.** The gRPC `NodeMsg` still carries strings, so a raw gRPC caller
decodes them itself, as the proto comments say.

**Why.** The papercut was the agent surface. Fixing it where the JSON is
built keeps the wire format and the UI untouched.

## 63. A restoring node reads published parts with its own `contentStore` credential, and only for URLs under its own `contentStore.url`

*Workstream: Suspend and resume (durable checkpoints).*

**The call.** `ArtifactStager::fetch_sealed_into_store` checks whether a
part URL is under the node's own `contentStore.url` (same scheme, bucket,
and `endpoint`, under the prefix at a `/` boundary). If it is, the fetch is
signed with the node's `contentStore.auth`: the KV credential, read on every
fetch so a rotated STS credential is picked up the way the publisher picks
it up, or the ambient AWS chain when `auth` is omitted. The ambient case is
a new `Credential::AwsAmbient` variant, which the `s3://` fetcher signs with
instead of `with_skip_signature`. It cannot be written in KV. Any other part
URL is fetched anonymously, and a failure names the store:

```text
this node has no credential for content store s3://legion-content-use1/:
a restoring node signs part fetches only for URLs under its own
contentStore.url (here: s3://legion-content-usw2/), so this fetch was
anonymous. Give this node the same contentStore url and auth as the node
that published the snapshot
```

**Alternatives.**
- Apply cluster `artifacts.auth` matchers to part URLs, as the bug report
  first assumed already happened. The matchers live on the manager and
  apply at job admission. Part URLs never pass admission, so the manager
  would have to match them and send the result with every restore and warm
  (a new field on `SnapshotRestore` and `CheckpointRequest`). That would
  also let one node read many stores.
- A worker-side list of read credentials per store (`contentStore.readAuth
  = [{ match, kv }]`).
- Sign every part fetch with the ambient chain, not only fetches from the
  node's own store.

**Trade-off.** A node can read only the store it publishes to. A cluster with
one bucket per region cannot restore across regions, and placement does not
yet know which store a node has, so a resume can land on a node pointed at
another bucket and fail with the message above. Both are open in TODO.md and
stated in `docs/suspend-and-resume.md` ("Limitations"). The bug report asked
for an explicit `artifacts.auth` matcher to win over the store credential.
That is not built, because no matcher ever reached this path.

**Why.** Simplicity. The publisher and the reader use the same block with the
same credential, so "this node can publish to the store" means "this node can
restore from it", and the operator configures nothing new. Store identity
includes the endpoint and the prefix boundary, so the store's credential is
never sent to another bucket, a sibling prefix, or a different S3-compatible
endpoint that serves the same bucket name. Matchers sent over the wire would
add a wire field and a second credential path for one cross-region case
nobody has asked for yet. A blanket ambient chain would quietly sign
requests to buckets the operator never named, and on a non-AWS host it can
stall on instance-metadata lookups before it fails.
## 64. `job plan` previews a preemption that only waits on `victimExemptAge`, and the submit retries on the clock

*Workstream: Scheduling (2026-10-12 re-run papercut).*

**The call.** Seconds after `band-fill` landed, `job plan` for the high-band
claim failed "no available nodes", while the `job run` right after it
preempted a minute later. Both ran the same pass. The victims were younger
than `victimExemptAge`, so the plan failed honestly for that instant, and the
submit blocked and happened to be woken by later churn. Now a pass that finds
nothing re-runs with the guard lifted. When that finds room, the guard alone
is in the way:

- A dry run places on it and lists the victims with `eligibleInSeconds`,
  printed as `once past victimExemptAge in 23s`.
- An applying plan still evicts nothing. The blocked placement carries a
  `retry_after`, and the eval worker requeues the eval for that moment
  instead of blocking it. A partial plan queues a delayed `job_reschedule`.

**Alternatives.**
- Keep the plan's error and only reword it. The preview would still
  contradict what the submit does a minute later, which is the bug.
- Lift the guard in the plan without saying so. The plan would claim an
  eviction the submit cannot make yet.
- Leave the submit blocked and rely on capacity events. Nothing marks a
  victim getting older, so a quiet cluster could leave the claim blocked
  indefinitely, and the preview's "in 23s" would be wrong.

**Trade-off.** One more pass over the candidate nodes when the guard rejects
something and nothing else fits, and a timed requeue that runs even if the
cluster changed meanwhile (it re-plans and blocks again if so). The retry is
set only in `enforce` mode, since `observe` never acts on it.

**Why.** The plan's job is to preview what the submit converges to. Saying
"would preempt, in 23s" is that answer, and the clock-based retry makes it
true instead of probable. Simplicity: one more field on an existing message,
no new knob.

## 65. The macOS in-process worker stops its tasks on a clean shutdown, and the next start runs them again

*Workstream: macOS local dev (2026-10-12 re-run papercut).*

**The call.** SIGTERM to `legion start manager --run-worker` left Apple
containers running. On macOS only, the in-process worker now stops every task
on SIGTERM or SIGINT, after session checkpoints, with each task's stop signal
and `killAfter` inside a 30 second cap. It leaves the worker state file alone,
so the next start finds the processes gone and re-runs the same allocations.
The in-process worker also no longer calls `process::exit` on any platform:
it returns, and the manager waits for it before finishing its own drain and
persistence shutdown. Before, the worker's exit could cut the manager's
drain short.

**Alternatives.**
- Leave tasks running and rely on re-adoption, which already works while the
  `container run` process lives. It keeps VMs holding memory after the
  developer stopped the cluster, which is the complaint.
- Stop tasks on every in-process worker, Linux included. A combined
  manager-and-worker node under systemd would restart every task on each
  upgrade, breaking the documented `KillMode=process` re-adoption story.
- Add a flag to choose. More surface for a dev-mode default nobody has asked
  to turn off.

**Trade-off.** Containers restart across a dev cluster restart, so in-memory
state is lost (volumes keep their data). A SIGKILL or crash still stops
nothing, and there is no opt-out.

**Why.** On macOS the single process is the cluster a developer runs in a
terminal, and stopping it should stop its work, as `docker compose up` does.
Keeping the state file means nothing new has to happen at start: the existing
dead-pid path and the manager's desired set bring the work back. Ease of use
for local dev, at no cost to the Linux operations story.

## 66. An instance's node is read from its current allocation, not stored on the row

*Workstream: Stateful (2026-10-12 re-run papercut).*

**The call.** `InstanceInfo.node_id` was only ever set for sessions, so every
row-backed instance read `nodeId: null`. `map_instance_info` now loads the
current allocation once (it already did, for the loss diagnostic) and reports
its node. `instance list` gained a NODE column.

**Alternatives.** Add a `node_id` column to `instance` and keep it in sync on
every bind, rebind, and release.

**Trade-off.** One allocation read per bound instance on get and list, the
same read the loss diagnostic already paid.

**Why.** The allocation is the source of truth for where an instance runs. A
second copy would need every placement path to update it, and the spot check
shows what happens when one copy is set on only some paths. Simplicity.
