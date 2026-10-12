# Launch Spot-Check Fixes

> **Judgement calls:** [launch-spot-check-fixes-plan_calls.md](../calls/launch-spot-check-fixes-plan_calls.md)

**Status: IN EXECUTION.** Every finding of the 2026-10-11 pre-launch spot
check ([launch-spot-check-findings.md](../launch-spot-check-findings.md))
is fixed in [PR #17](https://github.com/mikenomitch/legion/pull/17) (open).
The per-finding outcome is in [§8](#8-outcome-per-finding); what was
deliberately deferred is in [§8.7](#87-deliberately-deferred), and each of
those items is open in [TODO.md](../TODO.md). Not yet re-run on the AWS
rig. The worker-plane half (P0-1) executes Phase 1 of
[worker-plane-auth-plan.md](worker-plane-auth-plan.md), which stays the
design of record for it.

## 1. Problem statement

The spot check ran every headline feature on real machines: three raft
managers with ACLs on and two spot workers on AWS, plus a local macOS
cluster. Single-node demos mostly worked. The multi-node story did not:

- A worker on another host lost exec, logs, discovery, and the mesh (P0-1).
- Batch work never finished, DAG steps all started at once, and restart
  policies were ignored (P0-2, P0-4).
- Losing or restarting one manager took every worker offline, and nodes
  that came back stayed unplaceable (P0-5, P0-6).
- A worker restarted after its join token expired could not come back
  (P0-8), and its old tasks kept running unmanaged (P1-5).
- Stock images (nginx, Postgres) failed in Linux containers (P0-7).
- Static ports hot-looped the scheduler, reschedules ignored the only
  healthy node, and job status said `done` for running services (P1-2,
  P1-3, P1-8).

Each of these is what a first external user does in their first hour: add a
second worker, restart something, run nginx, run a pipeline. A user who hits
any of them concludes the orchestrator does not work.

## 2. Prior art

| Area | Nomad | Kubernetes | Take |
| --- | --- | --- | --- |
| Batch lifecycle and restarts | `type = "batch"`; the `restart` block (`attempts`, `interval`, `delay`, `mode = fail\|delay`) runs on the client | Job `restartPolicy: OnFailure\|Never`, `backoffLimit` | The worker owns the restart decision, so the worker must receive the policy. |
| Step dependencies | Lifecycle hooks (`prestart`, `poststart`, `poststop`), gated on the client | init containers, gated by the kubelet; Argo for DAGs | A dependency is satisfied by an observed outcome, never by a plan. |
| Client failover | Clients hold a server list, learn it from every `Node.UpdateStatus` reply, and rotate on failure | kubelets talk to a load-balanced API endpoint | Workers learn the manager set from the managers and rotate. |
| Liveness after a leader change | The new leader resets every node's heartbeat TTL on election (`initializeHeartbeatTimers`) and writes status transitions it observes | node lifecycle controller re-evaluates conditions | A new leader reconciles stored availability with what it sees. |
| Re-joining after credentials expire | Clients persist their node secret in the client state DB | Bootstrap tokens are one-shot; the kubelet then rotates its own client cert | The join secret is for joining; the node's own credential is for coming back. |
| Orphaned work after an agent restart | The client re-attaches to tasks from its state DB and kills what the server no longer wants | the kubelet garbage-collects containers it does not know | A restarted worker adopts or kills, never ignores. |
| Container defaults | Docker driver: Docker's 14 default capabilities, an `cap_add`/`cap_drop` knob | `securityContext.capabilities.add/drop` over the runtime default | Docker's set is the compatibility floor stock images assume. |
| Static ports and partial placement | Places what fits, records the rest as a blocked eval with "port collision" in the placement failure | Pods stay `Pending` with a reason | Place what fits, report the rest, never reject the whole plan. |
| Rescheduling | Penalizes previously used nodes (`node-reschedule-penalty`) rather than excluding them | the scheduler has no memory of the old node | Penalty, not exclusion. |
| Ids | UUIDs | UIDs | An id is never reused. |
| Logs after removal | Terminal allocs and their logs live until GC | `kubectl logs --previous` | Logs outlive the allocation record for a while. |

## 3. Jobs to be done

**Cluster operator**
- When I add a second machine, I want it to run work, serve logs, and join
  the mesh like the first one, so growing past one box is not a project.
- When I restart or lose one manager, I want my workers to keep reporting to
  the others, so one box going down is not an outage.
- When I reboot a worker months after it joined, I want it to come back on
  its own, so short-lived join tokens are safe to use.

**Job author**
- When I write a pipeline of steps, I want each step to start only after the
  steps it depends on actually succeeded, so a step never runs on missing
  input.
- When I run a stock image like nginx or Postgres, I want it to behave as it
  does under Docker, so I do not rebuild images for Legion.
- When my service uses a fixed port, I want updates and reschedules to just
  work on a small cluster, so a fixed port is not a trap.

**On-call responder**
- When something fails, I want the real error (a registry refusing a pull)
  where I look first, and the logs of a task that is already gone, so I can
  say why it broke.
- When I look at a job, I want its status to say whether it is running, done,
  or stuck, so I don't trust a `done` that means nothing.

**Autonomous agent**
- When I call a command, I want `--json`, consistent field names, and a
  non-zero exit on a missing key, so I can branch on the result without
  parsing prose.

## 4. Goals and non-goals

**Goals.** Every P0 and P1 finding fixed at its root cause, with a
regression test. Every P2 fixed or documented. Public docs describe the new
behavior, defaults, and limits.

**Non-goals.** The later phases of the worker-plane plan (host guard, gated
by default, the privilege floor), the UI, the MCP server beyond keeping it
consistent with the CLI, and re-running the AWS rig (the next spot check
does that).

## 5. What exists today

See the findings doc for the code each finding was traced to. Section 8
records the root cause each fix found, which in several cases differed from
the report's reading.

## 6. Design

The fixes group into six workstreams, each shippable on its own:

1. **Worker plane** (P0-1). Phase 1 of the worker-plane plan.
2. **Task lifecycle on the worker** (P0-2, P0-3, P1-1, P1-4, P1-9, P2-7,
   P2-8).
3. **Scheduler correctness** (P0-4, P1-2, P1-3, P1-8).
4. **Manager availability** (P0-5, P0-6, P0-8, P1-5, P2-5, P2-10).
5. **Linux container compatibility** (P0-7, P1-6, P2-6, P2-9, P2-11).
6. **CLI and diagnostics** (P1-7, P1-9, P2-1 to P2-4, P2-6, P2-7).

The surface each one adds is in §8 and in the public docs it links.

## 7. Testing and rollout

Every fix carries a unit or integration test that fails on the old code.
The scheduler changes carry before/after numbers from
`benchmarks/micro/scheduler_bench.rs`. Legion has no installed base, so
changed defaults (container capabilities, worker bind address, job status
vocabulary) change for every cluster; the PR description lists each break.

The next AWS spot check re-runs `tests/spot-check/smoke.sh` without the
shims to confirm the fixes on real machines.

## 8. Outcome per finding

Each entry gives the root cause the fix found (where it differs from the
report's reading) and what changed. Tests named here fail on the old code.

### 8.1 Worker plane (P0-1)

- **P0-1.** The worker served `127.0.0.1:4848`, registered
  `http://127.0.0.1:4848`, and checked no caller. Phase 1 of
  [worker-plane-auth-plan.md](worker-plane-auth-plan.md) shipped: the worker
  binds `0.0.0.0` and advertises an auto-detected `https://` address
  (`--advertise-addr`, `LEGION_WORKER_ADVERTISE_ADDR`); the listener is mutual
  TLS with per-RPC caller classes (`WORKER_RPC_CALLERS`); one manager-side
  factory (`src/manager/worker_plane.rs`) dials every worker and verifies
  `<node>.node.legion`; node keys are issued by CSR and never leave the node.
  `PROTOCOL_VERSION` is 3. Tests: `tests/worker_plane.rs`.
- **Found along the way.** Speculative edge-boot dispatch read
  `protocolVersion` from the wrong attribute map and never fired for real
  workers; it now does, for the first time.

### 8.2 Task lifecycle on the worker (P0-2, P0-3, P1-1, P1-4, P1-9a, P2-7a, P2-8)

- **P0-2.** The dispatcher never sent the task's lifecycle or restart policy,
  so every task restarted as a service. `TaskAllocation` carries
  `task_lifecycle` and `task_restart`; a batch task that exits 0 completes
  once. A `restart` block without `policy` takes the lifecycle default
  (`onFailure` for batch).
- **P0-3.** Artifact staging threads ran outside the tokio runtime, so the
  health monitor's `tokio::spawn` panicked. They now enter the runtime; edge
  boot's provisional launch had the same bug.
- **P1-1.** The resource governor was built before its config arrived. It is
  configured explicitly before any allocation starts.
- **P1-4.** Allocations pushed by `PostWork` never created their gated
  namespace (only the heartbeat path did). Both paths now create it.
- **P1-9a.** Failure reasons and events carry the task's last stderr lines.
- **P2-7a / P2-8.** The plugin dir defaults next to the binary; exec sessions
  get the task's environment.

### 8.3 Scheduler correctness (P0-4, P1-2, P1-3, P1-8)

- **P0-4.** A dependency counted as satisfied when it was merely planned in
  the same pass, and nothing re-evaluated the job when a step exited. Only a
  worker-reported status now satisfies `success`/`complete`, dependents are
  held until then, and a terminal report queues the job's next eval.
  Finished allocations stop holding capacity and static ports.
- **P1-2.** Only the apply step checked static ports, so the planner kept
  producing plans apply rejected, about twice a second. The planner tracks
  ports per node; a plan places what fits and reports the rest as unplaced;
  freed capacity (a stop, a scale-in, a finished task) retries jobs that are
  short; a static-port rollout that cannot surge stops the old allocation
  first.
- **P1-3.** A reschedule plans without the failed node first, then with it
  when nothing else fits.
- **P1-8.** Main shipped a derived job status in
  [job-status-plan.md](job-status-plan.md) (#21) while this work was in
  flight. Its derivation replaced this branch's at merge (calls entry 10).
  What this PR adds: `job run` with an unchanged spec starts a stopped,
  failed, or idle job again instead of answering "no update needed".

### 8.4 Manager availability (P0-5, P0-6, P0-8, P1-5, P2-5, P2-10)

- **P0-5.** Two causes. The forwarding receiver compared the physical raft
  group to the op's logical lane, refusing every forwarded status-lane write
  under `single`; and workers knew one manager. Both sides now use
  `raft::physical_group`; workers learn every manager from registration and
  heartbeat replies, persist the set, and fail over.
- **P2-5.** Followers replay a write the leader must handle to the leader
  (`src/manager/leader_forward.rs`, one hop), which also fixes
  `WorkerRegister` on a follower.
- **P0-6.** Re-registration kept `unavailable`; the repair heartbeat hit the
  P0-5 bug; and loss/recovery are two writes a leadership change can split.
  A status-leader reconcile repairs availability it has evidence for. Loss no
  longer overwrites an operator's `drained`.
- **P0-8.** The worker persists its node token and re-registers with it; the
  manager does not rotate a token a node presents for itself.
- **P1-5.** Partly fixed (root cause not reproduced): restore kills a task's
  leftover process group, and the systemd guidance is documented.
- **P2-10.** A restarted voter campaigned before any leader reached it. It
  now starts with elections off until it hears from a leader or a bounded
  wait passes. `transfer-leader` and `step-down` work through remote
  elections (P2-2).

### 8.5 Linux container compatibility (P0-7, P1-6, P2-6a, P2-9, P2-11)

- **P0-7.** Containers get Docker's default capabilities minus `NET_RAW`,
  adjustable with `security.capabilities`, and bounded by the worker type's
  `container.allowCaps` (default: the same 13). Layer extraction keeps
  ownership, setuid bits, and file capabilities; the stdio logs are chowned
  to the container user; `nofile` defaults to 1048576 (`ulimits`). Verified
  with stock nginx and the Postgres entrypoint under runc.
- **P1-6.** Already fixed on main; a regression test is added.
- **P2-6a.** The macOS 200 MiB floor is checked at submit and before start.
- **P2-9.** Image blobs go through the node content store, so a digest the
  node holds costs no registry request. A blob file left unflushed meant the
  store sometimes refused it as a digest mismatch; pulled blobs are now
  flushed first.
- **P2-11.** New `docs/installation.md`.

### 8.6 CLI and diagnostics (P1-7, P1-9b, P2-1 to P2-4, P2-6b, P2-7b)

- **P1-7.** Main already rebuilt `AUTOINCREMENT` for most paths; the `lanes`
  table-scoped snapshot did not carry `sqlite_sequence`. It now does, and the
  worker claims each log dir with an `owner.json`.
- **P1-9b.** Removed allocations are kept in `allocation_history` for 24h and
  their logs stay readable (`--include-removed`).
- **P2-1.** The quickstart works outside a checkout.
- **P2-2.** Precondition errors keep their cause and gRPC code.
- **P2-3.** `kv get` on a missing key exits 1 with `NotFound`.
- **P2-4.** Every command accepts `--json` (a test walks the tree), and JSON
  is camelCase throughout.
- **P2-6b / P2-7b.** The "node not found" and log-sink noise came from an
  `ERROR`-level miss on first registration and a sweep racing log-dir
  creation; both are gone.

### 8.7 Deliberately deferred

Each item is open in [TODO.md](../TODO.md) under "Follow-ups from the
2026-10-11 spot-check fixes".

- Re-run `tests/spot-check/smoke.sh` on the AWS rig without the shims.
- Worker plane: CA pin in join tokens (Phase 4), two-step CA rotation,
  `Worker/Probe`, the exec `ticket` class, an encrypted worker-to-manager
  channel; an edge-boot smoke test now that speculative dispatch fires.
- Raft: pre-vote for partitioned members; failover dials raft advertise
  addresses, which must be reachable from workers.
- Containers: capability-aware placement, the per-author ACL `host` grant,
  user namespaces and seccomp, `command` vs `ENTRYPOINT` differing between
  macOS and Linux; the release versions in `docs/installation.md` need
  checking against upstream.
- Lifecycle: batch exit reports are not persisted across a worker restart;
  `legion exec` into a gated exec task runs in the host netns.
- Scheduler: destructive static-port rollouts race the old process's
  graceful stop; no-op `rollout_tick` events; `batch_task_outcome_tx`
  ignores scale overrides.
- Logs: manager (24h) and worker (7 days) retention differ.
