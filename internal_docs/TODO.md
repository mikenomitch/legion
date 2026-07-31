# TODO

The running to-do list for Legion. **Open work is at the top**, grouped by area;
everything already shipped is in [Done](#done) at the bottom.

How to use it:

- Each group links its plan in [`plans/`](plans/) and its public page in
  [`docs/`](../docs/README.md) where one exists. If an item needs a plan and
  doesn't have one, that's noted.
- **When you finish something here, check it off and move it to Done in the same
  change.** A todo that outlives its feature is worse than no todo.
- Items that came out of a plan's "deferred" or "follow-up" section say so, so
  the plan stays the source of truth for the design.

---

## In flight (open PRs)

| PR | What it lands | Notes |
| --- | --- | --- |
| (branch `claude/pre-benchmark-features-mlgzyx`) | Pre-benchmark hardening: apply-time resource re-validation, raft engine metrics, worker /metrics + OTLP push, eval-path fixes, scheduler plan-path cleanups, `legion job scale` (autoscaler phase 1) | [calls](calls/pre-benchmark-hardening_calls.md) · [autoscaler calls #5/#7](calls/autoscaler-plan_calls.md) |
| [#162](https://github.com/mikenomitch/legion/pull/162) | Lanes hardening: worker-write forwarding, split-tolerant submits, adversarial fixes | Plan §8 + calls doc in [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md) |
| [#143](https://github.com/mikenomitch/legion/pull/143) | Note the node-level builtin resource override gap | Docs only; the fix itself is a todo below |
| [#84](https://github.com/mikenomitch/legion/pull/84) | Instrument DB hot paths, batch task-status updates | Old; rebase or close |
| [#108](https://github.com/mikenomitch/legion/pull/108) | Versioned job templates | Superseded by [#151](https://github.com/mikenomitch/legion/pull/151) (merged) — close |

---

## Scheduling & placement

- [ ] **Late-binding plan apply, Phase 4 backlog** (each optional, gated on
      the fallback-rank / `plan_apply_total{rejected}` metrics the shipped
      phases added): capacity-aware candidate skipping, per-component partial
      commit + eval refill, striped disjoint candidate lists for identical
      replicas, and a serialized/batched apply lane for physically-full
      pools — [plan §8](plans/late-binding-plan-apply-plan.md). The shipped
      phases are in Done below.
- [ ] **Fused top-k candidate scoring.** Ranked candidate lists materialize a
      scored vector per placement; the one-candidate path (`plan_candidates:
      0`) uses the fused argmax loop that does not. A fused top-k loop would
      give the default path the same win — worth doing when the
      candidate-construction benchmark (+2–10% plan-only) says it pays
      ([calls #20](calls/late-binding-plan-apply-plan_calls.md)).
- [ ] **Capacity exhaustion still fails evals terminally.** The late-binding
      requeue backstop only covers *conflict* errors; a replan that finds no
      eligible capacity at all ("no available nodes …") still ends the eval
      in `failed`. Blocked-and-retried capacity waits are the gang-scheduling
      plan's blocked-eval machinery
      ([plan](plans/gang-scheduling-plan.md)) — when that lands, consider
      routing plain single-task capacity failures through it too.
- [ ] Worker groups + taint/toleration equivalent. Node pools exist as an ACL
      and config concept but nothing scheduling-side consumes them.
- [ ] Priorities and eviction/preemption, with eviction toggled at the pool level
      — [plan](plans/preemption-and-eviction-plan.md) (IN EXECUTION: stop causes
      → graceful termination → priority bands → plan-atomic preemption →
      governance; pool toggle is `schedulerSettings.preemption.disabledPools`)
  - [x] **Phase 1a — stop causes.** Every allocation removal records *why*
        (`stop`/`update`/`drain`/`reschedule`/`destroy`/`node_lost`), delivered
        to the owning worker on its next heartbeat and carried in
        `allocation.removed` event metadata.
        [plan §Phase 1a](plans/preemption-and-eviction-plan.md) ·
        [docs](../docs/graceful-stop.md)
  - [ ] **Phase 1b — graceful termination.** `stop { signal, killAfter }`,
        per-runtime SIGTERM→SIGKILL ladders, the `LEGION_STOP_NOTICE_PATH`
        file, `allocation.stopping`, stops-before-starts reconcile ordering.
        **Blocked on a worker-side terminating set**: `reconcile_allocations`
        runs under the task-manager mutex inside the heartbeat loop, so a
        blocking grace wait would stall heartbeats and the self-fence watchdog
        and get the node declared lost. `stop_with` must signal and return,
        with a reaper doing the SIGKILL — and teardown (notably
        `volumes.unmount_for_allocation`) must move behind actual process
        exit. [plan §Phase 1b](plans/preemption-and-eviction-plan.md)
  - [ ] Decide Phase 1b's grace-vs-latency question: stops-before-starts puts a
        victim's shutdown on the preemptor's critical path, so a task ignoring
        SIGTERM delays an incident-response scale-up by up to `killAfter`.
        Either a shorter preemption-specific grace, or start the preemptor once
        capacity is *accounted* rather than *physically free*.
  - [ ] Add a plugin ABI version symbol. `plugins/tasks/` loads a bare
        `extern "C" fn() -> Box<dyn TaskPlugin>` with no version guard, so a
        stale third-party dylib mismatches silently instead of failing loudly.
        Phase 1a dodged this by not breaking the ABI (`stop_with` defaults to
        `stop`); the next trait change will not have that option.
  - [ ] Band governance ships as a `bands` selector on `job` ACL rules, not
        `operator:write` — which also delivers per-namespace band caps, so
        that is no longer a separate Phase 4 item. Needs band normalization
        before `enforce` (a selector never matches a request lacking the
        attribute), coverage on the update and template-render paths, and
        explicit cases in `tests/acl_endpoints.rs` — the
        `every_manager_rpc_is_classified` test cannot catch a dropped
        conditional check inside an existing endpoint.
  - [x] At least think through fair-share policies — find the 80/20 rather than
        building a full hierarchical scheduler. Thought through in the plan's
        prior art + non-goals: the 80/20 is priority bands now, per-namespace
        band caps later (phase 4); fair-share/hierarchical queues explicitly
        deferred (Kueue's policy-matrix issue history is the argument against).
- [ ] Gang scheduling — `gang` on a task makes its replicas place
      all-or-nothing, with capacity failures becoming blocked-and-retried
      evals (woken on `node_join`) instead of terminal eval failures —
      [plan](plans/gang-scheduling-plan.md) (PLANNED: gang contract + blocked
      evals → `min` quorum + `placementTimeout` → whole-gang recovery →
      blue-green updates; reservation/backfill anti-starvation and
      gang-grained preemption deliberately deferred, the latter coordinated
      with the preemption plan)
- [ ] Schedule onto the same node as an *existing job*. Soft `same_node_as`
      colocation works within one job spec
      ([scheduler/policy.rs](../src/manager/scheduler/policy.rs)); cross-job
      placement doesn't exist.
- [ ] Node-level builtin resource overrides are silently ignored —
      `ResourcePool::from_node` skips any builtin name, so `resources: { cpu: 40000 }`
      in a worker config disappears without a warning
      ([#143](https://github.com/mikenomitch/legion/pull/143)). Open questions: does
      an override replace the fingerprinted value or act as a second ceiling, and
      how does it compose with `reservedResources`? Either way an ignored key
      should warn or fail validation.
- [x] Node drain as a first-class operation — `legion node cordon|drain|uncordon`,
      eval-mediated eviction that respects stateful move protection
      ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] Fix the slowest bits of the scheduler using the benchmarks in
      [docs/testing-and-benchmarks.md](../docs/testing-and-benchmarks.md).
      Candidate construction is index-served
      ([docs/scheduling-indexes.md](../docs/scheduling-indexes.md)); the
      per-plan snapshot is fixed twice over (Arc'd nodes + cached free
      pools — [plan](plans/scheduler-plan-snapshot-plan.md) — then the
      cached sorted node list + Arc'd pool values, 2026-07-30); the
      plan-submission insert is fused into the apply (one quorum commit,
      2026-07-29); the O(fleet) `version_sample` is lazy (D4) and plan
      applies group-commit (D5), 2026-07-30; eval handling (including
      `plan_with_conflict_retries`' backoff sleeps) runs on the blocking
      pool instead of parking tokio runtime workers (2026-07-30); the
      remaining measured candidates all landed 2026-07-30 — shared
      per-task `ConstraintSet`/`PermitSet` parsing, settings parsed once
      at `Scheduler::new`, and the index shard-sort skipped when the
      caller discards the order (before/after in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md)).
      Further wins want profiling, not this list.
- [x] Scheduler performance measurement stack
      ([plan](plans/scheduler-performance-plan.md),
      [calls](calls/scheduler-performance-plan_calls.md),
      [docs](../docs/testing-and-benchmarks.md)) — scenario schema v2
      (heterogeneous `node_profiles` + mixed `job_types` with
      resources/constraints, per-scenario SLOs), four realistic scenarios,
      shared runner, the scenario × backend matrix
      (`tests/scheduler_scale_benchmark.rs`: local_disk / raft1 / raft3 /
      streaming) with recorded release numbers, `PlanTimings` phase
      instrumentation + `legion_scheduler_plan_phase_seconds`
      ([docs/metrics.md](../docs/metrics.md)), and the fused
      `submit_and_apply_plan` (before/after in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md)).
- [x] Architecture review proposals for scale
      ([plan](plans/scheduler-architecture-review-plan.md)) — all four
      accepted proposals shipped 2026-07-30 via
      [plan-path-scale-levers](plans/plan-path-scale-levers-plan.md)
      ([calls](calls/plan-path-scale-levers-plan_calls.md)) and
      [bounded-candidate-scoring](plans/bounded-candidate-scoring-plan.md)
      ([calls](calls/bounded-candidate-scoring-plan_calls.md)): D4 lazy
      version sample + read-model generation fence, D5 plan-apply group
      commit (leader-elect coalescer), D6 as cheap snapshots (cached
      sorted node list, Arc'd pools), D1 opt-in `score_sample` with the
      packing-quality gate. Docs in
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md);
      before/after in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
      D2 (cross-manager scheduling) and D3 (in-memory state) remain
      recorded keep/defer decisions; D8's architecture choice still waits
      on AWS-test data.
- [x] Heartbeat path at scale
      ([plan](plans/heartbeat-scale-plan.md),
      [calls](calls/heartbeat-scale-plan_calls.md),
      [docs](../docs/architecture-managers-workers.md)) — heartbeat scale
      benchmark (`tests/heartbeat_scale_benchmark.rs`), status-lane
      heartbeat group commit with per-id verdicts, skip-reprojection for
      bare refreshes (leader + follower projection), integer liveness
      clock (`last_heartbeat_unix`), batched lost-node recheck, partial
      index for the volume-destroy feed, worker heartbeat jitter and
      `heartbeatSeconds` worker-type config. Numbers in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
      Deferred (noted in plan): per-node lost windows negotiated at
      registration; batching `mark_nodes_as_lost`'s per-node
      availability-sync + eval enqueues after a mass failure; an ACL
      token-identity cache for worker-plane RPC rates.
- [ ] AWS comparative benchmark vs Nomad/K8s
      ([plan](plans/aws-comparative-benchmark-plan.md),
      [calls](calls/aws-comparative-benchmark-plan_calls.md)) — phases
      0-1 built and locally validated 2026-07-30; campaigns (phases 2-5)
      await an AWS account:
  - [x] Phase 0: `legion start worker --simulate N`
        ([mini-plan](plans/worker-simulation-plan.md),
        [docs](../docs/worker-simulation.md)) — one process, N wire-real
        nodes from a `node_profiles` file, real cadences, stub dispatch
        server; end-to-end test `tests/worker_simulation.rs`.
  - [x] Phase 1 rig, everything short of the cloud:
        [`benchmarks/aws/`](../benchmarks/aws/README.md) — Terraform
        (validate-clean; system-agnostic boxes + spot sim fleet + S3),
        per-system configs (Legion A lanes / Legion B streaming+S3 lease
        / Nomad / kubeadm+KWOK), scenario→jobs translator, measuring
        driver with fixture-tested counting math and a `watch`
        fault-drill mode, campaign runner + collect/fairness-record
        scripts, piggyback experiment scripts (storm, failover,
        streaming-RPO, 24h soak, upgrade-under-load), Grafana boards,
        projection fitting (`analysis/fit_curves.py`), CI gate
        (fixture tests + terraform validate), and the local no-AWS smoke
        (`scripts/local-smoke.sh`).
  - [ ] Phase 1 exit on real AWS: `bringup.sh` + 1k-node
        `run-campaign.sh --system legion-a --smoke` (needs an account).
  - [ ] Phases 2-5: Legion A/B campaigns, Nomad (verify nodesim `mock`
        driver + install script), K8s (verify kubeadm/KWOK install +
        watch parsing), 20k validation burst, analysis + REPORT.md.
- [ ] Scheduling-index follow-ups deferred from
      [plan](plans/scheduling-index-plan.md) (see its calls doc):
      index-served candidates for multi-task **group** co-placement (v1
      groups still scan — calls entry 10); pluggable index kinds;
      `count.per` served by partition enumeration; device-set
      (`ResourceValue::Items`) dims; task-level `index_hint`;
      incremental orthant enumeration with early termination.
- [ ] Worker-fingerprinted `disk` as a built-in schedulable resource — the
      default `fit` index already dims on `resources.disk` and picks it up
      automatically wherever it exists; today that is only operator-declared
      `config_attrs.resources.disk` (plan open question, lean yes; also
      leaned on by the artifact-cache plan's disk accounting).
- [ ] Add to AGENTS.md that scheduler changes ship with a before/after perf
      benchmark
- [x] Close the last plan-fencing window (2026-07-28): the scheduler now
      samples epochs from an in-memory mirror kept coherent with the
      allocation cache (epochs read before allocations, published together
      under one lock; the raft changeset projection refreshes both from the
      epoch rows it carries; rejections self-heal the mirror). An
      adversarial pass showed the old DB-sampled gap was NOT microseconds
      under raft — it spanned the whole changeset projection — and was a
      real oversubscription. Plan apply also re-validates node
      *availability* on the fresh row, closing the cordon/lost race the
      epoch CAS structurally cannot see.
      [Calls #4](calls/durability-and-ops-execution-plan_calls.md)
- [x] Apply-time re-validation of node resource TOTALS (2026-07-30): the
      plan-apply transaction reconstructs each placement node's free pool
      from the fresh node row and the allocation rows the transaction can
      see (`ResourcePool::free_from_rows`), mirroring the planner's ledger
      (Fresh plans exclude the replaced job's own rows), and rejects
      placements a capacity shrink no longer fits — closing the window
      where a worker re-registration shrinking capacity (or
      `reservedResources`/`maxAllocations` changes) between snapshot and
      apply bumped no epoch and was never re-checked. Ports were already
      apply-safe; resources now match. Found by the 2026-07-28 adversarial
      pass.
- [ ] Volume-pin staleness in planning: pins are read mid-plan-build with
      no epoch coupling, so a pin invalidated mid-plan (e.g.
      `volume delete --force`) can still direct a placement; later
      reconciliation covers it. Low severity, same pass.
- [x] A raft-mode CONCURRENT-planning oversubscription test through the
      real `Scheduler::plan_and_apply` path (2026-07-30):
      `concurrent_planners_cannot_oversubscribe_under_raft[_lanes]` in
      [tests/raft_apply_roundtrip.rs](../tests/raft_apply_roundtrip.rs)
      drives two barrier-released planners against raft's
      commit-then-project ordering under both topologies — the exact
      window the epoch mirror closes. Regression insurance; the local_disk
      twin is `concurrent_plans_cannot_oversubscribe_a_node`.
- [ ] Let a node override its builtin resource capacity (`cpu`, `memory`, `cores`)
    - `ResourcePool::from_node` skips builtin names when reading a node's
      `resources` block, so `resources: { cpu: 40000 }` in a worker config is
      silently ignored rather than applied or rejected. Capacity can only come
      from the fingerprint today, which leaves no escape hatch when a platform
      fingerprints badly (see the Apple Silicon CPU frequency case) and no way
      to deliberately under- or over-subscribe a node.
    - Decide whether an override replaces the fingerprint or is a separate
      ceiling, and how it composes with `reservedResources`. Whichever way it
      lands, an ignored key should warn or fail validation instead of passing
      silently.

## Task runtimes

Plans: [task-runtime-plugins](../docs/task-runtime-plugins.md).

- [x] **Task process supervision** — the worker reaps task children, applies a
      new `restart` policy (`always | onFailure | never`, `delay`,
      `successExitCodes`) sharing one budget with health-check restarts, and
      reports terminal outcomes so a batch task's `Success` comes from its exit
      code instead of its placement (`PROTOCOL_VERSION` 2, with a per-node
      legacy fallback for older workers) —
      [plan](plans/host-process-supervision-plan.md) ·
      [calls](calls/host-process-supervision-plan_calls.md) ·
      [docs](../docs/task-supervision.md)
- [x] **Worker-owned cgroups for `exec`** — opt-in `resourceEnforcement` on the
      worker config turns a task's `resources` into `memory.max`/`cpu.max` in a
      cgroup Legion creates and removes per allocation; an explicit
      `config.cgroup` still means "join this one"
      ([docs](../docs/task-supervision.md#resource-enforcement-cgroups))
- [x] **`exec` hardening block** — `noNewPrivileges`, capability bounding-set
      drops, `privateTmp`, `umask`, `nice`, applied in privilege order around
      the user switch ([docs](../docs/task-supervision.md#process-hardening))
- [ ] Deferred from the supervision plan: privileged runtime tests for the cgroup
      and hardening paths (CI covers derivation, validation, and the unconfined
      fallback; the root-only paths are manual against a Linux worker container);
      PID/IPC/UTS namespaces and seccomp for
      `exec` (a PID namespace needs a double-fork, which changes the pid the
      worker tracks — see
      [calls entry 10](calls/host-process-supervision-plan_calls.md)); enforce
      `resources.disk`; scheduler preference for enforcing nodes.
- [ ] Possible, not planned: activation and readiness for `exec` — an
      `sd_notify`-compatible readiness socket, `WatchdogSec`-style pings, and
      socket activation via pre-bound fds
      ([plan §8 phase 4](plans/host-process-supervision-plan.md))
- [ ] A **WASM** runtime
- [ ] A **`converge`** runtime — idempotent host resources (package, file,
      template, directory, user, service, mount) with per-resource
      `unchanged | changed | failed` reporting, i.e. the Chef-replacement shape —
      [plan](plans/host-convergence-plan.md) ·
      [calls](calls/host-convergence-plan_calls.md) (PLANNED: file-shaped
      resources → system resources → node-config jobs → why-run/drift →
      `host` ACL resource). Phase 1 depends on the exit-status work above.
- [ ] **`container` core gaps: exec on Linux, signals + stop knobs, Nomad-style
      CPU/memory enforcement, file delivery** —
      [plan](plans/container-runtime-exec-and-signals-plan.md) (PLANNED).
      Today `legion exec`/actions/UI/MCP fail against container tasks on Linux
      (exec was deferred for the OCI runtimes), no platform can signal a
      container task (`changeMode: signal` rejected at admission), teardown
      has no grace window or knobs (Linux is `delete --force`, SIGKILL),
      declared `resources` set no cgroup limits on Linux (`cpu` MHz enforced
      nowhere), and the unmounted task dir blocks KV/identity *file* delivery
      and file/tree artifacts.
  - [ ] Phase 1 — exec via `<runtime> exec` for youki/runc/crun
  - [ ] Phase 2 — `TaskPlugin::signal()` seam + `container kill`/`<runtime> kill`
  - [ ] Phase 3 — graceful stop + task-level `stopSignal`/`stopGracePeriod`
        fields (defaults SIGTERM/10 s, admission-capped; container both
        platforms + exec runtime)
  - [ ] Phase 4 — the `resource_enforcement()` interface (`Unsupported`/
        `WorkerCgroup`/`RuntimeApplied` + worker-computed `ResourceAssignment`)
        with Linux memory hard limits and `cpu` MHz → `cpu.weight`
        (share-slice cgroup, OOM-kill annotation, `cores`-xor-`cpu` and
        whole-`cores` validation)
  - [ ] Phase 5 — exclusive `cores` via cpuset pinning (worker core
        allocator, `legion/share`/`legion/reserve` slices, restart recovery)
  - [ ] Phase 6 — `exec` runtime adopts the interface (`WorkerCgroup`): one
        node-wide fence for container + exec tasks, real memory limits for
        exec, `cgroup.kill` teardown; explicit `config.cgroup` still wins
  - [ ] Phase 7 — task dir mounted at `/legion/task` + translated env; flips
        KV file injection, identity file delivery, and file/tree artifacts to
        supported (container file paths must live under the mount)
  - [ ] Follow-on (not in the plan): `sandbox` adopts `resource_enforcement()`
        as a `RuntimeApplied` consumer — pin vCPU threads to the assignment's
        cores, size the guest from its memory limit. The honest test of
        whether the three modes are the right cut (plan Q4).
  - Deliberately out of scope there (still open, see the plan's non-goals):
    Linux port mapping/network namespaces, NUMA/SMT-aware core placement,
    fencing non-Legion host processes off reserved cores, a delegated
    (non-root) cgroup hierarchy root, alloc-dir sharing
  - **Overlaps shipped work — reconcile before building phase 6.** `exec`
    already creates and sizes a per-allocation cgroup from `resources`, opt-in
    per worker via `resourceEnforcement` in the worker config
    ([host process supervision plan](plans/host-process-supervision-plan.md)
    §6.5, [docs](../docs/task-supervision.md#resource-enforcement-cgroups),
    `src/task_plugins/exec_isolation.rs`). Phase 6 should adopt that code as
    the `WorkerCgroup` implementation rather than write a second one, and
    decide one opt-in story for both runtimes: the exec work is off by default
    per worker (a `resources` block was only ever a scheduling estimate), which
    the container plan's default-on stance has to either match or explicitly
    overrule.
- [ ] `exec`: unveil support
- [ ] `exec`: network namespaces (netavark? CNI?) — see *Networking* below
- [ ] **Sandbox snapshots, fast start, and forking** —
      [plan](plans/sandbox-snapshots-and-fast-start-plan.md) (PLANNED; §0 storage
      convergence shipped with the sandbox runtime)
  - [ ] Phase 1 — disk snapshots (volume `snapshot` op, `local-dir` reflink,
        snapshot table + CRUD RPCs/CLI/MCP, ACL resource)
  - [ ] Phase 2 — restore in place (`RestoreInstance`, quiesce hooks wired to actions)
  - [ ] Phase 3 — memory snapshots (Firecracker pause/create/resume, `compat`
        constraints in the scheduler)
  - [ ] Phase 4 — fast start (regional snapshot stores, `legion snapshot warm`,
        warm-node scoring)
  - [ ] Phase 5 — forking (volume `clone`, identity disk, `legion-guest-agent`,
        `instance create --from-snapshot --count N`)
  - [ ] Phase 6 — lazy memory (UFFD handler, remote page sources)
- [ ] Split a task runtime's implementation into parts — isolation vs. runner vs.
      artifact consumption? (idea, needs a plan). The container-runtime plan's
      `resource_enforcement()` interface carves off the first slice of this —
      resource placement moves to worker core, runtimes consume a computed
      assignment — so revisit this idea once that has two real consumers.
- [ ] Shared files between tasks in the same allocation —
      [proposal](plans/shared-alloc-dir-proposal.md), unimplemented, and blocked
      on allocations holding more than one task
- [ ] Use a virtual filesystem for the alloc directory (idea)

## Rollouts

Plan: [rollout-strategy-plugins](../docs/rollout-strategy-plugins.md) · docs: [rollouts.md](../docs/rollouts.md)

- [ ] A **blue-green** strategy
- [ ] A **canary** strategy

## Artifacts

Plan: [artifact-system-plan.md](plans/artifact-system-plan.md) · docs: [artifacts.md](../docs/artifacts.md)

- [ ] **Artifact cache + pre-warming/pre-pull + scheduler locality scoring** —
      [plan](plans/artifact-cache-and-prewarm-plan.md) (PLANNED)
  - [ ] Phase 1 — content-addressed cache
  - [ ] Phase 2 — warm RPCs and policies
  - [ ] Phase 3 — cache advertisement + `artifact_locality` scoring
  - [ ] Multiple layers of pre-fetching; tag nodes as pre-warmed; pre-fetch as
        part of the deploy process
  - [ ] Unblocks `artifacts.containerImageDesugar` being on by default
- [ ] `identity` / `nodeIdentity` artifact auth end-to-end. The signing side
      shipped with workload identity; fetch-time use still fails with an
      actionable error.
- [ ] `onFetchFailure` policy distinct from `on_unhealthy` — today fetch
      flakiness consumes restart/move budgets
- [ ] Push container images directly into a worker's artifact cache, so a small
      cluster needs no registry at all
- [ ] Artifact staging on Windows: hardlink/reflink fallbacks, path semantics,
      image consumption (see *Windows support pass*)

## Stateful & volumes

Plans: [stateful-workloads](plans/stateful-workloads-and-instance-identity-plan.md), [volume-plugin-system](plans/volume-plugin-system-plan.md) · docs: [volumes.md](../docs/volumes.md), [stateful-workloads.md](../docs/stateful-workloads.md)

All deferred from the volume plan v1:

- [ ] **CSI bridge plugin** — the driver contract was designed for it; nothing in
      core has to change
- [ ] **Snapshot / restore / clone / expand** — op names are reserved in the
      contract. Overlaps with sandbox snapshot phases 1–2 and 5 above; build them
      together.
  - [ ] `clone` × node locality: same-node clone native, cross-node fan-out
        snapshot-mediated, `capabilities` reporting `cloneScope: node | cluster`
- [ ] **Multiple volumes per instance** (`stateful.volumes: [...]`) — the schema is
      already keyed by (instance, name), so this is purely additive
- [ ] **Cross-node data migration** — deliberately manual today (backup → delete →
      re-mint → restore) until snapshot/restore land
- [ ] Mount concurrently with artifact staging rather than after it (a real
      start-latency win; nothing in the contract forbids it)

## Service discovery & the mesh

Plan: [service-discovery-final-plan.md](plans/service-discovery-final-plan.md) · docs: [service-discovery.md](../docs/service-discovery.md)

v1 (catalog, ports, env, `list`/`resolve`/`doctor`) shipped, and so has the
connect layer: `connect.upstreams`, the per-node L4 forwarder, pluggable
balancing, and control-plane intentions. What is left of the v2 mesh, in the
plan's own adoption order:

- [ ] Node-local DNS + template functions (no-NAT resolver, TCP fallback,
      `ndots:1`, coalesce/splay/fail-static)
- [ ] Datapath plugin ABI + bridge/CNI shim (`mode: 'auto'`)
- [ ] **mTLS between forwarders**, so `service.allow` intentions rest on
      cryptographic workload identity (which has shipped) rather than on the
      control plane filtering the endpoint feed
- [ ] L7 waypoint (retries, timeouts, canary split), then sandboxed WASM
      middleware filters
- [ ] Mesh observability: RED metrics + service graph, `doctor` gaining
      live-traffic evidence
- [ ] Drain-before-SIGTERM (rollout integration)

## Networking

No plan yet — this needs one before any code.

- [ ] Overlay network. Unix-domain-socket transport is on the table.
- [ ] CNI, or a custom networking plugin that can speak CNI
- [ ] Networking fingerprinters (so service discovery can pick a datapath per node)
- [ ] Port forwarding
- [ ] Ingress proxy / load balancer, running as a core-binary app that can also
      be split out into its own job

## Persistence & cluster operations

Plans: [persistence-plan.md](plans/persistence-plan.md), [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md) · docs: [state-store.md](../docs/state-store.md), [raft-operations.md](../docs/raft-operations.md), [persistence-streaming.md](../docs/persistence-streaming.md)

- [ ] `raft_lanes::killing_the_all_lane_leader_reconverges_onto_one_survivor`
      is timing-sensitive under parallel test load: green in isolation
      (9/9 on a 4-vCPU container, 2026-07-30) but failed once during a full
      `cargo test --all` where ~20 test binaries starve its election
      timers. The test already accepts split outcomes by design; consider a
      longer election budget when `cargo test` runs the whole workspace, or
      serializing the cluster-gated suites.

- [x] **Real Raft persistence** — quorum-committed writes, per-lane log stores,
      snapshots/compaction, real membership
      ([#137](https://github.com/mikenomitch/legion/pull/137)). Ships
      changeset replication rather than the per-method determinism audit in
      [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md);
      the as-built design and its judgement calls are in
      [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md).
  - [x] Multi-raft lanes (control / scheduler / status) + lane routing —
        single-writer table ownership enforced at capture time, co-located
        lane leadership, per-lane snapshots
  - [x] **Lane-leadership convergence is bounded and cannot break a lane**
        (2026-07-28). The JC-40 loop campaigned for LEADERLESS lanes, which
        bypasses openraft's own election policy and restarts the node's
        candidacy faster than an election can finish — 132 terms in 210s on a
        2-core runner with the lane never electing. It now campaigns only for
        a lane led elsewhere, with per-lane exponential backoff and escalating
        stand-downs, and co-location is documented as BEST EFFORT rather than
        an invariant. Also fixed the root cause the livelock was hiding: a
        joiner counted as a voter while its lane membership was still JOINT,
        so bring-up looked complete mid-join.
        [Plan §JC-40 correction](plans/durability-and-ops-execution-plan.md) ·
        [calls](calls/durability-and-ops-execution-plan_calls.md) ·
        [docs/multi-raft.md](../docs/multi-raft.md)
  - [x] **Standing-split resilience** (2026-07-28, plan §8): a lane split no
        longer breaks the write path. Worker-plane writes (heartbeats,
        health, failure/volume reports) and internal lane halves FORWARD to
        the owning lane's leader over a new `ForwardLaneWrite` transport RPC
        — this also fixes worker liveness across plain `single`-topology
        failover, where the pre-fix cascade marked healthy workers lost in
        ~10s and self-fenced their stateful allocations at ~90s. Job submits
        are ACCEPTED by the control leader; the deferred projections
        (status seeds, volume claims, rollouts — with gating preserved —
        and evals) are completed by per-lane reconcilers on the checker
        cadence. `/readyz` still 503s on a standing split (operator
        signal), with an honest body and a restart-safe, flap-proof
        divergence timer.
        [Plan §8](plans/durability-and-ops-execution-plan.md) ·
        [calls #1, #3, #6, #12](calls/durability-and-ops-execution-plan_calls.md) ·
        [docs/multi-raft.md](../docs/multi-raft.md)
  - [x] **Adversarial verification pass on lanes v2** (2026-07-28): four
        skeptic sweeps over the election governor, the split behavior
        matrix, the snapshot/apply engine, and the plan fence. Confirmed
        fixes shipped: the catching-up hold is capped (uncapped, it starved
        convergence on any busy lane — the standing split was the steady
        state under load); anchor flap no longer resets the campaign loss
        ladder; first-campaign spacing is enforced; `raft status` counts
        joint-intersection voters instead of reporting `voters: []`
        mid-join; remove-member probes reachability, bounds its membership
        changes, and refuses self-removal (two-step decommission
        documented); the bootstrap seed-snapshot purge re-arms across a
        crash; status-lane orphan sweeps need two consecutive observations;
        standalone events propose on a locally-led lane; `rollout_tick` no
        longer stamps rollouts `failed` on transient lane errors; the
        plan-apply status follow-up no longer fails applied plans (the
        `job_update` 5s churn loop). The engine core (changeset capture,
        per-lane snapshots, ownership guard, log durability) survived
        attack — 8 claimed guarantees verified against the openraft 0.9.24
        source.
        [Calls #7–#16](calls/durability-and-ops-execution-plan_calls.md)
  - [x] The lane-isolation benchmark — measured 2026-07-28, three
        consecutive runs: cross-lane load degrades scheduler p99 50–93×
        under `single` (to ~100–130ms) vs 1.6–2.4× under `lanes` (2.4–3.4ms
        — essentially flat), with identical unloaded baselines and 4–5×
        placement throughput retained. `lanes_vs_single_scheduler_isolation_ab`
        in [tests/raft_perf_smoke.rs](../tests/raft_perf_smoke.rs);
        numbers in [docs/multi-raft.md](../docs/multi-raft.md) § "Lane
        isolation A/B".
  - [x] Raft engine metrics + per-lane bottleneck tracing (2026-07-30):
        the full `legion_raft_*` series — per-lane term / commit_index /
        applied_index / apply_lag / is_leader gauges on the checker tick,
        plus `capture_seconds` vs `propose_apply_seconds` histograms and
        `ops_total{lane,outcome}` at the replication call site, with a
        per-op debug trace of the capture/propose phase split
        ([docs/metrics.md](../docs/metrics.md)). Snapshot timings and
        per-peer replication lag stay reserved.
  - [ ] Per-lane tuning knobs and a Raft health UI page (the remaining
        two-thirds of the old "knobs, tracing, UI" item)
  - [ ] Speculative: Raft autopilot
  - [ ] Idempotency records are not atomic with their mutation: an RPC that
        errors after its mutation but before `idempotency_record` (control
        lane) re-executes on retry — for server-minted ids
        (`create_instance`) that duplicates the instance. Fold the record
        into the mutating op's own capture (it is JC-28 commutative, so any
        lane may carry it). Found by the 2026-07-28 split inventory.
  - [x] Eval NotRaftLeader retries back off exponentially (5s base → 60s
        cap, 1s jitter, in-process attempt tracking that resets on
        completion or failover) instead of a flat 5s forever (2026-07-30).
  - [ ] `destroy_job` under a standing split half-applies from the status/
        scheduler side (cleanups land, control delete rejected) until a
        retry against the control leader completes it. Consider reordering
        control-first + orphan sweeps, like the submit composite.
- [x] Loading state back in from a file — `legion state backup` +
      `--restore-from` ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] **Cluster federation** —
      [plan](plans/multi-cluster-federation-plan.md) (PLANNED, with a
      [calls doc](calls/multi-cluster-federation-plan_calls.md)): named
      clusters in the CLI/UI, symmetric peering (no hub), request
      forwarding, federated SSO, multi-cluster jobs, standby managers
      (learners + zone auto-replacement), and an async DR standby cluster
      with manual promotion and generation fencing. Cross-cluster service
      discovery deferred to a follow-up plan (see plan §11).
- [x] Version handshake between manager and worker — protocol version plus
      build-skew reporting, upgrade order in [docs/upgrades.md](../docs/upgrades.md)
      ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] **Node configuration jobs** — like sysbatch, but also runs on join and can
      add dynamic node metadata (CNI setup, volumes, artifacts, Ansible-shaped
      work). Specified as phase 3 of the
      [host convergence plan](plans/host-convergence-plan.md) (`nodeConfig`
      with `onJoin`, `gateReadiness`, `publishAttrs`), which subsumes the
      sysbatch/system-job items below.
- [ ] Reconfigure a worker from the manager with CRUD commands
  - [ ] Existing nodes of a changed type need the update pushed to them, which
        implies nodes store a reference to their type
- [ ] Root-privilege split: run networking and volume setup as root while the
      task itself doesn't

## Job types

- [ ] **Sysbatch** — run once on every node
- [ ] **System job** — N per X (1 per metal, 2 per region, …). Is this really a
      job type, or just a richer `count`? (`count: {num: 1, per: "id"}`
      approximates it today but does not converge newly joined nodes.)

## Auth, TLS & security

Plan: [auth-acl-plan.md](plans/auth-acl-plan.md) · docs: [auth-and-acls.md](../docs/auth-and-acls.md)

- [ ] **KV ACLs on `GetArtifactCredential`.** Artifact auth (`auth.kv`) lets any
      job read any KV credential path today. The enforcement point exists and is
      marked — see the SECURITY BOUNDARY comment on `get_artifact_credential` in
      [src/manager/server.rs](../src/manager/server.rs), the RPC comment in
      `proto/legion.proto`, and the call-out in
      [docs/artifacts.md](../docs/artifacts.md). Accepted for now; close it.
- [ ] Worker mTLS (deferred from auth phase 3)
- [ ] Dynamic auth-method dylib plugins (deferred from auth phase 2)
- [ ] Workload-identity API credentials (deferred from auth phase 4)
- [ ] Token-resolution cache (deferred from auth phase 4)
- [ ] **TLS is autoconfigured and auto-rotated.** Joining a cluster with TLS
      should be trivial.
  - [ ] **Plan this, don't just code it.** Should the "insecure client" really be
        a "public cert client" (Let's Encrypt by default, bring-your-own CA
        supported), leaving the "secure client" as the self-signed-CA path only
        workers hold? Or is TLS the wrong mechanism here entirely?
  - [ ] Fix the join-token and pre-register call destinations
  - [ ] Env var or flag for contacting the manager — the join-server port
        shouldn't need to be supplied, since it's in the JWT
- [ ] **Cert manager built in**, with public-CA routes, usable by workloads and
      not just the control plane

## Observability

docs: [observability.md](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- [ ] A trace viewer in the UI (make it dev-only at first)
- [x] Metrics endpoint and health probes — Prometheus `/metrics`, `/healthz`,
      `/readyz` ([docs/metrics.md](../docs/metrics.md), [#137](https://github.com/mikenomitch/legion/pull/137)).
- [x] Worker-side `/metrics` + OTLP metrics push (2026-07-30): opt-in
      worker Prometheus listener (`LEGION_WORKER_TELEMETRY_PORT`;
      `legion_worker_*` heartbeat/allocation families) and an OTLP/HTTP
      metrics push for both registries (`OTEL_*` env conventions,
      cumulative temporality, full histogram buckets) —
      [docs/metrics.md](../docs/metrics.md),
      [docs/observability.md](../docs/observability.md). Per-task runtime
      metrics on workers still open (below).
- [ ] Worker per-task runtime metrics (CPU/memory usage per allocation) —
      also wanted by autoscaler phase 2, which collects usage via
      heartbeats; build together.
- [ ] Feed simulated fleets into the `legion_worker_*` families. Found
      merging the worker-telemetry and `--simulate N` branches (2026-07-30):
      [simulate.rs](../src/worker/simulate.rs) runs its own heartbeat loop
      with private `heartbeats_ok`/`heartbeats_failed` atomics and never
      starts the worker telemetry listener, so a simulated host exposes no
      metrics — exactly the fleets the AWS campaign scrapes. Either have the
      fleet loop update the shared registry (one process, N nodes: prefer
      counters and a fleet-size gauge over per-node series) or give the
      harness its own `/metrics`. Noted in
      [docs/metrics.md](../docs/metrics.md) § Worker.
- [x] Refresh [docs/observability.md](../docs/observability.md) — corrected; it
      had claimed OTLP export did not exist while [src/otel.rs](../src/otel.rs)
      ships it ([#137](https://github.com/mikenomitch/legion/pull/137))

## Templates, registry & packaging

The Pack/Helm equivalent. Job templates are in flight on
[#151](https://github.com/mikenomitch/legion/pull/151); the registry half is untouched.

- [ ] Write the plan + calls doc for job templates (#151 shipped without one)
- [ ] There is a default registry
- [ ] I can add a registry
- [ ] I can run with `@<registry>/<job>`
- [ ] I can inspect a job in the registry (reads its metadata)
- [ ] UI for filling in variables from a job definition's metadata
- [ ] Plugins can be fetched by URL or from the registry
- [ ] Automatic build & push step for container images — `legion build` /
      `legion prep`, plus a context file
- [ ] Buildpack runner

## Ecosystem apps (the omakase experience)

Each of these should run inside the core binary *and* be splittable into its own
job, so day 1 is simple and day N is performant. Each needs its own plan.

- [ ] **Autoscaler** — [plan](plans/autoscaler-plan.md) (IN EXECUTION) ·
      [calls](calls/autoscaler-plan_calls.md)
  - [x] Phase 1 — `legion job scale` (2026-07-30,
        [docs/autoscaling.md](../docs/autoscaling.md)): `task_scale` count
        overrides outside the job version (control-lane; see calls #5),
        effective counts in every desired-count reader (scheduler plans,
        slot retirement, rollout group plans), the `job_scale` eval
        (retire surplus + reschedule shortfall), the ECS rollout rule with
        `pending_count` promotion on terminal rollouts (calls #7),
        `base_count` resubmit arbitration, the `scale` ACL resource,
        ScaleTask/GetJobScaleStatus RPCs, CLI (`legion job scale`), MCP
        tools, events, and the `scale_in` stop cause reported to the
        worker whose allocation was retired (added merging with Phase 1a
        of the preemption plan — calls #16). Deferred within phase 1: counts in
        `legion job info` (scale status is the view), an ACL-scenario
        test proving `scale:write` cannot RunJob (the endpoint matrix
        covers the deny side), and regenerating `ui/src/gen/legion_pb.ts`
        (no UI surface consumes the new RPCs yet; run the ui/README
        protoc command when the scaling panel is built).
  - [ ] Phase 2 — per-allocation CPU/memory usage collection (heartbeat →
        `allocation_health`), request-vs-usage in CLI/UI
  - [ ] Phase 3 — task autoscaler MVP: `scaling { min, max, target }` block,
        in-manager leader-gated evaluator, damping, scale-status surface
  - [ ] Phase 4 — full `checks` form, Prometheus source, `per-replica`
        strategy, activation-gated `min: 0`, `headroom`
  - [ ] Phase 5 — escape-to-job: `legion autoscaler agent`, control-plane
        identity policy, KV-lock leadership, `LEGION_MANAGER_ADDR` injection
  - [ ] Phase 6 — node autoscaling: placement-shortfall signal, node groups,
        exec-ABI providers, drain-based scale-in, `ForgetNode` RPC
  - [ ] Phase 7 — dylib `metrics`/`strategies` plugin kinds, `ui.link` Apps
        sidebar section
- [ ] Ingress proxy / load balancer (also listed under *Networking*)
- [ ] GitOps helper — deploy to git, job updates automatically
- [ ] Cert manager (also listed under *Auth, TLS & security*)
- [ ] Overlay network controller

## Plugin surface

docs: [plugin-system.md](../docs/plugin-system.md)

Shipped plugin categories: schedulers, task runtimes, rollout strategies,
artifacts, volumes, node checks. Still to build:

- [ ] Resource plugins
- [ ] Command & API-endpoint plugins
- [ ] Fingerprinter plugins (including networking fingerprinters)
- [ ] CNI / networking plugins
- [ ] Allocation-setup plugins (pre-fetch an image, snapshot, or data set)
- [ ] Task-group-setup plugins
- [ ] Logging plugins (log sinks exist, but not as loadable plugins)
- [ ] Eval hooks — react to cluster events from inside Legion, with the option to
      break out into a job (this is roughly how the autoscaler would work)
- [ ] Pluggable node statuses for degradation (disk pressure, network pressure, …)
- [ ] Pluggable configuration language?

## Agent surfaces

docs: [agent-readiness.md](../docs/agent-readiness.md)

The MCP server, self-documenting CLI, structured output, and documented gRPC
surface all shipped. What's left is the harder "can an agent actually operate
this" work:

- [ ] An agent can easily write a plugin (scaffolding, schema, a worked example)
- [ ] An agent can easily configure a worker to talk to a manager
- [ ] An agent can search the registry and find jobs worth installing and running

## Embedding

- [ ] Legion can be called as a library, not just as a CLI

## UI

docs: [ui-architecture.md](../docs/ui-architecture.md)

Deferred from [ui-jump-hints-plan.md](plans/ui-jump-hints-plan.md):

- [ ] Jump hints past the tenth row — a 20- or 50-row page labels only its first
      ten, and the rest look unfinished until you know it is deliberate
- [ ] Jump hints on the Overview dashboard and on detail-page tables; today only
      the six index pages register rows
- [ ] Verify the jump-hint chords survive Safari and Firefox. The letters avoid
      Chromium's reserved set, but "the page wins `⌘`+digit" is asserted from
      the reserved-key list, not measured
      ([calls entry 1](calls/ui-jump-hints-plan_calls.md))

- [ ] `ui/src/gen/legion_pb.ts` has drifted from `proto/legion.proto` — it is
      missing `WorkerConfig.heartbeat_seconds`, `TaskAllocation.task_connect`,
      `HeartbeatReply.upstream_endpoints` + the whole `UpstreamEndpoints`
      message, `ServiceSummary.allow`, and `PlannedAllocation.score_sampled`.
      Nothing in the UI reads them yet, so it typechecks; regenerate with
      `protoc -I proto --es_out=ui/src/gen --es_opt=target=ts proto/legion.proto`
      (needs `ui/node_modules/.bin` on PATH) and add it to CI, or the next
      person to need one of those fields will hand-write it too

Deferred from [job-updated-at-plan.md](plans/job-updated-at-plan.md):

- [ ] Per-page label for `SharedIndex`'s last column. It is hard-coded to
      "Updated", but Nodes puts `last_heartbeat` under it — a different signal
      wearing the same name ([calls entry 2](calls/job-updated-at-plan_calls.md))
- [ ] Show created/updated on the job detail page header. The index was where
      the wrong value was being read, so the header was left alone; it is a
      layout change with its own design surface

## Cross-cutting

- [ ] **Audit every name decision in depth.** Important, and gets harder the
      longer it waits.
- [ ] Performance and scale: prove Legion scales better than K8s, and than Nomad
- [x] **Binary size: ship a `dist` profile.** `cargo build --profile dist` (LTO +
      `codegen-units = 1` + strip) takes the binary from 43.7 MB to 26.0 MB. See
      "Building a shipping binary" in [development.md](development.md).
- [ ] **Re-measure binary size on linux/amd64.** The 26 MB figure is macOS arm64;
      Go binaries run 7-9% larger on amd64 and Legion has no cross-compile
      target set up, so the shipping number is probably a bit higher.
- [ ] **Decide whether the UI ships inside the binary.** `ui/` is served from
      disk today, so the 26 MB is not comparable to Nomad (150 MB) or k3s
      (81.5 MB), both of which embed their web assets. Embedding it is the
      simpler operator story; it costs binary size on workers that never serve
      a UI.

---

## Long term

- [ ] A **speed** scheduler that optimizes for fast placement. Needs a plan
      before any code — the mechanism is an open design question, not a settled
      one.
- [ ] A **VM** runtime beyond the microVM sandbox — libvirt? cloud-hypervisor?
      QEMU?
- [ ] **Windows support pass**
  - [ ] Artifact staging on Windows: hardlink/reflink fallbacks, path semantics,
        image consumption (see the follow-ups in
        [artifact-system-plan.md](plans/artifact-system-plan.md))

---

# Done

## Worker nodes

- [x] Add a worker node
- [x] CLI arg to get the address
- [x] Makes contact via RPC
- [x] Message passing between the two via RPC
- [x] Heartbeats; marked unhealthy on miss, watchable from the CLI
- [x] Basic fingerprinting system
- [x] Dynamic attributes that can be updated
- [x] Shows up in the manager's ledger of worker nodes
- [x] Can be restarted from the server

## Scheduling

docs: [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md), [jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md)

- [x] Ledger command — see every node and what's on it from the CLI
- [x] A framework for scheduler plugins
- [x] A "random" scheduler
- [x] Constraints
- [x] A "bin-pack" scheduler
- [x] A "spread" scheduler (the inverse of bin-pack)
- [x] Affinities and anti-affinities
- [x] Task groups
- [x] Rescheduling, including health-check-triggered reschedule after restart
- [x] Job health checks reschedule a job — initial failures restart it, failure
      after N times forces a reschedule
- [x] Dynamic metadata: a node attribute change re-evaluates affected jobs
      (`UpdateNodeDynamic` enqueues evals)
- [x] Plan epochs are sampled at *snapshot* time, not after planning. The
      version vector used to fence a plan submission was read after
      `build_plan`, so a competing plan that applied in between was already
      counted in the epoch and the compare-and-set passed against stale
      capacity — an intermittent node oversubscription
      (`concurrent_plans_cannot_oversubscribe_a_node`). The scheduler now
      samples every candidate node's epoch before it reads
      `cached_allocations_by_node`, in one batched query
      (`Store::allocation_version_vector_for_candidates`), and selects the
      affected subset at submit time.
- [x] **Declarable scheduling indexes** — ad-hoc `partition_by` × `dims`
      indexes over node attrs and free resources, nearest-fit candidate
      generation, automatic per-task selection, shadow mode, `job plan`
      explain output, Prometheus series, and the default-on `fit` index
      (free cpu/memory/disk by pool) armed above `index_min_nodes` —
      [plan](plans/scheduling-index-plan.md),
      [docs/scheduling-indexes.md](../docs/scheduling-indexes.md).
      Deferred pieces are open items above (group-path candidates, disk
      fingerprinting, backlog).
- [x] AGENTS.md now requires before/after benchmark numbers on scheduler
      changes, and `benches/scheduler_bench.rs` grew a
      `candidate_construction` group that isolates candidate-set cost
      (indexes on vs off) across fleet sizes.
- [x] **Late-binding plan apply** (phases 0–3) — plans carry a ranked
      candidate list per placement (format-2 submissions) and the applier
      binds each placement to the first candidate whose node epoch is
      unchanged, so parallel submits racing for one node cost a fallback
      rank instead of a conflict retry; coupled placements (overlapping
      lists, `distinct` tasks, multi-task groups) keep exact all-or-nothing
      semantics. Conflict-exhausted evals now requeue with jittered backoff
      (`eval.requeued`) instead of failing terminally. Includes the
      `plan_candidates` setting (default 8; 0 = kill switch + format-1
      emission), `job plan` candidate preview, `candidateRank`/`fallbacks`
      event metadata, the rank histogram + `fallback` outcome metrics, and
      the `tests/plan_contention.rs` burst harness (also the raft-mode
      concurrent-planning regression) —
      [plan](plans/late-binding-plan-apply-plan.md),
      [docs/late-binding-plan-apply.md](../docs/late-binding-plan-apply.md).
      Phase 4 backlog and the capacity-exhaustion gap are open items above.

## Task runtimes

docs: [task-runtime-plugins.md](../docs/task-runtime-plugins.md)

- [x] A framework for task-runtime plugins
- [x] Rename "task kind" → "task runtime"
      ([plan](plans/task-runtime-rename-plan.md), single-pass, no shim)
- [x] A **noop** runtime ([docs](../docs/task-runtime-noop.md))
- [x] An **exec** runtime ([docs](../docs/task-runtime-exec.md)) — runs the given
      command and args, as a specific user, joining an existing cgroup (v2 only),
      with landlock
- [x] A **container** runtime ([docs](../docs/task-runtime-container.md)) with no
      Docker dependency: macOS uses Apple `container`; Linux uses a low-level OCI
      runtime (`youki` by default, or any OCI-spec runtime via
      `LEGION_OCI_RUNTIME`). Linux runtimes can't pull images, so the runtime
      builds an OCI bundle from the image's OCI layout
      ([oci_bundle.rs](../src/task_plugins/oci_bundle.rs)).
- [x] A **sandbox** runtime ([docs](../docs/task-runtime-sandbox.md)) — OCI images
      in microVMs for untrusted code and agent workspaces

## Artifacts

[plan](plans/artifact-system-plan.md) · [docs](../docs/artifacts.md)

- [x] Grab container images (`oci://` fetcher + ORAS mediaType)
- [x] Auth for fetching (KV credentials via `GetArtifactCredential` + cluster
      `artifacts.auth` matchers)
- [x] Grab files onto disk (https/file/git/s3 fetchers, task dirs, checksums, safe
      unpack, `${artifact.<name>}` references, per-artifact status in CLI and UI)
- [x] A plugin system for fetchers (`plugins/artifacts/<scheme>/`)
- [x] OCI layout → runtime bundle conversion so Legion-managed images work on
      Linux container workloads

## Rollouts

[docs](../docs/rollouts.md)

- [x] A new version of a job starts a deployment
- [x] A framework for deployment strategies
- [x] An "immediate" strategy that cycles everything out in place
- [x] A "rolling" strategy
- [x] Health-gated progression, manual and automatic
- [x] A UI for all of it

## KV & secrets

[plan](plans/kv-reactive-task-updates-plan.md) · [docs](../docs/kv-store.md)

- [x] A task can read a KV value
- [x] Updating a value restarts the task, when configured to
- [x] A value can be used in the job definition
- [x] Reactive updates: injection into env and files, watch/on_change/on_delete
      policy, dependency graph, debounce, per-runtime capability gating

## Service discovery (v1)

[plan](plans/service-discovery-final-plan.md) · [docs](../docs/service-discovery.md)

- [x] Declare a named service on a task
- [x] Declare health checks on that task
- [x] Query address and port from other services
- [x] Catalog as a projection (no registration RPC, no drift, only-healthy)
- [x] `legion service list | resolve | doctor`; works identically on Linux and macOS
- [x] Connect to another service by name, load balanced, without writing any
      client code: `connect.upstreams` -> `$LEGION_UPSTREAM_<SVC>`, a node-local
      loopback forwarder, and `round_robin`/`random`/`least_conn` (pluggable —
      one trait impl plus one registry row)
- [x] Declare which other jobs may reach my service (`service.allow`, matched
      against the consuming job id). Enforced on the **connect path only**: the
      manager filters the per-node endpoint feed, so a denied consumer's
      forwarder has nothing to dial. Not cryptographic identity — a task that
      already knows a backend's `address:port` can still dial it directly, and
      that hop is unauthenticated. mTLS below is what closes it.
- [x] `legion service resolve --one`, `service` entries in the capability
      catalog, and `legion_service_list`/`legion_service_resolve` MCP tools.
      Note `service list -j` / `resolve -j` now emit the standard `{ok, data}`
      envelope instead of a bare array (`jq '.[0]'` -> `jq '.data[0]'`, or use
      `--one`, which needs no `jq`).

## Stateful workloads & volumes

[stateful plan](plans/stateful-workloads-and-instance-identity-plan.md) · [volume plan](plans/volume-plugin-system-plan.md) · [docs](../docs/volumes.md)

- [x] Instance identity: slot and keyed modes, storage handles, fencing
- [x] Volume plugin system: the `VolumePlugin` contract, exec ABI, built-in
      `local-dir`/`host`/`nfs`/`tmpfs`, a reference exec plugin, the conformance
      harness (`legion volume plugin test`), the volume table and reclaim,
      node-local pinning, `legion volume` CLI + MCP tools, UI, and the
      [Postgres](../docs/guides/postgres.md) / [Redis](../docs/guides/redis.md) guides
- [x] Block-mode volumes, consumed directly by the sandbox runtime

## Persistence

[plan](plans/persistence-plan.md) · [docs](../docs/persistence-streaming.md), [raft-operations.md](../docs/raft-operations.md)

- [x] Streaming SQLite replication, Litestream-style, with optional lease failover
- [x] Raft control plane: leader election, failover controls, membership,
      observability, CLI and UI operations

## Workload identity

[plan](plans/workload-identity-oidc-plan.md) · [docs](../docs/workload-identity.md)

- [x] Manager as an OIDC issuer: keys, discovery, JWKS, rotation
- [x] Token issuance RPC, job-spec API, worker delivery, change mode
- [x] Node identity tokens and control-plane identity (phase 7)

## Auth & ACLs

[plan](plans/auth-acl-plan.md) · [docs](../docs/auth-and-acls.md)

- [x] Phase 1 — the ACL model: resources, actions, scopes, default-deny, one-line
      enforcement, and the `tests/acl_endpoints.rs` coverage gate
- [x] Phase 2 — interactive SSO and richer admin
- [x] Phase 3 — worker-plane hardening (join tokens from the authenticated
      `CreateJoinToken` RPC; the insecure `:4748` bootstrap listener is gone)
- [x] Phase 4 — operations at scale

## Exec & actions

[plan](plans/exec-and-actions-plan.md) · [docs](../docs/exec-and-actions.md)

- [x] Worker exec core: `ExecSupport`/`exec()` on the runtime trait, PTY, session
      limits
- [x] Manager proxy, ACLs, audit events, and `legion exec`
- [x] Browser path: WebSocket bridge and an xterm.js terminal in the UI
- [x] Actions: spec schema, `ListJobActions`, server-side render and param
      validation, the `action` ACL resource, `legion action list|run`
- [x] Actions UI, MCP tools, and docs

## Observability

[docs](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- [x] Tracing in the scheduler
- [x] Benchmarks in a dedicated directory
      ([docs](../docs/testing-and-benchmarks.md))
- [x] Task log capture, tailing, rotation, and external shipping (including OTLP)
- [x] OTLP trace export for Legion's own spans ([src/otel.rs](../src/otel.rs))
- [x] Event stream: scheduler events as history and live tail (`ListEvents`,
      `StreamEvents`, CLI, UI)

## Job types & dependencies

[docs](../docs/jobs-tasks-and-scheduling.md)

- [x] Short-lived jobs — the `batch` lifecycle
- [x] Cron jobs — `schedule` on a batch task, taking a cron expression or
      `{ every: <duration> }`
- [x] Task groups co-locate on one node
- [x] DAGs: `depends_on` with `start`/`complete`/`success` conditions, topological
      sort, cycle rejection, cross-group dependencies (so DAGs span machines) and
      group-bound sidecars

## Agent surfaces

[docs](../docs/agent-readiness.md)

- [x] A self-documenting CLI for agents (`legion docs`, `legion capabilities`)
- [x] Structured output everywhere (`--json` + the standard envelope)
- [x] gRPC endpoints documented in `proto/legion.proto` as the source of truth
- [x] An MCP server (`legion mcp serve`), with mutating tools gated behind
      `--allow-write`
- [x] Idempotency keys on mutating RPCs; validate/plan/preview paths

## UI

[docs](../docs/ui-architecture.md)

- [x] Keyboard navigation: ⌘K command palette, `j`/`k`/`o` index selection, `u`
      to go up a level
- [x] Jump hints — hold ⌘ (Ctrl) to label index rows with digits and sidebar
      items with letters, then ⌘1 / ⌘⇧B to jump
      ([plan](plans/ui-jump-hints-plan.md),
      [calls](calls/ui-jump-hints-plan_calls.md)). Rows past the tenth, the
      Overview dashboard, and detail-page tables are open items above.
- [x] Job mtime — the Jobs index "Updated" column showed `created_at`, because
      jobs had no `updated_at` anywhere in the system. Real column, stamped on
      spec submissions and status transitions but not on allocation churn;
      `ListJobs` sorts by it by default and `job list` renders both timestamps
      readably ([plan](plans/job-updated-at-plan.md),
      [calls](calls/job-updated-at-plan_calls.md),
      [docs](../docs/jobs-tasks-and-scheduling.md#job-timestamps-created_at-and-updated_at)).
