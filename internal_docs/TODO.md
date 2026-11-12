# TODO

Legion's running to-do list. Open work is at the top, by area; shipped work is in [Done](#done).

- Each area links its plan in [`plans/`](plans/) and its page in [`docs/`](../docs/README.md); hardware and rig runs are under [Verify](#verify).
- Finish something here → check it off and move it to Done **in the same change**.
- Keep items short: what is wrong, where in the code or plan, and a link. History belongs in the plan or calls doc.
- Deferred work found along the way becomes a new open item in its area.

## In flight (open PRs)

| PR | What it lands |
| --- | --- |
| [#75](https://github.com/mikenomitch/legion/pull/75) | The `isolate` runtime, Phases 1–5: `legion-isolate`, the `js` and `python` images, the worker's isolate supervisor and datapath, tiers and burst, `legion isolate stats`/`get` ([plan](plans/isolate-runtime-plan.md) · [calls](calls/isolate-runtime-plan_calls.md) · [docs](../docs/task-runtime-isolate.md)) |
| [#39](https://github.com/mikenomitch/legion/pull/39) | Stop macOS single-process clusters cleanly (closes "Stopping the macOS manager leaves its Apple containers running") |
| [#37](https://github.com/mikenomitch/legion/pull/37) | UI appearance picker: built-in themes and custom colors |

[#41](https://github.com/mikenomitch/legion/pull/41) (keep plugins out of the
workspace) is superseded: the crate split ([#71](https://github.com/mikenomitch/legion/pull/71))
landed the same `members` fix and the plugin build's exit-status check, so it
can be closed.

---

## Launch blockers

Worker-plane auth Phases 0–8 shipped ([plan](plans/worker-plane-auth-plan.md) ·
[calls](calls/worker-plane-auth-plan_calls.md)); the AWS spot-check fixes are in
[launch-spot-check-findings.md](launch-spot-check-findings.md),
[launch-blockers-and-security-gaps-plan.md](plans/launch-blockers-and-security-gaps-plan.md),
and [launch-blockers-round-2-plan.md](plans/launch-blockers-round-2-plan.md).
Their remaining real-hardware runs are under Verify.

### Worker plane and the host guard

- [ ] **Vendor Docker's default seccomp allowlist** once CI can run it on every
      runtime; the shipped profile is a denylist over the same blocked set
      ([calls 37](calls/worker-plane-auth-plan_calls.md)).
- [ ] **A plain-`http` cross-origin redirect through the guarded proxy loses
      its label** (reqwest drops `Proxy-Authorization`), so it is refused but
      the fetch error lacks the guard's reason
      ([round-2 calls #1](calls/launch-blockers-round-2-plan_calls.md)).
- [ ] **`hostGuardRules` node addresses are as of registration**; refresh them
      on heartbeat if interfaces change under a running worker
      ([round-2 calls #4](calls/launch-blockers-round-2-plan_calls.md)).

### Spot-check leftovers

- [ ] **A node can read no content store but its own** (no per-store read
      credentials; cluster `artifacts.auth` never reaches part URLs). Decide
      whether cross-store restores deserve a surface.
- [ ] **No way to reach a microVM sandbox over HTTP** without a hand-built TAP,
      guest IP config, and host DNAT. Gated mode has no host-to-guest ingress
      relay (gated sandboxes never get a netns, `crates/worker/src/worker/daemon.rs`).
- [ ] **A provisional claim killed but still in its graceful stop when the
      worker crashes is not recorded** in `provisional_claims.json`, so it can
      outlive the crash ([round-2 calls #5](calls/launch-blockers-round-2-plan_calls.md)).

### Spot-check-fix follow-ups

[plan §8.7](plans/launch-spot-check-fixes-plan.md) · [calls](calls/launch-spot-check-fixes-plan_calls.md)

- [ ] **Scheduler benchmark numbers for the reschedule holds.** They add one
      indexed `reschedule_hold` read per `Reschedule` plan and a subquery to the
      keyed binder's ordering, and since 2026-11-08 the hold lookup matches by
      job, group, and task across configs; before/after `scheduler_bench`
      numbers were never taken.
- [ ] **On Linux `command` replaces the image `ENTRYPOINT`; on macOS it does
      not**, so one jobspec runs different commands per OS (calls 51). Needs a
      plan.
- [ ] **Pre-vote for partitioned raft members.** A member cut off by a
      partition still deposes the leader when it heals; openraft 0.9 has no
      pre-vote (calls 17).
- [ ] **Fold the partial-placement remainder into blocked evals.** A partial
      plan's remainder waits on `capacity_waiters`, a second retry path, and
      `job info` shows `blocked` only when nothing placed (calls 21).
- [ ] **Image pulls**:
  - the `container` runtime's extracted rootfs is not cached (ORAS layers are,
    by digest: [docs](../docs/artifacts.md#oras-layers));
  - [x] a hung pull blocks other starts of the same image — registry reads
    now time out after 60s of silence ([round-2 plan](plans/launch-blockers-round-2-plan.md));
  - [x] bundled webpki roots can't pull through a TLS-intercepting proxy —
    native roots were already loaded (object_store enables the reqwest
    feature); now named in the workspace;
  - on macOS a pull failure reports as a task exit, not a staging failure.

---

## Scheduling & placement

Plans: [plan-path-fleet-scaling](plans/plan-path-fleet-scaling-plan.md) ·
[eval-path-raft-cost](plans/eval-path-raft-cost-plan.md) ·
[raft-write-path](plans/raft-write-path-plan.md) ·
[placement-and-commit-cost](plans/placement-and-commit-cost-plan.md) · docs:
[scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md),
[benchmarks.md](../docs/benchmarks.md). Rig re-measurements are under Verify.

### Plan, commit and write-path cost

- [ ] **Plan-commit cost is the throughput ceiling past 10k nodes.** On AWS
      the plan phase is ~25x the micro-bench (26 ms/eval at 20k, 45 ms at 50k),
      `apply` is ~82% of it, and the slowest coalesced commit grows 9.2 → 23.3 ms
      from 5k to 50k nodes for the same plan size. Profile the commit at 50k
      (suspects: free-pool rebuild, reprojection, heartbeats sharing the lane)
      ([plan §13](plans/plan-path-fleet-scaling-plan.md)). Also caps edge ratification.
  - [ ] Batch more plans per commit: ~1.75 plans/commit today. Re-measure with
        `run-campaign.sh --eval-threads 8` at 20k now that conflicts are zero.
  - [ ] Plan N / apply N in one commit was declined (handlers write as they go;
        [call 2](calls/eval-path-raft-cost-plan_calls.md)). Cost it against
        this commit, not the eval loop.
- [ ] **The plan apply still parses each attempted node row** (review SCH5's
      unshipped half; [plan §6.2](plans/placement-and-commit-cost-plan.md) ·
      [calls #2](calls/placement-and-commit-cost-plan_calls.md)). Reusing the
      read model's `Arc<Node>` by epoch is unsound; hash the raw attribute bytes
      in the transaction and compare. Measure serialize+hash vs parse at 10k first.
- [ ] **One capture mutex serializes every lane, and every write runs twice**
      (review SCH1; `CAPTURE_MUTEX` in `raft/capture.rs`, one write connection).
      Group commit and single execution under `single` shipped. Open:
  - [ ] The leader still executes twice under `lanes`; unblocked by per-lane
        files ([plan §6.3.6](plans/raft-write-path-plan.md)).
  - [ ] A database file per lane (own WAL lock and capture mutex). Needs its own
        plan ([§6.4](plans/raft-write-path-plan.md) · [calls #6](calls/raft-write-path-plan_calls.md)).
- [ ] **Pipeline the scheduler lane**: capture entry N+1 while N commits. Keyed
      sandbox lifecycles are bound at ~50/s on 4 vCPU against a ~125/s target.
      Blocked on projecting from the changeset's own values, not re-read rows
      ([raft-write-path §6.2.4](plans/raft-write-path-plan.md) · [calls #5](calls/raft-write-path-plan_calls.md);
      [scheduler-lane-entry-cost §6.2](plans/scheduler-lane-entry-cost-plan.md)).
- [ ] **A keyed bind still visits every live instance of the task**, two index
      lookups each, so tens of thousands of running instances cost milliseconds
      per placement ([scheduler-lane-entry-cost §11](plans/scheduler-lane-entry-cost-plan.md)).
- [ ] **Power-cut test of the purge barrier.** Ordering is unit-tested
      (`purge_runs_the_barrier_before_deleting`); cut power right after a purge
      (unsynced VM block device or `dm-flakey`) to prove the applied marker never
      regresses below the purge horizon ([plan §6.1](plans/raft-write-path-plan.md)).
- [ ] **`STRING` and `TIMESTAMP` columns still carry NUMERIC affinity**: ids and
      timestamps that look numeric are stored as INTEGER. A re-type is a
      value-rewriting migration ([calls 2](calls/jsonb-text-affinity-plan_calls.md)).
- [ ] **Job stop/destroy at scale drains at ~4 evals/s** (AWS 10k cleanup).
      Profile with `legion_eval_phase_seconds{kind="job_stop"}`.
- [ ] **Other mutating handlers still commit on a runtime worker.** `StopJob`,
      `DestroyJob`, KV writes and most mutating RPCs call the store inline, as
      `RunJob` did; a burst starves evals and heartbeats. Move them behind
      `run_blocking_admission` as they show up.

### Eval loop and concurrency

- [ ] **A hung eval handler is never re-swept.** A stale worker can no longer
      settle an eval a newer claim holds (claim-epoch fence, 2026-11-08), and a
      plan's own settle is still unfenced (benign). A handler that never returns
      keeps its claim until leadership changes; a periodic re-sweep needs a
      stated bound on handler time ([docs](../docs/scheduler-and-eval-loop.md)).
- [ ] **Decide the `evalThreads` default** ([plan](plans/eval-thread-default-plan.md),
      picks no number yet). `available_parallelism()` placed slowest in the
      2026-08-05 sweep and the curve never turned over. When it moves, also fix
      the read pool (`evalThreads + 4`, capped at 16) re-coupling with the runtime
      on >12-core hosts, and `configs/manager.ncl`'s pinned `evalThreads = 4`.
- [ ] **`web_fleet` at 5k got slower with the `RunJob` offload** (1,175 → 968
      alloc/s, one run) while every other size got faster. Find whether faster
      admission changes coalescer batching or crowds heartbeats before tuning.
- [ ] **`legion_eval_queue_depth` lags up to a second on a deep queue** (above
      `evalThreads × 16` pending; [calls #6](calls/placement-and-commit-cost-plan_calls.md)).
      A `kind` label would want the same treatment.
- [ ] **Adaptive scheduler concurrency, phases 4–5**
      ([plan](plans/adaptive-scheduler-concurrency-plan.md)). Everything waits on
      the Phase 0 matrix (Verify):
  - [ ] Flip the default to `enforce` if the matrix shows it beats the best
        static `evalThreads`; then make the `evalThreads` guidance unconditional.
  - [ ] The `pool` label on `legion_plan_apply_total` (deferred with Phase 0;
        [calls #2](calls/adaptive-scheduler-concurrency-plan_calls.md)).
  - [ ] Phase 5: per-pool controllers, fallback rate and lane pipeline wait as
        extra signals, live reload of `adaptiveConcurrency`.
  - [ ] Only `job_stop` is exempt from throttling; if recovery latency matters,
        add a priority lane for recovery kinds ([calls #1](calls/adaptive-scheduler-concurrency-plan_calls.md)).
- [ ] **Late-binding plan apply, Phase 4 backlog** ([plan §8](plans/late-binding-plan-apply-plan.md)):
      capacity-aware candidate skipping, per-component partial commit + eval
      refill, striped disjoint candidate lists for identical replicas (highest
      value: rejections lose a plan's one candidate per task), and a batched
      apply lane for physically full pools.
- [ ] **Fused top-k candidate scoring** for ranked candidate lists, like the
      one-candidate argmax path; when `candidate_construction` says it pays
      ([calls #20](calls/late-binding-plan-apply-plan_calls.md)).

### Candidates and indexes

- [ ] **The `fit` index does not see `maxAllocations`.** A capped node still
      projects into a fitting cell, so under `bin_pack` capped nodes rank first
      and waste the candidate budget. Project `allocations` as a fourth dim or a
      cap flag that moves the node to the non-fitting tail.
- [ ] **Node namespace restrictions cost one attribute lookup per node per task**
      (`PermitSet::is_permitted`; 11.0 → 12.2 ms at 10k nodes;
      [calls 25](calls/legion-apps-plan_calls.md)). Precompute "accepts every
      namespace" per node, or fold it into `permits_restrictions`, and re-measure.
- [ ] **In-place rollout ticks build an O(fleet) exclusion set**
      (`crates/manager/src/manager/rollout.rs`, `available_node_ids`), forcing the plan's node-set
      copy. Express it as an allow-list ([plan §13](plans/plan-path-fleet-scaling-plan.md)).
- [ ] **Truncation-ineligible tasks still scan the fleet** (`count.per`,
      `distinct`, volumes, multi-task groups); a non-goal of the fleet-scaling plan.
- [ ] **Scheduling-index follow-ups** ([plan](plans/scheduling-index-plan.md)):
      index-served group co-placement (calls 10), pluggable index kinds,
      `count.per` by partition enumeration, device-set dims, task-level
      `index_hint`, incremental orthant enumeration with early termination.
- [ ] **Volume-pin staleness in planning**: pins are read mid-plan with no epoch
      coupling, so a pin invalidated mid-plan (`volume delete --force`) can still
      direct a placement. Low severity; reconciliation covers it.

### Correctness gaps

- [ ] **Fresh plans overcommit while the old config still runs** (audit SC7).
      Apply counts the replaced job's allocations as free. Remove them inside the
      same apply, with care for stateful release and rebind
      ([calls #17](calls/architecture-audit-2026-10-09_calls.md)).
- [ ] **The capacity pass has triggers it doesn't listen to yet** (audit SC4).
      Allocations freed by stop or scale-in and a node recovering by heartbeat
      queue nothing, and batch tasks are never woken ([calls #10](calls/architecture-audit-2026-10-09_calls.md)).
- [ ] **A partly placed job says nothing about the envelopes it left out.**
      When a session task's envelope does not fit an eligible node, the
      only traces are the "Placed N allocation(s)" event and a manager
      WARN log line; the pending/capacity-demand view skips `count.per`
      tasks, so an unplaced envelope never shows as pending
      ([`pending_capacity_work`](../crates/manager/src/storage/store/demand_ops.rs));
      and a session start or run on that task then answers
      `FAILED_PRECONDITION no node runs an envelope for <job>/<task> yet`,
      which misleads when the node is eligible but full
      ([`sessions.rs`](../crates/manager/src/manager/sessions.rs)). The
      cheapest diagnostic: one warn-level scheduler event per eval naming
      each unplaced task with its reason, from `outcome.unplaced`
      ([`eval.rs`](../crates/manager/src/manager/eval.rs)). Found while
      chasing an isolate test harness whose node was sized from its
      fingerprint, not `config_attrs.resources`
      ([`ResourcePool::from_node`](../crates/common/src/resources.rs));
      pre-existing for every session task.

### Placement features

- [ ] **Soft taints.** Pools act as hard taints (`crates/common/src/constraints.rs`) and the
      worker type's `namespaces` list restricts nodes; there is no prefer-not form.
- [ ] **Schedule onto the same node as an existing job.** `same_node_as` works
      only within one job spec (`crates/manager/src/manager/scheduler/policy.rs`).
- [ ] **Worker-fingerprinted `disk` as a built-in schedulable resource**; today
      only operator-declared `config_attrs.resources.disk` feeds the `fit` index.
- [ ] **Bring back `resources.cpus` as a core count, sized per node.** It is
      refused at submit today ([todo-closeout calls #2](calls/todo-closeout-2026-11-10_calls.md));
      it used to be an MHz alias for `cpu` that read like a CPU count. Bring it
      back meaning cores, Docker/Kubernetes-style (`cpus = 0.5` is half a
      core), converted to MHz with each node's fingerprinted per-core
      frequency at placement, not a fixed rate, so `cpus = 1` reserves one
      real core's worth on a 2.0 and a 3.5 GHz node alike. Decide how it
      composes with `cpu` and `cores`, how the `fit` index sees a per-node
      amount, and how the cgroup weight is derived; touches the plan path, so
      it needs `scheduler_bench` numbers and a plan.
- [ ] **Let a node override its builtin capacity** (`cpu`, `memory`, `cores`).
      A builtin name in a worker type's `resources` fails startup today
      ([calls](calls/node-resource-overrides_calls.md)). Decide replace vs
      ceiling and how it composes with `reservedResources`.
- [ ] **Scheduling plugins can only pick a node** (review SCH6): one
      `select_node`, one policy cluster-wide, no free resources in context, dylib
      re-opened per eval. Wants filter/score/reserve/permit stages per pool or
      job. Needs a plan.
- [ ] **Soft spread with a skew target** across zones or racks (review SCH7);
      `distinct` and `count.per` are hard only.
- [ ] **A feasibility cache by computed node class** (review SCH7); class keys
      exist only for blocked evals (`blocked.rs`).
- [ ] **Usage-aware placement and a descheduler** (review SCH7).

### Preemption

[plan](plans/preemption-and-eviction-plan.md) · [docs](../docs/preemption-and-eviction.md)

- [ ] **Should the per-pool preemption toggle be ACL-adjacent?**
      `disabledPools` lives in `schedulerSettings`, so a pool-scoped operator
      can't exempt their own pool (plan, Open Questions 6).
- [ ] **A fresh multi-task group preempts only for its first member**; if a
      later member can't fit, the whole plan fails.

### Gang scheduling

[plan](plans/gang-scheduling-plan.md) · [calls](calls/gang-scheduling-plan_calls.md) · [docs](../docs/gang-scheduling.md)

- [ ] **Gang variables go stale after a scale or a moved leader**: members keep
      the `LEGION_GANG_SIZE` / `LEGION_GANG_LEADER_ADDR` they were placed with.
      Restart the gang on scale, or deliver them as a re-read file (calls 9).
- [ ] **Readiness timeout** (Kueue's `waitForPodsReady`): `placementTimeout`
      covers placement only, not a placed member that hangs before running.
- [ ] **In-place gang restart** (JobSet `InPlaceRestart`).
- [ ] **Local restarts as gang losses**: a worker restarting a crashed member in
      place is invisible to the manager; a worker report would let
      `onMemberLoss` apply (calls 5).
- [ ] **Phase 4**: an integration test for the `createFirst` blue-green gang
      update, and the autoscaler consuming a blocked gang's shortfall.
- [ ] **Gang-aware wake**: a blocked gang re-plans on any capacity on its class.
      Wake only when `min - running - placeable` could fit, once measured (calls 2).
- [ ] **Whole-gang preemption victims**: a `gang = true` job is never
      preemptible because preemption won't go below quorum (calls 7).
- [ ] **Reservation / backfill for starving gangs.** Watch
      `legion_gang_blocked_jobs` first; needs its own plan.
- [ ] **Gangs ignore spot-interruption notices** (review AI7;
      [calls #8](calls/placement-and-commit-cost-plan_calls.md)). Wants an
      "interruption pending" node condition from IMDS or the `aws-asg` plugin.
      Needs its own plan.
- [ ] **Multi-level gang topology** (`keys = ["rack", "zone"]`); also at most
      eight domains are tried per plan ([plan §11](plans/placement-and-commit-cost-plan.md) ·
      [calls #5](calls/placement-and-commit-cost-plan_calls.md)).

### Edge boot

[plan](plans/edge-boot-plan.md) · [docs](../docs/edge-boot.md) · [calls](calls/edge-boot-plan_calls.md)

- [ ] **The node's resource ledger is never configured in production.**
      `configure_ledger` has only test callers, so `LocalLedger` admits every
      edge claim (the docs now say so). Sizing it alone would leak room:
      `LocalLedger::release` has no production caller and manager-placed
      allocations are never committed. Size it from `ResourcePool::from_node`
      and derive usage from live allocations (`crates/worker/src/worker/claims.rs`;
      [edge-admission-control §5.3](plans/edge-admission-control-plan.md)).
- [ ] **Speculative dispatch and late-binding apply don't compose**: when the
      apply falls back from `candidates[0]`, the claim goes unratified and holds
      staged work until its lease expires. Dispatch to top-n, or prefer
      claim-holding nodes in the apply walk.
- [ ] **Ratification throughput** ([plan](plans/edge-ratify-throughput-plan.md)):
  - [ ] A networked load test: `legion-loadgen --mode ratify` from `--simulate`
        nodes with per-node tokens against 3 voters; report where p99 crosses
        `ratifyTimeout` (Phase 3).
  - [ ] Profile the per-ratification apply (~2 ms of manager CPU per claim)
        before touching the cap or batching (§8, §11).
  - [ ] The worker never retries a refused `internal` ratification; add it if
        `internal` refusals aren't rare.
  - [ ] Managers stop speculating when apply latency nears the task's lease
        ([edge-admission-control](plans/edge-admission-control-plan.md) Phase 2).
  - [ ] Ratifications carry their deadline and refuse `expired` without
        committing ([edge-admission-control](plans/edge-admission-control-plan.md) Phase 3).
- [ ] **A pre-warm push for the worker spec cache** (`legion job prewarm --pool
      edge-eu`), so a pool needn't run a job before redeeming its tickets
      ([artifact cache plan](plans/artifact-cache-and-prewarm-plan.md)).
- [ ] Lift "no gates above `stage` for tasks with ports or connect upstreams";
      needs speculative port assignment reconciled at commit ([calls #13](calls/edge-boot-plan_calls.md)).
- [ ] Speculative attach for volume drivers that can fence a concurrent attach,
      via a volume-contract capability.
- [ ] A per-interface default-drop filter for the `sandbox` `boot` gate
      (today only the TAP link is held down; [calls #10](calls/edge-boot-plan_calls.md)).
- [ ] Island mode (plan phase 5), and ticket pre-distribution / bulk minting
      for warm fleets ([ratify §8](plans/edge-ratify-throughput-plan.md)).

### Heartbeats and liveness

[liveness plan](plans/liveness-off-the-raft-path-plan.md) ·
[cadence plan](plans/heartbeat-cadence-at-scale-plan.md) ·
[docs](../docs/heartbeat-cadence-and-scale.md)

- [ ] **Tighten failure detection at large fleets.** 20k nodes get 21s beats, so
      loss detection takes ~105s vs 10s on small fleets. Raise the default
      `targetRatePerSecond` (rerun 20k at 5,000 and 10,000), then split a cheap
      liveness ping from desired-state sync (wire change; needs a plan).
- [ ] **Per-pool heartbeat freshness.** `legion_heartbeat_freshness_ratio` is
      fleet-wide and blends fast and slow pools.

### AWS comparative benchmark rig

[plan](plans/aws-comparative-benchmark-plan.md) · [calls](calls/aws-comparative-benchmark-plan_calls.md) ·
[`benchmarks/aws/`](../benchmarks/aws/README.md)

- [ ] **Measure plan-apply conflicts under both `bin_pack` and `spread` on the
      rig.** The rig now runs each scenario's own `scheduling_policy`
      (`run-campaign.sh --scheduling-policy` overrides); no AWS run has used it yet.
- [ ] **Bake a rig AMI.** Control plane, loadgen, and spot workers each pay
      their own cloud-init (~10 min at bring-up, 6–8 min per ASG scale-up, which
      stalled a whole session). `scripts/rig-toolchain.sh` is written; it needs
      a `bake-rig-ami.sh` and a terraform lookup, like the operator AMI.
- [ ] **Per-run sim node prefixes.** Now that `stop-sim-fleet.sh` forgets
      its nodes, the per-campaign store reset and the one-scenario-per-campaign
      rule could go; both are left as they were ([calls 43](calls/aws-comparative-benchmark-plan_calls.md)).
- [ ] **Cache the built binary in S3, keyed on git rev.** `distfast` took 28
      min against 24.5 for full LTO (the cost is ~900 crates, not linking);
      `build-and-upload.sh` already fingerprints plugins, not the main binary.
- [ ] **Legion's fixed-2s ablation never settled onto 2s** at 5k (freshness
      0.46 with `adaptive = false`), so that row understates the cost. Find why.
- [ ] **Nomad's Python placement follower misses 14–34 events** in 50k cells,
      undercounting throughput. Use a compiled follower or Nomad's own count.
- [ ] **K8s at 200,000 pods**: the scheduler's apiserver stopped completing TLS
      handshakes. Point the driver at another apiserver and rerun the
      40-per-node `dense_fleet` cell.
- [ ] **Nomad can't run `mixed_heterogeneous`**: nodesim has no node `meta`.
      Patch it if the report needs that row.
- [ ] **K8s failover drill**: `failover-drill.sh` has no fault command for the
      scheduler's lease holder.

## Task runtimes

Plans: [task-runtime-plugins](../docs/task-runtime-plugins.md). Hardware and
rig runs for everything below live in the verify list.

### Start path and node throughput

- [ ] **Sandbox boot throughput, what is still open**
      ([plan](plans/sandbox-boot-throughput-plan.md) ·
      [calls](calls/sandbox-boot-throughput-plan_calls.md) ·
      [results](../benchmarks/aws/sandbox-boot/RESULTS.md)):
  - [ ] **A large node is bound by its root disks** (calls 13). Reflinked
        clones plus `debugfs` writes contend in XFS locks (~150–200 boots/s on
        96 vCPU). Write per-sandbox files without touching shared extents, or
        spread clones over several base copies, then re-measure.
  - [ ] **Parallel branches not stacked** (calls 1): `sandbox-warm-pool`,
        `edge-prewarm`, `elastic-session-envelopes`, `edge-admission-backpressure`,
        `edge-ratify-throughput`, `chunked-checkpoints`, `checkpoints-on-stream`,
        `multi-eval-plans`, `scheduler-lane-entry-cost`, `flat-commit-cost`,
        `web-fleet-throughput`; each needs its own measured case.
  - [ ] **A memory-snapshot start for sessions**: restore from a per-node
        template snapshot instead of booting (opt-in, falls back to a boot).
- [ ] **Start reports wait for a heartbeat.** A health transition reaches the
      manager on the next beat (~1 s, up to 21 s on adaptive cadence), so
      rollouts and discovery wait. Carry it on a `StreamWork` report frame
      ([plan](plans/task-start-critical-path-plan.md) phases 4–5,
      [calls 4](calls/task-start-critical-path-plan_calls.md)).
- [ ] **A pushed gated task's policy and ingress wait for the heartbeat.** Its
      namespace, proxy, resolver, and upstreams bind at launch (2026-11-08) but
      fail closed until the heartbeat delivers the policy, and ingress listeners
      bind on the heartbeat. Carry the policy assignment on the delta
      ([docs](../docs/network-handlers.md)).
- [ ] **The worker state file is rewritten whole on every start.**
      `persist_state` serializes every allocation, reads `/proc`, and fsyncs
      under the task-manager lock, so a burst on a dense node is quadratic.
      Group-commit off the lock or keep a record per allocation
      (`legion_task_start_stage_seconds{stage="register"}`).
- [ ] **TAP sandboxes shell out to `ip` on the start path**
      (`sandbox_checkpoint::HostTaps`, `sandbox::set_tap_link`). Use the
      ioctls instead, if the `network` stage shows up on a real KVM node.
- [ ] **Finish moving worker blocking work off the task-manager lock (audit
      W3).** Still under it: `reap_terminating`, `record_failure`/self-fence,
      `prepare`/`start` for `ratify_claim`, `restart_allocation`, edge-boot
      starts, snapshot restores (an `eager` restore holds it for the whole
      read), and `container stop` on reconcile. `heartbeat_tick` locks a std
      mutex from async; teardown needs a "tearing down" state
      ([calls #11](calls/architecture-audit-2026-10-09_calls.md)). Also under
      it: a start's filesystem work (`prepare_instance`, log dir and sink), a
      stop's fsync of the worker state file, and the runtime's memory
      `checkpoint`.

### Host runtimes (`exec`, `container`, new runtimes)

- [ ] **Supervision plan leftovers**: PID/IPC/UTS namespaces and seccomp for
      `exec` (a PID namespace needs a double-fork,
      [calls 10](calls/host-process-supervision-plan_calls.md)), enforce
      `resources.disk`, scheduler preference for enforcing nodes, and
      privileged CI tests for the cgroup and hardening paths.
- [ ] **A `converge` runtime** — idempotent host resources with per-resource
      `unchanged | changed | failed` reporting (PLANNED)
      ([plan](plans/host-convergence-plan.md) ·
      [calls](calls/host-convergence-plan_calls.md)).
- [ ] **NUMA/SMT-aware core placement.** The core allocator is
      lowest-free-first with no topology awareness; revisit with a workload
      that needs it ([docs](../docs/resource-enforcement.md#not-implemented)).
- [ ] **A delegated (non-root) cgroup hierarchy root** for non-root workers
      ([docs](../docs/resource-enforcement.md#defaults-and-turning-it-off)).
- [ ] **`legion alloc signal`** — an on-demand signal verb. The worker side
      exists (`WorkerTaskManager::signal_allocation`, used by template and KV
      change modes); it needs a plan, a manager-called Worker `SignalTask`, a
      Manager `SignalAllocation` (`job:write`, idempotent), CLI, catalog and MCP.
- [ ] **The `sandbox` runtime cannot take on-demand signals**; only the stop
      signal reaches the guest. The guest agent would implement
      `TaskPlugin::signal` ([docs](../docs/task-runtime-sandbox.md#current-scope-and-limitations)).
- [ ] **Three of the container runtime's `#[ignore]`d OCI tests are
      unexplained under runc** (`exec_runs_inside_the_container`,
      `sees_task_dir_files_and_their_rewrites`,
      `enforces_a_declared_memory_limit`), pending a cgroup v2 rerun
      (`crates/worker/src/task_plugins/container.rs`). The two stop tests were
      stale and are fixed.
- [ ] **Two workers on one host can still collide on cgroup leaves and gated
      netns names.** OCI ids are node-scoped (2026-11-08), but the cgroup leaf
      stays `legion-<alloc>` and netns names carry no node id. The orphan-netns
      sweep skips any namespace a process is in, so run gated tasks on one
      worker per host ([docs](../docs/network-handlers.md)).
- [ ] `exec`: unveil support.
- [ ] Split a task runtime into isolation / runner / artifact parts (idea,
      needs a plan); revisit once `resource_enforcement()` has two consumers.
- [ ] Shared files between tasks in one allocation —
      [proposal](plans/shared-alloc-dir-proposal.md), blocked on multi-task
      allocations.

### Isolates (the `isolate` runtime)

Phases 1–5 are built ([plan](plans/isolate-runtime-plan.md) ·
[calls](calls/isolate-runtime-plan_calls.md) ·
[docs](../docs/task-runtime-isolate.md)); what follows was deferred or
knowingly left limited. Rig and KVM runs are under [Verify](#verify).

- [ ] **Hibernation as a memory image.** A hibernated session isolate keeps
      only its `state` map; memory, globals and handles are lost. Blocked on
      an engine API to read and write a component's memories without
      `guest_debug` (320 µs per instantiation, 2× run time)
      ([calls 9](calls/isolate-runtime-plan_calls.md#9-hibernation-is-a-state-checkpoint-not-a-memory-image)).
- [ ] **The stackless WASI 0.3 world.** Every guest runs on a fiber, so a
      waiting isolate holds a stack and its slot and cannot hibernate
      mid-await (goal G3). Blocked on wasmtime releasing a store's fiber
      stack between calls; re-measure on each engine upgrade
      ([calls 7](calls/isolate-runtime-plan_calls.md#7-every-guest-runs-on-a-fiber-there-is-no-stackless-wasi-03-world-reverses-entry-8)).
- [ ] **`suspend.durability = "store"` for session isolates.** Refused at
      submit (and by the node as a backstop); checkpoints stay on the node,
      so no session isolate survives it, and there is no durable named
      isolate ([calls 59](calls/isolate-runtime-plan_calls.md#59-suspend-on-a-session-isolate-what-applies-what-is-refused-and-durability--store-is-not-built)).
- [ ] **Gateway wake and host-held connections for session isolates.** The
      wake gateway does not hold requests for a hibernated isolate, and no
      connection or WebSocket stays open across hibernation
      ([plan §6.7](plans/isolate-runtime-plan.md#67-ceding-memory-four-resident-tiers)).
- [ ] **Streaming guest logs.** Handler and session isolates' output
      reaches the envelope's task log one batch per invocation, when it
      ends, told apart only by a `[isolate <id> tenant=<key>]` prefix
      ([calls 70](calls/isolate-runtime-plan_calls.md#70-guest-logs-go-into-the-envelope-allocations-task-log-one-prefixed-line-each-once-per-invocation)).
- [ ] **Stage-time pre-initialization of `entry`.** An entry is parsed and
      run on every fresh instance (the 300 µs JS target is unmet); the
      `preinit-image`/`preinit-entry` derived kinds have no builder, and the
      node has no componentize toolchain
      ([calls 42](calls/isolate-runtime-plan_calls.md#42-entry-arrives-through-the-tasks-artifacts-and-is-staged-per-call-not-pre-initialized)).
- [ ] **Dynamic Wasm modules as runs.** A run executes source in an image
      or the task's own module; sending a component to compile and run is
      not built ([plan §6.5](plans/isolate-runtime-plan.md#65-the-start-path-and-compilation)).
- [ ] **ES modules in the `js` image.** StarlingMonkey 0.23 cannot link a
      module from source at run time, so `import`/`export` stops the run as
      `unsupported`; also not observed: unhandled promise rejections that
      are never awaited, and `process.exit` in a timer after the entry
      returns ([calls 50](calls/isolate-runtime-plan_calls.md#50-a-js-entry-is-a-script-es-modules-are-unsupported-files-go-through-legionfs)).
- [ ] **Python packages beyond the standard library** (curated images such
      as `python-data`, plan §14 Q4), and npm packages for `js`
      ([calls 48](calls/isolate-runtime-plan_calls.md#48-python-is-cpython-for-wasi-heavy-packages-run-in-a-vm)).
- [ ] **`js-small` (QuickJS).** Phase 0 measured 5.1× smaller idle than
      StarlingMonkey, meeting the plan's condition; not built
      ([calls 47](calls/isolate-runtime-plan_calls.md#47-the-default-js-image-is-starlingmonkey-quickjs-is-a-candidate-js-small)).
- [ ] **Streaming HTTP bodies.** Ingress, egress and `legion.fetch` buffer
      whole bodies up to `isolate.maxBodyBytes`; no WebSockets, no h2 to
      origins, no connection pooling per envelope
      ([calls 52](calls/isolate-runtime-plan_calls.md#52-egress-the-worker-dials-the-envelopes-own-datapath-listeners-and-upstreams-are-addressed-by-name),
      [53](calls/isolate-runtime-plan_calls.md#53-ingress-one-unix-socket-per-port-label-http11-inside-bodies-buffered)).
- [ ] **The VM task sharing the isolate's `connect` block.** Sandbox session
      tasks refuse `connect` and a gated network, so a VM run or child VM
      has no upstreams or injected credentials; needs a session datapath for
      sandboxes ([calls 65](calls/isolate-runtime-plan_calls.md#65-the-vm-task-does-not-share-the-isolates-connect-block-yet)).
- [ ] **Connection handoff from an isolate to its child VM**, and resuming
      a child that its own idle policy suspended (answers `child-gone`)
      ([calls 33](calls/isolate-runtime-plan_calls.md#33-a-sessions-address-is-always-its-isolate-its-vm-is-reached-through-it),
      [68](calls/isolate-runtime-plan_calls.md#68-a-child-vm-handle-is-bounded-and-reaches-only-the-childs-own-ports)).
- [ ] **Untagged session-isolate ids in the instance verbs.** An id derived
      from an idempotency key carries no node tag; only `isolate get` and
      `session run` take `--on-node`, so stop/suspend/resize cannot find it
      ([calls 43](calls/isolate-runtime-plan_calls.md#43-session-isolates-are-reached-through-their-ids-node-tag-untagged-ids-need---on-node)).
- [ ] **A keyed session-isolate start retried through another manager can
      start a duplicate**; the per-manager memo lasts 10 minutes and the
      index's duplicate sweep does not see isolates
      ([calls 57](calls/isolate-runtime-plan_calls.md#57-a-session-isolate-start-is-confirmed-by-the-nodes-reply-and-is-never-a-keyed-instance)).
- [ ] **Replica rebalancing and shrinking; shard rebalancing.** A cordon's
      replicas only grow and split each task evenly, holders never move
      between shards, and the shard count is fixed at host start
      ([calls 54](calls/isolate-runtime-plan_calls.md#54-a-cordons-capacity-10000-slots-per-host-clamped-by-vmmax_map_count-replicas-only-grow),
      [39](calls/isolate-runtime-plan_calls.md#39-each-host-runs-one-shard-per-core-shared-nothing)).
- [ ] **A `cpu.weight` for the host cgroup.** The cgroup's `memory.max`
      follows envelope resizes and tasks joining or leaving the cordon, and
      the host is placed in it before `exec`, but no CPU weight is set; the
      "cannot page out here" reason is only logged, not in
      `IsolateHostStats`
      ([calls 31](calls/isolate-runtime-plan_calls.md#31-paging-out-is-kernel-reclaim-on-the-hosts-cgroup-not-madvise-per-isolate)).
- [ ] **Per-job labels on isolate metrics** (or another way to break them
      down without the 256-series cap)
      ([calls 71](calls/isolate-runtime-plan_calls.md#71-isolate-metrics-have-no-per-job-labels-and-count-cpu-in-microseconds)).
- [ ] **Per-isolate attribution of egress denials.** `legion net denials`
      names the task; the isolate id and tenant are only in debug logs
      ([calls 10](calls/isolate-runtime-plan_calls.md#10-the-host-protocol-is-grpc-over-two-inherited-socketpairs-with-an-origin-on-every-request)).
- [ ] **Invocation-level idempotency in the host**; a retried call that was
      mid-flight when a host died runs again, and a host crash loses every
      resident session that never hibernated
      ([calls 69](calls/isolate-runtime-plan_calls.md#69-a-host-restart-fails-its-in-flight-calls-and-loses-resident-sessions-that-never-hibernated)).
- [ ] **`operator runtime status` counts 0 sessions for `isolate`**, and the
      `[runtime-disabled]` submit warning fires for an isolate task whose
      `auto` runs go to its VM
      ([calls 44](calls/isolate-runtime-plan_calls.md#44-disabling-the-isolate-runtime-the-node-decides-when-a-vm-can-take-the-run-and-a-drain-hibernates-only-what-it-covers)).
- [ ] **A compile prefetch before a host upgrade** (plan §13): a new
      `legion-isolate` recompiles each module on first use per node.
- [ ] **Phase 6 hardening and density.** `cordonBoundary = "vm"` (refused
      at worker start); MPK striping soak and default (today `isolate.layout`
      is node-wide, an `mpk` host without usable keys silently runs guard
      pages, and few-slot MPK hosts can refuse early); SELinux `execmem`
      denial for hosts (anonymous `PROT_EXEC` is allowed under seccomp
      alone); perf-counter quarantine; Spectre-mode knobs; 5-level paging
      ([plan §11 Phase 6](plans/isolate-runtime-plan.md#11-implementation-phases),
      [calls 14](calls/isolate-runtime-plan_calls.md#14-the-host-confines-itself-before-it-starts-a-thread-and-refuses-to-run-when-a-layer-is-missing),
      [22](calls/isolate-runtime-plan_calls.md#22-slots-use-guard-pages-by-default-mpk-striping-is-phase-6-the-layout-belongs-to-the-cordon-not-the-job)).
- [ ] **Drop the host's and the compiler's kernel uid.** They run as the
      worker's uid (usually root), 65534 only inside their user namespace;
      under `enforce` the mount root and Landlock leave that uid only the
      artifact directories, read-only, but a `best-effort` host missing
      both keeps root's file access. Needs a dedicated unprivileged uid per
      node or a subordinate id range
      ([calls 18](calls/isolate-runtime-plan_calls.md#18-the-host-keeps-the-workers-kernel-uid)).
- [ ] **Track in-flight invocations per host replica**, not per cordon:
      one replica can name an invocation in flight on a sibling replica of
      the same cordon for child-VM calls
      ([calls 28](calls/isolate-runtime-plan_calls.md#28-a-host-may-act-only-as-isolates-the-worker-issued-to-its-cordon-in-flight-invocations-are-tracked-per-cordon-not-per-replica)).
- [ ] **Bound a host call's bytes while it is decoded.** The worker limits
      each host call to `max(maxBodyBytes, 16 MiB) + 1 MiB` and `shards × 8`
      calls (16 to 64), so up to 64 × 17 MiB can be decoded at once per host
      replica; tonic buffers each message whole, and a connection-wide
      decode budget needs a custom body layer. A child-VM `put` or `exec`
      stdin over the decode limit fails at the transport
      ([calls 46](calls/isolate-runtime-plan_calls.md#46-the-worker-bounds-what-a-host-can-make-it-hold)).
- [ ] **Host heap from `wasi:http` is not charged to `memoryMax`.** Up to
      8 × `isolate.maxBodyBytes` of bodies each way per invocation (plus
      header sets) lives outside the isolate's account; there is no
      per-cordon egress limit in the worker
      ([calls 45](calls/isolate-runtime-plan_calls.md#45-a-guests-outbound-requests-at-most-8-in-flight-per-invocation-the-rest-wait-and-none-outlives-the-invocation)).
- [ ] **Tenant keys are caller-chosen.** Any caller who may run in the task
      and any HTTP client of an advertised port can name any tenant; the
      review's suggested jobspec switch to ignore a client-supplied
      `Legion-Tenant` on ingress is not built
      ([calls 27](calls/isolate-runtime-plan_calls.md#27-a-tenant-key-is-a-routing-key-not-an-identity-a-call-in-a-session-uses-the-sessions-tenant)).
- [ ] **Smaller items from the isolate security review** (kept open, low):
      `GetIsolate --on-node` answers `node X not found` for an unknown node
      to any caller with `job:read` on some job; `legion isolate stats`
      shows a shared cordon's host RSS and slots, so a reader of one job
      sees co-tenants' activity; the permissions of the Unix sockets under
      `<allocs>/<id>/.isolate` depend on the worker's umask and were not
      traced; whether target distributions enable Landlock (which decides
      how often a `best-effort` host lacks it) is unknown; the checkpoint
      fallback directory's `file_safe` maps `a/b` and `a_b` to one name
      (unreachable with minted ids); for 30 s after an invocation ends a
      compromised host can still write log lines under that invocation's id
      ([calls 70](calls/isolate-runtime-plan_calls.md#70-guest-logs-go-into-the-envelope-allocations-task-log-one-prefixed-line-each-once-per-invocation));
      manager-side existence answers before the job is known (an evicted
      session's `NOT_FOUND`, `UNAVAILABLE` naming the node, `already ended`
      for a recent stop, and the worker's ticket path checking after its
      lookup) ([calls 35](calls/isolate-runtime-plan_calls.md#35-a-session-id-the-caller-may-not-read-answers-not_found-for-every-session-verb-not-only-isolates));
      a release pipeline built with `--all-features` would ship the
      `confinement-test` hooks (the host then says so in its `Hello`
      notes). Outside the isolate runtime, the git
      fetcher keeps symlinks and archive links get only a lexical check;
      `config.entry` no longer follows links
      ([calls 17](calls/isolate-runtime-plan_calls.md#17-configentry-is-read-beneath-its-staging-directory-with-a-no-symlink-walk-on-every-kernel-not-openat2)),
      but other readers of a staged tree must not either.
- [ ] **Fuzz targets** for host-function argument decoding and the WIT
      boundary, run nightly; the escape corpus exists, fuzzing does not
      ([plan §12](plans/isolate-runtime-plan.md#12-testing-plan)).
- [ ] **aarch64.** x86-64 Linux only, by design until continuous fuzzing
      covers aarch64 Cranelift (plan §14 Q8).
- [ ] **Ship `legion-isolate` and the images in a release**: a third
      artifact per architecture beside `legion-guest`, with `isolate-images/`
      and its manifest ([development notes](development.md#the-isolate-binary-legion-isolate-and-its-images)).
- [ ] **Reproducible image builds.** Two builds from the same inputs differ
      (snapshot random state and hash seed), so a release must pin the
      built artifact; nobody can verify a shipped image by rebuilding it
      ([calls 75](calls/isolate-runtime-plan_calls.md#75-the-image-manifest-is-written-from-the-built-images-builds-are-not-reproducible-so-a-release-pins-the-artifact)).
- [ ] **V8 (Phase 7) and continuation migration (Phase 8)** stay behind the
      plan's trigger and research status
      ([calls 1](calls/isolate-runtime-plan_calls.md#1-the-engine-is-wasmtime-js-and-python-run-as-interpreters-inside-wasm-not-on-v8)).

### Worker robustness

- [ ] **A boot-fence report is lost if the worker crashes before its next
      heartbeat.** The `boot_fenced.json` record is cleared at restore and the
      failure is queued in memory; the manager then learns only from absence
      ([docs](../docs/stateful-workloads.md)).

### Sandboxes

- [ ] **A sandbox resized below its boot size and then suspended resumes
      cold**: since `memory_mib` is gone the checkpoint holds the boot-size
      guest. Restore should take the checkpoint's guest size, re-apply the
      balloon floor, and raise the VMM cgroup ceiling to match
      ([docs](../docs/resize.md)).
- [ ] **Sandbox base root disk cache, later phases**
      ([plan](plans/rootfs-base-cache-plan.md) ·
      [calls](calls/rootfs-base-cache-plan_calls.md)):
  - [ ] Phase 2: sandboxes with file, tree, or layer artifacts (and artifact
        kernels) still build their disk in full each start (calls 5).
  - [ ] Phase 3: `container` still extracts every image per allocation.
- [ ] **Gated sandboxes by default, with memory checkpoints**
      ([plan](plans/gated-sandbox-checkpoints-plan.md) ·
      [calls](calls/gated-sandbox-checkpoints-plan_calls.md)); Phases 1–5
      built without KVM:
  - [ ] Flip the default once G0.1 and G2.1 pass: `SANDBOX_GATED_BY_DEFAULT`
        in `crates/common/src/structs/network.rs`, the docs that say `auto` means `host`,
        and drop `LEGION_SANDBOX_GATED_DEFAULT` (calls 2).
  - [ ] Decide the Firecracker 1.17 floor for ingress (RX stick after pause,
        #6100) from the G5.1 measurement (calls 20).
  - [ ] An ingress `proxy` hop cannot reach a port inside a sandbox guest
        ([docs](../docs/network-handlers.md#ingress-into-a-sandbox)).
  - [ ] TAP-wired and loopback-only sandboxes cannot reach
        `connect.upstreams` and submit does not refuse them (calls 25).
  - [ ] An upstream removed while suspended leaves a dead listener in the
        restored guest (calls 14).
  - [ ] After a restore that moved a port, the new allocation's
        `LEGION_PORT_<label>` differs from the guest's real port (calls 13).
  - [ ] The egress proxy records service-graph bytes only when a splice
        ends (`crates/worker/src/worker/netd/proxy.rs`).
  - [ ] A gated resolver forwards queries while policy is pending
        (calls 22).
  - [ ] TAP guests are IPv4 only; a kernel without `CONFIG_IP_PNP` boots
        unaddressed until its first restore ([docs](../docs/task-runtime-sandbox.md#addressing)).
- [ ] **Sandbox exec, what is left** ([plan](plans/sandbox-exec-plan.md) ·
      [calls](calls/sandbox-exec-plan_calls.md)):
  - [ ] Bind a direct-exec ticket to the client's key (`cnf` + proof over
        `jti`); today whoever redeems first wins (plan §13).
  - [ ] Direct sessions minted by a JWT credential have no accessor to
        re-check; they end only at `max_session` (calls 12).
  - [ ] JWKS refresh and clock-skew settings live under `edgeBoot` even when
        only direct exec uses them (calls 11).
  - [ ] Phases 4–5: the gateway tier, and exec into unnamed sessions.

### Sessions and sandbox scaling

- [ ] **Sandbox scaling to very large session counts**
      ([plan](plans/sandbox-scaling-plan.md) ·
      [calls](calls/sandbox-scaling-plan_calls.md) ·
      [docs](../docs/sandboxes-at-scale.md)); Phases 0–6 shipped:
  - [ ] Build the missing bottleneck harnesses (loadgen session mode,
        sessions in `--simulate`, fixed-delay launch, sessions.db and
        ticket-cache benches) and run `benchmarks/aws/README.md`'s list.
  - [ ] Retried publishes are not pushed over the stream
        ([calls](calls/checkpoints-on-stream-plan_calls.md) #7).
  - [ ] Edge-boot provisional deltas still use `PostWork` (calls 39).
  - [ ] Checkpoint budget reports and `snapshot warm` do not wake blocked
        evals (calls 42).
  - [ ] A resubmit and a scale-in still settle in a separate entry
        ([calls](calls/multi-eval-plans-plan_calls.md) #5).
  - [ ] Rollout ticks and node-scoped passes do not block (calls 41).
  - [ ] Idle sessions: suspend an unpromoted session to a bucket manifest
        (plan §6.5, calls 18, 50).
  - [ ] Workload identity for sessions (plan §6.3, §6.8; calls 18, 33, 34).
  - [ ] Ship the session log off the node and journal per-session events
        (plan §6.4; calls 19, 32, 37).
  - [ ] More ways to promote: `promote.onVolume`, `snapshot create --name`.
  - [ ] Ports, services, and network policy for sessions (refused today in
        `validate_session_task`).
  - [ ] Sessions at scale on simulated nodes and multi-manager (plan §13
        10^4 run, leader-kill check; calls 58).
  - [ ] Router locality by image (deferred; revisit once checkpoint chunks
        are shared across generations, calls 27).
  - [ ] Ended sessions are remembered per manager, so a retried stop on
        another manager answers `NOT_FOUND` (calls 55).
  - [ ] Batch session creates (`CreateInstance` with `count`, calls 57).
  - [ ] Elastic envelopes: preempt only the grown part, and carry the old
        envelope's size across a job update
        ([calls](calls/elastic-session-envelopes-plan_calls.md) #5, #6).
  - [ ] Phase 5's 10^6 archive acceptance run, and an archive stage in
        `tests/raft_lifecycle_ops.rs` (calls 8).
  - [ ] Phase 7 wake for sessions; Phases 8–10 (snapshot-clone density,
        sub-VM tier seam, cells past §6.11's trigger).
- [ ] **Wake on request** ([plan](plans/wake-on-request-plan.md) ·
      [calls](calls/wake-on-request-plan_calls.md) ·
      [docs](../docs/gateway.md)); Phase 1 shipped:
  - [ ] Phase 2: gateway metrics on a `/metrics` listener.
  - [ ] Phase 3: wake `session`-mode sessions from bucket manifests.
  - [ ] Phase 4: node-local wake on connect, for clusters without a gateway.
  - [ ] Pass the client address (`Forwarded`) on kept-alive connections.
  - [ ] Reload the gateway's TLS certificate without a restart.
- [ ] **Park** ([plan](plans/park-plan.md) ·
      [calls](calls/park-plan_calls.md) · [docs](../docs/park.md));
      Phases 1–2 shipped:
  - [ ] Phase 3: timed wake (`park until=<time>`).
  - [ ] Phase 4: Legion-run tools wake their caller (`--notify <instance>`).
  - [ ] Phase 5: persist pending parks and `busy` across a worker restart
        (calls 7).
  - [ ] Publish a keyed park's checkpoint under `durability = "store"`
        (calls 6; changes pressure suspends too).
  - [ ] Park for `container` tasks on macOS needs a vsock-style relay.

### Suspend, resume, fork, and oversubscription

- [ ] **Suspend, resume, fork, and oversubscription**
      ([plan](plans/suspend-resume-and-oversubscription-plan.md) ·
      [calls](calls/suspend-resume-and-oversubscription-plan_calls.md) ·
      [docs](../docs/suspend-and-resume.md)). Phases 1–7 and Tracks 1–2
      shipped; Linux/KVM checks are in the verify list. Still open:
  - [ ] Node usage lives on the manager that got the heartbeat; another
        manager reports `usage_reported: false`.
  - [ ] Memory snapshots copy only the instance volume; refuse or snapshot
        attached block claims too.
  - [ ] Restore and fork are keyed-only; slot instances cannot be restored.
  - [ ] A pending restore has no `restore-pending` detail in `instance get`
        (plan §6.13).
  - [ ] `SnapshotReportOutcome::Restored` does not carry the snapshot's kind,
        so `legion_snapshot_restore_seconds` reads the snapshot row per report.
  - [ ] Hash checkpoint parts once, on ingest (capture hashes twice and a
        memory suspend stays paused while hashing).
  - [ ] Ship a static (musl) resume handler so musl guests get the refresh.
  - [ ] `reserve = true` holds are dropped by a job update or stop.
  - [ ] A checkpoint is `lost` only when its node is removed; a down node
        leaves the resume waiting.
  - [ ] A pressure suspend's pending report is lost on worker restart;
        persist pending local suspensions (calls 19).
  - [ ] Idle policy needs resource enforcement and cannot see raw host
        ports (calls 20, 59); revisit the 3-minute `idle.after` default with
        fleet data; surface a persistent idle-sweep skip as a `detail`.
  - [ ] cloud-hypervisor forks: switch to `net_fds` if a release restores
        net devices from `state.json`; page sharing needs
        `copyonwrite` restore, unreleased as of v53.
  - [ ] Forks copy guest addresses, so a static IP collides on the bridge;
        reassign in the resume handler if platforms need it.
  - [ ] A fixed-TAP task still runs one instance per node (no per-instance
        TAP for fresh boots).
  - [ ] Caller-supplied fork ids (`ForkInstanceRequest.instance_ids`).
  - [ ] Spread is enforced only on a fork's first placement; a spread
        placement can preempt and then skip, wasting the eviction.
  - [ ] Volume migration so a `local-dir` volume can follow a published
        checkpoint (call 8).
  - [ ] Manual checkpoint key rotation (`legion keyring checkpoint-key
        rotate`) ([calls 8](calls/checkpoint-encryption-plan_calls.md)).
  - [ ] Chunked publish Phases 2–3: a node-local chunk index plus released
        parts kept as cache, and dirty-bitmap reuse on Firecracker
        ([plan](plans/chunked-checkpoint-publish-plan.md#8-implementation-phases)).
  - [ ] `snapshot publish <id>` for a snapshot whose retries are spent.
  - [ ] Check a published resume for room to fetch its parts (call 134).
  - [ ] Publish pressure checkpoints after recovery under `durability =
        "store"`, and surface the idle skip (call 44).
  - [ ] Warms are leader-local and lost on restart; show their status
        (call 96).
  - [ ] With `plan_candidates.count = 0` restores bind in binder order;
        carry the instance id in format 1 or drop the kill switch.
  - [ ] Multi-manager: the content location index lives on the heartbeat's
        manager; forward to the scheduler-lane leader.
  - [ ] `contentStore.auth.nodeIdentity` (STS `AssumeRoleWithWebIdentity`).
  - [ ] Image layers in the content set, once OCI blobs live in the store.
  - [ ] Plan-and-apply is ~8% slower than main since Track 2; profile the
        apply transaction.
  - [ ] Track 2: the budget filter can overfill a node within one plan
        (call 47); a held share is not re-charged when `reserve` changes
        (call 37); make-room needs usage reports and its cooldown is fixed.
  - [ ] Dev mode (Apple `container`): env tokens are not mirrored into
        `/legion/identity`, and `container stats` runs on the heartbeat
        path (bounded at 5 s); move it off the loop.
  - [ ] Lazy memory Phases 2–3: stream a published restore's memory from
        the bucket, and prefetch the recorded working set (REAP)
        ([plan](plans/lazy-memory-restore-plan.md)).
  - [ ] Decide whether the content store needs a background scrub now that
        restores trust digests (lazy memory plan, open question 2).

## Rollouts

Plan: [rollout-strategy-plugins](../docs/rollout-strategy-plugins.md) · docs: [rollouts.md](../docs/rollouts.md)

- [ ] **A rollout's inherited backoff counts toward `healthyDeadline`, and
      backoff chains and `moveCount` stay per version.** A replacement waits out
      the previous version's hold (2026-11-08), but the wait does not reset the
      step's health clock, and a new version's first failure starts again at
      `moveDelay` ([docs](../docs/rollouts.md)).
- [ ] **Blue-green and canary strategies.** Neither exists yet.
- [ ] **Rollback as a new job version (audit SC1).** Rollback is terminal, so a later
      reschedule, KV restart, or `job scale` still targets the rolled-back config.
      Needs a plan ([calls #5](calls/architecture-audit-2026-10-09_calls.md)).
- [ ] **Rollout plugins have one hook** (`resolve`; no step/analysis/abort hooks;
      [review PLG5](architecture-review-2026-11-07.md)). Needs a plan
      ([calls 25](calls/device-model-and-plugin-seams-plan_calls.md#25-job-mutation-a-secrets-provider-rollout-hooks-and-controller-side-volumes-are-deferred)).

## Artifacts

Plan: [artifact-system-plan.md](plans/artifact-system-plan.md) · docs: [artifacts.md](../docs/artifacts.md)

- [ ] **Artifact cache and pre-warming, what is left** ([plan](plans/artifact-cache-and-prewarm-plan.md)):
  - [ ] Mutable-source caching with TTL revalidation and a jobspec `ttl`; today only
        `checksum`ed artifacts are cached ([call 65](calls/suspend-resume-and-oversubscription-plan_calls.md#65-the-node-content-store-caches-only-digest-pinned-single-files)).
  - [ ] Cache trees (git, dirs) and OCI images per blob, then default
        `artifacts.containerImageDesugar` on.
  - [ ] Phase 2: `WarmArtifacts` RPCs and policies; pre-fetch as part of deploy.
  - [ ] `legion artifact ls/gc` (CLI + MCP); UI cache hits and a Nodes-page cache card.
- [ ] **`identity` / `nodeIdentity` artifact auth.** Fetch-time use still errors.
- [ ] **`onFetchFailure` policy**, so fetch flakiness stops eating restart/move budgets.
- [ ] **Push container images straight into a worker's cache** (no registry needed).
- [ ] **Peer-to-peer distribution and lazy loading** (Dragonfly/Spegel, SOCI/Nydus;
      [review AI6](architecture-review-2026-11-07.md)). Needs worker-to-worker auth and
      a plan ([calls 23](calls/device-model-and-plugin-seams-plan_calls.md#23-p2p-distribution-lazy-loading-and-resumable-fetches-are-deferred)).
- [ ] **Resumable fetches and a stall timeout**; the 2 h deadline is the only guard
      ([calls 16](calls/device-model-and-plugin-seams-plan_calls.md#16-the-fetch-defaults-grow-to-1-tib-and-2-hours-there-is-no-stall-timeout)).

## Stateful & volumes

Plans: [stateful-workloads](plans/stateful-workloads-and-instance-identity-plan.md), [volume-plugin-system](plans/volume-plugin-system-plan.md) · docs: [volumes.md](../docs/volumes.md), [stateful-workloads.md](../docs/stateful-workloads.md)

- [ ] **Session durability and routing, what is left** ([plan](plans/session-durability-and-routing-plan.md)
      · [calls](calls/session-durability-and-routing-plan_calls.md) · [docs](../docs/session-mode.md)):
  - [ ] Remove the manager-routed derived-id fan-out (plan §8 item 4) once measured.
  - [ ] No test for the edge move over worker-to-worker TLS, nor end-to-end
        periodic / SIGTERM checkpoints and restore after restart.
  - [ ] Sweep `store` manifests whose job is gone (calls 33).
  - [ ] A moved `store` session runs the receiver's envelope version (calls 29);
        finding one scans every job and same-named tasks are ambiguous (calls 37).
  - [ ] Evicted/unreachable answers precede the job's ACL scope; return the job id.
  - [ ] Phase 6 (conditional, §6.8): batched raft session directory.
- [ ] **CSI bridge plugin.** The driver contract was designed for it.
- [ ] **Volume snapshot follow-ups** ([docs](../docs/volumes.md#snapshot-restore-and-clone)):
      a `legion volume snapshot` surface (if wanted), `expand`, and `clone` locality
      (`cloneScope: node | cluster`).
- [ ] **Multiple volumes per instance** (`stateful.volumes: [...]`; purely additive).
- [ ] **Cross-node data migration**; manual backup → re-mint → restore today.
- [ ] **Mount concurrently with artifact staging** (a start-latency win).

## Service discovery & the mesh

Plans: [service-mesh-plan.md](plans/service-mesh-plan.md), [service-discovery-final-plan.md](plans/service-discovery-final-plan.md) · docs: [service-discovery.md](../docs/service-discovery.md), [network-handlers.md](../docs/network-handlers.md)

- [ ] **Datapath plugin ABI + bridge/CNI shim** (`mode: 'auto'` picking a real overlay).
- [ ] **`legion net doctor` ignores live traffic**; pull the service-graph edges in.
- [ ] **A watchable catalog** (`WatchService` or health events on `StreamEvents`;
      [swappable seams plan](plans/swappable-seams-plan.md) §1), for outside load
      balancers and for the bundled ingress, which polls `ResolveService` every 2 s
      ([calls 9](calls/ingress-app-plan_calls.md)).
- [ ] **Configurable node port ranges** (task 20000–32000, datapath 61000–65535 are
      constants; [calls #7](calls/mesh-control-path-plan_calls.md)).
- [ ] **Endpoint pushes reach only streaming workers, and templates wait for the next
      beat** ([docs](../docs/service-discovery.md#how-endpoints-reach-a-node)).
- [ ] **The endpoint index's `local_disk` health path is a store hook**; project
      `allocation_health` in `local_disk` ([calls #1](calls/mesh-control-path-plan_calls.md)).

## Networking

Plan: [network-handlers-plan.md](plans/network-handlers-plan.md) · calls: [network-handlers-plan_calls.md](calls/network-handlers-plan_calls.md). M1–M10 shipped; the datapath half is unplanned.

- [ ] **Phase 7: UDP (SOCKS5 `UDP ASSOCIATE`) and transparent capture** (TUN +
      userspace stack) for tools that ignore `HTTP_PROXY`.
- [ ] **Leftovers from the shipped mesh phases:**
  - [ ] Ingress: PROXY v2 to the task; drain/grace sets lost on worker restart; no
        `proxy` hop into a gated task's namespace.
  - [ ] mTLS: revocation beyond lifetime; workload certs on demand (`certFrom = "legion"`).
  - [ ] Service graph: ingress attribution, persistence across failover.
  - [ ] Templates: `{{ kv }}` and conditionals in `connect.templates`; sandbox guests.
  - [ ] Hardened `exec` keeps the host `resolv.conf` (no `<key>.svc.legion`).
- [ ] **The `legion netd` process split** (service-mesh calls 17): listeners that
      outlive the worker, so re-adopted tasks keep upstream connections
      ([upgrades.md](../docs/upgrades.md#restarting-a-worker-in-place)). Needs a wire
      encoding for `NetdInput` (no secrets in `netd_state.json`) and a split of
      `ForwarderContext` ([calls #2](calls/swappable-seams-plan_calls.md)).
- [ ] **Unplanned datapath:** overlay network, CNI, networking fingerprinters, port
      forwarding.
- [ ] **ACME HTTP-01/TLS-ALPN-01 for a task's own TLS port** with no ingress in
      front. The bundled ingress answers HTTP-01 for the hosts it routes
      ([ingress.md](../docs/ingress.md#automatic-certificates)).

### Mesh plugins

[plan](plans/mesh-interfaces-plan.md) · [calls](calls/mesh-interfaces-plan_calls.md)

- [ ] **`oci://` plugin modules** (single `.wasm` files only; calls 31).
- [ ] **Streaming request bodies for plugins** (`body` buffers; calls 33).
- [ ] **Pool `dispatch_http_call` connections** (calls 39).
- [ ] **Plugin log lines in the allocation's logs** (calls 53).
- [ ] **Plugin ticks and shared queues** (ticks never fire; calls 54).
- [ ] **Attribute the plugin `request`-stage cost on the HTTP path** (+25–70 µs vs
      <7 µs in process; plan B1 unmet).
- [ ] **A plugins page in the UI**; **per-rule kernel lines in `net policy show`**.

### Credential injection

[plan](plans/egress-credential-injection-plan.md) · [calls](calls/egress-credential-injection-plan_calls.md) · [docs](../docs/network-handlers.md#credential-injection)

- [ ] **Phase 3: webhook provider and non-header placements** (query, basic auth).
- [ ] **Phase 4: opt-in `net.credential_used` audit events.**
- [ ] **TCP, ingress, and gated-proxy `http://` hand-offs dial a proxy in
      plaintext** and cannot reach a mesh port. HTTP-mode upstream hand-offs go
      over the mesh since 2026-11-08 ([docs](../docs/network-handlers.md)).
- [ ] **Credentials on policy objects**, with `kv:read` enforced (plan §11).

### Datapath performance

[plan](plans/datapath-performance-plan.md) · [calls](calls/datapath-performance-plan_calls.md)

- [ ] **kTLS** so mesh and ingress TLS flows splice (~20% bulk; plan §6.6).
- [ ] **Multiplexed node-to-node L4 tunnels (HBONE-style).** Wire-format change; own
      plan ([review NET3](architecture-review-2026-11-07.md)).
- [ ] **HTTP/2 between nodes in HTTP mode.**
- [ ] **Read nftables counters over netlink**, not by forking `nft` ([review NET6](architecture-review-2026-11-07.md)).
- [ ] **Intern endpoints** to drop per-dial clones ([review NET8](architecture-review-2026-11-07.md)).

### macOS gated networking

[plan](plans/macos-gated-plan.md) · [calls](calls/macos-gated-plan_calls.md) · [docs](../docs/network-handlers.md#gated-mode-on-macos)

- [ ] **`auto` resolves to `gated` on macOS 26** once the hardware run passes.
- [ ] **Upstreams from ungated Apple containers** (loopback is the VM's own; needs a
      per-VM `rdr` and a root worker, plan §6.6).
- [ ] **DNS for confined tasks** (calls 5); **a configurable gated subnet** (calls 4).

### Datapath simplification candidates (PR #51 survey)

Each removes code but needs a decision:

- [ ] Drop `legion_sign` and plugin credentials (~300); `legion net plugin new` (~190);
      the gated `:1081` capture listener (~200).
- [ ] No-behavior-change cleanups: one held-material store (~120), shared gated/`netd::http`
      plumbing (~120), the test-only policy-less forwarder (~70), port wrappers (~25).

## Persistence & cluster operations

Plans: [multi-raft](plans/multi-raft-persistence-plan.md), [durability-and-ops](plans/durability-and-ops-execution-plan.md), [manager-workload-identity](plans/manager-workload-identity-plan.md) · docs: [state-store.md](../docs/state-store.md), [raft-operations.md](../docs/raft-operations.md), [persistence-streaming.md](../docs/persistence-streaming.md)

- [ ] **A failed `local_disk` mutation keeps the writes it made before it
      failed**; raft rolls the whole entry back (per-op savepoint), so the two
      modes disagree. A savepoint around each local body is not enough: bodies
      publish events (`publish_staged`) and wake evals right after their
      `write_tx` commit, which would then precede the real commit. Defer those
      to the outer release first (`capture::local_projected_mutation`;
      `local_plan_batch_broadcasts_no_event_before_it_commits` pins the order).
- [ ] **Lost nodes are never garbage-collected**; `legion node forget` is manual. Add
      `node.gcAfter` (default off; [autoscaler-v2-plan.md](plans/autoscaler-v2-plan.md) §11).
- [ ] **A manager with no raft leader holds a read instead of failing it.** Not
      located (2026-11-08): `ListJobs` reads local SQLite, and a dead openraft
      core fails fast. Suspects are `Raft::ensure_linearizable`'s untimed wait
      and the pipeline caller's untimed `reply_rx.recv()` (`raft/driver.rs`),
      which can pile `replicated()` callers onto every runtime thread when a
      leader has no quorum. A timeout needs an ambiguous-outcome answer, since
      the queued write may still commit ([calls
      #2](calls/join-token-file-and-cli-timeouts_calls.md)).
- [ ] **`--restore-from` should take an `s3://` replica URL**, not only a local file
      ([plan §9](plans/manager-workload-identity-plan.md#9-implementation-phases)).
- [ ] **Credential introspection for streaming persistence** (resolved source in the
      status RPC, catalog, MCP; plan §9 phase 3).
- [ ] **EKS Pod Identity, `~/.aws/credentials`, SSO for streaming persistence**;
      blocked on `object_store` 0.11 ([calls #5](calls/manager-workload-identity-plan_calls.md)).
- [ ] **Delete the ~30 no-op read-model hooks** after a release with the shadow check
      ([calls 3](calls/read-model-coherence-plan_calls.md)); **shadow check for raft
      replicas** needs a per-lane barrier.
- [ ] **Raft lanes, what is left:** per-lane tuning knobs and a Raft health UI page;
      idempotency records not atomic with their mutation (duplicates on retry);
      `destroy_job` half-applies under a standing split; (speculative) autopilot.
- [ ] **Cluster federation** ([plan](plans/multi-cluster-federation-plan.md) PLANNED ·
      [calls](calls/multi-cluster-federation-plan_calls.md)).
- [ ] **Scheduler sharding** ([proposal](plans/scheduler-sharding-strategies-proposal.md)):
      pool-keyed shards with their own state file, or plan shipping (M3). Gated on
      Phase 0 measurements.
- [ ] **Node configuration jobs** ([host convergence plan](plans/host-convergence-plan.md) phase 3).
- [ ] **Reconfigure workers from the manager** (CRUD; nodes reference their type).
- [ ] **Root-privilege split**: networking/volume setup as root, the task not.
- [ ] **Stream raft snapshots as files (audit R5)** ([calls #26](calls/architecture-audit-2026-10-09_calls.md));
      **`state_backup`'s `VACUUM INTO` runs under the capture lock.**
- [ ] **An older-schema manager can be sent a newer snapshot (audit R2)** and won't start.
- [ ] **Streaming fencing is time-based (audit R4)**; a paused primary can ship once more.
- [ ] **No raft leadership transfer on shutdown (audit M3)**; openraft 0.9 lacks it.
- [ ] **Native GCS and Azure object storage** (`crates/worker/src/objects/`).
- [ ] **Worker object-store principal resolves on first use, not boot**; decide if
      `contentStore` refuses ambiguous AWS envs ([calls #3](calls/swappable-seams-plan_calls.md)).
- [ ] **Measure `file://` publish throughput** vs the old direct write (calls #17).
- [ ] **The raft log format is the SQLite schema** (review ST1; [calls #8](calls/raft-write-path-plan_calls.md)).
- [ ] **Data migrations can only touch control-lane tables** ([calls #9](calls/raft-write-path-plan_calls.md)).
- [ ] **Watch gaps** ([plan](plans/revisions-and-watches-plan.md)): `WatchObjects` on
      the journal with resume; journal lost on manager restart; no watches for nodes,
      rollouts, evals; `--index` on list CLIs; instance-only changes in `local_disk`.
- [ ] **Make `Store::replicated` async** ([review ST5](architecture-review-2026-11-07.md)).

## Job types

- [ ] **Per-replica job outcomes** (one failed replica can read `complete`;
      [calls 4](calls/job-status-plan_calls.md)).
- [ ] **Job status-change events** (`job.status_changed`; [plan](plans/job-status-plan.md)).
- [ ] **Sysbatch and system jobs** (maybe a richer `count`; see node configuration jobs).

## KV & secrets

Plan: [kv-encryption-plan.md](plans/kv-encryption-plan.md) · docs: [kv-encryption.md](../docs/kv-encryption.md)

- [ ] **Encrypt the rendered copies of KV values, or stop making them** (job/task
      configs, worker state, task dirs; plan §6.9). Needs a plan.
- [ ] **Native KMS root-key providers** (deferred; `exec` covers them, calls #7).
- [ ] **A `passphrase` / manual-unlock root-key provider.**
- [ ] **Missed KV deletes under `restart`/`rollout` are undetectable (audit M4)**
      ([calls #15](calls/architecture-audit-2026-10-09_calls.md)).
- [ ] **A `none` or `update` KV move during a rollout moves only the previous
      config's allocations.** Both actions migrate running allocations in place
      (`move_allocations_in_place`); an older version still rolling out stays on
      its config ([docs](../docs/kv-store.md)).

## Auth, TLS & security

Plan: [auth-acl-plan.md](plans/auth-acl-plan.md) · docs: [auth-and-acls.md](../docs/auth-and-acls.md)

- [ ] **Namespaces, what is left** ([plan](plans/namespaces-plan.md) · [calls](calls/namespaces-plan_calls.md)):
  - [ ] Quota admission scans the cluster in the control-lane write; scale-out and
        instance-create quota checks are racy pre-checks (call 7).
  - [ ] Unnamed sessions aren't charged (call 29); tenant-authored net policies (§8.7).
  - [ ] Not-found hint on more RPCs; namespace on volume routes and replies (call 30).
- [ ] **Deferred auth:** auth-method dylib plugins; workload-identity API credentials;
      token-resolution cache.
- [ ] **Autoconfigured, auto-rotated TLS for the CLI and UI.** Plan first (public cert
      vs self-signed CA). Includes the https CA default
      (`configs/tls/rootca.pem`), which is relative to the working directory.
- [ ] **A join secret can claim a `lost` node's name** in the same pool
      ([calls #3](calls/architecture-audit-2026-10-09_calls.md)); a token for
      another pool is refused since 2026-11-11. Closing it needs per-node
      bootstrap credentials, which overlaps the next item.
- [ ] **A restarted worker still needs its join token string** ([calls #6](calls/join-token-file-and-cli-timeouts_calls.md)).
- [ ] **Push direct exec revocations** instead of waiting a heartbeat ([calls #18](calls/security-gap-closure-plan_calls.md)).
- [ ] **Run `exec` health checks inside the task's runtime** ([calls #4](calls/security-gap-closure-plan_calls.md)).
- [ ] **Per-manager raft transport certificates** ([calls #1](calls/security-gap-closure-plan_calls.md)).
- [ ] **Cluster CA:** switch providers without re-joining workers
      ([calls #13](calls/cluster-ca-provider-plan_calls.md)); compare every manager's
      CA view; vault-pki against live Vault in CI; upstream-authority mode.

## Observability

docs: [observability.md](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- [ ] **A trace viewer in the UI** (dev-only first).
- [ ] **The Nickel step budget is fixed** at 1,000,000 (`EVAL_STEP_BUDGET`);
      a legitimately heavy config (sorting thousands of generated records)
      fails. Make it configurable if users hit it ([calls](calls/nickel-eval-budget_calls.md)).
- [ ] **Per-node metric families fold into `"other"` past 256 nodes**
      (`MAX_SERIES_PER_FAMILY`), so every `{node}` gauge is partial on a large
      fleet. Needs a different cap or a per-pool aggregate. There is also no
      per-node CPU-reservation gauge (only `legion node usage`).
- [ ] **Cluster-wide per-task CPU/memory view** (with autoscaler scaling inputs).
- [ ] **Usage accounting follow-ups** ([calls #10](calls/agent-governance-plan_calls.md)):
      enforcing budgets, measured usage, durable submitter attribution, device-hours.

## Templates, registry & packaging

Job templates shipped ([plan](plans/job-templates-plan.md)); the registry is untouched.

- [ ] **A job registry**: default + added registries, `@<registry>/<job>`, inspect
      metadata, a UI to fill in variables; plugins by URL or registry.
- [ ] **Build & push images** (`legion build`/`prep`); a buildpack runner.
- [ ] **Publish the next `legion-rs` to crates.io**; **release binaries and a Homebrew formula.**
      0.1.0 is published as one crate. A release now publishes six in order
      (`legion-proto`, `legion-guest`, `legion-common`, `legion-worker`,
      `legion-manager`, `legion-rs`), and `legion-worker` embeds
      `bpf/legion_net.bpf.o` from outside its package, which `cargo publish`
      will refuse ([development.md](development.md#the-guest-binary-legion-guest),
      [plan § 10](plans/workspace-crate-split-plan.md#10-rollout-and-backward-compatibility)).

## Ecosystem apps (the omakase experience)

Each runs as a Legion app ([plan](plans/legion-apps-plan.md) · [calls](calls/legion-apps-plan_calls.md)); each app needs its own plan.

- [ ] **Ingress, Phase 3** ([plan](plans/ingress-app-plan.md) ·
      [calls](calls/ingress-app-plan_calls.md) · [docs](../docs/ingress.md)):
  - [ ] **Per-namespace host ownership** (`domains = [{ namespaces, hosts }]`):
        the first claim wins today, so any job author can claim an unclaimed
        name DNS points at the ingress ([calls 13](calls/ingress-app-plan_calls.md)).
  - [ ] **HTTP/2 and gRPC to services** (HTTP/1.1 only; [calls 12](calls/ingress-app-plan_calls.md)).
  - [ ] **Metrics and an NDJSON access log** per route.
  - [ ] **Wake on request through the ingress** (resume or scale from zero).
  - [ ] **TCP/TLS passthrough routes**, opt-in **HSTS**, and **IPv6 listeners**.
  - [ ] **No gap on upgrade**: a rolling update releases 80/443 before the new
        replica binds them; socket activation would close it ([calls 2](calls/ingress-app-plan_calls.md)).
  - [ ] **A connection limit** (`maxConnections`, `503` past it) and **a load
        benchmark** (rps and p99, plain and TLS) behind the sizing advice.
  - [ ] **Multi-replica first registration**: replicas poll the Issuer's
        thumbprint every 5 s, so an early challenge can get `503` and a 5-min
        backoff ([calls 7](calls/ingress-app-plan_calls.md)).
  - [ ] **The DNS check runs from the ingress's node**, exact names only
        ([calls 8](calls/ingress-app-plan_calls.md)).
  - [ ] **A per-node app's count misses nodes it could not place on**; surface the
        job's blocked placements in `legion app status`
        ([calls 3](calls/app-per-node-replicas-plan_calls.md)).
  - [ ] **Check bundled apps' keys offline**: `legion job validate` without a
        manager reports `ingress` as not checked.
- [ ] **Apps substrate follow-ups:**
  - [ ] Sandboxes have no API socket, so sandbox apps get no config and serve no views.
  - [ ] `job plan`/`validate` skip the grant check, `requires`, and the validate call.
  - [ ] Apps have no health of their own (no Apps page, no stopped-app finding).
  - [ ] Reconciler: one serial loop, leader-local backoff, renders depend on the
        leader's build; a keyed action can run twice across leadership (calls 113, 129).
  - [ ] CLI gaps: `app rollback`, `app set --key`, running actions, `app test`.
  - [ ] Pages only in the sidebar; UI smoke tests not in CI (calls 82, 84).
  - [ ] `WatchAppExtensions`/validate cost: per-stream polls, no caps (calls 90).
  - [ ] Free-form maps unchecked (`memroy = 512`; calls 60); no Nickel contracts for keys.
  - [ ] Smaller: KV-reading manifests need `kv:read`; union-of-rules grant coverage;
        held upgrades don't hold the binary; git errors classified by stderr; app
        jobs can't autoscale; non-atomic stopped-status clear.
  - [ ] Deferred: registry installs, tenant-owned apps, per-namespace grants.
- [ ] **App security follow-ups** ([calls 99-110](calls/legion-apps-plan_calls.md)):
      coarse, replayable 60 s delegations; JWT viewers not re-checked; views default
      to the app's grant; grants outlive granters; `identities` and autoscaler
      sources not code fields; validate runs before host/band checks; backtracking
      `pattern`s; mixed-version viewer calls fail.
- [ ] **App object kinds follow-ups** ([calls](calls/app-object-kinds-plan_calls.md)):
      history lost on delete; fail-open checks with the autoscaler down; no SPA
      object browser; CEL gaps (subschemas, cost estimate, type-checking, read
      cache); per-namespace quotas; `WatchObjects` resume; `--allow-pending`;
      `writeOnly` stored in the clear; Rust-only framework; unused `antlr4rust`.
- [ ] **Autoscaler follow-ups** ([plan](plans/autoscaler-v2-plan.md) · [calls](calls/autoscaler-v2-plan_calls.md)):
  - [ ] No app or plugin-call metrics; quota stage is a refusal; warmup is approximate.
  - [ ] Scale cost: policy watch re-resolves everything; `GetScalingInputs` reads per
        job; single-request status report; moved shards lose windows.
  - [ ] Plugins: one request per program; config changes don't flag broken policies;
        no secret references.
  - [ ] Simulation sees only Legion's history; `autoscale` edits restart rollouts.
  - [ ] Node groups ignore static ports in fits-elsewhere; scale-in clocks in memory;
        no `aws-asg` release artifact.
  - [ ] Deferred: wake a service from zero, `service.concurrency`, keyed scaling,
        consolidation, predictive/vertical/WASM strategies.
- [ ] **GitOps follow-ups** ([plan](plans/gitops-plan.md) · [docs](../docs/gitops.md)):
      SSH/deploy keys, push webhooks, replicas, non-`normal` bands, identity
      `permissions`, `vars`/template output, `gitops diff`, image automation.
- [ ] **Cert manager follow-ups** ([plan](plans/cert-manager-plan.md) · [docs](../docs/cert-manager.md)):
      job panel, more DNS-01 providers and a self-check, a port certificate key, RSA,
      ARI and revocation, `ca` Issuer rotation, name policy, replicas.

## Plugin surface

docs: [plugin-system.md](../docs/plugin-system.md)

- [ ] **New plugin kinds:** resource, command/API endpoint, fingerprinter, CNI,
      allocation-setup, task-group-setup, logging.
- [ ] **Eval hooks** (react to cluster events; apps are the job half).
- [ ] **Pluggable node statuses for degradation.**
- [ ] **A stable plugin ABI (audit M6)** ([calls #8](calls/architecture-audit-2026-10-09_calls.md)).
- [ ] **Panic containment for task runtime dylibs** ([review PLG1](architecture-review-2026-11-07.md)).
- [ ] **In-manager plugin hooks on the WASM layer** (`crates/worker/src/wasm/`); needs a plan.
- [ ] **Lift a plugin quarantine without a restart**; a `legion plugin status`.
- [ ] **Devices follow-ups** ([docs](../docs/devices.md)): sandbox VFIO; MIG/MPS/DRA;
      an `exec` device cgroup; preemption and node groups ignore devices; NVLink
      topology across nodes; usage reporting; YAML CDI; streaming health; edge starts;
      gVisor/Kata device wiring.
- [ ] **Job mutation policies** (review PLG4); needs a plan.
- [ ] **`job validate` can't check job rules for a caller with neither
      `namespace:read` nor `acl:read`**: it reads the rules on the client
      (`GetNamespace`, else `ListNamespacePolicies`) and only warns. A
      manager-side validate RPC would close it ([namespaces.md](../docs/namespaces.md#job-rules)).
- [ ] **A secrets provider seam** (review PLG4).

## Agent surfaces

docs: [agent-readiness.md](../docs/agent-readiness.md)

- [ ] **Agent ergonomics:** writing a plugin, joining a worker, searching the registry.
- [ ] **External audit sink** (syslog/webhook/S3).
- [ ] **Inline narrowing for child tokens**; **multi-party approvals**.
- [ ] **Cluster-wide rate limits and budgets** (per manager, in memory today).
- [ ] **MCP Streamable HTTP: SSE, sessions, authorization** ([calls #8](calls/agent-governance-plan_calls.md)).
- [ ] **An intent layer above jobs** ([review AI5](architecture-review-2026-11-07.md)).

## Embedding

- [ ] Legion can be called as a library, not just as a CLI.

## UI

docs: [ui-architecture.md](../docs/ui-architecture.md)

- [ ] **Jump hints** past the tenth row and on Overview/detail tables ([plan](plans/ui-jump-hints-plan.md)).
- [ ] **Suspend/resume UI:** the `artifacts` budget on the content card; create from a
      snapshot ([calls](calls/suspend-resume-ui-plan_calls.md) 8, 15).
- [ ] **Sessions UI:** `ListInstances` node/envelope/mode/task filters; an envelope flag
      on `LedgerAllocation` ([calls](calls/session-ui-plan_calls.md) 6, 7).
- [ ] **Polish** ([plan](plans/ui-holistic-polish-plan.md)): Restart worker button;
      per-card "stale since"; per-identity ⌘K recents; a toast primitive.
- [ ] **Task and template views** ([plan](plans/ui-task-and-template-redesign-plan.md)):
      server-side action fan-out; per-check health; per-allocation restart counter;
      action run history; `PlanJobTemplate` and a structured template outcome;
      template-only tokens can't tell create from update; template catalog views.
- [ ] **Operator gaps** ([plan](plans/operator-ui-gaps-plan.md)):
      `LEGION_WORKER_PUBLIC_ADDRESS=auto`; manager reverse proxy
      ([plan](plans/manager-reverse-proxy-plan.md)); job-scoped `GetAllocation`; an
      indexed `scheduler_event.instance_id`.

## Cross-cutting

- [ ] **Spot-check papercuts** ([findings](launch-spot-check-findings.md#re-run-2026-10-12)):
  - [ ] **Stopping the macOS manager leaves its Apple containers running** — closed by
        [#39](https://github.com/mikenomitch/legion/pull/39).
  - [ ] **Orphaned Apple containers are never reaped.** A container labeled
        `legion.node=<this node>` whose allocation no longer exists (a crash, or
        a stop from before #39) keeps its published ports, so a new allocation
        of the job cannot bind them. Sweep this node's labeled containers at
        worker start; `container system stop`/`start` clears a hung one.
  - [ ] **A count=1 job got repeated second placements after a KV env-change
        restart.** Not root-caused; check `job_config_unwanted_reason`.
  - [ ] **Pressure evictions still loop forever by default** (unlimited `move_count`,
        no move backoff, can re-land on the evicting node; a short
        anti-affinity would stop that,
        [calls 58](calls/suspend-resume-and-oversubscription-plan_calls.md)).
  - [ ] **No `scheduler_bench` group covers preemption**, so the fleet-wide
        disruption-floor count (2026-11-08) has no before/after numbers.
- [ ] **A Nickel contract library for the jobspec** ([plan](plans/nickel-config-language-plan.md)).
- [ ] **Audit every name decision in depth.**
- [ ] **A batch job-submission RPC (`RunJobs`)**; needs a plan (idempotency, partial
      failure, ACL scope).
- [ ] **Flaky tests:** `acked_write_survives_any_single_node_loss` on small hosts;
      `worker::forwarder`, `wait_for_next_eval…`, and the guest-agent vsock relay test.
      Under load on a 4-core box (2026-11-08) these also failed once and passed
      alone: `raft_durability::leader_loss_mid_burst`,
      `raft_write_path::every_log_purge_fsyncs_the_state_db_first` and
      `proxy_hand_offs_cross_nodes_only_over_the_mesh_test`.
- [ ] **Crate split follow-ups** ([plan § 8](plans/workspace-crate-split-plan.md#8-implementation-phases)):
  - [ ] **The store out of `legion-manager`.** It is the largest compile
        (3.8 GB, 4.5 GB with tests). `storage` has 28 references into
        `manager` that are design, not placement: `Store` owns the scaling
        rollups, the status board and the cluster CA, and calls
        `Scheduler::new` inside a write. Needs its own plan and
        `scheduler_bench` numbers.
  - [ ] **A plugin API crate**, so a plugin stops compiling all five crates
        for a trait and a few types. The ABI fingerprint moves with it.
  - [ ] **A datapath crate** (`worker/netd`, 25.7k lines), if the worker's
        2.2 GB becomes the constraint.
- [ ] **Shrink the linux/amd64 binary further.** It is 89 MB, `.text` alone
      72 MB; the 26 MB the docs used to quote was a macOS arm64 build of a
      smaller tree. Most of it is Legion's own code and the generics
      instantiated for its types, not a dependency
      ([dev notes](development.md#finding-what-grew),
      [calls](calls/binary-size_calls.md)). What is left:
  - [ ] **The crate split added 5.8 MB** (91.5 → 97.3 MB,
        [plan § 5.4](plans/workspace-crate-split-plan.md#54-measured-before-and-after),
        [calls 3](calls/workspace-crate-split-plan_calls.md)). Likely generics
        instantiated once per crate (serde for `JobConfig` and friends,
        parsed in four crates). Confirm with a symbol-level diff against a
        `main` build, then put non-generic wrappers at the crate boundary for
        the heavy ones.
  - [ ] **`panic = "abort"`** is worth about 9 MB of unwind tables, but ~20
        `catch_unwind` sites contain plugin and rule panics (calls 6).
  - [ ] **Types decoded both from text and from `serde_json::Value` carry two
        decoders** (about 1 MB); routing both through one needs call-site
        changes.
  - [ ] **Each RPC costs 40-100 KB** of generated tonic, prost, and serde
        code; nothing bounds it.
  - [ ] **Nickel is about 8 MB by symbol owner**, the only dependency over
        5 MB besides `serde_json`.
  - [ ] **Re-measure macOS arm64**; the last figure (26.0 MB) is from
        2026-07-28.
- [ ] **Decide whether the UI ships inside the binary.**
- [ ] **Test hygiene:** `crates/manager/.legion/bootstrap-token` is checked in and the
      manager's unit tests rewrite it (untrack it and write it under the scratch root); move `legion-loadgen` to its own crate
      ([development.md](development.md#keeping-builds-and-test-runs-small)); fold the
      raft-ops tests into `tests/integration/` ([calls 1](calls/test-build-footprint_calls.md)).

## Long term

- [ ] A **speed** scheduler; needs a plan first.
- [ ] A **VM** runtime beyond the microVM sandbox.
- [ ] **Windows support pass**, including artifact staging ([plan](plans/artifact-system-plan.md)).

## Verify

Runs that need real hardware, a KVM or macOS host, root on cgroup v2, or the AWS rig. The code for each is in; these confirm it.

### Launch blockers

- [ ] Run a real gated container end to end on a Linux node (runc and crun, default mode): stock `nginx` reachable on its advertised port, `curl` through the proxy refused with `gated-default`, then allowed after `network.egress.allow` ([worker-plane-auth plan](plans/worker-plane-auth-plan.md) §6.12).
- [ ] Run the kernel host guard on a cgroup v2 node as root: `sudo -E cargo test --lib kernel_guard_loads -- --ignored --nocapture` (a dial to `169.254.169.254` fails with `EPERM`, counter reports it) ([plan §6.13](plans/worker-plane-auth-plan.md)).
- [ ] Run the container privilege floor on crun: `LEGION_OCI_RUNTIME=crun cargo test --lib oci_runtime_security -- --ignored` ([calls 40](calls/worker-plane-auth-plan_calls.md)).
- [ ] Re-run the three ignored runc tests (`oci_runtime_exec_runs_inside_the_container`, `…_sees_task_dir_files_and_their_rewrites`, `…_enforces_a_declared_memory_limit`) on a cgroup v2 host to tell environment from bug.
- [ ] Re-run the AWS spot check without shims (`tests/spot-check/smoke.sh` + RUNBOOK human pass), including container/exec paths on the `0710` allocation-dir grant and edge-boot speculative dispatch ([launch-spot-check-fixes plan](plans/launch-spot-check-fixes-plan.md)).
- [ ] Rerun `05-dag.ncl` on the 3-manager rig: a DAG advances past its first step ([launch-spot-check-findings.md](launch-spot-check-findings.md)).
- [ ] Rerun `05-dag.ncl` twice with different `run` values: the resubmit's rollout progresses ([rollouts.md](../docs/rollouts.md#lifecycle-specific-behavior)).
- [ ] Rerun the `durability = "store"` suspend and cross-node resume on the rig (contentStore auth on part fetch, node-independent `vmdev` links, gated-restore wait) ([suspend-and-resume.md](../docs/suspend-and-resume.md)).
- [ ] Reproduce P1-5 on the rig: re-run `install.sh` over a worker with running tasks and confirm no orphans or second copies, now that a worker under a new name stops an unlocked identity's tasks ([plan §8.4](plans/launch-spot-check-fixes-plan.md) · [round-2 calls #2](calls/launch-blockers-round-2-plan_calls.md)).
- [ ] On the rig: pull a private image and an S3 artifact through the guarded proxy (blob CDN and token redirects work; a redirect to `169.254.169.254` is refused), and with `HTTPS_PROXY` set ([round-2 plan](plans/launch-blockers-round-2-plan.md)).
- [ ] Run the sandbox spot check (RUNBOOK §12, `tests/spot-check/smoke.sh sandboxes`; needs `legion-guest` and the §12.0 config), never run so far.
- [ ] Verify the sandbox clean stop on the KVM rig, cloud-hypervisor and Firecracker: `09-sandboxes.ncl` task `ws`, write `/workspace/probe`, `instance stop` + `start`, plus the `sandbox-suspend` boots count ([calls](calls/sandbox-clean-stop_calls.md)).
- [ ] Confirm on the rig whether the disk-fit fix explains sc-wkr-1's 2G refusal ([calls 74](calls/launch-spot-check-fixes-plan_calls.md)).
- [ ] Confirm cgroup v2 resource enforcement turns on (P1-1), and measure worker-plane TLS dispatch cost at 1,000 nodes, on real hardware.

### Scheduling & placement

- [ ] Re-measure job intake with batching on, on 3 voters (5k nodes, `dense_fleet` at 200k allocations, 16 eval threads, 32 submitters): `raft_ops_total{op="enqueue_evals_isolated"}` and `{op="seed_task_statuses_batch"}` well below the submit rate ([eval-path-raft-cost plan](plans/eval-path-raft-cost-plan.md)).
- [ ] Re-measure eval-loop claim/complete batching: rerun cells G/H from [aws-phase4-findings.md](aws-phase4-findings.md); `complete_evals_and_claim_next` rate ÷ `evals_processed_total` should exceed 1.
- [ ] Re-measure the plan-apply conflict fix on AWS: the five-row table in [aws-phase2-findings.md](aws-phase2-findings.md), with `spread` as well as `bin_pack`; check `build_plan` stays flat above 5k and why `task_fetch` grew 33x ([plan-apply-conflict-reduction plan](plans/plan-apply-conflict-reduction-plan.md)).
- [ ] Re-run the `dense_fleet` 50k/100k/200k fill ladder and the eval-thread sweep above 32 with `--submitter native`; trust only cells with `driver_bound: false` and `cross_check.agree` ([aws-phase4-findings.md](aws-phase4-findings.md)).
- [ ] Re-measure a paced vs burst submission pair (600 nodes, 12,006 allocations) after the SCH3 claim fix, reading `legion_plan_apply_total{outcome}` ([placement-and-commit-cost plan](plans/placement-and-commit-cost-plan.md)).
- [ ] Run adaptive concurrency's Phase 0 matrix: fixed `evalThreads ∈ {1..32}` at 1k and 5k nodes on a conflict-bound workload, into `benchmarks/scheduler/README.md` ([plan §11, §16](plans/adaptive-scheduler-concurrency-plan.md)).
- [ ] Measure group commit on three voters and revisit the 64-op / 128 KiB batch bounds ([calls #4](calls/raft-write-path-plan_calls.md)).
- [ ] Run edge boot's Phase 0 start-path matrix (submit → first byte, both persistence modes, cold/warm image, single/cross-region) on a real raft cluster and registry ([calls #18](calls/edge-boot-plan_calls.md)).
- [ ] Re-measure the AWS campaign with heartbeats off raft: `heartbeat-commit-probe.sh` on gp3 at 250/1000/2500 nodes, `packing-density.sh`, all twelve `run-campaign.sh` cells, `storm.sh`, `soak-24h.sh`, `failover-drill.sh` with zero nodes lost, and the cadence phase-4 repro ([liveness plan §10.1](plans/liveness-off-the-raft-path-plan.md)).
- [ ] Run the `heartbeat = { adaptive = false }` comparison at 10k and 20k nodes against the adaptive default ([cadence plan §13](plans/heartbeat-cadence-at-scale-plan.md)).
- [ ] Re-run the ~3,000-node mass-loss storm: submissions keep landing while the reschedule backlog sits at priority 40; also confirm a job matching no node moves `evals_processed_total{kind="job_submit",status="error"}` by one ([node-loss-storm plan](plans/node-loss-storm-plan.md)).
- [ ] Run the rest of the cross-system matrix: CLI control row (row 5) on the NVMe rig, 10k storm on all three, memory per cell (row 6), and `REPORT.md` ([plan](plans/aws-comparative-benchmark-plan.md)).
- [ ] Run the Legion B (streaming) campaign and the 20k validation burst ([plan](plans/aws-comparative-benchmark-plan.md)).

### Task runtimes

- [ ] Run the new sandbox paths on KVM, both VMMs: the `#[ignore]`d tests for a session run with files in and `--get` out, balloon resize down and back up and `needs_restart` above boot size, `suspendWithOwner`, a `store` session's `--move`, and `runtime disable --drain` suspending a keyed sandbox ([session-runs](plans/session-runs-plan.md) · [resize](plans/resize-plan.md) · [child-sessions](plans/child-sessions-plan.md)).
- [ ] Run the sandbox-boot cases on Firecracker and with TAP guests (~1 h, ~$1) to decide whether `auto` should prefer Firecracker ([docs](../docs/task-runtime-sandbox.md#choosing-a-vmm), [calls 15](calls/sandbox-boot-throughput-plan_calls.md)).
- [ ] Measure root-disk-bound boot throughput on a node type with local NVMe ([results](../benchmarks/aws/sandbox-boot/RESULTS.md)).
- [ ] Run the lazy-memory restore matrix (1/4/8 GiB, lazy vs eager, cloud-hypervisor v51–v53 and Firecracker, first read after resume, no-userfaultfd fallback, and once with `LEGION_SANDBOX_GATED_DEFAULT=1`) on a ≥32 GiB c5.metal; record numbers in the sandbox doc's "Restore time", RESULTS.md, and [plan §9](plans/lazy-memory-restore-plan.md#9-testing-plan).
- [ ] Rootfs base cache: boot a cloned disk on Firecracker, and checkpoint and restore a cloned disk ([plan](plans/rootfs-base-cache-plan.md)).
- [ ] Run the sandbox campaign on the AWS rig (`run-sandbox-campaign.sh`) and record ceiling, raft/CPU cost per lifecycle, and memory per sandbox in plan §14 ([calls](calls/sandbox-scaling-plan_calls.md) #20, #60).
- [ ] Run the rig's keyed `ladder` cells with the `perf/multi-eval-plans` build vs main; merge only on a measured gain ([plan](plans/multi-eval-plans-plan.md) §12).
- [ ] Run elastic envelopes on real workers, through a follower's forwarded write, and in a fleet-wide grow burst ([plan](plans/elastic-session-envelopes-plan.md) Phase 3).
- [ ] Resume a real microVM behind the wake gateway on KVM ([plan](plans/wake-on-request-plan.md)).
- [ ] Park and resume a real sandbox on KVM, gated and TAP, with memory and disk checkpoints ([plan](plans/park-plan.md)).
- [ ] Sandbox exec Phase 0 at 1,000 and 10,000 concurrent sessions on the AWS rig with real workers (≤20/≤80 ms open latency); add a connection-reuse mode ([plan](plans/sandbox-exec-plan.md)).
- [ ] Sandbox exec Phase 1 on KVM: `sandbox_exec_over_vsock`, then exec after a memory resume, a session across suspend, `none` sandbox exec, a shell keeping a sandbox awake, and a UI-terminal action ([plan](plans/sandbox-exec-plan.md)).
- [ ] Delegated exec keys: a positive direct session into a `container` task on runc ([calls](calls/sandbox-exec-plan_calls.md)).
- [ ] Gated Phase 0: boot a gated sandbox on a real VMM (`sandbox_gated_boots`, G0.1) and the G0.2 throughput numbers ([verification](suspend-resume-linux-verification.md#gated-sandboxes)).
- [ ] Gated KVM checks G1.1–G1.5, G2.1, G3.1, G4.1, G5.1, G5.2, recorded in the verification doc ([plan](plans/gated-sandbox-checkpoints-plan.md)).
- [ ] Run the supervision plan's root-only cgroup and hardening paths against a Linux worker ([plan](plans/host-process-supervision-plan.md)).
- [ ] Suspend Phase 1 Linux checks on cgroup v2 ([§ Phase 1](suspend-resume-linux-verification.md#phase-1)).
- [ ] Suspend Phase 2 Linux checks: `memory.low`/`memory.max`, oversubscribe ratio, two-bursting eviction, `memory.reclaim`, guest size ([§ Phase 2](suspend-resume-linux-verification.md#phase-2)).
- [ ] Suspend Phase 3b KVM checks (`sandbox_memory_checkpoint_round_trip_*`), recording restore latency per VMM ([§ Phase 3b](suspend-resume-linux-verification.md#phase-3b)).
- [ ] Suspend Phase 3c Linux checks: memory snapshot/restore acceptance on both VMMs, pins, paused volume snapshot, TAP recreation, restored VMM cgroup ([§ Phase 3c](suspend-resume-linux-verification.md#phase-3c)).
- [ ] Suspend Phase 4 Linux checks: suspend <1.5 s / resume <300 ms for 1 GiB on both VMMs, identity refresh, RNG divergence, `container` disk suspend; publish latencies ([§ Phase 4](suspend-resume-linux-verification.md#phase-4)).
- [ ] Suspend Phase 5 Linux checks: idle suspend, busy never suspends, pressure order, quiet connection, local suspension across lost heartbeat, balloon on both VMMs ([§ Phase 5](suspend-resume-linux-verification.md#phase-5)).
- [ ] Confirm the pinned Firecracker accepts `free_page_reporting` (item 5.5), then turn it on.
- [ ] Suspend Phase 6 Linux checks: three memory forks diverge, TAP on both VMMs, Firecracker page sharing, `container` disk fork, `sandbox_fork_divergence_*` ([§ Phase 6](suspend-resume-linux-verification.md#phase-6)).
- [ ] Integration items 6.7 and 6.8: a spread fork of a TAP sandbox across two bridged KVM nodes, and the named TAP failures ([§ Phase 6](suspend-resume-linux-verification.md#phase-6)).
- [ ] Suspend Phase 7b Linux checks: real MinIO publish of a memory checkpoint and resume on a second KVM node, with plaintext and sealed publish/fetch times ([§ Phase 7b](suspend-resume-linux-verification.md#phase-7b)).
- [ ] Measure chunked publish/fetch against real MinIO and S3, and memory dirtiness on real Firecracker and cloud-hypervisor guests ([calls 4](calls/chunked-checkpoint-publish-plan_calls.md)).
- [ ] Track 2 Linux checks: a make-room round with real sandbox captures and a `snapshots` budget filled by real checkpoints ([§ Track 2](suspend-resume-linux-verification.md#track-2)).
- [ ] Track 1 KVM checks: per-instance TAPs on a bridge, TAP counters, in-place identity disk refresh, TAP sandbox forks ([§ Track 1](suspend-resume-linux-verification.md#track-1)).
- [ ] Rerun `scheduler_speed`, `candidate_construction`, `snapshot_budget`, and the `locality` group before/after on a quiet host; the suspend plan's §9 numbers and the content-store budget numbers are noise-dominated ([plan §9](plans/suspend-resume-and-oversubscription-plan.md#9-testing-plan)).
- [ ] Isolate Phase 0 on the AWS rig (nested and metal, 16 vCPU / 64 GiB, a `la57` CPU for 5-level paging): the findings doc's measurements, then rewrite plan §4 and §10 from them; every number so far is from a 4-vCPU container ([findings](isolate-runtime-phase0-findings.md) · [plan §11](plans/isolate-runtime-plan.md#11-implementation-phases)).
- [ ] Isolate density campaign (plan §12, Phases 4 and 6): fill a node with session isolates to its memory budget; report resident, paged-out and hibernated counts, wake latencies, and host RSS per isolate; include an MPK host with `vm.max_map_count` raised.
- [ ] Isolate VM tier on KVM: a `tier = "vm"` run and an `auto` run that goes to the VM (reply shape, `vmMs`, a fresh VM per run, `args` on `run.command`), `--fallback vm`, and child VMs (`vm.acquire`/`exec`/`fetch`, `children.max`, a session isolate keeping its child across hibernation and re-attaching, the child ending with its owner, `child-gone`); nothing has run against a real VM. The tier logic and child ownership pass end to end with an `exec` session task standing in for the sandbox, and the two sandbox variants in `tests/integration/isolate.rs` are written and ignored ("needs /dev/kvm"); run them on a KVM node ([calls 66](calls/isolate-runtime-plan_calls.md#66-vm-tier-and-child-vm-tests-run-with-an-exec-session-task-standing-in-for-the-sandbox) · [plan §12 "Burst"](plans/isolate-runtime-plan.md#12-testing-plan)).
- [ ] Isolate child re-attach by id after hibernation: a session isolate that hibernated calls `vm.attach(id)` on the child it started earlier and drives it. Not covered by any test (the test guest only calls `vm.acquire`); needs a runner fixture that calls `vm.attach`, then a run with the `exec` stand-in and on KVM ([calls 66](calls/isolate-runtime-plan_calls.md#66-vm-tier-and-child-vm-tests-run-with-an-exec-session-task-standing-in-for-the-sandbox)).
- [ ] Isolate HTTP end to end on a real node: a request through netd's own TCP ingress listener (TLS and the ingress chain) to an isolate handler, and an upstream credential installed by the heartbeat's `FetchDatapathMaterial` path rather than set directly; the end-to-end test enters at the supervisor's ingress socket and installs the credential itself ([plan §12](plans/isolate-runtime-plan.md#12-testing-plan)).
- [ ] Isolate page-out and the host cgroup on a cgroup v2 node with swap or zswap and resource enforcement on: `Reclaim{PAGE_OUT}` plus `memory.reclaim` actually moves pages, `pagedOut` counts them, the pressure response asks hosts before allocations, the forked host lands in its cgroup (placed before `exec`), `memory.max` follows envelope resizes, leaf names stay distinct for colliding cordon names, and each compile runs in its own `compile.<pid>.<n>` leaf ([calls 31](calls/isolate-runtime-plan_calls.md#31-paging-out-is-kernel-reclaim-on-the-hosts-cgroup-not-madvise-per-isolate)).
- [ ] Isolate Phase 1 acceptance on a real node with **release builds**: `legion session run` on a Rust component task under 2 ms end to end. Measured so far only with debug builds of the worker and the host in a 4-vCPU container: 4.8 ms p50 (min 4.4 ms) for a Rust component, 17.6 ms p50 for a Python hello world (`isolate_images::hello_world_end_to_end_timing`), which does not meet the target; also `isolate_bench` (`cargo bench -p legion-isolate --bench isolate_bench`) against the Phase 0 targets on the rig ([plan §15](plans/isolate-runtime-plan.md#15-acceptance-criteria)).
- [ ] Build the isolate images from a clean machine with the pinned toolchains (`pip install --require-hashes`, `npm ci` into `target/isolate-images-tools`); every build so far reused pre-installed toolchains ([images README](../crates/isolate/images/README.md)).

### Networking, persistence, apps and the rest

- [ ] Measure the manager-routed session start path across three managers with loadgen session mode, on the AWS rig (`benchmarks/aws/README.md`, [plan §8](plans/session-durability-and-routing-plan.md)).
- [ ] Run Phase 5b `store` sessions against real S3 or MinIO (`crates/worker/src/session_manifest.rs` is tested only on `object_store`'s in-memory store).
- [ ] Run Phase 5b with the `sandbox` runtime on KVM: publish and restore a session's sealed memory parts on another node ([plan](plans/session-durability-and-routing-plan.md)).
- [ ] Measure splice pipe-page limits on a node with tens of thousands of concurrent bulk flows ([calls #6](calls/datapath-performance-plan_calls.md)).
- [ ] macOS gated Phase 4: on a macOS 26 machine with a root worker, run the plan's three scenarios and settle its open questions ([plan §3](plans/macos-gated-plan.md)).
- [ ] Run the system worker's per-worker host-guard naming as root ([calls 95](calls/legion-apps-plan_calls.md)).
- [ ] Verify the system worker's `cpu.max` ceiling with a busy loop on a cgroup v2 host (plan §9 A1).
- [ ] Run the `#[ignore]`d API-socket peer-cgroup refusal test as root on delegated cgroup v2 (`tests/integration/api_socket.rs`; [calls 31](calls/legion-apps-plan_calls.md)).
- [ ] Run the cert manager against Let's Encrypt staging with Cloudflare DNS-01 and the HTTP-01 responder on a public node ([docs](../docs/cert-manager.md)).
- [ ] Run the 10,000-policy autoscaler load test on the AWS rig with the app on a system worker and real workers (calls 101); also take release-profile numbers for the scaling-input rollup benchmark.
- [ ] Run a GPU job end to end (exec and container, with/without `--cdi`, runc and crun) on a real NVIDIA node ([devices.md](../docs/devices.md#what-has-been-verified)).
- [ ] Run Legion's bundles under gVisor (`runsc`) and Kata ([task-runtime-container.md](../docs/task-runtime-container.md#runtime-classes)).
- [ ] Check jump-hint chords survive Safari and Firefox ([calls 1](calls/ui-jump-hints-plan_calls.md)).
- [ ] Linux visual check of `/nodes/<id>` against `legion node usage <id>` with enforcement on, and the idle countdown reaching zero within one poll of the sweep ([plan](plans/suspend-resume-ui-plan.md)).
- [ ] Browser check of the sessions UI against a live session cluster: Promote, End, paging, envelope and edge-nodes cards ([plan](plans/session-ui-plan.md)).
- [ ] Re-verify on the rig: microVM clean stop (volume writes survive `instance stop`), image pulls after leader failover, two local managers with separate `~/.legion/tokens/` files, and `rig_install_cli_token` on a 3-manager rig ([findings](launch-spot-check-findings.md#re-run-2026-10-12)).
- [ ] Bake the operator AMI against real AWS: console output survives the stop, bake finishes inside 90 minutes ([calls #29](calls/aws-comparative-benchmark-plan_calls.md)).


---

# Done

## Configuration language

- **Nickel replaces Jsonnet** — every config surface is `.ncl` through `crates/common/src/config_lang.rs`; `vars` + `--var`/`--var-code`/`--kv-var`/`--kv-var-code`; no `.jsonnet` compatibility ([plan](plans/nickel-config-language-plan.md) · [calls](calls/nickel-config-language-plan_calls.md) · [docs](../docs/configuration-language.md))
- **`.json` on four of five config surfaces** — extension-dispatched, for machine-emitted config; templates stay Nickel-only ([calls](calls/nickel-config-language-plan_calls.md) · [docs](../docs/configuration-language.md#json-for-machine-emitted-config))
- **Local jobspecs evaluated in place** — relative imports work from `job run/plan/validate`
- **Nickel evaluation is bounded** — a 1,000,000-step budget enforced through Nickel's cache, on a dedicated large-stack thread; a self-referencing field is an error, not a hang ([calls](calls/nickel-eval-budget_calls.md) · [docs](../docs/configuration-language.md#evaluation-limits))

## Worker nodes

- **Node liveness off the raft path** — heartbeats hit an in-memory `LivenessTable`, only transitions replicate; failover grace from liveness responsibility; mass-disruption breaker; peer liveness digest; display checkpoint ([plan](plans/liveness-off-the-raft-path-plan.md) · [calls](calls/liveness-off-the-raft-path-plan_calls.md) · [docs](../docs/node-liveness.md))
- **Heartbeat cadence at scale** — per-node lost windows, `heartbeatSeconds` up to 300 with a self-fence startup check, freshness metrics, `legion node cadence`, adaptive cadence on by default (`targetRatePerSecond` 2,500) ([plan](plans/heartbeat-cadence-at-scale-plan.md) · [calls](calls/heartbeat-cadence-at-scale-plan_calls.md) · [docs](../docs/heartbeat-cadence-and-scale.md))
- **A returning node is schedulable again** — recovery undoes both lost/unavailable halves, never un-cordons a drained node ([docs](../docs/node-liveness.md))
- **Worker basics** — add a worker, RPC contact and messaging, heartbeats with unhealthy-on-miss, fingerprinting, dynamic attributes, the manager's node ledger, restart from the server

## Scheduling

docs: [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md), [jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md)

- **Placement quality and commit cost** — bounded `plan_submission`, O(batch) claims, `bin_pack`/`spread` scored against capacity with count-scaled anti-affinity, in-memory plan apply, debug-level plan logs, topology-aware gang placement (review SCH2/3/4/5a/8, AI7a) ([plan](plans/placement-and-commit-cost-plan.md) · [calls](calls/placement-and-commit-cost-plan_calls.md) · [docs](../docs/scheduler-and-eval-loop.md))
- **Gang scheduling, Phase 1** — `gang = true | { min }`, all-or-nothing quorum placement, preemption floor, one-step rollout, `gang_reports` ([plan](plans/gang-scheduling-plan.md) · [calls](calls/gang-scheduling-plan_calls.md) · [docs](../docs/gang-scheduling.md))
- **Gang scheduling, Revision 2** — `gang.placementTimeout`, `gang.onMemberLoss = restart|replace|fail`, `LEGION_GANG_RANK/SIZE/MIN/LEADER_ADDR` ([plan](plans/gang-scheduling-plan.md#revision-2-placement-timeout-and-member-loss-2026-10-17) · [docs](../docs/gang-scheduling.md#placement-timeout))
- **Elastic session envelopes, Phase 1** — `stateful.envelope.max`/`shrinkAfter` grow and shrink a node's envelope ([plan](plans/elastic-session-envelopes-plan.md) · [calls](calls/elastic-session-envelopes-plan_calls.md) · [docs](../docs/session-mode.md#elastic-envelopes))
- **Submit/update/scale-out evals settle with their plans** — fused settle saves one entry each ([plan](plans/multi-eval-plans-plan.md) · [calls](calls/multi-eval-plans-plan_calls.md) · [docs](../docs/scheduler-and-eval-loop.md))
- **Index truncation samples a tied cell uniformly** — fixes the planner seeing only the lowest-id slice at rig scale ([findings](aws-phase5-findings.md#open-in-priority-order) · [calls](calls/scheduling-index-plan_calls.md) · [docs](../docs/scheduling-indexes.md))
- **Read-model refresh applies the changeset delta** — `cache_apply_allocation_delta` with fallback and round-robin scrub ([findings](aws-phase4-findings.md) · [calls](calls/eval-path-raft-cost-plan_calls.md))
- **Plan-apply conflict reduction** — two-pass bind (epoch as ranking hint), candidate lists restored, rejected plans no longer partially commit ([plan](plans/plan-apply-conflict-reduction-plan.md) · [calls](calls/plan-apply-conflict-reduction-plan_calls.md))
- **Priorities and preemption** — six priority bands, plan-atomic preemption, stop causes, graceful `stop { signal, killAfter }`, `bands` ACL selector, pool toggle, UI history; fair-share stays a non-goal ([plan](plans/preemption-and-eviction-plan.md) · [calls](calls/preemption-and-eviction-plan_calls.md) · [docs](../docs/preemption-and-eviction.md) · [graceful stop](../docs/graceful-stop.md))
- **Plugin ABI version symbol** — `declare_task_plugin_abi!()` makes stale dylibs fail at load ([docs](../docs/task-runtime-plugins.md))
- **Declarable scheduling indexes** — `partition_by` × `dims` indexes, nearest-fit candidates, shadow mode, default `fit` index ([plan](plans/scheduling-index-plan.md) · [docs](../docs/scheduling-indexes.md))
- **Late-binding plan apply (phases 0–3)** — ranked candidates per placement, conflict-exhausted evals requeue with backoff, `plan_candidates` ([plan](plans/late-binding-plan-apply-plan.md) · [docs](../docs/late-binding-plan-apply.md))
- **Adaptive scheduler concurrency (phases 1–3)** — AIMD planner gate with goodput guard, `legion scheduler concurrency`; default `observe` ([plan](plans/adaptive-scheduler-concurrency-plan.md) · [calls](calls/adaptive-scheduler-concurrency-plan_calls.md) · [docs](../docs/adaptive-scheduler-concurrency.md))
- **Builtin names in a worker type's `resources` fail startup** — no longer silently ignored ([calls](calls/node-resource-overrides_calls.md) · [docs](../docs/resource-enforcement.md))
- **`run_dispatch_future` requires a `Send` future** — a guard held across `.await` on the dispatch path no longer compiles
- **Scheduler basics** — ledger command, scheduler plugin framework, random/bin-pack/spread schedulers, constraints, affinities, task groups, rescheduling, health-check reschedule, attribute-change re-evaluation
- **Per-eval planning cost flat in fleet size** — persistent read-model maps, budgeted index walk, shared node set, O(jobs) scan indexes, eval phase metrics; confirmed on AWS to 50k nodes ([plan](plans/plan-path-fleet-scaling-plan.md) · [calls #15](calls/plan-path-fleet-scaling-plan_calls.md) · [docs](../docs/benchmarks.md))
- **`RunJob` no longer parks tokio workers** — submit under `block_in_place` (`run_blocking_admission`), PR #40 ([docs](../docs/benchmarks.md))
- **Read-connection pool fixed and sized `evalThreads + 4` (cap 16)** — steal-on-contention, `legion_store_read_lock_wait_seconds` ([calls 5](calls/eval-path-raft-cost-plan_calls.md))
- **Fewer raft rounds per eval** — fused complete-and-claim, one-round fan-out, batch claim/complete, intake group commit, eval loop off tokio threads, condvar coalescer handoff ([plan](plans/eval-path-raft-cost-plan.md) · [calls](calls/eval-path-raft-cost-plan_calls.md) · [docs](../docs/scheduler-and-eval-loop.md#batching))
- **Eval-notifier thundering herd fixed** — one wake per claimable eval, `legion_eval_claim_total{outcome}`
- **Plan-apply commit no longer re-reads a node's allocations** — `node_alloc_usage` cache; reprojection via a per-node `NodeUsage` memo ([docs](../docs/scheduler-and-eval-loop.md#the-per-node-usage-cache))
- **Planning cost no longer grows with cluster fill** — per-job allocation index, delta publish, cached permits and statements ([plan](plans/scheduler-performance-plan.md) · [calls](calls/scheduler-performance-plan_calls.md) · [docs](../docs/scheduler-and-eval-loop.md#node-candidate-construction))
- **Scheduler hot-path passes (2026-07-29 → 08-03)** — index-served candidates, cheap snapshots, fused plan submission, one transaction per plan batch ([plan](plans/plan-path-scale-levers-plan.md) · [calls](calls/plan-path-scale-levers-plan_calls.md))
- **Scheduler performance measurement stack** — scenario schema v2, backend matrix, `PlanTimings` ([plan](plans/scheduler-performance-plan.md) · [docs](../docs/testing-and-benchmarks.md))
- **Architecture review scale proposals D1/D4/D5/D6** ([plan](plans/scheduler-architecture-review-plan.md) · [calls](calls/bounded-candidate-scoring-plan_calls.md) · [docs](../docs/scheduler-and-eval-loop.md))
- **Heartbeat path at scale** — status-lane group commit, integer liveness clock, jitter, `heartbeatSeconds`; heartbeat store call off the runtime (~7x) ([plan](plans/heartbeat-scale-plan.md) · [calls](calls/heartbeat-scale-plan_calls.md) · [docs](../docs/architecture-managers-workers.md))
- **Node-loss storm affordable** — lower reschedule priority, enqueue-time empty-node filter, batched fan-out, `legion_nodes_recovered_total` ([plan](plans/node-loss-storm-plan.md) · [calls](calls/node-loss-storm-plan_calls.md) · [docs](../docs/node-liveness.md))
- **`mark_node_ready` writes `active`** ([call 2](calls/liveness-off-the-raft-path-plan_calls.md))
- **Plan fencing closed** — epoch mirror coherent with the allocation cache; apply re-validates availability and resource totals ([calls #4](calls/durability-and-ops-execution-plan_calls.md))
- **Raft write path: group commit and single execution under `single`** ([plan](plans/raft-write-path-plan.md) · [calls #1](calls/raft-write-path-plan_calls.md) · [docs](../docs/multi-raft.md#the-write-path-group-commit-and-single-execution))
- **One fsync per entry** — `synchronous = NORMAL` with a purge barrier ([plan](plans/raft-write-path-plan.md) · [calls #2](calls/raft-write-path-plan_calls.md) · [docs](../docs/multi-raft.md#snapshots-compaction-and-catch-up))
- **`legion start worker --simulate N`** ([plan](plans/worker-simulation-plan.md) · [docs](../docs/worker-simulation.md))
- **Benchmark rig** — Terraform, per-system configs, driver, campaign runner, experiments, unattended runs, durable results, operator-box bootstrap ([plan](plans/aws-comparative-benchmark-plan.md) · [calls](calls/aws-comparative-benchmark-plan_calls.md) · [docs](../benchmarks/aws/README.md))
- **`legion-loadgen` native submitter** ([calls §23-24](calls/aws-comparative-benchmark-plan_calls.md))
- **Nomad and K8s paths rebuilt with native submitters; comparison rerun on shared NVMe** — PR #40 ([calls](calls/aws-comparative-benchmark-plan_calls.md) · [docs](../docs/benchmarks.md))

## Task runtimes

- **Runtime disable, Phases 1–2** — `legion operator runtime disable|enable|status`: a replicated switch that refuses every start of a runtime cluster-wide or per namespace glob, blocks and wakes evals, and `--drain` suspends or stops what is running ([plan](plans/runtime-disable-plan.md) · [calls](calls/runtime-disable-plan_calls.md) · [docs](../docs/runtime-disable.md))
- **Session runs, Phases 1–2** — `legion session run` / `RunSession` on `exec`, `container` and `sandbox` session tasks: one call, a single-use session, output and result back ([plan](plans/session-runs-plan.md) · [calls](calls/session-runs-plan_calls.md) · [docs](../docs/session-runs.md)); prepared-VM starts wait on sandbox-scaling Phase 8
- **Child sessions, Phases 1–4** — a unit owns sessions or keyed instances of a sibling task; cascade, promotion, identity claims on keyed children, connection handoff, starts from inside a task with no token ([plan](plans/child-sessions-plan.md) · [calls](calls/child-sessions-plan_calls.md) · [docs](../docs/child-sessions.md))
- **Resize, Phases 1–4** — in-place `alloc resize` / `instance resize`, the `job resize` task override, `--move`; sizes stay with instances across suspend and resume ([plan](plans/resize-plan.md) · [calls](calls/resize-plan_calls.md) · [docs](../docs/resize.md))
- **The `isolate` runtime, Phases 1–5 (closes "A WASM runtime")** — WebAssembly components on wasmtime 50 in `legion-isolate`, a separate, self-confined host process per cordon replica per node (eight layers after the hardening round: user, network, and PID namespaces with a confined supervisor, an empty read-only mount root, `no_new_privs`, Landlock with its signal and abstract-socket scopes where the kernel has them, and a seccomp allowlist with argument filters; `confine = "enforce"` by default), a compiler confined the same way (`--confine`) with pinned shards, epoch preemption, `limits.{cpu,wall,output}` and `memoryMax`; writing `cwasm` derived content; `lifetime = request | tenant | session` with tenant keys, session isolates addressed by node tag, hibernation to a `state` checkpoint and page-out through the host cgroup's `memory.reclaim`; the `js` (StarlingMonkey) and `python` (CPython 3.14, whole stdlib) images built by `crates/isolate/images/build.sh`; guest output in the envelope's task log; session runs on isolate tasks with `tier`, `tenant`, `session_id`, `args`, the pre-flight import check, the per-node memo, `nextTier` and client-side `--fallback vm`; HTTP ingress and egress through the envelope's datapath with credential injection; child VMs through `legion:isolate/vm`; cordons and namespace policy `isolateCordons`; `legion isolate stats|get`, MCP tools and catalog entries; the runtime-disable `auto`→VM reaction; session-isolate resize. The hardening round added: `config.entry` read without following symlinks; a host acting only as isolates the worker issued it; bounds on guest egress (8 in flight per invocation) and on what a host can make the worker hold; VM-tier `args` only after an entry-ending `run.command`, plus `LEGION_RUN_ARGS`; prefixed cordon names (`ns:`, `job:`, `shared`); a session's tenant winning and duplicate routing headers refused; NOT_FOUND for any session id the caller cannot read; guest-log rate limits; targeted hibernation; the host cgroup's `memory.max` following envelope resizes; `onWorkerStop` and `durability = "store"` refused at submit and `cordonBoundary = "vm"` by worker-config validation; then, after a verification re-review, injective host cgroup leaf names, one origin check on every host call, guest-log lines only for issued isolates with control characters escaped and per-task rate budgets, host-call limits derived from the host config with a body byte budget, read-only nested mounts, capabilities dropped without a user namespace, a fixed VM entry name for `args`, a spawner thread for hosts, `compile --deadline-secs` and a compile cgroup, and the seccomp log switch only in test builds. Verified end to end through a manager, a worker, and real hosts in a container (debug builds); the VM tier only with an `exec` stand-in. Phase 0 measured in a container only; Phase 6 has only the `mpk`/`small` layouts as config. Deferred items are open under "Isolates" ([plan](plans/isolate-runtime-plan.md) · [calls](calls/isolate-runtime-plan_calls.md) · [docs](../docs/task-runtime-isolate.md) · [Phase 0 findings](isolate-runtime-phase0-findings.md))
docs: [task-runtime-plugins.md](../docs/task-runtime-plugins.md)

- **Runtimes** — the task-runtime plugin framework, the "task kind" → "task runtime" rename ([plan](plans/task-runtime-rename-plan.md)), and the [noop](../docs/task-runtime-noop.md), [exec](../docs/task-runtime-exec.md), [container](../docs/task-runtime-container.md) (Apple `container` / runc or crun, no Docker) and [sandbox](../docs/task-runtime-sandbox.md) runtimes
- **Container runtime's core gaps closed** — Linux exec, `TaskPlugin::signal()`, `resource_enforcement()` with memory limits and `cores` cpuset pinning, exec adopts `WorkerCgroup`, task dir at `/legion/task`; Phase 3 superseded by `stop { signal, killAfter }` ([plan](plans/container-runtime-exec-and-signals-plan.md) · [calls](calls/container-runtime-exec-and-signals-plan_calls.md) · [docs](../docs/resource-enforcement.md))
- **Sandbox boots in ~200 ms; a cluster boots at the sum of its nodes' rate** — raw CH disks, `stateful.placement`, router fixes, pooled launches, `legion-loadgen --mode sessions` rig ([plan](plans/sandbox-boot-throughput-plan.md) · [calls](calls/sandbox-boot-throughput-plan_calls.md) · [docs](../docs/sandboxes-at-scale.md#measured-on-aws))
- **Sandbox restore doesn't wait 5 s for an egress proxy socket** — waits only when gated; proxy bound before launch ([calls](calls/gated-restore-datapath-wait_calls.md) · [docs](../docs/suspend-and-resume.md))
- **Gated sandboxes, Phases 1–5** — memory checkpoints with vsock override, `gated-default` deny and `legion-guest` agent, connection/byte activity signals, TAP addressing via `sandbox.bridges`, ingress and upstreams over vsock ([plan](plans/gated-sandbox-checkpoints-plan.md) · [calls](calls/gated-sandbox-checkpoints-plan_calls.md) · [docs](../docs/network-handlers.md#sandboxes))
- **Sandbox guest size follows `resources`** — scheduler reservation and VM size can't drift ([plan](plans/suspend-resume-and-oversubscription-plan.md#8-implementation-phases) · [docs](../docs/task-runtime-sandbox.md#guest-size))
- **`sandbox` adopts `resource_enforcement()` as `WorkerCgroup`** — VMM in the allocation cgroup with `sandbox.vmmOverheadMiB` ([docs](../docs/resource-enforcement.md#sandbox-tasks))
- **Sandbox snapshot/fork/suspend footguns** — fixed TAP refused where guests could share a node, forks aren't their origin in token checks, `cold_boot` recorded ([calls](calls/suspend-resume-and-oversubscription-plan_calls.md#135-a-fixed-tap-is-refused-wherever-two-guests-could-share-a-node-not-removed) · [docs](../docs/suspend-and-resume.md#when-a-resume-cannot-restore))
- **Wake on request, Phase 1** — `suspend.wake`, `legion start gateway` holding requests through one resume ([plan](plans/wake-on-request-plan.md) · [docs](../docs/gateway.md))
- **Park, Phases 1–2** — `suspend.park` and `$LEGION_PARK_SOCKET` for exec, container and sandbox ([plan](plans/park-plan.md) · [calls](calls/park-plan_calls.md) · [docs](../docs/park.md))
- **Health-check startup fixes** — startup probing between `every` ticks, a `boot` check's first-interval failure not counted, `net_capture_unavailable` no longer sticks ([plan](plans/task-start-critical-path-plan.md) · [calls](calls/task-start-critical-path-plan_calls.md) · [docs](../docs/jobs-tasks-and-scheduling.md#startup-probing))
- **Gated namespaces exist before their listeners** — created before proxy/resolver binds ([plan](plans/task-start-critical-path-plan.md))
- **Worker lock under churn** — VMM reaps, cgroup leaf creation, dir removal, and log manifests moved off the task-manager lock (busy ~20% at 250 starts/s) ([plan](plans/sandbox-boot-throughput-plan.md) · [results](../benchmarks/aws/sandbox-boot/RESULTS.md#bare-metal-second-campaign-2026-10-14))
- **Task process supervision** — child reaping, `restart` policy, exit-code task outcomes ([plan](plans/host-process-supervision-plan.md) · [calls](calls/host-process-supervision-plan_calls.md) · [docs](../docs/task-supervision.md))
- **Worker-owned cgroups for `exec`** — superseded by node-wide resource enforcement for `exec` and `container` ([docs](../docs/resource-enforcement.md) · [calls 16](calls/container-runtime-exec-and-signals-plan_calls.md))
- **`exec` hardening block** — `noNewPrivileges`, capability drops, `privateTmp`, `umask`, `nice` ([docs](../docs/task-supervision.md#process-hardening))
- **Resource enforcement on by default** — absent block means enabled; unconfined fallback with a warning ([docs](../docs/resource-enforcement.md#defaults-and-turning-it-off))
- **Worker graceful shutdown** — the worker handles SIGTERM/SIGINT (`worker_shutdown_signal` → `stop_worker_daemon`; verified in code, was still listed open)
- **Sandbox base root disk cache, Phase 1** — per-image base disk cloned per sandbox, verified on KVM at ~10 ms/clone on XFS ([plan](plans/rootfs-base-cache-plan.md) · [docs](../docs/task-runtime-sandbox.md#the-root-disk-cache))
- **Edge-started sessions** — session tickets and `Worker.StartSession` ([plan](plans/session-durability-and-routing-plan.md) · [docs](../docs/session-mode.md#starting-at-the-edge))
- **Sandbox exec Phase 0 (one host)** — proxied vs direct session benchmark up to 500 sessions ([calls 26](calls/sandbox-exec-plan_calls.md) · [numbers](../benchmarks/scheduler/README.md))
- **Sandbox exec Phase 1** — exec into microVMs over vsock via the guest agent, `--instance` on exec/action ([plan](plans/sandbox-exec-plan.md) · [docs](../docs/task-runtime-sandbox.md#exec))
- **Sandbox exec Phase 2 (direct exec)** — `MintExecTicket`, `legion exec --direct`, ticket revocation on heartbeat ([docs](../docs/exec-and-actions.md#direct-exec))
- **Sandbox exec Phase 3 (delegated minting keys)** — `CreateExecKey`/`ListExecKeys`/`DeleteExecKey`, `legion exec-key`, `exec-ticket sign` ([docs](../docs/exec-and-actions.md#delegated-minting-keys))
- **Exec fixes found on the way** — worker listener mTLS (worker-plane-auth Phase 1), ticket caller class folded into [worker-plane-auth §6.1](plans/worker-plane-auth-plan.md#61-the-worker-listener-is-mutual-tls-always), JWKS fetch panic moved off the runtime ([calls 10](calls/sandbox-exec-plan_calls.md)), `legion ledger -j` uses the standard envelope
- **Suspend Phase 1: measure and govern** — VMMs in allocation cgroups, per-allocation usage, `legion node usage`, exec/ingress activity ([docs](../docs/resource-enforcement.md#measuring-usage))
- **Suspend Phase 2: burst and oversubscribe** — `memoryMax`, oversubscribe ratios, pressure response, `node.pressure` events ([docs](../docs/resource-enforcement.md#memory-pressure))
- **Sandbox balloon** — balloon on both VMMs and free page reporting on cloud-hypervisor ([docs](../docs/resource-enforcement.md#sandboxes-and-the-balloon))
- **Suspend Phase 3: local checkpoints** — node content store (3a, Linux-checked), runtime checkpoint/restore (3b), snapshot tables/RPCs/CLI/ACL (3c) ([docs](../docs/suspend-and-resume.md) · [content store](../docs/artifacts.md#the-node-content-store))
- **Allocation ids never reused** — `AUTOINCREMENT` allocation table ([call 28](calls/suspend-resume-and-oversubscription-plan_calls.md))
- **Memory checkpoints of gated sandboxes** — restores retarget the vsock per allocation ([plan](plans/gated-sandbox-checkpoints-plan.md) · [calls 5](calls/gated-sandbox-checkpoints-plan_calls.md))
- **Suspend Phase 4: suspend and resume** — `SuspendInstance`, `suspend` block, guest resume handler, fresh ids/tokens, metrics ([docs](../docs/suspend-and-resume.md))
- **Suspend Phase 5: automatic suspend** — idle policy, idle sweep, `suspend` pressure step ([docs](../docs/suspend-and-resume.md#automatic-suspend))
- **Suspend Phase 6: fork** — `ForkInstance`, `--from-snapshot`, snapshot aliases, per-fork identity ([docs](../docs/suspend-and-resume.md#fork))
- **Fork fixes** — identity epoch retires origin tokens ([docs](../docs/workload-identity.md#forks-and-snapshots)), identity-minted ACL tokens expire with it, persisted `cold_boot` record ([docs](../docs/suspend-and-resume.md#when-a-resume-cannot-restore)), stopped forks keep their restore source, `requireId` fork ids, TAP refusals (later superseded by removing fixed TAPs)
- **Suspend Phase 7a: content locality** — `ContentPublisher` (s3/file), location index, `locality` score, metric, bench group ([docs](../docs/artifacts.md#content-locality))
- **Suspend Phase 7b: durable checkpoints** — `durability = "store"`, publish/fetch, cross-node resume, `snapshot warm`, `checkpoint-fetch` capability, publish retry, unpinning after upload ([docs](../docs/suspend-and-resume.md#durability-surviving-the-node))
- **Cross-node fork `--spread`** — publishes and places forks apart ([docs](../docs/suspend-and-resume.md#spreading-forks-across-nodes))
- **Published checkpoint encryption** — per-part sealing under the keyring checkpoint key ([plan](plans/checkpoint-encryption-plan.md) · [docs](../docs/suspend-and-resume.md#published-checkpoint-encryption))
- **Phases 5/6/7b integration** — renumbered protos, ordered migrations, restore binder carries instance id, `restore_content` filled, keyed-instance bench groups
- **Resume guards (Track 2)** — partial `reserve`, make-room suspends, snapshots budget filter ([docs](../docs/suspend-and-resume.md#capacity-for-resumes))
- **Track 1: agent sandboxes** — auto TAPs, microVM idle detection, identity refresh without reboot, macOS idle suspend, payload fix ([guide](../docs/guides/agent-sandboxes.md))
- **`hint` idle signal as `suspend.park`** — workload says `busy`/`park` on the park socket ([plan](plans/park-plan.md) · [docs](../docs/park.md))
- **Lazy memory restore, Phase 1** — on-demand restore on cloud-hypervisor v52+, `memory_restore = lazy|eager` ([plan](plans/lazy-memory-restore-plan.md) · [docs](../docs/task-runtime-sandbox.md#how-memory-comes-back))
- **Sandbox `config.memory_mib` removed** — refused at submit; the guest boots at `resources.memoryMax`, else `memory`, else 512 MiB, read per unit at launch ([calls 26](calls/suspend-resume-and-oversubscription-plan_calls.md) · [docs](../docs/task-runtime-sandbox.md#guest-size))
- **A smaller guest kernel, documented** — the options Legion needs and a build recipe ([docs](../docs/task-runtime-sandbox.md#a-smaller-guest-kernel) · [calls 18](calls/sandbox-boot-throughput-plan_calls.md))
- **Container OCI stop tests drive `stop_with` then the hard stop**, and pass under runc 1.5.2

## Artifacts

- **Derived content, Phase 1** — the `derived` class of the node content store; sandbox root disk bases on it, listed in `legion node content` with hit and build metrics (closes root disk cache Phase 4); `content.budgets.derived` replaces `LEGION_SANDBOX_ROOTFS_CACHE` ([plan](plans/derived-content-plan.md) · [calls](calls/derived-content-plan_calls.md) · [docs](../docs/artifacts.md#derived-content)); the `cwasm` kind arrived with the isolate runtime (2026-11-11, built by a confined `legion-isolate compile` recipe); the `preinit-*` kinds still have no builder (see "Isolates") and `vm-base` waits for prepared VMs
[plan](plans/artifact-system-plan.md) · [docs](../docs/artifacts.md)

- **Artifact basics** — `oci://` fetcher and ORAS, KV-backed fetch auth, https/file/git/s3 fetchers with checksums and safe unpack, fetcher plugins, OCI layout → runtime bundle
- **Model weights fit the artifact cache** — 50% disk default budget, `{ share, max, min }`, per-layer ORAS cache, 1 TiB / 2 h fetch defaults (review AI6) ([plan](plans/device-model-and-plugin-seams-plan.md) · [calls](calls/device-model-and-plugin-seams-plan_calls.md) · [docs](../docs/artifacts.md#oras-layers))
- **Disk-aware content budgets and checkpoint room** — `content.budgets.*` as size or share, workers report free/reclaimable disk, placement uses it ([calls](calls/suspend-resume-and-oversubscription-plan_calls.md#131-published-checkpoints-are-unpinned-on-their-node-content-store-follow-up) · [docs](../docs/artifacts.md#the-node-content-store))
- **Node content store (artifact cache Phase 1 core)** — CAS + sqlite index, verified fetches written to the store, size budget + `minFreeDisk`, LRU GC, pinning, single-flight, hit/miss in `artifact_states` ([plan](plans/artifact-cache-and-prewarm-plan.md) · [docs](../docs/artifacts.md#the-node-content-store))
- **Content locality (artifact cache Phase 3)** — heartbeat deltas into an off-raft location index, `schedulerSettings.weights.locality` scoring, and `legion snapshot warm` making a warmed node win placement ([plan](plans/suspend-resume-and-oversubscription-plan.md) · [docs](../docs/artifacts.md#content-locality))
- **Default content budgets fit small disks** — floors give way proportionally so the budgets never add up past the filesystem ([docs](../docs/artifacts.md) · [calls](calls/todo-closeout-2026-11-11_calls.md))
- **Published parts: a bucket lifecycle-rule recipe** — Legion still deletes nothing (call 62) ([docs](../docs/suspend-and-resume.md#expiring-published-parts) · [calls 142](calls/suspend-resume-and-oversubscription-plan_calls.md))

## Rollouts

[docs](../docs/rollouts.md)

- **Rollout basics** — new versions start a deployment, strategy framework, `immediate` and `rolling` strategies, health-gated progression, UI

## KV & secrets

[plan](plans/kv-reactive-task-updates-plan.md) · [docs](../docs/kv-store.md)

- **KV basics and reactive updates** — tasks read KV, values in job definitions, env/file injection with watch/onChange/onDelete and restart
- **Live KV file updates** — `onChange = "update"` rewrites files in place with `changeMode` ([plan](plans/kv-cert-delivery-plan.md) · [calls](calls/kv-cert-delivery-plan_calls.md) · [docs](../docs/kv-store.md#live-file-updates))
- **KV-backed ingress certificates** — `tls.certFrom = { kv }` hot-swapped via `GetIngressCertificates`, plus cert-renewer/tls-edge examples ([docs](../docs/network-handlers.md#certificates-from-kv) · [guide](../docs/guides/tls-certificates.md))
- **Egress credential injection, Phases 1–2** — `connect.upstreams[].credentials` from KV or a minted workload identity, never entering the workload ([plan](plans/egress-credential-injection-plan.md) · [calls](calls/egress-credential-injection-plan_calls.md) · [docs](../docs/network-handlers.md#credential-injection))
- **KV encrypted at rest, on by default** — AES-256-GCM under wrapped data keys, rotation and rewrap, `exec` root-key provider (KMS/Vault helpers), root key in the raft join reply ([plan](plans/kv-encryption-plan.md) · [docs](../docs/kv-encryption.md))
- **Published checkpoints sealed** — AES-256-GCM STREAM chunks under keyring-derived part keys ([plan](plans/checkpoint-encryption-plan.md) · [calls](calls/checkpoint-encryption-plan_calls.md) · [docs](../docs/suspend-and-resume.md#published-checkpoint-encryption))
- **Published checkpoints chunked and deduplicated** — 1 MiB chunks, zero chunks skipped, ~1.1 GiB → 38–131 MiB per checkpoint ([plan](plans/chunked-checkpoint-publish-plan.md) · [calls](calls/chunked-checkpoint-publish-plan_calls.md) · [docs](../docs/suspend-and-resume.md#how-published-parts-are-stored))

## Service discovery (v1)

[plan](plans/service-discovery-final-plan.md) · [docs](../docs/service-discovery.md)

- **Service discovery v1** — named services with checks, catalog as a projection, `legion service list|resolve|doctor`, `connect.upstreams` load-balanced loopback forwarder, `service.allow` on the connect path, MCP tools

## Networking

Plans: [mesh control path](plans/mesh-control-path-plan.md) ([calls](calls/mesh-control-path-plan_calls.md)) · [datapath performance](plans/datapath-performance-plan.md) ([calls](calls/datapath-performance-plan_calls.md)) · [mesh interfaces](plans/mesh-interfaces-plan.md) ([calls](calls/mesh-interfaces-plan_calls.md)) · docs: [network-handlers.md](../docs/network-handlers.md), [network-plugins.md](../docs/network-plugins.md), [service-discovery.md](../docs/service-discovery.md)

- **Mesh control path (review NET1–NET9)** — per-service endpoint deltas pushed on `StreamWork`, memoized policy assignments, TLS sessions surviving cert refresh, bounded HTTP bodies (`http.retries.maxBodyBytes`), IPv6 via `structs::hostport`, nftables guard verdict maps, CA-rotation splay, sharded per-connection ledger, `127.64.0.0/10` VIP blocks and `61000–65535` datapath ports ([docs](../docs/service-discovery.md#how-endpoints-reach-a-node))
- **Datapath performance** — `least_latency` (P2C peak-EWMA) default balancer, HTTP connection pooling with stale-retry, `splice(2)` for bulk L4, no per-connection list copies, one-atomic `inc_by`, `net_bench` ([docs](../docs/network-handlers.md#connection-pooling))
- **Mesh interfaces Phase 0** — chain `dlp`/`log` handlers and `onError` actually run
- **Mesh interfaces S1–S5** — one `Handler` trait and compiled `Pipeline`; upstreams as one `to` with scheme-as-protocol; `service.mesh = true` and port-level TLS with `service.allow` as the caller list; one `{ kv | identity | file }` source grammar behind `FetchDatapathMaterial` ([docs](../docs/network-handlers.md#the-mesh))
- **Kernel classifier (Track A)** — eBPF host guard, host-mode egress at `connect()`, DNS capture, ring-buffer denials, transparent capture with SNI/Host peek ([findings](kernel-classifier-findings.md) · [docs](../docs/network-handlers.md#the-kernel-classifier))
- **WASM network plugins (Track B)** — proxy-wasm ABI on wasmi, `legion net plugin` CLI/RPCs/MCP, request/body/response/done stages, `legion_sign`, `dispatch_http_call`; also closes handlers-plan Phase 8 ([docs](../docs/network-plugins.md))
- **macOS gated networking, phases 1–3** — per-allocation egress proxy, Seatbelt for `exec`, Apple `container` VMs under a `pf` anchor ([plan](plans/macos-gated-plan.md) · [docs](../docs/network-handlers.md#gated-mode-on-macos))
- **Datapath simplification** — one egress judge (`netd::egress`), SOCKS5 dropped, dead code and `MaterialProvider` removed
- **Shared WASM embedding layer** — `crates/worker/src/wasm/` engine, memory cap, WASI preview1 shim, module fetching
- **M1 policy + datapath core** — `netpolicy` merge with provenance, `net_policy` RPCs and `legion net policy`, connect-time rules, external upstreams, fail-static policy delivery, `net.*` events and metrics ([plan](plans/network-handlers-plan.md) · [docs](../docs/network-handlers.md))
- **M2 HTTP/1.1 mode** — request-time rules, structured 403s, `dlp`, NDJSON access log, timeouts/retries/split/outlier ejection ([plan](plans/network-handlers-plan.md))
- **M3 name-is-config** — per-allocation loopback /24, `<key>.svc.legion` resolver with VIPs, egress policy at resolution ([plan](plans/network-handlers-plan.md))
- **M4 `gated` on Linux for `container` and `exec`** — worker-owned netns, in-namespace listeners and egress proxy, fail-closed gate ([plan](plans/network-handlers-plan.md))
- **M5 datapath across worker restarts** — `netd_state.json` replay and `net_capture_unavailable` health ([plan](plans/network-handlers-plan.md))
- **M9 `gated` for `sandbox` over vsock** — no-NIC boot, host-side proxy/resolver sockets, `legion guest-agent` relay ([plan](plans/network-handlers-plan.md))
- **M6 ingress chains, drain-before-stop, `legion net doctor`** ([plan](plans/network-handlers-plan.md))
- **M7 external handlers and mTLS** — `authz` webhooks, `proxy` hand-offs, ingress TLS, manager-held mesh CA with node certs, `legion net ca` ([plan](plans/network-handlers-plan.md))
- **M8 service graph** — caller-recorded RED edges, `ListServiceEdges`, `legion service edges`, UI graph ([plan](plans/network-handlers-plan.md))
- **M10 `connect.templates` and `local_first`** ([plan](plans/network-handlers-plan.md))
- **KV-held client CA for mutual ingress** — `network.ports[].tls.clients.ca = { kv }` ([plan](plans/mesh-interfaces-plan.md) · [docs](../docs/network-handlers.md#certificates-and-cas-from-kv))
- **`connect.upstreams` from a gated `sandbox` guest** — credential injection keeps keys out of gated sandboxes via vsock relays ([calls](calls/egress-credential-injection-plan_calls.md))
- **Every gated `container`/`exec` allocation gets the `127.0.0.53` resolver in its namespace** — already shipped with gated-by-default containers; a worker test now pins it (`specs.rs`)

## Stateful workloads & volumes

[stateful plan](plans/stateful-workloads-and-instance-identity-plan.md) · [volume plan](plans/volume-plugin-system-plan.md) · [docs](../docs/volumes.md)

- **Instance identity** — slot and keyed modes, storage handles, fencing
- **Volume plugin system** — `VolumePlugin` contract and exec ABI, built-in `local-dir`/`host`/`nfs`/`tmpfs`, conformance harness, `legion volume` CLI/MCP/UI, [Postgres](../docs/guides/postgres.md)/[Redis](../docs/guides/redis.md) guides; block-mode volumes for sandboxes
- **Idle sessions sleep on their node (Phase 5a)** — `suspend.durability = "local"`, `keepFor`, `checkpointEvery`, no raft entries ([plan](plans/session-durability-and-routing-plan.md) · [calls](calls/session-durability-and-routing-plan_calls.md) · [docs](../docs/session-mode.md#idle-sessions))
- **Edge boot** — provisional allocations, `stage`/`boot`/`serve` gate levels, manager-signed boot tickets; the ticket spend rides the ratification in one entry ([plan](plans/edge-boot-plan.md) · [calls](calls/edge-boot-plan_calls.md) · [docs](../docs/edge-boot.md))
- **Ratification throughput, Phase 1** — ratifications fenced by committed state and fill commit batches; racing creates refuse `instance_claimed` ([plan](plans/edge-ratify-throughput-plan.md) · [calls](calls/edge-ratify-throughput-plan_calls.md) · [docs](../docs/edge-boot.md#ratification-under-load))
- **Edge starts on a three-manager raft cluster** — dispatch carries `speculativeStart`, followers answer not-leader, `RATIFIED_SETTLE` window, own `LEGION_INSTANCE_ID` ([results](../benchmarks/aws/sandbox-boot/RESULTS.md#ratification-on-three-managers-2026-10-14) · [docs](../docs/edge-boot.md#ratification-on-a-raft-cluster))
- **Edge admission control, Phase 1** — per-node AIMD claim window, enforced `maxProvisionalPerNode`, retryable `OVERLOADED`, sub-second `ratifyTimeout` refused ([plan](plans/edge-admission-control-plan.md) · [calls](calls/edge-admission-control-plan_calls.md) · [docs](../docs/edge-boot.md#overload))
- **Sandbox scaling, Phases 0–3** — lifecycle entry assertions and `legion-loadgen --mode instances`, 22 → 8 entries per lifecycle, `StreamWork` delivery, `blocked` evals woken by matching capacity; reconnect resync fix ([plan](plans/sandbox-scaling-plan.md) · [calls](calls/sandbox-scaling-plan_calls.md) · [docs](../docs/sandboxes-at-scale.md))
- **Sandbox scaling, Phases 4–6** — server-minted 130-bit keyed ids, `suspend.archive = "slim" | "manifestOnly"`, `stateful.mode = "session"` with the session router and `PromoteInstance` ([docs](../docs/session-mode.md))
- **Suspend/resume don't wait on heartbeats** — checkpoint requests pushed on `StreamWork`, early-heartbeat reports ([plan](plans/checkpoints-on-stream-plan.md) · [calls](calls/checkpoints-on-stream-plan_calls.md) · [docs](../docs/suspend-and-resume.md#how-it-works))
- **Concurrent session starts (session durability Phase 1)** — launch off the task-manager lock, no per-start fsync, bounded launch pool ([plan](plans/session-durability-and-routing-plan.md))
- **Node-local `sessions.db` (Phase 2)** — sessions adopted on worker restart; manager decode limit raised to 64 MiB ([plan](plans/session-durability-and-routing-plan.md))
- **Tag-first session routing (Phase 3)** — start node's tag in the session id, fan-out as fallback ([plan](plans/session-durability-and-routing-plan.md))
- **Edge starts (Phase 4)** — `ListSessionNodes`, session tickets, `Worker.StartSession` with no manager on the path ([plan](plans/session-durability-and-routing-plan.md))
- **Session start hot path cut** — router room reservation, random tie-breaks, single-flight refresh, cached task configs, off-lock process start time and staging ([plan §6.5](plans/session-durability-and-routing-plan.md#65-everything-else-between-here-and-tens-of-thousands-a-second))
- **Checkpoint resumes boot off the task-manager lock** — several resumes per node in parallel ([calls](calls/lazy-memory-restore-plan_calls.md) #7)
- **Local session sleep (Phase 5a)** — `durability = "local"`, `suspend.keepFor`, idle suspend, eviction, routed suspend/resume ([docs](../docs/session-mode.md#idle-sessions))
- **Store-durable sessions (Phase 5b)** — sealed parts, session manifests with conditional generation claims (`file://`, `s3://`), cross-node resume and manifest fallback ([docs](../docs/session-mode.md#moving-a-session))
- **Batched duplicate sweep and generation-first winner rule** — plan §6.7 mechanism 3 ([plan](plans/session-durability-and-routing-plan.md))
- **No split brain for `store` sessions** — manifest create, superseded-on-write, fence list, probe-before-move, self-fencing after `stateful.fenceAfter` ([docs](../docs/session-mode.md#no-split-brain))
- **`suspend.checkpointEvery` and `suspend.onWorkerStop = "checkpoint"`** — staggered memory checkpoints and SIGTERM checkpoint/restore ([docs](../docs/session-mode.md#idle-sessions))
- **Volume snapshot/restore/clone ops** — in the driver contract, implemented by `local-dir`, in the conformance suite ([docs](../docs/volumes.md#snapshot-restore-and-clone))
- **`legion session start --resume-id` forces a move past a node listed `down`/`lost`** ([calls 48](calls/session-durability-and-routing-plan_calls.md) · [docs](../docs/session-mode.md))

## Persistence

[plan](plans/persistence-plan.md) · [docs](../docs/persistence-streaming.md), [raft-operations.md](../docs/raft-operations.md)

- **Streaming SQLite replication** — Litestream-style, optional lease failover
- **Raft control plane** — leader election, failover controls, membership, observability, CLI/UI
- **Revisions, watches, and blocking queries** — revisions on job/alloc/KV, `x-legion-min-revision`/`--wait`, `Manager.Watch`, opt-in linearizable reads (review ST2) ([plan](plans/revisions-and-watches-plan.md) · [docs](../docs/watches-and-blocking-queries.md))
- **Failover doesn't resync every worker; heartbeat report writes run off the runtime** — revisioned `StreamWork` resume, blocking-pool report writes (review ST3, ST5) ([docs](../docs/architecture-managers-workers.md#delivery))
- **Manager's own identity for state replication** — one credential path for WAL replica, restore, and lease; S3 lease ETag preconditions fixed ([plan](plans/manager-workload-identity-plan.md) · [calls](calls/manager-workload-identity-plan_calls.md) · [docs](../docs/persistence-streaming.md#credentials))
- **Replicated write path: group commit, one execution, one fsync** — shared capture per entry, speculative apply under `single`, `synchronous = NORMAL` (review SCH1) ([plan](plans/raft-write-path-plan.md) · [calls](calls/raft-write-path-plan_calls.md) · [docs](../docs/multi-raft.md#the-write-path-group-commit-and-single-execution))
- **Raft log tail retained by bytes** — `raft.logRetainBytes` (256 MiB) (review ST4) ([docs](../docs/raft-operations.md#operational-notes))
- **Data migrations run through the raft log** — schema-only `migrations.rs`, data steps in `storage/data_migrations.rs` run once cluster-wide (review ST1) ([docs](../docs/upgrades.md#data-migrations-run-once-through-the-log-after-the-upgrade))
- **`LEGION_DATA_DIR`** — moves every `.legion/*` default for one process ([plan](plans/data-dir-env-plan.md) · [docs](../docs/data-directory.md))
- **Read model follows SQLite in every mode** — changeset projection under `local_disk`, `data_version` rebuild, periodic scrub ([plan](plans/read-model-coherence-plan.md) · [docs](../docs/state-store.md#read-model-coherence))
- **JSONB columns rebuilt to TEXT affinity** — 46 columns, column order preserved ([plan](plans/jsonb-text-affinity-plan.md) · [docs](../docs/state-store.md#column-affinity))
- **Real raft persistence** — quorum-committed changeset replication, per-lane log stores, snapshots, membership ([plan](plans/durability-and-ops-execution-plan.md) · [docs](../docs/multi-raft.md))
- **Multi-raft lanes** — control/scheduler/status lanes, single-writer table ownership, per-lane snapshots ([docs](../docs/multi-raft.md))
- **Bounded lane-leadership convergence** — campaigns only for lanes led elsewhere, with backoff; joint-membership joiner fix ([calls](calls/durability-and-ops-execution-plan_calls.md) · [docs](../docs/multi-raft.md))
- **Standing-split resilience** — `ForwardLaneWrite`, per-lane reconcilers for deferred projections ([plan §8](plans/durability-and-ops-execution-plan.md) · [docs](../docs/multi-raft.md))
- **Adversarial verification of lanes v2** — catching-up hold cap, campaign spacing, `raft status` joint voters, safe remove-member, and more ([calls #7–#16](calls/durability-and-ops-execution-plan_calls.md))
- **Lane-isolation benchmark** — `lanes` keeps scheduler p99 flat under cross-lane load ([docs](../docs/multi-raft.md))
- **Manager write-path contention instrumentation** — coalesce, pipeline, and runtime saturation metrics ([calls](calls/manager-contention-instrumentation_calls.md) · [docs](../docs/metrics.md))
- **Raft engine metrics** — per-lane `legion_raft_*` gauges and capture/propose histograms ([docs](../docs/metrics.md))
- **Eval `NotRaftLeader` retries back off exponentially** (5 s → 60 s)

## Workload identity

[plan](plans/workload-identity-oidc-plan.md) · [docs](../docs/workload-identity.md)

- **Manager as OIDC issuer** — keys, discovery, JWKS, rotation; issuance RPC, job-spec API, worker delivery, change mode; node and control-plane identities
- **Manager uses a platform identity for itself** — no `managerIdentities` block ([plan](plans/manager-workload-identity-plan.md#6-why-the-manager-does-not-use-legions-own-idp))
- **Identity epochs** — fork/snapshot capture retires earlier tokens in Legion's verifiers ([calls](calls/suspend-resume-and-oversubscription-plan_calls.md#137-fork-and-snapshot-captures-retire-the-origins-earlier-tokens-with-a-per-instance-identity-epoch) · [docs](../docs/workload-identity.md#forks-and-snapshots))
- **Workload identity on by default** — zero-config issuer; `enabled = false` opts out ([calls](calls/autoscaler-v2-plan_calls.md) · [docs](../docs/workload-identity.md#manager-configuration))

## Ecosystem apps

Plans: [legion apps](plans/legion-apps-plan.md) ([calls](calls/legion-apps-plan_calls.md)) · [app object kinds](plans/app-object-kinds-plan.md) ([calls](calls/app-object-kinds-plan_calls.md)) · [autoscaler v2](plans/autoscaler-v2-plan.md) ([calls](calls/autoscaler-v2-plan_calls.md)) · docs: [apps.md](../docs/apps.md), [app-objects.md](../docs/app-objects.md), [autoscaling.md](../docs/autoscaling.md)

- **GitOps: the bundled `gitops` app** — `gitops/Repo` kind, render-in-child, preflight/apply/prune, `legion gitops add|status|sync`, UI page ([plan](plans/gitops-plan.md) · [calls](calls/gitops-plan_calls.md) · [docs](../docs/gitops.md))
- **Ingress, Phases 1 and 2** — bundled `ingress` app (seeded disabled), `service.ingress = { host, path, stripPath }`, an HTTP/1.1+h2 proxy on 80/443 with SNI certificates from KV and from the cert manager (HTTP-01 answered from the Issuer's thumbprint), "Point DNS here", the Ingress page, and exec `capabilities.add = ["net_bind_service"]` ([plan](plans/ingress-app-plan.md) · [calls](calls/ingress-app-plan_calls.md) · [docs](../docs/ingress.md))
- **Per-node replicas for apps** — `replicas = "per-node"` (`count = { num = 1, per = "id" }`), `legion app set --replicas`, seed `pool`; the ingress defaults to it ([plan](plans/app-per-node-replicas-plan.md) · [calls](calls/app-per-node-replicas-plan_calls.md) · [docs](../docs/apps.md#replicas))
- **Cert manager built in** — bundled `certs` app with `certs/Issuer` (`acme`/`ca`/`selfSigned`) and `certs/Certificate`, RFC 8555 client with DNS-01/HTTP-01, renewal with backoff, UI and `legion cert renew` ([plan](plans/cert-manager-plan.md) · [calls](calls/cert-manager-plan_calls.md) · [docs](../docs/cert-manager.md))
- **Apps A1: places to run** — `legion-system` namespace, reserved-prefix glob rule, node-side `namespaces`, default-on system worker per manager, registration-pinned node namespaces; follow-ups: host-guard names per cgroup parent, `*` pool globs skip `legion-`, list-valued `pools.default` ([docs](../docs/namespaces.md#reserved-namespaces))
- **Apps A2: identity that just works** — `permissions`/`policies` on the `legion` identity with grant checks at submit, `job_grant` table, `legion acl grants list|revoke`, per-allocation API socket and ambient `LEGION_ADDR=unix://…` ([docs](../docs/auth-and-acls.md#job-grants))
- **Apps A3: apps** — manifests, the Control-leader reconciler with `managedBy` stamps, `legion app …` CLI/RPCs/MCP, build-marker rollout of bundled apps, `app` ACL resource, app events ([docs](../docs/apps.md))
- **Apps A4–A5: UI tier 1 and sandboxed pages** — typed `ui` block and `legion-view/1` blocks, `InvokeAppAction`, SPA slot outlets, `srcdoc` iframe pages with a `PAGE_RPCS` bridge ([docs](../docs/apps.md#views-and-actions))
- **App actions run as the viewer** — `legion-on-behalf-of` delegation intersected with the app's grant ([plan](plans/app-actions-as-viewer-plan.md) · [calls](calls/app-actions-as-viewer-plan_calls.md) · [docs](../docs/auth-and-acls.md#delegated-identities))
- **Apps A6a: strict jobspec fields** — unknown keys fail submit with did-you-mean; `--allow-unknown-fields` records them ([docs](../docs/jobs-tasks-and-scheduling.md#unknown-fields))
- **Apps A6b: jobspec extensions** — manifest `extensions[]` at six points, `GetJobSchema`/`legion job schema`, in-place extension-only resubmits ([docs](../docs/apps.md#jobspec-extensions))
- **Apps hardening (security and correctness reviews, #60)** — KV refs need `kv:read`, no host-level constructs in app jobs, per-app uid, page RPC limits, CAS reconciler submits, corrupt-record isolation, leader-only actions, `--dry-run` everywhere, `uninstall --purge` ([docs](../docs/apps.md#trust-model))
- **App object kinds O1–O4** — generic `kinds` with storage, RPCs, `legion object …`; autoscaler policies and node groups as `autoscaler/ScalingPolicy`/`NodeGroup` objects (old `scaling_policy` table and verbs removed); `legion-for-object` authority with `via = "objects"`; CEL validation `rules` with cost bounds ([docs](../docs/app-objects.md))
- **Autoscaler Phase 2: task autoscaling as the bundled app** — `autoscale` jobspec field, layered defaults, CAS `ScaleTask` with pins/pauses/history, safeguard pipeline, `legion scaling …` CLI/MCP ([docs](../docs/autoscaling.md))
- **Autoscaler Phase 3: external sources, scale to zero, simulate** — `prometheus`/`http` sources, `min = 0` with activation, `legion scaling simulate` ([docs](../docs/autoscaling.md#sources))
- **Autoscaler Phase 4: plugins** — `legion-scaling/1` JSON-RPC protocol, plugins via app config, `exec`/`noop` targets, conformance suite, Rust SDK and examples ([docs](../docs/autoscaling-plugins.md))
- **Autoscaler Phase 5b: node groups** — capacity-demand decisions, join tokens per scale-up, drain → terminate → forget, CAS action intents, first-party `aws-asg` plugin ([docs](../docs/autoscaling.md#node-groups))
- **Autoscaler Phase 6: UI as app views** — Autoscaling page, job panel, task badge, node-group panel ([docs](../docs/autoscaling.md#in-the-ui))
- **Autoscaler Phase 7: sharded replicas** — 64 KV-leased shards, delta watches, per-plugin concurrency; 10,000-policy load test ([docs](../docs/autoscaling.md#replicas) · [benchmarks](../docs/benchmarks.md#the-autoscaler-at-10000-policies))
- **Apps hold credentials on a default cluster** — workload identity on by default ([calls 37](calls/autoscaler-v2-plan_calls.md) · [docs](../docs/workload-identity.md#manager-configuration))
- **App actions run as the viewer** — `<viewer> via app <app>` ([plan](plans/app-actions-as-viewer-plan.md))
- **Autoscaler Phase 0: `legion job scale`** — `task_scale` overrides, `job_scale` eval, `scale` ACL, ScaleTask/GetJobScaleStatus ([docs](../docs/autoscaling.md))
- **Autoscaler Phase 1: scaling inputs** — per-(node, task) usage rollups, `GetScalingInputs` ([docs](../docs/autoscaling.md#scaling-inputs))
- **Autoscaler engine core** — `crates/common/src/autoscaler/` spec, strategies, schedules, safeguards, simulator ([calls](calls/autoscaler-v2-plan_calls.md))
- **Autoscaler Phases 2–4** — bundled task-autoscaling app, full policies (`prometheus`, `http`, `min = 0`, `scaling simulate`), external plugins and standalone policies ([plan](plans/autoscaler-v2-plan.md))
- **Autoscaler Phase 5a: node-autoscaling substrate** — join-token `max_uses`/`attributes`, `ForgetNode` / `legion node forget`, `GetCapacityDemand` ([docs](../docs/autoscaling.md#capacity-demand))
- **Autoscaler Phases 5b, 6, 7** — node groups run by the app, the UI as app views, sharded replicas with a 10,000-policy load test ([plan](plans/autoscaler-v2-plan.md))
- **`app config get` prints Nickel** that `app config set -f` reads back; `-j` keeps the JSON envelope ([docs](../docs/apps.md))

## Auth & ACLs

[plan](plans/auth-acl-plan.md) · [docs](../docs/auth-and-acls.md)

- **ACL Phases 1–4** — default-deny model with one-line enforcement and the `acl_endpoints` coverage gate, SSO and admin, worker-plane hardening (join tokens via `CreateJoinToken`, `:4748` gone), operations at scale
- **Audit 2026-10-09 security fixes** — scoped and pruned idempotency keys (S4), worker-plane identity binding (S1), browser origin allow-list (S2), private keys redacted from state queries (S3), cluster-wide `RotateMeshCa` (S5), stream re-auth (S7) ([audit](architecture-audit-2026-10-09.md) · [calls](calls/architecture-audit-2026-10-09_calls.md))
- **Keyring `provider test` / `root-key generate` in the capability catalog** ([calls](calls/cluster-ca-provider-plan_calls.md))
- **A session task's generated envelope no longer makes its job host-level** — the `envelope` runtime was classified as an unknown plugin, so every job with a session task (sandbox, container, isolate) needed `host = true`; a `file://` isolate module still needs it ([calls 37](calls/isolate-runtime-plan_calls.md#37-a-session-tasks-generated-envelope-is-not-host-level-for-every-runtimes-session-tasks) · [docs](../docs/auth-and-acls.md#host-level-jobs))
- **Namespaces, Phases 1–4** — the namespace object and ACL resource, canonical `ns/job` ids, `-n`/`-A`, namespaced services/DNS/events, cross-namespace upstreams, UI switcher and pages, namespace policies with per-band quota and admission checks ([plan](plans/namespaces-plan.md) · [calls](calls/namespaces-plan_calls.md) · [docs](../docs/namespaces.md))
- **A lost node rejoins only with a token for its own pool** ([docs](../docs/auth-and-acls.md#worker-nodes-join-tokens-and-node-tokens))

## Exec & actions

[plan](plans/exec-and-actions-plan.md) · [docs](../docs/exec-and-actions.md)

- **Exec and actions** — runtime `exec()` with PTY and limits, manager proxy with ACLs and audit, `legion exec`, browser terminal, typed actions with `legion action list|run`, UI and MCP tools

## Observability

[docs](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- **Observability basics** — scheduler tracing, benchmarks directory, task log capture/rotation/shipping (OTLP), OTLP export of Legion's spans, the event stream (`ListEvents`/`StreamEvents`)
- **Server console logs never block** — lossy non-blocking writer; a 1,200-start burst no longer stalls heartbeats ([calls](calls/sandbox-boot-throughput-plan_calls.md) · [results](../benchmarks/aws/sandbox-boot/RESULTS.md#console-logging-under-a-burst-2026-10-14) · [docs](../docs/observability.md#logging-and-tracing-setup))
- **Task start stage timings** — `legion_task_start_stage_seconds{runtime,stage}` plus one breakdown log line per start ([plan](plans/task-start-critical-path-plan.md) · [docs](../docs/metrics.md))
- **Simulated fleets feed `legion_worker_*` metrics** — shared telemetry listener, fleet aggregates, rig file_sd wiring ([plan](plans/worker-simulation-plan.md) · [calls](calls/worker-simulation-plan_calls.md) · [docs](../docs/worker-simulation.md))
- **Usage accounting over time** — hourly rollups per namespace/job/token, `GetUsage`, `legion usage` (review AI9) ([plan](plans/agent-governance-plan.md) · [docs](../docs/usage-accounting.md))
- **Session, node-memory and snapshot metrics** — `legion_sessions_envelope_free{job}`, `legion_node_memory_pressure_some_avg10{node}`, `legion_node_memory_reserved_bytes{node}`, `legion_snapshot_{capture,restore}_seconds{kind}`, worker `legion_content_store_{bytes,pinned_bytes,budget_bytes}{class}` ([docs](../docs/metrics.md) · [calls 61](calls/sandbox-scaling-plan_calls.md) · [calls 141](calls/suspend-resume-and-oversubscription-plan_calls.md))
- **Lossy log writers queue at most 16,384 lines** (was 128,000 each) ([docs](../docs/observability.md#logging-and-tracing-setup) · [calls/todo-closeout-2026-11-11_calls.md](calls/todo-closeout-2026-11-11_calls.md))

## Job types & dependencies

[docs](../docs/jobs-tasks-and-scheduling.md)

- **Job types** — `batch` lifecycle, cron `schedule` (cron or `{ every }`), co-located task groups, `depends_on` DAGs across machines with group-bound sidecars
- **Job status derived from the workload** — `running`/`pending`/`failed`/…/`stopped` instead of the stored flag ([plan](plans/job-status-plan.md) · [calls](calls/job-status-plan_calls.md) · [docs](../docs/jobs-tasks-and-scheduling.md#job-status))

## Plugin surface

[plan](plans/device-model-and-plugin-seams-plan.md) · [calls](calls/device-model-and-plugin-seams-plan_calls.md)

- **Device model: GPUs work** — `legion-device/1` plugins, `resources.devices`, peer-group/NUMA-aware assignment, CDI, reference `nvidia`/`fake` plugins (review PLG2) ([docs](../docs/devices.md))
- **Plugin panic containment** — in-plugin containment with a C-ABI probe, quarantine, `legion_plugin_panics_total`; API v3 (review PLG1) ([docs](../docs/plugin-system.md#panics-and-quarantine))
- **Node checks load once and report typed results** (review PLG5) ([docs](../docs/plugin-system.md#3-node-check-plugins))
- **OCI runtime classes** — `container.runtimeClasses` (gVisor, Kata), `config.runtimeClass` placement (review PLG3) ([docs](../docs/task-runtime-container.md#runtime-classes))
- **Job rules** — CEL `jobRules` on namespace policies, with `--dry-run` impact (review PLG4) ([docs](../docs/namespaces.md#job-rules))

## Agent surfaces

[docs](../docs/agent-readiness.md)

- **Agent basics** — `legion docs`/`capabilities`, `--json` envelope everywhere, proto as API source of truth, `legion mcp serve` with `--allow-write`, idempotency keys and validate/plan paths
- **Agent governance (review AI2–AI4)** — durable audit journal (`legion audit list`), 30-day default token expiry, child tokens, approval gates and per-token rate limits, caller-aware capabilities (`--mine`), MCP 2025-06-18 with a tool per RPC ([plan](plans/agent-governance-plan.md) · [calls](calls/agent-governance-plan_calls.md) · [audit docs](../docs/audit-journal.md) · [approvals docs](../docs/approvals-and-rate-limits.md))
- **Compare-and-set and a field diff for jobs** — `job run --expected-version`, `PlanJobReply.diff` (review AI5) ([plan](plans/revisions-and-watches-plan.md) · [docs](../docs/watches-and-blocking-queries.md#compare-and-set-on-jobs))
- **Idempotency races closed** — `install_app`/`uninstall_app` claim their key in their own entry and replay the winner; a losing `delete_namespace_policy` is `deduplicated`, not `NOT_FOUND` ([docs](../docs/agent-readiness.md))
- **`job validate` and `legion_job_validate` check job rules with `namespace:read`** (via `GetNamespace`, falling back to `acl:read`) ([docs](../docs/namespaces.md#job-rules) · [calls 20](calls/device-model-and-plugin-seams-plan_calls.md))
- **`legion ledger --job <JOB>` lists a job's allocations**; the ledger is in the capability catalog ([docs](../docs/jobs-tasks-and-scheduling.md#listing-a-jobs-allocations))

## UI

[docs](../docs/ui-architecture.md)

- **Navigation** — ⌘K palette, `j`/`k`/`o`/`u`, jump hints ([plan](plans/ui-jump-hints-plan.md) · [calls](calls/ui-jump-hints-plan_calls.md))
- **Job status and timestamps** — index and page share derived status; real `updated_at` column ([plan](plans/job-updated-at-plan.md) · [calls](calls/job-updated-at-plan_calls.md))
- **Holistic polish** — polling everywhere, stop/cordon/drain/scale controls, status chips (`ListJobs.status`), code splitting, `ui-build` CI with drift check and pinned protoc ([plan](plans/ui-holistic-polish-plan.md) · [calls](calls/ui-holistic-polish-plan_calls.md))
- **Task page and template redesign** — tabbed task page, typed Actions tab, guided template launch with diff; template jobs show their tasks, ordered submissions ([plan](plans/ui-task-and-template-redesign-plan.md) · [calls](calls/ui-task-and-template-redesign-plan_calls.md) · [docs](../docs/ui-architecture.md#task-page))
- **Operator UI gaps (#20)** — instance and allocation pages with event filters, hidden-until-revealed KV with "Used by" (`kv get --json` `references`), public endpoint addresses, superseded rollouts cancel themselves ([plan](plans/operator-ui-gaps-plan.md) · [calls](calls/operator-ui-gaps-plan_calls.md))
- **Suspend/resume UI (UI-1 to UI-6)** — usage cards, snapshots card, suspend/resume/fork controls, idle countdown, durability and locality cards, plus the server RPCs they need ([plan](plans/suspend-resume-ui-plan.md) · [docs](../docs/ui-architecture.md#usage-pressure-and-snapshots))
- **Sessions in the UI** — labelled sessions, Promote/End, paging bounded at scale, `ListInstances` session paging and counts ([plan](plans/session-ui-plan.md) · [docs](../docs/ui-architecture.md#sessions))

## TODO bug and security sweep (2026-11-08)

Fixes for the open bug and security items; judgement calls in
[todo-bug-sweep-2026-11-08_calls.md](calls/todo-bug-sweep-2026-11-08_calls.md).

- **Security**
  - **HTTP `proxy` hand-offs go over the mesh** to a mesh endpoint; a remote plain endpoint is refused with `502` rather than sent in plaintext ([calls #1](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/network-handlers.md)).
  - **The manager's `resolve = "pin"` lookups are judged by a host guard** (built-in classes plus its own addresses) ([calls #5](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/artifacts.md#what-the-host-guard-checks)).
  - **A runtime's own image pull is judged by the worker's host guard**, like a staged `oci://` artifact ([docs](../docs/artifacts.md#what-the-host-guard-checks)).
  - **`local-dir` volumes guard themselves**: `<state dir>/local-volumes/` is traverse-only and each volume a `0700` guard, handed to an `exec` task's `user` ([calls #2](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/volumes.md)).
  - **Linux container ids are node-scoped** (`legion-<node>-<alloc>`), and stale cleanup deletes only a container built from this allocation's bundle ([calls #6](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/task-runtime-container.md)).
  - **The template collision guard no longer confirms a job exists** to a caller who cannot read it ([calls #9](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/job-templates.md)).
  - **A follower's audit records are forwarded to a lane leader** (`ForwardableLaneWrite::AppendAudit`) instead of only being logged ([docs](../docs/audit-journal.md)).
  - **The UI's ACL mirror matches the engine**, checked by shared parity cases (`tests/fixtures/acl_parity_cases.json`, Rust and `npm run test:unit`) ([docs](../docs/ui-architecture.md)).
- **Correctness**
  - **`deduplicated` tells the loser of a concurrent idempotency-key race that it lost**, on every keyed RPC, with the winner's reply where it can ([calls #3](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/agent-readiness.md)).
  - **`StopRollout` with `revert` always ends the rollout**; a failed re-placement queues a `job_reschedule` retry ([calls #8](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/rollouts.md)).
  - **A rollout's replacement honors the previous version's reschedule backoff** ([docs](../docs/rollouts.md)).
  - **A stale eval worker cannot settle an eval a newer claim holds** (claim-epoch fence) ([calls #10](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/scheduler-and-eval-loop.md)).
  - **The preemption floor counts the task's allocations across the fleet**, not just the loaded candidates ([docs](../docs/preemption-and-eviction.md)).
  - **`onChange = "none"` no longer becomes a rollout**: the allocations move to the new config in place ([calls #4](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/kv-store.md)).
  - **A fail-open body hop streams an oversized body through** instead of answering `413` ([calls #7](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/network-handlers.md)).
  - **`UpsertNetPlugin` re-checks running jobs that use the plugin** ([calls #11](calls/todo-bug-sweep-2026-11-08_calls.md) · [docs](../docs/network-plugins.md)).
  - **The task-config cache's read-through can't resurrect a destroyed list**, and the scrub covers task configs ([docs](../docs/state-store.md)).
  - **A failed changeset projection wakes blocked evals** (a `recovered` capacity event per available node) ([docs](../docs/state-store.md)).
  - **Single-node mode heals a crash mid-reschedule** at startup (missing-rollout reconcile and eval regeneration) ([docs](../docs/scheduler-and-eval-loop.md)).
  - **`node_alloc_usage` rows of removed nodes are swept** ([docs](../docs/scheduler-and-eval-loop.md)).
  - **The store's WAL checkpointer is off when rustyriver owns checkpointing** (streaming with a replica) ([docs](../docs/persistence-streaming.md)).
  - **The worker boot fence reports what it killed** as self-fenced failures and releases dead allocations' volumes ([docs](../docs/stateful-workloads.md)).
  - **Orphaned gated namespaces are swept** after a worker restart ([docs](../docs/network-handlers.md)).
  - **A pushed gated task binds its namespace, proxy, resolver, and upstreams at launch** ([docs](../docs/network-handlers.md)).
  - **No-op `rollout_tick` evals requeue quietly**, with no eval events ([docs](../docs/event-stream.md)).
  - **The sandbox VMM name is parsed once** (`SandboxVmm`) and the checkpoint matches are exhaustive.

## TODO close-out (2026-11-10)

Small items done, and items removed as duplicated, already done, or decided;
judgement calls in [todo-closeout-2026-11-10_calls.md](calls/todo-closeout-2026-11-10_calls.md).

- **Fixed**
  - **The default heartbeat ceiling is 55s**, under the stateful self-fence; a `maxInterval` at or past 90s warns at startup ([calls #1](calls/todo-closeout-2026-11-10_calls.md) · [docs](../docs/heartbeat-cadence-and-scale.md#known-limitations)).
  - **`resources.cpus` is refused** with a hint naming `cpu` and `cores`; the example jobs use `cpu` in MHz ([calls #2](calls/todo-closeout-2026-11-10_calls.md) · [docs](../docs/resource-enforcement.md)).
  - **`DeleteNetPlugin` is refused while a running job uses the plugin**, and `referenced_by_jobs` lists the jobs ([calls #3](calls/todo-closeout-2026-11-10_calls.md) · [docs](../docs/network-plugins.md)).
  - **`legion kv list --values` marks a value it cannot decrypt** (`locked`) instead of failing the list, and filters by ACL before decrypting ([calls #5](calls/todo-closeout-2026-11-10_calls.md) · [docs](../docs/kv-encryption.md)).
  - **A follower forwards `RatifyClaim` to the leader** through the typed not-leader status ([calls #6](calls/todo-closeout-2026-11-10_calls.md) · [docs](../docs/edge-boot.md#ratification-on-a-raft-cluster)).
  - **A checkpoint, suspend, or resume finds an edge-started allocation by the manager's id** (`checkpoint_begin` and its two followers translate to the local key).
  - **Workers publish `pool` as an attribute**, so `count.per = "pool"` works; one-per-node is `per = "id"` ([calls #7](calls/todo-closeout-2026-11-10_calls.md) · [docs](../docs/jobs-tasks-and-scheduling.md#countper-behavior)).
  - **`node.pressure` event metadata is camelCase** like every other event (`freedBytes`, `instanceId`, …) ([docs](../docs/event-stream.md)).
  - **`snapshot.restored` carries `durationMs`** ([docs](../docs/event-stream.md)).
  - **The volume-claim sync reads only jobs whose latest config declares a volume**, in one query.
- **Observability**
  - **`process_resident_memory_bytes`** on the manager's metrics endpoint, collected by the AWS rig as `manager_rss` ([docs](../docs/metrics.md#async-runtime)).
  - **`legion_log_dropped_lines_total{sink}`** and the worker's twin count the lines a full log buffer dropped ([calls #8](calls/todo-closeout-2026-11-10_calls.md) · [docs](../docs/observability.md)).
  - **`legion_runtime_spawn_delay_seconds` measures from the timer deadline**, so it sees a starved runtime ([docs](../docs/metrics.md#async-runtime)).
  - **`legion_scheduler_lazy_sample_fallback_total` and `schedulerSettings.lazy_version_sample` are gone**; both were inert.
  - **`liveness.batchWindow` is gone**; it was parsed and never read ([calls #4](calls/todo-closeout-2026-11-10_calls.md)).
- **UI**
  - **Instance tables show a TAP sandbox's guest address and bridge.**
  - **The job Logs tab has a Removed switch** (`include_removed`) ([docs](../docs/logging.md#logs-of-removed-allocations)).
- **Performance, tests and docs**
  - **Content digests are hashed with `ring`**, which uses the SHA extensions on aarch64 too ([calls #9](calls/todo-closeout-2026-11-10_calls.md)).
  - **`raft_seam` reads `storage/store/*.rs`**, so a mutation added in a submodule must be classified.
  - **The service-graph fold has a unit test** (`fold_edges`: bucket per minute, the 15-minute cap, unattributed samples, idle aging).
  - **Two timing flakes fixed**: `typical_rules_are_cheap_test` takes its fastest run, and the HTTP plugin rewrite test sets a 1s plugin deadline.
  - **The installation guide's cloud-hypervisor and guest-kernel versions were checked upstream**: the kernel tag it named did not exist; it now names the pair the sandbox-boot rig ran, and the arm64 asset ([docs](../docs/installation.md)).
- **Removed, already done**: the `proto-breaking-ok` label exists; the rig's driver no longer reports `0.0` for a cell shorter than its warmup; Linux containers have had their own network namespace since gated became the default; the guest agent has had a vsock exec channel and a static musl build since sandbox exec Phase 1.
- **Removed, duplicates** (the surviving item carries the detail): the rig's `schedulingPolicy`, TAP and loopback-only sandboxes' upstreams, pressure re-landing on the evicting node, `exec` network namespaces and the overlay controller (both "Unplanned datapath"), wake on connect and diff snapshots, the session lifecycle rate, "prove Legion scales", the ingress's catalog polling, the https CA default, two task-manager-lock items (audit W3), and three Verify runs listed twice.
- **Removed, decided**: seventeen limitations and ideas, each with where its decision lives ([calls #10](calls/todo-closeout-2026-11-10_calls.md)).

## Spot checks, audits and security closures

- **Pre-launch spot check fixes (2026-10-11, #17)** — workers on other hosts (reachable bind, mTLS, CSR issuance); task lifecycle (batch finish, restart policies, staged health checks, stderr in failures); scheduler (DAG waits, static ports, partial placement); manager availability (worker failover, follower write forwarding, `transfer-leader`); Linux container hardening and install guide; CLI/diagnostics (`--json` everywhere, camelCase); `ListManagers` lists every raft member; cluster-wide CA pin after failover; CH VMM versions compared as semver ([findings](launch-spot-check-findings.md) · [plan](plans/launch-spot-check-fixes-plan.md) · [calls](calls/launch-spot-check-fixes-plan_calls.md))
- **AWS spot-check re-run fixes (2026-10-12)** — exponential reschedule backoff as a replicated `reschedule_hold` with events and `--force-reschedule`; slot instances return to their volume's node uncharged; per-address `~/.legion/tokens/`; SIGPIPE exit 141; `state query` errors as `InvalidArgument`; errors name CLI flags; MCP node attrs as objects; `job plan` previews age-gated preemption; instance `nodeId`; macOS `--run-worker` stops its containers on shutdown ([calls](calls/launch-spot-check-fixes-plan_calls.md))
- **Architecture audit (2026-10-09)** — worker W1–W6 (W3 partial), raft/store R1–R5 (R5 partial), scheduler SC1–SC6 (SC4 partial), manager lifecycle M1–M6 (M6 partial) ([audit](architecture-audit-2026-10-09.md) · [calls](calls/architecture-audit-2026-10-09_calls.md))
- **Sandbox spot check (2026-10-12)** — `node.pressure` events from a lane-less follower are forwarded and journaled ([calls](calls/durability-and-ops-execution-plan_calls.md) · [docs](../docs/event-stream.md))
- **Swappable seams (2026-10-14)** — one datapath `Attachment`, one listener/dialer (`netd::listen`), typed `netd::Netd` boundary, DNS on `hickory-proto`, egress proxy HTTP on hyper (pipelined requests judged), mesh upstreams honour `mesh_tls`, one object-storage layer (`crates/worker/src/objects/`), VIP blocks follow the node's OS ([plan](plans/swappable-seams-plan.md) · [calls](calls/swappable-seams-plan_calls.md) · [docs](../docs/codebase-conventions.md#datapath-conventions))
- **Pluggable cluster CA providers** — `clusterCa.provider = builtin | file | exec` with a Vault PKI helper and `legion net ca provider test` ([plan](plans/cluster-ca-provider-plan.md) · [calls](calls/cluster-ca-provider-plan_calls.md) · [docs](../docs/cluster-ca.md))
- **Security gap closure (2026-10-17)** — naming a KV key needs `kv:read`; root worker confined to the allocation (name charset, `unpack_in`, no link following); host-level covers exec checks, `file://`, `auth.nodeIdentity`; unforgeable identities (sub templates, audiences, bound boot tickets); allowlisted exec env; reused idempotency keys with a different request fail; fleet certs need a worker-type grant; `templateKv` re-renders can't read new keys ([plan](plans/security-gap-closure-plan.md) · [calls](calls/security-gap-closure-plan_calls.md) · [docs](../docs/auth-and-acls.md))
- **Launch blockers, round 2 (2026-11-12)** — OCI/S3 fetches pinned through a guarded loopback proxy (redirects judged); manager `artifacts.hostGuard` for pin lookups; `net policy test` judges each node's advertised guard (`hostGuardRules`); per-identity worker lock stops an unlocked identity's tasks (P1-5); unratified provisional claims recorded so a crash's restart stops them; capability-aware placement (`container.allowCaps contains`); workers fail over to `server.advertiseAddr`; removed-allocation history 7 days / 1000 per job; static-port starts wait for the port; 60s registry read timeout ([plan](plans/launch-blockers-round-2-plan.md) · [calls](calls/launch-blockers-round-2-plan_calls.md) · [docs](../docs/artifacts.md#what-the-host-guard-checks))
- **Launch blockers and security gaps (2026-11-08)** — node caches serve private images only to credentialed jobs (`image_source`); lanes dependency stall, rollout DAG freeze, missing sidecar, fractional-step deadlock; record-by-record worker state; host-guarded git/OCI/S3 fetches; `legion exec` joins gated namespaces; old runc kept off user-namespaced placements ([plan](plans/launch-blockers-and-security-gaps-plan.md) · [calls](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/artifacts.md#who-a-cached-image-is-served-to))
- **Worker-plane auth Phase 0: ACLs on by default** — first management token minted to `<dataDir>/bootstrap-token` and `~/.legion/token`; `BootstrapAcl` removed ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/auth-and-acls.md#quick-start))
- **Phase 1: mTLS worker plane** — per-RPC caller classes, CSR issuance, CA pin in join tokens, advertise addresses, CA rotation by cross-signing ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/architecture-managers-workers.md#the-worker-plane))
- **Phase 2: host guard in the datapath** — resolve once, judge every address, dial what was judged; `net policy test`, `net doctor` ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/network-handlers.md#the-host-guard))
- **Phase 3: Linux containers gated by default** — ingress twins, `127.0.0.53` resolver, `gated-default` warning ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/task-runtime-container.md#networking-gated-by-default-on-linux))
- **Phase 4: encrypted worker-to-manager channel** — workers dial TLS pinned to the CA, ticket JWKS over it ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/auth-and-acls.md#workers-reach-managers-over-tls))
- **Raft peers dial each other over TLS** — `raft.legion` key derived from the cluster secret ([plan](plans/security-gap-closure-plan.md) · [calls #1](calls/security-gap-closure-plan_calls.md) · [docs](../docs/auth-and-acls.md#managers-reach-each-other-over-tls))
- **Phase 5: kernel host guard** — `inet legion_host_guard` nft table per allocation leaf ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/network-handlers.md#the-host-guard-in-the-kernel))
- **Phase 6: container privilege floor** — per-allocation user namespaces, idmapped mounts, default seccomp, cgroup namespace, `security` block ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/task-runtime-container.md#security))
- **AWS spot-check fixes to the floor, and youki dropped** — traverse-only state dirs, runc 1.2+ default or crun only, refused at preflight otherwise ([calls 40, 41](calls/worker-plane-auth-plan_calls.md) · [docs](../docs/installation.md#runc-the-default))
- **Old runc not placed for user-namespaced tasks** — `container.idmappedMounts` attribute and implicit constraint ([plan](plans/launch-blockers-and-security-gaps-plan.md) · [calls #11](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/installation.md))
- **Phase 7: `exec` trust** — gated `exec` refuses root, `setgroups(0)`, `host` grant for host-level jobs ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/task-runtime-exec.md#trust))
- **Phase 8: reachability diagnostics** — `Worker/Probe`, `workerPlane` on the node, `WORKER_UNREACHABLE` ([plan](plans/worker-plane-auth-plan.md) · [docs](../docs/architecture-managers-workers.md#reachability))
- **Public docs' "cannot be bypassed" claims corrected** for gated `container` and `exec`
- **Artifact fetches judged by the host guard** — http(s) pinned with redirects; Git pinned, OCI/S3 endpoints judged ([calls #6](calls/security-gap-closure-plan_calls.md) · [calls #8](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/artifacts.md#what-the-host-guard-checks))
- **A DAG advances past its first step** — completion split into scheduler- and status-lane writes, lane-ordered batch outcomes, retried terminal and failure reports, sidecars wait with held batch steps (#38; [plan](plans/launch-blockers-and-security-gaps-plan.md) · [calls #5](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/task-supervision.md))
- **A failed changeset projection no longer stops the raft group** ([docs](../docs/state-store.md))
- **A resubmitted DAG's rollout progresses** — finished old allocations retire first; dependency release under rollout decided ([docs](../docs/rollouts.md#lifecycle-specific-behavior))
- **Store checkpoints resume on another node** — part fetch uses the node's `contentStore.auth`, node-independent `vmdev` links, gated-restore wait and network mode keyed off the allocation, placement prefers matching `contentStore` ([calls #63](calls/launch-spot-check-fixes-plan_calls.md) · [calls](calls/gated-restore-datapath-wait_calls.md) · [calls #9, #10](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/suspend-and-resume.md))
- **Worker state read record by record** — unreadable records' processes stopped, file kept aside; dead records kill their cgroup leaf; systemd `KillMode` guidance fixed ([calls #6](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/upgrades.md))
- **`move_count` per keyed instance** — `MoveVerdict::InstanceStopped`, `instance start` retries with a fresh budget ([calls #3](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/jobs-tasks-and-scheduling.md#manager-side-move-logic))
- **Worker-reported failures record `last_stop`** — new `failed` cause ([calls 73](calls/launch-spot-check-fixes-plan_calls.md) · [docs](../docs/graceful-stop.md#the-causes))
- **Disk-fit check reads the worker state dir's mount** ([calls 74](calls/launch-spot-check-fixes-plan_calls.md))
- **Followers publish a config snapshot** — forwarded to the control-lane leader ([calls 75](calls/launch-spot-check-fixes-plan_calls.md) · [docs](../docs/ui-architecture.md))
- **`legion_leader_forwards_total{lane,op,outcome}`** ([docs](../docs/metrics.md#raft))
- **Terminal batch reports persisted on the worker** ([plan](plans/launch-blockers-and-security-gaps-plan.md) · [docs](../docs/task-supervision.md#batch-tasks-and-real-completion))
- **`TerminatedAllocation.success = false` sent for `restart.policy = "never"`** ([calls #2](calls/launch-blockers-and-security-gaps-plan_calls.md) · [docs](../docs/task-supervision.md#the-restart-block))
- **`legion exec` into a gated `exec` task joins its netns** ([docs](../docs/exec-and-actions.md))
- **Image supplementary groups applied in Linux containers** ([calls 76](calls/launch-spot-check-fixes-plan_calls.md) · [docs](../docs/task-runtime-container.md))

## Cross-cutting

- **Fixes 2026-10-19** — `teardown.sh` refuses without a terminal ([calls](calls/aws-comparative-benchmark-plan_calls.md)); SIGTERM during manager startup handled; flaky sim-fleet and stop-signal tests fixed; `clippy --all-targets -D warnings` passes; yanked `validit`/`wide` pins bumped
- **Layer extraction applies directory modes at layer end** ([calls 77](calls/launch-spot-check-fixes-plan_calls.md))
- **Storage tests' fd limit documented** ([docs](../docs/testing-and-benchmarks.md#running-tests))
- **Baked operator AMI for the AWS rig** — `bake-operator-ami.sh` ([calls #29](calls/aws-comparative-benchmark-plan_calls.md))

## Developer workflow & CI

- **Legion is five crates in a chain** — `legion-proto` → `legion-common` → `legion-worker` → `legion-manager` → the binary; a clean build peaks at 5.0 GB where it took 9.0, `cargo test --all` builds on a 16 GB machine (it was OOM-killed at 13.5 GB), and `cargo check` after an edit takes 3–19 s where it took 30 ([plan](plans/workspace-crate-split-plan.md) · [calls](calls/workspace-crate-split-plan_calls.md) · [docs](../docs/codebase-conventions.md#crates))
- **Smaller shipping binary** — `dist` on linux/amd64 from 97.3 MB to 89.1 MB: packed relocations, Nickel and CEL built for size, `jsonschema` without its HTTP resolver (a remote `$ref` is no longer fetched), WAT parsing out of the binary; and which crates own the bytes, which answers "which dependencies own the rest" ([calls](calls/binary-size_calls.md) · [dev notes](development.md#what-else-keeps-the-binary-small))
- **Shipping binary measured on linux/amd64** — 97 MB with `--profile dist` (91.5 MB before the crate split); README, AGENTS.md, `Cargo.toml` and development.md corrected from the 26 MB macOS figure ([development.md](development.md#building-a-shipping-binary---profile-dist))
- **Smaller, faster test builds** — one integration binary (12.9 GB → 1.1 GB), scratch-root temp dirs, line-table debuginfo, faster migrations ([calls](calls/test-build-footprint_calls.md) · [docs](../docs/testing-and-benchmarks.md#test-layout) · [dev notes](development.md#keeping-builds-and-test-runs-small))
- **Raft-entry-count tests read the applied marker** — `Store::raft_applied_entries` instead of lagging openraft metrics
- **CI cache, cancellation, pinned protoc** — `rust-cache` saved from `main`, superseded runs cancelled, pinned checksummed protoc
- **Plugin lockfiles current and one shared plugin target dir** — five stale lockfiles regenerated from the root lock; `build.rs` builds every plugin into `target/plugin-build/shared/` and copies each one's artifacts to the per-plugin layout ([docs](../docs/plugin-system.md#build-behavior) · [calls](calls/todo-closeout-2026-11-11_calls.md))
- **CI rebuilds `tests/netplugins/*.wasm` and fails on drift** — pinned toolchain, committed lockfiles, remapped paths ([docs](../docs/network-plugins.md))
- **Test scratch roots are swept by pid**; the `.tmp*` leakers (rootfs-cache background fill, edge-start launch threads, net-plugin store) fixed ([docs](../docs/testing-and-benchmarks.md) · [calls 9](calls/test-build-footprint_calls.md))
- **The AWS rig forgets its sim nodes and runs each scenario's scheduling policy** — `legion node forget --force` on stop (`SIM_FORGET=0` skips it); the policy is rendered into the manager and recorded per cell ([README](../benchmarks/aws/README.md) · [calls 43–44](calls/aws-comparative-benchmark-plan_calls.md))

