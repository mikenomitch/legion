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

- [ ] **The planner's node universe truncates to one slice at rig scale.**
      The 2026-08-06 storm drill found ALL of a 25k-allocation `spread`
      workload on one quarter of a 5,000-node fleet, replacements packing
      the next quarter, nodes filled to exactly `maxAllocations`, and —
      decisively — every candidate in sampled `plan_submission` rows from
      one slice, across constraint classes. Eliminated: naming,
      availability, class/pool distribution, tie-breaks, cache
      invalidation, and the capture/projection path (pinned by
      `tests/spread_under_raft.rs` plus two scheduler unit tests). Does
      NOT reproduce locally up to 300 nodes with the raft engine on. Next:
      a fleet-scale repro using the worker-registration RPC at the sim's
      register rate. The observability half is done (2026-08-06):
      `legion_read_model_nodes{set="all"|"available"}` sits next to
      `legion_nodes` on the same refresh tick
      ([metrics.md](../docs/metrics.md#cluster-shape)), so a truncated
      planning universe is one query instead of a plan-submission
      sampling session. Findings:
      [aws-phase5-findings.md](aws-phase5-findings.md).

- [x] **Batch the control-lane job submit** (the "fuse the five control-lane
      rounds" item, right-sized). The two `clear_*` rounds were already
      empty changesets; what remained was one `handle_job_submit` commit
      (and `synchronous=FULL` fsync) per job. Now group-committed with
      versions derived through the batch transaction, which also fixes the
      code's own commented version race for concurrent submits. Measured
      locally: per-job quorum rounds ~3.0 -> ~1.5, unit-eval throughput at
      the knee 419.6 -> 476/s. Remaining singles at ~0.5/job are
      arrival-gaps, not a defect.
- [x] **JSONB columns carry NUMERIC affinity, so every JSON write parses as
      a number first — fixed** (2026-08-06,
      [jsonb-text-affinity-plan.md](plans/jsonb-text-affinity-plan.md),
      doc in [state-store.md](../docs/state-store.md#column-affinity)).
      All 46 JSON columns across 17 tables rebuilt to TEXT affinity in one
      migration that preserves column order/count/rowids exactly — the
      load-bearing property, since raft changesets bind columns by
      position and a reordering rebuild would corrupt a mixed-version
      cluster mid-rolling-upgrade. `job_config` (the one CASCADE parent)
      gets a rename dance, with `job_config_submission`,
      `job_kv_reference`, and `job_update` rebuilt byte-identically purely
      to re-point their FKs. Local A/B on the raw JSON insert path: the
      NUMERIC twin is ~5-10% slower; before/after Criterion cells in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md).
- [ ] **`STRING` and `TIMESTAMP` columns still carry NUMERIC affinity** —
      deliberately excluded from the fix above. Same wasted parse on
      every id/timestamp bind, but for these the conversion can
      *succeed*: numeric-looking ids and unix-seconds timestamps are
      stored as INTEGERs today, so a re-type changes what future writes
      store while existing rows stay INTEGER, and INTEGER never
      compares equal to TEXT — a value-rewriting migration with its own
      risk budget, not a type rename. See
      [jsonb-text-affinity-plan_calls.md](calls/jsonb-text-affinity-plan_calls.md)
      entry 2.

## In flight (open PRs)

| PR | What it lands | Notes |
| --- | --- | --- |
| [#162](https://github.com/mikenomitch/legion/pull/162) | Lanes hardening: worker-write forwarding, split-tolerant submits, adversarial fixes | Plan §8 + calls doc in [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md) |
| [#143](https://github.com/mikenomitch/legion/pull/143) | Note the node-level builtin resource override gap | Docs only; the fix itself is a todo below |
| [#84](https://github.com/mikenomitch/legion/pull/84) | Instrument DB hot paths, batch task-status updates | Old; rebase or close |
| [#108](https://github.com/mikenomitch/legion/pull/108) | Versioned job templates | Superseded by [#151](https://github.com/mikenomitch/legion/pull/151) (merged) — close |

---

## Scheduling & placement

- [ ] **Find what makes `apply` 27.9 ms/eval.** Phase 3 measured
      plan-apply conflicts at **0%** (from 31.9%) with throughput
      unchanged at 80.35/s, so the ceiling is the replicated write path,
      not planning. `apply` is 67% of plan time, and 41.42 ms/eval over 4
      eval threads permits ~580 alloc/sec against 80 measured — so evals
      are far more serialized than 4-way. Prime suspect: the scheduler
      lane's single `pipeline_loop` thread. Collect
      `legion_store_write_lock_wait_seconds` and the lane pipeline
      metrics next campaign. See
      [aws-phase3-findings.md](aws-phase3-findings.md).
- [ ] **Warmup is longer than a small cell.** `run-campaign.sh` defaults
      `WARMUP=30`; phase 3's 1,000-node cell placed 5,004/5,004 and
      reported `throughput_per_sec: 0.0` with `steady_window_placed: 0`
      because every placement fell inside the warmup. Scale warmup to
      cell size, or floor the steady window. This is why phase 3 has no
      1,000-node throughput number.
- [x] **Read-connection pool: fixed, measured, and ruled out as the
      ceiling.** Steal-on-contention, cached heartbeat task configs, and a new
      `legion_store_read_lock_wait_seconds` (read-connection wait
      previously had **no metric at all**). The hypothesis that this was
      the ceiling was **refuted three times**: phase 3 measured
      `task_fetch` at 0.65 ms at 5,000 nodes on the *unmodified* binary, so
      the 107x growth seen in the phase-2 archives was coupled to the
      conflict rate, not to fleet size; the plan path was traced in code and
      holds **no** read connection across a quorum round, an `.await`, or
      `build_plan` (the apply runs on the write connection inside
      `replicated()`), so the pool cannot serialize two planners; and the
      2026-08-05 eval-thread sweep ran every cell with **four** connections
      and still got 251.68 alloc/s at 32 eval threads, up 14.8% on 16. The
      "~4 effective planners" figure that started the hunt was a metrics
      error — `plan_phase_seconds{total}` times planning only, so Little's
      law over it counts evals in the planning *stage*, not busy workers.
      Use `eval_handle_seconds`. See
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      entry 5.
- [x] **Take the read-connection pool off `available_parallelism()`.**
      Now `evalThreads + 4`, capped at 16. **No throughput claim** — the
      sweep above shows four connections were adequate through 32 eval
      threads, so this is a correction to the sizing *argument* plus a
      bounded hedge for the higher `evalThreads` values the sweep says
      operators will want. Core count was the wrong basis: the readers are
      latency-bound (8% CPU at saturation, and a page-cache miss holds the
      mutex through a `pread`), holds are uneven
      (`cache_refresh_allocation_scopes` keeps one connection across ~44
      per-node queries after every apply, on every replica), and a pool
      sized at the core count saturates in lockstep with the tokio runtime
      by construction, since the heartbeat body reads inline on runtime
      threads.
      **A config knob was deliberately not added**: `evalThreads` is the
      concurrent-reader count we already have, and a `readConnections`
      field would be a number no operator can size from first principles.
      The 16 cap is a memory budget — `SQLITE_CACHE_SIZE_KIB` is 64 MiB per
      connection and the caches do not share, so the cap holds the pool's
      worst case at 1 GiB (unit-tested).
- [x] **Cut the raft round-trips per eval** —
      [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md).
      `Store::complete_and_claim_next` fuses the eval loop's terminal
      write with the following claim (4-5 rounds/eval → 3-4), and
      `Store::enqueue_evals` makes every fan-out one round instead of one
      per row (leadership regeneration, drained and lost nodes). **The
      AWS re-measurement is NOT run** — see the open item below.
- [x] **Measure the eval-path raft cuts at 5,000 nodes.** Done 2026-08-05,
      but only after the rig itself was fixed — see
      [aws-phase4-findings.md](aws-phase4-findings.md). The benchmark driver
      spawned one CLI process per job from a pool of 4 on a 2-vCPU box, which
      capped submission at ~36 jobs/sec and meant every earlier cell reported
      the DRIVER's rate. Results on a rig that reports
      `submission.driver_bound: false`: **177.27 alloc/sec** at 4 eval
      threads, **219.25** at 16, and **736.1** on `dense_fleet` (44
      allocations per eval), all at 5,000 nodes on three 4-vCPU managers.
- [x] **Reprojection no longer reads a node's whole allocation list**
      (2026-08-06, phase 5 of
      [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md)).
      The read model keeps a per-node `NodeUsage` aggregate beside each
      cached allocation list — the in-memory sibling of `node_alloc_usage`,
      same type, same equivalence tests — so `reproject_model_nodes` is a
      subtraction over resource dims instead of one JSON normalization per
      allocation on the node. The delta path adjusts the memo with exactly
      the rows it spliced; every wholesale list reload (scope refresh,
      scrub, drift fallback, node removal) drops the entry and the next
      reprojection rebuilds it from the list, so an absent entry costs
      precisely the old walk. The 44 per-scope epoch reads became one read
      connection + one cached statement. Local A/B
      (`bench_local_reprojection_by_fill`, 44 touched nodes): 414µs → 50µs
      at 10 allocations/node, 1.92ms → 74µs at 40 — the memo arm grows
      1.5x over a 4x fill change where the walk grew 4.6x. Numbers in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md);
      calls in
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §3. **The `dense_fleet` ladder re-run is the regression test** — see
      the open re-measure item below.
- [x] **Job intake group-commits on the scheduler and status lanes**
      (2026-08-06, phase "intake" of
      [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md),
      documented in
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md)).
      A submitted job cost THREE quorum rounds, one per lane, and all
      three sat on top of the AWS 5,000-node profile in lockstep with the
      submission rate: `enqueue_eval_after_delay` 19.1 applied/s,
      `handle_job_submit` 16.6/s, `seed_task_statuses` 16.6/s, against
      21.9 evals/s overall — while the eval loop's own
      `complete_evals_and_claim_next`, already batched, sat at 4.2/s.
      Concurrent submitters now coalesce their scheduler-lane enqueue and
      their status-lane `task_status` seed through the existing
      `coalesce.rs` group commit, each request fenced by its own
      savepoint so one bad submission fails alone. No timer, so a lone
      `job run` on an idle cluster is byte-for-byte the path it was.
      `schedulerSettings.job_intake_coalescing = false` takes both back
      out. Calls:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §2. **Unverified at scale** — see the re-measure item below.
    - [ ] **The control-lane `handle_job_submit` is NOT batched, on
          purpose.** It derives a config's next version from committed
          state read through a read connection, so two submits of the
          same job inside one uncommitted batch transaction would both
          compute version N+1 — the "there is a race condition here with
          the version" note in `handle_job_submit_apply`, promoted from
          rare to routine. The composite is also not one store call:
          `submit_rendered_job` interleaves `clear_job_stopped_status`,
          `replace_job_kv_references`, `seed_keyed_instances`,
          `previous_job_config_id` and rollout creation between the
          control write and the eval enqueue. Batching it means making
          the submit a pure function of its request first — a rewrite of
          the RPC, not a storage change. Intake therefore still costs one
          quorum round per submitted job.
    - [ ] **Re-measure intake with batching on.** Same cell as the
          measurement above (5,000 nodes, `dense_fleet` at 200,000
          allocations, 16 eval threads, 32 concurrent submitters).
          `legion_raft_ops_total{op="enqueue_evals_isolated"}` and
          `{op="seed_task_statuses_batch"}` should be well below the
          submission rate, and
          `legion_store_coalesce_batch_size{path="eval_enqueue"}`'s
          `le="1"` bucket well below its `_count`. **A local run cannot
          measure this** — `Store::replicated` short-circuits when raft
          is off, so the batched and unbatched forms execute identical
          code.
- [ ] **Batch the eval loop (claim N, plan N, apply N, complete N).** Now the
      whole remaining cost. With the wake herd fixed and health checks shown
      to be `outcome=empty`, the only meaningful quorum traffic left is
      `complete_and_claim_next` (44/s) plus ~20/s of plan applies. The
      amortization curve says the same thing from the other side: 6
      allocations per eval gives 219 alloc/sec, 44 gives 736, so per-eval
      overhead is what sets the rate.
    - [x] **claim N and complete N are done** — phase 4 of
          [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md),
          documented in
          [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md#batching).
          `Store::claim_evals` takes a batch in one round and
          `Store::complete_evals_and_claim_next` settles the batch while
          claiming the next, so the 44/s `complete_and_claim_next` traffic
          becomes 44/N. The batch is a fair share of the live queue
          (`pending / evalThreads`, capped at 16), never lingers to fill,
          and has no config knob. **Unverified at scale**, see below.
    - [ ] **plan N and apply N are NOT done, and were declined on purpose.**
          Handlers write as they go across lanes rather than returning a
          plan, so committing N plans together means restructuring every
          handler to be a pure function of a snapshot — a scheduler rewrite,
          not a loop change ([call 2](calls/eval-path-raft-cost-plan_calls.md)).
          It also overlaps the D5 group commit in `coalesce.rs`, which
          already batches *concurrent* applies and gets *more* to batch as
          the claim/complete rounds get cheaper. This is the same target as
          "the plan-apply commit is serialized" in
          [aws-phase4-findings.md](aws-phase4-findings.md) (`apply` at 354 ms
          of a 381 ms total at 100,000 allocations) and should be costed out
          against that, not against the eval loop.
    - [ ] **Re-measure with batching on.** Rerun cells G/H from
          [aws-phase4-findings.md](aws-phase4-findings.md). Expect
          `legion_raft_ops_total{op="complete_evals_and_claim_next"}` well
          below the eval rate (it was 44/s at 44 evals/s), and the 3.4x
          `web_fleet` / `dense_fleet` gap to narrow. Divide that op's rate by
          `legion_evals_processed_total` for evals-per-round; if it stays at
          1, the queue never gets deeper than `evalThreads` and the
          bottleneck is upstream of the eval loop. **A local run cannot
          measure any of this** — `Store::replicated` short-circuits when
          raft is off, so a batched claim of N and N single claims execute
          the same SQL in the same process.
- [x] **Fix the eval-notifier thundering herd.** `notify_waiters()` woke
      every parked eval worker on ANY `eval` row change, and each answered
      with a `claim_eval` — a scheduler-lane quorum round whether or not it
      found work. Every eval crosses that path three times. Measured at
      16,678 scheduler-lane raft ops at 4 threads against 47,096 at 16, for
      identical work, with throughput FALLING as threads rose. Now wakes one
      worker per newly-claimable eval; empty claims are down to 0.3/sec and
      16 threads beat 4 by 23.7%. Adds
      `legion_eval_claim_total{outcome}`, whose absence is what hid it, and
      an `op` label on `legion_raft_ops_total`.
- [x] **Stop re-reading every allocation on a node inside the plan-apply
      commit.** `free_pool_for_node_tx` rebuilt a node's free pool by
      selecting every allocation row on it and parsing every `resources`
      blob, inside the serialized commit — so the cost of committing a plan
      scaled with cluster fill while the work per plan did not (~440 rows per
      eval at 50k allocations, ~1,760 at 200k, for the same ~44 placements;
      throughput 736.1 → 456.6 → 272.9 alloc/s). Now cached per node in
      `node_alloc_usage` and validated on every read against a fingerprint of
      the live rows (node allocation epoch + row count + id sum), so the
      cache can never inflate free capacity — the worst a stale row does is
      cost one rebuild. A Fresh plan's own allocations are read once per
      plan instead of re-filtered per node. Local A/B on the rebuild: 15.6
      → 8.6 µs per pool at 10 allocations/node, 37.8 → 9.4 µs at 40 (the
      old path grows 2.4x over that fill change, the new one 1.08x). See
      [aws-phase4-findings.md](aws-phase4-findings.md),
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md) and
      [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md#the-per-node-usage-cache).
      **Unmeasured end to end** — see the open item below.
- [ ] **Re-measure the `dense_fleet` fill ladder.** The 2026-08-05 re-run
      measured a uniform ~2.4x throughput lift with the slope UNCHANGED
      (~1.65x per doubling) — which ruled out the per-node usage cache and
      the delta refresh as the dominant term and pointed at
      `reproject_model_nodes`. That term is now gone too (the usage memo,
      2026-08-06 above), so re-run the 50k/100k/200k ladder once more:
      if the slope still does not flatten, the mechanism is off the
      store/refresh path entirely and the next measurement is
      control-plane CPU at saturation
      ([aws-phase4-findings.md](aws-phase4-findings.md) open item 4).
- [ ] **`node_alloc_usage` never sheds rows.** Node removal is a
      control-lane op and may not delete a scheduler-lane row, so a
      churning fleet accretes one inert ~100-byte row per departed node,
      in the table, the raft snapshots, and every plan changeset that
      republishes it. Harmless at rig scale; needs a scheduler-lane sweep
      (the `prune_node_health_orphans` shape) if node churn becomes a
      real workload. Same wart class: each applied plan's changeset now
      carries one `node_alloc_usage` upsert per touched node (~44 ×
      ~100 B on `dense_fleet`) — the write-side cost of the read-side
      cache, unmeasured on the rig.
- [ ] **The read-pool cap re-couples with the runtime on >12-core
      hosts.** The pool is `evalThreads + 4` capped at 16 for the 1 GiB
      page-cache budget, but `evalThreads` still defaults to
      `available_parallelism()` — so a 32-core host runs 32 eval workers
      against 16 read connections plus inline heartbeat reads, which is
      the saturating-in-lockstep shape the sizing change argued against.
      Resolves itself when the `evalThreads` default is decided
      ([eval-thread-default-plan.md](plans/eval-thread-default-plan.md));
      until then it is a documented corner, not a defect.
- [ ] **Decide the `evalThreads` default.** It is `available_parallelism()`,
      which is the 4 that placed slowest in the 2026-08-05 sweep at 5,000
      nodes: 177.27 alloc/s at 4 threads, 208.83 at 8, 219.25 at 16,
      **251.68 at 32**, and >= 263.08 at 64 where the LOAD GENERATOR became
      the constraint. An eval worker parks on quorum round trips rather than
      computing (8% CPU at saturation), so core count is the wrong basis —
      but the curve never turned over, so the right value is unknown.
      **Planned, not built** —
      [eval-thread-default-plan.md](plans/eval-thread-default-plan.md), which
      deliberately picks no number.
  - [x] **Fix the load generator before sweeping again (Phase 0a, the
        gate)** (2026-08-05). `legion-loadgen`
        (`benchmarks/loadgen/main.rs`, `cargo build --features loadgen`)
        submits a whole workload from one process over a pool of long-lived
        gRPC channels, with no per-job process spawn or dial. Measured on
        one macOS box, 5,001 web_fleet jobs, the two paths alternated back
        to back: **255-260 jobs/sec** on the CLI loop against **1,107-1,492
        jobs/sec** native on an idle box, and 105/108 against 741/564 on a
        loaded one — **5-7x either way**. End to end at 600 nodes,
        submission of 2,001 jobs went from 8.54s to 2.64s and the placement
        tail after the last submit went from 2.1s to 8.1s, so the scheduler
        is now the constraint by a fourfold wider margin.
        `driver.py --submitter {auto,native,cli}` selects it and every cell
        archives which path it took. The eval-thread sweep above can now be
        re-run past 32. Calls:
        [aws-comparative-benchmark-plan_calls.md](calls/aws-comparative-benchmark-plan_calls.md)
        §23-24.
  - [ ] **Native submitters for Nomad and K8s, before the next
        cross-system table.** `legion-loadgen` is Legion-only, so a cell run
        with `--submitter native` is comparable to another Legion cell and
        to nothing else; Nomad and K8s still spawn a CLI process per job.
        Until they have equivalents (Nomad's HTTP API and the apiserver
        REST API, each over one keep-alive session), every cross-system
        comparison must force `LEGION_SUBMITTER=cli` on all three. The rule
        is in `benchmarks/aws/README.md` § Methodology and the path taken is
        recorded per cell as `submitter` in `metrics.json` and
        `fairness-record.json`.
  - [ ] **Burst submission costs measured placement throughput — find out
        why.** The same 600-node/12,006-allocation cell placed 1,329
        alloc/sec when the CLI loop paced submission over 8.5s and 1,126
        alloc/sec when the native submitter delivered the same jobs in
        2.6s: about 15% lost to receiving the work faster. That is the
        first look the rig has ever had at Legion under a submission burst,
        and the obvious suspect is plan-apply conflict
        ([plan-apply-conflict-reduction-plan.md](plans/plan-apply-conflict-reduction-plan.md)).
        Measure `legion_plan_apply_total{outcome}` across a paced/burst
        pair before drawing a conclusion.
  - [ ] `configs/manager.ncl` pins `evalThreads = 4` explicitly, so it has
        to move with the default.
- [x] **The eval loop no longer blocks tokio worker threads on quorum
      rounds** (2026-08-06). The batch claim and the fused settle round
      both run under `spawn_blocking`, like the handler always did, so a
      parked quorum round trip never parks a runtime worker — the
      prerequisite for sweeping past 64 eval threads. The settle round
      also retries (bounded, `EVAL_SETTLE_MAX_ATTEMPTS`, fail-fast on
      `NotRaftLeader`) before abandoning a batch: on a STABLE leader
      nothing else ever recovers an orphaned batch, because
      `requeue_in_flight_evals` runs only at startup and on leadership
      acquisition. Calls:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §13. The status checker's `mark_nodes_as_lost`/`recheck_lost_nodes`
      moved onto the blocking pool in the same change.
- [ ] **A stable leader still never re-sweeps in-flight evals.** The
      settle retry above shrinks the window; the complete fix is a
      periodic `requeue_in_flight_evals` with an age threshold safely
      above the worst-case handler time (conflict backoffs can hold an
      eval for hundreds of ms × retries), so it cannot race a live
      handler. Needs that bound stated before it can ship. Related: the
      terminal write has no `status = 'in_flight'` guard, so a worker
      that survives a double leadership flap can settle an eval a new
      leader already requeued and re-claimed — narrow, pre-existing, and
      widened slightly by batching (audit finding, 2026-08-06).
- [ ] **`run_dispatch_future` has no `Send` bound** (`dispatcher.rs:408`),
      so holding a `MutexGuard` across an `.await` inside
      `dispatch_allocations` would **compile**. It does not happen today —
      the per-allocation loop is entirely synchronous and the awaits come
      after it — but this is the one spot on the plan path where the type
      system is not enforcing the invariant. Latent, not a live bug.

      which is the 4 that just lost to 16 by 23.7%. An eval worker parks on
      quorum round trips rather than computing (8% CPU at saturation), so
      core count is the wrong basis. A changed default needs a plan under
      [plans/README.md](plans/README.md)'s rule.
- [x] **Stop the coalescer polling for its handoff.** A submitter that lost
      the commit lock slept 1 ms on its reply channel and re-contended, so
      the lock sat free until some waiter's next tick and every waiter
      burned a wake-up per millisecond of every commit (phase 4: 347 ms
      follower wait against a 138 ms commit). The wait is now one
      `Mutex` + `Condvar`, woken exactly when the commit it queued behind
      ends. It also fixes a wedge: the old `commit_lock` was held across
      the batch runner, so a panic there **poisoned** it and every later
      submitter spun forever. `legion_store_coalesce_wait_rounds_total` now
      counts extra commits sat through, not poll ticks, and should read 0.
      Judgement calls, including why the leader deliberately does *not* run
      a second batch:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md) §3.
- [ ] **The plan-apply commit is still serialized, and batch size is the
      arrival rate.** The handoff fix above is worth milliseconds per
      commit; the 138 ms commit itself is untouched, and `batch_size` 1.32
      at 16 eval threads means only ~1.3 plan applies arrive per commit
      period — there is nothing queued to batch. No timer-free coalescer
      change can raise that (and a timer would tax the lone submitter), so
      the levers are a cheaper commit or more concurrent demand reaching
      the coalescer. Find out which by measuring how many threads are
      actually inside `submit_and_apply_plan` at once —
      [aws-phase4-findings.md](aws-phase4-findings.md) item 2.
- [x] **Skip the health-check write when nothing changed.** Shipped in
      #191 (`f8dbd68`) but never checked off here: `update_node_health_checks`
      compares against the current row on a READ connection first and
      returns before the write path when nothing changed — no savepoint,
      no write connection, no replicated-op machinery. The 2026-08-06
      audit re-verified it end to end. Residual: a *forwarded* health
      check (leadership-rejection path only) skips the compare on the
      origin and runs it on the target.
- [ ] **Re-measure the plan-apply fix on AWS.** Phase 4 of
      [plan-apply-conflict-reduction-plan.md](plans/plan-apply-conflict-reduction-plan.md).
      The two-pass bind is shipped and unit-proven but **unmeasured at
      scale** — every number in
      [aws-phase2-findings.md](aws-phase2-findings.md) is still the
      pre-change baseline. Re-run its five-row table; that table is the
      regression test. Success is `plan_apply_total{rejected}` under 5%
      (from 73.6%) and throughput that RISES with eval threads instead of
      falling. Watch `legion_scheduler_plan_bind_pass{pass="revalidated"}`
      — it counts the conflicts that used to be rejections, and should be
      high under bin_pack.
    - [ ] Run `spread` as well as `bin_pack`. Every phase-2 number is
          bin_pack (the manager template sets it and `translate.py` drops
          the scenario's `spread`), which is the maximally correlated
          policy and therefore the worst case for this conflict. It is
          also where the fix's regression risk lives: pass 2 improves
          packing under bin_pack but slightly loosens spread. Make the
          rig config and the scenario agree first.
    - [ ] Answer the two open questions the campaign could not:
          whether `build_plan` stays flat above 5k nodes (the whole scale
          claim rests on it, and it has never been tested there), and what
          made `task_fetch` grow 33x.
- [ ] **Late-binding plan apply, Phase 4 backlog** (each optional, gated on
      the fallback-rank / `plan_apply_total{rejected}` metrics the shipped
      phases added): capacity-aware candidate skipping, per-component partial
      commit + eval refill, striped disjoint candidate lists for identical
      replicas, and a serialized/batched apply lane for physically-full
      pools — [plan §8](plans/late-binding-plan-apply-plan.md). The shipped
      phases are in Done below.
      **The gating metric now has a number.** AWS rig 2026-08-04, 5,000
      allocations over 1,000 nodes with concurrent submission:
      `plan_apply_total{outcome="rejected"}` 472 against
      `{outcome="applied"}` 2503 — a **15.9% rejection rate**, each one a
      full eval retry. The rejections read "no candidate for task N could
      bind: all 1 candidate(s) were stale or invalid", while
      `legion_scheduler_index_candidates{index="fit"}` shows the index
      handing placement 33-64 candidates per task. So the index is not
      candidate-starved; the plan carries one candidate per task and loses
      it to a concurrent apply. That points straight at striped disjoint
      candidate lists as the highest-value item of the four.
- [ ] **Cut the raft round trips per eval, or widen the lane pipeline.**
      Same run: the scheduler lane took 67,165 raft ops for ~10,700 evals
      (~6 per eval — `claim_eval`, the plan apply, `complete_eval_with_event`,
      plus enqueues, each its own replicated proposal), and accumulated
      **206 seconds of `legion_raft_pipeline_wait_seconds`** at 3.07ms/op
      against the status lane's 0.09ms/op on comparable volume.
      `propose_apply` is a flat ~1.5ms on every lane, so this is queueing
      behind the lane's single `pipeline_loop` thread, not replication
      cost. Net effect: `legion_scheduler_plan_phase_seconds{phase="total"}`
      averages 9.32ms per eval (of which `apply` is 6.74ms, 72%) while
      achieved throughput implies ~75ms of wall time per eval per thread.
      The scheduling work itself is not the problem — `build_plan` is
      1.72ms and `available_nodes` 0.03ms over 1,000 nodes. Options:
      batch the eval lifecycle ops into fewer proposals, or give the
      pipeline more than one thread per lane. Wants measurement of which
      of the ~6 ops are actually separable before any design.
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
- [ ] Edge boot follow-ups — the feature shipped (phases 1–4;
      [plan](plans/edge-boot-plan.md) · [docs](../docs/edge-boot.md) ·
      [calls](calls/edge-boot-plan_calls.md)); these are what it left open.
  - [ ] **Phase 0's measurement matrix was never run.** The start-path
        breakdown (submit → eval claim → plan → apply → dispatch → prepare →
        boot → first byte) in both persistence modes, cold and warm image,
        single- and cross-region, belongs in
        `benchmarks/scheduler/README.md` per the AGENTS.md before/after rule.
        Until it exists the plan's open question — is `boot` or `stage` the
        right recommended default — stays open, and the public doc recommends
        neither. Needs a real multi-node raft cluster and a real registry; see
        [calls #18](calls/edge-boot-plan_calls.md).
  - [ ] A per-interface default-drop filter for the `sandbox` `boot` gate.
        Today Legion only holds the TAP link down, which is the 95% mechanism;
        a filter is stronger on hosts where link state alone is not enough, but
        owning firewall rules means owning backend choice, conflicts with the
        operator's own rules, and cleanup after a crash
        ([calls #10](calls/edge-boot-plan_calls.md)).
  - [ ] Lift the "no gates above `stage` for tasks with declared ports or
        connect upstreams" restriction. Both are assigned inside the apply, so
        this needs speculative port assignment reconciled at commit
        ([calls #13](calls/edge-boot-plan_calls.md)).
  - [ ] Speculative attach for volume drivers that can genuinely fence a
        concurrent attach, via a capability advertised in the volume contract.
        Today every non-node-local driver downgrades the gate to `stage`.
  - [ ] Island mode: bounded survival of unratified work across a
        control-plane outage, with its own consistency argument (plan phase 5).
  - [ ] Ticket pre-distribution and bulk minting for warm fleets, and
        ratification batching for a node starting many claims per second
        (plan phase 5).
  - [ ] A pre-warm push for the worker spec cache. A node today caches a task
        config once it has been dispatched an allocation of it, so an edge pool
        must run at least one allocation of a job before a ticket for it can be
        redeemed there. An explicit push (`legion job prewarm --pool edge-eu`)
        would remove that ordering constraint and compose with the
        [artifact cache and pre-warm plan](plans/artifact-cache-and-prewarm-plan.md).
  - [ ] Replace the string matching in `manager::edge_boot::classify` with a
        typed error out of the plan apply, if a second consumer of the
        rejection reason ever appears ([calls #17](calls/edge-boot-plan_calls.md)).
  - [ ] Speculative dispatch and late-binding plan apply do not compose yet.
        A claim is dispatched to `candidates[0]`, the planner's first choice;
        when the apply walk falls back to a later candidate the winning node
        is not the one holding the claim, so the claim goes unratified and
        holds its staged work until the lease expires. Correct but wasteful,
        and invisible except as a gap between
        `legion_provisional_claims_total` and the ratified count. Options:
        dispatch to the top *n* candidates and refuse the losers, or hand the
        apply walk the claim set so it prefers a node already holding one.
        Needs the fallback-rank metric to say how often it actually bites.
- [ ] Worker groups + taint/toleration equivalent. Node pools exist as an ACL
      and config concept but nothing scheduling-side consumes them.
- [ ] Preemption follow-ups — the feature shipped
      ([plan](plans/preemption-and-eviction-plan.md) ·
      [docs](../docs/preemption-and-eviction.md)); these are what it left open.
  - [ ] Decide whether the per-pool preemption toggle should be ACL-adjacent
        rather than manager config. `disabledPools` lives in
        `schedulerSettings`, so a pool-scoped operator cannot exempt their own
        pool without cluster-config access — even though `pools` already
        exists as an ACL selector. Named as a decision, not an oversight
        ([plan §Open Questions 6](plans/preemption-and-eviction-plan.md)).
  - [ ] Victim metadata is loaded by a store query on the preemption path
        rather than carried on the in-memory `Allocation` rows, so a plan that
        preempts pays a round trip inside planning. Deliberate — widening
        `Allocation` for four fields used by one code path would touch the
        read model, the caches, the raft changesets and the wire — but if
        `legion_scheduler_plan_phase_seconds{phase="build_plan"}` grows on a
        preempting cluster, that is the fix
        ([call 18](calls/preemption-and-eviction-plan_calls.md)).
  - [ ] Preemption for a multi-task group being placed fresh preempts for its
        first member; if a later member then cannot fit, the whole plan fails
        and commits nothing. Safe, but a group that *could* have been placed
        by evicting more is not.
  - [ ] `legion node drain` rate limiting via the disruption vocabulary was a
        Phase 4 candidate and was **evaluated and not taken**: drain has its
        own deadline-bounded semantics, and giving it a budget re-opens the
        cluster-wedge risk `disruption.minAvailable` is deliberately scoped to
        avoid ([call 10](calls/preemption-and-eviction-plan_calls.md)).
        Revisit only with a concrete operator request.
  - [ ] Revisit fair share only with real multi-tenant demand. Bands +
        per-namespace band caps (the `bands` ACL selector) are the 80/20;
        hierarchical queues stay a non-goal.
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
      The two costs D5 left on the table closed 2026-08-03 (phase 4 of
      [plan-path-scale-levers](plans/plan-path-scale-levers-plan.md)):
      a plan batch is now ONE transaction on every backend — one fsync
      locally instead of one per plan — and its post-apply `task_status`
      projection is one status-lane write for the batch instead of one
      per plan. Further wins want profiling, not this list.
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
      Phase 4 (2026-08-03) closed what D5 deferred: the batch is one
      transaction on every backend (local commit amortization, reversing
      judgement call #6) and its `task_status` projection is batched onto
      the status lane once per group commit. D2 (cross-manager
      scheduling) and D3 (in-memory state) remain recorded keep/defer
      decisions; D8's architecture choice still waits on AWS-test data.
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
  - [x] Unattended campaigns (2026-08-02): `scripts/remote-campaign.sh`
        drives a campaign from the loadgen box under tmux, so a run
        survives a sleeping laptop, a dropped session, and a changing
        egress IP (`admin_cidr` pins ingress to one CIDR). Scripts pick
        public vs private addresses from `BENCH_ON_RIG`; topology comes
        from a rig manifest instead of terraform state; scale steps use
        the ASG API under a narrow `orchestrator` IAM role; the spot
        fleet is released on any campaign exit (`--keep-fleet` opts
        out). See the README's "Unattended runs" and calls entries
        12-14.
  - [x] Quota headroom + durable results (2026-08-02):
        `var.worker_on_demand_percentage` splits the sim fleet across the
        spot and on-demand vCPU quotas (separate AWS limits) so neither
        has to cover a whole step; `experiments/packing-density.sh`
        measures the real nodes-per-instance ceiling the 1,250 estimate
        stands in for, feeding `run-campaign.sh --nodes-per-instance`;
        `var.archive_bucket` mirrors every cell to a bucket terraform
        does not manage (write, not delete), so results outlive both the
        VM and `teardown.sh`; `scripts/campaign-status.sh` publishes
        run state + console log to S3 with a 60s heartbeat, so a watcher
        with only HTTPS and credentials — no SSH, no repo, no terraform
        state — can check on an unattended campaign and tell "stalled"
        from "quiet". README "Watching from somewhere else" and "Keeping
        results after the rig is gone"; calls entries 11 and 15-17.
  - [x] Bootstrap the operator box the campaign is driven from
        (`scripts/bootstrap-operator-box.sh`, `scripts/phase1.sh`; calls
        entries 18-19). One command takes an AWS key to a running
        phase 1: builds an Ubuntu x86_64 box outside the rig's terraform
        (it *runs* that terraform, and must outlive `teardown.sh`, which
        destroys the state), gives it an Elastic IP so `admin_cidr` stays
        true, carries an IAM instance role instead of a copied key,
        writes `terraform.tfvars` from the box's own facts, and runs the
        six phase-1 steps under tmux with per-step checkpoints.
        `stop`/`start` for between sessions (~$268/mo running vs ~$12/mo
        stopped); `repair-iam` re-applies the role in place, which is the
        only remedy available when the box holds the sole copy of the
        terraform state.
  - [ ] Phase 1 exit on real AWS: `bringup.sh` + 1k-node
        `run-campaign.sh --system legion-a --smoke`. **In progress on a
        real account (2026-08-02).** Step 1 (local-smoke) passes on a
        real box. Bugs found and fixed while getting there, all in
        `bootstrap-operator-box.sh` unless noted: no default VPC in the
        account; `Subnets[0]` landing in an AZ that does not offer the
        instance type (us-east-1e has no c7i); `RunInstances` retrying
        every failure and blaming IAM propagation; `cargo` absent from a
        non-interactive SSH PATH (rustup writes to `~/.bashrc`, which
        Ubuntu skips for non-interactive shells) — fixed in `phase1.sh`
        and in cloud-init, and documented in `development.md`; `clang` +
        `libclang-dev` missing for `rusqlite`'s `session` feature forcing
        libsqlite3-sys through bindgen; `push`'s `rsync --delete`
        deleting `terraform.tfvars` and — far worse — `terraform.tfstate`,
        which would have orphaned the whole rig; the operator role
        missing `iam:TagInstanceProfile` while terraform tags every
        resource via `default_tags`.
        Unverified against real AWS, and first in line to shake out:
        the `orchestrator` role's `SetDesiredCapacity` path, the
        loadgen box's private-IP reachability to CP + spot workers, the
        tmux launch path in `remote-campaign.sh`, and the S3 status
        stream + archive-bucket mirror (both written and unit-checked,
        neither exercised against a real bucket). Run
        `packing-density.sh` during this step: it needs the same
        standing rig and retires the 1,250 assumption before any cell
        is sized from it.
  - [x] **Manager heartbeat throughput: ~7x, by taking the store call off
        the tokio runtime.** `Manager::heartbeat` called the blocking
        `Store::update_node_heartbeat` inline on a worker thread. That
        parked the thread for a whole fsync-bound raft commit, and — the
        part that actually mattered — left every other heartbeat stuck in
        the gRPC layer where the coalescer could not see it, so batches
        stayed at size 1 and `HEARTBEAT_BATCH_MAX = 128` was never
        approached. Now offloaded via `spawn_blocking`, so the queue
        builds, batches form, and one fsync amortizes across many beats.

        Measured on gp3 EBS (`experiments/heartbeat-commit-probe.sh`,
        3 voters + 1000 sim nodes on one box, 2s cadence = 500/sec needed):

        | worker threads | before | after |
        | --- | --- | --- |
        | 1 | 48.3/sec (9%) | **349.8/sec (69%)** |
        | 4 | 60.5/sec (12%) | **422.6/sec (84%)** |

        `cargo test --all --locked`: 1405 passed, 0 failed.

        Note the before-numbers barely move with thread count (48 -> 60):
        adding threads does not help when every thread is blocked, which
        is why "raise worker_threads" was never the fix.

        Still open, tracked separately: the remaining 16% gap at 4 threads,
        which is the single `pipeline_loop` OS thread per lane plus the
        fsync itself — see the liveness plan below for the architectural
        answer, and note the control plane now runs on local NVMe
        (`m6id.xlarge`), which removes most of the fsync cost.
  - [ ] ~~The manager sustains only ~80 heartbeat RPCs/sec regardless of
        fleet size or shape.** Measured on the first real AWS rig
        (2026-08-03; 3x m7i.xlarge, 3-voter lanes, sim fleets on
        c7i.2xlarge). This is a Legion-side ceiling, not a simulator one:

        | fleet layout | total heartbeats/sec |
        | --- | --- |
        | 250 nodes on 1 sim host | 80 |
        | 250 nodes across 2 sim hosts (125 each) | 80.3 |

        Doubling the simulator hardware bought nothing — per-host
        throughput *fell* from ~60/s to ~40/s so the total stayed pinned.
        A shared downstream resource is the constraint, and it is the
        manager. Supporting evidence:
        - `legion.Manager/Heartbeat` handler p99 **50ms**, which is slow
          for a counter bump plus a node-timestamp update, and at ~80/s
          implies only ~4 requests in flight
        - manager CPU **19-23%** of 4 vCPU, sim hosts **0.2%** — nothing
          is CPU-bound, so this is latency/serialization, not work
        - `legion_worker_heartbeats_total` is 100% `outcome="ok"` — no
          errors, no retries
        - the fleet degrades smoothly with density: 125 nodes/host holds
          97% cadence at 0.23s p99; 250 holds 64% at 2.49s

        Prime suspect: `Manager::heartbeat` calls
        `store.update_node_heartbeat`, which on a raft store commits
        through the status lane. A per-heartbeat raft commit would be
        latency-bound exactly like this. `docs/multi-raft.md` describes
        heartbeat group commit — worth checking whether batching actually
        engages at these arrival rates, or whether each heartbeat pays a
        full quorum round trip.

        **The instrumentation gap is closed** (2026-08-02, Done section):
        the next run of this rig can answer the open questions from
        `/metrics` alone — whether batching engages
        (`legion_store_coalesce_batch_size` le="1" vs `_count`), whether
        submitters wait or commit (`_wait_seconds{role}` vs
        `_commit_seconds`), whether the per-lane raft pipeline is the queue
        (`legion_raft_pipeline_wait_seconds` / `_depth`), and whether the
        tokio runtime is saturated by the blocking call
        (`legion_grpc_in_flight` vs `legion_runtime_worker_threads`, and
        `legion_runtime_spawn_delay_seconds`, which explains the 30ms
        `HealthCheck` p99 if it is). Read order:
        [docs/metrics.md](../docs/metrics.md) § "Diagnosing a manager
        throughput ceiling".

        **Consequences.** At ~80 hb/s a single manager supports roughly
        160 nodes at the 2s default cadence before liveness degrades, so
        the campaign cannot reach its 1k-10k scale steps without either
        fixing this or raising `heartbeatSeconds` (which changes what is
        being measured and must be recorded in the fairness record).
        Every phase 2-5 campaign is blocked on understanding it — a run
        at 1k+ nodes today measures this ceiling, not the scheduler.

        **Not the cause, ruled out by experiment:** the simulator's
        gRPC channel pool. `SIM_NODES_PER_CHANNEL` was changed 256 -> 64
        (4x the connections at 250 nodes) and rebuilt; cadence went 66%
        -> 64%, i.e. no change. That commit is reverted.

        **Design answer, SHIPPED 2026-08-03** (see Done):
        [heartbeat-cadence-at-scale-plan.md](plans/heartbeat-cadence-at-scale-plan.md)
        — sizing formula + freshness metric, a runtime warning,
        per-node lost windows derived from each node's cadence, and
        opt-in manager-dictated adaptive cadence. It does not raise the
        ~80/s ceiling; it stops the ceiling from marking healthy nodes
        lost, and makes the cadence a run was measured at part of the
        fairness record. Raising the ceiling itself is D8, now planned in
        [liveness-off-the-raft-path-plan.md](plans/liveness-off-the-raft-path-plan.md)
        ([calls](calls/liveness-off-the-raft-path-plan_calls.md)) — the
        AWS probe settled the D8 question the architecture review was
        waiting on (that plan has now SHIPPED too): gp3 48-86 hb/s vs local NVMe ~2000 hb/s on identical
        code, so the cost is the per-commit fsync. The `spawn_blocking`
        offload above collected most of the cheap amortization (gp3 to
        422/s, 84% of cadence at 1000 nodes), so the plan's phase 1 is now
        a measurement — attribute the residual 16% between the fsync and
        the single `pipeline_loop` thread per lane — and phases 2-4 take
        steady-state liveness off raft entirely, replicating only status
        transitions. Still needed because 84% of cadence at 1000 nodes is
        6x short of the campaign's 5k step and 12x short of its 10k step,
        and because `R` remains a property of the disk.
  - [ ] **Re-measure the AWS campaign now that heartbeats are off the raft
        path.** Neither shipped plan's scale runs have been executed — they
        need the shared rig. Owed:
        `benchmarks/aws/experiments/heartbeat-commit-probe.sh` on **gp3** at
        250/1000/2500 nodes (the acceptance number is the gp3-to-NVMe ratio
        approaching 1.0), `packing-density.sh`, all twelve `run-campaign.sh`
        cells, `storm.sh`, `soak-24h.sh` (raft log growth and snapshot
        cadence change materially when heartbeats stop producing log
        entries — possibly a larger win than the throughput), and
        `failover-drill.sh` extended with "zero nodes marked lost across the
        drill". Also the cadence plan's phase-4 reproduction: 1000 simulated
        nodes at `R ~= 80`, adaptive off reproducing the collapse and
        adaptive on marking zero nodes lost.
        [liveness plan §10.1](plans/liveness-off-the-raft-path-plan.md).
  - [ ] **Attribute the residual heartbeat-commit gap, then decide on
        `liveness.batchWindow`.** The knob ships parsed, validated, and
        **inert** (`0ms`), because the measurement it is contingent on has
        not been run: split the remaining gap between the fsync and the
        status lane's single `pipeline_loop` thread, at 1000 and 4000 nodes
        on both storage classes, reading
        `legion_store_coalesce_batch_size{path="heartbeat"}` against
        `legion_raft_pipeline_wait_seconds{lane="status"}`. Ship the window
        only if batches stay well below `HEARTBEAT_BATCH_MAX` under load —
        otherwise delete the field.
        [liveness plan §8 phase 1](plans/liveness-off-the-raft-path-plan.md),
        [call 4](calls/liveness-off-the-raft-path-plan_calls.md).
  - [ ] **No way to remove a node record, so simulated fleets orphan
        rows.** `Store::remove_node` (`src/storage/store.rs:5070`) is
        implemented, cross-lane correct, and unit-tested, but has no
        production caller: there is no `DeregisterNode` RPC and no
        `legion node` verb for it, so a row created by registration lives
        until the raft store is wiped. That is a product gap as much as a
        rig one — an operator decommissioning hardware has no way to
        remove the row either. On the rig it means `stop-sim-fleet.sh`
        leaves every node behind (the K8s path, by contrast, does
        `kubectl delete nodes -l type=kwok`), and a later smaller cell
        runs against the leftovers. Contamination is bimodal and both
        halves invalidate the cell: below 50% orphans you get the eval
        storm above; above 50% the mass-disruption breaker
        (`src/constants.rs:109`) marks *nobody* lost, so the cell runs
        with node-loss detection silently off, leaving one `error!` line
        (`src/manager/daemon.rs:824`) as the only evidence. Rig-side
        mitigations that need no product change: make the fleet gate in
        `run-step.sh:71` assert an exact live-node census instead of
        `registered >= NODES` (`4000 >= 1000` is why nothing caught it),
        and wipe + reinstall the control plane between an experiment and
        a campaign — `phase1.sh` runs `packing-density.sh` then
        `run-campaign.sh` with no wipe between. Do NOT scope node-name
        prefixes per run before deregistration exists: name *reuse* is
        currently the only thing bounding the node table, so unique
        prefixes would make every run leak its whole fleet.
  - [ ] **A mass node-loss event starves every other kind of eval.**
        Measured on the AWS rig 2026-08-04: ~3000 sim nodes vanished at
        once and job submission slowed to ~6s per job, with
        `evals_processed_total{kind="node_lost_reschedule"}` at 2752
        against `{kind="job_submit"}` at 66 and a 764-deep queue. Four
        separate causes, each fixable on its own:
        **Fixed 2026-08-06** —
        [node-loss-storm-plan.md](plans/node-loss-storm-plan.md)
        ([calls](calls/node-loss-storm-plan_calls.md)), public note in
        [docs/node-liveness.md](../docs/node-liveness.md) § "What a mass
        loss costs". Status per cause:
    - [x] **Equal priority means submissions queue behind recovery.**
          `node_lost_reschedule` evals now enqueue at
          `default_priority − NODE_LOST_RESCHEDULE_PRIORITY_DROP`
          (50 − 10 at the defaults), so a new submission overtakes the
          recovery backlog. Cross-kind `claim_eval` fairness was the
          rejected alternative ([call 1](calls/node-loss-storm-plan_calls.md)).
    - [x] **Lost nodes with no allocations still cost a full eval.** The
          predicate now runs at enqueue time (one chunked indexed query
          per batch, `Store::nodes_with_allocations`); the recheck
          re-runs it every tick so a racing in-flight placement is picked
          up one tick later, and the handler's early-return stays as the
          backstop.
    - [x] **The fan-out is one raft proposal per node, per 2s tick,
          inline in the status checker.** Both `mark_nodes_as_lost` and
          `recheck_lost_nodes` now go through
          `Store::enqueue_lost_node_reschedules`: one dedup query, one
          allocation-filter query, one `enqueue_evals` round for the
          whole batch (per-node forwarding-aware fallback under split
          lane leadership), and the checker runs the mark and the recheck
          on the blocking pool so the tick's gauge refresh and checkpoint
          never stall behind them.
    - [ ] **Failed evals may not reach `evals_processed_total`.**
          Suspected on the AWS run and then RETRACTED — the counter is
          incremented on both paths at `src/manager/daemon.rs:2267`, and
          the evidence turned out to be two scrapes compared across
          different times. Left here as one deliberate check for the next
          campaign rather than as a known bug: submit a job whose
          constraints match no node, and confirm
          `evals_processed_total{kind="job_submit",status="error"}`
          moves by exactly one.
    - [x] **Recovery is invisible.** `legion_nodes_recovered_total{path}`
          now counts both paths (`heartbeat`, `reregistration`), the
          re-registration path logs, and `marked_lost − recovered` is the
          net loss ([docs/metrics.md](../docs/metrics.md)).
    - [ ] **Re-measure the storm on the rig.** The 2026-08-04 numbers
          (submission ~6 s/job behind 2,752 reschedules) are the before;
          repeat the ~3,000-node loss and expect submissions to keep
          landing while `legion eval list` shows the backlog at
          priority 40.
        Context that still stands: a single-AZ loss in a three-AZ cluster
        is ~33% of the fleet, *under*
        `LIVENESS_MASS_DISRUPTION_FRACTION` (`src/constants.rs:109`), so
        the full storm fires unsuppressed — by design; these fixes are
        what make that affordable.
  - [ ] **`mark_node_ready` writes `status = "ready"`, which every liveness
        query filters out.** `mark_node_ready_apply`
        (`src/storage/store.rs`) sets `"ready"` rather than `"active"`, so a
        node recovered through it drops out of `active_node_liveness_rows`,
        out of `legion_nodes{status="active"}`, and out of the liveness scan
        entirely. It also flips `node.availability` unconditionally, so it
        would un-cordon a drained node. Nothing in production enqueues the
        `node_join` eval that calls it, so it is currently dead code on the
        real path — which is why the heartbeat recovery does the composite
        itself instead
        ([call 2](calls/liveness-off-the-raft-path-plan_calls.md)). Fix:
        normalize the status string to `"active"` and adopt the same
        `unavailable`-only availability guard, then delete the duplication.
  - [ ] **Node loss clobbers an operator's drain.** `mark_nodes_as_lost`
        calls `sync_node_availability(ids, "unavailable")` unconditionally,
        so a node an operator had `drained` loses that intent the moment it
        goes lost — and comes back `available` rather than `drained`. The
        recovery path deliberately does not paper over this (it only clears
        `unavailable`), because the drain intent is already gone from the row
        by then. Fixing it properly means recording the pre-loss
        availability, or keeping loss and drain on separate axes.
  - [ ] **Per-pool heartbeat freshness.** `legion_heartbeat_freshness_ratio`
        is fleet-wide, so a cluster with a slow batch pool and a fast
        latency pool has two populations and one blended ratio, which can
        mask a problem in the fast pool. Named as a known limitation on
        [docs/heartbeat-cadence-and-scale.md](../docs/heartbeat-cadence-and-scale.md).
  - [ ] **Should adaptive cadence default on?** Off by default today because
        the benchmark-fairness argument is strongest while the comparative
        numbers are being produced. The criterion for flipping: a campaign
        run showing a cluster past its ceiling surviving with adaptive on,
        detection latency inside the documented bound, and the same run
        reproducing the collapse with it off — with
        `targetRatePerSecond` auto-derived from measured `R` rather than
        configured.
        [cadence plan §11](plans/heartbeat-cadence-at-scale-plan.md).

  - [ ] ~~Investigate: a 1k simulated fleet sustains ~59 heartbeat
        RPCs/sec where ~500/sec is expected, with nothing saturated.~~
        Resolved by the entry above. **Correction to that entry's last
        bullet:** the follower disagreement (leader 942 active vs both
        followers 0 active / 1000 lost) was transient — measured just
        after a fleet teardown. With a fleet running, all three managers
        report ~997 active / ~3 lost. Node status replicates correctly;
        there is no replication bug.
        Measured on the first real AWS rig (2026-08-03, 1000 sim nodes on
        one c7i.2xlarge against a 3-voter lanes control plane). The
        numbers rule out the easy explanations:
        - sim host CPU **0.2%** of 8 vCPU, memory 3% — the fleet is idle,
          so this is not the simulator running out of room
        - manager CPU **19-23%** of 4 vCPU across all three — not
          saturated either
        - `legion.Manager/Heartbeat` gRPC **p99 50ms** — the RPC is fast
        - `legion_heartbeats_total` is per-RPC (`server.rs` increments it
          once per handler call), so this is not group-commit batching
          making the counter read low
        - effective interval works out to ~17s against a 2s
          `WORKER_HEARTBEAT_DURATION_SEC`, and the drill's heartbeat p99
          pins at exactly 10s — which is `HEARTBEAT_RETRY_MAX`, hinting
          the loop is in retry/backoff rather than steady state
        - node liveness disagrees across the cluster: the leader reports
          942 active / 58 lost while **both followers report 0 active /
          1000 lost**. Either follower `legion_nodes{status}` is
          leader-only by design and mismeasured here, or status is not
          replicating — worth settling on its own.
        Next step is the fleet's own worker-side counters
        (`legion_worker_heartbeats_total{outcome}` on :9101, see
        docs/metrics.md § Worker) to separate "heartbeats attempted and
        failing" from "heartbeats never attempted". Until this is
        understood, `experiments/packing-density.sh` cannot produce a
        density and every scale step stays sized from the plan's 1,250
        estimate.
  - [ ] **Warn when a manager binds a wildcard host with no
        `server.advertiseAddr`.** Found the hard way on the first real
        campaign. With `host: '0.0.0.0'` and `advertiseAddr` unset,
        `ServerConfig::advertised_address()` derives
        `http://127.0.0.1:4747`, and that address is embedded in *every
        join token the manager mints*. A remote worker decodes the token
        and dials its own loopback, retrying connection-refused forever.
        Nothing on the manager side looks wrong — the failure appears
        only in the worker's journal, and an operator watching the
        cluster sees "0 nodes registered" with no error at all. Options,
        roughly in order of preference: refuse to mint a join token whose
        address is loopback while bound to a wildcard host; or log a
        startup warning naming the derived address; or return the
        condition in `CreateJoinToken`'s response. The config comment in
        `configs/manager.ncl` already says what the field is for, and
        that was not enough. See `benchmarks/aws/configs/legion/
        manager-a.ncl.tpl` for the fix the rig now carries, and note
        `persistence.raft.advertiseAddr` is a *different* setting — having
        set that one is exactly why the gap was easy to miss.
  - [ ] Export process-level memory from the manager so the campaign can
        report the manager's own RSS instead of whole-box memory.
        `docs/metrics.md` has no process family and nothing registers a
        process collector, so `process_resident_memory_bytes` does not
        exist on `:4749`; `collect.sh` therefore reports node_exporter
        whole-box memory, an upper bound that includes the OS and (in
        `used`) reclaimable page cache — ~40% of `used` on an idle
        control-plane box. Deliberately not done mid-campaign: it changes
        the binary under test and invalidates the fairness record for
        every cell already collected. Do it before the phase-2 campaign.
        See calls entry 20 and the Metrics section of the plan.
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
      worker config turns a task's `resources` into real kernel limits; an
      explicit `config.cgroup` still means "join this one"
      ([docs](../docs/task-supervision.md#resource-enforcement-cgroups)).
      **Superseded in place** (2026-07-31) by the container-runtime plan's
      node-wide subsystem: the same config knob now governs `exec` *and*
      `container` through one `legion/share` + `legion/reserve` hierarchy,
      with `cpu.weight` in place of the old `cpu.max` quota and cpuset
      pinning for `cores` ([docs](../docs/resource-enforcement.md),
      [calls 16](calls/container-runtime-exec-and-signals-plan_calls.md)).
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
- [ ] **`sandbox` adopts `resource_enforcement()`** as a `RuntimeApplied`
      consumer — pin the VMM's vCPU threads to the assignment's cores, size the
      guest from its memory limit. Surfaced by the container-runtime plan
      ([plan Q4](plans/container-runtime-exec-and-signals-plan.md#11-open-questions),
      [docs](../docs/resource-enforcement.md)) as the honest test of whether
      the three modes are the right cut: a microVM's "memory limit" is guest
      size, not a cgroup ceiling, so the assignment may need to say which it
      means. Until it lands, `sandbox` tasks are ungoverned — their `resources`
      size the guest and are otherwise scheduling-only, and they can land on
      cores another task reserved.
- [ ] **Linux container port mapping / private network namespaces.** Containers
      keep host networking; assigned ports reach them without mapping, but
      there is no isolation. Named non-goal of the container-runtime plan —
      it belongs to the networking work below.
- [ ] **NUMA/SMT-aware core placement.** The core allocator hands out whole
      logical CPUs lowest-free-first with no topology awareness: no NUMA-node
      affinity, no SMT-sibling avoidance. Nomad gates this behind Enterprise;
      revisit with a workload that measurably needs it
      ([docs](../docs/resource-enforcement.md#not-implemented)).
- [ ] **A delegated (non-root) cgroup hierarchy root.** Enforcement assumes a
      root worker creating `legion/` at the cgroup root; a configurable root
      for delegated setups is unbuilt
      ([docs](../docs/resource-enforcement.md#turning-it-on)).
- [ ] **`legion alloc signal`** — an on-demand signal verb. The plumbing now
      exists (`TaskPlugin::signal`, routed through the registry); today signals
      are only delivered by declared policy (`changeSignal` on identity
      refresh, `stopSignal` on teardown). Additive; named non-goal of the
      container-runtime plan.
- [ ] **The `sandbox` runtime still cannot exec, signal, or deliver identity
      files** — all three need the guest agent the sandbox plan owns. The
      container runtime's `TaskPlugin::signal` override is the seam its
      guest-agent path would implement.
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

- [x] **A sync forward to a new lane leader panicked the manager.**
      `MultiRaftDriver::forward_lane_write` dialed the leader through
      `ChannelPool::get` *before* entering the raft runtime, and
      `Endpoint::connect_lazy` builds a tonic channel by spawning its
      connection task — which panics ("there is no reactor running") on a
      thread with no runtime context. Every synchronous caller reaches this
      path when its write lands on a follower
      (`Store::forward_after_rejection` → `enqueue_eval`), and the channel
      pool hid it: only the *first* forward to a given leader address dials,
      so it fired only just after leadership moved to a manager this node had
      not forwarded to before. The dial now happens inside
      `block_on_engine`. This was previously logged here as
      `raft_lanes::killing_the_all_lane_leader_reconverges_onto_one_survivor`
      being "timing-sensitive under parallel test load" — that diagnosis was
      wrong; the test was reporting a real crash. Reproduced at ~1-in-7 runs
      of that suite before the fix, 20/20 clean after.

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
  - [x] Contention instrumentation for the manager write path (2026-08-02):
        the families that were missing when the ~80 hb/s ceiling was
        investigated — group-commit effectiveness
        (`legion_store_coalesce_batch_size` / `_wait_seconds{role}` /
        `_commit_seconds` / `_wait_rounds_total`, labeled `heartbeat` and
        `plan_apply`), the per-lane pipeline queue in front of raft
        (`legion_raft_pipeline_wait_seconds`, `legion_raft_pipeline_depth`),
        and runtime saturation (`legion_grpc_in_flight`,
        `legion_runtime_spawn_delay_seconds`,
        `legion_runtime_worker_threads`). Read order in
        [docs/metrics.md](../docs/metrics.md) § "Diagnosing a manager
        throughput ceiling"; calls in
        [calls/manager-contention-instrumentation_calls.md](calls/manager-contention-instrumentation_calls.md).
        Instrumentation only — the blocking heartbeat path is unchanged.
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

- [ ] **`deduplicated` lies to the loser of a CONCURRENT idempotency-key
      race, on every mutating RPC except `RunJob`.** Two requests carrying
      the same key both miss `idempotency_lookup`, and `idempotency_record`
      (`INSERT OR IGNORE`) already returns which one won — but `stop_job`,
      `destroy_job`, `scale_task`, the `instance` verbs and the rest
      discard that boolean and hardcode `deduplicated: false`, so both
      callers are told they were first. `run_job` was fixed alongside the
      job-intake group commit (which widens the overlap window); the rest
      each need their own decision about what the loser should be handed
      back. Sequential retries — what the key is really for — are correct
      everywhere. Calls:
      [eval-path-raft-cost-plan_calls.md](calls/eval-path-raft-cost-plan_calls.md)
      §17.
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

Deferred from [ui-holistic-polish-plan.md](plans/ui-holistic-polish-plan.md):

- [ ] Restart worker from the worker page. `RestartWorker` exists and the CLI
      has `legion node restart`; it restarts the worker *process*, which wants
      its own confirmation copy before it sits next to Cordon / Drain
- [ ] A polled page whose one RPC keeps failing while the manager is otherwise
      up shows its last good data with no local hint — `retainOnError` keeps
      the last value and only the connection banner reports outages. A
      per-card "stale since" marker would close the gap
      ([calls entry 3](calls/ui-holistic-polish-plan_calls.md))
- [ ] ⌘K recents are per browser (localStorage); per-identity recents would
      need a manager-side store
      ([calls entry 7](calls/ui-holistic-polish-plan_calls.md))
- [ ] The CI generated-client check trusts the runner's apt `protoc`; pin it
      (buf, or a release download) if a runner image bump ever produces a
      false drift ([calls entry 9](calls/ui-holistic-polish-plan_calls.md))
- [ ] A toast primitive, should a feature need feedback away from the control
      that caused it; every message so far is an inline `Note`

## Cross-cutting

- [ ] **Ship a Nickel contract library for the jobspec.** `legion job schema`
      emits JSON Schema for the *rendered* JSON; an importable
      `legion/jobspec.ncl` a spec could apply as a contract would move whole
      classes of error to evaluation time. Needs a generator from the same
      source as the JSON Schema or it drifts — deferred out of the
      [Nickel migration](plans/nickel-config-language-plan.md).
- [ ] **Decide whether `legion fmt` is worth the dependency.**
      `nickel-lang-core`'s `format` feature pulls in topiary + tree-sitter;
      today users format with the standalone `nickel` CLI. Deferred out of the
      [Nickel migration](plans/nickel-config-language-plan.md).
- [ ] **Audit every name decision in depth.** Important, and gets harder the
      longer it waits.
- [ ] Performance and scale: prove Legion scales better than K8s, and than Nomad
- [ ] **`DestroyJob` reports a precondition failure as an opaque
      `Internal`.** Destroying a running job fails, correctly, with
      `"Job must be stopped before it can be destroyed"` — but that
      reason stays in the manager's log. The client gets
      `{"code": "Internal", "message": "Error deleting job"}`, which
      names neither the cause nor the fix. Wrong on three counts: the
      code should be a precondition/failed-precondition rather than
      `Internal`, the envelope should carry a `hint` ("stop the job
      first, or pass --asap"), and an agent reading `Internal` has no
      way to tell a bug from a thing it should do differently — exactly
      what the agent-readiness rules in AGENTS.md exist to prevent.
      Found when a benchmark teardown silently left 835 jobs behind.
      Worth auditing the other mutating verbs for the same pattern.
- [ ] **`schedulingPolicy` in the rig contradicts the scenarios.**
      `benchmarks/aws/configs/legion/manager-a.ncl.tpl` sets
      `schedulingPolicy = "bin_pack"` while
      `benchmarks/scheduler/scenarios/web_fleet.json` declares
      `"scheduling_policy": "spread"`. The manager setting wins, so every
      number from 2026-08-04 is a bin_pack number. That matters for the
      conflict work specifically: bin_pack is the maximally correlated
      policy — every planner's argmax is the same fullest-node-that-still
      -fits — so it is the worst case for optimistic concurrency and the
      best case for the two-pass bind. Decide which policy the campaign
      is supposed to measure, make the two agree, and measure the
      conflict rate under BOTH before quoting either.
- [ ] **Consider a batch job-submission RPC.** `RunJob` is one job per call,
      so a client submitting N jobs pays N round trips and N control-lane
      raft commits. Measured on the AWS rig 2026-08-04: a client submitting
      serially manages ~40 jobs/sec, of which ~25ms per job is CLI startup
      (~2ms), Nickel evaluation (~13ms) and the commit (~10ms) — and Legion
      placed each job before the next one arrived, so the client, not the
      scheduler, set the rate. The benchmark driver worked around it with a
      thread pool, but bulk submission is a real user path (CI fan-out,
      a `legion job run` over a directory, an agent registering a fleet of
      jobs). A `RunJobs` that commits once for the batch would cut both the
      round trips and the commits. Wants a plan first: batching changes
      idempotency (one key per batch or per job?), partial-failure
      semantics, and ACL scope evaluation, none of which are obvious.
      **Still open, and now measurable.** The benchmark rig no longer needs
      it — `legion-loadgen` reaches ~1,100-1,500 jobs/sec against one local
      manager with unary `RunJob` — but that is also the first honest
      baseline a `RunJobs` would have to beat, and the per-job control-lane
      commit is still there underneath it. The loadgen's concurrency curve
      is the evidence: throughput peaks near 128 in flight and *falls* to
      607 jobs/sec at 1,024, which is what a serialized commit looks like
      from the client side.
- [ ] **`a_task_that_ignores_its_stop_signal_is_killed_at_the_deadline` is
      flaky under parallel load.** Passes alone and in most full runs, fails
      intermittently in `cargo test --all` (seen 2026-08-04). Its own comment
      claims "a zero grace window is already expired, so the very first reap
      takes it — no sleeping, no flakiness", but it spawns a real process and
      asserts a reap deadline, so a loaded machine can reap before the child
      is scheduled. Not caused by the plan-apply work — `src/worker/task_manager.rs`
      is untouched by it. Make the assertion independent of the child actually
      having started, or gate on an observed start.
- [ ] **`acked_write_survives_any_single_node_loss` is bimodally flaky on
      small hosts.** Seen 2026-08-07 on a 4-core shared VM, debug build:
      either the whole test completes in ~1s or a round's survivors never
      re-elect and it dies at the 150s `wait_for` — nothing in between,
      and the failing state persists for stretches of minutes before
      clearing. Shown NOT to be caused by the JSONB-affinity migration:
      interleaved same-window runs had base `3a62fcd` failing 3/4 while
      the migration branch passed 3/4 (and each side showed the opposite
      pattern in other windows). The other three tests in
      `tests/raft_durability.rs` did not flap. Suspect the tight election
      timeouts the harness itself warns about ("under CPU starvation the
      tight election timeouts flap leadership") plus this box's scheduling
      jitter; wants either wider election timeouts under
      `cfg(debug_assertions)` in the harness or a diagnosis of why a
      two-survivor cluster can stay leaderless for 150 s.
- [ ] **`teardown.sh` silently aborts when run non-interactively, leaving the
      whole rig billing.** It prompts `read -r -p "Destroy the entire benchmark
      rig?"` unless `--yes` is passed. Under `nohup ... < /dev/null` (how any
      unattended teardown runs) `read` gets EOF and returns non-zero, and
      `set -e` kills the script *before* it prints "aborted" — so the log ends
      on a successful-looking S3 sync line and every instance stays up. Hit
      tonight: teardown appeared to succeed twice while the full rig kept
      running. Fix: detect a non-tty and either default to proceeding or fail
      loudly, never exit silently on a confirmation prompt. Audit the other
      rig scripts for the same `set -e` + `read` shape.
- [x] **Bake the operator box's toolchain into an AMI.** Standing the box up
      installed build-essential, protoc, clang, terraform, AWS CLI v2 and
      rustup in user-data on every launch: `cloud-init analyze blame` measured
      `modules-final/config-scripts_user` at **3245 seconds (54 minutes)** on
      one launch and ~14 minutes on another — same instance type, same region,
      same script, so it is `apt-get` throughput and nothing the rig controls.
      All of it paid before any Legion code compiles.
      `benchmarks/aws/scripts/bake-operator-ami.sh` pays it once into a tagged
      AMI (~20 min, ~$0.05/mo for the snapshot) and `bootstrap-operator-box.sh`
      launches from it, leaving only per-instance setup — the rig's SSH key
      pair — in user-data, so first boot is seconds. Both paths run the one
      `scripts/operator-toolchain.sh`, whose content hash is tagged onto the
      image: a toolchain change makes the bootstrap report the image stale and
      fall back to the slow path rather than launch a box missing a tool. No
      baked AMI means today's behavior exactly, with a log line saying so.
      See the README's "Bake the operator AMI first" and
      [calls #29](calls/aws-comparative-benchmark-plan_calls.md).
  - [ ] **Bake one against real AWS.** Written and exercised against a stubbed
        `aws` CLI only (every path: no AMI, current AMI, stale AMI, re-run,
        `--show`), because a campaign was billing at the time and an extra
        instance or image would have confused its teardown. Unverified: that
        console output survives the bake instance's stop (the version tag
        degrades to `unknown` if not — the power state, not the console, is
        the completion signal), and that the real bake lands inside the
        90-minute timeout.
- [ ] **Share one target dir across plugin builds.** `build.rs` gives every
      plugin its own `CARGO_TARGET_DIR` (`target/plugin-build/<kind>/<name>`)
      and builds them sequentially, and every plugin depends on the whole
      `legion` crate — so a `LEGION_BUILD_PLUGINS=1` build compiles the same
      ~470 dependency crates six times in release, ~2,950 crate-compilations
      and ~10.9 GB where ~600 and ~2 GB would do. It dominates the 16m48s
      clean build, and the AWS rig pays it on every `build-and-upload.sh`.
      The blocker is a name collision, not the target dir: `node_checks/example`
      and `volumes/example` are both crate `example`, so one shared dir would
      have them clobber each other's `libexample.dylib`, which is what the
      harvest step globs for. Rename the plugin crates unique, then share the
      dir — sequential builds mean no lock contention, and identical profile +
      dependency versions mean cargo reuses everything after the first plugin.
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

## Configuration language

- [x] **Nickel replaces Jsonnet as the configuration language** (2026-08-03,
      [plan](plans/nickel-config-language-plan.md) ·
      [calls](calls/nickel-config-language-plan_calls.md) ·
      [docs/configuration-language.md](../docs/configuration-language.md)).
      Every surface — job specs, manager config, worker configs, ACL policy
      files, job templates — is `.ncl`, evaluated through one module
      (`src/config_lang.rs`) with Nickel's positioned diagnostics. The forked
      `jsonnet-rs` git dependency and its C++ VM are gone. Jsonnet's ext-vars
      and top-level args collapse into a single `vars` record and four flags
      (`--var`, `--var-code`, `--kv-var`, `--kv-var-code`); job templates hide
      their `job` function with `| not_exported` and reject imports lexically
      before evaluation; `source_jsonnet` became `source_nickel` in the proto,
      the store, and the UI. `tests/config_jobs.rs` grew coverage for the
      manager configs, the worker configs, and the shipped job template, none
      of which were evaluated by any test before. **No compatibility path**:
      `.jsonnet` files are not recognized and stored template sources do not
      migrate (deliberate — see calls #4).
- [x] **`.json` accepted on four of the five config surfaces** (2026-08-03,
      [calls](calls/nickel-config-language-plan_calls.md) ·
      [docs](../docs/configuration-language.md#json-for-machine-emitted-config)).
      For machine-emitted config — generators already have a JSON serializer,
      and JSON is *not* a Nickel subset, so without this a generated object
      fails as `invalid contract expression`. Dispatch is by extension only
      (`config_lang::format_for`), no content sniffing. `--var` against a
      `.json` file is an error, not a silent no-op; Nickel's native
      `import "x.json"` is the documented way to mix the two. Job templates
      stay Nickel-only — a template's `job` is a function, which JSON cannot
      express — and `legion template create` rejects a `.json` file with that
      reason. Shipped example: `configs/jobs/machine-generated.json`.
- [x] **Local jobspecs are evaluated in place** (2026-08-03). `legion job
      run/plan/validate` used to copy a local spec into a temp directory,
      which stranded every relative import — `configs/jobs/demo-imports-2.ncl`
      did not run from the CLI. Pre-existing under Jsonnet, where the import
      callback `unwrap()`ed the failed read and panicked. Remote sources still
      stage, correctly: the fetcher brings the whole tree.

## Worker nodes

- [x] **Node liveness off the raft path, and a lost window that moves with the
      cadence.** Two plans, shipped together 2026-08-03:
      [liveness-off-the-raft-path-plan.md](plans/liveness-off-the-raft-path-plan.md)
      ([calls](calls/liveness-off-the-raft-path-plan_calls.md)) and
      [heartbeat-cadence-at-scale-plan.md](plans/heartbeat-cadence-at-scale-plan.md)
      ([calls](calls/heartbeat-cadence-at-scale-plan_calls.md)) ·
      [docs: node-liveness.md](../docs/node-liveness.md),
      [heartbeat-cadence-and-scale.md](../docs/heartbeat-cadence-and-scale.md)
  - [x] A steady-state heartbeat costs **no replicated commit and no fsync**:
        it is one write into the status-lane leader's in-memory
        `LivenessTable` (`src/storage/liveness.rs`). Only status transitions
        replicate. `legion_raft_pipeline_depth{lane="status"}` should sit at
        ~0 between display checkpoints.
  - [x] **The failover grace runs from liveness RESPONSIBILITY**, not process
        uptime — closes the "status-lane leadership change has no lost-node
        grace" item that used to live in Open. Acquisition seeds every active
        node at `now` and marks nothing lost for `liveness.failoverGrace`
        (60s), so an election on a manager that has been up for hours marks
        **zero** nodes lost where it previously marked the whole fleet.
        `legion_liveness_responsible_seconds` resets at every acquisition.
  - [x] **Per-node lost windows** (`node_health.lost_after_seconds`, additive
        migration, NULL = the old constant): a node is lost after
        `missedBeatsBeforeLost x its own interval`, so a 2s pool and a 60s
        pool coexist with 10s and 300s windows. `5 x 2s` reproduces the
        historical fixed 10s exactly. The worker reports its resolved cadence
        in `config_attrs.heartbeatSeconds`.
  - [x] **`heartbeatSeconds` clamp widened 30 -> 300**, gated on a hard
        startup check against `stateful.onLost.selfTerminateAfter` — closes
        the "unvalidated against each other" item that used to live in Open.
        A config that would make a healthy worker self-fence between beats
        fails manager startup, naming both numbers.
  - [x] **Mass-disruption circuit breaker**: >50% of the active fleet lost in
        one tick marks none and logs at `error`. Suppressed under 20 nodes.
  - [x] **Peer-gap suppression and the liveness digest**
        (`RaftTransport.ReportNodeLiveness`): a manager that does not lead the
        status lane sends ONE RPC per second carrying the ids it heard from,
        whatever the fleet size, and a quiet peer's nodes are not marked lost
        inside `liveness.peerDigestGrace`. Falls back to per-heartbeat
        forwarding against a manager that predates the RPC.
  - [x] **Freshness**: `legion_heartbeat_freshness_ratio` (the number to alert
        on) plus served-interval / lost-window / sustainable-nodes gauges and
        `legion_nodes_marked_lost_total`, a debounced runtime warning that
        computes the `heartbeatSeconds` to set, and `legion node cadence`
        (`--json`, MCP tool, capability entry, `GetHeartbeatCadence` + ACL row).
  - [x] **Adaptive cadence, opt-in** (`heartbeat.targetRatePerSecond`): fleet-
        size-derived interval on a Fibonacci-ish ladder with step-up-now /
        step-down-after-5m hysteresis, served on `HeartbeatReply
        .next_interval_seconds`, with widen-immediately / narrow-on-confirmation
        windows and a one-shot full-width re-phase in the worker sleep. The
        simulated fleet honors it through the same shared helpers.
  - [x] **A returning node is schedulable again.** Node loss is a cross-lane
        composite (status lane `lost`, control lane `unavailable`), and the
        scheduler gates on `availability` — so recovery undoes both halves. A
        worker that rides out a partition and resumes heartbeating recovers
        without restarting or re-registering. Guarded so a heartbeat can
        never un-cordon an operator-drained node.
  - [x] **Display checkpoint + `NodeMsg.heartbeat_source`**: one batched
        status-lane commit per 30s records each node's OBSERVED beat time, so
        every manager can show an approximate "last heartbeat" and say that it
        is approximate. Never read back as liveness. `"0s"` disables it.
  - [ ] The AWS scale runs for both plans are **not done** — see the open
        items in "Benchmarks and scale" above.
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

- [x] **The read-model refresh takes the changeset delta, not a whole-node
      reload.** 2026-08-05, from the phase-4 fill-scaling finding
      ([findings](aws-phase4-findings.md),
      [calls](calls/eval-path-raft-cost-plan_calls.md) #2).
      `cache_refresh_allocation_scopes` reloaded every touched node's ENTIRE
      allocation list after each committed changeset, on every replica — ~440
      rows at 50,000 allocations and ~1,760 at 200,000 to learn about the ~44
      the changeset already named. `cache_apply_allocation_delta` now reads
      exactly the named rows by primary key and splices them in, so the cost
      follows the change instead of the fill. What it gives up is
      self-healing, bounded three ways: a per-node fallback to the full
      reload when the delta contradicts the cache, a whole-changeset fallback
      when a record cannot be classified, and a round-robin scrub that
      reloads one node in full per projected changeset, so drift is repaired
      within one sweep of the fleet. Two new `warn!` lines fire only when the
      cache and committed state disagree.
- [x] **Plan-apply conflict reduction: the epoch is a ranking hint, not a
      gate.** 2026-08-04, from the AWS phase-2 finding that **74% of plan
      applies were rejected and re-planned**
      ([findings](aws-phase2-findings.md),
      [plan](plans/plan-apply-conflict-reduction-plan.md),
      [calls](calls/plan-apply-conflict-reduction-plan_calls.md)).
      Three things shipped together:
    - **Two-pass bind** in `apply_plan_submission_on`. Pass 1 is the old
      walk byte-for-byte; pass 2 revisits only epoch-moved candidates and
      admits them on the in-transaction revalidation that already runs
      (node row, availability, constraints, free pool with this plan's
      binds debited, static ports, `distinct`). Rejections now name
      capacity or constraint instead of staleness. `target_instance`
      ratification keeps the hard gate. New
      `legion_scheduler_plan_bind_pass{pass}`.
    - **Candidate lists restored.** `decouple_plan_components` was
      collapsing every multi-placement plan to ONE candidate whenever two
      placements shared a candidate node — which for a multi-replica job
      into one pool is always, so late binding was structurally off for
      the workloads it was built for. Overlap edge deleted (the in-tx
      pool debit already covers it); `distinct` and co-placement edges
      kept. Fallback tail now samples from the top-W by score instead of
      a deterministic top-N that handed every concurrent planner the same
      list.
    - **A pre-existing partial-apply bug**, found on the way: a *rejected*
      plan committed the placements it had already bound — allocation
      rows no `allocation.placed` event announced, inside a submission
      marked `rejected`. Only evictions were savepointed; the savepoint
      now covers the whole plan body. Removing the overlap edge would
      have widened that window sharply.
      1482 tests pass, clippy and fmt clean, and the oversubscription and
      job-config-epoch fences pass **unmodified**.
- [x] **Priorities and eviction/preemption, with eviction toggled at the pool
      level.** Six named priority bands (`critical` / `high` / `elevated` /
      `normal` / `low` / `background`) driving both eval-queue order and
      preemption; victims chosen during planning and committed — deleted,
      requeued, journalled — in the same epoch-CAS transaction that inserts
      the preemptor's allocations. Strictly cross-band, minimum victim age,
      minimal victim sets, `disruption.minAvailable` as a hard floor on
      voluntary disruption only, `enforce`/`observe`/`off` modes and
      `schedulerSettings.preemption.disabledPools` for the pool toggle,
      `job plan` preview, `allocation.preempted` events with a
      victim↔preemptor link, and `legion_preemptions_total`.
      [plan](plans/preemption-and-eviction-plan.md) ·
      [calls](calls/preemption-and-eviction-plan_calls.md) ·
      [docs](../docs/preemption-and-eviction.md)
  - [x] Phase 1a — stop causes. Every allocation removal records *why*,
        delivered to the owning worker and carried in `allocation.removed`
        metadata. [docs](../docs/graceful-stop.md)
  - [x] Phase 1b — graceful termination. `stop { signal, killAfter }`,
        per-runtime stop ladders, `LEGION_STOP_NOTICE_PATH`,
        `allocation.stopping`, stops-before-starts reconcile ordering, and the
        worker-side terminating set + reaper the blocking-`stop()` hazard
        required. [docs](../docs/graceful-stop.md)
  - [x] Phase 1b's grace-vs-latency question, resolved by taking neither
        option: no preemption-specific grace (it would make the ladder
        cause-dependent) and no accounted-not-free start (it would let a
        container runtime run victim and preemptor on the same reserved
        memory). The 5m `killAfter` cap is the backstop; documented as a known
        limitation.
  - [x] Plugin ABI version symbol. `declare_task_plugin_abi!()` is now
        required of every task plugin, so a stale dylib fails at load with a
        message naming the rebuild instead of mismatching silently.
        [docs](../docs/task-runtime-plugins.md)
  - [x] Phase 2 — priority bands, governed by a `bands` selector on `job` ACL
        rules (not `operator:write`), normalized before enforcement and
        covering the template-render path, with explicit cases in
        `tests/acl_endpoints.rs`. Delivers per-namespace band caps, so that is
        no longer a separate item. [docs](../docs/auth-and-acls.md)
  - [x] Phase 3 — plan-atomic preemption. [docs](../docs/preemption-and-eviction.md)
  - [x] Phase 4's UI item — preemption history on the job page, the band badge
        on the job header, and the `priority` column in `job list`.
  - [x] At least think through fair-share policies — find the 80/20 rather than
        building a full hierarchical scheduler. The 80/20 is priority bands
        plus per-namespace band caps via the `bands` ACL selector;
        fair-share/hierarchical queues stay an explicit non-goal (Kueue's
        policy-matrix issue history is the argument against).
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
      changes, and `benchmarks/micro/scheduler_bench.rs` grew a
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
- [x] **The `container` runtime's four core gaps, closed** (2026-07-31) —
      [plan](plans/container-runtime-exec-and-signals-plan.md) ·
      [calls](calls/container-runtime-exec-and-signals-plan_calls.md). Six of
      its seven phases; Phase 3 (graceful stop) was **superseded before merge**
      by the preemption plan's Phase 1b, which shipped the same ladder as
      `stop { signal, killAfter }` — see the Phase 3 entry below:
  - [x] Phase 1 — exec on Linux via `<runtime> exec` for youki/runc/crun, so
        `legion exec`, actions, the UI terminal, and MCP exec work on the OS
        the workloads ship on ([docs](../docs/exec-and-actions.md))
  - [x] Phase 2 — `TaskPlugin::signal()` seam with a tracked-pid default, and
        a container override that signals the container *init*
        (`container kill` / `<runtime> kill`); `changeMode: signal` identities
        on container tasks are accepted
        ([docs](../docs/workload-identity.md))
  - [~] Phase 3 — graceful stop. **Superseded, not shipped as designed.** The
        branch built `stopSignal` / `stopGracePeriod`; preemption's Phase 1b
        landed on `main` first with the same feature as
        `stop { signal, killAfter }`, one ladder for every stop cause. The
        duplicate surface was dropped at merge rather than shipped alongside
        — two jobspec fields for one behavior is the incoherence this repo
        keeps out. What the branch *kept* is the runtime-side half Phase 1b
        did not cover: the Linux container teardown ladder (signal → bounded
        wait → `delete --force` instead of an unconditional SIGKILL)
        ([docs](../docs/graceful-stop.md),
        [calls 19](calls/container-runtime-exec-and-signals-plan_calls.md))
  - [x] Phase 4 — the `resource_enforcement()` interface (`Unsupported` /
        `WorkerCgroup` / `RuntimeApplied` + a worker-computed
        `ResourceAssignment`), Linux memory hard limits, `cpu` MHz →
        `cpu.weight`, OOM-kill annotation from `memory.events`, and the
        whole-`cores` / `cores`-xor-`cpu` validation rules
        ([docs](../docs/resource-enforcement.md))
  - [x] Phase 5 — exclusive `cores` via cpuset pinning: worker core allocator,
        `legion/share` + `legion/reserve` slices, exhaustion failure path,
        assignment persisted and rebuilt on worker restart
  - [x] Phase 6 — the `exec` runtime adopts the interface (`WorkerCgroup`), so
        one node-wide fence governs container *and* exec tasks, exec gains
        real memory limits, and teardown reaps double-forked descendants via
        `cgroup.kill`; an explicit `config.cgroup` still wins
        ([docs](../docs/task-runtime-exec.md))
  - [x] Phase 7 — the task dir is mounted at `/legion/task` with translated
        env, flipping KV file injection, identity file delivery, and file/tree
        artifacts to supported in containers — refresh included, because the
        directory is mounted rather than individual files
        ([docs](../docs/task-runtime-container.md#the-task-directory-legiontask))
  - Enforcement is gated on the pre-existing opt-in `resourceEnforcement`
    worker config rather than shipping ungated, and it *replaces* the exec
    runtime's own per-allocation cgroup path (which conflated `cpus` with
    `cores` and used a CFS quota) — see calls 8, 10, and 16.
  - The two-phase stop this branch also built (signal under the lock, reap on
    the supervisor's tick) arrived independently in preemption's Phase 1b and
    is `main`'s implementation now; the branch keeps only its resource-release
    step, ordered strictly behind the reap so a cgroup is never removed while
    it still holds a live process
    ([calls 19](calls/container-runtime-exec-and-signals-plan_calls.md)).

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
- [x] **Edge boot: provisional allocations, gate levels, and boot tickets** —
      a node reaches a declared execution stage (`stage`/`boot`/`serve`) before
      the placement is durable, and an edge node authorizes a start itself from
      a short-lived manager-signed ticket. Ratification is the existing plan
      apply with the instance named, so the at-most-one-live-allocation
      invariant is unchanged and its suites run unmodified
      ([plan](plans/edge-boot-plan.md) · [docs](../docs/edge-boot.md) ·
      [calls](calls/edge-boot-plan_calls.md)). Phase 0's measurement and
      phase 5's backlog are open above.

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
- [x] Simulated fleets feed the `legion_worker_*` families (2026-08-02) —
      the sim fleet starts the shared worker telemetry listener (via
      `daemon::start_worker_telemetry`, so the real and simulated paths
      cannot drift on how telemetry is turned on) and reports into the
      process-global worker registry: the existing heartbeat/allocation
      families aggregated over the fleet, plus
      `legion_worker_simulated_nodes`, `..._nodes_target`, and
      `legion_worker_simulated_registrations_total{outcome}`. Per-node
      series deliberately omitted — cardinality at the ~1,250 nodes/host
      the AWS campaign packs, and the manager already holds the per-node
      view. The AWS rig consumes them: `start-sim-fleet.sh` sets
      `LEGION_WORKER_TELEMETRY_PORT=9101` and publishes file_sd targets
      (retracted by `stop-sim-fleet.sh`), Prometheus grows a
      `legion-sim-workers` job, and the campaign board grows a sim-fleet
      row. Like the rest of the rig that plumbing is unexercised until a
      real campaign runs. [docs/metrics.md](../docs/metrics.md) § Worker,
      [docs/worker-simulation.md](../docs/worker-simulation.md) § Metrics,
      [plan](plans/worker-simulation-plan.md),
      [calls](calls/worker-simulation-plan_calls.md).

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
- [x] Holistic polish (2026-09-02): every index and detail page polls (one
      `usePoll` helper, visibility-paused, last data kept across a transient
      failure); **Stop job**, **Cordon / Drain / Uncordon**, and a **Scale**
      panel in the UI; a lost-contact banner with retry; `?` shortcut sheet,
      recents and actions in ⌘K, per-page tab titles, a favicon, a real
      not-found page; Jobs status chips backed by new `ListJobs.status` +
      `status_counts` fields (also `legion job list --status` and the MCP
      tool); the Workers index shows liveness as the status with health and a
      "Heartbeat" column; route-level code splitting (initial bundle
      1.6 MB → 0.58 MB); a `ui-build` CI job with a generated-client drift
      check ([plan](plans/ui-holistic-polish-plan.md),
      [calls](calls/ui-holistic-polish-plan_calls.md),
      [docs](../docs/ui-architecture.md)). Closed the job-updated-at
      deferrals (created/updated on the job header, per-page last-column
      label) and the stale "gen has drifted" item — the client was already
      current, and CI now keeps it so.

## AWS rig: mitigations in place for the node-deregistration and plugin-build gaps

Both gaps are still open (see "No way to remove a node record" and "Share one
target dir across plugin builds" above); this records what `benchmarks/aws/`
now does about them so nobody re-derives it at 02:00.

- **Node records.** The rig no longer pretends the node table is a fleet
  count. `scripts/env.sh` gained `node_census` / `assert_fleet`: an exact
  assertion — right row count, every row `active` in `node_health` and
  `available` in `node`, every id from this fleet's `sim-<slice>-` prefixes —
  replacing the `registered >= NODES` gate in both `run-step.sh` and
  `start-sim-fleet.sh`, and printing the observed census
  (`total=4000 active=1000 lost=3000 …`) when it refuses.
  `scripts/reset-control-plane.sh` (stop the fleet, then reinstall, which
  wipes) is the only reliable cleanup, and `run-campaign.sh` runs it before
  every campaign, which also closes the `packing-density.sh` ->
  `run-campaign.sh` boundary in `phase1.sh`. Node-name prefixes remain
  reused, deliberately, for the reason stated above.
- **Fleet churn.** A cell now avoids a fleet restart when the running fleet
  already matches (system, scenario, node count) — a saturation sweep's
  points share one fleet for exactly this reason — waits for the eval
  backlog to drain before measuring, and archives the depth it started from
  in the cell's `pre-cell.json` and `pre_cell_eval_backlog` in the fairness
  record, so a contaminated cell is identifiable from the archive alone.
- **Plugin builds.** `build-and-upload.sh` now fingerprints `plugins/` plus
  the rustc version, and when the fingerprint matches the bundle already in
  S3 it builds with `LEGION_SKIP_PLUGIN_BUILD=1` and reuses that bundle
  (`--force-plugins` overrides). That removes the repeated ~10.9 GB /
  6x-470-crate rebuild from every campaign, but it is a cache, not a fix:
  the first build on a fresh box and any change under `plugins/` still pays
  in full, so the shared-target-dir item above stands.
