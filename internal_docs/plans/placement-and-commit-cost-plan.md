# Plan: Placement Quality and Commit Cost (review SCH2–SCH5, SCH8, AI7)

> **Judgement calls:** [`calls/placement-and-commit-cost-plan_calls.md`](../calls/placement-and-commit-cost-plan_calls.md)

**Status: SHIPPED** (2026-11-08): phases 1–5 below, measured in §12.
Deliberately deferred:
the node-row half of SCH5 (§6.2), spot-interruption awareness for gangs
(§6.6), multi-level topology (a gang that spans the fewest racks inside one
zone), and a balanced-allocation weight an operator can tune. The first
three have TODO entries; the weight is an open question (§11).

Public docs: [scheduler-and-eval-loop.md](../../docs/scheduler-and-eval-loop.md)
(claims, scoring, plan records), [gang-scheduling.md](../../docs/gang-scheduling.md)
(topology), [jobs-tasks-and-scheduling.md](../../docs/jobs-tasks-and-scheduling.md)
(policies), [late-binding-plan-apply.md](../../docs/late-binding-plan-apply.md),
[diagnosing-scheduler-performance.md](../../docs/diagnosing-scheduler-performance.md).

## 1. Problem statement

The [2026-11-07 architecture review](../architecture-review-2026-11-07.md)
found five scheduler-path problems and one AI-workload gap. Each is concrete
today:

- **SCH2 — `plan_submission` grows forever.** Every plan writes a row
  carrying every placement's ranked candidates as JSON. Nothing deletes it.
  The row rides the raft log and every snapshot: ~86M rows a day at 1k
  plans/s.
- **SCH5 — the apply round-trips its own input through SQLite.** Inside the
  serialized commit the plan is serialized to JSON, INSERTed, SELECTed back
  and re-parsed, and every candidate node the walk tries costs a node-row
  SELECT plus an attribute-JSON parse.
- **SCH3 — claiming slows as the queue grows.** Under the claims mutex each
  claim counts every pending eval, and each claimed eval runs a duplicate
  lookup whose `COALESCE(job_id, '')` cannot use an index. Measured on the
  Rust store at 50k pending evals: **115.7 ms per batch of 16**, and every
  enqueue paid an O(pending) count inside its write (1.58 ms at 50k).
- **SCH4 — `bin_pack` and `spread` count allocations.** The score is the
  node's allocation count, min-max normalized over a possibly truncated
  candidate list. A 32-CPU allocation weighs what a 0.1-CPU one does. On a
  two-node cluster where one node holds one 12 GiB allocation and the other
  three 256 MiB ones, `bin_pack` puts the next 2 GiB task on the node with
  three (it "looks fuller") and a later 14 GiB task then fits nowhere;
  `spread` puts the next task on the node that is already 75% full.
- **SCH8 — info logs per plan and per claim**, against AGENTS.md's tracing
  guidance.
- **AI7 — gangs ignore topology.** A training gang has quorum and
  rendezvous variables, but nothing keeps its members inside one rack or
  NVLink domain, so the collective runs over the slowest links in the
  cluster. Nothing reacts to spot-interruption notices either.

## 2. Prior art

- **Nomad.** Plans are submitted to the leader's plan queue in memory and
  applied by one plan applier; the plan itself is not a durable record.
  Rejected/partial results go back to the worker. The eval broker is an
  in-memory priority queue keyed by job, which is what makes duplicate
  collapse O(1). Scoring: BinPack/BestFit v3 scores `20 - (10^freeCpu +
  10^freeMem)` against node capacity; job anti-affinity is averaged in.
- **Kubernetes scheduler.** `NodeResourcesFit` with `MostAllocated`,
  `LeastAllocated` (default), and `RequestedToCapacityRatio`, plus
  `NodeResourcesBalancedAllocation` (penalizes the spread between per-resource
  fractions). All score *requested* resources against *allocatable*
  capacity, per node, independent of the other candidates.
- **Kueue topology-aware scheduling (TAS).** A `ResourceFlavor` names a
  topology (levels such as block → rack → host). A workload asks for
  `kueue.x-k8s.io/podset-required-topology: <level>` (all pods in one domain
  of that level, or wait) or `podset-preferred-topology` (try one domain,
  else go up a level). Domain choice is best-fit by default: the domain with
  the least free capacity that still fits the pod set.
- **Volcano / Slurm.** Volcano's network-topology-aware plugin and Slurm's
  `--switches` express "within N hops", again as required vs preferred.
- **Spot interruption.** Karpenter and the AWS node-termination handler
  cordon and drain on the two-minute notice; Kueue/JobSet restart the whole
  pod group. None of them treat a gang specially beyond "drain triggers the
  group's failure policy".

What we take: Kueue's required/preferred split and best-fit domain choice;
K8s-style per-node utilization scoring with a balanced-allocation term;
Nomad's "a plan is not a durable record". What we reject: a multi-level
topology tree (one attribute key is enough for the first cut — §6.5), and an
in-memory eval broker (the claim path only needs its statements bounded,
which indexes do without a second source of truth).

## 3. Jobs to be done

**Cluster operator**
- When the cluster has been up for a month, I want the replicated state and
  its snapshots to be the size of what is running, so a manager restart or a
  new member installs in seconds, not minutes.
- When a burst of ten thousand submissions lands, I want the scheduler to
  work through the queue at the same rate at the end as at the start, so a
  burst drains instead of snowballing.
- When I read manager logs, I want one line per state change, not one per
  plan, so the line I need is findable.

**Job author / app developer**
- When I run a mix of big and small services under `bin_pack`, I want small
  services packed together and big nodes kept whole for big services, so a
  large deploy does not fail on a cluster that has room in total.
- When I train on GPUs, I want all my workers placed in one rack or NVLink
  domain, or told plainly that no domain has room, so my collective runs at
  the speed the hardware promises.

**On-call responder**
- When a plan was rejected, I want the reason still on record, so I can
  classify the rejections without a counter that does not exist yet.

**Autonomous agent**
- When I dry-run a gang job, I want the plan to say which domain it would
  use, or why none fits, in structured fields, so I can pick a smaller size
  or a different key without guessing.

*Not in scope:* the security/platform-admin persona; nothing here changes a
boundary.

### User scenarios

1. A 500-node cluster runs 20k services. An operator restarts a manager; the
   snapshot it installs no longer carries 30 days of plan rows.
2. `legion job run trainer.ncl` with `gang = { topology = { key = "rack" } }`
   and `count = 8`: the eight workers land in rack-3, the one rack with room
   for eight; with no such rack the job waits and `job plan` says "the best
   rack (rack-3) fits 6 of 8".
3. A mixed `bin_pack` cluster: small services fill partly-used nodes first;
   a 14 GiB service later finds a whole node.

## 4. Goals and non-goals

Goals:
- `plan_submission` bounded by construction, with rejected plans kept for
  diagnosis (bounded), applied plans never written.
- The apply transaction consumes the plan as a struct; no JSON round trip.
- Every statement under the claims mutex O(batch), pinned by query-plan
  tests.
- `bin_pack`/`spread` scores are per-node resource utilization, independent
  of the candidate list.
- Gang `topology` with `required` and `preferred` modes, reported by
  `job plan` and the blocked-job report.

Non-goals:
- An in-memory eval broker (SCH3's "or" option): bounded statements fix the
  measured cost without a second queue to keep consistent with raft.
- Pipelining the lane or the capture mutex (SCH1): another workstream.
- Multi-level topology, `maxSkew`-style soft spread (SCH7), device topology
  (PLG2).
- Spot-interruption draining (§6.6: deferred, with a TODO).
- Changing the scheduling-plugin context (SCH6): plugins keep the
  count-based context they have.

## 5. Background — what exists today

- Plan path: `Scheduler::plan_and_apply_internal` (`crates/manager/src/manager/scheduler.rs`)
  builds placements and calls `Store::submit_and_apply_plan(_with_candidates)`
  (`crates/manager/src/storage/store.rs`), which group-commits into
  `submit_and_apply_plan_on`: `insert_plan_submission_on` (INSERT … RETURNING
  id) then `apply_plan_submission_on`, which SELECTs `placements, mode,
  evictions, target_instance` back by id and parses them
  (`parse_plan_submission_placements`). Applied plans `UPDATE … status =
  'applied'`; rejected ones `status = 'rejected'` with `rejection_reason`.
- Readers of `plan_submission`: only the apply itself, tests, and the
  rejection-classification query in
  `docs/diagnosing-scheduler-performance.md`. The staged two-step API
  (`insert_plan_submission` + `apply_plan_submission`) is used only by tests
  and `benchmarks/micro/eval_critical_path_bench.rs`. Edge-boot ratification
  goes through the fused path.
- Node rows in the walk: `nodes_by_id` memo per plan; each miss runs
  `SELECT {NODE_JOINED_ATTRS}` and `node_from_row`, which parses three JSON
  columns. The read model's `Arc<Node>` is maintained by the changeset
  projection after commit (`project_changeset` → `cache_node`), and
  `nodes_epoch` is a process-local counter of read-model node mutations.
- Claims: `store/eval_claims.rs` `claim_up_to`; indexes on `eval` are
  `(status, priority DESC, id)`, `(status, ready_at, priority DESC, id)`,
  `(status, job_id)`, `(status, node_id)`. Without statistics SQLite chooses
  `(status, ready_at, …)` for the head query and sorts every ready row.
- Scoring: `PlanScoringCache` / `NodeScoringState` (`scheduler.rs`),
  `score_node_with_cache` with `cached_load_bounds` (min-max over
  `assigned_count`). `common_resources` holds per-node *free* memory, cpu,
  cores, allocation slots — but not capacity. Capacity is born in
  `ResourcePool::from_node` (`crates/common/src/resources.rs`).
- Gangs: `GangConfig { min, placement_timeout, on_member_loss }`
  (`crates/common/src/structs/job.rs`), placed in `build_plan` through
  `place_task_allocations` over `base_eligible` (gangs never truncate), and
  reported as `GangReport` (`scheduler.rs`, `proto/legion.proto`).

## 6. Proposed design

### 6.1 Plan records (SCH2)

`plan_submission` stops being a log of every plan:

| Path | Before | After |
| --- | --- | --- |
| Fused submit-and-apply (every production plan), applied | row INSERTed, then `status = 'applied'` | **no row** |
| Fused, rejected | row with `status = 'rejected'` | row with `status = 'rejected'`, written at rejection |
| Staged (`insert_plan_submission`), applied | `status = 'applied'` | **row deleted** by the apply that consumes it, in the same op |
| Staged, rejected | `status = 'rejected'` | unchanged |

Retention is an id window: every insert deletes rows whose id is more than
`PLAN_SUBMISSION_RETAINED` (1,000) below its own, at most 64 per insert, in
the same replicated op. With applied plans no longer written, the window is
the last ~1,000 rejections and staged plans. A staged plan that is never
applied (an orphan) leaves the window like any other row. On startup and on
scheduler-lane leadership the leader also sweeps everything outside the
window, plus `queued` rows older than ten minutes, in bounded batches
(`Store::prune_plan_submissions`), which also drains the history an existing
data directory carries.

The diagnostic query in diagnosing-scheduler-performance.md keeps working;
it now sees the last 1,000 rejections instead of all of them.

### 6.2 Apply without the round trip (SCH5)

The fused path hands the walk the request's placements as a slice: no
`to_json`, INSERT, SELECT, or parse. Format-1 node pairs become singleton
candidate lists in memory. JSON is produced only for a rejection's record.
The staged path keeps reading its row (it was persisted across ops).

Node rows: the walk keeps reading and parsing the node row inside the
transaction — that read is the apply-time fence against cordons and
capacity changes that bump no allocation epoch. The read model's
`Arc<Node>` guarded by `nodes_epoch` would not be sound, and the sound
alternative (verify the read model's node against a keyed hash of the row's
bytes) is **deferred** to a TODO entry (calls #2).

### 6.3 Claims (SCH3)

- Duplicate lookup: `job_id IS ?` (and the same for job config and node)
  instead of `COALESCE(job_id, '') = ?`. Every writer stores an absent scope
  as NULL; `insert_eval` now normalizes blanks too.
- New partial indexes over pending rows: `idx_eval_pending_scope (type,
  job_id, job_config_id, node_id)` and `idx_eval_pending_kind_order (type,
  priority DESC, id)` for the throttled non-planning claim.
- `+ready_at` in the head and duplicate queries keeps SQLite off the
  `(status, ready_at, …)` index, so the head walks `priority DESC, id` and
  stops after the batch.
- The per-claim `COUNT(*)` becomes a count capped at the fair share's
  saturation point (`workers × max + held`). The queue-depth gauge is set
  exactly from it when the queue is shallower than the cap, and from a full
  count at most once a second when deeper; enqueue adds one.
- Query plans pinned by `eval_claims_test::claim_queries_use_their_indexes`.

### 6.4 Resource scoring (SCH4)

For each candidate, after placing the task:

```text
util_d   = 1 - (free_d - request_d) / capacity_d      for each dim d the task requests
                                                       with a finite capacity > 0
                                                       (memory, cpu, cores, allocation slots)
dominant = max_d util_d                                (0 when no dim qualifies)
balance  = 1 - (max_d util_d - min_d util_d)

bin_pack = dominant + 0.05 × balance
spread   = (1 - dominant) + 0.05 × balance
score    = affinity + bin_pack_weight × bin_pack + spread_weight × spread
           - anti_affinity × (collisions + 1) / count   (0 with no collision;
                                                   count-scaled after measurement, calls #1)
           + locality
```

Absolute fractions, so the score of a node does not depend on which other
nodes made the candidate list (index truncation, `nodes_considered`,
`score_sample`). Capacity rides on `ResourcePool` (`capacity` field set by
`from_node`), so the read model's free pools carry it at no extra lookup.
Policy plugins keep the count-based `SchedulingContext` they have (SCH6 owns
that seam).

### 6.5 Topology-aware gangs (AI7)

```nickel
gang = {
  min = 8,
  topology = { key = "rack", mode = "required" },   # or "preferred"; mode defaults to "required"
},
```

`key` is a node attribute name, looked up like `count.per` and `distinct`
(config, then fingerprint, then dynamic attributes). A **domain** is the set
of eligible nodes sharing one value of it.

- **required**: every member of the gang is placed inside one domain, or the
  gang waits (holding nothing, as any gang does). Nodes without the
  attribute are not candidates. A top-up of a running gang stays in the
  domain its running members occupy.
- **preferred**: try one domain the same way; if none fits the quorum,
  place across the fewest domains that can hold it, then across every
  eligible node.

Domain choice: estimate per domain how many replicas fit (free capacity per
node over the request, summed), then try domains that fit every replica,
smallest estimate first (best fit, as Kueue), then domains that fit the
quorum, largest first. Each try is a full placement inside the domain, and a
failed try is rolled back. At most eight domains are tried per gang per
plan.

`job plan` / `job info`:

```console
$ legion job plan trainer.ncl
Gang:
  worker: would place in rack=rack-3 (8 of 8; 4 racks have eligible nodes)
$ legion job plan trainer.ncl   # no rack has room
  worker: would NOT place — 6 of the 8 it needs fit (...); topology rack (required): no single rack fits the gang, the best (rack-3) fits 6
```

`GangReport` gains `topology_key`, `topology_mode`, `topology_domain`,
`topology_domains` (proto fields 13–16, JSON `topologyKey`, …).

Validation: `topology.key` must be a non-empty string; `mode` one of
`required`, `preferred`; unknown fields rejected, as for the rest of `gang`.

### 6.6 Spot interruption (AI7, deferred)

Evaluated and deferred. A spot notice today reaches Legion, if at all, as a
node drain (the `aws-asg` plugin or an external handler). A drain is already
a gang **member loss**, and `onMemberLoss = "restart"` already stops and
re-places the gang as a unit; the drained node is excluded. What is missing
is *proactive* handling: treating a notice as "this node will be gone in two
minutes" so the gang restarts elsewhere before the loss rather than after,
and not placing a fresh gang onto nodes with a pending notice. That needs a
node condition ("interruption pending") that does not exist, a source for it
(an IMDS watcher in the worker, or the ASG lifecycle hook in the plugin), and
a decision about checkpoints. It is a plan of its own; see the TODO entry.

### 6.7 Logs (SCH8)

`plan_and_apply` start/complete, the per-eval allocation-delta dumps, the
plan-apply transaction line, and "Claimed evals" move to `debug`, with
counts in place of delta maps. A plan slower than one second logs one `info`
line.

## 7. Proto, struct, config, and persistence changes

- `proto/legion.proto` `GangReport`: `topology_key = 13`, `topology_mode =
  14`, `topology_domain = 15`, `topology_domains = 16`.
- `GangConfig` gains `topology: Option<GangTopology { key, mode }>` and loses
  `Copy`. `configs/jobs/maximal.ncl` shows it.
- `PlanSubmission.id` becomes `Option<i64>` (`None` when no row was kept).
- `ResourcePool` gains `capacity` (memory, cpu, cores, allocation slots).
- Migration (DDL only, appended): the two partial `eval` indexes.

## 8. Implementation phases

Each ships alone.

1. **SCH8 logs.** Independent.
2. **SCH3 claims.** Indexes, predicates, capped count, gauge; query-plan
   test; measurement module `tests/integration/eval_claim_scale.rs`.
3. **SCH2 + SCH5 plan records and apply.** Struct pass-through, rejection
   records, staged delete, window prune, leader sweep. (The node-row half of
   SCH5 is deferred; calls #2.)
4. **SCH4 scoring.** Capacity on pools, utilization score, docs; tests in
   `tests/integration/resource_scoring.rs`.
5. **AI7 topology.** Jobspec field, placement, report, docs, maximal.ncl.

## 9. Testing plan

- Unit: `eval_claims_test` (query plans, NULL-scope collapse); store tests
  for "applied plans leave no row", "staged applied plan is deleted",
  "rejections are recorded and the window bounds them", "orphans are swept";
  scheduler tests for utilization scoring independent of the candidate list;
  gang topology unit tests (required fits/waits, preferred falls back,
  top-up stays in the domain, report fields).
- Integration: `resource_scoring::` (mixed workload no longer strands),
  `gang_scheduling::`, `scheduler_integration::`, `plan_contention::`,
  `eval_loop::`, `blocked_evals::`, `index_truncation_spread::`,
  `scheduling_index::`, `spread_under_raft::`, `raft_lanes::`,
  `raft_ownership_guard::`.
- Measurement, before/after on this host: `eval_claim_scale::` (claims at
  50k pending), `scheduler_benchmark_slo::` (all scenarios, local disk).

## 10. Rollout and backward compatibility

No compatibility shims (AGENTS.md). What changes for an existing cluster:
- Placement changes under both built-in policies (intended). Jobs are not
  re-placed; new placements follow the new score.
- `plan_submission` stops recording applied plans; history is swept on the
  next scheduler-lane leadership.
- `PlanSubmission.id` is optional in the Rust API.
- `gang.topology` is new; a manager without it rejects the field as unknown.

## 11. Open questions

- Should the balanced-allocation term be a `schedulerSettings.weights`
  knob? Lean: not until someone needs to turn it; it is a tie-break.
- Multi-level topology (prefer one rack, else one zone)? Lean: a list of
  keys later (`keys = ["rack", "zone"]`), when a user asks.

## 12. Measurements (2026-11-08)

Debug build, local-disk store, a 4-core VM shared with five other builds
(so expect ±10% between runs); base is `a447c87`.

**Claims (SCH3)** — `tests/integration/eval_claim_scale.rs`, 50,000 pending
evals of distinct scopes:

| | base | after |
| --- | ---: | ---: |
| Claim a fair-share batch of 16 | 115.7 ms | 0.48 ms |
| Throttled (non-planning) claim, none queued | 6.05 ms | 0.20 ms |
| One enqueue at depth | 1.58 ms | 0.18 ms |
| Filling the queue (50 ops of 1,000) | 44.8 s | 7.4 s |

**Scoring quality (SCH4)** — `tests/integration/resource_scoring.rs`:

| | base | after |
| --- | --- | --- |
| `bin_pack`, 2 GiB task next to one 12 GiB alloc vs three of 256 MiB | lands beside the three small ones; the 14 GiB job that follows fits nowhere | tops off the 12 GiB node; the 14 GiB job places |
| `spread`, 1 GiB task, same cluster | lands on the 75%-full node | lands on the emptier node |
| `index_truncation_spread` allocations per slice | 39 / 29 / 41 / 51 | 34 / 40 / 45 / 41 |

**Scheduler SLO benchmark, first cut (flat 0.2 anti-affinity penalty)**
(`scheduler_benchmark_slo::`, all scenarios, medians of three interleaved
base/after runs; evals/s, then build and apply phase means in µs). Superseded
by the follow-up below, which is what shipped:

| scenario | evals/s base | after | build base | after | apply base | after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| batch-burst | 75.2 | 69.0 | 4869 | 5407 | 7082 | 7669 |
| churn-large | 77.4 | 67.4 | 4406 | 5194 | 4727 | 5236 |
| churn-medium | 119.5 | 129.8 | 1857 | 1981 | 5159 | 4453 |
| dense-fleet | 32.5 | 35.1 | 9786 | 9541 | 18629 | 16648 |
| high-fanout | 134.6 | 113.8 | 2678 | 2388 | 3836 | 5292 |
| mixed-heterogeneous | 130.4 | 115.3 | 2920 | 3288 | 3721 | 4275 |
| steady-small | 173.0 | 174.8 | 1374 | 1198 | 3777 | 3943 |
| unit-fleet | 199.6 | 227.2 | 953 | 823 | 3584 | 3139 |
| web-fleet | 154.9 | 164.4 | 2062 | 1974 | 3763 | 3446 |

The four slower scenarios place many replicas of one task under `bin_pack`.
With the automatic anti-affinity weight set to 0 (stacking allowed), base and
after match — batch-burst 110.9 vs 112.0 evals/s, high-fanout 120.9 vs 120.4
(`scheduler_scale_benchmark::`, medians of three interleaved runs) — so the
new score and apply paths cost nothing measurable. The difference is
placement shape: the 0.2 anti-affinity penalty now outweighs packing for
small replicas, they spread over more nodes, and the apply pays a node-row
parse and capacity recheck per distinct node (calls #1; the SCH5 node-row
TODO would cut that cost).

**Follow-up: count-scaled anti-affinity.** The penalty became
`0.2 × (collisions + 1) / desired count` (calls #1). Same method, fresh base
binary from `a447c87`; the first table is the SLO gate (medians of three
interleaved rounds), the second the four scenarios that had regressed
(`scheduler_scale_benchmark::`, medians of five interleaved rounds):

| scenario | evals/s base | after | build base | after | apply base | after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| batch-burst | 80.3 | 108.6 | 4708 | 2812 | 6559 | 5553 |
| churn-large | 79.1 | 75.4 | 4265 | 4817 | 4546 | 4463 |
| churn-medium | 144.6 | 139.2 | 1561 | 1829 | 4173 | 4185 |
| dense-fleet | 35.3 | 35.7 | 8954 | 9309 | 17198 | 16438 |
| high-fanout | 132.1 | 116.2 | 2818 | 2437 | 3861 | 5064 |
| mixed-heterogeneous | 126.5 | 134.1 | 2935 | 2870 | 3928 | 3564 |
| steady-small | 191.8 | 131.9 | 1234 | 1591 | 3399 | 5265 |
| unit-fleet | 205.7 | 214.6 | 884 | 867 | 3496 | 3320 |
| web-fleet | 134.2 | 157.2 | 2403 | 2081 | 4221 | 3604 |

| scenario (5 rounds) | evals/s base | after | build base | after | apply base | after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| batch-burst | 71.5 | 105.4 | 5011 | 3037 | 7647 | 5589 |
| churn-large | 77.5 | 78.1 | 4196 | 4430 | 4604 | 4444 |
| high-fanout | 132.8 | 126.9 | 2776 | 2178 | 3848 | 4688 |
| mixed-heterogeneous | 125.0 | 124.3 | 2914 | 3117 | 3993 | 3865 |

Single runs swung by up to ±30% on this host (steady-small's base alone
ranged 137–192 evals/s across the three rounds, and its first round was
192 base vs 198 after), so the three-round medians for steady-small and
high-fanout are within noise. What is structural: many-replica tasks pack
again (batch-burst +47%), and two-replica tasks (high-fanout) still spread,
paying a higher apply per plan for the extra nodes.

No release micro-benchmark (`benchmarks/micro/scheduler_bench.rs`) was run:
a release build of the crate on this shared host would have held the build
lock for every other engineer for the better part of an hour.

## 13. Acceptance criteria

1. No per-plan or per-claim `info` lines at steady state.
2. Claim batch at 50k pending under 1 ms on the measurement host; query-plan
   test green.
3. A burst of applied plans leaves `plan_submission` empty; rejections leave
   at most 1,000 rows; orphans swept on leadership.
4. `resource_scoring::` green; SLO benchmark within noise of the base.
5. A `required` topology gang never spans two domains (test), waits when no
   domain fits, and `job plan` reports the domain.
