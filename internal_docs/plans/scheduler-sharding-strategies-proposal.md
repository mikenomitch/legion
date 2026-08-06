# Scheduler Sharding Strategies: Scaling the Control Plane by Adding Raft Groups (proposal)

**Status: PROPOSAL** — an exploration document, like
[`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md):
it evaluates the ways Legion's scheduler could be sharded across more raft
groups ("lanes", shards, or whole clusters) so that adding groups adds node
capacity and eval/allocation throughput. No code ships from this document;
its output is a ranked map of the option space, the invariants any option
must preserve, and a gated graduation path. It extends — and in places
corrects — the deferred "Phase 8: scheduler-lane key-range sharding" sketch
in [`multi-raft-persistence-plan.md`](multi-raft-persistence-plan.md) §Phase 8.

> **Judgement calls:** [`../calls/scheduler-sharding-strategies-proposal_calls.md`](../calls/scheduler-sharding-strategies-proposal_calls.md)

## 1. Problem statement

The question, as asked: *can the scheduler be sharded such that we add more
and more raft clusters or lanes and get more nodes or more eval/alloc
throughput?*

The short answer this document defends: **yes for throughput, partially for
nodes, and only one family of sharding is sound under Legion's replication
design.** "More lanes" in the current write-class sense buys nothing further;
node-ownership sharding of the scheduler lane multiplies the real serial
resources; and past a point, more raft groups inside one cluster cannot help
and the answer becomes more clusters (federation, already planned).

### What is actually serial today

A Legion cluster's placement throughput is bounded by three serial
resources, in the order the benchmarks found them:

1. **One raft group for all placement writes.** Every eval claim, plan
   apply, and completion rides the scheduler lane, and each lane drains
   through a single dedicated pipeline thread
   (`src/storage/raft/driver.rs:3093`). AWS phase 2 measured 3.07 ms of
   `pipeline_wait` per scheduler-lane op against 0.09 ms on the status lane
   ([`../aws-phase3-findings.md`](../aws-phase3-findings.md)); phase 3
   showed `apply` (containing two quorum rounds on two lanes) at 67% of
   plan time and throughput ~7× below what planning cost permits.
2. **One active planner.** Eval workers idle on every manager that does not
   lead the scheduler lane (`src/manager/daemon.rs:1798`). Follower CPU
   contributes nothing to placement.
3. **One SQLite writer for every group.** `CAPTURE_MUTEX` is a
   process-global static held for the full duration of every capture and
   every apply, across *all* lanes (`src/storage/raft/capture.rs:133`,
   `:200`, `:519`; acknowledged in `driver.rs:2750`). Lanes isolate raft
   logs, replication pipelines, and elections — they do **not** isolate
   SQLite write work. This is the ceiling that survives any amount of
   group-level sharding while the groups share one state DB.

### Where the measured ceiling has moved

| Date | Measurement | Number |
| --- | --- | --- |
| 2026-07-28 | lanes vs single, scheduler p99 under cross-lane flood | 3.37 ms vs 110.45 ms; 4–5× throughput retained ([docs/multi-raft.md](../../docs/multi-raft.md)) |
| 2026-08-04 (phase 3) | alloc/s @5k nodes, lanes, 4 eval threads | 80.35/s; `apply` = 67% of 41.42 ms plan time |
| 2026-08-05 (phase 4) | eval-thread sweep 4 → 32 | 177 → 252 alloc/s locally; rig found driver-bound |
| 2026-08-06 (phase 5) | after claim/complete fusion, batched enqueue, group commit, statement cache | **≥1,323–1,625 alloc/s, driver-bound at every fill level** |
| 2026-08-06 (storm) | internally-generated replacement (not driver-bound) | ~277 alloc/s sustained, 0.006 s placement gap |

Two lessons for this document. First, **in-group levers keep paying**: a
16–20× throughput improvement came from batching and fusing rounds on the
*same* single pipeline, not from adding one. Second, **the true post-batching
single-group ceiling is currently unmeasured** — every deep cell in phase 5
was floor-limited by the load generator. Sharding work sequenced before that
number exists would be built against a wall nobody has hit.

### The node axis is a different problem

"More nodes" is no longer primarily a raft-write problem. Steady-state
heartbeats never touch raft ([docs/node-liveness.md](../../docs/node-liveness.md));
cadence and per-type lost windows size the RPC load; digests decouple
manager count from fleet size. What still grows with fleet size:

- **The read model, on every manager.** `SchedulingReadModel` is an
  unconditional full mirror — every node, every allocation, every task
  config (`src/storage/store.rs:227`, built by four unfiltered table scans
  at `store.rs:2362`, refreshed on every replica after every apply,
  `src/storage/raft/state_machine.rs:590`). Sharding raft groups does not
  shrink it; only running fewer nodes per cluster does.
- **Snapshot cost.** `single` topology snapshots are whole-DB `VACUUM INTO`
  images; lane snapshots are table-scoped dumps. Both scale with state size.
- **Planner universe.** Already bounded by scheduling indexes and
  `score_sample`; node-ownership sharding (M4 below) additionally divides
  it by shard count.
- **Churn-driven eval load** — which is the throughput axis again.

So: eval/alloc throughput can scale in-cluster by sharding; node count
scales in-cluster until per-manager memory and snapshot size bite, and past
that the answer is more clusters, not more groups.

## 2. Prior art

- **Nomad** — one raft group, but *many* schedulers: every server runs
  scheduler workers that plan optimistically against local state; a single
  leader **plan queue** serializes apply with conflict rejection. C1M's
  3,750 placements/sec aggregate is this shape. Regions are separate
  clusters with request forwarding — Nomad never shards its raft group.
  *Take: optimistic parallel planning against replicated state (M3);
  cluster-per-region as the outer axis. Reject: nothing — but note its
  single apply arbiter is exactly the bottleneck Legion already measured.*
- **Kubernetes** — one etcd group, one active kube-scheduler; scale
  practice is many clusters ("cells"). ByteDance's **Gödel** and the
  multi-scheduler KEP shard the *scheduler* over shared state with
  optimistic concurrency (~5k pods/s reported) — evidence both that the
  single scheduler is everyone's eventual bottleneck and that shared-state
  optimism works at scale. *Take: the shared-state optimistic shape exists
  and is credible. Reject: etcd's whole-cluster single log (Legion's lanes
  already beat that).*
- **Omega** (Google, 2013) — the canonical shared-state design: multiple
  schedulers, full-state replicas, optimistic commit with cell-state
  arbitration. Its published weakness is conflict rate under contention.
  *Take: Legion's partitioned epochs + late binding are already an Omega-ish
  arbiter. Reject: multi-writer commit of the same node's capacity from
  independent logs — Omega had a single cell-state arbiter; independent
  raft groups have none (see M2).*
- **Borg** — cells of ~10k machines, one logically-central Borgmaster per
  cell; Google scales by adding cells, not by growing one. *Take: the cell
  as the honest outer unit (M6/federation). Reject: nothing.*
- **TiKV / CockroachDB** — true multi-raft: thousands of range-keyed groups
  per node, leadership deliberately spread, dynamic split/merge/rebalance
  run by a placement driver. The generality is real and so is the cost: the
  rebalancer is a whole subsystem, and every operator page grows a "regions"
  dimension. *Take: many groups over one voter set and one transport
  (Legion's engine already multiplexes by `group_id`,
  `proto/legion.proto:5918`); leadership spread as the throughput move.
  Reject: dynamic range management as a v1 requirement — a scheduler's hot
  rows are classifiable up front; a KV store's are not.*
- **YARN federation / Mesos** — a router or resource-offer layer over
  sub-schedulers that own capacity slices. *Take: the observation that
  hierarchical routing degenerates into either M4 (static ownership) or M6
  (clusters) — so it is not a distinct method worth building.*

## 3. Jobs to be done

**Cluster operator**
- When my fleet or deploy rate outgrows what one scheduler pipeline can do,
  I want to raise a number in the manager config (or add managers) and get
  more placement throughput, so I don't have to re-architect into multiple
  clusters just for speed.
- When the control plane is near its limit, I want the status surface to
  tell me *which* lane or shard is hot and what to add, so capacity planning
  is a dashboard read, not a forensic exercise.
- When one part of the cluster is having a bad day (a pool in a loss storm),
  I want the rest of the cluster's scheduling untouched, so blast radius is
  something I chose, not something I discover.

**Job author / app developer**
- When the operators change how the control plane is sharded, I want my
  jobspec and CLI to behave exactly as before, so I never learn what a
  "shard" is. *(This is an invariant, not a feature: no sharding method
  below may surface in the jobspec beyond the pools that already exist.)*

**On-call responder**
- When placements for one team stall, I want `legion raft status` and the
  metrics to name the shard and its leader, so the incident starts at the
  right machine.

**Maintainer**
- When choosing the scaling axis, I want the options ranked with their
  correctness costs stated, so we spend on the one that preserves the
  soundness argument the replication design is built on.

### User scenarios

1. *The batch pool eats the cluster.* A 40k-node fleet where one team's
   batch pool generates 80% of eval traffic. The operator pins that pool to
   its own scheduler shard; the service pools' p99 stops moving when batch
   storms. (M4.)
2. *Deploy-rate wall.* A CI-driven org whose deploy bursts outrun the
   scheduler lane. The operator raises `scheduler.shards` from 1 to 4 at
   the next maintenance window (v1: backup/restore; v2: online) and
   placement throughput roughly triples. (M4, and the honesty about its
   ceiling.)
3. *Two regions.* An operator wants EU workers scheduled in EU. No shard
   topology fixes WAN quorum; they run two clusters and peer them.
   (M6 — out of scope here, planned in
   [`multi-cluster-federation-plan.md`](multi-cluster-federation-plan.md).)

## 4. Goals and non-goals

### Goals

- Enumerate every credible sharding axis — more write-class lanes, job-keyed
  shards, node-owned shards, cross-manager planning, sharding the other
  lanes, federation — with architecture, performance model, trade-offs, and
  operator experience for each.
- Name the invariants any method must preserve, so the eventual
  implementation plan inherits a checklist rather than re-deriving it.
- Rank the methods and give a measurement-gated graduation path.

### Non-goals

- Shipping anything from this document.
- Replacing SQLite as the state-machine store (settled: architecture review
  D3) or abandoning row-changeset replication.
- Cross-cluster scheduling (federation plan's explicit non-goal: "no global
  task scheduler").
- TiKV-style dynamic range splitting/merging as a v1 requirement.

## 5. Background: what exists today

Recon current as of this branch; the load-bearing facts for what follows.

### 5.1 The lane engine is multi-group, and the lane set is frozen

- `Lane { Control, Scheduler, Status }` is a hard-coded enum
  (`src/storage/raft/mod.rs:41`), with `Lane::ALL` driving bring-up and
  exhaustiveness tests (`mod.rs:52`).
- `wire_name()` strings are simultaneously the gRPC `group_id`, the per-lane
  data-dir name, and the `raft_applied` key — documented frozen
  (`mod.rs:98`). The transport multiplexes all groups over one channel pool
  (`src/storage/raft/network.rs:270`, `driver.rs:2829`); one openraft core
  per lane (`driver.rs:2841`); one log DB per lane
  (`src/storage/raft/log_store.rs:86`); per-lane snapshots
  (`driver.rs:2856`). **The engine is genuinely N-group already** — adding
  groups is mechanical at this layer.
- Writer-minted commutative ids are lane-partitioned by residue:
  `ID_STRIDE = 8`, `id % 8 == lane.id_residue()` (`mod.rs:73`). **Eight
  residues exist**; three are taken. Any sharding that mints
  `scheduler_event` ids from more than five new groups changes `ID_STRIDE`,
  which is a format break.
- Topology is a two-valued string (`"single"` / `"lanes"`,
  `driver.rs:236`), fixed at first engine start via the `topology.json`
  stamp (`driver.rs:437`); changing it is backup → fresh cluster → restore
  (JC-43).

### 5.2 Single-writer table ownership is the soundness foundation

- `TABLE_OWNERS` maps every replicated table to exactly one lane (or
  `Commutative`/`LocalOnly`) (`src/storage/raft/ownership.rs:139`), with the
  rationale stated at `ownership.rs:5`: row-changeset replication from two
  independent logs into one table applies in different orders on different
  replicas — silent divergence.
- Enforced everywhere: `verify_changeset_ownership` rejects a cross-lane
  changeset *before proposal*, in every topology (`ownership.rs:456`,
  `:58`). Tests pin that the map is total (`:539`) and that per-lane
  snapshot table sets are pairwise disjoint (`:620`).
- Snapshot install is **table-scoped**: the manifest is validated against
  the lane's table set and the install wholesale-replaces those tables
  (`src/storage/raft/state_machine.rs:391`). Two groups sharing a table
  would clobber each other on install. Table ownership is a cluster-wide
  wire/disk contract ([docs/multi-raft.md](../../docs/multi-raft.md)
  §"Table ownership is a cluster-wide agreement").

### 5.3 The scheduler lane, concretely

- Scheduler-lane tables (`ownership.rs:185`): `allocation`,
  `allocation_epoch`, `allocation_epoch_partition`, `plan_submission`,
  `allocation_stop_notice`, `node_alloc_usage`, `instance`, `volume`,
  `eval`, `rollout`, `rollout_event`.
- The eval queue is replicated deliberately (JC-41 reversed JC-27: evals
  are not regenerable — an acknowledged `job stop`, a mid-flight rollout
  tick; `ownership.rs:94`). Every enqueue/claim/complete is a
  scheduler-lane quorum round, now heavily batched
  ([`eval-path-raft-cost-plan.md`](eval-path-raft-cost-plan.md)).
- Eval workers gate on per-lane leadership: `is_raft_leader(Lane::Scheduler)`
  at loop top and mid-batch (`daemon.rs:1798`, `:1901`), with an acquisition
  hook that recovers the queue on failover (`daemon.rs:1546`). The probe is
  per-lane (`store.rs:1427` → `driver.rs:989`) — the machinery for
  "workers for group k run on group k's leader" already exists in shape.

### 5.4 The three cross-cutting couplings

1. **`CAPTURE_MUTEX`** (§1): one process-global mutex serializes capture and
   apply across all groups (`capture.rs:133`). All lanes write one
   `store.sqlite` (`state_machine.rs:42`); SQLite is single-writer per file.
   More groups multiply *quorum pipelines*, not *state-DB write concurrency*.
   The lanes A/B numbers show why this still wins — quorum queueing (ms) was
   the dominant serialized cost, capture is local sub-ms work — and where it
   stops: when aggregate local write work saturates one thread.
2. **Composite operations span lanes** — job submit (control → status →
   scheduler), destroy, mark-lost, register, remove-node
   (`store.rs:8427`, `:8023`, `:6598`, `:6848`, `:6509`) — each half its own
   single-lane raft entry, completed under divergence by forwarding
   (`ForwardableLaneWrite`, `src/storage/raft/forward.rs:53` — notably
   including `enqueue_eval`) and by the lane reconcilers
   (`daemon.rs:1042`–`1118`). This standing-split resilience is what makes
   *deliberately* divergent leadership survivable — the enabler any
   leadership-spread design builds on.
3. **Co-location currently fights spread.** `ANCHOR_LANE = Lane::Control`
   and the convergence loop deliberately gather all lane leadership on one
   manager (`driver.rs:158`, `:3083`), because composites half-fail
   elsewhere and `legion state backup` requires one node leading every lane.
   Sharding for throughput *inverts* this for scheduler groups — spread is
   the point — so backup and the convergence policy are prerequisites to
   rework, not details.

### 5.5 The partition seams that already exist

- **`allocation_epoch_partition`** — per-scope optimistic-concurrency
  epochs, scope types `"node"` and `"job_config"`
  (`src/storage/migrations.rs:259`, `store.rs:3392`). This is the seam the
  multi-raft plan's Phase 8 named for sharding. It is a *conflict-detection*
  partition, not a data partition — but it proves the row-scope vocabulary
  (node-scoped vs job-scoped state) the sharding needs.
- **`node.pool`** (`src/structs/node.rs:16`) — an existing, user-visible,
  index-backed partition key: an implicit permit pins pool-less tasks to
  `default` (`src/constraints.rs:117`), `pool_binding()` resolves a task's
  target pool (`constraints.rs:145`), the scheduling index partitions by
  pool by default (`src/scheduling_index.rs:149`), preemption and ACLs are
  pool-scoped (`scheduler.rs:3176`, `src/structs/acl.rs:118`). Pools never
  touch lanes or epochs today.
- **Namespaces do not exist** (aspirational ACL comments only,
  `acl.rs:6`) — so pool is the only real key available to a v1.

## 6. The methods

Each method: what it is, what scales, what it costs, what the operator
sees, verdict.

### M1. More write-class lanes (vertical splitting)

**What.** Continue the lanes pattern: a KV lane, a service-discovery-health
lane, a rollout lane, an event lane…

**Verdict up front: refuse as a scaling axis.** The multi-raft plan's own
§1a analysis (worked for service discovery,
`multi-raft-persistence-plan.md:122`) already states the rule: lanes
partition by *write class*, not by feature, and there are only ~4 write
classes (user intent, placement, liveness/status, user data-plane KV).
Beyond that:

- Every new lane adds a leader, elections, a snapshot pipeline, a
  reconciler set, and a row in every operator's head — for write volume that
  was already fine sharing a class.
- The capture mutex (§5.4) means a new lane adds **zero** state-DB write
  concurrency.
- Cross-lane composites are the expensive, fragile part of the system
  (forwarding, reconcilers, half-apply bugs like the open `destroy_job`
  split in TODO); more lanes multiply exactly that surface.
- The residue ceiling (8) is real.

More lanes make the system *wider*, not *faster*. The three lanes are the
right vertical split; this axis is done.

### M2. Job-keyed scheduler shards (shared-state / Omega shape)

**What.** N scheduler groups; jobs hash to a shard; any shard places onto
any node; conflicts arbitrated optimistically.

**Why it looks right.** It is the Gödel/Omega shape, it needs no node
partitioning, jobs never straddle shards, and Legion's partitioned epochs +
late binding look like they were built for it.

**Why it is unsound here.** The epoch CAS arbitrates plans applying through
**one** log. With independent shard logs there is no apply-order arbiter:

- Node-scoped state — `allocation_epoch_partition` rows with
  `scope_type = "node"`, `node_alloc_usage` rows, the apply-time free-pool
  rebuild — would be written by *every* shard that ever places on that node.
  That is a direct single-writer violation (`ownership.rs:5`), and the
  capture guard would reject it; removing the guard re-opens silent replica
  divergence.
- Even granting per-row ownership of `allocation` (each row belongs to its
  job's shard), **oversubscription safety dies**: shard A and shard B can
  each pass apply-time revalidation against their own applied prefix and
  commit allocations that together overfill a node. Omega answers this with
  a single cell-state arbiter (which is M3's single apply group by another
  name) or accept-and-repair eviction. Legion's stated invariant is that a
  stale read model costs a conflict, "never an oversubscription"
  ([docs/scheduler-and-eval-loop.md](../../docs/scheduler-and-eval-loop.md)).

**Verdict: reject — but decompose it.** Its parallel-planning half is M3
(plan anywhere, apply through one arbiter). Its sharding half, made sound,
is M4 (make node ownership follow the shard so the multi-writer case cannot
arise). Building M2 as such would trade the replication design's central
guarantee for a shape we can get safely from M3+M4.

### M3. Cross-manager planning, single apply group (the Nomad shape)

**What.** Architecture review D2 step 2, unchanged: followers run eval
workers in `plan_only` mode against their replicated read model (which every
follower already maintains, `state_machine.rs:590`), ship plan submissions
to the scheduler-lane leader over a new manager-plane RPC, and the leader
applies them with the *same* epoch CAS and apply-time revalidation. Claims
and completions stay leader-side (they are scheduler-lane writes anyway);
the leader becomes a claim-dispatcher + apply-arbiter.

**What scales.** Planner CPU × manager count. Nothing else: one commit
pipeline, one capture mutex, unchanged.

**What it costs.**
- A wire surface (plan-submission RPC) and distributed-debugging burden —
  a plan built on manager B failing on manager A's arbiter is a new class
  of "why didn't it place".
- Follower staleness → higher lazy-sample fallback and conflict rates under
  churn (bounded: the fence semantics need zero change — that is what the
  epochs are for).
- It does not touch the measured bottleneck. Phase 3's arithmetic (planner
  ~7× under its own capacity) and phase 5's driver-bound results say
  planner CPU is not the wall today; the 32-thread local sweep was still
  rising, so even in-process planner scaling has headroom.

**Operator experience.** Invisible — the best possible surface. No config,
no reshard, no format change; managers you already run start contributing
CPU.

**Verdict: keep the seam, build on planner-bound evidence.** The moment a
*shard's* leader (post-M4) or the single leader (pre-M4) shows plan-phase
saturation with an idle pipeline, this is the cheapest multiplier — and it
composes with M4 (each shard can accept shipped plans independently).

### M4. Node-owned scheduler shards (pool-keyed, then hash-bucketed) — the headline

**What.** `raft.scheduler.shards = N`: the scheduler lane becomes N raft
groups (`scheduler-0…N-1` wire names), each **owning a disjoint set of
nodes** and everything scoped to them. v1 keys ownership by **node pool**
(each pool maps to exactly one shard; a shard hosts one or more pools);
v2 generalizes to fixed hash buckets for pools bigger than one shard's
throughput. A job routes to the shard that owns its pool: its evals,
allocations, epochs, plan submissions, rollouts, instances, and volume
claims all live in that one shard.

**Why this is the sound one.** Every replicated row keeps exactly one
writing group:

- Node-scoped rows (`allocation`, node-scope epochs, `node_alloc_usage`)
  belong to the shard owning the node.
- Job-scoped rows (`eval`, `rollout`, job-config epochs, `instance`) belong
  to the shard owning the job's pool — the same shard, because a task binds
  to one pool (`constraints.rs:145`) and v1 requires a job's tasks to
  resolve to pools of one shard.
- Capacity arithmetic is shard-local: two shards **cannot** race on one
  node's free pool, because only one shard can write placements for that
  node. Oversubscription stays impossible by construction, not by
  arbitration. The single-writer principle extends from table-granular to
  row-scope-granular; the capture guard learns to resolve a changed row →
  owning shard and reject cross-shard changesets exactly as it rejects
  cross-lane ones today (`ownership.rs:456`).

**What scales, and the honest ceiling.**

- **Commit pipelines × N.** Each shard gets its own pipeline thread and its
  own quorum stream — the exact effect the lanes A/B measured (quorum
  queueing parallelizes; local capture work is small). This attacks the
  resource phase 2/3 identified as the wall.
- **Planners × min(N, managers).** Shard leadership is *deliberately
  spread* (the inverse of today's convergence policy); each shard's leader
  runs that shard's eval workers against the shared read model, scoped to
  the shard's pools. Planner CPU scales with managers without M3's plan
  shipping. Per-shard planning universe is O(fleet/N) — the node axis gets
  cheaper too (available-nodes list, capacity snapshot, index shard all
  shrink per shard).
- **Blast radius ÷ N.** A leaderless or storm-swamped shard stalls its
  pools; every other pool's scheduling is untouched. Today a scheduler-lane
  leadership gap stalls the entire cluster.
- **The ceiling that remains:** the shared state DB. All shards apply
  through `CAPTURE_MUTEX` into one `store.sqlite`. Group commit means each
  batched op carries many plans, so the mutex holds µs–ms of local work per
  op while the pipeline holds quorum RTTs — near-linear scaling holds until
  aggregate local write work saturates roughly one core. Past that, the
  next step is per-shard state DBs (the original plan's "Option B"),
  which is real Store surgery: N write connections, snapshot scoping
  becomes trivial, but every cross-shard SQL read (job list, UI queries)
  must move to the read model or `ATTACH`. That fork is v2/v3, gated on
  measuring the mutex as the wall — there is a lot of runway first.
- **What does not scale:** per-manager memory. Every manager still applies
  and mirrors every shard (the read model stays global — necessary for
  followers to take over any shard, and for cross-pool reads). Node count
  per cluster is still bounded by the mirror and snapshot sizes; that
  boundary belongs to M6.

**The engineering bill, honestly.** Ranked by size:

1. **Row-scoped snapshots.** Today's lane snapshots are table-scoped dumps
   with manifest validation (`state_machine.rs:391`). Shard snapshots must
   dump/install *rows* (`WHERE` the shard's pool/bucket set), and the
   manifest must carry the ownership scope so an install can never clobber
   a sibling shard's rows in the shared tables. This is the biggest new
   mechanism, and it is also the mechanism M5 and v2 rebalancing reuse.
2. **Row-scope capture enforcement** — extend `verify_changeset_ownership`
   from table→lane to row→shard for the scheduler tables (resolving
   node→pool→shard and job→pool→shard at capture time).
3. **Leadership spread + backup rework.** A spread policy for scheduler
   groups (each manager campaigns for a balanced subset) replacing
   co-location for these groups only; `legion state backup` re-built as a
   fan-in export with per-shard applied indexes, since "one node leads
   every lane" will deliberately never be true again.
4. **Routing.** Job submit's scheduler-lane half targets the job's shard —
   the forwarding enum already carries `enqueue_eval` (`forward.rs:53`), so
   a control leader that does not lead the target shard forwards, exactly
   as the standing-split path does today. Mark-lost fan-out targets the
   lost node's shard. Mechanical, on built machinery.
5. **Mechanical format items** (all bundled into the same JC-43 break that
   enabling shards already is): shard wire names and data dirs,
   `raft_applied` keys, `topology.json` recording the shard map, widening
   `ID_STRIDE` (8 → 64) so shards can mint `scheduler_event` ids.

**Operator experience.** The surface, as the operator would type it:

```nickel
persistence = {
  type = "raft",
  raft = {
    topology = "lanes",
    scheduler = {
      shards = 4,                 # scheduler raft groups; default 1 (today's behavior)
      # pools map to shards by hash(pool) % shards unless pinned:
      assignments = { batch = 2, ml-training = 3 },
    },
  },
}
```

```console
$ legion raft status
lane=control    role=leader   term=12  ...
lane=status     role=leader   term=9   ...
lane=scheduler/0  role=leader    term=7  pools=[default, web]      queue=3    ...
lane=scheduler/1  role=follower  term=8  pools=[api]  leader=mgr-b queue=0    ...
lane=scheduler/2  role=follower  term=7  pools=[batch] leader=mgr-c queue=412 ⚠ ...
lane=scheduler/3  role=leader    term=6  pools=[ml-training]       queue=1    ...
```

- Per-shard metrics: the existing `legion_raft_*` series and eval-queue
  gauges gain a `shard` label; "which shard is hot" is a dashboard read.
- A job whose tasks bind pools on different shards is rejected at submit
  with the fix named:
  `pools "web" and "batch" map to scheduler shards 0 and 2; pin them to one
  shard (raft.scheduler.assignments) or split the job`.
- v1 resharding is a topology change: backup → fresh cluster → restore,
  same as `single`↔`lanes` (JC-43). Honest and shippable; v2's fixed
  virtual buckets (e.g. 64 buckets → N groups) make shard-count changes a
  bucket handoff (snapshot-install of a bucket's rows into the new owner)
  instead of a format break.
- Jobspec: unchanged. Workers: unchanged (heartbeats already forward).

**Verdict: this is the axis.** It is the only in-cluster sharding that
preserves the soundness argument, it multiplies both measured serial
resources, its blast-radius story is an operator feature in its own right,
and its hardest mechanism (row-scoped snapshots) is reusable by everything
else. Gate it on Phase 0 measurement (§9).

### M5. Sharding the other lanes (status, control/KV)

**Status lane, by node hash.** The mechanically easiest shard in the
system: `node_health` / `allocation_health` rows are strictly per-node, so
hash-bucketed status groups have *zero* cross-shard writes and no routing
ambiguity; liveness digests just gain a shard dimension. The multi-raft
plan already names this the release valve for health-check volume
(`multi-raft-persistence-plan.md`, cluster-shape levers). But the
liveness-off-raft work removed steady-state heartbeats from raft entirely —
what remains on the status pipeline is transitions and health *changes*,
plus display checkpoints, all group-committed. Until a measured fleet shows
transition storms saturating the status pipeline (think 100k+ nodes in a
mass-restart), this is machinery without a customer. It shares the
row-scoped snapshot mechanism with M4, so building M4 first makes this
cheap later.

**Control lane.** User-driven and low-rate; job intake's control half is
deliberately serial per job (version derivation). The only plausible
outgrowth is KV under heavy application use — sharding `kv_entry` by path
prefix is sound (single-writer per prefix) but speculative; KV-heavy users
are better served first by the existing coalescing. Defer without a design.

**Verdict: hold for evidence; inherit M4's machinery.**

### M6. Federation: more clusters (the outer axis)

Already fully planned in
[`multi-cluster-federation-plan.md`](multi-cluster-federation-plan.md)
(peering, single pane of glass, cluster-granular multi-cluster jobs, DR) —
not re-litigated here. What this document adds is the *positioning*:

- Federation is the only method that scales **every** axis linearly —
  throughput, node count, per-manager memory, snapshot size, blast radius,
  and geography — because nothing is shared.
- Its cost is the one M4 exists to defer: N control planes, and
  cluster-granular placement (no cross-cluster bin-packing — the plan's
  explicit non-goal).
- The healthy division of labor: **M4 raises the single-cluster ceiling so
  operators choose federation for geography, tenancy, and blast radius —
  not because the scheduler ran out.** A world where operators are forced
  into federation at 5k nodes for throughput reasons is the failure mode
  this proposal exists to prevent.

### M0 (the null hypothesis). Keep exhausting the single-group levers

Named last but sequenced first: phase 5 demonstrated the in-group levers
(fusion, batching, group commit, statement caching, background
checkpointing) still pay — 16–20× over four sessions — and the current
ceiling is *unmeasured* because the rig lost. Remaining in-group headroom
is real (native submitter, eval-thread knee above 32, leader direct-commit
was measured at only ~17% and declined). Every sharding method above is
more valuable *and more correctly designed* after the post-batching wall
is measured and attributed (pipeline wait vs capture/apply vs planner).
Sharding before then is building against a wall nobody has located.

## 7. What any sharding must preserve (the invariant checklist)

The implementation plan that graduates from this proposal inherits these as
acceptance criteria:

1. **Single-writer ownership per replicated row** — table-granular today,
   row-scope-granular under M4/M5 — enforced at capture time in every
   topology, with the exhaustiveness and disjointness tests extended, never
   weakened (`ownership.rs:456`, `:539`, `:620`).
2. **No oversubscription by construction.** A stale read costs a conflict
   or a rebuild, never overfill. Any method that converts this into
   "detected and repaired" is rejected (this is what kills M2).
3. **The epoch fence and apply-time revalidation semantics** survive
   unchanged within each shard.
4. **Composite operations complete under arbitrary leadership placement**,
   via forwarding + reconcilers — already true for the standing split; a
   spread topology makes it the design point, so the reconciler battery and
   `tests/raft_lanes.rs` shapes extend per shard.
5. **Jobspec and worker plane unchanged.** Pools remain the only
   user-visible partition vocabulary. One join token enrolls a manager into
   every group.
6. **Formats change only at declared break points.** Enabling/resizing
   shards is a topology change under the JC-43 rule (v1) or a bucket
   handoff (v2) — never a silent mixed-version disagreement (the
   `topology.json` stamp and snapshot manifests carry the shard map).
7. **The status surface names the hot group.** Per-shard rows in
   `legion raft status`, per-shard metrics labels, and `readyz` notes —
   scaling machinery an operator cannot see is scaling machinery they
   cannot trust.
8. **`shards = 1` is byte-identical to today** (the lanes precedent:
   `single` collapses to one group through the same code path,
   `driver.rs:3118`).

## 8. Comparison

| Method | Eval/alloc throughput | Node axis | Blast radius | Format break | New correctness surface | Operator burden | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M1 more write-class lanes | ~none (capture shared; classes exhausted) | none | worse (more composites) | yes, per lane | composite half-applies | +1 lane per feature, forever | **refuse** |
| M2 job-keyed shards | high on paper | none | per-shard | yes | **oversubscription across shards** — fails invariant 2 | reshard = job re-hash | **reject; decomposes into M3+M4** |
| M3 plan shipping | planner × managers | none | none | none | staleness → conflict rate | zero (invisible) | keep seam; build on planner-bound evidence |
| M4 node-owned shards | pipelines × N, planners × min(N, mgrs) | planning O(fleet/N); memory unchanged | per-pool | yes (v1 enable/resize) | row-scope ownership + snapshots | one knob; per-shard status; v1 reshard = restore | **the axis — gate on Phase 0** |
| M5 status/KV shards | n/a (heartbeat transitions) | transition-storm headroom | per-bucket | yes | shares M4's | near zero | hold for evidence |
| M6 federation | × clusters | × clusters (incl. memory) | per cluster | no (additive) | none new | N clusters, peering | planned elsewhere; complementary |

## 9. Sequencing (each step gated, none speculative)

1. **Phase 0 — locate the wall.** Ship the fast native submitter (already
   TODO's top gate), re-run the fill ladder and eval-thread sweep, and
   *attribute* the ceiling: per-shard-candidate metrics already exist for
   pipeline wait and plan phases; add a capture-mutex wait histogram (tiny,
   valuable regardless — the phase-3 lesson is that unmetered waits hide
   inside whoever holds the stopwatch). **Exit: a measured post-batching
   ceiling with a named bottleneck.**
2. **Phase 1 — graduate M4 v1** (only if Phase 0 names pipeline queueing
   again): a full implementation plan for pool-keyed shards — row-scoped
   snapshots, row-scope capture guard, spread policy for scheduler groups,
   backup fan-in, routing, `raft.scheduler.shards`, per-shard eval workers
   — with a lanes-style isolation/scaling A/B (alloc/s at shards = 1/2/4,
   plus one-shard-storm isolation) as the headline gate, and `shards = 1`
   equivalence as the safety gate.
3. **Phase 2 — M4 v2** on demand: virtual buckets + online bucket handoff
   (reshard without restore), hash sub-sharding for a pool bigger than one
   shard, and — only if the capture mutex is measured as the wall —
   the per-shard state DB fork.
4. **Phase 3 — M3** if a shard leader shows planner saturation with an
   idle pipeline.
5. **Phase 4 — M5 status shards** on transition-storm evidence, reusing
   Phase 1's machinery.
6. **Federation proceeds on its own plan throughout** — it is not
   sequenced behind any of this.

## 10. Testing story (for the graduating plan)

- **Equivalence:** `shards = 1` produces byte-identical replica state and
  passes the whole existing raft battery unchanged.
- **Ownership:** the exhaustiveness/disjointness tests extended to row
  scopes; a property test that a changeset touching two shards' rows is
  rejected before proposal; the cross-shard-oversubscription scenario
  (two shards, adversarial plans, one node) must be *unconstructible*, not
  merely rare.
- **Snapshots:** per-shard dump/install round-trip; install refusal on
  scope mismatch; bucket-handoff round-trip (v2).
- **Isolation/scaling A/B** (the lanes precedent,
  `tests/raft_perf_smoke.rs`): alloc/s vs shard count; a loss storm on one
  pool leaving sibling shards' p99 flat.
- **Failure:** kill a shard leader → only its pools stall, takeover within
  the election window, queue recovery on acquisition per shard; standing
  spread + forwarding under a manager loss.
- **Skew:** a pre-shard manager joining a sharded cluster is refused by the
  topology stamp with a message naming the upgrade path.

## 11. Open questions, with leans

1. **Multi-pool jobs under pool-keyed shards.** Lean: reject at submit
   naming the fix (pin the pools to one shard), because cross-shard
   placement atomicity is exactly the swamp M4 exists to avoid. Revisit
   only with a real user, and then via co-shard pinning, not cross-shard
   two-phase apply.
2. **Where pool-less (`default`-pool) jobs go.** Lean: `default` is a pool
   like any other — it maps to one shard, pinnable. A giant default pool is
   the v2 hash-sub-shard customer.
3. **Shared state DB vs per-shard DBs in v1.** Lean: shared (v1 ships
   without Store surgery; the mutex ceiling is far given group commit);
   fork to per-shard only on a measured mutex wall.
4. **Bucket count for v2.** Lean: 64 fixed buckets (matches the widened
   ID stride, comfortably above any plausible shard count, small enough
   that per-bucket manifests stay readable).
5. **Backup shape under standing spread.** Lean: per-shard export with
   per-shard applied indexes, restore reassembling into one adopted
   baseline — must land *with* M4 v1, not after, since spread makes the
   current all-lanes-leader precondition permanently unsatisfiable.
6. **Does `single` topology get shards?** Lean: no — shards imply `lanes`;
   `single` stays the simple tier.
7. **Read-model scoping.** Lean: keep the global mirror (followers must be
   able to take over any shard); scope only the *planner's* view per shard
   (available-nodes and capacity snapshots filtered to the shard's pools,
   which the pool-partitioned scheduling index already nearly provides).
   Per-shard mirrors are a memory optimization with a failover cost — not
   worth it until the mirror itself is the measured problem, which is M6
   territory anyway.
