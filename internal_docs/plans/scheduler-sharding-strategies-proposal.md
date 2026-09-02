# Scheduler Sharding Strategies: Scaling the Control Plane by Adding Raft Groups (proposal)

**Status: PROPOSAL, revision 2** — an exploration document, like
[`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md):
it evaluates the ways Legion's scheduler could be sharded across more raft
groups ("lanes", shards, or whole clusters) so that adding groups adds node
capacity and eval/allocation throughput. No code ships from this document;
its output is a ranked map of the option space, the invariants any option
must preserve, and a gated graduation path. It extends — and in places
corrects — the deferred "Phase 8: scheduler-lane key-range sharding" sketch
in [`multi-raft-persistence-plan.md`](multi-raft-persistence-plan.md) §Phase 8.

> **Judgement calls:** [`../calls/scheduler-sharding-strategies-proposal_calls.md`](../calls/scheduler-sharding-strategies-proposal_calls.md)

### What revision 2 changed, and why

Revision 1 recommended pool-keyed scheduler shards as N raft groups over the
**shared** `store.sqlite`, with shard leadership **spread** across managers
in v1, and row-scoped snapshots as the main new mechanism. A second pass
that questioned every decision against the pipeline code overturned two of
them:

1. **A raft group without its own state file is not a shard.** The pipeline
   is strictly serial per group — capture, then `block_on(client_write)`,
   then reply (`src/storage/raft/driver.rs:1499`, `:1535`) — and both the
   capture and the apply hold the process-global `CAPTURE_MUTEX`
   (`src/storage/raft/capture.rs:200`, `:522`). Extra groups over one file
   therefore overlap only the quorum round-trip. Since the batching work
   landed, the local write work per batch (tens of ms at depth) dwarfs the
   round-trip (1–3 ms on a LAN), so the arithmetic in §1.2 says shared-file
   shards buy roughly ×1.1 where revision 1 claimed "near-linear". **The
   unit that has to be multiplied is the SQLite writer, which means one
   state file per shard.** That also deletes revision 1's biggest new
   mechanism: whole-file snapshots already exist (`driver.rs:2795`), so
   row-scoped snapshots are no longer needed.
2. **Spreading leadership is the second rung, not the first.** With
   per-shard files, N shards led by *one* manager already give N
   independent writers and N pipelines on that box, with no change to
   composite operations, convergence, or `legion state backup`. Spread is
   what you do when a single manager's cores or fsync stream saturate —
   and it carries the composite/backup rework revision 1 put in v1.

Everything else — node-owned over job-keyed, refusing more write-class
lanes, federation as the outer axis, gating on measurement — survived the
review and is kept, with the reasoning tightened.

## 1. Problem statement

The question, as asked: *can the scheduler be sharded such that we add more
and more raft clusters or lanes and get more nodes or more eval/alloc
throughput?*

The answer this document defends: **yes for throughput, partially for
nodes, and only if a "shard" means a raft group plus its own state
partition, owning a disjoint set of nodes.** More lanes in the current
write-class sense buys nothing further; more raft groups over one SQLite
file buys almost nothing; node-owned shards with their own files multiply
the real serial resources; and past a point, more groups in one cluster
cannot help and the answer becomes more clusters (federation, already
planned).

### 1.1 What is actually serial today

1. **One raft group for all placement writes**, drained by one pipeline
   thread (`driver.rs:3093`). AWS phase 2 measured 3.07 ms of
   `pipeline_wait` per scheduler-lane op against 0.09 ms on the status lane
   ([`../aws-phase3-findings.md`](../aws-phase3-findings.md)); phase 3 put
   `apply` at 67% of plan time and throughput ~7× below planner capacity.
2. **One active planner.** Eval workers idle on every manager that does not
   lead the scheduler lane (`src/manager/daemon.rs:1809`).
3. **One SQLite writer for every group.** `CAPTURE_MUTEX` is a
   process-global static (`capture.rs:133`) held for the whole of every
   capture (`:200`) and every apply (`:522`), across all lanes, because all
   lanes write one `store.sqlite` (`src/storage/raft/state_machine.rs:42`;
   acknowledged at `driver.rs:2750`). Lanes isolate raft logs, replication
   pipelines, and elections — not SQLite write work.

### 1.2 The pipeline cycle, and why the file is the unit

Per group, one replicated op costs `T = W_c + R + W_a`: local capture work
(`W_c`, under the mutex), the propose → quorum round-trip (`R`, mutex
free), and the local apply (`W_a`, under the mutex). The two phases are
already metered separately — `legion_raft_capture_seconds` versus
`legion_raft_propose_apply_seconds` (`src/manager/telemetry.rs:765`) — so
the ratio is readable from any archived campaign.

- A single pipeline delivers at most `B / T` batches-worth of work per
  second, where `B` is the group-commit batch.
- **N groups over one file:** round-trips overlap, local work does not, so
  the ceiling is `min(N · B/T, B/(W_c + W_a))`. The gain saturates at
  `N ≈ T / (W_c + W_a)`.
- **N groups over N files:** `N · B/T` until the box's cores or fsync
  stream saturate (SQLite writers on different files do not contend).

Phase 5 ([`../aws-phase5-findings.md`](../aws-phase5-findings.md)) measured
the group-commit body at ~20 ms per batch on the rig after the statement
cache and WAL fixes, with the changeset apply at roughly a tenth of the body
in the split bench; same-AZ NVMe quorum commits are 1–3 ms. That puts
`T / (W_c + W_a)` at about **1.1** — shared-file shards would deliver
essentially nothing at throughput, and would only ever pay on WAN quorums or
at low load, where `R` dominates and the win is latency, not throughput.
(Phase 5's per-row costs were amplified by the read-model truncation bug,
and the JSON-affinity rebuild that followed
([`jsonb-text-affinity-plan.md`](jsonb-text-affinity-plan.md)) removed a
further 5–10% of raw insert cost; the Phase 0 re-measure in §9 will move
the ratio, but it would need to move by ~10× to change the conclusion.)

**TiKV is the instructive comparison:** its thousands of raft groups share
one RocksDB and still scale, because an LSM store admits concurrent
writers. SQLite is single-writer per file. Multi-raft over a shared store
scales exactly as far as the store's write concurrency does — for Legion,
that is one file per shard.

### 1.3 Where the measured ceiling has moved

| Date | Measurement | Number |
| --- | --- | --- |
| 2026-07-28 | lanes vs single, scheduler p99 under cross-lane flood | 3.37 ms vs 110.45 ms; 4–5× throughput retained ([docs/multi-raft.md](../../docs/multi-raft.md)) |
| 2026-08-04 (phase 3) | alloc/s @5k nodes, lanes, 4 eval threads | 80.35/s; `apply` = 67% of 41.42 ms plan time |
| 2026-08-05 (phase 4) | eval-thread sweep 4 → 32 | 177 → 252 alloc/s locally; rig found driver-bound |
| 2026-08-06 (phase 5) | after claim/complete fusion, batched enqueue, group commit, statement cache | **≥1,323–1,625 alloc/s, driver-bound at every fill level** |
| 2026-08-06 (storm) | internally-generated replacement (not driver-bound) | ~277 alloc/s sustained, 0.006 s placement gap |

Two lessons. **In-group levers keep paying** — a 16–20× improvement came
from batching and fusing rounds on the same pipeline. And **the
post-batching ceiling is unmeasured** — every deep cell in phase 5 was
floor-limited by the load generator. The lanes A/B (row 1) is also why
revision 1 over-reached: that flood was *unbatched* heartbeats and submits,
each paying a full `R`, which is the regime where overlapping round-trips
wins big. Batching moved the regime.

### 1.4 The node axis is a different problem

"More nodes" is no longer primarily a raft-write problem. Steady-state
heartbeats never touch raft ([docs/node-liveness.md](../../docs/node-liveness.md));
cadence and per-type lost windows size the RPC load; digests decouple
manager count from fleet size. What still grows with fleet size:

- **The read model, on every manager.** `SchedulingReadModel` is an
  unconditional full mirror (`src/storage/store.rs:227`, built by four
  unfiltered table scans at `store.rs:2362`, refreshed on every replica
  after every apply, `state_machine.rs:590`). No in-cluster sharding shrinks
  it — every manager is a voter of every group and must be able to take any
  shard over. At a rough 1–3 KB per node and per allocation in memory, a
  100k-node / 1M-allocation cluster is a few GB per manager: feasible on a
  manager box, and the honest in-cluster ceiling.
- **Snapshot and backup size** — per file, so per-shard files divide them.
- **Planner universe** — bounded by indexes and `score_sample`; node-owned
  shards additionally divide it by shard count.
- **Churn-driven eval load** — the throughput axis again.

So: eval/alloc throughput scales in-cluster by sharding; node count scales
in-cluster until per-manager memory bites, and past that the answer is more
clusters, not more groups.

## 2. Prior art

- **Nomad** — one raft group, but *many* schedulers: every server runs
  scheduler workers that plan optimistically against local state; a single
  leader **plan queue** serializes apply with conflict rejection. C1M's
  3,750 placements/sec aggregate is this shape. Regions are separate
  clusters. *Take: optimistic planning against replicated state (M3).
  Note: its single apply arbiter is exactly the bottleneck Legion measured.*
- **Kubernetes** — one etcd group, one active kube-scheduler; scale
  practice is many clusters. ByteDance's **Gödel** and the multi-scheduler
  KEP shard the *scheduler* over shared state with optimistic concurrency.
  *Take: the shared-state optimistic shape is credible. Reject: etcd's
  whole-cluster single log.*
- **Omega** — multiple schedulers, full-state replicas, optimistic commit
  through one cell-state arbiter. *Take: Legion's partitioned epochs + late
  binding are already an Omega-ish arbiter. Reject: multi-writer commit of
  one node's capacity from independent logs — Omega had one arbiter;
  independent raft groups have none (M2).*
- **Borg** — cells of ~10k machines, one Borgmaster per cell; Google adds
  cells. *Take: the cell as the outer unit (M6).*
- **TiKV / CockroachDB** — true multi-raft over one shared engine, with a
  placement driver for split/merge/rebalance. *Take: many groups over one
  voter set and one transport (Legion's engine already multiplexes by
  `group_id`, `proto/legion.proto:5918`). Reject: dynamic range management
  in v1. And the lesson in §1.2: their sharing works because RocksDB is
  multi-writer; ours cannot.*
- **YARN federation / Mesos** — routers over sub-schedulers owning capacity
  slices. *Take: the observation that hierarchical routing degenerates into
  either static ownership (M4) or clusters (M6), so it is not a distinct
  method.*

## 3. Jobs to be done

**Cluster operator**
- When my fleet or deploy rate outgrows one scheduler pipeline, I want to
  raise a number in the manager config (and later, add managers) and get
  more placement throughput, so I don't have to split into clusters just
  for speed.
- When the control plane is near its limit, I want the status surface to
  tell me *which* shard is hot and what to add next, so capacity planning
  is a dashboard read.
- When one part of the cluster is having a bad day (a pool in a loss
  storm), I want the rest of the cluster's scheduling untouched, so blast
  radius is something I chose.

**Job author / app developer**
- When the operators change how the control plane is sharded, I want my
  jobspec and CLI to behave exactly as before, so I never learn what a
  "shard" is. *(An invariant: no method below may surface in the jobspec
  beyond the pools that already exist.)*

**On-call responder**
- When placements for one team stall, I want `legion raft status` and the
  metrics to name the shard, so the incident starts at the right place.

**Maintainer**
- When choosing the scaling axis, I want the options ranked with their
  correctness costs stated, so we spend on the one that preserves the
  soundness argument the replication design is built on.

### User scenarios

1. *The batch pool eats the cluster.* A 40k-node fleet where one team's
   batch pool generates 80% of eval traffic. The operator pins that pool to
   its own shard; the service pools' p99 stops moving when batch storms.
2. *Deploy-rate wall.* A CI-driven org whose deploy bursts outrun the
   scheduler. The operator sets `scheduler.shards = 4` at a maintenance
   window (v1: backup/restore; later: online) and placement throughput
   roughly quadruples on the same manager box.
3. *Two regions.* An operator wants EU workers scheduled in EU. No shard
   topology fixes WAN quorum; they run two clusters and peer them
   ([`multi-cluster-federation-plan.md`](multi-cluster-federation-plan.md)).

## 4. Goals and non-goals

### Goals

- Enumerate every credible sharding axis with architecture, performance
  model, trade-offs, and operator experience.
- Name the invariants any method must preserve, so the eventual
  implementation plan inherits a checklist.
- Rank the methods and give a measurement-gated graduation path.

### Non-goals

- Shipping anything from this document.
- Replacing SQLite as the state-machine store (architecture review D3) or
  abandoning row-changeset replication.
- Cross-cluster scheduling (the federation plan's explicit non-goal).
- TiKV-style dynamic range splitting/merging as a v1 requirement.

## 5. Background: what exists today

### 5.1 The lane engine is multi-group; the lane set and layout are frozen

- `Lane { Control, Scheduler, Status }` is a hard-coded enum
  (`src/storage/raft/mod.rs:41`); `Lane::ALL` drives bring-up and the
  exhaustiveness tests (`mod.rs:52`).
- `wire_name()` strings double as gRPC `group_id`, per-lane data-dir name,
  and `raft_applied` key — documented frozen (`mod.rs:98`). One channel pool
  carries every group (`src/storage/raft/network.rs:270`, `driver.rs:2829`);
  one openraft core per lane (`driver.rs:2841`); one log DB per lane
  (`src/storage/raft/log_store.rs:86`); per-lane snapshot dirs
  (`driver.rs:2856`). **Adding groups is mechanical at this layer.**
- Snapshot scope is chosen by group count: `WholeDb` (`VACUUM INTO`) for
  one group, table-scoped `LaneTables` dumps otherwise (`driver.rs:2794`).
  The whole-file path is the one a per-file shard reuses.
- Writer-minted commutative ids are residue-partitioned: `ID_STRIDE = 8`,
  `id % 8 == lane.id_residue()` (`mod.rs:73`). Eight residues; three taken.
- Topology is a two-valued string fixed at first start by the
  `topology.json` stamp (`driver.rs:236`, `:437`); changing it is
  backup → fresh cluster → restore (JC-43).

### 5.2 Single-writer ownership is the soundness foundation

- `TABLE_OWNERS` maps every replicated table to one lane, `Commutative`, or
  `LocalOnly` (`src/storage/raft/ownership.rs:139`), with the reason at
  `ownership.rs:5`: row-changeset replication from two logs into one table
  applies in different orders on different replicas.
- `verify_changeset_ownership` rejects a cross-lane changeset before
  proposal, in every topology (`ownership.rs:456`, `:58`;
  `driver.rs:1487`). Tests pin totality (`:539`) and per-lane snapshot
  disjointness (`:620`).
- `scheduler_event` and `idempotency_record` are the `Commutative` class —
  insert-only, globally unique keys, writable from any lane's changeset,
  snapshotted with control (`ownership.rs:91`, `:271`, `:325`).
- Cross-lane **foreign keys exist**: `allocation`, `plan_submission`,
  `rollout`, `eval`, and friends carry `REFERENCES job / job_config /
  task_config / node … ON DELETE CASCADE` into control-lane tables
  (`src/storage/migrations.rs:159`, `:178`, `:306`, `:346`). Under lanes a
  cascade would be a cross-lane write inside a control changeset and the
  guard would reject it — which is why the composites delete in
  dependency order (destroy: status → scheduler → control,
  `store.rs:8050`) so cascades find nothing. `foreign_keys = ON` is live
  (the JSON-affinity rebuild had to order its table drops around it), so
  the constraints are real but latent: safety nets, not load-bearing.

### 5.3 The scheduler lane, concretely

- Owned tables (`ownership.rs:185`): `allocation`, `allocation_epoch`,
  `allocation_epoch_partition`, `plan_submission`,
  `allocation_stop_notice`, `node_alloc_usage`, `instance`, `volume`,
  `eval`, `rollout`, `rollout_event`.
- The eval queue is replicated deliberately (JC-41 reversed JC-27;
  `ownership.rs:94`); every enqueue/claim/complete is a scheduler-lane
  round, now heavily batched
  ([`eval-path-raft-cost-plan.md`](eval-path-raft-cost-plan.md)).
- Eval workers gate on per-lane leadership at loop top and mid-batch
  (`daemon.rs:1809`, `:1912`), with an acquisition hook that recovers the
  queue (`daemon.rs:1557`); the probe is per lane (`store.rs:1427` →
  `driver.rs:989`).

### 5.4 The cross-cutting couplings

1. **`CAPTURE_MUTEX` + one file** (§1.1–1.2).
2. **Composites span lanes** — submit, destroy, register, remove-node,
   mark-lost (`store.rs:8469`, `:8050`, `:6875`, `:6524`, and the
   batched mark-lost that follows `remove_allocs_for_node`) —
   each half its own entry, completed under divergence by forwarding
   (`ForwardableLaneWrite`, `src/storage/raft/forward.rs:53`, including
   `enqueue_eval`) and the lane reconcilers (`daemon.rs:1042`–`1118`).
3. **Co-location is deliberate.** `ANCHOR_LANE = Lane::Control` and the
   convergence loop gather all leadership on one manager
   (`driver.rs:158`, `:3083`), because composites half-fail elsewhere and
   `legion state backup` needs one node leading every lane. A design that
   *keeps* co-location inherits all of that for free; one that spreads
   leadership must rework it.

### 5.5 The partition seams that already exist

- **`allocation_epoch_partition`** — scope types `"node"` and
  `"job_config"` (`migrations.rs:259`): the row-scope
  vocabulary (node-scoped vs job-scoped state) that ownership by shard
  needs, even though today it partitions conflicts, not data.
- **`node.pool`** (`src/structs/node.rs:16`) — the only real,
  user-visible, index-backed partition key: an implicit permit pins
  pool-less tasks to `default` (`src/constraints.rs:117`), `pool_binding()`
  resolves a task's pool (`constraints.rs:145`), the scheduling index
  partitions by pool (`src/scheduling_index.rs:149`), preemption and ACLs
  are pool-scoped (`scheduler.rs:3176`, `src/structs/acl.rs:118`).
- **Namespaces do not exist** (`acl.rs:6`).
- **Store shape:** one `write_conn` and a read pool (`store.rs:707`), all
  over one file; `ATTACH` is mentioned only as a thing changesets cannot
  carry (`store.rs:16375`). rusqlite is `bundled` (`Cargo.toml:20`), so
  `SQLITE_MAX_ATTACHED` is the default 10 unless the build defines it.

## 6. The methods

### M1. More write-class lanes (vertical splitting) — refuse

A KV lane, a service-discovery-health lane, a rollout lane… The multi-raft
plan's §1a already fought this for service discovery
(`multi-raft-persistence-plan.md:122`): lanes partition by *write class*,
and there are ~4 classes. Each new lane adds a leader, elections, a snapshot
pipeline, a reconciler set, more composite half-apply surface — and, over
the shared file, **zero** write concurrency (§1.2). The residue ceiling is
8. More lanes make the system wider, not faster. The vertical split is done.

### M2. Job-keyed scheduler shards (shared-state / Omega shape) — reject, decompose

Jobs hash to shards; any shard places on any node; conflicts arbitrated
optimistically. Attractive because jobs never straddle shards and the
partitioned epochs look purpose-built. Unsound here because the epoch CAS
arbitrates plans applying through **one** log:

- Node-scoped rows (node-scope epochs, `node_alloc_usage`, the apply-time
  free-pool rebuild) would be written by every shard that places on that
  node — a direct single-writer violation the guard rejects; removing the
  guard re-opens silent divergence.
- Even with per-row ownership of `allocation`, two shards can each pass
  revalidation against their own applied prefix and together overfill a
  node. Omega answers with one arbiter (which is M3's single apply group)
  or accept-and-repair eviction, and Legion's invariant is that staleness
  costs a conflict, "never an oversubscription"
  ([docs/scheduler-and-eval-loop.md](../../docs/scheduler-and-eval-loop.md)).

Its parallel-planning half is M3; its sharding half, made sound, is M4.

### M3. Cross-manager planning, single apply group (the Nomad shape) — keep the seam, demote

Architecture review D2 step 2: followers run eval workers in `plan_only`
against their replicated read model, ship plan submissions to the leader,
which applies with the unchanged epoch CAS. Scales planner CPU × managers;
touches neither the pipeline nor the writer, so it does not address the
measured wall. Costs a wire surface and a new class of "why didn't it
place" across two managers.

Revision 2 demotes it further: once shards exist (M4), *spreading shard
leadership* (rung B) also multiplies planner CPU across managers — and
multiplies writers and fsync streams with it — using leadership the engine
already has instead of a plan-shipping RPC. M3 is the fallback if rung B is
blocked, or for a single shard whose leader is planner-bound with an idle
pipeline.

### M4. Node-owned scheduler shards, each a state partition — the axis

**What a shard is.** `raft.scheduler.shards = N` creates N scheduler groups
(`scheduler-0…N-1`), each with **its own log, its own snapshot dir, and its
own state file** `scheduler-<k>.sqlite` holding that shard's rows of the
scheduler tables. Each shard **owns a disjoint set of node pools**, and a
job lives in the shard that owns its pool: evals, allocations, epochs, plan
submissions, rollouts, instances, volume claims, and the `scheduler_event`
rows its ops emit are all in that one file. Control and status tables stay
in `store.sqlite`.

**Why the file.** §1.2: N writers need N files. The consequences are all
simplifying:

- **Ownership becomes physical.** A shard's capture runs on that shard's
  connection and can only touch that shard's file; the table-granular guard
  keeps working unchanged per file, and the only new check is *routing* —
  that every row's pool resolves to this shard — which is a cheap
  post-capture assertion, not a new enforcement regime.
- **Snapshots are whole-file.** The existing `WholeDb` `VACUUM INTO` path
  (`driver.rs:2795`) applies per shard; each file carries its own
  `raft_applied` marker. No row-scoped dump/install machinery.
- **The mutex becomes per file.** Capture and apply on shard 2 never wait
  for shard 0.
- **Per-file backup, snapshot, and compaction sizes** divide by N.

**Why node ownership (pool-keyed).** Every replicated row keeps exactly one
writer *and* one file: node-scoped rows belong to the node's pool's shard;
job-scoped rows belong to the job's pool's shard; a task binds to one pool
(`constraints.rs:145`), and v1 requires a job's tasks to resolve to pools of
one shard. Capacity arithmetic is shard-local, so oversubscription stays
impossible by construction, not by arbitration. Pools are also the only
partition vocabulary users already have; nothing new enters the jobspec.

**The three rungs.** Each independently shippable and independently
valuable; a deployment stops at whichever rung its wall demands.

*Rung A — shards co-located on the leader.* All N shard leaderships stay
with the control anchor (today's convergence policy, unchanged). On that
one manager: N pipeline threads, N write connections, N capture mutexes,
one shared `evalThreads` pool whose workers claim per shard (the claim
round for a batch targets one shard — deepest ready queue first). What
scales: writers × N, pipelines × N, planner universe ÷ N, blast radius per
pool. What does not: planner CPU and fsync are still one box. What stays
untouched: composites (the control leader leads every shard, so the
submit's scheduler half is a local op as today), the reconcilers,
convergence, and `legion state backup` (now N+1 `VACUUM INTO`s under one
hold — still on the all-groups leader).

*Rung B — spread shard leadership across managers.* The convergence policy
gains a per-shard target ("shard k prefers manager k mod M") in place of
"everything on the anchor". Shard leaders run their shard's eval workers
against their own (complete) read model. What scales further: planner CPU,
apply CPU, and fsync streams × managers. What it costs — and why it is not
in rung A: composite halves that touch a scheduler shard route via
forwarding as a *fast path*, not a fallback (the machinery exists:
`enqueue_eval` is already a `ForwardableLaneWrite`); `legion state backup`
becomes a fan-in export with per-shard applied indexes, because "one node
leads every group" is deliberately never true again; `/readyz`'s
divergence grace becomes shard-aware.

*Rung C — online resharding.* `legion raft shard move-pool <pool> --to <k>`
(quiesce the pool's evals, whole-file-snapshot the pool's rows into the
target shard, flip the replicated pool→shard map, sweep the source) and
`shard add`. Hash sub-shards for a pool bigger than one shard — the only
place a job's placements would span shards — come last, because they
require the split-apply saga: job-scope admission in the job's shard, per
node-shard sub-applies, coupled components routed to one shard. That is the
one part of the design with genuinely new failure modes (a stuck sub-apply
holding a job's admission), and it is deliberately last.

**The engineering bill (rung A), ranked by size.**

1. **A multi-file `Store`.** One write connection per shard; read
   connections open `store.sqlite` as `main`, `ATTACH` every shard file,
   and create `TEMP` views that `UNION ALL` each scheduler table across
   shards, so the ~all of `Store`'s read SQL keeps working unqualified.
   Migrations run per file. The default attach limit of 10 caps v1 at 8
   shards (main + 8 + headroom) — comfortably above what one box's cores
   can use, and it matches the widened id stride below.
2. **Routing.** A replicated `pool → shard` map in the control lane
   (defaults `hash(pool) % N`; `assignments` pins); submit resolves the
   job's shard and rejects multi-shard jobs; mark-lost and remove-node
   target the node's pool's shard; the shard map is part of the topology
   stamp in v1.
3. **Per-shard eval claims** and the per-shard leadership gate (the probe
   is already per group).
4. **Cross-file consequences to audit, not build:** the cross-lane
   `REFERENCES … ON DELETE CASCADE` constraints (§5.2) become unenforceable
   across files — the composites already delete in dependency order, so an
   audit test that no composite relies on a cascade is the deliverable;
   `scheduler_event` rows become shard-resident (unioned for reads; the
   event-stream cursor already tolerates per-process id ordering).
5. **Format items** bundled into the one JC-43 break that enabling shards
   already is: shard wire names / data dirs / `raft_applied` files,
   `topology.json` recording the shard map, widening `ID_STRIDE` 8 → 64.

**Operator experience.** As typed:

```nickel
persistence = {
  type = "raft",
  raft = {
    topology = "lanes",
    scheduler = {
      shards = 4,                 # scheduler raft groups + state files; default 1
      # pools map to shards by hash(pool) % shards unless pinned:
      assignments = { batch = 2, "ml-training" = 3 },
    },
  },
}
```

```text
<storePath>/store.sqlite            # control + status + commutative tables
<storePath>/scheduler-0.sqlite      # shard 0: allocation, eval, epochs, plan_submission, rollout, …
<storePath>/scheduler-1.sqlite
<dataDir>/scheduler-0/log.sqlite    # per-shard raft log + snapshots, as lanes have today
```

```console
$ legion raft status
lane=control      role=leader  term=12
lane=status       role=leader  term=9
lane=scheduler/0  role=leader  term=7  pools=[default, web]   queue=3    file=41 MB
lane=scheduler/1  role=leader  term=8  pools=[api]            queue=0    file=12 MB
lane=scheduler/2  role=leader  term=7  pools=[batch]          queue=412  file=380 MB  ⚠ hot
lane=scheduler/3  role=leader  term=6  pools=[ml-training]    queue=1    file=9 MB

$ legion state backup --out ./bk
wrote ./bk/store.sqlite, ./bk/scheduler-{0..3}.sqlite, ./bk/manifest.json (applied: control=18042 status=9911 scheduler/0=7720 …)
```

- A job whose tasks bind pools on different shards is rejected at submit
  with the fix named:
  `pools "web" and "batch" map to scheduler shards 0 and 2; pin them to one
  shard (raft.scheduler.assignments) or split the job`.
- Per-shard metrics: the `legion_raft_*`, pipeline, and eval-queue series
  gain a `shard` label; "which shard is hot" is a dashboard read.
- v1 resharding (changing `shards` or `assignments`) is a topology change
  — backup → fresh cluster → restore — the same rule as `single`↔`lanes`.
  Rung C makes it a command.
- Jobspec: unchanged. Workers: unchanged. `single` topology: unchanged
  (shards require `lanes`).

**Honest limits.** Capacity does not pool across shards: a pool pinned to
shard 2 cannot borrow shard 0's idle nodes, which is the Borg-cell cost in
miniature and the reason `default` stays a single pool until rung C's
sub-shards. Per-manager memory is untouched (§1.4). And rung A's ceiling is
one box: a manager whose cores or NVMe saturate under N shards needs rung B.

### M5. Sharding the other lanes — hold for evidence

**Status lane, by node hash:** `node_health` / `allocation_health` rows are
strictly per node, so hash-bucketed status shards have zero cross-shard
writes; with per-file shards each is just another file. But
liveness-off-raft removed steady-state heartbeats from raft entirely — what
remains is transitions and health changes, group-committed. Until a
measured fleet shows transition storms saturating the status pipeline
(100k+ nodes in a mass restart), this is machinery without a customer, and
rung A makes it cheap later. **Control lane / KV:** user-driven and
low-rate; KV-by-prefix is sound but speculative. Defer without a design.

### M6. Federation: more clusters — the outer axis, complementary

Fully planned in [`multi-cluster-federation-plan.md`](multi-cluster-federation-plan.md);
not re-litigated. What this document adds is positioning: federation is the
only method that scales **every** axis — including per-manager memory and
geography — because nothing is shared; its cost is N control planes and
cluster-granular placement. **M4 raises the single-cluster ceiling so
operators choose federation for geography, tenancy, and blast radius, not
because the scheduler ran out.**

### M0 (the null hypothesis). Keep exhausting the single-group levers

Sequenced first: phase 5 showed in-group levers still paying and ended
driver-bound with the ceiling unlocated. Remaining in-group headroom is
real (native submitter, eval-thread knee above 32). Every method above is
more valuable and more correctly designed after the post-batching wall is
measured and attributed — Phase 0 below makes that cheap.

### Considered and rejected on the way

- **Pipelined captures on one group** (allow capture k+1 before apply k for
  disjoint shards, tracking per-shard applied indexes in one log) — it
  re-implements what independent groups give for free, and breaks
  "captures run on applied state". Groups are the simpler mechanism.
- **Scheduler tables in their own file, still one group** — one writer,
  nothing gained.
- **Leader direct-commit at capture** (skip rollback + re-apply) —
  measured at ~17% by the phase-5 split bench and declined for the
  crash-consistency reasons recorded there; not a substitute for a second
  writer.
- **In-memory state machine (D3)** — would shrink `W_c + W_a` to
  microseconds and make shared-file groups scale; rejected by the
  architecture review for operability, and per-file shards reach the same
  parallelism while keeping the SQLite-file-you-can-query property.

## 7. What any sharding must preserve (the invariant checklist)

1. **Single-writer ownership per replicated row** — table-granular today,
   file-granular under M4 — enforced at capture time in every topology,
   with exhaustiveness and disjointness tests extended, never weakened.
2. **No oversubscription by construction.** A stale read costs a conflict
   or a rebuild, never overfill. (Kills M2.)
3. **The epoch fence and apply-time revalidation** survive unchanged within
   each shard.
4. **Composites complete under whatever leadership placement the rung
   allows** — inline under rung A, via forwarding + reconcilers under
   rung B.
5. **Jobspec and worker plane unchanged.** Pools remain the only
   user-visible partition vocabulary; one join token enrolls a manager into
   every group.
6. **Formats change only at declared break points** — enabling/resizing is
   a topology change (v1) or a `shard move-pool` (rung C); the
   `topology.json` stamp carries the shard map so mixed versions refuse
   loudly.
7. **The status surface names the hot group** — per-shard rows in
   `legion raft status`, per-shard metric labels, shard-aware `readyz`.
8. **`shards = 1` is behaviorally identical to today** and passes the
   whole existing raft battery. (Byte-identical files are *not* promised:
   under `lanes`, scheduler state moves into `scheduler-0.sqlite` even at
   N = 1, so there is one code path; `lanes` is opt-in and its on-disk
   shape is stamped. `single` is byte-identical.)

## 8. Comparison

| Method | Eval/alloc throughput | Node axis | Blast radius | Format break | New correctness surface | Operator burden | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M1 more write-class lanes | ~none (one writer; classes exhausted) | none | worse (more composites) | yes, per lane | composite half-applies | +1 lane per feature | **refuse** |
| M2 job-keyed shards | high on paper | none | per shard | yes | **oversubscription across shards** | reshard = re-hash jobs | **reject → M3 + M4** |
| M3 plan shipping | planner × managers | none | none | none | staleness → conflicts | zero | seam; fallback for rung B |
| M4 raft groups over one file (rev 1) | ≈ ×1.1 on LAN (§1.2) | — | per pool | yes | row-scoped snapshots | one knob | **dropped** |
| **M4 rung A** per-file shards, co-located | writers × N, pipelines × N on one box | planning ÷ N; memory unchanged | per pool | yes (enable/resize) | routing + cross-file FK audit | one knob; N files; per-shard status | **the axis — gate on Phase 0** |
| M4 rung B spread leadership | + planner/apply/fsync × managers | — | per pool | no (policy) | forwarding as fast path; backup fan-in | leadership map to read | on one-box saturation |
| M4 rung C online reshard / sub-shards | — | — | — | no | split-apply saga (sub-shards only) | `shard move-pool` | on demand |
| M5 status/KV shards | n/a | transition-storm headroom | per bucket | yes | shares rung A's | near zero | hold |
| M6 federation | × clusters | × clusters (incl. memory) | per cluster | no (additive) | none new | N clusters, peering | planned; complementary |

## 9. Sequencing (each step gated)

1. **Phase 0 — locate and attribute the wall.** Three parts, the first two
   nearly free:
   - Compute `(W_c + W_a) / T` from the archived campaigns' existing
     `legion_raft_capture_seconds` and `legion_raft_propose_apply_seconds`
     histograms on the scheduler lane (the split bench gives `W_a`'s share
     of the propose-apply number). This alone decides whether shared-file
     groups could ever pay, and it should confirm §1.2.
   - An in-process micro-benchmark: N pipelines over one file versus N
     files, same batch shape — the direct measurement of the per-file
     claim on the target disk.
   - The fast native submitter (already TODO's top gate) to re-run the fill
     ladder for the production number.
   **Exit:** a measured post-batching ceiling with a named bottleneck, and
   a measured per-file multiplier.
2. **Phase 1 — graduate rung A** into an implementation plan (only if
   Phase 0 names the writer/pipeline): multi-file Store with union views,
   routing map, per-shard claims, FK/cascade audit, format bundle,
   `raft.scheduler.shards`. Headline gate: a lanes-style A/B — alloc/s at
   shards = 1/2/4 on one box, plus a one-pool storm leaving sibling shards'
   p99 flat. Safety gate: `shards = 1` equivalence.
3. **Phase 2 — rung B** when a shard-A deployment shows one manager's cores
   or fsync saturating: spread policy, forwarding fast path, backup fan-in,
   shard-aware `readyz`.
4. **Phase 3 — rung C** on operator demand: `shard move-pool`, `shard add`;
   hash sub-shards last and only for a measured oversized pool.
5. **Phase 4 — M3** only if a shard leader is planner-bound with an idle
   pipeline and rung B is unavailable.
6. **Phase 5 — M5 status shards** on transition-storm evidence.
7. **Federation proceeds on its own plan throughout.**

## 10. Testing story (for the graduating plan)

- **Equivalence:** `shards = 1` passes the whole existing raft battery;
  union-view reads return the same rows as today's single-table reads (a
  differential test over the read API).
- **Ownership:** exhaustiveness/disjointness per file; a changeset whose
  rows resolve to another shard is rejected before proposal; the
  two-shards-one-node oversubscription scenario is *unconstructible*.
- **Cascades:** an audit test asserting no composite depends on a
  cross-lane `ON DELETE CASCADE`, run with foreign keys disabled.
- **Snapshots:** whole-file round-trip per shard; install refusal on a
  shard-map mismatch; `move-pool` round-trip (rung C).
- **Isolation/scaling A/B** (`tests/raft_perf_smoke.rs` precedent):
  alloc/s vs shard count on one box; one-pool storm isolation.
- **Failure:** kill the leader → every shard fails over together (rung A)
  and independently (rung B); queue recovery on acquisition per shard.
- **Skew:** a pre-shard manager joining a sharded cluster is refused by the
  topology stamp with the upgrade path named.

## 11. Open questions, with leans

1. **Multi-pool jobs.** Lean: reject at submit naming the fix; revisit via
   co-shard pinning, never via cross-shard two-phase apply outside rung C.
2. **A node changing pools on re-registration** while it holds
   allocations. Lean: reject in v1 (its rows would have to change files);
   rung C's `move-pool` machinery is the eventual answer.
3. **`default` pool sizing.** Lean: `default` is a pool like any other,
   pinnable; a giant `default` is rung C's sub-shard customer.
4. **Should `status` get its own file at the same time** (one file per
   group, uniformly)? Lean: no — one break at a time; status stays in
   `main` until M5 has a customer.
5. **Attach cap.** Lean: cap `shards` at 8 in v1 (default
   `SQLITE_MAX_ATTACHED`, matches the residue budget); raise via a bundled
   build define only when rung B needs more shards than one box can use.
6. **Backup shape under rung B.** Lean: per-shard export with per-shard
   applied indexes, restore reassembling into one adopted baseline — lands
   *with* rung B, not after.
7. **Read-model scoping.** Lean: keep the global mirror; scope only the
   planner's view per shard (the pool-partitioned index nearly provides
   it). Per-shard mirrors trade failover for memory — M6 territory.
8. **Event-id stride.** Lean: 64 (up to 61 shards plus the three lanes;
   bundled into the shard-enable break).
