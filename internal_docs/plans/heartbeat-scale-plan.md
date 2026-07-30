# Heartbeat Path at Scale (benchmark + optimization pass)

**Status: IN EXECUTION** — the local-measurable slice of architecture-review
item D8 ([`scheduler-architecture-review-plan.md`](scheduler-architecture-review-plan.md)).
D8's *deferral* was about deciding between big architectures
(gossip liveness, worker-side batching protocols) — that decision still
waits for AWS-test data. This plan is the pass that removes the
measurable per-heartbeat and per-tick waste that no architecture change
would forgive, and builds the benchmark that the AWS decision will read
from.

> **Judgement calls:** [`../calls/heartbeat-scale-plan_calls.md`](../calls/heartbeat-scale-plan_calls.md)

## Problem

At 10k workers on the default 2s cadence, the manager fields 5,000
heartbeats/second. Today each one costs, unconditionally
(`src/manager/server.rs:4548`, `src/storage/store.rs:3897`):

- **One raft consensus round** (a `client_write` through a single-threaded
  per-lane pipeline behind a global capture mutex) *per heartbeat* — plus
  a second one when the worker reports allocation health (i.e. always,
  on any node running tasks). Measured status-lane throughput on
  loopback is ~500-2,000 ops/s (`docs/multi-raft.md:527`), so 10k
  workers is 3-10x over the ceiling before scheduling does any work.
- An `info!` log line per heartbeat (5,000 lines/s of firehose).
- A redundant `get_node` re-read plus a read-model write-lock
  reprojection (`cache_node`) even when nothing scheduler-visible
  changed (the node was already active).
- An unconditional `volumes_to_destroy_for_node` query whose
  `OR`-disjunction shape scans the volume table even when no volume is
  being destroyed.
- Liveness scanning (`src/manager/daemon.rs:668`): a full
  `node ⟕ node_health` scan every 2s plus an RFC3339 **string parse per
  node per tick** (10k parses every 2s), and `recheck_lost_nodes` re-runs
  a per-lost-node eval-existence query every tick forever.
- Worker side (`src/worker/daemon.rs:408`): fixed 2s sleep, **no
  jitter** — a fleet restarted together stays phase-locked forever — and
  the cadence is a hard-coded constant, not configurable.

Nothing measures any of this: there is no heartbeat benchmark
(`tests/raft_perf_smoke.rs` floods heartbeats only as background load
for a scheduler-lane isolation A/B).

## Prior art

- **Nomad**: client heartbeats are leader-local soft state with TTL
  timers; only *status transitions* (alive→down) go through raft. Nomad
  jitters heartbeat intervals and scales the TTL with cluster size
  (`heartbeat_grace`, dynamic rate).
- **Kubernetes**: moved node heartbeats out of node objects into cheap
  `Lease` objects (dedicated etcd keyspace, small values) after
  full-object heartbeats melted etcd at 5k nodes; kubelet posts leases
  every 10s, status only on change or every 5m.
- **Serf/Consul**: gossip-based failure detection — no per-node writes to
  the consensus store at all. The review's D8 names this as the big-gun
  option if the AWS test demands it.

The common shape: **bare liveness is soft state; consensus is for
transitions.** This plan moves Legion toward that shape without changing
where liveness is stored yet (see non-goals).

## Jobs to be done

**Cluster operator**
- When I connect thousands of workers, I want heartbeat overhead to stay
  a small fraction of manager capacity, so scheduling throughput is
  spent on scheduling.
- When a rack restarts, I want workers to not re-synchronize into a
  thundering herd, so load stays smooth.
- When I plan capacity, I want a published heartbeats/second number for
  the manager on each backend, so I can size fleets honestly.

**Legion maintainer**
- When I optimize the heartbeat path, I want a benchmark that isolates
  it, so before/after is a number and regressions get caught.

## Goals

- A heartbeat scale benchmark (fleet size × backend, reporting
  heartbeats/sec and latency percentiles) with recorded numbers.
- Batch/coalesce heartbeat commits: many workers' heartbeats within a
  commit window become **one** status-lane write (one raft round).
- Strip per-heartbeat waste: the log line, the redundant re-read and
  reprojection, the unconditional volume scan.
- Make the liveness tick cheap: no per-node timestamp string parses, no
  per-lost-node re-queries every tick.
- Worker-side jitter, and a configurable heartbeat interval with the
  lost-window coupling documented.

## Non-goals

- Gossip/Serf-style liveness, lease objects, or moving liveness out of
  the store — those are the D8 architecture decisions that wait for AWS
  data; this plan makes the current shape as fast as it can honestly be.
- Changing the 10s lost-node detection window default.
- Worker-side batching of status updates beyond what already rides the
  heartbeat RPC.

## What exists today

See problem statement; the load-bearing code:

- Handler: `src/manager/server.rs:4548-4605`; store op:
  `Store::update_node_heartbeat` → `update_node_heartbeat_apply`
  (`src/storage/store.rs:3897-3949`), a `replicated(Lane::Status, ..)`
  single-row upsert + `get_node` + `cache_node`.
- Liveness: `src/manager/daemon.rs:668-751` (tick),
  `nodes_to_mark_lost` (`:929`), `Node::last_heartbeat_too_slow`
  (`src/structs/node.rs:68` — RFC3339 parse per call),
  `recheck_lost_nodes` (`store.rs:3635`).
- Worker loop: `src/worker/daemon.rs:408-468`;
  `WORKER_HEARTBEAT_DURATION_SEC = 2` (`src/constants.rs:41`).
- Batching precedent: `mark_nodes_as_lost_apply` batches N nodes into
  one `IN (...)` statement in one replicated op (`store.rs:3499`).
- The raft driver skips proposing entirely when a mutation captures an
  empty changeset (`driver.rs:1359`).

## Proposed design

### 1. Heartbeat scale benchmark (first, so before/after exists)

`tests/heartbeat_scale_benchmark.rs`, `#[ignore]`, mirroring the
scheduler scale benchmark's shape: fleet sizes {1k, 5k, 10k} × backends
{local_disk, raft1, raft3} × driver threads {4, 16}, each thread
heartbeating a partition of the fleet as fast as the store allows for a
fixed duration. Reports heartbeats/sec, p50/p99 latency, and (for the
liveness half) the wall-clock of one liveness tick at each fleet size.
JSON results to `target/benchmark-results/heartbeat-<stamp>.json`;
numbers recorded in `benchmarks/scheduler/README.md` alongside the
scheduler tables.

### 2. Heartbeat group commit

`update_node_heartbeat` goes through a coalescer (same leader-elect
drain pattern as the plan-apply coalescer): concurrent heartbeat RPCs
enqueue node ids; the batch leader commits **one** replicated
status-lane op `update_node_heartbeats(batch)` — a single `IN (...)`
upsert stamping one timestamp for the whole batch (the pattern
`mark_nodes_as_lost_apply` already proves), deduplicated by node id.
Callers still block until their batch commits, so failure semantics and
worker retry behavior are unchanged. A lone heartbeat commits exactly as
today. Timestamp skew within a batch is bounded by the batch window
(milliseconds) — noise against the 10s lost window.

### 3. Per-heartbeat waste removal

- The `info!` per heartbeat becomes `debug!`.
- The apply skips `get_node` + `cache_node` when the node was already
  `active` (only `last_heartbeat` changed — invisible to the scheduler
  read model). Status transitions (lost→active) keep the full
  reprojection.
- A partial index `ON volume(state) WHERE state = 'destroying'` (plus a
  fast `EXISTS` guard) so the per-heartbeat destroy-scan is O(matching)
  instead of O(volumes).

### 4. Cheap liveness ticks

- `node_health` gains `last_heartbeat_unix INTEGER` (migration;
  RFC3339 text column stays for display/API compatibility). The tick
  compares integers against one cutoff — zero parses. Rows written
  before the migration have NULL unix values: the reader falls back to
  parsing the text column once, so old rows age out naturally.
- `recheck_lost_nodes` keeps its safety-net role but stops re-querying
  eval existence for every lost node every tick: it remembers (in
  leader-local memory) which lost nodes it has already ensured a
  reschedule for, and only re-runs the existence check when that memory
  is cold (leader change, restart) or the node re-transitions.

### 5. Worker cadence

- Jitter: each sleep is `interval ± 10%` (uniform), breaking phase lock.
- `heartbeat_seconds` worker-config field (default 2, min 1, max 30;
  values above 3 log a warning naming the coupling). The lost window
  stays 10s and the doc states the rule: the manager marks nodes lost
  after 10s, so intervals above ~3s trade detection margin for load.

## Config / persistence changes

- Worker config: `heartbeat_seconds` (optional, default 2).
- Migration: `ALTER TABLE node_health ADD COLUMN last_heartbeat_unix
  INTEGER` + partial index on `volume(state)`.
- No proto changes; no job-config changes (maximal.jsonnet unaffected).

## Implementation phases

1. Benchmark + baseline numbers (no behavior change).
2. Waste removal + liveness-tick fixes (log level, skip-reprojection,
   partial index, unix column, recheck memory).
3. Heartbeat group commit.
4. Worker jitter + configurable interval.

## Testing plan

- Existing liveness tests stay green (`daemon.rs` grace-window tests,
  `raft_lanes.rs` status-lane visibility + forwarding suites,
  `metrics_endpoint.rs` heartbeat counter).
- New: batched-heartbeat unit tests (N ids → all stamped, one op;
  dedupe; unknown ids ignored exactly as today); active-node fast path
  (no reprojection) vs lost→active transition (reprojected);
  unix/text fallback parity test; recheck-memory test (no duplicate
  reschedule evals; memory resets on transition); jitter bounds test.
- Benchmark before/after recorded per phase.

## Rollout

Batching and waste removal are manager-internal with no visible
behavior change (heartbeat RPC semantics identical). The migration is
additive. The worker `heartbeat_seconds` field is optional with the old
constant as default; a worker with the field talks to any manager
version (the interval is worker-local). Documented in
`docs/architecture-managers-workers.md` (heartbeat/liveness section) and
`docs/testing-and-benchmarks.md` (the new benchmark).
