# Worker Simulation (`legion start worker --simulate N`)

**Status: SHIPPED** — all three phases landed together (engine +
campaign knobs + docs, 2026-07-30): `src/worker/simulate.rs`, CLI flags
on `start worker`, `tests/integration/worker_simulation.rs`,
`docs/worker-simulation.md`. Deliberately deferred: nothing from the
plan; the open questions resolved as leaned (one pre-register, no
kill-percent RPC).

> **Judgement calls:** [`../calls/worker-simulation-plan_calls.md`](../calls/worker-simulation-plan_calls.md)

This is the Phase-0 mini-plan required by
[`aws-comparative-benchmark-plan.md`](aws-comparative-benchmark-plan.md)
("Prerequisite: Legion worker simulation"). It gates the whole AWS
campaign: without it, Legion is the only system in the comparison that
has to pay one OS process per node while Nomad gets
[nomad-nodesim](https://github.com/hashicorp-forge/nomad-nodesim) and
Kubernetes gets [KWOK](https://kwok.sigs.k8s.io/).

## Problem

Legion has no fake-worker mode. Today the only way to put 1,000 nodes in
front of the scheduler over the real wire is 1,000 worker processes, each
paying a tokio runtime, its own gRPC channel, a fingerprint pass, a
worker gRPC server, and a state directory (`src/worker/daemon.rs:125-289`
starts all of it unconditionally). Hundreds per host is realistic;
thousands is not. Benchmark code that inserts `NodeMsg` rows directly
(`tests/integration/benchmark_support/mod.rs:257-280`) bypasses the wire entirely, so
it cannot exercise registration, heartbeat pressure, or liveness — the
exact things the AWS campaign must measure.

## Prior art

- **nomad-nodesim**: one process, N simulated Nomad clients over shared
  connections; registers nodes, heartbeats, and fetches allocations
  without running them. The shape this plan copies.
- **KWOK**: fake kubelets driven by "stages" (configurable delays for
  pod Ready transitions). We copy the single useful knob — a delay
  before a placed allocation reports running — and skip the stage
  machinery.
- **kubemark**: hollow kubelets, one pod per fake node — heavier than
  KWOK for no benchmark benefit; rejected the analogous "run the real
  task manager with a noop executor" design for the same reason.

## Jobs to be done

**Benchmark maintainer**
- When I drive the AWS campaign, I want one cheap instance to present
  ~1,250 nodes to the managers over the real wire, so control-plane
  scale costs benchmark dollars instead of fleet dollars.
- When I define a scale step, I want the simulated fleet to reuse the
  benchmark `node_profiles` shape I already have, so one file describes
  the fleet for both the local matrix and the AWS rig.

**Legion maintainer**
- When I change heartbeat, registration, or liveness code, I want a
  local way to point thousands of wire-real nodes at one manager, so
  scale regressions show up on my laptop and not in a campaign.

**Cluster operator**
- When a simulated fleet is attached to a cluster, I want those nodes
  clearly marked, so I never mistake them for capacity (`simulated:
  true` in attributes, visible in `legion node list`).

*Not in scope:* the autonomous-agent persona — this is a load-generation
surface, not an operational one; agents get nothing new beyond the
existing `node list` output where sim nodes are visible.

## Goals

- One process multiplexes N registered nodes over a small set of shared
  gRPC channels: pre-register, register, heartbeat, health-check report,
  allocation acks — the full worker-plane wire surface.
- Fingerprints come from a profile file reusing the benchmark
  `node_profiles` schema (`tests/integration/benchmark_support/mod.rs:47-60`).
- Dispatched allocations are accepted and acknowledged after a
  configurable delay (default: next heartbeat), and dropped when the
  manager stops desiring them. No task executes.
- Same cadences and retry semantics as a real worker (heartbeat
  interval, health-check interval, re-register on `Unauthenticated`),
  so manager-side load per simulated node ≈ load per real idle node.
- 10k nodes in one process on a `c7i.2xlarge`-class box (8 vCPU/16 GiB)
  with headroom; 1,250/process is the campaign operating point.

## Non-goals

- Running tasks, even noop ones — the runtime layer is exactly what we
  are not measuring. (The noop *runtime* still exists for real workers;
  a sim node never launches anything.)
- Simulating exec, logs, artifacts, volumes, service-discovery
  datapath, or workload identity. The sim process serves a stub worker
  endpoint that accepts task dispatch and answers everything else
  `Unimplemented`; `legion exec`/log streaming against a sim node fail
  cleanly and the docs say so.
- Fault injection (dropped heartbeats, slow acks) beyond `--ready-after`.
  The AWS churn/storm scenarios kill sim processes wholesale instead —
  same effect, no new surface.
- Multi-manager client-side balancing. Sim nodes dial the one address in
  the join token, like real workers do today.

## What exists today

- `src/worker/daemon.rs:88-98` `run_worker`: decode join token →
  `run_worker_with_manager` (connect, pre-register for `WorkerConfig`,
  register, then spawn heartbeat/health/log/self-fence loops and the
  worker gRPC server).
- Registration payload (`src/worker/daemon.rs:79-86,242-248`): `name`,
  `pool`, `config_attrs` (attributes + `resources` + `reservedResources`
  + `workerAddress` + optional `maxAllocations`), `fingerprint_attrs`
  (from `fingerprint_node`, `src/worker/node_check.rs`), `health_checks`.
- Heartbeat round-trip (`src/worker/daemon.rs:472-635`):
  `HeartbeatRequest { id, allocation_ids, failed_allocations,
  allocation_health, volume_reports }` → `HeartbeatReply {
  desired_allocations, volume_destroy, upstream_endpoints }`. Note:
  allocation state is *declarative* — rows are `placed` at plan apply
  (`src/storage/store.rs:6673`) and the manager currently ignores the
  reported `allocation_ids` (`src/manager/server.rs:6973`,
  `_reported_allocations`); heartbeats matter for liveness
  (`update_node_heartbeat`), failure reports, and `AllocationHealth`
  snapshots (which gate health-checked rollouts and discovery).
- Dispatch is push *and* pull: plan apply pushes `PostWork` to the
  node's advertised `workerAddress` (`src/manager/dispatcher.rs`;
  address missing ⇒ dispatch skipped with a log), and every heartbeat
  reply re-delivers the desired set.
- Liveness: heartbeats every `WORKER_HEARTBEAT_DURATION_SEC` (2s);
  the manager marks a node lost after `DEF_LOST_NODE_AT_SEC` (10s)
  without one (`src/manager/daemon.rs:684-713`, `src/constants.rs`);
  health-check RPCs every `WORKER_NODE_CHECK_DURATION_SEC` (30s).
- Credentials: join secret at register, per-node `node_token` attached
  to every worker-plane RPC afterwards (`src/worker/daemon.rs:34-63`);
  `Unauthenticated` heartbeats trigger re-registration
  (`src/worker/daemon.rs:444-455`).
- Benchmark profile schema: `NodeProfile { name, count, memory_gb,
  cpus, attrs }` (`tests/integration/benchmark_support/mod.rs:47-60`), fingerprint
  shape `{ memory_total, cpus: { "cpu-N": { frequency } } }`
  (`tests/integration/benchmark_support/mod.rs:235-244`).

## Proposed design

### 1. Surface

`--simulate N` extends `legion start worker` (no new verb):

```bash
legion start worker $TOKEN --simulate 1000 \
  --profiles benchmarks/scheduler/scenarios/web_fleet.json \
  --name-prefix sim-a \
  --ready-after 0s \
  --register-rate 100
```

- `--simulate <N>` — run N simulated nodes instead of one real worker.
  All other simulation flags require it.
- `--profiles <FILE>` — JSON file with a `node_profiles` array (the
  benchmark scenario schema; a scenario file works as-is). N is
  distributed across profiles proportionally to their `count` weights
  (largest-remainder rounding), so the same file serves every scale
  step. Default: one uniform profile (4 CPUs, 32 GiB, no attrs).
- `--name-prefix <P>` (default `sim`) — node ids are
  `<prefix>-<profile>-<idx>`, deterministic so a restarted process
  re-registers the same nodes instead of doubling the fleet.
- `--ready-after <DUR>` (default `0s`) — delay between receiving a
  desired allocation and reporting it in `allocation_ids` (the KWOK
  stage-delay analog; `0s` means "acked on the next heartbeat").
- `--register-rate <N/s>` (default 100) — registration ramp, so 10k
  nodes don't stampede a fresh manager in one second.

Every sim node carries `"simulated": true` in `config_attrs` so
operators (and result audits) can tell fleets apart:

```
$ legion node list -j | jq '.data[] | select(.attributes.simulated) | .id' | head -2
"sim-a-web-east-0"
"sim-a-web-east-1"
```

### 2. Engine (`src/worker/simulate.rs`)

One tokio runtime for the whole process:

- **Channels**: one shared tonic `Channel` per `SIM_NODES_PER_CHANNEL`
  (256) nodes, round-robin assigned. HTTP/2 multiplexes streams, so a
  handful of connections carries thousands of nodes — this mirrors
  nodesim and is the fairness argument for the "shared channels" claim.
- **Per node**: a `SimNode` holding name, profile-derived registration
  payload, its own `WorkerCredentials` (node tokens are per-node), and
  the set of currently-acked allocation ids with their first-seen
  timestamps (for `--ready-after`).
- **Registration**: one pre-register per process fetches the
  `WorkerConfig` (pool, resource defaults); each node then registers at
  the `--register-rate` ramp. Fingerprints are synthesized from the
  profile — `fingerprint_node()` is never called (it reads the host).
- **Heartbeat loop per node**: initial offset uniformly staggered
  across the heartbeat interval, then the standard cadence. Each tick
  reports acked allocation ids (informational manager-side today, but
  the wire bytes and handler work match a real fleet); the reply's
  `desired_allocations` become acked (after `--ready-after`); ids the
  manager stopped desiring are dropped. Failures back off and
  re-register on `Unauthenticated`, same as the real loop. No volume
  reports, no failure reports, no upstream syncing.
- **Health checks**: one synthesized passing check set at registration,
  re-sent on the real worker's health-check cadence so the manager-side
  write load matches a real fleet.
- **Shared stub worker server**: one tonic `Worker` server per process,
  advertised as every sim node's `workerAddress`, so plan-apply push
  dispatch (`PostWork`) — a real control-plane cost — stays in the
  measurement instead of being skipped for want of an address.
  `PostWork` routes on the payload's `node_id` and marks the allocation
  desired immediately (pull would deliver it next tick anyway);
  `GetTaskLogs`/`ExecTask`/`RestartWorker` return `Unimplemented`.
- **Not started**: task manager, state store, identity manager,
  forwarder, self-fence watchdog, log maintenance.
- **Process output**: one `info!` summary line every 30s — nodes
  registered/live, heartbeats/s achieved vs expected, acked
  allocations — so campaign scripts can assert fleet health cheaply.

### 3. Config / proto / persistence changes

None. The simulation speaks the existing worker-plane RPCs with existing
messages, and nothing about a sim node is persisted worker-side. This is
deliberate: any wire change here would invalidate the fairness claim
that sim nodes exercise the same manager paths as real ones.

## Phases

1. **Core engine**: `--simulate N` with the uniform default profile —
   register, heartbeat, ack, drop; summary logging. Ships alone: already
   enough for local scale testing of manager/heartbeat changes.
2. **Campaign knobs**: `--profiles`, `--name-prefix`, `--ready-after`,
   `--register-rate`, health-check cadence. Ships alone: everything the
   AWS rig invokes.
3. **Docs**: public page (`docs/worker-simulation.md`), TODO updates.
   No capabilities-catalog entry: `start`/`join` are deliberately outside
   the agent surface (`docs/agent-readiness.md` "operator-oriented"
   list), and simulation does not change that.

## Testing plan

- **Unit** (`src/worker/simulate.rs` `#[cfg(test)]`): profile-weighted
  distribution of N (exact totals, largest-remainder rounding, 1-node
  and N<profiles edge cases); deterministic naming; ack bookkeeping
  (desired → acked after ready-after, undesired → dropped).
- **Integration** (`tests/integration/worker_simulation.rs`): serve a real
  `ManagerDaemonService` (`src/manager/server.rs:1005`, ACLs disabled)
  over tonic on a loopback port; run a sim fleet against it; assert
  every node registers with profile-correct fingerprints and
  `simulated: true` and that `last_heartbeat` advances; submit a noop
  job via the service; assert allocations are placed on sim nodes and
  that the fleet acks them (desired set echoed) within a deadline;
  drop the fleet and assert nothing panics manager-side.
- **Liveness**: sim heartbeat cadence is the real worker's constant
  (`WORKER_HEARTBEAT_DURATION_SEC`), asserted in a unit test, so the
  manager's 10s lost-node sweep applies to sim nodes exactly as to
  real ones — the liveness path is shared, not forked.

## Rollout and compatibility

Additive flag on an existing command; no config, proto, or state
change; default behavior of `start worker` untouched. The flag ships
documented as a benchmarking/load-testing surface in
`docs/worker-simulation.md` with the limitation list (no exec, no logs,
no volumes, not capacity). `src/cmd/capabilities.rs` gets the flag noted
on the existing `start worker` entry.

## Open questions

- Whether 10k nodes in one process needs more than one pre-register
  (per-profile worker types). Lean: no — worker type only selects the
  `WorkerConfig`, and the campaign uses one type; revisit if profiles
  ever need distinct pools.
- Whether the AWS driver needs a "kill n% of sim nodes" RPC into the
  sim process for storm scenarios. Lean: no — the driver runs one sim
  process per instance slice and kills processes; coarser but honest.
