# Worker Simulation

`legion start worker --simulate N` runs N simulated nodes in one process
instead of one real worker. Use it to benchmark and load test the manager
and scheduler. Each simulated node registers, heartbeats, and reports
health over the normal worker-plane wire protocol at the same cadence as
a real worker. So N simulated nodes put the same control-plane load on a
manager as N idle real workers, at a small fraction of the process cost.
It is Legion's counterpart to Nomad's nodesim and Kubernetes' KWOK.

Simulated nodes never execute tasks. Allocations placed on them are
accepted, acknowledged, and dropped when the manager no longer wants
them. Nothing runs.

## Usage

```bash
legion start worker $TOKEN --simulate 1000 \
  --profiles benchmarks/scheduler/scenarios/web_fleet.json \
  --name-prefix sim-a \
  --ready-after 500ms \
  --register-rate 100 \
  --advertise-addr 10.0.1.17
```

`$TOKEN` is an ordinary join token (`legion join-token create`). All
flags except `--simulate` are optional:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--simulate <N>` | none | Run N simulated nodes (enables the mode). |
| `--profiles <FILE>` | one uniform profile (4 CPUs, 32 GiB, no attrs) | JSON file with a `node_profiles` array. This is the benchmark scenario schema, so a scenario file works as-is. |
| `--name-prefix <P>` | `sim` | Node ids are `<prefix>-<profile>-<idx>`. |
| `--ready-after <DUR>` | `0s` | Delay before a dispatched allocation is reported running (models task-start latency). |
| `--register-rate <N/s>` | `100` | Registration ramp, so a large fleet doesn't stampede a fresh manager. |
| `--advertise-addr <HOST[:PORT]>` | detected | Host the manager dials for task dispatch to this fleet. Same default as a real worker: `LEGION_WORKER_ADVERTISE_ADDR`, else the local address of the route to the manager (`127.0.0.1` when the manager is on the same machine). |

### Profiles

`--profiles` reuses the benchmark `node_profiles` schema
(`benchmarks/scheduler/scenarios/*.json`). One file describes a fleet
shape for both the local scheduler benchmarks and a simulated cluster:

```json
{
  "node_profiles": [
    { "name": "web",  "count": 3, "memory_gb": 32, "cpus": 8,
      "attrs": { "class": "web", "region": "us-east" } },
    { "name": "util", "count": 1, "memory_gb": 16, "cpus": 4,
      "attrs": { "class": "util" } }
  ]
}
```

The `count` fields are weights. `--simulate N` is split across profiles
in proportion to them (largest-remainder rounding), so the same file
works at every scale step. To reproduce a file's counts literally, pass
N equal to their sum. A profile may also set `max_allocations` to cap
the node's allocation capacity.

`memory_gb` and `cpus` become the node's fingerprinted capacity (each
CPU is fingerprinted at 3 GHz). `attrs` land in the node's attributes,
and job `constraints` and `affinities` match them the same way they
match real node attributes.

### Telling fleets apart

Every simulated node carries `"simulated": true` in its attributes:

```bash
legion node list -j | jq '[.data[] | select(.config_attrs.simulated)] | length'
```

Simulated nodes add no usable capacity. Keep simulated fleets out of
production clusters. If you can't, keep real workloads off them with a
constraint (for example `constraints = { simulated = { operator = "is_not_set" } }`).

## Behavior and fidelity

- **Same wire, same cadence.** Simulated nodes use the same
  registration and heartbeat code as a real worker. Heartbeats follow
  the worker type's `heartbeatSeconds` (default 2s), clamped and jittered
  by ±10% (see
  [architecture-managers-workers.md](architecture-managers-workers.md)).
  Health-check reports go out every 30s, and retries back off
  exponentially with re-registration on `Unauthenticated`. A simulated
  node reports its resolved cadence at registration and honors an
  interval served by the manager (with the same one-shot re-phase) just
  as a real worker does. That means a scale run measures the actual
  controller. The manager's lost-node detection applies unchanged. The
  window is `missedBeatsBeforeLost x that node's interval`, which is 10s
  at the defaults ([heartbeat-cadence-and-scale.md](heartbeat-cadence-and-scale.md)).
- **Shared channels.** Nodes share a small pool of gRPC channels (one
  per 256 nodes, max 16). One process holds a handful of connections
  regardless of fleet size.
- **Dispatch works, execution doesn't.** The process serves one stub
  worker endpoint and advertises it as every node's `workerAddress`, so
  the manager's push dispatch (`PostWork`) gets exercised. Dispatched
  allocations are acknowledged after `--ready-after` and echoed in
  heartbeats. No process is spawned and no runtime plugin loads.
- **One fleet identity.** The stub endpoint is a real mTLS worker plane
  with the same caller classes as a real worker
  ([auth-and-acls.md](auth-and-acls.md#the-worker-plane)). One listener
  serves one certificate for thousands of nodes, so the fleet's first node
  (by name) requests a node certificate after it registers, and every node
  in the process registers that node's id as `workerPlaneName`. Managers
  verify that name, instead of each node's own, only for nodes that also
  report `simulated: true`. Push dispatch starts once that certificate is
  issued; until then heartbeat replies deliver allocations as usual.
- **Restart-safe by naming.** There is no state directory. Node ids are
  deterministic, so a restarted process re-registers the same nodes
  (registration is an upsert). The manager sees a restart as a brief
  fleet blip, the same as a rebooted worker host.
- **Status line.** Every 30s the process logs `Simulated fleet
  status` with nodes registered, achieved vs. expected heartbeat rate,
  cumulative failures, and acked allocations. Scripts can watch this to
  confirm a fleet is healthy.

## Metrics

A simulated host exports the ordinary worker metrics from the ordinary
opt-in listener. Set `LEGION_WORKER_TELEMETRY_PORT` and scrape
`/metrics` as you would on any worker
([metrics.md § Worker](metrics.md#worker)). The listener starts before
the registration ramp, so a scrape during ramp-up shows the climb.

```bash
LEGION_WORKER_TELEMETRY_PORT=9101 legion start worker "$TOKEN" --simulate 1250
curl -s localhost:9101/metrics | grep legion_worker_
```

The usual families (`legion_worker_heartbeats_total`,
`legion_worker_heartbeat_duration_seconds`,
`legion_worker_allocations_running`) aggregate over the whole fleet.
This is one worker process carrying N nodes, so its metrics are per
process. Three more metrics describe the fleet:
`legion_worker_simulated_nodes`, `..._nodes_target`, and
`legion_worker_simulated_registrations_total{outcome}`. If registered
nodes are short of target and the `error` counter is flat, the ramp is
still climbing. If they are short of target and the counter is rising,
the ramp is stuck.

Per-node series are not exported, to keep cardinality down and because
the manager already has that view. OTLP push works the same as it does
for a real worker.

## Limitations

- `legion exec`, log streaming (`legion job logs`), and worker restart
  return `Unimplemented` against simulated nodes.
- Volume drivers are fingerprinted as absent, so jobs that need volumes
  will not place on simulated nodes.
- There is no allocation-health reporting. Tasks with health checks
  never report healthy from a simulated node, so health-gated rollouts
  will not converge there. Use plain noop tasks without checks for load.
- Failure reports, workload identity, service-discovery upstreams, and
  artifact staging are not simulated.
- The fleet-wide allocation gauge is updated incrementally and repaired
  from authoritative state every 30s. Under heavy churn, a scrape can
  disagree with the manager by a few allocations. Aborting a fleet
  leaves the gauges at their teardown values rather than zero. Counters
  and the heartbeat histogram are exact.
- All nodes in one process share one advertised dispatch address. This
  understates the manager's per-node connection cost at very large N,
  since a real fleet dials N addresses. The benchmark fairness notes
  record this.

## Scale guidance

A simulated node costs two lightweight async tasks and a few KB of
state. Thousands of nodes per process run comfortably on a small
instance. The AWS benchmark rig (`benchmarks/aws/`) runs 1,250 nodes per
`c7i.2xlarge` with plenty of headroom. For very large fleets, run
several processes (one per instance) instead of one giant process. The
storm and churn scenarios also need one process per kill target.
