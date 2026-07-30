# Worker Simulation

`legion start worker --simulate N` runs N *simulated* nodes in one
process instead of one real worker. It exists for benchmarking and load
testing the manager and scheduler: each simulated node registers,
heartbeats, and reports health over the unmodified worker-plane wire
protocol at real-worker cadences, so N simulated nodes put the same
control-plane load on a manager as N idle real workers — at a tiny
fraction of the process cost. It is the Legion analog of Nomad's
nodesim and Kubernetes' KWOK.

Simulated nodes never execute tasks. Allocations placed on them are
accepted, acknowledged, and dropped when the manager no longer desires
them — nothing runs.

## Usage

```bash
legion start worker $TOKEN --simulate 1000 \
  --profiles benchmarks/scheduler/scenarios/web_fleet.json \
  --name-prefix sim-a \
  --ready-after 500ms \
  --register-rate 100 \
  --advertise-host 10.0.1.17
```

`$TOKEN` is an ordinary join token (`legion join-token create`). All
flags except `--simulate` are optional:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--simulate <N>` | — | Run N simulated nodes (enables the mode). |
| `--profiles <FILE>` | one uniform profile (4 CPUs, 32 GiB, no attrs) | JSON file with a `node_profiles` array (the benchmark scenario schema — a scenario file works as-is). |
| `--name-prefix <P>` | `sim` | Node ids are `<prefix>-<profile>-<idx>`. |
| `--ready-after <DUR>` | `0s` | Delay before a dispatched allocation is reported running (models task-start latency). |
| `--register-rate <N/s>` | `100` | Registration ramp, so a large fleet doesn't stampede a fresh manager. |
| `--advertise-host <HOST>` | `127.0.0.1` | Host the manager dials for task dispatch to this fleet. Set it to the machine's reachable address when the manager runs elsewhere. |

### Profiles

`--profiles` reuses the benchmark `node_profiles` schema
(`benchmarks/scheduler/scenarios/*.json`), so one file describes a fleet
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

The `count` fields are **weights**, not literal counts: `--simulate N`
is distributed across profiles proportionally (largest-remainder
rounding), so the same file drives every scale step. To reproduce a
file's counts literally, pass N equal to their sum. A profile may also
set `max_allocations` to cap the node's allocation capacity.

`memory_gb` and `cpus` become the node's fingerprinted capacity (each
CPU is fingerprinted at 3 GHz); `attrs` land in the node's attributes
and are matched by job `constraints`/`affinities` exactly like real
node attributes.

### Telling fleets apart

Every simulated node carries `"simulated": true` in its attributes:

```bash
legion node list -j | jq '[.data[] | select(.config_attrs.simulated)] | length'
```

Simulated nodes are **not capacity**. Keep simulated fleets out of
production clusters, or at minimum keep real workloads off them with a
constraint (for example `constraints: { simulated: { operator: "is_not_set" } }`).

## Behavior and fidelity

- **Same wire, same cadence.** Registration, heartbeats at the
  worker-type's cadence (`heartbeatSeconds`, default 2s, clamped and
  ±10%-jittered by the same code as a real worker — see
  [architecture-managers-workers.md](architecture-managers-workers.md)),
  30s health-check reports, exponential retry with re-registration on
  `Unauthenticated`. The manager's lost-node detection (no heartbeat
  for 10s) applies to simulated nodes unchanged.
- **Shared channels.** Nodes multiplex over a small pool of gRPC
  channels (one per 256 nodes, max 16), so one process holds a
  handful of connections regardless of fleet size.
- **Dispatch works, execution doesn't.** The process serves one stub
  worker endpoint, advertised as every node's `workerAddress`, so the
  manager's push dispatch (`PostWork`) is exercised; dispatched
  allocations are acknowledged after `--ready-after` and echoed in
  heartbeats. No process is spawned, no runtime plugin loads.
- **Restart-safe by naming.** There is no state directory; node ids
  are deterministic, and a restarted process re-registers the same
  nodes (registration is an upsert). The manager sees a restart as a
  brief fleet blip, exactly like a rebooted worker host.
- **Status line.** Every 30s the process logs `Simulated fleet
  status` with nodes registered, achieved vs expected heartbeat rate,
  cumulative failures, and acked allocations — scripts can watch this
  to confirm a fleet is healthy.

## Limitations

- `legion exec`, log streaming (`legion job logs`), and worker restart
  return `Unimplemented` against simulated nodes.
- Volume drivers are fingerprinted as absent, so volume-requiring jobs
  will not place on simulated nodes.
- No allocation-health reporting: tasks with health checks never
  report healthy from a simulated node, so health-gated rollouts will
  not converge there. Use plain (checkless) noop tasks for load.
- Failure reports, workload identity, service-discovery upstreams, and
  artifact staging are not simulated.
- All nodes in one process share one advertised dispatch address; the
  manager's per-node connection cost is therefore understated at very
  large N (real fleets dial N addresses). Recorded in the benchmark
  fairness notes.

## Scale guidance

A simulated node costs two lightweight async tasks and a few KB of
state. Thousands of nodes per process are comfortable on a small
instance; the AWS benchmark rig (`benchmarks/aws/`) runs 1,250 nodes
per `c7i.2xlarge` with wide headroom. For very large fleets prefer
several processes (one per instance) over one giant process — the
storm/churn scenarios also want process-granular kill targets.
