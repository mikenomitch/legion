# Legion

**A workload orchestrator that is simple to start, fast at scale, and built to be extended.**

> [!NOTE]
> Legion is an experimental personal project, built largely by AI agents.
> Expect bugs and breaking changes. See [Status](#status) before you use it.

Legion runs your containers, binaries, and microVMs across a fleet of machines.
You describe a job, and Legion decides where it runs, starts it, keeps it
healthy, and rolls out new versions. It does the same job as
[Kubernetes](https://kubernetes.io) or [Nomad](https://www.nomadproject.io),
with far fewer moving parts.

Legion ships as **one ~26 MB binary** that is the manager, the worker, the CLI,
and an MCP server for AI agents. State lives in SQLite by default, so a
single-node cluster needs no etcd, no Consul, and no Raft. Turn on
replication when you need high availability.

```
                ┌──────────────────────┐
   legion CLI ─►│       Manager        │  scheduler, state (SQLite or Raft),
   UI / MCP   ─►│   gRPC on :4747      │  ACLs, KV, service catalog
                └──────────┬───────────┘
            ┌──────────────┼──────────────┐
       ┌────▼───┐     ┌────▼───┐     ┌────▼───┐
       │ Worker │     │ Worker │     │ Worker │   exec · container · sandbox (microVM)
       └────────┘     └────────┘     └────────┘   macOS · Linux
```

## Quickstart

Install from [crates.io](https://crates.io/crates/legion-rs). You need
[Rust](https://rustup.rs) and `protoc` (`brew install protobuf` or
`apt-get install protobuf-compiler`).

```bash
cargo install legion-rs
```

The crate is named `legion-rs` because `legion` on crates.io is an unrelated
library. It installs a binary called `legion`.

To build from a clone instead, run `cargo build --release` and add
`target/release` to your `PATH`.

Start a manager with a worker running in the same process. The defaults work
without a config file.

```bash
legion start manager --run-worker
```

In another terminal, write a job. Job specs use [Nickel](https://nickel-lang.org),
a small typed configuration language. Plain JSON works too.

```nickel
# hello.ncl
{
  name = "hello",
  tasks = [
    {
      name = "web",
      runtime = "exec",
      lifecycle = "service",
      count = 2,
      config = {
        command = ["sh", "-c", "exec python3 -m http.server $LEGION_PORT_HTTP"],
      },
      network = { ports = [{ name = "http" }] },   # Legion picks a free port
      service = { port = "http" },                 # and registers it for discovery
      checks = [{ type = "health", kind = "tcp", port = "http", every = "5s", timeout = "2s" }],
      resources = { memory = 128 },
    },
  ],
}
```

Preview it, run it, and watch it come up:

```bash
legion job plan hello.ncl       # what would change, without changing anything
legion job run hello.ncl
legion job status hello
legion job logs hello
```

Find the running service and hit it:

```bash
legion service resolve web
curl "http://$(legion service resolve web --one)"
```

Change the spec and run it again to roll out a new version. Scale without a
new version using `legion job scale hello --count 5`.

### Add more machines

Adding a worker is two commands. Mint a join token on the manager, then paste
it on the new machine. The token carries the manager's address, so the worker
needs no other config.

```bash
# on the manager
legion join-token create --ttl 24h

# on the new machine
legion join lgnjoin_...
```

Every command takes `--json` for scripting. See
[Auth & ACLs](docs/auth-and-acls.md) to lock the cluster down before you expose
it to a network.

### Example job specs

A few from [`configs/jobs/`](configs/jobs/), roughly simplest first:

- [`min.ncl`](configs/jobs/min.ncl): the smallest valid job, one container.
- [`container-service.ncl`](configs/jobs/container-service.ncl): nginx in a
  container on a dynamic port, with an HTTP health check.
- [`exec-service.ncl`](configs/jobs/exec-service.ncl): a host process run
  directly, no image, restarted on exit.
- [`batch-job.ncl`](configs/jobs/batch-job.ncl): a run-to-completion task
  with a retry policy.
- [`per-node.ncl`](configs/jobs/per-node.ncl): one instance on every node,
  like a DaemonSet or a Nomad system job.
- [`service-demo.ncl`](configs/jobs/service-demo.ncl): a web service on a
  dynamic port, registered for discovery.
- [`nickel-showcase.ncl`](configs/jobs/nickel-showcase.ncl): functions, an
  import, contracts, and `--var` variables with defaults, all in one spec.
- [`dag-separate-groups.ncl`](configs/jobs/dag-separate-groups.ncl): batch
  prep tasks that fan out and back in before the services in other groups
  start.
- [`kv-reactive-demo.ncl`](configs/jobs/kv-reactive-demo.ncl): a task that
  reads a value from KV, encrypted at rest by default, and restarts when it
  changes.
- [`connect-demo.ncl`](configs/jobs/connect-demo.ncl): a client
  load-balancing across two backends through the service mesh.
- [`postgres.ncl`](configs/jobs/postgres.ncl): a stateful database with a
  persistent volume.
- [`fullstack.ncl`](configs/jobs/fullstack.ncl): an nginx frontend, a
  Python API, and Postgres, wired together through the service mesh, with
  each tier scaling on its own.
- [`sandbox-egress-demo.ncl`](configs/jobs/sandbox-egress-demo.ncl): an agent
  task that can reach only allowlisted upstreams.
- [`sandbox-edge-boot.ncl`](configs/jobs/sandbox-edge-boot.ncl): per-user
  microVM sandboxes that boot before their placement is durable.
- [`maximal.ncl`](configs/jobs/maximal.ncl): every field Legion accepts.

## Why Legion?

Legion aims to be the orchestrator you can start in a minute and
still be happy with at ten thousand nodes.

### Easy to use

- **One binary, one command per node.** `legion start manager` and
  `legion join <token>` are the whole install.
- **Raft is optional.** A single manager runs on local SQLite. Switch to
  [quorum-replicated state](docs/multi-raft.md) or
  [WAL streaming with lease-fenced failover](docs/persistence-streaming.md)
  when you need HA.
- **Batteries included.** [Service discovery](docs/service-discovery.md), a
  [service mesh with mTLS](docs/network-handlers.md), a
  [versioned KV store](docs/kv-store.md) with
  [encryption at rest](docs/kv-encryption.md),
  [rollouts](docs/rollouts.md), [ACLs](docs/auth-and-acls.md),
  [workload identity (OIDC)](docs/workload-identity.md), and a web UI.
- **Readable job specs.** [Nickel](docs/configuration-language.md) gives you
  types, imports, and functions for DRY specs, and
  [job templates](docs/job-templates.md) let teams submit vetted jobs with a
  few typed variables. No YAML sprawl, and no Helm templates.
- **See a change before you make it.** `legion job validate` and
  `legion job plan` check a spec against the same rules the manager applies
  and show exactly what would change. Errors come back with a hint for
  fixing them.
- **State you can query.** Cluster state is plain SQLite, so
  `legion state query "<SQL>"` answers questions the CLI never anticipated.
- **OpenTelemetry everywhere.** Managers and workers export
  [traces over OTLP](docs/observability.md) to any OpenTelemetry backend,
  covering RPCs, evals, scheduling, and plan apply.
  [Prometheus metrics](docs/metrics.md) and the
  [event stream](docs/event-stream.md) cover the rest, so you can see every
  state change as it happens.

### Fast

In initial benchmarking on AWS, against Nomad and Kubernetes on the same
three control-plane boxes, Legion placed several times as many allocations
per second and kept most of that rate out to 20,000 nodes:

| Nodes  | Legion  | Nomad 2.0.5    | Kubernetes 1.36.5 |
| ------ | ------- | -------------- | ----------------- |
| 1,000  | **829** | 122            | 117               |
| 5,000  | **905** | failed         | 53                |
| 20,000 | **553** | -              | -                 |

Allocations placed per second, `web_fleet` workload on simulated nodes.
Legion also finished a 250,000-allocation burst on 50,000 nodes. Below
saturation, a job's allocations land in about 20ms at the median, even on
20,000 nodes. When a quarter of a 5,000-node fleet died at once, Legion
replaced every orphaned allocation in 27 seconds, against 111 for Nomad.
See [Benchmarks](docs/benchmarks.md) for the full tables and methodology,
and [Why Legion is faster than Kubernetes](docs/benchmarks.md#why-legion-is-faster-than-kubernetes)
for the architecture behind the gap.

- **No etcd bottleneck.** The scheduler plans against in-memory state and
  writes to SQLite (or Raft), so throughput is not capped by a separate
  key-value store.
- **A scheduler built for large fleets.**
  [Scheduling indexes](docs/scheduling-indexes.md) keep candidate selection
  cheap as the fleet grows, and the scheduler picks the right index for each
  task on its own. [Late-binding plan apply](docs/late-binding-plan-apply.md)
  stops concurrent submits from fighting over the same node.
- **Heartbeats that adapt to the fleet.** With
  [adaptive cadence](docs/heartbeat-cadence-and-scale.md), on by default,
  the manager stretches each worker's check-in interval as the fleet grows,
  so a busy control plane doesn't mistake healthy nodes for dead ones. A
  circuit breaker refuses to mark half the fleet lost in one tick.
- **Start work before consensus.** [Edge boot](docs/edge-boot.md) lets a
  worker boot a workload before its placement is durable, or authorize the
  start on its own from a short-lived ticket. The durable commit still
  decides which instance wins.
- **Small enough for the edge.** The shipping binary is ~26 MB, against
  ~82 MB for k3s and ~150 MB for Nomad.
- **Measured, not guessed.** Micro-benchmarks gate scheduler changes, and a
  [worker simulator](docs/worker-simulation.md) drives thousands of
  wire-real fake nodes from one process. See
  [Testing and Benchmarks](docs/testing-and-benchmarks.md).

### Flexible

- **Plugins everywhere.** Task runtimes, scheduling logic, rollout strategies,
  artifact fetchers, volumes, node health checks, and key management are all
  [plugins](docs/plugin-system.md). Scheduling plugins read the same
  in-memory state as the built-in policies, so custom placement logic costs
  no extra round trips.
- **Any kind of workload.** Long-running services, batch jobs, sidecars, and
  [stateful workloads](docs/stateful-workloads.md) with stable identity,
  [volumes](docs/volumes.md), and fencing.
- **A built-in DAG system.** Tasks declare `dependsOn` to form a
  [dependency graph](docs/jobs-tasks-and-scheduling.md), gated on another
  task starting, completing, or succeeding. Dependencies can span groups
  placed on different nodes, so you don't need a separate workflow engine.
- **Any isolation level.** Run plain processes with [`exec`](docs/task-runtime-exec.md),
  OCI images with [`container`](docs/task-runtime-container.md), or untrusted
  code in microVMs with [`sandbox`](docs/task-runtime-sandbox.md). Need
  something else? A [custom runtime](docs/task-runtime-plugins.md) is a Rust
  crate that implements one trait, and the
  [`noop` example](plugins/tasks/noop/src/lib.rs) is under 100 lines.
- **Runs on macOS and Linux**, with Windows support in progress.

### Built to run agents

Agent sandboxes need to start fast, stay contained, and talk only to what
they should. Legion handles all three.

- **MicroVMs built in.** The [`sandbox`](docs/task-runtime-sandbox.md) runtime
  boots workloads in Firecracker or Cloud Hypervisor on Linux, and in Apple's
  `container` on macOS.
- **Boot at the edge, ahead of the scheduler.** [Edge boot](docs/edge-boot.md)
  lets a worker start a sandbox before its placement is durable. You can hold
  its network shut until the manager confirms it is the only instance.
- **Per-job egress control.** [Network handlers](docs/network-handlers.md)
  give each task its own egress allowlist and proxy middleware, so a sandbox
  reaches `pypi.org` and nothing else.

### DevEx for humans and agents

- **MCP server built in.** `legion mcp serve` exposes the cluster to any MCP
  client, read-only unless you pass `--allow-write`.
- **Structured output everywhere.** Every CLI command emits a typed JSON
  envelope with `--json`, with error codes, hints, and a retryable flag.
- **A self-documenting CLI.** `legion capabilities` lists every operation,
  and `legion job schema` emits the job spec as JSON Schema.
- **Safe to retry.** Mutating RPCs accept idempotency keys, and
  `legion job plan` previews a change before anything runs.

See [Agent Readiness](docs/agent-readiness.md) for the full picture.

## Status

Legion is a personal project made for fun and largely an experiment in
agent-driven development. Agents wrote most of the code. Most testing has been
done by agents, with human spot checking. Expect bugs. Don't run it in
production.

Don't expect stability either. APIs and the job spec may change without
notice.

The [docs](docs/README.md) state plainly what is and is not implemented for
each feature.

### What's next

Open work lives in the [to-do list](internal_docs/TODO.md), with design plans
in [`internal_docs/plans/`](internal_docs/plans/). A few things I'd like to
add:

- **Sandbox snapshots and fast start**, with forking support and
  snapshot-aware scheduling.
- **An artifact cache with pre-warming**, so images and binaries are on a node
  before the task that needs them.
- **Gang scheduling**, to place a group of tasks all at once or not at all.
- **Sharded scheduler**, so placement throughput scales past a single
  Raft cluster.
- **Built-in ingress gateway, autoscaler, cert-manager, and GitOps tools**, or
  a more omakase starting experience.
- **Something akin to Kubernetes controllers**, so you can extend Legion with
  your own reconcile loops.
- **Resource registry for sharing and pulling jobs, templates, and plugins.**
- **More runtimes**, such as WASM or remote serverless workloads.
- **Namespaces, multi-cluster federation, and Windows support.**

## Documentation

The full index lives at **[docs/README.md](docs/README.md)**. Good places to
start:

- **Concepts:** [Managers and Workers](docs/architecture-managers-workers.md) ·
  [Jobs, Tasks, and Scheduling](docs/jobs-tasks-and-scheduling.md) ·
  [Configuration Language](docs/configuration-language.md) ·
  [Scheduler and Eval Loop](docs/scheduler-and-eval-loop.md)
- **Running workloads:** [Task Runtimes](docs/task-runtime-plugins.md) ·
  [Artifacts](docs/artifacts.md) · [Rollouts](docs/rollouts.md) ·
  [Scaling Jobs](docs/autoscaling.md) ·
  [Task Supervision](docs/task-supervision.md) ·
  [Resource Enforcement](docs/resource-enforcement.md)
- **Guides:** [Running Postgres](docs/guides/postgres.md) ·
  [Running Redis](docs/guides/redis.md)
- **Platform services:** [Service Discovery](docs/service-discovery.md) ·
  [Network Handlers](docs/network-handlers.md) · [KV Store](docs/kv-store.md) ·
  [Exec & Actions](docs/exec-and-actions.md)
- **Operating a cluster:** [Running Raft](docs/raft-operations.md) ·
  [Upgrades](docs/upgrades.md) · [Data Directory](docs/data-directory.md) ·
  [Logging](docs/logging.md) ·
  [Metrics](docs/metrics.md) · [Observability](docs/observability.md)
- **Extending Legion:** [Plugin System](docs/plugin-system.md) ·
  [Codebase Conventions](docs/codebase-conventions.md) ·
  [Testing and Benchmarks](docs/testing-and-benchmarks.md) ·
  [Benchmarks](docs/benchmarks.md)

More example job specs live in [`configs/jobs/`](configs/jobs/).

## Contributing

Read [AGENTS.md](AGENTS.md) for build commands, style, and the rules every
change follows. Plans, roadmaps, and development notes live in
[`internal_docs/`](internal_docs/README.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Legion by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
