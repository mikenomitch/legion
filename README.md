# Legion

**A workload orchestrator that is simple, performant, scalable, and exetensible.**

> [!NOTE]
> Legion is an experimental personal project, built largely by AI agents.
> Expect bugs, breaking changes, and known security issues. See
> [Status](#status) before you use it.

Legion runs your containers, binaries, and microVM sandboxes across a fleet of machines.
You describe a job, and Legion decides where it runs, starts it, keeps it
healthy, and rolls out new versions. It does the same job as
[Kubernetes](https://kubernetes.io) or [Nomad](https://www.nomadproject.io),
with far fewer moving parts.

Legion ships as **one ~26 MB binary** that is the manager, the worker, the CLI,
and an MCP server for AI agents. State lives in SQLite by default, and can be
run on a single machine, streamed to object-storage, or persisted using raft
depending on your needs.

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

Start a manager with a worker running in the same process. The defaults work
without a config file.

```bash
legion start manager --run-worker
```

In another terminal, write a job. Job specs use [Nickel](https://nickel-lang.org),
a small typed configuration language, but plain JSON works too.

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

Change the spec and run it again to roll out a new version. Scale using
`legion job scale hello --count 5`.

### Add more machines

Adding a worker is two commands. Mint a join token on the manager, then paste
it on the new machine.

```bash
# on the manager, mint a join token
legion join-token create --ttl 24h

# on the new machine, use the returned token
legion join lgnjoin_...
```

### Example job specs

A few from [`configs/jobs/`](configs/jobs/), roughly simplest first:

- [`min.ncl`](configs/jobs/min.ncl): a small valid job with one container.
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
- [`cert-renewer.ncl`](configs/jobs/cert-renewer.ncl) and
  [`tls-edge.ncl`](configs/jobs/tls-edge.ncl): a scheduled job that renews a
  Let's Encrypt certificate into KV, and services that serve it and swap it
  live on renewal, with no restarts.
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
- **Raft is optional.** A single manager runs on local SQLite. When you
  need HA, you can [stream to object storage](docs/persistence-streaming.md)
  or use [Raft](docs/multi-raft.md) depending on which suits your needs.
- **Batteries included.** [Service discovery](docs/service-discovery.md), a
  [service mesh with mTLS](docs/network-handlers.md), a
  [versioned KV store](docs/kv-store.md) with
  [encryption at rest](docs/kv-encryption.md),
  [rollouts](docs/rollouts.md), [ACLs](docs/auth-and-acls.md),
  [workload identity (OIDC)](docs/workload-identity.md), and a web UI.
- **Readable job specs.** [Nickel](docs/configuration-language.md) gives you
  types, imports, and functions for DRY specs, and
  [job templates](docs/job-templates.md) let teams submit vetted jobs with a
  few typed variables. This means no YAML or Helm charts.
- **State you can query.** Cluster state is plain SQLite, so
  `legion state query "<SQL>"` answers questions the CLI never anticipated.

### Fast

Legion is fast. For instance, on the same AWS hardware at 5,000 nodes, Legion outpaced Kubernetes
by a wide margin ([benchmarks](docs/benchmarks.md)):

- **17x** the services placed per second
- **28x** the batch allocations placed per second
- **42x** the densely packed services placed per second
- **Much faster recovery** when a large share of nodes die at once

Services placed per second as the fleet grows:

| Nodes  | Legion  | Nomad 2.0.5 | Kubernetes 1.36.5 |
| ------ | ------- | ----------- | ----------------- |
| 1,000  | **829** | 122         | 117               |
| 2,000  | **789** | 110         | 78                |
| 5,000  | **905** | -           | 53                |
| 10,000 | **738** | -           | -                 |

How it gets there:

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
  worker boot a workload before its placement is durable. This allows work
  to start ASAP, but then still be durably committed for future self-healing
  and consistency.

See [Testing and Benchmarks](docs/testing-and-benchmarks.md) for more details.

### Flexible

- **Plugins everywhere.** Task runtimes, scheduling logic, rollout strategies,
  artifact fetchers, volumes, node health checks, and key management are all
  [plugins](docs/plugin-system.md). Scheduling plugins read the same
  in-memory state as the built-in policies, so custom placement logic costs
  no extra round trips.
- **Any kind of workload.** Long-running services, batch jobs, sidecars, and
  [stateful workloads](docs/stateful-workloads.md) with stable identity,
  [volumes](docs/volumes.md), fencing, and
  [suspend and resume](docs/suspend-and-resume.md) (by hand, when idle, or
  under memory pressure) with snapshots you can roll an instance back to.
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
- **Small enough for the edge.** The shipping binary is ~26 MB, against
  ~82 MB for k3s and ~150 MB for Nomad.

### Built to run agents

Agent sandboxes need to start fast, stay contained, and talk only to what
they should. Legion handles all three.

- **MicroVMs built in.** The [`sandbox`](docs/task-runtime-sandbox.md) runtime
  boots workloads in Firecracker or Cloud Hypervisor on Linux, and in Apple's
  `container` on macOS.
- **Boot at the edge, ahead of the scheduler.** [Edge boot](docs/edge-boot.md)
  lets a worker start a sandbox before its placement is durable. You can hold
  its network shut until the manager confirms it is the only instance.
- **Simple egress control.** [Network handlers](docs/network-handlers.md)
  give each task its own egress allowlist and proxy middleware, so you
  can customize what traffic is allowed or inject secrets safely.
- **Agent sandboxes that park when idle.** A sandbox
  [suspends](docs/suspend-and-resume.md) to a memory checkpoint after a few
  quiet minutes, frees its reservation (or keeps a share, so the resume
  finds room), and resumes in place with its processes intact. `memoryMax` and an
  [oversubscription ratio](docs/resource-enforcement.md#oversubscription)
  pack the active ones tighter, and fork copies a running sandbox for an
  agent to explore from. See the
  [agent sandboxes guide](docs/guides/agent-sandboxes.md).

### DevEx for humans and agents

- **MCP server built in.** `legion mcp serve` exposes the cluster to any MCP
  client, read-only unless you pass `--allow-write`.
- **Structured output everywhere.** Every CLI command emits a typed JSON
  envelope with `--json`, with error codes, hints, and a retryable flag.
- **A self-documenting CLI.** `legion capabilities` lists every operation,
  and `legion job schema` emits the job spec as JSON Schema.
- **Safe to retry.** Mutating RPCs accept idempotency keys, and
  `legion job plan` previews a change before anything runs.
- **OpenTelemetry everywhere.** Managers and workers export
  [traces over OTLP](docs/observability.md) to any OpenTelemetry backend,
  covering RPCs, evals, scheduling, and plan apply.
  [Prometheus metrics](docs/metrics.md) and the
  [event stream](docs/event-stream.md) cover the rest, so you can see every
  state change as it happens.


See [Agent Readiness](docs/agent-readiness.md) for the full picture.

## Status

Legion is a personal project made for fun is largely an experiment with
agent-driven development. Most testing has been done by agents, with human
spot checking. Expect bugs. Don't run it in production.

Don't expect stability either. APIs and the job spec may change without
notice.

Traditionally, something like this should have been built carefully, 
layer-by-layer, where you have a ton of confidence in architectural decisions
before adding another layer. With recent LLM progress, I've taken a different
approach and just "gone for it" in terms of merging things and building on them,
with the expectation that I can rip out and adjust foundation after the fact.
This is part of the experimentation with new workflows in the agent era.

The core scheduler has gotten a decent amount of testing, bug fixes, and optimization,
but the networking stack/mesh, various runtimes, and sandboxing primitives have not.

The [docs](docs/README.md) should state what is and is not implemented for
each feature.

### Known security issues

Don't run Legion where untrusted workloads share a host with other work, or
on a network you don't trust. Known gaps:

- **The worker API authenticates no one.** It listens only on loopback, but
  any process on the same host, including an `exec` task or a host-network
  container, can run commands in other allocations on that node. The
  [worker-plane auth plan](internal_docs/plans/worker-plane-auth-plan.md)
  fixes this.
- **Linux containers and `exec` tasks share the host's network by default**,
  so they can reach the worker and manager ports, the cloud metadata service,
  and anything else on the host. With ACLs off (the default), the manager's
  port is full control of the cluster. `gated` mode doesn't help yet, because
  its egress proxy still dials loopback and link-local addresses for the
  task. Don't run untrusted code in them yet. The
  [same plan](internal_docs/plans/worker-plane-auth-plan.md) covers it.
- **The worker-to-manager channel is plaintext.**
- **Workers on other hosts can't serve exec, logs, or the service mesh yet**,
  because they advertise a loopback address. Covered by the same plan.

### What's next

Open work lives in the [to-do list](internal_docs/TODO.md), with design plans
in [`internal_docs/plans/`](internal_docs/plans/). A few things I'd like to
add:

- **Faster sandbox wake-ups**: wake on connect, lazy memory restore, and diff
  snapshots on top of [suspend and resume](docs/suspend-and-resume.md).
- **Sandbox scaling to very large session counts**: ephemeral sessions,
  node-reported state, idle sessions in object storage that wake on
  request, and sequenced delivery of work, so raft holds only what must be
  durable ([plan](internal_docs/plans/sandbox-scaling-plan.md)).
- **Exec into microVM sandboxes**, and exec sessions that skip the manager
  ([plan](internal_docs/plans/sandbox-exec-plan.md)).
- **Built-in ingress gateway, autoscaler, cert-manager, and GitOps tools**, or
  a more omakase starting experience.
- **Resource registry for sharing and pulling jobs, templates, and plugins.**
- **Sharded scheduler**, so placement throughput scales past a single
  Raft cluster.
- **Submission middleware**, so you can modify or gate job submissions.
- **Something akin to Kubernetes controllers**, so you can extend Legion with
  your own reconcile loops.
- **Gang scheduling**, to place a group of tasks all at once or not at all.
- **More runtimes**, such as WASM or remote serverless workloads.
- **Namespaces, multi-cluster federation, and Windows support.**

## Documentation

The full index lives at **[docs/README.md](docs/README.md)**. Good places to
start:

- **Concepts:** [Managers and Workers](docs/architecture-managers-workers.md) ·
  [Jobs, Tasks, and Scheduling](docs/jobs-tasks-and-scheduling.md) ·
  [Configuration Language](docs/configuration-language.md) ·
  [Scheduler and Eval Loop](docs/scheduler-and-eval-loop.md) ·
  [Adaptive Scheduler Concurrency](docs/adaptive-scheduler-concurrency.md)
- **Running workloads:** [Task Runtimes](docs/task-runtime-plugins.md) ·
  [Artifacts](docs/artifacts.md) · [Rollouts](docs/rollouts.md) ·
  [Scaling Jobs](docs/autoscaling.md) ·
  [Task Supervision](docs/task-supervision.md) ·
  [Resource Enforcement](docs/resource-enforcement.md) ·
  [Suspend and Resume](docs/suspend-and-resume.md)
- **Guides:** [Running Postgres](docs/guides/postgres.md) ·
  [Running Redis](docs/guides/redis.md) ·
  [Serving TLS Certificates from KV](docs/guides/tls-certificates.md) ·
  [Running Agent Sandboxes](docs/guides/agent-sandboxes.md)
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
