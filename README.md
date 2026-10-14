# Legion

**A workload orchestrator that is simple, performant, scalable, and exetensible.**

> [!NOTE]
> Legion is an experimental personal project, built largely by AI agents.
> Expect bugs, breaking changes, and known security issues. See
> [Status](#status) before you use it.

Legion runs your containers, microVM sandboxes, and binaries across a fleet of machines.
You describe a job, and Legion decides where it runs, starts it, keeps it
healthy, rolls out new versions, and routes to it. It does the same job as
[Kubernetes](https://kubernetes.io) or [Nomad](https://www.nomadproject.io),
with far fewer moving parts.

Legion ships as **one ~26 MB binary** that is the manager, the worker, the CLI,
and an MCP server. State lives in SQLite by default. It can run on a single machine,
and when you need HA you can either stream state to object storage or persist
with Raft.

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

## Why Legion?

Legion aims to be the orchestrator you can start in a minute and
still be happy with at fifty thousand nodes.

### Easy to use

- **One binary, one command per node.** `legion start manager` and
  `legion join <token>` are the whole install.
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
- **Raft is optional.** A single manager runs on local SQLite (that is easy
  to query). When you need HA, you can [stream to object storage](docs/persistence-streaming.md)
  or use [Raft](docs/multi-raft.md) depending on which suits your needs.
- **Easy local dev.** Works out of the box on Linux or on a Mac (using `container`).

### Fast

Legion is considerably faster than other schedulers.

In one test, on the same AWS hardware and disks at 5,000 nodes, Legion
placed work about 23X faster than K8s and 24X faster than Nomad (see our
[benchmarks](docs/benchmarks.md) for details).

How it gets there:

- **Using in-memory state instead of etcd.** The scheduler plans against in-memory
  state and writes to SQLite, persisting with built-in raft mechanisms,
  so scheduling decisions and persistence are faster.
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

See [Testing and Benchmarks](docs/testing-and-benchmarks.md) for more details.

For sandbox workloads (see below), Legion can scale even faster by booting work at the edge
and temporarily trading consistency for speed.

### Flexible

- **Plugins everywhere.** Task runtimes, scheduling logic, rollout strategies,
  artifact fetchers, volumes, node health checks, and key management are all
  [plugins](docs/plugin-system.md). Scheduling plugins read the same
  in-memory state as the built-in policies, so custom placement logic costs
  no extra round trips.
- **Any isolation level.** Run plain processes with [`exec`](docs/task-runtime-exec.md),
  OCI images with [`container`](docs/task-runtime-container.md), or untrusted
  code in microVMs with [`sandbox`](docs/task-runtime-sandbox.md). Need
  something else? A [custom runtime](docs/task-runtime-plugins.md) is a Rust
  crate that implements one trait, and the
  [`noop` example](plugins/tasks/noop/src/lib.rs) is under 100 lines.
- **Any kind of workload.** Long-running services, batch jobs, sidecars, and
  [stateful workloads](docs/stateful-workloads.md) with stable identity,
  [volumes](docs/volumes.md), fencing, and
  [suspend and resume](docs/suspend-and-resume.md) (by hand, when idle, or
  under memory pressure) with snapshots you can roll an instance back to.
- **A built-in DAG system.** Tasks declare `dependsOn` to form a
  [dependency graph](docs/jobs-tasks-and-scheduling.md), gated on another
  task starting, completing, or succeeding. Dependencies can span groups
  placed on different nodes, so you don't need a separate workflow engine.
- **Runs on macOS and Linux**, with Windows support in progress.
- **Small enough for the edge.** The shipping binary is ~26 MB, against
  ~82 MB for k3s and ~150 MB for Nomad.

### Built to run agents

Agent sandboxes need to start fast, stay contained, and talk only to what
they should. Legion handles all three.

- **MicroVMs built in.** The [`sandbox`](docs/task-runtime-sandbox.md) runtime
  boots workloads in Firecracker or Cloud Hypervisor on Linux, and in Apple's
  `container` on macOS.
- **Start work before consensus.** [Edge boot](docs/edge-boot.md) lets a
  worker boot a sandbox before its placement is durable. This allows work
  to start ASAP, but then still be durably committed for future self-healing
  and consistency.
- **A shell in any sandbox, with or without the manager in the path.**
  `legion exec` reaches into Linux microVM sandboxes through the guest
  agent. For high session counts, a platform mints single-use
  [exec tickets](docs/exec-and-actions.md#direct-exec) (or signs its own
  with a [delegated key](docs/exec-and-actions.md#delegated-minting-keys)),
  and the session runs client to worker with no manager on the data path.
- **Simple egress control.** [Network handlers](docs/network-handlers.md)
  give each task its own egress allowlist and proxy middleware, so you
  can customize what traffic is allowed, and
  [inject API keys](docs/network-handlers.md#credential-injection) on the
  way out so the sandbox never holds them.
- **Agent sandboxes that park when idle.** A sandbox
  [suspends](docs/suspend-and-resume.md) to a memory checkpoint after a few
  quiet minutes, frees its reservation (or keeps a share, so the resume
  finds room), and resumes in place with its processes intact, by hand or
  when the next request for it reaches the [gateway](docs/gateway.md). `memoryMax` and an
  [oversubscription ratio](docs/resource-enforcement.md#oversubscription)
  pack the active ones tighter, and fork copies a running sandbox for an
  agent to explore from. See the
  [agent sandboxes guide](docs/guides/agent-sandboxes.md).
- **Sandboxes that scale.** A keyed sandbox is a durable instance: it
  survives node loss, suspends and resumes, forks, and its id is never
  reused. A full lifecycle (create, run, suspend, resume, destroy) costs the
  control plane 8 replicated writes, down from 22. Concurrent creates and
  suspends share a single write, so the cost per sandbox falls under load.
  See [Sandboxes at scale](docs/sandboxes-at-scale.md).
- **A control plane the size of what's live, not of history.** Every
  manager keeps the whole replicated store, so its size is what a new
  manager has to copy and what every snapshot carries. A sandbox whose id
  Legion minted leaves nothing behind when it is destroyed: its id is 130
  random bits, so it is never handed out again without a tombstone to
  enforce it. An idle sandbox can
  [archive](docs/suspend-and-resume.md#archiving-idle-instances) its
  checkpoint record to the object store beside its memory snapshot, so a
  million parked sandboxes cost a small row each, or nothing at all.
- **Sessions that write nothing.** Durable records have a price: every
  keyed start goes through the replicated log, which caps the start rate
  (about 50 lifecycles a second on a 4-core manager). For sandboxes that
  are disposable, [session mode](docs/session-mode.md) takes the log off
  the start path. The job reserves room on each node up front, any manager
  sends a start straight to a node with room, and the nodes report what
  they run, so a start or stop is zero replicated writes. The same 4-core
  manager starts about 970 sessions a second, and start capacity grows with
  the nodes rather than with the log. A session that turns out to matter
  is promoted to a durable instance in one write, without restarting.
- **Work lands in one round trip.** Each node holds one stream to its
  manager, so a placement reaches it as soon as it commits, at any
  heartbeat interval. A node running 10,000 sandboxes sends a heartbeat of a
  few dozen bytes, where it used to exchange about 4.6 MB per beat.
- **Full clusters don't fail creates.** A sandbox that finds no room waits,
  and places itself the moment a node that can host it joins or frees
  capacity. `legion job info` shows what it's waiting for.

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

## Quickstart

Install from [crates.io](https://crates.io/crates/legion-rs). You need
[Rust](https://rustup.rs) and `protoc` (`brew install protobuf` or
`apt-get install protobuf-compiler`).

```bash
cargo install legion-rs
```

Start a manager with a worker running in the same process. The defaults work
without a config file, from any directory.

```bash
legion start manager --run-worker
```

ACLs are on from the first start. The manager mints a management token and
writes it to `~/.legion/tokens/127.0.0.1_4747` (and `.legion/bootstrap-token`),
which the CLI on this machine reads, so there is nothing to set up. From another machine,
export the same secret as `LEGION_TOKEN` (see [Auth & ACLs](docs/auth-and-acls.md)).

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
legion job info hello           # status: pending, then running
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

The token pins your cluster's CA, so the new worker refuses anything that
answers at the manager's address but isn't your manager. Managers reach
workers over mutual TLS on port 4848 for `legion exec`, `legion job logs`,
and dispatch, so allow TCP 4848 from the managers to the workers (and 4747
from the workers to the managers). The worker advertises its address on the
route to the manager; if managers must dial it somewhere else (NAT, a second
interface), pass `--advertise-addr 10.0.1.7` or set
`LEGION_WORKER_ADVERTISE_ADDR` (see
[Managers and Workers](docs/architecture-managers-workers.md#ports-and-who-dials-whom)).

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

- **`exec` tasks run on the host, and as root unless they set a `user`.**
  They share the host's network. On Linux nodes with cgroup v2 and `nft`,
  the [kernel host guard](docs/network-handlers.md#the-host-guard-in-the-kernel)
  keeps them off Legion's ports, the cloud metadata service, and other
  allocations' listeners, but a root task can remove it. Run untrusted code
  in `container` (gated by default on Linux) or `sandbox`, never `exec`.
- **Raft traffic between managers is plaintext.** Workers reach managers over
  TLS pinned to the cluster CA, and managers reach workers over mutual TLS,
  but peer managers talk to each other in the clear. Keep managers on a
  network you trust.
- **Revoking access ends a direct exec session within a heartbeat, not at
  once.** Deleting the token behind a [direct exec](docs/exec-and-actions.md#direct-exec)
  session, or the delegated key that signed its ticket, closes the session
  on the worker's next heartbeat: 2 seconds up to 2,500 nodes, longer on
  larger fleets, where the heartbeat interval grows.

### What's next

Open work lives in the [to-do list](internal_docs/TODO.md), with design plans
in [`internal_docs/plans/`](internal_docs/plans/). A few things I'd like to
add:

- **Faster sandbox wake-ups**: wake on connect, lazy memory restore, and diff
  snapshots on top of [suspend and resume](docs/suspend-and-resume.md).
- **Sandbox scaling to very large session counts**: idle sessions in
  object storage that wake on request, workload identity for sessions, and
  denser sandboxes per node
  ([plan](internal_docs/plans/sandbox-scaling-plan.md)).
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

- **Installing:** [Legion and its runtimes](docs/installation.md) (runc,
  cloud-hypervisor, a guest kernel, Apple `container`)
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
  [Suspend and Resume](docs/suspend-and-resume.md) ·
  [Gateway: Wake on Request](docs/gateway.md)
- **Guides:** [Running Postgres](docs/guides/postgres.md) ·
  [Running Redis](docs/guides/redis.md) ·
  [Serving TLS Certificates from KV](docs/guides/tls-certificates.md) ·
  [Running Agent Sandboxes](docs/guides/agent-sandboxes.md) ·
  [Sandboxes at Scale](docs/sandboxes-at-scale.md) ·
  [Session Mode](docs/session-mode.md)
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
