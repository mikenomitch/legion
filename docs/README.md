# Legion Documentation

Public documentation for Legion — a distributed scheduler similar to Kubernetes
or Nomad. These pages are written for people using, operating, or evaluating
Legion, and for contributors reading the code.

Plans, roadmaps, to-do lists, and development notes live in
[`internal_docs/`](../internal_docs/README.md) instead.

## Architecture

- [Configuration Language](configuration-language.md) — Nickel: the syntax
  essentials, the `vars` binding behind `--var`, imports, where imports are
  refused, and the `.json` path for machine-emitted config.
- [Managers and Workers](architecture-managers-workers.md) — cluster shape,
  responsibilities, and the manager/worker protocol.
- [Scheduler and Eval Loop](scheduler-and-eval-loop.md) — the control loop that
  drives placement.
- [Scheduling Indexes](scheduling-indexes.md) — declarable nearest-fit indexes
  over node attributes and free resources; how candidate selection scales.
- [Late-Binding Plan Apply](late-binding-plan-apply.md) — ranked candidate
  lists with commit-time node choice, and the conflict-requeue backstop; how
  concurrent submits stop fighting over one node.
- [Jobs, Tasks, and Scheduling](jobs-tasks-and-scheduling.md) — job spec shapes,
  placement semantics, and edge cases.
- [State Store](state-store.md) — manager and worker persistence.
- [Node Liveness](node-liveness.md) — where a node's "is it there?" answer
  lives, what an election does to it, and why a heartbeat costs no commit.
- [Heartbeat Cadence and Scale](heartbeat-cadence-and-scale.md) — sizing a
  control plane for a fleet, the per-node lost window, and the freshness
  ratio that says whether the fleet is keeping up.
- [UI Architecture](ui-architecture.md) — the SolidJS app and its design system.

## Workloads

- [Task Supervision](task-supervision.md) — process exits and the `restart`
  policy, real batch completion, worker-owned cgroup limits, and `exec`
  hardening.
- [Task Runtime Plugins](task-runtime-plugins.md) — the task-runtime plugin contract.
- [Task Runtime: exec](task-runtime-exec.md)
- [Task Runtime: container](task-runtime-container.md)
- [Task Runtime: sandbox](task-runtime-sandbox.md) — OCI images in microVMs for
  untrusted code and agent workspaces.
- [Task Runtime: noop](task-runtime-noop.md)
- [Resource Enforcement](resource-enforcement.md) — making declared `memory`,
  `cores`, and `cpu` real on Linux: hard memory limits, exclusive pinned
  cores, proportional CPU weight, and the interface runtimes adopt.
- [Job Templates](job-templates.md) — reusable, versioned job specs with typed
  variables, guided submission, and template-scoped delegation.
- [Artifacts](artifacts.md) — declaring, fetching, verifying, and staging inputs.
- [Stateful Workloads & Instance Identity](stateful-workloads.md) — slot and
  keyed instances, storage handles, fencing.
- [Volumes](volumes.md) — persistent storage for stateful tasks: plugins, the
  driver contract, node pinning, and reclaim.
- [Rollouts](rollouts.md) — update strategies, health gating, manual/auto
  progression.
- [Rollout Strategy Plugins](rollout-strategy-plugins.md)
- [Scaling Jobs](autoscaling.md) — `legion job scale`: count overrides
  without a new version or rollout; the manual half of the autoscaler.
- [Graceful Stop](graceful-stop.md) — the signal/grace/SIGKILL ladder every
  platform-initiated stop uses, the machine-readable stop notice, and why an
  allocation was removed.
- [Preemption and Eviction](preemption-and-eviction.md) — priority bands, when
  the scheduler displaces lower-band work, and how to preview or prevent it.
- [Edge Boot](edge-boot.md) — starting work before the placement is durable:
  gate levels, boot tickets, ratification, and the kill path.

## Platform Services

- [Service Discovery](service-discovery.md) — declaring services and finding
  healthy endpoints.
- [KV Store](kv-store.md) — CRUD, versioning, locks, and reactive task updates.
- [KV Encryption at Rest](kv-encryption.md) — on by default: the root key
  outside the store, wrapped data keys inside it, rotation, backups, and
  what to do when a key is missing.
- [Auth & ACLs](auth-and-acls.md) — tokens, policies, scopes, and enforcement.
- [Workload Identity (OIDC)](workload-identity.md) — short-lived, verifiable task
  identity for external providers.
- [Exec & Actions](exec-and-actions.md) — running commands inside a workload,
  interactive PTY, curated actions.

## Operations

- [Logging](logging.md) — task log capture, tailing, rotation, shipping, OTLP.
- [Observability](observability.md) — current tracing and metrics state.
- [Diagnosing Scheduler Performance](diagnosing-scheduler-performance.md) —
  symptom-first: throughput fell when you scaled up, the box is idle but
  capped, new work waits behind a backlog. What to read, what its
  neighbouring metric rules out, and what to change.
- [Event Stream](event-stream.md) — scheduler events as history and live tail.
- [Streaming Persistence](persistence-streaming.md) — WAL shipping and
  lease-fenced failover.
- [Running Raft](raft-operations.md) — leader election, failover, membership.

## Extending & Contributing

- [Plugin System](plugin-system.md) — how in-process `cdylib` plugins load, and
  the trade-offs.
- [Codebase Conventions](codebase-conventions.md)
- [Testing and Benchmarks](testing-and-benchmarks.md)
- [Worker Simulation](worker-simulation.md) — `--simulate N`: thousands of
  wire-real fake nodes from one process, for benchmarking and load testing.
- [Agent Readiness](agent-readiness.md) — MCP, structured output, capabilities,
  and the recommended agent workflow.
