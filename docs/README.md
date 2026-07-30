# Legion Documentation

Public documentation for Legion — a distributed scheduler similar to Kubernetes
or Nomad. These pages are written for people using, operating, or evaluating
Legion, and for contributors reading the code.

Plans, roadmaps, to-do lists, and development notes live in
[`internal_docs/`](../internal_docs/README.md) instead.

## Architecture

- [Managers and Workers](architecture-managers-workers.md) — cluster shape,
  responsibilities, and the manager/worker protocol.
- [Scheduler and Eval Loop](scheduler-and-eval-loop.md) — the control loop that
  drives placement.
- [Scheduling Indexes](scheduling-indexes.md) — declarable nearest-fit indexes
  over node attributes and free resources; how candidate selection scales.
- [Jobs, Tasks, and Scheduling](jobs-tasks-and-scheduling.md) — job spec shapes,
  placement semantics, and edge cases.
- [State Store](state-store.md) — manager and worker persistence.
- [UI Architecture](ui-architecture.md) — the SolidJS app and its design system.

## Workloads

- [Task Runtime Plugins](task-runtime-plugins.md) — the task-runtime plugin contract.
- [Task Runtime: exec](task-runtime-exec.md)
- [Task Runtime: container](task-runtime-container.md)
- [Task Runtime: sandbox](task-runtime-sandbox.md) — OCI images in microVMs for
  untrusted code and agent workspaces.
- [Task Runtime: noop](task-runtime-noop.md)
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

## Platform Services

- [Service Discovery](service-discovery.md) — declaring services and finding
  healthy endpoints.
- [KV Store](kv-store.md) — CRUD, versioning, locks, and reactive task updates.
- [Auth & ACLs](auth-and-acls.md) — tokens, policies, scopes, and enforcement.
- [Workload Identity (OIDC)](workload-identity.md) — short-lived, verifiable task
  identity for external providers.
- [Exec & Actions](exec-and-actions.md) — running commands inside a workload,
  interactive PTY, curated actions.

## Operations

- [Logging](logging.md) — task log capture, tailing, rotation, shipping, OTLP.
- [Observability](observability.md) — current tracing and metrics state.
- [Event Stream](event-stream.md) — scheduler events as history and live tail.
- [Streaming Persistence](persistence-streaming.md) — WAL shipping and
  lease-fenced failover.
- [Running Raft](raft-operations.md) — leader election, failover, membership.

## Extending & Contributing

- [Plugin System](plugin-system.md) — how in-process `cdylib` plugins load, and
  the trade-offs.
- [Codebase Conventions](codebase-conventions.md)
- [Testing and Benchmarks](testing-and-benchmarks.md)
- [Agent Readiness](agent-readiness.md) — MCP, structured output, capabilities,
  and the recommended agent workflow.
