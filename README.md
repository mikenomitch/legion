# Legion

A distributed scheduler similar to Kubernetes or Nomad.

## Documentation

Full index: [docs/README.md](docs/README.md).

- [Managers and Workers](docs/architecture-managers-workers.md)
- [Plugin System](docs/plugin-system.md)
- [Scheduler and Eval Loop](docs/scheduler-and-eval-loop.md)
- [Late-Binding Plan Apply](docs/late-binding-plan-apply.md)
- [Event Stream](docs/event-stream.md)
- [Jobs, Tasks, and Scheduling](docs/jobs-tasks-and-scheduling.md)
- [Job Templates (reusable specs, typed variables, delegated submission)](docs/job-templates.md)
- [Task Runtime Plugins](docs/task-runtime-plugins.md)
- [Task Supervision (exits, restarts, resource limits, hardening)](docs/task-supervision.md)
- [Task Runtime: exec](docs/task-runtime-exec.md)
- [Task Runtime: container](docs/task-runtime-container.md)
- [Task Runtime: sandbox (microVM)](docs/task-runtime-sandbox.md)
- [Task Runtime: noop](docs/task-runtime-noop.md)
- [Resource Enforcement (hard memory limits, exclusive cores, CPU weight)](docs/resource-enforcement.md)
- [Artifacts (images, files, git, S3, OCI/ORAS)](docs/artifacts.md)
- [Stateful Workloads & Instance Identity](docs/stateful-workloads.md)
- [Rollouts](docs/rollouts.md) and [Rollout Strategy Plugins](docs/rollout-strategy-plugins.md)
- [Scaling Jobs](docs/autoscaling.md)
- [Graceful Stop](docs/graceful-stop.md)
- [Preemption and Eviction](docs/preemption-and-eviction.md)
- [Service Discovery](docs/service-discovery.md)
- [State Store](docs/state-store.md)
- [Streaming Persistence](docs/persistence-streaming.md) and [Running Raft](docs/raft-operations.md)
- [Logging (task logs, tailing, rotation, shipping, OTLP traces)](docs/logging.md)
- [Observability](docs/observability.md)
- [UI Architecture](docs/ui-architecture.md)
- [KV Store](docs/kv-store.md)
- [Auth & ACLs](docs/auth-and-acls.md)
- [Exec & Actions (run commands in a workload, interactive PTY, actions)](docs/exec-and-actions.md)
- [Workload Identity (OIDC)](docs/workload-identity.md)
- [Testing and Benchmarks](docs/testing-and-benchmarks.md)
- [Worker Simulation (fake node fleets for load testing)](docs/worker-simulation.md)
- [Codebase Conventions](docs/codebase-conventions.md)
- [Agent Readiness (MCP, structured output, agent workflow)](docs/agent-readiness.md)

Plans, roadmaps, and development notes live in
[internal_docs/](internal_docs/README.md).

It aims to be differentiated from the above based on the following:
- Ease:
  - It is the simplest scheduler to get running. Start a manager node in one command. Connect from a worker node in one command.
  - Job definitions are simple.
  - UI out of the box.
  - Built-in service-discovery, secrets management, autoscaling, GitOps support, cert management
  - First-party registry of workloads and plugins. Auto-download any plugins necessary for a workload.
- Performance:
  - It is faster and more scalable than Nomad or K8s.
  - It is a small binary that can run on edge devices.
- Flexiblity:
  - It has a plugin system for:
    - Workload runtime (containers, WASM, binaries, VMs, etc)
    - Scheduler logic (optimize for utilization, resilience, speed)
    - Networking
    - Volumes
    - And more: storage, the UI, CLI commands, job submit hooks, worker-node health checking, logging, etc
  - It runs on MacOS, Linux, and Windows
  - It can run long-lived or short-lived jobs, with or without state
- Other Benefits:
  - OTEL compliant tracing, logs, and metrics
  - Well-documented GRPC interface
  - Jsonnet configuration language for simple and DRY job specifications
  - Raft is optional. Making life simpler — plain local SQLite by default, real quorum-replicated manager state (`persistence.type: raft`) when you need HA ([docs/multi-raft.md](docs/multi-raft.md))
  - Can easily be used as a library
