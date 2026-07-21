# Legion

A distributed scheduler similar to Kubernetes or Nomad.

## Documentation

- [Managers and Workers](docs/architecture-managers-workers.md)
- [Plugin System](docs/plugin-system.md)
- [Scheduler and Eval Loop](docs/scheduler-and-eval-loop.md)
- [Jobs, Tasks, and Scheduling](docs/jobs-tasks-and-scheduling.md)
- [Task Kind Plugins](docs/task-kind-plugins.md)
- [Task Kind: exec](docs/task-kind-exec.md)
- [Task Kind: container](docs/task-kind-container.md)
- [Task Kind: noop](docs/task-kind-noop.md)
- [Service Discovery](docs/service-discovery.md)
- [State Store](docs/state-store.md)
- [Observability](docs/observability.md)
- [UI Architecture](docs/ui-architecture.md)
- [KV Store](docs/kv-store.md)
- [Testing and Benchmarks](docs/testing-and-benchmarks.md)
- [Codebase Conventions](docs/codebase-conventions.md)

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
    - Workload kind (containers, WASM, binaries, VMs, etc)
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
  - Raft is optional. Making life simpler
  - Can easily be used as a library
