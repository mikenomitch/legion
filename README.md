# Legion

A distributed scheduler similar to Kubernetes or Nomad.

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
    - Workload type (containers, WASM, binaries, VMs, etc)
    - Scheduler logic (optimize for utilization, resilience, speed)
    - Networking
    - Volumes
    - And more: storage, the UI, CLI commands, job submit hooks, worker-node health checking, logging, etc
  - It runs on MacOS, Linux, and Windows
  - It can run long-lived or short-lived jobs, with or without state
- Other Benefits:
  - OTEL compliant tracing, logs, and metrics
  - Well-documented GRPC interface
  - OpenAPI spec for HTTP API
  - Jsonnet configuration language for simple and DRY job specifications
  - Raft is optional. Making life simpler
  - Built-in UI written completely in Rust with extensible pages
  - Can easily be used as a library
