# Legion Documentation

Public documentation for Legion, a distributed scheduler similar to Kubernetes
or Nomad. These pages are written for people using, operating, or evaluating
Legion, and for contributors reading the code.

Plans, roadmaps, to-do lists, and development notes live in
[`internal_docs/`](../internal_docs/README.md) instead.

## Installing

- [Installing Legion and Its Runtimes](installation.md): the `legion` binary,
  then what each runtime needs on a worker host: runc (or crun) for
  containers on Linux; `/dev/kvm`, cloud-hypervisor, and a guest kernel for
  microVM sandboxes (including AWS nested virtualization); Apple `container`
  on macOS.

## Architecture

- [Configuration Language](configuration-language.md): Nickel syntax
  essentials, the `vars` binding behind `--var`, imports, where imports are
  refused, and the `.json` path for machine-emitted config.
- [Managers and Workers](architecture-managers-workers.md): cluster shape,
  responsibilities, and the manager/worker protocol.
- [Scheduler and Eval Loop](scheduler-and-eval-loop.md): the control loop that
  drives placement.
- [Scheduling Indexes](scheduling-indexes.md): declarable nearest-fit indexes
  over node attributes and free resources, and how candidate selection scales.
- [Late-Binding Plan Apply](late-binding-plan-apply.md): ranked candidate
  lists with commit-time node choice, and the conflict-requeue backstop. Shows
  how concurrent submits stop fighting over one node.
- [Adaptive Scheduler Concurrency](adaptive-scheduler-concurrency.md): the
  manager measures its own plan-apply conflict rate and decides how many evals
  it plans at once, so `evalThreads` becomes a ceiling instead of a guess.
- [Jobs, Tasks, and Scheduling](jobs-tasks-and-scheduling.md): job spec shapes,
  placement semantics, and edge cases.
- [State Store](state-store.md): manager and worker persistence.
- [Data Directory](data-directory.md): where local state lives, and
  `LEGION_DATA_DIR` to move it.
- [Node Liveness](node-liveness.md): where node liveness is tracked, what a
  leader election does to it, and why a heartbeat costs no commit.
- [Heartbeat Cadence and Scale](heartbeat-cadence-and-scale.md): sizing a
  control plane for a fleet, the per-node lost window, and the freshness
  ratio that shows whether the fleet is keeping up.
- [UI Architecture](ui-architecture.md): the SolidJS app and its design system.

## Workloads

- [Task Supervision](task-supervision.md): process exits and the `restart`
  policy, batch completion, worker-owned cgroup limits, and `exec`
  hardening.
- [Task Runtime Plugins](task-runtime-plugins.md): the task-runtime plugin contract.
- [Task Runtime: exec](task-runtime-exec.md)
- [Task Runtime: container](task-runtime-container.md): OCI images on runc
  or crun (Linux) and Apple `container` (macOS). Capabilities, ulimits,
  layer ownership, stdio, and the per-node image cache.
- [Task Runtime: sandbox](task-runtime-sandbox.md): OCI images in microVMs for
  untrusted code and agent workspaces.
- [Task Runtime: isolate](task-runtime-isolate.md): WebAssembly isolates
  (Python, JavaScript, or your own component) in a confined, multi-tenant
  host process per cordon, with a microVM tier for code that needs a real
  machine: lifetimes, runs and tiers, the guest's world, the security model,
  and operating the host.
- [Task Runtime: noop](task-runtime-noop.md)
- [Resource Enforcement](resource-enforcement.md): enforcing declared `memory`,
  `cores`, and `cpu` on Linux with hard memory limits, exclusive pinned
  cores, proportional CPU weight, and the interface runtimes adopt.
- [Devices: GPUs and Other Hardware](devices.md): device plugins
  (`legion-device/1`), `resources.devices` requests, topology-aware
  assignment, CDI container edits, device health, and device quota.
- [Job Templates](job-templates.md): reusable, versioned job specs with typed
  variables, guided submission, and template-scoped delegation.
- [Artifacts](artifacts.md): declaring, fetching, verifying, and staging inputs.
- [Stateful Workloads & Instance Identity](stateful-workloads.md): slot and
  keyed instances, storage handles, fencing, and choosing a mode.
- [Sandboxes at Scale](sandboxes-at-scale.md): running many keyed sandboxes:
  what each lifecycle step costs the control plane, delivery within a round
  trip, blocked creates when capacity runs out, and measuring it.
- [Session Mode](session-mode.md): `stateful.mode = "session"`, units that
  start and stop with no raft write inside per-node envelopes, the session
  index and its `complete` flag, duplicates, and promotion to a keyed
  instance in place.
- [Session Runs](session-runs.md): `legion session run`, one call that runs
  code once in a fresh, single-use session of a task with a `run` block and
  returns stdout, stderr, the exit status, a JSON result, and named files;
  stopped runs as data, per-node idempotency, and who may run what.
- [Child Sessions](child-sessions.md): a session another unit of the job
  started and owns. Covers the `children` block, the typed owner reference,
  starting a child with `--owner` or from inside a task with no credential,
  when an owner's end ends its children, promotion, and the limits.
- [Resize](resize.md): `legion alloc resize` and `legion instance resize`,
  which change a running unit's `memory`, `memoryMax`, or `cpu` in place as
  an override over the spec. Covers what each runtime can change live, the
  grow and shrink ordering, sessions inside their envelope, the outcomes
  (`applied`, `deferred`, `refused`, `needs_restart`, `not_supported`), and
  the limits.
- [Volumes](volumes.md): persistent storage for stateful tasks. Covers plugins,
  the driver contract, node pinning, and reclaim.
- [Suspend and Resume](suspend-and-resume.md): suspending a keyed instance to
  free its reservation and resuming it where it left off, automatic suspend
  when idle or under memory pressure, identity refresh after a restore, disk
  and memory snapshots, restoring an instance in place, expiry, and the
  `snapshot` ACL resource.
- [Park](park.md): `suspend.park` and `$LEGION_PARK_SOCKET`, where the
  workload says when it is waiting (`park`) or working (`busy`) and its node
  suspends it at the park's deadline, with the checkpoint it chose.
- [Ingress](ingress.md): the bundled ingress app. `service.ingress = { host
  }` routes a public hostname (and a path prefix) to a service on ports 80
  and 443, with HTTP/2, WebSockets, balancing, automatic HTTPS through the
  cert manager, and the addresses to point DNS at; `legion app enable
  ingress` turns it on.
- [Gateway: Wake on Request](gateway.md): `legion start gateway`, a stateless
  front door that routes requests to keyed instances by host, header, or
  `CONNECT`, resumes a suspended one on a request, and holds the request
  until it serves (`suspend.wake`).
- [Rollouts](rollouts.md): update strategies, health gating, manual/auto
  progression.
- [Rollout Strategy Plugins](rollout-strategy-plugins.md)
- [Autoscaling](autoscaling.md): `autoscale = true` on a task, layered
  defaults (task, namespace, cluster), the safeguard pipeline, pins and
  pauses, `legion scaling status|explain|history`; Prometheus, HTTP, and
  plugin sources, scale to zero, `legion scaling simulate`, standalone
  policies for external targets and node groups (`autoscaler/ScalingPolicy`
  and `autoscaler/NodeGroup` objects, `legion object apply`); and `legion job
  scale`, which sets count overrides without a new version or rollout.
- [Writing Autoscaling Plugins](autoscaling-plugins.md): the
  `legion-scaling/1` JSON-RPC protocol for sources, strategies, and
  targets, `legion scaling plugin test`, the Rust SDK, and the example
  plugins.
- [Graceful Stop](graceful-stop.md): the signal, grace period, and SIGKILL
  sequence every platform-initiated stop uses, the machine-readable stop
  notice, and why an allocation was removed.
- [Preemption and Eviction](preemption-and-eviction.md): priority bands, when
  the scheduler displaces lower-band work, and how to preview or prevent it.
- [Gang Scheduling](gang-scheduling.md): `gang` on a task places its replicas
  all together or not at all. A gang that does not fit waits, holding
  nothing (bounded by `placementTimeout`), a lost member restarts the whole
  gang (`onMemberLoss`), and `job plan` reports whether it would fit now.
- [Edge Boot](edge-boot.md): starting work before the placement is durable.
  Covers gate levels, boot tickets, ratification, and the kill path.

## Guides

- [Running Postgres](guides/postgres.md): a single-writer database on a
  stateful slot, and what happens when its node dies.
- [Running Redis](guides/redis.md): the same machinery when durability is
  optional.
- [Running Agent Sandboxes](guides/agent-sandboxes.md): thousands of microVM
  sandboxes that suspend when idle and resume in place. Covers worker setup,
  oversubscription, the platform lifecycle, capacity, and failure modes.

## Platform Services

- [Service Discovery](service-discovery.md): declaring services and finding
  healthy endpoints.
- [Network Handlers](network-handlers.md): the built-in service mesh. Covers
  egress/ingress policy objects, HTTP mode with retries and canary splits,
  `<service>.svc.legion` names, `gated` networking (namespaces, and vsock for
  sandboxes), ingress chains, `authz`/`proxy` handlers, mTLS with the mesh
  CA, certificates served from KV, the service graph, drain-before-stop, and
  `legion net`.
- [Network Plugins](network-plugins.md): WASM modules on the proxy-wasm ABI
  that run inside the node datapath as handlers. Covers plugin objects,
  stages, properties, limits and failure, signing without the key, declared
  HTTP calls, and `legion net plugin`.
- [KV Store](kv-store.md): CRUD, versioning, locks, reactive task updates, and
  live file updates (`onChange = "update"`).
- [Cert Manager](cert-manager.md): the bundled `certs` app. Issuers (Let's
  Encrypt and other ACME CAs over DNS-01 or HTTP-01, a private CA kept in KV,
  self-signed) and Certificates written to KV and renewed before they expire,
  with rate-limit-safe backoff, status, a UI page, and `legion cert renew`.
- [Serving TLS Certificates from KV](guides/tls-certificates.md): a guide to
  serving a certificate kept in KV from the node or from nginx, with no
  restarts, and renewing it with the cert manager or a job of your own.
- [KV Encryption at Rest](kv-encryption.md): on by default. Covers the root key
  outside the store, wrapped data keys inside it, rotation, backups, and
  what to do when a key is missing.
- [Auth & ACLs](auth-and-acls.md): tokens, policies, scopes, and enforcement.
  Token lifetimes and child tokens for sub-agents.
- [Audit Journal](audit-journal.md): the durable, replicated record of every
  write and denial: accessor, principal, on-behalf-of chain, target, request
  digest, idempotency key, approval, and outcome.
- [Approvals and Rate Limits](approvals-and-rate-limits.md): human-approval
  gates for chosen writes, and per-token write rate limits.
- [Usage Accounting](usage-accounting.md): reserved CPU, memory, cores and
  custom resources integrated over time per namespace, job, and submitter.
- [Namespaces](namespaces.md): name and permission scopes for jobs (not an
  isolation boundary), qualified ids, `-n`/`LEGION_NAMESPACE`, delegated
  creation, crossing namespaces in the mesh, and namespace policies (aggregate
  quota per band, counts, allowed pools, runtimes and host volumes).
- [Runtime Disable](runtime-disable.md): `legion operator runtime disable`,
  the cluster-wide switch for a task runtime with a published escape: no new
  placement, session start, edge claim, resume, or restart in scope, an
  optional drain, and blocked work that places again on `enable`.
- [The Cluster CA](cluster-ca.md): the CA behind the worker plane, manager
  certificates, join tokens, and the mesh. Built in by default; or an
  intermediate from your own PKI (`file`) or an external signer such as
  Vault PKI (`exec`), with every certificate checked, managers checked for
  agreement, and rotation per provider.
- [Apps](apps.md): Legion's add-ons as jobs in `legion-system` with a granted
  identity, kept running by the app reconciler. Covers the manifest,
  placement on the managers' system workers, install and upgrade with a
  permission prompt, config and code fields, why direct edits are refused,
  bundled apps rolling with the build, and `legion app`.
- [App Objects](app-objects.md): object kinds apps declare (Legion's
  custom resources): the manifest's `kinds`, `legion object`, the `object`
  ACL resource and `requires`, status and the summary, bounded cleanup on
  delete, and the reconcile loop apps use.
- [GitOps](gitops.md): deploy jobs from git with the bundled gitops app.
  A Repo (`legion gitops add`) deploys every jobspec in a repository's
  path into its namespace and keeps it deployed: what a sync does, drift,
  prune, status, deploying from CI with `legion gitops sync --wait`,
  private repositories, and why gitops can deploy no more than a Repo's
  writer could.
- [Workload Identity (OIDC)](workload-identity.md): short-lived, verifiable task
  identity for external providers.
- [Exec & Actions](exec-and-actions.md): running commands inside a workload,
  interactive PTY, curated actions.

## Operations

- [Logging](logging.md): task log capture, tailing, rotation, shipping, OTLP.
- [Observability](observability.md): tracing and metrics.
- [Diagnosing Scheduler Performance](diagnosing-scheduler-performance.md):
  organized by symptom (throughput fell when you scaled up, the box is idle
  but capped, new work waits behind a backlog). Covers what to read, what
  the neighboring metric rules out, and what to change.
- [Event Stream](event-stream.md): scheduler events as history and live tail.
- [Watches, Revisions, and Blocking Queries](watches-and-blocking-queries.md):
  the revision on every job, allocation, and KV read; blocking reads
  (`--index`), `legion job watch` / `legion kv watch` with resume, linearizable
  and read-your-writes consistency, job compare-and-set, and plan diffs.
- [Streaming Persistence](persistence-streaming.md): WAL shipping and
  lease-fenced failover.
- [Running Raft](raft-operations.md): leader election, failover, membership.

## Extending & Contributing

- [Plugin System](plugin-system.md): how in-process `cdylib` plugins load, and
  the trade-offs.
- [Codebase Conventions](codebase-conventions.md)
- [Testing and Benchmarks](testing-and-benchmarks.md)
- [Benchmarks](benchmarks.md): placement throughput, latency, and recovery
  on AWS from 1k to 50k nodes, with Nomad and Kubernetes on the same rig.
- [Worker Simulation](worker-simulation.md): `--simulate N` runs thousands of
  simulated nodes, speaking the real wire protocol, from one process. Use it
  for benchmarking and load testing.
- [Agent Readiness](agent-readiness.md): MCP, structured output, capabilities,
  and the recommended agent workflow.
