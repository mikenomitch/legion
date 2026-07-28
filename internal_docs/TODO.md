# TODO

The running to-do list for Legion. **Open work is at the top**, grouped by area;
everything already shipped is in [Done](#done) at the bottom.

How to use it:

- Each group links its plan in [`plans/`](plans/) and its public page in
  [`docs/`](../docs/README.md) where one exists. If an item needs a plan and
  doesn't have one, that's noted.
- **When you finish something here, check it off and move it to Done in the same
  change.** A todo that outlives its feature is worse than no todo.
- Items that came out of a plan's "deferred" or "follow-up" section say so, so
  the plan stays the source of truth for the design.

---

## In flight (open PRs)

| PR | What it lands | Notes |
| --- | --- | --- |
| [#151](https://github.com/mikenomitch/legion/pull/151) | Job templates: typed parameters, guided UI submission, ACL-gated delegation | Supersedes [#108](https://github.com/mikenomitch/legion/pull/108); needs a plan + calls doc |
| [#137](https://github.com/mikenomitch/legion/pull/137) | Real Raft persistence (quorum-committed writes), node drain, metrics, version handshake | Deviates from [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md) — changeset replication instead of per-method determinism |
| [#152](https://github.com/mikenomitch/legion/pull/152) | Fix flaky artifact staging test marker race | Test-only |
| [#143](https://github.com/mikenomitch/legion/pull/143) | Note the node-level builtin resource override gap | Docs only; the fix itself is a todo below |
| [#84](https://github.com/mikenomitch/legion/pull/84) | Instrument DB hot paths, batch task-status updates | Old; rebase or close |
| [#108](https://github.com/mikenomitch/legion/pull/108) | Versioned job templates | Superseded by #151 — close |

---

## Scheduling & placement

- [ ] Worker groups + taint/toleration equivalent. Node pools exist as an ACL
      and config concept but nothing scheduling-side consumes them.
- [ ] Priorities and eviction/preemption, with eviction toggled at the pool level
      — [plan](plans/preemption-and-eviction-plan.md) (PLANNED: graceful stop →
      priority bands → plan-atomic preemption → governance; pool toggle is
      `schedulerSettings.preemption.disabledPools`)
  - [x] At least think through fair-share policies — find the 80/20 rather than
        building a full hierarchical scheduler. Thought through in the plan's
        prior art + non-goals: the 80/20 is priority bands now, per-namespace
        band caps later (phase 4); fair-share/hierarchical queues explicitly
        deferred (Kueue's policy-matrix issue history is the argument against).
- [ ] Schedule onto the same node as an *existing job*. Soft `same_node_as`
      colocation works within one job spec
      ([scheduler/policy.rs](../src/manager/scheduler/policy.rs)); cross-job
      placement doesn't exist.
- [ ] Node-level builtin resource overrides are silently ignored —
      `ResourcePool::from_node` skips any builtin name, so `resources: { cpu: 40000 }`
      in a worker config disappears without a warning
      ([#143](https://github.com/mikenomitch/legion/pull/143)). Open questions: does
      an override replace the fingerprinted value or act as a second ceiling, and
      how does it compose with `reservedResources`? Either way an ignored key
      should warn or fail validation.
- [x] Node drain as a first-class operation — `legion node cordon|drain|uncordon`,
      eval-mediated eviction that respects stateful move protection
      ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] Fix the slowest bits of the scheduler using the benchmarks in
      [docs/testing-and-benchmarks.md](../docs/testing-and-benchmarks.md).
      Candidate construction is index-served
      ([docs/scheduling-indexes.md](../docs/scheduling-indexes.md)) and the
      per-plan snapshot is fixed (Arc'd nodes + cached free pools —
      [plan](plans/scheduler-plan-snapshot-plan.md), numbers in
      [benchmarks/scheduler/README.md](../benchmarks/scheduler/README.md)).
      The allocations map is shared per node the same way
      (`SharedNodeAllocations`, copy-on-write). No O(cluster) per-plan
      deep clone remains on the plan path; next candidates would come
      from fresh profiling, not this list.
- [ ] Scheduling-index follow-ups deferred from
      [plan](plans/scheduling-index-plan.md) (see its calls doc):
      index-served candidates for multi-task **group** co-placement (v1
      groups still scan — calls entry 10); pluggable index kinds;
      `count.per` served by partition enumeration; device-set
      (`ResourceValue::Items`) dims; task-level `index_hint`;
      incremental orthant enumeration with early termination.
- [ ] Worker-fingerprinted `disk` as a built-in schedulable resource — the
      default `fit` index already dims on `resources.disk` and picks it up
      automatically wherever it exists; today that is only operator-declared
      `config_attrs.resources.disk` (plan open question, lean yes; also
      leaned on by the artifact-cache plan's disk accounting).
- [ ] Close the last plan-fencing window: `apply_plan_submission` commits (and
      so publishes the bumped epoch) *before* it refreshes the read-model
      allocation cache (`cache_add_allocations`). A plan that samples epochs in
      that microsecond-wide gap sees the new epoch but the pre-commit cache, so
      it can still plan against stale capacity and pass the compare-and-set.
      Uncovered while moving the epoch sample to snapshot time (see Done →
      Scheduling); the fix is to make the epoch bump and the cache refresh
      visible atomically, which changes the lock ordering around the write
      transaction and wants a plan.

## Task runtimes

Plans: [task-runtime-plugins](../docs/task-runtime-plugins.md).

- [ ] A **WASM** runtime
- [ ] `exec`: unveil support
- [ ] `exec`: network namespaces (netavark? CNI?) — see *Networking* below
- [ ] **Sandbox snapshots, fast start, and forking** —
      [plan](plans/sandbox-snapshots-and-fast-start-plan.md) (PLANNED; §0 storage
      convergence shipped with the sandbox runtime)
  - [ ] Phase 1 — disk snapshots (volume `snapshot` op, `local-dir` reflink,
        snapshot table + CRUD RPCs/CLI/MCP, ACL resource)
  - [ ] Phase 2 — restore in place (`RestoreInstance`, quiesce hooks wired to actions)
  - [ ] Phase 3 — memory snapshots (Firecracker pause/create/resume, `compat`
        constraints in the scheduler)
  - [ ] Phase 4 — fast start (regional snapshot stores, `legion snapshot warm`,
        warm-node scoring)
  - [ ] Phase 5 — forking (volume `clone`, identity disk, `legion-guest-agent`,
        `instance create --from-snapshot --count N`)
  - [ ] Phase 6 — lazy memory (UFFD handler, remote page sources)
- [ ] Split a task runtime's implementation into parts — isolation vs. runner vs.
      artifact consumption? (idea, needs a plan)
- [ ] Shared files between tasks in the same allocation —
      [proposal](plans/shared-alloc-dir-proposal.md), unimplemented, and blocked
      on allocations holding more than one task
- [ ] Use a virtual filesystem for the alloc directory (idea)

## Rollouts

Plan: [rollout-strategy-plugins](../docs/rollout-strategy-plugins.md) · docs: [rollouts.md](../docs/rollouts.md)

- [ ] A **blue-green** strategy
- [ ] A **canary** strategy

## Artifacts

Plan: [artifact-system-plan.md](plans/artifact-system-plan.md) · docs: [artifacts.md](../docs/artifacts.md)

- [ ] **Artifact cache + pre-warming/pre-pull + scheduler locality scoring** —
      [plan](plans/artifact-cache-and-prewarm-plan.md) (PLANNED)
  - [ ] Phase 1 — content-addressed cache
  - [ ] Phase 2 — warm RPCs and policies
  - [ ] Phase 3 — cache advertisement + `artifact_locality` scoring
  - [ ] Multiple layers of pre-fetching; tag nodes as pre-warmed; pre-fetch as
        part of the deploy process
  - [ ] Unblocks `artifacts.containerImageDesugar` being on by default
- [ ] `identity` / `nodeIdentity` artifact auth end-to-end. The signing side
      shipped with workload identity; fetch-time use still fails with an
      actionable error.
- [ ] `onFetchFailure` policy distinct from `on_unhealthy` — today fetch
      flakiness consumes restart/move budgets
- [ ] Push container images directly into a worker's artifact cache, so a small
      cluster needs no registry at all
- [ ] Artifact staging on Windows: hardlink/reflink fallbacks, path semantics,
      image consumption (see *Windows support pass*)

## Stateful & volumes

Plans: [stateful-workloads](plans/stateful-workloads-and-instance-identity-plan.md), [volume-plugin-system](plans/volume-plugin-system-plan.md) · docs: [volumes.md](../docs/volumes.md), [stateful-workloads.md](../docs/stateful-workloads.md)

All deferred from the volume plan v1:

- [ ] **CSI bridge plugin** — the driver contract was designed for it; nothing in
      core has to change
- [ ] **Snapshot / restore / clone / expand** — op names are reserved in the
      contract. Overlaps with sandbox snapshot phases 1–2 and 5 above; build them
      together.
  - [ ] `clone` × node locality: same-node clone native, cross-node fan-out
        snapshot-mediated, `capabilities` reporting `cloneScope: node | cluster`
- [ ] **Multiple volumes per instance** (`stateful.volumes: [...]`) — the schema is
      already keyed by (instance, name), so this is purely additive
- [ ] **Cross-node data migration** — deliberately manual today (backup → delete →
      re-mint → restore) until snapshot/restore land
- [ ] Mount concurrently with artifact staging rather than after it (a real
      start-latency win; nothing in the contract forbids it)

## Service discovery & the mesh

Plan: [service-discovery-final-plan.md](plans/service-discovery-final-plan.md) · docs: [service-discovery.md](../docs/service-discovery.md)

v1 (catalog, ports, env, `list`/`resolve`/`doctor`) shipped, and so has the
connect layer: `connect.upstreams`, the per-node L4 forwarder, pluggable
balancing, and control-plane intentions. What is left of the v2 mesh, in the
plan's own adoption order:

- [ ] Node-local DNS + template functions (no-NAT resolver, TCP fallback,
      `ndots:1`, coalesce/splay/fail-static)
- [ ] Datapath plugin ABI + bridge/CNI shim (`mode: 'auto'`)
- [ ] **mTLS between forwarders**, so `service.allow` intentions rest on
      cryptographic workload identity (which has shipped) rather than on the
      control plane filtering the endpoint feed
- [ ] L7 waypoint (retries, timeouts, canary split), then sandboxed WASM
      middleware filters
- [ ] Mesh observability: RED metrics + service graph, `doctor` gaining
      live-traffic evidence
- [ ] Drain-before-SIGTERM (rollout integration)

## Networking

No plan yet — this needs one before any code.

- [ ] Overlay network. Unix-domain-socket transport is on the table.
- [ ] CNI, or a custom networking plugin that can speak CNI
- [ ] Networking fingerprinters (so service discovery can pick a datapath per node)
- [ ] Port forwarding
- [ ] Ingress proxy / load balancer, running as a core-binary app that can also
      be split out into its own job

## Persistence & cluster operations

Plans: [persistence-plan.md](plans/persistence-plan.md), [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md) · docs: [state-store.md](../docs/state-store.md), [raft-operations.md](../docs/raft-operations.md), [persistence-streaming.md](../docs/persistence-streaming.md)

- [x] **Real Raft persistence** — quorum-committed writes, per-lane log stores,
      snapshots/compaction, real membership
      ([#137](https://github.com/mikenomitch/legion/pull/137)). Ships
      changeset replication rather than the per-method determinism audit in
      [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md);
      the as-built design and its judgement calls are in
      [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md).
  - [x] Multi-raft lanes (control / scheduler / status) + lane routing —
        single-writer table ownership enforced at capture time, co-located
        lane leadership, per-lane snapshots
  - [x] **Lane-leadership convergence is bounded and cannot break a lane**
        (2026-07-28). The JC-40 loop campaigned for LEADERLESS lanes, which
        bypasses openraft's own election policy and restarts the node's
        candidacy faster than an election can finish — 132 terms in 210s on a
        2-core runner with the lane never electing. It now campaigns only for
        a lane led elsewhere, with per-lane exponential backoff and escalating
        stand-downs, and co-location is documented as BEST EFFORT rather than
        an invariant. Also fixed the root cause the livelock was hiding: a
        joiner counted as a voter while its lane membership was still JOINT,
        so bring-up looked complete mid-join.
        [Plan §JC-40 correction](plans/durability-and-ops-execution-plan.md) ·
        [calls](calls/durability-and-ops-execution-plan_calls.md) ·
        [docs/multi-raft.md](../docs/multi-raft.md)
  - [ ] **Cross-lane composites have no home while lane leadership is split.**
        Convergence is best effort, so a cluster can sit with lanes led by
        different managers; a job submit then fails on the control leader (its
        scheduler half) and on the scheduler leader (its control half), and
        `/readyz` returns 503 until it converges. A real fix needs a
        leader-transfer API openraft 0.9 does not have — the alternative is a
        cross-lane write forwarder. See
        [docs/multi-raft.md](../docs/multi-raft.md) § "Lane leadership".
  - [ ] Per-lane tuning knobs, bottleneck tracing, Raft health UI
  - [ ] The 12× lane-isolation benchmark
  - [ ] Speculative: Raft autopilot
- [x] Loading state back in from a file — `legion state backup` +
      `--restore-from` ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] **Cluster federation** —
      [plan](plans/multi-cluster-federation-plan.md) (PLANNED, with a
      [calls doc](calls/multi-cluster-federation-plan_calls.md)): named
      clusters in the CLI/UI, symmetric peering (no hub), request
      forwarding, federated SSO, multi-cluster jobs, standby managers
      (learners + zone auto-replacement), and an async DR standby cluster
      with manual promotion and generation fencing. Cross-cluster service
      discovery deferred to a follow-up plan (see plan §11).
- [x] Version handshake between manager and worker — protocol version plus
      build-skew reporting, upgrade order in [docs/upgrades.md](../docs/upgrades.md)
      ([#137](https://github.com/mikenomitch/legion/pull/137))
- [ ] **Node configuration jobs** — like sysbatch, but also runs on join and can
      add dynamic node metadata (CNI setup, volumes, artifacts, Ansible-shaped
      work). Needs a plan.
- [ ] Reconfigure a worker from the manager with CRUD commands
  - [ ] Existing nodes of a changed type need the update pushed to them, which
        implies nodes store a reference to their type
- [ ] Root-privilege split: run networking and volume setup as root while the
      task itself doesn't

## Job types

- [ ] **Sysbatch** — run once on every node
- [ ] **System job** — N per X (1 per metal, 2 per region, …). Is this really a
      job type, or just a richer `count`?

## Auth, TLS & security

Plan: [auth-acl-plan.md](plans/auth-acl-plan.md) · docs: [auth-and-acls.md](../docs/auth-and-acls.md)

- [ ] **KV ACLs on `GetArtifactCredential`.** Artifact auth (`auth.kv`) lets any
      job read any KV credential path today. The enforcement point exists and is
      marked — see the SECURITY BOUNDARY comment on `get_artifact_credential` in
      [src/manager/server.rs](../src/manager/server.rs), the RPC comment in
      `proto/legion.proto`, and the call-out in
      [docs/artifacts.md](../docs/artifacts.md). Accepted for now; close it.
- [ ] Worker mTLS (deferred from auth phase 3)
- [ ] Dynamic auth-method dylib plugins (deferred from auth phase 2)
- [ ] Workload-identity API credentials (deferred from auth phase 4)
- [ ] Token-resolution cache (deferred from auth phase 4)
- [ ] **TLS is autoconfigured and auto-rotated.** Joining a cluster with TLS
      should be trivial.
  - [ ] **Plan this, don't just code it.** Should the "insecure client" really be
        a "public cert client" (Let's Encrypt by default, bring-your-own CA
        supported), leaving the "secure client" as the self-signed-CA path only
        workers hold? Or is TLS the wrong mechanism here entirely?
  - [ ] Fix the join-token and pre-register call destinations
  - [ ] Env var or flag for contacting the manager — the join-server port
        shouldn't need to be supplied, since it's in the JWT
- [ ] **Cert manager built in**, with public-CA routes, usable by workloads and
      not just the control plane

## Observability

docs: [observability.md](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- [ ] A trace viewer in the UI (make it dev-only at first)
- [x] Metrics endpoint and health probes — Prometheus `/metrics`, `/healthz`,
      `/readyz` ([docs/metrics.md](../docs/metrics.md), [#137](https://github.com/mikenomitch/legion/pull/137)).
      Broader instrumentation (worker-side metrics, OTLP metric push) still open.
- [x] Refresh [docs/observability.md](../docs/observability.md) — corrected; it
      had claimed OTLP export did not exist while [src/otel.rs](../src/otel.rs)
      ships it ([#137](https://github.com/mikenomitch/legion/pull/137))

## Templates, registry & packaging

The Pack/Helm equivalent. Job templates are in flight on
[#151](https://github.com/mikenomitch/legion/pull/151); the registry half is untouched.

- [ ] Write the plan + calls doc for job templates (#151 shipped without one)
- [ ] There is a default registry
- [ ] I can add a registry
- [ ] I can run with `@<registry>/<job>`
- [ ] I can inspect a job in the registry (reads its metadata)
- [ ] UI for filling in variables from a job definition's metadata
- [ ] Plugins can be fetched by URL or from the registry
- [ ] Automatic build & push step for container images — `legion build` /
      `legion prep`, plus a context file
- [ ] Buildpack runner

## Ecosystem apps (the omakase experience)

Each of these should run inside the core binary *and* be splittable into its own
job, so day 1 is simple and day N is performant. Each needs its own plan.

- [ ] **Autoscaler** — [plan](plans/autoscaler-plan.md) (PLANNED) ·
      [calls](calls/autoscaler-plan_calls.md)
  - [ ] Phase 1 — `legion job scale`: count overrides outside the job version,
        `job_scale` eval, `scale` ACL resource, resubmit-ownership rules
  - [ ] Phase 2 — per-allocation CPU/memory usage collection (heartbeat →
        `allocation_health`), request-vs-usage in CLI/UI
  - [ ] Phase 3 — task autoscaler MVP: `scaling { min, max, target }` block,
        in-manager leader-gated evaluator, damping, scale-status surface
  - [ ] Phase 4 — full `checks` form, Prometheus source, `per-replica`
        strategy, activation-gated `min: 0`, `headroom`
  - [ ] Phase 5 — escape-to-job: `legion autoscaler agent`, control-plane
        identity policy, KV-lock leadership, `LEGION_MANAGER_ADDR` injection
  - [ ] Phase 6 — node autoscaling: placement-shortfall signal, node groups,
        exec-ABI providers, drain-based scale-in, `ForgetNode` RPC
  - [ ] Phase 7 — dylib `metrics`/`strategies` plugin kinds, `ui.link` Apps
        sidebar section
- [ ] Ingress proxy / load balancer (also listed under *Networking*)
- [ ] GitOps helper — deploy to git, job updates automatically
- [ ] Cert manager (also listed under *Auth, TLS & security*)
- [ ] Overlay network controller

## Plugin surface

docs: [plugin-system.md](../docs/plugin-system.md)

Shipped plugin categories: schedulers, task runtimes, rollout strategies,
artifacts, volumes, node checks. Still to build:

- [ ] Resource plugins
- [ ] Command & API-endpoint plugins
- [ ] Fingerprinter plugins (including networking fingerprinters)
- [ ] CNI / networking plugins
- [ ] Allocation-setup plugins (pre-fetch an image, snapshot, or data set)
- [ ] Task-group-setup plugins
- [ ] Logging plugins (log sinks exist, but not as loadable plugins)
- [ ] Eval hooks — react to cluster events from inside Legion, with the option to
      break out into a job (this is roughly how the autoscaler would work)
- [ ] Pluggable node statuses for degradation (disk pressure, network pressure, …)
- [ ] Pluggable configuration language?

## Agent surfaces

docs: [agent-readiness.md](../docs/agent-readiness.md)

The MCP server, self-documenting CLI, structured output, and documented gRPC
surface all shipped. What's left is the harder "can an agent actually operate
this" work:

- [ ] An agent can easily write a plugin (scaffolding, schema, a worked example)
- [ ] An agent can easily configure a worker to talk to a manager
- [ ] An agent can search the registry and find jobs worth installing and running

## Embedding

- [ ] Legion can be called as a library, not just as a CLI

## UI

docs: [ui-architecture.md](../docs/ui-architecture.md)

Deferred from [ui-jump-hints-plan.md](plans/ui-jump-hints-plan.md):

- [ ] Jump hints past the tenth row — a 20- or 50-row page labels only its first
      ten, and the rest look unfinished until you know it is deliberate
- [ ] Jump hints on the Overview dashboard and on detail-page tables; today only
      the six index pages register rows
- [ ] Verify the jump-hint chords survive Safari and Firefox. The letters avoid
      Chromium's reserved set, but "the page wins `⌘`+digit" is asserted from
      the reserved-key list, not measured
      ([calls entry 1](calls/ui-jump-hints-plan_calls.md))

## Cross-cutting

- [ ] **Audit every name decision in depth.** Important, and gets harder the
      longer it waits.
- [ ] Performance and scale: prove Legion scales better than K8s, and than Nomad

---

## Long term

- [ ] A **speed** scheduler that optimizes for fast placement. Needs a plan
      before any code — the mechanism is an open design question, not a settled
      one.
- [ ] A **VM** runtime beyond the microVM sandbox — libvirt? cloud-hypervisor?
      QEMU?
- [ ] **Windows support pass**
  - [ ] Artifact staging on Windows: hardlink/reflink fallbacks, path semantics,
        image consumption (see the follow-ups in
        [artifact-system-plan.md](plans/artifact-system-plan.md))

---

# Done

## Worker nodes

- [x] Add a worker node
- [x] CLI arg to get the address
- [x] Makes contact via RPC
- [x] Message passing between the two via RPC
- [x] Heartbeats; marked unhealthy on miss, watchable from the CLI
- [x] Basic fingerprinting system
- [x] Dynamic attributes that can be updated
- [x] Shows up in the manager's ledger of worker nodes
- [x] Can be restarted from the server

## Scheduling

docs: [scheduler-and-eval-loop.md](../docs/scheduler-and-eval-loop.md), [jobs-tasks-and-scheduling.md](../docs/jobs-tasks-and-scheduling.md)

- [x] Ledger command — see every node and what's on it from the CLI
- [x] A framework for scheduler plugins
- [x] A "random" scheduler
- [x] Constraints
- [x] A "bin-pack" scheduler
- [x] A "spread" scheduler (the inverse of bin-pack)
- [x] Affinities and anti-affinities
- [x] Task groups
- [x] Rescheduling, including health-check-triggered reschedule after restart
- [x] Job health checks reschedule a job — initial failures restart it, failure
      after N times forces a reschedule
- [x] Dynamic metadata: a node attribute change re-evaluates affected jobs
      (`UpdateNodeDynamic` enqueues evals)
- [x] Plan epochs are sampled at *snapshot* time, not after planning. The
      version vector used to fence a plan submission was read after
      `build_plan`, so a competing plan that applied in between was already
      counted in the epoch and the compare-and-set passed against stale
      capacity — an intermittent node oversubscription
      (`concurrent_plans_cannot_oversubscribe_a_node`). The scheduler now
      samples every candidate node's epoch before it reads
      `cached_allocations_by_node`, in one batched query
      (`Store::allocation_version_vector_for_candidates`), and selects the
      affected subset at submit time.
- [x] **Declarable scheduling indexes** — ad-hoc `partition_by` × `dims`
      indexes over node attrs and free resources, nearest-fit candidate
      generation, automatic per-task selection, shadow mode, `job plan`
      explain output, Prometheus series, and the default-on `fit` index
      (free cpu/memory/disk by pool) armed above `index_min_nodes` —
      [plan](plans/scheduling-index-plan.md),
      [docs/scheduling-indexes.md](../docs/scheduling-indexes.md).
      Deferred pieces are open items above (group-path candidates, disk
      fingerprinting, backlog).
- [x] AGENTS.md now requires before/after benchmark numbers on scheduler
      changes, and `benches/scheduler_bench.rs` grew a
      `candidate_construction` group that isolates candidate-set cost
      (indexes on vs off) across fleet sizes.

## Task runtimes

docs: [task-runtime-plugins.md](../docs/task-runtime-plugins.md)

- [x] A framework for task-runtime plugins
- [x] Rename "task kind" → "task runtime"
      ([plan](plans/task-runtime-rename-plan.md), single-pass, no shim)
- [x] A **noop** runtime ([docs](../docs/task-runtime-noop.md))
- [x] An **exec** runtime ([docs](../docs/task-runtime-exec.md)) — runs the given
      command and args, as a specific user, joining an existing cgroup (v2 only),
      with landlock
- [x] A **container** runtime ([docs](../docs/task-runtime-container.md)) with no
      Docker dependency: macOS uses Apple `container`; Linux uses a low-level OCI
      runtime (`youki` by default, or any OCI-spec runtime via
      `LEGION_OCI_RUNTIME`). Linux runtimes can't pull images, so the runtime
      builds an OCI bundle from the image's OCI layout
      ([oci_bundle.rs](../src/task_plugins/oci_bundle.rs)).
- [x] A **sandbox** runtime ([docs](../docs/task-runtime-sandbox.md)) — OCI images
      in microVMs for untrusted code and agent workspaces

## Artifacts

[plan](plans/artifact-system-plan.md) · [docs](../docs/artifacts.md)

- [x] Grab container images (`oci://` fetcher + ORAS mediaType)
- [x] Auth for fetching (KV credentials via `GetArtifactCredential` + cluster
      `artifacts.auth` matchers)
- [x] Grab files onto disk (https/file/git/s3 fetchers, task dirs, checksums, safe
      unpack, `${artifact.<name>}` references, per-artifact status in CLI and UI)
- [x] A plugin system for fetchers (`plugins/artifacts/<scheme>/`)
- [x] OCI layout → runtime bundle conversion so Legion-managed images work on
      Linux container workloads

## Rollouts

[docs](../docs/rollouts.md)

- [x] A new version of a job starts a deployment
- [x] A framework for deployment strategies
- [x] An "immediate" strategy that cycles everything out in place
- [x] A "rolling" strategy
- [x] Health-gated progression, manual and automatic
- [x] A UI for all of it

## KV & secrets

[plan](plans/kv-reactive-task-updates-plan.md) · [docs](../docs/kv-store.md)

- [x] A task can read a KV value
- [x] Updating a value restarts the task, when configured to
- [x] A value can be used in the job definition
- [x] Reactive updates: injection into env and files, watch/on_change/on_delete
      policy, dependency graph, debounce, per-runtime capability gating

## Service discovery (v1)

[plan](plans/service-discovery-final-plan.md) · [docs](../docs/service-discovery.md)

- [x] Declare a named service on a task
- [x] Declare health checks on that task
- [x] Query address and port from other services
- [x] Catalog as a projection (no registration RPC, no drift, only-healthy)
- [x] `legion service list | resolve | doctor`; works identically on Linux and macOS
- [x] Connect to another service by name, load balanced, without writing any
      client code: `connect.upstreams` -> `$LEGION_UPSTREAM_<SVC>`, a node-local
      loopback forwarder, and `round_robin`/`random`/`least_conn` (pluggable —
      one trait impl plus one registry row)
- [x] Declare which other jobs may reach my service (`service.allow`, matched
      against the consuming job id). Enforced on the **connect path only**: the
      manager filters the per-node endpoint feed, so a denied consumer's
      forwarder has nothing to dial. Not cryptographic identity — a task that
      already knows a backend's `address:port` can still dial it directly, and
      that hop is unauthenticated. mTLS below is what closes it.
- [x] `legion service resolve --one`, `service` entries in the capability
      catalog, and `legion_service_list`/`legion_service_resolve` MCP tools.
      Note `service list -j` / `resolve -j` now emit the standard `{ok, data}`
      envelope instead of a bare array (`jq '.[0]'` -> `jq '.data[0]'`, or use
      `--one`, which needs no `jq`).

## Stateful workloads & volumes

[stateful plan](plans/stateful-workloads-and-instance-identity-plan.md) · [volume plan](plans/volume-plugin-system-plan.md) · [docs](../docs/volumes.md)

- [x] Instance identity: slot and keyed modes, storage handles, fencing
- [x] Volume plugin system: the `VolumePlugin` contract, exec ABI, built-in
      `local-dir`/`host`/`nfs`/`tmpfs`, a reference exec plugin, the conformance
      harness (`legion volume plugin test`), the volume table and reclaim,
      node-local pinning, `legion volume` CLI + MCP tools, UI, and the
      [Postgres](../docs/guides/postgres.md) / [Redis](../docs/guides/redis.md) guides
- [x] Block-mode volumes, consumed directly by the sandbox runtime

## Persistence

[plan](plans/persistence-plan.md) · [docs](../docs/persistence-streaming.md), [raft-operations.md](../docs/raft-operations.md)

- [x] Streaming SQLite replication, Litestream-style, with optional lease failover
- [x] Raft control plane: leader election, failover controls, membership,
      observability, CLI and UI operations

## Workload identity

[plan](plans/workload-identity-oidc-plan.md) · [docs](../docs/workload-identity.md)

- [x] Manager as an OIDC issuer: keys, discovery, JWKS, rotation
- [x] Token issuance RPC, job-spec API, worker delivery, change mode
- [x] Node identity tokens and control-plane identity (phase 7)

## Auth & ACLs

[plan](plans/auth-acl-plan.md) · [docs](../docs/auth-and-acls.md)

- [x] Phase 1 — the ACL model: resources, actions, scopes, default-deny, one-line
      enforcement, and the `tests/acl_endpoints.rs` coverage gate
- [x] Phase 2 — interactive SSO and richer admin
- [x] Phase 3 — worker-plane hardening (join tokens from the authenticated
      `CreateJoinToken` RPC; the insecure `:4748` bootstrap listener is gone)
- [x] Phase 4 — operations at scale

## Exec & actions

[plan](plans/exec-and-actions-plan.md) · [docs](../docs/exec-and-actions.md)

- [x] Worker exec core: `ExecSupport`/`exec()` on the runtime trait, PTY, session
      limits
- [x] Manager proxy, ACLs, audit events, and `legion exec`
- [x] Browser path: WebSocket bridge and an xterm.js terminal in the UI
- [x] Actions: spec schema, `ListJobActions`, server-side render and param
      validation, the `action` ACL resource, `legion action list|run`
- [x] Actions UI, MCP tools, and docs

## Observability

[docs](../docs/observability.md), [logging.md](../docs/logging.md), [event-stream.md](../docs/event-stream.md)

- [x] Tracing in the scheduler
- [x] Benchmarks in a dedicated directory
      ([docs](../docs/testing-and-benchmarks.md))
- [x] Task log capture, tailing, rotation, and external shipping (including OTLP)
- [x] OTLP trace export for Legion's own spans ([src/otel.rs](../src/otel.rs))
- [x] Event stream: scheduler events as history and live tail (`ListEvents`,
      `StreamEvents`, CLI, UI)

## Job types & dependencies

[docs](../docs/jobs-tasks-and-scheduling.md)

- [x] Short-lived jobs — the `batch` lifecycle
- [x] Cron jobs — `schedule` on a batch task, taking a cron expression or
      `{ every: <duration> }`
- [x] Task groups co-locate on one node
- [x] DAGs: `depends_on` with `start`/`complete`/`success` conditions, topological
      sort, cycle rejection, cross-group dependencies (so DAGs span machines) and
      group-bound sidecars

## Agent surfaces

[docs](../docs/agent-readiness.md)

- [x] A self-documenting CLI for agents (`legion docs`, `legion capabilities`)
- [x] Structured output everywhere (`--json` + the standard envelope)
- [x] gRPC endpoints documented in `proto/legion.proto` as the source of truth
- [x] An MCP server (`legion mcp serve`), with mutating tools gated behind
      `--allow-write`
- [x] Idempotency keys on mutating RPCs; validate/plan/preview paths

## UI

[docs](../docs/ui-architecture.md)

- [x] Keyboard navigation: ⌘K command palette, `j`/`k`/`o` index selection, `u`
      to go up a level
- [x] Jump hints — hold ⌘ (Ctrl) to label index rows with digits and sidebar
      items with letters, then ⌘1 / ⌘⇧B to jump
      ([plan](plans/ui-jump-hints-plan.md),
      [calls](calls/ui-jump-hints-plan_calls.md)). Rows past the tenth, the
      Overview dashboard, and detail-page tables are open items above.
