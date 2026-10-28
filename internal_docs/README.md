# Internal Docs

Working material: plans, to-dos, and development notes. Nothing here is written
for a public audience — it is allowed to be unfinished, speculative, blunt about
gaps, and out of date.

Anything fit for users, operators, or contributors reading the code belongs in
[`docs/`](../docs/README.md) instead. That includes deeply technical material:
architecture and behavior docs are public.

## What goes where

| Put it here | Put it in `docs/` |
| --- | --- |
| Design plans, proposals, critiques | The shipped feature's reference page |
| To-do lists, milestones, roadmaps | Documented current behavior |
| Judgement-call records | Limitations a *user* needs to know (also state them publicly) |
| Competitive gap analyses | Honest "not implemented yet" notes on a real page |
| Library evaluations, migration write-ups | Architecture and internals |
| Assorted development notes | Operational runbooks |

## Current work

- [TODO.md](TODO.md) — the higher-level To Do list. Open work at the top grouped
  by area, shipped work in the Done section at the bottom. Check things off as
  you finish them.
- [architecture-audit-2026-10-09.md](architecture-audit-2026-10-09.md) — the
  2026-10-09 whole-codebase review for design-level errors, with per-finding
  fix status.
- [differentiators.md](differentiators.md) — the running list of what makes
  Legion different from K8s and Nomad (part idea list, part roadmap).
- [stateful-vs-nomad-k8s.md](stateful-vs-nomad-k8s.md) — honest gap analysis of
  the stateful primitives against StatefulSets and Nomad.
- [systemd-and-chef-analysis.md](systemd-and-chef-analysis.md) — could Legion
  replace systemd and/or Chef? What works today, the four gaps that block the
  service-supervisor claim, and the `converge`-runtime shape for host config.

## Notes

- [launch-spot-check-findings.md](launch-spot-check-findings.md) — the
  2026-10-11 pre-launch spot check on a real 3-manager, 2-worker AWS cluster
  and macOS: what works, eight P0s (batch tasks never finish, DAG ordering,
  HA strands workers, stock images fail in Linux containers, and more), and
  the shims the run needed. The rig and runbook are in
  [`tests/spot-check/`](../tests/spot-check/README.md). Every finding is fixed in #17;
  outcomes and deferrals are in
  [launch-spot-check-fixes-plan.md](plans/launch-spot-check-fixes-plan.md).
- [aws-phase1-findings.md](aws-phase1-findings.md) — the first real AWS
  benchmark run (2026-08-02/03): phase 1 passed, and it surfaced a
  manager heartbeat ceiling that was fsync-bound through raft. Start here
  to pick that work back up — what was measured, what was fixed, what we
  got wrong, and what to do next.
- [aws-phase2-findings.md](aws-phase2-findings.md) — the second AWS run
  (2026-08-04), and the one that found the scheduler's actual ceiling:
  **74% of plan applies are rejected and re-planned**, so throughput
  *falls* as eval threads are added. Also the five-row throughput table
  that is now the regression test for every scheduler change, what the
  rig got wrong (every cell of the first campaign was invalid, each for a
  different reason), and one claim retracted after checking the source.
- [aws-phase5-findings.md](aws-phase5-findings.md) — the fifth AWS run: the
  scheduler now outruns the CLI load generator at every fill level; the
  night's commit-path and intake fixes with their evidence
- [aws-phase4-findings.md](aws-phase4-findings.md) — the fourth AWS run: the
  rig was measuring its own submission ceiling, so phases 2-3 throughput
  numbers are retracted
- [aws-phase3-findings.md](aws-phase3-findings.md) — the third AWS run
  (2026-08-04), which measured #189 and **retired the phase-2
  diagnosis**: plan-apply rejections went 31.9% -> **0%**, per-eval plan
  time fell 36%, p99 fell 42% — and throughput did not move (86.6 ->
  80.35 alloc/sec at 5,000 nodes). Conflicts were real and are fixed, but
  they were never the ceiling; `apply` (the raft commit) is 67% of plan
  time and the scheduler runs ~7x below what its own planning cost
  permits. Also a read-connection-pool hypothesis raised and refuted
  in the same session.
- [development.md](development.md) — running locally, Linux worker containers,
  gRPC poking, CI lint baseline.
- [suspend-resume-linux-verification.md](suspend-resume-linux-verification.md)
  — Linux-only checks (KVM, cgroup v2, `/proc`) for each phase of the
  suspend/resume/oversubscription plan, with commands and pass criteria.
- [rustyriver-migration.md](rustyriver-migration.md) — porting streaming
  persistence from the Litestream binary to the embeddable rustyriver crate.
- [ui-api-explorer.md](ui-api-explorer.md) — library evaluation behind the in-app
  API Explorer.
- [old-notes.md](old-notes.md) — original Bear notes, kept for archaeology.

## Plans

Design plans and proposals live in [`plans/`](plans/). Each plan carries a
`> **Judgement calls:**` pointer under its title.

**Every non-trivial feature gets a plan before the code** — a new user-facing
surface, subsystem, default, security boundary, format change, or migration.
When one is required and what goes in it:
[`plans/README.md`](plans/README.md).

| Plan | Status |
| --- | --- |
| [adaptive-scheduler-concurrency-plan.md](plans/adaptive-scheduler-concurrency-plan.md) | in execution (phases 1–3 shipped 2026-08-04, default `observe`); its motivating "4 beats 16" measurement was retracted the next day ([aws-phase4-findings.md](aws-phase4-findings.md)), so phase 0 now has to find a conflict-bound cell before phase 4's default flip can be argued |
| [artifact-system-plan.md](plans/artifact-system-plan.md) | shipped |
| [artifact-cache-and-prewarm-plan.md](plans/artifact-cache-and-prewarm-plan.md) | in execution (Phase 1 partly shipped as the node content store, Phase 3 partly as suspend plan Phase 7a) |
| [autoscaler-plan.md](plans/autoscaler-plan.md) | **superseded** by autoscaler-v2-plan; its Phase 1 (`legion job scale`, count overrides) shipped 2026-07-30 and is v2's Phase 0 |
| [autoscaler-v2-plan.md](plans/autoscaler-v2-plan.md) | planned (2026-10-19): the autoscaler as a Legion app; `autoscale = true` defaults, overridable field by field per task, namespace, or cluster and resolved live; a self-reporting safeguard pipeline, JSON-RPC source/strategy/target plugins, cluster-wide usage rollups, node groups from simulated pending demand, sharded replicas. Service wake from zero, keyed/`count.per` scaling, vertical and predictive scaling deferred |
| [legion-apps-plan.md](plans/legion-apps-plan.md) | planned (2026-10-19): the substrate for ecosystem apps (autoscaler, GitOps): the `legion-system` namespace, node-side namespace restrictions, a default-on system worker in each manager, jobspec-declared permissions granted up to the submitter's, a per-allocation API socket, app manifests and lifecycle, declarative UI extensions (sandboxed pages second), and jobspec extensions (natural keys apps declare on the job, group, task, volume, port, or service, validated against app-declared JSON Schemas, with `apps.<app>` as the collision-proof spelling), with unknown jobspec fields becoming errors unless `--allow-unknown-fields`. Registry installs, tenant apps, and app-defined object kinds deferred |
| [auth-acl-plan.md](plans/auth-acl-plan.md) | phases 1–4 executed |
| [container-runtime-exec-and-signals-plan.md](plans/container-runtime-exec-and-signals-plan.md) | shipped (all seven phases, 2026-07-31) |
| [exec-and-actions-plan.md](plans/exec-and-actions-plan.md) | shipped (all five phases) |
| [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md) | executed (workstreams A/B/C/D + adversarial verification, 2026-07-28); lanes topology opt-in |
| [egress-credential-injection-plan.md](plans/egress-credential-injection-plan.md) | in execution (Phase 1 shipped: KV-backed header credentials on external `https://` upstreams, injected by the node so the workload never holds them; identity/webhook providers and audit events planned; sandbox guests cannot reach upstreams yet) |
| [edge-admission-control-plan.md](plans/edge-admission-control-plan.md) | in execution (Phase 1 built: the per-node claim window that sheds edge starts `OVERLOADED` before boot when ratification is slow, and enforces `maxProvisionalPerNode`. Manager-side speculation shedding and deadline-aware ratification planned) |
| [edge-boot-plan.md](plans/edge-boot-plan.md) | shipped (phases 1–4); phase 0 measurement not run, phase 5 backlog |
| [edge-ratify-throughput-plan.md](plans/edge-ratify-throughput-plan.md) | in execution (Phase 1 shipped 2026-10-14: in-process ratify load harness, ratifications fenced by committed state instead of epochs, create decided inside the entry, ratify off the async runtime. Per-node RPC batching deferred on data, networked load test planned) |
| [gang-scheduling-plan.md](plans/gang-scheduling-plan.md) | in execution (Phases 1–3 shipped; rank/world size, readiness timeout, and Phase 4 open) |
| [gated-sandbox-checkpoints-plan.md](plans/gated-sandbox-checkpoints-plan.md) | in execution (Phases 1 to 5 built: gated memory checkpoints, default deny, the static guest agent, gated idle signals, Legion-addressed TAPs, ingress into gated and TAP sandboxes, and upstreams in gated sandboxes; Phase 0, the real-VMM gate, is pending, so the gated-by-default flip is held) |
| [host-convergence-plan.md](plans/host-convergence-plan.md) | planned |
| [host-process-supervision-plan.md](plans/host-process-supervision-plan.md) | in execution (phases 1–3 landed; phase 4 uncommitted) |
| [job-status-plan.md](plans/job-status-plan.md) | shipped (per-replica outcomes and status-change events deferred) |
| [job-templates-plan.md](plans/job-templates-plan.md) | in execution |
| [job-updated-at-plan.md](plans/job-updated-at-plan.md) | shipped (detail-page header + node column label deferred) |
| [chunked-checkpoint-publish-plan.md](plans/chunked-checkpoint-publish-plan.md) | in execution (Phase 1 built: published parts as sealed, deduplicated chunks per job and dedup window, zero chunks skipped. Fetching only missing chunks, dirty-page hashing, and a real-S3 run are deferred) |
| [cluster-ca-provider-plan.md](plans/cluster-ca-provider-plan.md) | shipped (2026-10-14: the `clusterCa` provider seam — `builtin` default, `file` operator intermediate, `exec` external signer with a Vault PKI helper; post-sign verification, CSR manager certificates, heartbeat bundle pins, a leader-recorded agreement check, `legion net ca provider test`; switching providers without re-joining workers and per-manager agreement deferred) |
| [checkpoint-encryption-plan.md](plans/checkpoint-encryption-plan.md) | shipped (2026-10-11: published checkpoint parts sealed under per-part keys from the keyring's checkpoint key, with manual checkpoint-key rotation, parallel sealing, and streaming decrypt deferred) |
| [kv-encryption-plan.md](plans/kv-encryption-plan.md) | shipped (all four phases, 2026-09-21: KV values encrypted at rest by default, replicated keyring with the root key outside the store, `file`/`env`/`exec` providers, rotation + rewrap, join-time root key delivery; rendered task-config copies deliberately left for a follow-up plan) |
| [kv-reactive-task-updates-plan.md](plans/kv-reactive-task-updates-plan.md) | shipped |
| [kv-cert-delivery-plan.md](plans/kv-cert-delivery-plan.md) | shipped: live KV file updates (`onChange = "update"`) and KV-backed ingress certificates (`certFrom = { kv }`) |
| [late-binding-plan-apply-plan.md](plans/late-binding-plan-apply-plan.md) | shipped (phases 0–3; Phase 4 backlog deferred) |
| [manager-workload-identity-plan.md](plans/manager-workload-identity-plan.md) | in execution (phase 1 shipped: one credential path for every streaming store, the platform-identity chain checked at boot, the lease's missing conditional puts; `--restore-from` from the replica and credential introspection deferred) |
| [persistence-plan.md](plans/persistence-plan.md) | phases 0–1, 3 done |
| [preemption-and-eviction-plan.md](plans/preemption-and-eviction-plan.md) | shipped (graceful stop, six priority bands, plan-atomic preemption) |
| [multi-cluster-federation-plan.md](plans/multi-cluster-federation-plan.md) | planned |
| [multi-raft-persistence-plan.md](plans/multi-raft-persistence-plan.md) | planned |
| [multi-eval-plans-plan.md](plans/multi-eval-plans-plan.md) | in execution: Phase 2 shipped (the fused settle in every placing handler whose plan is its last write, and no second settle write). Phase 1, one running eval per job, was built and measured and left out until a lane-bound rig run shows a gain. Phase 3 deferred behind it |
| [namespaces-plan.md](plans/namespaces-plan.md) | shipped 2026-10-19 (all four phases; public page [docs/namespaces.md](../docs/namespaces.md)): canonical `namespace/name` job ids (bare in `default`), explicit `namespaces` on ACL rules, services and instance keys namespaced with jobs, cross-namespace upstreams, tenant events, namespace metrics, the UI switcher and Namespaces pages, and namespace policies (aggregate per-band quota, counts, pools, runtimes, host volumes); hierarchy, rename/move and namespaced KV deliberately out |
| [read-model-coherence-plan.md](plans/read-model-coherence-plan.md) | in execution (all five phases built; raft shadow check deferred) |
| [service-mesh-plan.md](plans/service-mesh-plan.md) | **authoritative for the mesh**; shipped M1–M10 (policy, HTTP mode, name-is-config, gated, ingress, handlers, mTLS, graph, sandbox vsock, templates) with two named deferrals — unifies the SD §8 follow-on with the handlers plan |
| [network-handlers-plan.md](plans/network-handlers-plan.md) | shipped as phases of service-mesh-plan (policy objects, `gated` mode, handlers); the `legion netd` process split is the deferred piece |
| [datapath-performance-plan.md](plans/datapath-performance-plan.md) | shipped: HTTP-mode pooling, `splice(2)` for bulk L4 on Linux, `least_latency` as the default strategy, the per-byte `legion_net_bytes_total` fix, `net_bench`; kTLS and multiplexed mesh tunnels deferred |
| [macos-gated-plan.md](plans/macos-gated-plan.md) | in execution: phases 1–3 (placement, the confined datapath, Seatbelt for `exec`, `pf` for Apple `container` VMs) implemented and unit-tested; phase 4, running it on macOS hardware, is open, so `auto` stays `host` on macOS |
| [mesh-interfaces-plan.md](plans/mesh-interfaces-plan.md) | shipped. Phase 0 fixed chain `dlp`/`log` and chain `onError`. Surface: one typed handler pipeline (S1), one upstream `to` whose scheme is its protocol (S2), `service.mesh` and TLS on ports (S3), one caller list (S4), one material source grammar and `FetchDatapathMaterial` (S5), and the removed knobs (S6). Datapath: the eBPF kernel classifier (A0–A3; host-mode egress enforced at `connect()`, transparent capture) and WASM plugins on proxy-wasm (B1–B3; `legion net plugin`). Deferred: `oci://` modules, streamed request bodies, plugin UI, see TODO |
| [nickel-config-language-plan.md](plans/nickel-config-language-plan.md) | shipped (2026-08-03; jsonnet removed, no compat shim) |
| [scheduler-performance-plan.md](plans/scheduler-performance-plan.md) | in execution |
| [scheduler-architecture-review-plan.md](plans/scheduler-architecture-review-plan.md) | proposals accepted; D1/D4/D5/D6 graduated into the two plans below, D8's local slice into the heartbeat plan |
| [scheduler-sharding-strategies-proposal.md](plans/scheduler-sharding-strategies-proposal.md) | proposal, revision 3 (sharding the scheduler across more raft groups; extends multi-raft Phase 8; recommends pool-keyed shards that are raft group **plus** own state file, co-located first, spread later, with plan shipping (M3) as the peer lever for planner-bound large fleets; Phase 0 attributes the October campaigns' wall per fleet-size regime before anything graduates) |
| [plan-path-scale-levers-plan.md](plans/plan-path-scale-levers-plan.md) | shipped (D4/D5/D6, 2026-07-30; batch-as-a-transaction, 2026-08-03) |
| [plan-path-fleet-scaling-plan.md](plans/plan-path-fleet-scaling-plan.md) | in execution (per-eval cost that grows with node count, from the 2026-10-09 campaign: 99 ms/eval at 10k vs 36 at 5k; Phases 1-3 done; Phase 4 removed the per-eval node lookup and O(jobs) SQL scans and instruments the whole eval, AWS re-run pending) |
| [plan-apply-conflict-reduction-plan.md](plans/plan-apply-conflict-reduction-plan.md) | in execution (two-pass bind + candidate-list restoration + decorrelated tails landed; **measured on AWS 2026-08-04: rejections 31.9% -> 0%**, but throughput unchanged — see [aws-phase3-findings.md](aws-phase3-findings.md); the throughput half of that cell is retracted by [aws-phase4-findings.md](aws-phase4-findings.md), which found the rig measuring its own submission rate) |
| [eval-path-raft-cost-plan.md](plans/eval-path-raft-cost-plan.md) | in execution (claim/complete fusion + batched enqueue + eval-loop batching landed; phase 5 — in-memory reprojection memo + audit hardening — landed 2026-08-06; the AWS re-measurement is not run) |
| [jsonb-text-affinity-plan.md](plans/jsonb-text-affinity-plan.md) | shipped 2026-08-06 (every JSON column rebuilt to TEXT affinity; `STRING`/`TIMESTAMP` columns deliberately left NUMERIC — see the calls doc) |
| [node-loss-storm-plan.md](plans/node-loss-storm-plan.md) | shipped 2026-08-06 (below-default reschedule priority, empty-node filter, single-round batched fan-out, recovery counter); the AWS storm re-measurement is not run |
| [eval-thread-default-plan.md](plans/eval-thread-default-plan.md) | planned; **deliberately picks no number** (the 2026-08-05 sweep rose monotonically 177.27 → 251.68 alloc/s from 4 to 32 eval threads and its 64-thread cell was driver-bound, so `available_parallelism()` is wrong but the knee is unmeasured; Phase 0a is load-generator work and gates everything) |
| [bounded-candidate-scoring-plan.md](plans/bounded-candidate-scoring-plan.md) | shipped (opt-in `score_sample`, 2026-07-30) |
| [heartbeat-scale-plan.md](plans/heartbeat-scale-plan.md) | shipped (benchmark + batching + liveness + cadence, 2026-07-30) |
| [heartbeat-cadence-at-scale-plan.md](plans/heartbeat-cadence-at-scale-plan.md) | **shipped** 2026-08-03 (sizing + freshness warning + per-node lost windows + adaptive cadence); phase 5, adaptive cadence **on by default** at 2,500 beats/s, shipped 2026-10-09; the AWS reproduction is not run |
| [liveness-off-the-raft-path-plan.md](plans/liveness-off-the-raft-path-plan.md) | **shipped** 2026-08-03 (D8: heartbeats leave raft — leader-memory liveness, transitions-only replication, failover grace, digest, display checkpoint); phase 1's AWS measurement is not run |
| [aws-comparative-benchmark-plan.md](plans/aws-comparative-benchmark-plan.md) | in execution (cross-system campaign rerun 2026-10-14 with every store on NVMe, results in [docs/benchmarks.md](../docs/benchmarks.md)) |
| [worker-simulation-plan.md](plans/worker-simulation-plan.md) | shipped (2026-07-30) |
| [launch-spot-check-fixes-plan.md](plans/launch-spot-check-fixes-plan.md) | in execution (every 2026-10-11 spot-check finding fixed in #17, open; P1-5 in part; deferrals in §8.7; AWS re-run pending) |
| [worker-plane-auth-plan.md](plans/worker-plane-auth-plan.md) | in execution: Phases 0 to 8 built; hardware verification open (launch blocker: workers on other hosts lose exec, logs, and the mesh. Also owns local workload isolation: ACLs on by default, the host guard, gated containers by default, the container privilege floor, and the `host` grant for host-level jobs. Carries the sandbox-exec plan's ticket caller class) |
| [scheduler-diagnosis-tooling-plan.md](plans/scheduler-diagnosis-tooling-plan.md) | planned (`legion scheduler diagnose` + the metric gaps that blocked the 2026-08-04 diagnosis) |
| [scheduler-plan-snapshot-plan.md](plans/scheduler-plan-snapshot-plan.md) | shipped |
| [scheduling-index-plan.md](plans/scheduling-index-plan.md) | shipped (phases 0–3; group-path + backlog deferred) |
| [security-gap-closure-plan.md](plans/security-gap-closure-plan.md) | shipped (2026-10-17): the README's and TODO's known security gaps plus an audit's findings — KV reads through jobspecs, root-worker path escapes, node credentials leaving the node, forged identities, plaintext raft; direct exec revocation push, fleet certificates, and git/OCI fetch judging deferred |
| [service-discovery-final-plan.md](plans/service-discovery-final-plan.md) | v1 shipped; §8 mesh follow-on superseded by service-mesh-plan.md |
| [service-discovery-plan.md](plans/service-discovery-plan.md) | superseded (rationale + full mesh design) |
| [service-discovery-mesh-and-config.md](plans/service-discovery-mesh-and-config.md) | superseded (cookbook + mesh extensions) |
| [service-discovery-critique.md](plans/service-discovery-critique.md) | superseded (self-critique) |
| [stateful-workloads-and-instance-identity-plan.md](plans/stateful-workloads-and-instance-identity-plan.md) | shipped |
| [task-runtime-rename-plan.md](plans/task-runtime-rename-plan.md) | shipped |
| [task-start-critical-path-plan.md](plans/task-start-critical-path-plan.md) | in execution (phases 1–3 built: start stage timings, startup probing for health checks, gated namespaces before their listeners; phases 4–5, start reports and the streamed datapath off the heartbeat, deferred; the stage timings ran on real KVM in the sandbox-boot campaign, where they found its bottlenecks) |
| [ui-jump-hints-plan.md](plans/ui-jump-hints-plan.md) | shipped |
| [operator-ui-gaps-plan.md](plans/operator-ui-gaps-plan.md) | shipped (2026-10-12; public-address auto-detect, a manager reverse proxy, and `GetAllocation` deferred) |
| [manager-reverse-proxy-plan.md](plans/manager-reverse-proxy-plan.md) | planned (open an allocation's or instance's web UI through the manager when node ports are closed: one-time code for a target-scoped cookie, an L4 tunnel on the worker plane, a new `proxy` ACL resource; subpath first, a host per target in Phase 2, `legion proxy` port-forward in Phase 3; multi-host needs worker-plane-auth Phase 1) |
| [ui-holistic-polish-plan.md](plans/ui-holistic-polish-plan.md) | shipped (2026-09-02; embedding the UI in the binary deliberately left open) |
| [ui-task-and-template-redesign-plan.md](plans/ui-task-and-template-redesign-plan.md) | shipped 2026-10-12 (task page tabs, guided template launch, Actions tab; run history, server-side fan-out and a template plan RPC deferred) |
| [workload-identity-oidc-plan.md](plans/workload-identity-oidc-plan.md) | shipped (task identities) |
| [volume-plugin-system-plan.md](plans/volume-plugin-system-plan.md) | shipped (v1; CSI bridge + snapshot/clone deferred) |
| [sandbox-snapshots-and-fast-start-plan.md](plans/sandbox-snapshots-and-fast-start-plan.md) | superseded in part by suspend-resume-and-oversubscription (§0 convergence shipped; mechanics still the reference) |
| [suspend-resume-and-oversubscription-plan.md](plans/suspend-resume-and-oversubscription-plan.md) | in execution (Phases 1-7 shipped, Phase 8 deferred, KVM checks pending) |
| [suspend-resume-and-oversubscription-plan.md](plans/suspend-resume-and-oversubscription-plan.md) | in execution (Phases 1-3 and 7a shipped, KVM checks pending) |
| [lazy-memory-restore-plan.md](plans/lazy-memory-restore-plan.md) | in execution: Phase 1 built (restores trust the content store's digest instead of rehashing every part, cloud-hypervisor v52+ restores on demand, `config.memory_restore = "lazy" \| "eager"` with `lazy` the default). KVM measurement open. Streaming a published restore, working-set prefetch, and cloud-hypervisor `copyonwrite` planned (executes the suspend plan's Phase 8 lazy memory item) |
| [sandbox-scaling-plan.md](plans/sandbox-scaling-plan.md) | in execution: Phases 0–6 shipped (measurement, group commit and write trimming, `StreamWork` delivery, blocked evals, untombstoned minted ids, archived idle keyed instances, and the `session` instance mode with running sessions as node-owned soft state and promotion); session suspend, identity, and the session log deferred; Phase 7's wake on request is built under wake-on-request-plan (keyed and archived instances); Phases 8+ planned (snapshot-clone density, a sub-VM seam, cells past a measured trigger) |
| [scheduler-lane-entry-cost-plan.md](plans/scheduler-lane-entry-cost-plan.md) | in execution: Phase 1 shipped (keyed placements bind their instance from two indexes instead of scanning the task's whole population, the largest piece of serial lane time in the profile), with the pipelined lane (Phase 2) and one fsync per entry (Phase 3) designed, not built |
| [session-durability-and-routing-plan.md](plans/session-durability-and-routing-plan.md) | in execution in #36 (Phases 1–5 built; real-S3, sandbox-memory and edge-move runs open) (builds on session mode: concurrent starts on each node, sessions that survive a worker restart, the start node in the session id, edge starts picked by a load balancer outside Legion with no manager on the path, sessions that sleep on their node, and `store` sessions that move between nodes without split brain; a batched raft session directory deferred behind a trigger) |
| [wake-on-request-plan.md](plans/wake-on-request-plan.md) | in execution: Phase 1 shipped (`suspend.wake`, `legion start gateway` that resumes a suspended keyed instance on a request and holds it until the instance serves, `CONNECT` for any declared port, a wake webhook, wake fields on `GetInstance`); gateway metrics, waking sessions, and node-local wake on connect planned |
| [checkpoints-on-stream-plan.md](plans/checkpoints-on-stream-plan.md) | shipped Phase 1 (checkpoint requests pushed on `StreamWork`, reports on an early heartbeat, so suspend and resume no longer wait on the cadence). A report path that skips the heartbeat is deferred until early beats show up as load |
| [sandbox-exec-plan.md](plans/sandbox-exec-plan.md) | in execution: Phases 1 to 3 built and Phase 0 measured on one host; KVM and rig verification open (exec into Linux microVMs over the guest agent's vsock, and direct exec with worker-verified tickets as a third caller class on the worker-plane-auth listener; implemented together with worker-plane-auth, which it amends in §9) |
| [rootfs-base-cache-plan.md](plans/rootfs-base-cache-plan.md) | in execution: Phase 1 built and booted on real KVM (cloud-hypervisor) in the sandbox-boot campaign (one base root disk per image per node, cloned per sandbox, per-sandbox files by debugfs, `ensure_image_base` for prefetchers, one tag resolution per burst); artifacts on the cached path, the `container` runtime, visibility, and Firecracker planned |
| [sandbox-boot-throughput-plan.md](plans/sandbox-boot-throughput-plan.md) | shipped, phases 1–3 (raw disks and `hvc0` on cloud-hypervisor, `running` on guest readiness, `stateful.placement`, the worker's start and stop paths off the lock, measured on AWS: 0.23 s a boot, 69 a second from four 4-vCPU nodes); phase 4's lock fixes (a 96-vCPU node at 150 to 200 boots a second sustained); the root disk ceiling on large nodes, open |
| [suspend-resume-ui-plan.md](plans/suspend-resume-ui-plan.md) | shipped (UI-1 to UI-6, with the §7.3 server reads: ListNodeUsage, GetNodeContent, last_stop, checkpoint_support) |
| [session-ui-plan.md](plans/session-ui-plan.md) | shipped (UI only: sessions and envelopes on the job, instance, allocation, node, and task pages, Promote and End, and bounded fetching, polling, and rendering for very large session counts; `ListInstances` paging, counts, and node/envelope filters deferred to the API) |
| [elastic-session-envelopes-plan.md](plans/elastic-session-envelopes-plan.md) | in execution: Phase 1 built (`stateful.envelope.max` and `shrinkAfter`: each node grows and shrinks its session envelope between a floor and a ceiling, one scheduler-lane entry per resize, a shrink capped on the node first); preemptible elastic room and a fleet resize budget planned |
| [shared-alloc-dir-proposal.md](plans/shared-alloc-dir-proposal.md) | proposal |
| [swappable-seams-plan.md](plans/swappable-seams-plan.md) | shipped (2026-10-14: one attachment type for the datapath, one listener and dialer, the typed worker ↔ `netd` boundary, one object-storage layer, DNS on hickory-proto, the egress proxy on hyper, and the VIP and mesh-TLS fixes; the `legion netd` process split, a `NetdInput` wire encoding, GCS/Azure, and a `Vmm` trait deferred) |
| [data-dir-env-plan.md](plans/data-dir-env-plan.md) | shipped (`LEGION_DATA_DIR`; no config-file field, by design) |
| [isolate-runtime-plan.md](plans/isolate-runtime-plan.md) | planned (2026-10-28): a second, lighter sandbox tier. Isolates are WebAssembly components on wasmtime (pooling allocator, copy-on-write images, Cranelift ahead of time), with JS (StarlingMonkey) and Python (CPython for WASI) as pre-initialized images, in a confined `legion-isolate` host process per cordon (trust partition, default per namespace) with one shard per core; they start in microseconds, hold no stack while they await on the stackless callback ABI, page out and hibernate when idle, do all I/O through the worker's egress datapath, and burst to a `sandbox` microVM by an attached-VM API or by replay before egress. Rides session mode, nothing per isolate on raft. V8 (Phase 7, behind a trigger), continuation migration (Phase 8), and shared pool envelopes deferred |

## Judgement calls

[`calls/`](calls/) holds one record per plan: the decisions taken where the plan
was ambiguous, contradictory, or silent — what was decided, what the alternatives
were, and why.

**Every non-trivial feature must leave one.** Name the file after its plan with
`_calls` appended (`plans/<name>.md` → `calls/<name>_calls.md`), append to the
existing file rather than starting a second one, and link it from the plan
header. Entries give the call, the alternatives, the trade-off, and why — ranked
by project impact. The full template and a worked example:
[`calls/README.md`](calls/README.md).

- [architecture-audit-2026-10-09_calls.md](calls/architecture-audit-2026-10-09_calls.md)
- [artifact-system-plan_calls.md](calls/artifact-system-plan_calls.md)
- [artifact-cache-and-prewarm-plan_calls.md](calls/artifact-cache-and-prewarm-plan_calls.md)
- [autoscaler-plan_calls.md](calls/autoscaler-plan_calls.md)
- [autoscaler-v2-plan_calls.md](calls/autoscaler-v2-plan_calls.md)
- [legion-apps-plan_calls.md](calls/legion-apps-plan_calls.md)
- [auth-acl-plan_calls.md](calls/auth-acl-plan_calls.md)
- [aws-comparative-benchmark-plan_calls.md](calls/aws-comparative-benchmark-plan_calls.md)
- [bounded-candidate-scoring-plan_calls.md](calls/bounded-candidate-scoring-plan_calls.md)
- [cluster-ca-provider-plan_calls.md](calls/cluster-ca-provider-plan_calls.md)
- [container-runtime-exec-and-signals-plan_calls.md](calls/container-runtime-exec-and-signals-plan_calls.md)
- [container-task-kind-macos_calls.md](calls/container-task-kind-macos_calls.md)
- [data-dir-env-plan_calls.md](calls/data-dir-env-plan_calls.md)
- [datapath-performance-plan_calls.md](calls/datapath-performance-plan_calls.md)
- [edge-admission-control-plan_calls.md](calls/edge-admission-control-plan_calls.md)
- [edge-boot-plan_calls.md](calls/edge-boot-plan_calls.md)
- [edge-ratify-throughput-plan_calls.md](calls/edge-ratify-throughput-plan_calls.md)
- [egress-credential-injection-plan_calls.md](calls/egress-credential-injection-plan_calls.md)
- [elastic-session-envelopes-plan_calls.md](calls/elastic-session-envelopes-plan_calls.md)
- [exec-and-actions-plan_calls.md](calls/exec-and-actions-plan_calls.md)
- [gated-restore-datapath-wait_calls.md](calls/gated-restore-datapath-wait_calls.md): the 5 s datapath wait on every sandbox restore (no plan, a fix to the gated checkpoints plan's wait)
- [gated-sandbox-checkpoints-plan_calls.md](calls/gated-sandbox-checkpoints-plan_calls.md)
- [rootfs-base-cache-plan_calls.md](calls/rootfs-base-cache-plan_calls.md)
- [sandbox-boot-throughput-plan_calls.md](calls/sandbox-boot-throughput-plan_calls.md)
- [sandbox-clean-stop_calls.md](calls/sandbox-clean-stop_calls.md)
- [security-gap-closure-plan_calls.md](calls/security-gap-closure-plan_calls.md)
- [swappable-seams-plan_calls.md](calls/swappable-seams-plan_calls.md)
- [sandbox-exec-plan_calls.md](calls/sandbox-exec-plan_calls.md)
- [sandbox-scaling-plan_calls.md](calls/sandbox-scaling-plan_calls.md)
- [sandbox-snapshots-and-fast-start-plan_calls.md](calls/sandbox-snapshots-and-fast-start-plan_calls.md)
- [scheduler-lane-entry-cost-plan_calls.md](calls/scheduler-lane-entry-cost-plan_calls.md)
- [wake-on-request-plan_calls.md](calls/wake-on-request-plan_calls.md)
- [checkpoints-on-stream-plan_calls.md](calls/checkpoints-on-stream-plan_calls.md)
- [session-durability-and-routing-plan_calls.md](calls/session-durability-and-routing-plan_calls.md)
- [gang-scheduling-plan_calls.md](calls/gang-scheduling-plan_calls.md)
- [heartbeat-cadence-at-scale-plan_calls.md](calls/heartbeat-cadence-at-scale-plan_calls.md)
- [heartbeat-scale-plan_calls.md](calls/heartbeat-scale-plan_calls.md)
- [host-convergence-plan_calls.md](calls/host-convergence-plan_calls.md)
- [host-process-supervision-plan_calls.md](calls/host-process-supervision-plan_calls.md)
- [task-start-critical-path-plan_calls.md](calls/task-start-critical-path-plan_calls.md)
- [job-status-plan_calls.md](calls/job-status-plan_calls.md)
- [job-templates-plan_calls.md](calls/job-templates-plan_calls.md)
- [jsonb-text-affinity-plan_calls.md](calls/jsonb-text-affinity-plan_calls.md)
- [job-updated-at-plan_calls.md](calls/job-updated-at-plan_calls.md)
- [read-model-coherence-plan_calls.md](calls/read-model-coherence-plan_calls.md)
- [operator-ui-gaps-plan_calls.md](calls/operator-ui-gaps-plan_calls.md)
- [ui-holistic-polish-plan_calls.md](calls/ui-holistic-polish-plan_calls.md)
- [ui-task-and-template-redesign-plan_calls.md](calls/ui-task-and-template-redesign-plan_calls.md)
- [chunked-checkpoint-publish-plan_calls.md](calls/chunked-checkpoint-publish-plan_calls.md)
- [checkpoint-encryption-plan_calls.md](calls/checkpoint-encryption-plan_calls.md)
- [isolate-runtime-plan_calls.md](calls/isolate-runtime-plan_calls.md)
- [kv-encryption-plan_calls.md](calls/kv-encryption-plan_calls.md)
- [kv-reactive-task-updates-plan_calls.md](calls/kv-reactive-task-updates-plan_calls.md)
- [kv-cert-delivery-plan_calls.md](calls/kv-cert-delivery-plan_calls.md)
- [join-token-file-and-cli-timeouts_calls.md](calls/join-token-file-and-cli-timeouts_calls.md)
- [launch-spot-check-fixes-plan_calls.md](calls/launch-spot-check-fixes-plan_calls.md)
- [lazy-memory-restore-plan_calls.md](calls/lazy-memory-restore-plan_calls.md)
- [liveness-off-the-raft-path-plan_calls.md](calls/liveness-off-the-raft-path-plan_calls.md)
- [manager-contention-instrumentation_calls.md](calls/manager-contention-instrumentation_calls.md)
- [managers-page-raft-members_calls.md](calls/managers-page-raft-members_calls.md)
- [manager-reverse-proxy-plan_calls.md](calls/manager-reverse-proxy-plan_calls.md)
- [manager-workload-identity-plan_calls.md](calls/manager-workload-identity-plan_calls.md)
- [macos-gated-plan_calls.md](calls/macos-gated-plan_calls.md)
- [mesh-interfaces-plan_calls.md](calls/mesh-interfaces-plan_calls.md)
- [multi-cluster-federation-plan_calls.md](calls/multi-cluster-federation-plan_calls.md)
- [multi-raft-persistence-plan_calls.md](calls/multi-raft-persistence-plan_calls.md)
- [multi-eval-plans-plan_calls.md](calls/multi-eval-plans-plan_calls.md)
- [namespaces-plan_calls.md](calls/namespaces-plan_calls.md)
- [network-handlers-plan_calls.md](calls/network-handlers-plan_calls.md)
- [service-mesh-plan_calls.md](calls/service-mesh-plan_calls.md)
- [nickel-config-language-plan_calls.md](calls/nickel-config-language-plan_calls.md)
- [persistence-plan_calls.md](calls/persistence-plan_calls.md)
- [plan-apply-conflict-reduction-plan_calls.md](calls/plan-apply-conflict-reduction-plan_calls.md)
- [plan-path-fleet-scaling-plan_calls.md](calls/plan-path-fleet-scaling-plan_calls.md)
- [plan-path-scale-levers-plan_calls.md](calls/plan-path-scale-levers-plan_calls.md)
- [pre-benchmark-hardening_calls.md](calls/pre-benchmark-hardening_calls.md)
- [preemption-and-eviction-plan_calls.md](calls/preemption-and-eviction-plan_calls.md)
- [scheduler-sharding-strategies-proposal_calls.md](calls/scheduler-sharding-strategies-proposal_calls.md)
- [service-discovery-final-plan_calls.md](calls/service-discovery-final-plan_calls.md)
- [shared-alloc-dir-proposal_calls.md](calls/shared-alloc-dir-proposal_calls.md)
- [stateful-workloads-and-instance-identity-plan_calls.md](calls/stateful-workloads-and-instance-identity-plan_calls.md)
- [suspend-resume-and-oversubscription-plan_calls.md](calls/suspend-resume-and-oversubscription-plan_calls.md)
- [suspend-resume-ui-plan_calls.md](calls/suspend-resume-ui-plan_calls.md)
- [session-ui-plan_calls.md](calls/session-ui-plan_calls.md)
- [test-build-footprint_calls.md](calls/test-build-footprint_calls.md)
- [volume-plugin-system-plan_calls.md](calls/volume-plugin-system-plan_calls.md)
- [worker-plane-auth-plan_calls.md](calls/worker-plane-auth-plan_calls.md)
- [worker-simulation-plan_calls.md](calls/worker-simulation-plan_calls.md)
- [workload-identity-oidc-plan_calls.md](calls/workload-identity-oidc-plan_calls.md)
