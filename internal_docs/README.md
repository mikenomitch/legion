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
| [autoscaler-plan.md](plans/autoscaler-plan.md) | planned |
| [auth-acl-plan.md](plans/auth-acl-plan.md) | phases 1–4 executed |
| [container-runtime-exec-and-signals-plan.md](plans/container-runtime-exec-and-signals-plan.md) | shipped (all seven phases, 2026-07-31) |
| [exec-and-actions-plan.md](plans/exec-and-actions-plan.md) | shipped (all five phases) |
| [durability-and-ops-execution-plan.md](plans/durability-and-ops-execution-plan.md) | executed (workstreams A/B/C/D + adversarial verification, 2026-07-28); lanes topology opt-in |
| [egress-credential-injection-plan.md](plans/egress-credential-injection-plan.md) | in execution (Phase 1 shipped: KV-backed header credentials on external `https://` upstreams, injected by the node so the workload never holds them; identity/webhook providers and audit events planned; sandbox guests cannot reach upstreams yet) |
| [edge-boot-plan.md](plans/edge-boot-plan.md) | shipped (phases 1–4); phase 0 measurement not run, phase 5 backlog |
| [gang-scheduling-plan.md](plans/gang-scheduling-plan.md) | planned |
| [gated-sandbox-checkpoints-plan.md](plans/gated-sandbox-checkpoints-plan.md) | in execution (Phases 1 to 5 built: gated memory checkpoints, default deny, the static guest agent, gated idle signals, Legion-addressed TAPs, ingress into gated and TAP sandboxes, and upstreams in gated sandboxes; Phase 0, the real-VMM gate, is pending, so the gated-by-default flip is held) |
| [host-convergence-plan.md](plans/host-convergence-plan.md) | planned |
| [host-process-supervision-plan.md](plans/host-process-supervision-plan.md) | in execution (phases 1–3 landed; phase 4 uncommitted) |
| [job-status-plan.md](plans/job-status-plan.md) | shipped (per-replica outcomes and status-change events deferred) |
| [job-templates-plan.md](plans/job-templates-plan.md) | in execution |
| [job-updated-at-plan.md](plans/job-updated-at-plan.md) | shipped (detail-page header + node column label deferred) |
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
| [namespaces-plan.md](plans/namespaces-plan.md) | planned (namespace object + namespace-scoped ACLs; quota/pools and namespaced KV deferred) |
| [read-model-coherence-plan.md](plans/read-model-coherence-plan.md) | in execution (all five phases built; raft shadow check deferred) |
| [service-mesh-plan.md](plans/service-mesh-plan.md) | **authoritative for the mesh**; shipped M1–M10 (policy, HTTP mode, name-is-config, gated, ingress, handlers, mTLS, graph, sandbox vsock, templates) with two named deferrals — unifies the SD §8 follow-on with the handlers plan |
| [network-handlers-plan.md](plans/network-handlers-plan.md) | shipped as phases of service-mesh-plan (policy objects, `gated` mode, handlers); the `legion netd` process split is the deferred piece |
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
| [aws-comparative-benchmark-plan.md](plans/aws-comparative-benchmark-plan.md) | in execution (phases 0-2 run on real AWS; phases 3-5 blocked on the scheduler, not the rig — see [aws-phase2-findings.md](aws-phase2-findings.md)) |
| [worker-simulation-plan.md](plans/worker-simulation-plan.md) | shipped (2026-07-30) |
| [launch-spot-check-fixes-plan.md](plans/launch-spot-check-fixes-plan.md) | in execution (every 2026-10-11 spot-check finding fixed in #17, open; P1-5 in part; deferrals in §8.7; AWS re-run pending) |
| [worker-plane-auth-plan.md](plans/worker-plane-auth-plan.md) | in execution: Phases 0 to 8 built; hardware verification open (launch blocker: workers on other hosts lose exec, logs, and the mesh. Also owns local workload isolation: ACLs on by default, the host guard, gated containers by default, the container privilege floor, and the `host` grant for host-level jobs. Carries the sandbox-exec plan's ticket caller class) |
| [scheduler-diagnosis-tooling-plan.md](plans/scheduler-diagnosis-tooling-plan.md) | planned (`legion scheduler diagnose` + the metric gaps that blocked the 2026-08-04 diagnosis) |
| [scheduler-plan-snapshot-plan.md](plans/scheduler-plan-snapshot-plan.md) | shipped |
| [scheduling-index-plan.md](plans/scheduling-index-plan.md) | shipped (phases 0–3; group-path + backlog deferred) |
| [service-discovery-final-plan.md](plans/service-discovery-final-plan.md) | v1 shipped; §8 mesh follow-on superseded by service-mesh-plan.md |
| [service-discovery-plan.md](plans/service-discovery-plan.md) | superseded (rationale + full mesh design) |
| [service-discovery-mesh-and-config.md](plans/service-discovery-mesh-and-config.md) | superseded (cookbook + mesh extensions) |
| [service-discovery-critique.md](plans/service-discovery-critique.md) | superseded (self-critique) |
| [stateful-workloads-and-instance-identity-plan.md](plans/stateful-workloads-and-instance-identity-plan.md) | shipped |
| [task-runtime-rename-plan.md](plans/task-runtime-rename-plan.md) | shipped |
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
| [sandbox-scaling-plan.md](plans/sandbox-scaling-plan.md) | in execution: Phases 0–6 shipped (measurement, group commit and write trimming, `StreamWork` delivery, blocked evals, untombstoned minted ids, archived idle keyed instances, and the `session` instance mode with running sessions as node-owned soft state and promotion); session suspend, identity, and the session log deferred; Phase 7's wake on request is built under wake-on-request-plan (keyed and archived instances); Phases 8+ planned (snapshot-clone density, a sub-VM seam, cells past a measured trigger) |
| [session-durability-and-routing-plan.md](plans/session-durability-and-routing-plan.md) | in execution, Phase 5a (sessions sleeping on their node) built (builds on session mode: concurrent starts on each node, sessions that survive a worker restart, the start node in the session id, edge starts picked by a load balancer outside Legion with no manager on the path, and sessions that sleep on their node; a batched raft session directory deferred behind a trigger) |
| [wake-on-request-plan.md](plans/wake-on-request-plan.md) | in execution: Phase 1 shipped (`suspend.wake`, `legion start gateway` that resumes a suspended keyed instance on a request and holds it until the instance serves, `CONNECT` for any declared port, a wake webhook, wake fields on `GetInstance`); gateway metrics, waking sessions, and node-local wake on connect planned |
| [sandbox-exec-plan.md](plans/sandbox-exec-plan.md) | in execution: Phases 1 to 3 built and Phase 0 measured on one host; KVM and rig verification open (exec into Linux microVMs over the guest agent's vsock, and direct exec with worker-verified tickets as a third caller class on the worker-plane-auth listener; implemented together with worker-plane-auth, which it amends in §9) |
| [suspend-resume-ui-plan.md](plans/suspend-resume-ui-plan.md) | shipped (UI-1 to UI-6, with the §7.3 server reads: ListNodeUsage, GetNodeContent, last_stop, checkpoint_support) |
| [session-ui-plan.md](plans/session-ui-plan.md) | shipped (UI only: sessions and envelopes on the job, instance, allocation, node, and task pages, Promote and End, and bounded fetching, polling, and rendering for very large session counts; `ListInstances` paging, counts, and node/envelope filters deferred to the API) |
| [shared-alloc-dir-proposal.md](plans/shared-alloc-dir-proposal.md) | proposal |
| [data-dir-env-plan.md](plans/data-dir-env-plan.md) | shipped (`LEGION_DATA_DIR`; no config-file field, by design) |

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
- [auth-acl-plan_calls.md](calls/auth-acl-plan_calls.md)
- [aws-comparative-benchmark-plan_calls.md](calls/aws-comparative-benchmark-plan_calls.md)
- [bounded-candidate-scoring-plan_calls.md](calls/bounded-candidate-scoring-plan_calls.md)
- [container-runtime-exec-and-signals-plan_calls.md](calls/container-runtime-exec-and-signals-plan_calls.md)
- [container-task-kind-macos_calls.md](calls/container-task-kind-macos_calls.md)
- [data-dir-env-plan_calls.md](calls/data-dir-env-plan_calls.md)
- [edge-boot-plan_calls.md](calls/edge-boot-plan_calls.md)
- [egress-credential-injection-plan_calls.md](calls/egress-credential-injection-plan_calls.md)
- [exec-and-actions-plan_calls.md](calls/exec-and-actions-plan_calls.md)
- [gated-sandbox-checkpoints-plan_calls.md](calls/gated-sandbox-checkpoints-plan_calls.md)
- [sandbox-exec-plan_calls.md](calls/sandbox-exec-plan_calls.md)
- [sandbox-scaling-plan_calls.md](calls/sandbox-scaling-plan_calls.md)
- [wake-on-request-plan_calls.md](calls/wake-on-request-plan_calls.md)
- [session-durability-and-routing-plan_calls.md](calls/session-durability-and-routing-plan_calls.md)
- [gang-scheduling-plan_calls.md](calls/gang-scheduling-plan_calls.md)
- [heartbeat-cadence-at-scale-plan_calls.md](calls/heartbeat-cadence-at-scale-plan_calls.md)
- [heartbeat-scale-plan_calls.md](calls/heartbeat-scale-plan_calls.md)
- [host-convergence-plan_calls.md](calls/host-convergence-plan_calls.md)
- [host-process-supervision-plan_calls.md](calls/host-process-supervision-plan_calls.md)
- [job-status-plan_calls.md](calls/job-status-plan_calls.md)
- [job-templates-plan_calls.md](calls/job-templates-plan_calls.md)
- [jsonb-text-affinity-plan_calls.md](calls/jsonb-text-affinity-plan_calls.md)
- [job-updated-at-plan_calls.md](calls/job-updated-at-plan_calls.md)
- [read-model-coherence-plan_calls.md](calls/read-model-coherence-plan_calls.md)
- [operator-ui-gaps-plan_calls.md](calls/operator-ui-gaps-plan_calls.md)
- [ui-holistic-polish-plan_calls.md](calls/ui-holistic-polish-plan_calls.md)
- [ui-task-and-template-redesign-plan_calls.md](calls/ui-task-and-template-redesign-plan_calls.md)
- [checkpoint-encryption-plan_calls.md](calls/checkpoint-encryption-plan_calls.md)
- [kv-encryption-plan_calls.md](calls/kv-encryption-plan_calls.md)
- [kv-reactive-task-updates-plan_calls.md](calls/kv-reactive-task-updates-plan_calls.md)
- [kv-cert-delivery-plan_calls.md](calls/kv-cert-delivery-plan_calls.md)
- [launch-spot-check-fixes-plan_calls.md](calls/launch-spot-check-fixes-plan_calls.md)
- [liveness-off-the-raft-path-plan_calls.md](calls/liveness-off-the-raft-path-plan_calls.md)
- [manager-contention-instrumentation_calls.md](calls/manager-contention-instrumentation_calls.md)
- [manager-reverse-proxy-plan_calls.md](calls/manager-reverse-proxy-plan_calls.md)
- [manager-workload-identity-plan_calls.md](calls/manager-workload-identity-plan_calls.md)
- [multi-cluster-federation-plan_calls.md](calls/multi-cluster-federation-plan_calls.md)
- [multi-raft-persistence-plan_calls.md](calls/multi-raft-persistence-plan_calls.md)
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
- [volume-plugin-system-plan_calls.md](calls/volume-plugin-system-plan_calls.md)
- [worker-plane-auth-plan_calls.md](calls/worker-plane-auth-plan_calls.md)
- [worker-simulation-plan_calls.md](calls/worker-simulation-plan_calls.md)
- [workload-identity-oidc-plan_calls.md](calls/workload-identity-oidc-plan_calls.md)
