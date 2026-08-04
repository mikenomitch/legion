# AWS Comparative Benchmark: Legion vs Nomad vs Kubernetes

**Status: IN EXECUTION** — phase 2 is blocked on the scheduler, not on
the rig. The second campaign (2026-08-04,
[findings](../aws-phase2-findings.md)) ran Legion A across 1k/2k/5k
nodes and found that **74% of plan applies are rejected and re-planned**,
with throughput *falling* as eval threads are added. No comparative
number should be published from this state: it would measure a defect
that is being fixed, and the Nomad/K8s campaigns would be compared
against Legion's worst configuration. Phases 3-5 wait for
[plan-apply-conflict-reduction-plan.md](plan-apply-conflict-reduction-plan.md).
The rig itself is now sound — every failure of that campaign was a rig
bug, all fixed, recorded in calls entries 21-25.

Phase 0 shipped
([worker-simulation-plan.md](worker-simulation-plan.md), 2026-07-30).
**Phase 1 is complete** — its exit criterion passed on real AWS
2026-08-02/03 ([findings](../aws-phase1-findings.md)), though that
night's numbers measured the heartbeat ceiling rather than the scheduler
and must not be quoted. **Phase 2 ran 2026-08-04** and is the source of
the blocker above. Phases 3-5 (Nomad, K8s, analysis) not started; their
install scripts and driver adapters remain TO-VERIFY until they run.

Packing density is **measured at 4,000 nodes/instance** (99% cadence, 1%
CPU, 3% memory on a `c7i.2xlarge`), retiring the plan's 1,250 estimate
and the contaminated 125 from phase 1. Every scale step therefore needs
far fewer spot instances than the topology table below assumes: 10k
nodes is 3 instances, and the 20k validation burst is 5 — comfortably
inside both the ASG's 20-instance cap and the 256 vCPU spot quota, so the
quota increase phase 1 called for is **not needed**.

Campaigns can be run unattended since 2026-08-02:
`scripts/remote-campaign.sh` drives them from the loadgen box so a run
does not depend on the operator's machine staying awake or holding one
IP, and the spot fleet is released whenever a campaign exits. Also
TO-VERIFY until phase 1 runs on a real account. See calls entries 11-13.

> **Judgement calls:** [`../calls/aws-comparative-benchmark-plan_calls.md`](../calls/aws-comparative-benchmark-plan_calls.md)

## Problem

We want to say "Legion is the fastest scheduler at scale" and have it be
true, tested, and defensible. Local benchmarks
([`scheduler-performance-plan.md`](scheduler-performance-plan.md)) prove
the scheduler core; they cannot prove the claim, because the published
competitor numbers (Nomad C1M, kube-scheduler ~300 pods/sec) are
full-system: real network, real quorum, real API load, thousands of
kubelets/clients. A credible claim needs all three systems measured by
*us*, on the *same* hardware, with the *same* workloads — at
control-plane scale we can afford.

The budget reality: manager/server instances must be real, but thousands
of workers must be simulated (packed many-per-instance), and 10k+-node
behavior must be *projected* from measured scaling curves rather than
bought.

## Prior art

- **Nomad C1M/C2M**: 5 servers, real clients (C1M) then
  [nomad-nodesim](https://github.com/hashicorp-forge/nomad-nodesim) for
  simulated clients (C2M); measured job-registration→placement rates.
  [nomad-bench](https://www.hashicorp.com/en/blog/nomad-bench-load-testing-and-benchmarking-for-nomad)
  is their reusable rig (Terraform + simulated node fleets).
- **Kubernetes**: [KWOK](https://kwok.sigs.k8s.io/) (fake kubelets, the
  modern standard for scheduler perf; used by SIG-Scheduling),
  kubemark/hollow-nodes (heavier, older). `scheduler_perf` publishes
  ~256-300 pods/sec at 5k nodes.
- **Us**: the scenario schema + phase instrumentation from the scheduler
  performance plan are designed to be reused here — same workload shapes,
  same `legion_scheduler_plan_phase_seconds` breakdown, scraped by
  Prometheus instead of printed by a test harness.

## Jobs to be done

- **Maintainer**: one command (`terraform apply` + a runbook script) that
  stands up a cluster, drives a workload, collects metrics, and tears
  everything down before it costs real money.
- **The claim**: apples-to-apples placement throughput and latency for
  Legion / Nomad / K8s on identical hardware, plus honest projections to
  10k-50k nodes with the methodology published.
- **Operator-reader**: the numbers for the deployment they'd run
  (3-manager raft, streaming persistence), not a lab special.
- **Roadmap**: real-environment answers to the questions local benches
  can't ask (D8 heartbeat pressure, failover under load, streaming RPO —
  see the architecture review plan).

## Goals

- Measure all three systems on identical AWS topology: 3 control-plane
  instances + a packed simulated-worker fleet.
- Legion measured in both HA modes: 3-node raft (lanes topology) and
  streaming persistence (S3 replica + object-storage lease).
- Scale steps 1k / 2k / 5k / 10k simulated nodes; fit per-phase scaling
  curves; project to 50k with stated confidence and a single validation
  burst at 20k.
- Piggyback the expensive environment: failover drills, 24h soak,
  streaming-persistence RPO measurement, dashboard templates.
- Everything reproducible from a `benchmarks/aws/` directory (Terraform,
  configs, run scripts, analysis notebooks) — the methodology is part of
  the claim.

## Non-goals

- Data-plane performance (container start times, runtime overhead) — this
  is a *scheduler and control-plane* benchmark; all workloads are
  noop/pause-equivalent.
- A marketing page. This plan produces numbers + a methodology writeup;
  what gets published and how is a separate decision.
- Multi-region topologies, federation.

## Prerequisite: Legion worker simulation (gap)

Legion has no fake-worker mode today (checked: nothing in `src/worker/`).
Packing real workers works (the worker is one binary + noop runtime) but
each process costs a tokio runtime + gRPC channel + fingerprint loop;
hundreds per instance is realistic, thousands per instance is not, and
nodesim/KWOK give the others exactly that.

**Phase 0 ships `legion start worker --simulate N`** (or a dedicated
`legion nodesim` subcommand — decide in its own mini-plan): one process
multiplexing N registered nodes over shared gRPC channels — heartbeats,
fingerprints from a profile file (reusing the benchmark `node_profiles`
schema), noop task acks, no real task execution. This is also the fairness
argument: all three systems then simulate workers the same way (many fake
nodes per cheap instance).

## Topology

| Role | Count | Type | Notes |
|------|-------|------|-------|
| Control plane | 3 | `m7i.xlarge` (4 vCPU/16 GiB) | on-demand; same 3 boxes reused for each system, serially |
| Load/metrics | 1 | `m7i.large` | Prometheus + Grafana + workload driver; never colocated with control plane |
| Simulated workers | 4-12 | `c7i.2xlarge` spot | ~1,250 sim nodes per instance at 10k-node step |
| Object storage | — | S3 bucket | streaming replica + lease; results archive |

Per-system control plane, same boxes:

- **Legion A**: raft, 3 voters, lanes topology, `dist` profile binary.
- **Legion B**: streaming persistence (S3 replica, object-storage lease),
  1 primary + 2 standbys.
- **Nomad**: 3 servers (raft), nomad-nodesim fleet, current release,
  default-ish tuned config (documented).
- **K8s**: kubeadm 3-server stacked etcd, KWOK-managed fake nodes,
  current release. (KWOK stages tuned so pods go Running without kubelets.)

Cost envelope (rough, us-east-1, 2026 pricing): 3× m7i.xlarge ≈ $0.60/hr
+ 12× c7i.2xlarge spot ≈ $1.2-1.6/hr + m7i.large ≈ $0.10/hr → **≈ $2.50/hr
for the largest step; a full three-system campaign at all scale steps is
tens of dollars, the 24h Legion soak ~$20**. Spot interruption of worker
instances is tolerable (they're fake nodes; the driver re-registers).

## Workloads (shared across systems)

Ported from `benchmarks/scheduler/scenarios/*.json` by a small translator
in `benchmarks/aws/workloads/` (scenario JSON → Legion jobs / Nomad jobs /
K8s Deployments with nodeSelector):

1. **steady-service**: web-fleet shape — services with resource requests +
   class/region constraints, rolling re-submits.
2. **batch-burst**: batch-burst shape — bursts of high-fanout batch jobs,
   measure queue drain rate.
3. **mixed + churn**: mixed-heterogeneous shape with node churn (kill/add
   sim nodes at a fixed rate) — the realistic steady state.
4. **storm**: mass reschedule — kill 10% of nodes at once at the 5k step;
   measures recovery throughput (this is where schedulers actually differ).

Each at node steps 1k/2k/5k/10k, allocation counts scaled 5-50k, with a
warm-up period discarded.

## Metrics (identical collection for all three)

- **Placement throughput**: sustained allocations placed/sec (driver
  counts transitions to placed/running-equivalent state).
- **Placement latency**: submit→placed p50/p95/p99 (driver timestamps).
- **Recovery**: storm scenario time-to-100%-replaced.
- **Control-plane memory vs load**: peak and mean bytes per role at every
  scale step, so memory is comparable across low/medium/high load the
  same way throughput is. The scale steps (1k/2k/5k/10k) are the load
  axis — no separate experiment is needed, because every cell is already
  archived at a known node count. `collect.sh` records two series per
  cell and reduces them into `memory.json` next to `metrics.json`:
  - `used` = `MemTotal - MemAvailable`. What the box needs provisioned.
  - `anon` = used minus page cache and buffers. The closer proxy for the
    manager's own footprint. The split matters: on the idle rig the
    control plane measures 1.63 GB used against 0.92 GB anon, so page
    cache is ~40% of `used`, and a SQLite-backed store only grows that
    share. Reporting `used` alone would overstate resident cost.

  **Known limitation:** this is whole-box memory from node_exporter, not
  the manager process's RSS. Legion registers no process collector, so
  `process_resident_memory_bytes` is not exposed on `:4749`. The
  control-plane boxes run nothing else of consequence, so box memory
  tracks the manager closely, but these numbers are an upper bound and
  must be reported as such. Adding a process collector is the real fix
  and is deliberately *not* done mid-campaign: it changes the binary
  under test and so invalidates the fairness record for every cell
  already collected.
- **Control-plane cost**: CPU per manager (node_exporter), and for
  Legion the per-phase histogram (`legion_scheduler_plan_phase_seconds`)
  — the phase curves are what make projection defensible rather than
  hand-wavy. K8s: `scheduler_framework_extension_point_duration_seconds`
  etc.; Nomad: `nomad.worker.*` / plan-queue telemetry — nearest-equivalent
  phase families, mapped in the analysis notebook.
- **Fairness record**: exact versions, configs, instance types, tuning
  applied, in the results archive. Rule: default configs plus documented,
  defensible tuning only (e.g. K8s `percentageOfNodesToScore` left at
  default; QPS limits raised to not bottleneck the API client — recorded).

## Projection methodology

Fit each system's placement-latency and throughput curves over the four
measured steps (per-phase for Legion, aggregate for others); fit
log-linear and linear models, take the better fit per phase; project to
20k and 50k with prediction intervals; **validate** with one 20k-node spot
burst (16 more worker instances for ~2 hours). Projections published only
alongside the validation delta. If a curve is superlinear, the report says
so — that honesty is what makes the fast numbers believable (and feeds
architecture-review items D1/D4).

## Piggyback experiments (same rig, marginal cost ≈ 0)

- **Failover under load**: kill the Legion raft leader / Nomad leader /
  K8s active scheduler mid-steady-state; measure placement gap seconds.
  Legion B variant: kill the streaming primary, measure lease-failover
  gap + RPO (frames not yet shipped).
- **24h soak** (Legion only): mixed+churn at 2k nodes; watch for drift,
  leaks, eval-queue growth, SQLite file growth/vacuum behavior.
- **Upgrade under load** (Legion only): rolling manager upgrade
  mid-workload per `docs/upgrades.md`, measure disruption.
- **Dashboards**: the Grafana boards built here ship in-repo as
  `benchmarks/aws/dashboards/` and become the recommended operator boards
  (docs/observability.md follow-up).

## Phases

0. **Worker simulation** (`--simulate N`, own mini-plan; gates everything).
1. **Rig**: `benchmarks/aws/` Terraform (VPC, instances, S3, Prometheus,
   teardown), driver skeleton, Legion A single-step smoke (1k nodes).
   Exit: one command up, one command down, metrics landing.
2. **Legion campaign**: A + B, all workloads, all steps; phase-curve
   analysis; feed findings back into the optimization loop and the
   architecture review (D8 gets its data here).
3. **Nomad campaign** (nodesim), same workloads/steps.
4. **K8s campaign** (KWOK), same workloads/steps.
5. **Analysis + report**: curves, projections, 20k validation, fairness
   record, writeup in `benchmarks/aws/REPORT.md`; public docs get a
   methodology page only after review.

Each phase leaves a working rig; 2-4 are independently re-runnable as the
scheduler improves (re-run Legion-only campaigns cheaply after each
optimization lands).

## Testing plan

The rig itself gets a smoke path: a `--dry-run` driver mode against a
local docker-compose stand-in (1 manager + sim workers) asserting the
driver's counting/latency math against known-good fixtures, so a broken
driver never burns cluster-hours. Terraform is `terraform validate` +
plan-reviewed in CI (no apply).

## Rollout

Everything lands under `benchmarks/aws/` (internal tooling; not shipped in
the binary). The one product surface — `--simulate N` — gets its own plan,
docs page, and the standard agent-readiness treatment when Phase 0 starts.
Results are recorded like all benchmark numbers
(`benchmarks/scheduler/README.md` links the campaign archives), and no
public claim ships before the Phase 5 fairness record exists.
