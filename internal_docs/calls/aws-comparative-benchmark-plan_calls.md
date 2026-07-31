# Judgement calls: aws-comparative-benchmark-plan

Plan: [`../plans/aws-comparative-benchmark-plan.md`](../plans/aws-comparative-benchmark-plan.md)

Entries 1-4 are planning-stage calls; 5-10 were made while building the
rig (phases 0-1, 2026-07-30). Ranked by project impact; the number is
the rank.

## 1. Simulated workers everywhere, real control planes only

- **The call:** 3 real manager/server instances per system; all workers
  are simulated fleets packed onto cheap spot instances (Legion:
  a new `--simulate N` worker mode, phase 0; Nomad: nomad-nodesim; K8s:
  KWOK).
- **Alternatives:** real workers at smaller scale; kubemark hollow nodes
  for K8s.
- **Trade-off:** nothing data-plane is measured (explicit non-goal), and
  simulated heartbeat/status traffic must be argued representative. KWOK
  over kubemark trades fidelity of kubelet internals for 10x density and
  is what SIG-Scheduling itself uses.
- **Why:** the claim under test is scheduler + control-plane throughput;
  worker realism buys nothing for it and costs 10-100x the budget. Using
  each ecosystem's own standard simulator is also the fairness argument.

## 2. Project to 50k from measured 1k-10k curves, validate once at 20k

- **The call:** scaling claims above 10k nodes are model projections from
  fitted per-phase curves, published with prediction intervals and one
  20k-node spot-burst validation.
- **Alternatives:** buy the 50k run; cap claims at 10k.
- **Trade-off:** projections can be attacked; the defense is the
  validation burst plus publishing the raw curves and methodology.
- **Why:** a 50k simulated-node campaign is 5x the instance count for
  every system and scale step — most of the cost for one row of the
  table. The phase instrumentation exists precisely so projection is
  per-phase (defensible) rather than end-to-end (hand-waving).

## 3. Same 3 boxes reused serially per system, not three parallel clusters

- **The call:** one control-plane trio, torn down and re-provisioned per
  system; campaigns run serially.
- **Alternatives:** parallel clusters (faster wall-clock).
- **Trade-off:** longer campaign wall-clock; spot-market drift between
  campaigns (mitigated: on-demand for control planes, instance types
  pinned, node_exporter baselines recorded).
- **Why:** identical hardware is the whole credibility argument, and
  serial reuse makes "identical" literal instead of statistical.

## 4. Legion measured in both raft-lanes and streaming modes

- **The call:** two Legion rows (raft3+lanes, streaming+S3 lease), one
  row each for Nomad and K8s.
- **Alternatives:** one "best" Legion configuration.
- **Trade-off:** doubles Legion campaign time; risks the streaming row
  looking worse than the raft row in our own publication (it measures
  different guarantees).
- **Why:** both are shipped, recommended deployment modes; publishing
  only the faster one would be the kind of benchmark selectivity the
  fairness record exists to prevent.

## 5. Every latency timestamp is driver-side receive time

**Call.** The driver takes the submit mark and the placed mark on its
own monotonic clock, for every system: "placed" is when the placement
event arrives on the driver's watch stream (`legion events follow -j`,
Nomad's event stream, a kubectl pod watch), not the event's own
manager-side `created_at`.

**Alternatives.** Manager-stamped event times (no stream-delivery
latency in the number) with chrony-disciplined clocks across boxes.

**Trade-off.** Every latency includes stream delivery (~ms on a LAN),
so absolute numbers are a hair pessimistic for all three systems
equally. Accepted: a fixed common cost beats a variable cross-host skew
that would need its own error analysis to defend.

**Why (honest measurement).** One clock takes both marks, so no
cross-host skew can enter a latency, and no system gets a
differently-shaped correction. The methodology fits in one sentence in
the report, which is what a fairness argument has to survive.

## 6. `allocation.placed` events are the unit of counting, weighted by their `count`

**Call.** Legion emits one `allocation.placed` per plan apply with
`metadata.count` allocations in it (`src/storage/events.rs:233`); the
driver counts that weight and emits `count` identical latency samples
per event. Nomad/K8s adapters count per-allocation/per-pod sightings —
their watch streams are already per-object.

**Alternatives.** Per-allocation counting for Legion too, by expanding
`metadata.placements` — but that list caps at 20 entries by design, so
it cannot carry the count at batch sizes the campaign uses.

**Trade-off.** All allocations in one Legion plan apply share one
latency sample value, slightly quantizing Legion's percentile tails
(toward the plan boundary, not in Legion's favor). Recorded in the
fairness notes; revisit if per-allocation event granularity ever ships.

**Why (correctness within the wire we have).** Counting what the event
stream actually says beats inferring per-allocation times that were
never measured. The fixture suite (`driver/tests/`) pins this math.

## 7. Terraform is system-agnostic; per-system installs are SSH scripts

**Call.** `terraform/` builds only boxes, network, and bucket; Legion
A/B, Nomad, and K8s are installed and wiped by `scripts/install-*.sh` +
`wipe-control-plane.sh` over SSH onto the *same never-recreated
instances*.

**Alternatives.** Per-system Terraform stacks or AMIs — more
declarative, and what a long-lived environment would want.

**Trade-off.** Install scripts are imperative and must be idempotent by
discipline; a partially failed install is fixed by re-running, not by
`terraform apply`. Config drift between campaigns is handled by the
wipe script rather than immutable images.

**Why (the methodology is the claim).** "Identical hardware" is literal
only if the boxes never change identity between campaigns; recreating
instances per system (what per-system stacks naturally do) would reduce
it to "statistically identical". Judgement call 3 decided serial reuse;
this is its implementation consequence.

## 8. The dry-run gate is a process-based local smoke, not the planned docker-compose stand-in

**Call.** The plan's testing section named "a local docker-compose
stand-in (1 manager + sim workers)". Shipped instead:
`scripts/local-smoke.sh` runs the manager and the `--simulate` fleet as
plain local processes, plus pure-fixture unit tests for the counting
math (`driver/tests/`), wired into CI on `benchmarks/aws/**` changes.

**Alternatives.** The docker-compose stand-in as written.

**Trade-off.** The smoke doesn't exercise a containerized network
namespace; nothing the driver measures crosses one locally anyway.

**Why (simplicity).** The repo has no local-cluster compose file to
build on (dev tooling uses Apple's `container` CLI), and the
`--simulate` flag made real-wire fleets a plain process — compose would
have added a Docker dependency to reach the same loop. The event-shape
assumptions compose would have covered are pinned by the wire-real
integration test (`tests/worker_simulation.rs`) instead.

## 9. Batch workloads translate to K8s Deployments, not Jobs

**Call.** Every scenario job type becomes a Deployment with
`replicas = count` on KWOK nodes (`workloads/translate.py`), including
batch-burst shapes; Nomad gets `type = "service"` jobs symmetrically.

**Alternatives.** K8s Jobs / Nomad batch jobs for the batch shapes —
higher per-system fidelity.

**Trade-off.** K8s Job-controller throughput (completions, backoff) is
not measured; the batch-burst scenario measures pure "place N pods now"
pressure on all three schedulers instead. Stated in the rig README and
the fairness record.

**Why (apples to apples).** The claim under test is placement
throughput. "Keep N running" is the one semantic all three systems
express identically; per-system batch semantics would make the
batch-burst row compare controllers, not schedulers.

## 10. One bucket, one IAM role for the whole rig

**Call.** A single S3 bucket carries binaries, campaign archives, and
the Legion B streaming replica + lease, and one instance role with rw
on that bucket serves every box (`terraform/iam.tf`, commented as
such).

**Alternatives.** Split buckets (results vs replica) and per-role IAM
(control plane rw-replica, workers read-binaries, loadgen rw-results).

**Trade-off.** A compromised worker instance could read campaign
archives or the replica. Accepted for a disposable, single-tenant,
admin-CIDR-locked rig that `teardown.sh` deletes wholesale; the
downgrade is called out in iam.tf for whoever copies this into
something longer-lived.

**Why (ease).** Three buckets and three roles triple the Terraform and
the failure modes of every install script for a rig whose entire threat
model is "we delete it after the campaign".

