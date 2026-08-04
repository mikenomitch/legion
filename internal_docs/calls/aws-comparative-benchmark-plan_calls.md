# Judgement calls: aws-comparative-benchmark-plan

Plan: [`../plans/aws-comparative-benchmark-plan.md`](../plans/aws-comparative-benchmark-plan.md)

Entries 1-4 are planning-stage calls; 5-10 were made while building the
rig (phases 0-1, 2026-07-30); 11-13 came from making campaigns runnable
unattended (2026-08-02); 18-19 came from making the rig startable at all
from an arm64 macOS machine (2026-08-02); 20 came from adding memory to
the measured results during the first real campaign (2026-08-02). Ranked
by project impact; the number is the rank.

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

**Amended (entry 13).** Still one bucket, but now two roles: the loadgen
box takes an `orchestrator` role that adds `SetDesiredCapacity` on the
sim-fleet ASG. Bucket access is unchanged and shared.

## 11. Results are mirrored to a bucket terraform does not manage

**Call.** `var.archive_bucket` names an existing, externally-created S3
bucket. `collect.sh` mirrors every cell into it as the cell lands, and
`teardown.sh` re-syncs anything that missed before destroying anything.
The rig's IAM role gets `GetObject`/`PutObject` on it and deliberately
not `DeleteObject`. Unset is allowed, and teardown says loudly what that
costs.

**Alternatives.** (a) Keep the single bucket of entry 10 and rely on
teardown's pre-destroy sync to the operator's machine — the shipped
behavior until now. (b) Have terraform create a second bucket with
`prevent_destroy`. (c) Drop `force_destroy` from the rig bucket.

**Trade-off.** A prerequisite that terraform cannot create for you: one
`aws s3 mb` before the first campaign, or the warning fires on every
teardown. It also splits "where are the results" into two answers.

**Why (the campaign is the product).** (a) fails precisely when it
matters: entry 12 moved the driver's seat onto the loadgen box so a run
survives the operator's laptop, which means the machine running teardown
can itself be disposable — syncing results *to* it is syncing them to
the thing you are about to lose. (b) and (c) both break `teardown.sh`:
terraform refuses to destroy a `prevent_destroy` resource and refuses to
delete a non-empty bucket without `force_destroy`, so the one command
standing between an idle rig and a real bill starts failing — a rig you
cannot cheaply destroy is a rig that keeps billing. A bucket outside the
state file sidesteps the conflict entirely: terraform cannot destroy
what it does not own. Withholding delete follows from the same premise
as the rig being disposable — nothing inside a short-lived,
internet-facing environment should be able to erase the durable copy.

## 12. Campaigns are driven from the loadgen box, not the operator's machine

**Call.** `scripts/remote-campaign.sh` stages the rig tree, a topology
manifest, and the ssh key onto the loadgen box, then starts the campaign
there under `tmux`. Every script picks its address family from
`BENCH_ON_RIG`: public IPs when driven from outside, private IPs when
driven from the rig.

**Alternatives.** (a) Keep driving from the operator's machine and widen
`admin_cidr` to `0.0.0.0/0` for SSH. (b) Replace SSH ingress with SSM
Session Manager so there is no admin CIDR at all. (c) Run the driver from
a CI runner with a static egress IP.

**Trade-off.** Two execution modes to keep working, and the address-family
switch is a real trap for anyone adding a script (use `CP_ADDRS` /
`LOADGEN_ADDR`, never `*_PUBLIC_IP`, for anything you connect to). (b) is
cleaner and removes the admin CIDR entirely, but it rewrites every `rsh`
call site and adds VPC endpoints or an SSM agent dependency to a rig that
is otherwise plain SSH. (a) leaves port 22 open to the internet and does
nothing about the real problem below.

**Why (ease).** A campaign is hours of unbroken SSH from one address, and
`admin_cidr` pins ingress to one CIDR. Driving from a laptop or an
ephemeral cloud runner means the run dies when the machine sleeps, the
session drops, or the egress IP rotates — and the rig keeps billing until
someone notices. Moving the driver's seat inside the VPC removes all
three failure modes at once, and it costs nothing extra because the
loadgen box is already running and already inside the security group.

The private-IP rule on-rig is not a preference: same-VPC traffic
addressed to another instance's *public* IP hairpins out through the
internet gateway and arrives with the sender's public IP as its source,
so the security group's intra-rig `self` rule does not match it.

## 13. Scale steps move through the ASG API, not `terraform apply`

**Call.** `scale_workers` calls `autoscaling:SetDesiredCapacity`
(`scripts/env.sh`), the ASG carries
`lifecycle { ignore_changes = [desired_capacity] }`, and the loadgen box
gets an `orchestrator` role holding exactly that permission plus two
describes. `var.worker_instances` becomes the *initial* size only.

**Alternatives.** Put terraform state in an S3 backend and run
`terraform apply -var worker_instances=N` from the loadgen box, as the
operator's machine did.

**Trade-off.** Desired capacity now drifts from what terraform's state
records, which is why the lifecycle rule exists — without it an unrelated
apply mid-campaign would drag the fleet back and corrupt a scale step.
Anyone reading `var.worker_instances` expecting it to describe a running
fleet will be wrong.

**Why (simplicity).** The alternative hands an internet-facing instance
the IAM to rebuild or destroy every resource in the rig, plus a state
bucket to bootstrap and lock, all to turn one dial. Two ASG calls need
one narrowly-scoped permission and no state at all. It also keeps
`teardown.sh` the only thing that can destroy the rig, and keeps it on
the operator's machine where the confirmation prompt is.

## 14. The spot fleet is released on any campaign exit

**Call.** `run-campaign.sh` traps `EXIT` and scales the ASG to zero on
success, failure, or a killed session. `--keep-fleet` opts out. The
control plane is deliberately left running.

**Alternatives.** Leave the fleet up (the previous behavior); tear the
whole rig down on exit; a separate watchdog with a deadline.

**Trade-off.** A failed run loses the sim fleet's state, so a post-mortem
that needs live workers has to opt out with `--keep-fleet` before the
fact. Accepted: the driver's own output and the Prometheus snapshots are
what post-mortems actually read.

**Why (ease).** The point of unattended runs is not having to watch them,
and the spot fleet is the expensive half of the rig — up to 20 instances.
A campaign that dies at 03:00 should not still be billing at 09:00. The
control plane stays up because it holds the state a post-mortem needs and
is comparatively cheap, and because destroying the rig is a decision that
belongs to a human running `teardown.sh`.


## 15. Campaign state is published to S3, not only answerable over SSH

**Call.** The unattended runner publishes `status.json` (phase, step,
detail, timestamp) and a tail of its console log to
`s3://<bucket>/status/`, on every transition and on a 60s heartbeat.
`scripts/campaign-status.sh show|watch` reads it with nothing but a
bucket name, a region, and credentials, and returns the state as its
exit code: 0 running, 2 finished, 3 failed, 4 stale, 5 missing.

```bash
scripts/campaign-status.sh show --bucket <bucket> --region us-east-1
```

**Alternatives.** (a) `remote-campaign.sh status`, which entry 12
already provides, over SSH. (b) Push notifications (SNS/webhook) on
transitions. (c) Expose the campaign log over HTTP from the loadgen box.

**Trade-off.** State is polled, not pushed, and lags by up to the
heartbeat interval. Two places now report status, and they can disagree
if publishing fails (it is best-effort by design — status must never
fail a campaign).

**Why (ease, for the thing actually watching).** (a) assumes the watcher
can SSH from inside `admin_cidr` to a box that still exists. The most
likely watcher of an overnight run is an agent or cloud shell elsewhere,
and such environments routinely have HTTPS and no outbound port 22 at
all — this session was written from one. (c) has the same problem plus a
new listener and a security-group hole. (b) is genuinely better for
"tell me when it breaks" but needs an endpoint to deliver to, which a
transient watcher does not have; polling a bucket needs no inbound
anything. The heartbeat exists because the interesting failure is not a
crash — a crash sets `failed` — but a box that vanishes mid-step, which
without a liveness stamp is indistinguishable from a scale step being
quiet for ten minutes. Exit codes rather than parsed output so a watcher
can branch without a JSON parser.

## 16. Spot and on-demand are split by a knob, not a quota ticket

**Call.** `var.worker_on_demand_percentage` (default 0, all spot) sets
the share of the sim fleet launched on-demand, through the ASG's
existing `mixed_instances_policy`. `collect.sh` records it per cell.

**Alternatives.** (a) All spot, and wait on an AWS quota increase.
(b) All on-demand for simplicity. (c) Spread the fleet across regions,
which have independent quotas.

**Trade-off.** The on-demand share costs roughly 3x, and a fleet split
across purchase types is not perfectly homogeneous — part of it cannot
be spot-reclaimed. That is a fidelity difference and not only a price
one, which is why it lands in the fairness record rather than staying a
deployment detail.

**Why (ease).** AWS meters spot and on-demand vCPU as separate quotas,
so a 50/50 split needs neither limit to cover the whole fleet: the 20k
burst is 128 vCPU, which is 64 from each bucket instead of 128 from one.
A quota increase is still the right long-run answer and the runbook says
to file one — but it has unpredictable lead time, and this knob turns
"blocked until AWS answers" into "one line, a few dollars." (b) throws
away the cost model the whole campaign budget rests on. (c) was rejected
outright for measured cells: cross-region heartbeat RTT inflates
sim-side latency and delays ack visibility, corrupting the submit->placed
number the campaign exists to produce.

## 17. Nodes-per-instance is measured before it is trusted

**Call.** `experiments/packing-density.sh` climbs an ascending ladder of
densities on a single instance and reports the highest that stays
healthy — fully registered, no failures, >=95% of heartbeat cadence,
CPU and memory under 85%. The result feeds
`run-campaign.sh --nodes-per-instance <n>`; the 1,250 default is
explicitly labelled as the plan's estimate.

**Alternatives.** (a) Keep trusting 1,250. (b) Derive the ceiling
analytically from per-node cost (two async tasks, a few KB). (c) Let
each campaign auto-tune density as it goes.

**Trade-off.** Twenty minutes of rig time, and a number that is only
valid for this instance type and this build of the sim path — it has to
be re-run when either changes.

**Why (performance, honestly measured).** 1,250 sizes every scale step,
so being wrong is expensive in both directions and invisible in both.
Too low and every step over-provisions instances, cost, and quota. Too
high and the sim host saturates: `start-sim-fleet.sh` divides the
requested total by however many instances actually came up, so an
under-provisioned fleet does not fail — it silently packs more nodes per
box, and the campaign measures its own simulator. The cadence ratio is
the load-bearing check precisely because it catches that case, and the
`legion_worker_*` families the fleet now exports are what make it
observable at all. (b) cannot see the real constraints — the gRPC
channel pool caps at 16 channels of 256 nodes, so past ~4k nodes per
process the multiplexing itself becomes the variable. (c) would make
density a per-cell variable, which is exactly the kind of thing a
fairness record cannot explain away.

## 18. A disposable operator box, outside the rig's terraform

**The call.** `scripts/bootstrap-operator-box.sh` builds a dedicated
Ubuntu x86_64 EC2 instance to drive campaigns from, using plain AWS CLI
calls and its own state file — not a terraform module, and not part of
the rig.

```bash
benchmarks/aws/scripts/bootstrap-operator-box.sh launch \
    --access-key-id AKIA... --secret-access-key ...
```

**Alternatives.** (a) Drive campaigns from the operator's laptop, as the
runbook originally assumed. (b) Add the box to `terraform/` as a fourth
instance. (c) Document a manual setup checklist instead of scripting it.

**Trade-off.** A second, separately-managed lifecycle: `teardown.sh`
destroys the rig, `bootstrap-operator-box.sh destroy` destroys the box,
and forgetting the second leaves an instance billing. Mitigated by
`destroy` refusing quietly — it warns when rig instances are still up,
because the box holds the only copy of their terraform state.

Not having terraform also means not having its state file, so `create`
writes its own *before* the first API call and appends after each one.
Recording only on success — the shorter version — orphans a security
group, an IAM role, and a billing Elastic IP whenever `create` dies in
the middle, with nothing left that knows they exist. `destroy` therefore
treats every recorded field as optional and cleans up exactly what was
made. It also only deletes the EC2 key pair when this script is the
thing that imported it; a pre-existing key pair of the same name belongs
to something else.

**Why (ease).** (a) does not actually work from this project's primary
development machine: `build-and-upload.sh` produces the dist binary and
plugin dylibs for the rig's Linux x86_64 instances, which an arm64 macOS
laptop cannot cross-compile with the plugin build as it stands. It also
fights `admin_cidr` — every rig ingress rule pins to one CIDR, and a
laptop's egress IP rotates, so the box gets an Elastic IP and that CIDR
stays true for the campaign. (b) is circular: this box runs the
terraform, so it cannot be created by it, and it must outlive
`teardown.sh`, which destroys everything terraform manages including the
state. (c) is what the previous session left behind, and the setup is
eight tools deep — protoc, a Rust toolchain, terraform, AWS CLI v2 — with
a Rust build in the middle. Scripting it is the difference between
starting a campaign and spending an evening on prerequisites.

## 19. The operator box carries an IAM role, never a copied key

**The call.** Credentials passed to `bootstrap-operator-box.sh` are used
only for the local AWS calls that build the box. The instance gets an
IAM instance profile — `PowerUserAccess` plus an inline IAM policy
narrowed to `legion-bench*` role and instance-profile names — and no
long-lived key is ever written to it.

**Alternatives.** (a) Copy the operator's access key onto the box, which
is the obvious way to make a remote box "just work". (b) Hand-write a
least-privilege policy for exactly the rig's resource set. (c) Grant
`AdministratorAccess`.

**Trade-off.** `PowerUserAccess` is broader than this rig strictly needs,
so the box could touch unrelated resources in the account. The IAM half
— the part that mints roles — is the piece narrowed by name prefix,
because that is where the privilege-escalation path is.

**Why (simplicity, and the blast radius that matters).** The box is
internet-facing, sits inside a trust domain that already holds the rig's
SSH private key, and is deliberately left running unattended overnight.
A static access key on it is a credential that outlives the box and the
campaign; an instance role's credentials are rotated by AWS and die with
the instance, which is exactly the lifetime wanted here. (b) is the
right answer for a permanent account and the wrong trade for a
disposable benchmark rig: the rig creates VPCs, subnets, route tables,
security groups, instances, launch templates, an ASG, an S3 bucket, and
two IAM roles, and a hand-narrowed policy for that set is a maintenance
burden that breaks every time `terraform/` grows a resource. (c) gives
away the one thing worth withholding — unrestricted `iam:CreateRole` on
an internet-facing box is a full account takeover from a single SSH
compromise, which the name-prefix condition prevents at no usability
cost.

## 20. Memory is measured whole-box, not as manager process RSS

**The call.** Memory vs load is reported from node_exporter's whole-box
counters, split into `used` (`MemTotal - MemAvailable`) and `anon` (used
minus page cache and buffers), peak and mean per role, reduced into a
`memory.json` beside every cell's `metrics.json`. The scale steps are the
load axis, so no separate low/medium/high experiment exists.

**Alternatives.** (a) Add a process collector to Legion so
`process_resident_memory_bytes` is exposed on `:4749` and measure the
manager directly. (b) Run `node_exporter`'s `processes` collector or a
`namedprocess` exporter on the control plane. (c) Report only `used`, the
single number most systems quote.

**Trade-off.** Whole-box memory is an upper bound: it includes the OS,
and `used` includes reclaimable page cache. On the idle rig the control
plane measures 1.63 GB used against 0.92 GB anon, so page cache is ~40%
of `used` before Legion is even installed — a gap that widens as a
SQLite-backed store warms. Every published memory figure therefore has to
carry the caveat rather than read as "the manager uses this much".

**Why (honesty over precision, and not moving the thing being measured).**
(a) is the right answer and should happen — but not mid-campaign. It
changes the binary under test, which invalidates the fairness record for
every cell already collected, and the fairness record is the argument
that makes the comparison credible at all. A slightly loose number that
every system is measured with identically beats a precise number that
only some cells have. (b) needs a second exporter deployed and version-
pinned on the control plane, which is more moving parts in the fairness
record for the same upper-bound answer, since the boxes are single-tenant
anyway. (c) is what would have been reported by default and is the actual
trap: `used` alone would have overstated resident cost by ~40% while
looking like a clean measurement, and nothing in the output would have
said so.

## 21. Cells are sized by allocation target, not by a per-scenario multiplier

**The call.** `run-campaign.sh` computes `TARGET = nodes x ALLOCS_PER_NODE`
(default 5, so the steps are 5k/10k/25k/50k — the plan's stated band) and
`translate.py --target-allocations` solves for each scenario's multiplier.
The old `--count-multiplier nodes/1000` is still there and still works.

**Alternatives.** (a) Keep the multiplier and raise it. (b) Raise the
scenarios' own base counts. (c) Leave it and shorten the warmup instead.

**Trade-off.** A target is one more indirection between "what the plan
asks for" and "what the scenario emits", and it silently reshapes a
scenario whose job types are weighted differently from the others.

**Why (honest measurement).** The multiplier scaled each scenario from its
*own* base, and those bases — 180, 304 and 380 allocations — were chosen
for the local micro-benchmarks on a few dozen nodes. At the 1k step the
campaign was asking a 3-manager raft cluster to place 180 allocations. It
did so in 0.8 seconds, entirely inside the 30-second warmup, so the cell
reported `throughput_per_sec: 0.0`, `latency_samples: 0` and
`complete: true` — a cell that looks successful and measures nothing. (a)
leaves the three scenarios landing at different sizes for the same step,
which the fairness record cannot explain. (b) edits fixtures the local
benchmarks depend on. (c) treats the symptom: a workload that drains in
under a second has no steady state to find, whatever the warmup.

## 22. The driver submits concurrently, because serially it measured itself

**The call.** `driver.py --submit-concurrency` (default 4) submits through
a thread pool. `1` restores the old serial loop.

**Alternatives.** (a) Stay serial and accept the ceiling. (b) Ask Legion
for a batch-submission RPC. (c) Run several driver processes.

**Trade-off.** Concurrency puts the driver's own scheduling between the
submit timestamp and the wire, so a saturated pool would inflate measured
latency. Mitigated by stamping `record_submit` inside the worker thread
immediately before the call, and by keeping the default low.

**Why (the measurement was of the wrong thing).** Serially, each submit is
a fresh CLI process plus a Nickel evaluation plus an RPC — 25ms, so ~40
jobs/sec. Measured on a clean 1000-node cluster, the last placement landed
**0.0 seconds after the last submit**, and the run's total span equalled
the submit span exactly: Legion placed every job before the next one
arrived. The cell measured Python's subprocess loop. With concurrency 4 a
backlog finally forms (4.9s from last submit to last placement) and the
number describes the scheduler. The same loop feeds the Nomad and K8s
adapters, so lifting it lifts all three equally and the comparison stays
fair. (b) is the better long-term answer for real users and is now a TODO,
but it changes the product to fix the harness. (c) multiplies clock skew
across processes, which entry 5 exists to avoid.

**Sized to the box, not to a round number.** The default is 4, not 16,
because the loadgen box is an `m7i.large` — 2 vCPU. Each slot is a whole
CLI process; oversubscribing starves the event-watcher thread and the cell
measures the driver's own contention.

## 23. Each cell tears down its jobs, from an EXIT trap

**The call.** `run-step.sh` stops then destroys every job after
`collect.sh`, via `trap cleanup_jobs EXIT`.

**Alternatives.** (a) Leave jobs standing. (b) Wipe and reinstall the
control plane between cells. (c) Clean up inline after collect.

**Trade-off.** Teardown costs rig time proportional to job count, and on a
failed cell the trap fires before `collect.sh` has run, so a failure
archives nothing.

**Why (the fitted variable must be the only variable).** Cells run back to
back — three scenarios per step, four steps — and nothing else removed
what a cell submitted, so the 10k step would have been measured against a
cluster still carrying everything the 1k, 2k and 5k steps left running.
Throughput would decay across the campaign for reasons unrelated to node
count, which is precisely the axis being fitted. (b) is a guarantee rather
than a best effort, but it also clears the node table and forces a full
fleet re-registration, changing what the next cell measures. (c) is what
was tried first and is subtly wrong: the driver exits non-zero whenever a
cell is incomplete and `pipefail` aborts the script, so an inline teardown
ran only after *successful* cells — leaving the wreckage of exactly the
cells most likely to have left some.

**The failure it produced is worth recording, because it was unreadable.**
With teardown skipped, the next cell resubmitted identical specs for jobs
that already existed. Legion correctly treated that as a no-op, created no
allocations, and emitted no placement events — so the cell reported zero
placements and looked for all the world like a broken event stream. Two
runs were spent blaming the watcher.

## 24. Cadence evidence is archived per cell, over the cell's own window

**The call.** `collect.sh` snapshots `legion_heartbeat_freshness_ratio`,
`legion_liveness_tracked_nodes` and
`legion_liveness_loss_suppressed_total{reason}` with every cell, scoped to
`CELL_START_EPOCH` rather than a trailing 2h, and records the window and
its scope in `fairness-record.json`.

**Alternatives.** (a) Trust the cell and check cadence only when a number
looks wrong. (b) Keep the trailing 2h window already used for every other
series.

**Trade-off.** Three more series per cell, and a window that no longer
overlaps its neighbours cannot show a slow drift across a campaign.

**Why (phase 1 failed exactly here, invisibly).** The first campaign's
cell reported 0.9 placements/sec at a p50 of 109s, which measured the
heartbeat ceiling and not the scheduler — and *nothing in the archive said
so*. The freshness ratio is the one number that separates the two, and a
cell that cannot be certified cannot be quoted. (b) defeats the purpose:
collected right after a 4000-node packing-density run, a 1000-node cell
archived `liveness_tracked_nodes` peaking at 4000 and a freshness ratio
dipping to 0.009, neither of which happened during the cell. Evidence that
cannot be attributed to a cell cannot certify it.

## 25. Heartbeat cadence stays at the default; adaptive cadence is not measured

**The call.** The rig's `manager-a.ncl.tpl` sets no `heartbeat` block, so
`targetRatePerSecond` stays unset, nothing adapts, and every worker holds
a fixed 2s interval at every scale step.

**Alternatives.** (a) Set `targetRatePerSecond` so cadence backs off as the
fleet grows. (b) Measure both, doubling the Legion cell count.

**Trade-off.** At the 10k step a fixed 2s cadence is 5,000 heartbeats/sec
into three managers. If that exceeds what the control plane sustains, the
step degrades — and the campaign will have declined to use the feature
built to prevent it.

**Why (fairness, and a measurable denominator).** The campaign's rule is
default configs plus documented, defensible tuning, and Nomad and K8s will
run their defaults too; `targetRatePerSecond` is a Legion-only knob with
no counterpart in either. It would also serve a *varying* interval, which
silently invalidates `packing-density.sh`'s achieved-cadence arithmetic
(it divides by a fixed 2s) and makes "heartbeat cost at 10k nodes"
incomparable across cells. With entry 24's evidence in every cell, a
cadence shortfall at 5k or 10k becomes a recorded finding rather than a
contaminated number — which is the outcome the plan wants from a first
campaign. (a) remains the right thing to measure in a follow-up whose
stated subject is the cadence ladder itself.
