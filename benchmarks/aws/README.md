# AWS Comparative Benchmark Rig

The reproducible rig for
[`internal_docs/plans/aws-comparative-benchmark-plan.md`](../../internal_docs/plans/aws-comparative-benchmark-plan.md):
Legion (raft-lanes **and** streaming), Nomad, and Kubernetes measured on
identical AWS hardware with simulated worker fleets. The methodology is
part of the claim — everything a campaign produces is reproducible from
this directory.

**Status: Legion paths proven on AWS (phases 1-5, 2026-08); Nomad and
K8s paths rebuilt 2026-10-09 for the cross-system campaign** (Nomad
2.0.5 + nodesim, K8s 1.36.5 + KWOK 0.8.0). The campaign matrix is in the
plan's "Cross-system campaign" section.
The unattended path ("Unattended runs" below) is in the same state:
written and syntax/unit-checked, unexercised against real AWS. So is
`bootstrap-operator-box.sh` — its AWS calls and phase chain have been
run end-to-end against stubbed `aws`/`ssh` binaries and nothing more.
Treat the first `launch` as a test of the bootstrap as much as of the
rig. `bake-operator-ami.sh` is in that same state: every path exercised
against a stubbed `aws`, no image baked against real AWS yet.

## Layout

| Path | What |
| --- | --- |
| `terraform/` | The rig: VPC, 3× on-demand control plane, loadgen box, spot sim-worker ASG, S3 bucket, IAM. System-agnostic — per-system installs are scripts, so the same boxes serve every campaign serially. |
| `configs/` | Rendered-per-campaign config templates: Legion A (raft lanes), Legion B (streaming + S3 lease), Nomad servers, kubeadm + KWOK, Prometheus + Grafana stack. |
| `workloads/` | `translate.py`: scenario JSON → Legion jobspecs / Nomad jobs / K8s Deployments, plus the driver's expectation manifest. `profile_split.py`: the shared fleet-shape split. |
| `driver/` | `driver.py`: submits, watches placements, computes throughput/latency/recovery. `tests/`: the fixture suite that keeps the counting math honest. |
| `../loadgen/` | `legion-loadgen`: the native Legion job submitter (`cargo build --features loadgen`). Legion-only on purpose — read the submission-path rule under Methodology before comparing a cell built with it to another system. |
| `scripts/` | The runbook, as executables (below). `bootstrap-operator-box.sh` builds the box you drive from (`bake-operator-ami.sh` bakes its toolchain into an AMI first, from the one shared `operator-toolchain.sh`); `phase1.sh` is the whole of phase 1, checkpointed. |
| `experiments/` | Piggyback drills: storm, failover, streaming RPO, 24h soak, upgrade-under-load. See `experiments/README.md`. |
| `dashboards/` | Grafana boards provisioned onto the loadgen box. |
| `analysis/` | `fit_curves.py`: scaling fits + 20k/50k projections with prediction intervals and validation deltas. `sandbox_report.py`: the sandbox-scaling campaign's report and projections. |
| `results/` | Campaign archives land here (gitignored) and in `s3://<bucket>/campaigns/`. |

## Start here: one command

You need an AWS key and nothing else. This builds the box that drives the
campaign, configures the rig against it, and runs phase 1 remotely:

```bash
benchmarks/aws/scripts/bootstrap-operator-box.sh launch \
    --access-key-id AKIA... --secret-access-key ...     # or --profile <name>

benchmarks/aws/scripts/bootstrap-operator-box.sh logs -f
```

The credentials build the box and are never copied onto it — it carries
an IAM instance role instead. What it runs, checkpointed so a failure
resumes rather than restarts, is `scripts/phase1.sh`: the six steps of
the manual runbook below, ending at phase 1's exit criterion.

### Bake the operator AMI first (one command, once per region)

The box installs its whole toolchain — build-essential, protoc, clang,
terraform, AWS CLI v2, rustup — at first boot. Measured on this rig,
`cloud-init analyze blame` put `modules-final/config-scripts_user` at
**3245 seconds (54 minutes)** on one launch and ~14 minutes on another:
same instance type, same region, same script. It is nearly all
`apt-get`, and it is paid before a single line of Legion compiles.

Pay it once, into an AMI:

```bash
benchmarks/aws/scripts/bake-operator-ami.sh --region us-east-1   # ~20 min
benchmarks/aws/scripts/bake-operator-ami.sh --show               # what is baked
```

`launch` then finds that image by tag and first boot is seconds instead
of an hour. Cost is a c7i.large for the length of the bake plus ~$0.05/mo
for the snapshot.

**Nothing requires it.** With no baked AMI, `create` says so — naming the
bake command — and installs the toolchain in user-data exactly as it
always did. The slow path is a supported path, not a broken one;
`--slow-bootstrap` takes it deliberately.

**One list, two paths.** Both the bake and the fallback run
`scripts/operator-toolchain.sh`, the single copy of what gets installed.
The image is tagged with that file's hash, so when the toolchain changes,
`create` reports the baked image as stale and falls back rather than
launching a box missing the tool that was just added. Re-bake when it
says so, and otherwise every few months so a new box is not `apt-get
upgrade`-ing through a season of security updates. AMIs are regional —
bake in the region the campaign runs in.

**Why a separate box rather than your laptop.** `build-and-upload.sh`
compiles the dist binary and plugin dylibs *for the rig's Linux x86_64
instances*, so an arm64 macOS machine cannot produce the artifact at all.
And `admin_cidr` pins every rig ingress rule to one CIDR: the operator
box gets an Elastic IP, so that CIDR stays true for the whole campaign
where a laptop's egress IP would not. Terraform state lives on the box
too, which is why it must outlive `teardown.sh` — it is deliberately not
managed by the rig's terraform.

**Stop the box between sessions.** Idle it is ~$0.37/hr running and
~$0.016/hr stopped (~$268/mo vs ~$12/mo), and unlike the spot fleet —
which `run-campaign.sh` releases on any exit — nothing releases this one
for you. It holds an Elastic IP rather than an auto-assigned address, so
the IP, and therefore `admin_cidr`, survives the stop:

```bash
scripts/bootstrap-operator-box.sh stop
scripts/bootstrap-operator-box.sh start
```

Two teardowns, in this order:

```bash
scripts/bootstrap-operator-box.sh ssh 'cd ~/legion/benchmarks/aws && scripts/teardown.sh --yes'
scripts/bootstrap-operator-box.sh destroy
```

`teardown.sh` asks before destroying anything. With no terminal to ask on
(a command over `ssh`, `nohup`, `< /dev/null`) it refuses with exit 2 and
says so, rather than guessing, so an unattended teardown needs `--yes`.

`destroy` warns if rig instances are still up, because the box holds the
only copy of their terraform state.

## The runbook

What `phase1.sh` automates, if you would rather drive it yourself. Run
these **on the operator box** (`bootstrap-operator-box.sh ssh`), in
`~/legion/benchmarks/aws`:

```bash
cd benchmarks/aws

# 0. Prove the loop locally — no AWS, no cost. Run this after any
#    driver/translator change; CI runs the fixture tests on PRs.
scripts/local-smoke.sh

# 1. Configure + stand up the rig (~$0.75/hr idle-ish, see plan).
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # edit
scripts/bringup.sh
scripts/build-and-upload.sh          # dist binary + plugins -> S3
                                     # (plugins are fingerprinted and only
                                     #  rebuilt when they change; force with
                                     #  --force-plugins)

# 2. Legion campaign, both HA modes. One command each: scales the spot
#    fleet per step, starts sim fleets, drives every workload, collects,
#    archives. Start with --smoke (1k nodes, one workload).
#    For anything long, launch it on the loadgen box instead — see
#    "Unattended runs" below.
scripts/install-legion-a.sh
scripts/run-campaign.sh --system legion-a --smoke
scripts/run-campaign.sh --system legion-a
scripts/install-legion-b.sh
scripts/run-campaign.sh --system legion-b

# 3-4. Competitor campaigns (same boxes, serially — phase 3/4).
#      LEGION_SUBMITTER=cli on the Legion campaigns these are compared
#      against — see the submission-path rule under Methodology. Without
#      it, Legion submits natively and Nomad/K8s spawn a process per job,
#      and the table compares submitters instead of schedulers.
scripts/install-nomad.sh  && scripts/run-campaign.sh --system nomad
scripts/install-k8s.sh    && scripts/run-campaign.sh --system k8s

# 5. Piggyback drills (marginal cost ≈ 0 on the standing rig).
experiments/failover-drill.sh --system legion-a
experiments/streaming-rpo.sh
experiments/soak-24h.sh --hours 24
experiments/upgrade-under-load.sh

# 6. Analysis: fits, projections, validation deltas.
python3 analysis/fit_curves.py --campaign results/<campaign-id> --json report.json

# 7. Tear it all down before it costs real money (syncs archives first).
scripts/teardown.sh
```

The 20k validation burst is one extra flag pass:
`scripts/run-campaign.sh --system legion-a --steps "20000"` (the ASG
tops out at 20 instances; `fit_curves.py` automatically treats a 20k
cell as validation, not as fit input).

### Saturating the scheduler (the other axis)

Node count is one axis; allocations per node is the other, and it is the
one that decides whether the scheduler is being asked for real work. A
saturation sweep runs several densities at one node count, on one fleet,
one cell per point:

```bash
scripts/run-campaign.sh --system legion-a --steps "5000" \
    --allocs-per-node-sweep "5 10 20 40 80"
```

Each point lands in its own archive (`…-5000n-a25000`, `…-a50000`, …),
and `fit_curves.py` prints the curve — including the points where the
cluster stopped converging, which is the answer the sweep is looking for.
The points deliberately share one fleet: restarting it between them puts
a node-loss storm in the middle of the curve.

`--allocs-per-node N` (default 5) sets the density for an ordinary
scaling campaign, which is what puts the four steps at 5k/10k/25k/50k
allocations.

### Eval concurrency (`--eval-threads`)

The third axis, and the one phase 2 found pointing the wrong way: more
eval threads *cost* throughput (4 → 16 gave -12% at 1k nodes and -11% at
5k) because the extra planners collided and their plans were discarded.
Re-measuring that is how plan-apply conflict reduction gets judged, so
the manager's `evalThreads` is rendered per campaign rather than
hardcoded in `configs/legion/manager-a.ncl.tpl`:

```bash
scripts/run-campaign.sh --system legion-a --steps "1000 5000" --eval-threads 16
```

It is a control-plane setting, not a per-cell one, so it is one value per
campaign — a sweep is one campaign per point, and each campaign's
rendered config (archived in the fairness record) states the value it
actually ran. `legion-a` only; passing it to another system is an error
rather than a silent no-op, because a cell archived as a 16-thread result
whose config says 4 is worse than a failed run.

### Heartbeat cadence (adaptive, the default)

Run scale steps with Legion's default heartbeat cadence, which is adaptive.
`configs/legion/manager-a.ncl.tpl` sets no `heartbeat` block on purpose, so
the manager sizes the interval from fleet size at the default
`targetRatePerSecond` of 2,500/s (headroom 2):

| Fleet | Served interval | Lost window | Steady heartbeats/s |
| ---: | ---: | ---: | ---: |
| 1,000 | 2s | 10s | 500 |
| 2,500 | 2s | 10s | 1,250 |
| 5,000 | 5s | 25s | 1,000 |
| 10,000 | 8s | 40s | 1,250 |
| 20,000 | 21s | 105s | ~950 |
| 50,000 | 55s | 275s | ~910 |

The old fixed 2s cadence put ~10,000 heartbeat RPCs/s on the leader at 20k
nodes, more than a core of a 4-vCPU box, and that load slowed scheduling
enough to contaminate every large cell. Nomad adapts by default too (its
`max_heartbeats_per_second` defaults to 50), so the comparison is
default-against-default. Do not pin Legion to a fixed cadence for a
comparative run unless the subject of the run is the cadence itself.

What the rule costs is detection time, so every cell records it.
`collect.sh` archives `heartbeat_served_interval` and
`heartbeat_lost_window` next to `heartbeat_freshness`. Quote a throughput
number with its served interval, and treat a freshness ratio below 0.95 as
a cell that measured the heartbeat path rather than the scheduler.
`packing-density.sh` judges cadence against the served interval, not
against a fixed 2s. See
[docs/heartbeat-cadence-and-scale.md](../../docs/heartbeat-cadence-and-scale.md).

### Every campaign resets the control plane first

`run-campaign.sh` wipes and reinstalls the control plane before its first
step (`scripts/reset-control-plane.sh`, `--no-reset-control-plane` to opt
out). This is load-bearing, not hygiene: **Legion cannot deregister a
node**, so `stop-sim-fleet.sh` leaves every node record behind and the
node table becomes a high-water mark of every fleet the boxes have ever
carried. A 1,000-node cell run after a 4,000-node experiment otherwise
measures a scheduler holding 3,000 corpses while reporting itself as a
1,000-node result. Every cell now also gates on an exact node census —
right count, all active and available, all named from this fleet's
slices — and refuses to run otherwise, printing what it actually saw.

## Sandbox-boot rig (real KVM)

[`sandbox-boot/`](sandbox-boot/README.md) is a separate, smaller rig for
session sandboxes on real KVM: spot c8i workers with nested virtualization
running cloud-hypervisor microVMs, `legion-loadgen --mode sessions`, and a
host-side timeline of every guest. Its last campaign is in
[`sandbox-boot/RESULTS.md`](sandbox-boot/RESULTS.md).

## Sandbox-scaling campaign (cheap, standalone)

`scripts/run-sandbox-campaign.sh` measures the sandbox path: keyed-instance
lifecycles (create → running → suspend → resume → destroy) driven at a
ladder of open-loop rates against the real 3-manager control plane, plus one
density cell that holds thousands of sandboxes resident at once. It is
separate from the job campaigns, needs no spot fleet, and is the cheapest
campaign the rig runs
([sandbox-scaling plan](../../internal_docs/plans/sandbox-scaling-plan.md),
Phase 0 on AWS).

```bash
# The cheap shape: 3 control-plane boxes + one load box, no worker fleet.
scripts/bringup.sh -var worker_instances=0 -var loadgen_type=c7i.2xlarge
scripts/build-and-upload.sh

scripts/run-sandbox-campaign.sh --smoke     # ~5 min: one 25/s rung + 2,000 resident
scripts/run-sandbox-campaign.sh             # ladder 25 → 800/s, then 10,000 resident

scripts/teardown.sh
```

**What it costs.** Three `m6id.xlarge` ($0.237/h each) and one
`c7i.2xlarge` ($0.357/h) on demand in us-east-1 are about **$1.07/hour**.
Bringup and the build take ~20 minutes, the default campaign ~30-45 (it
stops climbing at the first rung that does not hold, and resets the control
plane once before the density cell). A full run, bringup to teardown, is
about **$1-2**. `--smoke` proves the path for well under a dollar.

**How it works.** `legion-loadgen --mode instances` runs on the load box
and embeds its simulated nodes (`--simulate`, checkpoint channel faked,
shaped by `workloads/sandbox_nodes.json`: a 48-vCPU, 192 GiB host holding
~3,000 sandboxes). The nodes speak the real worker protocol over the VPC:
registration, the `StreamWork` stream, heartbeats, checkpoint reports. So
the control plane under test, its raft quorum, and the network between
them are real; only the sandbox runtime is not.

| Cell | What runs | Default |
| --- | --- | --- |
| `ladder-r<rate>` | `rate × 20` sessions (200-20,000) at `<rate>`/s on `--sim-nodes` nodes, each running create → running → suspend → resume → destroy | rates 25, 50, 100, 200, 400, 800; 100 nodes |
| `density-<n>` | `<n>` sessions created at `--density-rate` and held running for `--density-hold` seconds on `--density-nodes` nodes (~1,000 per node: the dense-node case the heartbeat path is about) | 10,000 on 10 nodes, 250/s, 120 s |

Every cell records the loadgen summary and before/after snapshots of every
manager (CPU seconds and RSS of the `legion` process, applied raft
entries, heartbeats served); the density cell also samples them every 10 s
while the sandboxes are held. Flags: `--rates`, `--sim-nodes`,
`--sessions-max`, `--density*`, `--no-stop` (climb past the ceiling),
`--no-reset-control-plane`, `--system legion-b` (streaming persistence).

**The report.** The campaign ends with `analysis/sandbox_report.py`, which
writes `results/<campaign>/sandbox/report.md` (and `report.json`):

- per rung: offered and achieved lifecycles/s, failures, latency p50/p99
  per stage (create → running, placement, dispatch, suspend, resume,
  destroy), **raft entries per lifecycle**, and **manager CPU-seconds per
  lifecycle**;
- the **ceiling** (highest rung that held: every session completed at
  ≥ 90% of the offered rate) and the **operating point** (the held rung
  nearest 80% of it, whose latencies are the ones to quote);
- the density cell: peak resident sandboxes, **manager memory per resident
  sandbox**, and manager CPU and heartbeats/s while they were held;
- **projections**, each labelled with the measured number it scales and the
  assumption it makes: lifecycles per hour and per day at the operating
  point, **control-plane cost per million lifecycles**, and resident
  sandboxes a manager's memory holds.

`python3 analysis/sandbox_report.py --campaign results/<id>` re-renders it;
`--cp-hourly-usd` sets the price for a control-plane type the built-in
table does not know.

**What the numbers can and cannot claim.** Every latency is the control
plane's share of a sandbox start (from accepting `CreateInstance` to the
node holding the allocation, and the same for resume), measured on an HA
3-voter cluster across real network hops. Add the runtime's own boot or
restore time for the end-to-end figure: this rig has no KVM, so it cannot
boot a guest. Sustained rates and per-lifecycle costs are measured;
per-hour, per-day, per-million-lifecycle and resident-capacity figures are
projections, and the report prints them as such. Suspend and resume
latencies are bounded by the heartbeat cadence (checkpoint requests and
reports still ride heartbeats), so they say more about that protocol than
about throughput.

## Bottleneck measurements without a big fleet

Planned, not yet run (written 2026-10-12). This section lists the places
where Legion could run out of headroom as sandbox counts grow, and shows how
to measure each one without hundreds of real workers. Each entry names what
to measure, the tool, the exact command, and the signal or pass bar. Where
no harness exists yet, the entry says **TODO (gap)**. All the gaps are
collected under [Gaps](#gaps-harnesses-that-do-not-exist-yet).

Three cheap setups cover almost everything:

- **Laptop or one Linux box.** Use `cargo bench` or an ignored measurement
  test. These are in-process and need no network.
- **Local cluster plus a simulated fleet.** Run one manager
  (`cargo run --release -- start manager -c configs/manager.ncl`) and
  `legion start worker $TOKEN --simulate N --sim-checkpoints` (see
  [worker-simulation.md](../../docs/worker-simulation.md)). The control
  plane is real; the nodes are fake.
- **One real KVM worker.** For runtime costs (cold start, restore, memory per
  sandbox), use a spot-check worker (`tests/spot-check/`, an m7i.large with
  nested virtualization) or any Linux box with `/dev/kvm`. Bare metal gives
  the numbers to quote. Nested virtualization gives a lower bound.

To scrape the manager's metrics, read `http://<manager>:4749/metrics`
(`server.port + 2`). Workers export metrics only when
`LEGION_WORKER_TELEMETRY_PORT` is set ([metrics.md](../../docs/metrics.md)).
Take a before and after scrape around each run and record the deltas, not
the absolute values.

| # | Bottleneck | Tool | Fleet needed |
| --- | --- | --- | --- |
| 1 | Manager eval and plan | `scheduler_bench`, `eval_critical_path_bench`, `plan_contention` | none |
| 2 | Raft commit and group commit | `raft_perf_smoke`, `raft_lifecycle_ops`, `heartbeat-commit-probe.sh` | none, or the 3-manager rig |
| 3 | StreamWork delivery | `work_stream` measurement | none |
| 4 | Heartbeat load | `heartbeat_scale_benchmark`, `--simulate` | none, or one sim host |
| 5 | ListInstances and session listing | `instance_scale_measure`, the density cell; session listing is a TODO | none, or one sim host |
| 6 | Session starts per node (launch pool) | `session_start_latency_measure`, `session_start_cost_breakdown_measure`; a real-node probe is a TODO | none, or one worker |
| 7 | sessions.db group commit | TODO | one worker |
| 8 | Session ticket verification cache | TODO | one worker |
| 9 | Manager routing by node tag | a timed `instance get` loop, tagged vs untagged | two workers |
| 10 | Checkpoint write and restore | `instance suspend/start --wait -j` on a real worker | one KVM worker |
| 11 | Content store | the same, with `durability = "store"` | two workers |
| 12 | cloud-hypervisor cold start | a timed create loop | one KVM worker |
| 13 | Memory per idle sandbox | a fill loop plus `/proc/meminfo` | one KVM worker |
| 14 | UI at large session counts | `/testing` fixture, then a sim fleet | none |

### 1. Manager eval and plan

**Measure:** the time for one eval to produce a plan, as the node count
grows and as concurrent evals contend.
**Tool and command:**

```bash
cargo bench --bench scheduler_bench -- candidate_construction
cargo bench --bench scheduler_bench -- plan_under_fill
LEGION_BENCH_SCALING_NODES=1000,10000,50000 cargo bench --bench scheduler_bench -- plan_fleet_scaling
cargo bench --bench eval_critical_path_bench
cargo test --release --test plan_contention -- --ignored --nocapture
LEGION_BENCH_BACKENDS=local_disk,raft1,raft3 cargo test --release --test scheduler_scale_benchmark -- --ignored --nocapture
```

**Signal:** plan time grows no faster than linearly in nodes. Contention
should show rejected plans being retried, not lost. On a live manager, read
`legion_eval_phase_seconds`, `legion_scheduler_plan_phase_seconds`, and
`legion_eval_queue_depth`. A queue depth that keeps growing under steady
offered load is the failure.
**Bar:** no regression against the base branch (AGENTS.md requires before
and after numbers for scheduler changes).

### 2. Raft commit and group commit

**Measure:** the cost of a commit, how many writes the store batches into
one commit, and how many raft entries each lifecycle step takes.
**Tool and command:**

```bash
cargo test --release --test raft_perf_smoke --locked -- --ignored --nocapture
cargo test --release --test raft_lifecycle_ops -- --nocapture       # entries per keyed lifecycle stage, asserted exactly
benchmarks/aws/experiments/heartbeat-commit-probe.sh                 # on the 3-manager rig
```

**Signal:** `legion_store_coalesce_batch_size` should rise with load, while
`legion_store_coalesce_commit_seconds` stays flat; that shows group commit
working. Also watch `legion_store_coalesce_wait_seconds`,
`legion_raft_propose_apply_seconds`, `legion_raft_pipeline_wait_seconds`, and
`legion_raft_pipeline_depth`. For entries per operation, take
`legion_raft_applied_index` (summed over lanes) before and after a known
number of operations.
**Bar:** the keyed lane holds about 50 lifecycles/s at about 6 ms per entry
([sandbox-scaling-plan.md](../../internal_docs/plans/sandbox-scaling-plan.md) §5.1).
Session starts should move the applied index by far less than one entry per
start.

### 3. StreamWork delivery

**Measure:** the time from committing an allocation to the worker having it,
with a large desired set per node.
**Tool and command:**

```bash
LEGION_MEASURE_STREAM_ALLOCS=10000 cargo test --release --test work_stream -- a_streaming_heartbeat --nocapture
```

On a live cluster, read `legion_dispatch_send_seconds` with a simulated
fleet (`--simulate 200`, and again with `--sim-no-work-stream` for the
fallback path).
**Bar:** dispatch p50 ≤ network RTT + 5 ms and p99 ≤ 50 ms
([sandbox-scaling-plan.md](../../internal_docs/plans/sandbox-scaling-plan.md) §7).
A steady heartbeat with 10,000 allocations should not resend the whole set.

### 4. Heartbeat load

**Measure:** heartbeats per second one manager can serve, and how freshness
degrades past that.
**Tool and command:**

```bash
LEGION_HEARTBEAT_NODES=1000,5000,10000 LEGION_HEARTBEAT_THREADS=8 LEGION_HEARTBEAT_SECONDS=10 \
  cargo test --release --test heartbeat_scale_benchmark -- --ignored --nocapture
legion start worker $TOKEN --simulate 2000 --register-rate 200     # a live manager
```

**Signal:** `legion_heartbeat_freshness_ratio` stays near 1, and
`legion_heartbeat_served_interval_seconds` tracks the configured cadence.
`legion_heartbeat_sustainable_nodes` is the manager's own estimate of how
many nodes it can carry; also record `legion_heartbeats_total`,
`legion_grpc_in_flight`, and `legion_runtime_spawn_delay_seconds`. A rising
`legion_heartbeat_lost_window_seconds` means it has fallen behind.
**Bar:** the sustainable-nodes estimate exceeds the target fleet size with
headroom. With 1,000 allocations per node (the dense-sandbox case), the
cost of one steady beat comes from
`LEGION_MEASURE_HEARTBEAT_ALLOCS=1000,10000 cargo test --release --test instance_scale_measure -- --ignored --nocapture`.

### 5. ListInstances and session listing at large counts

**Measure:** the latency and reply size of `instance list` at 100k and 1M
instances, and the store cost behind it.
**Tool and command:** the store side (bytes per instance row, store-only
creates/s, suspended rows, and churn of unnamed instances) is measured
in-process:

```bash
LEGION_MEASURE_INSTANCES=100000,1000000 cargo test --release --test instance_scale_measure -- --ignored --nocapture
LEGION_MEASURE_SUSPENDED=50000 LEGION_MEASURE_CHURN=1000000 cargo test --release --test instance_scale_measure -- --ignored --nocapture
```

For list latency, hold instances resident on a simulated fleet with the
sandbox campaign's density cell (`scripts/run-sandbox-campaign.sh --density
100000 --density-nodes 100`, or `legion-loadgen --mode instances` against a
local manager), then time the listing while they are held:

```bash
time legion instance list <job> -j | jq '.data.instances | length'
```

**Signal:** list latency grows with the page size, not with the total
count, and manager RSS per resident instance stays flat. Record
`legion_grpc_request_duration_seconds` for the list method.
**TODO (gap):** `instance_scale_measure` does not time `ListInstances`
itself, and nothing lists *sessions* at large counts. Session listing is a
separate path: the manager asks each node for its sessions, then merges.
Simulated workers do not run sessions, so there is nothing to list. The plan
is to add a session table to `--simulate`, then time `instance list <job>
-j` at 10k, 100k, and 250k sessions across 10 to 100 sim nodes, checking
that `.data.complete` stays true and that the fan-out is one call per node
(`legion_grpc_requests_total{method}` before and after). Also watch reply
size: a full report is about 200 B per session, which reaches the 4 MiB
message limit at around 20k sessions per node.

### 6. Session starts per node (launch pool)

**Measure:** sustained starts per second on one node, and where
`edgeBoot.launchConcurrency` and `edgeBoot.queueTimeout` begin refusing.
**Tool and command:**

```bash
cargo test --release --test session_integration -- session_start_latency_measure --ignored --nocapture
```

That covers the control plane's share: pick a node, send the start, and
record it, against fake nodes on loopback. Earlier runs in a debug build
measured 379 starts/s serial and 972/s at 64 in flight.

On one real worker (spot-check rig, `11-sessions.ncl` with a larger
envelope), run a closed-loop burst:

```bash
seq 2000 | xargs -P 64 -I{} legion instance create sessions --task shell -j \
  | jq -r '.error.code // "ok"' | sort | uniq -c
```

Watch `legion_session_starts_total{outcome}` on the manager, and
`legion_worker_launch_refusals_total` on the worker (set
`LEGION_WORKER_TELEMETRY_PORT`). Repeat with `launchConcurrency` at 2, 8,
and 0 (twice the cores), and `queueTimeout` at 100ms and 1s.
**Bar:** with a launch that takes a fixed 30 ms, a node sustains at least
`launchConcurrency / 0.03` starts/s, and refusals appear only once the pool
stays full for longer than `queueTimeout`. Refusals are
`RESOURCE_EXHAUSTED` and retryable.
**TODO (gap):** no fake runtime launches with a fixed delay, so the 30 ms
bar cannot be isolated from real `exec` launch cost. `legion-loadgen` has no
session mode either. Until both exist, the real-worker burst above is the
measurement, and its launch time is whatever `exec` costs on that box.

### 7. sessions.db group commit

**Measure:** the cost of appending a session start or stop to the worker's
`<state-dir>/sessions.db`, and how many appends share one fsync.
**Tool and command:** **TODO (gap):** there is no micro-bench and no metric
for it. Until one exists, run the burst from 6 on one real worker, with and
without `sessions.db` (on a branch before session durability Phase 2), and
compare the starts/s. `strace -c -e fsync,fdatasync -p <worker pid>` during
the burst counts the syncs.
**Bar:** at the node's peak start rate, fsyncs per second stay well below
starts per second. A worker restart with 1,000 sessions re-adopts all of
them within one full-report period
([session-durability-and-routing-plan.md](../../internal_docs/plans/session-durability-and-routing-plan.md)).

### 8. Session ticket verification cache

**Measure:** the cost of verifying a session ticket on a cache hit versus a
miss. The worker caches 4,096 verified tickets.
**Tool and command:** **TODO (gap):** there is no harness and no hit or miss
metric. The rough measurement is to time 1,000 `legion session get <id>
--node <addr> --ticket <t>` calls from a manager box (RUNBOOK 12.10) using
one ticket, then using 1,000 distinct tickets. The difference is the
verification cost, plus the CLI's own startup and TLS handshake, which
dominate. A Rust bench calling the verifier
directly would give a clean number.
**Bar:** a cache hit costs microseconds. A miss costs one ES256 verify,
which is well under 1 ms.

### 9. Manager routing by node tag

**Measure:** the latency of `instance get` and `instance stop` on a session
whose id carries a node tag (the manager sends it to that node first, with
`Worker.GetSession`), compared with an untagged id, which the manager has
to look for across nodes. Ids derived from an idempotency key carry no tag,
so they take the fan-out path.
**Tool and command:** on the spot-check rig (two real workers) or a local
manager with real workers:

```bash
tagged=$(legion instance create sessions --task shell -j | jq -r '.data.instanceIds[0]')
untagged=$(legion instance create sessions --task shell --idempotency-key bench-1 -j | jq -r '.data.instanceIds[0]')
for id in $tagged $untagged; do
  t0=$(date +%s%N); for i in $(seq 200); do legion instance get $id -j >/dev/null; done
  echo "$id $(( ($(date +%s%N) - t0) / 200000000 )) ms/get"
done
```

Take `legion_grpc_requests_total{method}` before and after each loop: a
tagged get should add one worker `GetSession` call per get, and an untagged
one should add one call per node. `legion_grpc_request_duration_seconds` for
the get method gives p50 and p99.
**Signal:** routed get latency is one manager-to-worker round trip and stays
flat as nodes are added; fan-out latency is the slowest node's answer and
its call count grows with the fleet.
**TODO (gap):** two workers cannot show the fan-out's growth. Simulated
workers do not run sessions or serve `GetSession`, so the 100- and
1,000-node comparison waits for sessions in `--simulate` (see 5).

### 10. Checkpoint write and restore throughput

**Measure:** the suspend time and bytes for a memory checkpoint, and the
restore time, at guest sizes of 256 MiB, 512 MiB, 1 GiB, and 2 GiB.
**Tool and command:** on one KVM worker (the spot-check `09-sandboxes.ncl`
`ws` task, with `memory` varied):

```bash
for i in $(seq 10); do
  legion instance suspend ws-1 --wait -j | jq -c '.data | {bytes, durationMs}'
  legion instance start ws-1 --wait -j  | jq -c '.data | {restoreMs, coldBoot}'
done
```

Read `legion_instance_suspend_seconds`, `legion_instance_resume_seconds`, and
`legion_checkpoint_bytes` from the manager as a cross-check.
**Bar:** restore p50 ≤ 300 ms for a 512 MiB memory checkpoint on local
durability ([suspend-and-resume.md](../../docs/suspend-and-resume.md)).
Nested virtualization will miss that; record the miss and rerun on bare
metal before quoting it. Suspend time should scale with the memory the guest
has touched, not with its configured size.
**TODO (gap):** there is no harness for repeated suspend and resume on a
real runtime. The loop above is it, and the sim fleet fakes this channel.

### 11. Content store

**Measure:** publish time and bytes for a store-durability checkpoint, and
the fetch and restore time when the resume lands on another node.
**Tool and command:** the spot-check `10-sandbox-store.ncl` (RUNBOOK 12.6),
in a loop over cordon, start, and suspend:

```bash
legion instance suspend sbs-1 --wait -j | jq -c '.data | {bytes, durationMs, durability}'
legion instance start sbs-1 --wait --timeout 300 -j | jq -c '.data | {node, restoreMs}'
```

Read `legion_checkpoint_publish_seconds`, `legion_checkpoint_publish_bytes`,
and `legion_checkpoint_publish_total`. The fetch has no metric of its own;
`restoreMs` on a cross-node resume includes it.
**Signal:** publish MB/s (`bytes / durationMs`) against what the node gets
from a plain `aws s3 cp` of a file the same size, which is the ceiling. Also
record whether a second suspend of an unchanged guest publishes fewer bytes
than the first. There is no bar yet; the first run sets the baseline.

### 12. cloud-hypervisor cold start

**Measure:** the time from `instance create` to the first successful exec,
for a cached image and for a fresh one.
**Tool and command:** on one KVM worker:

```bash
for i in $(seq 20); do
  id=cold-$i; t0=$(date +%s.%N)
  legion instance create sandboxes --task ws --id $id >/dev/null
  until legion exec --instance $id -- true >/dev/null 2>&1; do sleep 0.1; done
  echo "$id $(echo "$(date +%s.%N) - $t0" | bc)"
done
```

Subtract the control plane's share (from `legion_provisional_*` or the
sandbox campaign's create-to-running stage) to get the runtime's share.
**Bar:** after the first boot, the image and root disk are cached and the
boot time is flat. Record p50 and p99.
**TODO (gap):** this measures through exec polling, which adds up to 100 ms.
A worker-side boot-time metric (VMM start to guest agent ready) would be
exact.

### 13. Memory per idle sandbox

**Measure:** the node memory each idle microVM really costs, compared with
its reservation.
**Tool and command:** on one KVM worker, start idle sandboxes 10 at a time
(a sleeping `13-band.ncl` VM at `mib=256`, or a session-mode `vm` task) and
record `MemAvailable` from `/proc/meminfo` and the sum of the
cloud-hypervisor processes' RSS (`ps -C cloud-hypervisor -o rss=`) after
each step. Also record `legion_node_memory_used_bytes`.
**Signal:** the slope of RSS per sandbox. A guest is backed only as it
touches memory, so an idle Alpine guest should cost far less than its
reservation. Report RSS per guest and the density this implies on a
192 GiB host.

### 14. The UI at large session counts

**Measure:** render and scroll time for the session listing.
**Tool and command:** `cd ui && npm run dev`, then open `/testing`. It
builds a 250,000-session listing in the browser
([ui-architecture.md](../../docs/ui-architecture.md)). Record the time to
first render and the scroll frame rate with the browser's performance panel.
**TODO (gap):** a live cluster with many sessions needs simulated workers
that run sessions (see 5). Until then, the fixture is the only large-count
test.
**Bar:** first render under 1 s, and scrolling stays smooth (no long tasks
over 50 ms) at 250k rows.

### Gaps: harnesses that do not exist yet

- `legion-loadgen` has no session mode, no edge-start mode
  (`Worker.StartSession` on a session ticket), and no real-runtime mode.
- Simulated workers (`--simulate`) do not run sessions or serve
  `GetSession`, so session listing, routed get versus fan-out at 100+
  nodes, and the UI cannot be tested at scale against a live manager.
- No fake runtime launches with a fixed delay, so the launch pool's bar
  (starts/s ≥ `launchConcurrency / 0.03` at 30 ms) cannot be isolated.
- No micro-bench and no metric for `sessions.db` appends and fsync batching.
- No hit or miss metric and no bench for the session ticket verification
  cache.
- No harness repeats cold start, suspend, and restore on a real runtime, and
  no worker-side boot-time metric.
- No content-store fetch metric; a cross-node resume's fetch is only visible
  inside `restoreMs`.
- No assertion on how many nodes `QuerySessions` asks; only
  `legion_grpc_requests_total{method}` shows it.
- `legion_worker_launch_refusals_total` is exported but not listed in
  `docs/metrics.md`.

### Recording results

Put each run under `results/bottlenecks/<date>/<number>-<name>/`, with the
exact command, the git SHA, the box type (and whether it was nested
virtualization), the before and after metric scrapes, and the numbers. Note
anything that missed its bar in the sandbox-scaling plan's results section,
not only here.

## Unattended runs (overnight, or from a cloud shell)

A full campaign is hours of continuous SSH from one address. Driving it
from your own machine means the run dies if the laptop sleeps, the
session drops, or your egress IP changes — and the rig keeps billing
until you notice. `admin_cidr` pins ingress to a single CIDR
(`terraform/security.tf`), so a rotating IP breaks the very next hop.

`scripts/remote-campaign.sh` moves the driver's seat onto the loadgen
box, inside the VPC. After `launch` returns, nothing on your side needs
to stay up:

```bash
scripts/remote-campaign.sh launch --system legion-a --smoke
```

```bash
scripts/remote-campaign.sh status
```

```bash
scripts/remote-campaign.sh logs -f
```

`launch` stages the rig tree to `/opt/bench/rig`, writes the topology
manifest, installs the SSH key, and starts the campaign under `tmux`.
`exec` runs any other script the same way
(`remote-campaign.sh exec experiments/soak-24h.sh --hours 24`), `attach`
joins the live tmux session, `fetch` pulls the S3 archives back, and
`stop` kills the run.

What makes this safe to leave alone:

- **The fleet is released on exit, however the campaign exits** — success,
  failure, or a killed session. The spot fleet is the expensive half of
  the rig, and an unattended run that dies at 03:00 would otherwise hold
  up to 20 instances until morning. Pass `--keep-fleet` to hold it for a
  post-mortem.
- **The control plane deliberately stays up**, because it holds the state
  a post-mortem needs. It is ~$0.75/hr, and `scripts/teardown.sh` is the
  only thing that should ever destroy it. Tear down in the morning.
- **Every cell archives to S3 as it completes**, so results survive even
  if the run dies partway.

### Watching from somewhere else

`remote-campaign.sh status` needs SSH, from an address inside
`admin_cidr`, to a box that still exists. An agent or cloud shell
checking on an overnight run has none of that — many such environments
have no outbound port 22 at all. So the campaign also publishes its own
state to S3, and anything with AWS credentials can read it:

```bash
scripts/campaign-status.sh show --bucket <bucket> --region us-east-1
scripts/campaign-status.sh watch --bucket <bucket>   # until terminal
```

No repo checkout, terraform state, rig manifest, or SSH required — the
bucket name is the only input. The exit code *is* the state, so a
watcher can branch on it without parsing: `0` running, `2` finished, `3`
failed, `4` stale, `5` no status found.

`4` is the one that matters for an unattended run. The runner heartbeats
every 60s, so "no update in 30 minutes" means the driver or its box is
gone — which a scale step's long silence would otherwise look exactly
like. The console log is mirrored alongside the status, so a watcher can
read the actual failure:

```bash
aws s3 cp s3://<bucket>/status/<campaign>/console.log -
```

### Keeping results after the rig is gone

Cells archive to the rig's own bucket as they complete, so results
already survive losing an instance. They do **not** survive
`teardown.sh`: that bucket is `force_destroy`, and teardown's local sync
writes to whatever machine ran it — for an unattended campaign, exactly
the disposable thing you might lose.

Set `archive_bucket` to a bucket terraform does not manage:

```bash
aws s3 mb s3://my-legion-benchmarks --region us-east-1
# terraform.tfvars: archive_bucket = "my-legion-benchmarks"
```

`collect.sh` then mirrors every cell there as it lands, and teardown
re-syncs anything that missed. Terraform cannot destroy a bucket it does
not own, and the rig's IAM role gets write but deliberately not delete —
so the durable copy is not erasable by anything running inside the rig.
Without it, teardown warns, and it is warning about real data loss.

Two details worth knowing:

- **Addresses switch automatically.** On the loadgen box the scripts
  reach other instances on *private* IPs. This is required, not
  cosmetic: same-VPC traffic sent to another instance's public IP
  hairpins out through the internet gateway and comes back with the
  sender's public IP as its source, so the security group's intra-rig
  rule does not match it. `scripts/env.sh` picks the right family from
  `BENCH_ON_RIG`.
- **Scale steps use the ASG API, not `terraform apply`.** The loadgen box
  has no terraform state, and desired capacity is a runtime knob rather
  than desired state. The ASG ignores `desired_capacity` drift so the two
  never fight, which also means `var.worker_instances` is the *initial*
  size only.

The loadgen box holds the rig's SSH private key at
`~/.ssh/bench_key`. That is not new with unattended runs — the drills in
`experiments/` build fault-injection commands ("stop the manager on the
victim") that the driver executes from that box, so it has always needed
to reach the rest of the rig. Treat the whole rig as one trust domain,
and tear it down when the campaign is over.

## Methodology (what makes the numbers defensible)

- **Same hardware, serially.** One control-plane trio hosts every
  system in turn; instance types pinned; `wipe-control-plane.sh`
  between campaigns.
- **Same disk.** Every system's store sits on the control plane's local
  NVMe: cloud-init bind-mounts it onto `/var/lib/legion`,
  `/var/lib/nomad`, and `/var/lib/etcd`. Before 2026-10-14 only Legion's
  was there and the competitors' raft log and WAL sat on gp3 (calls entry
  5). Rerun on NVMe, neither competitor got meaningfully faster: both are
  CPU-bound on their leader box, which `cpu_by_mode` and `disk_busy` in
  every cell now show directly.
- **The measurement does not tax the measured.** Control-plane scrapes run
  at 15s, not 5s: Nomad's leader exposes ~49,000 samples at 4,000 jobs
  and took 1.25s to encode each scrape (calls entry 12).
- **Each ecosystem's own simulator.** Legion `--simulate`
  (docs/worker-simulation.md), nomad-nodesim, KWOK — every system's
  fake nodes speak that system's real node protocol, shaped from the
  same `node_profiles` file (`workloads/profile_split.py` is the shared
  split).
- **The sim fleets are measured too.** Legion sim hosts export the
  ordinary `legion_worker_*` registry on `:9101`
  (`LEGION_WORKER_TELEMETRY_PORT`, set by `start-sim-fleet.sh`), scraped
  as the `legion-sim-workers` job and shown on the campaign board. That
  is how a run proves the workers were not the bottleneck instead of
  asserting it. The metrics are per sim *process* (~1,250 nodes), not per
  node — see docs/metrics.md § Worker. Nomad and K8s fleets are not
  scraped this way, so worker-side cost is only compared via instance CPU.
- **Every system submits through its own API, pooled.** Legion uses
  `legion-loadgen` (`benchmarks/loadgen/main.rs`): one process, a few
  long-lived gRPC connections, hundreds of concurrent `RunJob` calls.
  Nomad and K8s use the driver's `http_submit_all`: one process, one
  keep-alive HTTP connection per worker thread, `POST /v1/jobs` and
  `POST .../deployments`, at the same concurrency. This is the
  cross-system default since 2026-10-09 (calls entry 6). Before then only
  Legion had a native path and a comparison had to force all three onto
  the CLI loop, which capped every system at the loadgen box's
  process-spawn rate and made the table a tie by construction.

  `--submitter cli` (`LEGION_SUBMITTER=cli` for `run-step.sh`) still puts
  all three on the process-per-job loop, and is the control row. The
  remaining asymmetry is language: Legion's submitter is compiled and the
  others are Python threads. So **read `submission.driver_bound` before
  any competitor number**: `true` means the submitter, not the system,
  set the rate, and the cell is a floor. Every cell archives its path as
  `submitter` in `metrics.json` and `fairness-record.json`.

  **Burst cells measure throughput, and paced cells measure latency.** A
  burst cell's p99 is mostly drain time (`allocations / throughput`). For
  a latency number, run `run-campaign.sh ... --submit-rate <jobs/sec>`
  below capacity. It is Legion-only (`legion-loadgen --rate`, open loop)
  and archives under `<run>-r<rate>`. Keep the storm step last in
  `--steps`, or pass a storm step that is not in the list. The drill keeps
  its jobs resident, and the campaign refuses any ordering that would
  start a later step dirty.

  The CLI loop's history is why this matters. `web_fleet` is ~6
  allocations per job, so its ~36-103 jobs/sec ceiling is only ~600
  alloc/sec of demand, and Legion already served ≥336 alloc/sec while the
  driver was the constraint.
- **The counting path is checked, not trusted.** The driver counts
  placements by parsing one `legion events follow` NDJSON stream on one
  Python thread. Coalescing (`allocation.placed` carries the plan's count)
  has kept that ahead of the scheduler so far, but at native-submitter
  rates it is the next plausible floor, and a follower that drops events
  reports a *slower scheduler*, not a broken driver. So every Legion cell
  samples the manager's own `legion_allocations` gauge before submission
  and after settling and archives the comparison as `cross_check` in
  `metrics.json` (`agree: false` also prints a warning). The endpoint is
  `--manager`'s host on `server.port + 2` unless `--metrics-url` says
  otherwise; unreachable records `available: false` and nothing else.
  Read a disagreeing cell's throughput as suspect — either the follower
  fell behind or the fleet carried allocations from a previous cell.
- **Submission rate is a series, not an average.**
  `submission.per_minute` is jobs submitted per minute from the first
  submit. The phase-5 CLI loop sagged from 89 to 17 jobs/sec over a day
  and the per-cell average hid it; a falling series is the driver
  degrading and is visible without re-reading archives.
- **Driver-side clocks.** Submit and placed timestamps are taken by the
  same driver process for every system; no cross-host clock skew enters
  a latency, and all systems pay the same stream-delivery latency. The
  native submitter is the one exception, and it is closed rather than
  waived: it runs in its own process, so it stamps in UNIX epoch
  milliseconds and the driver hands it the `monotonic - epoch` offset
  (`--clock-offset-ms`) that rebases those stamps onto the same clock
  its placement events use. Same host, same tick rate; only an NTP
  *step* mid-cell could skew a latency.
- **Expectation manifests.** The translator declares how many
  allocations every job must produce; the driver reports shortfalls
  (`complete: false`) instead of averaging over them.
- **Fairness record per cell.** `collect.sh` archives versions, git
  revision + binary hash, rendered configs, and instance types with
  every cell. Rule: default configs plus documented, defensible tuning
  only — every non-default is commented in `configs/` and lands in the
  record. Heartbeat cadence is a default too: Legion runs adaptive
  cadence and Nomad its own `n/rate` scaling, and every Legion cell
  archives the served interval and lost window it ran at.
- **Projection honesty.** `fit_curves.py` publishes intervals and
  validation deltas, and flags superlinear fits instead of smoothing
  them.

## Known limitations (also see docs/worker-simulation.md)

- **Legion sim fleets changed shape on 2026-10-12** (sandbox-scaling plan
  Phases 0–2). Simulated nodes now report allocation health, as real
  workers do, and take their work over `Manager.StreamWork` instead of the
  heartbeat's desired set and `PostWork` pushes. Cells run before that date
  understate the heartbeat path's write load and pay full-set heartbeats;
  do not compare their heartbeat-path or dispatch numbers with later cells.
  Adding `--sim-no-work-stream` to the `legion start worker --simulate`
  unit `start-sim-fleet.sh` writes reproduces the old delivery path.
- Sim fleets share one dispatch address per instance, so per-node
  connection-establishment cost on the manager is understated at large
  N (same trade in nodesim/KWOK).
- nomad-nodesim sets a node's class but not its meta, so Nomad can only
  run class-constrained scenarios (`web_fleet`, `batch_burst`,
  `dense_fleet`). `translate.py` refuses `mixed_heterogeneous` for Nomad
  rather than producing jobs no node can take.
- K8s runs with its API client QPS raised (scheduler and
  controller-manager) but stock eviction timers, so its storm cell is
  capped at 30 minutes and reported as a replaced fraction. See
  `configs/k8s/kubeadm-config.yaml.tpl` and calls entry 7. Its etcd quota
  is raised to 8 GiB so the largest `dense_fleet` cells cannot end on a
  NOSPACE alarm (calls entry 13), and each KWOK slice talks to a different
  apiserver rather than all to cp-0 (calls entry 8).
- K8s campaigns before 2026-10-14 have no scheduler or apiserver metrics.
  Prometheus mounted `prometheus.yml` as a single file, which pins its
  inode, so the scrape jobs `install-k8s.sh` appends never loaded. It now
  mounts the directory.
- Nomad's leader serves a heartbeat after nearly every client RPC (Nomad
  2.0.5's client wakes its heartbeat loop on each successful RPC), so it
  handled ~2,500-3,300 `Node.UpdateStatus` calls/sec at 2k-5k nodes where
  its 50/sec TTL target suggests ~50. That is Nomad's real client inside
  nodesim, so it stays in, and every cell archives the rate as
  `nomad_heartbeats` (calls entry 9).
- Competitor watches reconnect when the server ends them (the K8s
  apiserver closes watchers that fall behind in a burst). Binds in the
  gap are stamped late, which biases that system's latency upward, so
  every cell records `watch_restarts`. Check it before quoting a K8s or
  Nomad latency. See calls entry 11.
- Nomad's down nodes stay in its node table until GC, so its fleet gate
  counts *ready* nodes; a restarted nodesim fleet registers new node IDs.
- Batch workloads translate to K8s Deployments (not Jobs) so all three
  systems schedule "keep N running" semantics; noted in the fairness
  record.
- `collect.sh` snapshots Prometheus over the cell's own window when
  `run-step.sh` supplies one (`CELL_START_EPOCH`), and falls back to a
  trailing 2h superset otherwise; the fairness record says which. The
  driver's own timestamps remain authoritative for the reported numbers.
- **Nothing removes node records.** There is no deregistration RPC
  (`Store::remove_node` has no production caller), so a fleet's nodes
  outlive it and only a control-plane reset clears them. Node names are
  therefore reused run to run on purpose — a unique per-run prefix would
  make every run leak its whole fleet instead of overwriting the last
  one's. The consequences are handled by the reset at campaign start and
  the per-cell census gate, but the underlying gap is real.
- **Fleet churn contaminates the cell that follows it.** Every fleet
  stop/start marks thousands of nodes lost and raises a reschedule eval
  per node (measured: 5,760 reschedules against 2,503 job submissions at
  the 10k step), which competes with the cell's own submissions for the
  eval workers. Cells now avoid the restart where they can, wait for the
  backlog to drain where they cannot, and archive the depth they started
  from in `pre-cell.json` and the fairness record — but a cell that
  started from a non-zero backlog is still a cell to read carefully.
