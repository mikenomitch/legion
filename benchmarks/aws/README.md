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
scripts/bootstrap-operator-box.sh ssh 'cd ~/legion/benchmarks/aws && scripts/teardown.sh'
scripts/bootstrap-operator-box.sh destroy
```

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
  cross-system default since 2026-10-09 (calls entry 5). Before then only
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
  `configs/k8s/kubeadm-config.yaml.tpl` and calls entry 6.
- Competitor watches reconnect when the server ends them (the K8s
  apiserver closes watchers that fall behind in a burst). Binds in the
  gap are stamped late, which biases that system's latency upward, so
  every cell records `watch_restarts`. Check it before quoting a K8s or
  Nomad latency. See calls entry 8.
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
