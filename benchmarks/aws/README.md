# AWS Comparative Benchmark Rig

The reproducible rig for
[`internal_docs/plans/aws-comparative-benchmark-plan.md`](../../internal_docs/plans/aws-comparative-benchmark-plan.md):
Legion (raft-lanes **and** streaming), Nomad, and Kubernetes measured on
identical AWS hardware with simulated worker fleets. The methodology is
part of the claim — everything a campaign produces is reproducible from
this directory.

**Status: rig prepared, awaiting an AWS account.** Everything here is
built and locally validated (`scripts/local-smoke.sh`, driver fixture
tests, `terraform validate`); the Nomad/K8s install paths are complete
but marked TO-VERIFY until their campaign phases run on real instances.
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
| `analysis/` | `fit_curves.py`: scaling fits + 20k/50k projections with prediction intervals and validation deltas. |
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
- **Submission paths are not interchangeable — the one rule that
  invalidates a comparison if you break it.** Legion has a native
  submitter, `legion-loadgen` (`benchmarks/loadgen/main.rs`): one
  process, a few long-lived gRPC connections, hundreds of concurrent
  `RunJob` calls. Nomad and K8s do not; they still spawn a CLI process
  per job, which is the shared loop that made the comparison fair in the
  first place. **A cross-system comparison must therefore either run all
  three with `--submitter cli` (`LEGION_SUBMITTER=cli` for
  `run-step.sh`), or add equivalent native submitters for Nomad and K8s
  first.** Anything else compares submitters, not schedulers, and it does
  so in Legion's favour. Every cell archives the path it took as
  `submitter` in both `metrics.json` and `fairness-record.json`, so this
  is checkable after the fact rather than a thing to remember.

  The submitter exists because the CLI loop was not fast enough to
  measure Legion's weakest case. `web_fleet` is ~6 allocations per job,
  so its ~36-103 jobs/sec ceiling is only ~600 alloc/sec of demand, and
  Legion already served ≥336 alloc/sec while the driver was the
  constraint — every such cell reported `submission.driver_bound: true`,
  which is a floor and not a measurement. A Legion-vs-Legion sweep
  (scaling, saturation, eval-thread) is what `native` is for; a
  Legion-vs-Nomad-vs-K8s table is not.
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
  record.
- **Projection honesty.** `fit_curves.py` publishes intervals and
  validation deltas, and flags superlinear fits instead of smoothing
  them.

## Known limitations (also see docs/worker-simulation.md)

- Sim fleets share one dispatch address per instance, so per-node
  connection-establishment cost on the manager is understated at large
  N (same trade in nodesim/KWOK).
- The Nomad/K8s driver adapters and install scripts are complete but
  unexercised against real clusters until phases 3-4; the nodesim
  `mock`-driver assumption and the K8s watch-parsing are the two
  flagged verification points.
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
