# Judgement calls: aws-comparative-benchmark-plan

Plan: [`../plans/aws-comparative-benchmark-plan.md`](../plans/aws-comparative-benchmark-plan.md)

Entries 1-4 are planning-stage calls; 15-20 were made while building the
rig (phases 0-1, 2026-07-30); 21-23 came from making campaigns runnable
unattended (2026-08-02); 28-29 came from making the rig startable at all
from an arm64 macOS machine (2026-08-02); 30 came from adding memory to
the measured results during the first real campaign (2026-08-02); 33-34
came from replacing the rig's process-per-job submission with a native
submitter, after three sessions of "throughput" numbers turned out to be
the driver's own ceiling (2026-08-05); 6, 7, 10, and 11 came from starting
the cross-system campaign on current Nomad and K8s releases (2026-10-09);
5, 8, 9, and 12-14 came from rerunning it with every system on the same
disk (2026-10-14).
Ranked by project impact; the number is the rank.

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

## 5. Every system's store sits on the same local NVMe

**The call.** Cloud-init mounts the control plane's instance store once,
at `/mnt/store`, and bind-mounts it onto `/var/lib/legion`,
`/var/lib/nomad`, and `/var/lib/etcd`. Each system keeps its stock data
path, and no config names the NVMe. Until 2026-10-14 only
`/var/lib/legion` was on it, so Nomad's raft log and etcd's WAL sat on the
gp3 root volume.

```text
instance store /dev/nvme1n1 mounted at /mnt/store, bound to /var/lib/legion /var/lib/nomad /var/lib/etcd
```

**Alternatives.** (a) Leave the competitors on gp3 and footnote it.
(b) Point each system's data dir at the NVMe in its own config
(`data_dir`, `etcd.local.dataDir`). (c) Give every system provisioned-IOPS
EBS instead of instance store.

**Trade-off.** The comparison no longer shows how any of the three behaves
on network storage, which is where many real clusters run etcd. Instance
store is also ephemeral, so the rig still cannot stop and start its
control plane. Bind mounts survive `wipe-control-plane.sh`'s `rm -rf`
only because `rm` cannot remove a mountpoint, and the wipe now prints
"Device or resource busy" for each one.

**Why (a comparison a skeptic cannot dismiss).** The rig had already
measured 86 commits/sec on gp3 against about 2,000 on NVMe for Legion,
and that finding is why Legion moved. Leaving the competitors behind made
"was it the disk?" an unanswerable objection to every number. It turned
out not to be the disk: with all three on NVMe, Nomad's 5k leader ran at
3.2 of 4 cores of user time with iowait near zero, and neither competitor
got meaningfully faster. That result is only worth something because the
rerun removed the doubt. (b) was rejected because three configs naming
one mount drift, and (c) costs money for a slower disk than the one the
boxes already have.

## 6. Every system submits through its own API over pooled connections

**The call.** Nomad and K8s get native submitters in the driver,
`http_submit_all` in `driver.py`: one process, one keep-alive HTTP
connection per worker thread, `POST /v1/jobs` for Nomad and `POST
.../deployments` for K8s, at the same concurrency `legion-loadgen` uses.
This retires the rule from entry 33 that a cross-system table must force
all three systems onto the CLI loop. The CLI loop survives as a control
row (`--submitter cli`), which still switches all three at once.

```text
# Cross-system table: the default. Each system's own API, pooled.
scripts/run-campaign.sh --system nomad --scenarios web_fleet

# Control row: all three on the process-per-job loop.
LEGION_SUBMITTER=cli scripts/run-step.sh --system k8s --nodes 5000 ...
```

**Alternatives.** (a) Keep the CLI-loop rule and run all three through
`nomad job run` / `kubectl apply` / `legion job run`. (b) Write Nomad and
K8s submitters in Rust or Go, matching `legion-loadgen`'s language.
(c) Use each system's batch path (none of the three has a true
multi-job submit API, so this collapses to (b)).

**Trade-off.** The submitters are no longer the same program, and
Legion's is compiled while the others are Python threads. A Python
submitter that cannot keep up would make Nomad or K8s look slower than
they are. That is caught rather than assumed away:
`submission.driver_bound` flags any cell whose placements finished with
its submissions, and a flagged competitor cell is reported as a floor.
Revisit with (b) if a competitor cell is ever driver-bound.

**Why (honesty of the headline).** Option (a) caps every system at the
loadgen box's process-spawn rate, ~36-103 jobs/sec measured. Phase 5
showed Legion consumes everything that loop produces, and Nomad's C2M
numbers suggest it can too, so the table would compare two copies of the
same ceiling and call it a tie. A comparison that cannot tell the
systems apart is not conservative, it is uninformative.

## 7. K8s gets its API client limits raised; eviction timers stay stock

**The call.** The kubeadm config raises the scheduler's and
controller-manager's API client QPS and burst to 1,000 and their
ReplicaSet/Deployment sync workers to 50. Everything that shapes
*scheduling and recovery behavior* stays stock: scoring plugins,
`percentageOfNodesToScore`, `node-eviction-rate`, and the 300-second
default not-ready toleration.

**Alternatives.** (a) Pure stock K8s. (b) Also tune recovery: raise
`--node-eviction-rate` and drop `default-not-ready-toleration-seconds`.

**Trade-off.** The K8s column is no longer "what kubeadm gives you".
Anyone comparing against a stock cluster will see faster K8s numbers
here than they would get, and the report must say so. In the other
direction, the stock recovery timers make K8s's storm number look very
slow: a 1,250-node slice taints at 0.1 nodes/sec, which is hours. The
drill caps the watch at 30 minutes and reports a replaced fraction.

**Why (measure the scheduler, not a token bucket).** The scheduler
binds each pod with one API call, and its client defaults to 50 QPS. At
defaults K8s cannot bind more than ~50 pods/sec however fast its
scheduling cycle is, and the controller-manager's 20 QPS limits how fast
pods even exist. That is a rate limiter on a client, and leaving it in
would make the K8s placement column a constant. SIG-Scalability raises
these for its own large-cluster tests. Recovery is different: the
eviction rate and tolerations are deliberate product defaults that
operators run with, so tuning them away would flatter K8s on exactly the
dimension the storm row exists to measure. If a reader wants the tuned
recovery number, that is a second, labeled row, not the default.

## 8. Each KWOK slice talks to a different apiserver

**The call.** `start-sim-fleet.sh` rewrites each slice's kubeconfig to
point at `CP_PRIVATE_IPS[slice % 3]`, so the three apiservers share the
lease renewals and pod status writes. The driver and the control-plane
components are unchanged. `K8S_SPREAD_APISERVERS=0` restores the old
behavior for an ablation cell.

**Alternatives.** (a) Keep every slice on cp-0, the `controlPlaneEndpoint`.
(b) Put a load balancer (an NLB or HAProxy) in front of the three
apiservers, as production kubeadm HA does.

**Trade-off.** Spreading by slice is static, so one apiserver can carry
more slices than another at uneven fleet sizes. The ablation cell exists
to say how much of any K8s change came from this rather than from the
disk.

**Why (each system run the way it is meant to be deployed).** With every
controller on cp-0, that one apiserver carried every lease renewal and
status write and sat at 4 of 4 cores in the 2026-10-10 archives while the
other two idled. Nomad's simulated clients already list all three servers,
so K8s was the only system funneled through one box. (b) is the real
production shape but adds a component the other two systems do not need
and that the storm drill would have to route around.

## 9. Nomad's heartbeat-per-RPC load is measured, not tuned away

**The call.** The Nomad leader served about 2,500-3,300
`Node.UpdateStatus` calls/sec at 2k-5k nodes, where its
`max_heartbeats_per_second = 50` design suggests about 50. That is Nomad
2.0.5's own client: every successful client RPC calls
`fireRpcRetryWatcher()` (`client/rpc.go`), and the heartbeat loop wakes
on that channel (`case <-c.rpcRetryWatcher()`), so a busy client
heartbeats after each of its other RPCs. nomad-nodesim embeds that client
unchanged, so the rig leaves it alone and archives the rate per cell
(`nomad_heartbeats`).

**Alternatives.** (a) Patch nodesim's client to heartbeat only on its
TTL. (b) Raise `max_heartbeats_per_second` or `min_heartbeat_ttl` on the
servers. (c) Drop Nomad's heartbeat load from the comparison by
subtracting it.

**Trade-off.** Nomad's leader spends real CPU on this, and Nomad is
CPU-bound at 5k, so its throughput would likely rise without it. A reader
who runs Nomad clients that do fewer RPCs than nodesim's may see less
heartbeat load than the rig did.

**Why (measure the system as shipped).** A real Nomad fleet runs the same
client code, so the load is Nomad's, not the simulator's. (a) would
benchmark a Nomad nobody runs, and (b) changes the server's failure
detection, which the storm row measures. Recording the rate per cell lets
anyone quantify the effect without the rig pretending it is not there.

## 10. Each competitor simulator runs one process per instance slice

**The call.** nomad-nodesim runs one process per (instance, node profile)
with a config that sets `node_class` and resources from the shared
profile split. KWOK runs out of cluster, one controller per worker
instance, each managing only the nodes labeled with its `bench-slice`.
The storm drill kills a slice by stopping that instance's unit, which is
the same fault it injects for Legion.

**Alternatives.** (a) One nodesim process per instance with default
node shape, as the previous script did. (b) KWOK deployed in-cluster as
upstream documents, managing every fake node. (c) Kill KWOK nodes for
the storm by deleting their leases or Node objects.

**Trade-off.** More moving parts per instance: up to four nodesim units
per worker, and a KWOK binary plus kubeconfig on every worker. nodesim
still cannot set node meta, so `mixed_heterogeneous` cannot run on
Nomad at all, and the translator refuses it.

**Why (the same fleet and the same fault on all three).** (a) registered
every node as class `""` with 10,000 MHz, so every class-constrained job
was infeasible and no Nomad cell could have completed. (b) cannot run on
this control plane: there is no CNI, so the control-plane nodes never go
Ready and a KWOK pod would never schedule. It would also make the whole
fleet one process, so a storm could only kill all of it. (c) is a
different fault from "a host died": deleting a Node is an orderly
removal that K8s handles without its not-ready path, which is the path
real outages exercise.

## 11. Competitor watches reconnect like real clients, and the restarts are counted

**The call.** When a placement watch ends mid-cell, the driver resumes
it rather than failing the cell. Nomad's event stream resumes from the
last index it saw. K8s's watch is `kubectl get pods --watch` printing
one custom-columns row per event, filtered to bound pods, and it is
restarted when it exits; its opening list counts anything bound in the
gap. Every cell records `watch_restarts` in `metrics.json`.

**Alternatives.** (a) Fail the cell on any watch interruption, as the
driver did. (b) A Python apiserver watch with a reader thread and a
parse thread. (c) Measure K8s latency server-side from each pod's
`PodScheduled` condition instead of a watch.

**Trade-off.** A bind that lands while the watch is down is stamped when
the relist delivers it, which is late, so a restart biases K8s latency
*upward*. In the 1k-node smoke the watch restarted twice. The count is
in every cell so a reader can discount a cell, and (c) remains the
cross-check to build if restarts are frequent at scale. `PodScheduled`
has one-second resolution, which is why it is not the primary clock.

**Why (the apiserver does this on purpose).** The apiserver closes
watchers that fall behind during a burst, and every K8s client,
informers included, answers by relisting. Failing the cell, (a), would
make K8s unmeasurable at exactly the rates the comparison is about. (b)
was tried first: a Python consumer decoding full pod objects was closed
as a slow watcher five times in one cell. kubectl decodes in Go, so the
same arrangement Legion's watch already uses, a CLI follower in a
subprocess, keeps the socket drained.

## 12. Control planes are scraped every 15s, and Prometheus mounts its config by directory

**The call.** The `legion-managers`, `nomad-servers`, `kube-scheduler`,
and `kube-apiserver` jobs scrape at 15s while node and sim-worker jobs
keep the global 5s. Prometheus' compose file mounts its config directory
rather than the single `prometheus.yml`.

**Alternatives.** (a) Keep 5s everywhere. (b) Scrape only the followers.
(c) Keep the file mount and make every writer edit in place.

**Trade-off.** Control-plane series are coarser, so a spike shorter than
15s can hide inside one sample. `collect.sh`'s rate()s still have four
samples per minute.

**Why (the measurement must not tax the thing measured).** A scrape runs
on the box under test. Nomad's leader exposes about 49,000 samples at
4,000 jobs (per-job summary gauges nothing can switch off) and took 1.25s
to encode each one, about a quarter of a core every 5s on a leader that is
CPU-bound. The directory mount fixes a worse problem: a single-file bind
mount pins the file's inode, every `sed -i` writes a new one, and so
`install-k8s.sh`'s appended scrape jobs never loaded. Every K8s campaign
before 2026-10-14 ran with no scheduler or apiserver metrics at all, and
`k8s_sched.json` was empty in every archive.

## 13. etcd's quota is raised to 8 GiB

**The call.** `kubeadm-config.yaml.tpl` passes
`--quota-backend-bytes=8589934592` to the stacked etcd.

**Alternatives.** (a) Keep the 2 GiB default and report a NOSPACE failure.
(b) Shorten the apiserver's compaction interval instead.

**Trade-off.** One more documented non-default in the K8s column.

**Why (end cells on the scheduler, not on a capacity limit).** The
`dense_fleet` sweep holds up to 200,000 pods plus their events between
compactions, and the published K8s 20-per-node cell "failed" with no
archive to say why. 8 GiB is the ceiling etcd's own docs suggest. It
changes how much etcd holds, not how fast it commits, and (b) would
change K8s's own write path.

## 14. Fixed 2s heartbeats are a labeled Legion row, not the default

**The call.** `LEGION_HEARTBEAT_FIXED=1` renders `heartbeat = { adaptive
= false }` into the manager config for one ablation campaign, so every
node beats every 2s at any fleet size. The comparative cells keep adaptive
cadence, as entry 37 requires. This row is the measurement entry 37
deferred.

**Alternatives.** (a) Pin every Legion cell to 2s. (b) Skip the ablation
and argue from the archived heartbeat rates alone.

**Trade-off.** One more Legion campaign on the rig, and a row readers can
misread as Legion's default.

**Why (answer "is adaptive cadence flattering Legion?" with a number).**
Adaptive cadence is Legion's default and Nomad scales its own TTL the same
way, so default-against-default stays the headline. But a reviewer asked
whether fewer heartbeats were flattering Legion's throughput, and the
cleanest answer is the same cell with the cheaper path turned off.

## 15. Every latency timestamp is driver-side receive time

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

## 16. `allocation.placed` events are the unit of counting, weighted by their `count`

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

## 17. Terraform is system-agnostic; per-system installs are SSH scripts

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

## 18. The dry-run gate is a process-based local smoke, not the planned docker-compose stand-in

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
integration test (`tests/integration/worker_simulation.rs`) instead.

## 19. Batch workloads translate to K8s Deployments, not Jobs

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

## 20. One bucket, one IAM role for the whole rig

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

**Amended (entry 23).** Still one bucket, but now two roles: the loadgen
box takes an `orchestrator` role that adds `SetDesiredCapacity` on the
sim-fleet ASG. Bucket access is unchanged and shared.

## 21. Results are mirrored to a bucket terraform does not manage

**Call.** `var.archive_bucket` names an existing, externally-created S3
bucket. `collect.sh` mirrors every cell into it as the cell lands, and
`teardown.sh` re-syncs anything that missed before destroying anything.
The rig's IAM role gets `GetObject`/`PutObject` on it and deliberately
not `DeleteObject`. Unset is allowed, and teardown says loudly what that
costs.

**Alternatives.** (a) Keep the single bucket of entry 20 and rely on
teardown's pre-destroy sync to the operator's machine — the shipped
behavior until now. (b) Have terraform create a second bucket with
`prevent_destroy`. (c) Drop `force_destroy` from the rig bucket.

**Trade-off.** A prerequisite that terraform cannot create for you: one
`aws s3 mb` before the first campaign, or the warning fires on every
teardown. It also splits "where are the results" into two answers.

**Why (the campaign is the product).** (a) fails precisely when it
matters: entry 22 moved the driver's seat onto the loadgen box so a run
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

## 22. Campaigns are driven from the loadgen box, not the operator's machine

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

## 23. Scale steps move through the ASG API, not `terraform apply`

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

## 24. The spot fleet is released on any campaign exit

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

## 25. Campaign state is published to S3, not only answerable over SSH

**Call.** The unattended runner publishes `status.json` (phase, step,
detail, timestamp) and a tail of its console log to
`s3://<bucket>/status/`, on every transition and on a 60s heartbeat.
`scripts/campaign-status.sh show|watch` reads it with nothing but a
bucket name, a region, and credentials, and returns the state as its
exit code: 0 running, 2 finished, 3 failed, 4 stale, 5 missing.

```bash
scripts/campaign-status.sh show --bucket <bucket> --region us-east-1
```

**Alternatives.** (a) `remote-campaign.sh status`, which entry 22
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

## 26. Spot and on-demand are split by a knob, not a quota ticket

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

## 27. Nodes-per-instance is measured before it is trusted

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

## 28. A disposable operator box, outside the rig's terraform

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

## 29. The operator box carries an IAM role, never a copied key

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

## 30. Memory is measured whole-box, not as manager process RSS

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

## 31. Cells are sized by allocation target, not by a per-scenario multiplier

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

## 32. The driver submits concurrently, because serially it measured itself

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
across processes, which entry 15 exists to avoid.

**Sized to the box, not to a round number.** The default is 4, not 16,
because the loadgen box is an `m7i.large` — 2 vCPU. Each slot is a whole
CLI process; oversubscribing starves the event-watcher thread and the cell
measures the driver's own contention.

**Superseded for Legion by entry 33 (2026-08-05).** Raising the pool moved
the ceiling (4 → 32 on a 16-vCPU box took it from ~36 to ~103 jobs/sec) but
never removed it, because the per-job cost is a whole process. This loop is
now the *fairness baseline* rather than the fast path: it is what Nomad and
K8s use, and what a cross-system comparison forces Legion back onto.

## 33. Legion gets a native submitter, and that makes cross-system comparison conditional

**Superseded in part by entry 6 (2026-10-09).** The competitors now have
native submitters too, so the cross-system rule below no longer forces
the CLI loop; it survives as the control row.

**The call.** A separate binary, `legion-loadgen`
(`benchmarks/loadgen/main.rs`, `cargo build --features loadgen`), submits a
whole workload from one process: a small pool of long-lived gRPC channels
and hundreds of concurrent `RunJob` calls, no per-job process spawn or
dial. `driver.py` uses it for the `legion` adapter only. Nomad and K8s keep
the process-per-job loop, unchanged.

The asymmetry is made explicit rather than hidden. `--submitter` is a
first-class flag, `cli` forces every system onto the shared loop, and the
path taken is archived per cell in `metrics.json` and
`fairness-record.json`:

```bash
# Legion-vs-Legion sweep (scaling, saturation, eval threads): fast path.
scripts/run-step.sh --system legion-a --nodes 5000 --scenario web_fleet

# Any Legion-vs-Nomad-vs-K8s table: all three on the shared CLI loop.
LEGION_SUBMITTER=cli scripts/run-step.sh --system legion-a ...
```

```json
{ "throughput_per_sec": 1126.16, "submitter": "native",
  "submission": { "span_secs": 2.639, "rate_per_sec": 758.24,
                  "drain_secs": 8.079, "driver_bound": false } }
```

**Alternatives.** (a) Keep raising `--submit-concurrency` on the CLI loop.
(b) Generate Python gRPC stubs from `proto/legion.proto` and submit from
the driver process. (c) Add a batch-submission RPC to Legion itself.
(d) Give all three systems native submitters at the same time. (e) Make
the fast path the only path and delete the CLI loop.

**Trade-off.** This is the entry's real cost, and it is a methodological
one: **a `native` cell is comparable to another Legion cell and to nothing
else.** The rig now has two submission paths with a 5-7x throughput gap
between them, and using the fast one for Legion and the slow one for Nomad
would produce a cross-system table that flatters Legion for reasons that
have nothing to do with either scheduler. That hazard did not exist while
one loop fed all three. It is mitigated by making the choice explicit,
loud, and archived — never by defaulting quietly — and the rule is written
into `benchmarks/aws/README.md` § Methodology, the driver's module
docstring, and the submitter's own header, because a rule recorded only in
a judgement-calls file is a rule nobody reads at 2am.

The secondary cost is a second client of the manager API that is not the
CLI. It is kept to a minimum: the binary calls the CLI's own
`connect_manager_client()`, so TLS, `LEGION_ADDR`, and token resolution
cannot drift from what a user gets, and it links the crate's generated
client so the proto keeps one source of truth.

**Why (performance — the rig was measuring itself).** `web_fleet` is ~6
allocations per job, so the CLI loop's ~36-103 jobs/sec ceiling is at most
~600 alloc/sec of demand — and Legion had already served ≥336 alloc/sec
with the driver, not Legion, as the constraint. Every such cell reported
`submission.driver_bound: true`, which is honest but is a floor, not a
measurement, and small-job performance is precisely Legion's weakest and
most important case.

Measured on one macOS box (M-series, single manager, `local_disk` store,
5,001 `web_fleet` jobs), alternating the two paths back to back so both saw
the same machine:

| | CLI loop (32) | native (128/4) |
| --- | --- | --- |
| idle box | 255-260 jobs/sec | 1,107-1,492 jobs/sec |
| loaded box | 105.0 / 108.3 | 740.5 / 563.8 |

**5-7x, consistently, and the best native point was 1,492 jobs/sec.** The
end-to-end effect is the one that matters: driving 2,001 jobs at a 600-node
fleet, submission went from 8.54s to 2.64s (234 → 758 jobs/sec) and the
placement tail after the last submit went from 2.1s to 8.1s. Both cells
completed and both reported `driver_bound: false`, but the fourfold drain
is the scheduler being asked for real work instead of being fed at its own
pace.

That burst also cost measured placement throughput — 1,329 alloc/sec on the
paced CLI cell against 1,126 on the native one, about 15% — which is a
*result*, not a regression in the submitter: it is the plan-apply conflict
behaviour showing up once submission stops rate-limiting the scheduler for
us, and it is exactly the effect the rig could not see before.

**Concurrency is measured, not assumed.** The default is 128 in flight over
4 connections because throughput here is *not* monotonic in concurrency —
32/643, 64/730, 128/1282, 256/1107, 512/686, 1024/607 jobs/sec on the local
box. Past a few hundred the surplus requests only queue at the manager's
control-lane commit. The peak moves with per-request latency, so it sits
higher on the rig (a commit there is a raft round trip) than on one box;
128 is near the local peak and still deep enough to hide that round trip.
Connections matter separately and less: 368 jobs/sec on one connection at
concurrency 32 against 643 on four, which is HTTP/2's 64 KiB per-connection
flow-control window.

(a) was already tried and is what the 36→103 jobs/sec drift is: process
spawn does not get cheaper. (b) puts a second copy of the proto in the
tree, which is exactly the drift the generated client exists to prevent,
and leaves submission on Python threads. (c) changes the product to fix
the harness — a batch RPC has to earn its place on its own merits, not as
a benchmark accessory. (d) is the right answer for a cross-system phase
and is now an open TODO item; it is not a reason to keep Legion's own
scaling curves unmeasurable in the meantime. (e) would delete the only
thing that makes the three-system table valid.

## 34. Cross-process submit stamps are epoch milliseconds, rebased by an offset the driver supplies

**The call.** `legion-loadgen` stamps each submission in UNIX epoch
milliseconds and adds a `--clock-offset-ms` the driver passes it, computed
as `now_ms() - int(time.time() * 1000)` immediately before the child
starts. The driver reads the resulting rows straight into the recorder;
`finalize()` still writes `submits.json` in the same shape as always, so
`replay` and every existing analysis are untouched.

**Alternatives.** (a) Have the submitter write `submits.json` itself and
let the driver read it verbatim. (b) Read `CLOCK_MONOTONIC` directly in
both processes via `libc`. (c) Have the driver stamp the submissions
instead, by streaming progress back from the child. (d) Use epoch
timestamps for placements too, and drop the monotonic clock entirely.

**Trade-off.** A wall-clock *step* (an NTP correction, not a slew) between
the offset sample and the end of a cell would skew that cell's latencies —
a failure mode the pure-monotonic CLI path does not have. Accepted because
the rig's hosts slew, a cell is minutes long, and the smoke test asserts
the resulting percentiles are plausible, which catches a mis-signed or
un-applied offset immediately (a raw epoch stamp shows up as a latency
about 55 years wide).

**Why (entry 15's rule, kept across a process boundary).** Entry 15 makes
every timestamp a driver-side reading on one clock, and that is what makes
a submit→placed latency mean anything. A second process cannot join that
clock: `time.monotonic()` and Rust's `Instant` are both deliberately
epoch-less, so there is no portable way to hand one process's monotonic
reading to another. Epoch time is the only clock two processes can name the
same instant on, so the offset is the bridge. (a) reads cleaner but leaves
the two files on different clocks unless the same arithmetic happens
anyway. (b) is not the same clock on every platform — CPython uses
`mach_absolute_time` on macOS while `CLOCK_MONOTONIC` there counts sleep
— so it would work on the rig and quietly lie on a developer's laptop.
(c) puts a pipe and the driver's GIL back in the measured path, which is
what the submitter exists to remove. (d) would expose *every* cell,
including the CLI ones, to clock steps to fix a problem only one path has.

## 35. Each cell tears down its jobs, from an EXIT trap

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

## 36. Cadence evidence is archived per cell, over the cell's own window

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

## 37. Heartbeat cadence stays at the default; adaptive cadence is not measured

> **Superseded (2026-10-09).** The default changed under this call: adaptive
> cadence is now on by default (heartbeat-cadence plan §13), so the rig's
> unset `heartbeat` block runs it. "Stay at the default" still holds, and now
> means adaptive, which matches Nomad also adapting by default. `collect.sh`
> archives the served interval and lost window per cell, and
> `packing-density.sh` divides by the served interval rather than a fixed 2s.

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
incomparable across cells. With entry 36's evidence in every cell, a
cadence shortfall at 5k or 10k becomes a recorded finding rather than a
contaminated number — which is the outcome the plan wants from a first
campaign. (a) remains the right thing to measure in a follow-up whose
stated subject is the cadence ladder itself.

## 38. "Stopped converging" is a stall, not a wall-clock deadline

- **The call:** the driver ends a cell on a **stall timeout** — no
  placement at all for 120s — with the old fixed `--settle-timeout-secs`
  demoted to an absolute cap that is *derived* from the expected
  allocation count (20 alloc/sec floor) rather than fixed at 600s.
  `metrics.json` gains an `outcome` field (`complete` / `stalled` /
  `timeout` / `watcher_died`).
- **Alternatives.** (a) Keep the flat 600s and raise it by hand per cell.
  (b) Scale the deadline with cell size but keep a single clock.
- **Trade-off.** Two knobs instead of one, and a cell that trickles
  placements slower than one per 120s is now reported as stalled even
  though it was still moving.
- **Why (measurement honesty).** The saturation sweep is the whole point
  of the density axis, and its larger points are legitimately long: 200k
  allocations needs ~10 minutes at a *healthy* 350 alloc/sec. Under a flat
  600s every one of those points would have been reported incomplete for
  arithmetic reasons, and the sweep would have read that as the
  scheduler's ceiling — inventing a finding out of a rig default. (b)
  fixes the arithmetic but still cannot distinguish "big" from "stuck",
  and that distinction is the answer the sweep exists to produce. The
  stall clock is size-independent, which is exactly the property the
  question needs; the cap only survives to bound a pathological trickle.
  `outcome` matters for the same reason: an incomplete cell that stalled
  is a scheduler result, one that hit the cap is a rig result, and
  `complete: false` alone cannot tell a reader which they are holding.

## 39. A second scenario for saturation, rather than pushing web_fleet harder

- **The call:** added `benchmarks/scheduler/scenarios/dense_fleet.json` —
  web_fleet's node profiles unchanged, but ~44 allocations per job
  (against 6) and 50 mCPU / 64 MiB requests (against 500 / 1024).
- **Alternatives.** (a) Sweep density on `web_fleet` as the rig already
  allowed. (b) Add a replica multiplier to `translate.py` so density and
  job count become independent axes on any scenario.
- **Trade-off.** A second scenario to keep honest, and one more cell in
  the local SLO gate (it passes: 31.3 evals/sec, p99 81ms, correctness
  1.000). `dense_fleet` is also less like a real fleet than `web_fleet`,
  so it measures the scheduler rather than describing a customer.
- **Why (the measurement would otherwise be of the rig).** Two independent
  ceilings sit below the scheduler's on `web_fleet`. The density axis
  scales job *count*, and each submit is a fresh CLI process on a 2-vCPU
  loadgen box, so every density point is also a submission-rate point —
  the driver saturates first. And at 500 mCPU an 8-core node holds ~16
  allocations, so a sweep past that measures capacity exhaustion, which is
  a correct refusal to place rather than a throughput number. Fat jobs
  with small requests move both ceilings out of the way. (b) is the more
  general fix and is the right follow-up, but it changes a translator that
  every existing cell depends on, which is not a change to make in the
  same session as the measurement it feeds. A 50-replica service into one
  pool is also precisely the shape `decouple_plan_components` used to
  collapse, so the scenario earns its place twice.

## 40. `evalThreads` is rendered per campaign, and refused elsewhere

- **The call:** `run-campaign.sh --eval-threads N` exports
  `LEGION_EVAL_THREADS`, which `install-legion-a.sh` renders into
  `manager-a.ncl.tpl`. Passing it to any system other than `legion-a`, or
  alongside `--no-reset-control-plane`, is a hard error.
- **Alternatives.** (a) Edit the committed template between runs, as
  phase 2 did. (b) Accept the flag everywhere and ignore it where it does
  not apply.
- **Trade-off.** The value has to survive the control-plane reinstall that
  every campaign performs, so it travels as an environment variable
  through `reset-control-plane.sh` — a slightly less obvious path than a
  plain command-line argument.
- **Why (reproducibility).** The phase-2 finding that more eval threads
  *cost* throughput is the single most useful result of that campaign and
  the one the plan-apply fix is judged against, and it could not be
  reproduced in one command. (b) is the dangerous option: a silently
  dropped flag produces a cell archived as a 16-thread result whose
  rendered config — the fairness record — says 4. A run that fails loudly
  is strictly better than a number that lies quietly, which is the same
  principle as entry 36.

## 41. The operator box's toolchain is baked into an AMI, and a stale image is refused rather than reused

- **The call:** `scripts/bake-operator-ami.sh` runs the toolchain install
  once on a throwaway instance and snapshots it into an AMI tagged
  `Name=legion-bench-operator-ami`. `bootstrap-operator-box.sh` looks that
  image up by tag and launches from it, leaving only per-instance work
  (the rig's SSH key pair) in user-data. Both paths run the *same*
  `scripts/operator-toolchain.sh`, and the AMI carries a tag holding that
  file's content hash. When the hashes disagree the baked image is
  **ignored**, not used: the bootstrap says so, says how to re-bake, and
  falls back to installing in user-data.

  ```bash
  scripts/bake-operator-ami.sh --region us-east-1   # once, ~20 min
  scripts/bake-operator-ami.sh --show               # baked? current?
  scripts/bootstrap-operator-box.sh launch --profile me   # finds it by tag
  ```

- **Alternatives.** (a) Leave it: keep installing in user-data every time.
  (b) Bake, but use whatever image carries the tag — no fingerprint, no
  staleness check. (c) Fingerprint it, and *fail* rather than fall back,
  forcing a re-bake before any box can be created. (d) Reach for Packer,
  or add the AMI to `terraform/`. (e) Keep two copies of the install list,
  one in the bake script and one in the user-data heredoc.
- **Trade-off.** A second artifact with its own lifecycle, in a repo whose
  standing complaint is already that the operator box is a second
  lifecycle (entry 28). AMIs are regional, so a campaign in a new region
  bakes again. And the strict fingerprint means a comment-only edit to
  `operator-toolchain.sh` invalidates a perfectly good image — `--ami
  <id>` exists for exactly that, and `--slow-bootstrap` for the reverse.
- **Why (ease, and the cost of being wrong).** The number is the argument:
  `cloud-init analyze blame` measured `modules-final/config-scripts_user`
  at **3245 seconds — 54 minutes** on one launch of this box, and ~14
  minutes on another with the same type, region, and script. That variance
  is the tell: it is `apt-get` throughput, which the rig does not control
  and cannot tune, spent before any Legion code compiles. (a) pays it on
  every box, and this box is created more often than its "build it once"
  framing suggests — every failed `create`, every region, every fresh
  account.

  (e) is the real risk this change introduces, and one file is the whole
  mitigation: a drifted install list produces a box that looks healthy and
  dies hours later inside a `cargo build` on a missing `protoc`, which is
  a far worse failure than a slow bootstrap. (b) has the same failure
  shape one step removed — the tag says "toolchain", the image says
  something older. (c) is the tempting strict answer and is wrong here for
  the reason entry 28 exists: `bootstrap-operator-box.sh` is the *first*
  thing a new operator runs, and a step 0 that refuses to work until you
  have baked an image is exactly the prerequisite hunt that script was
  written to delete. The fallback is a correct box, only slow, and it says
  which path it took and why. (d) buys image-build features this needs
  none of, against a tool nobody here has installed; terraform is
  circular for the same reason it is in entry 28 — this box runs the
  terraform, and must outlive `teardown.sh`.

  Two details are security calls rather than speed ones. The bake instance
  gets **no key pair**: an AMI baked from an instance that had one hands
  that key to every box launched from the image afterwards. And the rig's
  SSH key pair stays in per-instance user-data rather than moving into the
  toolchain script, because baking it would put the private half of the
  rig's trust domain inside a disk image and give every future operator
  box the same key. The bake epilogue removes the host keys and runs
  `cloud-init clean` for the same reason.

## 42. `teardown.sh` with no terminal refuses loudly instead of guessing

*Follow-up, 2026-10-19.*

**The call.** Without `--yes`, `teardown.sh` checks for a terminal on stdin.
With none (`nohup`, `< /dev/null`, a command over `ssh`) it prints that it is
not destroying the rig and why, and exits 2. The README's unattended
teardown passes `--yes`.

**Alternatives.** Proceed without asking when there is no terminal.

**Trade-off.** An unattended teardown still needs the flag; forgetting it
fails rather than tears down.

**Why.** Simplicity. The bug was a silent exit that left the rig billing;
a loud refusal fixes that without making a destroy happen on a guess.
