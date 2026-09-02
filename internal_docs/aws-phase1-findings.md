# AWS phase 1: what we found (2026-08-02/03)

The first time the comparative-benchmark rig ran against a real AWS
account. Phase 1's exit criterion passed, and the run turned up a
manager-side scaling limit that matters more than the benchmark numbers
it was supposed to produce.

Read this to pick the work back up. The plans it points at own the
decisions; this is the record of how we got to them.

- Plan: [`plans/aws-comparative-benchmark-plan.md`](plans/aws-comparative-benchmark-plan.md)
- Calls: [`calls/aws-comparative-benchmark-plan_calls.md`](calls/aws-comparative-benchmark-plan_calls.md)
- Open work: [`TODO.md`](TODO.md)

## Status

**Phase 1 complete.** `expected_total: 180, placed_total: 180,
complete: true` — every allocation the translator declared was placed and
counted, end to end on real hardware: translator → submission → event
stream → counting → S3 archive.

**Its performance numbers are not usable** and must not be quoted. The
cell reported 0.9 placements/sec at a p50 of 109s, which measures the
heartbeat ceiling below, not the scheduler.

**All AWS resources destroyed.** Only
`s3://legion-bench-archive-594906412132/` remains (8 result archives,
255 KB), deliberately — it is outside terraform so `teardown.sh` cannot
reach it. Local copies are in `benchmarks/aws/results/s3-archive/`,
extracted alongside the tarballs.

Total spend for the night: **roughly $8**.

## The headline finding: heartbeats were fsync-bound through raft

A 1000-node fleet at the 2s default cadence needs ~500 heartbeat
RPCs/sec. The rig sustained **~80/sec**, with nothing saturated —
manager CPU 19-23%, sim hosts 0.2%, and `RaftTransport/AppendEntries`
answering in 3ms.

`Manager::heartbeat` called the blocking `Store::update_node_heartbeat`
inline on a tokio worker thread. Parking the thread for an fsync-bound
raft commit was the visible cost. The damaging part was second-order:
every *other* heartbeat stayed queued in the gRPC layer where the
coalescer could not see it, so batches never exceeded size 1 and
`HEARTBEAT_BATCH_MAX = 128` was unreachable. The group commit written
precisely to amortize fsync was structurally prevented from running.

Fixed by offloading to `spawn_blocking`. Measured with
`benchmarks/aws/experiments/heartbeat-commit-probe.sh` on gp3, 3 voters
and 1000 sim nodes on one box:

| worker threads | before | after |
| --- | --- | --- |
| 1 | 48.3/sec (9% cadence) | **349.8/sec (69%)** |
| 4 | 60.5/sec (12%) | **422.6/sec (84%)** |

`cargo test --all --locked`: 1405 passed, 0 failed.

Two facts worth carrying forward:

- **Storage dominates.** Identical code and topology: gp3 ~86/sec, local
  NVMe ~2000/sec. The control plane now runs `m6id.xlarge` with the raft
  store on instance NVMe.
- **This was ~10% from a correctness bug**, not just a slow one. False
  node-loss begins when achieved cadence drops below
  `interval / lost_window` = 2s/10s = 0.20. We measured 0.22. Below it,
  healthy nodes get marked lost, which generates reschedule evals, which
  adds load.

### Why this is Legion's problem specifically

| System | A steady-state heartbeat costs | On network storage |
| --- | --- | --- |
| Nomad | resets an in-memory timer on the leader (`resetHeartbeatTimer`), no raft write | largely immune |
| Kubernetes | one small Lease write to etcd (KEP-589 shrank it from the 15 kB Node object) | milder |
| Legion | a full replicated raft commit through the status lane | most exposed |

Fast disk does not close that gap; it stops storage latency from
swamping it. That is what
[`plans/liveness-off-the-raft-path-plan.md`](plans/liveness-off-the-raft-path-plan.md)
exists to fix, and
[`plans/heartbeat-cadence-at-scale-plan.md`](plans/heartbeat-cadence-at-scale-plan.md)
covers what operators should do until it ships.

## Bugs found and fixed

Roughly fifteen, almost all in the rig rather than in Legion. Grouped by
the shape of the mistake, because the shapes repeat.

**Checks that knew something was wrong and threw the reason away.** The
most expensive category by far.

- `RunInstances` retried *every* failure and reported "IAM propagation",
  burning 80 seconds to hide the real error.
- `bringup.sh` died silently on a cloud-init failure — `set -e` on a
  non-zero `cloud-init status --wait`, with the box, the failing module,
  and the reason all left on the instance.
- `packing-density.sh` reported success while measuring nothing, so an
  unattended run would checkpoint "done" with every scale step still
  sized from the unvalidated 1,250 guess.
- The same script treated *absent* failure counters as failures. A
  counter selected by a failure label does not exist until it first
  fires, so a perfectly healthy fleet was indistinguishable from a broken
  one and **no step could ever report HEALTHY**.

**Wrong-address bugs.** Three variants of reaching for something that
could never answer.

- A manager bound to `0.0.0.0` with no `server.advertiseAddr` mints join
  tokens containing `http://127.0.0.1:4747`, so every remote worker dials
  its own loopback. Nothing on the manager side looks wrong; the evidence
  is only in the worker's journal. Tracked as a product fix in TODO.md.
- The sim fleet's systemd unit never received the manager address.
- Health probes hit ports that `security.tf` opens intra-rig only, from
  off-rig.

**Prerequisites that CI hides.** `clang`/`libclang-dev` for rusqlite's
`session` feature (bindgen), and `cargo` missing from a non-interactive
SSH PATH — both now in [`development.md`](development.md). Ubuntu 24.04
also dropped the `awscli` package outright.

**Silent data loss, caught before it bit.** `push`'s `rsync --delete`
would have deleted `terraform.tfstate`, orphaning the entire rig with
nothing left able to destroy it.

## What we got wrong

Recorded because the wrong turns cost more than the bugs, and the same
traps are still there.

1. **"The simulator's channel pool is the bottleneck."** Refuted by
   experiment: `SIM_NODES_PER_CHANNEL` 256 → 64 gave 250 nodes four
   connections instead of one, and cadence went 66% → 64%. Reverted.
2. **"~50ms raft commit."** `AppendEntries` p99 was 3ms. The 56ms was the
   *handler* p99 — queueing, not replication. The raft metric was in the
   same snapshot and went unread.
3. **"Offloading probably will not help much."** Reasoned from weak 1→4
   thread scaling (48→60/sec) that concurrency was not the constraint.
   Backwards: adding threads does not help when every thread is blocked.
   Offloading removes the blocking, and gave 7x.
4. **Monitoring that could not see a stall.** The first watcher only fired
   on new matching lines or process death. A run repeating
   `registered: 0/1000` produced neither, so a 20-minute stall looked
   exactly like progress until asked directly.

The through-line: every wrong turn came from reading one number and not
the one next to it.

## Where to pick up

1. **Land the liveness plan.** It is the architectural answer and it
   subsumes the remaining 16% gap at 4 threads (the single `pipeline_loop`
   OS thread per lane, plus the fsync).
2. **Re-measure phase 1 before trusting any campaign number.** The smoke
   cell's placement figures were contaminated by heartbeat cost; the fix
   and the NVMe change both landed after it ran.
3. **Re-run `packing-density.sh`.** The last measurement (125
   nodes/instance against the plan's 1,250) was taken against the
   *broken* heartbeat path, so it measured the ceiling, not the sim host.
   It should be much higher now, and every scale step's sizing depends on
   it — at 125 the 20k validation burst implies 160 instances and 1,280
   spot vCPU.
4. **File the spot vCPU increase** before any real campaign. On-demand
   quota is 1920 vCPU and fine; spot was never exercised beyond one
   instance.
5. Two hazards found in passing, both in TODO.md: `nodes_to_mark_lost`
   keys its grace off *process* uptime (so a manager acquiring status-lane
   leadership mid-life marks nodes lost immediately), and
   `DEFAULT_SELF_TERMINATE_AFTER` is 90s, which goes live the moment the
   heartbeat clamp widens past 30s.

## What the rig gained

- `scripts/bootstrap-operator-box.sh` — one command from an AWS key to a
  running phase 1, plus `stop`/`start`/`repair-iam`. Deliberately outside
  the rig's terraform: it runs that terraform and must outlive
  `teardown.sh`.
- `scripts/phase1.sh` — the six steps, checkpointed, so a failure resumes
  instead of restarting a 30-minute LTO build.
- `experiments/heartbeat-commit-probe.sh` — the whole control plane and
  fleet on one box. Answers engineering questions at $0.37/hr instead of
  the rig's $2.50/hr; it is what isolated storage as the variable.
- Memory as a first-class result (`memory.json` per cell, `used` vs
  `anon` split — page cache was ~40% of `used` on an idle control plane).
- 11 metric families for manager contention: coalescer batch size and
  wait, raft pipeline queue depth, runtime saturation. `docs/metrics.md`
  has a "Diagnosing a manager throughput ceiling" section with the read
  order. These would have turned this night into an afternoon.
