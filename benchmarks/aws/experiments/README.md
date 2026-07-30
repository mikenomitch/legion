# Piggyback experiments

The rig's marginal-cost-zero add-ons from
`internal_docs/plans/aws-comparative-benchmark-plan.md` §"Piggyback
experiments": each script runs against an already-standing campaign
environment and lands its results in the same `results/<campaign>/`
archive shape as a campaign cell, so the analysis and the fairness
record treat them uniformly.

All of these use `driver.py watch` — placements observed and the fault
injected from one process, so gap/recovery math never crosses a clock
boundary.

| Script | Measures | Systems |
| --- | --- | --- |
| `storm.sh` | Time-to-100%-replaced after killing ~10% of nodes at once | all four |
| `failover-drill.sh` | Placement-gap seconds after killing the leader/primary mid-load | legion-a, legion-b, nomad, k8s |
| `streaming-rpo.sh` | Lease-failover gap + RPO (writes not yet shipped) | legion-b |
| `soak-24h.sh` | Drift, leaks, eval-queue growth, store growth over 24h of mixed+churn | legion-a or -b |
| `upgrade-under-load.sh` | Placement disruption during a rolling manager upgrade | legion-a |

## Methodology notes (read before running)

**storm.sh** — kills whole sim *processes* (one instance slice ≈ the
target percentage), which is exactly how a rack loss presents: N nodes
stop heartbeating at once, the manager marks them lost on its 10s
schedule, and reschedules everything they held. The measured number is
`storm.recovery_secs` from the fault to the last replacement placement.
Expected replacements = allocations resident on the killed slice at
kill time (the script snapshots that via the state query first).
This is where schedulers actually differ; the 5k-node step is the
plan's designated storm cell.

**failover-drill.sh** — the fault is `systemctl stop` on the raft
leader (legion-a), the lease primary (legion-b), the Nomad leader, or
the active kube-scheduler; a background submit loop keeps a trickle of
new jobs flowing so the placement gap is observable. Reported number is
`storm.placement_gap_secs` — how long placements stopped. For legion-b
the gap includes lease TTL expiry (15s) by design; report it with the
lease parameters, not as a like-for-like raft comparison.

**streaming-rpo.sh** — a writer stamps sequenced KV keys at a fixed
rate against the streaming primary, the script kills the primary
process, and after the lease fails over it reads back the highest
surviving sequence. RPO = (last written − last survived) / write rate.
Also records the failover gap (first successful write post-kill). This
feeds the durability numbers in docs/persistence-streaming.md; the
plan's rule about honest limitations applies — publish RPO with the
`syncInterval` it was measured at.

**soak-24h.sh** — mixed+churn at 2k nodes for 24h (the plan's soak
shape). Per-hour snapshots: store file size on every manager,
eval-queue depth, RSS of the manager process, node/allocation counts,
plus a full Prometheus range export at the end. Failure criteria are
drift ones: monotonically-growing store without vacuum, eval queue that
never returns to zero, RSS growth without plateau.

**upgrade-under-load.sh** — per docs/upgrades.md order (managers one at
a time, verify quorum between steps) with the steady workload running;
`driver.py watch` spans the whole rolling window, and the disruption
number is the max placement gap observed during it.

## Results

Each script writes `results/<campaign>/experiments/<name>-<timestamp>/`
with the driver run dir, a `fault.json` (what was injected, when), and
the experiment-specific readings. `collect.sh`-style archiving to S3
happens at the end of each script.
