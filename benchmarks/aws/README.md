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

## Layout

| Path | What |
| --- | --- |
| `terraform/` | The rig: VPC, 3× on-demand control plane, loadgen box, spot sim-worker ASG, S3 bucket, IAM. System-agnostic — per-system installs are scripts, so the same boxes serve every campaign serially. |
| `configs/` | Rendered-per-campaign config templates: Legion A (raft lanes), Legion B (streaming + S3 lease), Nomad servers, kubeadm + KWOK, Prometheus + Grafana stack. |
| `workloads/` | `translate.py`: scenario JSON → Legion jobspecs / Nomad jobs / K8s Deployments, plus the driver's expectation manifest. `profile_split.py`: the shared fleet-shape split. |
| `driver/` | `driver.py`: submits, watches placements, computes throughput/latency/recovery. `tests/`: the fixture suite that keeps the counting math honest. |
| `scripts/` | The runbook, as executables (below). |
| `experiments/` | Piggyback drills: storm, failover, streaming RPO, 24h soak, upgrade-under-load. See `experiments/README.md`. |
| `dashboards/` | Grafana boards provisioned onto the loadgen box. |
| `analysis/` | `fit_curves.py`: scaling fits + 20k/50k projections with prediction intervals and validation deltas. |
| `results/` | Campaign archives land here (gitignored) and in `s3://<bucket>/campaigns/`. |

## The runbook

```bash
cd benchmarks/aws

# 0. Prove the loop locally — no AWS, no cost. Run this after any
#    driver/translator change; CI runs the fixture tests on PRs.
scripts/local-smoke.sh

# 1. Configure + stand up the rig (~$0.75/hr idle-ish, see plan).
cp terraform/terraform.tfvars.example terraform/terraform.tfvars   # edit
scripts/bringup.sh
scripts/build-and-upload.sh          # dist binary + plugins -> S3

# 2. Legion campaign, both HA modes. One command each: scales the spot
#    fleet per step, starts sim fleets, drives every workload, collects,
#    archives. Start with --smoke (1k nodes, one workload).
scripts/install-legion-a.sh
scripts/run-campaign.sh --system legion-a --smoke
scripts/run-campaign.sh --system legion-a
scripts/install-legion-b.sh
scripts/run-campaign.sh --system legion-b

# 3-4. Competitor campaigns (same boxes, serially — phase 3/4).
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
- **Driver-side clocks.** Submit and placed timestamps are taken by the
  same driver process for every system; no cross-host clock skew enters
  a latency, and all systems pay the same stream-delivery latency.
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
- `collect.sh` snapshots Prometheus over a trailing window, not the
  exact cell window; the driver's own timestamps are authoritative for
  the reported numbers.
