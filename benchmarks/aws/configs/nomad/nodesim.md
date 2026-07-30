# Simulated Nomad clients (nomad-nodesim)

The Nomad campaign's fake fleet is
[nomad-nodesim](https://github.com/hashicorp-forge/nomad-nodesim) — the
same tool HashiCorp used for C2M, and the ecosystem counterpart of
Legion's `--simulate` and K8s' KWOK.

`scripts/install-nomad.sh` builds it once (Go toolchain on the loadgen
box, binary uploaded to `s3://$BUCKET/bin/nomad-nodesim`) and
`scripts/start-sim-fleet.sh nomad <total-nodes>` runs it on each worker
instance:

```bash
nomad-nodesim \
  -server-addr "$CP0_IP:4647" \
  -node-num "$NODES_PER_INSTANCE" \
  -work-dir /var/lib/nodesim
```

Notes for the campaign (verify against the pinned nodesim revision when
the Nomad campaign starts — phase 3):

- Node resources: nodesim registers a fixed node fingerprint per
  process; shaping it to match the Legion `node_profiles` classes is done
  with its node-template config file. The translator emits
  `workloads/out/nomad/nodesim-<profile>.json` templates for this.
- Driver support: nodesim nodes advertise the `mock` driver; the
  translated jobs use it (`translate.py --system nomad`), which is what
  keeps Nomad's placements pure-scheduler like Legion's noop runtime.
- Heartbeats: nodesim heartbeats at the server-directed interval, like a
  real client — no cadence tuning on either side.
- Version pin: record the nodesim commit hash and the Nomad release in
  the campaign fairness record (`scripts/collect.sh` does both).
