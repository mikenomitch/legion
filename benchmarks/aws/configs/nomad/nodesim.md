# Simulated Nomad clients (nomad-nodesim)

The Nomad campaign's fake fleet is
[nomad-nodesim](https://github.com/hashicorp-forge/nomad-nodesim), the
tool HashiCorp used for C2M and the ecosystem counterpart of Legion's
`--simulate` and K8s' KWOK.

`scripts/install-nomad.sh` builds it once per pinned ref (Go from go.dev
on the loadgen box, binary cached in `s3://$BUCKET/bin/nomad-nodesim`),
and `scripts/start-sim-fleet.sh nomad <total-nodes> <scenario>` runs it
on each worker instance as one `nomad-nodesim@<profile>` unit per node
profile:

```hcl
# /etc/nodesim/web-east.hcl, rendered per instance slice
node_name_prefix = "nsim-0-web-east"
node_num         = 469
server_addr      = ["10.0.1.11:4647", "10.0.1.12:4647", "10.0.1.13:4647"]
work_dir         = "/var/lib/nodesim/web-east"
node {
  datacenter = "dc1"
  node_class = "web"
  resources {
    cpu_compute = 24000 # 8 cores x 3000 MHz, the same units as Legion's sim fingerprint
    memory_mb   = 32768
  }
}
```

Notes:

- **One process per profile** because nodesim gives every client in a
  process the same class and size. The split comes from
  `workloads/profile_split.py`, the same rule the other two fleets use.
- **No node meta.** nodesim sets `node_class` but not `meta`, so only
  class constraints are portable. The translator refuses others.
- **Driver.** nodesim's plugin catalog advertises Nomad's mock driver,
  registered as `mock_driver`. The translated jobs ask for exactly that
  name; `mock` matches no node.
- **Server limits.** `configs/nomad/server.hcl.tpl` lifts the per-IP
  connection caps, because ~1,250 clients share each worker's IP.
- **Version pin.** The nodesim ref and the Nomad release are in
  `scripts/versions.env` and land in every cell's fairness record.
