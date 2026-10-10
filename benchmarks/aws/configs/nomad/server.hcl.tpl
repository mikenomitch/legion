# Nomad server config for the comparison campaign (3 raft servers on the
# same boxes Legion used). Rendered per server by
# scripts/install-nomad.sh with envsubst: PRIVATE_IP, CP0_IP, CP1_IP,
# CP2_IP.
#
# Fairness rule (aws-comparative-benchmark-plan.md): default configs plus
# documented, defensible tuning only. Every non-default below is called
# out; the exact rendered files ship in the campaign's fairness record.

datacenter = "dc1"
data_dir   = "/var/lib/nomad"
bind_addr  = "0.0.0.0"

advertise {
  http = "${PRIVATE_IP}"
  rpc  = "${PRIVATE_IP}"
  serf = "${PRIVATE_IP}"
}

server {
  enabled          = true
  bootstrap_expect = 3

  server_join {
    retry_join = ["${CP0_IP}", "${CP1_IP}", "${CP2_IP}"]
  }
}

# NON-DEFAULT (documented): Prometheus-format telemetry so the same
# Prometheus scrapes Nomad's scheduler internals
# (nomad.nomad.worker.*, plan-queue depth) that it scrapes from Legion.
telemetry {
  prometheus_metrics         = true
  publish_allocation_metrics = true
  publish_node_metrics       = true
  collection_interval        = "5s"
}

# NON-DEFAULT (documented): no per-client connection caps. Nomad limits
# each remote IP to 100 HTTP and 100 RPC connections by default, and the
# rig puts ~1,250 simulated clients behind each worker instance's single
# IP. With the caps on, registration stalls at 100 nodes per instance —
# an artifact of simulating, not a property of Nomad. nomad-nodesim's own
# README says the same ("Disable limits! Otherwise you won't scale far").
# Legion and K8s have no equivalent per-IP cap to lift.
limits {
  http_max_conns_per_client = 0
  rpc_max_conns_per_client  = 0
}

# Everything else is stock, including the scheduler algorithm (binpack).
# Each system runs its own default placement policy; the fairness record
# names it rather than tuning the three toward one another.
