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

# Everything else is stock. Scheduler config (e.g. spread vs binpack) is
# set at runtime by the campaign scripts via
# `nomad operator scheduler set-config` and recorded per run.
