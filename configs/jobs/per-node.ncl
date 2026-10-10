# One instance on every node, like a Kubernetes DaemonSet or a Nomad system
# job. `per = "id"` groups nodes by their unique ID, so `num = 1` places
# exactly one allocation per node, including nodes that join later.
#
#   legion job run configs/jobs/per-node.ncl
#   legion job info node-agent
{
  name = "node-agent",
  task = {
    name = "agent",
    runtime = "exec",
    lifecycle = "service",
    count = { num = 1, per = "id" },
    config = {
      command = ["sh", "-c", "while true; do echo \"heartbeat from $(hostname)\"; sleep 30; done"],
    },
    resources = { cpus = 0.05, memory = 32 },
  },
}
