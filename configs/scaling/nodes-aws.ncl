# A node-group policy (docs/autoscaling.md, "Capacity demand").
#
# Today this file sizes the group: `legion scaling demand -f
# configs/scaling/nodes-aws.ncl` reports how many nodes of this shape the
# work the scheduler could not place needs. Running the group (minting its
# join tokens, asking the provider for machines, draining and forgetting
# them) is not built yet.
#
# Every field but `name`, `target` and `max` is optional; the values below
# other than those are the defaults, written out.
{
  name = "workers-general",
  target = {
    nodes = {
      pool = "default",
      workerType = "default",
      # What a member looks like before one has joined (to scale from zero).
      # `attributes` are what constraints and `distinct` see; every member
      # also carries `legion.nodegroup = "workers-general"`, stamped by the
      # group's join token.
      shape = { cpu = 8000, memory = 16384, attributes = { arch = "amd64" } },
      # The target plugin that starts and stops machines, and its settings.
      plugin = "aws-asg",
      config = { asg = "legion-workers" },
    },
  },
  min = 0,
  max = 20,
  up = {
    step = { percent = 100, replicas = 5 },
    every = "2m",
    joinTimeout = "10m",
  },
  down = {
    when = "empty",
    for = "10m",
    step = { percent = 10, replicas = 1 },
    every = "5m",
    drainDeadline = "10m",
  },
  expander = "least-waste",
  mode = "enforce",
}
