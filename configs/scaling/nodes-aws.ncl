# A node group on AWS (docs/autoscaling.md, "Node groups";
# docs/autoscaling-plugins.md, "The aws-asg plugin").
#
#   legion scaling apply configs/scaling/nodes-aws.ncl
#
# The autoscaler grows the Auto Scaling group `legion-workers` when work is
# waiting that its nodes could host, and drains, terminates, and forgets
# members that sit empty. It needs the `aws-asg` plugin installed in its app
# config (see configs/scaling/autoscaler-app-config.ncl), whose credentials
# come from the app's workload identity, never from this file.
#
# New instances join with the token the plugin writes to
# `joinTokenParameter` before each scale-up; their launch template's user
# data runs, for example:
#
#   token=$(aws ssm get-parameter --with-decryption \
#     --name /legion/workers-general/join --query Parameter.Value --output text)
#   legion join "$token" --attr legion.provider-id=$(ec2-metadata -i | cut -d' ' -f2)
#
# A node group is cluster-scoped: applying it needs operator:write and
# node:write. Every field but `name`, `target` and `max` is optional; the
# values below other than those are the defaults, written out.
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
      config = {
        asg = "legion-workers",
        joinTokenParameter = "/legion/workers-general/join",
      },
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
