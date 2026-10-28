# A node-group policy on a scripted provider (docs/autoscaling.md,
# "Capacity demand"; plan §6.11).
#
# This version of Legion does not run node groups: `legion scaling apply`
# refuses this file, and `legion scaling demand -f
# configs/scaling/nodes-exec.ncl` sizes the group from the work the
# scheduler could not place. `scripted` names a target plugin the operator
# installs in the autoscaler's app config, here an `exec` target:
#
#   plugins = { scripted = { builtin = "exec", config = { command = ["/opt/legion/nodes.sh"] } } }
#
# An `exec` target calls its command with the operation appended (a
# script's `$1`) and a JSON request on stdin, and reads JSON from stdout: `status`
# answers `{ "count": 3, "members": [{ "id": "vm-1" }] }`.
{
  name = "workers-scripted",
  target = {
    nodes = {
      pool = "default",
      workerType = "default",
      shape = { cpu = 4000, memory = 8192 },
      plugin = "scripted",
      config = { image = "legion-worker-2026-10" },
    },
  },
  min = 0,
  max = 10,
  up = { step = { replicas = 2 }, every = "2m", joinTimeout = "10m" },
  down = { when = "empty", for = "10m", step = { replicas = 1 }, every = "5m" },
}
