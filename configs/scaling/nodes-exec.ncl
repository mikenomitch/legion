# A node group on a scripted provider (docs/autoscaling.md, "Node groups").
#
#   legion scaling apply configs/scaling/nodes-exec.ncl
#
# `scripted` names a target plugin the operator installs in the autoscaler's
# app config, here the `exec` builtin, which declares the `nodes` capability:
#
#   plugins = { scripted = { builtin = "exec", config = { command = ["/opt/legion/nodes.sh"] } } }
#
# An `exec` target calls its command with the operation appended (a script's
# `$1`) and a JSON request on stdin, and reads JSON from stdout:
#
#   status     -> { "count": 3, "members": [{ "id": "vm-1", "state": "running" }] }
#   scale      <- { "desired": 5, "decision": "d-…", "join": { "token": "…", "attributes": { … } } }
#   terminate  <- { "ids": ["vm-2"], "decision": "d-…" }
#
# `scale` starts machines that run `legion join <token> --attr
# legion.provider-id=<their id>`; `terminate` stops exactly the ids given
# (an id already gone is success). `config` below is passed to the script as
# data. tests/integration/node_groups.rs drives a group like this one with a
# scripted provider.
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
