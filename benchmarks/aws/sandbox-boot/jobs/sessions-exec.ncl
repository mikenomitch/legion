# A session task on the exec runtime: a host process that sleeps. The
# runtime's own start costs a fork and an exec, so this measures the control
# plane and the node's admission path rather than a VMM boot.
#
# Each c8i.xlarge worker (4 vCPU, 8 GiB) keeps 6 GiB of envelope, so 768
# sessions of 8 MiB fit per node.
{
  name = "bench-exec",
  tasks = [{
    name = "shell",
    runtime = "exec",
    config = { command = ["sh", "-c", "exec sleep 86400"] },
    resources = { cpu = 4, memory = 8 },
    stateful = {
      mode = "session",
      envelope = { resources = { cpu = 3200, memory = 6144 } },
    },
  }],
}
