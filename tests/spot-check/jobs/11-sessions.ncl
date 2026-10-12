# Session mode (docs/session-mode.md): unnamed, node-held units that start
# with no raft write. For RUNBOOK 12.9-12.11 and smoke.sh's sessions and
# sessions-edge scenarios.
#
#   legion job run tests/spot-check/jobs/11-sessions.ncl
#   legion job status sessions                  # one envelope per worker per task
#   legion instance create sessions --task shell -j   # an unnamed session
#   legion instance list sessions -j            # complete, asOf, the sessions
#   legion instance promote <session-id> --id promoted-1 -j
#   legion instance stop <session-id>
#
# Two tasks:
#
# - `shell`: a host process (`exec`) that sleeps. Cheap, so a burst of starts
#   and a worker restart with dozens of sessions are quick to run.
# - `vm`: the same as a cloud-hypervisor microVM with no NIC, to check that a
#   sandbox runs as a session too.
#
# Each task's envelope reserves room on every worker whether or not sessions
# use it (1 GiB per task per worker here). Stop this job before the
# preemption drill (RUNBOOK 12.8), which needs that room.
let images = (import "lib/images.ncl") vars in
{
  name = "sessions",
  tasks = [
    {
      name = "shell",
      runtime = "exec",
      config = {
        command = [
          "sh",
          "-c",
          "echo \"session $LEGION_INSTANCE_ID mode=$LEGION_INSTANCE_MODE\"; exec sleep 86399",
        ],
      },
      # `sleep 86399`, not 86400: a marker `pgrep -x -f 'sleep 86399'` finds,
      # so the worker-restart check can compare session pids before and after.
      # One session's size, in the envelope's units: 32 per worker by memory.
      resources = { cpu = 20, memory = 32 },
      stateful = {
        mode = "session",
        envelope = { resources = { cpu = 1000, memory = 1024 } },
      },
    },
    {
      name = "vm",
      runtime = "sandbox",
      config = {
        image = images.agent,
        command = [
          "/bin/sh",
          "-c",
          "echo \"vm session $LEGION_INSTANCE_ID up\"; exec sleep 86400",
        ],
        vcpus = 1,
        hypervisor = "cloud-hypervisor",
      },
      # Four microVM sessions per worker.
      resources = { cpu = 250, memory = 256 },
      stateful = {
        mode = "session",
        envelope = { resources = { cpu = 1000, memory = 1024 } },
      },
      constraints = { os = "linux" },
    },
  ],
}
