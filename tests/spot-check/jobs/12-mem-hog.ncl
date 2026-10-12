# The memory-pressure drill (RUNBOOK 12.8, smoke.sh sandbox-pressure): one
# host process per worker that keeps allocating until the node is nearly out
# of memory, so the worker's pressure response has to act
# (docs/resource-enforcement.md, "Memory pressure").
#
#   legion job run tests/spot-check/jobs/12-mem-hog.ncl
#   legion node usage sc-wkr-1                   # PRESSURE column, then the order
#   legion events list --kind node.pressure
#   legion job stop mem-hog                      # always, when you are done
#
# Expected order on each node, from the response's steps: `reclaim` (frees
# little: neither the hog's anonymous memory nor a guest's is reclaimable
# without swap, and the balloon only takes what a guest uses above its
# reservation), then `suspend` for any running `sandboxes/ws` instance
# (it sets suspend.onPressure), then `evict-bursting` for this hog, which is
# far above its 128 MiB reservation. Nothing within its reservation is
# evicted: the rig does not oversubscribe.
#
# The hog grows 64 MiB every half second while the node's MemAvailable is
# above 200 MiB, then holds, so it keeps the node under the default trigger
# (5% of memory, about 390 MiB on an m7i.large) until something gives.
# Once evicted it is rescheduled and grows again, so stop the job after the
# first round of events. `background` is the lowest band: the hog is the
# first thing any step may take.
{
  name = "mem-hog",
  priority = "background",
  task = {
    name = "hog",
    runtime = "exec",
    lifecycle = "service",
    # One per worker, the way envelopes are placed.
    count = { num = 1, per = "id" },
    config = {
      command = [
        "python3",
        "-u",
        "-c",
        m%"
          import time
          def available_mib():
              for line in open("/proc/meminfo"):
                  if line.startswith("MemAvailable:"):
                      return int(line.split()[1]) // 1024
              return 0
          chunks = []
          while True:
              free = available_mib()
              if free > 200:
                  chunks.append(bytearray(64 * 1024 * 1024))
                  print("hog holds", len(chunks) * 64, "MiB; node MemAvailable", free, "MiB", flush=True)
              time.sleep(0.5)
        "%,
      ],
    },
    # Reserves 128 MiB, may burst to 7 GiB: everything above 128 MiB is what
    # `evict-bursting` looks at.
    resources = { cpu = 100, memory = 128, memoryMax = 7168 },
    constraints = { os = "linux" },
  },
}
