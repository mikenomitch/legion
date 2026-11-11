# Run untrusted Python snippets as isolates, with a microVM for the ones
# that need a real machine.
#
# Docs = docs/task-runtime-isolate.md
#
#   legion job validate configs/jobs/isolate.ncl
#   legion job run configs/jobs/isolate.ncl         # places the envelopes only
#   legion session run code-runner --task run --file main.py --input '{"n": 3}'
#   legion session run code-runner --task run --file main.py --tier vm
#
# `run` is the isolate task: each run gets a fresh Python isolate (CPython
# compiled to WebAssembly) in its node's `legion-isolate` host, started in
# one to three milliseconds (tens of microseconds for a small compiled
# component). A run that needs what an isolate lacks (native packages,
# subprocesses, a persistent filesystem: an isolate has only an in-memory
# scratch one) goes to `machine`, a sandbox session task that takes runs,
# either because the caller asked (`--tier vm`) or because the node's
# pre-flight check or its memory of earlier runs said so (`tier = "auto"`,
# the default).
#
# Worker setup: workers need the `legion-isolate` binary beside the worker
# (or LEGION_ISOLATE_BINARY) to advertise the `isolates` capability, and
# KVM plus a VMM for the sandbox (docs/task-runtime-sandbox.md).
{
  name = "code-runner",
  tasks = [
    {
      name = "run",
      runtime = "isolate",
      config = {
        # A built-in interpreter image. No `entry`: each run brings its own
        # code as its main file (main.py, else the first file it stages).
        image = "python",
        # A fresh instance per run: nothing one caller does is visible to
        # the next.
        lifetime = "request",
        # Required budgets per run. A run past one stops, says which, and
        # replies nextTier = "vm" so the caller can retry it there.
        limits = { cpu = "2s", wall = "30s", output = "256KiB" },
        # Where `tier = "vm"` runs go: `machine` below.
        vm = { task = "machine" },
      },
      # Each resident isolate reserves 16 MiB of the envelope and may grow
      # its linear memory to 256 MiB.
      resources = { cpu = 10, memory = 16, memoryMax = 256 },
      stateful = {
        mode = "session",
        # The room each node keeps for this task's isolates: about 2,000
        # resident isolates at 16 MiB each.
        envelope = { resources = { cpu = 16000, memory = 32768 } },
      },
      # Lets this task's isolates start `machine` too, and is required for
      # config.vm.task.
      children = { tasks = ["machine"] },
    },
    {
      name = "machine",
      runtime = "sandbox",
      config = {
        image = "python:3.13-slim",
        command = ["sleep", "infinity"],
        vcpus = 1,
        # No network for the VM: a gated sandbox session is not available
        # yet, and a snippet does not need one.
        network = { mode = "none" },
      },
      resources = { cpu = 1000, memory = 1024 },
      stateful = {
        mode = "session",
        envelope = {
          resources = { cpu = 8000, memory = 16384 },
          max = { cpu = 32000, memory = 65536 },
        },
      },
      # A VM run stages the isolate run's code as /legion/run/main.py.
      run = { command = ["python3", "/legion/run/main.py"], wall = "5m" },
    },
  ],
}
