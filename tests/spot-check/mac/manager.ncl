# Local development on macOS: one manager with an in-process worker, no ACLs,
# loopback only.
#
#   LEGION_DATA_DIR=/tmp/legion-sc legion start manager --run-worker -c tests/spot-check/mac/manager.ncl
{
  server = { port = 4747, host = "127.0.0.1" },
  # Relative paths resolve against the directory you start the manager from,
  # so run it from the repo root.
  workerConfigPath = "./tests/spot-check/mac/workers/",
  telemetry = { enabled = true, port = 4749 },
}
