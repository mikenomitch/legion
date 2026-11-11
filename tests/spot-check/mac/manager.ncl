# Local development on macOS: one manager with an in-process worker, loopback
# only. ACLs are on (the default): at first start the manager writes the
# initial management token to <dataDir>/bootstrap-token and to
# ~/.legion/tokens/127.0.0.1_4747, which the CLI reads for this address.
#
#   LEGION_DATA_DIR=/tmp/legion-sc legion start manager --run-worker -c tests/spot-check/mac/manager.ncl
#   set -x LEGION_TOKEN (cat /tmp/legion-sc/bootstrap-token)   # only if the CLI says Unauthenticated
{
  server = { port = 4747, host = "127.0.0.1" },
  # Relative paths resolve against the directory you start the manager from,
  # so run it from the repo root.
  workerConfigPath = "./tests/spot-check/mac/workers/",
  telemetry = { enabled = true, port = 4749 },
}
