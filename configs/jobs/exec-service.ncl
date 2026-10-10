# A host process run directly by the `exec` runtime, no container image.
# The binary must already exist on the worker (here, python3).
#
#   legion job run configs/jobs/exec-service.ncl
#   legion job logs exec-service
{
  name = "exec-service",
  task = {
    name = "http",
    runtime = "exec",
    lifecycle = "service",
    config = {
      command = ["sh", "-c", "exec python3 -m http.server $LEGION_PORT_HTTP"],
      env = { PYTHONUNBUFFERED = "1" },
    },
    network = { ports = [{ name = "http" }] },
    service = { port = "http" },
    # Restart on any exit, waiting a few seconds between attempts.
    restart = { policy = "always", delay = "3s" },
    resources = { cpus = 0.1, memory = 64 },
  },
}
