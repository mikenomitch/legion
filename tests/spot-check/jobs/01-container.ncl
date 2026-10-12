# Basic container: a small HTTP service from a private ECR image, one replica
# per worker on a fixed port.
#
#   legion job run tests/spot-check/jobs/01-container.ncl
#   curl http://<worker-public-ip>:8081/          # either worker
#   legion job logs hello-container -f            # one line per request
#   legion exec --job hello-container -it -- /bin/sh
#   legion action run hello-container env
#
# Exercises: private-registry pull (KV-held ECR credential), static ports and
# their spread (two replicas cannot share 8081 on one node), HTTP health
# checks, logs, exec, and actions.
let images = (import "lib/images.ncl") vars in
{
  name = "hello-container",
  task = {
    name = "web",
    runtime = "container",
    lifecycle = "service",
    count = 2,
    config = {
      image = images.web,
      command = ["python3", "/app/app.py"],
      env = { GREETING = "from a Legion container" },
    },
    network = { ports = [{ name = "http", static = 8081 }] },
    service = { port = "http", meta = { protocol = "http" } },
    checks = [
      { type = "health", kind = "http", path = "/healthz", port = "http", every = "5s", timeout = "2s" },
    ],
    actions = [
      {
        name = "env",
        description = "Print the Legion-provided environment",
        command = ["sh", "-c", "env | grep ^LEGION_ | sort"],
      },
    ],
    resources = { cpu = 250, memory = 128 },
  },
}
