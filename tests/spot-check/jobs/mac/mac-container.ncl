# The local image dev loop on macOS: build with Apple `container`, run it under
# Legion, change code, rebuild, roll out.
#
#   container build -t spot-check-web:dev tests/spot-check/images/web
#   legion job run tests/spot-check/jobs/mac/mac-container.ncl
#   curl http://localhost:18081/
#   legion exec --job mac-container -it -- /bin/sh
#
# Each Apple container runs in its own lightweight VM, so the host port is
# published into it: host 18081 -> container 8080 (`to`).
{
  name = "mac-container",
  task = {
    name = "web",
    runtime = "container",
    lifecycle = "service",
    config = {
      image = "spot-check-web:dev",
      command = ["python3", "/app/app.py"],
      env = { GREETING = "from Apple container on macOS" },
    },
    network = { ports = [{ name = "http", static = 18081, to = 8080 }] },
    checks = [
      { type = "health", kind = "http", path = "/healthz", port = "http", every = "5s", timeout = "2s" },
    ],
    resources = { memory = 256 },
    constraints = { os = "macos" },
  },
}
