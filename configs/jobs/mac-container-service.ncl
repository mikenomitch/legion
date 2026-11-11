# Long-running container service on macOS (Apple `container` runtime).
#
# Exercises the parts of the container runtime that a batch task does not:
# dynamic port publishing, health checks against the published port, service
# discovery, exec, and declared actions.
#
# Prerequisites on the worker node:
#   brew install container
#   container system start
#
# Run and inspect:
#   legion job run configs/jobs/mac-container-service.ncl
#   legion service resolve mac-web
#   curl "$(legion service resolve mac-web -j | jq -r '.[0] | "\(.address):\(.port)"')"
#   legion exec <alloc-id> -- nginx -v
#   legion exec <alloc-id> -it -- /bin/sh
#   legion action run <alloc-id> reload
#
# The container gets its own IP inside a VM, so the assigned host port is
# published into it (`--publish <host>:80`). Without that, neither the health
# check nor a service consumer could reach the workload.
{
  name = "mac-container-service",
  task = {
    name = "web",
    runtime = "container",
    lifecycle = "service",
    config = {
      image = "nginx:alpine",
      # nginx's own entrypoint is replaced, so run it in the foreground: a
      # container task is supervised by the process Legion spawns.
      command = ["nginx", "-g", "daemon off;"],
    },
    network = {
      # Dynamic host port, forwarded to nginx's port 80 inside the container.
      ports = [{ name = "http", to = 80 }],
    },
    service = {
      name = "mac-web",
      port = "http",
      tags = ["demo", "macos"],
    },
    checks = [
      {
        type = "health",
        kind = "http",
        path = "/",
        port = "http",
        every = "5s",
        timeout = "2s",
        unhealthyAfterCount = 3,
      },
    ],
    actions = [
      {
        name = "reload",
        description = "Reload nginx configuration without dropping connections",
        command = ["nginx", "-s", "reload"],
      },
      {
        # Actions run to completion and stream their output back, so they
        # must terminate: nginx's own access.log is a symlink to /dev/stdout,
        # and tailing that would hang the caller.
        name = "show-config",
        description = "Print the first N lines of the running nginx config",
        command = ["head", "-n", "${param.lines}", "/etc/nginx/nginx.conf"],
        params = [
          { name = "lines", type = "int", "default" = "20", min = 1, max = 1000 },
        ],
      },
    ],
    resources = {
      memory = 512,
      cores = 1,
    },
    constraints = {
      os = "macos",
    },
    count = 1,
  },
}
