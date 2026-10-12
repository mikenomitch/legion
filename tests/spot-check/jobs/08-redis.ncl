# Stateful Redis on AWS storage: AOF persistence on the worker's EBS data disk
# (mounted at /data by the rig), claimed through a `host` volume named in the
# worker config. The instance is pinned to the node that holds its data.
#
#   legion job run tests/spot-check/jobs/08-redis.ncl
#   legion action run redis cli-cmd --param cmd="SET greeting hello"
#   legion job run tests/spot-check/jobs/08-redis.ncl --force-reschedule
#   legion action run redis cli-cmd --param cmd="GET greeting"    # still hello
#   tests/spot-check/infra/ssh.sh <its worker> sudo reboot             # survives too
#   legion volume list --job redis
{
  name = "redis",
  task = {
    name = "store",
    runtime = "container",
    lifecycle = "service",
    count = 1,
    stateful = {
      mode = "slot",
      storage = { path = "/data", driver = "host", params = { name = "redisdata" } },
    },
    config = {
      image = "public.ecr.aws/docker/library/redis:7-alpine",
      # appendonly is the point: without it, a restart loses everything
      # written since the last snapshot. Runs as root so it can write the
      # host directory without a chown.
      command = [
        "sh",
        "-c",
        "exec redis-server --dir /data --appendonly yes --appendfsync everysec --port \"$LEGION_PORT_REDIS\" --protected-mode no",
      ],
    },
    network = { ports = [{ name = "redis", static = 6379 }] },
    service = { name = "redis", port = "redis" },
    checks = [{ type = "health", kind = "tcp", port = "redis", every = "10s", timeout = "3s" }],
    actions = [
      {
        name = "cli-cmd",
        description = "Run one redis command and print the reply",
        command = ["sh", "-c", "exec redis-cli -p \"$LEGION_PORT_REDIS\" ${param.cmd}"],
        params = [
          {
            name = "cmd",
            description = "Command to run, e.g. \"GET greeting\"",
            # Lands inside sh -c, so allow nothing that could break out.
            pattern = "[A-Za-z0-9 ._:*-]+",
          },
        ],
        timeout = "30s",
      },
      {
        name = "cli",
        description = "Interactive redis-cli",
        command = ["sh", "-c", "exec redis-cli -p \"$LEGION_PORT_REDIS\""],
        tty = true,
      },
    ],
    resources = { cpu = 250, memory = 256 },
  },
}
