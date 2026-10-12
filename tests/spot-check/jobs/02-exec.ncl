# Basic exec: a host process with no image, one replica per worker on a fixed
# port. The worker enforces the declared memory as a hard cgroup limit
# (resourceEnforcement is on in the rig's worker config).
#
#   legion job run tests/spot-check/jobs/02-exec.ncl
#   curl http://<worker-public-ip>:8082/
#   legion job logs hello-exec -f
#   legion exec --job hello-exec -it -- /bin/bash   # a shell on the HOST
#   legion exec --job hello-exec -- cat /proc/self/cgroup
#
# One replica per node on a fixed port, so on a single machine (macOS) pass
# --var-code count=1.
let params = { count | Number | default = 2 } & vars in
{
  name = "hello-exec",
  task = {
    name = "web",
    runtime = "exec",
    lifecycle = "service",
    count = params.count,
    config = {
      # exec does not run argv through a shell, so variables only expand
      # inside an explicit `sh -c`.
      command = [
        "sh",
        "-c",
        m%"
          mkdir -p "$LEGION_TASK_DIR/www"
          cd "$LEGION_TASK_DIR/www"
          cat > index.html <<EOF
          hello from a Legion exec task
          node: $(hostname)  alloc: $LEGION_ALLOC_ID  kernel: $(uname -r)
          EOF
          exec python3 -m http.server "$LEGION_PORT_HTTP"
        "%,
      ],
      env = { PYTHONUNBUFFERED = "1" },
    },
    network = { ports = [{ name = "http", static = 8082 }] },
    service = { port = "http" },
    checks = [{ type = "health", kind = "tcp", port = "http", every = "5s", timeout = "2s" }],
    restart = { policy = "always", delay = "3s" },
    resources = { cpu = 100, memory = 64 },
  },
}
