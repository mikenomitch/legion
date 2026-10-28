# A standalone scaling policy for a target outside Legion (docs/autoscaling.md,
# "Standalone policies").
#
#   legion scaling apply configs/scaling/render-farm.ncl --wait
#
# It scales a render farm through the `render-exec` target and reads its
# queue from Prometheus; both are installed in the autoscaler's app config
# (autoscaler-app-config.ncl) and open to the `media` namespace. Applying it
# needs `scale:write` in `media`.
{
  name = "render-farm",
  namespace = "media",
  description = "GPU render workers, one per 10 queued frames",
  target = {
    plugin = "render-exec",
    # Passed to the target as is: the policy's data, not code.
    config = { pool = "gpu" },
  },
  # Required, as replica counts: an external target has no `count`.
  min = 0,
  max = 50,
  checks = [
    {
      name = "queue",
      source = "team-prom",
      query = "sum(render_queue_depth)",
      window = "2m",
      strategy = { name = "per-replica", value = 10 },
      # Sleep at zero when the queue is empty; wake when a frame arrives.
      activation = 0,
    },
  ],
  down = { stabilize = "10m" },
  mode = "dry-run",
}
