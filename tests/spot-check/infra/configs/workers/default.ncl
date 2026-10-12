# The one worker type in the spot-check rig. Managers read this from
# workerConfigPath; join tokens minted with `--type default` apply it.
{
    workerType = "default",
    pool = "default",
    additionalNodeChecks = [],
    attributes = {},
    # Directories on the EBS data disk that worker.sh mounts at /data. Jobs
    # claim them by name with `driver = "host"` (docs/volumes.md).
    hostVolumes = {
        redisdata = { path = "/data/redis", create = true },
        pipeline = { path = "/data/pipeline", create = true },
    },
    # Enforce declared memory and CPU on exec tasks (cgroup v2), so the spot
    # check exercises the enforced path rather than the estimate-only default.
    resourceEnforcement = {
        enabled = true,
        cgroupParent = "legion",
    },
}
