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
    # The bridge worker.sh builds for the HTTP-reachable microVM test, and the
    # pool Legion leases its guest from (docs/task-runtime-sandbox.md,
    # Addressing). One address, because the rig forwards host port 8090 to
    # 172.30.0.2:8080 and the job runs one VM.
    sandbox = {
        bridges = {
            br0 = {
                subnet = "172.30.0.0/24",
                gateway = "172.30.0.1",
                range = "172.30.0.2/32",
                dns = ["1.1.1.1"],
            },
        },
    },
}
