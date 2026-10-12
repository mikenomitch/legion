# The in-process worker's type on macOS. The DAG's shared volume points at
# /tmp so it needs no setup.
{
    workerType = "default",
    pool = "default",
    additionalNodeChecks = [],
    attributes = {},
    hostVolumes = {
        pipeline = { path = "/tmp/legion-sc-pipeline", create = true },
    },
}
