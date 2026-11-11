# Legion B: streaming persistence (rustyriver S3 replica) with the
# object-storage lease providing single-primary failover fencing —
# docs/persistence-streaming.md. All three control-plane instances run
# this same config; whoever wins the lease is primary, the others stand
# by. Rendered by scripts/install-legion-b.sh with envsubst:
# BUCKET, REGION, PRIVATE_IP, SCHEDULING_POLICY.
{
  server = {
    port = 4747,
    host = "0.0.0.0",
    # See manager-a.ncl.tpl: without this, join tokens embed
    # http://127.0.0.1:4747 and every remote worker dials its own
    # loopback instead of the manager.
    advertiseAddr = "http://${PRIVATE_IP}:4747",
  },
  storePath = "/var/lib/legion/store",
  workerConfigPath = "/opt/legion/configs/workers/",
  evalThreads = 4,
  # The scenario's own `scheduling_policy` (bin_pack|spread), rendered by
  # the installer from LEGION_SCHEDULING_POLICY, which run-campaign.sh
  # sets from the scenario (or --scheduling-policy). It is a manager
  # setting, not a jobspec field, so a campaign runs one policy and
  # run-step.sh refuses a cell whose scenario asks for the other.
  schedulingPolicy = "${SCHEDULING_POLICY}",
  defaultPriority = 50,
  artifacts = {},

  telemetry = {
    enabled = true,
    port = 4749,
  },

  persistence = {
    type = "streaming",
    streaming = {
      replicaUrl = "s3://${BUCKET}/legion-b/replica",
      syncInterval = "1s",
      lease = {
        provider = "object_storage",
        key = "legion-b/leases/primary.json",
        ttlSeconds = 15,
        renewIntervalSeconds = 5,
        objectStorage = {
          bucket = "${BUCKET}",
          region = "${REGION}",
          # No accessKeyId/secretAccessKey: the instance profile's
          # ambient credentials are used (terraform/iam.tf).
        },
      },
    },
  },
}
