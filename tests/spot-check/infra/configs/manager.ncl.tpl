# Spot-check manager: one of three raft voters, ACLs on. install.sh renders
# @@NODE_ID@@, @@PRIVATE_IP@@, @@BOOTSTRAP@@, @@PEERS@@, @@ECR@@ and
# @@JOIN_TOKEN_LINE@@ per box.
{
  server = {
    port = 4747,
    host = "0.0.0.0",
    # Embedded in worker join tokens. Without it, a 0.0.0.0 bind advertises
    # 127.0.0.1 and remote workers dial their own loopback.
    advertiseAddr = "http://@@PRIVATE_IP@@:4747",
  },
  storePath = "/var/lib/legion/store",
  workerConfigPath = "/opt/legion/configs/workers/",
  telemetry = { enabled = true, port = 4749 },
  acl = { enabled = true },
  # Session tickets and direct exec tickets are signed by the workload-identity
  # issuer, and edge starts need the manager's switch (RUNBOOK 12.0, 12.10;
  # docs/edge-boot.md). With `issuer` unset, every raft manager defaults to
  # the one cluster issuer `https://manager.legion` (docs/workload-identity.md,
  # "The issuer"), and workers learn it from the managers, so a ticket minted
  # on any manager verifies on any worker. Setting `issuer` here means setting
  # the same value on all three.
  workloadIdentity = { enabled = true },
  edgeBoot = { enabled = true },
  artifacts = {
    # Images go through the artifact pipeline, so private ECR images can use
    # the KV-held pull credential below (docs/artifacts.md). install.sh seeds
    # `secrets/ecr-pull`, and refresh-ecr.sh renews it (ECR tokens last 12h).
    containerImageDesugar = true,
    auth = [
      { "match" = "oci://@@ECR@@/**", kv = "secrets/ecr-pull" },
    ],
  },
  persistence = {
    type = "raft",
    raft = {
      nodeId = "@@NODE_ID@@",
      bootstrap = @@BOOTSTRAP@@,
      advertiseAddr = "@@PRIVATE_IP@@:4747",
      peers = [@@PEERS@@],
      expectedVoters = 3,
      dataDir = "/var/lib/legion/raft",
      topology = "single",
      joinTokenTtlSeconds = 900,
      @@JOIN_TOKEN_LINE@@
    },
  },
}
