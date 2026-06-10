{
  server: {
    port: 4747,
    insecurePort: 4748,
    host: "127.0.0.1",
  },
  storePath: ".legion",
  workerConfigPath: "./configs/workers/",
  evalThreads: 4,
  schedulingPolicy: "bin_pack",
  defaultPriority: 50,
  schedulerSettings: {
    plan_conflict_max_retries: 3,
  },
  // Cluster-level artifact settings (docs/artifacts.md). Everything here is
  // optional; the values below match the built-in defaults.
  artifacts: {
    // Plain `http` requires explicit opt-in.
    allowedSchemes: ["https", "s3", "oci", "git", "file"],
    // Cluster auth matchers applied when a task artifact carries no `auth`.
    // First match wins; entries may use `kv` or `nodeIdentity` (never a
    // per-task identity). Example:
    //   auth: [
    //     { match: "oci://ghcr.io/acme/**", kv: "secrets/ghcr-pull" },
    //     { match: "s3://acme-tooling/**", nodeIdentity: "artifact-pull" },
    //   ],
    auth: [],
    // Whether container `image` fields desugar into Legion-managed oci://
    // artifacts. Off until the artifact cache lands (re-download per
    // allocation otherwise).
    containerImageDesugar: false,
  },
  // Workload identity (off by default). `nodeIdentities` declares identities
  // issued to worker nodes rather than tasks — referenced by artifact
  // `auth.nodeIdentity` and cluster artifact-auth matchers. Example:
  //   workloadIdentity: {
  //     enabled: true,
  //     issuer: "https://manager.example.com:4747",
  //     nodeIdentities: [
  //       {
  //         name: "artifact-pull",
  //         audience: ["sts.amazonaws.com"],
  //         ttl: "15m",
  //         subTemplate: "{region}:node:{pool}",
  //       },
  //     ],
  //   },
  persistence: {
    type: "local_disk",
    streaming: {
      litestreamBinary: "litestream",
      replicaUrl: "",
      syncInterval: "1s",
      lease: {
        provider: "none",
        key: "leases/default/primary.json",
        ttlSeconds: 15,
        renewIntervalSeconds: 5,
        objectStorage: {
          bucket: "",
          region: "auto",
        },
      },
    },
    raft: {
      nodeId: "mgr-a",
      bootstrap: true,
      peers: ["mgr-a", "mgr-b", "mgr-c"],
      joinTokenTtlSeconds: 900,
    },
  },
}
