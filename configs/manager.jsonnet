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
  // Debounce window (ms) for KV-driven job updates (see docs/kv-store.md).
  // Rapid puts/deletes to watched keys within the window are coalesced into
  // a single restart/rollout per job; the final config always reflects the
  // latest KV state. 0 reconciles synchronously on every KV write.
  // kvDebounceMs: 500,
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
  // Workload identity (OIDC issuer; see docs/workload-identity.md).
  // Off by default. When enabled the manager signs short-lived per-task JWTs
  // and serves the OIDC discovery document + JWKS over plain HTTP on the main
  // listener. `issuer` is the externally-published URL relying parties trust
  // (it must be reachable by AWS/GCP/Vault — typically via an HTTPS proxy).
  // `nodeIdentities` declares identities issued to worker nodes rather than
  // tasks — referenced by artifact `auth.nodeIdentity` and cluster
  // artifact-auth matchers.
  // workloadIdentity: {
  //   enabled: true,
  //   issuer: "https://manager.example.com:4747",
  //   jwksPath: "/.well-known/jwks.json",
  //   signingAlg: "ES256",
  //   defaultTtl: "15m",      // token TTL when an identity sets none
  //   rotationAge: "24h",     // rotate the signing key past this age
  //   subTemplate: "{region}:{job}:{group}:{task}",
  //   nodeIdentities: [
  //     {
  //       name: "artifact-pull",
  //       audience: ["sts.amazonaws.com"],
  //       ttl: "15m",
  //       subTemplate: "{region}:node:{pool}",
  //     },
  //   ],
  // },
  persistence: {
    type: "local_disk",
    streaming: {
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
