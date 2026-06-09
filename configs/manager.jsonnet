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
  // Workload identity (OIDC issuer; see docs/workload-identity.md).
  // Off by default. When enabled the manager signs short-lived per-task JWTs
  // and serves the OIDC discovery document + JWKS over plain HTTP on the main
  // listener. `issuer` is the externally-published URL relying parties trust
  // (it must be reachable by AWS/GCP/Vault — typically via an HTTPS proxy).
  // workloadIdentity: {
  //   enabled: true,
  //   issuer: "https://manager.example.com:4747",
  //   jwksPath: "/.well-known/jwks.json",
  //   signingAlg: "ES256",
  //   defaultTtl: "15m",      // token TTL when an identity sets none
  //   rotationAge: "24h",     // rotate the signing key past this age
  //   subTemplate: "{region}:{job}:{group}:{task}",
  // },
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
