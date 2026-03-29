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
  },
}
