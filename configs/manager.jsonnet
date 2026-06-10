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
    raft: {
      nodeId: "mgr-a",
      bootstrap: true,
      peers: ["mgr-a", "mgr-b", "mgr-c"],
      joinTokenTtlSeconds: 900,
    },
  },
  // Auth & ACLs (off by default; see docs/auth-and-acls.md).
  // When enabled, run `legion acl bootstrap` once to mint the initial
  // management token, then create policies/tokens with `legion acl ...`.
  acl: {
    enabled: false,
    // auditOnly: true,        // log denials without enforcing (migration mode)
    // loginTtl: "24h",        // lifetime of tokens minted via `legion login`
    // jwksRefreshInterval: "5m",
    // authMethods: [
    //   {
    //     name: "corp-oidc",
    //     type: "oidc",                       // discovery via issuer
    //     issuer: "https://idp.example.com",
    //     boundAudiences: ["legion"],
    //     userClaim: "email",
    //     bindingRules: [
    //       { claim: "groups", value: "platform", policies: ["ops"] },
    //       { policies: ["base-read"] },      // unconditional
    //     ],
    //     tokenTtl: "8h",
    //   },
    // ],
  },
}
