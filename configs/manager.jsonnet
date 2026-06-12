{
  server: {
    port: 4747,
    host: "127.0.0.1",
    // advertiseAddr: "http://10.0.0.5:4747",  // address embedded in join tokens
    // tls: { enabled: true, certPath: "/etc/legion/server.pem", keyPath: "/etc/legion/server.key" },
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
  // management token, then create policies/tokens with `legion acl ...` and
  // introduce workers with `legion join-token create`.
  acl: {
    enabled: false,
    // auditOnly: true,        // log denials without enforcing (migration mode)
    // loginTtl: "24h",        // lifetime of tokens minted via `legion login`
    // tokenMaxTtl: "720h",    // upper bound for client-token lifetimes
    // jwksRefreshInterval: "5m",
    // authMethods: [
    //   {
    //     name: "corp-oidc",
    //     type: "oidc",                       // discovery via issuer
    //     issuer: "https://idp.example.com",
    //     boundAudiences: ["legion"],
    //     userClaim: "email",
    //     oidcClientId: "legion",             // enables browser/CLI SSO login
    //     oidcScopes: ["openid", "email", "groups"],
    //     bindingRules: [
    //       { claim: "groups", value: "platform", policies: ["ops"] },
    //       { policies: ["base-read"] },      // unconditional
    //     ],
    //     tokenTtl: "8h",
    //   },
    //   {
    //     name: "corp-ldap",
    //     type: "webhook",                    // your endpoint verifies credentials
    //     webhookUrl: "https://auth.internal/legion/verify",
    //     tokenTtl: "8h",
    //   },
    // ],
  },
}
