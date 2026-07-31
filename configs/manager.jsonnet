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
  // Exec & actions (docs/exec-and-actions.md). Enabled by default; exec is
  // still per-caller default-deny once ACLs are on. Set enabled:false to turn
  // off ExecAllocation and the exec WebSocket bridge entirely.
  // exec: { enabled: true },
  // Telemetry: Prometheus /metrics plus /healthz + /readyz probes
  // (docs/metrics.md). Enabled by default; binds server.port + 2 (4749 here)
  // unless `port` is set. Set enabled:false to turn the listener off.
  // telemetry: { enabled: true, port: 4749 },
  schedulerSettings: {
    plan_conflict_max_retries: 3,
    // Plan-path scale levers (docs/scheduler-and-eval-loop.md). Both are on
    // by default; the flags exist as escape hatches.
    // lazy_version_sample: true,   // false = eager O(fleet) epoch sample per plan
    // plan_apply_coalescing: true, // false = one commit/raft propose per plan
    // plan_apply_batch_max: 16,    // max plans per group commit (1-128)
    // Bounded candidate scoring (docs/scheduler-and-eval-loop.md): off
    // unless configured. At or above `min_nodes` eligible candidates a
    // placement scores the best of `sample` uniformly drawn feasible nodes
    // instead of every one — flat per-placement cost on big fleets, at a
    // measured packing/spread quality cost. Feasibility is never sampled.
    // score_sample: { min_nodes: 1024, sample: 128 },
    // Declarable scheduling indexes (docs/scheduling-indexes.md): ad-hoc
    // candidate generation for the scheduler. A built-in `fit` index
    // (nearest-fit over free cpu/memory/disk within the task's pool) is
    // always declared and arms itself once the fleet reaches
    // `index_min_nodes` available nodes (default 128); below that, planning
    // uses the plain scan and indexes cost nothing.
    // index_min_nodes: 128,      // 0 disables indexing entirely
    // indexes: {
    //   fit: false,              // kill switch for the built-in fit index
    //   // Shorthand: `resources.*` entries are nearest-fit dims, everything
    //   // else is an exact-match partition key (`pool` = the node's pool).
    //   topo: ["pool", "region", "zone"],
    //   gpu: ["pool", "region", "resources.gpu_vram", "resources.memory"],
    //   // Full form when the defaults need overriding.
    //   big_jobs: {
    //     partition_by: ["pool"],
    //     dims: [
    //       { attr: "resources.memory", weight: 2 },           // scale: "log"
    //       { attr: "gpu_count", scale: "linear", step: 1 },
    //     ],
    //     order: "tight",        // "tight" packs, "loose" spreads
    //     mode: "on",            // "on" | "shadow" | "off"
    //   },
    // },
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
  //   // Map a verified control-plane (`legion`) token's claims to ACL
  //   // policies so a workload can authenticate back to Legion. Empty (the
  //   // default) = tokens verify to a principal but grant no policies.
  //   controlPlaneBindings: [
  //     { claim: "legion.io/job", value: "api", policies: ["kv-reader"] },
  //   ],
  // },
  persistence: {
    type: "local_disk",
    streaming: {
      replicaUrl: "",
      syncInterval: "1s",
      // One credential source for both the WAL uploads and the lease object.
      // "environment" (the default) reads AWS_*, falling back to LITESTREAM_*,
      // from the manager's process env — the historical behavior.
      credentials: {
        provider: "environment",  // environment | static | webIdentity
        // "static": literal keys in this file.
        // static: {
        //   accessKeyId: std.extVar("R2_ACCESS_KEY_ID"),
        //   secretAccessKey: std.extVar("R2_SECRET_ACCESS_KEY"),
        // },
        //
        // "webIdentity": the manager's own workload identity. A JWT written by
        // the *platform* (EKS IRSA, GKE, a SPIRE JWT-SVID, Vault's identity
        // token backend) is exchanged for short-lived credentials via STS
        // AssumeRoleWithWebIdentity. Legion never signs this token itself —
        // see docs/persistence-streaming.md#credentials for why.
        // webIdentity: {
        //   tokenFile: "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
        //   roleArn: "arn:aws:iam::123456789012:role/legion-manager-state",
        //   sessionName: "legion-manager",  // optional; shows up in CloudTrail
        //   durationSeconds: 3600,          // optional; 900..43200
        //   stsRegion: "us-east-1",         // optional; regional STS endpoint
        // },
      },
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
