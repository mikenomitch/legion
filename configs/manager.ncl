{
  server = {
    port = 4747,
    host = "127.0.0.1",
    # advertiseAddr = "http://10.0.0.5:4747",  # address embedded in join tokens
    # tls = { enabled = true, certPath = "/etc/legion/server.pem", keyPath = "/etc/legion/server.key" },
    # Browser origins allowed to call the gRPC-Web API and the exec WebSocket
    # (docs/ui-architecture.md). Loopback origins (localhost, 127.0.0.1, [::1],
    # any port) are always allowed; list a hosted UI here. "*" allows any.
    # allowedOrigins = ["https://legion-ui.example.com"],
  },
  # State store dir (store.sqlite, keys/, backups/). Defaults to
  # $LEGION_DATA_DIR, else ./.legion (docs/data-directory.md).
  # storePath = "/var/lib/legion",
  workerConfigPath = "./configs/workers/",
  evalThreads = 4,
  schedulingPolicy = "bin_pack",
  defaultPriority = 50,
  # Exec & actions (docs/exec-and-actions.md). Enabled by default; exec is
  # still per-caller default-deny once ACLs are on. Set `enabled = false` to turn
  # off ExecAllocation and the exec WebSocket bridge entirely.
  # exec = { enabled = true },
  # Direct exec tickets (`legion exec --direct`, `MintExecTicket`): how long a
  # ticket can be redeemed and how long its session may run. Signed with the
  # workload-identity key, so `workloadIdentity.enabled` must be true to mint.
  # exec = { tickets = { defaultTtl = "60s", maxTtl = "10m", maxSession = "1h" } },
  # Telemetry: Prometheus /metrics plus /healthz + /readyz probes
  # (docs/metrics.md). Enabled by default; binds server.port + 2 (4749 here)
  # unless `port` is set. Set `enabled = false` to turn the listener off.
  # telemetry = { enabled = true, port = 4749 },
  # Edge boot (docs/edge-boot.md): boot tickets and edge-initiated starts.
  # OFF BY DEFAULT, and turning it on is an explicit security decision — it
  # grants nodes the ability to start work on a capability, growing a
  # compromised node's blast radius from "runs what it was given" to "runs
  # anything a live ticket names". Tickets are signed by the
  # workload-identity issuer, so `workloadIdentity.enabled` must also be true.
  #
  # A job's own `speculativeStart` block does NOT need this switch: pre-commit
  # dispatch from the manager opens no new authorization surface.
  #
  # edgeBoot = {
  #   enabled = false,
  #   # Ticket lifetime when a mint does not ask for one.
  #   defaultTicketTtl = "5m",
  #   # Hard cap. Revocation is by TTL only, so this is the real bound on
  #   # how long a leaked ticket is dangerous.
  #   maxTicketTtl = "1h",
  #   # Cap on concurrent unratified claims per node.
  #   maxProvisionalPerNode = 32,
  # },
  # Automatic suspend (docs/suspend-and-resume.md, "Idle policy"). The idle
  # sweep reads the usage reports workers send every 10s and suspends
  # instances quiet past their task's `suspend.idle.after`. It reads no
  # store state until it has something to suspend.
  # suspend = {
  #   idleSweep = "15s",
  #   # When a resume waits on its checkpoint's node because the node is
  #   # full, suspend the most idle suspendable instances there until it
  #   # fits (docs/suspend-and-resume.md, "Capacity for resumes").
  #   makeRoom = true,
  # },
  schedulerSettings = {
    plan_conflict_max_retries = 3,
    # Score weights (docs/jobs-tasks-and-scheduling.md, "Score weights"). The
    # values below are the defaults. `spread` and `bin_pack` follow the
    # scheduling policy.
    # weights = {
    #   affinities = 1.0,
    #   automatic_anti_affinity = 0.2,
    #   # Prefer nodes whose content store already holds a placement's
    #   # digest-pinned artifacts. 0 turns it off (docs/artifacts.md,
    #   # "Content locality").
    #   locality = 1.0,
    # },
    # Late-binding plan apply (docs/late-binding-plan-apply.md): ranked
    # candidates per placement; the apply binds the first candidate whose
    # node epoch is unchanged, so concurrent submits cost a fallback rank
    # instead of a conflict retry. Defaults on at 8. 0 disables candidate
    # lists (single-node submissions — the mixed-version kill switch).
    # plan_candidates = 8,
    # Full form: plan_candidates = { count = 8, band = 0.01 },
    #
    # Delay (ms, jittered) before a conflict-exhausted eval is retried;
    # exhaustion requeues the eval instead of failing it terminally.
    # plan_conflict_requeue_delay_ms = 1000,
    #
    # Preemption (docs/preemption-and-eviction.md). When a job cannot place
    # and its priority band outranks work that can be displaced, the planner
    # evicts the minimum set of lower-band allocations and places in the same
    # transaction. Everything here matches the built-in defaults.
    #
    # preemption = {
    #   # enforce (default) | observe | off.
    #   #   enforce — evict and place, atomically.
    #   #   observe — run the identical victim selection, emit
    #   #             `allocation.preempt_planned`, evict nothing. The
    #   #             cautious-adoption posture: evidence before action.
    #   #   off     — never run the pass; bands only order the eval queue.
    #   # Default is `enforce` because the band choice IS the consent:
    #   # nothing is a victim unless its author put it below the preemptor.
    #   mode = "enforce",
    #   # Pools where preemption never runs, whatever the bands say — for
    #   # pools whose jobs each hold hours of un-checkpointed state.
    #   disabledPools = ["gpu"],
    #   # Minimum age before an allocation may be preempted. The cheapest
    #   # thrash guard there is: without it a plan can undo the placement a
    #   # previous plan made seconds earlier.
    #   victimExemptAge = "60s",
    # },
    #
    # Adaptive scheduler concurrency
    # (docs/adaptive-scheduler-concurrency.md). The manager measures its own
    # plan-apply conflict rate and tunes how many evals it will PLAN at once,
    # between `min` and `max`. Eval kinds that cannot conflict (today,
    # `job_stop`) are never throttled. `evalThreads` stays the worker-pool
    # size and becomes the ceiling the controller backs off from — only on
    # evidence, and only in `enforce`.
    #
    # adaptiveConcurrency = {
    #   # enforce — measure and actuate.
    #   # observe  — measure, export what it WOULD do, actuate nothing. The
    #   #            default, and the same evidence-before-action posture as
    #   #            `preemption.mode`.
    #   # off      — do not run the controller at all (today's behavior).
    #   mode = "observe",
    #   # Floor and ceiling on admitted planners. `max` defaults to
    #   # `evalThreads` and can never exceed it; `min` must be at least 1.
    #   # Setting `min` and `max` to the same value pins the limit.
    #   min = 1,
    #   # max = 16,
    #   # Trip points on the conflict rate. The gap between them is the dead
    #   # band that stops the controller hunting.
    #   # highWatermark = 0.20,   # above this, halve the limit
    #   # lowWatermark  = 0.05,   # below this for 2s, add one back
    # },
    #
    # Plan-path scale levers (docs/scheduler-and-eval-loop.md). Both are on
    # by default; the flags exist as escape hatches.
    # lazy_version_sample: RETIRED (no effect, warns once). Plans now take
    # their fence epochs from their own capacity snapshot.
    # plan_apply_coalescing = true, # false = one commit/raft propose per plan
    # plan_apply_batch_max = 16,    # max plans per group commit (1-128)
    # Job-intake group commit (docs/scheduler-and-eval-loop.md): concurrent
    # job submits batch their scheduler-lane placement eval and their
    # status-lane task_status seed. One switch for both halves.
    # job_intake_coalescing = true, # false = one op per submitter, per lane
    # Bounded candidate scoring (docs/scheduler-and-eval-loop.md): off
    # unless configured. At or above `min_nodes` eligible candidates a
    # placement scores the best of `sample` uniformly drawn feasible nodes
    # instead of every one — flat per-placement cost on big fleets, at a
    # measured packing/spread quality cost. Feasibility is never sampled.
    # score_sample = { min_nodes = 1024, sample = 128 },
    # Declarable scheduling indexes (docs/scheduling-indexes.md): ad-hoc
    # candidate generation for the scheduler. A built-in `fit` index
    # (nearest-fit over free cpu/memory/disk within the task's pool) is
    # always declared and arms itself once the fleet reaches
    # `index_min_nodes` available nodes (default 128); below that, planning
    # uses the plain scan and indexes cost nothing.
    # index_min_nodes = 128,      # 0 disables indexing entirely
    # indexes = {
    #   fit = false,              # kill switch for the built-in fit index
    #   # Shorthand: `resources.*` entries are nearest-fit dims, everything
    #   # else is an exact-match partition key (`pool` = the node's pool).
    #   topo = ["pool", "region", "zone"],
    #   gpu = ["pool", "region", "resources.gpu_vram", "resources.memory"],
    #   # Full form when the defaults need overriding.
    #   big_jobs = {
    #     partition_by = ["pool"],
    #     dims = [
    #       { attr = "resources.memory", weight = 2 },         # scale = "log"
    #       { attr = "gpu_count", scale = "linear", step = 1 },
    #     ],
    #     order = "tight",        # "tight" packs, "loose" spreads
    #     mode = "on",            # "on" | "shadow" | "off"
    #   },
    # },
  },
  # Debounce window (ms) for KV-driven job updates (see docs/kv-store.md).
  # Rapid puts/deletes to watched keys within the window are coalesced into
  # a single restart/rollout per job; the final config always reflects the
  # latest KV state. 0 reconciles synchronously on every KV write.
  # kvDebounceMs = 500,
  # KV encryption at rest (docs/kv-encryption.md). ON BY DEFAULT with no
  # block at all: every KV value is AES-256-GCM under a data key that is
  # itself wrapped by a root key the manager mints at
  # <storePath>/keys/root.key on first start. Keep that file with your
  # backups; a manager joining a raft cluster receives it automatically.
  # `legion keyring status` shows the state and the next step, if any.
  # Everything below matches the built-in defaults.
  # encryption = {
  #   enabled = true,
  #   rootKey = {
  #     provider = "file",                        # "file" | "env" | "exec"
  #     # path = "/var/lib/legion/keys/root.key", # file: default <storePath>/keys/root.key
  #     # var = "LEGION_ROOT_KEY",                # env: the variable holding the key line(s)
  #     # command = ["legion-kms-aws", "--key-id", "arn:aws:kms:…:key/…"],  # exec: a KMS helper
  #     # timeout = "10s",                        # exec: per-call limit
  #   },
  #   # Age past which the active data key is rotated automatically; values
  #   # are re-encrypted under the new key in the background afterwards.
  #   dataKeyRotationAge = "720h",
  # },
  # Cluster-level artifact settings (docs/artifacts.md). Everything here is
  # optional; the values below match the built-in defaults.
  artifacts = {
    # Plain `http` requires explicit opt-in.
    allowedSchemes = ["https", "s3", "oci", "git", "file"],
    # Cluster auth matchers applied when a task artifact carries no `auth`.
    # First match wins; entries may use `kv` or `nodeIdentity` (never a
    # per-task identity). Example:
    #   auth = [
    #     { match = "oci://ghcr.io/acme/**", kv = "secrets/ghcr-pull" },
    #     { match = "s3://acme-tooling/**", nodeIdentity = "artifact-pull" },
    #   ],
    auth = [],
    # Whether container `image` fields desugar into Legion-managed oci://
    # artifacts. Off until the artifact cache lands (re-download per
    # allocation otherwise).
    containerImageDesugar = false,
  },
  # Workload identity (OIDC issuer; see docs/workload-identity.md).
  # Off by default. When enabled the manager signs short-lived per-task JWTs
  # and serves the OIDC discovery document + JWKS over plain HTTP on the main
  # listener. `issuer` is the externally-published URL relying parties trust
  # (it must be reachable by AWS/GCP/Vault — typically via an HTTPS proxy).
  # `nodeIdentities` declares identities issued to worker nodes rather than
  # tasks — referenced by artifact `auth.nodeIdentity` and cluster
  # artifact-auth matchers.
  # workloadIdentity = {
  #   enabled = true,
  #   issuer = "https://manager.example.com:4747",
  #   jwksPath = "/.well-known/jwks.json",
  #   signingAlg = "ES256",
  #   defaultTtl = "15m",      # token TTL when an identity sets none
  #   rotationAge = "24h",     # rotate the signing key past this age
  #   subTemplate = "{region}:{job}:{group}:{task}",
  #   nodeIdentities = [
  #     {
  #       name = "artifact-pull",
  #       audience = ["sts.amazonaws.com"],
  #       ttl = "15m",
  #       subTemplate = "{region}:node:{pool}",
  #     },
  #   ],
  #   # Map a verified control-plane (`legion`) token's claims to ACL
  #   # policies so a workload can authenticate back to Legion. Empty (the
  #   # default) = tokens verify to a principal but grant no policies.
  #   controlPlaneBindings = [
  #     { claim = "legion.io/job", value = "api", policies = ["kv-reader"] },
  #   ],
  # },
  persistence = {
    type = "local_disk",
    streaming = {
      replicaUrl = "",
      syncInterval = "1s",
      lease = {
        provider = "none",
        key = "leases/default/primary.json",
        ttlSeconds = 15,
        renewIntervalSeconds = 5,
        objectStorage = {
          bucket = "",
          region = "auto",
        },
      },
    },
    raft = {
      nodeId = "mgr-a",
      bootstrap = true,
      peers = ["mgr-a", "mgr-b", "mgr-c"],
      joinTokenTtlSeconds = 900,
    },
  },
  # Heartbeat cadence and the lost-node window derived from it
  # (docs/heartbeat-cadence-and-scale.md). Omitting the whole block gives
  # adaptive cadence at 2,500 beats/s: every fleet up to 2,500 nodes keeps
  # the 2s floor and the historical 10s lost window (5 x 2s), and larger
  # fleets back off along the ladder instead of flooding the leader.
  heartbeat = {
    # Missed beats before a node is lost. The lost window is ALWAYS this
    # times THAT node's own interval, so a 2s pool and a 60s pool get
    # 10s and 300s windows from one setting.
    missedBeatsBeforeLost = 5,

    # Adaptive cadence: the manager computes each worker's interval from
    # FLEET SIZE and serves it on the heartbeat reply, so cadence backs off
    # as the fleet grows instead of healthy nodes being marked lost. The
    # served interval is a floor: a worker type with a slower
    # `heartbeatSeconds` keeps its own cadence. `false` turns it off, and
    # workers then keep their worker-type `heartbeatSeconds` at any size.
    adaptive = true,

    # Aggregate heartbeat rate the served interval is sized for:
    # interval = ceil(headroom x nodes / rate), snapped to the ladder
    # [2, 3, 5, 8, 13, 21, 34, 55, 89, 120]s. Lower it on a smaller
    # control plane; measure yours with `legion node cadence`.
    targetRatePerSecond = 2500,

    # Floor and ceiling on a served interval. Past the ceiling the manager
    # stops backing off and warns instead: the answer is more control plane.
    minInterval = "2s",
    maxInterval = "120s",

    # Headroom in the sizing formula `heartbeatSeconds >= H x N / R`.
    # 2 runs the control plane at half its measured ceiling.
    headroom = 2,
  },
  # Node liveness: where heartbeat state lives and how it survives an
  # election (docs/node-liveness.md). Heartbeats are tracked in the
  # status-lane leader's MEMORY; only status transitions are replicated.
  liveness = {
    # After acquiring status-lane leadership (or starting the process), mark
    # nothing lost for this long. A new leader inherits no liveness state and
    # rebuilds it from incoming heartbeats, so an election is not an outage.
    failoverGrace = "60s",

    # How often a manager that does NOT lead the status lane reports the
    # nodes it heard from to the leader. ONE RPC per manager per interval,
    # whatever the fleet size.
    digestInterval = "1s",

    # How long the leader keeps trusting a silent peer manager's last
    # digest. A manager-to-manager problem must not present as fleet-wide
    # node loss.
    peerDigestGrace = "30s",

    # How often the leader replicates a batched heartbeat checkpoint so
    # every manager can show an approximate "last heartbeat" time. DISPLAY
    # ONLY — never read back as liveness. "0s" disables it entirely, and
    # non-leaders then show no time rather than an approximate one.
    checkpointInterval = "30s",

    # Accumulation window for the heartbeat group commit. "0ms" (the
    # default) is commit-while-you-can batching. A steady-state heartbeat
    # does not commit at all, so this applies only to the checkpoint and to
    # the mixed-version forwarding fallback.
    # batchWindow = "0ms",
  },
  # Auth & ACLs (on by default; see docs/auth-and-acls.md). At first start
  # the manager mints the initial management token itself and writes it to
  # <dataDir>/bootstrap-token and, when absent, ~/.legion/token (mode 0600),
  # so the CLI on this machine works with nothing to set up. Create
  # policies/tokens with `legion acl ...` and introduce workers with
  # `legion join-token create`. `enabled = false` is for throwaway clusters
  # only: every caller, including any task, then controls the cluster.
  acl = {
    enabled = true,
    # bootstrapSecretFile = "/etc/legion/bootstrap-secret",  # IaC pre-seed (>= 24 chars), read at first start
    # auditOnly = true,        # log denials without enforcing (migration mode)
    # loginTtl = "24h",        # lifetime of tokens minted via `legion login`
    # tokenMaxTtl = "720h",    # upper bound for client-token lifetimes
    # jwksRefreshInterval = "5m",
    # authMethods = [
    #   {
    #     name = "corp-oidc",
    #     type = "oidc",                       # discovery via issuer
    #     issuer = "https://idp.example.com",
    #     boundAudiences = ["legion"],
    #     userClaim = "email",
    #     oidcClientId = "legion",             # enables browser/CLI SSO login
    #     oidcScopes = ["openid", "email", "groups"],
    #     bindingRules = [
    #       { claim = "groups", value = "platform", policies = ["ops"] },
    #       { policies = ["base-read"] },     # unconditional
    #     ],
    #     tokenTtl = "8h",
    #   },
    #   {
    #     name = "corp-ldap",
    #     type = "webhook",                    # your endpoint verifies credentials
    #     webhookUrl = "https://auth.internal/legion/verify",
    #     tokenTtl = "8h",
    #   },
    # ],
  },
}
