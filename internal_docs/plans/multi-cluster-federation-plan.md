# Plan: Multi-Cluster Federation and Manager Resiliency

> **Judgement calls:** [`calls/multi-cluster-federation-plan_calls.md`](../calls/multi-cluster-federation-plan_calls.md)

**Status: PLANNED.** Nothing in this plan is built. Deliberately deferred:
cross-cluster service discovery (§11), witness voters (§8.4), weighted
count splitting (§7.3), automatic DR failover (§9.5), and cluster
provisioning (a Cluster-API-shaped concern Legion does not take on).

---

## 1. Problem statement

Legion runs one cluster well: a raft-replicated manager quorum and a fleet
of workers. Nothing connects two Legion clusters. Every axis of "more than
one cluster" is unsolved, and they are different problems that deserve
different mechanisms. In layman's terms:

**P1 — One cluster can only get so big.** Every write in a cluster flows
through one consensus pipeline and one state store. That is what makes the
cluster consistent, and it is also a ceiling: past some size, the bookkeeping
machinery itself becomes the bottleneck. Kubernetes documents a hard envelope
of 5,000 nodes per cluster, and in practice etcd and API-server watch fan-out
bite far earlier (many operators cap clusters at 500–2,000 nodes). The
industry answer is not "make one cluster infinitely big," it is "run many
clusters and make that not hurt."

**P2 — One cluster is one blast radius.** A bad config push, a poisoned
upgrade, or a control-plane outage takes out everything in the cluster.
Separate clusters turn a global outage into a partial one. Separate clusters
are also how organizations express geography (one per region, close to
users), compliance (EU data stays on EU machines), and team isolation
(the ML team cannot break the payments team). These users *want* hard
boundaries. Federation must connect clusters without dissolving the
boundaries that made them separate clusters in the first place.

**P3 — Many clusters means many panes of glass.** Today the CLI, the UI,
and the MCP server each point at exactly one manager (`LEGION_ADDR`,
`VITE_MANAGER_ENDPOINT`, one token file). An operator with three clusters
re-exports env vars and clobbers `~/.legion/token` all day, and no surface
can answer "where is job `web` running, across everything?" The job to be
done: one place to see and operate the whole estate.

**P4 — Many clusters means many logins.** ACL tokens are per-cluster. An
operator (or CI pipeline, or agent) working across three clusters holds
three unrelated credentials, and revoking a person means visiting every
cluster. The job to be done: authenticate once, work everywhere, while
each cluster keeps the final say over what you may do locally.

**P5 — Deploying one app to many clusters is manual.** "Run 6 in us-east
and 3 in eu-west, roll eu first, stop if it fails" is a script and a
prayer today: N submits, N watches, hand-rolled ordering. The job to be
done: declare multi-cluster placement in the jobspec and get a coordinated,
observable, failure-aware rollout.

**P6 — Managers can be lost: a machine, a zone, or a whole site.** Raft
mode already survives minority loss inside one cluster. But:

- Losing a voter still eats fault-tolerance headroom until an operator
  intervenes. There is no warm pool of standby managers that step in.
- All voters typically live in one site. Lose the site, lose the control
  plane entirely. The recovery today is restore-from-backup onto a fresh
  cluster, with data loss back to the last backup and a hand-run runbook.

The jobs to be done, in plain terms: "a manager died and nobody had to do
anything," and "the datacenter burned down and we were taking traffic again
in minutes, having lost at most seconds of state changes." These are the
read-only-standby and backup-quorum questions this plan answers in §8 and §9.

**P7 — (Deferred) Services in one cluster can't find services in another.**
Cross-cluster service discovery and traffic is a real job, but it layers on
top of peering and has its own design space (the service-discovery plan
already scopes services as cluster-local and defers a `ServiceExport`-style
contract to a later plan). §11 sketches the shape so this plan doesn't
paint over it; the design belongs to a follow-up plan.

## 2. Prior art

Three bodies of prior art, researched in depth (mid-2026 docs). What follows
is the distilled version; the strongest lessons are marked **take** or
**reject**.

### 2.1 Kubernetes ecosystem: two generations of federation

**KubeFed (Federation v1/v2) is the definitive negative result.** It wrapped
every resource in a parallel `Federated*` API and pushed from a host cluster
holding credentials for every member. It was archived in 2023: the parallel
API broke every existing tool and forced migration, the push model hit
controller and credential-security walls, and rigid design spawned diverged
forks instead of contributions
([SIG-MC archival post](https://multicluster.sigs.k8s.io/blog/2022/2022-11-16_archiving-kubefed-on-jan-3-2023/),
[Kubernetes blog: Federation evolution](https://kubernetes.io/blog/2018/12/12/kubernetes-federation-evolution/)).
**Reject:** any design that changes the native jobspec API or centralizes
execution in a hub.

**The survivors all converged on the same shape.**
[Karmada](https://karmada.io/docs/core-concepts/architecture/) (CNCF
incubating, 100-cluster/500k-node scale test): unmodified native workload
objects plus *separate* placement/override policies, cluster-granular
scheduling (pick clusters, split counts), push for small fleets and pull
agents at scale.
[Open Cluster Management](https://open-cluster-management.io/docs/concepts/architecture/):
hub computes *placement decisions*, per-cluster agents pull and execute,
clusters stay autonomous during hub outages, registration is a double
opt-in where the member's private key never crosses the network.
[Rancher](https://ranchermanager.docs.rancher.com/reference-guides/rancher-manager-architecture/communicating-with-downstream-user-clusters):
agents open outbound tunnels (never hub-dials-member), an auth proxy gives
one login for every downstream cluster with RBAC synced down, and an
"authorized cluster endpoint" keeps direct access working when the hub is
down. [Argo CD](https://argocd-agent.readthedocs.io/latest/) re-learned the
same lesson the hard way: central push with hub-held credentials stopped
scaling around O(100) clusters and the fix (argocd-agent) moved controllers
into the member clusters. **Take:** native spec untouched, cluster-granular
placement, members autonomous when peers/hubs are unreachable, member-side
agents dial out.

**The hub's own store is where fleet designs die.**
[Rancher Fleet](https://www.suse.com/c/rancher_blog/scaling-kubernetes-gitops-with-fleet-experiment-results-and-lessons-learnt/)
markets "1M clusters" but SUSE's own experiment broke at 200,000
per-(cluster×bundle) bookkeeping objects, and the bottleneck was etcd, not
compute. **Take:** federation state in any one store must stay at
"job + placement decision" granularity, never per-(cluster × job × instance).

**Virtual-kubelet approaches (Admiralty, Liqo)** make a remote cluster look
like one giant node so the local scheduler can place individual pods there.
Technically elegant, permanently niche: the abstraction leaks (a "node" the
size of a cluster distorts bin-packing and spread), debugging spans proxy
and delegate objects, and it needs its own network fabric. **Reject:**
fleet-wide task-level scheduling. The ecosystem consensus is that placement
across clusters happens at *cluster* granularity and each cluster's own
scheduler places tasks locally.

**Why multi-cluster at all** (consistent across sources): scale ceilings,
blast radius, region/latency, compliance/sovereignty, team isolation
([k8s large-cluster limits](https://kubernetes.io/docs/setup/best-practices/cluster-large/),
[CNCF multi-cluster taxonomy](https://www.cncf.io/blog/2021/04/12/simplifying-multi-clusters-in-kubernetes/)).
This is the same list as §1, which is reassuring.

### 2.2 HashiCorp: Nomad regions, Consul's two attempts, Vault replication

**Nomad multi-region federation** is the closest analog to what Legion
should build
([federation architecture](https://developer.hashicorp.com/nomad/docs/architecture/cluster/federation)):
each region is a complete cluster (own raft, own scheduler, own state),
regions share *nothing* about jobs or nodes, and any server transparently
forwards a request tagged with another region's name. Its core invariant
falls straight out of that: *"Running workloads are never interrupted due to
federation failures."* Only a small meta-plane replicates (ACL policies,
global tokens, namespaces, node pools) — and it replicates hub-and-spoke
from one **authoritative region**. Every painful documented failure mode is
"the hub is down": no global token creation, no meta writes anywhere, no
new-region bootstrap, all multi-region deployments fail
([failure scenarios](https://developer.hashicorp.com/nomad/docs/operations/federation/failure)).
HashiCorp's own guidance includes pre-created per-region break-glass tokens
for when the hub is unavailable. **Take:** share-nothing job state and
transparent request forwarding. **Reject:** the authoritative hub.

**Consul built federation twice, and the second design repudiates the
first.** WAN federation (shared gossip pool, primary datacenter owning ACLs
and the CA) gave way to
[cluster peering](https://developer.hashicorp.com/consul/docs/east-west/cluster-peering):
fully autonomous clusters, explicit token-exchanged pairing, explicit and
minimal sharing (only exported services), no primary anything, built
precisely because the primary-DC dependency and cross-team ownership pain
were unfixable
([Consul 1.13 announcement](https://www.hashicorp.com/en/blog/consul-1-13-introduces-cluster-peering)).
**Take:** symmetric peering as the topology.

**Nomad's `multiregion` job block** is the surface model for §7: per-region
`count`/`meta` overrides, `strategy { max_parallel, on_failure }`, and a
deployment state machine (`pending → running → blocked → successful`) that
coordinates independent per-region deployments over RPC only. Its
documented gaps are instructive: no rollback of already-successful regions,
and removing a region from the block does not stop the job there
([multiregion block](https://developer.hashicorp.com/nomad/docs/job-specification/multiregion)).

**Vault replication** is the reference design for DR (§9)
([replication docs](https://developer.hashicorp.com/vault/docs/enterprise/replication),
[internals](https://developer.hashicorp.com/vault/docs/internals/replication)):
async WAL streaming to a warm, read-only standby cluster, with a
merkle-diff resync path when a standby falls off the retained log window.
Promotion is **deliberately manual**: *"Vault does not support an automatic
failover/promotion of a DR secondary cluster, and this is a deliberate
choice due to the difficulty in accurately evaluating why a failover should
or shouldn't happen"* — a partition and a dead primary look identical from
the standby's seat. Operators pre-provision a batch DR operation token so
promotion doesn't require assembling key-holders mid-incident, and the
split-brain repair is procedural: demote the returning primary immediately.
Vault's **batch tokens** carry one more lesson: the only credentials that
work across clusters without replication or hub availability are stateless,
self-verifying ones. **Take:** all of it, plus one improvement CockroachDB's
physical-cluster-replication docs admit they lack — mechanical fencing so a
zombie primary's orders are rejected, not just a DNS change (§9.4).

**Nomad/Consul Autopilot** is the reference for standby managers (§8)
([Nomad autopilot](https://developer.hashicorp.com/nomad/docs/manage/autopilot),
[Consul redundancy zones](https://developer.hashicorp.com/consul/docs/manage/scale/redundancy-zone)):
non-voting servers as read/standby capacity, **redundancy zones** keeping
one voter per zone with hot-standby non-voters auto-promoted when the
zone's voter dies (and demoted back on recovery), a stabilization window
before any server earns a vote, and version-aware upgrade migration.

### 2.3 Raft-level patterns: learners, stale reads, stretched vs. async

Every serious raft system grew **learners** (etcd
[learner design](https://etcd.io/docs/v3.6/learning/design-learner/), Consul
and Nomad non-voters, TiKV learner peers): full replication stream, no vote,
promotion gated on being caught up. Legion's join flow already uses learners
as a transition state; this plan makes them a destination (§8).

**Read consistency is a menu, not a binary.**
[Consul's consistency modes](https://developer.hashicorp.com/consul/api-docs/features/consistency)
are the best concrete model: `default` (leader-local), `consistent`
(quorum-checked), `stale` (any server answers, **with the staleness
reported** in response headers, and it keeps working with no quorum at all).
Legion's current behavior — every manager serves local reads, writes are
leader-only — is already "stale reads everywhere"; what is missing is the
reporting (§8.2).

**Stretching one quorum across sites vs. an async standby cluster** is the
central DR trade
([Vault reference architecture](https://developer.hashicorp.com/vault/tutorials/day-one-raft/raft-reference-architecture),
[CockroachDB PCR](https://www.cockroachlabs.com/docs/stable/physical-cluster-replication-overview),
[etcd FAQ](https://etcd.io/docs/v3.6/faq/)):

- **Stretched quorum:** RPO 0 (site loss loses nothing) and automatic
  failover, but every commit pays a cross-site round trip and you need
  three failure domains. HashiCorp draws the line at <8ms RTT between raft
  members; beyond that, stretching is malpractice.
- **Async standby cluster:** local-speed writes and only two sites needed,
  but RPO > 0 (the replication lag) and takeover is an explicit promotion
  with real split-brain risk if the old primary is not fenced.

The failure math for both is spelled out in §9.6. etcd's DR discipline is
also worth keeping: a restored/promoted cluster takes a **new cluster
identity** so members of the old one can never accidentally rejoin it —
Legion's cluster stamp and `forceNewCluster` already implement this
instinct.

## 3. User scenarios

**S1 — Three clusters, one laptop.** An operator runs `dev`, `staging`, and
`prod-us`. `legion cluster add` once per cluster, then
`legion job list -C prod-us`, `legion node ls -C staging`, and
`legion job list --all-clusters` to see everything with a CLUSTER column.
No env-var juggling, no token clobbering. The UI shows a cluster switcher
and an "all clusters" jobs view.

**S2 — One login for the estate.** The operator's company uses OIDC SSO.
They `legion login` once against `prod-us`. When they run a command against
peered `prod-eu`, it just works: `prod-eu` trusts `prod-us`-issued identity
and applies its *own* policies to the caller. Revoking the person at the
IdP (or their token on the home cluster) locks them out everywhere.

**S3 — One job, two regions, careful rollout.** A jobspec declares
placements in `prod-us` (count 6) and `prod-eu` (count 3) with
`maxParallel: 1`. Submitting to either cluster validates against both,
rolls `prod-us` first, waits for health, then rolls `prod-eu`. When the
eu rollout fails, the strategy pauses and `legion rollout status web`
shows a per-cluster picture. `prod-eu` being partitioned away never stops
`prod-us` from scheduling, healing, or rolling back its own copy.

**S4 — A manager dies at 3am, nobody wakes up.** A cluster runs 3 voters
across 3 zones plus one standby manager per zone. A zone's voter dies;
within a stabilization window the standby in that zone is promoted to
voter, quorum headroom is restored, and `legion raft history` shows what
happened and why. The old voter comes back, gets demoted to standby, and
the cluster returns to its declared shape.

**S5 — The site burns down.** `prod-us` streams every lane to a DR standby
cluster in another region. The primary site is lost. The on-call runs
`legion dr promote` on the standby with a pre-provisioned promote
credential. The standby becomes writable under a new cluster generation.
Workers (which had the standby's address from earlier heartbeats) re-home,
re-register, and their running allocations are adopted, not restarted —
the same startup-grace logic that already protects tasks across a manager
restart. Total state loss: the last few seconds of writes, and
`legion dr status` had been reporting that lag as a metric all along. When
the old site is powered back on, its managers see a higher cluster
generation and refuse to act as primary until explicitly demoted into
standby.

## 4. Goals and non-goals

### Goals

- **Peering:** explicit, symmetric, token-exchanged pairing of autonomous
  clusters. No authoritative hub, no shared quorum, no shared gossip.
- **Single pane of glass:** named clusters in the CLI/UI/MCP, transparent
  request forwarding between peers, and merged `--all-clusters` reads.
- **Federated auth:** log in once, operate on every peer, with authorization
  always decided locally by the target cluster.
- **Multi-cluster jobs:** per-cluster placements in the jobspec, coordinated
  rollout with `maxParallel`/`onFailure`, cluster-granular only.
- **Standby managers:** learners as a supported long-lived role, staleness
  made observable, and zone-aware automatic voter replacement.
- **DR:** an async standby cluster with continuous per-lane replication,
  measured RPO, manual promotion, and mechanical fencing of the old primary.
- **Invariant, non-negotiable:** running workloads and local scheduling are
  never interrupted by the failure of a peer, the federation layer, or a
  DR link. Every cluster remains a complete, standalone Legion.

### Non-goals

- **No global task scheduler.** Placement across clusters is cluster-granular;
  each cluster's scheduler owns its tasks. (Prior art: §2.1.)
- **No replicated job state.** A multi-cluster job is N independent jobs
  plus a coordinator; no cluster's store holds another cluster's rows.
- **No cluster provisioning.** Creating clusters is out of scope (the
  Cluster API lesson: lifecycle and workload federation are separate
  products).
- **No cross-cluster service discovery in this plan** (§11 sketches the
  contract for the follow-up plan).
- **No automatic DR failover in v1** (§9.5; manual promote is a feature,
  not a gap).
- **No WAN-stretched quorum guidance beyond documentation.** Stretching
  works today with tuned timings; we document when it is sane (§9.6) and
  build nothing new for it except zones (§8.3).

## 5. Background: what exists today

Code recon, current as of this branch:

- **One address knob in the whole product.** The CLI resolves
  `LEGION_ADDR` or falls back to `http://127.0.0.1:4747`
  (`src/cmd/helpers.rs:106`, `src/constants.rs:22`); one flat token file
  `~/.legion/token` (`src/cmd/helpers.rs:51`); no contexts or profiles.
  The UI bakes one endpoint at build time
  (`ui/src/grpcClient.ts:5`) with one localStorage token, one singleton
  gRPC-web client. The MCP server inherits the CLI's connection
  (`src/cmd/mcp.rs`). Workers get their manager address from the join
  token (`src/structs/token.rs:30`) and never re-resolve it
  (`src/worker/daemon.rs:161`).
- **No manager-to-manager RPC path exists.** `ManagerClient` is never used
  inside `src/manager/`. The only inter-manager wire is the raft transport
  (`proto/legion.proto:4296`), which is single-cluster by construction
  (cluster stamp + shared secret, `src/storage/raft/driver.rs:30`).
- **The Manager service is 85 RPCs on one listener**
  (`proto/legion.proto:18`, mounted at `src/manager/daemon.rs:380` beneath
  OIDC/CORS/gRPC-web/metrics layers). Manager→worker proxying exists
  (exec splice at `src/manager/exec.rs:229`, log fetch at
  `src/manager/server.rs:5799`), so "manager as trust-boundary proxy" is
  an established pattern.
- **Non-leader writes are rejected with a typed leader hint, never
  forwarded** (`src/storage/raft/driver.rs:576`,
  `src/manager/server.rs:88`, documented stance in
  [docs/multi-raft.md](../../docs/multi-raft.md) §"Non-leader write
  redirects"). Reads are served locally by every manager and are not
  leader-gated anywhere.
- **Learners exist as a join transition, not a role.** `add-learner` is an
  operator command (`src/storage/raft/driver.rs:3019`), join adds learners
  on every lane and promotes when caught up
  (`src/storage/raft/driver.rs:2228`), but nothing supports a permanent
  learner: no standby config, no staleness reporting, and
  `PersistenceRole` is only `Primary | Standby | Candidate`
  (`src/storage/persistence.rs:21`).
- **Snapshot + changeset machinery is reusable for DR.** Snapshots are
  whole-DB or per-lane logical dumps with atomic install
  (`src/storage/raft/state_machine.rs:913`, `:273`); every mutation is a
  captured SQLite changeset applied identically on every replica. A DR
  feed can be "snapshot install, then changeset stream" without new apply
  machinery.
- **Streaming persistence already ships an async-DR primitive.** rustyriver
  WAL shipping to object storage with an S3-lease failover loop
  (`src/storage/persistence.rs:192`, `:515`) — but its role flag is
  advisory: nothing in the RPC layer rejects writes on a lease standby.
- **Each manager is already an OIDC issuer.** Workload identity serves
  discovery + JWKS over plain HTTP on the main listener
  (`src/manager/oidc_http.rs`), and `JwtAuthMethod`
  (`src/manager/auth_method.rs:261`) already validates externally issued
  JWTs against a JWKS with binding rules mapping claims to policies. A
  `ControlPlaneIdentityMethod` exists (`src/manager/auth_method.rs:515`).
  Cluster-to-cluster authn can reuse all of this.
- **The event stream is federation-ready.** `StreamEvents` subscribes
  before replay, dedupes by id, and backfills on lag
  (`src/manager/server.rs:2369`), so a peer can consume a resumable,
  gap-free feed today. Event ids are lane-strided and per-cluster, so a
  merged feed needs a `(cluster, event_id)` cursor.
- **Placement vocabulary that already exists:** node pools
  (`src/structs/node.rs:16`), free-form node attributes + constraints, and
  `count: { num: N, per: "<attr>" }` spread
  (`src/structs/job.rs:190`, `src/manager/scheduler.rs:1064`). Namespaces
  are vocabulary-complete but value-empty: the ACL matcher handles
  namespace globs but everything is `"default"`
  (`src/structs/acl.rs:150`). There is no `region`/`cluster` field on the
  jobspec; the only region string in the product is the constant `"auto"`
  stamped into workload-identity claims (`src/constants.rs:37`).
- **Prior intent on record:** `internal_docs/TODO.md` lists "Cluster
  federation" as open, and the service-discovery plan scopes services
  cluster-local, deferring a `ServiceExport`-style federation to a later
  plan.

## 6. Proposed design — shape and terminology

A **federation** is a set of peered clusters. There is no hub: peering is
pairwise, symmetric, and explicit (Consul-peering-shaped, §2.2). Each
cluster keeps its own raft quorum, scheduler, state, ACLs, and workers.
What crosses the boundary, ever:

1. **Identity of clusters** (name, id, address, OIDC issuer) — at peering.
2. **Forwarded user requests** (§6.3) — on demand, carrying the caller's
   federated identity (§6.4).
3. **Multi-cluster job coordination RPCs** (§7) — submit/plan/rollout
   advance against a peer's copy, again as the calling *user*.
4. **The DR replication stream** (§9) — a dedicated pairing, not general
   peering.

Nothing else. No job rows, no node rows, no token rows, no gossip.

The sections below are ordered: client-side UX (§6.1), peering (§6.2),
forwarding (§6.3), federated auth (§6.4), multi-cluster jobs (§7), standby
managers (§8), DR (§9), UI (§10), service-discovery sketch (§11).

### 6.1 Named clusters in the client (no server changes)

A cluster registry file at `~/.legion/clusters.json` (path override:
`LEGION_CLUSTERS_FILE`), managed by a new CLI noun:

```bash
legion cluster add prod-us --addr https://mgr.us.example.com:4747
legion cluster add prod-eu --addr https://mgr.eu.example.com:4747
legion cluster use prod-us          # sets the default
legion cluster list
legion job list -C prod-eu          # -C / --cluster: one-off target
legion job list --all-clusters      # fan out, merge, add a CLUSTER column
```

- Tokens become per-cluster: `legion login` stores the token keyed by
  cluster name (`~/.legion/tokens/<name>`), fixing today's clobbering.
- Precedence stays backward compatible: `LEGION_ADDR`/`LEGION_TOKEN` env
  vars win over the registry, and with no registry file, behavior is
  byte-identical to today.
- `--all-clusters` in Phase 0 is client-side fan-out over the registry
  (works with zero server support); after peering ships it can also ride
  server-side forwarding (§6.3) so the UI and MCP get the same view.
- The MCP server gains the same `--cluster` targeting and a
  `legion_clusters` tool listing the registry.

### 6.2 Peering

Peering reuses Legion's established "token embeds address + secret" idiom
(worker join tokens, raft join tokens):

```bash
# On prod-us (any manager, needs operator:write):
legion federation invite
# -> lgnfed_...   expires in 15m, single-use

# On prod-eu:
legion federation join lgnfed_...
# -> peered with prod-us (2 clusters in federation)

legion federation members          # name, id, addr, state, last contact
legion federation leave prod-us    # unilateral, either side
```

Mechanics:

- The invite token carries the inviter's cluster id, name, advertise
  address, and a one-time secret. The join is a handshake over a new,
  small `Federation` gRPC service mounted on the existing listener: each
  side presents the secret once, then exchanges `{cluster_id, name,
  advertise_addr, oidc_issuer, jwks}` and stores the peer in a replicated
  `federation_peer` table. Names must be unique within a federation; a
  join that would collide is refused with a rename hint.
- **Steady-state peer authn has no shared secret.** After the handshake,
  every cluster-to-cluster call authenticates with a short-lived JWT
  signed by the caller's workload-identity issuer (`iss` = caller
  cluster, `aud` = target cluster id), verified against the JWKS pinned
  at peering and refreshed via OIDC discovery. This reuses
  `JwtAuthMethod` + the existing issuer machinery rather than inventing a
  credential (§5). Key rotation follows the existing signing-key rotation.
- Peering is **transitive-visible but not transitive-trusted**: joining a
  cluster introduces you to its existing peers' names/addresses (so
  `federation members` agrees everywhere), but each pairwise trust is
  established directly in the same handshake flow. A federation is
  expected to be small (single digits of clusters); full pairwise mesh is
  fine at that scale.
- Requirements and refusals: peering requires TLS on both listeners
  (refused otherwise, with the config to fix it named), compatible
  protocol versions (refused with the upgrade order otherwise), and
  distinct cluster ids (self-peering refused).
- ACL: `invite`/`join`/`leave` gate on `operator:write`; `members` on
  `operator:read`. All are registered in `tests/acl_endpoints.rs`.

### 6.3 Request forwarding: one endpoint sees everything

Any manager can serve requests *about* a peer by forwarding them:

- The CLI/UI/MCP attach a `x-legion-cluster: <name>` gRPC metadata header
  when targeting a non-local cluster (the `-C` flag sets it; no header
  means local, today's behavior).
- A thin routing layer in front of the Manager service checks the header.
  Local name or absent: dispatch as today. Peer name: proxy the call
  bytes to the peer's advertise address over the federation channel,
  attaching the caller's federated identity (§6.4). Unknown name: typed
  `NOT_FOUND` listing known clusters.
- The forwarder retries on the peer's non-leader hint (the typed
  `FAILED_PRECONDITION` names the leader; the forwarder redials it once).
  This deliberately does **not** change the intra-cluster stance of
  "reject + hint, never forward" — a cluster still never silently
  forwards for itself; forwarding is a federation-boundary feature.
- Metadata routing means zero changes to 85 request messages, and new
  RPCs get forwarding for free. The trade-off (not visible in proto
  tooling) is recorded as a judgement call.
- Streaming RPCs (`StreamEvents`, exec) forward as spliced streams, the
  same pattern as the existing manager→worker exec splice
  (`src/manager/exec.rs`).

Merged reads (`--all-clusters`) stay client-driven: the client fans out one
request per cluster (directly or via one manager with N headers) and
merges, labeling rows by cluster. No server ever aggregates federation
state, which keeps hub-store blowup (§2.1) structurally impossible.

### 6.4 Federated identity: authenticate once, authorize locally

The design splits authn from authz at the cluster boundary:

- **Authn federates.** When a manager forwards a request (§6.3), it has
  already authenticated the caller locally. It mints a short-lived
  (60s) **federated identity JWT**: `iss` = its own issuer, `aud` = target
  cluster, `sub` = `fed:<home-cluster>:<principal>`, plus claims for the
  auth method and the caller's policy names. The peer validates it against
  the pinned issuer from peering. Stateless and self-verifying — the
  Vault-batch-token lesson (§2.2): no token replication, no hub, works
  during any partition that leaves the two clusters connected.
- **Authz stays local.** The target cluster maps the claims to *its own*
  policies: by default, the caller's policy *names* apply where policies
  with the same names exist locally (else deny). Operators can add
  explicit binding rules per peer for finer mapping, using the existing
  binding-rules vocabulary:

```jsonnet
acl: {
  federation: {
    // default: matching policy names apply; deny otherwise
    bindingRules: [
      { peer: 'prod-us', claim: 'policies', value: 'ops', policies: ['ops-readonly'] },
    ],
    trustManagement: false,   // a peer's management token is NOT management here
  },
},
```

- `trustManagement` defaults to **false**: a management token on the home
  cluster arrives as an ordinary principal with no matching policies
  unless the peer opts in. This is the deliberate inversion of Nomad's
  replicated global management tokens, trading convenience for the
  boundary P2 users chose clusters to get.
- Direct login to any peer keeps working exactly as today (per-cluster
  break-glass, the Nomad operational lesson). Clusters with ACLs disabled
  forward and accept freely, same as their local posture.

## 7. Multi-cluster jobs

### 7.1 Surface

The jobspec stays native; multi-cluster placement is one new optional
block. Absent block = exactly today's behavior.

```jsonnet
{
  name: 'web',
  clusters: {
    placements: [
      { cluster: 'prod-us', count: 6 },
      { cluster: 'prod-eu', count: 3, meta: { locale: 'eu' } },
    ],
    strategy: {
      maxParallel: 1,        // clusters rolling at once; default: all
      onFailure: 'pause',    // pause (default) | continue | abort
    },
  },
  tasks: [ /* unchanged */ ],
}
```

Per-cluster overrides in v1 are deliberately minimal, matching Nomad's
region params: `count` and `meta` (merged into the job's meta, visible to
tasks and constraints). Arbitrary per-cluster spec patches are deferred;
if two clusters need different *specs*, submit two jobs.

### 7.2 Semantics

- **Submission fan-out, not replication.** The cluster you submit to acts
  as **coordinator for that submission**. It first runs `job plan`
  validation against every named cluster (fail fast, nothing submitted if
  any peer rejects or is unreachable), then submits an independent,
  complete copy to each cluster with that cluster's `count`/`meta`
  applied. Each copy is a normal local job: scheduled, healed, and rolled
  back by its own cluster, forever, regardless of federation health.
- **Coordinated rollout.** The coordinator drives a small per-submission
  state machine (persisted in its own replicated store, resumed across
  its own leader failovers): clusters advance `pending → rolling →
  done` respecting `maxParallel`, gated on each cluster's own
  health-gated rollout machinery. `onFailure`: `pause` stops advancing
  remaining clusters (finished ones stay), `continue` isolates the
  failure to that cluster, `abort` additionally rolls back clusters still
  mid-rollout. Honest limitation, same as Nomad's: clusters that already
  completed are **not** rolled back by `abort`.
- **Observability.** `legion job status web` on any involved cluster shows
  the local copy plus, when reachable, the federated rollout summary
  (fetched from the coordinator). `legion rollout status web
  --all-clusters` merges per-cluster rollout states. Events carry the
  submission's federation id so `legion events` can follow a
  multi-cluster rollout end to end.
- **Peer unreachable mid-rollout:** its placement is marked `unreachable`,
  strategy applies (`pause` by default), and the coordinator retries with
  backoff. The unreachable cluster's *existing* copy keeps running
  untouched — the §4 invariant.
- **Removing a cluster from `placements`** on a later version stops the
  job there (an explicit improvement over Nomad, where removal silently
  orphans the copy). This is safe because stop is idempotent and queued
  until the peer is reachable.
- **Idempotency:** all coordinator→peer mutations carry idempotency keys
  (`Store::idempotency_*`, the existing pattern), so retries across
  partitions and coordinator failovers are safe.

### 7.3 Deferred

Weighted/auto count splitting ("9 replicas, split by capacity") and
cross-cluster failover ("if prod-eu is down, run its 3 in prod-us") are
deferred: both need capacity signaling between clusters and change the
"each copy is a normal local job" story. The `placements` list is the
stable surface they would extend.

## 8. Standby managers (within one cluster)

This answers the first resiliency question: read-only managers that hold a
full copy and step in when a voter is lost.

### 8.1 Learners become a supported role

```jsonnet
persistence: {
  type: 'raft',
  raft: {
    nodeId: 'mgr-d',
    joinToken: 'lgnraft_...',
    standby: true,          // join and REMAIN a learner; never auto-promoted by join
    zone: 'us-east-1c',     // optional; see 8.3
  },
},
```

- A standby manager receives every lane's full replication stream (it is
  an openraft learner on all lanes, machinery that already exists), serves
  the full read API from its local replica, runs no scheduler loops, and
  rejects writes with the existing typed leader hint.
- `legion raft status` reports it as `standby` (not the misleading
  transitional `learner`), including per-lane catch-up lag.
- Promotion remains one command away: `legion raft command add-voter
  --target mgr-d` — now guaranteed safe because openraft only promotes a
  caught-up learner, and our join flow already enforces catch-up-then-
  promote per lane (`src/storage/raft/driver.rs:2228`).

### 8.2 Stale reads become observable instead of implicit

Legion already serves local (possibly stale) reads from every manager.
Rather than adding read modes, we make the staleness visible (the Consul
`stale` lesson: report it, let the client judge):

- Every read response from a non-leader gains trailing metadata:
  `x-legion-applied-lag-ms` (per-lane max) and `x-legion-leader-known`.
- `GetRaftStatus` and `legion raft status` gain per-lane
  `applied_lag_ms`; new gauges `legion_raft_applied_lag_ms{lane}` join the
  existing voter/quorum gauges; `/readyz` on a standby reports
  `role=standby` with the lag.
- The CLI prints a one-line staleness note on reads served with lag above
  a threshold (default 5s), so "you are reading from a standby during an
  outage" is visible, not silent.
- A quorum-checked consistent-read mode (ReadIndex) is deliberately
  deferred until someone needs it; leader-local reads already serve the
  linearizable-enough case since writes are leader-only.

**What this buys during incidents:** with the leader unreachable or quorum
lost, every manager (voter or standby) keeps answering reads — `legion job
list`, the UI, `state query` — with reported staleness. Diagnosis never
requires a healthy write path.

### 8.3 Zones: automatic voter replacement

Autopilot-lite, modeled on Consul/Nomad redundancy zones (§2.2), driven by
the control-lane leader:

- Managers may declare `raft.zone`. The invariant the leader maintains:
  **at most one voter per zone**, standbys are the spares.
- When a voter is unhealthy for longer than a stabilization window
  (default 30s, config `raft.zoneFailoverAfter`), the leader promotes the
  most caught-up standby — same zone preferred, any zone otherwise — and
  removes the dead voter, keeping voter count constant. Both actions are
  ordinary membership commands, recorded in `legion raft history` with a
  `zone-failover` actor, WARN-logged, and event-streamed.
- When the failed zone regains a healthy manager, the leader restores the
  declared spread: promote there, demote the borrowed voter back to
  standby (demotion = `ChangeMembers` to learner, existing machinery).
- Guard rails: zone failover never runs while any lane lacks a leader,
  never reduces voter count, never promotes a learner whose lag exceeds
  the catch-up bound, and is wholly disabled when no `zone` fields are
  configured (zero behavior change for existing clusters).
- 2-zone honesty, stated in docs: zones replace *machines*, not *sites*.
  Two zones with 3 voters still put 2 voters in one zone; losing that
  zone loses quorum. Site-level survival is §9 or a true 3-site stretch.

### 8.4 Deferred: witness voters

A witness (votes, stores no state) would make 2-site deployments quorate
without a third full manager. It is a real openraft/state-machine work
item with a narrow audience; deferred until demand shows up. Documented
here so the option is on the record (Spanner's 2+2+witness is the model).

## 9. DR: a standby cluster elsewhere

This answers the second resiliency question: a full backup control plane
that can take over when the primary site is lost.

### 9.1 Shape

A **DR standby is a separate, complete Legion cluster** (its own quorum,
usually 1 or 3 managers) that continuously replays the primary's state and
serves nothing but reads until promoted. It is *not* a member of the
primary's raft groups: no votes, no effect on the primary's quorum or
write latency (async, off the commit path).

```bash
# On the primary:
legion dr token
# -> lgndr_...  (pairing token: cluster id, addr, one-time secret)
legion dr token --promote > promote-cred.txt   # pre-provisioned, store offline

# On the standby cluster (fresh bootstrap):
legion start manager -c standby.jsonnet    # with:
```

```jsonnet
persistence: {
  type: 'raft',
  raft: {
    nodeId: 'dr-a',
    bootstrap: true,
    drStandbyOf: { addr: 'https://mgr.us.example.com:4747', token: 'lgndr_...' },
  },
},
```

### 9.2 The feed

- The standby's leader subscribes to the primary over a dedicated
  `DrReplication` streaming RPC (federation-style JWT auth, TLS
  required): initial **snapshot install per lane**, then the **committed
  changeset stream per lane** — exactly the bytes every in-cluster
  replica applies, reusing the snapshot-install and changeset-apply
  machinery (§5). Within the standby cluster, replayed writes replicate
  normally through its own quorum.
- Falling behind past the primary's log-compaction horizon degrades to
  snapshot re-install and resumes streaming (raft's own catch-up
  discipline; no unbounded log retention needed on the primary).
- **RPO is measured, not assumed:** `legion dr status` (both sides) and
  `legion_dr_lag_ms` / `legion_dr_last_ack_ts` gauges report per-lane
  replication lag continuously. Alert on it like any SLO.
- The standby rejects all writes with a typed `FAILED_PRECONDITION`
  naming it a DR standby of `<primary>`; reads work with §8.2 staleness
  reporting. ACL state replicates with everything else, so operator
  tokens from the primary work on the standby for reads and for
  `dr status`.

### 9.3 Promotion (manual, deliberate)

```bash
legion dr promote --credential-file promote-cred.txt
```

- Requires the pre-provisioned promote credential (`lgndrp_`, minted on
  the primary while it was healthy, Vault's break-glass pattern) — no
  dependency on the dead primary at promote time.
- Effects, atomically: stop the feed, bump the **cluster generation**
  (§9.4), mark the cluster writable/primary, start scheduler loops, log
  and event the promotion. The standby keeps its own cluster identity —
  it was always a distinct cluster (etcd's new-identity discipline comes
  free).
- Promotion is **manual by default and in v1, full stop.** The standby
  cannot distinguish "primary is dead" from "I am partitioned from the
  primary" (Vault's stated rationale, §2.2); a human (or the operator's
  own external arbiter calling the same RPC) makes that call. What Legion
  provides is everything that makes the manual call fast and safe: the
  lag metric, the pre-provisioned credential, one idempotent command, and
  fencing.

### 9.4 Fencing: the generation check

The failure CockroachDB's PCR documents but does not solve: after
failover, nothing stops clients and workers from still obeying the old
primary. Legion closes it mechanically:

- Every cluster carries a monotonic **cluster generation** (starts at 1;
  DR pairing records the pair; promotion sets the standby's generation to
  `primary_generation + 1`).
- Worker-plane responses carry `(cluster_lineage_id, generation)`. Workers
  persist the highest generation seen for their lineage and **reject
  instructions from a lower generation** with a loud log line. A zombie
  primary can answer its own CLI, but it cannot deschedule, kill, or
  reconfigure workloads on any worker that has heartbeated the promoted
  standby even once.
- The returning old primary is demoted explicitly: `legion dr demote
  --new-primary <addr>` wipes nothing, flips it into standby mode, and
  re-subscribes it to the new primary (snapshot install brings it up to
  date). Until someone runs that, it sits primary-shaped but inert
  against the fleet — safe, and visibly wrong in its own logs, `dr
  status`, and `/readyz`.

### 9.5 Worker re-homing (the RTO story)

- Heartbeat replies gain an ordered `manager_endpoints` list (primary
  advertise addr + DR standby addr). Workers persist it. On prolonged
  manager loss, a worker walks the list; a not-yet-promoted standby
  answers heartbeats with "standby, not promoted — keep trying" (so
  workers never flap), and a promoted one accepts re-registration.
- On takeover, the existing startup heartbeat grace already prevents the
  new control plane from declaring every node lost and descheduling the
  world; reconnecting workers re-assert their allocations and the
  replayed desired state adopts them.
- RTO therefore decomposes into: detection (operator alert on lag/health)
  + one promote command + worker re-home interval (bounded by heartbeat
  cadence and the walk of the endpoint list) + scheduler resume. Minutes,
  dominated by detection and the human decision — which is the design.

### 9.6 The failure math, spelled out

Quorum is `⌊N/2⌋ + 1`; even voter counts add machines without adding
tolerance (already documented in
[docs/multi-raft.md](../../docs/multi-raft.md) §"How many managers?").
What each deployment option actually survives:

| Deployment | Manager loss | Zone loss | Site loss | Writes during failure | RPO / RTO on site loss | Split-brain risk |
| --- | --- | --- | --- | --- | --- | --- |
| 3 voters, one site (today's default) | 1, self-healing election | n/a | **total control-plane outage** (workloads keep running headless) | survive 1 loss | RPO = last `state backup`; RTO = manual restore runbook | none |
| 3 voters + standbys, 3 zones (§8) | any 1, **replaced automatically** | 1 zone, replaced automatically | same as above | yes, throughout | same as above | none |
| Stretched 2+2+1 across 3 low-RTT sites (works today with tuned timings) | 2 | — | any 1 site, automatic | yes (quorum spans sites) | **RPO 0** / RTO ≈ election timeout | none (one quorum) |
| Primary (any shape above) + **async DR standby cluster** (§9) | per primary shape | per primary shape | promote the standby | primary-local speed; standby serves reads | **RPO = replication lag (seconds, measured)** / RTO = detect + promote + re-home (minutes) | real if promotion races a live primary — mitigated by manual promote + generation fencing + demote runbook |
| `streaming` mode + object-storage replica (exists today) | n/a (single manager) | n/a | restore from replica or lease-failover standby | no (single writer) | RPO = sync interval (~seconds) / RTO = restore or lease TTL | lease CAS fences the writer; RPC layer does not (known gap, §5) |

Guidance the docs will state plainly: inside one low-latency site or metro,
use zones (§8). Across true WAN distances, never stretch a quorum
(HashiCorp's <8ms RTT rule); pair clusters with DR replication instead and
accept the measured seconds of RPO. The stretched 2+2+1 row is for the
narrow case of three genuinely low-RTT sites.

## 10. UI: single pane of glass

Server work is nil (gRPC-web + permissive CORS already allow a browser to
talk to N managers). The work is client-side, tracking §6.1:

- `managerEndpoint` becomes a runtime list with a cluster switcher;
  clients and tokens keyed per cluster (fixes the singleton at
  `ui/src/grpcClient.ts:37`); `ConnectionGate` becomes per-cluster; the
  exec WebSocket URL derives per-cluster.
- Jobs/Nodes/Events pages gain an "all clusters" toggle: fan out, merge,
  cluster column — same client-driven merge as the CLI.
- A Federation page under Cluster: members, peering state, last contact,
  DR pairing status and lag.

## 11. Deferred sketch: cross-cluster service discovery

Recorded so the follow-up plan starts from the same shape (MCS-flavored,
§2.1): explicit per-service **export** in the jobspec
(`service: { export: true }` or an allowlist on the peering), peers pull
the exported slice of the catalog (the healthy-endpoints projection
already exists) over the federation channel, imported services resolve
under a cluster-qualified name (`web.prod-eu`), and the datapath question
(who routes the packets) stays out of the catalog contract exactly as the
mesh plan already separates catalog from datapath. Nothing in this plan's
peering surface will need to change for it.

## 12. Proto, struct, config, and persistence changes

**Proto (`proto/legion.proto`)**, all documented per repo rules:

- New `service Federation`: `PeerHandshake`, `GetFederationMembers`,
  `LeaveFederation`, `DrSubscribe` (server-streaming snapshot+changesets),
  `DrStatus`, `DrPromote`, `DrDemote`.
- `Manager` additions: `CreateFederationInvite`, `CreateDrToken` (+
  `--promote` variant), and `manager_endpoints` +
  `(cluster_lineage_id, generation)` on worker-plane replies (additive
  fields, no protocol bump).
- The `clusters` job block on the job-config message; federated rollout
  status message on rollout RPCs.
- No per-message `cluster` field: routing is the `x-legion-cluster`
  header (§6.3).

**Config (`src/structs/config.rs`)**: `cluster: { name }` (defaults to the
raft node's derived id; required for peering), `acl.federation`
(bindingRules, trustManagement), `raft.standby`, `raft.zone`,
`raft.zoneFailoverAfter`, `raft.drStandbyOf`.

**Job config (`src/structs/job.rs`)**: the `clusters` block
(`placements[]`, `strategy`), validated at submit (unknown cluster names,
duplicate entries, `maxParallel` bounds). Added to
`configs/jobs/maximal.jsonnet` when Phase 3 ships.

**New tables (all raft-replicated, control lane)**: `federation_peer`,
`federation_submission` (coordinator state machine),
`cluster_generation` (single row), `dr_pairing`. Worker-side: highest-seen
generation in the worker state file.

**CLI**: `legion cluster …`, `legion federation …`, `legion dr …`, `-C
/--cluster`, `--all-clusters`; every command through the standard output
envelope; `catalog()` in `src/cmd/capabilities.rs` updated; MCP tools for
cluster list/targeting (read-only) and federation status.

## 13. Implementation phases

Each ships alone and is valuable alone. Phases 4 and 5 are independent of
1–3 (they are single-cluster and pairwise-DR features) and can land in
parallel with them.

- **Phase 0 — Named clusters in the client.** `legion cluster` registry,
  per-cluster tokens, `-C`, client-side `--all-clusters`, MCP targeting.
  Zero server changes. *Value: ends env-var juggling; the estate is
  operable from one seat.*
- **Phase 1 — Peering + forwarding.** Cluster identity/name, `legion
  federation invite|join|members|leave`, the `Federation` service,
  header-routed forwarding with leader-hint redial, forwarded streams.
  UI cluster switcher + Federation page. *Value: any manager is a window
  onto the whole federation.*
- **Phase 2 — Federated identity.** Federated JWTs on forwarded calls,
  policy-name mapping + per-peer binding rules, `trustManagement` knob,
  `whoami` showing federated principals. *Value: log in once (S2).*
- **Phase 3 — Multi-cluster jobs.** `clusters` block, plan-everywhere
  validation, coordinator state machine, `maxParallel`/`onFailure`,
  federated rollout status, remove-means-stop. *Value: S3.*
- **Phase 4 — Standby managers.** `raft.standby`, standby role in
  status/UI, staleness metadata + lag gauges + CLI staleness note,
  `raft.zone` auto-replacement with stabilization windows and history
  entries. *Value: S4; voter loss self-heals.*
- **Phase 5 — DR standby cluster.** `dr token|status|promote|demote`,
  `DrSubscribe` feed over snapshots+changesets, lag metrics, cluster
  generation + worker-side fencing, `manager_endpoints` re-homing,
  runbook in `docs/`. *Value: S5; site loss is minutes and seconds, not
  a rebuild.*
- **Phase 6 — Service export/import.** Own plan, per §11.

## 14. Testing plan

- **Unit:** cluster registry resolution/precedence (Phase 0), invite/JWT
  validation and expiry, policy-name mapping and binding rules (2),
  `clusters` block validation and coordinator state-machine transitions
  incl. resume-after-failover (3), zone-failover decision logic under
  every health permutation (4), generation comparison and fencing (5).
- **Integration (in-process multi-cluster harness, extending the
  `tests/raft_durability.rs` pattern to N clusters):** peer two 3-node
  clusters and forward reads/writes/streams both ways; kill a peer
  mid-forward (typed error, local cluster unaffected); full S3 rollout
  with an induced failure per `onFailure` mode; partition the coordinator
  mid-rollout and heal; standby promotion under zone loss and demotion on
  recovery, asserting voter count never dips; DR: feed, kill primary,
  promote, verify a worker rejects the resurrected primary's instructions
  and the demote path resubscribes it; measure and assert reported RPO
  bounds on the DR feed under write load.
- **ACL:** every new RPC classified in `tests/acl_endpoints.rs` (the
  build-failing gate), including `Federation` service methods and the
  worker-plane additions.
- **Perf regression:** forwarding adds one hop — bound it in
  `raft_perf_smoke`-style tests; DR feed under the perf-smoke write load
  must not measurably move primary p99 (it is off the commit path; prove
  it).
- **Skew:** peering handshake across protocol versions (refusal message),
  old workers ignoring new heartbeat fields (proto3 additive, no bump).

## 15. Rollout and backward compatibility

- Everything is opt-in. No registry file, no `clusters` block, no
  `standby`/`zone`/`drStandbyOf` config → byte-identical behavior to
  today, including single-cluster and non-raft modes.
- `LEGION_ADDR`/`LEGION_TOKEN` keep working forever with highest
  precedence; existing scripts and CI are untouched.
- All new wire surface is additive; worker-plane fields ride existing
  messages without a `protocol_version` bump. Peering refuses across
  incompatible protocol versions with the standard upgrade-order message.
- Upgrade order within a federation: any (clusters are autonomous), but
  both sides of a *DR pairing* upgrade primary-first, documented in
  `docs/upgrades.md`.
- New public docs: `docs/federation.md` (peering, forwarding, federated
  auth, multi-cluster jobs), `docs/disaster-recovery.md` (standbys,
  zones, DR pairing, promotion runbook, the §9.6 table), and updates to
  `docs/multi-raft.md`, `docs/raft-operations.md`, `docs/upgrades.md`,
  `docs/agent-readiness.md`, `docs/metrics.md`.

## 16. Open questions

1. **Should `trustManagement` exist at all?** Lean: yes but default
   false; the escape hatch costs little and some single-team federations
   will want it. Revisit if it becomes a foot-gun in practice.
2. **Coordinator handoff.** If the submitting cluster is destroyed
   mid-rollout, the submission state machine dies with it (each cluster's
   copy is still fine and locally managed). Lean: accept for v1 and
   document; `job run` from another cluster re-adopts by federation id
   later if demand appears.
3. **Does the DR standby double as a read replica for latency** (Vault
   performance-replication-shaped)? Lean: it already serves reads (§9.2);
   don't market it as a read-scaling product until staleness UX (§8.2)
   has mileage.
4. **`cluster.name` mutability.** Renaming a peered cluster ripples
   through peers, job blocks, and CLI registries. Lean: immutable while
   any peering exists; rename = leave + rejoin.
5. **Namespaces.** Federated auth would compose better with real
   namespaces (the ACL vocabulary already exists, values don't). Lean:
   keep namespaces out of this plan; nothing here blocks them.

## 17. Acceptance criteria

- **Phase 0:** two clusters operated from one shell with zero env vars;
  `legion job list --all-clusters` labels rows; removing the registry
  file restores today's behavior exactly.
- **Phase 1:** `federation join` peers two live clusters in under a
  minute; `legion node ls -C <peer>` works against either manager;
  killing one cluster leaves the other's CLI/UI fully functional with a
  typed error only on `-C <dead-peer>` calls.
- **Phase 2:** one `legion login` demonstrably operates on both clusters;
  deleting the home-cluster token ends peer access within the JWT TTL;
  a peer with no matching policies denies with a message naming the
  mapping rule that failed.
- **Phase 3:** S3 end-to-end in the integration harness, including the
  `onFailure: 'pause'` path and remove-means-stop; a partitioned peer
  never blocks the healthy cluster's local rollout.
- **Phase 4:** zone-loss drill: voter killed, standby promoted within the
  stabilization window, `raft history` explains it, voter count never
  dipped; recovery restores declared spread; reads during a no-quorum
  window return with staleness metadata.
- **Phase 5:** DR drill in the harness: promote under primary loss with
  the offline credential; measured RPO ≤ configured sync bound under
  write load; a resurrected old primary's kill order is rejected by a
  re-homed worker and logged; `dr demote` resubscribes it cleanly.
