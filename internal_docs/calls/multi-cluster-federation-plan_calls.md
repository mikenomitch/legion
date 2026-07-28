# Judgement Calls: Multi-Cluster Federation and Manager Resiliency

Plan: [`plans/multi-cluster-federation-plan.md`](../plans/multi-cluster-federation-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.
These calls were made at planning time (2026-07-28), before any code.

## 1. Symmetric peering, no authoritative hub

**Call.** Federation is pairwise, token-exchanged peering between autonomous
clusters (plan §6.2). No cluster is special: no authoritative region, no
replicated meta-plane, no shared gossip pool, no hub store.

```bash
legion federation invite            # on either cluster
legion federation join lgnfed_...   # on the other
```

**Alternatives.** (a) Nomad's model: a designated authoritative region
replicating ACL policies, global tokens, and namespaces to all members —
proven, and it gives real single-source-of-truth semantics for the meta
plane. (b) A dedicated hub cluster (Rancher/Karmada shape) owning the
fleet view.

**Trade-off.** No hub means no global objects at all: nothing is "created
once, exists everywhere" (policies must be applied per cluster or mapped by
name), and a fleet-wide view is always a fan-out, never a lookup. Small
federations lose a little convenience every day to buy resilience for the
rare bad day.

**Why (simplicity + the P2 boundary).** Every documented painful failure
mode in Nomad/Consul federation is "the hub is down" (no token creation, no
meta writes, no bootstrap, all multi-region deploys fail), and HashiCorp's
own second-generation design (Consul cluster peering) removed the hub for
exactly that reason — we skip their first generation. It also preserves
what P2 users chose clusters *for*: hard boundaries. A hubless federation
has no cross-cluster availability coupling to explain at 3am.

## 2. Authn federates, authz never does

**Call.** Cross-cluster identity is a short-lived JWT signed by the home
cluster's existing OIDC issuer; the target cluster validates it against the
JWKS pinned at peering and maps it to *its own* policies (matching policy
names by default, per-peer binding rules for the rest). No token rows ever
replicate. A peer's management token is not management locally unless
`acl.federation.trustManagement: true` (plan §6.4).

**Alternatives.** (a) Nomad global tokens: replicate tokens/policies from
a hub — one credential literally exists everywhere, but requires a hub and
a live replication link, and revocation lag is replication lag. (b) Mint
per-cluster tokens automatically at peering (credential sprawl with worse
auditability).

**Trade-off.** "Same policy name, different meaning per cluster" is now
possible and is the operator's responsibility; a principal's effective
rights are only inspectable per cluster (`whoami` against each). Default-
deny on unmapped principals means federation reads are not magically open
after peering — one more setup step before S1's `--all-clusters` shines
under enforced ACLs.

**Why (extensibility + security).** The machinery already exists in-repo
(each manager is an OIDC issuer with discovery/JWKS; `JwtAuthMethod`
validates external JWTs with binding rules), so this is the smallest
mechanism that works — and it is the *only* credential shape that keeps
working across partitions with no hub, the lesson Vault's batch tokens
encode. Authorization staying local is the security boundary P2 promises:
compromising one cluster's admin plane grants nothing anywhere else by
default.

## 3. Job state is never replicated between clusters

**Call.** A multi-cluster job is N independent, complete local jobs plus a
coordinator state machine on the submitting cluster (plan §7.2). Each
cluster schedules, heals, and rolls back its copy using only local state,
regardless of federation health.

**Alternatives.** A replicated job store (submit once, exists everywhere,
survives the submitting cluster) — genuinely better DR for job *specs*,
and what "federation" naively suggests.

**Trade-off.** The coordinator is mortal: destroy the submitting cluster
mid-rollout and the cross-cluster rollout orchestration dies with it (each
copy remains locally healthy and locally managed). Job specs have no
cross-cluster DR beyond §9's whole-state replication. Accepted and
documented (open question 2 tracks re-adoption by federation id).

**Why (performance/scale + the §4 invariant).** Nomad's strongest property
— "running workloads are never interrupted due to federation failures" —
falls directly out of share-nothing job state, and Rancher Fleet's own
scaling experiment (dead at 200k per-cluster×bundle rows in the hub store)
shows where replicated bookkeeping ends up. Cross-cluster consensus on job
rows would also put a WAN in the write path Legion just spent the raft
work keeping fast.

## 4. Placement lives in the jobspec, not a separate policy object

**Call.** Multi-cluster placement is one optional `clusters` block in the
job (plan §7.1), Nomad-`multiregion`-shaped, with per-cluster overrides
limited to `count` and `meta` in v1.

```jsonnet
clusters: {
  placements: [
    { cluster: 'prod-us', count: 6 },
    { cluster: 'prod-eu', count: 3, meta: { locale: 'eu' } },
  ],
  strategy: { maxParallel: 1, onFailure: 'pause' },
},
```

**Alternatives.** Karmada's split: native spec untouched, a separate
PropagationPolicy/OverridePolicy object selects and places it. This is the
K8s ecosystem's headline adoption lesson, and it lets platform teams place
workloads whose specs they don't own.

**Trade-off.** We give up spec/placement separation: a team that wants to
place jobs it cannot edit has no hook, and rich per-cluster spec patches
have no natural home (deliberately deferred rather than inventing a patch
language). If Legion later grows a policy layer, this block becomes the
inline shorthand for it.

**Why (simplicity).** Karmada's separation solves a K8s-specific problem —
retrofitting placement onto an API you must not change and specs you don't
own. Legion owns its jobspec and its users author their jobs; a second
object type (new noun, new RPCs, new ACL resource, new UI page, and a
matching problem between policy and job) is the K8s-shaped complexity this
product exists to avoid. One block, visible in the job you're reading, is
explainable in a sentence. KubeFed's actual killer — a *parallel wrapped
API* — is not this: the spec stays native and the block is optional.

## 5. DR promotion is manual, and fencing is mechanical

**Call.** `legion dr promote` is a human (or operator-owned automation)
decision requiring a pre-provisioned offline credential; Legion ships no
auto-failover (plan §9.3). What *is* mechanical: a monotonic cluster
generation carried on the worker plane, so workers reject instructions
from a fenced ex-primary (§9.4).

**Alternatives.** (a) Built-in auto-failover on lost contact — the
"obvious" feature request. (b) Vault/CRDB status quo: manual promote with
*procedural-only* fencing (DNS flips, demote runbooks), no mechanical
guard.

**Trade-off.** Manual promotion puts human detection/decision time squarely
in the RTO (minutes, not seconds), and 3am promotion is a runbook step
someone must actually run. The generation check adds a small worker-plane
field and a persisted high-water mark to reason about forever.

**Why (correctness over RTO).** An async standby cannot distinguish a dead
primary from a partition; auto-promotion without an external arbiter
manufactures split-brain, which is why Vault refuses to ship it and says so
in its docs. We take that position *and* fix the gap CockroachDB's PCR
docs admit ("no mechanism to stop applications from connecting to the
original primary"): with generation fencing, the worst zombie-primary
outcome is a confused CLI, never a descheduled fleet. Auto-failover can be
layered later by operators driving the same RPC from their own arbiter —
the API is built for it, the default is not.

## 6. Standby managers are raft learners, and stale reads are reported, not moded

**Call.** The standby role is `raft: { standby: true }` — a permanent
openraft learner on every lane, promoted by the existing membership
commands or by zone auto-replacement (plan §8). Reads keep today's
semantics (every manager answers from local state); we add staleness
*reporting* (`x-legion-applied-lag-ms` metadata, lag gauges, a CLI note)
rather than Consul-style per-request consistency modes.

**Alternatives.** (a) A new replication tier for read replicas, separate
from raft membership (how several databases do read scaling). (b) The full
Consul menu: `default`/`consistent`/`stale` selectable per request, with
leader-side ReadIndex for linearizable follower reads.

**Trade-off.** No consistent-read-from-follower mode: a client that needs
read-your-writes must talk to the leader, same as today. Learners receive
every lane's full stream, so a WAN standby is a full-fidelity replica —
there is no filtered/cheap variant. Reads during quorum loss are served
with only advisory staleness (nothing refuses "too stale" yet).

**Why (simplicity + reuse).** The learner machinery, catch-up gating, and
membership commands already exist and are tested; `standby: true` is
config plus role reporting, not a subsystem. Making the existing behavior
*observable* beats adding modes nobody asked for — and matches how the
incident actually feels: during an outage you want any answer with an
honest lag number, which is exactly Consul's `stale` + `X-Consul-
LastContact` lesson. ReadIndex reads remain an additive follow-up if
demand appears.

## 7. Cross-cluster routing is a metadata header, not a field on 85 messages

**Call.** Forwarding is keyed on `x-legion-cluster` gRPC metadata, handled
by a routing layer in front of the Manager service; proto messages are
untouched and new RPCs inherit forwarding for free (plan §6.3). The
forwarder redials once on the peer's non-leader hint. The intra-cluster
stance ("reject + hint, never forward" within a cluster) is deliberately
unchanged.

**Alternatives.** (a) Nomad's shape: a `region` field on every request
message — visible in every generated client and in `grpcurl`, but ~85
message edits now and one more on every future RPC, forever. (b) No
server-side forwarding at all: clients fan out directly (CLI could, but
the UI/MCP single-endpoint story dies).

**Trade-off.** Routing is invisible to proto tooling: nothing in
`legion.proto` says forwarding exists, so it must be documented prominently
(proto file header comment + `docs/federation.md`), and generic gRPC
clients must know a header convention rather than a field. Debugging "why
did this go to prod-eu" requires looking at metadata, not the request body.

**Why (extensibility + maintenance).** One enforcement point instead of a
per-RPC convention that every future endpoint author must remember — the
same argument as the one-line `check_acl` pattern this repo already bets
on. Choosing forwarding at all (vs. client-only fan-out) is what makes one
manager a full window for the browser UI and MCP agents, which is the P3
job to be done.

## 8. Cluster-granular placement only — no fleet-wide task scheduler

**Call.** The federation places *jobs on clusters* (explicit per-cluster
counts in v1); each cluster's scheduler places tasks on nodes. Nothing
schedules an individual task across a cluster boundary, and no cluster
appears as a giant "node" of another (plan §4 non-goals, §7.3).

**Alternatives.** (a) Virtual-kubelet style (Admiralty/Liqo): peer
clusters as nodes, one scheduler places everything — real cloud-bursting,
zero new placement API. (b) Weighted auto-splitting ("9 replicas, best
placement across the federation") in v1.

**Trade-off.** No automatic capacity chasing: if prod-eu is full, its
placement fails there rather than spilling into prod-us; rebalancing
across clusters is the operator editing counts. Auto-split and
cross-cluster failover are deferred until capacity signaling exists
between peers.

**Why (performance + the ecosystem verdict).** A cluster-sized "node"
distorts every scheduler assumption (bin-packing, spread, dynamic ports,
volume locality) and would put a WAN inside eval loops Legion benchmarks
and gates on. The entire surviving multi-cluster ecosystem (Karmada, OCM,
Fleet, Azure Fleet, Google MCO) converged on cluster-granular placement;
the pod-level family stayed niche for exactly the leaky-abstraction
reasons above. Explicit counts are also the honest v1 of a surface that
weighted splitting can extend without breaking.

## 9. Removing a cluster from `placements` stops the job there

**Call.** Deviation from Nomad: dropping a cluster from the `clusters`
block on a new job version stops that cluster's copy (idempotent, queued
until the peer is reachable) rather than orphaning it (plan §7.2).

**Alternatives.** Nomad's documented behavior: removed regions "will not
be stopped and will be ignored by the deployment" — safer against fat
fingers, and it never needs the peer to be reachable at edit time.

**Trade-off.** A mis-edit can stop real workloads in a real region. The
spec-diff surface (`job plan`) must therefore show the stop prominently
("this version STOPS web in prod-eu (3 running)"), which is a hard
requirement on Phase 3, not a nicety.

**Why (simplicity — the spec is the truth).** An orphaned copy that the
spec no longer mentions is state drift by design: invisible in the file,
billed forever, discovered by surprise. Legion's declarative story is that
the jobspec describes the world; a placement that survives its own removal
breaks that contract worse than an explicit, plan-visible stop does.

## 10. Deferred with intent: service discovery, witnesses, weighted splitting

**Call.** Three real features are named, shaped, and deferred rather than
half-built: cross-cluster service export/import (plan §11 — the peering
surface is designed so the MCS-shaped follow-up needs no changes here),
witness voters for 2-site quorums (§8.4), and weighted count splitting
(§7.3).

**Alternatives.** Fold any of them into v1 — service discovery in
particular is the most-requested companion to federation.

**Trade-off.** A federation without service export answers "where is it
running" but not "how does my eu service call the us database" — users
will hit that wall and must be pointed at the follow-up plan. Witness-less
Legion has no great answer for exactly-two-sites-with-RPO-0 (the honest
answer today is the DR standby with seconds of RPO).

**Why (phasing discipline).** Each deferral has a stable extension seam in
the shipped surface (peering channel + catalog projection; membership
commands; the `placements` list), so deferring costs no rework — while
pulling any of them in would couple this plan to the mesh datapath
question, an openraft state-machine change, and a capacity-signaling
protocol respectively, each of which deserves its own plan per repo rules.
