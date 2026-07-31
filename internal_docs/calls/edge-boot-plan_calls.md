# Judgement calls: edge-boot-plan

Plan: [`../plans/edge-boot-plan.md`](../plans/edge-boot-plan.md)

Entries are ranked by project impact, highest first; an entry's number is its
rank. This is a design-time record — nothing here is built yet, so every entry
is a call about the *shape* of the feature, and the ones that survive contact
with Phase 0's measurements will be re-stated as implementation calls.

## 1. Ratification is the existing plan apply, not a new consistency mechanism

- **The call:** an unratified start is confirmed by submitting a normal plan
  submission with one placement and a one-element candidate list, applied
  through `apply_plan_submission_apply` (`src/storage/store.rs:2552`) with its
  epoch checks, its constraint re-validation, and its
  `assign_stateful_instance_tx` call. "The id was claimed by someone else" is
  not a new check — it is the existing stateful-instance binding failing, which
  is what the refusal *is*.
- **Alternatives:** (a) a dedicated `ClaimInstance` op with its own uniqueness
  index on `(job, instance_id)` and its own transaction, which would be simpler
  to read in isolation; (b) an advisory lock in KV taken by the node before
  booting, with the allocation written afterwards.
- **Trade-off:** the ratify path inherits the full weight of the plan applier —
  version vectors, node epochs, network assignment — for what is conceptually a
  one-node bind. It is more machinery per ratification than (a), and it couples
  ratification latency to plan-apply cost.
- **Why:** there must be exactly one place in this system that decides who owns
  an instance. A second path that also binds instances is a second path that
  can disagree with the first, and the disagreement would surface as two live
  sandboxes writing one volume — the precise failure the stateful plan exists
  to prevent. Reusing the applier also means the existing invariant tests cover
  the new path unmodified, which is the strongest evidence available that the
  guarantee still holds. (Simplicity: one owner of the invariant.)

## 2. Three gate levels (`stage`/`boot`/`serve`), not one speculative on/off switch

- **The call:** how far a node may get before the commit is a declared,
  per-task level — pull only, run with the network shut, or run fully — rather
  than a boolean "speculate: true".
- **Alternatives:** (a) a single boolean, with the safe-but-slow interpretation
  (pull only) — simplest surface, and enough for the image-pull win; (b) a
  single boolean with the fast interpretation (boot and serve, kill on loss) —
  the maximal latency win, and the shape a platform builder would ask for first.
- **Trade-off:** three levels is three code paths to build, gate, and test, and
  the `boot` level in particular needs real network-gating mechanism per task
  runtime (§6.7 of the plan) that neither boolean would need. It is the single
  largest cost in the plan.
- **Why:** the levels are not degrees of the same risk — they are *different*
  risks. `stage` risks wasted local work. `boot` risks nothing observable
  outside the node. `serve` risks externally visible side effects from a
  workload that turns out not to own its id. A platform running trusted
  first-party code and a platform running untrusted user code want genuinely
  different answers, and collapsing them into one switch forces the safe user
  to pay the unsafe user's risk or the fast user to pay the slow user's
  latency. Making the risk boundary something you type is what lets the feature
  ship default-on-capable at all. (Ease: the choice is stated in the jobspec,
  not discovered in an incident.)

## 3. Boot tickets are manager-signed capabilities verified offline, not ACL tokens forwarded to the node

- **The call:** an edge start is authorized by a short-lived JWT signed with the
  existing workload-identity signer, carrying job, job config, instance (or
  prefix), pool, gate, and use count; the worker verifies it against a cached
  JWKS with no manager round trip. End users never hold Legion credentials —
  the platform exchanges its own auth for a ticket.
- **Alternatives:** (a) the worker forwards the caller's ACL token to the
  manager for verification — reuses the entire existing auth model and adds no
  new token type, at the cost of a manager round trip on the fast path, which
  is the round trip the feature exists to remove; (b) a shared secret per node
  that the platform signs starts with — no new key management, but a symmetric
  secret at every edge node; (c) mTLS client certs for the edge callers, which
  the worker-plane roadmap wants anyway.
- **Trade-off:** revocation becomes TTL-only. A ticket, once minted, is valid
  until it expires; there is no way to pull it back without breaking the
  offline property. Short TTLs and refusal-at-ratification are the only
  mitigations, and both are documented rather than hidden.
- **Why:** (a) is not a different implementation of the same feature — it is a
  different feature, since a node that must call the manager to authorize a
  start cannot start work when the manager is unreachable, and availability
  independence is half the point. The asymmetric-key shape reuses machinery
  Legion already runs in production (signer, rotation, JWKS endpoint) and keeps
  the trust direction right: nodes verify, they never hold anything that lets
  them mint. (Security boundary: the capability is narrow, offline-checkable,
  and expires.)

## 4. Legion-issued identity is never available before ratification, at any gate level

- **The call:** workload identity tokens are scoped to an allocation id, which
  does not exist until ratification, so an unratified workload cannot
  authenticate to Legion or to any relying party trusting the cluster issuer.
  This holds even at `serve`, where the network is fully open.
- **Alternatives:** issue a provisional-scoped token (claim id in `sub`) so a
  `serve`-gated workload can reach its cloud provider immediately — the thing a
  platform builder will ask for the first week it ships.
- **Trade-off:** a `serve`-gated sandbox that needs cloud credentials at
  startup stalls for the ratification window (single-digit ms to a few hundred
  ms) exactly as if it had waited for the commit — so for that class of
  workload, `serve` buys less than it appears to.
- **Why:** the whole safety argument for `serve` is that a loser's side effects
  are bounded by what an unprivileged workload with a network can do. Handing
  it a cluster-issued identity removes that bound and makes a lost race
  indistinguishable from a won one to every downstream system. The restriction
  also costs no mechanism — it falls out of allocation-scoped issuance —
  which makes it exactly the kind of invariant that survives future
  refactors. (Security boundary that is structural, not enforced by a check.)

## 5. Allocation ids stay manager-minted; the edge addresses sandboxes by instance id

- **The call:** a provisional allocation has a claim id locally and receives its
  real allocation id at ratification. The stable, user-facing handle for the
  whole lifetime is the **instance id** (`user-42`) the platform already chose.
- **Alternatives:** (a) lease blocks of allocation ids to nodes ahead of time
  (one replicated write per block of N, amortized to ~0) so a claim carries its
  final id from millisecond zero — cleaner identity, no rebinding in the task
  manager, logs and exec addressable immediately; (b) mint the id at claim time
  on the node from a node-scoped range.
- **Trade-off:** the worker keeps a claim-id→allocation-id mapping for the
  ratification window, and anything addressed by allocation id (`GetTaskLogs`,
  exec) is unavailable for a provisional allocation until it is confirmed.
- **Why:** (a) requires explicit id assignment in the allocation insert path —
  today ids come from SQLite rowid assignment inside the replicated apply — and
  that is the hottest, most safety-critical write in the store. Buying a
  cosmetic identity improvement with a change to how every allocation in the
  system gets its id is a bad trade, and the trade is unnecessary: keyed
  sandboxes *already* have a stable, caller-chosen identity that survives
  reschedules, and it is the identity the race is about. (Simplicity: no new
  id-allocation subsystem; the handle users type is the one that was already
  stable.)

## 6. Fail-closed everywhere: unratified work dies, and it dies via the reconcile loop that already exists

- **The call:** every failure mode — refusal, timeout, partition, worker
  restart, manager amnesia — ends with the node killing the work. The
  implementation is a *protection window* exempting provisional allocations
  from `reconcile_allocations` (`src/worker/task_manager.rs:762`) until their
  lease expires; after that, the existing "stop anything the manager does not
  list" logic reaps them with no new reaper.
- **Alternatives:** (a) let unratified work keep running while the node retries
  ratification indefinitely, on the argument that a running sandbox is more
  useful than a dead one and the user's request is already in flight; (b) a
  dedicated provisional-allocation reaper with its own timer and its own
  bookkeeping.
- **Trade-off:** a node that cannot reach any manager kills work it could have
  kept running — a control-plane outage becomes a data-plane outage for
  provisional starts, which is precisely the "edge keeps working" property some
  users will want. That property is named as backlog (island mode, Phase 5)
  rather than delivered.
- **Why:** an unratified allocation whose owner is unknown is exactly the
  split-brain the fencing design forbids, and "it kept running because the
  manager was unreachable" is how two sandboxes end up writing one volume.
  Building the kill path out of the reconcile loop rather than beside it means
  the failure mode of a bug in this plan is "the sandbox dies" — the safe
  direction — instead of "a duplicate survives". Island mode can be argued
  later, with its own consistency story, on top of a mechanism that is
  conservative today. (Simplicity plus a safe failure direction.)

## 7. Shared writable volumes are not attached speculatively; unfenceable storage downgrades the gate at admission

- **The call:** a volume a second node could attach concurrently mounts only
  after ratification. Node-local volumes (the `local-dir` default, already
  node-pinned) are exempt because the only possible racer is on the same node,
  where the worker's claim table fences it. A task with unfenceable shared
  storage and `mode: 'boot'|'serve'` is **downgraded to `stage`** at admission,
  with a warning on `job validate`.
- **Alternatives:** (a) reject such a job at admission instead of downgrading —
  louder and impossible to miss; (b) allow the speculative attach and rely on
  ratification racing faster than corruption, which is not a guarantee anything
  can make.
- **Trade-off:** a silent-ish performance cliff — a job that asked for `boot`
  gets `stage`, and the only signals are a validate warning and a metric. A
  user who does not read the warning sees "edge boot did nothing".
- **Why:** (b) trades data integrity for latency, which is never the trade
  Legion should make. (a) is tempting, but the same jobspec is legal and useful
  the moment the driver grows a fencing capability, and rejecting a job for a
  *driver's* current capabilities makes the jobspec non-portable between
  clusters. Downgrading keeps the job running everywhere and degrades exactly
  the thing that cannot be delivered safely. (Extensibility: the ceiling is a
  driver capability, not a hard-coded rule.)

## 8. The `boot` gate is a strong network boundary for `sandbox` and a weaker one for host runtimes — and the docs say so

- **The call:** ship the network gate with per-runtime honesty: for `sandbox`,
  the TAP interface is held down with a default-drop filter and opened at
  ratification (a real boundary); for `container`/`exec`, the gate covers
  service registration and the loopback upstream listeners, and host egress is
  the operator's problem. State this asymmetry prominently in the public doc.
- **Alternatives:** (a) build real host-level egress filtering for all runtimes
  before shipping `boot`, so the mode means one thing everywhere; (b) restrict
  `boot` to the `sandbox` runtime and reject it elsewhere.
- **Trade-off:** the same jobspec word means different strengths of guarantee
  depending on the runtime — the kind of inconsistency that gets discovered
  during an incident if the documentation is weak.
- **Why:** (a) means owning a host firewall datapath across Linux distros and
  macOS before any of this ships, which is a networking subsystem, not a
  scheduler feature. (b) is defensible and remains the fallback if the docs
  cannot carry the nuance — but the gate has real value for host runtimes too
  (nothing routes to an unratified allocation), and refusing it there would
  push users toward `serve`, which is strictly worse. The honest-documentation
  path keeps the weaker guarantee available and labeled. (Ease, with the
  limitation stated publicly rather than discovered.)

## 9. Phase 0 measures before anything is built, and is allowed to kill the plan

- **The call:** the first phase produces no feature at all — it instruments the
  real start path (submit → eval → plan → apply → dispatch → prepare → boot →
  first byte) across persistence modes and image warmth, and records the
  control-plane share of time-to-first-byte as a number in
  `benchmarks/scheduler/README.md`. The later phases are justified against it.
- **Alternatives:** build Phase 1 first — the latency argument is
  self-evidently true in the cross-region case, and the measurement can follow
  the code.
- **Trade-off:** a phase of work that ships no user-visible capability, on a
  feature the user has already asked for.
- **Why:** the entire plan is a latency argument, and the sizes of its terms
  decide its shape: if a warm-image sandbox boots in 80 ms and the commit costs
  3 ms, then `stage` is the whole feature, the ticket machinery is premature,
  and the honest recommendation is snapshots
  ([sandbox-snapshots-and-fast-start-plan.md](../plans/sandbox-snapshots-and-fast-start-plan.md))
  instead. Building four phases of gating machinery to save 3 ms would be the
  most expensive mistake available here. It is also the repo's own rule —
  scheduler changes ship with before/after numbers — applied before the change
  rather than after. (Performance work needs a baseline; this one needs it
  before the design is frozen.)

## 10. Edge boot is off by default three times over

- **The call:** `speculativeStart` absent from every jobspec, `edgeBoot.enabled:
  false` on the manager, `edgeBoot.acceptClaims: false` on the worker. All three
  must be turned on for an edge-initiated start to be possible.
- **Alternatives:** a single cluster-level switch, with the job-level field
  assumed to imply consent.
- **Trade-off:** three places to configure, and a user whose jobspec says
  `mode: 'boot'` on a cluster that has not enabled it gets a job that quietly
  behaves as `off` (surfaced by a validate warning and a metric, but still a
  gap between what the spec says and what happens).
- **Why:** the job-level field is a *job author's* latency choice; the
  manager-level switch is a *security admin's* decision to allow a new
  authorization surface; the worker-level switch is an *operator's* decision
  about a specific node, which matters because edge nodes and core nodes have
  different exposure. These are three different people with three different
  blast radii, and collapsing them means one of them makes a decision that is
  not theirs to make. (Security boundary: consent is required from whoever owns
  the risk.)
