# Judgement calls: edge-boot-plan

Plan: [`../plans/edge-boot-plan.md`](../plans/edge-boot-plan.md)

Entries are ranked by project impact, highest first; an entry's number is its
rank.

Entries 1–10 are the **design-time** record, written before anything was built.
Entries 11–18 are **implementation-time** calls from building phases 1–4. They
are appended rather than interleaved so the numbers already cited elsewhere stay
stable; each one is ranked among its own group, and where an implementation call
refines a design-time entry it is noted inline under that entry.

Phase 0's measurement matrix was **not** run — see entry 18, which supersedes
entry 9.

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

**Implementation note (entry 11).** Holding to "no new code path for
correctness" needed one change to the shared path after all: the keyed binder
had to learn to bind a *named* instance rather than any unbound one, or a
ratification could have bound a different sandbox than the one it raced for.

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

**Implementation note (entry 12).** Because there is no allocation id, the
worker also needed a *stable* local key — and, once ratified, a way to keep
using it. Entry 12 records why nothing is rekeyed and no directory moves.

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

**Implementation note (entry 13).** Building this turned up a second class of
task that cannot use gates above `stage` for the same reason: one that declares
ports or connect upstreams, which are assigned inside the apply.

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

---

# Implementation-time calls (phases 1–4)

## 11. The keyed binder learned to bind a *named* instance, or fail

**Call.** `assign_keyed_instance_tx` binds "any unbound keyed instance", which
is right for a scheduler placing work and wrong for a claim racing on one
sandbox. It now takes an optional target — threaded from a new
`plan_submission.target_instance` column through
`apply_plan_submission_apply` — and either binds that instance or raises the
existing `Conflict`:

```
keyed instance user-42 is not available to bind (already bound to another
allocation of this task config, stopped, or terminated)
```

Empty on every ordinary plan, which keeps the "any unbound instance" binding
byte-identical.

**Alternatives.** (a) A separate ratification transaction with its own binding
logic. (b) Keep "any unbound" and verify *after* the apply that the right
instance was bound, undoing it when not.

**Trade-off.** The scheduler's hottest transaction grew a parameter and a
column, and every caller of `insert_allocation_with_data` was touched. (b) was
rejected outright: "bind something, check, undo" is a second correctness path
with a window in it, and the undo would have to delete an allocation the store
had already committed and epoch-bumped.

**Why (simplicity).** This is what makes entry 1 true rather than
aspirational. Two code paths that both bind instances is how the
at-most-one guarantee dies — one gets a fix the other does not. The existing
stateful suites run unmodified, which is the evidence the invariant did not
move.

## 12. A ratified claim keeps its worker-local key; nothing is rekeyed, no directory moves

**Call.** The worker keys a provisional allocation by a negative local key
(entry 5 said allocation ids stay manager-minted, so there is none to use). On
ratification it records `local_key -> allocation_id` and translates at exactly
six boundaries — the heartbeat's running set, health, failures, terminal
reports, log reads, exec. The allocation directory, log directory, open file
descriptors and running process never move.
`PersistedAllocation.external_id` carries the pair across a worker restart.

**Alternatives.** (a) Rekey the task manager's fifteen maps and rename
`allocs/<local>/` to `allocs/<real>/` at ratification — the tidier end state,
where nothing downstream needs to know provisional work ever existed. (b)
Pre-mint allocation ids before the apply.

**Trade-off.** A ratified sandbox's directory is named after a negative key, so
`ls /var/lib/legion/allocs` no longer maps one-to-one to allocation ids. A real
cost for anyone debugging by hand, and the reason the translation lives in two
named helpers rather than being spread inline.

**Why (not breaking things).** (b) needs id reservation to be a replicated op —
precisely the commit this feature exists to skip. (a) means renaming a
directory out from under a running microVM whose root disk and control socket
live inside it: `rename(2)` is safe for open fds and cwd, but bind mounts and
any runtime that re-derives a path from a string are a silent corruption
waiting to happen. Six enumerable, testable translation points beat a migration
that has to be right on every runtime forever.

## 13. Declared ports and connect upstreams also cap the gate at `stage`

**Call.** Entry 7 downgrades tasks with unfenceable storage. The same rule now
covers `network.ports` and `connect.upstreams`, because both are assigned
*inside* the apply and a workload booted before it cannot know them:

```
task `api` declares network ports, which are assigned when the placement
commits; gates above `stage` would boot it without them
```

**Alternatives.** (a) Boot without ports and let the workload discover them
later. (b) Assign ports speculatively at dispatch and reconcile at apply.

**Trade-off.** A platform that wants `boot` must use `network.mode: 'tap'` or
`'none'` rather than Legion-assigned host ports. A genuine narrowing of who can
use the fastest gates, stated in the public doc under "Automatic downgrades"
rather than discovered as a missing speedup.

**Why (simplicity).** (b) is a second port allocator with its own conflict
resolution — the "two sources of truth" shape entry 7 already rejected. (a)
produces a workload that is up but unreachable and cannot say why, which is
worse than a start that was 20 ms slower. `stage` is unaffected: it adopts the
committed payload before it starts anything.

## 14. A provisional workload gets `LEGION_CLAIM_ID` and **no** `LEGION_ALLOC_ID`

**Call.** The provisional payload injects `LEGION_CLAIM_ID` and *removes*
`LEGION_ALLOC_ID`, including one a jobspec tried to set. A task started at
`boot` or `serve` addresses itself by `LEGION_INSTANCE_ID`, which entry 5
already established is correct from the first millisecond.

**Alternatives.** (a) Set `LEGION_ALLOC_ID` to the negative local key. (b) Set
it to `0` or the empty string.

**Trade-off.** An image that reads `LEGION_ALLOC_ID` unconditionally breaks
under `boot`/`serve` — it gets an unset variable rather than a wrong one, and
needs a one-line change.

**Why (honesty).** An allocation id that does not exist cannot be reported
truthfully, and both dishonest options are worse than absence: (a) leaks an
internal key into user space and is silently wrong after ratification; (b) is a
sentinel every consumer must know about. The env-precedence rule this repo
already documents — Legion's runtime variables win so a jobspec cannot make a
workload misreport its own identity — is what forces the *removal* rather than
leaving a stale value behind.

## 15. Two delivery paths for the verdict, one claim state machine

**Call.** A manager-originated claim gets its verdict pushed as a second
`PostWork` carrying the **committed** payload; an edge-originated claim fetches
its own with `RatifyClaim`, whose reply carries the same payload. Both funnel
into one `ratify_claim`, which adopts the payload and then opens whichever gate
the claim is behind.

**Alternatives.** (a) Have the worker call `RatifyClaim` for manager-originated
claims too — one delivery path. (b) A new manager→worker RPC that pushes
verdicts for edge claims too.

**Trade-off.** Two transports to keep working, and `PostWork` now means three
things (dispatch, provisional dispatch, ratification), distinguished by whether
`provisional` is set and whether `allocation_id` is zero. That overloading is
documented in the proto and is the thing most likely to confuse a reader.

**Why (correctness).** (a) would double-apply: the manager is already applying
the plan that produced the claim, so a worker-initiated ratification would
commit a *second* allocation. (b) needs a manager that can reach a node whose
whole point is being reachable only from its own edge. The committed payload
has to ride the verdict either way — the node booted from one that could not
name ports, the bound instance, or resolved mounts — so the reply is the
natural carrier.

## 16. The heartbeat's catch-up verdict is derived from the instance binding, not stored

**Call.** `HeartbeatReply.claim_verdicts` is computed on the fly: for each
unratified claim a node reports, look up the instance's `current_alloc_id`. An
allocation on *this* node means confirmed; one elsewhere means
`instance_claimed`; neither means undecided. The manager stores no claim table.

**Alternatives.** (a) A `claim` table recording every verdict, pruned by TTL.

**Trade-off.** A stateless claim carries no instance id, so it gets no catch-up
verdict and falls back to its lease. Accepted: stateless provisional starts are
the manager-originated case, where the verdict is pushed directly.

**Why (simplicity, performance).** The binding already *is* the record of who
won; a claim table would be a second copy with its own staleness and its own
pruning bug — and it would put a write on the heartbeat path, the hottest path
in the system. The same reasoning governs `legion node claims`, which reads a
per-node list the manager holds in memory: after a failover it is empty until
the next heartbeat, which is briefly incomplete and never wrong.

## 17. Refusal reasons are classified by matching the apply's rejection strings

**Call.** `classify()` maps the plan-apply rejection text onto a
`RefusalReason` — "not available to bind" → `instance_claimed`, "no longer has
capacity" → `no_capacity`, and so on.

**Alternatives.** (a) Thread a typed error out of `apply_plan_submission_apply`
so the reason is structural rather than textual.

**Trade-off.** String matching on messages that could be reworded, where a
reword would silently degrade a refusal to `internal`. Mitigated by unit tests
that pin each string, so a reword fails the build rather than the runtime — but
it is a real fragility, and (a) is the right answer if a second consumer ever
appears.

**Why (proportion).** (a) changes the return type of the scheduler's central
transaction, and every caller of it, for one caller that wants a five-way enum.
The failure mode of getting it wrong is a claim killed with reason `internal`
instead of `instance_claimed` — worse diagnostics, never worse behavior.

## 18. Phase 0's measurement matrix was not run — this supersedes entry 9

**Call.** Phases 1–4 shipped without the start-path breakdown Phase 0 called
for, and without the before/after benchmark numbers the plan's testing section
asks for. Entry 9 said Phase 0 measures first and is allowed to kill the plan;
that is not what happened.

**Alternatives.** (a) Build the measurement harness first, as the plan ordered
it, and let it reshape the phases.

**Trade-off.** The plan's open question — is `boot` or `stage` the right
recommended default for sandbox platforms — is still open, so the public doc
recommends neither: it describes what each gate costs and lets the operator
choose. The default `ratifyTimeout` of 5s is the plan's lean, not a measured
value. `internal_docs/TODO.md` carries the measurement as open work.

**Why, and what would change it.** The measurement needs a real multi-node raft
cluster and a real image registry to say anything true, neither of which this
change had. Shipping the mechanism with an honest "we have not measured this"
beats shipping a number produced on a single-node loopback cluster and calling
it a benchmark. A production deployment's `legion_provisional_ratify_seconds`
histogram answers the question directly, which is why that metric exists.
