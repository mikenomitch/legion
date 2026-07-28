# Judgement Calls — Durability and Ops Execution

Companion to
[`plans/durability-and-ops-execution-plan.md`](../plans/durability-and-ops-execution-plan.md).
Started 2026-07-28 with the JC-40 lane-leadership correction; earlier phases of
that plan recorded their calls inline in the plan's `JC-*` sections, which stay
where they are. Extended the same day with the standing-split resilience
workstream (plan §8) and the adversarial-verification fixes.

Entries are ranked by project impact, highest first.

## 1. Worker-plane writes FORWARD to the lane leader; client-plane writes still reject with a hint

**The call.** A manager that cannot propose a worker's write (heartbeat,
health check, allocation-health snapshot, failure report, volume report —
plus the daemon's cross-lane halves like `sync_node_availability` and eval
enqueues) forwards the named operation with its full arguments to the owning
lane's leader over a new authenticated `RaftTransport.ForwardLaneWrite` RPC,
and answers the caller only after the forwarded write commits. Client-plane
RPCs keep the reject-plus-leader-hint contract.

**The alternatives.** (a) Keep reject-and-retry everywhere (the shipped
state). (b) Make workers chase per-lane leader hints. (c) Forward
*everything*, client plane included. (d) Suppress mark-lost while lanes are
split instead of fixing the write path.

**The trade-off it costs.** A new manager-plane wire surface (a closed serde
op enum, versioned with the transport), one extra network hop per affected
write while leadership is away, and a second code path (`*_local` executor
variants) per forwardable op.

**Why it won.** The verification pass priced alternative (a): workers dial
ONE manager forever, so a standing status-lane split away from that manager
rejected every heartbeat — the status leader marked every healthy worker
lost in ~10-12s from replicated staleness, stateless workloads double-ran
for the whole split, and each worker's self-fence watchdog killed its
stateful allocations at ~90s. A leadership shape became a data-plane
outage, and no reconciler can help: a heartbeat is new information, not
derivable state. (b) turns every worker into a raft-topology client and
still breaks composites that span lanes. (c) is unnecessary — clients can
redial, and everything client-facing is anchored on the control leader —
and read-your-writes over a forwarded client op would surprise (the local
replica lags the leader's commit). (d) masks the symptom and still lets
the self-fence fire. Forwarding also fixes plain `single`-topology
failover: workers pointed at a demoted ex-leader keep working through it.
Loop safety is structural: the executor runs the non-forwarding variant on
a store clone whose placeholder driver cannot resolve a forwarding target,
so a stale hint costs one typed rejection, never a cycle.

**User-facing surface.** None on workers — that is the point. Operators:
`docs/multi-raft.md` § "Non-leader write redirects and forwarding",
`ForwardLaneWrite` proto comments, and a rolling-upgrade note in
`docs/upgrades.md` (forwarding to an old manager degrades to the old
reject-and-retry).

## 2. Convergence never campaigns for a leaderless lane (JC-40 correction)

**The call.** The lane-convergence loop acts *only* on a lane that already has
a settled leader elsewhere. A lane with no leader is left entirely to raft.

**The alternatives.** (a) The shipped-and-broken behavior: campaign for a
leaderless lane immediately. (b) The first attempted fix: campaign only after
a grace window of 2x `electionTimeoutMaxMs`, on the reasoning that "the anchor
may be the only viable winner, and sitting out deadlocks the lane just as
hard". (c) Campaign but only when a local log check says we could win.

**The trade-off it costs.** If the anchor leader genuinely *is* the only node
that can win a leaderless lane, convergence contributes nothing to restoring
it — recovery waits on raft's own election timers, which are slower to fire
than an immediate external campaign.

**Why it won.** Reasoning (b) is simply false, and the openraft 0.9 source
says so: `RaftCore::handle_tick_election` campaigns on **every** non-leader
voter — this node included, candidates included — once its randomized timer
expires. Convergence standing aside does not stop this node from campaigning;
it stops this node from campaigning *twice*, on two uncoordinated schedules.
And the second schedule is strictly worse: `ExternalCommand::Elect` calls
`Engine::elect()` directly, skipping the leader-lease check and the
`smaller_log_timeout` deference openraft applies when a vote response revealed
a longer log elsewhere. Empirically (2-core runner, openraft tracing) the
extra campaigns restarted the node's own candidacy faster than the election
could finish: 132 terms in 210 s on the status lane, applied index frozen,
with both survivors' logs *identical*. Option (c) cannot be built honestly —
"is my log as current as the quorum's" is not locally knowable for a
non-leader in raft; that is precisely what an election decides.

**User-facing surface.** `docs/multi-raft.md` § "Lane leadership: co-located,
best effort" now states the guarantee as *a lane always elects*, with
co-location subordinate to it.

## 3. Job submit is ACCEPTED by the control leader; derivable cross-lane projections tolerate rejection and reconcile

**The call.** The control half of `handle_job_submit` — the operator's intent
— must commit locally (typed rejection otherwise, unchanged). Its cross-lane
projections (the `task_status` seed, the volume-claim sync), the plan-apply
status follow-up (`update_task_statuses_for_allocations`), and the RPC tail's
regenerable pieces tolerate exactly the typed `NotRaftLeader` rejection:
warn, succeed, and let the owning lane's leader complete the projection —
`seed_missing_task_statuses` and `sync_missing_task_volume_claims` run on the
checker cadence, worker reports upsert statuses anyway, and eval enqueues
forward (call 1). `handle_rollout_tick` likewise no longer stamps a rollout
terminally `failed` on a `NotRaftLeader` — it releases the eval for retry.

**The alternatives.** (a) Keep "a submit that spans a split has no home"
(the documented cost). (b) Forward the derived halves like call 1's ops.
(c) Wrap the composite in distributed-transaction machinery.

**The trade-off it costs.** Brief observable incompleteness: a just-accepted
job can show tasks without status rows, and claims/rollouts can appear a
checker tick (~2s) late. Store-level tests that expected all-or-error had to
learn the reconciler dance.

**Why it won.** Two of the costs of (a) were worse than documented. First,
the plan-apply status follow-up made every plan-applying eval FAIL after
mutating allocations under a status-lane split — and `job_update`'s retry
removes and re-places the job's whole allocation set, so the 5s retry loop
churned real workloads indefinitely. Second, an `Updated` submit whose
rollout creation was rejected was unrecoverable by retry (the retry returns
`Unchanged`, which never creates a rollout). Tolerating is sound precisely
and only for projections that are pure, idempotent functions of replicated
control state — the same argument JC-41 already accepted for
`regenerate_scheduling_evals`. (b) would forward ops whose executor derives
payload from its own possibly-lagging replica — a silent no-op the origin
reports as success; tolerate-and-reconcile converges from the data instead.
(c) buys atomicity nothing here needs (cross-op atomicity was never
promised, §A2) at the cost the whole design exists to avoid.

**User-facing surface.** A submit into a standing split returns success
(with "rollout deferred until raft lane leadership converges" on updated
jobs); `docs/multi-raft.md` § "What divergent leadership costs you" names
the brief lag windows.

## 4. The plan fence samples an epoch MIRROR kept coherent with the allocation cache

**The call.** The scheduler's epoch sampler reads a `scope_epochs` mirror
inside the read model — not the database — and every cache update maintains
one invariant: the published mirror epoch is never *ahead* of the published
allocations (epochs are read before allocations and published with them
under one lock; the changeset projection refreshes both from the
`allocation_epoch_partition` rows it carries). Plan apply additionally
re-validates node **availability** on the fresh row, and a plan rejection
self-heals the mirror by refreshing the rejected scopes.

**The alternatives.** (a) Keep sampling epochs from the DB and accept the
TODO's "microsecond" gap. (b) Make the epoch bump and the cache refresh
atomic by restructuring the write-path lock ordering (the TODO's own
suggestion). (c) Re-validate full resources at apply time instead of
fencing.

**The trade-off it costs.** A second copy of the epoch table in memory, a
maintenance invariant every future cache-touching path must respect (the
failure mode is a conservative spurious rejection — self-healed on the
rejection path — never phantom capacity), and slightly more DB reads per
cache patch.

**Why it won.** The adversarial pass showed (a)'s gap is not microseconds
under raft: an epoch is visible at commit but the cache is patched after the
whole changeset projection (table re-reads included), and two eval workers
genuinely plan concurrently — new epoch + stale cache passes the CAS against
phantom capacity, the exact oversubscription the fence exists to prevent.
(b) means holding the read-model write lock across the SQLite commit,
inverting the store's lock order for every allocation write. (c) rebuilds
resource ledgers inside the apply transaction — heavyweight, and still
doesn't fix the sampler. The availability re-check closes the sibling gap
the CAS structurally cannot see (cordon/lost writes live on other lanes and
bump no allocation epoch). Deliberately deferred, recorded in TODO.md:
apply-time re-validation of node resource *totals* (a re-registration that
shrinks capacity mid-plan still slips through) and volume-pin staleness.

**User-facing surface.** None visible when healthy; a plan racing a cordon
now rejects with "Node X is drained (cordoned, draining, or lost since the
plan snapshot)" and reschedules elsewhere.

## 5. Co-located leadership is documented as best effort, not an invariant

**The call.** The plan's JC-40 language ("one node leads all three in steady
state") is corrected in place, and the public doc states plainly that a
standing split is a supported state with named costs.

**The alternatives.** Keep the invariant framing and make the implementation
try harder — force a step-down over a side channel, or campaign indefinitely
until it wins.

**The trade-off it costs.** Operators lose a simple mental model. A cluster
can sit with lanes led by different managers, `/readyz` returning 503, with
degraded scheduling latency and no `state backup` until it converges — with
no bounded promise of when.

**Why it won.** The invariant was never true. openraft 0.9 has no
leader-transfer API, so the only lever is a campaign, and raft's election
safety can refuse a campaign forever. Documenting a guarantee the code cannot
keep is worse than documenting the real one: an operator who believes
convergence is guaranteed will not think to look at per-lane leaders when
scheduling degrades. Forcing a step-down means two nodes acting on one lane
at once, which under a stale anchor view is ping-pong — for zero gain, since
the campaign after the abdication is the campaign we already issue.
*(Amended by the standing-split resilience work: the costs shrank — submits
are accepted and worker liveness forwards, calls 1 and 3 — but the
best-effort framing stands.)*

**User-facing surface.** `docs/multi-raft.md` lists the costs explicitly.

## 6. Missing rollouts are derived BEFORE eval regeneration, so gating survives a lost submit tail

**The call.** A `reconcile_missing_rollouts` pass on the scheduler-lane
leader finds the acknowledged-update signature (latest config with a
predecessor, no rollout targeting it, no allocations on it, live allocations
on an older config) and creates the rollout exactly as `run_job` would have —
manual strategies wait, automatic ones get armed by regeneration's rollout
branch. It runs on the checker cadence and immediately before every
`regenerate_scheduling_evals` call.

**The alternatives.** (a) Leave regeneration's `job_submit` fallback as the
recovery (the shipped state). (b) Fix only the RPC retry path (make an
`Unchanged` resubmit create the missing rollout).

**The trade-off it costs.** The manager layer gains a reconciler that
mutates rollout state without an operator action, and its candidate query
must stay conservative (an operator-cancelled rollout must never be
recreated — any rollout row targeting the latest config, terminal included,
disqualifies the job).

**Why it won.** (a) silently converts a canary/staged/manual update into a
full-count placement — regeneration cannot know a rollout was intended, so
it bypasses exactly the gating the user configured; the ordering rule makes
that fallback unreachable for any job the reconciler was about to serve.
(b) heals only clients that retry; the split case (rollout half rejected)
and the crash case (between submit and rollout insert, no restart) both
converge with no client action under the reconciler. Ranked below the
forwarding and fence calls because it protects update *semantics* rather
than availability or capacity correctness.

**User-facing surface.** "Job update accepted; rollout deferred until raft
lane leadership converges" from `job run`, and the rollout appearing with
its configured strategy within a checker tick.

## 7. Bounded, escalating stand-downs instead of a fixed campaign rate

**The call.** Per-lane exponential backoff (2 → 16 election windows), a
verdict window so a campaign is never re-issued before the previous one is
decided, a stand-down after three consecutive losses lasting 60 election
windows, and that stand-down doubling to a ceiling of 480 windows for a lane
this node keeps failing to take. Any win resets everything. *(Amended
2026-07-28: an anchor flap no longer resets the loss ladder — only the
in-flight verdict is dropped — and the first-campaign spacing the constants
documented is actually enforced by `decide()`. Pre-amendment, a flapping
anchor fired an immediate campaign per re-acquisition and could never
accumulate the three losses that engage the stand-down.)*

**The alternatives.** A fixed backoff with a generous ceiling (what the code
had: 500 ms → 3 s, later scaled to 2.5x the lease); or a permanent stand-down
after N failures.

**The trade-off it costs.** A lane that becomes winnable again — its
incumbent degrades, this node catches up — may wait out an 8-minute cooldown
before convergence tries. Co-location is then restored slowly.

**Why it won.** A fixed rate is unbounded term inflation by another name: the
old ceiling still produced a campaign every few seconds forever, which is
exactly what a natural election cannot tolerate on slow hardware. A permanent
stand-down gives up a correctness-neutral optimization for good, and the
cluster's shape genuinely does change (nodes restart, logs catch up). Doubling
the stand-down makes the long-run cost of a lane that refuses convergence
converge to nothing while keeping recovery possible. Every duration is a
multiple of `raft.electionTimeoutMaxMs`, so the documented remedy for loaded,
WAN and CI clusters stretches the whole schedule with it rather than silently
breaking it.

**User-facing surface.** The stand-down is logged at `warn`, naming the lane
and the cooldown, and points at `docs/multi-raft.md`.

## 8. The catching-up hold is CAPPED, because log growth cannot tell catch-up from traffic

**The call.** The governor's "log grew since last poll → hold" gate expires
after 24 consecutive holds (~12s at the 500ms poll cadence); the campaign
then proceeds and the election is the judge.

**The alternatives.** (a) Keep the uncapped hold (the shipped state). (b)
Drop the hold entirely. (c) Compare against the leader's commit index (not
exposed to a non-leader in openraft 0.9).

**The trade-off it costs.** A node mid-way through a genuinely long catch-up
(a large snapshot install) can now campaign, lose, and cost the lane roughly
one election window — verified against the openraft source: the leader lease
makes voters reject the vote without adopting the term, but the campaigner's
raised term deposes the incumbent through the replication stream's
`HigherVote` reply. Backoff and stand-downs bound the aggregate to a small
duty cycle.

**Why it won.** (a) starves convergence on any lane with sustained write
traffic — a busy status lane grows its log every poll, so the hold fired
forever and the standing split became the steady state exactly under the
load lanes exist for; a bounded occasional deposition beats an unbounded
split. (b) throws away real value: bulk catch-up does finish within a few
polls, and holding through it avoids guaranteed-loss campaigns. This also
amends call 10's honest cost accounting: a lost campaign against a settled
lane is *not* free, and the governor's ladder is what bounds it.

**User-facing surface.** None directly; `docs/multi-raft.md`'s convergence
description stays accurate under load.

## 9. "Settled voter" means a UNIFORM membership for gating; the status headline counts joint-intersection voters

**The call.** `lane_membership_settled` requires a non-joint membership
containing the member. The join's post-promotion verification uses it.
*(Amended 2026-07-28: the aggregate `raft_status().voters` list now uses the
joint-INTERSECTION predicate — a member present in every config of every
lane — instead of the settled predicate.)*

**The alternatives.** Keep `voter_ids()` (openraft's union across the joint
configs) as the test everywhere; or use the settled predicate everywhere,
headline included.

**The trade-off it costs.** Two predicates to keep straight: gating
(join verification, readiness) stays strict-settled, display counts
strictest-config voters.

**Why it won.** The union predicate was the actual root cause of the CI
failure — a joint `[{a,b},{a,b,c}]` lane needs a majority of **each**
config, while `voter_ids()` already reports three voters, so the cluster
read as fault-tolerant and was not. But the settled predicate in the
HEADLINE overcorrected: during any join's joint window the whole membership
is joint, so every existing member failed the check and a healthy 3-voter
cluster mid-join reported `voters: []` with everyone a learner — an
operator-facing lie in the safe direction, still a lie. The intersection
counts exactly the members that are voters under the strictest rules in
force, keeps the incoming joiner out until uniform, and matches the
strictest-config `quorum`/`survives` arithmetic already shipped.

**User-facing surface.** A lane in a joint configuration reports the
strictest config's `quorum`/`survives` and a `membership change in flight`
advisory in `legion raft status`; the headline voter list no longer empties
mid-join.

## 10. The log-currency gate is "my log is still streaming in"

**The call.** Before campaigning, hold if this node's `last_log_index` for
that lane grew since the previous observation. Do not attempt a general "am I
as current as the quorum" check. *(See call 8 for the cap this gained.)*

**The alternatives.** Compare against the lane leader's index (not exposed to
a non-leader in openraft 0.9 — `RaftMetrics::replication` is `Some` only on
that group's leader); or add a side-channel RPC asking peers for their last
log id.

**The trade-off it costs.** The gate is one-sided: it catches a node that is
visibly catching up and misses a node that is quietly one uncommitted entry
behind another follower. On a lane under continuous write load the gate
holds convergence off — for at most the call-8 cap, after which the election
decides.

**Why it won.** It is the strongest signal actually available, it costs one
`u64` of state, and its false holds are bounded (call 8). The residual case
is handled by the only sound test raft offers: run the election and score
the result, which is what the backoff in call 7 does.

## 11. remove-member: active reachability probe, bounded changes, and no self-removal

**The call.** The feasibility guard's liveness input is a fresh dial of each
remaining voter's transport listener (2s timeout) rather than openraft's
replication view; each lane's membership change is bounded at 30s with a
recovery message; and the all-lane leader refuses to remove itself — the
documented decommission is `trigger-election` at another manager, then
`remove-member` from there.

**The alternatives.** (a) Keep the acked-once liveness (a member that acked
any entry under the current leadership counted as alive forever). (b) Allow
self-removal with anchor-lane-last ordering. (c) No timeout (openraft's
`change_membership` blocks until commit).

**The trade-off it costs.** An operator command now dials peers (seconds of
latency), a slow-but-alive member can be counted dead by the probe (the
guard then refuses a removal that would have been fine — retry when the
member responds), and decommissioning the leader takes two commands.

**Why it won.** The guard exists to prevent exactly one wedge: committing a
membership change whose resulting config cannot assemble a live quorum. On a
quiet lane the acked-once signal is arbitrarily stale — a member dead for
hours passed the guard, and the command then hung forever on a joint config
only resurrecting the dead member could commit. A probe answers the question
the guard is actually asking ("would this set ack a membership entry right
now"). Self-removal (b) half-applies *by construction* under the guard's own
rules — committing the self-excluding config on any lane deposes the node,
failing every later lane — and ordering tricks only shrink the window;
refusing with the two-step recipe is honest and always works.

**User-facing surface.** The refusal and timeout messages name the exact
recovery; `docs/raft-operations.md` documents the decommission recipe.

## 12. /readyz measures divergence with its own continuous-observation timer, and stays 503 on a standing split

**The call.** The readiness probe tracks when it first observed the current
not-fully-converged lane shape (any lane leaderless or leaders differing)
and grants the ~10s grace from that timestamp; genuine convergence resets
it. A standing split keeps returning 503, but the body now says the write
path degrades (accepted submits, forwarded liveness) rather than claiming
composites "cannot complete on any node".

**The alternatives.** (a) Keep the `election_epoch_ms` grace (any leader
change refreshes it). (b) Return 200 on a standing split now that the write
path works, moving the signal to metrics alone.

**The trade-off it costs.** Divergence duration is measured from first
*observation* (probe cadence granularity), and a 503 still pulls a
functioning manager out of an LB that keys on readiness.

**Why it won.** (a) failed in both directions: a manager restart left the
epoch at zero, denying any grace and failing rolling restarts straight into
503; and one flapping lane refreshed the grace forever, masking a standing
split of the other two as ready — the exact condition the check exists to
expose. (b) was rejected because a standing split, while no longer an
outage, is an abnormal leadership shape with real costs (latency, no
backup) that an operator should investigate; readiness is the one signal
every deployment already watches.

**User-facing surface.** The 503 body's plain-language description of what
still works and what is degraded, plus per-lane leader lines.

## 13. Status-lane orphan sweeps require two consecutive observations

**The call.** `prune_node_health_orphans` / `prune_allocation_health_orphans`
only delete a row seen as an orphan by two consecutive sweeps; the candidate
set is process-local, so a leadership change starts a fresh window.

**The alternatives.** Sweep immediately (the shipped state); or gate the
sweep on the sibling lanes' applied indexes being near their committed
indexes (not knowable from a non-leader in openraft 0.9).

**The trade-off it costs.** Orphans live one extra checker tick, and the
in-capture re-check no longer carries the whole burden.

**Why it won.** Orphanhood is judged against this node's LOCAL control/
scheduler replicas, and a freshly failed-over status leader can lag them by
minutes: its first sweep computed rows for just-registered nodes and
just-placed allocations as orphans and replicated the DELETEs cluster-wide —
erasing live health rows and, worse, `lost_at` fencing clocks that a dead
node can never re-heartbeat back. One tick of grace is free (orphans are
harmless by design — every reader joins away from them), and the fresh
window after failover is precisely the protection needed.

**User-facing surface.** None; the sweep logs are unchanged apart from
timing.

## 14. The bootstrap seed-snapshot purge re-arms on restart

**The call.** The gate for purging the log behind the bootstrap seed
snapshot is "a snapshot exists and the log was never purged" — not "no
snapshot exists" — so a crash between building the seed and purging the log
re-issues the purge on the next start instead of skipping the block forever.

**The alternatives.** A persisted "seed complete" marker; or making
build-and-purge atomic (openraft owns both operations, so it cannot be).

**The trade-off it costs.** None in practice: `purged == 0` with a snapshot
present cannot occur in healthy operation (every organic snapshot purges
behind itself, keeping its catch-up tail — which is also why the gate must
not be `purged < snapshot`, which would strip that tail on every restart).

**Why it won.** The crash window silently defeated the mechanism that makes
adopted pre-raft baselines reach joiners: with the log intact from index 1,
a blank joiner catches up by replay and misses every row that predates raft,
then wedges on the first changeset touching them. A gate derived from the
observable end state needs no new persistence and is idempotent by
construction.

**User-facing surface.** None; the upgrade path in `docs/upgrades.md`
behaves as documented even across a badly-timed crash.

## 15. Standalone events propose on a lane this node currently leads

**The call.** `emit_event` picks status → control → scheduler, first led
lane wins; `scheduler_event` is commutative (JC-28), so any lane may carry
the insert.

**The alternatives.** Pin to the status lane (the shipped state, which
silently dropped every standalone event on a node not leading it); or
forward events like call 1's ops.

**The trade-off it costs.** An event's journal id residue no longer names
the lane of the *subsystem* that emitted it, only the lane whose log carried
it — cosmetic, since ids were never dense and ordering is per lane anyway.

**Why it won.** Emission is best-effort by design, but "best effort" was
quietly "no effort" for every standalone event on two of three lane shapes.
The commutative class exists precisely so any lane can insert; using it
costs nothing. Forwarding would put journal writes on the network for a
surface that is explicitly allowed to drop under real failure.

**User-facing surface.** Event-journal completeness under divergent
leadership; `docs/event-stream.md` unchanged (the per-lane ordering caveat
already covered it).

## 16. Tests assert the real contract, and gained standing-split and forwarding cases

**The call.** `lane_leadership_co_locates_on_the_control_anchor` still demands
convergence, but only in the shape where the anchor demonstrably *can* win,
and adds a bound on the campaigns it took.
`killing_the_all_lane_leader_reconverges_onto_one_survivor` now requires every
lane to elect a surviving leader and *accepts either* co-location or a standing
split, asserting the composite contract in both.
`lanes_that_settle_on_different_leaders_stay_correct` holds a split
permanently and pins the NEW contract: the forwarded enqueue, the accepted
submit with its deferred half, the reconcilers completing it, and the healed
`Unchanged` retry. A new
`worker_heartbeats_forward_across_a_status_lane_split` pins call 1 end to end
over the real transport, in both directions (heartbeat in, mark-lost's
control half back).

**The alternatives.** Keep the strict co-location assertions and raise the
timeouts; or delete the assertions that no longer hold.

**The trade-off it costs.** The failover test no longer proves co-location
happens after a kill — only that it is *allowed* to, and that correctness
holds either way. The split test no longer pins the both-sides rejection
shape (it pins the acceptance shape instead).

**Why it won.** A test that asserts a guarantee the system does not make is a
flake generator, and raising its timeout only moves the flake. What it gave up
is replaced by strictly more: a lane always elects; convergence campaigns stay
bounded (asserted through `Store::raft_lane_campaigns`); the standing-split
correctness case; and the worker-liveness path that previously had no test at
all. The governor's own properties are unit tests against a fake clock, where
"never campaigns into an election over ten minutes" is provable in
microseconds.
