# Judgement Calls — Durability and Ops Execution

Companion to
[`plans/durability-and-ops-execution-plan.md`](../plans/durability-and-ops-execution-plan.md).
Started 2026-07-28 with the JC-40 lane-leadership correction; earlier phases of
that plan recorded their calls inline in the plan's `JC-*` sections, which stay
where they are.

Entries are ranked by project impact, highest first.

## 1. Convergence never campaigns for a leaderless lane (JC-40 correction)

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

## 2. Co-located leadership is documented as best effort, not an invariant

**The call.** The plan's JC-40 language ("one node leads all three in steady
state") is corrected in place, and the public doc states plainly that a
standing split is a supported state with named costs.

**The alternatives.** Keep the invariant framing and make the implementation
try harder — force a step-down over a side channel, or campaign indefinitely
until it wins.

**The trade-off it costs.** Operators lose a simple mental model. A cluster
can sit with lanes led by different managers, `/readyz` returning 503, and
cross-lane composites (job submit, registration, destroy, joins) unavailable
until it converges — with no bounded promise of when.

**Why it won.** The invariant was never true. openraft 0.9 has no
leader-transfer API, so the only lever is a campaign, and raft's election
safety can refuse a campaign forever. Documenting a guarantee the code cannot
keep is worse than documenting the real one: an operator who believes
convergence is guaranteed will not think to look at per-lane leaders when
submits start failing. Forcing a step-down means two nodes acting on one lane
at once, which under a stale anchor view is ping-pong — for zero gain, since
the campaign after the abdication is the campaign we already issue.

**User-facing surface.** `docs/multi-raft.md` lists the costs explicitly,
including that a submit issued during a scheduler-lane split fails on the
control leader *and* on the scheduler leader.

## 3. Bounded, escalating stand-downs instead of a fixed campaign rate

**The call.** Per-lane exponential backoff (2 → 16 election windows), a
verdict window so a campaign is never re-issued before the previous one is
decided, a stand-down after three consecutive losses lasting 60 election
windows, and that stand-down doubling to a ceiling of 480 windows for a lane
this node keeps failing to take. Any win resets everything.

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

## 4. "Settled voter" means a UNIFORM membership, everywhere it is asked

**The call.** `lane_membership_settled` requires a non-joint membership
containing the member. The join's post-promotion verification uses it, and
`raft_status().voters` now means "settled voter on **every** lane" rather
than "voter on the anchor lane".

**The alternatives.** Keep `voter_ids()` (openraft's union across the joint
configs) as the test, and treat the joint window as too short to matter.

**The trade-off it costs.** `legion raft status` reports a *smaller* voter
list than openraft does during a membership change, and a join takes slightly
longer to report success. Anyone reading `voters` as "nodes openraft knows
about" gets a different answer than before.

**Why it won.** This was the actual root cause of the CI failure, and it is a
production hazard, not a test artifact. openraft applies a membership change
as joint-then-uniform; a lane stuck in `[{a,b},{a,b,c}]` needs a majority of
**each** config, i.e. both `a` and `b`, while `voter_ids()` already reports
three voters. The cluster reads as fault-tolerant and is not: killing `a`
costs that lane its write path permanently. Every readiness signal that
matters — the join's own verification, the status headline, and the lanes
battery's bring-up gate — was passing on the weaker check.

**User-facing surface.** A lane in a joint configuration reports the
strictest config's `quorum`/`survives` (so a joint 2→3 growth says it
survives zero failures, which is true) and carries a plain-language
`membership change in flight` advisory, alongside the existing even-parity
wording, in `legion raft status`.

## 5. The log-currency gate is "my log is still streaming in"

**The call.** Before campaigning, hold if this node's `last_log_index` for
that lane grew since the previous observation. Do not attempt a general "am I
as current as the quorum" check.

**The alternatives.** Compare against the lane leader's index (not exposed to
a non-leader in openraft 0.9 — `RaftMetrics::replication` is `Some` only on
that group's leader); or add a side-channel RPC asking peers for their last
log id.

**The trade-off it costs.** The gate is one-sided: it catches a node that is
visibly catching up and misses a node that is quietly one uncommitted entry
behind another follower. On a lane under continuous write load the gate also
holds convergence off for as long as the writes last.

**Why it won.** It is the strongest signal actually available, it costs one
`u64` of state, and it never produces a false *hold* that matters — a lane
whose log is streaming in is one this node is genuinely behind on. The
residual case is handled by the only sound test raft offers: run the election
and score the result, which is what the backoff in call 3 does. Holding off
under write load is correct behavior for an optimization whose subject (the
lane) is serving fine under its incumbent.

## 6. Tests assert the real contract, and gained a permanent-split case

**The call.** `lane_leadership_co_locates_on_the_control_anchor` still demands
convergence, but only in the shape where the anchor demonstrably *can* win,
and adds a bound on the campaigns it took.
`killing_the_all_lane_leader_reconverges_onto_one_survivor` now requires every
lane to elect a surviving leader and *accepts either* co-location or a standing
split, asserting the composite contract in both. The aggregate-summary string
assertion in `killing_a_single_lane_leader_disturbs_only_that_lane` is replaced
by per-lane leader assertions plus "the headline must not name a single
leader". A new test,
`lanes_that_settle_on_different_leaders_stay_correct`, holds a split
permanently and pins the typed rejections from both sides, the intact halves,
the eval safety net, and the healed retry.

**The alternatives.** Keep the strict co-location assertions and raise the
timeouts; or delete the assertions that no longer hold.

**The trade-off it costs.** The failover test no longer proves co-location
happens after a kill — only that it is *allowed* to, and that correctness
holds either way.

**Why it won.** A test that asserts a guarantee the system does not make is a
flake generator, and raising its timeout only moves the flake. What it gave up
is replaced by strictly more: two properties that were previously untested (a
lane always elects; convergence campaigns stay bounded — asserted directly
through the new `Store::raft_lane_campaigns` counter) plus a permanent-split
correctness case that did not exist. The governor's own properties are unit
tests against a fake clock, where "never campaigns into an election over ten
minutes" is provable in microseconds.
