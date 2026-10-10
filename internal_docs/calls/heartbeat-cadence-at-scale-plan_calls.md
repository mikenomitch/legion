# Judgement Calls: Heartbeat Cadence at Scale

Decisions made while writing
[`../plans/heartbeat-cadence-at-scale-plan.md`](../plans/heartbeat-cadence-at-scale-plan.md)
(status SHIPPED). Entries marked *(Implementation)* were added while the plan
was executed, and entries marked *(Phase 5)* when adaptive cadence became the
default (2026-10-09); the rest are the plan's own calls.

**Ordered most-impactful first. The entry number is its rank.**

---

## 1. Adaptive cadence is a pure function of fleet size, never of measured saturation

**Call.** When adaptive cadence is on, the served interval is
`clamp(ceil(H × active_nodes / targetRatePerSecond), min, max)`, snapped to
a fixed ladder. Fleet size is the *only* input. The manager never reads its
own latency, freshness ratio, queue depth, or fsync time to decide how fast
workers should check in.

```jsonnet
heartbeat: {
  targetRatePerSecond: 80,   // what this control plane sustains — YOU measure it
  minInterval: "2s",
  maxInterval: "120s",
}
```

The operator can compute the manager's answer on paper, before running it.

**Alternatives.** (a) The obvious one, and what "adaptive" usually means:
close the loop on the thing you actually care about — watch
`legion_heartbeat_freshness_ratio`, back off when it sags, speed up when it
recovers. It needs no operator-supplied `R` at all, which is a real
usability win, and it tracks load sources other than node count (a
scheduling storm, a noisy neighbor on the EBS volume). (b) Feed both:
size as the baseline, saturation as a correction term.

**Trade-off.** The operator has to supply `targetRatePerSecond`, measured
on their own hardware, and it will be stale after they change instance
types or storage. The controller is also blind to non-heartbeat load: a
cluster whose managers are saturated by a scheduling storm at 100 nodes
will not back off, because 100 nodes is under the target. The freshness
warning (entry 7) is what covers that gap, and it covers it by telling a
human rather than by acting.

What would make us revisit: evidence from the AWS campaign that `R` swings
by more than ~2× under realistic mixed load, which would mean a
size-derived interval is not a good enough proxy in practice.

**Why (performance, and honesty about it).** Closing the loop on
saturation makes this a genuine control system with a plant delay: back off
→ load drops → freshness recovers → speed up → load returns → back off.
Its oscillation period would be set by the liveness tick rate, and — the
part that makes it unacceptable — **the lost-node window breathes with the
interval**, so an oscillating controller means an oscillating definition of
"dead". That is a correctness surface driven by a feedback loop, which is
not a thing to ship.

It is also the fairness argument. A system that speeds up when it is idle
and slows down when it is measured is exactly the "silently slows its own
heartbeats to look better on throughput" failure the brief warns about. A
pure function of node count cannot do that: at a given fleet size it always
serves the same interval, benchmark or production. Nomad reached the same
conclusion — `RateScaledInterval(rate, min, n)` takes `n`, not a latency —
and Consul's anti-entropy `scaleFactor(nodes)` is the same shape. Two
HashiCorp products at scale, neither of which closed the loop.


---

## 2. Adaptive cadence defaults ON, at a fixed `targetRatePerSecond` of 2,500

**Call.** *(Phase 5, 2026-10-09.)* A manager config that sets nothing now
runs adaptive cadence at 2,500 beats/s with the default headroom of 2, so
`interval = ceil(N / 1250)` seconds, snapped to the ladder:

| Fleet | Served | Lost window |
| ---: | ---: | ---: |
| 1,000 | 2s | 10s |
| 2,500 | 2s | 10s |
| 5,000 | 5s | 25s |
| 10,000 | 8s | 40s |
| 20,000 | 21s | 105s |
| 50,000 | 55s | 275s |

This supersedes the "adapt is opt-in" half of entry 7.

**Alternatives.** (a) Keep it opt-in, as entry 7 argued, and tell large
operators to set it. (b) Default on with a rate auto-derived from measured
`R`, which is what the plan's open question 1 leaned towards. (c) Default on
with a Nomad-like low rate (Nomad's `max_heartbeats_per_second` is 50, with
a 10s minimum TTL). (d) A higher default such as 5,000 or 10,000/s, closer to
what the handler can actually answer.

**Trade-off.** Every cluster past 2,500 nodes now detects dead machines more
slowly than before without having asked for it: 105s instead of 10s at 20k
nodes. The flip also happened before the plan's own criterion was met. No
campaign run has shown a cluster past its ceiling surviving with adaptive on
(entry 21 still stands). The evidence is the cost side only: at 20k nodes the
fixed 2s cadence put ~10,000 RPCs/s on a 4-vCPU leader, more than a core,
and scheduling slowed with it. And 2,500 is a judgement, not a measurement:
a smaller control plane than the AWS rig's may need less, which only the
freshness warning will tell its operator.

**Why (performance, then simplicity).** Opt-in protected nobody who needed
protecting. The operators who hit the wall are the ones who never read the
page, which entry 7 itself conceded. The AWS rig hit it too, and its
large cells were measuring heartbeat load rather than the scheduler. (b)
needs a measurement loop that does not exist and would make the default
depend on the box it ran on, which reintroduces the "slows down when
measured" worry entry 1 rejected. (c) would move a 1,000-node fleet off 2s
and widen its window to 40s, changing behavior for every medium cluster to
fix a problem only large ones have. (d) leaves the leader spending most of a
core on heartbeats at 20k nodes, which is the cost being removed.

2,500 is the largest round number that keeps every fleet up to 2,500 nodes
on the 2s floor and the historical 10s window, so small and medium clusters
see no change at all. Past that, steady load is capped at `rate / headroom`,
1,250 beats/s, about an eighth of the load that cost a core. The pre-rework
`R ~= 80` in the docs' table is not the right yardstick: it measured raft
commits, which a steady-state heartbeat no longer makes.

Benchmark fairness survives the flip because the controller is still a pure
function of fleet size (entry 1). A benchmark and a production cluster of the
same size are served the same interval. Nomad adapts by default as well, so
default-against-default is the honest comparison, and every run records its
served interval and lost window.


---

## 3. The served interval is a floor over the worker-type cadence, not an override

**Call.** *(Phase 5.)* A worker beats at `max(heartbeatSeconds, served)`.
A reply with no interval means "use your own", so a worker returns to its
worker-type cadence when the manager stops serving one.

```text
worker type heartbeatSeconds = 60, fleet of 800 nodes (served 2s)
  before: beats every 2s (override)
  now:    beats every 60s, window 300s, untouched
```

**Alternatives.** (a) Keep the override semantics phase 4 shipped: the
served interval replaces `heartbeatSeconds` outright. (b) Serve nothing while
the ladder sits on the floor, so small fleets see no wire change at all.

**Trade-off.** A worker type configured faster than `minInterval` (1s) is
slowed to the floor at any fleet size, where with adaptive off it kept 1s.
Its window widens to match, but detection for that pool goes from 5s to 10s
unless the operator lowers `minInterval`. The worker also has to remember
its configured value, one more piece of state in the heartbeat loop.

**Why (simplicity).** With adaptive cadence off by default, override was
harmless: only an operator who opted in met it. On by default, override
silently sped every slow batch pool up to 2s on every small cluster, which is
30x the load that pool was configured for, and nothing told the operator
their `heartbeatSeconds` had stopped meaning anything. A floor keeps
`heartbeatSeconds` meaningful at every fleet size. (b) leaves workers with
nothing to fall back to on a step down to the floor: an older worker keeps
the last interval it heard, and a 3s beater sits right at the 1.5x narrowing
threshold for a 2s rung, with a 10s window.


---

## 4. The lost-node window is `missedBeatsBeforeLost × that node's served interval`, and nothing else

**Call.** `DEF_LOST_NODE_AT_SEC = 10` stops being the answer and becomes a
fallback. The window is derived, per node, from the cadence that node is
actually running at, stored in a new nullable
`node_health.lost_after_seconds`. One new knob, defaulting to a value that
reproduces today exactly:

```jsonnet
heartbeat: {
  // 5 x the 2s default = the historical 10s. Unchanged for anyone
  // who never touches cadence.
  missedBeatsBeforeLost: 5,
}
```

The whole user-facing model is two sentences: *the manager tells workers
how often to check in; a node is lost after five missed check-ins.*

**Alternatives.** (a) Keep a separate absolute `lostNodeAfter` duration
alongside `heartbeatSeconds`, validated against each other at config load —
what an operator coming from Kubernetes would expect, since
`--node-monitor-grace-period` and `--node-status-update-frequency` are
independently settable there. (b) Nomad's shape: serve a TTL and add a
separate server-side `heartbeat_grace` the client is never told about, so
detection margin is tunable without changing what workers do.

**Trade-off.** Detection margin is no longer independently tunable. An
operator who wants a 60s cadence with a 90s window cannot have it — they
get 300s. That is a real loss of expressiveness, and (b) in particular is
the more sophisticated design: Nomad's hidden grace absorbs network and
processing delay without inflating the interval the client sleeps for.

**Why (simplicity).** Two independent duration knobs whose relationship
matters, with nothing enforcing it, is precisely the Kubernetes failure
mode: the `--node-monitor-grace-period` flag help says it "must be N times
more than kubelet's nodeStatusUpdateFrequency", and no code checks it. The
result was v1.32 raising the default from 40s to 50s after an apiserver
HTTP/2 timeout summing to 45s marked *every node* NotReady. That bug is a
two-independent-numbers bug.

Making the window a multiple removes the class. It is also what makes
adaptive cadence safe to build at all — if the window did not follow the
interval by construction, every back-off would need its own correctness
argument. And it is what makes the sizing table in the plan legal advice
rather than a trap: today, every row above `heartbeatSeconds: 3` would
cause false loss.

The per-node part is not optional even without adaptive cadence, because
`heartbeatSeconds` is already per **worker type**. A cluster with a 2s
latency pool and a 60s batch pool cannot have one global cutoff. This
closes the predecessor plan's first named deferral.


---

## 5. Non-leader managers relay the leader's served interval from the digest reply

**Call.** *(Phase 5.)* `NodeLivenessDigestReply` gains
`optional uint32 served_interval_seconds = 4`. The status-lane leader fills
it, and a manager that does not lead the lane serves that value on its own
heartbeat replies.

**Alternatives.** (a) Leave it leader-only, as phase 4 shipped. (b) Have
every manager run the controller itself from its own view of fleet size.
(c) Replicate the served interval through the status lane.

**Trade-off.** One more wire field, and a follower's interval lags the
leader's by up to one digest interval (1s). A follower that has no workers
sends no digest and learns nothing, which is harmless because it has nobody
to tell. Each manager's own `heartbeat` block is ignored for serving on a
follower: the leader's settings win.

**Why (performance).** Phase 4 set the served interval only on the status
leader, so under `lanes` a worker attached to any other manager never
backed off. A fleet spread over three managers kept two thirds of its beats
at the floor. The AWS rig attaches every sim host to one manager that need
not lead the status lane, so it could adapt not at all. (b) gives three
controllers three slightly different `N` and three ladders, and the leader
alone owns the lost windows those ladders are meant to match. (c) puts a
write on the raft path for a value that is recomputed every tick. The digest
reply already travels from the leader to every follower that has workers,
once a second.


---

## 6. A node's cadence is REPORTED by the worker at registration, in `config_attrs`

**Call.** *(Implementation.)* The worker puts its resolved interval into the
registration payload it already sends, and the manager derives that node's
lost window from it:

```rust
// src/worker/daemon.rs, building the registration config_attrs
config_attrs.insert("heartbeatSeconds".into(), json!(heartbeat_interval.as_secs()));
```

`node_health.lost_after_seconds` is seeded from
`missedBeatsBeforeLost x reported interval` at registration, and the simulated
fleet reports the same value from the same shared helper.

**Alternatives.** (a) The manager looks the cadence up from the worker type —
the obvious answer, since the manager already *serves* `heartbeatSeconds` at
pre-registration. (b) A new typed field on `WorkerRegisterRequest`.

**Trade-off.** The cadence rides in a free-form JSON bag rather than a typed
field, so nothing enforces its shape, and a worker that reports a cadence it
does not honor gets a window that does not match its behavior. Bounded: the
manager clamps what it reads, and the window is only ever *derived* from it —
a wrong value costs detection latency, never a false loss.

**Why (the constraint, then simplicity).** (a) is impossible as written: the
`node` table records pool and attributes but **not** the worker type, so there
is no link from a node back to its cadence, and adding one is a control-lane
schema change. (b) is a worker-plane proto change, which the companion
liveness plan names as a non-goal precisely so a worker built before and after
this work stays indistinguishable to a manager. `config_attrs` is already the
channel for "what this worker's config resolved to" — `resources`,
`maxAllocations`, `workerAddress` all travel there — so this is the existing
shape rather than a new one.

---

## 7. All three options ship, as layers, in the order measure → warn → adapt — and adapt is opt-in

> **Partly superseded (2026-10-09).** The layering stands. The opt-in half
> does not: adaptive cadence is now on by default. See entry 2.

**Call.** Documentation, warning, and adaptive cadence are not competing
answers to be picked between; they are phases 1, 2, and 4 of one plan, in
that order. Adaptive cadence ships **off** (`targetRatePerSecond` unset),
and every benchmark result carries the served interval and freshness ratio
next to the throughput number.

**Alternatives.** (a) Adaptive only. It is the option that actually fixes
the correctness bug, and the other two are advice — why ship advice when
you can ship a fix? (b) Docs + warning only, and treat cadence at scale as
an operator's sizing problem, which is defensible: Kubernetes ships 5,000
nodes of static config and no adaptation whatsoever. (c) Adaptive on by
default, since an operator who has not heard of the problem is exactly the
one who needs it.

**Trade-off.** The ordering delays the actual fix by three phases, and
opt-in means the operators most likely to hit the wall — the ones who never
read the page — get no protection. Both are real costs, accepted.

**Why (simplicity, then performance).** Two reasons the order is not
negotiable.

The first: an adaptive controller you cannot observe is worse than a fixed
interval you can. Phase 4's controller is validated by
`legion_heartbeat_freshness_ratio`, which is phase 1. Its remaining failure
mode — target rate set too high, or non-heartbeat load — surfaces as the
phase 2 warning. Shipping (c) first means shipping a mechanism that
silently changes cluster timing with no way to ask what it did, and the
first support question is unanswerable.

The second is fairness, and it is why opt-in wins over on-by-default for
now. The AWS campaign exists to publish Legion-vs-Nomad-vs-K8s numbers. A
Legion that adapts by default while being benchmarked is open to exactly
the accusation the brief raises. Off-by-default plus mandatory cadence
reporting makes the number defensible. Worth stating the other half
honestly, though: **Nomad adapts too**, by the same `n/rate` formula, so an
unpinned comparison already favours Nomad against a non-adaptive Legion.
The fix for that is pinning both and reporting both, not refusing to build
the feature.

The default flips once the campaign has data — the criterion is written
into the plan's open question 1 rather than left to taste. This matches how
`score_sample` and `preemption.mode` shipped: measurable, opt-in, promoted
on evidence.


---

## 8. Windows widen immediately; they narrow only after the manager sees a beat at the faster cadence

> **Refined in phase 5.** Widening now runs every tick and never lowers a
> window, and narrowing stops at the cadence a node was seen beating at. See
> entry 9.

**Call.** When the served interval goes **up**, a node's
`lost_after_seconds` is widened at the moment the manager decides, *before*
the worker is told. When it goes **down**, the window stays wide until the
manager observes that node beating at the new, faster rate.

**Alternatives.** Symmetric update — recompute every node's window from the
current served interval on every tick. It is one line instead of a state
rule, and the asymmetry looks like superstition until you write out the
race.

**Trade-off.** After a speed-up, the cluster carries stale-wide windows for
up to one old interval, so genuine failures in that gap are detected on the
old, slower schedule. Bounded, transient, and strictly on the safe side.

**Why (correctness).** The manager decides; the worker finds out one beat
later; the network may lose the message entirely; an older worker build
ignores the field forever. Every one of those is a window where the
manager's belief about a node's cadence is ahead of the node's. Symmetric
update punishes a worker for obeying the last instruction it actually
received — which is the exact bug this whole plan exists to prevent,
re-introduced by the fix. The asymmetry makes the safe direction the
default in every race.

This is Consul's session-TTL contract, per node: "Consul will not expire
the session before the TTL is reached, but it is allowed to delay the
expiration past the TTL." Lower bound guaranteed, upper bound advisory.


---

## 9. Widening runs every tick, and narrowing never goes below the cadence a node was seen at

**Call.** *(Phase 5.)* The status checker raises any node whose window is
below `missedBeatsBeforeLost x served` on every tick, not only on a step up,
and never lowers one. On a step down, a confirmed node's window becomes
`missedBeatsBeforeLost x max(next, observed gap)`.

**Alternatives.** Phase 4's rule: rewrite every node's window to the new
value on a step up, and to `5 x next` for every node seen beating within
1.5x of it on a step down.

**Trade-off.** One O(N) comparison per tick over a snapshot the tick already
holds, and a store write whenever a node registers into a backed-off fleet.
Windows can now sit wider than `5 x served` indefinitely (a slow pool, or a
node not seen at the faster cadence when the step down happened), which only
ever delays detection.

**Why (correctness).** Phase 4 had two holes that default-on would have hit
routinely. A node that registered after a step up got its 10s window from
its worker-type cadence, was then served 21s, and was marked lost before its
second beat. And the step-up rewrite lowered a 60s pool's 300s window to
`5 x served`, so an older worker that ignored the served interval was marked
lost. Under the floor semantics (entry 3) a 3s pool confirmed against a 2s
rung would also have narrowed to 10s.


---

## 10. The served interval is a quantized ladder with hysteresis, not a continuous `n/rate`

**Call.** `target` is computed continuously, then snapped **up** to the next
rung of `[2, 3, 5, 8, 13, 21, 34, 55, 89, 120]s` (~1.6× per rung). Step up
immediately; step down only after `target` has stayed below the next-lower
rung for 5 minutes.

**Alternatives.** Nomad's continuous form — serve `max(min, n/rate)`
directly, recomputed per heartbeat from the live timer count. It is simpler
code, it has no rungs to explain, and it is proven at scale.

**Trade-off.** Granularity: a fleet must grow ~60% to move a rung, so
between rungs the cluster runs at up to 1.6× the interval it strictly
needs, which is 1.6× the detection latency it strictly needs. Accepted.

**Why (simplicity, and a schema consequence).** Continuous means the served
interval changes on nearly every tick, which — given entry 4 — means every
node's `lost_after_seconds` changes on nearly every tick. Nomad can afford
that because its heartbeat timers are leader-local soft state in a map;
Legion's per-node window is a **replicated store column**, so continuous
recomputation is a fleet-wide write storm on the status lane, caused by the
feature meant to reduce status-lane load. The ladder turns that into a rare
event.

The operator-facing half matters as much: ten values an operator can
memorize, and "what will the manager do at 700 nodes?" answered by reading
a row. Consul reached for the same device in `agent/ae/ae.go` — doubling
per octave above 128 nodes — for the same reason, on the same kind of
central write path. Legion uses finer rungs because a 2× jump in the lost
window is a bigger deal than a 2× jump in a state-sync interval.


---

## 11. The opt-out is `heartbeat.adaptive = false`, not an unset or zero rate

**Call.** *(Phase 5.)* A boolean switch, default `true`.
`targetRatePerSecond` is ignored while it is false, and still validated, and
`targetRatePerSecond = 0` is rejected with an error naming the switch:

```text
heartbeat.targetRatePerSecond must be a positive number (got 0); set
heartbeat.adaptive = false to turn adaptive cadence off
```

**Alternatives.** (a) Treat `targetRatePerSecond = 0` (or a negative value)
as off, with no new field. (b) Reject a config that sets both
`adaptive = false` and a rate, as a contradiction.

**Trade-off.** A second field in the block where one could have carried both
meanings. Leaving a rate in place under `adaptive = false` is silently
ignored rather than flagged.

**Why (simplicity).** An unset rate used to mean off, and it has to mean
"the default" now, or every existing config would keep the old behavior and
the flip would reach nobody. A magic zero is the kind of knob an operator
reads as "zero heartbeats". `adaptive = false` reads as what it does, and it
shows up in `GetHeartbeatCadence`'s `adaptive_enabled` under the same name.
Allowing a rate alongside it keeps the switch a one-line edit in both
directions. Validating the rate either way means turning adaptive back on
can never surface a latent bad value at the next restart.


---

## 12. One-shot full-width re-phase on interval change, instead of Nomad's permanent 2× stagger

**Call.** On the beat where a worker's served interval changes, it sleeps
`U[0, new_interval)` once, then returns to the existing `new_interval ±
10%` steady state. Nomad's `ttl += RandomStagger(ttl)` — uniform in
`[base, 2×base)` on *every* beat — is not adopted.

**Alternatives.** Adopt Nomad's stagger wholesale. It is battle-tested, it
is one line, and it decorrelates continuously rather than only at
transitions.

**Trade-off.** A one-shot re-phase leaves the fleet on a tighter
distribution (±10%) in steady state, so a correlated external event — a
network partition healing, a rack powering on — can still produce a
partially phase-locked fleet that the ±10% walk decorrelates only slowly.
Nomad's version would grind that away continuously.

**Why (performance, and predictability).** Nomad's permanent stagger costs
a third of the target rate forever: uniform on `[base, 2×base)` has mean
`1.5×base`, so the achieved aggregate is `rate/1.5` — at the documented
`max_heartbeats_per_second: 50`, about 33/s. Worse for us, it makes the
operator's `sustainable_nodes ≈ R × I` arithmetic wrong by a variable
factor, and it doubles worst-case detection time on top of whatever the
window already allows. The sizing table in the plan is the main
operator-facing artifact here; a formula that is off by an unstated 1.5×
undermines it.

The actual herd risk is a *transition* risk — every worker learning about a
new interval within one old interval and re-phasing onto the same boundary.
A one-shot `U[0, new)` spreads the fleet uniformly across the new period in
exactly one beat, which is strictly better at the moment it matters, and
costs nothing the rest of the time. The ±10% steady-state jitter is also
already shipped and already the basis for the group-commit batching numbers
in the predecessor plan; changing it would invalidate those.


---

## 13. Slower failure detection is the accepted price, bounded by `maxInterval` and a warning — not an unbounded back-off

**Call.** Backing off cadence makes real failures take proportionally
longer to notice: at 1000 nodes with `R ≈ 80`, up to ~125s instead of 10s.
That is accepted and stated on the public page. The back-off stops at
`maxInterval` (default 120s), and at the bound the manager serves the
ceiling and **warns** rather than continuing to slow down.

**Alternatives.** (a) Unbounded back-off — always prefer "no false loss"
over "timely true loss", and let the interval go wherever the fleet
requires. (b) Cap the *window* instead of the interval, accepting some
false loss above a threshold as the lesser evil.

**Trade-off.** Between the bound and the cluster's real ceiling there is a
band where the manager is knowingly serving an interval it cannot sustain,
and false loss becomes possible again. We chose to make that band loud
rather than to make it impossible.

**Why (simplicity, honestly stated).** Unbounded back-off turns a capacity
problem into a silently-degrading-service problem: a cluster could drift to
a ten-minute detection window and still report itself healthy, and the
operator would discover it as "why did a dead node run for ten minutes".
The bound is where Legion says out loud that the answer is more control
plane, not more patience. It is also where option (b) — the warning — earns
its place in the final design rather than being made redundant by option
(c): the warning at the ceiling is the same warning, saying the same thing,
for a cluster that has exhausted adaptation.

Nomad accepts the same cost without the bound (100–200s TTL at 5k clients).
We keep the cost and add the stopping point.


---

## 14. The served interval is not checked against the stateful self-fence

**Call.** *(Phase 5, accepted limitation.)* The startup check that rejects a
worker-type `missedBeatsBeforeLost x heartbeatSeconds` at or past
`selfTerminateAfter` (entry 18) is not applied to the served interval. At the
default rate, windows pass the 90s fence from the 21s rung (about 16,250
nodes), and the interval itself nears it at the 89s rung (about 68,750
nodes). It is documented on the public page and in
`docs/stateful-workloads.md`, and tracked in `internal_docs/TODO.md`.

**Alternatives.** (a) Lower the default `maxInterval` to 55s, so a healthy
worker always has about 30 seconds of retries before it fences. (b) Clamp
the served interval at runtime below the smallest `selfTerminateAfter` among
running stateful allocations. (c) Refuse to start with a `maxInterval` whose
window reaches the default fence.

**Trade-off.** A cluster past ~68,750 nodes running stateful jobs at the
default fence can have healthy workers fence between beats. Before the flip
that needed an explicit opt-in. Now it needs only a very large fleet.

**Why (simplicity, scoped).** The window-based check is conservative: a
healthy worker resets its fence clock on every successful beat, and fails
over to fast retries, so at 21s it fences only after about 70 seconds of
failed beats. The real hazard starts at the 89s rung, far past any fleet
Legion has run. (c) would reject the default config itself. (a) changes a
second default in the same release. (b) is the right fix, but it couples the
controller to the allocation set, which breaks entry 1's pure function of
`N`. Writing it down, and naming the two settings that avoid it, is the
honest interim.


---

## 15. A cluster-wide 50% disruption circuit breaker, applied at detection rather than at eviction

**Call.** If more than 50% of active nodes would be marked lost in a single
liveness tick, mark **none** of them and log at `error`. Minimum fleet size
20, below which the rule does not apply.

**Alternatives.** (a) Kubernetes' actual design: mark the nodes NotReady
regardless, and rate-limit the *consequence* — `--node-eviction-rate` 0.1/s,
dropping to 0.01/s or to zero when a zone crosses
`--unhealthy-zone-threshold` (0.55), and stopping entirely when every zone
is fully disrupted. Truth in the API, safety in the actuator. (b) Nothing —
rely on `failoverGrace` and adaptive cadence to prevent the situation
arising.

**Trade-off.** A real correlated outage — one rack, one AZ, one power
event taking 60% of the fleet — is now *not reflected in node status* until
the tick where fewer than half are newly silent. The cluster's state will be
visibly wrong during a real disaster, which is a genuine cost and the
opposite of what (a) gives you.

**Why (simplicity, and the shape of Legion's plumbing).** Legion has no
zone concept, so the per-zone graduation that makes (a) work has no analog
here; a cluster-wide eviction rate limiter without zones would just delay a
cascade rather than stop it. And Legion's mark-lost path is *directly*
coupled to rescheduling — `mark_nodes_as_lost` is followed by
`recheck_lost_nodes`, and every lost allocation becomes an eval. Marking
truthfully and rate-limiting downstream would require a second throttle in
the eval path, i.e. two mechanisms where one will do.

The reasoning that decides it: a control plane does not genuinely lose half
its fleet in two seconds. When it looks like it did, the overwhelmingly
likelier explanation is that the control plane is what broke — which is
literally the v1.32 Kubernetes incident, where an apiserver HTTP/2 timeout
marked every node NotReady. Believing the fleet over the control plane in
that moment is the wrong prior. Stateful workloads keep their independent
protection either way: `selfTerminateAfter` fences from the worker side and
does not depend on the manager's verdict.

The `error` log is load-bearing — the whole point is that a human finds out
immediately, not that the situation is hidden.


---

## 16. Gossip liveness and a Kubernetes-style lease/state split stay non-goals

**Call.** The plan does not move liveness out of raft. No SWIM/Serf, no
separate lightweight lease record. Both remain architecture item D8,
awaiting the AWS campaign.

**Alternatives.** (a) Gossip. It is the architecturally correct answer:
Consul's per-node message rate is flat regardless of fleet size and touches
no consensus write path at all, with detection time growing only as
`log(N)`. Every number in this plan's problem statement disappears. (b) The
K8s split: keep the beat at 2s but make the write cheap, which is exactly
what KEP-589 did to take etcd write throughput from 150MB/min to 30MB/min
at 5k nodes.

**Trade-off.** We are shipping a mitigation for a problem whose root cause
we have decided not to fix yet, and phase 4's controller may be dead code
in a world where liveness is gossiped. The plan spends real effort — a
schema column, a proto field, a controller — on a shape that could be
superseded.

**Why (extensibility, and sequencing).** Both alternatives change *where
liveness lives*, which is the largest architectural decision open in the
scheduler review, and D8 was deferred specifically pending AWS data.
Choosing it now on the strength of one benchmark run would be the deferral
overturned by the first inconvenient number.

The K8s split is also the weaker analogy than it first appears: KEP-589's
problem was **bytes per write** (20KB Node objects), and Legion's
per-heartbeat cost is already one *batched* raft commit — group commit
shipped in the predecessor plan and coalesces up to 128 heartbeats into a
single consensus round. What remains is per-commit fsync, which a smaller
record does not fix. The K8s lever mostly does not apply.

And the effort is not wasted under either outcome. Phases 1–3 —
the freshness metric, per-node windows, failover grace, the circuit breaker
— are the instrumentation and the safety rails that *any* liveness
architecture needs, and they are how the D8 decision gets measured in the
first place. Only phase 4's controller is contingent, and it remains the
fallback for clusters that do not run gossip.


---

## 17. The `heartbeatSeconds` clamp widens from 30 to 300, gated on a `selfTerminateAfter` cross-check

**Call.** `heartbeat_interval_for`'s `1..=30` clamp becomes `1..=300`. Config
load rejects any value where `missedBeatsBeforeLost × heartbeatSeconds`
would exceed the smallest `stateful.onLost.selfTerminateAfter` in play
(default 90s):

```
ERROR worker config configs/workers/batch.jsonnet: heartbeatSeconds 30 gives a
  150s lost-node window, but job `db` sets stateful.onLost.selfTerminateAfter
  to 90s — those workers would kill their own stateful tasks between
  heartbeats. Raise selfTerminateAfter above 150s or lower heartbeatSeconds.
```

**Alternatives.** (a) Keep 30. It is a round number, it has never been
complained about, and widening a clamp is a compatibility-shaped change.
(b) Widen it and leave the interaction to documentation, as a note on the
stateful page.

**Trade-off.** A hard error at config load can block a cluster from
starting on a combination that was previously accepted — though only in
combinations that were already silently broken.

**Why (simplicity, by removing a landmine).** 30 was chosen when the lost
window was fixed at 10s, where it was already unusable; it is not a
considered ceiling and it is below what the sizing table needs at 2000
nodes. Widening it is required for the plan to have an answer at all in its
own last row.

The cross-check is the more important half.
`DEFAULT_SELF_TERMINATE_AFTER = 90s` means a worker kills its own stateful
tasks after 90 seconds without manager contact. At a 120s cadence, a
perfectly healthy worker crosses that threshold **between beats** — a
correctness bug produced by the interaction of two independently reasonable
settings, presenting as stateful tasks dying for no visible reason. That is
the worst possible bug to leave for a user to find, and it is exactly the
"two numbers whose relationship matters, unenforced" pattern entry 4 exists
to kill. Rejecting at config load rather than warning is deliberate: the
symptom is data-plane task death, not a slow dashboard.


---

## 18. The `selfTerminateAfter` cross-check is a hard startup failure, not a warning

**Call.** *(Implementation of the previous entry's gate.)* A worker config
whose `heartbeatSeconds x missedBeatsBeforeLost` reaches
`DEFAULT_SELF_TERMINATE_AFTER` (90s) **fails manager startup**, naming both
numbers and the remedy:

```
worker config ./configs/workers/batch.jsonnet: heartbeatSeconds 20 x
missedBeatsBeforeLost 5 is a 100s lost window, at or past the 90s stateful
self-fence (stateful.onLost.selfTerminateAfter). A healthy worker would kill
its own stateful tasks between beats. Lower heartbeatSeconds, lower
missedBeatsBeforeLost, or raise selfTerminateAfter on the jobs that run here
(docs/heartbeat-cadence-and-scale.md)
```

The old "above 3 seconds" warning is replaced by an `info` line stating the
resulting window, since above-3s is now an ordinary supported configuration.

**Alternatives.** (a) Warn and continue, which is what the old clamp did and
what most config-validation in Legion does. (b) Check against the *smallest*
`selfTerminateAfter` actually in play across submitted jobs rather than
against the default constant.

**Trade-off.** An operator who genuinely wants a 100s window and has raised
`selfTerminateAfter` on every stateful job in the cluster is still blocked,
because the check compares against the built-in default rather than against
what is deployed. That is a false positive, and it is accepted.

**Why (correctness).** The failure this prevents is a *healthy* worker killing
its own stateful tasks between beats — data loss with no error anywhere,
presenting as spontaneous task death. A warning at manager startup is read
once and forgotten; this is exactly the class of thing that should not be
discoverable in production. Option (b) is more precise but wrong-shaped: jobs
are submitted after the manager starts, so the check would have to run at
submit time and would then reject a *job* for a *worker-type* setting, which
is a much more confusing error to receive.

---

## 19. Cadence gets its own read-only RPC and `legion node cadence`, rather than fields on `legion node list`

**Call.** A new `GetHeartbeatCadence` RPC and a `legion node cadence` verb,
with `--json`, a `catalog()` entry (`read_only_safe: true`), an MCP tool,
and a `tests/acl_endpoints.rs` row (`node`/`read`/`any`).

```console
$ legion node cadence
Fleet             1000 nodes (987 active)
Served interval   2s
Lost window       10s  (5 missed beats)
Freshness         0.22  ── FALLING BEHIND (false loss below 0.20)
Sustainable       ~160 nodes at 2s on this control plane
Suggested         heartbeatSeconds: 25   (lost window 125s)
```

**Alternatives.** (a) Add the numbers to `legion node list`'s output or a
summary footer — no new RPC, no new ACL row, no new MCP tool, and the
operator is already there. (b) Metrics only: it is all in Prometheus, and a
CLI verb duplicates a scrape.

**Trade-off.** A new endpoint carries the full tax — proto comments, ACL
classification, capabilities entry, MCP tool, test row — for a report an
operator reads occasionally.

**Why (ease, at the moment it is needed).** These are fleet-level facts, not
per-node ones; hanging them off a list of nodes puts a cluster-wide answer
in a per-row command and makes `-j` output awkward for both consumers. And
(b) fails the scenario that matters: an on-call responder watching nodes
disappear needs to know *within seconds* whether the machines are gone or
the control plane is behind, and requiring a working Prometheus to answer
that is requiring a working monitoring stack during the incident where it
is least trustworthy. The suggested `heartbeatSeconds` also has to be
*computed* from live fleet size and observed rate — that is a server-side
calculation, so it needs a server-side endpoint regardless of where it is
printed.


---

## 20. `legion node cadence` answers zeros on a manager that does not own liveness, rather than forwarding

**Call.** *(Implementation.)* `GetHeartbeatCadence` on a manager that does not
lead the status lane returns `is_liveness_owner: false` and zeros, and the CLI
prints:

```console
$ legion node cadence
This manager does not own node liveness (it does not lead the status lane).
Ask the manager that does — `legion raft status` names it.
```

**Alternatives.** (a) Forward the call to the status-lane leader, so any
manager gives the real answer. (b) Answer from the local replica's persisted
heartbeat clocks, which every manager has.

**Trade-off.** An operator who happens to hit the wrong manager gets a
redirect instead of an answer, in the middle of an incident.

**Why (honesty).** (b) is the dangerous one: the persisted clocks are
checkpoint data, up to 30s stale by design, so a freshness ratio computed from
them would read catastrophically low on a perfectly healthy cluster — a metric
that manufactures the alarm it exists to detect. (a) is defensible but makes a
diagnostic command depend on the status leader being reachable, on exactly the
page an on-call responder opens when things are broken; the same reasoning the
liveness plan applies to `legion node list`'s read path. A one-line redirect
naming the command that finds the owner is a worse UX than an answer and a far
better one than a wrong answer.

---

## 21. Shipped without the AWS reproduction; the 1000-node collapse test is not run

**Call.** *(Implementation.)* Both plans' AWS acceptance runs —
`heartbeat-commit-probe.sh` on gp3 versus NVMe, and the 1000-simulated-node
adaptive-on/adaptive-off reproduction that is the cadence plan's phase-4
acceptance criterion — have **not** been run. Correctness is covered by unit
tests over the pure decision functions and by integration tests on the
`tests/raft_lanes.rs` 3-node harness; scale is not.

**Alternative.** Hold the change until the rig is available.

**Trade-off.** The headline performance claim — that `R` stops being a
property of the storage device — is argued from the design and from the
absence of commits on the path, not from a measurement. The claim that a
saturated fleet survives with adaptive cadence on is likewise unmeasured. Both
are stated as unverified rather than as results, here and in
`internal_docs/TODO.md`; no benchmark number in `benchmarks/` or in any doc
has been updated to claim otherwise.

**Why.** Every *correctness* property the plans exist to protect — zero losses
across an election, per-node windows, the circuit breaker, peer-gap
suppression, the digest under a standing lane split — is verifiable without
the rig and is verified. The performance properties are not, and saying so
plainly is better than either blocking the correctness fixes on shared
hardware or quietly implying the numbers exist.
