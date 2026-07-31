# Judgement calls — Durable Dispatch Outbox

Decisions taken while writing
[`../plans/dispatch-outbox-plan.md`](../plans/dispatch-outbox-plan.md).
**Entries are ranked by project impact, highest first; an entry's number is
its rank.** Insert new entries at their rank rather than appending.

Nothing here has shipped — these are design calls made in the plan. Calls
made *during* implementation get appended to this file at their rank, and
any open question resolved along the way becomes an entry with the
alternative that lost.

## 1. Commands are the fast path; desired-state reconcile stays the recovery path

**Call.** The outbox delivers allocations at RTT latency, and the existing
desired-state reconcile stays in the system permanently as the convergence
backstop. It stops running every 2s (Phase 3), but it is never deleted,
and any delivery anomaly — sequence regression, backlog truncation,
unknown node — falls back to it via `resync_required`.

**Alternatives.** (a) Pure command delivery, the shape Tensorlake
describes: commands are the only mechanism, and correctness comes from
durability plus sequence-number acks. This is the cleaner architecture and
gives the better steady-state number, because there is no second path to
keep alive. (b) Keep reconcile as-is and add nothing — every dispatch
failure heals in ≤2s already, which is genuinely fine for most clusters.

**Trade-off.** Two delivery paths must agree forever, which is a
maintenance burden and a class of bug (the paths drifting) that a
single-path design does not have. The plan pays for this with an
equivalence property test, which is real work and will occasionally fail
for reasons that are about the test rather than the system.

**Why (simplicity, and honestly: nerve).** Reconcile is what makes Legion
forgiving of bugs we have not thought of yet — including bugs in the
outbox. A pure command system is correct exactly to the degree its command
emission is correct, and the failure mode is a workload that never starts
and never self-corrects. Keeping the backstop means a sharding bug, a
missed command, or a bad migration costs latency instead of an outage, and
it means `dispatch.mode: 'push'` is a config change rather than a
rollback. The cost is one property test; the alternative's cost is
unbounded.

## 2. The command is written in the plan-apply transaction, not by an async writer

**Call.** `submit_and_apply_plan` gains the command write. The decision
and its delivery instruction commit together or not at all.

**Alternatives.** (a) An async writer that tails committed allocations and
emits commands afterwards — keeps the hottest write in the system exactly
as wide as it is now. (b) Deriving commands lazily at poll time from the
allocation table, which needs no new table at all.

**Trade-off.** It widens the write that every measurement says is the
dominant phase (`apply` is the largest phase on every backend in the
baseline matrix). We are adding bytes to the exact transaction we have
spent a campaign shrinking.

**Why (performance, narrowly).** The whole value of an outbox is that the
command cannot be lost relative to the decision — an async writer
reintroduces the window this plan exists to close, and a lazy derivation
is just today's reconcile with more steps. The cost is affordable
specifically because D5 group commit already landed: concurrent applies
share one quorum round, so the marginal command write amortizes across the
batch rather than paying a fsync each. If Phase 1's benchmark says
otherwise, alternative (a) is the fallback and the plan is worse but still
worth shipping.

## 3. Long-poll, not a server-push stream

**Call.** The worker calls `PollWork` with its cursor and the manager
parks the request; delivery is a reply to a request the worker made.

**Alternatives.** (a) A server-initiated bidi stream, which is the obvious
modern shape and removes the "who reconnects" question. (b) Server push
over the existing heartbeat stream, adding no RPC at all.

**Trade-off.** 10k nodes means 10k parked HTTP/2 streams and a per-node
`Notify` on the manager, which is real memory and real interaction with
connection limits. A push model would let the manager decide who to talk
to and when.

**Why (simplicity).** The cursor lives with the consumer, which makes
restart semantics trivial: a worker that restarts polls with what it has
and the manager tells it whether that is good enough. Push models put the
cursor on the server and turn every worker restart into a server-side
state question. Nomad has run blocking queries at this scale for a decade,
which makes this the boring choice rather than the clever one. Open
question 2 in the plan holds the memory measurement that could overturn
it.

## 4. `desired_allocations` gets a companion presence flag rather than overloading empty

**Call.** When the heartbeat stops carrying desired state (Phase 3), the
reply gains an explicit `desired_allocations_present` boolean rather than
letting an empty list mean "nothing to say".

**Alternatives.** (a) Overload the empty list — no wire change, and the
worker infers from its own polling state whether an empty set is
meaningful. (b) Version the meaning by protocol number alone, so a v3
worker simply knows empty means absent.

**Trade-off.** One more field on a message we send thousands of times a
second, for a case that only arises during a mode transition.

**Why (correctness at a boundary).** An empty `desired_allocations`
already has a load-bearing meaning today: *stop everything on this node*
(`proto/legion.proto:2583` — a worker learns to stop by noticing
absence). Overloading it makes "the manager has nothing to tell you"
indistinguishable on the wire from "drain this node", and the failure mode
of getting that wrong is a fleet-wide stop. A bool is cheap; that class of
bug is not.

## 5. Per-node sequence numbers, not one global sequence

**Call.** `seq` is monotonic per node.

**Alternatives.** A single global monotonic counter, which gives a total
order across the cluster and makes debugging ("what happened at seq
41822?") much nicer.

**Trade-off.** No cross-node ordering. We cannot say "node A's command
preceded node B's" from the outbox alone, which will be missed the first
time someone debugs a cross-node rollout.

**Why (performance).** A global counter is a single contended row updated
at exactly the rate this plan is trying to raise — it would be the new
bottleneck, in the same transaction we just widened. Per-node sequences
also match what the consumer needs: a worker only ever reasons about its
own cursor. Cross-node ordering, when we want it, belongs in the event
stream, which already exists for that purpose.

## 6. Three throughput numbers, always reported together

**Call.** The plan commits in advance to reporting
`commands_committed_per_sec`, `commands_delivered_per_sec`, and
`allocations_running_per_sec` as a triple, with correctness and resync
rate attached, and to never publishing the first as if it were the third.

**Alternatives.** Report the headline number like everyone else and put
the qualifications in a footnote — which is what the industry claim that
prompted this plan does, and it is effective marketing.

**Trade-off.** Our headline number will be smaller than a competitor's,
sometimes by an order of magnitude, for systems that may be doing the same
amount of real work. That is a genuine commercial cost and it should be
named rather than waved away.

**Why (the public claim).** The architecture review set the bar as "an
architecture whose measured scaling curves support 'fastest at scale'
without asterisks that embarrass us." A decision rate presented as a start
rate is exactly that asterisk, and it is discoverable by anyone who reads
two of our posts — which is how the claim that prompted this plan came
apart. The benchmark README already holds this line internally ("treat
absolute evals/sec as indicative"); this entry extends it to what we
publish.
