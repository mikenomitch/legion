# Judgement Calls: Edge Ratification Throughput

Calls made executing
[`plans/edge-ratify-throughput-plan.md`](../plans/edge-ratify-throughput-plan.md).
Entries are ranked by project impact, highest first. The number is the rank.

## 1. A ratification is fenced by committed state, not by epochs

**Call.** A plan submission that names a `target_instance` (a
ratification) carries no version vector, and the apply compares neither the
job-config epoch nor the node epoch for it. Its one candidate goes straight
to the in-transaction re-validation (fresh node row, availability,
constraints, permit, a free pool rebuilt from committed allocation rows,
ports, `distinct`), and the instance bind decides ownership. A ratification
still bumps both epochs when it binds, so scheduler plans stay fenced
against it.

```console
# before, the second start of a burst, on a sandbox that already booted
refused: plan epoch for job config edge-sandboxes-3 is stale; expected 41, latest 42
Error: sandbox start refused (internal): plan epoch for job config ...

# after
admitted: claim admitted locally
booted
ratified
```

**Alternatives.** (a) Keep the fences and have the worker retry `internal`
refusals inside its lease, which the public refusal table already promised.
(b) Keep the job-config fence but sample it inside the entry, so it always
passes. (c) Keep the node-epoch gate and drop only the job-config one, so
bursts across nodes confirm and bursts on one node still serialize.

**Trade-off.** Ratification's correctness now rests entirely on the
in-transaction checks. A future invariant that is protected *only* by the
epoch, and not re-derived in the apply, would not be enforced for
ratifications. That is the same exposure the scheduler's revalidation pass
(the late-binding plan's pass 2) already accepted for every plan whose node
epoch moved, so it is not a new class of risk, but it is now load-bearing
for edge boot as well. The comment at the walk names the checks a
ratification relies on, so the next person adding an invariant sees the
list.

**Why (performance, and correctness of the verdict).** The epochs were not
protecting anything a ratification does. The job-config epoch serializes
job-level arithmetic (a replica count planned against a snapshot), and a
keyed instance has no count. The node epoch stood in for "the planner's fit
check is still valid", and a ratification never planned against a
snapshot. The old comment said a moved epoch "may mean somebody already owns
this sandbox", but the bind in the same transaction reads exactly that, and
the existing lost-race test was already passing on the bind, not the epoch.
Meanwhile the fences cost most concurrent starts: measured on the base,
1,072 to 1,346 of 2,000 claims were refused at 64 and 256 callers (plan
§8). That is with the handler still blocking the runtime, which held
entries to about 1.7 claims. Filling the entries alone would have refused
all but one claim per entry. Option (a) would turn those refusals into retries,
which keeps the false conflicts and multiplies the entries they cost.
Option (b) is (this call) disguised as a fence, which invites the next
reader to "fix" it back. Option (c) leaves a dense node starting several
sandboxes a second refusing all but one per entry, which is the per-node
case the TODO asked about.

## 2. A node finds the raft leader itself, and followers do not forward `RatifyClaim`

**Call.** Only the raft leader ratifies. A follower answers `RatifyClaim`
with `FailedPrecondition` ("this manager is not the raft leader"), writes
nothing, and leaves the ticket unspent. It no longer returns an `internal`
refusal verdict. The node's ratifier then tries the other managers it
knows, in order, and keeps the one that ratified for the next claim, until
that one answers not-leader too.

```text
# before, three managers: a ratification that reached a follower
admitted
booted
refused: ... this manager is not the raft leader; retry on its leader ...
Error: sandbox start refused (internal): ...

# after
admitted
booted
ratified
```

**Alternatives.** (a) Let the follower forward. The existing
`leader_forward` layer already proxies a rejected unary Manager write to the
leader and replays the caller's bearer credential, so `RatifyClaim` would
only need to fail with the typed not-leader status that carries
`x-legion-raft-leader`. (b) Have the node retry `internal` refusals in
general, which entry 4 declined.

**Trade-off.** Every caller of `RatifyClaim` needs the retry. The node has
it, but a CLI or a third-party ratifier gets `FailedPrecondition` and must
find the leader itself (open in `TODO.md`). The first ratification after a
leader change pays one wasted round trip per manager tried, inside the
claim's lease. A call that timed out is not retried on another manager,
because the leader may have ratified it and a second ratification would be
refused and kill the winner. That claim dies, and its caller starts it
again. Only a not-leader answer or a call that never connected
(`Unavailable`) moves on, and any unreachable answer drops the cached
manager.

**Why (performance, then simplicity).** A forwarded ratification runs
twice. The follower's handler verifies the ticket and tries the write
before the layer sees the rejection, then the whole request is replayed on
the leader. On a path whose point is latency, a node that remembers the
leader sends each ratification one hop, to the manager that commits it.
The retry is safe because the follower wrote nothing and the ticket is
still unspent, so the same request can go to the next manager. The node
already holds the manager list it heartbeats with, so the retry needs no
new setting. Before this, with three managers, most edge starts were
refused and killed after boot. Option (a) can still be added on top for
other callers without changing the node.

## 3. A just-ratified claim is kept from the stop sweep for up to 30 seconds

**Call.** When a claim ratifies, the worker records it. Until a desired set
lists it, or for 30 seconds (`RATIFIED_SETTLE` in
`src/worker/task_manager.rs`), whichever comes first, the claim is
protected from the sweep that stops work a desired set leaves out. After
that, an absence means stop again.

**Alternatives.** (a) Stamp each desired set with the raft index it was
read at, and have the worker ignore a set older than the ratification's
commit index. (b) Send every heartbeat to the leader, so no desired set
comes from a lagging replica. (c) Reuse the unratified claim's protection,
which lasts for the claim's lease.

**Trade-off.** A claim the managers want gone right after its ratification,
such as an instance stopped within the window, can run up to 30 seconds
longer than it should. The window is a guess at how long an election takes,
not a fact the worker can check.

**Why (correctness, then simplicity).** A heartbeat answered by a follower
that had not applied the ratification yet, or by a leader that built the
set just before it, omitted the confirmed claim, and the worker stopped it:
378 ratified claims on one worker, with cause `unknown`. Option (a) is
exact, but it changes the heartbeat and `RatifyClaim` wire shapes to close
a window that is milliseconds long on a healthy cluster. Option (b) puts
every heartbeat on one manager. Option (c) is 5 seconds by default, shorter
than an election. With the window, the 300-id race survived a leader stop
with every winner still running 80 seconds later
([RESULTS](../../benchmarks/aws/sandbox-boot/RESULTS.md#ratification-on-three-managers-2026-10-14)).

## 4. Correct the refusal table instead of adding the worker retry

**Call.** The public refusal table in `docs/edge-boot.md` said `internal`
is retried inside the lease. The worker never did that (`drive_claim`
kills the work on every refused verdict). The port fixes the doc to say
no reason is retried, and leaves the retry as an open TODO item, to be
built with the `overloaded` refusal.

```text
The node kills the work on every refusal. No reason is retried inside the
lease, so a start refused as `internal` (a store error or an election
during the ratification) is lost, and the caller starts it again.
```

**Alternatives.** Build the retry now: on `internal`, call `RatifyClaim`
again until the lease runs out.

**Trade-off.** A store error or an election during a ratification still
costs the start, and the platform has to start it again.

**Why (simplicity, honesty of the public doc).** After entry 1, `internal`
no longer means a false conflict, so it is rare. A retry loop needs
backoff, lease accounting, and a rule for when the ticket was already
spent, and the `overloaded` refusal needs the same loop. Building it twice,
or building it inside a throughput port, is the wrong place. The doc must
not promise behavior the node does not have.

Entry 2's retry is a different thing. A not-leader answer is not a
verdict, and nothing was written, so sending the same request to another
manager is routing, not a second try at a refused claim.

## 5. Measure in-process, through the real handler, before building a networked load generator

**Call.** The first harness (`tests/ratify_throughput_measure.rs`) drives
the real `RatifyClaim` handler in-process with real tickets, on local
SQLite and on a single-node raft cluster, and the networked
`legion-loadgen --mode ratify` the TODO described is Phase 3.

**Alternatives.** Build the loadgen mode first: mint tickets over gRPC,
give the embedded simulated fleet a way to call `RatifyClaim` with each
node's credentials, and drive it against a real manager.

**Trade-off.** The numbers leave out the network hop, HTTP/2 framing,
node-token authentication, and a follower round trip. They are a ceiling for
the manager's commit path, not an end-to-end rate, and the public doc and the
plan say so.

**Why (simplicity).** The question that mattered first was where the time
goes inside the manager, and an in-process harness answers it with one
process, no ports, and a clock shared by caller and handler. It found the
false conflicts within one run. A networked mode needs the simulated fleet
to expose per-node credentials, which is a change to `legion start worker
--simulate` worth making when there is an AWS cell to run it in.

## 6. Defer per-node ratification batching

**Call.** No batch `RatifyClaims` RPC and no worker-side coalescing window
in this phase.

**Alternatives.** The TODO's proposal: the worker gathers claims for a
short window (a few milliseconds) and sends one RPC carrying all of them,
with the window and a batch cap as knobs.

**Trade-off.** A node starting hundreds of sandboxes a second still makes
one RPC per start, paying an HTTP/2 stream, a node-token check, and a
ticket signature verification each.

**Why (performance, measured).** Once the false conflicts were gone, the
manager's group commit already put concurrent ratifications from one node
into the same entry: one node's claims and 256 nodes' claims confirmed
within about 10 percent of each other at 256 callers (415 and 461 per
second on local SQLite, 372 and 381 on raft, plan §8). The serialized resource is the entry, and RPC batching shrinks
neither the per-ratification apply inside it nor the per-entry raft cost. It
would only cut per-RPC overhead, which runs in parallel across the
manager's cores, while adding the coalescing window to every start's
latency, on a path whose whole point is latency. If the AWS numbers show
manager CPU rather than the scheduler lane as the bound, this is the first
lever to pull.

## 7. No new knob for the group-commit cap

**Call.** Ratifications keep sharing the instance group commit's cap of 64
(`GROUP_COMMIT_BATCH_MAX`). No separate ratification cap, and
`schedulerSettings.plan_apply_batch_max` is not involved.

**Alternatives.** A `schedulerSettings` knob for the instance group-commit
cap, or a separate cap for ratifications, as the TODO suggested measuring.

**Trade-off.** An operator cannot raise the cap without a rebuild.

**Why (simplicity).** Ratifications never reached the plan coalescer (the
nested apply runs inline), so the knob the TODO named does not apply. Entries
reach the cap only at 256 callers, and the confirmed rate is already flat
from 64 callers (442 and 461 per second on local SQLite while entries grow
from 18 to 56), so a bigger cap would lengthen each entry without raising
the rate. A knob nobody needs to turn is one more thing to explain.

## 8. The port implemented Phase 1 from the plan

**Call.** The source branch (`perf/edge-ratify-throughput`, uncommitted
on base 89229706) carried this plan, the docs, the harness, and the
regression tests, but no `src/` changes, although the docs described the
change as shipped. The port onto `origin/main` (after PR #43) wrote the
code from §6 and kept the rest.

**Alternatives.** Port only the harness and the docs, mark the plan
`PLANNED`, and fix the docs to stop describing unshipped behavior.

**Trade-off.** The code was written fresh during the port rather than
reviewed as part of the source branch.

**Why (correctness, measured).** The three regression tests failed on
`origin/main` exactly as the plan describes, and the harness showed more
than half of concurrent starts killed. The change is small (a skipped
compare, an in-entry existence check, and `block_in_place`), each piece is
covered by a regression test, and the before/after numbers in §8 come
from the same harness.

## 9. The dense-node test counts slots with `maxAllocations`

**Call.** `concurrent_ratifications_on_one_node_confirm_up_to_its_capacity`
gives its node `maxAllocations: 8`. The source version relied on "8 cores
and 8192 MiB".

**Alternatives.** Give the shared test fixture real fingerprint totals
(`memory_total`, `cpus`).

**Trade-off.** The test exercises the `allocations` dimension, not memory
or CPU. The same free-pool check enforces all three.

**Why (simplicity).** The fixture's fingerprint uses keys the resource
pool does not read (`memory_mb`, `cpu`), so its node has unlimited memory
and CPU, and the source test confirmed all 24 claims once the epoch fence
was gone. Changing the shared fixture would touch every other test in the
file. One explicit limit in the one test that needs it is clearer.
