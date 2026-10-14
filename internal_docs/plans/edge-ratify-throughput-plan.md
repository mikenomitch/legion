# Plan: Edge Ratification Throughput

> **Judgement calls:** [`calls/edge-ratify-throughput-plan_calls.md`](../calls/edge-ratify-throughput-plan_calls.md)

**Status: IN EXECUTION.** Phase 1 shipped: the ratification load harness,
ratifications fenced by committed state instead of by epochs, the
create-on-ratify decision taken inside the entry, and ratification off the
async runtime. Phase 2 (per-node ratification batching) is deliberately
deferred: the Phase 1 numbers say it would not move the bottleneck (§8).
Phase 3 (the networked `legion-loadgen` ratify mode on the AWS rig) is
planned.

The public doc is [`docs/edge-boot.md`](../../docs/edge-boot.md)
("Ratification under load"). Before/after numbers are in §8.

## 1. Problem

Every edge start boots without a manager, but it still needs one durable
ratification: `RatifyClaim` → `edge_boot::ratify` → a one-placement plan
apply inside one scheduler-lane group-commit entry
(`src/manager/edge_boot.rs`, `src/storage/store/group_commit.rs`). That
write caps sustained edge starts per second, and nothing about it had been
measured.

Measuring it found something worse than a cap. **Concurrent ratifications of
one sandbox job refused each other.** A ratification sampled the job-config
epoch and its node's epoch before it queued, and the apply treated both as
hard compare-and-set fences. Every applied plan bumps both. So when 64
ratifications for one job shared a group-commit entry, the first one bound
and the other 63 failed the job-config check with `plan epoch for job config
… is stale`, classified as `internal`. The same happened to every claim from
one node after the first, on the node epoch.

A refusal is not a retry. The worker applies every refused verdict as a kill
(`src/worker/server.rs::drive_claim` → `edge::apply_verdict`), whatever the
public doc's refusal table says about `internal`. So under any real burst,
most sandboxes a node had already booted were killed, and the platform saw
random `refused` starts with an `internal` reason that pointed nowhere.

Two smaller problems sat behind it:

- **The handler blocked the async runtime.** `ratify_claim` called the
  blocking group-commit wait directly on a tokio worker thread. With four
  worker threads, at most four ratifications could be waiting for an entry
  at once, so the group commit could never fill, and a ratification burst
  stalled heartbeats and every other RPC on the manager.
- **The create raced itself.** An edge start for an id that never existed
  decided "create it" before it queued. Two nodes holding the same ticket
  both decided so, and the loser's create failed with `instance id … already
  exists`, which also classified as `internal` instead of
  `instance_claimed`.

## 2. Prior art

- **Nomad's plan applier** re-checks each node's fit against the latest
  state snapshot (`AllocsFit` in `evaluatePlan`) and commits the part of a
  plan that still fits. It has no version number for "the job": two plans
  for the same job conflict only if they overcommit a node. We take that
  for ratifications, which have no job-level arithmetic to protect.
- **Kubernetes** binds a pod with a per-pod `Binding` write, fenced by the
  pod's own `resourceVersion`. Two pods of one ReplicaSet bind to nodes
  independently, and the kubelet's admission is the last word on fit.
  Conflicts are per object, never per controller. A ratification is the
  same shape: one named object (the keyed instance) bound to one node.
- **Group commit** (PostgreSQL `commit_delay`, MySQL binlog group commit)
  amortizes one fsync or one replication round over many independent
  transactions. Legion already has it (`Store::scheduler_group_commit`).
  The work here is letting ratifications actually share it.

What we reject: per-node RPC batching on the worker as the first lever.
It is what the original TODO proposed, but it attacks per-RPC overhead
that runs in parallel on the manager's cores, not the serialized commit
(§8).

## 3. Jobs to be done

**Platform operator running a sandbox fleet**
- When a thousand users open sandboxes in the same second, I want every
  sandbox a node already booted to be kept as long as there is room for it,
  so users do not see random failures that a retry then fixes.
- When I size managers for an edge fleet, I want to know how many starts
  per second one manager confirms, so I can plan before the launch, not
  during it.

**On-call responder**
- When edge starts are refused, I want the reason to say what really
  happened (another node won, the node is full), so I do not chase
  contention that is not there.

**Autonomous agent**
- When I start sandboxes through `legion sandbox start --json`, I want a
  refusal to mean the start genuinely cannot happen, so I can act on the
  reason instead of blindly retrying.

*Not in scope:* the job author. Nothing in the jobspec changes.

## 4. Goals and non-goals

**Goals**

- Concurrent ratifications of one job, from many nodes or one node, all
  confirm when they fit.
- A refusal names the real cause: `instance_claimed`, `no_capacity`,
  `ineligible`, or a genuine store error as `internal`.
- A ratification burst fills group-commit entries and does not stall the
  manager's async runtime.
- A repeatable harness that reports confirmed starts per second, refusals
  by reason, verdict latency, entry size, and manager CPU per claim.

**Non-goals**

- Admission control and an `overloaded` refusal before lease expiry. That
  is separate work (TODO "Backpressure before lease expiry").
- Per-entry raft cost (changeset capture, pipelining) and plan-commit cost
  that grows with fleet size. Those are their own TODO items and dominate
  the numbers here once the false conflicts are gone.
- Worker-side retry of `internal` refusals inside the lease. It is a real
  doc/code gap, recorded in TODO, but after this change `internal` means a
  genuine store failure or an election, which the retry work should handle
  together with `overloaded`.
- A new knob. There is nothing to tune: the fence is a correctness
  property, not a preference.

## 5. Background: what exists today

- `RatifyClaim` (`src/manager/server.rs::ratify_claim`) authenticates the
  node, re-verifies the ticket, and calls `edge_boot::ratify` directly on
  the async runtime.
- `edge_boot::ratify` (`src/manager/edge_boot.rs`) samples
  `allocation_version_vector_for_candidates(job_config_id, [node])` from
  the epoch mirror, decides whether this start is the create, and submits
  one closure to `Store::scheduler_group_commit`: spend the ticket, create
  the instance, `submit_and_apply_plan_for_instance`, record
  `alloc.ratified`. Group commit is capped by `GROUP_COMMIT_BATCH_MAX`
  (64). The nested plan apply runs inline (`capture::in_mutation_body`), so
  ratifications never touch the plan coalescer or
  `schedulerSettings.plan_apply_batch_max`. The TODO's "capped by
  `plan_apply_batch_max` (default 16)" was stale.
- `apply_plan_submission_on` (`src/storage/store.rs`) checks the
  job-config epoch first, all or nothing, then walks candidates. For a
  plan with `target_instance` the node epoch was a hard gate and the
  revalidation pass never ran. The comment's reason was that "a moved
  epoch may mean somebody already owns this sandbox, which no capacity
  recheck can disprove".
- That reason does not hold. Inside the same transaction the walk
  re-derives everything the epoch stood in for: the fresh node row and its
  availability, constraints and permit, a free pool rebuilt from committed
  allocation rows (with `reservedResources` and `maxAllocations`), static
  ports, and `distinct`. Ownership is decided by
  `assign_keyed_instance_tx`, which binds the named instance only if no
  allocation of this task config holds it, reading committed rows in the
  same transaction. `two_nodes_racing_for_one_sandbox_leave_exactly_one_owner`
  already showed that bind, not an epoch, refusing the loser.
- The job-config epoch serializes plans that do job-level arithmetic: a
  count of replicas planned against a snapshot. Keyed instances have no
  count ("keyed allocations are driven by CreateInstance, not count",
  `assign_keyed_instance_tx`), and a ratification never plans against a
  snapshot.

## 6. Design

### 6.1 A ratification is fenced by committed state, not by epochs

`edge_boot::ratify` stops sampling a version vector, and the apply skips
both epoch comparisons for a submission that names a `target_instance`.
The candidate (there is exactly one) goes straight to the in-transaction
re-validation that the scheduler's revalidation pass already trusts, and
the instance bind is the ownership fence, as it always was.

A ratification still **bumps** the job-config and node epochs when it
binds. That keeps every scheduler plan fenced against it: a plan sampled
before a ratification landed is still rejected or falls back.

What the user sees: a burst of edge starts of one job all confirm, and a
full node refuses with the reason that is true. Before, most starts in a
burst ended like this, on a sandbox that had already booted:

```console
$ legion sandbox start edge-sandboxes \
    --id user-913 --node https://10.0.1.7:4848 --ticket "$TICKET"
admitted: claim admitted locally
booted
refused: plan epoch for job config edge-sandboxes-3 is stale; expected 41, latest 42
Error: sandbox start refused (internal): plan epoch for job config ...
```

After, the same start reads `admitted`, `booted`, `ratified`. A node that
is genuinely full refuses with `refused (no_capacity): no candidate for
task … (node w-17 no longer has capacity for task …)`.

### 6.2 The create is decided inside the entry

`ratify` still checks outside the entry whether the id could be a create
(a valid keyed id that does not exist yet). Inside the entry it checks
again, through the write connection, so a batch-mate's create in the same
entry is visible. If the row now exists, the start skips the create and
lets the bind decide, which refuses a lost race as `instance_claimed`.

### 6.3 Ratification runs off the async runtime

`ratify_claim` runs `edge_boot::ratify` through `block_in_place`, the
same helper admission work already uses. A waiting ratification holds a
blocking thread, not one of the runtime's worker threads, so a burst
fills the group commit up to its cap and heartbeats keep flowing.

### 6.4 The harness

`tests/ratify_throughput_measure.rs` (ignored by default) drives the real
`RatifyClaim` handler in-process: a multi-use prefix boot ticket minted
through `IssueBootTicket`, ticket re-verification, the spend, the
create-and-bind, the event, and the committed payload. It runs two fleet
shapes (256 nodes, and one dense node) on two backends (local SQLite and a
single-node raft cluster) at several concurrency levels, and reports
confirmed starts per second, refusals by reason, verdict p50/p99, mean
group-commit entry size, and manager CPU per claim.

## 7. Proto, struct, config, and persistence changes

None. The `RatifyClaim` proto comment no longer claims an epoch
compare-and-set. No new config, no new job-config field, no migration.
Ratification `plan_submission` rows now carry an empty
`expected_versions`.

## 8. Implementation phases

**Phase 1 (shipped).** §6.1 to §6.4, with regression tests and
before/after numbers (below).

Measured 2026-10-14 with `tests/ratify_throughput_measure.rs` on an Apple
M5 laptop (10 cores, 32 GB) shared with other agents' builds (load
average about 20). Base is `origin/main` at 37f43a5b, after PR #43.
Baseline and change ran back to back, twice. Round 1 is shown, and round
2 agreed within the noise. 2,000 claims per cell, 4 runtime threads.

| Store | Shape | Callers | Base confirmed/s | Base refused (`internal`) | Base claims/entry | After confirmed/s | After refused | After claims/entry |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| local | fleet | 1 | 334 | 0 | 1.0 | 320 | 0 | 1.0 |
| local | fleet | 64 | 404 | 1,072 | 1.6 | 442 | 0 | 18 |
| local | fleet | 256 | 418 | 1,145 | 1.7 | 461 | 0 | 56 |
| local | one node | 64 | 257 | 1,170 | 1.7 | 377 | 0 | 17 |
| local | one node | 256 | 256 | 1,111 | 1.6 | 415 | 0 | 54 |
| raft | fleet | 64 | 149 | 1,300 | 1.7 | 357 | 0 | 29 |
| raft | fleet | 256 | 157 | 1,305 | 1.7 | 381 | 0 | 61 |
| raft | one node | 64 | 122 | 1,346 | 1.8 | 345 | 0 | 28 |
| raft | one node | 256 | 118 | 1,337 | 1.8 | 372 | 0 | 61 |

What it says:

- On the base, 54 to 67 percent of concurrent starts were refused as
  `internal` with `plan epoch for job config ... is stale`, and the node
  killed each one. After, none are refused.
- Confirmed starts per second rise 1.1 to 1.6x on local SQLite and 2.4 to
  3.2x on raft, where the per-entry cost is higher and a full entry
  amortizes it.
- Verdict p50 rises with callers (about 140 ms at 64, 540 ms at 256 on
  local). On the base most verdicts were fast refusals. Now every claim
  queues for the lane, so latency follows Little's law at the commit rate.
- The rate is flat from 64 to 256 callers while entries grow from about
  20 to about 60, so the bound is the serialized per-ratification work
  inside the entry (about 2 ms of manager CPU per claim here), not the
  entry count or the cap.

**Phase 2 (deferred): per-node ratification batching.** One RPC carrying
many claims from a node that starts many per second, with a short
coalescing window on the worker. Deferred on the Phase 1 data: the
serialized resource is the entry (one transaction applying each
ratification in turn, plus the per-entry raft cost), and RPC batching
does not shrink either. It would cut per-RPC overhead (HTTP/2 stream,
auth, ticket signature check), which runs in parallel across the
manager's cores, and it would add the window to every start's latency.
Revisit if manager CPU, not the scheduler lane, becomes the measured
bound on the AWS rig.

**Phase 3 (planned): networked load test.** A `legion-loadgen --mode
ratify` against a real manager and `--simulate` nodes, on the AWS rig with
a 3-voter raft cluster, so the numbers include the network, HTTP/2, and a
follower round trip. Report the rate where p99 crosses `ratifyTimeout`.

## 9. Testing plan

- `tests/edge_boot_integration.rs`:
  - `concurrent_ratifications_for_one_job_all_confirm`: 16 nodes ratify 16
    new sandboxes of one job at once, all confirm.
  - `concurrent_ratifications_on_one_node_confirm_up_to_its_capacity`: 24
    claims on an 8-slot node, exactly 8 confirm and 16 are `no_capacity`.
  - `nodes_racing_to_create_one_sandbox_leave_exactly_one_owner`: 8
    claims from two nodes for one never-existed id, exactly one owner and
    every loser `instance_claimed`.
  - The existing suite (lost race, tombstone, cordon, ticket spend,
    reschedule) unchanged.
- `tests/ratify_throughput_measure.rs` for the numbers.
- The late-binding and plan-contention suites run unchanged: the
  scheduler's own plans keep both epoch fences.

## 10. Rollout

No gate and no knob. A cluster that upgrades sees ratifications stop
refusing each other. The visible differences: `alloc.refused` events with
`internal` and an `is stale` or `epoch moved` detail stop appearing, and
`legion_plan_apply_total{outcome="rejected"}` drops by the same amount.
Mixed-version managers are fine: the change is inside one op body, and the
changeset it produces is ordinary rows.

## 11. Open questions

- Should the instance group-commit cap (64) be its own knob, or separate
  for ratifications? Lean no: entries reach the cap only at 256 callers,
  and the confirmed rate is already flat from 64 callers, so a bigger cap
  would lengthen each entry without raising the rate. Revisit with Phase 3
  numbers.
- Where do the roughly 2 ms per ratification go? Profiling the apply
  inside the entry is the next lever for the confirmed rate.

## 12. Acceptance criteria

- Phase 1: the three regression tests fail on the base and pass here,
  and the harness shows zero `internal` refusals at every concurrency
  where the base refused more than half the claims. Met.
- Phase 3: a published ladder of offered rate against confirmed rate and
  p99 on the AWS rig.
