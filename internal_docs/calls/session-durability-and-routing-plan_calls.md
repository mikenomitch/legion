# Judgement Calls: Session Durability and Routing Back

Calls made in [`plans/session-durability-and-routing-plan.md`](../plans/session-durability-and-routing-plan.md).
Entries are ranked by project impact, highest first; the number is the rank.

## 1. Durability lives on the node, and raft gets no per-session rows by default

**Call.** Sessions survive a worker restart through a node-local
`sessions.db`, and sleep on their node's disk. Raft stores nothing per
session in Phases 1 to 3.

```text
$ legion instance start workspace-q4n7c2rd.9m3x... -j
{"ok":false,"error":{"code":"NOT_FOUND","message":"session ... was evicted from w-17 at ... (disk pressure); a local-only session cannot be restored", ...}}
```

**Alternatives.**
- (a) The proposal that prompted the plan: managers batch each session's
  location into a raft table, eventually consistent, and route from it.
- (b) A raft row per session written synchronously at start, which is
  keyed mode.

**Trade-off.** A session on a node that is down and never returns is
lost, and while the node is silent its sleeping sessions are missing from
listings (`complete = false`). A replicated directory would at least list
them.

**Why (performance, then simplicity).** Today's gap is that a session is
a process in memory, so a worker restart ends it; a raft row pointing at
it would not have saved it. Batching makes (a) cheap in entries (one per
manager per second) but not in rows: every manager stores every
node-held session and every snapshot carries them, which at the plan's
10^8 idle target is past sandbox-scaling §6.11's 4 GiB trigger. The node
already knows exactly what it holds. (a) stays as Phase 4 behind a
measured trigger (plan §6.5).

## 2. The node is in the session id

**Call.** A minted session id carries an 8-character keyed hash of its
start node: `workspace-q4n7c2rd.7k2qd9x4m1b8c3rj5t0v6wye2h`. Any manager
routes `get`, `stop`, `start`, and `promote` to that node with no lookup.

**Alternatives.**
- (a) Keep the per-manager index plus peer fan-out for every lookup (today).
- (b) Route from a replicated directory (entry 1's alternative).
- (c) Tag with the envelope allocation id, which is already a raft row.

**Trade-off.** Ids grow by 9 characters, and the shape changes, which
breaks any platform that parsed it. A session resumed on another node
keeps a stale tag and pays the fan-out. Idempotent creates cannot be
tagged, because their id is derived before placement.

**Why (performance, simplicity).** The tag is exact at mint and costs no
write, so the common read is one worker-plane call instead of one per
manager. A collision or stale tag costs a probe, never a wrong answer,
because the node answers for itself. (c) goes stale on every job update
that replaces envelopes in place.

## 3. Session tasks may suspend with `durability = "local"`

**Call.** Relax the submit refusal at `src/structs/session.rs:408`: a
session may sleep on its node only, pinned until `suspend.keepFor`
(default `24h`) or disk pressure evicts it.

**Alternatives.**
- (a) Require `durability = "store"` for session suspend, as
  sandbox-scaling §6.2 does, so no idle session is ever pinned to a node.
- (b) Keep refusing suspend and make platforms promote.

**Trade-off.** A local-only idle session is lost with its node or on
eviction, and its checkpoint takes node disk that would otherwise be
cache.

**Why (simplicity, performance).** The motivating case is "come back to
the same sandbox while its node is still there", which needs no bucket.
A bucket is extra infrastructure and an upload per suspend; `store`
stays available for platforms that want node-loss survival, and the
`evicted` answer makes the loss explicit rather than silent.

## 4. Phase 1 group-commits the session log instead of fsyncing each start

**Call.** `sessions.db` flushes every 50 ms, and a restarted worker also
adopts live session processes the log missed.

**Alternative.** Fsync every start before acknowledging it.

**Trade-off.** A session started in the last 50 ms before a host crash
(not a worker restart) is not on disk; on a host crash its process is gone
anyway.

**Why (performance).** The start path is the feature's whole point
(about 2.5 ms p50 in sandbox-scaling §16.3); a per-start fsync would
roughly double it on ordinary disks for no durability gain.

## 5. The plan is written against PR #30's branch, not `main`

**Call.** Citations point at #30's head (`86e0d24`), and the plan lands
as a separate docs-only change.

**Alternative.** Wait for #30 to merge, or add the plan to #30.

**Trade-off.** Line numbers may drift as #30 is rebased.

**Why (simplicity).** The design question is live now, and #30 is being
rebased by another thread; folding a new plan into it would widen a large
PR under review.
