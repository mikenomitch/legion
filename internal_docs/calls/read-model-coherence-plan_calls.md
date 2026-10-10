# Judgement Calls: Read Model Coherence

Calls made building [`read-model-coherence-plan.md`](../plans/read-model-coherence-plan.md).
Ordered by project impact, highest first. The number is the rank.

## 1. `local_disk` mutations update the read model by changeset projection, not by fixing each hook

`Store::replicated` in `local_disk` mode now attaches a SQLite session to the
body, lets it commit, and projects the read model from the changeset, the same
function raft replicas use. The ~30 hand-placed `cache_*` calls defer under the
new `in_local_projection` flag exactly as they already did under `in_capture`.

**Alternatives.** Fix the four drift classes the shadow check found, one hook
at a time, and rely on the shadow check to catch the next one. Or drive the
cache from `sqlite3_update_hook`, which is cheaper than a session.

**Trade-off.** `local_disk` mutations now serialize end to end on the global
capture mutex instead of per transaction. Bodies are SQL only (planning runs
before the op), so the serialized section barely grows, but a slow body now
blocks other writers for its whole length rather than per statement. The
session adds per-row bookkeeping on the five read-model tables.

**Why.** Simplicity, and the evidence. Two of the four drift classes (the
cache running *ahead* of SQLite between the halves of `insert_node` and
`mark_nodes_as_lost`) are not missing hooks. They are hooks writing values a
later op will commit, and per-hook fixes would have to reason about every
composite. Projection from what was committed cannot run ahead by
construction, and it makes both modes one code path, so a raft-only bug and a
`local_disk`-only bug can no longer hide from each other. `update_hook` was
rejected because it skips truncate-optimized `DELETE`s and `REPLACE` conflict
deletes, and does not report the cascaded rows `destroy_job` depends on in a
form we already parse.

## 2. Only the read model follows the changeset in `local_disk`. Events and eval wakes keep their direct paths

`project_read_model_from_changeset` runs the read-model half of
`project_changeset_side_effects` and stops. Event broadcast, journal pruning,
keyring reloads, and eval wake-ups still fire from the method bodies in
`local_disk` mode.

**Alternative.** Full parity with raft: defer every side effect and project
all of them, which would remove a second path for events too.

**Trade-off.** Two code paths remain for events and wake-ups.

**Why.** Blast radius. The bug class was read-model drift. Moving events would
change delivery timing and ordering for every `local_disk` user (events would
publish after the whole op instead of after each transaction) without a
measured problem to fix. Revisit if an event-delivery bug shows the same
shape.

## 3. The per-method `cache_*` calls stay in this change, as no-ops under the flag

**Alternative.** Delete them now, since nothing reaches their bodies from
inside a mutation any more.

**Trade-off.** About 30 call sites look like they do something and do not.
The comment above `cache_node` explains it, but a reader can still be misled.

**Why.** Reversibility. If Phase 4 has to be backed out, the hooks are the
fallback, and the deletion is a large mechanical diff in a 27,000-line file
that would bury the semantic change in review. The deletion is tracked in
`TODO.md` for after a release has run with the shadow check on.

## 4. Outside writes are detected and recovered from, not prevented or supported

`check_external_writes` compares `PRAGMA data_version` on the write connection
every 5 seconds and rebuilds the read model when another connection committed.

**Alternatives.** Prevent them with `PRAGMA locking_mode = EXCLUSIVE`. Or
support them with triggers that write a dirty-key table any writer would feed.

**Trade-off.** Up to 5 seconds of stale cache after an outside write, and a
full O(fleet) rebuild each time. A `TRUNCATE` or `RESTART` checkpoint from
another process also bumps `data_version` and costs one spurious rebuild
(verified in `outside_commits_rebuild_the_read_model`; the manager's own
`PASSIVE` checkpoints and the WAL restarts after them do not).

**Why.** Exclusive locking would also lock out our own read pool, which uses
separate connections. Triggers put an extra row write on the hottest path
(every allocation insert) and, in raft, into every changeset. Outside writes
are an emergency tool, so the cheap guard that limits the damage is the right
size.

## 5. The timed scrub repairs optimistically in raft and under the capture mutex in `local_disk`

`scrub_read_model_node` holds the node's cached `Arc`s from before its read
and publishes only if they are still the ones in the model. In `local_disk` it
also holds the capture mutex, so nothing can move.

**Alternative.** Take the capture mutex in raft too.

**Trade-off.** In raft, a node that changes faster than one scrub visit (a hot
node under heavy placement) is skipped every sweep. Those nodes are also the
ones the delta path's own per-changeset scrub and contradiction fallback
already cover.

**Why.** Performance. The capture mutex is the raft write path's serialization
point, and stalling it for background reads on every replica costs the
critical path for a repair that should never be needed. Raft projection runs
outside that mutex anyway, so holding it would not close the race.

## 6. The shadow check panics, always on in unit tests, opt-in elsewhere, and skips raft

**Alternatives.** Log and count instead of panicking. Enable it in production
behind a sampling rate. Cover raft replicas too.

**Trade-off.** Integration tests only get it with
`LEGION_READ_MODEL_SHADOW_CHECK=1`. Raft drift is caught only by the scrub and
delta counters.

**Why.** The check reloads the whole model after every write, which is fine
for test fixtures and ruinous at fleet scale. A panic names the op in the test
that broke it. Raft lanes apply concurrently and project outside the capture
mutex, so there is no quiet point after an op to compare at without a per-lane
barrier, which is deferred.

## 7. Projection drops a job config's task-config entry when it has no rows, and accepts the read-through race

`cache_set_task_configs_for_job_config` removes the key for an empty list
instead of caching it. That fixes the raft half of the `destroy_job` leak (an
empty entry per destroyed config, forever).

**Alternative.** Tombstone destroyed configs, or version the task-config cache
like the node map.

**Trade-off.** `cached_task_configs_for_job_config` reads through on a miss and
caches what it read. A reader that read just before a destroy committed can
still re-insert the destroyed list after the projection removed it. Rare,
bounded to one entry, and not repaired by the scrub (which skips task
configs). Tracked in `TODO.md`.

**Why.** Task configs are written once per job config and deleted once, so the
race needs a destroy to land inside one read. The fix is a cache-wide epoch
check, which is more machinery than the leak it closes.
