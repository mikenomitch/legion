# Event Stream

The manager emits a **scheduler event** whenever the scheduler does something
observable. Events are journaled in the state store and fanned out live, so the
same stream is available as history (with a cursor) and as a real-time tail —
from the CLI, the MCP server, the UI, and raw gRPC.

## What gets emitted

| Kind | Level | When |
| --- | --- | --- |
| `scheduler.started` | info | Eval loop started (manager startup). |
| `scheduler.recovered` | warn | Stale in-flight evals were requeued at startup. |
| `eval.enqueued` | info | An eval was added to the queue. |
| `eval.completed` | info | An eval handler finished successfully. |
| `eval.failed` | error | An eval handler returned an error. |
| `allocation.placed` | info | A plan was applied; one event per apply, with placement details in metadata. |
| `allocation.removed` | info/warn | Allocations were removed (job stop/update: info; node loss: warn). |
| `allocation.failed` | warn | A worker reported an allocation failure (includes the reason). |
| `allocation.plan_rejected` | warn | A placement plan was rejected by optimistic concurrency (will be retried). |
| `artifact.staging` | info | An allocation began staging its [artifacts](artifacts.md) (artifact list in metadata). |
| `artifact.staged` | info | Every artifact for an allocation staged successfully (digests, bytes, durations in metadata). |
| `artifact.failed` | warn | One artifact's fetch failed — per-artifact detail (redacted source, error, attempts) alongside the allocation-level `allocation.failed`. |
| `artifact.pinned` | info | Mutable artifact references were pinned to digests at submit (one event per changing submit; pins in metadata). |
| `volume.mounted` | info | A worker materialized a [volume](volumes.md) at a host path. For a node-local volume this is also when its instance becomes pinned to that node. |
| `volume.failed` | warn | A driver operation failed (mount/unmount/destroy). The detail is also recorded on the volume row, so `legion volume get` explains a stuck volume. |
| `volume.reclaiming` | info | A volume's instance no longer references it, so its storage is queued for the driver's `destroy`. |
| `volume.destroyed` | info | A driver confirmed a volume's storage is gone. |
| `node.registered` | info | A worker registered (or re-registered). |
| `node.ready` | info | A known node rejoined and was marked ready. |
| `node.lost` | warn | A node was marked lost (explicitly or via missed heartbeats). |
| `node.cordoned` | info | A node's availability was set to `drained`: no new placements land on it (existing allocations untouched). |
| `node.uncordoned` | info | A node's availability was restored to `available`: it re-entered the scheduling pool. |
| `node.drained` | info | A drain pass evicted a drained node's allocations for replacement elsewhere (evicted/held counts in metadata; held = stateful allocations protected by `onLost`). |
| `job.submitted` / `job.updated` | info | A job config was created / updated. |
| `job.stopped` / `job.destroyed` | info | A job was stopped / destroyed. |
| `job.failed` | error | A job was marked failed (e.g. move limit exceeded). |
| `rollout.event` | passthrough | Mirror of every rollout event (created, advanced, completed, failed, …). |

Each event carries: an increasing (but sparse — see
[cursors](#cursor-based-polling-cli-and-mcp)) `id`, `kind`, `level`
(`info`/`warn`/`error`), optional scope IDs (`job_id`, `node_id`, `eval_id`,
`allocation_id`, `rollout_id`), a human-readable `message`, a JSON
`metadata_json` object with kind-specific detail, and an RFC3339 `created_at`.

## Consuming the stream

### CLI

```bash
# Recent history (oldest first):
legion events list
legion events list --kind eval --job demo --level warn --limit 50 --json

# Live tail (replays from --after-id first when given):
legion events follow
legion events follow --kind rollout --json   # NDJSON: one envelope per line
```

`--kind` matches exactly or by category prefix: `--kind eval` matches
`eval.enqueued`, `eval.completed`, and `eval.failed`.

### Cursor-based polling (CLI and MCP)

Event IDs are assigned by the state store and never reused, so `after_id` is a
cursor — "give me everything above this ID", never an index or a count:

1. `legion events list --json` → remember `latestId`.
2. Poll `legion events list --after-id <latestId> --json`; update the cursor
   from the last event received (or the reply's `latestId`).

**IDs are sparse, and always were.** They increase, but they do not run
1, 2, 3: the journal is pruned (so old IDs vanish), and each raft write lane
mints IDs in its own stride so two lanes can never mint the same ID (see
["How it works"](#how-it-works)). Never treat a gap as a lost event, derive a
count from `lastId - firstId`, or synthesize an ID by adding 1 — carry the
cursor you were given.

The `legion_events` MCP tool exposes the same arguments (`after_id`, `limit`,
`kind`, `job_id`, `node_id`, `level`), which is the recommended way for MCP
agents to tail the stream incrementally.

### UI

The **Events** page (Cluster → Events) tails `StreamEvents` over gRPC-Web with
automatic reconnect-and-resume, plus client-side category/level/text filtering,
pause/resume, and expandable per-event metadata.

### gRPC

- `Manager.ListEvents(ListEventsRequest) -> ListEventsReply` — filtered window
  with `latest_id` for cursor initialization.
- `Manager.StreamEvents(StreamEventsRequest) -> stream SchedulerEvent` —
  server-streaming live tail. With `after_id` set, the journal is replayed from
  that cursor before the live feed begins, so reconnecting clients never miss
  events. Works over both native gRPC and gRPC-Web.

See `proto/legion.proto` for the authoritative message documentation.

## How it works

- Emission sites call one named method per event kind on the recorder facade —
  `store.events().job_stopped(name)` — so scheduler code stays legible. The
  `EventRecorder` in [`src/storage/events.rs`](../src/storage/events.rs) is the
  authoritative catalog of kinds and owns each event's message, level, scopes,
  and metadata shape.
- Under the hood, `Store::emit_event` writes the event to the
  `scheduler_event` table and publishes it on an in-process
  `tokio::sync::broadcast` channel
  ([`src/storage/store.rs`](../src/storage/store.rs)).
- Emission is **best-effort by design**: scheduling work never fails because an
  event could not be recorded (errors are logged and swallowed).
- `StreamEvents` subscribers that lag behind the broadcast buffer are
  transparently backfilled from the journal using their last-sent event ID, so
  slow consumers degrade to catch-up reads instead of dropping events.
- **Event IDs are minted, not counted.** An ID is a timestamp (unix
  milliseconds) in its high bits, a per-process sequence in the middle, and
  the raft write lane that minted it in the low 3 bits — every ID satisfies
  `id % 8 == lane_index`. The lanes' ID spaces are therefore disjoint, which
  is what lets any lane insert into the shared journal concurrently without
  two lanes ever minting the same ID (a collision would silently drop one
  node's event on some replicas and not others). IDs still sort
  ~chronologically and still increase per lane, which is all the cursor
  contract needs. In `single` topology (and in `local_disk` mode) every ID
  comes from one lane and the low bits are constant.
- Under `topology: lanes` the live tail is ordered per lane, not globally:
  two lanes commit independently, so a subscriber can occasionally receive a
  slightly newer lane's event before a slightly older one from another lane.
  The stream only ever emits increasing IDs, so that older event is skipped
  live — it is in the journal, and any cursor read (`--after-id`, a
  reconnect, `ListEvents`) returns it. The journal, not the live tail, is the
  complete record.
- The journal is bounded: the newest ~10,000 events are retained and older rows
  are pruned opportunistically during inserts. The stream is an observability
  surface, not an audit log — durable history belongs to the state store
  proper (jobs, rollouts, rollout events).
- Events deliberately have no foreign keys so they can outlive the objects they
  describe (a `job.destroyed` event survives the job row's deletion).

## Performance

Event emission sits on the scheduler's hottest write paths (eval enqueue,
plan apply, eval completion), so it is engineered to add as little serialized
work as possible:

- **In-transaction staging.** Hot paths insert the event row inside the same
  write transaction as the state change it describes (`stage_event_locked` +
  `publish_staged`), so emission costs no extra lock acquisition and no extra
  WAL commit — and the event commits or rolls back atomically with the change.
  Cold paths (node/job lifecycle, the rollout bridge) use the one-line
  `store.events()` recorder, which takes the lock itself.
- **Index-free journal.** `scheduler_event` has no secondary indexes and no
  `AUTOINCREMENT`, keeping the insert to a single b-tree append. The ID is
  computed in memory (no `SELECT MAX(id)` round trip) and appended as the
  rowid PK; cursor reads use that PK, and filtered reads scan the ≤10k-row
  table in about a millisecond, which is fine for their human/agent-paced
  call rate.
- **Ordered publication.** Events are broadcast while the write guard is still
  held, so `StreamEvents` subscribers observe strictly increasing IDs and the
  lag/backfill dedupe logic stays correct (per lane under `lanes` topology —
  see above).

Measured on `eval_critical_path_bench` (the worst case: tight multi-threaded
enqueue→claim→apply→complete loops with no other work), A/B'd in the same
binary via the toggle below: emission costs ~0–5% on that path — at or below
the noise floor at 4 and 8 workers — and shows no detectable change on
`scheduler_bench` placement throughput. Earlier designs that re-acquired the
write lock per event and maintained three secondary indexes cost 25–30% on
the same benchmark, which is why the constraints above exist.

`LEGION_EVENTS_OFF=1` disables persistence and streaming entirely (an
operator escape hatch, and the lever the A/B benchmarks use). `ListEvents`
then serves only pre-existing rows and `StreamEvents` goes quiet.
