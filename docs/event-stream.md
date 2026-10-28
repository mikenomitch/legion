# Event Stream

The manager emits a **scheduler event** whenever the scheduler does something
observable. Events are journaled in the state store and sent out live, so you
can read the same stream as history (with a cursor) or as a real-time tail.
It is available from the CLI, the MCP server, the UI, and raw gRPC.

## What gets emitted

| Kind | Level | When |
| --- | --- | --- |
| `scheduler.started` | info | Eval loop started (manager startup). |
| `scheduler.recovered` | warn | Stale in-flight evals were requeued at startup (a store migrated from a version that wrote claims; claims are leader memory now, so a current manager never has any). |
| `scheduler.concurrency_adjusted` | info | The adaptive concurrency controller moved the number of planners it admits. Emitted on the change, never per tick (docs/adaptive-scheduler-concurrency.md). |
| `scheduler.concurrency_saturated` | warn | Backing off did not improve goodput, so the cluster's plan conflicts are capacity-driven rather than concurrency-driven. Add nodes; waiting will not help. |
| `eval.enqueued` | info | An eval was added to the queue. |
| `eval.completed` | info | An eval handler finished successfully. |
| `eval.failed` | error | An eval handler returned an error. |
| `eval.requeued` | warn | An eval used up its plan-conflict retries and was released back to pending with a delay instead of failing. See [Late-binding plan apply](late-binding-plan-apply.md). |
| `eval.blocked` | warn | An eval's plan found no room, so the eval waits blocked instead of failing. Metadata carries the blocking `key` (`class:pool=...`, `node:<id>`, `escaped`) and the `detail` (task, cpu, memory, message, and for a [gang](gang-scheduling.md) short of its quorum a `gang` report: desired, min, running, placeable, eligible nodes). See [Blocked evals](scheduler-and-eval-loop.md#blocked-evals). |
| `eval.unblocked` | info | A blocked eval is back in the pending queue. Metadata carries the `key` and the `cause`: `register`, `node`, `ready`, `freed` (capacity on the node the event names), `missed` (capacity appeared while the eval was still running, so it never blocked), `startup`, or `timeout` (a [gang](gang-scheduling.md#placement-timeout) past its `placementTimeout`, woken for its last attempt). |
| `allocation.placed` | info | A plan was applied. One event per apply, with placement details in metadata, including `candidateRank` per placement (0 = first choice) and a `fallbacks` count. |
| `allocation.removed` | info/warn | Allocations were removed (job stop/update: info, node loss: warn). Metadata carries `cause`: `stop`, `update`, `drain`, `reschedule`, `destroy`, `node_lost`, `scale_in`, `preempted`, `node_pressure`, `restore`, `suspended`, `gang`, or `failed`. It is omitted when no cause was recorded. See [Graceful Stop](graceful-stop.md). |
| `allocation.stopping` | info | Allocations entered their grace window. Metadata carries `cause`, `maxGraceSeconds`, and per-allocation `graceSeconds`. The window is the manager's expectation. The worker's clock starts when it delivers the signal. See [Graceful Stop](graceful-stop.md). |
| `allocation.preempted` | warn | Allocations were displaced to make room for higher-band work. Metadata carries the victim's `band` plus `preemptorJob` and `preemptorBand`, which link the victim to the preemptor. One event per victim job. See [Preemption and Eviction](preemption-and-eviction.md). |
| `allocation.preempt_planned` | info | `preemption.mode = "observe"` only: the victim selection ran and committed nothing. Metadata lists the `victims` enforce mode would have taken. |
| `allocation.failed` | warn | A worker reported an allocation failure (includes the reason). |
| `allocation.reschedule_deferred` | warn | A failed allocation's replacement waits out the task's reschedule backoff. Nothing places it before `nextAttemptAt` unless an operator lifts the backoff (`legion job run <spec> --force-reschedule`, or `legion instance start <id>` for a keyed instance). Metadata carries `task`, `group`, `instanceId` (null for a stateless task), `attempt` (the consecutive failure count, from 1; the backoff doubles per earlier one), `delaySecs`, `nextAttemptAt` (RFC 3339), `reason` (one line, at most 400 characters; `allocation.failed` has the full text), `jobConfigId`, and `taskConfigId`. Also emitted for an `evict-bursting` eviction, which spends the same budget. See [Manager-side move logic](jobs-tasks-and-scheduling.md#manager-side-move-logic). |
| `allocation.reschedule_exhausted` | error | A failed allocation found its task's move budget spent (`move_count` moves inside `move_period`), so the task is not moved again and the job is marked failed. A `job.failed` event follows. Metadata carries `task`, `instanceId`, `moves`, `moveCount`, `movePeriodSecs`, `reason`, `jobConfigId`, and `taskConfigId`. |
| `allocation.terminated` | info/warn | A task's process exited and the allocation reached a terminal state without being rescheduled (info when the exit was a success, warn otherwise). Metadata carries `success` and the exit message. See [Task Supervision](task-supervision.md). |
| `allocation.plan_rejected` | warn | Optimistic concurrency rejected a placement plan (it will be retried). |
| `artifact.staging` | info | An allocation began staging its [artifacts](artifacts.md) (artifact list in metadata). |
| `artifact.staged` | info | Every artifact for an allocation staged successfully (digests, bytes, durations in metadata). |
| `artifact.failed` | warn | One artifact's fetch failed. Carries per-artifact detail (redacted source, error, attempts) alongside the allocation-level `allocation.failed`. |
| `artifact.pinned` | info | Mutable artifact references were pinned to digests at submit (one event per submit that changes pins, with the pins in metadata). |
| `net.denied` | warn | A node's datapath denied a connection or request by [network policy](network-handlers.md). Metadata carries the direction, destination, deciding rule (`policy/<name>#<index>`, `task#<index>`, or `fallthrough`), reason, peer identity, the task's `network.labels`, and how many identical denials the row stands for. |
| `net.policy_applied` | info | A network policy object was created or replaced (`legion net policy apply`). Metadata names it and counts the running allocations it affects. |
| `net.policy_deleted` | info | A network policy object was deleted. |
| `net.plugin_applied` | info | A network plugin object was created or replaced (`legion net plugin apply`). Metadata names it, its revision, and the module checksum. See [Network Plugins](network-plugins.md). |
| `net.plugin_deleted` | info | A network plugin object was deleted. |
| `app.installed` | info | An [app](apps.md) was installed (`legion app install`), or a bundled app seeded by the first Control leader (`by legion:bootstrap`). Metadata carries `app`, `version`, `source`, and `by`. App events are scoped to the app's job (`legion-system/<app>`). |
| `app.upgraded` | info | An app's manifest was replaced: by `legion app upgrade`, or, for a bundled app, by the manifest of the build its nodes moved to. Metadata carries `from`, `to`, `permissionsAdded`, `permissionsRemoved`, and `by`. |
| `app.uninstalled` | info | An app was uninstalled: its job stopped and its grants revoked (`revokedGrants`). |
| `app.enabled` / `app.disabled` | info | An app was enabled or disabled (`by`). |
| `app.config_changed` | info | An app's config was replaced. `codeFields` lists the code fields the change touched, for which the caller's grant was checked. |
| `app.reconciled` | info | The app reconciler changed an app's job. `action` says what: `created the job`, `config changed`, `settings changed (…)`, `upgraded 0.2.0 → 0.3.0`, `reverted a direct edit by <principal>`, `started the job, which was stopped`, `stopped (app disabled)`. Metadata also carries the render's `digest`. |
| `app.reconcile_failed` | warn | The reconciler could not bring an app's job in line with its record. `refused: true` is a submit the manager refused (validation, grant, quota), which waits for the next change to the app; otherwise it retries with backoff, and the event repeats at most every 10 minutes. |
| `volume.mounted` | info | A worker materialized a [volume](volumes.md) at a host path. For a node-local volume, this is also when its instance becomes pinned to that node. |
| `volume.failed` | warn | A driver operation failed (mount/unmount/destroy). The detail is also recorded on the volume row, so `legion volume get` explains a stuck volume. |
| `volume.reclaiming` | info | A volume's instance no longer references it, so its storage is queued for the driver's `destroy`. |
| `volume.destroyed` | info | A driver confirmed a volume's storage is gone. |
| `snapshot.created` | info | A node finished capturing a [snapshot](suspend-and-resume.md), which is now `local`. Metadata carries the snapshot ID, instance, kind, size, and capture time. |
| `snapshot.failed` | warn | A node could not capture a snapshot. The allocation kept running. Metadata carries the `error`. |
| `snapshot.deleted` | info | A snapshot is gone: deleted, expired by `snapshots.ttl` or `snapshots.keep`, or removed with its job. |
| `snapshot.restored` | info | An instance's replacement allocation started from a snapshot (`legion snapshot restore`). |
| `snapshot.restore_failed` | warn | An allocation failed to start from a snapshot. Metadata carries the node's `error`, for example `restore needs TAP device tap7: ...`. |
| `snapshot.published` | info | A `durability = "store"` snapshot finished uploading to its node's content store and is now `published` ([durability](suspend-and-resume.md#durability-surviving-the-node)). Metadata carries `uploadedBytes` and `publishMs`. |
| `snapshot.publish_failed` | warn | A `durability = "store"` snapshot could not be uploaded. It stays `local` and keeps working from its node, and the manager retries twice (after one minute, then five). Metadata carries the `error`, which says whether a retry is coming. |
| `snapshot.warmed` | info | A node fetched a published snapshot's parts after `legion snapshot warm`. Node-scoped. Metadata carries `fetchedBytes`. |
| `snapshot.warm_failed` | warn | A node could not warm a snapshot. Node-scoped. Metadata carries the `error`. |
| `instance.suspended` | info | A [suspend](suspend-and-resume.md) landed: the instance is `suspended` and its allocation is gone (or, with `suspend.reserve`, kept only as its reservation). Metadata carries the checkpoint's `snapshotId`, `kind`, `totalBytes`, `durationMs`, `reserved`, and `reason`: `manual` (SuspendInstance), `idle` (the task's [idle policy](suspend-and-resume.md#idle-policy)), `pressure` (the node's `suspend` pressure step), or `make_room` (suspended to make room for a resume on its node, see [Capacity for resumes](suspend-and-resume.md#capacity-for-resumes)). With a [held share](suspend-and-resume.md#holding-a-share-of-the-reservation), `reserved` is true. |
| `instance.archived` | info | A suspended instance's published checkpoint was [archived](suspend-and-resume.md#archiving-idle-instances) to the bucket (`suspend.archive`), in the entry that recorded the publish. Metadata carries `instanceId`, `snapshotId`, `manifestUrl`, and `manifestOnly` (true when the instance row was dropped too). |
| `instance.make_room` | info | A resume did not fit on its checkpoint's node, and the manager started suspends of idle suspendable instances there (manager `suspend.makeRoom`). Metadata carries the resume's `instanceId` and the instance ids it is `suspending`. |
| `instance.suspend_failed` | warn | A suspend's capture failed on the node. The instance keeps running. Metadata carries the `error`. |
| `instance.resumed` | info | A suspended instance restored from its checkpoint as a new allocation. Metadata carries `snapshotId` and `restoreMs`. |
| `instance.restore_failed` | warn | A resume, a [fork](suspend-and-resume.md#fork), or a create from a snapshot could not restore. Metadata carries `error` and `outcome`: `cold` (booted fresh on its storage), `suspended` (a resume under `onRestoreFailure = "fail"`), or `stopped` (a fork or new instance under `"fail"`). |
| `instance.forked` | info | A [fork](suspend-and-resume.md#fork)'s checkpoint landed and its forks are being placed on the origin's node (for a `--spread` fork, the checkpoint was published and each fork goes to its own node). Metadata carries `origin`, `instances`, `pauseMs`, and the checkpoint's `snapshotId`, `kind`, and `totalBytes`. |
| `instance.fork_failed` | warn | A fork's capture failed, or a spread fork's publish failed for good. The origin keeps running and the forks in `instances` were terminated. Metadata carries the `error`. |
| `instance.promoted` | info | A running [session](session-mode.md) was promoted to a keyed instance (PromoteInstance). Its node adopts the session in place as the new allocation. Metadata carries `instanceId` and `promotedFrom` (the session id); the event names the job, node, and allocation. Per-session starts and stops are not events (see [session mode](session-mode.md#limitations)). |
| `node.registered` | info | A worker registered (or re-registered). |
| `node.ready` | info | A known node rejoined and was marked ready. |
| `node.lost` | warn | A node was marked lost (explicitly or through missed heartbeats). |
| `node.cordoned` | info | A node's availability was set to `drained`. No new placements land on it, and existing allocations are untouched. |
| `node.uncordoned` | info | A node's availability was restored to `available`, so it re-entered the scheduling pool. |
| `node.pressure` | info/warn | A node's memory pressure response acted on one allocation: `reclaim` (info, nothing stopped), `suspend` (warn, the instance was checkpointed and is `suspended`, `cause = suspended`, nothing replaces it), `evict-bursting` or `evict-reserved` (warn, the allocation was stopped with `cause = node_pressure` and a replacement queued). The message names the step and the numbers. Metadata carries `step`, `freed_bytes`, `used_bytes`, `reserved_bytes`, `available_bytes`, `min_free_bytes`, `stall_percent`, `max_stall_percent`, `detail`, `hint`, `cause` for every step but `reclaim`, `snapshot_id` for `suspend`, and `instance_id` when the allocation fulfills a stateful instance (the same detail and hint also land on that instance's `last_stop`, see [Inspecting an instance](suspend-and-resume.md#inspecting-an-instance)). Counted by `legion_node_pressure_actions_total{node,step}`. See [Memory pressure](resource-enforcement.md#memory-pressure). |
| `node.drained` | info | A drain pass evicted a drained node's allocations for replacement elsewhere (evicted and held counts in metadata, where held means stateful allocations protected by `onLost`). |
| `job.submitted` / `job.updated` | info | A job config was created / updated. |
| `job.stopped` / `job.destroyed` | info | A job was stopped / destroyed. |
| `job.gang_member_lost` | info/warn | A [gang](gang-scheduling.md#when-a-member-is-lost) member was lost (it failed, its node was lost or drained, or memory pressure evicted it), and what that did to the gang. Info when the member is replaced alone; warn for a restart or a failed job. Metadata: `task`, `allocationId`, `cause`, `action` (`replace`, `restart`, `fail`), `running` (members still running after the loss), `min` (the quorum), `stopped` (allocations the action stopped). |
| `job.gang_timeout` | error | A gang did not reach its quorum within its [`placementTimeout`](gang-scheduling.md#placement-timeout), so its job was failed and any members still running were stopped. Metadata: `task`, `placementTimeoutSeconds`, `waitedSeconds`, `stopped`. |
| `job.failed` | error | A job was marked failed (for example, move limit exceeded, which `allocation.reschedule_exhausted` explains). |
| `job.kv_render_refused` | warn | A watched KV change re-rendered a job's `templateKv` template into lookups of keys the job did not name when it was submitted, so the job kept its last render ([KV store](kv-store.md)). Metadata: `keys`, the keys it would have read. Resubmitting the job checks them against the submitter. |
| `scale.applied` | info | A count override was applied ([Autoscaling](autoscaling.md)). Previous and new counts, `source` (`operator`, `autoscaler`, `wake`), and the reason (for the autoscaler, its decision's summary) are in metadata. |
| `scale.deferred` | info | A scale-in was recorded but deferred until the active rollout completes. |
| `scale.reset` | info | An override was dropped (an explicit `--reset`, or a resubmit that changed the spec count and reclaimed ownership). |
| `scale.pinned` | info | A person pinned an autoscaled task's count, floor, or ceiling for a while (`legion job scale --for`), or released the pin (`untilMs` null). Metadata: `min`, `max`, `untilMs`, `by`, `reason`. |
| `scale.paused` / `scale.resumed` | info | Autoscaling was paused or resumed for a task, a job, or the cluster (`legion scaling pause|resume`). Metadata: `scope` (`task`, `job`, `all`), `target`, `untilMs`, `by`, `reason`. |
| `scale.limited` | info | The autoscaler started holding a task's count at a bound or guard (`at max 8 (raise autoscale.max)`). Once per episode, not per evaluation. |
| `scale.flapping` | warn | The autoscaler's flap guard engaged: three direction changes in ten minutes; scale-in is held for twice `down.stabilize`. |
| `scale.check_failed` | warn | An autoscaling check lost its data (`check cpu has no data (usage is missing)`). Once per episode. |
| `rollout.event` | passthrough | Mirror of every rollout event (created, advanced, completed, failed, …). |
| `alloc.provisional_started` | info | A node started work before the placement was durable ([Edge Boot](edge-boot.md)). Metadata carries the claim ID, gate, origin, and, for an edge start, the ticket `jti`. It never carries the ticket itself. |
| `alloc.ratified` | info | A provisional allocation became durable. Allocation ID and ratification latency are in metadata. |
| `alloc.refused` | warn | A ratification was refused and the node is killing the work. The reason code (`instance_claimed`, `no_capacity`, …) is in metadata. |
| `boot_ticket.issued` | info | A boot ticket was minted. Metadata carries the `jti`, scope, and the minting accessor ID. This is the audit record for who authorized a start. |
| `keyring.rotated` | info | A new KV data key became active ([KV Encryption at Rest](kv-encryption.md)). The old one is retired and rewrapped away in the background. Metadata carries the key ID and reason (`operator request`, the rotation age, or the encryption-count guard). It never carries key material. |
| `keyring.rewrap_completed` | info | Every value is under the active data key (or, with `plaintext: true`, is a plaintext row). Counts and the retired keys collected are in metadata. |
| `keyring.root_key_rewrapped` | info | Every data key is wrapped under the current root key. Its ID is in metadata. |
| `keyring.locked` / `keyring.unlocked` | warn / info | This manager lost or regained the ability to unwrap a data key (for example, a missing root key or a provider outage). `locked` carries the key IDs and reasons. |
| `keyring.reset` | warn | Break-glass reset. The deleted KV key paths and dropped data keys are in metadata. |

Each event carries:

- `id`: increasing, but sparse (see
  [cursors](#cursor-based-polling-cli-and-mcp)).
- `kind` and `level` (`info`/`warn`/`error`).
- Optional scope IDs: `job_id` (canonical: `namespace/name`, bare in
  `default`), `node_id`, `eval_id`, `allocation_id`, `rollout_id`.
- `namespace`: the job's [namespace](namespaces.md) for an event about a job;
  empty for cluster events (nodes, raft, the scheduler). Namespace writes emit
  `namespace.applied` and `namespace.deleted`.
- A human-readable `message`.
- `metadata_json`: a JSON object with kind-specific detail.
- `created_at`: an RFC3339 timestamp.

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

`-n/--namespace` keeps only events about those namespaces' jobs (names or
globs, comma-separated: `legion events follow -n 'payments-*'`). Without it
you see every event you may see, cluster events included; `--job` resolves
in `-n` or `$LEGION_NAMESPACE` like any job argument.

**Who may read events.** `operator:read` sees every event. A caller without
it but with `job:read` in some namespaces reads those namespaces' events
through the same list and stream, filtered per item; cluster events need
`operator:read`. A stream re-checks the caller's grants on its interval, so a
revoked grant ends it.

`--kind` matches exactly or by category prefix. For example, `--kind eval`
matches `eval.enqueued`, `eval.completed`, `eval.failed`, `eval.requeued`,
`eval.blocked`, and `eval.unblocked`.

### Events about one instance or one allocation

```bash
legion events list --instance box-KPI6u4fi8GmwSWCo   # everything about a sandbox
legion events list --allocation 98                   # one allocation's history
legion events follow --instance moose
```

`--allocation` matches an event about that allocation: its `allocation_id`
field, or, for the batched events that cover several allocations and leave
that field unset, the ids listed in their metadata. These are
`placements[].allocationId` on `allocation.placed`,
`allocations[].allocationId` on `allocation.stopping` and
`allocation.preempted`, and `victims[].allocationId` on
`allocation.preempt_planned`. A batched event lists at most 20 allocations,
so the 21st allocation of one batch doesn't match. `allocation.removed` names
no ids and never matches.

`--instance` matches an event that is *about* the stateful instance:

- its metadata names it as `instanceId` (suspend, resume, restore, fork,
  snapshot, volume, promotion, provisional-start, and reschedule backoff
  events) or `instance_id`
  (`node.pressure`);
- it lists it in `instances` (the forks of `instance.forked` and
  `instance.fork_failed`) or `suspending` (`instance.make_room`);
- or it is about an allocation that fulfilled the instance, matched the way
  `--allocation` matches (`allocation.placed`, `allocation.failed`,
  `allocation.stopping`, and the rest).

The allocation half is resolved against the allocation rows the manager still
has. Once an old allocation's row is gone, its events still exist but only
match the instance through their metadata. `follow --instance` checks each
live event as it arrives, so it picks up an allocation placed after the
stream opened.

Both filters combine with the others. They are a full scan of the bounded
journal, which is fine at its size.

### Cursor-based polling (CLI and MCP)

The state store assigns event IDs and never reuses them, so `after_id` works as
a cursor. It means "give me everything above this ID". It is never an index or
a count.

1. Run `legion events list --json` and remember `latestId`.
2. Poll `legion events list --after-id <latestId> --json`. Update the cursor
   from the last event received (or the reply's `latestId`).

**IDs are sparse.** They increase, but they do not run 1, 2, 3. The journal is
pruned, so old IDs disappear. Each raft write lane also mints IDs in its own
stride, so two lanes can never mint the same ID (see
["How it works"](#how-it-works)). Don't treat a gap as a lost event, derive a
count from `lastId - firstId`, or make up an ID by adding 1. Carry the cursor
you were given.

The `legion_events` MCP tool takes the same arguments (`after_id`, `limit`,
`kind`, `job_id`, `node_id`, `level`, `instance_id`, `allocation_id`). It is the recommended way for MCP agents
to tail the stream incrementally.

### UI

The **Events** page (Cluster → Events) tails `StreamEvents` over gRPC-Web. It
reconnects and resumes automatically, and it offers client-side filtering by
category, level, and text, pause and resume, and expandable per-event metadata.

The instance page (`/jobs/<namespace>/<job>/instances/<id>`) and the allocation page
(`/allocations/<id>`) each show a timeline of just their own events, using the
`instance_id` and `allocation_id` filters. Like the Events page, they need
`operator:read`. Without it they say so and show the rest of the page.

### gRPC

- `Manager.ListEvents(ListEventsRequest) -> ListEventsReply`: a filtered window,
  with `latest_id` for cursor initialization.
- `Manager.StreamEvents(StreamEventsRequest) -> stream SchedulerEvent`: a
  server-streaming live tail. With `after_id` set, the journal is replayed from
  that cursor before the live feed begins, so reconnecting clients never miss
  events. It works over both native gRPC and gRPC-Web.

See `proto/legion.proto` for the authoritative message documentation.

## How it works

- Code emits each event through one named method per event kind on the
  recorder facade, such as `store.events().job_stopped(name)`. This keeps
  scheduler code readable. The `EventRecorder` in
  [`src/storage/events.rs`](../src/storage/events.rs) is the authoritative
  catalog of kinds. It owns each event's message, level, scopes, and metadata
  shape.
- Under the hood, `Store::emit_event` writes the event to the
  `scheduler_event` table and publishes it on an in-process
  `tokio::sync::broadcast` channel
  ([`src/storage/store.rs`](../src/storage/store.rs)).
- Emission is **best-effort**. Scheduling work never fails because an event
  could not be recorded. Errors are logged and swallowed.
- Every manager can emit. Events raised while handling a worker's heartbeat
  (`node.pressure`, `net.denied`) come from whichever
  manager that worker dials. A manager that leads no raft lane, such as a
  follower in the default `single` topology, forwards the event to the
  status-lane leader, so it lands in the shared journal like any other.
- When a `StreamEvents` subscriber falls behind the broadcast buffer, Legion
  backfills it from the journal using its last-sent event ID. Slow consumers
  fall back to catch-up reads instead of dropping events.
- **Event IDs are minted rather than counted.** An ID holds a timestamp (unix
  milliseconds) in its high bits, a per-process sequence in the middle, and
  the raft write lane that minted it in the low 3 bits. Every ID satisfies
  `id % 8 == lane_index`, so the lanes' ID spaces never overlap. This lets any
  lane insert into the shared journal concurrently without two lanes minting
  the same ID. (A collision would silently drop one node's event on some
  replicas and not others.) IDs still sort roughly in time order and still
  increase per lane, which is all the cursor contract needs. In `single`
  topology (and in `local_disk` mode), every ID comes from one lane and the
  low bits are constant.
- Under `topology = "lanes"`, the live tail is ordered per lane rather than
  globally. Two lanes commit independently, so a subscriber can occasionally
  receive a slightly newer event from one lane before a slightly older one
  from another. The stream only emits increasing IDs, so it skips that older
  event live. The event is still in the journal, and any cursor read
  (`--after-id`, a reconnect, `ListEvents`) returns it. The journal is the
  complete record, and the live tail is not.
- **ID ordering is per manager process, not per wall clock.** The timestamp
  bits come from the minting leader's clock, and the tie-breaking high-water
  mark lives in process memory. After a failover to a manager whose clock is
  behind the old leader's, new IDs can sort BELOW an existing `after_id`
  cursor until the new leader's clock passes the old one's. A poller holding
  a high cursor will miss that window's events (they are in the journal,
  below the cursor). If your consumer cares about gaps, re-read a cursor range
  after you see a leadership change (`legion raft history`), or treat the
  journal, queried by time, as the source of truth. Run managers under NTP.
  The window is as long as the clock skew.
- The journal is bounded. It keeps the newest ~10,000 events and prunes older
  rows during inserts. The stream is for observability and is not an audit
  log. Durable history belongs in the state store itself (jobs, rollouts,
  rollout events).
- Events have no foreign keys, so they can outlive the objects they describe.
  For example, a `job.destroyed` event survives the deletion of the job row.

## Performance

Event emission sits on the scheduler's busiest write paths (eval enqueue, plan
apply, eval completion). It is built to add as little serialized work as
possible:

- **In-transaction staging.** Hot paths insert the event row inside the same
  write transaction as the state change it describes (`stage_event_locked` +
  `publish_staged`). Emission costs no extra lock acquisition and no extra WAL
  commit, and the event commits or rolls back atomically with the change.
  Cold paths (node and job lifecycle, the rollout bridge) use the one-line
  `store.events()` recorder, which takes the lock itself.
- **Index-free journal.** `scheduler_event` has no secondary indexes and no
  `AUTOINCREMENT`, so each insert is a single b-tree append. The ID is
  computed in memory (no `SELECT MAX(id)` round trip) and appended as the
  rowid PK. Cursor reads use that PK. Filtered reads scan the ≤10k-row table
  in about a millisecond, which is fine for human- or agent-paced calls.
- **Ordered publication.** Events are broadcast while the write guard is still
  held. `StreamEvents` subscribers see strictly increasing IDs, and the lag
  and backfill dedupe logic stays correct (per lane under `lanes` topology, as
  described above).

On `eval_critical_path_bench`, emission costs about 0–5%. That benchmark is
the worst case: tight multi-threaded enqueue→claim→apply→complete loops with
no other work, compared in the same binary with the toggle below. The cost is
at or below the noise floor at 4 and 8 workers. Emission shows no detectable
change in `scheduler_bench` placement throughput.

`LEGION_EVENTS_OFF=1` turns off persistence and streaming entirely. It is an
operator escape hatch and is also what the A/B benchmarks use. With it set,
`ListEvents` serves only rows that already exist, and `StreamEvents` goes
quiet.
