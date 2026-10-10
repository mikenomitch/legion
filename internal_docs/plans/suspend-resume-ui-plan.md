# Plan: Suspend, Resume, and Oversubscription in the UI

> **Judgement calls:** [`calls/suspend-resume-ui-plan_calls.md`](../calls/suspend-resume-ui-plan_calls.md)

Status: **SHIPPED** (written 2026-10-10). UI-1 (usage and pressure) and
UI-2 (snapshots) shipped on top of server Phases 1, 2, 3 and 7a of
[`suspend-resume-and-oversubscription-plan.md`](suspend-resume-and-oversubscription-plan.md).
UI-3 to UI-6 shipped in pass 2, together with every server change §7.3 asked
for (`ListNodeUsage`, `InstanceInfo.last_stop` and `instance_id` in
`node.pressure`, `GetNodeContent` and `SnapshotInfo.holders`,
`InstanceInfo.checkpoint_support`, plus `forked_from` and CPU `cores`).
Deliberately deferred: usage history charts (the manager keeps only the
latest sample), instance create (including `--from-snapshot`) from the UI
(calls entry 15), content store budgets on the locality card (they arrive
with the disk budget on GetNodeUsage), and a UI test runner. The Linux visual
check of populated usage and a live idle countdown is still open.

This plan covers the UI for all seven primitives of the parent plan, built
and unbuilt. The parent plan owns the semantics. This one owns how they look
and which page answers which question.

---

## 1. Problem statement

The server side now measures every governed allocation, packs nodes past
their physical memory, stops bursting work when a node runs short, and takes
and restores snapshots. None of it is visible in the UI:

- The worker page's "Resource usage" card sums **reservations** from the
  ledger and calls them usage. A node reserved at 90% and using 30% looks
  full. A node oversubscribed at `×1.5` looks overcommitted at 140%.
- Nothing shows whether a node is under memory pressure, what its response
  is configured to do, or which allocation it would stop first. The CLI has
  `legion node usage`. The UI has nothing.
- An app developer whose allocation was evicted for pressure sees it vanish
  from the allocation table and come back elsewhere. The `node.pressure`
  event that explains it, with a detail sentence and a hint, sits on the
  Events page as one line among hundreds, readable only by `operator:read`.
- `memoryMax` appears nowhere, so "reserved 512, ceiling 4096" is invisible.
- Snapshots exist only in the CLI. A platform builder cannot see what
  snapshots an instance has, how big they are, or whether one failed, and
  cannot roll an instance back without a terminal.

Today an operator answers "is this node really full?" with
`legion node usage w-03`, and an app developer answers "why did my task
restart?" with `legion events list --kind node.pressure --job api`. The UI
should answer both on the page they are already looking at.

## 2. Prior art

- **Kubernetes pod status and `kubectl top`.** A pod evicted for node
  pressure carries `reason: Evicted` and a message like "The node was low on
  resource: memory. Container api was using 1.9Gi, which exceeds its request
  of 400Mi." ([node-pressure eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/)).
  We take the shape: the stop reason sits on the workload's own page, with
  the numbers, not only in a cluster event log. We reject surfacing it only
  per pod, because Legion removes the allocation row and the job page is
  where the developer looks.
- **Lens and the Kubernetes Dashboard** draw requests, limits, and usage as
  one bar per resource. Copied: a reserved bar with used drawn over it, so
  "reserved high, used low" reads at a glance.
- **Nomad UI** shows per-allocation memory and CPU utilization against the
  reservation, and `memory_max` as a separate "Max" figure
  ([Nomad UI allocation detail](https://developer.hashicorp.com/nomad/tutorials/web-ui/web-ui-workload-info)).
  Copied: `res/max` as one column, matching `legion node usage`. Rejected:
  Nomad's time-series charts, because the manager keeps the latest sample
  only (parent plan §6.2) and a chart of one point is a lie.
- **Fly.io Machines dashboard** lists `suspended` as a machine state beside
  `stopped`, with Start resuming it. Copied for pass 2: suspended is a state
  badge and resume is the existing Start action, not a new page.
- **E2B dashboard** lists paused sandboxes with their pause time. Copied for
  pass 2: a checkpoint column with its age and size.

## 3. Jobs to be done, by persona

**Cluster operator**
- When a node looks full, I want to see how much of what is reserved is
  actually used, so I can tell a busy node from a padded one.
- When I have turned on oversubscription, I want to see the ratio next to
  the capacity, so a reservation above 100% of physical memory reads as
  intended, not broken.
- When a node is short of memory, I want to see that it is, and what it will
  stop first and in what order, so a stop never surprises me.
- When I scan the worker list, I want a glance at which nodes are tight or
  under pressure, so I know which one to open.

**App developer**
- When my task restarts and I did not do it, I want the page to tell me the
  machine ran short and stopped it, with the numbers, so I know whether to
  raise my reservation or move pools.
- When I set a burst ceiling, I want to see my reservation, my ceiling, and
  what I really use side by side, so I can size the reservation honestly.

**Agent platform builder**
- When my users' sandboxes are parked, I want to see which ones, since when,
  and how much their saved state takes, so I can tell a user the truth about
  their workspace.
- When an instance is idle, I want to see how long until it parks itself, so
  I know whether a user will notice.
- When I need to roll a workspace back, park it, wake it, or copy it, I want
  to do it from the instance's row, so I do not need a terminal for one
  click.
- When I look at a job's snapshots, I want their kind, state, size, node,
  and expiry in one table, so I can clean up the ones that cost disk.

**Security / platform admin**
- When someone can read a snapshot's memory, I want the buttons that create
  or restore one to follow the snapshot permission, not the deploy one, so
  the UI never suggests an action the manager will refuse.

**On-call responder**
- When a restore fails, I want the reason on the job page, so I can decide
  to restore an older snapshot or start cold.

*Not in scope:* the autonomous agent persona. Agents use the MCP tools and
`-j` CLI, which already cover every operation here.

### User scenarios

1. **Padded node.** An operator opens `w-03`. The Usage card shows memory
   `96 GiB ×1.5`, reserved 131 GiB (91%), used 61 GiB (64% of physical),
   pressure `ok`. They see the node is padded, not full, and leave the ratio
   alone.
2. **Evicted batch job.** A developer's `batch` job restarted overnight. Its
   job page has a "Memory pressure" card: `evict-bursting` on `w-03`, the
   detail sentence, and the hint "raise resources.memory toward real
   usage". They raise `memory` and redeploy.
3. **Rollback.** A platform builder opens the `workspaces` job, finds
   `user-42` in the Snapshots card, presses Restore on `before-refactor`,
   confirms, and watches the instance go `pending` with a "restoring from"
   badge, then back to `active`.
4. **Parked fleet (pass 2).** The instances table filters to `suspended`,
   shows each checkpoint's kind, size, and node, and a Resume button.

## 4. Goals and non-goals

**Goals**

- Every server surface from the parent plan has a UI home on an existing
  page: worker page, workers index, job page, task page, Events.
- Reservation, ceiling, and measured use always appear together, in MiB,
  matching `legion node usage`.
- Every mutating control is gated by the same ACL the RPC checks, via
  `utils/acl.ts`, and sits behind a two-step confirm when it stops,
  replaces, or deletes something.
- The UI degrades cleanly on an older manager (no `GetNodeUsage`), an
  older worker (no usage, no pressure status), and a macOS worker (no
  cgroups, so nothing measured).
- Components are shaped so pass 2 adds the `suspended` badge, checkpoint
  column, idle countdown, and suspend, resume, and fork actions without
  restructuring.

**Non-goals**

- Usage history or charts. The manager holds one sample per node in memory.
  History belongs in Prometheus (`docs/metrics.md`).
- A locality or content-store view in pass 1. No RPC reads the location
  index or a node's content store (§6.7).
- Creating instances, including `--from-snapshot`. Instance create stays
  CLI-only, as the [UI polish plan](ui-holistic-polish-plan.md) decided.
- Editing worker config (`oversubscribe`, `memoryPressure`) from the UI.
- A UI unit-test runner. The UI has none today. §9 says how pass 1 is
  tested instead.

## 5. Background: what exists today

- `ui/src/components/NodeDeepView.tsx` builds "Resource usage" from
  fingerprint capacity and ledger reservations (`buildResourceUsage`). It
  never calls `GetNodeUsage`.
- `ui/src/pages/details/JobDetail.tsx` renders the Instances table inline
  (id, mode, state, slot, allocation, storage, created) and reads
  `allocation.preempted` events into a "Preemption history" card. That card
  is the pattern for pressure history.
- `ui/src/pages/Events.tsx` streams events and shows raw metadata JSON on
  expand. Its category list has no `snapshot`.
- `ui/src/utils/acl.ts` mirrors the manager's ACL evaluation. Its
  `AclResource` union lacks `snapshot`.
- `ui/src/utils/jobspec.ts` parses tasks but not `resources` or `snapshots`.
- `ui/src/pages/TestingUtils.tsx` is an unlisted placeholder at `/testing`.
- Server surfaces available now (`proto/legion.proto`): `GetNodeUsage`
  (`GetNodeUsageReply`, `NodeResourceUsage`, `NodeAllocationUsage`,
  `AllocationUsage`, `MemoryPressureStatus`), `InstanceInfo.usage`,
  `InstanceInfo.restore_snapshot_id`, `CreateSnapshot`, `ListSnapshots`,
  `GetSnapshot`, `DeleteSnapshot`, `RestoreInstance`, and the events
  `node.pressure`, `snapshot.created|failed|deleted|restored|restore_failed`.

## 6. Proposed design

### 6.1 Worker page: the Usage card (operator)

Replaces "Resource usage" for memory and CPU whenever `GetNodeUsage`
answers. Custom resources (GPUs, device lists) keep the old reservation bars
underneath, because usage does not measure them.

```
USAGE                                          sampled 4s ago · response on
┌───────────────────────────────────────────────────────────────────────┐
│ Memory   96 GiB ×1.5 (144 GiB schedulable)          [ OK · free 35 GiB ] │
│ ████████████████████████████████████████████░░░░  reserved 131 GiB 91%  │
│ ███████████████████████▒                          used 61 GiB  42%      │
│ CPU      64000 MHz                                                     │
│ ██████████████████████████░░░░░░░░░░░░░░░░░░░░░░  reserved 41200 64%   │
│ ████████████                                      used 18900   30%      │
└───────────────────────────────────────────────────────────────────────┘
ALLOCATION   JOB        INSTANCE  RES/MAX MiB  USED  PEAK  CPU   IDLE
#1042        workspace  user-42   512/4096     1830  1900  0.25  -
#1077        api        -         2048         1900  2010  0.40  2m41s

UNDER PRESSURE, IN ORDER
1  #1063 batch      low     400/1900   [reclaim] → [evict-bursting]
2  #1042 workspace  normal  512/1830   [reclaim] → [evict-bursting]
```

- Percentages are against `capacity` for reserved (what the scheduler packs
  against) and against physical capacity (`capacity / ratio`) for used, so
  an oversubscribed node never shows used above 100% of a number that does
  not exist.
- Pressure status reads `ok`, `under pressure`, `response off`, or `status
  unknown` (older worker), with free memory and stall as detail, and the
  thresholds in the tooltip.
- Step chips use one tone per step: `reclaim` info, `suspend` accent,
  `evict-bursting` warn, `evict-reserved` danger. Names, never numbers.
- Empty states name the cause. `usage_reported = false` says which of the
  four reasons the proto lists could apply. A reply with zero allocations
  measured says "Nothing governed is running here."
- A "Memory pressure" history card below lists `node.pressure` events for
  the node (§6.3), hidden when there are none.

### 6.2 Workers index: compact indicator (operator)

One "Memory" column: a thin two-layer bar (reserved, used over it), the
used and reserved percentages as text, `×1.5` when oversubscribed, and a red
dot while under pressure. Blank for unreported nodes. The page fans out one
`GetNodeUsage` per visible row at the index cadence (5s, 20 rows by default).
A batch RPC would be better (§7.3).

### 6.3 Pressure stops (app developer)

`node.pressure` events become a first-class view in three places:

- **Job page, "Memory pressure" card**, beside "Preemption history", same
  shape: when, allocation, node, step chip, the detail sentence, and the
  hint called out on its own line. Hidden when the job has none. Reclaim
  rows say "nothing stopped".
- **Worker page**, the same card filtered by node.
- **Events page**, expanded rows of `node.pressure` show Detail and Hint as
  labeled lines above the raw JSON. `snapshot.*` rows show snapshot,
  instance, kind, size, and error the same way. `snapshot` joins the
  category filter.

Reading events needs `operator:read`. A job-scoped developer without it sees
nothing rather than an error (§7.3 proposes an instance-level stop record).

### 6.4 Instances and allocations (app developer, platform builder)

The Instances table moves into `components/InstancesTable.tsx` with a
column list, so pass 2 adds columns without touching the job page.

Pass 1 columns: Instance, Mode, State, Slot, Allocation, **Memory**
(`used / res·max MiB`), **CPU**, **Idle**, Storage, Created. A pending
instance with `restore_snapshot_id` gets a "restoring from snap-…" badge.
Used comes from `InstanceInfo.usage` (no `node:read` needed). Reservation
and ceiling come from `GetNodeUsage` when the caller has `node:read`, else
from the task's `resources` in the job spec.

The job page Allocations table gains the same Memory and Idle columns from
`GetNodeUsage` over the job's nodes. The task page Definition gains a
Resources row: `0.5 cpus · 400 MiB reserved · 2048 MiB max`.

Pass 2 adds, with no restructuring:

```
INSTANCE  STATE      MEMORY          IDLE          CHECKPOINT                 ACTIONS
user-42   active     1830 / 512·4096  -            -                          [Snapshot] [Suspend] [Fork]
user-17   active     402 / 512·4096   2m41s · parks in 19s                    [Snapshot] [Suspend] [Fork]
user-09   suspended  -                -            snap-77e0 · memory · 1.1 GiB · local w-03 · 4m ago   [Resume] [Resume cold]
```

- `suspended` gets its own tone in `utils/status.ts` (accent, distinct from
  `stopped`), and a `suspend_reason` tooltip (`manual | idle | pressure`).
- The idle countdown is `idle.after − idle_seconds`, shown only for tasks
  whose spec has `suspend.idle`.
- Resume is `StartInstance`, Resume cold is `StartInstance{cold: true}`,
  both two-step. Suspend takes the job default checkpoint kind, with a kind
  menu when the runtime supports both. Fork asks for a count (1-10) and
  shows the returned ids as links.

### 6.5 Snapshots card (platform builder, security admin)

On the job page, for jobs with stateful tasks or any snapshots. Polled
`ListSnapshots{job_id}` at the live cadence.

```
SNAPSHOTS                         [instance ▾ all] [kind ▾ disk] [name ____] [Take snapshot]
SNAPSHOT               INSTANCE  KIND    PURPOSE  STATE      NODE  SIZE     CREATED  EXPIRES
before-refactor        user-42   memory  manual   local      w-03  1.1 GiB  2h ago   in 5d   [Restore] [Delete]
snap-77e0aa41          user-42   disk    manual   failed     w-03  -        1d ago   -       [Delete]
  ↳ capture failed: volume driver local-dir: snapshot: no space left on device
```

- **Take snapshot** needs an instance, a kind, and an optional name. Memory
  is offered only when the task is a `sandbox` with a `snapshots` block and
  not `memory = false`. Otherwise it is disabled with the reason in the
  tooltip. A memory snapshot pauses the guest, so the button is a two-step
  confirm ("Pause and capture?"). Disk is one click.
- **Restore** is two-step ("Replace the running allocation?"), offered on
  `local` rows of keyed instances that are not stopped or terminated, and
  disabled with the reason otherwise.
- **Delete** is two-step, hidden on `capturing` and `deleting` rows.
- Clicking a row expands it: error, duration, allocation, and the manifest
  from `GetSnapshot`.
- Unknown `purpose` and `state` values render as plain badges, so pass 2's
  `suspend`, `fork`, and `published` need no code to appear. Pass 2 adds the
  `durability` column, the alias name, and a Warm action for published
  snapshots.

**ACL gating** (cosmetic, the manager decides):

| Control | Needs | When missing |
| --- | --- | --- |
| Card | `snapshot:read` on the job | Hidden |
| Take snapshot | `snapshot:write` on the job | Disabled, tooltip names the permission |
| Delete | `snapshot:write` on the job | Disabled |
| Restore | `snapshot:read` and `job:write` on the job | Disabled |
| Suspend (pass 2) | `job:write`, plus `snapshot:write` for a memory checkpoint | Disabled |
| Fork (pass 2) | `job:write` and `snapshot:write` | Disabled |
| Resume (pass 2) | `job:write` | Disabled |

### 6.6 Events

`snapshot` joins the category filter now. Pass 2 adds `instance` for
`instance.suspended|suspend_failed|resumed|restore_failed|forked` and gives
them the same structured summary.

### 6.7 Locality

No surface in pass 1. The location index lives in manager memory and no RPC
reads it, and the node content store has no read RPC either. Faking it from
the fingerprint would show the wrong thing. When §7.3's
`GetNodeContent` lands, the worker page gets a "Content store" card (bytes
by class, budget, published count), and a snapshot row shows which nodes
hold its parts.

### 6.8 Tolerance

| Situation | What the UI shows |
| --- | --- |
| Manager predates `GetNodeUsage` (`Unimplemented`) | The old reservation card, unchanged |
| Caller lacks `node:read` | No Usage card on job pages, spec-derived reservation in the instances table |
| `usage_reported = false` | Capacity and reservations, with "No usage reported" and the reasons |
| Worker predates the pressure response | "status unknown", no order list |
| macOS worker (enforcement off, no `/proc/meminfo`) | "No usage reported" |
| `pressure_rank = 0` everywhere | "Nothing would be stopped for memory on this node" |

## 7. Proto and server dependencies

### 7.1 Used by pass 1

`GetNodeUsage`, `InstanceInfo.usage`, `InstanceInfo.restore_snapshot_id`,
`ListSnapshots`, `GetSnapshot`, `CreateSnapshot`, `DeleteSnapshot`,
`RestoreInstance`, `ListEvents{kind: "node.pressure" | "snapshot"}`.

### 7.2 What pass 2 waits on (parent plan §7)

| UI piece | Waits on |
| --- | --- |
| `suspended` badge, filter | `InstanceInfo.state = "suspended"` (Phase 4) |
| Checkpoint column | `InstanceInfo.checkpoint_id`, `checkpoint_kind`, `checkpoint_durability`, `checkpoint_node_id`, `suspended_at`, `suspend_reason` (Phase 4) |
| Last restore | `InstanceInfo.restored_from_snapshot_id`, `last_restore_ms` (Phase 4) |
| Suspend action | `SuspendInstance` (Phase 4) |
| Resume, Resume cold | `StartInstanceRequest.cold` (Phase 4) |
| Idle countdown | `InstanceInfo.idle_since` (Phase 5), the task's `suspend.idle.after` in the spec |
| `suspend` step chip acting | `MemoryPressureStatus.steps` including a live `suspend` (Phase 5) |
| Fork | `ForkInstance` (Phase 6) |
| `published` state, durability, Warm | `SnapshotInfo.state = "published"`, `WarmSnapshot` (Phase 7b) |
| `instance` event category | `instance.*` events (Phase 4) |

### 7.3 Server changes the UI needs

All four shipped in pass 2, with the shapes the
[calls doc](../calls/suspend-resume-ui-plan_calls.md) records (entries 2, 3,
7, 8): `ListNodeUsage`, `InstanceInfo.last_stop`, `GetNodeContent`, and
`InstanceInfo.checkpoint_support` (per operation, not one flag).

- **Batch usage for the workers index.** A `ListNodeUsage` RPC (or a
  `usage_summary` on `GetNodes` rows) returning capacity, reserved, used,
  ratio, and `under_pressure` per node. Today the index fans out one
  `GetNodeUsage` per row.
- **An instance-level stop record.** `InstanceInfo.last_stop { cause,
  detail, hint, at }`, so an instance row can say "stopped by node
  pressure" after its allocation row is gone, without `operator:read` on
  events. Also `instance_id` in `node.pressure` metadata.
- **A content read RPC.** `GetNodeContent(node_id)` with bytes by class,
  budgets, and advertised digests, for §6.7.
- **Memory-snapshot eligibility.** A field on `InstanceInfo` (or a
  `checkpoint_support` on the task) so the UI does not re-derive "memory
  allowed" from the spec. Stored network policies that gate a task are
  invisible to the spec parse, so the UI can offer memory and have the
  manager refuse it.

## 8. Phases

Each UI phase ships with the server phase it needs, and stands alone.

- **UI-1: usage and pressure** (server Phases 1-2, shipped). Usage card,
  pressure order, workers-index indicator, usage columns, `memoryMax`,
  pressure history cards, structured event summaries, fixtures at
  `/testing`.
- **UI-2: snapshots** (server Phase 3, shipped). Snapshots card with create,
  restore, delete, row expansion, ACL gating, `snapshot` events category,
  restoring badge.
- **UI-3: suspended instances** (server Phase 4, shipped). Badge, filter,
  checkpoint column, Suspend, Resume, Resume cold, `instance` events, last
  suspend and restore durations, `last_stop` on the row.
- **UI-4: idle and pressure suspend** (server Phase 5, shipped). Idle
  countdown, `suspend_reason`, a live `suspend` chip.
- **UI-5: fork** (server Phase 6, shipped). Fork action with count and
  spread, fork lineage, snapshot names shown and settable, `purpose = fork`
  rows. `create --from-snapshot` stays in the CLI (calls entry 15).
- **UI-6: locality and durability** (server Phase 7b plus §7.3's content
  RPC, shipped). Durability column, publish on create, Warm, snapshot
  holders, and the worker page's "Content and locality" card.

## 9. Testing plan

- `npm run typecheck` and `npm run build` on every phase (CI's `ui-build`).
- **Fixtures.** `/testing` (unlisted) renders the Usage card, the pressure
  history, and the snapshots table from fixtures in
  `ui/src/fixtures/suspendResume.ts`: an unreported node, a node with
  enforcement on and nothing running, an older worker without pressure
  status, and a populated oversubscribed node under pressure. Pass 2 adds
  suspended and forked fixtures beside them.
- **Live run.** An isolated manager and worker from the branch binary on
  non-default ports, the Vite dev server proxied to it, a job submitted,
  and each page fetched with console and network errors checked. On macOS
  that exercises the unreported path and the snapshot error paths for real.
- Linux with cgroups is where populated usage comes from. The parent plan's
  [Linux verification](../suspend-resume-linux-verification.md) covers the
  server. The UI part is a visual check of `/nodes/<id>` against
  `legion node usage <id>`.

## 10. Rollout and backward compatibility

No flags. Every new card hides or degrades when its RPC is missing or
denied (§6.8). A UI built from this branch against an older manager shows
exactly the old pages. Snapshot actions change the cluster, so they follow
the existing rule: the manager enforces, the UI only hides and disables.

## 11. Open questions

- **Should the workers index fan out at all before §7.3's batch RPC?**
  Lean yes: 20 cheap in-memory reads every 5s is fine at the default page
  size. Revisit if a 100-row page shows up in a profile.
- **Where does a suspended-fleet view live?** Lean: a `suspended` filter
  chip on the job's Instances table, not a new page, until a job has
  thousands of instances.

## 12. Acceptance criteria

- **UI-1:** on a Linux worker with enforcement on, the worker page's Usage
  card matches `legion node usage <id> -j` field for field, the order list
  matches its "UNDER PRESSURE, IN ORDER" rows, and a pressure eviction shows
  its detail and hint on the job page within one poll. On macOS the card
  says "No usage reported" and nothing errors.
- **UI-2:** take a disk snapshot from the job page, watch it go `capturing`
  to `local` or `failed`, restore it, and delete it. Each button is disabled
  for a token without the permission in §6.5.
- **UI-3:** suspend from the row, see `suspended` with the checkpoint, and
  resume. Resume cold discards the checkpoint.
- **UI-4:** an idle instance's countdown reaches zero within one poll of the
  manager suspending it.
- **UI-5:** Fork with count 3 links the three new instances.
- **UI-6:** a published snapshot shows its durability and which nodes hold
  it.
