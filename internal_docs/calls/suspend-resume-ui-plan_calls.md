# Judgement calls: Suspend, Resume, and Oversubscription in the UI

Plan: [`plans/suspend-resume-ui-plan.md`](../plans/suspend-resume-ui-plan.md).
Entries are ranked by project impact, highest first. The number is the rank.

## 1. Snapshot create, restore, and delete are UI actions, not CLI-only

**The call.** The job page's Snapshots card takes, restores, and deletes
snapshots, each gated by the snapshot ACL, and every action that stops,
replaces, pauses, or deletes something is a two-step confirm.

**Alternatives.** Keep snapshots read-only in the UI, as instance
create/destroy and volume delete are (the
[UI polish plan](../plans/ui-holistic-polish-plan.md) made both CLI-only,
arguing a two-click confirm is too weak a gate for destroying data).

**Trade-off.** A delete is irreversible and a restore replaces a running
allocation, behind the same two-click confirm the polish plan judged too weak
for volumes. A misclick on a snapshot row costs a capture or a rollback.

**Why.** Rolling a workspace back is the platform builder's job to be done
(plan §3), and the reason a snapshot exists. A snapshot is a copy, not the
data itself: deleting one never touches the live volume, and a restore keeps
the instance's id and storage and can be redone from another snapshot. That
is the line the polish plan drew, and snapshots fall on the reversible side
of it. Simplicity won over caution: the action lives where the snapshot is
seen. Instance create and volume delete stay CLI-only. Pass 2 applies the
same rule to Suspend, Resume, Resume cold, and Fork on instance rows: each
keeps the instance's id and storage, so each is reversible.

```
before-refactor  user-42  memory  manual  LOCAL  w-03  1.1 GiB   [Restore] [Delete]
                                         click Restore → [Replace the running allocation?]
```

## 2. Checkpoint eligibility is computed by the manager, per operation

**The call.** `InstanceInfo.checkpoint_support` carries one answer per
operation (memory and disk snapshot, memory suspend and the default suspend
kind, memory and disk fork, spread, publish), each with the exact
FailedPrecondition message the RPC would return. The manager computes it by
calling the same precondition functions CreateSnapshot, SuspendInstance, and
ForkInstance call. The UI enables controls from it and shows the blocker as
the tooltip.

**Alternatives.** One boolean, `memory_checkpoint_allowed`, as plan §7.3
sketched. Or keep deriving eligibility from the job spec (entry 13).

**Trade-off.** Sixteen fields instead of one, and a node, allocation, and
task-config lookup per active instance on every ListInstances (cached reads,
and only for `active` rows). Workers and VMMs change under it, so the answer
is as fresh as the last poll, not a promise.

**Why.** One flag is wrong as soon as the operations disagree, and they do:
a memory suspend needs no volume `snapshot` op while a memory snapshot does,
a disk fork of a TAP-wired sandbox is refused while a disk snapshot is not,
and spread needs a content store. A single flag would offer a fork the
manager refuses. Running the RPCs' own predicates means the UI and the
manager cannot drift, and the stored network policy the spec cannot see is
accounted for. The CLI and agents get the same field in `instance get -j`.

```
[memory (unavailable) ▾]  title: instance user-42 cannot take a memory snapshot:
                          runtime "exec" supports disk snapshots only
                          Hint: use --disk, or run this task with runtime = "sandbox"
```

## 3. `last_stop` is recorded for every stop cause, in the removal transaction

**The call.** Whenever the manager removes an instance's allocation with a
known stop cause, the same transaction writes `instance.last_stop` (cause,
a sentence, hint, time, allocation, node). The memory pressure path passes
the worker's own detail and hint, so a pressure eviction reads exactly like
its `node.pressure` event. The instance row shows platform causes
(`node_pressure`, `preempted`, `node_lost`, `drain`, `reschedule`) with the
hint underneath.

**Alternatives.** Record only platform causes. Derive it at read time from
the stop-notice table (short retention, no instance id) or the event journal
(bounded, needs `operator:read`). Put it on the allocation (deleted on
eviction).

**Trade-off.** One more `UPDATE` per removed stateful allocation, a schema
migration (new columns `last_stop` and `forked_from`), and a wider forwarded
`RemoveAllocationById` write (an optional `explanation`, defaulted on
deserialize). A rollout after an eviction replaces the record with
`update`, so the row says only what happened last.

**Why.** "Why did my task restart?" belongs on the row the developer is
already reading, with no extra permission (plan §3, app developer). Writing
it in the removal transaction means it can never describe a stop that did
not happen. Recording every cause keeps the field honest ("last stop") and
lets the UI decide what is worth calling out.

```
user-17  ACTIVE  [stopped by node pressure] 3h ago
                 raise resources.memory toward real usage
```

## 4. Pressure stops are told from events, not from an instance or allocation field

**The call.** "Why was my allocation stopped?" is answered by a Memory
pressure card on the job page that reads `node.pressure` events, with the
detail and hint on their own lines.

**Alternatives.** Show the cause on the allocation or instance row. The
manager deletes the allocation row on eviction, and `InstanceInfo` has no
last-stop field, so this needs a server change (`InstanceInfo.last_stop`).

**Trade-off.** Events need `operator:read`, which a job-scoped developer
often lacks, and the journal is bounded, so old evictions age out. The card
says "best-effort".

**Why.** It ships today with no server change, mirrors the existing
Preemption history card (one pattern for "the platform stopped your work"),
and the event already carries the detail sentence and hint the plan wanted
users to read.

**Update (pass 2).** Entry 3 adds the server field, and stateful instances
now explain themselves on their row. The card stays for history (several
stops, not just the last) and for stateless allocations, which have no
instance row. `node.pressure` metadata now names `instance_id`.

## 5. Used memory is a percent of physical capacity, reserved a percent of schedulable

**The call.** On an oversubscribed node, the reserved bar is against
`capacity` (physical × ratio, what the scheduler packs against) and the used
bar against physical capacity (`capacity / ratio`).

**Alternatives.** Both against `capacity` (one base, simpler to explain), or
both against physical (reserved then runs past 100%).

**Trade-off.** Two bars on one card have different denominators. A reader
who compares the two percentages directly gets the wrong idea.

**Why.** Each percentage then answers its own question. Reserved: how full
is this node for the scheduler? Used: how close is it to running out of
real memory, which is what triggers the pressure response. Against one
base, a 1.5× node shows used memory topping out at 67%, which reads as
headroom the node does not have. The capacity label shows both numbers
(`96 GiB ×1.5`) and the tooltip names the packing capacity.

## 6. Instance rows are keyed by id, and cells read the row through an accessor

**The call.** `InstancesTable` keys its rows by instance id, and every cell
receives an accessor to its row. Unchanged instances also keep their objects
across polls (`stableList`), so a cell re-renders only when its own row
changed, and the actions cell is created once per row and never remounted.

**Alternatives.** Merge objects across polls and key on object identity, as
the Snapshots card does (entry 14), or use `<Index>`.

**Trade-off.** Columns are a slightly odder shape (`cell: (row) =>
row().instance…`), and a cell that builds a component from `row()` rebuilds
it when that row changes.

**Why.** Instance rows change far more often than snapshot rows (usage
samples every 10 seconds, idle time, pending operations), so identity
merging alone would still remount a row, and disarm its Suspend, whenever
that row's numbers moved. Keying by id keeps the confirm armed through any
poll for as long as the instance exists. The live run checked it: Resume
armed, one 2-second poll later still armed, then confirmed.

## 7. ListNodeUsage returns GetNodeUsage replies, without allocations by default

**The call.** The batch RPC answers with one `GetNodeUsageReply` per node,
built by the same function as GetNodeUsage, with `allocations` cleared
unless `include_allocations` is set. Empty `node_ids` means every node, up
to 500. The workers index makes one call per refresh and falls back to the
old fan-out on a manager without it.

**Alternatives.** A new `NodeUsageSummary` message (only capacity, reserved,
used, ratio, and `under_pressure`), or a usage summary on `GetNodes` rows.

**Trade-off.** Each entry carries fields a fleet view does not need (per-node
PSI, the pressure thresholds). A reply message reused inside another reply
is unusual in this proto.

**Why.** One shape means the index and the worker page can never disagree,
and any field later added to GetNodeUsage (the disk budget the parallel
resume-guards work adds) reaches the batch view with no proto change. Usage
on `GetNodes` would make every node listing pay for an allocation scan even
when nobody looks at memory. `legion node usage` with no node and the
`legion_node_usage_list` MCP tool use the same call.

## 8. GetNodeContent reads only what the manager already holds

**The call.** GetNodeContent answers from the manager's in-memory location
index and its snapshot table: the node's advertised digests with the
snapshot and part role each belongs to, holder counts, the node's publish
and checkpoint capabilities, and its snapshots with parts held. It does not
ask the worker anything, so the content store's per-class budgets are not
in the reply. The worker card leaves a budget slot that the disk-budget
fields on GetNodeUsage (from the parallel resume-guards work) can fill.

**Alternatives.** Proxy to the worker for its content store's real state
(budgets, every entry, pins), as `GetTaskLogs` proxies.

**Trade-off.** Only entries of 1 MiB and up appear, a manager that just
started shows nothing until the next full report, and budgets are not
shown yet.

**Why.** The question the plan asked (§6.7) is locality: why did placement
prefer this node, and is it warm for this snapshot. The index is exactly
what the scheduler scores, so it answers that honestly. A worker proxy is a
new worker RPC and a second source for budgets that the usage report is
about to carry anyway.

## 9. A `done` job with live work reads `running`, derived in the UI

**The call.** The job header shows `running` when the stored status is
`done` and any allocation is not complete or failed, or any instance is
active, pending, or rescheduling. The tooltip says the manager records the
job as `done`.

**Alternatives.** Change the manager to store `running` (every job becomes
`done` at submit today, keyed or not), or keep showing `done` with the
existing tooltip.

**Trade-off.** The Jobs index still lists `done`, so the index and the
header use different words for the same job.

**Why.** The bug the screenshots found is pre-existing and not keyed-only:
`done` is the manager's word for "accepted", and the job page put it next to
ACTIVE instances and PLACED allocations. Changing the stored status touches
the status filter, `status_counts`, every client, and the failed/stopped
transitions, which is its own change. Deriving it on the page fixes the
reading where it was wrong, with no server risk. The index wording is a
TODO.

**Superseded** by [job-status-plan.md](../plans/job-status-plan.md): the
manager now derives the status for every surface, and the page's client-side
derivation is gone.

## 10. The workers index fans out one GetNodeUsage per row

**Superseded by entry 7** (ListNodeUsage). The fan-out remains only as the
fallback for a manager without the batch RPC.

**The call.** The memory column on the workers index calls `GetNodeUsage`
once per visible row at the 5-second index cadence.

**Alternatives.** Leave the column out until a batch RPC exists, or add a
`ListNodeUsage` RPC now.

**Trade-off.** 20 RPCs every 5 seconds at the default page size, more on a
100-row page. Each is an in-memory read on the manager.

**Why.** The operator's "which node is tight?" is answered at a glance
without opening every node, and pass 1 did not touch the server.

## 11. Fixtures render on the unlisted /testing page instead of a test runner

**The call.** The empty, older-worker, unreported, and populated usage
shapes, the pressure history, every snapshot state, and pass 2's instance,
fork, durability, and content shapes render from fixtures on `/testing`,
lazy-loaded so they do not ship in the main bundle.

**Alternatives.** Add Vitest with `@solidjs/testing-library` and assert on
rendered output, or a Storybook.

**Trade-off.** Nothing fails automatically when a fixture renders wrong. It
takes a human (or an agent with a browser) to look.

**Why.** The UI has no test runner, and adding one is a dependency decision
the project has not made. The `/testing` route already existed as an empty
placeholder. Typecheck and build still gate CI, and the server half of every
new field is covered by Rust tests.

## 12. Fork lineage is a stored column, not derived from the fork checkpoint

**The call.** ForkInstance writes `forked_from` (the origin's id) on each
fork's instance row, and `InstanceInfo.forked_from` serves it. The table
shows "fork of user-42" under the id, and the card shows a
`origin → forks` lineage line.

**Alternatives.** Derive lineage from the `purpose = fork` snapshot (its
`instance_id` is the origin) and each fork's restore source.

**Trade-off.** One more column on `instance`.

**Why.** The fork checkpoint is reclaimed once every fork has restored, so a
derived lineage disappears minutes after the fork. Lineage is the platform
builder's "where did this sandbox come from", which outlives the checkpoint.

## 13. Memory-snapshot eligibility is derived from the job spec

**Superseded by entry 2.** The spec rule stays only as the fallback for a
manager that does not send `checkpoint_support`.

**The call.** The Take snapshot form offers `memory` only when the task is a
`sandbox` with a `snapshots` block, `memory` not false, and no gated
networking in the spec. Otherwise the option is disabled with the reason.

**Alternatives.** Always offer both kinds and let the manager's
`FailedPrecondition` explain, or wait for a server field.

**Trade-off.** A stored network policy can gate a task the spec does not, so
the UI can offer memory and the manager refuses.

**Why.** For nearly every task the spec decides it, and a disabled option
with "Memory snapshots need runtime sandbox" teaches more than an error
after the click.

## 14. Snapshot rows keep their objects across polls

**The call.** `ListSnapshots` results are merged into the previous poll's
objects (`SnapshotInfo.equals`, now the shared `stableList` helper), so
unchanged rows are not remounted.

**Alternatives.** Use `<Index>` (the existing rule for rows that carry
inputs), or poll less often.

**Trade-off.** A few lines of merging in the fetcher.

**Why.** Found in the live run: the 2-second poll returned fresh objects,
the keyed `<For>` remounted each row, and an armed Delete disarmed before
the second click. `<Index>` does not help here, because each slot still
receives a new object and the actions cell re-renders. Snapshot rows change
rarely, so identity merging is enough here (instance rows needed more, see
entry 6).

## 15. Creating instances from a snapshot stays in the CLI

**The call.** The UI shows snapshot names (and sets and removes them) but
has no `create --from-snapshot` control. The Instances card's footer points
at `legion instance create --from-snapshot <name>`.

**Alternatives.** Add an instance-create form to the job page, with a
snapshot picker.

**Trade-off.** A platform builder who names a golden snapshot in the UI
still needs a terminal to start instances from it.

**Why.** The UI has no instance-creation surface at all, and the
[polish plan](../plans/ui-holistic-polish-plan.md) kept instance create and
destroy CLI-only on purpose. Adding create only for the snapshot case would
make the one entry point the least common one.

## 16. A disk suspend checkpoint's size reads "in place", an empty capture "0 B"

**The call.** The size column shows `in place` for a `purpose = suspend`
disk checkpoint and `0 B` for a captured snapshot of an empty volume,
instead of `-`.

**Alternatives.** Report the volume's size at suspend time as the
checkpoint's bytes.

**Trade-off.** The column mixes a word and numbers.

**Why.** The screenshots' "–" was not missing data: a manual disk snapshot
reports its volume copy's bytes (2.9 MiB in the live run), and a disk
suspend copies nothing because the volume stays where it is
(`checkpoint_bytes` is 0 by design). Reporting the volume's size would claim
a copy that does not exist. Saying "in place" tells the reader why.

## 17. No locality view in this pass

**Superseded by entry 8** (GetNodeContent and the worker page's "Content
and locality" card, plus `SnapshotInfo.holders` in the snapshot detail).

**The call.** Content locality has no UI surface yet.

**Alternatives.** Show artifact digests from the fingerprint or the jobspec
as "probably cached here".

**Trade-off.** Operators cannot see what a node's content store holds from
the UI.

**Why.** No RPC read the location index or a node's content store, and a
guess presented as a fact is worse than nothing on a page operators trust.
