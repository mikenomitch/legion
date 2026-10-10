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
seen. Instance create and volume delete stay CLI-only.

```
before-refactor  user-42  memory  manual  LOCAL  w-03  1.1 GiB   [Restore] [Delete]
                                         click Restore → [Replace the running allocation?]
```

## 2. Pressure stops are told from events, not from an instance or allocation field

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
users to read. The server change is listed in plan §7.3 so a later pass can
put the cause on the row too.

## 3. Used memory is a percent of physical capacity, reserved a percent of schedulable

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

## 4. The workers index fans out one GetNodeUsage per row

**The call.** The memory column on the workers index calls `GetNodeUsage`
once per visible row at the 5-second index cadence.

**Alternatives.** Leave the column out until a batch RPC exists, or add a
`ListNodeUsage` RPC now.

**Trade-off.** 20 RPCs every 5 seconds at the default page size, more on a
100-row page. Each is an in-memory read on the manager.

**Why.** The operator's "which node is tight?" is answered at a glance
without opening every node, and this pass does not touch the server (the
brief keeps Rust and proto with the parallel Phase 4 work). The batch RPC is
plan §7.3's first server change, and the column will switch to it without a
UI change in shape.

## 5. Fixtures render on the unlisted /testing page instead of a test runner

**The call.** The empty, older-worker, unreported, and populated usage
shapes, the pressure history, and every snapshot state render from fixtures
on `/testing`, lazy-loaded so they do not ship in the main bundle.

**Alternatives.** Add Vitest with `@solidjs/testing-library` and assert on
rendered output, or a Storybook.

**Trade-off.** Nothing fails automatically when a fixture renders wrong. It
takes a human (or an agent with a browser) to look.

**Why.** The UI has no test runner, and adding one is a dependency decision
the project has not made. The `/testing` route already existed as an empty
placeholder. A fixture page also serves the next pass, which adds suspended
and forked shapes beside these. Typecheck and build still gate CI.

## 6. Memory-snapshot eligibility is derived from the job spec

**The call.** The Take snapshot form offers `memory` only when the task is a
`sandbox` with a `snapshots` block, `memory` not false, and no gated
networking in the spec. Otherwise the option is disabled with the reason.

**Alternatives.** Always offer both kinds and let the manager's
`FailedPrecondition` explain, or wait for a server field
(`checkpoint_support`).

**Trade-off.** A stored network policy can gate a task the spec does not, so
the UI can offer memory and the manager refuses. The refusal shows in the
card's outcome note with the manager's hint.

**Why.** For nearly every task the spec decides it, and a disabled option
with "Memory snapshots need runtime sandbox" teaches more than an error
after the click. The server field is in plan §7.3.

## 7. Snapshot rows keep their objects across polls

**The call.** `ListSnapshots` results are merged into the previous poll's
objects (`SnapshotInfo.equals`), so unchanged rows are not remounted.

**Alternatives.** Use `<Index>` (the existing rule for rows that carry
inputs), or poll less often.

**Trade-off.** A few lines of merging in the fetcher.

**Why.** Found in the live run: the 2-second poll returned fresh objects,
the keyed `<For>` remounted each row, and an armed Delete disarmed before
the second click. `<Index>` does not help here, because each slot still
receives a new object and the actions cell re-renders. Stable objects fix it
at the source. `InstancesTable` carries a comment so pass 2 does the same
before adding Suspend and Resume buttons.

## 8. No locality view in this pass

**The call.** Content locality has no UI surface yet.

**Alternatives.** Show artifact digests from the fingerprint or the jobspec
as "probably cached here".

**Trade-off.** Operators cannot see what a node's content store holds from
the UI.

**Why.** The location index lives in manager memory and no RPC reads it, and
nothing reads a node's content store either. A guess presented as a fact is
worse than nothing on a page operators trust. `GetNodeContent` is in plan
§7.3.
