# Judgement calls: session starts from a per-node snapshot

> Plan: [`plans/session-snapshot-start-plan.md`](../plans/session-snapshot-start-plan.md).
> Entries are ranked by project impact, most impactful first. The number is
> the rank. The plan is not built; these are the calls its design makes.

## 1. The template is a guest of its own, never a session's

**Call.** A node boots a dedicated template guest with no session identity
and snapshots that, rather than snapshotting the first real session once it
is ready.

**Alternatives.** (a) Snapshot the first session of the task on the node.
(b) Let the operator name a snapshot to start from (`from_snapshot` for
sessions).

**Trade-off.** One extra boot per task per node, on a guest nobody uses,
and a template that holds only what the task's own command did before it
was ready.

**Why (security, then simplicity).** (a) would put one session's env,
token, files, and memory into every session restored after it, which the
plan's security job forbids. (b) asks every author to make and maintain a
snapshot per node, and a manual snapshot can carry a session's data just as
(a) does.

## 2. Opt-in per task, falling back to a boot

**Call.** `stateful.start = "snapshot"` is opt-in, default `boot`, and every
reason a snapshot cannot be used (missing, building, incompatible, failed
restore) boots instead and says why.

**Alternatives.** (a) On by default for sandbox sessions. (b) Fail the start
when the snapshot cannot be used.

**Trade-off.** Authors must know to ask for it, and a broken template shows
up as slower starts, not errors.

**Why (simplicity).** A restored guest wakes mid-instruction: open
connections, timers, and anything the workload derived from the clock or
the RNG before the snapshot carry over, which not every workload tolerates
(Lambda's `afterRestore` hooks exist for this). That makes it the author's
choice. (b) turns an optimization into an availability risk.

## 3. Templates are per node, not published

**Call.** Each node builds and keeps its own template as derived content.

**Alternatives.** Publish one template per task to the cluster's content
store and fetch it on other nodes.

**Trade-off.** Every node pays one template boot and capture per task.

**Why (simplicity).** A per-node template restores only on the CPU, kernel,
and VMM that took it, which is already true of the node that built it; a
published one needs the compatibility checks, chunked fetch, and pins that
published checkpoints have, for a saving of one boot per node.
