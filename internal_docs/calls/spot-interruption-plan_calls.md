# Judgement Calls: Spot Interruption Plan

Decisions made for
[`plans/spot-interruption-plan.md`](../plans/spot-interruption-plan.md)
while designing and building it (2026-11-12). **Entries are ranked by project
impact, highest first — the number is the rank.**

## 1. A notice moves gang members only; it does not drain the node

**Call.** A pending interruption keeps gangs off the node and moves its gang
members at once (each gang's `onMemberLoss`, stop cause `interruption`).
Other work stays until the node goes.

```console
$ legion node interrupt gpu-07 --in 2m --detail "spot reclaim"
```

**Alternatives.** Drain the node (AWS Node Termination Handler's default).

**Trade-off.** A plain service on the node is replaced only after the node is
lost, minutes later than a drain would have moved it.

**Why (scope, and the gang is where it hurts).** A gang member's loss stalls
every other member; a plain replica's loss costs one replica. Draining is a
policy with consequences for stateful work (`onLost`) that the operator's
handler can already choose (`legion node drain`), and it should stay that
handler's call.

## 2. The notice is a node dynamic attribute, not a new node field

**Call.** `legion.interruption = { deadline, source, detail }` in the node's
dynamic attributes; written through `UpdateNodeDynamic` by `legion node
interrupt` and by the manager when a worker reports one.

**Alternatives.** A column on `node` and a dedicated RPC.

**Trade-off.** The reserved key lives in a user-writable namespace, and an
attribute update re-evaluates every job on the node (as any dynamic update
does).

**Why (simplicity and agent-friendliness).** No migration, no new RPC, no ACL
entry; `node info` shows it, constraints can read it, and the existing MCP
tool sets it.

## 3. The watcher reads IMDS on the node; AWS only

**Call.** The worker polls EC2's `spot/instance-action` (IMDSv2, v1
fallback) when its type sets `spotInterruption = { provider = "aws" }`.

**Alternatives.** EventBridge/SQS on the manager (Karpenter), or a plugin
interface for every cloud.

**Trade-off.** Other clouds need an external handler running `legion node
interrupt`.

**Why (simplicity).** IMDS needs no credentials, no queue, and no manager
configuration, and it is the notice the node itself sees first. A plugin
interface for one implementation would be speculative.

## 4. Gangs move at once, whatever the deadline

**Call.** The move happens when the notice lands; the deadline is recorded,
not scheduled against.

**Alternatives.** Move at `deadline - grace`.

**Trade-off.** A gang that could have run another ninety seconds stops early.

**Why (performance of recovery).** A gang re-placing (stop grace, scheduling,
image pulls on new nodes) takes longer than the two minutes AWS gives, so
waiting only shortens the time the new gang has to start before the old
one's node is gone.
