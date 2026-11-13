# Judgement calls: Lane Isolation

Plan: [`plans/lane-isolation-plan.md`](../plans/lane-isolation-plan.md).

Entries are ranked by project impact, highest first; an entry's number is its
rank.

## 1. Three TODO items become one plan, and the plan is the deliverable

**The call.** "Pipeline the scheduler lane", "a database file per lane" and
"the leader still executes twice under `lanes`" get one plan with four
phases, and no code in this change. The TODO keeps the build work open, each
phase linked to its section.

**The alternatives.** (a) Build per-lane files now, since the TODO named it
first. (b) Pipeline the lane first, accepting re-read projection with an
allowlist so strict it covers only plan applies. (c) Three separate plans.

**The trade-off.** None of the three throughput wins lands here; the
measured ceilings (≈50 keyed lifecycles/s, lanes serializing on one lock)
stand until the phases are built.

**Why (simplicity, and correctness over speed).** The three items share
their blocker: everything that reads committed rows after an apply has to
stop doing so before two entries can be in flight (pipelining) or two files
can commit independently (lanes). (a) is a storage-layout change touching
snapshots, backup, commutative tables and every cross-lane read — the
previous write-path work called it too large to do safely inside another
change, and nothing has made it smaller. (b) would ship the anomaly calls
#5 of the write-path plan describes (N's projection publishing N+1's
speculative rows) for any op outside the allowlist that the allowlist
missed. (c) would make each plan restate the same background and the same
projection prerequisite. Phases 1 and 3 are independent, so the plan does
not serialize work that can run in parallel.

## 2. Projection from the changeset comes first, with a counted re-read fallback

**The call.** Phase 1 makes the apply-time projection read only the
changeset and the read model; an UPDATE whose unchanged columns are not in
the read model falls back to a re-read, counted per table.

**The alternative.** Make projection purely changeset-driven by configuring
the session to record full rows (SQLite has no such mode for UPDATE), or
by writing the full row on every UPDATE (changing every write's size).

**The trade-off.** Two code paths for a while; the fallback could hide a
missed table.

**Why (performance, with a guard).** The read model already holds every
column of the rows it mirrors, which are exactly the tables whose UPDATEs
need unchanged columns; every other table's projection only needs keys,
which every changeset record carries. The counter (and a zero-re-read test
on the scheduler suite) makes a regression visible instead of silent.

## 3. Per-lane files only under `lanes`, no conversion of existing stores

**The call.** `single` keeps one file. `lanes` stores created after phase 3
ship use one file per lane; existing `lanes` stores are not converted.

**The alternative.** Always use per-lane files (one layout), or convert on
startup.

**The trade-off.** Two on-disk layouts to test; an operator on an old
`lanes` store must back up and restore into a fresh cluster to get the
layout — the same path topology changes already require.

**Why (simplicity).** `single` gets nothing from separate files (one group,
one pipeline) and would pay cross-file reads for nothing. Conversion code
for a layout with no installed base is code nobody runs (AGENTS.md:
"Legion has no installed base to protect yet").
