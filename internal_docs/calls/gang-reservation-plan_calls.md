# Judgement Calls: Gang Reservations Plan

Decisions made for
[`plans/gang-reservation-plan.md`](../plans/gang-reservation-plan.md) while
designing and building it (2026-11-12). **Entries are ranked by project
impact, highest first — the number is the rank.**

## 1. Reservations are on by default, after five minutes, for thirty

**Call.** `schedulerSettings.gangReservation` defaults to `{ after = "5m",
hold = "30m" }`; `false` turns it off.

```nickel
schedulerSettings = { gangReservation = { after = "5m", hold = "30m" } },
```

**Alternatives.** (a) Off by default, an operator opts in. (b) Reserve at
the first block (YuniKorn's placeholders).

**Trade-off.** A cluster with a starving gang now idles some capacity on the
reserved nodes (the room accumulates for the gang instead of serving small
work) for up to thirty minutes per wait, without anyone asking for it.

**Why (the right default for a new cluster).** Starvation is the one failure
the hold-nothing design admits (gang calls 1); a default that leaves it open
means every operator discovers it in production. Five minutes keeps the
common case (a gang that fits soon) holding nothing at all, and one bounded
reservation per wait means a gang that cannot get there cannot fence capacity
forever.

## 2. Backfill by room, not by time

**Call.** Other work may use a reserved node's capacity beyond the reserved
members' requests; nothing else is reserved, and nothing is estimated.

**Alternatives.** Slurm-style backfill: let a job use reserved room if it
will finish before the gang could start, which needs runtime limits.

**Trade-off.** A reservation can hold room that would have served a short job
that finished long before the gang's room was all free.

**Why (simplicity; Legion has no runtime estimates).** Services and sessions
have no end time, and batch tasks rarely declare one. A rule that needs an
estimate Legion does not have is a rule that silently does nothing.

## 3. Nodes are sized by capacity and ordered by room free now

**Call.** A reservation picks nodes that could take the task, most free room
for it first, each reserving what its whole capacity holds (less other
gangs' reservations), until the members the quorum needs are covered; none
when even that cannot cover them.

**Alternatives.** (a) Reserve only what is free now (a reservation that grows
as room frees). (b) Prefer nodes running work most likely to finish soon.

**Trade-off.** Room on a reserved node frees only as fast as its work leaves;
a node whose work never leaves holds its share for `hold`.

**Why (simplicity).** (a) is the hold-nothing behavior with extra steps: the
room taken by the next small job is never free. (b) needs the estimates of
call 2.

## 4. A higher band ignores a reservation; the gang's own plan uses it

**Call.** A reservation binds plans of the gang's band and below. A job in a
band that outranks the gang's sees no reservation.

**Alternatives.** Bind everything.

**Trade-off.** A `critical` job can take room a `normal` gang has waited an
hour for.

**Why (consistency with bands).** A higher band could preempt the gang's
members once placed anyway (gang calls 26); holding room against it would be
a stronger guarantee for a waiting gang than for a running one.

## 5. Made by the leader's sweep, read by every plan from the table

**Call.** The scheduler leader's once-a-second gang sweep makes and releases
reservations (scheduler lane); every plan reads the small `gang_reservation`
table once.

**Alternatives.** Keep reservations in the read model (no read per plan), or
make them inside the eval that blocks.

**Trade-off.** One read of a tiny table per plan, and up to a second's lag
between a gang placing and its reservation's row going (the placing eval
deletes it in the same write, so the lag applies only to stops and expiry).

**Why (simplicity).** The read model's projection would need a new table in
its changeset path for a row per waiting gang; a table read costs
microseconds. A sweep keeps the blocking eval's write unchanged.
