# Plan: Gang Reservations (anti-starvation for waiting gangs)

> **Judgement calls:** [`calls/gang-reservation-plan_calls.md`](../calls/gang-reservation-plan_calls.md)

**Status: SHIPPED (2026-11-12).** Built as designed below, in one phase.
Deliberately not built: backfill by expected finish time (Legion has no
runtime estimates), reservations for anything but gangs, and preemption to
clear a reservation's nodes. Public doc:
[`docs/gang-scheduling.md`, "Reservations"](../../docs/gang-scheduling.md#reservations).
Parent plan: [gang-scheduling-plan.md](gang-scheduling-plan.md) (Revision 3).

## 1. Problem statement

A waiting gang holds nothing (gang plan, call 1). That makes waiting free and
deadlock-free, and it makes a large gang starvable: on a busy cluster, capacity
frees a node at a time, and the next small job in the queue takes it before
the gang ever sees enough at once. The gang plan named this as its first
limitation, shipped the `legion_gang_blocked_jobs` gauge as the tripwire, and
left "reservation / backfill" open with "needs its own plan". Today the
operator's only remedy is to fence capacity by hand with a pool and `permit`.

## 2. Prior art

- **Slurm** backfill: the highest-priority pending job gets a reservation at
  the earliest time it could start, computed from running jobs' time limits;
  lower-priority jobs may start only if they finish before it (time limits
  are required). Proven, but it needs runtime estimates Legion does not have.
- **Kueue**: admitted workloads hold quota; `waitForPodsReady` evicts on
  timeout. Quota is the reservation; it is cluster-wide, not per node.
- **YuniKorn**: placeholder pods hold resources for a gang
  (`placeholderTimeoutInSeconds`, default 15m) and are swapped for real pods.
  Real per-node holds, bounded by a timeout.
- **Volcano**: `reserve` action (experimental) holds the nodes a starving job
  needs, one job at a time, and lets other jobs use only what is left.
- **Nomad**: no reservations; system jobs and priorities only.

Taken: YuniKorn/Volcano's per-node hold, bounded by a timeout, for one wait at
a time, oldest first. Rejected: Slurm's time-based backfill (no estimates),
Kueue's cluster-wide quota (does not solve per-node fragmentation).

## 3. Jobs to be done

- **ML engineer** (submits large gangs): "My 64-GPU job should start
  eventually on a busy cluster, without me asking an admin to drain nodes."
- **Platform operator**: "I want large gangs to make progress without
  carving out a dedicated pool, and without letting one gang idle a rack
  forever."
- **Agent / pipeline**: "Tell me whether a waiting gang is accumulating room
  or just waiting, and until when."

Scenarios: (S1) a 16-member gang waits behind a stream of 1-member jobs on a
cluster that frees one member's room a minute; after the wait threshold it
reserves 16 members' worth on the nodes most likely to free up, small jobs
keep running beside it on what is left, and it places when the reserved room
is free. (S2) A gang that can never fit (it asks for more than the eligible
nodes hold empty) reserves nothing. (S3) A `critical` job arrives during a
`normal` gang's reservation and places on reserved room anyway.

## 4. Goals and non-goals

Goals: bounded, automatic progress for waiting gangs; nothing held for the
common case (a gang that fits soon); no new deadlock; visible in `job info`
and a metric; one knob in `schedulerSettings`.

Non-goals: time-based backfill; reservations for non-gang jobs; preempting to
clear reserved nodes; reserving across several waits of one gang; a per-job
override.

## 5. Background — what exists today

- `gang_wait` (`crates/manager/src/storage/store/gang_waits.rs`) records when
  a gang's wait began, kept across re-blocks; ended by a successful eval
  (`Store::delete_job_blocked_evals_tx`), stop, restart, destroy, timeout.
- The blocked-eval waker (`crates/manager/src/manager/daemon.rs`,
  `spawn_blocked_eval_waker`) runs on the scheduler-lane leader every second.
- The planner's capacity view is `ResourceLedger` over the read model's
  `node_pools` (`crates/manager/src/manager/scheduler.rs`).

## 6. Proposed design

### 6.1 Surface

```nickel
# manager config
schedulerSettings = {
  gangReservation = { after = "5m", hold = "30m" },   # default; `false` turns it off
},
```

`after`: how long a gang must have waited short of its quorum (from its first
block) before it reserves. `hold`: how long a reservation lasts if the gang
does not place. Both at least `1s`; unknown fields rejected at startup.

### 6.2 Making a reservation

The scheduler leader's gang sweep (every second): for each `gang_wait` older
than `after` with no reservation in this wait (`gang_wait.reserved_unix` is
NULL) and a blocked eval carrying a gang report, oldest first:

- need = `min - running` from the report;
- candidate nodes: available, could help the blocked key (pool, constraints,
  permit, namespace), not interrupted, inside the report's topology domain
  when it names one;
- each node holds `replicas_fit(capacity - other reservations, request)`;
- order by members that fit now (desc), then holds (desc), then id; take
  nodes until need is covered; if it cannot be, no reservation.

The row: `gang_reservation(job_config_id, job_id, task_config_id, task_name,
band, request, nodes {node: members}, created_unix, expires_unix)`.

### 6.3 Honoring it

Every plan loads the reservations (one read of a tiny table) and, unless it is
the gang's own plan or its band outranks the gang's, debits each reserved
node's pool by `members × request`. Pools may go negative (fits nothing). Work
beyond the reservation still places (backfill by room, not time).

### 6.4 Ending it

A successful eval of the config (the gang placed), any end of the wait (stop,
restart, update, destroy, timeout, a failed gang), or `expires_unix`. The
sweep also releases any whose wait is gone, and drops all when the setting is
off. One reservation per wait.

### 6.5 Seeing it

`GangReport.reserved_members` / `reserved_nodes` / `reservation_expires`
(proto 17–19), filled on `job info` from the table; the CLI prints `Reserved
room for N member(s) on M node(s) until ...`; gauge `legion_gang_reservations`.

## 7. Proto, struct, config, persistence

- Migration: `gang_reservation` table, `gang_wait.reserved_unix`.
- Raft ownership: `gang_reservation` on the scheduler lane.
- `GangReport` fields 17–19.
- `schedulerSettings.gangReservation`.

## 8. Implementation phases

One phase: it is useless in parts.

## 9. Testing plan

Unit: settings parse; `binds` by band; sweep reserves only after `after`, once
per wait; a same-band small job cannot take reserved room; a higher band can;
the gang places into it. Integration: the existing `gang_scheduling::` suite
(waiting gangs unchanged before `after`).

## 10. Rollout

Default on. A cluster's first gang to wait five minutes starts holding room;
`gangReservation = false` restores pure hold-nothing waiting.

## 11. Open questions

- Should a reservation be renewable after `hold` if no other gang waits? Lean:
  no until someone needs it; `hold` is generous.
- Should `after` be per job (a jobspec field)? Lean: no; it is a cluster
  fairness policy.

## 12. Acceptance criteria

A gang that waited past `after` holds a reservation visible in `job info`;
smaller same-band work cannot take reserved room; the gang places when the
room is free; the reservation is gone after placement, stop, or `hold`.
