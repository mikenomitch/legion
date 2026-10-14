# Judgement calls: the scheduler lane's per-entry cost

Calls made while executing
[`plans/scheduler-lane-entry-cost-plan.md`](../plans/scheduler-lane-entry-cost-plan.md).
Entries are ranked by project impact, highest first. The number is the rank.

## 1. Phase 1 cuts the keyed bind's scan, not the lane's structure

**Call.** The first change to the lane's per-entry cost is the keyed
binding query inside the plan apply, made to read only live keyed rows from
two indexes. The pipelined lane and the second fsync are designed in the
plan and not built.

**Alternatives.** (a) Build the pipelined lane first, as `TODO.md`'s lever
list suggested: capture entry N+1 while N commits, as Nomad's plan applier
does. (b) Cut the state DB's per-entry fsync first, since every entry pays
two. (c) Batch heterogeneous ops into one entry at the pipeline.

**Trade-off.** The lane still runs one entry at a time, and a keyed bind
still grows with the task's *live* instances. A platform with tens of
thousands of running sandboxes in one task still pays for them on every
placement (entry 3).

**Why (performance, measured, then simplicity).** The profile said where the
time was. Under the lifecycle load, the bind's one statement was about half
of the pipeline thread's wall time and two thirds of all capture time, and
it grew with every instance of the job, parked ones included. Pipelining
hides only the part of propose → apply that runs outside the capture mutex,
which on one voter is a fraction of what the bind cost, and it carries a
correctness audit this change does not: a speculative capture sees
in-flight entries only on the write connection, and capture bodies read
through the read pool too (`stop_keyed_instance_apply` reads its instance
with `get_instance`). The fsync cut cannot be measured on the hardware at
hand (macOS `fsync` does not flush the drive cache), and it changes a
durability argument an adversarial review put in place. Pipeline-level
batching overlaps the separate "more work per entry" effort. The bind fix
is exact, small, and measurable here.

## 2. The rewrite keeps today's binding choice exactly

**Call.** The new query picks the same instance the old one did: same
filters, same ordering, with `NOT IN (SELECT ...)` replaced by a correlated
`NOT EXISTS` probe plus an explicit `instance_id IS NOT NULL`.

**Alternatives.** (a) Use `instance.current_alloc_id` as the "bound"
signal, which an index could serve directly and would make the bind
independent of the live count. (b) Have the planner name the instance each
placement is for, so the apply only verifies it.

**Trade-off.** The bind stays proportional to the task's live instances.

**Why (simplicity, correctness).** "Bound" today means "has an allocation
of *this* task config", which `current_alloc_id` does not encode: during a
create-first rollout an instance holds allocations of two configs, and its
pointer names only the newest. Nothing enforces that an `active` instance
always has an allocation either, so a pointer-based rule would rest on an
invariant the code keeps by convention. Option (b) changes the plan's
shape, which belongs to the planning work. The `IS NOT NULL` term matters:
`instance_id` is declared `STRING PRIMARY KEY` on a rowid table, which
SQLite lets hold a NULL, and `NOT IN` never matched one.

## 3. The partial index leaves out parked instances, and accepts the live ones

**Call.** `idx_instance_keyed_live` holds keyed rows whose state is not
`terminated`, `stopped`, or `suspended`, keyed by `(job_id, group_name,
task_name, instance_id)`.

**Alternatives.** (a) A full index on `(job_id, group_name, task_name,
state)`. (b) No new instance index, only the `NOT EXISTS` rewrite.

**Trade-off.** The index costs a little on every instance write: an entry
is added when an instance becomes live and removed when it parks. A bind
still visits every live instance of the task, two index lookups each.

**Why (performance).** Parked instances are what a sandbox platform has
most of, and the bind never wants them. A partial index makes them free
instead of cheaper, and makes `count_live_keyed_instances` an index-only
count. Carrying `instance_id` in the key lets the `NOT EXISTS` probe run
before any table read, so a bound live row costs two index lookups. The
cost is that SQLite only uses a partial index when the query's `WHERE`
implies the index's, so the two terms must stay word for word. The query
plan test fails the build if they drift.

## 4. The allocation probe index replaces the old one rather than joining it

**Call.** `idx_allocation_instance_config (instance_id, task_config_id)`
replaces `idx_allocation_instance_id (instance_id)`.

**Alternative.** Add the composite index and keep the old one.

**Trade-off.** None found: every lookup by `instance_id` alone is served
by the composite's prefix.

**Why (performance).** Allocation rows are written on every placement and
removal. Keeping both would add an index write to each for no read the
composite cannot serve.

## 5. No knob

**Call.** The change is unconditional.

**Alternative.** A `schedulerSettings` switch back to the old query.

**Trade-off.** An operator cannot opt out of the index's write cost.

**Why (simplicity).** The rewrite returns the same instance, and the only
cost is index maintenance that is small next to the scan it removes. A
switch would keep a second query path alive for nothing a user would
choose (`AGENTS.md`: no dual code paths for the old form).
