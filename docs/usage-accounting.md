# Usage Accounting

Namespaces show what is charged **now** ([namespace
policies](namespaces.md#namespace-policies)). Usage accounting adds what was
used **over time**: reserved CPU, memory, cores and allocations, integrated
into hourly buckets per namespace, job, and the token that submitted the job.
It answers "how much did team-a use this week?" and "how much did this
agent spend?", and it is the basis future budgets will be checked against.

```console
$ legion usage --since 7d
Reserved usage 2026-11-01T10:00:00Z .. 2026-11-08T10:00:01Z
KEY       CPU_CORE_HOURS  MEMORY_GIB_HOURS  CORES_HOURS  ALLOC_HOURS  OTHER
default   12.40           48.00             0.00         168.00
team-a    412.30          1630.20           24.00        1008.00      gpu=96.00

$ legion namespace usage team-a --by job --since 24h
$ legion usage --by submitter --since 30d       # per submitting token (accessor)
$ legion usage --by hour --namespaces team-a --json
```

## What is measured

Every `governance.usage.interval` (default 60 s) the manager leading the
status lane reads the allocations that hold node capacity — exactly the set
[namespace quotas](namespaces.md#namespace-policies) charge, every
allocation not yet `complete` or `failed` — and adds each one's
**reservation** times the elapsed seconds to the current hour's bucket:

| Dimension | Unit stored | Reported as |
| --- | --- | --- |
| `cpu_mhz` | MHz-seconds | `cpuCoreHours` (÷ 1000 MHz ÷ 3600) and `cpuMhzHours` |
| `memory_mib` | MiB-seconds | `memoryGibHours` |
| `cores` | core-seconds | `coresHours` (exclusive cores) |
| `allocations` | allocation-seconds | `allocationHours` |
| `device:<group>` | instance-seconds | `otherHours["device:<group>"]` — each [device](devices.md) group the allocation was assigned, counted per instance, so a two-GPU task accrues two GPU-hours an hour |
| `resource:<name>` | unit-seconds | `otherHours[<name>]` — every other numeric reservation key, such as a custom resource |

A new numeric resource kind is accounted as `resource:<name>`, and a new
device group as `device:<group>`, without a schema change. A device request
that was never assigned (the jobspec's copy) charges nothing.

**This is reserved usage, not measured consumption**: a task that reserves
4 cores and idles is charged 4 cores, which is what capacity planning and
chargeback usually want. Measured CPU and memory are in `legion node usage`.

The **submitter** of a job is the accessor (or, for JWT callers, the
principal) of its latest successful `RunJob`/`RunJobTemplate` in the [audit
journal](audit-journal.md). A job whose submit has aged out of the journal,
or that was submitted by the manager itself, shows as `(unknown)`.

## Querying

`GetUsage` (CLI `legion usage`, `legion namespace usage <NS>`, MCP
`legion_usage`) takes namespaces (names or globs), a job, a submitter, a
window (`--since`, default 7d on the CLI and 24h on the RPC; `--until`), and
a grouping: `namespace` (default), `job`, `submitter`, or `hour`.

Access is per row: a row is shown when the caller holds `namespace:read` in
its namespace, and per-job and per-submitter views also need `job:read` on
the job. A caller that reaches no namespace at all gets the ordinary
permission error.

## Configuration

```nickel
governance = {
  usage = {
    enabled = true,
    interval = "60s",    # how often reservations are integrated
    retention = "90d",   # hourly buckets older than this are dropped
  },
}
```

## Accuracy and limits

- The accumulator keeps its last tick in memory. After a status-lane
  leadership change the new leader's first tick only sets its baseline, so
  up to one interval per failover goes unaccounted. A stalled tick is
  credited at most two intervals.
- An interval is credited entirely to the hour it ends in, so bucket
  boundaries are accurate to one interval.
- The current hour is partial until it ends.
- Budgets that *enforce* (refuse a submit, scale down) are not implemented;
  these rollups are what they will be checked against.
