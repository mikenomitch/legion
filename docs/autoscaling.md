# Autoscaling

Legion scales tasks two ways:

- **Automatically**: put `autoscale = true` on a task, and the bundled
  autoscaler holds its CPU near 70% of its reservation, between its `count`
  and four times its `count`, scaling out fast and in slowly. Every default
  can be overridden per task, per namespace, or cluster-wide, and every
  change can be explained ([Task autoscaling](#task-autoscaling)).
- **By hand**: `legion job scale web --count 6` changes a task's count
  without a new job version or a rollout ([Scaling by hand](#scaling-by-hand)).
  On an autoscaled task the same command is a time-boxed pin.

Both read the same cluster-wide measurements, [scaling inputs](#scaling-inputs),
and both write the same count override, so the scheduler, rollouts, and
quotas see one desired count.

Beyond Legion's own measurements, a policy can scale on a Prometheus query,
an HTTP endpoint, or a [plugin](#plugins) the operator installs, and go to
zero replicas when idle ([Scale to zero](#scale-to-zero)). Something that
is not a Legion task (a render farm, a fleet of VMs) gets a
[standalone policy](#standalone-policies) with a target plugin.
`legion scaling simulate` runs any policy over the last hour of history
before you submit it ([Simulating a policy](#simulating-a-policy)). Node
autoscaling (adding and removing machines) is not run yet; see
[Known limitations](#known-limitations).

## Task autoscaling

### One word

```nickel
{
  name = "web",
  task = {
    name = "web",
    runtime = "exec",
    count = 2,                          # with autoscale: the floor
    config = { command = ["./web"] },
    resources = { cpu = 500, memory = 512 },
    autoscale = true,
  },
}
```

`autoscale = true` means "the effective defaults". `legion job plan` prints
exactly what that is for this task, so nothing is hidden:

```console
$ legion job plan web.ncl
...
autoscale web/default/web:
  hold cpu at 70% of 500MHz per replica, 1m average over ready replicas
  bounds 2..8 (min = count, max = 4 × count)
  up:   immediately, at most +100% or +4 per minute (whichever is larger)
  down: after 5m below target, at most −10% or −1 per minute (whichever is larger)
  warmup 30s, tolerance ±10%, enforce
```

`legion job plan -j` carries the same in `autoscale[]`: the effective record,
these lines, and where every field came from.

With load, the count rises within one evaluation (15 seconds), at most
doubling, or adding four, per minute. When the load falls, nothing happens
for five minutes; then the count falls by one replica (or 10%, whichever is
larger) per minute, never below `count`. It never scales in while a rollout
runs, while measurements are missing, or in the first five minutes after the
autoscaler starts.

### The built-in defaults

| Field | Default | Why |
| --- | --- | --- |
| `min` | `"1x"` (the task's `count`) | The count you wrote is the floor: autoscaling only ever adds capacity unless you lower `min` yourself, so a three-replica HA service never drops to one because traffic was quiet. |
| `max` | `"4x"` | Absorbs a 4x spike without letting a runaway signal spend the cluster. Hitting it emits `scale.limited`. |
| `cpu` | `0.7` | Hold mean CPU over ready replicas at 70% of the reservation, the number the scheduler packs on. A task with `cores = N` is measured against N x 1000 millicores. CPU above the reservation is a valid reading (it is a weight) and scales out. |
| `memory` | unset | Memory often does not fall when load falls (caches, heaps), so a memory check would hold scale-in. Opt in with `memory = 0.8`. |
| `up` | `{ stabilize = "0s", step = { percent = 100, replicas = 4 }, every = "1m" }` | React at once to load. |
| `down` | `{ stabilize = "5m", step = { percent = 10, replicas = 1 }, every = "1m" }` | Scale in only after five quiet minutes, slowly. |
| `headroom` | `0` | Replicas added on top of what the checks want. |
| `warmup` | `"30s"` | A replica that just became healthy is warming: for scale-out it counts as idle, for scale-in as exactly at target (the Kubernetes HPA rule). |
| `tolerance` | `0.1` | Within ±10% of the target, nothing moves. |
| `window` | `"1m"` | The averaging window of the `cpu` and `memory` readings. |
| `schedule` | none | Time windows that raise the floor or lower the ceiling. |
| `onMissing` | `"hold"` | A check without data blocks scale-in; checks with data may still scale out. |
| `mode` | `"enforce"` | `"dry-run"` records what it would do and changes nothing. |

### Overriding the defaults

Every field can be overridden at three levels, each with the same record
shape. A field set at a level wins over every level below it; a field left
unset falls through.

| Level | Where | Who can set it |
| --- | --- | --- |
| task | `autoscale = { … }` in the jobspec | the job author (`job:write`) |
| namespace | `autoscale = { … }` in a namespace policy | `acl:write` (namespace policies' gate) |
| cluster | `legion scaling defaults apply defaults.ncl` | `operator:write` |
| built-in | the table above | Legion |

```nickel
# defaults.ncl — legion scaling defaults apply defaults.ncl       (cluster)
{ cpu = 0.6, max = "6x", down = { stabilize = "10m" } }

# payments.ncl — legion namespace policy apply -f payments.ncl    (namespace)
{
  name = "payments",
  namespaces = ["payments-*"],
  autoscale = { down = { step = { replicas = 1 }, every = "2m" }, mode = "dry-run" },
}

# the jobspec                                                      (task)
autoscale = { max = 20 },
```

The rules:

- **Field by field, including nested records.** `down = { stabilize = "10m" }`
  keeps the inherited `down.step` and `down.every`. The exception is a
  `step` record: writing `step` replaces the whole inherited `step`, and a
  half you leave out is 0. `step = { replicas = 2 }` means exactly 2 per
  window, not "2 or 100%".
- **`min` and `max` take a count or a multiple of `count`.** `max = 20`, or
  `max = "6x"`. Multiples round up (`"1.5x"` of 3 is 5).
- **An inherited `min` cannot lower the floor.** At the cluster and
  namespace levels, `min` must be a multiple of at least `"1x"`. Defaults
  may raise every task's floor (`min = "2x"`), never put a task below the
  count its author wrote. Only the task itself may set `min` below `count`.
  So `min <= count` is checked only for a task's own `min`, while
  `count <= max` is checked whatever level `max` came from.
- **Signals are all or nothing, at every level.** `cpu`, `memory`, and
  `checks` move as one unit: the highest level that names any of them
  supplies all of them. A cluster default of `{ memory = 0.8 }` makes memory
  the only default signal (it does not add memory to CPU); a task with
  `autoscale = { checks = [...] }` does not also scale on CPU. Defaults may
  set `cpu` and `memory`, never `checks`.
- **Namespace policies have no precedence among themselves.** Every policy
  governing a namespace applies. Two that set one field to different values
  leave that field unresolvable for tasks that do not set it themselves:
  their submit fails naming both policies, and a task already running holds
  as `invalid`.
- **Defaults are resolved live.** The jobspec stores what you wrote (`true`,
  or the sparse record). Change a cluster default or a namespace policy and
  every inheriting task's effective policy changes, with no resubmit.
- **A change is checked against everything that inherits it.**
  `legion scaling defaults apply` and `legion namespace policy apply` resolve
  every affected policy first. If any would become invalid (a cluster `min`
  above some task's ceiling, say), the apply is refused and lists them;
  `--dry-run` shows the same report and writes nothing. With `--force` the
  change applies and those tasks hold with state `invalid: <why>` until
  fixed.

```console
$ legion scaling defaults apply defaults.ncl --dry-run
scaling defaults: dry run, nothing written (v2); 3 policies would change, 0 invalid
 TARGET               | CHANGE
----------------------+----------------
 web/default/web      | max, cpu, down.stabilize
 ...
$ legion scaling defaults get -n payments-prod
cluster defaults v3 (by alice, 2026-10-28T14:00:00Z): {"cpu":0.6,"down":{"stabilize":"10m"},"max":"6x"}
namespace policies with autoscale for payments-prod: payments
effective defaults for namespace payments-prod:
 FIELD          | VALUE                    | FROM
----------------+--------------------------+-------------------------
 min            | 1x                       | built-in
 max            | 6x                       | cluster defaults
 cpu            | 0.6                      | cluster defaults
 down.step      | 1 replica                | namespace policy payments
 ...
```

Where each value of one task's policy came from is one command away:

```console
$ legion scaling explain payments-prod/api --policy
...
  effective policy 9c1e04b2d6aa
 FIELD          | VALUE                   | FROM
----------------+-------------------------+--------------------------
 min            | 4 ("1x" of count 4)     | built-in
 max            | 20                      | jobspec
 cpu            | 0.6                     | cluster defaults
 down.stabilize | 10m                     | cluster defaults
 down.step      | 1 replica               | namespace policy payments
 down.every     | 2m                      | namespace policy payments
 mode           | dry-run                 | namespace policy payments
```

### The full policy surface

Everything beyond `true` is optional; a field left out takes its effective
default.

```nickel
autoscale = {
  min = 4,
  max = 16,                # or "4x"
  cpu = 0.65,              # hold mean CPU at 65% of the reservation
  memory = 0.8,            # and memory at 80% (off by default)
  checks = [               # explicit checks; the largest desired count wins
    {
      name = "backlog",
      source = "legion",
      query = "task.unplaced",
      window = "2m",
      strategy = { name = "threshold", steps = [{ above = 0, add = 1 }] },
      scaleIn = false,     # may push the count up, never pull it down
    },
  ],
  up   = { stabilize = "30s", step = { replicas = 2 }, every = "1m" },
  down = { stabilize = "10m", step = { replicas = 1 }, every = "2m" },
  headroom = 1,            # add N replicas after the merge
  warmup = "45s",
  tolerance = 0.1,
  window = "2m",
  schedule = [             # raise the floor (or lower the ceiling) in a window
    { cron = "0 8 * * 1-5", for = "10h", min = 10, timezone = "America/New_York" },
  ],
  onMissing = "hold",      # hold (default) | ignore
  mode = "dry-run",        # enforce (default) | dry-run
}
```

- **Checks** read the `legion` source, Legion's own measurements
  ([Scaling inputs](#scaling-inputs)), through a fixed vocabulary:

  | Query | Meaning |
  | --- | --- |
  | `task.cpu.utilization` | mean CPU over ready replicas, as a fraction of the reservation (what `cpu` uses) |
  | `task.memory.utilization` | the same for memory (what `memory` uses) |
  | `task.unplaced` | replicas the scheduler could not place |
  | `service.rps`, `service.errors` | requests and errors per second into the service the task backs (internal callers only) |
  | `service.latency_ms.mean`, `service.latency_ms.max` | its latency |

  A `service.*` query needs a `service` block on the task. Other sources
  (Prometheus, HTTP, plugins) are installed by the operator and named by
  the check's `source`: see [Sources](#sources).
- **Strategies** turn a check's reading into a desired count:
  `target-value` (`{ name = "target-value", target = 0.7 }`, the HPA formula
  `ceil(current x reading / target)` with the tolerance band and the warmup
  rule; the `cpu`/`memory` shorthands use it), `per-replica`
  (`{ value = 500 }`: `ceil(reading / value)`), `threshold`
  (`{ steps = [{ above, add }] }`: the current count plus the `add` of the
  highest step the reading exceeds), `pass-through` (the reading is the
  count), and `fixed` (`{ value = N }`, reads nothing). Any other `name` is
  a strategy [plugin](#plugins) the operator installed; the rest of the
  record is its config.
- **Merge** is the maximum over checks with data. A `scaleIn = false` check
  counts only when it wants more than the current count.
- **`activation`** (a number) on a check makes it one that can wake a
  `min = 0` target, and put it to sleep ([Scale to zero](#scale-to-zero)).
- **Schedules** are five-field crontab (`0 8 * * 1-5` is Monday to Friday at
  08:00) in an IANA `timezone` (default UTC), open for `for` (at most 7
  days). They only raise `min` or lower `max`; a schedule floor above the
  ceiling yields to the ceiling.
- **`onMissing`**: `hold` (the default) blocks scale-in while any check has
  no data; `ignore` drops a check without data for that evaluation.

**Checked at submit** (`legion job run`, `legion job plan`, and offline in
`legion job validate`), each error naming the task and the fix:

```
error: web/default/web: autoscale needs measured usage, and runtime 'noop' does not
  report it. Hint: only exec, container, sandbox report usage; use checks on another
  `legion` query (task.unplaced, service.*), or remove autoscale
error: web/default/web: count 2 is above autoscale.max = 1 (from jobspec). Hint: raise
  max in the task's autoscale record, or lower count
error: web/default/web: autoscale.min = 0 needs a check that can wake it from zero; 'cpu'
  reads the task's own usage, which is empty at 0 replicas. Hint: add a check with
  `activation` on an external source or a schedule, or set min >= 1
```

Also refused: `autoscale` on a keyed stateful task, a `count.per` task, or a
batch or scheduled task; two autoscaled tasks in one group, or group
members with different counts; a gang task whose floor is below its
quorum (`gang.min`); an unknown `legion` query; a source or strategy
plugin that is not installed, or not open to the job's namespace
([Sources](#sources)).
Leaving `resources.cpu` unset is a warning, not an error: the task is then
measured against the default reservation.

### Sources

A check reads one **source**. Four kinds exist:

| Source | Installed | `query` is | Window |
| --- | --- | --- | --- |
| `legion` | always | one of the fixed vocabulary above | averaged by the autoscaler over the check's `window` |
| a `prometheus` builtin | by the operator, `{ builtin = "prometheus", config = { address } }` | PromQL that returns one series (`sum(...)`, `max(...)`) | with a `window`, a range query over it (about 12 points) averaged; an instant query otherwise |
| an `http` builtin | by the operator, `{ builtin = "http", config = { url } }` | a JSON pointer into the document at `url` (`/queue/depth`; empty: the document is the number) | averaged by the autoscaler, like `legion` |
| a plugin | by the operator ([Plugins](#plugins)) | whatever the plugin defines | the plugin's `value` is taken as averaged; a `series` is averaged by the autoscaler |

```nickel
autoscale = {
  min = 1,
  max = 20,
  checks = [{
    name = "lag",
    source = "team-prom",            # installed in the autoscaler's app config
    query = "sum(kafka_consumergroup_lag{group=\"ingest\"})",
    window = "2m",
    strategy = { name = "per-replica", value = 500 },
  }],
}
```

A policy names a source; it never carries an address. The operator decides
which sources exist and which namespaces may use each one
([Plugins](#plugins)), so a submit naming a source that is not installed,
or one closed to the job's namespace, is refused:

```
error: payments-prod/api/default/api: check 'lag' uses source 'team-prom', which is not
  available to namespace payments-prod (allowed: analytics-*)
error: web/default/web: check 'lag' uses source 'team-prom', which is not installed. Hint:
  the operator installs plugins in the autoscaler's app config (`legion app config set
  autoscaler`); no source plugin is installed for namespace default; the `legion` source
  needs no install
```

`legion job validate` cannot see the installed plugins, so it checks
everything else offline; `legion job plan` and `legion job run` check
these too.

Each distinct `(source, query, window)` is read once per evaluation, however
many policies share it. A source that times out (10 seconds), errors, or
returns no series makes the check **missing**: it votes nothing, and nothing
scales in while it lasts (`onMissing = "hold"`). Its message is in `legion
scaling explain` and a `scale.check_failed` event.

### Scale to zero

`min = 0` lets a target sleep at zero replicas. It needs a check that can
wake it: a check with `activation`, on a source that has data at zero
replicas (a queue, an external count), or a schedule with `min >= 1`. A
policy without one is refused, because nothing could ever wake it:

```
error: web/default/web: autoscale.min = 0 needs a check that can wake it from zero; 'cpu'
  reads the task's own usage, which is empty at 0 replicas. Hint: add a check with
  `activation` on an external source or a schedule, or set min >= 1
```

```nickel
autoscale = {
  min = 0,
  max = 30,
  checks = [{
    name = "lag",
    source = "team-prom",
    query = "sum(kafka_consumergroup_lag{group=\"ingest\"})",
    strategy = { name = "per-replica", value = 500 },
    activation = 1,           # above 1 queued message, the worker is awake
  }],
  down = { stabilize = "10m" },
}
```

It follows KEDA's split:

- **At zero, only activation checks vote.** When any reads above its
  `activation`, the target wakes to what that check's strategy wants (at
  least 1); the step limit and the bounds apply as for any scale-out. The
  `cpu` and `memory` shorthands and other checks are silent at zero: there
  is nothing to measure.
- **From 1 up, every check votes** as usual.
- **Going to sleep** uses the scale-in path: the target reaches zero only
  when every activation check reads at or below its `activation`, through
  the `down.stabilize` window and the scale-in step limit. Missing data is
  never idle: a source that stops answering holds the count.
- `count` is still the starting size, and must be at least 1; the target
  sleeps once its activation checks say so.

A task at zero has no allocations; `legion scaling status` shows it at
`0` with its bounds. Nothing wakes a request-serving `service` on its first
request: the gateway holds requests only for keyed instances
([gateway.md](gateway.md)), so a `service` at zero refuses connections
until a check wakes it. Wake it from a signal that exists while it sleeps
(a queue, a schedule). Suspend and resume (`suspend.wake` on keyed
instances, [suspend-and-resume.md](suspend-and-resume.md)) are a different
mechanism for a different kind of task: keyed tasks cannot be autoscaled,
so the two never meet.

### Groups

If `autoscale` is set on a task whose group has other tasks, the policy
targets the whole group: every task in it moves to the same count in one
write, because the group is what gets placed together. Utilization is then
the group's usage over the group's reservations. At most one task per group
may carry `autoscale`, and the group's tasks must share a count. `legion
scaling status` shows such a target as kind `group`.

### Who owns the count

With `autoscale`, the policy owns the live count:

- **`count` is the floor and the starting size.** Raising `count` from 2 to
  4 in a resubmit lifts a task the autoscaler has at 3 to 4 at once (the
  submit moves it into the new bounds in the same write) and leaves one at
  6 alone. Lowering `count` only lowers the floor; the autoscaler brings
  the count down at its scale-in rate.
- **Resubmits never reset the count.** The override is kept and rebased onto
  the new spec. Removing `autoscale` returns the task to the manual rule
  ([Ownership](#ownership-overrides-vs-resubmits)) on that same submit.
- **Editing the policy restarts nothing.** A resubmit whose only changes are
  in `autoscale`, and in the `count` of autoscaled tasks, still creates a
  job version, but the running allocations move to it in place instead of a
  rollout: `Job updated in place: only autoscale (and autoscaled counts)
  changed, so nothing restarts`. This needs no rollout in flight and every
  allocation on the previous version; otherwise it rolls out as usual.
- **A manual count is a pin.** `legion job scale web --count 12` on an
  autoscaled task is refused unless it says how long
  ([Pins](#pins-a-manual-scale-on-an-autoscaled-task)).

### How it decides

Every evaluation runs one fixed pipeline. Each stage passes the number on or
changes it and says why; those notes are what `legion scaling explain`
prints.

| Stage | What it does |
| --- | --- |
| checks | each check's strategy turns its reading (averaged over its window) into a desired count. A check without data contributes nothing. Warming replicas count as idle for scale-out and as at target for scale-in. |
| merge | the maximum over checks with data |
| headroom | adds `headroom` replicas |
| stabilize | scale-out takes the lowest recommendation in `up.stabilize`, scale-in the highest in `down.stabilize` |
| hold | holds scale-in: for one `down.stabilize` after the autoscaler starts (`collecting 5m of history after restart`), while a check has no data (`onMissing = "hold"`), and after flapping (3 direction changes in 10 minutes hold scale-in for twice `down.stabilize`) |
| step | the larger of `percent` of the count at the start of the `every` window and `replicas`, minus what was already applied in that direction in the window |
| bounds | clamps into `min..max`, with open schedule windows. After the step limit on purpose: a raised floor or a lowered ceiling applies at once |
| capacity | holds scale-out while more than half of the last scale-out is still unplaced |
| rollout | defers scale-in while a rollout runs (scale-out still applies) |
| pin | a pin replaces the result, or bounds it |
| pause | a pause keeps the count |
| mode | `dry-run` turns a change into a "would" |

The write itself is a `ScaleTask` with the decision attached,
compare-and-set against the override version the evaluation read: if anyone
changed the count since (a person, or a second autoscaler replica), the
write is refused and the next evaluation re-reads. A namespace quota is
enforced by that write: a scale-out the quota cannot admit is refused, and
the target reports `held` with the quota's message.

### Pins: a manual scale on an autoscaled task

A person's count on an autoscaled task is a time-boxed pin, not a fight with
the autoscaler:

```console
$ legion job scale web --count 12 --for 2h --reason "bad deploy"
web/default/web: 6 -> 12; autoscaling pinned until 05:10 UTC (legion job scale web --release)
$ legion job scale web --min 10 --for 1h     # pin only the floor; still autoscaled above it
$ legion job scale web --max 4 --for 30m     # or only the ceiling
$ legion job scale web --release             # end the pin now
```

- `--for` is required on an autoscaled task. Without it the command is
  refused with a hint (`add --for, as in legion job scale web --count 12
  --for 1h, or remove autoscale from the job to own the count by hand`). A
  pin ends on its own; when it does, the count stays where it is and the
  autoscaler takes over at its normal rate, so a forgotten pin comes down
  gently.
- A count pin may exceed `max`: it is an operator's explicit override.
- `--min`/`--max` move the count into the pinned range at once.
- `--for` on a task without `autoscale` is refused: its count is yours
  anyway.
- The server enforces pins: the autoscaler's writes to a count-pinned task
  are refused, and its writes outside a pinned floor or ceiling too.

### Pauses

```console
$ legion scaling pause web --for 1h --reason "incident 4411"
paused autoscaling for web (every autoscaled task) until 15:10 UTC
$ legion scaling pause --all --reason "freeze"        # the cluster-wide kill switch
$ legion scaling resume web
$ legion scaling resume --all
```

A pause covers a task (`--group`/`--task`), a job, or the whole cluster.
Without `--for` it holds until resumed. It is replicated state, enforced in
two places: the autoscaler skips paused targets (state `paused`), and
`ScaleTask` refuses autoscaler writes to them, so a pause holds even
against a misbehaving app. People can still scale a paused task by hand.
`--all` needs `operator:write`; a job's pause needs `scale:write` on it.
`legion app disable autoscaler` stops the app altogether.

### Seeing what it did, and why

```console
$ legion scaling status
 TARGET           | KIND | NOW | WANT | BOUNDS | MODE    | STATE   | LAST
------------------+------+-----+------+--------+---------+---------+---------------------------
 web/default/web  | task | 6   | 6    | 2..8   | enforce | ok      | 4 → 6 2m ago (autoscaler)
 api/default/api  | task | 4   | 4    | 4..16  | dry-run | dry-run | would 4 → 7 1m ago
autoscaler: running (alloc 42), last evaluation 3s ago

$ legion scaling explain web
web/default/web   task   enforce   now 6   bounds 2..8   evaluated 4s ago

 CHECK | INPUT                     | WANTS
-------+---------------------------+-------
 cpu   | 0.91 vs 0.70 (4 ready)    | 6

 STAGE     | RESULT | NOTE
-----------+--------+---------------------------------
 merge     | 6      | cpu wants 6
 stabilize | 6      | up: no window
 step      | 6      | up ≤ +4 per 1m; +0 used
 bounds    | 6      | 2..8
 decision  | 4 → 6  | applied 14:02:25 (d-81f3a2c4)
autoscaler: running (alloc 42), last evaluation 4s ago

$ legion scaling history web
 TIME           | TASK        | CHANGE  | BY         | SUMMARY
----------------+-------------+---------+------------+----------------------------------------------
 10-28 14:02:25 | default/web | 4 → 6   | autoscaler | cpu 0.91 vs 0.70 (4 ready)
 10-28 13:31:02 | default/web | 5 → 4   | autoscaler | cpu 0.41 vs 0.70 (5 ready); rate-limited from 2
 10-28 12:58:40 | default/web | 2 → 5   | alice      | launch traffic
```

- **States** (`legion scaling status`, `explain`): `ok` (where the policy
  wants it), `scaled` (this evaluation changed the count), `held` (a
  stabilization window, the step limit, a hold, or a refused write held a
  change), `limited` (a bound or the capacity guard), `deferred` (a
  scale-in waits for a rollout), `pinned`, `paused`, `dry-run`,
  `missing-data`, `invalid` (the policy does not resolve), `error`, `stale`
  (no evaluation in three of the app's intervals), and `not-evaluated`.
- **History** is durable: every count write, by people and the autoscaler,
  applied or deferred; pins and releases; and a dry-run policy's "would"
  rows (`applied = false`, one per distinct change, not one per
  evaluation). It is capped at 200 rows per task. Each autoscaler row
  carries the digest of the effective policy that decided, and the manager
  keeps that policy, so `legion scaling history -j` shows the exact policy
  even after the defaults that shaped it changed.
- **Evaluations that held are not stored**: the last one per task is in the
  status-lane leader's memory, which `status` and `explain` read.
- **Events**: `scale.applied` (with `source = autoscaler` and the decision's
  summary), `scale.deferred`, `scale.reset`, `scale.pinned`,
  `scale.paused`, `scale.resumed`, `scale.limited` (once per episode, when a
  bound or guard starts holding the count), `scale.flapping`, and
  `scale.check_failed` (a check lost its data) ([event-stream.md](event-stream.md)).
  There is no event per evaluation.
- **MCP**: `legion_scaling_status`, `legion_scaling_explain`,
  `legion_scaling_history`, and `legion_scaling_simulate` (read-only);
  `legion_scaling_pause` and `legion_scaling_apply` (write-gated)
  ([agent-readiness.md](agent-readiness.md)).

### Simulating a policy

`legion scaling simulate` runs a policy over recent history, or a CSV
series, and prints the decisions it makes. Nothing is written.

```console
$ legion scaling simulate web                     # the live policy, last hour
$ legion scaling simulate web.ncl --since 30m     # the policy you are about to submit
$ legion scaling simulate web.ncl --series load.csv
$ legion scaling simulate render-farm.ncl --series queue.csv   # a standalone policy

$ legion scaling simulate web
web/default/web: replay 360 evaluation(s) from 13:02 UTC to 14:02 UTC (history), policy 9c1e04aa61f2
 TIME      | CHANGE | KIND    | SUMMARY
-----------+--------+---------+--------------------------------------------------
 13:31 UTC | 4 → 6  | applied | cpu 0.91 vs 0.70 (4 ready)
 13:44 UTC | 6 → 5  | applied | cpu 0.41 vs 0.70 (6 ready); rate-limited from 4
```

- **The target** is a job (its live policy), a jobspec file (resolved with
  `legion job plan`, so the layered defaults apply exactly as on submit), or
  a standalone policy file (resolved with `legion scaling apply --dry-run`).
- **History** is the hour of 10-second points the status-lane leader keeps
  for each task ([Scaling inputs](#scaling-inputs)), with the task's count,
  healthy and unplaced replicas, and the `legion` source's readings. Each
  check's readings are averaged over its `window` the way the live
  autoscaler averages them.
- **Two modes.**
  - `--replay` (the default for a job): the count is what the target really
    had, and readiness comes from its real healthy replicas through the
    warmup rule. The policy's own decisions spend the step limits when it
    makes them, and every other change of the count (a person's scale, a
    pin, the live autoscaler) when it is seen, as the live engine does. Run
    against the live policy, a replay makes the decisions the live
    autoscaler made in that hour (`legion scaling history`).
  - `--follow` (the default for a file and for `--series`): the count moves
    with the policy's own decisions, new replicas warm for `warmup`, and
    over history the `legion` utilizations are rescaled to the simulated
    count (the load is conserved: twice the replicas at half the
    utilization). This answers "what would this policy have done?".
- **The engine is warm** by default (one `down.stabilize` of samples behind
  it); `--cold` simulates a fresh start, with its scale-in hold.
- **CSV** (`--series`): a header with `time` (unix seconds or RFC 3339),
  optional `ready` and `total`, and one column per check by name; an empty
  cell is missing data. The readings are taken as the policy's (already
  averaged).
- `--all` shows every evaluation, held ones too, and `--json` the decisions
  with every stage.

What a simulation does not see: checks on sources other than `legion` over
history (they read as missing; supply them with `--series`), plugin
strategies (missing too), pins, pauses, rollouts, and the namespace quota.
History covers at most the last hour, from when the answering manager took
the status lane, and only tasks with usage or `autoscale`.

### Where it runs

The autoscaler is the bundled app `autoscaler`, the job
`legion-system/autoscaler` ([apps.md](apps.md)). It is enabled on every new
cluster, runs on the managers' system workers (so it runs with no worker
joined), and needs nothing configured: it reaches Legion through its
allocation's API socket as its own workload identity, which workload
identity being on by default makes available on a fresh cluster
([workload-identity.md](workload-identity.md)). Its grant is `scale:read`
and `scale:write` in every namespace: it can resize, not redeploy.

It watches policies (`WatchScalingPolicies`: a full set whenever a job,
a standalone policy, a pause, the defaults, a namespace policy, or its own
app config changes), reads every target's inputs
in one batched `GetScalingInputs` per interval, evaluates, writes, and
reports (`ReportScalingStatus`, which is also its heartbeat). Its config
(`legion app config set autoscaler -f config.ncl`):

| Field | Default | Meaning |
| --- | --- | --- |
| `interval` | `"15s"` | How often every policy is evaluated. |
| `mode` | `"enforce"` | `"dry-run"` forces every policy into dry-run: a cluster-wide trial. |
| `plugins` | `{}` | The installed sources, strategies, and targets beyond the built-ins ([Plugins](#plugins)). |

**When the app is gone, the manager says so.** With no report for three of
the app's intervals, `legion scaling status` reads
`autoscaler: NOT RUNNING since 14:05 UTC (app job legion-system/autoscaler:
active, 0 allocation(s) placed): nothing is being autoscaled`, and every
target reads `stale`. A disabled app reads `disabled`. Nothing scales while
the app is down; nothing else is affected.

## Plugins

Sources, strategies, and targets beyond the built-ins are **plugins**:
programs (or URLs) that speak a small JSON-RPC protocol, `legion-scaling/1`.
Writing one is in [autoscaling-plugins.md](autoscaling-plugins.md).

**The operator installs them** in the autoscaler's app config:

```nickel
# legion app config set autoscaler -f autoscaler.ncl
{
  plugins = {
    team-prom = {
      builtin = "prometheus",
      config = { address = "http://prometheus.internal:9090" },
      namespaces = ["payments-*"],      # only policies in these namespaces
    },
    calendar = {
      source = "https://example.com/calendar-strategy-0.1.0.tar.gz",
      checksum = "sha256:9f1c…",
      command = ["python3", "calendar_strategy.py"],
      config = { windows = [{ start = "02:00", end = "03:30" }] },
    },
    forecast = { url = "http://forecast.internal:8080/rpc" },
  },
}
```

| Field | Meaning |
| --- | --- |
| `builtin` | `prometheus`, `http` (sources), `exec`, `noop` (targets) |
| `source` | an artifact URL; the app's job fetches it into its task directory at `code/plugins/<name>/` (archives unpacked) |
| `checksum` | `sha256:<hex>` of the artifact |
| `command` | the program and its arguments; with `source`, relative paths resolve in the plugin's directory. Default `./<name>` |
| `url` | a plugin served over HTTP: each request is a POST |
| `config` | the plugin's own settings, sent once at start (addresses, regions, request headers). Stored as written: anyone with `app:read` on the autoscaler can read it |
| `namespaces` | names or `*`-globs of the namespaces whose policies may use it; default every namespace |

Exactly one of `builtin`, `source`/`command`, or `url`. A plugin may not be
named `legion`, `noop`, or after a built-in strategy. A config with a
mistake (an unknown builtin, a missing `address`, a bad checksum) is
refused by `legion app config set`. Changing the plugins re-renders the
app's job, which restarts it. Every plugin field but `namespaces` is a code
field (`source`, `command`, `url`, `builtin`, and `config`, which holds an
`exec` target's command and a source's address): changing one changes what
the app runs or reaches, so it needs the app's whole grant
([apps.md](apps.md)).

**Policies name plugins**: a check's `source`, a strategy's `name`, a
standalone policy's `target.plugin`. A policy never carries a command or a
URL. That is the trust boundary:

- Plugins run inside the autoscaler, with its identity, which can resize
  every job. A tenant who could name a command could run code as that
  identity; one who could name a URL could aim the autoscaler's network
  position at internal endpoints. Tenants supply only data: a query, a
  target value, a config record checked against the plugin's schema.
- **A shared source answers any query its tenants write.** A tenant's
  Prometheus query runs against everything that Prometheus can see; its
  only output is a number that moves their own count, but it is a side
  channel. Give each tenant group its own source (with `namespaces`) if
  their metrics must not mix.

**At run time** the autoscaler starts each program plugin on first use and
keeps it running, with a 10-second timeout per call (120 seconds for
targets), restarts with backoff when it crashes or times out, and logs
calls with credentials redacted. Before a policy's config reaches a plugin
it is checked against the plugin's `policyConfigSchema`; a policy that
fails it, or that uses a plugin as a kind it does not implement, is held
`invalid`, with the reason in `legion scaling explain` and a
`scale.check_failed` event. A plugin that is not answering makes its checks
missing data (a source, a strategy) or its target `error`.

## Standalone policies

A policy whose target is not a Legion task is its own object: `legion
scaling apply` stores it, versioned, on the Control lane, and the
autoscaler runs it next to the task policies.

```nickel
# legion scaling apply render-farm.ncl
{
  name = "render-farm",
  namespace = "media",                 # default: -n, else `default`
  description = "GPU render workers",
  target = { plugin = "render-exec", config = { pool = "gpu" } },
  min = 0,
  max = 50,
  checks = [{
    name = "queue", source = "team-prom", query = "sum(render_queue_depth)",
    strategy = { name = "per-replica", value = 10 }, activation = 0,
  }],
  down = { stabilize = "10m" },
  mode = "dry-run",
}
```

```console
$ legion scaling apply render-farm.ncl --dry-run
scaling policy media/render-farm: would be created; dry run, nothing written
$ legion scaling apply render-farm.ncl --wait
scaling policy media/render-farm: created (v1)
autoscaler: dry-run: queue 120 / 10 per replica
$ legion scaling list -n media
 POLICY                 | KIND     | NAMESPACE | MODE    | BOUNDS | TARGET
------------------------+----------+-----------+---------+--------+--------------------
 media/encode/default/x | task     | media     | enforce | 2..8   | job media/encode
 media/render-farm      | external | media     | dry-run | 0..50  | plugin render-exec
$ legion scaling explain render-farm -n media --standalone
$ legion scaling history render-farm -n media --standalone
$ legion scaling pause render-farm -n media --standalone --for 1h
$ legion scaling delete render-farm -n media
```

- **The record** is the `autoscale` record (checks, `up`, `down`, `headroom`,
  `warmup`, `tolerance`, `window`, `schedule`, `onMissing`, `mode`) plus
  `name`, `namespace`, `description`, and `target`. `min`, `max`, and
  `checks` are required: `min` and `max` are replica counts (there is no
  `count` for `"Nx"`), and `cpu`, `memory`, and `legion` queries read a
  task's own usage, which an external target has none of. The fields it
  leaves out take the layered defaults like a task's (cluster defaults,
  then the namespace's policies), live.
- **The target** is a target plugin: `noop` (built in: it records
  decisions and events and changes nothing), an `exec` builtin, or an
  installed plugin. `target.config` is passed to it as data. Each
  evaluation asks the target for its count (`target/status`); a change is
  recorded in the policy's history first (so a pause refuses it on the
  server) and then made (`target/scale`). A failed `target/scale` adds a
  `failed` history row and reports `error`.
- **`--dry-run`** validates, resolves, and diffs against the stored version
  (`max: 10 -> 20`) without writing. Applying an unchanged file writes
  nothing and keeps the version. **`--wait`** waits until the autoscaler
  has evaluated the new version and reports what it found: the plugin's
  kind and schema are checked only there.
- **History, pauses, status** work as for tasks, with `--standalone` on
  `explain`, `history`, `pause`, and `resume` naming the policy instead of
  a job. `legion scaling status` lists standalone policies with tasks
  (kind `external`). `pause --all` covers them too. Deleting a policy
  deletes its pause; its history stays until the cap ages it out.
- **Node groups** (`target = { nodes = … }`) are standalone policies too,
  cluster-scoped, needing `operator:write` and `node:write`. They are not
  run yet: `scaling apply` refuses one, and `legion scaling demand -f`
  sizes it ([Capacity demand](#capacity-demand)).

The `exec` builtin runs one command per operation, volume-driver style: the
operation (`status`, `scale`) appended to the command (a script's `$1`), a JSON request on
stdin (`{ "apiVersion": 1, "op": "scale", "policy": "media/render-farm",
"config": { "pool": "gpu" }, "desired": 9, "decision": "d-81f3a2c0" }`), and
a JSON reply on stdout (`status` answers `{ "count": 7, "ready": 7 }`). The
command comes from the app config, never the policy.
## Scaling by hand

`legion job scale` changes how many copies of a task run. It does not create a
new job version or start a rollout. For nodes, `GetCapacityDemand` (and
`legion scaling demand`) answers how many nodes of a given shape the work the
scheduler could not place needs ([Capacity demand](#capacity-demand)).

### Why it exists

Editing `count` in a jobspec and resubmitting is a deploy. It creates a new
job config version, cancels any in-flight rollout, and starts a fresh one. If
you just want two more replicas during a traffic spike, you shouldn't get a
new version and a deployment. `job scale` instead writes a **count override**,
a separate record layered over the spec, and reconciles the running
allocations to match it.

### The surface

```
$ legion job scale web --count 6 --reason "launch traffic"
web/default/web: 4 -> 6 (spec count 2; override active)

$ legion job scale web --count 6          # idempotent re-issue
$ legion job scale web                    # no --count: print scale status
$ legion job scale web --reset            # drop the override, return to spec count
$ legion job scale multi --task api --count 3   # disambiguate multi-task jobs
```

- `--task` / `--group` are optional when the job has exactly one scalable
  task (the common case). If it has several, the error lists the candidates.
  Naming any task of a group whose task is autoscaled targets the group.
- `--reason` is recorded on the override, in the history, and in the event
  stream.
- `--for`, `--min`, `--max`, and `--release` are for autoscaled tasks
  ([Pins](#pins-a-manual-scale-on-an-autoscaled-task)).
- `--json` / `-j` emits the standard envelope. `--idempotency-key` makes the
  RPC retry-safe (`deduplicated = true` on replay).
- The same operation is available over gRPC (`ScaleTask`) and MCP
  (`legion_job_scale`, write-gated). The job page in the UI has a **Scale**
  card with the same per-task view and Scale / Reset controls
  ([UI architecture](ui-architecture.md)).

`legion job scale <name>` (no `--count`) shows the effective count per
task: `web/default/web: count 6 (spec 2, scaled by operator)`, and for an
autoscaled task its policy, pin, and pause.
`legion job info` shows each task's ready and placed replicas, how many could
not be placed, and its measured usage against its reservation
([Scaling inputs](#scaling-inputs)).

### Semantics

The **effective desired count** is the override if one exists and binds to
the current spec (see [Ownership](#ownership-overrides-vs-resubmits)),
otherwise the spec's `count.num`. Everything that reads desired count uses the
effective value through one accessor: the scheduler's plan paths, slot-mode
stateful retirement, rollout group plans, quota, `GetJobScaleStatus`, and
`GetScalingInputs`.

- **Scale-out** enqueues a `job_scale` eval that plans in reschedule mode.
  Only the shortfall is placed, and existing allocations are untouched.
- **Scale-in** retires surplus allocations. Slot-mode stateful tasks lose
  their highest slot indices first (honoring
  `stateful.storage.retainOnScaleIn`). Stateless tasks lose their newest
  allocations first.
- **Keyed stateful tasks** are rejected because they have no count. Use the
  instance API instead. Tasks with `count = { per = ... }` are also rejected.
- Every write gets a new **version**, a cluster-wide counter that never
  repeats; `ScaleTaskRequest.expected_version` makes a write
  compare-and-set against it.

### Ownership: overrides vs resubmits

For a task without `autoscale`, an override records the spec count it was
laid over (`base_count`). When you resubmit the job:

- If the new spec count **equals** `base_count`, you didn't touch count, so
  the override survives the deploy.
- If the new spec count **differs**, the spec expresses new intent. The
  override is cleared and the spec wins. The submit reply tells you this
  happened.

So your scale sticks across deploys, and editing the spec still does what it
says. This is a simplified version of Kubernetes' scale-subresource field
ownership. It avoids Nomad's behavior of silently resetting count on
resubmit. An autoscaled task follows [its own rule](#who-owns-the-count).

### Rollout interaction

Legion follows the ECS rule: **scale-out applies during a rollout, scale-in
waits.** Rollout ticks recompute their targets from the effective count, so a
scale-out mid-rollout grows the step targets on the next tick. A scale-in
during an active rollout is recorded but only applied when the rollout
completes or is cancelled:

```
$ legion job scale web --count 2
deferred: rollout web-7 is in progress; scale-in to 2 applies when it completes
```

The autoscaler follows the same rule, and its own pipeline defers scale-in
during a rollout before it ever writes.

A resubmit that only changes `count` (with no override involved) still
creates a new version and a rollout, unless the task is autoscaled. Spec
changes make versions, and `job scale` doesn't.

For the same reason, scaling does **not** change the job's `updated_at`. The
jobs list's "Updated" column only tracks spec submissions and status
transitions (see [jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md)).
Scale activity shows up elsewhere: the events, the history
(`legion scaling history`), and the override's timestamp in
`legion job scale <name>`.

## Scaling inputs

A scaling decision needs to know how busy a task is across the whole
cluster. Workers measure usage and service traffic and send it on their
heartbeats, but a worker talks to one manager, and each manager only hears its
own workers. `GetScalingInputs` is the one read that sees all of it, from any
manager. `legion job info` shows the same numbers per task:

```
$ legion job info shop
...
 TASK        | REPLICAS               | CPU USED / RESERVED              | MEMORY USED / RESERVED  | REQUESTS                     | DATA
-------------+------------------------+----------------------------------+-------------------------+------------------------------+------------------
 default/api | 4/4 ready, 1 unplaced  | 5760 / 4800 MHz (120%, max 180%) | 640 / 1024 MiB (62%)    | 40.0 rps, 20.0% err, 20ms avg | current (3s ago)
```

`legion job info -j` adds the same data as `scalingInputs`, the MCP tool
`legion_job_info` returns it too, and the job page in the UI shows it in a
**Request vs usage** card ([UI architecture](ui-architecture.md)). The table,
the key, and the card are left out when the caller lacks `scale:read`.

### What it returns

One entry per task:

| Field | Meaning |
| --- | --- |
| `desired_replicas` | The effective count: the count override if one binds, else the spec's `count`. 0 for a stopped or failed job. |
| `placed_replicas`, `ready_replicas` | Allocations placed, across every version of the job, and those whose latest health report is healthy. |
| `unplaced_replicas` | `desired - placed`, floored at 0. Always 0 for keyed and `count.per` tasks, and for batch and cron tasks. |
| `rollout_active` | Whether a rollout of the job is in flight. |
| `reservation` | One replica's `cpu` MHz (or `cores`) and `memory` MiB, after `taskResourceDefaults`. |
| `cpu` | Measured millicores and MHz summed over the replicas that reported, their reserved MHz, mean utilization (sum of used over sum of reserved), the busiest replica's utilization, and a per-replica histogram. |
| `memory` | Measured MiB (`memory.current`) against reserved MiB, and the largest single replica's ratio. |
| `requests` | For a task with a `service` block: requests per second, errors per second, error ratio, mean and max latency, summed over every calling node. |
| `freshness` | `state` (`current`, `missing`, `resyncing`), the newest worker sample time, the age of the oldest confirmation, and how many nodes reported. |
| `history` | With `history_seconds`, up to an hour of 10-second totals. |

The CPU arithmetic is `GetNodeUsage`'s. Workers report millicores, and a
replica's MHz is millicores / 1000 x its node's `mhz_per_core`. Utilization
divides in millicores: a `cpu` reservation in MHz becomes millicores through
the node's clock, and a `cores = N` reservation is N x 1000 millicores on any
node. A replica on a node that reports no clock speed (Apple Silicon) still
counts in `used_millicores`, but an MHz reservation there cannot be turned
into a utilization, so it is counted in `unmeasurable_replicas` and left out
of `utilization` and the histogram. The histogram buckets are fixed at
utilization bounds 0.1, 0.25, 0.5, 0.7, 0.85, 1.0, and 1.5, plus one bucket
for everything above.

Ask for several tasks at once, or for every job in some namespaces:

```bash
grpcurl -d '{"targets":[{"job":"shop"},{"job":"payments/ledger","taskName":"db"}]}' \
  ... legion.Manager/GetScalingInputs
grpcurl -d '{"namespaces":["*"],"historySeconds":600}' ... legion.Manager/GetScalingInputs
```

A selector that names no job or task yields an entry with `error` set (`job
not found`, `no task matches ...`). It does not fail the batch. At most 1000
selectors fit in one call.

### Where the numbers come from

Usage never touches the state store. Every 10 seconds each manager folds the
latest usage report of every node it hears from, and the service-graph
samples its workers sent since the last fold, into one rollup per
(node, task). A rollup holds sums, maxima, and the utilization histogram, and
is stamped with the worker's sample time. A manager that does not lead the
status lane sends the rollups that changed to the leader, on the liveness
digest RPC ([node-liveness.md](node-liveness.md#managers-that-do-not-own-liveness)).
The leader keeps the newest rollup per (node, task), whichever manager sent
it. A node that moves to another manager is therefore never counted twice: the
newer sample replaces the older one. The leader adds them up per task when
asked, and keeps an hour of 10-second totals in memory. Other managers
forward `GetScalingInputs` to it, so every manager answers the same.

A single manager does all of this in memory, with no RPC.

### Freshness

The numbers trail the workers by at most one report interval plus one rollup
interval, about 20 seconds.

- **`current`**: the task's rollups are in hand, and every manager holding
  one confirmed it within the last 35 seconds. `age_seconds` says how long
  ago the oldest confirmation was, and is usually under 10.
- **`missing`**: nothing reports usage for the task. It has no replicas, its
  runtime reports no usage (`noop` and third-party runtimes; see
  [resource-enforcement.md](resource-enforcement.md#measuring-usage)), its
  workers run with enforcement off, or the managers that held its rollups
  stopped confirming them. A node whose latest report is older than 30
  seconds contributes nothing.
- **`resyncing`**: the answering manager took the status lane less than one
  rollup interval (10 seconds) ago.

Treat `missing` and `resyncing` as "no data", not as zero load.

### Failover

Nothing here is replicated, so a manager that takes the status lane starts
empty. Each other manager's next rollup sync is a delta the new leader cannot
apply. The leader answers it with a resync request, and the manager sends
its full set at once. Until every manager has ticked once (up to 10
seconds), targets read `resyncing`. The new leader's history starts at the
takeover; `leader_for_seconds` in the reply says how long ago that was.

When a manager stops sending rollups (it crashed, or it cannot reach the
leader), the leader drops its rollups after 35 seconds. Its nodes' workers
fail over to another manager, which reports them from then on.

### Requests count internal callers only

Request rate, errors, and latency come from the service graph
([network-handlers.md](network-handlers.md#service-graph)). Edges are
recorded on the calling side, so only callers inside Legion that go through
the datapath are counted. Traffic from outside the cluster into an advertised
port, and traffic that bypasses the datapath in `host` mode, are invisible
here. A service backed by several tasks reports the service's totals on each
of them. Requests are attributed through the caller's declared upstream key,
or as a service in the caller's own namespace when it calls without
declaring; calls to external hosts are not counted.

### Access

`scale:read` on each job, the same grant as `legion job scale <name>`. The
request is gated before anything is looked up. A request that names a job
outside the caller's grant is refused whole with `PermissionDenied` (not
`job not found`). A namespace listing leaves such jobs out.

## Capacity demand

`GetCapacityDemand` answers the question node autoscaling starts from: *how
many nodes of this shape would the work that is waiting need?* It is a
read-only simulation on whichever manager answers, computed off the
scheduler's path, and with the scheduler's own placement rules, so it cannot
disagree with the scheduler about what fits.

```
$ legion scaling demand --name general --cpu 4000 --memory 8192 --attr zone=a
GROUP                 NODES  REPLICAS  ABSORBED  CPU (req/cap)          MEMORY (req/cap)
general                   3        12         0  10100/12000 MHz        7744/24576 MiB

Pending work: 17 replica(s) in 6 task group(s)
  huge (huge): 1 pending (0/1 placed), each {"allocations":1,"cpu":1000,"memory":65536}
  mpi (mpi): 4 pending (0/4 placed, gang quorum 4), each {"allocations":1,"cpu":1000,"memory":512}
  ...

Unfittable (no template can host them):
  huge (huge): 1 replica(s)
    general: resources: one replica needs memory 65536 > 8192 per node
  spread (spread): 2 replica(s)
    general: distinct: distinct zone leaves no value for more replicas: every new node of this template has the same value
  train (train): 2 replica(s)
    general: permit: task train's permit does not tolerate pool "default"
```

Several templates at once, from a file (Nickel or JSON): a node-group policy
(its `name` and `target.nodes` are the template), a bare template, or a list
of either:

```nickel
# groups.ncl  —  legion scaling demand -f groups.ncl -j
[
  { name = "general", workerType = "default", shape = { cpu = 8000, memory = 16384 } },
  { name = "gpu", pool = "gpu", shape = { cpu = 16000, memory = 65536,
                                          attributes = { accel = "a100" },
                                          resources = { gpu = 4 } } },
  { name = "like-web-3", likeNode = "web-3", upcoming = 2 },
]
```

### The pending work

Every task of an active job that is short of its effective count (the count
`legion job scale` set, or the spec's), counted against its allocations in
every config version, plus keyed instances that have no allocation. A task
group's tasks place together, so a group is one unit: its replicas reserve
what its short tasks reserve together.

Left out, and why:

- **Jobs whose eval is still pending or in flight.** The scheduler has not
  tried the capacity that exists yet, so they are listed under
  `awaiting_scheduler` rather than counted. A blocked eval (no room
  anywhere) is exactly what this is for.
- **`count.per` tasks**, which follow nodes rather than ask for them, **cron
  tasks**, and **batch tasks that already finished**.
- **Free room on existing nodes.** The work is pending because the scheduler
  already found no room for it; capacity demand sizes new nodes only.

### The template

Each template is one node group: a name, a worker type (default `default`),
an optional pool, a shape, and optionally an existing node to copy. The
manager builds the node a new member would register as: the worker type's
pool, `namespaces`, attributes, custom `resources`, `reservedResources`,
`maxAllocations` and oversubscription, then the shape on top (`cpu` and
`memory` become its capacity, attributes and resources are added), then
`legion.nodegroup = <name>`, which every member of a node group carries
([Node attributes from the join](auth-and-acls.md#node-attributes-from-the-join)).
`likeNode` copies a member's observed capacity and attributes instead, which
is a better template than a declared shape once one member has joined; a
shape on top of a copy can only lower it. The reply echoes the node it built
(`node_pool`, `node_attributes_json`), so you can see what the manager
understood.

### How it packs

The pending replicas are packed onto copies of each template's node, first
fit decreasing (the largest share of a node first), with the scheduler's own
functions:

| Rule | What refuses |
| --- | --- |
| `permit` | the task's permit does not tolerate the template's pool or restrictions |
| `namespace` | the template's nodes accept only some namespaces ([Namespaces](namespaces.md)), not the job's |
| `constraint` | a constraint does not hold on the template's attributes (the reply names it: `accel = "a100"`) |
| `volume-driver` | the template's nodes lack a volume driver the task needs |
| `resources` | one replica is larger than an empty node (`one replica needs memory 65536 > 8192 per node`) |
| `distinct` | `distinct` leaves no value for more replicas: every new node of one template has the same attributes, so a `distinct` key the template sets hosts one replica |
| `static-port` | two tasks of one group claim the same static port, so they can never share a node (one replica per node per port is packed as such) |
| `gang` | a gang's missing quorum places on one template together, or not at all |
| `invalid` | the task's resources, constraints or permit do not parse |

**Upcoming nodes** (`upcoming`, members asked for that have not joined)
absorb pending work first, across every template, so a group that was just
asked to grow is not asked again for the same replicas. Then **each template
is answered on its own**: "if only this group grew, how many nodes would the
work it can host need?" Choosing between groups that could host the same work
is the caller's job.

### What it returns

- Per template (`groups`): `nodes` (the `nodegroup.pending` input), the
  `replicas` they would host, what its upcoming nodes `absorbed`, the CPU and
  memory those replicas reserve against what the new nodes offer (the inputs
  a least-waste choice between groups needs), and `refusals`: the pending
  work it cannot host, with the rule.
- `pending`: the work considered, one entry per task group, with desired,
  placed and pending counts, a gang's quorum, what one replica reserves, and
  what upcoming nodes absorbed.
- `unfittable`: replicas no single template can host, with every template's
  rule. That is a jobspec or template problem, not a capacity problem: no
  number of new nodes of these shapes will place them.
- `awaiting_scheduler`, and `truncated` when the pending work is larger than
  one call simulates (20,000 replicas; node counts are then a floor).

The same read is the MCP tool `legion_scaling_demand` (read-only).

### Access

`node:read`, checked before anything is read, and then on each template's
pool (the pool the built node ends up in). Node counts cover all pending
work, but a `pending` or `unfittable` entry names its job, group and tasks
only when the caller has `job:read` on that job; others keep their counts and
rules and read `redacted = true`, and `awaiting_scheduler` leaves them out
and counts them in `awaiting_scheduler_redacted`.

### Limits

- It is approximate, on the safe side. First fit is not the scheduler's
  scoring, and preemption, disk for node-local volumes, and checkpoint
  headroom are not modelled, so it can over-ask by a node. It never counts
  room on existing nodes.
- A rollout that is stuck for room shows no demand: during a rollout the old
  version's allocations still count as placed.
- A task group with one short member is sized as if the group's short tasks
  move together, which is what a group placement needs.
- It is a point-in-time read with no memory: a caller that wants
  stabilization keeps it.
- Nothing calls it on a schedule yet. The node-group policy shape and its
  decision engine exist as a library (`src/autoscaler/nodes.rs`); running node
  groups (minting tokens, calling a provider, draining and forgetting nodes)
  is not built.

## Access control

Scaling has its own ACL resource: **`scale`** (actions `read`/`write`, scoped
by job name). `job:write` does not imply `scale = "write"`, and the reverse is
also true. This lets you give an autoscaler or a teammate the right to resize
a job without the right to redeploy it. See [auth-and-acls.md](auth-and-acls.md).

| RPC | ACL |
| --- | --- |
| `ScaleTask` (`legion job scale`) | `scale:write` on the job |
| `GetJobScaleStatus`, `GetScalingInputs` | `scale:read` on the job |
| `WatchScalingPolicies` | `scale:read`; jobs and policies outside the grant are left out |
| `GetScalingStatus`, `GetScaleHistory` | `scale:read` on the job(s) or the standalone policy |
| `PauseScaling` | `scale:write` on the job or the standalone policy; `operator:write` for `--all` |
| `ApplyScalingPolicy` (`legion scaling apply`) | `scale:write` on the policy's namespace and name; a node group: `operator:write` and `node:write` |
| `DeleteScalingPolicy` | `scale:write` on the policy, checked before the lookup |
| `ListScalingPolicies` | `scale:read`; policies outside the grant are left out |
| `GetScalingDefaults` | `scale:read` on the namespace |
| `ApplyScalingDefaults` | `operator:write` |
| `ReportScalingStatus`, `RecordScalingAction` | app-plane: only the bundled autoscaler's own identity (or a management token) |

A standalone policy is scoped like a job of the same name in its
namespace: a rule with `names = ["render-*"]` on `scale` covers the
`render-farm` policy. Using a plugin is not an ACL: the plugin's
`namespaces` allowlist in the autoscaler's app config decides which
namespaces' policies may name it ([Plugins](#plugins)), and installing one
is changing that app config ([apps.md](apps.md)).

The app-plane RPCs accept a credential only when its job grant belongs to an
allocation of `legion-system/autoscaler`, so a person with `scale:write`
cannot forge the app's status or its dry-run records. Likewise
`ScaleTask`'s `source = "autoscaler"` is honored only from that identity;
from anyone else the write is recorded as `operator` (and is then a pin on
an autoscaled task, which needs `--for`). With ACLs disabled every caller
is trusted, as everywhere else.

## Observability

- Events: `scale.applied`, `scale.deferred`, `scale.reset`,
  `scale.pinned`, `scale.paused`, `scale.resumed`, `scale.limited`,
  `scale.flapping`, and `scale.check_failed` in the event stream
  ([event-stream.md](event-stream.md)). A standalone policy's events carry
  `policy` in their metadata instead of a job. The event journal is capped
  and best-effort; the durable record is the history.
- The autoscaler's own log: `Scaling plugin started` (name, version,
  kinds) on every start of a plugin program, `Scaling plugin call failed`
  on a timeout, crash, or malformed reply, and each call at `debug` with
  credentials redacted (`legion app logs autoscaler`).
- `legion scaling status | explain | history` and `legion job scale <name>`
  (see above).
- `legion_scaling_rollup_syncs_total{outcome}` counts each manager's rollup
  ticks (`local` on the status-lane leader, `ok`, `resync`, `error`
  elsewhere), and `legion_scaling_rollup_rows{kind}` the rollups it built
  (`local`) and holds from every manager (`merged`, on the leader)
  ([metrics.md](metrics.md#scaling-inputs)).
- Allocations retired by a scale-in report the `scale_in` stop cause to the
  worker that ran them. They also carry it in `allocation.removed` event
  metadata ([graceful-stop.md](graceful-stop.md)). A task can use this to
  tell "my replica count shrank" apart from "I am being replaced by a new
  version".

## Failure modes

- **Unknown job / task**: `not_found` / `invalid_argument`, listing the
  candidates.
- **Keyed or `count.per` task**: `invalid_argument` with the reason (and
  `autoscale` on one is refused at submit).
- **A manual count on an autoscaled task without `--for`**:
  `failed_precondition` with the pin hint.
- **No capacity for the new count**: the `job_scale` eval places what fits,
  and the override stays. The rest is placed when capacity appears: a worker
  registering or a node being uncordoned queues a capacity pass that
  reschedules jobs short of their count
  ([jobs-tasks-and-scheduling.md](jobs-tasks-and-scheduling.md#node-added)).
  Capacity freed by other jobs stopping does not trigger it. While more than
  half of its last scale-out is unplaced, the autoscaler holds further
  scale-out (`limited`).
- **The autoscaler is not running**: `legion scaling status` says `NOT
  RUNNING` and since when, with the app job's state. Check `legion app
  status autoscaler`: a disabled app, no node in pool `legion-system` (the
  system worker turned off), or no credentials (`workloadIdentity.enabled =
  false` on the managers leaves the app with no way to reach Legion; its
  health says `idle: cannot reach Legion`).
- **A policy reads `invalid`**: a defaults change applied with `--force`, or
  two namespace policies that disagree. `legion scaling explain` prints the
  reason; fix the task or the defaults.
- **A target reads `missing-data`**: its runtime reports no usage, or the
  rollups are stale; see the scaling inputs failure modes below. Nothing
  scales in while it lasts (scale-out on other checks still can).
- **A write reads `held` with `write refused: version conflict`**: someone
  else changed the count between the read and the write; the next
  evaluation re-reads. `paused` or `pinned` in the message: a pause or a pin
  arrived in between.
- **Manager is not the scheduler-lane leader**: the manager forwards the
  request to the leader and returns its answer. Only when that leader cannot
  be reached does the caller get the standard retryable
  `failed_precondition` envelope with the leader hint. `GetScalingInputs`,
  `GetScalingStatus`, and `ReportScalingStatus` are answered by the
  status-lane leader the same way.
- **A check reads `missing data` with a plugin's message**: the source timed
  out (10 seconds), answered an error, or returned no data; a Prometheus
  query that returns several series says so (`the query returned 3 series;
  a check needs one number. Hint: aggregate it, as in sum(...) or
  max(...)`), and an `http` source names the JSON pointer
  that found nothing. Nothing scales in while it lasts.
- **A policy reads `invalid` naming a plugin**: the plugin is not installed
  (the app config changed after the submit), is closed to the namespace,
  does not implement the kind the policy uses, or the policy's config does
  not match the plugin's `policyConfigSchema`. `legion scaling explain`
  prints which.
- **A plugin that keeps failing**: it is restarted with backoff (up to a
  minute between starts); its last stderr lines are in the error. Test it
  with `legion scaling plugin test`.
- **A standalone policy reads `error`**: its target's `target/status` or
  `target/scale` failed. A failed scale is a `failed` row in `legion scaling
  history --standalone NAME`; the next evaluation asks the target again.
- **`legion scaling apply` refuses a node group** (`failed_precondition`):
  node-group policies are not run in this version.
- **Inputs that read `missing` for a task that is running**: its runtime
  reports no usage (`noop`, third-party runtimes), its workers have
  `resourceEnforcement` off, or the manager its workers report to cannot
  reach the status-lane leader. `legion_scaling_rollup_syncs_total{outcome="error"}`
  rising on that manager says which.

## Known limitations

- **No wake on request.** A `min = 0` service wakes only from a check (a
  queue, a schedule); its first request is refused, not held, while it is
  at zero.
- **Plugins run inside the autoscaler.** They share its single process and
  its identity's network position; a slow plugin delays the evaluations
  that use it (each call is bounded by its timeout). Calls to one program
  plugin are serialized.
- **Simulation sees only Legion's history.** Over history, checks on other
  sources and plugin strategies read as missing (supply them with
  `--series`), and pins, pauses, rollouts, and quotas are not replayed.
- **The `noop` target keeps its count in the autoscaler's memory**: it
  restarts at the policy's `min`.
- **No secret references in plugin config.** A Prometheus `Authorization`
  header or a plugin's API key in the app config is stored as written and
  readable with `app:read` on the autoscaler; the autoscaler redacts it
  from its own logs only.
- **No node autoscaling.** A scale-out that does not fit waits for capacity
  like any other placement. `GetCapacityDemand` (`legion scaling demand`)
  reports how many nodes of a given shape would host it, and join tokens
  take `max_uses` and `attributes` for node groups, but nothing acts on
  that demand yet: `legion scaling apply` refuses a node-group policy.
- **Governed runtimes only.** CPU and memory come from `exec`, `container`,
  and `sandbox` tasks on workers with resource enforcement on.
- **Requests count internal callers only**
  ([Requests count internal callers only](#requests-count-internal-callers-only)),
  and there is no in-flight request (concurrency) signal.
- **The quota guard is the write.** A scale-out the namespace quota cannot
  admit is refused by `ScaleTask` rather than capped ahead of time, so the
  target reports `held` with the quota's message instead of scaling to
  exactly what fits.
- **One autoscaler replica.** When it restarts, its windows start empty: no
  scale-in for one `down.stabilize`, and the averaging windows refill.
  While it is down nothing scales.
- **Status lives in memory.** The last evaluation of each target, and the
  app's heartbeat, are held by the status-lane leader. A takeover starts
  empty and shows `starting`, then fills on the app's next report.
- **Group targets share one count.** A group's tasks cannot be autoscaled
  separately.
- Scaling inputs are held in the status-lane leader's memory. A takeover
  starts them empty for up to 10 seconds and starts the hour of history
  afresh. History is kept for at most 4096 tasks; past that, a task still
  has current inputs but no history.
- `count.per` tasks cannot be scaled.
