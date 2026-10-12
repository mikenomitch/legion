# Running Redis on Legion

Redis is often used for two different workloads. As a cache, it should be
cheap to lose: a replica dies, you take a cold start, and nobody gets paged. As
a store for sessions, queues, or rate limiter state, losing it is an outage.

These two uses need different things from a scheduler. This guide runs both and
shows the difference in the spec.

Runnable spec: [`configs/jobs/redis.ncl`](../../configs/jobs/redis.ncl).

## Run it

```bash
legion job run configs/jobs/redis.ncl
legion volume list --job redis
```

```
 VOLUME    DRIVER     JOB    INSTANCE       STATE    NODE
 4471be…   local-dir  redis  redis-store-0  created  node-a1*
```

There is one volume, for the durable task. The cache tasks have none.

```bash
legion service resolve redis        # the durable one
legion service resolve redis-cache  # the pool
```

## The durable one

```nickel
{
  name = "store",
  runtime = "exec",
  count = 1,
  group = "store",

  stateful = {
    mode = "slot",
    storage = { path = "/var/lib/redis", driver = "local-dir", params = { size = "5G" } },
  },

  config = {
    command = [
      "sh",
      "-c",
      "exec redis-server --dir \"$LEGION_STORAGE_DIR\" --appendonly yes --appendfsync everysec --port \"$LEGION_PORT_REDIS\"",
    ],
  },
  network = { mode = "auto", ports = [{ name = "redis", to = 6379 }] },
  service = { name = "redis", port = "redis" },
  checks = [{ type = "health", name = "ping", kind = "tcp", port = "redis", every = "10s", timeout = "3s" }],
}
```

**Run the command through `sh -c`.** The `exec` runtime runs `command` as an
argv, not through a shell, so `$LEGION_STORAGE_DIR` would reach `redis-server`
as literal text. `sh -c` expands the env Legion sets for the task. `exec`
replaces the shell, so signals reach Redis directly.

**Use a TCP health check.** Exec checks run on the worker without the task's
env or a shell, so `redis-cli -p $LEGION_PORT_REDIS ping` would not expand the
port either. A `tcp` check on the `redis` port needs neither.

**Use `$LEGION_STORAGE_DIR` instead of `path`.** This is an `exec` task. It
runs in the host mount namespace, so there is nothing to mount
`/var/lib/redis` into. Legion exports the host path as `LEGION_STORAGE_DIR`
(and `LEGION_VOLUME_DATA`), and records the `path` field as intent. A
`container` task would use the path directly. See
[docs/volumes.md](../volumes.md#how-path-works-per-task-runtime).

**Set `--appendonly yes`.** Without it, Redis persists only through periodic
RDB snapshots. A restart then silently drops everything written since the last
save, and the volume does little good. `everysec` fsyncs once a second. You can
lose up to a second of writes on a hard kill, but Redis avoids an fsync on every
command. If that second matters, use `--appendfsync always`, which is durable
but slower.

**Set `count = 1`.** Two Redis processes on one AOF directory corrupt it. Slot
mode guarantees at most one live allocation per instance, even across a network
partition (see
[Node loss and fencing](../stateful-workloads.md#node-loss-and-fencing-at-most-one)).

### Verify it persists

```bash
redis-cli -h <addr> -p <port> SET greeting hello
legion job run configs/jobs/redis.ncl --force-reschedule
# wait for it to come back
redis-cli -h <addr> -p <port> GET greeting   # "hello"
```

## The cache

```nickel
{
  name = "cache",
  runtime = "exec",
  count = 2,
  group = "cache",
  # No `stateful` block at all.
  config = {
    command = [
      "sh",
      "-c",
      "exec redis-server --save '' --appendonly no --maxmemory 256mb --maxmemory-policy allkeys-lru --port \"$LEGION_PORT_REDIS\"",
    ],
  },
}
```

**Leave out the `stateful` block.** A cache does not need a stable identity.
Without the block it gets no volume, no node pin, and no fencing window, so
Legion replaces a lost replica in seconds instead of waiting out
`onLost.replaceAfter`. Stateless failover is faster because nothing has to be
proven dead first.

Scale it freely:

```bash
# bump count, resubmit — new replicas start cold and warm up
legion job run configs/jobs/redis.ncl
```

### If the cache needs scratch space

Some workloads need a big scratch area but not durability. Claim one under
`volumes` instead of `stateful.storage`:

```nickel
volumes = [{ name = "scratch", driver = "tmpfs", params = { size = "1G" },
            path = "/scratch" }],
```

`tmpfs` is RAM-backed, wiped on unmount, and Linux only. It advertises itself as
ephemeral, so admission rejects it under `stateful.storage`. That field promises
data survives a reschedule, and a tmpfs cannot keep that promise. Trying it
produces:

```
task cache stateful.storage: volume driver `tmpfs` is ephemeral (its contents do
not survive an unmount), so it cannot back stateful.storage — which promises an
instance's data survives reschedules. Declare it under `volumes` instead if you
want scratch space
```

A `tmpfs` claim is shared per task, so two allocations on one node share it. For
private per-allocation scratch, use `$LEGION_TASK_DIR`. It already exists and
Legion cleans it up on stop.

## Day-2 operations

The store task declares [actions](../exec-and-actions.md). Exec-runtime actions
run as sibling host processes with the task's env, so `$LEGION_PORT_REDIS`
resolves without you having to look up the port:

```bash
legion action run redis cli --tty                    # interactive redis-cli
legion action run redis command --param cmd='INFO keyspace'
legion action run redis save                         # BGREWRITEAOF now
```

The `command` action limits its param to Redis-safe characters, because the
param ends up inside a shell.

## Choosing between them

| | Cache | Store |
| --- | --- | --- |
| `stateful` block | none | `mode = "slot"` |
| Volume | none (or a `tmpfs` claim) | `local-dir` / `nfs` |
| Persistence | `--save ''`, `--appendonly no` | `--appendonly yes` |
| Eviction | `maxmemory` + `allkeys-lru` | none (evicting stored data is data loss) |
| Failover | seconds | waits out the fencing window |
| `count` | as many as you want | 1 per data directory |

If you are unsure which one you have, consider what happens when the process is
killed and comes back empty. If the answer is "some extra database load", it is a
cache. If it is "users are logged out" or "we lost the queue", it is a store.

## Surviving a node loss

`local-dir` pins the store to its node. If that node dies, the instance waits
instead of starting empty somewhere else. To let it move, put the data on
network storage:

```nickel
storage = {
  path = "/var/lib/redis",
  driver = "nfs",
  params = { server = "10.0.0.5", export = "/exports/redis" },
}
```

Redis on NFS is workable because its write pattern is append-then-rewrite, not
random O_DIRECT I/O. Measure it, though: `appendfsync everysec` over a slow
export shows up as latency. If you need to survive a node loss and recover
quickly, use Redis replication instead (see below).

## Replication and Sentinel

For a replicated setup, run the primary and replicas as separate tasks in one
group. The replica task points at the primary's service address:

```nickel
config = {
  command = ["sh", "-c", "exec redis-server --dir \"$LEGION_STORAGE_DIR\" --replicaof redis.service.legion 6379"],
},
```

Each replica is its own slot instance with its own volume, so a restarted
replica resyncs from local data instead of a full transfer.

Automatic promotion needs Sentinel (or a client library that handles it).
Sentinel itself is a stateless task with `count = 3`. It needs no volume, only
quorum. Legion's [KV store](../kv-store.md) can hold the current primary
address for clients that would rather resolve than gossip.

## Troubleshooting

**Data disappeared after a restart.** Check that `--appendonly yes` is set. This
is the most common cause. Run `redis-cli CONFIG GET appendonly` to confirm.

**`redis-server` fails with a permissions error on the dir.** The worker's user
owns the volume directory. Either run the task as that user, or set
`params = { mode = "0777" }` on the volume (blunt, but fine for a dev cluster).

**The store instance will not place.** This is usually the node-local pin. See
[docs/volumes.md](../volumes.md#when-a-replica-will-not-place).

**The cache pool has fewer replicas than `count`.** This is not a volume issue.
Check `legion job list` and the eval output for capacity or constraints.

## See also

- [docs/volumes.md](../volumes.md): plugins, the contract, operating volumes
- [docs/stateful-workloads.md](../stateful-workloads.md): instances, fencing, retention
- [Running Postgres](postgres.md): the same machinery when durability is mandatory
