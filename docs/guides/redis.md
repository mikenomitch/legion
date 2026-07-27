# Running Redis on Legion

Redis is two different workloads wearing one binary. As a **cache** it should be
cheap to lose: a replica dies, you take a cold start, nobody pages. As a
**store** — sessions, queues, rate limiter state — losing it is an outage.

Those want opposite things from a scheduler, so this guide runs both and makes
the difference explicit in the spec.

Runnable spec: [`configs/jobs/redis.jsonnet`](../../configs/jobs/redis.jsonnet).

## Run it

```bash
legion job run configs/jobs/redis.jsonnet
legion volume list --job redis
```

```
 VOLUME    DRIVER     JOB    INSTANCE       STATE    NODE
 4471be…   local-dir  redis  redis-store-0  created  node-a1*
```

One volume, for the durable task. The cache tasks have none — that is the point.

```bash
legion service resolve redis        # the durable one
legion service resolve redis-cache  # the pool
```

## The durable one

```jsonnet
{
  name: 'store',
  kind: 'exec',
  count: 1,
  group: 'store',

  stateful: {
    mode: 'slot',
    storage: { path: '/var/lib/redis', driver: 'local-dir', params: { size: '5G' } },
  },

  config: {
    command: [
      'redis-server',
      '--dir', '$LEGION_STORAGE_DIR',
      '--appendonly', 'yes',
      '--appendfsync', 'everysec',
      '--port', '$LEGION_PORT_REDIS',
    ],
  },
}
```

**`$LEGION_STORAGE_DIR`, not `path`.** This is an `exec` task: it runs in the
host mount namespace, so there is nothing to mount `/var/lib/redis` into.
Legion exports the real host path as `LEGION_STORAGE_DIR` (and
`LEGION_VOLUME_DATA`) and the `path` field is recorded as intent. A `container`
task would use the path directly. See
[docs/volumes.md](../volumes.md#how-path-works-per-task-kind).

**`--appendonly yes` is not optional here.** Without it Redis persists only via
periodic RDB snapshots, so a restart silently drops everything written since the
last save — and the volume becomes decorative. `everysec` fsyncs once a second:
you can lose up to a second of writes on a hard kill, in exchange for not
fsyncing on every command. `--appendfsync always` is the durable-but-slow
setting if that second matters.

**`count: 1`.** Two Redis processes on one AOF directory corrupt it. Slot mode
guarantees at most one live allocation per instance even across a network
partition (see
[Node loss and fencing](../stateful-workloads.md#node-loss-and-fencing-at-most-one)),
which is exactly the guarantee this needs.

### Verify it persists

```bash
redis-cli -h <addr> -p <port> SET greeting hello
legion job run configs/jobs/redis.jsonnet --force-reschedule
# wait for it to come back
redis-cli -h <addr> -p <port> GET greeting   # "hello"
```

## The cache

```jsonnet
{
  name: 'cache',
  kind: 'exec',
  count: 2,
  group: 'cache',
  // No `stateful` block at all.
  config: {
    command: [
      'redis-server',
      '--save', '',
      '--appendonly', 'no',
      '--maxmemory', '256mb',
      '--maxmemory-policy', 'allkeys-lru',
      '--port', '$LEGION_PORT_REDIS',
    ],
  },
}
```

**No `stateful` block** is the design, not an omission. A cache has nothing
worth a stable identity: it gains no volume, no node pin, and no fencing window,
so a lost replica is replaced in seconds instead of waiting out
`onLost.replaceAfter`. Stateless failover is *faster* precisely because nothing
has to be proven dead first.

Scale it freely:

```bash
# bump count, resubmit — new replicas start cold and warm up
legion job run configs/jobs/redis.jsonnet
```

### If the cache needs scratch space

Some workloads want a big scratch area without wanting durability. Claim one
under `volumes` rather than `stateful.storage`:

```jsonnet
volumes: [{ name: 'scratch', driver: 'tmpfs', params: { size: '1G' },
            path: '/scratch' }],
```

`tmpfs` is RAM-backed, wiped on unmount, and **Linux only**. It advertises
itself as *ephemeral*, which is why admission rejects it under
`stateful.storage`: that field promises data survives a reschedule, and a tmpfs
cannot. Trying it produces:

```
task cache stateful.storage: volume driver `tmpfs` is ephemeral (its contents do
not survive an unmount), so it cannot back stateful.storage — which promises an
instance's data survives reschedules. Declare it under `volumes` instead if you
want scratch space
```

A `tmpfs` claim is shared per task, so two allocations on one node share it. For
private per-allocation scratch, `$LEGION_TASK_DIR` already exists and is cleaned
up on stop.

## Choosing between them

| | Cache | Store |
| --- | --- | --- |
| `stateful` block | none | `mode: 'slot'` |
| Volume | none (or a `tmpfs` claim) | `local-dir` / `nfs` |
| Persistence | `--save ''`, `--appendonly no` | `--appendonly yes` |
| Eviction | `maxmemory` + `allkeys-lru` | none — evicting real data is data loss |
| Failover | seconds | waits out the fencing window |
| `count` | as many as you want | 1 per data directory |

If you are unsure which you have, ask what happens when the process is killed
and comes back empty. If the answer is "some extra database load", it is a
cache. If it is "users are logged out" or "we lost the queue", it is a store.

## Surviving a node loss

`local-dir` pins the store to its node: if that node dies the instance waits
rather than starting empty somewhere else. To let it move, put the data on
network storage:

```jsonnet
storage: {
  path: '/var/lib/redis',
  driver: 'nfs',
  params: { server: '10.0.0.5', export: '/exports/redis' },
}
```

Redis on NFS is workable because its write pattern is append-then-rewrite rather
than random O_DIRECT I/O, but measure it: `appendfsync everysec` over a slow
export will show up as latency. If a node loss must be survivable *and* fast,
Redis replication is the better answer — see below.

## Replication and Sentinel

For a replicated setup, run the primary and replicas as separate tasks in one
group. The replica task points at the primary's service address:

```jsonnet
config: {
  command: ['redis-server', '--dir', '$LEGION_STORAGE_DIR',
            '--replicaof', 'redis.service.legion', '6379'],
},
```

Each replica is its own slot instance with its own volume, so a restarted
replica resyncs from local data instead of a full transfer.

Automatic promotion needs Sentinel (or a client library that handles it).
Sentinel itself is a stateless task with `count: 3` — it needs no volume, only
quorum. Legion's [KV store](../kv-store.md) can hold the current primary
address for clients that would rather resolve than gossip.

## Troubleshooting

**Data disappeared after a restart.** Check `--appendonly yes` is actually set —
this is the single most common cause. `redis-cli CONFIG GET appendonly`
confirms.

**`redis-server` fails with a permissions error on the dir.** The worker's user
owns the volume directory. Either run the task as that user, or set
`params: { mode: '0777' }` on the volume (blunt, but honest for a dev cluster).

**The store instance will not place.** The node-local pin — see
[docs/volumes.md](../volumes.md#when-a-replica-will-not-place).

**The cache pool has fewer replicas than `count`.** Nothing to do with volumes:
check `legion job list` and the eval output for capacity or constraints.

## See also

- [docs/volumes.md](../volumes.md) — plugins, the contract, operating volumes
- [docs/stateful-workloads.md](../stateful-workloads.md) — instances, fencing, retention
- [Running Postgres](postgres.md) — the same machinery when durability is mandatory
