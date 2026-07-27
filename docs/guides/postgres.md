# Running Postgres on Legion

Postgres is the honest test of a scheduler's stateful story. It is a single
writer, it will corrupt itself if two processes open one data directory, and
losing its disk means losing the product. This guide runs one on Legion and
then walks through what happens when things go wrong.

Runnable spec: [`configs/jobs/postgres.jsonnet`](../../configs/jobs/postgres.jsonnet).

## Run it

```bash
legion job run configs/jobs/postgres.jsonnet
legion job list
legion volume list --job postgres
```

```
 VOLUME    DRIVER     JOB       INSTANCE          STATE    NODE
 9f2c8a…   local-dir  postgres  postgres-db-0     created  node-a1*

* node-local: the owning instance can only be replaced on that node.
```

Connect through service discovery rather than a hardcoded address:

```bash
legion service resolve postgres
# 10.0.0.4:29315
psql "postgres://postgres:legion-demo@10.0.0.4:29315/legion"
```

## The spec, and why each part is there

```jsonnet
{
  name: 'postgres',
  tasks: [{
    name: 'postgres',
    kind: 'container',
    count: 1,
    group: 'db',

    stateful: {
      mode: 'slot',
      storage: {
        path: '/var/lib/postgresql/data',
        driver: 'local-dir',
        params: { size: '20G' },
        retainOnScaleIn: true,
        retainOnStop: true,
      },
      onLost: { selfTerminateAfter: '90s', replaceAfter: '3m' },
    },

    config: {
      image: 'postgres:16-alpine',
      command: ['postgres'],
      env: {
        PGDATA: '/var/lib/postgresql/data/pgdata',
        POSTGRES_PASSWORD: 'legion-demo',
        POSTGRES_DB: 'legion',
      },
    },
    network: { mode: 'auto', ports: [{ name: 'pg', to: 5432 }] },
    service: { name: 'postgres', port: 'pg' },
  }],
}
```

**`stateful.mode: 'slot'`** gives the task a stable instance whose id and
storage survive reschedules. It also buys the guarantee Postgres actually needs:
at most one live allocation per instance, *including across a network
partition*. A missed heartbeat is not proof a node is dead, so Legion waits out
`onLost.replaceAfter` before rebinding the data directory — by which time the
partitioned worker has provably self-fenced. See
[Node loss and fencing](../stateful-workloads.md#node-loss-and-fencing-at-most-one).

**`count: 1`** is the whole cluster here. Postgres does not scale by running N
copies of one data directory; scaling means streaming replication, which is a
different topology (see [Scaling out](#scaling-out-replicas) below).

**`storage.path`** is a real bind mount because this is a `container` task. An
`exec` task would read `$LEGION_STORAGE_DIR` instead — see
[docs/volumes.md](../volumes.md#how-path-works-per-task-kind).

**`PGDATA` is a subdirectory of the mount**, not the mount itself. The official
image refuses to `initdb` into a directory that already exists with content, and
a freshly mounted volume can carry `lost+found`. Pointing `PGDATA` one level
down sidesteps a first-boot failure that is otherwise baffling.

**Retention flags are explicit** even though both default to `true`. Losing a
database to a scale-in is not something to leave to a default a reader has to
look up.

## Where the data lives

```bash
legion volume get 9f2c8a1b…
```

```
Volume:     9f2c8a1b…
Driver:     local-dir
State:      created
Scope:      node-local (instance is pinned to its node)
Job:        postgres
Instance:   postgres-db-0
Node:       node-a1
Mounted at: /var/lib/legion/workers/node-a1/volumes/local-dir/9f2c8a1b…
Params:     {"size":"20G"}
```

`local-dir` records `size` but does not enforce it — a directory has no quota.
If you need a hard bound, put the volume on a filesystem that has one (see
[Putting it on a real disk](#putting-it-on-a-real-disk)).

## Day-2 operations

The spec declares [actions](../exec-and-actions.md) — pre-baked commands that
run *inside* the container, so the image's own `psql`/`pg_dump` do the work and
nothing needs installing on your machine:

```bash
legion action run postgres psql --tty            # interactive psql
legion action run postgres sql -p query='SELECT count(*) FROM t'
legion action run postgres dump > backup.sql     # pg_dump to stdout
```

That trio covers the daily loop: poke at data, run a one-off statement, take an
ad-hoc backup. Worth adding as your deployment matures — a `vacuum` action
(`VACUUM ANALYZE`), a `top` action (`SELECT * FROM pg_stat_activity`), and a
`restore` action taking a dump on stdin. Storage-side operations deliberately
stay out of the jobspec: `legion volume list/get/delete` and
`legion instance replace` are the operator's tools, not the job's.

The spec also carries a **commented-out scheduled backup task** — a nightly
`pg_dump` to compressed files. Uncomment it and point it somewhere durable
(object storage, a `host` volume on a backup disk). Volumes are storage, not
backups: a dead disk and `volume delete --force` both lose data; only a copy
that leaves the node is a safety net.

## What survives what

| Event | Data | Instance id |
| --- | --- | --- |
| Task crashes and restarts | ✅ | ✅ |
| `legion job run` with a new image | ✅ | ✅ |
| Worker process restarts | ✅ | ✅ |
| Node reboots | ✅ | ✅ |
| `legion job stop` then `job run` | ✅ (`retainOnStop: true`) | ✅ |
| `legion job destroy` | ❌ reclaimed | ❌ |
| `legion volume delete --force` | ❌ abandoned | ✅ (fresh, empty volume) |

Try it:

```bash
psql … -c 'CREATE TABLE t (x int); INSERT INTO t VALUES (1);'
legion job run configs/jobs/postgres.jsonnet --force-reschedule
# wait for it to come back
psql … -c 'SELECT * FROM t;'   # 1
```

## When the node dies

`local-dir` is node-local, so the instance is pinned. If `node-a1` goes away,
the replacement does **not** place elsewhere — it waits, and says so:

```
Failed to place task postgres: instance postgres-db-0 holds node-local storage
on node node-a1, which is unavailable. Waiting for that node to return; use
`legion volume delete --force <id>` to abandon the data if it never will
```

This is the correct behavior for a database. Silently starting Postgres on an
empty disk somewhere else would look like a successful failover and be a total
data loss. Your options:

1. **Bring the node back.** The instance places itself the moment it returns,
   with its data. Usually the right answer.
2. **Restore from a backup** onto a fresh volume:
   `legion volume delete --force <id>`, then let the instance place anywhere and
   restore into it.
3. **Do not use node-local storage** — see below.

## Making it survive a node loss

Put the data somewhere every node can reach:

```jsonnet
storage: {
  path: '/var/lib/postgresql/data',
  driver: 'nfs',
  params: { server: '10.0.0.5', export: '/exports/postgres', options: 'vers=4,hard' },
}
```

`nfs` is not node-local, so nothing is pinned: if the node dies, Legion places
the replacement anywhere that can reach the server and Postgres comes up on its
own data. The fencing window still applies — the replacement waits for
`onLost.replaceAfter` before binding, which is exactly what stops two Postgres
processes from opening one data directory across a partition.

### What node loss looks like on NFS, step by step

The question that matters: *the node dies — how does Postgres come back with
the same volume?* Answer: automatically. The walkthrough, with the default
fencing policy:

1. `node-a1` (running `postgres-db-0`) drops off the network. Nothing is
   replaced yet: a missed heartbeat is not proof of death, and the volume row
   still says `nfs`, unpinned.
2. At ~90s the partitioned worker self-fences: it kills the Postgres process
   locally (`onLost.selfTerminateAfter`). Its mount is released with it.
3. At 3m (`onLost.replaceAfter`) the manager confirms the loss and frees the
   allocation. `legion instance get postgres-db-0` had been showing
   `pending-loss-confirmation` the whole time.
4. The scheduler places a replacement on any node advertising the `nfs`
   driver — no pin, because the volume is not node-local. The new worker's
   `mount` reattaches the **same** export subdirectory: the volume row carries
   the server/export params and the driver's recorded attrs, and the subdir
   defaults to the volume id, so the mount is deterministic.
5. Postgres starts against its own data directory. Same instance id, same
   volume, different node. Total downtime ≈ `replaceAfter` plus Postgres
   recovery.

Nothing to run, nothing to reattach by hand. If you set `replaceAfter:
'never'`, step 3 waits for you: `legion instance replace postgres-db-0` is the
manual trigger. Compare the `local-dir` version of the same story in
[When the node dies](#when-the-node-dies) — that one *waits for the node*,
because the bytes are on it.

Two things to know before doing this in production:

- **NFS and Postgres need care.** Use `hard` mounts (a soft mount can return
  I/O errors mid-write, which Postgres treats as corruption), and confirm your
  server honors `fsync`. Postgres's own documentation is the authority here.
- **The mount needs privileges.** The worker must run as root, or `nfs` reports
  itself unavailable and the job is rejected at submit with the reason. Check
  with `legion volume drivers`.

## Putting it on a real disk

To pin Postgres to a specific NVMe rather than the worker's state directory,
declare it on the worker and claim it:

```jsonnet
// configs/workers/db.jsonnet
{
  workerType: 'db',
  pool: 'db',
  hostVolumes: {
    pgdata: { path: '/mnt/nvme0/postgres', create: true },
  },
}
```

```jsonnet
// jobspec
volumes: [{ name: 'pgdata', driver: 'host', params: { name: 'pgdata' },
            path: '/var/lib/postgresql/data' }],
// and drop stateful.storage — the host volume IS the storage now
```

Trade-off: a `host` volume is shared and has no per-instance lifecycle, so it is
the right tool for exactly one Postgres. `legion volume delete` on it will
**not** remove the data — Legion never deletes operator-declared paths.

## Scaling out (replicas)

`count: 3` on this spec gives you three independent Postgres instances with
three independent data directories, not a replicated cluster. That is a
perfectly good primitive — it is what you want for per-tenant databases — but it
is not high availability.

For streaming replication, run the primary and the replicas as separate tasks in
one group: the replica task's command runs `pg_basebackup` against the
primary's service address on first boot (an empty `$LEGION_STORAGE_DIR` is the
signal), then starts in standby mode. Each replica gets its own slot instance
and its own volume, and `rollout.slots.order: 'sequential'` restarts them
highest-ordinal-first on a config change so the primary is touched last.

Automatic failover (promoting a replica when the primary dies) needs a
consensus participant Legion does not provide today; Patroni or repmgr running
alongside is the usual answer, using [Legion's KV
store](../kv-store.md) for leader election.

## Backups

Volumes are storage, not backups: `legion volume delete --force` and a failed
disk both lose data. Two built-in paths:

- **On demand:** `legion action run postgres dump > backup.sql`.
- **Scheduled:** the spec ships a commented-out nightly `pg_dump` task —
  uncomment it and point the output somewhere durable (object storage via
  `aws s3 cp -`, or a `host` volume on a dedicated backup disk):

```jsonnet
// already in configs/jobs/postgres.jsonnet, commented out
tasks+: [{
  name: 'backup',
  kind: 'container',
  group: 'db',
  lifecycle: 'batch',
  schedule: '0 3 * * *',
  config: {
    image: 'postgres:16-alpine',
    command: ['sh', '-c', 'pg_dump "$DATABASE_URL" | gzip > "$LEGION_TASK_DIR/pg-$(date +%F).sql.gz"'],
    env: { DATABASE_URL: 'postgres://postgres:legion-demo@postgres.service.legion:5432/legion' },
  },
}],
```

## Troubleshooting

**The task starts and immediately exits.** Check `legion job logs`. The usual
cause is `PGDATA` pointing at the mount root — see above.

**The volume is stuck in `pending`.** No worker has mounted it yet. `legion
volume get <id>` shows `lastError` if a driver failed; `legion volume drivers`
shows whether the plugin is usable on any node.

**The instance will not place.** Almost always the node-local pin. `legion
instance get postgres-db-0` and `legion volume list --job postgres` together
tell you which node it wants.

**Two Postgres processes ran at once.** This should be impossible without an
operator override. The one way to force it is `legion instance replace` on a
node that is merely partitioned rather than dead — that command exists to skip
the fencing gate, and this is the risk it carries.

## See also

- [docs/volumes.md](../volumes.md) — plugins, the contract, operating volumes
- [docs/stateful-workloads.md](../stateful-workloads.md) — instances, fencing, retention
- [Running Redis](redis.md) — the same machinery when durability is optional
