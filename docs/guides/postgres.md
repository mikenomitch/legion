# Running Postgres on Legion

Postgres is a good test of how a scheduler handles stateful work. It has a
single writer, it corrupts itself if two processes open one data directory,
and losing its disk means losing your data. This guide runs Postgres on Legion
and then walks through what happens when things go wrong.

Runnable spec: [`configs/jobs/postgres.ncl`](../../configs/jobs/postgres.ncl).

## Run it

```bash
legion job run configs/jobs/postgres.ncl
legion job list
legion volume list --job postgres
```

```
 VOLUME    DRIVER     JOB       INSTANCE          STATE    NODE
 9f2c8a…   local-dir  postgres  postgres-db-0     created  node-a1*

* node-local: the owning instance can only be replaced on that node.
```

Connect through service discovery instead of a hardcoded address:

```bash
legion service resolve postgres
# 10.0.0.4:29315
psql "postgres://postgres:legion-demo@10.0.0.4:29315/legion"
```

## The spec, and why each part is there

```nickel
{
  name = "postgres",
  tasks = [{
    name = "postgres",
    runtime = "container",
    count = 1,
    group = "db",

    stateful = {
      mode = "slot",
      storage = {
        path = "/var/lib/postgresql/data",
        driver = "local-dir",
        params = { size = "20G" },
        retainOnScaleIn = true,
        retainOnStop = true,
      },
      onLost = { selfTerminateAfter = "90s", replaceAfter = "3m" },
    },

    config = {
      image = "postgres:16-alpine",
      command = ["sh", "-c", "exec docker-entrypoint.sh postgres -p \"$LEGION_PORT_PG\""],
      env = {
        PGDATA = "/var/lib/postgresql/data/pgdata",
        POSTGRES_PASSWORD = "legion-demo",
        POSTGRES_DB = "legion",
      },
    },
    network = { mode = "auto", ports = [{ name = "pg" }] },
    service = { name = "postgres", port = "pg" },
    checks = [{ type = "health", kind = "tcp", port = "pg", every = "10s", timeout = "5s", boot_grace_time = "60s" }],
  }],
}
```

**The command runs the image's entrypoint itself.** On Linux, a container
task's `command` replaces the image's `ENTRYPOINT` as well as its `CMD`, and
`docker-entrypoint.sh` is what runs `initdb` on a fresh volume and drops from
root to the `postgres` user. It does that with `chown` and `su-exec`, which
need the `CHOWN`, `SETUID`, and `SETGID` capabilities that container tasks get
by default (see
[Capabilities](../task-runtime-container.md#capabilities-security)).

**Postgres listens on the port Legion assigned**, `$LEGION_PORT_PG`, not 5432.
Linux containers share the node's network, so a fixed port would collide with
a second Postgres on the node and would not be the port the service
advertises. The same command works on macOS, where Apple `container` publishes
the assigned port to the same port inside the VM.

**The health check is TCP.** Exec checks run on the worker host, outside the
container, where `pg_isready` and the task's env do not exist.

**`stateful.mode = "slot"`** gives the task a stable instance whose id and
storage survive reschedules. It also gives Postgres the guarantee it needs: at
most one live allocation per instance, even across a network partition. A
missed heartbeat doesn't prove a node is dead. So Legion waits out
`onLost.replaceAfter` before rebinding the data directory. By then, the
partitioned worker is guaranteed to have stopped its copy (self-fenced). See
[Node loss and fencing](../stateful-workloads.md#node-loss-and-fencing-at-most-one).

**`count = 1`** is the whole cluster here. Postgres doesn't scale by running N
copies of one data directory. Scaling means streaming replication, which is a
different topology (see [Scaling out](#scaling-out-replicas) below).

**`storage.path`** is a bind mount because this is a `container` task. An
`exec` task would read `$LEGION_STORAGE_DIR` instead. See
[docs/volumes.md](../volumes.md#how-path-works-per-task-runtime).

**`PGDATA` is a subdirectory of the mount.** The official image refuses to
`initdb` into a directory that already has content, and a freshly mounted
volume can contain `lost+found`. Pointing `PGDATA` one level down avoids a
confusing failure on first boot.

**Retention flags are explicit** even though both default to `true`. You
shouldn't have to look up a default to know whether a scale-in will delete
your database.

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

`local-dir` records `size` but doesn't enforce it, because a directory has no
quota. If you need a hard limit, put the volume on a filesystem that has one
(see [Putting it on a real disk](#putting-it-on-a-real-disk)).

## Day-2 operations

The spec declares [actions](../exec-and-actions.md). These are predefined
commands that run inside the container. The image's own `psql`/`pg_dump` do
the work, so you don't need to install anything on your machine:

```bash
legion action run postgres psql --tty            # interactive psql
legion action run postgres sql --param query='SELECT count(*) FROM t'
legion action run postgres dump > backup.sql     # pg_dump to stdout
```

These three cover the daily work: look at data, run a one-off statement, and
take an ad-hoc backup. As your deployment matures, consider adding a `vacuum`
action (`VACUUM ANALYZE`), a `top` action (`SELECT * FROM pg_stat_activity`),
and a `restore` action that takes a dump on stdin. Storage operations stay out
of the jobspec on purpose. `legion volume list/get/delete` and
`legion instance replace` are tools for the operator, not the job.

For scheduled backups, add the nightly `pg_dump` task shown in
[Backups](#backups) and point it somewhere durable (object storage, or a
`host` volume on a backup disk). Volumes are not
backups. A dead disk or `volume delete --force` both lose data. Only a copy
that leaves the node protects you.

## What survives what

| Event | Data | Instance id |
| --- | --- | --- |
| Task crashes and restarts | ✅ | ✅ |
| `legion job run` with a new image | ✅ | ✅ |
| Worker process restarts | ✅ | ✅ |
| Node reboots | ✅ | ✅ |
| `legion job stop` then `job run` | ✅ (`retainOnStop = true`) | ✅ |
| `legion job destroy` | ❌ reclaimed | ❌ |
| `legion volume delete --force` | ❌ abandoned | ✅ (fresh, empty volume) |

Try it:

```bash
psql … -c 'CREATE TABLE t (x int); INSERT INTO t VALUES (1);'
legion job run configs/jobs/postgres.ncl --force-reschedule
# wait for it to come back
psql … -c 'SELECT * FROM t;'   # 1
```

## When the node dies

`local-dir` is node-local, so the instance is pinned. If `node-a1` goes away,
the replacement does **not** place elsewhere. It waits, and tells you why:

```
Failed to place task postgres: instance postgres-db-0 holds node-local storage
on node node-a1, which is unavailable. Waiting for that node to return; use
`legion volume delete --force <id>` to abandon the data if it never will
```

This is the correct behavior for a database. Silently starting Postgres on an
empty disk somewhere else would look like a successful failover but lose all
your data. Your options:

1. **Bring the node back.** The instance places itself as soon as the node
   returns, with its data. This is usually the right answer.
2. **Restore from a backup** onto a fresh volume:
   `legion volume delete --force <id>`, then let the instance place anywhere and
   restore into it.
3. **Don't use node-local storage.** See below.

## Making it survive a node loss

Put the data somewhere every node can reach:

```nickel
storage = {
  path = "/var/lib/postgresql/data",
  driver = "nfs",
  params = { server = "10.0.0.5", export = "/exports/postgres", options = "vers=4,hard" },
}
```

`nfs` is not node-local, so nothing is pinned. If the node dies, Legion places
the replacement on any node that can reach the server, and Postgres comes up
on its own data. The fencing window still applies. The replacement waits for
`onLost.replaceAfter` before binding, which stops two Postgres processes from
opening one data directory across a partition.

### What node loss looks like on NFS, step by step

When the node dies, Postgres comes back on the same volume automatically.
Here is what happens with the default fencing policy:

1. `node-a1` (running `postgres-db-0`) drops off the network. Nothing is
   replaced yet. A missed heartbeat doesn't prove the node is dead, and the
   volume row still says `nfs`, unpinned.
2. At ~90s the partitioned worker self-fences. It kills the Postgres process
   locally (`onLost.selfTerminateAfter`), which also releases its mount.
3. At 3m (`onLost.replaceAfter`) the manager confirms the loss and frees the
   allocation. Until then, `legion instance get postgres-db-0` shows
   `pending-loss-confirmation`.
4. The scheduler places a replacement on any node that advertises the `nfs`
   driver. There is no pin, because the volume is not node-local. The new
   worker's `mount` reattaches the **same** export subdirectory. The volume
   row carries the server/export params and the driver's recorded attrs, and
   the subdir defaults to the volume id, so the mount always lands in the same
   place.
5. Postgres starts against its own data directory. Same instance id, same
   volume, different node. Total downtime is about `replaceAfter` plus
   Postgres recovery.

You don't need to run or reattach anything by hand. If you set
`replaceAfter = "never"`, step 3 waits for you, and you trigger it with
`legion instance replace postgres-db-0`. Compare this with the `local-dir`
case in [When the node dies](#when-the-node-dies). That one waits for the node,
because the data is on it.

Two things to know before doing this in production:

- **NFS and Postgres need care.** Use `hard` mounts, because a soft mount can
  return I/O errors mid-write, which Postgres treats as corruption. Confirm
  your server honors `fsync`. Postgres's own documentation is the authority
  here.
- **The mount needs privileges.** The worker must run as root. Otherwise `nfs`
  reports itself unavailable and the job is rejected at submit with the
  reason. Check with `legion volume drivers`.

## Putting it on a real disk

To pin Postgres to a specific NVMe drive instead of the worker's state
directory, declare the drive on the worker and claim it in the jobspec:

```nickel
# configs/workers/db.ncl
{
  workerType = "db",
  pool = "db",
  hostVolumes = {
    pgdata = { path = "/mnt/nvme0/postgres", create = true },
  },
}
```

```nickel
# jobspec
volumes = [{ name = "pgdata", driver = "host", params = { name = "pgdata" },
            path = "/var/lib/postgresql/data" }],
# and drop stateful.storage — the host volume IS the storage now
```

The trade-off is that a `host` volume is shared and has no per-instance
lifecycle, so it only suits a single Postgres instance. `legion volume delete`
on it will **not** remove the data, because Legion never deletes
operator-declared paths.

## Scaling out (replicas)

`count = 3` on this spec gives you three independent Postgres instances with
three independent data directories. That is not a replicated cluster. It's
useful for per-tenant databases, but it is not high availability.

For streaming replication, run the primary and the replicas as separate tasks
in one group. On first boot (when `$LEGION_STORAGE_DIR` is empty), the replica
task's command runs `pg_basebackup` against the primary's service address,
then starts in standby mode. Each replica gets its own slot instance and its
own volume. On a config change, `rollout.slots.order = "sequential"` restarts
them highest ordinal first, so the primary restarts last.

Automatic failover (promoting a replica when the primary dies) needs a
consensus participant that Legion doesn't provide today. The usual answer is
to run Patroni or repmgr alongside, using [Legion's KV
store](../kv-store.md) for leader election.

## Backups

Volumes are not backups. `legion volume delete --force` and a failed disk both
lose data. There are two built-in ways to back up:

- **On demand:** `legion action run postgres dump > backup.sql`.
- **Scheduled:** add a nightly `pg_dump` task to the spec's `tasks` array.
  Point the output somewhere durable (object storage via `aws s3 cp -`, or a
  `host` volume on a dedicated backup disk):

```nickel
# add this entry to the spec's `tasks` array
{
  name = "backup",
  runtime = "container",
  group = "db",
  lifecycle = "batch",
  schedule = "0 3 * * *",
  # Reach the database through service discovery. The worker load-balances
  # $LEGION_UPSTREAM_POSTGRES (127.0.0.1:<port>) to the healthy Postgres.
  connect = { upstreams = [{ to = "postgres" }] },
  config = {
    image = "postgres:16-alpine",
    command = ["sh", "-c", "pg_dump \"postgres://postgres:legion-demo@$LEGION_UPSTREAM_POSTGRES/legion\" | gzip > \"$LEGION_TASK_DIR/pg-$(date +%F).sql.gz\""],
  },
}
```

The task connects through a [`connect` upstream](../service-discovery.md#connecting-to-services-the-connect-block)
because the Postgres task's host port is dynamic (`network.mode = "auto"`), so
there is no fixed address to hardcode.

## Troubleshooting

**The task starts and immediately exits.** Check `legion job logs`. The usual
cause is `PGDATA` pointing at the mount root (see above).

**The volume is stuck in `pending`.** No worker has mounted it yet. `legion
volume get <id>` shows `lastError` if a driver failed. `legion volume drivers`
shows whether the plugin is usable on any node.

**The instance won't place.** This is almost always the node-local pin. Run
`legion instance get postgres-db-0` and `legion volume list --job postgres`
to see which node it wants.

**Two Postgres processes ran at once.** This should be impossible without an
operator override. The only way to force it is `legion instance replace` on a
node that is partitioned but not dead. That command exists to skip the
fencing gate, and this is the risk it carries.

## See also

- [docs/volumes.md](../volumes.md): plugins, the contract, operating volumes
- [docs/stateful-workloads.md](../stateful-workloads.md): instances, fencing, retention
- [Running Redis](redis.md): the same machinery when durability is optional
