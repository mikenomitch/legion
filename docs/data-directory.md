# Data Directory

Every manager and worker keeps local state on disk: the manager's SQLite state
store and raft log, and each worker's state file, task logs, and identity
tokens. By default all of it lives under `./.legion`, relative to the process's
working directory.

Set `LEGION_DATA_DIR` to move the whole tree somewhere else:

```bash
LEGION_DATA_DIR=/var/lib/legion legion start manager -c manager.ncl
LEGION_DATA_DIR=/var/lib/legion legion start worker "$TOKEN"
```

One variable is enough to run a second, isolated cluster from the same
checkout, give a systemd unit or container a single directory to mount and back
up, or start over without deleting your existing data.

## What lives where

| State | Default | Override |
| --- | --- | --- |
| Manager state store (`store.sqlite`, `keys/`, `backups/`) | `<data dir>` | `storePath` |
| Raft log, snapshots, cluster secret | `<data dir>/raft` | `persistence.raft.dataDir` |
| Worker state, allocation dirs, task logs | `<data dir>/workers/<worker-id>/` | `LEGION_WORKER_STATE_PATH` (state file only) |
| Worker node certificate and its private key (`node.pem`, `node.key`, `ca.pem`, each mode `0600`) | `identity/` beside the worker state file | moves with `LEGION_WORKER_STATE_PATH` |
| Identity token backing files | `<data dir>/identity/<alloc-id>/<name>/token` | none |
| Worker node key, certificate, and cluster CA bundle (`node.key`, `node.pem`, `ca.pem`, mode `0600`) | `<data dir>/workers/<worker-id>/identity/` (beside the state file) | moves with `LEGION_WORKER_STATE_PATH` |
| Simulator fleet key and certificate | `<data dir>/sim/<name prefix>/identity/` | none |
| Initial management token (`bootstrap-token`, mode `0600`), written by the manager that mints it | `<data dir>/bootstrap-token` | none |

`<data dir>` is `$LEGION_DATA_DIR` when it is set and non-empty, and `.legion`
otherwise. A relative value resolves against the process's working directory.

An explicit setting always wins over the data dir. If your manager config sets
`storePath`, that path is used whether or not `LEGION_DATA_DIR` is set. The
shipped `configs/manager.ncl` leaves `storePath` unset so the variable applies.

The CLI's saved login token (`~/.legion/token`) is per-user, not per-process,
and does not move. Use `LEGION_TOKEN_FILE` to relocate it. A manager writes
the initial management token there at first start only when the file does not
exist yet (see [Auth & ACLs](auth-and-acls.md#where-the-initial-token-goes)),
so a second cluster started from the same account keeps using the first
cluster's token until you export the second one's `bootstrap-token` as
`LEGION_TOKEN`.

## Running two clusters from one checkout

A manager and worker on one host also share ports, so a second cluster needs
its own ports as well as its own data dir:

```bash
# Second manager: copy configs/manager.ncl with a different `server.port`.
LEGION_DATA_DIR=/tmp/legion-b legion start manager -c manager-b.ncl

# Second worker: its own data dir and worker-plane port.
LEGION_DATA_DIR=/tmp/legion-b LEGION_WORKER_PORT=4858 \
  legion start worker "$TOKEN"
```

The worker's `identity/` directory holds the private key of its node
certificate, which is what lets a manager trust the worker plane on that
node. Treat it like any other credential: it is owner-only on disk, and
deleting it only makes the worker request a new certificate on its next
start.

A worker picks its ID from `<data dir>/workers/`. With one data dir per
cluster, each worker finds only its own ID and needs no `--name`.

## Upgrading from the old store default

Earlier builds defaulted `storePath` to `.legion/store.sqlite`. The store path
is a directory, so a manager that ran without `storePath` has its database at
`.legion/store.sqlite/store.sqlite`. The shipped configs always set
`storePath = ".legion"` and are not affected.

When `storePath` is unset and `<data dir>/store.sqlite` is a directory, the
manager keeps using that legacy location and logs a warning at startup:

```text
WARN Using the legacy default state store location; set `storePath` to this path to keep using it path=.legion/store.sqlite
```

To silence it, either set `storePath` to that path, or stop the manager and
move the directory's contents up one level.

## Limitations

- There is no config-file equivalent. The worker has no config file, and the
  manager already has `storePath` and `raft.dataDir` for per-path control.
- The identity token directory cannot be moved independently of the data dir.
