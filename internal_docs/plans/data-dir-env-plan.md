# `LEGION_DATA_DIR`: one root for a process's on-disk state

> **Judgement calls:** [data-dir-env-plan_calls.md](../calls/data-dir-env-plan_calls.md)

**Status: SHIPPED.** Deferred: the CLI token file (`~/.legion/token`) stays
per-user and does not follow the variable.

## Problem

Every piece of local state a manager or worker writes defaults to a path under
`./.legion`: the state store, the raft log, worker state, and identity token
files. The only override is per-file: `storePath` and `raft.dataDir` in the
manager config, and `LEGION_WORKER_STATE_PATH` for one worker file. Nothing
moves the whole tree.

That bites whenever two Legion processes share a working directory:

- A developer with a long-running local cluster cannot start a clean, throwaway
  one from the same checkout without copying and editing `configs/manager.ncl`.
  The shipped config also pins `storePath = ".legion"`, so a stale store from
  another branch (a failed migration, say) blocks `cargo run -- start manager`
  until it is deleted.
- `LEGION_WORKER_STATE_PATH` moves the state file, but the worker still picks
  its ID by scanning `./.legion/workers` (`src/worker/daemon.rs:518`). A second
  worker sees the first one's IDs and refuses to start without `--name`.
- Identity token fallbacks always land in `./.legion/identity`.

## Prior art

- **Nomad** has one `data_dir` (config field or `-data-dir` flag) and derives
  the server's raft/state and the client's alloc dirs from it.
  [docs](https://developer.hashicorp.com/nomad/docs/configuration#data_dir)
- **Consul** is the same: `data_dir`, with `-data-dir` on the CLI.
- **etcd** takes `--data-dir` or `ETCD_DATA_DIR`, and every flag has an env
  twin. [docs](https://etcd.io/docs/v3.5/op-guide/configuration/)

We take the single root from all three and the env-var form from etcd, because
`LEGION_ADDR`, `LEGION_TOKEN`, `LEGION_WORKER_PORT` and friends are already how
Legion is configured per process. We reject a config field for now: the
manager config is the only file that would carry it, and the worker has no
config file.

## Jobs to be done

**Developer working on Legion**
- When I already have a local cluster running, I want to start a second, clean
  one from the same checkout, so I can test something without touching the
  first one's data.
- When my local data is broken by a branch switch, I want to point the cluster
  somewhere fresh with one variable, so I am not editing a checked-in config.

**Cluster operator**
- When I run Legion under systemd or in a container, I want all of its state
  under one directory I choose, so backup, volume mounts and cleanup cover
  exactly one path.

*Not in scope:* job authors and agents. Neither sees where a process keeps its
state.

## Goals and non-goals

Goals:
- One env var moves every `.legion/*` default for a manager or worker process.
- Behavior with the variable unset is identical to today, including existing
  dev clusters started from `configs/manager.ncl`.

Non-goals:
- A config-file `dataDir` field. Revisit if operators ask for it.
- Moving the per-user CLI token (`~/.legion/token`). It is user credentials,
  not cluster state, and `LEGION_TOKEN_FILE` already overrides it.

## What exists today

- `DEF_STORE_PATH = ".legion/store.sqlite"` (`src/constants.rs:57`). The store
  path is a *directory* (`Store::db_path` joins `store.sqlite` to it), so the
  built-in default produces `.legion/store.sqlite/store.sqlite`. Every shipped
  config overrides it with `storePath = ".legion"`.
- `DEF_RAFT_DATA_DIR = ".legion/raft"` (`src/structs/config.rs:1482`).
- `DEF_WORKER_STATE_DIR`, `DEF_WORKER_STATE_PATH` (`src/constants.rs:148`).
- `IDENTITY_TOKEN_DIR = ".legion/identity"` (`src/worker/identity.rs:57`).

## Design

```bash
LEGION_DATA_DIR=/tmp/scratch-cluster cargo run -- start manager -c configs/manager.ncl
LEGION_DATA_DIR=/tmp/scratch-cluster cargo run -- start worker "$TOKEN"
```

Resolution, most specific first:

1. An explicit config field (`storePath`, `raft.dataDir`) or
   `LEGION_WORKER_STATE_PATH` for the worker state file.
2. `$LEGION_DATA_DIR/<sub-path>`.
3. `./.legion/<sub-path>`, today's default.

| State | Default |
| --- | --- |
| State store (`storePath`) | `<data dir>` |
| Raft log (`raft.dataDir`) | `<data dir>/raft` |
| Worker state | `<data dir>/workers/<id>/worker_state.json` |
| Identity token fallbacks | `<data dir>/identity` |

An empty `LEGION_DATA_DIR` is treated as unset.

### The store default changes to `<data dir>`

For the variable to apply to the shipped configs, they cannot pin `storePath`.
So the built-in store default becomes the data dir itself, which is where
every shipped config already put it, and `configs/manager.ncl` and
`configs/manager-raft.ncl` drop their explicit `storePath`/`raft.dataDir`.

Compatibility: a manager that ran with *no* `storePath` has its DB at
`<data dir>/store.sqlite/store.sqlite`. When `storePath` is unset and
`<data dir>/store.sqlite` is a directory, the manager keeps using it and logs a
warning naming the fix.

## Phases

One phase. The change is small and every piece only matters together.

## Testing

- Unit tests for resolution: unset, set, empty, and the legacy store fallback.
- Manual: run two clusters side by side from one checkout, one with the
  variable set, and confirm neither touches the other's files.

## Rollout

No action for existing users. Unset, every path resolves exactly as before.

## Acceptance criteria

- With `LEGION_DATA_DIR` set, a manager and worker started from the repo root
  write nothing under `./.legion`.
- With it unset, an existing dev store at `./.legion/store.sqlite` and a legacy
  `./.legion/store.sqlite/store.sqlite` both still open.
