# Judgement calls: `LEGION_DATA_DIR`

Plan: [data-dir-env-plan.md](../plans/data-dir-env-plan.md). Entries are
ranked by project impact, highest first.

## 1. The store default became the data dir, with a legacy fallback

**Call.** With `storePath` unset, the manager now uses `<data dir>` as its
store directory instead of `<data dir>/store.sqlite`. The shipped
`configs/manager.ncl` and `configs/manager-raft.ncl` drop their explicit
`storePath = ".legion"` so the variable applies to them. When
`<data dir>/store.sqlite` is a directory, the manager keeps opening it and
warns once:

```text
WARN Using the legacy default state store location; set `storePath` to this path to keep using it path=.legion/store.sqlite
```

**Alternatives.** (a) Keep the old default and leave `storePath` pinned in the
shipped configs. The variable would then move workers, raft and identity but
not the manager's store when started from the shipped config, which is the
exact case that motivated it. (b) Let `LEGION_DATA_DIR` override an explicit
`storePath`. (c) Change the default with no fallback.

**Trade-off.** The default is now a changed default, with a compatibility
branch that lives in the code until we decide no one has a legacy store left.
Anyone with *both* a `.legion/store.sqlite/` legacy dir and a config that
leaves `storePath` unset keeps the legacy store until they act on the warning.

**Why (simplicity).** The old default produced
`.legion/store.sqlite/store.sqlite`, which no shipped config used, because
every one overrode it. Matching the default to what everyone already runs
removes a footgun and lets one variable work everywhere. (b) inverts the
precedence every other Legion setting follows, explicit config beats ambient
env. (c) would silently start a new empty cluster next to an existing one.

## 2. An env var, not a config field or CLI flag

**Call.** The only surface is `LEGION_DATA_DIR`.

```bash
LEGION_DATA_DIR=/tmp/scratch legion start manager -c configs/manager.ncl
```

**Alternatives.** Nomad and Consul's `data_dir` config field plus a
`-data-dir` flag.

**Trade-off.** No way to set it from the manager config file. Operators who
want everything in the config keep using `storePath` and `raft.dataDir`.

**Why (simplicity).** The worker has no config file, so a config field covers
only half the processes. Per-process settings in Legion are already env vars
(`LEGION_ADDR`, `LEGION_WORKER_PORT`, `LEGION_WORKER_STATE_PATH`), and one env
var reaches both the manager and the worker the same way.

## 3. The CLI token file does not follow the data dir

**Call.** `~/.legion/token` stays in the user's home.

**Alternatives.** Put it under `<data dir>/token`.

**Trade-off.** "Everything under one dir" has one exception, stated in the
public doc.

**Why (simplicity).** It is a user's login, not a process's state. Moving it
would log you out whenever you switched clusters by data dir, and
`LEGION_TOKEN_FILE` already relocates it.
