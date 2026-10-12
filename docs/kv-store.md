# KV Store

The manager includes a key-value store. Use it to hold configuration and
feature flags, and to feed values into jobs and tasks. This page covers the
data model, the API, and how jobs react when a key changes.

## Data Model

Primary objects:

- `kv_entry`:
  - `key` (string, primary key)
  - `value` (text), stored encrypted by default (see below)
  - `value_type` (`string`, `number`, `json`)
  - `version` (int64, optimistic concurrency)
  - `enc_key_id`: the data key the value is encrypted under, `NULL` for a
    plaintext row
  - timestamps
- `kv_lock`:
  - `key`, `lock_id`, optional owner
  - acquired/expiry timestamps

## Encryption at rest

The manager encrypts values before they reach the store. It uses AES-256-GCM
under a data key, and that data key is wrapped by a root key the manager keeps
outside the database. The state file, backups, the streamed replica, and raft
snapshots all hold ciphertext.

Encryption is on by default. Keep `<storePath>/keys/root.key` with your
backups and you need nothing else. The API and CLI work the same way: `value`
is always plaintext on the wire, and `KvEntry.encrypted` reports how the row
is stored. Raw state access (`legion state query`, `state:read`) sees only
ciphertext, so `kv` path policies are the only way to read a value.

For providers, rotation, locked keyrings, backups and restores, and recovery,
see [KV Encryption at Rest](kv-encryption.md).

## CRUD Operations

Manager API and CLI support:

- put
- get
- list by prefix
- delete
- json patch (for JSON values only)

Value normalization rules:

- `string`: stored as-is
- `number`: must parse as JSON number
- `json`: must parse as valid JSON

### Reading a key that does not exist

`legion kv get` on a missing key fails: it prints the standard error envelope
with code `NotFound` and exits non-zero, in both output formats.

```bash
$ legion kv get app/flag -j; echo "exit=$?"
{ "ok": false, "error": { "code": "NotFound", "message": "kv key app/flag not found",
  "hint": "`legion kv list` shows the keys that exist; `legion kv put` creates one",
  "retryable": false } }
exit=1
```

So the obvious shell idiom works: `legion kv get app/flag || legion kv put
app/flag on`. The `GetKv` RPC itself still answers a missing key with an empty
`entry` rather than an error (see its proto comment); the CLI is what turns
that into `NotFound`. There is no MCP tool for KV.

## Versioned Writes

`expected_version` semantics:

- write/update succeeds only when expected equals current version
- create can require `expected_version=0`
- mismatch returns conflict

This is used for optimistic concurrency.

## Locking Model

Optional lock enforcement for mutating operations:

- Acquire lock with TTL (`acquire_kv_lock`).
- Renew/release lock by `lock_id`.
- Expired locks are cleaned lazily during lock checks.
- Mutations that provide wrong/missing lock ID conflict when lock is active.

`force` on acquire can take over an existing lock.

## JSON Patch

`patch_kv` requires entry type `json`.

- Patch payload must be RFC6902-style JSON patch array.
- Patch is applied server-side and bumps version.

## Job KV References

Job submit path resolves KV references inside jobspec JSON:

- String values starting with `$` and containing `/` are treated as KV references.
- Referenced KV value replaces that string in jobspec.
- Resolved references are tracked in `job_kv_reference` with JSON pointers.

Legacy `$` references are implicitly watched and required:

- A put to the key re-materializes the job config and rolls it out.
- A delete marks the job failed with reason `missing_kv_dependency`
  (the reference can no longer be rendered). See "Fail semantics" below.

## Reactive updates (KV-driven task updates)

Tasks can opt into KV-backed env and file injections with explicit
per-reference update policies. See `configs/jobs/kv-reactive-demo.ncl`
for a minimal runnable example and `configs/jobs/maximal.ncl` for the
full surface.

### Task spec API

```nickel
task = {
  runtime = "exec",
  # Task-level policy defaults for all injections below.
  kv = { watch = true, onChange = "restart", onDelete = "fail" },
  env = [
    {
      name = "FEATURE_FLAGS",
      value = {
        # Exactly one of:
        template = "{{ kv \"flags/global\" }}",  # inline template
        templateKv = "templates/feature-flags",  # KV key holding the template
      },
      # Optional per-entry policy override (field-by-field).
      kv = { onChange = "rollout" },
    },
  ],
  files = [
    {
      path = "/etc/app/runtime.conf",
      mode = "0640",
      file = { template = "tenant={{ kv \"tenant/id\" }}\n" },
    },
  ],
}
```

Templates replace `{{ kv "some/key" }}` (or bare `kv "some/key"`) with the
current KV value. The manager renders templates at submit and on every
reconciliation. The result lands in the task config the worker receives (the
`env` map or the `files[]` array). A `templateKv` source tracks the template
key itself and every key the template looks up, so a change to either triggers
the entry's policy.

If a required key is missing at submit time, the submit fails with
`kv entry <key> not found`. Legion never admits a job with unresolvable
references.

### Policy semantics

Per-reference policy fields (all optional):

| Field      | Values                                | Hard default |
|------------|---------------------------------------|--------------|
| `watch`    | `true` / `false`                      | `false`      |
| `onChange` | `none`, `update` (files only), `restart`, `rollout`, `fail` | `none` |
| `onDelete` | `none`, `restart`, `rollout`, `fail`  | `fail`       |

Precedence is field by field. An entry-level `kv` block overrides only the
fields it sets. Unset fields inherit from the task-level `kv` block, and fields
unset in both places use the hard defaults above. For example, an entry can
override `onChange` while inheriting `watch = true` from the task, or set
`watch = false` to opt out entirely.

A put resolves through `onChange` and a delete through `onDelete`. Edges with
`watch = false` (the default) never act. A put still refreshes the rendered
value in the stored desired config, and the task picks it up on its next
restart or update. When one KV change matches several edges of a job, the
strongest action wins: `none < update < restart < rollout < fail`.

Actions:

- `none`: re-render and store the desired state. Nothing is scheduled.
- `update` (files only): re-render, then rewrite the file under the running
  task and apply its `changeMode`. The allocation is not replaced. See
  [Live file updates](#live-file-updates).
- `restart`: re-render, then enqueue a `job_update` eval. This re-evaluates
  all of the job's allocations against the new config, including tasks that
  don't use the changed key. A watched-key restart replaces every task's
  allocations in the job in place, without a staged rollout, so co-located
  tasks pick up the change together. Use `rollout` if you need staged
  progression.
- `rollout`: re-render, then create a rollout for the new config revision
  (manual progression is honored) and enqueue `rollout_tick`.
- `fail`: mark the job failed (see below). Typically used as `onDelete`.

A deleted key can't be re-rendered, so entries that depend on it keep their
last rendered value. `onDelete = "restart"` and `onDelete = "rollout"` still
fire even though the rendered config is unchanged.

### Live file updates

`onChange = "update"` rewrites a rendered file under the running task when its
KV value changes, instead of replacing the allocation. Use it for anything a
process can reload: a TLS certificate, a config file, a feature-flag list.

```nickel
files = [
  {
    path = "tls/bundle.pem",
    mode = "0400",
    file = { template = "{{ kv \"certs/app.example.com\" }}" },
    kv = { watch = true, onChange = "update" },
    changeMode = "signal",       # noop (default) | signal | restart
    changeSignal = "SIGHUP",     # required by signal
    splay = "10s",               # optional random delay before acting
  },
],
```

What happens on a `kv put`:

1. The manager re-renders the job config and moves the job's running
   allocations to it in place. Nothing is stopped or rescheduled, and no
   rollout is created.
2. On its next heartbeat, each worker compares the file's new content and
   mode with what the allocation has. When either changed, it rewrites the
   file atomically (temp file created with the declared mode, fsync, rename),
   so a reader never sees a torn file and a key is never readable at a
   broader mode.
3. The worker applies `changeMode`:
   - `noop`: nothing more. Use it for clients that re-read the file.
   - `signal`: send `changeSignal` (for example `SIGHUP` for nginx or
     HAProxy). The runtime must support signals (`exec` and `container` do).
   - `restart`: restart the task in place, on the same node.

   When several files of one allocation change together, the strongest mode
   wins: one restart, or each distinct signal once. `splay` delays the action
   by a random amount up to the largest `splay` among them, so a fleet does
   not reload at once.

The new content is also recorded in the worker's copy of the task config, so a
later restart launches with it.

Rules, checked at submit:

- `update` is rejected for env entries, including when it comes from the
  task-level default. A running process's environment cannot change:
  `task api env FLAGS: onChange = "update" cannot change a running process's
  environment; use "restart" or "rollout"`.
- `update` is rejected for `onDelete`. A deleted key has nothing to render.
- `changeMode`, `changeSignal`, and `splay` are rejected unless the file's
  `onChange` resolves to `update`.
- `changeMode = "signal"` needs `changeSignal`, from the same signal set as
  workload identities.

Limitations:

- The rewrite reaches the task on the worker's next heartbeat, so expect up
  to one heartbeat interval of delay after the debounce window.
- A job that is `failed` from a missing KV dependency recovers through
  `restart` even when the edge says `update`. Only replacing the allocations
  clears their failure stamps.
- A put that changes both an `update` file and an env entry watched with
  `restart` or `rollout` takes the stronger action. The file arrives with the
  replacement.
- Only files carrying `onChange = "update"` are rewritten live. Any other
  rendered file, and every env value, still waits for the next start.

### Debounce / coalescing

The manager coalesces bursts of KV writes so jobs don't see a storm of evals
or rollouts. Set the window with `kvDebounceMs` in the manager config. The
default is `500`. Set it to `0` to disable debouncing and reconcile
synchronously inside the KV RPC.

- The first event opens a window, and every KV event within the window joins
  the pending batch. Later events don't extend the window, so reconciliation
  waits at most one window.
- Within a batch, the last event per key picks the policy. A put followed by a
  delete resolves through `onDelete`. A delete followed by a put resolves
  through `onChange`.
- The flush re-renders from the KV state at flush time, so the final config
  reflects the latest values. At most one action fires per job per window (the
  strongest across all matched edges).
- Pending events live in memory on the manager that took the KV write. With
  debouncing on, the manager logs reconciliation errors because there is no
  RPC caller to return them to. With `kvDebounceMs = 0`, errors return on the
  KV RPC.
- A flush can still be lost: it can fail, the manager can crash inside the
  window, or it can lose raft leadership. To recover, the control-lane leader
  sweeps every 30 seconds. It re-renders each job with KV references from the
  current KV state, and reconciles any job whose stored config no longer
  matches, as if the put had just arrived. It also fails any job whose
  `onDelete = "fail"` key is gone while the job is not failed. The manager
  logs `Job missed a KV-driven reconciliation; reconciling now` when the sweep
  acts.
- The sweep can't recover a missed delete whose policy is `restart` or
  `rollout`. A deleted key keeps its last rendered value, so the job's config
  still looks current and nothing records that the delete went unhandled. If
  the manager crashes inside the debounce window right after such a delete,
  the job keeps running on the last rendered value until its next update.
- A recovered put applies the strongest `onChange` among the job's present
  keys, because the sweep can't tell which key changed.

### Fail semantics and recovery

When the resolved action is `fail` (usually `onDelete = "fail"` and the key was
deleted):

- The job status becomes `failed` (visible in `job list` / `job info`).
- The affected allocations get the status `failed:missing_kv_dependency`
  (visible in the ledger, the UI, and state-store queries). A missing
  injection key only marks allocations of the tasks that depend on it. Sibling
  tasks in the same job are left alone. A missing legacy `$key` reference
  makes the whole rendered config unresolvable, so it marks every allocation
  of the job.
- In both cases Legion doesn't tear down allocations or reschedule anything.
  The job stays failed with the stale config, with no restart or rollout
  storm.
- The KV dependency edges are retained.

Recovery paths:

- Put the missing key back. The put resolves through the edge's `onChange`.
  A `restart` or `rollout` action re-renders the config, clears the
  `failed:missing_kv_dependency` stamps, clears the job's `failed` status
  (it reads `running` again once its allocations are back), and replaces the
  failed allocations.
- `onChange = "none"` doesn't recover on its own. A put re-renders and stores
  the desired config but schedules nothing, so the job stays `failed` with its
  marked allocations until you act.
- Run `job run` to resubmit the jobspec. This is the recovery path for the
  `onChange = "none"` case, and it works in every case. It clears the failed
  status and re-runs the job through the normal submit and eval flow with the
  latest KV state.

### Capability matrix

Each task runtime reports whether it supports KV injections. Legion rejects
a submit that needs an unsupported capability with this error:
`Task <name> runtime <runtime> does not support kv env/file injection`.

| Kind        | env injection | file injection |
|-------------|---------------|----------------|
| `exec`      | yes           | yes            |
| `container` | yes           | yes (`path` must resolve under `/legion/task`) |
| `noop`      | no            | no             |
| plugins     | per `supports_kv_env_injection` / `supports_kv_file_injection` |

Container file paths: a container task sees the allocation's task directory
bind-mounted at `/legion/task` and nothing else from the host. A rendered
file's `path` must resolve under that directory, and relative paths resolve
against it. Legion rejects an out-of-range path at submit.

```nickel
files = [
  { path = "config/app.toml", file = { template = "tenant={{ kv \"tenant/id\" }}\n" } },
  #  -> readable inside the container at /legion/task/config/app.toml
],
```

Legion mounts the directory rather than individual files, so a file rewritten
on the host is visible inside the running container. Files are written
atomically at start, and with `onChange = "update"` they are rewritten the
same way while the container runs (see
[Live file updates](#live-file-updates)). Without `update`, a re-render only
reaches the container when the allocation restarts or is replaced. See
[docs/task-runtime-container.md](task-runtime-container.md).

### Env precedence

Injected env vars are rendered into the task config `env` map. They override
a static `env` entry of the same name in the task config. Use distinct names if
you need both.

### Testing this locally in a basic way

Start a manager with an in-process worker:

```sh
cargo run -- start manager -c configs/manager.ncl
```

Seed the watched key, then run the demo job (one exec task with a watched env and
file injection with `onChange = "restart"`, `onDelete = "fail"`):

```sh
cargo run -- kv put demo/banner hello
cargo run -- job run configs/jobs/kv-reactive-demo.ncl
cargo run -- job list -j        # job kv-reactive-demo running
```

Trigger a restart by changing the key, then watch the allocation be
replaced (within the debounce window, ~500ms by default):

```sh
cargo run -- kv put demo/banner world
cargo run -- ledger             # allocation id changed; task re-ran with BANNER=world
cat /tmp/legion/kv-demo.conf    # banner=world
```

Delete the key to see `onDelete = "fail"`:

```sh
cargo run -- kv delete demo/banner
cargo run -- job list -j        # status: failed
cargo run -- ledger -j          # allocation status: failed:missing_kv_dependency
```

Recover by putting the key back:

```sh
cargo run -- kv put demo/banner back
cargo run -- job list -j        # status: running; task restarted with BANNER=back
```
